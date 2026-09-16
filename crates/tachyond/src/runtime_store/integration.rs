//! One local operator gate. No model registration, git workflow, or automatic retry.
use super::{
    campaign_launch::{Launch, LAUNCHES},
    RuntimeStore,
};
use ghost::harness::{
    runtime::{NoopEventSink, NoopOutputStore, ToolContext, ToolIdentity, ToolPolicy},
    tools::workspace::integration::LockedFiles,
};
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tachyon_api::{
    integration::{IntegrationPlan, PatchBundle, MAX_PLAN_BYTES},
    types::{ApiRequest, ApiResponse, ArtifactPublication, ArtifactRegistration},
};
use tachyond::artifact_store::ArtifactStore;

const JOURNAL: TableDefinition<&str, &[u8]> = TableDefinition::new("local_integrations_v1");
const LIMIT: usize = 65_536;
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Serialize, Deserialize)]
struct Journal {
    #[serde(default)]
    revision: u64,
    campaign: String,
    root: String,
    plan: IntegrationPlan,
    expected_state: String,
    approval_artifact: String,
    phase: String,
    files: Vec<FileRecord>,
}
#[derive(Serialize, Deserialize)]
struct FileRecord {
    path: String,
    before: String,
    after: String,
    version: String,
    outcome: String,
    #[serde(default)]
    observed_version: Option<String>,
    #[serde(default)]
    observed_sha256: Option<String>,
}

fn context(root: &Path) -> Result<ToolContext, String> {
    if root.canonicalize().map_err(err)? != root {
        return Err("approved root changed or contains symlinks".into());
    }
    let mut policy = ToolPolicy::worker_default(root.to_owned());
    policy.max_write_bytes = LIMIT;
    policy.sync_writes = true;
    Ok(ToolContext {
        workspace_root: root.into(),
        cwd: root.into(),
        identity: ToolIdentity::default(),
        deadline: Instant::now() + Duration::from_secs(8),
        cancellation: tokio_util::sync::CancellationToken::new(),
        policy: Arc::new(policy),
        event_sink: Arc::new(NoopEventSink),
        output_store: Arc::new(NoopOutputStore),
        host_service: None,
    })
}

impl RuntimeStore {
    fn integration_state(&self, id: &str) -> Result<(Launch, String), String> {
        let tx = self.database.begin_read().map_err(err)?;
        let table = tx.open_table(LAUNCHES).map_err(err)?;
        let value = table
            .get(id)
            .map_err(err)?
            .ok_or("campaign has no approved manifest")?;
        let launch: Launch = serde_json::from_slice(value.value()).map_err(err)?;
        launch.validate_digest()?;
        if launch.manifest.campaign_id != id {
            return Err("manifest scope mismatch".into());
        }
        let campaign = self.research_request(&ApiRequest::CampaignGet { id: id.into() })?;
        let state = hash(&serde_json::to_vec(&(value.value(), campaign)).map_err(err)?);
        Ok((launch, state))
    }

    fn save_integration(&self, key: &str, journal: &mut Journal) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        {
            let mut table = tx.open_table(JOURNAL).map_err(err)?;
            let current = table
                .get(key)
                .map_err(err)?
                .map(|v| serde_json::from_slice::<Journal>(v.value()).map_err(err))
                .transpose()?;
            if let Some(current) = current {
                if current.revision != journal.revision
                    || current.plan != journal.plan
                    || current.campaign != journal.campaign
                    || current.root != journal.root
                    || current.expected_state != journal.expected_state
                {
                    return Err("integration journal compare-and-swap conflict".into());
                }
            } else {
                if journal.revision != 0 {
                    return Err("integration journal disappeared".into());
                }
                if table.len().map_err(err)? >= 1000 {
                    return Err("integration journal capacity reached".into());
                }
                for row in table.iter().map_err(err)? {
                    let (_, value) = row.map_err(err)?;
                    let current: Journal = serde_json::from_slice(value.value()).map_err(err)?;
                    if current.root == journal.root && current.phase != "completed" {
                        return Err("workspace has unfinished integration".into());
                    }
                }
            }
            journal.revision = journal
                .revision
                .checked_add(1)
                .ok_or("journal revision exhausted")?;
            let bytes = serde_json::to_vec(journal).map_err(err)?;
            table.insert(key, bytes.as_slice()).map_err(err)?;
        }
        tx.commit().map_err(err)
    }

    pub(crate) fn integration_request(
        &self,
        request: &ApiRequest,
        storage: &Path,
    ) -> Result<ApiResponse, String> {
        let (id, paths, approval) = match request {
            ApiRequest::CampaignIntegrationSnapshot { id, paths } => (id, paths.clone(), None),
            ApiRequest::CampaignIntegrate {
                id,
                plan,
                expected_state,
                confirm,
            } => {
                if !confirm {
                    return Err("explicit operator confirmation required".into());
                }
                plan.validate()?;
                if expected_state.len() != 64
                    || !expected_state.bytes().all(|c| c.is_ascii_hexdigit())
                {
                    return Err("expected_state must be a SHA-256 snapshot token".into());
                }
                (
                    id,
                    plan.expected_versions.keys().cloned().collect(),
                    Some((plan, expected_state)),
                )
            }
            _ => return Err("not an integration request".into()),
        };
        let (launch, _) = self.integration_state(id)?;
        let context = context(&launch.manifest.workspace)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(err)?;
        runtime.block_on(async {
            let locked = LockedFiles::acquire(&context, &paths).await.map_err(|e| format!("{e:?}"))?;
            let snapshots = locked.snapshots(&context).await.map_err(|e| format!("{e:?}"))?;
            let (current_launch, state) = self.integration_state(id)?;
            if current_launch.manifest.workspace != context.workspace_root { return Err("approved workspace changed while waiting for locks".into()); }
            let Some((plan, expected_state)) = approval else {
                return Ok(ApiResponse::CampaignIntegration { report: json!({
                    "expected_state": state,
                    "expected_versions": snapshots.iter().map(|s| (s.path.clone(), s.version.clone())).collect::<BTreeMap<_,_>>(),
                    "sha256": snapshots.iter().map(|s| (s.path.clone(), hash(&s.bytes))).collect::<BTreeMap<_,_>>(),
                    "note": "Pass root versions as host inputs; child stat versions are not root baselines."
                }) });
            };
            let key = hash(&serde_json::to_vec(&(id, &plan.command_id)).map_err(err)?);
            let root = context.workspace_root.to_str().ok_or("root is not UTF-8")?.to_owned();
            let existing = {
                let tx = self.database.begin_write().map_err(err)?;
                let table = tx.open_table(JOURNAL).map_err(err)?;
                let mut existing = None;
                for row in table.iter().map_err(err)? {
                    let (k, v) = row.map_err(err)?;
                    let j: Journal = serde_json::from_slice(v.value()).map_err(err)?;
                    if k.value() == key { existing = Some(j); }
                    else if j.root == root && j.phase != "completed" {
                        return Err(format!("workspace has unfinished integration {}; recover that identical plan first", j.plan.command_id));
                    }
                }
                existing
            };
            if let Some(j) = &existing {
                if j.plan != *plan || j.expected_state != *expected_state || j.root != root || j.campaign != *id {
                    return Err("integration command identity conflict".into());
                }
            } else if *expected_state != state {
                return Err("campaign state conflict; inspect integration snapshot again".into());
            }
            // The work/candidate reference, not a path supplied by a model, selects the source.
            let execution = self.campaign_execution(id, &plan.work_id)?.ok_or("source work not retained in campaign")?;
            let candidate = execution.candidate.as_ref().ok_or("source work has no retained candidate")?;
            if !candidate.candidate_refs.as_ref().is_some_and(|refs| refs.contains(&plan.artifact_id)) {
                return Err("artifact is not a retained candidate reference for this work".into());
            }
            let artifacts = ArtifactStore::open_retained(&storage.join("campaigns").join(id).join("artifacts"), self.retained.clone(), id)?;
            let source = artifacts.metadata(&plan.work_id, &plan.artifact_id)?.ok_or("source artifact not found")?;
            if source.sha256 != plan.artifact_sha256 || source.size_bytes > MAX_PLAN_BYTES as u64
                || source.publication != (ArtifactPublication::Ready { version: plan.artifact_sha256.clone() })
                || source.work_id.as_deref() != Some(&plan.work_id)
                || candidate.work_id != plan.work_id
                || candidate.generation != execution.policy.work.generation
                || candidate.assignment != execution.policy.work.assignment
                || execution.policy.model.identity.campaign_id != *id
                || execution.policy.model.identity.work_id != plan.work_id
                || source.generation != Some(candidate.generation)
                || source.assignment != Some(candidate.assignment)
                || source.attempt_id.as_deref() != Some(&execution.policy.model.identity.attempt_id) {
                return Err("source artifact identity/size conflict".into());
            }
            let bytes = artifacts.read(&plan.work_id, &plan.artifact_id, 0, MAX_PLAN_BYTES)?;
            if bytes.len() as u64 != source.size_bytes || hash(&bytes) != plan.artifact_sha256 { return Err("source artifact digest conflict".into()); }
            let bundle: PatchBundle = serde_json::from_slice(&bytes).map_err(err)?;
            let mut edits = BTreeMap::new();
            for edit in bundle.files {
                if edit.old.is_empty() || edit.old.len() > LIMIT || edit.new.len() > LIMIT
                    || edit.old.contains('\0') || edit.new.contains('\0') || edits.insert(edit.path.clone(), edit).is_some() {
                    return Err("empty, binary, oversized or duplicate exact edit".into());
                }
            }
            if edits.keys().ne(plan.expected_versions.keys()) { return Err("bundle targets differ from approved baseline files".into()); }
            let scope = format!("integration-{key}");
            let mut journal = if let Some(j) = existing { j } else {
                let mut outputs = Vec::new();
                let mut conflicts = Vec::new();
                for snapshot in &snapshots {
                    let edit = &edits[&snapshot.path];
                    if plan.expected_versions[&snapshot.path] != snapshot.version {
                        conflicts.push(format!("{}: version conflict", snapshot.path));
                        continue;
                    }
                    let text = std::str::from_utf8(&snapshot.bytes).map_err(err)?;
                    if text.contains('\0') || text.match_indices(&edit.old).count() != 1 {
                        conflicts.push(format!("{}: old must match exactly once in UTF-8 text", snapshot.path));
                        continue;
                    }
                    let output = text.replacen(&edit.old, &edit.new, 1).into_bytes();
                    if output.len() > LIMIT { return Err("edited file exceeds 65536 bytes".into()); }
                    outputs.push(output);
                }
                if !conflicts.is_empty() { return Err(format!("preflight conflict; no files changed: {}", conflicts.join("; "))); }
                let mut files = Vec::new();
                for (s, output) in snapshots.iter().zip(outputs) {
                    let before = retain(&artifacts, &scope, &s.bytes)?;
                    let after = retain(&artifacts, &scope, &output)?;
                    files.push(FileRecord { path: s.path.clone(), before, after, version: s.version.clone(), outcome: "pending".into(), observed_version: None, observed_sha256: None });
                }
                let approval_artifact = retain(&artifacts, &scope, &serde_json::to_vec(&(id, plan, expected_state)).map_err(err)?)?;
                let mut j = Journal { revision: 0, campaign: id.clone(), root, plan: plan.clone(), expected_state: expected_state.clone(), approval_artifact, phase: "prepared".into(), files };
                self.save_integration(&key, &mut j)?;
                j
            };
            // Recovery also preflights ALL files before resuming any replacement.
            let mut outputs = Vec::new();
            let mut conflict = Vec::new();
            if journal.files.len() != snapshots.len() { return Err("journal file inventory conflict".into()); }
            let approval_bytes = serde_json::to_vec(&(id, plan, expected_state)).map_err(err)?;
            if journal.approval_artifact != hash(&approval_bytes)
                || artifacts.read(&scope, &journal.approval_artifact, 0, MAX_PLAN_BYTES + 4096)? != approval_bytes {
                return Err("retained approval conflict".into());
            }
            for (file, snapshot) in journal.files.iter().zip(&snapshots) {
                let before = artifacts.read(&scope, &file.before, 0, LIMIT + 1)?;
                let after = artifacts.read(&scope, &file.after, 0, LIMIT + 1)?;
                if before.len() > LIMIT || after.len() > LIMIT || hash(&before) != file.before || hash(&after) != file.after
                    || plan.expected_versions.get(&file.path) != Some(&file.version) {
                    return Err("retained integration snapshot digest/identity conflict".into());
                }
                let text = std::str::from_utf8(&before).map_err(err)?;
                let edit = edits.get(&file.path).ok_or("journal target conflict")?;
                if text.contains('\0') || text.match_indices(&edit.old).count() != 1
                    || text.replacen(&edit.old, &edit.new, 1).as_bytes() != after {
                    return Err("retained output differs from approved exact edit".into());
                }
                let pending_original = file.outcome != "applied" && snapshot.version == file.version && snapshot.bytes == before;
                let already_output = file.outcome != "pending" && snapshot.bytes == after;
                if file.path != snapshot.path || !(pending_original || already_output) {
                    conflict.push(snapshot.path.clone());
                }
                outputs.push((after, already_output));
            }
            if !conflict.is_empty() {
                return Ok(ApiResponse::CampaignIntegration { report: json!({"journal":journal,"verification_required":true,"error":format!("recovery conflict: {}; no further writes", conflict.join(", "))}) });
            }
            if journal.phase == "completed" {
                return Ok(ApiResponse::CampaignIntegration { report: json!({"journal":journal,"verification_required":true}) });
            }
            journal.phase = "applying".into();
            self.save_integration(&key, &mut journal)?;
            for (index, (output, already_output)) in outputs.iter().enumerate() {
                if !already_output {
                    journal.files[index].outcome = "applying".into();
                    self.save_integration(&key, &mut journal)?;
                    if let Err(error) = locked.replace(&context, &snapshots[index], output).await {
                        return Ok(ApiResponse::CampaignIntegration { report: json!({"journal":journal,"verification_required":true,"error":format!("{error:?}")}) });
                    }
                    #[cfg(test)]
                    if STOP_AFTER_REPLACE.with(|v| v.replace(false)) { return Err("injected crash after replacement before acknowledgement".into()); }
                }
                journal.files[index].outcome = "applied".into();
                self.save_integration(&key, &mut journal)?;
            }
            #[cfg(test)]
            BEFORE_FINAL_READ.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() { hook(); }
            });
            // Earlier files can change while later files are being replaced, even under flock.
            let final_snapshots = locked.snapshots(&context).await.map_err(|e| format!("final readback failed; retry identical plan: {e:?}"))?;
            for (file, snapshot) in journal.files.iter_mut().zip(final_snapshots) {
                let actual = hash(&snapshot.bytes);
                file.observed_version = Some(snapshot.version);
                file.observed_sha256 = Some(actual.clone());
                if file.path != snapshot.path || file.after != actual {
                    self.save_integration(&key, &mut journal)?;
                    return Ok(ApiResponse::CampaignIntegration { report: json!({"journal":journal,"verification_required":true,"error":"final output conflict; no further writes"}) });
                }
            }
            journal.phase = "completed".into();
            self.save_integration(&key, &mut journal)?;
            Ok(ApiResponse::CampaignIntegration { report: json!({"journal":journal,"verification_required":true}) })
        })
    }
}

#[cfg(test)]
thread_local! { static STOP_AFTER_REPLACE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
thread_local! { static BEFORE_FINAL_READ: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) }; }

#[cfg(test)]
mod tests;

fn retain(artifacts: &ArtifactStore, scope: &str, bytes: &[u8]) -> Result<String, String> {
    let id = hash(bytes);
    let temp = tempfile::tempdir().map_err(err)?;
    let mut file = std::fs::File::create(temp.path().join("snapshot")).map_err(err)?;
    file.write_all(bytes).map_err(err)?;
    file.sync_all().map_err(err)?;
    let registered = artifacts.register(
        scope,
        temp.path(),
        ArtifactRegistration {
            id: id.clone(),
            path: "snapshot".into(),
            kind: "integration-evidence".into(),
            description: "Operator-approved integration snapshot; not correctness evidence".into(),
            size_bytes: bytes.len() as u64,
            sha256: id.clone(),
            task_id: None,
            work_id: None,
            generation: None,
            assignment: None,
            attempt_id: None,
            publication: ArtifactPublication::Pending,
        },
    )?;
    if registered.sha256 != id
        || registered.size_bytes != bytes.len() as u64
        || registered.publication
            != (ArtifactPublication::Ready {
                version: id.clone(),
            })
        || artifacts.read(scope, &id, 0, bytes.len() + 1)? != bytes
    {
        return Err("integration evidence was not retained as exact Ready bytes".into());
    }
    Ok(id)
}
