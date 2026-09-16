//! Host-owned immutable tool event objects; metadata shares runtime.redb.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use tachyon_api::types::{Actor, AgentEvent, EventEnvelope, WorkRequest};
#[cfg(test)]
mod retained_tests;
pub(crate) mod upload;

pub(crate) const TRACES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("context_traces_v1");
pub(crate) const TRACE_ASSIGNMENTS: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("context_trace_assignments_v1");
const MAX_OPERATION: usize = 64 * 1024 * 1024;
const MAX_CAMPAIGN: u64 = 256 * 1024 * 1024;
const MAX_GLOBAL: u64 = 1024 * 1024 * 1024;

pub(crate) struct TraceLimits {
    pub operation: usize,
    pub campaign: u64,
    pub global: u64,
}

impl TraceLimits {
    pub(crate) fn configured() -> Result<Self, String> {
        let read = |key, default| -> Result<u64, String> {
            match std::env::var(key) {
                Ok(value) => value
                    .parse()
                    .map_err(|_| err("invalid host trace capacity")),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(_) => Err(err("invalid host trace capacity")),
            }
        };
        let campaign = read("TACHYON_TRACE_CAMPAIGN_BYTES", MAX_CAMPAIGN)?;
        let global = read("TACHYON_TRACE_GLOBAL_BYTES", MAX_GLOBAL)?;
        if campaign == 0 || campaign > global || global > 64 * MAX_GLOBAL {
            return Err(err("invalid host trace capacity bounds"));
        }
        Ok(Self {
            operation: MAX_OPERATION,
            campaign,
            global,
        })
    }
}

pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    tx.open_table(TRACES).map_err(err)?;
    tx.open_table(TRACE_ASSIGNMENTS).map_err(err)?;
    Ok(())
}

impl RuntimeStore {
    pub(crate) fn adopt_retained_traces(&self) -> Result<(), String> {
        let tx = self.database.begin_read().map_err(err)?;
        let table = tx.open_table(TRACES).map_err(err)?;
        let mut known = std::collections::BTreeSet::new();
        for row in table.iter().map_err(err)? {
            let (key, value) = row.map_err(err)?;
            let r: Resource = serde_json::from_slice(value.value()).map_err(err)?;
            if r.reference.kind != ResourceKind::Trace {
                continue;
            }
            self.retained.adopt(
                key.value().0,
                "trace",
                &r.reference.id,
                r.data["size_bytes"]
                    .as_u64()
                    .ok_or("invalid trace census size")?,
                &r.reference.version,
            )?;
            if (r.data["retention_state"].is_null() || r.data["retention_state"] == "ready")
                && self.trace_read(&r, 0, 1).is_ok()
            {
                let receipt = self
                    .retained
                    .get(key.value().0, "trace", &r.reference.id)?
                    .ok_or("missing trace reservation")?;
                self.retained.ready(&receipt, receipt.expected)?;
            }
            known.insert(r.reference.id);
        }
        if self.trace_root.try_exists().map_err(err)? {
            if self.trace_root.canonicalize().map_err(err)? != self.trace_root {
                return Err("trace census root must be canonical without symlinks".into());
            }
            for entry in std::fs::read_dir(&self.trace_root).map_err(err)? {
                let entry = entry.map_err(err)?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "invalid trace object name")?;
                let metadata = entry.metadata().map_err(err)?;
                if !entry.file_type().map_err(err)?.is_file() {
                    return Err("trace census entry must be a regular file".into());
                }
                if !known.contains(&name) {
                    self.retained.adopt(
                        "host-orphans",
                        "trace_orphan",
                        &name,
                        metadata.len(),
                        "startup census",
                    )?;
                }
            }
        }
        Ok(())
    }
    /// Explicit host upload registration. Only an exact member of the host's
    /// managed-input allowlist can be snapshotted; no broker write action exists.
    pub(crate) fn host_register_input_document(
        &self,
        campaign: &str,
        work: &str,
        input: &tachyon_api::campaign::ChildInput,
        path: &std::path::Path,
        artifacts: &ArtifactStore,
    ) -> Result<ResourceRef, String> {
        artifacts.require_retained(&self.retained, campaign)?;
        self.admitted_work(campaign, work)?;
        let allowed = input
            .files
            .iter()
            .find(|f| f.path == path)
            .ok_or("document not in input allowlist")?;
        let id = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(campaign, work, &input.root, path, &allowed.sha256))
                    .map_err(err)?
            )
        );
        let reference = ResourceRef {
            kind: ResourceKind::Document,
            work_id: work.into(),
            id: id.clone(),
            version: allowed.sha256.clone(),
        };
        if let Ok(existing) = self.trace_resolve(campaign, &reference) {
            let expected: ArtifactRegistration =
                serde_json::from_value(existing.data["artifact"].clone()).map_err(err)?;
            if artifacts.metadata(work, &id)?.as_ref() != Some(&expected) {
                return Err(err("document registration mismatch"));
            }
            artifacts.read(work, &id, 0, 1)?;
            return Ok(reference);
        }
        let size = std::fs::metadata(input.root.join(path)).map_err(err)?.len();
        if size > MAX_CONTEXT_ARTIFACT_BYTES {
            return Err(err("document exceeds 4 MiB"));
        }
        let tx = self.database.begin_write().map_err(err)?;
        let mut table = tx.open_table(TRACES).map_err(err)?;
        use redb::ReadableTableMetadata;
        if table.get((campaign, id.as_str())).map_err(err)?.is_some() {
            return Err("document publication already pending".into());
        }
        if table.len().map_err(err)? >= 20_000 {
            return Err(err("document record capacity exhausted"));
        }
        let mut used = 0u64;
        for row in table.range((campaign, "")..).map_err(err)? {
            let (key, value) = row.map_err(err)?;
            if key.value().0 != campaign {
                break;
            }
            let resource: Resource = serde_json::from_slice(value.value()).map_err(err)?;
            used = used.saturating_add(
                resource.data["size_bytes"]
                    .as_u64()
                    .ok_or("invalid resource size")?,
            );
        }
        if used.saturating_add(size) > self.trace_limits.campaign {
            return Err(err("document campaign capacity exhausted"));
        }
        let pending = Resource {
            reference: reference.clone(),
            occurred_at_ms: None,
            data: json!({"schema_version":1,"size_bytes":size,"retention_state":"staging"}),
        };
        table
            .insert(
                (campaign, id.as_str()),
                serde_json::to_vec(&pending).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        drop(table);
        tx.commit().map_err(err)?;
        // ArtifactStore calls the runtime ledger; never hold this writer across it.
        let artifact = artifacts.register(
            work,
            &input.root,
            ArtifactRegistration {
                id: id.clone(),
                path: path.to_str().ok_or("non-UTF8 document path")?.into(),
                kind: "document".into(),
                description: "Explicit host-managed input".into(),
                size_bytes: size,
                sha256: allowed.sha256.clone(),
                task_id: None,
                work_id: Some(work.into()),
                generation: None,
                assignment: None,
                attempt_id: None,
                publication: ArtifactPublication::Pending,
            },
        )?;
        artifact_resource(&artifact, None)?;
        let resource = Resource {
            reference: reference.clone(),
            occurred_at_ms: None,
            data: json!({"schema_version":1,"size_bytes":size,"artifact":artifact}),
        };
        let encoded = serde_json::to_vec(&resource).map_err(err)?;
        if encoded.len() > 6000 {
            return Err(err("document descriptor exceeds bound"));
        }
        let tx = self.database.begin_write().map_err(err)?;
        Self::require_unarchived_in(&tx, campaign)?;
        let mut table = tx.open_table(TRACES).map_err(err)?;
        table
            .insert((campaign, id.as_str()), encoded.as_slice())
            .map_err(err)?;
        drop(table);
        tx.commit().map_err(err)?;
        Ok(reference)
    }

    /// Called on the blocking pool with launch-owned identities, never worker claims.
    pub(crate) fn record_tool_trace(
        &self,
        funding: &super::super::admission::AdmittedWork,
        work: &WorkRequest,
        attempt: &str,
        worker: &str,
        event: &EventEnvelope,
    ) -> Result<(), String> {
        if event.session_id != worker
            || event.task_id.as_deref() != Some(worker)
            || !matches!(&event.actor, Actor::Worker { id } if id == worker)
            || event.conversation_id.is_some()
            || event.parent_task_id.is_some()
        {
            return Err(err("trace envelope scope mismatch"));
        }
        let (call, phase, content) = match &event.kind {
            AgentEvent::ToolStarted {
                id,
                name,
                arguments,
                ..
            } => (
                id,
                "request",
                serde_json::to_vec(&json!({"name":name,"arguments":arguments})).map_err(err)?,
            ),
            AgentEvent::ToolFinished { id, output, .. } => {
                (id, "result", output.as_bytes().to_vec())
            }
            _ => return Ok(()),
        };
        if call.is_empty()
            || call.len() > 256
            || event.tool_call_id.as_ref().is_some_and(|id| id != call)
        {
            return Err(err("invalid trace call identity"));
        }
        let campaign = &funding.admission.campaign_id;
        let id = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    campaign,
                    &work.work_id,
                    work.generation,
                    work.assignment,
                    attempt,
                    call,
                    phase
                ))
                .map_err(err)?
            )
        );
        let stored = &content[..content.len().min(self.trace_limits.operation)];
        let version = format!("{:x}", Sha256::digest(stored));
        let resource = Resource {
            reference: ResourceRef {
                kind: ResourceKind::Trace,
                work_id: work.work_id.clone(),
                id: id.clone(),
                version,
            },
            occurred_at_ms: Some(event.occurred_at_ms),
            data: json!({"schema_version":1,"attempt_id":attempt,"call_id":call,"phase":phase,
                "generation":work.generation,"assignment":work.assignment,
                "original_sha256":format!("{:x}", Sha256::digest(&content)),
                "redaction_policy":"provider-credential-and-bearer-v1",
                "size_bytes":stored.len(),"original_bytes":content.len(),"truncated_bytes":content.len()-stored.len()}),
        };
        let encoded = serde_json::to_vec(&resource).map_err(err)?;
        if encoded.len() > 6000 {
            return Err(err("oversized trace descriptor"));
        }
        // A single runtime write transaction fences assignment and serializes quota admission.
        let tx = self.database.begin_write().map_err(err)?;
        let admitted = Self::admitted_work_in(&tx, &work.work_id)?;
        // Capturing an already emitted event spends nothing and must remain valid
        // while a parent has temporarily released its running slot in wait/ask.
        if admitted.admission != funding.admission
            || admitted.dispatch_id != funding.dispatch_id
            || work.generation != admitted.admission.generation
            || !matches!(&admitted.state, super::super::admission::DispatchState::Registered { worker_id } if worker_id == worker)
        {
            return Err(err("stale trace assignment"));
        }
        let executions = tx.open_table(EXECUTIONS).map_err(err)?;
        if let Some(row) = executions.get(work.work_id.as_str()).map_err(err)? {
            let current = decode_record(row.value(), &work.work_id)?;
            if current.policy.work != *work
                || current.policy.model.identity.attempt_id != attempt
                || current.phase != super::super::execution::ExecutionPhase::ExecutingUnknown
                || current.settled
            {
                return Err(err("stale trace execution"));
            }
        } else {
            let assignments = tx.open_table(TRACE_ASSIGNMENTS).map_err(err)?;
            let row = assignments
                .get((campaign.as_str(), work.work_id.as_str(), attempt))
                .map_err(err)?
                .ok_or("missing trace assignment")?;
            let (expected_work, expected_worker): (WorkRequest, String) =
                serde_json::from_slice(row.value()).map_err(err)?;
            if work.attempt.is_some() || expected_work != *work || expected_worker != worker {
                return Err(err("stale trace assignment"));
            }
        }
        drop(executions);
        self.store_trace_object(tx, campaign, resource, stored)
    }

    /// Caller binds scope from host authority before supplying diagnostic bytes.
    pub(crate) fn record_context_object(
        &self,
        campaign: &str,
        work: &str,
        attempt: &str,
        request: &str,
        phase: &str,
        bytes: &[u8],
    ) -> Result<ResourceRef, String> {
        if bytes.len() > self.trace_limits.operation {
            return Err(err("context object exceeds operation bound"));
        }
        let id = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(campaign, work, attempt, request, phase)).map_err(err)?
            )
        );
        let resource = Resource {
            reference: ResourceRef {
                kind: ResourceKind::Trace,
                work_id: work.into(),
                id,
                version: format!("{:x}", Sha256::digest(bytes)),
            },
            occurred_at_ms: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(err)?
                    .as_millis() as u64,
            ),
            data: json!({"schema_version":1,"attempt_id":attempt,"request_id":request,"phase":phase,"size_bytes":bytes.len()}),
        };
        let reference = resource.reference.clone();
        let tx = self.database.begin_write().map_err(err)?;
        if Self::admitted_work_in(&tx, work)?.admission.campaign_id != campaign {
            return Err(err("context scope mismatch"));
        }
        self.store_trace_object(tx, campaign, resource, bytes)?;
        Ok(reference)
    }

    fn store_trace_object(
        &self,
        tx: WriteTransaction,
        campaign: &str,
        resource: Resource,
        stored: &[u8],
    ) -> Result<(), String> {
        self.store_trace_object_checked(tx, campaign, resource, stored, || true)
    }

    fn store_trace_object_checked(
        &self,
        tx: WriteTransaction,
        campaign: &str,
        resource: Resource,
        stored: &[u8],
        permitted: impl Fn() -> bool,
    ) -> Result<(), String> {
        self.store_trace_object_committing(tx, campaign, resource, stored, permitted, |_| Ok(()))
    }

    pub(super) fn store_trace_object_committing(
        &self,
        tx: WriteTransaction,
        campaign: &str,
        resource: Resource,
        stored: &[u8],
        permitted: impl Fn() -> bool,
        commit: impl FnOnce(&WriteTransaction) -> Result<(), String>,
    ) -> Result<(), String> {
        let id = &resource.reference.id;
        Self::require_unarchived_in(&tx, campaign)?;
        let size = resource.data["size_bytes"]
            .as_u64()
            .ok_or("invalid trace size")?;
        let encoded = serde_json::to_vec(&resource).map_err(err)?;
        let table = tx.open_table(TRACES).map_err(err)?;
        let old = table
            .get((campaign, id.as_str()))
            .map_err(err)?
            .map(|row| serde_json::from_slice::<Resource>(row.value()).map_err(err))
            .transpose()?;
        if let Some(old) = old {
            if old.reference != resource.reference || old.data != resource.data {
                return Err(err("trace ID conflict"));
            }
            self.trace_read(&old, 0, 1)?;
            drop(table);
            commit(&tx)?;
            return tx.commit().map_err(err);
        }
        let mut campaign_bytes = 0u64;
        let mut global_bytes = 0u64;
        let mut known_objects = std::collections::BTreeSet::new();
        let mut records = 0;
        for row in table.iter().map_err(err)? {
            let (key, value) = row.map_err(err)?;
            records += 1;
            let r: Resource = serde_json::from_slice(value.value()).map_err(err)?;
            if r.reference.kind == ResourceKind::Trace {
                known_objects.insert(r.reference.id.clone());
                global_bytes = global_bytes
                    .checked_add(r.data["size_bytes"].as_u64().ok_or("invalid trace size")?)
                    .ok_or("trace size overflow")?;
            }
            if key.value().0 == campaign {
                campaign_bytes = campaign_bytes
                    .saturating_add(r.data["size_bytes"].as_u64().ok_or("invalid trace size")?);
            }
        }
        if records >= 20_000 || campaign_bytes.saturating_add(size) > self.trace_limits.campaign {
            return Err(err("trace campaign/count capacity exhausted"));
        }
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&self.trace_root)
            .map_err(err)?;
        if std::fs::symlink_metadata(&self.trace_root)
            .map_err(err)?
            .file_type()
            .is_symlink()
            || std::fs::metadata(&self.trace_root).map_err(err)?.uid()
                != nix::unistd::Uid::effective().as_raw()
            || std::fs::metadata(&self.trace_root)
                .map_err(err)?
                .permissions()
                .mode()
                & 0o077
                != 0
        {
            return Err(err("trace root must be private and not a symlink"));
        }
        let mut physical = 0u64;
        let mut orphan_bytes = 0u64;
        let mut files = 0;
        for entry in std::fs::read_dir(&self.trace_root).map_err(err)? {
            let entry = entry.map_err(err)?;
            let size = entry.metadata().map_err(err)?.len();
            physical = physical.checked_add(size).ok_or("trace size overflow")?;
            if !known_objects.contains(
                &entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "invalid trace object name")?,
            ) {
                orphan_bytes = orphan_bytes
                    .checked_add(size)
                    .ok_or("trace size overflow")?;
            }
            files += 1;
            if files >= 20_000 {
                return Err(err("trace file capacity exhausted"));
            }
        }
        if physical
            .max(
                global_bytes
                    .checked_add(orphan_bytes)
                    .ok_or("trace size overflow")?,
            )
            .saturating_add(size)
            > self.trace_limits.global
        {
            return Err(err("trace global capacity exhausted"));
        }
        drop(table);
        let receipt = self.retained.reserve_in(
            &tx,
            campaign,
            "trace",
            id,
            size,
            &resource.reference.version,
            false,
        )?;
        let mut pending = resource.clone();
        pending.data["retention_state"] = json!("staging");
        tx.open_table(TRACES)
            .map_err(err)?
            .insert(
                (campaign, id.as_str()),
                serde_json::to_vec(&pending).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        tx.commit().map_err(err)?;
        // Durable intent precedes file creation. A retry must not overwrite it.
        if !permitted() {
            return Err("trace publication expired after reservation".into());
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(if resource.data["retention_state"] == "staging" {
                0o600
            } else {
                0o400
            })
            .open(self.trace_root.join(&id))
            .map_err(err)?;
        let written = (|| -> Result<(), String> {
            file.write_all(stored).map_err(err)?;
            if resource.data["retention_state"] == "staging" {
                file.set_len(size).map_err(err)?;
            }
            file.sync_all().map_err(err)?;
            std::fs::File::open(&self.trace_root)
                .map_err(err)?
                .sync_all()
                .map_err(err)?;
            if let Some(parent) = self.trace_root.parent() {
                std::fs::File::open(parent)
                    .map_err(err)?
                    .sync_all()
                    .map_err(err)?;
            }
            let tx = self.database.begin_write().map_err(err)?;
            let mut table = tx.open_table(TRACES).map_err(err)?;
            table
                .insert((campaign, id.as_str()), encoded.as_slice())
                .map_err(err)?;
            drop(table);
            if resource.data["retention_state"] != "staging" {
                self.retained.ready_in(&tx, &receipt, stored.len() as u64)?;
            }
            commit(&tx)?;
            tx.commit().map_err(err)
        })();
        written
    }

    pub(crate) fn trace_resolve(
        &self,
        campaign: &str,
        reference: &ResourceRef,
    ) -> Result<Resource, String> {
        let tx = self.database.begin_read().map_err(err)?;
        let table = tx.open_table(TRACES).map_err(err)?;
        let row = table
            .get((campaign, reference.id.as_str()))
            .map_err(err)?
            .ok_or("unknown trace")?;
        let resource: Resource = serde_json::from_slice(row.value()).map_err(err)?;
        if resource.reference != *reference || resource.data["retention_state"] == "staging" {
            return Err(err("trace version/scope mismatch"));
        }
        Ok(resource)
    }

    pub(crate) fn trace_read(
        &self,
        resource: &Resource,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>, String> {
        let r = &resource.reference;
        if r.id.len() != 64 || !r.id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(err("invalid object identity"));
        }
        let size = resource.data["size_bytes"]
            .as_u64()
            .ok_or("invalid trace size")?;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
            .open(self.trace_root.join(&r.id))
            .map_err(err)?;
        if !file.metadata().map_err(err)?.is_file()
            || file.metadata().map_err(err)?.len() != size
            || size > MAX_OPERATION as u64
        {
            return Err(err("invalid trace object"));
        }
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        let mut total = 0u64;
        loop {
            let n = file.read(&mut buffer).map_err(err)?;
            if n == 0 {
                break;
            }
            total += n as u64;
            if total > size {
                return Err(err("trace grew"));
            }
            hash.update(&buffer[..n]);
        }
        if total != size || format!("{:x}", hash.finalize()) != r.version {
            return Err(err("trace checksum mismatch"));
        }
        file.seek(SeekFrom::Start(offset)).map_err(err)?;
        let mut bytes = Vec::new();
        file.take(limit.min(1024) as u64)
            .read_to_end(&mut bytes)
            .map_err(err)?;
        Ok(bytes)
    }

    pub(super) fn trace_list(
        &self,
        campaign: &str,
        query: &Query,
        kind: Option<ResourceKind>,
    ) -> Result<Page, String> {
        self.trace_list_phase(campaign, query, kind, None)
    }

    pub(super) fn trace_list_phase(
        &self,
        campaign: &str,
        query: &Query,
        kind: Option<ResourceKind>,
        phase: Option<&str>,
    ) -> Result<Page, String> {
        let tx = self.database.begin_read().map_err(err)?;
        let table = tx.open_table(TRACES).map_err(err)?;
        let mut page = Page::default();
        let start = query.after.as_deref().unwrap_or("");
        if !start.is_empty() && (start.len() != 64 || !start.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(err("invalid trace cursor"));
        }
        let mut scanned = 0;
        for row in table.range((campaign, start)..).map_err(err)? {
            let (key, value) = row.map_err(err)?;
            if key.value().0 != campaign {
                break;
            }
            if key.value().1 == start {
                continue;
            }
            if scanned == MAX_SCAN {
                return Ok(page);
            }
            scanned += 1;
            let resource: Resource = serde_json::from_slice(value.value()).map_err(err)?;
            self.admitted_work(campaign, &resource.reference.work_id)?;
            if resource.data["retention_state"] != "staging"
                && phase.is_none_or(|phase| resource.data["phase"] == phase)
                && matches_query(&resource, query, kind)?
            {
                page.resources.push(resource);
                let previous = page.next_cursor.clone();
                page.next_cursor = Some(key.value().1.into());
                if check_page(&page).is_err() || page.resources.len() > query.limit {
                    page.resources.pop();
                    page.next_cursor = previous;
                    return Ok(page);
                }
            }
            page.next_cursor = Some(key.value().1.into());
        }
        page.next_cursor = None;
        check_page(&page)?;
        Ok(page)
    }
}
