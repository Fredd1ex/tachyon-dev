//! Exact, one-shot host catalog selections. No worker-supplied execution policy.
use super::*;
use crate::runtime_store::{
    admission::Admission, campaign_ledger::Pool, coordination::WorkAddress, groups::GroupSpec,
    RuntimeStore,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tachyon_api::{
    agents::{Reply, Request},
    types::WorkRequest,
};
use tachyon_model::accounting::{RequestClass, RequestReservation};

const RECEIPTS: TableDefinition<&str, &[u8]> = TableDefinition::new("agent_catalog_receipts_v1");
const COMMANDS: TableDefinition<&str, &str> = TableDefinition::new("agent_catalog_commands_v1");
const PREPARATIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("agent_catalog_preparations_v1");

#[derive(Serialize, Deserialize)]
struct Preparation {
    request: Request,
    parent: WorkAddress,
    fingerprint: serde_json::Value,
    proposal_sha256: Option<String>,
}

impl RuntimeStore {
    pub(crate) fn adopt_retained_snapshots(&self) -> Result<(), String> {
        let write = self.database.begin_write().map_err(|e| e.to_string())?;
        write.open_table(PREPARATIONS).map_err(|e| e.to_string())?;
        write.open_table(RECEIPTS).map_err(|e| e.to_string())?;
        write.commit().map_err(|e| e.to_string())?;
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        for definition in [PREPARATIONS, RECEIPTS] {
            let table = tx.open_table(definition).map_err(|e| e.to_string())?;
            for row in table.iter().map_err(|e| e.to_string())? {
                let (_, value) = row.map_err(|e| e.to_string())?;
                let record: serde_json::Value =
                    serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
                let fingerprint = &record["fingerprint"];
                let Some(profiles) = fingerprint["profiles"].as_array() else {
                    continue;
                };
                let candidates = fingerprint["candidates"]
                    .as_array()
                    .ok_or("invalid snapshot census candidates")?;
                if candidates.len() != profiles.len() {
                    return Err("invalid snapshot census profiles".into());
                }
                let campaign = fingerprint["parent"]["campaign_id"]
                    .as_str()
                    .ok_or("invalid snapshot census campaign")?;
                for (candidate, profile) in candidates.iter().zip(profiles) {
                    let profile: tachyon_api::campaign::ChildProfile =
                        serde_json::from_value(profile.clone()).map_err(|e| e.to_string())?;
                    let workspace = PathBuf::from(
                        candidate["workspace"]
                            .as_str()
                            .ok_or("invalid snapshot census workspace")?,
                    );
                    let id = format!("{}:0", workspace.display());
                    if self.retained.get(campaign, "snapshot", &id)?.is_some() {
                        continue;
                    }
                    let destination = workspace
                        .parent()
                        .ok_or("invalid snapshot census destination")?;
                    let descriptor = serde_json::json!({
                        "campaign_id": campaign, "parent": fingerprint["parent"],
                        "proposal_sha256": record["proposal_sha256"], "work_id": candidate["work"]["work_id"],
                        "request": record["request"], "fingerprint": fingerprint,
                    });
                    let encoded = serde_json::to_vec(&serde_json::json!({"schema_version":1,"destination":destination,"descriptor":descriptor,"profile":profile})).map_err(|e| e.to_string())?;
                    let identity = serde_json::to_string(&serde_json::json!({"descriptor":format!("{:x}", Sha256::digest(&encoded)),"files":null})).map_err(|e| e.to_string())?;
                    // Old descriptors recorded a finite input upper bound, not exact
                    // sizes. Preserve that uncertainty as debt, never as free bytes.
                    let expected = profile
                        .max_input_bytes
                        .checked_add(524288)
                        .ok_or("snapshot census overflow")?;
                    let write = self.database.begin_write().map_err(|e| e.to_string())?;
                    self.retained
                        .reserve_in(&write, campaign, "snapshot", &id, expected, &identity, true)?;
                    let quarantine = destination.with_file_name(format!(
                        ".quarantine-{}",
                        destination
                            .file_name()
                            .ok_or("invalid snapshot slot")?
                            .to_str()
                            .ok_or("invalid snapshot slot")?
                    ));
                    if quarantine.try_exists().map_err(|e| e.to_string())? {
                        self.retained.reserve_in(
                            &write,
                            campaign,
                            "snapshot",
                            &format!("{}:1", workspace.display()),
                            expected,
                            &identity,
                            true,
                        )?;
                    }
                    write.commit().map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct HostCandidate {
    pub admission: Admission,
    pub verification: Admission,
    pub model: RequestReservation,
    pub work: WorkRequest,
    pub evaluator_id: String,
    pub executable: PathBuf,
    pub workspace: PathBuf,
    pub home: PathBuf,
    pub evaluate: Evaluator,
}

#[derive(Clone)]
pub(crate) struct HostTemplate {
    pub template_id: String,
    pub parent: WorkAddress,
    pub candidates: Vec<HostCandidate>,
    /// None means spawn; Some means an exact group, not generated objectives.
    pub group_id: Option<String>,
    pub max_running: usize,
}

#[derive(Default)]
pub(crate) struct HostCatalog {
    pub(super) templates: BTreeMap<String, HostTemplate>,
    pub(super) profiles: BTreeMap<String, tachyon_api::campaign::ChildProfile>,
    pub(in crate::runtime_store) admitted: BTreeMap<LaunchKey, Arc<HostExecution>>,
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    request: Request,
    fingerprint: serde_json::Value,
    policies: Vec<ExecutionPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proposal_sha256: Option<String>,
}

impl HostTemplate {
    fn fingerprint(&self) -> serde_json::Value {
        serde_json::json!({
            "parent": self.parent, "group_id": self.group_id, "max_running": self.max_running,
            "candidates": self.candidates.iter().map(|c| {
                let mut value = serde_json::json!({
                "admission": c.admission, "verification": c.verification,
                "model": c.model, "work": c.work, "evaluator_id": c.evaluator_id,
                "executable": c.executable, "workspace": c.workspace, "home": c.home
                });
                if let Evaluator::Command { staging, config, .. } | Evaluator::CommandDescriptor { staging, config } = &c.evaluate {
                    value["command"] = serde_json::json!({"staging": staging, "config": config});
                }
                value
            }).collect::<Vec<_>>()
        })
    }
}

impl HostScheduler {
    pub(crate) fn approve_profile(
        &self,
        template: HostTemplate,
        profile: tachyon_api::campaign::ChildProfile,
    ) -> Result<(), String> {
        if template.template_id != profile.template_id(&template.parent.campaign_id)
            || template.candidates.len() != profile.max_proposals
            || !(1..=31).contains(&profile.max_proposals)
            || template.candidates.iter().any(|c| {
                c.work
                    .constraints
                    .as_ref()
                    .is_none_or(|w| w.permissions != profile.permissions)
            })
        {
            return Err("dynamic profile does not match approved slots".into());
        }
        let key = template.template_id.clone();
        self.approve(template)?;
        self.broker
            .store
            .host_catalog
            .lock()
            .map_err(|_| "host catalog unavailable")?
            .profiles
            .insert(key, profile);
        Ok(())
    }

    /// Explicit host re-approval restores descriptors, never replays a process.
    pub(crate) fn restore_proposals(&self) -> Result<(), String> {
        let catalog = self
            .broker
            .store
            .host_catalog
            .lock()
            .map_err(|_| "host catalog unavailable")?;
        let tx = self
            .broker
            .store
            .database
            .begin_write()
            .map_err(|e| e.to_string())?;
        let table = tx.open_table(RECEIPTS).map_err(|e| e.to_string())?;
        let mut restore = Vec::new();
        for row in table.iter().map_err(|e| e.to_string())? {
            let (_, value) = row.map_err(|e| e.to_string())?;
            let receipt: Receipt =
                serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
            if receipt.proposal_sha256.is_none() {
                continue;
            }
            let Some(policy) = receipt.policies.first() else {
                return Err("empty proposal receipt".into());
            };
            if catalog.templates.values().any(|t| {
                catalog.profiles.contains_key(&t.template_id)
                    && t.candidates
                        .iter()
                        .any(|c| c.admission.work_id == policy.work.work_id)
            }) {
                let parent = serde_json::from_value(receipt.fingerprint["parent"].clone())
                    .map_err(|e| e.to_string())?;
                restore.push((parent, receipt.request));
            }
        }
        drop(table);
        let preparations = tx.open_table(PREPARATIONS).map_err(|e| e.to_string())?;
        for row in preparations.iter().map_err(|e| e.to_string())? {
            let (_, value) = row.map_err(|e| e.to_string())?;
            let pending: Preparation =
                serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
            if catalog.templates.values().any(|t| {
                t.parent.campaign_id == pending.parent.campaign_id
                    && catalog.profiles.contains_key(&t.template_id)
            }) {
                restore.push((pending.parent, pending.request));
            }
        }
        drop(preparations);
        drop(tx);
        drop(catalog);
        for (parent, request) in restore {
            self.broker.store.admit_catalog(parent, request)?;
        }
        Ok(())
    }

    /// Must be called by the trusted host. This reserves no money or Work slots.
    /// The evaluator_id must identify the exact host callback semantics on reopen.
    pub(crate) fn approve(&self, template: HostTemplate) -> Result<(), String> {
        let id = |s: &str| !s.trim().is_empty() && s.len() <= 256;
        if !id(&template.template_id)
            || !id(&template.parent.work_id)
            || !id(&template.parent.campaign_id)
            || !(1..=32).contains(&template.candidates.len())
            || template.group_id.as_deref().is_some_and(|s| !id(s))
            || (template.group_id.is_none() && template.candidates.len() != 1)
            || !(1..=4096).contains(&template.max_running)
        {
            return Err("invalid approved template bounds".into());
        }
        let mut ids = std::collections::BTreeSet::new();
        for c in &template.candidates {
            if let Evaluator::Command { config, .. } | Evaluator::CommandDescriptor { config, .. } =
                &c.evaluate
            {
                if c.evaluator_id != format!("command:{}", config.config_hash()?) {
                    return Err("child command evaluator binding mismatch".into());
                }
            }
            let a = &c.admission;
            let v = &c.verification;
            let i = &c.model.identity;
            let (tokens, cost) = c.model.estimate.upper_bound().map_err(|e| e.to_string())?;
            if a.campaign_id != template.parent.campaign_id
                || v.campaign_id != a.campaign_id
                || a.pool != Pool::Work
                || v.pool != Pool::Verification
                || c.work.work_id != a.work_id
                || c.work.objective != a.objective
                || c.work.generation != a.generation
                || c.work.assignment == 0
                || i.campaign_id != a.campaign_id
                || i.work_id != a.work_id
                || i.generation != a.generation
                || i.instruction_revision != a.instruction_revision
                || i.class != RequestClass::Work
                || !id(&i.attempt_id)
                || !id(&c.evaluator_id)
                || [a, v].iter().any(|a| {
                    a.objective.trim().is_empty()
                        || a.objective.len() > 32_768
                        || a.generation == 0
                        || a.instruction_revision == 0
                })
                || [
                    &c.model.estimate.model,
                    &c.model.estimate.provider,
                    &c.model.estimate.pricing_revision,
                ]
                .iter()
                .any(|s| !id(s))
                || c.model.estimate.base_url.len() > 4096
                || tokens > a.upper_bound.tokens
                || cost > a.upper_bound.cost_micro_usd
                || !c.executable.is_absolute()
                || !c.workspace.is_absolute()
                || !c.home.is_absolute()
                || [&c.executable, &c.workspace, &c.home]
                    .iter()
                    .any(|p| p.to_str().is_none_or(|s| s.len() > 4096))
                || [&a.work_id, &v.work_id]
                    .iter()
                    .any(|w| !id(w) || **w == template.parent.work_id || !ids.insert((*w).clone()))
            {
                return Err("invalid approved execution policy".into());
            }
        }
        let mut catalog = self
            .broker
            .store
            .host_catalog
            .lock()
            .map_err(|_| "host catalog unavailable")?;
        if catalog.templates.contains_key(&template.template_id) {
            return Err("template already approved".into());
        }
        let count: usize = catalog.templates.values().map(|t| t.candidates.len()).sum();
        let initial = self
            .catalog
            .keys()
            .filter(|key| !catalog.admitted.contains_key(*key))
            .count();
        if count + template.candidates.len() + initial > 256 {
            return Err("host catalog full".into());
        }
        if catalog.templates.values().any(|t| {
            (t.parent.campaign_id == template.parent.campaign_id
                && t.group_id.is_some()
                && t.group_id == template.group_id)
                || t.candidates.iter().any(|c| {
                    ids.contains(&c.admission.work_id) || ids.contains(&c.verification.work_id)
                })
        }) || self.catalog.values().any(|c| {
            ids.contains(&c.policy.work.work_id)
                || ids.contains(&c.policy.verification.admission.work_id)
        }) {
            return Err("catalog identity conflict".into());
        }
        // Re-approval on host restart restores descriptors from committed receipts;
        // it never fabricates a fresh admission or replays an unknown launch.
        let key = serde_json::to_string(&(&template.parent.campaign_id, &template.template_id))
            .map_err(|e| e.to_string())?;
        let tx = self
            .broker
            .store
            .database
            .begin_write()
            .map_err(|e| e.to_string())?;
        let receipt: Option<Receipt> = tx
            .open_table(RECEIPTS)
            .map_err(|e| e.to_string())?
            .get(key.as_str())
            .map_err(|e| e.to_string())?
            .map(|v| serde_json::from_slice(v.value()).map_err(|e| e.to_string()))
            .transpose()?;
        if receipt
            .as_ref()
            .is_some_and(|r| r.fingerprint != template.fingerprint())
        {
            return Err("recovered template policy conflict".into());
        }
        drop(tx);
        let parent = template.parent.clone();
        self.approved
            .lock()
            .map_err(|_| "approval registry unavailable")?
            .push(template.template_id.clone());
        catalog
            .templates
            .insert(template.template_id.clone(), template);
        drop(catalog);
        if let Some(receipt) = receipt {
            self.broker.store.admit_catalog(parent, receipt.request)?;
        }
        Ok(())
    }
}

impl RuntimeStore {
    pub(in crate::runtime_store) fn catalog_templates(
        &self,
        actor: &WorkAddress,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Reply, String> {
        let catalog = self
            .host_catalog
            .lock()
            .map_err(|_| "host catalog unavailable")?;
        let mut selected = catalog.templates.values().filter(|t| {
            !catalog.profiles.contains_key(&t.template_id)
                && &t.parent == actor
                && after.is_none_or(|a| t.template_id.as_str() > a)
        });
        let templates: Vec<_> = selected
            .by_ref()
            .take(limit)
            .map(|t| tachyon_api::agents::Template {
                template_id: t.template_id.clone(),
                group_id: t.group_id.clone(),
                work_count: t.candidates.len(),
                max_running: t.max_running,
            })
            .collect();
        let next_cursor = selected
            .next()
            .and_then(|_| templates.last().map(|t| t.template_id.clone()));
        Ok(Reply::Templates {
            templates,
            next_cursor,
        })
    }

    /// Worker calls hold permit authority; host re-approval restores receipts only.
    /// Catalog lock then one writer; publication follows commit under the lock.
    pub(in crate::runtime_store) fn admit_catalog(
        &self,
        actor: WorkAddress,
        request: Request,
    ) -> Result<Reply, String> {
        self.admit_catalog_with_context(actor, request, None)
    }

    /// At most one new execution action per tick. Work commits with catalog
    /// admission; Verify persists intent until the ordinary review claim writer.
    pub(crate) fn allocation_execution_tick(
        &self,
        campaign: &str,
        owner: &str,
        allocation: &tachyon_api::campaign::CampaignAllocation,
        groups: &[(String, Vec<String>)],
        approved: &[String],
    ) -> Result<bool, String> {
        use tachyon_api::campaign::AllocationControl;
        allocation.validate()?;
        if groups.len() > 64 || groups.iter().any(|(_, w)| w.len() > 32) || approved.len() > 256 {
            return Err("allocation execution projection bound".into());
        }
        for signal in &allocation.signals {
            if !matches!(
                signal.action,
                AllocationControl::Work { .. } | AllocationControl::Verify { .. }
            ) {
                continue;
            }
            let Some((context, replay)) =
                self.allocation_action_context(campaign, owner, signal, groups)?
            else {
                continue;
            };
            match &signal.action {
                AllocationControl::Work { template_id, .. } => {
                    let template = {
                        let catalog = self
                            .host_catalog
                            .lock()
                            .map_err(|_| "host catalog unavailable")?;
                        if !approved.contains(template_id)
                            || catalog.profiles.contains_key(template_id)
                        {
                            return Err("allocation requires an approved fixed template".into());
                        }
                        catalog
                            .templates
                            .get(template_id)
                            .ok_or("unapproved allocation template")?
                            .clone()
                    };
                    let request = if template.group_id.is_some() {
                        Request::Group {
                            template_id: template_id.clone(),
                            command_id: signal.command_id.clone(),
                            max_running: None,
                        }
                    } else {
                        Request::Spawn {
                            template_id: template_id.clone(),
                            command_id: signal.command_id.clone(),
                        }
                    };
                    self.admit_catalog_action(template.parent, request, None, Some(&context))?;
                    // Replays also restore descriptors after a post-commit crash,
                    // but can never recreate a missing catalog receipt.
                    if !replay {
                        return Ok(true);
                    }
                }
                AllocationControl::Verify {
                    work_id,
                    generation,
                    ..
                } => {
                    if replay {
                        continue;
                    }
                    let policy = {
                        let catalog = self
                            .host_catalog
                            .lock()
                            .map_err(|_| "host catalog unavailable")?;
                        if !approved
                            .iter()
                            .filter_map(|id| catalog.templates.get(id))
                            .any(|t| {
                                t.parent.campaign_id == campaign
                                    && t.candidates.iter().any(|c| {
                                        c.work.work_id == *work_id
                                            && c.work.generation == *generation
                                    })
                            })
                        {
                            return Err("unapproved allocation verification descriptor".into());
                        }
                        let entry = catalog
                            .admitted
                            .get(&(campaign.into(), work_id.clone(), *generation))
                            .ok_or("missing admitted verification descriptor")?;
                        if matches!(entry.evaluate, Evaluator::CommandDescriptor { .. }) {
                            return Err("descriptor-only verification cannot execute".into());
                        }
                        entry.policy.clone()
                    };
                    self.stage_allocation_review(&context, &policy)?;
                    return Ok(true);
                }
                _ => unreachable!(),
            }
        }
        Ok(false)
    }

    pub(in crate::runtime_store) fn admit_catalog_with_context(
        &self,
        actor: WorkAddress,
        request: Request,
        artifacts: Option<&tachyond::artifact_store::ArtifactStore>,
    ) -> Result<Reply, String> {
        self.admit_catalog_action(actor, request, artifacts, None)
    }

    pub(in crate::runtime_store) fn admit_catalog_action(
        &self,
        actor: WorkAddress,
        request: Request,
        artifacts: Option<&tachyond::artifact_store::ArtifactStore>,
        allocation: Option<&crate::runtime_store::groups::PolicyActionContext>,
    ) -> Result<Reply, String> {
        use sha2::{Digest, Sha256};
        request.validate()?;
        if let Some(context) = allocation {
            let tachyon_api::campaign::AllocationControl::Work {
                template_id,
                parent_work_id,
                ..
            } = &context.signal.action
            else {
                return Err("not an allocation catalog action".into());
            };
            let selected = match &request {
                Request::Spawn {
                    template_id,
                    command_id,
                }
                | Request::Group {
                    template_id,
                    command_id,
                    ..
                } => (template_id, command_id),
                _ => return Err("allocation may select only fixed catalog templates".into()),
            };
            if selected != (template_id, &context.signal.command_id)
                || &actor.work_id != parent_work_id
                || actor.campaign_id != context.fence.campaign_id
            {
                return Err("allocation catalog selector conflict".into());
            }
        }
        let dynamic = match &request {
            Request::Propose {
                profile_id,
                objective,
                context_refs,
                ..
            } => Some(vec![tachyon_api::agents::Proposal {
                profile_id: profile_id.clone(),
                objective: objective.clone(),
                context_refs: context_refs.clone(),
            }]),
            Request::ProposeGroup { specs, .. } => Some(specs.clone()),
            _ => None,
        };
        let generated_key = match &request {
            Request::Propose { command_id, .. } | Request::ProposeGroup { command_id, .. } => {
                format!(
                    "proposal-{:x}",
                    Sha256::digest(
                        serde_json::to_vec(&(&actor, command_id)).map_err(|e| e.to_string())?
                    )
                )
            }
            _ => String::new(),
        };
        let (template_id, command_id, running, is_group) = match &request {
            Request::Propose { command_id, .. } => (&generated_key, command_id, None, false),
            Request::ProposeGroup {
                command_id,
                max_running,
                ..
            } => (&generated_key, command_id, Some(*max_running), true),
            Request::Spawn {
                template_id,
                command_id,
            } => (template_id, command_id, None, false),
            Request::Group {
                template_id,
                command_id,
                max_running,
            } => (template_id, command_id, *max_running, true),
            _ => return Err("not an admission request".into()),
        };
        let mut catalog = self
            .host_catalog
            .lock()
            .map_err(|_| "host catalog unavailable")?;
        let key =
            serde_json::to_string(&(&actor.campaign_id, template_id)).map_err(|e| e.to_string())?;
        let command_key =
            serde_json::to_string(&(&actor.campaign_id, command_id)).map_err(|e| e.to_string())?;
        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
        let receipts = tx.open_table(RECEIPTS).map_err(|e| e.to_string())?;
        let commands = tx.open_table(COMMANDS).map_err(|e| e.to_string())?;
        if commands
            .get(command_key.as_str())
            .map_err(|e| e.to_string())?
            .is_some_and(|v| v.value() != key)
        {
            return Err("admission command ID conflict".into());
        }
        let old: Option<Receipt> = receipts
            .get(key.as_str())
            .map_err(|e| e.to_string())?
            .map(|v| serde_json::from_slice(v.value()).map_err(|e| e.to_string()))
            .transpose()?;
        let preparations = tx.open_table(PREPARATIONS).map_err(|e| e.to_string())?;
        let pending: Option<Preparation> = preparations
            .get(key.as_str())
            .map_err(|e| e.to_string())?
            .map(|v| serde_json::from_slice(v.value()).map_err(|e| e.to_string()))
            .transpose()?;
        if pending
            .as_ref()
            .is_some_and(|p| p.request != request || p.parent != actor)
        {
            return Err("pending preparation payload conflict".into());
        }
        let mut pending_ids = std::collections::BTreeSet::new();
        for row in preparations.iter().map_err(|e| e.to_string())? {
            let (_, value) = row.map_err(|e| e.to_string())?;
            let p: Preparation =
                serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
            let pending_command = match &p.request {
                Request::Propose { command_id, .. } | Request::ProposeGroup { command_id, .. } => {
                    command_id
                }
                _ => return Err("invalid pending request kind".into()),
            };
            if p.parent.campaign_id == actor.campaign_id
                && pending_command == command_id
                && (p.request != request || p.parent != actor)
            {
                return Err("pending command ID conflict".into());
            }
            for candidate in p.fingerprint["candidates"]
                .as_array()
                .ok_or("invalid pending candidates")?
            {
                pending_ids.insert(
                    candidate["work"]["work_id"]
                        .as_str()
                        .ok_or("invalid pending work")?
                        .to_owned(),
                );
            }
        }
        drop(preparations);
        if old.as_ref().is_some_and(|r| r.request != request) {
            return Err("admission replay payload conflict".into());
        }
        drop(commands);
        drop(receipts);
        drop(tx);
        let mut selected_profiles = Vec::new();
        let template = if let Some(specs) = dynamic {
            if old
                .as_ref()
                .is_some_and(|r| r.policies.len() != specs.len())
            {
                return Err("invalid proposal receipt size".into());
            }
            let mut candidates = Vec::new();
            let mut ceiling = usize::MAX;
            for (index, spec) in specs.iter().enumerate() {
                let (source, profile) = catalog
                    .profiles
                    .iter()
                    .filter_map(|(key, p)| catalog.templates.get(key).map(|t| (t, p)))
                    .find(|(t, p)| {
                        p.profile_id == spec.profile_id
                            && t.parent.campaign_id == actor.campaign_id
                            && (t.parent == actor
                                || catalog.profiles.iter().any(|(id, inherited)| {
                                    inherited.profile_ids.contains(&spec.profile_id)
                                        && catalog.templates.get(id).is_some_and(|owner| {
                                            owner.parent.campaign_id == actor.campaign_id
                                                && owner
                                                    .candidates
                                                    .iter()
                                                    .any(|c| c.work.work_id == actor.work_id)
                                        })
                                }))
                    })
                    .ok_or("unapproved dynamic profile or parent")?;
                if spec.objective.len() > profile.max_objective_bytes
                    || spec.context_refs.len() > profile.max_context_refs
                {
                    return Err("dynamic profile objective/context bound exceeded".into());
                }
                ceiling = ceiling.min(source.max_running);
                let tx = self.database.begin_write().map_err(|e| e.to_string())?;
                let works = tx
                    .open_table(crate::runtime_store::admission::WORK)
                    .map_err(|e| e.to_string())?;
                let mut occupied = std::collections::BTreeSet::new();
                for candidate in &source.candidates {
                    if works
                        .get(candidate.work.work_id.as_str())
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        occupied.insert(candidate.work.work_id.clone());
                    }
                }
                drop(works);
                let mut candidate = if let Some(receipt) = &old {
                    source
                        .candidates
                        .iter()
                        .find(|c| c.work.work_id == receipt.policies[index].work.work_id)
                        .ok_or("receipt outside approved profile slots")?
                        .clone()
                } else if let Some(pending) = &pending {
                    let id = pending.fingerprint["candidates"][index]["work"]["work_id"]
                        .as_str()
                        .ok_or("invalid pending slot")?;
                    source
                        .candidates
                        .iter()
                        .find(|c| c.work.work_id == id)
                        .ok_or("pending slot outside approved profile")?
                        .clone()
                } else {
                    source
                        .candidates
                        .iter()
                        .find(|c| {
                            !candidates
                                .iter()
                                .any(|chosen: &HostCandidate| chosen.work.work_id == c.work.work_id)
                                && !pending_ids.contains(&c.work.work_id)
                                && !occupied.contains(&c.work.work_id)
                        })
                        .ok_or("dynamic lifetime proposal pool exhausted")?
                        .clone()
                };
                drop(tx);
                candidate.admission.objective = spec.objective.clone();
                candidate.verification.objective = spec.objective.clone();
                candidate.work.objective = spec.objective.clone();
                let context = if let Some(receipt) = &old {
                    receipt.policies[index]
                        .work
                        .constraints
                        .as_ref()
                        .ok_or("missing receipt constraints")?
                        .input_context
                        .clone()
                } else if let Some(pending) = &pending {
                    serde_json::from_value(
                        pending.fingerprint["candidates"][index]["work"]["constraints"]
                            ["input_context"]
                            .clone(),
                    )
                    .map_err(|e| e.to_string())?
                } else {
                    let mut context = Vec::new();
                    for resource in &spec.context_refs {
                        let page = self.host_research_context(
                            &actor.campaign_id,
                            &tachyon_api::context::Request::Read {
                                resource: resource.clone(),
                                offset: 0,
                                limit: 1024,
                            },
                            artifacts,
                        )?;
                        if page.resources.len() != 1 || page.resources[0].reference != *resource {
                            return Err("invalid proposal evidence reference".into());
                        }
                        context.extend(page.resources);
                    }
                    context
                };
                if context
                    .iter()
                    .map(|r| &r.reference)
                    .ne(spec.context_refs.iter())
                    || serde_json::to_vec(&context)
                        .map_err(|e| e.to_string())?
                        .len()
                        > 32768
                {
                    return Err("invalid bounded proposal context".into());
                }
                candidate.work.constraints = Some(tachyon_api::types::WorkConstraints {
                    permissions: profile.permissions.clone(),
                    input_context: context,
                });
                candidate.work.context_refs = spec.context_refs.clone();
                selected_profiles.push(profile.clone());
                candidates.push(candidate);
            }
            HostTemplate {
                template_id: template_id.clone(),
                parent: actor.clone(),
                candidates,
                group_id: is_group.then(|| generated_key.clone()),
                max_running: ceiling,
            }
        } else {
            if catalog.profiles.contains_key(template_id) {
                return Err("dynamic slots cannot be selected as fixed templates".into());
            }
            catalog
                .templates
                .get(template_id)
                .ok_or("unapproved template")?
                .clone()
        };
        if template.parent != actor
            || template.group_id.is_some() != is_group
            || running.is_some_and(|n| n > template.max_running)
        {
            return Err("template parent/kind/cap denied".into());
        }
        let mut fingerprint = template.fingerprint();
        if !selected_profiles.is_empty() {
            fingerprint["profiles"] =
                serde_json::to_value(&selected_profiles).map_err(|e| e.to_string())?;
        }
        let proposal_sha256 = (!selected_profiles.is_empty()).then(|| {
            format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&(&request, &fingerprint)).expect("serializable proposal")
                )
            )
        });
        let mut prepared = super::snapshot::Prepared::default();
        if pending
            .as_ref()
            .is_some_and(|p| p.fingerprint != fingerprint || p.proposal_sha256 != proposal_sha256)
        {
            return Err("pending preparation host approval conflict".into());
        }
        if old.is_none() && !selected_profiles.is_empty() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis();
            for candidate in &template.candidates {
                if u128::from(candidate.work.deadline_ms) <= now {
                    return Err("approved work deadline expired".into());
                }
                for admission in [&candidate.admission, &candidate.verification] {
                    if self
                        .campaign_execution(&actor.campaign_id, &admission.work_id)?
                        .is_some()
                        || self
                            .campaign_command_gate(&actor.campaign_id, &admission.work_id)?
                            .is_some()
                    {
                        return Err("execution proof exists; staging recovery denied".into());
                    }
                }
            }
            // Check durable ownership before touching staging. The catalog lock
            // serializes proposals; recheck admissions below after file I/O.
            let tx = self.database.begin_write().map_err(|e| e.to_string())?;
            let parent = Self::admitted_work_in(&tx, &actor.work_id)?;
            Self::admitted_funding_in(&tx, &parent)?;
            for candidate in &template.candidates {
                for admission in [&candidate.admission, &candidate.verification] {
                    if tx
                        .open_table(crate::runtime_store::admission::WORK)
                        .map_err(|e| e.to_string())?
                        .get(admission.work_id.as_str())
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        return Err("staging recovery denied: work already admitted".into());
                    }
                }
            }
            {
                let mut preparations = tx.open_table(PREPARATIONS).map_err(|e| e.to_string())?;
                let preparation = Preparation {
                    request: request.clone(),
                    parent: actor.clone(),
                    fingerprint: fingerprint.clone(),
                    proposal_sha256: proposal_sha256.clone(),
                };
                preparations
                    .insert(
                        key.as_str(),
                        serde_json::to_vec(&preparation)
                            .map_err(|e| e.to_string())?
                            .as_slice(),
                    )
                    .map_err(|e| e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())?;
            for (candidate, profile) in template.candidates.iter().zip(&selected_profiles) {
                prepared.stage_retained(
                    profile,
                    &candidate.workspace,
                    &serde_json::json!({
                        "campaign_id": actor.campaign_id, "parent": actor,
                        "proposal_sha256": proposal_sha256, "work_id": candidate.work.work_id,
                        "request": request, "fingerprint": fingerprint,
                    }),
                    Some(&self.retained),
                )?;
            }
        }
        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
        let mut receipts = tx.open_table(RECEIPTS).map_err(|e| e.to_string())?;
        let mut commands = tx.open_table(COMMANDS).map_err(|e| e.to_string())?;
        if commands
            .get(command_key.as_str())
            .map_err(|e| e.to_string())?
            .is_some_and(|v| v.value() != key)
            || receipts
                .get(key.as_str())
                .map_err(|e| e.to_string())?
                .is_some()
                != old.is_some()
        {
            return Err("admission changed during preparation".into());
        }
        if let Some(context) = allocation {
            let replay = Self::check_allocation_action_in(&tx, context)?;
            if replay != old.is_some() {
                return Err("allocation/catalog receipt conflict".into());
            }
            if !replay {
                Self::allocation_work_scope_in(&tx, context, &actor)?;
            }
        }
        let policies = if let Some(old) = old {
            if old.request != request
                || old.fingerprint != fingerprint
                || old.proposal_sha256 != proposal_sha256
            {
                return Err("admission replay payload conflict".into());
            }
            if old.policies.len() != template.candidates.len() {
                return Err("invalid admission receipt size".into());
            }
            for (p, c) in old.policies.iter().zip(&template.candidates) {
                if p.funding.admission != c.admission
                    || p.verification.admission != c.verification
                    || p.work != c.work
                    || p.model != c.model
                    || p.evaluator_id != c.evaluator_id
                {
                    return Err("invalid admission receipt policy".into());
                }
                for funding in [&p.funding, &p.verification] {
                    let current = Self::admitted_work_in(&tx, &funding.admission.work_id)?;
                    if current.admission != funding.admission
                        || current.dispatch_id != funding.dispatch_id
                    {
                        return Err("invalid admission receipt funding".into());
                    }
                }
            }
            old.policies
        } else {
            #[cfg(test)]
            super::snapshot::checkpoint("admission")?;
            let parent = Self::admitted_work_in(&tx, &actor.work_id)?;
            Self::admitted_funding_in(&tx, &parent)?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis();
            if template
                .candidates
                .iter()
                .any(|c| u128::from(c.work.deadline_ms) <= now)
            {
                return Err("approved work deadline expired".into());
            }
            // Existing work cannot be adopted by a newly approved template.
            for c in &template.candidates {
                for a in [&c.admission, &c.verification] {
                    if tx
                        .open_table(crate::runtime_store::admission::WORK)
                        .map_err(|e| e.to_string())?
                        .get(a.work_id.as_str())
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        return Err("template work already exists".into());
                    }
                }
            }
            if let Some(group_id) = &template.group_id {
                Self::create_campaign_group_in(
                    &tx,
                    GroupSpec {
                        campaign_id: actor.campaign_id.clone(),
                        group_id: group_id.clone(),
                        parent: Self::work_group_in(&tx, &actor.campaign_id, &actor.work_id)?,
                        max_running: running.unwrap_or(template.max_running),
                        work: template
                            .candidates
                            .iter()
                            .map(|c| c.admission.clone())
                            .collect(),
                    },
                    Some(template.max_running),
                )?;
            }
            let mut policies = Vec::new();
            for c in &template.candidates {
                let funding =
                    Self::host_admit_agent_work_in(&tx, c.admission.clone(), Some(actor.clone()))?;
                if template.group_id.is_none() {
                    Self::inherit_work_group_in(
                        &tx,
                        &actor.campaign_id,
                        &actor.work_id,
                        &c.admission.work_id,
                    )?;
                }
                let verification = Self::admit_campaign_work_in(&tx, c.verification.clone())?;
                policies.push(ExecutionPolicy {
                    funding,
                    verification,
                    model: c.model.clone(),
                    work: c.work.clone(),
                    evaluator_id: c.evaluator_id.clone(),
                });
            }
            let receipt = Receipt {
                request: request.clone(),
                fingerprint,
                policies: policies.clone(),
                proposal_sha256,
            };
            receipts
                .insert(
                    key.as_str(),
                    serde_json::to_vec(&receipt)
                        .map_err(|e| e.to_string())?
                        .as_slice(),
                )
                .map_err(|e| e.to_string())?;
            commands
                .insert(command_key.as_str(), key.as_str())
                .map_err(|e| e.to_string())?;
            policies
        };
        drop(commands);
        drop(receipts);
        if let Some(context) = allocation {
            Self::finish_allocation_action_in(&tx, context)?;
        }
        tx.open_table(PREPARATIONS)
            .map_err(|e| e.to_string())?
            .remove(key.as_str())
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        #[cfg(test)]
        super::snapshot::checkpoint("committed")?;
        let work_ids = policies.iter().map(|p| p.work.work_id.clone()).collect();
        for (policy, c) in policies.into_iter().zip(template.candidates) {
            let a = &policy.funding.admission;
            catalog
                .admitted
                .entry((a.campaign_id.clone(), a.work_id.clone(), a.generation))
                .or_insert_with(|| {
                    Arc::new(HostExecution {
                        policy,
                        executable: c.executable,
                        workspace: c.workspace,
                        home: c.home,
                        evaluate: c.evaluate,
                    })
                });
        }
        Ok(Reply::Admitted {
            command_id: command_id.clone(),
            work_ids,
            group_id: template.group_id,
        })
    }
}
