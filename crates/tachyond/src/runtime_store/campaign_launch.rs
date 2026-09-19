//! Native, explicit same-user host entry point. No automatic process recovery.
use super::{
    admission::{Admission, DispatchOutcome},
    campaign_ledger::{Envelope, Pool, Units},
    execution::{Evaluation, ExecutionPhase, ExecutionPolicy},
    model_accounting::ModelBroker,
    RuntimeStore,
};
use redb::{ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};
use tachyon_api::{
    campaign::CampaignManifest,
    types::{ApiRequest, ApiResponse, Campaign, CampaignStatus, LifetimeClass, WorkRequest},
};
use tachyon_model::{
    accounting::{RequestClass, RequestEstimate, RequestReservation, WorkIdentity},
    Model, ModelConfig,
};
use tachyond::{artifact_store::ArtifactStore, verification::CommandEvaluator};
use tokio::sync::watch;

pub(super) const LAUNCHES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("local_campaign_launches_v1");
mod continuation;
mod reconciliation;
mod retention;

#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Launch {
    schema_version: u32,
    pub(super) manifest: CampaignManifest,
    base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    manifest_sha256: Option<String>,
}

impl Launch {
    fn digest(manifest: &CampaignManifest) -> Result<String, String> {
        use sha2::{Digest, Sha256};
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(manifest).map_err(err)?)
        ))
    }

    pub(super) fn validate_digest(&self) -> Result<(), String> {
        if self
            .manifest_sha256
            .as_ref()
            .is_some_and(|digest| Self::digest(&self.manifest).as_ref() != Ok(digest))
        {
            return Err("immutable manifest digest mismatch".into());
        }
        if (self.manifest.children.is_some()
            || self.manifest.web.is_some()
            || self.manifest.compute.is_some()
            || self.manifest.oversight.is_some()
            || self.manifest.retained_storage_bytes.is_some())
            && self.manifest_sha256.is_none()
        {
            return Err("child/compute/storage launch requires manifest digest".into());
        }
        Ok(())
    }
}

struct Active {
    id: String,
    oversight: bool,
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

/// Bounded independently owned campaigns sharing host admission capacity.
/// The thread owns the Tokio runtime and execution future until cleanup finishes.
pub(crate) struct CampaignService {
    store: Arc<RuntimeStore>,
    root: PathBuf,
    active: Mutex<BTreeMap<String, Active>>,
    stopping: std::sync::atomic::AtomicBool,
}

impl CampaignService {
    pub(crate) fn monitor_capacity(&self) -> Result<tachyon_api::monitor::Capacity, String> {
        use tachyon_api::monitor::*;
        let active = self
            .active
            .try_lock()
            .map_err(|_| "campaign registry unavailable")?;
        Ok(Capacity {
            resource: CapacityResource::Campaign,
            sampled_at_ms: super::monitor::now_ms(),
            limit: Decimal(self.store.host_capacity.limits.max_campaigns as u128),
            held: Decimal(active.len() as u128),
            queued: Decimal(0),
            unresolved: Observed::Unknown,
        })
    }
}

#[cfg(test)]
#[test]
fn monitor_campaign_contention_preserves_stale_and_allows_shutdown() {
    use tachyon_api::monitor::*;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let campaigns = Arc::new(CampaignService {
        store: store.clone(),
        root: dir.path().into(),
        active: Mutex::new(BTreeMap::new()),
        stopping: false.into(),
    });
    let registry = Arc::new(Mutex::new(crate::Registry {
        campaigns: Some(campaigns.clone()),
        ..Default::default()
    }));
    let shutdown = registry.lock().unwrap().service_shutdown.clone();
    let (monitor, sampler) =
        crate::monitor::Monitor::start(Arc::downgrade(&registry), store, shutdown);
    let lease = monitor
        .acquire(
            MonitorQuery {
                scope: MonitorScope::Host,
                after: None,
                limit: MAX_PAGE,
            },
            true,
        )
        .unwrap();
    let first = lease.latest(None).unwrap().unwrap();
    assert!(first.payload.is_some() && first.stale.is_none());
    let guard = campaigns.active.lock().unwrap();
    let stale = (0..3).find_map(|_| lease.latest(Some(&first.version)).unwrap());
    monitor.stop();
    let (done, ended) = std::sync::mpsc::channel();
    let joiner = std::thread::spawn(move || {
        sampler.join().unwrap();
        done.send(()).unwrap();
    });
    let stopped = ended.recv_timeout(Duration::from_secs(1));
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = guard;
        panic!("poison campaign monitor fixture");
    }));
    joiner.join().unwrap();
    let unavailable = campaigns.monitor_capacity().is_err();
    campaigns.active.clear_poison();
    stopped.unwrap();
    let stale = stale.expect("campaign source must not block the sampler");
    assert_eq!(stale.payload, first.payload);
    assert_eq!(stale.stale, Some(MonitorError::Unavailable));
    assert!(unavailable);
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn validate_endpoint(base_url: &str) -> Result<(), String> {
    let url = url::Url::parse(base_url).map_err(|_| "invalid host provider endpoint")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "host provider endpoint must be HTTP(S), without credentials, query or fragment".into(),
        );
    }
    Ok(())
}

impl CampaignService {
    pub(crate) fn new(store: Arc<RuntimeStore>, root: PathBuf) -> Result<Self, String> {
        // Complete the campaign artifact census before any launch can admit writes.
        {
            let tx = store.database.begin_read().map_err(err)?;
            let table = tx.open_table(LAUNCHES).map_err(err)?;
            for row in table.iter().map_err(err)? {
                let (key, value) = row.map_err(err)?;
                let launch: Launch = serde_json::from_slice(value.value()).map_err(err)?;
                launch.validate_digest()?;
                store
                    .retained
                    .configure(key.value(), launch.manifest.retained_storage_bytes)?;
            }
        }
        let campaigns = root.join("campaigns");
        if campaigns.try_exists().map_err(err)? {
            for entry in std::fs::read_dir(campaigns).map_err(err)? {
                let entry = entry.map_err(err)?;
                let path = entry.path().join("artifacts");
                if path.try_exists().map_err(err)? {
                    let id = entry
                        .file_name()
                        .into_string()
                        .map_err(|_| "invalid campaign directory")?;
                    ArtifactStore::open_retained(&path, store.retained.clone(), &id)?;
                }
            }
        }
        let mut after: Option<String> = None;
        loop {
            use std::ops::Bound::{Excluded, Unbounded};
            let tx = store.database.begin_write().map_err(err)?;
            let table = tx.open_table(LAUNCHES).map_err(err)?;
            let mut scanned = 0;
            let mut next = after.clone();
            // Retained history can outgrow the live campaign cap. Bound each
            // startup writer slice instead of holding one transaction over it all.
            for row in table
                .range::<&str>((after.as_deref().map_or(Unbounded, Excluded), Unbounded))
                .map_err(err)?
                .take(64)
            {
                let (key, value) = row.map_err(err)?;
                let launch: Launch = serde_json::from_slice(value.value()).map_err(err)?;
                launch.validate_digest()?;
                if launch.schema_version != 1 || launch.manifest.campaign_id != key.value() {
                    return Err("invalid launch record".into());
                }
                let ApiResponse::Campaign { campaign } =
                    store.research_request(&ApiRequest::CampaignGet {
                        id: key.value().into(),
                    })?
                else {
                    unreachable!()
                };
                if matches!(
                    campaign.status,
                    CampaignStatus::Running | CampaignStatus::Cancelling
                ) {
                    RuntimeStore::set_campaign_status_in(
                        &tx,
                        key.value(),
                        CampaignStatus::Interrupted,
                    )?;
                }
                for work in std::iter::once(format!("{}-root", key.value()))
                    .chain(launch.manifest.child_work_ids())
                {
                    if let Some(record) = store.campaign_execution(key.value(), &work)? {
                        if matches!(
                            record.phase,
                            ExecutionPhase::ExecutingUnknown | ExecutionPhase::ReviewingUnknown
                        ) {
                            let identity = super::host_capacity::identity(
                                key.value(),
                                &work,
                                &record.policy.model.identity.attempt_id,
                                record.policy.work.generation,
                                &record.policy.funding.dispatch_id,
                            );
                            store.host_capacity.resident.reserve_unknown(&identity)?;
                            store.host_capacity.execution.reserve_unknown(&identity)?;
                        }
                    }
                }
                next = Some(key.value().to_owned());
                scanned += 1;
            }
            drop(table);
            tx.commit().map_err(err)?;
            if scanned < 64 {
                break;
            }
            after = next;
        }
        Ok(Self {
            store,
            root,
            active: Mutex::new(BTreeMap::new()),
            stopping: false.into(),
        })
    }

    pub(crate) fn run(
        &self,
        manifest: &CampaignManifest,
        authorized: bool,
    ) -> Result<ApiResponse, String> {
        self.activate(manifest, authorized, false)
    }

    pub(crate) fn request_assessment(
        &self,
        id: &str,
        command: &str,
        authorized: bool,
    ) -> Result<ApiResponse, String> {
        let active = self
            .active
            .lock()
            .map_err(|_| "campaign registry unavailable")?;
        let live = active
            .get(id)
            .is_some_and(|a| a.oversight && !a.task.is_finished());
        drop(active);
        if !live && !self.store.assessment_request_known(id, command)? {
            return Err("campaign has no active oversight owner".into());
        }
        self.store
            .request_campaign_assessment(id, command, authorized)
    }

    pub(crate) fn resume(&self, id: &str, authorized: bool) -> Result<ApiResponse, String> {
        if !authorized {
            return Err("--unisolated-development authorization required".into());
        }
        let tx = self.store.database.begin_read().map_err(err)?;
        let table = tx.open_table(LAUNCHES).map_err(err)?;
        let launch: Launch = serde_json::from_slice(
            table
                .get(id)
                .map_err(err)?
                .ok_or("no authorized launch")?
                .value(),
        )
        .map_err(err)?;
        launch.validate_digest()?;
        drop(table);
        drop(tx);
        self.activate(&launch.manifest, authorized, true)
    }

    pub(crate) fn inspect(&self, id: &str) -> Result<ApiResponse, String> {
        self.store.poll_human_acceptance(id, false)?;
        let tx = self.store.database.begin_read().map_err(err)?;
        let table = tx.open_table(LAUNCHES).map_err(err)?;
        let launch: Option<Launch> = table
            .get(id)
            .map_err(err)?
            .map(|row| serde_json::from_slice(row.value()).map_err(err))
            .transpose()?;
        if let Some(launch) = &launch {
            launch.validate_digest()?;
        }
        drop(table);
        drop(tx);
        let ApiResponse::Campaign { campaign } = self
            .store
            .research_request(&ApiRequest::CampaignGet { id: id.into() })?
        else {
            unreachable!()
        };
        let mut diagnostics = vec!["No jobs started. Process/model outcomes are unknown unless independently reconciled; durable leases are not execution proof.".into()];
        diagnostics.push(format!(
            "retention: {}",
            serde_json::to_string(&self.store.retention_get(id)?).map_err(err)?
        ));
        diagnostics.push(format!(
            "storage_inventory: {}",
            self.storage_inventory(id)?
        ));
        if launch.is_some() {
            diagnostics.extend(self.reconciliation_inspection(id)?);
        }
        if let Some(gate) = self
            .store
            .campaign_command_gate(id, &format!("{id}-root"))?
        {
            if let Some(diagnostic) = gate.human_diagnostic {
                diagnostics.push(diagnostic);
            }
        }
        for profile in launch.iter().flat_map(|launch| {
            launch
                .manifest
                .children
                .iter()
                .flat_map(|c| &c.dynamic)
                .flat_map(|d| &d.profiles)
        }) {
            super::scheduler::snapshot::validate_roots(
                profile,
                &self.root.canonicalize().map_err(err)?,
            )?;
            for slot in profile.slots(id).specs {
                let work_id = slot.work_id.as_ref().ok_or("missing profile slot")?;
                if self.store.admitted_work(id, work_id).is_ok() {
                    diagnostics.push(format!("{work_id}: committed admission; directory unchanged, execution not replayable"));
                    continue;
                }
                for name in [
                    work_id.clone(),
                    format!(".proposal-{work_id}"),
                    format!(".quarantine-{work_id}"),
                ] {
                    let path = profile.managed_root.join(&name);
                    match std::fs::symlink_metadata(&path) {
                        Ok(meta) => diagnostics.push(format!(
                            "{}: {} (not verified; retained)",
                            path.display(),
                            if meta.file_type().is_symlink() {
                                "symlink, recovery denied"
                            } else {
                                "staging/retained entry"
                            }
                        )),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                        Err(e) => return Err(err(e)),
                    }
                }
            }
        }
        Ok(ApiResponse::CampaignInspection {
            campaign,
            diagnostics,
        })
    }

    pub(crate) fn retention(&self, id: &str, archived: bool) -> Result<ApiResponse, String> {
        let active = self
            .active
            .lock()
            .map_err(|_| "campaign registry unavailable")?;
        if self.stopping.load(std::sync::atomic::Ordering::Acquire)
            || active.get(id).is_some_and(|a| !a.task.is_finished())
        {
            return Err("retention changes require an idle campaign".into());
        }
        self.store.retention_set(id, archived)
    }

    /// Explicit host approval of stored policy, without scheduling any execution.
    pub(crate) fn recover(&self, id: &str, authorized: bool) -> Result<ApiResponse, String> {
        if !authorized {
            return Err("--unisolated-development authorization required".into());
        }
        let active = self
            .active
            .lock()
            .map_err(|_| "campaign registry unavailable")?;
        if self.stopping.load(std::sync::atomic::Ordering::Acquire)
            || active.get(id).is_some_and(|a| !a.task.is_finished())
        {
            return Err("recovery requires an idle campaign".into());
        }
        let tx = self.store.database.begin_read().map_err(err)?;
        let table = tx.open_table(LAUNCHES).map_err(err)?;
        let launch: Launch = serde_json::from_slice(
            table
                .get(id)
                .map_err(err)?
                .ok_or("no authorized launch")?
                .value(),
        )
        .map_err(err)?;
        launch.validate_digest()?;
        drop(table);
        drop(tx);
        let m = &launch.manifest;
        m.validate(now())?;
        let ApiResponse::Campaign { campaign } = self
            .store
            .research_request(&ApiRequest::CampaignGet { id: id.into() })?
        else {
            unreachable!()
        };
        if campaign.objective != m.objective {
            return Err("stored objective approval conflict".into());
        }
        let gate = self
            .store
            .campaign_command_gate(id, &format!("{id}-root"))?
            .ok_or("no original root policy; unknown launch remains untouched")?;
        let policy = gate.original_policy.unwrap_or(gate.policy);
        let host = tachyon_util::config::Config::try_load_from(
            &tachyon_util::config::Config::default_path(),
        )
        .map_err(err)?;
        if host.provider_base_url() != launch.base_url
            || host
                .provider
                .as_ref()
                .is_some_and(|p| p.name != "openrouter")
        {
            return Err("stored host provider policy conflict".into());
        }
        let children = m.children.as_ref().ok_or("no child policy to recover")?;
        for profile in children.dynamic.iter().flat_map(|d| &d.profiles) {
            super::scheduler::snapshot::validate_roots(
                profile,
                &self.root.canonicalize().map_err(err)?,
            )?;
        }
        // This broker is descriptor-only: no credentials and no scheduling loop.
        let model = Model::new(ModelConfig {
            base_url: launch.base_url.clone(),
            api_key: String::new(),
            model: m.model.clone(),
            temperature: 0.0,
            max_completion_tokens: Some(m.output_tokens),
            context_length: None,
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: None,
            debug: false,
            debug_log: None,
        });
        let broker = Arc::new(ModelBroker::new(self.store.clone(), model));
        let root = self.root.join("campaigns").join(id);
        let config = CommandEvaluator {
            acceptance_mode: m.evaluator.acceptance_mode,
            result_contract: m.evaluator.result_contract,
            metrics: m.evaluator.metrics.clone(),
            allow_extra_metrics: m.evaluator.allow_extra_metrics,
            stage: m.evaluator.stage,
            argv: m.evaluator.argv.clone(),
            cwd: ".".into(),
            timeout_ms: m.evaluator.timeout_ms,
            output_bytes: m.evaluator.output_bytes,
            input_bytes: m.evaluator.input_bytes,
            max_attempts: 1,
            max_total_command_ms: m.evaluator.timeout_ms,
        };
        let scheduler =
            super::scheduler::HostScheduler::command_children(broker, children.max_resident)?;
        for profile in children.dynamic.iter().flat_map(|d| &d.profiles) {
            let template = child_template(
                m,
                &profile.slots(id),
                &policy,
                None,
                &root.join("verification"),
                &config,
            )?;
            scheduler.approve_profile(template, profile.clone())?;
        }
        scheduler.restore_proposals()?;
        drop(scheduler);
        drop(active);
        self.inspect(id)
    }

    fn activate(
        &self,
        manifest: &CampaignManifest,
        authorized: bool,
        resume: bool,
    ) -> Result<ApiResponse, String> {
        self.activate_request(manifest, authorized, resume, None)
    }

    fn activate_request(
        &self,
        manifest: &CampaignManifest,
        authorized: bool,
        resume: bool,
        continuation: Option<&tachyon_api::continuation::ContinuationRequest>,
    ) -> Result<ApiResponse, String> {
        if !authorized {
            return Err("--unisolated-development authorization required; native execution is NOT a sandbox".into());
        }
        manifest.validate(now())?;
        // Reject aliases and broad roots before credentials, writes or process creation.
        for path in [
            manifest.workspace.as_path(),
            manifest.home.as_path(),
            manifest.executable.as_path(),
        ]
        .into_iter()
        .chain(
            manifest
                .evaluator
                .argv
                .first()
                .map(|s| std::path::Path::new(s)),
        ) {
            if path.canonicalize().map_err(err)? != *path {
                return Err("paths must be canonical, without symlinks".into());
            }
        }
        let child_paths: Vec<_> = manifest
            .children
            .iter()
            .flat_map(|c| &c.templates)
            .flat_map(|t| &t.specs)
            .flat_map(|s| [&s.workspace, &s.home])
            .collect();
        for path in &child_paths {
            if path.canonicalize().map_err(err)?.as_path() != path.as_path() || !path.is_dir() {
                return Err("child paths must be canonical existing dedicated directories".into());
            }
        }
        if !manifest.workspace.is_dir() || !manifest.home.is_dir() || !manifest.executable.is_file()
        {
            return Err("dedicated existing directories and Ghost executable required".into());
        }
        let protected = self.root.canonicalize().map_err(err)?;
        for p in manifest
            .children
            .iter()
            .flat_map(|c| &c.dynamic)
            .flat_map(|d| &d.profiles)
        {
            super::scheduler::snapshot::validate_roots(p, &protected)?;
        }
        for path in [&manifest.workspace, &manifest.home]
            .into_iter()
            .chain(child_paths)
        {
            if path.starts_with(&protected) || protected.starts_with(path) {
                return Err("workspace/home must be disjoint from daemon storage".into());
            }
            if std::env::var_os("HOME").is_some_and(|h| PathBuf::from(h).starts_with(path)) {
                return Err("workspace/home must not be the user's home or an ancestor".into());
            }
        }
        let ApiResponse::Campaign { campaign } =
            self.store.research_request(&ApiRequest::CampaignGet {
                id: manifest.campaign_id.clone(),
            })?
        else {
            unreachable!()
        };
        if (!resume && campaign.status != CampaignStatus::Draft)
            || campaign.objective != manifest.objective
        {
            return Err("requires an inert Draft campaign with the exact objective; executions are never replayed".into());
        }
        if resume && continuation.is_none() {
            self.ready_to_resume(&manifest.campaign_id)?;
        }
        let config = tachyon_util::config::Config::try_load_from(
            &tachyon_util::config::Config::default_path(),
        )
        .map_err(err)?;
        if config
            .provider
            .as_ref()
            .is_some_and(|p| p.name != "openrouter")
        {
            return Err(
                "campaigns require the host OpenRouter provider; no provider fallback".into(),
            );
        }
        let base_url = config.provider_base_url();
        validate_endpoint(&base_url)?;
        let api_key = config
            .resolve_key("openrouter")
            .ok_or("host provider credentials unavailable")?;
        let model = Model::new(ModelConfig {
            base_url: base_url.clone(),
            api_key,
            model: manifest.model.clone(),
            temperature: 0.0,
            max_completion_tokens: Some(manifest.output_tokens),
            context_length: None,
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: None,
            debug: false,
            debug_log: None,
        });
        let store = self.store.clone();
        let root = self.root.clone();
        let prepared = continuation.is_some();
        self.launch_request(
            manifest,
            base_url,
            resume,
            continuation,
            move |launch, signal| execute_prepared(store, root, launch, model, signal, prepared),
        )
    }

    fn ready_to_resume(&self, id: &str) -> Result<(), String> {
        let record = self
            .store
            .campaign_execution(id, &format!("{id}-root"))?
            .ok_or("no ready execution; unknown launches are never replayed")?;
        if record.settled
            || !matches!(
                record.phase,
                ExecutionPhase::EvidenceReady | ExecutionPhase::AwaitingVerification
            )
        {
            return Err("only evidence_ready or awaiting_verification may resume; unknown execution/review is never replayed".into());
        }
        Ok(())
    }

    #[cfg(test)]
    fn launch<F, Fut>(
        &self,
        manifest: &CampaignManifest,
        base_url: String,
        resume: bool,
        execute: F,
    ) -> Result<ApiResponse, String>
    where
        F: FnOnce(Launch, watch::Sender<bool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<ExecutionPhase, String>>,
    {
        self.launch_request(manifest, base_url, resume, None, execute)
    }

    fn launch_request<F, Fut>(
        &self,
        manifest: &CampaignManifest,
        base_url: String,
        resume: bool,
        continuation: Option<&tachyon_api::continuation::ContinuationRequest>,
        execute: F,
    ) -> Result<ApiResponse, String>
    where
        F: FnOnce(Launch, watch::Sender<bool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<ExecutionPhase, String>>,
    {
        manifest.validate(now())?;
        validate_endpoint(&base_url)?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| "campaign registry unavailable")?;
        if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
            return Err("daemon is stopping".into());
        }
        active.retain(|_, entry| !entry.task.is_finished());
        {
            let tx = self.store.database.begin_write().map_err(err)?;
            RuntimeStore::require_unarchived_in(&tx, &manifest.campaign_id)?;
        }
        if let Some(request) = continuation {
            if self.continuation_claimed(request)? {
                return self.store.research_request(&ApiRequest::CampaignGet {
                    id: manifest.campaign_id.clone(),
                });
            }
        }
        if active.contains_key(&manifest.campaign_id) {
            return Err("this local campaign is already active".into());
        }
        if active.len() >= self.store.host_capacity.limits.max_campaigns {
            return Err("host campaign capacity exhausted".into());
        }
        if resume && continuation.is_none() {
            self.ready_to_resume(&manifest.campaign_id)?;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(err)?;
        let launch = Launch {
            schema_version: 1,
            manifest: manifest.clone(),
            base_url,
            manifest_sha256: Some(Launch::digest(manifest)?),
        };
        let tx = self.store.database.begin_write().map_err(err)?;
        if let Some(request) = continuation {
            self.claim_continuation(&tx, manifest, request)?;
        }
        {
            let mut table = tx.open_table(LAUNCHES).map_err(err)?;
            let existing: Option<Launch> = table
                .get(manifest.campaign_id.as_str())
                .map_err(err)?
                .map(|v| serde_json::from_slice(v.value()).map_err(err))
                .transpose()?;
            if let Some(existing) = &existing {
                existing.validate_digest()?;
            }
            if (resume
                && !existing.as_ref().is_some_and(|old| {
                    old.schema_version == launch.schema_version
                        && old.manifest == launch.manifest
                        && old.base_url == launch.base_url
                }))
                || (!resume && existing.is_some())
            {
                return Err("immutable launch conflict; no policy edits or replay".into());
            }
            table
                .insert(
                    manifest.campaign_id.as_str(),
                    serde_json::to_vec(&launch).map_err(err)?.as_slice(),
                )
                .map_err(err)?;
        }
        self.store.retained.configure_in(
            &tx,
            &manifest.campaign_id,
            manifest.retained_storage_bytes,
        )?;
        super::campaign_oversight::initialize_campaign_in(&tx, manifest)?;
        let campaign = RuntimeStore::set_campaign_status_in(
            &tx,
            &manifest.campaign_id,
            CampaignStatus::Running,
        )?;
        tx.commit().map_err(err)?;
        let store = self.store.clone();
        let id = manifest.campaign_id.clone();
        let (cancel, _) = watch::channel(false);
        let signal = cancel.clone();
        let task = std::thread::Builder::new()
            .name("local-campaign".into())
            .spawn(move || {
                // Catch both callback construction and polling panics so the durable
                // handle cannot remain Running after its owned thread has exited.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime.block_on(execute(launch, signal.clone()))
                }));
                if let Ok(Err(error)) = &result {
                    eprintln!("tachyond: local campaign {id}: {error}");
                }
                let status = if *signal.borrow() && matches!(&result, Ok(Ok(_))) {
                    CampaignStatus::Cancelled
                } else {
                    match result {
                        Ok(Ok(ExecutionPhase::Reviewed(Evaluation::Accepted))) => {
                            CampaignStatus::Accepted
                        }
                        Ok(Ok(ExecutionPhase::Reviewed(Evaluation::AcceptedHuman))) => {
                            CampaignStatus::AcceptedHuman
                        }
                        Ok(Ok(ExecutionPhase::AwaitingAcceptance)) => {
                            CampaignStatus::AwaitingAcceptance
                        }
                        Ok(Ok(ExecutionPhase::Reviewed(Evaluation::Rejected))) => {
                            CampaignStatus::Rejected
                        }
                        _ => CampaignStatus::Unverified,
                    }
                };
                if let Err(e) = store.local_campaign_status(&id, status) {
                    eprintln!("tachyond: campaign status write failed: {e}");
                }
            })
            .map_err(|e| {
                let _ = self
                    .store
                    .local_campaign_status(&manifest.campaign_id, CampaignStatus::Interrupted);
                err(e)
            })?;
        active.insert(
            manifest.campaign_id.clone(),
            Active {
                id: manifest.campaign_id.clone(),
                oversight: manifest.oversight.is_some() && !resume,
                cancel,
                task,
            },
        );
        Ok(ApiResponse::Campaign { campaign })
    }

    pub(crate) fn cancel(&self, id: &str) -> Result<ApiResponse, String> {
        let active = self
            .active
            .lock()
            .map_err(|_| "campaign registry unavailable")?;
        let entry = active.get(id).filter(|a| !a.task.is_finished());
        let Some(entry) = entry else {
            if self
                .store
                .campaign_execution(id, &format!("{id}-root"))?
                .is_some_and(|r| r.phase == ExecutionPhase::AwaitingAcceptance)
            {
                self.store.poll_human_acceptance(id, true)?;
                return self
                    .store
                    .research_request(&ApiRequest::CampaignGet { id: id.into() });
            }
            return Err("campaign is not active in this daemon".into());
        };
        let campaign = self
            .store
            .local_campaign_status(id, CampaignStatus::Cancelling)?;
        if campaign.status != CampaignStatus::Cancelling {
            return Ok(ApiResponse::Campaign { campaign });
        }
        entry.cancel.send_replace(true);
        self.cancel_admitted(id)?;
        Ok(ApiResponse::Campaign { campaign })
    }

    pub(crate) fn progress(&self, id: &str) -> Result<ApiResponse, String> {
        self.store.poll_human_acceptance(id, false)?;
        let ApiResponse::Campaign { campaign } = self
            .store
            .research_request(&ApiRequest::CampaignGet { id: id.into() })?
        else {
            unreachable!()
        };
        let mut activity = tachyon_api::types::CampaignActivity::default();
        activity.host_resident_waiting = self.store.host_capacity.resident.waiting(id)?;
        activity.host_execution_waiting = self.store.host_capacity.execution.waiting(id)?;
        activity.host_model_waiting = self.store.host_capacity.model.waiting(id)?;
        activity.owned = self
            .active
            .lock()
            .map_err(|_| "campaign registry unavailable")?
            .get(id)
            .is_some_and(|a| !a.task.is_finished());
        let tx = self.store.database.begin_read().map_err(err)?;
        let table = tx.open_table(LAUNCHES).map_err(err)?;
        if let Some(row) = table.get(id).map_err(err)? {
            let launch: Launch = serde_json::from_slice(row.value()).map_err(err)?;
            launch.validate_digest()?;
            let mut ids = vec![format!("{id}-root"), format!("{id}-verification")];
            for work_id in launch.manifest.child_work_ids() {
                ids.extend([work_id.clone(), format!("{work_id}-verification")]);
            }
            for work in ids {
                if let Some(status) = self.store.local_work_status(id, &work)? {
                    activity.admitted += 1;
                    activity.queued +=
                        usize::from(status.work.state == super::admission::DispatchState::Admitted);
                    activity.active += usize::from(status.active);
                    activity.waiting +=
                        usize::from(status.wait.is_some() && !status.active && !status.terminal);
                    activity.terminal += usize::from(status.terminal);
                }
            }
        }
        if campaign.status == CampaignStatus::AwaitingAcceptance {
            activity.waiting = activity.admitted.saturating_sub(activity.terminal);
        }
        Ok(ApiResponse::CampaignProgress { campaign, activity })
    }

    fn cancel_admitted(&self, id: &str) -> Result<(), String> {
        let tx = self.store.database.begin_read().map_err(err)?;
        let table = tx.open_table(LAUNCHES).map_err(err)?;
        let launch: Launch =
            serde_json::from_slice(table.get(id).map_err(err)?.ok_or("missing launch")?.value())
                .map_err(err)?;
        launch.validate_digest()?;
        let mut works = vec![format!("{id}-root"), format!("{id}-verification")];
        // Root-only cancellation is owned by the command loop. Its durable
        // Cancelling state and root signal fence execution; transferred verifier
        // funding cannot be cancelled as though it were an unspent reservation.
        if launch.manifest.children.is_none() {
            return Ok(());
        }
        for work_id in launch.manifest.child_work_ids() {
            works.extend([work_id.clone(), format!("{work_id}-verification")]);
        }
        for work in works {
            if self
                .store
                .local_work_status(id, &work)?
                .is_some_and(|s| !s.terminal)
            {
                self.store.host_cancel_work(id, &work, 1)?;
            }
        }
        Ok(())
    }

    pub(crate) fn shutdown(&self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::Release);
        let entries = std::mem::take(&mut *self.active.lock().unwrap());
        for active in entries.values() {
            if !active.task.is_finished() {
                let _ = self
                    .store
                    .local_campaign_status(&active.id, CampaignStatus::Cancelling);
                active.cancel.send_replace(true);
                if let Err(error) = self.cancel_admitted(&active.id) {
                    eprintln!("tachyond: campaign cancellation: {error}");
                }
            }
        }
        for (_, active) in entries {
            let _ = active.task.join();
        }
    }
}

impl Drop for CampaignService {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl RuntimeStore {
    fn local_work_status(
        &self,
        campaign: &str,
        work: &str,
    ) -> Result<Option<super::groups::GroupWorkStatus>, String> {
        let tx = self.database.begin_read().map_err(err)?;
        let table = tx.open_table(super::admission::WORK).map_err(err)?;
        if table.get(work).map_err(err)?.is_none() {
            return Ok(None);
        }
        self.admitted_work(campaign, work)?;
        self.campaign_work_status(campaign, work).map(Some)
    }

    fn local_campaign_status(&self, id: &str, status: CampaignStatus) -> Result<Campaign, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let ApiResponse::Campaign { campaign: current } =
            self.research_request(&ApiRequest::CampaignGet { id: id.into() })?
        else {
            unreachable!()
        };
        // An owner finishing after an operator decision must not overwrite the
        // committed attestation (including a concurrent daemon shutdown signal).
        if matches!(
            current.status,
            CampaignStatus::AcceptedHuman | CampaignStatus::Rejected
        ) && self
            .campaign_command_gate(id, &format!("{id}-root"))?
            .is_some_and(|gate| gate.human_receipt.is_some())
        {
            return Ok(current);
        }
        if status == CampaignStatus::Cancelling
            && !matches!(
                current.status,
                CampaignStatus::Running | CampaignStatus::AwaitingAcceptance
            )
        {
            return Ok(current);
        }
        let status = if current.status == CampaignStatus::Cancelling
            && matches!(
                status,
                CampaignStatus::Accepted | CampaignStatus::AcceptedHuman | CampaignStatus::Rejected
            ) {
            CampaignStatus::Cancelled
        } else {
            status
        };
        let campaign = Self::set_campaign_status_in(&tx, id, status)?;
        tx.commit().map_err(err)?;
        Ok(campaign)
    }
}

fn child_template(
    m: &CampaignManifest,
    t: &tachyon_api::campaign::CampaignTemplate,
    policy: &ExecutionPolicy,
    artifacts: Option<Arc<ArtifactStore>>,
    staging: &std::path::Path,
    config: &CommandEvaluator,
) -> Result<super::scheduler::HostTemplate, String> {
    use super::scheduler::{HostCandidate, HostTemplate};
    let children = m.children.as_ref().ok_or("missing child policy")?;
    let profile = children
        .dynamic
        .iter()
        .flat_map(|d| &d.profiles)
        .find(|p| p.template_id(&m.campaign_id) == t.template_id);
    let mut candidates = Vec::new();
    for (index, s) in t.specs.iter().enumerate() {
        let mut config = config.clone();
        config.max_attempts = s.evaluator.as_ref().map_or(1, |e| e.max_attempts);
        config.max_total_command_ms = s
            .evaluator
            .as_ref()
            .map_or(config.timeout_ms, |e| e.max_total_command_ms);
        let evaluator_id = format!("command:{}", config.config_hash()?);
        let work_id = s.resolved_id(&m.campaign_id, &t.template_id, index);
        let admission = Admission {
            campaign_id: m.campaign_id.clone(),
            work_id: work_id.clone(),
            objective: s.objective.clone(),
            generation: 1,
            instruction_revision: 1,
            pool: Pool::Work,
            upper_bound: Units {
                tokens: s.work_tokens,
                cost_micro_usd: s.work_cost_micro_usd,
            },
        };
        let verification = Admission {
            work_id: format!("{work_id}-verification"),
            pool: Pool::Verification,
            upper_bound: Units {
                tokens: s.verification_tokens,
                cost_micro_usd: s.verification_cost_micro_usd,
            },
            ..admission.clone()
        };
        let mut model = policy.model.clone();
        model.identity.work_id = work_id.clone();
        model.identity.attempt_id = format!("{work_id}-attempt");
        candidates.push(HostCandidate {
            admission,
            verification,
            model,
            work: WorkRequest {
                context_refs: vec![],
                constraints: profile.map(|p| tachyon_api::types::WorkConstraints {
                    permissions: p.permissions.clone(),
                    input_context: Vec::new(),
                }),
                work_id,
                objective: s.objective.clone(),
                ..policy.work.clone()
            },
            evaluator_id: evaluator_id.clone(),
            executable: m.executable.clone(),
            workspace: s.workspace.clone(),
            home: s.home.clone(),
            evaluate: match &artifacts {
                Some(artifacts) => super::scheduler::Evaluator::Command {
                    artifacts: artifacts.clone(),
                    staging: staging.to_owned(),
                    config: config.clone(),
                },
                None => super::scheduler::Evaluator::CommandDescriptor {
                    staging: staging.to_owned(),
                    config: config.clone(),
                },
            },
        });
    }
    Ok(HostTemplate {
        template_id: t.template_id.clone(),
        group_id: t.group_id.clone(),
        max_running: (if profile.is_some() {
            children.max_running
        } else {
            t.max_running
        })
        .min(m.allocation.as_ref().map_or(usize::MAX, |a| a.max_running)),
        parent: super::coordination::WorkAddress {
            campaign_id: m.campaign_id.clone(),
            work_id: format!("{}-root", m.campaign_id),
        },
        candidates,
    })
}

#[cfg(test)]
pub(in crate::runtime_store) async fn execute(
    store: Arc<RuntimeStore>,
    storage_root: PathBuf,
    launch: Launch,
    model: Model,
    cancel: watch::Sender<bool>,
) -> Result<ExecutionPhase, String> {
    execute_prepared(store, storage_root, launch, model, cancel, false).await
}

async fn execute_prepared(
    store: Arc<RuntimeStore>,
    storage_root: PathBuf,
    launch: Launch,
    model: Model,
    cancel: watch::Sender<bool>,
    prepared: bool,
) -> Result<ExecutionPhase, String> {
    let m = launch.manifest;
    store
        .retained
        .configure(&m.campaign_id, m.retained_storage_bytes)?;
    for p in m
        .children
        .iter()
        .flat_map(|c| &c.dynamic)
        .flat_map(|d| &d.profiles)
    {
        super::scheduler::snapshot::validate_roots(p, &storage_root.canonicalize().map_err(err)?)?;
    }
    let child_work_ids = m.child_work_ids();
    let c = m.campaign_id.clone();
    let work = format!("{c}-root");
    let verifier = format!("{c}-verification");
    // Human review has no command claim: its protected verifier is registered
    // up front, as before, and both leases are released before operator input.
    let human = m.evaluator.acceptance_mode == Some(tachyon_api::campaign::AcceptanceMode::Human);
    let config = CommandEvaluator {
        acceptance_mode: m.evaluator.acceptance_mode,
        result_contract: m.evaluator.result_contract,
        metrics: m.evaluator.metrics.clone(),
        allow_extra_metrics: m.evaluator.allow_extra_metrics,
        stage: m.evaluator.stage,
        argv: m.evaluator.argv.clone(),
        cwd: ".".into(),
        timeout_ms: m.evaluator.timeout_ms,
        output_bytes: m.evaluator.output_bytes,
        input_bytes: m.evaluator.input_bytes,
        max_attempts: m.evaluator.max_attempts,
        max_total_command_ms: m.evaluator.max_total_command_ms,
    };
    let evaluator_id = format!("command:{}", config.config_hash()?);
    let mut work_units = Units {
        tokens: m.work_tokens,
        cost_micro_usd: m.work_cost_micro_usd,
    };
    let mut verification_units = Units {
        tokens: m.verification_tokens,
        cost_micro_usd: m.verification_cost_micro_usd,
    };
    let envelope = Envelope {
        work: work_units,
        verification: verification_units,
        max_active_inferences: u64::from(m.max_active_inferences),
    };
    let oversight_permit = if let Some(o) = &m.oversight {
        work_units.tokens = work_units
            .tokens
            .checked_sub(o.tokens)
            .ok_or("oversight token allowance")?;
        work_units.cost_micro_usd = work_units
            .cost_micro_usd
            .checked_sub(o.cost_micro_usd)
            .ok_or("oversight cost allowance")?;
        if store.campaign_command_gate(&c, &work)?.is_none() {
            use super::model_accounting::services::{ServicePolicy, ServicePurpose};
            store.host_authorize_campaign_envelope(
                &format!("{c}-authorize"),
                &c,
                envelope.clone(),
            )?;
            let policy = ServicePolicy {
                purpose: ServicePurpose::CampaignOversight,
                allowance: Units {
                    tokens: o.tokens,
                    cost_micro_usd: o.cost_micro_usd,
                },
                estimate: RequestEstimate {
                    base_url: launch.base_url.clone(),
                    model: m.model.clone(),
                    provider: m
                        .web
                        .as_ref()
                        .map(|w| w.inference_provider.clone())
                        .unwrap_or_else(|| "openrouter".into()),
                    pricing_revision: m.pricing_revision.clone(),
                    max_request_bytes: m.max_request_bytes,
                    input_tokens: m.input_tokens,
                    output_tokens: m.output_tokens,
                    input_micro_usd_per_million: m.input_micro_usd_per_million,
                    output_micro_usd_per_million: m.output_micro_usd_per_million,
                    other_micro_usd: m.other_micro_usd,
                },
                max_requests: o.max_assessments,
                timeout_ms: o.timeout_ms,
            };
            let campaign = c.clone();
            Some(
                store
                    .storage(move |s| {
                        s.host_authorize_service(&campaign, "oversight", policy, None)
                            .map_err(err)
                    })
                    .await?,
            )
        } else {
            None
        }
    } else {
        None
    };
    if let Some(children) = &m.children {
        for s in children
            .execution_templates(&c)
            .iter()
            .flat_map(|t| &t.specs)
        {
            work_units.tokens -= s.work_tokens;
            work_units.cost_micro_usd -= s.work_cost_micro_usd;
            verification_units.tokens -= s.verification_tokens;
            verification_units.cost_micro_usd -= s.verification_cost_micro_usd;
        }
    }
    let policy = if let Some(gate) = store.campaign_command_gate(&c, &work)? {
        gate.original_policy.unwrap_or(gate.policy)
    } else {
        store.host_authorize_campaign_envelope(&format!("{c}-authorize"), &c, envelope)?;
        store.host_configure_work_limits(
            &c,
            m.children.as_ref().map_or(
                super::groups::WorkLimits {
                    total_work: 2,
                    max_depth: 0,
                    max_running: if human { 2 } else { 1 },
                    max_resident: if human { 2 } else { 1 },
                },
                |children| super::groups::WorkLimits {
                    total_work: children.total_work,
                    max_depth: children.max_depth,
                    max_running: children.max_running,
                    max_resident: children.max_resident,
                },
            ),
        )?;
        for (id, pool, upper_bound) in [
            (&work, Pool::Work, work_units),
            (&verifier, Pool::Verification, verification_units),
        ] {
            let admission = Admission {
                campaign_id: c.clone(),
                work_id: id.clone(),
                objective: m.objective.clone(),
                generation: 1,
                instruction_revision: 1,
                pool,
                upper_bound,
            };
            store.host_admit_agent_work(admission, None)?;
            // Command review claims its lease after the root releases execution.
            if pool == Pool::Verification && !human {
                continue;
            }
            let claim = store
                .claim_campaign_work_matching(|w| {
                    w.admission.campaign_id == c && w.admission.work_id == *id
                })?
                .ok_or("campaign dispatch not ready")?;
            store.reconcile_campaign_dispatch(
                &claim,
                DispatchOutcome::Registered {
                    worker_id: id.clone(),
                },
            )?;
        }
        ExecutionPolicy {
            funding: store.admitted_work(&c, &work)?,
            verification: store.admitted_work(&c, &verifier)?,
            evaluator_id,
            work: WorkRequest {
                context_refs: vec![],
                constraints: None,
                work_id: work.clone(),
                objective: m.objective.clone(),
                generation: 1,
                assignment: 1,
                lifetime_class: LifetimeClass::Short,
                deadline_ms: m.deadline_ms,
                attempt: None,
            },
            model: RequestReservation {
                identity: WorkIdentity {
                    campaign_id: c.clone(),
                    work_id: work.clone(),
                    attempt_id: format!("{c}-attempt"),
                    generation: 1,
                    instruction_revision: 1,
                    class: RequestClass::Work,
                },
                estimate: RequestEstimate {
                    base_url: launch.base_url,
                    model: m.model.clone(),
                    provider: m
                        .web
                        .as_ref()
                        .map(|w| w.inference_provider.clone())
                        .unwrap_or_else(|| "openrouter".into()),
                    pricing_revision: m.pricing_revision.clone(),
                    max_request_bytes: m.max_request_bytes,
                    input_tokens: m.input_tokens,
                    output_tokens: m.output_tokens,
                    input_micro_usd_per_million: m.input_micro_usd_per_million,
                    output_micro_usd_per_million: m.output_micro_usd_per_million,
                    other_micro_usd: m.other_micro_usd,
                },
            },
        }
    };
    let root = storage_root.join("campaigns").join(&c);
    let staging = root.join("verification");
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&staging)
        .map_err(err)?;
    let artifacts = Arc::new(ArtifactStore::open_retained(
        &root.join("artifacts"),
        store.retained.clone(),
        &c,
    )?);
    let mut controls = m
        .children
        .as_ref()
        .map(|c| c.controls.clone())
        .unwrap_or_default();
    let history = m.children.as_ref().is_some_and(|c| c.history);
    if history {
        controls.push(tachyon_api::agents::Control::Resource);
    }
    let mut broker = ModelBroker::new(store.clone(), model).with_controls(controls);
    if let Some(web) = m.web.clone() {
        broker = broker.with_web(
            web,
            tachyon_util::config::Config::try_load_from(
                &tachyon_util::config::Config::default_path(),
            )
            .map_err(err)?
            .web,
        )?;
    }
    if history {
        broker = broker.with_research_artifacts(artifacts.clone());
    }
    let broker = Arc::new(broker);
    let oversight_broker = broker.clone();
    let oversight_manifest = m.clone();
    let oversight_cancel = cancel.subscribe();
    let oversight_deadline =
        tokio::time::Instant::now() + Duration::from_millis(m.deadline_ms.saturating_sub(now()));
    let (finished, completion) = watch::channel(false);
    let work_execution = async {
        let (root_cancel, _) = watch::channel(*cancel.borrow());
        broker
            .launches
            .lock()
            .map_err(|_| "launch registry unavailable")?
            .insert((c.clone(), work.clone(), 1), root_cancel.clone());
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(m.deadline_ms.saturating_sub(now()));
        let mut scheduler = {
            let m = m.clone();
            let c = c.clone();
            let policy = policy.clone();
            let broker = broker.clone();
            let artifacts = artifacts.clone();
            let staging = staging.clone();
            let config = config.clone();
            tokio::task::spawn_blocking(move || -> Result<_, String> {
                if let Some(children) = m.children.as_ref().filter(|_| !prepared) {
                    use super::scheduler::HostScheduler;
                    let mut scheduler =
                        HostScheduler::command_children(broker.clone(), children.max_resident)?;
                    scheduler.select_allocation(&c, m.allocation.clone())?;
                    for t in &children.execution_templates(&c) {
                        let profile = children
                            .dynamic
                            .iter()
                            .flat_map(|d| &d.profiles)
                            .find(|p| p.template_id(&c) == t.template_id);
                        let template = child_template(
                            &m,
                            t,
                            &policy,
                            Some(artifacts.clone()),
                            &staging,
                            &config,
                        )?;
                        if let Some(profile) = profile {
                            scheduler.approve_profile(template, profile.clone())?;
                        } else {
                            scheduler.approve(template)?;
                        }
                    }
                    scheduler.restore_proposals()?;
                    Ok(Some(scheduler))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(err)??
        };
        let execution = Box::pin(broker.execute_campaign_command_loop_prepared(
            &m.executable,
            &m.workspace,
            &m.home,
            policy.clone(),
            deadline,
            artifacts.clone(),
            staging.clone(),
            config.clone(),
            prepared,
        ));
        tokio::pin!(execution);
        let mut scheduler_error = None;
        let mut cancellation = cancel.subscribe();
        root_cancel.send_replace(*cancellation.borrow_and_update());
        let mut result = loop {
            tokio::select! {
                result = &mut execution => break result,
                _ = cancellation.changed() => { root_cancel.send_replace(*cancellation.borrow()); }
                _ = tokio::time::sleep(Duration::from_millis(10)), if scheduler.is_some() && scheduler_error.is_none() => {
                    if *cancel.borrow() { continue; }
                    if let Err(e) = scheduler.as_mut().unwrap().tick().await {
                        scheduler_error = Some(e);
                        root_cancel.send_replace(true);
                    }
                }
            }
        };
        // A stopped root awaiting human input retains only its owner/deadline monitor.
        // No worker, evaluator process, model request, or host capacity permit is held.
        while result
            .as_ref()
            .is_ok_and(|r| r.phase == ExecutionPhase::AwaitingAcceptance)
        {
            let campaign = c.clone();
            let cancelled = *cancel.borrow();
            result = store
                .storage(move |s| {
                    s.poll_human_acceptance(&campaign, cancelled)?
                        .ok_or("missing human acceptance execution".into())
                })
                .await;
            if result
                .as_ref()
                .is_ok_and(|r| r.phase == ExecutionPhase::AwaitingAcceptance)
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        // Root completion stops admission before draining every child owner. Never
        // detach a worker or publish success while cleanup is uncertain.
        let mut cleanup_error = None;
        let mut admitted_children = Vec::new();
        if let Some(scheduler) = &mut scheduler {
            for work_id in child_work_ids {
                for id in [work_id.clone(), format!("{work_id}-verification")] {
                    match store.local_work_status(&c, &id) {
                        Ok(Some(status)) => {
                            if !status.terminal {
                                if let Err(e) = store.host_cancel_work(&c, &id, 1) {
                                    cleanup_error = Some(e);
                                }
                            }
                            admitted_children.push(id);
                        }
                        Ok(None) => {}
                        Err(e) => cleanup_error = Some(e),
                    }
                }
            }
            if let Err(e) = scheduler.shutdown().await {
                cleanup_error = Some(e);
            }
            let ledger = store.campaign_ledger(&c)?;
            for id in admitted_children {
                match store.local_work_status(&c, &id) {
                    Ok(Some(status)) if status.terminal => {
                        // Terminal leases prove neither final usage nor allocation closure.
                        let dispatch = &status.work.dispatch_id;
                        if !ledger.as_ref().is_some_and(|l| {
                            l.allocations.get(dispatch) == Some(&true)
                                || l.reservations.get(dispatch).is_some_and(|r| {
                                    matches!(r.usage, super::campaign_ledger::Usage::Final(_))
                                })
                        }) {
                            cleanup_error = Some("child accounting remains uncertain".into());
                        }
                    }
                    _ => cleanup_error = Some("child termination remains uncertain".into()),
                }
            }
        }
        drop(scheduler.take());
        if scheduler_error.is_none()
            && cleanup_error.is_none()
            && !*cancel.borrow()
            && tokio::time::Instant::now() < deadline
            && result.as_ref().is_ok_and(|r| {
                matches!(
                    r.phase,
                    ExecutionPhase::EvidenceReady | ExecutionPhase::AwaitingVerification
                )
            })
        {
            // Children may have occupied the verifier slot when the root finished.
            // Resume the evidence gate under the original root retry policy. Child
            // admission stays withdrawn even if verification authorizes root repair.
            let review = Box::pin(broker.execute_campaign_command_loop(
                &m.executable,
                &m.workspace,
                &m.home,
                policy,
                deadline,
                artifacts,
                staging,
                config,
            ));
            tokio::pin!(review);
            result = loop {
                tokio::select! {
                    result = &mut review => break result,
                    _ = cancellation.changed() => { root_cancel.send_replace(*cancellation.borrow()); }
                }
            };
        }
        broker
            .launches
            .lock()
            .map_err(|_| "launch registry unavailable")?
            .remove(&(c.clone(), work.clone(), 1));
        if let Some(e) = scheduler_error.or(cleanup_error) {
            return Err(e);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("campaign cleanup exceeded deadline".into());
        }
        if *cancel.borrow() && store.campaign_work_status(&c, &work)?.terminal {
            return Ok(ExecutionPhase::Reviewed(Evaluation::Unverified));
        }
        Ok(result?.phase)
    };
    let mut work_execution = Box::pin(work_execution);
    let work_execution = async {
        let result = loop {
            tokio::select! {
                result = &mut work_execution => break result,
                _ = tokio::time::sleep(Duration::from_millis(25)), if !*cancel.borrow() => {
                    let campaign = c.clone();
                    let root = work.clone();
                    let state = store.storage(move |s| s.campaign_work_status(&campaign, &root)).await;
                    // Conversation controls persist intent, not task handles. The
                    // existing owner must signal execution, review and human waits.
                    match state {
                        Ok(state) if state.cancellation_requested => { cancel.send_replace(true); }
                        Err(error) => {
                            eprintln!("tachyond: campaign cancellation polling: {error}");
                            cancel.send_replace(true);
                        }
                        _ => {}
                    }
                }
            }
        };
        finished.send_replace(true);
        result
    };
    let oversight = async {
        if let Some(permit) = oversight_permit {
            super::campaign_oversight::run(
                oversight_broker,
                oversight_manifest,
                permit,
                oversight_cancel,
                completion,
                oversight_deadline,
            )
            .await;
        }
    };
    let (result, ()) = tokio::join!(work_execution, oversight);
    result
}

#[cfg(test)]
pub(super) mod tests {
    mod acceptance;
    mod compute;
    use super::*;
    mod concurrency;
    mod continuation;
    mod human;
    mod parallel_acceptance;
    mod reconciliation;
    mod retention;
    mod swarm;
    mod work;

    pub(in crate::runtime_store) fn fixture(
    ) -> (tempfile::TempDir, Arc<RuntimeStore>, CampaignManifest) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: "r".into(),
                title: "fixture".into(),
                objective: "fixture".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: "c".into(),
                research_id: research.id,
                title: "fixture".into(),
                objective: "fixture".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let m = CampaignManifest {
            web: None,
            oversight: None,
            retained_storage_bytes: None,
            compute: None,
            allocation: None,
            schema_version: 1,
            campaign_id: campaign.id,
            objective: "fixture".into(),
            executable: "/nonexistent/ghost".into(),
            workspace: "/nonexistent/work".into(),
            home: "/nonexistent/home".into(),
            deadline_ms: now() + 60000,
            work_tokens: 100,
            work_cost_micro_usd: 100,
            verification_tokens: 10,
            verification_cost_micro_usd: 10,
            max_active_inferences: 2,
            model: "fixture".into(),
            pricing_revision: "fixture-v1".into(),
            max_request_bytes: 32000,
            input_tokens: 20,
            output_tokens: 10,
            input_micro_usd_per_million: 1000000,
            output_micro_usd_per_million: 1000000,
            other_micro_usd: 0,
            evaluator: tachyon_api::campaign::CampaignEvaluator {
                acceptance_mode: None,
                result_contract: Default::default(),
                metrics: Default::default(),
                allow_extra_metrics: false,
                stage: None,
                argv: vec!["/usr/bin/true".into()],
                timeout_ms: 100,
                output_bytes: 1024,
                input_bytes: 1024,
                max_attempts: 1,
                max_total_command_ms: 100,
            },
            children: None,
        };
        (dir, store, m)
    }

    fn status(store: &RuntimeStore, m: &CampaignManifest) -> CampaignStatus {
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignGet {
                id: m.campaign_id.clone(),
            })
            .unwrap()
        else {
            panic!()
        };
        campaign.status
    }

    #[test]
    fn authorization_denial_and_metadata_never_construct_execution() {
        let (dir, store, m) = fixture();
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        assert!(service
            .run(&m, false)
            .unwrap_err()
            .contains("authorization required"));
        assert!(service
            .recover(&m.campaign_id, false)
            .unwrap_err()
            .contains("authorization required"));
        assert_eq!(status(&store, &m), CampaignStatus::Draft);
        assert!(service.active.lock().unwrap().is_empty());
        assert!(store.campaign_ledger(&m.campaign_id).unwrap().is_none());
        assert_eq!(
            store
                .database
                .begin_read()
                .unwrap()
                .open_table(LAUNCHES)
                .unwrap()
                .iter()
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn inspection_and_recovery_of_unknown_root_never_start_jobs() {
        let (dir, store, m) = fixture();
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        let launch = Launch {
            schema_version: 1,
            manifest: m.clone(),
            base_url: "http://127.0.0.1:1/v1".into(),
            manifest_sha256: Some(Launch::digest(&m).unwrap()),
        };
        let tx = store.database.begin_write().unwrap();
        tx.open_table(LAUNCHES)
            .unwrap()
            .insert(
                m.campaign_id.as_str(),
                serde_json::to_vec(&launch).unwrap().as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
        let ApiResponse::CampaignInspection {
            campaign,
            diagnostics,
        } = service.inspect(&m.campaign_id).unwrap()
        else {
            panic!()
        };
        assert_eq!(campaign.status, CampaignStatus::Draft);
        assert!(diagnostics[0].contains("No jobs started"));
        assert!(service
            .recover(&m.campaign_id, true)
            .unwrap_err()
            .contains("no original root policy"));
        assert!(service.active.lock().unwrap().is_empty());
        assert!(store.campaign_ledger(&m.campaign_id).unwrap().is_none());
        assert_eq!(status(&store, &m), CampaignStatus::Draft);
        assert!(!dir.path().join("campaigns").exists());
    }

    #[test]
    fn endpoint_rejects_embedded_secrets_and_invalid_transport_without_credentials() {
        for endpoint in [
            "",
            "not a url",
            "file:///tmp/provider",
            "https://user:secret@example.com",
            "https://example.com?api_key=secret",
            "https://example.com#secret",
        ] {
            assert!(validate_endpoint(endpoint).is_err());
        }
        assert!(validate_endpoint("http://127.0.0.1:1234/v1").is_ok());
        assert!(validate_endpoint("https://openrouter.ai/api/v1").is_ok());
    }

    #[test]
    fn invalid_envelope_is_rejected_before_running_or_callback() {
        let (dir, store, mut m) = fixture();
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        m.work_tokens = u64::MAX;
        assert!(service
            .launch(&m, "http://127.0.0.1".into(), false, |_, _| async {
                panic!("invalid budget must never execute")
            })
            .is_err());
        assert_eq!(status(&store, &m), CampaignStatus::Draft);
        assert!(service.active.lock().unwrap().is_empty());
        assert!(store.campaign_ledger(&m.campaign_id).unwrap().is_none());
    }

    #[test]
    fn callback_errors_and_panics_are_durable_and_never_replayed() {
        for failure in 0..3 {
            let (dir, store, m) = fixture();
            let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
            service
                .launch(&m, "http://127.0.0.1".into(), false, move |_, _| {
                    assert_ne!(failure, 1, "callback construction panic");
                    async move {
                        assert_ne!(failure, 2, "callback polling panic");
                        Err("fixture execution error".into())
                    }
                })
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !service
                .active
                .lock()
                .unwrap()
                .get(&m.campaign_id)
                .unwrap()
                .task
                .is_finished()
            {
                assert!(std::time::Instant::now() < deadline, "callback hung");
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(status(&store, &m), CampaignStatus::Unverified);
            assert!(service.ready_to_resume(&m.campaign_id).is_err());
            assert!(service
                .launch(&m, "http://127.0.0.1".into(), false, |_, _| async {
                    panic!("failed callback must not replay")
                })
                .is_err());
            service.shutdown();
            let reopened = CampaignService::new(store.clone(), dir.path().into()).unwrap();
            assert_eq!(status(&store, &m), CampaignStatus::Unverified);
            reopened.shutdown();
        }
    }

    #[test]
    fn actual_setup_failure_finishes_unverified_with_protected_holds() {
        let (dir, store, m) = fixture();
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        let base_url = "http://127.0.0.1:1".to_string();
        let model = Model::new(ModelConfig {
            base_url: base_url.clone(),
            api_key: "fixture-only".into(),
            model: m.model.clone(),
            temperature: 0.0,
            max_completion_tokens: Some(m.output_tokens),
            context_length: None,
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: None,
            debug: false,
            debug_log: None,
        });
        let run_store = store.clone();
        // An existing regular file cannot contain daemon-owned staging directories.
        let invalid_root = dir.path().join("runtime.redb");
        service
            .launch(&m, base_url, false, move |launch, cancel| {
                execute(run_store, invalid_root, launch, model, cancel)
            })
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !service
            .active
            .lock()
            .unwrap()
            .get(&m.campaign_id)
            .unwrap()
            .task
            .is_finished()
        {
            assert!(std::time::Instant::now() < deadline, "setup failure hung");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(status(&store, &m), CampaignStatus::Unverified);
        let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
        let tx = store.database.begin_write().unwrap();
        let limits = RuntimeStore::coordination_limits_in(&tx, &m.campaign_id).unwrap();
        assert_eq!(limits.total_work, 2);
        assert_eq!(limits.max_depth, 0);
        assert_eq!(limits.max_running, 1);
        assert_eq!(limits.max_resident, 1);
        drop(tx);
        let ApiResponse::CampaignProgress { activity, .. } =
            service.progress(&m.campaign_id).unwrap()
        else {
            panic!()
        };
        assert_eq!(activity.admitted, 2);
        assert_eq!(activity.active, 1);
        assert_eq!(activity.queued, 1);
        for (suffix, pool, tokens) in [
            ("root", Pool::Work, m.work_tokens),
            ("verification", Pool::Verification, m.verification_tokens),
        ] {
            let admitted = store
                .admitted_work(&m.campaign_id, &format!("{}-{suffix}", m.campaign_id))
                .unwrap();
            let hold = &ledger.reservations[&admitted.dispatch_id];
            assert_eq!(hold.pool, pool);
            assert_eq!(hold.reserved.tokens, tokens);
            assert_eq!(hold.usage, super::super::campaign_ledger::Usage::Unknown);
        }
        assert!(service.ready_to_resume(&m.campaign_id).is_err());
        service.shutdown();
    }

    #[test]
    fn dispatch_is_nonblocking_durable_bounded_and_cancellable() {
        let (dir, store, m) = fixture();
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        let begin = std::time::Instant::now();
        let response = service
            .launch(
                &m,
                "http://127.0.0.1".into(),
                false,
                |_, cancel| async move {
                    let mut stop = cancel.subscribe();
                    while !*stop.borrow_and_update() {
                        stop.changed().await.unwrap();
                    }
                    Ok(ExecutionPhase::Reviewed(Evaluation::Unverified))
                },
            )
            .unwrap();
        assert!(begin.elapsed() < Duration::from_secs(1));
        assert!(
            matches!(response, ApiResponse::Campaign { campaign } if campaign.status == CampaignStatus::Running)
        );
        assert_eq!(status(&store, &m), CampaignStatus::Running);
        assert!(service
            .launch(&m, "http://127.0.0.1".into(), false, |_, _| async {
                panic!("must not construct second future")
            })
            .is_err());
        service.cancel(&m.campaign_id).unwrap();
        service.shutdown();
        assert_eq!(status(&store, &m), CampaignStatus::Cancelled);
        drop(service);
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        assert_eq!(status(&store, &m), CampaignStatus::Cancelled);
        assert!(service.active.lock().unwrap().is_empty());
    }

    #[test]
    fn startup_marks_unknown_launch_interrupted_without_replay() {
        let (dir, store, m) = fixture();
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        service
            .launch(&m, "http://127.0.0.1".into(), false, |_, _| async {
                Ok(ExecutionPhase::Reviewed(Evaluation::Accepted))
            })
            .unwrap();
        while !service
            .active
            .lock()
            .unwrap()
            .get(&m.campaign_id)
            .unwrap()
            .task
            .is_finished()
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(status(&store, &m), CampaignStatus::Accepted);
        service.shutdown();
        store
            .local_campaign_status(&m.campaign_id, CampaignStatus::Running)
            .unwrap();
        let recovered = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        assert_eq!(status(&store, &m), CampaignStatus::Interrupted);
        assert!(recovered.active.lock().unwrap().is_empty());
        assert!(recovered.ready_to_resume(&m.campaign_id).is_err());
        assert_eq!(
            store.host_capacity.resident.available(),
            store.host_capacity.limits.max_resident_workers
        );
        assert_eq!(
            store.host_capacity.execution.available(),
            store.host_capacity.limits.max_execution_jobs
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires freshly built GHOST_TEST_BIN; explicit localhost fixture only"]
    async fn actual_ghost_launch_uses_supplied_local_model_without_user_credentials() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (dir, store, mut m) = fixture();
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        m.workspace = workspace.path().canonicalize().unwrap();
        m.home = home.path().canonicalize().unwrap();
        m.executable =
            PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"))
                .canonicalize()
                .unwrap();
        m.validate(now()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let http = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
                assert!(header.len() < 16384);
            }
            let header = String::from_utf8(header).unwrap();
            let length: usize = header
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse().unwrap())
                })
                .unwrap();
            assert!(length <= 32000);
            let mut body = vec![0; length];
            socket.read_exact(&mut body).await.unwrap();
            let response = "data: {\"choices\":[{\"delta\":{\"content\":\"fixture complete\"}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7,\"cost\":0.000007}}\n\ndata: [DONE]\n\n";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        });
        let model = Model::new(ModelConfig {
            base_url: base_url.clone(),
            api_key: "local-fixture-only".into(),
            model: m.model.clone(),
            temperature: 0.0,
            max_completion_tokens: Some(m.output_tokens),
            context_length: None,
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: None,
            debug: false,
            debug_log: None,
        });
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        let run_store = store.clone();
        let root = dir.path().to_owned();
        let response = service
            .launch(&m, base_url, false, move |launch, cancel| async move {
                let result = execute(run_store, root, launch, model, cancel).await;
                assert!(result.is_ok(), "{result:?}");
                result
            })
            .unwrap();
        assert!(
            matches!(response, ApiResponse::Campaign { campaign } if campaign.status == CampaignStatus::Running)
        );
        tokio::time::timeout(Duration::from_secs(20), async {
            while !service
                .active
                .lock()
                .unwrap()
                .get(&m.campaign_id)
                .unwrap()
                .task
                .is_finished()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        service.shutdown();
        tokio::time::timeout(Duration::from_secs(2), http)
            .await
            .unwrap()
            .unwrap();
        let record = store
            .campaign_execution(&m.campaign_id, &format!("{}-root", m.campaign_id))
            .unwrap()
            .unwrap();
        assert!(
            record.candidate.is_some(),
            "actual Ghost must return correlated evidence"
        );
        assert_eq!(
            status(&store, &m),
            CampaignStatus::Unverified,
            "no artifact was published; never accept text as verified"
        );
    }
}
