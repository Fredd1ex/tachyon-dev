//! Trusted host catalog only. No model-selected policy, grants, or process replay.
#![allow(dead_code)]
#![forbid(unsafe_code)]
use super::{
    admission::DispatchOutcome,
    execution::{Evaluation, ExecutionPhase, ExecutionPolicy, ExecutionRecord},
    model_accounting::ModelBroker,
};
use redb::{ReadableTable, TableDefinition};
use std::{
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

mod catalog;
pub(crate) mod snapshot;
#[allow(unused_imports)]
pub(crate) use catalog::{HostCandidate, HostCatalog, HostTemplate};

pub(crate) type LaunchKey = (String, String, u64);
pub(crate) type LaunchRegistry = Arc<Mutex<BTreeMap<LaunchKey, watch::Sender<bool>>>>;
type Callback = Arc<
    dyn Fn(tachyon_api::types::WorkResult) -> Pin<Box<dyn Future<Output = Evaluation> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub(crate) enum Evaluator {
    Callback(Callback),
    Command {
        artifacts: Arc<tachyond::artifact_store::ArtifactStore>,
        staging: PathBuf,
        config: tachyond::verification::CommandEvaluator,
    },
    /// Reauthorization only: no artifact store is opened and no command can run.
    CommandDescriptor {
        staging: PathBuf,
        config: tachyond::verification::CommandEvaluator,
    },
}
const OUTCOMES: TableDefinition<&str, &[u8]> = TableDefinition::new("campaign_scheduler_outcomes");

/// Caller authorizes every exact policy and path. This does not admit new work.
#[derive(Clone)]
pub(crate) struct HostExecution {
    pub policy: ExecutionPolicy,
    pub executable: PathBuf,
    pub workspace: PathBuf,
    pub home: PathBuf,
    pub evaluate: Evaluator,
}

/// Sole owner of task handles; broker retains only cancellation senders, no cycle.
pub(crate) struct HostScheduler {
    broker: Arc<ModelBroker>,
    catalog: BTreeMap<LaunchKey, Arc<HostExecution>>,
    tasks: BTreeMap<LaunchKey, JoinHandle<Result<ExecutionRecord, String>>>,
    completed: BTreeMap<LaunchKey, Result<ExecutionRecord, String>>,
    capacity: usize,
    approved: Mutex<Vec<String>>,
    allocation: Option<(String, String, tachyon_api::campaign::CampaignAllocation)>,
}

impl HostScheduler {
    pub(crate) fn outcome(
        &self,
        campaign: &str,
        work: &str,
        generation: u64,
    ) -> Result<Option<Result<ExecutionRecord, String>>, String> {
        let key = (campaign.to_owned(), work.to_owned(), generation);
        let entry = self
            .catalog
            .get(&key)
            .cloned()
            .or(self
                .broker
                .store
                .host_catalog
                .lock()
                .map_err(|_| "host catalog unavailable")?
                .admitted
                .get(&key)
                .cloned())
            .ok_or("work outside host catalog")?;
        let tx = self
            .broker
            .store
            .database
            .begin_write()
            .map_err(|e| e.to_string())?;
        let table = tx.open_table(OUTCOMES).map_err(|e| e.to_string())?;
        let result = table
            .get(entry.policy.funding.dispatch_id.as_str())
            .map_err(|e| e.to_string())?
            .map(|v| serde_json::from_slice(v.value()).map_err(|e| e.to_string()))
            .transpose();
        result
    }

    pub(crate) fn new(
        broker: Arc<ModelBroker>,
        entries: Vec<HostExecution>,
        capacity: usize,
    ) -> Result<Self, String> {
        if !(1..=64).contains(&capacity) || entries.len() > 256 {
            return Err("invalid scheduler bounds".into());
        }
        let mut catalog = BTreeMap::new();
        let mut verifiers = std::collections::BTreeSet::new();
        for entry in entries {
            let p = &entry.policy;
            let a = &p.funding.admission;
            if !entry.executable.is_absolute()
                || !entry.workspace.is_absolute()
                || !entry.home.is_absolute()
                || p.work.work_id != a.work_id
                || p.work.generation != a.generation
                || a.pool != super::campaign_ledger::Pool::Work
                || p.verification.admission.pool != super::campaign_ledger::Pool::Verification
                || !verifiers.insert(p.verification.admission.work_id.clone())
            {
                return Err("invalid scheduler catalog".into());
            }
            let current = broker.store.admitted_work(&a.campaign_id, &a.work_id)?;
            broker
                .store
                .campaign_work_status(&a.campaign_id, &a.work_id)?;
            if current.admission != *a || current.dispatch_id != p.funding.dispatch_id {
                return Err("catalog admission conflict".into());
            }
            let key = (a.campaign_id.clone(), a.work_id.clone(), a.generation);
            if catalog.insert(key, Arc::new(entry)).is_some() {
                return Err("duplicate catalog work".into());
            }
        }
        if catalog.keys().any(|(_, w, _)| verifiers.contains(w)) {
            return Err("verifier cannot be a launch policy".into());
        }
        broker
            .resident_capacity
            .store(capacity, std::sync::atomic::Ordering::Release);
        Ok(Self {
            broker,
            catalog,
            tasks: BTreeMap::new(),
            completed: BTreeMap::new(),
            capacity,
            approved: Mutex::new(Vec::new()),
            allocation: None,
        })
    }

    /// The root has a separate command-loop owner and consumes one resident slot.
    pub(crate) fn command_children(
        broker: Arc<ModelBroker>,
        resident: usize,
    ) -> Result<Self, String> {
        if !(2..=64).contains(&resident) {
            return Err("child scheduler requires bounded residents".into());
        }
        let scheduler = Self::new(broker, Vec::new(), resident - 1)?;
        scheduler
            .broker
            .resident_capacity
            .store(resident, std::sync::atomic::Ordering::Release);
        Ok(scheduler)
    }

    /// Bounded polling; no task, authority lock or transaction crosses a wait.
    /// Completed failures are durable; unknown execution/review is never replayed.
    pub(crate) async fn tick(&mut self) -> Result<usize, String> {
        let approved = self
            .approved
            .lock()
            .map_err(|_| "approval registry unavailable")?
            .clone();
        self.catalog.extend(
            self.broker
                .store
                .storage(move |store| {
                    let catalog = store
                        .host_catalog
                        .lock()
                        .map_err(|_| "host catalog unavailable")?;
                    Ok(approved
                        .iter()
                        .filter_map(|id| catalog.templates.get(id))
                        .flat_map(|t| &t.candidates)
                        .filter_map(|c| {
                            let a = &c.admission;
                            let key = (a.campaign_id.clone(), a.work_id.clone(), a.generation);
                            catalog.admitted.get(&key).map(|e| (key, e.clone()))
                        })
                        .collect::<BTreeMap<_, _>>())
                })
                .await?,
        );
        self.reap().await?;
        if let Some((campaign, owner, allocation)) = &self.allocation {
            if allocation.mode == tachyon_api::campaign::AllocationMode::Deterministic {
                let campaign = campaign.clone();
                let owner = owner.clone();
                let cap = allocation.max_running;
                let allocation = allocation.clone();
                let free = self.capacity.saturating_sub(self.tasks.len());
                // Includes the command-loop root, whose cancellation owner is the
                // campaign runner rather than this child scheduler's task map.
                let live = self
                    .broker
                    .launches
                    .lock()
                    .map_err(|_| "launch registry unavailable")?
                    .iter()
                    .filter(|(key, cancel)| {
                        key.0 == campaign
                            && !*cancel.borrow()
                            && self.tasks.get(*key).is_none_or(|t| !t.is_finished())
                    })
                    .map(|((_, w, generation), _)| (w.clone(), *generation))
                    .collect::<Vec<_>>();
                let approved = self
                    .approved
                    .lock()
                    .map_err(|_| "approval registry unavailable")?
                    .clone();
                self.broker
                    .store
                    .storage(move |store| {
                        let mut groups = {
                            let catalog = store
                                .host_catalog
                                .lock()
                                .map_err(|_| "host catalog unavailable")?;
                            approved
                                .iter()
                                .filter_map(|id| catalog.templates.get(id))
                                .filter(|t| {
                                    t.parent.campaign_id == campaign
                                        && !catalog.profiles.contains_key(&t.template_id)
                                })
                                .filter_map(|t| {
                                    t.group_id.as_ref().map(|g| {
                                        (
                                            g.clone(),
                                            t.candidates
                                                .iter()
                                                .map(|c| c.admission.work_id.clone())
                                                .collect(),
                                        )
                                    })
                                })
                                .collect::<Vec<_>>()
                        };
                        let admitted = store
                            .host_catalog
                            .lock()
                            .map_err(|_| "host catalog unavailable")?
                            .admitted
                            .keys()
                            .filter(|k| k.0 == campaign)
                            .map(|k| k.1.clone())
                            .collect::<std::collections::BTreeSet<_>>();
                        for group in store.list_campaign_groups(&campaign, None, 64)? {
                            if !groups.iter().any(|(id, _)| id == &group.spec.group_id)
                                && group
                                    .spec
                                    .work
                                    .iter()
                                    .all(|w| admitted.contains(&w.work_id))
                            {
                                groups.push((
                                    group.spec.group_id,
                                    group.spec.work.into_iter().map(|w| w.work_id).collect(),
                                ));
                            }
                        }
                        for signal in &allocation.signals {
                            let selected = tachyon_api::campaign::CampaignAllocation {
                                signals: vec![signal.clone()],
                                ..allocation.clone()
                            };
                            if store
                                .allocation_control_tick(&campaign, &owner, &selected, &groups)?
                                || store.allocation_execution_tick(
                                    &campaign, &owner, &selected, &groups, &approved,
                                )?
                            {
                                return Ok(());
                            }
                        }
                        if allocation
                            .allowed_actions
                            .contains(&tachyon_api::campaign::AllocationAction::Resize)
                        {
                            store.allocation_tick(&campaign, &owner, cap, free, &groups, &live)?;
                        }
                        Ok(())
                    })
                    .await?;
            }
        }
        self.dispatch().await
    }

    pub(crate) fn select_allocation(
        &mut self,
        campaign: &str,
        allocation: Option<tachyon_api::campaign::CampaignAllocation>,
    ) -> Result<(), String> {
        if let Some(allocation) = allocation {
            allocation.validate()?;
            if !(1..=64).contains(&allocation.max_running) {
                return Err("allocation concurrency cap must be 1..64".into());
            }
            let owner = uuid::Uuid::new_v4().to_string();
            self.broker
                .store
                .select_allocation_owner(campaign, &owner)?;
            self.allocation = Some((campaign.into(), owner, allocation));
        }
        Ok(())
    }

    async fn reap(&mut self) -> Result<(), String> {
        let finished: Vec<_> = self
            .tasks
            .iter()
            .filter(|(_, t)| t.is_finished())
            .map(|(k, _)| k.clone())
            .collect();
        for key in finished {
            let result = self.tasks.remove(&key).unwrap().await;
            let outcome = result
                .map_err(|e| format!("scheduler task lost: {e}"))
                .and_then(|r| r)
                .map_err(|e| e.chars().take(1024).collect());
            self.completed.insert(key, outcome);
        }
        for (key, outcome) in &self.completed {
            let bytes = serde_json::to_vec(&outcome).map_err(|e| e.to_string())?;
            let dispatch_id = self.catalog[key].policy.funding.dispatch_id.clone();
            self.broker
                .store
                .storage(move |store| {
                    let tx = store.database.begin_write().map_err(|e| e.to_string())?;
                    tx.open_table(OUTCOMES)
                        .map_err(|e| e.to_string())?
                        .insert(dispatch_id.as_str(), bytes.as_slice())
                        .map_err(|e| e.to_string())?;
                    tx.commit().map_err(|e| e.to_string())?;
                    Ok(())
                })
                .await?;
            self.broker
                .launches
                .lock()
                .map_err(|_| "launch registry unavailable")?
                .remove(&key);
        }
        self.completed.clear();
        Ok(())
    }

    async fn dispatch(&mut self) -> Result<usize, String> {
        for key in self.tasks.keys() {
            let polled = key.clone();
            let verifier = self.catalog[key]
                .policy
                .verification
                .admission
                .work_id
                .clone();
            if self
                .broker
                .store
                .storage(move |store| {
                    Ok(store
                        .campaign_work_status(&polled.0, &polled.1)?
                        .cancellation_requested
                        || store
                            .campaign_work_status(&polled.0, &verifier)?
                            .cancellation_requested)
                })
                .await?
            {
                if let Some(cancel) = self
                    .broker
                    .launches
                    .lock()
                    .map_err(|_| "launch registry unavailable")?
                    .get(key)
                {
                    cancel.send_replace(true);
                }
            }
        }
        // Deferred verification gets first chance at newly available capacity.
        let deferred: Vec<_> = self
            .catalog
            .iter()
            .filter(|(k, _)| !self.tasks.contains_key(*k))
            .map(|(k, e)| (k.clone(), e.clone()))
            .collect();
        for (key, entry) in deferred {
            if self.tasks.len() >= self.capacity {
                break;
            }
            let polled = key.clone();
            if let Some(record) = self
                .broker
                .store
                .storage(move |store| store.campaign_execution(&polled.0, &polled.1))
                .await?
            {
                let mut approved = entry.policy.clone();
                let c = key.0.clone();
                let w = key.1.clone();
                let gate = self
                    .broker
                    .store
                    .storage(move |store| store.campaign_command_gate(&c, &w))
                    .await?;
                let finalized = gate.as_ref().is_some_and(|g| g.finalize_rework);
                let original = gate
                    .and_then(|g| g.original_policy)
                    .unwrap_or_else(|| record.policy.clone());
                approved.funding.state = original.funding.state.clone();
                if approved != original {
                    return Err("recovery policy differs from approved catalog".into());
                }
                if matches!(
                    record.phase,
                    ExecutionPhase::EvidenceReady | ExecutionPhase::AwaitingVerification
                ) {
                    if !self
                        .broker
                        .store
                        .storage(move |store| store.allocation_review_dispatchable(&record))
                        .await?
                    {
                        continue;
                    }
                    self.start(key, entry, original).await?;
                } else if matches!(record.phase, ExecutionPhase::Reviewed(_)) && !record.settled {
                    if matches!(entry.evaluate, Evaluator::Command { .. }) && !finalized {
                        self.start(key, entry, original).await?;
                    } else {
                        self.broker
                            .store
                            .storage(move |store| store.settle_campaign_execution(&key.0, &key.1))
                            .await?;
                    }
                }
            }
        }
        for _ in self.tasks.len()..self.capacity {
            let catalog = self.catalog.clone();
            let claim = self
                .broker
                .store
                .storage(move |store| {
                    store.claim_campaign_work_matching(|work| {
                        let a = &work.admission;
                        catalog
                            .get(&(a.campaign_id.clone(), a.work_id.clone(), a.generation))
                            .is_some_and(|e| {
                                e.policy.funding.admission == *a
                                    && e.policy.funding.dispatch_id == work.dispatch_id
                            })
                    })
                })
                .await?;
            let Some(claim) = claim else {
                break;
            };
            let a = &claim.admission;
            let key = (a.campaign_id.clone(), a.work_id.clone(), a.generation);
            let entry = self.catalog[&key].clone();
            let funding = self
                .broker
                .store
                .storage(move |store| {
                    store.reconcile_campaign_dispatch(
                        &claim,
                        DispatchOutcome::Registered {
                            worker_id: format!("ghost-{}", claim.dispatch_id),
                        },
                    )?;
                    store.admitted_work(&claim.admission.campaign_id, &claim.admission.work_id)
                })
                .await?;
            let mut policy = entry.policy.clone();
            policy.funding = funding;
            self.start(key, entry, policy).await?;
        }
        Ok(self.tasks.len())
    }

    async fn start(
        &mut self,
        key: LaunchKey,
        entry: Arc<HostExecution>,
        policy: ExecutionPolicy,
    ) -> Result<(), String> {
        let polled = key.clone();
        let verifier = policy.verification.admission.work_id.clone();
        let cancelled = self
            .broker
            .store
            .storage(move |store| {
                Ok(store
                    .campaign_work_status(&polled.0, &polled.1)?
                    .cancellation_requested
                    || store
                        .campaign_work_status(&polled.0, &verifier)?
                        .cancellation_requested)
            })
            .await?;
        let mut registry = self
            .broker
            .launches
            .lock()
            .map_err(|_| "launch registry unavailable")?;
        if registry.contains_key(&key) {
            return Err("launch already owned".into());
        }
        let (cancel, _) = watch::channel(cancelled);
        registry.insert(key.clone(), cancel);
        let broker = self.broker.clone();
        let task = tokio::spawn(async move {
            match entry.evaluate.clone() {
                Evaluator::CommandDescriptor { .. } => {
                    Err("descriptor-only recovery cannot execute".into())
                }
                Evaluator::Command {
                    artifacts,
                    staging,
                    config,
                } => {
                    let deadline = Instant::now()
                        + Duration::from_millis(
                            policy.work.deadline_ms.saturating_sub(
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map_err(|e| e.to_string())?
                                    .as_millis() as u64,
                            ),
                        );
                    let campaign = policy.funding.admission.campaign_id.clone();
                    let work = policy.work.work_id.clone();
                    let mut cancel = broker
                        .launches
                        .lock()
                        .map_err(|_| "launch registry unavailable")?
                        .get(&(campaign.clone(), work.clone(), policy.work.generation))
                        .ok_or("missing launch owner")?
                        .subscribe();
                    loop {
                        let record = broker
                            .execute_campaign_command_loop(
                                &entry.executable,
                                &entry.workspace,
                                &entry.home,
                                policy.clone(),
                                deadline,
                                artifacts.clone(),
                                staging.clone(),
                                config.clone(),
                            )
                            .await?;
                        if record.settled
                            || matches!(
                                record.phase,
                                ExecutionPhase::ExecutingUnknown | ExecutionPhase::ReviewingUnknown
                            )
                        {
                            break Ok(record);
                        }
                        if *cancel.borrow() || Instant::now() >= deadline {
                            break broker
                                .store
                                .storage(move |store| {
                                    store.host_finalize_command_rework(&campaign, &work)
                                })
                                .await;
                        }
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(25)) => {},
                            _ = cancel.changed() => {},
                        }
                        if *cancel.borrow() || Instant::now() >= deadline {
                            break broker
                                .store
                                .storage(move |store| {
                                    store.host_finalize_command_rework(&campaign, &work)
                                })
                                .await;
                        }
                    }
                }
                Evaluator::Callback(evaluate) => {
                    broker
                        .execute_campaign(
                            &entry.executable,
                            &entry.workspace,
                            &entry.home,
                            policy,
                            Instant::now() + Duration::from_secs(86400),
                            move |result| evaluate(result),
                        )
                        .await
                }
            }
        });
        self.tasks.insert(key, task);
        Ok(())
    }

    pub(crate) async fn run(
        &mut self,
        mut stop: watch::Receiver<bool>,
        period: Duration,
    ) -> Result<(), String> {
        if period < Duration::from_millis(10) || period > Duration::from_secs(30) {
            return Err("invalid tick period".into());
        }
        loop {
            if *stop.borrow() {
                break;
            }
            if let Err(error) = self.tick().await {
                self.shutdown().await?;
                return Err(error);
            }
            tokio::select! { _ = tokio::time::sleep(period) => {}, _ = stop.changed() => { if stop.has_changed().is_err() { break; } } }
        }
        self.shutdown().await
    }

    pub(crate) async fn shutdown(&mut self) -> Result<(), String> {
        for key in self.tasks.keys() {
            if let Some(cancel) = self
                .broker
                .launches
                .lock()
                .map_err(|_| "launch registry unavailable")?
                .get(key)
            {
                cancel.send_replace(true);
            }
        }
        // Do not dispatch more work while draining.
        let mut failure = None;
        while !self.tasks.is_empty() {
            if let Err(e) = self.reap().await {
                failure = Some(e);
            }
            if !self.tasks.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        if let Err(e) = self.reap().await {
            failure = Some(e);
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for HostScheduler {
    fn drop(&mut self) {
        // Signal rather than abort: launch_private owns asynchronous kill/reap.
        if let Ok(registry) = self.broker.launches.lock() {
            for key in self.tasks.keys() {
                if let Some(cancel) = registry.get(key) {
                    cancel.send_replace(true);
                }
            }
        }
        if let (Ok(mut catalog), Ok(approved)) =
            (self.broker.store.host_catalog.lock(), self.approved.lock())
        {
            for id in approved.iter() {
                catalog.profiles.remove(id);
                if let Some(template) = catalog.templates.remove(id) {
                    for c in template.candidates {
                        catalog.admitted.remove(&(
                            c.admission.campaign_id,
                            c.admission.work_id,
                            c.admission.generation,
                        ));
                    }
                }
            }
        }
    }
}
