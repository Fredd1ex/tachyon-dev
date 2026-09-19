//! Explicit host invocation or approved host catalog. No registry auto-spawn or IPC grant.
#![allow(dead_code)]

use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use std::{future::Future, path::Path};
use tachyon_api::types::{AgentEvent, EventEnvelope, WorkOutcome, WorkRequest, WorkResult};
use tachyon_model::accounting::{RequestClass, RequestReservation};
use tokio::time::Instant;

pub(super) mod command;
pub(super) mod human;

use super::{
    admission::{AdmittedWork, DispatchState, PENDING, WORK},
    campaign_ledger::{LedgerCommand, Pool, Units, Usage},
    model_accounting::ModelBroker,
    RuntimeStore,
};

pub(super) const EXECUTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("campaign_executions");
pub(super) const EXECUTION_ERRORS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("campaign_execution_errors_v1");
pub(super) const VERIFIERS: TableDefinition<&str, &str> =
    TableDefinition::new("campaign_execution_verifiers");
const MAX_EVIDENCE: usize = 128 * 1024;

pub(super) fn evaluation_receipt(policy: &ExecutionPolicy) -> String {
    match &policy.work.attempt {
        Some(attempt) => format!(
            "evaluation:{}:{}",
            policy.verification.dispatch_id, attempt.id
        ),
        None => format!("evaluation:{}", policy.verification.dispatch_id),
    }
}

pub(super) fn initialize(write: &WriteTransaction) -> Result<(), String> {
    write.open_table(EXECUTIONS).map_err(error)?;
    write.open_table(EXECUTION_ERRORS).map_err(error)?;
    write.open_table(VERIFIERS).map_err(error)?;
    command::initialize(write)?;
    human::initialize(write)?;
    super::research_context::initialize(write)?;
    Ok(())
}

fn error(e: impl std::fmt::Display) -> String {
    format!("campaign execution: {e}")
}

/// Already authorized host configuration, never a worker-selected policy.
/// Verification is a separate registered Work in the same protected pool, not
/// a replacement model policy on the original Work. No model reviewer is called.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionPolicy {
    pub funding: AdmittedWork,
    pub model: RequestReservation,
    pub work: WorkRequest,
    pub verification: AdmittedWork,
    pub evaluator_id: String,
}

/// Acceptance under the configured evaluator is NOT a claim of correctness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Evaluation {
    Accepted,
    AcceptedHuman,
    Rejected,
    Unverified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ExecutionPhase {
    /// Includes crash, failed spawn, malformed transport and lost acknowledgement.
    /// Never replay the process. The launcher's claim is an additional fence.
    ExecutingUnknown,
    EvidenceReady,
    AwaitingVerification,
    AwaitingAcceptance,
    ReviewingUnknown,
    Reviewed(Evaluation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionRecord {
    #[serde(default)]
    pub stopping_snapshot: Option<tachyon_api::context::ResourceRef>,
    schema_version: u32,
    pub policy: ExecutionPolicy,
    pub phase: ExecutionPhase,
    pub candidate: Option<WorkResult>,
    pub settled: bool,
    /// Additive migration: legacy one-shot records never acquire retry authority.
    #[serde(default)]
    pub rework_pending: bool,
}

pub(super) fn decode_record(bytes: &[u8], key: &str) -> Result<ExecutionRecord, String> {
    let record: ExecutionRecord = serde_json::from_slice(bytes).map_err(error)?;
    if record.schema_version != 1
        || record.policy.work.work_id != key
        || record.policy.funding.admission.work_id != key
        || record
            .policy
            .work
            .attempt
            .as_ref()
            .is_some_and(|a| a.id != record.policy.model.identity.attempt_id)
        || (record.settled && !matches!(record.phase, ExecutionPhase::Reviewed(_)))
        || (record.rework_pending
            && (record.settled || record.phase != ExecutionPhase::Reviewed(Evaluation::Rejected)))
        || (matches!(
            record.phase,
            ExecutionPhase::EvidenceReady
                | ExecutionPhase::AwaitingVerification
                | ExecutionPhase::AwaitingAcceptance
                | ExecutionPhase::ReviewingUnknown
                | ExecutionPhase::Reviewed(
                    Evaluation::Accepted | Evaluation::AcceptedHuman | Evaluation::Rejected
                )
        ) && record.candidate.is_none())
    {
        return Err(error("invalid execution record schema/identity/state"));
    }
    Ok(record)
}

impl RuntimeStore {
    /// One bounded storage phase. Transactions and synchronous guards stay on the
    /// blocking worker; cancelling the waiter cannot undo a commit already begun.
    pub(super) async fn storage<T: Send + 'static>(
        self: &std::sync::Arc<Self>,
        operation: impl FnOnce(&Self) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || operation(&store))
            .await
            .map_err(error)?
    }
    pub(super) fn execution_owns_verifier_in(
        tx: &WriteTransaction,
        work: &AdmittedWork,
    ) -> Result<bool, String> {
        Ok(tx
            .open_table(VERIFIERS)
            .map_err(error)?
            .get(work.dispatch_id.as_str())
            .map_err(error)?
            .is_some())
    }

    /// Trusted recovery only: host has confirmed the original process terminated
    /// and recovered its complete bounded stdout. Never use a worker IPC report
    /// as proof of termination. Unknown billing is unaffected by this evidence.
    pub(crate) fn host_collect_campaign_evidence(
        &self,
        previous: &ExecutionRecord,
        events: Vec<EventEnvelope>,
    ) -> Result<ExecutionRecord, String> {
        self.collect_campaign_evidence(previous, events, None, None)
    }

    fn collect_campaign_evidence(
        &self,
        previous: &ExecutionRecord,
        events: Vec<EventEnvelope>,
        candidate_refs: Option<Vec<String>>,
        artifacts: Option<&tachyond::artifact_store::ArtifactStore>,
    ) -> Result<ExecutionRecord, String> {
        if previous.phase != ExecutionPhase::ExecutingUnknown {
            return Err(error("execution not awaiting evidence"));
        }
        let mut candidate = collected_candidate(previous, &events)?;
        let final_context = candidate.as_ref().and_then(|c| c.final_context.clone());
        candidate = candidate.filter(|c| matches!(c.outcome, WorkOutcome::Completed { .. }));
        if let Some(candidate) = &mut candidate {
            candidate.candidate_refs = candidate_refs;
        }
        let tx = self.database.begin_write().map_err(error)?;
        let initial = previous.policy.funding.admission.instruction_revision;
        let latest = Self::latest_instruction_revision_in(&tx, &previous.policy.funding.admission)?;
        let recognized =
            Self::recognized_instruction_revision_in(&tx, &previous.policy.funding.admission)?;
        // A worker revision is evidence metadata, not authority to apply steering.
        let candidate =
            candidate.filter(|c| recognized == Some(c.instruction_revision.unwrap_or(initial)));
        let mut next = ExecutionRecord {
            phase: if candidate
                .as_ref()
                .is_some_and(|c| c.instruction_revision.unwrap_or(initial) == latest)
            {
                ExecutionPhase::EvidenceReady
            } else {
                ExecutionPhase::Reviewed(Evaluation::Unverified)
            },
            candidate,
            ..previous.clone()
        };
        if let Err(failure) =
            self.record_stopping_context(tx, previous, &mut next, artifacts, final_context)
        {
            // A storage failure must not erase collected evidence or authorize continuation.
            next.stopping_snapshot = None;
            next.phase = ExecutionPhase::Reviewed(Evaluation::Unverified);
            let tx = self.database.begin_write().map_err(error)?;
            Self::update_execution_in(&tx, previous, &next)?;
            tx.open_table(EXECUTION_ERRORS)
                .map_err(error)?
                .insert(
                    next.policy.work.work_id.as_str(),
                    serde_json::to_vec(&(
                        &next.policy.model.identity.attempt_id,
                        format!("stopping snapshot unavailable: {failure}"),
                    ))
                    .map_err(error)?
                    .as_slice(),
                )
                .map_err(error)?;
            tx.commit().map_err(error)?;
        }
        Ok(next)
    }

    /// Authoritative completion of the configured non-spending evaluator, including
    /// explicit host recovery of a lost result. Does not execute or retry review.
    pub(crate) fn host_finish_campaign_evaluation(
        &self,
        previous: &ExecutionRecord,
        mut evaluation: Evaluation,
    ) -> Result<(), String> {
        if previous.phase != ExecutionPhase::ReviewingUnknown
            || evaluation == Evaluation::AcceptedHuman
        {
            return Err(error("review not awaiting evidence"));
        }
        let write = self.database.begin_write().map_err(error)?;
        let mut table = write.open_table(EXECUTIONS).map_err(error)?;
        let key = previous.policy.work.work_id.as_str();
        let current = decode_record(
            table
                .get(key)
                .map_err(error)?
                .ok_or_else(|| error("missing execution"))?
                .value(),
            key,
        )?;
        if current != *previous {
            return Err(error("review transition conflict"));
        }
        evaluation = command::gate_evaluation_in(&write, previous, evaluation)?;
        let admission = &previous.policy.funding.admission;
        let latest = Self::latest_instruction_revision_in(&write, admission)?;
        if Self::admitted_work_in(&write, &admission.work_id)?.admission != *admission
            || previous.candidate.as_ref().is_none_or(|c| {
                c.instruction_revision
                    .unwrap_or(admission.instruction_revision)
                    != latest
            })
        {
            evaluation = Evaluation::Unverified;
        }
        Self::campaign_ledger_command_in(
            &write,
            &evaluation_receipt(&previous.policy).replacen("evaluation:", "evaluation-final:", 1),
            &previous.policy.funding.admission.campaign_id,
            LedgerCommand::Reconcile {
                reservation_id: evaluation_receipt(&previous.policy),
                usage: Usage::Final(Units::default()),
            },
        )?;
        let next = ExecutionRecord {
            phase: ExecutionPhase::Reviewed(evaluation),
            ..previous.clone()
        };
        Self::group_terminal_in(&write, &previous.policy.verification)?;
        table
            .insert(key, serde_json::to_vec(&next).map_err(error)?.as_slice())
            .map_err(error)?;
        drop(table);
        write.commit().map_err(error)
    }

    pub(crate) fn campaign_execution(
        &self,
        campaign: &str,
        work: &str,
    ) -> Result<Option<ExecutionRecord>, String> {
        let read = self.database.begin_read().map_err(error)?;
        let table = read.open_table(EXECUTIONS).map_err(error)?;
        let record: Option<ExecutionRecord> = table
            .get(work)
            .map_err(error)?
            .map(|v| decode_record(v.value(), work))
            .transpose()?;
        if record
            .as_ref()
            .is_some_and(|r| r.policy.funding.admission.campaign_id != campaign)
        {
            return Err(error("campaign scope mismatch"));
        }
        Ok(record)
    }

    /// Compare-and-set also protects against concurrent review/resume callers.
    pub(super) fn update_execution_in(
        write: &WriteTransaction,
        previous: &ExecutionRecord,
        next: &ExecutionRecord,
    ) -> Result<(), String> {
        let mut table = write.open_table(EXECUTIONS).map_err(error)?;
        let key = previous.policy.work.work_id.as_str();
        let current = decode_record(
            table
                .get(key)
                .map_err(error)?
                .ok_or_else(|| error("missing execution"))?
                .value(),
            key,
        )?;
        if current != *previous {
            return Err(error("execution transition conflict"));
        }
        if (previous.phase == ExecutionPhase::ExecutingUnknown
            && matches!(
                next.phase,
                ExecutionPhase::EvidenceReady | ExecutionPhase::Reviewed(_)
            ))
            || (matches!(
                previous.phase,
                ExecutionPhase::EvidenceReady | ExecutionPhase::AwaitingVerification
            ) && matches!(next.phase, ExecutionPhase::Reviewed(_)))
        {
            // Unverified attempts cannot repair. Known cleanup ends their Work
            // lifecycle even when an unknown charge still prevents settlement.
            if next.phase != ExecutionPhase::Reviewed(Evaluation::Unverified)
                && write
                    .open_table(command::COMMAND_GATES)
                    .map_err(error)?
                    .get(key)
                    .map_err(error)?
                    .is_some()
            {
                Self::group_review_pending_in(write, &previous.policy.funding)?;
            } else {
                Self::group_terminal_in(write, &previous.policy.funding)?;
            }
            if matches!(next.phase, ExecutionPhase::Reviewed(_)) {
                let mut verifier = Self::admitted_work_in(
                    &write,
                    &previous.policy.verification.admission.work_id,
                )?;
                if verifier.state == DispatchState::Admitted {
                    Self::campaign_ledger_command_in(
                        &write,
                        &format!("unspent:{}", verifier.dispatch_id),
                        &verifier.admission.campaign_id,
                        LedgerCommand::Reconcile {
                            reservation_id: verifier.dispatch_id.clone(),
                            usage: Usage::Final(Units::default()),
                        },
                    )?;
                    verifier.state = DispatchState::Cancelled;
                    write
                        .open_table(WORK)
                        .map_err(error)?
                        .insert(
                            verifier.admission.work_id.as_str(),
                            serde_json::to_vec(&verifier).map_err(error)?.as_slice(),
                        )
                        .map_err(error)?;
                    write
                        .open_table(PENDING)
                        .map_err(error)?
                        .remove(verifier.admission.work_id.as_str())
                        .map_err(error)?;
                }
                Self::group_terminal_in(&write, &previous.policy.verification)?;
            }
        }
        table
            .insert(key, serde_json::to_vec(next).map_err(error)?.as_slice())
            .map_err(error)?;
        drop(table);
        if previous.phase != next.phase
            && matches!(
                next.phase,
                ExecutionPhase::AwaitingAcceptance | ExecutionPhase::AwaitingVerification
            )
        {
            super::campaign_oversight::trigger_in(
                write,
                &next.policy.funding.admission.campaign_id,
                "blocked_state",
            )?;
        }
        Ok(())
    }

    /// Resume accounting only, with no launch or evaluator replay. Closure and the
    /// durable settled bit commit together, only after both pools have final holds.
    pub(crate) fn settle_campaign_execution(
        &self,
        campaign: &str,
        work: &str,
    ) -> Result<ExecutionRecord, String> {
        let write = self.database.begin_write().map_err(error)?;
        let record = Self::settle_campaign_execution_in(&write, campaign, work)?;
        write.commit().map_err(error)?;
        Ok(record)
    }

    pub(super) fn settle_campaign_execution_in(
        write: &WriteTransaction,
        campaign: &str,
        work: &str,
    ) -> Result<ExecutionRecord, String> {
        let mut table = write.open_table(EXECUTIONS).map_err(error)?;
        let mut record = decode_record(
            table
                .get(work)
                .map_err(error)?
                .ok_or_else(|| error("missing execution"))?
                .value(),
            work,
        )?;
        if record.policy.funding.admission.campaign_id != campaign {
            return Err(error("campaign scope mismatch"));
        }
        if !record.settled && matches!(record.phase, ExecutionPhase::Reviewed(_)) {
            let ids = [
                &record.policy.funding.dispatch_id,
                &record.policy.verification.dispatch_id,
            ];
            let ledger = Self::campaign_ledger_in(&write, campaign)?;
            if ledger.reservations.values().any(|r| {
                r.allocation.as_ref().is_some_and(|id| ids.contains(&id))
                    && !matches!(r.usage, Usage::Final(_))
            }) {
                return Ok(record);
            }
            if command::retain_rejected_in(&write, &record)? {
                record.rework_pending = true;
                table
                    .insert(work, serde_json::to_vec(&record).map_err(error)?.as_slice())
                    .map_err(error)?;
                return Ok(record);
            }
            // Local stopped Work can retain its original open allocations. This
            // never reopens settled money and is not an automatic launch grant.
            if record.phase == ExecutionPhase::Reviewed(Evaluation::Unverified)
                && record.policy.work.deadline_ms
                    > std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(error)?
                        .as_millis() as u64
                && ids.iter().all(|id| {
                    ledger.allocations.get(*id) == Some(&false)
                        && ledger
                            .reservations
                            .get(*id)
                            .is_some_and(|r| !r.cancellation_requested)
                })
            {
                let local = write
                    .open_table(TableDefinition::<&str, &[u8]>::new(
                        "local_campaign_launches_v1",
                    ))
                    .map_err(error)?;
                let gates = write.open_table(command::COMMAND_GATES).map_err(error)?;
                if local.get(campaign).map_err(error)?.is_some()
                    && gates.get(work).map_err(error)?.is_some_and(|row| {
                        serde_json::from_slice::<command::CommandGateRecord>(row.value())
                            .is_ok_and(|g| !g.finalize_rework)
                    })
                {
                    return Ok(record);
                }
            }
            record.rework_pending = false;
            if ids.iter().any(|id| {
                !ledger.allocations.contains_key(*id)
                    && !ledger
                        .reservations
                        .get(*id)
                        .is_some_and(|r| matches!(r.usage, Usage::Final(_)))
            }) {
                return Ok(record);
            }
            for id in ids {
                if !ledger.allocations.contains_key(id) {
                    continue;
                }
                Self::campaign_ledger_command_in(
                    &write,
                    &format!("execution-close:{id}"),
                    campaign,
                    LedgerCommand::CloseAllocation {
                        reservation_id: id.clone(),
                    },
                )?;
            }
            record.settled = true;
            if let Some(candidate) = &record.candidate {
                let category = match candidate.outcome {
                    tachyon_api::WorkOutcome::Failed { .. } => {
                        Some(tachyon_api::attention::AttentionCategory::WorkFailed)
                    }
                    tachyon_api::WorkOutcome::TimedOut { .. } => {
                        Some(tachyon_api::attention::AttentionCategory::WorkTimedOut)
                    }
                    _ => None,
                };
                let admission = &record.policy.funding.admission;
                let revision = Self::latest_instruction_revision_in(write, admission)?;
                if let Some(category) =
                    category.filter(|_| candidate.instruction_revision == Some(revision))
                {
                    Self::admit_attention_in(
                        write,
                        super::attention::AttentionSource {
                            scope: tachyon_api::todo::TodoScope::Campaign {
                                campaign_id: campaign.into(),
                            },
                            campaign_id: Some(campaign.into()),
                            work_id: Some(work.into()),
                            generation: admission.generation,
                            instruction_revision: revision,
                            category,
                            cause_id: format!("terminal:{}", candidate.assignment),
                        },
                        crate::unix_now_ms(),
                    )?;
                }
            }
            Self::group_terminal_in(&write, &record.policy.funding)?;
            table
                .insert(work, serde_json::to_vec(&record).map_err(error)?.as_slice())
                .map_err(error)?;
        }
        drop(table);
        Ok(record)
    }
}

impl ModelBroker {
    /// Functional internal runner for an explicitly registered assignment. Host
    /// supplies a deterministic, non-spending, cooperative async evaluator; this
    /// is NOT a command sandbox or an LLM reviewer. Dropping its future must stop
    /// evaluation. Arbitrary blocking callbacks cannot be deadline-enforced here.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_campaign<F, Fut>(
        &self,
        executable: &Path,
        workspace: &Path,
        home: &Path,
        policy: ExecutionPolicy,
        deadline: Instant,
        evaluate: F,
    ) -> Result<ExecutionRecord, String>
    where
        F: FnOnce(WorkResult) -> Fut,
        Fut: Future<Output = Evaluation>,
    {
        self.execute_campaign_with_artifacts(
            executable, workspace, home, policy, deadline, None, evaluate,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_campaign_with_artifacts<F, Fut>(
        &self,
        executable: &Path,
        workspace: &Path,
        home: &Path,
        policy: ExecutionPolicy,
        deadline: Instant,
        artifacts: Option<std::sync::Arc<tachyond::artifact_store::ArtifactStore>>,
        evaluate: F,
    ) -> Result<ExecutionRecord, String>
    where
        F: FnOnce(WorkResult) -> Fut,
        Fut: Future<Output = Evaluation>,
    {
        self.execute_campaign_attempt(
            executable, workspace, home, policy, deadline, artifacts, false, evaluate,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_campaign_attempt<F, Fut>(
        &self,
        executable: &Path,
        workspace: &Path,
        home: &Path,
        policy: ExecutionPolicy,
        deadline: Instant,
        artifacts: Option<std::sync::Arc<tachyond::artifact_store::ArtifactStore>>,
        prepared: bool,
        evaluate: F,
    ) -> Result<ExecutionRecord, String>
    where
        F: FnOnce(WorkResult) -> Fut,
        Fut: Future<Output = Evaluation>,
    {
        // Resume must not extend the original assignment's deadline either.
        let now = Instant::now();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(error)?
            .as_millis();
        let remaining_ms = u128::from(policy.work.deadline_ms).saturating_sub(now_ms);
        let deadline = deadline.min(
            now.checked_add(std::time::Duration::from_millis(remaining_ms as u64))
                .ok_or_else(|| error("invalid execution deadline"))?,
        );
        let campaign = &policy.funding.admission.campaign_id;
        let key = &policy.work.work_id;
        let paths_absolute =
            executable.is_absolute() && workspace.is_absolute() && home.is_absolute();
        let lookup_campaign = campaign.clone();
        let lookup_work = key.clone();
        let existing = self
            .store
            .storage(move |store| store.campaign_execution(&lookup_campaign, &lookup_work))
            .await?;
        // Capacity exhaustion is admission waiting, not evidence that a worker
        // was launched. Do not write ExecutingUnknown until resident and active slots exist.
        // The transaction below still rechecks policy and one-shot ownership.
        let mut host_launch = if existing.is_none() || prepared {
            Some(
                self.admit_launch(&policy.funding, &policy.work, deadline)
                    .await
                    .map_err(error)?,
            )
        } else {
            None
        };
        let initial_policy = policy.clone();
        let (mut record, launch) = self
            .store
            .storage(move |store| {
                let policy = initial_policy;
                let campaign = &policy.funding.admission.campaign_id;
                let key = &policy.work.work_id;
                let write = store.database.begin_write().map_err(error)?;
                let mut table = write.open_table(EXECUTIONS).map_err(error)?;
                let existing: Option<ExecutionRecord> = table
                    .get(key.as_str())
                    .map_err(error)?
                    .map(|v| decode_record(v.value(), key))
                    .transpose()?;
                if let Some(record) = existing {
                    if record.policy != policy {
                        return Err(error("execution policy conflict"));
                    }
                    if prepared && record.phase != ExecutionPhase::ExecutingUnknown {
                        return Err(error("prepared attempt is no longer awaiting launch"));
                    }
                    Ok((record, prepared))
                } else {
                    let a = &policy.funding.admission;
                    let v = &policy.verification.admission;
                    let i = &policy.model.identity;
                    if a.pool != Pool::Work
                        || v.pool != Pool::Verification
                        || a.campaign_id != v.campaign_id
                        || a.work_id == v.work_id
                        || policy.work.work_id != a.work_id
                        || policy.work.objective != a.objective
                        || policy.work.generation != a.generation
                        || policy.work.assignment == 0
                        || policy
                            .work
                            .attempt
                            .as_ref()
                            .is_some_and(|a| a.id != i.attempt_id)
                        || i.work_id != a.work_id
                        || i.campaign_id != a.campaign_id
                        || i.generation != a.generation
                        || i.instruction_revision != a.instruction_revision
                        || i.class != RequestClass::Work
                        || policy.evaluator_id.trim().is_empty()
                        || policy.evaluator_id.len() > 256
                        || deadline <= Instant::now()
                        || !paths_absolute
                    {
                        return Err(error("invalid execution policy"));
                    }
                    let (tokens, cost) = policy.model.estimate.upper_bound().map_err(error)?;
                    if tokens > a.upper_bound.tokens || cost > a.upper_bound.cost_micro_usd {
                        return Err(error("model request exceeds work allocation"));
                    }
                    let ledger = RuntimeStore::campaign_ledger_in(&write, campaign)?;
                    if ledger
                        .allocations
                        .contains_key(&policy.verification.dispatch_id)
                    {
                        return Err(error("verification requires unused registered reservation"));
                    }
                    let mut owners = write.open_table(VERIFIERS).map_err(error)?;
                    if owners
                        .get(policy.verification.dispatch_id.as_str())
                        .map_err(error)?
                        .is_some()
                    {
                        return Err(error("verification allocation already assigned"));
                    }
                    for funding in [&policy.funding, &policy.verification] {
                        if RuntimeStore::admitted_work_in(&write, &funding.admission.work_id)?
                            != *funding
                        {
                            return Err(error("requires current registered admission"));
                        }
                        if funding == &policy.verification
                            && funding.state == DispatchState::Admitted
                        {
                            let hold = ledger
                                .reservations
                                .get(&funding.dispatch_id)
                                .ok_or_else(|| error("missing verifier reservation"))?;
                            if hold.cancellation_requested
                                || hold.usage != Usage::Unknown
                                || hold.pool != v.pool
                                || hold.reserved != v.upper_bound
                            {
                                return Err(error("invalid queued verifier reservation"));
                            }
                            // Own the queued verifier before launch; generic dispatch must not run it.
                            continue;
                        }
                        RuntimeStore::admitted_funding_in(&write, funding)?;
                        RuntimeStore::campaign_ledger_command_in(
                            &write,
                            &format!("fund:{}", funding.dispatch_id),
                            campaign,
                            LedgerCommand::FundAllocation {
                                reservation_id: funding.dispatch_id.clone(),
                                work_id: funding.admission.work_id.clone(),
                            },
                        )?;
                    }
                    owners
                        .insert(policy.verification.dispatch_id.as_str(), key.as_str())
                        .map_err(error)?;
                    let record = ExecutionRecord {
                        stopping_snapshot: None,
                        schema_version: 1,
                        policy: policy.clone(),
                        phase: ExecutionPhase::ExecutingUnknown,
                        candidate: None,
                        settled: false,
                        rework_pending: false,
                    };
                    table
                        .insert(
                            key.as_str(),
                            serde_json::to_vec(&record).map_err(error)?.as_slice(),
                        )
                        .map_err(error)?;
                    drop(owners);
                    drop(table);
                    write.commit().map_err(error)?;
                    Ok((record, true))
                }
            })
            .await?;
        if launch {
            // No DB transaction or registry/authority lock crosses this await.
            let events = self
                .launch_private_admitted(
                    executable,
                    workspace,
                    home,
                    policy.funding.clone(),
                    policy.model.clone(),
                    policy.work.clone(),
                    deadline,
                    host_launch.take(),
                )
                .await;
            let events = match events {
                Ok(events) => events,
                Err(launch_error) => {
                    let message = if launch_error
                        .to_string()
                        .contains("continuation configuration mismatch")
                    {
                        "continuation configuration mismatch: executable, Ghost version or package version"
                    } else {
                        "local launch failed; execution outcome may be unknown; operator reconciliation required"
                    };
                    let work = policy.work.work_id.clone();
                    let attempt = policy.model.identity.attempt_id.clone();
                    self.store
                        .storage(move |store| {
                            let tx = store.database.begin_write().map_err(error)?;
                            tx.open_table(EXECUTION_ERRORS)
                                .map_err(error)?
                                .insert(
                                    work.as_str(),
                                    serde_json::to_vec(&(attempt, message))
                                        .map_err(error)?
                                        .as_slice(),
                                )
                                .map_err(error)?;
                            tx.commit().map_err(error)
                        })
                        .await?;
                    return Ok(record);
                }
            };
            let mut candidate_refs = None;
            if let Some(artifacts) = artifacts.clone() {
                let previous = record.clone();
                let collected_events = events.clone();
                let candidate = self
                    .store
                    .storage(move |_| collected_candidate(&previous, &collected_events))
                    .await?;
                let workspace = workspace.to_owned();
                let attempt = policy.model.identity.attempt_id.clone();
                let DispatchState::Registered { worker_id } = &policy.funding.state else {
                    return Err(error("missing worker registration"));
                };
                if let Some(candidate) =
                    candidate.filter(|c| matches!(c.outcome, WorkOutcome::Completed { .. }))
                {
                    artifacts.require_retained(&self.store.retained, campaign)?;
                    candidate_refs = tachyond::artifact_store::publish_candidate(
                        artifacts,
                        workspace,
                        attempt,
                        worker_id.clone(),
                        candidate,
                        events.clone(),
                        deadline,
                    )
                    .await
                    .candidate_refs;
                }
            }
            let snapshot_artifacts = artifacts.clone();
            record = self
                .store
                .storage(move |store| {
                    store.collect_campaign_evidence(
                        &record,
                        events,
                        candidate_refs,
                        snapshot_artifacts.as_deref(),
                    )
                })
                .await?;
        }
        if matches!(
            record.phase,
            ExecutionPhase::EvidenceReady | ExecutionPhase::AwaitingVerification
        ) {
            let previous = record.clone();
            if let Some(pending) = self
                .store
                .storage(move |store| {
                    store.prepare_human_acceptance(&previous, artifacts.as_deref())
                })
                .await?
            {
                return Ok(pending);
            }
        }
        if matches!(
            record.phase,
            ExecutionPhase::EvidenceReady | ExecutionPhase::AwaitingVerification
        ) {
            let next = ExecutionRecord {
                phase: ExecutionPhase::ReviewingUnknown,
                ..record.clone()
            };
            // Reserve before callback and atomically claim the review. A callback
            // panic/cancellation leaves this hold unknown, never silently refunded.
            let receipt = evaluation_receipt(&policy);
            let review_policy = policy.clone();
            record = self
                .store
                .storage(move |store| {
                    let policy = review_policy;
                    let campaign = &policy.funding.admission.campaign_id;
                    let key = &policy.work.work_id;
                    let write = store.database.begin_write().map_err(error)?;
                    let mut table = write.open_table(EXECUTIONS).map_err(error)?;
                    let current = decode_record(
                        table
                            .get(key.as_str())
                            .map_err(error)?
                            .ok_or_else(|| error("missing execution"))?
                            .value(),
                        key,
                    )?;
                    if current != record {
                        return Err(error("review already claimed"));
                    }
                    let mut verifier = RuntimeStore::admitted_work_in(
                        &write,
                        &policy.verification.admission.work_id,
                    )?;
                    if verifier.admission != policy.verification.admission
                        || verifier.dispatch_id != policy.verification.dispatch_id
                    {
                        return Err(error("verifier identity conflict"));
                    }
                    // Cancellation can commit while this runner waits for review
                    // capacity. Check under the claim writer, before funding a
                    // cancelled verifier; no evaluator has run at this phase.
                    if RuntimeStore::group_cancelled_in(&write, &policy.funding)?
                        || RuntimeStore::group_cancelled_in(&write, &verifier)?
                    {
                        let next = ExecutionRecord {
                            phase: ExecutionPhase::Reviewed(Evaluation::Unverified),
                            ..record.clone()
                        };
                        drop(table);
                        RuntimeStore::update_execution_in(&write, &record, &next)?;
                        RuntimeStore::group_terminal_in(&write, &policy.funding)?;
                        let record =
                            RuntimeStore::settle_campaign_execution_in(&write, campaign, key)?;
                        write.commit().map_err(error)?;
                        return Ok(record);
                    }
                    let allocation_action =
                        RuntimeStore::allocation_review_claim_in(&write, &current)?;
                    if verifier.state == DispatchState::Admitted {
                        let ledger = RuntimeStore::campaign_ledger_in(&write, campaign)?;
                        let hold = ledger
                            .reservations
                            .get(&verifier.dispatch_id)
                            .ok_or_else(|| error("missing verifier hold"))?;
                        if hold.cancellation_requested || hold.usage != Usage::Unknown {
                            return Err(error("verifier hold is not executable"));
                        }
                        if ledger.admissions_paused
                            || !RuntimeStore::group_claim_in(&write, &verifier)?
                        {
                            record.phase = ExecutionPhase::AwaitingVerification;
                            table
                                .insert(
                                    key.as_str(),
                                    serde_json::to_vec(&record).map_err(error)?.as_slice(),
                                )
                                .map_err(error)?;
                            drop(table);
                            write.commit().map_err(error)?;
                            return Ok(record);
                        }
                        verifier.state = DispatchState::Registered {
                            worker_id: policy.evaluator_id.clone(),
                        };
                        write
                            .open_table(WORK)
                            .map_err(error)?
                            .insert(
                                verifier.admission.work_id.as_str(),
                                serde_json::to_vec(&verifier).map_err(error)?.as_slice(),
                            )
                            .map_err(error)?;
                        write
                            .open_table(PENDING)
                            .map_err(error)?
                            .remove(verifier.admission.work_id.as_str())
                            .map_err(error)?;
                        RuntimeStore::campaign_ledger_command_in(
                            &write,
                            &format!("fund:{}", verifier.dispatch_id),
                            campaign,
                            LedgerCommand::FundAllocation {
                                reservation_id: verifier.dispatch_id.clone(),
                                work_id: verifier.admission.work_id.clone(),
                            },
                        )?;
                    }
                    if policy.work.attempt.is_some()
                        && RuntimeStore::group_funding_in(&write, &verifier).is_err()
                        && !RuntimeStore::group_repair_claim_in(&write, &verifier)?
                    {
                        record.phase = ExecutionPhase::AwaitingVerification;
                        table
                            .insert(
                                key.as_str(),
                                serde_json::to_vec(&record).map_err(error)?.as_slice(),
                            )
                            .map_err(error)?;
                        drop(table);
                        write.commit().map_err(error)?;
                        return Ok(record);
                    }
                    RuntimeStore::admitted_funding_in(&write, &verifier)?;
                    RuntimeStore::campaign_ledger_command_in(
                        &write,
                        &receipt,
                        campaign,
                        LedgerCommand::ReserveAllocated {
                            reservation_id: receipt.clone(),
                            allocation_id: policy.verification.dispatch_id.clone(),
                            pool: Pool::Verification,
                            reserved: policy.verification.admission.upper_bound,
                        },
                    )?;
                    table
                        .insert(
                            key.as_str(),
                            serde_json::to_vec(&next).map_err(error)?.as_slice(),
                        )
                        .map_err(error)?;
                    drop(table);
                    if let Some(context) = allocation_action {
                        RuntimeStore::finish_allocation_action_in(&write, &context)?;
                    }
                    write.commit().map_err(error)?;
                    Ok(next)
                })
                .await?;
            if record.phase != ExecutionPhase::ReviewingUnknown {
                return Ok(record);
            }
            let candidate = record.candidate.clone().unwrap();
            let mut evaluation = if Instant::now() >= deadline {
                Evaluation::Unverified
            } else {
                let key = (
                    campaign.clone(),
                    policy.work.work_id.clone(),
                    policy.work.generation,
                );
                let mut cancellation = self
                    .launches
                    .lock()
                    .map_err(|_| error("launch registry unavailable"))?
                    .get(&key)
                    .map(|sender| sender.subscribe());
                tokio::select! {
                    biased;
                    _ = async {
                        if let Some(receiver) = &mut cancellation {
                            if !*receiver.borrow() { let _ = receiver.changed().await; }
                        } else { std::future::pending::<()>().await; }
                    } => Evaluation::Unverified,
                    result = tokio::time::timeout_at(deadline, evaluate(candidate)) => result.unwrap_or(Evaluation::Unverified),
                }
            };
            if Instant::now() >= deadline {
                evaluation = Evaluation::Unverified;
            }
            // A deterministic evaluator makes no provider call. This zero is host
            // knowledge, never usage copied from WorkResult or worker telemetry.
            self.store
                .storage(move |store| store.host_finish_campaign_evaluation(&record, evaluation))
                .await?;
        }
        let (campaign, key) = (campaign.clone(), key.clone());
        self.store
            .storage(move |store| store.settle_campaign_execution(&campaign, &key))
            .await
    }
}

fn collected_candidate(
    previous: &ExecutionRecord,
    events: &[EventEnvelope],
) -> Result<Option<WorkResult>, String> {
    let DispatchState::Registered { worker_id } = &previous.policy.funding.state else {
        return Err(error("missing worker registration"));
    };
    if serde_json::to_vec(events).map_err(error)?.len() > tachyon_model::broker::MAX_FRAME
        || events.iter().any(|e| {
            e.session_id != *worker_id
                || e.task_id.as_ref() != Some(worker_id)
                || !matches!(&e.actor, tachyon_api::types::Actor::Worker { id } if id == worker_id)
                || e.conversation_id.is_some()
                || e.parent_task_id.is_some()
        })
    {
        return Err(error("invalid collected evidence scope/bound"));
    }
    Ok(terminal_observation(&previous.policy.work, events.to_vec()))
}

#[cfg(test)]
fn terminal_candidate(work: &WorkRequest, events: Vec<EventEnvelope>) -> Option<WorkResult> {
    terminal_observation(work, events)
        .filter(|c| matches!(c.outcome, WorkOutcome::Completed { .. }))
}

fn terminal_observation(work: &WorkRequest, events: Vec<EventEnvelope>) -> Option<WorkResult> {
    let mut candidates = events.into_iter().filter_map(|e| match e.kind {
        AgentEvent::WorkCandidate { candidate } => Some(candidate),
        _ => None,
    });
    let mut candidate = candidates.next()?;
    if candidates.next().is_some()
        || candidate.work_id != work.work_id
        || candidate.objective != work.objective
        || candidate.generation != work.generation
        || candidate.assignment != work.assignment
        || candidate.attempt_id.as_deref() != work.attempt.as_ref().map(|a| a.id.as_str())
        || serde_json::to_vec(&candidate).ok()?.len() > MAX_EVIDENCE
    {
        return None;
    }
    // Timing is untrusted display data, not evaluation evidence or billing.
    candidate.timing = None;
    candidate.candidate_refs = None;
    Some(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_candidate_requires_exact_bounded_single_completed_assignment() {
        let work: WorkRequest = serde_json::from_value(serde_json::json!({
            "work_id":"work", "objective":"objective", "generation":7, "assignment":3,
            "deadline_ms":100, "lifetime_class":"short"
        }))
        .unwrap();
        let event = serde_json::json!({
            "event_id":1, "sequence":1, "occurred_at_ms":0, "session_id":"worker", "task_id":"worker",
            "actor":{"kind":"worker", "id":"worker"}, "kind":"work_candidate",
            "candidate":{"work_id":"work", "objective":"objective", "generation":7, "assignment":3, "outcome":"completed", "result":"ok", "candidate_refs":["forged"]}
        });
        let decode = |e| serde_json::from_value::<EventEnvelope>(e).unwrap();
        assert!(terminal_candidate(&work, vec![decode(event.clone())])
            .unwrap()
            .candidate_refs
            .is_none());
        assert!(terminal_candidate(&work, vec![]).is_none());
        assert!(
            terminal_candidate(&work, vec![decode(event.clone()), decode(event.clone())]).is_none()
        );
        for (field, value) in [
            ("work_id", serde_json::json!("other")),
            ("objective", serde_json::json!("other")),
            ("generation", serde_json::json!(8)),
            ("assignment", serde_json::json!(4)),
            ("attempt_id", serde_json::json!("another-attempt")),
            ("result", serde_json::json!("x".repeat(MAX_EVIDENCE))),
        ] {
            let mut forged = event.clone();
            forged["candidate"][field] = value;
            assert!(terminal_candidate(&work, vec![decode(forged)]).is_none());
        }
        let mut attempted = work.clone();
        attempted.attempt = Some(tachyon_api::types::WorkAttempt {
            continuation: None,
            id: "attempt".into(),
            feedback: None,
        });
        assert!(terminal_candidate(&attempted, vec![decode(event.clone())]).is_none());
        let mut scoped = event.clone();
        scoped["candidate"]["attempt_id"] = serde_json::json!("attempt");
        assert!(terminal_candidate(&attempted, vec![decode(scoped)]).is_some());
        let mut failed = event;
        failed["candidate"]["outcome"] = serde_json::json!("failed");
        failed["candidate"]["message"] = serde_json::json!("failure");
        assert!(terminal_candidate(&work, vec![decode(failed)]).is_none());
    }
}
