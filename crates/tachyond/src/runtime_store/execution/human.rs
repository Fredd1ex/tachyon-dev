//! Host-only attestation over a retained candidate. Never a worker question answer.
use super::command::{CommandGateRecord, COMMAND_GATES};
use super::*;
use sha2::{Digest, Sha256};
use tachyon_api::{
    campaign::{AcceptanceMode, AcceptanceReceipt, AcceptanceRequest, HumanDecision},
    types::{ApiRequest, ApiResponse, ArtifactPublication, CampaignStatus},
};
use tachyond::artifact_store::ArtifactStore;

const RECEIPTS: TableDefinition<&str, &[u8]> = TableDefinition::new("human_acceptance_receipts_v1");

pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    tx.open_table(RECEIPTS).map_err(error)?;
    Ok(())
}

fn now() -> Result<u64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(error)?
        .as_millis()
        .try_into()
        .map_err(error)
}

fn gate_in(tx: &WriteTransaction, work: &str) -> Result<CommandGateRecord, String> {
    serde_json::from_slice(
        tx.open_table(COMMAND_GATES)
            .map_err(error)?
            .get(work)
            .map_err(error)?
            .ok_or("no human acceptance gate")?
            .value(),
    )
    .map_err(error)
}

fn record_in(tx: &WriteTransaction, work: &str) -> Result<ExecutionRecord, String> {
    decode_record(
        tx.open_table(EXECUTIONS)
            .map_err(error)?
            .get(work)
            .map_err(error)?
            .ok_or("no retained execution")?
            .value(),
        work,
    )
}

fn save_gate(tx: &WriteTransaction, gate: &CommandGateRecord) -> Result<(), String> {
    tx.open_table(COMMAND_GATES)
        .map_err(error)?
        .insert(
            gate.policy.work.work_id.as_str(),
            serde_json::to_vec(gate).map_err(error)?.as_slice(),
        )
        .map_err(error)?;
    Ok(())
}

fn state_hash(
    tx: &WriteTransaction,
    record: &ExecutionRecord,
    gate: &CommandGateRecord,
) -> Result<String, String> {
    let mut binding = gate.clone();
    if let Some(request) = &mut binding.human_request {
        request.expected_state_sha256.clear();
    }
    let a = &record.policy.funding.admission;
    let funding = RuntimeStore::admitted_work_in(tx, &a.work_id)?;
    let verifier =
        RuntimeStore::admitted_work_in(tx, &record.policy.verification.admission.work_id)?;
    let revision = RuntimeStore::latest_instruction_revision_in(tx, a)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(record, binding, funding, verifier, revision)).map_err(error)?
        )
    ))
}

fn invalid_reason(
    tx: &WriteTransaction,
    record: &ExecutionRecord,
    gate: &CommandGateRecord,
) -> Result<Option<&'static str>, String> {
    let a = &record.policy.funding.admission;
    if matches!(
        RuntimeStore::campaign_status_in(tx, &a.campaign_id)?,
        CampaignStatus::Cancelling | CampaignStatus::Cancelled
    ) {
        return Ok(Some("human acceptance cancelled"));
    }
    if now()? >= gate.deadline_ms.unwrap_or(record.policy.work.deadline_ms) {
        return Ok(Some("human acceptance deadline expired"));
    }
    let ledger = RuntimeStore::campaign_ledger_in(tx, &a.campaign_id)?;
    for work in [&record.policy.funding, &record.policy.verification] {
        let current = RuntimeStore::admitted_work_in(tx, &work.admission.work_id)?;
        if current != *work
            || !matches!(current.state, DispatchState::Registered { .. })
            || RuntimeStore::group_cancelled_in(tx, work)?
            || ledger
                .reservations
                .get(&work.dispatch_id)
                .is_none_or(|r| r.cancellation_requested)
        {
            return Ok(Some(
                "human acceptance identity or cancellation fence changed",
            ));
        }
    }
    let verifier = &record.policy.verification.admission;
    if RuntimeStore::latest_instruction_revision_in(tx, verifier)? != verifier.instruction_revision
    {
        return Ok(Some("human acceptance verification superseded by steering"));
    }
    let latest = RuntimeStore::latest_instruction_revision_in(tx, a)?;
    if record
        .candidate
        .as_ref()
        .is_none_or(|c| c.instruction_revision.unwrap_or(a.instruction_revision) != latest)
    {
        return Ok(Some("human acceptance candidate superseded by steering"));
    }
    Ok(None)
}

fn finish_in(
    tx: &WriteTransaction,
    record: &ExecutionRecord,
    gate: &mut CommandGateRecord,
    evaluation: Evaluation,
    status: CampaignStatus,
    diagnostic: &str,
) -> Result<ExecutionRecord, String> {
    gate.finalize_rework = true;
    gate.human_diagnostic = Some(diagnostic.into());
    save_gate(tx, gate)?;
    let next = ExecutionRecord {
        phase: ExecutionPhase::Reviewed(evaluation),
        ..record.clone()
    };
    RuntimeStore::update_execution_in(tx, record, &next)?;
    RuntimeStore::group_terminal_in(tx, &record.policy.funding)?;
    RuntimeStore::group_terminal_in(tx, &record.policy.verification)?;
    let campaign = &record.policy.funding.admission.campaign_id;
    let status = if matches!(
        RuntimeStore::campaign_status_in(tx, campaign)?,
        CampaignStatus::Cancelling | CampaignStatus::Cancelled
    ) {
        CampaignStatus::Cancelled
    } else {
        status
    };
    RuntimeStore::set_campaign_status_in(tx, campaign, status)?;
    RuntimeStore::settle_campaign_execution_in(
        tx,
        &record.policy.funding.admission.campaign_id,
        &record.policy.work.work_id,
    )
}

impl RuntimeStore {
    /// Called on the blocking pool only after host publication and confirmed Ghost exit.
    pub(in crate::runtime_store) fn prepare_human_acceptance(
        &self,
        previous: &ExecutionRecord,
        artifacts: Option<&ArtifactStore>,
    ) -> Result<Option<ExecutionRecord>, String> {
        let campaign = &previous.policy.funding.admission.campaign_id;
        let work = &previous.policy.work.work_id;
        let Some(mut gate) = self.campaign_command_gate(campaign, work)? else {
            return Ok(None);
        };
        if gate.config.acceptance_mode != Some(AcceptanceMode::Human) {
            return Ok(None);
        }
        if previous.settled
            || !matches!(
                previous.phase,
                ExecutionPhase::EvidenceReady | ExecutionPhase::AwaitingVerification
            )
        {
            return Err("human acceptance requires known stopped candidate evidence".into());
        }
        if work != &format!("{campaign}-root") {
            return Err("human acceptance is root-only".into());
        }
        let config_hash = gate.config.config_hash()?;
        let selected = (|| {
            let candidate = previous.candidate.as_ref()?;
            if candidate.work_id != *work
                || candidate.generation != previous.policy.work.generation
                || candidate.assignment != previous.policy.work.assignment
                || !matches!(candidate.outcome, WorkOutcome::Completed { .. })
                || candidate
                    .attempt_id
                    .as_ref()
                    .is_some_and(|id| id != &previous.policy.model.identity.attempt_id)
            {
                return None;
            }
            let refs = candidate.candidate_refs.as_ref()?;
            if refs.len() != 1 {
                return None;
            }
            let artifacts = artifacts?;
            let snapshot = artifacts.metadata(work, &refs[0]).ok()??;
            if snapshot.id.is_empty()
                || snapshot.id.len() > 256
                || !snapshot
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
                || snapshot.publication
                    != (ArtifactPublication::Ready {
                        version: snapshot.sha256.clone(),
                    })
                || snapshot.size_bytes > gate.config.input_bytes
                || snapshot.work_id.as_ref() != Some(work)
                || snapshot.generation != Some(candidate.generation)
                || snapshot.assignment != Some(candidate.assignment)
                || snapshot.attempt_id.as_ref() != Some(&previous.policy.model.identity.attempt_id)
                || gate.snapshot.as_ref().is_some_and(|s| s != &snapshot)
            {
                return None;
            }
            // read verifies the full retained object digest, without exposing its contents.
            artifacts.read(work, &snapshot.id, 0, 1).ok()?;
            Some(snapshot)
        })();
        let Some(snapshot) = selected else {
            return self.host_finalize_command_rework(campaign, work).map(Some);
        };
        let tx = self.database.begin_write().map_err(error)?;
        if record_in(&tx, work)? != *previous
            || gate_in(&tx, work)? != gate
            || gate.policy != previous.policy
            || gate.policy.evaluator_id != format!("command:{config_hash}")
        {
            return Err("human acceptance preparation state conflict".into());
        }
        // The protected allocation stays open until a confirmed final decision.
        // This evaluator cannot spend, so its accounting receipt is final zero now,
        // independently of acceptance. No inference reservation remains while waiting.
        let receipt = evaluation_receipt(&previous.policy);
        Self::campaign_ledger_command_in(
            &tx,
            &receipt,
            campaign,
            LedgerCommand::ReserveAllocated {
                reservation_id: receipt.clone(),
                allocation_id: previous.policy.verification.dispatch_id.clone(),
                pool: Pool::Verification,
                reserved: previous.policy.verification.admission.upper_bound,
            },
        )?;
        Self::campaign_ledger_command_in(
            &tx,
            &receipt.replacen("evaluation:", "evaluation-final:", 1),
            campaign,
            LedgerCommand::Reconcile {
                reservation_id: receipt,
                usage: Usage::Final(Units::default()),
            },
        )?;
        gate.snapshot = Some(snapshot.clone());
        gate.finalize_rework = true;
        let pending = ExecutionRecord {
            phase: ExecutionPhase::AwaitingAcceptance,
            ..previous.clone()
        };
        Self::update_execution_in(&tx, previous, &pending)?;
        Self::group_review_pending_in(&tx, &previous.policy.funding)?;
        Self::group_review_pending_in(&tx, &previous.policy.verification)?;
        let revision = previous
            .candidate
            .as_ref()
            .unwrap()
            .instruction_revision
            .unwrap_or(previous.policy.funding.admission.instruction_revision);
        gate.human_request = Some(AcceptanceRequest {
            campaign_id: campaign.clone(),
            work_id: work.clone(),
            candidate: snapshot.id,
            candidate_sha256: snapshot.sha256,
            config_hash,
            generation: previous.policy.work.generation,
            assignment: previous.policy.work.assignment,
            instruction_revision: revision,
            deadline_ms: gate.deadline_ms.unwrap_or(previous.policy.work.deadline_ms),
            expected_state_sha256: String::new(),
        });
        let hash = state_hash(&tx, &pending, &gate)?;
        gate.human_request.as_mut().unwrap().expected_state_sha256 = hash;
        save_gate(&tx, &gate)?;
        let result = if let Some(reason) = invalid_reason(&tx, &pending, &gate)? {
            finish_in(
                &tx,
                &pending,
                &mut gate,
                Evaluation::Unverified,
                CampaignStatus::Unverified,
                reason,
            )?
        } else {
            Self::set_campaign_status_in(&tx, campaign, CampaignStatus::AwaitingAcceptance)?;
            pending
        };
        tx.commit().map_err(error)?;
        Ok(Some(result))
    }

    /// Only observes/finalizes known stopped work. Never launches, retries or calls a model.
    pub(crate) fn poll_human_acceptance(
        &self,
        campaign: &str,
        cancel: bool,
    ) -> Result<Option<ExecutionRecord>, String> {
        let work = format!("{campaign}-root");
        let Some(record) = self.campaign_execution(campaign, &work)? else {
            return Ok(None);
        };
        if record.phase != ExecutionPhase::AwaitingAcceptance {
            return Ok(Some(record));
        }
        let tx = self.database.begin_write().map_err(error)?;
        let record = record_in(&tx, &work)?;
        if record.phase != ExecutionPhase::AwaitingAcceptance {
            return Ok(Some(record));
        }
        let mut gate = gate_in(&tx, &work)?;
        let reason = if cancel {
            Some("human acceptance cancelled")
        } else {
            invalid_reason(&tx, &record, &gate)?
        };
        if let Some(reason) = reason {
            let next = finish_in(
                &tx,
                &record,
                &mut gate,
                Evaluation::Unverified,
                if cancel {
                    CampaignStatus::Cancelled
                } else {
                    CampaignStatus::Unverified
                },
                reason,
            )?;
            tx.commit().map_err(error)?;
            return Ok(Some(next));
        }
        Ok(Some(record))
    }

    pub(crate) fn acceptance_request(&self, api: &ApiRequest) -> Result<ApiResponse, String> {
        let campaign = match api {
            ApiRequest::CampaignAcceptanceGet(q) => &q.campaign_id,
            ApiRequest::CampaignAcceptanceDecide(d) => {
                d.validate()?;
                &d.campaign_id
            }
            _ => return Err("not a human acceptance request".into()),
        };
        if campaign.is_empty()
            || campaign.len() > 256
            || !campaign
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err("invalid campaign identifier".into());
        }
        self.poll_human_acceptance(campaign, false)?;
        let work = format!("{campaign}-root");
        let tx = self.database.begin_write().map_err(error)?;
        if let ApiRequest::CampaignAcceptanceDecide(decision) = api {
            if let Some(row) = tx
                .open_table(RECEIPTS)
                .map_err(error)?
                .get(decision.command_id.as_str())
                .map_err(error)?
            {
                let receipt: AcceptanceReceipt =
                    serde_json::from_slice(row.value()).map_err(error)?;
                if receipt.decision != *decision {
                    return Err("human decision command_id payload conflict".into());
                }
                return Ok(ApiResponse::CampaignAcceptance {
                    request: None,
                    receipt: Some(receipt),
                });
            }
        }
        let record = record_in(&tx, &work)?;
        let mut gate = gate_in(&tx, &work)?;
        if gate.config.acceptance_mode != Some(AcceptanceMode::Human)
            || gate.policy != record.policy
        {
            return Err("not a human acceptance gate".into());
        }
        let pending = record.phase == ExecutionPhase::AwaitingAcceptance;
        if let ApiRequest::CampaignAcceptanceGet(_) = api {
            return Ok(ApiResponse::CampaignAcceptance {
                request: if pending { gate.human_request } else { None },
                receipt: gate.human_receipt,
            });
        }
        let ApiRequest::CampaignAcceptanceDecide(decision) = api else {
            unreachable!()
        };
        if !pending
            || record.settled
            || gate.human_receipt.is_some()
            || invalid_reason(&tx, &record, &gate)?.is_some()
        {
            return Err("human acceptance no longer pending/current; inspect again".into());
        }
        let request = gate
            .human_request
            .clone()
            .ok_or("missing acceptance request")?;
        if decision.candidate != request.candidate
            || decision.candidate_sha256 != request.candidate_sha256
            || decision.expected_state_sha256 != request.expected_state_sha256
            || state_hash(&tx, &record, &gate)? != request.expected_state_sha256
            || gate.config.config_hash()? != request.config_hash
            || gate
                .snapshot
                .as_ref()
                .is_none_or(|s| s.id != request.candidate || s.sha256 != request.candidate_sha256)
            || record
                .candidate
                .as_ref()
                .and_then(|c| c.candidate_refs.as_deref())
                != Some(std::slice::from_ref(&request.candidate))
        {
            return Err("human acceptance candidate/version/state conflict".into());
        }
        let receipt = AcceptanceReceipt {
            request,
            decision: decision.clone(),
            source: AcceptanceMode::Human,
            host_uid: nix::unistd::Uid::effective().as_raw(),
            recorded_at_ms: now()?,
        };
        gate.human_receipt = Some(receipt.clone());
        let (evaluation, status) = match decision.decision {
            HumanDecision::Accept => (Evaluation::AcceptedHuman, CampaignStatus::AcceptedHuman),
            HumanDecision::Reject => (Evaluation::Rejected, CampaignStatus::Rejected),
        };
        finish_in(
            &tx,
            &record,
            &mut gate,
            evaluation,
            status,
            "explicit trusted same-user human attestation; not automated verification",
        )?;
        tx.open_table(RECEIPTS)
            .map_err(error)?
            .insert(
                decision.command_id.as_str(),
                serde_json::to_vec(&receipt).map_err(error)?.as_slice(),
            )
            .map_err(error)?;
        if now()? >= receipt.request.deadline_ms {
            return Err("human acceptance deadline expired before decision commit".into());
        }
        tx.commit().map_err(error)?;
        Ok(ApiResponse::CampaignAcceptance {
            request: None,
            receipt: Some(receipt),
        })
    }
}
