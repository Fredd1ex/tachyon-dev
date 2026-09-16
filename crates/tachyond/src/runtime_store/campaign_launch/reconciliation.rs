//! Local privileged attestation. No provider transport, process killing or retry.
use super::*;
use crate::runtime_store::{
    admission::{AdmittedWork, DispatchState, PENDING, WORK},
    campaign_ledger::{LedgerCommand, Usage},
    execution::{self, command::COMMAND_GATES, EXECUTIONS},
    model_accounting::{Record, REQUESTS},
};
use redb::{ReadableTableMetadata, WriteTransaction};
use sha2::{Digest, Sha256};
use tachyon_api::campaign::{ReconciliationReceipt, RecoveryRecord};
use tachyon_model::accounting::RequestUsage;

const RECEIPTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("local_reconciliation_receipts_v1");
const BINDINGS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("local_reconciliation_bindings_v1");

// Conservative global CAS: unrelated campaign changes may require reinspection.
// Length framing prevents ambiguous concatenations. No secrets are returned.
pub(super) fn state_hash(tx: &WriteTransaction) -> Result<String, String> {
    let mut hash = Sha256::new();
    for name in [
        "campaigns",
        "local_campaign_launches_v1",
        "campaign_ledger_roots",
        "campaign_admitted_work",
        "campaign_executions",
        "campaign_execution_errors_v1",
        "campaign_command_gates_v1",
        "campaign_model_requests",
        "campaign_model_dispatches",
        "campaign_work_limits",
        "campaign_coordination_v1",
        "local_reconciliation_receipts_v1",
        "local_reconciliation_bindings_v1",
        "local_continuation_claims_v1",
        "native_jobs_v1",
    ] {
        hash.update((name.len() as u64).to_be_bytes());
        hash.update(name.as_bytes());
        let table = tx
            .open_table(TableDefinition::<&str, &[u8]>::new(name))
            .map_err(err)?;
        hash.update(table.len().map_err(err)?.to_be_bytes());
        for row in table.iter().map_err(err)? {
            let (key, value) = row.map_err(err)?;
            for bytes in [key.value().as_bytes(), value.value()] {
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(bytes);
            }
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}

impl CampaignService {
    pub(super) fn reconciliation_inspection(&self, id: &str) -> Result<Vec<String>, String> {
        let tx = self.store.database.begin_write().map_err(err)?;
        let mut diagnostics = vec![format!("expected_state_sha256: {}", state_hash(&tx)?)];
        diagnostics.extend(RuntimeStore::inspect_jobs_in(&tx, id)?);
        if let Some(ledger) = self.store.campaign_ledger(id)? {
            diagnostics.push(format!(
                "ledger: {}",
                serde_json::to_string(&ledger).map_err(err)?
            ));
        }
        for row in tx.open_table(WORK).map_err(err)?.iter().map_err(err)? {
            let (_, value) = row.map_err(err)?;
            let work: AdmittedWork = serde_json::from_slice(value.value()).map_err(err)?;
            if work.admission.campaign_id == id {
                diagnostics.push(format!(
                    "work: {}",
                    serde_json::to_string(&work).map_err(err)?
                ));
            }
        }
        for row in tx
            .open_table(EXECUTIONS)
            .map_err(err)?
            .iter()
            .map_err(err)?
        {
            let (key, value) = row.map_err(err)?;
            let record = execution::decode_record(value.value(), key.value())?;
            if record.policy.funding.admission.campaign_id == id {
                if let Some(row) = tx
                    .open_table(execution::EXECUTION_ERRORS)
                    .map_err(err)?
                    .get(key.value())
                    .map_err(err)?
                {
                    let (attempt, message): (String, String) =
                        serde_json::from_slice(row.value()).map_err(err)?;
                    if attempt == record.policy.model.identity.attempt_id {
                        diagnostics.push(format!(
                            "execution_diagnostic: work={:?} {message}",
                            key.value()
                        ));
                    }
                }
                diagnostics.push(format!("execution: {}", serde_json::json!({
                    "work_id": key.value(), "attempt_id": record.policy.model.identity.attempt_id,
                    "generation": record.policy.model.identity.generation,
                    "reservation_id": record.policy.funding.dispatch_id,
                    "phase": record.phase, "settled": record.settled,
                    "candidate_retained": record.candidate.is_some()
                })));
                if record.phase == ExecutionPhase::Reviewed(Evaluation::Unverified) {
                    diagnostics.push(format!("continuation: work={:?} expected_stopping_reason=stopped_unverified; {}", key.value(),
                        if record.settled { "denied: allocations settled; money cannot reopen" } else { "logical reattachment only; explicit claim rechecks checkpoint, configuration, cleanup, billing, allocation and deadline" }));
                }
            }
        }
        let dispatches = tx
            .open_table(TableDefinition::<&str, &[u8]>::new(
                "campaign_model_dispatches",
            ))
            .map_err(err)?;
        for row in dispatches.iter().map_err(err)? {
            let (key, value) = row.map_err(err)?;
            let dispatch: serde_json::Value = serde_json::from_slice(value.value()).map_err(err)?;
            let request: RequestReservation =
                serde_json::from_value(dispatch["request"].clone()).map_err(err)?;
            if request.identity.campaign_id == id {
                let (allocation, request_id): (String, String) =
                    serde_json::from_str(key.value()).map_err(err)?;
                let reservation = dispatch["receipt"]
                    .as_str()
                    .ok_or("missing model receipt")?;
                let binding_key =
                    serde_json::to_string(&("reservation", reservation)).map_err(err)?;
                let binding: Option<serde_json::Value> = tx
                    .open_table(BINDINGS)
                    .map_err(err)?
                    .get(binding_key.as_str())
                    .map_err(err)?
                    .map(|v| serde_json::from_slice(v.value()).map_err(err))
                    .transpose()?;
                diagnostics.push(format!("model_request: {}", serde_json::json!({
                    "identity": request.identity, "allocation_id": allocation, "request_id": request_id,
                    "reservation_id": dispatch["receipt"], "provider": request.estimate.provider,
                    "provider_request_id": binding.as_ref().map(|b| &b["provider_request_id"]),
                    "provider_binding": if binding.is_some() { "operator_attested" } else { "not_recorded_independent_binding_required" }
                })));
            }
        }
        for row in tx
            .open_table(crate::runtime_store::research_context::traces::TRACES)
            .map_err(err)?
            .range((id, "")..)
            .map_err(err)?
        {
            let (key, value) = row.map_err(err)?;
            if key.value().0 != id {
                break;
            }
            let resource: tachyon_api::context::Resource =
                serde_json::from_slice(value.value()).map_err(err)?;
            if resource.data["phase"] == "work_context_snapshot" {
                let current_state = tx
                    .open_table(EXECUTIONS)
                    .map_err(err)?
                    .get(resource.reference.work_id.as_str())
                    .map_err(err)?
                    .map(|row| execution::decode_record(row.value(), &resource.reference.work_id))
                    .transpose()?
                    .is_some_and(|record| {
                        record.policy.funding.admission.campaign_id == id
                            && record.stopping_snapshot.as_ref() == Some(&resource.reference)
                            && resource.data["source"] == "host_stop"
                            && resource.data["retention_state"] != "staging"
                    });
                diagnostics.push(format!("continuation_checkpoint: {}", serde_json::json!({"checkpoint":resource.reference,"occurred_at_ms":resource.occurred_at_ms,"current_state":current_state,"data_is_untrusted":true})));
            }
        }
        Ok(diagnostics)
    }

    /// Called on the blocking IPC connection thread, outside the actor/registry.
    /// The lifecycle guard excludes activation until the whole transaction commits.
    pub(crate) fn reconcile(
        &self,
        id: &str,
        receipt: &ReconciliationReceipt,
        authorized: bool,
        authoritative: bool,
    ) -> Result<ApiResponse, String> {
        if !authorized || !authoritative {
            return Err("--unisolated-development and --confirm-authoritative required".into());
        }
        receipt.validate()?;
        if receipt.campaign_id != id {
            return Err("receipt campaign scope mismatch".into());
        }
        let active = self
            .active
            .lock()
            .map_err(|_| "campaign registry unavailable")?;
        if self.stopping.load(std::sync::atomic::Ordering::Acquire)
            || active.get(id).is_some_and(|a| !a.task.is_finished())
        {
            return Err(
                "reconciliation requires an idle campaign; scheduler tasks are still active".into(),
            );
        }
        let tx = self.store.database.begin_write().map_err(err)?;
        let payload = serde_json::to_vec(receipt).map_err(err)?;
        let previous = tx
            .open_table(RECEIPTS)
            .map_err(err)?
            .get(receipt.command_id.as_str())
            .map_err(err)?
            .map(|v| v.value().to_vec());
        if let Some(previous) = previous {
            if previous != payload {
                return Err("reconciliation command payload/scope conflict".into());
            }
            drop(tx);
            self.release_reconciled_capacity(receipt)?;
            return self
                .store
                .research_request(&ApiRequest::CampaignGet { id: id.into() });
        }
        let launch: Launch = serde_json::from_slice(
            tx.open_table(LAUNCHES)
                .map_err(err)?
                .get(id)
                .map_err(err)?
                .ok_or("no authorized launch")?
                .value(),
        )
        .map_err(err)?;
        launch.validate_digest()?;
        if launch.manifest.campaign_id != id {
            return Err("launch scope mismatch".into());
        }
        if state_hash(&tx)? != receipt.expected_state_sha256 {
            return Err("stale reconciliation state; inspect again".into());
        }
        let mut targets = std::collections::BTreeSet::new();
        // Billing first, independent of record ordering. Any later failure aborts all changes.
        for item in &receipt.records {
            let RecoveryRecord::ModelUsage {
                work_id,
                attempt_id,
                generation,
                instruction_revision,
                reservation_id,
                allocation_id,
                request_id,
                provider,
                provider_request_id,
                input_tokens,
                output_tokens,
                cost_micro_usd,
            } = item
            else {
                continue;
            };
            if !targets.insert(("model", reservation_id)) {
                return Err("duplicate model target".into());
            }
            let mut table = tx.open_table(REQUESTS).map_err(err)?;
            let mut record: Record = serde_json::from_slice(
                table
                    .get(reservation_id.as_str())
                    .map_err(err)?
                    .ok_or("unknown model reservation")?
                    .value(),
            )
            .map_err(err)?;
            let identity = &record.request.identity;
            let funding = RuntimeStore::admitted_work_in(&tx, work_id)?;
            let ledger = RuntimeStore::campaign_ledger_in(&tx, id)?;
            let hold = ledger
                .reservations
                .get(reservation_id)
                .ok_or("missing model hold")?;
            if record.schema_version != 1
                || identity.campaign_id != id
                || identity.work_id != *work_id
                || identity.attempt_id != *attempt_id
                || identity.generation != *generation
                || identity.instruction_revision != *instruction_revision
                || record.request.estimate.provider != *provider
                || record.allocation_id.as_ref() != Some(allocation_id)
                || funding.dispatch_id != *allocation_id
                || funding.admission.campaign_id != id
                || funding.admission.generation != *generation
                || hold.allocation.as_ref() != Some(allocation_id)
                || hold.pool != funding.admission.pool
                || !ledger.allocations.contains_key(allocation_id)
            {
                return Err("model receipt identity/funding mismatch".into());
            }
            let key = serde_json::to_string(&(allocation_id, request_id)).map_err(err)?;
            let dispatches = tx
                .open_table(TableDefinition::<&str, &[u8]>::new(
                    "campaign_model_dispatches",
                ))
                .map_err(err)?;
            let dispatch: serde_json::Value = serde_json::from_slice(
                dispatches
                    .get(key.as_str())
                    .map_err(err)?
                    .ok_or("unknown provider dispatch request ID")?
                    .value(),
            )
            .map_err(err)?;
            if dispatch["schema_version"] != 1
                || dispatch["claimed"] != true
                || dispatch["receipt"] != *reservation_id
                || dispatch["request"] != serde_json::to_value(&record.request).map_err(err)?
            {
                return Err("dispatch request mismatch".into());
            }
            let usage = RequestUsage::Final {
                input_tokens: *input_tokens,
                output_tokens: *output_tokens,
                cost_micro_usd: *cost_micro_usd,
            };
            if record.usage != RequestUsage::Unknown && record.usage != usage {
                return Err("immutable final model usage conflict".into());
            }
            let binding = serde_json::to_vec(item).map_err(err)?;
            let mut bindings = tx.open_table(BINDINGS).map_err(err)?;
            for key in [
                serde_json::to_string(&("provider", provider, provider_request_id)).map_err(err)?,
                serde_json::to_string(&("reservation", reservation_id)).map_err(err)?,
            ] {
                if bindings
                    .get(key.as_str())
                    .map_err(err)?
                    .is_some_and(|v| v.value() != binding)
                {
                    return Err("immutable provider request binding conflict".into());
                }
                bindings
                    .insert(key.as_str(), binding.as_slice())
                    .map_err(err)?;
            }
            RuntimeStore::campaign_ledger_command_in(
                &tx,
                &format!("final:{reservation_id}"),
                id,
                LedgerCommand::Reconcile {
                    reservation_id: reservation_id.clone(),
                    usage: Usage::Final(Units {
                        tokens: input_tokens
                            .checked_add(*output_tokens)
                            .ok_or("token overflow")?,
                        cost_micro_usd: *cost_micro_usd,
                    }),
                },
            )?;
            record.usage = usage;
            table
                .insert(
                    reservation_id.as_str(),
                    serde_json::to_vec(&record).map_err(err)?.as_slice(),
                )
                .map_err(err)?;
        }
        for item in &receipt.records {
            let RecoveryRecord::Cleanup {
                work_id,
                attempt_id,
                generation,
                reservation_id,
                ..
            } = item
            else {
                continue;
            };
            if !targets.insert(("cleanup", work_id)) {
                return Err("duplicate cleanup target".into());
            }
            let previous = execution::decode_record(
                tx.open_table(EXECUTIONS)
                    .map_err(err)?
                    .get(work_id.as_str())
                    .map_err(err)?
                    .ok_or("cleanup requires a recorded execution identity")?
                    .value(),
                work_id,
            )?;
            if previous.policy.funding.admission.campaign_id != id
                || previous.policy.model.identity.attempt_id != *attempt_id
                || previous.policy.model.identity.generation != *generation
                || previous.policy.funding.dispatch_id != *reservation_id
                || previous.settled
                || previous.rework_pending
                || !matches!(
                    previous.phase,
                    ExecutionPhase::ExecutingUnknown
                        | ExecutionPhase::ReviewingUnknown
                        | ExecutionPhase::EvidenceReady
                        | ExecutionPhase::AwaitingVerification
                        | ExecutionPhase::Reviewed(Evaluation::Unverified)
                )
            {
                return Err("cleanup target identity/state mismatch".into());
            }
            if previous.phase == ExecutionPhase::ReviewingUnknown {
                // This hold belongs to the host's non-spending command evaluator,
                // never to a model reviewer. Cleanup does not settle model requests.
                let gates = tx.open_table(COMMAND_GATES).map_err(err)?;
                let gate: execution::command::CommandGateRecord = serde_json::from_slice(
                    gates
                        .get(work_id.as_str())
                        .map_err(err)?
                        .ok_or("unknown reviewer requires a bound host command gate")?
                        .value(),
                )
                .map_err(err)?;
                if gate.policy != previous.policy
                    || previous.policy.evaluator_id
                        != format!("command:{}", gate.config.config_hash()?)
                {
                    return Err("reviewer is not the bound non-spending command".into());
                }
                let evaluation = execution::evaluation_receipt(&previous.policy);
                RuntimeStore::campaign_ledger_command_in(
                    &tx,
                    &evaluation.replacen("evaluation:", "evaluation-final:", 1),
                    id,
                    LedgerCommand::Reconcile {
                        reservation_id: evaluation,
                        usage: Usage::Final(Units::default()),
                    },
                )?;
            }
            let mut next = previous.clone();
            RuntimeStore::cleanup_jobs_in(&tx, id, work_id, attempt_id, *generation)?;
            next.phase = ExecutionPhase::Reviewed(Evaluation::Unverified);
            RuntimeStore::update_execution_in(&tx, &previous, &next)?;
            for original in [&previous.policy.funding, &previous.policy.verification] {
                let mut work = RuntimeStore::admitted_work_in(&tx, &original.admission.work_id)?;
                if work.admission != original.admission || work.dispatch_id != original.dispatch_id
                {
                    return Err("cleanup admission changed".into());
                }
                let ledger = RuntimeStore::campaign_ledger_in(&tx, id)?;
                if !ledger.allocations.contains_key(&work.dispatch_id) {
                    // In the local broker path, every provider call first converts
                    // this hold atomically with its request reservation. An untouched
                    // admission is unused funding, not an unknown provider invoice.
                    for row in tx.open_table(REQUESTS).map_err(err)?.iter().map_err(err)? {
                        let (_, value) = row.map_err(err)?;
                        let request: Record = serde_json::from_slice(value.value()).map_err(err)?;
                        if request.request.identity.work_id == work.admission.work_id {
                            return Err(
                                "unconverted admission has model requests; cannot release".into()
                            );
                        }
                    }
                    RuntimeStore::campaign_ledger_command_in(
                        &tx,
                        &format!("unspent:{}", work.dispatch_id),
                        id,
                        LedgerCommand::Reconcile {
                            reservation_id: work.dispatch_id.clone(),
                            usage: Usage::Final(Units::default()),
                        },
                    )?;
                }
                // Cancelled is termination, NOT ConfirmedUnspent. Unknown holds remain.
                let retained_command = tx
                    .open_table(COMMAND_GATES)
                    .map_err(err)?
                    .get(work_id.as_str())
                    .map_err(err)?
                    .is_some();
                if !retained_command || ledger.allocations.get(&work.dispatch_id) != Some(&false) {
                    work.state = DispatchState::Cancelled;
                }
                tx.open_table(WORK)
                    .map_err(err)?
                    .insert(
                        work.admission.work_id.as_str(),
                        serde_json::to_vec(&work).map_err(err)?.as_slice(),
                    )
                    .map_err(err)?;
                tx.open_table(PENDING)
                    .map_err(err)?
                    .remove(work.admission.work_id.as_str())
                    .map_err(err)?;
                RuntimeStore::group_terminal_in(&tx, &work)?;
            }
        }
        let mut works = Vec::new();
        for item in &receipt.records {
            if let RecoveryRecord::NativeCleanup {
                work_id,
                attempt_id,
                generation,
                lease_id,
                ..
            } = item
            {
                if !targets.insert(("native_cleanup", lease_id)) {
                    return Err("duplicate native cleanup target".into());
                }
                RuntimeStore::cleanup_job_in(&tx, id, work_id, attempt_id, *generation, lease_id)?;
            }
        }
        for row in tx
            .open_table(EXECUTIONS)
            .map_err(err)?
            .iter()
            .map_err(err)?
        {
            let (key, value) = row.map_err(err)?;
            let record = execution::decode_record(value.value(), key.value())?;
            if record.policy.funding.admission.campaign_id == id
                && record.phase == ExecutionPhase::Reviewed(Evaluation::Unverified)
            {
                works.push(key.value().to_owned());
            }
        }
        for work in works {
            RuntimeStore::settle_campaign_execution_in(&tx, id, &work)?;
        }
        // Manual cleanup never promotes a campaign to verified, even if every hold is final.
        let ApiResponse::Campaign { campaign } = self
            .store
            .research_request(&ApiRequest::CampaignGet { id: id.into() })?
        else {
            return Err("missing campaign".into());
        };
        if campaign.status != CampaignStatus::Cancelled {
            RuntimeStore::set_campaign_status_in(&tx, id, CampaignStatus::Unverified)?;
        }
        tx.open_table(RECEIPTS)
            .map_err(err)?
            .insert(receipt.command_id.as_str(), payload.as_slice())
            .map_err(err)?;
        tx.commit().map_err(err)?;
        self.release_reconciled_capacity(receipt)?;
        self.store
            .research_request(&ApiRequest::CampaignGet { id: id.into() })
    }

    fn release_reconciled_capacity(&self, receipt: &ReconciliationReceipt) -> Result<(), String> {
        self.store.release_cleaned_jobs()?;
        // Called only under the campaign lifecycle lock, after exact receipt CAS
        // validation and commit (or byte-identical durable receipt replay).
        for record in &receipt.records {
            if let RecoveryRecord::Cleanup {
                work_id,
                attempt_id,
                generation,
                reservation_id,
                ..
            } = record
            {
                let identity = super::super::host_capacity::identity(
                    &receipt.campaign_id,
                    work_id,
                    attempt_id,
                    *generation,
                    reservation_id,
                );
                self.store
                    .host_capacity
                    .resident
                    .release_unknown(&identity)?;
                self.store
                    .host_capacity
                    .execution
                    .release_unknown(&identity)?;
            }
        }
        Ok(())
    }
}
