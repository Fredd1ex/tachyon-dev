//! A retained host collection boundary, independent of model history compaction.
use super::*;
use sha2::{Digest, Sha256};

impl RuntimeStore {
    pub(crate) fn record_stopping_context(
        &self,
        tx: WriteTransaction,
        previous: &ExecutionRecord,
        next: &mut ExecutionRecord,
        artifacts: Option<&ArtifactStore>,
        final_context: Option<WorkerContextMetadata>,
    ) -> Result<(), String> {
        let identity = &next.policy.model.identity;
        let admission = &next.policy.funding.admission;
        let campaign = &admission.campaign_id;
        let mut latest: Option<Resource> = None;
        let mut produced = Vec::new();
        let mut output_handles = std::collections::BTreeSet::new();
        for row in tx
            .open_table(traces::TRACES)
            .map_err(err)?
            .range((campaign.as_str(), "")..)
            .map_err(err)?
        {
            let (key, value) = row.map_err(err)?;
            if key.value().0 != campaign {
                break;
            }
            let resource: Resource = serde_json::from_slice(value.value()).map_err(err)?;
            if resource.reference.work_id != identity.work_id
                || resource.data["attempt_id"] != identity.attempt_id
                || resource.data["generation"]
                    .as_u64()
                    .is_some_and(|g| g != identity.generation)
                || resource.data["retention_state"] == "staging"
            {
                continue;
            }
            if resource.data["phase"] == "work_context_snapshot" {
                if latest.as_ref().is_none_or(|old| {
                    (old.occurred_at_ms, &old.reference.id)
                        < (resource.occurred_at_ms, &resource.reference.id)
                }) {
                    latest = Some(resource);
                }
            } else {
                if resource.data["phase"] == "retained_output"
                    && resource.data["retention_state"] == "ready"
                    && resource.data["generation"] == identity.generation
                {
                    if let Some(handle) = resource.data["live_handle_id"].as_str() {
                        output_handles.insert(handle.to_owned());
                    }
                }
                produced.push(resource.reference);
                if produced.len() > 256 {
                    return Err(err("stopping resource inventory exceeds bound"));
                }
            }
        }
        let final_provided = final_context.is_some();
        let mut metadata = if final_provided {
            final_context.filter(|m| m.valid())
        } else if let Some(resource) = latest {
            let size = resource.data["size_bytes"]
                .as_u64()
                .ok_or("invalid snapshot size")?;
            if size > 65536 {
                return Err(err("oversized prior snapshot"));
            }
            let mut bytes = Vec::new();
            while (bytes.len() as u64) < size {
                let chunk = self.trace_read(&resource, bytes.len() as u64, 1024)?;
                if chunk.is_empty() {
                    return Err(err("incomplete prior snapshot"));
                }
                bytes.extend(chunk);
            }
            let snapshot: WorkContextSnapshot = serde_json::from_slice(&bytes).map_err(err)?;
            if snapshot.schema_version != 1
                || snapshot.work_id != identity.work_id
                || snapshot.attempt_id != identity.attempt_id
                || snapshot.generation != identity.generation
                || snapshot.reattachable
            {
                return Err(err("prior snapshot identity mismatch"));
            }
            snapshot
                .worker_claims_informational_only
                .filter(|m| m.valid())
        } else {
            None
        };
        // Live IDs only have durable meaning through owned, explicitly mapped exports.
        let mut filtered = final_provided && metadata.is_none();
        if let Some(metadata) = &mut metadata {
            metadata.known_output_handles.retain(|id| {
                let owned = output_handles.contains(id);
                filtered |= !owned;
                owned
            });
        }
        if let Some(artifacts) = artifacts {
            let mut after = None;
            loop {
                let page = artifacts.list(&identity.work_id, after.as_deref(), 100)?;
                if page.is_empty() {
                    break;
                }
                after = page.last().map(|a| a.id.clone());
                for artifact in page {
                    if artifact.attempt_id.as_deref() == Some(&identity.attempt_id)
                        && artifact.generation == Some(identity.generation)
                        && artifact.assignment == Some(next.policy.work.assignment)
                        && matches!(artifact.publication, ArtifactPublication::Ready { .. })
                    {
                        produced.push(artifact_resource(&artifact, None)?.reference);
                    }
                }
                if produced.len() > 256 {
                    return Err(err("stopping resource inventory exceeds bound"));
                }
            }
        }
        let handles = Self::context_work_handles_in(&tx, admission)?;
        let groups = Self::context_group_handles_in(&tx, campaign, &handles)?;
        if handles.len() > 256 || groups.len() > 256 || produced.len() > 256 {
            return Err(err("stopping inventory exceeds bound"));
        }
        let ledger = Self::campaign_ledger_in(&tx, campaign)?;
        let remaining = ledger
            .allocations
            .contains_key(&next.policy.funding.dispatch_id)
            .then(|| ledger.allocation_available(&next.policy.funding.dispatch_id))
            .transpose()?;
        let revision = Self::latest_instruction_revision_in(&tx, admission)?;
        let applied = Self::recognized_instruction_revision_in(&tx, admission)?;
        let questions = Self::continuation_questions_in(&tx, campaign, &identity.work_id)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(err)?
            .as_millis() as u64;
        let snapshot = WorkContextSnapshot {
            schema_version: 1,
            work_id: identity.work_id.clone(),
            attempt_id: identity.attempt_id.clone(),
            generation: identity.generation,
            instruction_revision: revision,
            request_id: "host-stop".into(),
            objective: admission.objective.clone(),
            stopping: Some(StoppingContext {
                phase: if next.phase == super::super::execution::ExecutionPhase::EvidenceReady {
                    StoppingPhase::EvidenceReady
                } else {
                    StoppingPhase::Unverified
                },
                reason: if next.candidate.is_some() {
                    SnapshotStoppingReason::CandidateCollected
                } else {
                    SnapshotStoppingReason::StoppedUnverified
                },
                accepted_instruction_revision: revision,
                applied_instruction_revision: applied,
                instruction_refs: Self::context_instruction_refs_in(&tx, admission)?,
                logical_work_handles: handles,
                group_handles: groups,
                produced_resource_refs: produced,
                activation_observation: if metadata.is_some() {
                    if final_provided {
                        ActivationObservation::Final
                    } else {
                        ActivationObservation::Stale
                    }
                } else {
                    ActivationObservation::Unknown
                },
                worker_observations_filtered: filtered,
            }),
            worker_claims_informational_only: metadata,
            pending_question_refs: Some(
                questions
                    .iter()
                    .filter(|q| {
                        q.answer.is_none()
                            && q.deadline_ms > now
                            && q.generation == identity.generation
                            && q.instruction_revision == revision
                    })
                    .map(|q| q.request_id.clone())
                    .collect(),
            ),
            remaining_tokens: remaining.map(|r| r.tokens),
            remaining_cost_micro_usd: remaining.map(|r| r.cost_micro_usd),
            selected_resource_refs: next.policy.work.context_refs.clone(),
            reattachable: false,
        };
        let bytes = serde_json::to_vec(&snapshot).map_err(err)?;
        if bytes.len() > 65536.min(self.trace_limits.operation) {
            return Err(err("stopping snapshot exceeds operation bound"));
        }
        let reference = ResourceRef {
            kind: ResourceKind::Trace,
            work_id: identity.work_id.clone(),
            id: format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&(
                        campaign,
                        &identity.work_id,
                        &identity.attempt_id,
                        "host-stop"
                    ))
                    .map_err(err)?
                )
            ),
            version: format!("{:x}", Sha256::digest(&bytes)),
        };
        let resource = Resource {
            reference: reference.clone(),
            occurred_at_ms: Some(now),
            data: json!({"schema_version":1,"attempt_id":identity.attempt_id,"request_id":"host-stop",
                "phase":"work_context_snapshot","size_bytes":bytes.len(),"source":"host_stop"}),
        };
        next.stopping_snapshot = Some(reference);
        self.store_trace_object_committing(
            tx,
            campaign,
            resource,
            &bytes,
            || true,
            |tx| {
                let stopping = snapshot.stopping.as_ref().unwrap();
                let handles = Self::context_work_handles_in(tx, admission)?;
                let groups = Self::context_group_handles_in(tx, campaign, &handles)?;
                let ledger = Self::campaign_ledger_in(tx, campaign)?;
                let current_remaining = ledger
                    .allocations
                    .contains_key(&next.policy.funding.dispatch_id)
                    .then(|| ledger.allocation_available(&next.policy.funding.dispatch_id))
                    .transpose()?;
                if Self::latest_instruction_revision_in(tx, admission)? != revision
                    || Self::recognized_instruction_revision_in(tx, admission)? != applied
                    || handles != stopping.logical_work_handles
                    || groups != stopping.group_handles
                    || current_remaining != remaining
                    || Self::continuation_questions_in(tx, campaign, &identity.work_id)?
                        != questions
                {
                    return Err(err("coordination changed during stopping publication"));
                }
                Self::update_execution_in(tx, previous, next)
            },
        )
    }
}
