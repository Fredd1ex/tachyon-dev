//! Durable observations, never a second source of execution or budget authority.
use super::*;
use tachyon_api::context::{ResourceRef, WorkContextSnapshot, WorkerContextMetadata};
use tachyon_model::broker::protocol_error;

impl RuntimeStore {
    pub(super) fn record_work_context(
        &self,
        nonce: uuid::Uuid,
        id: &str,
        metadata: Option<WorkerContextMetadata>,
        questions_available: bool,
    ) -> tachyon_model::Result<ResourceRef> {
        let state = self
            .model_permits
            .lock()
            .map_err(|_| err("permit authority unavailable"))?;
        let grant = state.grants.get(&nonce).ok_or_else(protocol_error)?;
        if !grant.active
            || grant.paused
            || grant.closed.load(std::sync::atomic::Ordering::Acquire)
            || state.current.get(&grant.request.identity.work_id) != Some(&nonce)
            || metadata.as_ref().is_some_and(|m| !m.valid())
        {
            return Err(protocol_error());
        }
        let tx = self.database.begin_write().map_err(err)?;
        Self::admitted_funding_in(&tx, &grant.funding).map_err(err)?;
        let ledger =
            Self::campaign_ledger_in(&tx, &grant.funding.admission.campaign_id).map_err(err)?;
        let remaining = ledger
            .allocations
            .contains_key(&grant.funding.dispatch_id)
            .then(|| ledger.allocation_available(&grant.funding.dispatch_id))
            .transpose()
            .map_err(err)?;
        let identity = &grant.request.identity;
        let executions = tx
            .open_table(crate::runtime_store::execution::EXECUTIONS)
            .map_err(err)?;
        let selected_resource_refs = executions
            .get(identity.work_id.as_str())
            .map_err(err)?
            .map(|row| {
                let record =
                    crate::runtime_store::execution::decode_record(row.value(), &identity.work_id)?;
                if record.policy.model.identity.attempt_id != identity.attempt_id
                    || record.policy.model.identity.generation != identity.generation
                    || record.policy.funding.admission.campaign_id != identity.campaign_id
                {
                    return Err("snapshot execution identity mismatch".to_string());
                }
                Ok(record.policy.work.context_refs)
            })
            .transpose()
            .map_err(err)?;
        drop(executions);
        let selected_resource_refs = match selected_resource_refs {
            Some(refs) => refs,
            None => {
                let assignments = tx
                    .open_table(crate::runtime_store::research_context::traces::TRACE_ASSIGNMENTS)
                    .map_err(err)?;
                let row = assignments
                    .get((
                        grant.funding.admission.campaign_id.as_str(),
                        identity.work_id.as_str(),
                        identity.attempt_id.as_str(),
                    ))
                    .map_err(err)?;
                row.map(|row| {
                    serde_json::from_slice::<(tachyon_api::types::WorkRequest, String)>(row.value())
                        .map(|(work, _)| work.context_refs)
                })
                .transpose()
                .map_err(err)?
                .unwrap_or_default()
            }
        };
        drop(tx);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let pending_question_refs = if questions_available {
            Some(
                self.work_attention(&grant.funding.admission.campaign_id, &identity.work_id)
                    .map_err(err)?
                    .into_iter()
                    .filter(|q| {
                        q.answer.is_none()
                            && q.deadline_ms > now
                            && q.generation == identity.generation
                            && q.instruction_revision == identity.instruction_revision
                    })
                    .map(|q| q.request_id)
                    .collect(),
            )
        } else {
            None
        };
        let snapshot = WorkContextSnapshot {
            stopping: None,
            schema_version: 1,
            work_id: identity.work_id.clone(),
            attempt_id: identity.attempt_id.clone(),
            generation: identity.generation,
            instruction_revision: identity.instruction_revision,
            request_id: id.into(),
            objective: grant.funding.admission.objective.clone(),
            worker_claims_informational_only: metadata,
            pending_question_refs,
            remaining_tokens: remaining.map(|r| r.tokens),
            remaining_cost_micro_usd: remaining.map(|r| r.cost_micro_usd),
            selected_resource_refs,
            reattachable: false,
        };
        self.record_context_object(
            &grant.funding.admission.campaign_id,
            &identity.work_id,
            &identity.attempt_id,
            id,
            "work_context_snapshot",
            &serde_json::to_vec(&snapshot).map_err(err)?,
        )
        .map_err(err)
    }

    pub(super) fn record_model_context_result(
        &self,
        nonce: uuid::Uuid,
        id: &str,
        bytes: &[u8],
    ) -> tachyon_model::Result<()> {
        let state = self
            .model_permits
            .lock()
            .map_err(|_| err("permit authority unavailable"))?;
        let grant = state.grants.get(&nonce).ok_or_else(protocol_error)?;
        let identity = &grant.request.identity;
        // Oversize provider output is explicitly marked, never silently cut into invalid JSON.
        let overflow = if bytes.len() > 1024 * 1024 {
            let original: serde_json::Value = serde_json::from_slice(bytes).unwrap_or_default();
            serde_json::to_vec(&serde_json::json!({"schema_version":1,"request_id":id,"output_retained":false,"reason":"exceeds_1_mib","original_bytes":bytes.len(),"snapshot":original["snapshot"],"succeeded":original["succeeded"],"usage":original["completion"]["usage"],"input_message_count":original["input_message_count"]})).map_err(err)?
        } else {
            Vec::new()
        };
        self.record_context_object(
            &grant.funding.admission.campaign_id,
            &identity.work_id,
            &identity.attempt_id,
            id,
            "model_result",
            if bytes.len() <= 1024 * 1024 {
                bytes
            } else {
                &overflow
            },
        )
        .map_err(err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::context::{Query, Request};

    #[test]
    fn snapshots_reopen_complete_handles_scope_hash_and_no_reexecution() {
        let (dir, store, funding, request, actor) = super::super::boundary_tests::setup();
        let permit = store
            .host_issue_model_permit(request.clone(), funding.clone(), None)
            .unwrap();
        let (permit, request, _) = store
            .prepare_model_boundary(
                permit.0,
                request,
                "snapshot-1",
                Instant::now() + std::time::Duration::from_secs(10),
            )
            .unwrap();
        let metadata = WorkerContextMetadata {
            activated_packages: [("workspace".into(), "1".into()), ("ctx".into(), "1".into())]
                .into(),
            known_output_handles: (0..256).map(|i| format!("output:{i:0249}")).collect(),
        };
        let reference = store
            .record_work_context(permit.0, "snapshot-1", Some(metadata.clone()), true)
            .unwrap();
        assert_eq!(
            reference,
            store
                .record_work_context(permit.0, "snapshot-1", Some(metadata.clone()), true)
                .unwrap()
        );
        assert!(store
            .prepare_model_boundary(
                permit.0,
                request,
                "snapshot-1",
                Instant::now() + std::time::Duration::from_secs(10)
            )
            .is_err());
        let ledger =
            serde_json::to_value(store.campaign_ledger(&actor.campaign_id).unwrap()).unwrap();
        drop(store);
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let page = store
            .host_research_context(
                &actor.campaign_id,
                &Request::Snapshot {
                    query: Query {
                        literal: None,
                        after: None,
                        limit: 16,
                        since_ms: None,
                        version: None,
                    },
                },
                None,
            )
            .unwrap();
        assert_eq!(page.resources.len(), 1);
        assert_eq!(page.resources[0].reference, reference);
        let read = |scope: &str, offset, limit| {
            store.host_research_context(
                scope,
                &Request::Read {
                    resource: reference.clone(),
                    offset,
                    limit,
                },
                None,
            )
        };
        assert!(read("foreign", 0, 1024).is_err());
        assert!(read(&actor.campaign_id, 0, 1025).is_err());
        let mut bytes = Vec::new();
        loop {
            let page = read(&actor.campaign_id, bytes.len() as u64, 1024).unwrap();
            let part: Vec<u8> =
                serde_json::from_value(page.resources[0].data["bytes"].clone()).unwrap();
            if part.is_empty() {
                break;
            }
            bytes.extend(part);
        }
        let snapshot: WorkContextSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snapshot.worker_claims_informational_only, Some(metadata));
        assert_eq!(snapshot.objective, funding.admission.objective);
        assert!(!snapshot.reattachable);
        assert_eq!(
            ledger,
            serde_json::to_value(store.campaign_ledger(&actor.campaign_id).unwrap()).unwrap()
        );
        assert!(store
            .record_work_context(permit.0, "snapshot-1", None, true)
            .is_err());
        let path = store.trace_root.join(&reference.id);
        std::fs::remove_file(&path).unwrap();
        std::fs::write(path, vec![0; bytes.len()]).unwrap();
        assert!(read(&actor.campaign_id, 0, 1).is_err());
    }

    #[test]
    fn oversized_model_output_is_an_explicit_gap_not_a_partial_json_document() {
        let (_dir, store, funding, request, actor) = super::super::boundary_tests::setup();
        let permit = store
            .host_issue_model_permit(request, funding, None)
            .unwrap();
        store
            .record_model_context_result(permit.0, "large", &vec![b'x'; 1024 * 1024 + 1])
            .unwrap();
        let page = store
            .host_research_context(
                &actor.campaign_id,
                &Request::Traces {
                    query: Query {
                        literal: None,
                        after: None,
                        limit: 16,
                        since_ms: None,
                        version: None,
                    },
                },
                None,
            )
            .unwrap();
        let page = store
            .host_research_context(
                &actor.campaign_id,
                &Request::Read {
                    resource: page.resources[0].reference.clone(),
                    offset: 0,
                    limit: 1024,
                },
                None,
            )
            .unwrap();
        let bytes: Vec<u8> =
            serde_json::from_value(page.resources[0].data["bytes"].clone()).unwrap();
        let summary: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(summary["output_retained"], false);
        assert_eq!(summary["original_bytes"], 1024 * 1024 + 1);
    }
}
