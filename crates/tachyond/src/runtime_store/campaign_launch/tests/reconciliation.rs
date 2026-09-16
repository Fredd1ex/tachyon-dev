use super::*;
use crate::runtime_store::{
    campaign_ledger::{LedgerCommand, Usage},
    execution::EXECUTIONS,
    model_accounting::{Record, REQUESTS},
};
use tachyon_api::campaign::{
    CleanupConfirmation, CleanupOutcome, ReconciliationReceipt, RecoveryRecord,
};
use tachyon_model::accounting::RequestUsage;

fn setup() -> (
    tempfile::TempDir,
    Arc<RuntimeStore>,
    CampaignService,
    CampaignManifest,
    ExecutionPolicy,
) {
    let (dir, store, m) = fixture();
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    store
        .host_authorize_campaign_envelope(
            "grant",
            &m.campaign_id,
            Envelope {
                work: Units {
                    tokens: 100,
                    cost_micro_usd: 100,
                },
                verification: Units {
                    tokens: 10,
                    cost_micro_usd: 10,
                },
                max_active_inferences: 4,
            },
        )
        .unwrap();
    for (work, pool, amount) in [
        ("work", Pool::Work, 100),
        ("review", Pool::Verification, 10),
    ] {
        store
            .admit_campaign_work(Admission {
                work_id: work.into(),
                campaign_id: m.campaign_id.clone(),
                objective: "fixture".into(),
                generation: 1,
                instruction_revision: 1,
                pool,
                upper_bound: Units {
                    tokens: amount,
                    cost_micro_usd: amount,
                },
            })
            .unwrap();
    }
    store
        .dispatch_campaign_batch(2, |_| DispatchOutcome::Registered {
            worker_id: "fixture".into(),
        })
        .unwrap();
    let policy = ExecutionPolicy {
        funding: store.admitted_work(&m.campaign_id, "work").unwrap(),
        verification: store.admitted_work(&m.campaign_id, "review").unwrap(),
        evaluator_id: "fixture".into(),
        work: WorkRequest {
            context_refs: vec![],
            work_id: "work".into(),
            objective: "fixture".into(),
            generation: 1,
            assignment: 1,
            lifetime_class: LifetimeClass::Short,
            deadline_ms: m.deadline_ms,
            attempt: None,
            constraints: None,
        },
        model: RequestReservation {
            identity: WorkIdentity {
                campaign_id: m.campaign_id.clone(),
                work_id: "work".into(),
                attempt_id: "attempt".into(),
                generation: 1,
                instruction_revision: 1,
                class: RequestClass::Work,
            },
            estimate: RequestEstimate {
                base_url: "http://127.0.0.1:1/v1".into(),
                model: "fixture".into(),
                provider: "openrouter".into(),
                pricing_revision: "fixture".into(),
                max_request_bytes: 1000,
                input_tokens: 20,
                output_tokens: 10,
                input_micro_usd_per_million: 1000000,
                output_micro_usd_per_million: 1000000,
                other_micro_usd: 0,
            },
        },
    };
    store
        .campaign_ledger_command(
            "fund",
            &m.campaign_id,
            LedgerCommand::FundAllocation {
                reservation_id: policy.funding.dispatch_id.clone(),
                work_id: "work".into(),
            },
        )
        .unwrap();
    store
        .campaign_ledger_command(
            "reserve",
            &m.campaign_id,
            LedgerCommand::ReserveAllocated {
                reservation_id: "model:fixture".into(),
                allocation_id: policy.funding.dispatch_id.clone(),
                pool: Pool::Work,
                reserved: Units {
                    tokens: 30,
                    cost_micro_usd: 30,
                },
            },
        )
        .unwrap();
    let tx = store.database.begin_write().unwrap();
    tx.open_table(LAUNCHES)
        .unwrap()
        .insert(
            m.campaign_id.as_str(),
            serde_json::to_vec(&Launch {
                schema_version: 1,
                manifest: m.clone(),
                base_url: "http://127.0.0.1:1/v1".into(),
                manifest_sha256: Some(Launch::digest(&m).unwrap()),
            })
            .unwrap()
            .as_slice(),
        )
        .unwrap();
    tx.open_table(EXECUTIONS).unwrap().insert("work", serde_json::to_vec(&serde_json::json!({
        "schema_version":1,"policy":policy,"phase":"ExecutingUnknown","candidate":null,"settled":false
    })).unwrap().as_slice()).unwrap();
    tx.open_table(REQUESTS)
        .unwrap()
        .insert(
            "model:fixture",
            serde_json::to_vec(&Record {
                schema_version: 1,
                request: policy.model.clone(),
                usage: RequestUsage::Unknown,
                allocation_id: Some(policy.funding.dispatch_id.clone()),
            })
            .unwrap()
            .as_slice(),
        )
        .unwrap();
    tx.open_table(TableDefinition::<&str, &[u8]>::new("campaign_model_dispatches")).unwrap()
        .insert(serde_json::to_string(&(&policy.funding.dispatch_id, "request-1")).unwrap().as_str(),
            serde_json::to_vec(&serde_json::json!({"schema_version":1,"owner":"fixture","request":policy.model,"receipt":"model:fixture","claimed":true})).unwrap().as_slice()).unwrap();
    RuntimeStore::set_campaign_status_in(&tx, &m.campaign_id, CampaignStatus::Interrupted).unwrap();
    tx.commit().unwrap();
    (dir, store, service, m, policy)
}

fn receipt(
    service: &CampaignService,
    m: &CampaignManifest,
    policy: &ExecutionPolicy,
) -> ReconciliationReceipt {
    let diagnostics = service.reconciliation_inspection(&m.campaign_id).unwrap();
    ReconciliationReceipt {
        schema_version: 1,
        command_id: "operator-1".into(),
        campaign_id: m.campaign_id.clone(),
        expected_state_sha256: diagnostics[0]
            .strip_prefix("expected_state_sha256: ")
            .unwrap()
            .into(),
        evidence_reference: "case-42".into(),
        records: vec![
            RecoveryRecord::ModelUsage {
                work_id: "work".into(),
                attempt_id: "attempt".into(),
                generation: 1,
                instruction_revision: 1,
                reservation_id: "model:fixture".into(),
                allocation_id: policy.funding.dispatch_id.clone(),
                request_id: "request-1".into(),
                provider: "openrouter".into(),
                provider_request_id: "provider-42".into(),
                input_tokens: 21,
                output_tokens: 10,
                cost_micro_usd: 35,
            },
            RecoveryRecord::Cleanup {
                work_id: "work".into(),
                attempt_id: "attempt".into(),
                generation: 1,
                reservation_id: policy.funding.dispatch_id.clone(),
                confirmation: CleanupConfirmation::OperatorAttestsAllProcessesTerminated,
                outcome: CleanupOutcome::Unverified,
            },
        ],
    }
}

#[test]
fn reconciliation_releases_exact_retained_host_slots_once() {
    let (_dir, store, service, m, policy) = setup();
    let receipt = receipt(&service, &m, &policy);
    let identity = crate::runtime_store::host_capacity::identity(
        &m.campaign_id,
        "work",
        "attempt",
        1,
        &policy.funding.dispatch_id,
    );
    let before = store.host_capacity.resident.available();
    store
        .host_capacity
        .resident
        .reserve_unknown(&identity)
        .unwrap();
    store
        .host_capacity
        .execution
        .reserve_unknown(&identity)
        .unwrap();
    assert_eq!(store.host_capacity.resident.available(), before - 1);
    let mut invalid = receipt.clone();
    if let RecoveryRecord::Cleanup { attempt_id, .. } = &mut invalid.records[1] {
        *attempt_id = "wrong-attempt".into();
    }
    assert!(service
        .reconcile(&m.campaign_id, &invalid, true, true)
        .is_err());
    assert_eq!(store.host_capacity.resident.available(), before - 1);
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    assert_eq!(store.host_capacity.resident.available(), before);
    assert_eq!(
        store.host_capacity.execution.available(),
        store.host_capacity.limits.max_execution_jobs
    );
}

#[test]
fn native_cleanup_receipt_is_exact_state_fenced_and_does_not_refund_unknown_cost() {
    let (_dir, store, service, m, policy) = setup();
    let mut evidence = receipt(&service, &m, &policy);
    let session = uuid::Uuid::new_v4();
    let lease = uuid::Uuid::new_v4();
    assert!(store
        .acquire_job(
            session,
            lease,
            &policy.model.identity,
            tachyon_model::broker::JobWorkload::Cpu {},
            1000
        )
        .unwrap()
        .is_some());
    evidence.records = vec![RecoveryRecord::NativeCleanup {
        work_id: "work".into(),
        attempt_id: "attempt".into(),
        generation: 1,
        lease_id: lease.to_string(),
        confirmation: CleanupConfirmation::OperatorAttestsAllProcessesTerminated,
    }];
    assert!(service
        .reconcile(&m.campaign_id, &evidence, true, true)
        .is_err());
    evidence.expected_state_sha256 = receipt(&service, &m, &policy).expected_state_sha256;
    let mut stale = evidence.clone();
    if let RecoveryRecord::NativeCleanup { generation, .. } = &mut stale.records[0] {
        *generation += 1;
    }
    assert!(service
        .reconcile(&m.campaign_id, &stale, true, true)
        .is_err());
    assert!(service
        .reconcile(&m.campaign_id, &evidence, true, false)
        .is_err());
    service
        .reconcile(&m.campaign_id, &evidence, true, true)
        .unwrap();
    service
        .reconcile(&m.campaign_id, &evidence, true, true)
        .unwrap();
    let diagnostics = service.reconciliation_inspection(&m.campaign_id).unwrap();
    let job = diagnostics
        .iter()
        .find(|line| line.starts_with("native_job_wall_time:"))
        .unwrap();
    assert!(job.contains("\"final_ms\":1000"));
    assert_eq!(
        store.host_capacity.cpu.available_permits(),
        store.host_capacity.limits.max_cpu_jobs
    );
    store.release_job(session, lease, true).unwrap();
    assert_eq!(
        store.host_capacity.cpu.available_permits(),
        store.host_capacity.limits.max_cpu_jobs
    );
}

#[test]
fn reconciliation_hash_fences_every_recovery_state_table() {
    let (_dir, store, service, m, policy) = setup();
    let receipt = receipt(&service, &m, &policy);
    for name in [
        "campaigns",
        "local_campaign_launches_v1",
        "campaign_ledger_roots",
        "campaign_admitted_work",
        "campaign_executions",
        "campaign_command_gates_v1",
        "campaign_model_requests",
        "campaign_model_dispatches",
        "campaign_work_limits",
        "campaign_coordination_v1",
        "local_reconciliation_receipts_v1",
        "local_reconciliation_bindings_v1",
    ] {
        // Hashing must include every row, not just the receipt's named targets.
        let definition = TableDefinition::<&str, &[u8]>::new(name);
        let tx = store.database.begin_write().unwrap();
        tx.open_table(definition)
            .unwrap()
            .insert("unrelated-descendant", b"{}".as_slice())
            .unwrap();
        tx.commit().unwrap();
        assert!(
            service
                .reconcile(&m.campaign_id, &receipt, true, true)
                .unwrap_err()
                .contains("stale reconciliation state"),
            "{name}"
        );
        let tx = store.database.begin_write().unwrap();
        tx.open_table(definition)
            .unwrap()
            .remove("unrelated-descendant")
            .unwrap();
        tx.commit().unwrap();
    }
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
}

#[test]
fn reconciliation_competing_receipts_have_one_winner_and_fence_old_execution() {
    let (_dir, store, service, m, policy) = setup();
    let first = receipt(&service, &m, &policy);
    let mut second = first.clone();
    second.command_id = "operator-2".into();
    let previous = store
        .campaign_execution(&m.campaign_id, "work")
        .unwrap()
        .unwrap();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let threads: Vec<_> = [&first, &second]
            .into_iter()
            .map(|receipt| {
                let service = &service;
                let campaign = &m.campaign_id;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    service.reconcile(campaign, receipt, true, true)
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(results
        .iter()
        .find_map(|r| r.as_ref().err())
        .unwrap()
        .contains("stale reconciliation state"));
    let tx = store.database.begin_write().unwrap();
    let mut late = previous.clone();
    late.phase = ExecutionPhase::EvidenceReady;
    assert!(RuntimeStore::update_execution_in(&tx, &previous, &late)
        .unwrap_err()
        .contains("execution transition conflict"));
    drop(tx);
    assert_eq!(
        store
            .dispatch_campaign_batch(4, |_| panic!("replayed"))
            .unwrap(),
        0
    );
    assert_eq!(status(&store, &m), CampaignStatus::Unverified);
}

#[test]
fn reconciliation_atomic_conflicts_flags_debt_idempotency_and_restart_without_replay() {
    let (dir, store, service, m, policy) = setup();
    let receipt = receipt(&service, &m, &policy);
    let before = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
    for (authorized, authoritative) in [(false, false), (true, false), (false, true)] {
        assert!(service
            .reconcile(&m.campaign_id, &receipt, authorized, authoritative)
            .is_err());
    }
    for mutation in 0..8 {
        let mut changed = receipt.clone();
        match mutation {
            0 => changed.expected_state_sha256 = "0".repeat(64),
            1 => {
                if let RecoveryRecord::ModelUsage { request_id, .. } = &mut changed.records[0] {
                    *request_id = "other".into();
                }
            }
            2 => {
                if let RecoveryRecord::ModelUsage { allocation_id, .. } = &mut changed.records[0] {
                    *allocation_id = "other".into();
                }
            }
            3 => {
                if let RecoveryRecord::ModelUsage { generation, .. } = &mut changed.records[0] {
                    *generation += 1;
                }
            }
            4 => {
                if let RecoveryRecord::ModelUsage { attempt_id, .. } = &mut changed.records[0] {
                    *attempt_id = "other".into();
                }
            }
            5 => {
                if let RecoveryRecord::Cleanup { generation, .. } = &mut changed.records[1] {
                    *generation += 1;
                }
            }
            6 => changed.records.push(changed.records[0].clone()),
            _ => changed.campaign_id = "other".into(),
        }
        assert!(
            service
                .reconcile(&m.campaign_id, &changed, true, true)
                .is_err(),
            "mutation {mutation}"
        );
        assert_eq!(
            store.campaign_ledger(&m.campaign_id).unwrap().unwrap(),
            before,
            "whole receipt rolled back: {mutation}"
        );
        assert_eq!(
            store
                .campaign_execution(&m.campaign_id, "work")
                .unwrap()
                .unwrap()
                .phase,
            ExecutionPhase::ExecutingUnknown
        );
    }
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
    assert!(ledger.admissions_paused);
    assert_eq!(ledger.debt.cost_micro_usd, 5);
    assert_eq!(ledger.envelope, before.envelope);
    assert_eq!(ledger.allocations[&policy.funding.dispatch_id], true);
    assert!(
        store
            .campaign_execution(&m.campaign_id, "work")
            .unwrap()
            .unwrap()
            .settled
    );
    assert_eq!(status(&store, &m), CampaignStatus::Unverified);
    assert!(service.ready_to_resume(&m.campaign_id).is_err());
    assert!(service.active.lock().unwrap().is_empty());
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    let mut changed = receipt.clone();
    changed.evidence_reference = "other".into();
    assert!(service
        .reconcile(&m.campaign_id, &changed, true, true)
        .unwrap_err()
        .contains("conflict"));
    changed.campaign_id = "other-campaign".into();
    assert!(service
        .reconcile("other-campaign", &changed, true, true)
        .unwrap_err()
        .contains("scope conflict"));
    drop(service);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    assert_eq!(status(&store, &m), CampaignStatus::Unverified);
    assert_eq!(
        store.campaign_ledger(&m.campaign_id).unwrap().unwrap(),
        ledger
    );
    assert!(service.active.lock().unwrap().is_empty());
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    assert_eq!(
        store
            .dispatch_campaign_batch(4, |_| panic!("replayed"))
            .unwrap(),
        0
    );
}

#[test]
fn reconciliation_cleanup_never_settles_model_billing_and_final_usage_is_monotonic() {
    let (_dir, store, service, m, policy) = setup();
    store
        .campaign_ledger_command(
            "provisional",
            &m.campaign_id,
            LedgerCommand::Reconcile {
                reservation_id: "model:fixture".into(),
                usage: Usage::Provisional(Units {
                    tokens: 31,
                    cost_micro_usd: 35,
                }),
            },
        )
        .unwrap();
    let mut receipt = receipt(&service, &m, &policy);
    let billing = receipt.records.remove(0);
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    let record = store
        .campaign_execution(&m.campaign_id, "work")
        .unwrap()
        .unwrap();
    assert_eq!(
        record.phase,
        ExecutionPhase::Reviewed(Evaluation::Unverified)
    );
    assert!(!record.settled);
    assert_eq!(
        store
            .campaign_ledger(&m.campaign_id)
            .unwrap()
            .unwrap()
            .allocations[&policy.funding.dispatch_id],
        false
    );
    receipt.command_id = "operator-2".into();
    receipt.expected_state_sha256 = service.reconciliation_inspection(&m.campaign_id).unwrap()[0]
        .split(": ")
        .nth(1)
        .unwrap()
        .into();
    receipt.records = vec![billing];
    let mut lower = receipt.clone();
    if let RecoveryRecord::ModelUsage { cost_micro_usd, .. } = &mut lower.records[0] {
        *cost_micro_usd = 34;
    }
    assert!(service
        .reconcile(&m.campaign_id, &lower, true, true)
        .unwrap_err()
        .contains("decrease"));
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    assert!(
        store
            .campaign_execution(&m.campaign_id, "work")
            .unwrap()
            .unwrap()
            .settled
    );
    receipt.command_id = "operator-3".into();
    receipt.expected_state_sha256 = service.reconciliation_inspection(&m.campaign_id).unwrap()[0]
        .split(": ")
        .nth(1)
        .unwrap()
        .into();
    let mut rebound = receipt.clone();
    if let RecoveryRecord::ModelUsage {
        provider_request_id,
        ..
    } = &mut rebound.records[0]
    {
        *provider_request_id = "different-provider-request".into();
    }
    assert!(service
        .reconcile(&m.campaign_id, &rebound, true, true)
        .unwrap_err()
        .contains("binding conflict"));
    if let RecoveryRecord::ModelUsage { cost_micro_usd, .. } = &mut receipt.records[0] {
        *cost_micro_usd = 36;
    }
    assert!(service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap_err()
        .contains("immutable final"));
}

#[test]
fn reconciliation_billing_alone_does_not_terminate_execution() {
    let (_dir, store, service, m, policy) = setup();
    let mut receipt = receipt(&service, &m, &policy);
    receipt.records.truncate(1);
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    let record = store
        .campaign_execution(&m.campaign_id, "work")
        .unwrap()
        .unwrap();
    assert_eq!(record.phase, ExecutionPhase::ExecutingUnknown);
    assert!(!record.settled);
    assert!(
        !store
            .campaign_ledger(&m.campaign_id)
            .unwrap()
            .unwrap()
            .allocations[&policy.funding.dispatch_id]
    );
    assert_eq!(status(&store, &m), CampaignStatus::Unverified);
    assert!(service.active.lock().unwrap().is_empty());
}

#[test]
fn reconciliation_rejects_active_task_even_after_cancel_signal() {
    let (_dir, store, service, m, policy) = setup();
    let receipt = receipt(&service, &m, &policy);
    let (release, wait) = std::sync::mpsc::channel();
    let (cancel, _) = watch::channel(true);
    service.active.lock().unwrap().insert(
        m.campaign_id.clone(),
        Active {
            id: m.campaign_id.clone(),
            cancel,
            task: std::thread::spawn(move || {
                wait.recv().unwrap();
            }),
        },
    );
    let result = service.reconcile(&m.campaign_id, &receipt, true, true);
    release.send(()).unwrap();
    service
        .active
        .lock()
        .unwrap()
        .remove(&m.campaign_id)
        .unwrap()
        .task
        .join()
        .unwrap();
    assert!(result.unwrap_err().contains("scheduler tasks"));
    assert_eq!(
        store
            .campaign_execution(&m.campaign_id, "work")
            .unwrap()
            .unwrap()
            .phase,
        ExecutionPhase::ExecutingUnknown
    );
}

#[test]
fn reconciliation_review_cleanup_preserves_candidate_and_cancelled_status_not_billing() {
    use crate::runtime_store::execution::{
        command::{CommandGateRecord, COMMAND_GATES},
        evaluation_receipt,
    };
    let (_dir, store, service, m, mut policy) = setup();
    let config = CommandEvaluator {
        acceptance_mode: None,
        result_contract: Default::default(),
        metrics: Default::default(),
        allow_extra_metrics: false,
        stage: None,
        argv: m.evaluator.argv.clone(),
        cwd: ".".into(),
        timeout_ms: 100,
        output_bytes: 1024,
        input_bytes: 1024,
        max_attempts: 1,
        max_total_command_ms: 100,
    };
    policy.evaluator_id = format!("command:{}", config.config_hash().unwrap());
    store
        .campaign_ledger_command(
            "review-fund",
            &m.campaign_id,
            LedgerCommand::FundAllocation {
                reservation_id: policy.verification.dispatch_id.clone(),
                work_id: "review".into(),
            },
        )
        .unwrap();
    let evaluation = evaluation_receipt(&policy);
    store
        .campaign_ledger_command(
            "review-reserve",
            &m.campaign_id,
            LedgerCommand::ReserveAllocated {
                reservation_id: evaluation.clone(),
                allocation_id: policy.verification.dispatch_id.clone(),
                pool: Pool::Verification,
                reserved: Units {
                    tokens: 10,
                    cost_micro_usd: 10,
                },
            },
        )
        .unwrap();
    let candidate = serde_json::json!({"work_id":"work","objective":"fixture","generation":1,"assignment":1,
        "outcome":"completed","result":"retained candidate","artifacts":[]});
    let tx = store.database.begin_write().unwrap();
    tx.open_table(EXECUTIONS).unwrap().insert("work", serde_json::to_vec(&serde_json::json!({
        "schema_version":1,"policy":policy,"phase":"ReviewingUnknown","candidate":candidate,"settled":false
    })).unwrap().as_slice()).unwrap();
    tx.open_table(COMMAND_GATES)
        .unwrap()
        .insert(
            "work",
            serde_json::to_vec(&CommandGateRecord {
                human_request: None,
                human_receipt: None,
                human_diagnostic: None,
                policy: policy.clone(),
                config,
                snapshot: None,
                select_collected: false,
                staging_root: "/nonexistent/staging".into(),
                evidence: None,
                evidence_at_ms: None,
                finalize_rework: false,
                original_policy: None,
                history: vec![],
                deadline_ms: None,
            })
            .unwrap()
            .as_slice(),
        )
        .unwrap();
    RuntimeStore::set_campaign_status_in(&tx, &m.campaign_id, CampaignStatus::Cancelled).unwrap();
    tx.commit().unwrap();
    let before = store
        .campaign_execution(&m.campaign_id, "work")
        .unwrap()
        .unwrap();
    let mut receipt = receipt(&service, &m, &policy);
    receipt.records.remove(0);
    service
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    let after = store
        .campaign_execution(&m.campaign_id, "work")
        .unwrap()
        .unwrap();
    assert_eq!(after.candidate, before.candidate);
    assert_eq!(
        after.phase,
        ExecutionPhase::Reviewed(Evaluation::Unverified)
    );
    assert!(!after.settled);
    assert_eq!(status(&store, &m), CampaignStatus::Cancelled);
    let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
    assert_eq!(ledger.reservations["model:fixture"].usage, Usage::Unknown);
    assert_eq!(
        ledger.reservations[&evaluation].usage,
        Usage::Final(Units::default())
    );
    assert!(!ledger.allocations[&policy.funding.dispatch_id]);
}
