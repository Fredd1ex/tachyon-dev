use super::*;
use crate::runtime_store::execution::{ExecutionPhase, ExecutionRecord, EXECUTIONS, VERIFIERS};
use tachyon_api::campaign::{
    AllocationAction, AllocationControl, AllocationMode, AllocationSignal, CampaignAllocation,
};

fn allocation(action: AllocationControl) -> CampaignAllocation {
    CampaignAllocation {
        mode: AllocationMode::Deterministic,
        max_running: 2,
        allowed_actions: vec![
            AllocationAction::Work,
            AllocationAction::Verify,
            AllocationAction::Stop,
        ],
        signals: vec![AllocationSignal {
            command_id: "policy-once".into(),
            group_id: "children".into(),
            expected_revision: 1,
            action,
        }],
    }
}

fn groups() -> Vec<(String, Vec<String>)> {
    vec![("children".into(), vec!["child-a".into(), "child-b".into()])]
}

#[tokio::test]
async fn allocation_work_catalog_commit_replay_and_stale_transaction() {
    for failure in [
        "none",
        "nested",
        "budget",
        "manual",
        "instructions",
        "generation",
        "owner",
        "scope",
    ] {
        let (dir, store, root, template) = setup(limits());
        let c = template.parent.campaign_id.clone();
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
        let scheduler = HostScheduler::command_children(broker, 3).unwrap();
        scheduler.approve(template.clone()).unwrap();
        let permit = register(&store, &root);
        store
            .broker_control(permit.0, &root.policy.model, group())
            .unwrap();
        let mut next = template.clone();
        if failure == "nested" {
            let child = store.host_catalog.lock().unwrap().admitted
                [&(c.clone(), "child-a".into(), 1)]
                .clone();
            register(&store, &child);
            next.parent.work_id = "child-a".into();
        }
        if failure == "scope" {
            next.parent.work_id = "verify-parent".into();
        }
        next.template_id = "next-approved".into();
        next.group_id = Some("next-group".into());
        for (i, candidate) in next.candidates.iter_mut().enumerate() {
            let id = format!("next-{i}");
            candidate.admission.work_id = id.clone();
            candidate.verification.work_id = format!("verify-{id}");
            candidate.model.identity.work_id = id.clone();
            candidate.work.work_id = id;
        }
        if failure == "budget" {
            next.candidates[1].verification.upper_bound.tokens = 1001;
        }
        scheduler.approve(next.clone()).unwrap();
        store.select_allocation_owner(&c, "owner").unwrap();
        let config = allocation(AllocationControl::Work {
            template_id: next.template_id.clone(),
            parent_work_id: next.parent.work_id.clone(),
            generation: if failure == "generation" { 2 } else { 1 },
            instruction_revision: if failure == "instructions" { 2 } else { 1 },
        });
        let (context, replay) = store
            .allocation_action_context(&c, "owner", &config.signals[0], &groups())
            .unwrap()
            .unwrap();
        assert!(!replay);
        if failure == "manual" {
            store.resize_campaign_group(&c, "children", 1, 1).unwrap();
        }
        if failure == "owner" {
            store.select_allocation_owner(&c, "replacement").unwrap();
        }
        let before = store.campaign_group_status(&c, "children").unwrap();
        let ledger = store.campaign_ledger(&c).unwrap();
        let request = Request::Group {
            template_id: next.template_id.clone(),
            command_id: "policy-once".into(),
            max_running: None,
        };
        let result =
            store.admit_catalog_action(next.parent.clone(), request.clone(), None, Some(&context));
        if !["none", "nested"].contains(&failure) {
            assert!(result.is_err(), "{failure}");
            assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
            assert_eq!(store.campaign_group_status(&c, "children").unwrap(), before);
            assert!(store.admitted_work(&c, "next-0").is_err());
            assert!(store.campaign_group_status(&c, "next-group").is_err());
            continue;
        }
        result.unwrap();
        let after = store.campaign_group_status(&c, "children").unwrap();
        assert_eq!(after.0.revision, before.0.revision + 1);
        let admitted_ledger = store.campaign_ledger(&c).unwrap();
        assert_eq!(
            admitted_ledger.as_ref().unwrap().envelope,
            ledger.as_ref().unwrap().envelope
        );
        assert_eq!(
            admitted_ledger.as_ref().unwrap().reservations.len(),
            ledger.as_ref().unwrap().reservations.len() + 4
        );
        // A lost publication is recoverable by exact committed receipt replay.
        store
            .host_catalog
            .lock()
            .unwrap()
            .admitted
            .remove(&(c.clone(), "next-0".into(), 1));
        let keys = vec![template.template_id.clone(), next.template_id.clone()];
        assert!(!store
            .allocation_execution_tick(&c, "owner", &config, &groups(), &keys)
            .unwrap());
        assert!(store.host_catalog.lock().unwrap().admitted.contains_key(&(
            c.clone(),
            "next-0".into(),
            1
        )));
        assert_eq!(store.campaign_ledger(&c).unwrap(), admitted_ledger);
        assert_eq!(store.campaign_group_status(&c, "children").unwrap(), after);
        let mut collision = context.clone();
        collision.signal.expected_revision += 1;
        collision.fence.group_revision += 1;
        assert!(store
            .admit_catalog_action(next.parent.clone(), request, None, Some(&collision))
            .is_err());
        drop(scheduler);
        drop(store);
        let reopened = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let scheduler = HostScheduler::command_children(
            Arc::new(ModelBroker::new(
                reopened.clone(),
                model(&root.policy.model),
            )),
            3,
        )
        .unwrap();
        scheduler.approve(template).unwrap();
        scheduler.approve(next).unwrap();
        assert!(!reopened
            .allocation_execution_tick(&c, "owner", &config, &groups(), &keys)
            .unwrap());
        assert_eq!(reopened.campaign_ledger(&c).unwrap(), admitted_ledger);
        assert_eq!(
            reopened.campaign_group_status(&c, "children").unwrap(),
            after
        );
    }
}

// Seed host-collected evidence, not provider output or policy-generated work.
fn awaiting(store: &RuntimeStore, campaign: &str) -> ExecutionRecord {
    let claim = store
        .claim_campaign_work_matching(|w| w.admission.work_id == "child-a")
        .unwrap()
        .unwrap();
    store
        .reconcile_campaign_dispatch(
            &claim,
            DispatchOutcome::Registered {
                worker_id: "fixture-worker".into(),
            },
        )
        .unwrap();
    let funding = store.admitted_work(campaign, "child-a").unwrap();
    store
        .campaign_ledger_command(
            "fund-fixture",
            campaign,
            LedgerCommand::FundAllocation {
                reservation_id: funding.dispatch_id.clone(),
                work_id: "child-a".into(),
            },
        )
        .unwrap();
    let mut policy = store.host_catalog.lock().unwrap().admitted
        [&(campaign.into(), "child-a".into(), 1)]
        .policy
        .clone();
    policy.funding = funding.clone();
    let record: ExecutionRecord = serde_json::from_value(serde_json::json!({
        "schema_version":1, "policy":policy, "phase":"AwaitingVerification", "settled":false,
        "candidate":{"work_id":"child-a", "objective":policy.work.objective,
            "generation":1, "assignment":1, "outcome":"completed", "result":"known evidence"}
    }))
    .unwrap();
    store.host_acknowledge_work_terminal(&funding).unwrap();
    let tx = store.database.begin_write().unwrap();
    tx.open_table(EXECUTIONS)
        .unwrap()
        .insert("child-a", serde_json::to_vec(&record).unwrap().as_slice())
        .unwrap();
    tx.open_table(VERIFIERS)
        .unwrap()
        .insert(policy.verification.dispatch_id.as_str(), "child-a")
        .unwrap();
    tx.commit().unwrap();
    record
}

#[tokio::test]
async fn allocation_verify_intent_claim_is_atomic_and_unknown_never_replays() {
    for failure in [
        "none",
        "funded",
        "manual",
        "claim-conflict",
        "lost-review",
        "descriptor",
        "unfunded",
        "unknown",
        "accepted",
        "instructions",
        "generation",
    ] {
        let (_dir, store, root, template) = setup(limits());
        let c = template.parent.campaign_id.clone();
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
        let scheduler = HostScheduler::command_children(broker.clone(), 3).unwrap();
        scheduler.approve(template.clone()).unwrap();
        let permit = register(&store, &root);
        store
            .broker_control(permit.0, &root.policy.model, group())
            .unwrap();
        let mut ready = awaiting(&store, &c);
        if failure == "unfunded" {
            store
                .campaign_ledger_command(
                    "unspent-verifier-fixture",
                    &c,
                    LedgerCommand::Reconcile {
                        reservation_id: ready.policy.verification.dispatch_id.clone(),
                        usage: Usage::Final(Units::default()),
                    },
                )
                .unwrap();
        }
        if failure == "funded" {
            use crate::runtime_store::admission::{PENDING, WORK};
            let tx = store.database.begin_write().unwrap();
            let mut verifier = ready.policy.verification.clone();
            assert!(RuntimeStore::group_claim_in(&tx, &verifier).unwrap());
            verifier.state = DispatchState::Registered {
                worker_id: ready.policy.evaluator_id.clone(),
            };
            tx.open_table(WORK)
                .unwrap()
                .insert(
                    verifier.admission.work_id.as_str(),
                    serde_json::to_vec(&verifier).unwrap().as_slice(),
                )
                .unwrap();
            tx.open_table(PENDING)
                .unwrap()
                .remove(verifier.admission.work_id.as_str())
                .unwrap();
            RuntimeStore::campaign_ledger_command_in(
                &tx,
                "pre-funded-verifier",
                &c,
                LedgerCommand::FundAllocation {
                    reservation_id: verifier.dispatch_id,
                    work_id: verifier.admission.work_id,
                },
            )
            .unwrap();
            tx.commit().unwrap();
        }
        if failure == "unknown" {
            ready.phase = ExecutionPhase::ReviewingUnknown;
        }
        if failure == "accepted" {
            ready.phase = ExecutionPhase::Reviewed(Evaluation::Accepted);
        }
        if failure == "unknown" || failure == "accepted" {
            let tx = store.database.begin_write().unwrap();
            tx.open_table(EXECUTIONS)
                .unwrap()
                .insert("child-a", serde_json::to_vec(&ready).unwrap().as_slice())
                .unwrap();
            tx.commit().unwrap();
        }
        store.select_allocation_owner(&c, "owner").unwrap();
        let config = allocation(AllocationControl::Verify {
            work_id: "child-a".into(),
            generation: if failure == "generation" { 2 } else { 1 },
            instruction_revision: if failure == "instructions" { 2 } else { 1 },
        });
        let (context, _) = store
            .allocation_action_context(&c, "owner", &config.signals[0], &groups())
            .unwrap()
            .unwrap();
        let mut approved = ready.policy.clone();
        if failure == "descriptor" {
            approved.evaluator_id.push_str("-different");
        }
        let before = store.campaign_group_status(&c, "children").unwrap();
        let ledger = store.campaign_ledger(&c).unwrap();
        let staged = store.stage_allocation_review(&context, &approved);
        if !["none", "funded", "manual", "claim-conflict", "lost-review"].contains(&failure) {
            assert!(staged.is_err(), "{failure}");
            assert_eq!(store.campaign_group_status(&c, "children").unwrap(), before);
            assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
            continue;
        }
        staged.unwrap();
        store.stage_allocation_review(&context, &approved).unwrap();
        assert_eq!(
            store
                .campaign_group_status(&c, "children")
                .unwrap()
                .0
                .revision,
            1,
            "intent is not the claim receipt"
        );
        assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
        if failure == "manual" {
            store.resize_campaign_group(&c, "children", 1, 1).unwrap();
        }
        if failure == "claim-conflict" {
            store
                .campaign_ledger_command(
                    &crate::runtime_store::execution::evaluation_receipt(&ready.policy),
                    &c,
                    LedgerCommand::ReserveAllocated {
                        reservation_id: "conflicting-command".into(),
                        allocation_id: ready.policy.funding.dispatch_id.clone(),
                        pool: Pool::Work,
                        reserved: Units {
                            tokens: 1,
                            cost_micro_usd: 1,
                        },
                    },
                )
                .unwrap();
        }
        let claim_ledger = store.campaign_ledger(&c).unwrap();
        let claim_group = store.campaign_group_status(&c, "children").unwrap();
        if failure == "lost-review" {
            let runner = broker.clone();
            let run_root = root.clone();
            let policy = ready.policy.clone();
            assert!(tokio::spawn(async move {
                runner
                    .execute_campaign(
                        &run_root.executable,
                        &run_root.workspace,
                        &run_root.home,
                        policy,
                        Instant::now() + Duration::from_secs(5),
                        |_| async { panic!("lost evaluator after durable claim") },
                    )
                    .await
            })
            .await
            .is_err());
            assert_eq!(
                store
                    .campaign_execution(&c, "child-a")
                    .unwrap()
                    .unwrap()
                    .phase,
                ExecutionPhase::ReviewingUnknown
            );
            assert_eq!(
                store
                    .campaign_group_status(&c, "children")
                    .unwrap()
                    .0
                    .revision,
                2
            );
            let unknown = store.campaign_ledger(&c).unwrap();
            assert!(!store
                .allocation_execution_tick(&c, "owner", &config, &groups(), &[template.template_id])
                .unwrap());
            let replayed = broker
                .execute_campaign(
                    &root.executable,
                    &root.workspace,
                    &root.home,
                    ready.policy,
                    Instant::now() + Duration::from_secs(5),
                    |_| async { panic!("unknown review replay") },
                )
                .await
                .unwrap();
            assert_eq!(replayed.phase, ExecutionPhase::ReviewingUnknown);
            assert!(!replayed.settled);
            assert_eq!(store.campaign_ledger(&c).unwrap(), unknown);
            continue;
        }
        let evaluated = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let called = evaluated.clone();
        let result = broker
            .execute_campaign(
                &root.executable,
                &root.workspace,
                &root.home,
                ready.policy.clone(),
                Instant::now() + Duration::from_secs(5),
                move |_| async move {
                    called.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Evaluation::Accepted
                },
            )
            .await;
        if failure == "manual" || failure == "claim-conflict" {
            assert!(result.is_err());
            assert_eq!(store.campaign_ledger(&c).unwrap(), claim_ledger);
            assert_eq!(
                store.campaign_group_status(&c, "children").unwrap(),
                claim_group
            );
            assert_eq!(
                store.campaign_execution(&c, "child-a").unwrap(),
                Some(ready)
            );
            assert_eq!(evaluated.load(std::sync::atomic::Ordering::SeqCst), 0);
            continue;
        }
        let reviewed = result.unwrap();
        assert_eq!(
            reviewed.phase,
            ExecutionPhase::Reviewed(Evaluation::Accepted)
        );
        assert!(reviewed.settled);
        assert_eq!(evaluated.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            store
                .campaign_group_status(&c, "children")
                .unwrap()
                .0
                .revision,
            2
        );
        let settled = store.campaign_ledger(&c).unwrap();
        assert!(!store
            .allocation_execution_tick(&c, "owner", &config, &groups(), &[template.template_id])
            .unwrap());
        broker
            .execute_campaign(
                &root.executable,
                &root.workspace,
                &root.home,
                ready.policy,
                Instant::now() + Duration::from_secs(5),
                |_| async { panic!("review replay") },
            )
            .await
            .unwrap();
        assert_eq!(store.campaign_ledger(&c).unwrap(), settled);
    }
}

#[tokio::test]
async fn allocation_verify_scheduler_reconciles_reopened_exact_intent() {
    let (dir, store, root, mut template) = setup(limits());
    let c = template.parent.campaign_id.clone();
    let evaluated = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let called = evaluated.clone();
    template.candidates[0].evaluate =
        crate::runtime_store::scheduler::Evaluator::Callback(Arc::new(move |_| {
            let called = called.clone();
            Box::pin(async move {
                called.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Evaluation::Accepted
            })
        }));
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let scheduler = HostScheduler::command_children(broker, 2).unwrap();
    scheduler.approve(template.clone()).unwrap();
    let permit = register(&store, &root);
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    let ready = awaiting(&store, &c);
    store.host_cancel_work(&c, "child-b", 1).unwrap();
    let config = allocation(AllocationControl::Verify {
        work_id: "child-a".into(),
        generation: 1,
        instruction_revision: 1,
    });
    store.select_allocation_owner(&c, "previous-owner").unwrap();
    assert!(store
        .allocation_execution_tick(
            &c,
            "previous-owner",
            &config,
            &groups(),
            &[template.template_id.clone()]
        )
        .unwrap());
    let ledger = store.campaign_ledger(&c).unwrap();
    drop(scheduler);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    assert_eq!(
        store.campaign_execution(&c, "child-a").unwrap(),
        Some(ready)
    );
    assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let mut scheduler = HostScheduler::command_children(broker, 2).unwrap();
    scheduler.approve(template.clone()).unwrap();
    store.admit_catalog(template.parent, group()).unwrap();
    scheduler.select_allocation(&c, Some(config)).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            scheduler.tick().await.unwrap();
            if store
                .campaign_execution(&c, "child-a")
                .unwrap()
                .unwrap()
                .settled
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(evaluated.load(std::sync::atomic::Ordering::SeqCst), 1);
    let after = store.campaign_ledger(&c).unwrap();
    for _ in 0..3 {
        scheduler.tick().await.unwrap();
    }
    assert_eq!(store.campaign_ledger(&c).unwrap(), after);
    assert_eq!(evaluated.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        store
            .campaign_group_status(&c, "children")
            .unwrap()
            .0
            .revision,
        2
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake provider only"]
async fn allocation_real_work_signal_dispatches_only_exact_catalog() {
    assert!(std::env::var_os("GHOST_TEST_BIN").is_some());
    let (_dir, store, root, template) = setup(limits());
    let c = template.parent.campaign_id.clone();
    let (url, mut wire, http) = fixture(store.clone(), "valid").await;
    let mut next = template.clone();
    next.template_id = "exact-spawn".into();
    next.group_id = None;
    next.candidates.truncate(1);
    let child = &mut next.candidates[0];
    child.work.work_id = "explicit-next".into();
    child.admission.work_id = child.work.work_id.clone();
    child.verification.work_id = "verify-explicit-next".into();
    child.model.identity.work_id = child.work.work_id.clone();
    child.model.estimate.base_url = url;
    child.model.estimate.max_request_bytes = 32000;
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&child.model)));
    let mut scheduler = HostScheduler::command_children(broker, 2).unwrap();
    scheduler.approve(template).unwrap();
    scheduler.approve(next).unwrap();
    let permit = register(&store, &root);
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    for id in ["child-a", "child-b"] {
        store.host_cancel_work(&c, id, 1).unwrap();
    }
    let envelope = store.campaign_ledger(&c).unwrap().unwrap().envelope;
    scheduler
        .select_allocation(
            &c,
            Some(allocation(AllocationControl::Work {
                template_id: "exact-spawn".into(),
                parent_work_id: "parent".into(),
                generation: 1,
                instruction_revision: 1,
            })),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            scheduler.tick().await.unwrap();
            if store
                .campaign_execution(&c, "explicit-next")
                .unwrap()
                .is_some_and(|r| r.settled)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(wire.recv().await.is_some());
    let after = store.campaign_ledger(&c).unwrap().unwrap();
    assert_eq!(after.envelope, envelope);
    assert_eq!(
        store
            .campaign_execution(&c, "explicit-next")
            .unwrap()
            .unwrap()
            .phase,
        ExecutionPhase::Reviewed(Evaluation::Accepted)
    );
    for _ in 0..3 {
        scheduler.tick().await.unwrap();
    }
    assert!(wire.try_recv().is_err());
    assert_eq!(store.campaign_ledger(&c).unwrap().unwrap(), after);
    scheduler.shutdown().await.unwrap();
    http.abort();
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake provider only"]
async fn allocation_real_stop_signal_cleans_process_but_retains_unknown_billing() {
    assert!(std::env::var_os("GHOST_TEST_BIN").is_some());
    let (_dir, store, root, mut template) = setup(limits());
    let c = template.parent.campaign_id.clone();
    let (url, mut wire, http) = fixture(store.clone(), "stall").await;
    for candidate in &mut template.candidates {
        candidate.model.estimate.base_url = url.clone();
        candidate.model.estimate.max_request_bytes = 32000;
    }
    let broker = Arc::new(ModelBroker::new(
        store.clone(),
        model(&template.candidates[0].model),
    ));
    let mut scheduler = HostScheduler::command_children(broker.clone(), 2).unwrap();
    scheduler.approve(template).unwrap();
    let permit = register(&store, &root);
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    assert_eq!(scheduler.tick().await.unwrap(), 1);
    tokio::time::timeout(Duration::from_secs(10), wire.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(store.campaign_work_status(&c, "child-a").unwrap().active);
    scheduler
        .select_allocation(
            &c,
            Some(allocation(AllocationControl::Stop {
                work_id: "child-a".into(),
                generation: 1,
            })),
        )
        .unwrap();
    scheduler.tick().await.unwrap();
    assert!(
        store
            .campaign_work_status(&c, "child-a")
            .unwrap()
            .cancellation_requested
    );
    assert!(
        !store
            .campaign_work_status(&c, "child-b")
            .unwrap()
            .cancellation_requested
    );
    // Keep the sibling queued while reaping the selected branch's cleanup.
    store.resize_campaign_group(&c, "children", 2, 0).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while scheduler.tick().await.unwrap() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let status = store.campaign_work_status(&c, "child-a").unwrap();
    assert!(status.terminal && !status.active);
    assert_eq!(store.campaign_group_status(&c, "children").unwrap().2, 0);
    assert!(!broker
        .launches
        .lock()
        .unwrap()
        .contains_key(&(c.clone(), "child-a".into(), 1)));
    let record = store.campaign_execution(&c, "child-a").unwrap().unwrap();
    assert!(
        !record.settled,
        "process exit does not settle a stalled provider request"
    );
    let ledger = store.campaign_ledger(&c).unwrap().unwrap();
    assert!(ledger.reservations.values().any(|r| r.allocation.as_deref()
        == Some(&record.policy.funding.dispatch_id)
        && !matches!(r.usage, Usage::Final(_))));
    assert_eq!(scheduler.tick().await.unwrap(), 0);
    assert_eq!(store.campaign_ledger(&c).unwrap().unwrap(), ledger);
    scheduler.shutdown().await.unwrap();
    http.abort();
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake provider only"]
async fn allocation_real_verify_signal_resumes_collected_evidence_without_model_replay() {
    use crate::runtime_store::groups::GroupSpec;
    assert!(std::env::var_os("GHOST_TEST_BIN").is_some());
    let (_dir, store, root, mut template) = setup(limits());
    let c = template.parent.campaign_id.clone();
    let (url, mut wire, http) = fixture(store.clone(), "valid").await;
    let evaluated = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let called = evaluated.clone();
    template.candidates[0].model.estimate.base_url = url;
    template.candidates[0].model.estimate.max_request_bytes = 32000;
    template.candidates[0].evaluate =
        crate::runtime_store::scheduler::Evaluator::Callback(Arc::new(move |_| {
            let called = called.clone();
            Box::pin(async move {
                called.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Evaluation::Accepted
            })
        }));
    let verifier = template.candidates[0].verification.clone();
    let broker = Arc::new(ModelBroker::new(
        store.clone(),
        model(&template.candidates[0].model),
    ));
    let mut scheduler = HostScheduler::command_children(broker, 2).unwrap();
    scheduler.approve(template).unwrap();
    let permit = register(&store, &root);
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    store.host_cancel_work(&c, "child-b", 1).unwrap();
    // A separately paused host verifier group produces genuine deferred evidence
    // without steering/relinquishing the primary group's policy controller.
    store
        .create_campaign_group(GroupSpec {
            group_id: "verifier-capacity".into(),
            campaign_id: c.clone(),
            parent: None,
            max_running: 1,
            work: vec![Admission {
                work_id: "capacity-fixture".into(),
                upper_bound: Units {
                    tokens: 1,
                    cost_micro_usd: 1,
                },
                ..verifier.clone()
            }],
        })
        .unwrap();
    store
        .resize_campaign_group(&c, "verifier-capacity", 1, 0)
        .unwrap();
    let tx = store.database.begin_write().unwrap();
    RuntimeStore::inherit_work_group_in(&tx, &c, "capacity-fixture", &verifier.work_id).unwrap();
    tx.commit().unwrap();
    assert_eq!(scheduler.tick().await.unwrap(), 1);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if store
                .campaign_execution(&c, "child-a")
                .unwrap()
                .is_some_and(|r| r.phase == ExecutionPhase::AwaitingVerification)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(wire.recv().await.is_some());
    assert_eq!(evaluated.load(std::sync::atomic::Ordering::SeqCst), 0);
    let ready = store.campaign_execution(&c, "child-a").unwrap().unwrap();
    assert!(ready.candidate.is_some());
    scheduler
        .select_allocation(
            &c,
            Some(allocation(AllocationControl::Verify {
                work_id: "child-a".into(),
                generation: 1,
                instruction_revision: 1,
            })),
        )
        .unwrap();
    let waiting_ledger = store.campaign_ledger(&c).unwrap();
    for _ in 0..3 {
        scheduler.tick().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(evaluated.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(store.campaign_ledger(&c).unwrap(), waiting_ledger);
    assert_eq!(
        store
            .campaign_group_status(&c, "children")
            .unwrap()
            .0
            .revision,
        1
    );
    store
        .resize_campaign_group(&c, "verifier-capacity", 2, 1)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            scheduler.tick().await.unwrap();
            if store
                .campaign_execution(&c, "child-a")
                .unwrap()
                .unwrap()
                .settled
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let reviewed = store.campaign_execution(&c, "child-a").unwrap().unwrap();
    assert_eq!(reviewed.policy, ready.policy);
    assert_eq!(reviewed.candidate, ready.candidate);
    assert_eq!(
        reviewed.phase,
        ExecutionPhase::Reviewed(Evaluation::Accepted)
    );
    assert_eq!(evaluated.load(std::sync::atomic::Ordering::SeqCst), 1);
    let ledger = store.campaign_ledger(&c).unwrap();
    for _ in 0..3 {
        scheduler.tick().await.unwrap();
    }
    assert!(
        wire.try_recv().is_err(),
        "verification never reruns the model"
    );
    assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
    assert_eq!(
        store
            .campaign_group_status(&c, "children")
            .unwrap()
            .0
            .revision,
        2
    );
    scheduler.shutdown().await.unwrap();
    http.abort();
}
