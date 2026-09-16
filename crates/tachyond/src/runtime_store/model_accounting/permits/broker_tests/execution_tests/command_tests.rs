use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::fs::PermissionsExt;
use tachyon_api::types::{ArtifactPublication, ArtifactRegistration, EventEnvelope};
use tachyond::{
    artifact_store::ArtifactStore,
    verification::{CommandEvaluator, CommandOutcome},
};

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake model only"]
async fn actual_scheduler_pending_repair_resize_cancel_and_total_bound() {
    use crate::runtime_store::scheduler::{Evaluator, HostExecution, HostScheduler};
    for mode in [
        "resume",
        "cancel",
        "shutdown",
        "total",
        "review-cancel",
        "review-cancel-before",
    ] {
        let (_dir, store, funding, mut request) = tests::setup_with_capacity(true, 1);
        let store = Arc::new(store);
        let (url, mut wire, http) = fixture(store.clone(), "artifact-repair").await;
        request.estimate.base_url = url;
        request.estimate.max_request_bytes = 32000;
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        for dir in [&objects, &staging] {
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let artifacts = Arc::new(
            ArtifactStore::open_retained(
                objects.path(),
                store.retained.clone(),
                &funding.admission.campaign_id,
            )
            .unwrap(),
        );
        let config = CommandEvaluator {
            acceptance_mode: None,
            result_contract: Default::default(),
            metrics: Default::default(),
            allow_extra_metrics: false,
            stage: None,
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "test \"$(cat candidate)\" = corrected".into(),
            ],
            cwd: ".".into(),
            timeout_ms: 2000,
            output_bytes: 1024,
            input_bytes: 1024,
            max_attempts: 2,
            max_total_command_ms: 4000,
        };
        if mode.starts_with("review-cancel") {
            store
                .create_campaign_group(crate::runtime_store::groups::GroupSpec {
                    campaign_id: funding.admission.campaign_id.clone(),
                    group_id: "review-group".into(),
                    parent: None,
                    max_running: 1,
                    work: vec![Admission {
                        work_id: "verification-work".into(),
                        pool: Pool::Verification,
                        objective: "deterministic fixture evaluation".into(),
                        ..funding.admission.clone()
                    }],
                })
                .unwrap();
            store
                .resize_campaign_group(&funding.admission.campaign_id, "review-group", 1, 0)
                .unwrap();
        }
        let mut policy = policy(&store, &funding, request.clone());
        policy.evaluator_id = format!("command:{}", config.config_hash().unwrap());
        let executable = std::path::PathBuf::from(
            std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"),
        );
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&request)));
        let failed = broker
            .execute_campaign_command(
                &executable,
                workspace.path(),
                home.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(10),
                artifacts.clone(),
                staging.path().into(),
                config.clone(),
                None,
            )
            .await
            .unwrap();
        assert!(!failed.settled);
        if mode.starts_with("review-cancel") {
            assert_eq!(failed.phase, ExecutionPhase::AwaitingVerification);
        } else {
            assert!(failed.rework_pending);
        }
        let campaign = &funding.admission.campaign_id;
        let work = &funding.admission.work_id;
        assert!(!store.campaign_work_status(campaign, work).unwrap().terminal);
        for _ in 0..3 {
            wire.try_recv().unwrap();
        }
        assert!(wire.try_recv().is_err());
        store
            .resize_campaign_group(campaign, "execution-group", 1, 0)
            .unwrap();
        let mut scheduler = HostScheduler::new(
            broker,
            vec![HostExecution {
                policy: policy.clone(),
                executable,
                workspace: workspace.path().into(),
                home: home.path().into(),
                evaluate: Evaluator::Command {
                    artifacts,
                    staging: staging.path().into(),
                    config: config.clone(),
                },
            }],
            1,
        )
        .unwrap();
        if mode == "review-cancel-before" {
            store
                .cancel_campaign_group(campaign, "review-group", 2)
                .unwrap();
        }
        assert_eq!(
            store.campaign_execution(campaign, work).unwrap().unwrap(),
            failed
        );
        assert_eq!(scheduler.tick().await.unwrap(), 1);
        assert!(wire.try_recv().is_err(), "paused rework cannot launch");
        match mode {
            "resume" => {
                store
                    .resize_campaign_group(campaign, "execution-group", 2, 1)
                    .unwrap();
            }
            "cancel" => {
                store
                    .cancel_campaign_group(campaign, "execution-group", 2)
                    .unwrap();
            }
            "shutdown" => {
                scheduler.shutdown().await.unwrap();
            }
            "review-cancel" => {
                store
                    .cancel_campaign_group(campaign, "review-group", 2)
                    .unwrap();
            }
            "review-cancel-before" => {}
            "total" => {
                let mut gate = store
                    .campaign_command_gate(campaign, work)
                    .unwrap()
                    .unwrap();
                gate.evidence.as_mut().unwrap().elapsed_ms = config.max_total_command_ms;
                let tx = store.database.begin_write().unwrap();
                tx.open_table(crate::runtime_store::execution::command::COMMAND_GATES)
                    .unwrap()
                    .insert(work.as_str(), serde_json::to_vec(&gate).unwrap().as_slice())
                    .unwrap();
                tx.commit().unwrap();
            }
            _ => unreachable!(),
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                scheduler.tick().await.unwrap();
                // Deferred runner outcomes are not terminal Work acknowledgments.
                if scheduler
                    .outcome(campaign, work, 1)
                    .unwrap()
                    .is_some_and(|outcome| outcome.expect(mode).settled)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        scheduler.shutdown().await.unwrap();
        let record = store.campaign_execution(campaign, work).unwrap().unwrap();
        assert!(
            record.settled && !record.rework_pending,
            "{mode}: {record:?}"
        );
        assert_eq!(
            record.phase,
            ExecutionPhase::Reviewed(if mode == "resume" {
                Evaluation::Accepted
            } else if mode.starts_with("review-cancel") {
                Evaluation::Unverified
            } else {
                Evaluation::Rejected
            })
        );
        let mut calls = 0;
        while wire.try_recv().is_ok() {
            calls += 1;
        }
        assert_eq!(calls, if mode == "resume" { 3 } else { 0 });
        assert!(store.campaign_work_status(campaign, work).unwrap().terminal);
        let ledger = store.campaign_ledger(campaign).unwrap().unwrap();
        assert_eq!(ledger.active_inferences(), 0, "{mode}");
        if mode.starts_with("review-cancel") {
            assert!(!ledger
                .allocations
                .contains_key(&policy.verification.dispatch_id));
        }
        assert_eq!(record.policy.funding, policy.funding);
        assert_eq!(ledger.envelope.work.tokens, 100);
        for _ in 0..3 {
            scheduler.tick().await.unwrap();
        }
        assert!(
            wire.try_recv().is_err(),
            "finalized work must not launch again"
        );
        http.abort();
        let _ = http.await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake model only"]
async fn actual_ghost_command_loop_repairs_and_exhausts_without_refunding() {
    let executable =
        std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"));
    for scenario in [
        "repair",
        "typed",
        "deadline",
        "total",
        "exhaust",
        "concurrent",
        "funds",
        "unknown",
        "unknown-repair",
        "steering",
        "steering-race",
        "generation-race",
        "generation",
        "terminal-race",
        "cancel",
        "cancel-race",
        "billing",
    ] {
        let mode = match scenario {
            "exhaust" => "artifact-ready",
            "unknown" => "stall",
            "unknown-repair" => "artifact-repair-stall",
            _ => "artifact-repair",
        };
        let (_dir, store, funding, mut request, actor) = if scenario.starts_with("steering") {
            let (dir, store, funding, request, actor) =
                super::super::super::boundary_tests::setup();
            (dir, store, funding, request, Some(actor))
        } else {
            let (dir, store, funding, request) = tests::setup_with_capacity(true, 1);
            (dir, store, funding, request, None)
        };
        let work_id = funding.admission.work_id.clone();
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let artifacts_root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        for dir in [&artifacts_root, &staging] {
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let artifacts = Arc::new(
            ArtifactStore::open_retained(
                artifacts_root.path(),
                store.retained.clone(),
                &funding.admission.campaign_id,
            )
            .unwrap(),
        );
        let store = Arc::new(store);
        let (url, mut wire, http) = fixture(store.clone(), mode).await;
        request.estimate.base_url = url;
        request.estimate.max_request_bytes = 32000;
        if scenario.starts_with("steering") {
            request.estimate.input_tokens = 10;
        }
        let mut config = CommandEvaluator {
            acceptance_mode: None,
            result_contract: Default::default(),
            metrics: Default::default(),
            allow_extra_metrics: false,
            stage: None,
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "test \"$(cat candidate)\" = corrected".into(),
            ],
            cwd: ".".into(),
            timeout_ms: 2000,
            output_bytes: 1024,
            input_bytes: 1024,
            max_attempts: 2,
            max_total_command_ms: 4000,
        };
        if matches!(
            scenario,
            "steering-race" | "cancel-race" | "generation-race" | "terminal-race"
        ) {
            config.argv[2] = format!("test \"$(cat candidate)\" = corrected || exit 1; touch '{}'; while [ ! -f '{}' ]; do sleep 0.01; done", workspace.path().join("review-ready").display(), workspace.path().join("review-release").display());
        }
        let mut policy = policy(&store, &funding, request.clone());
        policy.evaluator_id = format!("command:{}", config.config_hash().unwrap());
        if scenario == "typed" {
            policy.work.attempt = Some(tachyon_api::types::WorkAttempt {
                continuation: None,
                id: request.identity.attempt_id.clone(),
                feedback: None,
            });
        }
        let broker = ModelBroker::new(store.clone(), model(&request));
        if matches!(
            scenario,
            "funds"
                | "steering"
                | "cancel"
                | "billing"
                | "concurrent"
                | "deadline"
                | "total"
                | "generation"
        ) {
            let failed = broker
                .execute_campaign_command(
                    &executable,
                    workspace.path(),
                    home.path(),
                    policy.clone(),
                    Instant::now()
                        + Duration::from_millis(if scenario == "deadline" { 1500 } else { 10000 }),
                    artifacts.clone(),
                    staging.path().into(),
                    config.clone(),
                    None,
                )
                .await
                .unwrap();
            assert!(failed.rework_pending);
            if scenario == "generation" {
                let tx = store.database.begin_write().unwrap();
                let mut changed = RuntimeStore::admitted_work_in(&tx, &work_id).unwrap();
                changed.admission.generation += 1;
                tx.open_table(crate::runtime_store::admission::WORK)
                    .unwrap()
                    .insert(
                        work_id.as_str(),
                        serde_json::to_vec(&changed).unwrap().as_slice(),
                    )
                    .unwrap();
                tx.commit().unwrap();
            }
            if scenario == "total" {
                // Simulate persisted evaluator wall time consuming the total allowance.
                let mut gate = store
                    .campaign_command_gate(&funding.admission.campaign_id, &work_id)
                    .unwrap()
                    .unwrap();
                gate.evidence.as_mut().unwrap().elapsed_ms = config.max_total_command_ms;
                let tx = store.database.begin_write().unwrap();
                let gates: redb::TableDefinition<&str, &[u8]> =
                    redb::TableDefinition::new("campaign_command_gates_v1");
                tx.open_table(gates)
                    .unwrap()
                    .insert(
                        work_id.as_str(),
                        serde_json::to_vec(&gate).unwrap().as_slice(),
                    )
                    .unwrap();
                tx.commit().unwrap();
            }
            if scenario == "deadline" {
                tokio::time::sleep(Duration::from_millis(1600)).await;
            }
            if matches!(scenario, "funds" | "billing") {
                store
                    .campaign_ledger_command(
                        "spend-remainder",
                        &funding.admission.campaign_id,
                        LedgerCommand::ReserveAllocated {
                            reservation_id: "spent".into(),
                            allocation_id: funding.dispatch_id.clone(),
                            pool: Pool::Work,
                            reserved: Units {
                                tokens: 60,
                                cost_micro_usd: 60,
                            },
                        },
                    )
                    .unwrap();
                if scenario == "funds" {
                    store
                        .campaign_ledger_command(
                            "final-remainder",
                            &funding.admission.campaign_id,
                            LedgerCommand::Reconcile {
                                reservation_id: "spent".into(),
                                usage: Usage::Final(Units {
                                    tokens: 60,
                                    cost_micro_usd: 60,
                                }),
                            },
                        )
                        .unwrap();
                }
            }
            if scenario == "cancel" {
                store
                    .host_cancel_work(&funding.admission.campaign_id, &work_id, 1)
                    .unwrap();
            }
            if let Some(actor) = actor.clone() {
                use crate::runtime_store::coordination::{ControlCommand, WorkAddress};
                let target = WorkAddress {
                    campaign_id: actor.campaign_id.clone(),
                    work_id: work_id.clone(),
                };
                store
                    .host_agent_control(actor)
                    .unwrap()
                    .command(
                        &target,
                        "repair-steering",
                        ControlCommand::Steer {
                            expected_revision: 1,
                            instructions: "latest repair instruction".into(),
                        },
                    )
                    .unwrap();
                let parked = broker
                    .execute_campaign_command_loop(
                        &executable,
                        workspace.path(),
                        home.path(),
                        policy.clone(),
                        Instant::now() + Duration::from_secs(10),
                        artifacts.clone(),
                        staging.path().into(),
                        config.clone(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    parked, failed,
                    "accepted but unapplied steering cannot authorize repair"
                );
                store.host_ack_agent_steering(&target, 2).unwrap();
            }
        }
        let run = || {
            broker.execute_campaign_command_loop(
                &executable,
                workspace.path(),
                home.path(),
                policy.clone(),
                Instant::now()
                    + Duration::from_millis(if scenario.starts_with("unknown") {
                        1500
                    } else {
                        10000
                    }),
                artifacts.clone(),
                staging.path().into(),
                config.clone(),
            )
        };
        let record = if matches!(
            scenario,
            "steering-race" | "cancel-race" | "generation-race" | "terminal-race"
        ) {
            let steer = async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while tokio::fs::metadata(workspace.path().join("review-ready"))
                        .await
                        .is_err()
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                use crate::runtime_store::coordination::{ControlCommand, WorkAddress};
                if scenario == "terminal-race" {
                    let reviewing = store
                        .campaign_execution(&funding.admission.campaign_id, &work_id)
                        .unwrap()
                        .unwrap();
                    store
                        .host_finish_campaign_evaluation(&reviewing, Evaluation::Unverified)
                        .unwrap();
                    store
                        .settle_campaign_execution(&funding.admission.campaign_id, &work_id)
                        .unwrap();
                } else if scenario == "generation-race" {
                    let tx = store.database.begin_write().unwrap();
                    let mut changed = RuntimeStore::admitted_work_in(&tx, &work_id).unwrap();
                    changed.admission.generation += 1;
                    tx.open_table(crate::runtime_store::admission::WORK)
                        .unwrap()
                        .insert(
                            work_id.as_str(),
                            serde_json::to_vec(&changed).unwrap().as_slice(),
                        )
                        .unwrap();
                    tx.commit().unwrap();
                } else if scenario == "cancel-race" {
                    store
                        .host_cancel_work(&funding.admission.campaign_id, &work_id, 1)
                        .unwrap();
                } else {
                    let actor = actor.clone().unwrap();
                    let target = WorkAddress {
                        campaign_id: actor.campaign_id.clone(),
                        work_id: work_id.clone(),
                    };
                    store
                        .host_agent_control(actor)
                        .unwrap()
                        .command(
                            &target,
                            "late-steering",
                            ControlCommand::Steer {
                                expected_revision: 1,
                                instructions: "candidate must satisfy a new revision".into(),
                            },
                        )
                        .unwrap();
                }
                tokio::fs::write(workspace.path().join("review-release"), b"release")
                    .await
                    .unwrap();
            };
            let (record, ()) = tokio::join!(run(), steer);
            if scenario == "terminal-race" {
                assert!(record.is_err(), "late review must lose the terminal CAS");
                let gate = store
                    .campaign_command_gate(&funding.admission.campaign_id, &work_id)
                    .unwrap()
                    .unwrap();
                assert!(gate.evidence.is_none() && gate.snapshot.is_none());
                assert_eq!(gate.history.len(), 1);
                let terminal = store
                    .campaign_execution(&funding.admission.campaign_id, &work_id)
                    .unwrap()
                    .unwrap();
                assert!(terminal.settled);
                assert_eq!(
                    terminal.phase,
                    ExecutionPhase::Reviewed(Evaluation::Unverified)
                );
                http.abort();
                continue;
            }
            record.unwrap()
        } else if scenario == "concurrent" {
            let (a, b) = tokio::join!(run(), run());
            assert!(a.is_ok() || b.is_ok());
            store
                .campaign_execution(&funding.admission.campaign_id, &work_id)
                .unwrap()
                .unwrap()
        } else {
            run().await.unwrap()
        };
        if matches!(
            scenario,
            "unknown"
                | "unknown-repair"
                | "funds"
                | "cancel"
                | "billing"
                | "deadline"
                | "total"
                | "generation"
        ) {
            assert_eq!(
                record.settled,
                matches!(scenario, "funds" | "cancel" | "deadline" | "total")
            );
            assert_eq!(
                record.phase,
                if scenario.starts_with("unknown") {
                    ExecutionPhase::ExecutingUnknown
                } else {
                    ExecutionPhase::Reviewed(Evaluation::Rejected)
                }
            );
            let mut calls = 0;
            while wire.try_recv().is_ok() {
                calls += 1;
            }
            assert_eq!(
                calls,
                if scenario == "unknown" {
                    1
                } else if scenario == "unknown-repair" {
                    4
                } else {
                    3
                }
            );
            if scenario == "unknown-repair" {
                assert_eq!(
                    store
                        .campaign_command_gate(&funding.admission.campaign_id, &work_id)
                        .unwrap()
                        .unwrap()
                        .history
                        .len(),
                    1
                );
            }
            assert_eq!(run().await.unwrap(), record);
            assert!(wire.try_recv().is_err());
            http.abort();
            let _ = http.await;
            drop(broker);
            // Retained artifacts own the runtime database authority too.
            drop(artifacts);
            drop(store);
            let reopened = Arc::new(
                RuntimeStore::open(&_dir.path().join("runtime.redb"))
                    .unwrap_or_else(|error| panic!("{scenario}: {error}; record: {record:?}")),
            );
            let artifacts = Arc::new(
                ArtifactStore::open_retained(
                    artifacts_root.path(),
                    reopened.retained.clone(),
                    &funding.admission.campaign_id,
                )
                .unwrap(),
            );
            let broker = ModelBroker::new(reopened, model(&request));
            let replay = broker
                .execute_campaign_command_loop(
                    &executable,
                    workspace.path(),
                    home.path(),
                    policy,
                    Instant::now() + Duration::from_secs(10),
                    artifacts,
                    staging.path().into(),
                    config,
                )
                .await
                .unwrap();
            assert_eq!(
                replay, record,
                "reopen cannot replay unknown execution or replenish funds"
            );
            continue;
        }
        assert_eq!(
            record.phase,
            ExecutionPhase::Reviewed(
                if matches!(
                    scenario,
                    "steering-race" | "cancel-race" | "generation-race"
                ) {
                    Evaluation::Unverified
                } else if mode == "artifact-repair" {
                    Evaluation::Accepted
                } else {
                    Evaluation::Rejected
                }
            ),
            "{scenario}: {record:?}"
        );
        assert!(record.settled && !record.rework_pending);
        assert_eq!(record.policy.funding, policy.funding);
        assert_eq!(record.policy.work.objective, policy.work.objective);
        assert_eq!(record.policy.work.generation, policy.work.generation);
        assert_ne!(
            record.policy.model.identity.attempt_id,
            policy.model.identity.attempt_id
        );
        let gate = store
            .campaign_command_gate(&funding.admission.campaign_id, &work_id)
            .unwrap()
            .unwrap();
        assert_eq!(gate.history.len(), 1);
        assert_eq!(
            gate.history[0].evidence.as_ref().unwrap().outcome,
            CommandOutcome::Fail
        );
        assert_ne!(
            gate.history[0].snapshot.as_ref().unwrap().id,
            gate.snapshot.as_ref().unwrap().id
        );
        let mut calls = 0;
        while let Ok(message) = wire.try_recv() {
            if calls == 3 {
                assert!(message["messages"]
                    .to_string()
                    .contains("Host verification rejected"));
                assert!(message["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|m| m["role"] != "tool"));
                if scenario == "steering" {
                    assert!(message["messages"]
                        .to_string()
                        .contains("latest repair instruction"));
                }
            }
            calls += 1;
        }
        assert_eq!(calls, 6);
        if scenario == "steering" {
            assert_eq!(
                record.candidate.as_ref().unwrap().instruction_revision,
                Some(2)
            );
        }
        let ledger = store
            .campaign_ledger(&funding.admission.campaign_id)
            .unwrap()
            .unwrap();
        let spent: u64 = ledger
            .reservations
            .values()
            .filter(|r| r.allocation.as_ref() == Some(&funding.dispatch_id))
            .map(|r| match r.usage {
                Usage::Final(units) => units.tokens,
                _ => panic!("billing must be final"),
            })
            .sum();
        assert_eq!(spent, 42);
        let mut changed_config = config.clone();
        changed_config.timeout_ms -= 1;
        let mut changed_policy = policy.clone();
        changed_policy.evaluator_id = format!("command:{}", changed_config.config_hash().unwrap());
        assert!(broker
            .execute_campaign_command_loop(
                &executable,
                workspace.path(),
                home.path(),
                changed_policy,
                Instant::now() + Duration::from_secs(10),
                artifacts.clone(),
                staging.path().into(),
                changed_config
            )
            .await
            .is_err());
        let replay = broker
            .execute_campaign_command_loop(
                &executable,
                workspace.path(),
                home.path(),
                policy,
                Instant::now() + Duration::from_secs(10),
                artifacts,
                staging.path().into(),
                config,
            )
            .await
            .unwrap();
        assert_eq!(replay, record);
        assert!(wire.try_recv().is_err());
        assert_eq!(
            store
                .campaign_ledger(&funding.admission.campaign_id)
                .unwrap()
                .unwrap(),
            ledger
        );
        http.abort();
        let _ = http.await;
        drop(broker);
        drop(store);
        let reopened = RuntimeStore::open(&_dir.path().join("runtime.redb")).unwrap();
        let page = reopened
            .host_research_context(
                &funding.admission.campaign_id,
                &tachyon_api::context::Request::Attempts {
                    query: tachyon_api::context::Query {
                        literal: None,
                        after: None,
                        limit: 8,
                        since_ms: None,
                        version: None,
                    },
                },
                None,
            )
            .unwrap();
        assert_eq!(page.resources.len(), 2);
        assert_eq!(page.resources[0].data["observation"]["outcome"], "Fail");
        assert!(page.resources.iter().all(|r| r.occurred_at_ms.is_some()));
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake model only"]
async fn actual_ghost_command_publishes_registered_candidate() {
    let executable =
        std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"));
    for scenario in [
        "artifact-ready",
        "artifact-stale",
        "artifact-missing",
        "metrics-pass",
        "metrics-fail",
    ] {
        let mode = if scenario.starts_with("metrics-") {
            "artifact-ready"
        } else {
            scenario
        };
        let (_dir, store, funding, mut request) = tests::setup_with_capacity(true, 1);
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let artifacts_root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        for dir in [&artifacts_root, &staging] {
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let artifacts = Arc::new(
            ArtifactStore::open_retained(
                artifacts_root.path(),
                store.retained.clone(),
                &funding.admission.campaign_id,
            )
            .unwrap(),
        );
        let store = Arc::new(store);
        let (url, mut wire, http) = fixture(store.clone(), mode).await;
        request.estimate.base_url = url;
        request.estimate.max_request_bytes = 32000;
        let mut config = CommandEvaluator {
            acceptance_mode: None,
            result_contract: Default::default(),
            metrics: Default::default(),
            allow_extra_metrics: false,
            stage: None,
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "test \"$(cat candidate)\" = original".into(),
            ],
            cwd: ".".into(),
            timeout_ms: 2000,
            output_bytes: 1024,
            input_bytes: 1024,
            max_attempts: 1,
            max_total_command_ms: 2000,
        };
        if scenario.starts_with("metrics-") {
            config.result_contract = tachyon_api::campaign::ResultContract::JsonMetrics;
            config.metrics =
                serde_json::from_value(serde_json::json!({"score":{"min":1,"max":1}})).unwrap();
            config.argv[2] = format!(
                "test \"$(cat candidate)\" = original || exit 1; printf '{{\"score\":{}}}'",
                if scenario == "metrics-pass" { 1 } else { 0 }
            );
        }
        let mut policy = policy(&store, &funding, request.clone());
        policy.evaluator_id = format!("command:{}", config.config_hash().unwrap());
        let broker = ModelBroker::new(store.clone(), model(&request));
        let record = broker
            .execute_campaign_command(
                &executable,
                workspace.path(),
                home.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(10),
                artifacts.clone(),
                staging.path().into(),
                config.clone(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            record.phase,
            ExecutionPhase::Reviewed(if scenario == "metrics-fail" {
                Evaluation::Rejected
            } else if mode == "artifact-ready" {
                Evaluation::Accepted
            } else {
                Evaluation::Unverified
            }),
            "{mode}: {record:?}"
        );
        assert!(record.settled);
        let candidate = record.candidate.clone().unwrap();
        let gate = store
            .campaign_command_gate(&funding.admission.campaign_id, "work")
            .unwrap()
            .unwrap();
        if mode == "artifact-ready" {
            let snapshot = gate.snapshot.unwrap();
            assert_eq!(candidate.candidate_refs, Some(vec![snapshot.id.clone()]));
            assert!(
                matches!(&candidate.outcome, tachyon_api::types::WorkOutcome::Completed { artifacts, .. } if artifacts == &["result"])
            );
            assert_eq!(
                snapshot.sha256,
                format!("{:x}", Sha256::digest(b"original"))
            );
            let expected = if scenario == "metrics-fail" {
                CommandOutcome::Fail
            } else {
                CommandOutcome::Pass
            };
            let saved = gate.evidence.unwrap();
            assert_eq!(saved.outcome, expected);
            assert_eq!(saved.config_hash, config.config_hash().unwrap());
            assert_eq!(saved.candidate_sha256, snapshot.sha256);
            if scenario == "metrics-fail" {
                assert_eq!(saved.diagnostic, "metric score violated inclusive min");
            }
            tokio::fs::write(workspace.path().join("result"), b"modified")
                .await
                .unwrap();
            let evidence = tachyond::verification::evaluate_command(
                artifacts.clone(),
                staging.path().into(),
                config.clone(),
                candidate,
                snapshot,
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
            assert_eq!(evidence.outcome, expected);
        } else {
            assert!(gate.evidence.is_none());
            assert!(candidate.candidate_refs.as_ref().is_none_or(Vec::is_empty));
        }
        let mut pending = false;
        while let Ok(request) = wire.try_recv() {
            pending |= request["messages"]
                .to_string()
                .contains("publication pending");
        }
        assert_eq!(pending, mode != "artifact-missing");
        let ledger = store
            .campaign_ledger(&funding.admission.campaign_id)
            .unwrap();
        let replay = broker
            .execute_campaign_command(
                &executable,
                workspace.path(),
                home.path(),
                policy,
                Instant::now() + Duration::from_secs(10),
                artifacts.clone(),
                staging.path().into(),
                config,
                None,
            )
            .await
            .unwrap();
        assert_eq!(replay, record);
        assert_eq!(
            store
                .campaign_ledger(&funding.admission.campaign_id)
                .unwrap(),
            ledger
        );
        assert!(wire.try_recv().is_err());
        http.abort();
    }
}

#[tokio::test]
async fn command_rework_pending_requires_final_billing_and_never_replays() {
    for (max_attempts, altered) in [
        (1, 0),
        (2, 0),
        (2, 1),
        (2, 2),
        (2, 3),
        (2, 4),
        (2, 5),
        (2, 6),
    ] {
        let (dir, store, funding, request) = tests::setup_with_group(true);
        let workspace = tempfile::tempdir().unwrap();
        let artifacts_root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        for dir in [&artifacts_root, &staging] {
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let artifacts = Arc::new(ArtifactStore::open(artifacts_root.path()).unwrap());
        std::fs::write(workspace.path().join("result"), "bad").unwrap();
        let snapshot = artifacts
            .register(
                "work",
                workspace.path(),
                ArtifactRegistration {
                    id: "candidate-1".into(),
                    path: "result".into(),
                    kind: "file".into(),
                    description: "fixture".into(),
                    size_bytes: 3,
                    sha256: format!("{:x}", Sha256::digest(b"bad")),
                    task_id: None,
                    work_id: Some("work".into()),
                    generation: Some(1),
                    assignment: Some(1),
                    attempt_id: Some(request.identity.attempt_id.clone()),
                    publication: ArtifactPublication::Pending,
                },
            )
            .unwrap();
        let config = CommandEvaluator {
            acceptance_mode: None,
            result_contract: Default::default(),
            metrics: Default::default(),
            allow_extra_metrics: false,
            stage: None,
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "test \"$(cat candidate)\" = good".into(),
            ],
            cwd: ".".into(),
            timeout_ms: 1000,
            output_bytes: 1024,
            input_bytes: 1024,
            max_attempts,
            max_total_command_ms: 2000,
        };
        let store = Arc::new(store);
        let mut policy = policy(&store, &funding, request.clone());
        policy.evaluator_id = format!("command:{}", config.config_hash().unwrap());
        let c = &funding.admission.campaign_id;
        let broker = ModelBroker::new(store.clone(), model(&request));
        let missing = workspace.path().join("missing");
        // A symlink alias must not hide overlap with the verification root.
        let alias_root = tempfile::tempdir().unwrap();
        let alias = alias_root.path().join("workspace");
        std::os::unix::fs::symlink(staging.path(), &alias).unwrap();
        assert!(broker
            .execute_campaign_command(
                &missing,
                &alias,
                workspace.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(5),
                artifacts.clone(),
                staging.path().into(),
                config.clone(),
                snapshot.clone(),
            )
            .await
            .is_err());
        assert!(store.campaign_command_gate(c, "work").unwrap().is_none());
        let unknown = broker
            .execute_campaign_command(
                &missing,
                workspace.path(),
                workspace.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(5),
                artifacts.clone(),
                staging.path().into(),
                config.clone(),
                snapshot.clone(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.phase, ExecutionPhase::ExecutingUnknown);
        assert!(!unknown.rework_pending);
        // Simulate trusted host recovery of complete output from a known stopped
        // process. This fixture does not pretend to be a second Ghost launch.
        let event: EventEnvelope = serde_json::from_value(serde_json::json!({
            "event_id":1, "sequence":1, "occurred_at_ms":0, "session_id":"worker", "task_id":"worker",
            "actor":{"kind":"worker", "id":"worker"}, "kind":"work_candidate",
            "candidate":{"work_id":"work", "objective":"bounded", "generation":1, "assignment":1,
                "outcome":"completed", "result":"ok", "artifacts":["result"]}
        })).unwrap();
        let mut recovered = store
            .host_collect_campaign_evidence(&unknown, vec![event])
            .unwrap();
        // Recovery fixture supplies host-owned publication evidence, not worker refs.
        recovered.candidate.as_mut().unwrap().candidate_refs = Some(vec![snapshot.id.clone()]);
        let tx = store.database.begin_write().unwrap();
        tx.open_table(crate::runtime_store::execution::EXECUTIONS)
            .unwrap()
            .insert("work", serde_json::to_vec(&recovered).unwrap().as_slice())
            .unwrap();
        tx.commit().unwrap();
        std::fs::write(workspace.path().join("result"), "good").unwrap();
        store
            .campaign_ledger_command(
                "unknown-provider",
                c,
                LedgerCommand::ReserveAllocated {
                    reservation_id: "unknown-provider".into(),
                    allocation_id: funding.dispatch_id.clone(),
                    pool: Pool::Work,
                    reserved: Units {
                        tokens: 30,
                        cost_micro_usd: 30,
                    },
                },
            )
            .unwrap();
        let reviewed = broker
            .execute_campaign_command(
                &missing,
                workspace.path(),
                workspace.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(5),
                artifacts.clone(),
                staging.path().into(),
                config.clone(),
                snapshot.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            reviewed.phase,
            ExecutionPhase::Reviewed(Evaluation::Rejected)
        );
        assert!(
            !reviewed.settled && !reviewed.rework_pending,
            "unknown billing blocks rework"
        );
        let gate = store.campaign_command_gate(c, "work").unwrap().unwrap();
        assert_eq!(
            gate.evidence.as_ref().unwrap().outcome,
            CommandOutcome::Fail
        );
        assert_eq!(
            gate.evidence.as_ref().unwrap().candidate_sha256,
            snapshot.sha256
        );
        store
            .campaign_ledger_command(
                "final-provider",
                c,
                LedgerCommand::Reconcile {
                    reservation_id: "unknown-provider".into(),
                    usage: Usage::Final(Units {
                        tokens: 30,
                        cost_micro_usd: 30,
                    }),
                },
            )
            .unwrap();
        if altered != 0 {
            // Inject inconsistent durable host state; it must not retain rework.
            let mut changed = gate.clone();
            match altered {
                1 => changed.config.timeout_ms -= 1,
                2 => changed.evidence.as_mut().unwrap().candidate_sha256 = "0".repeat(64),
                3 => changed.evidence.as_mut().unwrap().outcome = CommandOutcome::Unverified,
                4 => changed.evidence.as_mut().unwrap().outcome = CommandOutcome::Timeout,
                5 => changed.evidence.as_mut().unwrap().outcome = CommandOutcome::SpawnFailure,
                _ => {
                    let mut changed_record = reviewed.clone();
                    changed_record.candidate.as_mut().unwrap().candidate_refs = None;
                    let tx = store.database.begin_write().unwrap();
                    tx.open_table(crate::runtime_store::execution::EXECUTIONS)
                        .unwrap()
                        .insert(
                            "work",
                            serde_json::to_vec(&changed_record).unwrap().as_slice(),
                        )
                        .unwrap();
                    tx.commit().unwrap();
                }
            }
            let tx = store.database.begin_write().unwrap();
            let table: redb::TableDefinition<&str, &[u8]> =
                redb::TableDefinition::new("campaign_command_gates_v1");
            tx.open_table(table)
                .unwrap()
                .insert("work", serde_json::to_vec(&changed).unwrap().as_slice())
                .unwrap();
            tx.commit().unwrap();
            let closed = store.settle_campaign_execution(c, "work").unwrap();
            assert!(closed.settled && !closed.rework_pending);
            continue;
        }
        let completed = store.settle_campaign_execution(c, "work").unwrap();
        assert_eq!(completed.rework_pending, max_attempts > 1);
        assert_eq!(completed.settled, max_attempts == 1);
        let ledger = store.campaign_ledger(c).unwrap();
        let replay = broker
            .execute_campaign_command(
                &missing,
                workspace.path(),
                workspace.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(5),
                artifacts.clone(),
                staging.path().into(),
                config.clone(),
                snapshot.clone(),
            )
            .await
            .unwrap();
        assert_eq!(replay, completed);
        assert_eq!(
            store.campaign_command_gate(c, "work").unwrap().unwrap(),
            gate
        );
        assert_eq!(store.campaign_ledger(c).unwrap(), ledger);
        let mut changed = config.clone();
        changed.argv = vec!["/bin/true".into()];
        let mut changed_policy = policy.clone();
        changed_policy.evaluator_id = format!("command:{}", changed.config_hash().unwrap());
        assert!(broker
            .execute_campaign_command(
                &missing,
                workspace.path(),
                workspace.path(),
                changed_policy,
                Instant::now() + Duration::from_secs(5),
                artifacts.clone(),
                staging.path().into(),
                changed,
                snapshot.clone()
            )
            .await
            .is_err());
        drop(broker);
        drop(store);
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        assert_eq!(
            store.settle_campaign_execution(c, "work").unwrap(),
            completed
        );
        assert_eq!(store.campaign_ledger(c).unwrap(), ledger);
        let broker = ModelBroker::new(store.clone(), model(&request));
        let stopped = store.host_finalize_command_rework(c, "work").unwrap();
        assert!(stopped.settled && !stopped.rework_pending);
        let finalized_gate = store.campaign_command_gate(c, "work").unwrap().unwrap();
        assert!(finalized_gate.finalize_rework);
        let replay = broker
            .execute_campaign_command(
                &missing,
                workspace.path(),
                workspace.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(5),
                artifacts,
                staging.path().into(),
                config,
                snapshot,
            )
            .await
            .unwrap();
        assert_eq!(replay, stopped);
        assert_eq!(
            store.campaign_command_gate(c, "work").unwrap().unwrap(),
            finalized_gate
        );
        assert_eq!(
            store.host_finalize_command_rework(c, "work").unwrap(),
            stopped
        );
        let ledger = store.campaign_ledger(c).unwrap();
        drop(broker);
        drop(store);
        let reopened = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        assert_eq!(
            reopened.settle_campaign_execution(c, "work").unwrap(),
            stopped
        );
        assert_eq!(reopened.campaign_ledger(c).unwrap(), ledger);
        let stopped = reopened.host_finalize_command_rework(c, "work").unwrap();
        assert!(stopped.settled && !stopped.rework_pending);
        assert_eq!(stopped.policy.model.identity.attempt_id, "attempt-1");
        assert_eq!(
            reopened
                .campaign_ledger(c)
                .unwrap()
                .unwrap()
                .committed(Pool::Work)
                .unwrap()
                .tokens,
            30
        );
    }
}
