use super::*;
use crate::runtime_store::{
    admission::{Admission, DispatchOutcome},
    execution::{Evaluation, ExecutionPhase, ExecutionPolicy},
};
use tachyon_api::types::{LifetimeClass, WorkRequest};

mod command_tests;
mod research_context_tests;

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost HTTP only"]
async fn scheduler_real_parallel_shrink_cancel_and_lost_review() {
    use crate::runtime_store::{
        campaign_ledger::Envelope,
        groups::{GroupSpec, WorkLimits},
        scheduler::{HostExecution, HostScheduler},
    };
    use tachyon_api::types::{ApiRequest, ApiResponse};
    let executable =
        std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"));
    let (_template_dir, _template_store, template, mut request) = tests::setup();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("work")).unwrap();
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
    let c = &campaign.id;
    store
        .host_authorize_campaign_envelope(
            "grant",
            c,
            Envelope {
                work: Units {
                    tokens: 1000,
                    cost_micro_usd: 1000,
                },
                verification: Units {
                    tokens: 1000,
                    cost_micro_usd: 1000,
                },
                max_active_inferences: 20,
            },
        )
        .unwrap();
    store
        .host_configure_work_limits(
            c,
            WorkLimits {
                total_work: 10,
                max_depth: 1,
                max_running: 2,
                max_resident: 10,
            },
        )
        .unwrap();
    let admissions: Vec<_> = (0..3)
        .map(|n| Admission {
            campaign_id: c.clone(),
            work_id: format!("explicit-{n}"),
            objective: format!("objective-explicit-{n}"),
            ..template.admission.clone()
        })
        .collect();
    store
        .create_campaign_group(GroupSpec {
            campaign_id: c.clone(),
            group_id: "workers".into(),
            parent: None,
            max_running: 2,
            work: admissions.clone(),
        })
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    request.estimate.base_url = format!("http://{}", listener.local_addr().unwrap());
    request.estimate.max_request_bytes = 32000;
    let (arrivals, mut incoming) = tokio::sync::mpsc::unbounded_channel();
    let http = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let arrivals = arrivals.clone();
            tokio::spawn(async move {
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(socket.read_u8().await.unwrap());
                    assert!(headers.len() < 16384);
                }
                let headers = String::from_utf8(headers).unwrap();
                let length: usize = headers
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
                let body = String::from_utf8(body).unwrap();
                let id = (0..3)
                    .map(|n| format!("explicit-{n}"))
                    .find(|id| body.contains(&format!("objective-{id}")))
                    .unwrap();
                let (release, wait) = tokio::sync::oneshot::channel();
                arrivals.send((id, release)).unwrap();
                let _ = wait.await;
                let response = format!("{VALID}data: [DONE]\n\n");
                let _ = socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await;
            });
        }
    });
    let mut entries = Vec::new();
    for a in admissions {
        let funding = store.admitted_work(c, &a.work_id).unwrap();
        let verification = store
            .admit_campaign_work(Admission {
                work_id: format!("verify-{}", a.work_id),
                pool: Pool::Verification,
                ..a.clone()
            })
            .unwrap();
        let mut model = request.clone();
        model.identity.campaign_id = c.clone();
        model.identity.work_id = a.work_id.clone();
        let work = WorkRequest {
            context_refs: vec![],
            constraints: None,
            attempt: None,
            work_id: a.work_id,
            objective: a.objective,
            generation: a.generation,
            assignment: 1,
            lifetime_class: LifetimeClass::Short,
            deadline_ms: (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + 30000) as u64,
        };
        entries.push(HostExecution {
            policy: ExecutionPolicy {
                funding,
                verification,
                model,
                work,
                evaluator_id: "fixture".into(),
            },
            executable: executable.clone(),
            workspace: dir.path().join("work"),
            home: dir.path().join("work"),
            evaluate: crate::runtime_store::scheduler::Evaluator::Callback(Arc::new(|candidate| {
                Box::pin(async move {
                    if candidate.work_id == "explicit-2" {
                        panic!("lost evaluator callback");
                    }
                    Evaluation::Accepted
                })
            })),
        });
    }
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&request)));
    let recovery_catalog = entries.clone();
    let mut scheduler = HostScheduler::new(broker.clone(), entries, 2).unwrap();
    assert_eq!(scheduler.tick().await.unwrap(), 2);
    let first = tokio::time::timeout(Duration::from_secs(5), incoming.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.0, second.0);
    assert_eq!(store.campaign_group_status(c, "workers").unwrap().2, 2);
    assert!(
        incoming.try_recv().is_err(),
        "root cap must block third Ghost"
    );
    store.resize_campaign_group(c, "workers", 1, 1).unwrap();
    assert_eq!(
        store.campaign_group_status(c, "workers").unwrap().2,
        2,
        "shrink drains, never evicts"
    );
    first.1.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            scheduler.tick().await.unwrap();
            if store.campaign_work_status(c, &first.0).unwrap().terminal {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.campaign_group_status(c, "workers").unwrap().2, 1);
    assert!(incoming.try_recv().is_err());
    store.host_cancel_work(c, &second.0, 1).unwrap();
    assert!(
        store.campaign_work_status(c, &second.0).unwrap().active,
        "cancel intent is not cleanup"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            scheduler.tick().await.unwrap();
            if store.campaign_work_status(c, &second.0).unwrap().terminal {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(second.1);
    // Terminal acknowledgement can precede the runner releasing its task slot.
    // Keep driving the explicit scheduler rather than assuming autonomous ticks.
    let third = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            scheduler.tick().await.unwrap();
            tokio::select! {
                arrival = incoming.recv() => break arrival,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(third.0, "explicit-2");
    third.1.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            scheduler.tick().await.unwrap();
            if scheduler
                .outcome(c, "explicit-2", 1)
                .unwrap()
                .is_some_and(|r| r.is_err())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store
            .campaign_execution(c, "explicit-2")
            .unwrap()
            .unwrap()
            .phase,
        ExecutionPhase::ReviewingUnknown
    );
    let cancelled = store.campaign_execution(c, &second.0).unwrap().unwrap();
    assert_eq!(
        cancelled.phase,
        ExecutionPhase::Reviewed(Evaluation::Unverified)
    );
    assert!(
        !cancelled.settled,
        "cancelled provider billing remains unknown"
    );
    scheduler.shutdown().await.unwrap();
    drop(scheduler);
    drop(broker);
    drop(store);
    let reopened = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    assert_eq!(
        reopened
            .campaign_execution(c, "explicit-2")
            .unwrap()
            .unwrap()
            .phase,
        ExecutionPhase::ReviewingUnknown
    );
    assert!(reopened
        .claim_campaign_work_matching(|w| w.admission.pool == Pool::Work)
        .unwrap()
        .is_none());
    let mut recovered = HostScheduler::new(
        Arc::new(ModelBroker::new(reopened.clone(), model(&request))),
        recovery_catalog,
        2,
    )
    .unwrap();
    assert!(recovered
        .outcome(c, "explicit-2", 1)
        .unwrap()
        .unwrap()
        .is_err());
    assert_eq!(recovered.tick().await.unwrap(), 0);
    assert!(
        incoming.try_recv().is_err(),
        "restart never replays launch/review"
    );
    http.abort();
}

fn policy(
    store: &RuntimeStore,
    funding: &AdmittedWork,
    model: RequestReservation,
) -> ExecutionPolicy {
    store
        .admit_campaign_work(Admission {
            work_id: "verification-work".into(),
            pool: Pool::Verification,
            objective: "deterministic fixture evaluation".into(),
            ..funding.admission.clone()
        })
        .unwrap();
    store
        .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
            worker_id: "host-evaluator".into(),
        })
        .unwrap();
    ExecutionPolicy {
        funding: store
            .admitted_work(&funding.admission.campaign_id, &funding.admission.work_id)
            .unwrap(),
        model,
        work: WorkRequest {
            context_refs: vec![],
            constraints: None,
            attempt: None,
            work_id: funding.admission.work_id.clone(),
            objective: funding.admission.objective.clone(),
            generation: funding.admission.generation,
            assignment: 1,
            lifetime_class: LifetimeClass::Short,
            deadline_ms: (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + 10000) as u64,
        },
        verification: store
            .admitted_work(&funding.admission.campaign_id, "verification-work")
            .unwrap(),
        evaluator_id: "fixture-only-v1".into(),
    }
}

#[tokio::test]
async fn scheduler_reopen_unknown_wait_does_not_replay_or_release_residency() {
    use crate::runtime_store::scheduler::{HostExecution, HostScheduler};
    let (dir, store, funding, request) = tests::setup_with_capacity(true, 2);
    let store = Arc::new(store);
    let policy = policy(&store, &funding, request.clone());
    let missing = dir.path().join("missing-ghost");
    let broker = ModelBroker::new(store.clone(), model(&request));
    let before = broker
        .execute_campaign(
            &missing,
            dir.path(),
            dir.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            |_| async { panic!("no evidence") },
        )
        .await
        .unwrap();
    assert_eq!(before.phase, ExecutionPhase::ExecutingUnknown);
    let wait = store
        .host_suspend_parent(
            &policy.funding,
            0,
            vec!["verification-work".into()],
            crate::runtime_store::groups::WaitMode::All,
            policy.work.deadline_ms,
        )
        .unwrap();
    drop(broker);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let entry = HostExecution {
        policy: policy.clone(),
        executable: missing,
        workspace: dir.path().to_owned(),
        home: dir.path().to_owned(),
        evaluate: crate::runtime_store::scheduler::Evaluator::Callback(Arc::new(|_| {
            Box::pin(async { panic!("replayed review") })
        })),
    };
    let mut scheduler = HostScheduler::new(
        Arc::new(ModelBroker::new(store.clone(), model(&request))),
        vec![entry],
        2,
    )
    .unwrap();
    assert_eq!(scheduler.tick().await.unwrap(), 0);
    let c = &funding.admission.campaign_id;
    assert_eq!(
        store.campaign_execution(c, "work").unwrap().unwrap(),
        before
    );
    let status = store.campaign_work_status(c, "work").unwrap();
    assert!(!status.active && !status.terminal);
    assert_eq!(status.wait, Some(wait));
    assert!(store
        .host_issue_model_permit(request.clone(), policy.funding.clone(), None)
        .is_err());
    assert!(scheduler.outcome(c, "work", 1).unwrap().is_none());
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let signal = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        stop.send_replace(true);
    };
    let (result, ()) = tokio::join!(scheduler.run(receiver, Duration::from_millis(10)), signal);
    result.unwrap();
    assert_eq!(
        store.campaign_execution(c, "work").unwrap().unwrap(),
        before
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_and_execution_storage_leave_heartbeat_running() {
    use crate::runtime_store::scheduler::{HostExecution, HostScheduler};
    for execution in [false, true] {
        let (dir, store, funding, request) = tests::setup_with_capacity(true, 2);
        let store = Arc::new(store);
        let policy = policy(&store, &funding, request.clone());
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&request)));
        let entry = HostExecution {
            policy: policy.clone(),
            executable: dir.path().join("missing-ghost"),
            workspace: dir.path().to_owned(),
            home: dir.path().to_owned(),
            evaluate: crate::runtime_store::scheduler::Evaluator::Callback(Arc::new(|_| {
                Box::pin(async { panic!("no evidence") })
            })),
        };
        let mut scheduler = HostScheduler::new(broker.clone(), vec![entry.clone()], 2).unwrap();
        let (release, writer) = locked_writer(store.clone()).await;
        let start = Instant::now();
        let heartbeat = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let elapsed = start.elapsed();
            let _ = release.send(());
            writer.await.unwrap();
            assert!(
                elapsed < Duration::from_secs(1),
                "executor blocked: {elapsed:?}"
            );
        };
        let operation = async {
            if execution {
                broker
                    .execute_campaign(
                        &entry.executable,
                        &entry.workspace,
                        &entry.home,
                        policy,
                        Instant::now() + Duration::from_secs(5),
                        |_| async { panic!("no evidence") },
                    )
                    .await
                    .unwrap();
            } else {
                scheduler.tick().await.unwrap();
            }
        };
        tokio::join!(operation, heartbeat);
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn verifier_cap_one_durable_handoff_and_restart() {
    use crate::runtime_store::admission::DispatchState;
    let (dir, store, funding, request) = tests::setup_with_capacity(true, 1);
    let policy = policy(&store, &funding, request.clone());
    assert_eq!(policy.verification.state, DispatchState::Admitted);
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("missing");
    let store = Arc::new(store);
    let broker = ModelBroker::new(store.clone(), model(&request));
    let record = broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            |_| async { panic!("no evidence") },
        )
        .await
        .unwrap();
    assert_eq!(record.phase, ExecutionPhase::ExecutingUnknown);
    let event = serde_json::from_value(serde_json::json!({
        "event_id":1, "sequence":1, "occurred_at_ms":0,
        "session_id":"worker", "task_id":"worker", "actor":{"kind":"worker", "id":"worker"},
        "kind":"work_candidate", "candidate":{
            "work_id":policy.work.work_id, "objective":policy.work.objective,
            "generation":policy.work.generation, "assignment":policy.work.assignment,
            "outcome":"completed", "result":"ok"
        }
    }))
    .unwrap();
    store
        .host_collect_campaign_evidence(&record, vec![event])
        .unwrap();
    let campaign = &funding.admission.campaign_id;
    store
        .admit_campaign_work(Admission {
            work_id: "other".into(),
            upper_bound: Units::default(),
            ..funding.admission.clone()
        })
        .unwrap();
    assert_eq!(
        store
            .dispatch_campaign_batch(2, |w| {
                assert_eq!(w.admission.work_id, "other");
                DispatchOutcome::Unknown
            })
            .unwrap(),
        1
    );
    let waiting = broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            |_| async { panic!("capacity blocked") },
        )
        .await
        .unwrap();
    assert_eq!(waiting.phase, ExecutionPhase::AwaitingVerification);
    assert!(!store
        .campaign_ledger(campaign)
        .unwrap()
        .unwrap()
        .allocations
        .contains_key(&policy.verification.dispatch_id));
    drop(broker);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    assert_eq!(
        store.campaign_execution(campaign, "work").unwrap(),
        Some(waiting)
    );
    let other = store.admitted_work(campaign, "other").unwrap();
    store.host_acknowledge_work_terminal(&other).unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let run = broker.execute_campaign(
        &missing,
        root.path(),
        root.path(),
        policy.clone(),
        Instant::now() + Duration::from_secs(10),
        |_| async {
            let write = store.database.begin_write().unwrap();
            drop(write);
            tx.send(()).unwrap();
            std::future::pending::<Evaluation>().await
        },
    );
    let mut run = Box::pin(run);
    tokio::select! { _ = rx => {}, _ = &mut run => panic!("review finished") }
    drop(run);
    let reviewing = store.campaign_execution(campaign, "work").unwrap().unwrap();
    assert_eq!(reviewing.phase, ExecutionPhase::ReviewingUnknown);
    drop(broker);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let broker = ModelBroker::new(store.clone(), model(&request));
    let replay = broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            |_| async { panic!("duplicate evaluator") },
        )
        .await
        .unwrap();
    assert_eq!(replay, reviewing);
    store
        .host_finish_campaign_evaluation(&reviewing, Evaluation::Accepted)
        .unwrap();
    assert!(store
        .host_finish_campaign_evaluation(&reviewing, Evaluation::Accepted)
        .is_err());
}

#[tokio::test]
async fn stopping_snapshot_publication_preserves_evidence_and_unknown_activation() {
    use tachyon_api::context::{ActivationObservation, WorkContextSnapshot, WorkerContextMetadata};
    for (fail, known, final_kind) in [
        (false, false, 0),
        (false, true, 0),
        (true, false, 0),
        (false, true, 1),
        (false, true, 2),
        (false, false, 3),
    ] {
        let (_dir, store, funding, request) = tests::setup_with_capacity(true, 1);
        let policy = policy(&store, &funding, request.clone());
        let root = tempfile::tempdir().unwrap();
        let mut store = store;
        if fail {
            store.trace_limits.operation = 1;
        }
        let store = Arc::new(store);
        let broker = ModelBroker::new(store.clone(), model(&request));
        let previous = broker
            .execute_campaign(
                &root.path().join("missing"),
                root.path(),
                root.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(2),
                |_| async { panic!("no evaluator") },
            )
            .await
            .unwrap();
        let mut metadata = WorkerContextMetadata {
            activated_packages: [("ctx".into(), "observed-version".into())].into(),
            known_output_handles: vec!["output:process-local".into()],
        };
        let mut produced = Vec::new();
        if known {
            let identity = &policy.model.identity;
            let snapshot = WorkContextSnapshot {
                schema_version: 1,
                stopping: None,
                work_id: identity.work_id.clone(),
                attempt_id: identity.attempt_id.clone(),
                generation: identity.generation,
                instruction_revision: identity.instruction_revision,
                request_id: "observed-boundary".into(),
                objective: policy.work.objective.clone(),
                worker_claims_informational_only: Some(metadata.clone()),
                pending_question_refs: None,
                remaining_tokens: None,
                remaining_cost_micro_usd: None,
                selected_resource_refs: vec![],
                reattachable: false,
            };
            store
                .record_context_object(
                    &funding.admission.campaign_id,
                    &identity.work_id,
                    &identity.attempt_id,
                    "observed-boundary",
                    "work_context_snapshot",
                    &serde_json::to_vec(&snapshot).unwrap(),
                )
                .unwrap();
            produced.push(
                store
                    .record_context_object(
                        &funding.admission.campaign_id,
                        &identity.work_id,
                        &identity.attempt_id,
                        "observed-result",
                        "tool_result",
                        b"evidence",
                    )
                    .unwrap(),
            );
            // A different attempt's resource is not produced by this stopping Work.
            store
                .record_context_object(
                    &funding.admission.campaign_id,
                    &identity.work_id,
                    "other-attempt",
                    "other-result",
                    "tool_result",
                    b"other evidence",
                )
                .unwrap();
        }
        let mut final_context = metadata.clone();
        final_context
            .activated_packages
            .insert("artifact".into(), "untrusted-final-version".into());
        if final_kind == 1 {
            use crate::runtime_store::research_context::traces::TRACES;
            use tachyon_api::context::{Resource, ResourceKind, ResourceRef};
            let identity = &policy.model.identity;
            let tx = store.database.begin_write().unwrap();
            for scope in [
                "owned",
                "work",
                "attempt",
                "campaign",
                "generation",
                "staging",
            ] {
                let handle = format!("output:{scope}");
                final_context.known_output_handles.push(handle.clone());
                let resource = Resource {
                    reference: ResourceRef {
                        kind: ResourceKind::Trace,
                        work_id: if scope == "work" {
                            "foreign".into()
                        } else {
                            identity.work_id.clone()
                        },
                        id: format!("export-{scope}"),
                        version: "fixture-version".into(),
                    },
                    occurred_at_ms: None,
                    data: serde_json::json!({"phase":"retained_output", "live_handle_id":handle, "size_bytes":0,
                        "attempt_id":if scope == "attempt" { "foreign" } else { &identity.attempt_id },
                        "generation":identity.generation + u64::from(scope == "generation"),
                        "retention_state":if scope == "staging" { "staging" } else { "ready" }}),
                };
                let campaign = if scope == "campaign" {
                    "foreign"
                } else {
                    &funding.admission.campaign_id
                };
                tx.open_table(TRACES)
                    .unwrap()
                    .insert(
                        (campaign, resource.reference.id.as_str()),
                        serde_json::to_vec(&resource).unwrap().as_slice(),
                    )
                    .unwrap();
                if scope == "owned" {
                    produced.push(resource.reference);
                }
            }
            tx.commit().unwrap();
        }
        if final_kind == 2 {
            final_context
                .activated_packages
                .insert("bad".into(), "x".repeat(257));
        }
        let event = serde_json::from_value(serde_json::json!({
            "event_id":1,"sequence":1,"occurred_at_ms":0,"session_id":"worker",
            "task_id":"worker","actor":{"kind":"worker","id":"worker"},
            "kind":"work_candidate","candidate":{
                "work_id":policy.work.work_id,"objective":policy.work.objective,
                "generation":policy.work.generation,"assignment":policy.work.assignment,
                "outcome": if final_kind == 3 { "failed" } else { "completed" },
                "result":"collected evidence survives", "message":"worker failed",
                "final_context": (final_kind != 0).then_some(&final_context)
            }
        }))
        .unwrap();
        let stopped = store
            .host_collect_campaign_evidence(&previous, vec![event])
            .unwrap();
        assert_eq!(stopped.candidate.is_some(), final_kind != 3);
        assert_eq!(
            store
                .campaign_execution(&funding.admission.campaign_id, &policy.work.work_id)
                .unwrap()
                .unwrap(),
            stopped
        );
        if fail {
            assert_eq!(
                stopped.phase,
                ExecutionPhase::Reviewed(Evaluation::Unverified)
            );
            assert!(stopped.stopping_snapshot.is_none());
        } else {
            assert!(stopped.stopping_snapshot.is_some(), "{final_kind}: {:?}", {
                let tx = store.database.begin_read().unwrap();
                tx.open_table(crate::runtime_store::execution::EXECUTION_ERRORS)
                    .unwrap()
                    .get(policy.work.work_id.as_str())
                    .unwrap()
                    .map(|v| String::from_utf8_lossy(v.value()).into_owned())
            });
            let resource = store
                .trace_resolve(
                    &funding.admission.campaign_id,
                    stopped.stopping_snapshot.as_ref().unwrap(),
                )
                .unwrap();
            let size = resource.data["size_bytes"].as_u64().unwrap();
            let mut bytes = Vec::new();
            while (bytes.len() as u64) < size {
                bytes.extend(
                    store
                        .trace_read(&resource, bytes.len() as u64, 1024)
                        .unwrap(),
                );
            }
            let snapshot: WorkContextSnapshot = serde_json::from_slice(&bytes).unwrap();
            metadata.known_output_handles.clear();
            final_context.known_output_handles.clear();
            if final_kind == 1 {
                final_context
                    .known_output_handles
                    .push("output:owned".into());
            }
            assert_eq!(
                snapshot.worker_claims_informational_only,
                match final_kind {
                    1 | 3 => Some(final_context),
                    2 => None,
                    _ => known.then_some(metadata),
                }
            );
            assert_eq!(snapshot.work_id, policy.model.identity.work_id);
            assert_eq!(snapshot.attempt_id, policy.model.identity.attempt_id);
            assert_eq!(snapshot.objective, funding.admission.objective);
            assert_eq!(snapshot.selected_resource_refs, policy.work.context_refs);
            let stopping = snapshot.stopping.unwrap();
            assert_eq!(stopping.produced_resource_refs.len(), produced.len());
            assert!(produced
                .iter()
                .all(|r| stopping.produced_resource_refs.contains(r)));
            assert_eq!(
                stopping.activation_observation,
                if matches!(final_kind, 1 | 3) {
                    ActivationObservation::Final
                } else if final_kind == 2 {
                    ActivationObservation::Unknown
                } else if known {
                    ActivationObservation::Stale
                } else {
                    ActivationObservation::Unknown
                }
            );
            assert_eq!(
                stopping.worker_observations_filtered,
                known || final_kind != 0
            );
            assert!(!snapshot.reattachable);
            std::fs::remove_file(store.trace_root.join(&resource.reference.id)).unwrap();
            assert!(store.trace_read(&resource, 0, 1024).is_err());
        }
    }
}

#[tokio::test]
async fn review_rechecks_cancelled_or_terminal_verification_without_spending() {
    use crate::runtime_store::groups::GroupSpec;
    for (cancelled, unknown) in [(true, false), (true, true), (false, false)] {
        let (_dir, store, funding, request) = tests::setup_with_group(true);
        store
            .create_campaign_group(GroupSpec {
                group_id: "verifiers".into(),
                campaign_id: funding.admission.campaign_id.clone(),
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
        let policy = policy(&store, &funding, request.clone());
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        let store = Arc::new(store);
        let broker = ModelBroker::new(store.clone(), model(&request));
        let record = broker
            .execute_campaign(
                &missing,
                root.path(),
                root.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(2),
                |_| async { panic!("no evidence") },
            )
            .await
            .unwrap();
        let event = serde_json::from_value(serde_json::json!({
            "event_id":1, "sequence":1, "occurred_at_ms":0,
            "session_id":"worker", "task_id":"worker",
            "actor":{"kind":"worker", "id":"worker"},
            "kind":"work_candidate", "candidate":{
                "work_id":policy.work.work_id, "objective":policy.work.objective,
                "generation":policy.work.generation, "assignment":policy.work.assignment,
                "outcome":"completed", "result":"ok"
            }
        }))
        .unwrap();
        if unknown {
            store
                .campaign_ledger_command(
                    "unknown-provider",
                    &funding.admission.campaign_id,
                    LedgerCommand::ReserveAllocated {
                        reservation_id: "unknown-provider".into(),
                        allocation_id: funding.dispatch_id.clone(),
                        pool: Pool::Work,
                        reserved: Units {
                            tokens: 10,
                            cost_micro_usd: 10,
                        },
                    },
                )
                .unwrap();
        }
        let ready = store
            .host_collect_campaign_evidence(&record, vec![event])
            .unwrap();
        let campaign = &funding.admission.campaign_id;
        if cancelled {
            store
                .cancel_campaign_group(campaign, "verifiers", 1)
                .unwrap();
        } else {
            store
                .host_acknowledge_work_terminal(&policy.verification)
                .unwrap();
        }
        let before = store.campaign_ledger(campaign).unwrap();
        let result = broker
            .execute_campaign(
                &missing,
                root.path(),
                root.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(2),
                |_| async { panic!("ineligible evaluator started") },
            )
            .await;
        if cancelled {
            let record = result.unwrap();
            assert_eq!(
                record.phase,
                ExecutionPhase::Reviewed(Evaluation::Unverified)
            );
            assert_eq!(record.candidate, ready.candidate);
            assert_eq!(record.settled, !unknown);
            let ledger = store.campaign_ledger(campaign).unwrap().unwrap();
            assert!(!ledger
                .reservations
                .contains_key(&format!("evaluation:{}", policy.verification.dispatch_id)));
            if unknown {
                assert_eq!(
                    ledger.reservations["unknown-provider"],
                    before.as_ref().unwrap().reservations["unknown-provider"]
                );
                store
                    .campaign_ledger_command(
                        "provider-final",
                        campaign,
                        LedgerCommand::Reconcile {
                            reservation_id: "unknown-provider".into(),
                            usage: Usage::Final(Units {
                                tokens: 5,
                                cost_micro_usd: 5,
                            }),
                        },
                    )
                    .unwrap();
                assert!(
                    store
                        .settle_campaign_execution(campaign, "work")
                        .unwrap()
                        .settled
                );
            }
            let final_ledger = store.campaign_ledger(campaign).unwrap().unwrap();
            assert_eq!(
                final_ledger.committed(Pool::Verification).unwrap().tokens,
                0
            );
            assert_eq!(
                final_ledger
                    .committed(Pool::Verification)
                    .unwrap()
                    .cost_micro_usd,
                0
            );
        } else {
            assert!(result.is_err());
            assert_eq!(store.campaign_ledger(campaign).unwrap(), before);
            assert_eq!(
                store.campaign_execution(campaign, "work").unwrap(),
                Some(ready)
            );
        }
        assert_eq!(
            store
                .campaign_group_status(campaign, "verifiers")
                .unwrap()
                .2,
            0
        );
    }
}

#[tokio::test]
async fn execution_expired_resume_and_second_close_failure() {
    let (_dir, store, funding, request) = tests::setup_with_group(true);
    let root = tempfile::tempdir().unwrap();
    let mut policy = policy(&store, &funding, request.clone());
    policy.work.deadline_ms -= 9000;
    let store = Arc::new(store);
    let broker = ModelBroker::new(store.clone(), model(&request));
    let missing = root.path().join("missing");
    let record = broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(20),
            |_| async { panic!("no evidence") },
        )
        .await
        .unwrap();
    let event = serde_json::from_value(serde_json::json!({
        "event_id":1, "sequence":1, "occurred_at_ms":0,
        "session_id":"worker", "task_id":"worker", "actor":{"kind":"worker", "id":"worker"},
        "kind":"work_candidate", "candidate":{
            "work_id":policy.work.work_id, "objective":policy.work.objective,
            "generation":policy.work.generation, "assignment":policy.work.assignment,
            "outcome":"completed", "result":"ok"
        }
    }))
    .unwrap();
    assert_eq!(
        store
            .campaign_group_status(&funding.admission.campaign_id, "execution-group")
            .unwrap()
            .2,
        1
    );
    store
        .host_collect_campaign_evidence(&record, vec![event])
        .unwrap();
    let (_, status, active) = store
        .campaign_group_status(&funding.admission.campaign_id, "execution-group")
        .unwrap();
    assert_eq!(active, 0);
    assert!(status[0].terminal);
    assert!(
        !store
            .campaign_execution(&funding.admission.campaign_id, "work")
            .unwrap()
            .unwrap()
            .settled
    );
    // Force a command-receipt conflict on the second close, after the first writes.
    let campaign = &funding.admission.campaign_id;
    store
        .campaign_ledger_command(
            &format!("execution-close:{}", policy.verification.dispatch_id),
            campaign,
            LedgerCommand::FundAllocation {
                reservation_id: policy.verification.dispatch_id.clone(),
                work_id: policy.verification.admission.work_id.clone(),
            },
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(20),
            |_| async { panic!("expired review") },
        )
        .await
        .is_err());
    let reviewed = store.campaign_execution(campaign, "work").unwrap().unwrap();
    assert_eq!(
        reviewed.phase,
        ExecutionPhase::Reviewed(Evaluation::Unverified)
    );
    assert!(!reviewed.settled);
    let ledger = store.campaign_ledger(campaign).unwrap().unwrap();
    assert_eq!(ledger.allocations[&funding.dispatch_id], false);
    assert_eq!(ledger.allocations[&policy.verification.dispatch_id], false);
    assert!(store.settle_campaign_execution(campaign, "work").is_err());
    assert_eq!(store.campaign_ledger(campaign).unwrap().unwrap(), ledger);
    assert_eq!(
        store.campaign_execution(campaign, "work").unwrap().unwrap(),
        reviewed
    );
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost HTTP only"]
async fn actual_ghost_execution_review_and_settlement() {
    let executable =
        std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"));
    for (mode, evaluation) in [
        ("subprocess", Evaluation::Accepted),
        ("valid", Evaluation::Rejected),
        ("valid", Evaluation::Unverified),
        ("stall", Evaluation::Unverified),
        ("malformed", Evaluation::Unverified),
    ] {
        let (dir, store, funding, mut request) = tests::setup_with_capacity(true, 1);
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(store);
        let (url, mut wire, http) = fixture(store.clone(), mode).await;
        request.estimate.base_url = url;
        request.estimate.max_request_bytes = 32000;
        let policy = policy(&store, &funding, request.clone());
        let broker = ModelBroker::new(store.clone(), model(&request));
        let expected = evaluation.clone();
        let timeout = mode == "valid" && evaluation == Evaluation::Unverified;
        let reviewed = std::sync::atomic::AtomicBool::new(false);
        let evaluate = |candidate: tachyon_api::types::WorkResult| {
            let (store, funding, reviewed) = (&store, &funding, &reviewed);
            let expected = expected.clone();
            async move {
                assert!(
                    !reviewed.swap(true, std::sync::atomic::Ordering::SeqCst),
                    "concurrent review replay"
                );
                assert_eq!(candidate.work_id, "work");
                assert_eq!(candidate.outcome.completed_result(), Some("ok"));
                assert!(candidate.timing.is_none());
                // Prove callback has no writer/authority lock and verification is protected.
                let _authority = store.model_permits.try_lock().unwrap();
                let write = store.database.begin_write().unwrap();
                let ledger =
                    RuntimeStore::campaign_ledger_in(&write, &funding.admission.campaign_id)
                        .unwrap();
                assert!(ledger
                    .reservations
                    .values()
                    .any(|r| r.pool == Pool::Verification
                        && r.allocation.is_some()
                        && r.usage == Usage::Unknown));
                drop(write);
                drop(_authority);
                if timeout {
                    std::future::pending::<()>().await;
                }
                expected
            }
        };
        let run = broker.execute_campaign(
            &executable,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            &evaluate,
        );
        let concurrent = broker.execute_campaign(
            &executable,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            &evaluate,
        );
        let (record, concurrent) = tokio::join!(biased; run, concurrent);
        let record = record.unwrap();
        let concurrent = concurrent.unwrap();
        // spawn_blocking claim order is independent of the futures' poll order.
        let record = if record.phase == ExecutionPhase::ExecutingUnknown {
            concurrent
        } else {
            record
        };
        let group_status = store
            .campaign_group_status(&funding.admission.campaign_id, "execution-group")
            .unwrap();
        assert_eq!(
            group_status.2,
            usize::from(record.phase == ExecutionPhase::ExecutingUnknown)
        );
        assert_eq!(
            group_status.1[0].terminal,
            record.phase != ExecutionPhase::ExecutingUnknown
        );
        wire.try_recv()
            .expect("real Ghost called localhost provider");
        if mode == "stall" {
            assert_eq!(record.phase, ExecutionPhase::ExecutingUnknown);
            assert!(!record.settled);
        } else if mode == "malformed" {
            assert!(!reviewed.load(std::sync::atomic::Ordering::SeqCst));
            assert!(!matches!(
                record.phase,
                ExecutionPhase::Reviewed(Evaluation::Accepted)
            ));
            assert!(!record.settled, "unknown provider usage must remain held");
        } else {
            assert_eq!(record.phase, ExecutionPhase::Reviewed(evaluation));
            assert!(record.settled);
            let ledger = store
                .campaign_ledger(&funding.admission.campaign_id)
                .unwrap()
                .unwrap();
            assert_eq!(ledger.active_inferences(), 0);
            assert_eq!(ledger.committed(Pool::Verification).unwrap().tokens, 0);
            assert_eq!(
                ledger.committed(Pool::Work).unwrap().tokens,
                if mode == "subprocess" { 14 } else { 7 }
            );
        }
        let repeated = broker
            .execute_campaign(
                &executable,
                root.path(),
                root.path(),
                policy.clone(),
                Instant::now() + Duration::from_secs(2),
                |_| async { panic!("review replay") },
            )
            .await
            .unwrap();
        assert_eq!(record, repeated);
        assert_eq!(record.policy.work.generation, 1);
        assert_eq!(record.policy.model.identity.attempt_id, "attempt-1");
        let ledger = store
            .campaign_ledger(&funding.admission.campaign_id)
            .unwrap();
        http.abort();
        let _ = http.await;
        drop(broker);
        drop(store);
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        assert_eq!(
            store
                .campaign_execution(&funding.admission.campaign_id, "work")
                .unwrap(),
            Some(record.clone())
        );
        assert_eq!(
            store
                .settle_campaign_execution(&funding.admission.campaign_id, "work")
                .unwrap(),
            record
        );
        assert_eq!(
            store
                .campaign_ledger(&funding.admission.campaign_id)
                .unwrap(),
            ledger
        );
    }
}

#[tokio::test]
async fn execution_invalid_policy_never_claims_or_transfers_funding() {
    let (_dir, store, funding, request) = tests::setup();
    let root = tempfile::tempdir().unwrap();
    let policy = policy(&store, &funding, request.clone());
    let store = Arc::new(store);
    let before = store
        .campaign_ledger(&funding.admission.campaign_id)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    for case in 0..9 {
        let mut invalid = policy.clone();
        match case {
            0 => invalid.verification.admission.pool = Pool::Work,
            1 => invalid.verification.admission.campaign_id.push('x'),
            2 => invalid.verification.dispatch_id.push('x'),
            3 => invalid.model.identity.generation += 1,
            4 => invalid.work.objective.push('x'),
            5 => invalid.model.estimate.input_tokens = 101,
            6 => {
                invalid.verification.state =
                    crate::runtime_store::admission::DispatchState::Admitted
            }
            7 => invalid.evaluator_id.clear(),
            _ => invalid.work.deadline_ms = 0,
        }
        assert!(
            broker
                .execute_campaign(
                    &root.path().join("missing"),
                    root.path(),
                    root.path(),
                    invalid,
                    Instant::now() + Duration::from_secs(2),
                    |_| async { panic!("unauthorized evaluator") }
                )
                .await
                .is_err(),
            "case {case}"
        );
        assert!(store
            .campaign_execution(&funding.admission.campaign_id, "work")
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .campaign_ledger(&funding.admission.campaign_id)
                .unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn execution_spawn_failure_and_recovery_without_replay() {
    let (_dir, store, funding, request) = tests::setup();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(store);
    let policy = policy(&store, &funding, request.clone());
    let broker = ModelBroker::new(store.clone(), model(&request));
    let missing = root.path().join("missing");
    let record = broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            |_| async { panic!("no candidate") },
        )
        .await
        .unwrap();
    assert_eq!(record.phase, ExecutionPhase::ExecutingUnknown);
    assert!(!record.settled);
    // Host proof of terminated/no candidate resolves lifecycle, never success.
    let recovered = store
        .host_collect_campaign_evidence(&record, vec![])
        .unwrap();
    assert_eq!(
        recovered.phase,
        ExecutionPhase::Reviewed(Evaluation::Unverified)
    );
    let settled = store
        .settle_campaign_execution(&funding.admission.campaign_id, "work")
        .unwrap();
    assert!(settled.settled);
    assert_eq!(
        store
            .campaign_ledger(&funding.admission.campaign_id)
            .unwrap()
            .unwrap()
            .committed(Pool::Work)
            .unwrap()
            .tokens,
        0
    );
    assert_eq!(
        broker
            .execute_campaign(
                &missing,
                root.path(),
                root.path(),
                policy,
                Instant::now() + Duration::from_secs(2),
                |_| async { panic!("replay") }
            )
            .await
            .unwrap(),
        settled
    );
}

#[tokio::test]
async fn execution_unknown_holds_block_closure_and_review_recovery_is_fenced() {
    use tachyon_api::types::{Actor, EventEnvelope, WorkOutcome, WorkResult};
    let (dir, store, funding, request) = tests::setup_with_group(true);
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(store);
    let policy = policy(&store, &funding, request.clone());
    let broker = ModelBroker::new(store.clone(), model(&request));
    let missing = root.path().join("missing");
    let record = broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            |_| async { panic!() },
        )
        .await
        .unwrap();
    let candidate = WorkResult {
        attempt_id: None,
        candidate_refs: None,
        final_context: None,
        instruction_revision: None,
        work_id: "work".into(),
        objective: "bounded".into(),
        generation: 1,
        assignment: 1,
        evidence: Default::default(),
        timing: None,
        outcome: WorkOutcome::Completed {
            result: "ok".into(),
            artifacts: vec![],
            context: String::new(),
            suggested_reuse: false,
        },
    };
    let event: EventEnvelope = serde_json::from_value(serde_json::json!({
        "event_id": 1, "sequence": 1, "occurred_at_ms": 0, "session_id": "worker", "task_id": "worker",
        "actor": Actor::Worker { id: "worker".into() },
        "kind": "work_candidate", "candidate": candidate,
    })).unwrap();
    for field in [
        "session_id",
        "task_id",
        "actor",
        "conversation_id",
        "parent_task_id",
    ] {
        let mut foreign = serde_json::to_value(&event).unwrap();
        foreign[field] = if field == "actor" {
            serde_json::json!({"kind":"worker", "id":"foreign"})
        } else {
            serde_json::json!("foreign")
        };
        assert!(store
            .host_collect_campaign_evidence(&record, vec![serde_json::from_value(foreign).unwrap()])
            .is_err());
    }
    let ready = store
        .host_collect_campaign_evidence(&record, vec![event])
        .unwrap();
    assert_eq!(ready.phase, ExecutionPhase::EvidenceReady);
    let extra = LedgerCommand::ReserveAllocated {
        reservation_id: "late-provider".into(),
        allocation_id: funding.dispatch_id.clone(),
        pool: Pool::Work,
        reserved: Units {
            tokens: 30,
            cost_micro_usd: 30,
        },
    };
    store
        .campaign_ledger_command("late-provider", &funding.admission.campaign_id, extra)
        .unwrap();
    // Cancel an in-flight evaluator, then resume without replaying it.
    let (tx, rx) = tokio::sync::oneshot::channel();
    let run = broker.execute_campaign(
        &missing,
        root.path(),
        root.path(),
        policy.clone(),
        Instant::now() + Duration::from_secs(20),
        |_| async {
            tx.send(()).unwrap();
            std::future::pending::<Evaluation>().await
        },
    );
    let mut run = Box::pin(run);
    tokio::select! { _ = rx => {}, _ = &mut run => panic!("evaluation finished") }
    drop(run);
    let reviewing = store
        .campaign_execution(&funding.admission.campaign_id, "work")
        .unwrap()
        .unwrap();
    assert_eq!(reviewing.phase, ExecutionPhase::ReviewingUnknown);
    store
        .create_campaign_group(crate::runtime_store::groups::GroupSpec {
            group_id: "waiting".into(),
            campaign_id: funding.admission.campaign_id.clone(),
            parent: None,
            max_running: 3,
            work: (0..3)
                .map(|i| Admission {
                    work_id: format!("waiting-{i}"),
                    upper_bound: Units::default(),
                    ..funding.admission.clone()
                })
                .collect(),
        })
        .unwrap();
    // Primary process has stopped, but the unresolved evaluator holds one root slot.
    assert_eq!(
        store
            .dispatch_campaign_batch(3, |_| DispatchOutcome::Unknown)
            .unwrap(),
        2
    );
    let held = store
        .campaign_ledger(&funding.admission.campaign_id)
        .unwrap();
    assert_eq!(
        held.as_ref().unwrap().reservations
            [&format!("evaluation:{}", policy.verification.dispatch_id)]
            .usage,
        Usage::Unknown
    );
    drop(broker);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    assert_eq!(
        store
            .campaign_ledger(&funding.admission.campaign_id)
            .unwrap(),
        held
    );
    assert_eq!(
        store
            .settle_campaign_execution(&funding.admission.campaign_id, "work")
            .unwrap(),
        reviewing
    );
    let broker = ModelBroker::new(store.clone(), model(&request));
    let replay = broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            policy.clone(),
            Instant::now() + Duration::from_secs(2),
            |_| async { panic!("replayed review") },
        )
        .await
        .unwrap();
    assert_eq!(replay, reviewing);
    assert_eq!(
        store
            .dispatch_campaign_batch(3, |_| DispatchOutcome::Unknown)
            .unwrap(),
        0
    );
    store
        .host_finish_campaign_evaluation(&reviewing, Evaluation::Rejected)
        .unwrap();
    assert_eq!(
        store
            .dispatch_campaign_batch(3, |_| DispatchOutcome::Unknown)
            .unwrap(),
        1
    );
    assert!(store
        .host_finish_campaign_evaluation(&reviewing, Evaluation::Accepted)
        .is_err());
    let pending = store
        .settle_campaign_execution(&funding.admission.campaign_id, "work")
        .unwrap();
    assert!(!pending.settled);
    assert_eq!(
        pending.phase,
        ExecutionPhase::Reviewed(Evaluation::Rejected)
    );
    for usage in [
        Usage::Provisional(Units {
            tokens: 7,
            cost_micro_usd: 7,
        }),
        Usage::Final(Units {
            tokens: 7,
            cost_micro_usd: 7,
        }),
    ] {
        store
            .campaign_ledger_command(
                &format!("late:{usage:?}"),
                &funding.admission.campaign_id,
                LedgerCommand::Reconcile {
                    reservation_id: "late-provider".into(),
                    usage: usage.clone(),
                },
            )
            .unwrap();
        let current = store
            .settle_campaign_execution(&funding.admission.campaign_id, "work")
            .unwrap();
        assert_eq!(current.settled, matches!(usage, Usage::Final(_)));
    }
    let ledger = store
        .campaign_ledger(&funding.admission.campaign_id)
        .unwrap()
        .unwrap();
    assert_eq!(ledger.committed(Pool::Work).unwrap().tokens, 7);
    assert_eq!(ledger.committed(Pool::Verification).unwrap().tokens, 0);
    let mut changed = policy;
    changed.work.generation += 1;
    assert!(broker
        .execute_campaign(
            &missing,
            root.path(),
            root.path(),
            changed,
            Instant::now() + Duration::from_secs(2),
            |_| async { panic!() }
        )
        .await
        .is_err());
}
