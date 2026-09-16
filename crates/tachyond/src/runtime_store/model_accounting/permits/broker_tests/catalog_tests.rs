use super::*;
use crate::runtime_store::{
    admission::{Admission, DispatchOutcome, DispatchState},
    campaign_ledger::Envelope,
    coordination::WorkAddress,
    execution::{Evaluation, ExecutionPolicy},
    groups::WorkLimits,
    scheduler::{HostCandidate, HostExecution, HostScheduler, HostTemplate},
};
use tachyon_api::{
    agents::{Control, Request},
    types::{ApiRequest, ApiResponse, LifetimeClass, WorkRequest},
};

fn setup(
    limits: WorkLimits,
) -> (
    tempfile::TempDir,
    Arc<RuntimeStore>,
    HostExecution,
    HostTemplate,
) {
    let (_dir, _store, funding, request) = tests::setup();
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
    store
        .host_authorize_campaign_envelope(
            "grant",
            &campaign.id,
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
        .host_configure_work_limits(&campaign.id, limits)
        .unwrap();
    let candidate = |id: &str| {
        let admission = Admission {
            campaign_id: campaign.id.clone(),
            work_id: id.into(),
            objective: format!("objective-{id}"),
            ..funding.admission.clone()
        };
        let verification = Admission {
            work_id: format!("verify-{id}"),
            pool: Pool::Verification,
            ..admission.clone()
        };
        let mut model = request.clone();
        model.identity.campaign_id = campaign.id.clone();
        model.identity.work_id = id.into();
        model.estimate.max_request_bytes = 64000;
        HostCandidate {
            work: WorkRequest {
                context_refs: vec![],
                constraints: None,
                attempt: None,
                work_id: id.into(),
                objective: admission.objective.clone(),
                generation: 1,
                assignment: 1,
                deadline_ms: (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    + 60000) as u64,
                lifetime_class: LifetimeClass::Short,
            },
            admission,
            verification,
            model,
            evaluator_id: "fixture".into(),
            executable: std::env::var_os("GHOST_TEST_BIN")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| "/nonexistent/ghost".into()),
            workspace: dir.path().join("work"),
            home: dir.path().join("work"),
            evaluate: crate::runtime_store::scheduler::Evaluator::Callback(Arc::new(|_| {
                Box::pin(async { Evaluation::Accepted })
            })),
        }
    };
    let root = candidate("parent");
    let execution = HostExecution {
        policy: ExecutionPolicy {
            funding: store.host_admit_agent_work(root.admission, None).unwrap(),
            verification: store.admit_campaign_work(root.verification).unwrap(),
            work: root.work,
            model: root.model,
            evaluator_id: root.evaluator_id,
        },
        executable: root.executable,
        workspace: root.workspace,
        home: root.home,
        evaluate: root.evaluate,
    };
    let template = HostTemplate {
        template_id: "approved-batch".into(),
        parent: WorkAddress {
            campaign_id: campaign.id.clone(),
            work_id: "parent".into(),
        },
        candidates: vec![candidate("child-a"), candidate("child-b")],
        group_id: Some("children".into()),
        max_running: 2,
    };
    (dir, store, execution, template)
}

fn limits() -> WorkLimits {
    WorkLimits {
        total_work: 16,
        max_depth: 2,
        max_running: 3,
        max_resident: 16,
    }
}

mod allocation_tests;
mod dynamic_tests;
mod policy_actions;
mod work_tests;

#[tokio::test]
async fn child_scheduler_does_not_claim_another_owners_catalog_or_root() {
    let (_dir, store, root, template) = setup(limits());
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let mut unrelated = HostScheduler::command_children(broker.clone(), 3).unwrap();
    let owner = HostScheduler::command_children(broker, 3).unwrap();
    owner.approve(template.clone()).unwrap();
    let permit = register(&store, &root);
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    assert_eq!(unrelated.tick().await.unwrap(), 0);
    for c in &template.candidates {
        assert_eq!(
            store
                .admitted_work(&c.admission.campaign_id, &c.admission.work_id)
                .unwrap()
                .state,
            DispatchState::Admitted,
        );
    }
    assert!(store
        .campaign_execution(&template.parent.campaign_id, &template.parent.work_id)
        .unwrap()
        .is_none());
    unrelated.shutdown().await.unwrap();
    store.host_revoke_model_permit(&permit).unwrap();
}

#[test]
fn scheduler_rejects_work_and_verification_role_swaps() {
    let (_dir, store, root, _template) = setup(limits());
    for verification in [false, true] {
        let mut entry = root.clone();
        if verification {
            entry.policy.verification.admission.pool = Pool::Work;
        } else {
            entry.policy.funding.admission.pool = Pool::Verification;
        }
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
        assert!(HostScheduler::new(broker, vec![entry], 3).is_err());
    }
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN and IPython; localhost HTTP only"]
async fn actual_ghost_wait_preserves_python_at_root_cap_one() {
    assert!(std::env::var_os("GHOST_TEST_BIN").is_some());
    for (python, cancel) in [(false, false), (true, false), (true, true)] {
        let (_dir, store, mut root, mut template) = setup(WorkLimits {
            max_running: 1,
            max_resident: 2,
            ..limits()
        });
        template.group_id = None;
        template.max_running = 1;
        template.candidates.truncate(1);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        root.policy.model.estimate.base_url = url.clone();
        template.candidates[0].model.estimate.base_url = url;
        let broker = Arc::new(
            ModelBroker::new(store.clone(), model(&root.policy.model))
                .with_controls([Control::Spawn, Control::Wait]),
        );
        let mut scheduler = HostScheduler::new(broker, vec![root.clone()], 2).unwrap();
        scheduler.approve(template.clone()).unwrap();
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
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().unwrap())
                        })
                        .unwrap();
                    assert!(length <= 64000);
                    let mut bytes = vec![0; length];
                    socket.read_exact(&mut bytes).await.unwrap();
                    let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    let parent = String::from_utf8(bytes)
                        .unwrap()
                        .contains("objective-parent");
                    let outputs: Vec<_> = wire["messages"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|m| m["role"] == "tool")
                        .cloned()
                        .collect();
                    let tool = if parent && outputs.is_empty() {
                        if python {
                            Some((
                                "ipython",
                                serde_json::json!({"code": "import json, os\nfrom pathlib import Path\nPath('resident-pid').write_text(str(os.getpid()))\na = require('agents')\nretained = {'answer': 41}\nr = await a.spawn(template_id='approved-batch', command_id='once')\nids = json.loads(r['content'])['work_ids']\nw = await a.wait(work_ids=ids, mode='all', timeout_ms=10000)\nassert json.loads(w['content'])['completed'] == ids\nprint('retained-after-wait', retained['answer'] + 1)"}),
                            ))
                        } else {
                            Some((
                                "agents",
                                serde_json::json!({"action":"spawn","template_id":"approved-batch","command_id":"once"}),
                            ))
                        }
                    } else if parent && !python && outputs.len() == 1 {
                        Some((
                            "agents",
                            serde_json::json!({"action":"wait","work_ids":["child-a"],"mode":"all","timeout_ms":10000}),
                        ))
                    } else {
                        None
                    };
                    let response = if let Some((name, arguments)) = tool {
                        format!(
                            "data: {}\n\ndata: [DONE]\n\n",
                            serde_json::json!({
                                "choices":[{"delta":{"tool_calls":[{"index":0,"id":format!("call-{}",outputs.len()),"function":{"name":name,"arguments":arguments.to_string()}}]}}],
                                "usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}
                            })
                        )
                    } else {
                        if parent {
                            let output = outputs.last().expect("wait output");
                            let envelope: serde_json::Value =
                                serde_json::from_str(output["content"].as_str().unwrap()).unwrap();
                            assert_ne!(envelope["is_error"], true, "{envelope}");
                            if python {
                                assert!(
                                    envelope["content"]
                                        .as_str()
                                        .unwrap()
                                        .contains("retained-after-wait 42"),
                                    "{envelope}"
                                );
                            } else {
                                let wait: serde_json::Value =
                                    serde_json::from_str(envelope["content"].as_str().unwrap())
                                        .unwrap();
                                assert_eq!(wait["resumed"], true);
                                assert_eq!(wait["completed"], serde_json::json!(["child-a"]));
                            }
                        }
                        let (release, wait) = tokio::sync::oneshot::channel();
                        arrivals.send((parent, release)).unwrap();
                        wait.await.unwrap();
                        format!("{VALID}data: [DONE]\n\n")
                    };
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
                });
            }
        });
        let campaign = &template.parent.campaign_id;
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut seen = Vec::new();
        let mut held_child = None;
        loop {
            assert!(
                Instant::now() < deadline,
                "wait did not complete; python={python}, seen={seen:?}"
            );
            scheduler.tick().await.unwrap();
            while let Ok((parent, release)) = incoming.try_recv() {
                let state = store.campaign_work_status(campaign, "parent").unwrap();
                if parent {
                    assert_eq!(seen, vec![false]);
                    assert!(state.active && state.wait.is_none());
                } else {
                    assert!(!state.active && state.wait.is_some());
                }
                seen.push(parent);
                if cancel {
                    assert!(
                        !parent,
                        "cancelled parent must never issue another model call"
                    );
                    store.host_cancel_work(campaign, "parent", 1).unwrap();
                    held_child = Some(release);
                } else {
                    release.send(()).unwrap();
                }
            }
            if scheduler
                .outcome(campaign, "parent", 1)
                .unwrap()
                .is_some_and(|r| r.unwrap().settled)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            seen,
            if cancel {
                vec![false]
            } else {
                vec![false, true]
            }
        );
        if cancel {
            let record = store
                .campaign_execution(campaign, "parent")
                .unwrap()
                .unwrap();
            assert_eq!(
                record.phase,
                crate::runtime_store::execution::ExecutionPhase::Reviewed(Evaluation::Unverified)
            );
            assert!(
                store
                    .campaign_work_status(campaign, "parent")
                    .unwrap()
                    .cancellation_requested
            );
            let child = store.admitted_work(campaign, "child-a").unwrap();
            let ledger = store.campaign_ledger(campaign).unwrap().unwrap();
            assert!(
                ledger.reservations.values().any(|hold| {
                    hold.allocation.as_ref() == Some(&child.dispatch_id)
                        && hold.usage == Usage::Unknown
                }),
                "parent cancellation must not settle the child's pending provider request"
            );
            held_child.take().unwrap().send(()).unwrap();
        }
        scheduler.shutdown().await.unwrap();
        assert!(
            store
                .campaign_work_status(campaign, "parent")
                .unwrap()
                .terminal
        );
        if python {
            // Observation only: a guest PID is never allocation/cleanup authority.
            let pid = std::fs::read_to_string(_dir.path().join("work/resident-pid")).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let state = std::fs::read_to_string(format!("/proc/{pid}/stat"));
                if state
                    .as_ref()
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
                    || state
                        .as_ref()
                        .is_ok_and(|s| s.split_once(") ").is_some_and(|(_, s)| s.starts_with('Z')))
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "resident kernel survived cleanup: {state:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        http.abort();
    }
}

#[test]
fn catalog_approval_is_bounded_without_reserving_work() {
    let (_dir, store, root, template) = setup(limits());
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let scheduler = HostScheduler::new(broker, vec![root], 3).unwrap();
    let before = store.campaign_ledger(&template.parent.campaign_id).unwrap();
    let mut unfunded = template.clone();
    unfunded.candidates[0].admission.upper_bound = Units::default();
    assert!(scheduler.approve(unfunded).is_err());
    let mut fake = template.clone();
    fake.candidates[0].model.estimate.input_tokens = 0;
    assert!(scheduler.approve(fake).is_err());
    for batch in 0..8 {
        let mut approved = template.clone();
        approved.template_id = format!("batch-{batch}");
        approved.group_id = Some(format!("group-{batch}"));
        approved.candidates = (0..if batch == 7 { 31 } else { 32 })
            .map(|n| {
                let mut c = template.candidates[0].clone();
                let id = format!("child-{batch}-{n}");
                c.admission.work_id = id.clone();
                c.work.work_id = id.clone();
                c.model.identity.work_id = id.clone();
                c.verification.work_id = format!("verify-{id}");
                c
            })
            .collect();
        scheduler.approve(approved.clone()).unwrap();
        assert!(
            scheduler.approve(approved).is_err(),
            "approval cannot replace callbacks/policy"
        );
    }
    let mut overflow = template.clone();
    overflow.candidates.truncate(1);
    assert!(scheduler.approve(overflow).unwrap_err().contains("full"));
    assert_eq!(
        before,
        store.campaign_ledger(&template.parent.campaign_id).unwrap()
    );
    assert!(store.host_catalog.lock().unwrap().admitted.is_empty());
    let tachyon_api::agents::Reply::Templates {
        templates,
        next_cursor,
    } = store.catalog_templates(&template.parent, None, 2).unwrap()
    else {
        panic!()
    };
    assert_eq!(templates.len(), 2);
    assert_eq!(next_cursor.as_deref(), Some("batch-1"));
    let encoded = serde_json::to_string(&templates).unwrap();
    assert!(
        !encoded.contains("workspace")
            && !encoded.contains("model")
            && !encoded.contains("executable")
    );
    for actor in [
        WorkAddress {
            work_id: "child-0-0".into(),
            ..template.parent.clone()
        },
        WorkAddress {
            campaign_id: "another-campaign".into(),
            ..template.parent.clone()
        },
    ] {
        let tachyon_api::agents::Reply::Templates { templates, .. } =
            store.catalog_templates(&actor, None, 32).unwrap()
        else {
            panic!()
        };
        assert!(
            templates.is_empty(),
            "discovery must not cross parent or campaign scope"
        );
    }
    drop(scheduler);
    let tachyon_api::agents::Reply::Templates { templates, .. } =
        store.catalog_templates(&template.parent, None, 32).unwrap()
    else {
        panic!()
    };
    assert!(
        templates.is_empty(),
        "finished owner withdraws launch authority"
    );
}
fn group() -> Request {
    Request::Group {
        template_id: "approved-batch".into(),
        command_id: "group-once".into(),
        max_running: None,
    }
}
fn register(store: &RuntimeStore, root: &HostExecution) -> ModelPermit {
    let a = &root.policy.funding.admission;
    let claim = store
        .claim_campaign_work_matching(|w| w.admission.work_id == a.work_id)
        .unwrap()
        .unwrap();
    store
        .reconcile_campaign_dispatch(
            &claim,
            DispatchOutcome::Registered {
                worker_id: "parent-worker".into(),
            },
        )
        .unwrap();
    store
        .host_issue_model_permit(
            root.policy.model.clone(),
            store.admitted_work(&a.campaign_id, &a.work_id).unwrap(),
            None,
        )
        .unwrap()
}

#[test]
fn catalog_atomic_bounds_scope_replay_and_recovery() {
    for failure in [
        "none",
        "depth",
        "total",
        "work-budget",
        "verification-budget",
        "work-cost",
        "verification-cost",
        "cap",
        "expired",
        "cancelled-parent",
    ] {
        let mut bounds = limits();
        if failure == "depth" {
            bounds.max_depth = 0;
        }
        if failure == "total" {
            bounds.total_work = 5;
        }
        let (dir, store, root, mut template) = setup(bounds);
        if failure == "work-budget" {
            template.candidates[1].admission.upper_bound.tokens = 1000;
        }
        if failure == "verification-budget" {
            template.candidates[1].verification.upper_bound.tokens = 1000;
        }
        if failure == "work-cost" {
            template.candidates[1].admission.upper_bound.cost_micro_usd = 1000;
        }
        if failure == "verification-cost" {
            template.candidates[1]
                .verification
                .upper_bound
                .cost_micro_usd = 1000;
        }
        if failure == "expired" {
            template.candidates[1].work.deadline_ms = 1;
        }
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
        let scheduler = HostScheduler::new(broker, vec![root.clone()], 3).unwrap();
        let permit = register(&store, &root);
        if failure == "cancelled-parent" {
            store
                .host_cancel_work(&template.parent.campaign_id, "parent", 1)
                .unwrap();
        }
        let before = store.campaign_ledger(&template.parent.campaign_id).unwrap();
        scheduler.approve(template.clone()).unwrap();
        assert_eq!(
            before,
            store.campaign_ledger(&template.parent.campaign_id).unwrap(),
            "approval must be lazy"
        );
        assert!(store
            .admitted_work(&template.parent.campaign_id, "child-a")
            .is_err());
        for request in [
            Request::Spawn {
                template_id: "approved-batch".into(),
                command_id: "wrong-kind".into(),
            },
            Request::Group {
                template_id: "unknown".into(),
                command_id: "unknown".into(),
                max_running: None,
            },
        ] {
            assert!(store
                .broker_control(permit.0, &root.policy.model, request)
                .is_err());
        }
        let mut wrong = template.parent.clone();
        wrong.work_id = "stranger".into();
        assert!(store.admit_catalog(wrong, group()).is_err());
        let mut foreign = template.parent.clone();
        foreign.campaign_id = "foreign".into();
        assert!(store.admit_catalog(foreign, group()).is_err());
        let request = if failure == "cap" {
            Request::Group {
                template_id: template.template_id.clone(),
                command_id: "group-once".into(),
                max_running: Some(3),
            }
        } else {
            group()
        };
        let result = store.broker_control(permit.0, &root.policy.model, request);
        if failure != "none" {
            assert!(result.is_err(), "{failure}");
            assert_eq!(
                before,
                store.campaign_ledger(&template.parent.campaign_id).unwrap(),
                "{failure}"
            );
            for id in ["child-a", "child-b", "verify-child-a", "verify-child-b"] {
                assert!(store
                    .admitted_work(&template.parent.campaign_id, id)
                    .is_err());
            }
            assert!(store
                .campaign_group_status(&template.parent.campaign_id, "children")
                .is_err());
            assert_eq!(
                store
                    .host_agent_control(template.parent.clone())
                    .unwrap()
                    .list(None, 32)
                    .unwrap()
                    .items
                    .len(),
                1
            );
            assert!(store.host_catalog.lock().unwrap().admitted.is_empty());
            continue;
        }
        let receipt = serde_json::to_value(result.unwrap()).unwrap();
        assert_eq!(
            receipt["work_ids"],
            serde_json::json!(["child-a", "child-b"])
        );
        assert_eq!(receipt["outcome"], "admitted");
        let after = store.campaign_ledger(&template.parent.campaign_id).unwrap();
        assert!(
            store
                .resize_campaign_group(&template.parent.campaign_id, "children", 1, 3)
                .is_err(),
            "shared resize must preserve the approved host ceiling"
        );
        assert!(
            store
                .broker_control(
                    permit.0,
                    &root.policy.model,
                    Request::GroupResize {
                        group_id: "children".into(),
                        expected_revision: 1,
                        max_running: 3,
                    }
                )
                .is_err(),
            "worker cannot grow past catalog host ceiling"
        );
        for (revision, cap) in [(1, 1), (2, 2)] {
            store
                .broker_control(
                    permit.0,
                    &root.policy.model,
                    Request::GroupResize {
                        group_id: "children".into(),
                        expected_revision: revision,
                        max_running: cap,
                    },
                )
                .unwrap();
        }
        for _ in 0..3 {
            assert_eq!(
                receipt,
                serde_json::to_value(
                    store
                        .broker_control(permit.0, &root.policy.model, group())
                        .unwrap()
                )
                .unwrap()
            );
        }
        // Model the commit/publication crash window: the receipt survives while
        // the in-memory descriptors are absent. Replay must republish, not admit.
        store.host_catalog.lock().unwrap().admitted.clear();
        assert_eq!(
            receipt,
            serde_json::to_value(
                store
                    .broker_control(permit.0, &root.policy.model, group())
                    .unwrap()
            )
            .unwrap()
        );
        assert_eq!(store.host_catalog.lock().unwrap().admitted.len(), 2);
        assert_eq!(
            after,
            store.campaign_ledger(&template.parent.campaign_id).unwrap()
        );
        assert!(store
            .broker_control(
                permit.0,
                &root.policy.model,
                Request::Group {
                    template_id: template.template_id.clone(),
                    command_id: "different-command".into(),
                    max_running: None,
                }
            )
            .is_err());
        assert!(store
            .broker_control(
                permit.0,
                &root.policy.model,
                Request::Group {
                    template_id: template.template_id.clone(),
                    command_id: "group-once".into(),
                    max_running: Some(1),
                }
            )
            .is_err());
        for id in ["child-a", "child-b"] {
            let status = store
                .host_agent_control(template.parent.clone())
                .unwrap()
                .status(&WorkAddress {
                    work_id: id.into(),
                    ..template.parent.clone()
                })
                .unwrap();
            assert_eq!(status.parent.as_deref(), Some("parent"));
            assert_eq!(status.admission, DispatchState::Admitted);
            assert!(status.result.is_none());
        }
        drop(scheduler);
        drop(store);
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
        let mut recovered_root = root;
        recovered_root.policy.funding = store
            .admitted_work(&template.parent.campaign_id, "parent")
            .unwrap();
        let scheduler = HostScheduler::new(broker, vec![recovered_root], 3).unwrap();
        let mut changed = template.clone();
        changed.candidates[0].work.objective.push('!');
        assert!(scheduler.approve(changed).is_err());
        scheduler.approve(template.clone()).unwrap();
        assert_eq!(
            store.host_catalog.lock().unwrap().admitted.len(),
            2,
            "recovery publishes without worker replay"
        );
        assert_eq!(
            receipt,
            serde_json::to_value(
                store
                    .admit_catalog(template.parent.clone(), group())
                    .unwrap()
            )
            .unwrap()
        );
        assert_eq!(
            after,
            store.campaign_ledger(&template.parent.campaign_id).unwrap()
        );
    }
}

#[test]
fn catalog_spawn_concurrent_replay_command_collision_and_active_cap() {
    let (_dir, store, root, template) = setup(WorkLimits {
        max_running: 1,
        ..limits()
    });
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let scheduler = HostScheduler::new(broker, vec![root.clone()], 3).unwrap();
    let permit = register(&store, &root);
    let mut one = template.clone();
    one.template_id = "one".into();
    one.group_id = None;
    one.candidates.truncate(1);
    one.max_running = 1;
    scheduler.approve(one).unwrap();
    let mut two = template.clone();
    two.template_id = "two".into();
    two.group_id = None;
    two.candidates.remove(0);
    two.max_running = 1;
    scheduler.approve(two).unwrap();
    let request = Request::Spawn {
        template_id: "one".into(),
        command_id: "once".into(),
    };
    let mut forged = root.policy.model.clone();
    forged.estimate.input_micro_usd_per_million = 0;
    assert!(store
        .broker_control(permit.0, &forged, request.clone())
        .is_err());
    assert!(store.host_catalog.lock().unwrap().admitted.is_empty());
    let replies = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    serde_json::to_value(
                        store
                            .broker_control(permit.0, &root.policy.model, request.clone())
                            .unwrap(),
                    )
                    .unwrap()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(replies.iter().all(|r| *r == replies[0]));
    assert!(store
        .broker_control(
            permit.0,
            &root.policy.model,
            Request::Spawn {
                template_id: "two".into(),
                command_id: "once".into()
            }
        )
        .is_err());
    assert!(
        store
            .claim_campaign_work_matching(|w| w.admission.work_id == "child-a")
            .unwrap()
            .is_none(),
        "parent retains the sole slot; admission does not wait"
    );
    assert_eq!(store.host_catalog.lock().unwrap().admitted.len(), 1);
    store.host_revoke_model_permit(&permit).unwrap();
    assert!(store
        .broker_control(permit.0, &root.policy.model, request)
        .is_err());
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost HTTP only"]
async fn actual_ghost_catalog_spawn_and_group_admission() {
    assert!(
        std::env::var_os("GHOST_TEST_BIN").is_some(),
        "fresh Ghost binary required"
    );
    for grouped in [false, true] {
        let (_dir, store, mut root, mut template) = setup(limits());
        // This fixture intentionally holds parent + two children at HTTP barriers.
        let mut store = Arc::try_unwrap(store).ok().unwrap();
        store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(
            tachyon_util::config::ResourceLimits {
                max_model_calls: 3,
                max_execution_jobs: 3,
                ..Default::default()
            },
        )
        .unwrap();
        let store = Arc::new(store);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        root.policy.model.estimate.base_url = url.clone();
        for c in &mut template.candidates {
            c.model.estimate.base_url = url.clone();
        }
        if !grouped {
            template.group_id = None;
            template.candidates.truncate(1);
        }
        let request = if grouped {
            group()
        } else {
            Request::Spawn {
                template_id: template.template_id.clone(),
                command_id: "spawn-once".into(),
            }
        };
        let mut cross_parent = template.clone();
        cross_parent.template_id = "other-parent".into();
        cross_parent.parent.work_id = "child-a".into();
        cross_parent.group_id = None;
        cross_parent.candidates.truncate(1);
        let c = &mut cross_parent.candidates[0];
        c.admission.work_id = "grandchild".into();
        c.work.work_id = "grandchild".into();
        c.model.identity.work_id = "grandchild".into();
        c.verification.work_id = "verify-grandchild".into();
        let broker = Arc::new(
            ModelBroker::new(store.clone(), model(&root.policy.model)).with_controls([
                Control::Spawn,
                Control::Group,
                Control::Status,
            ]),
        );
        let mut scheduler = HostScheduler::new(broker, vec![root.clone()], 3).unwrap();
        scheduler.approve(template.clone()).unwrap();
        scheduler.approve(cross_parent).unwrap();
        let actions = vec![
            request.clone(),
            request,
            Request::Spawn {
                template_id: "unknown".into(),
                command_id: "unknown".into(),
            },
            Request::Spawn {
                template_id: "other-parent".into(),
                command_id: "foreign".into(),
            },
        ];
        let (arrivals, mut incoming) = tokio::sync::mpsc::unbounded_channel();
        let http = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let arrivals = arrivals.clone();
                let actions = actions.clone();
                tokio::spawn(async move {
                    let mut headers = Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        headers.push(socket.read_u8().await.unwrap());
                        assert!(headers.len() < 16384);
                    }
                    let headers = String::from_utf8(headers).unwrap();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().unwrap())
                        })
                        .unwrap();
                    assert!(length <= 64000);
                    let mut bytes = vec![0; length];
                    socket.read_exact(&mut bytes).await.unwrap();
                    let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    let text = String::from_utf8(bytes).unwrap();
                    let id = ["parent", "child-a", "child-b"]
                        .into_iter()
                        .find(|id| text.contains(&format!("objective-{id}")))
                        .unwrap()
                        .to_owned();
                    let outputs: Vec<_> = wire["messages"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|m| m["role"] == "tool")
                        .cloned()
                        .collect();
                    let response = if id == "parent" && outputs.is_empty() {
                        let calls: Vec<_> = actions.iter().enumerate().map(|(i, a)| serde_json::json!({
                            "index": i, "id": format!("admission-{i}"), "function": {"name":"agents", "arguments":serde_json::to_string(a).unwrap()}
                        })).collect();
                        format!(
                            "data: {}\n\ndata: [DONE]\n\n",
                            serde_json::json!({
                                "choices":[{"delta":{"tool_calls":calls}}],
                                "usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}
                            })
                        )
                    } else {
                        let (release, wait) = tokio::sync::oneshot::channel();
                        arrivals.send((id, outputs, release)).unwrap();
                        wait.await.unwrap();
                        format!("{VALID}data: [DONE]\n\n")
                    };
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
                });
            }
        });
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut releases = std::collections::BTreeMap::new();
        while releases.len() < template.candidates.len() + 1 {
            assert!(
                Instant::now() < deadline,
                "admission/parallel launch timed out"
            );
            scheduler.tick().await.unwrap();
            while let Ok((id, outputs, release)) = incoming.try_recv() {
                if id == "parent" {
                    assert_eq!(outputs.len(), 4);
                    let envelopes: Vec<serde_json::Value> = outputs
                        .iter()
                        .map(|m| serde_json::from_str(m["content"].as_str().unwrap()).unwrap())
                        .collect();
                    assert_eq!(
                        envelopes.iter().filter(|o| o["is_error"] == true).count(),
                        2
                    );
                    let receipts: Vec<serde_json::Value> = envelopes
                        .iter()
                        .filter(|o| o["is_error"] != true)
                        .map(|o| serde_json::from_str(o["content"].as_str().unwrap()).unwrap())
                        .collect();
                    assert_eq!(receipts[0], receipts[1]);
                    assert_eq!(receipts[0]["outcome"], "admitted");
                    assert_eq!(
                        receipts[0]["work_ids"].as_array().unwrap().len(),
                        template.candidates.len()
                    );
                    for c in &template.candidates {
                        assert!(store
                            .admitted_work(&template.parent.campaign_id, &c.work.work_id)
                            .is_ok());
                        assert!(scheduler
                            .outcome(&template.parent.campaign_id, &c.work.work_id, 1)
                            .unwrap()
                            .is_none());
                    }
                    assert!(store
                        .admitted_work(&template.parent.campaign_id, "grandchild")
                        .is_err());
                }
                assert!(
                    releases.insert(id, release).is_none(),
                    "duplicate model launch"
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let campaign = &template.parent.campaign_id;
        let ledger = store.campaign_ledger(campaign).unwrap().unwrap();
        // The ledger also counts each queued verifier's reserved hold.
        assert_eq!(
            ledger.active_inferences(),
            (template.candidates.len() + 1) * 2
        );
        assert_eq!(
            ledger
                .reservations
                .iter()
                .filter(|(id, r)| !ledger.allocations.contains_key(*id)
                    && r.pool == Pool::Work
                    && r.usage == Usage::Unknown)
                .count(),
            template.candidates.len() + 1,
            "real overlapping model claims share the campaign"
        );
        assert_eq!(ledger.allocations.len(), template.candidates.len() + 1);
        // Parent completion recursively cancels unfinished children. Keep its
        // HTTP barrier held until child verification has settled.
        let mut parent_release = Some(releases.remove("parent").unwrap());
        for release in releases.into_values() {
            release.send(()).unwrap();
        }
        loop {
            assert!(
                Instant::now() < deadline,
                "execution/verification timed out"
            );
            scheduler.tick().await.unwrap();
            if parent_release.is_some()
                && template.candidates.iter().all(|c| {
                    scheduler
                        .outcome(campaign, &c.work.work_id, 1)
                        .unwrap()
                        .is_some_and(|r| {
                            let record = r.unwrap();
                            assert_eq!(
                                record.phase,
                                crate::runtime_store::execution::ExecutionPhase::Reviewed(
                                    Evaluation::Accepted
                                )
                            );
                            record.settled
                        })
                })
            {
                parent_release.take().unwrap().send(()).unwrap();
            }
            let ids = std::iter::once("parent")
                .chain(template.candidates.iter().map(|c| c.work.work_id.as_str()));
            if ids.into_iter().all(|id| {
                scheduler
                    .outcome(campaign, id, 1)
                    .unwrap()
                    .is_some_and(|r| r.unwrap().settled)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for _ in 0..3 {
            assert_eq!(scheduler.tick().await.unwrap(), 0);
        }
        assert!(
            incoming.try_recv().is_err(),
            "replay must not launch a second child"
        );
        let ledger = store.campaign_ledger(campaign).unwrap().unwrap();
        assert_eq!(ledger.active_inferences(), 0);
        assert_eq!(
            ledger.allocations.len(),
            (template.candidates.len() + 1) * 2
        );
        for c in &template.candidates {
            let record = store
                .campaign_execution(campaign, &c.work.work_id)
                .unwrap()
                .unwrap();
            assert_eq!(record.policy.model, c.model);
            assert_eq!(record.policy.funding.admission, c.admission);
            assert_eq!(record.policy.verification.admission, c.verification);
            assert!(record.settled);
        }
        scheduler.shutdown().await.unwrap();
        http.abort();
    }
}
