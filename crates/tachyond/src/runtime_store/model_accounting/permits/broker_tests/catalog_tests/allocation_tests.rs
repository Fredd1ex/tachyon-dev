use super::*;
use tachyon_api::campaign::{AllocationMode, CampaignAllocation};

#[tokio::test]
async fn reallocate_host_scope_cas_receipts_and_real_accountant() {
    use crate::runtime_store::model_accounting::{AdmittedAccounting, DaemonAccounting};
    use tachyon_api::campaign::{AllocationAction, AllocationControl, AllocationSignal};
    for failure in [
        "none",
        "group-revision",
        "ledger-revision",
        "generation",
        "target-generation",
        "verification",
        "scope",
        "cancelled",
        "target-cancelled",
        "terminal",
        "target-terminal",
        "closed",
        "debt",
        "owner",
    ] {
        let (dir, store, root, template) = setup(limits());
        let c = template.parent.campaign_id.clone();
        let scheduler = HostScheduler::command_children(
            Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model))),
            3,
        )
        .unwrap();
        scheduler.approve(template).unwrap();
        let permit = register(&store, &root);
        store
            .broker_control(permit.0, &root.policy.model, group())
            .unwrap();
        let mut works = Vec::new();
        for id in ["child-a", "child-b"] {
            let child =
                store.host_catalog.lock().unwrap().admitted[&(c.clone(), id.into(), 1)].clone();
            register(&store, &child);
            let funding = store.admitted_work(&c, id).unwrap();
            store
                .campaign_ledger_command(
                    &format!("fund:{}", funding.dispatch_id),
                    &c,
                    LedgerCommand::FundAllocation {
                        reservation_id: funding.dispatch_id.clone(),
                        work_id: id.into(),
                    },
                )
                .unwrap();
            works.push((funding, child.policy.model.clone()));
        }
        let (source, _) = &works[0];
        let (target, request) = &works[1];
        let hold = Units {
            tokens: 90,
            cost_micro_usd: 90,
        };
        store
            .campaign_ledger_command(
                "target-hold",
                &c,
                LedgerCommand::ReserveAllocated {
                    reservation_id: "target-hold".into(),
                    allocation_id: target.dispatch_id.clone(),
                    pool: Pool::Work,
                    reserved: hold,
                },
            )
            .unwrap();
        let accountant = AdmittedAccounting {
            accounting: DaemonAccounting {
                store: &store,
                authorized: request.clone(),
            },
            funding: target.clone(),
        };
        assert!(accountant.reserve(request).await.is_err());
        if failure == "cancelled" {
            store.host_cancel_work(&c, "child-a", 1).unwrap();
        }
        if failure == "target-cancelled" {
            store.host_cancel_work(&c, "child-b", 1).unwrap();
        }
        if failure == "terminal" {
            store.host_acknowledge_work_terminal(source).unwrap();
        }
        if failure == "target-terminal" {
            store.host_acknowledge_work_terminal(target).unwrap();
        }
        if failure == "closed" {
            store
                .campaign_ledger_command(
                    "close-source",
                    &c,
                    LedgerCommand::CloseAllocation {
                        reservation_id: source.dispatch_id.clone(),
                    },
                )
                .unwrap();
        }
        if failure == "debt" {
            store
                .campaign_ledger_command(
                    "debt",
                    &c,
                    LedgerCommand::Reconcile {
                        reservation_id: "target-hold".into(),
                        usage: Usage::Provisional(Units {
                            tokens: 91,
                            cost_micro_usd: 90,
                        }),
                    },
                )
                .unwrap();
        }
        store.select_allocation_owner(&c, "owner").unwrap();
        let before = store.campaign_ledger(&c).unwrap().unwrap();
        let group_before = store.campaign_group_status(&c, "children").unwrap();
        let config = CampaignAllocation {
            mode: AllocationMode::Deterministic,
            max_running: 2,
            allowed_actions: vec![AllocationAction::Reallocate],
            signals: vec![AllocationSignal {
                command_id: "transfer-once".into(),
                group_id: "children".into(),
                expected_revision: group_before.0.revision + u64::from(failure == "group-revision"),
                action: AllocationControl::Reallocate {
                    source_work_id: if failure == "scope" {
                        "parent"
                    } else {
                        "child-a"
                    }
                    .into(),
                    source_generation: if failure == "generation" { 2 } else { 1 },
                    target_work_id: if failure == "verification" {
                        "verify-child-b"
                    } else {
                        "child-b"
                    }
                    .into(),
                    target_generation: if failure == "target-generation" { 2 } else { 1 },
                    tokens: 25,
                    cost_micro_usd: 25,
                    expected_ledger_revision: before.revision
                        + u64::from(failure == "ledger-revision"),
                },
            }],
        };
        let approved = vec![("children".into(), vec!["child-a".into(), "child-b".into()])];
        let result = store.allocation_control_tick(
            &c,
            if failure == "owner" { "wrong" } else { "owner" },
            &config,
            &approved,
        );
        if failure != "none" {
            assert!(result.is_err(), "{failure}");
            assert_eq!(
                store.campaign_ledger(&c).unwrap().unwrap(),
                before,
                "{failure}"
            );
            assert_eq!(
                store.campaign_group_status(&c, "children").unwrap(),
                group_before,
                "{failure}"
            );
            if failure == "group-revision" {
                // The ledger write precedes group CAS: neither receipt may survive failure.
                let mut retry = config.clone();
                retry.signals[0].expected_revision = group_before.0.revision;
                assert!(store
                    .allocation_control_tick(&c, "owner", &retry, &approved)
                    .unwrap());
                let moved = store.campaign_ledger(&c).unwrap().unwrap();
                assert_eq!(moved.revision, before.revision + 1);
                assert_eq!(
                    moved
                        .allocation_allowance(&target.dispatch_id)
                        .unwrap()
                        .tokens,
                    125
                );
            }
            continue;
        }
        assert!(result.unwrap());
        let moved = store.campaign_ledger(&c).unwrap().unwrap();
        assert_eq!(moved.reservations, before.reservations);
        assert_eq!(moved.envelope, before.envelope);
        assert_eq!(
            moved.committed(Pool::Work).unwrap(),
            before.committed(Pool::Work).unwrap()
        );
        assert_eq!(
            moved.allocation_allowance(&target.dispatch_id).unwrap(),
            Units {
                tokens: 125,
                cost_micro_usd: 125
            }
        );
        let mut raised_request = request.clone();
        raised_request.estimate.output_tokens += 1;
        assert!(
            accountant.reserve(&raised_request).await.is_err(),
            "transfer cannot raise host request policy"
        );
        assert_eq!(store.campaign_ledger(&c).unwrap().unwrap(), moved);
        accountant.reserve(request).await.unwrap();
        let after = store.campaign_ledger(&c).unwrap();
        assert!(!store
            .allocation_control_tick(&c, "owner", &config, &approved)
            .unwrap());
        let mut conflict = config.clone();
        conflict.signals[0].expected_revision += 1;
        assert!(store
            .allocation_control_tick(&c, "owner", &conflict, &approved)
            .is_err());
        drop(accountant);
        drop(scheduler);
        drop(store);
        let reopened = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        assert!(!reopened
            .allocation_control_tick(&c, "owner", &config, &approved)
            .unwrap());
        assert_eq!(reopened.campaign_ledger(&c).unwrap(), after);
    }
}

#[tokio::test]
async fn allocation_explicit_controls_are_atomic_scoped_and_replay_safe() {
    use tachyon_api::campaign::{AllocationAction, AllocationControl, AllocationSignal};
    for stop in [false, true] {
        let (dir, store, root, template) = setup(limits());
        let c = template.parent.campaign_id.clone();
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
        let scheduler = HostScheduler::command_children(broker, 3).unwrap();
        scheduler.approve(template).unwrap();
        let permit = register(&store, &root);
        store
            .broker_control(permit.0, &root.policy.model, group())
            .unwrap();
        let approved = vec![("children".into(), vec!["child-a".into(), "child-b".into()])];
        store.select_allocation_owner(&c, "owner").unwrap();
        let active = store
            .claim_campaign_work_matching(|w| w.admission.work_id == "child-a")
            .unwrap()
            .unwrap();
        let before = store.campaign_group_status(&c, "children").unwrap();
        let ledger = store.campaign_ledger(&c).unwrap();
        let mut allocation = CampaignAllocation {
            mode: AllocationMode::Deterministic,
            max_running: 2,
            allowed_actions: vec![AllocationAction::Pause, AllocationAction::Stop],
            signals: vec![AllocationSignal {
                command_id: "explicit-control".into(),
                group_id: "children".into(),
                expected_revision: before.0.revision + 1,
                action: if stop {
                    AllocationControl::Stop {
                        work_id: "child-a".into(),
                        generation: 1,
                    }
                } else {
                    AllocationControl::Pause
                },
            }],
        };
        assert!(store
            .allocation_control_tick(&c, "owner", &allocation, &approved)
            .is_err());
        assert_eq!(store.campaign_group_status(&c, "children").unwrap(), before);
        assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
        allocation.signals[0].expected_revision = before.0.revision;
        if stop {
            for (id, generation) in [("parent", 1), ("child-a", 2), ("invented", 1)] {
                allocation.signals[0].action = AllocationControl::Stop {
                    work_id: id.into(),
                    generation,
                };
                assert!(store
                    .allocation_control_tick(&c, "owner", &allocation, &approved)
                    .is_err());
                assert_eq!(store.campaign_group_status(&c, "children").unwrap(), before);
                assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
            }
            allocation.signals[0].action = AllocationControl::Stop {
                work_id: "child-a".into(),
                generation: 1,
            };
        }
        assert!(store
            .allocation_control_tick(&c, "other-owner", &allocation, &approved)
            .is_err());
        assert!(store
            .allocation_control_tick(&c, "owner", &allocation, &approved)
            .unwrap());
        let after = store.campaign_group_status(&c, "children").unwrap();
        assert_eq!(after.0.revision, before.0.revision + 1);
        assert_eq!(after.0.max_running, if stop { 2 } else { 0 });
        assert_eq!(after.2, 1, "intent is not active cleanup");
        assert_eq!(after.1.len(), before.1.len(), "no child created");
        assert_eq!(
            store.campaign_ledger(&c).unwrap(),
            ledger,
            "active holds stay reserved"
        );
        assert_eq!(
            store
                .campaign_work_status(&c, "child-a")
                .unwrap()
                .cancellation_requested,
            stop
        );
        assert!(
            !store
                .campaign_work_status(&c, "child-b")
                .unwrap()
                .cancellation_requested
        );
        assert!(
            !store
                .campaign_work_status(&c, "parent")
                .unwrap()
                .cancellation_requested
        );
        assert!(!store
            .allocation_control_tick(&c, "owner", &allocation, &approved)
            .unwrap());
        assert_eq!(store.campaign_group_status(&c, "children").unwrap(), after);
        allocation.signals[0].expected_revision += 1;
        assert!(store
            .allocation_control_tick(&c, "owner", &allocation, &approved)
            .is_err());
        store.host_acknowledge_work_terminal(&active).unwrap();
        assert_eq!(store.campaign_group_status(&c, "children").unwrap().2, 0);
        assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
        allocation.signals[0].expected_revision = before.0.revision;
        let terminal = store.campaign_group_status(&c, "children").unwrap();
        drop(scheduler);
        drop(store);
        let reopened = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        assert!(!reopened
            .allocation_control_tick(&c, "owner", &allocation, &approved)
            .unwrap());
        assert_eq!(
            reopened.campaign_group_status(&c, "children").unwrap(),
            terminal
        );
        assert_eq!(reopened.campaign_ledger(&c).unwrap(), ledger);
    }
}

#[tokio::test]
async fn allocation_manual_revision_relinquishes_explicit_control() {
    use tachyon_api::campaign::{AllocationAction, AllocationControl, AllocationSignal};
    let (_dir, store, root, template) = setup(limits());
    let c = template.parent.campaign_id.clone();
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let scheduler = HostScheduler::command_children(broker, 3).unwrap();
    scheduler.approve(template).unwrap();
    let permit = register(&store, &root);
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    let allocation = CampaignAllocation {
        mode: AllocationMode::Deterministic,
        max_running: 2,
        allowed_actions: vec![AllocationAction::Pause],
        signals: vec![AllocationSignal {
            command_id: "pause".into(),
            group_id: "children".into(),
            expected_revision: 1,
            action: AllocationControl::Pause,
        }],
    };
    store.resize_campaign_group(&c, "children", 1, 1).unwrap();
    let before = store.campaign_group_status(&c, "children").unwrap();
    store.select_allocation_owner(&c, "replacement").unwrap();
    assert!(!store
        .allocation_control_tick(
            &c,
            "replacement",
            &allocation,
            &[("children".into(), vec!["child-a".into(), "child-b".into()])]
        )
        .unwrap());
    assert_eq!(store.campaign_group_status(&c, "children").unwrap(), before);
}

#[tokio::test]
async fn allocation_agent_steering_relinquishes_before_the_next_tick() {
    let (_dir, store, root, template) = setup(limits());
    let c = template.parent.campaign_id.clone();
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let scheduler = HostScheduler::command_children(broker, 3).unwrap();
    scheduler.approve(template).unwrap();
    let permit = register(&store, &root);
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    let before = store.campaign_group_status(&c, "children").unwrap();
    for revision in [2, 1] {
        let result = store.broker_control(
            permit.0,
            &root.policy.model,
            Request::Steer {
                work_id: "child-a".into(),
                command_id: "steering".into(),
                expected_revision: revision,
                instructions: "Preserve the manually chosen concurrency".into(),
            },
        );
        if revision == 2 {
            assert!(result.is_err());
            assert_eq!(store.campaign_group_status(&c, "children").unwrap(), before);
        } else {
            result.unwrap();
        }
    }
    store.select_allocation_owner(&c, "new-owner").unwrap();
    store
        .allocation_tick(
            &c,
            "new-owner",
            1,
            2,
            &[("children".into(), vec!["child-a".into(), "child-b".into()])],
            &[],
        )
        .unwrap();
    assert_eq!(
        store
            .campaign_group_status(&c, "children")
            .unwrap()
            .0
            .max_running,
        2
    );
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost HTTP only"]
async fn allocation_real_host_group_pipeline_grows_without_new_work_then_drains() {
    assert!(std::env::var_os("GHOST_TEST_BIN").is_some());
    for (mode, blocked) in [
        (AllocationMode::Fixed, "none"),
        (AllocationMode::ModelProposed, "none"),
        (AllocationMode::Deterministic, "none"),
        (AllocationMode::Deterministic, "unknown"),
        (AllocationMode::Deterministic, "unallocated"),
        (AllocationMode::Deterministic, "unallocated-provisional"),
        (AllocationMode::Deterministic, "debt"),
    ] {
        let (_dir, store, root, mut template) = setup(limits());
        let c = template.parent.campaign_id.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut headers = Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        headers.push(socket.read_u8().await.unwrap());
                        assert!(headers.len() <= 16384);
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
                    assert!(length <= 64000);
                    socket.read_exact(&mut vec![0; length]).await.unwrap();
                    let response = format!("{VALID}data: [DONE]\n\n");
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
                });
            }
        });
        let mut third = template.candidates[1].clone();
        third.admission.work_id = "child-c".into();
        third.verification.work_id = "verify-child-c".into();
        third.model.identity.work_id = "child-c".into();
        third.work.work_id = "child-c".into();
        template.candidates.push(third);
        for candidate in &mut template.candidates {
            candidate.model.estimate.base_url = url.clone();
            candidate.evaluate =
                crate::runtime_store::scheduler::Evaluator::Callback(Arc::new(|result| {
                    Box::pin(async move {
                        if result.work_id == "child-a" {
                            Evaluation::Accepted
                        } else {
                            Evaluation::Rejected
                        }
                    })
                }));
        }
        let broker = Arc::new(ModelBroker::new(
            store.clone(),
            model(&template.candidates[0].model),
        ));
        let mut scheduler = HostScheduler::command_children(broker, 4).unwrap();
        scheduler
            .select_allocation(
                &c,
                Some(CampaignAllocation {
                    allowed_actions: tachyon_api::campaign::default_allocation_actions(),
                    signals: vec![],
                    mode,
                    max_running: 2,
                }),
            )
            .unwrap();
        scheduler.approve(template.clone()).unwrap();
        let permit = register(&store, &root);
        store
            .broker_control(
                permit.0,
                &root.policy.model,
                Request::Group {
                    template_id: template.template_id.clone(),
                    command_id: "admit".into(),
                    max_running: Some(1),
                },
            )
            .unwrap();
        let initial = store.campaign_group_status(&c, "children").unwrap();
        assert_eq!(initial.0.max_running, 1);
        assert_eq!(initial.1.len(), 3);
        if blocked != "none" {
            assert_eq!(scheduler.tick().await.unwrap(), 1);
            tokio::time::timeout(Duration::from_secs(10), async {
                while !store
                    .campaign_execution(&c, "child-a")
                    .unwrap()
                    .is_some_and(|r| r.settled)
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            use crate::runtime_store::campaign_ledger::LedgerCommand;
            store
                .campaign_ledger_command(
                    "fund-root",
                    &c,
                    LedgerCommand::FundAllocation {
                        reservation_id: root.policy.funding.dispatch_id.clone(),
                        work_id: "parent".into(),
                    },
                )
                .unwrap();
            store
                .campaign_ledger_command(
                    "unknown",
                    &c,
                    LedgerCommand::ReserveAllocated {
                        reservation_id: "unowned-request".into(),
                        allocation_id: root.policy.funding.dispatch_id.clone(),
                        pool: Pool::Work,
                        reserved: Units {
                            tokens: 1,
                            cost_micro_usd: 1,
                        },
                    },
                )
                .unwrap();
            if blocked.starts_with("unallocated") {
                store
                    .campaign_ledger_command(
                        "settle-allocated",
                        &c,
                        LedgerCommand::Reconcile {
                            reservation_id: "unowned-request".into(),
                            usage: Usage::Final(Units::default()),
                        },
                    )
                    .unwrap();
                store
                    .campaign_ledger_command(
                        "unallocated",
                        &c,
                        LedgerCommand::Reserve {
                            reservation_id: "unallocated-request".into(),
                            pool: Pool::Work,
                            reserved: Units {
                                tokens: 1,
                                cost_micro_usd: 1,
                            },
                        },
                    )
                    .unwrap();
                if blocked == "unallocated-provisional" {
                    store
                        .campaign_ledger_command(
                            "provisional",
                            &c,
                            LedgerCommand::Reconcile {
                                reservation_id: "unallocated-request".into(),
                                usage: Usage::Provisional(Units::default()),
                            },
                        )
                        .unwrap();
                }
            }
            if blocked == "debt" {
                store
                    .campaign_ledger_command(
                        "debt",
                        &c,
                        LedgerCommand::Reconcile {
                            reservation_id: "unowned-request".into(),
                            usage: Usage::Final(Units {
                                tokens: 2,
                                cost_micro_usd: 2,
                            }),
                        },
                    )
                    .unwrap();
            }
            scheduler.tick().await.unwrap();
            let (group, work, _) = store.campaign_group_status(&c, "children").unwrap();
            assert_eq!(
                group.max_running, 1,
                "{blocked} must suppress useful-outcome expansion"
            );
            assert_eq!(work.len(), 3);
            scheduler.shutdown().await.unwrap();
            server.abort();
            continue;
        }
        let mut peak = 1;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                scheduler.tick().await.unwrap();
                let (group, work, active) = store.campaign_group_status(&c, "children").unwrap();
                peak = peak.max(group.max_running);
                assert_eq!(work.len(), 3, "policy cannot create Work");
                assert!(active <= 2, "never doubles beyond host cap");
                if work.iter().all(|w| w.terminal) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        scheduler.tick().await.unwrap();
        assert_eq!(
            peak,
            if mode == AllocationMode::Deterministic {
                2
            } else {
                1
            }
        );
        assert_eq!(
            store
                .campaign_group_status(&c, "children")
                .unwrap()
                .0
                .max_running,
            1
        );
        let members = store
            .host_agent_control(template.parent.clone())
            .unwrap()
            .list(None, 32)
            .unwrap();
        assert_eq!(
            members.items.len(),
            4,
            "parent plus three approved children only"
        );
        scheduler.shutdown().await.unwrap();
        server.abort();
    }
}

#[tokio::test]
async fn allocation_controller_takeover_and_manual_resize_are_durable() {
    let (_dir, store, root, template) = setup(limits());
    let c = template.parent.campaign_id.clone();
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let mut scheduler = HostScheduler::command_children(broker, 3).unwrap();
    scheduler
        .select_allocation(
            &c,
            Some(CampaignAllocation {
                allowed_actions: tachyon_api::campaign::default_allocation_actions(),
                signals: vec![],
                mode: AllocationMode::Deterministic,
                max_running: 1,
            }),
        )
        .unwrap();
    scheduler.approve(template.clone()).unwrap();
    let permit = register(&store, &root);
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    store.select_allocation_owner(&c, "replacement").unwrap();
    assert!(scheduler
        .tick()
        .await
        .unwrap_err()
        .contains("stale allocation controller"));
    let groups = vec![("children".into(), vec!["child-a".into(), "child-b".into()])];
    store
        .allocation_tick(&c, "replacement", 1, 2, &groups, &[])
        .unwrap();
    let (g, work, _) = store.campaign_group_status(&c, "children").unwrap();
    assert_eq!(g.max_running, 1);
    assert_eq!(work.len(), 2);
    let ledger = store.campaign_ledger(&c).unwrap();
    store
        .resize_campaign_group(&c, "children", g.revision, 2)
        .unwrap();
    store
        .allocation_tick(&c, "replacement", 1, 2, &groups, &[])
        .unwrap();
    assert_eq!(
        store
            .campaign_group_status(&c, "children")
            .unwrap()
            .0
            .max_running,
        2
    );
    assert_eq!(
        store.campaign_ledger(&c).unwrap(),
        ledger,
        "resizing never reserves money"
    );
}
