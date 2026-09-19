use super::*;
use crate::runtime_store::{
    campaign_launch::tests::fixture,
    campaign_ledger::{Envelope, LedgerCommand, Pool, Units},
    model_accounting::services::{ServicePolicy, ServicePurpose},
};
use tachyon_api::{
    campaign::CampaignOversight,
    todo::{TodoActor, TodoRequest},
};
use tachyon_model::{accounting::RequestEstimate, Model, ModelConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const VALID: &str = r#"{"summary":"Evidence remains advisory","findings":[],"refs":[],"blockers":[],"attention":"none"}"#;
fn policy(m: &CampaignManifest, endpoint: &str) -> ServicePolicy {
    let o = m.oversight.as_ref().unwrap();
    ServicePolicy {
        purpose: ServicePurpose::CampaignOversight,
        allowance: Units {
            tokens: o.tokens,
            cost_micro_usd: o.cost_micro_usd,
        },
        max_requests: o.max_assessments,
        timeout_ms: o.timeout_ms,
        estimate: RequestEstimate {
            base_url: endpoint.into(),
            model: m.model.clone(),
            provider: "openrouter".into(),
            pricing_revision: m.pricing_revision.clone(),
            max_request_bytes: m.max_request_bytes,
            input_tokens: m.input_tokens,
            output_tokens: m.output_tokens,
            input_micro_usd_per_million: m.input_micro_usd_per_million,
            output_micro_usd_per_million: m.output_micro_usd_per_million,
            other_micro_usd: m.other_micro_usd,
        },
    }
}
fn setup(destination: bool) -> (tempfile::TempDir, Arc<RuntimeStore>, CampaignManifest) {
    let (dir, store, mut m) = fixture();
    m.oversight = Some(CampaignOversight {
        tokens: 60,
        cost_micro_usd: 60,
        max_assessments: 2,
        timeout_ms: 3000,
        conversation_id: destination.then(|| tachyon_api::FOREGROUND_ID.into()),
    });
    m.validate(super::super::monitor::now_ms()).unwrap();
    let tx = store.database.begin_write().unwrap();
    initialize_campaign_in(&tx, &m).unwrap();
    RuntimeStore::set_campaign_status_in(&tx, &m.campaign_id, CampaignStatus::Running).unwrap();
    tx.open_table(crate::runtime_store::campaign_launch::LAUNCHES).unwrap().insert(m.campaign_id.as_str(), serde_json::to_vec(&json!({
        "schema_version":1, "manifest":m, "base_url":"http://127.0.0.1:1", "manifest_sha256":hash(&m).unwrap()
    })).unwrap().as_slice()).unwrap();
    tx.commit().unwrap();
    store
        .host_authorize_campaign_envelope(
            "grant",
            &m.campaign_id,
            Envelope {
                work: Units {
                    tokens: m.work_tokens,
                    cost_micro_usd: m.work_cost_micro_usd,
                },
                verification: Units {
                    tokens: m.verification_tokens,
                    cost_micro_usd: m.verification_cost_micro_usd,
                },
                max_active_inferences: 2,
            },
        )
        .unwrap();
    (dir, store, m)
}
type Request = (Value, tokio::sync::oneshot::Sender<(String, bool)>);

#[test]
fn conversation_campaign_selection_uses_only_validated_stored_links() {
    use tachyon_api::conversation_campaign::Request as Conversation;
    for linked in [false, true] {
        let (_dir, store, m) = setup(linked);
        let list = store
            .conversation_campaign(tachyon_api::FOREGROUND_ID, &Conversation::List {})
            .unwrap();
        assert_eq!(
            list["campaigns"].as_array().unwrap().len(),
            usize::from(linked)
        );
        assert_eq!(
            store
                .conversation_campaign("another-conversation", &Conversation::List {})
                .unwrap()["campaigns"],
            json!([])
        );
        let status = Conversation::Status {
            campaign_id: m.campaign_id.clone(),
            work_id: None,
            after: None,
            limit: 1,
            include_plan: false,
        };
        let denied = store
            .conversation_campaign(tachyon_api::FOREGROUND_ID, &status)
            .unwrap_err();
        assert!(
            denied.contains(if linked {
                "root enrollment"
            } else {
                "not explicitly linked"
            }),
            "{denied}"
        );
        // A recognizable name is not root enrollment or a conversation link.
        assert!(store
            .conversation_campaign("another-conversation", &status)
            .is_err());
        if linked {
            use crate::runtime_store::{admission::Admission, groups::WorkLimits};
            store
                .host_configure_work_limits(&m.campaign_id, WorkLimits::default())
                .unwrap();
            store
                .host_admit_agent_work(
                    Admission {
                        campaign_id: m.campaign_id.clone(),
                        work_id: format!("{}-root", m.campaign_id),
                        objective: m.objective.clone(),
                        generation: 1,
                        instruction_revision: 1,
                        pool: Pool::Work,
                        upper_bound: Units {
                            tokens: 1,
                            cost_micro_usd: 1,
                        },
                    },
                    None,
                )
                .unwrap();
            for n in 0..20 {
                todo(&store, &m, n);
            }
            let snapshot = store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &status)
                .unwrap();
            assert!(snapshot.get("plan").is_none());
            assert_eq!(snapshot["works"].as_array().unwrap().len(), 1);
            assert_eq!(
                snapshot["works"][0]["work_id"],
                format!("{}-root", m.campaign_id)
            );
            let mut with_plan = status.clone();
            if let Conversation::Status { include_plan, .. } = &mut with_plan {
                *include_plan = true;
            }
            let snapshot = store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &with_plan)
                .unwrap();
            assert_eq!(snapshot["plan"]["todos"].as_array().unwrap().len(), 16);
            assert!(!snapshot["plan"]["next_cursor"].is_null());

            let work_id = format!("{}-root", m.campaign_id);
            let steer = Conversation::Steer {
                campaign_id: m.campaign_id.clone(),
                work_id: work_id.clone(),
                command_id: "first-steer".into(),
                expected_revision: 1,
                instructions: "Keep the candidate".into(),
            };
            let before = store.campaign_ledger(&m.campaign_id).unwrap();
            let receipt = store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &steer)
                .unwrap();
            assert_eq!(receipt["accepted_revision"], 2);
            let mut later = steer.clone();
            if let Conversation::Steer {
                command_id,
                expected_revision,
                ..
            } = &mut later
            {
                *command_id = "later-steer".into();
                *expected_revision = 2;
            }
            store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &later)
                .unwrap();
            let snapshot = store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &status)
                .unwrap();
            assert_eq!(snapshot["works"][0]["accepted_revision"], 3);
            assert_eq!(snapshot["works"][0]["applied_revision"], 1);
            assert_eq!(snapshot["works"][0]["delivered_revision"], 0);
            let work = store.admitted_work(&m.campaign_id, &work_id).unwrap();
            let tx = store.database.begin_write().unwrap();
            let boundary =
                RuntimeStore::prepare_agent_boundary_in(&tx, &work.admission, "boundary")
                    .unwrap()
                    .unwrap();
            tx.commit().unwrap();
            let snapshot = store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &status)
                .unwrap();
            assert_eq!(snapshot["works"][0]["applied_revision"], 3);
            assert_eq!(snapshot["works"][0]["delivered_revision"], 0);
            let tx = store.database.begin_write().unwrap();
            RuntimeStore::acknowledge_agent_boundary_in(
                &tx,
                &work.admission,
                &boundary.id,
                boundary.cursor,
            )
            .unwrap();
            tx.commit().unwrap();
            let snapshot = store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &status)
                .unwrap();
            assert_eq!(snapshot["works"][0]["delivered_revision"], 3);
            assert_eq!(store.campaign_ledger(&m.campaign_id).unwrap(), before);

            let cancel = Conversation::Cancel {
                campaign_id: m.campaign_id.clone(),
                work_id,
                command_id: "cancel".into(),
                generation: 1,
            };
            let cancelled = store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &cancel)
                .unwrap();
            let ledger = store.campaign_ledger(&m.campaign_id).unwrap();
            assert_eq!(
                store
                    .conversation_campaign(tachyon_api::FOREGROUND_ID, &steer)
                    .unwrap(),
                receipt
            );
            assert_eq!(
                store
                    .conversation_campaign(tachyon_api::FOREGROUND_ID, &cancel)
                    .unwrap(),
                cancelled
            );
            assert_eq!(store.campaign_ledger(&m.campaign_id).unwrap(), ledger);
            let snapshot = store
                .conversation_campaign(tachyon_api::FOREGROUND_ID, &status)
                .unwrap();
            assert_eq!(snapshot["works"][0]["cancellation_done"], true);
        }
        let tx = store.database.begin_write().unwrap();
        let mut changed = m.clone();
        changed.oversight.as_mut().unwrap().conversation_id =
            Some(tachyon_api::FOREGROUND_ID.into());
        changed.objective = "tampered".into();
        tx.open_table(crate::runtime_store::campaign_launch::LAUNCHES).unwrap().insert(m.campaign_id.as_str(), serde_json::to_vec(&json!({"schema_version":1,"manifest":changed,"base_url":"http://127.0.0.1:1","manifest_sha256":hash(&m).unwrap()})).unwrap().as_slice()).unwrap();
        tx.commit().unwrap();
        assert!(store
            .conversation_campaign(tachyon_api::FOREGROUND_ID, &Conversation::List {})
            .unwrap_err()
            .contains("digest"));
    }
}

async fn provider(
    m: &CampaignManifest,
) -> (
    Model,
    String,
    tokio::sync::mpsc::Receiver<Request>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let model = Model::new(ModelConfig {
        base_url: endpoint.clone(),
        api_key: "loopback-only".into(),
        model: m.model.clone(),
        temperature: 0.0,
        max_completion_tokens: Some(m.output_tokens),
        context_length: None,
        parallel_tool_calls: false,
        reasoning: Default::default(),
        routing: None,
        debug: false,
        debug_log: None,
    });
    let (send, recv) = tokio::sync::mpsc::channel(4);
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
                assert!(header.len() < 16384);
            }
            let length: usize = String::from_utf8(header)
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            let mut bytes = vec![0; length];
            socket.read_exact(&mut bytes).await.unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            let (reply, wait) = tokio::sync::oneshot::channel();
            if send.send((body, reply)).await.is_err() {
                break;
            }
            let Ok((text, billed)) = wait.await else {
                continue;
            };
            let mut frame = json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]});
            if billed {
                frame["usage"] = json!({"prompt_tokens":2,"completion_tokens":1,"total_tokens":3,"cost":0.000003});
            }
            let body = format!("data: {frame}\n\ndata: [DONE]\n\n");
            let _ = socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
        }
    });
    (model, endpoint, recv, task)
}
fn todo(store: &RuntimeStore, m: &CampaignManifest, n: u64) {
    let scope = TodoScope::Campaign {
        campaign_id: m.campaign_id.clone(),
    };
    store
        .todos(crate::runtime_store::todo::TodoAuthority::Bound {
            scope: scope.clone(),
            actor: TodoActor {
                source: "operator".into(),
                actor: "fixture".into(),
            },
        })
        .unwrap()
        .execute(TodoRequest::Add {
            scope,
            command_id: format!("todo-{n}"),
            expected_revision: n,
            title: format!("Task {n}"),
            description: "x".repeat(1024),
        })
        .unwrap();
}

#[tokio::test]
async fn counted_requests_own_charges_are_fresh_other_spend_stale_and_bound_persists() {
    let (_dir, store, m) = setup(true);
    for n in 0..25 {
        todo(&store, &m, n);
    }
    let (model, endpoint, mut http, server) = provider(&m).await;
    let permit = store
        .host_authorize_service(&m.campaign_id, "oversight", policy(&m, &endpoint), None)
        .unwrap();
    let broker = Arc::new(ModelBroker::new(store.clone(), model));
    // A real root request uses the same broker and ledger, but its own admitted
    // Work permit and remaining allowance. Oversight never occupies that slot.
    use crate::runtime_store::{
        admission::{Admission, DispatchOutcome},
        model_accounting::ModelBrokerRequest,
    };
    use tachyon_model::accounting::{RequestClass, RequestReservation, WorkIdentity};
    let root = format!("{}-root", m.campaign_id);
    store
        .admit_campaign_work(Admission {
            campaign_id: m.campaign_id.clone(),
            work_id: root.clone(),
            objective: m.objective.clone(),
            generation: 1,
            instruction_revision: 1,
            pool: Pool::Work,
            upper_bound: Units {
                tokens: 40,
                cost_micro_usd: 40,
            },
        })
        .unwrap();
    let work = store
        .claim_campaign_work_matching(|w| w.admission.work_id == root)
        .unwrap()
        .unwrap();
    store
        .reconcile_campaign_dispatch(
            &work,
            DispatchOutcome::Registered {
                worker_id: root.clone(),
            },
        )
        .unwrap();
    let reservation = RequestReservation {
        identity: WorkIdentity {
            campaign_id: m.campaign_id.clone(),
            work_id: root.clone(),
            attempt_id: "root-attempt".into(),
            generation: 1,
            instruction_revision: 1,
            class: RequestClass::Work,
        },
        estimate: policy(&m, &endpoint).estimate,
    };
    let root_permit = store
        .host_issue_model_permit(
            reservation.clone(),
            store.admitted_work(&m.campaign_id, &root).unwrap(),
            None,
        )
        .unwrap();
    let root_messages = [ChatMessage::new(Role::User, "root request")];
    let mut sink = |_: &str| {};
    let root_call = broker.execute(
        ModelBrokerRequest {
            permit: &root_permit,
            request_id: "root-request",
            reservation,
            messages: &root_messages,
            tools: None,
            streamed_argument: None,
            deadline: Instant::now() + Duration::from_secs(5),
        },
        &mut sink,
    );
    let reply_root = async {
        let (body, reply) = http.recv().await.unwrap();
        assert_eq!(body["messages"][0]["content"], "root request");
        reply.send(("root evidence".into(), true)).unwrap();
    };
    let (root_result, ()) = tokio::join!(root_call, reply_root);
    root_result.unwrap();
    for n in 0..2 {
        if n == 1 {
            store
                .request_campaign_assessment(&m.campaign_id, "explicit", true)
                .unwrap();
        }
        let claim = store.observe_oversight(&m, true).unwrap().unwrap();
        let prepared = assessment::prepare(
            &claim.request,
            &[Capability::Todo, Capability::Monitor],
            &registry::builtin(),
        )
        .unwrap();
        assert_eq!(prepared.snapshot.todos.len(), 20);
        assert!(prepared.snapshot.todos_partial);
        let messages = [
            ChatMessage::new(Role::System, prepared.system),
            ChatMessage::new(Role::User, prepared.input),
        ];
        let mut sink = |_: &str| {};
        let call = broker.execute_service(&permit, &claim.request.request_id, &messages, &mut sink);
        let host = async {
            let (body, reply) = http.recv().await.unwrap();
            assert!(body.get("tools").is_none());
            assert_eq!(body["model"], "fixture");
            assert!(store.observe_oversight(&m, true).unwrap().is_none());
            if n == 1 {
                store
                    .campaign_ledger_command(
                        "other-spend",
                        &m.campaign_id,
                        LedgerCommand::ReserveAllocated {
                            reservation_id: "other".into(),
                            allocation_id: work.dispatch_id.clone(),
                            pool: Pool::Work,
                            reserved: Units {
                                tokens: 1,
                                cost_micro_usd: 1,
                            },
                        },
                    )
                    .unwrap();
            }
            reply.send((VALID.into(), true)).unwrap();
        };
        let (completion, ()) = tokio::join!(call, host);
        let completion = completion.unwrap();
        let result = assessment::validate_completion(
            &completion.text,
            false,
            completion.finish_reason.as_deref(),
            &prepared.snapshot,
        )
        .unwrap();
        store.finish_oversight(&m, &claim, Ok(result)).unwrap();
    }
    let ApiResponse::CampaignAssessments { records } =
        store.campaign_assessments(&m.campaign_id).unwrap()
    else {
        panic!()
    };
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].status, "published");
    assert_eq!(records[1].status, "stale");
    assert!(store.observe_oversight(&m, true).unwrap().is_none());
    let own = service_id(&m.campaign_id, "oversight");
    let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
    let usage: Vec<_> = ledger
        .reservations
        .values()
        .filter(|r| r.allocation.as_deref() == Some(&own))
        .collect();
    assert_eq!(usage.len(), 2);
    assert!(usage.iter().all(|r| r.usage
        == Usage::Final(Units {
            tokens: 3,
            cost_micro_usd: 3
        })));
    assert_eq!(
        ledger
            .reservations
            .values()
            .filter(|r| matches!(r.usage, Usage::Final(_)))
            .count(),
        3
    );
    assert_eq!(ledger.envelope.verification.tokens, 10);
    let delivery = store
        .claim_assessment_delivery(tachyon_api::FOREGROUND_ID, 1)
        .unwrap()
        .unwrap();
    assert_eq!(delivery.assessment.id, records[0].request_id);
    assert!(store
        .claim_assessment_delivery("unrelated", 1)
        .unwrap()
        .is_none());
    let replay = store
        .claim_assessment_delivery(tachyon_api::FOREGROUND_ID, 5001)
        .unwrap()
        .unwrap();
    assert_eq!(delivery.assessment, replay.assessment);
    let id = &delivery.assessment.id;
    let mut metadata = tachyon_api::InteractionMetadata::new(
        format!("{id}:published"),
        id,
        tachyon_api::FOREGROUND_ID,
        2,
    );
    metadata.causation_id = Some(id.clone());
    let event = tachyon_api::InteractionEventEnvelope {
        metadata,
        event: tachyon_api::InteractionEvent::UserVisibleNotificationPublished {
            text: delivery.assessment.advisory(),
        },
    };
    for invalid in 0..7 {
        let mut unrelated = event.clone();
        match invalid {
            0 => unrelated.metadata.turn_id = Some("7".into()),
            1 => unrelated.metadata.conversation_id = "other".into(),
            2 => unrelated.metadata.correlation_id = "other".into(),
            3 => unrelated.metadata.causation_id = None,
            4 => unrelated.metadata.generation = 1,
            5 => unrelated.metadata.protocol_version = 99,
            _ => {
                unrelated.event = tachyon_api::InteractionEvent::ConversationFinished {
                    text: delivery.assessment.advisory(),
                }
            }
        }
        assert!(store.acknowledge_assessment_delivery(&unrelated).is_err());
    }
    use std::io::{BufRead, BufReader};
    let socket = _dir.path().join("foreground.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let mut foreground =
        crate::tests::task(tachyon_api::FOREGROUND_ID, tachyon_api::AgentState::Running);
    foreground.control_socket = Some(socket.to_string_lossy().into_owned());
    let mut registry = crate::Registry {
        runtime_store: Some(store.clone()),
        ..Default::default()
    };
    registry
        .tasks
        .insert(tachyon_api::FOREGROUND_ID.into(), foreground);
    registry.tasks.insert(
        "busy-worker".into(),
        crate::tests::task("busy-worker", tachyon_api::AgentState::Running),
    );
    let events = registry.subscribe(tachyon_api::FOREGROUND_ID).unwrap();
    let registry = Arc::new(std::sync::Mutex::new(registry));
    crate::messaging::deliver_attention(&registry, &store);
    let (stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut wire = String::new();
    BufReader::new(stream).read_line(&mut wire).unwrap();
    let command: tachyon_api::InteractionCommandEnvelope =
        serde_json::from_str(wire.trim().strip_prefix("input\t").unwrap()).unwrap();
    assert!(
        matches!(&command.command, tachyon_api::InteractionCommand::PublishCampaignAssessment { assessment } if assessment == &delivery.assessment)
    );
    assert_eq!(command.metadata.turn_id, None);
    let wire = serde_json::to_string(&event).unwrap();
    crate::push_event(
        &registry,
        "busy-worker",
        tachyon_api::EventStream::Stdout,
        &wire,
    );
    assert!(events.try_recv().is_err());
    crate::push_event(
        &registry,
        tachyon_api::FOREGROUND_ID,
        tachyon_api::EventStream::Stderr,
        &wire,
    );
    assert!(events.try_recv().is_err());
    crate::push_event(
        &registry,
        tachyon_api::FOREGROUND_ID,
        tachyon_api::EventStream::Stdout,
        &wire,
    );
    let received: tachyon_api::InteractionEventEnvelope =
        serde_json::from_str(&events.try_recv().unwrap().data).unwrap();
    assert_eq!(received.metadata.turn_id, None);
    assert_eq!(received.metadata.message_id, event.metadata.message_id);
    crate::push_event(
        &registry,
        tachyon_api::FOREGROUND_ID,
        tachyon_api::EventStream::Stdout,
        &wire,
    );
    assert!(events.try_recv().is_err());
    assert_eq!(
        registry.lock().unwrap().tasks["busy-worker"].info.state,
        tachyon_api::AgentState::Running
    );
    assert_eq!(store.pending_history().unwrap().len(), 1);
    assert!(tokio::time::timeout(Duration::from_millis(30), http.recv())
        .await
        .is_err());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_execute_disabled_zero_calls_and_opt_in_funds_before_root() {
    for enabled in [false, true] {
        let (dir, store, mut m) = fixture();
        m.max_active_inferences = 3;
        if enabled {
            m.oversight = Some(CampaignOversight {
                tokens: 60,
                cost_micro_usd: 60,
                max_assessments: 1,
                timeout_ms: 1000,
                conversation_id: None,
            });
        }
        let (model, endpoint, mut http, server) = provider(&m).await;
        let launch: Launch = serde_json::from_value(json!({"schema_version":1, "manifest":m,
            "base_url":endpoint, "manifest_sha256":hash(&m).unwrap()}))
        .unwrap();
        let tx = store.database.begin_write().unwrap();
        initialize_campaign_in(&tx, &m).unwrap();
        RuntimeStore::set_campaign_status_in(&tx, &m.campaign_id, CampaignStatus::Running).unwrap();
        tx.open_table(crate::runtime_store::campaign_launch::LAUNCHES)
            .unwrap()
            .insert(
                m.campaign_id.as_str(),
                serde_json::to_vec(&launch).unwrap().as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
        let (cancel, _) = watch::channel(false);
        let execute = crate::runtime_store::campaign_launch::execute(
            store.clone(),
            dir.path().into(),
            launch,
            model,
            cancel,
        );
        let respond = async {
            if enabled {
                let (body, reply) = tokio::time::timeout(Duration::from_secs(5), http.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(body.get("tools").is_none());
                reply.send((VALID.into(), true)).unwrap();
            }
        };
        let (_result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(execute, respond)
        })
        .await
        .unwrap();
        let root = store
            .admitted_work(&m.campaign_id, &format!("{}-root", m.campaign_id))
            .unwrap();
        assert_eq!(
            root.admission.upper_bound.tokens,
            if enabled { 40 } else { 100 }
        );
        assert_eq!(
            root.admission.upper_bound.cost_micro_usd,
            if enabled { 40 } else { 100 }
        );
        let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
        assert_eq!(ledger.envelope.verification.tokens, 10);
        assert_eq!(
            ledger
                .allocations
                .contains_key(&service_id(&m.campaign_id, "oversight")),
            enabled
        );
        assert!(tokio::time::timeout(Duration::from_millis(30), http.recv())
            .await
            .is_err());
        server.abort();
    }
}

#[tokio::test]
async fn loop_coalesces_explicit_requests_internal_only_and_unknown_never_retries() {
    for billed in [true, false] {
        let (_dir, store, m) = setup(false);
        let (model, endpoint, mut http, server) = provider(&m).await;
        let permit = store
            .host_authorize_service(&m.campaign_id, "oversight", policy(&m, &endpoint), None)
            .unwrap();
        let broker = Arc::new(ModelBroker::new(store.clone(), model));
        let (cancel, cancellation) = watch::channel(false);
        let (done, completion) = watch::channel(false);
        let owner = tokio::spawn(run(
            broker,
            m.clone(),
            permit,
            cancellation,
            completion,
            Instant::now() + Duration::from_secs(5),
        ));
        let (_, reply) = http.recv().await.unwrap();
        for command in ["one", "one", "two"] {
            store
                .request_campaign_assessment(&m.campaign_id, command, true)
                .unwrap();
        }
        reply.send((VALID.into(), billed)).unwrap();
        if billed {
            let (_, reply) = http.recv().await.unwrap();
            reply.send((VALID.into(), true)).unwrap();
        }
        done.send_replace(true);
        tokio::time::timeout(Duration::from_secs(3), owner)
            .await
            .unwrap()
            .unwrap();
        let ApiResponse::CampaignAssessments { records } =
            store.campaign_assessments(&m.campaign_id).unwrap()
        else {
            panic!()
        };
        assert_eq!(records.len(), if billed { 2 } else { 1 });
        assert!(store
            .request_campaign_assessment(&m.campaign_id, "one", true)
            .is_ok());
        assert!(store
            .request_campaign_assessment(&m.campaign_id, "late", true)
            .is_err());
        assert_eq!(
            records[0].status,
            if billed { "published" } else { "unknown" }
        );
        assert!(store
            .claim_assessment_delivery(tachyon_api::FOREGROUND_ID, 10)
            .unwrap()
            .is_none());
        assert!(tokio::time::timeout(Duration::from_millis(30), http.recv())
            .await
            .is_err());
        drop(cancel);
        server.abort();
    }
}

#[test]
fn simultaneous_claims_replay_reopen_and_transactional_trigger_rollback() {
    let (dir, store, m) = setup(false);
    let mut tasks = Vec::new();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    for _ in 0..2 {
        let store = store.clone();
        let m = m.clone();
        let barrier = barrier.clone();
        tasks.push(std::thread::spawn(move || {
            barrier.wait();
            store.observe_oversight(&m, true).unwrap().is_some()
        }));
    }
    barrier.wait();
    assert_eq!(
        tasks
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
    {
        let tx = store.database.begin_write().unwrap();
        trigger_in(&tx, &m.campaign_id, "new_finding").unwrap();
    }
    let tx = store.database.begin_write().unwrap();
    assert!(load(&tx, &m.campaign_id).unwrap().pending.is_empty());
    drop(tx);
    drop(store);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    assert!(store.observe_oversight(&m, true).unwrap().is_none());
    let ApiResponse::CampaignAssessments { records } =
        store.campaign_assessments(&m.campaign_id).unwrap()
    else {
        panic!()
    };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "claimed_unknown");
}

#[test]
fn owner_close_races_explicit_request_without_acknowledging_orphaned_work() {
    let (_dir, store, m) = setup(false);
    let claim = store.observe_oversight(&m, true).unwrap().unwrap();
    store
        .finish_oversight(&m, &claim, Err("fixture".into()))
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let closing = {
        let store = store.clone();
        let id = m.campaign_id.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            store.close_oversight(&id, false).unwrap()
        })
    };
    let requesting = {
        let store = store.clone();
        let id = m.campaign_id.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            store.request_campaign_assessment(&id, "last", true).is_ok()
        })
    };
    barrier.wait();
    assert_ne!(closing.join().unwrap(), requesting.join().unwrap());
}

#[tokio::test]
async fn cancellation_deadline_and_denied_funding_make_no_http() {
    let (_dir, store, mut m) = setup(true);
    let (model, endpoint, mut http, server) = provider(&m).await;
    let mut denied = policy(&m, &endpoint);
    denied.allowance.tokens = 101;
    assert!(store
        .host_authorize_service(&m.campaign_id, "oversight", denied, None)
        .is_err());
    let permit = store
        .host_authorize_service(&m.campaign_id, "oversight", policy(&m, &endpoint), None)
        .unwrap();
    let claim = store.observe_oversight(&m, true).unwrap().unwrap();
    let broker = ModelBroker::new(store.clone(), model);
    let tx = store.database.begin_write().unwrap();
    RuntimeStore::set_campaign_status_in(&tx, &m.campaign_id, CampaignStatus::Cancelling).unwrap();
    tx.commit().unwrap();
    assert!(broker
        .execute_service(
            &permit,
            &claim.request.request_id,
            &[ChatMessage::new(Role::User, "x")],
            &mut |_| {}
        )
        .await
        .is_err());
    m.deadline_ms = 0;
    assert!(store.observe_oversight(&m, true).is_err());
    assert!(tokio::time::timeout(Duration::from_millis(30), http.recv())
        .await
        .is_err());
    server.abort();
}

#[tokio::test]
async fn malformed_is_charged_and_late_cancelled_or_expired_results_never_publish() {
    for mode in 0..3 {
        let (_dir, store, m) = setup(true);
        let (model, endpoint, mut http, server) = provider(&m).await;
        let permit = store
            .host_authorize_service(&m.campaign_id, "oversight", policy(&m, &endpoint), None)
            .unwrap();
        let broker = Arc::new(ModelBroker::new(store.clone(), model));
        let (cancel, cancellation) = watch::channel(false);
        let (done, completion) = watch::channel(false);
        let owner = tokio::spawn(run(
            broker,
            m.clone(),
            permit,
            cancellation,
            completion,
            Instant::now()
                + if mode == 2 {
                    Duration::from_secs(1)
                } else {
                    Duration::from_secs(5)
                },
        ));
        let (_, reply) = tokio::time::timeout(Duration::from_secs(3), http.recv())
            .await
            .unwrap()
            .unwrap();
        if mode == 0 {
            reply
                .send((
                    VALID.replace("\"refs\":[]", "\"refs\":[\"invented\"]"),
                    true,
                ))
                .unwrap();
            done.send_replace(true);
            tokio::time::timeout(Duration::from_secs(3), owner)
                .await
                .unwrap()
                .unwrap();
        } else {
            if mode == 1 {
                cancel.send_replace(true);
            }
            tokio::time::timeout(Duration::from_secs(3), owner)
                .await
                .unwrap()
                .unwrap();
            reply.send((VALID.into(), true)).unwrap();
        }
        let ApiResponse::CampaignAssessments { records } =
            store.campaign_assessments(&m.campaign_id).unwrap()
        else {
            panic!()
        };
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].status,
            if mode == 0 {
                "invalid_or_failed"
            } else {
                "unknown"
            }
        );
        assert!(records[0].published.is_none());
        assert!(store
            .claim_assessment_delivery(tachyon_api::FOREGROUND_ID, 1)
            .unwrap()
            .is_none());
        let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
        let own = service_id(&m.campaign_id, "oversight");
        let usage: Vec<_> = ledger
            .reservations
            .values()
            .filter(|r| r.allocation.as_deref() == Some(&own))
            .collect();
        assert_eq!(usage.len(), 1);
        assert_eq!(
            usage[0].usage,
            if mode == 0 {
                Usage::Final(Units {
                    tokens: 3,
                    cost_micro_usd: 3,
                })
            } else {
                Usage::Unknown
            }
        );
        assert!(tokio::time::timeout(Duration::from_millis(30), http.recv())
            .await
            .is_err());
        server.abort();
    }
}

#[test]
fn manifest_absence_serialization_and_protected_root_bounds() {
    let (_dir, _store, mut m) = fixture();
    let old = serde_json::to_value(&m).unwrap();
    assert!(old.get("oversight").is_none());
    let mut enabled = old.clone();
    enabled["oversight"] =
        json!({"tokens":60,"cost_micro_usd":60,"max_assessments":2,"timeout_ms":1000});
    m = serde_json::from_value(enabled.clone()).unwrap();
    m.validate(crate::runtime_store::monitor::now_ms()).unwrap();
    for field in ["max_assessments", "timeout_ms", "tokens", "cost_micro_usd"] {
        let mut invalid = enabled.clone();
        invalid["oversight"][field] = 0.into();
        assert!(serde_json::from_value::<CampaignManifest>(invalid)
            .unwrap()
            .validate(crate::runtime_store::monitor::now_ms())
            .is_err());
    }
    enabled["oversight"]["tokens"] = 71.into();
    assert!(serde_json::from_value::<CampaignManifest>(enabled.clone())
        .unwrap()
        .validate(crate::runtime_store::monitor::now_ms())
        .is_err());
    enabled["oversight"]["grant"] = true.into();
    assert!(serde_json::from_value::<CampaignManifest>(enabled).is_err());
    m.oversight = None;
    assert_eq!(serde_json::to_value(&m).unwrap(), old);
}

#[test]
fn documented_complete_manifest_and_transactional_blocked_budget_triggers() {
    let docs = include_str!("../../../../../docs/tachyon/CAMPAIGN_OVERSIGHT.md");
    let example = docs
        .split("```json\n")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    let mut json: Value = serde_json::from_str(example).unwrap();
    json["deadline_ms"] = 10000.into();
    CampaignManifest::parse(&serde_json::to_vec(&json).unwrap(), 1).unwrap();
    let (_dir, store, m) = setup(false);
    let claim = store.observe_oversight(&m, true).unwrap().unwrap();
    store
        .finish_oversight(&m, &claim, Err("no provider in trigger fixture".into()))
        .unwrap();
    assert!(store.observe_oversight(&m, true).unwrap().is_none());
    let scope = TodoScope::Campaign {
        campaign_id: m.campaign_id.clone(),
    };
    let facade = store
        .todos(crate::runtime_store::todo::TodoAuthority::Bound {
            scope: scope.clone(),
            actor: TodoActor {
                source: "operator".into(),
                actor: "fixture".into(),
            },
        })
        .unwrap();
    let TodoResponse::Mutation { todo, .. } = facade
        .execute(TodoRequest::Add {
            scope: scope.clone(),
            command_id: "add".into(),
            expected_revision: 0,
            title: "task".into(),
            description: String::new(),
        })
        .unwrap()
    else {
        panic!()
    };
    // Ordinary plan edits fence old evidence but do not alone trigger a call.
    assert!(store.observe_oversight(&m, true).unwrap().is_none());
    facade
        .execute(TodoRequest::Update {
            scope,
            command_id: "block".into(),
            expected_revision: todo.revision,
            id: todo.id,
            title: None,
            description: None,
            status: Some(tachyon_api::todo::TodoStatus::Blocked),
        })
        .unwrap();
    store
        .campaign_ledger_command(
            "reserve-expense",
            &m.campaign_id,
            LedgerCommand::Reserve {
                reservation_id: "expense".into(),
                pool: Pool::Work,
                reserved: Units {
                    tokens: 30,
                    cost_micro_usd: 30,
                },
            },
        )
        .unwrap();
    store
        .campaign_ledger_command(
            "report-expense",
            &m.campaign_id,
            LedgerCommand::Reconcile {
                reservation_id: "expense".into(),
                usage: Usage::Final(Units {
                    tokens: 25,
                    cost_micro_usd: 25,
                }),
            },
        )
        .unwrap();
    let tx = store.database.begin_write().unwrap();
    let state = load(&tx, &m.campaign_id).unwrap();
    assert_eq!(
        state.pending,
        BTreeSet::from(["blocked_state".into(), "budget_threshold".into()])
    );
    assert_eq!(state.markers.budget, 1);
    drop(tx);
    let next = store.observe_oversight(&m, true).unwrap().unwrap();
    assert!(next.request.revision > claim.request.revision);
    assert_ne!(next.fence, claim.fence);
}
