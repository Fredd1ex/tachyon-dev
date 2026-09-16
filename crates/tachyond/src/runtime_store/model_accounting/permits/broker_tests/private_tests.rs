use super::*;

#[tokio::test]
async fn private_spool_upload_rejects_tampering_and_retains_disconnect() {
    use sha2::{Digest, Sha256};
    use tachyon_model::broker::{ResourceUpload as U, UploadReply};
    for mode in [
        "ready",
        "tamper",
        "disconnect",
        "deadline",
        "revoked",
        "quota",
    ] {
        let (_dir, mut store, funding, request) = tests::setup();
        if mode == "quota" {
            store.trace_limits.campaign = 1;
        }
        let store = Arc::new(store);
        let permit = store
            .host_issue_model_permit(request.clone(), funding, None)
            .unwrap();
        let broker = ModelBroker::new(store.clone(), model(&request));
        let (host, client) = tachyon_model::broker::private_pair().unwrap();
        let worker_store = store.clone();
        let nonce = permit.0;
        let worker = async move {
            let digest = format!("{:x}", Sha256::digest(b"test"));
            let reply = client
                .resource_upload(U::Begin {
                    handle: format!("output:{}", uuid::Uuid::new_v4()),
                    retained: 4,
                    total: 4,
                    storage_failed: false,
                    sha256: digest.clone(),
                })
                .await
                .unwrap();
            if mode == "quota" {
                assert!(matches!(reply, UploadReply::Denied));
                return;
            }
            assert!(matches!(reply, UploadReply::Accepted { offset: 0 }));
            if mode == "disconnect" {
                return;
            }
            if mode == "deadline" {
                tokio::time::sleep(Duration::from_millis(400)).await;
                return;
            }
            if mode == "revoked" {
                worker_store
                    .model_permits
                    .lock()
                    .unwrap()
                    .grants
                    .get_mut(&nonce)
                    .unwrap()
                    .active = false;
            }
            let reply = client
                .resource_upload(U::Chunk {
                    offset: 0,
                    bytes: if mode == "tamper" {
                        b"bad!".to_vec()
                    } else {
                        b"test".to_vec()
                    },
                })
                .await
                .unwrap();
            if mode == "revoked" {
                assert!(matches!(reply, UploadReply::Denied));
                return;
            }
            assert!(matches!(reply, UploadReply::Accepted { offset: 4 }));
            let reply = client
                .resource_upload(U::Finish {
                    sha256: Some(digest),
                })
                .await
                .unwrap();
            assert_eq!(matches!(reply, UploadReply::Ready { .. }), mode == "ready");
        };
        let (served, ()) = tokio::join!(
            broker.serve_private(
                host,
                &permit,
                request,
                Instant::now() + Duration::from_millis(if mode == "deadline" { 200 } else { 5000 })
            ),
            worker
        );
        assert!(served.is_err()); // EOF or deadline, never reconnect.
        if mode != "ready" && mode != "quota" {
            // Detached cancellation records a gap, but does not delete evidence.
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let store = store.clone();
                    let retained = tokio::task::spawn_blocking(move || {
                        use crate::runtime_store::research_context::traces::TRACES;
                        let tx = store.database.begin_read().unwrap();
                        let table = tx.open_table(TRACES).unwrap();
                        for row in table.iter().unwrap() {
                            let (_, value) = row.unwrap();
                            let resource: tachyon_api::context::Resource =
                                serde_json::from_slice(value.value()).unwrap();
                            if resource.data["retention_state"] == "gap" {
                                assert_eq!(resource.data["size_bytes"], 4);
                                assert_eq!(
                                    std::fs::read_dir(&store.trace_root).unwrap().count(),
                                    1
                                );
                                return true;
                            }
                        }
                        false
                    })
                    .await
                    .unwrap();
                    if retained {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
    }
}

#[tokio::test]
async fn private_deadline_bounds_idle_handshake_without_side_effects() {
    let (_dir, store, funding, request) = tests::setup();
    let store = Arc::new(store);
    let permit = store
        .host_issue_model_permit(request.clone(), funding, None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let before = store
        .campaign_ledger(&request.identity.campaign_id)
        .unwrap();
    let (host, _idle_client) = tachyon_model::broker::private_pair().unwrap();
    let served = tokio::time::timeout(
        Duration::from_secs(2),
        broker.serve_private(
            host,
            &permit,
            request.clone(),
            Instant::now() + Duration::from_millis(20),
        ),
    )
    .await
    .expect("handshake must obey the host deadline");
    assert!(served.is_err());
    assert_eq!(
        store
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn transport_replay_never_dispatches_twice() {
    let (_dir, store, funding, mut request) = tests::setup();
    let store = Arc::new(store);
    let (url, mut wire, http) = fixture(store.clone(), "valid").await;
    request.estimate.base_url = url;
    let permit = store
        .host_issue_model_permit(request.clone(), funding, None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let (host, client) = tachyon_model::broker::private_pair().unwrap();
    let worker = async {
        let mut call = tachyon_model::broker::Request {
            context: None,
            id: "same-logical-id".into(),
            messages: vec![],
            tools: vec![],
        };
        client.request(&call).await.unwrap();
        wire.recv().await.unwrap();
        call.messages.push(ChatMessage::new(
            tachyon_model::Role::System,
            "different payload",
        ));
        assert!(client.request(&call).await.is_err());
        assert!(wire.try_recv().is_err());
    };
    let (served, ()) = tokio::join!(
        broker.serve_private(
            host,
            &permit,
            request.clone(),
            Instant::now() + Duration::from_secs(5)
        ),
        worker
    );
    assert!(served.is_err());
    let ledger = store
        .campaign_ledger(&request.identity.campaign_id)
        .unwrap()
        .unwrap();
    assert_eq!(ledger.reservations.len(), 2);
    assert_eq!(ledger.active_inferences(), 0);
    http.abort();
}

#[tokio::test]
async fn ghost_loop_uses_real_accounting_and_tools() {
    use ghost::harness::{
        agent::{run_loop, AgentLoopEvent, AgentLoopEventSink},
        runtime::*,
    };
    struct Events;
    impl AgentLoopEventSink for Events {
        fn emit(&self, _: AgentLoopEvent) {}
    }
    let (dir, store, funding, mut request) = tests::setup();
    let store = Arc::new(store);
    let (url, mut wire, http) = fixture(store.clone(), "loop").await;
    request.estimate.base_url = url;
    request.estimate.max_request_bytes = 32000;
    let permit = store
        .host_issue_model_permit(request.clone(), funding.clone(), None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let (host, client) = tachyon_model::broker::private_pair().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let mut policy = ToolPolicy::worker_default(root.clone());
    policy.enabled_tools.retain(|name| name == "ls");
    let context = ToolContext {
        workspace_root: root.clone(),
        cwd: root,
        identity: ToolIdentity::default(),
        deadline: std::time::Instant::now() + Duration::from_secs(5),
        cancellation: tokio_util::sync::CancellationToken::new(),
        policy: Arc::new(policy),
        event_sink: Arc::new(NoopEventSink),
        output_store: Arc::new(NoopOutputStore),
        host_service: None,
    };
    let worker = async {
        let mut messages = vec![ChatMessage::new(tachyon_model::Role::User, "list files")];
        let result = run_loop(
            &client,
            &mut messages,
            &native_registry(),
            &context,
            3,
            &Events,
        )
        .await
        .unwrap();
        assert_eq!(result.0, "ok");
        assert_eq!(result.1.total_tokens, 14);
        assert!(messages.iter().any(|m| m.role == tachyon_model::Role::Tool));
        drop(client);
    };
    let (served, ()) = tokio::join!(
        broker.serve_private(
            host,
            &permit,
            request.clone(),
            Instant::now() + Duration::from_secs(5)
        ),
        worker
    );
    assert!(served.is_err());
    let first = wire.recv().await.unwrap();
    let second = wire.recv().await.unwrap();
    assert_eq!(first["tools"].as_array().unwrap().len(), 1);
    assert!(second["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["role"] == "tool"));
    let ledger = store
        .campaign_ledger(&request.identity.campaign_id)
        .unwrap()
        .unwrap();
    assert_eq!(ledger.reservations.len(), 3); // Admission hold plus two model requests.
    assert_eq!(ledger.active_inferences(), 0);
    assert_eq!(
        ledger
            .allocation_available(&funding.dispatch_id)
            .unwrap()
            .tokens,
        86
    );
    http.abort();
}

#[tokio::test]
async fn scope_denials_and_disconnect() {
    for mode in [
        "revoked",
        "stale",
        "scope",
        "deadline",
        "messages",
        "bytes",
        "disconnect",
    ] {
        let (_dir, store, funding, mut request) = tests::setup();
        let store = Arc::new(store);
        let (url, mut wire, http) = fixture(store.clone(), "stall").await;
        request.estimate.base_url = url;
        let permit = store
            .host_issue_model_permit(request.clone(), funding.clone(), None)
            .unwrap();
        let broker = ModelBroker::new(store.clone(), model(&request));
        if mode == "revoked" {
            store.host_revoke_model_permit(&permit).unwrap();
        }
        if mode == "stale" {
            store
                .host_issue_model_permit(request.clone(), funding, Some(&permit))
                .unwrap();
        }
        let mut bound = request.clone();
        if mode == "scope" {
            bound.identity.attempt_id.push('x');
        }
        let deadline = Instant::now()
            + if mode == "deadline" {
                Duration::ZERO
            } else {
                Duration::from_secs(5)
            };
        let (host, client) = tachyon_model::broker::private_pair().unwrap();
        let worker = async {
            if mode == "disconnect" {
                let call = client.chat(&[], &[]);
                tokio::pin!(call);
                tokio::select! {
                    _ = &mut call => panic!("stalled request returned"),
                    seen = wire.recv() => { seen.unwrap(); }
                }
            } else {
                let messages = match mode {
                    "messages" => vec![ChatMessage::new(tachyon_model::Role::User, ""); 257],
                    "bytes" => vec![ChatMessage::new(
                        tachyon_model::Role::User,
                        "x".repeat(1024 * 1024),
                    )],
                    _ => vec![],
                };
                assert!(client.chat(&messages, &[]).await.is_err());
                assert!(wire.try_recv().is_err());
            }
        };
        let (served, ()) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(broker.serve_private(host, &permit, bound, deadline), worker)
        })
        .await
        .expect("disconnect must not wait for host deadline");
        assert!(served.is_err());
        let ledger = store
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            ledger.reservations.len(),
            1 + usize::from(mode == "disconnect")
        );
        // Before dispatch the admission itself occupies the slot; after transfer
        // the unknown model request does. Neither failure refunds that slot.
        assert_eq!(ledger.active_inferences(), 1);
        assert_eq!(ledger.allocations.len(), usize::from(mode == "disconnect"));
        if mode == "disconnect" {
            let read = store.database.begin_read().unwrap();
            for row in read.open_table(REQUESTS).unwrap().iter().unwrap() {
                let (_, bytes) = row.unwrap();
                let record: Record = serde_json::from_slice(bytes.value()).unwrap();
                assert_eq!(record.usage, RequestUsage::Unknown);
            }
        }
        http.abort();
    }
}
