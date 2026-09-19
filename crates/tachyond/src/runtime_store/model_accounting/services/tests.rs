use super::*;
use crate::runtime_store::campaign_ledger::Envelope;
use tachyon_api::types::{ApiRequest, ApiResponse};
use tachyon_model::{Model, ModelConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn units(n: u64) -> Units {
    Units {
        tokens: n,
        cost_micro_usd: n,
    }
}

fn setup() -> (tempfile::TempDir, Arc<RuntimeStore>, String, ServicePolicy) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let ApiResponse::Research { research } = store
        .research_request(&ApiRequest::ResearchCreate {
            command_id: "research".into(),
            title: "test".into(),
            objective: "test".into(),
        })
        .unwrap()
    else {
        panic!()
    };
    let ApiResponse::Campaign { campaign } = store
        .research_request(&ApiRequest::CampaignCreate {
            command_id: "campaign".into(),
            research_id: research.id,
            title: "test".into(),
            objective: "test".into(),
        })
        .unwrap()
    else {
        panic!()
    };
    store
        .host_authorize_campaign_envelope(
            "envelope",
            &campaign.id,
            Envelope {
                work: units(100),
                verification: units(100),
                max_active_inferences: 4,
            },
        )
        .unwrap();
    let policy = ServicePolicy {
        purpose: ServicePurpose::CampaignOversight,
        allowance: units(60),
        max_requests: 2,
        timeout_ms: 3000,
        estimate: RequestEstimate {
            base_url: "http://127.0.0.1:1".into(),
            model: "fake".into(),
            provider: "fake".into(),
            pricing_revision: "fixed-1".into(),
            max_request_bytes: 10000,
            input_tokens: 20,
            output_tokens: 10,
            input_micro_usd_per_million: 1_000_000,
            output_micro_usd_per_million: 1_000_000,
            other_micro_usd: 0,
        },
    };
    (dir, store, campaign.id, policy)
}

fn model(policy: &ServicePolicy) -> Model {
    Model::new(ModelConfig {
        base_url: policy.estimate.base_url.clone(),
        api_key: "local-only".into(),
        model: policy.estimate.model.clone(),
        temperature: 0.0,
        max_completion_tokens: Some(policy.estimate.output_tokens),
        context_length: None,
        parallel_tool_calls: false,
        reasoning: Default::default(),
        routing: None,
        debug: false,
        debug_log: None,
    })
}

fn claim(
    store: &RuntimeStore,
    permit: &ServicePermit,
    campaign: &str,
    policy: &ServicePolicy,
    id: &str,
) -> tachyon_model::Result<String> {
    store.claim_service(
        permit.nonce,
        &permit.allocation_id,
        id,
        &reservation(campaign, &permit.allocation_id, id, policy),
        Instant::now() + Duration::from_secs(5),
    )
}

fn final_usage(
    store: &RuntimeStore,
    permit: &ServicePermit,
    campaign: &str,
    policy: &ServicePolicy,
    id: &str,
    receipt: &str,
    n: u64,
) {
    DaemonAccounting {
        store,
        authorized: reservation(campaign, &permit.allocation_id, id, policy),
    }
    .reconcile_funded_sync(
        receipt,
        RequestUsage::Final {
            input_tokens: n,
            output_tokens: 0,
            cost_micro_usd: n,
        },
        Some(&permit.allocation_id),
    )
    .unwrap();
}

#[test]
fn service_exhaustion_is_not_campaign_exhaustion_or_a_new_grant() {
    let (_dir, store, campaign, policy) = setup();
    let permit = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .unwrap();
    let scope = tachyon_api::todo::TodoScope::Campaign {
        campaign_id: campaign.clone(),
    };
    for request in ["first", "second"] {
        let receipt = claim(&store, &permit, &campaign, &policy, request).unwrap();
        final_usage(&store, &permit, &campaign, &policy, request, &receipt, 30);
        assert!(store
            .attention_snapshot(&scope, None, 10)
            .unwrap()
            .records
            .is_empty());
    }
    let before = store.campaign_ledger(&campaign).unwrap().unwrap();
    assert_eq!(
        before.allocation_available(&permit.allocation_id).unwrap(),
        units(0)
    );
    assert!(claim(&store, &permit, &campaign, &policy, "third").is_err());
    assert_eq!(store.campaign_ledger(&campaign).unwrap().unwrap(), before);
    assert!(store
        .attention_snapshot(&scope, None, 10)
        .unwrap()
        .records
        .is_empty());
}

#[test]
fn work_and_service_identity_conflicts_roll_back_in_both_orders() {
    for service_first in [false, true] {
        let (_dir, store, campaign, policy) = setup();
        let id = service_id(&campaign, "oversight");
        let admission = crate::runtime_store::admission::Admission {
            work_id: id.clone(),
            campaign_id: campaign.clone(),
            objective: "fixture".into(),
            generation: 1,
            instruction_revision: 1,
            pool: Pool::Work,
            upper_bound: units(1),
        };
        if service_first {
            let _permit = store
                .host_authorize_service(&campaign, "oversight", policy, None)
                .unwrap();
            let before = store.campaign_ledger(&campaign).unwrap();
            assert!(store.admit_campaign_work(admission).is_err());
            assert_eq!(store.campaign_ledger(&campaign).unwrap(), before);
            assert!(store.admitted_work(&campaign, &id).is_err());
        } else {
            store.admit_campaign_work(admission).unwrap();
            let before = store.campaign_ledger(&campaign).unwrap();
            assert!(store
                .host_authorize_service(&campaign, "oversight", policy, None)
                .is_err());
            assert_eq!(store.campaign_ledger(&campaign).unwrap(), before);
        }
    }
}

#[test]
fn immutable_funding_reopen_counts_and_no_work() {
    let (dir, store, campaign, policy) = setup();
    let permit = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .unwrap();
    let ledger = store.campaign_ledger(&campaign).unwrap().unwrap();
    assert_eq!(ledger.committed(Pool::Work).unwrap().tokens, 60);
    assert_eq!(ledger.committed(Pool::Verification).unwrap().tokens, 0);
    assert_eq!(ledger.active_inferences(), 0);
    assert!(store
        .host_authorize_service(&campaign, "another", policy.clone(), None)
        .is_err());
    assert_eq!(store.campaign_ledger(&campaign).unwrap().unwrap(), ledger);
    assert!(store
        .admitted_work(&campaign, &permit.allocation_id)
        .is_err());
    assert!(store.list_tasks().unwrap().is_empty());
    assert_eq!(
        store.host_capacity.resident.available(),
        store.host_capacity.limits.max_resident_workers
    );
    assert_eq!(
        store.host_capacity.execution.available(),
        store.host_capacity.limits.max_execution_jobs
    );
    let receipt = claim(&store, &permit, &campaign, &policy, "first").unwrap();
    assert!(claim(&store, &permit, &campaign, &policy, "first").is_err());
    assert!(claim(&store, &permit, &campaign, &policy, "second").is_err());
    final_usage(&store, &permit, &campaign, &policy, "first", &receipt, 7);
    permit.revoke();
    assert!(claim(&store, &permit, &campaign, &policy, "second").is_err());
    drop(store);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    assert!(claim(&store, &permit, &campaign, &policy, "second").is_err());
    for field in 0..7 {
        let mut changed = policy.clone();
        match field {
            0 => changed.allowance = units(61),
            1 => changed.max_requests += 1,
            2 => changed.timeout_ms += 1,
            3 => changed.estimate.model.push('x'),
            4 => changed.estimate.pricing_revision.push('x'),
            5 => changed.estimate.input_tokens += 1,
            _ => changed.estimate.output_tokens += 1,
        }
        assert!(store
            .host_authorize_service(&campaign, "oversight", changed, None)
            .is_err());
    }
    let permit = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .unwrap();
    assert!(claim(&store, &permit, &campaign, &policy, "first").is_err());
    let receipt = claim(&store, &permit, &campaign, &policy, "second").unwrap();
    final_usage(&store, &permit, &campaign, &policy, "second", &receipt, 7);
    assert!(claim(&store, &permit, &campaign, &policy, "third").is_err());
    let ledger = store.campaign_ledger(&campaign).unwrap().unwrap();
    assert_eq!(
        ledger.allocation_available(&permit.allocation_id).unwrap(),
        units(46)
    );
    assert_eq!(ledger.committed(Pool::Work).unwrap().tokens, 60);
    assert_eq!(ledger.envelope.verification, units(100));
}

#[test]
fn aggregate_exhaustion_and_authoritative_attempt() {
    let (_dir, store, campaign, mut policy) = setup();
    policy.max_requests = 10;
    let permit = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .unwrap();
    for field in 0..4 {
        let mut request = reservation(&campaign, &permit.allocation_id, "id", &policy);
        match field {
            0 => request.identity.class = RequestClass::Verification,
            1 => request.identity.attempt_id = "forged".into(),
            2 => request.identity.work_id = "forged".into(),
            _ => request.estimate.input_tokens = 1,
        }
        assert!(store
            .claim_service(
                permit.nonce,
                &permit.allocation_id,
                "id",
                &request,
                Instant::now() + Duration::from_secs(1)
            )
            .is_err());
    }
    for id in ["one", "two"] {
        let receipt = claim(&store, &permit, &campaign, &policy, id).unwrap();
        final_usage(&store, &permit, &campaign, &policy, id, &receipt, 30);
    }
    assert!(claim(&store, &permit, &campaign, &policy, "three").is_err());
    let ledger = store.campaign_ledger(&campaign).unwrap().unwrap();
    assert_eq!(
        ledger.allocation_available(&permit.allocation_id).unwrap(),
        units(0)
    );
    assert_eq!(ledger.committed(Pool::Work).unwrap().tokens, 60);
    assert_eq!(ledger.committed(Pool::Verification).unwrap().tokens, 0);
    // Remaining root funds remain spendable, but cannot refill the service grant.
    store
        .campaign_ledger_command(
            "remaining-root",
            &campaign,
            LedgerCommand::Reserve {
                reservation_id: "root".into(),
                pool: Pool::Work,
                reserved: units(40),
            },
        )
        .unwrap();
    assert!(claim(&store, &permit, &campaign, &policy, "three").is_err());
}

#[test]
fn concurrent_claims_and_replacement_have_one_authority() {
    let (_dir, store, campaign, policy) = setup();
    let permit = Arc::new(
        store
            .host_authorize_service(&campaign, "oversight", policy.clone(), None)
            .unwrap(),
    );
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let (store, permit, campaign, policy, barrier) = (
                store.clone(),
                permit.clone(),
                campaign.clone(),
                policy.clone(),
                barrier.clone(),
            );
            std::thread::spawn(move || {
                barrier.wait();
                claim(&store, &permit, &campaign, &policy, "same")
            })
        })
        .collect();
    let receipts: Vec<_> = threads
        .into_iter()
        .filter_map(|t| t.join().unwrap().ok())
        .collect();
    assert_eq!(receipts.len(), 1);
    assert!(store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .is_err());
    let replacement = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), Some(&permit))
        .unwrap();
    assert!(claim(&store, &permit, &campaign, &policy, "next").is_err());
    // Revocation does not discard billing evidence from the old authority.
    final_usage(&store, &permit, &campaign, &policy, "same", &receipts[0], 7);
    assert!(claim(&store, &replacement, &campaign, &policy, "same").is_err());
    claim(&store, &replacement, &campaign, &policy, "next").unwrap();
}

#[tokio::test]
async fn timeout_preserves_claim_and_drop_revokes_lease() {
    let (_dir, store, campaign, mut policy) = setup();
    let (url, mut seen, server) = provider(store.clone(), campaign.clone(), "stall").await;
    policy.estimate.base_url = url;
    policy.timeout_ms = 300;
    let permit = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&policy));
    assert!(broker
        .execute_service(&permit, "timeout", &[], &mut |_| {})
        .await
        .is_err());
    seen.try_recv().unwrap();
    let nonce = permit.nonce;
    let id = permit.allocation_id.clone();
    drop(permit);
    assert!(*store.model_permits.lock().unwrap().services.current[&id]
        .cancelled
        .borrow());
    assert!(store
        .claim_service(
            nonce,
            &id,
            "next",
            &reservation(&campaign, &id, "next", &policy),
            Instant::now() + Duration::from_secs(1)
        )
        .is_err());
    let ledger = store.campaign_ledger(&campaign).unwrap().unwrap();
    assert_eq!(ledger.active_inferences(), 1);
    assert_eq!(ledger.allocation_available(&id).unwrap(), units(30));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_storage_deadline_never_starts_http() {
    let (_dir, store, campaign, mut policy) = setup();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    policy.estimate.base_url = format!("http://{}", listener.local_addr().unwrap());
    policy.timeout_ms = 100;
    let permit = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&policy));
    let (release, wait) = std::sync::mpsc::channel();
    let (ready, started) = tokio::sync::oneshot::channel();
    let db = store.clone();
    let writer = tokio::task::spawn_blocking(move || {
        let _write = db.database.begin_write().unwrap();
        ready.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    started.await.unwrap();
    assert!(broker
        .execute_service(&permit, "blocked", &[], &mut |_| {})
        .await
        .is_err());
    permit.revoke();
    release.send(()).unwrap();
    writer.await.unwrap();
    let db = store.clone();
    let id = permit.allocation_id.clone();
    tokio::task::spawn_blocking(move || {
        let _authority = db.model_permits.lock().unwrap();
        let write = db.database.begin_write().unwrap();
        let record = load(&write, &id).unwrap();
        let ledger = RuntimeStore::campaign_ledger_in(&write, &campaign).unwrap();
        assert!(record.requests.len() <= 1);
        for receipt in record.requests.values() {
            assert_eq!(ledger.reservations[receipt].usage, Usage::Unknown);
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(listener.into_std().unwrap().accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
    );
}

/// A real local HTTP provider checks that claims are durable and no transaction
/// or authority lock crosses the network boundary.
async fn provider(
    store: Arc<RuntimeStore>,
    campaign: String,
    mode: &'static str,
) -> (
    String,
    tokio::sync::mpsc::Receiver<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(socket.read_u8().await.unwrap());
            }
            let headers = String::from_utf8(headers).unwrap();
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            let mut bytes = vec![0; length];
            socket.read_exact(&mut bytes).await.unwrap();
            let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(wire["model"], "fake");
            assert!(wire
                .get("tools")
                .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty)));
            let (db, campaign) = (store.clone(), campaign.clone());
            tokio::task::spawn_blocking(move || {
                let _authority = db.model_permits.try_lock().unwrap();
                let write = db.database.begin_write().unwrap();
                let ledger = RuntimeStore::campaign_ledger_in(&write, &campaign).unwrap();
                assert_eq!(ledger.active_inferences(), 1);
                assert_eq!(
                    db.host_capacity.resident.available(),
                    db.host_capacity.limits.max_resident_workers
                );
                assert_eq!(
                    db.host_capacity.execution.available(),
                    db.host_capacity.limits.max_execution_jobs
                );
                let record = load(&write, &service_id(&campaign, "oversight")).unwrap();
                assert!(!record.requests.is_empty());
            })
            .await
            .unwrap();
            tx.send(()).await.unwrap();
            if mode == "stall" {
                std::future::pending::<()>().await;
            }
            let usage = if mode == "final" {
                ",\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7,\"cost\":0.000007}"
            } else {
                ""
            };
            let body = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"ok\"}}}}]{usage}}}\n\ndata: [DONE]\n\n");
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
    });
    (url, rx, task)
}

#[tokio::test]
async fn http_final_unknown_and_denials_before_dispatch() {
    for mode in ["final", "unknown"] {
        let (_dir, store, campaign, mut policy) = setup();
        let (url, mut seen, server) = provider(store.clone(), campaign.clone(), mode).await;
        policy.estimate.base_url = url;
        let permit = store
            .host_authorize_service(&campaign, "oversight", policy.clone(), None)
            .unwrap();
        let mut wrong = policy.clone();
        wrong.estimate.model = "wrong".into();
        let wrong_broker = ModelBroker::new(store.clone(), model(&wrong));
        assert!(wrong_broker
            .execute_service(&permit, "wrong", &[], &mut |_| {})
            .await
            .is_err());
        assert!(seen.try_recv().is_err());
        let broker = ModelBroker::new(store.clone(), model(&policy));
        assert!(broker
            .execute_service(&permit, "one", &[], &mut |_| {})
            .await
            .is_ok());
        seen.recv().await.unwrap();
        assert!(broker
            .execute_service(&permit, "one", &[], &mut |_| {})
            .await
            .is_err());
        if mode == "final" {
            assert!(broker
                .execute_service(&permit, "two", &[], &mut |_| {})
                .await
                .is_ok());
            seen.recv().await.unwrap();
        } else {
            assert!(broker
                .execute_service(&permit, "two", &[], &mut |_| {})
                .await
                .is_err());
        }
        let ledger = store.campaign_ledger(&campaign).unwrap().unwrap();
        assert_eq!(
            ledger.allocation_available(&permit.allocation_id).unwrap(),
            units(if mode == "final" { 46 } else { 30 })
        );
        assert_eq!(ledger.active_inferences(), usize::from(mode != "final"));
        assert!(broker
            .execute_service(&permit, "three", &[], &mut |_| {})
            .await
            .is_err());
        assert!(seen.try_recv().is_err());
        server.abort();
    }
}

#[tokio::test]
async fn cancellation_interrupts_http_and_unknown_survives_reauthorization() {
    let (dir, store, campaign, mut policy) = setup();
    let (url, mut seen, server) = provider(store.clone(), campaign.clone(), "stall").await;
    policy.estimate.base_url = url;
    let permit = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&policy));
    {
        let mut sink = |_: &str| {};
        let call = broker.execute_service(&permit, "one", &[], &mut sink);
        tokio::pin!(call);
        tokio::select! { _ = &mut call => panic!("unexpected completion"), _ = seen.recv() => {} }
        permit.revoke();
        assert!(call.await.is_err());
    }
    let replacement = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), Some(&permit))
        .unwrap();
    assert!(claim(&store, &replacement, &campaign, &policy, "one").is_err());
    assert!(claim(&store, &replacement, &campaign, &policy, "two").is_err());
    server.abort();
    let _ = server.await;
    drop(broker);
    drop(store);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    let permit = store
        .host_authorize_service(&campaign, "oversight", policy.clone(), None)
        .unwrap();
    assert!(claim(&store, &permit, &campaign, &policy, "two").is_err());
    let write = store.database.begin_write().unwrap();
    assert_eq!(
        load(&write, &permit.allocation_id).unwrap().requests.len(),
        1
    );
    let ledger = RuntimeStore::campaign_ledger_in(&write, &campaign).unwrap();
    assert_eq!(
        ledger.allocation_available(&permit.allocation_id).unwrap(),
        units(30)
    );
}
