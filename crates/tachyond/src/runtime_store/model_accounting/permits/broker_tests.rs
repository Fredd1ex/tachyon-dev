use super::*;
#[cfg(target_os = "linux")]
mod catalog_tests;
mod cpu_jobs_tests;
#[cfg(target_os = "linux")]
mod execution_tests;
mod private_tests;
#[cfg(target_os = "linux")]
mod subprocess_tests;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const VALID: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7,\"cost\":0.0000061}}\n\n";

async fn locked_writer(
    store: Arc<RuntimeStore>,
) -> (std::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
    let (release, wait) = std::sync::mpsc::channel();
    let (ready, started) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        let _write = store.database.begin_write().unwrap();
        ready.send(()).unwrap();
        // Watchdog lets a regression fail rather than hanging runtime shutdown.
        let _ = wait.recv_timeout(Duration::from_secs(30));
    });
    started.await.unwrap();
    (release, task)
}

#[tokio::test(flavor = "current_thread")]
async fn closed_launch_late_claim_stays_unknown_without_http() {
    let (_dir, store, funding, mut request) = tests::setup();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    request.estimate.base_url = format!("http://{}", listener.local_addr().unwrap());
    let store = Arc::new(store);
    let lease = PermitLease(Arc::new(false.into()));
    let permit = store
        .issue_model_permit(request.clone(), funding, None, lease.0.clone())
        .unwrap();
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&request)));
    let validated = Arc::new(tokio::sync::Notify::new());
    store.model_permits.lock().unwrap().claim_validated = Some(validated.clone());
    let (release, writer) = locked_writer(store.clone()).await;
    let task = tokio::spawn(async move {
        broker
            .execute(call(&permit, "late", &request), &mut |_| {})
            .await
    });
    tokio::time::timeout(Duration::from_secs(30), validated.notified())
        .await
        .unwrap();
    // Validation has passed, but the durable claim cannot finish until release.
    drop(lease);
    release.send(()).unwrap();
    writer.await.unwrap();
    assert!(task.await.unwrap().is_err());
    // Provider I/O belongs to the completed future, never the detached writer.
    assert!(matches!(
        listener.into_std().unwrap().accept(),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
    let read = store.database.begin_read().unwrap();
    let table = read.open_table(DISPATCHES).unwrap();
    let records: Vec<DispatchRecord> = table
        .iter()
        .unwrap()
        .map(|row| serde_json::from_slice(row.unwrap().1.value()).unwrap())
        .collect();
    assert_eq!(records.len(), 1);
    assert!(records[0].claimed);
    assert_eq!(
        store
            .campaign_ledger(&records[0].request.identity.campaign_id)
            .unwrap()
            .unwrap()
            .reservations[&records[0].receipt]
            .usage,
        Usage::Unknown
    );
}

pub(super) fn model(request: &RequestReservation) -> Model {
    Model::new(tachyon_model::ModelConfig {
        base_url: request.estimate.base_url.clone(),
        api_key: "localhost-fixture-key".into(),
        model: request.estimate.model.clone(),
        temperature: 0.0,
        max_completion_tokens: Some(request.estimate.output_tokens),
        context_length: None,
        parallel_tool_calls: true,
        reasoning: Default::default(),
        routing: None,
        debug: false,
        debug_log: None,
    })
}

fn call<'a>(
    permit: &'a ModelPermit,
    id: &'a str,
    request: &RequestReservation,
) -> ModelBrokerRequest<'a> {
    ModelBrokerRequest {
        permit,
        request_id: id,
        reservation: request.clone(),
        messages: &[],
        tools: None,
        streamed_argument: None,
        deadline: Instant::now() + Duration::from_secs(5),
    }
}

pub(super) async fn fixture(
    store: Arc<RuntimeStore>,
    mode: &'static str,
) -> (
    String,
    tokio::sync::mpsc::Receiver<serde_json::Value>,
    tokio::task::JoinHandle<()>,
) {
    fixture_with_barrier(store, mode, None).await
}

async fn fixture_with_barrier(
    store: Arc<RuntimeStore>,
    mode: &'static str,
    barrier: Option<std::net::SocketAddr>,
) -> (
    String,
    tokio::sync::mpsc::Receiver<serde_json::Value>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let redirect = url.clone();
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(socket.read_u8().await.unwrap());
                assert!(headers.len() < 16384);
            }
            let headers = String::from_utf8(headers).unwrap();
            assert!(headers.starts_with("POST /chat/completions HTTP/1.1\r\n"));
            assert!(headers.contains("Bearer localhost-fixture-key"));
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            assert!(length <= 32000);
            let mut bytes = vec![0; length];
            socket.read_exact(&mut bytes).await.unwrap();
            let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let db = store.clone();
            tokio::task::spawn_blocking(move || {
                // The actual socket is reachable only after a durable claim, and
                // neither authority nor the DB writer is held during HTTP.
                let _authority = db.model_permits.try_lock().unwrap();
                let write = db.database.begin_write().unwrap();
                let table = write.open_table(DISPATCHES).unwrap();
                assert!(table.iter().unwrap().any(|row| {
                    let (_, value) = row.unwrap();
                    let record: DispatchRecord = serde_json::from_slice(value.value()).unwrap();
                    assert!(record.claimed);
                    let ledger = RuntimeStore::campaign_ledger_in(
                        &write,
                        &record.request.identity.campaign_id,
                    )
                    .unwrap();
                    ledger.reservations[&record.receipt].usage == Usage::Unknown
                }));
            })
            .await
            .unwrap();
            tx.send(wire.clone()).await.unwrap();
            if mode == "lost" {
                continue;
            }
            if mode == "stall"
                || (mode == "artifact-repair-stall"
                    && wire["messages"]
                        .to_string()
                        .contains("Host verification rejected"))
            {
                std::future::pending::<()>().await;
            }
            let body = match mode {
                "services" | "services-python" if wire["messages"].as_array().unwrap().iter().all(|m| m["role"] != "tool") => {
                    let calls = if mode == "services-python" {
                        vec![serde_json::json!({"index":0,"id":"services","function":{"name":"ipython","arguments":serde_json::json!({"code":"import json\nt = require('todo')\nm = require('monitor')\nprint((await t.add(title='durable-plan-marker', command_id='local-model', expected_revision=0))['content'])\nprint((await m.snapshot())['content'])\nprint((await work.status())['content'])"}).to_string()}})]
                    } else {
                        [("todo", serde_json::json!({"action":"add","title":"durable-plan-marker","command_id":"local-model","expected_revision":0})), ("monitor", serde_json::json!({"action":"snapshot"})), ("work", serde_json::json!({"action":"status"}))].into_iter().enumerate().map(|(i, (name, input))| serde_json::json!({"index":i,"id":format!("service-{i}"),"function":{"name":name,"arguments":input.to_string()}})).collect()
                    };
                    format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"tool_calls":calls}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}}))
                }
                "services" | "services-python" => format!("{VALID}data: [DONE]\n\n"),
                "history" if wire["messages"].as_array().unwrap().iter().all(|m| m["role"] != "tool") => {
                    let actions = [
                        serde_json::json!({"action":"attempts","query":{"limit":8}}),
                        serde_json::json!({"action":"findings","query":{"limit":8}}),
                        serde_json::json!({"action":"attempts","query":{"limit":8},"campaign_id":"forged"}),
                    ];
                    let calls: Vec<_> = actions.iter().enumerate().map(|(i, a)| serde_json::json!({"index":i,"id":format!("history-{i}"),"function":{"name":"history","arguments":a.to_string()}})).collect();
                    format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"tool_calls":calls}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}}))
                }
                "history" => format!("{VALID}data: [DONE]\n\n"),
                "artifact-ready" | "artifact-stale" | "artifact-missing" | "artifact-repair" | "artifact-repair-stall" => {
                    let tools = wire["messages"].as_array().unwrap().iter().filter(|m| m["role"] == "tool").count();
                    let command = if mode == "artifact-repair" && wire["messages"].to_string().contains("Host verification rejected") { "printf corrected > result" } else { "printf original > result" };
                    let next = match tools {
                        0 => Some(("exec", serde_json::json!({"argv":["/bin/sh", "-c", command]}))),
                        1 if mode != "artifact-missing" => Some(("artifact", serde_json::json!({"path":"result", "kind":"file", "description":"candidate fixture"}))),
                        2 if mode == "artifact-stale" => Some(("exec", serde_json::json!({"argv":["/bin/sh", "-c", "printf modified > result"]}))),
                        _ => None,
                    };
                    if let Some((name, arguments)) = next {
                        format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":format!("artifact-step-{tools}"),"function":{"name":name,"arguments":arguments.to_string()}}]}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}}))
                    } else {
                        format!("{VALID}data: [DONE]\n\n")
                    }
                }
                "boundary-native" | "boundary-python" if wire["messages"].as_array().unwrap().iter().all(|m| m["role"] != "tool") => {
                    let barrier = barrier.expect("explicit tool barrier");
                    let command = format!("/bin/bash -c 'exec 3<>/dev/tcp/{}/{}; printf ready >&3; read -r release <&3; printf durable-stdout-marker'", barrier.ip(), barrier.port());
                    let (name, arguments) = if mode == "boundary-python" {
                        ("ipython", serde_json::json!({"code": format!("proc = require('exec')\nr = await proc.run(argv=['/bin/sh', '-c', {command:?}])\nprint(r)")}))
                    } else {
                        ("exec", serde_json::json!({"argv":["/bin/sh", "-c", command]}))
                    };
                    format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"boundary-tool","function":{"name":name,"arguments":arguments.to_string()}}]}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}}))
                }
                "boundary-native" | "boundary-python" => format!("{VALID}data: [DONE]\n\n"),
                "agents" if wire["messages"].as_array().unwrap().iter().all(|m| m["role"] != "tool") => {
                    let actions = [
                        serde_json::json!({"action":"status","work_id":"child"}),
                        serde_json::json!({"action":"list","limit":32}),
                        serde_json::json!({"action":"send","work_id":"child","command_id":"ghost-send","text":"hello child"}),
                        serde_json::json!({"action":"result","work_id":"child"}),
                        serde_json::json!({"action":"status","work_id":"stranger"}),
                    ];
                    let calls: Vec<_> = actions.iter().enumerate().map(|(i, a)| serde_json::json!({"index":i,"id":format!("agent-{i}"),"function":{"name":"agents","arguments":a.to_string()}})).collect();
                    format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"tool_calls":calls}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}}))
                }
                "agents" => format!("{VALID}data: [DONE]\n\n"),
                "spool" if wire["messages"].as_array().unwrap().iter().all(|m| m["role"] != "tool") => {
                    let arguments = serde_json::json!({"argv":["/bin/sh","-c","head -c 1200000 /dev/zero | tr '\\0' A; head -c 1200000 /dev/zero | tr '\\0' B"]});
                    format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"spool-exec","function":{"name":"exec","arguments":arguments.to_string()}}]}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}}))
                }
                "spool" if wire["messages"].as_array().unwrap().iter().filter(|m| m["role"] == "tool").count() == 1 => {
                    format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"spool-ctx","function":{"name":"ctx","arguments":"{\"action\":\"list\"}"}}]}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}}))
                }
                "spool" => format!("{VALID}data: [DONE]\n\n"),
                "subprocess" if wire["messages"].as_array().unwrap().iter().all(|m| m["role"] != "tool") =>
                    "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"env-1\",\"function\":{\"name\":\"exec\",\"arguments\":\"{\\\"argv\\\":[\\\"/usr/bin/env\\\"]}\"}}]}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7,\"cost\":0.0000061}}\n\ndata: [DONE]\n\n".into(),
                "subprocess" => format!("{VALID}data: [DONE]\n\n"),
                "loop" if wire["messages"].as_array().unwrap().iter().all(|m| m["role"] != "tool") =>
                    "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"ls-1\",\"function\":{\"name\":\"ls\",\"arguments\":\"{\\\"path\\\":\\\".\\\"}\"}}]}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7,\"cost\":0.0000061}}\n\ndata: [DONE]\n\n".into(),
                "loop" => format!("{VALID}data: [DONE]\n\n"),
                "valid" => format!("{VALID}data: [DONE]\n\n"),
                "partial" => VALID.into(),
                "oversized" => "x".repeat(1024 * 1024 + 1),
                "sparse" => "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":18446744073709551615}]}}]}\n\ndata: [DONE]\n\n".into(),
                "malformed" => "data: {\"usage\":{}}\n\ndata: [DONE]\n\n".into(),
                "redirect" => String::new(),
                _ => panic!("unknown fixture"),
            };
            let status = if mode == "redirect" {
                "307 Temporary Redirect"
            } else {
                "200 OK"
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nLocation: {redirect}/chat/completions\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (url, rx, task)
}

#[tokio::test]
async fn broker_valid_denial_stale_retry_and_persisted_reconciliation() {
    let (dir, store, funding, mut request) = tests::setup();
    let store = Arc::new(store);
    let (url, mut wire, server) = fixture(store.clone(), "valid").await;
    request.estimate.base_url = url;
    let permit = store
        .host_issue_model_permit(request.clone(), funding.clone(), None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let mut sink = |_: &str| {};
    for field in 0..4 {
        let mut denied = call(&permit, "denied", &request);
        match field {
            0 => denied.reservation.identity.attempt_id.push('x'),
            1 => denied.reservation.estimate.input_tokens -= 1,
            2 => denied.reservation.estimate.model.push('x'),
            _ => denied.deadline = Instant::now(),
        }
        assert!(broker.execute(denied, &mut sink).await.is_err());
    }
    assert!(wire.try_recv().is_err());
    assert!(store
        .campaign_ledger(&request.identity.campaign_id)
        .unwrap()
        .unwrap()
        .allocations
        .is_empty());
    let tools = [ToolSpec::new(
        "answer",
        "answer",
        serde_json::json!({"type":"object"}),
    )];
    for id in ["one", "two"] {
        let mut invocation = call(&permit, id, &request);
        if id == "two" {
            invocation.tools = Some(&tools);
            invocation.streamed_argument = Some(("answer", "text"));
        }
        broker.execute(invocation, &mut sink).await.unwrap();
        let body = wire.recv().await.unwrap();
        assert_eq!(body["model"], "fake");
        assert_eq!(body["max_tokens"], 10);
        assert!(body.get("max_completion_tokens").is_none());
        assert_eq!(
            body["provider"],
            serde_json::json!({"only":["fake"],"allow_fallbacks":false,"require_parameters":true})
        );
        assert_eq!(body["stream_options"]["include_usage"], true);
        if id == "two" {
            assert_eq!(body["tool_choice"], "required");
        }
        let serialized = body.to_string();
        for secret in [
            permit.0.to_string(),
            request.identity.campaign_id.clone(),
            "localhost-fixture-key".into(),
        ] {
            assert!(!serialized.contains(&secret));
        }
        assert!(broker
            .execute(call(&permit, id, &request), &mut sink)
            .await
            .is_err());
    }
    let replacement = store
        .host_issue_model_permit(request.clone(), funding.clone(), Some(&permit))
        .unwrap();
    assert!(broker
        .execute(call(&permit, "stale", &request), &mut sink)
        .await
        .is_err());
    store.host_revoke_model_permit(&replacement).unwrap();
    assert!(broker
        .execute(call(&replacement, "revoked", &request), &mut sink)
        .await
        .is_err());
    assert!(wire.try_recv().is_err());
    let ledger = store
        .campaign_ledger(&request.identity.campaign_id)
        .unwrap()
        .unwrap();
    assert_eq!(ledger.allocations.len(), 1);
    assert_eq!(
        ledger.allocation_available(&funding.dispatch_id).unwrap(),
        Units {
            tokens: 86,
            cost_micro_usd: 86
        }
    );
    assert_eq!(ledger.active_inferences(), 0);
    server.abort();
    let _ = server.await;
    drop(broker);
    drop(store);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    assert_eq!(
        store
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap()
            .unwrap(),
        ledger
    );
    let read = store.database.begin_read().unwrap();
    for row in read.open_table(REQUESTS).unwrap().iter().unwrap() {
        let (_, bytes) = row.unwrap();
        let record: Record = serde_json::from_slice(bytes.value()).unwrap();
        assert_eq!(
            record.usage,
            RequestUsage::Final {
                input_tokens: 5,
                output_tokens: 2,
                cost_micro_usd: 7
            }
        );
        assert_eq!(
            record.allocation_id.as_deref(),
            Some(funding.dispatch_id.as_str())
        );
    }
}

#[tokio::test]
async fn broker_unknown_http_outcomes_deadlines_and_reopen_fence() {
    for (mode, required_tool) in [
        ("partial", false),
        ("malformed", false),
        ("oversized", false),
        ("sparse", false),
        ("redirect", false),
        ("lost", false),
        ("stall", false),
        ("stall", true),
    ] {
        let (dir, store, funding, mut request) = tests::setup();
        let store = Arc::new(store);
        let (url, mut wire, server) = fixture(store.clone(), mode).await;
        request.estimate.base_url = url;
        let permit = store
            .host_issue_model_permit(request.clone(), funding.clone(), None)
            .unwrap();
        let broker = ModelBroker::new(store.clone(), model(&request));
        let tools = [ToolSpec::new(
            "answer",
            "answer",
            serde_json::json!({"type":"object"}),
        )];
        let mut invocation = call(&permit, "uncertain", &request);
        invocation.deadline = Instant::now() + Duration::from_millis(500);
        // Required-tool calls use the identical mandatory deadline path.
        if required_tool {
            invocation.tools = Some(&tools);
            invocation.streamed_argument = Some(("answer", "text"));
        }
        let result = broker.execute(invocation, &mut |_: &str| {}).await;
        if mode != "partial" {
            assert!(result.is_err(), "{mode}");
        }
        if mode == "stall" {
            assert!(
                matches!(result, Err(ModelError::Accounting(message)) if message.contains("deadline"))
            );
        }
        wire.recv().await.unwrap();
        assert!(broker
            .execute(call(&permit, "uncertain", &request), &mut |_: &str| {})
            .await
            .is_err());
        assert!(wire.try_recv().is_err());
        let ledger = store
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap()
            .unwrap();
        assert_eq!(ledger.active_inferences(), 1);
        assert_eq!(
            ledger.allocation_available(&funding.dispatch_id).unwrap(),
            Units {
                tokens: 70,
                cost_micro_usd: 70
            }
        );
        server.abort();
        let _ = server.await;
        drop(broker);
        drop(store);
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        assert_eq!(
            store
                .campaign_ledger(&request.identity.campaign_id)
                .unwrap()
                .unwrap(),
            ledger
        );
        let broker = ModelBroker::new(store.clone(), model(&request));
        assert!(broker
            .execute(call(&permit, "new", &request), &mut |_: &str| {})
            .await
            .is_err());
        let fresh = store
            .host_issue_model_permit(request.clone(), funding, None)
            .unwrap();
        assert!(broker
            .execute(call(&fresh, "uncertain", &request), &mut |_: &str| {})
            .await
            .is_err());
        let read = store.database.begin_read().unwrap();
        for row in read.open_table(REQUESTS).unwrap().iter().unwrap() {
            let (_, bytes) = row.unwrap();
            let record: Record = serde_json::from_slice(bytes.value()).unwrap();
            assert_eq!(record.usage, RequestUsage::Unknown);
        }
    }
}

#[tokio::test]
async fn broker_deadline_during_blocked_reservation_never_starts_http() {
    let (_dir, store, funding, mut request) = tests::setup();
    let store = Arc::new(store);
    let (url, mut wire, server) = fixture(store.clone(), "valid").await;
    request.estimate.base_url = url;
    let permit = store
        .host_issue_model_permit(request.clone(), funding.clone(), None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let (release, wait) = std::sync::mpsc::channel();
    let (locked, acquired) = tokio::sync::oneshot::channel();
    let db = store.clone();
    let writer = std::thread::spawn(move || {
        let _write = db.database.begin_write().unwrap();
        locked.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    acquired.await.unwrap();
    let mut invocation = call(&permit, "blocked", &request);
    invocation.deadline = Instant::now() + Duration::from_millis(500);
    let mut sink = |_: &str| {};
    let execution = broker.execute(invocation, &mut sink);
    let observe_claim = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if store.model_permits.try_lock().is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    };
    // On a single-thread executor the timer and observer must still run while
    // the blocking reservation owns authority and waits for the redb writer.
    let (result, ()) = tokio::join!(execution, observe_claim);
    assert!(matches!(result, Err(ModelError::Accounting(message)) if message.contains("deadline")));
    release.send(()).unwrap();
    writer.join().unwrap();
    let db = store.clone();
    tokio::task::spawn_blocking(move || {
        // Wait for the non-cancellable commit, not merely its caller's timeout.
        let _authority = db.model_permits.lock().unwrap();
        let ledger = db
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap()
            .unwrap();
        assert_eq!(ledger.active_inferences(), 1);
        assert_eq!(
            ledger
                .allocation_available(&funding.dispatch_id)
                .unwrap()
                .tokens,
            70
        );
    })
    .await
    .unwrap();
    assert!(wire.try_recv().is_err());
    server.abort();
    let _ = server.await;
}
