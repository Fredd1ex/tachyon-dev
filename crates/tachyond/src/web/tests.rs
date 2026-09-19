use super::*;
use serde_json::{json, Value};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    sync::Mutex,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn command(id: &str, turn: &str, fetch: bool) -> WebCommand {
    WebCommand {
        command_id: id.into(),
        request_id: id.into(),
        tool_call_id: id.into(),
        caller_id: tachyon_api::FOREGROUND_ID.into(),
        turn_id: turn.into(),
        request: if fetch {
            WebRequest::Fetch {
                urls: vec!["https://example.com/paper.pdf".into()],
                instruction: None,
                follow_links: None,
            }
        } else {
            WebRequest::Search {
                query: "rust".into(),
                domains: None,
                max_results: 3,
            }
        },
    }
}

fn service(
    mode: &'static str,
) -> (
    tempfile::TempDir,
    Arc<WebService>,
    tokio::sync::mpsc::UnboundedReceiver<Value>,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let model = Model::new(ModelConfig {
        base_url: format!("http://{}", listener.local_addr().unwrap()),
        api_key: "local-web-secret".into(),
        model: "fixture".into(),
        temperature: 0.0,
        max_completion_tokens: Some(2048),
        context_length: None,
        parallel_tool_calls: true,
        reasoning: Default::default(),
        routing: None,
        debug: false,
        debug_log: None,
    });
    let service =
        Arc::new(WebService::new(store, model, "fixture".into(), WebPolicy::default()).unwrap());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let _enter = service.runtime.enter();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    service.runtime.spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut headers = vec![];
                while !headers.ends_with(b"\r\n\r\n") { headers.push(socket.read_u8().await.unwrap()); assert!(headers.len() < 16384); }
                let headers = String::from_utf8(headers).unwrap();
                assert!(headers.to_ascii_lowercase().contains("authorization: bearer local-web-secret"));
                let len: usize = headers.lines().find_map(|l| { let (k,v) = l.split_once(':')?; k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse().unwrap()) }).unwrap();
                let mut bytes = vec![0; len]; socket.read_exact(&mut bytes).await.unwrap();
                let wire: Value = serde_json::from_slice(&bytes).unwrap();
                if mode == "inference" {
                    tx.send(wire).unwrap();
                    let body = format!("data: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"delta":{"content":"local-web-secret","tool_calls":[{"index":0,"id":"local-web-secret","function":{"name":"local-web-secret","arguments":"local-web-secret"}}]},"finish_reason":"local-web-secret"}]}));
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                    return;
                }
                if mode == "worker" && wire["tools"][0]["type"] == "function" {
                    let tools = wire["tools"].as_array().unwrap();
                    let search = tools.iter().find(|t| t["function"]["name"] == "websearch").expect("native web tool");
                    assert_eq!(search["function"]["parameters"]["required"], json!(["query"]));
                    assert!(search["function"]["parameters"]["properties"].get("kind").is_none());
                    let final_step = wire["messages"].as_array().unwrap().iter().any(|m| m["role"] == "tool");
                    let cancel = wire["messages"].as_array().unwrap().iter().any(|m| m["role"] == "user" && m["content"].as_str().is_some_and(|s| s.contains("cancel pending retrieval")));
                    tx.send(wire).unwrap();
                    let delta = if final_step { json!({"content":"assignment complete"}) } else {
                        json!({"tool_calls":[{"index":0,"id":"lookup","type":"function","function":{"name":"websearch","arguments":json!({"query":if cancel { "wait-for-cancel" } else { "fixture current facts" }}).to_string()}}]})
                    };
                    let body = format!("data: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"delta":delta,"finish_reason":if final_step {"stop"} else {"tool_calls"}}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}));
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                    return;
                }
                let fetch = wire["tools"][0]["type"] == "openrouter:web_fetch";
                let wait = mode == "worker" && wire["messages"][1]["content"].as_str().is_some_and(|s| s.contains("wait-for-cancel"));
                tx.send(wire).unwrap();
                if mode == "ratelimit" {
                    socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                    return;
                }
                if mode == "rejected" {
                    let body = json!({"error":{"code":400,"message":"local-web-secret Bearer other-secret private prompt"}}).to_string();
                    socket.write_all(format!("HTTP/1.1 400 Bad Request\r\nX-Request-Id: fixture-request-400\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                    return;
                }
                if mode == "hang" || wait { std::future::pending::<()>().await; }
                let mut usage = json!({"prompt_tokens":10,"completion_tokens":2,"total_tokens":12,"cost":0.007001,
                    "server_tool_use": if fetch { json!({"web_fetch_requests":1}) } else { json!({"web_search_requests":1}) }});
                if mode == "unknown" { usage.as_object_mut().unwrap().remove("cost"); }
                let body = format!("data: {}\n\ndata: {}\n\n{}", json!({"choices":[{"delta":{"content":"local-web-secret report","annotations":[{"type":"url_citation","url_citation":{"url":"https://example.com/paper.pdf","title":"Paper"}}]},"finish_reason":"stop"}]}), json!({"usage":usage}), match mode { "incomplete" => "", "stream_error" => "data: {\"error\":{\"message\":\"private error body\"}}\n\n", _ => "data: [DONE]\n\n" });
                let _ = socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
            });
        }
    });
    drop(_enter);
    (dir, service, rx)
}

#[test]
fn public_same_user_route_search_fetch_citations_replay_without_worker() {
    let (_dir, service, mut wire) = service("final");
    let registry = Arc::new(Mutex::new(crate::Registry {
        web: Some(service.clone()),
        foreground_id: Some("foreground-process".into()),
        ..Default::default()
    }));
    registry.lock().unwrap().tasks.insert(
        "foreground-process".into(),
        crate::tests::task("foreground-process", tachyon_api::AgentState::Running),
    );
    let (mut client, server) = UnixStream::pair().unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let mut reader = BufReader::new(client.try_clone().unwrap());
    let reg = registry.clone();
    let handler = std::thread::spawn(move || crate::handle_connection(server, reg).unwrap());
    let mut receipts = std::collections::BTreeMap::new();
    for (id, fetch) in [("search", false), ("fetch", true), ("search", false)] {
        let command = command(id, "turn", fetch);
        let mut metadata =
            tachyon_api::InteractionMetadata::new(id, "turn", tachyon_api::FOREGROUND_ID, 1);
        metadata.turn_id = Some("turn".into());
        let request = tachyon_api::ApiRequest::ConversationWeb {
            metadata,
            command: command.clone(),
        };
        assert!(matches!(
            crate::dispatch(&request, &registry),
            tachyon_api::ApiResponse::Error { .. }
        ));
        writeln!(client, "{}", serde_json::to_string(&request).unwrap()).unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let tachyon_api::ApiResponse::ConversationWeb {
            command: echoed,
            result,
        } = serde_json::from_str(&line).unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(echoed, command);
        let report = result.unwrap();
        assert!(report
            .usage
            .receipt_id
            .as_ref()
            .is_some_and(|id| id.len() == 64));
        assert_eq!(report.usage.input_tokens, Some(10));
        assert_eq!(report.usage.output_tokens, Some(2));
        assert_eq!(report.usage.cost_micro_usd, Some(7001));
        if let Some(previous) = receipts.insert(id, report.usage.clone()) {
            assert_eq!(previous, report.usage);
        }
        assert_eq!(report.citations[0].title.as_deref(), Some("Paper"));
        assert!(!report.answer.contains("local-web-secret"));
    }
    for fetch in [false, true] {
        let wire = wire.try_recv().unwrap();
        assert!(wire["provider"].get("only").is_none());
        assert_eq!(wire["tools"][0]["parameters"]["engine"], "exa");
        assert_eq!(wire["tools"][0]["parameters"]["max_uses"], 1);
        assert_eq!(wire["max_tool_calls"], 1);
        assert_eq!(
            wire["tools"][0]["type"],
            if fetch {
                "openrouter:web_fetch"
            } else {
                "openrouter:web_search"
            }
        );
    }
    assert!(wire.try_recv().is_err());
    assert_eq!(registry.lock().unwrap().tasks.len(), 1);
    assert!(registry.lock().unwrap().campaigns.is_none());
    for fault in 0..4 {
        let mut command = command("invalid", "new-root", false);
        let mut metadata =
            tachyon_api::InteractionMetadata::new("invalid", "new-root", "foreground", 1);
        metadata.turn_id = Some("new-root".into());
        match fault {
            0 => metadata.protocol_version = 99,
            1 => metadata.conversation_id = "other".into(),
            2 => metadata.turn_id = Some("different".into()),
            _ => command.caller_id = "worker".into(),
        }
        writeln!(
            client,
            "{}",
            serde_json::to_string(&tachyon_api::ApiRequest::ConversationWeb { metadata, command })
                .unwrap()
        )
        .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(matches!(
            serde_json::from_str::<tachyon_api::ApiResponse>(&line).unwrap(),
            tachyon_api::ApiResponse::ConversationWeb { result: Err(_), .. }
        ));
    }
    assert!(wire.try_recv().is_err());
    drop(reader);
    drop(client);
    handler.join().unwrap();
}

#[test]
fn public_sessions_isolate_reused_turns_but_share_replay_holds_and_limits() {
    for limit in ["unknown", "requests", "server-calls", "tokens", "cost"] {
        let (_dir, mut service, mut wire) = service(if limit == "unknown" {
            "unknown"
        } else {
            "final"
        });
        let provider = Arc::get_mut(&mut Arc::get_mut(&mut service).unwrap().provider).unwrap();
        match limit {
            "requests" => provider.policy.max_requests = 2,
            "server-calls" => provider.policy.max_server_calls = 2,
            "tokens" => provider.policy.turn_tokens = 67584 + 12,
            "cost" => provider.policy.turn_cost_micro_usd = 250000 + 7001,
            _ => {}
        }
        // Pre-upgrade records have no session identity. Never clear or reassign
        // their unresolved claims to whichever session happens to start next.
        let legacy = serde_json::to_string(&("conversation", "foreground", "4")).unwrap();
        service
            .store
            .web_reserve(
                &legacy,
                "fixture",
                &service.policy,
                &command("legacy", "4", false),
                1,
            )
            .unwrap();
        let registry = Arc::new(Mutex::new(crate::Registry {
            web: Some(service.clone()),
            foreground_id: Some("foreground".into()),
            ..Default::default()
        }));
        registry.lock().unwrap().tasks.insert(
            "foreground".into(),
            crate::tests::task("foreground", tachyon_api::AgentState::Running),
        );
        let (mut client, server) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let reg = registry.clone();
        let handler = std::thread::spawn(move || crate::handle_connection(server, reg).unwrap());
        let mut receipts = std::collections::BTreeMap::new();
        for session in ["session-a", "session-b", "session-a"] {
            // Only the host registry selects the session, not RPC metadata or
            // the model's command/request/tool-call IDs (identical across sessions).
            registry
                .lock()
                .unwrap()
                .tasks
                .get_mut("foreground")
                .unwrap()
                .info
                .session_id = session.into();
            for (index, id) in ["one", "one", "two", "three"].into_iter().enumerate() {
                let mut metadata = tachyon_api::InteractionMetadata::new(
                    format!("metadata-{session}-{index}"),
                    "correlation",
                    "foreground",
                    1,
                );
                metadata.turn_id = Some("4".into());
                let cmd = command(id, "4", id == "two");
                writeln!(
                    client,
                    "{}",
                    serde_json::to_string(&tachyon_api::ApiRequest::ConversationWeb {
                        metadata,
                        command: cmd.clone(),
                    })
                    .unwrap()
                )
                .unwrap();
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let tachyon_api::ApiResponse::ConversationWeb {
                    command: echoed,
                    result,
                } = serde_json::from_str(&line).unwrap()
                else {
                    panic!("wrong response")
                };
                assert_eq!(echoed, cmd);
                if id == "three" || (limit == "unknown" && id == "two") {
                    let error = result.unwrap_err();
                    assert!(
                        error.contains(if limit == "unknown" {
                            "unresolved spend"
                        } else {
                            "allowance"
                        }),
                        "{limit}: {error}"
                    );
                } else {
                    let report = result.unwrap();
                    if let Some(previous) = receipts.insert((session, id), report.clone()) {
                        assert_eq!(
                            serde_json::to_value(previous).unwrap(),
                            serde_json::to_value(report).unwrap()
                        );
                    }
                }
            }
        }
        assert_ne!(
            receipts[&("session-a", "one")].usage.receipt_id,
            receipts[&("session-b", "one")].usage.receipt_id
        );
        for _ in 0..if limit == "unknown" { 2 } else { 4 } {
            assert!(wire.try_recv().is_ok());
        }
        assert!(wire.try_recv().is_err());
        assert!(service
            .store
            .web_reserve(
                &legacy,
                "fixture",
                &service.policy,
                &command("legacy", "4", false),
                1,
            )
            .unwrap_err()
            .contains("no redispatch"));
        assert!(service
            .runtime
            .block_on(service.lookup("", "foreground", command("empty", "4", false)))
            .is_err());
        drop(reader);
        drop(client);
        handler.join().unwrap();
    }
}

#[test]
fn worker_channel_redacts_completion_and_revokes_replaced_actor() {
    for mode in ["inference", "final"] {
        let (_dir, service, mut wire) = service(mode);
        let registry = Arc::new(Mutex::new(crate::Registry::default()));
        let mut task = crate::tests::task("worker", tachyon_api::AgentState::Running);
        task.info.pid = Some(std::process::id());
        task.generation = 1;
        task.assignment = 1;
        registry.lock().unwrap().tasks.insert("worker".into(), task);
        let binding = WorkerBinding {
            registry: Arc::downgrade(&registry),
            worker: "worker".into(),
            actor_pid: std::process::id(),
            request: tachyon_api::WorkRequest {
                work_id: "still-valid-work".into(),
                objective: "fixture".into(),
                generation: 1,
                assignment: 1,
                context_refs: vec![],
                constraints: None,
                attempt: None,
                deadline_ms: crate::unix_now_ms() + 10000,
                lifetime_class: tachyon_api::LifetimeClass::Short,
            },
        };
        assert!(service
            .assignment(
                Arc::downgrade(&registry),
                "worker".into(),
                std::process::id() + 1,
                binding.request.clone(),
            )
            .is_err());
        service.runtime.block_on(async {
            let (channel, client) = tachyon_model::broker::private_pair().unwrap();
            let host = service.serve_channel(
                channel,
                "still-valid-work",
                "root",
                Instant::now() + Duration::from_secs(5),
                Some(&binding),
            );
            let guest = async {
                if mode == "inference" {
                    let result = client
                        .chat(
                            &[tachyon_model::ChatMessage::new(
                                tachyon_model::Role::User,
                                "fixture",
                            )],
                            &[],
                        )
                        .await
                        .unwrap();
                    assert_eq!(result.finish_reason.as_deref(), Some("[REDACTED]"));
                    assert!(!serde_json::to_string(&result)
                        .unwrap()
                        .contains("local-web-secret"));
                } else {
                    let mut cmd = command("one", "forged-root", false);
                    cmd.caller_id = "still-valid-work".into();
                    assert!(client.web_lookup(cmd.clone()).await.is_err());
                    assert!(wire.try_recv().is_err());
                    cmd.turn_id = "root".into();
                    assert!(client.web_lookup(cmd.clone()).await.is_ok());
                    assert!(client.web_lookup(cmd).await.is_ok());
                }
                wire.recv().await.unwrap();
                // Keep work/generation/assignment valid; only the actor changes.
                registry
                    .lock()
                    .unwrap()
                    .tasks
                    .get_mut("worker")
                    .unwrap()
                    .info
                    .pid = Some(std::process::id() + 1);
                let mut cmd = command("after-restart", "root", false);
                cmd.caller_id = "still-valid-work".into();
                assert!(client.web_lookup(cmd).await.is_err());
                assert!(wire.try_recv().is_err());
            };
            let (_, _) = tokio::join!(host, guest);
        });
    }
}

#[test]
fn actual_warm_ghost_assignment_services_without_credentials_or_browser() {
    let Ok(binary) = std::env::var("GHOST_TEST_BIN") else {
        eprintln!("GHOST_TEST_BIN not set; optional actual Ghost fixture skipped");
        return;
    };
    let (_dir, service, mut wire) = service("worker");
    let workspace = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(binary)
        .args(["--chat", "--agent-id", "warm-fixture", "--cwd"])
        .arg(workspace.path())
        .env_clear()
        .env("HOME", workspace.path())
        .env("XDG_CONFIG_HOME", workspace.path())
        .env("PATH", "/nonexistent")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let stdout = child.stdout.take().unwrap();
    let stdin = Arc::new(Mutex::new(child.stdin.take().unwrap()));
    let mut child = Kill(child);
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if tx.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let registry = Arc::new(Mutex::new(crate::Registry {
        web: Some(service.clone()),
        ..Default::default()
    }));
    let mut task = crate::tests::task("warm-fixture", tachyon_api::AgentState::Running);
    task.stdin = Some(stdin.clone());
    task.info.pid = Some(child.0.id());
    task.generation = 1;
    task.assignment = 1;
    registry
        .lock()
        .unwrap()
        .tasks
        .insert("warm-fixture".into(), task);
    let mut receipts = std::collections::BTreeSet::new();
    let mut requests = vec![];
    for assignment in 1..=5 {
        {
            let mut reg = registry.lock().unwrap();
            let task = reg.tasks.get_mut("warm-fixture").unwrap();
            task.assignment = assignment;
            task.info.state = tachyon_api::AgentState::Running;
        }
        let request = tachyon_api::WorkRequest {
            work_id: "same-work".into(),
            objective: if assignment == 4 {
                "cancel pending retrieval"
            } else {
                "Look up fixture facts"
            }
            .into(),
            generation: 1,
            assignment,
            context_refs: vec![],
            constraints: None,
            attempt: None,
            deadline_ms: crate::unix_now_ms() + 15000,
            lifetime_class: tachyon_api::LifetimeClass::Short,
        };
        if assignment == 2 {
            // Host binds generation/assignment before delivery; changed guest context
            // must not mint another scope or issue a provider retrieval.
            let bootstrap = service
                .assignment(
                    Arc::downgrade(&registry),
                    "warm-fixture".into(),
                    child.0.id(),
                    request.clone(),
                )
                .unwrap();
            let mut forged = request.clone();
            forged.generation = 99;
            writeln!(
                stdin.lock().unwrap(),
                "{}",
                serde_json::to_string(&tachyon_api::web::WorkerAssignment {
                    work: forged,
                    host_service: Some(bootstrap),
                    context_only: false
                })
                .unwrap()
            )
            .unwrap();
        } else {
            crate::deliver_work(&registry, "warm-fixture", &request, false).unwrap();
        }
        if assignment == 4 {
            service.runtime.block_on(async {
                loop {
                    let next = tokio::time::timeout(Duration::from_secs(5), wire.recv())
                        .await
                        .unwrap()
                        .unwrap();
                    let waiting = next["tools"][0]["type"] == "openrouter:web_search"
                        && next["messages"][1]["content"]
                            .as_str()
                            .is_some_and(|s| s.contains("wait-for-cancel"));
                    requests.push(next);
                    if waiting {
                        break;
                    }
                }
            });
            registry
                .lock()
                .unwrap()
                .tasks
                .get_mut("warm-fixture")
                .unwrap()
                .info
                .state = tachyon_api::AgentState::Interrupted;
        }
        loop {
            let line = rx.recv_timeout(std::time::Duration::from_secs(20)).unwrap();
            assert!(!line.contains("local-web-secret"));
            let Ok(event) = serde_json::from_str::<tachyon_api::EventEnvelope>(&line) else {
                continue;
            };
            let tachyon_api::AgentEvent::WorkCandidate { candidate } = event.kind else {
                continue;
            };
            assert_eq!(candidate.work_id, "same-work");
            assert_eq!(candidate.assignment, assignment);
            assert_eq!(
                matches!(
                    candidate.outcome,
                    tachyon_api::WorkOutcome::Completed { .. }
                ),
                assignment != 4,
                "{candidate:?}"
            );
            let evidence = candidate
                .evidence
                .tools
                .iter()
                .find(|t| t.tool_name == "websearch")
                .unwrap();
            assert_eq!(evidence.output["is_error"], matches!(assignment, 2 | 4));
            if !matches!(assignment, 2 | 4) {
                let report: WebResult =
                    serde_json::from_str(evidence.output["content"].as_str().unwrap()).unwrap();
                assert_eq!(report.usage.cost_micro_usd, Some(7001));
                assert!(receipts.insert(report.usage.receipt_id.unwrap()));
            }
            assert!(candidate
                .evidence
                .tools
                .iter()
                .all(|t| !matches!(t.tool_name.as_str(), "browser" | "exec" | "python")));
            break;
        }
        if assignment == 4 {
            use sha2::{Digest, Sha256};
            assert_eq!(service.capacity.available_permits(), 4);
            let root = format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&(
                        "same-work",
                        Option::<String>::None,
                        Option::<String>::None,
                        Some(1u64),
                        Some(4u64)
                    ))
                    .unwrap()
                )
            );
            let mut retry = command("after-cancel", &root, false);
            retry.caller_id = "same-work".into();
            assert!(service
                .runtime
                .block_on(service.lookup_bound("same-work", retry, None, None))
                .unwrap_err()
                .contains("unresolved"));
        }
    }
    // Plain daemon agent input also gets a bound service, but keeps its legacy
    // completion protocol rather than inventing a durable Work record.
    crate::deliver_task(&registry, "warm-fixture", "Look up fixture facts", false).unwrap();
    loop {
        let line = rx.recv_timeout(std::time::Duration::from_secs(20)).unwrap();
        assert!(!line.contains("local-web-secret"));
        if serde_json::from_str::<tachyon_api::EventEnvelope>(&line).is_ok_and(|event| {
            matches!(event.kind, tachyon_api::AgentEvent::WorkerCompleted { .. })
        }) {
            break;
        }
    }
    while let Ok(request) = wire.try_recv() {
        requests.push(request);
    }
    assert_eq!(requests.len(), 16); // Eleven inference calls, four final lookups and one cancelled lookup.
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["tools"][0]["type"] == "openrouter:web_search")
            .count(),
        5
    );
    assert_eq!(receipts.len(), 3);
    assert!(!workspace.path().join(".tachyon/browser").exists());
    registry.lock().unwrap().tasks.clear();
    drop(stdin);
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            std::time::Instant::now() < until,
            "Ghost did not close after the final assignment and EOF"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    reader.join().unwrap();
    assert!(registry.lock().unwrap().works.is_empty());
}

#[test]
fn private_failure_preserves_diagnostic_and_replay_without_another_request() {
    let (_dir, service, mut wire) = service("rejected");
    service.runtime.block_on(async {
        let (channel, client) = tachyon_model::broker::private_pair().unwrap();
        let host = service.serve_private(
            channel,
            "foreground",
            "root",
            Instant::now() + Duration::from_secs(5),
        );
        let guest = async {
            let command = command("rejected", "root", false);
            for _ in 0..2 {
                let error = client
                    .web_lookup(command.clone())
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(error.contains("web retrieval failed: provider HTTP 400; request id fixture-request-400; spend may be unknown"), "{error}");
                assert!(!error.contains("local-web-secret"));
                assert!(!error.contains("other-secret"));
                assert!(!error.contains("private prompt"));
            }
            drop(client);
        };
        let _ = tokio::join!(host, guest);
    });
    assert!(wire.try_recv().is_ok());
    assert!(wire.try_recv().is_err());
}

#[test]
fn service_unknown_and_incomplete_block_root_but_not_other_turns() {
    for mode in ["unknown", "incomplete", "stream_error", "ratelimit"] {
        let (_dir, service, mut wire) = service(mode);
        service.runtime.block_on(async {
            let first = service
                .lookup("session", "foreground", command("one", "root", false))
                .await;
            if mode != "ratelimit" {
                let report = first.unwrap();
                assert_eq!(
                    report.status,
                    if mode == "unknown" {
                        WebStatus::Unverified
                    } else {
                        WebStatus::Partial
                    }
                );
                assert_eq!(report.answer, "[REDACTED] report");
                assert_eq!(report.citations[0].title.as_deref(), Some("Paper"));
                assert_eq!(report.usage.input_tokens, Some(10));
                assert_eq!(report.usage.output_tokens, Some(2));
                assert_eq!(report.usage.cost_micro_usd, None);
                assert!(report.usage.receipt_id.is_some());
                let replay = service
                    .lookup("session", "foreground", command("one", "root", false))
                    .await
                    .unwrap();
                assert_eq!(report.usage, replay.usage);
                assert_eq!(
                    serde_json::to_value(&report).unwrap(),
                    serde_json::to_value(&replay).unwrap()
                );
            } else {
                assert!(first.is_err());
                assert!(service
                    .lookup("session", "foreground", command("one", "root", false))
                    .await
                    .is_err());
            }
            assert!(service
                .lookup("session", "foreground", command("two", "root", true))
                .await
                .is_err());
            let _ = service
                .lookup("session", "foreground", command("three", "fresh", true))
                .await;
        });
        assert!(wire.try_recv().is_ok());
        assert!(wire.try_recv().is_ok());
        assert!(wire.try_recv().is_err());
    }
}

#[test]
fn public_disconnect_cancels_http_and_retains_claim() {
    let (_dir, service, mut wire) = service("hang");
    let registry = Arc::new(Mutex::new(crate::Registry {
        web: Some(service.clone()),
        foreground_id: Some("foreground-process".into()),
        ..Default::default()
    }));
    registry.lock().unwrap().tasks.insert(
        "foreground-process".into(),
        crate::tests::task("foreground-process", tachyon_api::AgentState::Running),
    );
    let (mut client, server) = UnixStream::pair().unwrap();
    let handler = std::thread::spawn(move || {
        let _ = crate::handle_connection(server, registry);
    });
    let mut metadata = tachyon_api::InteractionMetadata::new("one", "root", "foreground", 1);
    metadata.turn_id = Some("root".into());
    writeln!(
        client,
        "{}",
        serde_json::to_string(&tachyon_api::ApiRequest::ConversationWeb {
            metadata,
            command: command("one", "root", false)
        })
        .unwrap()
    )
    .unwrap();
    service.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), wire.recv())
            .await
            .unwrap()
            .unwrap();
    });
    drop(client);
    handler.join().unwrap();
    assert!(service
        .runtime
        .block_on(service.lookup(
            "foreground-process",
            "foreground",
            command("two", "root", true)
        ))
        .is_err());
    assert!(wire.try_recv().is_err());
}

#[test]
fn standalone_private_channel_binds_source_and_root_without_campaign() {
    let (_dir, service, mut wire) = service("final");
    service.runtime.block_on(async {
        let (channel, client) = tachyon_model::broker::private_pair().unwrap();
        let host = service.serve_private(
            channel,
            "memory",
            "root",
            Instant::now() + Duration::from_secs(5),
        );
        let guest = async {
            assert!(client
                .web_lookup(command("forged", "root", false))
                .await
                .is_err());
            let mut cmd = command("search", "root", false);
            cmd.caller_id = "memory".into();
            assert_eq!(
                client
                    .web_lookup(cmd.clone())
                    .await
                    .unwrap()
                    .citations
                    .len(),
                1
            );
            assert!(client.web_lookup(cmd).await.is_ok());
            let mut wrong = command("wrong-turn", "new-root", false);
            wrong.caller_id = "memory".into();
            assert!(client.web_lookup(wrong).await.is_err());
            drop(client);
        };
        let (_, _) = tokio::join!(host, guest);
    });
    assert!(wire.try_recv().is_ok());
    assert!(wire.try_recv().is_err());
}

#[test]
fn independent_turns_share_bounded_capacity_and_cancellation_keeps_holds() {
    let (_dir, service, mut wire) = service("hang");
    service.runtime.block_on(async {
        let mut tasks = vec![];
        for index in 0..4 {
            let service = service.clone();
            tasks.push(tokio::spawn(async move {
                service
                    .lookup(
                        "session",
                        "foreground",
                        command(&format!("call-{index}"), &format!("root-{index}"), false),
                    )
                    .await
            }));
        }
        for _ in 0..4 {
            tokio::time::timeout(Duration::from_secs(5), wire.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            service
                .lookup("session", "foreground", command("fifth", "fresh", false))
                .await
                .unwrap_err(),
            "web service busy"
        );
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        assert!(service
            .lookup("session", "foreground", command("repeat", "root-0", false))
            .await
            .is_err());
        assert!(wire.try_recv().is_err());
    });
}
