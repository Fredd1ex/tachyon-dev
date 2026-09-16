use super::*;
use serde_json::json;

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN and IPython; localhost HTTP only"]
async fn actual_ghost_core_ask_cli_answer_retains_python_and_complete_is_a_proposal() {
    assert!(std::env::var_os("GHOST_TEST_BIN").is_some());
    for python in [false, true] {
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
        let evaluated = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = evaluated.clone();
        root.evaluate =
            crate::runtime_store::scheduler::Evaluator::Callback(Arc::new(move |candidate| {
                assert_eq!(candidate.outcome.completed_result(), Some("retained 42"));
                assert_eq!(candidate.instruction_revision, Some(1));
                assert_eq!(
                    candidate
                        .evidence
                        .tools
                        .iter()
                        .filter(|t| t.output["is_error"] == true)
                        .count(),
                    2,
                    "completion must preserve failed sibling tools and failed Python cells"
                );
                // Worker references remain proposals; the existing host collector
                // rebuilds these from registered artifact evidence.
                assert_eq!(candidate.candidate_refs, None);
                if let tachyon_api::types::WorkOutcome::Completed { context, .. } =
                    &candidate.outcome
                {
                    assert!(
                        context.contains("host review required")
                            && context.contains("candidate_refs")
                    );
                }
                gate.store(true, std::sync::atomic::Ordering::Release);
                Box::pin(async { Evaluation::Rejected })
            }));
        // Only child admission is optional. Core work controls need no grant.
        let broker = Arc::new(
            ModelBroker::new(store.clone(), model(&root.policy.model))
                .with_controls([Control::Spawn]),
        );
        let mut scheduler = HostScheduler::new(broker, vec![root], 2).unwrap();
        scheduler.approve(template.clone()).unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let parent_calls = calls.clone();
        let child_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let progress = child_progress.clone();
        let http = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(socket.read_u8().await.unwrap());
                    assert!(headers.len() < 16384);
                }
                let length: usize = String::from_utf8(headers)
                    .unwrap()
                    .lines()
                    .find_map(|line| {
                        let (k, v) = line.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(length < 64000);
                let mut bytes = vec![0; length];
                socket.read_exact(&mut bytes).await.unwrap();
                let parent = String::from_utf8(bytes.clone())
                    .unwrap()
                    .contains("objective-parent");
                let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert!(wire["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|t| t["function"]["name"] == "work"));
                let response = if parent {
                    let n = parent_calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    let (tool, arguments) = if python {
                        assert_eq!(n, 0, "explicit complete must not call the model again");
                        (
                            "ipython",
                            json!({"code":"import json\nretained = 40\ns = json.loads((await work.status())['content'])\nassert s['objective'] == 'objective-parent'\nassert s['instruction_revision'] == 1\na = require('agents')\nawait a.spawn(template_id='approved-batch', command_id='spawn')\nr = json.loads((await work.ask(request_id='question', question='How much?', timeout_ms=10000))['content'])\nassert r['resumed']\nretained += int(r['answer'])\nassert retained == 42\nawait work.complete(summary=f'retained {retained}', candidate_refs=[], unresolved_questions=['host review required'])"}),
                        )
                    } else {
                        match n {
                            0 => ("work", json!({"action":"status"})),
                            1 => (
                                "agents",
                                json!({"action":"spawn","template_id":"approved-batch","command_id":"spawn"}),
                            ),
                            2 => (
                                "work",
                                json!({"action":"ask","request_id":"question","question":"How much?","timeout_ms":10000}),
                            ),
                            3 => {
                                let output = wire["messages"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .rev()
                                    .find(|m| m["role"] == "tool")
                                    .unwrap();
                                assert!(output["content"].as_str().unwrap().contains("resumed"));
                                (
                                    "work",
                                    json!({"action":"complete","summary":"retained 42","candidate_refs":[],"unresolved_questions":["host review required"]}),
                                )
                            }
                            _ => panic!("completion was not intercepted"),
                        }
                    };
                    let mut arguments = arguments;
                    if python {
                        let code = arguments["code"].as_str().unwrap().to_owned();
                        arguments["code"] = json!(format!("{code}\nassert (await work.complete(summary='override', candidate_refs=[], unresolved_questions=[]))['is_error']\nraise RuntimeError('failure after completion')"));
                    }
                    let mut tool_calls = vec![
                        json!({"index":0,"id":format!("call-{n}"),"function":{"name":tool,"arguments":arguments.to_string()}}),
                    ];
                    if !python && n == 3 {
                        tool_calls.push(json!({"index":1,"id":"conflicting-completion","function":{"name":"work","arguments":json!({"action":"complete","summary":"override","candidate_refs":[],"unresolved_questions":[]}).to_string()}}));
                        tool_calls.push(json!({"index":2,"id":"failed-sibling","function":{"name":"exec","arguments":json!({"argv":["/bin/sh","-c","exit 7"]}).to_string()}}));
                    }
                    format!(
                        "data: {}\n\ndata: [DONE]\n\n",
                        json!({"choices":[{"delta":{"tool_calls":tool_calls}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.0000061}})
                    )
                } else {
                    progress.store(true, std::sync::atomic::Ordering::Release);
                    format!("{VALID}data: [DONE]\n\n")
                };
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
            }
        });
        let campaign = &template.parent.campaign_id;
        let api = Arc::new(std::sync::Mutex::new(crate::Registry {
            runtime_store: Some(store.clone()),
            ..Default::default()
        }));
        let (mut operator, server) = std::os::unix::net::UnixStream::pair().unwrap();
        operator
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let handler = std::thread::spawn(move || crate::handle_connection(server, api));
        let mut reader = std::io::BufReader::new(operator.try_clone().unwrap());
        let mut dispatch = |request: &ApiRequest| {
            use std::io::{BufRead, Write};
            let mut bytes = serde_json::to_vec(request).unwrap();
            bytes.push(b'\n');
            operator.write_all(&bytes).unwrap();
            let mut response = String::new();
            reader.read_line(&mut response).unwrap();
            serde_json::from_str::<ApiResponse>(&response).unwrap()
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut answered = false;
        loop {
            assert!(
                Instant::now() < deadline,
                "core fixture timed out python={python}"
            );
            scheduler.tick().await.unwrap();
            if !answered && child_progress.load(std::sync::atomic::Ordering::Acquire) {
                let status = store.campaign_work_status(campaign, "parent").unwrap();
                assert!(
                    !status.active && status.wait.is_some(),
                    "root cap one must be released for child"
                );
                let ApiResponse::CampaignAttentionList { questions, .. } =
                    dispatch(&ApiRequest::CampaignAttentionList {
                        id: campaign.clone(),
                        after: None,
                        limit: 32,
                    })
                else {
                    panic!()
                };
                assert_eq!(questions.len(), 1);
                let q = &questions[0];
                let answer = ApiRequest::CampaignAttentionAnswer {
                    id: campaign.clone(),
                    work_id: q.work_id.clone(),
                    request_id: q.request_id.clone(),
                    generation: q.generation,
                    instruction_revision: q.instruction_revision,
                    answer: "2".into(),
                };
                // Same typed endpoint used by the CLI; no worker send/steer shortcut.
                assert!(matches!(
                    dispatch(&answer),
                    ApiResponse::CampaignAttentionAnswered { .. }
                ));
                assert!(matches!(
                    dispatch(&answer),
                    ApiResponse::CampaignAttentionAnswered { .. }
                ));
                answered = true;
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
        assert!(answered && evaluated.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Acquire),
            if python { 1 } else { 4 }
        );
        assert_eq!(
            store
                .campaign_execution(campaign, "parent")
                .unwrap()
                .unwrap()
                .phase,
            crate::runtime_store::execution::ExecutionPhase::Reviewed(Evaluation::Rejected)
        );
        scheduler.shutdown().await.unwrap();
        drop(dispatch);
        drop(reader);
        drop(operator);
        handler.join().unwrap().unwrap();
        http.abort();
    }
}
