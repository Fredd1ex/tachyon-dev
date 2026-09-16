use super::*;
use sha2::{Digest, Sha256};
use tachyon_api::campaign::{ChildInput, ChildProfile, DynamicChildren, InputFile};
use tachyon_api::types::{WorkPermissions, WorkTaskType};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake model only"]
async fn actual_nested_python_wait_and_recursive_cancel() {
    for cancelling in [false, true] {
        let (dir, store, mut m) = fixture();
        let mut store = Arc::try_unwrap(store).ok().unwrap();
        store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(
            tachyon_util::config::ResourceLimits {
                max_execution_jobs: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let store = Arc::new(store);
        let paths: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
        m.workspace = paths[0].path().canonicalize().unwrap();
        m.home = paths[1].path().canonicalize().unwrap();
        m.executable = PathBuf::from(std::env::var_os("GHOST_TEST_BIN").unwrap())
            .canonicalize()
            .unwrap();
        m.objective = "nested-root".into();
        m.work_tokens = 3000;
        m.work_cost_micro_usd = 3000;
        m.verification_tokens = 30;
        m.verification_cost_micro_usd = 30;
        m.max_active_inferences = 6;
        m.max_request_bytes = 64000;
        std::fs::write(m.workspace.join("result"), b"hello\n").unwrap();
        std::fs::write(paths[3].path().join("approved.txt"), b"hello\n").unwrap();
        m.evaluator.argv = vec![
            "/usr/bin/grep".into(),
            "-qx".into(),
            "hello".into(),
            "candidate".into(),
        ];
        m.evaluator.timeout_ms = 1000;
        m.evaluator.max_total_command_ms = 1000;
        m.children = Some(CampaignChildren {
            max_depth: 2,
            total_work: 6,
            max_running: 1,
            max_resident: 3,
            controls: vec![
                Control::Spawn,
                Control::Group,
                Control::Wait,
                Control::Status,
                Control::Result,
                Control::Steer,
            ],
            history: false,
            completion: ChildCompletion::CancelOutstanding,
            templates: vec![],
            dynamic: Some(DynamicChildren {
                max_proposals: 2,
                profiles: vec![ChildProfile {
                    profile_ids: vec!["inspect".into()],
                    evaluator: None,
                    profile_id: "inspect".into(),
                    max_proposals: 2,
                    max_objective_bytes: 1024,
                    max_context_refs: 0,
                    managed_root: paths[2].path().canonicalize().unwrap(),
                    inputs: vec![ChildInput {
                        root: paths[3].path().canonicalize().unwrap(),
                        files: vec![InputFile {
                            path: "approved.txt".into(),
                            sha256: format!("{:x}", Sha256::digest(b"hello\n")),
                        }],
                    }],
                    max_input_files: 1,
                    max_input_bytes: 6,
                    permissions: WorkPermissions {
                        task_type: WorkTaskType::CodingReadOnly,
                        allow_exec: false,
                        allow_python: true,
                    },
                    work_tokens: 1000,
                    work_cost_micro_usd: 1000,
                    verification_tokens: 10,
                    verification_cost_micro_usd: 10,
                }],
            }),
        });
        m.allocation = Some(tachyon_api::campaign::CampaignAllocation {
            allowed_actions: tachyon_api::campaign::default_allocation_actions(),
            signals: vec![],
            mode: tachyon_api::campaign::AllocationMode::Deterministic,
            max_running: 1,
        });
        m.validate(now()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (seen, mut grandchild_seen) = watch::channel(false);
        let (release, _) = watch::channel(false);
        let released = release.clone();
        let resumed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = resumed.clone();
        let http = tokio::spawn(async move {
            let mut requests = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let seen = seen.clone();
                        let mut release = released.subscribe();
                        let observed = observed.clone();
                        requests.spawn(async move {
                            let mut header = Vec::new();
                            while !header.ends_with(b"\r\n\r\n") { header.push(socket.read_u8().await.unwrap()); assert!(header.len() < 16384); }
                            let length: usize = String::from_utf8(header).unwrap().lines().find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse().unwrap())
                            }).unwrap();
                            assert!(length <= 64000);
                            let mut bytes = vec![0; length];
                            socket.read_exact(&mut bytes).await.unwrap();
                            let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                            let messages = wire["messages"].as_array().unwrap();
                            let objective = messages.iter().find(|m| m["role"] == "user").unwrap()["content"].to_string();
                            let grand = objective.contains("nested-grandchild");
                            let root = objective.contains("nested-root");
                            let outputs = messages.iter().filter(|m| m["role"] == "tool").count();
                            if grand && outputs == 0 {
                                seen.send_replace(true);
                                while !*release.borrow_and_update() { release.changed().await.unwrap(); }
                            }
                            let action = if !grand && outputs == 0 {
                                let objective = if root { "nested-child" } else { "nested-grandchild" };
                                let command = if root { "root-proposal" } else { "child-proposal" };
                                Some(("ipython", serde_json::json!({"code":format!("import json, os\na = require('agents')\nretained = {{'pid': os.getpid(), 'values': [40, 2]}}\nr = await a.group(specs=[{{'profile_id':'inspect','objective':'{objective}','context_refs':[]}}], command_id='{command}', max_running=1)\nids = json.loads(r['content'])['work_ids']\nw = json.loads((await a.wait(work_ids=ids, mode='all', timeout_ms=10000))['content'])\nassert w['resumed'] and w['completed'] == ids\nassert os.getpid() == retained['pid'] and sum(retained['values']) == 42\ns = json.loads((await a.result(work_id=ids[0]))['content'])\nassert s['snapshot']['settled'] and s['snapshot']['phase'] == 'accepted'\nawait a.steer(work_id=ids[0], command_id='late-{command}', expected_revision=1, instructions='Late follow-up')\nh = json.loads((await a.result(work_id=ids[0], revision=1))['content'])\nassert h['snapshot']['has_candidate'] and not h['snapshot']['current']\nprint('nested-state-retained-42')")})))
                            } else if outputs == usize::from(!grand) {
                                if !grand {
                                    let reply = messages.iter().filter(|m| m["role"] == "tool").last().unwrap()["content"].to_string();
                                    assert!(reply.contains("nested-state-retained-42"), "{reply}");
                                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                }
                                Some(("artifact", serde_json::json!({"path":if root {"result"} else {"inputs/0/approved.txt"}, "kind":"file", "description":"candidate"})))
                            } else { None };
                            let delta = action.map_or_else(|| serde_json::json!({"content":"Published candidate"}), |(name, args)| serde_json::json!({"tool_calls":[{"index":0,"id":format!("call-{outputs}"),"function":{"name":name,"arguments":args.to_string()}}]}));
                            let response = serde_json::json!({"choices":[{"delta":delta}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.000007}});
                            let response = format!("data: {response}\n\ndata: [DONE]\n\n");
                            let _ = socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await;
                        });
                    }
                    result = requests.join_next(), if !requests.is_empty() => { result.unwrap().unwrap(); }
                }
            }
        });
        let model = Model::new(ModelConfig {
            base_url: base_url.clone(),
            api_key: "localhost-fixture".into(),
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
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        let run_store = store.clone();
        let storage = dir.path().to_owned();
        let (outcome, finished) = tokio::sync::oneshot::channel();
        service
            .launch(&m, base_url, false, move |launch, cancel| async move {
                let result = execute(run_store, storage, launch, model, cancel).await;
                let _ = outcome.send(result.clone());
                result
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !*grandchild_seen.borrow_and_update() {
                grandchild_seen.changed().await.unwrap();
            }
        })
        .await
        .expect("grandchild must run at root cap one");
        let ids = m.child_work_ids();
        let groups = store
            .list_campaign_groups(&m.campaign_id, None, 64)
            .unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.iter().filter(|g| g.spec.parent.is_some()).count(), 1);
        assert!(groups
            .iter()
            .all(|g| serde_json::to_value(g).unwrap()["policy_controller"].is_string()));
        for id in [format!("{}-root", m.campaign_id), ids[0].clone()] {
            let status = store.campaign_work_status(&m.campaign_id, &id).unwrap();
            assert!(!status.active && status.wait.is_some());
        }
        if cancelling {
            service.cancel(&m.campaign_id).unwrap();
        } else {
            release.send_replace(true);
        }
        let result = tokio::time::timeout(Duration::from_secs(20), finished)
            .await
            .expect("nested owners must drain")
            .unwrap();
        if !cancelling {
            assert!(
                matches!(
                    result,
                    Ok(ExecutionPhase::Reviewed(
                        super::super::super::super::execution::Evaluation::Accepted
                    ))
                ),
                "{result:?}"
            );
            assert_eq!(resumed.load(std::sync::atomic::Ordering::SeqCst), 2);
        }
        for id in ids {
            assert!(
                store
                    .campaign_work_status(&m.campaign_id, &id)
                    .unwrap()
                    .terminal
            );
        }
        assert_eq!(
            store
                .campaign_ledger(&m.campaign_id)
                .unwrap()
                .unwrap()
                .envelope
                .work
                .tokens,
            3000
        );
        service.shutdown();
        http.abort();
        let _ = http.await;
    }
}
