use super::*;

mod nested;
use tachyon_api::agents::Control;
use tachyon_api::campaign::{CampaignChild, CampaignChildren, CampaignTemplate, ChildCompletion};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn cancellation_with_uncertain_cleanup_is_unverified() {
    let (dir, store, m) = fixture();
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    service
        .launch(
            &m,
            "http://127.0.0.1".into(),
            false,
            |_, cancel| async move {
                let mut cancelled = cancel.subscribe();
                while !*cancelled.borrow_and_update() {
                    cancelled.changed().await.unwrap();
                }
                Err("cleanup uncertainty".into())
            },
        )
        .unwrap();
    service.cancel(&m.campaign_id).unwrap();
    service.shutdown();
    assert_eq!(status(&store, &m), CampaignStatus::Unverified);
    assert!(service.ready_to_resume(&m.campaign_id).is_err());
}

#[test]
fn launch_digest_preserves_legacy_and_rejects_changed_policy() {
    let (_dir, _store, manifest) = fixture();
    let mut launch = Launch {
        schema_version: 1,
        manifest: manifest.clone(),
        base_url: "http://127.0.0.1".into(),
        manifest_sha256: Some(Launch::digest(&manifest).unwrap()),
    };
    launch.validate_digest().unwrap();
    let bytes = serde_json::to_vec(&launch).unwrap();
    let restored: Launch = serde_json::from_slice(&bytes).unwrap();
    restored.validate_digest().unwrap();
    launch.manifest.work_tokens += 1;
    assert!(launch.validate_digest().is_err());
    launch.manifest_sha256 = None;
    launch.validate_digest().unwrap();
    launch.manifest.children = Some(CampaignChildren {
        max_depth: 1,
        dynamic: None,
        total_work: 4,
        max_running: 2,
        max_resident: 2,
        controls: vec![Control::Spawn],
        history: false,
        completion: ChildCompletion::CancelOutstanding,
        templates: vec![],
    });
    assert!(
        launch.validate_digest().is_err(),
        "child launch cannot silently use legacy digest defaults"
    );
}

#[test]
fn child_host_paths_are_rejected_before_provider_lookup_or_admission() {
    let (dir, store, mut m) = fixture();
    let paths: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    m.executable = PathBuf::from("/usr/bin/true").canonicalize().unwrap();
    m.evaluator.argv[0] = m.executable.to_string_lossy().into_owned();
    m.workspace = paths[0].path().canonicalize().unwrap();
    m.home = paths[1].path().canonicalize().unwrap();
    m.children = Some(CampaignChildren {
        max_depth: 1,
        dynamic: None,
        total_work: 4,
        max_running: 2,
        max_resident: 2,
        controls: vec![Control::Spawn],
        history: false,
        completion: ChildCompletion::CancelOutstanding,
        templates: vec![CampaignTemplate {
            template_id: "child".into(),
            group_id: None,
            max_running: 1,
            specs: vec![CampaignChild {
                evaluator: None,
                work_id: Some("child".into()),
                objective: "fixture".into(),
                workspace: dir.path().canonicalize().unwrap(),
                home: paths[2].path().canonicalize().unwrap(),
                work_tokens: 40,
                work_cost_micro_usd: 40,
                verification_tokens: 2,
                verification_cost_micro_usd: 2,
            }],
        }],
    });
    m.max_active_inferences = 4;
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    assert!(service
        .run(&m, true)
        .unwrap_err()
        .contains("disjoint from daemon storage"));
    let alias = paths[3].path().join("alias");
    std::os::unix::fs::symlink(paths[0].path(), &alias).unwrap();
    m.children.as_mut().unwrap().templates[0].specs[0].workspace = alias;
    assert!(service.run(&m, true).unwrap_err().contains("canonical"));
    assert_eq!(status(&store, &m), CampaignStatus::Draft);
    assert!(store.campaign_ledger(&m.campaign_id).unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost only"]
async fn actual_campaign_group_wait_command_verification_and_cancel() {
    campaign_group_cases(false, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake model only"]
async fn actual_dynamic_children_managed_inputs_wait_verify_and_cancel() {
    campaign_group_cases(true, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake model only"]
async fn actual_campaign_children_repair_final_results_and_exhaustion() {
    for dynamic in [false, true] {
        for mode in ["pass", "exhaust", "unknown"] {
            campaign_group_cases(dynamic, Some(mode)).await;
        }
    }
}

async fn campaign_group_cases(dynamic: bool, child_repair: Option<&'static str>) {
    let cases = if child_repair.is_some() {
        vec![
            (false, true, 1, false, child_repair == Some("unknown")),
            (false, true, 2, false, child_repair == Some("unknown")),
        ]
    } else {
        vec![
            (false, true, 2, false, false),
            (true, true, 2, false, false),
            (false, false, 2, false, false),
            (false, true, 1, false, false),
            (false, true, 2, true, false),
            (false, true, 1, false, true),
        ]
    };
    for (cancelling, wait_root, running, retry, unknown_child) in cases {
        let child_count = if child_repair.is_some() { 1 } else { running };
        let spawn = child_repair.is_some() && running == 1;
        let history = wait_root && !cancelling;
        let (dir, store, mut m) = fixture();
        let paths: Vec<_> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
        m.workspace = paths[0].path().canonicalize().unwrap();
        m.home = paths[1].path().canonicalize().unwrap();
        m.executable =
            PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"))
                .canonicalize()
                .unwrap();
        m.objective = "fixture".into();
        m.work_tokens = 3000;
        m.work_cost_micro_usd = 3000;
        m.verification_tokens = 30;
        m.verification_cost_micro_usd = 30;
        m.max_active_inferences = (child_count as u32 + 1) * 2;
        m.max_request_bytes = 64000;
        m.evaluator.argv = vec![
            "/usr/bin/grep".into(),
            "-qx".into(),
            "hello".into(),
            "candidate".into(),
        ];
        m.evaluator.timeout_ms = 1000;
        m.evaluator.max_attempts = if retry { 2 } else { 1 };
        m.evaluator.max_total_command_ms = 1000 * u64::from(m.evaluator.max_attempts);
        m.children = Some(CampaignChildren {
            max_depth: 1,
            dynamic: None,
            total_work: (child_count + 1) * 2,
            max_running: running,
            max_resident: if wait_root { child_count + 1 } else { 2 },
            controls: vec![
                Control::Templates,
                Control::Spawn,
                Control::Group,
                Control::Wait,
                Control::Status,
                Control::Result,
            ],
            history,
            completion: ChildCompletion::CancelOutstanding,
            templates: vec![CampaignTemplate {
                template_id: "pair".into(),
                group_id: (!spawn).then(|| "pair-group".into()),
                max_running: running,
                specs: (0..child_count)
                    .map(|i| CampaignChild {
                        evaluator: child_repair.map(|_| tachyon_api::campaign::ChildEvaluator {
                            max_attempts: 2,
                            max_total_command_ms: 2000,
                        }),
                        work_id: if history {
                            None
                        } else {
                            Some(format!("child-{i}"))
                        },
                        objective: format!("child-objective-{i}"),
                        workspace: paths[2 + i * 2].path().canonicalize().unwrap(),
                        home: paths[3 + i * 2].path().canonicalize().unwrap(),
                        work_tokens: 1000,
                        work_cost_micro_usd: 1000,
                        verification_tokens: 10,
                        verification_cost_micro_usd: 10,
                    })
                    .collect(),
            }],
        });
        if dynamic {
            use sha2::{Digest, Sha256};
            use tachyon_api::campaign::{ChildInput, ChildProfile, DynamicChildren, InputFile};
            use tachyon_api::types::{WorkPermissions, WorkTaskType};
            std::fs::write(paths[3].path().join("approved.txt"), b"hello\n").unwrap();
            std::fs::write(paths[3].path().join("unapproved.txt"), b"private\n").unwrap();
            let children = m.children.as_mut().unwrap();
            children.templates.clear();
            children.dynamic = Some(DynamicChildren {
                max_proposals: child_count,
                profiles: vec![ChildProfile {
                    profile_ids: vec![],
                    evaluator: child_repair.map(|_| tachyon_api::campaign::ChildEvaluator {
                        max_attempts: 2,
                        max_total_command_ms: 2000,
                    }),
                    profile_id: "inspect".into(),
                    max_proposals: child_count,
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
                        task_type: if child_repair.is_some() {
                            WorkTaskType::Coding
                        } else {
                            WorkTaskType::CodingReadOnly
                        },
                        allow_exec: false,
                        allow_python: false,
                    },
                    work_tokens: 1000,
                    work_cost_micro_usd: 1000,
                    verification_tokens: 10,
                    verification_cost_micro_usd: 10,
                }],
            });
        }
        m.validate(now()).unwrap();
        let child_ids = m.child_work_ids();
        let http_child_ids = child_ids.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let barrier = Arc::new(tokio::sync::Barrier::new(child_count + 1));
        let arrivals = barrier.clone();
        let (release, _) = watch::channel(false);
        let (child_seen, mut saw_child) = watch::channel(false);
        let released = release.clone();
        let child_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = child_calls.clone();
        let http = tokio::spawn(async move {
            let mut requests = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let barrier = arrivals.clone();
                        let mut release = released.subscribe();
                        let child_seen = child_seen.clone();
                         let child_ids = http_child_ids.clone();
                         let counted = counted.clone();
                        requests.spawn(async move {
                            let mut header = Vec::new();
                            while !header.ends_with(b"\r\n\r\n") {
                                header.push(socket.read_u8().await.unwrap());
                                assert!(header.len() < 16384);
                            }
                            let length: usize = String::from_utf8(header).unwrap().lines().find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse().unwrap())
                            }).unwrap();
                            assert!(length <= 64000);
                            let mut bytes = vec![0; length];
                            socket.read_exact(&mut bytes).await.unwrap();
                            let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                            assert_eq!(wire["tools"].as_array().unwrap().iter().any(|t| t["function"]["name"] == "history"), history);
                            let text = String::from_utf8(bytes).unwrap();
                             let child = wire["messages"].as_array().unwrap().iter()
                                 .find(|m| m["role"] == "user").unwrap()["content"].to_string().contains("child-objective-");
                             if dynamic && child {
                                 assert!(!wire["tools"].as_array().unwrap().iter().any(|t| matches!(t["function"]["name"].as_str(), Some("exec" | "ipython" | "agent_browser"))));
                             }
                             let repair = text.contains("Host verification rejected");
                             if child { counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
                            let outputs = wire["messages"].as_array().unwrap().iter().filter(|m| m["role"] == "tool").count();
                             if child && outputs == 0 && !repair {
                                child_seen.send_replace(true);
                                if wait_root { barrier.wait().await; }
                                while !*release.borrow_and_update() { release.changed().await.unwrap(); }
                            }
                            let action = if !child && outputs == 0 {
                                Some(("agents", serde_json::json!({"action":"templates", "limit":32})))
                            } else if !child && outputs == 1 {
                                let reply = wire["messages"].as_array().unwrap().iter().find(|m| m["role"] == "tool").unwrap()["content"].as_str().unwrap();
                                 assert!(!reply.contains("workspace") && !reply.contains("pricing"));
                                  if dynamic {
                                     assert!(!reply.contains("inspect"), "slots are not fixed selectors");
                                      if spawn { Some(("agents", serde_json::json!({"action":"spawn", "profile_id":"inspect", "objective":"child-objective-0", "context_refs":[], "command_id":"pair-once"}))) }
                                      else { Some(("agents", serde_json::json!({"action":"group", "specs":(0..child_count).map(|i| serde_json::json!({"profile_id":"inspect", "objective":format!("child-objective-{i}"), "context_refs":[]})).collect::<Vec<_>>(), "command_id":"pair-once", "max_running":running}))) }
                                 } else {
                                     assert!(reply.contains("pair"));
                                      if spawn { Some(("agents", serde_json::json!({"action":"spawn", "template_id":"pair", "command_id":"pair-once"}))) }
                                      else { Some(("agents", serde_json::json!({"action":"group", "template_id":"pair", "command_id":"pair-once", "max_running":running}))) }
                                 }
                            } else if !child && outputs == 2 && wait_root {
                                 Some(("agents", serde_json::json!({"action":"wait", "work_ids":child_ids, "mode":"all", "timeout_ms":if child_repair.is_some() && unknown_child { 3000 } else { 10000 }})))
                             } else if !child && child_repair.is_some() && outputs == 3 {
                                 let reply: serde_json::Value = serde_json::from_str(wire["messages"].as_array().unwrap().iter().filter(|m| m["role"] == "tool").last().unwrap()["content"].as_str().unwrap()).unwrap();
                                 let reply: serde_json::Value = serde_json::from_str(reply["content"].as_str().expect("tool content")).unwrap();
                                 // Dynamic reads retain output handles. Unknown accounting
                                 // denies their export, ending Work without a repair candidate.
                                 let pending_repair = unknown_child && !dynamic;
                                 assert_eq!(reply["completed"].as_array().unwrap().len(), usize::from(!pending_repair), "wait must see final logical Work, not the first failed attempt: {reply}");
                                 assert_eq!(reply["outstanding"].as_array().unwrap().len(), usize::from(pending_repair));
                                 Some(("agents", serde_json::json!({"action":"result", "work_id":child_ids[0]})))
                             } else {
                                 if !child && child_repair.is_some() && outputs == 4 {
                                     let reply: serde_json::Value = serde_json::from_str(wire["messages"].as_array().unwrap().iter().filter(|m| m["role"] == "tool").last().unwrap()["content"].as_str().unwrap()).unwrap();
                                     let reply: serde_json::Value = serde_json::from_str(reply["content"].as_str().expect("tool content")).unwrap();
                                     assert_eq!(reply["snapshot"]["phase"], if unknown_child && dynamic { "unverified" } else if unknown_child { "rework_pending" } else if child_repair == Some("exhaust") { "rejected" } else { "accepted" }, "{reply}");
                                     assert_eq!(reply["snapshot"]["settled"], !unknown_child);
                                 }
                                 if child && child_repair.is_some() {
                                     let good = repair && child_repair != Some("exhaust");
                                     match outputs {
                                         0 if dynamic => Some(("read", serde_json::json!({"path":if repair { "result" } else { "inputs/0/approved.txt" }}))),
                                         1 if dynamic => {
                                             let reply = wire["messages"].as_array().unwrap().iter().filter(|m| m["role"] == "tool").last().unwrap()["content"].as_str().unwrap();
                                             assert!(reply.contains(if repair { "wrong" } else { "hello" }), "repair must retain outputs and the original input: {reply}");
                                             Some(("write", serde_json::json!({"path":"inputs/0/approved.txt", "content":"forbidden"})))
                                         }
                                         2 if dynamic => {
                                             let reply = wire["messages"].as_array().unwrap().iter().filter(|m| m["role"] == "tool").last().unwrap()["content"].as_str().unwrap();
                                             assert!(reply.contains("read-only"), "immutable baseline write must be denied: {reply}");
                                             Some(("write", serde_json::json!({"path":"result", "content":if good { "hello\n" } else { "wrong\n" }})))
                                         }
                                         0 => Some(("write", serde_json::json!({"path":"result", "content":if good { "hello\n" } else { "wrong\n" }}))),
                                         n if n == if dynamic { 3 } else { 1 } => Some(("artifact", serde_json::json!({"path":"result", "kind":"file", "description":"candidate"}))),
                                         _ => None,
                                     }
                                 } else { match outputs - if child { 0 } else if child_repair.is_some() { 4 } else if wait_root { 3 } else { 2 } {
                                      0 if dynamic && child => Some(("read", serde_json::json!({"path":"inputs/0/approved.txt"}))),
                                     0 => Some(("exec", serde_json::json!({"command": if retry && !child && !repair { "printf 'wrong\\n' > result" } else { "printf 'hello\\n' > result" }}))),
                                     1 => Some(("artifact", serde_json::json!({"path":if dynamic && child { "inputs/0/approved.txt" } else { "result" }, "kind":"file", "description":"candidate"}))),
                                    _ => None,
                                 } }
                            };
                            if !child && !wait_root && action.is_none() {
                                let mut seen = child_seen.subscribe();
                                while !*seen.borrow_and_update() { seen.changed().await.unwrap(); }
                            }
                            let delta = action.map_or_else(|| serde_json::json!({"content":"Published candidate"}), |(name, arguments)| {
                                serde_json::json!({"tool_calls":[{"index":0,"id":format!("call-{outputs}"),"function":{"name":name,"arguments":arguments.to_string()}}]})
                            });
                            let mut response = serde_json::json!({"choices":[{"delta":delta}], "usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.000007}});
                             if unknown_child && child && outputs >= if dynamic && child_repair.is_some() { 4 } else { 2 } {
                                response.as_object_mut().unwrap().remove("usage");
                            }
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
        let root = dir.path().to_owned();
        let (outcome, finished) = tokio::sync::oneshot::channel();
        service
            .launch(&m, base_url, false, move |launch, cancel| async move {
                let result = execute(run_store, root, launch, model, cancel).await;
                let _ = outcome.send(result.clone());
                result
            })
            .unwrap();
        if wait_root {
            tokio::time::timeout(Duration::from_secs(15), barrier.wait())
                .await
                .expect("children must run after the parent releases its running slot");
            // With two slots and one child, child launch can precede parent suspension.
            tokio::time::timeout(Duration::from_secs(5), async {
                while store
                    .campaign_work_status(&m.campaign_id, &format!("{}-root", m.campaign_id))
                    .unwrap()
                    .wait
                    .is_none()
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("root must enter its durable wait before releasing child HTTP");
            let parent = store
                .campaign_work_status(&m.campaign_id, &format!("{}-root", m.campaign_id))
                .unwrap();
            assert!(
                !parent.active && parent.wait.is_some(),
                "parent releases its registered slot"
            );
            assert_eq!(status(&store, &m), CampaignStatus::Running);
            let ApiResponse::CampaignProgress { activity, .. } =
                service.progress(&m.campaign_id).unwrap()
            else {
                panic!()
            };
            assert!(activity.owned);
            assert_eq!(activity.admitted, (child_count + 1) * 2);
            assert_eq!(activity.active, child_count);
            assert_eq!(activity.waiting, 1);
        } else {
            tokio::time::timeout(Duration::from_secs(15), async {
                while !*saw_child.borrow_and_update() {
                    saw_child.changed().await.unwrap();
                }
            })
            .await
            .expect("root completion must cancel an outstanding child");
        }
        let root_work = store
            .admitted_work(&m.campaign_id, &format!("{}-root", m.campaign_id))
            .unwrap();
        assert_eq!(
            root_work.admission.upper_bound.tokens,
            3000 - child_count as u64 * 1000
        );
        let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
        assert_eq!(ledger.envelope.work.tokens, 3000);
        if cancelling {
            service.cancel(&m.campaign_id).unwrap();
        }
        if wait_root && !cancelling {
            if dynamic && child_repair.is_some() {
                std::fs::write(paths[3].path().join("approved.txt"), b"changed source\n").unwrap();
            }
            release.send_replace(true);
        }
        tokio::time::timeout(Duration::from_secs(20), async {
            while !service
                .active
                .lock()
                .unwrap()
                .get(&m.campaign_id)
                .unwrap()
                .task
                .is_finished()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("campaign and all child owners must drain");
        service.shutdown();
        let outcome = finished.await.unwrap();
        if child_repair.is_some() {
            assert_eq!(
                child_calls.load(std::sync::atomic::Ordering::SeqCst),
                (if dynamic { 5 } else { 3 }) * if unknown_child { 1 } else { 2 },
                "bounded attempts must not issue additional HTTP"
            );
            let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
            assert_eq!(ledger.envelope.work.tokens, 3000);
            assert_eq!(ledger.envelope.work.cost_micro_usd, 3000);
            assert_eq!(ledger.envelope.verification.tokens, 30);
            if !unknown_child {
                let spent: u64 = ledger
                    .reservations
                    .values()
                    .filter(|r| r.allocation.is_some())
                    .map(|r| match r.usage {
                        super::super::super::campaign_ledger::Usage::Final(units) => {
                            assert_eq!(units.tokens, units.cost_micro_usd);
                            units.tokens
                        }
                        _ => panic!("successful cleanup must have final holds"),
                    })
                    .sum();
                assert_eq!(
                    spent,
                    7 * (7 + child_calls.load(std::sync::atomic::Ordering::SeqCst) as u64)
                );
            }
        }
        if dynamic {
            let profile = &m
                .children
                .as_ref()
                .unwrap()
                .dynamic
                .as_ref()
                .unwrap()
                .profiles[0];
            for child in profile.slots(&m.campaign_id).specs {
                assert_eq!(
                    std::fs::read(child.workspace.join("inputs/0/approved.txt")).unwrap(),
                    b"hello\n"
                );
                assert!(!child.workspace.join("inputs/0/unapproved.txt").exists());
                assert!(child.home.is_dir());
            }
        }
        assert_eq!(
            status(&store, &m),
            if unknown_child || cancelling || !wait_root {
                CampaignStatus::Unverified
            } else {
                CampaignStatus::Accepted
            },
            "cancelling={cancelling}, wait_root={wait_root}, retry={retry}, unknown_child={unknown_child}, outcome={outcome:?}"
        );
        if unknown_child {
            assert!(
                outcome.is_err(),
                "unknown child must block campaign success"
            );
            assert_eq!(
                store
                    .campaign_execution(&m.campaign_id, &format!("{}-root", m.campaign_id))
                    .unwrap()
                    .unwrap()
                    .phase,
                ExecutionPhase::Reviewed(Evaluation::Accepted),
                "even an accepted root must not hide unresolved child usage; root={:?}; outcome={outcome:?}",
                store.campaign_execution(&m.campaign_id, &format!("{}-root", m.campaign_id)).unwrap()
            );
        }
        for id in std::iter::once(format!("{}-root", m.campaign_id)).chain(child_ids) {
            let work = store.campaign_work_status(&m.campaign_id, &id).unwrap();
            assert!(!work.active, "no active lease after cleanup: {id}");
            if !id.ends_with("-root") {
                assert!(work.terminal, "child owner must finish cleanup: {id}; cancelling={cancelling}, wait_root={wait_root}, retry={retry}, unknown_child={unknown_child}, outcome={outcome:?}; execution={:?}", store.campaign_execution(&m.campaign_id, &id));
                if unknown_child {
                    let record = store
                        .campaign_execution(&m.campaign_id, &id)
                        .unwrap()
                        .unwrap();
                    assert!(!record.settled);
                    if dynamic || child_repair.is_none() {
                        assert_eq!(
                            record.phase,
                            ExecutionPhase::Reviewed(Evaluation::Unverified)
                        );
                        assert!(
                            record.candidate.is_none(),
                            "failed export cannot publish a candidate"
                        );
                    }
                    let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
                    assert!(
                        ledger.reservations.values().any(|r| {
                            r.allocation.as_ref() == Some(&work.work.dispatch_id)
                                && r.usage == super::super::super::campaign_ledger::Usage::Unknown
                        }),
                        "missing child usage must retain the charge hold"
                    );
                }
            }
            if !cancelling && !unknown_child && wait_root {
                let gate = store
                    .campaign_command_gate(&m.campaign_id, &id)
                    .unwrap()
                    .unwrap();
                let snapshot = gate.snapshot.unwrap();
                if child_repair.is_some() && !id.ends_with("-root") {
                    use sha2::{Digest, Sha256};
                    assert_eq!(
                        gate.history[0].snapshot.as_ref().unwrap().sha256,
                        format!("{:x}", Sha256::digest(b"wrong\n"))
                    );
                    assert_eq!(
                        snapshot.sha256,
                        format!(
                            "{:x}",
                            Sha256::digest(if child_repair == Some("exhaust") {
                                b"wrong\n"
                            } else {
                                b"hello\n"
                            })
                        )
                    );
                    assert_eq!(
                        gate.history[0].execution.policy.funding,
                        gate.policy.funding
                    );
                    assert_eq!(
                        gate.history[0].execution.policy.verification,
                        gate.policy.verification
                    );
                    assert_ne!(gate.history[0].snapshot.as_ref().unwrap().id, snapshot.id);
                }
                assert_eq!(
                    gate.history.len(),
                    usize::from(
                        (retry && id.ends_with("-root"))
                            || (child_repair.is_some() && !id.ends_with("-root"))
                    )
                );
                assert_eq!(snapshot.work_id.as_deref(), Some(id.as_str()));
                assert_eq!(
                    snapshot.attempt_id.as_deref(),
                    Some(gate.policy.model.identity.attempt_id.as_str())
                );
                assert_eq!(gate.evidence.unwrap().candidate_sha256, snapshot.sha256);
            }
        }
        release.send_replace(true);
        http.abort();
        let _ = http.await;
    }
}
