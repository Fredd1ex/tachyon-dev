//! Scripted observations, not model competence or benchmark evidence.
use super::*;
use ghost::harness::{
    runtime::{NoopEventSink, NoopOutputStore, ToolContext, ToolIdentity, ToolPolicy},
    tools::workspace::apply_exact_patch,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tachyon_api::{
    agents::Control,
    campaign::{CampaignChild, CampaignChildren, CampaignTemplate, ChildCompletion},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const INPUT: &str = include_str!("acceptance_input.rs.txt");
const NOTE: &str = "// Host single-writer integration revision.\n";
const REPORT: &str = "// Report: observed adds fail before repair and pass after repair.\n// Exact patch: - a - b; + a + b\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN and local rustc; no API credits"]
async fn actual_dogfood_sequential_rust_repair() {
    dogfood(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN and local rustc; no API credits"]
async fn actual_dogfood_distributed_evidence_and_single_writer() {
    dogfood(true).await;
}

fn content(output: &Value) -> Value {
    assert_ne!(output["is_error"], true, "{output}");
    serde_json::from_str(output["content"].as_str().unwrap()).unwrap()
}

fn writer(root: PathBuf) -> ToolContext {
    ToolContext {
        workspace_root: root.clone(),
        cwd: root.clone(),
        identity: ToolIdentity::default(),
        deadline: std::time::Instant::now() + Duration::from_secs(10),
        cancellation: tokio_util::sync::CancellationToken::new(),
        policy: Arc::new(ToolPolicy::worker_default(root)),
        event_sink: Arc::new(NoopEventSink),
        output_store: Arc::new(NoopOutputStore),
        host_service: None,
    }
}

async fn dogfood(distributed: bool) {
    let (dir, store, mut m) = fixture();
    let paths: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    m.workspace = paths[0].path().canonicalize().unwrap();
    m.home = paths[1].path().canonicalize().unwrap();
    m.executable = PathBuf::from(
        std::env::var_os("GHOST_TEST_BIN")
            .expect("build Ghost, then explicitly set GHOST_TEST_BIN"),
    )
    .canonicalize()
    .unwrap();
    let rustc = std::process::Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .expect("local Rust toolchain");
    assert!(rustc.status.success());
    let rustc = PathBuf::from(String::from_utf8(rustc.stdout).unwrap().trim())
        .join("bin/rustc")
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert!(PathBuf::from(&rustc).is_absolute());
    // Copy a checked-in input, never edit this repository from a worker.
    for workspace in [&m.workspace, &paths[2].path().to_owned()] {
        std::fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("src/runtime_store/campaign_launch/tests/acceptance_input.rs.txt"),
            workspace.join("project.rs"),
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(workspace.join("project.rs")).unwrap(),
            INPUT
        );
    }
    m.deadline_ms = now() + 60000;
    m.work_tokens = 3000;
    m.work_cost_micro_usd = 3000;
    m.retained_storage_bytes = Some(8 * 1024 * 1024);
    m.max_request_bytes = 64000;
    m.max_active_inferences = 4;
    m.evaluator.argv = vec!["/bin/sh".into(), "-c".into(), format!("'{rustc}' --test --crate-name acceptance candidate -o checked && ./checked --test-threads=1")];
    m.evaluator.timeout_ms = 10000;
    m.evaluator.max_total_command_ms = 10000;
    m.evaluator.input_bytes = 4096;
    m.evaluator.output_bytes = 4096;
    m.children = distributed.then(|| CampaignChildren {
        max_depth: 1,
        dynamic: None,
        total_work: 4,
        max_running: 1,
        max_resident: 2,
        controls: vec![Control::Spawn, Control::Wait, Control::Result],
        history: true,
        completion: ChildCompletion::CancelOutstanding,
        templates: vec![CampaignTemplate {
            template_id: "repair".into(),
            group_id: None,
            max_running: 1,
            specs: vec![CampaignChild {
                evaluator: None,
                work_id: Some("repair-child".into()),
                objective: "inspect-child-rust".into(),
                workspace: paths[2].path().canonicalize().unwrap(),
                home: paths[3].path().canonicalize().unwrap(),
                work_tokens: 1000,
                work_cost_micro_usd: 1000,
                verification_tokens: 2,
                verification_cost_micro_usd: 2,
            }],
        }],
    });
    m.validate(now()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let model = Model::new(ModelConfig {
        base_url: base_url.clone(),
        api_key: "localhost-fake-not-a-credential".into(),
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
    let root_path = m.workspace.clone();
    let (asking, asked) = tokio::sync::oneshot::channel();
    let http = tokio::spawn(async move {
        let mut asking = Some(asking);
        let mut calls = [0usize; 2];
        let mut stale = [String::new(), String::new()];
        let mut trace_seen = false;
        let mut child_source = String::new();
        let mut child_failure = String::new();
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
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse().unwrap())
                })
                .unwrap();
            assert!(length <= 64000);
            let mut bytes = vec![0; length];
            socket.read_exact(&mut bytes).await.unwrap();
            let wire: Value = serde_json::from_slice(&bytes).unwrap();
            let messages = wire["messages"].as_array().unwrap();
            let child = messages.iter().any(|m| {
                m["role"] == "user" && m["content"].to_string().contains("inspect-child-rust")
            });
            let index = usize::from(child);
            let n = calls[index];
            calls[index] += 1;
            assert!(
                calls.iter().sum::<usize>() <= 40,
                "script exceeded finite call budget"
            );
            let outputs: Vec<Value> = messages
                .iter()
                .filter(|m| m["role"] == "tool")
                .map(|m| serde_json::from_str(m["content"].as_str().unwrap()).unwrap())
                .collect();
            assert_eq!(outputs.len(), n, "one observed tool per scripted turn");
            let last = outputs.last().cloned().unwrap_or(Value::Null);
            let prefix = distributed && !child;
            let action = if prefix && n < 7 {
                match n {
                    0 => (
                        "agents",
                        json!({"action":"spawn","template_id":"repair","command_id":"repair-once"}),
                    ),
                    1 => {
                        assert_eq!(content(&last)["work_ids"], json!(["repair-child"]));
                        (
                            "agents",
                            json!({"action":"wait","work_ids":["repair-child"],"mode":"all","timeout_ms":20000}),
                        )
                    }
                    2 => {
                        assert_eq!(content(&last)["completed"].as_array().unwrap().len(), 1);
                        (
                            "agents",
                            json!({"action":"result","work_id":"repair-child"}),
                        )
                    }
                    3 => {
                        assert_eq!(content(&last)["snapshot"]["phase"], "accepted");
                        (
                            "history",
                            json!({"action":"traces","query":{"limit":16,"literal":"\"call_id\":\"call-0\""}}),
                        )
                    }
                    4 => {
                        let page = content(&last);
                        let resource = page["resources"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|r| {
                                r["reference"]["work_id"] == "repair-child"
                                    && r["data"]["phase"] == "result"
                            })
                            .expect("actual child failure trace");
                        (
                            "history",
                            json!({"action":"read","resource":resource["reference"],"offset":0,"limit":1024}),
                        )
                    }
                    5 => {
                        let page = content(&last);
                        let trace = &page["resources"][0];
                        let bytes: Vec<u8> =
                            serde_json::from_value(trace["data"]["bytes"].clone()).unwrap();
                        assert_eq!(
                            bytes,
                            child_failure.as_bytes()[..child_failure.len().min(1024)]
                        );
                        assert_eq!(
                            trace["reference"]["version"],
                            format!("{:x}", Sha256::digest(child_failure.as_bytes()))
                        );
                        assert!(String::from_utf8(bytes).unwrap().contains("adds"));
                        trace_seen = true;
                        (
                            "history",
                            json!({"action":"artifacts","query":{"limit":16}}),
                        )
                    }
                    6 => {
                        let page = content(&last);
                        let resource = page["resources"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|r| r["reference"]["work_id"] == "repair-child")
                            .expect("verified child artifact");
                        (
                            "history",
                            json!({"action":"read","resource":resource["reference"],"offset":0,"limit":1024}),
                        )
                    }
                    _ => unreachable!(),
                }
            } else {
                let step = n - if prefix { 7 } else { 0 };
                match step {
                    0 => {
                        if prefix {
                            let data = content(&last);
                            let bytes: Vec<u8> = serde_json::from_value(
                                data["resources"][0]["data"]["bytes"].clone(),
                            )
                            .expect("retained artifact bytes");
                            child_source = String::from_utf8(bytes).unwrap();
                            assert_eq!(
                                child_source,
                                format!("{}{}", INPUT.replace("a - b", "a + b"), REPORT)
                            );
                            assert!(trace_seen);
                        }
                        (
                            "exec",
                            json!({"command":format!("'{rustc}' --test project.rs -o tests && ./tests --test-threads=1"),"timeout_ms":10000}),
                        )
                    }
                    1 => {
                        assert_eq!(
                            last["metadata"]["exit_code"], 101,
                            "must observe the real failing test: {last}"
                        );
                        assert!(last["content"].as_str().unwrap().contains("adds"));
                        if child {
                            child_failure =
                                messages.iter().rev().find(|m| m["role"] == "tool").unwrap()
                                    ["content"]
                                    .as_str()
                                    .unwrap()
                                    .into();
                        }
                        ("read", json!({"path":"project.rs"}))
                    }
                    2 => {
                        assert_ne!(last["is_error"], true);
                        stale[index] = last["metadata"]["version"].as_str().unwrap().into();
                        if !child {
                            // HTTP request is the barrier: Ghost has finished its read and
                            // cannot issue the stale edit until this host write completes.
                            apply_exact_patch(
                                &writer(root_path.clone()),
                                "project.rs",
                                &stale[index],
                                INPUT,
                                &format!("{NOTE}{INPUT}"),
                            )
                            .await
                            .unwrap();
                        }
                        (
                            "edit",
                            json!({"path":"project.rs","old":"a - b","new":"a + b","expected_version":stale[index]}),
                        )
                    }
                    3 if !child => {
                        assert_eq!(last["is_error"], true, "{last}");
                        assert!(last.to_string().contains("conflict"));
                        assert_eq!(
                            std::fs::read_to_string(root_path.join("project.rs")).unwrap(),
                            format!("{NOTE}{INPUT}")
                        );
                        ("read", json!({"path":"project.rs"}))
                    }
                    4 if !child => {
                        let fresh = last["metadata"]["version"].as_str().unwrap();
                        assert_ne!(fresh, stale[index]);
                        if prefix {
                            assert!(child_source.contains("a + b"));
                            let context = writer(root_path.clone());
                            assert!(apply_exact_patch(
                                &context,
                                "project.rs",
                                &stale[index],
                                "a - b",
                                "a + b"
                            )
                            .await
                            .is_err());
                            apply_exact_patch(
                                &context,
                                "project.rs",
                                fresh,
                                INPUT,
                                child_source.strip_suffix(REPORT).unwrap(),
                            )
                            .await
                            .unwrap();
                            ("read", json!({"path":"project.rs"}))
                        } else {
                            (
                                "edit",
                                json!({"path":"project.rs","old":"a - b","new":"a + b","expected_version":fresh}),
                            )
                        }
                    }
                    s if s == if child { 3 } else { 5 } => {
                        assert_ne!(last["is_error"], true, "{last}");
                        (
                            "exec",
                            json!({"command":format!("'{rustc}' --test project.rs -o tests && ./tests --test-threads=1"),"timeout_ms":10000}),
                        )
                    }
                    s if s == if child { 4 } else { 6 } => {
                        assert_eq!(last["metadata"]["exit_code"], 0, "{last}");
                        assert!(last["content"].as_str().unwrap().contains("1 passed"));
                        if child || !distributed {
                            (
                                "write",
                                json!({"path":"candidate.rs","content":format!("{}{}{REPORT}", if child { "" } else { NOTE }, INPUT.replace("a - b", "a + b"))}),
                            )
                        } else {
                            asking.take().unwrap().send(()).unwrap();
                            (
                                "work",
                                json!({"action":"ask","request_id":"publish","question":"Publish this observed repair for host verification?","timeout_ms":10000}),
                            )
                        }
                    }
                    7 if prefix => {
                        assert_eq!(
                            content(&last)["answer"],
                            "Publish the reviewed exact repair."
                        );
                        (
                            "write",
                            json!({"path":"candidate.rs","content":format!("{NOTE}{child_source}")}),
                        )
                    }
                    s if s
                        == if child {
                            5
                        } else if distributed {
                            8
                        } else {
                            7
                        } =>
                    {
                        assert_ne!(last["is_error"], true, "{last}");
                        (
                            "artifact",
                            json!({"path":"candidate.rs","kind":"file","description":"Exact Rust repair, patch and observed test report"}),
                        )
                    }
                    s if s
                        == if child {
                            6
                        } else if distributed {
                            9
                        } else {
                            8
                        } =>
                    {
                        assert_ne!(last["is_error"], true, "{last}");
                        let response = format!(
                            "data: {}\n\ndata: [DONE]\n\n",
                            json!({"choices":[{"delta":{"content":"Published observed repair; host verification decides acceptance."}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.000007}})
                        );
                        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
                        if !child {
                            return calls;
                        }
                        continue;
                    }
                    _ => panic!("unexpected script step {step}, child={child}"),
                }
            };
            let (name, args) = action;
            let response = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":format!("call-{n}"),"function":{"name":name,"arguments":args.to_string()}}]}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.000007}})
            );
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        }
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
    tokio::time::timeout(Duration::from_secs(40), async {
        if distributed {
            asked.await.unwrap();
            loop {
                let ApiResponse::CampaignAttentionList { questions, .. } = store
                    .attention_request(&ApiRequest::CampaignAttentionList {
                        id: m.campaign_id.clone(),
                        after: None,
                        limit: 1,
                    })
                    .unwrap()
                else {
                    panic!()
                };
                if let Some(q) = questions.first() {
                    assert_eq!(q.request_id, "publish");
                    let answer = ApiRequest::CampaignAttentionAnswer {
                        id: m.campaign_id.clone(),
                        work_id: q.work_id.clone(),
                        request_id: q.request_id.clone(),
                        generation: q.generation,
                        instruction_revision: q.instruction_revision,
                        answer: "Publish the reviewed exact repair.".into(),
                    };
                    assert!(matches!(
                        store.attention_request(&answer).unwrap(),
                        ApiResponse::CampaignAttentionAnswered { .. }
                    ));
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        assert_eq!(
            finished.await.unwrap().unwrap(),
            ExecutionPhase::Reviewed(Evaluation::Accepted)
        );
    })
    .await
    .expect("bounded offline campaign must finish");
    let calls = tokio::time::timeout(Duration::from_secs(2), http)
        .await
        .expect("provider script must finish, not remain waiting for an extra request")
        .unwrap();
    // Completion channel fires before the service's final status write. Join only
    // after execute has returned; shutdown must not cancel a running fixture.
    let active = service
        .active
        .lock()
        .unwrap()
        .remove(&m.campaign_id)
        .unwrap();
    active.task.join().unwrap();
    assert_eq!(status(&store, &m), CampaignStatus::Accepted);
    assert_eq!(calls, if distributed { [17, 7] } else { [9, 0] });
    let questions = store
        .work_attention(&m.campaign_id, &format!("{}-root", m.campaign_id))
        .unwrap();
    assert_eq!(questions.len(), usize::from(distributed));
    if distributed {
        assert_eq!(
            questions[0].answer.as_deref(),
            Some("Publish the reviewed exact repair.")
        );
    }
    let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
    let mut spent = Units {
        tokens: 0,
        cost_micro_usd: 0,
    };
    for r in ledger
        .reservations
        .values()
        .filter(|r| r.allocation.is_some())
    {
        let super::super::super::campaign_ledger::Usage::Final(u) = r.usage else {
            panic!("usage must be final")
        };
        spent.tokens += u.tokens;
        spent.cost_micro_usd += u.cost_micro_usd;
    }
    assert_eq!(spent.tokens, 7 * calls.iter().sum::<usize>() as u64);
    assert_eq!(spent.cost_micro_usd, spent.tokens);
    assert_eq!(ledger.envelope.work.tokens, 3000);
    assert_eq!(ledger.envelope.verification.tokens, 10);
    let artifacts = ArtifactStore::open(
        &dir.path()
            .join("campaigns")
            .join(&m.campaign_id)
            .join("artifacts"),
    )
    .unwrap();
    for (work, workspace, expected) in std::iter::once((
        format!("{}-root", m.campaign_id),
        m.workspace.clone(),
        format!("{NOTE}{}{REPORT}", INPUT.replace("a - b", "a + b")),
    ))
    .chain(distributed.then(|| {
        (
            "repair-child".into(),
            paths[2].path().to_owned(),
            format!("{}{REPORT}", INPUT.replace("a - b", "a + b")),
        )
    })) {
        let gate = store
            .campaign_command_gate(&m.campaign_id, &work)
            .unwrap()
            .unwrap();
        let snapshot = gate.snapshot.unwrap();
        assert_eq!(
            snapshot.sha256,
            format!("{:x}", Sha256::digest(expected.as_bytes()))
        );
        assert_eq!(gate.evidence.unwrap().candidate_sha256, snapshot.sha256);
        let execution = store
            .campaign_execution(&m.campaign_id, &work)
            .unwrap()
            .unwrap();
        assert!(execution.settled);
        assert_eq!(
            execution.phase,
            ExecutionPhase::Reviewed(Evaluation::Accepted)
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join("project.rs")).unwrap(),
            expected.strip_suffix(REPORT).unwrap()
        );
        std::fs::write(workspace.join("candidate.rs"), "changed after acceptance").unwrap();
        assert_eq!(
            artifacts.read(&work, &snapshot.id, 0, 4096).unwrap(),
            expected.as_bytes()
        );
    }
}
