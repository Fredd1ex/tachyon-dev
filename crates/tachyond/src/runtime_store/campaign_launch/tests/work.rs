use super::*;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN and IPython; localhost only"]
async fn actual_root_only_work_status_ask_complete_command_gate() {
    let (dir, store, mut m) = fixture();
    let workspace = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    m.workspace = workspace.path().canonicalize().unwrap();
    m.home = home.path().canonicalize().unwrap();
    m.executable = PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"))
        .canonicalize()
        .unwrap();
    m.evaluator.argv = vec![
        "/usr/bin/grep".into(),
        "-qx".into(),
        "42".into(),
        "candidate".into(),
    ];
    m.evaluator.timeout_ms = 1000;
    m.evaluator.max_total_command_ms = 1000;
    m.validate(now()).unwrap();
    assert!(m.children.is_none());
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let http = tokio::spawn(async move {
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
            assert!(length <= 32000);
            let mut body = vec![0; length];
            socket.read_exact(&mut body).await.unwrap();
            let wire: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let tools = wire["tools"].as_array().unwrap();
            assert!(tools.iter().any(|t| t["function"]["name"] == "work"));
            assert!(!tools.iter().any(|t| t["function"]["name"] == "agents"));
            let messages = wire["messages"].as_array().unwrap();
            let n = counted.fetch_add(1, Ordering::SeqCst);
            // Each actor response occurs once, with one result for each prior call.
            for previous in 0..n {
                assert_eq!(
                    messages
                        .iter()
                        .filter(|m| m["role"] == "assistant"
                            && m["tool_calls"].as_array().is_some_and(|calls| calls
                                .iter()
                                .any(|c| c["id"] == format!("root-{previous}"))))
                        .count(),
                    1
                );
                assert_eq!(
                    messages
                        .iter()
                        .filter(|m| m["role"] == "tool"
                            && m["tool_call_id"] == format!("root-{previous}"))
                        .count(),
                    1
                );
            }
            let (name, args) = match n {
                0 => ("work", json!({"action":"status","work_id":"foreign"})),
                1 => {
                    let denied = messages
                        .iter()
                        .find(|m| m["role"] == "tool" && m["tool_call_id"] == "root-0")
                        .unwrap();
                    assert!(
                        denied["content"].as_str().unwrap().contains("error"),
                        "{denied}"
                    );
                    (
                        "ipython",
                        json!({"code":"import json\nretained = 40\ns = json.loads((await work.status())['content'])\nassert s['objective'] == 'fixture'\nassert s['phase'] == 'running' and s['instruction_revision'] == 1\nassert s['remaining_tokens'] > 0 and s['remaining_cost_micro_usd'] > 0\nassert s['pending_questions'] == []\nr = json.loads((await work.ask(request_id='offset', question='Which offset?', timeout_ms=10000))['content'])\nassert r['resumed']\nretained += int(r['answer'])\nassert retained == 42\nfrom pathlib import Path\nPath('result').write_text(str(retained))"}),
                    )
                }
                2 => (
                    "ipython",
                    json!({"code":"a = require('artifact')\nr = await a.register(path='result', kind='file', description='root-only result')\nassert not r['is_error'], r\nassert r['metadata']['artifact']['publication']['state'] == 'pending', r\nassert r['metadata']['artifact']['work_id'].endswith('-root'), r"}),
                ),
                3 => {
                    let artifact_result = messages
                        .iter()
                        .find(|m| m["role"] == "tool" && m["tool_call_id"] == "root-2")
                        .unwrap();
                    let result: serde_json::Value =
                        serde_json::from_str(artifact_result["content"].as_str().unwrap()).unwrap();
                    assert_eq!(result["is_error"], false, "{result}");
                    (
                        "ipython",
                        json!({"code":"assert retained == 42\nawait work.complete(summary=f'retained {retained}', candidate_refs=['untrusted-reference'], unresolved_questions=['independent review required'])"}),
                    )
                }
                _ => panic!("completion must not request another model response"),
            };
            let response = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":format!("root-{n}"),"function":{"name":name,"arguments":args.to_string()}}]}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.000007}})
            );
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        }
    });
    let model = Model::new(ModelConfig {
        base_url: base_url.clone(),
        api_key: String::new(),
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
    service
        .launch(&m, base_url, false, move |launch, cancel| {
            execute(run_store, root, launch, model, cancel)
        })
        .unwrap();
    let work = format!("{}-root", m.campaign_id);
    let verifier = format!("{}-verification", m.campaign_id);
    let mut answered = false;
    tokio::time::timeout(Duration::from_secs(20), async {
        while !service.active.lock().unwrap()[&m.campaign_id]
            .task
            .is_finished()
        {
            if store.admitted_work(&m.campaign_id, &work).is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let questions = store.work_attention(&m.campaign_id, &work).unwrap();
            if !answered && !questions.is_empty() {
                assert_eq!(questions.len(), 1);
                let status = store.campaign_work_status(&m.campaign_id, &work).unwrap();
                assert!(!status.active && status.wait.is_some());
                let queued = store
                    .campaign_work_status(&m.campaign_id, &verifier)
                    .unwrap();
                assert!(!queued.active && !queued.terminal);
                let answer = ApiRequest::CampaignAttentionAnswer {
                    id: m.campaign_id.clone(),
                    work_id: work.clone(),
                    request_id: "offset".into(),
                    generation: 1,
                    instruction_revision: 1,
                    answer: "2".into(),
                };
                let mut foreign = answer.clone();
                if let ApiRequest::CampaignAttentionAnswer { work_id, .. } = &mut foreign {
                    *work_id = verifier.clone();
                }
                assert!(store.attention_request(&foreign).is_err());
                store.attention_request(&answer).unwrap();
                store.attention_request(&answer).unwrap();
                answered = true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(answered);
    assert_eq!(status(&store, &m), CampaignStatus::Accepted);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(store.attention_notifications.lock().unwrap().len(), 1);
    let record = store
        .campaign_execution(&m.campaign_id, &work)
        .unwrap()
        .unwrap();
    assert!(record.settled);
    let candidate = record.candidate.unwrap();
    assert_eq!(candidate.outcome.completed_result(), Some("retained 42"));
    assert_eq!(candidate.instruction_revision, Some(1));
    assert_eq!(candidate.candidate_refs.as_ref().unwrap().len(), 1);
    assert_ne!(candidate.candidate_refs.unwrap()[0], "untrusted-reference");
    let ApiResponse::CampaignProgress { activity, .. } = service.progress(&m.campaign_id).unwrap()
    else {
        panic!()
    };
    assert_eq!(activity.admitted, 2);
    assert_eq!(activity.active, 0);
    assert_eq!(activity.terminal, 2);
    service.shutdown();
    assert!(!http.is_finished(), "fake provider assertions failed");
    http.abort();
}
