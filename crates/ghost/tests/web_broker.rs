#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]

use serde_json::json;
use std::{process::Stdio, time::Duration};
use tachyon_api::{agents, web::*};
use tachyon_model::{broker::*, Completion, ToolCall};

#[tokio::test]
async fn actual_ghost_bootstrap_exposes_only_granted_web_tools() {
    let root = tempfile::tempdir().unwrap();
    let listener = PrivateListener::bind().unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_ghost"))
        .args(["--broker", "--agent-id", "web-fixture", "--cwd"])
        .arg(root.path())
        .env_clear()
        .env("HOME", root.path())
        .env("PATH", "/nonexistent")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    listener
        .bootstrap_controls(&mut stdin, vec![agents::Control::WebFetch])
        .await
        .unwrap();
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 15000;
    write_frame(
        &mut stdin,
        &json!({"work_id":"web-work","objective":"fetch fixture","generation":1,
        "assignment":1,"deadline_ms":deadline,"lifetime_class":"short"}),
    )
    .await
    .unwrap();
    let server = tokio::spawn(async move {
        let host = listener
            .accept(pid, nix::unistd::Uid::effective().as_raw())
            .await
            .unwrap();
        let mut stream = host.authenticate().await.unwrap();
        let mut models = 0;
        let mut lookups = 0;
        while let Ok(frame) = read_frame::<FrameRequest>(&mut stream).await {
            let reply = match frame {
                FrameRequest::Model(request) => {
                    assert!(request.tools.iter().any(|s| s.name == WEBFETCH));
                    assert!(!request
                        .tools
                        .iter()
                        .any(|s| s.name == WEBSEARCH || s.name == "agents"));
                    let tool_calls = if models == 0 {
                        vec![ToolCall {
                            id: "fetch-call".into(),
                            name: WEBFETCH.into(),
                            arguments: json!({"urls":["https://example.org/paper.pdf?exact=%2F"]})
                                .to_string(),
                        }]
                    } else {
                        assert_eq!(models, 1);
                        let transcript = serde_json::to_string(&request.messages).unwrap();
                        assert!(transcript.contains("unverified"));
                        assert!(transcript.contains("paper.pdf?exact=%2F"));
                        assert!(transcript.contains("fixture report"));
                        vec![]
                    };
                    models += 1;
                    FrameReply::Model(Reply {
                        id: request.id,
                        completion: Some(Completion {
                            text: if models == 2 {
                                "fixture complete".into()
                            } else {
                                String::new()
                            },
                            tool_calls,
                            usage: Default::default(),
                            finish_reason: None,
                        }),
                    })
                }
                FrameRequest::Control(agents::Request::WebFetch { command }) => {
                    assert_eq!(command.caller_id, "web-work");
                    assert_eq!(command.tool_call_id, "fetch-call");
                    let WebRequest::Fetch {
                        urls,
                        instruction: None,
                        follow_links: None,
                    } = &command.request
                    else {
                        panic!("unexpected web arguments")
                    };
                    assert_eq!(urls, &["https://example.org/paper.pdf?exact=%2F"]);
                    let result = WebResult {
                        usage: Default::default(),
                        answer: "fixture report".into(),
                        citations: vec![],
                        annotations: vec![],
                        status: WebStatus::Unverified,
                        notice: "retrieval unconfirmed".into(),
                        host_observed_at: 42,
                        observed_search_uses: None,
                        observed_fetch_uses: None,
                        requested_urls: urls.clone(),
                    };
                    lookups += 1;
                    FrameReply::Control(agents::Reply::WebFetch { command, result })
                }
                _ => {
                    panic!("unexpected frame: web lookup must not start a browser/process service")
                }
            };
            write_frame(&mut stream, &reply).await.unwrap();
        }
        assert_eq!(models, 2);
        assert_eq!(lookups, 1);
    });
    let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    server.await.unwrap();
    assert!(String::from_utf8_lossy(&output.stdout).contains("fixture complete"));
}
