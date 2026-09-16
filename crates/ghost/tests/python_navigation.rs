#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]

use serde_json::{json, Value};
use std::{os::unix::fs::PermissionsExt, process::Stdio, time::Duration};
use tachyon_api::{agents, context};
use tachyon_model::{broker::*, Completion, ToolCall};

#[tokio::test]
#[ignore = "requires existing IPython; actual Ghost with private fake broker and browser, no installation"]
async fn actual_ghost_python_browser_artifact_and_filtered_ctx_share_broker() {
    let root = tempfile::tempdir().unwrap();
    assert!(
        ghost::harness::backend::Local::new(root.path())
            .check_ipython()
            .is_ok(),
        "this fixture requires existing IPython; it never installs dependencies"
    );
    let browser = root.path().join("browser-fixture");
    std::fs::write(&browser, include_str!("fixtures/browser.sh")).unwrap();
    std::fs::set_permissions(&browser, std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener = PrivateListener::bind().unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_ghost"))
        .args(["--broker", "--agent-id", "fixture", "--cwd"])
        .arg(root.path())
        .env_clear()
        .env("HOME", root.path())
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("TACHYON_AGENT_BROWSER_BIN", &browser)
        .env("TACHYON_LIGHTPANDA_BIN", &browser)
        .env("TACHYON_HARNESS_TOOLS_DIR", root.path().join("tools"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    listener
        .bootstrap_controls(&mut stdin, vec![agents::Control::Resource])
        .await
        .unwrap();
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 20000;
    write_frame(
        &mut stdin,
        &json!({"work_id":"current", "objective":"fixture", "generation":1,
        "assignment":1, "deadline_ms":deadline, "lifetime_class":"short",
        "attempt":{"id":"attempt", "feedback":null}}),
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
        let mut acquisitions = 0;
        let mut active = None;
        let mut pages = 0;
        while let Ok(frame) = read_frame::<FrameRequest>(&mut stream).await {
            let reply = match frame {
                FrameRequest::Model(request) => {
                    assert!(request.tools.iter().any(|t| t.name == "work"));
                    assert!(!request.tools.iter().any(|t| t.name == "agents"));
                    let completion = if models == 0 {
                        Completion { text: String::new(), tool_calls: vec![ToolCall {
                            id: "cell".into(), name: "ipython".into(), arguments: json!({"code": r#"
import json
from pathlib import Path
assert work is not None
browser = require('browser')
r = await browser.run(args='read https://fixture.invalid')
assert not r['is_error'], r
assert 'fixture browser evidence' in r['content'], r
Path('result').write_text(r['content'])
artifact = require('artifact')
r = await artifact.register(path='result', kind='file', description='browser evidence')
assert not r['is_error'], r
assert r['metadata']['artifact']['publication']['state'] == 'pending', r
ctx = require('ctx')
p = json.loads((await ctx.search(scope='campaign', kinds=['artifact'], query='evidence', limit=1))['content'])
assert p['resources'] == [] and p['next_cursor'] == 'next', p
p = json.loads((await ctx.search(scope='campaign', kinds=['artifact'], query='evidence', limit=1, cursor=p['next_cursor']))['content'])
assert len(p['resources']) == 1 and p['next_cursor'] is None, p
print('combined navigation passed')
await work.complete(summary='combined navigation passed', candidate_refs=[], unresolved_questions=[])
"#}).to_string(),
                        }], usage: Default::default(), finish_reason: None }
                    } else {
                        panic!(
                            "completion must not request another model boundary: {:?}",
                            request.messages
                        );
                    };
                    models += 1;
                    FrameReply::Model(Reply {
                        id: request.id,
                        completion: Some(completion),
                    })
                }
                FrameRequest::CpuJob(CpuJobRequest::Acquire { .. }) => {
                    assert!(active.is_none());
                    let permit = uuid::Uuid::new_v4();
                    active = Some(permit);
                    acquisitions += 1;
                    FrameReply::CpuJob(CpuJobReply::Granted {
                        permit,
                        device_ids: vec![],
                    })
                }
                FrameRequest::CpuJob(CpuJobRequest::Release { permit }) => {
                    assert_eq!(active.take(), Some(permit));
                    FrameReply::CpuJob(CpuJobReply::Released)
                }
                FrameRequest::Control(agents::Request::Resource {
                    request: context::Request::Search { query },
                }) => {
                    assert_eq!(query.limit, 1);
                    assert_eq!(
                        query.after.as_deref(),
                        if pages == 0 { None } else { Some("next") }
                    );
                    assert!(pages < 2);
                    let page = context::Page {
                        resources: vec![context::Resource {
                            reference: context::ResourceRef {
                                kind: if pages == 0 {
                                    context::ResourceKind::Trace
                                } else {
                                    context::ResourceKind::Artifact
                                },
                                work_id: "current".into(),
                                id: "host-descriptor".into(),
                                version: "v1".into(),
                            },
                            occurred_at_ms: None,
                            data: json!({"description":"evidence"}),
                        }],
                        next_cursor: if pages == 0 {
                            Some("next".into())
                        } else {
                            None
                        },
                    };
                    pages += 1;
                    FrameReply::Control(agents::Reply::Resource { page })
                }
                FrameRequest::Work(tachyon_api::work::Request::Complete {
                    summary,
                    candidate_refs,
                    unresolved_questions,
                }) => FrameReply::Work(tachyon_api::work::Reply::Proposed {
                    proposal: tachyon_api::work::CompletionProposal {
                        summary,
                        candidate_refs,
                        unresolved_questions,
                        instruction_revision: 1,
                    },
                }),
                // This fixture checks publication intent, not durable host storage.
                FrameRequest::ResourceUpload(_) => FrameReply::ResourceUpload(UploadReply::Denied),
                _ => panic!("unexpected broker frame"),
            };
            write_frame(&mut stream, &reply).await.unwrap();
        }
        assert_eq!(models, 1);
        assert_eq!(pages, 2);
        assert_eq!(
            acquisitions, 2,
            "browser read and work-end close both use host CPU admission"
        );
        assert!(active.is_none());
    });
    let output = tokio::time::timeout(Duration::from_secs(25), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    server.await.unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let events: Vec<Value> = stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect();
    let artifacts: Vec<_> = events
        .iter()
        .filter(|event| event["kind"] == "artifact_registered")
        .collect();
    assert_eq!(artifacts.len(), 1, "{stdout}");
    let artifact = &artifacts[0]["artifact"];
    assert_eq!(artifact["work_id"], "current");
    assert_eq!(artifact["attempt_id"], "attempt");
    assert_eq!(artifact["publication"]["state"], "pending");
    assert_eq!(artifact["path"], "result");
    let candidate = events
        .iter()
        .find(|e| e["kind"] == "work_candidate")
        .unwrap();
    let metadata = &candidate["candidate"]["final_context"];
    assert!(metadata["activated_packages"]["browser"].is_string());
    assert!(metadata["activated_packages"]["artifact"].is_string());
}
