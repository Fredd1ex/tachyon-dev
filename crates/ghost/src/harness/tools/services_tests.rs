use super::services::package;
use crate::harness::runtime::{ToolContext, ToolErrorCode};
use crate::harness::{
    backend::Local,
    profiles,
    runtime::{BrowserAvailability, NoopEventSink, NoopOutputStore, ToolIdentity, ToolPolicy},
};
use serde_json::{json, Value};
use std::sync::Arc;
use tachyon_api::agents::{Control, Reply, Request};
use tachyon_api::{agents::services::*, monitor::*, todo::TodoError};
use tachyon_model::broker::*;

#[tokio::test]
async fn service_native_python_parity_and_instruction_only_reset() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Local::new(root.path()));
    let python = backend.check_ipython().is_ok();
    let mut policy = ToolPolicy::worker_default(root.path().into());
    policy
        .enabled_tools
        .extend(["todo".into(), "monitor".into()]);
    let context = ToolContext {
        workspace_root: root.path().into(),
        cwd: root.path().into(),
        identity: ToolIdentity::default(),
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
        cancellation: Default::default(),
        policy: Arc::new(policy),
        event_sink: Arc::new(NoopEventSink),
        output_store: Arc::new(NoopOutputStore),
        host_service: None,
    };
    let (host, mut client) = private_pair().unwrap();
    client.controls = vec![Control::Todo, Control::Monitor];
    let client = Arc::new(client);
    let mut packages =
        profiles::worker(backend, BrowserAvailability::Unavailable("fixture".into()));
    packages.register(package(client.clone(), true)).unwrap();
    packages.register(package(client, false)).unwrap();
    let installed = packages.into_registry();
    let registry = installed
        .for_work(&context.policy, &[], &Default::default())
        .unwrap();
    let info = registry.python_require("todo", &context.policy).unwrap();
    assert_eq!(
        info["methods"]["add"],
        json!({"tool":"todo","input":{"action":"add"},"asynchronous":true})
    );
    assert_eq!(info["methods"].as_object().unwrap().len(), 3);
    let monitor = registry.python_require("monitor", &context.policy).unwrap();
    assert_eq!(monitor["methods"].as_object().unwrap().len(), 1);
    assert!(monitor["methods"].get("cancel").is_none());
    let server = tokio::spawn(async move {
        let mut stream = host.authenticate().await.unwrap();
        for _ in 0..if python { 2 } else { 1 } {
            for n in 0..2 {
                let FrameRequest::Control(request) = read_frame(&mut stream).await.unwrap() else {
                    panic!()
                };
                let reply = match (n, request) {
                    (
                        0,
                        Request::Todo {
                            request:
                                TodoRequest::Add {
                                    scope: Scope::CurrentWork,
                                    command_id,
                                    expected_revision: 0,
                                    title,
                                    description,
                                },
                        },
                    ) => {
                        assert_eq!(command_id, "stable");
                        assert_eq!(title, "plan");
                        assert!(description.is_empty());
                        Reply::Todo {
                            scope: tachyon_api::todo::TodoScope::Work {
                                work_id: "w".into(),
                            },
                            result: Err(TodoError::RevisionConflict {
                                current_revision: 7,
                            }),
                        }
                    }
                    (
                        1,
                        Request::Monitor {
                            request:
                                MonitorRequest::Snapshot {
                                    scope: Scope::CurrentWork,
                                    after: None,
                                    limit: 16,
                                },
                        },
                    ) => Reply::Monitor {
                        query: MonitorQuery {
                            scope: MonitorScope::Work {
                                campaign_id: "c".into(),
                                work_id: "w".into(),
                            },
                            after: None,
                            limit: 16,
                        },
                        result: Ok(MonitorPayload {
                            durable: Durable {
                                sampled_at_ms: 42,
                                inference: Observed::Unknown,
                                ..Default::default()
                            },
                            capacities: vec![],
                            registered: Registered::default(),
                        }),
                    },
                    _ => panic!("unexpected service input"),
                };
                write_frame(&mut stream, &FrameReply::Control(reply))
                    .await
                    .unwrap();
            }
        }
    });
    let failure = registry
        .execute(
            "todo",
            &context,
            json!({"action":"add","title":"plan","command_id":"stable","expected_revision":0}),
        )
        .await
        .unwrap_err();
    assert_eq!(failure.code, ToolErrorCode::Conflict);
    assert_eq!(
        failure.metadata,
        json!({"kind":"revision_conflict","current_revision":7})
    );
    let sample = registry
        .execute("monitor", &context, json!({"action":"snapshot"}))
        .await
        .unwrap();
    let sample: Value = serde_json::from_str(&sample.content).unwrap();
    assert_eq!(
        sample["payload"]["durable"]["inference"]["knowledge"],
        "unknown"
    );
    assert_eq!(
        sample["payload"]["durable"]["native_jobs"]["finalized"],
        "0"
    );
    if python {
        let result = registry
            .execute(
                "ipython",
                &context,
                json!({"code":r#"
import json
t = require('todo')
m = require('monitor')
try:
    await t.add(title='plan', command_id='stable', expected_revision=0)
    assert False, 'expected structured conflict'
except RuntimeError as error:
    r = error.args[0]
    assert r['code'] == 'conflict', r
    assert r['metadata'] == {'kind':'revision_conflict','current_revision':7}, r
r = json.loads((await m.snapshot())['content'])
assert r['payload']['durable']['sampled_at_ms'] == 42, r
assert r['payload']['durable']['inference']['knowledge'] == 'unknown', r
assert r['payload']['durable']['native_jobs']['finalized'] == '0', r
assert not hasattr(m, 'cancel')
"#}),
            )
            .await
            .unwrap();
        assert!(!result.is_error, "{result:?}");
    } else {
        eprintln!("SKIP Python parity: existing IPython unavailable");
    }
    server.await.unwrap();
    for (name, input) in [
        ("todo", json!({"action":"list","scope":"current_campaign"})),
        ("todo", json!({"action":"list","work_id":"foreign"})),
        (
            "todo",
            json!({"action":"add","title":"missing concurrency fields"}),
        ),
        ("monitor", json!({"action":"cancel"})),
        ("monitor", json!({"action":"snapshot","scope":"host"})),
    ] {
        assert!(registry.execute(name, &context, input).await.is_err());
    }
    let reset = installed
        .for_work(&context.policy, &[], &Default::default())
        .unwrap();
    assert_eq!(
        reset
            .activation_snapshot()
            .packages
            .get("todo")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(reset.activation_snapshot().packages.len(), 2);
    // Activation only restores instructions. No broker query occurred on reset.
    reset.finish_work().await;
    registry.finish_work().await;
}
