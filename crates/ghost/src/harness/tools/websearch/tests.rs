use super::*;
use crate::harness::{
    backend::Local,
    registry::{builtins, packages::Packages},
    runtime::{
        NoopOutputStore, ToolEventSink, ToolIdentity, ToolPolicy, ToolRegistry, ToolTelemetry,
    },
    tools::webfetch,
};
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};
use tachyon_api::{
    agents::{Reply, Request},
    web::{Citation, WebResult, WEBFETCH},
};
use tachyon_model::broker::*;

#[derive(Default)]
struct Events(Mutex<Vec<(String, Value, ToolResult)>>);
impl ToolEventSink for Events {
    fn emit(&self, _: ToolTelemetry) {}
    fn record_result(&self, name: &str, _: &ToolContext, input: &Value, result: &ToolResult) {
        self.0
            .lock()
            .unwrap()
            .push((name.into(), input.clone(), result.clone()));
    }
    fn register_artifact(&self, _: tachyon_api::types::ArtifactRegistration) -> Result<(), String> {
        panic!("web lookup must not register artifacts")
    }
}

fn setup(
    root: &std::path::Path,
    client: Arc<BrokerClient>,
) -> (ToolRegistry, ToolContext, Arc<Events>) {
    let mut packages = Packages::default();
    packages.register(package(client.clone()).unwrap()).unwrap();
    packages
        .register(webfetch::package(client).unwrap())
        .unwrap();
    packages.register(builtins::ctx()).unwrap();
    let mut policy = ToolPolicy::worker_default(root.into());
    policy
        .enabled_tools
        .extend([WEBSEARCH.into(), WEBFETCH.into()]);
    // Native web lookup does not require process or filesystem capabilities.
    policy.capabilities.clear();
    let events = Arc::new(Events::default());
    let context = ToolContext {
        workspace_root: root.into(),
        cwd: root.into(),
        identity: ToolIdentity {
            work_id: Some("work".into()),
            task_id: Some("task".into()),
            call_id: Some("native".into()),
            ..Default::default()
        },
        deadline: Instant::now() + Duration::from_secs(30),
        cancellation: Default::default(),
        policy: Arc::new(policy),
        event_sink: events.clone(),
        output_store: Arc::new(NoopOutputStore),
        host_service: None,
    };
    (packages.into_registry(), context, events)
}

fn report(request: &WebRequest, status: WebStatus) -> WebResult {
    let url = "https://example.org/source";
    WebResult {
        usage: Default::default(),
        answer: "bounded report with evidence".into(),
        citations: vec![Citation {
            url: url.into(),
            title: Some("Source".into()),
            excerpt: None,
            source_index: None,
            start_index: None,
            end_index: None,
        }],
        annotations: vec![json!({"type":"url_citation","url_citation":{
            "url":url,"title":"Source","content":null,"source_index":null,"start_index":null,"end_index":null
        }})],
        status,
        notice: "Not proof of retrieval for each target".into(),
        host_observed_at: 42,
        observed_search_uses: Some(1),
        observed_fetch_uses: None,
        requested_urls: match request {
            WebRequest::Fetch { urls, .. } => urls.clone(),
            _ => vec![],
        },
    }
}

#[tokio::test]
async fn native_web_contract_policy_errors_retention_and_transcript() {
    let root = tempfile::tempdir().unwrap();
    let (host, mut client) = private_pair().unwrap();
    assert!(package(Arc::new(private_pair().unwrap().1)).is_none());
    client.controls = vec![Control::WebSearch];
    let client = Arc::new(client);
    assert!(webfetch::package(client.clone()).is_none());
    let mut client = Arc::try_unwrap(client).ok().unwrap();
    client.controls.push(Control::WebFetch);
    let (installed, mut context, events) = setup(root.path(), Arc::new(client));
    Arc::make_mut(&mut context.policy).max_model_content_bytes = 300;
    let registry = installed
        .for_work(&context.policy, &[], &Default::default())
        .unwrap();
    for (name, method) in [(WEBSEARCH, "search"), (WEBFETCH, "fetch")] {
        let info = registry.python_require(name, &context.policy).unwrap();
        assert_eq!(
            info["methods"],
            json!({method:{"tool":name,"input":{},"asynchronous":true}})
        );
        assert_eq!(
            info["schemas"][0]["parameters"],
            WebRequest::tool_parameters(name).unwrap()
        );
        assert!(info["schemas"][0]["parameters"]["properties"]
            .get("kind")
            .is_none());
        if name == WEBSEARCH {
            assert_eq!(
                info["schemas"][0]["parameters"]["required"],
                json!(["query"])
            );
            assert_eq!(
                info["schemas"][0]["parameters"]["properties"]["query"]["minLength"],
                1
            );
        }
        let mut denied = context.clone();
        Arc::make_mut(&mut denied.policy).enabled_tools.remove(name);
        assert!(!registry
            .definitions(&denied.policy)
            .iter()
            .any(|s| s.name == name));
        assert!(registry.python_require(name, &denied.policy).is_err());
        assert_eq!(
            registry
                .execute(name, &denied, json!({}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        assert!(registry.definitions(&denied.policy).iter().any(|s| s.name
            == if name == WEBSEARCH {
                WEBFETCH
            } else {
                WEBSEARCH
            }));
    }
    for (name, input) in [
        (WEBSEARCH, json!({"query":"x","api_key":"secret"})),
        (WEBSEARCH, json!({"query":"x","provider":"local"})),
        (WEBSEARCH, json!({"query":"x","unknown":true})),
        (WEBSEARCH, json!({"query":"x","kind":"fetch"})),
        (WEBSEARCH, json!({"query":"x","max_results":4})),
        (WEBSEARCH, json!({"query":7})),
        (WEBSEARCH, json!({"query":"x","caller_id":"foreign"})),
        (WEBFETCH, json!({"urls":[]})),
        (
            WEBFETCH,
            json!({"urls":["https://example.org"],"follow_links":true}),
        ),
        (
            WEBFETCH,
            json!({"urls":["https://example.org"],"budget":999}),
        ),
        (WEBFETCH, json!({"urls":"https://example.org"})),
    ] {
        assert_eq!(
            registry
                .execute(name, &context, input)
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::InvalidInput
        );
    }
    // Discovery, validation and native lookup need no browser, executable or files.
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    let server = tokio::spawn(async move {
        let mut stream = host.authenticate().await.unwrap();
        let mut previous = None;
        let mut turn = None;
        for n in 0..5 {
            let FrameRequest::Control(request) = read_frame(&mut stream).await.unwrap() else {
                panic!("not a web control")
            };
            let (command, search) = match request {
                Request::WebSearch { command } => (command, true),
                Request::WebFetch { command } => (command, false),
                _ => panic!("unexpected control"),
            };
            assert_eq!(command.caller_id, "work");
            if let Some(turn) = &turn {
                assert_eq!(&command.turn_id, turn);
            }
            turn = Some(command.turn_id.clone());
            if n < 2 {
                assert_eq!(
                    command.request,
                    WebRequest::Search {
                        query: "exact query".into(),
                        domains: Some(vec!["example.org".into()]),
                        max_results: 2
                    }
                );
                if let Some(previous) = &previous {
                    assert_eq!(&command, previous);
                }
                previous = Some(command.clone());
            } else {
                assert_eq!(
                    command.request,
                    WebRequest::Fetch {
                        urls: vec!["https://example.org/paper.pdf?x=1&y=%2F".into()],
                        instruction: Some("exact instruction".into()),
                        follow_links: Some(false),
                    }
                );
            }
            let reply = if n == 4 {
                Reply::Denied
            } else {
                let status = match n {
                    0 | 1 => WebStatus::Partial,
                    2 => WebStatus::Unverified,
                    _ => WebStatus::Failed,
                };
                let result = report(&command.request, status);
                if search {
                    Reply::WebSearch { command, result }
                } else {
                    Reply::WebFetch { command, result }
                }
            };
            write_frame(&mut stream, &FrameReply::Control(reply))
                .await
                .unwrap();
        }
    });
    for n in 0..2 {
        let mut limited = context.clone();
        if n == 1 {
            Arc::make_mut(&mut limited.policy).max_return_bytes = 128;
        }
        let result = registry
            .execute(
                WEBSEARCH,
                &limited,
                json!({"query":"exact query","domains":["example.org"],"max_results":2}),
            )
            .await
            .unwrap();
        assert!(!result.is_error);
        assert_eq!(result.truncated, n == 1);
        assert_eq!(result.metadata["status"], "partial");
        let reference = result.output_ref.as_ref().unwrap();
        let stored = registry
            .work_outputs()
            .unwrap()
            .page(reference, 0, 8192)
            .await
            .unwrap();
        assert!(stored.content.contains("bounded report with evidence"));
        assert!(stored.content.contains("https://example.org/source"));
        assert!(result.to_json(300).len() <= 300);
    }
    let input = json!({"urls":["https://example.org/paper.pdf?x=1&y=%2F"],"instruction":"exact instruction","follow_links":false});
    for (n, status) in ["unverified", "failed"].into_iter().enumerate() {
        let result = registry
            .execute(
                WEBFETCH,
                &context.for_call(format!("fetch-{n}")),
                input.clone(),
            )
            .await
            .unwrap();
        assert_eq!(result.is_error, n == 1);
        assert_eq!(result.metadata["status"], status);
        let value: Value = serde_json::from_str(&result.content).unwrap();
        assert_eq!(value["requested_urls"], input["urls"]);
        assert_eq!(value["citations"][0]["url"], "https://example.org/source");
    }
    assert_eq!(
        registry
            .execute(WEBFETCH, &context.for_call("revoked"), input.clone())
            .await
            .unwrap_err()
            .code,
        ToolErrorCode::Io
    );
    server.await.unwrap();
    assert_eq!(
        registry
            .execute(WEBFETCH, &context.for_call("closed"), input)
            .await
            .unwrap_err()
            .code,
        ToolErrorCode::Io
    );
    let recorded = events.0.lock().unwrap();
    assert!(recorded
        .iter()
        .any(|(n, _, r)| n == WEBSEARCH && r.content.contains("url_citation")));
    assert!(recorded.last().unwrap().2.is_error);
    drop(recorded);
    registry.finish_work().await;
}

#[tokio::test]
#[ignore = "requires existing IPython; no installation or live web provider"]
async fn actual_ipython_web_thin_proxies_make_two_exact_native_calls() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Local::new(root.path()));
    backend.check_ipython().expect("existing IPython required");
    let (host, mut client) = private_pair().unwrap();
    client.controls = vec![Control::WebSearch, Control::WebFetch];
    let (mut installed, mut context, events) = setup(root.path(), Arc::new(client));
    installed
        .register(crate::harness::runtime::IpythonTool::new(backend))
        .unwrap();
    Arc::make_mut(&mut context.policy)
        .capabilities
        .insert(crate::harness::runtime::Capability::ExecuteProcess);
    let registry = installed
        .for_work(&context.policy, &[], &Default::default())
        .unwrap();
    let server = tokio::spawn(async move {
        let mut stream = host.authenticate().await.unwrap();
        let mut turn = None;
        for n in 0..2 {
            let FrameRequest::Control(request) = read_frame(&mut stream).await.unwrap() else {
                panic!("no process/browser/network controls expected")
            };
            let command = match request {
                Request::WebSearch { command } if n == 0 => command,
                Request::WebFetch { command } if n == 1 => command,
                _ => panic!("wrong native operation"),
            };
            assert_eq!(command.caller_id, "work");
            assert!(command.tool_call_id.starts_with("python-1-"));
            if let Some(turn) = &turn {
                assert_eq!(&command.turn_id, turn);
            }
            turn = Some(command.turn_id.clone());
            let result = report(&command.request, WebStatus::Partial);
            let reply = if n == 0 {
                assert_eq!(
                    command.request,
                    WebRequest::Search {
                        query: "exact query".into(),
                        domains: Some(vec!["example.org".into()]),
                        max_results: 2
                    }
                );
                Reply::WebSearch { command, result }
            } else {
                assert_eq!(
                    command.request,
                    WebRequest::Fetch {
                        urls: vec!["https://example.org/paper.pdf?x=1&y=%2F".into()],
                        instruction: Some("exact instruction".into()),
                        follow_links: Some(false)
                    }
                );
                Reply::WebFetch { command, result }
            };
            write_frame(&mut stream, &FrameReply::Control(reply))
                .await
                .unwrap();
        }
    });
    let result = registry.execute("ipython", &context.for_call("cell"), json!({"code":r#"
import json
s = require('websearch')
f = require('webfetch')
assert not hasattr(s, 'fetch') and not hasattr(f, 'search')
for bad in [{'query':'x', 'api_key':'secret'}, {'query':'x', 'kind':'fetch'}]:
    try:
        await s.search(**bad)
        assert False, 'invalid arguments accepted'
    except RuntimeError as error:
        assert error.args[0]['code'] == 'invalid_input', error
a = await s.search(query='exact query', domains=['example.org'], max_results=2)
b = await f.fetch(urls=['https://example.org/paper.pdf?x=1&y=%2F'], instruction='exact instruction', follow_links=False)
for r in [a,b]:
    assert not r['is_error'] and r['metadata']['status'] == 'partial', r
    p = json.loads(r['content'])
    assert p['status'] == 'partial' and p['citations'][0]['url'] == 'https://example.org/source', p
assert json.loads(b['content'])['requested_urls'] == ['https://example.org/paper.pdf?x=1&y=%2F']
print('web proxies passed')
"#})).await.unwrap();
    assert!(!result.is_error, "{result:?}");
    assert!(result.content.contains("web proxies passed"));
    server.await.unwrap();
    assert_eq!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, _, r)| (n == WEBSEARCH || n == WEBFETCH) && !r.is_error)
            .count(),
        2
    );
    registry.finish_work().await;
}
