use super::*;
use crate::harness::{
    backend::Local,
    profiles,
    runtime::{BrowserAvailability, NoopEventSink, NoopOutputStore, ToolIdentity, ToolPolicy},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tachyon_api::{
    agents::{Control, Reply, Request},
    context::{self, Page, Resource, ResourceKind, ResourceRef},
};
use tachyon_model::broker::{private_pair, read_frame, write_frame, FrameReply, FrameRequest};

#[tokio::test]
async fn scoped_pages_share_history_authority_and_preserve_live_navigation() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Local::new(root.path()));
    let python = backend.check_ipython().is_ok();
    let mut context = ToolContext {
        workspace_root: root.path().into(),
        cwd: root.path().into(),
        identity: ToolIdentity {
            work_id: Some("current".into()),
            ..Default::default()
        },
        deadline: Instant::now() + Duration::from_secs(20),
        cancellation: Default::default(),
        policy: Arc::new(ToolPolicy::worker_default(root.path().into())),
        event_sink: Arc::new(NoopEventSink),
        output_store: Arc::new(NoopOutputStore),
        host_service: None,
    };
    let (host, mut client) = private_pair().unwrap();
    client.controls = vec![Control::Resource];
    let mut packages = profiles::worker(backend, BrowserAvailability::Unavailable("test".into()));
    packages
        .register(crate::harness::tools::history::package(Arc::new(client)))
        .unwrap();
    Arc::make_mut(&mut context.policy)
        .enabled_tools
        .insert("history".into());
    let registry = packages
        .into_registry()
        .for_work(&context.policy, &[], &Default::default())
        .unwrap();
    let kinds = [
        ResourceKind::Attempt,
        ResourceKind::Finding,
        ResourceKind::Artifact,
        ResourceKind::Trace,
        ResourceKind::Document,
    ];
    let resources: Vec<_> = kinds
        .into_iter()
        .enumerate()
        .map(|(i, kind)| Resource {
            reference: ResourceRef {
                kind,
                work_id: if i == 0 { "other" } else { "current" }.into(),
                id: format!("evidence-{i}"),
                version: "v1".into(),
            },
            occurred_at_ms: None,
            data: json!({"observation":"needle"}),
        })
        .collect();
    let expected = resources.clone();
    let server = tokio::spawn(async move {
        let mut stream = host.authenticate().await.unwrap();
        for n in 0..(6 + usize::from(python)) {
            let FrameRequest::Control(Request::Resource {
                request: context::Request::Search { query },
            }) = read_frame(&mut stream).await.unwrap()
            else {
                panic!("expected typed resource query")
            };
            assert_eq!(query.limit, 8);
            assert_eq!(
                query.after.as_deref(),
                if n == 2 || n == 5 {
                    Some("stable-host-cursor")
                } else {
                    None
                }
            );
            assert_eq!(
                query.literal.as_deref(),
                if n >= 2 { Some("needle") } else { None }
            );
            let mut page = Page {
                resources: resources.clone(),
                next_cursor: Some("stable-host-cursor".into()),
            };
            if n == 4 || n == 5 {
                for resource in &mut page.resources {
                    resource.reference.kind = if n == 4 {
                        ResourceKind::Finding
                    } else {
                        ResourceKind::Document
                    };
                    resource.data = json!({"observation":"needle", "padding":"x".repeat(1200)});
                }
                if n == 5 {
                    page.next_cursor = None;
                }
                assert!(serde_json::to_vec(&page).unwrap().len() <= context::MAX_PAGE_BYTES);
            }
            write_frame(&mut stream, &FrameReply::Control(Reply::Resource { page }))
                .await
                .unwrap();
        }
    });
    let live = registry
        .work_outputs()
        .unwrap()
        .put("live needle".into())
        .await
        .unwrap();
    let result = registry
        .execute("ctx", &context, json!({"action":"list"}))
        .await
        .unwrap();
    assert_eq!(result.metadata["references"], json!([live]));
    let result = registry
        .execute(
            "ctx",
            &context,
            json!({"action":"search","reference":live,"query":"needle"}),
        )
        .await
        .unwrap();
    assert!(!result.is_error);
    for (input, expected_count) in [
        (json!({"action":"list","scope":"campaign","limit":8}), 5),
        (json!({"action":"list","scope":"current_work","limit":8}), 4),
        (
            json!({"action":"search","scope":"campaign","kinds":["document","artifact"],"query":"needle","cursor":"stable-host-cursor","limit":8}),
            2,
        ),
        (
            json!({"action":"search","scope":"current_work","kinds":["attempt"],"query":"needle","limit":8}),
            0,
        ),
    ] {
        let result = registry.execute("ctx", &context, input).await.unwrap();
        assert!(result.content.len() <= context::MAX_PAGE_BYTES);
        let page: Page = serde_json::from_str(&result.content).unwrap();
        assert_eq!(page.resources.len(), expected_count);
        assert_eq!(page.next_cursor.as_deref(), Some("stable-host-cursor"));
        for resource in page.resources {
            assert!(expected.contains(&resource));
        }
    }
    // Filtering a full host page to nothing must still advance its cursor, not
    // report exhaustion or fetch unbounded extra pages behind the caller's back.
    let mut cursor = None;
    let mut seen = std::collections::BTreeSet::new();
    for n in 0..2 {
        let result = registry
            .execute(
                "ctx",
                &context,
                json!({
                    "action":"search", "scope":"campaign", "kinds":["document"],
                    "query":"needle", "limit":8, "cursor":cursor
                }),
            )
            .await
            .unwrap();
        assert!(result.content.len() <= context::MAX_PAGE_BYTES);
        let page: Page = serde_json::from_str(&result.content).unwrap();
        assert_eq!(page.resources.len(), if n == 0 { 0 } else { 5 });
        for resource in page.resources {
            assert_eq!(resource.reference.kind, ResourceKind::Document);
            assert!(seen.insert(resource.reference.id));
        }
        cursor = page.next_cursor;
        assert_eq!(cursor.is_some(), n == 0);
    }
    assert_eq!(seen.len(), 5);
    if python {
        let result = registry.execute("ipython", &context, json!({"code":r#"
import json
ctx = require('ctx')
p = json.loads((await ctx.search(query='needle', scope='campaign', kinds=['trace'], limit=8))['content'])
assert len(p['resources']) == 1
assert p['resources'][0]['reference']['kind'] == 'trace'
assert p['next_cursor'] == 'stable-host-cursor'
"#})).await.unwrap();
        assert!(!result.is_error, "{result:?}");
    } else {
        eprintln!("SKIP: IPython unavailable (native scoped checks still ran)");
    }
    server.await.unwrap();
    for invalid in [
        json!({"action":"list","scope":"other"}),
        json!({"action":"list","scope":"campaign","campaign":"other"}),
        json!({"action":"list","scope":"campaign","cursor":0}),
        json!({"action":"list","scope":"campaign","limit":17}),
        json!({"action":"list","scope":"campaign","kinds":["observation"]}),
        json!({"action":"list","scope":"campaign","kinds":["trace","trace"]}),
        json!({"action":"search","scope":"campaign","query":""}),
    ] {
        assert!(registry.execute("ctx", &context, invalid).await.is_err());
    }
    Arc::make_mut(&mut context.policy)
        .enabled_tools
        .remove("history");
    for scope in ["campaign", "current_work"] {
        let error = registry
            .execute("ctx", &context, json!({"action":"list","scope":scope}))
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            crate::harness::runtime::ToolErrorCode::PermissionDenied
        );
    }
    assert!(registry
        .execute("ctx", &context, json!({"action":"list"}))
        .await
        .is_ok());
    registry.finish_work().await;

    // Installing/enabling history without the host Resource action is not a grant.
    let (host, client) = private_pair().unwrap();
    drop(host);
    let mut packages = crate::harness::registry::builtins::native();
    packages
        .register(crate::harness::tools::history::package(Arc::new(client)))
        .unwrap();
    Arc::make_mut(&mut context.policy)
        .enabled_tools
        .insert("history".into());
    let registry = packages
        .into_registry()
        .for_work(&context.policy, &[], &Default::default())
        .unwrap();
    let error = registry
        .execute("ctx", &context, json!({"action":"list","scope":"campaign"}))
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        crate::harness::runtime::ToolErrorCode::PermissionDenied
    );
    registry.finish_work().await;
}
