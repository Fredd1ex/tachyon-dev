use super::*;
use tachyon_model::broker::{CpuJobReply as Reply, CpuJobRequest as Request};

#[tokio::test]
async fn cpu_job_polling_does_not_block_release_or_consume_regular_request_budget() {
    let (_dir, mut store, funding, request) = tests::setup();
    store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(
        tachyon_util::config::ResourceLimits {
            max_cpu_jobs: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let store = Arc::new(store);
    let permit = store
        .host_issue_model_permit(request.clone(), funding, None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let (host, client) = tachyon_model::broker::private_pair().unwrap();
    let (other_host, other) = tachyon_model::broker::private_pair().unwrap();
    let worker = async {
        assert!(matches!(
            client.cpu_job(Request::Profile).await.unwrap(),
            Reply::Profile {
                max_cpu_jobs: 1,
                max_gpu_jobs: 0
            }
        ));
        let Reply::Acquired { permit: cpu } = client.cpu_job(Request::TryAcquire).await.unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            other
                .cpu_job(Request::Release { permit: cpu })
                .await
                .unwrap(),
            Reply::Denied
        ));
        // Poll more times than the old entire session frame budget. Both this
        // channel and a sibling share one host ceiling, with immediate replies.
        for _ in 0..400 {
            assert!(matches!(
                client.cpu_job(Request::TryAcquire).await.unwrap(),
                Reply::Busy
            ));
            assert!(matches!(
                other.cpu_job(Request::TryAcquire).await.unwrap(),
                Reply::Busy
            ));
        }
        assert!(matches!(
            client
                .cpu_job(Request::Release { permit: cpu })
                .await
                .unwrap(),
            Reply::Released
        ));
        assert!(matches!(
            client
                .cpu_job(Request::Release { permit: cpu })
                .await
                .unwrap(),
            Reply::Released
        ));
        assert!(matches!(
            other
                .cpu_job(Request::Release { permit: cpu })
                .await
                .unwrap(),
            Reply::Denied
        ));
        // The first queued session now wins rather than a polling race.
        assert!(matches!(
            other.cpu_job(Request::TryAcquire).await.unwrap(),
            Reply::Busy
        ));
        let Reply::Acquired { permit: queued } = client.cpu_job(Request::TryAcquire).await.unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            client
                .cpu_job(Request::Release { permit: queued })
                .await
                .unwrap(),
            Reply::Released
        ));
        let Reply::Acquired { permit: cpu } = other.cpu_job(Request::TryAcquire).await.unwrap()
        else {
            panic!()
        };
        store.host_revoke_model_permit(&permit).unwrap();
        assert!(matches!(
            other.cpu_job(Request::TryAcquire).await.unwrap(),
            Reply::Denied
        ));
        assert!(matches!(
            other
                .cpu_job(Request::Release { permit: cpu })
                .await
                .unwrap(),
            Reply::Released
        ));
        assert_eq!(store.host_capacity.cpu.available_permits(), 1);
        drop(client);
        drop(other);
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let (first, second, ()) = tokio::join!(
        broker.serve_private(host, &permit, request.clone(), deadline),
        broker.serve_private(other_host, &permit, request.clone(), deadline),
        worker,
    );
    assert!(first.is_err() && second.is_err());
    assert_eq!(store.host_capacity.cpu.available_permits(), 1);
}

#[tokio::test]
async fn cpu_job_disconnect_retains_unconfirmed_capacity() {
    let (_dir, store, funding, request) = tests::setup();
    let store = Arc::new(store);
    let permit = store
        .host_issue_model_permit(request.clone(), funding, None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let (host, client) = tachyon_model::broker::private_pair().unwrap();
    let worker = async move {
        assert!(matches!(
            client.cpu_job(Request::TryAcquire).await.unwrap(),
            Reply::Acquired { .. }
        ));
    };
    let (served, ()) = tokio::join!(
        broker.serve_private(
            host,
            &permit,
            request,
            Instant::now() + Duration::from_secs(5)
        ),
        worker,
    );
    assert!(served.is_err());
    assert_eq!(store.host_capacity.cpu.available_permits(), 1);
}

#[tokio::test]
async fn cpu_job_exec_waits_without_spawning_and_releases_after_async_cleanup() {
    use ghost::harness::runtime::*;
    use serde_json::json;
    let (dir, mut store, funding, request) = tests::setup();
    store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(
        tachyon_util::config::ResourceLimits {
            max_cpu_jobs: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let store = Arc::new(store);
    let permit = store
        .host_issue_model_permit(request.clone(), funding, None)
        .unwrap();
    let broker = ModelBroker::new(store.clone(), model(&request));
    let (host, client) = tachyon_model::broker::private_pair().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let mut policy = ToolPolicy::worker_default(root.clone());
    policy.exec_term_grace = Duration::from_millis(200);
    policy.max_exec_output_bytes = 8192;
    let context = ToolContext {
        workspace_root: root.clone(),
        cwd: root.clone(),
        identity: ToolIdentity::default(),
        deadline: std::time::Instant::now() + Duration::from_secs(10),
        cancellation: tokio_util::sync::CancellationToken::new(),
        policy: Arc::new(policy),
        event_sink: Arc::new(NoopEventSink),
        output_store: Arc::new(NoopOutputStore),
        host_service: Some(Arc::new(client)),
    };
    let worker_store = store.clone();
    let worker = async move {
        let client = context.host_service.as_ref().unwrap();
        let Reply::Acquired { permit: held } = client.cpu_job(Request::TryAcquire).await.unwrap()
        else {
            panic!()
        };
        let tool = ExecTool::new();
        let error = tool
            .execute(
                &context,
                json!({"command":"touch forbidden", "timeout_ms":80}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Timeout);
        assert!(!root.join("forbidden").exists());
        let mut cancelled = context.for_call("cancelled");
        assert!(Arc::ptr_eq(
            cancelled.host_service.as_ref().unwrap(),
            client
        ));
        cancelled.cancellation = context.cancellation.child_token();
        let cancel = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancelled.cancellation.cancel();
        };
        let (result, ()) = tokio::join!(
            tool.execute(&cancelled, json!({"command":"touch forbidden"})),
            cancel
        );
        assert_eq!(result.unwrap_err().code, ToolErrorCode::Cancelled);
        assert!(!root.join("forbidden").exists());
        // The same invocation is deliberately not gated in an ordinary context.
        let mut local = context.clone();
        local.host_service = None;
        assert!(
            !tool
                .execute(&local, json!({"argv":["/bin/true"]}))
                .await
                .unwrap()
                .is_error
        );
        assert!(matches!(
            client
                .cpu_job(Request::Release { permit: held })
                .await
                .unwrap(),
            Reply::Released
        ));

        let registry = native_registry()
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let first = registry
            .execute(
                "exec",
                &context,
                json!({"action":"start", "command":"sleep 30 & echo $! > running; wait"}),
            )
            .await
            .unwrap();
        while !root.join("running").exists() {
            assert!(std::time::Instant::now() < context.deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let second = registry
            .execute(
                "exec",
                &context,
                json!({"action":"start", "command":"touch second"}),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!root.join("second").exists());
        let queued = registry
            .execute(
                "exec",
                &context,
                json!({"action":"start", "command":"touch forbidden"}),
            )
            .await
            .unwrap();
        registry
            .execute(
                "exec",
                &context,
                json!({"action":"cancel", "operation":queued.metadata["operation"]}),
            )
            .await
            .unwrap();
        let queued = registry
            .execute(
                "exec",
                &context,
                json!({"action":"wait", "operation":queued.metadata["operation"], "wait_ms":1000}),
            )
            .await
            .unwrap();
        assert_eq!(queued.metadata["state"], "cancelled", "{queued:?}");
        assert!(!root.join("forbidden").exists());
        assert_eq!(worker_store.host_capacity.cpu.available_permits(), 0);
        registry
            .execute(
                "exec",
                &context,
                json!({"action":"cancel", "operation":first.metadata["operation"]}),
            )
            .await
            .unwrap();
        let done = registry
            .execute(
                "exec",
                &context,
                json!({"action":"wait", "operation":second.metadata["operation"], "wait_ms":3000}),
            )
            .await
            .unwrap();
        assert_eq!(done.metadata["state"], "completed", "{done:?}");
        assert!(root.join("second").exists());
        let first = registry
            .execute(
                "exec",
                &context,
                json!({"action":"status", "operation":first.metadata["operation"]}),
            )
            .await
            .unwrap();
        assert_eq!(first.metadata["state"], "cancelled", "{first:?}");
        registry.finish_work().await;

        // Failed spawn releases without pretending a process ever existed.
        assert!(tool
            .execute(&context, json!({"argv":["/missing-cpu-test-executable"]}))
            .await
            .is_err());
        assert!(
            !tool
                .execute(&context, json!({"argv":["/bin/true"], "timeout_ms":1000}))
                .await
                .unwrap()
                .is_error
        );
        drop(context);
    };
    let (served, ()) = tokio::join!(
        broker.serve_private(
            host,
            &permit,
            request,
            Instant::now() + Duration::from_secs(12)
        ),
        worker,
    );
    assert!(served.is_err());
    assert_eq!(store.host_capacity.cpu.available_permits(), 1);
}
