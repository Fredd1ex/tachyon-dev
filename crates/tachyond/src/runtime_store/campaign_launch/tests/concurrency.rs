use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn startup_reconciles_multiple_bounded_pages_without_replaying() {
    let (dir, store, first) = fixture();
    let ApiResponse::Research { research } = store
        .research_request(&ApiRequest::ResearchCreate {
            command_id: "history".into(),
            title: "history".into(),
            objective: "fixture".into(),
        })
        .unwrap()
    else {
        panic!()
    };
    let mut manifests = vec![first];
    for index in 0..65 {
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: format!("historical-{index}"),
                research_id: research.id.clone(),
                title: "history".into(),
                objective: "fixture".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let mut manifest = manifests[0].clone();
        manifest.campaign_id = campaign.id;
        manifests.push(manifest);
    }
    let tx = store.database.begin_write().unwrap();
    for manifest in &manifests {
        tx.open_table(LAUNCHES)
            .unwrap()
            .insert(
                manifest.campaign_id.as_str(),
                serde_json::to_vec(&Launch {
                    schema_version: 1,
                    manifest: manifest.clone(),
                    base_url: "http://127.0.0.1".into(),
                    manifest_sha256: Some(Launch::digest(manifest).unwrap()),
                })
                .unwrap()
                .as_slice(),
            )
            .unwrap();
        RuntimeStore::set_campaign_status_in(&tx, &manifest.campaign_id, CampaignStatus::Running)
            .unwrap();
    }
    tx.commit().unwrap();
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    for manifest in &manifests {
        assert_eq!(status(&store, manifest), CampaignStatus::Interrupted);
        assert!(store
            .campaign_ledger(&manifest.campaign_id)
            .unwrap()
            .is_none());
    }
    assert!(service.active.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake provider only"]
async fn actual_simultaneous_campaigns_share_capacity_and_cancel_independently() {
    for (resident_cap, model_cap, execution_cap, cancel_first, cpu_cap) in [
        (1, 2, 2, false, 2),
        (2, 1, 2, false, 2),
        (2, 2, 2, false, 2),
        (2, 2, 2, true, 2),
        (2, 2, 1, false, 2),
        (2, 2, 2, false, 1),
    ] {
        let (dir, store, mut first) = fixture();
        let mut store = Arc::try_unwrap(store).ok().unwrap();
        store.host_capacity = super::super::super::host_capacity::HostCapacity::new(
            tachyon_util::config::ResourceLimits {
                max_campaigns: 2,
                max_resident_workers: resident_cap,
                max_execution_jobs: execution_cap,
                max_model_calls: model_cap,
                max_cpu_jobs: cpu_cap,
                ..Default::default()
            },
        )
        .unwrap();
        let store = Arc::new(store);
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: "second-research".into(),
                title: "second".into(),
                objective: "fixture".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: "second-campaign".into(),
                research_id: research.id,
                title: "second".into(),
                objective: "fixture".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let workspace1 = tempfile::tempdir().unwrap();
        let workspace2 = tempfile::tempdir().unwrap();
        let home1 = tempfile::tempdir().unwrap();
        let home2 = tempfile::tempdir().unwrap();
        first.executable =
            PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"))
                .canonicalize()
                .unwrap();
        first.workspace = workspace1.path().canonicalize().unwrap();
        first.home = home1.path().canonicalize().unwrap();
        if cpu_cap == 1 {
            first.compute = Some(tachyon_api::campaign::ComputeEnvelope {
                profiles: Default::default(),
                cpu_job_ms: 15000,
                gpu_job_ms: 0,
                max_gpu_jobs: 0,
                max_cpu_timeout_ms: 15000,
                max_gpu_timeout_ms: 0,
            });
        }
        let mut second = first.clone();
        second.campaign_id = campaign.id;
        second.workspace = workspace2.path().canonicalize().unwrap();
        second.home = home2.path().canonicalize().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let (release, gate) = watch::channel(false);
        let http = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..if cpu_cap == 1 { 4 } else { 2 } {
                let (mut socket, _) = listener.accept().await.unwrap();
                let seen = seen.clone();
                let mut gate = gate.clone();
                tasks.spawn(async move {
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        header.push(socket.read_u8().await.unwrap());
                        assert!(header.len() < 16384);
                    }
                    let header = String::from_utf8(header).unwrap();
                    let length: usize = header.lines().find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
                    }).unwrap();
                    assert!(length <= 32000);
                    socket.read_exact(&mut vec![0; length]).await.unwrap();
                    let index = seen.fetch_add(1, Ordering::SeqCst);
                    while !*gate.borrow_and_update() { gate.changed().await.unwrap(); }
                    let delta = if cpu_cap == 1 && index < 2 {
                        serde_json::json!({"tool_calls":[{"index":0,"id":"cpu-job","function":{"name":"exec","arguments":serde_json::json!({"command":"touch cpu-running; while [ ! -f cpu-exit ]; do sleep 0.02; done", "timeout_ms":15000}).to_string()}}]})
                    } else {
                        serde_json::json!({"content":"fixture complete"})
                    };
                    let response = format!("data: {}\n\ndata: [DONE]\n\n", serde_json::json!({"choices":[{"delta":delta}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.000007}}));
                    let _ = socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await;
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        });
        let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
        for manifest in [&first, &second] {
            let model = Model::new(ModelConfig {
                base_url: base_url.clone(),
                api_key: "local-fixture".into(),
                model: manifest.model.clone(),
                temperature: 0.0,
                max_completion_tokens: Some(manifest.output_tokens),
                context_length: None,
                parallel_tool_calls: false,
                reasoning: Default::default(),
                routing: None,
                debug: false,
                debug_log: None,
            });
            let run_store = store.clone();
            let root = dir.path().to_owned();
            service
                .launch(manifest, base_url.clone(), false, move |launch, cancel| {
                    execute(run_store, root, launch, model, cancel)
                })
                .unwrap();
        }
        let expected = resident_cap.min(model_cap).min(execution_cap);
        tokio::time::timeout(Duration::from_secs(20), async {
            while requests.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(requests.load(Ordering::SeqCst), expected);
        assert_eq!(service.active.lock().unwrap().len(), 2);
        for m in [&first, &second] {
            assert!(
                matches!(service.progress(&m.campaign_id).unwrap(), ApiResponse::CampaignProgress { activity, .. } if activity.owned)
            );
        }
        if resident_cap == 1 {
            let mut waiting = 0;
            let mut records = 0;
            for m in [&first, &second] {
                waiting += store
                    .host_capacity
                    .resident
                    .waiting(&m.campaign_id)
                    .unwrap();
                records += usize::from(
                    store
                        .campaign_execution(&m.campaign_id, &format!("{}-root", m.campaign_id))
                        .unwrap()
                        .is_some(),
                );
            }
            assert_eq!(waiting, 1);
            assert_eq!(
                records, 1,
                "capacity waiting must not create an unknown execution"
            );
        }
        if resident_cap == 2 && model_cap == 1 {
            assert_eq!(
                [&first, &second]
                    .iter()
                    .map(|m| store.host_capacity.model.waiting(&m.campaign_id).unwrap())
                    .sum::<usize>(),
                1
            );
        }
        if execution_cap == 1 {
            assert_eq!(
                [&first, &second]
                    .iter()
                    .map(|m| store
                        .host_capacity
                        .execution
                        .waiting(&m.campaign_id)
                        .unwrap())
                    .sum::<usize>(),
                1
            );
            assert_eq!(
                [&first, &second]
                    .iter()
                    .filter(|m| store
                        .campaign_execution(&m.campaign_id, &format!("{}-root", m.campaign_id))
                        .unwrap()
                        .is_some())
                    .count(),
                1
            );
            assert_eq!(store.host_capacity.model.available(), 1);
        }
        if cancel_first {
            service.cancel(&first.campaign_id).unwrap();
            assert_eq!(status(&store, &second), CampaignStatus::Running);
        }
        release.send_replace(true);
        if cpu_cap == 1 {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !first.workspace.join("cpu-running").exists()
                    && !second.workspace.join("cpu-running").exists()
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            let tx = store.database.begin_write().unwrap();
            assert_eq!(
                [&first, &second]
                    .iter()
                    .map(|m| RuntimeStore::inspect_jobs_in(&tx, &m.campaign_id)
                        .unwrap()
                        .len())
                    .sum::<usize>(),
                1,
                "capacity-queued exec must not reserve compute time"
            );
            drop(tx);
            assert_eq!(
                [&first, &second]
                    .iter()
                    .filter(|m| m.workspace.join("cpu-running").exists())
                    .count(),
                1
            );
            assert_eq!(store.host_capacity.cpu.available_permits(), 0);
            assert_eq!(requests.load(Ordering::SeqCst), 2);
            for m in [&first, &second] {
                std::fs::write(m.workspace.join("cpu-exit"), b"").unwrap();
            }
        }
        tokio::time::timeout(Duration::from_secs(20), async {
            while service
                .active
                .lock()
                .unwrap()
                .values()
                .any(|a| !a.task.is_finished())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        service.shutdown();
        tokio::time::timeout(Duration::from_secs(2), http)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            requests.load(Ordering::SeqCst),
            if cpu_cap == 1 { 4 } else { 2 }
        );
        assert_eq!(store.host_capacity.cpu.available_permits(), cpu_cap);
        if cpu_cap == 1 {
            assert!(first.workspace.join("cpu-running").exists());
            assert!(second.workspace.join("cpu-running").exists());
        }
        for m in [&first, &second] {
            let record = store
                .campaign_execution(&m.campaign_id, &format!("{}-root", m.campaign_id))
                .unwrap()
                .unwrap();
            if m.campaign_id != first.campaign_id || !cancel_first {
                assert!(
                    record.candidate.is_some(),
                    "sibling must finish with evidence"
                );
                assert_eq!(status(&store, m), CampaignStatus::Unverified);
            }
            let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
            assert_eq!(ledger.envelope.work.tokens, m.work_tokens);
        }
        // Both owners completed kill/reap, including the cancelled process.
        assert_eq!(store.host_capacity.execution.available(), execution_cap);
        let mut permits = Vec::new();
        for _ in 0..resident_cap {
            permits.push(
                tokio::time::timeout(
                    Duration::from_secs(1),
                    store.host_capacity.resident.acquire("probe"),
                )
                .await
                .unwrap()
                .unwrap(),
            );
        }
    }
}

#[test]
fn bounded_registry_reaps_finished_campaigns_without_touching_siblings() {
    let (dir, store, first) = fixture();
    let mut store = Arc::try_unwrap(store).ok().unwrap();
    store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(
        tachyon_util::config::ResourceLimits {
            max_campaigns: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let store = Arc::new(store);
    let ApiResponse::Research { research } = store
        .research_request(&ApiRequest::ResearchCreate {
            command_id: "siblings".into(),
            title: "siblings".into(),
            objective: "fixture".into(),
        })
        .unwrap()
    else {
        panic!()
    };
    let mut manifests = vec![first];
    for command in ["second", "third"] {
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: command.into(),
                research_id: research.id.clone(),
                title: command.into(),
                objective: "fixture".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let mut manifest = manifests[0].clone();
        manifest.campaign_id = campaign.id;
        manifests.push(manifest);
    }
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    for manifest in &manifests[..2] {
        service
            .launch(
                manifest,
                "http://127.0.0.1".into(),
                false,
                |_, cancel| async move {
                    let mut stop = cancel.subscribe();
                    while !*stop.borrow_and_update() {
                        stop.changed().await.unwrap();
                    }
                    Ok(ExecutionPhase::Reviewed(Evaluation::Unverified))
                },
            )
            .unwrap();
    }
    assert!(service
        .launch(
            &manifests[2],
            "http://127.0.0.1".into(),
            false,
            |_, _| async { panic!("over cap") }
        )
        .unwrap_err()
        .contains("capacity exhausted"));
    assert_eq!(status(&store, &manifests[2]), CampaignStatus::Draft);
    service.cancel(&manifests[0].campaign_id).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !service.active.lock().unwrap()[&manifests[0].campaign_id]
        .task
        .is_finished()
    {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    service
        .launch(
            &manifests[2],
            "http://127.0.0.1".into(),
            false,
            |_, _| async { Ok(ExecutionPhase::Reviewed(Evaluation::Unverified)) },
        )
        .unwrap();
    assert_eq!(status(&store, &manifests[1]), CampaignStatus::Running);
    assert!(!service
        .active
        .lock()
        .unwrap()
        .contains_key(&manifests[0].campaign_id));
    service.shutdown();
    assert_eq!(status(&store, &manifests[1]), CampaignStatus::Cancelled);
}
