use super::*;
use tachyon_api::types::{AgentEvent, LifetimeClass, WorkRequest};

#[test]
fn spool_upload_sixty_four_mib_stream_and_expired_lease() {
    use crate::runtime_store::research_context::traces::{upload::Upload, TRACES};
    use sha2::{Digest, Sha256};
    use tachyon_api::context::{Request as R, Resource};
    use tachyon_model::broker::{ResourceUpload as U, UploadReply, UPLOAD_CHUNK};
    let (_dir, mut store, funding, request) = tests::setup();
    store.trace_limits.campaign = 64 * 1024 * 1024 + 1;
    let store = Arc::new(store);
    let campaign = &funding.admission.campaign_id;
    let chunk = vec![b'x'; UPLOAD_CHUNK];
    let size = 64 * 1024 * 1024;
    let mut hash = Sha256::new();
    for _ in 0..size / UPLOAD_CHUNK {
        hash.update(&chunk);
    }
    let begin = U::Begin {
        handle: format!("output:{}", uuid::Uuid::new_v4()),
        retained: size as u64,
        total: size as u64,
        storage_failed: false,
        sha256: format!("{:x}", hash.finalize()),
    };
    let create = |begin| {
        Upload::begin(
            store.clone(),
            campaign,
            &funding.admission.work_id,
            &request.identity.attempt_id,
            request.identity.generation,
            begin,
        )
    };
    let mut upload = create(begin.clone()).unwrap();
    assert!(create(begin).is_err());
    for offset in (0..size).step_by(UPLOAD_CHUNK) {
        upload
            .apply(U::Chunk {
                offset: offset as u64,
                bytes: chunk.clone(),
            })
            .unwrap();
    }
    let UploadReply::Ready { resource } = upload.apply(U::Finish { sha256: None }).unwrap() else {
        panic!()
    };
    let page = store
        .host_research_context(
            campaign,
            &R::Read {
                resource,
                offset: size as u64 - 1024,
                limit: 1024,
            },
            None,
        )
        .unwrap();
    assert_eq!(
        page.resources[0].data["bytes"],
        serde_json::json!(vec![b'x'; 1024])
    );
    drop(upload);

    let begin = U::Begin {
        handle: format!("output:{}", uuid::Uuid::new_v4()),
        retained: 1,
        total: 1,
        storage_failed: false,
        sha256: format!("{:x}", Sha256::digest(b"x")),
    };
    let mut expired = create(begin.clone()).unwrap();
    let tx = store.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(TRACES).unwrap();
        let pending = table
            .iter()
            .unwrap()
            .filter_map(|row| {
                let (key, value) = row.unwrap();
                let mut resource: Resource = serde_json::from_slice(value.value()).unwrap();
                if resource.data["retention_state"] != "staging" {
                    return None;
                }
                resource.data["lease_expires_ms"] = serde_json::json!(0);
                Some((key.value().1.to_owned(), resource))
            })
            .collect::<Vec<_>>();
        for (id, r) in pending {
            table
                .insert(
                    (campaign.as_str(), id.as_str()),
                    serde_json::to_vec(&r).unwrap().as_slice(),
                )
                .unwrap();
        }
    }
    tx.commit().unwrap();
    // Expire even when the new request is a duplicate; retain bytes and identity.
    assert!(create(begin).is_err());
    expired
        .apply(U::Chunk {
            offset: 0,
            bytes: b"x".to_vec(),
        })
        .unwrap();
    assert!(expired.apply(U::Finish { sha256: None }).is_err());
    drop(expired);
    // Expiry retains the charge, not just the old handle. A fresh upload cannot
    // reuse the byte still reserved for the expired object's retained staging.
    assert!(create(U::Begin {
        handle: format!("output:{}", uuid::Uuid::new_v4()),
        retained: 1,
        total: 1,
        storage_failed: false,
        sha256: format!("{:x}", Sha256::digest(b"x")),
    })
    .is_err());
    assert_eq!(std::fs::read_dir(&store.trace_root).unwrap().count(), 2);
    let tx = store.database.begin_read().unwrap();
    let mut gaps = 0;
    for row in tx.open_table(TRACES).unwrap().iter().unwrap() {
        let (_, value) = row.unwrap();
        let resource: Resource = serde_json::from_slice(value.value()).unwrap();
        if resource.data["retention_state"] == "gap" {
            gaps += 1;
            assert_eq!(resource.data["size_bytes"], 1);
        }
    }
    assert_eq!(gaps, 1);
}

#[test]
fn spool_upload_staging_digest_quotas_and_cleanup() {
    use crate::runtime_store::research_context::traces::upload::Upload;
    use sha2::{Digest, Sha256};
    use tachyon_api::context::{Query, Request as R};
    use tachyon_model::broker::{ResourceUpload as U, UploadReply};
    for failure in [
        "none",
        "tamper",
        "short",
        "offset",
        "oversize",
        "cancel",
        "campaign",
        "global",
        "disk",
        "commit",
        "late_deadline",
    ] {
        let (_dir, mut store, funding, request) = tests::setup();
        if failure == "campaign" {
            store.trace_limits.campaign = 3;
        }
        if failure == "global" {
            store.trace_limits.global = 3;
        }
        if failure == "disk" {
            std::fs::write(&store.trace_root, b"not-directory").unwrap();
        }
        let store = Arc::new(store);
        let campaign = &funding.admission.campaign_id;
        let list = R::Traces {
            query: Query {
                literal: None,
                after: None,
                limit: 16,
                since_ms: None,
                version: None,
            },
        };
        let digest = format!("{:x}", Sha256::digest(b"abcd"));
        let upload = Upload::begin(
            store.clone(),
            campaign,
            &funding.admission.work_id,
            &request.identity.attempt_id,
            request.identity.generation,
            U::Begin {
                handle: format!("output:{}", uuid::Uuid::new_v4()),
                retained: 4,
                total: 10,
                storage_failed: false,
                sha256: digest.clone(),
            },
        );
        if matches!(failure, "campaign" | "global" | "disk") {
            assert!(upload.is_err(), "{failure}");
            assert!(store
                .host_research_context(campaign, &list, None)
                .unwrap()
                .resources
                .is_empty());
            continue;
        }
        let mut upload = upload.unwrap();
        assert!(store
            .host_research_context(campaign, &list, None)
            .unwrap()
            .resources
            .is_empty());
        if failure != "cancel" {
            let bytes = match failure {
                "tamper" => b"abce".to_vec(),
                "short" => b"abc".to_vec(),
                "oversize" => vec![0; 32769],
                _ => b"abcd".to_vec(),
            };
            let result = upload.apply(U::Chunk {
                offset: if failure == "offset" { 1 } else { 0 },
                bytes,
            });
            if matches!(failure, "offset" | "oversize") {
                assert!(result.is_err());
            } else {
                result.unwrap();
                let checks = std::cell::Cell::new(0);
                let finished = upload.apply_checked(
                    U::Finish {
                        sha256: Some(digest),
                    },
                    || {
                        checks.set(checks.get() + 1);
                        failure != "late_deadline" || checks.get() == 1
                    },
                    |_| {
                        if failure == "commit" {
                            Err("commit authority denied".into())
                        } else {
                            Ok(())
                        }
                    },
                );
                assert_eq!(finished.is_ok(), failure == "none", "{failure}");
                if let Ok(UploadReply::Ready { resource }) = finished {
                    let page = store
                        .host_research_context(
                            campaign,
                            &R::Read {
                                resource,
                                offset: 0,
                                limit: 4,
                            },
                            None,
                        )
                        .unwrap();
                    assert_eq!(
                        page.resources[0].data["bytes"],
                        serde_json::json!([97, 98, 99, 100])
                    );
                    assert_eq!(page.resources[0].data["discarded_bytes"], 6);
                    assert_eq!(page.resources[0].data["full_output"], false);
                }
            }
        }
        drop(upload);
        let resources = store
            .host_research_context(campaign, &list, None)
            .unwrap()
            .resources;
        assert_eq!(resources.len(), 1);
        assert_eq!(
            resources[0].data["retention_state"],
            if failure == "none" { "ready" } else { "gap" }
        );
        if failure != "none" {
            assert_eq!(std::fs::read_dir(&store.trace_root).unwrap().count(), 1);
            assert_eq!(resources[0].data["size_bytes"], 4);
        }
    }
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake provider only"]
async fn actual_ghost_spool_retained_after_exit_beyond_model_envelope() {
    use tachyon_api::context::{Query, Request as R};
    let executable = std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").unwrap());
    let (dir, store, funding, mut request) = tests::setup();
    let workspace = tempfile::tempdir().unwrap();
    let store = Arc::new(store);
    let (url, mut wire, http) = fixture(store.clone(), "spool").await;
    request.estimate.base_url = url;
    request.estimate.max_request_bytes = 32000;
    let broker = ModelBroker::new(store.clone(), model(&request));
    let events = broker
        .launch_private(
            &executable,
            workspace.path(),
            workspace.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        AgentEvent::Reply {
            final_reply: true,
            ..
        }
    )));
    assert!(events.iter().any(|e| matches!(&e.kind, AgentEvent::ToolTelemetry { tool_name, success: true, .. } if tool_name == "ctx")));
    wire.recv().await.unwrap();
    let second = wire.recv().await.unwrap();
    assert!(second.to_string().len() < 32000);
    http.abort();
    let _ = http.await;
    drop(broker);
    drop(store);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    let campaign = &funding.admission.campaign_id;
    let mut query = Query {
        literal: Some("retained_output".into()),
        after: None,
        limit: 16,
        since_ms: None,
        version: None,
    };
    let mut resources = Vec::new();
    loop {
        let page = store
            .host_research_context(
                campaign,
                &R::Traces {
                    query: query.clone(),
                },
                None,
            )
            .unwrap();
        resources.extend(page.resources);
        query.after = page.next_cursor;
        if query.after.is_none() {
            break;
        }
    }
    let spool = resources
        .iter()
        .find(|r| r.data["size_bytes"] == 2400000)
        .expect("full stdout spool, not emitted envelope");
    let handle = spool.data["live_handle_id"].as_str().unwrap();
    assert!(
        serde_json::to_string(&events).unwrap().contains(handle),
        "original live ctx handle is mapped"
    );
    for (offset, byte) in [(1100000, b'A'), (1300000, b'B')] {
        let read = R::Read {
            resource: spool.reference.clone(),
            offset,
            limit: 1024,
        };
        let page = store.host_research_context(campaign, &read, None).unwrap();
        assert_eq!(
            page.resources[0].data["bytes"],
            serde_json::json!(vec![byte; 1024])
        );
        assert!(store.host_research_context("foreign", &read, None).is_err());
    }
    for field in ["work", "hash"] {
        let mut resource = spool.reference.clone();
        if field == "work" {
            resource.work_id.push('x');
        } else {
            resource.version = "0".repeat(64);
        }
        assert!(store
            .host_research_context(
                campaign,
                &R::Read {
                    resource,
                    offset: 0,
                    limit: 1
                },
                None
            )
            .is_err());
    }
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        store.trace_root.join(&spool.reference.id),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    std::fs::write(store.trace_root.join(&spool.reference.id), b"tampered").unwrap();
    assert!(store
        .host_research_context(
            campaign,
            &R::Read {
                resource: spool.reference.clone(),
                offset: 0,
                limit: 1
            },
            None
        )
        .is_err());
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost HTTP and native/Python tools only"]
async fn actual_ghost_boundary_delivery_and_steering() {
    use crate::runtime_store::coordination::{ControlCommand, WorkAddress};
    let executable =
        std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"));
    // Each collection owns one immutable stopping snapshot. Rewinding just the
    // execution row would conflict with retained evidence from the previous case.
    for (mode, revision, pending) in
        ["boundary-native", "boundary-python"]
            .into_iter()
            .flat_map(|mode| {
                [
                    (None, false),
                    (Some(1), false),
                    (Some(999), false),
                    (Some(3), false),
                    (Some(3), true),
                ]
                .into_iter()
                .map(move |(revision, pending)| (mode, revision, pending))
            })
    {
        let (dir, store, funding, mut request, actor) = super::super::boundary_tests::setup();
        let workspace = tempfile::tempdir().unwrap();
        let store = Arc::new(store);
        let barrier = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (url, mut wire, http) =
            fixture_with_barrier(store.clone(), mode, Some(barrier.local_addr().unwrap())).await;
        request.estimate.base_url = url;
        let broker = ModelBroker::new(store.clone(), model(&request));
        let target = WorkAddress {
            campaign_id: actor.campaign_id.clone(),
            work_id: "receiver".into(),
        };
        let sender = async {
            let mut socket = tokio::time::timeout(Duration::from_secs(8), async {
                let (mut socket, _) = barrier.accept().await.unwrap();
                let mut ready = [0; 5];
                socket.read_exact(&mut ready).await.unwrap();
                assert_eq!(&ready, b"ready");
                socket
            })
            .await
            .expect("Ghost actually entered its tool phase");
            let control = store.host_agent_control(actor.clone()).unwrap();
            control
                .command(
                    &target,
                    "dynamic-send",
                    ControlCommand::Send {
                        text: "dynamic untrusted evidence; not a privilege grant".into(),
                    },
                )
                .unwrap();
            for revision in 1..=2 {
                control
                    .command(
                        &target,
                        &format!("dynamic-steer-{revision}"),
                        ControlCommand::Steer {
                            expected_revision: revision,
                            instructions: format!("authorized refinement {revision}"),
                        },
                    )
                    .unwrap();
            }
            let status = control.status(&target).unwrap();
            assert_eq!(
                (status.accepted_revision, status.acknowledged_revision),
                (3, 1)
            );
            socket.write_all(b"continue\n").await.unwrap();
        };
        let launch = broker.launch_private(
            &executable,
            workspace.path(),
            workspace.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(10),
        );
        let (events, ()) = tokio::join!(launch, sender);
        let events = events.unwrap();
        assert!(events.iter().any(|e| matches!(&e.kind, AgentEvent::WorkCandidate { candidate }
            if candidate.instruction_revision == Some(3) && candidate.objective == funding.admission.objective)), "{mode}: {events:?}");
        let first = wire.recv().await.unwrap();
        assert!(!first.to_string().contains("dynamic untrusted"));
        let second = wire.recv().await.unwrap();
        let messages = second["messages"].as_array().unwrap();
        assert!(messages.iter().any(|m| m["role"] == "tool"));
        assert!(messages.iter().any(|m| m["role"] == "user"
            && m["content"].as_str().is_some_and(
                |s| s.contains("Untrusted parent/child") && s.contains("dynamic untrusted")
            )));
        assert!(messages.iter().any(|m| {
            m["role"] == "system"
                && m["content"]
                    .as_str()
                    .is_some_and(|s| s.contains("authorized refinement 2"))
        }));
        assert!(!second.to_string().contains("authorized refinement 1"));
        let status = store
            .host_agent_control(actor.clone())
            .unwrap()
            .status(&target)
            .unwrap();
        assert_eq!(
            (
                status.accepted_revision,
                status.acknowledged_revision,
                status.delivered_revision,
                status.delivery_cursor
            ),
            (3, 3, 3, 3)
        );
        assert_eq!(
            store
                .admitted_work(&target.campaign_id, "receiver")
                .unwrap(),
            funding
        );
        assert_eq!(
            store
                .campaign_ledger(&target.campaign_id)
                .unwrap()
                .unwrap()
                .allocation_available(&funding.dispatch_id)
                .unwrap()
                .tokens,
            46
        );
        assert!(store
            .model_permits
            .lock()
            .unwrap()
            .grants
            .values()
            .all(|g| !g.active || g.closed.load(std::sync::atomic::Ordering::Acquire)));
        // Evidence-only storage fixture: no evaluator invocation or new funding.
        use crate::runtime_store::execution::{ExecutionPhase, ExecutionRecord, EXECUTIONS};
        let previous: ExecutionRecord = serde_json::from_value(serde_json::json!({
            "schema_version":1,
            "policy":{"funding":funding,"model":request,"work":work(&funding),
                "verification":funding,"evaluator_id":"unused-fixture"},
            "phase":"ExecutingUnknown","candidate":null,"settled":false
        }))
        .unwrap();
        let control = store.host_agent_control(actor).unwrap();
        if pending {
            control
                .command(
                    &target,
                    "later",
                    ControlCommand::Steer {
                        expected_revision: 3,
                        instructions: "future refinement".into(),
                    },
                )
                .unwrap();
        }
        {
            let tx = store.database.begin_write().unwrap();
            tx.open_table(EXECUTIONS)
                .unwrap()
                .insert(
                    "receiver",
                    serde_json::to_vec(&previous).unwrap().as_slice(),
                )
                .unwrap();
            tx.commit().unwrap();
            let mut evidence = events.clone();
            for event in &mut evidence {
                if let AgentEvent::WorkCandidate { candidate } = &mut event.kind {
                    candidate.instruction_revision = revision;
                }
            }
            let collected = store
                .host_collect_campaign_evidence(&previous, evidence)
                .unwrap();
            assert_eq!(
                collected.phase == ExecutionPhase::EvidenceReady,
                revision == Some(3) && !pending,
                "{mode}, revision {revision:?}: {collected:?}; persisted error: {:?}; events: {events:?}",
                store.database.begin_read().unwrap()
                    .open_table(crate::runtime_store::execution::EXECUTION_ERRORS).unwrap()
                    .get("receiver").unwrap()
                    .map(|value| String::from_utf8_lossy(value.value()).into_owned())
            );
            assert!(
                collected.stopping_snapshot.is_some(),
                "{mode}, {revision:?}, pending={pending}: {collected:?}"
            );
            assert_eq!(collected.candidate.is_some(), revision == Some(3));
        }
        assert_eq!(control.status(&target).unwrap().acknowledged_revision, 3);
        if revision == Some(3) {
            assert_eq!(
                control.status(&target).unwrap().result.unwrap().current,
                !pending
            );
            if !pending {
                control
                    .command(
                        &target,
                        "later",
                        ControlCommand::Steer {
                            expected_revision: 3,
                            instructions: "future refinement".into(),
                        },
                    )
                    .unwrap();
            }
            assert!(!control.status(&target).unwrap().result.unwrap().current);
            store.host_ack_agent_steering(&target, 4).unwrap();
            let historical = control.status(&target).unwrap().result.unwrap();
            assert!(!historical.current);
            assert_eq!(historical.revision, 3);
        }
        http.abort();
        let _ = http.await;
        drop(control);
        drop(broker);
        drop(store);
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        use tachyon_api::context::{Query, Request as ContextRequest};
        let page = store
            .host_research_context(
                &target.campaign_id,
                &ContextRequest::Traces {
                    query: Query {
                        literal: None,
                        after: None,
                        limit: 16,
                        since_ms: None,
                        version: None,
                    },
                },
                None,
            )
            .unwrap();
        assert_eq!(
            page.resources
                .iter()
                .filter(|r| r.data["phase"] == "work_context_snapshot"
                    && r.data["source"] != "host_stop")
                .count(),
            2,
            "{mode}"
        );
        assert_eq!(
            page.resources
                .iter()
                .filter(|r| r.data["source"] == "host_stop")
                .count(),
            1,
            "{mode}: {page:?}"
        );
        assert_eq!(
            page.resources
                .iter()
                .filter(|r| r.data["phase"] == "model_result")
                .count(),
            2,
            "{mode}"
        );
        let tools = page
            .resources
            .into_iter()
            .filter(|r| r.data["phase"] == "request" || r.data["phase"] == "result")
            .collect::<Vec<_>>();
        assert_eq!(tools.len(), 2, "{mode}");
        for descriptor in tools {
            let read = ContextRequest::Read {
                resource: descriptor.reference.clone(),
                offset: 0,
                limit: 1024,
            };
            assert!(store
                .host_research_context("wrong-campaign", &read, None)
                .is_err());
            let page = store
                .host_research_context(&target.campaign_id, &read, None)
                .unwrap();
            assert!(serde_json::to_vec(&page).unwrap().len() <= 8192);
            let bytes: Vec<u8> =
                serde_json::from_value(page.resources[0].data["bytes"].clone()).unwrap();
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains("durable-stdout-marker"), "{mode}: {text}");
            if descriptor.data["phase"] == "request" {
                assert!(text.contains(if mode == "boundary-python" {
                    "proc = require"
                } else {
                    "/bin/sh"
                }));
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost HTTP only"]
async fn actual_ghost_agents_scoped_control() {
    use crate::runtime_store::coordination::WorkAddress;
    use tachyon_api::agents::Control;
    let executable =
        std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"));
    let (_dir, store, funding, mut request) = tests::setup_agents();
    let workspace = tempfile::tempdir().unwrap();
    let store = Arc::new(store);
    let (url, mut wire, http) = fixture(store.clone(), "agents").await;
    request.estimate.base_url = url;
    request.estimate.max_request_bytes = 32000;
    let broker = ModelBroker::new(store.clone(), model(&request)).with_controls([
        Control::Status,
        Control::List,
        Control::Result,
        Control::Send,
    ]);
    let events = broker
        .launch_private(
            &executable,
            workspace.path(),
            workspace.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert!(events.iter().any(
        |e| matches!(&e.kind, AgentEvent::Reply {text, final_reply: true, ..} if text == "ok")
    ));
    let first = wire.recv().await.unwrap();
    let schema = first["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["function"]["name"] == "agents")
        .unwrap();
    assert_eq!(
        schema["function"]["parameters"]["properties"]["action"]["enum"],
        serde_json::json!(["status", "list", "result", "send"])
    );
    let second = wire.recv().await.unwrap();
    let outputs: Vec<serde_json::Value> = second["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| serde_json::from_str(m["content"].as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(outputs.len(), 5);
    assert_eq!(outputs.iter().filter(|o| o["is_error"] == true).count(), 1);
    let text = serde_json::to_string(&outputs).unwrap();
    assert!(text.contains("accepted_revision"));
    assert!(text.contains("ghost-send"));
    assert!(text.contains("snapshot"));
    let control = store
        .host_agent_control(WorkAddress {
            campaign_id: request.identity.campaign_id,
            work_id: "work".into(),
        })
        .unwrap();
    let messages = control.messages(0, 32).unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].recipient.work_id, "child");
    assert_eq!(messages[0].command_id, "ghost-send");
    http.abort();
}

fn work(funding: &AdmittedWork) -> WorkRequest {
    WorkRequest {
        context_refs: vec![],
        constraints: None,
        attempt: None,
        work_id: funding.admission.work_id.clone(),
        objective: funding.admission.objective.clone(),
        generation: funding.admission.generation,
        assignment: 1,
        deadline_ms: (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 10000) as u64,
        lifetime_class: LifetimeClass::Short,
    }
}

#[test]
fn durable_trace_scope_conflicts_truncation_capacity_and_disk_errors() {
    use crate::runtime_store::research_context::traces::TRACE_ASSIGNMENTS;
    use tachyon_api::context::{Query, Request as ContextRequest};
    for failure in ["none", "disk", "quota"] {
        let (dir, mut store, funding, request) = tests::setup();
        store.trace_limits.operation = 2048;
        if failure == "quota" {
            store.trace_limits.campaign = 1;
        }
        if failure == "disk" {
            std::fs::write(&store.trace_root, b"not a directory").unwrap();
        }
        let invocation = work(&funding);
        let registered = store
            .admitted_work(&funding.admission.campaign_id, &funding.admission.work_id)
            .unwrap();
        let crate::runtime_store::admission::DispatchState::Registered { worker_id } =
            &registered.state
        else {
            panic!()
        };
        let campaign = &funding.admission.campaign_id;
        let attempt = &request.identity.attempt_id;
        let tx = store.database.begin_write().unwrap();
        tx.open_table(TRACE_ASSIGNMENTS)
            .unwrap()
            .insert(
                (
                    campaign.as_str(),
                    invocation.work_id.as_str(),
                    attempt.as_str(),
                ),
                serde_json::to_vec(&(&invocation, worker_id))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
        let event: tachyon_api::EventEnvelope = serde_json::from_value(serde_json::json!({
            "event_id":1,"sequence":1,"occurred_at_ms":10,"session_id":worker_id,"task_id":worker_id,
            "actor":{"kind":"worker","id":worker_id},"kind":"tool_finished","id":"call","output":"x".repeat(4096)
        })).unwrap();
        let list = ContextRequest::Traces {
            query: Query {
                literal: None,
                after: None,
                limit: 16,
                since_ms: None,
                version: None,
            },
        };
        for field in ["worker", "assignment", "generation", "attempt"] {
            let mut forged = event.clone();
            let mut wrong = invocation.clone();
            match field {
                "worker" => forged.session_id.push('x'),
                "assignment" => wrong.assignment += 1,
                "generation" => wrong.generation += 1,
                _ => {}
            }
            assert!(store
                .record_tool_trace(
                    &funding,
                    &wrong,
                    if field == "attempt" {
                        "forged"
                    } else {
                        attempt
                    },
                    worker_id,
                    &forged
                )
                .is_err());
            assert!(store
                .host_research_context(campaign, &list, None)
                .unwrap()
                .resources
                .is_empty());
        }
        let recorded = store.record_tool_trace(&funding, &invocation, attempt, worker_id, &event);
        if failure != "none" {
            assert!(recorded.is_err());
            assert!(store
                .host_research_context(campaign, &list, None)
                .unwrap()
                .resources
                .is_empty());
            continue;
        }
        recorded.unwrap();
        store
            .record_tool_trace(&funding, &invocation, attempt, worker_id, &event)
            .unwrap();
        let mut conflict = event.clone();
        if let AgentEvent::ToolFinished { output, .. } = &mut conflict.kind {
            *output = "changed".into();
        }
        assert!(store
            .record_tool_trace(&funding, &invocation, attempt, worker_id, &conflict)
            .is_err());
        drop(store);
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let resources = store
            .host_research_context(campaign, &list, None)
            .unwrap()
            .resources;
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].data["truncated_bytes"], 2048);
        assert_eq!(resources[0].data["original_bytes"], 4096);
        let reference = resources[0].reference.clone();
        for offset in [0, 1024, 2048] {
            let read = ContextRequest::Read {
                resource: reference.clone(),
                offset,
                limit: 1024,
            };
            let page = store.host_research_context(campaign, &read, None).unwrap();
            let bytes = page.resources[0].data["bytes"].as_array().unwrap();
            assert_eq!(bytes.len(), if offset == 2048 { 0 } else { 1024 });
            assert!(serde_json::to_vec(&page).unwrap().len() <= 8192);
            assert!(store.host_research_context("wrong", &read, None).is_err());
        }
        let mut forged = reference.clone();
        forged.work_id = "other".into();
        assert!(store
            .host_research_context(
                campaign,
                &ContextRequest::Read {
                    resource: forged,
                    offset: 0,
                    limit: 1
                },
                None
            )
            .is_err());
        assert!(store
            .host_research_context(
                campaign,
                &ContextRequest::Read {
                    resource: reference,
                    offset: 0,
                    limit: 1025
                },
                None
            )
            .is_err());
    }
}

#[test]
fn explicit_input_documents_are_allowlisted_immutable_and_scoped() {
    use sha2::{Digest, Sha256};
    use tachyon_api::{
        campaign::{ChildInput, InputFile},
        context::Request as ContextRequest,
    };
    let (dir, store, funding, request) = tests::setup();
    let source = tempfile::tempdir().unwrap();
    let objects = tempfile::tempdir().unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(objects.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let artifacts = tachyond::artifact_store::ArtifactStore::open_retained(
        objects.path(),
        store.retained.clone(),
        &funding.admission.campaign_id,
    )
    .unwrap();
    let content = b"explicit input, not an automatically captured user prompt";
    std::fs::write(source.path().join("input.txt"), content).unwrap();
    let input = ChildInput {
        root: source.path().into(),
        files: vec![InputFile {
            path: "input.txt".into(),
            sha256: format!("{:x}", Sha256::digest(content)),
        }],
    };
    let campaign = &funding.admission.campaign_id;
    let work = &funding.admission.work_id;
    assert!(store
        .host_register_input_document(
            campaign,
            work,
            &input,
            std::path::Path::new("other"),
            &artifacts
        )
        .is_err());
    let reference = store
        .host_register_input_document(
            campaign,
            work,
            &input,
            std::path::Path::new("input.txt"),
            &artifacts,
        )
        .unwrap();
    std::fs::remove_file(source.path().join("input.txt")).unwrap();
    assert_eq!(
        store
            .host_register_input_document(
                campaign,
                work,
                &input,
                std::path::Path::new("input.txt"),
                &artifacts
            )
            .unwrap(),
        reference
    );
    assert_eq!(
        store.retained.summary(campaign).unwrap()["campaign_charged_bytes"],
        content.len() as u64
    );
    drop(store);
    drop(artifacts);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    let artifacts = tachyond::artifact_store::ArtifactStore::open_retained(
        objects.path(),
        store.retained.clone(),
        campaign,
    )
    .unwrap();
    let read = ContextRequest::Read {
        resource: reference,
        offset: 0,
        limit: 1024,
    };
    let page = store
        .host_research_context(campaign, &read, Some(&artifacts))
        .unwrap();
    let bytes: Vec<u8> = serde_json::from_value(page.resources[0].data["bytes"].clone()).unwrap();
    assert_eq!(bytes, content);
    assert!(store
        .host_research_context("wrong", &read, Some(&artifacts))
        .is_err());
    assert!(store.host_research_context(campaign, &read, None).is_err());
    let redacted = model(&request).redact_trace(
        "print('user input') localhost-fixture-key Authorization: Bearer private-token\nnext",
    );
    assert!(redacted.contains("print('user input')"));
    assert!(!redacted.contains("localhost-fixture-key"));
    assert!(!redacted.contains("private-token"));
    let model = model(&request);
    assert_eq!(
        model.redact_trace("Bearer first\nBEARER second\" bearer 'literal'"),
        "Bearer [REDACTED]\nBEARER [REDACTED]\" bearer [REDACTED]'literal'"
    );
    let repeated = "Bearer token\n".repeat(10_000);
    assert_eq!(
        model.redact_trace(&repeated),
        "Bearer [REDACTED]\n".repeat(10_000)
    );
}

#[tokio::test]
#[ignore = "requires an explicitly freshly built GHOST_TEST_BIN; see docs/ghost/BROKER.md"]
async fn actual_ghost_subprocess_deadline_retains_unknown_dispatch() {
    let executable = std::path::PathBuf::from(
        std::env::var_os("GHOST_TEST_BIN").expect("explicit freshly built Ghost binary"),
    );
    let (_dir, store, funding, mut request) = tests::setup();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(store);
    let (url, mut wire, http) = fixture(store.clone(), "stall").await;
    request.estimate.base_url = url;
    request.estimate.max_request_bytes = 32000;
    let broker = ModelBroker::new(store.clone(), model(&request));
    assert!(broker
        .launch_private(
            &executable,
            root.path(),
            root.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(2)
        )
        .await
        .is_err());
    wire.try_recv()
        .expect("provider request was durably claimed before deadline");
    assert_eq!(
        store.host_capacity.resident.available(),
        store.host_capacity.limits.max_resident_workers,
        "timeout with confirmed kill/reap releases residency even while billing stays unknown"
    );
    assert_eq!(
        store.host_capacity.model.available(),
        store.host_capacity.limits.max_model_calls
    );
    let ledger = store
        .campaign_ledger(&request.identity.campaign_id)
        .unwrap()
        .unwrap();
    assert_eq!(ledger.active_inferences(), 1);
    assert_eq!(
        ledger
            .allocation_available(&funding.dispatch_id)
            .unwrap()
            .tokens,
        70
    );
    assert!(store
        .model_permits
        .lock()
        .unwrap()
        .grants
        .values()
        .all(|grant| !grant.active || grant.closed.load(std::sync::atomic::Ordering::Acquire)));
    http.abort();
}

#[tokio::test]
#[ignore = "requires an explicitly freshly built GHOST_TEST_BIN; see docs/ghost/BROKER.md"]
async fn actual_ghost_subprocess_ledger_tool_final_and_no_relaunch() {
    let executable = std::path::PathBuf::from(
        std::env::var_os("GHOST_TEST_BIN").expect("explicit freshly built Ghost binary"),
    );
    let (dir, store, funding, mut request) = tests::setup();
    let workspace = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(store);
    let (url, mut wire, http) = fixture(store.clone(), "subprocess").await;
    request.estimate.base_url = url;
    request.estimate.max_request_bytes = 32000;
    let broker = ModelBroker::new(store.clone(), model(&request));
    let events = broker
        .launch_private(
            &executable,
            workspace.path(),
            home.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert!(events.iter().any(|event| matches!(&event.kind, AgentEvent::Reply { text, final_reply: true, .. } if text == "ok")));
    assert!(events.iter().any(|event| matches!(&event.kind, AgentEvent::ToolTelemetry { tool_name, success: true, .. } if tool_name == "exec")));
    wire.recv().await.unwrap();
    let second = wire.recv().await.unwrap();
    let tool = second["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["role"] == "tool")
        .unwrap()
        .to_string();
    assert!(
        tool.contains(&format!("HOME={}", workspace.path().display())),
        "{tool}"
    );
    for secret in [
        "OPENAI_API_KEY",
        "OPENROUTER_API_KEY",
        "TACHYON_DAEMON_TOKEN",
        "broker-env-sentinel",
        "localhost-fixture-key",
        "GHOST_TEST_BIN",
    ] {
        assert!(!tool.contains(secret), "{tool}");
    }
    let ledger = store
        .campaign_ledger(&request.identity.campaign_id)
        .unwrap()
        .unwrap();
    assert_eq!(ledger.reservations.len(), 3);
    assert_eq!(ledger.active_inferences(), 0);
    assert_eq!(
        ledger
            .allocation_available(&funding.dispatch_id)
            .unwrap()
            .tokens,
        86
    );
    assert!(broker
        .launch_private(
            &executable,
            workspace.path(),
            home.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(10)
        )
        .await
        .is_err());
    assert!(wire.try_recv().is_err());
    http.abort();
    let _ = http.await;
    drop(broker);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let broker = ModelBroker::new(store.clone(), model(&request));
    assert!(broker
        .launch_private(
            &executable,
            workspace.path(),
            home.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(10)
        )
        .await
        .is_err());
    assert_eq!(
        store
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap()
            .unwrap(),
        ledger
    );
}

#[tokio::test]
async fn unauthorized_and_expired_never_spawn() {
    use std::os::unix::fs::PermissionsExt;
    for mode in ["scope", "expired", "cancelled", "forged"] {
        let (_dir, store, funding, request) = tests::setup();
        let store = Arc::new(store);
        let broker = ModelBroker::new(store.clone(), model(&request));
        let mut invocation = work(&funding);
        let mut funding = funding;
        match mode {
            "expired" => invocation.deadline_ms = 0,
            "scope" => invocation.work_id.push('x'),
            "forged" => funding.dispatch_id.push('x'),
            _ => {
                store
                    .campaign_ledger_command(
                        "cancel-launch",
                        &funding.admission.campaign_id,
                        LedgerCommand::Cancel {
                            reservation_id: funding.dispatch_id.clone(),
                        },
                    )
                    .unwrap();
            }
        }
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("fixture");
        std::fs::write(&executable, "#!/bin/sh\ntouch spawned\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(broker
            .launch_private(
                &executable,
                root.path(),
                root.path(),
                funding.clone(),
                request.clone(),
                invocation,
                Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err());
        assert!(!root.path().join("spawned").exists());
        assert!(store
            .model_permits
            .lock()
            .unwrap()
            .grants
            .values()
            .all(|grant| !grant.active || grant.closed.load(std::sync::atomic::Ordering::Acquire)));
        assert!(store
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap()
            .unwrap()
            .allocations
            .is_empty());
    }
}

#[tokio::test]
async fn launch_requires_current_persisted_attempt_before_issuing_permit() {
    for mutation in ["unbound", "dropped", "assignment", "model", "terminal"] {
        let (_dir, store, funding, request) = tests::setup();
        let mut current_work = work(&funding);
        current_work.attempt = Some(tachyon_api::types::WorkAttempt {
            continuation: None,
            id: request.identity.attempt_id.clone(),
            feedback: None,
        });
        if mutation != "unbound" {
            let tx = store.database.begin_write().unwrap();
            tx.open_table(crate::runtime_store::execution::EXECUTIONS).unwrap().insert(
                current_work.work_id.as_str(),
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "policy": {"funding": funding, "model": request, "work": current_work,
                        "verification": funding, "evaluator_id": "test"},
                    "phase": if mutation == "terminal" { serde_json::json!({"Reviewed": "Unverified"}) }
                        else { serde_json::json!("ExecutingUnknown") },
                    "candidate": null, "settled": mutation == "terminal"
                })).unwrap().as_slice(),
            ).unwrap();
            tx.commit().unwrap();
        }
        let mut launched_request = request.clone();
        match mutation {
            "dropped" => current_work.attempt = None,
            "assignment" => current_work.assignment += 1,
            "model" => launched_request.estimate.input_tokens += 1,
            _ => {}
        }
        let store = Arc::new(store);
        let broker = ModelBroker::new(store.clone(), model(&request));
        let root = tempfile::tempdir().unwrap();
        assert!(broker
            .launch_private(
                &root.path().join("missing"),
                root.path(),
                root.path(),
                funding,
                launched_request,
                current_work,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .is_err());
        assert!(
            store.model_permits.lock().unwrap().grants.is_empty(),
            "{mutation}"
        );
    }
}

#[tokio::test]
async fn failed_spawn_claim_survives_reopen_and_never_launches_replacement() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, store, funding, request) = tests::setup();
    let root = tempfile::tempdir().unwrap();
    let executable = root.path().join("initially-missing");
    let broker = ModelBroker::new(Arc::new(store), model(&request));
    assert!(broker
        .launch_private(
            &executable,
            root.path(),
            root.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .is_err());
    assert_eq!(
        broker.store.host_capacity.resident.available(),
        broker.store.host_capacity.limits.max_resident_workers,
        "confirmed spawn failure releases physical admission, not the durable unknown launch fence"
    );
    drop(broker);
    std::fs::write(&executable, "#!/bin/sh\ntouch spawned\n").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let broker = ModelBroker::new(store.clone(), model(&request));
    assert!(broker
        .launch_private(
            &executable,
            root.path(),
            root.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .is_err());
    assert!(!root.path().join("spawned").exists());
    assert!(store
        .model_permits
        .lock()
        .unwrap()
        .grants
        .values()
        .all(|grant| !grant.active || grant.closed.load(std::sync::atomic::Ordering::Acquire)));
}

#[tokio::test]
async fn post_handshake_stdout_limit_and_exit_deadline() {
    use std::os::unix::fs::PermissionsExt;
    for oversized in [true, false] {
        let (_dir, store, mut funding, request) = tests::setup();
        let store = Arc::new(store);
        let broker = ModelBroker::new(store.clone(), model(&request));
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("fixture");
        // Local protocol fixture only: no model, provider, or tool execution.
        std::fs::write(
            &executable,
            format!(
                r##"#!/usr/bin/python3
import json, socket, struct, sys, time
def frame():
    size = struct.unpack('>I', sys.stdin.buffer.read(4))[0]
    return json.loads(sys.stdin.buffer.read(size))
bootstrap = frame()
work = frame()
with open('identity', 'w') as out:
    out.write(sys.argv[sys.argv.index('--agent-id') + 1])
channel = socket.socket(socket.AF_UNIX)
channel.connect(bootstrap['path'])
channel.sendall(bytes(bootstrap['capability']))
assert channel.recv(1) == b'\x01'
if {}:
    sys.stdout.buffer.write(b'x' * (1024 * 1024 + 1))
    sys.stdout.buffer.flush()
else:
    channel.close()
    sys.stdout.close()
    import os
    os.close(1)
time.sleep(60)
"##,
                if oversized { "True" } else { "False" }
            ),
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let registered = match store
            .admitted_work(&funding.admission.campaign_id, &funding.admission.work_id)
            .unwrap()
            .state
        {
            crate::runtime_store::admission::DispatchState::Registered { worker_id } => worker_id,
            _ => panic!("expected registered worker"),
        };
        funding.state = crate::runtime_store::admission::DispatchState::Registered {
            worker_id: "forged-caller-identity".into(),
        };
        let started = Instant::now();
        assert!(broker
            .launch_private(
                &executable,
                root.path(),
                root.path(),
                funding.clone(),
                request.clone(),
                work(&funding),
                started
                    + if oversized {
                        Duration::from_secs(5)
                    } else {
                        Duration::from_millis(500)
                    },
            )
            .await
            .is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(
            std::fs::read_to_string(root.path().join("identity")).unwrap(),
            registered
        );
        assert!(store
            .model_permits
            .lock()
            .unwrap()
            .grants
            .values()
            .all(|grant| !grant.active || grant.closed.load(std::sync::atomic::Ordering::Acquire)));
        assert!(store
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap()
            .unwrap()
            .allocations
            .is_empty());
    }
}

#[tokio::test]
async fn stalled_bootstrap_deadline_and_cancellation_kill_worker() {
    use std::os::unix::fs::PermissionsExt;
    for cancel in [false, true] {
        let (_dir, store, funding, request) = tests::setup();
        let store = Arc::new(store);
        let broker = ModelBroker::new(store.clone(), model(&request));
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("fixture");
        std::fs::write(&executable, "#!/bin/sh\nprintf '%s' \"$$\" > worker.pid\n/usr/bin/env > worker.env\n/bin/sleep 60 &\nprintf '%s' \"$!\" > descendant.pid\nwait\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let launch = broker.launch_private(
            &executable,
            root.path(),
            root.path(),
            funding.clone(),
            request.clone(),
            work(&funding),
            Instant::now() + Duration::from_millis(300),
        );
        if cancel {
            assert!(tokio::time::timeout(Duration::from_millis(150), launch)
                .await
                .is_err());
        } else {
            let contention = async {
                let pid_path = root.path().join("worker.pid");
                let pid = loop {
                    if let Ok(text) = std::fs::read_to_string(&pid_path) {
                        if let Ok(pid) = text.parse::<i32>() {
                            break pid;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                };
                let blocked = store.clone();
                let (ready, started) = tokio::sync::oneshot::channel();
                let (release, wait) = std::sync::mpsc::channel();
                let locks = tokio::task::spawn_blocking(move || {
                    let _authority = blocked.model_permits.lock().unwrap();
                    let _write = blocked.database.begin_write().unwrap();
                    ready.send(()).unwrap();
                    let _ = wait.recv_timeout(Duration::from_secs(3));
                });
                started.await.unwrap();
                let killed = tokio::time::timeout(Duration::from_secs(1), async {
                    while nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await;
                let _ = release.send(());
                locks.await.unwrap();
                killed.unwrap();
            };
            let (result, ()) = tokio::join!(launch, contention);
            assert!(result.is_err());
        }
        let pid: i32 = std::fs::read_to_string(root.path().join("worker.pid"))
            .unwrap()
            .parse()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let descendant: u32 = std::fs::read_to_string(root.path().join("descendant.pid"))
            .unwrap()
            .parse()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match std::fs::read_to_string(format!("/proc/{descendant}/stat")) {
                    // An orphan zombie is awaiting the OS init reaper, not running.
                    Ok(stat) if !stat.split_once(") ").unwrap().1.starts_with('Z') => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    _ => break,
                }
            }
        })
        .await
        .unwrap();
        let environment = std::fs::read_to_string(root.path().join("worker.env")).unwrap();
        assert!(environment.contains(&format!("HOME={}", root.path().display())));
        for secret in [
            "OPENAI_API_KEY",
            "OPENROUTER_API_KEY",
            "TACHYON_DAEMON_TOKEN",
            "broker-env-sentinel",
            "GHOST_TEST_BIN",
        ] {
            assert!(!environment.contains(secret));
        }
        assert!(store
            .model_permits
            .lock()
            .unwrap()
            .grants
            .values()
            .all(|grant| !grant.active || grant.closed.load(std::sync::atomic::Ordering::Acquire)));
        assert!(store
            .campaign_ledger(&request.identity.campaign_id)
            .unwrap()
            .unwrap()
            .allocations
            .is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_preparation_cannot_leave_live_permit_or_spawn() {
    let (_dir, store, funding, request) = tests::setup();
    let workspace = tempfile::tempdir().unwrap();
    let store = Arc::new(store);
    let broker = ModelBroker::new(store.clone(), model(&request));
    let (release, writer) = locked_writer(store.clone()).await;
    let launch = broker.launch_private(
        std::path::Path::new("/bin/true"),
        workspace.path(),
        workspace.path(),
        funding.clone(),
        request.clone(),
        work(&funding),
        Instant::now() + Duration::from_millis(50),
    );
    assert!(launch.await.is_err());
    release.send(()).unwrap();
    writer.await.unwrap();
    // Drain the late preparation through its final durable launch claim, not a
    // sleep followed by runtime shutdown with unobserved blocking work.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let done = store
                .storage(|store| {
                    let tx = store.database.begin_write().map_err(|e| e.to_string())?;
                    let table = tx
                        .open_table(TableDefinition::<&str, &str>::new(
                            "campaign_ghost_launches",
                        ))
                        .map_err(|e| e.to_string())?;
                    let done = table.iter().map_err(|e| e.to_string())?.next().is_some();
                    Ok(done)
                })
                .await
                .unwrap();
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let state = store.model_permits.lock().unwrap();
    assert_eq!(state.grants.len(), 1);
    let (nonce, grant) = state.grants.iter().next().unwrap();
    assert!(grant.closed.load(std::sync::atomic::Ordering::Acquire));
    let permit = ModelPermit(*nonce);
    drop(state);
    assert!(store
        .model_permit_accounting(Some(&permit), "after-cancel")
        .unwrap()
        .reserve_only(&request)
        .is_err());
}
