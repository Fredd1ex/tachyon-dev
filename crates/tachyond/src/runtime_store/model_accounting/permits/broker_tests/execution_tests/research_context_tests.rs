use super::*;
use crate::runtime_store::execution::{ExecutionRecord, EXECUTIONS};
use serde_json::json;
use tachyon_api::{
    agents::{Control, Reply, Request as ControlRequest},
    context::*,
};
use tachyond::{artifact_store::ArtifactStore, verification::CommandEvaluator};

// Synthetic evaluator observations, persisted in the real lifecycle record format.
fn retained(
    store: &RuntimeStore,
    request: &RequestReservation,
    artifacts: Option<&ArtifactStore>,
    workspace: &std::path::Path,
) {
    use sha2::{Digest, Sha256};
    let funding = store
        .admitted_work(&request.identity.campaign_id, "child")
        .unwrap();
    let mut request = request.clone();
    request.identity.work_id = "child".into();
    let mut policy = policy(store, &funding, request);
    let config = CommandEvaluator {
        acceptance_mode: None,
        result_contract: Default::default(),
        metrics: Default::default(),
        allow_extra_metrics: false,
        stage: None,
        argv: vec!["/bin/true".into()],
        cwd: ".".into(),
        timeout_ms: 100,
        output_bytes: 1024,
        input_bytes: 4096,
        max_attempts: 2,
        max_total_command_ms: 200,
    };
    policy.evaluator_id = format!("command:{}", config.config_hash().unwrap());
    let mut records = Vec::new();
    for (id, phase, outcome, at) in [
        ("retained-1", "Rejected", "Fail", 10),
        ("current-2", "Accepted", "Pass", 20),
    ] {
        policy.model.identity.attempt_id = id.into();
        let snapshot = artifacts.map(|store| {
            let bytes = id.repeat(100);
            std::fs::write(workspace.join(id), &bytes).unwrap();
            store
                .register(
                    "child",
                    workspace,
                    tachyon_api::ArtifactRegistration {
                        id: id.into(),
                        path: id.into(),
                        kind: "candidate".into(),
                        description: "fixture".into(),
                        size_bytes: bytes.len() as u64,
                        sha256: format!("{:x}", Sha256::digest(bytes.as_bytes())),
                        task_id: None,
                        work_id: Some("child".into()),
                        generation: Some(1),
                        assignment: Some(1),
                        attempt_id: Some(id.into()),
                        publication: Default::default(),
                    },
                )
                .unwrap()
        });
        let candidate = json!({"work_id":"child", "objective":"bounded", "generation":1, "assignment":1,
            "attempt_id":id, "candidate_refs":[id], "outcome":"completed", "result":"RAW_CANDIDATE_NOT_CONTEXT"});
        let execution = json!({"schema_version":1, "policy":policy, "phase":{"Reviewed":phase}, "candidate":candidate, "settled":false});
        let _: ExecutionRecord = serde_json::from_value(execution.clone()).unwrap();
        records.push(json!({"execution":execution, "snapshot":snapshot, "evidence_at_ms":at,
            "evidence":{"config_hash":config.config_hash().unwrap(), "candidate_sha256":snapshot.as_ref().map(|s| s.sha256.clone()).unwrap_or_else(|| "a".repeat(64)),
                "artifact_id":id, "outcome":outcome, "exit_code":if outcome == "Fail" { 1 } else { 0 }, "elapsed_ms":1,
                "stdout":[82,65,87,95,76,79,71], "stderr":[], "truncated":false,"diagnostic":"RAW_LOG_NOT_CONTEXT"}}));
    }
    let current = records.pop().unwrap();
    let gate = json!({"policy":policy,"config":config,"snapshot":current["snapshot"],"staging_root":workspace,
        "evidence":current["evidence"],"evidence_at_ms":current["evidence_at_ms"],"finalize_rework":false,"history":records});
    let tx = store.database.begin_write().unwrap();
    tx.open_table(EXECUTIONS)
        .unwrap()
        .insert(
            "child",
            serde_json::to_vec(&current["execution"])
                .unwrap()
                .as_slice(),
        )
        .unwrap();
    tx.open_table(redb::TableDefinition::<&str, &[u8]>::new(
        "campaign_command_gates_v1",
    ))
    .unwrap()
    .insert("child", serde_json::to_vec(&gate).unwrap().as_slice())
    .unwrap();
    tx.commit().unwrap();
}

fn query() -> Query {
    Query {
        literal: None,
        after: None,
        limit: 16,
        since_ms: None,
        version: None,
    }
}

#[test]
fn research_context_scan_and_serialized_byte_budgets_page_without_losing_results() {
    let (dir, store, funding, reservation) = tests::setup();
    store
        .campaign_ledger_command(
            "release-unused-fixture",
            &reservation.identity.campaign_id,
            LedgerCommand::Reconcile {
                reservation_id: funding.dispatch_id.clone(),
                usage: Usage::Final(Units::default()),
            },
        )
        .unwrap();
    store
        .admit_campaign_work(Admission {
            work_id: "child".into(),
            upper_bound: Units {
                tokens: 10,
                cost_micro_usd: 10,
            },
            ..funding.admission.clone()
        })
        .unwrap();
    store
        .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
            worker_id: "fixture-child".into(),
        })
        .unwrap();
    retained(&store, &reservation, None, dir.path());
    let campaign = &reservation.identity.campaign_id;
    let attempts = store
        .host_research_context(campaign, &Request::Attempts { query: query() }, None)
        .unwrap();
    for n in 0..70 {
        let gap = store
            .admit_campaign_work(Admission {
                work_id: format!("a-gap-{n:03}"),
                upper_bound: Units {
                    tokens: 1,
                    cost_micro_usd: 1,
                },
                ..funding.admission.clone()
            })
            .unwrap();
        store
            .campaign_ledger_command(
                &format!("gap-final-{n}"),
                campaign,
                LedgerCommand::Reconcile {
                    reservation_id: gap.dispatch_id,
                    usage: Usage::Final(Units::default()),
                },
            )
            .unwrap();
    }
    for n in 0..8 {
        store
            .host_record_finding(
                campaign,
                Finding {
                    id: format!("large-{n}"),
                    work_id: "child".into(),
                    author: "host".into(),
                    claim: "\\".repeat(1024),
                    conditions: "\\".repeat(1024),
                    evidence: vec![attempts.resources[0].reference.clone()],
                    parents: vec![],
                },
                None,
            )
            .unwrap();
    }
    let mut q = query();
    let mut found = Vec::new();
    q.literal = Some("large".into());
    let first = store
        .host_research_context(campaign, &Request::Search { query: q.clone() }, None)
        .unwrap();
    assert!(first.resources.is_empty());
    assert!(first.next_cursor.is_some());
    q.after = first.next_cursor;
    for _ in 0..10 {
        let page = store
            .host_research_context(campaign, &Request::Search { query: q.clone() }, None)
            .unwrap();
        assert!(
            serde_json::to_vec(&tachyon_model::broker::FrameReply::Control(
                Reply::Resource { page: page.clone() }
            ))
            .unwrap()
            .len()
                <= MAX_PAGE_BYTES
        );
        assert!(
            page.resources.len() <= 1,
            "byte cap is stricter than item cap"
        );
        found.extend(page.resources.into_iter().map(|r| r.reference.id));
        q.after = page.next_cursor;
        if q.after.is_none() {
            break;
        }
    }
    assert!(q.after.is_none());
    assert_eq!(found.len(), 8);
    found.sort();
    found.dedup();
    assert_eq!(found.len(), 8);
}

#[test]
fn research_context_reopen_findings_lineage_bounds_and_scope() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, store, _, reservation) = tests::setup_agents();
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let artifacts = ArtifactStore::open(root.path()).unwrap();
    retained(&store, &reservation, Some(&artifacts), workspace.path());
    let campaign = &reservation.identity.campaign_id;
    let before = store
        .host_research_context(campaign, &Request::Attempts { query: query() }, None)
        .unwrap();
    assert_eq!(before.resources.len(), 2);
    assert_eq!(before.resources[0].reference.id, "retained-1");
    assert_eq!(
        before.resources[1].data["parent"],
        json!(before.resources[0].reference)
    );
    assert!(!serde_json::to_string(&before).unwrap().contains("RAW_"));
    assert!(store.pending_history().unwrap().is_empty());
    assert!(store
        .host_research_context(campaign, &Request::Findings { query: query() }, None)
        .unwrap()
        .resources
        .is_empty());
    let finding = Finding {
        id: "conditional-claim".into(),
        work_id: "child".into(),
        author: "host-test".into(),
        claim: "Candidate failed under the recorded evaluator".into(),
        conditions: "Only this candidate hash and evaluator configuration".into(),
        evidence: vec![before.resources[0].reference.clone()],
        parents: vec![],
    };
    let reference = store
        .host_record_finding(campaign, finding.clone(), None)
        .unwrap();
    assert_eq!(
        reference,
        store
            .host_record_finding(campaign, finding.clone(), None)
            .unwrap()
    );
    for invalid in [
        Finding {
            conditions: String::new(),
            ..finding.clone()
        },
        Finding {
            evidence: vec![],
            ..finding.clone()
        },
        Finding {
            claim: "changed".into(),
            ..finding.clone()
        },
    ] {
        assert!(store.host_record_finding(campaign, invalid, None).is_err());
    }
    let mut wrong = finding.clone();
    wrong.id = "wrongscope".into();
    wrong.evidence[0].work_id = "foreign".into();
    assert!(store.host_record_finding(campaign, wrong, None).is_err());
    let mut derived = finding.clone();
    derived.id = "derived".into();
    derived.evidence = vec![reference.clone()];
    derived.parents = vec![reference];
    store.host_record_finding(campaign, derived, None).unwrap();
    let candidates = store
        .host_research_context(
            campaign,
            &Request::Artifacts { query: query() },
            Some(&artifacts),
        )
        .unwrap();
    assert_eq!(candidates.resources.len(), 2);
    let first = candidates.resources[0].reference.clone();
    let last = candidates.resources[1].reference.clone();
    {
        use sha2::{Digest, Sha256};
        assert_eq!(
            first.version,
            format!("{:x}", Sha256::digest("retained-1".repeat(100).as_bytes()))
        );
    }
    // Reads must use the immutable host snapshot, not the mutable source path.
    std::fs::write(workspace.path().join("retained-1"), "replacement").unwrap();
    assert!(store
        .host_record_candidate_lineage(campaign, last.clone(), vec![first.clone()], &artifacts)
        .is_err());
    store
        .host_record_candidate_lineage(campaign, first.clone(), vec![], &artifacts)
        .unwrap();
    store
        .host_record_candidate_lineage(campaign, last.clone(), vec![first.clone()], &artifacts)
        .unwrap();
    assert!(store
        .host_record_candidate_lineage(campaign, first.clone(), vec![last.clone()], &artifacts)
        .is_err());
    let bytes = store
        .host_research_context(
            campaign,
            &Request::Read {
                resource: first.clone(),
                offset: 1,
                limit: 1024,
            },
            Some(&artifacts),
        )
        .unwrap();
    assert_eq!(
        bytes.resources[0].data["bytes"],
        json!(&"retained-1".repeat(100).as_bytes()[1..])
    );
    let mut wrong_version = first;
    wrong_version.version = "b".repeat(64);
    assert!(store
        .host_research_context(
            campaign,
            &Request::Read {
                resource: wrong_version,
                offset: 0,
                limit: 1
            },
            Some(&artifacts)
        )
        .is_err());
    drop(store);
    drop(artifacts);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    let artifacts = ArtifactStore::open(root.path()).unwrap();
    assert_eq!(
        before,
        store
            .host_research_context(campaign, &Request::Attempts { query: query() }, None)
            .unwrap()
    );
    let after = store
        .host_research_context(
            campaign,
            &Request::Artifacts { query: query() },
            Some(&artifacts),
        )
        .unwrap();
    assert_eq!(
        after.resources[1].data["parents"],
        json!([candidates.resources[0].reference])
    );
    let mut q = query();
    q.limit = 1;
    let mut all = Vec::new();
    for _ in 0..10 {
        let page = store
            .host_research_context(
                campaign,
                &Request::Search { query: q.clone() },
                Some(&artifacts),
            )
            .unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() <= MAX_PAGE_BYTES);
        assert!(page.resources.len() <= 1);
        all.extend(page.resources);
        q.after = page.next_cursor;
        if q.after.is_none() {
            break;
        }
    }
    assert!(q.after.is_none());
    assert_eq!(all.len(), 6);
    for (literal, count) in [("RAW_", 0), ("Fail", 1), (".*", 0)] {
        let mut q = query();
        q.literal = Some(literal.into());
        assert_eq!(
            store
                .host_research_context(campaign, &Request::Attempts { query: q }, None)
                .unwrap()
                .resources
                .len(),
            count
        );
    }
    let mut q = query();
    q.since_ms = Some(11);
    assert_eq!(
        store
            .host_research_context(campaign, &Request::Attempts { query: q }, None)
            .unwrap()
            .resources
            .len(),
        1
    );
    let mut q = query();
    q.after = Some(json!({"campaign":"foreign", "stage":0,"key":"child","index":0}).to_string());
    assert!(store
        .host_research_context(campaign, &Request::Search { query: q }, None)
        .is_err());
    assert!(store
        .host_research_context("", &Request::Search { query: query() }, None)
        .is_err());
    assert!(serde_json::from_value::<Request>(
        json!({"action":"attempts","query":{"limit":1},"campaign_id":campaign})
    )
    .is_err());
    let mut q = query();
    q.literal = Some("x".repeat(257));
    assert!(Request::Search { query: q }.validate().is_err());
}

#[tokio::test]
async fn research_context_private_broker_rechecks_allowlist_scope_and_revocation() {
    use std::os::unix::fs::PermissionsExt;
    for allowed in [false, true] {
        let (dir, store, funding, reservation) = tests::setup_agents();
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let artifacts = Arc::new(ArtifactStore::open(root.path()).unwrap());
        retained(&store, &reservation, Some(&artifacts), dir.path());
        drop(store);
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let permit = store
            .host_issue_model_permit(reservation.clone(), funding, None)
            .unwrap();
        let broker = ModelBroker::new(store.clone(), model(&reservation))
            .with_research_artifacts(artifacts)
            .with_controls(if allowed {
                vec![Control::Resource]
            } else {
                vec![]
            });
        let (host, client) = tachyon_model::broker::private_pair().unwrap();
        let worker = async {
            let mut resolved = None;
            let reply = client
                .control(&ControlRequest::Resource {
                    request: Request::Attempts { query: query() },
                })
                .await
                .unwrap();
            if allowed {
                assert!(matches!(reply, Reply::Resource { page } if page.resources.len() == 2));
            } else {
                assert!(matches!(reply, Reply::Denied));
            }
            let reply = client
                .control(&ControlRequest::Resource {
                    request: Request::Artifacts { query: query() },
                })
                .await
                .unwrap();
            if allowed {
                let Reply::Resource { page } = reply else {
                    panic!("configured artifacts denied")
                };
                assert_eq!(page.resources.len(), 2);
                let mut resource = page.resources[0].reference.clone();
                resolved = Some(resource.clone());
                let reply = client
                    .control(&ControlRequest::Resource {
                        request: Request::Read {
                            resource: resource.clone(),
                            offset: 0,
                            limit: 1024,
                        },
                    })
                    .await
                    .unwrap();
                assert!(
                    matches!(reply, Reply::Resource { page } if page.resources[0].data["bytes"].as_array().unwrap().len() == 1000)
                );
                resource.work_id = "foreign".into();
                assert!(matches!(
                    client
                        .control(&ControlRequest::Resource {
                            request: Request::Read {
                                resource,
                                offset: 0,
                                limit: 1
                            },
                        })
                        .await
                        .unwrap(),
                    Reply::Denied
                ));
            } else {
                assert!(matches!(reply, Reply::Denied));
            }
            let mut q = query();
            q.after = Some(json!({"campaign":"foreign","stage":0,"key":"","index":0}).to_string());
            assert!(matches!(
                client
                    .control(&ControlRequest::Resource {
                        request: Request::Search { query: q }
                    })
                    .await
                    .unwrap(),
                Reply::Denied
            ));
            store.host_revoke_model_permit(&permit).unwrap();
            if let Some(resource) = resolved {
                assert!(store
                    .broker_control_with_context(
                        permit.0,
                        &reservation,
                        ControlRequest::Resource {
                            request: Request::Read {
                                resource,
                                offset: 0,
                                limit: 1
                            }
                        },
                        None,
                    )
                    .unwrap_err()
                    .contains("revoked"));
            }
            assert!(matches!(
                client
                    .control(&ControlRequest::Resource {
                        request: Request::Attempts { query: query() }
                    })
                    .await
                    .unwrap(),
                Reply::Denied
            ));
            drop(client);
        };
        let (_, ()) = tokio::join!(
            broker.serve_private(
                host,
                &permit,
                reservation.clone(),
                Instant::now() + Duration::from_secs(3)
            ),
            worker
        );
    }
}

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake model only"]
async fn actual_ghost_research_context_queries_retained_attempts_after_reopen() {
    let executable =
        std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"));
    let (dir, store, funding, mut reservation) = tests::setup_agents();
    retained(&store, &reservation, None, dir.path());
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let workspace = tempfile::tempdir().unwrap();
    let (url, mut wire, http) = fixture(store.clone(), "history").await;
    reservation.estimate.base_url = url;
    reservation.estimate.max_request_bytes = 32000;
    let broker =
        ModelBroker::new(store.clone(), model(&reservation)).with_controls([Control::Resource]);
    let work = WorkRequest {
        context_refs: vec![],
        constraints: None,
        attempt: None,
        work_id: "work".into(),
        objective: "retrieve prior observations".into(),
        generation: 1,
        assignment: 1,
        deadline_ms: (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 10000) as u64,
        lifetime_class: LifetimeClass::Short,
    };
    // The objective is immutable host assignment data.
    let work = WorkRequest {
        context_refs: vec![],
        objective: funding.admission.objective.clone(),
        ..work
    };
    broker
        .launch_private(
            &executable,
            workspace.path(),
            workspace.path(),
            funding,
            reservation,
            work,
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    let first = wire.recv().await.unwrap();
    assert!(first["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["function"]["name"] == "history"));
    assert!(!first["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["function"]["name"] == "agents"));
    assert!(!first["messages"].to_string().contains("retained-1"));
    let second = wire.recv().await.unwrap();
    let outputs: Vec<serde_json::Value> = second["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| serde_json::from_str(m["content"].as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(outputs.len(), 3);
    assert_eq!(outputs.iter().filter(|o| o["is_error"] == true).count(), 1);
    let text = serde_json::to_string(&outputs).unwrap();
    assert!(text.contains("retained-1"));
    assert!(text.contains("Rejected"));
    assert!(!text.contains("RAW_LOG_NOT_CONTEXT"));
    http.abort();
}
