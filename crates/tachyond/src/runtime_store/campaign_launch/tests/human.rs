use super::*;
use crate::runtime_store::{
    campaign_ledger::{LedgerCommand, Usage},
    execution::{
        command::{CommandGateRecord, COMMAND_GATES},
        ExecutionRecord, EXECUTIONS,
    },
};
use sha2::{Digest, Sha256};
use tachyon_api::{
    campaign::{
        AcceptanceDecision, AcceptanceMode, AcceptanceQuery, AcceptanceRequest, HumanDecision,
    },
    types::{ArtifactPublication, ArtifactRegistration},
};

fn human_manifest(m: &mut CampaignManifest) {
    m.evaluator.acceptance_mode = Some(AcceptanceMode::Human);
    m.evaluator.argv.clear();
    m.evaluator.timeout_ms = 0;
    m.evaluator.output_bytes = 0;
    m.evaluator.max_total_command_ms = 0;
    m.evaluator.max_attempts = 1;
}

fn query(id: &str) -> ApiRequest {
    ApiRequest::CampaignAcceptanceGet(AcceptanceQuery {
        campaign_id: id.into(),
    })
}

fn decision(request: &AcceptanceRequest, accept: bool) -> ApiRequest {
    ApiRequest::CampaignAcceptanceDecide(AcceptanceDecision {
        command_id: "operator-decision-1".into(),
        campaign_id: request.campaign_id.clone(),
        candidate: request.candidate.clone(),
        candidate_sha256: request.candidate_sha256.clone(),
        expected_state_sha256: request.expected_state_sha256.clone(),
        decision: if accept {
            HumanDecision::Accept
        } else {
            HumanDecision::Reject
        },
        confirm: true,
    })
}

/// Host-seeded known stopped evidence for transactional tests; no simulated worker authority.
fn pending(
    unknown_billing: bool,
) -> (
    tempfile::TempDir,
    Arc<RuntimeStore>,
    CampaignManifest,
    AcceptanceRequest,
) {
    let (dir, store, mut m) = fixture();
    human_manifest(&mut m);
    let work = format!("{}-root", m.campaign_id);
    let verifier = format!("{}-verification", m.campaign_id);
    store
        .host_authorize_campaign_envelope(
            "grant",
            &m.campaign_id,
            Envelope {
                work: Units {
                    tokens: 100,
                    cost_micro_usd: 100,
                },
                verification: Units {
                    tokens: 10,
                    cost_micro_usd: 10,
                },
                max_active_inferences: 2,
            },
        )
        .unwrap();
    for (id, pool, amount) in [
        (&work, Pool::Work, 100),
        (&verifier, Pool::Verification, 10),
    ] {
        store
            .admit_campaign_work(Admission {
                work_id: id.clone(),
                campaign_id: m.campaign_id.clone(),
                objective: m.objective.clone(),
                generation: 1,
                instruction_revision: 1,
                pool,
                upper_bound: Units {
                    tokens: amount,
                    cost_micro_usd: amount,
                },
            })
            .unwrap();
    }
    store
        .dispatch_campaign_batch(2, |_| DispatchOutcome::Registered {
            worker_id: "host-fixture".into(),
        })
        .unwrap();
    let config = CommandEvaluator {
        acceptance_mode: Some(AcceptanceMode::Human),
        result_contract: Default::default(),
        metrics: Default::default(),
        allow_extra_metrics: false,
        stage: None,
        argv: vec![],
        cwd: ".".into(),
        timeout_ms: 0,
        output_bytes: 0,
        input_bytes: 1024,
        max_attempts: 1,
        max_total_command_ms: 0,
    };
    let policy = ExecutionPolicy {
        funding: store.admitted_work(&m.campaign_id, &work).unwrap(),
        verification: store.admitted_work(&m.campaign_id, &verifier).unwrap(),
        evaluator_id: format!("command:{}", config.config_hash().unwrap()),
        work: WorkRequest {
            work_id: work.clone(),
            objective: m.objective.clone(),
            generation: 1,
            assignment: 1,
            lifetime_class: LifetimeClass::Short,
            deadline_ms: m.deadline_ms,
            attempt: None,
            constraints: None,
            context_refs: vec![],
        },
        model: RequestReservation {
            identity: WorkIdentity {
                campaign_id: m.campaign_id.clone(),
                work_id: work.clone(),
                attempt_id: "host-attempt".into(),
                generation: 1,
                instruction_revision: 1,
                class: RequestClass::Work,
            },
            estimate: RequestEstimate {
                base_url: "http://127.0.0.1:1".into(),
                model: "fixture".into(),
                provider: "openrouter".into(),
                pricing_revision: "fixture".into(),
                max_request_bytes: 32000,
                input_tokens: 20,
                output_tokens: 10,
                input_micro_usd_per_million: 1000000,
                output_micro_usd_per_million: 1000000,
                other_micro_usd: 0,
            },
        },
    };
    for funding in [&policy.funding, &policy.verification] {
        store
            .campaign_ledger_command(
                &format!("fund:{}", funding.dispatch_id),
                &m.campaign_id,
                LedgerCommand::FundAllocation {
                    reservation_id: funding.dispatch_id.clone(),
                    work_id: funding.admission.work_id.clone(),
                },
            )
            .unwrap();
    }
    if unknown_billing {
        store
            .campaign_ledger_command(
                "unknown",
                &m.campaign_id,
                LedgerCommand::ReserveAllocated {
                    reservation_id: "unknown-model".into(),
                    allocation_id: policy.funding.dispatch_id.clone(),
                    pool: Pool::Work,
                    reserved: Units {
                        tokens: 30,
                        cost_micro_usd: 30,
                    },
                },
            )
            .unwrap();
    }
    let workspace = dir.path().join("work");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(workspace.join("result"), b"original").unwrap();
    let artifacts = ArtifactStore::open(&dir.path().join("artifacts")).unwrap();
    let snapshot = artifacts
        .register(
            &work,
            &workspace,
            ArtifactRegistration {
                id: "candidate-1".into(),
                path: "result".into(),
                kind: "file".into(),
                description: "fixture".into(),
                size_bytes: 8,
                sha256: format!("{:x}", Sha256::digest(b"original")),
                task_id: None,
                work_id: Some(work.clone()),
                generation: Some(1),
                assignment: Some(1),
                attempt_id: Some("host-attempt".into()),
                publication: ArtifactPublication::Pending,
            },
        )
        .unwrap();
    let record: ExecutionRecord = serde_json::from_value(serde_json::json!({"schema_version":1,"policy":policy,"phase":"EvidenceReady","candidate":{"work_id":work,"objective":"fixture","generation":1,"assignment":1,"outcome":"completed","result":"candidate","artifacts":["result"],"candidate_refs":[snapshot.id]},"settled":false})).unwrap();
    let gate = CommandGateRecord {
        human_request: None,
        human_receipt: None,
        human_diagnostic: None,
        policy: policy.clone(),
        config,
        snapshot: None,
        select_collected: true,
        staging_root: dir.path().into(),
        evidence: None,
        evidence_at_ms: None,
        finalize_rework: false,
        original_policy: None,
        history: vec![],
        deadline_ms: Some(m.deadline_ms),
    };
    let tx = store.database.begin_write().unwrap();
    tx.open_table(EXECUTIONS)
        .unwrap()
        .insert(
            work.as_str(),
            serde_json::to_vec(&record).unwrap().as_slice(),
        )
        .unwrap();
    tx.open_table(COMMAND_GATES)
        .unwrap()
        .insert(work.as_str(), serde_json::to_vec(&gate).unwrap().as_slice())
        .unwrap();
    tx.commit().unwrap();
    let prepared = store
        .prepare_human_acceptance(&record, Some(&artifacts))
        .unwrap()
        .unwrap();
    assert_eq!(prepared.phase, ExecutionPhase::AwaitingAcceptance);
    std::fs::write(workspace.join("result"), b"modified").unwrap();
    assert_eq!(
        artifacts.read(&work, &snapshot.id, 0, 64).unwrap(),
        b"original"
    );
    let ApiResponse::CampaignAcceptance {
        request: Some(request),
        receipt: None,
    } = store.acceptance_request(&query(&m.campaign_id)).unwrap()
    else {
        panic!()
    };
    (dir, store, m, request)
}

#[test]
fn human_decision_is_fenced_idempotent_attributed_and_persists_without_refunding_unknown_usage() {
    for unknown in [false, true] {
        let (dir, store, m, request) = pending(unknown);
        let before = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
        assert_eq!(before.active_inferences(), usize::from(unknown));
        assert!(before.allocations.values().all(|closed| !closed));
        let pending_record = store
            .campaign_execution(&m.campaign_id, &request.work_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            before.reservations
                [&crate::runtime_store::execution::evaluation_receipt(&pending_record.policy)]
                .usage,
            Usage::Final(Units::default())
        );
        assert!(store
            .host_finish_campaign_evaluation(&pending_record, Evaluation::Accepted)
            .is_err());
        let accept = decision(&request, true);
        for field in [
            "candidate",
            "candidate_sha256",
            "expected_state_sha256",
            "confirm",
        ] {
            let mut wrong = serde_json::to_value(&accept).unwrap();
            wrong[field] = if field == "confirm" {
                false.into()
            } else if field == "candidate" {
                "other".into()
            } else {
                "f".repeat(64).into()
            };
            assert!(store
                .acceptance_request(&serde_json::from_value(wrong).unwrap())
                .is_err());
        }
        assert_eq!(
            store.campaign_ledger(&m.campaign_id).unwrap().unwrap(),
            before
        );
        let result = store.acceptance_request(&accept).unwrap();
        let ApiResponse::CampaignAcceptance {
            request: None,
            receipt: Some(receipt),
        } = &result
        else {
            panic!()
        };
        assert_eq!(receipt.source, AcceptanceMode::Human);
        assert_eq!(receipt.host_uid, nix::unistd::Uid::effective().as_raw());
        assert_eq!(receipt.request, request);
        assert_eq!(
            serde_json::to_value(store.acceptance_request(&accept).unwrap()).unwrap(),
            serde_json::to_value(&result).unwrap()
        );
        assert!(store
            .acceptance_request(&decision(&request, false))
            .is_err());
        let mut stale = accept.clone();
        if let ApiRequest::CampaignAcceptanceDecide(d) = &mut stale {
            d.command_id = "new-command-old-state".into();
        }
        assert!(store.acceptance_request(&stale).is_err());
        let record = store
            .campaign_execution(&m.campaign_id, &request.work_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            record.phase,
            ExecutionPhase::Reviewed(Evaluation::AcceptedHuman)
        );
        assert_eq!(record.settled, !unknown);
        assert!(store
            .campaign_command_gate(&m.campaign_id, &request.work_id)
            .unwrap()
            .unwrap()
            .evidence
            .is_none());
        let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
        if unknown {
            assert_eq!(ledger.reservations["unknown-model"].usage, Usage::Unknown);
            assert!(ledger.allocations.values().all(|closed| !closed));
        } else {
            assert!(ledger.allocations.values().all(|closed| *closed));
        }
        drop(store);
        let reopened = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        assert_eq!(
            serde_json::to_value(reopened.acceptance_request(&accept).unwrap()).unwrap(),
            serde_json::to_value(&result).unwrap()
        );
        assert_eq!(
            reopened.campaign_ledger(&m.campaign_id).unwrap().unwrap(),
            ledger
        );
        if unknown {
            reopened
                .campaign_ledger_command(
                    "final-model",
                    &m.campaign_id,
                    LedgerCommand::Reconcile {
                        reservation_id: "unknown-model".into(),
                        usage: Usage::Final(Units {
                            tokens: 7,
                            cost_micro_usd: 7,
                        }),
                    },
                )
                .unwrap();
            assert!(
                reopened
                    .settle_campaign_execution(&m.campaign_id, &request.work_id)
                    .unwrap()
                    .settled
            );
        }
    }
}

#[test]
fn human_rejection_cancel_deadline_and_newer_steering_never_accept_old_state() {
    for scenario in ["reject", "cancel", "deadline", "steer", "steer-verifier"] {
        let (_dir, store, m, request) = pending(false);
        let accept = decision(&request, true);
        match scenario {
            "reject" => {
                store
                    .acceptance_request(&decision(&request, false))
                    .unwrap();
            }
            "cancel" => {
                store.poll_human_acceptance(&m.campaign_id, true).unwrap();
            }
            "deadline" => {
                let tx = store.database.begin_write().unwrap();
                let mut gate = store
                    .campaign_command_gate(&m.campaign_id, &request.work_id)
                    .unwrap()
                    .unwrap();
                gate.deadline_ms = Some(now() - 1);
                tx.open_table(COMMAND_GATES)
                    .unwrap()
                    .insert(
                        request.work_id.as_str(),
                        serde_json::to_vec(&gate).unwrap().as_slice(),
                    )
                    .unwrap();
                tx.commit().unwrap();
            }
            "steer" | "steer-verifier" => {
                let work = if scenario == "steer-verifier" {
                    format!("{}-verification", m.campaign_id)
                } else {
                    request.work_id.clone()
                };
                let admission = store
                    .admitted_work(&m.campaign_id, &work)
                    .unwrap()
                    .admission;
                let tx = store.database.begin_write().unwrap();
                RuntimeStore::continuation_instruction_in(
                    &tx,
                    &admission,
                    "host-steer",
                    "new host instruction",
                )
                .unwrap();
                tx.commit().unwrap();
            }
            _ => unreachable!(),
        }
        assert!(store.acceptance_request(&accept).is_err(), "{scenario}");
        let record = store
            .campaign_execution(&m.campaign_id, &request.work_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            record.phase,
            ExecutionPhase::Reviewed(if scenario == "reject" {
                Evaluation::Rejected
            } else {
                Evaluation::Unverified
            })
        );
        assert!(record.settled);
        assert!(record.candidate.is_some());
        assert_ne!(status(&store, &m), CampaignStatus::AcceptedHuman);
    }
}

#[test]
fn human_cancel_races_decision_without_later_old_acceptance() {
    let (_dir, store, m, request) = pending(true);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let cancel_store = store.clone();
    let cancel_barrier = barrier.clone();
    let campaign = m.campaign_id.clone();
    let cancel = std::thread::spawn(move || {
        cancel_barrier.wait();
        cancel_store.poll_human_acceptance(&campaign, true).unwrap();
    });
    barrier.wait();
    let accepted = store.acceptance_request(&decision(&request, true)).is_ok();
    cancel.join().unwrap();
    let record = store
        .campaign_execution(&m.campaign_id, &request.work_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        record.phase,
        ExecutionPhase::Reviewed(if accepted {
            Evaluation::AcceptedHuman
        } else {
            Evaluation::Unverified
        })
    );
    let mut stale = decision(&request, true);
    if let ApiRequest::CampaignAcceptanceDecide(d) = &mut stale {
        d.command_id = "later-old-accept".into();
    }
    assert!(store.acceptance_request(&stale).is_err());
    let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
    assert_eq!(ledger.reservations["unknown-model"].usage, Usage::Unknown);
    assert!(ledger.allocations.values().all(|closed| !closed));
    assert!(!record.settled);
}

#[test]
fn pending_human_acceptance_reopens_without_starting_jobs_and_one_decision_wins() {
    let (dir, store, m, request) = pending(false);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    assert!(service.active.lock().unwrap().is_empty());
    let ApiResponse::CampaignAcceptance {
        request: Some(reopened),
        ..
    } = store.acceptance_request(&query(&m.campaign_id)).unwrap()
    else {
        panic!()
    };
    assert_eq!(reopened, request);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = [true, false]
        .into_iter()
        .map(|accept| {
            let store = store.clone();
            let barrier = barrier.clone();
            let mut action = decision(&request, accept);
            if let ApiRequest::CampaignAcceptanceDecide(d) = &mut action {
                d.command_id = format!("concurrent-{accept}");
            }
            std::thread::spawn(move || {
                barrier.wait();
                store.acceptance_request(&action).is_ok()
            })
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .filter_map(|thread| thread.join().unwrap().then_some(()))
            .count(),
        1
    );
    assert!(service.active.lock().unwrap().is_empty());
    assert!(
        store
            .campaign_execution(&m.campaign_id, &request.work_id)
            .unwrap()
            .unwrap()
            .settled
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake provider only"]
async fn actual_ghost_human_acceptance_typed_dispatch_no_pending_model_or_execution() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for scenario in [
        "accept",
        "reject",
        "cancel",
        "deadline",
        "steer",
        "steer-verifier",
    ] {
        let (dir, store, mut m) = fixture();
        human_manifest(&mut m);
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        m.workspace = workspace.path().canonicalize().unwrap();
        m.home = home.path().canonicalize().unwrap();
        m.executable =
            PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"))
                .canonicalize()
                .unwrap();
        if scenario == "deadline" {
            m.deadline_ms = now() + 5000;
        }
        m.validate(now()).unwrap();
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
                let n = counted.fetch_add(1, Ordering::SeqCst);
                assert!(
                    n < 3,
                    "no additional inference is allowed for human acceptance"
                );
                let tool = match n {
                    0 => Some((
                        "write",
                        serde_json::json!({"path":"result","content":"original"}),
                    )),
                    1 => Some((
                        "artifact",
                        serde_json::json!({"path":"result","kind":"file","description":"human fixture"}),
                    )),
                    _ => Some((
                        "work",
                        serde_json::json!({"action":"complete","summary":"Candidate ready for host review","candidate_refs":["untrusted-reference"],"unresolved_questions":[]}),
                    )),
                };
                let delta = if let Some((name, args)) = tool {
                    assert!(wire["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|t| t["function"]["name"] == name));
                    serde_json::json!({"tool_calls":[{"index":0,"id":format!("fixture-{n}"),"function":{"name":name,"arguments":args.to_string()}}]})
                } else {
                    serde_json::json!({"content":"Candidate ready for host review"})
                };
                let response = format!(
                    "data: {}\n\ndata: [DONE]\n\n",
                    serde_json::json!({"choices":[{"delta":delta}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.000007}})
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
        let service = Arc::new(CampaignService::new(store.clone(), dir.path().into()).unwrap());
        let run_store = store.clone();
        let root = dir.path().to_owned();
        service
            .launch(&m, base_url, false, move |launch, cancel| {
                execute(run_store, root, launch, model, cancel)
            })
            .unwrap();
        let registry = Arc::new(Mutex::new(crate::Registry {
            runtime_store: Some(store.clone()),
            campaigns: Some(service.clone()),
            ..Default::default()
        }));
        let (mut operator, server) = std::os::unix::net::UnixStream::pair().unwrap();
        operator
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let handler = std::thread::spawn(move || crate::handle_connection(server, registry));
        let mut reader = std::io::BufReader::new(operator.try_clone().unwrap());
        let mut dispatch = |request: &ApiRequest| {
            use std::io::{BufRead, Write};
            let mut bytes = serde_json::to_vec(request).unwrap();
            bytes.push(b'\n');
            operator.write_all(&bytes).unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            serde_json::from_str::<ApiResponse>(&line).unwrap()
        };
        let request = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let ApiResponse::CampaignAcceptance {
                    request: Some(request),
                    ..
                } = dispatch(&query(&m.campaign_id))
                {
                    break request;
                }
                assert!(
                    !service.active.lock().unwrap()[&m.campaign_id]
                        .task
                        .is_finished(),
                    "execution ended before human request: {scenario}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            request.candidate_sha256,
            format!("{:x}", Sha256::digest(b"original"))
        );
        assert_eq!(status(&store, &m), CampaignStatus::AwaitingAcceptance);
        let candidate = store
            .campaign_execution(&m.campaign_id, &request.work_id)
            .unwrap()
            .unwrap()
            .candidate
            .unwrap();
        assert_eq!(candidate.instruction_revision, Some(1));
        assert_eq!(
            candidate.outcome.completed_result(),
            Some("Candidate ready for host review")
        );
        assert_eq!(
            candidate.candidate_refs.as_deref(),
            Some(std::slice::from_ref(&request.candidate))
        );
        assert_ne!(request.candidate, "untrusted-reference");
        assert_eq!(
            store
                .campaign_ledger(&m.campaign_id)
                .unwrap()
                .unwrap()
                .active_inferences(),
            0
        );
        assert_eq!(
            store.host_capacity.execution.available(),
            store.host_capacity.limits.max_execution_jobs
        );
        assert_eq!(
            store.host_capacity.model.available(),
            store.host_capacity.limits.max_model_calls
        );
        assert_eq!(
            store.host_capacity.resident.available(),
            store.host_capacity.limits.max_resident_workers
        );
        assert_eq!(
            store.host_capacity.cpu.available_permits(),
            store.host_capacity.limits.max_cpu_jobs
        );
        let ApiResponse::CampaignProgress { activity, .. } =
            dispatch(&ApiRequest::CampaignProgress {
                id: m.campaign_id.clone(),
            })
        else {
            panic!()
        };
        assert_eq!(activity.active, 0);
        assert!(activity.waiting > 0);
        assert!(matches!(
            dispatch(&ApiRequest::CampaignAttentionAnswer {
                id: m.campaign_id.clone(),
                work_id: request.work_id.clone(),
                request_id: request.candidate.clone(),
                generation: request.generation,
                instruction_revision: request.instruction_revision,
                answer: "yes".into(),
            }),
            ApiResponse::Error { .. }
        ));
        assert_eq!(status(&store, &m), CampaignStatus::AwaitingAcceptance);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        tokio::fs::write(workspace.path().join("result"), b"modified")
            .await
            .unwrap();
        let accept = decision(&request, true);
        for field in ["candidate", "expected_state_sha256"] {
            let mut invalid = serde_json::to_value(&accept).unwrap();
            invalid[field] = if field == "candidate" {
                "other".into()
            } else {
                "0".repeat(64).into()
            };
            assert!(matches!(
                dispatch(&serde_json::from_value(invalid).unwrap()),
                ApiResponse::Error { .. }
            ));
        }
        let reply = match scenario {
            "accept" | "reject" => {
                let action = decision(&request, scenario == "accept");
                let reply = dispatch(&action);
                assert!(
                    matches!(
                        &reply,
                        ApiResponse::CampaignAcceptance {
                            receipt: Some(_),
                            ..
                        }
                    ),
                    "{reply:?}"
                );
                assert_eq!(
                    serde_json::to_value(dispatch(&action)).unwrap(),
                    serde_json::to_value(&reply).unwrap()
                );
                assert!(matches!(
                    dispatch(&decision(&request, scenario != "accept")),
                    ApiResponse::Error { .. }
                ));
                Some(reply)
            }
            "cancel" => {
                dispatch(&ApiRequest::CampaignCancel {
                    id: m.campaign_id.clone(),
                });
                None
            }
            "deadline" => {
                tokio::time::sleep(Duration::from_millis(
                    m.deadline_ms.saturating_sub(now()) + 50,
                ))
                .await;
                None
            }
            "steer" | "steer-verifier" => {
                let work = if scenario == "steer-verifier" {
                    format!("{}-verification", m.campaign_id)
                } else {
                    request.work_id.clone()
                };
                let admission = store
                    .admitted_work(&m.campaign_id, &work)
                    .unwrap()
                    .admission;
                let tx = store.database.begin_write().unwrap();
                RuntimeStore::continuation_instruction_in(
                    &tx,
                    &admission,
                    "newer-host-steering",
                    "new instruction",
                )
                .unwrap();
                tx.commit().unwrap();
                None
            }
            _ => unreachable!(),
        };
        if reply.is_none() {
            assert!(matches!(dispatch(&accept), ApiResponse::Error { .. }));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while !service.active.lock().unwrap()[&m.campaign_id]
                .task
                .is_finished()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let record = store
            .campaign_execution(&m.campaign_id, &request.work_id)
            .unwrap()
            .unwrap();
        assert!(record.settled);
        assert_eq!(
            record.phase,
            ExecutionPhase::Reviewed(match scenario {
                "accept" => Evaluation::AcceptedHuman,
                "reject" => Evaluation::Rejected,
                _ => Evaluation::Unverified,
            })
        );
        let gate = store
            .campaign_command_gate(&m.campaign_id, &request.work_id)
            .unwrap()
            .unwrap();
        assert!(
            gate.evidence.is_none(),
            "human acceptance must not fabricate command evidence"
        );
        drop(dispatch);
        drop(reader);
        drop(operator);
        handler.join().unwrap().unwrap();
        service.shutdown();
        let artifacts = ArtifactStore::open(
            &dir.path()
                .join("campaigns")
                .join(&m.campaign_id)
                .join("artifacts"),
        )
        .unwrap();
        assert_eq!(
            artifacts
                .read(&request.work_id, &request.candidate, 0, 64)
                .unwrap(),
            b"original"
        );
        http.abort();
        let _ = http.await;
    }
}
