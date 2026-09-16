use super::*;
use sha2::{Digest, Sha256};
use tachyon_api::continuation::{ContinuationRequest, StoppingReason};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn idle(service: &CampaignService, id: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while service
            .active
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|a| !a.task.is_finished())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("campaign did not finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires freshly built GHOST_TEST_BIN; localhost fake provider only"]
async fn actual_stopped_snapshot_continuation_is_fresh_and_idempotent() {
    let (dir, store, mut m) = fixture();
    let workspace = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let child_workspace = tempfile::tempdir().unwrap();
    let child_home = tempfile::tempdir().unwrap();
    m.executable = PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"))
        .canonicalize()
        .unwrap();
    m.workspace = workspace.path().canonicalize().unwrap();
    m.home = home.path().canonicalize().unwrap();
    m.max_request_bytes = 262144;
    m.deadline_ms = now() + 180000;
    m.max_active_inferences = 4;
    m.children = Some(tachyon_api::campaign::CampaignChildren {
        max_depth: 1,
        dynamic: None,
        total_work: 4,
        max_running: 1,
        max_resident: 2,
        controls: vec![
            tachyon_api::agents::Control::Status,
            tachyon_api::agents::Control::List,
            tachyon_api::agents::Control::Result,
            tachyon_api::agents::Control::Group,
            tachyon_api::agents::Control::GroupStatus,
            tachyon_api::agents::Control::Wait,
        ],
        history: true,
        completion: tachyon_api::campaign::ChildCompletion::CancelOutstanding,
        templates: vec![tachyon_api::campaign::CampaignTemplate {
            template_id: "unused".into(),
            group_id: Some("retained-group".into()),
            max_running: 1,
            specs: vec![tachyon_api::campaign::CampaignChild {
                evaluator: None,
                work_id: Some("unused-child".into()),
                objective: "unused".into(),
                workspace: child_workspace.path().canonicalize().unwrap(),
                home: child_home.path().canonicalize().unwrap(),
                work_tokens: 30,
                work_cost_micro_usd: 30,
                verification_tokens: 2,
                verification_cost_micro_usd: 2,
            }],
        }],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let http = tokio::spawn(async move {
        for index in 0..5 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
                assert!(header.len() < 16384);
            }
            let length: usize = std::str::from_utf8(&header)
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            assert!(length <= 262144);
            let mut body = vec![0; length];
            socket.read_exact(&mut body).await.unwrap();
            let body = String::from_utf8(body).unwrap();
            sent.send(body.clone()).unwrap();
            let mut delta = match index {
                0 => {
                    serde_json::json!({"tool_calls":[{"index":0,"id":"original-cell","function":{"name":"ipython","arguments":serde_json::json!({"code":"old_variable = 41\nfrom pathlib import Path\np = Path('sideeffect-counter')\np.write_text(str(int(p.read_text()) + 1) if p.exists() else '1')\na = require('agents')\ng = await a.group(template_id='unused', command_id='original-group', max_running=1)\nassert not g['is_error'], g\nw = await a.wait(work_ids=['unused-child'], mode='all', timeout_ms=1000)\nassert not w['is_error'], w\nawait work.ask(request_id='retained-question', question='Which result should be investigated?', timeout_ms=10)\nprint('original cell ran')\nprint('x' * 20000)"}).to_string()}}]})
                }
                2 => serde_json::json!({"tool_calls":[
                    {"index":0,"id":"final-cell","function":{"name":"ipython","arguments":serde_json::json!({"code":"artifact = require('artifact')\nfor description in ['first immutable observation', 'second immutable observation']:\n    r = await artifact.register(path='sideeffect-counter', kind='file', description=description)\n    assert not r['is_error'], r\nawait work.complete(summary='Stopped with retained evidence', candidate_refs=[], unresolved_questions=[])"}).to_string()}}
                ]}),
                3 => {
                    serde_json::json!({"tool_calls":[{"index":0,"id":"fresh-cell","function":{"name":"ipython","arguments":serde_json::json!({"code":"assert 'old_variable' not in globals()\nfrom pathlib import Path\nassert Path('sideeffect-counter').read_text() == '1'\na = require('agents')\ns = await a.status(work_id='unused-child')\nassert not s['is_error'], s\nimport json\nr = json.loads(s['content'])\nassert r['outcome'] == 'status'\nassert r['status']['work_id'] == 'unused-child'\ng = await a.group_status(group_id='retained-group')\nassert not g['is_error'], g\nr = await a.result(work_id='unused-child')\nassert not r['is_error'], r\nassert json.loads(r['content'])['snapshot'] is not None, r\nprint('fresh process, no old variables; logical child remains usable')"}).to_string()}}]})
                }
                _ => serde_json::json!({"content":"Stopped without publishing an artifact"}),
            };
            if index == 3 {
                let wire: serde_json::Value = serde_json::from_str(&body).unwrap();
                let context = wire["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|m| m["content"].as_str())
                    .flat_map(str::lines)
                    .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                    .find(|v| v["snapshot"]["stopping"].is_object())
                    .expect("stopping bootstrap");
                let refs: Vec<_> = context["snapshot"]["stopping"]["produced_resource_refs"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|r| r["kind"] == "artifact")
                    .collect();
                assert_eq!(refs.len(), 2);
                let mut arguments: serde_json::Value = serde_json::from_str(
                    delta["tool_calls"][0]["function"]["arguments"]
                        .as_str()
                        .unwrap(),
                )
                .unwrap();
                arguments["code"] = format!("{}\nh = require('history')\nrefs = json.loads({})\nfor ref in refs:\n    page = await h.read(resource=ref, offset=0, limit=1024)\n    assert not page['is_error'], page\n    assert json.loads(page['content'])['resources'][0]['data']['bytes'] == [49], page\nprint('retained artifacts read by exact version')",
                    arguments["code"].as_str().unwrap(),
                    serde_json::to_string(&serde_json::to_string(&refs).unwrap()).unwrap()).into();
                delta["tool_calls"][0]["function"]["arguments"] = arguments.to_string().into();
            }
            let response = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                serde_json::json!({"choices":[{"delta":delta}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"cost":0.000007}})
            );
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        }
        listener
    });
    let model = || {
        Model::new(ModelConfig {
            base_url: base.clone(),
            api_key: "localhost-only".into(),
            model: m.model.clone(),
            temperature: 0.0,
            max_completion_tokens: Some(m.output_tokens),
            context_length: None,
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: None,
            debug: false,
            debug_log: None,
        })
    };
    let service = Arc::new(CampaignService::new(store.clone(), dir.path().into()).unwrap());
    let run_store = store.clone();
    let root = dir.path().to_owned();
    let first_model = model();
    service
        .launch(&m, base.clone(), false, move |launch, signal| {
            execute(run_store, root, launch, first_model, signal)
        })
        .unwrap();
    idle(&service, &m.campaign_id).await;
    let work = format!("{}-root", m.campaign_id);
    let original = store
        .campaign_execution(&m.campaign_id, &work)
        .unwrap()
        .unwrap();
    assert_eq!(
        original.phase,
        ExecutionPhase::Reviewed(Evaluation::Unverified),
        "{original:?}"
    );
    assert!(!original.settled, "{original:?}; errors: {:?}", {
        let tx = store.database.begin_read().unwrap();
        tx.open_table(crate::runtime_store::execution::EXECUTION_ERRORS)
            .unwrap()
            .get(work.as_str())
            .unwrap()
            .map(|v| String::from_utf8_lossy(v.value()).into_owned())
    });
    let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
    let before = ledger
        .allocation_available(&original.policy.funding.dispatch_id)
        .unwrap();
    let tx = store.database.begin_write().unwrap();
    let mut snapshots = Vec::new();
    for row in tx
        .open_table(crate::runtime_store::research_context::traces::TRACES)
        .unwrap()
        .iter()
        .unwrap()
    {
        let (_, value) = row.unwrap();
        let resource: tachyon_api::context::Resource =
            serde_json::from_slice(value.value()).unwrap();
        if resource.reference.work_id == work && resource.data["phase"] == "work_context_snapshot" {
            snapshots.push(resource);
        }
    }
    snapshots.sort_by_key(|r| r.occurred_at_ms);
    assert_eq!(snapshots.len(), 3);
    drop(tx);
    let checkpoints: Vec<serde_json::Value> = service
        .reconciliation_inspection(&m.campaign_id)
        .unwrap()
        .iter()
        .filter_map(|line| line.strip_prefix("continuation_checkpoint: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|value: &serde_json::Value| value["checkpoint"]["work_id"] == work)
        .collect();
    assert_eq!(checkpoints.len(), snapshots.len());
    let current: Vec<_> = checkpoints
        .iter()
        .filter(|v| v["current_state"] == true)
        .collect();
    assert_eq!(current.len(), 1);
    assert_eq!(
        current[0]["checkpoint"],
        serde_json::to_value(&original.stopping_snapshot).unwrap()
    );
    let tx = store.database.begin_write().unwrap();
    let request = ContinuationRequest {
        schema_version: 1,
        command_id: "continue-once".into(),
        campaign_id: m.campaign_id.clone(),
        checkpoint: original.stopping_snapshot.clone().unwrap(),
        expected_state_sha256: super::super::reconciliation::state_hash(&tx).unwrap(),
        expected_stopping_reason: StoppingReason::StoppedUnverified,
        executable_sha256: format!(
            "{:x}",
            Sha256::digest(std::fs::read(&m.executable).unwrap())
        ),
        ghost_version: env!("CARGO_PKG_VERSION").into(),
        instruction: "Check the retained work without repeating previous side effects".into(),
        goal: Some(m.objective.clone()),
    };
    drop(tx);
    let mut wrong_binary = request.clone();
    wrong_binary.executable_sha256 = "0".repeat(64);
    assert!(service
        .continue_work(&m.campaign_id, &wrong_binary, true)
        .unwrap_err()
        .contains("configuration mismatch"));
    let mut stale = request.clone();
    stale.checkpoint = snapshots[0].reference.clone();
    let tx = store.database.begin_write().unwrap();
    let error = service.claim_continuation(&tx, &m, &stale).unwrap_err();
    assert!(error.contains("stale checkpoint"), "{error}");
    drop(tx);
    for scenario in ["state", "goal"] {
        let mut invalid = request.clone();
        match scenario {
            "state" => invalid.expected_state_sha256 = "0".repeat(64),
            _ => invalid.goal = Some("changed goal".into()),
        }
        let tx = store.database.begin_write().unwrap();
        assert!(service.claim_continuation(&tx, &m, &invalid).is_err());
    }
    for scenario in ["unknown", "closed", "expired", "exhausted"] {
        use crate::runtime_store::campaign_ledger::{LedgerCommand, Usage};
        let tx = store.database.begin_write().unwrap();
        match scenario {
            "expired" => {
                let mut gate = store
                    .campaign_command_gate(&m.campaign_id, &work)
                    .unwrap()
                    .unwrap();
                gate.deadline_ms = Some(now() - 1);
                tx.open_table(crate::runtime_store::execution::command::COMMAND_GATES)
                    .unwrap()
                    .insert(work.as_str(), serde_json::to_vec(&gate).unwrap().as_slice())
                    .unwrap();
            }
            "closed" => {
                RuntimeStore::campaign_ledger_command_in(
                    &tx,
                    "test-close",
                    &m.campaign_id,
                    LedgerCommand::CloseAllocation {
                        reservation_id: original.policy.funding.dispatch_id.clone(),
                    },
                )
                .unwrap();
            }
            _ => {
                RuntimeStore::campaign_ledger_command_in(
                    &tx,
                    "test-reserve",
                    &m.campaign_id,
                    LedgerCommand::ReserveAllocated {
                        reservation_id: "test-hold".into(),
                        allocation_id: original.policy.funding.dispatch_id.clone(),
                        pool: Pool::Work,
                        reserved: before,
                    },
                )
                .unwrap();
                if scenario == "exhausted" {
                    RuntimeStore::campaign_ledger_command_in(
                        &tx,
                        "test-final",
                        &m.campaign_id,
                        LedgerCommand::Reconcile {
                            reservation_id: "test-hold".into(),
                            usage: Usage::Final(before),
                        },
                    )
                    .unwrap();
                }
            }
        }
        let mut invalid = request.clone();
        invalid.expected_state_sha256 = super::super::reconciliation::state_hash(&tx).unwrap();
        let error = service.claim_continuation(&tx, &m, &invalid).unwrap_err();
        assert!(
            error.contains(match scenario {
                "unknown" => "operator reconciliation",
                "expired" => "deadline expired",
                _ => "no remaining allocation",
            }),
            "{scenario}: {error}"
        );
    }
    let mut threads = Vec::new();
    for _ in 0..2 {
        let service = service.clone();
        let store = store.clone();
        let m = m.clone();
        let base = base.clone();
        let request = request.clone();
        let root = dir.path().to_owned();
        let model = model();
        threads.push(std::thread::spawn(move || {
            service.launch_request(&m, base, true, Some(&request), move |launch, signal| {
                execute_prepared(store, root, launch, model, signal, true)
            })
        }));
    }
    for thread in threads {
        thread.join().unwrap().unwrap();
    }
    idle(&service, &m.campaign_id).await;
    let continued = store
        .campaign_execution(&m.campaign_id, &work)
        .unwrap()
        .unwrap();
    assert_ne!(
        continued.policy.model.identity.attempt_id,
        original.policy.model.identity.attempt_id
    );
    assert_eq!(continued.policy.work.assignment, 2);
    assert_eq!(continued.policy.model.identity.instruction_revision, 2);
    assert_eq!(continued.policy.funding, original.policy.funding);
    assert_eq!(continued.policy.verification, original.policy.verification);
    assert_eq!(
        continued.phase,
        ExecutionPhase::Reviewed(Evaluation::Unverified),
        "{continued:?}"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("sideeffect-counter")).unwrap(),
        "1"
    );
    let after = store
        .campaign_ledger(&m.campaign_id)
        .unwrap()
        .unwrap()
        .allocation_available(&original.policy.funding.dispatch_id)
        .unwrap();
    assert_eq!(before.tokens - after.tokens, 14);
    assert_eq!(before.cost_micro_usd - after.cost_micro_usd, 14);
    let after_ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
    for (id, reservation) in &ledger.reservations {
        assert_eq!(after_ledger.reservations.get(id), Some(reservation));
    }
    let mut competing = request.clone();
    competing.command_id = "competing-stale-command".into();
    let tx = store.database.begin_write().unwrap();
    assert!(service
        .claim_continuation(&tx, &m, &competing)
        .unwrap_err()
        .contains("stale continuation state"));
    drop(tx);
    let mut requests = Vec::new();
    while let Ok(body) = received.try_recv() {
        requests.push(body);
    }
    assert_eq!(requests.len(), 5);
    assert!(requests[3].contains("Host-authorized work instructions, revision 2"));
    assert!(requests[3].contains("durable_output_resources"));
    assert!(requests[3].contains("retained-question"));
    assert!(requests[3].contains("output:"));
    assert!(requests[3].contains("unused-child"));
    assert!(requests[3].contains("retained-group"));
    assert!(requests[3].contains(&request.checkpoint.version));
    assert!(!requests[3].contains("old_variable = 41"));
    assert!(requests[4].contains("fresh process, no old variables"));
    assert!(!requests[4].contains("AssertionError"));
    let wire: serde_json::Value = serde_json::from_str(&requests[4]).unwrap();
    let output = wire["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "tool")
        .unwrap();
    let output: serde_json::Value =
        serde_json::from_str(output["content"].as_str().unwrap()).unwrap();
    assert_eq!(output["is_error"], false, "{output}");
    assert_eq!(output["metadata"]["exit_code"], 0, "{output}");
    assert!(output["content"]
        .as_str()
        .unwrap()
        .contains("fresh process, no old variables; logical child remains usable"));
    let feedback = continued
        .policy
        .work
        .attempt
        .as_ref()
        .unwrap()
        .feedback
        .as_ref()
        .unwrap();
    let context: serde_json::Value =
        serde_json::from_str(feedback.lines().last().unwrap()).unwrap();
    let snapshot: tachyon_api::context::WorkContextSnapshot =
        serde_json::from_value(context["snapshot"].clone()).unwrap();
    let stopping = snapshot.stopping.unwrap();
    assert!(stopping
        .logical_work_handles
        .contains(&"unused-child".into()));
    assert!(stopping
        .group_handles
        .iter()
        .any(|g| g.group_id == "retained-group"));
    assert_eq!(
        stopping.activation_observation,
        tachyon_api::context::ActivationObservation::Final
    );
    let metadata = snapshot.worker_claims_informational_only.as_ref().unwrap();
    assert!(metadata.activated_packages.contains_key("artifact"));
    assert!(!metadata.known_output_handles.is_empty());
    assert!(!stopping.worker_observations_filtered);
    let artifact_refs: Vec<_> = stopping
        .produced_resource_refs
        .iter()
        .filter(|r| r.kind == tachyon_api::context::ResourceKind::Artifact)
        .collect();
    assert_eq!(artifact_refs.len(), 2);
    assert!(snapshot.selected_resource_refs.is_empty());
    for reference in artifact_refs {
        assert!(requests[3].contains(&reference.version));
    }
    let mappings = context["durable_output_resources"].as_object().unwrap();
    assert!(!mappings.is_empty());
    for reference in mappings.values() {
        store
            .trace_resolve(
                &m.campaign_id,
                &serde_json::from_value(reference.clone()).unwrap(),
            )
            .unwrap();
    }
    service
        .continue_work(&m.campaign_id, &request, true)
        .unwrap();
    assert!(service
        .continue_work(&m.campaign_id, &request, false)
        .unwrap_err()
        .contains("authorization required"));
    let mut conflicting = request.clone();
    conflicting.instruction = "different payload".into();
    assert!(service
        .continue_work(&m.campaign_id, &conflicting, true)
        .unwrap_err()
        .contains("payload/scope conflict"));
    assert_eq!(
        store
            .campaign_command_gate(&m.campaign_id, &work)
            .unwrap()
            .unwrap()
            .history
            .len(),
        1
    );
    let listener = tokio::time::timeout(Duration::from_secs(5), http)
        .await
        .unwrap()
        .unwrap();
    let mut mismatch = request.clone();
    mismatch.command_id = "known-version-mismatch".into();
    mismatch.ghost_version = "not-the-launched-version".into();
    let tx = store.database.begin_write().unwrap();
    mismatch.checkpoint = continued.stopping_snapshot.clone().unwrap();
    mismatch.expected_state_sha256 = super::super::reconciliation::state_hash(&tx).unwrap();
    drop(tx);
    let run_store = store.clone();
    let root = dir.path().to_owned();
    let mismatch_model = model();
    service
        .launch_request(
            &m,
            base.clone(),
            true,
            Some(&mismatch),
            move |launch, signal| {
                execute_prepared(run_store, root, launch, mismatch_model, signal, true)
            },
        )
        .unwrap();
    idle(&service, &m.campaign_id).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "failed continuation bootstrap must not contact the provider"
    );
    let diagnostics = service.reconciliation_inspection(&m.campaign_id).unwrap();
    assert!(
        diagnostics
            .iter()
            .any(|line| line.contains("continuation configuration mismatch")),
        "{diagnostics:?}"
    );
    assert_eq!(
        store
            .campaign_ledger(&m.campaign_id)
            .unwrap()
            .unwrap()
            .allocation_available(&original.policy.funding.dispatch_id)
            .unwrap(),
        after
    );
    service.shutdown();
    drop(service);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let recovered = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    assert!(recovered.active.lock().unwrap().is_empty());
    assert_eq!(
        store.host_capacity.resident.available(),
        store.host_capacity.limits.max_resident_workers - 1
    );
    assert_eq!(
        store.host_capacity.execution.available(),
        store.host_capacity.limits.max_execution_jobs - 1
    );
    recovered
        .continue_work(&m.campaign_id, &mismatch, true)
        .unwrap();
    assert!(recovered.active.lock().unwrap().is_empty());
    let unknown = store
        .campaign_execution(&m.campaign_id, &work)
        .unwrap()
        .unwrap();
    let tx = store.database.begin_write().unwrap();
    let receipt = tachyon_api::campaign::ReconciliationReceipt {
        schema_version: 1,
        command_id: "exact-cleanup".into(),
        campaign_id: m.campaign_id.clone(),
        expected_state_sha256: super::super::reconciliation::state_hash(&tx).unwrap(),
        evidence_reference: "localhost-fixture-cleanup".into(),
        records: vec![tachyon_api::campaign::RecoveryRecord::Cleanup {
            work_id: work.clone(),
            attempt_id: unknown.policy.model.identity.attempt_id,
            generation: 1,
            reservation_id: unknown.policy.funding.dispatch_id,
            confirmation:
                tachyon_api::campaign::CleanupConfirmation::OperatorAttestsAllProcessesTerminated,
            outcome: tachyon_api::campaign::CleanupOutcome::Unverified,
        }],
    };
    drop(tx);
    recovered
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    recovered
        .reconcile(&m.campaign_id, &receipt, true, true)
        .unwrap();
    assert_eq!(
        store.host_capacity.resident.available(),
        store.host_capacity.limits.max_resident_workers
    );
    assert_eq!(
        store.host_capacity.execution.available(),
        store.host_capacity.limits.max_execution_jobs
    );
    assert_eq!(
        store
            .campaign_ledger(&m.campaign_id)
            .unwrap()
            .unwrap()
            .allocation_available(&original.policy.funding.dispatch_id)
            .unwrap(),
        after
    );
}
