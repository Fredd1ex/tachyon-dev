use super::*;
use crate::parallel_acceptance::{provider::LocalProvider, request, Foreground};
use crate::runtime_store::coordination::WorkAddress;
use serde_json::{json, Value};
use tachyon_api::context::{Finding, Query, Request as ResourceRequest};
use tachyon_api::{InteractionCommand, InteractionEvent};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires freshly built FOREGROUND_TEST_BIN and GHOST_TEST_BIN; loopback only"]
async fn parallel_acceptance_campaign_attention_oversight() {
    let (dir, store, mut m) = fixture();
    let mut store = Arc::try_unwrap(store).ok().unwrap();
    store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(
        tachyon_util::config::ResourceLimits {
            max_model_calls: 3,
            ..Default::default()
        },
    )
    .unwrap();
    let store = Arc::new(store);
    let workspace = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    m.workspace = workspace.path().canonicalize().unwrap();
    m.home = home.path().canonicalize().unwrap();
    m.executable = PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"))
        .canonicalize()
        .unwrap();
    m.objective = "Investigate benchmark; retain the checked candidate".into();
    m.model = "scripted-test-model".into();
    m.work_tokens = 10_000;
    m.work_cost_micro_usd = 10_000;
    m.max_request_bytes = 256_000;
    m.output_tokens = 128;
    m.max_active_inferences = 8;
    let branch_workspace = tempfile::tempdir().unwrap();
    let branch_home = tempfile::tempdir().unwrap();
    m.children = Some(tachyon_api::campaign::CampaignChildren {
        max_depth: 1,
        dynamic: None,
        total_work: 6,
        max_running: 2,
        max_resident: 2,
        controls: vec![
            tachyon_api::agents::Control::Spawn,
            tachyon_api::agents::Control::Group,
            tachyon_api::agents::Control::Wait,
        ],
        history: true,
        completion: tachyon_api::campaign::ChildCompletion::CancelOutstanding,
        templates: vec![tachyon_api::campaign::CampaignTemplate {
            template_id: "speculative".into(),
            group_id: Some("scripted-branches".into()),
            max_running: 1,
            specs: vec![tachyon_api::campaign::CampaignChild {
                evaluator: None,
                work_id: Some("speculative-branch".into()),
                objective: "speculative branch".into(),
                workspace: branch_workspace.path().canonicalize().unwrap(),
                home: branch_home.path().canonicalize().unwrap(),
                work_tokens: 1000,
                work_cost_micro_usd: 1000,
                verification_tokens: 2,
                verification_cost_micro_usd: 2,
            }],
        }],
    });
    let unrelated_workspace = tempfile::tempdir().unwrap();
    let unrelated_home = tempfile::tempdir().unwrap();
    let mut unrelated = m.children.as_ref().unwrap().templates[0].clone();
    unrelated.template_id = "unrelated".into();
    unrelated.specs[0].work_id = Some("unrelated-branch".into());
    unrelated.specs[0].objective = "unrelated queued branch".into();
    unrelated.specs[0].workspace = unrelated_workspace.path().canonicalize().unwrap();
    unrelated.specs[0].home = unrelated_home.path().canonicalize().unwrap();
    m.children.as_mut().unwrap().templates[0]
        .specs
        .push(unrelated.specs.remove(0));
    m.oversight = Some(tachyon_api::campaign::CampaignOversight {
        tokens: 1000,
        cost_micro_usd: 1000,
        max_assessments: 2,
        timeout_ms: 10_000,
        conversation_id: Some(tachyon_api::FOREGROUND_ID.into()),
    });
    m.evaluator.argv = vec![
        "/usr/bin/grep".into(),
        "-qx".into(),
        "42".into(),
        "candidate".into(),
    ];
    m.evaluator.timeout_ms = 1000;
    m.evaluator.max_total_command_ms = 1000;
    m.validate(now()).unwrap();
    let mut provider = LocalProvider::start().await;
    let mut foreground = Foreground::start(dir.path(), &provider.endpoint, store.clone()).await;
    foreground.user(
        "investigate",
        "Investigate benchmark; this campaign is already host-authorized.",
    );
    let main_reply = crate::parallel_acceptance::investigation(&mut provider).await;
    let service = Arc::new(CampaignService::new(store.clone(), dir.path().into()).unwrap());
    foreground.registry.lock().unwrap().campaigns = Some(service.clone());
    let run_store = store.clone();
    let root = dir.path().to_owned();
    let model = Model::new(ModelConfig {
        base_url: provider.endpoint.clone(),
        api_key: "local-test-only".into(),
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
    // Explicit trusted host authorization, not permission inferred from user prose.
    service
        .launch(
            &m,
            provider.endpoint.clone(),
            false,
            move |launch, cancel| async move {
                let result = execute(run_store, root, launch, model, cancel).await;
                assert!(result.is_ok(), "acceptance campaign execution: {result:?}");
                result
            },
        )
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        let work = format!("{}-root", m.campaign_id);
        let actor = WorkAddress { campaign_id: m.campaign_id.clone(), work_id: work.clone() };
        let target = WorkAddress { campaign_id: m.campaign_id.clone(), work_id: "speculative-branch".into() };
        let mut root_request = None;
        let mut assessment = None;
        for _ in 0..2 {
            let next = request(&mut provider).await;
            if next.body.get("tools").is_none() { assessment = Some(next); }
            else { root_request = Some(next); }
        }
        assert!(store.campaign_work_status(&m.campaign_id, &work).unwrap().active);
        assessment.unwrap().respond(&[r#"{"summary":"Investigation running; no conclusion yet","findings":[],"refs":[],"blockers":[],"attention":"none"}"#]).await;
        loop {
            let ApiResponse::CampaignAssessments { records } = store.campaign_assessments(&m.campaign_id).unwrap() else { panic!() };
            if records.len() == 1 && records[0].status != "claimed_unknown" { break; }
            tokio::task::yield_now().await;
        }
        root_request.unwrap().respond_tool("agents", json!({"action":"group", "template_id":"speculative", "command_id":"spawn-branches"})).await;
        let mut branch_request = None;
        let mut root_request = None;
        for _ in 0..2 {
            let next = request(&mut provider).await;
            let child = next.body["messages"].as_array().unwrap().iter().any(|m| m["role"] == "user" && m["content"].as_str().is_some_and(|s| s.contains("speculative branch")));
            if child { branch_request = Some(next); } else { root_request = Some(next); }
        }
        crate::parallel_acceptance::simple_paths(&mut foreground, &mut provider, &store).await;
        assert_eq!(service.active.lock().unwrap().len(), 1);
        assert!(!store.campaign_work_status(&m.campaign_id, "unrelated-branch").unwrap().active);
        root_request.unwrap().respond_tool("write", json!({"path":"result", "content":"42\n"})).await;
        let next_worker = request(&mut provider).await;
        let last = next_worker.body["messages"].as_array().unwrap().iter().rev().find(|m| m["role"] == "tool").unwrap();
        let output: Value = serde_json::from_str(last["content"].as_str().unwrap()).unwrap();
        assert_ne!(output["is_error"], true, "{output}");
        let traces = store.host_research_context(&m.campaign_id, &ResourceRequest::Traces {
            query: Query { literal: None, after: None, limit: 16, since_ms: None, version: None },
        }, None).unwrap();
        assert!(!traces.resources.is_empty(), "the real worker must retain its tool trace");
        store.host_record_finding(&m.campaign_id, Finding {
            id: "benchmark-candidate".into(), work_id: work.clone(), author: "acceptance-host".into(),
            claim: "The scripted candidate is available for independent command verification".into(),
            conditions: "Local scripted benchmark only; not evidence of performance improvement".into(),
            evidence: vec![traces.resources[0].reference.clone()], parents: vec![],
        }, None).unwrap();
        let assessment = request(&mut provider).await;
        assert!(assessment.body.get("tools").is_none());
        let input: Value = serde_json::from_str(assessment.body["messages"][1]["content"].as_str().unwrap()).unwrap();
        assert!(input["triggers"].as_array().unwrap().contains(&json!("new_finding")));
        let reference = input["evidence"].as_array().unwrap().iter()
            .find(|e| e["reference"].as_str().unwrap().starts_with("finding:benchmark-candidate:"))
            .unwrap()["reference"].clone();
        assessment.respond(&[&json!({"summary":"A candidate is ready; independent verification is still required", "findings":["Candidate recorded"], "refs":[reference], "blockers":[], "attention":"operator"}).to_string()]).await;
        loop {
            let ApiResponse::CampaignAssessments { records } = store.campaign_assessments(&m.campaign_id).unwrap() else { panic!() };
            if records.len() == 2 && records[1].status != "claimed_unknown" {
                assert_eq!(records[1].status, "published");
                break;
            }
            tokio::task::yield_now().await;
        }
        // The actual daemon outbox delivers through the real foreground stdin.
        // Drain a possible launch advisory before the finding advisory.
        loop {
            crate::messaging::deliver_attention(&foreground.registry, &store);
            let notice = foreground.until(|e| matches!(e.event, InteractionEvent::UserVisibleNotificationPublished { .. })).await;
            assert_eq!(notice.metadata.turn_id, None);
            if matches!(&notice.event, InteractionEvent::UserVisibleNotificationPublished { text } if text.contains("A candidate is ready")) { break; }
        }
        assert_eq!(foreground.checkpoint(1).await["next_commit"], 1);

        foreground.completed_delegation("scripted-call-1", "Summarize the retained benchmark finding", "A candidate is retained; command verification is still pending.");
        main_reply.respond_tool("spawn_agent", json!({"task":"Summarize the retained benchmark finding"})).await;
        let main_reply = request(&mut provider).await;
        assert!(main_reply.body.get("tools").is_none(), "must be dedicated post-delegation synthesis");
        assert_eq!(main_reply.body["messages"][0]["content"], format!("{}\n\n{}",
            tachyon_orchestrator::conversation::prompt::SYNTHESIS_PROMPT,
            tachyon_orchestrator::conversation::prompt::SYNTHESIS_EVIDENCE_GUIDANCE));
        assert!(main_reply.body["messages"][1]["content"].as_str().unwrap().contains("command verification is still pending"));

        next_worker.respond_tool("work", json!({"action":"ask", "request_id":"keep-candidate", "question":"Keep this candidate for verification?", "timeout_ms":20000})).await;
        let question = loop {
            assert!(!service.active.lock().unwrap()[&m.campaign_id].task.is_finished(), "worker ended before question: {:?}", store.campaign_execution(&m.campaign_id, &work));
            let questions = store.work_attention(&m.campaign_id, &work).unwrap();
            if store.campaign_work_status(&m.campaign_id, &work).unwrap().wait.is_some() {
                if let Some(question) = questions.into_iter().next() { break question; }
            }
            tokio::task::yield_now().await;
        };
        crate::messaging::deliver_attention(&foreground.registry, &store);
        foreground.until(|e| matches!(&e.event, InteractionEvent::UserVisibleNotificationPublished { text } if text.contains("attention"))).await;
        let scope = tachyon_api::todo::TodoScope::Campaign { campaign_id: m.campaign_id.clone() };
        let snapshot = store.attention_snapshot(&scope, None, 10).unwrap();
        assert_eq!(snapshot.records.len(), 1);
        let attention = &snapshot.records[0];
        assert!(attention.delivered_at_ms.is_some());
        let published_count = store.pending_history().unwrap().len();
        for phase in [tachyon_api::attention::AttentionAcknowledgement::Displayed, tachyon_api::attention::AttentionAcknowledgement::Acknowledged] {
            for _ in 0..2 {
                let mut client = tachyon_api::transport::Connection::connect(&foreground.socket).unwrap();
                assert!(matches!(client.exchange(&ApiRequest::AttentionAcknowledge { scope: scope.clone(), id: attention.id.clone(), phase }).unwrap(), ApiResponse::AttentionAcknowledged { .. }));
            }
        }
        crate::messaging::deliver_attention(&foreground.registry, &store);
        assert_eq!(store.pending_history().unwrap().len(), published_count);
        assert!(store.claim_attention_frame(now() + 10000).unwrap().is_none());
        let replay_subscriber = foreground.registry.lock().unwrap().subscribe(tachyon_api::FOREGROUND_ID).unwrap();
        for cached in replay_subscriber.try_iter() {
            let event: tachyon_api::EventEnvelope = serde_json::from_str(&cached.data).unwrap();
            assert!(matches!(event.kind, tachyon_api::AgentEvent::Usage { .. }));
        }
        for event in foreground.events.iter().filter(|e| matches!(e.event, InteractionEvent::UserVisibleNotificationPublished { .. })) {
            crate::push_event(&foreground.registry, tachyon_api::FOREGROUND_ID, tachyon_api::EventStream::Stdout, &serde_json::to_string(event).unwrap());
        }
        let replay: tachyon_api::InteractionEventEnvelope = serde_json::from_str(&replay_subscriber.try_recv().unwrap().data).unwrap();
        assert_eq!(replay.metadata.attention.as_ref().unwrap().ids, [attention.id.clone()]);
        assert!(foreground.events.iter().any(|original| original.metadata.message_id == replay.metadata.message_id && original.event == replay.event));
        assert!(replay_subscriber.try_recv().is_err(), "only the same at-least-once attention frame may replay");
        assert_eq!(store.pending_history().unwrap().len(), published_count);
        let mut client = tachyon_api::transport::Connection::connect(&foreground.socket).unwrap();
        let answer = client.exchange(&ApiRequest::CampaignAttentionAnswer {
            id: m.campaign_id.clone(), work_id: work.clone(), request_id: question.request_id,
            generation: question.generation, instruction_revision: question.instruction_revision, answer: "yes".into(),
        }).unwrap();
        assert!(!matches!(answer, ApiResponse::Error { .. }), "{answer:?}");
        drop(client);
        let worker = request(&mut provider).await;
        let control = store.host_agent_control(actor).unwrap();
        // A separate user turn goes through the actual foreground tool loop and
        // authenticated daemon endpoint, not the host-only test facade.
        crate::parallel_acceptance::independent(&foreground, &mut provider, "steer", "Stop the speculative branch; retain this candidate and complete verification. Cancel the unrelated queued branch.").await
            .respond_tool("campaign", json!({"operation":"list"})).await;
        let query = request(&mut provider).await;
        let output = |r: &crate::parallel_acceptance::provider::Request| -> Value {
            serde_json::from_str(r.body["messages"].as_array().unwrap().last().unwrap()["content"].as_str().unwrap()).unwrap()
        };
        let linked = output(&query);
        assert_eq!(linked["campaigns"][0]["campaign_id"], m.campaign_id);
        query.respond_tool("campaign", json!({"operation":"status","campaign_id":linked["campaigns"][0]["campaign_id"]})).await;
        let steer = request(&mut provider).await;
        let state = output(&steer);
        assert!(state.get("plan").is_none());
        let branch_id = state["works"].as_array().unwrap().iter().find(|w| w["work_id"] == target.work_id).unwrap();
        let unrelated_id = state["works"].as_array().unwrap().iter().find(|w| w["work_id"] == "unrelated-branch").unwrap();
        steer.respond_tool("campaign", json!({"operation":"steer","campaign_id":m.campaign_id,"work_id":branch_id["work_id"],"command_id":"user-stop-branch","expected_revision":branch_id["accepted_revision"],"instructions":"Stop the speculative branch; retain this candidate and complete verification."})).await;
        let receipt = request(&mut provider).await;
        assert_eq!(output(&receipt)["status"], "accepted");
        assert_eq!(output(&receipt)["accepted_revision"], 2);
        receipt.respond_tool("campaign", json!({"operation":"status","campaign_id":m.campaign_id,"work_id":target.work_id})).await;
        let pending = request(&mut provider).await;
        assert_eq!(output(&pending)["works"][0]["applied_revision"], 1);
        let accepted = control.status(&target).unwrap();
        assert_eq!((accepted.accepted_revision, accepted.acknowledged_revision), (2, 1));
        branch_request.unwrap().respond_tool("work", json!({"action":"status"})).await;
        let branch = request(&mut provider).await;
        let applied = control.status(&target).unwrap();
        assert_eq!((applied.accepted_revision, applied.acknowledged_revision), (2, 2));
        assert!(branch.body["messages"].as_array().unwrap().iter().any(|m| m["role"] == "system" && m["content"].as_str().is_some_and(|s| s.contains("Stop the speculative branch"))));
        pending.respond_tool("campaign", json!({"operation":"status","campaign_id":m.campaign_id,"work_id":target.work_id})).await;
        let cancel = request(&mut provider).await;
        assert_eq!(output(&cancel)["works"][0]["applied_revision"], 2);
        cancel.respond_tool("campaign", json!({"operation":"cancel","campaign_id":m.campaign_id,"work_id":unrelated_id["work_id"],"command_id":"user-cancel-unrelated","generation":unrelated_id["generation"]})).await;
        let receipt = request(&mut provider).await;
        assert_eq!(output(&receipt)["status"], "accepted");
        receipt.respond_tool("campaign", json!({"operation":"status","campaign_id":m.campaign_id,"work_id":unrelated_id["work_id"]})).await;
        let confirmation = request(&mut provider).await;
        assert_eq!(output(&confirmation)["works"][0]["work_id"], "unrelated-branch");
        assert_eq!(output(&confirmation)["works"][0]["cancellation_done"], true);
        assert!(!store.campaign_work_status(&m.campaign_id, &work).unwrap().cancellation_requested);
        assert!(!store.campaign_work_status(&m.campaign_id, &target.work_id).unwrap().cancellation_requested);
        confirmation.respond(&["Steering applied; the unrelated queued branch is cancelled."]).await;
        foreground.finished(8, "Steering applied; the unrelated queued branch is cancelled.").await;
        branch.respond_tool("work", json!({"action":"complete", "summary":"Speculative branch stopped without a candidate", "candidate_refs":[], "unresolved_questions":[]})).await;
        worker.respond_tool("artifact", json!({"path":"result", "kind":"file", "description":"benchmark candidate"})).await;
        let worker = request(&mut provider).await;
        worker.respond_tool("work", json!({"action":"complete", "summary":"Candidate 42 retained", "candidate_refs":[], "unresolved_questions":[]})).await;
        loop {
            if service.active.lock().unwrap()[&m.campaign_id].task.is_finished() { break; }
            tokio::task::yield_now().await;
        }
        assert_eq!(status(&store, &m), CampaignStatus::Accepted);
        let record = store.campaign_execution(&m.campaign_id, &work).unwrap().unwrap();
        assert_eq!(record.phase, ExecutionPhase::Reviewed(Evaluation::Accepted));
        assert!(record.settled);
        let gate = store.campaign_command_gate(&m.campaign_id, &work).unwrap().unwrap();
        let evidence = gate.evidence.unwrap();
        assert_eq!(evidence.outcome, tachyond::verification::CommandOutcome::Pass);
        assert_eq!(evidence.exit_code, Some(0));
        assert_eq!(evidence.candidate_sha256, gate.snapshot.unwrap().sha256);
        assert_eq!(evidence.config_hash, gate.config.config_hash().unwrap());
        let candidate = record.candidate.unwrap();
        assert_eq!(candidate.instruction_revision, Some(1));
        assert!(!candidate.candidate_refs.as_ref().unwrap().is_empty());
        assert!(candidate.candidate_refs.as_ref().unwrap().contains(&evidence.artifact_id));
        foreground.send("verified-evidence", InteractionCommand::PublishBackgroundUpdate {
            event: tachyon_api::EventEnvelope {
                event_id: 1, session_id: work.clone(), conversation_id: Some(tachyon_api::FOREGROUND_ID.into()),
                turn_id: Some("1".into()), task_id: Some(work.clone()), parent_task_id: None, tool_call_id: None,
                actor: tachyon_api::Actor::Worker { id: work }, sequence: 1, occurred_at_ms: 1,
                kind: tachyon_api::AgentEvent::WorkResult { result: candidate.clone() },
            },
        });
        // Stdin intake and the HTTP final response are independent. Admit the
        // evidence first so a turn-9 checkpoint cannot predate its delivery.
        let admitted = foreground.checkpoint_where(1, |checkpoint| {
            checkpoint["evidence"].as_array().is_some_and(|records| records.iter().any(|record| {
                record["result"] == json!(candidate)
            }))
        }).await;
        main_reply.respond(&["Command verification passed; candidate 42 and its evidence are retained."]).await;
        foreground.finished(1, "Command verification passed; candidate 42 and its evidence are retained.").await;
        let checkpoint = foreground.checkpoint(9).await;
        assert!(!checkpoint["evidence"].as_array().unwrap().is_empty());
        assert_eq!(checkpoint["evidence"], admitted["evidence"]);
        assert!(checkpoint["assessments"].as_array().unwrap().iter().any(|a| a["assessment"]["summary"] == "A candidate is ready; independent verification is still required"));
        let command = store.observe_context_usage(
            tachyon_api::FOREGROUND_ID, 0, 0, 1, 3, 3, now(),
        ).unwrap().expect("post-final compaction");
        crate::write_task_input(
            crate::task_input(&foreground.registry, tachyon_api::FOREGROUND_ID).unwrap(),
            tachyon_api::FOREGROUND_ID,
            &serde_json::to_string(&command).unwrap(),
        ).unwrap();
        let compacted = foreground.checkpoint_where(9, |value| value["context_epoch"] == command.epoch).await;
        assert_ne!(compacted["messages"], checkpoint["messages"]);
        assert_eq!(compacted["evidence"], checkpoint["evidence"]);
        assert_eq!(compacted["assessments"], checkpoint["assessments"]);
        let ledger = store.campaign_ledger(&m.campaign_id).unwrap().unwrap();
        let own = crate::runtime_store::model_accounting::services::service_id(&m.campaign_id, "oversight");
        let charged: Vec<_> = ledger.reservations.values().filter(|r| r.allocation.as_deref() == Some(&own)).collect();
        assert_eq!(charged.len(), 2);
        assert!(charged.iter().all(|r| r.usage == crate::runtime_store::campaign_ledger::Usage::Final(Units { tokens: 16, cost_micro_usd: 16 })));
        assert!(provider.requests.try_recv().is_err());
        foreground.capture("campaign-attention", Some(&m.campaign_id));
    }).await;
    service.shutdown();
    drop(foreground);
    provider.shutdown().await;
    result.expect("combined campaign acceptance stalled");
}
