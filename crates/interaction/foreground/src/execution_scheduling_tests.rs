use super::*;
use crate::model::test_provider::LocalProvider;
use crate::turns::tests::completed_evidence;

#[tokio::test]
async fn pending_parent_subset_reassesses_once_after_insufficient_evidence() {
    let mut provider = LocalProvider::start().await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let evidence = |objective: &str, result: &str| {
            let mut record = completed_evidence(1, result);
            let crate::turns::EvidenceRecord::Correlated(envelope) = &mut record else {
                unreachable!()
            };
            let AgentEvent::WorkerCompleted {
                objective: stored, ..
            } = &mut envelope.kind
            else {
                unreachable!()
            };
            *stored = objective.into();
            record
        };
        let state = Arc::new(Mutex::new(ConversationState {
            assessments: vec![],
            messages: vec![],
            evidence: vec![
                evidence("New York weather", "NEW YORK ONLY"),
                evidence("London weather", "LONDON PARTIAL"),
            ],
            pending: BTreeMap::new(),
            next_commit: 1,
            context_epoch: 0,
        }));
        let changed = Arc::new(tokio::sync::Notify::new());
        let (tx, _rx) = std::sync::mpsc::channel();
        let future = process_turn(
            (
                2,
                "Do I need a coat in London?".into(),
                InteractionMetadata::new("m", "c", tachyon_api::FOREGROUND_ID, 0),
                true,
                InteractionDecision::WaitForActiveTurn,
                None,
                TokenUsage::default(),
                std::time::Instant::now(),
                Some(provider.model.clone()),
                None,
                state.clone(),
                Arc::new(Mutex::new(BTreeMap::from([(
                    1,
                    "London and New York weather".into(),
                )]))),
                changed.clone(),
                tx,
                AgentRole::Conversation,
                None,
            ),
            |_, event| {
                assert!(!matches!(
                    event,
                    InteractionEvent::ConversationIntentProduced { .. }
                ));
            },
        );
        tokio::pin!(future);
        let request = tokio::select! {
            _ = &mut future => panic!("completed without evidence assessment"),
            request = provider.requests.recv() => request.unwrap(),
        };
        assert!(request.body.to_string().contains("LONDON PARTIAL"));
        assert!(!request.body.to_string().contains("NEW YORK ONLY"));
        request
            .respond_tool(
                "submit_answerability",
                serde_json::json!({"outcome":"NeedsNewWork"}),
            )
            .await;
        assert!(futures_util::poll!(&mut future).is_pending());
        changed.notify_waiters();
        assert!(futures_util::poll!(&mut future).is_pending());
        assert!(provider.requests.try_recv().is_err());
        state
            .lock()
            .unwrap()
            .evidence
            .push(evidence("London temperature", "LONDON 8 C"));
        changed.notify_waiters();
        let retry = tokio::select! {
            _ = &mut future => panic!("completed without reassessment"),
            request = provider.requests.recv() => request.unwrap(),
        };
        assert!(retry.body.to_string().contains("LONDON 8 C"));
        assert!(!retry.body.to_string().contains("NEW YORK ONLY"));
        retry
            .respond_tool(
                "submit_answerability",
                serde_json::json!({"outcome":"AnswerFromContext"}),
            )
            .await;
        let answer = tokio::select! {
            _ = &mut future => panic!("completed without answer"),
            request = provider.requests.recv() => request.unwrap(),
        };
        let tools = answer.body["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "memory");
        answer.respond(&["Wear a coat in London."]).await;
        future.await;
        assert_eq!(state.lock().unwrap().next_commit, 1, "parent still pending");
        assert!(state.lock().unwrap().pending.contains_key(&2));
        assert!(provider.requests.try_recv().is_err());
    })
    .await
    .unwrap();
    provider.shutdown().await;
}

// The held HTTP socket is the barrier: no sleeps or paid provider calls.
#[tokio::test]
async fn assessment_cannot_apply_after_evidence_revision_parent_terminal_or_cancellation() {
    for change in ["revision", "terminal", "cancel", "superseded"] {
        let mut provider = LocalProvider::start().await;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let state = Arc::new(Mutex::new(ConversationState {
                assessments: vec![],
                messages: vec![],
                evidence: vec![completed_evidence(1, "OLD FACT")],
                pending: BTreeMap::new(),
                next_commit: 1,
                context_epoch: 0,
            }));
            let active = Arc::new(Mutex::new(BTreeMap::from([
                (1, "inspect release".into()),
                (2, "Is release safe?".into()),
            ])));
            let changed = Arc::new(tokio::sync::Notify::new());
            let (tx, _rx) = std::sync::mpsc::channel();
            let events = Arc::new(Mutex::new(Vec::new()));
            let captured = events.clone();
            let future = process_turn(
                (
                    2,
                    "Is release safe?".into(),
                    InteractionMetadata::new("m", "c", tachyon_api::FOREGROUND_ID, 0),
                    true,
                    InteractionDecision::WaitForActiveTurn,
                    None,
                    TokenUsage::default(),
                    std::time::Instant::now(),
                    Some(provider.model.clone()),
                    None,
                    state.clone(),
                    active,
                    changed.clone(),
                    tx,
                    AgentRole::Conversation,
                    None,
                ),
                move |_, event| captured.lock().unwrap().push(event),
            );
            tokio::pin!(future);
            let assessment = tokio::select! {
                _ = &mut future => panic!("completed before assessment"),
                request = provider.requests.recv() => request.unwrap(),
            };
            assert!(assessment.body.to_string().contains("OLD FACT"));
            {
                let mut state = state.lock().unwrap();
                match change {
                    "revision" => state.evidence = vec![completed_evidence(1, "REVISED FACT")],
                    "terminal" => {
                        state.pending.insert(
                            1,
                            durable_turn_messages(
                                "inspect release".into(),
                                "PARENT TERMINAL".into(),
                            ),
                        );
                    }
                    "superseded" => {
                        state.pending.insert(
                            2,
                            durable_turn_messages(
                                "revised request".into(),
                                "REPLACEMENT TERMINAL".into(),
                            ),
                        );
                    }
                    _ => state.cancel_pending_turns(3),
                }
            }
            // Deliberately notify while inference, not the dependency wait, owns
            // the task. The post-inference state check must still observe it.
            changed.notify_waiters();
            assessment
                .respond_tool(
                    "submit_answerability",
                    serde_json::json!({"outcome":"AnswerFromContext"}),
                )
                .await;
            if matches!(change, "cancel" | "superseded") {
                future.await;
                assert!(events.lock().unwrap().is_empty());
                let state = state.lock().unwrap();
                if change == "cancel" {
                    assert!(state.pending.is_empty());
                    assert_eq!(state.next_commit, 3);
                } else {
                    assert_eq!(state.pending[&2][1].plain(), "REPLACEMENT TERMINAL");
                    assert_eq!(state.next_commit, 1);
                }
                assert!(provider.requests.try_recv().is_err());
                return;
            }
            let reassessment = tokio::select! {
                _ = &mut future => panic!("stale assessment completed turn"),
                request = provider.requests.recv() => request.unwrap(),
            };
            let body = reassessment.body.to_string();
            assert!(body.contains(if change == "revision" {
                "REVISED FACT"
            } else {
                "PARENT TERMINAL"
            }));
            reassessment
                .respond_tool(
                    "submit_answerability",
                    serde_json::json!({"outcome":"AnswerFromContext"}),
                )
                .await;
            let answer = tokio::select! {
                _ = &mut future => panic!("completed before answer"),
                request = provider.requests.recv() => request.unwrap(),
            };
            let tools = answer.body["tools"].as_array().unwrap();
            assert_eq!(tools.len(), 1);
            assert_eq!(tools[0]["function"]["name"], "memory");
            answer.respond(&["Current answer"]).await;
            future.await;
            assert_eq!(
                events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|e| matches!(e, InteractionEvent::ConversationFinished { .. }))
                    .count(),
                1
            );
            assert!(provider.requests.try_recv().is_err());
        })
        .await
        .expect("scheduling deadlock");
        provider.shutdown().await;
    }
}

#[tokio::test]
async fn independent_turn_skips_predecessor_evidence_and_late_answer_cannot_commit_after_cancel() {
    let mut provider = LocalProvider::start().await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let state = Arc::new(Mutex::new(ConversationState {
            assessments: vec![],
            messages: vec![],
            evidence: vec![completed_evidence(1, "UNRELATED FACT")],
            pending: BTreeMap::new(),
            next_commit: 1,
            context_epoch: 0,
        }));
        let (tx, _rx) = std::sync::mpsc::channel();
        let future = process_turn(
            (
                2,
                "Tell me a joke".into(),
                InteractionMetadata::new("m", "c", tachyon_api::FOREGROUND_ID, 0),
                true,
                InteractionDecision::AnswerNow,
                None,
                TokenUsage::default(),
                std::time::Instant::now(),
                Some(provider.model.clone()),
                None,
                state.clone(),
                Arc::new(Mutex::new(BTreeMap::new())),
                Arc::new(tokio::sync::Notify::new()),
                tx,
                AgentRole::Conversation,
                None,
            ),
            |_, _| panic!("cancelled output published"),
        );
        tokio::pin!(future);
        let request = tokio::select! {
            _ = &mut future => panic!("completed without response"),
            request = provider.requests.recv() => request.unwrap(),
        };
        assert!(!request.body.to_string().contains("UNRELATED FACT"));
        assert!(!request.body.to_string().contains("submit_answerability"));
        state.lock().unwrap().cancel_pending_turns(3);
        request.respond(&["Late joke"]).await;
        future.await;
        let state = state.lock().unwrap();
        assert!(state.messages.is_empty());
        assert!(state.pending.is_empty());
        assert_eq!(state.next_commit, 3);
    })
    .await
    .unwrap();
    provider.shutdown().await;
}
