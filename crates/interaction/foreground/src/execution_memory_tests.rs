use super::*;
use crate::model::test_provider::LocalProvider;
use std::sync::atomic::Ordering;

// Scripted completions prove wiring and policy delivery, not model obedience.
#[tokio::test]
async fn preference_recall_policy_and_results_reach_real_outbound_requests() {
    for (question, recalled, answer, dependency) in [
        (
            "Recommend a dinner.",
            Some("Prefer vegetarian meals and bullet lists."),
            "- Try lentil soup.",
            false,
        ),
        (
            "Recommend a dinner, but use one sentence, not bullets.",
            Some("Prefer vegetarian meals and bullet lists."),
            "Try lentil soup.",
            false,
        ),
        (
            "Recommend a dinner from these options.",
            Some("Prefer vegetarian meals and bullet lists."),
            "- Try lentil soup.",
            true,
        ),
        (
            "Recommend a dinner.",
            Some(""),
            "Lentil soup is one option.",
            false,
        ),
        (
            "Recommend a dinner.",
            None,
            "Lentil soup is one option.",
            false,
        ),
        ("Hello!", Some("unused"), "Hello!", false),
        ("What is 2 + 2?", Some("unused"), "4.", false),
    ] {
        let mut provider = LocalProvider::start().await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let service = Arc::new(
            move |conversation: &str, turn: u64, query: &str, history: bool| {
                assert_eq!(conversation, "preference-test");
                assert_eq!(turn, 1);
                assert_eq!(query, "meal and answer format preferences");
                assert!(!history);
                observed.fetch_add(1, Ordering::SeqCst);
                match recalled {
                    None => Err("offline memory unavailable".into()),
                    Some("") => Ok((vec![], false)),
                    Some(text) => Ok((
                        vec![tachyon_api::types::MemoryRecallItem {
                            kind: tachyon_api::types::MemoryRecallKind::Preference,
                            memory_id: Some("preference-1".into()),
                            descriptor: None,
                            text: text.into(),
                            occurred_at_ms: 1,
                        }],
                        false,
                    )),
                }
            },
        );
        let rendered = AgentRole::Conversation
            .primary(
                &tachyon_orchestrator::registry::builtin(),
                &Default::default(),
            )
            .unwrap();
        let mut conversation = vec![
            ChatMessage::new(Role::System, rendered.prompt),
            ChatMessage::new(Role::User, question),
        ];
        let metadata = InteractionMetadata::new("m", "root", "preference-test", 0);
        let model = provider.model.clone();
        let recall = question.starts_with("Recommend");
        crate::streaming::CAPTURED_EVENTS
            .scope(
                Default::default(),
                crate::delegation::MEMORY_SERVICE.scope(
                    service,
                    tokio::time::timeout(std::time::Duration::from_secs(10), async {
                        let future = loop_until_done(
                            &model,
                            &mut conversation,
                            None,
                            AgentRole::Conversation,
                            Some(1),
                            !dependency,
                            false,
                            dependency,
                            false,
                            None,
                            Some(&metadata),
                            false,
                            None,
                            &|_, _| {},
                        );
                        tokio::pin!(future);
                        let request = tokio::select! {
                            _ = &mut future => panic!("missing primary request"),
                            request = provider.requests.recv() => request.unwrap(),
                        };
                        let policy = request.body["messages"][0]["content"].as_str().unwrap();
                        for clause in [
                            "recall relevant preferences and constraints",
                            "including recommendations",
                            "explicit current instructions take precedence",
                            "recalled format preferences",
                            "Never invent preferences",
                            "Reuse sufficient recall from this turn",
                            "Do not consult memory for greetings or ordinary context-free requests",
                        ] {
                            assert!(policy.contains(clause), "missing {clause}");
                        }
                        let memory = request.body["tools"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|tool| tool["function"]["name"] == "memory")
                            .unwrap();
                        if dependency {
                            assert_eq!(request.body["tools"].as_array().unwrap().len(), 1);
                        }
                        assert!(
                            memory["function"]["parameters"]["properties"]["action"]["enum"]
                                .as_array()
                                .unwrap()
                                .contains(&serde_json::json!("recall"))
                        );
                        assert!(request.body.get("tool_choice").is_none_or(|v| v == "auto"));
                        let mut request = if recall {
                            request.respond_tool("memory", serde_json::json!({
                            "action":"recall", "query":"meal and answer format preferences",
                            "include_history":false,
                        })).await;
                            let next = tokio::select! {
                                _ = &mut future => panic!("missing post-recall request"),
                                request = provider.requests.recv() => request.unwrap(),
                            };
                            let messages = next.body["messages"].as_array().unwrap();
                            let result = messages.iter().find(|m| m["role"] == "tool").unwrap();
                            let output = result["content"].as_str().unwrap();
                            match recalled {
                                Some("") => assert!(output.contains("\"items\":[]")),
                                Some(text) => assert!(output.contains(text)),
                                None => assert!(output.contains("Memory recall unavailable")),
                            }
                            assert!(messages
                                .iter()
                                .any(|m| m["role"] == "user" && m["content"] == question));
                            let memory = next.body["tools"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .find(|t| t["function"]["name"] == "memory")
                                .unwrap();
                            assert!(!memory["function"]["parameters"]["properties"]["action"]
                                ["enum"]
                                .as_array()
                                .unwrap()
                                .contains(&serde_json::json!("recall")));
                            next
                        } else {
                            request
                        };
                        if dependency {
                            // Even a provider ignoring the narrowed action schema cannot recall twice.
                            request.respond_tool("memory", serde_json::json!({
                            "action":"recall", "query":"meal and answer format preferences",
                            "include_history":false,
                        })).await;
                            request = tokio::select! {
                                _ = &mut future => panic!("missing duplicate-recall rejection"),
                                request = provider.requests.recv() => request.unwrap(),
                            };
                            assert!(request
                                .body
                                .to_string()
                                .contains("Memory was already recalled for this turn"));
                        }
                        request.respond(&[answer]).await;
                        assert!(
                            matches!(future.await.unwrap(), Turn::Done(text, _) if text == answer)
                        );
                        assert_eq!(calls.load(Ordering::SeqCst), usize::from(recall));
                        assert!(provider.requests.try_recv().is_err());
                    }),
                ),
            )
            .await
            .unwrap();
        provider.shutdown().await;
    }
}

#[tokio::test]
async fn fresh_work_can_recall_first_and_dependency_rejects_unadvertised_work() {
    for dependency in [false, true] {
        let mut provider = LocalProvider::start().await;
        let model = provider.model.clone();
        let service = Arc::new(|_: &str, _: u64, _: &str, _: bool| Ok((vec![], false)));
        let metadata = InteractionMetadata::new("m", "root", "c", 0);
        let mut conversation = vec![ChatMessage::new(Role::User, "Recommend a current option.")];
        crate::delegation::MEMORY_SERVICE.scope(service,
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                let future = loop_until_done(&model, &mut conversation, None,
                    AgentRole::Conversation, Some(1), !dependency, !dependency, dependency,
                    false, None, Some(&metadata), false, None, &|_, _| {});
                tokio::pin!(future);
                let mut request = tokio::select! {
                    _ = &mut future => panic!("missing request"),
                    request = provider.requests.recv() => request.unwrap(),
                };
                if dependency {
                    request.start_stream().await;
                    request.send_delta(serde_json::json!({"tool_calls":[{
                        "index":0,"id":"unauthorized","type":"function",
                        "function":{"name":"spawn_agent","arguments":"{}"}
                    }]})).await;
                    request.finish_completion("tool_calls").await;
                    assert!(matches!(future.await, Err(error) if error.contains("allow only memory")));
                } else {
                    request.respond_tool("memory", serde_json::json!({
                        "action":"recall","query":"relevant preferences","include_history":false
                    })).await;
                    let next = tokio::select! {
                        _ = &mut future => panic!("recall must precede forced delegation"),
                        request = provider.requests.recv() => request.unwrap(),
                    };
                    let messages = next.body["messages"].as_array().unwrap();
                    assert_eq!(messages.iter().filter(|m| m["role"] == "tool").count(), 1);
                    assert!(!messages.iter().any(|m| m.to_string().contains("spawn_agent")));
                    next.respond_error().await;
                    assert!(future.await.is_err());
                }
                assert!(provider.requests.try_recv().is_err());
            })
        ).await.unwrap();
        provider.shutdown().await;
    }
}
