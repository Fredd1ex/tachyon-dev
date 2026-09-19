use super::*;

fn interaction_into(
    threads: &mut Vec<Thread>,
    conversation: &str,
    turn: &str,
    event: InteractionEvent,
) {
    let mut metadata = tachyon_api::InteractionMetadata::new("event", "request", conversation, 1);
    metadata.turn_id = Some(turn.into());
    let wire = serde_json::to_string(&InteractionEventEnvelope { metadata, event }).unwrap();
    let mut envelope: InteractionEventEnvelope = serde_json::from_str(&wire).unwrap();
    envelope.metadata.turn_id = Some(session_archive::conversation_turn(
        &envelope.metadata.conversation_id,
        envelope.metadata.turn_id.as_deref().unwrap(),
    ));
    let root = find_or_create_thread(threads, FOREGROUND_ID, true, None);
    apply_interaction_event(&mut threads[root], envelope);
}

fn structured_into(threads: &mut Vec<Thread>, event: &EventEnvelope) {
    let wire = serde_json::to_string(event).unwrap();
    let mut event: EventEnvelope = serde_json::from_str(&wire).unwrap();
    qualify_event_turn(&mut event);
    record_correlated_metrics(threads, &event);
    let id = match &event.actor {
        Actor::Worker { id } => id.as_str(),
        _ => FOREGROUND_ID,
    };
    let idx = find_or_create_thread(threads, id, id == FOREGROUND_ID, None);
    apply_actor_event(
        &mut threads[idx],
        event.kind,
        &event.actor,
        event.turn_id.as_deref(),
    );
}

fn envelope(kind: AgentEvent) -> EventEnvelope {
    EventEnvelope {
        event_id: 1,
        session_id: "host".into(),
        conversation_id: Some("host".into()),
        turn_id: Some("2".into()),
        task_id: None,
        parent_task_id: None,
        tool_call_id: None,
        actor: Actor::Foreground,
        sequence: 1,
        occurred_at_ms: 1,
        kind,
    }
}

#[test]
fn typed_metrics_render_after_acceptance_or_trustworthy_early_delivery() {
    for early in [false, true] {
        let mut threads = Vec::new();
        interaction_into(
            &mut threads,
            "old",
            "2",
            InteractionEvent::UserTurnAccepted {
                text: "old prompt".into(),
            },
        );
        interaction_into(
            &mut threads,
            "old",
            "2",
            InteractionEvent::ConversationFinished {
                text: "old answer".into(),
            },
        );
        threads[0].history_len = threads[0].items.len();
        let accept = |threads: &mut Vec<Thread>| {
            interaction_into(
                threads,
                "host",
                "2",
                InteractionEvent::UserTurnAccepted {
                    text: "hello".into(),
                },
            )
        };
        if !early {
            accept(&mut threads);
        }
        for kind in [
            AgentEvent::Timing {
                turn: 2,
                stage: "first_visible".into(),
                elapsed_ms: 1250,
            },
            AgentEvent::Timing {
                turn: 2,
                stage: "completed".into(),
                elapsed_ms: 4500,
            },
            AgentEvent::Usage {
                turn: Some(2),
                prompt_tokens: 1000,
                completion_tokens: 200,
                total_tokens: 1200,
                context_tokens: 1000,
                context_window: None,
            },
        ] {
            structured_into(&mut threads, &envelope(kind));
        }
        if early {
            accept(&mut threads);
        }
        interaction_into(
            &mut threads,
            "host",
            "2",
            InteractionEvent::ConversationFinished {
                text: "Hello back".into(),
            },
        );
        let key = session_archive::conversation_turn("host", "2");
        let cells = build_turn_cells(&threads[0]);
        let cell = cells
            .iter()
            .find(|cell| threads[0].items[cell.prompt].turn.as_deref() == Some(&key))
            .unwrap();
        let direct = turn_cell_badges(&threads[0], cell);
        assert!(direct.contains("done 4.5s"), "{direct}");
        assert!(direct.contains("total 1.2k"), "{direct}");
        assert!(!threads[0].metrics.contains_key("2"));
        let old = &threads[0].metrics["conversation:old:2"];
        assert!(old.accepted_at_ms.is_some());
        assert!(old.completed_ms.is_none());
        assert!(old.self_usage.is_none());

        interaction_into(
            &mut threads,
            "host",
            "3",
            InteractionEvent::UserTurnAccepted {
                text: "another prompt".into(),
            },
        );
        for assignment in 1..=3 {
            structured_into(
                &mut threads,
                &envelope(AgentEvent::WorkerStarted {
                    turn: Some(2),
                    worker_id: "reused-researcher".into(),
                    objective: "research".into(),
                }),
            );
            let mut usage = envelope(AgentEvent::Usage {
                turn: Some(1),
                prompt_tokens: 2000,
                completion_tokens: 300,
                total_tokens: 2300,
                context_tokens: 2000,
                context_window: None,
            });
            usage.actor = Actor::Worker {
                id: "reused-researcher".into(),
            };
            usage.session_id = "reused-researcher".into();
            usage.task_id = Some(format!("work-{assignment}"));
            structured_into(&mut threads, &usage);
            structured_into(&mut threads, &usage);
            let result = serde_json::from_value(serde_json::json!({
                "work_id": format!("work-{assignment}"), "objective": "research", "generation": 1,
                "assignment": assignment, "outcome": "completed", "result": "done"
            }))
            .unwrap();
            usage.kind = AgentEvent::WorkResult { result };
            structured_into(&mut threads, &usage);
            structured_into(&mut threads, &usage);
        }
        let cells = build_turn_cells(&threads[0]);
        let cell = cells
            .iter()
            .find(|cell| threads[0].items[cell.prompt].turn.as_deref() == Some(&key))
            .unwrap();
        let badges = turn_cell_badges(&threads[0], cell);
        assert!(badges.contains("3 agents"), "{badges}");
        assert!(badges.contains("3 complete"), "{badges}");
        assert!(badges.contains("total 8.1k"), "{badges}");
        assert_eq!(threads[0].metrics[&key].worker_usage.len(), 3);
        let layout = turn_cell_layout(0, 0, &threads, cell, 200, 0, false, "", false, None);
        let rendered = layout
            .lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("done 4.5s"), "{rendered}");
        assert!(rendered.contains("total 8.1k"), "{rendered}");
        assert!(!rendered.contains("conversation:"), "{rendered}");
        assert!(!rendered.contains("elapsed"), "{rendered}");
        assert!(!rendered.contains("review tokens"), "{rendered}");
        let expanded = turn_cell_layout(0, 0, &threads, cell, 200, 0, false, "", true, None);
        let details = expanded
            .lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            details.contains("measured foreground model tokens 1.2k (prompt 1.0k + completion 200"),
            "{details}"
        );
        assert!(
            details.contains("worker reported tokens 6.9k (prompt 6.0k + completion 900)"),
            "{details}"
        );
        assert!(details.contains("review tokens unavailable"), "{details}");
        let old = cells
            .iter()
            .find(|cell| {
                threads[0].items[cell.prompt].turn.as_deref() == Some("conversation:old:2")
            })
            .unwrap();
        assert!(turn_cell_badges(&threads[0], old).is_empty());
        let pending = cells
            .iter()
            .find(|cell| {
                threads[0].items[cell.prompt].turn.as_deref() == Some("conversation:host:3")
            })
            .unwrap();
        assert!(turn_cell_badges(&threads[0], pending).is_empty());
        threads[0]
            .unread_turns
            .extend([key.clone(), "conversation:old:2".into()]);
        mark_ready_turn_seen(&mut threads, &key);
        assert!(threads[0].unread_turns.contains("conversation:old:2"));
        assert!(!threads[0].unread_turns.contains(&key));
    }
}

#[test]
fn async_finish_and_unscoped_metrics_do_not_cross_conversations() {
    let mut threads = Vec::new();
    for conversation in ["old", "host"] {
        for turn in ["2", "3"] {
            interaction_into(
                &mut threads,
                conversation,
                turn,
                InteractionEvent::UserTurnAccepted {
                    text: "request".into(),
                },
            );
        }
    }
    let mut unknown = envelope(AgentEvent::Usage {
        turn: Some(2),
        prompt_tokens: 100,
        completion_tokens: 20,
        total_tokens: 120,
        context_tokens: 100,
        context_window: None,
    });
    unknown.conversation_id = None;
    structured_into(&mut threads, &unknown);
    for conversation in ["old", "host"] {
        interaction_into(
            &mut threads,
            conversation,
            "2",
            InteractionEvent::ConversationFinished {
                text: "async answer".into(),
            },
        );
    }
    let key = "conversation:host:2";
    assert!(threads[0].unread_turns.contains(key));
    assert!(threads[0].unread_turns.contains("conversation:old:2"));
    assert!(!turn_badges(&threads[0], Some(key), 99_000, false).contains("total"));
    mark_ready_turn_seen(&mut threads, key);
    interaction_into(
        &mut threads,
        "host",
        "2",
        InteractionEvent::ConversationFinished {
            text: "async answer".into(),
        },
    );
    assert!(!threads[0].unread_turns.contains(key));
    assert!(threads[0].unread_turns.contains("conversation:old:2"));
    let cells = build_turn_cells(&threads[0]);
    for cell in cells {
        assert!(turn_cell_badges(&threads[0], &cell).is_empty());
    }
}
