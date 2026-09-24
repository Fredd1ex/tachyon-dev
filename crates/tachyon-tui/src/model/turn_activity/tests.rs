use super::*;

fn event(actor: &str, task: Option<&str>, kind: AgentEvent) -> EventEnvelope {
    EventEnvelope {
        event_id: 1,
        session_id: format!("session-{actor}"),
        conversation_id: Some("host".into()),
        turn_id: Some("conversation:host:2".into()),
        task_id: task.map(str::to_owned),
        parent_task_id: None,
        tool_call_id: None,
        actor: if actor == FOREGROUND_ID {
            Actor::Foreground
        } else {
            Actor::Worker { id: actor.into() }
        },
        sequence: 1,
        occurred_at_ms: 11_000,
        kind,
    }
}

fn start(id: &str, name: &str) -> AgentEvent {
    AgentEvent::ToolStarted {
        turn: Some(1),
        id: id.into(),
        name: name.into(),
        arguments: "SECRET_ARGUMENT".into(),
        identity: None,
    }
}

fn finish(id: &str) -> AgentEvent {
    AgentEvent::ToolFinished {
        turn: Some(1),
        id: id.into(),
        output: "SECRET_OUTPUT".into(),
        identity: None,
    }
}

fn assignment_event(
    generation: u64,
    assignment: u64,
    attempt: &str,
    mut kind: AgentEvent,
) -> EventEnvelope {
    let identity = tachyon_api::types::ToolTelemetryIdentity {
        task_id: Some("reused-work".into()),
        work_id: Some("reused-work".into()),
        generation: Some(generation),
        assignment: Some(assignment),
        attempt_id: Some(attempt.into()),
    };
    match &mut kind {
        AgentEvent::ToolStarted {
            identity: fence, ..
        }
        | AgentEvent::ToolFinished {
            identity: fence, ..
        } => {
            *fence = Some(identity);
        }
        AgentEvent::WorkResult { result } => {
            result.work_id = "reused-work".into();
            result.generation = generation;
            result.assignment = assignment;
            result.attempt_id = Some(attempt.into());
        }
        _ => panic!("not an assignment event"),
    }
    let mut e = event("warm-worker", Some("reused-work"), kind);
    // Daemon scope: worker process session, host conversation/turn, child-local
    // payload turn, logical work ID and producer fence retained verbatim.
    e.session_id = "warm-worker".into();
    e.turn_id = Some("2".into());
    e.tool_call_id = match &e.kind {
        AgentEvent::ToolStarted { id, .. } | AgentEvent::ToolFinished { id, .. } => {
            Some(id.clone())
        }
        _ => None,
    };
    e
}

fn work_result() -> AgentEvent {
    AgentEvent::WorkResult {
        result: serde_json::from_value(serde_json::json!({
            "work_id":"reused-work", "objective":"task", "generation":1, "assignment":1,
            "outcome":"completed", "result":"done"
        }))
        .unwrap(),
    }
}

fn deliver(threads: &mut Vec<Thread>, envelope: &EventEnvelope) {
    let mut e: EventEnvelope =
        serde_json::from_str(&serde_json::to_string(envelope).unwrap()).unwrap();
    qualify_event_turn(&mut e);
    record_correlated_metrics(threads, &e);
    let actor = match &e.actor {
        Actor::Worker { id } => id.as_str(),
        _ => FOREGROUND_ID,
    };
    let index = find_or_create_thread(threads, actor, actor == FOREGROUND_ID, None);
    apply_actor_event(&mut threads[index], e.kind, &e.actor, e.turn_id.as_deref());
}

#[test]
fn task_card_observes_live_tools_before_work_result_and_rejects_stale_fences() {
    use crate::app::{build_turn_cells, panels::activity, transcript::layout::inline_cell_layout};
    use ratatui::{backend::TestBackend, widgets::Paragraph, Terminal};
    let mut threads = vec![Thread::new_foreground()];
    let turn = "conversation:host:2";
    accept(&mut threads[0], turn);
    threads[0].add_turn(
        ItemKind::Spawn,
        "worker warm-worker: Fix selection and detail UX".into(),
        Some(turn.into()),
    );
    let mut started = start("call", "Read");
    if let AgentEvent::ToolStarted { arguments, .. } = &mut started {
        *arguments =
            r#"{"filePath":"crates/tachyon-tui/src/app/verification/pty.rs","token":"NEVER_SHOW"}"#
                .into();
    }
    deliver(&mut threads, &assignment_event(1, 1, "first", started));
    let rows = activity::tasks(&threads, 0, Some(turn));
    assert_eq!(rows.len(), 1);
    assert!(activity::latest(&threads, 0, &rows[0], 120).contains("Read crates/"));
    let mut ended = finish("call");
    if let AgentEvent::ToolFinished { output, .. } = &mut ended {
        *output = r#"{"summary":"Read 80 lines","sources":["pty.rs"]}"#.into();
    }
    deliver(&mut threads, &assignment_event(1, 1, "first", ended));
    assert!(activity::latest(&threads, 0, &rows[0], 120).contains("Read 80 lines"));
    deliver(
        &mut threads,
        &assignment_event(1, 2, "second", start("call", "NewSearch")),
    );
    deliver(
        &mut threads,
        &assignment_event(1, 1, "first", finish("call")),
    );
    assert!(activity::latest(&threads, 0, &rows[0], 120).contains("NewSearch"));
    assert!(!activity::latest(&threads, 0, &rows[0], 120).contains("result recorded"));
    for width in [1, 2, 12, 40, 120] {
        let cells = build_turn_cells(&threads[0]);
        let layout = inline_cell_layout(0, 0, &threads, &cells[0], width, 0, true, "", false, None);
        let at = layout.activity_row.unwrap();
        assert!(layout.lines[at].width() <= width as usize);
        assert!(layout.lines[at + 1].width() <= width as usize);
        assert_eq!(layout.hits[at], layout.hits[at + 1]);
        let mut terminal = Terminal::new(TestBackend::new(width, 30)).unwrap();
        terminal
            .draw(|f| f.render_widget(Paragraph::new(layout.lines.clone()), f.area()))
            .unwrap();
        let screen = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect::<String>();
        assert!(!screen.contains("NEVER_SHOW"));
        if width == 120 {
            assert!(screen.contains("NewSearch"));
        }
    }
}

#[test]
fn daemon_scoped_new_assignment_survives_old_finishes_results_and_reused_call_ids() {
    for (old, new) in [((1, 1), (1, 2)), ((1, 9), (2, 1))] {
        for finish_before_start in [false, true] {
            let mut threads = vec![Thread::new_foreground()];
            accept(&mut threads[0], "conversation:host:2");
            deliver(
                &mut threads,
                &event(
                    FOREGROUND_ID,
                    None,
                    AgentEvent::WorkerStarted {
                        turn: Some(2),
                        worker_id: "warm-worker".into(),
                        objective: "task".into(),
                    },
                ),
            );
            let old_start =
                assignment_event(old.0, old.1, "old-attempt", start("same-call", "OldSearch"));
            let old_finish = assignment_event(old.0, old.1, "old-attempt", finish("same-call"));
            let new_start =
                assignment_event(new.0, new.1, "new-attempt", start("same-call", "NewSearch"));
            deliver(&mut threads, &old_start);
            if finish_before_start {
                deliver(&mut threads, &old_finish);
            }
            deliver(&mut threads, &new_start);
            assert_eq!(summary(&threads[0].activity), "warm-worker: NewSearch");
            // Host delivery sequence/time cannot make an old producer fence new.
            for mut late in [
                old_finish,
                old_start,
                assignment_event(old.0, old.1, "old-attempt", work_result()),
            ] {
                late.sequence = 999;
                late.occurred_at_ms = 99_999;
                deliver(&mut threads, &late);
                assert_eq!(summary(&threads[0].activity), "warm-worker: NewSearch");
            }
            let cells = build_turn_cells(&threads[0]);
            let copy = selected_chat_cell_text(&threads, Some(0));
            let layout = turn_cell_layout(0, 0, &threads, &cells[0], 200, 0, true, "", true, None);
            let output = (0..layout.lines.len())
                .map(|row| elapsed::overlay(&layout, row, 200, 100_000).to_string())
                .collect::<Vec<_>>()
                .join("\n");
            assert_eq!(
                output.matches("warm-worker: NewSearch").count(),
                1,
                "{output}"
            );
            assert!(!output.contains("warm-worker: OldSearch"));
            assert!(!output.contains("old-attempt"));
            assert!(!output.contains("new-attempt"));
            assert_eq!(selected_chat_cell_text(&threads, Some(0)), copy);
            deliver(
                &mut threads,
                &assignment_event(new.0, new.1, "new-attempt", finish("same-call")),
            );
            deliver(&mut threads, &new_start);
            assert!(
                summary(&threads[0].activity).is_empty(),
                "finished call was revived"
            );
        }
    }
}

#[test]
fn assignment_results_before_starts_and_legacy_terminal_events_do_not_close_new_fences() {
    let mut threads = vec![Thread::new_foreground()];
    accept(&mut threads[0], "conversation:host:2");
    let mut old_result = assignment_event(1, 1, "a", work_result());
    old_result.actor = Actor::Foreground;
    old_result.session_id = "host-session".into();
    deliver(&mut threads, &old_result);
    deliver(
        &mut threads,
        &assignment_event(1, 1, "a", start("same-call", "OldSearch")),
    );
    assert!(summary(&threads[0].activity).is_empty());
    deliver(
        &mut threads,
        &assignment_event(1, 2, "b", start("same-call", "NewSearch")),
    );
    let foreground = event(FOREGROUND_ID, None, start("same-call", "ForegroundSearch"));
    deliver(&mut threads, &foreground);
    for late in [
        old_result,
        event(
            "warm-worker",
            None,
            AgentEvent::WorkerCompleted {
                worker_id: "warm-worker".into(),
                objective: "task".into(),
                result: "old done".into(),
                artifacts: vec![],
                context: String::new(),
                suggested_reuse: false,
            },
        ),
        event("warm-worker", Some("reused-work"), finish("same-call")),
    ] {
        deliver(&mut threads, &late);
        assert_eq!(
            summary(&threads[0].activity),
            "foreground: ForegroundSearch | warm-worker: NewSearch"
        );
    }
    let mut new_result = assignment_event(1, 2, "b", work_result());
    new_result.actor = Actor::Foreground;
    deliver(&mut threads, &new_result);
    assert_eq!(
        summary(&threads[0].activity),
        "foreground: ForegroundSearch"
    );
    // Receiving a future terminal fence first suppresses even unseen call IDs.
    deliver(&mut threads, &assignment_event(2, 1, "c", work_result()));
    deliver(
        &mut threads,
        &assignment_event(2, 1, "c", start("unseen", "ClosedSearch")),
    );
    assert_eq!(
        summary(&threads[0].activity),
        "foreground: ForegroundSearch"
    );
    deliver(
        &mut threads,
        &assignment_event(2, 2, "d", start("same-call", "CurrentSearch")),
    );
    assert!(summary(&threads[0].activity).contains("CurrentSearch"));
}

#[test]
fn assignment_attempts_and_incomplete_fences_never_alias_or_fall_back_to_legacy() {
    let mut threads = vec![Thread::new_foreground()];
    let new_start = assignment_event(3, 7, "current-attempt", start("same-call", "CurrentSearch"));
    deliver(&mut threads, &new_start);
    for kind in [
        start("same-call", "WrongAttempt"),
        finish("same-call"),
        work_result(),
    ] {
        deliver(
            &mut threads,
            &assignment_event(3, 7, "different-attempt", kind),
        );
        assert_eq!(summary(&threads[0].activity), "warm-worker: CurrentSearch");
    }
    for missing in 0..5 {
        let mut malformed = assignment_event(3, 8, "next-attempt", finish("same-call"));
        if let AgentEvent::ToolFinished {
            identity: Some(identity),
            ..
        } = &mut malformed.kind
        {
            match missing {
                0 => identity.generation = None,
                1 => identity.assignment = None,
                2 => identity.work_id = None,
                3 => identity.task_id = Some("other-work".into()),
                _ => identity.work_id = Some("other-work".into()),
            }
        }
        deliver(&mut threads, &malformed);
        assert_eq!(summary(&threads[0].activity), "warm-worker: CurrentSearch");
    }
    // A finish for the valid next assignment is a tombstone, not a request to
    // finish a same-named call from the previous assignment.
    deliver(
        &mut threads,
        &assignment_event(3, 8, "next-attempt", finish("same-call")),
    );
    deliver(
        &mut threads,
        &assignment_event(3, 8, "next-attempt", start("same-call", "AlreadyFinished")),
    );
    deliver(&mut threads, &new_start);
    assert!(summary(&threads[0].activity).is_empty());
    deliver(
        &mut threads,
        &assignment_event(3, 8, "next-attempt", start("other-call", "OtherSearch")),
    );
    assert_eq!(summary(&threads[0].activity), "warm-worker: OtherSearch");
}

#[test]
fn advancing_assignment_reclaims_scopes_without_losing_the_latest_fence() {
    let mut threads = vec![Thread::new_foreground()];
    for assignment in 1..=MAX_SCOPES as u64 + 10 {
        deliver(
            &mut threads,
            &assignment_event(1, assignment, "attempt", start("same-call", "Search")),
        );
        assert_eq!(summary(&threads[0].activity), "warm-worker: Search");
        assert_eq!(threads[0].activity.assignments.len(), 1);
        assert_eq!(threads[0].activity.scopes.len(), 1);
    }
    deliver(
        &mut threads,
        &assignment_event(1, 1, "attempt", work_result()),
    );
    assert_eq!(summary(&threads[0].activity), "warm-worker: Search");
    threads[0].finish_reply("done".into(), Some("conversation:host:2".into()));
    assert!(threads[0].activity.assignments.is_empty());
    deliver(
        &mut threads,
        &assignment_event(2, 1, "later", start("same-call", "LateSearch")),
    );
    assert!(summary(&threads[0].activity).is_empty());
}

#[test]
fn assignment_fences_follow_work_across_workers_but_not_across_conversations() {
    let mut threads = vec![Thread::new_foreground()];
    let original = assignment_event(1, 1, "original", start("same-call", "OriginalSearch"));
    deliver(&mut threads, &original);
    let mut replacement =
        assignment_event(2, 1, "replacement", start("same-call", "ReplacementSearch"));
    replacement.actor = Actor::Worker {
        id: "replacement-worker".into(),
    };
    replacement.session_id = "replacement-worker".into();
    deliver(&mut threads, &replacement);
    let mut unrelated = original.clone();
    unrelated.conversation_id = Some("other-conversation".into());
    deliver(&mut threads, &unrelated);
    deliver(
        &mut threads,
        &assignment_event(1, 1, "original", work_result()),
    );
    assert!(threads[0]
        .activity
        .finish_actor("warm-worker")
        .contains("conversation:other-conversation:2"));
    assert_eq!(
        summary(&threads[0].activity),
        "replacement-worker: ReplacementSearch"
    );
    // A different conversation's old fence must not advance the host work.
    let mut unrelated_next = assignment_event(3, 1, "other", start("same-call", "OtherSearch"));
    unrelated_next.conversation_id = Some("other-conversation".into());
    deliver(&mut threads, &unrelated_next);
    let mut result = assignment_event(2, 1, "replacement", work_result());
    result.actor = Actor::Foreground;
    deliver(&mut threads, &result);
    assert!(summary(&threads[0].activity).is_empty());
    assert_eq!(
        threads[0]
            .activity
            .summary("conversation:other-conversation:2", None),
        "warm-worker: OtherSearch"
    );
}

fn accept(thread: &mut Thread, turn: &str) {
    thread.add(ItemKind::User, "question".into());
    thread.reserve_reply();
    let mut metadata = tachyon_api::InteractionMetadata::new("accepted", "request", "host", 1);
    metadata.turn_id = Some(turn.into());
    metadata.occurred_at_ms = 10_000;
    apply_interaction_event(
        thread,
        InteractionEventEnvelope {
            metadata,
            event: InteractionEvent::UserTurnAccepted {
                text: "question".into(),
            },
        },
    );
}

fn summary(activity: &Activity) -> String {
    activity.summary("conversation:host:2", None)
}

#[test]
fn provisional_stream_final_share_one_slot_and_generic_status_cannot_regress() {
    let mut thread = Thread::new_foreground();
    let turn = "conversation:host:2";
    accept(&mut thread, turn);
    let status = |thread: &mut Thread, message: &str| {
        apply_correlated_agent_event(
            thread,
            AgentEvent::Status {
                turn: Some(99),
                phase: "working".into(),
                message: message.into(),
            },
            Some(turn),
        );
    };
    status(&mut thread, "I will compare the two specifications.");
    let slot = thread
        .items
        .iter()
        .position(|i| i.kind == ItemKind::PendingReply)
        .unwrap();
    let revision = thread.items[slot].revision;
    for text in [
        "",
        "working",
        "Working...",
        "Working on that.",
        "queued",
        "Checking information...",
    ] {
        status(&mut thread, text);
        assert_eq!(
            thread.items[slot].text,
            "I will compare the two specifications."
        );
        assert_eq!(thread.items[slot].revision, revision);
    }
    status(&mut thread, "Comparing the second specification now.");
    thread.add_reply_fragment("The **answer**".into(), Some(turn.into()), false);
    status(&mut thread, "A delayed contextual status.");
    thread.add_reply_fragment(" is here.".into(), Some(turn.into()), false);
    assert_eq!(thread.items[slot].text, "The **answer** is here.");
    thread.finish_reply("Final answer.".into(), Some(turn.into()));
    let copy = selected_chat_cell_text(&[thread], Some(0)).unwrap();
    assert!(copy.contains("Final answer."));
    assert!(!copy.contains("specification"));
    assert!(!copy.contains("delayed"));
}

#[test]
fn followup_finishes_before_parent_and_late_status_delta_error_do_not_reopen() {
    let mut thread = Thread::new_foreground();
    for turn in ["2", "3"] {
        accept(&mut thread, turn);
    }
    thread.finish_reply("quick answer".into(), Some("3".into()));
    assert!(busy(&thread));
    thread.update_pending_reply_status(Some("3".into()), "working", "late status");
    thread.add_reply_fragment("late delta".into(), Some("3".into()), false);
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::Error {
            turn: Some(3),
            message: "late failure".into(),
        },
        Some("3"),
    );
    assert_eq!(
        thread
            .items
            .iter()
            .find(|i| i.kind == ItemKind::Reply)
            .unwrap()
            .text,
        "quick answer"
    );
    thread.finish_reply("parent answer".into(), Some("2".into()));
    assert!(!busy(&thread));
    thread.update_pending_reply_status(Some("2".into()), "working", "late parent");
    assert!(!busy(&thread));
    assert_eq!(
        thread
            .items
            .iter()
            .filter(|i| i.kind == ItemKind::Reply)
            .count(),
        2
    );
    assert!(thread.unread_turns.contains("2"));
}

#[test]
fn timeout_is_terminal_but_recovered_final_can_replace_it() {
    let mut thread = Thread::new_foreground();
    accept(&mut thread, "2");
    let mut metadata = tachyon_api::InteractionMetadata::new("timeout", "request", "host", 2);
    metadata.turn_id = Some("2".into());
    apply_interaction_event(
        &mut thread,
        InteractionEventEnvelope {
            metadata,
            event: InteractionEvent::ForegroundRequestTimedOut { deadline_ms: 100 },
        },
    );
    thread.update_pending_reply_status(Some("2".into()), "working", "late");
    thread.add_reply_fragment("late delta".into(), Some("2".into()), false);
    assert!(!busy(&thread));
    assert!(!thread
        .items
        .iter()
        .any(|i| i.kind == ItemKind::PendingReply || i.kind == ItemKind::Reply));
    thread.finish_reply("late live final".into(), Some("2".into()));
    assert!(!thread.items.iter().any(|i| i.kind == ItemKind::Reply));
    reply(
        &mut thread,
        Some("2".into()),
        ReplyUpdate::Recovered("recovered".into()),
    );
    assert_eq!(
        thread
            .items
            .iter()
            .filter(|i| i.kind == ItemKind::Reply)
            .count(),
        1
    );
}

#[test]
fn terminal_failure_is_visible_preserves_partial_and_only_durable_recovery_supersedes_it() {
    for partial in [false, true] {
        for timeout in [false, true] {
            let mut thread = Thread::new_foreground();
            accept(&mut thread, "2");
            thread.update_pending_reply_status(
                Some("2".into()),
                "working",
                "Checking the sources.",
            );
            if partial {
                thread.add_reply_fragment(
                    "An unfinished **answer**.".into(),
                    Some("2".into()),
                    false,
                );
            }
            if timeout {
                let mut metadata =
                    tachyon_api::InteractionMetadata::new("timeout", "request", "host", 2);
                metadata.turn_id = Some("2".into());
                apply_interaction_event(
                    &mut thread,
                    InteractionEventEnvelope {
                        metadata,
                        event: InteractionEvent::ForegroundRequestTimedOut { deadline_ms: 100 },
                    },
                );
            } else {
                apply_correlated_agent_event(
                    &mut thread,
                    AgentEvent::Error {
                        turn: Some(2),
                        message: "provider unavailable".into(),
                    },
                    Some("2"),
                );
            }
            let render = |thread: &Thread, width| {
                let cells = build_turn_cells(thread);
                let layout = turn_cell_layout(
                    0,
                    0,
                    std::slice::from_ref(thread),
                    &cells[0],
                    width,
                    0,
                    false,
                    "",
                    false,
                    None,
                );
                assert!(
                    layout.timers.is_empty(),
                    "terminal error retained an active timer"
                );
                assert_eq!(layout.lines.len(), layout.hits.len());
                layout
                    .lines
                    .iter()
                    .map(Line::to_string)
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            let text = render(&thread, 80);
            assert!(
                text.contains(if timeout {
                    "Failed: request timed out after 100ms"
                } else {
                    "Failed: provider unavailable"
                }),
                "{text}"
            );
            assert_eq!(text.contains("Partial answer (incomplete):"), partial);
            assert_eq!(text.contains("An unfinished answer."), partial);
            assert!(!text.contains("Checking the sources"));
            assert!(!busy(&thread));
            let cells = build_turn_cells(&thread);
            let failed_revision = cell_revision(&thread, &cells[0]);
            for live in [true, false] {
                if live {
                    apply_correlated_agent_event(
                        &mut thread,
                        AgentEvent::Reply {
                            turn: Some(2),
                            text: "late agent final".into(),
                            final_reply: true,
                        },
                        Some("2"),
                    );
                } else {
                    let mut metadata =
                        tachyon_api::InteractionMetadata::new("final", "request", "host", 3);
                    metadata.turn_id = Some("2".into());
                    apply_interaction_event(
                        &mut thread,
                        InteractionEventEnvelope {
                            metadata,
                            event: InteractionEvent::ConversationFinished {
                                text: "late interaction final".into(),
                            },
                        },
                    );
                }
                thread.add_reply_fragment("late delta".into(), Some("2".into()), false);
                thread.update_pending_reply_status(Some("2".into()), "working", "late status");
                assert_eq!(render(&thread, 80), text);
            }
            // The marker survives archive serialization, unlike ephemeral activity.
            let mut restored = restore_session(session_snapshot(&[thread]));
            assert_eq!(render(&restored[0], 80), text);
            restored[0].finish_reply("late after restore".into(), Some("2".into()));
            assert_eq!(render(&restored[0], 80), text);
            let copy = selected_chat_cell_text(&restored, Some(0)).unwrap();
            assert_eq!(copy.contains("An unfinished **answer**."), partial);
            assert!(!copy.contains("late"));
            reply(
                &mut restored[0],
                Some("2".into()),
                ReplyUpdate::Recovered("durable answer".into()),
            );
            let recovered = render(&restored[0], 80);
            assert!(recovered.contains("durable answer"));
            assert!(!recovered.contains("Failed:"));
            assert!(!recovered.contains("Partial answer"));
            assert_ne!(
                cell_revision(&restored[0], &build_turn_cells(&restored[0])[0]),
                failed_revision
            );
        }
    }
}

#[test]
fn completed_timing_before_live_final_is_not_a_failure() {
    let mut thread = Thread::new_foreground();
    accept(&mut thread, "conversation:host:2");
    thread.update_pending_reply_status(
        Some("conversation:host:2".into()),
        "working",
        "Checking sources.",
    );
    let mut threads = vec![thread];
    record_correlated_metrics(
        &mut threads,
        &event(
            FOREGROUND_ID,
            None,
            AgentEvent::Timing {
                turn: 2,
                stage: "completed".into(),
                elapsed_ms: 1234,
            },
        ),
    );
    assert!(threads[0].metrics["conversation:host:2"]
        .ended_at_ms
        .is_some());
    threads[0].finish_reply(
        "legitimate final".into(),
        Some("conversation:host:2".into()),
    );
    let cells = build_turn_cells(&threads[0]);
    assert_eq!(
        turn_response(&threads[0], &cells[0]).unwrap().text,
        "legitimate final"
    );
    assert!(!busy(&threads[0]));
}

#[test]
fn contextual_status_wraps_in_cached_layout_at_realistic_and_narrow_widths() {
    let mut threads = vec![Thread::new_foreground()];
    accept(&mut threads[0], "2");
    let message = "I am comparing the specifications and checking the cited sources before answering your question about compatibility, deployment, and long term maintenance.";
    threads[0].update_pending_reply_status(Some("2".into()), "working", message);
    let cells = build_turn_cells(&threads[0]);
    let mut cache = TurnLayoutCache::default();
    let mut heights = Vec::new();
    for width in [40, 80, 120, 40] {
        cache.prepare(width, &cells, threads[0].structure_revision);
        let revision = cell_revision(&threads[0], &cells[0]);
        let layout = cache.layout(cell_key(&cells[0]), revision, 0, || {
            turn_cell_layout(0, 0, &threads, &cells[0], width, 0, true, "", false, None)
        });
        let status_rows = layout
            .lines
            .iter()
            .skip(8)
            .filter(|line| {
                line.spans
                    .first()
                    .is_some_and(|span| span.content == "    ")
            })
            .collect::<Vec<_>>();
        assert!(status_rows.len() > 1);
        assert!(status_rows
            .iter()
            .flat_map(|line| &line.spans)
            .all(|span| !span.style.add_modifier.contains(Modifier::ITALIC)));
        assert!(status_rows
            .iter()
            .all(|line| line.width() <= width as usize));
        let reconstructed = status_rows
            .iter()
            .map(|line| line.spans.last().unwrap().content.as_ref())
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(reconstructed, message);
        heights.push(layout.lines.len());
        let baseline = layout.lines.clone();
        let hits = layout.hits.clone();
        for now in [12_000, 80_000, u64::MAX] {
            let cached = cache.layout(cell_key(&cells[0]), revision, 0, || {
                panic!("timer reflowed status")
            });
            for (row, line) in cached.lines.iter().enumerate().skip(8) {
                if line
                    .spans
                    .first()
                    .is_some_and(|span| span.content == "    ")
                {
                    assert_eq!(elapsed::overlay(cached, row, width, now), *line);
                }
            }
            assert_eq!(cached.lines, baseline);
            assert_eq!(cached.hits, hits);
        }
    }
    assert_eq!(heights[0], heights[3]);
    assert!(heights[0] > heights[1]);
    assert_eq!(cache.builds, 4);
    for width in [0, 1, 4, 8, 24, 80] {
        for text in [
            "word ".repeat(10_000),
            "x".repeat(10_000),
            "界 wide words ".repeat(100),
        ] {
            let rows = status_lines(&text, width, "    > ", Style::default());
            assert!(rows.len() <= 256);
            assert!(rows.iter().all(|row| row.width() <= width as usize));
        }
    }
}

#[test]
fn background_status_never_replaces_foreground_provisional() {
    let mut thread = Thread::new_foreground();
    accept(&mut thread, "2");
    thread.update_pending_reply_status(Some("2".into()), "working", "Checking your question.");
    apply_actor_event(
        &mut thread,
        AgentEvent::Status {
            turn: Some(2),
            phase: "working".into(),
            message: "background maintenance".into(),
        },
        &Actor::Background,
        Some("2"),
    );
    assert_eq!(thread.items[1].text, "Checking your question.");
}

#[test]
fn finish_before_start_duplicates_and_reused_call_ids_are_scoped() {
    let mut activity = Activity::default();
    let a = event("a", Some("job-a"), start("call", "Search"));
    activity.record(&event("a", Some("job-a"), finish("call")));
    activity.record(&a);
    assert!(summary(&activity).is_empty());
    let b = event("b", Some("job-b"), start("call", "Read"));
    activity.record(&b);
    activity.record(&b);
    activity.record(&event(FOREGROUND_ID, None, start("call", "Search")));
    assert_eq!(summary(&activity).matches("b: Read").count(), 1);
    assert!(summary(&activity).contains("foreground: Search"));
    activity.record(&event("a", Some("job-a"), finish("call")));
    assert!(summary(&activity).contains("b: Read"));
    let mut other = b.clone();
    other.turn_id = Some("conversation:other:2".into());
    activity.record(&other);
    activity.record(&event("b", Some("job-b"), finish("call")));
    assert!(!summary(&activity).contains("b: Read"));
    assert!(activity
        .summary("conversation:other:2", None)
        .contains("b: Read"));
    assert!(!summary(&activity).contains("SECRET"));
}

#[test]
fn old_assignment_result_cannot_clear_new_assignment_and_all_outcomes_close() {
    for outcome in [
        serde_json::json!({"outcome":"completed","result":"done"}),
        serde_json::json!({"outcome":"failed","message":"failed"}),
        serde_json::json!({"outcome":"cancelled","reason":"cancelled"}),
    ] {
        let mut activity = Activity::default();
        activity.record(&event("warm", Some("old"), start("call", "OldTool")));
        activity.record(&event("warm", Some("new"), start("call", "NewTool")));
        let mut value =
            serde_json::json!({"work_id":"old","objective":"task","generation":1,"assignment":1});
        value
            .as_object_mut()
            .unwrap()
            .extend(outcome.as_object().unwrap().clone());
        let result = AgentEvent::WorkResult {
            result: serde_json::from_value(value).unwrap(),
        };
        activity.record(&event("warm", Some("old"), result.clone()));
        activity.record(&event("warm", Some("old"), start("late", "LateTool")));
        assert_eq!(summary(&activity), "warm: NewTool");
        // Terminal delivery before its start is also remembered.
        let mut early = Activity::default();
        early.record(&event(
            FOREGROUND_ID,
            None,
            start("lookup", "ForegroundSearch"),
        ));
        early.record(&event(FOREGROUND_ID, Some("old"), result));
        early.record(&event("warm", Some("old"), start("call", "OldTool")));
        assert_eq!(summary(&early), "foreground: ForegroundSearch");
    }
}

#[test]
fn worker_completion_error_and_subscription_end_clear_activity_without_revival() {
    for terminal in [
        AgentEvent::WorkerCompleted {
            worker_id: "a".into(),
            objective: "task".into(),
            result: "done".into(),
            artifacts: vec![],
            context: String::new(),
            suggested_reuse: false,
        },
        AgentEvent::Error {
            turn: Some(1),
            message: "failed".into(),
        },
        AgentEvent::WorkerReleaseRequested {
            reason: "cancelled".into(),
        },
    ] {
        for early in [false, true] {
            let mut activity = Activity::default();
            if !early {
                activity.record(&event("a", Some("task"), start("call", "Search")));
            }
            activity.record(&event("a", None, terminal.clone()));
            activity.record(&event("a", Some("task"), start("late", "Search")));
            assert!(summary(&activity).is_empty());
        }
    }
    let mut activity = Activity::default();
    activity.record(&event("a", Some("task"), start("call", "Search")));
    activity.finish_actor("a");
    activity.record(&event("a", Some("other"), start("late", "Search")));
    assert!(summary(&activity).is_empty());
    let mut next_turn = event("a", Some("other"), start("call", "NextSearch"));
    next_turn.turn_id = Some("conversation:host:3".into());
    activity.record(&next_turn);
    assert_eq!(
        activity.summary("conversation:host:3", None),
        "a: NextSearch"
    );
}

#[test]
fn completed_turns_release_monitor_capacity_without_admitting_late_starts() {
    let mut thread = Thread::new_foreground();
    for n in 0..MAX_SCOPES + 5 {
        let turn = format!("conversation:host:{n}");
        let mut e = event("worker", Some("work"), start("call", "Search"));
        e.turn_id = Some(turn.clone());
        record(&mut thread, &e);
        assert_eq!(thread.activity.summary(&turn, None), "worker: Search");
        thread.finish_reply("answer".into(), Some(turn.clone()));
        assert!(thread.activity.scopes.is_empty());
        record(&mut thread, &e);
        assert!(thread.activity.summary(&turn, None).is_empty());
    }
}

#[test]
fn monitor_caps_fail_closed_and_labels_are_bounded_without_control_characters() {
    let mut activity = Activity::default();
    for i in 0..MAX_CALLS {
        activity.record(&event("a", None, start(&i.to_string(), "Search")));
    }
    assert!(summary(&activity).ends_with("+125 tools"));
    activity.record(&event("a", None, start("overflow", "Search")));
    assert!(summary(&activity).is_empty());
    for i in 0..MAX_SCOPES + 10 {
        activity.record(&event(&format!("worker-{i}"), None, start("call", "Read")));
    }
    assert_eq!(activity.scopes.len(), MAX_SCOPES);
    assert!(summary(&activity).len() < 180);
    assert!(activity.saturated());
    activity.finish_turn("conversation:host:2");
    activity.record(&event("worker", None, start("late", "Uncertain")));
    assert!(
        summary(&activity).is_empty(),
        "capacity reclamation must not revive dropped events"
    );
    let mut activity = Activity::default();
    activity.record(&event(
        "a",
        None,
        start("call", &format!("\n\t{}", "x".repeat(200))),
    ));
    assert_eq!(summary(&activity), format!("a: {}", "x".repeat(32)));
}

#[test]
fn active_overlay_ticks_resize_copy_cache_and_archive_are_independent() {
    let turn = "conversation:host:2";
    let mut threads = vec![Thread::new_foreground()];
    accept(&mut threads[0], turn);
    threads[0].add_reply_fragment(
        "A **formatted** streaming answer. ".repeat(30),
        Some(turn.into()),
        false,
    );
    for actor in [FOREGROUND_ID, "worker-a", "worker-b"] {
        record_correlated_metrics(&mut threads, &event(actor, None, start("call", "Search")));
    }
    let cells = build_turn_cells(&threads[0]);
    let cell = &cells[0];
    let copy = selected_chat_cell_text(&threads, Some(0));
    let mut cache = TurnLayoutCache::default();
    for width in [1, 12, 80, 200] {
        cache.prepare(width, &cells, threads[0].structure_revision);
        let revision = cell_revision(&threads[0], cell);
        let layout = cache.layout(cell_key(cell), revision, 0, || {
            turn_cell_layout(0, 0, &threads, cell, width, 0, true, "", false, None)
        });
        let lines = layout.lines.clone();
        let hits = layout.hits.clone();
        for at in [12_000, 82_000, u64::MAX] {
            let layout = cache.layout(cell_key(cell), revision, 0, || {
                panic!("tick rebuilt Markdown")
            });
            let rendered = (0..layout.lines.len())
                .map(|row| {
                    let line = elapsed::overlay(layout, row, width, at);
                    if line != layout.lines[row] {
                        assert!(line.width() <= width as usize);
                    }
                    line.to_string()
                })
                .collect::<Vec<_>>()
                .join("\n");
            if width == 200 {
                for label in ["3 tools"] {
                    assert!(rendered.contains(label), "{rendered}");
                }
                assert!(!rendered.contains("worker-a"));
                assert!(!rendered.contains("foreground:"));
            }
            assert!(!rendered.contains("SECRET"));
            assert_eq!(layout.lines, lines);
            assert_eq!(layout.hits, hits);
            assert_eq!(selected_chat_cell_text(&threads, Some(0)), copy);
        }
    }
    assert_eq!(cache.builds, 4);
    let before = cell_revision(&threads[0], cell);
    record_correlated_metrics(&mut threads, &event("worker-a", None, finish("call")));
    assert_ne!(cell_revision(&threads[0], cell), before);
    assert!(!summary(&threads[0].activity).contains("worker-a"));
    threads[0].history_len = threads[0].items.len();
    let layout = turn_cell_layout(0, 0, &threads, cell, 200, 0, false, "", false, None);
    let text = (0..layout.lines.len())
        .map(|row| elapsed::overlay(&layout, row, 200, 82_000).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!text.contains("Search"));
    assert!(!text.contains("elapsed"));
    let restored = restore_session(session_snapshot(&threads));
    assert!(summary(&restored[0].activity).is_empty());
}

#[test]
fn worker_rows_show_parallel_tools_and_terminal_parent_suppresses_late_tools() {
    let turn = "conversation:host:2";
    let mut threads = vec![Thread::new_foreground()];
    accept(&mut threads[0], turn);
    for id in ["a", "b"] {
        let e = event(
            FOREGROUND_ID,
            None,
            AgentEvent::WorkerStarted {
                turn: Some(2),
                worker_id: id.into(),
                objective: "task".into(),
            },
        );
        record_correlated_metrics(&mut threads, &e);
        apply_actor_event(&mut threads[0], e.kind, &e.actor, e.turn_id.as_deref());
        record_correlated_metrics(&mut threads, &event(id, Some(id), start("call", "Search")));
    }
    let render = |threads: &[Thread]| {
        let cells = build_turn_cells(&threads[0]);
        let layout = turn_cell_layout(0, 0, threads, &cells[0], 200, 0, true, "", true, None);
        (0..layout.lines.len())
            .map(|row| elapsed::overlay(&layout, row, 200, 20_000).to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };
    let live = render(&threads);
    for actor in ["a", "b"] {
        assert!(
            live.lines()
                .any(|line| line.contains(&format!("{actor}: Search"))
                    && line.contains("elapsed")
                    && !line.contains("| a: Search | b: Search")),
            "{live}"
        );
    }
    let copy = selected_chat_cell_text(&threads, Some(0));
    record_correlated_metrics(&mut threads, &event("a", Some("a"), finish("call")));
    assert!(!render(&threads).contains("a: Search"));
    assert!(render(&threads).contains("b: Search"));
    assert_eq!(selected_chat_cell_text(&threads, Some(0)), copy);
    threads[0].finish_reply("answer".into(), Some(turn.into()));
    record_correlated_metrics(
        &mut threads,
        &event("b", Some("b"), start("late", "LateTool")),
    );
    let final_text = render(&threads);
    assert!(!final_text.contains(": Search"));
    assert!(!final_text.contains("LateTool"));
}

#[test]
fn real_draws_keep_detached_viewport_selection_and_history_cache_during_tool_changes() {
    let mut threads = vec![Thread::new_foreground()];
    accept(&mut threads[0], "conversation:old:2");
    threads[0].finish_reply(
        "Old **formatted** answer. ".repeat(100),
        Some("conversation:old:2".into()),
    );
    threads[0].history_len = threads[0].items.len();
    accept(&mut threads[0], "conversation:host:2");
    let selected = Some(0);
    let copy = selected_chat_cell_text(&threads, selected);
    for width in [24, 100] {
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(width, 15)).unwrap();
        let mut cache = TurnLayoutCache::default();
        let mut projection = TurnProjection::default();
        let mut view = TranscriptView::default();
        let mut scroll = TranscriptScroll::default();
        scroll.scroll_up(1);
        scroll.new_activity = true;
        let mut baseline = None;
        for kind in [
            start("call", "Search"),
            finish("call"),
            start("call", "StaleSearch"),
        ] {
            record_correlated_metrics(&mut threads, &event(FOREGROUND_ID, None, kind));
            terminal
                .draw(|f| {
                    draw_conversation(
                        f,
                        f.area(),
                        &threads,
                        true,
                        "",
                        &mut scroll,
                        &mut cache,
                        &mut view,
                        selected,
                        None,
                        &mut projection,
                    )
                })
                .unwrap();
            let old_cell = &projection.cells[0];
            let old_revision = cell_revision(&threads[0], old_cell);
            cache.layout(cell_key(old_cell), old_revision, 0, || {
                panic!("history rebuilt")
            });
            let state = (
                terminal.backend().buffer().clone(),
                scroll.top,
                view.anchor_turn,
            );
            if let Some(baseline) = &baseline {
                assert_eq!(&state, baseline);
            } else {
                baseline = Some(state);
            }
            assert!(!scroll.follow);
            assert_eq!(selected_chat_cell_text(&threads, selected), copy);
            let builds = cache.builds;
            terminal
                .draw(|f| {
                    draw_conversation(
                        f,
                        f.area(),
                        &threads,
                        true,
                        "",
                        &mut scroll,
                        &mut cache,
                        &mut view,
                        selected,
                        None,
                        &mut projection,
                    )
                })
                .unwrap();
            assert_eq!(cache.builds, builds, "tick rebuilt layout");
        }
    }
}
