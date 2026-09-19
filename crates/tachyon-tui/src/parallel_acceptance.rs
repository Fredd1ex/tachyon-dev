use super::*;

#[test]
fn parallel_acceptance_ui_pipeline() {
    #[derive(serde::Deserialize)]
    struct Capture {
        scenario: String,
        events: Vec<InteractionEventEnvelope>,
        acknowledgement: AgentEvent,
    }
    let capture: Capture =
        serde_json::from_str(include_str!("../tests/fixtures/parallel_acceptance.json")).unwrap();
    assert_eq!(capture.scenario, "campaign-attention");
    let dir = tempfile::tempdir().unwrap();
    let mut attention = attention::State::open(dir.path()).unwrap();
    let mut threads = vec![Thread::new_foreground()];
    let main = session_archive::conversation_turn(FOREGROUND_ID, "1");
    let independent = session_archive::conversation_turn(FOREGROUND_ID, "2");
    let mut notice: Option<tachyon_api::HistoryEntry> = None;
    for mut event in capture.events {
        if let InteractionEvent::UserTurnAccepted { text } = &event.event {
            threads[0].add(ItemKind::User, text.clone());
            threads[0].reserve_reply();
        }
        if let Some(turn) = &event.metadata.turn_id {
            event.metadata.turn_id = Some(session_archive::conversation_turn(
                &event.metadata.conversation_id,
                turn,
            ));
        }
        if let Some(entry) = attention::publication(&event) {
            let duplicate = notice
                .as_ref()
                .is_some_and(|previous| previous.event_id == entry.event_id);
            assert_eq!(attention.receive(entry.clone(), &mut threads), !duplicate);
            assert!(!attention.receive(entry.clone(), &mut threads));
            notice = Some(entry);
        } else {
            apply_interaction_event(&mut threads[0], event.clone());
            if event.metadata.turn_id.as_deref() == Some(&main)
                && matches!(event.event, InteractionEvent::UserTurnAccepted { .. })
            {
                apply_correlated_agent_event(
                    &mut threads[0],
                    capture.acknowledgement.clone(),
                    Some(&main),
                );
                assert!(threads[0]
                    .items
                    .iter()
                    .any(|item| item.kind == ItemKind::PendingReply
                        && item.text.contains("I'm looking into it.")));
            }
        }
        if event.metadata.turn_id.as_deref() == Some(&independent)
            && matches!(event.event, InteractionEvent::ConversationFinished { .. })
        {
            assert!(threads[0].completed_turns.contains(&independent));
            assert!(!threads[0].completed_turns.contains(&main));
            assert!(threads[0]
                .items
                .iter()
                .any(|item| item.kind == ItemKind::PendingReply
                    && item.turn.as_deref() == Some(&main)));
        }
        if matches!(
            event.event,
            InteractionEvent::UserVisibleNotificationPublished { .. }
        ) {
            assert!(
                !threads[0].completed_turns.contains(&main),
                "overlay must not complete the stalled turn"
            );
        }
    }
    attention.save().unwrap();
    let mut reopened = attention::State::open(dir.path()).unwrap();
    let count = threads[0].items.len();
    assert!(!reopened.receive(notice.unwrap(), &mut threads));
    assert_eq!(threads[0].items.len(), count);
    assert_eq!(threads[0].completed_turns.len(), 8);
    let cells = build_turn_cells(&threads[0]);
    let copied = (0..cells.len())
        .map(|selected| {
            selected_chat_cell_text(&threads, Some(selected))
                .unwrap()
                .replace(&format!("{}:\n", names().user), "User:\n")
                .replace(&format!("{}:\n", names().conversation), "Assistant:\n")
        })
        .collect::<Vec<_>>()
        .join("\n\n---\n\n");
    assert_eq!(
        copied.trim(),
        include_str!("../tests/fixtures/parallel_acceptance.copy.txt").trim()
    );
    for item in &mut threads[0].items {
        item.timestamp = 0;
    }

    for width in [54, 120] {
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(width, 100)).unwrap();
        let mut cache = TurnLayoutCache::default();
        let mut projection = TurnProjection::default();
        let mut view = TranscriptView::default();
        let mut scroll = TranscriptScroll::default();
        let mut previous = None;
        for _ in 0..2 {
            terminal
                .draw(|frame| {
                    draw_conversation(
                        frame,
                        frame.area(),
                        &threads,
                        false,
                        "",
                        &mut scroll,
                        &mut cache,
                        &mut view,
                        None,
                        None,
                        &mut projection,
                    )
                })
                .unwrap();
            let buffer = terminal.backend().buffer().clone();
            if let Some(previous) = previous {
                assert_eq!(buffer, previous);
            }
            let screen = buffer
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>();
            if width == 120 {
                let rendered = buffer
                    .content
                    .chunks(width as usize)
                    .map(|row| {
                        row.iter()
                            .map(|cell| cell.symbol())
                            .collect::<String>()
                            .trim_end()
                            .to_owned()
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
                    .replace(icon::TURN, "^");
                assert_eq!(
                    rendered.trim_end(),
                    include_str!("../tests/fixtures/parallel_acceptance.screen.txt").trim_end()
                );
                if let Some(root) = std::env::var_os("PARALLEL_ACCEPTANCE_CAPTURE_DIR") {
                    std::fs::write(
                        std::path::PathBuf::from(root).join("parallel_acceptance.screen.txt"),
                        rendered.trim_end(),
                    )
                    .unwrap();
                }
            }
            for text in [
                "WorkerState",
                "Scheduled.",
                "pending",
                "in_progress",
                "blocked",
                "completed",
                "advisory",
                "Work needs your input",
                "Command verification",
            ] {
                assert!(screen.contains(text), "missing {text} at width {width}");
            }
            assert!(!screen.contains("conversation:foreground:"));
            previous = Some(buffer);
        }
    }
    // Exercise selection and key routing, without the system clipboard or its file fallback.
    let expected = selected_chat_cell_text(&threads, Some(2)).unwrap();
    let mut copied = Vec::new();
    assert!(handle_copy_key(
        event::KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
        MouseCapture::default(),
        false,
        "",
        &threads,
        Some(2),
        |text| {
            copied.push(text.to_owned());
            true
        }
    ));
    assert_eq!(copied, [expected]);
}
