use super::*;
use tachyon_api::types::Actor;

fn agent_info(lifetime_class: LifetimeClass) -> AgentInfo {
    AgentInfo {
        id: "worker".into(),
        task: "inspect".into(),
        state: AgentState::Running,
        pid: None,
        workspace: String::new(),
        created_secs: 0,
        retained: true,
        lease_until_secs: None,
        session_id: "worker".into(),
        lifetime_class,
        purpose: "research".into(),
        owner: "daemon".into(),
        last_activity_secs: 0,
        checkpoint_available: false,
        turns_used: 2,
        turn_budget: Some(3),
        task_type: "research".into(),
        description: "inspect".into(),
        persistent: lifetime_class == LifetimeClass::Persistent,
        sandboxed: false,
        stage_until_secs: None,
        logical_task_id: None,
        origin_turn_id: None,
        parent_task_id: None,
        tool_call_id: None,
    }
}

#[test]
fn agent_lifetime_describes_budget_daemon_and_manual_policies() {
    assert_eq!(
        agent_lifetime(&agent_info(LifetimeClass::Short)),
        ("short · retained".into(), "1 assignment".into())
    );
    assert_eq!(
        agent_lifetime(&agent_info(LifetimeClass::Long)).1,
        "daemon stop"
    );
    assert_eq!(
        agent_lifetime(&agent_info(LifetimeClass::Persistent)).1,
        "manual release"
    );
}

#[test]
fn terminal_agent_duration_stops_at_last_activity() {
    let mut info = agent_info(LifetimeClass::Short);
    info.created_secs = 100;
    info.last_activity_secs = 130;
    info.state = AgentState::Completed;

    assert_eq!(agent_duration(&info), "30s");
    assert_eq!(agent_pane_status(&info, false).0, "idle · retained");

    info.retained = false;
    info.state = AgentState::Terminated;
    assert_eq!(agent_duration(&info), "30s");
    assert_eq!(agent_pane_status(&info, false).0, "killed");
}

#[test]
fn agent_pane_orders_actionable_and_retained_workers_first() {
    let mut active = agent_info(LifetimeClass::Short);
    active.id = "active".into();
    active.created_secs = 10;
    let mut idle = agent_info(LifetimeClass::Short);
    idle.id = "idle".into();
    idle.state = AgentState::Completed;
    let mut failed = agent_info(LifetimeClass::Short);
    failed.id = "failed".into();
    failed.state = AgentState::Failed;
    failed.retained = false;
    let infos = HashMap::from([
        (failed.id.clone(), failed),
        (idle.id.clone(), idle),
        (active.id.clone(), active),
    ]);

    assert_eq!(pane_agent_ids(&infos), ["active", "idle", "failed"]);
}

#[test]
fn window_logo_button_has_balanced_internal_padding() {
    assert_eq!(WINDOW_LOGO_BUTTON, " 󰘵 ");
    assert_eq!(WINDOW_LOGO_BUTTON.chars().count(), 3);
}

#[test]
fn legacy_pid_prefix_is_display_only_and_never_a_live_turn() {
    let identity = "archived:42:2";
    assert_eq!(session_archive::display_turn(identity), "2");
    assert_eq!(live_turn_number(identity), None);
    assert!(!later_turn(identity, "1"));
    assert_eq!(identity, "archived:42:2");
}

#[test]
fn scheduled_tab_columns_show_durable_task_state() {
    assert_eq!(PaneTab::Foreground.adjacent(true), PaneTab::Agents);
    let columns = scheduled_task_columns(&ScheduledTaskInfo {
        id: "scheduled-task-1".into(),
        conversation_id: "foreground".into(),
        turn: 7,
        objective: "inspect release artifacts".into(),
        mode: ScheduledTaskMode::StartAt,
        created_at_ms: now_seconds(),
        due_at_ms: now_seconds().saturating_add(60_000),
        status: ScheduledTaskStatus::Pending,
        work_id: None,
    });
    assert_eq!(columns[0], "schedule t7");
    assert_eq!(columns[1], "pending");
    assert_eq!(columns[2], "start at");
    assert!(columns[3].starts_with("in "));
    assert_eq!(columns[5], "inspect release artifacts");
}

#[test]
fn task_count_excludes_foreground() {
    let mut foreground = agent_info(LifetimeClass::Long);
    foreground.id = FOREGROUND_ID.into();
    foreground.state = AgentState::Running;
    let mut waiting = agent_info(LifetimeClass::Short);
    waiting.id = "worker-idle".into();
    waiting.state = AgentState::Waiting;
    let infos = HashMap::from([
        (foreground.id.clone(), foreground),
        (waiting.id.clone(), waiting),
    ]);

    assert_eq!(status_task_count(&infos), 1);
}

#[test]
fn status_hides_zero_tasks_and_pluralizes_active_tasks() {
    assert_eq!(status_task_label(0), None);
    assert_eq!(status_task_label(1).as_deref(), Some("1 task"));
    assert_eq!(status_task_label(2).as_deref(), Some("2 tasks"));
}

#[test]
fn agent_counts_use_singular_and_plural_labels() {
    assert_eq!(agent_count(1), "1 agent");
    assert_eq!(agent_count(3), "3 agents");
}

#[test]
fn turn_badges_do_not_count_more_outcomes_than_started_workers() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("3".into()));
    thread.add_turn(
        ItemKind::Spawn,
        "worker fresh: lookup".into(),
        Some("3".into()),
    );
    for worker in ["old-one", "old-two", "fresh"] {
        thread.add_turn(
            ItemKind::SpawnResult,
            format!("worker {worker}: result"),
            Some("3".into()),
        );
    }
    let cell = build_turn_cells(&thread).pop().expect("turn cell");

    let badges = turn_cell_badges(&thread, &cell);
    assert!(badges.contains("󰚩 1 agent"), "{badges}");
    assert!(badges.contains("󰄬 1 complete"), "{badges}");
    assert!(!badges.contains("3 complete"), "{badges}");
}

#[test]
fn released_lifecycle_uses_success_marker() {
    assert_eq!(lifecycle_badge(AgentState::Released), "✓ released");
}

#[test]
fn pending_reply_uses_activity_with_safe_fallback() {
    assert_eq!(
        pending_reply_activity("using search", true),
        "using search..."
    );
    assert_eq!(pending_reply_activity("", true), "Checking information...");
    assert_eq!(pending_reply_activity("   ", false), "Submitting...");
}

#[test]
fn main_conversation_is_always_expanded_in_turn_cells() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "first question".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "first answer".into(), Some("2".into()));
    thread.add_turn(ItemKind::User, "second question".into(), Some("3".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));
    let cells = build_turn_cells(&thread);
    let latest = latest_conversation_timestamp(&thread, &cells[1]);
    for cell in &cells {
        let layout = turn_cell_layout(
            0,
            0,
            std::slice::from_ref(&thread),
            cell,
            80,
            latest,
            false,
            "working",
            false,
            None,
        );
        let text = layout
            .lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains(&names().user));
        assert!(text.contains(&names().conversation));
        assert!(!text.contains("> first question"));
        assert!(!text.contains("trace ·"));
    }
}

#[test]
fn mouse_defaults_clear_reporting_and_toggle_roundtrips_without_a_terminal() {
    let mut mode = MouseCapture::default();
    let mut disabled = Vec::new();
    disabled.execute(DisableMouseCapture).unwrap();
    let mut enabled = Vec::new();
    enabled.execute(EnableMouseCapture).unwrap();
    let mut output = Vec::new();
    mode.apply(&mut output).unwrap();
    assert_eq!(output, disabled);
    assert_ne!(output, enabled);
    for (expected, bytes) in [(true, &enabled), (false, &disabled)] {
        output.clear();
        let notice = mode.toggle(|next| next.apply(&mut output));
        assert_eq!(mode.0, expected);
        assert_eq!(&output, bytes);
        assert_eq!(notice, mode.label());
    }
    for initial in [false, true] {
        mode = MouseCapture(initial);
        let notice = mode.toggle(|next| {
            assert_eq!(next.0, !initial);
            Err(io::Error::other("injected terminal failure"))
        });
        assert_eq!(mode, MouseCapture(initial));
        assert!(notice.contains("Mouse toggle failed: injected terminal failure"));
        assert!(notice.contains("partial; retry /mouse"));
    }
}

#[test]
fn mouse_help_shows_current_mode_and_toggle() {
    let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 45)).unwrap();
    for mode in [MouseCapture::default(), MouseCapture(true)] {
        terminal
            .draw(|f| draw_command_palette(f, f.area(), mode, &mut 0))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(text.contains(mode.label()));
        assert!(text.contains("toggle native selection / clickable capture"));
        assert!(text.contains("terminal-owned by default"));
        assert!(text.contains("quit Tachyon"));
    }
}

#[test]
fn native_forwarded_copy_is_consumed_without_clipboard_or_quit() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(
        ItemKind::Reply,
        "do not copy this cell".into(),
        Some("2".into()),
    );
    for code in ['c', 'C'] {
        for kind in [
            event::KeyEventKind::Press,
            event::KeyEventKind::Repeat,
            event::KeyEventKind::Release,
        ] {
            for pane in [false, true] {
                for input in ["", "draft"] {
                    assert!(handle_copy_key(
                        event::KeyEvent::new_with_kind(
                            KeyCode::Char(code),
                            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
                            kind
                        ),
                        MouseCapture::default(),
                        pane,
                        input,
                        std::slice::from_ref(&thread),
                        Some(0),
                        |_| panic!("native Copy must not enqueue cell text"),
                    ));
                }
            }
        }
    }
}

pub(super) fn two_turn_fixture() -> Vec<InteractionEventEnvelope> {
    let wire: serde_json::Value =
        serde_json::from_str(include_str!("../../two_turn_fixture.json")).unwrap();
    wire.as_array()
        .unwrap()
        .iter()
        .map(|value| {
            let event = decode_interaction_event(&value.to_string()).unwrap();
            assert_eq!(serde_json::to_value(&event).unwrap(), *value);
            event
        })
        .collect()
}

#[test]
fn operational_ticks_preserve_transcript_layout_selection_scroll_and_copy() {
    let mut threads = vec![Thread::new_foreground()];
    for event in two_turn_fixture() {
        apply_interaction_event(&mut threads[0], event);
    }
    let selected = Some(0);
    let expected_copy = selected_chat_cell_text(&threads, selected);
    let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 60)).unwrap();
    let mut cache = TurnLayoutCache::default();
    let mut projection = TurnProjection::default();
    let mut view = TranscriptView::default();
    let mut scroll = TranscriptScroll::default();
    scroll.scroll_up(1);
    let mut baseline = None;
    for tick in 0..4 {
        let operational = daemon_state_cache::View {
            rows: format!("Host sample {tick}"),
            stale: tick == 3,
            ..Default::default()
        };
        terminal
            .draw(|f| {
                draw_conversation(
                    f,
                    f.area(),
                    &threads,
                    false,
                    "",
                    &mut scroll,
                    &mut cache,
                    &mut view,
                    selected,
                    None,
                    &mut projection,
                );
                // Separate pane rendering cannot borrow or mutate transcript state.
                if tick > 0 {
                    let mut pane =
                        Terminal::new(ratatui::backend::TestBackend::new(72, 18)).unwrap();
                    pane.draw(|p| {
                        draw_agent_pane(
                            p,
                            p.area(),
                            &threads,
                            1,
                            None,
                            None,
                            &HashMap::new(),
                            &[],
                            PaneTab::Resources,
                            &operational,
                            0,
                        )
                    })
                    .unwrap();
                }
            })
            .unwrap();
        let actual = (
            terminal.backend().buffer().clone(),
            cache.builds,
            scroll.clone(),
        );
        if let Some(expected) = &baseline {
            assert_eq!(&actual, expected);
        } else {
            baseline = Some(actual);
        }
        assert_eq!(selected_chat_cell_text(&threads, selected), expected_copy);
    }
}

#[test]
fn operational_views_render_cached_unknown_and_stale_without_io() {
    for width in [28, 72] {
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(width, 18)).unwrap();
        for tab in [PaneTab::Todos, PaneTab::Resources] {
            for stale in [false, true] {
                let view = daemon_state_cache::View {
                    stale,
                    ..Default::default()
                };
                let mut previous = None;
                for _ in 0..2 {
                    terminal
                        .draw(|f| {
                            draw_agent_pane(
                                f,
                                f.area(),
                                &[],
                                0,
                                None,
                                None,
                                &HashMap::new(),
                                &[],
                                tab,
                                &view,
                                0,
                            )
                        })
                        .unwrap();
                    let buffer = terminal.backend().buffer().clone();
                    let rendered: String =
                        buffer.content.iter().map(|cell| cell.symbol()).collect();
                    assert!(rendered.contains("TODO"));
                    assert!(rendered.contains("RESOURCES"));
                    if stale {
                        assert!(rendered.contains("STALE"));
                    }
                    if let Some(previous) = &previous {
                        assert_eq!(&buffer, previous);
                    }
                    previous = Some(buffer);
                }
            }
        }
    }
}

#[test]
fn two_turn_protocol_render_copy_and_cache_snapshots() {
    let events = two_turn_fixture();
    let mut threads = vec![Thread::new_foreground()];
    for prompt in ["  first α\n", "second 界\r\n"] {
        threads[0].add(ItemKind::User, prompt.into());
        threads[0].reserve_reply();
    }
    let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 60)).unwrap();
    let mut cache = TurnLayoutCache::default();
    let mut projection = TurnProjection::default();
    let mut view = TranscriptView::default();
    let mut scroll = TranscriptScroll::default();
    let mut replies = [None, None];
    let mut identities = HashSet::new();
    for (step, event) in events.iter().enumerate() {
        assert!(identities.insert(event.metadata.message_id.clone()));
        let before = cache.builds;
        apply_interaction_event(&mut threads[0], event.clone());
        match &event.event {
            InteractionEvent::ConversationDelta { text } if step != 6 => {
                replies[usize::from(event.metadata.turn_id.as_deref() == Some("3"))] =
                    Some(text.as_str());
            }
            InteractionEvent::ConversationFinished { text } => {
                replies[usize::from(event.metadata.turn_id.as_deref() == Some("3"))] =
                    Some(text.as_str());
            }
            _ => {}
        }
        if step == 4 {
            // Turn 3 can finish while turn 2 remains pending, with no foreground events.
            assert!(threads[0].completed_turns.contains("3"));
            assert!(!threads[0].completed_turns.contains("2"));
            assert_eq!(
                session_snapshot(&threads)[0].items[1].text,
                "Checking α\n\n"
            );
        }
        // Stabilize display timestamps, not protocol identities or payloads.
        for (index, item) in threads[0].items.iter_mut().enumerate() {
            item.timestamp = 100 + index as u64;
        }
        for redraw in 0..3 {
            let builds = cache.builds;
            let screen = terminal.backend().buffer().clone();
            terminal
                .draw(|f| {
                    draw_conversation(
                        f,
                        f.area(),
                        &threads,
                        false,
                        "",
                        &mut scroll,
                        &mut cache,
                        &mut view,
                        None,
                        None,
                        &mut projection,
                    );
                })
                .unwrap();
            if redraw > 0 {
                assert_eq!(cache.builds, builds, "unchanged redraw at {step}");
                assert_eq!(terminal.backend().buffer(), &screen);
            }
            for (selected, prompt) in ["  first α\n", "second 界\r\n"].iter().enumerate() {
                let mut expected = format!("{}:\n{prompt}", names().user);
                if let Some(reply) = replies[selected] {
                    expected.push_str(&format!("\n\n{}:\n{reply}", names().conversation));
                }
                assert_eq!(
                    selected_chat_cell_text(&threads, Some(selected)),
                    Some(expected.clone())
                );
                let mut copied = Vec::new();
                assert!(handle_copy_key(
                    event::KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
                    MouseCapture::default(),
                    false,
                    "",
                    &threads,
                    Some(selected),
                    |text| {
                        copied.push(text.to_owned());
                        true
                    },
                ));
                assert_eq!(copied, [expected]);
            }
        }
        if (2..=7).contains(&step) {
            assert_eq!(
                cache.builds - before,
                usize::from(step != 6),
                "event {step}"
            );
        }
    }
    let builds = cache.builds;
    let screen = terminal.backend().buffer().clone();
    let rendered: String = screen.content.iter().map(|cell| cell.symbol()).collect();
    assert!(rendered.contains("Corrected α"));
    assert!(rendered.contains("ready();"));
    assert!(rendered.contains("UNRELATED NOTIFICATION"));
    assert!(!rendered.contains("LATE MUST NOT APPEND"));
    assert!(!rendered.contains("Checking α"));
    for id in 1..=3 {
        let mut metric = envelope(
            id,
            AgentEvent::MemoryRecalled {
                turn: Some(99),
                preference_count: id as u32,
                history_count: 0,
            },
        );
        metric.turn_id = Some("99".into());
        record_correlated_metrics(&mut threads, &metric);
        assert!(threads[0].metric_revisions.contains_key("99"));
        terminal
            .draw(|f| {
                draw_conversation(
                    f,
                    f.area(),
                    &threads,
                    false,
                    "",
                    &mut scroll,
                    &mut cache,
                    &mut view,
                    None,
                    None,
                    &mut projection,
                );
            })
            .unwrap();
        assert_eq!(cache.builds, builds);
        assert_eq!(terminal.backend().buffer(), &screen);
    }
    apply_correlated_agent_event(
        &mut threads[0],
        AgentEvent::WorkResult {
            result: evidence_result(1),
        },
        Some("2"),
    );
    for item in &mut threads[0].items {
        if let Some(work) = &mut item.work {
            item.hidden = false;
            work.raw_open = true;
        }
    }
    let raw = layout_text(&evidence_layout(&threads, true, Some("work-1")));
    assert!(raw.contains("secret-code"));
    assert!(raw.contains("secret-output"));
    let expected = format!(
        "{}:\n  first α\n\n\n{}:\n{}",
        names().user,
        names().conversation,
        replies[0].unwrap()
    );
    assert_eq!(
        selected_chat_cell_text(&threads, Some(0)),
        Some(expected.clone())
    );
    assert!(handle_copy_key(
        event::KeyEvent::new(
            KeyCode::Char('C'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT
        ),
        MouseCapture(true),
        true,
        "draft",
        &threads,
        Some(0),
        |text| {
            assert_eq!(text, expected);
            assert_eq!(
                copy_to_clipboard_with(
                    text,
                    |_, bytes| {
                        assert_eq!(bytes, expected);
                        true
                    },
                    |_| panic!("fake clipboard succeeded")
                ),
                CopyOutcome::SystemClipboard
            );
            true
        },
    ));
}

#[test]
fn copy_handler_preserves_payload_across_streaming_and_completion() {
    let mut threads = vec![Thread::new_foreground()];
    threads[0].add_turn(ItemKind::User, "  question\n".into(), Some("2".into()));
    threads[0].add_turn(
        ItemKind::PendingReply,
        "private status".into(),
        Some("2".into()),
    );
    let key = event::KeyEvent::new(
        KeyCode::Char('C'),
        KeyModifiers::CONTROL | KeyModifiers::SHIFT,
    );
    let check = |threads: &[Thread], reply: Option<&str>| {
        let mut copied = None;
        assert!(handle_copy_key(
            key,
            MouseCapture(true),
            false,
            "",
            threads,
            Some(0),
            |text| {
                copied = Some(text.to_owned());
                true
            }
        ));
        let mut expected = format!("{}:\n  question\n", names().user);
        if let Some(reply) = reply {
            expected.push_str(&format!("\n\n{}:\n{reply}", names().conversation));
        }
        assert_eq!(copied, Some(expected));
    };
    check(&threads, None);
    let body = "  markdown **bold** α\n```rust\n    code();  \n```\n\n";
    let mut seen = HashSet::new();
    for (id, text) in [(1, body), (2, "<dsml tool_calls>private evidence")] {
        let event = envelope(
            id,
            AgentEvent::ReplyDelta {
                turn: Some(2),
                text: text.into(),
            },
        );
        for _ in 0..2 {
            if accept_event(&mut seen, &event) {
                apply_agent_event(&mut threads[0], event.kind.clone());
            }
        }
        check(&threads, Some(if id == 1 { body } else { body.trim_end() }));
    }
    for _ in 0..2 {
        apply_interaction_event(
            &mut threads[0],
            interaction(InteractionEvent::ConversationFinished { text: body.into() }),
        );
        check(&threads, Some(body));
    }
    apply_agent_event(
        &mut threads[0],
        AgentEvent::ReplyDelta {
            turn: Some(2),
            text: "late duplicate".into(),
        },
    );
    check(&threads, Some(body));
}

#[test]
fn copy_ignores_expanded_raw_evidence_and_unrelated_worker_focus() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.finish_reply("```\r\n  α  \r\n```\r\n".into(), Some("2".into()));
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::WorkResult {
            result: evidence_result(1),
        },
        Some("2"),
    );
    let mut worker = Thread::new_foreground();
    worker.is_foreground = false;
    worker.id = "unrelated-worker".into();
    worker.add_turn(
        ItemKind::Reply,
        "private worker answer".into(),
        Some("2".into()),
    );
    let mut threads = vec![worker, thread];
    for expanded in [false, true] {
        for item in &mut threads[1].items {
            item.hidden = !expanded;
            if let Some(work) = &mut item.work {
                work.raw_open = expanded;
            }
        }
        let cells = build_turn_cells(&threads[1]);
        let _ = turn_cell_layout(
            1,
            0,
            &threads,
            &cells[0],
            80,
            0,
            false,
            "",
            expanded,
            Some("work-1"),
        );
        let mut copied = None;
        assert!(handle_copy_key(
            event::KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
            MouseCapture::default(),
            false,
            "",
            &threads,
            Some(0),
            |text| {
                copied = Some(text.to_owned());
                true
            }
        ));
        assert_eq!(
            copied,
            Some(format!(
                "{}:\nquestion\n\n{}:\n```\r\n  α  \r\n```\r\n",
                names().user,
                names().conversation
            ))
        );
    }
}

#[test]
fn copy_key_matrix_and_empty_selection_never_fall_back_to_other_text() {
    let mut threads = vec![Thread::new_foreground()];
    threads[0].add_turn(ItemKind::Reply, "standalone\n".into(), Some("2".into()));
    threads[0].add_turn(ItemKind::User, String::new(), Some("3".into()));
    threads[0].add_turn(ItemKind::PendingReply, "status".into(), Some("3".into()));
    for (code, modifiers, pane, input, selected, handled, payload) in [
        (
            'c',
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
            true,
            "draft",
            Some(0),
            true,
            true,
        ),
        (
            'C',
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
            false,
            "",
            Some(0),
            true,
            true,
        ),
        ('c', KeyModifiers::CONTROL, false, "", Some(0), false, false),
        ('y', KeyModifiers::NONE, false, "", Some(0), true, true),
        ('y', KeyModifiers::NONE, true, "", Some(0), false, false),
        (
            'y',
            KeyModifiers::NONE,
            false,
            "draft",
            Some(0),
            false,
            false,
        ),
        ('y', KeyModifiers::CONTROL, false, "", Some(0), false, false),
        ('y', KeyModifiers::NONE, false, "", None, true, false),
        (
            'C',
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
            false,
            "",
            None,
            true,
            false,
        ),
        ('y', KeyModifiers::NONE, false, "", Some(1), true, false),
        ('y', KeyModifiers::NONE, false, "", Some(99), true, false),
    ] {
        let mut copied = None;
        assert_eq!(
            handle_copy_key(
                event::KeyEvent::new(KeyCode::Char(code), modifiers),
                MouseCapture(true),
                pane,
                input,
                &threads,
                selected,
                |text| {
                    copied = Some(text.to_owned());
                    false
                }
            ),
            handled
        );
        assert_eq!(
            copied,
            payload.then(|| format!("{}:\nstandalone\n", names().conversation))
        );
    }
    let release = event::KeyEvent::new_with_kind(
        KeyCode::Char('C'),
        KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        event::KeyEventKind::Release,
    );
    assert!(handle_copy_key(
        release,
        MouseCapture(true),
        false,
        "",
        &threads,
        Some(0),
        |_| panic!("release copied")
    ));
    assert!(selected_chat_cell_text(&[], None).is_none());
}

#[test]
fn copy_key_case_kind_and_modifier_matrix() {
    let mut threads = vec![Thread::new_foreground()];
    threads[0].finish_reply("  **α**\n\n".into(), Some("2".into()));
    let expected = format!("{}:\n  **α**\n\n", names().conversation);
    let before = serde_json::to_value(session_snapshot(&threads)).unwrap();
    for capture in [MouseCapture::default(), MouseCapture(true)] {
        for kind in [
            event::KeyEventKind::Press,
            event::KeyEventKind::Repeat,
            event::KeyEventKind::Release,
        ] {
            for (code, modifiers, shortcut, yank) in [
                ('y', KeyModifiers::NONE, false, true),
                ('Y', KeyModifiers::NONE, false, false),
                ('Y', KeyModifiers::SHIFT, false, false),
                ('y', KeyModifiers::SHIFT, false, false),
                ('y', KeyModifiers::CONTROL, false, false),
                ('c', KeyModifiers::CONTROL, false, false),
                ('C', KeyModifiers::SHIFT, false, false),
                (
                    'c',
                    KeyModifiers::CONTROL | KeyModifiers::SHIFT,
                    true,
                    false,
                ),
                (
                    'C',
                    KeyModifiers::CONTROL | KeyModifiers::SHIFT,
                    true,
                    false,
                ),
                (
                    'C',
                    KeyModifiers::CONTROL | KeyModifiers::SHIFT | KeyModifiers::ALT,
                    true,
                    false,
                ),
            ] {
                for pane in [false, true] {
                    for input in ["", "draft"] {
                        let mut copied = Vec::new();
                        let handled = shortcut || (yank && !pane && input.is_empty());
                        assert_eq!(
                            handle_copy_key(
                                event::KeyEvent::new_with_kind(
                                    KeyCode::Char(code),
                                    modifiers,
                                    kind
                                ),
                                capture,
                                pane,
                                input,
                                &threads,
                                Some(0),
                                |text| {
                                    copied.push(text.to_owned());
                                    true
                                },
                            ),
                            handled,
                            "{code} {modifiers:?} {kind:?} {capture:?} {pane} {input}"
                        );
                        let copies = handled
                            && (!shortcut || capture.0)
                            && kind != event::KeyEventKind::Release;
                        assert_eq!(
                            copied,
                            if copies {
                                vec![expected.clone()]
                            } else {
                                vec![]
                            }
                        );
                        assert_eq!(
                            serde_json::to_value(session_snapshot(&threads)).unwrap(),
                            before
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn clipboard_queue_is_nonblocking_bounded_and_drains_snapshots_on_close() {
    let (notifications, completed) = mpsc::channel();
    let (delivered, deliveries) = mpsc::channel();
    let (started, ready) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let clipboard = clipboard_worker(
        move |text| {
            delivered.send(text.to_owned()).unwrap();
            let _ = started.send(());
            blocked.recv_timeout(Duration::from_secs(5)).unwrap();
            // Failure must not prevent the next queued request from being handled.
            CopyOutcome::Failed
        },
        notifications,
    )
    .unwrap();
    let (returned, result) = mpsc::channel();
    let ui = std::thread::spawn(move || {
        let mut threads = vec![Thread::new_foreground()];
        threads[0].add_turn(ItemKind::User, "question".into(), Some("2".into()));
        threads[0].add_reply_fragment("  first α\r\n".into(), Some("2".into()), false);
        let key = event::KeyEvent::new(
            KeyCode::Char('C'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        );
        let enqueue = |threads: &[Thread]| {
            let mut accepted = false;
            assert!(handle_copy_key(
                key,
                MouseCapture(true),
                false,
                "",
                threads,
                Some(0),
                |text| {
                    accepted = clipboard.try_send(text.to_owned()).is_ok();
                    accepted
                }
            ));
            accepted
        };
        assert!(enqueue(&threads));
        ready.recv_timeout(Duration::from_secs(2)).unwrap();
        threads[0].add_reply_fragment("second\n".into(), Some("2".into()), false);
        assert!(enqueue(&threads));
        threads[0].add_reply_fragment("not queued".into(), Some("2".into()), false);
        for _ in 0..100 {
            assert!(!enqueue(&threads));
        }
        // Session shutdown must return even while delivery is still blocked.
        drop(clipboard);
        returned.send(()).unwrap();
    });
    result
        .recv_timeout(Duration::from_secs(2))
        .expect("enqueue or session close blocked on delivery");
    ui.join().unwrap();
    let prefix = format!("{}:\nquestion\n\n{}:\n", names().user, names().conversation);
    assert_eq!(
        deliveries.recv_timeout(Duration::from_secs(2)).unwrap(),
        format!("{prefix}  first α\r\n")
    );
    assert!(matches!(
        deliveries.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    assert!(matches!(
        completed.recv_timeout(Duration::from_secs(2)).unwrap(),
        TuiEvent::Clipboard(CopyOutcome::Failed)
    ));
    assert_eq!(
        deliveries.recv_timeout(Duration::from_secs(2)).unwrap(),
        format!("{prefix}  first α\r\nsecond\n")
    );
    release.send(()).unwrap();
    assert!(matches!(
        completed.recv_timeout(Duration::from_secs(2)).unwrap(),
        TuiEvent::Clipboard(CopyOutcome::Failed)
    ));
    assert_eq!(
        deliveries.recv_timeout(Duration::from_secs(2)),
        Err(mpsc::RecvTimeoutError::Disconnected)
    );
}

#[test]
fn clipboard_completion_reports_system_file_and_failure_distinctly() {
    let (notifications, completed) = mpsc::channel();
    let clipboard = clipboard_worker(
        |text| {
            copy_to_clipboard_with(
                text,
                |_, _| text == "system",
                |_| (text == "file").then(|| "/tmp/fake-clipboard.txt".into()),
            )
        },
        notifications,
    )
    .unwrap();
    for (payload, notice) in [
        ("system", "System clipboard copied"),
        (
            "file",
            "Saved to /tmp/fake-clipboard.txt (system clipboard unavailable)",
        ),
        (
            "failure",
            "Copy failed: clipboard helpers and file fallback unavailable",
        ),
    ] {
        clipboard.try_send(payload.into()).unwrap();
        let TuiEvent::Clipboard(outcome) = completed.recv_timeout(Duration::from_secs(2)).unwrap()
        else {
            panic!("expected clipboard completion");
        };
        assert_eq!(outcome.notice(), notice);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn clipboard_forked_owner_survives_success_but_not_failure_or_timeout() {
    struct Owner(nix::unistd::Pid);
    impl Drop for Owner {
        fn drop(&mut self) {
            let _ = nix::sys::signal::kill(self.0, nix::sys::signal::Signal::SIGKILL);
        }
    }
    for (ending, success) in [
        ("exit 0", true),
        ("exit 1", false),
        ("exec sleep 60", false),
    ] {
        let path = std::env::temp_dir().join(format!(
            "tachyon-fake-copy-owner-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // Mimic wl-copy's default fork-to-serve lifetime, without a display
        // connection or any clipboard access. The owner stays in the group.
        let script = format!("cat >/dev/null; sleep 60 & printf '%s' $! > \"$1\"; {ending}");
        let result = clipboard_command(
            &["sh", "-c", &script, "fake-copy", path.to_str().unwrap()],
            "synthetic payload",
        );
        let pid = std::fs::read_to_string(&path)
            .unwrap()
            .parse::<i32>()
            .unwrap();
        std::fs::remove_file(path).unwrap();
        let owner = Owner(nix::unistd::Pid::from_raw(pid));
        assert_eq!(result, success);
        let alive = || {
            std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .is_some_and(|stat| !stat.split_once(") ").unwrap().1.starts_with(['Z', 'X']))
        };
        if success {
            std::thread::sleep(Duration::from_millis(100));
            assert!(alive(), "successful helper's clipboard owner was killed");
        } else {
            let deadline = Instant::now() + Duration::from_secs(2);
            while alive() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(!alive(), "failed helper leaked its owner");
        }
        drop(owner);
    }
}

#[test]
fn yank_without_selection_copies_latest_cell() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::Reply, "latest answer".into(), Some("2".into()));
    let mut copied = None;
    assert!(handle_copy_key(
        event::KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
        MouseCapture::default(),
        false,
        "",
        &[thread],
        None,
        |text| {
            copied = Some(text.to_owned());
            true
        }
    ));
    assert_eq!(
        copied,
        Some(format!("{}:\nlatest answer", names().conversation))
    );
}

#[test]
fn clipboard_fallback_order_and_exact_bytes_without_system_clipboard() {
    let text = "  α\r\n```\n  code  \n```\n";
    let commands = [
        vec!["wl-copy"],
        vec!["xclip", "-selection", "clipboard"],
        vec!["xsel", "-b"],
    ];
    for success in 0..=4 {
        let mut calls = Vec::new();
        let mut fallback_called = false;
        let result = copy_to_clipboard_with(
            text,
            |cmd, payload| {
                assert_eq!(payload, text);
                calls.push(cmd.iter().map(|s| s.to_string()).collect::<Vec<_>>());
                calls.len() - 1 == success
            },
            |payload| {
                assert_eq!(payload, text);
                fallback_called = true;
                (success == 3).then(|| "clipboard.txt".into())
            },
        );
        assert_eq!(
            result,
            match success {
                0..=2 => CopyOutcome::SystemClipboard,
                3 => CopyOutcome::Saved("clipboard.txt".into()),
                _ => CopyOutcome::Failed,
            }
        );
        assert_eq!(calls, commands[..(success + 1).min(3)]);
        assert_eq!(fallback_called, success >= 3);
    }
    // Only harmless child processes: verify stdin bytes, EOF, spawn and exit failures.
    assert!(clipboard_command(
        &[
            "sh",
            "-c",
            "test \"$(od -An -tx1 | tr -d ' \\n')\" = '2020610d0a620a'"
        ],
        "  a\r\nb\n"
    ));
    assert!(!clipboard_command(
        &["/nonexistent/tachyon-copy-test"],
        text
    ));
    assert!(!clipboard_command(&["sh", "-c", "exit 1"], text));
    assert!(!clipboard_command(
        &["sh", "-c", "exec 0<&-; exit 0"],
        &"x".repeat(1024 * 1024)
    ));
}

#[test]
fn clipboard_stalled_reader_and_exit_wait_reach_fallback() {
    for (script, text) in [
        ("exec sleep 60", "x".repeat(1024 * 1024)),
        ("cat >/dev/null; exec sleep 60", "small".into()),
    ] {
        let started = std::time::Instant::now();
        let mut attempts = 0;
        let mut fallback_called = false;
        assert_eq!(
            copy_to_clipboard_with(
                &text,
                |_, payload| {
                    attempts += 1;
                    // Exercise one real blocked helper, not any installed clipboard.
                    attempts == 1 && clipboard_command(&["sh", "-c", script], payload)
                },
                |payload| {
                    assert_eq!(payload, text);
                    fallback_called = true;
                    Some("clipboard.txt".into())
                },
            ),
            CopyOutcome::Saved("clipboard.txt".into())
        );
        assert_eq!(attempts, 3);
        assert!(fallback_called);
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
    }
}

#[test]
fn selected_chat_cell_copy_includes_user_and_reply() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "first question".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "first answer".into(), Some("2".into()));
    thread.add_turn(ItemKind::User, "second question".into(), Some("3".into()));
    thread.add_turn(ItemKind::Reply, "second answer".into(), Some("3".into()));
    let threads = vec![thread];

    let expected = format!(
        "{}:\nfirst question\n\n{}:\nfirst answer",
        names().user,
        names().conversation
    );
    assert_eq!(
        selected_chat_cell_text(&threads, Some(0)).as_deref(),
        Some(expected.as_str())
    );
    assert!(selected_chat_cell_text(&threads, None)
        .expect("latest chat cell")
        .contains("second answer"));
}

#[test]
fn page_navigation_scrolls_one_viewport_and_end_follows_latest() {
    let mut scroll = TranscriptScroll::default();
    scroll.sync(100, 20, 1);
    assert_eq!(scroll.top, 80);
    scroll.scroll_up(20);
    assert_eq!(scroll.top, 60);
    assert!(!scroll.follow);
    scroll.scroll_down(20, 100, 20);
    assert_eq!(scroll.top, 80);
    assert!(scroll.follow);
    scroll.scroll_up(1);
    scroll.end();
    scroll.sync(120, 20, 2);
    assert_eq!(scroll.top, 100);
}

#[test]
fn arrow_selection_opens_one_turn_and_collapses_the_previous_one() {
    let view = TranscriptView {
        attention_hits: Vec::new(),
        total_height: 30,
        viewport: 10,
        turns: 3,
        anchor_turn: Some(2),
        starts: vec![0, 10, 20],
        heights: vec![10, 10, 10],
    };
    let mut scroll = TranscriptScroll::default();
    scroll.sync(30, 10, 1);
    let mut open = None;

    select_trace_turn(&mut open, &view, &mut scroll, -1);
    assert_eq!(open, Some(2));
    assert_eq!(scroll.top, 20);
    assert!(!scroll.follow);

    select_trace_turn(&mut open, &view, &mut scroll, -1);
    assert_eq!(open, Some(1));
    assert_eq!(scroll.top, 10);

    select_trace_turn(&mut open, &view, &mut scroll, 1);
    assert_eq!(open, Some(2));
    select_trace_turn(&mut open, &view, &mut scroll, 1);
    assert_eq!(open, None);
    assert!(scroll.follow);
}

#[test]
fn page_keys_scroll_inside_a_tall_selected_trace_before_moving_turns() {
    let view = TranscriptView {
        attention_hits: Vec::new(),
        total_height: 50,
        viewport: 10,
        turns: 2,
        anchor_turn: Some(0),
        starts: vec![0, 40],
        heights: vec![40, 10],
    };
    let mut scroll = TranscriptScroll {
        top: 0,
        follow: false,
        ..TranscriptScroll::default()
    };
    let mut open = Some(0);

    page_trace_turn(&mut open, &view, &mut scroll, 1);
    assert_eq!(open, Some(0));
    assert_eq!(scroll.top, 10);
    page_trace_turn(&mut open, &view, &mut scroll, 1);
    assert_eq!(scroll.top, 20);
    page_trace_turn(&mut open, &view, &mut scroll, 1);
    assert_eq!(scroll.top, 30);
    page_trace_turn(&mut open, &view, &mut scroll, 1);
    assert_eq!(open, Some(1));
    assert_eq!(scroll.top, 40);
}

#[test]
fn detached_height_growth_preserves_top_anchor() {
    let mut scroll = TranscriptScroll::default();
    scroll.sync(100, 20, 10);
    scroll.scroll_up(30);
    assert_eq!(scroll.top, 50);
    scroll.sync(140, 19, 11);
    assert_eq!(scroll.top, 50);
    assert!(scroll.new_activity);
}

#[test]
fn turn_response_prefers_reply_over_stale_pending() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
    thread.add_turn(
        ItemKind::PendingReply,
        "stale pending".into(),
        Some("2".into()),
    );
    let cells = build_turn_cells(&thread);
    let response = turn_response(&thread, &cells[0]).expect("response");
    assert_eq!(response.kind, ItemKind::Reply);
    assert_eq!(response.text, "answer");
}

#[test]
fn turn_grouping_maps_out_of_order_correlated_items_in_two_passes() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "first".into(), Some("2".into()));
    thread.add_turn(ItemKind::User, "second".into(), Some("3".into()));
    thread.add_turn(
        ItemKind::SpawnResult,
        "late first result".into(),
        Some("2".into()),
    );
    thread.add_turn(
        ItemKind::SpawnResult,
        "unknown result".into(),
        Some("99".into()),
    );
    let cells = build_turn_cells(&thread);
    assert!(cells[0].items.contains(&2));
    assert!(!cells[1].items.contains(&2));
    assert!(!cells.iter().any(|cell| cell.items.contains(&3)));
}

#[test]
fn activity_indicator_reserves_content_height_and_reaches_final_line() {
    let viewport = transcript_content_height(20, true);
    assert_eq!(viewport, 19);
    let mut scroll = TranscriptScroll::default();
    scroll.sync(25, viewport, 1);
    scroll.scroll_up(1);
    scroll.sync(30, viewport, 2);
    assert_eq!(scroll.top, 5);
    scroll.scroll_down(usize::MAX, 30, viewport);
    assert_eq!(scroll.top, 11);
    assert!(scroll.follow);
}

#[test]
fn activity_indicator_requires_unseen_rows_below_viewport() {
    let scroll = TranscriptScroll {
        top: 10,
        follow: false,
        new_activity: true,
        seen_latest_revision: 2,
    };
    assert!(!should_show_activity(&scroll, 30, 20));
    assert!(should_show_activity(&scroll, 31, 20));

    let mut quiet = scroll.clone();
    quiet.new_activity = false;
    assert!(!should_show_activity(&quiet, 31, 20));
}

#[test]
fn turn_cache_retains_one_layout_per_turn_at_the_active_width() {
    let mut thread = Thread::new_foreground();
    for index in 0..200 {
        thread.add_turn(
            ItemKind::User,
            format!("question {index}"),
            Some(index.to_string()),
        );
    }
    let cells = build_turn_cells(&thread);
    let mut cache = TurnLayoutCache::default();
    cache.prepare(80, &cells, thread.structure_revision);
    for cell in &cells {
        cache.layout(cell_key(cell), cell_revision(&thread, cell), 0, || {
            CellLayout {
                lines: vec![Line::raw("cell")],
                hits: vec![None],
                ..CellLayout::default()
            }
        });
    }
    assert_eq!(cache.layouts.len(), 200);
    cache.layout(
        cell_key(&cells[0]),
        CellRevision {
            item: u64::MAX,
            metric: u64::MAX,
            worker: u64::MAX,
        },
        0,
        || CellLayout {
            lines: vec![Line::raw("updated")],
            hits: vec![None],
            ..CellLayout::default()
        },
    );
    assert_eq!(cache.layouts.len(), 200);
    cache.prepare(81, &cells, thread.structure_revision);
    assert!(cache.layouts.is_empty());
}

#[test]
fn turn_projection_survives_stream_mutations_without_rebuilding() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
    let mut projection = TurnProjection::default();
    assert!(projection.update(&thread));
    let revision = cell_revision(&thread, &projection.cells[0]);
    thread.add_reply_fragment("delta".into(), Some("2".into()), false);
    assert!(!projection.update(&thread));
    assert_ne!(cell_revision(&thread, &projection.cells[0]), revision);
}

#[test]
fn traces_default_hidden_and_only_conversation_header_is_click_target() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
    thread.add_turn(
        ItemKind::System,
        "raw diagnostic detail".into(),
        Some("2".into()),
    );
    let cells = build_turn_cells(&thread);
    let latest = latest_conversation_timestamp(&thread, &cells[0]);
    let collapsed = turn_cell_layout(
        0,
        0,
        std::slice::from_ref(&thread),
        &cells[0],
        80,
        latest,
        false,
        "",
        false,
        None,
    );
    let collapsed_text = collapsed
        .lines
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!collapsed_text.contains("raw diagnostic detail"));
    assert!(!collapsed_text.contains("trace ·"));
    assert!(collapsed
        .hits
        .iter()
        .any(|hit| *hit == Some(ClickTarget::TraceSummary(0))));
    let open = turn_cell_layout(
        0,
        0,
        std::slice::from_ref(&thread),
        &cells[0],
        80,
        latest,
        false,
        "",
        true,
        None,
    );
    assert!(open
        .lines
        .iter()
        .any(|line| line.to_string().contains("trace ·")));
    assert!(open
        .lines
        .iter()
        .any(|line| line.to_string().contains("raw diagnostic detail")));
}

#[test]
fn selected_conversation_has_no_header_or_body_selection_background() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
    let cell = build_turn_cells(&thread).pop().expect("chat cell");
    let layout = turn_cell_layout(
        0,
        0,
        std::slice::from_ref(&thread),
        &cell,
        80,
        latest_conversation_timestamp(&thread, &cell),
        false,
        "",
        true,
        None,
    );

    assert_eq!(layout.hits[0], Some(ClickTarget::TraceSummary(0)));
    assert!(layout.hits[1..].iter().all(|hit| hit.is_none()));
    assert_eq!(
        layout
            .lines
            .iter()
            .filter(|line| line.style.bg == Some(Color::Rgb(30, 32, 36)))
            .count(),
        0
    );
    assert!(layout
        .lines
        .iter()
        .flat_map(|line| &line.spans)
        .all(|span| span.style.bg != Some(Color::Rgb(30, 32, 36))));
}

#[test]
fn inactive_turn_metadata_is_dimmed_and_latest_metadata_is_green() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "old question".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "old answer".into(), Some("2".into()));
    thread.add_turn(ItemKind::User, "new question".into(), Some("3".into()));
    thread.add_turn(ItemKind::Reply, "new answer".into(), Some("3".into()));
    thread.items[0].timestamp = 1;
    thread.items[1].timestamp = 2;
    thread.items[2].timestamp = 3;
    thread.items[3].timestamp = 4;
    let cells = build_turn_cells(&thread);

    let old = main_conversation_layout(&thread, &cells[0], 80, 4, false, "");
    let latest = main_conversation_layout(&thread, &cells[1], 80, 4, false, "");

    assert_eq!(
        old.lines[0].spans.last().unwrap().style.fg,
        Some(Color::DarkGray)
    );
    assert_eq!(
        latest.lines[0].spans.last().unwrap().style.fg,
        Some(Color::Green)
    );
}

#[test]
fn correlated_standalone_reply_is_marked_restored() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::Reply, "recovered answer".into(), Some("2".into()));
    let cell = build_turn_cells(&thread).pop().expect("standalone reply");

    let layout = main_conversation_layout(
        &thread,
        &cell,
        80,
        latest_conversation_timestamp(&thread, &cell),
        false,
        "",
    );

    assert!(layout.lines[0].to_string().contains("RESTORED"));
}

#[test]
fn resting_turn_layout_matches_main_conversation_lines() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
    thread.add_turn(ItemKind::System, "diagnostic".into(), Some("2".into()));
    let cells = build_turn_cells(&thread);
    let latest = latest_conversation_timestamp(&thread, &cells[0]);
    let expected = main_conversation_layout(&thread, &cells[0], 80, latest, false, "");
    let resting = turn_cell_layout(
        0,
        0,
        std::slice::from_ref(&thread),
        &cells[0],
        80,
        latest,
        false,
        "",
        false,
        None,
    );
    assert_eq!(resting.lines, expected.lines);
}

#[test]
fn selected_trace_orders_and_indents_model_and_worker_hierarchy() {
    let worker_id = "worker-123456789-secret";
    let mut threads = vec![Thread::new_foreground()];
    threads[0].add_turn(ItemKind::User, "question".into(), Some("2".into()));
    threads[0].add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
    threads[0].add_turn(
        ItemKind::System,
        "[timing] completed 1048ms".into(),
        Some("2".into()),
    );
    threads[0].add_turn(
        ItemKind::Spawn,
        format!("worker {worker_id}: Check release evidence"),
        Some("2".into()),
    );
    threads[0].add_turn(
        ItemKind::Error,
        format!("work {worker_id}: Check release evidence\nreview failed: stale evidence"),
        Some("2".into()),
    );
    let worker = find_or_create_thread(&mut threads, worker_id, false, None);
    threads[worker].add_tool(
        "agent_browser {\"action\":\"get\"}".into(),
        "tool-1".into(),
        Some("2".into()),
    );
    threads[worker].add_turn(ItemKind::Reply, "Evidence checked".into(), Some("2".into()));

    let cells = build_turn_cells(&threads[0]);
    let latest = latest_conversation_timestamp(&threads[0], &cells[0]);
    let lines = turn_cell_layout(
        0,
        0,
        &threads,
        &cells[0],
        100,
        latest,
        false,
        "",
        true,
        Some(worker_id),
    )
    .lines
    .iter()
    .map(ToString::to_string)
    .collect::<Vec<_>>();
    let model = lines
        .iter()
        .position(|line| line.contains("Model"))
        .unwrap();
    let agents = lines
        .iter()
        .position(|line| line.contains("Agents"))
        .unwrap();
    let worker_heading = lines
        .iter()
        .position(|line| line.contains("Check release evidence"))
        .unwrap();
    let worker_tool = lines
        .iter()
        .position(|line| line.contains("agent_browser"))
        .unwrap();
    assert!(model < agents && agents < worker_heading && worker_heading < worker_tool);
    assert!(lines[worker_heading].contains(icon::EXPANDED));
    assert!(lines[worker_tool].contains("      "));
    assert!(lines.iter().any(|line| line.contains("review failed")));
    assert!(lines.iter().any(|line| line.contains("1.0s")));
    assert!(!lines.iter().any(|line| line.contains(worker_id)));
    assert!(lines.iter().any(|line| line.contains("worker-1")));
}

#[test]
fn trace_summary_counts_are_singular_plural_and_non_repetitive() {
    assert_eq!(trace_count_summary(1, 1, 0), "  trace · 1 tool");
    assert_eq!(trace_count_summary(1, 0, 1), "  trace · 1 agent");
    assert_eq!(
        trace_count_summary(5, 2, 1),
        "  trace · 5 events · 2 tools · 1 agent"
    );
    assert_eq!(
        trace_count_summary(6, 1, 2),
        "  trace · 6 events · 1 tool · 2 agents"
    );
}

#[test]
fn trace_timing_uses_human_duration() {
    assert_eq!(
        trace_summary("[timing] completed 1048ms"),
        format!("{} turn completed · 1.0s", icon::SUCCESS)
    );
    assert_eq!(
        trace_summary("[timing] ready 420ms"),
        format!("{} ready · +420ms", icon::WAITING)
    );
}

#[test]
fn semantic_icon_mapping_detects_tool_families() {
    assert_eq!(tool_icon("agent_browser"), icon::BROWSER);
    assert_eq!(tool_icon("web_search"), icon::SEARCH);
    assert_eq!(tool_icon("read_file"), icon::FILE);
    assert_eq!(tool_icon("shell"), icon::TOOL);
    for value in [
        icon::MODEL,
        icon::AGENT,
        icon::DURATION,
        icon::TOKENS,
        icon::RUNNING,
        icon::WAITING,
        icon::SUCCESS,
        icon::WARNING,
        icon::FAILURE,
        icon::COLLAPSED,
        icon::EXPANDED,
    ] {
        assert!(!value.is_empty());
    }
}

#[test]
fn model_timeline_compacts_completed_lifecycle_noise() {
    let mut thread = Thread::new_foreground();
    for text in [
        "[timing] model_request_1_started 0ms",
        "[ready] provider ready",
        "[working] generating",
        "[timing] first_visible 6100ms",
        "[timing] publication_started 42000ms",
        "commit complete",
        "[timing] completed 42800ms",
    ] {
        thread.add(ItemKind::System, text.into());
    }
    thread.add(ItemKind::Error, "provider warning".into());
    thread.add_tool("search {}".into(), "tool-1".into(), None);
    let items = (0..thread.items.len())
        .map(|index| (0, index))
        .collect::<Vec<_>>();
    let (timeline, remaining) = compact_model_timeline(&items, &[thread]);
    assert_eq!(
        timeline.as_deref(),
        Some("first answer 6.1s -> completed 42.8s")
    );
    assert_eq!(remaining.len(), 2);
}

#[test]
fn model_timeline_distinguishes_receipt_provider_and_answer_without_defaults() {
    let mut thread = Thread::new_foreground();
    for text in [
        "[timing] input_accepted 1ms",
        "[timing] provider_first_output 30ms",
        "[timing] acknowledgement_published 500ms",
        "[timing] model_request_1_completed 900ms",
    ] {
        thread.add(ItemKind::System, text.into());
    }
    let items = (0..thread.items.len())
        .map(|index| (0, index))
        .collect::<Vec<_>>();
    let (timeline, _) = compact_model_timeline(&items, std::slice::from_ref(&thread));
    assert_eq!(
        timeline.as_deref(),
        Some("accepted 1ms -> first output 30ms -> ack 500ms")
    );
    thread.add(ItemKind::System, "[timing] first_answer 1100ms".into());
    thread.add(ItemKind::System, "[timing] completed 1400ms".into());
    let items = (0..thread.items.len())
        .map(|index| (0, index))
        .collect::<Vec<_>>();
    let (timeline, _) = compact_model_timeline(&items, &[thread]);
    assert_eq!(
        timeline.as_deref(),
        Some(
            "accepted 1ms -> first output 30ms -> ack 500ms -> first answer 1.1s -> completed 1.4s"
        )
    );
}

#[test]
fn closed_worker_is_one_row_and_still_explains_outcome() {
    let id = "worker-123456789";
    let mut threads = vec![Thread::new_foreground()];
    threads[0].add_turn(ItemKind::User, "question".into(), Some("2".into()));
    threads[0].add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
    threads[0].add_turn(
        ItemKind::Spawn,
        format!("worker {id}: Inspect release evidence"),
        Some("2".into()),
    );
    let worker = find_or_create_thread(&mut threads, id, false, None);
    threads[worker].add_turn(ItemKind::Reply, "done".into(), Some("2".into()));
    let cells = build_turn_cells(&threads[0]);
    let latest = latest_conversation_timestamp(&threads[0], &cells[0]);
    let layout = turn_cell_layout(
        0, 0, &threads, &cells[0], 100, latest, false, "", true, None,
    );
    let rows = layout
        .lines
        .iter()
        .filter(|line| line.to_string().contains("Inspect release evidence"))
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].to_string().contains("complete"));
    assert!(!layout
        .lines
        .iter()
        .any(|line| line.to_string() == "complete"));
    assert!(layout
        .hits
        .iter()
        .any(|hit| { matches!(hit, Some(ClickTarget::Worker(0, worker)) if worker == id) }));
}

#[test]
fn worker_detail_toggle_keeps_only_one_worker_open() {
    let mut open = None;
    toggle_worker(&mut open, 0, "one".into());
    assert_eq!(open, Some((0, "one".into())));
    toggle_worker(&mut open, 0, "two".into());
    assert_eq!(open, Some((0, "two".into())));
    toggle_worker(&mut open, 0, "two".into());
    assert_eq!(open, None);
}

#[test]
fn worker_normal_details_elide_ids_and_summarize_review_errors() {
    let id = "worker-123456789-secret";
    let summary = worker_error_summary(
        &format!("work {id}: objective\nreview failed: stale evidence from task-77"),
        Some(id),
    );
    assert_eq!(summary, "review failed");
    assert!(!summary.contains(id));
    assert_eq!(
        elide_work_id("work task-77: validating release"),
        "work · validating release"
    );
}

#[test]
fn markdown_bullets_hang_and_preserve_blank_paragraphs() {
    let lines = markdown_body_lines(
        "- first entry wraps onto another line\n\nSummary - alpha item - beta item",
        16,
        Color::White,
    );
    let text = lines.iter().map(ToString::to_string).collect::<Vec<_>>();
    assert!(text[0].contains(icon::BULLET));
    assert!(text.iter().any(|line| line.starts_with("      ")));
    assert_eq!(text.iter().filter(|line| line.is_empty()).count(), 1);
    assert_eq!(
        text.iter()
            .filter(|line| line.contains(icon::BULLET))
            .count(),
        3
    );
    assert!(text.iter().any(|line| line.trim() == "Summary"));
}

#[test]
fn escape_end_reset_helper_closes_nested_selection_and_follows() {
    let mut trace = Some(2);
    let mut worker = Some((2, "worker".into()));
    let mut scroll = TranscriptScroll {
        follow: false,
        new_activity: true,
        ..TranscriptScroll::default()
    };
    assert!(close_trace_details(&mut trace, &mut worker, &mut scroll));
    assert_eq!(trace, None);
    assert_eq!(worker, None);
    assert!(scroll.follow);
    assert!(!close_trace_details(&mut trace, &mut worker, &mut scroll));
}

#[test]
fn footer_trace_mode_is_concise_and_contextual() {
    assert_eq!(footer_mode_text(None, true), None);
    assert_eq!(
        footer_mode_text(Some(1), false),
        Some(format!(
            "DETAILS    Ctrl+O collapse · Ctrl+D diagnostics · {} Pg scroll · {} Esc close · {} help",
            icon::SCROLL,
            icon::CLOSE,
            icon::HELP
        ))
    );
    assert_eq!(
        footer_mode_text(None, false),
        Some(format!(
            "HISTORY    ↑↓ scroll · {} End live · {} help",
            icon::LIVE,
            icon::HELP
        ))
    );
}

#[test]
fn selected_worker_changes_the_single_cached_turn_variant() {
    let mut cache = TurnLayoutCache::default();
    let key = CellKey {
        prompt_timestamp: 1,
        prompt_index: 0,
    };
    let revision = CellRevision {
        item: 1,
        metric: 1,
        worker: 1,
    };
    cache.layout(key.clone(), revision, 2, || CellLayout {
        lines: vec![Line::raw("closed")],
        hits: vec![None],
        ..CellLayout::default()
    });
    cache.layout(key, revision, 6, || CellLayout {
        lines: vec![Line::raw("worker open")],
        hits: vec![None],
        ..CellLayout::default()
    });
    assert_eq!(cache.layouts.len(), 1);
    assert_eq!(cache.builds, 2);
}

#[test]
fn selected_turn_nests_correlated_worker_tools() {
    let mut threads = vec![Thread::new_foreground()];
    threads[0].add_turn(ItemKind::User, "question".into(), Some("2".into()));
    threads[0].add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
    let worker = find_or_create_thread(&mut threads, "worker-123456", false, None);
    threads[worker].add_tool(
        "agent_browser {\"action\":\"get\"}".into(),
        "tool-1".into(),
        Some("2".into()),
    );
    let cells = build_turn_cells(&threads[0]);
    let latest = latest_conversation_timestamp(&threads[0], &cells[0]);
    let closed = turn_cell_layout(0, 0, &threads, &cells[0], 80, latest, false, "", true, None);
    assert!(!closed
        .lines
        .iter()
        .any(|line| line.to_string().contains("agent_browser")));
    let open = turn_cell_layout(
        0,
        0,
        &threads,
        &cells[0],
        80,
        latest,
        false,
        "",
        true,
        Some("worker-123456"),
    );
    assert!(open
        .lines
        .iter()
        .any(|line| line.to_string().contains("agent_browser")));
    assert!(worker_turn_revisions(&threads).contains_key("2"));
}

#[test]
fn trace_toggle_keeps_at_most_one_drawer_open() {
    let mut open = None;
    toggle_trace(&mut open, 0);
    assert_eq!(open, Some(0));
    toggle_trace(&mut open, 0);
    assert_eq!(open, None);
    toggle_trace(&mut open, 1);
    assert_eq!(open, Some(1));
    toggle_trace(&mut open, 2);
    assert_eq!(open, Some(2));
}

#[test]
fn ctrl_o_targets_viewport_anchor_or_live_turn() {
    let view = TranscriptView {
        attention_hits: Vec::new(),
        turns: 4,
        anchor_turn: Some(1),
        ..TranscriptView::default()
    };
    assert_eq!(ctrl_o_target(&view, false), Some(1));
    assert_eq!(ctrl_o_target(&view, true), Some(3));
}

#[test]
fn turn_cache_invalidates_only_changed_streaming_turn() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "first".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "done".into(), Some("2".into()));
    thread.add_turn(ItemKind::User, "second".into(), Some("3".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));
    let mut projection = TurnProjection::default();
    projection.update(&thread);
    let mut cache = TurnLayoutCache::default();
    cache.prepare(80, &projection.cells, thread.structure_revision);
    for cell in &projection.cells {
        cache.layout(cell_key(cell), cell_revision(&thread, cell), 0, || {
            CellLayout {
                lines: vec![Line::raw("cell")],
                hits: vec![None],
                ..CellLayout::default()
            }
        });
    }
    assert_eq!(cache.builds, 2);
    thread.add_reply_fragment("delta".into(), Some("3".into()), false);
    assert!(!projection.update(&thread));
    for cell in &projection.cells {
        cache.layout(cell_key(cell), cell_revision(&thread, cell), 0, || {
            CellLayout {
                lines: vec![Line::raw("cell")],
                hits: vec![None],
                ..CellLayout::default()
            }
        });
    }
    assert_eq!(cache.builds, 3);
}

#[test]
fn metric_revision_is_not_masked_by_a_newer_item_revision() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
    thread.metric_revisions.insert("2".into(), 1);
    let cell = &build_turn_cells(&thread)[0];
    let before = cell_revision(&thread, cell);
    assert!(before.item > before.metric);

    thread.metric_revisions.insert("2".into(), 2);
    let after = cell_revision(&thread, cell);
    assert_ne!(before, after);
}

#[test]
fn clear_reset_discards_unified_view_projection_and_cache() {
    let mut scroll = TranscriptScroll::default();
    scroll.scroll_up(1);
    let mut view = TranscriptView {
        attention_hits: Vec::new(),
        total_height: 10,
        viewport: 5,
        turns: 1,
        anchor_turn: Some(0),
        starts: vec![0],
        heights: vec![10],
    };
    let mut cache = TurnLayoutCache::default();
    cache.width = Some(80);
    let mut open_trace = Some(0);
    let mut projection = TurnProjection {
        structure_revision: Some(2),
        cells: Vec::new(),
    };
    reset_transcript(
        &mut scroll,
        &mut view,
        &mut cache,
        &mut open_trace,
        &mut projection,
    );
    assert_eq!(scroll, TranscriptScroll::default());
    assert_eq!(view.total_height, 0);
    assert!(cache.layouts.is_empty());
    assert_eq!(cache.width, None);
    assert_eq!(open_trace, None);
    assert_eq!(projection.structure_revision, None);
}

fn envelope(event_id: u64, kind: AgentEvent) -> EventEnvelope {
    EventEnvelope {
        event_id,
        session_id: "conversation".into(),
        conversation_id: Some("conversation".into()),
        turn_id: Some("2".into()),
        task_id: None,
        parent_task_id: None,
        tool_call_id: None,
        actor: Actor::Foreground,
        sequence: event_id,
        occurred_at_ms: 1,
        kind,
    }
}

fn interaction(event: InteractionEvent) -> InteractionEventEnvelope {
    InteractionEventEnvelope {
        metadata: tachyon_api::InteractionMetadata {
            web_availability: None,
            attention: None,
            protocol_version: tachyon_api::INTERACTION_PROTOCOL_VERSION,
            message_id: "event-1".into(),
            cwd: None,
            correlation_id: "turn-2".into(),
            causation_id: Some("command-1".into()),
            conversation_id: FOREGROUND_ID.into(),
            turn_id: Some("2".into()),
            generation: 1,
            occurred_at_ms: 1,
        },
        event,
    }
}

#[test]
fn workspace_selection_is_explicit_and_per_message() {
    let local = |path: &str| Ok(std::path::PathBuf::from(path));
    assert_eq!(
        foreground_workspace_request("research this".into(), local("/project/a")).unwrap(),
        ("research this".into(), Some("/project/a".into()))
    );
    assert_eq!(
        foreground_workspace_request("/managed research this".into(), local("/project/a")).unwrap(),
        ("research this".into(), None)
    );
    assert_eq!(
        foreground_workspace_request("next".into(), local("/project/b"))
            .unwrap()
            .1,
        Some("/project/b".into())
    );
    assert!(
        foreground_workspace_request("next".into(), Err(std::io::Error::other("gone")))
            .unwrap_err()
            .contains("cannot select current workspace")
    );
    assert!(foreground_workspace_request(
        "/managed next".into(),
        Err(std::io::Error::other("gone"))
    )
    .unwrap()
    .1
    .is_none());
}

#[test]
fn typed_interaction_stream_projects_one_final_reply() {
    let mut thread = Thread::new_foreground();
    thread.add(ItemKind::User, "hello".into());
    thread.reserve_reply();

    apply_interaction_event(
        &mut thread,
        interaction(InteractionEvent::UserTurnAccepted {
            text: "hello".into(),
        }),
    );
    apply_interaction_event(
        &mut thread,
        interaction(InteractionEvent::ConversationDelta { text: "Hi ".into() }),
    );
    apply_interaction_event(
        &mut thread,
        interaction(InteractionEvent::ConversationFinished {
            text: "Hi there.".into(),
        }),
    );

    assert_eq!(thread.items.len(), 2);
    assert_eq!(thread.items[0].turn.as_deref(), Some("2"));
    assert_eq!(thread.items[1].kind, ItemKind::Reply);
    assert_eq!(thread.items[1].text, "Hi there.");
    assert!(!thread.streaming);
}

#[test]
fn scheduled_notification_is_projected_as_standalone_reply() {
    let mut thread = Thread::new_foreground();
    let mut event = interaction(InteractionEvent::UserVisibleNotificationPublished {
        text: "Your coffee is ready.".into(),
    });
    event.metadata.message_id = "reminder-delivery-reminder-1".into();
    event.metadata.correlation_id = "reminder-1".into();
    event.metadata.turn_id = None;
    apply_interaction_event(&mut thread, event);

    assert_eq!(thread.items.len(), 1);
    assert_eq!(thread.items[0].kind, ItemKind::Reply);
    assert_eq!(thread.items[0].text, "Your coffee is ready.");
    assert_eq!(thread.items[0].turn, None);
    let cells = build_turn_cells(&thread);
    assert_eq!(cells.len(), 1);
    assert_eq!(cells[0].prompt, 0);
    let layout = main_conversation_layout(
        &thread,
        &cells[0],
        80,
        latest_conversation_timestamp(&thread, &cells[0]),
        false,
        "",
    );
    assert!(!layout.lines[0].to_string().contains("RESTORED"));
}

#[test]
fn reminder_schedule_badges_report_committed_changes() {
    assert_eq!(
        schedule_badges(
            &ScheduleTurnMetrics {
                scheduled: 1,
                tasks_scheduled: 0,
                cancelled: 0,
                fired: 0,
            },
            false,
        ),
        ["reminder scheduled"]
    );
    assert_eq!(
        schedule_badges(
            &ScheduleTurnMetrics {
                scheduled: 0,
                tasks_scheduled: 0,
                cancelled: 1,
                fired: 0,
            },
            false,
        ),
        ["reminder cancelled"]
    );
    assert_eq!(
        schedule_badges(
            &ScheduleTurnMetrics {
                tasks_scheduled: 1,
                ..ScheduleTurnMetrics::default()
            },
            false,
        ),
        ["agent task scheduled"]
    );
}

#[test]
fn interaction_envelope_is_decoded_before_line_fallback() {
    let wire = serde_json::to_string(&interaction(InteractionEvent::ConversationFinished {
        text: "done".into(),
    }))
    .unwrap();
    let decoded = decode_interaction_event(&wire).expect("typed interaction event");
    assert!(matches!(
        decoded.event,
        InteractionEvent::ConversationFinished { text } if text == "done"
    ));
}

#[test]
fn user_echo_reconciles_the_daemon_turn_id() {
    let mut thread = Thread::new_foreground();
    thread.add(ItemKind::User, "hello".into());
    classify_line(&mut thread, "[turn:2] [user] hello");
    assert_eq!(
        thread.items.last().and_then(|item| item.turn.as_deref()),
        Some("2")
    );
}

#[test]
fn memory_lifecycle_events_render_beside_token_usage_without_trace_rows() {
    let mut threads = vec![Thread::new_foreground()];
    for (event_id, kind) in [
        (
            1,
            AgentEvent::Usage {
                turn: Some(3),
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                context_tokens: 10,
                context_window: Some(100),
            },
        ),
        (
            2,
            AgentEvent::MemoryRecalled {
                turn: Some(3),
                preference_count: 1,
                history_count: 4,
            },
        ),
        (
            3,
            AgentEvent::MemoryMutation {
                turn: Some(3),
                result: MemoryMutationResult::Applied {
                    kind: MemoryMutationKind::Remember,
                    memory_id: "preference-1".into(),
                    replaced_memory_id: None,
                },
            },
        ),
    ] {
        record_correlated_metrics(
            &mut threads,
            &EventEnvelope {
                event_id,
                session_id: FOREGROUND_ID.into(),
                conversation_id: Some(FOREGROUND_ID.into()),
                turn_id: Some("3".into()),
                task_id: None,
                parent_task_id: None,
                tool_call_id: None,
                actor: Actor::Foreground,
                sequence: event_id,
                occurred_at_ms: event_id,
                kind,
            },
        );
    }
    let thread = &threads[0];
    assert!(thread.items.is_empty());
    let metrics = &thread.metrics["3"];
    let mut badges = vec![format!(
        "{} total {}",
        icon::TOKENS,
        format_count(metrics.self_usage.as_ref().unwrap().total)
    )];
    badges.extend(memory_badges(&metrics.memory, false));
    let badges = badges.join(" · ");
    assert!(badges.contains("total 15"));
    assert!(badges.contains("memory saved"));
    assert!(badges.contains("memory recalled 5"));
}

#[test]
fn reserved_reply_is_reconciled_and_filled_in_place() {
    let mut thread = Thread::new_foreground();
    thread.add(ItemKind::User, "hello".into());
    thread.reserve_reply();

    classify_line(&mut thread, "[turn:2] [user] hello");
    assert_eq!(thread.items.len(), 2);
    assert_eq!(thread.items[0].turn.as_deref(), Some("2"));
    assert_eq!(thread.items[1].turn.as_deref(), Some("2"));
    assert_eq!(thread.items[1].kind, ItemKind::PendingReply);

    thread.add_reply_fragment("answer".into(), Some("2".into()), false);
    assert_eq!(thread.items.len(), 2);
    assert_eq!(thread.items[1].kind, ItemKind::Reply);
    assert_eq!(thread.items[1].text, "answer");
}

#[test]
fn turn_status_updates_only_its_existing_pending_reply() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "first".into(), Some("2".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
    thread.add_turn(ItemKind::User, "second".into(), Some("3".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));

    apply_agent_event(
        &mut thread,
        AgentEvent::Status {
            turn: Some(3),
            phase: "queued".into(),
            message: "Earlier answer is still running; this turn will respond in context.".into(),
        },
    );
    apply_agent_event(
        &mut thread,
        AgentEvent::Status {
            turn: Some(2),
            phase: "working".into(),
            message: "using context".into(),
        },
    );
    apply_agent_event(
        &mut thread,
        AgentEvent::Status {
            turn: Some(3),
            phase: "working".into(),
            message: String::new(),
        },
    );

    assert_eq!(thread.items[1].text, "using context");
    assert_eq!(
        thread.items[3].text,
        "Earlier answer is still running; this turn will respond in context."
    );
    assert_eq!(
        thread
            .items
            .iter()
            .filter(|item| item.kind == ItemKind::System)
            .count(),
        3,
        "status traces remain available"
    );

    let cell = build_turn_cells(&thread).pop().expect("latest turn");
    let layout = main_conversation_layout(&thread, &cell, 100, u64::MAX, true, "working");
    let rendered = layout
        .lines
        .iter()
        .map(Line::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("Earlier answer is still running"));
    assert!(!rendered.contains("working..."));
}

#[test]
fn streamed_acknowledgement_stays_pending_until_conversation_finishes() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "slow request".into(), Some("2".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
    thread.add_reply_fragment("Let me check that for you.".into(), Some("2".into()), false);
    thread.add_turn(ItemKind::User, "tell me a joke".into(), Some("3".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));

    let cells = build_turn_cells(&thread);
    let pending = main_conversation_layout(&thread, &cells[0], 100, u64::MAX, false, "working");
    let pending_text = pending
        .lines
        .iter()
        .map(Line::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!pending_text.contains("󰔟"));
    assert_eq!(ready_earlier_turn(&[thread]), None);

    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "slow request".into(), Some("2".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
    thread.add_reply_fragment("Checking now.".into(), Some("2".into()), false);
    thread.add_turn(ItemKind::User, "tell me a joke".into(), Some("3".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));
    thread.finish_reply("Here is the answer.".into(), Some("2".into()));

    assert!(thread.completed_turns.contains("2"));
    assert_eq!(ready_earlier_turn(&[thread]), Some(2));
}

#[test]
fn dismissed_ready_turn_is_not_restored_by_replayed_completion() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "slow request".into(), Some("2".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
    thread.add_turn(ItemKind::User, "new request".into(), Some("3".into()));
    thread.finish_reply("finished".into(), Some("2".into()));
    mark_ready_turn_seen(std::slice::from_mut(&mut thread), "2");
    thread.finish_reply("finished".into(), Some("2".into()));

    assert_eq!(ready_earlier_turn(&[thread]), None);
    assert_eq!(
        ready_notice(2),
        format!("{} response 2 ready", icon::SUCCESS)
    );
}

#[test]
fn untyped_foreground_output_does_not_leak_into_model_trace() {
    let mut thread = Thread::new_foreground();
    classify_line(&mut thread, "**Temperature:** raw worker evidence");
    assert!(thread.items.is_empty());
}

#[test]
fn typed_tool_telemetry_projects_into_the_correlated_trace() {
    let mut thread = Thread::new_foreground();
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::ToolTelemetry {
            tool_name: "grep".into(),
            call_id: Some("call-1".into()),
            duration_ms: 38,
            success: true,
            truncated: false,
            bytes_out: 420,
            error_code: None,
            identity: tachyon_api::types::ToolTelemetryIdentity {
                task_id: Some("task-1".into()),
                work_id: Some("work-1".into()),
                generation: Some(1),
                assignment: Some(1),
                attempt_id: None,
            },
        },
        Some("2"),
    );

    assert_eq!(thread.items.len(), 1);
    assert_eq!(thread.items[0].kind, ItemKind::System);
    assert_eq!(thread.items[0].turn.as_deref(), Some("2"));
    assert!(thread.items[0]
        .text
        .contains("grep complete · 38ms · 420 bytes"));
}

#[test]
fn conversation_finished_replaces_turn_reservation_once() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "first".into(), Some("2".into()));
    thread.add_turn(
        ItemKind::PendingReply,
        "queued status".into(),
        Some("2".into()),
    );
    thread.add_turn(ItemKind::User, "second".into(), Some("3".into()));
    thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));

    apply_interaction_event(
        &mut thread,
        interaction(InteractionEvent::ConversationFinished {
            text: "first answer".into(),
        }),
    );
    apply_interaction_event(
        &mut thread,
        interaction(InteractionEvent::ConversationFinished {
            text: "corrected answer".into(),
        }),
    );

    assert_eq!(thread.items.len(), 4);
    assert_eq!(thread.items[1].kind, ItemKind::Reply);
    assert_eq!(thread.items[1].text, "corrected answer");
    assert_eq!(thread.items[3].kind, ItemKind::PendingReply);
    assert_eq!(
        thread
            .items
            .iter()
            .filter(|item| item.kind == ItemKind::Reply && item.turn.as_deref() == Some("2"))
            .count(),
        1
    );
}

#[test]
fn conversation_finish_replaces_the_acknowledgement_timestamp() {
    let mut thread = Thread::new_foreground();
    thread.add_reply_fragment("Checking now.".into(), Some("2".into()), false);
    thread.items[0].timestamp = 1;
    thread.finish_reply("Finished.".into(), Some("2".into()));

    assert!(thread.items[0].timestamp > 1);
}

#[test]
fn final_reply_is_idempotent_per_turn() {
    let mut thread = Thread::new_foreground();
    apply_agent_event(
        &mut thread,
        AgentEvent::Reply {
            turn: Some(2),
            text: "first".into(),
            final_reply: true,
        },
    );
    apply_agent_event(
        &mut thread,
        AgentEvent::Reply {
            turn: Some(2),
            text: "corrected".into(),
            final_reply: true,
        },
    );
    let replies = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Reply)
        .collect::<Vec<_>>();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].text, "corrected");
}

#[test]
fn interleaved_reply_deltas_stay_grouped_by_turn() {
    let mut thread = Thread::new_foreground();
    for (turn, text) in [(2, "slow "), (3, "hello "), (2, "answer"), (3, "there")] {
        apply_agent_event(
            &mut thread,
            AgentEvent::ReplyDelta {
                turn: Some(turn),
                text: text.into(),
            },
        );
    }

    let replies = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Reply)
        .collect::<Vec<_>>();
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0].text, "slow answer");
    assert_eq!(replies[1].text, "hello there");
}

#[test]
fn late_reply_is_inserted_into_its_turn_block() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "second question".into(), Some("2".into()));
    thread.add_turn(ItemKind::User, "third question".into(), Some("3".into()));
    thread.finish_reply("third answer".into(), Some("3".into()));
    thread.finish_reply("second answer".into(), Some("2".into()));

    let timeline = thread
        .items
        .iter()
        .filter(|item| matches!(item.kind, ItemKind::User | ItemKind::Reply))
        .map(|item| (item.turn.as_deref().unwrap_or_default(), item.text.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        timeline,
        [
            ("2", "second question"),
            ("2", "second answer"),
            ("3", "third question"),
            ("3", "third answer"),
        ]
    );
}

#[test]
fn worker_start_uses_its_correlated_origin_turn() {
    let mut thread = Thread::new_foreground();
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::WorkerStarted {
            turn: Some(2),
            worker_id: "worker-1".into(),
            objective: "objective".into(),
        },
        Some("3"),
    );
    let spawn = thread
        .items
        .iter()
        .find(|item| item.kind == ItemKind::Spawn)
        .expect("spawn item");
    assert_eq!(spawn.turn.as_deref(), Some("2"));
}

#[test]
fn overlapping_work_results_increment_only_their_envelope_turn() {
    let mut thread = Thread::new_foreground();
    for worker_id in ["worker-1", "worker-2", "worker-3"] {
        thread.add_turn(
            ItemKind::Spawn,
            format!("worker {worker_id}: turn two"),
            Some("2".into()),
        );
    }
    thread.add_turn(
        ItemKind::Spawn,
        "worker worker-4: turn three".into(),
        Some("3".into()),
    );

    let mut seen = HashSet::new();
    for (event_id, turn, worker_id) in [(1, "2", "worker-1"), (2, "3", "worker-4")] {
        let mut result = envelope(
            event_id,
            AgentEvent::WorkResult {
                result: tachyon_api::types::WorkResult {
                    attempt_id: None,
                    instruction_revision: None,
                    work_id: worker_id.into(),
                    candidate_refs: None,
                    final_context: None,
                    evidence: Default::default(),
                    timing: None,
                    objective: format!("work for turn {turn}"),
                    generation: 1,
                    assignment: 1,
                    outcome: WorkOutcome::Completed {
                        result: "done".into(),
                        artifacts: Vec::new(),
                        context: String::new(),
                        suggested_reuse: false,
                    },
                },
            },
        );
        result.turn_id = Some(turn.into());
        assert!(accept_event(&mut seen, &result));
        let actor = result.actor.clone();
        let envelope_turn = result.turn_id.clone();
        apply_actor_event(&mut thread, result.kind, &actor, envelope_turn.as_deref());
    }

    assert_eq!(
        turn_badges(&thread, Some("2"), u64::MAX, false),
        "󰚩 3 agents · 󰄬 1 complete"
    );
    assert_eq!(
        turn_badges(&thread, Some("3"), u64::MAX, false),
        "󰚩 1 agent · 󰄬 1 complete"
    );
}

fn evidence_result(assignment: u64) -> tachyon_api::types::WorkResult {
    serde_json::from_value(serde_json::json!({
        "work_id": "work-1", "objective": "Inspect Python", "generation": 1,
        "assignment": assignment, "outcome": "completed", "result": "done",
        "evidence": {"omitted": 7, "tools": [{
            "call_id": "call-1", "parent_call_id": "python-parent", "tool_name": "python",
            "arguments": {"code": "print('secret-code')"},
            "output": {"content": "secret-output", "is_error": true, "truncated": true,
                "metadata": {"omitted_bytes": 400}, "error": {"message": "execution failed"}}
        }]},
        "timing": {"execution_ms": 1000, "inference_ms": null, "tool_ms": 0, "review_ms": 20}
    }))
    .unwrap()
}

fn evidence_layout(threads: &[Thread], open: bool, worker: Option<&str>) -> CellLayout {
    let cell = build_turn_cells(&threads[0]).remove(0);
    turn_cell_layout(0, 0, threads, &cell, 200, 0, false, "", open, worker)
}

fn layout_text(layout: &CellLayout) -> String {
    layout
        .lines
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn terminal_evidence_is_lazy_bounded_and_assignment_scoped() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.add_tool("python {}".into(), "call-1".into(), Some("2".into()));
    thread.add_tool_result("call-1".into(), "live-output".into(), Some("2".into()));
    let mut result = evidence_result(1);
    result.evidence.tools[0].parent_call_id = None;
    for _ in 0..2 {
        apply_correlated_agent_event(
            &mut thread,
            AgentEvent::WorkResult {
                result: result.clone(),
            },
            Some("2"),
        );
    }
    assert_eq!(thread.items.len(), 3);
    assert!(thread.items[1].output.is_none());
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::ToolStarted {
            turn: None,
            id: "call-1".into(),
            name: "python".into(),
            arguments: "{}".into(),
            identity: None,
        },
        Some("2"),
    );
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::ToolFinished {
            turn: None,
            id: "call-1".into(),
            output: "replayed output".into(),
            identity: None,
        },
        Some("2"),
    );
    assert_eq!(thread.items.len(), 3);
    assert!(thread.items[1].output.is_none());
    let saved = serde_json::to_value(thread.items[1].work.as_ref().unwrap()).unwrap();
    let restored: WorkDetail = serde_json::from_value(saved).unwrap();
    assert_eq!(
        restored.tool.as_ref().unwrap().parent_call_id.as_deref(),
        None
    );
    assert!(thread
        .items
        .iter()
        .all(|item| item.turn.as_deref() == Some("2")));
    let mut threads = vec![thread];
    DETAIL_FORMATS.with(|count| count.set(0));
    for (open, worker) in [(false, None), (false, Some("work-1")), (true, None)] {
        let text = layout_text(&evidence_layout(&threads, open, worker));
        assert!(!text.contains("secret-code"));
        assert!(!text.contains("terminal evidence"));
    }
    let layout = evidence_layout(&threads, true, Some("work-1"));
    let text = layout_text(&layout);
    for expected in [
        "python",
        "error / truncated",
        "7 omitted",
        "inference unknown",
        "tools 0ms",
        "review 20ms separate",
        "not additive",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    assert!(!text.contains("secret-code"));
    assert!(!text.contains("secret-output"));
    assert!(layout.hits.contains(&Some(ClickTarget::Item(0, 1))));
    DETAIL_FORMATS.with(|count| assert_eq!(count.get(), 0));
    threads[0].items[1].hidden = false;
    let text = layout_text(&evidence_layout(&threads, true, Some("work-1")));
    for expected in ["secret-code", "secret-output", "execution failed"] {
        assert!(text.contains(expected));
    }
    DETAIL_FORMATS.with(|count| assert_eq!(count.get(), 1));
    apply_correlated_agent_event(
        &mut threads[0],
        AgentEvent::WorkResult {
            result: evidence_result(2),
        },
        Some("2"),
    );
    assert_eq!(threads[0].items.len(), 5);
    assert_eq!(
        threads[0]
            .items
            .iter()
            .filter(|item| item.work.as_ref().is_some_and(|work| work.tool.is_some()))
            .count(),
        2
    );
}

#[test]
fn evidence_clean_view_preserves_code_and_raw_is_opt_in() {
    let mut result = evidence_result(1);
    let tool = &mut result.evidence.tools[0];
    tool.arguments = serde_json::json!({"code": "if True:\n    print('unique-code')\n\n    pass"});
    tool.output = serde_json::json!({
        "content": "unique-output", "is_error": false, "truncated": false,
        "error": null, "output_ref": null, "continuation": null,
        "metadata": {"exit_code": 0, "timed_out": false}
    });
    let mut thread = Thread::new_foreground();
    apply_correlated_agent_event(&mut thread, AgentEvent::WorkResult { result }, Some("2"));
    thread.items[0].hidden = false;
    let render = |thread: &Thread| {
        let mut layout = CellLayout {
            lines: Vec::new(),
            hits: Vec::new(),
            ..CellLayout::default()
        };
        push_trace_item(
            &mut layout,
            std::slice::from_ref(thread),
            0,
            0,
            100,
            "",
            None,
        );
        layout
    };
    let layout = render(&thread);
    let text = layout_text(&layout);
    assert!(
        text.contains("Code\n  if True:\n      print('unique-code')\n   \n      pass"),
        "{text}"
    );
    assert!(text.contains("Output\n  unique-output"));
    assert!(text.contains("exit0"));
    assert_eq!(text.matches("unique-code").count(), 1);
    assert_eq!(text.matches("unique-output").count(), 1);
    for absent in [
        "null",
        "arguments",
        "native envelope",
        "call-1",
        "python-parent",
        "output_ref",
        "metadata",
        "error",
        "truncated",
    ] {
        assert!(!text.contains(absent), "unexpected {absent}: {text}");
    }
    assert!(layout.hits.contains(&Some(ClickTarget::RawEvidence(0, 0))));
    thread.items[0].work.as_mut().unwrap().raw_open = true;
    let text = layout_text(&render(&thread));
    for expected in [
        "call call-1",
        "parent python-parent",
        "arguments",
        "native envelope",
        "metadata",
        "assignment 1/1",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    thread.items[0].hidden = true;
    DETAIL_FORMATS.with(|count| count.set(0));
    assert!(!layout_text(&render(&thread)).contains("unique-code"));
    DETAIL_FORMATS.with(|count| assert_eq!(count.get(), 0));
}

#[test]
fn evidence_preview_bounds_legacy_json_and_narrow_rendering() {
    let mut result = evidence_result(1);
    let tool = &mut result.evidence.tools[0];
    tool.arguments = serde_json::json!({"code": "x\n".repeat(100_000)});
    tool.output = serde_json::json!({
        "content": "x".repeat(1_000_000), "truncated": true,
        "error": "execution failed",
        "output_ref": "stored-output-1", "metadata": {"omitted_bytes": 999999}
    });
    let details = work_tool_details(tool, true);
    assert!(details.len() < 16 * 1024);
    assert!(details.contains("stored-output-1"));
    assert!(details.contains("omitted_bytes"));
    assert!(details.contains("display truncated"));
    let mut thread = Thread::new_foreground();
    apply_correlated_agent_event(&mut thread, AgentEvent::WorkResult { result }, Some("2"));
    thread.items[0].hidden = false;
    for (width, raw) in [
        (1, false),
        (80, false),
        (200, false),
        (1, true),
        (80, true),
        (200, true),
    ] {
        thread.items[0].work.as_mut().unwrap().raw_open = raw;
        let mut layout = CellLayout {
            lines: Vec::new(),
            hits: Vec::new(),
            ..CellLayout::default()
        };
        push_trace_item(
            &mut layout,
            std::slice::from_ref(&thread),
            0,
            0,
            width,
            "      ",
            None,
        );
        assert!(layout.lines.len() <= 130);
        assert_eq!(layout.lines.len(), layout.hits.len());
        assert!(layout_text(&layout).len() < 17 * 1024);
        assert!(layout_text(&layout).contains("display truncated"));
        assert!(layout_text(&layout).contains("error / truncated"));
        if width > 1 {
            assert!(layout_text(&layout).contains("execution failed"));
        }
    }
}

#[test]
fn evidence_does_not_claim_nested_or_uncorrelated_live_calls() {
    for turn in [None, Some("2")] {
        let mut thread = Thread::new_foreground();
        thread.add_tool("python {}".into(), "call-1".into(), turn.map(str::to_owned));
        let mut result = evidence_result(1);
        if turn.is_none() {
            result.evidence.tools[0].parent_call_id = None;
        }
        apply_correlated_agent_event(&mut thread, AgentEvent::WorkResult { result }, turn);
        assert!(thread.items[0].work.is_none());
        assert_eq!(thread.items.len(), 3);
    }
}

#[test]
fn evidence_and_timing_replay_invalidate_cell_and_worker_revisions() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    let mut result = evidence_result(1);
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::WorkResult {
            result: result.clone(),
        },
        Some("2"),
    );
    let cell = build_turn_cells(&thread).remove(0);
    let before = cell_revision(&thread, &cell);
    thread.is_foreground = false;
    let worker_before = worker_turn_revisions(std::slice::from_ref(&thread))["2"];
    result.timing.as_mut().unwrap().review_ms = Some(123);
    result.evidence.tools[0].output = serde_json::json!({"content": "updated"});
    apply_correlated_agent_event(&mut thread, AgentEvent::WorkResult { result }, Some("2"));
    assert_ne!(before, cell_revision(&thread, &cell));
    assert_ne!(
        worker_before,
        worker_turn_revisions(std::slice::from_ref(&thread))["2"]
    );
    assert_eq!(thread.items.len(), 3);
    assert_eq!(
        thread.items[2]
            .work
            .as_ref()
            .unwrap()
            .timing
            .as_ref()
            .unwrap()
            .review_ms,
        Some(123)
    );
}

#[test]
fn evidence_does_not_merge_ambiguous_live_calls_and_replays_without_ids() {
    let mut thread = Thread::new_foreground();
    for _ in 0..2 {
        thread.add_tool("python {}".into(), "call-1".into(), Some("2".into()));
    }
    let mut result = evidence_result(1);
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::WorkResult {
            result: result.clone(),
        },
        Some("2"),
    );
    assert!(thread.items[..2].iter().all(|item| item.work.is_none()));
    result.assignment = 2;
    result.evidence.tools[0].call_id = None;
    for _ in 0..2 {
        apply_correlated_agent_event(
            &mut thread,
            AgentEvent::WorkResult {
                result: result.clone(),
            },
            Some("2"),
        );
    }
    assert_eq!(thread.items.len(), 6);
}

#[test]
fn legacy_work_result_has_no_invented_tools_or_timing() {
    let old: SessionItem = serde_json::from_value(serde_json::json!({
        "kind": "tool", "text": "python {}"
    }))
    .unwrap();
    assert!(old.work.is_none());
    let mut value = serde_json::to_value(evidence_result(1)).unwrap();
    value.as_object_mut().unwrap().remove("evidence");
    value.as_object_mut().unwrap().remove("timing");
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    apply_correlated_agent_event(
        &mut thread,
        AgentEvent::WorkResult {
            result: serde_json::from_value(value).unwrap(),
        },
        Some("2"),
    );
    assert_eq!(thread.items.len(), 2);
    let text = layout_text(&evidence_layout(&[thread], true, Some("work-1")));
    assert!(!text.contains("execution "));
    assert!(!text.contains("python"));
}

#[test]
fn ten_thousand_workers_bound_rows_and_never_format_closed_worker_details() {
    let mut root = Thread::new_foreground();
    root.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    let mut threads = vec![root];
    for index in 0..10_000 {
        let mut worker = Thread::new_foreground();
        worker.id = format!("worker-{index:05}");
        worker.is_foreground = false;
        apply_correlated_agent_event(
            &mut worker,
            AgentEvent::WorkResult {
                result: evidence_result(1),
            },
            Some("2"),
        );
        // Even previously opened cells must not format when their worker closes.
        worker.items[0].hidden = false;
        threads.push(worker);
    }
    DETAIL_FORMATS.with(|count| count.set(0));
    let closed = evidence_layout(&threads, false, Some("worker-09999"));
    assert!(closed.lines.len() < 20);
    let open = evidence_layout(&threads, true, None);
    assert!(open.lines.len() < 40);
    assert_eq!(
        open.hits
            .iter()
            .filter(|hit| matches!(hit, Some(ClickTarget::Worker(..))))
            .count(),
        20
    );
    assert!(layout_text(&open).contains("9980 worker summaries omitted"));
    DETAIL_FORMATS.with(|count| assert_eq!(count.get(), 0));
    let selected = evidence_layout(&threads, true, Some("worker-09999"));
    assert!(selected.lines.len() < 100);
    assert_eq!(
        selected
            .hits
            .iter()
            .filter(|hit| matches!(hit, Some(ClickTarget::Worker(..))))
            .count(),
        20
    );
    assert!(selected
        .hits
        .contains(&Some(ClickTarget::Worker(0, "worker-09999".into()))));
    DETAIL_FORMATS.with(|count| assert_eq!(count.get(), 1));
}

#[test]
fn event_ids_are_deduplicated_per_session() {
    let mut seen = HashSet::new();
    let event = envelope(
        7,
        AgentEvent::Status {
            turn: Some(2),
            phase: "working".into(),
            message: String::new(),
        },
    );
    assert!(accept_event(&mut seen, &event));
    assert!(!accept_event(&mut seen, &event));

    let legacy = envelope(0, event.kind.clone());
    assert!(accept_event(&mut seen, &legacy));
    assert!(accept_event(&mut seen, &legacy));
}

#[test]
fn reply_projection_removes_persisted_mojibake_dsml() {
    let damaged = "Let me check those cities.\n\n<� DSML� tool_calls>\n<� DSML� invoke name=\"spawn_agents\">\n<� DSML� parameter name=\"agents\">secret";
    assert_eq!(sanitize_reply_text(damaged), "Let me check those cities.");

    let mut thread = Thread::new_foreground();
    thread.finish_reply(damaged.into(), Some("2".into()));
    assert_eq!(thread.items[0].text, "Let me check those cities.");
}

#[test]
fn reply_projection_preserves_normal_dsml_discussion() {
    assert_eq!(
        sanitize_reply_text("Explain DSML tool_calls in plain English."),
        "Explain DSML tool_calls in plain English."
    );
}

#[test]
fn reply_projection_hides_heavily_corrupted_lines() {
    assert_eq!(
        sanitize_reply_text("Weather is mild.\n� � � � � � � � � �"),
        "Weather is mild."
    );
    assert_eq!(
        sanitize_reply_text("� � � � � � � �"),
        "[model output could not be decoded]"
    );
}

#[test]
fn turn_latency_uses_user_acceptance_not_worker_start() {
    let mut thread = Thread::new_foreground();
    thread.items.push(Item {
        kind: ItemKind::User,
        work: None,
        attention: None,
        text: "request".into(),
        hidden: false,
        output: None,
        tool_id: None,
        turn: Some("2".into()),
        timestamp: 1_000,
        revision: 1,
    });
    thread.items.push(Item {
        kind: ItemKind::Spawn,
        attention: None,
        work: None,
        text: "worker".into(),
        hidden: false,
        output: None,
        tool_id: None,
        turn: Some("2".into()),
        timestamp: 4_000,
        revision: 2,
    });
    let badges = turn_badges(&thread, Some("2"), 6_000, true);
    assert!(badges.contains("5.0s"));
}

#[test]
fn turn_badges_surface_partial_structured_worker_failures() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(
        ItemKind::Spawn,
        "worker one: inspect".into(),
        Some("2".into()),
    );
    thread.add_turn(
        ItemKind::Spawn,
        "worker two: inspect".into(),
        Some("2".into()),
    );
    thread.add_turn(
        ItemKind::SpawnResult,
        "worker one: inspect\ndone".into(),
        Some("2".into()),
    );
    thread.add_turn(
        ItemKind::Error,
        "work two: inspect\nfailed".into(),
        Some("2".into()),
    );

    let badges = turn_badges(&thread, Some("2"), u64::MAX, false);
    assert!(badges.contains("󰄬 1 complete"), "{badges}");
    assert!(badges.contains("× 1 failed"), "{badges}");
}

#[test]
fn turn_badges_show_structured_latency_and_aggregate_usage() {
    let mut threads = vec![Thread::new_foreground()];
    for (event_id, kind) in [
        (
            1,
            AgentEvent::Timing {
                turn: 2,
                stage: "first_visible".into(),
                elapsed_ms: 1_250,
            },
        ),
        (
            2,
            AgentEvent::Timing {
                turn: 2,
                stage: "completed".into(),
                elapsed_ms: 4_500,
            },
        ),
        (
            3,
            AgentEvent::Usage {
                turn: Some(2),
                prompt_tokens: 1_000,
                completion_tokens: 200,
                total_tokens: 1_200,
                context_tokens: 1_000,
                context_window: Some(10_000),
            },
        ),
    ] {
        record_correlated_metrics(&mut threads, &envelope(event_id, kind));
    }
    let mut worker = envelope(
        4,
        AgentEvent::Usage {
            turn: Some(1),
            prompt_tokens: 2_000,
            completion_tokens: 300,
            total_tokens: 2_300,
            context_tokens: 2_000,
            context_window: Some(10_000),
        },
    );
    worker.actor = Actor::Worker {
        id: "worker-1".into(),
    };
    worker.session_id = "worker-1".into();
    worker.task_id = Some("assignment-1".into());
    record_correlated_metrics(&mut threads, &worker);
    record_correlated_metrics(&mut threads, &worker);

    let badges = turn_badges(&threads[0], Some("2"), 99_000, false);
    assert!(!badges.contains("󱎫"), "{badges}");
    assert!(badges.contains("󰅐 done 4.5s"), "{badges}");
    assert!(
        badges.contains(&format!("{} total 3.5k", icon::TOKENS)),
        "{badges}"
    );

    let trace_badges = turn_badges(&threads[0], Some("2"), 99_000, true);
    assert!(
        trace_badges.contains("󱎫 first response 1.2s"),
        "{trace_badges}"
    );
    assert!(
        trace_badges.contains("󰅐 response completed 4.5s"),
        "{trace_badges}"
    );
    assert!(
        trace_badges.contains(&format!("{} foreground tokens 1.2k", icon::TOKENS)),
        "{trace_badges}"
    );
    assert!(
        trace_badges.contains(&format!("{} aggregate tokens 3.5k", icon::TOKENS)),
        "{trace_badges}"
    );

    let mut compact_thread = Thread::new_foreground();
    compact_thread.metrics.insert(
        "3".into(),
        TurnMetrics {
            first_visible_ms: Some(4_000),
            completed_ms: Some(4_500),
            self_usage: Some(TokenTotals {
                prompt: 900,
                completion: 100,
                total: 1_000,
            }),
            worker_usage: HashMap::new(),
            worker_outcomes: HashMap::new(),
            memory: MemoryTurnMetrics::default(),
            schedule: ScheduleTurnMetrics::default(),
            ..TurnMetrics::default()
        },
    );
    let compact_badges = turn_badges(&compact_thread, Some("3"), 99_000, false);
    assert!(!compact_badges.contains("󱎫"), "{compact_badges}");
    assert!(compact_badges.contains("󰅐 done 4.5s"), "{compact_badges}");
    assert!(
        compact_badges.contains(&format!("{} total 1.0k", icon::TOKENS)),
        "{compact_badges}"
    );
}

#[test]
fn trace_summaries_compact_lifecycle_events() {
    assert_eq!(
        trace_summary("[timing] model_request_1_started 0ms"),
        format!("{} model request 1 · +0ms", icon::DURATION)
    );
    assert_eq!(
        trace_summary("[timing] model_request_1_completed 3220ms"),
        format!("{} model request 1 · 3.2s", icon::SUCCESS)
    );
    assert_eq!(
        trace_summary("[working] I'll check the weather"),
        format!("{} working · I'll check the weather", icon::RUNNING)
    );
}

#[test]
fn visible_late_completion_clears_its_ready_notice() {
    let mut thread = Thread::new_foreground();
    thread.items.push(Item {
        kind: ItemKind::User,
        text: "slow request".into(),
        attention: None,
        work: None,
        hidden: false,
        output: None,
        tool_id: None,
        turn: Some("2".into()),
        timestamp: 1_000,
        revision: 1,
    });
    thread.items.push(Item {
        kind: ItemKind::User,
        text: "foreground request".into(),
        attention: None,
        work: None,
        hidden: false,
        output: None,
        tool_id: None,
        turn: Some("3".into()),
        timestamp: 2_000,
        revision: 2,
    });
    thread.items.push(Item {
        kind: ItemKind::PendingReply,
        text: "Still checking that for you.".into(),
        attention: None,
        work: None,
        hidden: false,
        output: None,
        tool_id: None,
        turn: Some("3".into()),
        timestamp: 2_000,
        revision: 2,
    });
    thread.items.push(Item {
        kind: ItemKind::Reply,
        text: "late result".into(),
        attention: None,
        work: None,
        hidden: false,
        output: None,
        tool_id: None,
        turn: Some("2".into()),
        timestamp: 3_000,
        revision: 3,
    });
    thread.completed_turns.insert("2".into());
    thread.unread_turns.insert("2".into());
    let mut threads = vec![thread];
    assert_eq!(ready_earlier_turn(&threads), Some(2));
    let mut projection = TurnProjection::default();
    projection.update(&threads[0]);
    let view = TranscriptView {
        attention_hits: Vec::new(),
        total_height: 10,
        viewport: 10,
        turns: 2,
        anchor_turn: Some(0),
        starts: vec![0, 5],
        heights: vec![5, 5],
    };
    mark_visible_ready_turns_seen(
        &mut threads,
        &projection,
        &view,
        &TranscriptScroll::default(),
    );
    assert_eq!(ready_earlier_turn(&threads), None);
}
