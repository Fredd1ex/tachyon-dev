use super::*;
use ratatui::{backend::TestBackend, Terminal};

fn fixture(text: &str) -> Vec<Thread> {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
    thread.add_turn(ItemKind::PendingReply, text.into(), Some("2".into()));
    thread.metrics.entry("2".into()).or_default().accepted_at_ms = Some(10_000);
    vec![thread]
}

// Render through a terminal buffer, not just the intermediate Line strings.
fn screen(threads: &[Thread], width: u16) -> (String, ratatui::buffer::Buffer) {
    let cells = build_turn_cells(&threads[0]);
    let layout = turn_cell_layout(0, 0, threads, &cells[0], width, 0, true, "", false, None);
    let mut terminal = Terminal::new(TestBackend::new(width, layout.lines.len() as u16)).unwrap();
    terminal
        .draw(|f| {
            let lines = (0..layout.lines.len())
                .map(|row| elapsed::overlay(&layout, row, width, 22_000))
                .collect::<Vec<_>>();
            f.render_widget(Paragraph::new(lines), f.area());
        })
        .unwrap();
    let buffer = terminal.backend().buffer().clone();
    // The assistant header is row 4; exclude its wall-clock timestamp from goldens.
    let text = (5..buffer.area.height)
        .map(|y| {
            (0..width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n");
    (text, buffer)
}

#[test]
fn pending_screen_goldens_are_normal_chat_text() {
    for (message, narrow, wide) in [
        ("Working on that.", "Working on that.", "Working on that."),
        (
            "I am checking the sources before answering your question.",
            "I am checking the sources before\n    answering your question.",
            "I am checking the sources before answering your question.",
        ),
    ] {
        let threads = fixture(message);
        for (width, body) in [(40, narrow), (100, wide), (40, narrow)] {
            let (text, buffer) = screen(&threads, width);
            assert_eq!(text, format!("\n    elapsed 12s\n\n    {body}\n"));
            assert!(!text.contains("󰔟"));
            for y in 8..buffer.area.height {
                for x in 0..width {
                    assert!(!buffer[(x, y)].modifier.contains(Modifier::ITALIC));
                    assert_eq!(buffer[(x, y)].bg, Color::Reset);
                }
            }
        }
    }
}

#[test]
fn tool_badges_screen_goldens_resize_stream_and_complete_in_place() {
    let mut threads = fixture("I am checking the sources.");
    for id in ["raw-worker-a", "raw-worker-b"] {
        threads[0].add_turn(
            ItemKind::Spawn,
            format!("worker {id}: research"),
            Some("2".into()),
        );
    }
    for (index, name) in [
        "Search", "Search", "Fetch", "Read", "Read", "Write", "Write",
    ]
    .into_iter()
    .enumerate()
    {
        threads[0].activity.record(&EventEnvelope {
            event_id: index as u64,
            session_id: "session".into(),
            conversation_id: None,
            turn_id: Some("2".into()),
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: Actor::Worker {
                id: format!("raw-worker-{index}"),
            },
            sequence: index as u64,
            occurred_at_ms: 11_000,
            kind: AgentEvent::ToolStarted {
                turn: Some(2),
                id: "call".into(),
                name: name.into(),
                arguments: "secret".into(),
                identity: None,
            },
        });
    }
    let slot = threads[0]
        .items
        .iter()
        .position(|i| i.kind == ItemKind::PendingReply)
        .unwrap();
    for streaming in [false, true] {
        if streaming {
            threads[0].add_reply_fragment(
                "A **streaming** answer.".into(),
                Some("2".into()),
                false,
            );
        }
        for width in [100, 40, 24, 100] {
            let (text, _) = screen(&threads, width);
            let badge = text.lines().nth(1).unwrap();
            let expected = match width {
                100 => "    󰚩 2 agents · elapsed 12s · 7 tools",
                40 => "    󰚩 2 agents · elapsed 12s · 7 tools",
                _ => "    elapsed 12s",
            };
            assert_eq!(badge, expected);
            assert!(!text.contains("raw-worker"));
            assert!(!text.contains("secret"));
            assert!(!text.contains('|'));
            assert!(
                text.find("elapsed").unwrap()
                    < text
                        .find(if streaming { "streaming" } else { "checking" })
                        .unwrap()
            );
        }
    }
    threads[0].finish_reply("The final answer.".into(), Some("2".into()));
    threads[0].metrics.get_mut("2").unwrap().completed_ms = Some(14_000);
    assert_eq!(threads[0].items[slot].kind, ItemKind::Reply);
    let copy = selected_chat_cell_text(&threads, Some(0));
    for width in [100, 40, 24, 100] {
        let (text, _) = screen(&threads, width);
        assert!(!text.contains("Search"));
        assert!(!text.contains("tools"));
        assert!(!text.contains("elapsed"));
        if width == 100 {
            assert_eq!(
                text,
                "\n    󰚩 2 agents · 󰅐 done 14.0s\n\n    The final answer.\n"
            );
        }
        assert_eq!(selected_chat_cell_text(&threads, Some(0)), copy);
    }
}
