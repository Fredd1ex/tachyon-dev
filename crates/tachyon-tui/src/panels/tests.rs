use super::*;
use crate::app::{selected_chat_cell_text, ItemKind};
use crossterm::event::KeyCode;
use ratatui::{backend::TestBackend, Terminal};

thread_local! { pub(super) static DETAIL_FORMATS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn paint(
    panel: &mut Inspector,
    threads: &[Thread],
    turn: usize,
    width: u16,
    height: u16,
) -> String {
    let mut projection = TurnProjection::default();
    projection.update(&threads[0]);
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|f| panel.draw(f, f.area(), threads, &projection, turn))
        .unwrap();
    panel
        .lines
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn joke_has_no_weather_workers_or_duplicate_bodies() {
    let mut fg = Thread::new_foreground();
    fg.add_turn(
        ItemKind::User,
        "weather question".into(),
        Some("weather".into()),
    );
    fg.add_tool("Search".into(), "a".into(), Some("weather".into()));
    fg.add_turn(ItemKind::User, "tell a joke".into(), Some("joke".into()));
    fg.finish_reply("funny answer".into(), Some("joke".into()));
    fg.add_tool("Unscoped".into(), "unknown".into(), None);
    let mut worker = Thread::new_foreground();
    worker.is_foreground = false;
    worker.task = Some("weather worker".into());
    worker.add_tool("OtherSearch".into(), "b".into(), Some("weather".into()));
    let threads = vec![fg, worker];
    let copy = selected_chat_cell_text(&threads, Some(1));
    let mut panel = Inspector::default();
    for (width, height) in [(100, 30), (40, 12), (1, 1), (2, 2), (100, 30)] {
        let text = paint(&mut panel, &threads, 1, width, height);
        assert_eq!(text, "No correlated activity for this turn.");
        assert!(panel.rows.is_empty());
        assert!(panel.area.height <= 2);
        assert_eq!(selected_chat_cell_text(&threads, Some(1)), copy);
    }
    let text = paint(&mut panel, &threads, 0, 100, 30);
    assert!(text.contains("Search"));
    assert!(!text.contains("OtherSearch"));
    assert!(!text.contains("question"));
    assert!(!text.contains("Unscoped"));
}

#[test]
fn individual_expansion_is_lazy_cached_and_mouse_owned() {
    DETAIL_FORMATS.with(|n| n.set(0));
    let mut fg = Thread::new_foreground();
    fg.add_turn(ItemKind::User, "question".into(), Some("turn".into()));
    fg.add_tool(
        "Search {\"query\":\"weather\",\"api_key\":\"credential\"}".into(),
        "a".into(),
        Some("turn".into()),
    );
    fg.add_tool("Read".into(), "b".into(), Some("turn".into()));
    fg.finish_reply("answer".into(), Some("turn".into()));
    let threads = vec![fg];
    let copy = selected_chat_cell_text(&threads, Some(0));
    let mut panel = Inspector::default();
    let text = paint(&mut panel, &threads, 0, 100, 30);
    assert!(!text.contains("started"));
    assert!(!text.contains("question"));
    assert!(!text.contains("answer"));
    DETAIL_FORMATS.with(|n| assert_eq!(n.get(), 0));
    panel.key(KeyCode::Enter);
    let text = paint(&mut panel, &threads, 0, 100, 30);
    assert!(!text.contains("credential"));
    assert_eq!(panel.expanded.len(), 1);
    for width in [40, 100, 100] {
        paint(&mut panel, &threads, 0, width, 30);
    }
    DETAIL_FORMATS.with(|n| assert_eq!(n.get(), 1));
    panel.key(KeyCode::Char(' '));
    paint(&mut panel, &threads, 0, 100, 30);
    assert!(panel.expanded.is_empty());
    let y = panel
        .hits
        .iter()
        .position(|hit| *hit == Some((0, 2)))
        .unwrap();
    panel.click(panel.area.x, panel.area.y + y as u16);
    paint(&mut panel, &threads, 0, 100, 30);
    assert_eq!(panel.expanded.len(), 1);
    assert!(panel.expanded.contains(&(0, 2)));
    panel.key(KeyCode::Char('d'));
    assert!(paint(&mut panel, &threads, 0, 100, 30).contains("foreground tokens"));
    assert_eq!(selected_chat_cell_text(&threads, Some(0)), copy);
}

#[test]
fn assignments_and_overlapping_turns_keep_evidence_and_status_local() {
    use crate::app::apply_correlated_agent_event;
    use tachyon_api::types::AgentEvent;
    let mut fg = Thread::new_foreground();
    for turn in ["weather", "joke"] {
        fg.add_turn(ItemKind::User, format!("prompt {turn}"), Some(turn.into()));
    }
    for (turn, assignment, task, outcome) in [
        ("weather", 1, "Old forecast", "completed"),
        ("joke", 2, "Other task", "failed"),
        ("weather", 3, "New forecast", "completed"),
    ] {
        let result = serde_json::from_value(serde_json::json!({
            "work_id": "reused-work", "objective": task, "generation": 1,
            "assignment": assignment, "outcome": outcome, "result": "Weather result",
            "message": "failure details",
            "evidence": {"omitted": 0, "tools": [{"tool_name": "Search", "call_id": "reused-call",
                "arguments": {"query": "London", "authorization": "DO_NOT_SHOW"},
                "output": {"sources": [{"title": "Forecast", "url": "https://example.com/weather?private=HIDDEN"}],
                    "content": "RAW_ONLY", "exit_code": 1}
            }]}
        })).unwrap();
        apply_correlated_agent_event(&mut fg, AgentEvent::WorkResult { result }, Some(turn));
    }
    fg.finish_reply("assistant body".into(), Some("weather".into()));
    // Model the archive boundary without mutating persisted evidence.
    fg.history_len = fg.items.len();
    let threads = vec![fg];
    let mut panel = Inspector::default();
    let text = paint(&mut panel, &threads, 0, 100, 30);
    assert_eq!(panel.rows.len(), 4);
    assert!(text.contains(&format!(
        "Old forecast / {} Search - error",
        crate::app::icon::SEARCH
    )));
    assert!(text.contains("New forecast - complete"));
    assert!(!text.contains("Other task"));
    assert!(!text.contains("reused-work"));
    assert!(!text.contains("assistant body"));
    panel.key(KeyCode::Enter);
    let text = paint(&mut panel, &threads, 0, 100, 30);
    assert!(text.contains("https://example.com/weather"));
    assert!(!text.contains("Arguments:"));
    assert!(!text.contains("HIDDEN"));
    assert!(!text.contains("DO_NOT_SHOW"));
    assert!(!text.contains("RAW_ONLY"));
    panel.key(KeyCode::Char('r'));
    let text = paint(&mut panel, &threads, 0, 100, 30);
    assert!(text.contains("RAW_ONLY"));
    assert!(text.contains("assignment 1"));
    assert!(!text.contains("DO_NOT_SHOW"));
    let text = paint(&mut panel, &threads, 1, 100, 30);
    assert!(text.contains("Other task - failed"));
    assert!(!text.contains("forecast"));
    assert!(panel.expanded.is_empty());
    let rows = activity::tasks(&threads, 0, Some("weather"));
    assert_eq!(rows.len(), 1);
    panel.open(Some(rows[0].id));
    let text = paint(&mut panel, &threads, 0, 100, 30);
    assert!(text.contains("New forecast"));
    assert!(text.contains("Search - error"));
    assert!(text.contains("sources:"));
    for forbidden in [
        "Old forecast",
        "Other task",
        "RAW_ONLY",
        "DO_NOT_SHOW",
        "assistant body",
    ] {
        assert!(!text.contains(forbidden), "{forbidden}: {text}");
    }
    assert_eq!(panel.rows.len(), 2);
    panel.key(KeyCode::Enter);
    paint(&mut panel, &threads, 0, 100, 30);
    assert_eq!(panel.expanded.len(), 1);
}

#[test]
fn detail_wrap_is_cell_bounded_and_never_splits_wide_glyphs() {
    for width in [1, 2, 10, 80] {
        for row in activity::wrap("wide \u{754c} and narrow\nsecond line", width) {
            assert!(Line::raw(row).width() <= width as usize);
        }
    }
}

#[test]
fn explicit_details_float_without_expanding_the_answer() {
    use crate::app::{build_turn_cells, transcript::layout::inline_cell_layout};
    let mut fg = Thread::new_foreground();
    fg.add_turn(ItemKind::User, "question".into(), Some("turn".into()));
    fg.add_tool(
        "websearch {\"query\":\"PRIVATE_ARGS\"}".into(),
        "call".into(),
        Some("turn".into()),
    );
    fg.add_tool_result(
        "call".into(),
        serde_json::json!({"results": "long result ".repeat(100)}).to_string(),
        Some("turn".into()),
    );
    fg.finish_reply("The answer remains.".into(), Some("turn".into()));
    let threads = vec![fg];
    let cells = build_turn_cells(&threads[0]);
    let rows = activity::tasks(&threads, 0, Some("turn"));
    for width in [12, 24, 92] {
        let layout = inline_cell_layout(0, 0, &threads, &cells[0], width, 0, false, "", true, None);
        assert!(!layout
            .lines
            .iter()
            .any(|line| line.to_string().contains("long result")));
        let mut inspector = Inspector::default();
        inspector.open(Some(rows[0].id));
        let mut projection = TurnProjection::default();
        projection.update(&threads[0]);
        let mut terminal = Terminal::new(TestBackend::new(width, 20)).unwrap();
        terminal
            .draw(|f| inspector.draw(f, f.area(), &threads, &projection, 0))
            .unwrap();
        assert_eq!(inspector.rows.len(), 1);
        assert!(inspector.expanded.contains(&rows[0].id));
        assert_eq!(layout.lines.len(), layout.hits.len());
        let text = layout
            .lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(!text.contains("long result"));
        assert!(!text.contains("PRIVATE_ARGS"));
        assert!(!text.contains("Arguments:"));
        assert!(text.contains("answer"));
    }
}

#[test]
fn inline_live_tool_results_are_scoped_lazy_and_not_a_modal() {
    use crate::app::{build_turn_cells, elapsed, transcript::layout::inline_cell_layout};
    let mut fg = Thread::new_foreground();
    fg.add_turn(
        ItemKind::User,
        "weather question".into(),
        Some("weather".into()),
    );
    fg.add_turn(
        ItemKind::PendingReply,
        "Checking weather.".into(),
        Some("weather".into()),
    );
    fg.add_tool(
        "websearch {\"query\":\"London\"}".into(),
        "call".into(),
        Some("weather".into()),
    );
    let mut threads = vec![fg];
    let render = |threads: &[Thread], turn, width, open| {
        let cells = build_turn_cells(&threads[0]);
        let layout = inline_cell_layout(
            0,
            turn,
            threads,
            &cells[turn],
            width,
            0,
            false,
            "",
            open,
            None,
        );
        let mut terminal = Terminal::new(TestBackend::new(width, 100)).unwrap();
        let lines = (0..layout.lines.len())
            .map(|row| elapsed::overlay(&layout, row, width, 20_000))
            .collect::<Vec<_>>();
        terminal
            .draw(|f| f.render_widget(ratatui::widgets::Paragraph::new(lines), f.area()))
            .unwrap();
        terminal
            .backend()
            .buffer()
            .content
            .chunks(width as usize)
            .map(|row| {
                row.iter()
                    .map(|c| c.symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    DETAIL_FORMATS.with(|n| n.set(0));
    let text = render(&threads, 0, 100, false);
    assert!(text
        .lines()
        .any(|line| line.trim_start().starts_with("websearch") && line.ends_with("started")));
    assert!(text.find("websearch").unwrap() < text.find("Checking weather.").unwrap());
    assert_eq!(text.matches("Checking weather.").count(), 1);
    assert!(!text.contains("ACTIVITY"));
    assert!(!text.contains("Arguments"));
    assert!(!text.contains("TODO"));
    DETAIL_FORMATS.with(|n| assert_eq!(n.get(), 0));
    threads[0].add_tool_result(
        "call".into(),
        r#"{"sources":[{"title":"Forecast","url":"https://example.com?secret=hidden"}]}"#.into(),
        Some("weather".into()),
    );
    let text = render(&threads, 0, 100, true);
    assert!(text.contains("result recorded"));
    assert!(!text.contains("sources:"));
    assert!(!text.contains("hidden"));
    assert!(!text.contains("Arguments:"));
    threads[0].add_turn(ItemKind::User, "joke please".into(), Some("joke".into()));
    threads[0].finish_reply("A funny answer.".into(), Some("joke".into()));
    for width in [1, 2, 24, 40, 100] {
        let text = render(&threads, 1, width, true);
        assert!(!text.contains("websearch"));
        assert!(!text.contains("Forecast"));
        assert!(!text.contains("TODO"));
    }
}

#[test]
fn task_cards_are_two_lines_without_fabricated_live_timing() {
    use crate::app::{
        build_turn_cells, elapsed, transcript::layout::inline_cell_layout, ClickTarget,
    };
    let mut fg = Thread::new_foreground();
    fg.add_turn(ItemKind::User, "question".into(), Some("turn".into()));
    fg.add_turn(
        ItemKind::PendingReply,
        "Checking.".into(),
        Some("turn".into()),
    );
    fg.add_turn(
        ItemKind::Spawn,
        "worker researcher: Check forecast".into(),
        Some("turn".into()),
    );
    fg.items.last_mut().unwrap().timestamp = 10_000;
    let mut threads = vec![fg];
    let cells = build_turn_cells(&threads[0]);
    let layout = inline_cell_layout(0, 0, &threads, &cells[0], 100, 0, true, "", false, None);
    let text = |layout: &crate::app::CellLayout, now| {
        (0..layout.lines.len())
            .map(|row| elapsed::overlay(layout, row, 100, now).to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert!(text(&layout, 12_000).contains("Check forecast"));
    assert!(text(&layout, 15_000).contains("-> started"));
    assert_eq!(
        layout
            .hits
            .iter()
            .filter(|h| **h == Some(ClickTarget::Item(0, 2)))
            .count(),
        2
    );
    assert!(layout.hits.contains(&Some(ClickTarget::Item(0, 2))));
    threads[0].finish_reply("Answer.".into(), Some("turn".into()));
    threads[0]
        .metrics
        .entry("turn".into())
        .or_default()
        .ended_at_ms = Some(16_000);
    let cells = build_turn_cells(&threads[0]);
    let layout = inline_cell_layout(0, 0, &threads, &cells[0], 100, 0, false, "", false, None);
    assert_eq!(text(&layout, 20_000), text(&layout, 200_000));
    assert!(!text(&layout, 20_000).contains("-> started"));
    assert!(!text(&layout, 20_000).contains("view subagent"));
}

// Reconstructs the reported three-city clutter using the real event reducer:
// starts, long objectives, repeated nested calls, and authoritative host results.
#[test]
fn three_city_activity_is_bounded_merged_and_final_collapsed() {
    use crate::app::{
        apply_correlated_agent_event, build_turn_cells, transcript::layout::inline_cell_layout,
    };
    use tachyon_api::types::AgentEvent;
    let mut fg = Thread::new_foreground();
    fg.add_turn(
        ItemKind::User,
        "Compare three cities".into(),
        Some("cities".into()),
    );
    fg.add_turn(
        ItemKind::PendingReply,
        "Checking the cities.".into(),
        Some("cities".into()),
    );
    fg.add_tool(
        "spawn_agents {}".into(),
        "spawn-call".into(),
        Some("cities".into()),
    );
    let mut workers = Vec::new();
    for (index, city) in ["London", "Paris", "Tokyo"].into_iter().enumerate() {
        let id = format!("opaque-worker-{index}");
        let objective = format!(
            "Compare {city}; {}\nReturn all observations with sources.",
            "Follow these detailed instructions. ".repeat(40)
        );
        for _ in 0..2 {
            apply_correlated_agent_event(
                &mut fg,
                AgentEvent::WorkerStarted {
                    turn: None,
                    worker_id: id.clone(),
                    objective: objective.clone(),
                },
                Some("cities"),
            );
        }
        let result = serde_json::from_value(serde_json::json!({
            "work_id": format!("opaque-work-{index}"), "objective": objective, "generation": 2, "assignment": 1,
            "outcome": "completed", "result": "Recorded observations",
            "timing": {"execution_ms": 2300},
            "evidence": {"omitted": 0, "tools": [
                {"tool_name": "websearch", "call_id": "reused", "arguments": {"query": city}, "output": {"content": "RAW_ONLY"}},
                {"tool_name": "webfetch", "call_id": "nested", "parent_call_id": "reused", "arguments": {}, "output": {"content": "RAW_ONLY"}}
            ]}
        })).unwrap();
        let mut worker = Thread::new_foreground();
        worker.id = id;
        worker.is_foreground = false;
        worker.task = Some("Mutable unrelated assignment".into());
        apply_correlated_agent_event(
            &mut worker,
            AgentEvent::WorkResult { result },
            Some("cities"),
        );
        workers.push(worker);
    }
    // A late older assignment must not supersede current terminal truth.
    let old = serde_json::from_value(serde_json::json!({
        "work_id": "opaque-work-0", "objective": "Stale assignment", "generation": 1,
        "assignment": 99, "outcome": "failed", "message": "old failure"
    }))
    .unwrap();
    apply_correlated_agent_event(
        &mut workers[0],
        AgentEvent::WorkResult { result: old },
        Some("cities"),
    );
    let foreign = serde_json::from_value(serde_json::json!({
        "work_id": "other-work", "objective": "Foreign turn", "generation": 999,
        "assignment": 1, "outcome": "completed", "result": "Do not leak"
    }))
    .unwrap();
    apply_correlated_agent_event(
        &mut workers[0],
        AgentEvent::WorkResult { result: foreign },
        Some("other"),
    );
    let mut threads = vec![fg];
    threads.extend(workers);
    DETAIL_FORMATS.with(|n| n.set(0));
    let rows = activity::tasks(&threads, 0, Some("cities"));
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row.label.ends_with(" - complete")));
    assert_eq!(
        activity::compact_row(&threads, &rows[0], 40),
        "Compare London             complete 2.3s"
    );
    for width in [0, 1, 2, 12, 24, 40, 100] {
        let cells = build_turn_cells(&threads[0]);
        let layout = inline_cell_layout(0, 0, &threads, &cells[0], width, 0, true, "", false, None);
        let at = layout.activity_row.unwrap();
        for line in &layout.lines[at..at + 6] {
            assert!(line.width() <= width as usize, "{width}: {line}");
        }
        assert!(layout.hits.contains(&Some(crate::app::ClickTarget::Item(
            rows[0].id.0,
            rows[0].id.1
        ))));
        let text = layout
            .lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        for forbidden in [
            "opaque-worker",
            "opaque-work",
            "spawn_agents",
            "webfetch",
            "websearch",
            "view subagent",
            "[truncated]",
            "outcome unknown",
            "RAW_ONLY",
            "Stale assignment",
            "Foreign turn",
            "Mutable unrelated assignment",
        ] {
            assert!(!text.contains(forbidden), "{forbidden}: {text}");
        }
    }
    DETAIL_FORMATS.with(|n| assert_eq!(n.get(), 0));
    threads[0].finish_reply("Three-city answer.".into(), Some("cities".into()));
    let copy = selected_chat_cell_text(&threads, Some(0));
    let cells = build_turn_cells(&threads[0]);
    let collapsed = inline_cell_layout(0, 0, &threads, &cells[0], 100, 0, false, "", false, None);
    let text = collapsed
        .lines
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!text.contains("Ctrl+O details"));
    assert!(text.contains("Compare London"));
    DETAIL_FORMATS.with(|n| assert_eq!(n.get(), 0));
    let expanded = inline_cell_layout(0, 0, &threads, &cells[0], 100, 0, false, "", true, None);
    assert_eq!(expanded.lines, collapsed.lines);
    let expanded_text = expanded
        .lines
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    for city in ["London", "Paris", "Tokyo"] {
        assert_eq!(expanded_text.matches(&format!("Compare {city}")).count(), 1);
    }
    for forbidden in [
        "outcome unknown",
        "Stale assignment",
        "Foreign turn",
        "RAW_ONLY",
        "webfetch",
    ] {
        assert!(!expanded_text.contains(forbidden));
    }
    DETAIL_FORMATS.with(|n| assert_eq!(n.get(), 0));
    assert_eq!(selected_chat_cell_text(&threads, Some(0)), copy);
}

#[test]
fn compact_tasks_have_explicit_overflow_and_exact_turn_identity() {
    use crate::app::{build_turn_cells, transcript::layout::inline_cell_layout};
    let mut fg = Thread::new_foreground();
    fg.add_turn(ItemKind::User, "question".into(), Some("turn".into()));
    fg.add_turn(
        ItemKind::PendingReply,
        "Checking.".into(),
        Some("turn".into()),
    );
    for id in ["a", "aa", "b", "c", "d", "e", "f", "g"] {
        fg.add_turn(
            ItemKind::Spawn,
            format!("worker {id}: Task {id}"),
            Some("turn".into()),
        );
    }
    fg.add_turn(
        ItemKind::SpawnResult,
        "worker a: Finished task\nresult".into(),
        Some("turn".into()),
    );
    fg.add_turn(
        ItemKind::SpawnResult,
        "worker aa: Other turn\nresult".into(),
        Some("other".into()),
    );
    let mut unrelated = Thread::new_foreground();
    unrelated.add_turn(
        ItemKind::Spawn,
        "worker foreign: Foreign conversation".into(),
        Some("turn".into()),
    );
    let threads = vec![fg, unrelated];
    let rows = activity::tasks(&threads, 0, Some("turn"));
    assert_eq!(rows.len(), 8);
    assert_eq!(
        rows.iter()
            .filter(|r| r.label.ends_with(" - complete"))
            .count(),
        1
    );
    assert!(rows
        .iter()
        .any(|r| r.label.contains("Task aa") && r.label.ends_with(" - started")));
    let cells = build_turn_cells(&threads[0]);
    let layout = inline_cell_layout(0, 0, &threads, &cells[0], 80, 0, true, "", false, None);
    let at = layout.activity_row.unwrap();
    assert!(layout.lines[at + 6].to_string().contains("+5 more"));
    assert!(!layout.lines[at + 7].to_string().contains("Task"));
    DETAIL_FORMATS.with(|n| n.set(0));
    let expanded = inline_cell_layout(0, 0, &threads, &cells[0], 80, 0, true, "", true, None);
    DETAIL_FORMATS.with(|n| assert_eq!(n.get(), 0));
    assert_eq!(
        expanded
            .hits
            .iter()
            .filter(|hit| matches!(hit, Some(crate::app::ClickTarget::Item(..))))
            .count(),
        6
    );
    for width in 0..80 {
        for text in [
            "界界界\nemoji 🦀 task",
            "first\tsecond",
            "a\u{301} long title",
        ] {
            let line = activity::aligned_row(text, "complete 2.3s", width);
            assert!(Line::raw(&line).width() <= width);
            assert!(!line.contains(['\n', '\t']));
        }
    }
}
