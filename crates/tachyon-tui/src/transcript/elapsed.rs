use super::{
    format_duration, now_seconds, projected_turn, response, truncate_text, turn_cell_badges,
    worker_record, CellLayout, ItemKind, Thread, TurnCell, WorkerTrace,
};
use ratatui::{
    style::{Color, Style},
    text::{Line, Span},
};
use std::time::{Duration, Instant};
use tachyon_api::types::{Actor, AgentEvent, EventEnvelope};

// Epoch milliseconds at startup, advanced monotonically thereafter. Host timestamps
// and observed Spawn timestamps use milliseconds, unlike AgentInfo lifetime fields.
pub(super) fn frame_ms() -> u64 {
    static CLOCK: std::sync::OnceLock<(u64, Instant)> = std::sync::OnceLock::new();
    let (epoch, instant) = CLOCK.get_or_init(|| (now_seconds(), Instant::now()));
    epoch.saturating_add(instant.elapsed().as_millis().min(u64::MAX as u128) as u64)
}

#[derive(Clone)]
pub(super) struct Badge {
    row: usize,
    start: u64,
    end: Option<u64>,
    label: &'static str,
    activity: String,
    metadata: Option<String>,
    task_title: Option<String>,
}

pub(super) fn shift_rows(layout: &mut CellLayout, count: usize) {
    for timer in &mut layout.timers {
        timer.row += count;
    }
}

pub(super) fn inline_badge(
    layout: &mut CellLayout,
    row: usize,
    start: u64,
    end: Option<u64>,
    activity: String,
    metadata: String,
    label: &'static str,
) {
    layout.timers.push(Badge {
        row,
        start,
        end,
        label,
        activity,
        metadata: Some(metadata),
        task_title: None,
    });
}

pub(super) fn task_badge(layout: &mut CellLayout, row: usize, start: u64, title: String) {
    inline_badge(
        layout,
        row,
        start,
        None,
        "started".into(),
        String::new(),
        "elapsed",
    );
    layout.timers.last_mut().unwrap().task_title = Some(title);
}

pub(super) fn record(thread: &mut Thread, envelope: &EventEnvelope) {
    let turn = match &envelope.kind {
        AgentEvent::Timing { turn, .. } => projected_turn(Some(*turn), envelope.turn_id.as_deref()),
        AgentEvent::WorkerStarted { turn, .. } => {
            projected_turn(*turn, envelope.turn_id.as_deref())
        }
        _ => envelope.turn_id.clone(),
    };
    let Some(turn) = turn else { return };
    let metrics = thread.metrics.entry(turn.clone()).or_default();
    let at = envelope.occurred_at_ms;
    match &envelope.kind {
        AgentEvent::Timing {
            stage, elapsed_ms, ..
        } if matches!(envelope.actor, Actor::Foreground) => {
            if stage == "input_accepted" {
                metrics
                    .accepted_at_ms
                    .get_or_insert(at.saturating_sub(*elapsed_ms));
            } else if stage == "completed" {
                metrics.ended_at_ms.get_or_insert(at);
            }
        }
        AgentEvent::Reply { .. } | AgentEvent::Error { .. }
            if matches!(envelope.actor, Actor::Foreground) =>
        {
            metrics.ended_at_ms.get_or_insert(at);
        }
        AgentEvent::WorkerStarted { worker_id, .. } => {
            metrics
                .worker_started_at_ms
                .entry(worker_id.clone())
                .or_insert(at);
        }
        AgentEvent::WorkerCompleted { worker_id, .. } => {
            metrics
                .worker_ended_at_ms
                .entry(worker_id.clone())
                .or_insert(at);
        }
        AgentEvent::WorkResult { result } => {
            metrics
                .worker_ended_at_ms
                .entry(result.work_id.clone())
                .or_insert(at);
            if let Actor::Worker { id } = &envelope.actor {
                metrics.worker_ended_at_ms.entry(id.clone()).or_insert(at);
            }
        }
        _ => return,
    }
    thread.metric_revisions.insert(turn, thread.revision);
}

fn live(thread: &Thread, cell: &TurnCell) -> bool {
    cell.prompt >= thread.history_len
        && thread.items[cell.prompt]
            .turn
            .as_deref()
            .is_some_and(|turn| !turn.starts_with("visit:") && !turn.starts_with("archived:"))
}

fn checkpoint(thread: &Thread, cell: &TurnCell) -> u64 {
    cell.items
        .iter()
        .map(|index| thread.items[*index].timestamp)
        .max()
        .unwrap_or(0)
}

pub(super) fn main_start(thread: &Thread, cell: &TurnCell) -> Option<u64> {
    let prompt = &thread.items[cell.prompt];
    if prompt.kind != ItemKind::User || !live(thread, cell) {
        return None;
    }
    let metrics = prompt
        .turn
        .as_ref()
        .and_then(|turn| thread.metrics.get(turn));
    let start = metrics.and_then(|m| m.accepted_at_ms)?;
    if metrics.is_some_and(|m| m.ended_at_ms.is_some() || m.completed_ms.is_some())
        || prompt
            .turn
            .as_ref()
            .is_some_and(|turn| thread.completed_turns.contains(turn))
        || cell.items.iter().any(|index| {
            let item = &thread.items[*index];
            item.kind == ItemKind::Error
                && item.work.is_none()
                && !item.text.starts_with("work ")
                && !item.text.starts_with("worker ")
        })
    {
        return None;
    }
    Some(start)
}

pub(super) fn main_badge(layout: &mut CellLayout, thread: &Thread, cell: &TurnCell, row: usize) {
    let Some(start) = main_start(thread, cell) else {
        return;
    };
    let prompt = &thread.items[cell.prompt];
    layout.timers.push(Badge {
        row,
        start,
        end: None,
        label: "elapsed",
        activity: prompt
            .turn
            .as_deref()
            .map(|turn| thread.activity.compact_summary(turn))
            .unwrap_or_default(),
        metadata: Some(turn_cell_badges(thread, cell)),
        task_title: None,
    });
}

pub(super) fn worker_badge(
    layout: &mut CellLayout,
    thread: &Thread,
    cell: &TurnCell,
    worker: &WorkerTrace,
    threads: &[Thread],
) {
    let metrics = thread.items[cell.prompt]
        .turn
        .as_ref()
        .and_then(|turn| thread.metrics.get(turn));
    let start = metrics
        .and_then(|m| m.worker_started_at_ms.get(&worker.id))
        .copied()
        .or_else(|| {
            cell.items
                .iter()
                .map(|i| &thread.items[*i])
                .filter(|item| {
                    item.kind == ItemKind::Spawn
                        && worker_record(&item.text).is_some_and(|(id, _)| id == worker.id)
                })
                .map(|item| item.timestamp)
                .min()
        });
    let terminal = worker
        .items
        .iter()
        .map(|(t, i)| &threads[*t].items[*i])
        .filter(|item| {
            matches!(
                item.kind,
                ItemKind::SpawnResult | ItemKind::Error | ItemKind::Reply
            )
        })
        .collect::<Vec<_>>();
    let execution = terminal
        .iter()
        .filter_map(|item| item.work.as_ref()?.timing.as_ref()?.execution_ms)
        .last();
    let (start, end, label) = if let Some(duration) = execution {
        (0, Some(duration), "execution")
    } else {
        let Some(start) = start else { return };
        let end = metrics
            .and_then(|m| m.worker_ended_at_ms.get(&worker.id))
            .copied()
            .or_else(|| terminal.iter().map(|item| item.timestamp).max())
            .or_else(|| metrics.and_then(|m| m.ended_at_ms))
            .or_else(|| {
                let completed = thread.items[cell.prompt]
                    .turn
                    .as_ref()
                    .is_some_and(|turn| thread.completed_turns.contains(turn));
                (!live(thread, cell) || completed).then(|| checkpoint(thread, cell))
            });
        (start, end, "elapsed")
    };
    layout.timers.push(Badge {
        row: layout.lines.len() - 1,
        start,
        end,
        label,
        metadata: None,
        task_title: None,
        // A reused worker can have a new fenced assignment while the trace
        // retains the previous assignment's frozen timing.
        activity: if live(thread, cell)
            && !metrics.is_some_and(|m| m.ended_at_ms.is_some())
            && !thread.items[cell.prompt]
                .turn
                .as_ref()
                .is_some_and(|turn| thread.completed_turns.contains(turn))
        {
            thread.items[cell.prompt]
                .turn
                .as_deref()
                .map(|turn| thread.activity.summary(turn, Some(&worker.id)))
                .unwrap_or_default()
        } else {
            String::new()
        },
    });
}

pub(super) fn overlay(layout: &CellLayout, row: usize, width: u16, now_ms: u64) -> Line<'static> {
    let Some(timer) = layout.timers.iter().find(|timer| timer.row == row) else {
        return layout.lines[row].clone();
    };
    // Whole seconds only, with a bounded representation even for ancient timestamps.
    let seconds = timer.end.unwrap_or(now_ms).saturating_sub(timer.start) / 1000;
    let duration = if seconds >= 360_000 {
        "99h+".into()
    } else {
        format_duration(Duration::from_secs(seconds))
    };
    let suffix = format!(" {} {duration}", timer.label);
    if let Some(title) = &timer.task_title {
        let indent = if width >= 8 { "    " } else { "" };
        let text = crate::app::panels::activity::aligned_row(
            title,
            &format!("{}{suffix}", timer.activity),
            width as usize - indent.len(),
        );
        return Line::styled(
            format!("{indent}{text}"),
            Style::default().fg(Color::DarkGray),
        );
    }
    if let Some(metadata) = &timer.metadata {
        let rail = layout.lines[row]
            .spans
            .first()
            .filter(|span| span.content == "│ ");
        let mut line = response::badge_line(
            metadata,
            &timer.activity,
            Some(suffix.trim()),
            width.saturating_sub(if rail.is_some() { 2 } else { 0 }),
        );
        if let Some(rail) = rail.filter(|_| width >= 2) {
            line.spans.insert(0, rail.clone());
        }
        return line;
    }
    let suffix = truncate_text(&suffix, width as usize);
    let budget = (width as usize).saturating_sub(suffix.len());
    let mut line = layout.lines[row].clone();
    if !timer.activity.is_empty() {
        line.spans.push(Span::styled(
            format!(" | {}", timer.activity),
            Style::default().fg(Color::Cyan),
        ));
    }
    let mut remaining = budget;
    for span in &mut line.spans {
        let mut text = String::new();
        for ch in span.content.chars() {
            let width = Span::raw(ch.to_string()).width();
            if width > remaining {
                break;
            }
            remaining -= width;
            text.push(ch);
        }
        span.content = text.into();
    }
    line.spans
        .push(Span::styled(suffix, Style::default().fg(Color::DarkGray)));
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::*;

    fn accept(thread: &mut Thread, turn: &str, at: u64) {
        let mut metadata = tachyon_api::InteractionMetadata::new("event", "request", "test", 1);
        metadata.turn_id = Some(turn.into());
        metadata.occurred_at_ms = at;
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

    fn layout(threads: &[Thread], index: usize) -> CellLayout {
        let cells = build_turn_cells(&threads[0]);
        turn_cell_layout(
            0,
            index,
            threads,
            &cells[index],
            80,
            0,
            true,
            "",
            true,
            None,
        )
    }

    fn text(layout: &CellLayout, at: u64) -> String {
        (0..layout.lines.len())
            .map(|row| overlay(layout, row, 80, at).to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn pending_without_ack_ticks_only_badges_without_rebuilding_history() {
        let mut threads = vec![Thread::new_foreground()];
        accept(&mut threads[0], "conversation:old:2", 1_000);
        threads[0].history_len = threads[0].items.len();
        accept(&mut threads[0], "conversation:new:2", 10_000);
        let mut projection = TurnProjection::default();
        projection.update(&threads[0]);
        let mut cache = TurnLayoutCache::default();
        cache.prepare(80, &projection.cells, threads[0].structure_revision);
        let mut before = Vec::new();
        for (index, cell) in projection.cells.iter().enumerate() {
            let cached = cache.layout(cell_key(cell), cell_revision(&threads[0], cell), 0, || {
                layout(&threads, index)
            });
            before.push(text(cached, 22_000));
        }
        assert!(before[1].contains("elapsed 12s"));
        assert!(!before[1].contains("working"));
        assert_eq!(cache.builds, 2);
        for (index, cell) in projection.cells.iter().enumerate() {
            let cached = cache.layout(cell_key(cell), cell_revision(&threads[0], cell), 0, || {
                panic!("timer rebuilt layout")
            });
            assert_eq!(text(cached, 22_999), before[index]);
            let after = text(cached, 82_000);
            assert_eq!(after.lines().count(), before[index].lines().count());
            if index == 0 {
                assert_eq!(after, before[index]);
            } else {
                assert!(after.contains("elapsed 1m 12s"));
                for row in 0..cached.lines.len() {
                    if !cached.timers.iter().any(|timer| timer.row == row) {
                        assert_eq!(
                            overlay(cached, row, 80, 22_000),
                            overlay(cached, row, 80, 82_000)
                        );
                    }
                }
            }
        }
        assert_eq!(cache.builds, 2);
        assert!(!projection.update(&threads[0]));
    }

    #[test]
    fn partial_completion_keeps_turn_timer_running() {
        let mut threads = vec![Thread::new_foreground()];
        accept(&mut threads[0], "2", 10_000);
        for id in ["a", "b", "c"] {
            threads[0].add_turn(
                ItemKind::Spawn,
                format!("worker {id}: task"),
                Some("2".into()),
            );
        }
        threads[0].add_turn(
            ItemKind::SpawnResult,
            "worker a: task\ndone".into(),
            Some("2".into()),
        );
        let layout = layout(&threads, 0);
        assert!(text(&layout, 22_000).contains("1 complete · elapsed 12s"));
        assert!(text(&layout, 82_000).contains("1 complete · elapsed 1m 12s"));
    }

    #[test]
    fn reused_worker_uses_first_scoped_start_unknown_start_is_omitted() {
        let mut threads = vec![Thread::new_foreground()];
        for (turn, at) in [
            ("conversation:old:2", 1_000),
            ("conversation:new:2", 10_000),
        ] {
            accept(&mut threads[0], turn, at);
            for start in [at + 1_000, at + 5_000] {
                threads[0].add_turn(
                    ItemKind::Spawn,
                    "worker reused: task".into(),
                    Some(turn.into()),
                );
                threads[0].items.last_mut().unwrap().timestamp = start;
            }
        }
        let worker = find_or_create_thread(&mut threads, "unknown", false, None);
        threads[worker].add_turn(
            ItemKind::System,
            "observed".into(),
            Some("conversation:new:2".into()),
        );
        let layout = layout(&threads, 1);
        let output = text(&layout, 22_000);
        assert!(
            output
                .lines()
                .any(|line| line.contains("reused") && line.contains("elapsed 11s")),
            "{output}"
        );
        assert!(output
            .lines()
            .any(|line| line.contains("unknown") && !line.contains("elapsed")));
    }

    #[test]
    fn final_reply_removes_timer_slot_across_event_orders_and_resize() {
        for timing_first in [false, true] {
            for with_timing in [false, true] {
                let mut threads = vec![Thread::new_foreground()];
                accept(&mut threads[0], "2", 10_000);
                threads[0].add_reply_fragment("acknowledgement".into(), Some("2".into()), false);
                let cells = build_turn_cells(&threads[0]);
                let cell = &cells[0];
                let mut cache = TurnLayoutCache::default();
                cache.prepare(80, &cells, threads[0].structure_revision);
                let initial_revision = cell_revision(&threads[0], cell);
                assert!(text(
                    cache.layout(cell_key(cell), initial_revision, 0, || layout(&threads, 0)),
                    22_000
                )
                .contains("elapsed"));
                for timing in [timing_first, !timing_first] {
                    if timing {
                        if with_timing {
                            threads[0].touch();
                            let revision = threads[0].revision;
                            threads[0].metrics.get_mut("2").unwrap().completed_ms = Some(14_000);
                            threads[0].metric_revisions.insert("2".into(), revision);
                        }
                    } else if timing_first {
                        let mut metadata =
                            tachyon_api::InteractionMetadata::new("final", "request", "test", 2);
                        metadata.turn_id = Some("2".into());
                        metadata.occurred_at_ms = 24_000;
                        apply_interaction_event(
                            &mut threads[0],
                            InteractionEventEnvelope {
                                metadata,
                                event: InteractionEvent::ConversationFinished {
                                    text: "final answer".into(),
                                },
                            },
                        );
                    } else {
                        apply_correlated_agent_event(
                            &mut threads[0],
                            AgentEvent::Reply {
                                turn: Some(2),
                                text: "final answer".into(),
                                final_reply: true,
                            },
                            Some("2"),
                        );
                    }
                    let revision = cell_revision(&threads[0], cell);
                    cache.layout(cell_key(cell), revision, 0, || layout(&threads, 0));
                }
                let copy = selected_chat_cell_text(&threads, Some(0));
                for archived in [false, true] {
                    if archived {
                        threads[0].history_len = threads[0].items.len();
                    }
                    for width in [1, 24, 80, 120] {
                        cache.prepare(width, &cells, threads[0].structure_revision);
                        let revision = cell_revision(&threads[0], cell);
                        let cached = cache.layout(cell_key(cell), revision, 0, || {
                            turn_cell_layout(0, 0, &threads, cell, width, 0, false, "", false, None)
                        });
                        assert!(cached.timers.is_empty());
                        let expected =
                            main_conversation_layout(&threads[0], cell, width, 0, false, "");
                        // No timer placeholder, including an otherwise invisible blank slot.
                        if !archived {
                            assert_eq!(cached.lines, expected.lines);
                        }
                        assert_eq!(text(cached, 22_000), text(cached, 82_000));
                        assert!(!text(cached, 82_000).contains("elapsed"));
                        assert_eq!(selected_chat_cell_text(&threads, Some(0)), copy);
                        if width == 120 && with_timing {
                            assert!(text(cached, 82_000).contains("done 14.0s"));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn terminal_and_archived_pending_freeze() {
        for archived in [false, true] {
            let mut threads = vec![Thread::new_foreground()];
            accept(&mut threads[0], "2", 10_000);
            threads[0].add_turn(ItemKind::Spawn, "worker a: task".into(), Some("2".into()));
            threads[0].items.last_mut().unwrap().timestamp = 11_000;
            if archived {
                threads[0].history_len = threads[0].items.len();
            } else {
                threads[0].metrics.get_mut("2").unwrap().ended_at_ms = Some(15_000);
                threads[0].add_turn(ItemKind::Error, "failed".into(), Some("2".into()));
            }
            let layout = layout(&threads, 0);
            assert_eq!(text(&layout, 22_000), text(&layout, 82_000));
            assert!(layout.timers.iter().all(|timer| timer.end.is_some()));
            let mut main = CellLayout::default();
            main_badge(&mut main, &threads[0], &build_turn_cells(&threads[0])[0], 0);
            assert!(main.lines.is_empty());
            assert!(main.hits.is_empty());
            assert!(main.timers.is_empty());
        }
    }

    #[test]
    fn worker_execution_is_not_request_wall_time_and_all_outcomes_freeze() {
        for kind in [ItemKind::SpawnResult, ItemKind::Error] {
            for execution in [None, Some(2_000)] {
                let mut threads = vec![Thread::new_foreground()];
                accept(&mut threads[0], "2", 10_000);
                threads[0].add_turn(ItemKind::Spawn, "worker a: task".into(), Some("2".into()));
                threads[0].items.last_mut().unwrap().timestamp = 11_000;
                threads[0].add_turn(
                    kind.clone(),
                    "work a: task\nresult".into(),
                    Some("2".into()),
                );
                let item = threads[0].items.last_mut().unwrap();
                item.timestamp = 20_000;
                item.work = Some(WorkDetail {
                    raw_open: false,
                    key: AssignmentKey {
                        work_id: "a".into(),
                        generation: 1,
                        assignment: 1,
                    },
                    slot: None,
                    tool: None,
                    omitted: 0,
                    timing: Some(tachyon_api::types::WorkTiming {
                        execution_ms: execution,
                        ..Default::default()
                    }),
                });
                let layout = layout(&threads, 0);
                let badge = layout.timers.last().unwrap();
                assert_eq!(
                    badge.end,
                    Some(if execution.is_some() { 2_000 } else { 20_000 })
                );
                assert_eq!(
                    overlay(&layout, badge.row, 80, 22_000),
                    overlay(&layout, badge.row, 80, 82_000)
                );
                assert!(overlay(&layout, badge.row, 80, 82_000)
                    .to_string()
                    .contains(if execution.is_some() {
                        "execution 2s"
                    } else {
                        "elapsed 9s"
                    }));
            }
        }
    }

    #[test]
    fn timer_rows_are_width_bounded_without_reflow() {
        let mut threads = vec![Thread::new_foreground()];
        accept(&mut threads[0], "2", 0);
        let layout = layout(&threads, 0);
        let row = layout.timers[0].row;
        for width in [0, 1, 5, 12, 24, 80] {
            for at in [0, 999, 59_999, 60_000, 3_600_000, u64::MAX] {
                assert!(overlay(&layout, row, width, at).width() <= width as usize);
            }
        }
        assert!(overlay(&layout, row, 80, u64::MAX)
            .to_string()
            .contains("99h+"));
    }

    #[test]
    fn ticking_preserves_wrapped_body_headers_selection_and_copy() {
        let mut threads = vec![Thread::new_foreground()];
        accept(&mut threads[0], "2", 10_000);
        threads[0].add_reply_fragment(
            "A long **formatted** answer with [a citation](https://example.test). ".repeat(20),
            Some("2".into()),
            false,
        );
        threads[0].add_turn(
            ItemKind::Spawn,
            "worker warm: task".into(),
            Some("2".into()),
        );
        threads[0].items.last_mut().unwrap().timestamp = 11_000;
        let cells = build_turn_cells(&threads[0]);
        let cell = &cells[0];
        let expected_copy = selected_chat_cell_text(&threads, Some(0));
        for width in [1, 12, 54, 120] {
            for selected in [None, Some("warm")] {
                let mut cache = TurnLayoutCache::default();
                cache.prepare(width, &cells, threads[0].structure_revision);
                let revision = cell_revision(&threads[0], cell);
                let cached = cache.layout(cell_key(cell), revision, 0, || {
                    turn_cell_layout(0, 0, &threads, cell, width, 0, true, "", true, selected)
                });
                let baseline = cached.lines.clone();
                let hits = cached.hits.clone();
                assert_eq!(cached.timers.len(), 2);
                for at in [22_000, 82_000, 3_610_000, u64::MAX] {
                    let cached = cache.layout(cell_key(cell), revision, 0, || {
                        panic!("ticking rebuilt a wrapped answer")
                    });
                    for (row, line) in baseline.iter().enumerate() {
                        let rendered = overlay(cached, row, width, at);
                        if cached.timers.iter().any(|timer| timer.row == row) {
                            assert!(rendered.width() <= width as usize);
                        } else {
                            assert_eq!(&rendered, line);
                        }
                    }
                    assert_eq!(cached.lines, baseline);
                    assert_eq!(cached.hits, hits);
                    assert_eq!(selected_chat_cell_text(&threads, Some(0)), expected_copy);
                }
                assert_eq!(cache.builds, 1);
            }
        }
    }

    #[test]
    fn host_timestamps_are_milliseconds_and_scoped_to_the_envelope() {
        let mut threads = vec![Thread::new_foreground()];
        let mut event = EventEnvelope {
            event_id: 1,
            session_id: "session".into(),
            conversation_id: Some("new".into()),
            turn_id: Some("2".into()),
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: Actor::Foreground,
            sequence: 1,
            occurred_at_ms: 10_010,
            kind: AgentEvent::Timing {
                turn: 2,
                stage: "input_accepted".into(),
                elapsed_ms: 10,
            },
        };
        qualify_event_turn(&mut event);
        record_correlated_metrics(&mut threads, &event);
        assert_eq!(
            threads[0].metrics["conversation:new:2"].accepted_at_ms,
            Some(10_000)
        );
        event.kind = AgentEvent::WorkerStarted {
            turn: Some(2),
            worker_id: "warm".into(),
            objective: "task".into(),
        };
        event.occurred_at_ms = 12_000;
        record_correlated_metrics(&mut threads, &event);
        event.occurred_at_ms = 13_000;
        record_correlated_metrics(&mut threads, &event);
        assert_eq!(
            threads[0].metrics["conversation:new:2"].worker_started_at_ms["warm"],
            12_000
        );
        event.turn_id = Some("conversation:other:2".into());
        record_correlated_metrics(&mut threads, &event);
        assert_eq!(
            threads[0].metrics["conversation:other:2"].worker_started_at_ms["warm"],
            13_000
        );
        event.turn_id = Some("conversation:new:2".into());
        event.kind = AgentEvent::Error {
            turn: Some(2),
            message: "failed".into(),
        };
        event.occurred_at_ms = 15_000;
        record_correlated_metrics(&mut threads, &event);
        assert_eq!(
            threads[0].metrics["conversation:new:2"].ended_at_ms,
            Some(15_000)
        );
        assert_eq!(threads[0].metrics["conversation:other:2"].ended_at_ms, None);
        assert!(!threads[0].metrics.contains_key("2"));
    }
}
