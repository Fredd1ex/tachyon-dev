//! Conversation cell composition and diagnostic expansion.
use crate::app::model::items::ItemKind;
use crate::app::model::metrics::TokenTotals;
use crate::app::model::thread::Thread;
use crate::app::response::main_conversation_layout;
use crate::app::transcript::text::{format_count, trace_summary, wrap_text};
use crate::app::transcript::trace::{
    compact_model_timeline, compacted_model_line, ensure_worker_trace, push_trace_heading,
    push_trace_item, trace_count_summary, worker_record, worker_trace_state, WorkerTrace,
};
use crate::app::transcript_cache::{CellLayout, TurnCell};
use crate::app::ui::activity::short_preview;
use crate::app::{elapsed, icon, session_archive, ClickTarget};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

pub(in crate::app) fn turn_cell_layout(
    thread_index: usize,
    turn_index: usize,
    threads: &[Thread],
    cell: &TurnCell,
    width: u16,
    latest_timestamp: u64,
    active: bool,
    activity: &str,
    open: bool,
    open_worker: Option<&str>,
) -> CellLayout {
    compose_cell(
        thread_index,
        turn_index,
        threads,
        cell,
        width,
        latest_timestamp,
        active,
        activity,
        open,
        open_worker,
        false,
    )
}

pub(in crate::app) fn inline_cell_layout(
    thread_index: usize,
    turn_index: usize,
    threads: &[Thread],
    cell: &TurnCell,
    width: u16,
    latest_timestamp: u64,
    active: bool,
    activity: &str,
    open: bool,
    open_worker: Option<&str>,
) -> CellLayout {
    compose_cell(
        thread_index,
        turn_index,
        threads,
        cell,
        width,
        latest_timestamp,
        active,
        activity,
        open,
        open_worker,
        true,
    )
}

fn compose_cell(
    thread_index: usize,
    turn_index: usize,
    threads: &[Thread],
    cell: &TurnCell,
    width: u16,
    latest_timestamp: u64,
    active: bool,
    activity: &str,
    open: bool,
    open_worker: Option<&str>,
    inline: bool,
) -> CellLayout {
    let thread = &threads[thread_index];
    let content_width = width.saturating_sub(if open && !inline { 2 } else { 0 });
    let mut layout = main_conversation_layout(
        thread,
        cell,
        content_width,
        latest_timestamp,
        active,
        activity,
    );
    for (row, hit) in layout.hits.iter_mut().enumerate() {
        *hit = if thread.items[cell.prompt].attention.is_some()
            && content_width > 4
            && row >= 2
            && row + 1 < layout.lines.len()
            && layout.lines[row].width() > 0
        {
            Some(ClickTarget::Attention(thread_index, cell.prompt))
        } else if row == 0 {
            Some(ClickTarget::TraceSummary(turn_index))
        } else {
            None
        };
    }
    if let Some(at) = layout
        .activity_row
        .filter(|_| cell.prompt >= thread.history_len)
    {
        if let Some((turn, text)) = &thread.checklist {
            if thread.items[cell.prompt].turn.as_ref() == Some(turn) && !text.is_empty() {
                let indent = if content_width >= 8 { "    " } else { "" };
                let lines: Vec<_> = text
                    .lines()
                    .map(|row| {
                        Line::styled(
                            format!(
                                "{indent}{}",
                                crate::app::panels::activity::compact(
                                    row,
                                    content_width as usize - indent.len(),
                                )
                            ),
                            Style::default().fg(Color::Gray),
                        )
                    })
                    .collect();
                // The main timer precedes this slot; task timers are composed below.
                layout.activity_row = Some(at + lines.len());
                layout.hits.splice(at..at, vec![None; lines.len()]);
                layout.lines.splice(at..at, lines);
            }
        }
    }
    if let Some(at) = layout.activity_row.filter(|_| inline && !open) {
        use crate::app::panels::activity::{compact, compact_row, task_title, tasks};
        let turn = thread.items[cell.prompt].turn.as_deref();
        let rows = tasks(threads, thread_index, turn);
        let terminal = turn.is_some_and(|t| thread.completed_turns.contains(t))
            || cell.prompt < thread.history_len
            || turn
                .and_then(|t| thread.metrics.get(t))
                .is_some_and(|m| m.ended_at_ms.is_some());
        let mut lines = Vec::new();
        let mut hits = Vec::new();
        let indent = if content_width >= 8 { "    " } else { "" };
        let budget = content_width as usize - indent.len();
        if terminal && !rows.is_empty() {
            let complete = rows
                .iter()
                .filter(|r| r.label.ends_with(" - complete"))
                .count();
            let text = format!(
                "{} {}, {complete} complete · Ctrl+O details",
                rows.len(),
                if rows.len() == 1 { "task" } else { "tasks" }
            );
            lines.push(Line::styled(
                format!("{indent}{}", compact(&text, budget)),
                Style::default().fg(Color::DarkGray),
            ));
            hits.push(Some(ClickTarget::TraceSummary(turn_index)));
        } else {
            for row in rows.iter().take(3) {
                lines.push(Line::styled(
                    format!("{indent}{}", compact_row(threads, row, budget)),
                    Style::default().fg(Color::DarkGray),
                ));
                // Selecting the row opens details; actions do not consume rows.
                let item = &threads[row.id.0].items[row.id.1];
                let source = &threads[row.id.0];
                let worker = if row.id.0 != thread_index {
                    Some(
                        source
                            .id
                            .strip_prefix("visit:")
                            .and_then(|id| id.split_once(':').map(|(_, id)| id))
                            .unwrap_or(&source.id),
                    )
                } else {
                    item.work
                        .as_ref()
                        .map(|w| w.key.work_id.as_str())
                        .or_else(|| worker_record(&item.text).map(|(id, _)| id))
                };
                if item.kind == ItemKind::Spawn && row.label.ends_with(" - started") {
                    let title = task_title(threads, row);
                    elapsed::task_badge(&mut layout, at + lines.len() - 1, item.timestamp, title);
                }
                hits.push(Some(
                    worker
                        .map(|id| ClickTarget::Worker(turn_index, id.to_owned()))
                        .unwrap_or(ClickTarget::TraceSummary(turn_index)),
                ));
            }
            if rows.len() > 3 {
                lines.push(Line::raw(format!(
                    "{indent}{}",
                    compact(
                        &format!("+{} more · Ctrl+O details", rows.len() - 3),
                        budget
                    )
                )));
                hits.push(Some(ClickTarget::TraceSummary(turn_index)));
            }
        }
        layout.hits.splice(at..at, hits);
        layout.lines.splice(at..at, lines);
    }
    if let Some(at) = layout.activity_row.filter(|_| inline && open) {
        let rows = crate::app::panels::activity::project(
            threads,
            thread_index,
            thread.items[cell.prompt].turn.as_deref(),
        );
        let mut lines = Vec::new();
        let mut activity_hits = Vec::new();
        let omitted = rows.len().saturating_sub(8);
        let indent = if content_width >= 8 { "    " } else { "" };
        let row_width = content_width.saturating_sub(indent.len() as u16);
        for row in rows.into_iter().take(8) {
            for text in crate::app::panels::activity::wrap(&row.label, row_width) {
                lines.push(Line::styled(
                    format!("{indent}{text}"),
                    Style::default().fg(Color::DarkGray),
                ));
            }
            let item = &threads[row.id.0].items[row.id.1];
            if item.kind == ItemKind::Spawn {
                let turn = item.turn.as_deref().unwrap_or("");
                let id = worker_record(&item.text).map(|(id, _)| id).unwrap_or("");
                let unambiguous = cell
                    .items
                    .iter()
                    .filter(|i| {
                        let other = &thread.items[**i];
                        other.kind == ItemKind::Spawn
                            && worker_record(&other.text)
                                .is_some_and(|(other_id, _)| other_id == id)
                    })
                    .count()
                    == 1;
                let terminal = thread.completed_turns.contains(turn)
                    || cell.prompt < thread.history_len
                    || cell
                        .items
                        .iter()
                        .any(|i| thread.items[*i].kind == ItemKind::Reply);
                let metrics = thread.metrics.get(turn);
                let end = metrics
                    .and_then(|m| m.worker_ended_at_ms.get(id).copied().or(m.ended_at_ms))
                    .or_else(|| {
                        terminal.then(|| {
                            cell.items
                                .iter()
                                .map(|i| thread.items[*i].timestamp)
                                .max()
                                .unwrap_or(item.timestamp)
                        })
                    });
                let summary = if end.is_none() {
                    thread.activity.summary(turn, Some(id))
                } else {
                    String::new()
                };
                if unambiguous {
                    elapsed::inline_badge(
                        &mut layout,
                        at + lines.len(),
                        item.timestamp,
                        end,
                        summary,
                        String::new(),
                        "elapsed",
                    );
                    lines.push(Line::raw(""));
                }
                let action = format!("{indent}{} view subagent", icon::AGENT);
                let row = lines.len();
                lines.push(Line::raw(action));
                activity_hits.push((row, ClickTarget::Worker(turn_index, id.to_owned())));
            }
            if let Some(work) = item.work.as_ref().filter(|w| w.tool.is_none()) {
                let count = threads
                    .iter()
                    .flat_map(|t| &t.items)
                    .filter(|i| {
                        i.turn == item.turn
                            && i.work
                                .as_ref()
                                .is_some_and(|w| w.key == work.key && w.tool.is_some())
                    })
                    .count();
                let metadata = if count > 0 {
                    format!("{count} recorded tool calls")
                } else {
                    String::new()
                };
                if let Some(ms) = work.timing.as_ref().and_then(|t| t.execution_ms) {
                    elapsed::inline_badge(
                        &mut layout,
                        at + lines.len(),
                        0,
                        Some(ms),
                        String::new(),
                        metadata,
                        "execution",
                    );
                    lines.push(Line::raw(""));
                } else if !metadata.is_empty() {
                    for text in crate::app::panels::activity::wrap(&metadata, row_width) {
                        lines.push(Line::raw(format!("{indent}{text}")));
                    }
                }
                let row = lines.len();
                lines.push(Line::raw(format!("{indent}{} view subagent", icon::AGENT)));
                activity_hits.push((
                    row,
                    ClickTarget::Worker(turn_index, work.key.work_id.clone()),
                ));
            }
            if open && item.kind != ItemKind::Spawn {
                let detail = crate::app::panels::activity::details(threads, &row, false);
                for text in crate::app::panels::activity::wrap(
                    &detail.replace(
                        "[r] show bounded, redacted raw output",
                        "Ctrl+D: inspect raw diagnostics",
                    ),
                    row_width,
                ) {
                    lines.push(Line::styled(
                        format!("{indent}{text}"),
                        Style::default().fg(Color::Gray),
                    ));
                }
            }
        }
        if omitted > 0 {
            for text in crate::app::panels::activity::wrap(
                &format!("{omitted} more records: Ctrl+O, Ctrl+D to inspect"),
                row_width,
            ) {
                lines.push(Line::raw(format!("{indent}{text}")));
            }
        }
        layout.hits.splice(at..at, vec![None; lines.len()]);
        for (row, hit) in activity_hits {
            layout.hits[at + row] = Some(hit);
        }
        layout.lines.splice(at..at, lines);
    }
    if !thread.hide_history && thread.history_len > 0 {
        let start = if cell.prompt < thread.history_len {
            0
        } else {
            thread.history_len
        };
        if !thread.items[start..cell.prompt]
            .iter()
            .any(|i| matches!(i.kind, ItemKind::User | ItemKind::Reply))
        {
            let label = if cell.prompt < thread.history_len {
                thread
                    .history_label
                    .clone()
                    .unwrap_or_else(|| "Previous session (date unknown)".into())
            } else {
                format!(
                    "Current session {}",
                    session_archive::date_label(thread.session_started)
                )
            };
            layout.lines.splice(
                0..0,
                [
                    Line::raw(""),
                    session_archive::separator(&label, content_width),
                    Line::raw(""),
                ],
            );
            layout.hits.splice(0..0, [None, None, None]);
            elapsed::shift_rows(&mut layout, 3);
        }
    }
    if !open || inline {
        return layout;
    }
    if let Some(metrics) = thread.items[cell.prompt]
        .turn
        .as_ref()
        .and_then(|turn| thread.metrics.get(turn))
    {
        let mut usage = Vec::new();
        if let Some(tokens) = &metrics.self_usage {
            usage.push(format!(
                "measured foreground model tokens {} (prompt {} + completion {}; classifier/synthesis not separated)",
                format_count(tokens.total), format_count(tokens.prompt), format_count(tokens.completion)
            ));
        } else {
            usage.push("foreground model tokens unavailable".into());
        }
        if !metrics.worker_usage.is_empty() {
            let tokens =
                metrics
                    .worker_usage
                    .values()
                    .fold(TokenTotals::default(), |mut sum, tokens| {
                        sum.prompt = sum.prompt.saturating_add(tokens.prompt);
                        sum.completion = sum.completion.saturating_add(tokens.completion);
                        sum.total = sum.total.saturating_add(tokens.total);
                        sum
                    });
            usage.push(format!(
                "worker reported tokens {} (prompt {} + completion {})",
                format_count(tokens.total),
                format_count(tokens.prompt),
                format_count(tokens.completion)
            ));
        } else {
            usage.push("worker reported tokens unavailable".into());
        }
        usage.push("review tokens unavailable".into());
        for text in usage {
            for line in wrap_text(&text, content_width.saturating_sub(4) as usize) {
                layout.lines.push(Line::from(Span::styled(
                    format!("    {line}"),
                    Style::default().fg(Color::DarkGray),
                )));
                layout.hits.push(None);
            }
        }
    }
    let mut diagnostic_items = cell
        .items
        .iter()
        .copied()
        .filter(|index| {
            !matches!(
                thread.items[*index].kind,
                ItemKind::User | ItemKind::PendingReply | ItemKind::Reply
            )
        })
        .map(|index| (thread_index, index))
        .collect::<Vec<_>>();
    let turn = thread.items[cell.prompt].turn.as_deref();
    if let Some(turn) = turn {
        for (source_thread, worker) in threads
            .iter()
            .enumerate()
            .filter(|(_, worker)| !worker.is_foreground)
        {
            diagnostic_items.extend(
                worker
                    .items
                    .iter()
                    .enumerate()
                    .filter(|(_, item)| item.turn.as_deref() == Some(turn))
                    .map(|(index, _)| (source_thread, index)),
            );
        }
    }
    diagnostic_items
        .sort_by_key(|(source_thread, index)| threads[*source_thread].items[*index].timestamp);
    let tools = diagnostic_items
        .iter()
        .filter(|(source_thread, index)| {
            threads[*source_thread].items[*index].kind == ItemKind::Tool
        })
        .count();
    if diagnostic_items.is_empty() {
        return layout;
    }
    if !open {
        return layout;
    }
    let mut worker_traces = std::collections::BTreeMap::<String, WorkerTrace>::new();
    let mut model_items = Vec::new();
    let mut orchestration_items = Vec::new();
    for &(source_thread, index) in &diagnostic_items {
        let source = &threads[source_thread];
        let item = &source.items[index];
        if source_thread != thread_index {
            // Page-local thread IDs are namespaced, but Spawn/evidence IDs retain
            // their producer identity. The exact turn filter above provides scope.
            let id = source
                .id
                .strip_prefix("visit:")
                .and_then(|id| id.split_once(':').map(|(_, id)| id))
                .unwrap_or(&source.id);
            let worker = ensure_worker_trace(&mut worker_traces, id, source.task.as_deref());
            worker.items.push((source_thread, index));
            continue;
        }
        if let Some(work) = &item.work {
            let objective = if work.tool.is_none() {
                worker_record(&item.text).and_then(|(_, objective)| objective)
            } else {
                None
            };
            let worker = ensure_worker_trace(&mut worker_traces, &work.key.work_id, objective);
            worker.items.push((source_thread, index));
            continue;
        }
        match item.kind {
            ItemKind::Spawn => {
                if let Some((id, objective)) = worker_record(&item.text) {
                    ensure_worker_trace(&mut worker_traces, id, objective);
                } else {
                    orchestration_items.push((source_thread, index));
                }
            }
            ItemKind::SpawnResult => {
                if let Some((id, objective)) = worker_record(&item.text) {
                    let worker = ensure_worker_trace(&mut worker_traces, id, objective);
                    worker.items.push((source_thread, index));
                } else {
                    orchestration_items.push((source_thread, index));
                }
            }
            ItemKind::Error if item.text.starts_with("worker ") => {
                let (id, _) = worker_record(&item.text).expect("checked worker record");
                let worker = ensure_worker_trace(&mut worker_traces, id, None);
                worker.items.push((source_thread, index));
            }
            ItemKind::Error if item.text.starts_with("work ") => {
                let worker =
                    worker_record(&item.text).and_then(|(id, _)| worker_traces.get_mut(id));
                if let Some(worker) = worker {
                    worker.items.push((source_thread, index));
                } else {
                    orchestration_items.push((source_thread, index));
                }
            }
            ItemKind::Error if item.text.to_ascii_lowercase().contains("review") => {
                orchestration_items.push((source_thread, index));
            }
            ItemKind::System
                if item.text.starts_with("work ")
                    || item.text.starts_with("worker release requested:") =>
            {
                orchestration_items.push((source_thread, index));
            }
            _ => model_items.push((source_thread, index)),
        }
    }
    let workers = worker_traces.len();
    let summary = trace_count_summary(diagnostic_items.len(), tools, workers);
    layout.lines.push(Line::from(Span::styled(
        summary,
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC),
    )));
    layout
        .hits
        .push(Some(ClickTarget::TraceSummary(turn_index)));
    if !model_items.is_empty() {
        push_trace_heading(&mut layout, icon::MODEL, "Model", "  ");
        let (timeline, remaining_model_items) = compact_model_timeline(&model_items, threads);
        let compacted = timeline.is_some();
        if let Some(timeline) = timeline {
            layout.lines.push(Line::from(vec![
                Span::styled(
                    format!("    {} ", icon::DURATION),
                    Style::default().fg(Color::Cyan),
                ),
                Span::styled(timeline, Style::default().fg(Color::Gray)),
            ]));
            layout.hits.push(None);
        }
        let mut previous_status = None;
        for (source_thread, index) in remaining_model_items {
            let item = &threads[source_thread].items[index];
            if item.kind == ItemKind::System {
                for line in item
                    .text
                    .lines()
                    .filter(|line| !compacted_model_line(line, compacted))
                {
                    let status = trace_summary(line);
                    if previous_status.as_deref() == Some(status.as_str()) {
                        continue;
                    }
                    previous_status = Some(status.clone());
                    layout.lines.push(Line::from(vec![
                        Span::styled("    · ", Style::default().fg(Color::DarkGray)),
                        Span::styled(status, Style::default().fg(Color::DarkGray)),
                    ]));
                    layout.hits.push(None);
                }
                continue;
            }
            previous_status = None;
            push_trace_item(
                &mut layout,
                threads,
                source_thread,
                index,
                content_width,
                "    ",
                None,
            );
        }
    }
    let mut rendered = 0;
    if !orchestration_items.is_empty() || !worker_traces.is_empty() {
        push_trace_heading(&mut layout, icon::AGENT, "Agents", "  ");
        for (source_thread, index) in orchestration_items {
            push_trace_item(
                &mut layout,
                threads,
                source_thread,
                index,
                content_width,
                "    ",
                None,
            );
        }
        const WORKER_ROW_LIMIT: usize = 20;
        let selected_outside = open_worker.is_some_and(|id| {
            worker_traces.contains_key(id)
                && !worker_traces
                    .keys()
                    .take(WORKER_ROW_LIMIT)
                    .any(|key| key == id)
        });
        let ordinary_limit = WORKER_ROW_LIMIT - usize::from(selected_outside);
        let outside_worker = if selected_outside {
            worker_traces.remove(open_worker.unwrap())
        } else {
            None
        };
        for worker in worker_traces
            .values()
            .take(ordinary_limit)
            .chain(outside_worker.iter())
        {
            rendered += 1;
            let objective = worker.objective.as_deref().unwrap_or("Worker task");
            let expanded = open_worker == Some(worker.id.as_str());
            let (state, color) = worker_trace_state(&worker, threads);
            let state_icon = match state {
                "complete" => icon::SUCCESS,
                "running" => icon::RUNNING,
                _ => icon::FAILURE,
            };
            layout.lines.push(Line::from(vec![
                Span::styled(
                    format!(
                        "    {} {} ",
                        if expanded {
                            icon::EXPANDED
                        } else {
                            icon::COLLAPSED
                        },
                        icon::AGENT
                    ),
                    Style::default().fg(Color::Green),
                ),
                Span::styled(short_preview(objective), Style::default().fg(Color::Gray)),
                Span::styled(
                    format!(" · {state_icon} {state}"),
                    Style::default().fg(color),
                ),
                Span::styled(
                    format!(" · {}", worker.id.chars().take(8).collect::<String>()),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
            layout
                .hits
                .push(Some(ClickTarget::Worker(turn_index, worker.id.clone())));
            elapsed::worker_badge(&mut layout, thread, cell, worker, threads);
            if expanded {
                for &(source_thread, index) in &worker.items {
                    if threads[source_thread].items[index].kind == ItemKind::SpawnResult
                        && threads[source_thread].items[index].work.is_none()
                    {
                        continue;
                    }
                    push_trace_item(
                        &mut layout,
                        threads,
                        source_thread,
                        index,
                        content_width,
                        "      ",
                        Some(&worker.id),
                    );
                }
            }
        }
    }
    if workers > rendered {
        layout.lines.push(Line::from(Span::styled(
            format!(
                "    {} worker summaries omitted (showing {rendered} of {workers})",
                workers - rendered
            ),
            Style::default().fg(Color::DarkGray),
        )));
        layout.hits.push(None);
    }
    layout.lines.push(Line::raw(""));
    layout.hits.push(None);
    add_selected_rail(&mut layout);
    layout
}

pub(in crate::app) fn add_selected_rail(layout: &mut CellLayout) {
    for line in &mut layout.lines {
        line.spans
            .insert(0, Span::styled("│ ", Style::default().fg(Color::DarkGray)));
    }
}
