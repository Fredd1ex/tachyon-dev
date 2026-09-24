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

#[cfg(test)]
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
    _open: bool,
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
        false,
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
    let content_width = width;
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
    let turn = thread.items[cell.prompt].turn.as_deref();
    let activity_start = layout.activity_row;
    let before = layout.lines.len();
    if inline && !open {
        super::active_work::insert(
            &mut layout,
            threads,
            thread_index,
            turn,
            turn_index,
            content_width,
        );
    }
    let has_cards = layout.lines.len() > before;
    if cell.prompt >= thread.history_len {
        if let Some(text) = turn.and_then(|turn| {
            thread
                .checklist
                .as_ref()
                .filter(|(t, _)| t == turn)
                .map(|(_, text)| text)
                .or_else(|| thread.recorded_checklists.get(turn))
        }) {
            super::progress::insert(&mut layout, text, content_width);
        }
    }
    layout.activity_row = activity_start.map(|at| at + usize::from(has_cards));
    let host_session = |item: &crate::app::Item| {
        item.turn
            .as_deref()
            .and_then(|t| t.strip_prefix("conversation:foreground:"))
            .and_then(|t| t.rsplit_once(':'))
            .map(|(host, _)| host.to_owned())
    };
    if let Some(host) = host_session(&thread.items[cell.prompt]) {
        let previous = thread.items[..cell.prompt]
            .iter()
            .rev()
            .find(|i| {
                matches!(
                    i.kind,
                    ItemKind::User | ItemKind::Reply | ItemKind::PendingReply
                )
            })
            .and_then(host_session);
        if previous.as_ref() != Some(&host) {
            let label = format!("Session {host}");
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
    } else if !thread.hide_history && thread.history_len > 0 {
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
    layout
}
