//! Diagnostic timeline and worker evidence drawing.
use crate::app::model::items::ItemKind;
use crate::app::model::thread::Thread;
use crate::app::transcript::text::{
    agent_count, api_target, human_millis, spawn_display_label, styled_markdown, tool_icon,
    tool_parts, trace_summary, truncate_text, worker_objective, wrap_text,
};
use crate::app::transcript_cache::CellLayout;
use crate::app::ui::activity::{short_preview, spinner_glyph};
use crate::app::ui::format::timestamp_label;
use crate::app::{icon, names, ClickTarget};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

pub(in crate::app) fn timing_stage(text: &str) -> Option<(&str, u64)> {
    let timing = text.strip_prefix("[timing] ")?;
    let (stage, elapsed) = timing.rsplit_once(' ')?;
    Some((stage, elapsed.strip_suffix("ms")?.parse().ok()?))
}

pub(in crate::app) fn compact_timing_stage(stage: &str) -> bool {
    matches!(
        stage,
        "input_accepted"
            | "acknowledgement_published"
            | "provider_first_output"
            | "first_answer"
            | "routing"
            | "ready"
    ) || stage == "first_visible"
        || stage.contains("first_token")
        || stage == "completed"
        || stage.contains("routed")
        || stage.ends_with("_started")
        || stage.ends_with("_completed")
}

pub(in crate::app) fn compacted_model_line(line: &str, compacted: bool) -> bool {
    if timing_stage(line).is_some_and(|(stage, _)| compact_timing_stage(stage)) {
        return true;
    }
    if !compacted {
        return false;
    }
    let lower = line.to_ascii_lowercase();
    lower.starts_with("[ready]")
        || lower.starts_with("[working]")
        || lower.contains("publication")
        || lower.contains("publish")
        || lower.contains("commit")
}

pub(in crate::app) fn compact_model_timeline(
    items: &[(usize, usize)],
    threads: &[Thread],
) -> (Option<String>, Vec<(usize, usize)>) {
    let mut routed = None;
    let mut accepted = None;
    let mut acknowledgement = None;
    let mut output = None;
    let mut first = None;
    let mut completed = None;
    for &(thread, index) in items {
        for line in threads[thread].items[index].text.lines() {
            let Some((stage, elapsed)) = timing_stage(line) else {
                continue;
            };
            if stage == "input_accepted" {
                accepted.get_or_insert(elapsed);
            } else if stage == "acknowledgement_published" {
                acknowledgement.get_or_insert(elapsed);
            } else if stage == "provider_first_output" || stage.contains("first_token") {
                output.get_or_insert(elapsed);
            } else if stage == "first_answer" || stage == "first_visible" {
                first.get_or_insert(elapsed);
            } else if stage == "completed" {
                completed = Some(elapsed);
            } else if stage == "routing" || stage.contains("routed") {
                routed.get_or_insert(elapsed);
            }
        }
    }
    let mut points = Vec::new();
    if let Some(elapsed) = accepted {
        points.push((elapsed, format!("accepted {}", human_millis(elapsed))));
    }
    if let Some(elapsed) = acknowledgement {
        points.push((elapsed, format!("ack {}", human_millis(elapsed))));
    }
    if let Some(elapsed) = routed {
        points.push((elapsed, format!("routed {}", human_millis(elapsed))));
    }
    if let Some(elapsed) = output {
        points.push((elapsed, format!("first output {}", human_millis(elapsed))));
    }
    if let Some(elapsed) = first {
        points.push((elapsed, format!("first answer {}", human_millis(elapsed))));
    }
    if let Some(elapsed) = completed {
        points.push((elapsed, format!("completed {}", human_millis(elapsed))));
    }
    points.sort_by_key(|(elapsed, _)| *elapsed);
    let points = points
        .into_iter()
        .map(|(_, label)| label)
        .collect::<Vec<_>>();
    let has_completion = completed.is_some();
    let remaining = items
        .iter()
        .copied()
        .filter(|(thread, index)| {
            let item = &threads[*thread].items[*index];
            if has_completion && item.kind == ItemKind::System {
                return item
                    .text
                    .lines()
                    .any(|line| !compacted_model_line(line, true));
            }
            if item.kind == ItemKind::System
                && item.text.lines().all(|line| {
                    timing_stage(line).is_some_and(|(stage, _)| compact_timing_stage(stage))
                })
            {
                return false;
            }
            true
        })
        .collect();
    ((!points.is_empty()).then(|| points.join(" -> ")), remaining)
}

pub(in crate::app) struct WorkerTrace {
    pub(in crate::app) id: String,
    pub(in crate::app) objective: Option<String>,
    pub(in crate::app) items: Vec<(usize, usize)>,
}

pub(in crate::app) fn worker_record(text: &str) -> Option<(&str, Option<&str>)> {
    if let Some(id) = text.strip_prefix("spawned worker ") {
        return Some((id.trim(), None));
    }
    let record = text
        .strip_prefix("worker ")
        .or_else(|| text.strip_prefix("work "))?;
    let (id, detail) = record.split_once(": ").unwrap_or((record, ""));
    let objective = detail
        .lines()
        .next()
        .map(str::trim)
        .filter(|text| !text.is_empty());
    Some((id.trim(), objective))
}

pub(in crate::app) fn worker_error_detail(text: &str) -> &str {
    let Some(record) = text
        .strip_prefix("worker ")
        .or_else(|| text.strip_prefix("work "))
    else {
        return text;
    };
    let detail = record
        .split_once(": ")
        .map(|(_, detail)| detail)
        .unwrap_or(record);
    detail
        .split_once('\n')
        .map(|(_, error)| error)
        .unwrap_or(detail)
}

pub(in crate::app) fn worker_error_summary(text: &str, worker_id: Option<&str>) -> String {
    let detail = worker_error_detail(text);
    let first = detail.lines().next().unwrap_or(detail).trim();
    let summary = if first.to_ascii_lowercase().contains("review failed") {
        "review failed"
    } else {
        first
    };
    worker_id
        .map(|id| summary.replace(id, "worker"))
        .unwrap_or_else(|| summary.to_string())
}

pub(in crate::app) fn elide_work_id(text: &str) -> String {
    text.strip_prefix("work ")
        .and_then(|record| record.split_once(": "))
        .map(|(_, detail)| format!("work · {detail}"))
        .unwrap_or_else(|| text.to_string())
}

pub(in crate::app) fn ensure_worker_trace<'a>(
    workers: &'a mut std::collections::BTreeMap<String, WorkerTrace>,
    id: &str,
    objective: Option<&str>,
) -> &'a mut WorkerTrace {
    let worker = workers.entry(id.to_owned()).or_insert_with(|| WorkerTrace {
        id: id.to_owned(),
        objective: objective.map(str::to_owned),
        items: Vec::new(),
    });
    if worker.objective.is_none() {
        worker.objective = objective.map(str::to_owned);
    }
    worker
}

pub(in crate::app) fn trace_count_summary(events: usize, tools: usize, agents: usize) -> String {
    let mut counts = Vec::new();
    if events != tools && events != agents {
        counts.push(format!(
            "{events} event{}",
            if events == 1 { "" } else { "s" }
        ));
    }
    if tools > 0 {
        counts.push(format!("{tools} tool{}", if tools == 1 { "" } else { "s" }));
    }
    if agents > 0 {
        counts.push(agent_count(agents));
    }
    format!("  trace · {}", counts.join(" · "))
}

pub(in crate::app) fn push_trace_heading(
    layout: &mut CellLayout,
    icon: &str,
    label: &str,
    indent: &str,
) {
    layout.lines.push(Line::from(Span::styled(
        format!("{indent}{icon} {label}"),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )));
    layout.hits.push(None);
}

pub(in crate::app) fn worker_trace_state(
    worker: &WorkerTrace,
    threads: &[Thread],
) -> (&'static str, Color) {
    let errors = worker.items.iter().filter_map(|(thread, item)| {
        let item = &threads[*thread].items[*item];
        (item.kind == ItemKind::Error).then_some(item.text.to_ascii_lowercase())
    });
    for error in errors {
        if error.contains("review") {
            return ("review failed", Color::Red);
        }
        return ("failed", Color::Red);
    }
    if worker.items.iter().any(|(thread, item)| {
        matches!(
            threads[*thread].items[*item].kind,
            ItemKind::SpawnResult | ItemKind::Reply
        )
    }) {
        ("complete", Color::Green)
    } else {
        ("running", Color::Yellow)
    }
}

pub(in crate::app) fn work_tool_details(
    tool: &tachyon_api::types::WorkToolEvidence,
    raw: bool,
) -> String {
    use std::io::Write;

    // Stop serialization itself, not just the resulting string: legacy/session
    // JSON has no enforced input budget. Each section gets a fair preview.
    struct Preview(Vec<u8>);
    impl Write for Preview {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let count = bytes.len().min(2048usize.saturating_sub(self.0.len()));
            if count == 0 && !bytes.is_empty() {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            self.0.extend_from_slice(&bytes[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[cfg(test)]
    DETAIL_FORMATS.with(|count| count.set(count.get() + 1));
    let mut details = String::new();
    if raw {
        for (label, id) in [
            ("call", tool.call_id.as_deref()),
            ("parent", tool.parent_call_id.as_deref()),
        ] {
            if let Some(id) = id {
                details.push_str(&format!("{label} {}\n", truncate_text(id, 128)));
            }
        }
    }
    for (label, value) in [
        (
            "output_ref",
            raw.then(|| tool.output.get("output_ref")).flatten(),
        ),
        (
            "continuation",
            raw.then(|| tool.output.get("continuation")).flatten(),
        ),
        ("error", tool.output.get("error")),
        (
            "metadata",
            raw.then(|| tool.output.get("metadata")).flatten(),
        ),
        ("Code", tool.arguments.get("code")),
        (
            "Output",
            tool.output
                .get("content")
                .or_else(|| tool.output.as_str().map(|_| &tool.output)),
        ),
        ("arguments", raw.then_some(&tool.arguments)),
        ("output (native envelope)", raw.then_some(&tool.output)),
    ] {
        let Some(value) = value.filter(|value| !value.is_null()) else {
            continue;
        };
        let mut preview = Preview(Vec::new());
        let result = if let Some(text) = value.as_str() {
            preview.write_all(text.as_bytes())
        } else {
            serde_json::to_writer_pretty(&mut preview, value).map_err(std::io::Error::other)
        };
        details.push_str(label);
        details.push('\n');
        details.push_str(&String::from_utf8_lossy(&preview.0));
        if result.is_err() {
            details.push_str("\n[display truncated; stored evidence unchanged]");
        }
        details.push('\n');
    }
    details
}

#[cfg(test)]
thread_local! {
    pub(in crate::app) static DETAIL_FORMATS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(in crate::app) fn push_trace_item(
    layout: &mut CellLayout,
    threads: &[Thread],
    source_thread: usize,
    index: usize,
    width: u16,
    indent: &str,
    worker_id: Option<&str>,
) {
    let source = &threads[source_thread];
    let item = &source.items[index];
    let hit = Some(ClickTarget::Item(source_thread, index));
    if let Some(work) = &item.work {
        let assignment = format!("assignment {}/{}", work.key.generation, work.key.assignment);
        if let Some(tool) = &work.tool {
            let error = tool
                .output
                .get("is_error")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
                || tool
                    .output
                    .get("error")
                    .is_some_and(|value| !value.is_null())
                || tool
                    .output
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    == Some("error");
            let truncated = tool
                .output
                .get("truncated")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let exit = tool
                .output
                .get("exit_code")
                .or_else(|| tool.output.get("metadata").and_then(|m| m.get("exit_code")))
                .and_then(serde_json::Value::as_i64);
            let timed_out = tool
                .output
                .get("timed_out")
                .or_else(|| tool.output.get("metadata").and_then(|m| m.get("timed_out")))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let error = error || exit.is_some_and(|code| code != 0) || timed_out;
            let status = exit
                .map(|code| format!("exit{code}{}", if error { " / error" } else { "" }))
                .unwrap_or_else(|| if error { "error" } else { "recorded" }.into());
            layout.lines.push(Line::from(Span::styled(
                format!(
                    "{indent}{} {} · {}{}{} · click to {}",
                    if item.hidden {
                        icon::COLLAPSED
                    } else {
                        icon::EXPANDED
                    },
                    truncate_text(&tool.tool_name, 128),
                    status,
                    if truncated { " / truncated" } else { "" },
                    if timed_out { " / timed out" } else { "" },
                    if item.hidden { "expand" } else { "collapse" }
                ),
                Style::default().fg(if error { Color::Red } else { Color::Green }),
            )));
            layout.hits.push(hit.clone());
            if !item.hidden {
                layout.lines.push(Line::raw(format!(
                    "{indent}  {} Raw diagnostics{}",
                    if work.raw_open {
                        icon::EXPANDED
                    } else {
                        icon::COLLAPSED
                    },
                    if work.raw_open {
                        format!(" · {assignment}")
                    } else {
                        String::new()
                    }
                )));
                layout
                    .hits
                    .push(Some(ClickTarget::RawEvidence(source_thread, index)));
                let details = work_tool_details(tool, work.raw_open);
                let mut rendered = 1;
                let mut rendered_bytes = layout.lines.last().unwrap().to_string().len() + 5;
                'detail: for source in details.lines() {
                    // Hard wrap without splitting whitespace: Python indentation is evidence.
                    let chars: Vec<char> = if source.is_empty() {
                        vec![' ']
                    } else {
                        source.chars().collect()
                    };
                    let width = width.saturating_sub(indent.len() as u16 + 2).max(1) as usize;
                    for chunk in chars.chunks(width) {
                        let line: String = chunk.iter().collect();
                        let text = format!("{indent}  {line}");
                        // Include the selected rail and newline in the byte budget.
                        let bytes = text.len() + 5;
                        if rendered == 128 || rendered_bytes + bytes > 16 * 1024 {
                            layout.lines.push(Line::raw(format!(
                                "{indent}  [display truncated; stored evidence unchanged]"
                            )));
                            layout.hits.push(hit.clone());
                            break 'detail;
                        }
                        rendered += 1;
                        rendered_bytes += bytes;
                        layout.lines.push(Line::from(Span::styled(
                            text,
                            Style::default().fg(Color::Gray),
                        )));
                        layout.hits.push(hit.clone());
                    }
                }
            }
            return;
        }
        let mut details = vec![format!(
            "{assignment} · terminal evidence: producer budget 32 results / ~16 KiB; {} omitted; absence is not success",
            work.omitted
        )];
        if let Some(timing) = &work.timing {
            let measured =
                |value: Option<u64>| value.map(human_millis).unwrap_or_else(|| "unknown".into());
            details.push(format!(
                "execution {} (includes inference {}, tools {}); review {} separate; not additive",
                measured(timing.execution_ms),
                measured(timing.inference_ms),
                measured(timing.tool_ms),
                measured(timing.review_ms)
            ));
        }
        for detail in details {
            for line in wrap_text(
                &detail,
                width.saturating_sub(indent.len() as u16).max(1) as usize,
            ) {
                layout.lines.push(Line::from(Span::styled(
                    format!("{indent}{line}"),
                    Style::default().fg(Color::DarkGray),
                )));
                layout.hits.push(None);
            }
        }
    }
    match item.kind {
        ItemKind::Tool => {
            let running = item.output.is_none();
            let (tool_name, arguments) = tool_parts(&item.text);
            let display_arguments = worker_id
                .map(|id| arguments.replace(id, "worker"))
                .unwrap_or_else(|| arguments.clone());
            let badge = if running {
                format!(" {} running", spinner_glyph())
            } else {
                " ✓".to_string()
            };
            let badge_style =
                Style::default().fg(if running { Color::Yellow } else { Color::Green });
            layout.lines.push(Line::from(vec![
                Span::styled(
                    format!("{indent}{} ", tool_icon(&tool_name)),
                    Style::default().fg(Color::Blue),
                ),
                Span::styled(
                    format!("[{}] ", timestamp_label(item.timestamp)),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(tool_name, Style::default().fg(Color::Green)),
                Span::styled(badge, badge_style),
                if item.hidden {
                    Span::styled(
                        format!(" · {} · click to expand", short_preview(&display_arguments)),
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::ITALIC),
                    )
                } else {
                    Span::raw("")
                },
            ]));
            layout.hits.push(hit.clone());
            if !item.hidden {
                if let Some(target) = api_target(&display_arguments) {
                    layout.lines.push(Line::from(Span::styled(
                        format!("{indent}    request · {target}"),
                        Style::default().fg(Color::Cyan),
                    )));
                    layout.hits.push(hit.clone());
                }
                for line in wrap_text(&display_arguments, width.saturating_sub(8) as usize) {
                    layout.lines.push(Line::from(Span::styled(
                        format!("{indent}    {line}"),
                        Style::default().fg(Color::DarkGray),
                    )));
                    layout.hits.push(hit.clone());
                }
                if let Some(output) = &item.output {
                    layout.lines.push(Line::from(Span::styled(
                        format!("{indent}    output"),
                        Style::default()
                            .fg(Color::Rgb(255, 140, 0))
                            .add_modifier(Modifier::BOLD),
                    )));
                    layout.hits.push(hit.clone());
                    for source in output.lines() {
                        for line in wrap_text(source, width.saturating_sub(10) as usize) {
                            layout.lines.push(Line::from(Span::styled(
                                format!("{indent}      {line}"),
                                Style::default()
                                    .fg(Color::Rgb(220, 223, 228))
                                    .bg(Color::Rgb(30, 32, 36)),
                            )));
                            layout.hits.push(hit.clone());
                        }
                    }
                }
            }
        }
        ItemKind::ToolResult => {
            let summary = short_preview(&item.text);
            layout.lines.push(Line::from(vec![
                Span::styled(
                    format!("{indent}  > "),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("output · {summary} · click to expand"),
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::ITALIC),
                ),
            ]));
            layout.hits.push(hit.clone());
            if !item.hidden {
                for source in item.text.lines() {
                    for line in wrap_text(source, width.saturating_sub(8) as usize) {
                        layout.lines.push(Line::from(Span::styled(
                            format!("{indent}  {line}"),
                            Style::default()
                                .fg(Color::Rgb(220, 223, 228))
                                .bg(Color::Rgb(30, 32, 36)),
                        )));
                        layout.hits.push(hit.clone());
                    }
                }
            }
        }
        ItemKind::Spawn => {
            layout.lines.push(Line::from(vec![
                Span::styled(format!("{indent}󰚩 "), Style::default().fg(Color::Green)),
                Span::styled(
                    format!(
                        "[{}] {}",
                        timestamp_label(item.timestamp),
                        spawn_display_label(&names().conversation, &item.text)
                    ),
                    Style::default().fg(Color::Green),
                ),
            ]));
            layout.hits.push(hit.clone());
        }
        ItemKind::SpawnResult => {
            layout.lines.push(Line::from(vec![
                Span::styled(format!("{indent}󰄬 "), Style::default().fg(Color::Green)),
                Span::styled(
                    format!(
                        "[{}] task completed · {}",
                        timestamp_label(item.timestamp),
                        short_preview(worker_objective(&item.text))
                    ),
                    Style::default().fg(Color::Green),
                ),
            ]));
            layout.hits.push(hit.clone());
        }
        ItemKind::Error => {
            let detail = worker_error_summary(&item.text, worker_id);
            for source in detail.lines() {
                let mut row = vec![
                    Span::styled(
                        format!("{indent}{} ", icon::FAILURE),
                        Style::default().fg(Color::Red),
                    ),
                    Span::styled(
                        format!("[{}] ", timestamp_label(item.timestamp)),
                        Style::default().fg(Color::DarkGray),
                    ),
                ];
                row.extend(styled_markdown(source, Style::default().fg(Color::Red)));
                layout.lines.push(Line::from(row));
                layout.hits.push(hit.clone());
            }
        }
        ItemKind::System => {
            for source in item.text.lines() {
                let summary = trace_summary(source);
                let summary = worker_id
                    .map(|id| summary.replace(id, "worker"))
                    .unwrap_or(summary);
                let summary = elide_work_id(&summary);
                layout.lines.push(Line::from(vec![
                    Span::styled(format!("{indent}· "), Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("[{}] ", timestamp_label(item.timestamp)),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(summary, Style::default().fg(Color::DarkGray)),
                ]));
                layout.hits.push(hit.clone());
            }
        }
        ItemKind::Reply if !source.is_foreground => {
            for line in item.text.lines() {
                let line = worker_id
                    .map(|id| line.replace(id, "worker"))
                    .unwrap_or_else(|| line.to_string());
                for line in wrap_text(&line, width.saturating_sub(8) as usize) {
                    layout.lines.push(Line::from(Span::styled(
                        format!("{indent}  {line}"),
                        Style::default().fg(Color::Gray),
                    )));
                    layout.hits.push(hit.clone());
                }
            }
        }
        ItemKind::User | ItemKind::PendingReply | ItemKind::Reply => {}
    }
}
