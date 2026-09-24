//! Conversation response bodies and their shared metadata area.
use super::{
    agent_count, correlated_worker_outcomes, elapsed, format_count, icon, markdown_body_lines,
    memory_badges, name_block_background, names, pending_reply_activity, sanitize_reply_text,
    schedule_badges, session_archive, timestamp_label, turn_activity, turn_response, CellLayout,
    Hit, ItemKind, Thread, TurnCell,
};
use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span},
};

/// One fixed row: reserve time first, then admit only whole badges. Clock ticks
/// can drop optional labels but cannot reflow the body or split a count label.
pub(super) fn badge_line(
    metadata: &str,
    activity: &str,
    elapsed: Option<&str>,
    width: u16,
) -> Line<'static> {
    let width = width as usize;
    let time = elapsed.unwrap_or("");
    let indent = if width >= 16 { "    " } else { "" };
    let budget = width.saturating_sub(indent.len());
    if time.len() > budget {
        let short = time.strip_prefix("elapsed ").unwrap_or(time);
        return Line::styled(
            if short.len() <= width {
                short.to_owned()
            } else {
                String::new()
            },
            Style::default().fg(Color::DarkGray),
        );
    }
    let mut badges = Vec::new();
    let mut used = time.len();
    for badge in metadata.split(" · ").filter(|s| !s.is_empty()) {
        let size = Line::raw(badge).width() + if used > 0 { 3 } else { 0 };
        if used + size <= budget {
            badges.push(badge);
            used += size;
        }
    }
    if !time.is_empty() {
        badges.push(time);
    }
    for badge in activity.split(" · ").filter(|s| !s.is_empty()) {
        let size = Line::raw(badge).width() + if used > 0 { 3 } else { 0 };
        if used + size > budget {
            break;
        }
        badges.push(badge);
        used += size;
    }
    Line::styled(
        format!("{indent}{}", badges.join(" · ")),
        Style::default().fg(Color::Gray),
    )
}

pub(super) fn main_conversation_layout(
    thread: &Thread,
    cell: &TurnCell,
    width: u16,
    latest_timestamp: u64,
    active: bool,
    activity: &str,
) -> CellLayout {
    let prompt = &thread.items[cell.prompt];
    if matches!(prompt.kind, ItemKind::Reply | ItemKind::PendingReply) {
        return standalone_reply_layout(thread, cell, width, latest_timestamp);
    }
    let mut lines = Vec::new();
    let mut hits = Vec::new();
    let mut badge_row = None;
    let mut activity_row = None;
    let mut push = |line: Line<'static>, target: Hit| {
        lines.push(line);
        hits.push(target);
        lines.len() - 1
    };
    let is_latest = cell
        .items
        .iter()
        .any(|index| thread.items[*index].timestamp == latest_timestamp);
    let metadata_style = if is_latest {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let badges = turn_cell_badges(thread, cell);
    let response = turn_response(thread, cell);
    let turn_suffix = prompt
        .turn
        .as_deref()
        .map(session_archive::display_turn)
        .map(|turn| format!("  {} {turn}", icon::TURN))
        .unwrap_or_default();
    let trailing = format!("{turn_suffix}  [{}]", timestamp_label(prompt.timestamp));
    let label = format!(" {} ", names().user);
    let mut header = vec![Span::styled(
        label,
        Style::default()
            .fg(Color::Black)
            .bg(name_block_background(Color::Gray))
            .add_modifier(Modifier::BOLD),
    )];
    let padding = (width as usize)
        .saturating_sub(Line::from(header.clone()).width() + Line::raw(&trailing).width())
        .max(1);
    header.push(Span::raw(" ".repeat(padding)));
    header.push(Span::styled(trailing, metadata_style));
    push(Line::from(header), None);
    push(Line::raw(""), None);
    let body_width = width.saturating_sub(4).min(92) as usize;
    let prompt_color = if prompt.timestamp == latest_timestamp {
        Color::White
    } else {
        Color::Gray
    };
    for line in markdown_body_lines(&prompt.text, body_width, prompt_color) {
        push(line, None);
    }
    push(Line::raw(""), None);

    let failure = prompt
        .turn
        .as_ref()
        .and_then(|turn| thread.metrics.get(turn))
        .and_then(|metrics| metrics.failure.as_deref());
    if let Some(message) = failure {
        push(
            Line::from(Span::styled(
                format!(" {} ", names().conversation),
                Style::default()
                    .fg(Color::Black)
                    .bg(name_block_background(Color::Red)),
            )),
            None,
        );
        activity_row = Some(push(Line::raw(""), None));
        for line in turn_activity::status_lines(
            &format!("Failed: {message}"),
            width,
            "    ",
            Style::default().fg(Color::Red),
        ) {
            push(line, None);
        }
        if let Some(partial) = response.filter(|item| item.kind == ItemKind::Reply) {
            push(Line::raw(""), None);
            for line in turn_activity::status_lines(
                "Partial answer (incomplete):",
                width,
                "    ",
                Style::default().fg(Color::Yellow),
            ) {
                push(line, None);
            }
            for line in
                markdown_body_lines(&sanitize_reply_text(&partial.text), body_width, Color::Gray)
            {
                push(line, None);
            }
        }
    } else if let Some(response) =
        response.or_else(|| elapsed::main_start(thread, cell).map(|_| prompt))
    {
        let response_turn_suffix = response
            .turn
            .as_deref()
            .map(session_archive::display_turn)
            .map(|turn| format!("  {} {turn}", icon::TURN))
            .unwrap_or_default();
        let mut header = vec![Span::styled(
            format!(" {} ", names().conversation),
            Style::default()
                .fg(Color::Black)
                .bg(name_block_background(Color::Green))
                .add_modifier(Modifier::BOLD),
        )];
        let trailing = format!(
            "{response_turn_suffix}  [{}]",
            timestamp_label(response.timestamp)
        );
        let padding = (width as usize)
            .saturating_sub(Line::from(header.clone()).width() + Line::raw(&trailing).width())
            .max(1);
        header.push(Span::raw(" ".repeat(padding)));
        header.push(Span::styled(trailing, metadata_style));
        push(Line::from(header), None);
        if !badges.is_empty() || elapsed::main_start(thread, cell).is_some() {
            push(Line::raw(""), None);
            // Record the slot structurally, never by searching rendered text.
            badge_row = Some(push(badge_line(&badges, "", None, width), None));
        }
        activity_row = Some(push(Line::raw(""), None));
        let response_color = if response.timestamp == latest_timestamp {
            Color::White
        } else {
            Color::Gray
        };
        let body = if response.kind != ItemKind::Reply {
            let pending = if response.kind == ItemKind::User {
                pending_reply_activity("", true)
            } else if response.text.trim().is_empty() && active {
                pending_reply_activity(activity, response.turn.is_some())
            } else {
                pending_reply_activity(&response.text, response.turn.is_some())
            };
            turn_activity::status_lines(
                &pending,
                width,
                "    ",
                Style::default().fg(response_color),
            )
        } else {
            markdown_body_lines(
                &sanitize_reply_text(&response.text),
                body_width,
                response_color,
            )
        };
        for line in body {
            push(line, None);
        }
    }
    push(Line::raw(""), None);
    let mut layout = CellLayout {
        activity_row,
        lines,
        hits,
        ..CellLayout::default()
    };
    if let Some(row) = badge_row {
        elapsed::main_badge(&mut layout, thread, cell, row);
    }
    layout
}

fn standalone_reply_layout(
    thread: &Thread,
    cell: &TurnCell,
    width: u16,
    latest_timestamp: u64,
) -> CellLayout {
    let response = &thread.items[cell.prompt];
    let metadata_style = if response.timestamp == latest_timestamp {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let trailing = format!("  [{}]", timestamp_label(response.timestamp));
    let mut header = vec![Span::styled(
        format!(" {} ", names().conversation),
        Style::default()
            .fg(Color::Black)
            .bg(name_block_background(Color::Green))
            .add_modifier(Modifier::BOLD),
    )];
    if response.turn.is_some() {
        header.push(Span::styled(
            "  RESTORED",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
        ));
    }
    let padding = (width as usize)
        .saturating_sub(Line::from(header.clone()).width() + Line::raw(&trailing).width())
        .max(1);
    header.push(Span::raw(" ".repeat(padding)));
    header.push(Span::styled(trailing, metadata_style));
    let mut lines = vec![Line::from(header), Line::raw("")];
    let mut hits = vec![None, None];
    let body_width = width.saturating_sub(4).min(92) as usize;
    let color = if response.timestamp == latest_timestamp {
        Color::White
    } else {
        Color::Gray
    };
    for line in markdown_body_lines(&sanitize_reply_text(&response.text), body_width, color) {
        lines.push(line);
        hits.push(None);
    }
    lines.push(Line::raw(""));
    hits.push(None);
    CellLayout {
        lines,
        hits,
        ..CellLayout::default()
    }
}

pub(super) fn turn_cell_badges(thread: &Thread, cell: &TurnCell) -> String {
    let mut spawned = 0;
    let mut completed = 0;
    let mut failed = 0;
    for item in cell.items.iter().map(|index| &thread.items[*index]) {
        match item.kind {
            ItemKind::Spawn => spawned += 1,
            ItemKind::SpawnResult => completed += 1,
            ItemKind::Error if item.text.starts_with("work ") => failed += 1,
            _ => {}
        }
    }
    let metrics = thread.items[cell.prompt]
        .turn
        .as_deref()
        .and_then(|turn| thread.metrics.get(turn));
    let (completed, failed) = correlated_worker_outcomes(metrics, spawned, completed, failed);
    let mut badges = Vec::new();
    if spawned > 0 {
        badges.push(format!("{} {}", icon::AGENT, agent_count(spawned)));
    }
    if completed > 0 {
        badges.push(if spawned == 0 {
            format!("{} {} complete", icon::SUCCESS, agent_count(completed))
        } else {
            format!("{} {completed} complete", icon::SUCCESS)
        });
    }
    if failed > 0 {
        badges.push(format!("{} {failed} failed", icon::FAILURE));
    }
    if let Some(done) = metrics.and_then(|metrics| metrics.completed_ms) {
        badges.push(format!("󰅐 done {:.1}s", done as f64 / 1000.0));
    }
    if let Some(metrics) = metrics {
        if let Some(self_usage) = &metrics.self_usage {
            let total = metrics
                .worker_usage
                .values()
                .fold(self_usage.total, |sum, usage| {
                    sum.saturating_add(usage.total)
                });
            badges.push(format!("{} total {}", icon::TOKENS, format_count(total)));
        }
        badges.extend(memory_badges(&metrics.memory, false));
        badges.extend(schedule_badges(&metrics.schedule, false));
    }
    badges.join(" · ")
}
