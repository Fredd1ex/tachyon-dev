//! Composer and footer drawing.
use crate::app::editor::chars;
use crate::app::model::thread::Thread;
use crate::app::navigation::{ready_earlier_turn, ready_notice};
use crate::app::ui::activity::compact_current_dir;
use crate::app::{icon, INPUT_PROMPT_MARKER, WINDOW_LOGO};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;
use std::collections::HashMap;
use tachyon_api::types::{AgentInfo, AgentState, DaemonInfo};
use tachyon_api::{BACKGROUND_ID, FOREGROUND_ID, MEMORY_ID};

pub(in crate::app) fn draw_input(
    f: &mut Frame,
    area: Rect,
    input: &str,
    cursor: usize,
    daemon: Option<&DaemonInfo>,
    foreground_busy: bool,
) {
    // Render the multi-line input with a visible cursor block.
    let cursor_visible = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() % 1000 < 500)
        .unwrap_or(true);
    let mut lines: Vec<Line> = Vec::new();
    let input_lines: Vec<&str> = if input.is_empty() {
        vec![""]
    } else {
        input.split('\n').collect()
    };

    let mut remaining = cursor;
    for (li, text) in input_lines.iter().enumerate() {
        let mut spans: Vec<Span> = Vec::new();
        // Prompt marker on the first line only.
        let prefix = if li == 0 { INPUT_PROMPT_MARKER } else { "  " };
        spans.push(Span::styled(prefix, Style::default().fg(Color::Cyan)));

        if input.is_empty() && li == 0 {
            spans.push(Span::styled(
                " ",
                if cursor_visible {
                    Style::default().bg(Color::White)
                } else {
                    Style::default()
                },
            ));
            spans.push(Span::styled(
                "Ask Tachyon anything...",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::ITALIC),
            ));
            lines.push(Line::from(spans));
            continue;
        }

        let n = chars(text);
        let cur_on_line = if remaining <= n {
            Some(remaining)
        } else {
            None
        };
        if let Some(pos) = cur_on_line {
            // Before cursor, cursor char, after cursor.
            let before: String = text.chars().take(pos).collect();
            spans.push(Span::styled(before, Style::default().fg(Color::White)));
            if pos < n {
                let c = text.chars().nth(pos).unwrap();
                spans.push(if cursor_visible {
                    Span::styled(
                        c.to_string(),
                        Style::default().fg(Color::Black).bg(Color::White),
                    )
                } else {
                    Span::styled(c.to_string(), Style::default().fg(Color::White))
                });
                let after: String = text.chars().skip(pos + 1).collect();
                spans.push(Span::styled(after, Style::default().fg(Color::White)));
            } else {
                // Cursor at end of line.
                if cursor_visible {
                    spans.push(Span::styled(" ", Style::default().bg(Color::White)));
                }
            }
        } else {
            spans.push(Span::styled(
                text.to_string(),
                Style::default().fg(Color::White),
            ));
        }
        lines.push(Line::from(spans));
        // Move past this line's chars + the newline char itself.
        if remaining >= n + 1 {
            remaining -= n + 1;
        } else {
            remaining = 0;
        }
    }

    // Daemon hint on the bottom-right is in the statusline; keep input clean.
    let _ = daemon;
    let _ = foreground_busy;
    f.render_widget(Paragraph::new(lines), area);
}

/// Compact contextual guidance that stays subordinate to the conversation.
pub(in crate::app) fn footer_mode_text(open_trace: Option<usize>, follow: bool) -> Option<String> {
    if open_trace.is_some() {
        Some(format!(
            "DETAILS    click task · Ctrl+D diagnostics · {} Pg scroll · {} Esc close · {} help",
            icon::SCROLL,
            icon::CLOSE,
            icon::HELP
        ))
    } else if !follow {
        Some(format!(
            "HISTORY    ↑↓ scroll · {} End live · {} help",
            icon::LIVE,
            icon::HELP
        ))
    } else {
        None
    }
}

pub(in crate::app) fn status_task_count(agent_infos: &HashMap<String, AgentInfo>) -> usize {
    agent_infos
        .values()
        .filter(|agent| {
            agent.id != FOREGROUND_ID
                && agent.id != MEMORY_ID
                && agent.id != BACKGROUND_ID
                && matches!(
                    agent.state,
                    AgentState::Starting | AgentState::Running | AgentState::Waiting
                )
        })
        .count()
}

pub(in crate::app) fn status_task_label(count: usize) -> Option<String> {
    match count {
        0 => None,
        1 => Some("1 task".into()),
        count => Some(format!("{count} tasks")),
    }
}

pub(in crate::app) fn draw_statusline(
    f: &mut Frame,
    area: Rect,
    daemon: Option<&DaemonInfo>,
    agent_infos: &HashMap<String, AgentInfo>,
    threads: &[Thread],
    open_trace: Option<usize>,
    follow: bool,
) {
    let (daemon_label, state_color) = match daemon {
        Some(info) if info.provider_ready => (None, Color::Green),
        Some(_) => (Some("no API key"), Color::Yellow),
        None => (Some("offline"), Color::Red),
    };
    if let Some(mode) = footer_mode_text(open_trace, follow) {
        let (label, controls) = mode.split_once("    ").unwrap_or((&mode, ""));
        let sections = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(1), Constraint::Length(2)])
            .split(area);
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(
                    format!(" {label} "),
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::White)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(controls, Style::default().fg(Color::DarkGray)),
            ])),
            sections[0],
        );
        f.render_widget(
            Paragraph::new(Span::styled("● ", Style::default().fg(state_color)))
                .alignment(Alignment::Right),
            sections[1],
        );
        return;
    }

    let task = status_task_label(status_task_count(agent_infos));
    let mut right = Vec::new();
    let mut push_right = |span: Span<'static>| {
        if !right.is_empty() {
            right.push(Span::raw("     "));
        }
        right.push(span);
    };
    if let Some(task) = task {
        push_right(Span::styled(task, Style::default().fg(Color::DarkGray)));
    }
    if let Some(ready) = ready_earlier_turn(threads).map(ready_notice) {
        push_right(Span::styled(ready, Style::default().fg(Color::Green)));
    }
    if let Some(label) = daemon_label {
        push_right(Span::styled(label, Style::default().fg(Color::DarkGray)));
    }
    push_right(Span::styled(
        format!(
            "{WINDOW_LOGO}  v{}",
            daemon
                .map(|info| info.version.as_str())
                .unwrap_or(env!("CARGO_PKG_VERSION"))
        ),
        Style::default().fg(Color::Gray),
    ));
    right.push(Span::styled(" ● ", Style::default().fg(state_color)));
    let right_width = right
        .iter()
        .map(|span| span.content.chars().count())
        .sum::<usize>() as u16;
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(1), Constraint::Length(right_width)])
        .split(area);
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                " TACHYON ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(compact_current_dir(), Style::default().fg(Color::DarkGray)),
        ])),
        sections[0],
    );
    f.render_widget(
        Paragraph::new(Line::from(right)).alignment(Alignment::Right),
        sections[1],
    );
}
