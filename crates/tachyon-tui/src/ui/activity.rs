//! Activity state labels and animation styling.
use crate::app::model::items::ItemKind;
use crate::app::model::thread::Thread;
use crate::app::name_block_background;
use crate::app::transcript::text::truncate_text;
use ratatui::style::{Color, Style};
use ratatui::text::Span;
use tachyon_api::types::{AgentInfo, AgentState};

#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq)]
pub(in crate::app) enum ActivityState {
    Ready,
    Completed,
    Thinking,
    Working,
    Waiting,
    Composing,
    Error,
}

#[allow(dead_code)]
pub(in crate::app) fn thread_state(thread: &Thread) -> ActivityState {
    if thread
        .items
        .last()
        .map(|item| item.kind == ItemKind::Error)
        .unwrap_or(false)
    {
        return ActivityState::Error;
    }
    if thread.streaming {
        return ActivityState::Composing;
    }
    if thread
        .items
        .iter()
        .any(|item| item.kind == ItemKind::Tool && item.output.is_none())
    {
        return ActivityState::Working;
    }
    if let Some(last) = thread.items.last() {
        match last.kind {
            ItemKind::Reply | ItemKind::Tool | ItemKind::ToolResult | ItemKind::SpawnResult => {
                return ActivityState::Ready;
            }
            ItemKind::System if last.text.contains("queued") || last.text.contains("waiting") => {
                return ActivityState::Waiting;
            }
            ItemKind::System
                if last.text.contains("done")
                    || last.text.contains("exited")
                    || last.text.contains("complete") =>
            {
                return ActivityState::Completed;
            }
            ItemKind::System | ItemKind::User => return ActivityState::Thinking,
            ItemKind::PendingReply => return ActivityState::Waiting,
            ItemKind::Spawn => return ActivityState::Working,
            ItemKind::Error => return ActivityState::Error,
        }
    }
    ActivityState::Ready
}

pub(in crate::app) fn agent_pane_status(
    info: &AgentInfo,
    reviewing: bool,
) -> (&'static str, Color) {
    if reviewing {
        return ("reviewing", Color::Yellow);
    }
    match info.state {
        AgentState::Created | AgentState::Starting => ("starting", Color::Yellow),
        AgentState::Running => ("running", Color::Cyan),
        AgentState::Waiting => ("waiting", Color::Yellow),
        AgentState::Staged => ("staged", Color::Yellow),
        AgentState::Completed if info.retained => ("idle · retained", Color::Green),
        AgentState::Completed => ("completed", Color::Green),
        AgentState::Failed => ("failed", Color::Red),
        AgentState::Interrupted => ("stopped", Color::DarkGray),
        AgentState::Terminated => ("killed", Color::DarkGray),
        AgentState::Released => ("released", Color::DarkGray),
    }
}

#[allow(dead_code)]
pub(in crate::app) fn wave_label_spans(label: &str, block_color: Color) -> Vec<Span<'static>> {
    label
        .chars()
        .map(|ch| {
            Span::styled(
                ch.to_string(),
                Style::default()
                    .fg(Color::Black)
                    .bg(name_block_background(block_color)),
            )
        })
        .collect()
}

/// Time-based spinner frame (animates while an agent is active).
pub(in crate::app) fn spinner_glyph() -> &'static str {
    const F: [&str; 8] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧"];
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    F[(ms / 80) as usize % F.len()]
}

pub(in crate::app) fn short_preview(text: &str) -> String {
    let first = text.lines().next().unwrap_or("").trim();
    if first.chars().count() > 40 {
        format!("{}…", first.chars().take(40).collect::<String>())
    } else if first.is_empty() {
        "…".to_string()
    } else {
        first.to_string()
    }
}

pub(in crate::app) fn compact_current_dir() -> String {
    let Ok(path) = std::env::current_dir() else {
        return "?".into();
    };
    let full = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .and_then(|home| {
            path.strip_prefix(home)
                .ok()
                .map(std::path::Path::to_path_buf)
        })
        .map(|relative| format!("~/{}", relative.display()))
        .unwrap_or_else(|| path.display().to_string());
    if full.chars().count() <= 28 {
        return full;
    }
    let parts: Vec<String> = path
        .components()
        .filter_map(|component| {
            let text = component.as_os_str().to_string_lossy();
            (!text.is_empty()).then_some(text.into_owned())
        })
        .collect();
    if parts.len() >= 2 {
        format!("…/{}/{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        truncate_text(&full, 28)
    }
}
