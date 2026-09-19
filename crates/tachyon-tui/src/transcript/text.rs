//! Conversation text, markdown, and compact accounting labels.
use crate::app::icon;
#[cfg(test)]
use crate::app::model::items::Item;
use crate::app::model::items::ItemKind;
use crate::app::model::metrics::{MemoryTurnMetrics, ScheduleTurnMetrics, TurnMetrics};
use crate::app::model::thread::Thread;
use crate::app::ui::activity::short_preview;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
#[cfg(test)]
use tachyon_api::types::AgentState;

pub(in crate::app) fn sanitize_reply_text(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let text = if let Some(dsml) = lower.find("dsml") {
        let protocol = &lower[dsml..];
        if protocol.contains("tool_calls")
            || protocol.contains("invoke name=")
            || protocol.contains("parameter name=")
        {
            if let Some(start) = text[..dsml].rfind('<') {
                text[..start].trim_end()
            } else {
                text
            }
        } else {
            text
        }
    } else {
        text
    };
    if !text.contains('\u{fffd}') {
        return text.to_owned();
    }
    let cleaned = text
        .lines()
        .filter(|line| {
            let chars = line.chars().count();
            let replacements = line.chars().filter(|ch| *ch == '\u{fffd}').count();
            chars == 0 || replacements.saturating_mul(4) < chars
        })
        .collect::<Vec<_>>()
        .join("\n");
    if cleaned.trim().is_empty() && text.contains('\u{fffd}') {
        "[model output could not be decoded]".into()
    } else {
        cleaned
    }
}

pub(in crate::app) fn spawn_display_label(default_label: &str, text: &str) -> String {
    if let Some(id) = text.strip_prefix("spawned worker ") {
        format!("ghost {id}")
    } else if let Some((worker, objective)) = text
        .strip_prefix("worker ")
        .and_then(|text| text.split_once(": "))
    {
        format!(
            "{default_label} worker {} · {}",
            truncate_text(worker, 8),
            short_preview(objective)
        )
    } else {
        format!("{default_label} {}", short_preview(text))
    }
}

pub(in crate::app) fn worker_objective(text: &str) -> &str {
    text.split_once(": ")
        .map(|(_, objective)| objective.lines().next().unwrap_or(objective))
        .unwrap_or(text)
}

pub(in crate::app) fn agent_count(count: usize) -> String {
    format!("{count} agent{}", if count == 1 { "" } else { "s" })
}

pub(in crate::app) fn bounded_worker_outcomes(
    spawned: usize,
    completed: usize,
    failed: usize,
) -> (usize, usize) {
    let completed = completed.min(spawned);
    let failed = failed.min(spawned.saturating_sub(completed));
    (completed, failed)
}

#[allow(dead_code)]
pub(in crate::app) fn worker_progress_badge(thread: &Thread, turn: Option<&str>) -> Option<String> {
    let turn = turn?;
    let spawned = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Spawn && item.turn.as_deref() == Some(turn))
        .count();
    if spawned == 0 {
        return None;
    }
    let completed = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::SpawnResult && item.turn.as_deref() == Some(turn))
        .count();
    Some(if completed == 0 {
        format!("󰚩 {} running", agent_count(spawned))
    } else if completed == spawned {
        format!("󰄬 {} complete", agent_count(completed))
    } else {
        format!("󰚩 {} · 󰄬 {completed} complete", agent_count(spawned))
    })
}

pub(in crate::app) fn is_worker_runtime_detail(text: &str) -> bool {
    text.contains("agent-browser ready")
        || text.starts_with("[ghost] workspace:")
        || text.starts_with("[ghost] model:")
        || text == "[ghost] ready"
        || text.starts_with("[foreground] workspace:")
        || text.starts_with("[foreground] model:")
        || text == "[foreground] ready"
        || text == "[ready] startup"
}

pub(in crate::app) fn trace_summary(text: &str) -> String {
    if let Some(timing) = text.strip_prefix("[timing] ") {
        let Some((stage, elapsed)) = timing.rsplit_once(' ') else {
            return format!("{} {timing}", icon::DURATION);
        };
        let elapsed = elapsed
            .strip_suffix("ms")
            .and_then(|millis| millis.parse::<u64>().ok())
            .map(human_millis)
            .unwrap_or_else(|| elapsed.to_string());
        let label = stage.replace('_', " ");
        if stage.ends_with("_started") {
            return format!(
                "{} {} · +{elapsed}",
                icon::DURATION,
                label.trim_end_matches(" started")
            );
        }
        if stage.ends_with("_completed") {
            return format!(
                "{} {} · {elapsed}",
                icon::SUCCESS,
                label.trim_end_matches(" completed")
            );
        }
        return match stage {
            "ready" => format!("{} ready · +{elapsed}", icon::WAITING),
            "publication_started" => format!("{} publishing · {elapsed}", icon::RUNNING),
            "completed" => format!("{} turn completed · {elapsed}", icon::SUCCESS),
            _ => format!("{} {label} · {elapsed}", icon::DURATION),
        };
    }
    if let Some(detail) = text.strip_prefix("[working]") {
        let detail = detail.trim();
        return if detail.is_empty() {
            format!("{} working", icon::RUNNING)
        } else {
            format!("{} working · {detail}", icon::RUNNING)
        };
    }
    if let Some(detail) = text.strip_prefix("[ready]") {
        let detail = detail.trim();
        return if detail.is_empty() {
            format!("{} ready", icon::WAITING)
        } else {
            format!("{} ready · {detail}", icon::WAITING)
        };
    }
    if let Some(detail) = text.strip_prefix("[warning]") {
        return format!("{} {}", icon::WARNING, detail.trim());
    }
    text.to_string()
}

pub(in crate::app) fn human_millis(millis: u64) -> String {
    if millis < 1_000 {
        format!("{millis}ms")
    } else {
        format!("{:.1}s", millis as f64 / 1_000.0)
    }
}

// ---- main loop -----------------------------------------------------------

pub(in crate::app) fn truncate_text(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_string();
    }
    if width <= 3 {
        return text.chars().take(width).collect();
    }
    format!("{}...", text.chars().take(width - 3).collect::<String>())
}

#[cfg(test)]
pub(in crate::app) fn lifecycle_badge(state: AgentState) -> String {
    match state {
        AgentState::Created | AgentState::Starting => "◌ starting".into(),
        AgentState::Running => "● running".into(),
        AgentState::Waiting => "◌ waiting".into(),
        AgentState::Staged => "◌ staged".into(),
        AgentState::Completed => "✓ complete".into(),
        AgentState::Failed => "× failed".into(),
        AgentState::Interrupted => "× stopped".into(),
        AgentState::Terminated => "× killed".into(),
        AgentState::Released => "✓ released".into(),
    }
}

pub(in crate::app) fn pending_reply_activity(activity: &str, accepted: bool) -> String {
    let activity = activity.trim();
    if activity.is_empty() {
        return if accepted {
            "Checking information...".into()
        } else {
            "Submitting...".into()
        };
    }
    if activity.ends_with(['.', '!', '?']) {
        activity.to_string()
    } else {
        format!("{activity}...")
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::app) fn correlated_worker_outcomes(
    metrics: Option<&TurnMetrics>,
    spawned: usize,
    completed: usize,
    failed: usize,
) -> (usize, usize) {
    if let Some(metrics) = metrics.filter(|metrics| !metrics.worker_outcomes.is_empty()) {
        let completed = metrics
            .worker_outcomes
            .values()
            .filter(|success| **success)
            .count();
        return bounded_worker_outcomes(
            spawned,
            completed,
            metrics.worker_outcomes.len() - completed,
        );
    }
    bounded_worker_outcomes(spawned, completed, failed)
}

#[cfg(test)]
pub(in crate::app) fn turn_badges(
    thread: &Thread,
    turn: Option<&str>,
    timestamp: u64,
    show_traces: bool,
) -> String {
    let previous_reply = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Reply && item.timestamp < timestamp)
        .map(|item| item.timestamp)
        .max()
        .unwrap_or(0);
    let in_turn = |item: &&Item| match turn {
        Some(turn) => item.turn.as_deref() == Some(turn),
        None => {
            item.turn.is_none() && item.timestamp > previous_reply && item.timestamp <= timestamp
        }
    };
    let spawned = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Spawn && in_turn(item))
        .count();
    let completed = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::SpawnResult && in_turn(item))
        .count();
    let failed = thread
        .items
        .iter()
        .filter(|item| {
            item.kind == ItemKind::Error && item.text.starts_with("work ") && in_turn(item)
        })
        .count();
    let metrics = turn.and_then(|turn| thread.metrics.get(turn));
    let (completed, failed) = correlated_worker_outcomes(metrics, spawned, completed, failed);
    let started_at = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::User && item.turn.as_deref() == turn)
        .map(|item| item.timestamp)
        .min()
        .or_else(|| {
            thread
                .items
                .iter()
                .filter(|item| {
                    item.kind == ItemKind::System
                        && item.text.starts_with("[working]")
                        && in_turn(item)
                })
                .map(|item| item.timestamp)
                .min()
        })
        .or_else(|| {
            thread
                .items
                .iter()
                .filter(|item| item.kind == ItemKind::Spawn && in_turn(item))
                .map(|item| item.timestamp)
                .min()
        })
        .or_else(|| {
            thread
                .items
                .iter()
                .filter(|item| item.kind == ItemKind::User && item.timestamp < timestamp)
                .map(|item| item.timestamp)
                .max()
        });
    let mut badges = Vec::new();
    if spawned > 0 {
        badges.push(if show_traces {
            format!("󰚩 workers started {spawned}")
        } else {
            format!("󰚩 {}", agent_count(spawned))
        });
    }
    let first = metrics.and_then(|metrics| metrics.first_visible_ms);
    let done = metrics.and_then(|metrics| metrics.completed_ms);
    if completed > 0 {
        badges.push(if show_traces {
            format!("󰄬 workers completed {completed}")
        } else if spawned == 0 {
            format!("󰄬 {} complete", agent_count(completed))
        } else {
            format!("󰄬 {completed} complete")
        });
    }
    if failed > 0 {
        badges.push(if show_traces {
            format!("× workers failed {failed}")
        } else {
            format!("× {failed} failed")
        });
    }
    if let Some(first) = first {
        if show_traces {
            badges.push(format!("󱎫 first response {:.1}s", first as f64 / 1000.0));
        }
    } else if let Some(started_at) = started_at {
        let elapsed = timestamp.saturating_sub(started_at) as f64 / 1000.0;
        if show_traces {
            badges.push(format!("󱎫 first response {:.1}s", elapsed));
        }
    }
    if let Some(done) = done {
        badges.push(if show_traces {
            format!("󰅐 response completed {:.1}s", done as f64 / 1000.0)
        } else {
            format!("󰅐 done {:.1}s", done as f64 / 1000.0)
        });
    }
    if let Some(metrics) = metrics {
        if let Some(self_usage) = &metrics.self_usage {
            let total = metrics
                .worker_usage
                .values()
                .fold(self_usage.total, |sum, usage| {
                    sum.saturating_add(usage.total)
                });
            if show_traces {
                badges.push(format!(
                    "{} foreground tokens {} (prompt {} + completion {})",
                    icon::TOKENS,
                    format_count(self_usage.total),
                    format_count(self_usage.prompt),
                    format_count(self_usage.completion)
                ));
            }
            badges.push(if show_traces {
                format!("{} aggregate tokens {}", icon::TOKENS, format_count(total))
            } else {
                format!("{} total {}", icon::TOKENS, format_count(total))
            });
        }
        badges.extend(memory_badges(&metrics.memory, show_traces));
        badges.extend(schedule_badges(&metrics.schedule, show_traces));
    } else if let Some(turn) = turn.and_then(|turn| turn.parse::<u64>().ok()) {
        if let Some((prompt, completion, total)) = thread.usage.get(&turn) {
            if show_traces {
                badges.push(format!(
                    "{} reported tokens {} (prompt {} + completion {})",
                    icon::TOKENS,
                    format_count(*total),
                    format_count(*prompt),
                    format_count(*completion)
                ));
            } else {
                badges.push(format!("{} total {}", icon::TOKENS, format_count(*total)));
            }
        }
    }
    badges.join(" · ")
}

pub(in crate::app) fn memory_badges(memory: &MemoryTurnMetrics, detailed: bool) -> Vec<String> {
    let mut badges = Vec::new();
    if memory.saved > 0 {
        badges.push(if detailed {
            format!("memory saved {}", memory.saved)
        } else {
            "memory saved".into()
        });
    }
    if memory.forgotten > 0 {
        badges.push(if detailed {
            format!("memory forgotten {}", memory.forgotten)
        } else {
            "memory forgotten".into()
        });
    }
    if memory.corrected > 0 {
        badges.push(if detailed {
            format!("memory updated {}", memory.corrected)
        } else {
            "memory updated".into()
        });
    }
    let recalled = memory
        .recalled_preferences
        .saturating_add(memory.recalled_history);
    if recalled > 0 {
        badges.push(if detailed {
            format!(
                "memory recalled {} preferences + {} history",
                memory.recalled_preferences, memory.recalled_history
            )
        } else {
            format!("memory recalled {recalled}")
        });
    }
    if memory.failed > 0 {
        badges.push("memory unchanged".into());
    }
    badges
}

pub(in crate::app) fn schedule_badges(
    schedule: &ScheduleTurnMetrics,
    detailed: bool,
) -> Vec<String> {
    let mut badges = Vec::new();
    if schedule.scheduled > 0 {
        badges.push(if detailed {
            format!("reminders scheduled {}", schedule.scheduled)
        } else {
            "reminder scheduled".into()
        });
    }
    if schedule.tasks_scheduled > 0 {
        badges.push(if detailed {
            format!("agent tasks scheduled {}", schedule.tasks_scheduled)
        } else {
            "agent task scheduled".into()
        });
    }
    if schedule.cancelled > 0 {
        badges.push(if detailed {
            format!("reminders cancelled {}", schedule.cancelled)
        } else {
            "reminder cancelled".into()
        });
    }
    if schedule.fired > 0 {
        badges.push(if detailed {
            format!("reminders fired {}", schedule.fired)
        } else {
            "reminder fired".into()
        });
    }
    badges
}

pub(in crate::app) fn format_count(value: impl Into<u64>) -> String {
    let value = value.into();
    if value >= 1_000_000 {
        format!("{:.1}m", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

pub(in crate::app) fn tool_parts(text: &str) -> (String, String) {
    let text = text.trim();
    let (name, raw_args) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
    let args = match serde_json::from_str::<serde_json::Value>(raw_args.trim()) {
        Ok(value) => {
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| raw_args.trim().into())
        }
        Err(_) => raw_args.trim().to_string(),
    };
    (name.to_string(), args)
}

pub(in crate::app) fn api_target(arguments: &str) -> Option<String> {
    let start = arguments
        .find("https://")
        .or_else(|| arguments.find("http://"))?;
    let rest = &arguments[start..];
    let end = rest
        .char_indices()
        .find(|(_, ch)| matches!(ch, '"' | '\'' | '`' | ' ' | '\n' | '&' | ')'))
        .map(|(index, _)| index)
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

pub(in crate::app) fn tool_icon(name: &str) -> &'static str {
    let name = name.to_ascii_lowercase();
    if name.contains("browser") || name.contains("http") || name.contains("fetch") {
        icon::BROWSER
    } else if name.contains("search") || name.contains("grep") || name.contains("glob") {
        icon::SEARCH
    } else if name.contains("file") || name.contains("read") || name.contains("write") {
        icon::FILE
    } else {
        icon::TOOL
    }
}

pub(in crate::app) fn flattened_list_parts(line: &str) -> Option<Vec<&str>> {
    let parts = line.split(" - ").collect::<Vec<_>>();
    (parts.len() >= 3
        && parts[1..]
            .iter()
            .all(|part| part.trim().chars().count() >= 2))
    .then_some(parts)
}

pub(in crate::app) fn markdown_body_lines(
    text: &str,
    width: usize,
    color: Color,
) -> Vec<Line<'static>> {
    let base = Style::default().fg(color);
    let mut lines = Vec::new();
    for source in text.split('\n') {
        if source.trim().is_empty() {
            lines.push(Line::raw(""));
            continue;
        }
        let trimmed = source.trim();
        let explicit = ["- ", "* ", "• "]
            .into_iter()
            .find_map(|marker| trimmed.strip_prefix(marker));
        let (lead, entries) = if let Some(entry) = explicit {
            (None, vec![entry])
        } else if let Some(parts) = flattened_list_parts(trimmed) {
            (Some(parts[0]), parts[1..].to_vec())
        } else {
            (Some(trimmed), Vec::new())
        };
        if let Some(lead) = lead.filter(|lead| !lead.trim().is_empty()) {
            for chunk in wrap_text(lead, width) {
                let mut row = vec![Span::raw("    ")];
                row.extend(styled_markdown(&chunk, base));
                lines.push(Line::from(row));
            }
        }
        for entry in entries {
            let chunks = wrap_text(entry.trim(), width.saturating_sub(2));
            for (index, chunk) in chunks.into_iter().enumerate() {
                let mut row = if index == 0 {
                    vec![
                        Span::raw("    "),
                        Span::styled(format!("{} ", icon::BULLET), base),
                    ]
                } else {
                    vec![Span::raw("      ")]
                };
                row.extend(styled_markdown(&chunk, base));
                lines.push(Line::from(row));
            }
        }
    }
    lines
}

/// Wrap text at word boundaries, hard-splitting only words wider than the view.
pub(in crate::app) fn wrap_text(s: &str, width: usize) -> Vec<String> {
    let width = width.max(10);
    if s.is_empty() {
        return vec![String::new()];
    }
    let mut out = Vec::new();
    let mut line = String::new();
    for word in s.split_whitespace() {
        if word.chars().count() > width {
            if !line.is_empty() {
                out.push(std::mem::take(&mut line));
            }
            let chars: Vec<char> = word.chars().collect();
            for chunk in chars.chunks(width) {
                out.push(chunk.iter().collect());
            }
        } else if line.is_empty() {
            line.push_str(word);
        } else if line.chars().count() + 1 + word.chars().count() <= width {
            line.push(' ');
            line.push_str(word);
        } else {
            out.push(std::mem::take(&mut line));
            line.push_str(word);
        }
    }
    if !line.is_empty() {
        out.push(line);
    }
    out
}

/// Render the small Markdown subset commonly returned by the assistant.
pub(in crate::app) fn styled_markdown(text: &str, base: Style) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut rest = text;
    let mut emphasis = false;
    let mut code = false;
    while !rest.is_empty() {
        let marker = if rest.starts_with("**") {
            Some((2, true))
        } else if rest.starts_with('`') {
            Some((1, false))
        } else {
            None
        };
        if let Some((len, is_emphasis)) = marker {
            if is_emphasis {
                emphasis = !emphasis;
            } else {
                code = !code;
            }
            rest = &rest[len..];
            continue;
        }
        let next = rest
            .find("**")
            .into_iter()
            .chain(rest.find('`'))
            .min()
            .unwrap_or(rest.len());
        let chunk = &rest[..next];
        let mut style = base;
        if emphasis {
            style = style.add_modifier(Modifier::BOLD);
        }
        if code {
            style = style
                .fg(Color::Rgb(255, 190, 100))
                .bg(Color::Rgb(35, 35, 35));
        }
        spans.push(Span::styled(chunk.to_string(), style));
        rest = &rest[next..];
    }
    spans
}
