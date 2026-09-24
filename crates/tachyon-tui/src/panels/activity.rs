//! Exact-scope activity projection. No neighboring-turn or global-worker fallback.
use crate::app::transcript::trace::worker_record;
use crate::app::{icon, transcript::text::tool_icon};
use crate::app::{ItemKind, Thread};

pub(in crate::app) type RowId = (usize, usize);
pub(in crate::app) struct ActivityRow {
    pub id: RowId,
    pub revision: u64,
    pub label: String,
}

pub(in crate::app) fn project(
    threads: &[Thread],
    foreground: usize,
    turn: Option<&str>,
) -> Vec<ActivityRow> {
    let Some(turn) = turn else {
        return Vec::new();
    };
    let terminal = threads[foreground].completed_turns.contains(turn)
        || threads[foreground]
            .metrics
            .get(turn)
            .is_some_and(|m| m.ended_at_ms.is_some());
    let mut rows = Vec::new();
    for (ti, thread) in threads.iter().enumerate() {
        if ti != foreground && thread.is_foreground {
            continue;
        }
        for (ii, item) in thread.items.iter().enumerate() {
            if item.turn.as_deref() != Some(turn) {
                continue;
            }
            // A worker's current task/name is mutable across assignments. Only
            // persisted host work evidence can attribute its task to this turn.
            if ti != foreground && item.work.is_none() {
                continue;
            }
            if item.kind == ItemKind::Spawn {
                if let Some((id, _)) = worker_record(&item.text) {
                    if thread.items.iter().any(|other| {
                        other.turn == item.turn
                            && (other
                                .work
                                .as_ref()
                                .is_some_and(|w| w.tool.is_none() && w.key.work_id == id)
                                || (other.kind == ItemKind::SpawnResult
                                    && worker_record(&other.text)
                                        .is_some_and(|(key, _)| key == id)))
                    }) {
                        continue;
                    }
                    if thread.items[..ii].iter().any(|other| {
                        other.turn == item.turn
                            && other.kind == ItemKind::Spawn
                            && worker_record(&other.text).is_some_and(|(key, _)| key == id)
                    }) {
                        continue;
                    }
                }
            }
            let canonical = item
                .work
                .as_ref()
                .and_then(|w| thread.canonical_works.get(&w.key.work_id));
            let label = if let Some(work) = canonical {
                format!("{} - {:?}", safe(&work.title, 140), work.phase)
            } else if let Some(work) = &item.work {
                if let Some(tool) = &work.tool {
                    let error = tool.output.get("is_error").and_then(|v| v.as_bool()) == Some(true)
                        || tool.output.get("error").is_some_and(|v| !v.is_null())
                        || tool.output.get("status").and_then(|v| v.as_str()) == Some("error")
                        || tool
                            .output
                            .get("exit_code")
                            .or_else(|| tool.output.pointer("/metadata/exit_code"))
                            .and_then(|v| v.as_i64())
                            .is_some_and(|n| n != 0)
                        || tool
                            .output
                            .get("timed_out")
                            .or_else(|| tool.output.pointer("/metadata/timed_out"))
                            .and_then(|v| v.as_bool())
                            == Some(true);
                    let task = thread
                        .items
                        .iter()
                        .find(|i| {
                            i.turn == item.turn
                                && i.work
                                    .as_ref()
                                    .is_some_and(|w| w.key == work.key && w.tool.is_none())
                        })
                        .and_then(|i| worker_record(&i.text).and_then(|(_, objective)| objective));
                    format!(
                        "{} / {} {} - {}",
                        safe(task.unwrap_or("Work"), 100),
                        tool_icon(&tool.tool_name),
                        safe(&tool.tool_name, 80),
                        if error { "error" } else { "recorded" }
                    )
                } else {
                    let task = worker_record(&item.text)
                        .and_then(|(_, objective)| objective)
                        .unwrap_or("Work");
                    let result = item.text.split_once('\n').map(|(_, s)| s).unwrap_or("");
                    let status = if item.kind == ItemKind::SpawnResult {
                        "complete"
                    } else if result.starts_with("blocked:") {
                        "blocked"
                    } else if result.starts_with("cancelled:") {
                        "cancelled"
                    } else if result == "timed out" {
                        "timed out"
                    } else {
                        "failed"
                    };
                    format!(
                        "{} {} - {status}",
                        if status == "complete" {
                            icon::SUCCESS
                        } else {
                            icon::FAILURE
                        },
                        safe(task, 140)
                    )
                }
            } else if item.kind == ItemKind::Tool {
                format!(
                    "{} {} - {}",
                    tool_icon(item.text.split_whitespace().next().unwrap_or("Tool")),
                    safe(item.text.split_whitespace().next().unwrap_or("Tool"), 100),
                    if item.output.is_some() {
                        "result recorded"
                    } else if terminal || ii < thread.history_len {
                        "outcome unknown"
                    } else {
                        "started"
                    }
                )
            } else if matches!(item.kind, ItemKind::Spawn | ItemKind::SpawnResult) {
                let Some((id, task)) = worker_record(&item.text) else {
                    continue;
                };
                let reused = thread
                    .items
                    .iter()
                    .filter(|other| {
                        other.turn == item.turn
                            && other.kind == ItemKind::Spawn
                            && worker_record(&other.text)
                                .is_some_and(|(other_id, _)| other_id == id)
                    })
                    .count()
                    > 1;
                format!(
                    "{} {} - {}",
                    if item.kind == ItemKind::SpawnResult {
                        icon::SUCCESS
                    } else if terminal || reused || ii < thread.history_len {
                        icon::WARNING
                    } else {
                        icon::RUNNING
                    },
                    safe(task.unwrap_or("Work"), 140),
                    if item.kind == ItemKind::SpawnResult {
                        "complete"
                    } else if terminal || reused || ii < thread.history_len {
                        "outcome unknown"
                    } else {
                        "started"
                    }
                )
            } else if item.kind == ItemKind::Error {
                "Error - details available".into()
            } else {
                continue;
            };
            rows.push(ActivityRow {
                id: (ti, ii),
                revision: item.revision,
                label,
            });
        }
    }
    rows
}

/// Compact titles are mechanical previews, not guesses about task semantics.
pub(in crate::app) fn compact(text: &str, width: usize) -> String {
    use ratatui::text::Span;
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if Span::raw(&text).width() <= width {
        return text;
    }
    let mut result = String::new();
    for ch in text.chars() {
        if Span::raw(&result).width() + Span::raw(ch.to_string()).width() + 1 > width {
            break;
        }
        result.push(ch);
    }
    if width > 0 {
        result.push('…');
    }
    result
}

pub(in crate::app) fn task_title(threads: &[Thread], row: &ActivityRow) -> String {
    let item = &threads[row.id.0].items[row.id.1];
    let title = worker_record(&item.text)
        .and_then(|(_, task)| task)
        .unwrap_or_else(|| item.text.split_whitespace().next().unwrap_or("Task"));
    let title = title.split(['\n', ';']).next().unwrap_or(title);
    safe(title, 512).replace(" [truncated]", "…")
}

pub(in crate::app) fn compact_row(threads: &[Thread], row: &ActivityRow, width: usize) -> String {
    let item = &threads[row.id.0].items[row.id.1];
    let status = row.label.rsplit_once(" - ").map(|(_, s)| s).unwrap_or("");
    let title = task_title(threads, row);
    let timing = item
        .work
        .as_ref()
        .and_then(|w| w.timing.as_ref())
        .and_then(|t| t.execution_ms)
        .map(|ms| format!(" {:.1}s", ms as f64 / 1000.0))
        .unwrap_or_default();
    aligned_row(&title, &format!("{status}{timing}"), width)
}

pub(in crate::app) fn latest(
    threads: &[Thread],
    foreground: usize,
    row: &ActivityRow,
    width: usize,
) -> String {
    let item = &threads[row.id.0].items[row.id.1];
    let canonical = item
        .work
        .as_ref()
        .and_then(|w| threads[row.id.0].canonical_works.get(&w.key.work_id));
    let text = if let Some(work) = canonical {
        let tool = work
            .latest_tool
            .as_ref()
            .map(|t| {
                format!(
                    "{} ({})",
                    t.name,
                    if t.finished { "finished" } else { "running" }
                )
            })
            .unwrap_or_else(|| "No tool activity".into());
        format!(
            "{tool}; {} started, {} finished",
            work.metrics.tools_started, work.metrics.tools_finished
        )
    } else if let Some(work) = &item.work {
        if work.tool.is_none() {
            safe(
                item.text
                    .split_once('\n')
                    .map(|(_, s)| s)
                    .unwrap_or("Result recorded"),
                256,
            )
        } else {
            row.label.clone()
        }
    } else if item.kind == ItemKind::SpawnResult {
        safe(
            item.text
                .split_once('\n')
                .map(|(_, s)| s)
                .unwrap_or("Complete"),
            256,
        )
    } else if let Some((id, _)) = worker_record(&item.text) {
        item.turn
            .as_deref()
            .filter(|turn| {
                !threads[foreground].completed_turns.contains(*turn)
                    && row.id.1 >= threads[row.id.0].history_len
            })
            .and_then(|turn| threads[foreground].activity.latest(turn, id))
            .map(str::to_owned)
            .unwrap_or_else(|| {
                row.label
                    .rsplit_once(" - ")
                    .map(|(_, s)| s)
                    .unwrap_or("No correlated tool activity")
                    .to_owned()
            })
    } else if item.kind == ItemKind::Tool {
        let (name, args) = item
            .text
            .split_once(char::is_whitespace)
            .unwrap_or((&item.text, ""));
        let path = (args.len() <= 64 * 1024)
            .then(|| serde_json::from_str::<serde_json::Value>(args).ok())
            .flatten()
            .and_then(|v| {
                v.get("filePath")
                    .or_else(|| v.get("path"))
                    .and_then(|v| v.as_str())
                    .map(|s| safe(s, 256))
            });
        let status = row.label.rsplit_once(" - ").map(|(_, s)| s).unwrap_or("");
        format!(
            "{}{} - {status}",
            safe(name, 80),
            path.map(|p| format!(" {p}")).unwrap_or_default()
        )
    } else {
        row.label.clone()
    };
    let indent = if width >= 4 { "  " } else { "" };
    format!(
        "{indent}{}",
        compact(&format!("-> {text}"), width.saturating_sub(indent.len()))
    )
}

pub(in crate::app) fn aligned_row(title: &str, status: &str, width: usize) -> String {
    let right = compact(status, width);
    let right_width = ratatui::text::Span::raw(&right).width();
    let left = compact(title, width.saturating_sub(right_width + 2).min(56));
    let gap = width.saturating_sub(ratatui::text::Span::raw(&left).width() + right_width);
    format!("{left}{}{right}", " ".repeat(gap))
}

/// Default monitoring shows current tasks, not the retained diagnostic log.
pub(in crate::app) fn tasks(
    threads: &[Thread],
    foreground: usize,
    turn: Option<&str>,
) -> Vec<ActivityRow> {
    let mut rows = project(threads, foreground, turn);
    let has_tasks = rows.iter().any(|row| {
        let item = &threads[row.id.0].items[row.id.1];
        matches!(item.kind, ItemKind::Spawn | ItemKind::SpawnResult)
            || item.work.as_ref().is_some_and(|w| w.tool.is_none())
    });
    let mut newest = std::collections::BTreeMap::new();
    let mut worker_work =
        std::collections::BTreeMap::<&str, std::collections::BTreeSet<&str>>::new();
    for row in &rows {
        if let Some(work) = &threads[row.id.0].items[row.id.1].work {
            let fence = (work.key.generation, work.key.assignment);
            newest
                .entry(work.key.work_id.clone())
                .and_modify(|old| *old = std::cmp::max(*old, fence))
                .or_insert(fence);
            let source = &threads[row.id.0];
            if !source.is_foreground && work.tool.is_none() {
                // Archived worker IDs have a visit namespace; the already exact
                // turn filter supplies that same namespace on every record.
                let worker = source
                    .id
                    .strip_prefix("visit:")
                    .and_then(|id| id.split_once(':').map(|(_, id)| id))
                    .unwrap_or(&source.id);
                worker_work
                    .entry(worker)
                    .or_default()
                    .insert(&work.key.work_id);
            }
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    rows.retain(|row| {
        let item = &threads[row.id.0].items[row.id.1];
        if let Some(work) = &item.work {
            return work.tool.is_none()
                && newest.get(&work.key.work_id)
                    == Some(&(work.key.generation, work.key.assignment))
                && seen.insert(work.key.work_id.clone());
        }
        if item.kind == ItemKind::Tool {
            return !has_tasks;
        }
        if item.kind == ItemKind::Spawn && row.label.ends_with(" - outcome unknown") {
            // Retain unmatched legacy lifecycle evidence in diagnostics only.
            return false;
        }
        if let Some((id, _)) = worker_record(&item.text) {
            // A uniquely attributed host result represents this worker's start,
            // even when the opaque worker ID differs from its work ID. Never
            // join unrelated work by a matching objective or mutable task name.
            if worker_work.get(id).is_some_and(|work| work.len() == 1) {
                return false;
            }
            return !newest.contains_key(id) && seen.insert(id.to_owned());
        }
        true
    });
    rows
}

// Evidence is untrusted terminal text. Redact credential-shaped text as well as
// sensitive JSON fields; never display a URL's credentials, query or fragment.
pub(in crate::app) fn safe(text: &str, limit: usize) -> String {
    let text: String = text
        .chars()
        .take(limit)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let lower = text.to_ascii_lowercase();
    if [
        "bearer ",
        "api_key",
        "api-key",
        "apikey",
        "password",
        "secret",
        "token",
        "authorization",
        "credential",
        "cookie",
        "private_key",
        "private key",
        "sk-",
    ]
    .iter()
    .any(|s| lower.contains(s))
    {
        return "[redacted]".into();
    }
    if text.starts_with("https://") || text.starts_with("http://") {
        let clean = text.split(['?', '#']).next().unwrap_or("");
        if clean.contains('@') {
            return "[redacted URL]".into();
        }
        return clean.to_owned();
    }
    if text.contains("://") {
        return "[embedded URL withheld]".into();
    }
    if text.chars().count() == limit {
        format!("{text} [truncated]")
    } else {
        text
    }
}

fn preview(value: &serde_json::Value, depth: usize) -> String {
    if depth == 0 {
        return "[bounded]".into();
    }
    match value {
        serde_json::Value::Object(fields) => fields
            .iter()
            .take(16)
            .map(|(k, v)| {
                let key = safe(k, 80);
                format!(
                    "{key}: {}",
                    if key == "[redacted]" {
                        key.clone()
                    } else {
                        preview(v, depth - 1)
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::Array(values) => values
            .iter()
            .take(8)
            .map(|v| preview(v, depth - 1))
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::String(s) => safe(s, 512),
        other => other.to_string(),
    }
}

pub(in crate::app) fn details(threads: &[Thread], row: &ActivityRow, raw: bool) -> String {
    #[cfg(test)]
    super::tests::DETAIL_FORMATS.with(|n| n.set(n.get() + 1));
    let item = &threads[row.id.0].items[row.id.1];
    let mut parts = Vec::new();
    if let Some(work) = &item.work {
        if let Some(tool) = &work.tool {
            for field in ["sources", "results", "error"] {
                if let Some(value) = tool.output.get(field).filter(|v| !v.is_null()) {
                    parts.push(format!("{field}: {}", preview(value, 3)));
                }
            }
            // Arguments and native output require the secondary raw opt-in.
            if raw {
                parts.push(format!("Arguments: {}", preview(&tool.arguments, 3)));
                parts.push(format!("Output: {}", preview(&tool.output, 3)));
            }
        } else {
            parts.push(format!(
                "Result: {}",
                safe(
                    item.text
                        .split_once('\n')
                        .map(|(_, s)| s)
                        .unwrap_or("Unavailable"),
                    2048
                )
            ));
        }
        if raw {
            parts.push(format!(
                "Work {} / generation {} / assignment {} / omitted {}",
                safe(&work.key.work_id, 128),
                work.key.generation,
                work.key.assignment,
                work.omitted
            ));
        }
    } else if item.kind == ItemKind::Tool {
        if let Some(output) = item
            .output
            .as_ref()
            .filter(|s| s.len() <= 64 * 1024)
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        {
            for field in ["sources", "results", "error"] {
                if let Some(value) = output.get(field).filter(|v| !v.is_null()) {
                    parts.push(format!("{field}: {}", preview(value, 3)));
                }
            }
        }
        let args = item
            .text
            .split_once(char::is_whitespace)
            .map(|(_, args)| args)
            .unwrap_or("");
        // Legacy free-form arguments/output may contain arbitrary credentials.
        // Only structured values have a safe field-level preview.
        if raw {
            parts.push(format!(
                "Arguments: {}",
                (args.len() <= 64 * 1024)
                    .then(|| serde_json::from_str::<serde_json::Value>(args).ok())
                    .flatten()
                    .map(|v| preview(&v, 3))
                    .unwrap_or_else(|| "unstructured or oversized arguments withheld".into())
            ));
        }
        if raw {
            parts.push(format!(
                "Output: {}",
                item.output
                    .as_ref()
                    .filter(|s| s.len() <= 64 * 1024)
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                    .map(|v| preview(&v, 3))
                    .unwrap_or_else(|| "unstructured or oversized output withheld".into())
            ));
        }
    } else {
        parts.push(safe(&item.text, 2048));
    }
    parts.join("\n").chars().take(8192).collect()
}

pub(in crate::app) fn wrap(text: &str, width: u16) -> Vec<String> {
    let width = usize::from(width.max(1));
    let mut rows = Vec::new();
    for source in text.lines() {
        let mut line = String::new();
        let mut used = 0;
        for c in source.chars() {
            let mut glyph = c.to_string();
            let mut size = ratatui::text::Span::raw(glyph.clone()).width();
            if size > width {
                glyph = "?".into();
                size = 1;
            }
            if used + size > width {
                rows.push(std::mem::take(&mut line));
                used = 0;
            }
            if rows.len() >= 127 {
                rows.push("[display truncated]".chars().take(width).collect());
                return rows;
            }
            line.push_str(&glyph);
            used += size;
        }
        rows.push(line);
    }
    rows
}
