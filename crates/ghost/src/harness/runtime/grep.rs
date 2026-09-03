#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tachyon_model::ToolSpec;
use tokio::io::AsyncReadExt;

use super::binary::discover;
use super::path::resolve_existing;
use super::traversal::{relative_utf8, walk, TraversalOptions, WalkControl};
use super::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult,
    SANITIZED_PATH,
};

const CAPABILITIES: &[Capability] = &[Capability::ReadFilesystem];

pub struct GrepTool {
    schema: ToolSpec,
    rg: Option<PathBuf>,
}

impl Default for GrepTool {
    fn default() -> Self {
        Self::new()
    }
}

impl GrepTool {
    pub fn new() -> Self {
        Self::with_rg(discover("rg", SANITIZED_PATH))
    }

    fn with_rg(rg: Option<PathBuf>) -> Self {
        Self {
            schema: ToolSpec::new(
                "grep",
                "Search UTF-8 workspace files with bounded context using Rust regex syntax or fixed strings; respects ignore files.",
                json!({
                    "type": "object",
                    "properties": {
                        "pattern": { "type": "string" },
                        "path": { "type": "string", "default": "." },
                        "context": { "type": "integer", "minimum": 0, "default": 0 },
                        "limit": { "type": "integer", "minimum": 1 },
                        "fixed_string": { "type": "boolean", "default": false },
                        "hidden": { "type": "boolean", "default": false }
                    },
                    "required": ["pattern"],
                    "additionalProperties": false
                }),
            ),
            rg,
        }
    }
}

impl Tool for GrepTool {
    fn name(&self) -> &'static str {
        "grep"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: GrepInput = decode_input(input)?;
            if input.pattern.is_empty() || input.pattern.len() > 16 * 1024 {
                return Err(ToolError::invalid(
                    "pattern must contain between 1 and 16384 bytes",
                ));
            }
            let requested_limit = input.limit.unwrap_or(context.policy.max_grep_matches);
            if requested_limit == 0 {
                return Err(ToolError::invalid("limit must be at least 1"));
            }
            let limit = requested_limit.min(context.policy.max_grep_matches);
            let requested_context = input.context.unwrap_or(0);
            let context_lines = requested_context.min(context.policy.max_grep_context_lines);
            let context_limited = requested_context > context_lines;
            let matcher = if input.fixed_string.unwrap_or(false) {
                Matcher::Fixed(input.pattern.clone())
            } else {
                Matcher::Regex(
                    RegexBuilder::new(&input.pattern)
                        .size_limit(10 * 1024 * 1024)
                        .dfa_size_limit(2 * 1024 * 1024)
                        .build()
                        .map_err(|error| ToolError::invalid(format!("invalid regex: {error}")))?,
                )
            };
            let requested_path = input.path.unwrap_or_else(|| ".".into());
            let root = resolve_existing(context, &requested_path).await?;
            if !tokio::fs::metadata(&root)
                .await
                .map_err(tool_io_error)?
                .is_dir()
            {
                return Err(ToolError::invalid("grep path is not a directory"));
            }

            let candidate_filter = match &self.rg {
                Some(rg) => {
                    rg_candidates(
                        rg,
                        &root,
                        &input.pattern,
                        input.fixed_string.unwrap_or(false),
                        input.hidden.unwrap_or(false),
                        context.policy.max_search_file_bytes,
                        context.policy.max_return_bytes,
                        context.policy.max_traversal_entries,
                        &context.policy.exec_path,
                    )
                    .await
                }
                None => None,
            };
            let backend = if candidate_filter.is_some() {
                "rg"
            } else {
                "native"
            };

            let root_for_walk = root.clone();
            let deadline = context.deadline.min(
                Instant::now()
                    .checked_add(context.policy.max_duration)
                    .unwrap_or(context.deadline),
            );
            let options = TraversalOptions {
                root: root.clone(),
                hidden: input.hidden.unwrap_or(false),
                max_entries: context.policy.max_traversal_entries,
                deadline,
                cancellation: context.cancellation.clone(),
            };
            let max_file_bytes = context.policy.max_search_file_bytes;
            let max_line_bytes = context.policy.max_search_line_bytes;
            let output_budget = context.policy.max_return_bytes.saturating_sub(4096) / 2;
            let task = tokio::task::spawn_blocking(move || {
                let mut matches = Vec::with_capacity(limit.min(128));
                let mut output_bytes = 0_usize;
                let mut stopped_for_results = false;
                let mut binary_files = 0_usize;
                let mut oversized_files = 0_usize;
                let mut oversized_lines = 0_usize;
                let stats = walk(options, |entry| {
                    if !entry.file_type().is_some_and(|kind| kind.is_file()) {
                        return Ok(WalkControl::Continue);
                    }
                    let Some(relative) = relative_utf8(&root_for_walk, entry) else {
                        return Ok(WalkControl::Continue);
                    };
                    if candidate_filter
                        .as_ref()
                        .is_some_and(|candidates| !candidates.contains(&relative))
                    {
                        return Ok(WalkControl::Continue);
                    }
                    let metadata = match entry.metadata() {
                        Ok(metadata) => metadata,
                        Err(_) => return Ok(WalkControl::Continue),
                    };
                    if metadata.len() > max_file_bytes as u64 {
                        oversized_files += 1;
                        return Ok(WalkControl::Continue);
                    }
                    let mut bytes = Vec::with_capacity(metadata.len() as usize);
                    if File::open(entry.path())
                        .and_then(|file| {
                            file.take(max_file_bytes as u64 + 1).read_to_end(&mut bytes)
                        })
                        .is_err()
                    {
                        return Ok(WalkControl::Continue);
                    }
                    if bytes.len() > max_file_bytes {
                        oversized_files += 1;
                        return Ok(WalkControl::Continue);
                    }
                    if bytes.contains(&0) {
                        binary_files += 1;
                        return Ok(WalkControl::Continue);
                    }
                    let text = match std::str::from_utf8(&bytes) {
                        Ok(text) => text,
                        Err(_) => {
                            binary_files += 1;
                            return Ok(WalkControl::Continue);
                        }
                    };
                    let lines = text.split('\n').collect::<Vec<_>>();
                    for (index, raw_line) in lines.iter().enumerate() {
                        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
                        if line.len() > max_line_bytes {
                            oversized_lines += 1;
                            continue;
                        }
                        if !matcher.is_match(line) {
                            continue;
                        }
                        if matches.len() == limit {
                            stopped_for_results = true;
                            return Ok(WalkControl::Stop);
                        }
                        let before = lines[index.saturating_sub(context_lines)..index]
                            .iter()
                            .map(|line| bounded_line(line, max_line_bytes))
                            .collect::<Vec<_>>();
                        let after_end = lines.len().min(index + context_lines + 1);
                        let after = lines[index + 1..after_end]
                            .iter()
                            .map(|line| bounded_line(line, max_line_bytes))
                            .collect::<Vec<_>>();
                        let estimated = relative.len()
                            + line.len()
                            + before.iter().map(String::len).sum::<usize>()
                            + after.iter().map(String::len).sum::<usize>()
                            + 128;
                        if output_bytes.saturating_add(estimated) > output_budget {
                            stopped_for_results = true;
                            return Ok(WalkControl::Stop);
                        }
                        output_bytes += estimated;
                        matches.push(GrepMatch {
                            path: relative.clone(),
                            line: index + 1,
                            text: line.to_string(),
                            before,
                            after,
                        });
                    }
                    Ok(WalkControl::Continue)
                })?;
                Ok::<_, ToolError>((
                    matches,
                    stopped_for_results,
                    binary_files,
                    oversized_files,
                    oversized_lines,
                    stats,
                ))
            })
            .await
            .map_err(join_error)??;
            let (
                mut matches,
                stopped_for_results,
                binary_files,
                oversized_files,
                oversized_lines,
                stats,
            ) = task;
            matches.sort_by(|left, right| {
                left.path
                    .cmp(&right.path)
                    .then_with(|| left.line.cmp(&right.line))
            });
            let truncated = stopped_for_results || stats.entry_limit_reached || context_limited;
            let content = if matches.is_empty() {
                String::new()
            } else {
                format!(
                    "{}\n",
                    matches
                        .iter()
                        .map(|entry| format!("{}:{}:{}", entry.path, entry.line, entry.text))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            };
            let mut result = ToolResult::success(
                content,
                json!({
                    "path": requested_path,
                    "pattern": input.pattern,
                    "fixed_string": input.fixed_string.unwrap_or(false),
                    "matches": matches,
                    "visited_entries": stats.visited,
                    "traversal_errors": stats.errors,
                    "binary_files_skipped": binary_files,
                    "oversized_files_skipped": oversized_files,
                    "oversized_lines_skipped": oversized_lines,
                    "effective_limit": limit,
                    "effective_context": context_lines,
                    "backend": backend,
                }),
            );
            result.truncated = truncated;
            Ok(result)
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn rg_candidates(
    rg: &Path,
    root: &Path,
    pattern: &str,
    fixed_string: bool,
    hidden: bool,
    max_file_bytes: usize,
    max_output_bytes: usize,
    max_candidates: usize,
    search_path: &str,
) -> Option<HashSet<String>> {
    let mut command = tokio::process::Command::new(rg);
    command
        .arg("--files-with-matches")
        .arg("--null")
        .arg("--no-messages")
        .arg("--max-filesize")
        .arg(max_file_bytes.to_string())
        .current_dir(root)
        .env_clear()
        .env("PATH", search_path)
        .env("HOME", root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if fixed_string {
        command.arg("--fixed-strings");
    }
    if hidden {
        command.arg("--hidden");
    }
    command.arg("--").arg(pattern).arg(".");

    let mut child = command.spawn().ok()?;
    let stdout = child.stdout.take()?;
    let stderr = child.stderr.take()?;
    let stdout_task = tokio::spawn(read_capped(stdout, max_output_bytes));
    let stderr_task = tokio::spawn(read_capped(stderr, 16 * 1024));
    let status = child.wait().await.ok()?;
    let (stdout, stdout_truncated) = stdout_task.await.ok()?.ok()?;
    let _ = stderr_task.await;
    if stdout_truncated || !matches!(status.code(), Some(0) | Some(1)) {
        return None;
    }

    let mut candidates = HashSet::new();
    for path in stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        let path = std::str::from_utf8(path).ok()?;
        let path = path.strip_prefix("./").unwrap_or(path);
        let parsed = Path::new(path);
        if parsed.is_absolute()
            || parsed
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return None;
        }
        if candidates.len() == max_candidates {
            return None;
        }
        candidates.insert(path.replace(std::path::MAIN_SEPARATOR, "/"));
    }
    Some(candidates)
}

async fn read_capped(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    cap: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut output = Vec::with_capacity(cap.min(64 * 1024));
    let mut buffer = [0_u8; 8192];
    let mut truncated = false;
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        let remaining = cap.saturating_sub(output.len());
        output.extend_from_slice(&buffer[..count.min(remaining)]);
        truncated |= count > remaining;
    }
    Ok((output, truncated))
}

enum Matcher {
    Fixed(String),
    Regex(Regex),
}

impl Matcher {
    fn is_match(&self, line: &str) -> bool {
        match self {
            Self::Fixed(pattern) => line.contains(pattern),
            Self::Regex(pattern) => pattern.is_match(line),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrepInput {
    pattern: String,
    path: Option<String>,
    context: Option<usize>,
    limit: Option<usize>,
    fixed_string: Option<bool>,
    hidden: Option<bool>,
}

#[derive(Serialize)]
struct GrepMatch {
    path: String,
    line: usize,
    text: String,
    before: Vec<String>,
    after: Vec<String>,
}

fn bounded_line(line: &str, max_bytes: usize) -> String {
    let line = line.strip_suffix('\r').unwrap_or(line);
    if line.len() <= max_bytes {
        return line.to_string();
    }
    let mut end = max_bytes.min(line.len());
    while !line.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}[truncated]", &line[..end])
}

fn tool_io_error(error: std::io::Error) -> ToolError {
    let code = match error.kind() {
        std::io::ErrorKind::NotFound => ToolErrorCode::NotFound,
        std::io::ErrorKind::PermissionDenied => ToolErrorCode::PermissionDenied,
        _ => ToolErrorCode::Io,
    };
    ToolError::new(code, error.to_string(), false)
}

fn join_error(error: tokio::task::JoinError) -> ToolError {
    ToolError::new(
        ToolErrorCode::Internal,
        format!("grep traversal task failed: {error}"),
        true,
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::harness::runtime::{NoopEventSink, NoopOutputStore, ToolIdentity, ToolPolicy};

    fn context(root: &Path) -> ToolContext {
        let root = root.canonicalize().unwrap();
        ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(2),
            cancellation: CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root)),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
        }
    }

    #[tokio::test]
    async fn regex_search_returns_sorted_structured_context() {
        let workspace = tempdir().unwrap();
        std::fs::write(workspace.path().join("b.txt"), "before\nneedle 2\nafter\n").unwrap();
        std::fs::write(workspace.path().join("a.txt"), "before\nneedle 1\nafter\n").unwrap();
        let result = GrepTool::new()
            .execute(
                &context(workspace.path()),
                json!({"pattern":"needle [0-9]", "context":1}),
            )
            .await
            .unwrap();
        assert_eq!(result.content, "a.txt:2:needle 1\nb.txt:2:needle 2\n");
        assert_eq!(result.metadata["matches"][0]["before"][0], "before");
        assert_eq!(result.metadata["matches"][0]["after"][0], "after");
    }

    #[tokio::test]
    async fn fixed_search_respects_ignore_binary_and_match_limits() {
        let workspace = tempdir().unwrap();
        std::fs::write(workspace.path().join("a.txt"), "a.b\n").unwrap();
        std::fs::write(workspace.path().join("b.txt"), "a.b\n").unwrap();
        std::fs::write(workspace.path().join("ignored.txt"), "a.b\n").unwrap();
        std::fs::write(workspace.path().join("binary"), b"a.b\0").unwrap();
        std::fs::write(workspace.path().join(".gitignore"), "ignored.txt\n").unwrap();
        let result = GrepTool::new()
            .execute(
                &context(workspace.path()),
                json!({"pattern":"a.b", "fixed_string":true, "limit":1}),
            )
            .await
            .unwrap();
        assert_eq!(result.content, "a.txt:1:a.b\n");
        assert!(result.truncated);
        assert!(!result.content.contains("ignored"));
    }

    #[tokio::test]
    async fn invalid_regex_and_pre_cancelled_search_fail_predictably() {
        let workspace = tempdir().unwrap();
        let invalid = GrepTool::new()
            .execute(&context(workspace.path()), json!({"pattern":"["}))
            .await
            .unwrap_err();
        assert_eq!(invalid.code, ToolErrorCode::InvalidInput);

        let context = context(workspace.path());
        context.cancellation.cancel();
        let cancelled = GrepTool::new()
            .execute(&context, json!({"pattern":"text"}))
            .await
            .unwrap_err();
        assert_eq!(cancelled.code, ToolErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn ripgrep_acceleration_matches_native_results_and_spawn_failure_falls_back() {
        let workspace = tempdir().unwrap();
        std::fs::write(workspace.path().join("a.txt"), "before\nneedle 1\nafter\n").unwrap();
        std::fs::write(workspace.path().join("b.txt"), "needle 2\n").unwrap();
        let context = context(workspace.path());
        let input = json!({"pattern":"needle [0-9]", "context":1});
        let native = GrepTool::with_rg(None)
            .execute(&context, input.clone())
            .await
            .unwrap();

        let accelerated_tool = GrepTool::new();
        if accelerated_tool.rg.is_some() {
            let accelerated = accelerated_tool
                .execute(&context, input.clone())
                .await
                .unwrap();
            assert_eq!(accelerated.content, native.content);
            assert_eq!(accelerated.metadata["matches"], native.metadata["matches"]);
            assert_eq!(accelerated.metadata["backend"], "rg");
        }

        let fallback = GrepTool::with_rg(Some(workspace.path().join("missing-rg")))
            .execute(&context, input)
            .await
            .unwrap();
        assert_eq!(fallback.content, native.content);
        assert_eq!(fallback.metadata["backend"], "native");
    }
}
