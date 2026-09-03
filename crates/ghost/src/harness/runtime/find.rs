#![forbid(unsafe_code)]

use std::time::Instant;

use globset::GlobBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tachyon_model::ToolSpec;

use super::path::resolve_existing;
use super::traversal::{relative_utf8, walk, TraversalOptions, WalkControl};
use super::{
    decode_input, Capability, Continuation, Tool, ToolContext, ToolError, ToolErrorCode,
    ToolFuture, ToolResult,
};

const CAPABILITIES: &[Capability] = &[Capability::ReadFilesystem];

pub struct FindTool {
    schema: ToolSpec,
}

impl Default for FindTool {
    fn default() -> Self {
        Self::new()
    }
}

impl FindTool {
    pub fn new() -> Self {
        Self {
            schema: ToolSpec::new(
                "find",
                "Find bounded workspace paths using case-sensitive *, **, ?, and [] globs while respecting ignore files.",
                json!({
                    "type": "object",
                    "properties": {
                        "pattern": { "type": "string" },
                        "path": { "type": "string", "default": "." },
                        "limit": { "type": "integer", "minimum": 1 },
                        "hidden": { "type": "boolean", "default": false }
                    },
                    "required": ["pattern"],
                    "additionalProperties": false
                }),
            ),
        }
    }
}

impl Tool for FindTool {
    fn name(&self) -> &'static str {
        "find"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: FindInput = decode_input(input)?;
            if input.pattern.is_empty() || input.pattern.len() > 4096 {
                return Err(ToolError::invalid(
                    "pattern must contain between 1 and 4096 bytes",
                ));
            }
            let requested_limit = input.limit.unwrap_or(context.policy.max_find_results);
            if requested_limit == 0 {
                return Err(ToolError::invalid("limit must be at least 1"));
            }
            let limit = requested_limit.min(context.policy.max_find_results);
            let requested_path = input.path.unwrap_or_else(|| ".".into());
            let root = resolve_existing(context, &requested_path).await?;
            if !tokio::fs::metadata(&root)
                .await
                .map_err(tool_io_error)?
                .is_dir()
            {
                return Err(ToolError::invalid("find path is not a directory"));
            }
            let glob = GlobBuilder::new(&input.pattern)
                .literal_separator(true)
                .backslash_escape(true)
                .build()
                .map_err(|error| ToolError::invalid(format!("invalid glob: {error}")))?
                .compile_matcher();
            let match_relative_path = input.pattern.contains('/');
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
            let task = tokio::task::spawn_blocking(move || {
                let mut matches = Vec::with_capacity(limit.min(256));
                let mut matched_more = false;
                let stats = walk(options, |entry| {
                    let Some(relative) = relative_utf8(&root_for_walk, entry) else {
                        return Ok(WalkControl::Continue);
                    };
                    let candidate = if match_relative_path {
                        relative.as_str()
                    } else {
                        entry.file_name().to_str().unwrap_or_default()
                    };
                    if !glob.is_match(candidate) {
                        return Ok(WalkControl::Continue);
                    }
                    if matches.len() == limit {
                        matched_more = true;
                        return Ok(WalkControl::Stop);
                    }
                    let kind = entry
                        .file_type()
                        .map(|kind| {
                            if kind.is_dir() {
                                "directory"
                            } else if kind.is_file() {
                                "file"
                            } else {
                                "other"
                            }
                        })
                        .unwrap_or("other");
                    matches.push(FindMatch {
                        path: relative,
                        kind,
                    });
                    Ok(WalkControl::Continue)
                })?;
                Ok::<_, ToolError>((matches, matched_more, stats))
            })
            .await
            .map_err(join_error)??;
            let (mut matches, matched_more, stats) = task;
            matches.sort_by(|left, right| left.path.cmp(&right.path));
            let truncated = matched_more || stats.entry_limit_reached;
            let after = truncated.then(|| {
                matches
                    .last()
                    .map(|entry| entry.path.clone())
                    .unwrap_or_default()
            });
            let content = if matches.is_empty() {
                String::new()
            } else {
                format!(
                    "{}\n",
                    matches
                        .iter()
                        .map(|entry| entry.path.as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            };
            let mut result = ToolResult::success(
                content,
                json!({
                    "path": requested_path,
                    "pattern": input.pattern,
                    "matches": matches,
                    "visited_entries": stats.visited,
                    "traversal_errors": stats.errors,
                    "effective_limit": limit,
                }),
            );
            result.truncated = truncated;
            result.continuation = truncated.then_some(Continuation {
                next_offset: None,
                after,
            });
            Ok(result)
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FindInput {
    pattern: String,
    path: Option<String>,
    limit: Option<usize>,
    hidden: Option<bool>,
}

#[derive(Serialize)]
struct FindMatch {
    path: String,
    #[serde(rename = "type")]
    kind: &'static str,
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
        format!("find traversal task failed: {error}"),
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
    async fn finds_sorted_paths_and_respects_gitignore_and_hidden_policy() {
        let workspace = tempdir().unwrap();
        std::fs::create_dir_all(workspace.path().join("src/nested")).unwrap();
        std::fs::write(workspace.path().join("src/z.rs"), "").unwrap();
        std::fs::write(workspace.path().join("src/a.rs"), "").unwrap();
        std::fs::write(workspace.path().join("src/nested/b.rs"), "").unwrap();
        std::fs::write(workspace.path().join("ignored.rs"), "").unwrap();
        std::fs::write(workspace.path().join(".hidden.rs"), "").unwrap();
        std::fs::write(workspace.path().join(".gitignore"), "ignored.rs\n").unwrap();
        let context = context(workspace.path());

        let result = FindTool::new()
            .execute(&context, json!({"pattern":"*.rs"}))
            .await
            .unwrap();
        assert_eq!(result.content, "src/a.rs\nsrc/nested/b.rs\nsrc/z.rs\n");
        let hidden = FindTool::new()
            .execute(&context, json!({"pattern":"*.rs", "hidden":true}))
            .await
            .unwrap();
        assert!(
            hidden.content.contains(".hidden.rs"),
            "hidden output: {:?}",
            hidden.content
        );
        assert!(!hidden.content.contains("ignored.rs"));
    }

    #[tokio::test]
    async fn slash_patterns_match_relative_paths_and_limits_are_reported() {
        let workspace = tempdir().unwrap();
        std::fs::create_dir_all(workspace.path().join("src/nested")).unwrap();
        std::fs::write(workspace.path().join("src/a.rs"), "").unwrap();
        std::fs::write(workspace.path().join("src/nested/b.rs"), "").unwrap();
        let result = FindTool::new()
            .execute(
                &context(workspace.path()),
                json!({"pattern":"src/**/*.rs", "limit":1}),
            )
            .await
            .unwrap();
        assert_eq!(result.content, "src/a.rs\n");
        assert!(result.truncated);
        assert_eq!(
            result.continuation.unwrap().after.as_deref(),
            Some("src/a.rs")
        );
    }
}
