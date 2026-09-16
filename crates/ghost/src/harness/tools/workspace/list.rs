#![forbid(unsafe_code)]

use std::collections::BinaryHeap;
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;

use crate::harness::runtime::path::resolve_existing;
use crate::harness::runtime::{
    decode_input, Capability, Continuation, Tool, ToolContext, ToolError, ToolErrorCode,
    ToolFuture, ToolResult,
};

const CAPABILITIES: &[Capability] = &[Capability::ReadFilesystem];

pub struct LsTool {
    schema: ToolSpec,
}

impl Default for LsTool {
    fn default() -> Self {
        Self::new()
    }
}

impl LsTool {
    pub fn new() -> Self {
        Self {
            schema: ToolSpec::new(
                "ls",
                "List a bounded, deterministic set of entries in a workspace directory.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "default": "." },
                        "limit": { "type": "integer", "minimum": 1 }
                    },
                    "additionalProperties": false
                }),
            ),
        }
    }
}

impl Tool for LsTool {
    fn name(&self) -> &'static str {
        "ls"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: LsInput = decode_input(input)?;
            let requested_limit = input.limit.unwrap_or(context.policy.max_ls_entries);
            if requested_limit == 0 {
                return Err(ToolError::invalid("limit must be at least 1"));
            }
            let limit = requested_limit.min(context.policy.max_ls_entries);
            let requested_path = input.path.unwrap_or_else(|| ".".into());
            let path = resolve_existing(context, &requested_path).await?;
            if !tokio::fs::metadata(&path).await.map_err(io_error)?.is_dir() {
                return Err(ToolError::invalid("ls path is not a directory"));
            }

            let mut directory = tokio::fs::read_dir(&path).await.map_err(io_error)?;
            let mut smallest = BinaryHeap::with_capacity(limit.saturating_add(1));
            let mut total_entries = 0_u64;
            while let Some(entry) = directory.next_entry().await.map_err(io_error)? {
                if entry.file_name() == crate::harness::runtime::path::NATIVE_WRITE_LOCK_DIRECTORY {
                    continue;
                }
                total_entries += 1;
                smallest.push(Candidate {
                    name: entry.file_name().to_string_lossy().into_owned(),
                    path: entry.path(),
                });
                if smallest.len() > limit.saturating_add(1) {
                    smallest.pop();
                }
            }

            let mut candidates = smallest.into_vec();
            candidates.sort_by(|left, right| left.name.cmp(&right.name));
            let truncated = candidates.len() > limit || requested_limit > limit;
            candidates.truncate(limit);

            let mut entries = Vec::with_capacity(candidates.len());
            let mut lines = Vec::with_capacity(candidates.len());
            for candidate in candidates {
                let metadata = tokio::fs::symlink_metadata(&candidate.path)
                    .await
                    .map_err(io_error)?;
                let file_type = metadata.file_type();
                let kind = if file_type.is_symlink() {
                    "symlink"
                } else if file_type.is_dir() {
                    "directory"
                } else if file_type.is_file() {
                    "file"
                } else {
                    "other"
                };
                let suffix = match kind {
                    "directory" => "/",
                    "symlink" => "@",
                    _ => "",
                };
                lines.push(format!("{}{suffix}", candidate.name));
                entries.push(json!({
                    "name": candidate.name,
                    "type": kind,
                    "size_bytes": metadata.len(),
                    "readonly": metadata.permissions().readonly(),
                }));
            }
            let after = truncated.then(|| {
                entries
                    .last()
                    .and_then(|entry| entry.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            });
            let mut result = ToolResult::success(
                if lines.is_empty() {
                    String::new()
                } else {
                    format!("{}\n", lines.join("\n"))
                },
                json!({
                    "path": requested_path,
                    "entries": entries,
                    "total_entries": total_entries,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LsInput {
    path: Option<String>,
    limit: Option<usize>,
}

#[derive(Eq, PartialEq)]
struct Candidate {
    name: String,
    path: PathBuf,
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn io_error(error: std::io::Error) -> ToolError {
    let code = match error.kind() {
        std::io::ErrorKind::NotFound => ToolErrorCode::NotFound,
        std::io::ErrorKind::PermissionDenied => ToolErrorCode::PermissionDenied,
        _ => ToolErrorCode::Io,
    };
    ToolError::new(code, error.to_string(), false)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::harness::runtime::{NoopEventSink, NoopOutputStore, ToolIdentity, ToolPolicy};

    #[tokio::test]
    async fn output_is_sorted_and_bounded() {
        let directory = tempdir().unwrap();
        for name in ["zeta", "alpha", "middle"] {
            std::fs::write(directory.path().join(name), name).unwrap();
        }
        let root = directory.path().canonicalize().unwrap();
        let context = ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(2),
            cancellation: CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root)),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        };
        let result = LsTool::new()
            .execute(&context, json!({"limit": 2}))
            .await
            .unwrap();
        assert_eq!(result.content, "alpha\nmiddle\n");
        assert!(result.truncated);
        assert_eq!(
            result.continuation.unwrap().after.as_deref(),
            Some("middle")
        );
    }
}
