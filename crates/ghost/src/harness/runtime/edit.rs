#![forbid(unsafe_code)]

use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;

use super::path::resolve_write_target;
use super::write::{atomic_replace, read_bounded_file};
use super::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult,
};

const CAPABILITIES: &[Capability] = &[Capability::WriteFilesystem];

pub struct EditTool {
    schema: ToolSpec,
}

impl Default for EditTool {
    fn default() -> Self {
        Self::new()
    }
}

impl EditTool {
    pub fn new() -> Self {
        Self {
            schema: ToolSpec::new(
                "edit",
                "Atomically replace one exact, unique string in a UTF-8 workspace file.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "old": { "type": "string", "minLength": 1 },
                        "new": { "type": "string" }
                    },
                    "required": ["path", "old", "new"],
                    "additionalProperties": false
                }),
            ),
        }
    }
}

impl Tool for EditTool {
    fn name(&self) -> &'static str {
        "edit"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: EditInput = decode_input(input)?;
            if input.old.is_empty() {
                return Err(ToolError::invalid("old must not be empty"));
            }
            if input.old.len() > context.policy.max_write_bytes
                || input.new.len() > context.policy.max_write_bytes
            {
                return Err(ToolError::invalid(format!(
                    "edit operands exceed the {} byte write limit",
                    context.policy.max_write_bytes
                )));
            }
            let target = resolve_write_target(context, &input.path, false).await?;
            let original = read_bounded_file(&target.path, context.policy.max_write_bytes).await?;
            if original.contains(&0) {
                return Err(ToolError::new(
                    ToolErrorCode::UnsupportedBinary,
                    "edit supports UTF-8 text files, not binary data",
                    false,
                ));
            }
            let text = std::str::from_utf8(&original).map_err(|_| {
                ToolError::new(
                    ToolErrorCode::UnsupportedBinary,
                    "edit supports UTF-8 text files, not binary data",
                    false,
                )
            })?;
            let mut matches = text.match_indices(&input.old);
            let Some((offset, _)) = matches.next() else {
                return Err(ToolError::new(
                    ToolErrorCode::NotFound,
                    "old text was not found",
                    false,
                ));
            };
            if matches.next().is_some() {
                return Err(ToolError::new(
                    ToolErrorCode::AmbiguousEdit,
                    "old text occurs more than once",
                    false,
                ));
            }

            let new_size = original
                .len()
                .checked_sub(input.old.len())
                .and_then(|size| size.checked_add(input.new.len()))
                .ok_or_else(|| ToolError::invalid("edited file size overflow"))?;
            if new_size > context.policy.max_write_bytes {
                return Err(ToolError::invalid(format!(
                    "edited file exceeds the {} byte write limit",
                    context.policy.max_write_bytes
                )));
            }
            let mut replacement = Vec::with_capacity(new_size);
            replacement.extend_from_slice(&original[..offset]);
            replacement.extend_from_slice(input.new.as_bytes());
            replacement.extend_from_slice(&original[offset + input.old.len()..]);
            atomic_replace(context, &target, &replacement, Some(&original)).await?;

            Ok(ToolResult::success(
                format!("edited one occurrence in {}", input.path),
                json!({
                    "path": input.path,
                    "occurrences": 1,
                    "bytes_removed": input.old.len(),
                    "bytes_inserted": input.new.len(),
                    "changed_bytes": input.old.len() + input.new.len(),
                    "size_delta": input.new.len() as i64 - input.old.len() as i64,
                }),
            ))
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EditInput {
    path: String,
    old: String,
    new: String,
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
    async fn replaces_one_exact_match() {
        let workspace = tempdir().unwrap();
        std::fs::write(workspace.path().join("file"), "before old after").unwrap();
        let result = EditTool::new()
            .execute(
                &context(workspace.path()),
                json!({"path":"file", "old":"old", "new":"new value"}),
            )
            .await
            .unwrap();
        assert_eq!(result.metadata["occurrences"], 1);
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("file")).unwrap(),
            "before new value after"
        );
    }

    #[tokio::test]
    async fn no_match_and_ambiguous_match_preserve_the_original() {
        let workspace = tempdir().unwrap();
        let path = workspace.path().join("file");
        std::fs::write(&path, "same and same").unwrap();
        let context = context(workspace.path());
        let tool = EditTool::new();

        let missing = tool
            .execute(&context, json!({"path":"file", "old":"missing", "new":"x"}))
            .await
            .unwrap_err();
        assert_eq!(missing.code, ToolErrorCode::NotFound);
        let ambiguous = tool
            .execute(&context, json!({"path":"file", "old":"same", "new":"x"}))
            .await
            .unwrap_err();
        assert_eq!(ambiguous.code, ToolErrorCode::AmbiguousEdit);
        assert_eq!(std::fs::read_to_string(path).unwrap(), "same and same");
    }

    #[tokio::test]
    async fn rejects_binary_files_without_modifying_them() {
        let workspace = tempdir().unwrap();
        let path = workspace.path().join("file");
        std::fs::write(&path, [0xff, 0x00, 0x01]).unwrap();
        let error = EditTool::new()
            .execute(
                &context(workspace.path()),
                json!({"path":"file", "old":"x", "new":"y"}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::UnsupportedBinary);
        assert_eq!(std::fs::read(path).unwrap(), [0xff, 0x00, 0x01]);
    }
}
