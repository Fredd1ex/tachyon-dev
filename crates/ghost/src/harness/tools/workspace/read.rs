#![forbid(unsafe_code)]

use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;
use tokio::io::{AsyncReadExt, BufReader};

use crate::harness::runtime::path::resolve_existing;
use crate::harness::runtime::{
    decode_input, Capability, Continuation, Tool, ToolContext, ToolError, ToolErrorCode,
    ToolFuture, ToolResult,
};

const CAPABILITIES: &[Capability] = &[Capability::ReadFilesystem];

pub struct ReadTool {
    schema: ToolSpec,
}

impl Default for ReadTool {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadTool {
    pub fn new() -> Self {
        Self {
            schema: ToolSpec::new(
                "read",
                "Read a bounded line range from a UTF-8 text file in the workspace.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "offset": { "type": "integer", "minimum": 1, "default": 1 },
                        "limit": { "type": "integer", "minimum": 1 }
                    },
                    "required": ["path"],
                    "additionalProperties": false
                }),
            ),
        }
    }
}

impl Tool for ReadTool {
    fn name(&self) -> &'static str {
        "read"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: ReadInput = decode_input(input)?;
            let offset = input.offset.unwrap_or(1);
            if offset == 0 {
                return Err(ToolError::invalid("offset must be at least 1"));
            }
            let requested_limit = input.limit.unwrap_or(context.policy.max_read_lines);
            if requested_limit == 0 {
                return Err(ToolError::invalid("limit must be at least 1"));
            }
            let limit = requested_limit.min(context.policy.max_read_lines);
            let path = resolve_existing(context, &input.path).await?;
            let metadata = tokio::fs::metadata(&path).await.map_err(io_error)?;
            if !metadata.is_file() {
                return Err(ToolError::invalid("read path is not a regular file"));
            }

            let file = tokio::fs::File::open(&path).await.map_err(io_error)?;
            let mut reader = BufReader::new(file);
            let mut buffer = [0_u8; 8192];
            let mut content = Vec::with_capacity(context.policy.max_return_bytes.min(64 * 1024));
            let mut current_line = 1_u64;
            let mut returned_lines = 0_usize;
            let mut truncated = requested_limit > limit;
            let mut saw_more = false;

            'read: loop {
                let count = reader.read(&mut buffer).await.map_err(io_error)?;
                if count == 0 {
                    break;
                }
                for &byte in &buffer[..count] {
                    if byte == 0 {
                        return Err(ToolError::new(
                            ToolErrorCode::UnsupportedBinary,
                            "read supports UTF-8 text files, not binary data",
                            false,
                        ));
                    }
                    if current_line >= offset && returned_lines < limit {
                        if content.len() == context.policy.max_return_bytes {
                            truncated = true;
                            saw_more = true;
                            break 'read;
                        }
                        content.push(byte);
                    } else if current_line >= offset && returned_lines >= limit {
                        truncated = true;
                        saw_more = true;
                        break 'read;
                    }
                    if byte == b'\n' {
                        if current_line >= offset && returned_lines < limit {
                            returned_lines += 1;
                        }
                        current_line += 1;
                    }
                }
            }
            if !content.is_empty() && !content.ends_with(b"\n") {
                returned_lines += 1;
            }

            let content = match String::from_utf8(content) {
                Ok(content) => content,
                Err(error) if error.utf8_error().error_len().is_none() && truncated => {
                    let valid = error.utf8_error().valid_up_to();
                    String::from_utf8(error.into_bytes()[..valid].to_vec())
                        .expect("validated UTF-8 prefix")
                }
                Err(_) => {
                    return Err(ToolError::new(
                        ToolErrorCode::UnsupportedBinary,
                        "read supports UTF-8 text files, not binary data",
                        false,
                    ));
                }
            };
            let next_offset =
                (truncated && content.ends_with('\n')).then_some(offset + returned_lines as u64);
            let mut result = ToolResult::success(
                content,
                json!({
                    "path": input.path,
                    "offset": offset,
                    "lines": returned_lines,
                    "size_bytes": metadata.len(),
                }),
            );
            result.truncated = truncated || saw_more;
            result.continuation = result.truncated.then_some(Continuation {
                next_offset,
                after: None,
            });
            Ok(result)
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    path: String,
    offset: Option<u64>,
    limit: Option<usize>,
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
    async fn reads_requested_lines_and_returns_continuation() {
        let directory = tempdir().unwrap();
        std::fs::write(directory.path().join("sample.txt"), "one\ntwo\nthree\n").unwrap();
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
        };
        let result = ReadTool::new()
            .execute(
                &context,
                json!({"path":"sample.txt", "offset":2, "limit":1}),
            )
            .await
            .unwrap();
        assert_eq!(result.content, "two\n");
        assert!(result.truncated);
        assert_eq!(result.continuation.unwrap().next_offset, Some(3));
    }

    #[tokio::test]
    async fn rejects_unknown_input_fields() {
        let tool = ReadTool::new();
        let error = decode_input::<ReadInput>(json!({"path":"x", "extra":true})).unwrap_err();
        assert_eq!(error.code, ToolErrorCode::InvalidInput);
        assert_eq!(tool.name(), "read");
    }

    #[tokio::test]
    async fn rejects_binary_input() {
        let directory = tempdir().unwrap();
        std::fs::write(directory.path().join("binary"), [0xff, 0x00, 0x01]).unwrap();
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
        };
        let error = ReadTool::new()
            .execute(&context, json!({"path":"binary"}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::UnsupportedBinary);
    }
}
