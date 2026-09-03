#![forbid(unsafe_code)]

use std::fmt::Write as _;

use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tachyon_api::types::ArtifactRegistration;
use tachyon_model::ToolSpec;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use super::path::resolve_existing;
use super::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult,
};

const CAPABILITIES: &[Capability] = &[Capability::RegisterArtifact];

pub struct ArtifactTool {
    schema: ToolSpec,
}

impl Default for ArtifactTool {
    fn default() -> Self {
        Self::new()
    }
}

impl ArtifactTool {
    pub fn new() -> Self {
        Self {
            schema: ToolSpec::new(
                "artifact",
                "Register an existing workspace file as a typed, SHA-256-hashed artifact without copying its bytes.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "kind": {
                            "type": "string",
                            "enum": ["dataset", "report", "model", "log", "archive", "image", "document", "file", "other"]
                        },
                        "description": { "type": "string" }
                    },
                    "required": ["path", "kind", "description"],
                    "additionalProperties": false
                }),
            ),
        }
    }
}

impl Tool for ArtifactTool {
    fn name(&self) -> &'static str {
        "artifact"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: ArtifactInput = decode_input(input)?;
            if input.description.is_empty() || input.description.len() > 4096 {
                return Err(ToolError::invalid(
                    "description must contain between 1 and 4096 bytes",
                ));
            }
            let path = resolve_existing(context, &input.path).await?;
            let before = tokio::fs::metadata(&path).await.map_err(io_error)?;
            if before.is_dir() {
                return Err(ToolError::invalid(
                    "directory artifacts require a bounded manifest and are not supported yet",
                ));
            }
            if !before.is_file() {
                return Err(ToolError::invalid("artifact path is not a regular file"));
            }
            if before.len() > context.policy.max_artifact_bytes {
                return Err(ToolError::invalid(format!(
                    "artifact exceeds the {} byte hashing limit",
                    context.policy.max_artifact_bytes
                )));
            }

            let mut file = tokio::fs::File::open(&path).await.map_err(io_error)?;
            let mut hasher = Sha256::new();
            let mut total = 0_u64;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let count = file.read(&mut buffer).await.map_err(io_error)?;
                if count == 0 {
                    break;
                }
                total = total.saturating_add(count as u64);
                if total > context.policy.max_artifact_bytes {
                    return Err(ToolError::invalid(format!(
                        "artifact exceeds the {} byte hashing limit",
                        context.policy.max_artifact_bytes
                    )));
                }
                hasher.update(&buffer[..count]);
            }

            let file_after = file.metadata().await.map_err(io_error)?;
            let canonical_after = tokio::fs::canonicalize(&path).await.map_err(io_error)?;
            let path_after = tokio::fs::metadata(&path).await.map_err(io_error)?;
            if canonical_after != path
                || before.len() != total
                || file_after.len() != total
                || path_after.len() != total
                || modified(&before) != modified(&path_after)
            {
                return Err(ToolError::new(
                    ToolErrorCode::Io,
                    "artifact changed while it was being hashed",
                    true,
                ));
            }

            let digest = hasher.finalize();
            let mut sha256 = String::with_capacity(64);
            for byte in digest {
                write!(&mut sha256, "{byte:02x}").expect("writing to String cannot fail");
            }
            let registration = ArtifactRegistration {
                id: Uuid::new_v4().to_string(),
                path: input.path.clone(),
                kind: input.kind.as_str().into(),
                description: input.description,
                size_bytes: total,
                sha256: sha256.clone(),
                task_id: context.identity.task_id.clone(),
                work_id: context.identity.work_id.clone(),
                generation: context.identity.generation,
                assignment: context.identity.assignment,
                attempt_id: context.identity.attempt_id.clone(),
            };
            context
                .event_sink
                .register_artifact(registration.clone())
                .map_err(|error| {
                    ToolError::new(
                        ToolErrorCode::DependencyUnavailable,
                        format!("artifact registration sink failed: {error}"),
                        true,
                    )
                })?;

            Ok(ToolResult::success(
                format!("registered artifact {}", input.path),
                json!({
                    "artifact": registration,
                    "bytes_copied": 0,
                }),
            ))
        })
    }
}

fn modified(metadata: &std::fs::Metadata) -> Option<std::time::SystemTime> {
    metadata.modified().ok()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactInput {
    path: String,
    kind: ArtifactKind,
    description: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ArtifactKind {
    Dataset,
    Report,
    Model,
    Log,
    Archive,
    Image,
    Document,
    File,
    Other,
}

impl ArtifactKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Dataset => "dataset",
            Self::Report => "report",
            Self::Model => "model",
            Self::Log => "log",
            Self::Archive => "archive",
            Self::Image => "image",
            Self::Document => "document",
            Self::File => "file",
            Self::Other => "other",
        }
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
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::harness::runtime::{
        NoopOutputStore, ToolEventSink, ToolIdentity, ToolPolicy, ToolTelemetry,
    };

    #[derive(Default)]
    struct RecordingSink {
        artifacts: Mutex<Vec<ArtifactRegistration>>,
    }

    impl ToolEventSink for RecordingSink {
        fn emit(&self, _event: ToolTelemetry) {}

        fn register_artifact(&self, artifact: ArtifactRegistration) -> Result<(), String> {
            self.artifacts.lock().unwrap().push(artifact);
            Ok(())
        }
    }

    fn context(root: &Path, sink: Arc<RecordingSink>) -> ToolContext {
        let root = root.canonicalize().unwrap();
        ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity {
                call_id: None,
                task_id: Some("task-1".into()),
                work_id: Some("work-1".into()),
                generation: Some(2),
                assignment: Some(3),
                attempt_id: Some("attempt-1".into()),
            },
            deadline: Instant::now() + Duration::from_secs(2),
            cancellation: CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root)),
            event_sink: sink,
            output_store: Arc::new(NoopOutputStore),
        }
    }

    #[tokio::test]
    async fn hashes_and_registers_a_file_with_provenance() {
        let workspace = tempdir().unwrap();
        std::fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let sink = Arc::new(RecordingSink::default());
        let result = ArtifactTool::new()
            .execute(
                &context(workspace.path(), Arc::clone(&sink)),
                json!({
                    "path":"report.txt",
                    "kind":"report",
                    "description":"Test report"
                }),
            )
            .await
            .unwrap();
        assert_eq!(result.metadata["artifact"]["size_bytes"], 3);
        assert_eq!(
            result.metadata["artifact"]["sha256"],
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let artifacts = sink.artifacts.lock().unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].work_id.as_deref(), Some("work-1"));
        assert_eq!(artifacts[0].generation, Some(2));
        assert_eq!(artifacts[0].attempt_id.as_deref(), Some("attempt-1"));
    }

    #[tokio::test]
    async fn rejects_directories_and_oversized_files_without_registering() {
        let workspace = tempdir().unwrap();
        std::fs::create_dir(workspace.path().join("directory")).unwrap();
        std::fs::write(workspace.path().join("large"), "12345").unwrap();
        let sink = Arc::new(RecordingSink::default());
        let directory = ArtifactTool::new()
            .execute(
                &context(workspace.path(), Arc::clone(&sink)),
                json!({"path":"directory", "kind":"other", "description":"dir"}),
            )
            .await
            .unwrap_err();
        assert_eq!(directory.code, ToolErrorCode::InvalidInput);

        let mut context = context(workspace.path(), Arc::clone(&sink));
        Arc::make_mut(&mut context.policy).max_artifact_bytes = 4;
        let oversized = ArtifactTool::new()
            .execute(
                &context,
                json!({"path":"large", "kind":"file", "description":"large"}),
            )
            .await
            .unwrap_err();
        assert_eq!(oversized.code, ToolErrorCode::InvalidInput);
        assert!(sink.artifacts.lock().unwrap().is_empty());
    }
}
