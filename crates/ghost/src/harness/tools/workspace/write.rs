#![forbid(unsafe_code)]

use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::harness::runtime::output_store::sync_directory;
use crate::harness::runtime::path::{is_allowed, resolve_write_target, WriteTarget};
use crate::harness::runtime::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult,
};

const CAPABILITIES: &[Capability] = &[Capability::WriteFilesystem];

pub struct WriteTool {
    schema: ToolSpec,
}

impl Default for WriteTool {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteTool {
    pub fn new() -> Self {
        Self {
            schema: ToolSpec::new(
                "write",
                "Atomically create or replace one UTF-8 file in the workspace.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "content": { "type": "string" },
                        "create_parents": { "type": "boolean", "default": false }
                    },
                    "required": ["path", "content"],
                    "additionalProperties": false
                }),
            ),
        }
    }
}

impl Tool for WriteTool {
    fn name(&self) -> &'static str {
        "write"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: WriteInput = decode_input(input)?;
            if input.content.len() > context.policy.max_write_bytes {
                return Err(ToolError::invalid(format!(
                    "content exceeds the {} byte write limit",
                    context.policy.max_write_bytes
                )));
            }
            let target =
                resolve_write_target(context, &input.path, input.create_parents.unwrap_or(false))
                    .await?;
            let outcome = atomic_replace(context, &target, input.content.as_bytes(), None).await?;
            Ok(ToolResult::success(
                format!("wrote {} bytes to {}", input.content.len(), input.path),
                json!({
                    "path": input.path,
                    "bytes_written": input.content.len(),
                    "created": outcome.created,
                    "replaced": !outcome.created,
                }),
            ))
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteInput {
    path: String,
    content: String,
    create_parents: Option<bool>,
}

#[derive(Debug)]
pub(crate) struct AtomicWriteOutcome {
    pub created: bool,
}

pub(crate) async fn atomic_replace(
    context: &ToolContext,
    target: &WriteTarget,
    content: &[u8],
    expected: Option<&[u8]>,
) -> Result<AtomicWriteOutcome, ToolError> {
    let canonical_parent = tokio::fs::canonicalize(&target.parent)
        .await
        .map_err(io_error)?;
    if canonical_parent != target.parent || !is_allowed(context, &canonical_parent) {
        return Err(changed_target("target parent changed before write"));
    }
    revalidate_target(context, target).await?;

    let temporary = target
        .parent
        .join(format!(".tachyon-write-{}.tmp", Uuid::new_v4()));
    let mut guard = TemporaryGuard::new(temporary.clone());
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .await
        .map_err(io_error)?;

    if target.existed {
        let permissions = tokio::fs::metadata(&target.path)
            .await
            .map_err(io_error)?
            .permissions();
        file.set_permissions(permissions).await.map_err(io_error)?;
    }
    file.write_all(content).await.map_err(io_error)?;
    file.flush().await.map_err(io_error)?;
    if context.policy.sync_writes {
        file.sync_data().await.map_err(io_error)?;
    }
    drop(file);

    revalidate_target(context, target).await?;
    if let Some(expected) = expected {
        let current = read_bounded_file(&target.path, context.policy.max_write_bytes).await?;
        if current != expected {
            return Err(changed_target("file changed while edit was in progress"));
        }
    }
    tokio::fs::rename(&temporary, &target.path)
        .await
        .map_err(io_error)?;
    guard.committed = true;
    if context.policy.sync_writes {
        sync_directory(target.parent.clone())
            .await
            .map_err(io_error)?;
    }
    Ok(AtomicWriteOutcome {
        created: !target.existed,
    })
}

async fn revalidate_target(context: &ToolContext, target: &WriteTarget) -> Result<(), ToolError> {
    match tokio::fs::symlink_metadata(&target.path).await {
        Ok(_metadata) if !target.existed => Err(changed_target(
            "new write target appeared before replacement",
        )),
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(changed_target(
                    "write target type changed before replacement",
                ));
            }
            let canonical = tokio::fs::canonicalize(&target.path)
                .await
                .map_err(io_error)?;
            if canonical != target.path || !is_allowed(context, &canonical) {
                return Err(changed_target("write target changed before replacement"));
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && !target.existed => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Err(changed_target(
            "write target disappeared before replacement",
        )),
        Err(error) => Err(io_error(error)),
    }
}

pub(crate) async fn read_bounded_file(path: &Path, max_bytes: usize) -> Result<Vec<u8>, ToolError> {
    let metadata = tokio::fs::metadata(path).await.map_err(io_error)?;
    if !metadata.is_file() {
        return Err(ToolError::invalid("edit target is not a regular file"));
    }
    if metadata.len() > max_bytes as u64 {
        return Err(ToolError::invalid(format!(
            "file exceeds the {max_bytes} byte edit limit"
        )));
    }
    let file = tokio::fs::File::open(path).await.map_err(io_error)?;
    let mut content = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes as u64 + 1)
        .read_to_end(&mut content)
        .await
        .map_err(io_error)?;
    if content.len() > max_bytes {
        return Err(ToolError::invalid(format!(
            "file exceeds the {max_bytes} byte edit limit"
        )));
    }
    Ok(content)
}

fn changed_target(message: &str) -> ToolError {
    ToolError::new(ToolErrorCode::Io, message, true)
}

fn io_error(error: io::Error) -> ToolError {
    let code = match error.kind() {
        io::ErrorKind::NotFound => ToolErrorCode::NotFound,
        io::ErrorKind::PermissionDenied => ToolErrorCode::PermissionDenied,
        _ => ToolErrorCode::Io,
    };
    ToolError::new(code, error.to_string(), false)
}

struct TemporaryGuard {
    path: PathBuf,
    committed: bool,
}

impl TemporaryGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }
}

impl Drop for TemporaryGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
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
    async fn creates_parents_only_when_requested_and_replaces_atomically() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let tool = WriteTool::new();
        let missing_parent = tool
            .execute(&context, json!({"path":"nested/file", "content":"one"}))
            .await
            .unwrap_err();
        assert_eq!(missing_parent.code, ToolErrorCode::NotFound);
        assert!(!workspace.path().join("nested").exists());

        let created = tool
            .execute(
                &context,
                json!({"path":"nested/file", "content":"one", "create_parents":true}),
            )
            .await
            .unwrap();
        assert_eq!(created.metadata["created"], true);
        let replaced = tool
            .execute(&context, json!({"path":"nested/file", "content":"two"}))
            .await
            .unwrap();
        assert_eq!(replaced.metadata["replaced"], true);
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("nested/file")).unwrap(),
            "two"
        );
    }

    #[tokio::test]
    async fn oversized_content_preserves_the_original() {
        let workspace = tempdir().unwrap();
        std::fs::write(workspace.path().join("file"), "original").unwrap();
        let mut context = context(workspace.path());
        Arc::make_mut(&mut context.policy).max_write_bytes = 4;
        let error = WriteTool::new()
            .execute(&context, json!({"path":"file", "content":"too long"}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::InvalidInput);
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("file")).unwrap(),
            "original"
        );
    }

    #[tokio::test]
    async fn changed_expected_content_aborts_and_removes_the_temporary_file() {
        let workspace = tempdir().unwrap();
        let path = workspace.path().join("file");
        std::fs::write(&path, "changed").unwrap();
        let context = context(workspace.path());
        let target = resolve_write_target(&context, "file", false).await.unwrap();
        let error = atomic_replace(&context, &target, b"replacement", Some(b"original"))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Io);
        assert!(error.retryable);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "changed");
        assert!(std::fs::read_dir(workspace.path())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tachyon-write-")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replacement_preserves_permissions_and_rejects_symlink_targets() {
        use std::os::unix::fs::PermissionsExt;

        let workspace = tempdir().unwrap();
        let path = workspace.path().join("file");
        std::fs::write(&path, "old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let context = context(workspace.path());
        WriteTool::new()
            .execute(&context, json!({"path":"file", "content":"new"}))
            .await
            .unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );

        let target = workspace.path().join("target");
        std::fs::write(&target, "protected").unwrap();
        std::os::unix::fs::symlink(&target, workspace.path().join("link")).unwrap();
        let error = WriteTool::new()
            .execute(&context, json!({"path":"link", "content":"overwrite"}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::PermissionDenied);
        assert_eq!(std::fs::read_to_string(target).unwrap(), "protected");
    }
}
