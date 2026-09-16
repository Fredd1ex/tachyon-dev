#![forbid(unsafe_code)]

use std::io;
use std::path::{Path, PathBuf};

use super::version::{check, conflict, version};
use super::write_lock::{anchored, check_active, lock_target, open_parent, same_directory};
use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::harness::runtime::path::{is_allowed, WriteTarget};
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
                        "expected_version": { "type": "string", "description": "Optional read metadata.version; reject a stale snapshot. Pass when overwriting inspected content." },
                        "expected_sha256": { "type": "string", "description": "Optional SHA-256 of the entire original file, not a read slice." },
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
            let (_writer, target) =
                lock_target(context, &input.path, input.create_parents.unwrap_or(false)).await?;
            let (snapshot, original) = if input.expected_version.is_some()
                || input.expected_sha256.is_some()
            {
                if !target.existed {
                    return Err(conflict());
                }
                let snapshot = version(&tokio::fs::metadata(&target.path).await.map_err(io_error)?);
                let original = if input.expected_sha256.is_some() {
                    Some(read_bounded_file(&target.path, context.policy.max_write_bytes).await?)
                } else {
                    None
                };
                check(
                    input.expected_version.as_deref(),
                    input.expected_sha256.as_deref(),
                    &snapshot,
                    original.as_deref(),
                )?;
                (Some(snapshot), original)
            } else {
                (None, None)
            };
            let outcome = atomic_replace(
                context,
                &target,
                input.content.as_bytes(),
                original.as_deref(),
                snapshot.as_deref(),
            )
            .await?;
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
    expected_version: Option<String>,
    expected_sha256: Option<String>,
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
    expected_version: Option<&str>,
) -> Result<AtomicWriteOutcome, ToolError> {
    let canonical_parent = tokio::fs::canonicalize(&target.parent)
        .await
        .map_err(io_error)?;
    if canonical_parent != target.parent || !is_allowed(context, &canonical_parent) {
        return Err(changed_target("target parent changed before write"));
    }
    revalidate_target(context, target).await?;
    let parent = match &target.locked_parent {
        Some(parent) => parent.try_clone().map_err(io_error)?,
        None => open_parent(&target.parent).map_err(io_error)?,
    };
    if !same_directory(&parent, &target.parent)? {
        return Err(conflict());
    }
    let directory = anchored(&parent);
    let destination = directory.join(target.path.file_name().expect("validated target"));
    let temporary = directory.join(format!(".tachyon-write-{}.tmp", Uuid::new_v4()));
    // Creation and guard installation must not be separated by an await: a
    // cancelled Tokio open can create the file after cleanup has already run.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(io_error)?;
    let mut guard = TemporaryGuard::new(temporary.clone());
    let mut file = tokio::fs::File::from_std(file);

    if target.existed {
        let permissions = tokio::fs::symlink_metadata(&destination)
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
    let staged_version = version(&file.metadata().await.map_err(io_error)?);
    drop(file);

    revalidate_target(context, target).await?;
    if let Some(expected) = expected {
        let current = read_bounded_file(&destination, context.policy.max_write_bytes).await?;
        if current != expected {
            return Err(changed_target("file changed while edit was in progress"));
        }
    }
    if let Some(expected) = expected_version {
        let metadata = tokio::fs::symlink_metadata(&destination)
            .await
            .map_err(io_error)?;
        if !metadata.is_file() || version(&metadata) != expected {
            return Err(conflict());
        }
    }
    check_active(context)?;
    #[cfg(test)]
    BEFORE_COMMIT.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook(&temporary);
        }
    });
    if version(&std::fs::symlink_metadata(&temporary).map_err(io_error)?) != staged_version {
        return Err(changed_target("staged output changed before replacement"));
    }
    check_active(context)?;
    if !same_directory(&parent, &target.parent)? {
        return Err(conflict());
    }
    // Do not detach a rename onto Tokio's blocking pool: cancellation could drop
    // the writer lock while that queued rename still mutates the target.
    std::fs::rename(&temporary, &destination).map_err(io_error)?;
    guard.committed = true;
    if context.policy.sync_writes {
        // The job owns an opened directory, never a /proc fd path that could be reused
        // after cancellation. Only sync is detached; rename stays under the lock.
        let directory = std::fs::File::open(&directory).map_err(io_error)?;
        tokio::task::spawn_blocking(move || directory.sync_all())
            .await
            .map_err(|e| io_error(io::Error::other(e)))?
            .map_err(io_error)?;
    }
    if !same_directory(&parent, &target.parent)? {
        return Err(changed_target(
            "target parent moved during replacement; inspect retained files",
        ));
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
            if metadata.permissions().readonly() {
                return Err(ToolError::new(
                    ToolErrorCode::PermissionDenied,
                    "read-only files cannot be replaced",
                    false,
                ));
            }
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
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let mut file = tokio::fs::File::from_std(options.open(path).map_err(io_error)?);
    let metadata = file.metadata().await.map_err(io_error)?;
    if !metadata.is_file() {
        return Err(ToolError::invalid("edit target is not a regular file"));
    }
    if metadata.len() > max_bytes as u64 {
        return Err(ToolError::invalid(format!(
            "file exceeds the {max_bytes} byte edit limit"
        )));
    }
    let mut content = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut content)
        .await
        .map_err(io_error)?;
    if content.len() > max_bytes {
        return Err(ToolError::invalid(format!(
            "file exceeds the {max_bytes} byte edit limit"
        )));
    }
    if version(&file.metadata().await.map_err(io_error)?) != version(&metadata)
        || version(&tokio::fs::symlink_metadata(path).await.map_err(io_error)?)
            != version(&metadata)
    {
        return Err(conflict());
    }
    Ok(content)
}

fn changed_target(message: &str) -> ToolError {
    ToolError::new(ToolErrorCode::Conflict, message, true)
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

#[cfg(test)]
thread_local! {
    static BEFORE_COMMIT: std::cell::RefCell<Option<Box<dyn FnOnce(&Path)>>> = const { std::cell::RefCell::new(None) };
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
    use crate::harness::runtime::path::resolve_write_target;
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
            host_service: None,
        }
    }

    #[tokio::test]
    async fn late_parent_swap_cannot_redirect_staging_rename_or_cleanup() {
        let workspace = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let context = context(workspace.path());
        let parent = workspace.path().join("nested");
        let moved = workspace.path().join("moved");
        std::fs::create_dir(&parent).unwrap();
        std::fs::write(parent.join("file"), "old").unwrap();
        std::fs::write(outside.path().join("file"), "outside").unwrap();
        let other = outside.path().to_owned();
        let original = parent.clone();
        let detached = moved.clone();
        BEFORE_COMMIT.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |_| {
                std::fs::rename(&original, &detached).unwrap();
                std::os::unix::fs::symlink(other, original).unwrap();
            }))
        });
        let error = WriteTool::new()
            .execute(&context, json!({"path":"nested/file","content":"new"}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Conflict);
        assert_eq!(
            std::fs::read(outside.path().join("file")).unwrap(),
            b"outside"
        );
        assert_eq!(std::fs::read(moved.join("file")).unwrap(), b"old");
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 1);
        assert!(!std::fs::read_dir(&moved).unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")));
    }

    #[tokio::test]
    async fn poisoned_staging_and_fifo_reads_fail_without_replacing_original() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        std::fs::write(workspace.path().join("file"), "old").unwrap();
        BEFORE_COMMIT.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(|temporary| {
                std::fs::write(temporary, "bad").unwrap();
            }))
        });
        let error = WriteTool::new()
            .execute(&context, json!({"path":"file","content":"new"}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Conflict);
        assert_eq!(
            std::fs::read(workspace.path().join("file")).unwrap(),
            b"old"
        );
        let fifo = workspace.path().join("fifo");
        nix::unistd::mkfifo(
            &fifo,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )
        .unwrap();
        assert!(read_bounded_file(&fifo, 1024).await.is_err());
    }

    #[tokio::test]
    async fn cancellation_before_commit_cleans_staging_and_releases_lock() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let path = workspace.path().join("file");
        std::fs::write(&path, "old").unwrap();
        let (held, target) = lock_target(&context, "file", false).await.unwrap();
        context.cancellation.cancel();
        let error = atomic_replace(&context, &target, b"cancelled", None, None)
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Cancelled);
        assert_eq!(std::fs::read(&path).unwrap(), b"old");
        assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 2);
        drop(held);
        let mut context = context;
        context.cancellation = CancellationToken::new();
        WriteTool::new()
            .execute(&context, json!({"path":"file", "content":"committed"}))
            .await
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"committed");
    }

    #[test]
    fn cancelled_pending_temporary_write_cannot_overwrite_a_later_commit() {
        let workspace = tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (release, wait) = std::sync::mpsc::channel();
            let (started, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            });
            ready.await.unwrap();
            let path = workspace.path().join("file");
            let temporary = workspace.path().join("staged.tmp");
            let pending = async {
                let file = std::fs::File::create_new(&temporary).unwrap();
                let _guard = TemporaryGuard::new(temporary.clone());
                let mut file = tokio::fs::File::from_std(file);
                file.write_all(b"cancelled").await.unwrap();
                file.flush().await.unwrap();
                panic!("blocked write must not finish");
            };
            let mut pending = Box::pin(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
            assert!(temporary.exists());
            drop(pending);
            assert!(!temporary.exists());
            std::fs::write(&path, "committed").unwrap();
            release.send(()).unwrap();
            blocker.await.unwrap();
            // Runtime shutdown joins the remaining blocking file work.
        });
        drop(runtime);
        assert_eq!(
            std::fs::read(workspace.path().join("file")).unwrap(),
            b"committed"
        );
        assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn inspected_versions_reject_same_length_changes_and_missing_targets() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let path = workspace.path().join("file");
        std::fs::write(&path, "old value").unwrap();
        let read = super::super::ReadTool::new()
            .execute(&context, json!({"path":"file"}))
            .await
            .unwrap();
        assert_eq!(
            read.metadata["sha256"],
            super::super::version::sha256(b"old value")
        );
        // Restore mtime to exercise ctime rather than length/mtime alone.
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(5));
        std::fs::write(&path, "old other").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        let error = super::super::apply_exact_patch(
            &context,
            "file",
            read.metadata["version"].as_str().unwrap(),
            "old",
            "new",
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Conflict);
        for precondition in ["expected_version", "expected_sha256"] {
            let mut input = json!({"path":"file", "content":"replacement"});
            input[precondition] = read.metadata[if precondition == "expected_version" {
                "version"
            } else {
                "sha256"
            }]
            .clone();
            assert_eq!(
                WriteTool::new()
                    .execute(&context, input)
                    .await
                    .unwrap_err()
                    .code,
                ToolErrorCode::Conflict
            );
        }
        assert_eq!(std::fs::read(&path).unwrap(), b"old other");
        std::fs::remove_file(&path).unwrap();
        assert_eq!(WriteTool::new().execute(&context, json!({
            "path":"file", "content":"replacement", "expected_version":read.metadata["version"],
        })).await.unwrap_err().code, ToolErrorCode::Conflict);
        assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn concurrent_integrators_have_one_winner_without_temporary_leaks() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        std::fs::write(workspace.path().join("file"), "old").unwrap();
        let read = super::super::ReadTool::new()
            .execute(&context, json!({"path":"file"}))
            .await
            .unwrap();
        let version = read.metadata["version"].as_str().unwrap();
        let write = WriteTool::new();
        let (a, b) = tokio::join!(
            super::super::apply_exact_patch(&context, "file", version, "old", "aaa"),
            write.execute(
                &context,
                json!({"path":"file", "content":"bbb", "expected_version":version})
            ),
        );
        assert_ne!(a.is_ok(), b.is_ok());
        let error = if let Err(error) = a {
            error
        } else {
            b.unwrap_err()
        };
        assert_eq!(error.code, ToolErrorCode::Conflict);
        let content = std::fs::read(workspace.path().join("file")).unwrap();
        assert!(content == b"aaa" || content == b"bbb");
        assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn digest_is_content_only_and_both_preconditions_must_match() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let path = workspace.path().join("file");
        std::fs::write(&path, "old").unwrap();
        let read = super::super::ReadTool::new()
            .execute(&context, json!({"path":"file"}))
            .await
            .unwrap();
        WriteTool::new()
            .execute(&context, json!({"path":"file", "content":"old"}))
            .await
            .unwrap();
        let error = WriteTool::new()
            .execute(
                &context,
                json!({
                    "path":"file", "content":"new", "expected_version":read.metadata["version"],
                    "expected_sha256":read.metadata["sha256"],
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Conflict);
        super::super::EditTool::new().execute(&context, json!({
            "path":"file", "old":"old", "new":"new", "expected_sha256":read.metadata["sha256"],
        })).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn late_symlink_swap_and_inode_replacement_preserve_targets() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let path = workspace.path().join("file");
        std::fs::write(&path, "old").unwrap();
        let target = resolve_write_target(&context, "file", false).await.unwrap();
        let snapshot = version(&std::fs::metadata(&path).unwrap());
        std::fs::rename(&path, workspace.path().join("original")).unwrap();
        std::os::unix::fs::symlink("original", &path).unwrap();
        assert_eq!(
            atomic_replace(&context, &target, b"new", None, Some(&snapshot))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::Conflict
        );
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "old").unwrap();
        assert_eq!(
            atomic_replace(&context, &target, b"new", None, Some(&snapshot))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::Conflict
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"old");
        assert_eq!(
            std::fs::read(workspace.path().join("original")).unwrap(),
            b"old"
        );
        assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn truncated_read_envelopes_never_advertise_a_complete_digest() {
        let workspace = tempdir().unwrap();
        let mut context = context(workspace.path());
        std::fs::write(workspace.path().join("file"), "x".repeat(800)).unwrap();
        let read = super::super::ReadTool::new()
            .execute(&context, json!({"path":"file"}))
            .await
            .unwrap();
        assert!(read.metadata["sha256"].is_string());
        let wire: Value = serde_json::from_str(&read.to_json(1000)).unwrap();
        assert_eq!(wire["truncated"], true);
        assert!(wire["metadata"]["sha256"].is_null());
        assert_eq!(wire["metadata"]["version"], read.metadata["version"]);

        Arc::make_mut(&mut context.policy).max_read_lines = 1;
        let read = super::super::ReadTool::new()
            .execute(&context, json!({"path":"file", "limit":2}))
            .await
            .unwrap();
        assert!(read.truncated);
        assert!(read.metadata["sha256"].is_null());
    }

    #[tokio::test]
    async fn partial_read_of_large_file_has_version_but_no_digest() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let path = workspace.path().join("file");
        std::fs::write(&path, "one\ntwo\n").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(1 << 30)
            .unwrap();
        let read = super::super::ReadTool::new()
            .execute(&context, json!({"path":"file", "limit":1}))
            .await
            .unwrap();
        assert_eq!(read.content, "one\n");
        assert!(read.truncated);
        assert!(read.metadata["version"]
            .as_str()
            .unwrap()
            .starts_with("stat-v1:"));
        assert!(read.metadata["sha256"].is_null());
        assert_eq!(read.metadata["size_bytes"], 1 << 30);
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

    #[cfg(unix)]
    #[tokio::test]
    async fn readonly_inputs_reject_write_edit_and_late_permission_changes() {
        use std::os::unix::fs::PermissionsExt;
        let workspace = tempdir().unwrap();
        let path = workspace.path().join("input");
        std::fs::write(&path, "baseline").unwrap();
        let context = context(workspace.path());
        let target = resolve_write_target(&context, "input", false)
            .await
            .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
        assert_eq!(
            atomic_replace(&context, &target, b"overwrite", None, None)
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        assert_eq!(
            WriteTool::new()
                .execute(&context, json!({"path":"input", "content":"overwrite"}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        assert_eq!(
            crate::harness::tools::workspace::EditTool::new()
                .execute(
                    &context,
                    json!({"path":"input", "old":"baseline", "new":"overwrite"})
                )
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"baseline");
        WriteTool::new()
            .execute(&context, json!({"path":"candidate", "content":"repair"}))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn changed_expected_content_aborts_and_removes_the_temporary_file() {
        let workspace = tempdir().unwrap();
        let path = workspace.path().join("file");
        std::fs::write(&path, "changed").unwrap();
        let context = context(workspace.path());
        let target = resolve_write_target(&context, "file", false).await.unwrap();
        let error = atomic_replace(&context, &target, b"replacement", Some(b"original"), None)
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Conflict);
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
