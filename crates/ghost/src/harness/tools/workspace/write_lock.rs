#![forbid(unsafe_code)]

use std::fs::File;
use std::time::{Duration, Instant};

use crate::harness::runtime::path::{resolve_write_target, WriteTarget};
use crate::harness::runtime::{ToolContext, ToolError, ToolErrorCode};

use crate::harness::runtime::path::NATIVE_WRITE_LOCK_DIRECTORY as LOCK_DIRECTORY;

// File owns the descriptor: dropping a pending future or the guard releases flock.
pub(super) async fn lock_target(
    context: &ToolContext,
    requested: &str,
    create_parents: bool,
) -> Result<(File, WriteTarget), ToolError> {
    check_active(context)?;
    let target = resolve_write_target(context, requested, create_parents).await?;
    let (file, parent) = open_lock(&target).map_err(|error| {
        ToolError::new(
            ToolErrorCode::PermissionDenied,
            format!("cannot open native writer lock (requires a writable parent and private lock directory): {error}"),
            false,
        )
    })?;
    loop {
        check_active(context)?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => {
                return Err(ToolError::new(ToolErrorCode::Io, error.to_string(), false));
            }
        }
        tokio::select! {
            _ = context.cancellation.cancelled() => {},
            _ = tokio::time::sleep_until(context.deadline.into()) => {},
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        }
    }
    check_active(context)?;
    // Existence and permissions may have changed while another process held the lock.
    let mut current = resolve_write_target(context, requested, false).await?;
    if current.path != target.path || !same_directory(&parent, &current.parent)? {
        return Err(super::version::conflict());
    }
    current.locked_parent = Some(parent);
    Ok((file, current))
}

pub(super) fn check_active(context: &ToolContext) -> Result<(), ToolError> {
    if context.cancellation.is_cancelled() {
        return Err(ToolError::new(
            ToolErrorCode::Cancelled,
            "native write cancelled",
            true,
        ));
    }
    if Instant::now() >= context.deadline {
        return Err(ToolError::new(
            ToolErrorCode::Timeout,
            "native write deadline elapsed",
            true,
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_lock(target: &WriteTarget) -> std::io::Result<(File, File)> {
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    use nix::errno::Errno;
    use nix::fcntl::OFlag;
    use nix::sys::stat::{mkdirat, Mode};

    let directory_flags =
        (OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits();
    // nix 0.29 openat returns a raw fd. /proc/self/fd provides descriptor-relative
    // opens with std's owned File instead, without unsafe conversion or fd leaks.
    // O_PATH needs search permission, not read permission on every ancestor.
    // Each borrowed directory stays open until its child open has completed.
    let parent = open_parent(&target.parent)?;
    if parent.metadata()?.mode() & 0o222 == 0 {
        return Err(io::Error::other("target parent is read-only"));
    }
    match mkdirat(
        Some(parent.as_raw_fd()),
        LOCK_DIRECTORY,
        Mode::from_bits_truncate(0o700),
    ) {
        Ok(()) | Err(Errno::EEXIST) => {}
        Err(error) => return Err(error.into()),
    }
    let directory = File::options()
        .read(true)
        .custom_flags(directory_flags)
        .open(anchored(&parent).join(LOCK_DIRECTORY))?;
    let metadata = directory.metadata()?;
    if metadata.uid() != nix::unistd::geteuid().as_raw() || metadata.mode() & 0o7777 != 0o700 {
        return Err(io::Error::other(
            "lock directory must be host-owned mode 0700",
        ));
    }
    // Parent directory identity plus basename survives atomic target replacement
    // and agrees across cwd aliases and overlapping authorized workspace roots.
    let name = super::version::sha256(target.path.file_name().unwrap().as_bytes());
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags((OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK).bits())
        .open(anchored(&directory).join(name))?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(io::Error::other(
            "lock must be a host-owned, single-link regular file of mode 0600",
        ));
    }
    Ok((file, parent))
}

#[cfg(not(target_os = "linux"))]
fn open_lock(_target: &WriteTarget) -> std::io::Result<(File, File)> {
    Err(std::io::Error::other(
        "native writer locking requires Linux descriptor-relative /proc/self/fd access",
    ))
}

pub(super) fn anchored(directory: &File) -> std::path::PathBuf {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        std::path::PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
    }
    #[cfg(not(target_os = "linux"))]
    unreachable!("native writer locking requires Linux")
}

pub(super) fn open_parent(path: &std::path::Path) -> std::io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::OFlag;
        use std::os::unix::fs::OpenOptionsExt;
        let flags =
            (OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits();
        let mut parent = File::options().read(true).custom_flags(flags).open("/")?;
        for component in path.components() {
            if let std::path::Component::Normal(name) = component {
                parent = File::options()
                    .read(true)
                    .custom_flags(flags)
                    .open(anchored(&parent).join(name))?;
            }
        }
        Ok(parent)
    }
    #[cfg(not(target_os = "linux"))]
    Err(std::io::Error::other(
        "native writer locking requires Linux",
    ))
}

pub(super) fn same_directory(parent: &File, path: &std::path::Path) -> Result<bool, ToolError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        let current = open_parent(path).and_then(|f| f.metadata());
        let original = parent.metadata();
        Ok(match (original, current) {
            (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
            _ => false,
        })
    }
    #[cfg(not(target_os = "linux"))]
    Ok(false)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::{BufRead, Write};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use crate::harness::runtime::{NoopEventSink, NoopOutputStore, Tool, ToolIdentity, ToolPolicy};
    use crate::harness::tools::workspace::{apply_exact_patch, EditTool, ReadTool, WriteTool};

    const FIXTURE: &str =
        "harness::tools::workspace::write_lock::tests::native_lock_process_fixture";

    fn context(root: &Path) -> ToolContext {
        let root = root.canonicalize().unwrap();
        ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(10),
            cancellation: CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root)),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        }
    }

    struct Process(Child);

    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn spawn(root: &Path, path: &str, mode: &str, expected: &str) -> Process {
        Process(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", FIXTURE, "--nocapture"])
                .env("GHOST_LOCK_FIXTURE_ROOT", root)
                .env("GHOST_LOCK_FIXTURE_PATH", path)
                .env("GHOST_LOCK_FIXTURE_MODE", mode)
                .env("GHOST_LOCK_FIXTURE_EXPECTED", expected)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        )
    }

    fn ready(process: &mut Process) {
        let mut stdout = std::io::BufReader::new(process.0.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            assert_ne!(
                stdout.read_line(&mut line).unwrap(),
                0,
                "fixture exited before ready"
            );
            if line.trim() == "LOCK_FIXTURE_READY" {
                break;
            }
        }
        // Keep stdout open until the test runner has finished printing its result.
        process.0.stdout = Some(stdout.into_inner());
    }

    #[tokio::test]
    async fn native_lock_process_fixture() {
        let Some(root) = std::env::var_os("GHOST_LOCK_FIXTURE_ROOT") else {
            return;
        };
        let context = context(Path::new(&root));
        let path = std::env::var("GHOST_LOCK_FIXTURE_PATH").unwrap();
        let mode = std::env::var("GHOST_LOCK_FIXTURE_MODE").unwrap();
        let expected = std::env::var("GHOST_LOCK_FIXTURE_EXPECTED").unwrap();
        let held = if mode == "hold" {
            Some(lock_target(&context, &path, false).await.unwrap())
        } else {
            None
        };
        println!("LOCK_FIXTURE_READY");
        std::io::stdout().flush().unwrap();
        if mode == "hold" {
            let mut release = String::new();
            std::io::stdin().read_line(&mut release).unwrap();
            drop(held);
            return;
        }
        let result = if mode == "patch" {
            apply_exact_patch(&context, &path, &expected, "old", "patch").await
        } else {
            let key = if mode == "sha" {
                "expected_sha256"
            } else {
                "expected_version"
            };
            let mut input = json!({"path":path, "content":"write"});
            input[key] = json!(expected);
            WriteTool::new().execute(&context, input).await
        };
        let result = match result {
            Ok(_) => "won",
            Err(error) if error.code == ToolErrorCode::Conflict => "conflict",
            Err(error) => panic!("unexpected fixture failure: {error:?}"),
        };
        std::fs::write(context.cwd.join(format!("result-{mode}")), result).unwrap();
    }

    #[tokio::test]
    async fn ipc_conditional_writers_share_lock_across_nested_roots_and_aliases() {
        for digest in [false, true] {
            let root = tempdir().unwrap();
            std::fs::create_dir(root.path().join("nested")).unwrap();
            std::fs::write(root.path().join("nested/file"), "old").unwrap();
            std::os::unix::fs::symlink("nested", root.path().join("alias")).unwrap();
            let context = context(root.path());
            let read = ReadTool::new()
                .execute(&context, json!({"path":"nested/file"}))
                .await
                .unwrap();
            let held = lock_target(&context, "nested/file", false).await.unwrap();
            let mut patch = spawn(
                root.path(),
                "alias/./file",
                "patch",
                read.metadata["version"].as_str().unwrap(),
            );
            let mode = if digest { "sha" } else { "write" };
            let key = if digest { "sha256" } else { "version" };
            let mut write = spawn(
                &root.path().join("nested"),
                "file",
                mode,
                read.metadata[key].as_str().unwrap(),
            );
            ready(&mut patch);
            ready(&mut write);
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(patch.0.try_wait().unwrap().is_none());
            assert!(write.0.try_wait().unwrap().is_none());
            assert_eq!(
                std::fs::read(root.path().join("nested/file")).unwrap(),
                b"old"
            );
            drop(held);
            assert!(patch.0.wait().unwrap().success());
            assert!(write.0.wait().unwrap().success());
            let a = std::fs::read_to_string(root.path().join("result-patch")).unwrap();
            let b =
                std::fs::read_to_string(root.path().join(format!("nested/result-{mode}"))).unwrap();
            assert!(matches!(
                (a.as_str(), b.as_str()),
                ("won", "conflict") | ("conflict", "won")
            ));
            let held = lock_target(&context, "nested/file", false).await.unwrap();
            drop(held);
        }
    }

    #[tokio::test]
    async fn ipc_wait_cancellation_deadline_other_files_and_process_exit() {
        let root = tempdir().unwrap();
        std::fs::write(root.path().join("file"), "old").unwrap();
        let mut holder = spawn(root.path(), "file", "hold", "");
        ready(&mut holder);
        let mut context = context(root.path());
        let cancellation = context.cancellation.clone();
        let cancel = async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            cancellation.cancel();
        };
        let tool = EditTool::new();
        let (result, _) = tokio::join!(
            tool.execute(&context, json!({"path":"file", "old":"old", "new":"bad"})),
            cancel,
        );
        assert_eq!(result.unwrap_err().code, ToolErrorCode::Cancelled);
        context.cancellation = CancellationToken::new();
        context.deadline = Instant::now() + Duration::from_millis(80);
        assert_eq!(
            tool.execute(&context, json!({"path":"file", "old":"old", "new":"bad"}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::Timeout
        );
        context.deadline = Instant::now() + Duration::from_secs(5);
        // Aborting a pending future closes its fd; it must not leave a queued writer.
        assert!(tokio::time::timeout(
            Duration::from_millis(40),
            tool.execute(&context, json!({"path":"file", "old":"old", "new":"bad"}))
        )
        .await
        .is_err());
        WriteTool::new()
            .execute(&context, json!({"path":"other", "content":"independent"}))
            .await
            .unwrap();
        assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
        holder.0.kill().unwrap();
        holder.0.wait().unwrap();
        tool.execute(
            &context,
            json!({"path":"file", "old":"old", "new":"released"}),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(root.path().join("file")).unwrap(),
            b"released"
        );
        assert!(std::fs::read_dir(root.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")));
    }

    #[tokio::test]
    async fn unsafe_lock_artifacts_fail_closed_without_chmod_or_truncation() {
        use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
        for kind in [
            "directory-symlink",
            "directory-mode",
            "file-symlink",
            "file-mode",
            "hardlink",
            "fifo",
            "readonly",
        ] {
            let root = tempdir().unwrap();
            let context = context(root.path());
            std::fs::write(root.path().join("file"), "old").unwrap();
            let directory = root.path().join(LOCK_DIRECTORY);
            if kind == "directory-symlink" {
                symlink(root.path(), &directory).unwrap();
            } else {
                std::fs::create_dir(&directory).unwrap();
                std::fs::set_permissions(
                    &directory,
                    std::fs::Permissions::from_mode(if kind == "directory-mode" {
                        0o755
                    } else {
                        0o700
                    }),
                )
                .unwrap();
                let lock = directory.join(super::super::version::sha256(b"file"));
                match kind {
                    "file-symlink" => symlink("../file", &lock).unwrap(),
                    "hardlink" => std::fs::hard_link(root.path().join("file"), &lock).unwrap(),
                    "fifo" => {
                        nix::unistd::mkfifo(&lock, nix::sys::stat::Mode::from_bits_truncate(0o600))
                            .unwrap()
                    }
                    "readonly" => std::fs::set_permissions(
                        root.path(),
                        std::fs::Permissions::from_mode(0o555),
                    )
                    .unwrap(),
                    _ => {
                        std::fs::write(&lock, "do not truncate").unwrap();
                        std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o644))
                            .unwrap();
                    }
                }
            }
            let error = WriteTool::new()
                .execute(&context, json!({"path":"file", "content":"bad"}))
                .await
                .unwrap_err();
            assert_eq!(error.code, ToolErrorCode::PermissionDenied, "{kind}");
            assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
            if kind == "file-mode" {
                let lock = directory.join(super::super::version::sha256(b"file"));
                assert_eq!(std::fs::read(&lock).unwrap(), b"do not truncate");
                assert_eq!(std::fs::metadata(lock).unwrap().mode() & 0o777, 0o644);
            }
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    #[tokio::test]
    async fn search_only_ancestors_and_unlistable_writable_parent_support_locks() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempdir().unwrap();
        let mut context = context(root.path());
        // Directory fsync separately requires a readable target parent.
        Arc::make_mut(&mut context.policy).sync_writes = false;
        let ancestor = root.path().join("ancestor");
        let parent = ancestor.join("parent");
        std::fs::create_dir_all(&parent).unwrap();
        std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o111)).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o300)).unwrap();
        let result = WriteTool::new()
            .execute(
                &context,
                json!({"path":"ancestor/parent/file", "content":"new"}),
            )
            .await;
        std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
        result.unwrap();
        assert_eq!(std::fs::read(parent.join("file")).unwrap(), b"new");
    }

    #[tokio::test]
    async fn outside_paths_create_no_locks_and_noncooperating_writes_are_not_excluded() {
        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let context = context(root.path());
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
        let error = WriteTool::new()
            .execute(&context, json!({"path":"escape/file", "content":"bad"}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::OutsideWorkspace);
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);

        let path = root.path().join("file");
        std::fs::write(&path, "old").unwrap();
        let read = ReadTool::new()
            .execute(&context, json!({"path":"file"}))
            .await
            .unwrap();
        let held = lock_target(&context, "file", false).await.unwrap();
        // A real noncooperating handle can mutate the target even while flock is held.
        let mut other = File::options().write(true).open(&path).unwrap();
        other.write_all(b"new").unwrap();
        drop(other);
        drop(held);
        let error = WriteTool::new()
            .execute(
                &context,
                json!({"path":"file", "content":"bad", "expected_sha256":read.metadata["sha256"]}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Conflict);
        assert_eq!(std::fs::read(path).unwrap(), b"new");
    }

    #[tokio::test]
    async fn lock_files_are_private_persistent_and_reserved() {
        use std::os::unix::fs::MetadataExt;
        let root = tempdir().unwrap();
        let context = context(root.path());
        WriteTool::new()
            .execute(&context, json!({"path":"file", "content":"old"}))
            .await
            .unwrap();
        let directory = root.path().join(LOCK_DIRECTORY);
        let name = super::super::version::sha256(b"file");
        let before = std::fs::metadata(directory.join(&name)).unwrap();
        assert_eq!(before.mode() & 0o777, 0o600);
        assert_eq!(std::fs::metadata(&directory).unwrap().mode() & 0o777, 0o700);
        WriteTool::new()
            .execute(&context, json!({"path":"file", "content":"new"}))
            .await
            .unwrap();
        assert_eq!(
            before.ino(),
            std::fs::metadata(directory.join(&name)).unwrap().ino()
        );
        assert_eq!(
            WriteTool::new()
                .execute(
                    &context,
                    json!({"path":format!("{LOCK_DIRECTORY}/{name}"), "content":"bad"})
                )
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::OutsideWorkspace
        );
        assert_eq!(WriteTool::new().execute(&context, json!({"path":format!("{LOCK_DIRECTORY}/new/file"), "content":"bad", "create_parents":true})).await.unwrap_err().code, ToolErrorCode::OutsideWorkspace);
        assert!(!directory.join("new").exists());
        std::os::unix::fs::symlink(LOCK_DIRECTORY, root.path().join("alias")).unwrap();
        assert_eq!(
            ReadTool::new()
                .execute(&context, json!({"path":format!("alias/{name}")}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::OutsideWorkspace
        );
        let listing = crate::harness::tools::workspace::LsTool::new()
            .execute(&context, json!({}))
            .await
            .unwrap();
        assert!(!listing.content.contains(LOCK_DIRECTORY));
        let found = crate::harness::tools::workspace::FindTool::new()
            .execute(&context, json!({"pattern":"**", "hidden":true}))
            .await
            .unwrap();
        assert!(!found.content.contains(LOCK_DIRECTORY));
    }
}
