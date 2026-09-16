//! Host-only locked batch primitives. Not registered as a model-facing tool.
use super::{version, write, write_lock};
use crate::harness::runtime::path::WriteTarget;
use crate::harness::runtime::{ToolContext, ToolError};
use std::{
    fs::File,
    path::{Component, Path},
};

pub struct LockedFiles {
    _locks: Vec<File>,
    coordinator: WriteTarget,
    targets: Vec<(String, WriteTarget)>,
}

pub struct Snapshot {
    pub path: String,
    pub version: String,
    pub bytes: Vec<u8>,
}

impl LockedFiles {
    /// The coordinator lock is acquired before every target lock. Sorted canonical
    /// targets agree with the ordinary native writer's per-parent lock namespace.
    pub async fn acquire(context: &ToolContext, paths: &[String]) -> Result<Self, ToolError> {
        if context.cwd != context.workspace_root || paths.is_empty() || paths.len() > 32 {
            return Err(ToolError::invalid(
                "integration requires root cwd and 1..32 files",
            ));
        }
        let mut sorted = paths.to_vec();
        sorted.sort();
        if sorted.windows(2).any(|p| p[0] == p[1]) {
            return Err(ToolError::invalid("duplicate integration target"));
        }
        for path in &sorted {
            if path.len() > 4096
                || path
                    .split('/')
                    .any(|s| s.is_empty() || s == "." || s == "..")
                || Path::new(path)
                    .components()
                    .any(|c| !matches!(c, Component::Normal(_)))
                || path == ".tachyon-integration-coordinator"
            {
                return Err(ToolError::invalid(
                    "integration paths must be normalized relative file paths",
                ));
            }
            let mut current = context.workspace_root.clone();
            for component in Path::new(path).components() {
                current.push(component);
                let meta = std::fs::symlink_metadata(&current).map_err(io)?;
                if meta.file_type().is_symlink() {
                    return Err(ToolError::invalid("integration rejects symlinks"));
                }
            }
        }
        let (coordinator, coordinator_target) =
            write_lock::lock_target(context, ".tachyon-integration-coordinator", false).await?;
        let mut batch = Self {
            _locks: vec![coordinator],
            coordinator: coordinator_target,
            targets: Vec::new(),
        };
        for path in sorted {
            let (lock, target) = write_lock::lock_target(context, &path, false).await?;
            if target.path != context.workspace_root.join(&path) || !target.existed {
                return Err(version::conflict());
            }
            batch._locks.push(lock);
            batch.targets.push((path, target));
        }
        Ok(batch)
    }

    pub async fn snapshots(&self, context: &ToolContext) -> Result<Vec<Snapshot>, ToolError> {
        self.check_root()?;
        let mut snapshots = Vec::new();
        for (path, target) in &self.targets {
            write_lock::check_active(context)?;
            let parent = target.locked_parent.as_ref().expect("locked target");
            if !write_lock::same_directory(parent, &target.parent)? {
                return Err(version::conflict());
            }
            let pinned = write_lock::anchored(parent).join(target.path.file_name().unwrap());
            let before = version::version(&std::fs::symlink_metadata(&pinned).map_err(io)?);
            let bytes = write::read_bounded_file(&pinned, context.policy.max_write_bytes).await?;
            if version::version(&std::fs::symlink_metadata(&pinned).map_err(io)?) != before
                || !write_lock::same_directory(parent, &target.parent)?
            {
                return Err(version::conflict());
            }
            snapshots.push(Snapshot {
                path: path.clone(),
                version: before,
                bytes,
            });
        }
        self.check_root()?;
        Ok(snapshots)
    }

    pub async fn replace(
        &self,
        context: &ToolContext,
        snapshot: &Snapshot,
        output: &[u8],
    ) -> Result<(), ToolError> {
        self.check_root()?;
        let (_, target) = self
            .targets
            .iter()
            .find(|(p, _)| p == &snapshot.path)
            .ok_or_else(|| ToolError::invalid("target was not locked"))?;
        if output.len() > context.policy.max_write_bytes {
            return Err(ToolError::invalid("integration output exceeds write limit"));
        }
        write::atomic_replace(
            context,
            target,
            output,
            Some(&snapshot.bytes),
            Some(&snapshot.version),
        )
        .await?;
        Ok(())
    }

    fn check_root(&self) -> Result<(), ToolError> {
        if !write_lock::same_directory(
            self.coordinator
                .locked_parent
                .as_ref()
                .expect("locked coordinator"),
            &self.coordinator.parent,
        )? {
            return Err(version::conflict());
        }
        Ok(())
    }
}

fn io(error: std::io::Error) -> ToolError {
    ToolError::new(
        crate::harness::runtime::ToolErrorCode::Io,
        error.to_string(),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::runtime::{NoopEventSink, NoopOutputStore, ToolIdentity, ToolPolicy};
    use std::{
        io::{BufRead, Write},
        process::{Command, Stdio},
        sync::Arc,
        time::{Duration, Instant},
    };

    fn context(root: &Path) -> ToolContext {
        ToolContext {
            workspace_root: root.into(),
            cwd: root.into(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(5),
            cancellation: tokio_util::sync::CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root.into())),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        }
    }

    #[tokio::test]
    async fn replaced_root_or_parent_is_not_the_locked_directory() {
        for swap_root in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("root");
            std::fs::create_dir_all(root.join("nested")).unwrap();
            std::fs::write(root.join("nested/a"), "old").unwrap();
            let context = context(&root);
            let locked = LockedFiles::acquire(&context, &["nested/a".into()])
                .await
                .unwrap();
            let snapshots = locked.snapshots(&context).await.unwrap();
            let replaced = if swap_root {
                root.clone()
            } else {
                root.join("nested")
            };
            std::fs::rename(&replaced, temp.path().join("detached")).unwrap();
            std::fs::create_dir_all(root.join("nested")).unwrap();
            std::fs::write(root.join("nested/a"), "external").unwrap();
            assert!(locked.snapshots(&context).await.is_err());
            assert!(locked
                .replace(&context, &snapshots[0], b"new")
                .await
                .is_err());
            assert_eq!(std::fs::read(root.join("nested/a")).unwrap(), b"external");
        }
    }

    #[tokio::test]
    async fn batch_lock_process_fixture() {
        let Some(root) = std::env::var_os("GHOST_INTEGRATION_LOCK_FIXTURE") else {
            return;
        };
        let context = context(Path::new(&root));
        let held = LockedFiles::acquire(&context, &["b".into(), "a".into()])
            .await
            .unwrap();
        println!("BATCH_LOCK_READY");
        std::io::stdout().flush().unwrap();
        let mut release = String::new();
        std::io::stdin().read_line(&mut release).unwrap();
        drop(held);
    }

    #[tokio::test]
    async fn batch_holds_all_native_locks_cross_process_and_releases_on_exit() {
        use crate::harness::runtime::{Tool, ToolErrorCode};
        use crate::harness::tools::workspace::WriteTool;
        let root = tempfile::tempdir().unwrap();
        for name in ["a", "b"] {
            std::fs::write(root.path().join(name), b"old").unwrap();
        }
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "harness::tools::workspace::integration::tests::batch_lock_process_fixture",
                "--nocapture",
            ])
            .env("GHOST_INTEGRATION_LOCK_FIXTURE", root.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(stdout.read_line(&mut line).unwrap(), 0);
            if line.trim() == "BATCH_LOCK_READY" {
                break;
            }
        }
        for name in ["a", "b"] {
            let mut context = context(root.path());
            context.deadline = Instant::now() + Duration::from_millis(80);
            let result = WriteTool::new()
                .execute(&context, serde_json::json!({"path":name,"content":"bad"}))
                .await;
            assert_eq!(result.unwrap_err().code, ToolErrorCode::Timeout);
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let context = context(root.path());
        let held = LockedFiles::acquire(&context, &["a".into(), "b".into()])
            .await
            .unwrap();
        let snapshots = held.snapshots(&context).await.unwrap();
        assert_eq!(snapshots[0].path, "a");
        // A noncooperating shell/editor can still write while advisory locks are held.
        std::fs::write(root.path().join("a"), b"external").unwrap();
        assert_eq!(
            held.replace(&context, &snapshots[0], b"new")
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::Conflict
        );
        assert_eq!(std::fs::read(root.path().join("a")).unwrap(), b"external");
        assert_eq!(std::fs::read(root.path().join("b")).unwrap(), b"old");
    }
}
