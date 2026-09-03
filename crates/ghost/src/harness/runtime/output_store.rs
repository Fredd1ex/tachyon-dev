#![forbid(unsafe_code)]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{OutputStoreFuture, ToolOutputRef, ToolOutputStore};

pub struct WorkspaceOutputStore {
    workspace_root: PathBuf,
    directory: PathBuf,
    max_bytes: usize,
    max_entry_bytes: usize,
    used_bytes: AtomicUsize,
}

impl WorkspaceOutputStore {
    pub async fn open(
        workspace_root: impl AsRef<Path>,
        max_bytes: usize,
        max_entry_bytes: usize,
    ) -> io::Result<Self> {
        let workspace_root = tokio::fs::canonicalize(workspace_root).await?;
        let requested = workspace_root.join(".tachyon/tool-output");
        validate_nearest_existing_parent(&workspace_root, &requested).await?;
        tokio::fs::create_dir_all(&requested).await?;
        let directory = tokio::fs::canonicalize(&requested).await?;
        ensure_within(&workspace_root, &directory)?;

        let mut used_bytes = 0_usize;
        let mut entries = tokio::fs::read_dir(&directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let metadata = tokio::fs::symlink_metadata(entry.path()).await?;
            if metadata.is_file() {
                used_bytes = used_bytes.saturating_add(metadata.len() as usize);
            }
        }
        Ok(Self {
            workspace_root,
            directory,
            max_bytes,
            max_entry_bytes,
            used_bytes: AtomicUsize::new(used_bytes),
        })
    }

    async fn put_value(&self, value: String) -> Option<ToolOutputRef> {
        let bytes = value.len();
        if bytes > self.max_entry_bytes || !self.reserve(bytes) {
            return None;
        }
        if self.validate_directory().await.is_err() {
            self.release(bytes);
            return None;
        }

        let id = Uuid::new_v4().to_string();
        let destination = self.directory.join(format!("{id}.json"));
        let temporary = self.directory.join(format!(".{id}.tmp"));
        let written = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await?;
            file.write_all(value.as_bytes()).await?;
            file.flush().await?;
            file.sync_data().await?;
            drop(file);
            self.validate_directory().await?;
            tokio::fs::rename(&temporary, &destination).await?;
            sync_directory(self.directory.clone()).await
        }
        .await;
        if written.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
            let _ = tokio::fs::remove_file(&destination).await;
            self.release(bytes);
            return None;
        }
        Some(ToolOutputRef { id })
    }

    async fn get_value(&self, reference: &ToolOutputRef) -> Option<String> {
        let id = Uuid::parse_str(&reference.id).ok()?;
        self.validate_directory().await.ok()?;
        let path = self.directory.join(format!("{id}.json"));
        let metadata = tokio::fs::symlink_metadata(&path).await.ok()?;
        if !metadata.is_file() || metadata.len() as usize > self.max_entry_bytes {
            return None;
        }
        let file = tokio::fs::File::open(path).await.ok()?;
        let mut value = Vec::with_capacity(metadata.len() as usize);
        file.take(self.max_entry_bytes as u64 + 1)
            .read_to_end(&mut value)
            .await
            .ok()?;
        if value.len() > self.max_entry_bytes {
            return None;
        }
        String::from_utf8(value).ok()
    }

    fn reserve(&self, bytes: usize) -> bool {
        let mut current = self.used_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.max_bytes {
                return false;
            }
            match self.used_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    async fn validate_directory(&self) -> io::Result<()> {
        let directory = tokio::fs::canonicalize(&self.directory).await?;
        ensure_within(&self.workspace_root, &directory)
    }
}

impl ToolOutputStore for WorkspaceOutputStore {
    fn put<'a>(&'a self, value: String) -> OutputStoreFuture<'a, Option<ToolOutputRef>> {
        Box::pin(self.put_value(value))
    }

    fn get<'a>(&'a self, reference: &'a ToolOutputRef) -> OutputStoreFuture<'a, Option<String>> {
        Box::pin(self.get_value(reference))
    }
}

async fn validate_nearest_existing_parent(workspace_root: &Path, path: &Path) -> io::Result<()> {
    let mut existing = path.to_path_buf();
    loop {
        match tokio::fs::symlink_metadata(&existing).await {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !existing.pop() {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "output directory has no existing parent",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    let canonical = tokio::fs::canonicalize(existing).await?;
    ensure_within(workspace_root, &canonical)
}

fn ensure_within(workspace_root: &Path, path: &Path) -> io::Result<()> {
    if path.starts_with(workspace_root) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "output directory escapes the workspace",
        ))
    }
}

pub(crate) async fn sync_directory(directory: PathBuf) -> io::Result<()> {
    tokio::task::spawn_blocking(move || std::fs::File::open(directory)?.sync_all())
        .await
        .map_err(io::Error::other)?
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn output_survives_store_reopen_and_counts_against_the_limit() {
        let workspace = tempdir().unwrap();
        let first = WorkspaceOutputStore::open(workspace.path(), 8, 8)
            .await
            .unwrap();
        let reference = first.put("1234".into()).await.unwrap();
        drop(first);

        let reopened = WorkspaceOutputStore::open(workspace.path(), 8, 8)
            .await
            .unwrap();
        assert_eq!(reopened.get(&reference).await.as_deref(), Some("1234"));
        assert!(reopened.put("56789".into()).await.is_none());
    }

    #[tokio::test]
    async fn rejects_invalid_reference_ids() {
        let workspace = tempdir().unwrap();
        let store = WorkspaceOutputStore::open(workspace.path(), 8, 8)
            .await
            .unwrap();
        assert!(store
            .get(&ToolOutputRef {
                id: "../escape".into()
            })
            .await
            .is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_a_tachyon_symlink_that_escapes_the_workspace() {
        let workspace = tempdir().unwrap();
        let outside = tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), workspace.path().join(".tachyon")).unwrap();
        let error = WorkspaceOutputStore::open(workspace.path(), 8, 8)
            .await
            .err()
            .expect("escaping symlink is rejected");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(!outside.path().join("tool-output").exists());
    }
}
