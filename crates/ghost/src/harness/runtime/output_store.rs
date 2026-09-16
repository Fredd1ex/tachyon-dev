#![forbid(unsafe_code)]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{OutputStoreFuture, ToolOutputRef, ToolOutputStore};

/// Live-work output extension shared by exec and ctx. Files are anonymous and
/// never addressed by caller paths. Capacity is reserved before a process starts.
#[derive(Default)]
pub(crate) struct WorkOutputStore {
    entries: std::sync::Mutex<std::collections::BTreeMap<String, std::sync::Arc<Spool>>>,
}

pub(crate) struct Spool {
    state: tokio::sync::Mutex<SpoolState>,
    cap: AtomicUsize,
}

struct SpoolState {
    file: tokio::fs::File,
    retained: usize,
    total: u64,
    failed: bool,
}

impl WorkOutputStore {
    pub(crate) fn snapshot(&self) -> Self {
        Self {
            entries: std::sync::Mutex::new(self.entries.lock().unwrap().clone()),
        }
    }
    pub fn create(&self, cap: usize) -> io::Result<(ToolOutputRef, std::sync::Arc<Spool>)> {
        let mut entries = self.entries.lock().unwrap();
        if cap > 64 * 1024 * 1024
            || entries.len() >= 256
            || entries
                .values()
                .map(|s| s.cap.load(Ordering::Acquire))
                .sum::<usize>()
                + cap
                > 128 * 1024 * 1024
        {
            return Err(io::Error::other("work output capacity exhausted"));
        }
        let spool = std::sync::Arc::new(Spool {
            state: tokio::sync::Mutex::new(SpoolState {
                file: tokio::fs::File::from_std(tempfile::tempfile()?),
                retained: 0,
                total: 0,
                failed: false,
            }),
            cap: cap.into(),
        });
        let reference = ToolOutputRef {
            id: format!("output:{}", Uuid::new_v4()),
        };
        entries.insert(reference.id.clone(), spool.clone());
        Ok((reference, spool))
    }

    pub fn list(&self) -> Vec<ToolOutputRef> {
        self.entries
            .lock()
            .unwrap()
            .keys()
            .map(|id| ToolOutputRef { id: id.clone() })
            .collect()
    }

    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }

    pub fn remove(&self, reference: &ToolOutputRef) {
        self.entries.lock().unwrap().remove(&reference.id);
    }

    pub async fn page(
        &self,
        reference: &ToolOutputRef,
        cursor: usize,
        limit: usize,
    ) -> Result<super::ToolResult, super::ToolError> {
        self.page_query(reference, cursor, limit, None).await
    }

    async fn page_query(
        &self,
        reference: &ToolOutputRef,
        cursor: usize,
        limit: usize,
        query: Option<&str>,
    ) -> Result<super::ToolResult, super::ToolError> {
        use tokio::io::AsyncSeekExt;
        if limit == 0 || limit > 8192 {
            return Err(super::ToolError::invalid("limit must be 1..8192"));
        }
        let spool = self
            .entries
            .lock()
            .unwrap()
            .get(&reference.id)
            .cloned()
            .ok_or_else(|| {
                super::ToolError::new(
                    super::ToolErrorCode::PermissionDenied,
                    "unknown output reference in this work",
                    false,
                )
            })?;
        let mut state = spool.state.lock().await;
        if cursor > state.retained {
            return Err(super::ToolError::invalid("cursor exceeds retained output"));
        }
        let count = limit.min(state.retained - cursor);
        let mut bytes = vec![0; count];
        state
            .file
            .seek(std::io::SeekFrom::Start(cursor as u64))
            .await
            .map_err(page_error)?;
        state
            .file
            .read_exact(&mut bytes)
            .await
            .map_err(page_error)?;
        let (content, decoding_truncated) =
            super::bound_utf8(&String::from_utf8_lossy(&bytes), limit);
        let mut result = super::ToolResult::success(
            content,
            serde_json::json!({
                "reference": reference, "cursor": cursor, "next_cursor": cursor + count,
                "retained_bytes": state.retained, "total_bytes": state.total,
                "discarded_bytes": state.total.saturating_sub(state.retained as u64),
                "storage_failed": state.failed, "has_more": cursor + count < state.retained,
                "encoding": "utf8_lossy", "decoding_truncated": decoding_truncated,
                "utf8_valid": std::str::from_utf8(&bytes).is_ok()
            }),
        );
        result.truncated = decoding_truncated
            || state.total > state.retained as u64
            || cursor + count < state.retained;
        if let Some(query) = query {
            if query.is_empty() || query.len() > 1024 || query.len() >= limit {
                return Err(super::ToolError::invalid(
                    "query must be 1..1024 bytes and shorter than limit",
                ));
            }
            let matches = bytes
                .windows(query.len())
                .enumerate()
                .filter(|(_, window)| *window == query.as_bytes())
                .take(128)
                .map(|(offset, _)| cursor + offset)
                .collect::<Vec<_>>();
            let match_limit = matches.len() == 128;
            let next = if match_limit {
                matches.last().copied().unwrap() + 1
            } else {
                cursor + count.saturating_sub(query.len() - 1)
            };
            result.metadata["matches"] = serde_json::json!(matches);
            result.metadata["match_limit_reached"] = serde_json::json!(match_limit);
            result.metadata["next_cursor"] = serde_json::json!(next);
            result.metadata["has_more"] =
                serde_json::json!(match_limit || cursor + count < state.retained);
            result.metadata["search_scope"] =
                serde_json::json!("one page; literal UTF-8 bytes; overlapping matches");
            result.content.clear();
        }
        Ok(result)
    }
}

fn page_error(error: io::Error) -> super::ToolError {
    super::ToolError::new(super::ToolErrorCode::Io, error.to_string(), false)
}

impl ToolOutputStore for WorkOutputStore {
    fn export_page<'a>(
        &'a self,
        reference: &'a ToolOutputRef,
        offset: u64,
    ) -> OutputStoreFuture<'a, Result<super::OutputPage, String>> {
        Box::pin(async move {
            use tokio::io::AsyncSeekExt;
            let spool = self
                .entries
                .lock()
                .unwrap()
                .get(&reference.id)
                .cloned()
                .ok_or("unknown output reference")?;
            let mut state = spool.state.lock().await;
            let retained = state.retained as u64;
            if offset > retained {
                return Err("output offset exceeds snapshot".into());
            }
            let mut bytes = vec![
                0;
                (retained - offset).min(tachyon_model::broker::UPLOAD_CHUNK as u64)
                    as usize
            ];
            state.file.flush().await.map_err(|e| e.to_string())?;
            state
                .file
                .seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(|e| e.to_string())?;
            state
                .file
                .read_exact(&mut bytes)
                .await
                .map_err(|e| e.to_string())?;
            Ok(super::OutputPage {
                bytes,
                retained,
                total: state.total,
                storage_failed: state.failed,
            })
        })
    }
    fn put<'a>(&'a self, value: String) -> OutputStoreFuture<'a, Option<ToolOutputRef>> {
        Box::pin(async move {
            if value.len() > 64 * 1024 * 1024 {
                return None;
            }
            let (reference, spool) = self.create(value.len()).ok()?;
            spool.append(value.as_bytes()).await;
            if spool.state.lock().await.failed {
                self.remove(&reference);
                return None;
            }
            Some(reference)
        })
    }
    fn get<'a>(&'a self, reference: &'a ToolOutputRef) -> OutputStoreFuture<'a, Option<String>> {
        Box::pin(async move {
            use tokio::io::AsyncSeekExt;
            let spool = self.entries.lock().unwrap().get(&reference.id).cloned()?;
            let mut state = spool.state.lock().await;
            let mut bytes = vec![0; state.retained];
            state.file.seek(std::io::SeekFrom::Start(0)).await.ok()?;
            state.file.read_exact(&mut bytes).await.ok()?;
            String::from_utf8(bytes).ok()
        })
    }
    fn page<'a>(
        &'a self,
        reference: &'a ToolOutputRef,
        cursor: usize,
        limit: usize,
    ) -> OutputStoreFuture<'a, Result<super::ToolResult, super::ToolError>> {
        Box::pin(WorkOutputStore::page(self, reference, cursor, limit))
    }
    fn references(&self) -> Vec<ToolOutputRef> {
        self.list()
    }
    fn search_page<'a>(
        &'a self,
        reference: &'a ToolOutputRef,
        cursor: usize,
        limit: usize,
        query: &'a str,
    ) -> OutputStoreFuture<'a, Result<super::ToolResult, super::ToolError>> {
        Box::pin(self.page_query(reference, cursor, limit, Some(query)))
    }
}

impl Spool {
    pub async fn seal(&self) {
        let state = self.state.lock().await;
        self.cap.store(state.retained, Ordering::Release);
    }
    pub async fn append(&self, bytes: &[u8]) {
        use tokio::io::AsyncSeekExt;
        let mut state = self.state.lock().await;
        state.total = state.total.saturating_add(bytes.len() as u64);
        let count = bytes.len().min(
            self.cap
                .load(Ordering::Acquire)
                .saturating_sub(state.retained),
        );
        if count == 0 || state.failed {
            return;
        }
        let offset = state.retained as u64;
        let written = async {
            state.file.seek(std::io::SeekFrom::Start(offset)).await?;
            state.file.write_all(&bytes[..count]).await?;
            state.file.flush().await
        }
        .await;
        if written.is_ok() {
            state.retained += count;
        } else {
            state.failed = true;
        }
    }
}

#[cfg(test)]
#[tokio::test]
async fn cpu_permit_released_exactly_once_after_spool_write_failure() {
    use super::*;
    use std::{sync::Arc, time::Duration};
    use tachyon_model::broker::*;
    let root = tempfile::tempdir().unwrap();
    let (host, client) = private_pair().unwrap();
    let context = ToolContext {
        workspace_root: root.path().into(),
        cwd: root.path().into(),
        identity: ToolIdentity::default(),
        deadline: std::time::Instant::now() + Duration::from_secs(5),
        cancellation: tokio_util::sync::CancellationToken::new(),
        policy: Arc::new(ToolPolicy::worker_default(root.path().into())),
        event_sink: Arc::new(NoopEventSink),
        output_store: Arc::new(NoopOutputStore),
        host_service: Some(Arc::new(client)),
    };
    let registry = native_registry()
        .for_work(&context.policy, &[], &Default::default())
        .unwrap();
    let outputs = registry.work_outputs().unwrap();
    let server = async {
        let mut stream = host.authenticate().await.unwrap();
        assert!(matches!(
            read_frame(&mut stream).await.unwrap(),
            FrameRequest::CpuJob(CpuJobRequest::Acquire { .. })
        ));
        let spools: Vec<_> = outputs.entries.lock().unwrap().values().cloned().collect();
        assert_eq!(spools.len(), 2);
        for spool in &spools {
            // Read-only descriptors deterministically reject capture writes.
            spool.state.lock().await.file = tokio::fs::File::open("/dev/null").await.unwrap();
        }
        let id = Uuid::new_v4();
        write_frame(
            &mut stream,
            &FrameReply::CpuJob(CpuJobReply::Granted {
                permit: id,
                device_ids: Vec::new(),
            }),
        )
        .await
        .unwrap();
        assert!(
            matches!(read_frame(&mut stream).await.unwrap(), FrameRequest::CpuJob(CpuJobRequest::Release { permit }) if permit == id)
        );
        write_frame(&mut stream, &FrameReply::CpuJob(CpuJobReply::Released))
            .await
            .unwrap();
        // The next request is not a second release from the permit's Drop.
        assert!(matches!(
            read_frame(&mut stream).await.unwrap(),
            FrameRequest::CpuJob(CpuJobRequest::Profile)
        ));
        for spool in &spools {
            assert!(spool.state.lock().await.failed);
        }
        write_frame(
            &mut stream,
            &FrameReply::CpuJob(CpuJobReply::Profile {
                max_cpu_jobs: 1,
                max_gpu_jobs: 0,
            }),
        )
        .await
        .unwrap();
    };
    let worker = async {
        let result = registry
            .execute(
                "exec",
                &context,
                serde_json::json!({"command":"printf stdout; printf stderr >&2"}),
            )
            .await
            .unwrap();
        assert!(!result.is_error, "{result:?}");
        context
            .host_service
            .as_ref()
            .unwrap()
            .cpu_job(CpuJobRequest::Profile)
            .await
            .unwrap();
        registry.finish_work().await;
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(server, worker);
    })
    .await
    .unwrap();
}

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
    async fn live_pages_bound_binary_decoding_search_overlap_and_storage() {
        let store = WorkOutputStore::default();
        let (reference, spool) = store.create(16).unwrap();
        spool.append(b"abcneedledef\xff").await;
        let first = store.search_page(&reference, 0, 8, "needle").await.unwrap();
        assert_eq!(first.metadata["matches"], serde_json::json!([]));
        let next = first.metadata["next_cursor"].as_u64().unwrap() as usize;
        let second = store
            .search_page(&reference, next, 8, "needle")
            .await
            .unwrap();
        assert_eq!(second.metadata["matches"], serde_json::json!([3]));
        let binary = store.page(&reference, 12, 1).await.unwrap();
        assert!(binary.content.len() <= 1);
        assert_eq!(binary.metadata["next_cursor"], 13);
        assert_eq!(binary.metadata["decoding_truncated"], true);
        spool.append(b"more than capacity").await;
        let page = store.page(&reference, 0, 8192).await.unwrap();
        assert_eq!(page.metadata["retained_bytes"], 16);
        assert!(page.metadata["discarded_bytes"].as_u64().unwrap() > 0);
        assert!(store.page(&reference, 0, 8193).await.is_err());
        assert!(store.page(&reference, 17, 1).await.is_err());
        store.clear();
        assert!(store.page(&reference, 0, 1).await.is_err());

        // Reservation does not allocate these bytes on disk or in memory.
        let (a, _) = store.create(64 * 1024 * 1024).unwrap();
        store.create(64 * 1024 * 1024).unwrap();
        assert!(store.create(1).is_err());
        store.remove(&a);
        assert!(store.create(64 * 1024 * 1024 + 1).is_err());
        assert!(store.create(64 * 1024 * 1024).is_ok());
    }

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
