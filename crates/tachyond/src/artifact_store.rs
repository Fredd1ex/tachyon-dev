//! Host-only immutable snapshots. Filesystem publication and metadata commit are
//! separate durable steps; pending records are reconciled when the store opens.
#![forbid(unsafe_code)]

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use rustix::fs::{openat, Mode, OFlags as OFlag};
use sha2::{Digest, Sha256};
use tachyon_api::types::{
    AgentEvent, AgentInfo, ApiRequest, ApiResponse, ArtifactPublication, ArtifactRegistration,
    EventEnvelope,
};

const MANIFEST: TableDefinition<&str, &str> = TableDefinition::new("artifacts_v1");
pub const MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_STORE_BYTES: u64 = 10 * MAX_SNAPSHOT_BYTES;
const MAX_READ_BYTES: usize = 1024 * 1024;
const MAX_RECORDS: u64 = 20_000;
type Result<T> = std::result::Result<T, String>;

fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn key(scope: &str, id: &str) -> String {
    format!("{}-{}", digest(scope.as_bytes()), digest(id.as_bytes()))
}

pub struct ArtifactStore {
    retained: Option<(crate::retained_storage::RetainedStorage, String)>,
    root: PathBuf,
    db: Database,
    publication_lock: Mutex<()>,
    #[cfg(test)]
    after_copy: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    fail_metadata_commit: std::sync::atomic::AtomicBool,
}

impl ArtifactStore {
    pub fn require_retained(
        &self,
        ledger: &crate::retained_storage::RetainedStorage,
        campaign: &str,
    ) -> Result<()> {
        if self
            .retained
            .as_ref()
            .is_some_and(|(bound, scope)| bound.same_authority(ledger) && scope == campaign)
        {
            Ok(())
        } else {
            Err("artifact store lacks the runtime's retained campaign authority".into())
        }
    }

    /// Standalone store with its original per-store caps. Daemon publication must
    /// use `open_retained` to participate in shared admission. Root must be private.
    pub fn open(root: &Path) -> Result<Self> {
        if !root.is_absolute() {
            return Err("artifact root must be absolute".into());
        }
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(root)
            .map_err(error)?;
        let canonical = root.canonicalize().map_err(error)?;
        if canonical != root {
            return Err("artifact root must be canonical, without symlinks".into());
        }
        let metadata = fs::metadata(root).map_err(error)?;
        if metadata.uid() != nix::unistd::Uid::effective().as_raw() || metadata.mode() & 0o077 != 0
        {
            return Err("artifact root must be owned by the daemon user with mode 0700".into());
        }
        for name in ["staging", "objects"] {
            let path = root.join(name);
            fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .or_else(|e| {
                    if e.kind() == std::io::ErrorKind::AlreadyExists {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })
                .map_err(error)?;
            if fs::symlink_metadata(&path)
                .map_err(error)?
                .file_type()
                .is_symlink()
            {
                return Err("managed directory is a symlink".into());
            }
        }
        let db = Database::create(root.join("manifest.redb")).map_err(error)?;
        let tx = db.begin_write().map_err(error)?;
        {
            tx.open_table(MANIFEST).map_err(error)?;
        }
        tx.commit().map_err(error)?;
        // Persist newly created directory entries as well as the manifest entry.
        for directory in root.ancestors() {
            File::open(directory)
                .map_err(error)?
                .sync_all()
                .map_err(error)?;
        }
        let store = Self {
            retained: None,
            root: canonical,
            db,
            publication_lock: Mutex::new(()),
            #[cfg(test)]
            after_copy: Mutex::new(None),
            #[cfg(test)]
            fail_metadata_commit: std::sync::atomic::AtomicBool::new(false),
        };
        store.reconcile()?;
        Ok(store)
    }

    /// Bind host-owned campaign authority and adopt existing metadata before writes.
    pub fn open_retained(
        root: &Path,
        retained: crate::retained_storage::RetainedStorage,
        campaign: &str,
    ) -> Result<Self> {
        let mut store = Self::open(root)?;
        let tx = store.db.begin_read().map_err(error)?;
        let table = tx.open_table(MANIFEST).map_err(error)?;
        let mut known = std::collections::BTreeSet::new();
        for row in table.iter().map_err(error)? {
            let (key, value) = row.map_err(error)?;
            let mut record: ArtifactRegistration =
                serde_json::from_str(value.value()).map_err(error)?;
            let ready = matches!(record.publication, ArtifactPublication::Ready { .. });
            record.publication = ArtifactPublication::Pending;
            let identity = serde_json::to_string(&record).map_err(error)?;
            retained.adopt(
                campaign,
                "artifact",
                key.value(),
                record.size_bytes,
                &identity,
            )?;
            if ready && store.verified_object(key.value(), &record).is_ok() {
                let receipt = retained
                    .get(campaign, "artifact", key.value())?
                    .ok_or("missing artifact reservation")?;
                retained.ready(&receipt, record.size_bytes)?;
            }
            known.insert(key.value().to_owned());
        }
        for dir in ["staging", "objects"] {
            for entry in fs::read_dir(root.join(dir)).map_err(error)? {
                let entry = entry.map_err(error)?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "invalid artifact name")?;
                if !entry.file_type().map_err(error)?.is_file() {
                    return Err("artifact census entry must be a regular file".into());
                }
                if !known.contains(&name) {
                    retained.adopt(
                        campaign,
                        "artifact_orphan",
                        &format!("{dir}/{name}"),
                        entry.metadata().map_err(error)?.len(),
                        "startup census",
                    )?;
                }
            }
        }
        store.retained = Some((retained, campaign.into()));
        Ok(store)
    }

    fn put(&self, key: &str, record: &ArtifactRegistration) -> Result<()> {
        #[cfg(test)]
        if record.publication != ArtifactPublication::Pending
            && self
                .fail_metadata_commit
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err("injected manifest commit failure".into());
        }
        let json = serde_json::to_string(record).map_err(error)?;
        let tx = self.db.begin_write().map_err(error)?;
        {
            let mut table = tx.open_table(MANIFEST).map_err(error)?;
            if table.get(key).map_err(error)?.is_none()
                && table.len().map_err(error)? >= MAX_RECORDS
            {
                return Err("artifact manifest record limit reached".into());
            }
            table.insert(key, json.as_str()).map_err(error)?;
        }
        tx.commit().map_err(error)
    }

    fn get(&self, key: &str) -> Result<Option<ArtifactRegistration>> {
        let tx = self.db.begin_read().map_err(error)?;
        let table = tx.open_table(MANIFEST).map_err(error)?;
        table
            .get(key)
            .map_err(error)?
            .map(|v| serde_json::from_str(v.value()).map_err(error))
            .transpose()
    }

    pub fn metadata(&self, scope: &str, id: &str) -> Result<Option<ArtifactRegistration>> {
        self.get(&key(scope, id))
    }

    /// Host-only bounded copy of an exact Ready version. Never reads the workspace.
    /// The caller owns the new destination in a private staging directory.
    pub fn copy_ready(
        &self,
        scope: &str,
        expected: &ArtifactRegistration,
        target: &Path,
        max_bytes: u64,
    ) -> Result<()> {
        let key = key(scope, &expected.id);
        let record = self.get(&key)?.ok_or("artifact not registered")?;
        if record != *expected
            || record.size_bytes > max_bytes
            || record.publication
                != (ArtifactPublication::Ready {
                    version: record.sha256.clone(),
                })
        {
            return Err("artifact version is not the exact bounded Ready registration".into());
        }
        let mut source = self.verified_object(&key, &record)?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o400)
            .open(target)
            .map_err(error)?;
        let mut hash = Sha256::new();
        let mut total = 0;
        let mut buffer = [0u8; 65536];
        loop {
            let n = source.read(&mut buffer).map_err(error)?;
            if n == 0 {
                break;
            }
            total += n as u64;
            if total > record.size_bytes {
                return Err("snapshot grew during copy".into());
            }
            hash.update(&buffer[..n]);
            output.write_all(&buffer[..n]).map_err(error)?;
        }
        if total != record.size_bytes || format!("{:x}", hash.finalize()) != record.sha256 {
            return Err("snapshot copy digest mismatch".into());
        }
        Ok(())
    }

    /// Bounded, scoped pagination, no arbitrary filesystem paths or DB handles.
    pub fn list(
        &self,
        scope: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ArtifactRegistration>> {
        if limit == 0 || limit > 100 {
            return Err("list limit must be 1..100".into());
        }
        let prefix = format!("{}-", digest(scope.as_bytes()));
        let start = after_id
            .map(|id| format!("{}!", key(scope, id)))
            .unwrap_or(prefix.clone());
        let end = format!("{prefix}~");
        let tx = self.db.begin_read().map_err(error)?;
        let table = tx.open_table(MANIFEST).map_err(error)?;
        table
            .range(start.as_str()..end.as_str())
            .map_err(error)?
            .take(limit)
            .map(|row| serde_json::from_str(row.map_err(error)?.1.value()).map_err(error))
            .collect()
    }

    pub fn read(&self, scope: &str, id: &str, offset: u64, limit: usize) -> Result<Vec<u8>> {
        if limit == 0 || limit > MAX_READ_BYTES {
            return Err("read limit must be 1..1048576".into());
        }
        let key = key(scope, id);
        let record = self.get(&key)?.ok_or("artifact not registered")?;
        if !matches!(record.publication, ArtifactPublication::Ready { .. }) {
            return Err("artifact is not ready".into());
        }
        let mut file = self.verified_object(&key, &record)?;
        file.seek(SeekFrom::Start(offset)).map_err(error)?;
        let mut bytes = Vec::new();
        file.take(limit as u64)
            .read_to_end(&mut bytes)
            .map_err(error)?;
        Ok(bytes)
    }

    pub fn register(
        &self,
        scope: &str,
        workspace: &Path,
        record: ArtifactRegistration,
    ) -> Result<ArtifactRegistration> {
        self.register_checked(scope, workspace, record, || true)
    }

    fn register_checked(
        &self,
        scope: &str,
        workspace: &Path,
        mut record: ArtifactRegistration,
        permitted: impl Fn() -> bool,
    ) -> Result<ArtifactRegistration> {
        let _guard = self.publication_lock.lock().map_err(error)?;
        if record.id.is_empty()
            || record.id.len() > 256
            || record.path.len() > 4096
            || scope.is_empty()
            || scope.len() > 256
            || record.description.is_empty()
            || record.description.len() > 4096
            || record.kind.len() > 64
            || [&record.task_id, &record.work_id, &record.attempt_id]
                .iter()
                .any(|v| v.as_ref().is_some_and(|s| s.len() > 256))
            || record.size_bytes > MAX_SNAPSHOT_BYTES
            || record.sha256.len() != 64
            || !record
                .sha256
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err("invalid or oversized artifact registration".into());
        }
        record.publication = ArtifactPublication::Pending;
        let key = key(scope, &record.id);
        if let Some(existing) = self.get(&key)? {
            let mut request = existing.clone();
            request.publication = ArtifactPublication::Pending;
            if request != record {
                return Err("registration ID conflicts with existing manifest".into());
            }
            if matches!(existing.publication, ArtifactPublication::Ready { .. }) {
                self.verified_object(&key, &existing)?;
            }
            return Ok(existing);
        }
        let workspace = workspace.canonicalize().map_err(error)?;
        if self.root.starts_with(&workspace) || workspace.starts_with(&self.root) {
            return Err("artifact storage and workspace must be disjoint".into());
        }
        // Include retained staging/orphans in the finite physical byte budget.
        let mut used = 0u64;
        let mut entries = 0usize;
        for dir in ["staging", "objects"] {
            for entry in fs::read_dir(self.root.join(dir)).map_err(error)? {
                entries += 1;
                if entries >= 20_000 {
                    return Err("artifact store file limit reached".into());
                }
                used = used
                    .checked_add(entry.map_err(error)?.metadata().map_err(error)?.len())
                    .ok_or("storage size overflow")?;
            }
        }
        if used.saturating_add(record.size_bytes.saturating_mul(2)) > MAX_STORE_BYTES {
            return Err(
                "artifact store byte limit reached; explicit host retention action required".into(),
            );
        }
        let mut source = open_source(&workspace, &record.path)?;
        let before = source.metadata().map_err(error)?;
        if !before.is_file() || before.len() != record.size_bytes {
            return Err("stale or non-regular artifact source".into());
        }
        let receipt = self
            .retained
            .as_ref()
            .map(|(ledger, campaign)| {
                ledger.reserve(
                    campaign,
                    "artifact",
                    &key,
                    record.size_bytes,
                    &serde_json::to_string(&record).map_err(error)?,
                )
            })
            .transpose()?;
        if !permitted() || receipt.as_ref().is_some_and(|r| r.ready.is_some()) {
            return Err("artifact publication expired or already reconciled".into());
        }
        self.put(&key, &record)?;
        let result = (|| {
            let staging = self.root.join("staging").join(&key);
            let mut target = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&staging)
                .map_err(error)?;
            let mut hasher = Sha256::new();
            let mut total = 0u64;
            let mut buffer = [0u8; 65536];
            loop {
                let n = source.read(&mut buffer).map_err(error)?;
                if n == 0 {
                    break;
                }
                total += n as u64;
                if total > record.size_bytes {
                    return Err("artifact grew during snapshot".into());
                }
                hasher.update(&buffer[..n]);
                target.write_all(&buffer[..n]).map_err(error)?;
            }
            #[cfg(test)]
            if let Some(hook) = self.after_copy.lock().unwrap().take() {
                hook();
            }
            let after = source.metadata().map_err(error)?;
            let current = open_source(&workspace, &record.path)?
                .metadata()
                .map_err(error)?;
            if total != record.size_bytes
                || format!("{:x}", hasher.finalize()) != record.sha256
                || stamp(&before) != stamp(&after)
                || stamp(&before) != stamp(&current)
            {
                return Err("artifact source changed or checksum mismatch".into());
            }
            target
                .set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o400))
                .map_err(error)?;
            target.sync_all().map_err(error)?;
            // No overwrite, and no hard link to mutable workspace bytes.
            fs::hard_link(&staging, self.root.join("objects").join(&key)).map_err(error)?;
            File::open(self.root.join("objects"))
                .map_err(error)?
                .sync_all()
                .map_err(error)?;
            record.publication = ArtifactPublication::Ready {
                version: record.sha256.clone(),
            };
            self.put(&key, &record)?;
            if let (Some((ledger, _)), Some(receipt)) = (&self.retained, &receipt) {
                ledger.ready(receipt, total)?;
            }
            // Staging is intentionally retained, including on failure, for explicit recovery/retention.
            Ok(())
        })();
        if let Err(reason) = result {
            record.publication = ArtifactPublication::Failed { reason };
            self.put(&key, &record)?;
        }
        Ok(record)
    }

    fn verified_object(&self, key: &str, record: &ArtifactRegistration) -> Result<File> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
            .open(self.root.join("objects").join(key))
            .map_err(error)?;
        let meta = file.metadata().map_err(error)?;
        if !meta.is_file() || meta.len() != record.size_bytes || meta.len() > MAX_SNAPSHOT_BYTES {
            return Err("invalid stored artifact size/type".into());
        }
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 65536];
        let mut total = 0u64;
        loop {
            let n = file.read(&mut buffer).map_err(error)?;
            if n == 0 {
                break;
            }
            total += n as u64;
            if total > record.size_bytes {
                return Err("stored artifact grew".into());
            }
            hasher.update(&buffer[..n]);
        }
        if total != record.size_bytes || format!("{:x}", hasher.finalize()) != record.sha256 {
            return Err("stored artifact checksum mismatch".into());
        }
        file.rewind().map_err(error)?;
        Ok(file)
    }

    fn reconcile(&self) -> Result<()> {
        let tx = self.db.begin_read().map_err(error)?;
        let table = tx.open_table(MANIFEST).map_err(error)?;
        if table.len().map_err(error)? > MAX_RECORDS {
            return Err("artifact manifest record limit exceeded; host retention required".into());
        }
        for row in table.iter().map_err(error)? {
            let (key, value) = row.map_err(error)?;
            let mut record: ArtifactRegistration =
                serde_json::from_str(value.value()).map_err(error)?;
            if record.publication == ArtifactPublication::Pending {
                record.publication = match self.verified_object(key.value(), &record) {
                    Ok(_) => ArtifactPublication::Ready {
                        version: record.sha256.clone(),
                    },
                    Err(e) => ArtifactPublication::Failed {
                        reason: format!("incomplete publication recovered: {e}"),
                    },
                };
                self.put(key.value(), &record)?;
            }
        }
        // Unknown objects and staging files are never promoted or automatically deleted.
        Ok(())
    }
}

fn stamp(m: &fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}

/// Publish only explicit registrations from collected private-launch evidence.
/// A cancelled waiter cannot promote candidate references; blocking copies retain
/// their semaphore permit until finished and remain byte/count bounded.
pub async fn publish_candidate(
    store: std::sync::Arc<ArtifactStore>,
    workspace: PathBuf,
    attempt: String,
    worker: String,
    mut candidate: tachyon_api::types::WorkResult,
    events: Vec<EventEnvelope>,
    deadline: tokio::time::Instant,
) -> tachyon_api::types::WorkResult {
    use tachyon_api::types::{ArtifactPublication, WorkOutcome};
    static SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);
    candidate.candidate_refs = None;
    let fallback = candidate.clone();
    let Ok(Ok(slot)) = tokio::time::timeout_at(deadline, SLOTS.acquire()).await else {
        return fallback;
    };
    let (cancelled, waiter) = tokio::sync::oneshot::channel::<()>();
    let publication = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        if serde_json::to_vec(&events)
            .map_or(true, |bytes| bytes.len() > tachyon_model::broker::MAX_FRAME)
        {
            return candidate;
        }
        let WorkOutcome::Completed {
            artifacts: paths, ..
        } = &candidate.outcome
        else {
            return candidate;
        };
        let mut registrations = std::collections::BTreeMap::new();
        for event in events {
            if event.session_id != worker
                || event.task_id.as_deref() != Some(&worker)
                || !matches!(&event.actor, tachyon_api::types::Actor::Worker { id } if id == &worker)
                || event.conversation_id.is_some()
                || event.parent_task_id.is_some()
            {
                return candidate;
            }
            let AgentEvent::ArtifactRegistered { mut artifact } = event.kind else {
                continue;
            };
            artifact.publication = ArtifactPublication::Pending;
            // No normalization aliases, scope repair, or worker-selected Ready metadata.
            if artifact.work_id.as_deref() != Some(&candidate.work_id)
                || artifact.generation != Some(candidate.generation)
                || artifact.assignment != Some(candidate.assignment)
                || artifact
                    .attempt_id
                    .as_ref()
                    .is_some_and(|id| id != &attempt)
                || artifact.task_id.as_ref().is_some_and(|id| id != &worker)
                || artifact.path.is_empty()
                || artifact
                    .path
                    .split('/')
                    .any(|p| p.is_empty() || p == "." || p == "..")
            {
                return candidate;
            }
            artifact.attempt_id = Some(attempt.clone());
            artifact.task_id = Some(worker.clone());
            if let Some(previous) = registrations.insert(artifact.id.clone(), artifact.clone()) {
                if previous != artifact {
                    return candidate;
                }
            }
        }
        if registrations.len() > 32
            || registrations
                .values()
                .map(|a| u128::from(a.size_bytes))
                .sum::<u128>()
                > 16 * 1024 * 1024
        {
            return candidate;
        }
        let mut ready = std::collections::BTreeMap::new();
        for registration in registrations.into_values() {
            if tokio::time::Instant::now() >= deadline {
                return candidate;
            }
            let Ok(artifact) =
                store.register_checked(&candidate.work_id, &workspace, registration, || {
                    tokio::time::Instant::now() < deadline && !cancelled.is_closed()
                })
            else {
                return candidate;
            };
            if !matches!(artifact.publication, ArtifactPublication::Ready { .. })
                || ready.insert(artifact.path, artifact.id).is_some()
            {
                return candidate;
            }
        }
        candidate.candidate_refs = paths.iter().map(|path| ready.get(path).cloned()).collect();
        candidate
    });
    let result = match tokio::time::timeout_at(deadline, publication).await {
        Ok(Ok(candidate)) => candidate,
        _ => fallback,
    };
    drop(waiter);
    result
}

/// Walk with directory descriptors and O_NOFOLLOW, not check-then-open paths.
fn open_source(workspace: &Path, requested: &str) -> Result<File> {
    let parts: Vec<_> = Path::new(requested).components().collect();
    if parts.is_empty() || parts.iter().any(|p| !matches!(p, Component::Normal(_))) {
        return Err("artifact path must be relative without traversal".into());
    }
    let mut dir = File::open("/").map_err(error)?;
    for part in workspace.components() {
        if matches!(part, Component::RootDir) {
            continue;
        }
        if !matches!(part, Component::Normal(_)) {
            return Err("workspace must be canonical and absolute".into());
        }
        let fd = openat(
            &dir,
            Path::new(part.as_os_str()),
            OFlag::RDONLY | OFlag::DIRECTORY | OFlag::NOFOLLOW | OFlag::CLOEXEC,
            Mode::empty(),
        )
        .map_err(error)?;
        dir = File::from(fd);
    }
    for (index, part) in parts.iter().enumerate() {
        let mut flags = OFlag::RDONLY | OFlag::NOFOLLOW | OFlag::CLOEXEC | OFlag::NONBLOCK;
        if index + 1 != parts.len() {
            flags |= OFlag::DIRECTORY;
        }
        let fd: OwnedFd =
            openat(&dir, Path::new(part.as_os_str()), flags, Mode::empty()).map_err(error)?;
        dir = File::from(fd);
    }
    Ok(dir)
}

/// Blocking host publication. Call only on the publication worker, never an event owner.
pub fn publish_event(
    data: &str,
    info: &AgentInfo,
    generation: u64,
    assignment: u64,
) -> Option<String> {
    let mut envelope: EventEnvelope = serde_json::from_str(data).ok()?;
    let AgentEvent::ArtifactRegistered { artifact } = &mut envelope.kind else {
        return None;
    };
    let result = (|| {
        if artifact.generation.is_some_and(|v| v != generation)
            || artifact.assignment.is_some_and(|v| v != assignment)
        {
            return Err("stale artifact assignment".into());
        }
        artifact.task_id = info.logical_task_id.clone();
        artifact.work_id = info.logical_task_id.clone();
        artifact.generation = Some(generation);
        artifact.assignment = Some(assignment);
        let store = managed_store()?;
        let scope = info.logical_task_id.as_deref().unwrap_or(&info.id);
        store.register(scope, Path::new(&info.workspace), artifact.clone())
    })();
    match result {
        Ok(ready) => *artifact = ready,
        Err(reason) => artifact.publication = ArtifactPublication::Failed { reason },
    }
    serde_json::to_string(&envelope).ok()
}

static STORE: OnceLock<Result<ArtifactStore>> = OnceLock::new();

pub fn initialize_retained(retained: crate::retained_storage::RetainedStorage) -> Result<()> {
    let root = std::env::var_os("TACHYON_ARTIFACT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| tachyon_util::daemon::databases_dir().join("artifacts"));
    STORE
        .set(ArtifactStore::open_retained(
            &root,
            retained,
            "host-artifacts",
        ))
        .map_err(|_| "artifact store already initialized".to_string())?;
    managed_store().map(|_| ())
}

fn managed_store() -> Result<&'static ArtifactStore> {
    STORE
        .get()
        .ok_or("host retained artifact storage not initialized")?
        .as_ref()
        .map_err(Clone::clone)
}

pub fn query(request: &ApiRequest) -> Result<ApiResponse> {
    let store = managed_store()?;
    match request {
        ApiRequest::ArtifactGet { scope, id } => Ok(ApiResponse::Artifact {
            artifact: store.metadata(scope, id)?,
        }),
        ApiRequest::ArtifactRead {
            scope,
            id,
            offset,
            limit,
        } => Ok(ApiResponse::ArtifactBytes {
            bytes: store.read(scope, id, *offset, *limit as usize)?,
        }),
        ApiRequest::ArtifactList {
            scope,
            after,
            limit,
        } => Ok(ApiResponse::ArtifactList {
            artifacts: store.list(scope, after.as_deref(), *limit as usize)?,
        }),
        _ => Err("not an artifact query".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn retained_mutation_and_metadata_failure_never_free_reservations() {
        for metadata_failure in [false, true] {
            let root = tempdir().unwrap();
            let database = Arc::new(Database::create(root.path().join("runtime.redb")).unwrap());
            let ledger = crate::retained_storage::RetainedStorage::new(database, 5).unwrap();
            let workspace = tempdir().unwrap();
            let path = workspace.path().join("report.txt");
            fs::write(&path, "abc").unwrap();
            let store = ArtifactStore::open_retained(
                &root.path().join("artifacts"),
                ledger.clone(),
                "campaign",
            )
            .unwrap();
            if metadata_failure {
                store
                    .fail_metadata_commit
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            } else {
                *store.after_copy.lock().unwrap() = Some(Box::new(move || {
                    fs::write(path, "bad").unwrap();
                }));
            }
            let result = store.register("work", workspace.path(), registration());
            assert!(
                result.is_err()
                    || matches!(
                        result.unwrap().publication,
                        ArtifactPublication::Failed { .. }
                    )
            );
            let receipt = ledger
                .get("campaign", "artifact", &key("work", "artifact-1"))
                .unwrap()
                .unwrap();
            assert_eq!(receipt.ready, None);
            assert!(ledger
                .reserve("campaign", "trace", "next", 3, "hash")
                .is_err());
        }
    }

    #[test]
    fn retained_artifact_census_and_registration_replay_charge_once() {
        let root = tempdir().unwrap();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let artifact_root = root.path().join("artifacts");
        let store = ArtifactStore::open(&artifact_root).unwrap();
        store
            .register("work", workspace.path(), registration())
            .unwrap();
        drop(store);
        let ledger = crate::retained_storage::RetainedStorage::new(
            Arc::new(Database::create(root.path().join("runtime.redb")).unwrap()),
            5,
        )
        .unwrap();
        for _ in 0..2 {
            let store =
                ArtifactStore::open_retained(&artifact_root, ledger.clone(), "campaign").unwrap();
            store
                .register("work", workspace.path(), registration())
                .unwrap();
            assert_eq!(ledger.summary("campaign").unwrap()["root_charged_bytes"], 3);
        }
        assert!(ledger
            .reserve("campaign", "trace", "next", 3, "hash")
            .is_err());
    }

    fn storage_dir() -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        fs::set_permissions(
            dir.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        dir
    }

    fn registration() -> ArtifactRegistration {
        ArtifactRegistration {
            id: "artifact-1".into(),
            path: "report.txt".into(),
            kind: "report".into(),
            description: "test report".into(),
            size_bytes: 3,
            sha256: digest(b"abc"),
            task_id: None,
            work_id: None,
            generation: None,
            assignment: None,
            attempt_id: None,
            publication: ArtifactPublication::Pending,
        }
    }

    #[test]
    fn retained_cancelled_reservation_does_not_write_and_cross_campaigns_charge_separately() {
        let root = tempdir().unwrap();
        let database = Arc::new(Database::create(root.path().join("runtime.redb")).unwrap());
        let ledger = crate::retained_storage::RetainedStorage::new(database.clone(), 9).unwrap();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), b"abc").unwrap();
        let store = ArtifactStore::open_retained(
            &root.path().join("cancelled"),
            ledger.clone(),
            "cancelled",
        )
        .unwrap();
        let writer = database.begin_write().unwrap();
        let path = workspace.path().to_owned();
        let task = std::thread::spawn(move || {
            assert!(store
                .register_checked("work", &path, registration(), || false)
                .is_err());
            assert!(store.metadata("work", "artifact-1").unwrap().is_none());
            assert_eq!(fs::read_dir(store.root.join("staging")).unwrap().count(), 0);
        });
        drop(writer);
        task.join().unwrap();
        for campaign in ["a", "b"] {
            let store =
                ArtifactStore::open_retained(&root.path().join(campaign), ledger.clone(), campaign)
                    .unwrap();
            for _ in 0..2 {
                assert!(matches!(
                    store
                        .register("work", workspace.path(), registration())
                        .unwrap()
                        .publication,
                    ArtifactPublication::Ready { .. }
                ));
            }
            assert_eq!(
                ledger.summary(campaign).unwrap()["campaign_charged_bytes"],
                3
            );
        }
        assert_eq!(
            ledger.summary("cancelled").unwrap()["root_charged_bytes"],
            9
        );
        assert_eq!(
            ledger.summary("cancelled").unwrap()["campaign_unresolved_bytes"],
            3
        );
    }

    #[test]
    fn retained_census_missing_ready_object_stays_unresolved_and_symlinks_fail() {
        let root = tempdir().unwrap();
        let ledger = crate::retained_storage::RetainedStorage::new(
            Arc::new(Database::create(root.path().join("runtime.redb")).unwrap()),
            9,
        )
        .unwrap();
        let path = root.path().join("artifacts");
        let store = ArtifactStore::open(&path).unwrap();
        assert!(store.require_retained(&ledger, "campaign").is_err());
        let mut record = registration();
        record.publication = ArtifactPublication::Ready {
            version: record.sha256.clone(),
        };
        store.put(&key("work", &record.id), &record).unwrap();
        drop(store);
        drop(ArtifactStore::open_retained(&path, ledger.clone(), "campaign").unwrap());
        let bound = ArtifactStore::open_retained(&path, ledger.clone(), "campaign").unwrap();
        bound.require_retained(&ledger, "campaign").unwrap();
        assert!(bound.require_retained(&ledger, "other").is_err());
        let other = crate::retained_storage::RetainedStorage::new(
            Arc::new(Database::create(root.path().join("other.redb")).unwrap()),
            9,
        )
        .unwrap();
        assert!(bound.require_retained(&other, "campaign").is_err());
        drop(bound);
        assert_eq!(
            ledger.summary("campaign").unwrap()["campaign_unresolved_bytes"],
            3
        );
        std::os::unix::fs::symlink(root.path().join("missing"), path.join("objects/orphan"))
            .unwrap();
        assert!(ArtifactStore::open_retained(&path, ledger, "campaign").is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collected_publication_revalidates_claims_and_resolves_only_exact_paths() {
        for case in [
            "pending",
            "ready",
            "duplicate",
            "conflict",
            "missing",
            "hash",
            "size",
            "generation",
            "assignment",
            "work",
            "attempt",
            "worker",
            "path",
            "alias",
            "symlink",
            "multiple",
            "budget",
        ] {
            let storage = storage_dir();
            let workspace = tempdir().unwrap();
            fs::write(workspace.path().join("report.txt"), "abc").unwrap();
            std::os::unix::fs::symlink("report.txt", workspace.path().join("link")).unwrap();
            let store = Arc::new(ArtifactStore::open(storage.path()).unwrap());
            let mut artifact = registration();
            artifact.work_id = Some("work".into());
            artifact.generation = Some(7);
            artifact.assignment = Some(3);
            let mut candidate: tachyon_api::types::WorkResult = serde_json::from_value(serde_json::json!({
                "work_id":"work", "objective":"bounded", "generation":7, "assignment":3,
                "outcome":"completed", "result":"ok", "artifacts":["report.txt"], "candidate_refs":["forged"]
            })).unwrap();
            match case {
                "ready" | "duplicate" => {
                    artifact.publication = ArtifactPublication::Ready {
                        version: "forged".into(),
                    }
                }
                "hash" => artifact.sha256 = digest(b"bad"),
                "size" => artifact.size_bytes = 2,
                "generation" => artifact.generation = Some(8),
                "assignment" => artifact.assignment = Some(4),
                "work" => artifact.work_id = Some("other".into()),
                "attempt" => artifact.attempt_id = Some("other".into()),
                "path" => artifact.path = "../report.txt".into(),
                "alias" => artifact.path = "./report.txt".into(),
                "symlink" => artifact.path = "link".into(),
                "budget" => artifact.size_bytes = 16 * 1024 * 1024 + 1,
                "multiple" => {
                    if let tachyon_api::types::WorkOutcome::Completed { artifacts, .. } =
                        &mut candidate.outcome
                    {
                        artifacts.push("unregistered".into());
                    }
                }
                _ => {}
            }
            let event = |artifact| {
                serde_json::from_value::<EventEnvelope>(serde_json::json!({
                "event_id":1, "sequence":1, "occurred_at_ms":0, "session_id":"worker", "task_id":"worker",
                "actor":{"kind":"worker", "id":"worker"}, "kind":"artifact_registered", "artifact":artifact
            })).unwrap()
            };
            let mut events = vec![event(artifact.clone())];
            match case {
                "missing" => events.clear(),
                "worker" => events[0].session_id = "other".into(),
                "duplicate" => {
                    artifact.publication = ArtifactPublication::Pending;
                    events.push(event(artifact));
                }
                "conflict" => {
                    artifact.sha256 = digest(b"bad");
                    events.push(event(artifact));
                }
                _ => {}
            }
            let candidate = publish_candidate(
                store.clone(),
                workspace.path().into(),
                "attempt".into(),
                "worker".into(),
                candidate,
                events,
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await;
            if ["pending", "ready", "duplicate"].contains(&case) {
                assert_eq!(
                    candidate.candidate_refs,
                    Some(vec!["artifact-1".into()]),
                    "{case}"
                );
                fs::write(workspace.path().join("report.txt"), "bad").unwrap();
                assert_eq!(store.read("work", "artifact-1", 0, 10).unwrap(), b"abc");
                assert_eq!(store.list("work", None, 100).unwrap().len(), 1);
            } else {
                assert_eq!(candidate.candidate_refs, None, "{case}");
            }
            if ["missing", "path", "alias", "budget", "conflict"].contains(&case) {
                assert!(store.list("work", None, 100).unwrap().is_empty(), "{case}");
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn publication_deadline_keeps_runtime_live_and_never_installs_late_refs() {
        let storage = storage_dir();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let store = Arc::new(ArtifactStore::open(storage.path()).unwrap());
        let (entered, wait_entered) = tokio::sync::oneshot::channel();
        let (release, wait_release) = std::sync::mpsc::channel();
        *store.after_copy.lock().unwrap() = Some(Box::new(move || {
            entered.send(()).unwrap();
            wait_release
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        }));
        let mut artifact = registration();
        artifact.work_id = Some("work".into());
        artifact.generation = Some(1);
        artifact.assignment = Some(1);
        let event = serde_json::from_value(serde_json::json!({
            "event_id":1, "sequence":1, "occurred_at_ms":0, "session_id":"worker", "task_id":"worker",
            "actor":{"kind":"worker", "id":"worker"}, "kind":"artifact_registered", "artifact":artifact
        })).unwrap();
        let candidate = serde_json::from_value(serde_json::json!({
            "work_id":"work", "objective":"bounded", "generation":1, "assignment":1,
            "outcome":"completed", "result":"ok", "artifacts":["report.txt"]
        }))
        .unwrap();
        let publication = tokio::spawn(publish_candidate(
            store.clone(),
            workspace.path().into(),
            "attempt".into(),
            "worker".into(),
            candidate,
            vec![event],
            tokio::time::Instant::now() + std::time::Duration::from_millis(500),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(2), wait_entered)
            .await
            .unwrap()
            .unwrap();
        // The single event thread can poll the deadline while the copy is blocked.
        let result = publication.await.unwrap();
        assert!(result.candidate_refs.is_none());
        release.send(()).unwrap();
        let finished = store.clone();
        tokio::task::spawn_blocking(move || {
            for _ in 0..200 {
                if finished
                    .metadata("work", "artifact-1")
                    .unwrap()
                    .is_some_and(|a| matches!(a.publication, ArtifactPublication::Ready { .. }))
                {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            panic!("publication did not finish after release");
        })
        .await
        .unwrap();
        assert!(result.candidate_refs.is_none());
    }

    #[test]
    fn ready_ack_preserves_version_after_workspace_mutation_deletion_and_reopen() {
        let storage = storage_dir();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let store = ArtifactStore::open(storage.path()).unwrap();
        let ack = store
            .register("work", workspace.path(), registration())
            .unwrap();
        assert_eq!(
            ack.publication,
            ArtifactPublication::Ready {
                version: digest(b"abc")
            }
        );
        assert_eq!(store.metadata("work", &ack.id).unwrap(), Some(ack.clone()));
        let deleted_workspace = workspace.path().to_owned();
        fs::write(workspace.path().join("report.txt"), "new bytes").unwrap();
        workspace.close().unwrap();
        drop(store);
        let store = ArtifactStore::open(storage.path()).unwrap();
        assert_eq!(
            store
                .register("work", &deleted_workspace, registration())
                .unwrap(),
            ack
        );
        assert_eq!(store.read("work", "artifact-1", 0, 10).unwrap(), b"abc");
        assert_eq!(store.read("work", "artifact-1", 1, 1).unwrap(), b"b");
        assert!(store.read("other", "artifact-1", 0, 10).is_err());
        assert!(store
            .read("work", "artifact-1", 0, MAX_READ_BYTES + 1)
            .is_err());
        assert_eq!(store.list("work", None, 1).unwrap().len(), 1);
        assert!(store
            .list("work", Some("artifact-1"), 1)
            .unwrap()
            .is_empty());
        assert!(store.list("other", None, 1).unwrap().is_empty());
    }

    #[test]
    fn concurrent_duplicates_are_idempotent_and_conflicts_fail() {
        let storage = storage_dir();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let store = Arc::new(ArtifactStore::open(storage.path()).unwrap());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                let root = workspace.path().to_owned();
                std::thread::spawn(move || store.register("work", &root, registration()).unwrap())
            })
            .collect();
        let records: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(records.iter().all(|r| r == &records[0]));
        assert_eq!(store.list("work", None, 100).unwrap().len(), 1);
        let mut conflict = registration();
        conflict.description = "different request".into();
        assert!(store.register("work", workspace.path(), conflict).is_err());
    }

    #[test]
    fn checksum_mismatch_fails_closed_and_cannot_be_read() {
        let storage = storage_dir();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "bad").unwrap();
        let store = ArtifactStore::open(storage.path()).unwrap();
        let ack = store
            .register("work", workspace.path(), registration())
            .unwrap();
        assert!(matches!(
            ack.publication,
            ArtifactPublication::Failed { .. }
        ));
        assert!(store.read("work", "artifact-1", 0, 10).is_err());
        assert!(!storage
            .path()
            .join("objects")
            .join(key("work", "artifact-1"))
            .exists());
    }

    #[test]
    fn source_alteration_during_copy_never_emits_ready_metadata() {
        for replace in [false, true] {
            let storage = storage_dir();
            let workspace = tempdir().unwrap();
            let path = workspace.path().join("report.txt");
            fs::write(&path, "abc").unwrap();
            let store = ArtifactStore::open(storage.path()).unwrap();
            *store.after_copy.lock().unwrap() = Some(Box::new(move || {
                if replace {
                    fs::remove_file(&path).unwrap();
                }
                fs::write(path, "xyz").unwrap();
            }));
            let ack = store
                .register("work", workspace.path(), registration())
                .unwrap();
            assert!(matches!(
                ack.publication,
                ArtifactPublication::Failed { .. }
            ));
            assert_eq!(store.metadata("work", &ack.id).unwrap(), Some(ack));
            assert!(store.read("work", "artifact-1", 0, 10).is_err());
            assert!(!storage
                .path()
                .join("objects")
                .join(key("work", "artifact-1"))
                .exists());
        }
    }

    #[test]
    fn pending_metadata_is_visible_until_copy_and_ready_commit_finish() {
        let storage = storage_dir();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let store = Arc::new(ArtifactStore::open(storage.path()).unwrap());
        let (copied, wait_copy) = std::sync::mpsc::channel();
        let (release, wait_release) = std::sync::mpsc::channel();
        *store.after_copy.lock().unwrap() = Some(Box::new(move || {
            copied.send(()).unwrap();
            wait_release.recv().unwrap();
        }));
        let publisher = store.clone();
        let path = workspace.path().to_owned();
        let thread =
            std::thread::spawn(move || publisher.register("work", &path, registration()).unwrap());
        wait_copy
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            store
                .metadata("work", "artifact-1")
                .unwrap()
                .unwrap()
                .publication,
            ArtifactPublication::Pending
        );
        assert!(store.read("work", "artifact-1", 0, 10).is_err());
        release.send(()).unwrap();
        let ready = thread.join().unwrap();
        assert!(matches!(
            ready.publication,
            ArtifactPublication::Ready { .. }
        ));
        assert_eq!(store.metadata("work", "artifact-1").unwrap(), Some(ready));
        assert_eq!(store.read("work", "artifact-1", 0, 10).unwrap(), b"abc");
    }

    #[test]
    fn metadata_commit_failure_cannot_acknowledge_ready() {
        let storage = storage_dir();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let store = ArtifactStore::open(storage.path()).unwrap();
        store
            .fail_metadata_commit
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(store
            .register("work", workspace.path(), registration())
            .is_err());
        assert_eq!(
            store
                .metadata("work", "artifact-1")
                .unwrap()
                .unwrap()
                .publication,
            ArtifactPublication::Pending
        );
        assert!(store.read("work", "artifact-1", 0, 10).is_err());
        drop(store);
        let store = ArtifactStore::open(storage.path()).unwrap();
        assert!(matches!(
            store
                .metadata("work", "artifact-1")
                .unwrap()
                .unwrap()
                .publication,
            ArtifactPublication::Ready { .. }
        ));
        assert_eq!(store.read("work", "artifact-1", 0, 10).unwrap(), b"abc");
    }

    #[test]
    fn recovery_does_not_promote_an_object_without_a_recorded_hash() {
        let storage = storage_dir();
        let store = ArtifactStore::open(storage.path()).unwrap();
        let mut record = registration();
        record.sha256.clear();
        let key = key("work", &record.id);
        store.put(&key, &record).unwrap();
        fs::write(storage.path().join("objects").join(&key), "abc").unwrap();
        drop(store);
        let store = ArtifactStore::open(storage.path()).unwrap();
        assert!(matches!(
            store
                .metadata("work", &record.id)
                .unwrap()
                .unwrap()
                .publication,
            ArtifactPublication::Failed { .. }
        ));
        assert!(store.read("work", &record.id, 0, 10).is_err());
    }

    #[test]
    fn manifest_quota_counts_failed_records_without_files() {
        let storage = storage_dir();
        let store = ArtifactStore::open(storage.path()).unwrap();
        let mut record = registration();
        record.publication = ArtifactPublication::Failed {
            reason: "test failure".into(),
        };
        let json = serde_json::to_string(&record).unwrap();
        let tx = store.db.begin_write().unwrap();
        {
            let mut table = tx.open_table(MANIFEST).unwrap();
            for id in 0..MAX_RECORDS {
                table
                    .insert(id.to_string().as_str(), json.as_str())
                    .unwrap();
            }
        }
        tx.commit().unwrap();
        assert!(store.put("new", &record).is_err());
        store.put("0", &record).unwrap();
        assert_eq!(
            fs::read_dir(storage.path().join("objects"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn scope_traversal_symlinks_special_files_and_workspace_storage_are_denied() {
        let storage = storage_dir();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let _socket =
            std::os::unix::net::UnixListener::bind(workspace.path().join("socket")).unwrap();
        std::os::unix::fs::symlink("report.txt", workspace.path().join("link")).unwrap();
        std::os::unix::fs::symlink(workspace.path(), workspace.path().join("dirlink")).unwrap();
        let store = ArtifactStore::open(storage.path()).unwrap();
        for path in [
            "../report.txt",
            "/etc/passwd",
            "link",
            "dirlink/report.txt",
            "report.txt/../report.txt",
            ".",
            "socket",
        ] {
            let mut record = registration();
            record.path = path.into();
            assert!(
                store.register("work", workspace.path(), record).is_err(),
                "{path}"
            );
        }
        assert!(store
            .register("work", storage.path(), registration())
            .is_err());
        assert!(store.list("work", None, 100).unwrap().is_empty());
    }

    #[test]
    fn recovery_promotes_only_verified_published_objects_and_retains_orphans() {
        let storage = storage_dir();
        let store = ArtifactStore::open(storage.path()).unwrap();
        for (id, bytes) in [
            ("published", Some("abc")),
            ("corrupt", Some("bad")),
            ("incomplete", None),
        ] {
            let mut record = registration();
            record.id = id.into();
            let key = key("work", id);
            store.put(&key, &record).unwrap();
            if let Some(bytes) = bytes {
                fs::write(storage.path().join("objects").join(&key), bytes).unwrap();
            } else {
                fs::write(storage.path().join("staging").join(&key), "ab").unwrap();
            }
        }
        fs::write(storage.path().join("objects/orphan"), "orphan").unwrap();
        drop(store);
        let store = ArtifactStore::open(storage.path()).unwrap();
        assert!(matches!(
            store
                .metadata("work", "published")
                .unwrap()
                .unwrap()
                .publication,
            ArtifactPublication::Ready { .. }
        ));
        for id in ["corrupt", "incomplete"] {
            assert!(matches!(
                store.metadata("work", id).unwrap().unwrap().publication,
                ArtifactPublication::Failed { .. }
            ));
            assert!(store.read("work", id, 0, 10).is_err());
        }
        assert!(storage.path().join("objects/orphan").exists());
        assert!(storage
            .path()
            .join("staging")
            .join(key("work", "incomplete"))
            .exists());
        assert_eq!(store.list("work", None, 100).unwrap().len(), 3);
    }

    #[test]
    fn corrupt_ready_bytes_are_not_served_and_size_limits_are_finite() {
        let storage = storage_dir();
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let store = ArtifactStore::open(storage.path()).unwrap();
        let mut too_large = registration();
        too_large.size_bytes = MAX_SNAPSHOT_BYTES + 1;
        assert!(store.register("work", workspace.path(), too_large).is_err());
        store
            .register("work", workspace.path(), registration())
            .unwrap();
        let path = storage
            .path()
            .join("objects")
            .join(key("work", "artifact-1"));
        fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
        fs::write(path, "bad").unwrap();
        assert!(store.read("work", "artifact-1", 0, 10).is_err());
    }
}
