use super::*;
use std::sync::{Arc, Barrier};
use tachyon_model::broker::ResourceUpload;

fn begin(store: Arc<RuntimeStore>, handle: &str, size: u64) -> Result<upload::Upload, String> {
    upload::Upload::begin(
        store,
        "campaign",
        "work",
        "attempt",
        1,
        ResourceUpload::Begin {
            handle: handle.into(),
            retained: size,
            total: size,
            storage_failed: false,
            sha256: format!("{:x}", Sha256::digest(b"abc")),
        },
    )
}

fn artifact() -> ArtifactRegistration {
    ArtifactRegistration {
        id: "a".into(),
        path: "a".into(),
        kind: "report".into(),
        description: "test".into(),
        size_bytes: 3,
        sha256: format!("{:x}", Sha256::digest(b"abc")),
        task_id: None,
        work_id: None,
        generation: None,
        assignment: None,
        attempt_id: None,
        publication: ArtifactPublication::Pending,
    }
}

#[test]
fn concurrent_artifact_and_upload_share_root_and_campaign_caps() {
    for root_cap in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        store.retained = tachyond::retained_storage::RetainedStorage::new(
            store.database.clone(),
            if root_cap { 5 } else { 100 },
        )
        .unwrap();
        store
            .retained
            .configure("campaign", Some(if root_cap { 100 } else { 5 }))
            .unwrap();
        let artifacts = ArtifactStore::open_retained(
            &dir.path().join("artifacts"),
            store.retained.clone(),
            "campaign",
        )
        .unwrap();
        let workspace = dir.path().join("work");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::write(workspace.join("a"), b"abc").unwrap();
        let store = Arc::new(store);
        let barrier = Arc::new(Barrier::new(2));
        let b = barrier.clone();
        let a = std::thread::spawn(move || {
            b.wait();
            artifacts
                .register("work", &workspace, artifact())
                .is_ok_and(|record| matches!(record.publication, ArtifactPublication::Ready { .. }))
        });
        barrier.wait();
        let t = if let Ok(mut upload) = begin(
            store.clone(),
            "output:00000000-0000-0000-0000-000000000001",
            3,
        ) {
            upload
                .apply(ResourceUpload::Chunk {
                    offset: 0,
                    bytes: b"abc".to_vec(),
                })
                .unwrap();
            assert!(matches!(
                upload
                    .apply(ResourceUpload::Finish { sha256: None })
                    .unwrap(),
                tachyon_model::broker::UploadReply::Ready { .. }
            ));
            true
        } else {
            false
        };
        assert_ne!(a.join().unwrap(), t);
        let summary = store.retained.summary("campaign").unwrap();
        assert_eq!(summary["root_charged_bytes"], 3);
        assert_eq!(summary["campaign_charged_bytes"], 3);
        assert_eq!(summary["campaign_unresolved_bytes"], 0);
        assert!(begin(store, "output:00000000-0000-0000-0000-000000000002", 3).is_err());
    }
}

#[test]
fn failed_upload_and_duplicate_remain_charged_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("runtime.redb");
    let store = Arc::new(RuntimeStore::open(&path).unwrap());
    store.retained.configure("campaign", Some(5)).unwrap();
    let handle = "output:00000000-0000-0000-0000-000000000001";
    let mut upload = begin(store.clone(), handle, 3).unwrap();
    assert!(begin(store.clone(), handle, 3).is_err());
    upload
        .apply(ResourceUpload::Chunk {
            offset: 0,
            bytes: b"bad".to_vec(),
        })
        .unwrap();
    assert!(upload
        .apply(ResourceUpload::Finish { sha256: None })
        .is_err());
    drop(upload);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&path).unwrap());
    assert!(begin(store, "output:00000000-0000-0000-0000-000000000002", 3).is_err());
}

#[test]
fn legacy_trace_census_is_idempotent_and_adopts_over_cap_debt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("runtime.redb");
    let store = RuntimeStore::open(&path).unwrap();
    store.retained.configure("campaign", Some(5)).unwrap();
    let resource = Resource {
        reference: ResourceRef {
            kind: ResourceKind::Trace,
            work_id: "work".into(),
            id: "old".into(),
            version: "old".into(),
        },
        occurred_at_ms: None,
        data: json!({"size_bytes":7}),
    };
    let tx = store.database.begin_write().unwrap();
    tx.open_table(TRACES)
        .unwrap()
        .insert(
            ("campaign", "old"),
            serde_json::to_vec(&resource).unwrap().as_slice(),
        )
        .unwrap();
    tx.commit().unwrap();
    drop(store);
    let store = RuntimeStore::open(&path).unwrap();
    store.adopt_retained_traces().unwrap();
    assert!(store
        .retained
        .get("campaign", "trace", "old")
        .unwrap()
        .unwrap()
        .ready
        .is_none());
    assert_eq!(
        store
            .retained
            .get("campaign", "trace", "old")
            .unwrap()
            .unwrap()
            .expected,
        7
    );
    assert!(store
        .retained
        .reserve("campaign", "artifact", "new", 1, "new")
        .is_err());
}

#[test]
fn cancelled_upload_reservation_never_creates_an_object() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    store.retained.configure("campaign", Some(3)).unwrap();
    let writer = store.database.begin_write().unwrap();
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let worker = store.clone();
    let permitted = active.clone();
    let task = std::thread::spawn(move || {
        upload::Upload::begin_checked(
            worker,
            "campaign",
            "work",
            "attempt",
            1,
            ResourceUpload::Begin {
                handle: "output:00000000-0000-0000-0000-000000000001".into(),
                retained: 3,
                total: 3,
                storage_failed: false,
                sha256: format!("{:x}", Sha256::digest(b"abc")),
            },
            || permitted.load(std::sync::atomic::Ordering::Acquire),
        )
        .is_err()
    });
    active.store(false, std::sync::atomic::Ordering::Release);
    drop(writer);
    assert!(task.join().unwrap());
    assert_eq!(std::fs::read_dir(&store.trace_root).unwrap().count(), 0);
    assert_eq!(
        store.retained.summary("campaign").unwrap()["campaign_unresolved_bytes"],
        3
    );
    assert!(begin(store, "output:00000000-0000-0000-0000-000000000002", 1).is_err());
}

#[test]
fn trace_census_rejects_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    std::fs::create_dir(&store.trace_root).unwrap();
    let outside = dir.path().join("outside");
    std::fs::write(&outside, b"abc").unwrap();
    std::os::unix::fs::symlink(&outside, store.trace_root.join("orphan")).unwrap();
    assert!(store.adopt_retained_traces().is_err());
    assert_eq!(
        store.retained.summary("host-orphans").unwrap()["root_charged_bytes"],
        0
    );
}
