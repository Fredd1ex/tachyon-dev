use super::*;
use tachyon_api::types::{ArtifactPublication, ArtifactRegistration};

#[test]
fn retention_preserves_metadata_artifacts_and_survives_reopen() {
    assert!(serde_json::from_value::<ApiRequest>(serde_json::json!({
        "cmd":"local_retention_set", "id":"campaign-id"
    }))
    .is_err());
    let (dir, store, m) = fixture();
    let id = &m.campaign_id;
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    let before = store
        .research_request(&ApiRequest::CampaignGet { id: id.clone() })
        .unwrap();
    let ApiResponse::Campaign { campaign } = &before else {
        panic!()
    };
    let parent = campaign.research_id.clone();
    let workspace = dir.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(workspace.join("candidate"), b"abc").unwrap();
    let artifacts =
        ArtifactStore::open(&dir.path().join("campaigns").join(id).join("artifacts")).unwrap();
    let record = artifacts
        .register(
            "work",
            &workspace,
            ArtifactRegistration {
                id: "candidate".into(),
                path: "candidate".into(),
                kind: "file".into(),
                description: "retained".into(),
                size_bytes: 3,
                sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
                task_id: None,
                work_id: Some("work".into()),
                generation: None,
                assignment: None,
                attempt_id: None,
                publication: ArtifactPublication::Pending,
            },
        )
        .unwrap();
    assert!(service.retention(&parent, true).is_err());
    service.retention(id, true).unwrap();
    service.retention(id, true).unwrap();
    service.retention(&parent, true).unwrap();
    assert!(service.retention(id, false).is_err());
    assert!(store
        .research_request(&ApiRequest::CampaignCreate {
            command_id: "blocked".into(),
            research_id: parent.clone(),
            title: "new".into(),
            objective: "new".into(),
        })
        .is_err());
    assert_eq!(
        artifacts.metadata("work", "candidate").unwrap(),
        Some(record.clone())
    );
    assert_eq!(artifacts.read("work", "candidate", 0, 3).unwrap(), b"abc");
    assert!(artifacts.read("other", "candidate", 0, 3).is_err());
    let inventory = service.storage_inventory(id).unwrap();
    assert_eq!(inventory["artifact"]["object_path_bytes"], 3);
    assert_eq!(inventory["shared_quota_enforced"], true);
    assert!(matches!(
        service.inspect(id).unwrap(),
        ApiResponse::CampaignInspection { .. }
    ));
    assert_eq!(
        serde_json::to_value(
            store
                .research_request(&ApiRequest::CampaignGet { id: id.clone() })
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    let tx = store.database.begin_write().unwrap();
    assert!(RuntimeStore::set_campaign_status_in(&tx, id, CampaignStatus::Running).is_err());
    drop(tx);
    drop(artifacts);
    drop(service);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    let artifacts = ArtifactStore::open_retained(
        &dir.path().join("campaigns").join(id).join("artifacts"),
        store.retained.clone(),
        id,
    )
    .unwrap();
    assert_eq!(
        store.retained.summary(id).unwrap()["campaign_charged_bytes"],
        3
    );
    assert!(matches!(
        store.retention_get(id).unwrap(),
        ApiResponse::LocalRetention { archived: true, .. }
    ));
    service.retention(&parent, false).unwrap();
    service.retention(id, false).unwrap();
    assert_eq!(
        serde_json::to_value(
            store
                .research_request(&ApiRequest::CampaignGet { id: id.clone() })
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    assert_eq!(
        artifacts.metadata("work", "candidate").unwrap(),
        Some(record)
    );
    assert_eq!(artifacts.read("work", "candidate", 0, 3).unwrap(), b"abc");
    assert!(service.active.lock().unwrap().is_empty());
}

#[test]
fn retention_refuses_live_and_last_known_active_campaigns() {
    let (dir, store, m) = fixture();
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    service.active.lock().unwrap().insert(
        m.campaign_id.clone(),
        Active {
            oversight: false,
            id: m.campaign_id.clone(),
            cancel: watch::channel(false).0,
            task: std::thread::spawn(move || {
                receive.recv().unwrap();
            }),
        },
    );
    assert!(service
        .retention(&m.campaign_id, true)
        .unwrap_err()
        .contains("idle"));
    send.send(()).unwrap();
    service
        .active
        .lock()
        .unwrap()
        .remove(&m.campaign_id)
        .unwrap()
        .task
        .join()
        .unwrap();
    for status in [
        CampaignStatus::Running,
        CampaignStatus::Cancelling,
        CampaignStatus::AwaitingAcceptance,
    ] {
        let tx = store.database.begin_write().unwrap();
        RuntimeStore::set_campaign_status_in(&tx, &m.campaign_id, status).unwrap();
        tx.commit().unwrap();
        assert!(service.retention(&m.campaign_id, true).is_err());
    }
}

#[test]
fn trace_inventory_is_additive_without_counting_artifact_references_twice() {
    use crate::runtime_store::research_context::traces::TRACES;
    let (dir, store, m) = fixture();
    let service = CampaignService::new(store.clone(), dir.path().into()).unwrap();
    let tx = store.database.begin_write().unwrap();
    for (id, state, bytes, artifact) in [
        ("a", "ready", 7, false),
        ("b", "staging", 11, false),
        ("c", "gap", 13, false),
        ("d", "ready", 17, true),
    ] {
        let resource = serde_json::json!({"reference":{"kind":"trace","work_id":"work","id":id,"version":"v"},
            "occurred_at_ms":null,"data":{"size_bytes":bytes,"retention_state":state,"artifact":if artifact { serde_json::json!({}) } else { serde_json::Value::Null }}});
        tx.open_table(TRACES)
            .unwrap()
            .insert(
                (m.campaign_id.as_str(), id),
                serde_json::to_vec(&resource).unwrap().as_slice(),
            )
            .unwrap();
    }
    tx.commit().unwrap();
    let inventory = service.storage_inventory(&m.campaign_id).unwrap();
    assert_eq!(inventory["trace"]["ready_recorded_bytes"], 7);
    assert_eq!(inventory["trace"]["reserved_upper_bytes"], 11);
    assert_eq!(inventory["trace"]["uncertain_recorded_bytes"], 13);
    assert_eq!(inventory["trace"]["artifact_reference_bytes_excluded"], 17);
    assert_eq!(inventory["observed_artifact_plus_trace_charge_bytes"], 31);
    assert!(service
        .retention(&m.campaign_id, true)
        .unwrap_err()
        .contains("pending trace"));
}

#[test]
fn retention_and_launch_race_has_only_one_winner() {
    let (dir, store, m) = fixture();
    let service = Arc::new(CampaignService::new(store.clone(), dir.path().into()).unwrap());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let archive = {
        let service = service.clone();
        let barrier = barrier.clone();
        let id = m.campaign_id.clone();
        std::thread::spawn(move || {
            barrier.wait();
            service.retention(&id, true)
        })
    };
    let (release, wait) = watch::channel(false);
    barrier.wait();
    let launched = service.launch(
        &m,
        "http://127.0.0.1".into(),
        false,
        move |_, _| async move {
            let mut wait = wait;
            wait.changed().await.unwrap();
            Ok(ExecutionPhase::Reviewed(Evaluation::Unverified))
        },
    );
    let archived = archive.join().unwrap();
    assert_ne!(launched.is_ok(), archived.is_ok());
    if launched.is_ok() {
        release.send(true).unwrap();
        service
            .active
            .lock()
            .unwrap()
            .remove(&m.campaign_id)
            .unwrap()
            .task
            .join()
            .unwrap();
    }
}
