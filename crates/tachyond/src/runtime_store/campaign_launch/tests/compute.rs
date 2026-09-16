use super::*;
use tachyon_api::campaign::{ComputeEnvelope, ComputeProfile};
use tachyon_model::broker::JobWorkload;
#[allow(non_upper_case_globals)]
const Cpu: JobWorkload = JobWorkload::Cpu {};
#[allow(non_upper_case_globals)]
const Gpu: JobWorkload = JobWorkload::Gpu {};
use uuid::Uuid;

fn configure(store: &RuntimeStore, m: &mut CampaignManifest) -> WorkIdentity {
    m.compute = Some(ComputeEnvelope {
        profiles: Default::default(),
        cpu_job_ms: 1000,
        gpu_job_ms: 1000,
        max_gpu_jobs: 1,
        max_cpu_timeout_ms: 1000,
        max_gpu_timeout_ms: 1000,
    });
    save(store, m);
    WorkIdentity {
        campaign_id: m.campaign_id.clone(),
        work_id: format!("{}-root", m.campaign_id),
        attempt_id: "attempt".into(),
        generation: 1,
        instruction_revision: 1,
        class: RequestClass::Work,
    }
}

fn save(store: &RuntimeStore, m: &CampaignManifest) {
    let tx = store.database.begin_write().unwrap();
    tx.open_table(LAUNCHES)
        .unwrap()
        .insert(
            m.campaign_id.as_str(),
            serde_json::to_vec(&Launch {
                schema_version: 1,
                manifest: m.clone(),
                base_url: "fixture".into(),
                manifest_sha256: Some(Launch::digest(m).unwrap()),
            })
            .unwrap()
            .as_slice(),
        )
        .unwrap();
    tx.commit().unwrap();
}

#[test]
fn native_cpu_concurrent_budget_has_one_winner_and_zero_refund_replay() {
    let (_dir, store, mut manifest) = fixture();
    let identity = configure(&store, &mut manifest);
    let barrier = std::sync::Barrier::new(8);
    let results = std::thread::scope(|scope| {
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let (store, identity, barrier) = (&store, &identity, &barrier);
                scope.spawn(move || {
                    let (session, lease) = (Uuid::new_v4(), Uuid::new_v4());
                    barrier.wait();
                    let deadline = std::time::Instant::now() + Duration::from_secs(5);
                    loop {
                        let result = store.acquire_job(session, lease, identity, Cpu, 600);
                        if !matches!(result, Ok(None)) {
                            break (session, lease, result);
                        }
                        // Mutex contention is not evidence of budget enforcement.
                        assert!(std::time::Instant::now() < deadline);
                        std::thread::yield_now();
                    }
                })
            })
            .collect();
        tasks
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    let winners: Vec<_> = results
        .iter()
        .filter(|(_, _, r)| matches!(r, Ok(Some(_))))
        .collect();
    assert_eq!(winners.len(), 1);
    assert_eq!(results.iter().filter(|(_, _, r)| r.is_err()).count(), 7);
    for (_, _, result) in &results {
        if let Err(error) = result {
            assert_eq!(error, "native compute budget exhausted");
        }
    }
    let (session, lease, _) = winners[0];
    assert!(store.release_job(Uuid::new_v4(), *lease, true).is_err());
    store.release_job(*session, *lease, true).unwrap();
    store.release_job(*session, *lease, true).unwrap();
    let jobs = RuntimeStore::inspect_jobs_in(
        &store.database.begin_write().unwrap(),
        &identity.campaign_id,
    )
    .unwrap();
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].contains("\"final_ms\":0"));
}

#[test]
fn native_gpu_explicit_inventory_exclusive_across_campaigns_and_restart() {
    let (dir, store, mut manifest) = fixture();
    let mut store = Arc::try_unwrap(store).ok().unwrap();
    let identity = configure(&store, &mut manifest);
    assert!(store
        .acquire_job(Uuid::new_v4(), Uuid::new_v4(), &identity, Gpu, 100)
        .is_err());
    let limits = tachyon_util::config::ResourceLimits {
        max_gpu_jobs: 1,
        gpu_device_ids: vec!["GPU-fixture-0".into()],
        ..Default::default()
    };
    store.host_capacity =
        crate::runtime_store::host_capacity::HostCapacity::new(limits.clone()).unwrap();
    let session = Uuid::new_v4();
    let lease = Uuid::new_v4();
    assert_eq!(
        store
            .acquire_job(session, lease, &identity, Gpu, 600)
            .unwrap()
            .unwrap(),
        ["GPU-fixture-0"]
    );
    let mut second = manifest.clone();
    second.campaign_id = "second".into();
    let other = configure(&store, &mut second);
    let next_session = Uuid::new_v4();
    let next_lease = Uuid::new_v4();
    assert!(store
        .acquire_job(next_session, next_lease, &other, Gpu, 600)
        .unwrap()
        .is_none());
    // Reopen redb, without retaining any in-memory leases or queue state.
    let path = dir.path().join("runtime.redb");
    assert!(path.exists());
    drop(store);
    let mut store = RuntimeStore::open(&path).unwrap();
    store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(limits).unwrap();
    assert!(store
        .acquire_job(next_session, next_lease, &other, Gpu, 600)
        .unwrap()
        .is_none());
    assert!(!matches!(
        store.acquire_job(Uuid::new_v4(), Uuid::new_v4(), &identity, Gpu, 600),
        Ok(Some(_))
    ));
    let tx = store.database.begin_write().unwrap();
    assert!(RuntimeStore::cleanup_job_in(
        &tx,
        &identity.campaign_id,
        &identity.work_id,
        &identity.attempt_id,
        2,
        &lease.to_string()
    )
    .is_err());
    RuntimeStore::cleanup_job_in(
        &tx,
        &identity.campaign_id,
        &identity.work_id,
        &identity.attempt_id,
        1,
        &lease.to_string(),
    )
    .unwrap();
    tx.commit().unwrap();
    store.release_cleaned_jobs().unwrap();
    assert_eq!(
        store
            .acquire_job(next_session, next_lease, &other, Gpu, 600)
            .unwrap()
            .unwrap(),
        ["GPU-fixture-0"]
    );
    let tx = store.database.begin_write().unwrap();
    RuntimeStore::cleanup_job_in(
        &tx,
        &identity.campaign_id,
        &identity.work_id,
        &identity.attempt_id,
        1,
        &lease.to_string(),
    )
    .unwrap();
    tx.commit().unwrap();
    store.release_cleaned_jobs().unwrap();
    assert!(store
        .acquire_job(Uuid::new_v4(), Uuid::new_v4(), &identity, Gpu, 400)
        .unwrap()
        .is_none());
}

#[test]
fn native_elapsed_is_host_monotonic_and_unknown_cleanup_never_refunds() {
    let (_dir, store, mut manifest) = fixture();
    let identity = configure(&store, &mut manifest);
    let (session, lease) = (Uuid::new_v4(), Uuid::new_v4());
    assert!(store
        .acquire_job(session, lease, &identity, Cpu, 500)
        .unwrap()
        .is_some());
    // Reserve the unknown job before measuring elapsed time. Slow cleanup may
    // legitimately consume more than the first hold, even in a correct host.
    let unknown = Uuid::new_v4();
    assert!(store
        .acquire_job(session, unknown, &identity, Cpu, 500)
        .unwrap()
        .is_some());
    std::thread::sleep(Duration::from_millis(2));
    store.release_job(session, lease, false).unwrap();
    let tx = store.database.begin_write().unwrap();
    let jobs = RuntimeStore::inspect_jobs_in(&tx, &identity.campaign_id).unwrap();
    let record = jobs
        .iter()
        .map(|job| {
            serde_json::from_str::<serde_json::Value>(
                job.strip_prefix("native_job_wall_time: ").unwrap(),
            )
            .unwrap()
        })
        .find(|job| job["lease_id"] == lease.to_string())
        .unwrap();
    assert!(record["final_ms"].as_u64().unwrap() >= 2);
    drop(tx);
    let tx = store.database.begin_write().unwrap();
    RuntimeStore::cleanup_jobs_in(
        &tx,
        &identity.campaign_id,
        &identity.work_id,
        &identity.attempt_id,
        identity.generation,
    )
    .unwrap();
    tx.commit().unwrap();
    store.release_cleaned_jobs().unwrap();
    assert!(store
        .acquire_job(session, Uuid::new_v4(), &identity, Cpu, 500)
        .is_err());
    // Even an old session claiming no spawn cannot refund authoritative cleanup.
    store.release_job(session, unknown, true).unwrap();
    assert!(store
        .acquire_job(session, Uuid::new_v4(), &identity, Cpu, 500)
        .is_err());
}

#[test]
fn native_cpu_overrun_releases_capacity_but_preserves_debt_after_reopen() {
    let (dir, store, mut manifest) = fixture();
    let identity = configure(&store, &mut manifest);
    manifest.compute.as_mut().unwrap().cpu_job_ms = 1;
    save(&store, &manifest);
    let (session, lease) = (Uuid::new_v4(), Uuid::new_v4());
    let capacity = store.host_capacity.cpu.available_permits();
    assert!(store
        .acquire_job(session, lease, &identity, Cpu, 1)
        .unwrap()
        .is_some());
    assert_eq!(store.host_capacity.cpu.available_permits(), capacity - 1);
    std::thread::sleep(Duration::from_millis(2));
    store.release_job(session, lease, false).unwrap();
    assert_eq!(store.host_capacity.cpu.available_permits(), capacity);
    let jobs = RuntimeStore::inspect_jobs_in(
        &store.database.begin_write().unwrap(),
        &identity.campaign_id,
    )
    .unwrap();
    let record: serde_json::Value =
        serde_json::from_str(jobs[0].strip_prefix("native_job_wall_time: ").unwrap()).unwrap();
    assert_eq!(record["reserved_ms"], 1);
    assert!(record["final_ms"].as_u64().unwrap() >= 2);
    store.release_job(session, lease, true).unwrap();
    assert_eq!(store.host_capacity.cpu.available_permits(), capacity);
    assert!(store
        .acquire_job(session, Uuid::new_v4(), &identity, Cpu, 1)
        .is_err());
    assert_eq!(
        RuntimeStore::inspect_jobs_in(
            &store.database.begin_write().unwrap(),
            &identity.campaign_id,
        )
        .unwrap(),
        jobs
    );
    drop(store);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    assert!(store
        .acquire_job(session, Uuid::new_v4(), &identity, Cpu, 1)
        .is_err());
    let mut other_manifest = manifest.clone();
    other_manifest.campaign_id = "other".into();
    let other = configure(&store, &mut other_manifest);
    let other_lease = Uuid::new_v4();
    assert!(store
        .acquire_job(session, other_lease, &other, Cpu, 1)
        .unwrap()
        .is_some());
    store.release_job(session, other_lease, true).unwrap();
    assert_eq!(store.host_capacity.cpu.available_permits(), capacity);
}

#[test]
fn native_descendants_have_no_implicit_gpu_grant_or_extra_budget() {
    let (_dir, store, mut manifest) = fixture();
    let identity = configure(&store, &mut manifest);
    let mut child = identity.clone();
    child.work_id = "child".into();
    assert_eq!(store.compute_bounds(&child).unwrap().1.max_gpu_jobs, 0);
    assert_eq!(store.compute_bounds(&identity).unwrap().1.max_gpu_jobs, 1);
    let p = manifest.compute.as_mut().unwrap();
    p.profiles.insert(
        "declared".into(),
        ComputeProfile {
            max_gpu_jobs: 2,
            max_cpu_timeout_ms: 10,
            max_gpu_timeout_ms: 10,
        },
    );
    assert!(p.validate().is_err());
    assert!(store
        .acquire_job(Uuid::new_v4(), Uuid::new_v4(), &identity, Cpu, 600)
        .unwrap()
        .is_some());
    assert!(store
        .acquire_job(Uuid::new_v4(), Uuid::new_v4(), &child, Cpu, 600)
        .is_err());
    assert!(store
        .acquire_job(Uuid::new_v4(), Uuid::new_v4(), &identity, Cpu, 1001)
        .is_err());
}

#[test]
fn native_profile_gpu_cap_and_timeout_do_not_block_another_campaign() {
    let (_dir, store, mut manifest) = fixture();
    let mut store = Arc::try_unwrap(store).ok().unwrap();
    let mut child = configure(&store, &mut manifest);
    store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(
        tachyon_util::config::ResourceLimits {
            max_gpu_jobs: 2,
            gpu_device_ids: vec!["GPU-a".into(), "GPU-b".into()],
            ..Default::default()
        },
    )
    .unwrap();
    manifest.children = Some(serde_json::from_value(serde_json::json!({
        "total_work":2,"max_running":2,"max_resident":2,"controls":[],"history":false,"completion":"cancel_outstanding",
        "templates":[{"template_id":"analysis","group_id":null,"max_running":2,"specs":[
            {"work_id":"child-1","objective":"one","workspace":"/fixture/one","home":"/fixture/home-one","work_tokens":1,"work_cost_micro_usd":1,"verification_tokens":1,"verification_cost_micro_usd":1},
            {"work_id":"child-2","objective":"two","workspace":"/fixture/two","home":"/fixture/home-two","work_tokens":1,"work_cost_micro_usd":1,"verification_tokens":1,"verification_cost_micro_usd":1}
        ]}]
    })).unwrap());
    let p = manifest.compute.as_mut().unwrap();
    p.max_gpu_jobs = 2;
    p.profiles.insert(
        "analysis".into(),
        ComputeProfile {
            max_gpu_jobs: 1,
            max_cpu_timeout_ms: 50,
            max_gpu_timeout_ms: 100,
        },
    );
    save(&store, &manifest);
    child.work_id = "child-1".into();
    assert_eq!(store.compute_bounds(&child).unwrap().1.max_gpu_jobs, 1);
    assert!(store
        .acquire_job(Uuid::new_v4(), Uuid::new_v4(), &child, Gpu, 101)
        .is_err());
    assert!(store
        .acquire_job(Uuid::new_v4(), Uuid::new_v4(), &child, Cpu, 51)
        .is_err());
    let (session, lease) = (Uuid::new_v4(), Uuid::new_v4());
    assert_eq!(
        store
            .acquire_job(session, lease, &child, Gpu, 100)
            .unwrap()
            .unwrap(),
        ["GPU-a"]
    );
    child.work_id = "child-2".into();
    let (queued, next) = (Uuid::new_v4(), Uuid::new_v4());
    assert!(store
        .acquire_job(queued, next, &child, Gpu, 100)
        .unwrap()
        .is_none());
    let mut other_manifest = manifest.clone();
    other_manifest.campaign_id = "other".into();
    let other = configure(&store, &mut other_manifest);
    let (other_session, other_lease) = (Uuid::new_v4(), Uuid::new_v4());
    assert!(store
        .acquire_job(other_session, other_lease, &other, Gpu, 100)
        .unwrap()
        .is_none());
    // Polling the occupied profile rotates its campaign without granting a job.
    assert!(store
        .acquire_job(queued, next, &child, Gpu, 100)
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .acquire_job(other_session, other_lease, &other, Gpu, 100)
            .unwrap()
            .unwrap(),
        ["GPU-b"]
    );
    store.release_job(session, lease, true).unwrap();
    assert_eq!(
        store
            .acquire_job(queued, next, &child, Gpu, 100)
            .unwrap()
            .unwrap(),
        ["GPU-a"]
    );
}
