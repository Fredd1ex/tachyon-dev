use super::*;
use tempfile::TempDir;

struct Fixture {
    storage: TempDir,
    workspace: TempDir,
    store: RuntimeStore,
    id: String,
    request: ApiRequest,
}

impl Fixture {
    fn new() -> Self {
        let storage = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&storage.path().join("runtime.redb")).unwrap();
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: "r".into(),
                title: "integration".into(),
                objective: "fixture".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: "c".into(),
                research_id: research.id,
                title: "integration".into(),
                objective: "fixture".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let id = campaign.id;
        for path in ["a", "b"] {
            std::fs::write(workspace.path().join(path), "old\n").unwrap();
        }
        let manifest = json!({"schema_version":1,"campaign_id":id,"objective":"fixture",
            "executable":"/nonexistent/ghost","workspace":workspace.path(),"home":"/nonexistent/home",
            "deadline_ms":u64::MAX,"work_tokens":100,"work_cost_micro_usd":100,
            "verification_tokens":10,"verification_cost_micro_usd":10,"max_active_inferences":2,
            "model":"fixture","pricing_revision":"fixture","max_request_bytes":32000,
            "input_tokens":20,"output_tokens":10,"input_micro_usd_per_million":1,
            "output_micro_usd_per_million":1,"other_micro_usd":0,
            "evaluator":{"argv":["/usr/bin/true"],"timeout_ms":100,"output_bytes":1024,
                "input_bytes":1024,"max_attempts":1,"max_total_command_ms":100}});
        let manifest: tachyon_api::campaign::CampaignManifest =
            serde_json::from_value(manifest).unwrap();
        let launch = json!({"schema_version":1,"manifest":manifest,"base_url":"http://127.0.0.1:1",
            "manifest_sha256":hash(&serde_json::to_vec(&manifest).unwrap())});
        let tx = store.database.begin_write().unwrap();
        tx.open_table(LAUNCHES)
            .unwrap()
            .insert(id.as_str(), serde_json::to_vec(&launch).unwrap().as_slice())
            .unwrap();
        tx.commit().unwrap();
        store.retained.configure(&id, None).unwrap();
        let artifact_root = storage.path().join("campaigns").join(&id).join("artifacts");
        let artifacts =
            ArtifactStore::open_retained(&artifact_root, store.retained.clone(), &id).unwrap();
        let child = tempfile::tempdir().unwrap();
        let bundle = json!({"files":[{"path":"a","old":"old","new":"new"},{"path":"b","old":"old","new":"new"}]});
        let bytes = serde_json::to_vec(&bundle).unwrap();
        std::fs::write(child.path().join("patch.json"), &bytes).unwrap();
        for work in ["child-1", "child-2"] {
            artifacts
                .register(
                    work,
                    child.path(),
                    ArtifactRegistration {
                        id: "patch".into(),
                        path: "patch.json".into(),
                        kind: "file".into(),
                        description: "child exact patch".into(),
                        size_bytes: bytes.len() as u64,
                        sha256: hash(&bytes),
                        task_id: None,
                        work_id: Some(work.into()),
                        generation: Some(1),
                        assignment: Some(1),
                        attempt_id: Some("attempt".into()),
                        publication: ArtifactPublication::Pending,
                    },
                )
                .unwrap();
            let funding = json!({"schema_version":1,"admission":{"work_id":work,"campaign_id":id,
                "objective":"fixture","instruction_revision":1,"generation":1,"pool":"Work",
                "upper_bound":{"tokens":100,"cost_micro_usd":100}},"dispatch_id":"dispatch","state":"Admitted"});
            let record = json!({"schema_version":1,"phase":"EvidenceReady","settled":false,
                "policy":{"funding":funding,"verification":funding,"evaluator_id":"fixture",
                    "work":{"work_id":work,"objective":"fixture","generation":1,"assignment":1,
                        "lifetime_class":"short","deadline_ms":u64::MAX,"context_refs":[]},
                    "model":{"identity":{"campaign_id":id,"work_id":work,"attempt_id":"attempt","generation":1,"instruction_revision":1,"class":"Work"},
                        "estimate":{"base_url":"http://127.0.0.1:1","model":"fixture","provider":"fixture","pricing_revision":"fixture",
                            "max_request_bytes":32000,"input_tokens":20,"output_tokens":10,"input_micro_usd_per_million":1,
                            "output_micro_usd_per_million":1,"other_micro_usd":0}}},
                "candidate":{"work_id":work,"objective":"fixture","generation":1,"assignment":1,
                    "outcome":"completed","result":"patch","artifacts":["patch.json"],"candidate_refs":["patch"]}});
            let tx = store.database.begin_write().unwrap();
            tx.open_table(super::super::execution::EXECUTIONS)
                .unwrap()
                .insert(work, serde_json::to_vec(&record).unwrap().as_slice())
                .unwrap();
            tx.commit().unwrap();
        }
        drop(artifacts);
        // No child workspace is consulted by the production integration entry point.
        drop(child);
        let ApiResponse::CampaignIntegration { report } = store
            .integration_request(
                &ApiRequest::CampaignIntegrationSnapshot {
                    id: id.clone(),
                    paths: vec!["b".into(), "a".into()],
                },
                storage.path(),
            )
            .unwrap()
        else {
            panic!()
        };
        let request = ApiRequest::CampaignIntegrate {
            id: id.clone(),
            plan: IntegrationPlan {
                command_id: "operator-1".into(),
                work_id: "child-1".into(),
                artifact_id: "patch".into(),
                artifact_sha256: hash(&bytes),
                expected_versions: serde_json::from_value(report["expected_versions"].clone())
                    .unwrap(),
            },
            expected_state: report["expected_state"].as_str().unwrap().into(),
            confirm: true,
        };
        Self {
            storage,
            workspace,
            store,
            id,
            request,
        }
    }
    fn run(&self, request: &ApiRequest) -> Result<serde_json::Value, String> {
        match self
            .store
            .integration_request(request, self.storage.path())?
        {
            ApiResponse::CampaignIntegration { report } => Ok(report),
            _ => panic!(),
        }
    }
    fn content(&self, path: &str) -> String {
        std::fs::read_to_string(self.workspace.path().join(path)).unwrap()
    }
}

#[test]
fn two_child_plans_first_wins_second_stale_changes_nothing() {
    let f = Fixture::new();
    let mut second = f.request.clone();
    if let ApiRequest::CampaignIntegrate { plan, .. } = &mut second {
        plan.work_id = "child-2".into();
        plan.command_id = "operator-2".into();
    }
    let report = f.run(&f.request).unwrap();
    assert_eq!(report["journal"]["phase"], "completed");
    assert_eq!(report["verification_required"], true);
    let before = std::fs::metadata(f.workspace.path().join("a"))
        .unwrap()
        .modified()
        .unwrap();
    assert!(f.run(&second).unwrap_err().contains("preflight conflict"));
    assert_eq!(f.content("a"), "new\n");
    assert_eq!(f.content("b"), "new\n");
    assert_eq!(f.run(&f.request).unwrap()["journal"]["phase"], "completed");
    assert_eq!(
        before,
        std::fs::metadata(f.workspace.path().join("a"))
            .unwrap()
            .modified()
            .unwrap()
    );
}

#[test]
fn one_stale_file_preflights_all_without_editing_other_file() {
    let f = Fixture::new();
    std::fs::write(f.workspace.path().join("b"), "external").unwrap();
    assert!(f
        .run(&f.request)
        .unwrap_err()
        .contains("b: version conflict"));
    assert_eq!(f.content("a"), "old\n");
    assert_eq!(f.content("b"), "external");
}

#[test]
fn concurrent_integrations_have_one_winner() {
    let f = Fixture::new();
    let mut second = f.request.clone();
    if let ApiRequest::CampaignIntegrate { plan, .. } = &mut second {
        plan.command_id = "operator-2".into();
        plan.work_id = "child-2".into();
    }
    let barrier = std::sync::Barrier::new(2);
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            barrier.wait();
            f.run(&f.request)
        });
        let second = scope.spawn(|| {
            barrier.wait();
            f.run(&second)
        });
        (first.join().unwrap(), second.join().unwrap())
    });
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(f.content("a"), "new\n");
    assert_eq!(f.content("b"), "new\n");
}

#[test]
fn concurrent_identical_requests_apply_once() {
    let f = Fixture::new();
    let barrier = std::sync::Barrier::new(2);
    let (first, second) = std::thread::scope(|scope| {
        let run = || {
            barrier.wait();
            f.run(&f.request).unwrap()
        };
        let first = scope.spawn(run);
        let second = scope.spawn(run);
        (first.join().unwrap(), second.join().unwrap())
    });
    assert_eq!(first, second);
    assert_eq!(first["journal"]["phase"], "completed");
    for file in first["journal"]["files"].as_array().unwrap() {
        assert_eq!(file["observed_sha256"], hash(b"new\n"));
        assert!(file["observed_version"]
            .as_str()
            .unwrap()
            .starts_with("stat-v1:"));
    }
}

#[test]
fn final_readback_detects_earlier_output_changed_during_batch() {
    let f = Fixture::new();
    let path = f.workspace.path().join("a");
    BEFORE_FINAL_READ.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::fs::write(path, "external").unwrap();
        }))
    });
    let report = f.run(&f.request).unwrap();
    assert!(report["error"]
        .as_str()
        .unwrap()
        .contains("final output conflict"));
    assert_eq!(report["journal"]["phase"], "applying");
    assert_eq!(
        report["journal"]["files"][0]["observed_sha256"],
        hash(b"external")
    );
    assert!(f.run(&f.request).unwrap()["error"]
        .as_str()
        .unwrap()
        .contains("recovery conflict"));
    assert_eq!(f.content("a"), "external");
    assert_eq!(f.content("b"), "new\n");
}

#[test]
fn recovery_preflights_untouched_files_and_rejects_stale_journal_updates() {
    let f = Fixture::new();
    STOP_AFTER_REPLACE.with(|v| v.set(true));
    f.run(&f.request).unwrap_err();
    std::fs::write(f.workspace.path().join("b"), "external").unwrap();
    let report = f.run(&f.request).unwrap();
    assert!(report["error"]
        .as_str()
        .unwrap()
        .contains("recovery conflict"));
    assert_eq!(f.content("a"), "new\n");
    assert_eq!(f.content("b"), "external");
    let mut first: Journal = serde_json::from_value(report["journal"].clone()).unwrap();
    let mut stale: Journal = serde_json::from_value(report["journal"].clone()).unwrap();
    let key = hash(&serde_json::to_vec(&(&f.id, "operator-1")).unwrap());
    f.store.save_integration(&key, &mut first).unwrap();
    assert!(f
        .store
        .save_integration(&key, &mut stale)
        .unwrap_err()
        .contains("compare-and-swap"));
}

#[test]
fn recovery_rejects_poisoned_original_snapshot_identity() {
    let f = Fixture::new();
    STOP_AFTER_REPLACE.with(|v| v.set(true));
    f.run(&f.request).unwrap_err();
    let key = hash(&serde_json::to_vec(&(&f.id, "operator-1")).unwrap());
    let artifacts = ArtifactStore::open_retained(
        &f.storage
            .path()
            .join("campaigns")
            .join(&f.id)
            .join("artifacts"),
        f.store.retained.clone(),
        &f.id,
    )
    .unwrap();
    let poison = retain(&artifacts, &format!("integration-{key}"), b"poison").unwrap();
    drop(artifacts);
    let tx = f.store.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(JOURNAL).unwrap();
        let mut journal: Journal =
            serde_json::from_slice(table.get(key.as_str()).unwrap().unwrap().value()).unwrap();
        journal.files[0].before = poison;
        table
            .insert(
                key.as_str(),
                serde_json::to_vec(&journal).unwrap().as_slice(),
            )
            .unwrap();
    }
    tx.commit().unwrap();
    assert!(f
        .run(&f.request)
        .unwrap_err()
        .contains("approved exact edit"));
    assert_eq!(f.content("a"), "new\n");
    assert_eq!(f.content("b"), "old\n");
}

#[test]
fn oversize_original_and_exhausted_retention_never_write_root() {
    for oversized in [true, false] {
        let f = Fixture::new();
        if oversized {
            std::fs::write(f.workspace.path().join("b"), vec![b'x'; LIMIT + 1]).unwrap();
        } else {
            let summary = f.store.retained.summary(&f.id).unwrap();
            let limit = summary["campaign_limit_bytes"].as_u64().unwrap();
            let charged = summary["campaign_charged_bytes"].as_u64().unwrap();
            f.store
                .retained
                .reserve(&f.id, "test", "fill", limit - charged, "fixture")
                .unwrap();
        }
        assert!(f.run(&f.request).is_err());
        assert_eq!(f.content("a"), "old\n");
        if !oversized {
            assert_eq!(f.content("b"), "old\n");
        }
    }
}

#[test]
fn source_requires_exact_campaign_candidate_and_execution_identity() {
    for kind in [
        "campaign",
        "candidate",
        "generation",
        "assignment",
        "attempt",
    ] {
        let f = Fixture::new();
        let tx = f.store.database.begin_write().unwrap();
        {
            let mut table = tx.open_table(super::super::execution::EXECUTIONS).unwrap();
            let mut record: serde_json::Value =
                serde_json::from_slice(table.get("child-1").unwrap().unwrap().value()).unwrap();
            match kind {
                "campaign" => {
                    record["policy"]["funding"]["admission"]["campaign_id"] =
                        json!("other-campaign")
                }
                "candidate" => record["candidate"]["candidate_refs"] = json!([]),
                "generation" => record["policy"]["work"]["generation"] = json!(2),
                "assignment" => record["policy"]["work"]["assignment"] = json!(2),
                _ => record["policy"]["model"]["identity"]["attempt_id"] = json!("other-attempt"),
            }
            table
                .insert("child-1", serde_json::to_vec(&record).unwrap().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
        assert!(f.run(&f.request).is_err(), "{kind}");
        assert_eq!(f.content("a"), "old\n");
        assert_eq!(f.content("b"), "old\n");
    }
}

#[test]
fn crash_after_first_rename_reopens_and_resumes_without_replacing_twice() {
    let mut f = Fixture::new();
    STOP_AFTER_REPLACE.with(|v| v.set(true));
    assert!(f.run(&f.request).unwrap_err().contains("injected crash"));
    assert_eq!(f.content("a"), "new\n");
    assert_eq!(f.content("b"), "old\n");
    let metadata = std::fs::metadata(f.workspace.path().join("a")).unwrap();
    let db = f.storage.path().join("runtime.redb");
    drop(f.store);
    f.store = RuntimeStore::open(&db).unwrap();
    assert_eq!(f.run(&f.request).unwrap()["journal"]["phase"], "completed");
    use std::os::unix::fs::MetadataExt;
    assert_eq!(
        metadata.ino(),
        std::fs::metadata(f.workspace.path().join("a"))
            .unwrap()
            .ino()
    );
    assert_eq!(f.content("b"), "new\n");
}

#[test]
fn crash_then_external_write_conflicts_and_preserves_snapshots_and_other_file() {
    let f = Fixture::new();
    STOP_AFTER_REPLACE.with(|v| v.set(true));
    f.run(&f.request).unwrap_err();
    let mut competing = f.request.clone();
    if let ApiRequest::CampaignIntegrate { plan, .. } = &mut competing {
        plan.command_id = "cannot-bypass-partial".into();
    }
    assert!(f
        .run(&competing)
        .unwrap_err()
        .contains("unfinished integration"));
    let mut changed = f.request.clone();
    if let ApiRequest::CampaignIntegrate { expected_state, .. } = &mut changed {
        *expected_state = "0".repeat(64);
    }
    assert!(f.run(&changed).unwrap_err().contains("identity conflict"));
    std::fs::write(f.workspace.path().join("a"), "external").unwrap();
    let report = f.run(&f.request).unwrap();
    assert!(report["error"]
        .as_str()
        .unwrap()
        .contains("recovery conflict"));
    assert_eq!(f.content("a"), "external");
    assert_eq!(f.content("b"), "old\n");
    let key = hash(&serde_json::to_vec(&(&f.id, "operator-1")).unwrap());
    let artifacts = ArtifactStore::open_retained(
        &f.storage
            .path()
            .join("campaigns")
            .join(&f.id)
            .join("artifacts"),
        f.store.retained.clone(),
        &f.id,
    )
    .unwrap();
    assert_eq!(
        artifacts
            .read(
                &format!("integration-{key}"),
                report["journal"]["files"][0]["before"].as_str().unwrap(),
                0,
                LIMIT
            )
            .unwrap(),
        b"old\n"
    );
}

#[test]
fn rejects_missing_confirmation_state_scope_and_paths() {
    let f = Fixture::new();
    for kind in ["confirm", "state", "scope", "digest"] {
        let mut request = f.request.clone();
        if let ApiRequest::CampaignIntegrate {
            plan,
            confirm,
            expected_state,
            ..
        } = &mut request
        {
            match kind {
                "confirm" => *confirm = false,
                "state" => *expected_state = "0".repeat(64),
                "scope" => plan.work_id = "other".into(),
                _ => plan.artifact_sha256 = "0".repeat(64),
            }
        }
        assert!(f.run(&request).is_err(), "{kind}");
    }
    std::os::unix::fs::symlink("a", f.workspace.path().join("alias")).unwrap();
    for path in [
        "/etc/passwd",
        "../a",
        "./a",
        "a/../b",
        "alias",
        "a//x",
        ".tachyon-native-write-locks/x",
    ] {
        assert!(
            f.run(&ApiRequest::CampaignIntegrationSnapshot {
                id: f.id.clone(),
                paths: vec![path.into()]
            })
            .is_err(),
            "{path}"
        );
    }
    assert_eq!(f.content("a"), "old\n");
    assert_eq!(f.content("b"), "old\n");
}
