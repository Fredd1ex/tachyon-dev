use super::*;
use std::os::unix::fs::PermissionsExt;
use tachyon_api::types::ArtifactPublication;

fn config(argv: &[&str]) -> CommandEvaluator {
    CommandEvaluator {
        acceptance_mode: None,
        result_contract: Default::default(),
        metrics: Default::default(),
        allow_extra_metrics: false,
        stage: None,
        argv: argv.iter().map(|s| s.to_string()).collect(),
        cwd: ".".into(),
        timeout_ms: 2000,
        output_bytes: 1024,
        input_bytes: 1024,
        max_attempts: 2,
        max_total_command_ms: 4000,
    }
}

fn candidate() -> WorkResult {
    serde_json::from_value(serde_json::json!({
        "work_id":"work", "objective":"bounded", "generation":1, "assignment":1,
        "outcome":"completed", "result":"ok", "artifacts":["input"], "candidate_refs":["artifact"]
    }))
    .unwrap()
}

#[test]
fn json_metrics_rejects_ambiguous_or_incomplete_measurements() {
    let mut config = config(&["/usr/bin/true"]);
    let legacy_hash = config.config_hash().unwrap();
    let legacy = br#"{"argv":["/usr/bin/true"],"cwd":".","timeout_ms":2000,"output_bytes":1024,"input_bytes":1024,"max_attempts":2,"max_total_command_ms":4000}"#;
    assert_eq!(serde_json::to_vec(&config).unwrap(), legacy);
    assert_eq!(legacy_hash, format!("{:x}", Sha256::digest(legacy)));
    let serialized = serde_json::to_value(&config).unwrap();
    assert!(serialized.get("result_contract").is_none());
    config.result_contract = tachyon_api::campaign::ResultContract::JsonMetrics;
    config.metrics =
        serde_json::from_value(serde_json::json!({"score":{"min":0.9,"max":1}})).unwrap();
    assert_ne!(config.config_hash().unwrap(), legacy_hash);
    for bytes in [
        r#"{"score":0.9}"#,
        " {\"score\":1} \n",
        r#"{"score":9e-1}"#,
        r#"{"score":1E+0}"#,
    ] {
        assert!(
            evaluate_metrics(&config, bytes.as_bytes(), false).is_ok(),
            "{bytes}"
        );
    }
    for bytes in [
        "",
        "[]",
        "null",
        r#"{"score":0.95} {}"#,
        r#"{"score":1,"score":0}"#,
        r#"{"score":"1"}"#,
        r#"{"score":null}"#,
        r#"{"score":true}"#,
        r#"{"score":false}"#,
        r#"{"score":NaN}"#,
        r#"{"score":Infinity}"#,
        r#"{"score":1e999}"#,
        r#"{"score":-1e999}"#,
        r#"{"score":1,"\u0073core":0}"#,
        r#"{"score":{}}"#,
        r#"{"score":0.89}"#,
        r#"{"score":1.01}"#,
        r#"{}"#,
        r#"{"score":1,"extra":2}"#,
    ] {
        assert!(
            evaluate_metrics(&config, bytes.as_bytes(), false).is_err(),
            "{bytes}"
        );
    }
    assert!(evaluate_metrics(&config, br#"{"score":1}"#, true).is_err());
    let strict_hash = config.config_hash().unwrap();
    config.allow_extra_metrics = true;
    assert_ne!(config.config_hash().unwrap(), strict_hash);
    assert!(evaluate_metrics(&config, br#"{"score":1,"extra":2}"#, false).is_ok());
    assert!(evaluate_metrics(&config, br#"{"score":1,"extra":null}"#, false).is_err());
    assert!(evaluate_metrics(&config, br#"{"score":1,"extra":1e999}"#, false).is_err());
    assert!(evaluate_metrics(&config, br#"{"score":1,"extra":1,"extra":2}"#, false).is_err());
    config.stage = Some(tachyon_api::campaign::EvaluationStage::FinalHeldout);
    assert!(config.config_hash().is_err());
    config.max_attempts = 1;
    assert!(config.config_hash().is_ok());
}

fn snapshot(store: &ArtifactStore, workspace: &Path) -> ArtifactRegistration {
    std::fs::write(workspace.join("input"), b"original").unwrap();
    store
        .register(
            "work",
            workspace,
            ArtifactRegistration {
                id: "artifact".into(),
                path: "input".into(),
                kind: "file".into(),
                description: "fixture".into(),
                size_bytes: 8,
                sha256: format!("{:x}", Sha256::digest(b"original")),
                task_id: None,
                work_id: Some("work".into()),
                generation: Some(1),
                assignment: Some(1),
                attempt_id: Some("attempt-1".into()),
                publication: ArtifactPublication::Pending,
            },
        )
        .unwrap()
}

#[tokio::test]
async fn metric_command_requires_complete_stdout_and_successful_exit() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    for dir in [&root, &staging] {
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let store = Arc::new(ArtifactStore::open(root.path()).unwrap());
    let snapshot = snapshot(&store, workspace.path());
    for (script, pass) in [
        ("printf '{\"score\":1}'", true),
        ("printf '{\"score\":1}' >&2", false),
        ("printf '{\"score\":1}'; exit 1", false),
        ("printf '{\"score\":1}'; head -c 1000 /dev/zero", false),
        ("printf '{\"score\":1}'; head -c 1000 /dev/zero >&2", true),
        ("printf '{\"score\":1,\"score\":0}'", false),
    ] {
        let mut config = config(&["/bin/sh", "-c", script]);
        config.result_contract = tachyon_api::campaign::ResultContract::JsonMetrics;
        config.metrics = serde_json::from_value(serde_json::json!({"score":{"min":1}})).unwrap();
        let result = evaluate_command(
            store.clone(),
            staging.path().into(),
            config,
            candidate(),
            snapshot.clone(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(
            result.outcome,
            if pass {
                CommandOutcome::Pass
            } else {
                CommandOutcome::Fail
            },
            "{script}: {result:?}"
        );
        assert!(result.stdout.len() + result.stderr.len() <= 1024);
        assert!(result.diagnostic.len() <= 256);
    }
}

#[tokio::test]
async fn command_uses_ready_bytes_not_changed_workspace_and_bounds_evidence() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    for dir in [&root, &staging] {
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let store = Arc::new(ArtifactStore::open(root.path()).unwrap());
    let snapshot = snapshot(&store, workspace.path());
    std::fs::write(workspace.path().join("input"), "changed").unwrap();
    let command = config(&["/bin/sh", "-c", "test \"$(cat candidate)\" = original && test -z \"${OPENROUTER_API_KEY}${AWS_SECRET_ACCESS_KEY}${SSH_AUTH_SOCK}\"; result=$?; head -c 100000 /dev/zero; head -c 100000 /dev/zero >&2; exit $result"]);
    let result = evaluate_command(
        store,
        staging.path().into(),
        command.clone(),
        candidate(),
        snapshot.clone(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(result.outcome, CommandOutcome::Pass, "{result:?}");
    assert_eq!(result.config_hash, command.config_hash().unwrap());
    assert_eq!(result.candidate_sha256, snapshot.sha256);
    assert!(result.truncated);
    assert_eq!(result.stdout.len() + result.stderr.len(), 1024);
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::fs::read_dir(staging.path()).unwrap().count() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn command_rejects_malformed_identity_and_unsafe_staging_before_spawn() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    for dir in [&root, &staging] {
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let store = Arc::new(ArtifactStore::open(root.path()).unwrap());
    let snapshot = snapshot(&store, workspace.path());
    for case in 0..3 {
        let mut bad = snapshot.clone();
        match case {
            0 => bad.sha256 = "G".repeat(64),
            1 => bad.sha256 = snapshot.sha256.to_uppercase(),
            _ => bad.id.clear(),
        }
        assert!(evaluate_command(
            store.clone(),
            staging.path().into(),
            config(&["/bin/true"]),
            candidate(),
            bad,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .is_err());
    }
    let alias = workspace.path().join("alias");
    std::os::unix::fs::symlink(staging.path(), &alias).unwrap();
    let file = workspace.path().join("not-a-directory");
    std::fs::write(&file, "fixture").unwrap();
    for path in [alias, file] {
        let result = evaluate_command(
            store.clone(),
            path,
            config(&["/bin/true"]),
            candidate(),
            snapshot.clone(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, CommandOutcome::Unverified);
    }
    std::fs::set_permissions(staging.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let result = evaluate_command(
        store,
        staging.path().into(),
        config(&["/bin/true"]),
        candidate(),
        snapshot,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(result.outcome, CommandOutcome::Unverified);
    assert_eq!(std::fs::read_dir(staging.path()).unwrap().count(), 0);
}

#[test]
fn ready_copy_rejects_symlink_destination_and_changed_registration() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = ArtifactStore::open(root.path()).unwrap();
    let snapshot = snapshot(&store, workspace.path());
    let target = staging.path().join("candidate");
    std::os::unix::fs::symlink(workspace.path().join("input"), &target).unwrap();
    assert!(store.copy_ready("work", &snapshot, &target, 1024).is_err());
    assert_eq!(
        std::fs::read(workspace.path().join("input")).unwrap(),
        b"original"
    );
    let target = staging.path().join("fresh");
    assert!(store.copy_ready("work", &snapshot, &target, 7).is_err());
    let mut forged = snapshot.clone();
    forged.path = "other".into();
    assert!(store.copy_ready("work", &forged, &target, 1024).is_err());
    assert!(!target.exists());
    store.copy_ready("work", &snapshot, &target, 8).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"original");
    assert_eq!(
        std::fs::metadata(target).unwrap().permissions().mode() & 0o777,
        0o400
    );
}

#[tokio::test]
async fn command_fail_spawn_timeout_and_untrusted_refs_never_pass() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    for dir in [&root, &staging] {
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let store = Arc::new(ArtifactStore::open(root.path()).unwrap());
    let snapshot = snapshot(&store, workspace.path());
    for (argv, outcome) in [
        (vec!["/bin/false"], CommandOutcome::Fail),
        (vec!["/missing/evaluator"], CommandOutcome::SpawnFailure),
        (
            vec!["/bin/sh", "-c", "trap '' TERM; sleep 30 & wait"],
            CommandOutcome::Timeout,
        ),
    ] {
        let mut command = config(&argv);
        command.timeout_ms = 100;
        let result = evaluate_command(
            store.clone(),
            staging.path().into(),
            command,
            candidate(),
            snapshot.clone(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        if outcome == CommandOutcome::Timeout {
            // A killed descendant can remain unreaped by the host's init.
            assert!(
                matches!(
                    result.outcome,
                    CommandOutcome::Timeout | CommandOutcome::Unverified
                ),
                "{result:?}"
            );
            if result.outcome == CommandOutcome::Unverified {
                assert_eq!(result.diagnostic, "process cleanup unknown");
            }
        } else {
            assert_eq!(result.outcome, outcome, "{result:?}");
        }
        assert!(result.elapsed_ms < 1500);
    }
    for case in 0..5 {
        let mut forged = snapshot.clone();
        match case {
            0 => forged.generation = Some(2),
            1 => forged.sha256 = "0".repeat(64),
            2 => forged.publication = ArtifactPublication::Pending,
            3 => forged.work_id = Some("other".into()),
            _ => forged.id = "../input".into(),
        }
        let result = evaluate_command(
            store.clone(),
            staging.path().into(),
            config(&["/bin/true"]),
            candidate(),
            forged,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, CommandOutcome::Unverified);
    }
    for refs in [
        None,
        Some(vec![]),
        Some(vec!["input".into()]),
        Some(vec![snapshot.id.clone(), "extra".into()]),
    ] {
        let mut candidate = candidate();
        candidate.candidate_refs = refs;
        if let WorkOutcome::Completed { artifacts, .. } = &mut candidate.outcome {
            *artifacts = vec![snapshot.id.clone()];
        }
        let result = evaluate_command(
            store.clone(),
            staging.path().into(),
            config(&["/bin/true"]),
            candidate,
            snapshot.clone(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, CommandOutcome::Unverified);
        assert!(result.exit_code.is_none());
    }
}

#[test]
fn config_bounds_and_hash_cover_all_authority() {
    let base = config(&["/bin/true"]);
    for case in 0..7 {
        let mut changed = base.clone();
        match case {
            0 => changed.argv.push("different".into()),
            1 => changed.cwd = "subdir".into(),
            2 => changed.timeout_ms -= 1,
            3 => changed.output_bytes -= 1,
            4 => changed.input_bytes -= 1,
            5 => changed.max_attempts = 1,
            _ => changed.max_total_command_ms += 1,
        }
        assert_ne!(base.config_hash().unwrap(), changed.config_hash().unwrap());
    }
    for case in 0..12 {
        let mut bad = base.clone();
        match case {
            0 => bad.argv[0] = "relative".into(),
            1 => bad.cwd = "../workspace".into(),
            2 => bad.timeout_ms = 0,
            3 => bad.output_bytes = usize::MAX,
            4 => bad.input_bytes = u64::MAX,
            5 => bad.max_attempts = 9,
            6 => bad.max_total_command_ms = 1,
            7 => bad.output_bytes = 0,
            8 => bad.input_bytes = 0,
            9 => bad.max_attempts = 0,
            10 => bad.cwd = "/absolute".into(),
            _ => bad.argv.push("nul\0argument".into()),
        }
        assert!(bad.config_hash().is_err());
    }
}

#[tokio::test]
async fn command_environment_and_dropped_future_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    for dir in [&root, &staging] {
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let store = Arc::new(ArtifactStore::open(root.path()).unwrap());
    let snapshot = snapshot(&store, workspace.path());
    let env = evaluate_command(
        store.clone(),
        staging.path().into(),
        config(&["/usr/bin/env"]),
        candidate(),
        snapshot.clone(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(env.outcome, CommandOutcome::Pass);
    let output = String::from_utf8(env.stdout).unwrap();
    assert_eq!(output.lines().count(), 3);
    assert!(output.lines().all(|line| ["PATH=", "HOME=", "LANG="]
        .iter()
        .any(|key| line.starts_with(key))));
    let mut nested = config(&["/bin/sh", "-c", "test -f candidate && /usr/bin/env"]);
    nested.cwd = "nested/work".into();
    let second = evaluate_command(
        store.clone(),
        staging.path().into(),
        nested,
        candidate(),
        snapshot.clone(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(second.outcome, CommandOutcome::Pass);
    let second = String::from_utf8(second.stdout).unwrap();
    assert_ne!(
        output.lines().find(|line| line.starts_with("HOME=")),
        second.lines().find(|line| line.starts_with("HOME=")),
    );
    let pid_file = workspace.path().join("pid");
    let command = config(&[
        "/bin/sh",
        "-c",
        "echo $$ > \"$1\"; exec /bin/sleep 30",
        "fixture",
        pid_file.to_str().unwrap(),
    ]);
    let mut run = Box::pin(evaluate_command(
        store,
        staging.path().into(),
        command,
        candidate(),
        snapshot,
        Instant::now() + Duration::from_secs(5),
    ));
    let pid = tokio::select! {
        result = &mut run => panic!("process ended before cancellation: {result:?}"),
        pid = async {
            loop {
                if let Ok(text) = tokio::fs::read_to_string(&pid_file).await {
                    if let Ok(pid) = text.trim().parse::<i32>() { break pid; }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        } => pid,
    };
    drop(run);
    tokio::time::timeout(Duration::from_secs(2), async {
        while nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
            != Err(nix::errno::Errno::ESRCH)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::fs::read_dir(staging.path()).unwrap().count() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn unknown_command_outcome_is_not_a_failure_or_retry_signal() {
    assert!(serde_json::from_str::<CommandOutcome>("\"Unknown\"").is_err());
}

#[test]
fn cleanup_requires_observed_group_exit_not_just_a_kill_signal() {
    use std::os::unix::process::CommandExt;
    let mut child = std::process::Command::new("/bin/sleep")
        .arg("30")
        .process_group(0)
        .spawn()
        .unwrap();
    let mut guard = ProcessGroupGuard(Some(child.id()));
    assert!(!group_gone(child.id()));
    child.kill().unwrap();
    assert!(
        !group_gone(child.id()),
        "an unreaped process is not cleanup proof"
    );
    child.wait().unwrap();
    assert!(group_gone(child.id()));
    guard.0 = None;
}
