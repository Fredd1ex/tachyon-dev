//! Explicit host command gate over one exact ArtifactStore snapshot.
//! Native execution is NOT a sandbox. Only trusted evaluators may be configured;
//! candidate data must not be executed, sourced, or used as command configuration.
use crate::artifact_store::ArtifactStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tachyon_api::types::{ArtifactRegistration, WorkOutcome, WorkResult};
use tachyon_util::process::{cleanup_remaining_group, terminate_process_group, ProcessGroupGuard};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    time::Instant,
};

#[cfg(test)]
mod tests;

/// Host-owned executable and arguments, never interpolated from model output.
/// `candidate` in cwd contains the verified bytes. Environment is cleared; no
/// credentials, inherited HOME, shell startup files, or workspace are supplied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandEvaluator {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_mode: Option<tachyon_api::campaign::AcceptanceMode>,
    #[serde(
        default,
        skip_serializing_if = "tachyon_api::campaign::ResultContract::is_default"
    )]
    pub result_contract: tachyon_api::campaign::ResultContract,
    #[serde(
        default,
        skip_serializing_if = "std::collections::BTreeMap::is_empty",
        deserialize_with = "tachyon_api::campaign::deserialize_metric_constraints"
    )]
    pub metrics: std::collections::BTreeMap<String, tachyon_api::campaign::MetricBounds>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_extra_metrics: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<tachyon_api::campaign::EvaluationStage>,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub timeout_ms: u64,
    pub output_bytes: usize,
    pub input_bytes: u64,
    /// Includes the initial attempt. Only the explicit host loop may continue;
    /// the one-shot wrapper retains rejected Work as pending.
    pub max_attempts: u32,
    pub max_total_command_ms: u64,
}

impl CommandEvaluator {
    pub fn config_hash(&self) -> Result<String, String> {
        let human = self.acceptance_mode == Some(tachyon_api::campaign::AcceptanceMode::Human);
        if human
            && (!self.argv.is_empty()
                || self.timeout_ms != 0
                || self.output_bytes != 0
                || self.max_total_command_ms != 0
                || self.max_attempts != 1
                || self.stage.is_some()
                || self.result_contract != tachyon_api::campaign::ResultContract::ExitSuccess
                || !self.metrics.is_empty()
                || self.allow_extra_metrics
                || self.cwd != Path::new("."))
        {
            return Err("human acceptance forbids executable evaluation policy".into());
        }
        self.result_contract
            .validate(&self.metrics, self.allow_extra_metrics)?;
        if self.stage == Some(tachyon_api::campaign::EvaluationStage::FinalHeldout)
            && self.max_attempts != 1
        {
            return Err("final_heldout requires max_attempts=1".into());
        }
        if (!human
            && (self.argv.is_empty()
                || self.argv.len() > 256
                || !Path::new(&self.argv[0]).is_absolute()
                || self.argv.iter().any(|s| s.contains('\0'))
                || self.argv.iter().map(String::len).sum::<usize>() > 32768))
            || (self.cwd != Path::new(".")
                && (self.cwd.as_os_str().is_empty()
                    || self
                        .cwd
                        .components()
                        .any(|p| !matches!(p, Component::Normal(_)))))
            || self.cwd.as_os_str().len() > 4096
            || (!human
                && (!(1..=300_000).contains(&self.timeout_ms)
                    || !(1..=32768).contains(&self.output_bytes)))
            || !(1..=16 * 1024 * 1024).contains(&self.input_bytes)
            || !(1..=8).contains(&self.max_attempts)
            || self.max_total_command_ms
                < self.timeout_ms.saturating_mul(u64::from(self.max_attempts))
            || self.max_total_command_ms > 2_400_000
        {
            return Err("invalid host command evaluator bounds".into());
        }
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).map_err(|e| e.to_string())?)
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandOutcome {
    Pass,
    Fail,
    Timeout,
    SpawnFailure,
    Unverified,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandEvidence {
    pub config_hash: String,
    pub candidate_sha256: String,
    pub artifact_id: String,
    pub outcome: CommandOutcome,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub truncated: bool,
    /// Fixed host diagnostic, never an unbounded OS error or environment value.
    pub diagnostic: String,
}

struct Staging(Option<tempfile::TempDir>);

impl Staging {
    fn path(&self) -> &Path {
        self.0.as_ref().unwrap().path()
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if let Some(dir) = self.0.take() {
            // A trusted evaluator may create build output. Recursive deletion
            // must not run on a Tokio event thread, including future cancellation.
            tokio::task::spawn_blocking(move || drop(dir));
        }
    }
}

async fn capture(
    mut stream: impl AsyncRead + Unpin,
    cap: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut bytes = Vec::new();
    let mut truncated = false;
    let mut buffer = [0; 8192];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Ok((bytes, truncated));
        }
        let keep = n.min(cap.saturating_sub(bytes.len()));
        bytes.extend_from_slice(&buffer[..keep]);
        truncated |= keep < n;
    }
}

fn group_gone(pid: u32) -> bool {
    // Sending SIGKILL is not evidence that all group members have exited.
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(-(pid as i32)), None)
        == Err(nix::errno::Errno::ESRCH)
}

/// The caller must reserve protected verification allowance before calling.
/// Acceptance describes only these copied bytes under this host configuration.
/// Filesystem checks/copies run off Tokio's event threads. No workspace fallback.
pub async fn evaluate_command(
    store: Arc<ArtifactStore>,
    staging_root: PathBuf,
    config: CommandEvaluator,
    candidate: WorkResult,
    snapshot: ArtifactRegistration,
    deadline: Instant,
) -> Result<CommandEvidence, String> {
    if config.acceptance_mode.is_some() {
        return Err("human acceptance cannot execute a command".into());
    }
    let config_hash = config.config_hash()?;
    if snapshot.sha256.len() != 64
        || !snapshot
            .sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || snapshot.id.is_empty()
        || snapshot.id.len() > 256
    {
        return Err("invalid bounded snapshot identity".into());
    }
    let started = Instant::now();
    let deadline = deadline.min(started + Duration::from_millis(config.timeout_ms));
    let mut evidence = CommandEvidence {
        config_hash,
        candidate_sha256: snapshot.sha256.clone(),
        artifact_id: snapshot.id.clone(),
        outcome: CommandOutcome::Unverified,
        exit_code: None,
        elapsed_ms: 0,
        stdout: vec![],
        stderr: vec![],
        truncated: false,
        diagnostic: String::new(),
    };
    if snapshot.work_id.as_deref() != Some(candidate.work_id.as_str())
        || snapshot.generation != Some(candidate.generation)
        || snapshot.assignment != Some(candidate.assignment)
        || candidate
            .attempt_id
            .as_ref()
            .is_some_and(|id| snapshot.attempt_id.as_ref() != Some(id))
        || !matches!(&candidate.outcome, WorkOutcome::Completed { .. })
        || candidate.candidate_refs.as_deref() != Some(std::slice::from_ref(&snapshot.id))
    {
        evidence.diagnostic = "candidate does not identify the exact scoped artifact".into();
        return Ok(evidence);
    }
    let cwd = config.cwd.clone();
    let cap = config.input_bytes;
    let staging = tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let meta = std::fs::symlink_metadata(&staging_root).map_err(|e| e.to_string())?;
        if !staging_root.is_absolute()
            || staging_root.canonicalize().map_err(|e| e.to_string())? != staging_root
            || !meta.is_dir()
            || meta.uid() != nix::unistd::Uid::effective().as_raw()
            || meta.mode() & 0o077 != 0
        {
            return Err("staging root must be host-owned canonical mode 0700".to_string());
        }
        let dir = tempfile::Builder::new()
            .prefix("verification-")
            .tempdir_in(staging_root)
            .map_err(|e| e.to_string())?;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(|e| e.to_string())?;
        let cwd = dir.path().join(cwd);
        std::fs::create_dir_all(&cwd).map_err(|e| e.to_string())?;
        store.copy_ready(&candidate.work_id, &snapshot, &cwd.join("candidate"), cap)?;
        Ok::<_, String>((Staging(Some(dir)), cwd))
    });
    let (dir, cwd) = match tokio::time::timeout_at(deadline, staging).await {
        Ok(Ok(Ok(staging))) => staging,
        result => {
            evidence.outcome = if result.is_err() {
                CommandOutcome::Timeout
            } else {
                CommandOutcome::Unverified
            };
            evidence.diagnostic = "snapshot staging unavailable or deadline exceeded".into();
            evidence.elapsed_ms = started.elapsed().as_millis() as u64;
            return Ok(evidence);
        }
    };
    if Instant::now() >= deadline {
        evidence.outcome = CommandOutcome::Timeout;
        return Ok(evidence);
    }
    let mut command = tokio::process::Command::new(&config.argv[0]);
    command
        .args(&config.argv[1..])
        .current_dir(cwd)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("LANG", "C.UTF-8")
        .env("HOME", dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            evidence.outcome = CommandOutcome::SpawnFailure;
            evidence.diagnostic = "host evaluator spawn failed".into();
            evidence.elapsed_ms = started.elapsed().as_millis() as u64;
            return Ok(evidence);
        }
    };
    let pid = child.id().ok_or("spawned evaluator has no pid")?;
    let mut guard = ProcessGroupGuard(Some(pid));
    let stdout = child.stdout.take().ok_or("missing stdout")?;
    let stderr = child.stderr.take().ok_or("missing stderr")?;
    let grace = Duration::from_millis(100);
    // Readers live in this future, not detached tasks; dropping it kills the group.
    let run = async {
        let (status, out, err) = tokio::try_join!(
            child.wait(),
            capture(stdout, config.output_bytes / 2),
            capture(stderr, config.output_bytes - config.output_bytes / 2)
        )?;
        Ok::<_, std::io::Error>((status, out, err))
    };
    match tokio::time::timeout_at(deadline, run).await {
        Ok(Ok((status, (out, out_truncated), (err, err_truncated)))) => {
            evidence.exit_code = status.code();
            evidence.stdout = out;
            evidence.stderr = err;
            evidence.truncated = out_truncated || err_truncated;
            if cleanup_remaining_group(pid, grace).await.is_ok() && group_gone(pid) {
                guard.0 = None;
                evidence.outcome = if status.success() {
                    CommandOutcome::Pass
                } else {
                    CommandOutcome::Fail
                };
                if status.success()
                    && config.result_contract == tachyon_api::campaign::ResultContract::JsonMetrics
                {
                    match evaluate_metrics(&config, &evidence.stdout, out_truncated) {
                        Ok(()) => evidence.diagnostic = "host json_metrics constraints satisfied; local measurement, not a correctness proof".into(),
                        Err(reason) => {
                            evidence.outcome = CommandOutcome::Fail;
                            evidence.diagnostic = reason;
                        }
                    }
                }
            } else {
                evidence.diagnostic = "process cleanup unknown".into();
            }
        }
        Err(_) => {
            evidence.outcome = CommandOutcome::Timeout;
            evidence.diagnostic = "command wall limit exceeded; partial output discarded".into();
            if terminate_process_group(&mut child, pid, grace)
                .await
                .is_ok()
                && group_gone(pid)
            {
                guard.0 = None;
            } else {
                evidence.outcome = CommandOutcome::Unverified;
                evidence.diagnostic = "process cleanup unknown".into();
            }
        }
        Ok(Err(_)) => {
            evidence.diagnostic = "process or output collection failed".into();
            if terminate_process_group(&mut child, pid, grace)
                .await
                .is_ok()
                && group_gone(pid)
            {
                guard.0 = None;
            } else {
                evidence.diagnostic = "process cleanup unknown".into();
            }
        }
    }
    evidence.elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(evidence)
}

fn evaluate_metrics(
    config: &CommandEvaluator,
    stdout: &[u8],
    truncated: bool,
) -> Result<(), String> {
    use serde::de::{MapAccess, Visitor};
    use std::collections::BTreeMap;
    struct Metrics;
    impl<'de> Visitor<'de> for Metrics {
        type Value = BTreeMap<String, f64>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("one object of unique finite numeric metrics")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut metrics = BTreeMap::new();
            while let Some((name, value)) = map.next_entry::<String, f64>()? {
                if !value.is_finite() || metrics.insert(name, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate or nonfinite metric"));
                }
            }
            Ok(metrics)
        }
    }
    if truncated {
        return Err("json_metrics stdout exceeded its capture bound".into());
    }
    let mut parser = serde_json::Deserializer::from_slice(stdout);
    let values = serde::de::Deserializer::deserialize_map(&mut parser, Metrics)
        .and_then(|values| parser.end().map(|()| values))
        .map_err(|_| "json_metrics requires exactly one JSON object with unique finite numeric values; no trailing data".to_string())?;
    for (name, bounds) in &config.metrics {
        let value = values
            .get(name)
            .ok_or_else(|| format!("missing required metric {name}"))?;
        if bounds
            .min
            .as_ref()
            .is_some_and(|min| *value < min.as_f64().unwrap())
        {
            return Err(format!("metric {name} violated inclusive min"));
        }
        if bounds
            .max
            .as_ref()
            .is_some_and(|max| *value > max.as_f64().unwrap())
        {
            return Err(format!("metric {name} violated inclusive max"));
        }
    }
    if !config.allow_extra_metrics && values.keys().any(|name| !config.metrics.contains_key(name)) {
        return Err("json_metrics contains an unconfigured metric".into());
    }
    Ok(())
}
