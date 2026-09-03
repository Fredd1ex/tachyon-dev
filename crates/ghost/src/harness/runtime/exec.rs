#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

use super::path::resolve_existing;
use super::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult,
};

const CAPABILITIES: &[Capability] = &[Capability::ExecuteProcess];
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

pub struct ExecTool {
    schema: ToolSpec,
}

impl Default for ExecTool {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecTool {
    pub fn new() -> Self {
        Self {
            schema: ToolSpec::new(
                "exec",
                "Run a bounded process in the workspace. Prefer argv for direct execution; command explicitly invokes the configured non-login shell.",
                json!({
                    "type": "object",
                    "properties": {
                        "argv": {
                            "type": "array",
                            "items": { "type": "string" },
                            "minItems": 1,
                            "maxItems": 256
                        },
                        "command": { "type": "string" },
                        "cwd": { "type": "string", "default": "." },
                        "timeout_ms": { "type": "integer", "minimum": 1 }
                    },
                    "oneOf": [
                        { "required": ["argv"], "not": { "required": ["command"] } },
                        { "required": ["command"], "not": { "required": ["argv"] } }
                    ],
                    "additionalProperties": false
                }),
            ),
        }
    }
}

impl Tool for ExecTool {
    fn name(&self) -> &'static str {
        "exec"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn manages_own_lifecycle(&self) -> bool {
        true
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: ExecInput = decode_input(input)?;
            let invocation = invocation(context, &input)?;
            let requested_cwd = input.cwd.unwrap_or_else(|| ".".into());
            let cwd = resolve_existing(context, &requested_cwd).await?;
            if !tokio::fs::metadata(&cwd).await.map_err(io_error)?.is_dir() {
                return Err(ToolError::invalid("exec cwd is not a directory"));
            }

            let requested_timeout = match input.timeout_ms {
                Some(0) => return Err(ToolError::invalid("timeout_ms must be greater than zero")),
                Some(milliseconds) => Duration::from_millis(milliseconds),
                None => DEFAULT_TIMEOUT,
            };
            let now = Instant::now();
            let deadline_remaining = context.deadline.saturating_duration_since(now);
            let timeout = requested_timeout
                .min(context.policy.max_exec_duration)
                .min(context.policy.max_duration)
                .min(deadline_remaining);
            if timeout.is_zero() || context.cancellation.is_cancelled() {
                return Err(ToolError::new(
                    if context.cancellation.is_cancelled() {
                        ToolErrorCode::Cancelled
                    } else {
                        ToolErrorCode::Timeout
                    },
                    "exec cancelled before process spawn",
                    true,
                ));
            }

            let started = Instant::now();
            let mut command = invocation.command(context);
            command
                .current_dir(&cwd)
                .env_clear()
                .env("PATH", &context.policy.exec_path)
                .env("HOME", &context.workspace_root)
                .env("TACHYON_JAILED", "1")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            for (name, value) in &context.policy.exec_env {
                command.env(name, value);
            }
            configure_process_group(&mut command);

            let mut child = command.spawn().map_err(|error| {
                ToolError::new(
                    ToolErrorCode::ProcessFailed,
                    format!("failed to spawn process: {error}"),
                    false,
                )
            })?;
            let process_id = child.id().ok_or_else(|| {
                ToolError::new(
                    ToolErrorCode::Internal,
                    "spawned process has no process id",
                    false,
                )
            })?;
            let stdout = child.stdout.take().ok_or_else(|| {
                ToolError::new(ToolErrorCode::Internal, "stdout pipe unavailable", false)
            })?;
            let stderr = child.stderr.take().ok_or_else(|| {
                ToolError::new(ToolErrorCode::Internal, "stderr pipe unavailable", false)
            })?;
            let stream_cap = context.policy.max_exec_output_bytes / 2;
            let stdout_task = tokio::spawn(capture_stream(stdout, stream_cap));
            let stderr_task = tokio::spawn(capture_stream(stderr, stream_cap));
            let deadline = tokio::time::Instant::now() + timeout;

            let (status, termination) = tokio::select! {
                _ = context.cancellation.cancelled() => {
                    let status = terminate_process_group(
                        &mut child,
                        process_id,
                        context.policy.exec_term_grace,
                    ).await?;
                    (status, Termination::Cancelled)
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let status = terminate_process_group(
                        &mut child,
                        process_id,
                        context.policy.exec_term_grace,
                    ).await?;
                    (status, Termination::Timeout)
                }
                status = child.wait() => {
                    let status = status.map_err(io_error)?;
                    cleanup_remaining_group(process_id, context.policy.exec_term_grace).await?;
                    (status, Termination::Completed)
                }
            };

            let stdout = finish_capture(stdout_task, context.policy.exec_term_grace).await?;
            let stderr = finish_capture(stderr_task, context.policy.exec_term_grace).await?;
            let output_truncated = stdout.truncated || stderr.truncated;
            let content = render_output(&stdout, &stderr);
            let exit_code = status.code();
            let is_error = termination != Termination::Completed || !status.success();
            let error_code = if termination == Termination::Timeout {
                Some(ToolErrorCode::Timeout)
            } else if termination == Termination::Cancelled {
                Some(ToolErrorCode::Cancelled)
            } else if !status.success() {
                Some(ToolErrorCode::ProcessFailed)
            } else {
                None
            };
            let mut result = ToolResult::success(
                content,
                json!({
                    "invocation": invocation.summary(),
                    "cwd": requested_cwd,
                    "duration_ms": started.elapsed().as_millis() as u64,
                    "exit_code": exit_code,
                    "termination": termination.as_str(),
                    "error_code": error_code,
                    "stdout_bytes": stdout.total_bytes,
                    "stderr_bytes": stderr.total_bytes,
                    "stdout_truncated": stdout.truncated,
                    "stderr_truncated": stderr.truncated,
                    "process_group_cleanup": process_group_cleanup_mode(),
                }),
            );
            result.is_error = is_error;
            result.truncated = output_truncated;
            Ok(result)
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecInput {
    argv: Option<Vec<String>>,
    command: Option<String>,
    cwd: Option<String>,
    timeout_ms: Option<u64>,
}

enum Invocation {
    Direct(Vec<String>),
    Shell(String),
}

impl Invocation {
    fn command(&self, context: &ToolContext) -> Command {
        match self {
            Self::Direct(argv) => {
                let mut command = Command::new(&argv[0]);
                command.args(&argv[1..]);
                command
            }
            Self::Shell(script) => {
                let mut command = Command::new(&context.policy.exec_shell);
                command.arg("-c").arg(script);
                command
            }
        }
    }

    fn summary(&self) -> Value {
        match self {
            Self::Direct(argv) => json!({"mode":"direct", "argv":argv}),
            Self::Shell(script) => json!({"mode":"shell", "command":script}),
        }
    }
}

fn invocation(context: &ToolContext, input: &ExecInput) -> Result<Invocation, ToolError> {
    let invocation = match (&input.argv, &input.command) {
        (Some(argv), None) if !argv.is_empty() => Invocation::Direct(argv.clone()),
        (None, Some(command)) if context.policy.allow_shell_exec => {
            Invocation::Shell(command.clone())
        }
        (None, Some(_)) => {
            return Err(ToolError::new(
                ToolErrorCode::PermissionDenied,
                "shell execution is disabled by policy",
                false,
            ));
        }
        _ => return Err(ToolError::invalid("provide exactly one of argv or command")),
    };
    let bytes = match &invocation {
        Invocation::Direct(argv) => {
            if argv.len() > 256 || argv.iter().any(|argument| argument.contains('\0')) {
                return Err(ToolError::invalid(
                    "argv is invalid or exceeds 256 arguments",
                ));
            }
            argv.iter().map(String::len).sum()
        }
        Invocation::Shell(command) => {
            if command.is_empty() || command.contains('\0') {
                return Err(ToolError::invalid(
                    "command must be non-empty and contain no NUL",
                ));
            }
            command.len()
        }
    };
    if bytes > context.policy.max_exec_command_bytes {
        return Err(ToolError::invalid(format!(
            "command exceeds the {} byte limit",
            context.policy.max_exec_command_bytes
        )));
    }
    Ok(invocation)
}

#[derive(Debug)]
struct CapturedStream {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    total_bytes: u64,
    truncated: bool,
}

async fn capture_stream(
    mut stream: impl AsyncRead + Unpin,
    cap: usize,
) -> std::io::Result<CapturedStream> {
    let head_cap = cap * 3 / 4;
    let tail_cap = cap.saturating_sub(head_cap);
    let mut capture = CapturedStream {
        head: Vec::with_capacity(head_cap.min(64 * 1024)),
        tail: VecDeque::with_capacity(tail_cap.min(64 * 1024)),
        total_bytes: 0,
        truncated: false,
    };
    let mut buffer = [0_u8; 8192];
    loop {
        let count = stream.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        capture.total_bytes = capture.total_bytes.saturating_add(count as u64);
        let mut offset = 0;
        if capture.head.len() < head_cap {
            let take = count.min(head_cap - capture.head.len());
            capture.head.extend_from_slice(&buffer[..take]);
            offset = take;
        }
        for byte in &buffer[offset..count] {
            if tail_cap == 0 {
                capture.truncated = true;
                continue;
            }
            if capture.tail.len() == tail_cap {
                capture.tail.pop_front();
                capture.truncated = true;
            }
            capture.tail.push_back(*byte);
        }
    }
    capture.truncated |= capture.total_bytes > cap as u64;
    Ok(capture)
}

fn render_output(stdout: &CapturedStream, stderr: &CapturedStream) -> String {
    let mut output = String::new();
    append_stream(&mut output, "stdout", stdout);
    append_stream(&mut output, "stderr", stderr);
    output
}

fn append_stream(output: &mut String, name: &str, stream: &CapturedStream) {
    if stream.total_bytes == 0 {
        return;
    }
    if !output.is_empty() {
        output.push('\n');
    }
    output.push_str(&format!("[{name}]\n"));
    output.push_str(&String::from_utf8_lossy(&stream.head));
    if stream.truncated {
        let retained = stream.head.len() + stream.tail.len();
        let omitted = stream.total_bytes.saturating_sub(retained as u64);
        output.push_str(&format!("\n[... {omitted} bytes omitted ...]\n"));
    }
    let tail = stream.tail.iter().copied().collect::<Vec<_>>();
    output.push_str(&String::from_utf8_lossy(&tail));
}

async fn finish_capture(
    mut task: JoinHandle<std::io::Result<CapturedStream>>,
    grace: Duration,
) -> Result<CapturedStream, ToolError> {
    tokio::select! {
        result = &mut task => result
            .map_err(join_error)?
            .map_err(io_error),
        _ = tokio::time::sleep(grace.max(Duration::from_millis(100))) => {
            task.abort();
            let _ = task.await;
            Err(ToolError::new(
                ToolErrorCode::ProcessFailed,
                "process output pipes remained open after cleanup",
                true,
            ))
        }
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.as_std_mut().process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
async fn terminate_process_group(
    child: &mut Child,
    process_id: u32,
    grace: Duration,
) -> Result<ExitStatus, ToolError> {
    signal_group(process_id, nix::sys::signal::Signal::SIGTERM)?;
    match tokio::time::timeout(grace, child.wait()).await {
        Ok(status) => status.map_err(io_error),
        Err(_) => {
            signal_group(process_id, nix::sys::signal::Signal::SIGKILL)?;
            tokio::time::timeout(grace.max(Duration::from_millis(100)), child.wait())
                .await
                .map_err(|_| cleanup_error("process did not exit after SIGKILL"))?
                .map_err(io_error)
        }
    }
}

#[cfg(not(unix))]
async fn terminate_process_group(
    child: &mut Child,
    _process_id: u32,
    grace: Duration,
) -> Result<ExitStatus, ToolError> {
    child.start_kill().map_err(io_error)?;
    tokio::time::timeout(grace.max(Duration::from_millis(100)), child.wait())
        .await
        .map_err(|_| cleanup_error("process did not exit after kill"))?
        .map_err(io_error)
}

#[cfg(unix)]
async fn cleanup_remaining_group(process_id: u32, grace: Duration) -> Result<(), ToolError> {
    use nix::errno::Errno;
    use nix::sys::signal::{kill, killpg, Signal};
    use nix::unistd::Pid;

    let group = Pid::from_raw(process_id as i32);
    match killpg(group, Signal::SIGTERM) {
        Ok(()) => {}
        Err(Errno::ESRCH) => return Ok(()),
        Err(error) => {
            return Err(cleanup_error(&format!(
                "failed to terminate descendants: {error}"
            )))
        }
    }
    let deadline = Instant::now() + grace;
    loop {
        match kill(Pid::from_raw(-(process_id as i32)), None) {
            Err(Errno::ESRCH) => return Ok(()),
            Ok(()) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(()) => break,
            Err(error) => {
                return Err(cleanup_error(&format!(
                    "failed to inspect descendants: {error}"
                )))
            }
        }
    }
    signal_group(process_id, Signal::SIGKILL)
}

#[cfg(not(unix))]
async fn cleanup_remaining_group(_process_id: u32, _grace: Duration) -> Result<(), ToolError> {
    Ok(())
}

#[cfg(unix)]
fn signal_group(process_id: u32, signal: nix::sys::signal::Signal) -> Result<(), ToolError> {
    use nix::errno::Errno;
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;

    match killpg(Pid::from_raw(process_id as i32), signal) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(cleanup_error(&format!(
            "failed to signal process group: {error}"
        ))),
    }
}

#[cfg(unix)]
fn process_group_cleanup_mode() -> &'static str {
    "term_then_kill_process_group"
}

#[cfg(not(unix))]
fn process_group_cleanup_mode() -> &'static str {
    "direct_child_only"
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Termination {
    Completed,
    Timeout,
    Cancelled,
}

impl Termination {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
        }
    }
}

fn io_error(error: std::io::Error) -> ToolError {
    ToolError::new(ToolErrorCode::ProcessFailed, error.to_string(), false)
}

fn join_error(error: tokio::task::JoinError) -> ToolError {
    ToolError::new(
        ToolErrorCode::Internal,
        format!("process output task failed: {error}"),
        true,
    )
}

fn cleanup_error(message: &str) -> ToolError {
    ToolError::new(ToolErrorCode::ProcessFailed, message, true)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::harness::runtime::{
        InMemoryOutputStore, NoopEventSink, NoopOutputStore, ToolIdentity, ToolOutputStore,
        ToolPolicy, ToolRegistry,
    };

    fn context(root: &Path) -> ToolContext {
        let root = root.canonicalize().unwrap();
        let mut policy = ToolPolicy::worker_default(root.clone());
        policy.exec_term_grace = Duration::from_millis(100);
        policy.max_exec_duration = Duration::from_secs(3);
        ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(5),
            cancellation: CancellationToken::new(),
            policy: Arc::new(policy),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
        }
    }

    #[tokio::test]
    async fn direct_and_shell_execution_report_success_and_nonzero_exit() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let tool = ExecTool::new();
        let direct = tool
            .execute(&context, json!({"argv":["/bin/printf", "direct"]}))
            .await
            .unwrap();
        assert!(!direct.is_error);
        assert!(direct.content.contains("direct"));
        assert_eq!(direct.metadata["invocation"]["mode"], "direct");

        let shell = tool
            .execute(
                &context,
                json!({"command":"printf shell | tr a-z A-Z; exit 7"}),
            )
            .await
            .unwrap();
        assert!(shell.is_error);
        assert!(shell.content.contains("SHELL"));
        assert_eq!(shell.metadata["exit_code"], 7);
        assert_eq!(shell.metadata["error_code"], "process_failed");
    }

    #[tokio::test]
    async fn drains_concurrent_streams_and_bounds_large_output() {
        let workspace = tempdir().unwrap();
        let mut context = context(workspace.path());
        Arc::make_mut(&mut context.policy).max_exec_output_bytes = 1024;
        let result = ExecTool::new()
            .execute(
                &context,
                json!({
                    "command":"i=0; while [ $i -lt 2000 ]; do printf stdout; printf stderr >&2; i=$((i+1)); done"
                }),
            )
            .await
            .unwrap();
        assert!(!result.is_error);
        assert!(result.truncated);
        assert!(result.content.contains("bytes omitted"));
        assert!(result.metadata["stdout_bytes"].as_u64().unwrap() > 512);
        assert!(result.metadata["stderr_bytes"].as_u64().unwrap() > 512);
    }

    #[tokio::test]
    async fn rejects_zero_timeout_denied_cwd_and_disabled_shell() {
        let workspace = tempdir().unwrap();
        let mut context = context(workspace.path());
        let zero = ExecTool::new()
            .execute(&context, json!({"argv":["/bin/true"], "timeout_ms":0}))
            .await
            .unwrap_err();
        assert_eq!(zero.code, ToolErrorCode::InvalidInput);
        let denied = ExecTool::new()
            .execute(&context, json!({"argv":["/bin/true"], "cwd":"../outside"}))
            .await
            .unwrap_err();
        assert_eq!(denied.code, ToolErrorCode::OutsideWorkspace);
        Arc::make_mut(&mut context.policy).allow_shell_exec = false;
        let shell = ExecTool::new()
            .execute(&context, json!({"command":"true"}))
            .await
            .unwrap_err();
        assert_eq!(shell.code, ToolErrorCode::PermissionDenied);
    }

    #[tokio::test]
    async fn environment_is_scrubbed() {
        let workspace = tempdir().unwrap();
        let result = ExecTool::new()
            .execute(&context(workspace.path()), json!({"argv":["/usr/bin/env"]}))
            .await
            .unwrap();
        for secret in [
            "OPENROUTER_API_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "GITHUB_TOKEN",
            "SSH_AUTH_SOCK",
        ] {
            assert!(!result.content.contains(secret), "leaked {secret}");
        }
        assert!(result.content.contains("TACHYON_JAILED=1"));
    }

    #[tokio::test]
    async fn spawn_failure_is_structured_and_large_output_receives_a_reference() {
        let workspace = tempdir().unwrap();
        let mut context = context(workspace.path());
        let missing = ExecTool::new()
            .execute(
                &context,
                json!({"argv":["/definitely/missing/tachyon-command"]}),
            )
            .await
            .unwrap_err();
        assert_eq!(missing.code, ToolErrorCode::ProcessFailed);

        let mut registry = ToolRegistry::default();
        registry.register(ExecTool::new()).unwrap();
        Arc::make_mut(&mut context.policy).max_model_content_bytes = 256;
        Arc::make_mut(&mut context.policy).max_exec_output_bytes = 4096;
        let store = Arc::new(InMemoryOutputStore::new(16 * 1024, 8 * 1024));
        context.output_store = store.clone();
        let result = registry
            .execute(
                "exec",
                &context,
                json!({"command":"yes output | head -c 2048"}),
            )
            .await
            .unwrap();
        let reference = result.output_ref.expect("large exec output is stored");
        assert!(store.get(&reference).await.unwrap().contains("output"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_escalates_and_cleans_up_descendants() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let result = ExecTool::new()
            .execute(
                &context,
                json!({
                    "command":"trap '' TERM; echo $$ > leader.pid; while :; do sleep 30; done",
                    "timeout_ms":100
                }),
            )
            .await
            .unwrap();
        assert!(result.is_error);
        assert_eq!(result.metadata["termination"], "timeout");
        assert!(result.metadata["duration_ms"].as_u64().unwrap() >= 190);
        let pid = std::fs::read_to_string(workspace.path().join("leader.pid"))
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        assert_process_gone(pid).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_cleans_up_the_process_group_before_returning() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let cancellation = context.cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancellation.cancel();
        });
        let result = ExecTool::new()
            .execute(
                &context,
                json!({"command":"sleep 30 & child=$!; echo $child > child.pid; wait"}),
            )
            .await
            .unwrap();
        assert!(result.is_error);
        assert_eq!(result.metadata["termination"], "cancelled");
        let pid = std::fs::read_to_string(workspace.path().join("child.pid"))
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        assert_process_gone(pid).await;
    }

    #[cfg(unix)]
    async fn assert_process_gone(pid: i32) {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;

        for _ in 0..50 {
            if matches!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("descendant process {pid} is still alive");
    }
}
