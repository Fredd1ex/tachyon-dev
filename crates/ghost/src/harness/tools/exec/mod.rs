#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::collections::VecDeque;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::harness::runtime::output_store::Spool;
use crate::harness::runtime::{CleanupFuture, ToolOutputRef, ToolRegistry};
use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::harness::runtime::path::resolve_existing;
use crate::harness::runtime::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult,
};

const CAPABILITIES: &[Capability] = &[Capability::ExecuteProcess];
pub const USAGE: &str = include_str!("usage.md");
pub const INTERFACE: &str = "`exec` runs argv or a permitted shell command (action=run default). action=start returns a pending work-scoped operation and stream refs; status/output/wait/cancel take operation. Inspect done, exit/signal and termination. Bound producer output; partial output is not proof of success. Output pages default/max 8 KiB; ctx navigates stream refs. Work end cancels processes; refs do not survive restart.";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

pub struct ExecTool {
    schema: ToolSpec,
    operations: Mutex<HashMap<uuid::Uuid, HashMap<String, Arc<Operation>>>>,
}

struct Operation {
    cancel: CancellationToken,
    done: CancellationToken,
    result: Mutex<Option<Result<ToolResult, ToolError>>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stdout: ToolOutputRef,
    stderr: ToolOutputRef,
}

use tachyon_util::process::ProcessGroupGuard;

impl Default for ExecTool {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecTool {
    pub fn new() -> Self {
        Self {
            operations: Default::default(),
            schema: ToolSpec::new(
                "exec",
                "Run a bounded process in the workspace. Prefer argv for direct execution; command explicitly invokes the configured non-login shell.",
                json!({
                    "type": "object",
                    "properties": {
                        "action": {"enum":["run", "start", "status", "output", "wait", "cancel"], "default":"run"},
                        "operation": {"type":"string"},
                        "stream": {"enum":["stdout", "stderr"]},
                        "cursor": {"type":"integer", "minimum":0},
                        "limit": {"type":"integer", "minimum":1, "maximum":8192},
                        "wait_ms": {"type":"integer", "minimum":0, "maximum":30000},
                        "argv": {
                            "type": "array",
                            "items": { "type": "string" },
                            "minItems": 1,
                            "maxItems": 256
                        },
                        "command": { "type": "string" },
                        "cwd": { "type": "string", "default": "." },
                        "timeout_ms": { "type": "integer", "minimum": 1 }
                        ,"workload": {"type":"object", "properties":{"class":{"enum":["cpu","gpu"]}}, "required":["class"], "additionalProperties":false}
                    },
                    "oneOf": [
                        { "required": ["argv"], "not": { "anyOf": [{"required":["command"]}, {"required":["operation"]}] } },
                        { "required": ["command"], "not": { "anyOf": [{"required":["argv"]}, {"required":["operation"]}] } },
                        { "required": ["action", "operation"], "not": { "anyOf": [{"required":["argv"]}, {"required":["command"]}] } }
                    ],
                    "additionalProperties": false
                }),
            ),
        }
    }
}

impl Tool for ExecTool {
    fn end_work(&self, scope: uuid::Uuid) -> CleanupFuture {
        let operations = self
            .operations
            .lock()
            .unwrap()
            .remove(&scope)
            .unwrap_or_default();
        for operation in operations.values() {
            operation.cancel.cancel();
        }
        let tasks = operations
            .values()
            .filter_map(|operation| operation.task.lock().unwrap().take())
            .map(AbortOnDropHandle::new)
            .collect::<Vec<_>>();
        Box::pin(async move {
            for task in tasks {
                let _ = task.await;
            }
        })
    }

    fn execute_with_registry<'a>(
        &'a self,
        context: &'a ToolContext,
        input: Value,
        registry: &'a ToolRegistry,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let args: ExecInput = decode_input(input.clone())?;
            let action_name = args.action.clone().unwrap_or_else(|| "run".into());
            let action = action_name.as_str();
            if registry.work_scope().is_none() && action == "run" {
                return self.execute(context, input).await;
            }
            let scope = registry
                .work_scope()
                .ok_or_else(|| ToolError::invalid("async exec requires a per-work registry"))?;
            let outputs = registry.work_outputs()?;
            if action == "start" || action == "run" {
                if args.operation.is_some()
                    || args.stream.is_some()
                    || args.cursor.is_some()
                    || args.limit.is_some()
                    || args.wait_ms.is_some()
                {
                    return Err(ToolError::invalid(
                        "run/start accept only invocation fields",
                    ));
                }
                invocation(context, &args)?;
                if args.timeout_ms == Some(0) {
                    return Err(ToolError::invalid("timeout_ms must be greater than zero"));
                }
                let cap = context.policy.max_exec_output_bytes.min(64 * 1024 * 1024) / 2;
                let (stdout, out) = outputs.create(cap).map_err(io_error)?;
                let (stderr, err) = match outputs.create(cap) {
                    Ok(pair) => pair,
                    Err(error) => {
                        outputs.remove(&stdout);
                        return Err(io_error(error));
                    }
                };
                let operation = Arc::new(Operation {
                    cancel: context.cancellation.child_token(),
                    done: CancellationToken::new(),
                    result: Mutex::new(None),
                    task: Mutex::new(None),
                    stdout,
                    stderr,
                });
                let id = format!("exec:{}", uuid::Uuid::new_v4());
                self.operations
                    .lock()
                    .unwrap()
                    .entry(scope)
                    .or_default()
                    .insert(id.clone(), operation.clone());
                let mut context = context.clone();
                context.cancellation = operation.cancel.clone();
                // Keep previews small independently of the disk retention budget.
                Arc::make_mut(&mut context.policy).max_exec_output_bytes =
                    context.policy.max_exec_output_bytes.min(8192);
                Arc::make_mut(&mut context.policy).exec_term_grace =
                    context.policy.exec_term_grace.min(Duration::from_secs(1));
                let task_operation = operation.clone();
                let pending = operation_status(&id, &operation);
                let task = tokio::spawn(async move {
                    let result = run(&context, args, Some((out.clone(), err.clone()))).await;
                    out.seal().await;
                    err.seal().await;
                    *task_operation.result.lock().unwrap() = Some(result);
                    task_operation.done.cancel();
                });
                *operation.task.lock().unwrap() = Some(task);
                if action == "run" {
                    operation.done.cancelled().await;
                    let mut result = operation.result.lock().unwrap().as_ref().unwrap().clone()?;
                    result.metadata["operation"] = json!(id);
                    result.metadata["output_handles"] =
                        json!({"stdout": operation.stdout, "stderr": operation.stderr});
                    return Ok(result);
                }
                return Ok(pending);
            }
            if args.argv.is_some()
                || args.command.is_some()
                || args.cwd.is_some()
                || args.timeout_ms.is_some()
                || args.workload.is_some()
            {
                return Err(ToolError::invalid(
                    "control actions do not accept invocation fields",
                ));
            }
            let id = args
                .operation
                .as_deref()
                .ok_or_else(|| ToolError::invalid("operation is required"))?;
            let operation = self
                .operations
                .lock()
                .unwrap()
                .get(&scope)
                .and_then(|work| work.get(id))
                .cloned()
                .ok_or_else(|| {
                    ToolError::new(
                        ToolErrorCode::PermissionDenied,
                        "unknown exec reference in this work",
                        false,
                    )
                })?;
            match action {
                "status" => {}
                "cancel" => operation.cancel.cancel(),
                "wait" => {
                    let milliseconds = args.wait_ms.unwrap_or(1000);
                    if milliseconds > 30000 {
                        return Err(ToolError::invalid("wait_ms exceeds 30000"));
                    }
                    tokio::select! {
                        _ = operation.done.cancelled() => {},
                        _ = context.cancellation.cancelled() => {},
                        _ = tokio::time::sleep(Duration::from_millis(milliseconds).min(context.policy.max_duration).min(context.deadline.saturating_duration_since(Instant::now()))) => {},
                    }
                }
                "output" => {
                    let reference = match args.stream.as_deref().unwrap_or("stdout") {
                        "stdout" => &operation.stdout,
                        "stderr" => &operation.stderr,
                        _ => return Err(ToolError::invalid("stream must be stdout or stderr")),
                    };
                    return outputs
                        .page(
                            reference,
                            args.cursor.unwrap_or(0),
                            args.limit.unwrap_or(8192),
                        )
                        .await;
                }
                _ => return Err(ToolError::invalid("unknown exec action")),
            }
            Ok(operation_status(id, &operation))
        })
    }
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
            if input
                .action
                .as_deref()
                .is_some_and(|action| action != "run")
                || input.operation.is_some()
            {
                return Err(ToolError::invalid(
                    "async exec requires a per-work registry",
                ));
            }
            run(context, input, None).await
        })
    }
}

fn operation_status(id: &str, operation: &Operation) -> ToolResult {
    let result = operation.result.lock().unwrap();
    let (state, metadata) = match result.as_ref() {
        None => ("pending", json!(null)),
        Some(Ok(result)) => (
            result.metadata["termination"]
                .as_str()
                .unwrap_or("completed"),
            result.metadata.clone(),
        ),
        Some(Err(error)) => (
            if error.metadata["stage"] == "spawn" {
                "spawn_failed"
            } else if error.code == ToolErrorCode::Timeout {
                "timeout"
            } else if error.code == ToolErrorCode::Cancelled {
                "cancelled"
            } else {
                "failed"
            },
            json!(error),
        ),
    };
    ToolResult::success(
        String::new(),
        json!({"operation":id, "state":state, "done":result.is_some(),
        "cancel_requested":operation.cancel.is_cancelled(), "stdout":operation.stdout, "stderr":operation.stderr, "result":metadata}),
    )
}

async fn run(
    context: &ToolContext,
    input: ExecInput,
    spools: Option<(Arc<Spool>, Arc<Spool>)>,
) -> Result<ToolResult, ToolError> {
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

    let deadline = tokio::time::Instant::from_std(started + timeout);
    let workload = input.workload.unwrap_or_default();
    let duration = tachyon_model::broker::job_duration_ms(timeout)
        .ok_or_else(|| ToolError::invalid("native duration overflow"))?;
    let (mut job_lease, devices) = crate::harness::runtime::cpu_jobs::JobLease::acquire_job(
        context, deadline, workload, duration,
    )
    .await?;
    // Scoped selection hint only: same-user native code can override this.
    command.env("CUDA_VISIBLE_DEVICES", devices.join(","));
    let mut child = loop {
        if context.cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
            return Err(ToolError::new(
                if context.cancellation.is_cancelled() {
                    ToolErrorCode::Cancelled
                } else {
                    ToolErrorCode::Timeout
                },
                "exec stopped before process spawn",
                true,
            ));
        }
        match command.spawn() {
            Ok(child) => {
                if let Some(permit) = &mut job_lease {
                    permit.cleanup_confirmed = false;
                }
                break child;
            }
            // A concurrent fork can briefly retain a just-written executable.
            // ETXTBSY means no program ran, so this retry never replays execution.
            Err(error) if error.raw_os_error() == Some(nix::errno::Errno::ETXTBSY as i32) => {
                tokio::select! {
                    _ = context.cancellation.cancelled() => {},
                    _ = tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + Duration::from_millis(10))) => {},
                }
            }
            Err(error) => {
                let mut error = ToolError::new(
                    ToolErrorCode::ProcessFailed,
                    format!("failed to spawn process: {error}"),
                    false,
                );
                error.metadata = json!({"stage":"spawn"});
                return Err(error);
            }
        }
    };
    let process_id = child.id().ok_or_else(|| {
        ToolError::new(
            ToolErrorCode::Internal,
            "spawned process has no process id",
            false,
        )
    })?;
    let mut group = ProcessGroupGuard(Some(process_id));
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ToolError::new(ToolErrorCode::Internal, "stdout pipe unavailable", false))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ToolError::new(ToolErrorCode::Internal, "stderr pipe unavailable", false))?;
    let stream_cap = context.policy.max_exec_output_bytes.min(64 * 1024 * 1024) / 2;
    let (out, err) = spools
        .map(|(out, err)| (Some(out), Some(err)))
        .unwrap_or_default();
    // JoinHandle drop detaches. Abort both readers on every early return or cancellation.
    let stdout_task = AbortOnDropHandle::new(tokio::spawn(capture_stream(stdout, stream_cap, out)));
    let stderr_task = AbortOnDropHandle::new(tokio::spawn(capture_stream(stderr, stream_cap, err)));

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

    if let Some(permit) = job_lease {
        // SIGKILL delivery alone is not native cleanup confirmation. If the
        // group has live descendants, retain host capacity.
        let confirmed = tokio::time::timeout(
            context
                .policy
                .exec_term_grace
                .max(Duration::from_millis(100)),
            async {
                loop {
                    if !tachyon_util::process::group_has_live_processes(process_id)
                        .await
                        .map_err(io_error)?
                    {
                        return Ok::<_, ToolError>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
        )
        .await
        .map_err(|_| {
            ToolError::new(
                ToolErrorCode::ProcessFailed,
                "native cleanup unconfirmed; host CPU permit retained",
                false,
            )
        })?;
        confirmed?;
        group.0 = None;
        permit.release().await?;
    }
    group.0 = None;
    let (stdout, stderr) = tokio::join!(
        finish_capture(stdout_task, context.policy.exec_term_grace),
        finish_capture(stderr_task, context.policy.exec_term_grace),
    );
    let stdout = stdout?;
    let stderr = stderr?;
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
            "signal": exit_signal(&status),
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
}

fn exit_signal(status: &ExitStatus) -> Option<i32> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    }
    #[cfg(not(unix))]
    {
        let _ = status;
        None
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecInput {
    action: Option<String>,
    operation: Option<String>,
    stream: Option<String>,
    cursor: Option<usize>,
    limit: Option<usize>,
    wait_ms: Option<u64>,
    argv: Option<Vec<String>>,
    command: Option<String>,
    cwd: Option<String>,
    timeout_ms: Option<u64>,
    workload: Option<tachyon_model::broker::JobWorkload>,
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
    spool: Option<Arc<Spool>>,
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
        if let Some(spool) = &spool {
            spool.append(&buffer[..count]).await;
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
    mut task: AbortOnDropHandle<std::io::Result<CapturedStream>>,
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
    tachyon_util::process::terminate_process_group(child, process_id, grace)
        .await
        .map_err(io_error)
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
    tachyon_util::process::cleanup_remaining_group(process_id, grace)
        .await
        .map_err(io_error)
}

#[cfg(not(unix))]
async fn cleanup_remaining_group(_process_id: u32, _grace: Duration) -> Result<(), ToolError> {
    Ok(())
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

#[cfg(not(unix))]
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
            host_service: None,
        }
    }

    #[tokio::test]
    async fn cleanup_timeout_aborts_all_active_supervisors() {
        let tool = ExecTool::new();
        let scope = uuid::Uuid::new_v4();
        let mut dropped = Vec::new();
        for index in 0..2 {
            let stopped = CancellationToken::new();
            let guard = stopped.clone().drop_guard();
            let task = tokio::spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            tool.operations
                .lock()
                .unwrap()
                .entry(scope)
                .or_default()
                .insert(
                    index.to_string(),
                    Arc::new(Operation {
                        cancel: Default::default(),
                        done: Default::default(),
                        result: Mutex::new(None),
                        task: Mutex::new(Some(task)),
                        stdout: ToolOutputRef {
                            id: "unused".into(),
                        },
                        stderr: ToolOutputRef {
                            id: "unused".into(),
                        },
                    }),
                );
            dropped.push(stopped);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), tool.end_work(scope))
                .await
                .is_err()
        );
        for stopped in dropped {
            tokio::time::timeout(Duration::from_secs(1), stopped.cancelled())
                .await
                .unwrap();
        }
        assert!(!tool.operations.lock().unwrap().contains_key(&scope));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_start_racing_work_end_leaves_no_operations_or_outputs() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let tool = Arc::new(ExecTool::new());
        let mut installed = ToolRegistry::default();
        installed.register_batch(vec![tool.clone()]).unwrap();
        for _ in 0..32 {
            let work = installed
                .for_work(&context.policy, &[], &Default::default())
                .unwrap();
            let caller = work.clone();
            let context = context.clone();
            let start = tokio::spawn(async move {
                caller
                    .execute(
                        "exec",
                        &context,
                        json!({"action":"start", "argv":["/bin/sleep", "30"]}),
                    )
                    .await
            });
            work.finish_work().await;
            let _ = start.await.unwrap();
            assert!(tool.operations.lock().unwrap().is_empty());
            assert!(work.work_outputs().unwrap().list().is_empty());
        }
    }

    #[tokio::test]
    async fn dropping_capture_wait_aborts_the_reader() {
        use tokio::io::AsyncWriteExt;
        let (reader, mut writer) = tokio::io::duplex(16);
        let task = AbortOnDropHandle::new(tokio::spawn(capture_stream(reader, 16, None)));
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            finish_capture(task, Duration::from_secs(30)),
        )
        .await
        .is_err());
        tokio::task::yield_now().await;
        assert!(writer.write_all(b"x").await.is_err());
    }

    #[tokio::test]
    async fn native_gpu_denial_precedes_spawn_and_host_selection_is_scoped() {
        use tachyon_model::broker::*;
        let workspace = tempdir().unwrap();
        let mut context = context(workspace.path());
        let tool = ExecTool::new();
        let invocation = json!({"command":"touch spawned; printf '%s' \"$CUDA_VISIBLE_DEVICES\"", "workload":{"class":"gpu"}, "timeout_ms":1000});
        assert_eq!(
            tool.execute(&context, invocation.clone())
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        assert!(!workspace.path().join("spawned").exists());
        let (host, client) = private_pair().unwrap();
        context.host_service = Some(Arc::new(client));
        let server = tokio::spawn(async move {
            let mut stream = host.authenticate().await.unwrap();
            assert!(matches!(
                read_frame(&mut stream).await.unwrap(),
                FrameRequest::CpuJob(CpuJobRequest::Acquire {
                    workload: JobWorkload::Gpu {},
                    max_duration_ms: 1000
                })
            ));
            write_frame(&mut stream, &FrameReply::CpuJob(CpuJobReply::Denied))
                .await
                .unwrap();
            assert!(matches!(
                read_frame(&mut stream).await.unwrap(),
                FrameRequest::CpuJob(CpuJobRequest::Acquire {
                    workload: JobWorkload::Gpu {},
                    ..
                })
            ));
            let id = uuid::Uuid::new_v4();
            write_frame(
                &mut stream,
                &FrameReply::CpuJob(CpuJobReply::Granted {
                    permit: id,
                    device_ids: vec!["GPU-simulated".into()],
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
        });
        assert_eq!(
            tool.execute(&context, invocation.clone())
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        assert!(!workspace.path().join("spawned").exists());
        let result = tool.execute(&context, invocation).await.unwrap();
        assert!(result.content.contains("GPU-simulated"), "{result:?}");
        assert!(workspace.path().join("spawned").exists());
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_exec_kills_descendants_not_only_the_leader() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let deadline = context.deadline;
        let task = tokio::spawn(async move {
            ExecTool::new()
                .execute(
                    &context,
                    json!({"command":"sleep 30 & echo $! > child.pid; wait"}),
                )
                .await
        });
        let pid = loop {
            if let Ok(text) = tokio::fs::read_to_string(workspace.path().join("child.pid")).await {
                if let Ok(pid) = text.trim().parse::<i32>() {
                    break pid;
                }
            }
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        task.abort();
        let _ = task.await;
        assert_process_gone(pid).await;
    }

    #[tokio::test]
    async fn async_pending_scope_pages_search_completion_and_spawn_failure() {
        let workspace = tempdir().unwrap();
        let mut context = context(workspace.path());
        Arc::make_mut(&mut context.policy).max_exec_output_bytes = 32768;
        let installed = crate::harness::runtime::native_registry();
        let work = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let other = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let start = work.execute("exec", &context, json!({"action":"start", "command":"sleep 0.2; printf needle; head -c 1000000 /dev/zero | tr '\\000' x & head -c 1000000 /dev/zero | tr '\\000' y >&2 & wait; exit 7"})).await.unwrap();
        assert_eq!(start.metadata["state"], "pending");
        let id = &start.metadata["operation"];
        assert_eq!(
            other
                .execute("exec", &context, json!({"action":"status", "operation":id}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        assert_eq!(
            other
                .execute(
                    "ctx",
                    &context,
                    json!({"action":"read", "reference":start.metadata["stdout"]})
                )
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        let done = work
            .execute(
                "exec",
                &context,
                json!({"action":"wait", "operation":id, "wait_ms":3000}),
            )
            .await
            .unwrap();
        assert_eq!(done.metadata["state"], "completed");
        assert_eq!(done.metadata["result"]["exit_code"], 7);
        for stream in ["stdout", "stderr"] {
            let page = work
                .execute(
                    "exec",
                    &context,
                    json!({"action":"output", "operation":id, "stream":stream}),
                )
                .await
                .unwrap();
            assert_eq!(page.content.len(), 8192);
            assert_eq!(page.metadata["retained_bytes"], 16384);
            assert!(page.metadata["discarded_bytes"].as_u64().unwrap() > 900000);
            let next = work.execute("ctx", &context, json!({"action":"read", "reference":start.metadata[stream], "cursor":page.metadata["next_cursor"]})).await.unwrap();
            assert_eq!(next.content.len(), 8192);
            assert_eq!(next.metadata["has_more"], false);
        }
        let search = work
            .execute(
                "ctx",
                &context,
                json!({"action":"search", "reference":start.metadata["stdout"], "query":"needle"}),
            )
            .await
            .unwrap();
        assert_eq!(search.metadata["matches"], json!([0]));
        let list = work
            .execute("ctx", &context, json!({"action":"list"}))
            .await
            .unwrap();
        assert_eq!(list.metadata["references"].as_array().unwrap().len(), 2);
        let missing = work
            .execute(
                "exec",
                &context,
                json!({"action":"start", "argv":["/definitely/missing/ghost"]}),
            )
            .await
            .unwrap();
        let failed = work
            .execute(
                "exec",
                &context,
                json!({"action":"wait", "operation":missing.metadata["operation"], "wait_ms":1000}),
            )
            .await
            .unwrap();
        assert_eq!(failed.metadata["state"], "spawn_failed");
        assert_eq!(failed.metadata["result"]["code"], "process_failed");
        for stream in ["stdout", "stderr"] {
            let page = work
                .execute(
                    "ctx",
                    &context,
                    json!({"action":"read", "reference":missing.metadata[stream]}),
                )
                .await
                .unwrap();
            assert!(page.content.is_empty());
            assert_eq!(page.metadata["retained_bytes"], 0);
        }
        work.finish_work().await;
        assert!(work.work_outputs().unwrap().list().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn live_output_and_last_work_drop_cleanup() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let installed = crate::harness::runtime::native_registry();
        let work = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let start = work
            .execute(
                "exec",
                &context,
                json!({"action":"start", "command":"echo $$ > pid; printf ready; sleep 30"}),
            )
            .await
            .unwrap();
        loop {
            let page = work
                .clone()
                .execute(
                    "ctx",
                    &context,
                    json!({"action":"read", "reference":start.metadata["stdout"]}),
                )
                .await
                .unwrap();
            if page.content == "ready" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert!(Instant::now() < context.deadline);
        }
        let status = work
            .execute(
                "exec",
                &context,
                json!({"action":"wait", "operation":start.metadata["operation"], "wait_ms":0}),
            )
            .await
            .unwrap();
        assert_eq!(status.metadata["state"], "pending");
        let pid = tokio::fs::read_to_string(workspace.path().join("pid"))
            .await
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        drop(work);
        assert_process_gone(pid).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn async_cancel_timeout_and_work_finish_reap_real_processes() {
        for mode in ["cancel", "timeout", "cleanup"] {
            let workspace = tempdir().unwrap();
            let context = context(workspace.path());
            let installed = crate::harness::runtime::native_registry();
            let work = installed
                .for_work(&context.policy, &[], &Default::default())
                .unwrap();
            let start = work.execute("exec", &context, json!({"action":"start", "command":"trap '' TERM; echo $$ > pid; while :; do sleep 30; done", "timeout_ms":if mode == "timeout" { 150 } else { 3000 }})).await.unwrap();
            let pid = loop {
                if let Ok(text) = tokio::fs::read_to_string(workspace.path().join("pid")).await {
                    if let Ok(pid) = text.trim().parse::<i32>() {
                        break pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                assert!(Instant::now() < context.deadline);
            };
            if mode == "cleanup" {
                work.finish_work().await;
                assert!(work
                    .execute(
                        "exec",
                        &context,
                        json!({"action":"status", "operation":start.metadata["operation"]})
                    )
                    .await
                    .is_err());
            } else {
                if mode == "cancel" {
                    work.execute(
                        "exec",
                        &context,
                        json!({"action":"cancel", "operation":start.metadata["operation"]}),
                    )
                    .await
                    .unwrap();
                }
                let done = work.execute("exec", &context, json!({"action":"wait", "operation":start.metadata["operation"], "wait_ms":3000})).await.unwrap();
                assert_eq!(
                    done.metadata["state"],
                    if mode == "cancel" {
                        "cancelled"
                    } else {
                        "timeout"
                    }
                );
                assert_eq!(done.metadata["result"]["signal"], 9);
                work.finish_work().await;
            }
            assert_process_gone(pid).await;
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
