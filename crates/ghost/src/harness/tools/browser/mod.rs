#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;

use crate::harness::backend::{ExecRequest, Local};
use crate::harness::runtime::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture,
};

pub const USAGE: &str = include_str!("usage.md");

pub fn agent_browser() -> ToolSpec {
    ToolSpec::new(
        "agent_browser",
        "Use the fixed browser. Prefer `read <URL>` for text. For interaction use `open`, `snapshot -i -c`, current refs, fresh snapshots after changes, targeted `get text`, and `close`.",
        json!({
            "type": "object",
            "properties": {
                "args": { "type": "string", "description": "One command's arguments; omit the executable and engine options." }
            },
            "required": ["args"],
            "additionalProperties": false,
        }),
    )
}

const CAPABILITIES: &[Capability] = &[Capability::ExecuteProcess];
const BROWSER_TIMEOUT: Duration = Duration::from_secs(20);
const BROWSER_MAX_OUTPUT_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug)]
pub enum BrowserAvailability {
    Available,
    Unavailable(String),
}

pub struct AgentBrowserTool {
    schema: ToolSpec,
    program: String,
    availability: BrowserAvailability,
}

impl AgentBrowserTool {
    pub fn new(_backend: Arc<Local>, availability: BrowserAvailability) -> Self {
        Self {
            schema: agent_browser(),
            program: std::env::var("TACHYON_AGENT_BROWSER_BIN")
                .unwrap_or_else(|_| "agent-browser".into()),
            availability,
        }
    }
}

impl Tool for AgentBrowserTool {
    fn name(&self) -> &'static str {
        "agent_browser"
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
            if let BrowserAvailability::Unavailable(reason) = &self.availability {
                return Err(ToolError::new(
                    ToolErrorCode::DependencyUnavailable,
                    format!("agent_browser is unavailable: {reason}"),
                    true,
                ));
            }
            let input: BrowserInput = decode_input(input)?;
            let request = browser_request(input, &self.program)?;
            let configured_max_output = std::env::var("AGENT_BROWSER_MAX_OUTPUT").ok();
            let mut policy = browser_policy(&context.policy, configured_max_output.as_deref());
            for name in ["AGENT_BROWSER_ENGINE", "AGENT_BROWSER_EXECUTABLE_PATH"] {
                if let Ok(value) = std::env::var(name) {
                    policy.exec_env.insert(name.into(), value);
                }
            }
            let browser_context = ToolContext {
                workspace_root: context.workspace_root.clone(),
                cwd: context.cwd.clone(),
                identity: context.identity.clone(),
                deadline: context.deadline,
                cancellation: context.cancellation.clone(),
                policy: Arc::new(policy),
                event_sink: Arc::clone(&context.event_sink),
                output_store: Arc::clone(&context.output_store),
            };
            let argv = std::iter::once(request.program)
                .chain(request.args)
                .collect::<Vec<_>>();
            crate::harness::runtime::ExecTool::new()
                .execute(
                    &browser_context,
                    json!({"argv": argv, "timeout_ms": BROWSER_TIMEOUT.as_millis() as u64}),
                )
                .await
        })
    }
}

fn browser_policy(
    base: &crate::harness::runtime::ToolPolicy,
    configured_max_output: Option<&str>,
) -> crate::harness::runtime::ToolPolicy {
    let mut policy = base.clone();
    policy.max_exec_duration = policy.max_exec_duration.min(BROWSER_TIMEOUT);
    policy.max_exec_output_bytes = policy.max_exec_output_bytes.min(BROWSER_MAX_OUTPUT_BYTES);
    let child_max_output = configured_max_output
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(BROWSER_MAX_OUTPUT_BYTES)
        .min(BROWSER_MAX_OUTPUT_BYTES);
    policy.exec_env.insert(
        "AGENT_BROWSER_MAX_OUTPUT".into(),
        child_max_output.to_string(),
    );
    policy
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserInput {
    args: String,
}

fn browser_request(input: BrowserInput, program: &str) -> Result<ExecRequest, ToolError> {
    if input.args.len() > 256 * 1024 {
        return Err(ToolError::invalid("browser args exceed 256 KiB"));
    }
    let args =
        shell_words::split(&input.args).map_err(|error| ToolError::invalid(error.to_string()))?;
    if args.is_empty() {
        return Err(ToolError::invalid("no args for agent_browser"));
    }
    let forbidden = [
        "--engine",
        "--executable-path",
        "--provider",
        "--cdp",
        "--auto-connect",
        "--max-output",
    ];
    if let Some(argument) = args.iter().find(|argument| {
        forbidden
            .iter()
            .any(|option| argument == option || argument.starts_with(&format!("{option}=")))
    }) {
        return Err(ToolError::invalid(format!(
            "agent_browser cannot override its fixed Lightpanda configuration with {argument}"
        )));
    }
    Ok(ExecRequest {
        program: program.into(),
        args,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::harness::runtime::{
        NoopEventSink, NoopOutputStore, ToolIdentity, ToolPolicy, ToolRegistry,
    };

    fn context(root: &Path) -> ToolContext {
        let root = root.canonicalize().unwrap();
        let mut policy = ToolPolicy::worker_default(root.clone());
        policy.exec_term_grace = Duration::from_millis(100);
        policy.max_exec_duration = Duration::from_secs(3);
        ToolContext {
            workspace_root: root.clone(),
            cwd: root,
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(5),
            cancellation: CancellationToken::new(),
            policy: Arc::new(policy),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
        }
    }

    fn shell_browser(root: &Path, availability: BrowserAvailability) -> AgentBrowserTool {
        let mut tool = AgentBrowserTool::new(Arc::new(Local::new(root)), availability);
        tool.program = "/bin/sh".into();
        tool
    }

    #[test]
    fn rejects_unknown_browser_actions() {
        let error = browser_request(
            BrowserInput {
                args: "--engine chromium open https://example.com".into(),
            },
            "agent-browser",
        )
        .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::InvalidInput);
    }

    #[tokio::test]
    async fn unavailable_browser_returns_a_structured_dependency_error() {
        let workspace = tempdir().unwrap();
        let error = shell_browser(
            workspace.path(),
            BrowserAvailability::Unavailable("preflight failed".into()),
        )
        .execute(
            &context(workspace.path()),
            json!({"args":"open about:blank"}),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code, ToolErrorCode::DependencyUnavailable);
        assert!(error.message.contains("preflight failed"));
    }

    #[tokio::test]
    async fn browser_gets_eof_stdin_and_browser_specific_output_cap() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let tool = shell_browser(workspace.path(), BrowserAvailability::Available);
        let result = tool
            .execute(
                &context,
                json!({
                    "args": "-c 'if read value; then exit 9; fi; i=0; while [ $i -lt 10000 ]; do printf o; printf e >&2; i=$((i+1)); done'"
                }),
            )
            .await
            .unwrap();

        assert!(!result.is_error);
        assert!(result.truncated);
        assert!(result.content.contains("[stdout]"));
        assert!(result.content.contains("[stderr]"));
        assert!(result.content.len() < BROWSER_MAX_OUTPUT_BYTES + 256);
        assert_eq!(result.metadata["stdout_bytes"], 10000);
        assert_eq!(result.metadata["stderr_bytes"], 10000);
    }

    #[test]
    fn browser_policy_clamps_duration_capture_and_child_output() {
        let workspace = tempdir().unwrap();
        let root = workspace.path().canonicalize().unwrap();
        let mut base = ToolPolicy::worker_default(root);
        base.max_exec_duration = Duration::from_secs(60);
        base.max_exec_output_bytes = 64 * 1024;

        let policy = browser_policy(&base, Some("999999"));
        assert_eq!(policy.max_exec_duration, BROWSER_TIMEOUT);
        assert_eq!(policy.max_exec_output_bytes, BROWSER_MAX_OUTPUT_BYTES);
        assert_eq!(
            policy.exec_env["AGENT_BROWSER_MAX_OUTPUT"],
            BROWSER_MAX_OUTPUT_BYTES.to_string()
        );

        base.max_exec_duration = Duration::from_secs(2);
        base.max_exec_output_bytes = 1024;
        let policy = browser_policy(&base, Some("512"));
        assert_eq!(policy.max_exec_duration, Duration::from_secs(2));
        assert_eq!(policy.max_exec_output_bytes, 1024);
        assert_eq!(policy.exec_env["AGENT_BROWSER_MAX_OUTPUT"], "512");

        let policy = browser_policy(&base, Some("not-a-number"));
        assert_eq!(
            policy.exec_env["AGENT_BROWSER_MAX_OUTPUT"],
            BROWSER_MAX_OUTPUT_BYTES.to_string()
        );
        let policy = browser_policy(&base, Some("0"));
        assert_eq!(
            policy.exec_env["AGENT_BROWSER_MAX_OUTPUT"],
            BROWSER_MAX_OUTPUT_BYTES.to_string()
        );
    }

    #[tokio::test]
    async fn browser_timeout_is_clamped_by_assignment_deadline_and_structured() {
        let workspace = tempdir().unwrap();
        let mut context = context(workspace.path());
        context.deadline = Instant::now() + Duration::from_millis(50);
        let result = shell_browser(workspace.path(), BrowserAvailability::Available)
            .execute(&context, json!({"args":"-c 'sleep 30'"}))
            .await
            .unwrap();

        assert!(result.is_error);
        assert_eq!(result.metadata["termination"], "timeout");
        assert_eq!(result.metadata["error_code"], "timeout");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn browser_cancellation_cleans_up_its_process_group_before_returning() {
        let workspace = tempdir().unwrap();
        let context = context(workspace.path());
        let child_pid_path = workspace.path().join("child.pid");
        let cancellation = context.cancellation.clone();
        let trigger = tokio::spawn(async move {
            for _ in 0..100 {
                if child_pid_path.exists() {
                    cancellation.cancel();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("browser descendant did not start");
        });
        let mut registry = ToolRegistry::default();
        registry
            .register(shell_browser(
                workspace.path(),
                BrowserAvailability::Available,
            ))
            .unwrap();

        let result = registry
            .execute(
                "agent_browser",
                &context,
                json!({"args":"-c 'sleep 30 & child=$!; echo $child > child.pid; wait'"}),
            )
            .await
            .unwrap();
        trigger.await.unwrap();

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
        panic!("browser descendant process {pid} is still alive");
    }
}
