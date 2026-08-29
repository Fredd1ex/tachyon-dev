#![forbid(unsafe_code)]

//! Execution backend for ghost tools.
//!
//! Ghost resolves every tool call (ipython/agent_browser) through a
//! [`Backend`]. v0 ships [`Local`] — commands run on the host but are confined
//! to a workspace directory (the "workdir jail") and run with a scrubbed
//! environment so the agent can't touch the user's home/config/credentials.
//!
//! A [`Firecracker`] backend (microVM + vsock guest agent) slots in here later
//! as the real sandbox boundary; ghost's agent logic does not change.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;

static IPYTHON_REQUEST: AtomicU64 = AtomicU64::new(1);

/// A request to run a command in the agent environment.
#[derive(Debug, Clone)]
pub struct ExecRequest {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub struct ExecResult {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

impl ExecResult {
    pub fn combined(&self) -> String {
        let mut s = self.stdout.clone();
        if !self.stderr.is_empty() {
            if !s.is_empty() {
                s.push('\n');
            }
            s.push_str(&self.stderr);
        }
        let code = self.exit_code.unwrap_or(-1);
        s.push_str(&format!("\n[exit {code}]"));
        s
    }
}

/// The execution environment an agent runs in.
pub trait Backend: Send + Sync {
    #[allow(async_fn_in_trait)] // used only with concrete generics; no dyn.
    async fn run(&self, req: &ExecRequest) -> ExecResult;

    #[allow(async_fn_in_trait)]
    async fn run_ipython(&self, code: &str) -> ExecResult {
        self.run(&ExecRequest {
            program: "ipython".into(),
            args: vec![
                "--no-banner".into(),
                "--no-confirm-exit".into(),
                "--quick".into(),
                "-c".into(),
                code.into(),
            ],
        })
        .await
    }
}

/// Local host execution, confined to a workspace directory.
///
/// Everything the agent runs starts in `workspace` with a scrubbed
/// environment: HOME is redirected inside the workspace and well-known
/// credential/env vars are removed, so a misbehaving agent is limited to the
/// workspace (plus whatever it can reach through system facilities — the real
/// hard boundary is the Firecracker backend).
pub struct Local {
    workspace: PathBuf,
    /// Timeout for a single command.
    timeout: std::time::Duration,
    ipython: Arc<Mutex<Option<IpythonSession>>>,
}

struct IpythonSession {
    _child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
}

impl Local {
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        Self {
            workspace: workspace.into(),
            timeout: tool_timeout(),
            ipython: Arc::new(Mutex::new(None)),
        }
    }

    async fn persistent_ipython(&self, code: &str) -> ExecResult {
        let mut session = self.ipython.lock().await;
        let mut new_session = false;
        if session.is_none() {
            let mut cmd = Command::new("ipython");
            cmd.args([
                "--simple-prompt",
                "--no-banner",
                "--no-confirm-exit",
                "--quick",
            ])
            .current_dir(&self.workspace)
            .kill_on_drop(true)
            .env_clear()
            .env(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            )
            .env("HOME", &self.workspace)
            .env("TMPDIR", "/tmp")
            .env("TACHYON_JAILED", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
            let mut child = match cmd.spawn() {
                Ok(child) => child,
                Err(error) => {
                    return ExecResult {
                        stderr: format!("failed to start ipython: {error}"),
                        ..Default::default()
                    };
                }
            };
            let stdin = child.stdin.take().expect("ipython stdin is piped");
            let stdout = child.stdout.take().expect("ipython stdout is piped");
            *session = Some(IpythonSession {
                _child: child,
                stdin,
                stdout: BufReader::new(stdout),
            });
            new_session = true;
        }

        let request = IPYTHON_REQUEST.fetch_add(1, Ordering::Relaxed);
        let sentinel = format!("__TACHYON_IPYTHON_{request}__");
        let current = session.as_mut().expect("session initialized");
        let checkpoint = self.workspace.join(".tachyon/ipython.pkl");
        let checkpoint_literal = serde_json::to_string(&checkpoint.display().to_string())
            .unwrap_or_else(|_| "\".tachyon/ipython.pkl\"".into());
        let restore = if new_session {
            format!(
                "import pathlib as _tachyon_pathlib, pickle as _tachyon_pickle\n_tachyon_file = _tachyon_pathlib.Path({checkpoint_literal})\nif _tachyon_file.exists():\n    try:\n        globals().update(_tachyon_pickle.load(_tachyon_file.open('rb')))\n    except Exception:\n        pass\n"
            )
        } else {
            String::new()
        };
        let snapshot = format!(
            "\ntry:\n    _tachyon_file.parent.mkdir(parents=True, exist_ok=True)\n    _tachyon_values = {{}}\n    for _tachyon_key, _tachyon_value in list(globals().items()):\n        if not _tachyon_key.startswith('_'):\n            try:\n                _tachyon_pickle.dumps(_tachyon_value)\n                _tachyon_values[_tachyon_key] = _tachyon_value\n            except Exception:\n                pass\n    _tachyon_pickle.dump(_tachyon_values, _tachyon_file.open('wb'))\nexcept Exception:\n    pass\n"
        );
        let wrapped = format!("{restore}{code}{snapshot}print({sentinel:?}, flush=True)\n");
        if let Err(error) = current.stdin.write_all(wrapped.as_bytes()).await {
            *session = None;
            return ExecResult {
                stderr: format!("ipython session write failed: {error}"),
                ..Default::default()
            };
        }
        if let Err(error) = current.stdin.flush().await {
            *session = None;
            return ExecResult {
                stderr: format!("ipython session flush failed: {error}"),
                ..Default::default()
            };
        }

        let mut stdout = String::new();
        loop {
            let mut line = String::new();
            match current.stdout.read_line(&mut line).await {
                Ok(0) => {
                    *session = None;
                    return ExecResult {
                        stdout,
                        stderr: "ipython session exited".into(),
                        ..Default::default()
                    };
                }
                Ok(_) if line.contains(&sentinel) => break,
                Ok(_) => stdout.push_str(&line),
                Err(error) => {
                    *session = None;
                    return ExecResult {
                        stdout,
                        stderr: format!("ipython session read failed: {error}"),
                        ..Default::default()
                    };
                }
            }
        }
        ExecResult {
            exit_code: Some(0),
            stdout,
            ..Default::default()
        }
    }
}

fn tool_timeout() -> std::time::Duration {
    let seconds = std::env::var("TACHYON_TOOL_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20)
        .max(1);
    std::time::Duration::from_secs(seconds)
}

impl Backend for Local {
    async fn run_ipython(&self, code: &str) -> ExecResult {
        match tokio::time::timeout(self.timeout, self.persistent_ipython(code)).await {
            Ok(result) => result,
            Err(_) => {
                if let Some(mut session) = self.ipython.lock().await.take() {
                    let _ = session._child.kill().await;
                }
                ExecResult {
                    stderr: "ipython timed out".into(),
                    timed_out: true,
                    ..Default::default()
                }
            }
        }
    }

    async fn run(&self, req: &ExecRequest) -> ExecResult {
        let mut cmd = Command::new(&req.program);
        cmd.args(&req.args)
            .current_dir(&self.workspace)
            .env_clear()
            .env(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            )
            // The agent's HOME is its own workspace dir, not the user's.
            .env("HOME", &self.workspace)
            .env("TMPDIR", "/tmp")
            // Agent identity so it can't masquerade.
            .env("TACHYON_JAILED", "1")
            .env_remove("OPENROUTER_API_KEY")
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .env_remove("GITHUB_TOKEN")
            .env_remove("GH_TOKEN")
            .env_remove("NPM_TOKEN")
            .env_remove("SSH_AUTH_SOCK")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for name in [
            "AGENT_BROWSER_ENGINE",
            "AGENT_BROWSER_EXECUTABLE_PATH",
            "AGENT_BROWSER_MAX_OUTPUT",
        ] {
            if let Some(value) = std::env::var_os(name) {
                cmd.env(name, value);
            }
        }

        match tokio::time::timeout(self.timeout, cmd.output()).await {
            Ok(Ok(out)) => ExecResult {
                exit_code: out.status.code(),
                stdout: String::from_utf8_lossy(&out.stdout).to_string(),
                stderr: String::from_utf8_lossy(&out.stderr).to_string(),
                timed_out: false,
            },
            Ok(Err(e)) => ExecResult {
                exit_code: None,
                stdout: String::new(),
                stderr: format!("failed to run {}: {e}", req.program),
                timed_out: false,
            },
            Err(_) => ExecResult {
                exit_code: None,
                stdout: String::new(),
                stderr: format!("{} timed out", req.program),
                timed_out: true,
            },
        }
    }
}

/// Create a workspace directory for an agent if it doesn't exist.
pub fn ensure_workspace(id: &str) -> std::io::Result<PathBuf> {
    let dir = tachyon_util::daemon::workspaces_dir().join(id);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::{Backend, Local};

    #[tokio::test]
    async fn ipython_reuses_variables_within_a_worker() {
        if std::process::Command::new("ipython")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let workspace =
            std::env::temp_dir().join(format!("tachyon-ipython-test-{}", std::process::id()));
        std::fs::create_dir_all(&workspace).expect("create test workspace");
        let backend = Local::new(&workspace);

        let first = backend.run_ipython("x = 41\nprint(x)").await;
        let second = backend.run_ipython("print(x + 1)").await;

        assert!(!first.timed_out, "first IPython call timed out");
        assert!(!second.timed_out, "second IPython call timed out");
        assert!(
            first.stdout.contains("41"),
            "first output: {}",
            first.stdout
        );
        assert!(
            second.stdout.contains("42"),
            "second output: {}",
            second.stdout
        );
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn ipython_checkpoint_restores_serializable_variables() {
        if std::process::Command::new("ipython")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let workspace = std::env::temp_dir().join(format!(
            "tachyon-ipython-restart-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&workspace).expect("create test workspace");
        let first = Local::new(&workspace);
        let result = first.run_ipython("x = 41\nprint(x)").await;
        assert!(!result.timed_out, "checkpoint setup timed out");
        drop(first);

        let second = Local::new(&workspace);
        let restored = second.run_ipython("print(x + 1)").await;
        assert!(!restored.timed_out, "checkpoint restore timed out");
        assert!(
            restored.stdout.contains("42"),
            "restored output: {}",
            restored.stdout
        );
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn ipython_timeout_terminates_the_persistent_session() {
        if std::process::Command::new("ipython")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let workspace = std::env::temp_dir().join(format!(
            "tachyon-ipython-timeout-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&workspace).expect("create test workspace");
        let mut backend = Local::new(&workspace);
        backend.timeout = std::time::Duration::from_millis(100);

        let result = backend.run_ipython("import time\ntime.sleep(10)").await;

        assert!(result.timed_out);
        assert_eq!(result.stderr, "ipython timed out");
        assert!(backend.ipython.lock().await.is_none());
        let _ = std::fs::remove_dir_all(workspace);
    }
}
