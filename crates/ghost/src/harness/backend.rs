#![forbid(unsafe_code)]

//! Local execution is not an OS sandbox. Python is optional and starts lazily.

use crate::harness::{
    runtime::{ToolContext, ToolRegistry},
    tools::python::bridge::Session,
};
use std::{os::unix::fs::PermissionsExt, path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::{process::Command, sync::Mutex};

const EXEC_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const IPYTHON_INSTALL_HINT: &str = "Install IPython on Arch Linux with `pacman -S ipython` using package-install privileges; normal Ghost operation does not require those privileges.";

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
        s.push_str(&format!("\n[exit {}]", self.exit_code.unwrap_or(-1)));
        s
    }
}

pub trait Backend: Send + Sync {
    #[allow(async_fn_in_trait)]
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

type PythonSlot = Arc<Mutex<Option<Session>>>;

pub struct Local {
    workspace: PathBuf,
    exec_path: std::ffi::OsString,
    timeout: Duration,
    ipython: PythonSlot,
    work_ipython: std::sync::Mutex<std::collections::HashMap<uuid::Uuid, PythonSlot>>,
}
impl Local {
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        Self {
            workspace: workspace.into(),
            exec_path: EXEC_PATH.into(),
            timeout: tool_timeout(),
            ipython: Arc::new(Mutex::new(None)),
            work_ipython: Default::default(),
        }
    }

    /// Presence only: no imports, installation, or kernel startup.
    pub fn check_ipython(&self) -> Result<PathBuf, String> {
        for directory in std::env::split_paths(&self.exec_path) {
            let executable = self.workspace.join(directory).join("ipython");
            if std::fs::metadata(&executable)
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            {
                return Ok(executable);
            }
        }
        Err(format!(
            "no executable ipython found in backend PATH ({}). {IPYTHON_INSTALL_HINT} Python tools are unavailable; other capabilities remain available.",
            self.exec_path.to_string_lossy()
        ))
    }

    pub(crate) async fn python(
        &self,
        code: &str,
        host: Option<(&ToolRegistry, &ToolContext)>,
    ) -> ExecResult {
        let deadline = host.map_or(std::time::Instant::now() + self.timeout, |(_, c)| {
            c.deadline
                .min(std::time::Instant::now() + self.timeout.min(c.policy.max_duration))
        });
        let cancel = host
            .map(|(_, c)| c.cancellation.clone())
            .unwrap_or_default();
        let execution = async {
            let scope = host.and_then(|(registry, _)| registry.work_scope());
            let sessions = match scope {
                Some(scope) => self
                    .work_ipython
                    .lock()
                    .unwrap()
                    .entry(scope)
                    .or_default()
                    .clone(),
                None => self.ipython.clone(),
            };
            let mut slot = sessions.lock().await;
            // Own the session across every await: dropping this future kills the kernel,
            // leaving the slot empty rather than reusing a half-completed transaction.
            let mut session = match slot.take() {
                Some(session) => session,
                None => {
                    Session::start(&self.check_ipython()?, &self.workspace, &self.exec_path).await?
                }
            };
            let result = session.execute(code, host, deadline).await?;
            *slot = Some(session);
            Ok::<_, String>(result)
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => ExecResult { stderr: "ipython cancelled; session closed; side effects may have occurred; no replay".into(), ..Default::default() },
            _ = tokio::time::sleep_until(deadline.into()) => ExecResult { timed_out: true, stderr: "ipython timed out; session closed; side effects may have occurred; no replay".into(), ..Default::default() },
            result = execution => result.unwrap_or_else(|error| ExecResult { stderr: format!("{error}; session closed; side effects may have occurred; no replay"), ..Default::default() }),
        }
    }

    pub(crate) fn end_work(&self, scope: uuid::Uuid) -> crate::harness::runtime::CleanupFuture {
        let session = self.work_ipython.lock().unwrap().remove(&scope);
        Box::pin(async move {
            if let Some(session) = session {
                let session = session.lock().await.take();
                if let Some(session) = session {
                    session.close().await;
                }
            }
        })
    }
}

fn tool_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("TACHYON_TOOL_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20)
            .max(1),
    )
}

impl Backend for Local {
    async fn run_ipython(&self, code: &str) -> ExecResult {
        self.python(code, None).await
    }
    async fn run(&self, req: &ExecRequest) -> ExecResult {
        let mut cmd = Command::new(&req.program);
        cmd.args(&req.args)
            .current_dir(&self.workspace)
            .kill_on_drop(true)
            .env_clear()
            .env("PATH", &self.exec_path)
            .env("HOME", &self.workspace)
            .env("TMPDIR", "/tmp")
            .env("TACHYON_JAILED", "1")
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
                stdout: String::from_utf8_lossy(&out.stdout).into(),
                stderr: String::from_utf8_lossy(&out.stderr).into(),
                timed_out: false,
            },
            Ok(Err(e)) => ExecResult {
                stderr: format!("failed to run {}: {e}", req.program),
                ..Default::default()
            },
            Err(_) => ExecResult {
                stderr: format!("{} timed out", req.program),
                timed_out: true,
                ..Default::default()
            },
        }
    }
}

pub fn ensure_workspace(id: &str) -> std::io::Result<PathBuf> {
    let dir = tachyon_util::daemon::workspaces_dir().join(id);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{
        profiles,
        runtime::{BrowserAvailability, NoopEventSink, NoopOutputStore, ToolIdentity, ToolPolicy},
    };
    use serde_json::json;

    fn context(root: &std::path::Path) -> ToolContext {
        ToolContext {
            workspace_root: root.into(),
            cwd: root.into(),
            identity: ToolIdentity::default(),
            deadline: std::time::Instant::now() + Duration::from_secs(20),
            cancellation: Default::default(),
            policy: Arc::new(ToolPolicy::worker_default(root.into())),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        }
    }

    #[tokio::test]
    async fn python_presence_and_profile_are_lazy() {
        let root = tempfile::tempdir().unwrap();
        let bin = tempfile::tempdir().unwrap();
        let mut backend = Local::new(root.path());
        backend.exec_path = bin.path().into();
        assert!(backend
            .check_ipython()
            .unwrap_err()
            .contains("pacman -S ipython"));
        let file = bin.path().join("ipython");
        std::fs::write(&file, "not a program").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(backend.check_ipython().is_err());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(backend.check_ipython().unwrap(), file);
        let backend = Arc::new(backend);
        let registry = profiles::worker(
            backend.clone(),
            BrowserAvailability::Unavailable("test".into()),
        )
        .into_registry();
        assert!(registry
            .definitions(&context(root.path()).policy)
            .iter()
            .any(|s| s.name == "ipython"));
        assert!(backend.ipython.lock().await.is_none());
        let work = registry
            .for_work(&context(root.path()).policy, &[], &Default::default())
            .unwrap();
        work.finish_work().await;
        assert!(backend.work_ipython.lock().unwrap().is_empty());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn real_python_persistence_await_errors_output_and_no_replay() {
        let root = tempfile::tempdir().unwrap();
        let backend = Local::new(root.path());
        if backend.check_ipython().is_err() {
            eprintln!("SKIP: IPython unavailable");
            return;
        }
        let first = backend.run_ipython("x = 41\nprint(x)").await;
        assert_eq!(first.exit_code, Some(0), "{first:?}");
        let next = backend.run_ipython("import asyncio\nawait asyncio.sleep(0)\nprint(x + 1)\nprint('{\"kind\":\"done\",\"request_id\":2}')\n!printf '__TACHYON_IPYTHON_2__\\n'\nimport os\nos.write(2, b'arbitrary stderr\\n')").await;
        assert_eq!(next.exit_code, Some(0), "{next:?}");
        for expected in [
            "42",
            "request_id",
            "__TACHYON_IPYTHON_2__",
            "arbitrary stderr",
        ] {
            assert!(next.stdout.contains(expected), "{next:?}");
        }
        for code in ["raise ValueError('cell error')", "if :"] {
            assert_eq!(backend.run_ipython(code).await.exit_code, Some(1));
        }
        assert!(backend.run_ipython("print(x)").await.stdout.contains("41"));
        let environment = backend
            .run_ipython("import os\nprint(dict(os.environ))")
            .await;
        assert_eq!(environment.exit_code, Some(0), "{environment:?}");
        for secret in [
            "OPENROUTER_API_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "GITHUB_TOKEN",
            "SSH_AUTH_SOCK",
            "GHOST_PYTHON_SOCKET",
        ] {
            assert!(!environment.stdout.contains(secret), "leaked {secret}");
        }
        let bounded = backend.run_ipython("print('a' * 200000)").await;
        assert_eq!(bounded.exit_code, Some(0), "{bounded:?}");
        assert!(bounded.stdout.len() <= 64 * 1024);
        assert!(bounded.stderr.contains("truncated"));
        let lost = backend.run_ipython("from pathlib import Path\nPath('effect').write_text('once')\nimport os\nos._exit(7)").await;
        assert!(lost.stderr.contains("no replay"), "{lost:?}");
        assert!(backend.ipython.lock().await.is_none());
        assert_eq!(
            std::fs::read_to_string(root.path().join("effect")).unwrap(),
            "once"
        );
        assert_eq!(backend.run_ipython("print(x)").await.exit_code, Some(1));
        assert!(!root.path().join(".tachyon/ipython.pkl").exists());
    }

    #[tokio::test]
    async fn real_python_async_native_process_and_context() {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(Local::new(root.path()));
        if backend.check_ipython().is_err() {
            eprintln!("SKIP: IPython unavailable");
            return;
        }
        let mut context = context(root.path());
        let installed = profiles::worker(
            backend.clone(),
            BrowserAvailability::Unavailable("test".into()),
        )
        .into_registry();
        let registry = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        for code in [
            r#"workspace = require('workspace', asynchronous=True)
sync = require('workspace')
for bad in [object(), 'x' * (1024 * 1024), float('nan'), float('inf')]:
    for asynchronous in [False, True]:
        try:
            if asynchronous:
                await workspace.read(path=bad)
            else:
                sync.read(path=bad)
        except (TypeError, ValueError):
            pass
        else:
            assert False, 'expected local encoding error'
        assert not (await workspace.ls())['is_error']
        assert not sync.ls()['is_error']
try:
    await workspace.grep(query='wrong argument')
except RuntimeError as error:
    assert 'invalid_input' in str(error)
else:
    assert False, 'expected native argument error'
assert not (await workspace.grep(pattern='TODO', limit=20))['is_error']
assert not sync.grep(pattern='TODO', limit=20)['is_error']"#,
            r#"import asyncio
proc = require('exec')
p = await proc.start(argv=['/bin/sleep', '1'])
async def conflict():
    await asyncio.sleep(0.01)
    for call in [lambda: require('workspace'), lambda: sync.ls()]:
        try:
            call()
        except RuntimeError as error:
            assert 'concurrent' in str(error)
        else:
            assert False, 'expected overlapping sync call rejection'
await asyncio.gather(proc.wait(operation=p['metadata']['operation'], wait_ms=100), conflict())
assert not sync.ls()['is_error']
await proc.cancel(operation=p['metadata']['operation'])"#,
            "workspace = require('workspace', asynchronous=True)\nproc = require('exec')\nctx = require('ctx')\nimport asyncio\nassert require('exec').schemas == proc.schemas\ncall = workspace.write(path='lazy', content='needle')\nfrom pathlib import Path\nassert not Path('lazy').exists()\nawait call\nassert 'needle' in (await workspace.grep(pattern='needle'))['content']\np = await proc.start(command='printf hello; sleep 0.3; printf world', timeout_ms=3000)\nassert p['metadata']['done'] is False",
            "operation = p['metadata']['operation']\nassert (await proc.status(operation=operation))['metadata']['operation'] == operation\nstate = await proc.wait(operation=operation, wait_ms=0)\nfor _ in range(10):\n    state = await proc.wait(operation=operation, wait_ms=500)\n    if state['metadata']['done']: break\nassert state['metadata']['done']\npage = await ctx.read(reference=p['metadata']['stdout'], limit=5)\nassert page['content'] == 'hello'\nassert (await proc.output(operation=operation, limit=5))['content'] == 'hello'\nassert (await ctx.list())['metadata']['references']\nassert (await ctx.search(reference=p['metadata']['stdout'], query='hello'))['metadata']\nr = await proc.run(argv=['/bin/sh', '-c', 'exit 7'])\nassert r['is_error'] and r['metadata']['exit_code'] == 7",
            "bad = await proc.start(argv=['/no/such/ghost-executable'])\ns = await proc.wait(operation=bad['metadata']['operation'], wait_ms=1000)\nassert s['metadata']['done'] and s['metadata']['state'] == 'spawn_failed'\np = await proc.start(argv=['/bin/sleep', '10'])\nawait proc.cancel(operation=p['metadata']['operation'])\ns = await proc.wait(operation=p['metadata']['operation'], wait_ms=2000)\nassert s['metadata']['done']",
            "p = await proc.start(argv=['/bin/sleep', '1'])\nticks = []\nasync def tick():\n    await asyncio.sleep(0.02)\n    ticks.append(True)\nasync def conflict():\n    await asyncio.sleep(0.01)\n    try:\n        await ctx.list()\n    except RuntimeError as e:\n        assert 'concurrent' in str(e)\n    else:\n        assert False\nawait asyncio.gather(proc.wait(operation=p['metadata']['operation'], wait_ms=100), tick(), conflict())\nassert ticks\nawait proc.cancel(operation=p['metadata']['operation'])",
        ] {
            let result = registry
                .execute("ipython", &context, json!({"code":code}))
                .await
                .unwrap();
            assert!(!result.is_error, "{result:?}");
        }
        for (tool, code) in [
            (
                "exec",
                "await proc.status(operation=p['metadata']['operation'])",
            ),
            ("ctx", "await ctx.list()"),
        ] {
            Arc::make_mut(&mut context.policy)
                .enabled_tools
                .remove(tool);
            let result = registry
                .execute("ipython", &context, json!({"code":code}))
                .await
                .unwrap();
            assert!(
                result.is_error && result.content.contains("permission_denied"),
                "{result:?}"
            );
        }
        for tool in ["exec", "ctx"] {
            Arc::make_mut(&mut context.policy)
                .enabled_tools
                .insert(tool.into());
        }
        let result = registry.execute("ipython", &context, json!({"code":"r = await proc.run(argv=['/bin/sh', '-c', 'kill -TERM $$'])\nassert r['is_error'] and r['metadata']['signal'] == 15\np = await proc.start(argv=['/bin/sleep', '10'])\ntask = asyncio.create_task(proc.wait(operation=p['metadata']['operation'], wait_ms=3000))\nawait asyncio.sleep(0.02)\ntask.cancel()\ntry:\n    await task\nexcept asyncio.CancelledError:\n    pass"})).await.unwrap();
        assert!(
            result.is_error,
            "cancelled wire transaction must close kernel: {result:?}"
        );
        let result = registry
            .execute(
                "ipython",
                &context,
                json!({"code":"assert 'proc' not in globals()"}),
            )
            .await
            .unwrap();
        assert!(!result.is_error, "{result:?}");
        registry.finish_work().await;
        assert!(backend.work_ipython.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn real_python_native_exec_uses_cpu_hook_but_direct_shell_is_not_enforced() {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(Local::new(root.path()));
        if backend.check_ipython().is_err() {
            eprintln!("SKIP: IPython unavailable");
            return;
        }
        let mut context = context(root.path());
        let (host, client) = tachyon_model::broker::private_pair().unwrap();
        drop(host);
        context.host_service = Some(Arc::new(client));
        let installed = profiles::worker(backend, BrowserAvailability::Unavailable("test".into()))
            .into_registry();
        let registry = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let result = registry
            .execute(
                "ipython",
                &context,
                json!({"code": r#"
proc = require('exec')
try:
    await proc.run(command='touch native-bypass')
except RuntimeError as error:
    assert 'host native job admission unavailable' in str(error)
else:
    assert False, 'native exec bypassed host admission'
!touch direct-shell
"#}),
            )
            .await
            .unwrap();
        assert!(!result.is_error, "{result:?}");
        assert!(!root.path().join("native-bypass").exists());
        assert!(root.path().join("direct-shell").exists());
        registry.finish_work().await;

        // A live cap-one CPU channel is separate from the Python bridge stream.
        use tachyon_model::broker::{
            private_pair, read_frame, write_frame, CpuJobReply, CpuJobRequest, FrameReply,
            FrameRequest,
        };
        let (host, client) = private_pair().unwrap();
        context.host_service = Some(Arc::new(client));
        context.deadline = std::time::Instant::now() + Duration::from_secs(10);
        let registry = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let server = tokio::spawn(async move {
            let mut stream = host.authenticate().await.unwrap();
            let mut active = None;
            let mut acquisitions = 0;
            let mut busy = 0;
            while let Ok(FrameRequest::CpuJob(request)) = read_frame(&mut stream).await {
                let reply = match request {
                    CpuJobRequest::TryAcquire | CpuJobRequest::Acquire { .. }
                        if active.is_none() =>
                    {
                        let id = uuid::Uuid::new_v4();
                        active = Some(id);
                        acquisitions += 1;
                        CpuJobReply::Granted {
                            permit: id,
                            device_ids: Vec::new(),
                        }
                    }
                    CpuJobRequest::TryAcquire | CpuJobRequest::Acquire { .. } => {
                        busy += 1;
                        CpuJobReply::Busy
                    }
                    CpuJobRequest::Release { permit } | CpuJobRequest::Unspawned { permit } => {
                        assert_eq!(active.take(), Some(permit));
                        CpuJobReply::Released
                    }
                    _ => panic!("unexpected CPU request"),
                };
                write_frame(&mut stream, &FrameReply::CpuJob(reply))
                    .await
                    .unwrap();
            }
            assert!(active.is_none());
            assert_eq!(acquisitions, 2);
            assert!(busy > 0);
        });
        let result = registry
            .execute(
                "ipython",
                &context,
                json!({"code":r#"
import asyncio, pathlib
proc = require('exec')
first = await proc.start(command='touch first-running; sleep 30')
while not pathlib.Path('first-running').exists():
    await asyncio.sleep(0.01)
second = await proc.start(command='touch second-running')
await asyncio.sleep(0.15)
assert not pathlib.Path('second-running').exists()
await proc.cancel(operation=first['metadata']['operation'])
done = await proc.wait(operation=second['metadata']['operation'], wait_ms=3000)
assert done['metadata']['state'] == 'completed', done
assert pathlib.Path('second-running').exists()
"#}),
            )
            .await
            .unwrap();
        assert!(!result.is_error, "{result:?}");
        registry.finish_work().await;
        drop(context);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn real_python_agents_uses_native_validation_and_private_mock_host() {
        use tachyon_api::agents::{Control, Reply, Request};
        use tachyon_model::broker::{
            private_pair, read_frame, write_frame, FrameReply, FrameRequest,
        };
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(Local::new(root.path()));
        if backend.check_ipython().is_err() {
            eprintln!("SKIP: IPython unavailable");
            return;
        }
        let (host, mut client) = private_pair().unwrap();
        client.controls = vec![
            Control::Spawn,
            Control::Group,
            Control::List,
            Control::Send,
            Control::Cancel,
            Control::GroupStatus,
            Control::GroupResize,
        ];
        let server = tokio::spawn(async move {
            let mut stream = host.authenticate().await.unwrap();
            for n in 0..7 {
                let FrameRequest::Control(request) = read_frame(&mut stream).await.unwrap() else {
                    panic!("recursive model call")
                };
                let reply = match (n, request) {
                    (
                        0,
                        Request::List {
                            limit: 2,
                            after: None,
                        },
                    ) => Reply::List {
                        work_ids: vec!["child".into()],
                        next_cursor: None,
                    },
                    (
                        1,
                        Request::Send {
                            work_id,
                            command_id,
                            text,
                        },
                    ) => {
                        assert_eq!(
                            (work_id.as_str(), command_id.as_str(), text.as_str()),
                            ("child", "python-stable", "hello")
                        );
                        Reply::Accepted {
                            command_id,
                            sequence: 1,
                            accepted_revision: 1,
                        }
                    }
                    (
                        2,
                        Request::Cancel {
                            work_id,
                            generation: 1,
                        },
                    ) => Reply::CancellationRequested {
                        work_id,
                        generation: 1,
                    },
                    (3, Request::GroupStatus { group_id }) => Reply::Group {
                        group_id,
                        revision: 1,
                        max_running: 2,
                        active: 2,
                        total: 3,
                        cancellation_requested: false,
                    },
                    (
                        4,
                        Request::GroupResize {
                            group_id,
                            expected_revision: 1,
                            max_running: 0,
                        },
                    ) => Reply::Group {
                        group_id,
                        revision: 2,
                        max_running: 0,
                        active: 2,
                        total: 3,
                        cancellation_requested: false,
                    },
                    (
                        5,
                        Request::Spawn {
                            template_id,
                            command_id,
                        },
                    ) => {
                        assert_eq!(template_id, "approved-child");
                        Reply::Admitted {
                            command_id,
                            work_ids: vec!["new-child".into()],
                            group_id: None,
                        }
                    }
                    (
                        6,
                        Request::Group {
                            template_id,
                            command_id,
                            max_running: Some(1),
                        },
                    ) => {
                        assert_eq!(template_id, "approved-batch");
                        Reply::Admitted {
                            command_id,
                            work_ids: vec!["a".into(), "b".into()],
                            group_id: Some("batch".into()),
                        }
                    }
                    _ => panic!("unexpected control request"),
                };
                write_frame(&mut stream, &FrameReply::Control(reply))
                    .await
                    .unwrap();
            }
        });
        let mut packages =
            profiles::worker(backend, BrowserAvailability::Unavailable("test".into()));
        packages
            .register(crate::harness::tools::agents::package(Arc::new(client)))
            .unwrap();
        let installed = packages.into_registry();
        let mut context = context(root.path());
        assert!(
            !installed
                .definitions(&context.policy)
                .iter()
                .any(|s| s.name == "agents"),
            "installation must not grant permission"
        );
        Arc::make_mut(&mut context.policy)
            .enabled_tools
            .insert("agents".into());
        let registry = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        for input in [
            json!({"action":"spawn"}),
            json!({"action":"send","work_id":"child","command_id":"x","text":"x".repeat(4097)}),
            json!({"action":"list","limit":2,"campaign_id":"forged"}),
        ] {
            assert!(registry.execute("agents", &context, input).await.is_err());
        }
        let code = r#"import json
a = require('agents')
assert hasattr(a, 'spawn') and hasattr(a, 'group') and not hasattr(a, 'wait')
for args in [dict(limit=33), dict(limit=2, campaign_id='forged')]:
    try:
        await a.list(**args)
    except RuntimeError as e:
        assert 'invalid_input' in str(e)
    else:
        assert False
r = await a.list(limit=2)
assert json.loads(r['content'])['work_ids'] == ['child']
r = await a.send(work_id='child', command_id='python-stable', text='hello')
assert json.loads(r['content'])['outcome'] == 'accepted'
r = await a.cancel(work_id='child', generation=1)
assert json.loads(r['content'])['outcome'] == 'cancellation_requested'
r = await a.group_status(group_id='owned')
assert json.loads(r['content'])['active'] == 2
r = await a.group_resize(group_id='owned', expected_revision=1, max_running=0)
assert json.loads(r['content'])['max_running'] == 0
r = await a.spawn(template_id='approved-child', command_id='spawn-1')
assert json.loads(r['content'])['work_ids'] == ['new-child']
r = await a.group(template_id='approved-batch', command_id='group-1', max_running=1)
assert json.loads(r['content'])['group_id'] == 'batch'
try:
    await a.spawn(template_id='approved-child', command_id='bad', objective='invented')
except RuntimeError as e:
    assert 'invalid_input' in str(e)
else:
    assert False
try:
    require('ipython')
except RuntimeError:
    pass
else:
    assert False
"#;
        let result = registry
            .execute("ipython", &context, json!({"code":code}))
            .await
            .unwrap();
        assert!(!result.is_error, "{result:?}");
        Arc::make_mut(&mut context.policy)
            .enabled_tools
            .remove("agents");
        let result = registry
            .execute("ipython", &context, json!({"code":"await a.list(limit=2)"}))
            .await
            .unwrap();
        assert!(result.is_error && result.content.contains("permission_denied"));
        server.await.unwrap();
        registry.finish_work().await;
    }

    #[tokio::test]
    async fn real_python_manipulated_proxies_cannot_grant_host_authority() {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(Local::new(root.path()));
        if backend.check_ipython().is_err() {
            eprintln!("SKIP: IPython unavailable");
            return;
        }
        let mut context = context(root.path());
        let registry = profiles::worker(
            backend.clone(),
            BrowserAvailability::Unavailable("test".into()),
        )
        .into_registry()
        .for_work(&context.policy, &[], &Default::default())
        .unwrap();
        let code = r#"hostcall = require.__globals__['hostcall']
def denied(tool, arguments):
    try:
        hostcall('call', tool=tool, input=arguments)
    except RuntimeError:
        pass
    else:
        assert False, 'host accepted forged call'
denied('read', dict(path='missing'))
ws = require('workspace')
assert not ws.asynchronous
assert ws.methods['search'] == ws.methods['grep']
assert set(ws.schemas) == {'read', 'write', 'edit', 'ls', 'find', 'grep'}
assert not ws.ls()['is_error']
for package in ['exec', 'ctx']:
    proxy = require(package, asynchronous=False)
    assert set(proxy.methods) == set(proxy.schemas[package]['properties']['action']['enum'])
    assert all(m['asynchronous'] for m in proxy.methods.values())
for tool in ['ipython', 'browser', 'tools', 'agents', 'search', 'unknown']:
    denied(tool, {})
ws.methods['forged'] = dict(tool='agents', input={'action':'list','limit':1}, asynchronous=False)
try:
    ws.forged()
except RuntimeError:
    pass
else:
    assert False, 'local method granted agents authority'
"#;
        let result = registry
            .execute("ipython", &context, json!({"code":code}))
            .await
            .unwrap();
        assert!(!result.is_error, "{result:?}");
        assert!(!context.policy.enabled_tools.contains("agents"));
        Arc::make_mut(&mut context.policy)
            .enabled_tools
            .remove("read");
        let result = registry.execute("ipython", &context, json!({"code":"denied('read', dict(path='missing'))\nws.methods['forged']['tool'] = 'read'\nws.methods['forged']['input'] = {'path':'missing'}\ntry:\n    ws.forged()\nexcept RuntimeError as e:\n    assert 'permission_denied' in str(e)\nelse:\n    assert False"})).await.unwrap();
        assert!(!result.is_error, "{result:?}");
        registry.finish_work().await;
    }

    #[tokio::test]
    async fn real_python_require_native_parity_policy_and_scope() {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(Local::new(root.path()));
        if backend.check_ipython().is_err() {
            eprintln!("SKIP: IPython unavailable");
            return;
        }
        std::fs::write(root.path().join("sample"), "needle\n").unwrap();
        let mut context = context(root.path());
        let installed = profiles::worker(
            backend.clone(),
            BrowserAvailability::Unavailable("test".into()),
        )
        .into_registry();
        let registry = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let direct = registry
            .execute("read", &context, json!({"path":"sample"}))
            .await
            .unwrap();
        let result = registry.execute("ipython", &context, json!({"code":"ws = require('workspace')\nassert require('workspace').schemas == ws.schemas\nimport json\nprint(json.dumps(ws.read(path='sample')))"})).await.unwrap();
        assert!(!result.is_error, "{result:?}");
        let value: serde_json::Value = serde_json::from_str(
            result
                .content
                .lines()
                .find(|line| line.starts_with('{'))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(value["content"], direct.content);
        assert_eq!(value["metadata"], direct.metadata);
        assert_eq!(registry.activation_snapshot().packages.len(), 1);
        let search = registry
            .execute(
                "ipython",
                &context,
                json!({"code":"print(ws.search(pattern='needle'))"}),
            )
            .await
            .unwrap();
        assert!(!search.is_error, "{search:?}");
        assert!(search.content.contains("needle"));
        for name in ["missing", "ipython", "browser"] {
            let result = registry
                .execute(
                    "ipython",
                    &context,
                    json!({"code":format!("require('{name}')")}),
                )
                .await
                .unwrap();
            assert!(result.is_error, "{result:?}");
        }
        Arc::make_mut(&mut context.policy)
            .enabled_tools
            .remove("read");
        let denied = registry
            .execute(
                "ipython",
                &context,
                json!({"code":"ws.read(path='sample')"}),
            )
            .await
            .unwrap();
        assert!(
            denied.is_error && denied.content.contains("permission_denied"),
            "{denied:?}"
        );
        let other = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let result = other
            .execute("ipython", &context, json!({"code":"print(ws)"}))
            .await
            .unwrap();
        assert!(
            result.is_error && result.content.contains("NameError"),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn real_python_work_end_isolated_success_failure_drop_and_cancellation() {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(Local::new(root.path()));
        if backend.check_ipython().is_err() {
            eprintln!("SKIP: IPython unavailable");
            return;
        }
        let context = context(root.path());
        let installed = profiles::worker(
            backend.clone(),
            BrowserAvailability::Unavailable("test".into()),
        )
        .into_registry();
        let first = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        let other = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        for (work, value) in [(&first, 41), (&other, 99)] {
            let result = work
                .execute(
                    "ipython",
                    &context,
                    json!({"code": format!("x = {value}\nimport os\nprint(os.getpid())")}),
                )
                .await
                .unwrap();
            assert!(!result.is_error, "{result:?}");
        }
        let mut history =
            vec![
                tachyon_model::ChatMessage::new(tachyon_model::Role::User, "history".repeat(100),);
                10
            ];
        crate::harness::session::compact_context_messages(&mut history, 1);
        assert!(history.len() < 10);
        assert!(first
            .execute("ipython", &context, json!({"code":"print(x)"}))
            .await
            .unwrap()
            .content
            .contains("41"));
        first.finish_work().await;
        first.clone().finish_work().await;
        assert_eq!(backend.work_ipython.lock().unwrap().len(), 1);
        assert!(other
            .execute("ipython", &context, json!({"code":"print(x)"}))
            .await
            .unwrap()
            .content
            .contains("99"));
        assert!(first
            .execute("ipython", &context, json!({"code":"x = 0"}))
            .await
            .is_err());
        other.finish_work().await;

        for ending in [
            "success",
            "failure",
            "drop",
            "cancel",
            "active_finish",
            "future_drop",
            "replacement",
        ] {
            let mut work = installed
                .for_work(&context.policy, &[], &Default::default())
                .unwrap();
            let result = work
                .execute(
                    "ipython",
                    &context,
                    json!({"code":"import os, subprocess\nchild = subprocess.Popen(['sleep', '30'], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\nprint(os.getpid())\nprint(child.pid)"}),
                )
                .await
                .unwrap();
            let pid: i32 = result.content.lines().next().unwrap().parse().unwrap();
            let child_pid: i32 = result.content.lines().nth(1).unwrap().parse().unwrap();
            if ending == "failure" {
                assert!(
                    work.execute(
                        "ipython",
                        &context,
                        json!({"code":"raise ValueError('failure')"})
                    )
                    .await
                    .unwrap()
                    .is_error
                );
            }
            if matches!(ending, "cancel" | "active_finish" | "future_drop") {
                let mut context = context.clone();
                context.cancellation = Default::default();
                let call = work.execute(
                    "ipython",
                    &context,
                    json!({"code":"import time\ntime.sleep(30)"}),
                );
                tokio::pin!(call);
                tokio::select! {
                    result = &mut call => panic!("cell finished early: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                }
                match ending {
                    "cancel" => {
                        context.cancellation.cancel();
                        let _ = call.await;
                    }
                    "active_finish" => {
                        let (result, ()) = tokio::join!(call, work.finish_work());
                        assert!(result.is_err());
                    }
                    _ => {}
                }
            }
            if ending == "replacement" {
                work = installed
                    .for_work(&context.policy, &[], &Default::default())
                    .unwrap();
            } else if ending != "drop" {
                work.finish_work().await;
            }
            drop(work);
            tokio::time::timeout(Duration::from_secs(3), async {
                while nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                while nix::sys::signal::kill(nix::unistd::Pid::from_raw(child_pid), None).is_ok() {
                    // An orphan may remain a zombie until the host's init reaps it.
                    if std::fs::read_to_string(format!("/proc/{child_pid}/stat"))
                        .is_ok_and(|stat| stat.contains(") Z "))
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("kernel {pid} survived {ending}"));
            assert!(backend.work_ipython.lock().unwrap().is_empty());
        }
        let untouched = installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap();
        untouched.finish_work().await;
        assert!(backend.work_ipython.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn real_python_timeout_cancel_and_drop_close_session() {
        let root = tempfile::tempdir().unwrap();
        let backend = Local::new(root.path());
        if backend.check_ipython().is_err() {
            eprintln!("SKIP: IPython unavailable");
            return;
        }
        assert_eq!(backend.run_ipython("x = 1").await.exit_code, Some(0));
        let slot = backend.ipython.lock().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), backend.run_ipython("x = 2"))
                .await
                .is_err()
        );
        assert!(
            slot.is_some(),
            "dropping a lock waiter must not evict the kernel"
        );
        drop(slot);
        assert_eq!(
            backend.run_ipython("assert x == 1").await.exit_code,
            Some(0)
        );
        let registry = ToolRegistry::default();
        let mut context = context(root.path());
        context.deadline = std::time::Instant::now() + Duration::from_millis(100);
        let result = backend
            .python(
                "require.__globals__['control'].sendall(b'\\x00\\x00\\x00\\x10{')\nimport time; time.sleep(10)",
                Some((&registry, &context)),
            )
            .await;
        assert!(result.timed_out, "{result:?}");
        assert!(backend.ipython.lock().await.is_none());
        context.deadline = std::time::Instant::now() + Duration::from_secs(10);
        let trigger = context.cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let result = backend
            .python("import time; time.sleep(10)", Some((&registry, &context)))
            .await;
        assert!(result.stderr.contains("cancelled"));
        assert!(backend.ipython.lock().await.is_none());
        assert!(tokio::time::timeout(
            Duration::from_millis(100),
            backend.run_ipython("import time; time.sleep(10)")
        )
        .await
        .is_err());
        assert!(backend.ipython.lock().await.is_none());
    }
}
