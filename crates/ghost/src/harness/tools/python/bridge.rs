#![forbid(unsafe_code)]

use crate::harness::{
    backend::ExecResult,
    runtime::{ToolContext, ToolError, ToolRegistry},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Instant,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    process::{Child, Command},
};

const MAX_FRAME: usize = 1024 * 1024;
const MAX_CALLS: u64 = 128;

// One host-owned catalog for require and dispatch, never supplied by the client.
pub(crate) const PACKAGES: &[(&str, &[&str])] = &[
    (
        "workspace",
        &["read", "write", "edit", "ls", "find", "grep"],
    ),
    ("exec", &["exec"]),
    ("ctx", &["ctx"]),
    ("agents", &["agents"]),
    ("history", &["history"]),
    ("work", &["work"]),
    ("todo", &["todo"]),
    ("monitor", &["monitor"]),
    ("websearch", &["websearch"]),
    ("webfetch", &["webfetch"]),
    ("browser", &["agent_browser"]),
    ("artifact", &["artifact"]),
];

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Message {
    Require {
        request_id: u64,
        id: u64,
        name: String,
    },
    Call {
        request_id: u64,
        id: u64,
        tool: String,
        input: Value,
    },
    Done {
        request_id: u64,
        success: bool,
        output: String,
        truncated: bool,
    },
}

#[derive(Serialize)]
struct Cell<'a> {
    preloaded: Vec<Value>,
    request_id: u64,
    code: &'a str,
}

#[derive(Serialize)]
struct Reply {
    request_id: u64,
    id: u64,
    #[serde(flatten)]
    outcome: Outcome,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum Outcome {
    Ok { value: Value },
    Error { error: ToolError },
}

async fn receive<R: AsyncRead + Unpin>(stream: &mut R) -> Result<Message, String> {
    let size = stream.read_u32().await.map_err(|e| e.to_string())? as usize;
    if size == 0 || size > MAX_FRAME {
        return Err("invalid Python frame size".into());
    }
    let mut bytes = vec![0; size];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::from_slice(&bytes).map_err(|e| format!("invalid Python frame: {e}"))
}

async fn send<W: AsyncWrite + Unpin>(stream: &mut W, value: &impl Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if bytes.len() > MAX_FRAME {
        return Err("Python reply exceeds frame limit".into());
    }
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|e| e.to_string())?;
    stream.write_all(&bytes).await.map_err(|e| e.to_string())
}

struct SocketDirectory(PathBuf);
impl Drop for SocketDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("control"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

pub(crate) struct Session {
    _child: Child,
    _group: Option<ProcessGroup>,
    stream: UnixStream,
    next: u64,
    authorized: BTreeSet<String>,
}

struct ProcessGroup(u32);

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.0 as i32),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}

impl Session {
    pub(crate) async fn close(mut self) {
        if let Some(group) = &self._group {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(group.0 as i32),
                nix::sys::signal::Signal::SIGKILL,
            );
        } else {
            let _ = self._child.start_kill();
        }
        let _ = self._child.wait().await;
    }
    pub(crate) async fn start(
        executable: &Path,
        workspace: &Path,
        path: &std::ffi::OsStr,
    ) -> Result<Self, String> {
        let directory = SocketDirectory(
            std::env::temp_dir().join(format!("ghost-python-{}", uuid::Uuid::new_v4())),
        );
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory.0)
            .map_err(|e| e.to_string())?;
        let socket = directory.0.join("control");
        let listener = UnixListener::bind(&socket).map_err(|e| e.to_string())?;
        // Keep helper globals out of the user's persistent IPython namespace.
        let bootstrap = format!(
            "exec({}, {{'__name__': '__ghost_bridge__'}})",
            serde_json::to_string(include_str!("kernel.py")).expect("script literal")
        );
        let mut command = Command::new(executable);
        // Broker kernels stay in the host-owned launch group so host cancellation
        // kills the resident interpreter even if Ghost cannot run its destructors.
        let host_owned = std::env::var_os("GHOST_BROKER_SESSION").is_some();
        if !host_owned {
            command.process_group(0);
        }
        let mut child = command
            .args([
                "--no-banner",
                "--no-confirm-exit",
                "--quick",
                "-c",
                &bootstrap,
            ])
            .current_dir(workspace)
            .kill_on_drop(true)
            .env_clear()
            .env("PATH", path)
            .env("HOME", workspace)
            .env("TMPDIR", "/tmp")
            .env("TACHYON_JAILED", "1")
            .env("GHOST_PYTHON_SOCKET", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("failed to start IPython: {e}"))?;
        // Guard startup too, and retain the group ID even if wait reaps the leader.
        let group = (!host_owned)
            .then(|| ProcessGroup(child.id().expect("newly spawned IPython has a pid")));
        let stream = tokio::select! {
            result = listener.accept() => result.map_err(|e| e.to_string())?.0,
            status = child.wait() => return Err(format!("IPython initialization failed: {status:?}")),
        };
        // The private pathname is removed after connection; no daemon credentials.
        drop(directory);
        Ok(Self {
            _child: child,
            _group: group,
            stream,
            next: 0,
            authorized: BTreeSet::new(),
        })
    }

    pub(crate) async fn execute(
        &mut self,
        code: &str,
        host: Option<(&ToolRegistry, &ToolContext)>,
        deadline: Instant,
    ) -> Result<ExecResult, String> {
        if code.len() > 256 * 1024 {
            return Err("ipython code exceeds 256 KiB".into());
        }
        self.next += 1;
        let request = self.next;
        let preloaded = host
            .and_then(|(registry, context)| registry.python_require("work", &context.policy).ok())
            .into_iter()
            .collect::<Vec<_>>();
        for info in &preloaded {
            self.authorized
                .insert(info["package"].as_str().unwrap().into());
        }
        send(
            &mut self.stream,
            &Cell {
                preloaded,
                request_id: request,
                code,
            },
        )
        .await?;
        let mut expected = 1;
        loop {
            let message = receive(&mut self.stream).await?;
            let (request_id, id) = match &message {
                Message::Done {
                    request_id,
                    success,
                    output,
                    truncated,
                } => {
                    if *request_id != request || output.len() > MAX_FRAME {
                        return Err("invalid Python completion".into());
                    }
                    return Ok(ExecResult {
                        exit_code: Some(if *success { 0 } else { 1 }),
                        stdout: output.clone(),
                        stderr: if *truncated {
                            "[Python output truncated at 64 KiB]".into()
                        } else {
                            String::new()
                        },
                        timed_out: false,
                    });
                }
                Message::Require { request_id, id, .. } | Message::Call { request_id, id, .. } => {
                    (*request_id, *id)
                }
            };
            if request_id != request
                || id != expected
                || id > MAX_CALLS
                || Instant::now() >= deadline
            {
                return Err("invalid, excessive, or expired Python hostcall".into());
            }
            expected += 1;
            let outcome = match (host, message) {
                (Some((registry, context)), Message::Require { name, .. }) if name.len() <= 64 => {
                    registry.python_require(&name, &context.policy).map(|value| {
                        self.authorized.insert(name);
                        value
                    })
                }
                (Some((registry, context)), Message::Call { tool, input, .. })
                    if PACKAGES.iter().any(|(_, operations)| operations.contains(&tool.as_str())) && input.is_object() => {
                    let package = PACKAGES.iter().find(|(_, operations)| operations.contains(&tool.as_str())).unwrap().0;
                    let authorization = if self.authorized.contains(package) {
                        registry.python_require(package, &context.policy).map(|_| ())
                    } else {
                        Err(ToolError::invalid("require the package before calling it"))
                    };
                    if let Err(error) = authorization {
                        send(&mut self.stream, &Reply { request_id: request, id, outcome: Outcome::Error { error } }).await?;
                        continue;
                    }
                    let mut context = context.for_call(format!("python-{request}-{id}"));
                    context.deadline = deadline;
                    let result = Box::pin(registry.execute(&tool, &context, input)).await;
                    if result.as_ref().is_err_and(|e| matches!(e.code, crate::harness::runtime::ToolErrorCode::Timeout | crate::harness::runtime::ToolErrorCode::Cancelled)) {
                        return Err("Python hostcall interrupted; outcome may be unknown".into());
                    }
                    result.map(|r| serde_json::from_str::<Value>(&r.to_json(MAX_FRAME / 2)).expect("ToolResult JSON"))
                }
                _ => Err(crate::harness::runtime::ToolError::invalid("hostcall denied: require an authorized native package; recursive ipython calls are unsupported")),
            };
            let reply = Reply {
                request_id: request,
                id,
                outcome: match outcome {
                    Ok(value) => Outcome::Ok { value },
                    Err(error) => Outcome::Error { error },
                },
            };
            send(&mut self.stream, &reply).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{
        registry::{
            manifest::{Manifest, Package},
            packages::Packages,
        },
        runtime::{
            Capability, NoopEventSink, NoopOutputStore, Tool, ToolFuture, ToolIdentity, ToolPolicy,
            ToolResult,
        },
    };
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct SlowRead {
        name: &'static str,
        calls: Arc<AtomicUsize>,
        schema: tachyon_model::ToolSpec,
    }
    impl Tool for SlowRead {
        fn name(&self) -> &'static str {
            self.name
        }
        fn schema(&self) -> &tachyon_model::ToolSpec {
            &self.schema
        }
        fn capabilities(&self) -> &'static [Capability] {
            &[Capability::ReadFilesystem]
        }
        fn execute<'a>(&'a self, context: &'a ToolContext, _: Value) -> ToolFuture<'a> {
            Box::pin(async move {
                assert_eq!(context.identity.call_id.as_deref(), Some("python-1-2"));
                self.calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                Ok(ToolResult::success("unexpected".into(), json!({})))
            })
        }
    }

    async fn read_value(peer: &mut UnixStream) -> Value {
        let size = peer.read_u32().await.unwrap();
        let mut bytes = vec![0; size as usize];
        peer.read_exact(&mut bytes).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn proxy_metadata_uses_native_schemas_without_local_grants() {
        let root = tempfile::tempdir().unwrap();
        let policy = ToolPolicy::worker_default(root.path().into());
        let registry = crate::harness::profiles::worker(
            Arc::new(crate::harness::backend::Local::new(root.path())),
            crate::harness::runtime::BrowserAvailability::Lazy,
        )
        .into_registry()
        .for_work(&policy, &[], &Default::default())
        .unwrap();
        for (package, _) in PACKAGES {
            if matches!(
                *package,
                "agents" | "history" | "work" | "todo" | "monitor" | "websearch" | "webfetch"
            ) {
                assert!(registry.python_require(package, &policy).is_err());
                assert!(!policy.enabled_tools.contains(*package));
                continue;
            }
            let info = registry.python_require(package, &policy).unwrap();
            for schema in info["schemas"].as_array().unwrap() {
                let native = registry
                    .definitions(&policy)
                    .into_iter()
                    .find(|s| s.name == schema["name"].as_str().unwrap())
                    .unwrap();
                assert_eq!(schema["parameters"], native.parameters);
                if *package == "workspace" {
                    assert_eq!(info["methods"][&native.name]["asynchronous"], false);
                    assert_eq!(info["methods"][&native.name]["input"], json!({}));
                } else if matches!(*package, "browser" | "artifact") {
                    let method = if *package == "browser" {
                        "run"
                    } else {
                        "register"
                    };
                    assert_eq!(
                        info["methods"][method],
                        json!({
                            "tool": native.name, "input": {}, "asynchronous": true
                        })
                    );
                    assert_eq!(info["methods"].as_object().unwrap().len(), 1);
                } else {
                    let actions = native.parameters["properties"]["action"]["enum"]
                        .as_array()
                        .unwrap();
                    assert_eq!(info["methods"].as_object().unwrap().len(), actions.len());
                    for action in actions {
                        assert_eq!(
                            info["methods"][action.as_str().unwrap()],
                            json!({
                                "tool": native.name, "input": {"action": action}, "asynchronous": true
                            })
                        );
                    }
                }
            }
            if *package == "workspace" {
                assert_eq!(info["methods"]["search"], info["methods"]["grep"]);
            }
        }
        assert!(registry.python_require("ipython", &policy).is_err());
        for (package, tool) in [("browser", "agent_browser"), ("artifact", "artifact")] {
            let mut revoked = policy.clone();
            revoked.enabled_tools.remove(tool);
            assert!(registry.python_require(package, &revoked).is_err());
        }
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn startup_drop_and_leader_exit_kill_descendants() {
        use std::os::unix::fs::PermissionsExt;
        for exit in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let executable = root.path().join("ipython");
            std::fs::write(
                &executable,
                format!(
                    "#!/bin/sh\nsleep 30 &\necho $! > child.pid\n{}\n",
                    if exit { "exit 1" } else { "wait" }
                ),
            )
            .unwrap();
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
            let startup = Session::start(
                &executable,
                root.path(),
                std::ffi::OsStr::new("/usr/bin:/bin"),
            );
            if exit {
                assert!(
                    tokio::time::timeout(std::time::Duration::from_secs(5), startup)
                        .await
                        .unwrap()
                        .is_err()
                );
            } else {
                let mut startup = Box::pin(startup);
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        tokio::select! {
                            _ = &mut startup => panic!("unexpected startup completion"),
                            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                                if root.path().join("child.pid").exists() {
                                    break;
                                }
                            }
                        }
                    }
                })
                .await
                .unwrap();
                drop(startup);
            }
            let pid = std::fs::read_to_string(root.path().join("child.pid"))
                .unwrap()
                .trim()
                .parse::<i32>()
                .unwrap();
            for _ in 0..100 {
                if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
                    == Err(nix::errno::Errno::ESRCH)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(
                nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
                Err(nix::errno::Errno::ESRCH),
                "startup descendant survived"
            );
        }
    }

    #[tokio::test]
    async fn fake_hostcall_timeout_and_cancellation_dispatch_once_then_close() {
        for (package, tool, input, cancel) in [
            ("workspace", "read", json!({}), false),
            ("workspace", "read", json!({}), true),
            (
                "exec",
                "exec",
                json!({"action":"start","argv":["fake"]}),
                false,
            ),
            (
                "exec",
                "exec",
                json!({"action":"start","argv":["fake"]}),
                true,
            ),
            (
                "exec",
                "exec",
                json!({"action":"wait","operation":"exec:fake"}),
                false,
            ),
            (
                "exec",
                "exec",
                json!({"action":"wait","operation":"exec:fake"}),
                true,
            ),
            ("ctx", "ctx", json!({"action":"list"}), true),
        ] {
            let root = tempfile::tempdir().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let mut packages = Packages::default();
            packages
                .register(Package {
                    manifest: Manifest {
                        name: package,
                        version: "1",
                        description: "fake",
                        interface: "fake",
                        usage: "fake",
                        operations: match tool {
                            "exec" => &["exec"],
                            "ctx" => &["ctx"],
                            _ => &["read"],
                        },
                    },
                    tools: vec![Arc::new(SlowRead {
                        name: tool,
                        calls: calls.clone(),
                        schema: tachyon_model::ToolSpec::new(
                            tool,
                            "fake",
                            json!({"type":"object"}),
                        ),
                    })],
                })
                .unwrap();
            let mut policy = ToolPolicy::worker_default(root.path().into());
            policy.max_duration = std::time::Duration::from_millis(50);
            let context = ToolContext {
                workspace_root: root.path().into(),
                cwd: root.path().into(),
                identity: ToolIdentity::default(),
                deadline: Instant::now() + std::time::Duration::from_secs(1),
                cancellation: Default::default(),
                policy: Arc::new(policy),
                event_sink: Arc::new(NoopEventSink),
                output_store: Arc::new(NoopOutputStore),
                host_service: None,
            };
            let registry = packages
                .into_registry()
                .for_work(&context.policy, &[], &Default::default())
                .unwrap();
            let (stream, mut peer) = UnixStream::pair().unwrap();
            let child = Command::new("sleep")
                .arg("10")
                .process_group(0)
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let mut session = Session {
                _group: Some(ProcessGroup(child.id().unwrap())),
                _child: child,
                stream,
                next: 0,
                authorized: BTreeSet::new(),
            };
            let trigger = context.cancellation.clone();
            let fake = tokio::spawn(async move {
                assert_eq!(read_value(&mut peer).await["request_id"], 1);
                send(
                    &mut peer,
                    &json!({"kind":"require","request_id":1,"id":1,"name":package}),
                )
                .await
                .unwrap();
                assert_eq!(read_value(&mut peer).await["outcome"], "ok");
                send(
                    &mut peer,
                    &json!({"kind":"call","request_id":1,"id":2,"tool":tool,"input":input}),
                )
                .await
                .unwrap();
                if cancel {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    trigger.cancel();
                }
                assert_eq!(peer.read(&mut [0u8; 1]).await.unwrap(), 0);
            });
            let result = session
                .execute("unused", Some((&registry, &context)), context.deadline)
                .await;
            assert!(result.unwrap_err().contains("outcome may be unknown"));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            drop(session);
            fake.await.unwrap();
        }
    }

    #[tokio::test]
    async fn fake_frames_reject_size_truncation_and_untyped_fields() {
        for bytes in [
            0u32.to_be_bytes().to_vec(),
            ((MAX_FRAME + 1) as u32).to_be_bytes().to_vec(),
            vec![0, 0, 0, 8, b'{'],
        ] {
            assert!(receive(&mut bytes.as_slice()).await.is_err());
        }
        for value in [
            json!({"kind":"unknown"}),
            json!({"kind":"require","request_id":1,"id":1,"name":"workspace","extra":true}),
        ] {
            let mut bytes = Vec::new();
            send(&mut bytes, &value).await.unwrap();
            assert!(receive(&mut bytes.as_slice()).await.is_err());
        }
        let mut bytes = Vec::new();
        send(&mut bytes, &json!({"kind":"done","request_id":1,"success":true,"output":"{\"kind\":\"call\"}","truncated":false})).await.unwrap();
        assert!(matches!(
            receive(&mut bytes.as_slice()).await.unwrap(),
            Message::Done { .. }
        ));
    }

    #[tokio::test]
    async fn fake_kernel_bad_ids_and_loss_never_replay() {
        for message in [
            Some(
                json!({"kind":"done","request_id":99,"success":true,"output":"","truncated":false}),
            ),
            Some(json!({"kind":"require","request_id":1,"id":2,"name":"workspace"})),
            None,
        ] {
            let (stream, mut peer) = UnixStream::pair().unwrap();
            let child = Command::new("sleep")
                .arg("10")
                .process_group(0)
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let mut session = Session {
                _group: Some(ProcessGroup(child.id().unwrap())),
                _child: child,
                stream,
                next: 0,
                authorized: BTreeSet::new(),
            };
            let fake = tokio::spawn(async move {
                let size = peer.read_u32().await.unwrap();
                let mut bytes = vec![0; size as usize];
                peer.read_exact(&mut bytes).await.unwrap();
                assert_eq!(
                    serde_json::from_slice::<Value>(&bytes).unwrap()["code"],
                    "side_effect()"
                );
                if let Some(message) = message {
                    send(&mut peer, &message).await.unwrap();
                }
            });
            assert!(session
                .execute(
                    "side_effect()",
                    None,
                    Instant::now() + std::time::Duration::from_secs(1)
                )
                .await
                .is_err());
            assert_eq!(session.next, 1);
            fake.await.unwrap();
        }
    }
}
