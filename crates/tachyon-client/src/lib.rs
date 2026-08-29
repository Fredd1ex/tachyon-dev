#![forbid(unsafe_code)]

//! IPC client for talking to the Tachyon daemon.
//!
//! Every method mirrors a `tachyon_api::types::ApiRequest` one-to-one. Both
//! the CLI and the TUI use this client, keeping the foreground and the user
//! on the exact same daemon API.

use std::io;
use std::time::Duration;

use tachyon_api::transport::Connection;
use tachyon_api::types::{AgentInfo, ApiRequest, ApiResponse, DaemonInfo};

/// Error surfaced to the caller.
#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    Api(String),
    NoDaemon,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Io(e) => write!(f, "ipc error: {e}"),
            ClientError::Api(m) => write!(f, "{m}"),
            ClientError::NoDaemon => {
                write!(f, "daemon is not running (try `tachyon daemon start`)")
            }
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        ClientError::Io(e)
    }
}

impl From<ClientError> for String {
    fn from(e: ClientError) -> Self {
        e.to_string()
    }
}

pub struct Client {
    conn: Connection,
}

impl Client {
    /// Connect to the daemon socket. Returns NoDaemon if nothing is listening.
    pub fn connect() -> Result<Self, ClientError> {
        let path = tachyon_util::daemon::socket_path();
        match Connection::connect(&path) {
            Ok(conn) => Ok(Client { conn }),
            Err(e) => Err(if std::path::Path::new(&path).exists() {
                ClientError::NoDaemon
            } else {
                ClientError::from(e)
            }),
        }
    }

    /// Exchange one request, mapping socket errors and daemon errors uniformly.
    pub fn request(
        &mut self,
        req: &ApiRequest,
        timeout: Duration,
    ) -> Result<ApiResponse, ClientError> {
        self.conn.set_read_timeout(Some(timeout))?;
        let resp = self.conn.exchange(req)?;
        match resp {
            ApiResponse::Error { message, .. } => Err(ClientError::Api(message)),
            other => Ok(other),
        }
    }

    // ---- Mirrored API methods -----------------------------------------

    pub fn daemon_status(&mut self) -> Result<DaemonInfo, ClientError> {
        match self.request(&ApiRequest::DaemonStatus, Duration::from_secs(5))? {
            ApiResponse::DaemonStatus { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected daemon status response".into())),
        }
    }

    pub fn agent_start(
        &mut self,
        task: String,
        cwd: Option<String>,
    ) -> Result<AgentInfo, ClientError> {
        match self.request(
            &ApiRequest::AgentStart {
                task,
                cwd,
                depends_on: Vec::new(),
                lifetime_class: Default::default(),
                purpose: String::new(),
                logical_task_id: None,
                origin_turn_id: None,
                parent_task_id: None,
                tool_call_id: None,
            },
            Duration::from_secs(10),
        )? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected start response".into())),
        }
    }

    pub fn agent_list(&mut self) -> Result<Vec<AgentInfo>, ClientError> {
        match self.request(&ApiRequest::AgentList, Duration::from_secs(5))? {
            ApiResponse::Agents { agents } => Ok(agents),
            _ => Err(ClientError::Api("unexpected list response".into())),
        }
    }

    pub fn agent_status(&mut self, id: Option<String>) -> Result<Vec<AgentInfo>, ClientError> {
        match self.request(&ApiRequest::AgentStatus { id }, Duration::from_secs(5))? {
            ApiResponse::Agent { info } => Ok(vec![info]),
            ApiResponse::Agents { agents } => Ok(agents),
            _ => Err(ClientError::Api("unexpected status response".into())),
        }
    }

    pub fn agent_cat(&mut self, id: String) -> Result<AgentInfo, ClientError> {
        match self.request(&ApiRequest::AgentCat { id }, Duration::from_secs(5))? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected cat response".into())),
        }
    }

    pub fn agent_logs(
        &mut self,
        id: String,
        follow: bool,
        lines: u32,
    ) -> Result<Vec<String>, ClientError> {
        match self.request(
            &ApiRequest::AgentLogs { id, follow, lines },
            Duration::from_secs(5),
        )? {
            ApiResponse::Logs { lines, .. } => Ok(lines),
            _ => Err(ClientError::Api("unexpected logs response".into())),
        }
    }

    pub fn agent_stop(&mut self, id: String) -> Result<AgentInfo, ClientError> {
        match self.request(&ApiRequest::AgentStop { id }, Duration::from_secs(10))? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected stop response".into())),
        }
    }

    pub fn agent_await(&mut self, id: String) -> Result<AgentInfo, ClientError> {
        match self.request(&ApiRequest::AgentAwait { id }, Duration::from_secs(5))? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected await response".into())),
        }
    }

    pub fn agent_release(&mut self, id: String) -> Result<AgentInfo, ClientError> {
        match self.request(&ApiRequest::AgentRelease { id }, Duration::from_secs(10))? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected release response".into())),
        }
    }

    pub fn agent_replan(&mut self, id: String, task: String) -> Result<AgentInfo, ClientError> {
        match self.request(
            &ApiRequest::AgentReplan { id, task },
            Duration::from_secs(10),
        )? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected replan response".into())),
        }
    }

    pub fn agent_kill(&mut self, id: String) -> Result<AgentInfo, ClientError> {
        match self.request(&ApiRequest::AgentKill { id }, Duration::from_secs(10))? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected kill response".into())),
        }
    }

    pub fn agent_interrupt(&mut self, id: String) -> Result<AgentInfo, ClientError> {
        match self.request(&ApiRequest::AgentInterrupt { id }, Duration::from_secs(10))? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected interrupt response".into())),
        }
    }

    pub fn agent_restart(&mut self, id: String) -> Result<AgentInfo, ClientError> {
        match self.request(&ApiRequest::AgentRestart { id }, Duration::from_secs(10))? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected restart response".into())),
        }
    }

    pub fn agent_resume(&mut self, id: String) -> Result<AgentInfo, ClientError> {
        match self.request(&ApiRequest::AgentResume { id }, Duration::from_secs(10))? {
            ApiResponse::Agent { info } => Ok(info),
            _ => Err(ClientError::Api("unexpected resume response".into())),
        }
    }

    pub fn agent_exec(
        &mut self,
        id: String,
        command: Vec<String>,
    ) -> Result<ExecOutcome, ClientError> {
        match self.request(
            &ApiRequest::AgentExec { id, command },
            Duration::from_secs(120),
        )? {
            ApiResponse::Exec {
                exit_code,
                stdout,
                stderr,
                ..
            } => Ok(ExecOutcome {
                exit_code,
                stdout,
                stderr,
            }),
            _ => Err(ClientError::Api("unexpected exec response".into())),
        }
    }

    pub fn agent_attach(&mut self, id: String) -> Result<String, ClientError> {
        match self.request(&ApiRequest::AgentAttach { id }, Duration::from_secs(5))? {
            ApiResponse::Attach { output, .. } => Ok(output),
            _ => Err(ClientError::Api("unexpected attach response".into())),
        }
    }

    pub fn agent_chat(&mut self, id: String, text: String) -> Result<(), ClientError> {
        match self.request(&ApiRequest::AgentChat { id, text }, Duration::from_secs(5))? {
            ApiResponse::Chat { .. } => Ok(()),
            _ => Err(ClientError::Api("unexpected chat response".into())),
        }
    }

    /// Send a message to the foreground runtime.
    pub fn foreground_chat(&mut self, text: String) -> Result<(), ClientError> {
        match self.request(&ApiRequest::ForegroundChat { text }, Duration::from_secs(5))? {
            ApiResponse::Chat { .. } => Ok(()),
            _ => Err(ClientError::Api(
                "unexpected foreground chat response".into(),
            )),
        }
    }

    /// Open a live stream of foreground events.
    pub fn foreground_subscribe(&mut self) -> Result<(), ClientError> {
        let _ = self.request(&ApiRequest::ForegroundSubscribe, Duration::from_secs(5))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ExecOutcome {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// A live subscribe stream to an agent's events.
///
/// Call `next()` repeatedly; returns `None` when the agent ends or the daemon
/// drops the connection. Runs on its own socket, so it can live on a thread
/// while the main loop keeps reading input.
pub struct Subscription {
    conn: Connection,
}

impl Subscription {
    pub fn open(id: &str) -> Result<Self, ClientError> {
        let mut conn = Connection::connect(tachyon_util::daemon::socket_path())?;
        let req = ApiRequest::AgentSubscribe { id: id.to_string() };
        conn.send(&req).map_err(ClientError::Io)?;
        Ok(Subscription { conn })
    }

    /// Block for the next event; `None` when the agent ends.
    pub fn next(&mut self) -> Option<ApiResponse> {
        self.conn.recv().ok()
    }

    /// Non-blocking check for a pending event.
    pub fn try_next(&mut self) -> Option<Option<ApiResponse>> {
        match self.conn.recv() {
            Ok(resp) => Some(Some(resp)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => None,
            Err(_) => Some(None),
        }
    }
}

pub use tachyon_api::types as api;
