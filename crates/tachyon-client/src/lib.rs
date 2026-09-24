#![forbid(unsafe_code)]

//! IPC client for talking to the Tachyon daemon.
//!
//! Every method mirrors a `tachyon_api::types::ApiRequest` one-to-one. Both
//! the CLI and the TUI use this client, keeping the foreground and the user
//! on the exact same daemon API.

use std::io;
use std::time::Duration;

use tachyon_api::transport::Connection;
use tachyon_api::types::{AgentInfo, ApiRequest, ApiResponse, DaemonInfo, ScheduledTaskInfo};

/// Error surfaced to the caller.
#[derive(Debug)]
pub enum ClientError {
    InteractionGap,
    Monitor(tachyon_api::monitor::MonitorError),
    Todo(tachyon_api::todo::TodoError),
    Io(io::Error),
    Api(String),
    NoDaemon,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::InteractionGap => {
                write!(f, "interaction projection changed; resnapshot required")
            }
            ClientError::Monitor(error) => write!(f, "monitor error: {error:?}"),
            ClientError::Todo(error) => write!(f, "todo error: {error:?}"),
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

#[cfg(test)]
mod interaction_tests;

impl Client {
    /// Allows an owning worker to interrupt a blocked request during teardown.
    pub fn shutdown_handle(&self) -> io::Result<std::os::unix::net::UnixStream> {
        self.conn.shutdown_handle()
    }

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
            ApiResponse::TodoError { error } => Err(ClientError::Todo(error)),
            ApiResponse::MonitorError { error } => Err(ClientError::Monitor(error)),
            other => Ok(other),
        }
    }

    // ---- Mirrored API methods -----------------------------------------

    /// Bounded host retrieval; use a dedicated connection for parallel calls.
    pub fn conversation_web(
        &mut self,
        metadata: tachyon_api::InteractionMetadata,
        command: tachyon_api::web::WebCommand,
    ) -> Result<tachyon_api::web::WebResult, ClientError> {
        command.validate().map_err(|e| ClientError::Api(e.into()))?;
        match self.request(
            &ApiRequest::ConversationWeb {
                metadata,
                command: command.clone(),
            },
            Duration::from_secs(125),
        )? {
            ApiResponse::ConversationWeb {
                command: returned,
                result,
            } if returned == command => {
                let result = result.map_err(ClientError::Api)?;
                if !result.usage.valid() {
                    return Err(ClientError::Api("invalid web usage receipt".into()));
                }
                Ok(result)
            }
            _ => Err(ClientError::Api("unexpected web response".into())),
        }
    }

    pub fn monitor_get(
        &mut self,
        query: tachyon_api::monitor::MonitorQuery,
    ) -> Result<tachyon_api::monitor::MonitorSnapshot, ClientError> {
        query.validate().map_err(ClientError::Monitor)?;
        match self.request(&ApiRequest::MonitorGet { query }, Duration::from_secs(30))? {
            ApiResponse::Monitor { snapshot } => Ok(snapshot),
            _ => Err(ClientError::Api("unexpected monitor response".into())),
        }
    }

    pub fn todo(
        &mut self,
        request: tachyon_api::todo::TodoRequest,
    ) -> Result<tachyon_api::todo::TodoResponse, ClientError> {
        match self.request(&ApiRequest::Todo(request), Duration::from_secs(30))? {
            ApiResponse::Todo { response } => Ok(response),
            _ => Err(ClientError::Api("unexpected todo response".into())),
        }
    }

    pub fn todo_snapshot(
        &mut self,
        scope: tachyon_api::todo::TodoScope,
        limit: Option<usize>,
        cursor: Option<tachyon_api::todo::TodoCursor>,
    ) -> Result<tachyon_api::todo::TodoResponse, ClientError> {
        match self.request(
            &ApiRequest::TodoSnapshot {
                scope,
                limit,
                cursor,
            },
            Duration::from_secs(30),
        )? {
            ApiResponse::Todo { response } => Ok(response),
            _ => Err(ClientError::Api("unexpected todo snapshot response".into())),
        }
    }

    pub fn todo_list(
        &mut self,
        scope: tachyon_api::todo::TodoScope,
        filter: tachyon_api::todo::TodoFilter,
        limit: Option<usize>,
        cursor: Option<tachyon_api::todo::TodoCursor>,
    ) -> Result<tachyon_api::todo::TodoResponse, ClientError> {
        self.todo(tachyon_api::todo::TodoRequest::List {
            scope,
            filter,
            limit,
            cursor,
        })
    }

    pub fn todo_add(
        &mut self,
        scope: tachyon_api::todo::TodoScope,
        command_id: String,
        expected_revision: u64,
        title: String,
        description: String,
    ) -> Result<tachyon_api::todo::TodoResponse, ClientError> {
        self.todo(tachyon_api::todo::TodoRequest::Add {
            scope,
            command_id,
            expected_revision,
            title,
            description,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn todo_update(
        &mut self,
        scope: tachyon_api::todo::TodoScope,
        command_id: String,
        id: String,
        expected_revision: u64,
        title: Option<String>,
        description: Option<String>,
        status: Option<tachyon_api::todo::TodoStatus>,
    ) -> Result<tachyon_api::todo::TodoResponse, ClientError> {
        self.todo(tachyon_api::todo::TodoRequest::Update {
            scope,
            command_id,
            id,
            expected_revision,
            title,
            description,
            status,
        })
    }

    /// Trusted host control lookup. Scope selects storage, not caller authority.
    pub fn artifact_get(
        &mut self,
        scope: String,
        id: String,
    ) -> Result<Option<tachyon_api::types::ArtifactRegistration>, ClientError> {
        match self.request(
            &ApiRequest::ArtifactGet { scope, id },
            Duration::from_secs(30),
        )? {
            ApiResponse::Artifact { artifact } => Ok(artifact),
            _ => Err(ClientError::Api("unexpected artifact response".into())),
        }
    }

    pub fn artifact_list(
        &mut self,
        scope: String,
        after: Option<String>,
        limit: u32,
    ) -> Result<Vec<tachyon_api::types::ArtifactRegistration>, ClientError> {
        match self.request(
            &ApiRequest::ArtifactList {
                scope,
                after,
                limit,
            },
            Duration::from_secs(30),
        )? {
            ApiResponse::ArtifactList { artifacts } => Ok(artifacts),
            _ => Err(ClientError::Api("unexpected artifact list response".into())),
        }
    }

    pub fn artifact_read(
        &mut self,
        scope: String,
        id: String,
        offset: u64,
        limit: u32,
    ) -> Result<Vec<u8>, ClientError> {
        match self.request(
            &ApiRequest::ArtifactRead {
                scope,
                id,
                offset,
                limit,
            },
            Duration::from_secs(30),
        )? {
            ApiResponse::ArtifactBytes { bytes } => Ok(bytes),
            _ => Err(ClientError::Api(
                "unexpected artifact bytes response".into(),
            )),
        }
    }

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
                deadline_ms: None,
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

    pub fn scheduled_task_list(&mut self) -> Result<Vec<ScheduledTaskInfo>, ClientError> {
        match self.request(&ApiRequest::ScheduledTaskList, Duration::from_secs(5))? {
            ApiResponse::ScheduledTasks { schedules } => Ok(schedules),
            _ => Err(ClientError::Api(
                "unexpected scheduled task list response".into(),
            )),
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
        self.foreground_chat_with_cwd(text, None)
    }

    /// Select an existing absolute host directory, or None for managed work.
    pub fn foreground_chat_with_cwd(
        &mut self,
        text: String,
        cwd: Option<String>,
    ) -> Result<(), ClientError> {
        match self.request(
            &ApiRequest::ForegroundChat { text, cwd },
            Duration::from_secs(5),
        )? {
            ApiResponse::Chat { .. } => Ok(()),
            _ => Err(ClientError::Api(
                "unexpected foreground chat response".into(),
            )),
        }
    }

    /// Open a live stream of foreground events.
    pub fn foreground_subscribe(&mut self) -> Result<InteractionSubscription, ClientError> {
        InteractionSubscription::open(None)
    }

    pub fn interaction_snapshot(
        &mut self,
    ) -> Result<tachyon_api::interaction_manager::Snapshot, ClientError> {
        match self.request(&ApiRequest::InteractionSnapshot, Duration::from_secs(5))? {
            ApiResponse::InteractionFrame {
                frame: tachyon_api::interaction_manager::Frame::Snapshot { snapshot },
            } => self.assemble_interaction_snapshot(snapshot),
            _ => Err(ClientError::Api("unexpected interaction snapshot".into())),
        }
    }

    /// Assemble one exact revision off the UI thread. A gap never exposes mixed pages.
    pub fn assemble_interaction_snapshot(
        &mut self,
        mut snapshot: tachyon_api::interaction_manager::Snapshot,
    ) -> Result<tachyon_api::interaction_manager::Snapshot, ClientError> {
        while let Some(offset) = snapshot.projection_next {
            match self.request(
                &ApiRequest::InteractionProjection {
                    revision: snapshot.revision.clone(),
                    offset,
                },
                Duration::from_secs(5),
            )? {
                ApiResponse::InteractionProjection { page }
                    if page.revision == snapshot.revision
                        && page.next_offset.is_none_or(|next| next > offset) =>
                {
                    snapshot
                        .projection
                        .responses
                        .extend(page.projection.responses);
                    snapshot.projection.works.extend(page.projection.works);
                    snapshot
                        .projection
                        .progress
                        .extend(page.projection.progress);
                    snapshot.projection_next = page.next_offset;
                }
                _ => return Err(ClientError::InteractionGap),
            }
        }
        for content in &snapshot.history_content {
            let text = self.interaction_text(&content.reference, content.total_bytes)?;
            if let Some(entry) = snapshot
                .history
                .iter_mut()
                .find(|e| e.event_id == content.event_id)
            {
                entry.text = text;
            }
        }
        for response in &mut snapshot.projection.responses {
            self.hydrate_response(response)?;
        }
        Ok(snapshot)
    }

    pub fn hydrate_response(
        &mut self,
        response: &mut tachyon_api::interaction_manager::Response,
    ) -> Result<(), ClientError> {
        if let Some(reference) = &response.answer_ref {
            response.answer = self.interaction_text(reference, response.answer_bytes)?;
        }
        Ok(())
    }

    /// Handles are opaque; concatenate byte pages before decoding UTF-8.
    pub fn interaction_text(&mut self, reference: &str, total: u64) -> Result<String, ClientError> {
        let mut bytes = Vec::new();
        let mut offset = 0;
        while offset < total {
            let limit = (total - offset).min(65536) as usize;
            match self.request(
                &ApiRequest::InteractionContent {
                    reference: reference.into(),
                    offset,
                    limit: Some(limit),
                },
                Duration::from_secs(5),
            )? {
                ApiResponse::InteractionContent { page }
                    if page.reference == reference
                        && page.offset == offset
                        && !page.bytes.is_empty()
                        && page.bytes.len() <= limit as usize =>
                {
                    offset += page.bytes.len() as u64;
                    bytes.extend(page.bytes);
                }
                _ => return Err(ClientError::Api("invalid interaction content page".into())),
            }
        }
        String::from_utf8(bytes).map_err(|e| ClientError::Api(e.to_string()))
    }

    pub fn interaction_submit(
        &mut self,
        command: tachyon_api::interaction_manager::Submit,
    ) -> Result<tachyon_api::interaction_manager::Receipt, ClientError> {
        match self.request(
            &ApiRequest::InteractionSubmit {
                command: command.clone(),
            },
            Duration::from_secs(5),
        )? {
            ApiResponse::InteractionReceipt { receipt } if receipt.command == command => {
                Ok(receipt)
            }
            _ => Err(ClientError::Api("unexpected interaction receipt".into())),
        }
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

/// Dedicated manager connection. Dropping this never cancels a turn.
pub struct InteractionSubscription {
    conn: Connection,
}
impl InteractionSubscription {
    pub fn open(
        after: Option<tachyon_api::interaction_manager::Revision>,
    ) -> Result<Self, ClientError> {
        let mut conn = Connection::connect(tachyon_util::daemon::socket_path())?;
        conn.send(&ApiRequest::InteractionAttach { after })?;
        Ok(Self { conn })
    }
    pub fn recv(&mut self) -> Result<tachyon_api::interaction_manager::Frame, ClientError> {
        match self.conn.recv()? {
            ApiResponse::InteractionFrame { mut frame } => {
                use tachyon_api::interaction_manager::{Frame, ProjectionChange};
                match &mut frame {
                    Frame::Snapshot { snapshot } => {
                        if snapshot.projection_next.is_some()
                            || !snapshot.history_content.is_empty()
                            || snapshot
                                .projection
                                .responses
                                .iter()
                                .any(|r| r.answer_ref.is_some())
                        {
                            *snapshot = Client::connect()?
                                .assemble_interaction_snapshot(snapshot.clone())?;
                        }
                    }
                    Frame::Update { update } => {
                        for change in &mut update.changes {
                            if let ProjectionChange::Response { response } = change {
                                if response.answer_ref.is_some() {
                                    Client::connect()?.hydrate_response(response)?;
                                }
                            }
                        }
                    }
                    _ => {}
                }
                Ok(frame)
            }
            ApiResponse::Error { message, .. } => Err(ClientError::Api(message)),
            _ => Err(ClientError::Api("unexpected interaction frame".into())),
        }
    }
}

/// Dedicated latest-value connection. No event replay and no overflow queue.
pub struct MonitorSubscription {
    conn: Connection,
}
impl MonitorSubscription {
    pub fn open(
        query: tachyon_api::monitor::MonitorQuery,
        after: Option<tachyon_api::monitor::MonitorVersion>,
    ) -> Result<Self, ClientError> {
        query.validate().map_err(ClientError::Monitor)?;
        let mut conn = Connection::connect(tachyon_util::daemon::socket_path())?;
        conn.send(&ApiRequest::MonitorSubscribe { query, after })?;
        Ok(Self { conn })
    }
    pub fn recv(&mut self) -> Result<tachyon_api::monitor::MonitorSnapshot, ClientError> {
        match self.conn.recv()? {
            ApiResponse::Monitor { snapshot } => Ok(snapshot),
            ApiResponse::MonitorError { error } => Err(ClientError::Monitor(error)),
            ApiResponse::Error { message, .. } => Err(ClientError::Api(message)),
            _ => Err(ClientError::Api("unexpected monitor response".into())),
        }
    }
}

#[cfg(test)]
mod monitor_tests {
    use super::*;
    use tachyon_api::monitor::*;
    use tachyon_api::transport::{read_request, write_response};

    #[test]
    fn monitor_client_keeps_typed_snapshots_stale_errors_and_disconnects() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "tachyon-monitor-client-{}-{nonce}.sock",
            std::process::id()
        ));
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let conn = Connection::connect(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let query = MonitorQuery {
            scope: MonitorScope::Host,
            after: None,
            limit: 100,
        };
        let expected = query.clone();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
            assert_eq!(
                read_request(&mut reader).unwrap(),
                ApiRequest::MonitorGet {
                    query: expected.clone()
                }
            );
            for epoch in ["old-daemon", "new-daemon"] {
                write_response(
                    &mut stream,
                    &ApiResponse::Monitor {
                        snapshot: MonitorSnapshot {
                            query: expected.clone(),
                            version: MonitorVersion {
                                epoch: epoch.into(),
                                sequence: 1,
                            },
                            payload: None,
                            stale: Some(MonitorError::Unavailable),
                        },
                    },
                )
                .unwrap();
            }
            write_response(
                &mut stream,
                &ApiResponse::MonitorError {
                    error: MonitorError::Stopped,
                },
            )
            .unwrap();
        });
        let mut client = Client { conn };
        let snapshot = client.monitor_get(query).unwrap();
        assert_eq!(snapshot.version.epoch, "old-daemon");
        assert_eq!(snapshot.stale, Some(MonitorError::Unavailable));
        let mut subscription = MonitorSubscription { conn: client.conn };
        assert_eq!(subscription.recv().unwrap().version.epoch, "new-daemon");
        assert!(matches!(
            subscription.recv(),
            Err(ClientError::Monitor(MonitorError::Stopped))
        ));
        assert!(matches!(subscription.recv(), Err(ClientError::Io(_))));
        server.join().unwrap();
    }
}

/// Dedicated durable feed connection. Empty batches are checkpoints/heartbeats.
pub struct OperationalSubscription {
    conn: Connection,
}

impl OperationalSubscription {
    pub fn open(
        scope: tachyon_api::todo::TodoScope,
        after: tachyon_api::operational_events::OperationalWatermark,
    ) -> Result<Self, ClientError> {
        let mut conn = Connection::connect(tachyon_util::daemon::socket_path())?;
        conn.send(&ApiRequest::OperationalSubscribe { scope, after })?;
        Ok(Self { conn })
    }

    pub fn recv(
        &mut self,
    ) -> Result<tachyon_api::operational_events::OperationalBatch, ClientError> {
        match self.conn.recv()? {
            ApiResponse::OperationalBatch { batch } => Ok(batch),
            ApiResponse::TodoError { error } => Err(ClientError::Todo(error)),
            ApiResponse::Error { message, .. } => Err(ClientError::Api(message)),
            _ => Err(ClientError::Api(
                "unexpected operational feed response".into(),
            )),
        }
    }
}
