#![forbid(unsafe_code)]

//! Protocol version and message types for the Tachyon daemon IPC.
//!
//! Messages are newline-delimited JSON (`Ndjson`) over a Unix domain socket.
//! Each `ApiRequest` maps 1:1 to a `tachyon` CLI command (or TUI action).

use serde::{Deserialize, Serialize};

/// Protocol version string.
pub const PROTO_VERSION: &str = "0.1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LifetimeClass {
    Short,
    Long,
    Persistent,
}

impl Default for LifetimeClass {
    fn default() -> Self {
        Self::Long
    }
}

impl std::fmt::Display for LifetimeClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Short => "short",
                Self::Long => "long",
                Self::Persistent => "persistent",
            }
        )
    }
}

/// A single agent's view of state, as seen by the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub id: String,
    pub task: String,
    pub state: AgentState,
    /// Host pid of the harness process, if running locally.
    pub pid: Option<u32>,
    /// Working directory / workspace for the agent.
    pub workspace: String,
    /// Unix timestamp (seconds) of creation.
    pub created_secs: u64,
    /// Whether the foreground has retained this session for future reuse.
    #[serde(default)]
    pub retained: bool,
    /// Optional Unix timestamp at which an explicit retention lease expires.
    #[serde(default)]
    pub lease_until_secs: Option<u64>,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub lifetime_class: LifetimeClass,
    #[serde(default)]
    pub purpose: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub last_activity_secs: u64,
    #[serde(default)]
    pub checkpoint_available: bool,
    /// Completed turns used by this logical session.
    #[serde(default)]
    pub turns_used: u32,
    /// Maximum turns before a short-lived session is eligible for release.
    #[serde(default)]
    pub turn_budget: Option<u32>,
    /// Human-facing task category, such as research, weather, or ml-training.
    #[serde(default)]
    pub task_type: String,
    /// Human-facing description of the assigned work.
    #[serde(default)]
    pub description: String,
    /// Whether this session is intended to survive daemon/process recovery.
    #[serde(default)]
    pub persistent: bool,
    /// Whether the worker currently runs inside a real sandbox backend.
    /// `false` means no enforced sandbox is active.
    #[serde(default)]
    pub sandboxed: bool,
    /// When a staged worker will be terminated, if staging is active.
    #[serde(default)]
    pub stage_until_secs: Option<u64>,
    #[serde(default)]
    pub logical_task_id: Option<String>,
    #[serde(default)]
    pub origin_turn_id: Option<String>,
    #[serde(default)]
    pub parent_task_id: Option<String>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
}

/// Lifecycle state names, mirroring the spec (CREATED → … → terminal).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentState {
    Created,
    Starting,
    Waiting,
    Running,
    Completed,
    Failed,
    Interrupted,
    Terminated,
    Released,
    Staged,
}

impl std::fmt::Display for AgentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AgentState::Created => "created",
                AgentState::Starting => "starting",
                AgentState::Waiting => "waiting",
                AgentState::Running => "running",
                AgentState::Completed => "completed",
                AgentState::Failed => "failed",
                AgentState::Interrupted => "interrupted",
                AgentState::Terminated => "terminated",
                AgentState::Released => "released",
                AgentState::Staged => "staged",
            }
        )
    }
}

impl AgentState {
    /// Whether this is a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            AgentState::Completed
                | AgentState::Failed
                | AgentState::Interrupted
                | AgentState::Terminated
                | AgentState::Released
        )
    }
}

/// Info about the daemon itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub pid: u32,
    pub version: String,
    pub proto_version: String,
    /// Whether a provider (and API key) is configured.
    pub provider_ready: bool,
    /// Path to the daemon's socket.
    pub socket: String,
}

/// All requests the daemon accepts. Mirrors the CLI subcommands 1:1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ApiRequest {
    /// `tachyon daemon status`
    DaemonStatus,
    /// `tachyon start <task>`
    AgentStart {
        task: String,
        cwd: Option<String>,
        #[serde(default)]
        depends_on: Vec<String>,
        #[serde(default)]
        lifetime_class: LifetimeClass,
        #[serde(default)]
        purpose: String,
        #[serde(default)]
        logical_task_id: Option<String>,
        #[serde(default)]
        origin_turn_id: Option<String>,
        #[serde(default)]
        parent_task_id: Option<String>,
        #[serde(default)]
        tool_call_id: Option<String>,
    },
    /// Submit work through the Background Coordinator boundary. Tachyond
    /// owns worker creation and returns the same authoritative agent stream.
    BackgroundDelegate {
        task: String,
        cwd: Option<String>,
        #[serde(default)]
        depends_on: Vec<String>,
        #[serde(default)]
        lifetime_class: LifetimeClass,
        #[serde(default)]
        purpose: String,
        #[serde(default)]
        logical_task_id: Option<String>,
        #[serde(default)]
        origin_turn_id: Option<String>,
        #[serde(default)]
        parent_task_id: Option<String>,
        #[serde(default)]
        tool_call_id: Option<String>,
    },
    /// `tachyon list` (aliases: ps, ls)
    AgentList,
    /// `tachyon status [<id>]` — if `Some`, single agent.
    AgentStatus { id: Option<String> },
    /// `tachyon cat <id>` (alias: inspect)
    AgentCat { id: String },
    /// `tachyon logs <id> [-f]`
    AgentLogs {
        id: String,
        follow: bool,
        lines: u32,
    },
    /// `tachyon stop <id>`
    AgentStop { id: String },
    /// Return the daemon-authoritative state without waiting on the worker.
    AgentAwait { id: String },
    /// Explicitly terminate, persist, and clean up a worker.
    AgentRelease { id: String },
    /// Stage a worker for delayed termination.
    AgentStage { id: String, ttl_secs: u64 },
    /// Retain a worker session, optionally until a Unix timestamp.
    AgentRetain {
        id: String,
        lease_until_secs: Option<u64>,
        #[serde(default)]
        lifetime_class: Option<LifetimeClass>,
    },
    /// Replace a worker while retaining its id and dependencies.
    AgentReplan { id: String, task: String },
    /// `tachyon interrupt <id>`
    AgentInterrupt { id: String },
    /// `tachyon kill <id>`
    AgentKill { id: String },
    /// `tachyon resume <id>`
    AgentResume { id: String },
    /// `tachyon restart <id>`
    AgentRestart { id: String },
    /// `tachyon exec <id> -- <cmd>`
    AgentExec { id: String, command: Vec<String> },
    /// `tachyon attach <id>`
    AgentAttach { id: String },
    /// `tachyon top`
    Top,
    /// Subscribe to an agent's live output stream. Unlike other requests, the
    /// connection stays open and the daemon writes a sequence of `Event`
    /// responses until the agent finishes.
    AgentSubscribe { id: String },
    /// Send a chat message to a running harness (ghost in `--chat` mode).
    AgentChat { id: String, text: String },
    /// Send a user chat message to the foreground. The foreground replies
    /// asynchronously; subscribe via `ForegroundSubscribe` to receive its
    /// stream (text tokens + agent spawns).
    #[serde(alias = "orchestrator_chat")]
    ForegroundChat { text: String },
    /// Subscribe to the foreground conversation stream.
    #[serde(alias = "orchestrator_subscribe")]
    ForegroundSubscribe,
}

/// The kind of a streamed event line.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EventStream {
    Stdout,
    Stderr,
    Stage,
    Exit,
}

/// Structured Ghost event carried in `ApiResponse::Event.data` as JSON.
/// Legacy line markers remain temporarily for daemon supervision compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentEvent {
    Usage {
        turn: Option<u64>,
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
    },
    Status {
        turn: Option<u64>,
        phase: String,
        message: String,
    },
    Reply {
        turn: Option<u64>,
        text: String,
        final_reply: bool,
    },
    ReplyDelta {
        turn: Option<u64>,
        text: String,
    },
    Timing {
        turn: u64,
        stage: String,
        elapsed_ms: u64,
    },
    WorkerStarted {
        turn: Option<u64>,
        worker_id: String,
        objective: String,
    },
    WorkerCompleted {
        worker_id: String,
        objective: String,
        result: String,
        artifacts: Vec<String>,
        context: String,
        suggested_reuse: bool,
    },
    WorkerReleaseRequested {
        reason: String,
    },
    ToolStarted {
        turn: Option<u64>,
        id: String,
        name: String,
        arguments: String,
    },
    ToolFinished {
        turn: Option<u64>,
        id: String,
        output: String,
    },
    Error {
        turn: Option<u64>,
        message: String,
    },
}

/// The producer responsible for an event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Actor {
    User,
    #[serde(alias = "orchestrator")]
    Foreground,
    Background,
    Worker {
        id: String,
    },
    System,
}

/// Stable name for the typed payload carried by an [`EventEnvelope`].
///
/// This alias keeps the existing `AgentEvent` API and wire representation
/// intact while allowing consumers to describe the payload as an event kind.
pub type EventKind = AgentEvent;

/// A correlated event emitted during a conversation or background task.
///
/// `kind` is flattened so the existing `AgentEvent` `kind` discriminator and
/// payload fields remain at the top level of the JSON object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventEnvelope {
    pub event_id: u64,
    pub session_id: String,
    pub conversation_id: Option<String>,
    pub turn_id: Option<String>,
    pub task_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub actor: Actor,
    pub sequence: u64,
    pub occurred_at_ms: u64,
    #[serde(flatten)]
    pub kind: EventKind,
}

/// The daemon's typed response to a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiResponse {
    /// A generic success with an optional message.
    Ok { message: Option<String> },
    /// A typed error.
    Error { code: u16, message: String },
    /// Response to `DaemonStatus`.
    DaemonStatus { info: DaemonInfo },
    /// Response to `AgentStart` / `AgentStatus` / `AgentCat`.
    Agent { info: AgentInfo },
    /// Response to `AgentList` / `Top`.
    Agents { agents: Vec<AgentInfo> },
    /// Response to `AgentLogs`.
    Logs { id: String, lines: Vec<String> },
    /// Response to `AgentExec`.
    Exec {
        id: String,
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    /// Response to `AgentAttach`.
    Attach { id: String, output: String },
    /// A streamed event line from an `AgentSubscribe`/`ForegroundSubscribe`
    /// connection.
    Event { stream: EventStream, data: String },
    /// Acknowledgement after `AgentChat` / `ForegroundChat`.
    Chat { id: String },
}

impl ApiResponse {
    pub fn error(msg: impl Into<String>) -> Self {
        ApiResponse::Error {
            message: msg.into(),
            code: 1,
        }
    }
    pub fn ok() -> Self {
        ApiResponse::Ok { message: None }
    }
    pub fn ok_msg(msg: impl Into<String>) -> Self {
        ApiResponse::Ok {
            message: Some(msg.into()),
        }
    }
    pub fn as_error(&self) -> Option<&str> {
        match self {
            ApiResponse::Error { message, .. } => Some(message),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_requests_use_stable_wire_names_and_fields() {
        let cases = [
            (
                ApiRequest::AgentAwait { id: "a1".into() },
                r#"{"cmd":"agent_await","id":"a1"}"#,
            ),
            (
                ApiRequest::AgentRelease { id: "a1".into() },
                r#"{"cmd":"agent_release","id":"a1"}"#,
            ),
            (
                ApiRequest::AgentReplan {
                    id: "a1".into(),
                    task: "new objective".into(),
                },
                r#"{"cmd":"agent_replan","id":"a1","task":"new objective"}"#,
            ),
        ];
        for (request, expected) in cases {
            assert_eq!(serde_json::to_string(&request).unwrap(), expected);
            assert_eq!(
                serde_json::from_str::<ApiRequest>(expected).unwrap(),
                request
            );
        }
    }

    #[test]
    fn released_is_terminal_and_serializes_lowercase() {
        assert!(AgentState::Released.is_terminal());
        assert_eq!(
            serde_json::to_string(&AgentState::Released).unwrap(),
            "\"released\""
        );
    }

    #[test]
    fn background_delegate_has_a_stable_wire_name() {
        let request = ApiRequest::BackgroundDelegate {
            task: "inspect the repository".into(),
            cwd: Some("/tmp/workspace".into()),
            depends_on: vec!["a1".into()],
            lifetime_class: LifetimeClass::Short,
            purpose: "research".into(),
            logical_task_id: Some("task-2".into()),
            origin_turn_id: Some("turn-4".into()),
            parent_task_id: Some("task-1".into()),
            tool_call_id: Some("call-7".into()),
        };
        let wire = serde_json::to_string(&request).unwrap();
        assert!(wire.contains(r#""cmd":"background_delegate""#));
        assert_eq!(serde_json::from_str::<ApiRequest>(&wire).unwrap(), request);
    }

    #[test]
    fn task_correlation_fields_default_when_omitted() {
        let request: ApiRequest =
            serde_json::from_str(r#"{"cmd":"agent_start","task":"inspect","cwd":null}"#).unwrap();
        assert_eq!(
            request,
            ApiRequest::AgentStart {
                task: "inspect".into(),
                cwd: None,
                depends_on: Vec::new(),
                lifetime_class: LifetimeClass::Long,
                purpose: String::new(),
                logical_task_id: None,
                origin_turn_id: None,
                parent_task_id: None,
                tool_call_id: None,
            }
        );

        let info: AgentInfo = serde_json::from_str(
            r#"{"id":"a1","task":"inspect","state":"created","pid":null,"workspace":"/tmp/workspace","created_secs":1}"#,
        )
        .unwrap();
        assert_eq!(info.logical_task_id, None);
        assert_eq!(info.origin_turn_id, None);
        assert_eq!(info.parent_task_id, None);
        assert_eq!(info.tool_call_id, None);
    }

    #[test]
    fn worker_completion_preserves_evidence_identity() {
        let event = AgentEvent::WorkerCompleted {
            worker_id: "worker-1".into(),
            objective: "inspect the input".into(),
            result: "the input is valid".into(),
            artifacts: Vec::new(),
            context: "workspace retained".into(),
            suggested_reuse: true,
        };
        let wire = serde_json::to_string(&event).unwrap();
        let restored: AgentEvent = serde_json::from_str(&wire).unwrap();
        match restored {
            AgentEvent::WorkerCompleted {
                worker_id,
                objective,
                result,
                ..
            } => {
                assert_eq!(worker_id, "worker-1");
                assert_eq!(objective, "inspect the input");
                assert_eq!(result, "the input is valid");
            }
            _ => panic!("expected worker completion"),
        }
    }

    #[test]
    fn event_envelope_round_trips_with_all_correlation_ids() {
        let event = EventEnvelope {
            event_id: 42,
            session_id: "session-1".into(),
            conversation_id: Some("conversation-1".into()),
            turn_id: Some("turn-3".into()),
            task_id: Some("task-child".into()),
            parent_task_id: Some("task-parent".into()),
            tool_call_id: Some("call-7".into()),
            actor: Actor::Worker {
                id: "worker-1".into(),
            },
            sequence: 9,
            occurred_at_ms: 1_725_000_000_123,
            kind: AgentEvent::ToolFinished {
                turn: Some(3),
                id: "call-7".into(),
                output: "complete".into(),
            },
        };

        let wire = serde_json::to_string(&event).unwrap();
        assert_eq!(
            wire,
            r#"{"event_id":42,"session_id":"session-1","conversation_id":"conversation-1","turn_id":"turn-3","task_id":"task-child","parent_task_id":"task-parent","tool_call_id":"call-7","actor":{"kind":"worker","id":"worker-1"},"sequence":9,"occurred_at_ms":1725000000123,"kind":"tool_finished","turn":3,"id":"call-7","output":"complete"}"#
        );
        assert_eq!(serde_json::from_str::<EventEnvelope>(&wire).unwrap(), event);
    }

    #[test]
    fn event_envelope_round_trips_without_optional_correlation_ids() {
        let event = EventEnvelope {
            event_id: 1,
            session_id: "session-1".into(),
            conversation_id: None,
            turn_id: None,
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: Actor::Foreground,
            sequence: 0,
            occurred_at_ms: 1_725_000_000_000,
            kind: EventKind::Reply {
                turn: None,
                text: "hello".into(),
                final_reply: true,
            },
        };

        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(value["kind"], "reply");
        assert_eq!(value["actor"]["kind"], "foreground");
        assert!(value["conversation_id"].is_null());
        assert_eq!(
            serde_json::from_value::<EventEnvelope>(value).unwrap(),
            event
        );
    }

    #[test]
    fn foreground_requests_accept_legacy_wire_names() {
        assert_eq!(
            serde_json::from_str::<ApiRequest>(r#"{"cmd":"orchestrator_chat","text":"hello"}"#)
                .unwrap(),
            ApiRequest::ForegroundChat {
                text: "hello".into()
            }
        );
        assert_eq!(
            serde_json::from_str::<ApiRequest>(r#"{"cmd":"orchestrator_subscribe"}"#).unwrap(),
            ApiRequest::ForegroundSubscribe
        );
        assert_eq!(
            serde_json::to_string(&ApiRequest::ForegroundSubscribe).unwrap(),
            r#"{"cmd":"foreground_subscribe"}"#
        );
        assert_eq!(
            serde_json::to_string(&ApiRequest::ForegroundChat {
                text: "hello".into()
            })
            .unwrap(),
            r#"{"cmd":"foreground_chat","text":"hello"}"#
        );
    }

    #[test]
    fn foreground_actor_accepts_legacy_wire_name() {
        assert_eq!(
            serde_json::from_str::<Actor>(r#"{"kind":"orchestrator"}"#).unwrap(),
            Actor::Foreground
        );
        assert_eq!(
            serde_json::to_string(&Actor::Foreground).unwrap(),
            r#"{"kind":"foreground"}"#
        );
    }
}
