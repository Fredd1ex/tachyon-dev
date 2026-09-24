//! Single foreground conversation, with daemon-incarnation-scoped revisions.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Revision {
    pub epoch: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Submit {
    pub conversation_id: String,
    pub session_id: String,
    pub command_id: String,
    pub text: String,
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Admission {
    /// Recorded before delivery. Execution may or may not have happened.
    Uncertain,
    /// Written to host transport, not an execution or display acknowledgement.
    Delivered,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub command: Submit,
    pub admission: Admission,
    /// Missing on receipts persisted before command origin tracking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<CommandOrigin>,
    /// Host acceptance is independent of the transport delivery status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted: Option<AcceptedTurn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandOrigin {
    pub session_id: String,
    pub command_id: String,
    pub host_message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcceptedTurn {
    pub turn_id: String,
    pub event_id: String,
}

/// Qualify a local turn without rebinding an already qualified (possibly old)
/// turn to the current session. Consumers must never join on the numeric suffix.
pub fn canonical_turn_id(session_id: &str, turn_id: &str) -> String {
    let turn_id = turn_id.strip_prefix("conversation:").unwrap_or(turn_id);
    let turn_id = turn_id
        .strip_prefix("foreground:")
        .filter(|rest| rest.contains(':'))
        .unwrap_or(turn_id);
    if turn_id.contains(':') {
        turn_id.into()
    } else {
        format!("{session_id}:{turn_id}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    pub revision: Revision,
    pub session_id: String,
    pub event: Option<crate::InteractionEventEnvelope>,
    /// Replace records by their exact keys. Never replay raw telemetry to derive these.
    #[serde(default)]
    pub changes: Vec<ProjectionChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub revision: Revision,
    pub conversation_id: String,
    pub session_id: Option<String>,
    pub host_state: Option<crate::AgentState>,
    /// Most recent 200 canonical messages; older history uses HistoryQuery.
    pub history: Vec<crate::HistoryEntry>,
    #[serde(default)]
    pub history_content: Vec<HistoryContent>,
    #[serde(default)]
    pub projection: Projection,
    /// Continue the projection at this offset using this snapshot's revision.
    #[serde(default)]
    pub projection_next: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryContent {
    pub event_id: String,
    pub reference: String,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionPage {
    pub revision: Revision,
    pub projection: Projection,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Projection {
    pub responses: Vec<Response>,
    pub works: Vec<Work>,
    pub progress: Vec<Progress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProjectionChange {
    Response { response: Response },
    Work { work: Work },
    Progress { progress: Progress },
    RemoveResponse { turn_id: String },
    RemoveWork { work_id: String },
    RemoveProgress { scope: crate::todo::TodoScope },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResponsePhase {
    Accepted,
    Working,
    Answering,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Failure {
    pub kind: FailureKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Provider,
    Timeout,
    Interrupted,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Metrics {
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub context_tokens: Option<u64>,
    pub context_window: Option<u64>,
    pub tools_started: u64,
    pub tools_finished: u64,
    /// Terminal assignment evidence count. None does not mean zero calls.
    #[serde(default)]
    pub observed_invocations: Option<u64>,
    pub timing: Option<crate::WorkTiming>,
    pub response_ms: Option<u64>,
    pub first_answer_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolActivity {
    pub call_id: String,
    pub name: String,
    pub finished: bool,
    pub success: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Response {
    pub session_id: String,
    pub turn_id: String,
    pub command_origin: Option<CommandOrigin>,
    pub generation: u64,
    pub revision: u64,
    pub phase: ResponsePhase,
    pub answer: String,
    /// Full UTF-8 byte length. Inline answer is a prefix of at most 64 KiB.
    #[serde(default)]
    pub answer_bytes: u64,
    /// Read with InteractionContent. Deltas append; finals use a separate key.
    #[serde(default)]
    pub answer_ref: Option<String>,
    pub pending: Option<String>,
    pub intents: Vec<crate::InteractionIntent>,
    pub failure: Option<Failure>,
    pub final_event_id: Option<String>,
    pub metrics: Metrics,
    pub latest_tool: Option<ToolActivity>,
    #[serde(default)]
    pub work_ids: Vec<String>,
    #[serde(default)]
    pub work_counts: WorkCounts,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkCounts {
    pub active: u64,
    pub completed: u64,
    pub unsuccessful: u64,
    pub unknown: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentPage {
    pub reference: String,
    pub offset: u64,
    /// Byte-oriented pagination, so UTF-8 characters can cross page boundaries.
    pub bytes: Vec<u8>,
    pub next_offset: Option<u64>,
}

impl ResponsePhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Interrupted)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkPhase {
    Waiting,
    Running,
    Reviewing,
    Completed,
    Blocked,
    Failed,
    Cancelled,
    TimedOut,
    Unknown,
}

impl WorkPhase {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Blocked | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

/// Created only from a host-admitted WorkRecord, never a model intent or error.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Work {
    pub work_id: String,
    pub worker_id: String,
    pub origin_turn_id: Option<String>,
    pub generation: u64,
    pub assignment: u64,
    pub attempt_id: Option<String>,
    pub revision: u64,
    pub title: String,
    pub phase: WorkPhase,
    pub metrics: Metrics,
    pub latest_tool: Option<ToolActivity>,
    /// Heavy results remain in the existing work/history/artifact authorities.
    pub result_available: bool,
    pub candidate_refs: Vec<String>,
    pub todo_scope: crate::todo::TodoScope,
}

/// None revision means unknown, not an empty checklist. Counts cover the scope,
/// not merely the bounded first page of items returned by Todo.List.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Progress {
    pub scope: crate::todo::TodoScope,
    pub scope_revision: Option<u64>,
    pub pending: u64,
    pub in_progress: u64,
    pub blocked: u64,
    pub completed: u64,
    pub cancelled: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Frame {
    Snapshot {
        snapshot: Snapshot,
    },
    Update {
        update: Update,
    },
    /// Replay coverage was lost. Reattach for canonical history and current state.
    ResnapshotRequired {
        current: Revision,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_snapshot_and_update_have_concrete_empty_projection() {
        let snapshot: Snapshot = serde_json::from_value(serde_json::json!({
            "revision":{"epoch":"old","sequence":3}, "conversation_id":"foreground",
            "session_id":null, "host_state":null, "history":[]
        }))
        .unwrap();
        assert_eq!(snapshot.projection, Projection::default());
        assert!(snapshot.projection_next.is_none());
        let update: Update = serde_json::from_value(serde_json::json!({
            "revision":{"epoch":"old","sequence":3}, "session_id":"old", "event":null
        }))
        .unwrap();
        assert!(update.changes.is_empty());
    }
    #[test]
    fn legacy_receipts_remain_uncertain_without_invented_binding() {
        let receipt: Receipt = serde_json::from_value(serde_json::json!({
            "command": {"conversation_id":"foreground", "session_id":"old", "command_id":"a", "text":"same", "cwd":null},
            "admission":"uncertain"
        })).unwrap();
        assert!(receipt.origin.is_none());
        assert!(receipt.accepted.is_none());
    }
    #[test]
    fn canonical_turn_is_idempotent_and_never_rebinds_old_sessions() {
        assert_eq!(canonical_turn_id("host", "7"), "host:7");
        assert_eq!(canonical_turn_id("host", "host:7"), "host:7");
        assert_eq!(canonical_turn_id("new", "old:7"), "old:7");
        assert_eq!(canonical_turn_id("new", "conversation:old:7"), "old:7");
        assert_eq!(
            canonical_turn_id("new", "conversation:foreground:old:7"),
            "old:7"
        );
        assert_eq!(
            canonical_turn_id("new", "conversation:foreground:7"),
            "foreground:7"
        );
    }
}
