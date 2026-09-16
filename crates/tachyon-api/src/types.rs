#![forbid(unsafe_code)]

//! Protocol version and message types for the Tachyon daemon IPC.
//!
//! Messages are newline-delimited JSON (`Ndjson`) over a Unix domain socket.
//! Requests include user-facing actions and daemon-owned record operations.

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

/// One daemon-owned assignment delivered to a worker harness.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkRequest {
    /// Explicit host-validated handles, not automatic prompt capture.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_refs: Vec<crate::context::ResourceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<WorkConstraints>,
    /// Host attempt identity and bounded repair feedback; absent for ordinary work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<WorkAttempt>,
    /// Stable idempotency key for this logical assignment.
    pub work_id: String,
    pub objective: String,
    /// Process generation and warm-worker assignment fence.
    pub generation: u64,
    pub assignment: u64,
    /// Absolute Unix deadline in milliseconds.
    pub deadline_ms: u64,
    pub lifetime_class: LifetimeClass,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkConstraints {
    pub permissions: WorkPermissions,
    pub input_context: Vec<crate::context::Resource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkPermissions {
    pub task_type: WorkTaskType,
    pub allow_exec: bool,
    pub allow_python: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkTaskType {
    CodingReadOnly,
    Coding,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkAttempt {
    pub id: String,
    pub feedback: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<crate::continuation::ContinuationBootstrap>,
}

/// Non-terminal progress associated with a [`WorkRequest`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkEvent {
    pub work_id: String,
    pub generation: u64,
    pub assignment: u64,
    #[serde(flatten)]
    pub kind: WorkEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum WorkEventKind {
    Started,
    Progress { message: String },
}

/// Exactly one terminal outcome for a logical work assignment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkResult {
    /// Final worker observations before cleanup, never permissions or installed-version authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_context: Option<crate::context::WorkerContextMetadata>,
    /// Attempt fence copied from the assignment, not worker-selected authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    /// Host-resolved immutable artifact IDs; worker claims are discarded on collection.
    /// `WorkOutcome` artifact strings remain workspace paths for history/legacy workers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_refs: Option<Vec<String>>,
    /// Host-canonical model-boundary revision; absent for legacy/non-broker workers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_revision: Option<u64>,
    pub work_id: String,
    pub objective: String,
    pub generation: u64,
    pub assignment: u64,
    /// Assignment-scoped observed tool results, not worker-authored citations.
    #[serde(default)]
    pub evidence: WorkEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing: Option<WorkTiming>,
    #[serde(flatten)]
    pub outcome: WorkOutcome,
}

/// Measured wall-clock stages, not additive CPU time. Missing values are unknown.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkTiming {
    /// Ghost execution including setup/cleanup, excluding daemon review.
    pub execution_ms: Option<u64>,
    /// Sequential model request waits within execution, including provider retries.
    pub inference_ms: Option<u64>,
    /// Sequential parallel-tool batch waits within execution (not summed call durations).
    pub tool_ms: Option<u64>,
    /// Daemon candidate-to-decision wait, including coordinator queue/IPC, not model time.
    pub review_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkEvidence {
    pub tools: Vec<WorkToolEvidence>,
    /// Results excluded by the evidence budget; absence is not proof of success.
    pub omitted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkToolEvidence {
    pub call_id: Option<String>,
    pub parent_call_id: Option<String>,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    /// Bounded native ToolResult envelope, including error and truncation status.
    pub output: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WorkOutcome {
    Completed {
        result: String,
        #[serde(default)]
        artifacts: Vec<String>,
        #[serde(default)]
        context: String,
        #[serde(default)]
        suggested_reuse: bool,
    },
    Blocked {
        reason: String,
    },
    Failed {
        message: String,
    },
    Cancelled {
        reason: String,
    },
    TimedOut {
        deadline_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkReviewRequest {
    pub review_id: String,
    pub coordinator_generation: u64,
    pub candidate: WorkResult,
    pub worker: WorkReviewContext,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkReviewContext {
    pub worker_id: String,
    pub current_lifetime_class: LifetimeClass,
    pub turns_used: u32,
    pub turn_budget: Option<u32>,
    pub purpose: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkReviewDecision {
    pub review_id: String,
    pub coordinator_generation: u64,
    pub work_id: String,
    pub generation: u64,
    pub assignment: u64,
    #[serde(flatten)]
    pub recommendation: WorkReviewRecommendation,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "recommendation", rename_all = "snake_case")]
pub enum WorkReviewRecommendation {
    Accept { lifecycle: LifecycleRecommendation },
    Rework { revised_objective: Option<String> },
    Inconclusive { failure: WorkReviewFailure },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "lifecycle", rename_all = "snake_case")]
pub enum LifecycleRecommendation {
    KeepCurrent,
    Release,
    Retain { lifetime_class: LifetimeClass },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkReviewFailure {
    InvalidRequest,
    ModelUnavailable,
    TimedOut,
    ProviderError,
    MalformedOutput,
    TruncatedOutput,
    CoordinatorUnavailable,
}

impl WorkOutcome {
    pub fn completed_result(&self) -> Option<&str> {
        match self {
            Self::Completed { result, .. } => Some(result),
            _ => None,
        }
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
    /// Independently supervised semantic result reviewer state.
    #[serde(default)]
    pub background: BackgroundCoordinatorInfo,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackgroundCoordinatorInfo {
    pub online: bool,
    pub generation: u64,
    #[serde(default)]
    pub pending_reviews: Vec<PendingWorkReviewInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingWorkReviewInfo {
    pub work_id: String,
    pub worker_id: String,
    pub deadline_ms: u64,
}

/// Immutable, top-level research metadata. Creation does not start work.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Research {
    /// Server-generated `research-` followed by 32 lowercase UUID hex digits.
    pub id: String,
    pub title: String,
    pub objective: String,
    /// Server creation time, Unix milliseconds; unchanged on replay.
    pub created_at_ms: u64,
}

/// Draft is explicitly inert: no execution, budget, permissions, or inference.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CampaignStatus {
    Draft,
    Running,
    Cancelling,
    Cancelled,
    Accepted,
    AwaitingAcceptance,
    AcceptedHuman,
    Rejected,
    Unverified,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Campaign {
    /// Server-generated `campaign-` followed by 32 lowercase UUID hex digits.
    pub id: String,
    pub research_id: String,
    pub title: String,
    pub objective: String,
    /// Server creation time, Unix milliseconds; unchanged on replay.
    pub created_at_ms: u64,
    pub status: CampaignStatus,
}

/// Bounded launch observations; persisted active leases are not proof of a live process.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CampaignActivity {
    /// Process-local admission queues, not durable active leases or money holds.
    #[serde(default)]
    pub host_resident_waiting: usize,
    #[serde(default)]
    pub host_execution_waiting: usize,
    #[serde(default)]
    pub host_model_waiting: usize,
    pub owned: bool,
    pub admitted: usize,
    pub queued: usize,
    pub active: usize,
    pub waiting: usize,
    pub terminal: usize,
}

/// Creation limits are UTF-8 byte counts; blank text is rejected, not normalized.
pub const RESEARCH_TITLE_MAX_BYTES: usize = 256;
pub const RESEARCH_OBJECTIVE_MAX_BYTES: usize = 16_384;
pub const RESEARCH_COMMAND_ID_MAX_BYTES: usize = 128;
pub const RESEARCH_LIST_MAX_LIMIT: u32 = 100;

fn default_research_limit() -> u32 {
    50
}

/// All requests the daemon accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ApiRequest {
    /// Same-user host retention control; accepts a Research or Campaign ID.
    LocalRetentionSet {
        id: String,
        archived: bool,
    },
    LocalRetentionGet {
        id: String,
    },
    CampaignAcceptanceGet(crate::campaign::AcceptanceQuery),
    CampaignAcceptanceDecide(crate::campaign::AcceptanceDecision),
    /// Privileged same-user host input, never a worker tool or cross-campaign grant.
    CampaignAttentionList {
        id: String,
        after: Option<String>,
        limit: usize,
    },
    CampaignAttentionAnswer {
        id: String,
        work_id: String,
        request_id: String,
        generation: u64,
        instruction_revision: u64,
        answer: String,
    },
    /// Same-user local host control only. Never exposed as a model tool.
    CampaignRun {
        manifest: crate::campaign::CampaignManifest,
        unisolated_development: bool,
    },
    CampaignCancel {
        id: String,
    },
    CampaignProgress {
        id: String,
    },
    CampaignResume {
        id: String,
        unisolated_development: bool,
    },
    CampaignContinue {
        id: String,
        request: crate::continuation::ContinuationRequest,
        #[serde(default)]
        unisolated_development: bool,
    },
    CampaignInspect {
        id: String,
    },
    CampaignIntegrationSnapshot {
        id: String,
        paths: Vec<String>,
    },
    CampaignIntegrate {
        id: String,
        plan: crate::integration::IntegrationPlan,
        expected_state: String,
        #[serde(default)]
        confirm: bool,
    },
    CampaignRecover {
        id: String,
        unisolated_development: bool,
    },
    CampaignReconcile {
        id: String,
        receipt: crate::campaign::ReconciliationReceipt,
        #[serde(default)]
        unisolated_development: bool,
        #[serde(default)]
        confirm_authoritative: bool,
    },
    /// Host control API only; does not grant workers filesystem or database access.
    ArtifactGet {
        scope: String,
        id: String,
    },
    ArtifactRead {
        scope: String,
        id: String,
        offset: u64,
        limit: u32,
    },
    /// Cursor is the last returned registration ID (ordered by its opaque hash).
    ArtifactList {
        scope: String,
        after: Option<String>,
        limit: u32,
    },
    /// Command IDs share a namespace across both creation operations. Exact
    /// payload retries replay the original record; reuse otherwise conflicts.
    ResearchCreate {
        command_id: String,
        title: String,
        objective: String,
    },
    ResearchGet {
        id: String,
    },
    /// Ascending ID order, strictly after the cursor (not a snapshot).
    ResearchList {
        after: Option<String>,
        #[serde(default = "default_research_limit")]
        limit: u32,
    },
    /// Creates only inert Draft metadata under an existing Research.
    CampaignCreate {
        command_id: String,
        research_id: String,
        title: String,
        objective: String,
    },
    CampaignGet {
        id: String,
    },
    /// Same exclusive ID ordering as ResearchList. Keep the filter unchanged
    /// between pages. None lists all campaigns; a nonexistent Research ID
    /// yields an empty page.
    CampaignList {
        research_id: Option<String>,
        after: Option<String>,
        #[serde(default = "default_research_limit")]
        limit: u32,
    },
    /// `tachyon daemon status`
    DaemonStatus,
    /// `tachyon start <task>`
    AgentStart {
        task: String,
        /// Existing absolute host directory; absent/empty selects a new managed workspace.
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
        #[serde(default)]
        deadline_ms: Option<u64>,
    },
    /// Submit work through the Background Coordinator boundary. Tachyond
    /// owns worker creation and returns the same authoritative agent stream.
    BackgroundDelegate {
        task: String,
        /// Host-selected directory, not model prose. Absent/empty selects managed work.
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
        #[serde(default)]
        deadline_ms: Option<u64>,
    },
    /// `tachyon list` (aliases: ps, ls)
    AgentList,
    /// `tachyon status [<id>]` — if `Some`, single agent.
    AgentStatus {
        id: Option<String>,
    },
    /// `tachyon cat <id>` (alias: inspect)
    AgentCat {
        id: String,
    },
    /// `tachyon logs <id> [-f]`
    AgentLogs {
        id: String,
        follow: bool,
        lines: u32,
    },
    /// `tachyon stop <id>`
    AgentStop {
        id: String,
    },
    /// Return the daemon-authoritative state without waiting on the worker.
    AgentAwait {
        id: String,
    },
    /// Explicitly terminate, persist, and clean up a worker.
    AgentRelease {
        id: String,
    },
    /// Stage a worker for delayed termination.
    AgentStage {
        id: String,
        ttl_secs: u64,
    },
    /// Retain a worker session, optionally until a Unix timestamp.
    AgentRetain {
        id: String,
        lease_until_secs: Option<u64>,
        #[serde(default)]
        lifetime_class: Option<LifetimeClass>,
    },
    /// Replace a worker while retaining its id and dependencies.
    AgentReplan {
        id: String,
        task: String,
    },
    /// `tachyon interrupt <id>`
    AgentInterrupt {
        id: String,
    },
    /// `tachyon kill <id>`
    AgentKill {
        id: String,
    },
    /// `tachyon resume <id>`
    AgentResume {
        id: String,
    },
    /// `tachyon restart <id>`
    AgentRestart {
        id: String,
    },
    /// `tachyon exec <id> -- <cmd>`
    AgentExec {
        id: String,
        command: Vec<String>,
    },
    /// `tachyon attach <id>`
    AgentAttach {
        id: String,
    },
    /// `tachyon top`
    Top,
    /// Subscribe to an agent's live output stream. Unlike other requests, the
    /// connection stays open and the daemon writes a sequence of `Event`
    /// responses until the agent finishes.
    AgentSubscribe {
        id: String,
    },
    /// Subscribe to one logical work assignment. Terminal results are replayed
    /// even if its warm worker has since accepted another assignment.
    WorkSubscribe {
        work_id: String,
    },
    /// Send a chat message to a running harness (ghost in `--chat` mode).
    AgentChat {
        id: String,
        text: String,
    },
    /// Send a user chat message to the foreground. The foreground replies
    /// asynchronously; subscribe via `ForegroundSubscribe` to receive its
    /// stream (text tokens + agent spawns).
    #[serde(alias = "orchestrator_chat")]
    ForegroundChat {
        text: String,
        /// Host-selected absolute directory. Omitted means an isolated managed workspace.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },
    /// Subscribe to the foreground conversation stream.
    #[serde(alias = "orchestrator_subscribe")]
    ForegroundSubscribe,
    /// Query canonical user-visible history in the half-open time range.
    HistoryQuery {
        since_ms: u64,
        until_ms: u64,
        #[serde(default = "default_history_limit")]
        limit: u32,
    },
    /// Retrieve bounded private context for one Foreground turn. Tachyond
    /// enforces retrieval policy and never exposes raw database access.
    MemoryRecall {
        query: String,
        conversation_id: String,
        turn: u64,
        #[serde(default)]
        include_history: bool,
        #[serde(default = "default_memory_recall_items")]
        max_items: u32,
        #[serde(default = "default_memory_recall_chars")]
        max_chars: u32,
    },
    /// Apply one model-proposed memory action after daemon-side validation.
    MemoryMutate {
        intent: MemoryIntent,
        source_event_id: String,
        conversation_id: String,
        turn: u64,
        occurred_at_ms: u64,
    },
    /// Persist a one-shot user reminder. Tachyond computes and owns the deadline.
    ReminderCreate {
        source_event_id: String,
        conversation_id: String,
        turn: u64,
        text: String,
        delay_seconds: Option<u64>,
        local_time: Option<String>,
        day: Option<ScheduleDay>,
        created_at_ms: u64,
    },
    /// List reminders that have not reached a terminal state.
    ReminderList,
    /// Cancel a pending reminder by its daemon-authoritative ID.
    ReminderCancel {
        id: String,
        conversation_id: String,
        turn: u64,
    },
    /// Schedule daemon-owned agent work to start at, or finish by, a deadline.
    ScheduledTaskCreate {
        source_event_id: String,
        conversation_id: String,
        turn: u64,
        objective: String,
        mode: ScheduledTaskMode,
        delay_seconds: Option<u64>,
        local_time: Option<String>,
        day: Option<ScheduleDay>,
        created_at_ms: u64,
    },
    /// List pending or running daemon-owned scheduled agent work.
    ScheduledTaskList,
}

fn default_history_limit() -> u32 {
    100
}

fn default_memory_recall_items() -> u32 {
    12
}

fn default_memory_recall_chars() -> u32 {
    6000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoryRole {
    User,
    Assistant,
    Notification,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoryKind {
    #[default]
    Conversation,
    Task,
    ConversationSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryEntry {
    pub event_id: String,
    #[serde(default)]
    pub kind: HistoryKind,
    pub conversation_id: String,
    pub turn_id: Option<String>,
    pub occurred_at_ms: u64,
    pub role: HistoryRole,
    pub text: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub task_state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextCompactionCommand {
    pub request_id: String,
    pub epoch: u64,
    pub target_tokens: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryRecallKind {
    Preference,
    TaskHistory,
    History,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryRecallItem {
    pub kind: MemoryRecallKind,
    #[serde(default)]
    pub memory_id: Option<String>,
    #[serde(default)]
    pub descriptor: Option<MemoryDescriptor>,
    pub text: String,
    pub occurred_at_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    Fact,
    #[default]
    Preference,
    Constraint,
    Goal,
    Routine,
    Relationship,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCardinality {
    One,
    #[default]
    Many,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryDescriptor {
    #[serde(default)]
    pub kind: MemoryKind,
    #[serde(default = "default_memory_namespace")]
    pub namespace: String,
    #[serde(default = "default_memory_relation")]
    pub relation: String,
    #[serde(default = "default_memory_scope")]
    pub scope: String,
    #[serde(default)]
    pub cardinality: MemoryCardinality,
    #[serde(default)]
    pub topics: Vec<String>,
}

fn default_memory_namespace() -> String {
    "uncategorized".into()
}

fn default_memory_relation() -> String {
    "prefers".into()
}

fn default_memory_scope() -> String {
    "global".into()
}

impl Default for MemoryDescriptor {
    fn default() -> Self {
        Self {
            kind: MemoryKind::Preference,
            namespace: default_memory_namespace(),
            relation: default_memory_relation(),
            scope: default_memory_scope(),
            cardinality: MemoryCardinality::Many,
            topics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum MemoryIntent {
    Ignore,
    Remember {
        #[serde(default)]
        descriptor: MemoryDescriptor,
        value: String,
    },
    Forget {
        target_ids: Vec<String>,
    },
    Correct {
        target_ids: Vec<String>,
        #[serde(default)]
        descriptor: MemoryDescriptor,
        value: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryMutationKind {
    Remember,
    Forget,
    Correct,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MemoryMutationResult {
    Ignored,
    Applied {
        kind: MemoryMutationKind,
        memory_id: String,
        replaced_memory_id: Option<String>,
    },
    AlreadyApplied {
        kind: MemoryMutationKind,
        memory_id: String,
    },
    Rejected {
        reason: String,
    },
    Unavailable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReminderStatus {
    Pending,
    Delivering,
    Delivered,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleDay {
    Next,
    Today,
    Tomorrow,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReminderInfo {
    pub id: String,
    pub conversation_id: String,
    pub turn: u64,
    pub text: String,
    pub created_at_ms: u64,
    pub due_at_ms: u64,
    pub status: ReminderStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduledTaskMode {
    StartAt,
    FinishBy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduledTaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduledTaskInfo {
    pub id: String,
    pub conversation_id: String,
    pub turn: u64,
    pub objective: String,
    pub mode: ScheduledTaskMode,
    pub created_at_ms: u64,
    pub due_at_ms: u64,
    pub status: ScheduledTaskStatus,
    pub work_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackgroundScheduleRequest {
    pub request_id: String,
    pub action: BackgroundScheduleAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum BackgroundScheduleAction {
    Create {
        source_event_id: String,
        conversation_id: String,
        turn: u64,
        text: String,
        delay_seconds: Option<u64>,
        local_time: Option<String>,
        day: Option<ScheduleDay>,
        created_at_ms: u64,
    },
    List,
    Cancel {
        id: String,
    },
    CreateTask {
        source_event_id: String,
        conversation_id: String,
        turn: u64,
        objective: String,
        mode: ScheduledTaskMode,
        delay_seconds: Option<u64>,
        local_time: Option<String>,
        day: Option<ScheduleDay>,
        created_at_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackgroundScheduleDecision {
    pub request_id: String,
    pub action: BackgroundScheduleAction,
    pub approved: bool,
    pub reason: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRegistration {
    pub id: String,
    pub path: String,
    pub kind: String,
    pub description: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub task_id: Option<String>,
    pub work_id: Option<String>,
    pub generation: Option<u64>,
    pub assignment: Option<u64>,
    pub attempt_id: Option<String>,
    /// Missing on legacy hash-only registrations; never implies durable publication.
    #[serde(default)]
    pub publication: ArtifactPublication,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ArtifactPublication {
    #[default]
    Pending,
    Ready {
        version: String,
    },
    Failed {
        reason: String,
    },
}

#[cfg(test)]
mod artifact_wire_tests {
    use super::*;

    #[test]
    fn legacy_hash_only_registration_is_pending_and_ready_ack_round_trips() {
        let legacy = serde_json::json!({
            "id":"a", "path":"report.txt", "kind":"report", "description":"report",
            "size_bytes":3, "sha256":"abc"
        });
        let mut artifact: ArtifactRegistration = serde_json::from_value(legacy).unwrap();
        assert_eq!(artifact.publication, ArtifactPublication::Pending);
        artifact.publication = ArtifactPublication::Ready {
            version: "abc".into(),
        };
        let ack = ApiResponse::Artifact {
            artifact: Some(artifact),
        };
        let decoded: ApiResponse =
            serde_json::from_str(&serde_json::to_string(&ack).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(ack).unwrap(),
            serde_json::to_value(decoded).unwrap()
        );
        for json in [
            r#"{"cmd":"artifact_get","scope":"work","id":"a"}"#,
            r#"{"cmd":"artifact_list","scope":"work","after":null,"limit":10}"#,
            r#"{"cmd":"artifact_read","scope":"work","id":"a","offset":0,"limit":10}"#,
        ] {
            let request: ApiRequest = serde_json::from_str(json).unwrap();
            assert_eq!(
                serde_json::from_str::<ApiRequest>(&serde_json::to_string(&request).unwrap())
                    .unwrap(),
                request
            );
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolTelemetryIdentity {
    pub task_id: Option<String>,
    pub work_id: Option<String>,
    pub generation: Option<u64>,
    pub assignment: Option<u64>,
    pub attempt_id: Option<String>,
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
        #[serde(default)]
        context_tokens: u32,
        #[serde(default)]
        context_window: Option<u32>,
    },
    ContextCompacted {
        request_id: String,
        epoch: u64,
        retained_context_tokens: u32,
    },
    MemorySaved {
        turn: Option<u64>,
        memory_id: String,
    },
    MemoryRecalled {
        turn: Option<u64>,
        preference_count: u32,
        history_count: u32,
    },
    MemoryMutation {
        turn: Option<u64>,
        result: MemoryMutationResult,
    },
    ReminderScheduled {
        turn: Option<u64>,
        reminder_id: String,
        due_at_ms: u64,
    },
    ReminderCancelled {
        turn: Option<u64>,
        reminder_id: String,
    },
    ReminderFired {
        turn: Option<u64>,
        reminder_id: String,
    },
    ScheduledTaskCreated {
        turn: Option<u64>,
        schedule_id: String,
        due_at_ms: u64,
        mode: ScheduledTaskMode,
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
    WorkCandidate {
        candidate: WorkResult,
    },
    WorkProgress {
        event: WorkEvent,
    },
    WorkResult {
        result: WorkResult,
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
    ToolTelemetry {
        tool_name: String,
        call_id: Option<String>,
        duration_ms: u64,
        success: bool,
        truncated: bool,
        bytes_out: u64,
        error_code: Option<String>,
        identity: ToolTelemetryIdentity,
    },
    ArtifactRegistered {
        artifact: ArtifactRegistration,
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
    LocalRetention {
        id: String,
        archived: bool,
    },
    CampaignAcceptance {
        request: Option<crate::campaign::AcceptanceRequest>,
        receipt: Option<crate::campaign::AcceptanceReceipt>,
    },
    CampaignAttentionList {
        questions: Vec<crate::work::Attention>,
        next_cursor: Option<String>,
    },
    CampaignAttentionAnswered {
        attention: crate::work::Attention,
    },
    CampaignInspection {
        campaign: Campaign,
        diagnostics: Vec<String>,
    },
    CampaignIntegration {
        report: serde_json::Value,
    },
    CampaignProgress {
        campaign: Campaign,
        activity: CampaignActivity,
    },
    Artifact {
        artifact: Option<ArtifactRegistration>,
    },
    ArtifactList {
        artifacts: Vec<ArtifactRegistration>,
    },
    ArtifactBytes {
        bytes: Vec<u8>,
    },
    Research {
        research: Research,
    },
    ResearchList {
        records: Vec<Research>,
        next_after: Option<String>,
    },
    Campaign {
        campaign: Campaign,
    },
    CampaignList {
        campaigns: Vec<Campaign>,
        next_after: Option<String>,
    },
    /// A generic success with an optional message.
    Ok {
        message: Option<String>,
    },
    /// A typed error.
    Error {
        code: u16,
        message: String,
    },
    /// Response to `DaemonStatus`.
    DaemonStatus {
        info: DaemonInfo,
    },
    /// Response to `AgentStart` / `AgentStatus` / `AgentCat`.
    Agent {
        info: AgentInfo,
    },
    /// Response to `AgentList` / `Top`.
    Agents {
        agents: Vec<AgentInfo>,
    },
    /// Response to `AgentLogs`.
    Logs {
        id: String,
        lines: Vec<String>,
    },
    /// Canonical user-visible history in chronological order.
    History {
        entries: Vec<HistoryEntry>,
    },
    /// Bounded private context assembled by the Memory Agent.
    MemoryRecall {
        items: Vec<MemoryRecallItem>,
        truncated: bool,
    },
    MemoryMutation {
        result: MemoryMutationResult,
    },
    Reminder {
        reminder: ReminderInfo,
    },
    Reminders {
        reminders: Vec<ReminderInfo>,
    },
    ScheduledTask {
        schedule: ScheduledTaskInfo,
    },
    ScheduledTasks {
        schedules: Vec<ScheduledTaskInfo>,
    },
    /// Response to `AgentExec`.
    Exec {
        id: String,
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    /// Response to `AgentAttach`.
    Attach {
        id: String,
        output: String,
    },
    /// A streamed event line from an `AgentSubscribe`/`ForegroundSubscribe`
    /// connection.
    Event {
        stream: EventStream,
        data: String,
    },
    /// Acknowledgement after `AgentChat` / `ForegroundChat`.
    Chat {
        id: String,
    },
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
    fn research_campaign_wire_round_trips() {
        let requests = [
            ApiRequest::ResearchCreate {
                command_id: "r".into(),
                title: "Title".into(),
                objective: "Objective".into(),
            },
            ApiRequest::ResearchGet {
                id: "research-id".into(),
            },
            ApiRequest::ResearchList {
                after: Some("research-id".into()),
                limit: 2,
            },
            ApiRequest::CampaignCreate {
                command_id: "c".into(),
                research_id: "research-id".into(),
                title: "Draft".into(),
                objective: "Not executed".into(),
            },
            ApiRequest::CampaignGet {
                id: "campaign-id".into(),
            },
            ApiRequest::CampaignList {
                research_id: Some("research-id".into()),
                after: None,
                limit: 2,
            },
        ];
        let names = [
            "research_create",
            "research_get",
            "research_list",
            "campaign_create",
            "campaign_get",
            "campaign_list",
        ];
        for (request, name) in requests.into_iter().zip(names) {
            let wire = serde_json::to_value(&request).unwrap();
            assert_eq!(wire["cmd"], name);
            assert_eq!(serde_json::from_value::<ApiRequest>(wire).unwrap(), request);
        }
        assert_eq!(
            serde_json::from_str::<ApiRequest>(r#"{"cmd":"research_list"}"#).unwrap(),
            ApiRequest::ResearchList {
                after: None,
                limit: 50
            }
        );
        let research = Research {
            id: "research-id".into(),
            title: "Title".into(),
            objective: "Objective".into(),
            created_at_ms: 123,
        };
        let campaign = Campaign {
            id: "campaign-id".into(),
            research_id: research.id.clone(),
            title: "Draft".into(),
            objective: "Not executed".into(),
            created_at_ms: 124,
            status: CampaignStatus::Draft,
        };
        for response in [
            ApiResponse::Research {
                research: research.clone(),
            },
            ApiResponse::ResearchList {
                records: vec![research],
                next_after: Some("research-id".into()),
            },
            ApiResponse::Campaign {
                campaign: campaign.clone(),
            },
            ApiResponse::CampaignList {
                campaigns: vec![campaign],
                next_after: None,
            },
        ] {
            let wire = serde_json::to_value(&response).unwrap();
            let decoded: ApiResponse = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
        }
        assert_eq!(
            serde_json::to_string(&CampaignStatus::Draft).unwrap(),
            "\"draft\""
        );
        for (status, name) in [
            (CampaignStatus::Running, "running"),
            (CampaignStatus::Cancelling, "cancelling"),
            (CampaignStatus::Cancelled, "cancelled"),
            (CampaignStatus::Accepted, "accepted"),
            (CampaignStatus::Rejected, "rejected"),
            (CampaignStatus::Unverified, "unverified"),
            (CampaignStatus::Interrupted, "interrupted"),
        ] {
            let wire = serde_json::to_string(&status).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            assert_eq!(
                serde_json::from_str::<CampaignStatus>(&wire).unwrap(),
                status
            );
        }
        assert!(serde_json::from_str::<CampaignStatus>("\"unknown-status\"").is_err());
    }

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
            deadline_ms: None,
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
                deadline_ms: None,
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
    fn daemon_status_defaults_background_state_for_older_daemons() {
        let info: DaemonInfo = serde_json::from_str(
            r#"{"pid":7,"version":"0.1.4","proto_version":"0.1","provider_ready":true,"socket":"/tmp/tachyon.sock"}"#,
        )
        .unwrap();
        assert_eq!(info.background, BackgroundCoordinatorInfo::default());
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
    fn work_protocol_preserves_identity_and_separates_failure_from_evidence() {
        let request = WorkRequest {
            context_refs: vec![],
            constraints: None,
            attempt: None,
            work_id: "work-1".into(),
            objective: "verify the latest release".into(),
            generation: 4,
            assignment: 2,
            deadline_ms: 123_456,
            lifetime_class: LifetimeClass::Short,
        };
        let wire = serde_json::to_string(&request).unwrap();
        assert_eq!(serde_json::from_str::<WorkRequest>(&wire).unwrap(), request);

        let result = WorkResult {
            final_context: None,
            attempt_id: None,
            candidate_refs: None,
            instruction_revision: None,
            evidence: Default::default(),
            timing: None,
            work_id: request.work_id,
            objective: request.objective,
            generation: request.generation,
            assignment: request.assignment,
            outcome: WorkOutcome::TimedOut {
                deadline_ms: request.deadline_ms,
            },
        };
        let wire = serde_json::to_string(&AgentEvent::WorkResult {
            result: result.clone(),
        })
        .unwrap();
        assert!(wire.contains(r#""outcome":"timed_out""#));
        assert!(!wire.contains(r#""result":""#));
        assert!(result.outcome.completed_result().is_none());
        assert!(!wire.contains("timing"));
        assert!(!wire.contains("final_context"));
        let mut timed = result.clone();
        timed.final_context = Some(crate::context::WorkerContextMetadata {
            activated_packages: [("artifact".into(), "observed".into())].into(),
            known_output_handles: vec!["output:local".into()],
        });
        timed.timing = Some(WorkTiming {
            execution_ms: Some(100),
            inference_ms: Some(60),
            tool_ms: Some(20),
            review_ms: Some(10),
        });
        assert_eq!(
            serde_json::from_value::<WorkResult>(serde_json::to_value(&timed).unwrap()).unwrap(),
            timed
        );
        assert_eq!(
            serde_json::from_str::<WorkTiming>("{}").unwrap(),
            WorkTiming::default()
        );
        let mut legacy = serde_json::to_value(&result).unwrap();
        legacy.as_object_mut().unwrap().remove("evidence");
        assert!(serde_json::from_value::<WorkResult>(legacy.clone())
            .unwrap()
            .final_context
            .is_none());
        assert!(serde_json::from_value::<WorkResult>(legacy.clone())
            .unwrap()
            .timing
            .is_none());
        assert_eq!(
            serde_json::from_value::<WorkResult>(legacy)
                .unwrap()
                .evidence,
            WorkEvidence::default()
        );
    }

    #[test]
    fn work_subscription_has_a_stable_wire_shape() {
        let request = ApiRequest::WorkSubscribe {
            work_id: "work-1".into(),
        };
        let wire = r#"{"cmd":"work_subscribe","work_id":"work-1"}"#;
        assert_eq!(serde_json::to_string(&request).unwrap(), wire);
        assert_eq!(serde_json::from_str::<ApiRequest>(wire).unwrap(), request);
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
    fn tool_telemetry_round_trips_with_runtime_and_call_identity() {
        let event = AgentEvent::ToolTelemetry {
            tool_name: "grep".into(),
            call_id: Some("model-call-2".into()),
            duration_ms: 38,
            success: true,
            truncated: false,
            bytes_out: 420,
            error_code: None,
            identity: ToolTelemetryIdentity {
                task_id: Some("task-1".into()),
                work_id: Some("work-1".into()),
                generation: Some(2),
                assignment: Some(3),
                attempt_id: None,
            },
        };

        let wire = serde_json::to_string(&event).unwrap();
        assert!(wire.contains(r#""kind":"tool_telemetry""#));
        assert!(wire.contains(r#""call_id":"model-call-2""#));
        assert_eq!(serde_json::from_str::<AgentEvent>(&wire).unwrap(), event);
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
                text: "hello".into(),
                cwd: None,
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
                text: "hello".into(),
                cwd: None,
            })
            .unwrap(),
            r#"{"cmd":"foreground_chat","text":"hello"}"#
        );
        let selected = ApiRequest::ForegroundChat {
            text: "hello".into(),
            cwd: Some("/project/selected".into()),
        };
        let wire = serde_json::to_string(&selected).unwrap();
        assert_eq!(serde_json::from_str::<ApiRequest>(&wire).unwrap(), selected);
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

    #[test]
    fn memory_recall_request_and_events_round_trip() {
        let request = ApiRequest::MemoryRecall {
            query: "what did we work on yesterday?".into(),
            conversation_id: "conversation-1".into(),
            turn: 7,
            include_history: true,
            max_items: 8,
            max_chars: 4096,
        };
        let wire = serde_json::to_string(&request).unwrap();
        assert_eq!(serde_json::from_str::<ApiRequest>(&wire).unwrap(), request);
        let legacy = serde_json::from_str::<ApiRequest>(
            r#"{"cmd":"memory_recall","query":"preferences","conversation_id":"conversation-1","turn":7,"max_items":8,"max_chars":4096}"#,
        )
        .unwrap();
        assert!(matches!(
            legacy,
            ApiRequest::MemoryRecall {
                include_history: false,
                ..
            }
        ));

        let event = AgentEvent::MemoryRecalled {
            turn: Some(7),
            preference_count: 2,
            history_count: 4,
        };
        let wire = serde_json::to_string(&event).unwrap();
        assert_eq!(serde_json::from_str::<AgentEvent>(&wire).unwrap(), event);

        let mutation = ApiRequest::MemoryMutate {
            intent: MemoryIntent::Correct {
                target_ids: vec!["preference-1".into()],
                descriptor: MemoryDescriptor::default(),
                value: "dislikes pickles".into(),
            },
            source_event_id: "message-7".into(),
            conversation_id: "conversation-1".into(),
            turn: 7,
            occurred_at_ms: 123,
        };
        let wire = serde_json::to_string(&mutation).unwrap();
        assert_eq!(serde_json::from_str::<ApiRequest>(&wire).unwrap(), mutation);
    }

    #[test]
    fn reminder_requests_and_responses_round_trip() {
        let request = ApiRequest::ReminderCreate {
            source_event_id: "turn-1:123:1".into(),
            conversation_id: "foreground".into(),
            turn: 1,
            text: "Your coffee is ready.".into(),
            delay_seconds: Some(60),
            local_time: None,
            day: None,
            created_at_ms: 123,
        };
        let wire = serde_json::to_string(&request).unwrap();
        assert_eq!(serde_json::from_str::<ApiRequest>(&wire).unwrap(), request);

        let reminder = ReminderInfo {
            id: "reminder-123-1".into(),
            conversation_id: "foreground".into(),
            turn: 1,
            text: "Your coffee is ready.".into(),
            created_at_ms: 123,
            due_at_ms: 60_123,
            status: ReminderStatus::Pending,
        };
        let response = ApiResponse::Reminder {
            reminder: reminder.clone(),
        };
        let wire = serde_json::to_string(&response).unwrap();
        assert!(matches!(
            serde_json::from_str::<ApiResponse>(&wire).unwrap(),
            ApiResponse::Reminder { reminder: decoded } if decoded == reminder
        ));
    }

    #[test]
    fn scheduled_task_deadlines_round_trip() {
        let request = ApiRequest::ScheduledTaskCreate {
            source_event_id: "turn-2:456:task".into(),
            conversation_id: "foreground".into(),
            turn: 2,
            objective: "Get the weather forecast.".into(),
            mode: ScheduledTaskMode::FinishBy,
            delay_seconds: None,
            local_time: Some("20:00".into()),
            day: Some(ScheduleDay::Next),
            created_at_ms: 456,
        };
        let wire = serde_json::to_string(&request).unwrap();
        assert_eq!(serde_json::from_str::<ApiRequest>(&wire).unwrap(), request);

        let response = ApiResponse::ScheduledTask {
            schedule: ScheduledTaskInfo {
                id: "scheduled-task-456-2".into(),
                conversation_id: "foreground".into(),
                turn: 2,
                objective: "Get the weather forecast.".into(),
                mode: ScheduledTaskMode::FinishBy,
                created_at_ms: 456,
                due_at_ms: 60_456,
                status: ScheduledTaskStatus::Pending,
                work_id: None,
            },
        };
        let wire = serde_json::to_string(&response).unwrap();
        assert!(matches!(
            serde_json::from_str::<ApiResponse>(&wire).unwrap(),
            ApiResponse::ScheduledTask { schedule }
                if schedule.id == "scheduled-task-456-2"
                    && schedule.mode == ScheduledTaskMode::FinishBy
        ));
        assert_eq!(
            serde_json::to_string(&ApiRequest::ScheduledTaskList).unwrap(),
            r#"{"cmd":"scheduled_task_list"}"#
        );
    }
}
