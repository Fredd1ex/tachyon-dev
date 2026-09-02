# Interaction Control Flow

Tachyon separates the user-facing Foreground runtime from background work.
Foreground owns conversation ordering and speech; Tachyond manages processes
and lifecycle operations.

## Actors

```text
User
  |
  v
TUI / Tachyon
  |
  v
tachyon-foreground
  |
  +-- interaction router
  +-- worker requests
  +-- worker result synthesis
  |
  v
Tachyond
  |
  +-- ephemeral Ghost workers
  +-- subscriptions and lifecycle
```

## First Message

When no turn is active:

1. The TUI sends the message to Tachyond.
2. Tachyond forwards a typed command to Foreground.
3. Foreground creates a conversational turn and begins processing it.
4. Foreground may answer directly or temporarily request workers.
5. Tachyond starts the requested worker processes and streams their events.
6. Foreground receives correlated worker results and passes the private evidence to a
   no-tools synthesis pass.
7. The synthesis pass emits only the final spoken response to the user.

Foreground may request workers but does not execute their work directly.
Worker lifecycle and tool events remain background state; they are not part of
the user-facing conversation unless trace mode is enabled.

Before background work begins, Foreground may generate one brief, context-aware
spoken acknowledgment with a separate no-tools model call. This acknowledgment
must not mention workers, tools, delegation, or internal processing. It runs
alongside worker execution and is omitted if it cannot be generated safely.

## Follow-Up Messages

New input is accepted while another turn is active. It is not blindly queued or
blindly run concurrently.

The interaction router is a separate model call with no tools. It receives the
active turn context and incoming message, then returns exactly one decision:

```rust
enum InteractionDecision {
    AnswerNow,
    AttachToActiveTurn,
    InterruptAndReplan,
    WaitForActiveTurn,
}
```

For queued follow-ups, a second no-tools answerability check runs before the
tool-enabled Conversation loop. It returns `AnswerFromContext` or
`NeedsNewWork`. When context is sufficient, tools are disabled for that turn,
which prevents an unnecessary worker from being created for an answer already
supported by completed evidence. Routing controls when the turn runs, but never
determines whether evidence is sufficient. An answerability timeout, malformed
response, or provider error defaults to `NeedsNewWork`; coordination is then
restricted to delegation so the model cannot replace required external work
with an unsupported direct response.

### `AnswerNow`

The message is independent of the active work. It starts as a concurrent turn
and does not wait for the active worker result.

### `AttachToActiveTurn`

The message adds information or changes the active request. It waits until the
active turn finishes, then runs with the completed conversation context.

### `WaitForActiveTurn`

The message depends on information still being gathered. It is queued and the
user receives an immediate status update explaining that the response will
follow when the active work completes.

### `InterruptAndReplan`

The message changes the objective enough that the active turn should be
replanned. Worker processes can now be interrupted through Tachyond; conversation
level replanning and dependency-aware cancellation are still pending.

## Queue And Concurrency

Deferred turns use a bounded queue with capacity 64. This prevents unlimited
input from consuming memory while a long-running task is active.

Independent turns may run concurrently. Worker subtasks are also run
concurrently whenever they are independent; dependent steps remain ordered.
Each turn receives a snapshot of the conversation and produces a delta. Deltas
are stored by turn ID and committed in user-message order, so a faster later
response cannot reorder the durable conversation history.

```text
turn 1 starts ─────────────────────── completes
turn 2 arrives ─ router: AnswerNow ── completes

commit order: turn 1, then turn 2
```

For related turns:

```text
turn 1 starts ─ worker lookup ─────── completes
turn 2 arrives ─ router: Wait ─────── waits
                                      runs with turn 1 context
```

## Worker Visibility

Worker lifecycle events are streamed independently through Tachyond. The TUI
can render them as `ghost <uuid>` entries in trace mode. Tool activity is
nested under the originating turn rather than being treated as a separate user
conversation. Raw worker output is private input to the synthesis pass and is
never emitted as the final user response.

Worker output is accumulated across multiline `[agent]` events before it is
returned to Foreground. This prevents a partial final line from causing
Foreground to repeat a lookup unnecessarily.

## UI Decoupling

The TUI is a client of Tachyond, not a client of Foreground or Ghost. The
current TUI still accepts compatibility line markers while rebuilding local
conversation state.

The target boundary is:

```text
TUI ── typed state/events ──> Tachyond ── process/API ──> Foreground
                              │
                              └── workers
```

Tachyond should expose normalized events containing actor and relationship
metadata, rather than requiring the TUI to parse strings:

```rust
struct EventEnvelope {
    event_id: u64,
    session_id: String,
    conversation_id: Option<String>,
    turn_id: Option<String>,
    task_id: Option<String>,
    parent_task_id: Option<String>,
    tool_call_id: Option<String>,
    actor: Actor,
    sequence: u64,
    kind: EventKind,
    occurred_at_ms: u64,
}
```

The TUI should consume these events and render a local view model. It should
not decide whether a worker is relevant, infer parentage from output, launch
Ghost, or implement lifecycle policy.

Normal chat reserves a compact pending activity item for each locally accepted
user turn. The completed reply keeps that correlation but moves into the
event-time completion timeline, so a large late answer never expands above
content the user is currently reading. Turn labels and compact latency/activity
badges remain visible in normal chat for orientation; trace mode adds the full
diagnostic event stream.

This makes Orchestrator and worker visualization explicit without coupling the
UI to their implementation:

- `Actor::Orchestrator` renders the user-facing conversation.
- `Actor::Worker { id }` renders an ephemeral agent activity card.
- `EventKind::ToolStarted` and `ToolFinished` render nested activity.
- `EventKind::PermissionRequested` renders a user approval card.
- `EventKind::TaskStateChanged` updates agent status.

Once this protocol exists, alternate clients such as a web UI, log viewer, or
desktop application can observe the same Tachyond state without changing the
Orchestrator.

The typed envelope is now the authoritative chat subscription format. Legacy
line markers remain only as temporary process-supervision and one-shot CLI
compatibility output. Clients may decode legacy typed `AgentEvent` JSON during
migration, but must not switch between structured and line-marker semantics on
an event-by-event basis. Event IDs are deduplicated per session.

## Safety Defaults

- Router failures default to `WaitForActiveTurn`.
- Deferred input is bounded.
- Orchestrator tool loops have a finite iteration limit.
- Delegation is limited to one delegation phase per Orchestrator turn.
- Individual tool calls default to a 20-second timeout, configurable with
  `TACHYON_TOOL_TIMEOUT_SECS`.
- Tachyond places an absolute deadline in each `WorkRequest`, defaulting to 120
  seconds and configurable with `TACHYON_WORKER_RESULT_TIMEOUT_SECS`; progress
  events do not extend it. Foreground waits for the daemon-owned terminal result
  and does not run a competing deadline.
- Timeout, cancellation, blocked, and failed `WorkResult` outcomes contain no
  evidence field. Completed sibling results may still be synthesized without
  mixing failure text into factual evidence.
- Final user responses are generated by a no-tools synthesis pass.
- Background acknowledgments are generated separately from tool planning.
- Tachyond is responsible for terminal reduction, timeout cancellation, process
  lifecycle, and stale generation/assignment rejection.
- No portal, sandbox, or microVM behavior is part of this interaction layer.

## Current Limitation

The control flow distinguishes an interrupt decision, but Ghost does not yet
stop an active model/tool loop when it receives one. The next lifecycle phase
will add typed Tachyond operations for interruption, graceful termination,
replanning, and forced cleanup.

## V2: Three Ghost Profiles

V2 separates task execution, task coordination, and user-facing conversation.
The single-Orchestrator flow above remains the V1 architecture and reference
behavior. V2 is the target architecture for continuous, natural conversation
while work is still running.

```text
User
  |
  v
Conversational Agent
  ^                    |
  | signals and results |
  |                    v
Background Coordinator <-> Tachyond
                         |
                         v
                   Ghost Agent Harnesses
```

### Profiles

The Ghost harness has three explicit runtime profiles:

#### `ghost`

The Agent Harness completes an assigned task in the background. It owns local
execution through its configured backend and reports evidence, progress, and a
terminal result. It does not manage other agents or decide how to speak to the
user.

#### `background`

The Background Coordinator manages Agent Harnesses through Tachyond. It owns
task decomposition, dependency ordering, parallel execution, lifecycle
operations, result collection, and signals to the Conversational Agent. It does
not write user-facing conversation.

#### `conversational`

The Conversational Agent owns the user-facing flow. It receives task signals
from Background, answers independent messages quickly, handles dependent
questions naturally, and decides how and when background results should enter
the conversation. It does not perform task execution directly.

### Conversational Agent

The Conversational Agent owns the user-facing dialogue. It should:

- Answer simple and independent messages immediately.
- Produce short, natural, TTS-friendly responses.
- Acknowledge work without mentioning workers, tools, prompts, or delegation.
- Tell the user when a dependent answer must wait for background evidence.
- Continue casual conversation while unrelated tasks run.
- Incorporate completed task results when they become relevant.
- Never expose raw worker narration, IDs, commands, or internal reasoning.

The Conversational Agent does not manage Agent Harness processes directly. It
consumes the Background Coordinator's typed state and result events as private
conversational context.

### Background Coordinator

The Background Coordinator owns agent management and task state. It should:

- Decompose substantive requests into useful subtasks.
- Run independent subtasks concurrently whenever practical.
- Keep dependent steps ordered.
- Start, monitor, retain, replan, and release Agent Harnesses through Tachyond.
- Publish progress and terminal results through typed events.
- Preserve task identity and dependencies across conversational turns.
- Never generate user-facing prose as a substitute for the Conversation Agent.

The coordinator is task-agnostic. Parallelism is determined by dependencies,
resource limits, safety constraints, and the user's request, not by domain-
specific rules or examples.

### Typed Task Bridge

The Conversational Agent and Background Coordinator communicate through a typed
task bridge rather than raw Agent Harness output:

```rust
struct TaskUpdate {
    task_id: String,
    origin_turn_id: String,
    parent_task_id: Option<String>,
    tool_call_id: Option<String>,
    state: TaskState,
    progress: Option<String>,
    result: Option<String>,
    error: Option<String>,
    timestamp_ms: u64,
}
```

Expected states include `Started`, `Running`, `Waiting`, `Completed`,
`Failed`, `Cancelled`, and `NeedsInput`. Worker output is private evidence;
only normalized results, relevant status, and policy signals are passed to the
Conversational Agent.

The Background Coordinator never sends prose intended for direct speech. The
Conversational Agent decides how a signal becomes a natural response.

### Concurrent Conversation

Every incoming user message is classified relative to active tasks by the
Conversational Agent:

- Independent messages run immediately on a conversation snapshot.
- Messages that depend on a task receive a natural waiting response and are
  associated with that task's result.
- Messages that change an active objective create a replan request for the
  Background Coordinator.
- Completed task results can resume pending user questions or be announced
  when the current conversational turn is not being interrupted.

Conversation turns remain acceptance-ordered in durable history even when
independent turns execute and publish concurrently. Publication does not wait
for the durable commit cursor: an independent answer may become visible before
an earlier, still-running turn. Dependent turns wait for their prerequisite
turn or correlated evidence. Task updates carry logical task, origin turn,
parent task, and tool-call IDs so a result cannot be applied to the wrong
request.

### V2 Configuration

V2 replaces the single runtime role configuration with three independently
customizable role configurations. The existing model settings are not shared
implicitly between the roles.

```toml
[conversation]
model = "..."
temperature = 0.2
context_length = 131072
max_completion_tokens = 4096
parallel_tool_calls = true
persona = "..."

[background]
model = "..."
temperature = 0.2
context_length = 131072
max_completion_tokens = 4096
parallel_tool_calls = true
persona = "..."

[worker]
model = "..."
temperature = 0.2
context_length = 131072
max_completion_tokens = 4096
parallel_tool_calls = true
persona = "..."
```

All fields are illustrative configuration names and must remain optional with
safe defaults. Each role is resolved independently; omitted Worker settings do
not inherit Background settings. API keys remain environment-only. The Conversational Agent's
configuration controls dialogue and TTS output; the Background Coordinator's
configuration controls decomposition and worker supervision; the Worker's
configuration controls assigned task execution and reporting.
The configured `persona` for each role is appended to that role's system
prompt at runtime. A role must never silently use the other role's persona or
model settings.

### Latency And Presentation

The interaction path is designed to keep the user-facing loop responsive without
leaking internal planning:

The realtime target is for the API endpoint to be the only meaningful latency
source. Local Tachyon work must be asynchronous and non-blocking: accepting
input, routing state, daemon IPC, worker bookkeeping, evidence correlation, and
TUI updates must not add a user-visible wait.

- The Conversational Agent emits a short, validated acknowledgement from the
  same model completion that planned delegation. It does not make a separate
  acknowledgement request.
- Interaction classification runs asynchronously after a queued message is
  accepted. It has a short timeout and safely defaults to waiting for the active
  turn, so a slow routing request cannot block input.
- A normal turn uses one Conversation completion. It answers with ordinary text
  when existing context is sufficient, or selects `spawn_agent`/`spawn_agents`
  for fresh work. It does not make a separate fresh-work classifier request or
  pay for a synthetic direct-response tool schema.
- Independent Background tasks are submitted concurrently through Tachyond.
- Parallel work is an incremental evidence stream, not a single blocking batch.
  Each completed task publishes typed evidence immediately while unrelated tasks
  continue running.
- Dependent follow-ups wait for committed prerequisite context or correlated
  evidence from the prerequisite task, whichever makes the answer eligible.
- A follow-up classified as waiting for an active result answers from that
  correlated context with tools disabled. It must not launch a duplicate worker
  merely because the underlying evidence is current or externally sourced.
- A follow-up resumes as soon as the evidence required for its objective is
  available. It must not wait for unrelated tasks in the same parallel batch.
- If evidence relevance cannot be determined confidently, the system waits
  rather than guessing or presenting incomplete information.
- Every delegated result receives one bounded, no-tools Conversation synthesis
  before publication. Synthesis streams eligible deltas immediately, so raw
  reports, headings, labels, and internal formatting do not become spoken
  output. Workers should still return concise evidence and include limitations
  only when they affect correctness.
- Conversational responses are concise by default and expand when the user asks
  for detail, clarification, comparison, reasoning, or another format.
- Worker, Background, and Conversation histories use separate checkpoints, so
  unrelated transcripts do not inflate a role's context.
- Each role has an independent model and context-length configuration. Context
  fitting preserves the system prompt and the newest usable messages for that
  role.
- Publication-eligible Conversation completions stream typed `ReplyDelta`
  events and conclude with one authoritative `Reply`. Completions that may
  contain planning or tool calls remain buffered. Context-only replies and
  post-worker synthesis run without tools, so their deltas are safe to publish.
- Provider protocol markup emitted in textual output is never user-facing
  prose. The stream filters protocol markers across chunk
  boundaries; a recoverable nested delegation becomes a typed tool call, and a
  malformed one falls back to delegating the original user objective. The TUI
  also sanitizes final replies, rendered items, and persisted session history so
  protocol text from an older client cannot remain visible after an upgrade.
- Concurrent reply deltas are projected by turn ID. Interleaved streams must
  update their existing turn rather than creating fragmented reply cards.
- Normal chat is an append-only completion timeline: user messages, compact
  pending activity, and completed replies are projected by event time. A reply
  that finishes after a newer user message is labeled as an earlier request
  rather than being inserted above the active exchange. Trace mode retains full
  event-time diagnostics; durable model history remains acceptance ordered.
- Commit/evidence readiness uses notifications rather than timer polling.
  Checkpoint serialization and writes run on one ordered background writer and
  never hold the conversation mutex during filesystem work.
- The TUI establishes its first subscription immediately and checks terminal
  input/events at frame cadence. Tachyond logs streamed events through a
  buffered logger thread rather than opening and writing the log on the event
  forwarding path.

The user-visible acknowledgement is not a task result. It must never claim that
work is complete, invent progress, expose tools or workers, or appear after the
dependent result. If no safe acknowledgement is present in the planning
completion, the system emits only a status event and continues silently until
the result is ready.

Any implementation that waits for an entire parallel batch before exposing
completed evidence violates this realtime contract. Batch completion may still
be used for final bookkeeping, but it must not gate an answer whose required
evidence is already available.

### Observability And UI Rules

- Conversation tool payloads stay minimal and expose delegation only. Worker
  lifecycle controls remain owned by the Background Coordinator.
- Tool descriptions and role prompts should be concise so coordination metadata
  does not consume unnecessary model context or increase request latency.
- Provider reasoning controls are explicit on every request. A role configured
  with reasoning disabled sends `reasoning.enabled = false`; omitting the field
  can activate a model provider's expensive default reasoning mode.
- Typed `WorkerStarted` and `WorkerCompleted` events retain the originating turn
  so worker badges, evidence, and expanded traces cannot drift to a newer user
  message. Worker release events are also observable.
- `Ctrl+O` trace mode is a chronological, uncollapsed diagnostic projection. It
  overrides worker and tool-body collapse, shows full tool arguments and
  results, and labels events authored by the Background actor. Normal chat
  remains limited to user-facing Conversation turns.
- In chat mode, structured reply events are authoritative; legacy raw reply
  lines are emitted only for one-shot CLI output so a reply cannot render twice.
- A worker released after successful completion must not be counted as failed,
  even if its process exits with a termination signal during cleanup.
- The Agents tab renders the Background Coordinator as one compact row. Its
  summary must not introduce an extra column or wrap the worker table layout.

### Background Priorities And Interrupts

Background updates carry a priority and remain correlated to their task and
conversation:

```rust
enum TaskPriority {
    Normal,
    Urgent,
    Important,
}
```

- `Normal` results are queued and woven into a natural later response.
- `Urgent` results request attention; the Conversational Agent decides whether
  interrupting the current exchange is appropriate.
- `Important` results interrupt the current conversational flow and are
  presented directly to the user.

The Background Coordinator may request an interrupt, but it cannot directly
interrupt speech or write user-facing text. The Conversational Agent remains
the policy and output authority.

### Output Arbitration

Background execution and conversational reasoning may run concurrently.
Independent messages are processed on immutable conversation snapshots and may
publish immediately, even while an earlier turn is still running. A dependent
message waits on its correlated task relationship rather than blocking
unrelated input. Each turn has one ordered delta stream and one authoritative
final reply; clients arbitrate audio playback separately if simultaneous text
replies become eligible.

### Scheduling Contract

The scheduler favors concurrency but has explicit ownership boundaries:

- Each conversation turn owns an immutable input snapshot.
- Background tasks own their task state and communicate through typed updates.
- No task writes directly to another task's conversation or output buffer.
- The Conversation Agent is the only component allowed to publish user-facing
  output, with publication eligibility evaluated per turn.
- Background work may run concurrently with model calls and unrelated turns.
- Dependent turns wait on task notifications without holding conversation locks.
- Shared state uses message passing or short-lived locks; model and network work
  must never run while a state lock is held.
- Completion, cancellation, and interrupt notifications are idempotent.
- Bounded queues and explicit task ownership prevent unbounded work and cycles.

The user should experience a continuous conversation rather than a visible
sequence of scheduled turns. Scheduling metadata, worker IDs, retries, and
coordination state remain private unless trace mode is enabled.

### TTS Contract

The Conversational Agent's output is the literal text spoken to the user. It
should use plain language, short sentences, natural transitions, and ordinary
punctuation. It should avoid markdown, headings, tables, raw commands,
internal labels, and implementation details unless the user requests them.

The system must not hard-code domain-specific conversational scripts. Natural
acknowledgments, waiting responses, jokes, status explanations, and final
results are generated from the current user intent and typed task context.

### V2 Safety Defaults

- Background work never blocks unrelated conversation.
- Dependent answers do not claim results before task completion.
- Task updates are scoped to the originating conversation and task ID.
- Coordinator failures become concise Conversational Agent messages.
- Worker and coordinator prompts cannot directly become user-facing speech.
- The existing bounded queue, loop limits, delegation limits, and Tachyond
  lifecycle authority remain in effect.

### V2 Implementation Boundary

The current stabilization layer provides centralized role profiles, correlated
typed event envelopes, structured final Conversation replies, per-turn
publication eligibility, durable acceptance-ordered history, visible terminal
answers, safe typed reply streaming, and TUI event deduplication. Tachyond
persists and forwards `logical_task_id`, `origin_turn_id`, `parent_task_id`, and
`tool_call_id`. Ghost checkpoints the full correlated evidence envelope and
uses origin-turn correlation for normal wakeups; lexical objective matching is
retained only for legacy uncorrelated checkpoint evidence. Direct responses use
ordinary assistant text; protocol markup is filtered incrementally, while
delegation arguments remain private. Typed delegation may atomically reuse
a ready retained background worker with the same worker profile, lifetime, and
workspace policy, clears old terminal replay before assignment, and bounds
result waits. Persistent supervisors latch and replay typed readiness after a
daemon reconnect. Workspace creation, process spawn, task input, and lifecycle
waits occur outside Tachyond's global registry lock. Timing events expose
`routing`, `ready`, per-request model boundaries, `evidence_ready`, synthesis,
`first_visible`, `publication_started`, and `completed` elapsed stages. Normal
chat renders `first` and `done` separately. Token badges distinguish the
conversation agent's `self` usage from `total` usage aggregated across
correlated worker assignments. Worker usage is retained for subscription replay
and replaced by logical task ID, preventing reconnects or warm-worker reuse from
double-counting it. Routing and answerability model calls are included in self
usage when the provider returns usage before their deadline.

The following V2 components remain architectural work rather than prompt or UI
policy:

- A durable first-class turn table replacing the in-memory pending map and
  commit cursor.
- A separately running Background Coordinator owning decomposition and worker
  lifecycle policy.
- Incremental task completion that does not block a batch on its slowest
  unrelated worker.
- One combined endpoint decision for messages arriving during active work; the
  semantic dependency router is still an additional endpoint request for those
  queued messages.
- A centralized Tachyond lifecycle reducer and replayable event subscription.
- Transport-receipt timestamps supplementing source-side model, evidence,
  synthesis, publication, and completion timing events.
