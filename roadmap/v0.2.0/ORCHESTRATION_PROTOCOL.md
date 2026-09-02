# Tachyon v0.2.0 Orchestration Protocol

## Status

Approved migration contract. Implementation is proceeding in behavior-preserving
slices; completed boundaries are recorded in `MIGRATION.md`.

## Problem Statement

The current role-driven Ghost control loop couples conversation, task
coordination, worker execution, tools, prompts, and lifecycle behavior. This
causes unnecessary prompt/token usage, serial foreground latency, unclear
state ownership, and hard-to-trace races.

Observed regression to eliminate:

- A simple greeting uses roughly 1.5k tokens.
- A three-city weather question created three agents, consumed roughly 82.8k
  tokens, and took roughly 20 seconds.
- A coat follow-up waited behind the weather work instead of beginning as a new
  foreground turn.

## Target Topology

```text
User
  |
  v
Interaction Manager (Rust, foreground ordering)
  |
  +--> Conversational Agent (LLM, one foreground request)
  |       |
  |       +--> structured interaction/task intents
  |
  +--> Tachyond (Rust, durable task state and lifecycle)
           |
           +--> Background Coordinator (LLM, asynchronous decisions)
           +--> Ghost Harnesses (worker execution only)
           +--> Memory Agent (ephemeral compaction job)
```

## Directory And Dependency Plan

```text
crates/
  runtime/
    protocol/        # command/event types, IDs, versions, errors
    model/           # OpenRouter client, routing, usage, provider metadata
    telemetry/       # spans, metrics, profiling payloads
    credentials/     # secret resolution only
  tachyond/          # durable runtime, queues, state, scheduler, recovery
  orchestrators/
    conversation/    # foreground prompt and structured intent extraction
    coordinator/     # background task reasoning and semantic updates
    memory/          # snapshot/consolidation prompt and result validation
    context/         # deterministic bounded context assembly
  ghost/
    harness/         # worker request loop, cancellation, checkpoints
    tools/           # filesystem, shell, Python, browser, artifacts
  tachyon-tui/       # render ordered interaction/task events
```

This is a dependency boundary, not a requirement to make every directory a
separate process or crate on day one. A component becomes independently
packaged when it has a stable protocol and separately testable behavior.

Dependency direction:

```text
runtime <- orchestrators
runtime <- ghost
runtime <- tachyond
tachyond supervises orchestrators and ghost
orchestrators do not import ghost internals
ghost does not import conversation/coordinator policy
```

## Component Responsibilities

### Interaction Manager

Rust actor with one mailbox and one owner of interaction state.

Owns:

- user turn acceptance and ordering
- foreground request lane
- stream routing to the TUI
- interruption and notification timing
- current topic and pending interaction state
- conversion of conversational intents into durable commands

Does not own task lifecycle or semantic task planning.

### Conversational Agent

Answers: "What should Tachyon say to the user now?"

Input is a bounded conversation context and current user turn. Output is visible
text/deltas plus validated structured intents. It has no autonomous tool loop,
no worker tool schemas, and no authority to mutate durable state.

### Background Coordinator

Answers: "What work should currently happen?"

It is triggered by task state changes, user steering, findings, or scheduled
review. It emits task commands and semantic updates. It never blocks the
foreground lane or writes user-visible text directly.

### Memory Agent

Answers: "What information should survive this context epoch?"

It runs only when Tachyond schedules compaction/consolidation. It receives a
prior snapshot plus canonical events, returns validated candidate projections,
and exits.

### Ghost Harness

Receives `WorkRequest`, executes a bounded worker model/tool loop, and emits
`WorkEvent` plus one `WorkResult`. It owns no conversation, task scheduling, or
cross-worker coordination policy.

### Tachyond

Owns durable command handling, task projection, priority queues, scheduler,
process supervision, cancellation, retries, recovery, and generation state.

## Protocol Surface

All messages are versioned and include `event_id`, `correlation_id`,
`causation_id`, `occurred_at`, and the originating component/generation.
The first concrete foreground command/event types live in
`crates/tachyon-api/src/interaction.rs`.

### Interaction Commands

```text
AcceptUserTurn
BeginConversation
CancelConversation
PublishBackgroundUpdate
NotifyUser
```

### Interaction Events

```text
UserTurnAccepted
ConversationDelta
ConversationFinished
ConversationIntentProduced
ForegroundRequestTimedOut
UserVisibleNotificationPublished
```

### Task Commands

```text
StartTask
SteerTask
StopTask
CancelTask
ScheduleTask
PersistCommitment
```

### Task Events

```text
TaskQueued
TaskStarted
TaskBlocked
TaskWaitingForUser
TaskFindingProduced
TaskCompleted
TaskFailed
TaskCancelled
```

### Worker Protocol

```text
WorkRequest
  task_id
  work_id
  generation_id
  objective
  bounded_context
  tool_policy
  token_budget
  deadline
  cancellation_token

WorkEvent
  Started | Progress | ToolStarted | ToolFinished | Finding | Usage | Failed

WorkResult
  Completed | Blocked | Failed | Cancelled
  evidence
  summary
  usage
```

### Memory Protocol

```text
CompactionRequest
  logical_agent_id
  source_generation
  target_generation
  previous_snapshot
  events_since_snapshot
  token_budget

CompactionResult
  candidate_snapshots
  source_event_cursor
  validation_metadata
```

## Request Classification

The Interaction Manager classifies each accepted user turn before scheduling:

| Class | Execution path | Example |
| --- | --- | --- |
| Independent conversation | Conversation Agent only | "hi" |
| Dependent conversation | Conversation Agent with committed facts | "will I need a coat?" |
| Direct operation | Deterministic adapter, optional Conversation phrasing | weather lookup, simple URL fetch |
| Background work | Immediate acknowledgement plus async Coordinator/task flow | repository investigation |

Classification may use deterministic rules plus the Conversational Agent's
structured intent. The classifier never waits for the Coordinator or a worker.

## Context And Token Policy

### Conversation Context

Include only:

- concise conversation prompt
- current user message
- bounded recent turns
- relevant persisted facts/preferences
- active task summaries when relevant

Exclude worker tool schemas, coordinator prompts, raw worker logs, unrelated
task history, and memory-compaction instructions.

### Budgets

Each request has explicit limits for input tokens, completion tokens, tool
calls, wall-clock deadline, and total task spend. Token/cost limits are enforced
by Tachyond, not suggested only in prompts.

Initial target measurements after instrumentation:

- Greeting: one Conversation call, zero tools/workers/coordinator jobs.
- Direct weather: zero Ghost workers and bounded direct-operation spend.
- Foreground follow-up: no queueing behind background work.

Numerical budgets are set after baseline breakdown identifies system prompt,
history, tool-schema, and completion costs.

## Scheduling And Priority

```text
High: user messages, cancellation, critical failures, urgent findings
Normal: task state changes, coordinator requests, normal findings
Low: progress logs, metrics, compaction, maintenance
```

Queues are bounded. Overflow policy is explicit per message type: critical
commands are retained, coalescible progress is collapsed, and dropped telemetry
is counted.

## Trace And Profile Schema

For each interaction and model request record:

```text
user_received
interaction_queued
interaction_dequeued
context_ready
request_started
first_byte
first_token
first_token_rendered
generation_finished

role, model, provider, routing_policy
input_tokens, output_tokens, cached_tokens, cost
queue_duration, request_duration, tool_duration
correlation_id, causation_id, task_id, generation_id
```

## Migration Sequence

### Phase 0: Contracts And Baseline

1. Approve this document and `INVARIANTS.md`.
2. Map current Ghost/Tachyond responsibilities to target owners.
3. Add structured timing/token traces without changing behavior.
4. Add replay fixtures for greeting, weather, follow-up during background work,
   duplicate events, restart, cancellation, and late events.

### Phase 1: Shared Runtime

1. Extract protocol, IDs, errors, model client, credentials, and telemetry.
2. Preserve existing behavior through adapters.
3. Verify that no role imports another role's control loop.

### Phase 2: Foreground Path

1. Introduce the Interaction Manager actor and foreground queue.
2. Extract the Conversation Agent with a minimal prompt/context contract.
3. Route independent and dependent turns through it.
4. Add direct-operation adapters for weather and comparable deterministic work.

### Phase 3: Background Path

1. Extract Background Coordinator as an event-triggered component.
2. Implement Task State Manager projections in Tachyond.
3. Convert Ghost to `WorkRequest -> WorkEvent* -> WorkResult`.

### Phase 4: Memory And Generations

1. Implement deterministic memory storage/retrieval first.
2. Add token-budget monitoring and snapshot scheduling.
3. Add ephemeral Memory Agent and atomic context-epoch rollover.

### Phase 5: Remove Legacy Paths

1. Remove the remaining Ghost coordinator compatibility mode after parity tests.
2. Delete compatibility adapters after replay, fault-injection, and performance
   acceptance tests pass.

## Acceptance Criteria

1. Conversation runs without starting or importing the Ghost control loop.
2. Ghost runs a work request without importing conversation/coordinator policy.
3. A greeting invokes one foreground model request and no background work.
4. A weather query uses direct operations, not autonomous workers.
5. A follow-up can render its first token while an unrelated task is active.
6. Current provider, routing policy, role, timing, token usage, and cost are
   visible in trace data.
7. Duplicate, late, failed, cancelled, and restart events preserve deterministic
   state and do not deadlock the foreground path.
