# Tachyon v0.2.0 UI Protocol

## Purpose

The TUI renders typed runtime state and ordered events from Tachyond. It does
not infer task state from model prose, parse worker logs to determine state, or
treat a process as the source of task truth.

The default conversation remains quiet. Operational detail is available through
compact turn traces, dedicated runtime views, and the Info panel.

## UI Data Contract

The TUI subscribes to a replayable, versioned stream of projections. Every
projection includes its revision, event cursor, and correlation IDs so a
reconnect can detect gaps and request a snapshot.

```text
UiSnapshot
  session
  interaction
  tasks
  workers
  memory_jobs
  provider
  queues
  event_cursor
  revision

UiEvent
  InteractionChanged
  TurnChanged
  TaskChanged
  WorkerChanged
  MemoryJobChanged
  QueueChanged
  ProviderUsageRecorded
  NotificationChanged
```

The TUI applies each event idempotently. A snapshot is authoritative after
reattachment; incremental events only advance a known revision.

## Primary Views

### Conversation

The default view shows only user-visible turns and assistant responses.

Each active turn displays one compact state badge:

```text
queued | assembling context | streaming | direct operation | waiting | complete | failed
```

Expanded traces show causal, structured work for that turn. They do not expose
raw internal prompts or credentials.

### Runtime Tabs

Replace the current ambiguous `ORCHESTRATORS / AGENTS` presentation with:

```text
CONVERSATION | BACKGROUND | WORKERS | MEMORY
```

`MEMORY` is hidden unless a compaction or consolidation job exists. Tabs are
keyboard-selectable with Left/Right and mouse-selectable. Empty tabs render an
explicit empty state, never a blank table.

#### Conversation Tab

Shows the foreground runtime only:

```text
component       state                 generation  queue  current turn
Conversation    streaming/ready/...   14          0      turn-42
Interaction     active/idle/...       -           1      turn-43
```

#### Background Tab

Shows Coordinator state and durable task projections, not worker log lines:

```text
task       state              priority  coordinator decision  budget
weather    completed          normal    update stored          2% used
research   waiting-for-user   high      needs clarification    18% used
```

#### Workers Tab

Shows only active or retained worker execution instances:

```text
worker     task       state       generation  age  last structured event
ghost-7    research   running     3           2m   browser complete
```

#### Memory Tab

Shows ephemeral maintenance jobs while they exist:

```text
target             state       source -> target  event cursor  budget
conversation       compacting  14 -> 15          502           37% used
```

## Task Controls

Controls are applied to durable task IDs, never a guessed worker process ID.

```text
cancel       request cancellation
pause        prevent new work from starting
resume       allow queued work to continue
steer        append user direction to a task
prioritize   change durable scheduler priority
```

The UI optimistically marks a command as pending, then replaces it only when
the corresponding authoritative command result/event arrives.

## Trace Model

An expanded turn trace is a causal tree keyed by correlation and causation IDs:

```text
Turn 43
  Conversation request
    queue: 2ms
    context: 8ms, 312 input tokens
    provider: Makora, routing: cost
    first token: 410ms
    completion: 1.1s, 48 output tokens
  StartTask weather-11
    Direct operation
      New York: 420ms
      London: 510ms
      Tokyo: 620ms
    Background update: stored
```

Required fields where applicable:

```text
role
model
provider
routing profile
queue duration
context duration
request duration
first byte
first token
first rendered token
input/output/cached tokens
cost
task ID
worker ID
generation ID
terminal status
```

Trace expansion is local UI state. It never requests regenerated model output.

## Foreground And Background Presentation

Foreground activity is always distinct from background activity.

- A background task must not make the input appear blocked.
- If an independent user turn is accepted, the UI immediately renders its
  `queued` state and then its foreground state transitions.
- If a turn depends on unfinished work, the UI displays the committed facts and
  a concise `work still active` indicator rather than silently waiting.
- The UI must never imply that a worker is the Conversational Agent.

## Notifications And Interruptions

Background updates are represented as durable notifications:

```text
PendingNotification
  notification_id
  source_task_id
  importance
  timing: immediate | next_turn_boundary | when_idle | on_request
  summary
  requires_user_action
```

The footer displays only a compact count, for example:

```text
1 background update ready
```

The user can open, defer, dismiss, or request details. The Interaction Manager
decides when a notification is injected into visible conversation; the
Coordinator cannot write directly to it.

## Info Panel

The Info panel is a global runtime summary, not a log viewer. It includes:

```text
daemon health and uptime
foreground interaction state and generation
active context usage/budget
provider, model, routing profile, active policy, fallback setting
foreground and background queue depths
active/completed/blocked/failed task counts
worker and memory-job counts
latest event cursor/revision
```

It must not display API keys, prompt bodies, full tool output, or credentials.

## Rendering And Performance

1. UI redraw is event-driven and frame-rate bounded for streams.
2. Conversation rendering remains viewport-only and cacheable by revision.
3. Structured state tables use stable row IDs so selection survives updates.
4. Progress/log events are coalesced before they reach the renderer.
5. A disconnected TUI can reattach from snapshot plus event cursor without
   requiring components to regenerate state.

## Accessibility And Interaction

```text
Tab             open/close runtime panel
Left/Right      select runtime tab when panel is open and input is empty
Up/Down         select a row in the active tab
Enter           expand/collapse details or confirm contextual action
Esc             close overlay
```

Text editing retains its normal cursor/navigation keys whenever input is not
empty. Mouse behavior mirrors keyboard behavior; neither is required for core
operation.

## Acceptance Criteria

1. The UI can identify whether latency occurred in queueing, context assembly,
   provider TTFT, tools, workers, or response rendering.
2. A follow-up submitted during background work visibly enters the foreground
   lane immediately.
3. A weather lookup appears as a direct operation rather than three anonymous
   agents.
4. Task state remains accurate after TUI detach/reattach, duplicate events, and
   late worker events.
5. Empty runtime tabs have an explicit explanation and suggested next action.
6. All controls target durable IDs and show pending/authoritative outcomes.
7. No secret, raw prompt, or unrelated worker context appears in normal UI or
   trace output.
