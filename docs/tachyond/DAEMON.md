# Tachyond

Tachyond is Tachyon's persistent process and task authority. It owns agent
processes, worker lifecycles, IPC connections, subscriptions, workspaces, and
durable task metadata. Foreground performs user-facing model work, Ghost performs
worker model/tool work, and Tachyond supervises where that work runs.

## Responsibilities

Tachyond is responsible for:

- Starting and supervising the standalone Foreground process.
- Starting temporary Background-compatible and worker Ghost processes.
- Starting, retaining, replanning, staging, releasing, and restoring workers.
- Assigning stable agent IDs and tracking parent relationships.
- Creating per-worker workspaces and cleaning generated workspaces up.
- Relaying stdout, stderr, structured events, and terminal state.
- Accepting user or agent control requests through the Unix socket API.
- Persisting task metadata and lifecycle decisions.
- Persisting and delivering one-shot reminders and scheduled agent work.
- Enforcing dependency ordering for tasks that declare prerequisites.
- Reconnecting restored agents after a daemon restart.

Tachyond does not decide how a user-facing response should sound. It does not
own model prompts, personas, TTS style, or conversational policy. Foreground
adapts provider-neutral policy from `crates/orchestrators`.

## Process Model

```text
Tachyond
  |
  +-- tachyon-foreground (id `foreground`)
  |
  +-- Background Coordinator Ghost
  |
  +-- Worker Ghosts
```

The foreground runtime uses the stable `foreground` daemon identity and
`ForegroundChat`/`ForegroundSubscribe` API requests. Legacy serialized request
and actor names are accepted only as decode aliases.

## IPC

Clients communicate with Tachyond over a newline-delimited JSON Unix socket.
The socket path is managed by `tachyon-util` and is shared by the CLI, TUI,
Foreground, Ghost, and diagnostic tools.

Requests include operations for:

- Daemon status and health.
- Starting, listing, inspecting, subscribing to, and chatting with agents.
- Sending input to an agent.
- Executing a command in an agent workspace.
- Retaining, staging, releasing, and replanning agents.
- Reading logs and attaching to an existing stream.

Responses are typed API values. Streaming requests return event envelopes over
the same connection until the stream ends.

## Scheduling

Foreground exposes contextual reminder creation, listing, and cancellation,
plus scheduled agent work with `start_at` and `finish_by` semantics.
The Background Coordinator validates each typed scheduling command and returns
it to Tachyond, which owns the timer and durable state in `runtime.redb`.
Relative delays and absolute local `HH:MM` times are converted into
daemon-authoritative wall-clock deadlines when committed.
When a reminder becomes due, Tachyond sends private trigger context to
Foreground. The Conversation model produces a new standalone chat message, and
delivery remains retryable until Foreground acknowledges publication. Overdue
pending reminders are recovered after daemon restart. If no user-facing
subscriber is connected when a deadline passes, the reminder remains pending
and is delivered when Tachyon is opened again.

For agent work, `start_at` waits until the deadline before launching a worker;
`finish_by` launches immediately and passes the requested time through as the
worker's hard deadline. Results are stored durably and published through the
same modeled standalone notification path. Scheduled work is available through
a typed list API for runtime inspection. Recurrence, named timezone expressions,
task cancellation, and explicit misfire policies remain.

## Agent Lifecycle

An agent normally moves through these states:

```text
Starting -> Running -> Waiting -> Completed
                     |          |
                     v          v
                   Failed     Released
```

The exact state depends on whether the process is a short-lived worker, a
retained warm worker, a persistent supervised process, or Foreground.

### Start

When an agent starts, Tachyond:

1. Allocates an ID.
2. Resolves or creates its workspace.
3. Records task, owner, purpose, lifetime, and dependency metadata.
4. Spawns the Ghost process.
5. Registers subscriptions before worker output is pumped.
6. Returns typed agent metadata to the caller.

Tasks with unsatisfied dependencies remain visible in `Waiting` state until
their prerequisites complete.

### Retention

Workers can be assigned a lifetime class:

- `short`: disposable work; may be released after completion.
- `long`: retained for useful follow-up work.
- `persistent`: supervised durable work with a control socket.

Retention is daemon state, not a prompt convention. Tachyond remains the final
authority over process cleanup.

### Replan

Replanning replaces a worker's objective while preserving its identity and
task metadata. The daemon increments the worker generation so an old process
cannot overwrite the state of its replacement.

### Release And Stage

`release` terminates and cleans up a worker. `stage` places a worker in a
visible grace period before cleanup, allowing a user or coordinator to retain
it. A staged worker remains inspectable until its deadline or an explicit
release.

## Event Streaming

Tachyond broadcasts worker output to active subscribers and records event logs
best-effort for diagnostics. Events may include:

- Structured status changes.
- Tool start and completion.
- Reply deltas and final replies.
- Worker completion and release requests.
- stdout, stderr, and process exit information.

Worker completion is retained long enough for a late subscriber to receive the
terminal result. This prevents a fast worker from completing between `start`
and `subscribe` and leaving its caller blocked.

Raw worker output is for the Background Coordinator and trace views. Foreground
currently receives correlated evidence for user-facing synthesis.

## Concurrency And Safety

Tachyond is designed to run independent workers concurrently while avoiding
shared-state races:

- Registry mutations are protected by short-lived locks.
- Process and network waits occur outside registry lock ownership.
- Worker generations prevent stale process pumps from changing replacement
  state.
- Subscriptions are isolated per agent.
- Completion and cleanup operations are safe to repeat.
- Dependency checks happen before a worker is started.
- Work queues and task lifetimes prevent unbounded retained work.
- A worker cannot manage Tachyond through its local execution backend unless a
  future explicit control capability grants it access.

Tachyond does not hold a conversation lock while waiting for model responses,
worker output, or command execution. Foreground owns conversational scheduling
and spoken-output arbitration.

## Workspaces And Security

An explicitly requested workspace is used when supplied. Otherwise Tachyond
creates a dedicated directory under its managed workspaces path. Generated
workspaces are cleaned after completion or release unless workspace retention
is explicitly enabled for development.

The current local backend is a guardrail, not a hard security boundary. Future
Firecracker or container backends can replace process execution without
changing the Tachyond lifecycle protocol.

API keys are not stored by Tachyond configuration. Provider credentials are
resolved from `OPENROUTER_API_KEY` or the operating system credential store
configured by `tachyon providers login`; neither requires systemd.

## Restart And Recovery

On restart, Tachyond rebuilds its registry from persisted task metadata and
attempts to restore eligible agents. A restored process receives its existing
identity and workspace. Generation tracking prevents an old process or stale
subscription from modifying the restored instance.

Agents that cannot be restored are marked failed and remain inspectable for
diagnostics. The user-facing Foreground always starts with a fresh checkpoint
and turn sequence; durable background task, history, and curated-memory data
remain independently daemon-owned.

## V2 Task Bridge

The initial V2 bridge is implemented as the typed `BackgroundDelegate` request.
Foreground submits work through that boundary instead of using
the generic user-facing `AgentStart` request. Tachyond owns worker creation,
subscriptions, lifecycle, and result delivery.

The full V2 Background Coordinator will expose normalized updates to the
Conversational Agent through a typed task bridge:

```rust
struct TaskUpdate {
    task_id: String,
    parent_task_id: Option<String>,
    state: TaskState,
    priority: TaskPriority,
    progress: Option<String>,
    result: Option<String>,
    error: Option<String>,
    timestamp_ms: u64,
}
```

The coordinator may request attention for `Normal`, `Urgent`, or `Important`
updates. The Conversational Agent owns the policy decision about whether to
weave a result into the next response or interrupt the user's current flow.
Tachyond transports these facts but never writes the spoken response.

## Current Limitations

- Fully normalized event envelopes with actor and parent metadata are still
  being introduced.
- Active model/tool interruption and dependency-aware cancellation are not
  complete.
- Local execution is not a hard sandbox boundary.
- Foreground is process-separated; the Background Coordinator still requires
  independent scheduling and extraction from Ghost compatibility code.
