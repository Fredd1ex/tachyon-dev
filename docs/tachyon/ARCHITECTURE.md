# Current Architecture

## Measured Stage Timing

Existing `work_candidate` and terminal `work_result` events carry optional
`candidate.timing` / `result.timing` (`WorkTiming`). No additional IPC events are
emitted for this measurement. For example, a reviewed result can contain:

```json
{
  "timing": {
    "execution_ms": 1000,
    "inference_ms": 600,
    "tool_ms": 300,
    "review_ms": 250
  }
}
```

- `execution_ms`: monotonic elapsed Ghost execution, including instruction setup
  and work cleanup, excluding daemon review. It is not dispatch-to-terminal time.
- `inference_ms`: accumulated sequential model-request waits within execution,
  including any provider retries inside the request. It is not token-generation
  compute time.
- `tool_ms`: accumulated parallel-tool **batch** waits within execution. Parallel
  calls and nested hostcalls are not summed separately. Setup, cleanup and other
  loop overhead need not equal either stage.
- `review_ms`: monotonic daemon wait from accepting a completed candidate for
  review to accepting its decision or handling review failure. Includes queueing,
  coordinator IPC and scheduling; it is not review model inference. Accept,
  rework, inconclusive, unavailable-coordinator and timeout paths measure this
  same interval. A worker-supplied review value is discarded.

Inference and tools are subsets of execution: **do not sum all four fields**.
Execution plus review describes the two measured disjoint phases, but excludes
dispatch and transport gaps and is not an authoritative total work lifetime.
Parallel workers likewise must not be summed into turn wall time.

Missing `timing`, missing fields, and JSON `null` mean unavailable, not zero.
Legacy persisted results deserialize without timing. Successful Ghost execution
reports all three execution fields; a measured zero (for example no tool batches)
is valid. Failed/interrupted Ghost execution and daemon-generated terminal
outcomes currently leave execution fields unavailable rather than publish partial
totals as complete. Review may still be measured when execution is unavailable;
even a no-model decision measures review wait, not inference. Values use integer
milliseconds with sub-millisecond precision truncated after aggregation.

Consumers should read terminal `AgentEvent::WorkResult.result.timing`, keyed by
`(work_id, generation, assignment)`, and retain the existing envelope `task_id`,
`parent_task_id`, `turn_id`, and `tool_call_id` correlation. Daemon routing and
terminal replay retain this payload and existing generation fencing. A candidate
is not a second completed work item and has no daemon review measurement.

Foreground already emits `Timing` stages `model_request_N_started` and
`model_request_N_completed`: these are offsets from turn acceptance, not durations.
Subtract matching pairs within the same turn for foreground inference; missing
pairs are unavailable. `first_visible` and completion timings are also turn
offsets, not additive stages. Existing `ToolTelemetry.duration_ms` remains useful
for individual calls, correlated through `identity.work_id`, `generation`,
`assignment`, and `call_id`; do not sum overlapping or nested calls into tool
wall time. `ToolStarted`/`ToolFinished` envelope timestamps are not a replacement
for batch measurement: Ghost publishes batch results after the batch joins.

## Runtime Ownership

```text
User
  |
  v
Tachyon CLI / TUI
  |
  | Unix socket IPC
  v
Tachyond
  |
  +-- tachyon-foreground (daemon-owned, id `foreground`)
  |
  +-- worker Ghost processes

Provider-neutral conversation and coordination policy lives in
`crates/orchestrators`. The live user-facing turn runtime is the standalone
`tachyon-foreground` process; Ghost no longer has a Conversation role.
```

### Tachyond

Tachyond is the runtime authority. It currently:

- Starts Foreground when the daemon starts.
- Starts worker Ghost processes.
- Tracks in-memory agent state.
- Routes chat and subscription requests. User turns and worker evidence cross
  the foreground subprocess boundary as versioned `InteractionCommandEnvelope`
  values.
- Owns process handles and worker workspaces.
- Publishes line-oriented events over IPC.

The lifecycle contract is defined in
[`../tachyond/LIFECYCLE.md`](../tachyond/LIFECYCLE.md). Semantic retention
decisions currently remain in orchestration policy; Tachyond is authoritative
for process signals, replacement, lease enforcement, and cleanup.

### Foreground

Foreground is a dedicated process started and supervised by Tachyond. It:

- Owns the user-facing conversation.
- Decides whether to answer, delegate, wait, or replan.
- Requests workers through Tachyond.
- Synthesizes worker results.
- Temporarily delegates worker requests directly while the Background
  Coordinator is extracted.

Foreground does not run worker tools directly. Its current model schemas are
`spawn_agent` and `spawn_agents`.

### Workers

Workers are Ghost processes started by Tachyond for focused work. They have
execution tools such as `ipython` and `agent_browser`. They do not
create or control other agents.

Workers may be one-shot or warm sessions. Warm workers remain in `waiting`
after a completed delegated task when orchestration policy retains their
session. Ghost reports semantic completion and Tachyond enforces lifecycle.

Retention defaults to keep-alive. The coordinator must explicitly release a
worker when its context, workspace, and artifacts are no longer worth keeping.
Tachyond may override this only for an explicit safety limit or shutdown.

### TUI

The TUI is a separate `tachyon-tui` client crate. It:

- Sends user messages through the daemon API.
- Subscribes to Foreground and worker output.
- Renders conversation and lifecycle state.
- Provides local presentation controls.

The TUI starts neither Foreground nor Ghost and makes no orchestration
decisions. It still accepts compatibility line markers while outbound event
normalization is completed.

The CLI and TUI share `tachyon-client` for IPC access but have separate
presentation and entry-point code.

## Current Protocol

The shared API is newline-delimited JSON over a Unix domain socket. It supports
daemon status, agent start/list/status/logs/stop/kill/restart, chat, and live
subscriptions. It also exposes daemon-owned `interrupt` and `resume` process
operations. `stop` uses SIGTERM, `interrupt` uses SIGINT, and `kill` uses
SIGKILL on Unix.

The streamed payload is represented as `EventStream` plus a string, with typed
JSON agent events being introduced inside the payload. The lifecycle protocol
must add stable session, turn, task, parent-task, actor, event-kind, and
timestamp fields.

The daemon-to-foreground input path is typed in
`tachyon-api/src/interaction.rs`. Each command carries protocol version,
message/correlation/causation IDs, conversation and optional turn identity,
generation, and timestamp. The stable daemon identity is `foreground`; API
requests are `ForegroundChat` and `ForegroundSubscribe`.

## Internal Campaign Accounting

`tachyond::runtime_store::campaign_ledger` is an internal accounting foundation,
not execution authorization or scheduler integration. Campaign metadata remains
Draft. Only an already host-authorized caller may create one immutable envelope;
there is no child grant, top-up, transfer, or resume operation. Retries must use
new reservation IDs against the same root. Work and verification allowances are
disjoint, but unresolved reservations share the active-inference slot limit.

Unknown usage and cancellation retain the entire hold and slot. Provisional
usage is a cumulative lower bound and holds the componentwise maximum of the
estimate and known usage. Final usage cannot decrease that lower bound; it frees
the slot and only the unused hold. Actuals above estimates remain recordable even
when commitments exceed the allowance (negative remaining budget). Per-attempt
overrun debt pauses admissions in both pools, including zero-unit admissions;
other released holds do not forgive it.

Root updates and successful-command receipts commit in one serialized redb write
transaction. Identical replay returns the original snapshot, not current state;
failed commands leave no receipt. Reads, mutation inputs, and replay snapshots
validate schema, campaign identity, envelope/slot limits, debt/pause consistency,
and pool conservation excluding recorded overruns. These are local invariant
checks, not tamper detection or reconstruction of historical authorization from
receipts. A coherently rewritten database cannot be authenticated by this ledger.

Aggregate totals use numeric JSON `u128` fields and direct typed serde_json
decoding, tested above `u64::MAX` through persistence and replay. Do not route these
records through floats or assume arbitrary JSON consumers preserve these integers.
Individual grants and usage reports remain `u64`.

The methods are synchronous and can block on the database's single writer and
disk commit. Future async integration must move them off the event-loop thread.
Reservations and full-snapshot receipts have no retention bounds: scans and
snapshot writes grow with campaign history, and cumulative receipt storage can
grow quadratically. No usage-evidence verification, cross-campaign retry identity,
execution fencing, external-side-effect atomicity, or runtime budget enforcement
is implemented here.

## Deferred Infrastructure

These are intentionally deferred until the interaction model is reliable:

- Capability portals and user approval.
- Firecracker or other hard isolation.
- Resource allocation and environment profiles.
- Webcam, microphone, and display access.
- Durable task recovery across daemon restarts.
