# Agent Lifecycle Contract

This document defines the boundary between Ghost's semantic worker signals,
Tachyond's authoritative task state, and Unix process operations. The terms in
this document are protocol concepts; they are not interchangeable with Unix
signals.

## Authority

Ghost reports what it believes about its work. The Orchestrator owns the
context-aware retention decision. Tachyond owns process handles and enforces
that decision, but must not infer that a completed task is no longer useful.
The TUI only displays the daemon's state.

Ghost must not self-terminate merely because it believes a task is complete.
It emits a structured completion event. The Orchestrator then chooses one of:

- Keep the worker warm for related work, such as an ML training workspace.
- Keep the worker warm with an explicit bounded lease.
- Release the worker immediately when its artifacts and context are no longer
  useful.
- Interrupt or kill the worker if policy or the user requires it.

Tachyond may apply a safety limit only when the Orchestrator has not supplied a
valid lease, the daemon is shutting down, or a resource safety threshold is
exceeded. That fallback must be visible in the lifecycle reason and must not be
presented as a context-aware decision.

## Default Retention Policy

Retention is determined by the session's lifetime class. A completed task does
not have one universal retention outcome:

- **Short** workers are disposable sessions with a three-assignment budget.
  Tachyond releases them when that budget is exhausted and cleans up their
  private workspace.
- **Long** workers remain reusable while the current daemon is alive. The
  Orchestrator may release them explicitly or Tachyond may retire them through
  an expired lease or safety policy.
- **Persistent** workers run under an independent supervisor. A graceful daemon
  shutdown leaves the supervisor alive; the next daemon reattaches through its
  control socket. They require explicit release.

This keeps weather lookups and other one-off work from accumulating while still
supporting development, data analysis, and ML workflows where the next useful
action belongs in the same environment.

## Lifetime Classes

Every logical worker session should have a `lifetime_class`. This is an
Orchestrator-selected policy hint that Tachyond enforces mechanically:

| Class | Example | Default behavior |
| --- | --- | --- |
| `short` | Weather lookup or a small sequence of related checks. | Reuse for at most three completed assignments, then release; no restart recovery. |
| `long` | Research, coding, or an ML project worker. | Retain and reuse during the current daemon lifetime; do not restore after daemon restart. |
| `persistent` | A personal workspace or long-running service. | Preserve logical identity, workspace, artifacts, and checkpoints across process or daemon restarts; release explicitly. |

The class belongs to the logical session, not the process. A session may be
restarted while keeping its identity, workspace, artifacts, and conversation
context. After verifying that a result satisfies its objective, the Background
Coordinator may promote or demote the class as future needs become clear.
Crossing the persistent boundary recreates an idle completed worker under the
appropriate supervised or daemon-bound process topology. Tachyond alone applies
the state transition and signals or kills the process.

`long` optimizes for reuse during the current interaction. `persistent`
optimizes for continuation over time and requires durable recovery metadata.
Persistent session metadata, workspace reconstruction, Foreground conversation
checkpoints, and serializable IPython variable checkpoints are implemented. A
restarted session receives the same logical identity and restores checkpointed
state. Unserializable Python objects, active network handles, and external
processes still require explicit artifact/checkpoint handling.

## Agent Properties

The minimum metadata needed for the Orchestrator to manage workers at a glance
is:

| Property | Purpose |
| --- | --- |
| `session_id` | Stable identity across process restarts. |
| `lifetime_class` | `short`, `long`, or `persistent` retention policy. |
| `state` | Running, waiting, completed, failed, released, or another lifecycle state. |
| `retention` | Whether the session is currently retained and its optional lease deadline. |
| `owner` | Orchestrator, parent task, or user/workspace that owns the session. |
| `purpose` | Human-readable role, such as `research`, `weather`, or `ml-training`. |
| `workspace` | Filesystem location containing the live working state. |
| `artifacts` | Checkpoints, model files, reports, logs, and other recoverable outputs. |
| `activity` | Last heartbeat, current operation, and idle duration. |
| `health` | Healthy, degraded, stalled, or failed. |
| `resource_usage` | CPU, memory, disk, and optional VM/resource cost. |
| `rebuild_cost` | Estimated cost/time to recreate the session from artifacts. |
| `checkpoint` | Whether the workspace can resume after process replacement. |
| `capabilities` | Tools, network access, hardware, and permissions available to it. |
| `priority` | Relative importance when resources are constrained. |
| `lease` | Explicit expiration, if the Orchestrator chose bounded retention. |

The first implementation may omit live resource metrics, but `session_id`,
`lifetime_class`, `state`, `retention`, `owner`, `purpose`, `workspace`,
`activity`, and `lease` are required for the initial management view.

## Management View

The Orchestrator should receive a compact session summary rather than infer
policy from raw process IDs:

```text
session       lifetime    state    purpose       retained  idle   action
ml-project    persistent   waiting  ml-training   yes       2m     keep
research      long         waiting  research      yes       8m     keep
weather       short        completed weather      yes       1m     keep (1/3)
browser-7     short        failed   lookup        no        -      release
```

The `action` column is an Orchestrator decision. Tachyond reports facts and
executes the selected action; it does not infer that an idle process is safe to
free merely because it is not currently consuming CPU.

The TUI agent pane and trace/chat worker-start notices should expose the
lifetime class, retention state, and short-session budget. For example:

```text
research  long  waiting  retained
weather   short completed retained  turns 1/3
```

Long and persistent sessions expose their retention state directly. Short
sessions disappear after their third completed assignment unless the
Coordinator promotes them; any idle completed session may also be explicitly
released or reclassified after result verification.

The `staged` state is the manual-intervention window. A staged worker remains
alive until `stage_until_secs`; `agent_retain` returns it to `waiting` and
clears the termination deadline. If the deadline passes, Tachyond applies the
normal release sequence.

## Semantic States

| State | Meaning | Process expectation |
| --- | --- | --- |
| `created` | Task exists but has not started. | No child process required. |
| `starting` | Tachyond is creating the execution environment. | `spawn`/`exec` is in progress. |
| `running` | The worker may be executing model or tool work. | Child is alive. |
| `waiting` | The current task is complete; the Orchestrator has retained the worker for possible reuse. | Child remains alive and idle. |
| `staged` | The worker is marked for termination but remains recoverable during its grace TTL. | Child remains alive until the stage deadline or a retain action. |
| `completed` | The logical task is complete; retention is tracked separately. | Child lifetime follows the Orchestrator's retention lease. |
| `interrupted` | Work was asked to stop cooperatively. | Child may still exist until it exits or escalation occurs. |
| `failed` | Work ended unsuccessfully or the child exited abnormally. | Child is not expected to remain alive. |
| `terminated` | Tachyond forcibly ended the worker. | Child must not remain alive. |
| `released` | The task/session lease ended and resources were reclaimed. | Child and private workspace are gone. |

`waiting` is the warm-session state. It is not the same as `completed`:
`completed` means no future use is expected, while `waiting` means reuse is
allowed but not guaranteed.

## Ghost-to-Tachyond Events

Ghost reports these semantic events over its existing stdout event channel:

| Event | Required fields | Meaning |
| --- | --- | --- |
| `worker_completed` | task, result, `suggested_reuse` | Current objective reached a terminal answer; Ghost may suggest retention but cannot decide it. |
| `worker_waiting` | reason, idle deadline | Worker is safe to retain but has no active work. |
| `worker_release_requested` | reason | Ghost believes the session has no remaining value. |
| `worker_failed` | error, retryable | Work ended with an error. |
| `worker_heartbeat` | task, activity timestamp | Optional liveness signal for long tools. |

These events do not directly send Unix signals. Tachyond validates the event,
updates the daemon-authoritative state, forwards the relevant result to the
Orchestrator, and applies retention policy.

## Orchestrator Controls

| Control | Meaning |
| --- | --- |
| `agent_retain(id)` | Keep the logical session and workspace available. |
| `agent_retain(id, lease)` | Keep it until an explicit lease deadline. |
| `agent_release(id)` | End the session and reclaim its process/workspace. |
| `agent_replan(id, task)` | Reuse the retained session with a new objective. |
| `agent_await(id)` | Wait for a semantic state transition or inspect completion. |

Retention is class-driven by default. The Orchestrator uses `agent_retain` to
keep or promote a session and `agent_release` when its context says that
retaining the worker is no longer worthwhile. Tachyond returns the retention
decision and lease in agent status.

## Tachyond Operations and Unix Equivalents

| Tachyond operation | Semantic effect | Unix mechanism |
| --- | --- | --- |
| `interrupt` | Ask active work to stop and preserve diagnostic context. | `kill(pid, SIGINT)`; escalate only after timeout. |
| `stop` | Gracefully terminate a worker/session. | `kill(pid, SIGTERM)`, then `waitpid`; escalate to `SIGKILL` after timeout. |
| `kill` | Immediate termination. | `kill(pid, SIGKILL)`, then `waitpid`. |
| `release` | Terminate and reclaim the worker lease and workspace. | `SIGTERM` -> `waitpid` -> `SIGKILL` if needed, then recursive workspace cleanup. |
| `stage` | Mark a worker for delayed termination so the user or Orchestrator can intervene. | No signal initially; deadline enforcement later uses release semantics. |
| `restart` | Replace the process while retaining task identity. | Kill old child, `fork`/`exec` equivalent via `Command::spawn`, create fresh pipes. |
| `replan` | Replace the worker with a new objective. | Same process sequence as `restart`, with a new task objective. |
| `await` | Wait for a semantic state transition, not merely process exit. | Event/condition wait; `waitpid` is only used when the child exits. |
| warm retention | Keep an idle session available. | No signal; retain child, pipes, workspace, and session lease. |
| idle retirement | Enforce an expired or missing retention lease. | `SIGTERM` -> `waitpid` -> `SIGKILL` fallback, then cleanup. |

The implementation uses Rust's `Child::kill`, `Child::wait`, and
`Command::spawn` as the safe wrappers around these operations. Direct raw
syscalls are not required.

## Completion and Retention Flow

```text
Ghost finishes objective
        |
        v
worker_completed { artifacts, context, suggested_reuse }
        |
        v
Orchestrator evaluates future context and sends a retention decision
        |
        +--> retain -> Tachyond marks task waiting
        +--> retain with lease -> Tachyond marks task waiting until deadline
        +--> release -> Tachyond terminates and cleans up
        +--> replan -> Tachyond reuses the retained session

Tachyond enforces the decision; it never infers context from process idleness.
```

The Orchestrator should not need to mention worker IDs or process details to
the user. It receives the result and a concise lifecycle status from Tachyond.

## Current Implementation Boundary

Implemented today:

- `SIGINT`, `SIGTERM`, and `SIGKILL` lifecycle controls.
- Explicit release with process termination and workspace cleanup.
- Warm worker reuse with default retention and a provisional idle TTL fallback.
- Structured reply events used to detect the current worker answer.

Still required before this contract is complete:

- Explicit `worker_completed`, `worker_retain`, and `worker_release_requested`
  event/command variants.
- A durable retention lease and session identity independent of a process ID.
- Orchestrator-owned retention decisions before applying the idle fallback.
- A blocking semantic `await` operation.
- Graceful-stop timeout followed by `SIGKILL` escalation in one shared helper.
- Durable lease/retention metadata across daemon restart.
