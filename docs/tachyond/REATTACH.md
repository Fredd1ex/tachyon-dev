# Persistent Worker Reattachment

## Constraint

Tachyond currently owns each Ghost child's anonymous stdin/stdout pipes. If
Tachyond crashes, a replacement daemon can recover the worker's PID and
workspace metadata, but it cannot recover the old pipe file descriptors. The
replacement cannot safely attach to the old conversation by opening
`/proc/<pid>/fd`; the required read/write ends belonged to the dead daemon.

Recreating Ghost from metadata is therefore not true reattachment and may
create duplicate workers.

## Required Design

Persistent workers need a supervisor-owned control endpoint that outlives an
individual Tachyond instance:

```text
Tachyond
   |
   | Unix socket control/event protocol
   v
Persistent worker supervisor
   |
   v
Ghost + IPython process
```

The supervisor owns the Ghost process and its pipes. Tachyond connects to the
supervisor using a stable endpoint under the session workspace. If Tachyond
crashes, a new daemon reconnects to the endpoint and resumes event/control
ownership without recreating Ghost or its interpreter.

## Endpoint Contract

The supervisor must provide:

- Stable `session_id` and endpoint path.
- Event replay from a sequence number so the new daemon can catch up.
- Bidirectional user/Orchestrator input.
- Heartbeat and process health.
- `SIGINT`, `SIGTERM`, `SIGKILL`, and wait/reap ownership.
- Explicit release and endpoint cleanup.
- Authentication by workspace ownership and daemon runtime identity.

The supervisor should be a small process, not another model harness. It should
not make retention decisions; the Orchestrator still decides whether the
session remains useful.

## Current State

Implemented now:

- Durable session metadata and workspace recovery.
- Foreground conversation checkpoints.
- Serializable IPython variable checkpoints.
- Startup notices to the restored Orchestrator.

Not yet implemented:

- A process-independent supervisor.
- Live event replay and control-socket reattachment.
- Safe adoption of a worker after Tachyond crashes.

Until the supervisor exists, Tachyond must report recovery as `recreated`, not
`reattached`.
