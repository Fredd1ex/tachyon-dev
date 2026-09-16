# Ghost Execution And Output

This is the bounded, local Phase 2 execution patch, not a scheduler, process
checkpoint system, or sandbox. See [HARNESS.md](HARNESS.md) for registry ownership
and [SANDBOX.md](SANDBOX.md) for the isolation boundary.

## Exec Contract

The existing `exec` invocation remains valid:

```json
{"argv":["/bin/printf","hello"],"cwd":".","timeout_ms":1000}
```

Exactly one of `argv` and policy-permitted shell `command` is required for
`run` (the default) and `start`. Existing environment scrubbing, workspace cwd
validation, argv/command limits, null stdin, preview formatting, exit status,
and structured errors remain. Shell changes do not persist.

```json
{"action":"start","command":"printf ready; sleep 1; printf finished","timeout_ms":5000}
```

Start returns a pending acknowledgement before waiting for spawn or completion:
`metadata.operation` is an opaque `exec:<uuid>` string; `metadata.stdout` and
`metadata.stderr` are typed objects such as `{"id":"output:<uuid>"}`. Use the
exact returned values. These are not paths, PIDs, client-selected work IDs, or
authorization tokens portable to another work. Server-side membership in the
registry's generated work scope is authoritative, including across clones.

| Action | Fields And Behavior |
|---|---|
| `run` | Original invocation; waits and returns the original result shape. In a work registry it launches and awaits the same supervisor used by start. |
| `start` | Original invocation; validates arguments, reserves spool capacity, then acknowledges a pending supervisor task. Cwd/spawn errors can arrive asynchronously. |
| `status` | `operation`; immediate snapshot, no unbounded output embedded. |
| `wait` | `operation`, optional `wait_ms` (default 1000, max 30000); bounded observation only, not a new execution deadline. |
| `cancel` | `operation`; idempotent cancellation request, returns a snapshot. Wait for `done=true` to confirm termination and cleanup. |
| `output` | `operation`, `stream` (`stdout` default or `stderr`), `cursor` (byte offset, default 0), `limit` (1..8192, default 8192). |

`pending` includes queued-for-spawn, running, and cleanup/drain in progress.
Terminal states are `completed`, `timeout`, `cancelled`, `spawn_failed`, or
`failed` (validation, pipe, I/O, or cleanup error). `completed` does not imply
success: inspect `result.exit_code`, `result.signal`, and `result.error_code`.
The result metadata also reports per-stream observed bytes and preview
truncation. Status/control calls themselves succeed when the observation is
valid, even if the observed process failed. Unknown/cross-work operation or
output refs return permission denied. Malformed inputs return invalid input.

## Host CPU Admission

Broker-backed exec borrows a shared host native-job lease before spawn through
`ToolContext.host_service`. CPU is the default; explicit GPU requests require host
inventory and a manifest/profile grant. Rust-backed Python exec and the browser
command runner use the same hook. Ordinary local CPU exec remains ungated.
`[campaign_resources] max_cpu_jobs` defaults to 2 (valid 1..256); GPU defaults to
zero jobs and empty inventory. See [COMPUTE](COMPUTE.md) for the canonical contract.

The existing sequential broker answers `try_acquire` immediately with a permit,
busy, or denied. Busy supervisors sleep with the socket mutex released, bounded by
their cancellation token and original execution deadline; no blocking acquisition
holds up another job's release. `start` can therefore remain pending without ever
spawning. Capacity stays charged until confirmed native process-group cleanup.
Release remains authorized after revocation. Uncertain cleanup or lost transport
retains the permit and full duration hold durably rather than guessing it is free.

Cancelling an acquire/release waiter does not interrupt its in-flight CPU frame or
poison the shared broker connection. Late grants with no spawned process release
automatically; confirmed cleanup release continues independently of supervisor
cancellation. Historical duplicate releases succeed only in the issuing private
Work session, including after revocation. Session IDs fence foreign release; redb
records retain accounting and unresolved occupancy across restart. Exact operator
cleanup receipts release occupancy without refunding unknown cost. There is no
automatic orphan discovery or process replay.

This is not hard CPU enforcement. Python's direct `!shell` and arbitrary OS calls
bypass native job admission. Threads and escaped or long-lived helper processes
are not independently counted. See [CAMPAIGNS](CAMPAIGNS.md#shared-host-admission).

## Lifetime And Termination

The installed ExecTool owns independent maps keyed by `for_work`'s generated
scope. Clones share one work. Tasks do not retain the registry. `finish_work`
first ends dispatch, then detaches and cancels that work's operations and waits
for their completion; dropping the last work handle provides the same fallback
while a Tokio runtime remains available. Dropping a start/wait caller does not
abort the supervisor. The original cancellation token is inherited by each
operation; canceling one operation does not cancel its siblings.

All detached supervisor handles are guarded for abort on drop before cleanup
awaits any of them. If the five-second teardown hook times out or is dropped,
every remaining supervisor is aborted, not just the one currently awaited.
This fallback is not a guarantee of completed asynchronous reaping at shutdown.

Execution timeout is the minimum of requested/default 120 seconds, policy exec
duration, policy tool duration, and the remaining original context deadline.
Polling cannot extend it. Unix termination sends TERM to the process group,
waits the grace period, escalates to KILL, and reaps the direct child. Normal
leader exit also cleans up remaining group members. Work-supervised grace is
clamped to at most one second to fit the existing five-second work teardown
budget. Output capture continues concurrently during termination. A bounded
post-cleanup drain failure is an explicit failed operation, not success.

The shared native runner also serves non-work direct exec and internal browser
calls without requiring async references or a registry. Transient `ETXTBSY`
spawn failures retry every 10 ms within the same cancellation/deadline bound;
other spawn failures do not retry. No successfully spawned command is replayed.
Non-Unix cleanup only kills/reaps the direct child. Process-group escape,
hostile filesystem changes, uninterruptible kernel I/O, and abrupt runtime/host
death are not hardened containment guarantees.

## Bounded Output

`runtime/output_store.rs` adds an anonymous temporary-file live-work store and
extends the shared `ToolOutputStore` interface with bounded page/search and
reference enumeration. Exec writes fixed 8192-byte chunks; no reader buffers
an entire line. Stdout and stderr drain concurrently into separate prefix
spools, even after retention fills. Discarded bytes are counted, never appended
past the cap. A disk write failure disables further retention but not draining.

- Per-operation spool reservation is at most 64 MiB, further limited by
  `max_exec_output_bytes`, divided equally between stdout and stderr (odd byte
  rounded down). Default policy may retain much less than 64 MiB.
- Per-work spool reservations total at most 128 MiB and 256 entries, shared by
  exec streams and published registry result envelopes (at most 128 starts/runs
  when no envelopes consume entries). Completed/spawn-failed entries consume capacity
  until work end; there is no eviction/release action. Exhaustion rejects new
  exec invocations before process spawn; envelope publication can instead return
  no output reference. Temp files allocate only retained bytes.
- Start's in-memory head/tail preview is separately capped at 8192 raw bytes;
  direct run preserves its policy-sized preview, hard-clamped to 64 MiB. UTF-8
  rendering and JSON metadata add bounded overhead; these are not RSS quotas.
- Read content is at most 8192 UTF-8 bytes, with metadata/JSON framing additional.
  Registry response/line/model budgets can further shorten delivered content.
- Pages report `cursor`, `next_cursor`, `has_more`, `retained_bytes`,
  `total_bytes`, `discarded_bytes`, `storage_failed`, `utf8_valid`, and
  `decoding_truncated`. `truncated` includes omitted retained-page bytes,
  discarded bytes, or decoding clipping. These are snapshots, not EOF promises:
  check operation `done` before treating a current end as final.
- Read cursors count raw source bytes. Lossy UTF-8 replacement can split a
  multibyte character at a page boundary; very small byte limits can clip the
  replacement text, but the source cursor still advances. There is no binary
  round-trip API in this patch. Search compares literal UTF-8 bytes directly.

## Ctx Contract

The eager `ctx` package requires ReadFilesystem capability and uses the same
live-work output interfaces as exec, not raw database or workspace search.

```json
{"action":"list"}
```

```json
{"action":"read","reference":{"id":"output:<returned-uuid>"},"cursor":0,"limit":8192}
```

```json
{"action":"search","reference":{"id":"output:<returned-uuid>"},"query":"error","cursor":0}
```

List returns up to 32 lexically ordered references per call with an entry
cursor. Concurrent starts can change that ordering; list cursors are not stable
snapshots. Read is identical to exec output. Search scans one page, not the whole
operation, and returns at most 128 overlapping byte-offset matches. Queries are
1..1024 UTF-8 bytes and must be shorter than limit. Its next cursor overlaps
the page boundary to find split matches; resume there as new data arrives.
No regex, workspace/history search, or hidden full-output retrieval is used.

## Deliberate Limits

Ctx catalogs both live-work exec stream refs and oversized registry result
envelopes. After applying return-byte/line bounds, registry dispatch publishes a
JSON envelope exceeding `max_model_content_bytes` to the live-work store and
returns its `output_ref` when storage succeeds. Ctx read/search accesses that
bounded JSON, not unbounded original tool output. Each envelope reserves its
serialized size, at most 64 MiB, against the shared work capacity.

Dispatch without a work scope falls back to the context's output store. The
existing WorkspaceOutputStore's durable JSON-envelope refs remain unchanged and
are not accepted by ctx; possession of those workspace-wide refs is not work
authorization. Live-work publication does not also persist a durable copy.

Live refs and operation state expire at work end and do not survive Ghost
restart. Temp files are anonymous, not recoverable checkpoints. There is no
durable process continuation, reattachment, operation replay, stdin streaming,
resource scheduler, or model/network dependency. Python exposes this same dispatch
through awaitable `proc = require("exec")` and `ctx = require("ctx")`. For example:

```python
proc = require("exec")
ctx = require("ctx")
p = await proc.start(argv=["/bin/printf", "hello"], timeout_ms=1000)
state = await proc.wait(operation=p["metadata"]["operation"], wait_ms=1000)
print(state["metadata"])
print((await ctx.read(reference=p["metadata"]["stdout"], limit=8192))["content"])
```

Method names supply only native `action`; keyword arguments and result dictionaries
are unchanged. In particular, references are objects, not paths or strings, and
wait is bounded, not a promise of completion. Python calls recheck current package
and ToolContext authority. Only one hostcall may be outstanding; await sequentially.
Cell deadline/token is inherited by starts, and cancelling an in-flight Python
await closes the kernel without replaying or automatically cancelling an already
admitted supervisor. Work cleanup still owns that supervisor. See
[PYTHON.md](PYTHON.md) for synchronous workspace compatibility and full limits.

## Verification

Run `cargo fmt -p ghost` and `cargo test -p ghost`. Local tests cover pending
start/live reads, cloned/foreign scopes, completion/nonzero exit/spawn failure,
concurrent million-byte streams without newlines, paged read/search overlap,
binary decoding bounds, operation/work reservation caps, timeout and cancellation
with real TERM-resistant processes, explicit work finish, last-handle drop,
cleanup-timeout abort of all supervisors, and work-scoped registry envelope reads.
The existing native exec, browser, and Python compatibility tests remain enabled.
