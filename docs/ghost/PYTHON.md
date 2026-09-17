# Ghost Python Bridge

## Review Evidence

Work results now carry a typed evidence bundle from actual tool results,
including Python hostcalls. Entries retain tool names, call/parent-call IDs,
bounded arguments, and structured output with error/truncation status. Bundles
are limited to 32 entries and 16 KiB serialized, with explicit omission counts.
This lets Background inspect supporting output rather than only the worker's
final assertions. It does not prove extraction succeeded or replace independent
verification. Missing or truncated decisive evidence can still fail review.

Background rework currently ends the assignment as failed; bounded continuation
is tracked in [the implementation TODO](todos/todo.md).

## Scope

### Implementation Boundary

IPython integration belongs in Rust unless it requires running inside the
interpreter. Keep policy out of the Python shim:

- `backend.rs` owns lazy executable discovery, work-scoped sessions, deadlines,
  cancellation, and cleanup.
- `tools/python/bridge.rs` owns process startup, typed protocol validation,
  the package/operation allowlist, require tracking, and native dispatch.
- `registry/activation.rs` owns authorization and activation and builds the
  require reply from native schemas/guidance. Its method descriptors select
  operation/action mappings, the workspace search alias, and always-async flags.
  Agents methods derive from the existing host-selected native action enum;
  no separate Python agents catalog or local permission grant exists.
- `tools/python/mod.rs` owns the native IPython tool schema and result adapter.
- `tools/python/kernel.py` is the sole interpreter shim: IPython hooks, persistent
  namespace, top-level await, framed socket I/O, generic proxy construction, and
  bounded FD capture (including shell output). Replacing these interpreter-local
  hooks directly with Rust would require an extension/embedding boundary; this
  adapter adds neither unsafe code nor dependencies to do so.

The require reply's method descriptors are convenience metadata, not authority.
Python can mutate them or bypass the proxy entirely; Rust still checks its own
catalog, prior authorized require, current policy, and native argument validation.
The synchronous `require` API and default synchronous workspace methods remain
unchanged; exec, ctx, and host-enabled agents methods remain awaitable even when
`asynchronous=False` is passed.

`ipython` is optional. Normal chat, registry discovery/activation, and native tools
do not start Python. Ghost checks for an executable `ipython` on its existing
scrubbed backend PATH when needed, never installs dependencies, and returns an
installation hint when unavailable. This increment adds no dependencies or runtime.

Tool-local guidance favors native read/grep for straightforward inspection and
Python for retained intermediates, aggregation, transformation, or programmatic
iteration, while honoring explicit Python requests. Combining calls is optional.
Eager interfaces carry bounded sampling and coverage checks; full manuals remain
on demand. These are efficiency/reliability hints, not permission enforcement:
registry authorization remains in Rust.

One kernel starts lazily per active work scope on the existing `Local` backend.
Calls serialize within a scope; separate live works do not evict each other.
Registry clones for the same work share the scope; a new
`for_work` registry starts a fresh session on its first Python call. Variables and
imports persist between cells, including top-level `await`. `%cd` persists;
`!cd` does not. Workspace native tool paths still use the current Rust
`ToolContext.cwd`, not Python's `%cd` directory.

## Workspace Proxies

```python
ws = require("workspace", asynchronous=True)
print(ws.guidance)
print(ws.schemas)  # Actual native JSON input schemas, not invented Python APIs.
result = await ws.read(path="README.md")
print(result["content"])
matches = await ws.grep(pattern="TODO")
```

The bridged packages are `workspace`, `exec`, `ctx`, `browser`, `artifact`, and
host-enabled `history`, `work`, `agents`, `todo`, and `monitor`
(see [AGENTS.md](AGENTS.md)). Workspace operations are
`read`, `write`, `edit`, `ls`, `find`, and `grep`. `search` is an alias for **grep**,
with exactly grep's schema (`pattern`, not a separate `query` API). Methods take
keyword arguments and return the native ToolResult envelope as a dictionary.
Native failures raised as ToolError become Python RuntimeError with the structured
error dictionary; native ToolResult `is_error` remains visible in the returned
dictionary. No duplicate file/search implementation exists in Python.

For durable plan and monitor methods, scope grants, revision conflicts and source
authority, see [Durable Plans And Monitoring](TODOS_MONITOR.md). These services use
the same generic descriptors; no Python plan or monitoring state is authoritative.

`require` uses the existing authorized, idempotent activation API. It selects
concise guidance and returns current schemas; it never grants permissions.
`tools` must be enabled, and the whole package must be authorized. Unknown or
denied packages fail in Rust. Every proxy call revalidates the package and then
uses the same registry execution, current ToolContext policy, deadline,
cancellation, telemetry, output store, and native input decoding as direct calls.
Existing proxies can persist across cells, but never retain old permissions.

Use native `tools` action=`list`, action=`activate` package=`NAME`, or
action=`help` package=`NAME` for discovery and detailed help. Authorized native
schemas remain exposed and callable without activation, including `exec`, `ctx`,
`artifact`, `ipython`, and browser tools when available. Recursive IPython remains
denied. Browser calls await the existing Rust browser runner, not the Python
backend; CPU admission uses the inherited host service's separate broker channel.
The serialized Python bridge does not hold that broker connection while waiting.

```python
browser = require('browser')
page = await browser.run(args='snapshot')  # Native agent_browser string args.
artifact = require('artifact')
pending = await artifact.register(path='report.txt', kind='report', description='Results')
```

Rust descriptors map `run` to `agent_browser` and `register` to `artifact`, with
no injected action field. Both methods are always awaitable and expose the exact
native schemas. No browser implementation, hashing, registration, or publication
logic exists in Python. Browser require/help does not provision or start a browser.
Artifact registration emits the native pending event; it is not Ready publication
and bypasses no host publication check. Cached proxies recheck current policy.

`ctx.list(scope='campaign', kinds=['document','artifact'], limit=8)` and
`ctx.search(query='literal', scope='current_work', limit=8, cursor='...')` use the
same history authority and bounded descriptor pages. See [HISTORY](HISTORY.md).
Without scope/kinds, shipped live list and reference-search semantics are unchanged.

Oversized workspace hostcall results use the registry's live-work envelope store,
as do oversized direct tool results. Returned `output_ref` values can be paged or
searched through `ctx` during the same work. Exec stream refs use that same store.
See [EXECUTION.md](EXECUTION.md) for shared capacity and
work-end expiry; envelope storage does not recover output already truncated by
native return limits or Python's capture/wire budgets.

## Awaitable Process And Output Proxies

These are copyable cells in the same live work:

```python
proc = require("exec")
ctx = require("ctx")
p = await proc.start(argv=["/bin/sh", "-c", "printf hello; sleep 1; printf world"], timeout_ms=5000)
operation = p["metadata"]["operation"]
print(p)  # Native dictionary envelope, not a Python process object.
```

```python
state = await proc.wait(operation=operation, wait_ms=1000)
print(state["metadata"])  # Check done; repeat wait if still pending.
page = await ctx.read(reference=p["metadata"]["stdout"], cursor=0, limit=8192)
print(page["content"])
```

`proc.start/run/status/output/wait/cancel` send `exec` with `action` equal to
the method name; `ctx.read/list/search` do the same for `ctx`. All other keyword
arguments and all returned envelopes are unchanged. Inspect `proc.schemas["exec"]`
and `ctx.schemas["ctx"]`. There are no `process`, `ref`, `max_bytes`, or `timeout`
aliases: use native `operation`, typed `reference` dictionary, `limit`,
`timeout_ms` (execution), and `wait_ms` (bounded observation). No Python process
runner, scheduler, polling loop, or result class is added. Start acknowledges
pending before spawn; a missing executable can therefore fail later in status.
Run returns the native run envelope, not a start/status envelope.

`require` remains synchronous, authorized, and idempotent. For existing documented
callers, `require("workspace")` retains synchronous methods (including `search`
as a grep alias). Opt into awaitables with `asynchronous=True`; exec and ctx methods
are always awaitable. Async methods send nothing until awaited. Socket I/O yields
the event loop, but the wire deliberately allows only one outstanding hostcall.
Overlapping calls, including synchronous require during an async call, raise
RuntimeError: await each call before the next. Ordinary asyncio timers can run
during a hostcall. This is not concurrent tool dispatch.

Cancelling an in-flight Python await closes the transport and loses the kernel,
rather than leaving an unread response or retrying a side effect. A Rust exec
supervisor may already exist and continues under its original deadline/token
until cancellation or work cleanup; a lost acknowledgement is not safe to retry.
Use `proc.cancel(operation=operation)` for normal process cancellation and bounded
wait to observe completion. Cell timeouts also bound processes started in that cell;
later cells do not extend their original deadline.

## Protocol And Lifecycle

Control uses a private Unix-domain socket in a random mode-0700 directory,
unlinked immediately after connection. Frames are a four-byte big-endian length
and typed JSON, capped at 1 MiB before Rust allocation. This is not a stdout
marker protocol. Python FD-level output capture includes Python prints, shell
output, and `os.write` to stdout/stderr; arbitrary printed JSON cannot dispatch a
tool or complete a cell. The two output streams are merged, continuously drained,
and retain a 64 KiB prefix with a truncation notice.

Rust validates exact request IDs, sequential hostcall IDs, message variants and
fields, supported operations, object inputs, and a maximum 128 hostcalls per
cell. Code is capped at 256 KiB. Replies have matching IDs and explicit `ok` or
`error` outcomes. Native result envelopes have a 512 KiB wire budget within the
1 MiB frame. The effective cell deadline is the minimum of backend timeout,
current ToolContext deadline, and policy maximum duration. Hostcalls inherit it;
it cannot be extended by Python. Cancellation/deadlines also cover startup and
the session lock wait.

Protocol failure, EOF/kernel loss, interrupted hostcalls, cell timeout, or dropped
in-flight execution closes the socket and kills the kernel process group. The
session is removed from its slot before awaiting any transaction, so a cancelled
future cannot leave a half-consumed protocol available to the next call. Python
syntax/runtime exceptions are normal cell failures and retain the live session.

**Work completion closes the kernel.** The host awaits the generic registry
`finish_work()` on success, failure, or objective cancellation before publishing
the result. Python detaches only that work's slot, kills its process group, and
reaps the kernel. Repeated finish is safe; calls on a finished registry are rejected.
Dropping/replacing the last work handle schedules the same cleanup as a fallback.
Creating another work does not end a still-owned objective. Healthy kernels survive
cells and history compaction within the same objective, not completed objectives.
Untouched work cleanup does not start Python.

Before teardown, Ghost captures optional `WorkResult.final_context` from the native
registry: activated package versions and known live output IDs, not Python globals.
This includes a package first required in the last cell followed by `work.complete`
without another model call, and ordinary failures after registry setup. Interrupted
capture can be absent. The host treats these as bounded informational claims, filters
unmapped/unowned output IDs, and keeps its own budgets, questions and logical handles.
`final` describes the observation boundary, not authorization or installed-version
attestation. See [CONTINUATION](CONTINUATION.md) for exact executable/version checks
before a fresh registry can restore validated activation.

Cleanup futures have a five-second host ceiling and run independently of the
finish waiter. Drop-only asynchronous cleanup needs a live, driven Tokio runtime;
abrupt process termination/runtime shutdown cannot guarantee it. Hosts must await
`finish_work()` before shutdown. Context-free `Local::run_ipython` is a lower-level
backend API, not a work scope; its session lasts until backend drop.

**There is no automatic replay, retry, pickle snapshot, or checkpoint restore.**
Kernel failure loses variables. Side effects already executed may remain, and an
interrupted hostcall may have an unknown outcome. The next explicit call may start
a new empty kernel; callers must inspect external state before retrying writes.
Legacy `.tachyon/ipython.pkl` files are neither read nor removed.

## Limits

This is a local subprocess adapter, not a Python security sandbox. User code has
the host process's filesystem/network rights and can intentionally tamper with
Python internals or exit the process. Registry authorization protects hostcalls,
not arbitrary Python OS access. No daemon credentials, database handles, or raw
ToolContext objects are sent to Python. The environment remains scrubbed.

Hostcalls are restricted to the active kernel thread. Legacy synchronous workspace
calls and require block that thread; awaitable calls use nonblocking socket I/O.
Concurrent hostcalls and background tasks/output are unsupported. A cell leaving output
descriptors open fails the bridge rather than hanging indefinitely. Descendants
that deliberately escape the process group are not contained by this adapter.
Startup diagnostics currently report process status rather than captured import
tracebacks. Transport is Unix-only, matching the existing local backend.

## Verification

`cargo fmt -p ghost` and `cargo test -p ghost` cover this increment. Real-IPython
tests run when the backend finds IPython and otherwise print a skip reason. They
exercise persistence, top-level await, syntax/runtime errors, control-looking
Python/shell output, stderr, native read parity, search mapping, idempotent
activation, unknown/denied require, policy changes, work isolation, cancellation,
timeout, dropped futures, and kernel loss without replay. Work-end tests verify
kernel PID exit on success, cell failure followed by finish, cancellation, active
finish, and dropped work, as well as interleaved work isolation and idempotence.
Fake framed-protocol
tests require no Python and cover invalid sizes, truncated frames, unknown fields,
wrong IDs, EOF, and hostcall timeout/cancellation with exactly one registry entry.
Existing native registry/package tests continue to check schemas and permissions.

No main-loop integration is outstanding: registry dispatch passes the active
registry through a default Tool hook, and IPython uses it without adding handles
to ToolContext. Future increments may deliberately extend the allowlist to other
non-recursive packages, but must define their lifecycle and permission semantics
first.
