# Private Model And Control Transport

## Agents Bridge

The same one-use private connection now carries tagged `FrameRequest` and
`FrameReply` enums (`kind`: `model`, `control`, `boundary` or `boundary_ack`,
depending on direction; `payload`: the typed body).
Length framing and the peer/token handshake are unchanged. This is an in-repo
protocol change, with no compatibility decoder for old untagged model frames;
rebuild host and Ghost together. No additional socket or public IPC authentication
surface was added. See [AGENTS.md](AGENTS.md) for exact native/Python APIs.

Host configuration is a separate `ModelBroker::with_controls` allowlist, empty
by default. The private bootstrap includes only its action catalog, not a permit
or a worker-selectable actor. A control-enabled broker session installs the
`agents` package; normal local chat does not. Python `require('agents')` dispatches
through `ToolRegistry::execute("agents", ...)`, rechecking current package/tool
policy. Its finite native-hostcall allowlist still excludes recursive IPython,
model calls, and browser calls. Loading a package does not grant host permissions.

Each control operation checks the host allowlist and exact current active Work
permit/reservation identity, then uses the existing coordination facade. Targets
are Work IDs within the enrolled self/direct-parent/direct-child graph; the host
supplies campaign and sender identity. Send and steer retain the store's stricter
direction rules. Authority is held through the synchronous operation so revocation
serializes with acceptance. Storage runs on the blocking pool; no transaction or
authority lock crosses await, transport I/O, or model execution.

Model completions finish before tool execution. Parallel tool calls share the
client connection mutex, including control calls made from Python. Mutex waiting
and I/O remain bounded by the tool context and host session deadlines. Cancellation
after taking the connection closes it rather than consuming a late reply on a new
call; a blocking commit can still finish, so an interrupted command is unknown,
not rejected. The connection remains sequential, with no pipelining or reconnect.
Model logical IDs claim new inference only. Controls create no model claim and
use durable stable command IDs for send/steer and catalog admission idempotency. All frame types share
the 128-request session bound and 1 MiB framing ceiling; controls additionally
enforce 256-byte IDs, 4 KiB text, and 32-item pages. Result snapshots exclude full
candidate/transcript output. Scoped Work cancel and group status/resize are now
available by explicit allowlist; see [AGENTS](AGENTS.md). Spawn/group additionally
require an exact host-approved template bound to the permitted parent. They accept
only a template key, command ID, and (group only) an optional lower initial running
cap. They reserve child and verification Work atomically from the existing campaign,
return durable queued handles before completion, and publish approved executable
descriptors for the host scheduler's next tick. No worker paths, arbitrary objectives,
context refs, budgets or provider policy enter this interface. Parent wait and
automatic model-boundary message delivery are described in [LIFECYCLE](LIFECYCLE.md).

For enrolled Work, each model request first reserves its existing allocation and
prepares durable host instruction context atomically. The host writes a `boundary`
response containing the immutable objective, effective revision/instructions,
at most 32 unread messages, and a rolling 32-message data context. The client must
return the exact ID/cursor in `boundary_ack`; this is delivery acknowledgment, not
authority to apply instructions. The host persists delivery before claiming HTTP.
Unexpected, stale or unsolicited frames fail closed. An insufficient reservation
leaves accepted instructions and messages pending without advancing application.

Only the host injects canonical instructions and labeled untrusted message data
into the provider request. Worker history, including system-role text, grants no
authority. A revision change replaces the permit while preserving the allocation,
pricing, deadline and launch cancellation lease. Future funding/claim checks use
the persisted effective revision. Successful completion records canonical revision
metadata for Ghost's optional `WorkResult.instruction_revision`; missing legacy
metadata cannot stand in for a newer applied revision.

Delivery cursor advancement is idempotent for the current receipt; after a newer
boundary is prepared, old receipts are stale. Reopening preserves applied context,
delivery and unknown request holds but creates no authority and replays no launch
or provider call. Fresh explicit host authorization can reassemble the bounded
context; it cannot reuse an old reserved request ID. Transport failure is not proof
that a request or instruction transaction failed to commit.

## Current Path

Production-compiled Unix APIs now connect the existing Ghost `AgentModel` loop to
the daemon's exact-policy `ModelBroker::execute`:

1. Trusted host calls `tachyon_model::broker::private_pair()` for a fresh
   `HostChannel` and `BrokerClient`.
2. Host runs `ModelBroker::serve_private(channel, &permit, reservation, deadline)`
   with its selected model, exact authorized reservation, and absolute deadline.
3. `BrokerClient` implements Ghost `AgentModel`; pass it to the existing `run_loop`.
   There is no second agent loop and no worker-side accounting implementation.

Linux also has an **internal, explicitly invoked subprocess path**:
`ModelBroker::launch_private`. It is not called by the normal daemon scheduler,
campaign APIs, or ordinary Ghost spawn helpers. Campaigns remain disabled.

The next internal callable is `ModelBroker::execute_campaign` (Linux). It consumes
an already authorized `ExecutionPolicy`: exact registered work funding, model
reservation, original `WorkRequest`, a separate registered verification Work in
the same campaign's protected pool, and a stable evaluator configuration ID. The
host supplies executable/workspace/HOME paths, an absolute deadline, and a
deterministic non-spending async evaluator returning `Accepted`, `Rejected`, or
`Unverified`. It launches real Ghost through `launch_private`, not a second agent
loop. See [ADMISSION.md](ADMISSION.md#internal-execution) for durable states.

Only one bounded, completed `WorkCandidate` matching work ID, objective, generation,
and assignment is eligible for review. Envelope scope is also checked. Missing,
duplicate, oversized, or mismatched candidates cannot be accepted. Worker timing
is removed from evaluator evidence; worker metrics never settle model billing.
Acceptance means acceptance under the configured evaluator, not proven correctness.
This patch supplies no command evaluator or model reviewer. Its tests use explicit
fixture evaluators. A callback must be cooperative, cancellable, deterministic and
non-spending; the deadline cannot preempt arbitrary blocking host code.

The trusted host first admits work and records a registered worker identity, then
passes that admission, an exact model reservation, a `WorkRequest`, absolute
executable/workspace/HOME paths, and a deadline to the helper. The helper validates
the binding, issues a permit, checks live funding, and commits a durable one-launch
claim before spawning. It uses the registered identity, not an identity supplied by
the worker. Spawn failure, cancellation, or daemon restart never clears that claim
or proves the allocation unspent. There is no automatic relaunch.

The host creates a random temporary directory atomically with mode 0700 and binds
one Unix listener inside it. A framed bootstrap containing its pathname and a fresh
one-use token goes only to child stdin, followed by the framed work request. Neither
is placed in argv, environment, or model prompts. One accept consumes the listener;
safe Tokio `peer_cred` must match the spawned child PID and host effective UID before
the existing token handshake can authorize any model traffic. The directory and
socket are removed on accept, rejection, cancellation, or timeout. No unsafe code,
raw inherited descriptors, database handle, or provider credentials cross this boundary.

Ghost's explicit `--broker` mode bypasses `Config::load` and provider credential
resolution and invokes the existing task/model/tool loop through `BrokerClient`.
It is one-shot, incompatible with `--chat` and `--task`, and requires an agent ID.
Ordinary chat and spawn helpers are unchanged. Stdin is bootstrap-only; the separate
Unix stream carries model/control frames. Stdout retains existing JSON events and diagnostic
lines, with the host collecting at most 1 MiB and returning typed `EventEnvelope`s.
The envelope must identify the registered worker as its session, actor and task,
without a foreign conversation or parent task. Event IDs and payloads remain
worker evidence, not billing authority. Stderr is discarded. Invalid or oversized
stdout cancels outstanding model I/O rather than waiting for the host deadline.

The child environment is cleared, then only fixed PATH (`/usr/bin:/bin`), LANG,
and host-configured HOME are set. Tool execution applies its existing separate
environment policy, including workspace HOME. No provider/daemon environment or
host executable override is inherited. The caller must supply a suitable isolated
HOME and workspace; this is credential minimization, **not filesystem or network
isolation**. Tool processes that deliberately escape their process group still need
the future OS containment layer.

## Authority

Every pair has a generated UUID-v4 capability, sent once as 16 bytes with a
one-byte acknowledgement before framing. The capability is separate from the
model permit and has no Debug, logging, or environment delivery API. The subprocess
bootstrap serializes only this transport token to private stdin.
It is carried only over private IPC; provider and daemon credentials never
enter this IPC. Pair possession and trusted host binding are the authority boundary,
not OS isolation. The handshake is not remote identity authentication. Host code
must not hand the client endpoint to an unrelated assignment.

Worker model requests contain only a logical ID, messages, and tool definitions. There
are no worker-selected model, provider, endpoint, pricing, funding, purpose,
identity, output cap, or deadline fields. System messages and tools remain untrusted
content, not authority. Unknown top-level fields are rejected. Host policy maps
every request to the bound permit; dispatch revalidates exact policy and liveness.
Existing broad `ApiRequest` grants confer no access to this channel.

## Bounds And Failure

- Frames are big-endian u32 byte length plus JSON, limited to 1 MiB before input
  allocation. Serialization uses a capped writer; there is no unbounded reply buffer.
- Each request allows at most 256 messages, 64 tool definitions, and a 256-byte
  logical ID. Each connection accepts at most 128 sequential requests. No queue,
  pipelining, reconnect, retry, or reserve-only operation is exposed.
- Accounted provider SSE is capped at 1 MiB total, with at most 64 indexed tool
  calls. Non-success HTTP bodies are not collected for accounted requests. These
  are additional safety ceilings, not pricing or token estimates. Host output-token
  and complete provider-request byte bounds are still enforced independently.
- IDs are fresh UUIDs in the adapter. The existing durable allocation-scoped
  dispatch table rejects previously claimed IDs before HTTP, including after
  permit replacement or restart. Replies must match the outstanding ID exactly.
- The absolute host deadline covers handshake, ingress, execution, and reply
  backpressure. Only one provider future runs per connection. Host cancellation,
  EOF, I/O failure, or unsolicited pipelined bytes drop outstanding provider I/O.
- A cancelled adapter call takes and drops its socket; it cannot accidentally
  read a late reply on a later call. Errors poison the connection. No detached
  provider task or automatic retry exists.
- A committed dispatch claim remains unknown after cancellation unless final
  reconciliation has already begun. Storage tasks may still finish. Reply loss
  does not reverse recorded billing, refund unknown usage, or permit replay.
- Provider/storage error details are not forwarded. Worker errors are generic;
  completion usage is display data, never worker-authored billing evidence.
- Subprocess bootstrap and execution share the host deadline, also capped by the
  work deadline. Ghost independently bounds bootstrap at five seconds. Its broker
  shutdown does not wait indefinitely for Tokio's uncancellable stdin read.
- The launcher owns a fresh process group, kills it on return or future drop,
  revokes its permit, and reaps the leader on normal/timeout completion. Tokio's
  kill-on-drop/reaper handles cancellation. Normal exit is observed with safe
  `waitid(WNOWAIT)` before group cleanup and leader reaping, avoiding recycled PID
  cleanup. Admission/claim storage calls remain synchronous host setup; a deadline
   is rechecked before spawning after storage returns. Final leader reaping has a
   separate one-second cleanup limit; kill-on-drop/reaper handles a delayed reap.

## Verification

Focused tests cover framing, one-use handshake, wrong PID/UID, private directory
permissions/removal, malformed/missing bootstrap against Cargo's actual Ghost
binary, denied/expired funding, cancellation and bootstrap-deadline process cleanup.
Post-handshake stdout/exit tests use a local `/usr/bin/python3` protocol fixture.
They also check that a forged caller-side registration cannot replace the stored
worker identity. Spawn-failure tests verify the durable claim after reopening.
The daemon end-to-end test launches Ghost, executes `/usr/bin/env` as an actual
tool between two localhost fake HTTP/SSE model responses, checks credential
exclusion and final events, reconciles the real ledger, and denies relaunch after
reopening the store. It never calls an external provider.

The agents subprocess test has the fake model issue parallel status/list/send/result
calls plus an unrelated-target request, checks tool results in the next model
request, and verifies the durable message in the store. Control tests cover
allowlist denial, exact identity mismatch, live revocation, command replay/conflict,
accepted versus acknowledged revisions, current versus historical result snapshots,
and reopen persistence without model dispatch claims. A real IPython test uses a
mock private host to check `require('agents')`, native argument-validation parity,
policy changes, and rejection of recursive IPython calls without credentials.

Execution coordinator tests additionally cover accepted/rejected fixture review,
evaluator timeout, malformed provider evidence, in-flight duplicate callers,
persisted restart queries, cancelled-review recovery, unknown/provisional holds
blocking closure, and exactly-once unused-budget return. Scope/size/duplicate and
missing-candidate checks use typed collected-event fixtures; the spawned Ghost
tests use real private transport and local fake provider responses, not live models.

Because tachyond is a binary crate, Cargo does not supply `CARGO_BIN_EXE_ghost` to
its tests. The cross-package test is explicitly ignored by default and requires a
fresh build plus an explicit absolute executable path; it never guesses freshness:

```sh
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" OPENAI_API_KEY=broker-env-sentinel OPENROUTER_API_KEY=broker-env-sentinel TACHYON_DAEMON_TOKEN=broker-env-sentinel cargo test -p tachyond subprocess_tests -- --include-ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond execution_tests -- --include-ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond catalog_tests -- --include-ignored
cargo test -p ghost --test broker_bootstrap
cargo test -p tachyon-model broker --lib
cargo test -p ghost real_python_agents
cargo test -p tachyond controls_recheck
```

These subprocess tests expect an unprivileged Linux user, consistent with Ghost's
root guard; the launcher does not inherit a root-guard override.

## Remaining Architecture

The proposed microVM boundary is defined in [SECURITY.md](SECURITY.md). Current
Linux subprocess PID/UID peer checks are host-local, not guest authentication:
guest PIDs cannot be compared with host PIDs. A future VM backend needs a scoped,
authenticated host-to-instance channel while preserving host-only ledger, secrets,
provider access, evaluator authority, and no-replay semantics. No VM transport is
implemented by this document.

The opt-in [host catalog scheduler](GROUPS.md#runnable-host-adapter) now integrates
registered launches, task ownership, cancellation and review polling. Ordinary
production activation, arbitrary objective/context mapping and orphan recovery remain gated.
Exact host-approved catalog selection and bounded [dynamic investigation profiles](AGENTS.md#dynamic-investigations)
are implemented; neither permits model-generated execution policy. Admission receipts and parent links are durable, while host
callbacks/paths must be explicitly re-approved on restart before execution can resume.
This helper requires prior host registration; it does not install a live worker in
the ordinary daemon registry or turn on campaigns. `execute_campaign` now reviews
collected evidence and closes allocations when all holds are final, but does not
authorize reuse of this one-shot worker. The existing registry/background review
helper is deliberately not called: it is not the protected campaign review route.
Daemon hard-crash
or machine-restart orphan cleanup requires supervisor/cgroup integration.
Generation migration, multi-policy assignment grants, authoritative late billing,
and provider pricing/routing validation remain release gates in
[MODEL_ACCOUNTING.md](MODEL_ACCOUNTING.md).
