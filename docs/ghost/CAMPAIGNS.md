# Local Campaign Activation

This is an explicit **unisolated local development** entry point. It is not a
microVM, container, sandbox, or robust guest authority boundary. Ghost and the
trusted evaluator run as the daemon user. They can access that user's files,
processes, credentials and sockets despite the cleared child environment and
scoped broker protocol. Never run untrusted objectives, executables or evaluators.

## Commands

Safe local retention is available with `tachyon campaign archive <id>`,
`restore <id>`, and `retention <id>` (also accepting Research IDs). Archive requires
an idle, settled campaign; Research requires all campaigns archived first. These
commands change only durable archive markers, preserve references and data, and
never start jobs. Restore Research before its campaigns. There is no permanent
purge or automatic campaign deletion. `inspect` includes partial aggregated storage
counts, explicitly **not** a shared storage quota or complete disk census. See
[STORAGE](STORAGE.md) for guards, charge policy, exclusions and remaining admission
work.

The evaluator defaults to `result_contract:"exit_success"`. Opt into structured
local measurements with `result_contract:"json_metrics"`, required `metrics`
such as `{"accuracy":{"min":0.95,"max":1},"latency_ms":{"max":100}}`, and
optional `allow_extra_metrics` (default false). The command emits one object such
as `{"accuracy":0.97,"latency_ms":80}` on bounded stdout. Every command bound
and `argv` remains required. These settings are immutable, hash-bound host policy,
not worker-selected thresholds. Root and children reuse the existing command gate,
retained candidate snapshots, and protected verification pool without model spend.
See [VERIFICATION](VERIFICATION.md#structured-local-metrics) for exact parsing,
inclusive finite binary64 bounds, output limits, evidence and trust semantics.

Optional evaluator `stage:"development"` labels local development measurements.
`stage:"final_heldout"` requires `max_attempts:1`, no children and no explicit
continuation, preventing repeated exposure within this campaign. Neither label
proves data independence or scientific confidence. Omitted fields preserve legacy
command behavior.

For a separate root-only human completion route, use evaluator
`{"acceptance_mode":"human","input_bytes":1048576,"max_attempts":1}`.
Do not provide a command, metrics, stage, children or allocation policy. Nonempty
argv and nonzero command bounds are rejected. After exact host publication, the
campaign becomes `awaiting_acceptance` with no execution/inference capacity held
for the wait. The original deadline and protected funding still apply.

```sh
tachyon campaign acceptance campaign-<id>
tachyon campaign accept campaign-<id> --candidate <artifact-id> \
  --candidate-sha256 <sha256> --expected-state <expected_state_sha256> \
  --command-id <unique-command-id> --confirm
tachyon campaign reject campaign-<id> --candidate <artifact-id> \
  --candidate-sha256 <sha256> --expected-state <expected_state_sha256> \
  --command-id <unique-command-id> --confirm
```

These are trusted same-user host attestations, not worker question answers or
automated verification. The CLI never starts a daemon or an extra job. Acceptance
is `accepted_human`; rejection has a human-source receipt and never triggers repair.
Exactly matching command/payload replay returns the original host-attributed receipt;
changed payloads, stale state/version, cancellation, expiry and newer steering
cannot overwrite it. Child/group human acceptance and human continuation are
explicitly unsupported. See [VERIFICATION](VERIFICATION.md#explicit-human-acceptance)
for the exact typed API, immutable candidate/state contract, accounting, attribution
limitations and restart behavior.

Use a daemon built from the same checkout. Campaign commands do not implicitly
start the daemon or route objectives through foreground chat.

```sh
tachyon campaign create "Candidate experiment" "Publish one candidate containing hello"
tachyon campaign run /absolute/path/campaign.json --unisolated-development
tachyon campaign status campaign-<returned-id>
tachyon campaign cancel campaign-<returned-id>
tachyon campaign resume campaign-<returned-id> --unisolated-development
```

`create` creates inert Research and Draft Campaign metadata and prints the campaign
ID. Put that ID and the **exact same objective** in the manifest. Creation is two
metadata requests: if campaign creation fails, the research record remains inert.
Existing Draft campaigns created through the Research/Campaign API also work.

`run` requires the flag, reads the bounded JSON file in the CLI, validates it, and
sends a typed payload, not a filesystem path for the daemon to read. The daemon
validates again. Unknown fields (including credentials, artifact roots and launch
grants) and missing bounds are errors. There is no activation from free text,
ordinary chat, metadata creation, status polling, or model parent controls.

The daemon commits the immutable authorization and Running state before returning
the handle. The connection does not wait for inference, Ghost, or evaluation.
Multiple campaigns may be active up to the configured host campaign cap; excess
launches are rejected, not silently queued. Each daemon-owned thread owns its Tokio runtime and
the command-loop future until process cleanup finishes. The existing command
loop supplies admission, model accounting, artifact selection, protected command
verification and bounded repair attempts. An optional `children` policy connects
exact approved templates to a separate host-owned child scheduler. The root is
never in that scheduler's launch catalog: it has one command-loop owner.

## Shared Host Admission

The daemon reads `[campaign_resources]` from the existing host `config.toml` when
opening its runtime store. Defaults are deliberately finite:

```toml
[campaign_resources]
max_campaigns = 4
max_resident_workers = 8
max_execution_jobs = 2
max_cpu_jobs = 2
max_model_calls = 2
```

These are host counts, **not campaign root allowances**. Valid ranges are 1..64
campaigns, 1..256 resident Ghost processes, 1..256 active Ghost executions,
1..256 native CPU jobs, and
1..64 broker model calls. Invalid
configuration fails closed. Each launch keeps its own immutable manifest, budgets,
task, cancellation channel and Tokio runtime. Status/cancel/resume select only
that campaign; recovery/reconciliation require that campaign's owner to be idle,
not every sibling. Shutdown signals all owners before joining any of them.

The shared Rust `HostCapacity` uses independent semaphore-backed resident, execution and model
queues. Within each resource, ready campaigns get round-robin turns and requests
within a campaign are FIFO. The arbiter holds a mutex only for bounded in-memory
operations; no database, process, filesystem or network work runs under that mutex.
Each queue rejects beyond 4096 waiting requests. Grants are non-preemptive: fairness
means turns when a resource becomes available, not eviction of running work.
Startup validation visits retained launch history in writer slices of at most 64
records; it does not create runtimes or replay those historical launches.

Root and child Ghost launches borrow resident then execution capacity before process creation.
A first attempt waiting on capacity has no `ExecutingUnknown` record yet. Resident
capacity stays occupied while the process is parked in `agents.wait` or `work.ask`;
both the shared execution permit and the **campaign-local** running lease are handed
off after successful suspension. A normal wait/ask reply requires reacquiring both.
The durable running lease is reacquired first, then the fair host execution queue;
no shared execution permit is held while waiting for the durable running lease.
Thus a host
resident cap of one cannot support a simultaneously resident parent and child.
Model calls separately wait for the campaign-only model cap, including calls from
different root ledgers. `campaign status` exposes `host_resident_waiting` and
`host_model_waiting`, independently of durable active leases and budget holds.
The progress API also reports `host_execution_waiting`; no new UI is supplied.

Queue cancellation/deadline removes the request without spawning a worker. Failed
spawns release borrowed capacity. Normal/cancelled/timed-out process owners release
resident and any active execution capacity after the existing process-group
kill/reap cleanup succeeds.
Dropped owners or uncertain cleanup retain the slot for that runtime store's lifetime;
reaping a finished campaign registry entry does not restore it. Exact operator
cleanup receipts and conservative restart reconstruction of unknown resident/execution
identities are implemented; see [CONTINUATION](CONTINUATION.md). This is not
automatic orphan discovery. The
durable execution/billing fences remain authoritative and unknown work is not replayed.
Model permits count local broker calls, not proof that an upstream provider stopped
after disconnect; unknown provider charges remain held in the original root ledger.

Ordinary chat does not borrow these permits. The separate conservative campaign cap
leaves logical foreground headroom without inventing tracking of ordinary provider
calls or guaranteeing CPU scheduling, latency, memory or provider concurrency.
The execution semaphore counts logical active Ghost assignments, not native jobs,
threads, CPU cycles, or verifier commands. It does not pause the operating system
process or arbitrary Python background work during a broker wait. There is still no OS-enforced GPU quota,
arbitrary Python job admission, resident-memory byte quota, or aggregate
artifact/workspace/output-spool retention quota. Tool execution can run directly in
Ghost and spawn local processes. Existing per-artifact-store limits and global emitted
trace byte limits are distinct storage boundaries, not a combined host disk budget.
Native processes can escape process groups; hard containment and OS resource
enforcement require the separately deferred Firecracker/guest or other OS boundary.

All broker-backed native `exec` run/start supervisors, including Rust-backed exec
from IPython and the internal browser command runner, share `max_cpu_jobs` through
`ToolContext.host_service`. Ordinary local exec explicitly has no service and is
ungated. Private `cpu_job` requests now also support typed CPU/GPU acquisition,
with explicit stable-ID GPU inventory (disabled by default), root compute envelopes
and descendant profile bounds. Session-owned cleanup remains authoritative. See
[COMPUTE](COMPUTE.md) for grants, accounting and the non-enforcing device hint.

Busy replies return immediately on the existing sequential broker. The Rust
supervisor sleeps 50 ms with the socket mutex released, then polls until admission,
cancellation or its original deadline. Polls do not consume the model/control
request budget. Bounded class-specific campaign round-robin queues retain no budget
while waiting; FIFO session entries expire after two seconds without polling.
Releases bypass new-admission authorization, so revocation does not obstruct
historical cleanup. Other sequential broker RPCs can still delay a release; a busy
CPU acquisition never waits on capacity while holding the stream.

The permit stays charged through async native cleanup, not just until `start`
returns or the leader exits. Failed spawn releases without native resources;
successful cleanup confirms no live process-group members (Linux zombies are
already terminated). Dropped supervisors, disconnected sessions and uncertain
cleanup retain capacity through durable redb job records, including after restart.
Exact authoritative operator cleanup can release occupancy without an unknown-cost
refund. There is no automatic
reclaim based on Ghost exit, since exec uses separate process groups.
These are logical native-job counts, not CPU utilization limits: threads, escaped
descendants, long-lived browser daemons, provisioning helpers and arbitrary Python
`!shell`/OS calls are not contained or separately counted. Hard enforcement still
requires an OS boundary.

Optional root integer job-duration holds and host-monotonic reconciliation now
measure conservative job wall time, not CPU usage or billing. Unified campaign/
workspace storage accounting snapshots remain separate work.

Local evidence (no paid provider):

```sh
cargo build -p ghost --bin ghost --offline
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --offline actual_simultaneous_campaigns -- --ignored
```

This runs real Ghost processes through two manifest launch owners, at resident/model
caps 1/2, 2/1 and 2/2, plus sibling-preserving cancellation. Separate queue tests prove
round-robin turns, cancellation/timeouts, independent counts and retained uncertain
cleanup. This is not a foreground-latency or adversarial containment benchmark.

## Manifest

Optional `allocation` selects local group concurrency control, for example
`"allocation":{"mode":"deterministic","max_running":2}`. Omitting it preserves
existing fixed behavior. `fixed`, `model_proposed` and `deterministic` are the only
modes; both fields are required and the cap is 1..64, at most
`children.max_running`. The cap also limits each template's host resize ceiling.
Model-proposed mode uses the existing explicitly allowed, revision-checked
`agents.group_resize` route, without an extra model invocation.

By default deterministic mode changes concurrency only for admitted template or dynamic groups,
including nested groups,
with existing funded queued work. Resize cannot generate objectives, admit Work,
increase allowances or infer cancellation from scientific failures. Root concurrency is not
adaptive. Manual resize or steering
relinquishes automatic control durably; restart does not undo user control.
Optional `allowed_actions` defaults to `["resize"]`. The additional supported
actions are `work`, `verify`, `pause` and `stop`, activated only by explicit bounded `signals` with
command ID, exact approved group ID and expected revision. Pause sets that group's
cap to zero. Stop additionally names existing Work and its exact generation, and
requests branch cancellation through existing cleanup, not graceful completion or
whole-root pause. At most one signal applies per tick; command replay is durable.
Work selects an existing fixed approved template key plus its exact parent Work,
generation and instruction revision, with policy CAS/receipt committed in the
catalog admission transaction. It accepts no new objective or budget. Verify names
an existing `AwaitingVerification` execution and its current generation/instructions;
a bounded durable intent is rechecked and receipted atomically with the existing
verifier claim, not with the earlier intent write. Unknown/accepted review is never
replayed. Exact reapproval can reconcile unclaimed intent after reopen. See the
action shapes and precise handoff boundaries in [POLICIES](POLICIES.md).
Allowance reallocation remains unsupported; both actions use existing root budgets
and protected verifier allowances without cross-pool transfers.
See [POLICIES](POLICIES.md) for fences, resource accounting, supported actions,
recovery limitations and the local no-credit tests.

All top-level fields below except `children` and `allocation` are required. The following is illustrative, **not a pricing
recommendation**: replace the ID, paths, deadline, model and every monetary/token
bound with your own explicit authorization before running it.

```json
{
  "schema_version": 1,
  "campaign_id": "campaign-00000000000000000000000000000000",
  "objective": "Publish one candidate containing hello",
  "executable": "/absolute/path/to/ghost",
  "workspace": "/absolute/path/to/dedicated-workspace",
  "home": "/absolute/path/to/dedicated-empty-home",
  "deadline_ms": 1,
  "work_tokens": 100000,
  "work_cost_micro_usd": 1000000,
  "verification_tokens": 1000,
  "verification_cost_micro_usd": 10000,
  "max_active_inferences": 2,
  "model": "your-explicit-model-id",
  "pricing_revision": "your-reviewed-price-revision",
  "max_request_bytes": 262144,
  "input_tokens": 65536,
  "output_tokens": 4096,
  "input_micro_usd_per_million": 1000000,
  "output_micro_usd_per_million": 1000000,
  "other_micro_usd": 0,
  "evaluator": {
    "argv": ["/usr/bin/grep", "-qx", "hello", "candidate"],
    "timeout_ms": 1000,
    "output_bytes": 4096,
    "input_bytes": 1048576,
    "max_attempts": 1,
    "max_total_command_ms": 1000
  }
}
```

`deadline_ms` is an absolute Unix millisecond deadline, in the future and at most
24 hours away. The example's `1` deliberately fails validation. No default
deadline, allowance, token estimate, price or retry count is invented at launch.
Prices use integer microUSD per million tokens, and budgets use integer microUSD.
The host attests the full request upper bound, including tools, reasoning, cache
and other charges. This is not live price discovery or tokenizer estimation.
The same work budget covers all requests and repair attempts; retries do not
replenish it. Missing final provider billing evidence retains unresolved holds.
`max_active_inferences` must be 2 through 64: the ledger initially holds both
root work and protected verification before converting them into allocations.
The inference bound counts unresolved ledger reservations, including queued
verification holds; it is not the running-worker bound. A two-child group generally
needs more than two inference slots even with `children.max_running: 2`.

Paths must be existing, canonical absolute paths, without symlink aliases, `..`,
`.` components, or broad roots, including the evaluator executable. Workspace and child HOME must be separate and
disjoint from daemon storage, and neither may be the user's HOME or its ancestor.
Use dedicated disposable directories. The manifest cannot choose artifact or
verification staging roots: the daemon creates private directories beneath its
data directory at `campaigns/<campaign-id>/`. Same-user access is still possible;
0700 permissions are not guest isolation.

The evaluator receives the one exact host-collected immutable candidate snapshot
as `candidate` in a fresh staging directory. The command and arguments are fixed,
not interpolated from worker output. Do not source or execute candidate bytes.
Acceptance means only that this evaluator passed on these bytes, not general
correctness. Evaluator limits match the existing command gate: at most 8 attempts,
300 seconds per command, 32 KiB output, 16 MiB candidate input and 2400 seconds
total command time. The manifest itself is limited to 64 KiB.

## Provider And Trust

The daemon loads the existing host provider base URL and resolves the OpenRouter
key using the existing environment-first, OS-credential-store-second mechanism.
Malformed host configuration and explicitly configured non-OpenRouter providers
are rejected, not replaced with defaults. The endpoint must be HTTP(S) without
URL userinfo, query or fragment; these are rejected before credential lookup.
No key is accepted in the manifest or CLI argv, and no key is persisted in launch
records or sent to Ghost. Configure the provider using `tachyon providers`.
The explicit manifest model and request bounds are used; launch fixes temperature
to zero, disables parallel tool calls and reasoning, and supplies no routing
override. The stored base URL, model, pricing, evaluator and manifest are immutable
for resume. Credential rotation does not modify the authorization.

Host launch/resume/cancel requests use the existing local control socket and its
OS permissions. This is **same-user local control trust**, not proof that a human
typed a CLI command. Any process that already has broad daemon socket access can
exercise that authority. No new broad socket grant or launch tool is given to
Ghost. Without `children`, the private ModelBroker has no parent control grants;
with it, only the explicit allowlist and optional history capability are enabled.
Preventing malicious same-user native code from reaching host authority remains
an isolation/guest-authentication gap, deliberately not solved here.

## Status And Recovery

```sh
tachyon campaign inspect campaign-<returned-id>
tachyon campaign recover campaign-<returned-id> --unisolated-development
```

For host-only final billing and explicit cleanup attestation, see
[Local Operational Recovery](RECOVERY.md). `campaign reconcile` is separate from
staging recovery and never replays execution or fetches provider evidence.

`inspect` reports durable admissions and retained managed slot paths without
starting jobs or claiming that a marker/snapshot is valid. `recover` explicitly
reapproves the stored dynamic profile policy on an idle host. It checks the exact
stored objective, manifest/deadline and current host provider configuration, then
rehydrates committed receipts and reconciles known uncommitted staging. It does
not resolve provider credentials, contact a model, launch Ghost, run an evaluator,
change campaign status, or schedule admitted work. An original root command policy
must already exist; early unknown launches without one stay blocked. A recovery
error may follow successful recovery of another independent proposal; rerunning
is receipt-idempotent and does not allocate another budget.

Startup still does not reapprove policy or mutate profile directories. Recovery
requires the explicit security flag; `resume` remains the separate, narrowly gated
verification operation described below. Quarantine and markerless directories
need operator review, not automatic deletion. See [Dynamic Investigations](AGENTS.md#dynamic-investigations)
for exact ownership, hash checks, finite retention caps and fail-closed cases.

Campaign metadata retains its version-1 record and snake-case wire encoding.
Actual states are `draft`, `running`, `cancelling`, `cancelled`, `accepted`,
`rejected`, `unverified` and `interrupted`. Status is persisted, not inferred from
whether a CLI connection is open. Accepted/rejected describe evaluation, not final
billing settlement. Unverified includes failed launch, transport, missing artifact,
deadline and verification failures; detailed execution/ledger evidence remains in
the runtime store, but this minimal CLI does not yet expose all diagnostics.
Callback errors and unwinding panics also become Unverified. Running acknowledges
the owned task, not successful admission: ledger/admission and execution setup
still occur asynchronously and can fail after the start response. Early setup
failures may have no execution record. No such failure is automatically replayed.

Cancel signals the owned execution and records cancellation in admitted work;
process cleanup is asynchronous. A running trusted evaluator can take up to its
configured timeout to finish. Daemon shutdown signals and joins the owned worker.
Startup changes interrupted Running/Cancelling launches to Interrupted and does
not reconstruct or replay processes, Python cells, model requests or reviews.

Resume is explicit and requires the native-execution flag again. Only existing
`EvidenceReady` or `AwaitingVerification` execution records are eligible, under
the original immutable manifest, endpoint and deadline. Unknown execution,
unknown review, terminal results, expired deadlines and merely registered work
are not resumable. There is no deadline extension, blanket retry or new budget.
Generated execution policies, TUI controls and microVM isolation are outside this
entry point. Fixed templates cannot change approved objectives or policies.
Optional `children.dynamic` profiles also support bounded investigation objectives
and evidence references within preapproved finite slots, quotas and managed input
snapshots. See [Dynamic Investigations](AGENTS.md#dynamic-investigations) for the
current interface, bounds and crash-recovery limitations.

## Approved Child Swarm

Omitting `children` retains the original single-objective behavior, without agent
controls. Core `work.status`, `work.ask` and `work.complete` remain available.
The host enrolls the root and protected verifier with two lifetime Works and depth
zero. Command review uses one running lease and one resident; verification claims
the running slot only after root execution releases it. Human review retains
upfront protected verifier registration with two running/resident leases, both
released before waiting for acceptance. This grants no child catalog, Spawn
allowlist, extra allocation or provider authority. Human acceptance still reviews
the host-collected current candidate, not the worker's proposed references.
If present, all its limits, `controls`, `history`, `completion` and
`templates` are explicit. There are no default child budgets. Each template has
one to 32 exact specs. A non-null `group_id` makes it a group; null or an omitted
`group_id` makes it a spawn template and requires exactly one spec. Template and
work IDs are selectors, not credentials. Child `work_id` is optional: when absent
or null, the host deterministically uses `<campaign-id>-<template-id>-<zero-based-spec-index>`.
Explicit child work IDs must be globally unique in the runtime store; choose fresh
IDs for a new campaign. Both forms are immutable across resume. The host also generates
`<campaign-id>-root`, `<campaign-id>-verification`, `<work-id>-verification` and
attempt IDs. Do not separately admit or supply policies for those generated IDs.

For a concrete two-child run, create a Draft with the exact objective below, put
the returned campaign ID into this JSON, and replace `deadline_ms` with a future
Unix-millisecond deadline at most 24 hours away. Replace Alice's paths and model
with your host's canonical executable, dedicated existing directories and approved
model. The fixed deadline below is an example, not a sliding authorization. The
price figures are illustrative host attestations, not a provider price lookup.

```sh
tachyon campaign create "Hello pair" "Discover approved templates, run hello-pair as a group, wait for both children, then publish one file containing hello."
date +%s%3N
```

```json
{
  "schema_version": 1,
  "campaign_id": "campaign-00000000000000000000000000000000",
  "objective": "Discover approved templates, run hello-pair as a group, wait for both children, then publish one file containing hello.",
  "executable": "/home/alice/Projects/tachyon/target/debug/ghost",
  "workspace": "/home/alice/campaigns/hello/root-work",
  "home": "/home/alice/campaigns/hello/root-home",
  "deadline_ms": 1789257600000,
  "work_tokens": 90000,
  "work_cost_micro_usd": 900000,
  "verification_tokens": 300,
  "verification_cost_micro_usd": 3000,
  "max_active_inferences": 8,
  "model": "your-approved-model-id",
  "pricing_revision": "your-reviewed-price-revision",
  "max_request_bytes": 65536,
  "input_tokens": 8192,
  "output_tokens": 1024,
  "input_micro_usd_per_million": 1000000,
  "output_micro_usd_per_million": 1000000,
  "other_micro_usd": 0,
  "evaluator": {
    "argv": ["/usr/bin/grep", "-qx", "hello", "candidate"],
    "timeout_ms": 1000,
    "output_bytes": 4096,
    "input_bytes": 1048576,
    "max_attempts": 2,
    "max_total_command_ms": 2000
  },
  "children": {
    "total_work": 6,
    "max_running": 2,
    "max_resident": 3,
    "controls": ["templates", "spawn", "group", "list", "status", "result", "send", "steer", "cancel", "wait", "group_status", "group_resize"],
    "history": false,
    "completion": "cancel_outstanding",
    "templates": [{
      "template_id": "hello-pair",
      "group_id": "hello-pair-group",
      "max_running": 2,
      "specs": [{
        "objective": "Publish one file containing hello.",
        "workspace": "/home/alice/campaigns/hello/left-work",
        "home": "/home/alice/campaigns/hello/left-home",
        "work_tokens": 30000,
        "work_cost_micro_usd": 300000,
        "verification_tokens": 100,
        "verification_cost_micro_usd": 1000
      }, {
        "objective": "Publish one file containing hello.",
        "workspace": "/home/alice/campaigns/hello/right-work",
        "home": "/home/alice/campaigns/hello/right-home",
        "work_tokens": 30000,
        "work_cost_micro_usd": 300000,
        "verification_tokens": 100,
        "verification_cost_micro_usd": 1000
      }]
    }]
  }
}
```

Run the JSON with `tachyon campaign run /absolute/path/hello.json --unisolated-development`.
The two omitted work IDs become `<campaign-id>-hello-pair-0` and
`<campaign-id>-hello-pair-1`; use the actual handles returned by `agents.group`
for status, result and wait calls. Paths for all workspaces and HOME directories must be
mutually disjoint, canonical and outside daemon storage. Children inherit the
root executable, model, endpoint, request bounds, prices and deadline; specs
cannot supply overrides or grant paths through model arguments.

The top-level money/token totals cover **root plus every approved child**. Here,
the root retains 30000 work tokens, 300000 work microUSD, 100 verification tokens
and 1000 verification microUSD. These root allocations are protected before the
root runs. Child admission draws only from the same envelope; it does not borrow
the root's allocation or create funds. Every spec has its own protected verifier.
Children use the same command, model policy and deadline. By default each child
gets exactly one attempt and one command timeout of total command time, even if
the root allows repair. To opt in, add
`"evaluator":{"max_attempts":2,"max_total_command_ms":2000}` to an individual
template spec or to a `children.dynamic.profiles` entry. Both bounds are required
when this optional object is present. Attempts include the initial execution and
must be 1..8; total command time must cover `timeout_ms * max_attempts` and be at
most 2400000 ms. This object cannot override argv, model, permissions or prices,
and is never accepted in model spawn/group arguments. Omitting it preserves old
manifest serialization and one-attempt behavior. The root's retry policy remains
independent.

The child scheduler uses `execute_campaign_command_loop`, with one owner for all
attempts of the same Work. A fresh Ghost process gets failure feedback, a fresh
Attempt ID and an increased assignment, not a fresh Work or allocation. All calls
and retries share the original work allowance and protected verifier allocation.
Failed attempts release running capacity for verification/repair but do not mark
the child terminal. A parent's wait sees the final child only after bounded review
and final billing, or returns partial outstanding results at its own timeout or
resource block. `agents.result` projects retained repairable rejection as
`rework_pending`, not final rejection. Unknown billing holds remain charged and
block further attempts. Attempt/fund/command-time exhaustion closes known rejection;
an expired host deadline stops continuation without granting more time or money.

Dynamic repair reuses the originally prepared workspace and HOME, retaining work
outputs intentionally. It never reruns input preparation, recaptures context,
revalidates the mutable workspace as an untouched snapshot, or copies changed
source files over the baseline. The original input hashes and quotas stay fixed.
Use `"task_type":"coding"` in the explicit profile permissions to permit native
candidate writes; `coding_read_only` remains read-only. Exec and Python retain
their separate explicit flags. Native write/edit reject read-only existing files,
including the copied baseline inputs, even though an atomic rename would otherwise
replace them. Each published attempt candidate gets its own immutable ArtifactStore
snapshot. These tool checks are not protection against explicitly enabled arbitrary
native code or hostile same-user filesystem access.

`total_work` includes the root, children **and all verifiers** (six here), with a
maximum of 256. Optional `children.max_depth` defaults to one and accepts 1..8;
root Work has depth zero. `max_running` bounds
execution leases across the campaign, including verifier execution;
`max_resident` bounds scheduler task ownership including the separately owned
root, which remains resident while waiting. These bounds are not memory limits.
With two running slots and three residents, `agents.wait` releases the registered
root's slot so both children can overlap. It reacquires a slot before returning.
Queued Work and verifier budget holds also consume `max_active_inferences` slots
until converted to allocations. The manifest therefore requires at least
`2 * (child spec count + 1)` inference slots, leaving room for the root to request
its next action before waiting. With the current 64-slot ceiling this entry point
supports at most 31 declared child specs, even when their running cap is one.

To authorize nested dynamic delegation, set `children.max_depth` to at least two
and explicitly set the delegating profile's `profile_ids` allowlist, for example
`"profile_ids":["inspect"]`. The default is empty, not every profile. Selectors
must name configured profiles without duplicates; self-inheritance is allowed,
but never bypasses the depth limit. The root may select all host-configured
profiles. A dynamic child may select only profiles allowed by its own source
profile. Fixed public templates remain root-bound; internal exact host templates
can bind a child parent. Native spawn/group and Python use the same typed proposal
path. Neither accepts a worker-supplied parent, ancestry, model, budget or path.

Profile slots and `dynamic.max_proposals` are campaign-lifetime totals across
every depth, not quotas multiplied per parent. Each stable slot reserves its
fixed per-child allowance from the same root Work/verification envelope. Increasing
depth creates neither slots nor money. Completed/cancelled slots are never reused.
Command IDs are campaign-scoped and require the exact original parent and payload,
including interrupted preparation. Reapproval restores that recorded parent.
Nested cap-one waits need residency for root, child and grandchild (at least three),
in addition to the unchanged inference-hold constraint above.

Only actions explicitly listed in `controls` appear in the Ghost agents interface.
`agents` package discovery/loading grants nothing by itself. Use
`{"action":"templates","limit":32}` to discover only this logical parent's
approved selectors and group/count/running bounds, never host paths or model
policy. Use `group` with that selector and a stable command ID, then `wait` on the
returned work IDs. Repeating the exact admission command returns the same handles;
changing its payload conflicts. Children cannot select their parent's templates.
`history: true` separately enables read-only, campaign-scoped retained research
context and artifact queries; it is not access to arbitrary local history paths.
`resource` is not accepted in the agent control allowlist; use the explicit history
switch instead.

The only completion policy is `cancel_outstanding`: terminal parent Work cancels
its unfinished descendants recursively, and explicit parent cancellation records
recursive intent atomically. Queued descendants are confirmed unspent; active
descendants retain holds until their Tokio task/process owners confirm cleanup.
Root completion stops child
dispatch, cancels queued/unfinished children and joins all started task owners.
The campaign cannot report Accepted before that cleanup. Uncertain termination,
storage failure or cleanup past the campaign deadline becomes Unverified, not
success. Terminal child leases alone are insufficient: child budget holds must
also be final or closed, so missing child billing evidence remains Unverified.
Cancellation likewise remains asynchronous, with owned process cleanup;
it never authorizes detached workers. A command may need its bounded timeout to
finish cleanup. A blocked host storage operation may delay cleanup past the
deadline; the host still drains rather than abandoning workers.

The durable launch contains a SHA-256 digest of the canonical typed manifest,
including every template, bound and allowlist. Catalog receipts also bind exact
command configuration and host staging location. Older single-root launch records
without a digest remain readable; new child policies require it. Resume cannot
edit these policies or replay unknown executions/reviews, including children.

`campaign status` reports whether this daemon still owns the campaign task, total
admitted work, queued work, active leases, waiting work and terminal work. Counts
include verifiers and are bounded observations, not a transaction-wide snapshot.
After restart, active counts are last-known durable leases, not live-process
proof; the campaign is Interrupted and no process is replayed.

## Tests

Default tests do not launch a daemon or call a provider:

```sh
cargo test -p tachyon-api campaign::tests
cargo test -p tachyon --lib campaign_requires_explicit_native_execution_flag
cargo test -p tachyond --bin tachyond campaign_launch::tests
```

The explicit integration test starts a freshly built Ghost against an in-process
localhost HTTP fixture and injects a fixture model configuration. It never loads
user provider configuration or credentials, and never starts an installed daemon:

```sh
cargo build -p ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --bin tachyond actual_ghost_launch_uses_supplied_local_model_without_user_credentials -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --bin tachyond actual_root_only_work_status_ask_complete_command_gate -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --bin tachyond actual_ghost_human_acceptance_typed_dispatch_no_pending_model_or_execution -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --bin tachyond actual_campaign_group_wait_command_verification_and_cancel -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --bin tachyond actual_dynamic_children_managed_inputs_wait_verify_and_cancel -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --bin tachyond actual_campaign_children_repair_final_results_and_exhaustion -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --bin tachyond actual_scheduler_pending_repair_resize_cancel_and_total_bound -- --ignored
```

The fixture deliberately publishes no artifact: the expected result is Unverified,
not acceptance of free text. The existing command-loop integration suite separately
covers successful artifact verification and bounded repair.
The swarm fixture exercises actual campaign activation, template discovery, group
admission, two overlapping children at running cap two, parent wait handoff,
separate immutable verification snapshots, protected root funding, activity
counts and cancellation while waiting. Its HTTP server and workers are local test
fixtures; it does not contact the configured provider or change an installed daemon.
Child repair fixtures additionally exercise actual spawn/group, cap-one and cap-two
parent waits, final accepted/rejected result reads, exact HTTP attempt counts,
unknown billing with partial wait timeout, original snapshot retention after source
mutation, native baseline write denial, and pause/resume/cancel during pending repair.
