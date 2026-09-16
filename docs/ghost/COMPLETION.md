# Ghost Completion Status

Canonical local feature/gate status, reconciled against the working tree on
2026-09-16. **Bounded local Ghost feature milestone implemented and locally
validated**; the broader production release gates remain deferred below. No missing
required interface was found in the narrowed local milestone: Python browser/artifact
bridge, unified scoped `ctx`, stopping snapshots with `final_context`, production
operator integrating writer, and root-only core Work controls are present.
This is not completion of the original security, scalability or future target.
The implementations below do not establish every requirement in the
[whole harness target](../../roadmap/harness/README.md) or
[tool-runtime roadmap](../../roadmap/v0.3.0/GHOST_TOOL_RUNTIME.md).
See [HARNESS](HARNESS.md) and [TODO](todos/todo.md). Source and repository fixtures
are the evidence authority; this snapshot includes uncommitted local source.
No installation, promotion, commit or permanent planner/reviewer/state agent is
implied. Artifact publication commits immutable evidence, not a source-code commit.

Source entrypoints for the five reconciled features: Ghost
`harness/tools/python/bridge.rs`, `harness/tools/ctx/mod.rs` and `main.rs::run_loop`;
daemon `runtime_store/research_context/stopping.rs`, `runtime_store/integration.rs`
and `runtime_store/campaign_launch.rs`; CLI `crates/tachyon/src/campaign.rs`.
The typed contracts live in `crates/tachyon-api/src/{context,integration,work}.rs`.

## Implemented Local Boundaries

| Area | Implemented and tested boundary |
| --- | --- |
| Retained storage | Host-owned `runtime.redb` root/campaign reservations gate artifact publication, trace objects/uploads and managed profile snapshots. Optional strict `retained_storage_bytes` and configurable host `max_retained_storage_bytes` have finite 64 GiB defaults. Durable intent precedes writes; Ready metadata reconciles actual bytes, unresolved failures retain charge, quarantine recopy reserves separately, and startup metadata census adopts old content as debt without deletion. ArtifactStore uses a cross-store adapter, not a two-database atomicity claim. Logical per-resource bytes exclude anonymous live output, verification scratch and unmanaged workspace/native writes. See [STORAGE](STORAGE.md). |
| Harness/tools | Registry policy/discovery, eager/lazy interfaces, compaction reconstruction, persistent Work-local IPython and typed Rust bridge, native workspace tools, bounded async exec/live `ctx`, lazy browser lifecycle. Python browser.run and artifact.register dispatch existing native tools with the same policy, CPU hook and pending-publication semantics; recursive IPython is denied. |
| Core Work | Broker-native/Python `work.status`, durable `work.ask` with CLI answers and resident-state-preserving capacity handoff, and `work.complete` as an unverified proposal. Ordinary chat gains no host authority. See [WORK](WORK.md). |
| Delegation | Approved spawn/group templates and bounded dynamic objectives/context/input snapshots; durable replay, cap/depth/ownership denials, status/list/result, recursive cancellation, revisioned resize and drain-on-shrink. Public Work depth defaults to one, explicitly bounded to eight with inherited profile allowlists; at most 31 declared child slots are shared across all depths, not multiplied per parent. See [CAMPAIGNS](CAMPAIGNS.md). |
| Communication/wait | Durable send/steer with separate accepted, applied and delivered revisions at permitted model boundaries. `wait` supports all/any/count, partial timeout/resource-blocked results, running-slot release/reacquisition and bounded residency. See [LIFECYCLE](LIFECYCLE.md). |
| Campaign accounting | Explicit CLI run/status/cancel/inspect and narrowly gated resume; shared campaign model-token/microUSD ledger, per-request reservations, protected verification, unresolved holds and unchanged allowance across retries. Ordinary dispatch is not thereby budget-enforced. See [CAMPAIGNS](CAMPAIGNS.md). |
| Concurrent campaigns | Bounded per-campaign task/runtime/cancellation ownership; configurable shared resident-process, active-execution and broker-model count caps with bounded round-robin admission queues, scoped waiting status, isolated cancellation and unchanged independent root budgets. Root/child launch acquires resident and execution slots before first `ExecutingUnknown`; wait/ask releases execution while retaining residency and reacquires before reply. Real localhost Ghost tests cover resident/model caps 1/2, 2/1 and 2/2, execution caps 1/2, and nested Python wait at host execution cap one. These are logical process-local limits, not native tool-job or CPU/memory/disk quotas. See [CAMPAIGNS](CAMPAIGNS.md#shared-host-admission). |
| Verification/repair | Host command gate over exact immutable Ready artifacts, bounded root and separately opted-in child repair, stable Work with fresh attempts, retained failed candidates, unknown-billing fencing and root cleanup before acceptance. Source bytes must match explicit registration before Ready metadata is committed. See [VERIFICATION](VERIFICATION.md) and [ARTIFACTS](ARTIFACTS.md). |
| Durable evidence | Scoped history attempts, authored findings, artifact versions, explicitly registered documents and emitted tool request/result traces, including submitted Python code. Private broker snapshots, bounded model-result objects and streamed retained process spools share checksum-verified trace paging and quotas. Original live output IDs map to durable scoped refs; native read envelopes are retained when capacity permits. `history.snapshot` is host-advertised; `ctx` external reads reuse history authorization. Not uncapped output or full model-conversation history. See [HISTORY](HISTORY.md) and [TRACE](TRACE.md). |
| Recovery | Explicit `campaign recover` reconciles approved managed staging; `campaign reconcile` applies fenced operator-authoritative billing/cleanup receipts. Neither replays unknown execution, fetches provider evidence nor restores a kernel. See [RECOVERY](RECOVERY.md). |
| Native compute | Shared CPU count and explicit stable-ID GPU inventory (empty/disabled by default); typed CPU/GPU exec, Rust-owned cleanup leases, fair nonblocking admission and optional root `cpu_job_ms`/`gpu_job_ms` envelopes with descendant profile bounds. Pre-spawn holds and final conservative host-monotonic job-wall-time accounting persist in `runtime.redb`; unresolved occupancy survives restart. Exact operator cleanup releases occupancy without refunding unknown cost. Python Rust-backed exec and browser runners share the hook; arbitrary Python OS calls remain ungated. GPU selection is an environment hint, not device access enforcement or passthrough. See [COMPUTE](COMPUTE.md). |
| Allocation | Fixed, model-proposed and deterministic modes; adaptive resize plus finite host-selected Work/Verify/Pause/branch Stop signals and scoped unused-allowance transfers. Work's fixed-catalog admission shares policy CAS/receipt transaction; Verify persists exact bounded intent then commits its receipt with the existing verifier claim, never replays unknown review or changes evaluator config. Durable manual takeover, publication/intent reconciliation, nested parent scope and protected root budgets are covered. Transfers conserve campaign/pool funds and cannot move settled or uncertain usage. Real-Ghost tests cover Work dispatch, deferred Verify without model replay, capacity waiting, and Stop cleanup with unresolved billing retained. See [POLICIES](POLICIES.md). |
| Explicit continuation | `campaign continue` claims one fresh process/attempt for the same eligible stopped-unverified root Work, using the exact committed host stopping checkpoint and state hash. The bounded inventory includes child/group handles, instruction refs, questions, allocation availability and produced resource versions; pre-inference diagnostics cannot authorize continuation. Identical command replay does not relaunch; original allocations, usage, permissions and deadline remain authoritative. Executable SHA-256, launched binary/version and retained package activation are checked before inference. Valid `final_context` supplies informational final observations, with stale/unknown fallback when absent. Real-Ghost localhost tests exercise prior child/group result access and exact retained artifact reads in the new attempt without duplicate side effects. No kernel/cell replay or child scheduler restart. See [CONTINUATION](CONTINUATION.md). |
| Retained host slots | Unknown local execution identities reconstruct conservative resident/execution occupancy after recovery; current-process uncertain cleanup retains identity-bound slots. A committed exact operator cleanup receipt releases them once only with no active campaign task. Native CPU/GPU holds now have separate durable restart accounting in [COMPUTE](COMPUTE.md). Billing alone is not cleanup; no automatic orphan discovery is provided. See [CONTINUATION](CONTINUATION.md#claims-and-recovery). |
| Conditional workspace edits | `read` returns a descriptor-metadata version and a digest only for a complete bounded read. `edit`/`write` accept `expected_version` and `expected_sha256`; the host `workspace::apply_exact_patch` helper requires a version. Cooperating native writers use per-target cross-process advisory locks through validation/atomic rename/sync on Linux, not an OS/filesystem CAS against shell writers. Unconditional calls remain possible. See [workspace usage](../../crates/ghost/src/harness/tools/workspace/usage.md) for lock storage and limitations. |
| Scripted local dogfooding | Actual Ghost sequential Rust repair and bounded root/child evidence retrieval, test-local single-writer versioned integration, one host question answer, exact immutable command-verified source/patch/report and per-response fake usage accounting. See [ACCEPTANCE](ACCEPTANCE.md). Proves exercised code paths, not model competence, a production worktree workflow or benchmark performance. |
| Structured/human acceptance | `json_metrics` validates complete unique finite numeric stdout against host bounds over the exact Ready candidate/config hash. `final_heldout` is single-attempt with no children or explicit continuation, not proof of unseen data. Separately, root-only human acceptance retains `AwaitingAcceptance` and exact-version, state-fenced idempotent accept/reject receipts; no evaluator/model runs while waiting. No child/group human acceptance, repair or continuation, and no independent human identity authentication. See [VERIFICATION](VERIFICATION.md). |

## Remaining Local Work

This section records limits and future extensions, not missing interfaces in the
narrowed milestone. Features remain opt-in/disabled by default. No default config,
model, budget or authority changes are made here. Ordinary answers and simple
read-only lookups still need no campaign, delegation, Python or exec.

- Resource budgeting: hard enforcement for arbitrary Python/OS work and
  resident-memory byte limits, unmanaged workspace/log/native-output disk limits,
  safe operator purge/release, and measured foreground protection. Retained
  artifacts, traces/uploads and managed input snapshots now share authoritative
  root/campaign byte reservations; this is not whole-filesystem enforcement.
  Simultaneous public campaigns
  now share fair resident-process/active-execution/model-call caps; ordinary chat is outside those
  campaign-only queues. Unknown cleanup retains identity-bound resident/execution
  slots, including conservative recovery reconstruction and exact operator release;
  this does not establish physical resource measurement. Native job holds now also survive restart. Existing durable
  execution/billing fences, lifetime/depth, deadlines, per-store artifact bounds
  and global emitted-trace limits are not substitutes for all host resource quotas.
- Native compute's remaining boundary is physical enforcement, not logical GPU
  grants or duration holds. CPU/GPU admission, root duration budgets, exact cleanup
  accounting and restart occupancy are implemented in [COMPUTE](COMPUTE.md).
  Arbitrary Python OS calls remain outside the hook; no GPU passthrough, hard memory
   guarantee or whole-workspace byte admission follows.
- Future allocation/delegation: general git worktree/merge workflows.
  Scoped unused-allowance transfers within an eligible campaign/pool are already
  implemented with ledger revision checks and durable receipts; they create no
  new root funds. Work/Verify/Pause/Stop now have finite
  explicit host-signal adapters, not an autonomous objective generator or a public
  free-form model action authority. Adaptive outcome-based behavior remains resize.
- Complete external-source retention beyond bounded descriptor navigation and
   required durable model-operation evidence. Vercel agent-browser remains the
  selected web interface; an additional HTTP/search tool is intentionally excluded
  from this increment, not a missing implementation requirement.
   Scoped `ctx` list/search now reuses history's five indexed kinds with one bounded
   page per call, host-derived Work/campaign scope and unchanged live defaults.
   The document upload adapter and emitted traces remain bounded; bytes
  discarded upstream cannot be recovered by history.
- Extend context retention beyond the bounded explicit root continuation below:
  arbitrary non-modeled handle inventories,
  and broader child lifecycle/reattach contracts. Host collection now retains a
  bounded stopping snapshot with logical child/group handles and exact produced
  resource refs, atomically linked to execution after staged object persistence.
  Pre-inference snapshots alone no longer authorize continuation. The fresh-process endpoint is not a
  complete production checkpoint. Never replay arbitrary cells or unknown side effects.
  Automated provider reconciliation/orphan identification, backup/restore and
  safe cleanup/retention policy remain incomplete. Local archive/restore markers
  are implemented and do not delete or free storage.
- The production operator integrating writer is implemented in
  [INTEGRATION](INTEGRATION.md): retained multi-file exact patches, root state/version
  checks, advisory locks, before/after snapshots, durable journal and explicit
  identical-plan recovery. It is not a multi-file atomic commit or general merge/git
  worktree framework. Root-only coordinated core Work/attention is also implemented
  without child templates or agents grants. Future workflow acceptance includes
  actual Tachyon-change workflows with reviewable patches and human interventions.
  Command verification still selects one exact candidate (which may be a JSON
  multi-file patch), not an arbitrary multi-artifact bundle.
  Local command/metric contracts and explicit root human
  attestation exist, but do not establish general correctness or broader workflow
  acceptance. Child/group human acceptance is not implemented.

### Finite Limits

- Host defaults: four campaigns, eight resident workers, two active executions,
  two model calls and two native CPU jobs; GPU inventory is empty/disabled.
  Child depth defaults to one, maximum eight, with 31 shared child slots.
  Ghost's loop defaults to 100 iterations; deadlines, request bounds, funds and
  finite repair attempts remain independent fences, not hard OS quotas.
- Retained storage defaults to 64 GiB per campaign and host. Spools default to
  64 MiB/process, 32 MiB/stream and 128 MiB/Work. No automatic eviction or purge.
- Scoped `ctx` filters one page of at most 16 descriptors across five kinds;
  empty filtered pages are not exhaustion. Durable reads are at most 1024 bytes.
- Stopping snapshots cap Work handles, group handles and produced refs at 256 each,
  and serialized bytes at 64 KiB or the lower trace operation limit. Valid
  `final_context` supplies informational final activation; absent metadata falls
  back to stale/unknown. Only host-owned retained output mappings survive.
- Integration caps: 32 existing UTF-8 files, 64 KiB/file and 64 KiB plan/bundle,
  1000 retained journals. No create/delete, force overwrite or automatic rollback;
  changed/partial output requires manual investigation and fresh verification.
- Same-UID trusted native execution is the accepted local boundary. Recovery is
  explicit operator action, never automatic kernel/process restoration or replay.

### Local Context Retention Boundary

Private broker requests optionally carry typed worker activation versions and
known live output IDs, independently of compacted model messages. The host stores
a version-1 `WorkContextSnapshot` before inference with canonical objective,
Work/attempt/generation/instruction revision, pending question IDs, selected
host input refs and allocation availability after boundary reservations. Worker
claims at snapshot capture are informational, not an executable/package attestation;
explicit continuation adds the separate pin/bootstrap checks below.
Snapshot schemas accept no worker budget or
identity overrides. Missing metadata is null, not an empty activation assertion.

Snapshot and model-result objects share existing host trace quotas and exact-hash
retrieval, with 1024-byte pages and no raw database or path interface. Successful
model-result records include request ID, snapshot reference, usage observation,
input message count, tool names and credential/bearer-redacted completion output
up to 1 MiB serialized. Larger output records an explicit retention gap; raw
system/provider instructions and prompt transcripts are not saved. The existing
accounting journal, not these observations, remains billing authority. Interrupted
provider I/O may leave only the pre-inference snapshot and accounting record.

Retained registry reconstruction fails on missing packages, changed versions
or denied permissions rather than silently dropping interfaces. It is wired into
explicit root continuation with host executable pinning and launched-process
bootstrap validation before inference. There is no kernel restore, cell replay,
automatic attempt replay or hot swapping.

Retained process-spool export is now wired into successful Ghost Work completion:
bounded raw pages cross the private authenticated channel before anonymous files
close, with staged quota admission and disk hash/length checks before Ready.
Defaults permit 64 MiB per process (32 MiB per stream), 128 MiB per Work, under
existing shared trace quotas. Descriptors map original live handles and distinguish
discarded bytes, storage failure and incomplete uploads. Errors/cancellation skip
export and record a gap when storage remains available, never successful retention.
The real Ghost fake-provider fixture writes 2.4 MB, exercises live `ctx`, exits,
reopens host storage and reads distinct pages beyond 1 MiB with scope/hash fencing.

Explicit new-attempt reconstruction now supplies the retained checkpoint, selected
resource refs, authoritative questions, direct logical Work handles and current
instructions. Retained output mappings are historical refs, not restored live `ctx`
handles; reading them still requires the original history authority. Only an eligible
`stopped_unverified` root can continue; stale/ambiguous checkpoints, unknown execution
or billing, missing activation metadata and insufficient/closed allocations fail closed.
Ghost captures final stopping observations before tool teardown on normal completion,
loop error and cooperative cancellation; abrupt loss cannot promise them. A complete
arbitrary non-output-handle inventory and general reattach remain out of scope.
Uncapped process output and full model conversations are not retained; bytes
discarded upstream remain unavailable. This closes the retained-spool gap, not
the entire continuation/reattach target.

## Deferred Gates

Feature/local-fixture completion comes first; isolation/security review and release
validation remain separate pending gates, not exclusions that make local work
"all complete." Native execution is not a sandbox. Firecracker/guest transport,
OS enforcement, hostile-process isolation, independent security audit and release
hardening remain deferred under [SECURITY](SECURITY.md). Synthetic policy tests
are not end-to-end scale/foreground-latency benchmarks. Authorized matched-budget
real-model comparisons and representative research measurements remain pending.
No permanent agent or interaction refactor is marked complete by this status.
Separately tracked interaction work follows swarm acceptance: campaign/Conversation
projections, TUI steer/pause/detach/reattach, routing/authorization and follow-up
evidence reuse. Existing attention and ordinary-answer tests do not complete that
refactor. Cross-Research grants/revocation are also separate pending work, not
supplied by campaign-local history. See [TODO](todos/todo.md).

## Evidence

### Current Local Acceptance Gate

Run from repository root with cached Rust dependencies, `rustc`, local linker and
existing IPython. Build the actual fixture binary first; an installed/stale Ghost
or merely compiling ignored tests does not satisfy this gate. With a nondefault
Cargo target directory, set `GHOST_TEST_BIN` to that freshly built absolute path.

```sh
cargo test --workspace --offline --quiet
cargo build -p ghost --bin ghost --offline --quiet
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --offline -- --ignored
git diff --check
```

Revalidated on 2026-09-16: **741 default workspace tests passed**, with 31 opt-in
tests ignored. A fresh Ghost build passed, followed by **all 29 ignored daemon
tests passing with normal parallelism and again serially**. The combined serial
command initially exceeded its outer tool timeout; the complete rerun passed in
168 seconds with a larger command deadline. No test deadline was relaxed.

The earlier two fixture blockers are resolved in the current source:

- Steering cases now use independent stores/attempts. Rewinding only an execution
  row cannot rewind its immutable stopping snapshot; tests retain the snapshot
  assertion and distinguish it from pre-inference diagnostic snapshots.
- The repair/reopen fixture drops its retained ArtifactStore as well as the broker
  and RuntimeStore before reopening. ArtifactStore also holds the runtime database
  authority; retaining it correctly prevents a second database open.

This is local functional acceptance of the exercised bounded contracts, not
production security, isolation, model-quality, or scalability certification. Other
ignored workspace tests are not implicitly covered. No paid provider, installed
daemon or benchmark ran. Earlier gate records below are historical evidence.

The additional ignored Ghost bridge fixture also passed (one test), using the same
fresh binary and existing IPython, fake browser and private fake broker:

```sh
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p ghost --test python_navigation --offline -- --ignored
```

`git diff --check` passed in this reconciliation. No OS credential-store ignored
test was run.

### User Entrypoints

Use matching compiled `tachyon`, `tachyond` and `ghost` binaries and an explicitly
started daemon. Campaign support being compiled does not activate a campaign or
change default config. See [CAMPAIGNS](CAMPAIGNS.md) for the bounded manifest and
operator-selected model, pricing, budgets, permissions, executable and deadline.

```sh
tachyon campaign create "Local check" "Inspect the approved input"
tachyon campaign run /absolute/path/campaign.json --unisolated-development
tachyon campaign status CAMPAIGN
tachyon campaign inspect CAMPAIGN
tachyon campaign attention list CAMPAIGN
tachyon campaign integration-snapshot CAMPAIGN src/example.rs
```

Use the returned ID and exact objective in the manifest. `create` is inert metadata;
only explicit authorized `run` spends. Read-only/simple root manifests need no child
templates. The unisolated flag is required for execution, not read-only inspection.
Operator integration additionally requires a reviewed plan, state token and
`--confirm`; see [INTEGRATION](INTEGRATION.md). Models cannot grant themselves
budget, new permissions, acceptance or integration authority.

### Python And Context Navigation

The local source audit closes the browser/artifact Python adapters and scoped
durable `ctx` list/search, not the broader retention or isolation targets. No
Python implementation, dependency, database, API variant, daemon change or grant
was added. The existing typed history Search already includes all five kinds;
ctx filters one bounded page rather than draining or copying the index.

Passed on 2026-09-16:

```sh
cargo test -p ghost -p tachyon-api -p tachyon-model --offline --quiet
cargo test -p tachyond research_context --offline --quiet
cargo build -p ghost --bin ghost --offline --quiet
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_research_context_queries_retained_attempts_after_reopen --offline -- --ignored
git diff --check
```

New Ghost tests exercise actual IPython with a fake browser executable and private
CPU broker, native artifact hash/pending parity, cached-proxy revocation, lazy
metadata without browser provisioning, all five descriptor kinds via a fake
Resource broker, live/default navigation, Work filtering, unchanged cursors on
empty pages, invalid bounds/kinds/scope and missing history/Resource authority.
The existing redb tests separately cover reopen, bounded indexes, cross-campaign
denials and revoked permits. The existing actual-Ghost reopen fixture exercises
history retrieval, not a new all-kinds scoped-ctx redb fixture. Real-IPython tests
ran without skips. No real network browser/provider, install, UI or commit was used.
One build retry was needed after shared-target disk exhaustion; the reopen gate
passed on retry without deleting shared artifacts.

### Retained Storage Gate

The 2026-09-16 local retained-storage change passed
`cargo test -p tachyon-api -p tachyon-util -p tachyond --offline --quiet`:
336 tests passed, 27 explicitly ignored, no failures. No installed daemon or real
provider was used. The ignored actual-Ghost and credential-store fixtures were
not run in this gate. `git diff --check` also passed.

Rust regressions cover competing real ArtifactStore publication and trace upload
under the same root/campaign caps, unresolved failed uploads after reopen,
source mutation, artifact metadata-commit failure, duplicate reservations,
overflow, actual-byte reconciliation, legacy artifact/trace/snapshot adoption,
snapshot quarantine recopy admission and committed descriptor restore without
freeing debt. Input-document publication exercises the bound adapter without
holding the runtime writer across the ArtifactStore callback.

### Current Entrypoints

The current source contains the following focused checks and end-to-end entrypoints.
These are focused entrypoints; the current full-suite results are recorded above.
The ignored tests launch actual Ghost
against a Rust localhost fake provider, not a paid provider or installed daemon.

```sh
cargo test -p ghost harness::tools::workspace --offline
cargo test -p tachyond verification::tests --offline
cargo test -p tachyond reconciliation_releases_exact_retained_host_slots_once --offline
cargo test -p tachyon-api -p tachyon -p tachyond --offline
cargo build -p ghost --bin ghost --offline
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_stopped_snapshot_continuation_is_fresh_and_idempotent --offline -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_human_acceptance_typed_dispatch_no_pending_model_or_execution --offline -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --offline -- --ignored
```

Source evidence: `campaign_launch/continuation.rs` and its `tests/continuation.rs`
fixture cover fresh Python state without repeated side effects, retained logical
context and idempotent claims. `host_capacity.rs` and `campaign_launch/tests/reconciliation.rs`
cover retained occupancy and exact cleanup release. Workspace `version.rs`,
`read.rs`, `write.rs`, `edit.rs` and `mod.rs` implement the conditional writer/helper.
`verification/tests.rs` covers strict metric parsing and real command stdout/exit
requirements; it is not a separate actual-Ghost metric workflow claim.
`execution/human.rs` and `campaign_launch/tests/human.rs` cover retained root
acceptance and typed dispatch with no pending model or execution permits.
Daemon paths here are relative to `crates/tachyond/src/runtime_store/`, except
`verification/tests.rs`, which is under `crates/tachyond/src/`.

### Earlier Gate Records

The following records describe earlier trees, not current-suite counts or fresh
verification of subsequent continuation/acceptance changes.

Retained-spool gate checks executed locally with no paid provider:

```sh
cargo test -p ghost -p tachyon-model -p tachyond --offline
cargo test -p tachyond spool_upload --offline
cargo build -p ghost --bin ghost --offline
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_spool_retained_after_exit_beyond_model_envelope --offline -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_subprocess --offline -- --ignored
```

The upload checks include a 64 MiB object streamed from a reusable 32 KiB buffer,
expired lease fencing, staged invisibility, corrupt/short/out-of-order/oversized
chunks, campaign/global quota and disk rejection, private-channel revocation,
disconnect/deadline cleanup, and original-handle lookup after Ghost exit/reopen.

Earlier 2026-09-15 baseline, before the subsequent concurrent local edits, executed
successfully from repository root without a paid provider or installed daemon:

```sh
cargo test --workspace --offline
cargo build -p ghost --bin ghost --offline
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --offline -- --ignored
```

The default suite leaves opt-in integrations ignored; the last command explicitly
ran the tachyond local Ghost integrations, including core ask/complete with
IPython, cap-one wait, delivered steering, dynamic children, root/child repair,
immutable publication, reopened history and adaptive resize. Other ignored
workspace tests (such as a disposable OS credential store) are not claimed.
Relevant fixtures live in `crates/ghost/src/harness/`,
`crates/tachyond/src/runtime_store/campaign_launch/tests/`,
`crates/tachyond/src/runtime_store/model_accounting/permits/broker_tests/`, and
the artifact/verification modules. These prove local contracts, not model quality,
full roadmap completion, isolation or production recovery.

The 2026-09-15 concurrent-campaign gate includes the CLI/API/daemon
default tests and all 19 opt-in daemon Ghost integrations against a freshly built
local `GHOST_TEST_BIN`. The new fixture is
`campaign_launch/tests/concurrency.rs`; queue contracts are in `host_capacity.rs`.

The bounded nesting gate adds `campaign_launch/tests/swarm/nested.rs`: actual
localhost Ghost root -> child -> grandchild under running cap one, two suspended
Python calls retaining PID/mutable state, settled results on resume, historical
candidates retained after late steering, and recursive cancellation/drain.
Catalog tests cover default/denied inheritance, depth and total
limits, campaign-wide slot exhaustion, exact command collisions, parent-scoped
observations and restored nested receipts. Group tests cover ancestor-capped
adaptive resize and pause without changing ledger funds. These are local tests,
not model-quality, isolation or production checkpoint evidence.

Integration regression verification on the subsequent 2026-09-15 tree:
`cargo test --workspace --offline --quiet` passed with normal test threads, as did
`cargo build -p ghost --bin ghost --offline`. The eager-guidance bound remains
3000 bytes. Boundary steering now checks tool request/result records separately
from the two context snapshots and two model-result records; that integration
passed in each full parallel run.

The catalog fixture now holds the parent's HTTP response until child verification
settles: releasing every response together allowed normal recursive parent cleanup
to cancel children before their verification allocations were created. Its latest
parallel integration run passed without changing production capacity limits.
The repair fixture no longer treats an unsettled deferred scheduler outcome as
terminal or relies on a 100 ms sleep. Review claims now check cancellation under
the same database writer as verifier funding. A cancelled queued verifier becomes
Unverified without an evaluation reservation or callback; unknown provider holds
still prevent settlement. Tests cover cancellation before runner start and during
deferred review, unchanged unknown holds, and later authoritative reconciliation.

Fresh Ghost spool export also exposed a lifecycle regression: a nonzero worker
exit after successful output parsing and confirmed kill/reap was treated as unknown
execution, stranding its running lease. That known-cleanup path now returns no
candidate and records Unverified, independently of unresolved billing. Failed
export never publishes a candidate or grants repair authority. Dynamic-read fixtures
assert that terminal failure explicitly; no-output repair fixtures still assert
pending repair under unknown usage. Neither path refunds unknown charges.

At that retained-spool checkpoint, the workspace default suite and opt-in daemon Ghost integrations
pass with normal test threads and a freshly built local `GHOST_TEST_BIN`, including
nesting, child repair, unknown-usage cleanup and durable spool reads after exit.
Spool snapshot ownership survives tool teardown; export precedes candidate
publication while private permit authority is still live. Barrier tests prove that
cancelled uploads and wrong reply frames cannot feed subsequent channel calls.
No production capacity, storage quota, funding fence or version check was weakened.

The shared active-execution increment was checked with a freshly built local Ghost
and the opt-in daemon integrations. The concurrent-campaign fixture now also
holds two real roots behind an HTTP barrier with resident/model caps two and
execution cap one, checks no unknown execution for the queued root, then verifies
cleanup returns capacity. The nested fixture runs under host execution cap one.
Default tests cover independent pools, queue cancellation and retained unknown
capacity; the wait barrier test holds the shared slot in another campaign and
requires reacquisition and renewed authority validation before a normal reply,
including revocation while queued. That workspace default suite and the
opt-in daemon integrations passed. This does not establish native
CPU-job overlap limits, GPU denial-before-spawn, storage quotas or tool accounting.

The subsequent CPU-job-only review fixes cancellation of in-flight acquire and
confirmed-release RPCs: the bounded host session may finish the frame after the
supervisor stops waiting, and a late grant with no spawned process is released.
Busy polling returns the socket mutex between attempts. Historical releases bypass
current grant locking/revocation and replay idempotently only within the issuing
private Work session. Random session-scoped monotonic lease IDs avoid an unbounded
released-ID set; the active map is bounded by `max_cpu_jobs`. Disconnect drops that
map but deliberately forgets outstanding semaphore permits, retaining physical
capacity for the host runtime lifetime. Supervisor drop or SIGKILL delivery alone
still does not prove cleanup. There is no automatic orphan reconciliation.

Verified locally with no paid model, installation or commit:

```sh
cargo test -p ghost -p tachyon-model -p tachyond --offline --quiet
cargo build -p ghost --bin ghost --offline --quiet
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_simultaneous_campaigns_share_capacity_and_cancel_independently --offline -- --ignored
```

The concurrency fixture now additionally runs two actual Ghost campaigns with
worker/model caps two and native CPU cap one, observes only one command spawned,
then confirms both complete and return capacity. Focused tests cover running and
queued supervisor cancellation, late grant/release barriers, exact duplicate and
foreign releases, bounded lease bookkeeping, failed spawn, failed spool writes,
and real IPython async start/cancel/wait without sharing its kernel wire protocol
with CPU frames. Child-call contexts and browser runners inherit the host service;
direct local policy behavior and arbitrary Python OS bypass remain unchanged.
That CPU-only checkpoint established a native job-count primitive. The subsequent
[COMPUTE](COMPUTE.md) gate adds logical GPU admission, root duration holds and
restart-safe unresolved native occupancy. Measured CPU utilization, process/device
containment, memory/storage quotas and foreground protection remain outside it.
