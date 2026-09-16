# Local Allocation Policies

`crates/tachyond/src/allocation_policy.rs` retains the pure proposal interface.
The local `HostScheduler` has adaptive resize and opt-in explicit Work/Verify/
Pause/Stop/Reallocate signals in `runtime_store/groups/allocation.rs` and the existing catalog
adapter. Ordinary Ghost chat and manifests without
`allocation` remain fixed. No installation, remote setup, planner agent, extra
model invocation or separate scheduling loop is involved.

## Host Selection

The strict campaign manifest accepts optional
`"allocation":{"mode":"deterministic","max_running":2}`. Both fields are
required when present; unknown fields/modes are rejected. The cap is 1..64 and
cannot exceed `children.max_running`. It also clamps each template's durable host
ceiling, never enlarges it. No budgets are added. Modes:

- `fixed`: existing dispatch and explicitly selected group caps, no policy ticks.
- `model_proposed`: the existing host-authorized `agents.group_resize` route is
  the baseline. It requires the GroupResize control and expected group revision;
  model output is not authority. No automatic model calls or planner are added.
- `deterministic`: adapt existing admitted explicit-template and dynamic proposal
  groups, including nested groups.
  A group can start below its template ceiling using `agents.group`'s existing
  `max_running` argument. A settled Accepted outcome suggests +1 if funded queued
  work and additional capacity can use it; Rejected suggests -1 down to one.
  Unknown outcomes never justify growth. A host cap can also force shrinkage.

Each settled Work outcome is consumed at most once. Useful observations blocked
by uncertainty are deferred, not repeatedly applied. Manual resize (including
zero) durably relinquishes policy control for that group. Accepted steering
conservatively relinquishes all existing campaign groups, including when steering
their logical parent. Restart/reselection does not clear that relinquishment.
There is currently no automatic-policy re-enable command for a relinquished group.

`validate_resize` still rejects non-resize proposals; the other actions use exact
finite host signals below. Work selects an already approved fixed spawn/group key;
Verify selects existing deferred evidence and its original protected verifier.
Neither action invents objectives, evaluator configuration or budgets. Root
concurrency is not adaptively controlled. Reallocate moves existing unused Work
allowance only; there is no work/verification pool transfer or new money.

## Explicit Controls

`allowed_actions` defaults to `["resize"]`; the supported names are `resize`,
`work`, `verify`, `pause`, `stop`, and `reallocate`. An empty list disables automatic actions. `signals` defaults
to empty and is limited to 32 exact host requests, only in deterministic mode:

```json
{"allocation":{"mode":"deterministic","max_running":2,
  "allowed_actions":["resize","pause","stop"],
  "signals":[{"command_id":"stop-branch-1","group_id":"approved-group",
    "expected_revision":1,
    "action":{"kind":"stop","work_id":"existing-child","generation":1}}]}}
```

Use `"action":{"kind":"pause"}` for a group pause. These are immediate explicit
signals once that exact approved group exists, not outcome predicates or model
inferences. A Rejected scientific outcome never produces Stop. Unknown action
names/fields, duplicate command IDs/actions, missing allowlist entries and invalid
identity/generation/revision bounds are rejected.

Each tick selects at most one new signal across the campaign, in manifest order,
before resize/dispatch; a tick selecting a signal performs no adaptive resizes.
Completed receipts are skipped. There is no growing pending queue. Signals for
unavailable/unapproved groups have no effects. A stale
revision on an available group fails closed. The writer binds the exact campaign,
group, controller, steering epoch, expected group revision and Stop generation.
Pause/Stop application and an exact command payload receipt commit atomically.
Work and Verify transaction boundaries are described below.
Receipts are group-scoped and bounded to 32 per group; identical tick replay has
no effects, including after reopen, and changed payloads conflict.

Pause sets only the selected group's running cap to zero and relinquishes its
automatic controller; existing executions drain. Stop requests cancellation of
the selected existing Work and its enrolled logical descendants, all of which
must remain within that group's ancestry scope. It does not stop the root, cancel
siblings, pause the whole campaign or prove graceful completion. It reuses durable
work cancellation and the scheduler's existing live-handle polling/cleanup path.
Queued Work can be reconciled as unspent; active/unknown holds and leases remain
until the existing owner proves cleanup/accounting. Unlaunched separate verifier
holds are not newly reclaimed by this adapter. Manual resize/accepted steering
still relinquishes automation durably; a configured signal cannot undo it.

### Reallocate Existing Allowance

```json
{"allocation":{"mode":"deterministic","max_running":2,
  "allowed_actions":["reallocate"],
  "signals":[{"command_id":"move-unused-1","group_id":"approved-group",
    "expected_revision":3,
    "action":{"kind":"reallocate","source_work_id":"existing-a",
      "source_generation":1,"target_work_id":"existing-b","target_generation":1,
      "tokens":1000,"cost_micro_usd":0,"expected_ledger_revision":12}}]}}
```

Revision numbers must match current host-owned state; they are not automatically
refreshed on failure. Both allocations must already be open, in this campaign's
Work pool, with registered, nonterminal, uncancelled Work inside the approved
controlled group subtree. Both generations, controller/steering authority and
group revision are checked under the writer. Paused branches and ledger debt/pause
deny transfers. Unrelated Work and protected verification funds cannot participate.

This exact signal explicitly authorizes increasing the target's allowance above
its original grant by the specified amount. It does not raise its per-request
model limits or the root envelope. Only available funds move; tokens and microUSD
are independent, and a single nonzero dimension is valid. Unknown/provisional
request holds and settled charges remain attached to their original allocation.
Original grants and received/sent counters remain durable for accounting.

Transfer, ledger receipt, group revision and policy receipt commit atomically.
Even a late group CAS failure rolls back the transfer. Exact repeated signals,
including after reopen/closure, never move funds twice; changed payloads conflict.
No model tool, public worker grant, automatic transfer proposal, allocation reopen,
debt forgiveness, root top-up or configurable extra per-allocation cap is added.
The finite signal amount and available source funds are the host-approved limit.

### Work Admission

For an existing approved controlled group, an explicit Work action is:

```json
{"command_id":"admit-next","group_id":"existing-group","expected_revision":1,
 "action":{"kind":"work","template_id":"approved-next",
   "parent_work_id":"existing-parent","generation":1,"instruction_revision":1}}
```

Enable `work` in `allowed_actions`. The template must be a fixed key approved by
this scheduler, not dynamic profile slots. Its exact parent, candidate identities,
objectives, Work/verification allowances, paths, model and evaluator remain host
descriptors. No objective/context/cap/budget override is accepted. The named parent
must be inside the controlled group subtree or an enrolled ancestor of all its
explicit members. The template's immutable parent relation must match; generation
and latest accepted instructions are rechecked. This permits the root parent and
nested enrolled controllers, not unrelated or cross-campaign actors. An existing
controlled group is required; this is not automatic root/group bootstrapping.

The trusted mapper calls the same catalog admission implementation as agent
Spawn/Group. It does not manufacture a model permit: host selection authorizes the
mapper, while the parent's existing funding/liveness and all admission limits still
apply. One writer checks owner/controller epoch and expected group revision, then
commits admissions, protected verifier reservations, logical parent/group relations,
catalog receipt and policy receipt/revision together. A late receipt/funding/scope
failure rolls back all of them. This uses only remaining authorized root envelope,
never reallocates another Work's allowance or borrows verification money.

Descriptor publication follows commit under the catalog lock. If publication is
interrupted, exact signal replay restores descriptors from the catalog receipt;
it cannot create a missing receipt or reserve again. Explicit reapproval after
reopen must match the original catalog fingerprint. New descriptors dispatch on
the next tick through the ordinary funding/claim/launch path.

### Verify Handoff

Enable `verify` and select an existing deferred execution:

```json
{"command_id":"verify-existing","group_id":"existing-group","expected_revision":2,
 "action":{"kind":"verify","work_id":"existing-child",
   "generation":1,"instruction_revision":1}}
```

The execution must already be `AwaitingVerification` with known collected candidate
evidence, the exact approved descriptor, current instructions and an eligible
existing verifier hold/open allocation. `EvidenceReady` is not silently promoted by
the signal; unknown review/execution, accepted/settled results, closed/insufficient
funding, debt, cancelled work, or an existing current evaluation reservation fail
closed. Existing host-recorded command repair attempts may match their immutable
original catalog policy; no attempt or evaluator configuration is replaced.

Verify has an explicit **two-phase durable handoff**, not atomic intent-plus-spend:

1. The signal writer validates the fence, descriptor, evidence and funding, and
   records one exact pending execution snapshot per group. It changes no allowance,
   group revision, claim or completion receipt. A Work cannot have two such intents
   through overlapping groups. A pending review suppresses adaptive resize.
2. The scheduler uses its existing approved execution/evaluator handle. Immediately
   before review, the existing claim writer rechecks the exact snapshot, controller,
   group revision and latest instructions. The verifier claim/funding, evaluation
   reservation, `ReviewingUnknown` transition, policy receipt/revision and removal
   of pending intent commit together. Capacity waiting leaves only intent; a failed
   claim rolls back funding/counters/receipt. No primary process is relaunched.

Reopen retains intent without running anything. Explicit identical host reapproval
and reselection can rebind it to a new owner if the evidence/fence remain eligible.
Manual takeover invalidates pending authority; stale intent is retained but does
not spawn failing tasks every tick. Cancellation can still take the ordinary
unverified-settlement path. A lost evaluator after the claim retains unknown
review/billing and the committed receipt; neither intent replay nor later ticks
invoke it again. Normal deferred verification outside these selected signals keeps
its previous behavior; `allowed_actions` is not an evaluator-disable switch.

## Pure Interface

`AllocationPolicy::propose` still returns Work, Verify, Resize, Pause and Stop
suggestions for fixed, model-proposed and deterministic implementations. These
suggestions grant no money, authority, process cleanup or correctness evidence.
Its admission-oriented validator checks explicit queued identities and allowances,
remaining budget, lifetime admission slots, physical capacity, action limits and
all fence fields. No objective, verification job, model or budget is generated.
The host retains the complete admission specifications.

The new `propose_resize`/`validate_resize` entry points handle already-admitted
queues without emitting or silently dropping admission actions. They preserve
the original interface's tests and semantics, but do not pretend that existing
admission holds are fresh root headroom. Pure limits remain 4096 queue records,
64 recent outcomes, 128 active executions, 32 actions and 256 bytes per identity.
Pure callers must advance their own outcome windows; the host adapter persists
its consumed outcomes. A stale fence is never revived by steering.

## Transactions And Resources

The adapter runs after reap and before dispatch in the existing Tokio scheduler
tick. One serialized writer projects the group, ledger and resource state,
validates the proposal, and uses the same group resize mutation as manual CAS.
The fence contains campaign/group identity, group revision, a distinct group
controller ID and persistent control epoch. Explicit host selection replaces the
durable campaign owner token: previous schedulers cannot apply further policy
ticks, even after restart. This is a fenced ownership token, not a timed lease or
automatic process recovery. No transaction or authority lock crosses an await.

Resize queue entries must already belong to the exact approved template and be Admitted,
uncancelled and nonterminal. Their allowances come from existing unconverted
ledger holds. Resize never reserves, converts or releases those holds.
Resource headroom is the minimum of root execution slots, scheduler resident
slots and ledger inference headroom; nested groups additionally use every
ancestor's remaining running capacity and cap. The target obeys own/profile or
template/root caps. An ancestor pause blocks dispatch and adaptation without
permanently pausing the descendant's own policy.
An in-flight owned request keeps its full hold and occupied slot. Live cancellation
handles include the command-loop root, also while waiting. Unowned unknown
execution, unknown review, unresolved allocated billing without a live owner,
cancellation and accounting debt cannot justify expansion. Dispatch still performs
its own authoritative admission/funding/claim checks. Shrink only changes future
claims and drains active executions; it does not evict or pretend to free slots.

Per tick the adapter accepts at most 64 groups, 32 explicit members per group,
64 live execution handles and one resize per group (64 actions total). Dynamic
groups are discovered from durable admissions and approved host descriptors, not
from invented worker queues. Profile slots are global per campaign, not per parent;
resizing creates no slots or new allowances. The durable
root scan is bounded by existing WorkLimits (4096 internally, 256 in manifests).
It decodes bounded whole records, not scalable indexed snapshots; ledger scans
retain the existing ledger's finite lifetime bounds. Synthetic 1000-record pure
policy projections are not claims that a local manifest admits 1000 workers.

## Verification

The integration test uses actual Ghost processes, approved group admission,
the scheduler, a localhost fake model and host callback verification. It compares
fixed/model-proposed with deterministic resize, asserts fixed Work count, tests
unknown billing/debt suppression, and spends no API credits:

```sh
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond allocation_real_host_group_pipeline -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond policy_actions -- --include-ignored
cargo test -p tachyond allocation -- --nocapture
cargo test -p tachyon-api campaign::tests
```

Regular tests cover stale ownership/revision/control epochs, restart metadata,
manual relinquishment, unchanged ledger reservations and shrink-drain semantics.
Execution-action tests cover atomic catalog rollback and publication recovery,
parent/nested scope, instruction/generation fences, pending Verify reopen, exact
descriptor checks, claim rollback and no unknown-review replay. The new real-Ghost
tests prove exact Work signal dispatch, Verify over genuinely deferred collected
evidence without another model call, capacity waiting without new reservations,
and Stop cleanup during a stalled fake provider request, retaining unresolved
billing rather than claiming settlement.
Synthetic tests compare 1/8/32/128/1000 logical records with a separate cap of eight
and print local elapsed time. These are correctness checks, not real swarm
throughput or cost benchmarks. No wall-clock latency guarantee is claimed.
