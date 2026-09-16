# Durable Campaign Groups

CURRENT: bounded, host-internal scheduling primitives in Tachyond's existing
campaign admission, ledger, and execution store. A group is a scheduling container,
not an agent, policy, funding grant, or execution permission. A trusted Rust host
scheduler now drives approved policies through real private Ghost launches.
Scoped `agents` cancel/group status/resize and exact approved-template spawn/group
admission are available through the private broker and generated Python proxy.
No public IPC group command, arbitrary objective generation, or TUI route exists.

## Host Contract

An already authorized host first creates the campaign's existing money/inference
envelope, then calls `host_configure_work_limits(campaign, WorkLimits::default())`
before admitting any Work. Configuration is immutable and replayable with the
same payload. It creates no money. Defaults are 16 total logical Work identities,
one nested group level, three active executions, and 16 resident leases across the campaign. Host
configuration is hard-bounded to 4096 Work, eight nested levels, and a positive
root concurrency no greater than total Work.

This is explicit opt-in, not a new authorization protocol. Groups require the
configuration. Existing non-group campaigns retain their previous admission
contract; campaigns with existing Work cannot be retrofitted or have their work
count reset. Once configured, **all** campaign admissions, including direct and
verification Work, consume the same lifetime total and execution capacity. Terminal
or cancelled Work does not refund the lifetime count. Model attempts do not add
logical Work.

| Host Method | Contract |
|---|---|
| `create_campaign_group(GroupSpec)` | Explicit campaign-scoped group ID, optional same-campaign parent group, positive `max_running`, nonempty immutable vector of complete admission specs. Root groups have depth zero. |
| `resize_campaign_group(campaign, id, expected_revision, max_running)` | Compare-and-set revision; changes concurrency only. Zero pauses claims. No new Work, new money, extension of deadlines, or cancellation of running Work. |
| `campaign_group_status(campaign, id)` | Consistent bounded subtree snapshot, sorted by Work ID, with dispatch state plus active, terminal-acknowledged and cancellation-requested flags; includes active descendant count. |
| `list_campaign_groups(campaign, after, limit)` | At most 64 groups, sorted by immutable group ID. Exclusive `after` is the last returned ID. Zero limit returns nothing. |
| `cancel_campaign_group(campaign, id, expected_revision)` | Atomically cancel queued descendant Work; persist cancellation intent for claimed descendants. No automatic process signal or cleanup claim. |
| `host_acknowledge_work_terminal(admission)` | Trusted exact-identity acknowledgement that execution and its effects have stopped; releases execution capacity without changing billing. Never call based on a timeout or worker report alone. |
| `campaign_work_status(campaign, work)` | Read current membership, occupancy, wait specification and wait revision, including direct Work; resolves lost suspend/resume replies. |
| `host_suspend_parent(identity, expected_revision, work_ids, mode, deadline_ms)` | Host-only durable suspension of registered, active, funded Work with no unknown/provisional request holds. Releases execution capacity, not lifetime membership or money. |
| `host_poll_parent_wait(identity, revision, resume)` | Bounded immediate snapshot with completed/outstanding references, ready/resumed/resource-blocked flags and configured resident capacity. Optional CAS resume reacquires root and every ancestor slot. |

Creation stores the exact immutable payload as its collision-free replay
fingerprint. Identical creation returns the current group, including subsequent
resize/cancellation state; a different payload with the same scoped ID conflicts.
Input order is part of the payload. A Work belongs to at most one scheduling
group. Existing direct or grouped Work cannot be adopted into another group.
Group identity, membership, Work admissions, pending entries, root lifetime count,
ledger reservations, and ledger receipts commit in one serialized redb write
transaction. Insufficient allowance or any other error rolls everything back.
Child groups reserve from the existing campaign ledger, not an independent purse.

Resize and cancellation use strict expected revisions; stale revisions conflict,
including retries after a successful mutation. Read status to resolve an uncertain
reply. Group membership is immutable, and empty groups cannot inflate hierarchy.
Listing is a current-state keyset page, not a multi-page snapshot: newly inserted
IDs before the cursor require a fresh enumeration. Status is bounded by the
configured lifetime Work limit, not a log stream.

## Dispatch And Completion

The existing `dispatch_campaign_batch` claim path now atomically checks and
claims root and ancestor group capacity before committing `DispatchingUnknown`.
A blocked group does not stop scanning for eligible Work in other groups or
campaigns. Descendants share every ancestor cap, so nesting never multiplies
concurrency. Shrinking below current occupancy drains naturally; no replacement
claim occurs until occupancy falls below the new limit.

`DispatchingUnknown` and unfinished `Registered` both hold capacity across store
reopen. Registration, cancellation intent, elapsed time, final billing, and client
disconnect never release it. Queued cancellation and trusted `ConfirmedUnspent`
dispatch outcomes release capacity atomically with the admission transition.
Claimed cancellation remains visible until authoritative cleanup acknowledgement;
new funding/model-boundary checks deny cancelled or terminal-acknowledged Work.

The internal `execute_campaign` path is connected, not a separate mock scheduler:
authoritative process evidence collection commits the execution transition and
primary Work terminal acknowledgement together for callback executions. Command-bound
Work instead releases running capacity while remaining nonterminal through review,
bounded repair and unresolved billing. Completed deterministic evaluation
does the same for verification Work. If no usable candidate exists after confirmed
process termination, verification is not run and its slot is released too.
Lost spawn/process/review outcomes remain unknown and retain their respective
slots. Terminal execution acknowledgement and final allocation settlement are
separate: unknown provider usage continues holding money after a process slot is
released. This is not a claim that a candidate is correct.

A candidate event is not exit evidence. The private launcher observes leader exit,
signals its process group before reaping the leader, and refuses successful evidence
collection if that signal fails (an already absent group is harmless). This is not
a process-tree sandbox: escaped process groups and external effects need separate
host cleanup evidence; sending SIGKILL does not synchronously prove every descendant
has exited. Recovery acknowledgements must account for those limits.

Review eligibility is rechecked in the same transaction as the review claim and
reservation, including on resume. Cancelled or terminal-acknowledged verification
Work cannot start a new evaluator. A denied review remains EvidenceReady (or its existing awaiting phase) for host
resolution; cancellation alone does not release an active verifier's slot.

The runner accepts an already admitted, not-yet-registered verification Work as
well as the existing registered form. Prefer the admitted form: its exclusive
execution ownership prevents generic dispatch, and registration/funding/occupancy
are claimed atomically only when review can start. Primary execution can therefore
run at root capacity one. Evidence collection releases its slot; if another Work
takes it first, review persists `AwaitingVerification` without invoking a callback
or creating a review hold. The authorized scheduler must call `execute_campaign`
again with the exact original policy after capacity becomes available. The policy
snapshot stays immutable even when its verifier's stored registration changes.
The opt-in host scheduler performs this retry. No scheduler is installed in normal
daemon startup. `ReviewingUnknown` is never replayed.

## Runnable Host Adapter

`runtime_store::scheduler::{HostScheduler, HostExecution}` is callable inside the
Rust daemon on Tokio. The caller must already have authority for every exact Work,
verification admission, model/pricing/budget policy, evaluator ID, executable,
workspace and home. Create/enroll explicit group Work first, then supply a catalog
of `HostExecution { policy, executable, workspace, home, evaluate }`. The evaluator
is `Evaluator::Command` with an exact command/artifact/staging binding, or
`Evaluator::Callback` with an `Arc` callback returning a boxed Send future with
`Evaluation`. Callbacks must be cooperative and non-spending. No evaluator may
construct a new budget root. The [campaign CLI manifest](CAMPAIGNS.md) uses command
bindings for both root and children, not callback reviewers.
Each command child uses the shared bounded command loop with its exact host config;
callbacks still use the one-shot runner. Child/profile repair limits are optional in
the manifest and default to one attempt. No root policy is added to the child catalog.

```rust,ignore
let broker = Arc::new(ModelBroker::new(store.clone(), approved_model)
    .with_controls([Control::Spawn, Control::Group, Control::Status, Control::Cancel,
                    Control::GroupStatus, Control::GroupResize]));
let mut scheduler = HostScheduler::new(broker, approved_catalog, 2)?;
// Optional exact one-shot templates, bound to an enrolled logical parent:
scheduler.approve(approved_child_template)?;
let active_tasks = scheduler.tick().await?;
let last_outcome = scheduler.outcome(campaign_id, work_id, generation)?;
// Alternatively drive ticks until a watch receiver signals shutdown:
scheduler.run(stop_receiver, Duration::from_millis(25)).await?;
// For manual ticks, explicitly drain before dropping the host:
scheduler.shutdown().await?;
```

The initial catalog has 0..256 entries; local task capacity is 1..64, additionally bounded
by durable root/ancestor claims. Non-catalog pending Work is skipped before any
claim, including standalone verification entries. Each claim retains the existing
64-entry rotating scan. A zero-task tick is not queue exhaustion. Deferred review
is polled before new launches, without replaying the primary. This is not weighted
fair scheduling or a wall-clock database latency guarantee.

The child-only campaign adapter starts with an empty catalog and explicitly
reserves one resident slot for the separately owned root command loop. Approved
templates populate child policies after transactional admission. Dropping the
owner withdraws its in-memory template authority; durable receipts remain for
exact host re-approval, never automatic process replay.

While a command child awaits review, final billing or repair capacity, its scheduler
owner remains bounded by the task/resident cap, but the reaped attempt releases
durable running occupancy. The logical child stays nonterminal so parent waits
cannot finish on the first rejected candidate. Review and every replacement attempt
must claim ordinary root/ancestor capacity again. A zero group cap pauses repairs;
shrinking drains without a replacement launch, and cancellation stops continuation.
The host Work deadline is fixed across polling and attempts. A finalized command
with unknown billing is not launched again; accounting-only reconciliation remains
possible. Shutdown of a known completed review acknowledges cleanup separately from
unresolved billing, preserving charge holds. Unknown execution/review is never
treated as cleanup proof.

One scheduler owns its launch/review JoinHandles keyed by campaign/Work/generation.
Its shared broker retains cancellation senders only, not scheduler/task ownership,
so there is no reference cycle. `outcome` reports the last durably reaped task result
or error, not necessarily the latest accounting snapshot; `campaign_execution`
remains authoritative. `None` is pending/unobserved, not success. Callback panics
leave `ReviewingUnknown` plus a durable task error after the next tick. Recovery
requires the original approved policy and never launches an existing unknown
registration/execution or replays an unknown reviewer.

Cancel persists intent first, then signals a scheduler-owned task. Group cancellation
intent is picked up on the next tick. The launcher drops active broker/provider I/O,
revokes its permit, sends group KILL, and reaps the leader within the existing cleanup
bound. Only successful cleanup returns empty candidate evidence and acknowledges
terminal Work as Unverified; cleanup failure remains unknown and retains capacity.
Unknown billing is not refunded. This retains the process-group containment limits
above, not proof of termination of escaped descendants. Explicit shutdown drains
and persists outcomes. Drop only signals cleanup as a runtime-live fallback; abrupt
runtime death cannot guarantee reaping or a diagnostic outcome beyond durable
execution/dispatch uncertainty.

### Lazy Approved Catalog

`HostTemplate` contains a key, exact `WorkAddress` parent, 1..32 `HostCandidate`s,
optional host group ID, and positive host `max_running` ceiling. Each candidate
contains complete admission and verification specs, WorkRequest, model policy,
evaluator ID/callback and host-only launch paths. `HostScheduler::approve` checks
identity/policy consistency and catalog collisions without reserving money or
creating Work. A non-group template must contain exactly one candidate. The host
must publish its approved key/meaning in the parent's fixed instructions.

The initial scheduler catalog plus approved candidate count is bounded to 256.
Approval bounds objective strings to 32 KiB, IDs/model labels to 256 bytes, and
UTF-8 launch paths/provider endpoints to 4 KiB each.
Templates never evict to reset that bound. The private Rust broker admits a selected
template only for its exact current permitted parent, atomically with logical
parent mappings, verification reservations and durable replay receipts. Root
WorkLimits, logical parent depth, protected budgets, and group/root execution caps
remain shared; no new budget root or automatic objective is created. Admission can
return queued handles even when the parent occupies the last running slot.

Public dynamic nesting is opt-in via `children.max_depth` (default one, maximum
eight) and each source profile's explicit `profile_ids` inheritance allowlist.
All levels share one campaign-lifetime slot pool and root envelope. A proposed
group's scheduling parent is the delegating Work's existing group, or the root
when that Work is ungrouped. A single proposed child inherits its parent's group
without creating a group. Work parent links remain a separate immutable,
server-derived relation: group membership does not grant sibling or transitive
status/result/message access. Group controls retain their existing requirement
that every affected Work be a direct child of the actor.

Claims and wait resumption obey all ancestor caps as well as the root cap. Waiting
releases running capacity, not resident state or lifetime allowance. Deterministic
allocation discovers admitted dynamic groups from the durable group registry and
approved execution descriptors, including nested groups. Its resource projection
includes ancestor occupied capacity; it cannot bypass an ancestor pause, unknown
billing or the ordinary claim checks. Cancelling or completing a logical parent
cancels descendants recursively even when they share a group with unrelated Work;
unrelated siblings are not cancelled by that operation.

Committed descriptors are published under one shared host lock and imported by the
next scheduler tick. No manual child register is required. On restart the host must
re-supply exact templates/evaluator semantics; approval checks the durable policy
fingerprint and restores admitted descriptors without a worker replay. Unknown
dispatch/execution/review remains fenced. The receipt contains only stable child
handles for the worker, never host paths or full policies. See [AGENTS](AGENTS.md)
for exact request/replay semantics and the future objective/context mapper boundary.

Catalog-created groups persist a separate worker resize ceiling, so a low initial
`max_running` can later grow only up to the host ceiling and root cap. Older direct
host-created groups retain their existing root-bounded resize behavior. There is
no CLI approval bypass or ordinary daemon activation.

## Parent Waits

The private broker now enforces the host precondition at a parked tool control
boundary: hold permit authority, reject unknown/provisional inference, commit the
inactive execution lease, and fence the permit. Native/Python `agents.wait` retains
the original process and kernel and returns only after successful reacquisition.
Other direct host callers must still stop/fence the parent themselves. Killing and
relaunching is not resume. See [LIFECYCLE](LIFECYCLE.md) for the exact bounded
protocol, failure/cancellation handling, configured residency, and remaining gaps.

Waits contain 1..64 unique existing Work IDs in the same campaign, never self or
cyclic dependencies. Modes are `All`, `Any`, and `Count(n)`. Deadlines are absolute
host wall-clock milliseconds, at most five minutes from suspension. Polling never
sleeps or waits on an actor; it returns partial completed/outstanding sets. Ready
means the threshold, deadline, or a detected resource block was reached, not that
the parent owns a slot. Only `resumed == true` authorizes host resumption. Debt,
root occupancy, ancestor caps, cancellation, and terminal state are rechecked.
Failed capacity reacquisition preserves the durable wait and revision. Cancellation
does not claim process cleanup. Deadline completion does not cancel outstanding
children or extend their deadlines.

Child admission must happen before suspension. Failed admission (total count,
depth, allowance, inference capacity) is an explicit host resource-blocked outcome,
not a missing reference to wait on; the adapter must return that error immediately.
Paused/cancelled dependency groups and debt are reported as resource-blocked during
polling. Unknown effects may remain outstanding until the bounded deadline.
Waits retain lifetime membership, resident leases, and all accounting.
`WorkLimits.max_resident` separately bounds logical residency, not memory/CPU.
Full resident capacity with queued waited work denies suspension. The scheduler's
task cap also includes parked residents and must allow at least parent plus child.

## Boundaries And Next Work

- No public execution grant until host authorization and scope checks are ready.
- Scoped controls, exact catalog child admission, and the host scheduler are
  implemented. Arbitrary objective/context mapping and ordinary daemon activation remain gated.
- Each claim now scans at most 64 pending entries with a durable rotating key cursor;
  batches launch at most 32. Zero claims can mean a blocked page, not queue exhaustion.
  Schedule another bounded tick. This is key rotation, not weighted campaign fairness
  or a wall-clock bound on per-campaign decoding. Callback duration remains host-owned.
- Resident wait/permit handoff and host-bounded command child repair are implemented.
  Crash process recovery and independent memory/CPU/GPU/storage limits remain.
- Campaign interaction/UI work remains separate from the native host scheduler.

The real multiworker acceptance test uses a freshly built Ghost and only localhost
HTTP fixtures: two overlapping launches, three explicit Works, root cap two,
shrink/drain, active cancellation without false billing release, lost evaluator
error persistence, and restart without replay. Run:

```sh
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond scheduler_real_parallel_shrink_cancel_and_lost_review -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_catalog_spawn_and_group_admission -- --ignored
```

## Verification

Group tests cover concurrent replay and claim caps, shared nested/root capacity,
depth and total limits, direct verification non-bypass, concurrent membership
conflicts, atomic insufficient-funds
rollback, immutable replay, exclusive pagination, resize conflicts/draining,
queued/active cancellation, final billing without terminal acknowledgement,
unknown-slot retention, reopen, and unrelated campaign progress.
Parent tests additionally cover capacity-one handoff, durable wait/revision recovery,
all/any/count modes, bounded deadline partial results, paused ancestor resume,
concurrent cancellation, unknown inference rejection and debt without new spend.
Verifier tests cover capacity-one deferred review, restart before/after the review
claim, no duplicate evaluator, and a callback that acquires the database writer.
The pending cursor test proves a blocked 64-entry page advances across reopen.
Catalog tests additionally cover lazy approval, concurrent command replay, unknown
and cross-parent keys, both budget pools, logical depth/total rollback, a queued
child under root cap one, and exact re-approval recovery. The real Ghost parent asks
for spawn and group in separate runs, replays admission, receives durable handles
while child HTTP replies are held open, and observes children launched automatically
in parallel through the same campaign ledger and exact per-Work allocations.

```sh
cargo test -p tachyond -p tachyon-api
cargo check --workspace
cargo build -p ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_execution_review_and_settlement -- --ignored --nocapture
```

The explicit spawned Ghost test now uses root execution capacity one and localhost fake HTTP, including accepted,
rejected, unverified, stalled and malformed outcomes with grouped primary Work.
It does not contact a real provider, install software, or restart the daemon.
