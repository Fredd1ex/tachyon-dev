# Campaign Ledger Foundation

The daemon owns campaign accounting in its existing `runtime.redb`. This document
describes the ledger foundation; its explicit public execution integration is
documented in [CAMPAIGNS](CAMPAIGNS.md). Envelope authorization alone does not
launch work. Ordinary dispatch is not made budget-enforced by the ledger itself.

The [admission gate](ADMISSION.md) now composes a reservation, durable campaign
work, and pending dispatch index in the **same redb transaction**. Its bounded
dispatch engine has fake tests; the explicit campaign runner also has real local
Ghost integration tests. Claimed-but-uncertain launches retain their holds and are
never automatically replayed. Confirmed pre-dispatch cancellation or authoritative
zero-spend evidence can release a hold atomically with its dispatch outcome.

## Internal Rust Surface

- `RuntimeStore::host_authorize_campaign_envelope(command_id, campaign_id, envelope)` creates exactly one immutable root for an existing campaign. The caller must already have host authorization. The method is deliberately not wired to any IPC handler; it does not itself implement an authorization protocol.
- `RuntimeStore::campaign_ledger_command(command_id, campaign_id, command)` accepts typed `Reserve`, `Reconcile`, `Cancel`, `FundAllocation`, `ReserveAllocated`, `TransferAvailable`, and `CloseAllocation` commands. These remain trusted internal operations, not authorization endpoints.
- `RuntimeStore::campaign_ledger(campaign_id)` reads the latest root, or `None` when no ledger exists.
- `Ledger::committed(pool)` returns spent actuals plus unresolved holds plus available open allocations. `allocation_available(id)` returns the exact remaining funded bounds. `active_inferences()` counts unresolved reservations, excluding converted allocation parents, not observed running processes.

`Envelope`, `Units`, `Pool`, `Usage`, `Reservation`, `LedgerCommand`, `Ledger`, and
`Totals` are crate-internal Rust records in `runtime_store::campaign_ledger`.
They are not public API wire types.

## Exact Invariants

- Units are integer aggregate inference tokens and micro USD (one millionth of a US dollar). No floating-point estimates, pricing conversion, or implicit exchange rates occur in the store.
- The envelope has disjoint work and protected verification allowances. Neither pool can borrow the other's remainder. Their sums must fit `u64` in each dimension. Every unresolved request or unconverted admission holds one slot from the shared `max_active_inferences` limit, including a zero-unit reservation. Converted allocation parents hold funds, not inference slots.
- A serialized redb write transaction checks campaign existence, command replay, limits, root mutation, and receipt insertion atomically. Successful admissions cannot exceed either pool dimension or the active-inference limit. Actual usage may exceed estimates and is not rejected merely for exceeding the envelope.
- Each provider request, including retries, needs a fresh reservation ID against the same root or its explicit admitted allocation. Stable Work identity does not change on retry; execution Attempt IDs and per-request receipts are separate. There is no top-up, envelope replacement, nested allocation, or resume operation.
- Unknown usage holds the entire estimate. Provisional usage is a cumulative lower bound and holds the componentwise maximum of estimate and known usage. Final usage consumes actuals and releases only the unused estimate and active slot. Reports are cumulative, never additive deltas; known usage cannot decrease and final usage is immutable.
- `Cancel` records intent only. It never releases unresolved allowance or active slots. Confirmed unspent work must be explicitly reconciled with `Usage::Final(Units::default())`; that cannot override already known positive usage.
- Usage above an individual reservation records the excess as debt even if other allowance remains. Debt is summed separately from committed usage, not charged twice. Any debt permanently pauses new admissions to both pools in this increment; reconciliation and cancellation remain available. No evidence is erased to make totals fit.
- Individual units are `u64`; checked aggregate accounting uses `u128` so actuals beyond the grant's representable range remain recordable. Debt uses nonnegative componentwise differences. No aggregate arithmetic wraps.
- Successful command IDs are global within the ledger receipt table. Identical replay returns the original snapshot without mutation, including after reopen; changes to campaign, operation, or payload conflict. Failed commands leave no receipt and may be retried. Read the root separately for its current state.
- Unknown/provisional usage, cancellation intent, final actuals, debt, pause state, and receipts survive database reopen. Recovery never treats unresolved work as unspent.

## Admitted Funding

`FundAllocation { reservation_id, work_id }` converts an unresolved, uncancelled
registered admission hold in place, checking its persisted Work scope. Arbitrary
root-funded request holds cannot be converted and closed to erase uncertainty. The
original bounds and pool remain immutable. `ReserveAllocated` uses only its
available bounds, not additional root headroom; work and verification cannot cross
fund each other. Conversion, child reservation, and daemon request record commit
together in the explicit admitted adapter. Persisted legacy roots and requests
default to no allocations/root funding; existing ordinary callers are unchanged.

In each integer dimension, a child's covered amount is its reserved bound while
unresolved, or `min(final, reserved)` after settlement. Available allocation is its
original bound plus received transfers minus sent transfers minus the sum of covered children. Actual above a child's bound is
debt, not another grant: while open, settled actuals + unresolved componentwise
max(reserved, provisional) + available = original allocation + net transfers + child overrun debt.
The root counts these terms once, never the original admission hold as well.
Debt is also reported separately and pauses all new admissions/requests, including
requests inside otherwise available allocations. It is not added again to charges.

Final reconciliation returns only unused child bounds to the open allocation.
`CloseAllocation` rejects any unresolved child; after all children are final it
sets available funding to zero, returning precisely that remainder to the root.
Repeated closure/final receipts cannot refund twice. A transferred parent cannot
be independently reconciled or cancelled. Unknown/provisional usage and cancellation
intent survive restart; no timeout, uncertain Work completion, or failed request
speculatively releases funding. Closing without requests requires explicit trusted
conversion and closure; neither operation is production-wired.

## Reallocating Available Funds

`TransferAvailable { source_allocation_id, target_allocation_id, amounts,
expected_revision }` moves only unused funds between distinct open allocations in
the same campaign and pool. Tokens and microUSD are independent checked integers;
either dimension may be zero, but not both. Original admission holds remain
immutable. Durable received/sent totals determine each allocation's new allowance;
pool-wide received and sent totals must match. No root envelope or request estimate
is changed. Unknown/provisional holds, settled usage, and debt never move.

The ledger revision advances on state changes, not identical receipts or no-op
closure/reconciliation. A stale revision, insufficient available funds, closed or
invalid allocation, debt/pause, arithmetic overflow or cross-pool transfer fails
without mutation or receipt. Exact receipt replay returns its original snapshot,
even after closure; a new transfer after closure fails. Closing refunds only the
adjusted unused remainder, once. Cumulative per-allocation transfer counters are
checked `u64`; counter exhaustion fails closed.

The host policy adapter additionally restricts both endpoints to registered,
nonterminal, uncancelled Work in the controlled group subtree, checks both
generations and group/controller fences, and rejects paused branches. It does not
transfer verification allowances. An exact finite manifest signal is explicit host
authority to raise the target above its original allowance, not a model grant.
There is no separate configurable per-allocation ceiling: the exact authorized
amount, source availability and immutable pool envelope bound the increase.
Per-request model/pricing limits remain host-approved; request admission uses the
adjusted available allocation. See [POLICIES](POLICIES.md) for configuration.

## Storage Limits

Separate shared host semaphores now bound resident Ghost processes, active Ghost
execution and broker model calls across campaigns in one runtime store. They do
not consume token/microUSD allowance. Wait/ask retains resident capacity while
releasing execution, then reacquires execution before replying. These are logical
concurrency permits, not RSS or disk quotas. A separate `max_cpu_jobs` ceiling
(default 2) gates broker native exec, Rust-backed Python exec and the browser
command runner until confirmed cleanup. Ordinary local exec and arbitrary Python
OS calls remain ungated. Explicit GPU grants, root integer job-duration envelopes
and durable native cleanup holds are described in [COMPUTE](COMPUTE.md). These are
logical controls, not hard CPU/device enforcement. Aggregate campaign/workspace
storage admission remains separate work. Existing artifact and trace limits are separate stores;
there is no unified storage accounting snapshot. Do not infer a root disk grant
from a model budget or treat conservative job-wall-time measurements as exact CPU billing.

Two ledger-specific additive, versioned JSON-record tables (`campaign_ledger_roots` and
`campaign_ledger_receipts`) use the existing runtime database schema. No separate
database or campaign metadata migration is introduced. Reservations are stored in
the root record and receipts retain original result snapshots; this favors a small,
auditable transaction over throughput or storage efficiency. There is no pruning,
event journal, external usage deduplication protocol, or large-campaign scaling
claim yet.

This ledger only covers tokens, micro USD, and reserved active inference counts.
It does not enforce wall time, tool calls, storage, processes, or the full proposed
resource envelope. It cannot verify usage evidence, identify a retry assigned to a
different campaign by an untrusted caller, or authenticate verification work.
Trusted future execution integration must supply those identities and evidence,
obtain host authorization, reserve before dispatch, and reconcile after dispatch.
The internal admission gate now provides the atomic reserve-before-dispatch store
boundary, but does not supply host authentication, pricing, or inference enforcement.
No existing execution path is budget-enforced by this foundation.

Provisional reports must be genuine lower bounds, not revisable estimates. Final
corrections, debt remediation, host authorization IPC design, execution readiness,
and launch transitions are intentionally deferred.
