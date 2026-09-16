# Campaign Admission Gate

## Current Boundary

This is a host-only Rust admission and internal execution boundary. The existing
daemon opens the new tables in `runtime.redb`; no IPC handler, model tool,
scheduler, task recovery path, or production worker dispatcher calls admission.
Registration dispatch callbacks remain host supplied; the opt-in trusted catalog
[host scheduler](GROUPS.md#runnable-host-adapter) claims existing admitted Work and
drives real Ghost execution after registration. It never admits new Work. Campaign metadata
stays `Draft`, and existing jobs neither acquire campaign grants nor change behavior.

The host must authorize the exact campaign/work payload before calling
`RuntimeStore::admit_campaign_work`. A previously host-authorized immutable ledger
envelope is required. Neither this method nor the envelope method authenticates a
caller: these are internal trust-boundary preconditions, not capabilities granted
by the local IPC connection. No public fund-grant or admission request was added.

## Atomic Admission

`Admission` carries campaign scope, work ID, objective, instruction revision,
generation, pool, and an integer token/micro-USD upper bound supplied by the host.
Revisions and generations start at one. The work ID is also the admission command
ID, globally unique within this admission store, and the stable Work identity for
admitted model funding. Model retries and new execution Attempt IDs do not create
new objectives or admissions. An existing admission cannot be revised or moved to
another campaign; its dispatch ID is not a per-model-request receipt. Relaunch of
an uncertain dispatch is still unsupported, not permission to create another Work.

One redb write transaction reserves against the authoritative root, writes durable
work with an immutable generated dispatch/reservation ID, and inserts the pending
outbox index. The ledger mutation helper does not open or commit a nested
transaction. Any failure or dropped transaction rolls back all three writes and
the ledger receipt. Concurrent identical requests return one identity and hold;
changed payloads conflict. Admission replay returns the current work lifecycle,
not its original snapshot, and never re-enqueues it.

The additive tables are `campaign_admitted_work` (versioned records) and
`campaign_dispatch_pending` (work-ID index), plus `campaign_dispatch_cursor` for
bounded rotating claims. This index is the durable queue;
reopening the store needs no in-memory queue reconstruction or scan of all work.

## Dispatch And Recovery

- `Admitted`: committed work is queued, but no external launch has been attempted.
- `DispatchingUnknown`: a serialized claim removed the pending entry and committed before calling the dispatcher. This also represents a crash before launch, a lost acknowledgement, or an uncertain launch result. There is no lease expiry or automatic replay.
- `Registered { worker_id }`: a trusted dispatcher/reconciler confirmed registration for the exact immutable campaign, work payload, generation, and dispatch ID. This is durable acknowledgement, not proof of completion or usage reconciliation.
- `ConfirmedUnspent`: trusted evidence establishes no execution/spend; the reservation is finalized at exact zero in the same transaction as the state transition.
- `Cancelled`: cancellation won before claim, so the launch was impossible and the hold can safely finalize at zero. Cancellation after claim is rejected rather than guessed unspent.

Claim revalidates the current reservation's pool, upper bound, unresolved usage,
cancellation flag, and campaign debt pause under the same serialized writer.
Missing reservations or identity mismatches fail closed with an error. Entries
blocked by cancellation, known usage, or a campaign debt pause remain pending for
host resolution but are skipped so they cannot starve unrelated campaigns.
Standalone ledger mutations are trusted operations and must not be used to release
a dispatching/registered work hold without authoritative usage evidence.

`dispatch_campaign_batch` claims one item at a time, at most 32 per call (or the
requested smaller limit). It holds no database transaction or registry lock across
the supplied dispatcher. Each claim inspects at most 64 pending entries, rotating
after its last inspected key and wrapping once. Cursor progress commits even when
no claim succeeds and survives restart. A zero batch is not proof of an empty queue;
the scheduler must schedule another bounded tick. Callback duration and per-root
decoding cost are not wall-clock bounded. This is not weighted campaign fairness.
Execution-owned queued verifiers are skipped by generic dispatch.
Concurrent consumers cannot claim the same item. Explicit reconciliation uses the
full persisted identity, rejects stale generations and cross-campaign identities,
and treats identical acknowledgements as no-ops; conflicting terminal outcomes fail.
An uncertain claim stays discoverable through `admitted_work(campaign, work_id)`.
There is no production reconciliation scanner or authenticated evidence protocol yet.

## Execution Blocker

The original admission boundary below still applies to ordinary daemon dispatch.
An explicit host-only end-to-end runner is now available; see Internal Execution.

The opt-in shared model boundary and daemon-owned request reservation adapter now
exist; see [MODEL_ACCOUNTING.md](MODEL_ACCOUNTING.md). The private funded storage
helper validates a registered admission and atomically
converts its hold into finite request funding. Request records have separate
stable Work and execution Attempt fields and fresh per-provider-request receipts.
The internal `PermitAccounting` route requires an opaque host-issued capability
before each reservation/dispatch claim. No production caller selects this route or
connects the outbox to Ghost.
Admission funding is not unlimited model authorization: the host must separately
authorize each exact request policy and attempt, and requests cannot borrow from
another pool or from root headroom beyond the admitted allocation.

`host_issue_model_permit` binds the exact request identity, policy/pricing limits,
purpose, and registered funding. Work/admission/request IDs are not credentials.
Same-admission Attempt/policy replacement invalidates the old permit, and explicit
revocation denies new requests without rejecting historical billing. Replacements
and retries retain the original allocation. Reservation idempotence is separate
from durable at-most-one dispatch claims; a replayed request ID cannot trigger
another provider execution through the internal adapter. A claim that wins before
revocation may still execute, so revocation never implies zero usage.

Permits are in-memory, host-minted random opaque Rust values with redacted Debug
and no serialization. Restart invalidates them, not durable holds or dispatch
claims. No credential transport or broad IPC grant was added, and no provider
secrets or database handles are sent to workers. This is not OS isolation or local
IPC peer authentication. The current one-policy-per-Work grant and immutable
admission generation/revision deliberately do not implement generation-advancing
relaunch. The typed private local worker channel now exists; lifecycle/funding
migration and OS containment remain release gates for broader execution.

Work completion uses explicit trusted `CloseAllocation` after every child request
has final evidence. Unknown/provisional holds block closure rather than freeing
uncertain spend; cancellation does not close an allocation. Request finalization
refunds unused bounds to the allocation, and closure refunds its remainder to the
root exactly once. The dispatch registration record remains registration evidence;
the internal runner stores completion separately in `campaign_executions`.
Transferred admission holds cannot be independently
finalized at zero by the legacy dispatch reconciliation path.

The ordinary daemon spawn/recovery lifecycle does not validate campaign reservation
context. Wiring a normal spawn callback would therefore bypass inference accounting
and falsely imply budget enforcement. The explicit private-broker runner is the
budgeted route; no adapter into ordinary scheduling is installed.

Before enabling production execution, the host needs an authorization policy for
exact work payloads and a defensible upper-bound estimator/pricing policy. Every
worker inference and retry must validate its campaign/reservation/generation,
reserve before calling the provider, and return authoritative usage to the ledger.
Multi-inference worker jobs cannot consume this single-attempt hold as unlimited
permission. Trusted worker registration/unknown-outcome recovery also needs an
identity/evidence protocol; a broad local IPC connection is not that protocol.

Admission alone enforces holds, not actual spending by ordinary existing workers.
See [BUDGETS.md](BUDGETS.md) for accounting and overrun semantics. It adds no UI,
prompt content, logs, model calls, automatic retries, or transition to Running.

## Verification

Daemon tests cover concurrent admission and dispatch, payload/scope/generation
conflicts, rollback after all admission writes, absence before commit, unauthorized
and over-budget rejection, cancellation before launch, restart recovery of pending
work versus non-replay of persisted claims (including callback panic after a fake
external effect), registered acknowledgements, confirmed
zero versus unknown failed launches, bounded slices, and store writes inside the
fake dispatcher (no writer held across the callback), cancel/claim races, blocked
queue isolation, and conflicting outcomes preserving spent and final-zero usage.
These admission tests are store/fake tests. Separate execution tests launch a freshly
built Ghost with localhost fake HTTP and deterministic host evaluators; no external
provider is used.

## Internal Execution

`ModelBroker::execute_campaign` in `runtime_store/execution.rs` is callable by
trusted host Rust code after explicit authorization and registration using the
existing admission dispatch/reconciliation methods. It does not create grants,
admit arbitrary jobs, register normal daemon tasks, or scan/activate Draft campaigns.
Neither `ApiRequest` nor model tools can reach it. The ordinary daemon registry and
background review helper are unchanged; routing through that helper would bypass
the protected verification contract.

The host supplies an admitted (preferred) or registered verification Work reservation with a distinct ID,
same campaign and `Pool::Verification`, plus a stable deterministic evaluator ID.
That allocation is exclusively bound to the execution. It does not change the
original model Work's one-policy grant, objective, generation, or Attempt ID. Both
allocations come from existing admitted holds, never new root allowance. The
base evaluator is a host-supplied non-spending async callback. The separate
[command wrapper and bounded repair coordinator](VERIFICATION.md) run a trusted
native command, not an LLM reviewer. A full verification child bound is held before evaluation;
only host-observed completion/cancellation of this non-spending callback permits
final zero model usage. A future LLM reviewer must reserve broker requests against
separate verification funding, not call the existing provider reviewer directly.

`campaign_executions` persists versioned records with immutable policy, bounded candidate, phase and
settled bit. `campaign_execution_verifiers` prevents sharing verification funding.
`RuntimeStore::campaign_execution(campaign, work)` queries current state:

- `ExecutingUnknown`: durable claim before launch. Spawn failure, malformed transport, unconfirmed cancellation and lost acknowledgement remain here, with no false success or automatic replay. Scheduler cancellation with successful host cleanup instead records Unverified evidence; unknown billing remains held.
- `EvidenceReady`: one exact completed candidate is persisted, but not yet accepted.
- `AwaitingVerification`: evidence is durable, but queued verification cannot yet claim execution capacity or campaign debt pauses admission. No evaluator was invoked. Retry the same policy on a later scheduler tick, never relaunch the primary.
- `ReviewingUnknown`: review claim and protected hold committed before callback. A crash or cancelled caller cannot replay the evaluator.
- `Reviewed(Accepted | Rejected | Unverified)`: configured evaluator decision, or `Unverified` for absent/invalid terminal candidates. Review timeout is `Unverified`, never acceptance. Rejection creates no rework clone or new generation.
- `settled`: both allocations closed atomically with this bit, only when every child hold in both pools has final accounting. Review completion alone cannot release unknown/provisional model spend.

Acceptance means only acceptance by the configured evaluator, not correctness.
Candidate evidence is limited to 128 KiB serialized and must be a single completed
result matching Work ID, objective, generation and assignment. Multiple candidates,
even identical ones, are conflicting evidence. Collected event envelopes must match
the registered worker actor, session and task, with no conversation or parent task.
These checks bind untrusted output to an assignment; they do not authenticate its
claims or make artifact references safe to open or execute.

The explicit `execute_campaign_command_loop` additionally fences optional typed
Attempt identity in the request, tool events and result. Replacement attempts keep
the immutable admission generation and dispatch allocation, advance the assignment
ordinal, and get fresh process/verification claim keys. An atomic transition archives
the prior execution before launch; it requires known termination, final billing,
remaining Work and protected verification funds, current applied steering, capacity
and host attempt/deadline bounds. Neither an Attempt ID nor a new permit grants
money. The original one-shot APIs and dispatch-keyed launch claims remain unchanged.

Launch and evaluation use the earlier of the caller deadline and the original Work
deadline; resume cannot extend it. Evaluators must be cooperative async, non-spending
host code whose work stops when its future is dropped. Neither synchronous callback
construction nor blocking polls can be preempted by Tokio's timeout. A late return
is classified Unverified, not accepted. Caller cancellation after the review claim
but before completion persists leaves unknown verification usage, including across
restart; it neither frees the hold nor permits another evaluator invocation.

Admission `Registered` remains immutable registration evidence; execution completion
is queried in this companion lifecycle, not inferred from that admission state.
Model accounting uses only host broker/provider evidence. No callback accepts token
or billing metrics. Unused bounds return to the root once; settled replay returns
the persisted original Work/generation and does not launch or evaluate again.

Explicit resume with the same policy can claim persisted `EvidenceReady` or
`AwaitingVerification`, or finish
accounting for `Reviewed`; unknown effects are never rerun. Trusted recovery helpers
`host_collect_campaign_evidence` and `host_finish_campaign_evaluation` require host
proof of original process termination/complete output or the configured evaluator's
result respectively. They are not worker-report APIs. Late authoritative provider
reconciliation can unblock `settle_campaign_execution` without replaying work or
review. No automatic recovery scanner or late-provider evidence transport exists.

All launch/review awaits are outside database transactions and registry/authority
locks. Serialized claims and compare-and-set transitions fence concurrent callers.
An admitted verifier is exclusively owned before primary launch and claims its
registered state, root/ancestor slot, allocation and review hold together after
primary evidence. A capacity-one root no longer requires simultaneous registrations.
If no candidate exists, the unlaunched verifier is cancelled at confirmed zero.
Parent suspension/resume is separately host-only and requires a stopped parent and
revoked permits; see [GROUPS.md](GROUPS.md). It does not provide resident-resource
capacity or a model-facing pause protocol.
Public scheduling, authorization UI/API, normal registry projection, supervisor
orphan cleanup, public evaluator configuration, and independent
resource/sandbox controls remain future work. See [BROKER.md](BROKER.md) for the
fresh-build `GHOST_TEST_BIN` test commands and containment limitations.
