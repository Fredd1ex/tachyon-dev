# Model Request Accounting

## Current Boundary

`tachyon-model::Model::chat_accounted` is an opt-in production-library entry point
for one HTTP provider attempt. It uses the same completion/tool streaming code as
ordinary chat, with `AccountingContext` supplied separately from the provider body.
`chat` and `chat_requiring_tool` still pass no accounting context. Ghost's local
`Model` adapter continues to use ordinary chat. A new private `BrokerClient`
`AgentModel` adapter connects the same `run_loop` to accounted daemon execution
over a preconnected Unix socketpair. This production-compiled path is tested with
real Ghost tools, localhost HTTP, and `runtime.redb`; subprocess launch is not
integrated and ordinary workers do not opt in. See [BROKER.md](BROKER.md).

The shared client and Ghost loop have no application-level model retry loop.
Repeated calls to `chat_accounted`, including retries, reserve separately. Accounted
HTTP requests disable reqwest retries and redirects, pin OpenRouter `provider.only`,
set `provider.allow_fallbacks: false`, and require parameter support with
`provider.require_parameters: true`. This fences each client HTTP attempt, not invisible
executions/retries inside a remote provider. Provider routing/billing guarantees
must be verified before production authorization; the client cannot authenticate
or observe those hidden attempts.

## Host Contract

`RequestReservation` contains two independent typed records:

- `WorkIdentity`: campaign ID, stable objective/work ID, execution attempt ID, generation, instruction revision, and request class. A provider retry does not invent a new objective. A new host execution attempt changes `attempt_id`, not `work_id`.
- `RequestEstimate`: exact endpoint/model/provider and pricing revision, maximum complete serialized request bytes, input token upper bound, enforced output token cap, input/output prices in integer microUSD per million tokens, and an additional microUSD bound for other charges.

These bounds are explicit host configuration/attestation, not live pricing or a
tokenizer implementation. The host must establish that the input bound covers
every permitted body within the byte bound, including message framing and tools,
and that pricing covers reasoning, cache reads/writes, and other provider charges.
Unknown pricing must block host authorization, not be encoded as a guessed zero.
Explicit zero prices are possible for a genuinely free, host-authorized request.
The library checks endpoint/model/output cap equality and actual serialized body
length. Accounted requests serialize the authorized output cap as OpenRouter
`max_tokens` (including reasoning), not `max_completion_tokens`. It does not use
`fit_context`'s bytes/4 heuristic as an accounting estimate.

Reservation tokens are input bound plus output cap. Reservation cost is the ceiling
of the combined input/output rate products divided by one million, plus the other
charge bound. Arithmetic is checked; overflow denies the request. These are upper
bounds, not predictions of exact spend. Host policy remains responsible for their
validity and for trusting the configured endpoint's OpenRouter billing semantics.

`RequestClass::Work` and `Compaction` consume the work pool; `Verification` consumes
the protected verification pool. Classification must be host-authorized. The enum
does not introduce model compaction or verifier execution. Current deterministic
history compaction does not make a model request and receives no model charge.

## Reserve And Reconcile

`RequestAccounting` uses object-safe boxed `Send` futures, matching existing Rust
async conventions without adding an executor or third-party dependency. Its
`reserve` must atomically commit an unresolved hold and exclusive dispatch claim
on every successful invocation. A pre-reserved hold may be claimed once; returning
the same receipt on a repeated dispatch claim is forbidden. Only then is the
provider future constructed and polled.
Denial or accounting failure prevents dispatch. There is no retry of reservation,
request, or reconciliation hidden in the contract.

The private `runtime_store::model_accounting::DaemonAccounting` storage helper uses
the existing ledger. It accepts only its exact host-authorized
identity and estimate. A redb transaction writes the ledger hold and versioned
`campaign_model_requests` record together, keyed by a fresh `model:<UUID>` receipt.
The additive table is created on first internal reservation. Reconciliation checks
the complete persisted scope, updates the ledger and evidence atomically, and
rejects conflicting final outcomes. No transaction/lock spans an await or provider
call. The synchronous storage helpers remain ready futures for internal callers;
the broker's `PermittedAccounting` adapter runs reservation and reconciliation in
`tokio::task::spawn_blocking`, never on the async executor.

The private `AdmittedAccounting` test adapter uses that same implementation and
request table. Its root-funded counterpart is also private; neither is an exposed
daemon authorization route. The funded helper
requires a persisted registered admission matching campaign, stable `work_id`,
generation, revision, and pool; `attempt_id` remains independently host-authorized.
Its first reservation atomically converts the admission hold into an allocation,
reserves inside it, and persists the scoped request receipt. No root headroom is
needed for this transfer, and failure rolls it back. Subsequent requests and retries
consume the same finite allocation, never another objective or a fresh grant.
Receipt reconciliation checks both complete request policy and funding scope.

## Internal Permits

The daemon's internal `host_issue_model_permit` API now mints an opaque
`ModelPermit` using the existing UUID v4 random source, without new dependencies.
Only the trusted host chooses policy and funding. A worker-supplied campaign,
admission, attempt, receipt, or request ID cannot construct a capability. The
permit's nonce field is private to its module, with no byte/string accessor,
Clone, Serialize, Deserialize, or Display implementation. Debug prints only
`ModelPermit([REDACTED])`. This is a Rust in-process authority boundary, not an OS
sandbox or authentication of a local socket peer.

The store owns the in-memory authority map. A grant binds exact campaign, stable
Work, Attempt, generation, instruction revision, purpose, endpoint, provider,
model, pricing revision, all request bounds/rates, and the persisted registered
admission/funding allocation. Even smaller/different request bounds require host
reauthorization. There is one current exact policy per Work in this increment:
replacement requires its current permit and invalidates the previous permit
atomically. Revocation is idempotent; stale replacement cannot overwrite a newer
permit. Multiple simultaneous purpose/model policies per assignment are not yet
supported. Issuance/replacement creates no ledger grant or allowance.

`model_permit_accounting` requires a permit and a bounded logical request ID.
The returned `PermitAccounting` revalidates the permit on **every** reservation
or dispatch claim, under the same authority lock as replacement/revocation. The
funding check, ledger hold, request evidence, and dispatch record commit under one
redb writer. A request can only consume its original finite allocation; changing
Attempt IDs or permits cannot reset it or borrow root/verifier headroom.

`reserve_only` is idempotent reservation, explicitly **not** permission to execute.
The `RequestAccounting::reserve` implementation atomically claims dispatch once,
including when claiming a previously reserved hold. Durable
`campaign_model_dispatches` records key request IDs by funding allocation, bind
the complete request policy, and retain the claim across restart. Reusing an ID
with another policy conflicts. Replaying a claimed ID fails before provider
construction, rather than returning an idempotent receipt and executing again.
Reserved-but-unclaimed requests cannot be claimed by a replacement permit; their
separate persisted owner ID is evidence, not a credential. Existing holds are
rechecked for cancellation, settled usage, closed funding, and campaign pause
before claim. Genuine retries need distinct request IDs and consume the same
remaining allowance.

The claim is the authorization linearization point. Revocation cannot cancel a
claim that already won, prevent its later socket write, prove zero spend, or stop
remote execution. A crash/drop after claim, even before HTTP starts, retains an
unknown hold and never authorizes replay. This is at-most-one local dispatch claim,
not exactly-once remote provider execution. No locks cross provider I/O or await.

Revoked/replaced permits can reconcile their exact historical request ID, receipt,
policy, and funding scope. Final evidence remains idempotent and conflicting final
usage fails; refunds do not revive authorization. Reopening the store invalidates
all in-memory permits. A fresh explicit host authorization of the same historical
policy can reconcile durable evidence, but cannot replay its claimed or historical
unclaimed requests. There is no automatic credential recovery, expiry, or scanner.
Grant history is retained in memory until the store closes; GC/retention is still
a production decision.

No permit nonce is stored in request/dispatch evidence, provider JSON, prompts,
environment variables, or logs. A separate random, non-authorizing owner ID is
persisted in dispatch evidence. The separate socketpair handshake capability never
exports the permit. No public IPC method, provider secret delivery, raw database
access, or existing IPC grant was added. The private Ghost adapter is connected in
tests, not worker launch; ordinary Ghost model calls remain outside this boundary.

Admission generation/revision are still immutable. Permit issuance rejects a
different generation/revision rather than inventing new funding or treating a
replacement as a relaunch. Same-admission Attempt/policy replacement and stale
permit fencing are implemented; advancing an assignment generation while retaining
its original allocation needs a host lifecycle/migration design. Historical billing
is deliberately independent of new-request liveness, but no generation-advance
worker protocol is claimed here.

## Internal Broker

The callable Rust API is
`runtime_store::model_accounting::{ModelBroker, ModelBrokerRequest, ModelPermit}`:

```rust
let broker = ModelBroker::new(store.clone(), host_selected_model);
let completion = broker.execute(ModelBrokerRequest {
    permit: &permit,
    request_id: "unique-logical-request-id",
    reservation: exact_host_authorized_reservation,
    messages: &messages,
    tools: None,
    streamed_argument: None,
    deadline: host_deadline, // tokio::time::Instant, mandatory and absolute
}, &mut on_delta).await?;
```

This is a trusted in-process API, not a worker RPC. The host constructs the model
(including credentials), issues the exact-policy permit against registered admitted
funding, and chooses the deadline. Workers are not handed a `Model`, provider key,
store, or broad fund grant. The broker keeps both model and store private. Request
IDs are bounded to 256 bytes and deduplicated within the original Work allocation.
Both ordinary chat and required-tool streaming (`tools` plus `streamed_argument`)
go through the same `chat_accounted` invocation and mandatory deadline. Exact
identity/policy and fresh funding eligibility are revalidated at dispatch claim,
not just when the broker or permit is constructed. Byte/output/routing checks and
integer token/cost bounds are the existing model contract above, not a new estimate.

The deadline covers reservation, HTTP/SSE, and reconciliation. An already-expired
call cannot reserve or send HTTP. Deadline expiry drops the provider future; caller
drop does the same, with no detached provider task. Storage commits cannot be
cancelled once running: a reservation finishing after cancellation retains its
unknown hold and durable claim but cannot start HTTP. Already-observed final billing
may still commit if cancellation races reconciliation; otherwise the hold remains
unknown. Timeout is not proof of zero spend, nor does it promise unknown state if
final reconciliation already started. The host's synchronous delta callback must
remain short and nonblocking, as with other cooperative async APIs.

There is no additional async semaphore or scheduling queue in this increment.
The existing atomic ledger enforces finite allocation and active-inference limits,
including unresolved holds. The private transport bounds frames, counts, output,
and lifetime, with one outstanding request per host-created pair. Host launch must
still bound the number of assignments/channels and blocking-pool pressure. No mutex
or transaction crosses network I/O. No assignment lifecycle, worker startup,
automatic retry, allocation closure, or provider credential delivery is implied.

## Usage And Closure

`LedgerCommand::CloseAllocation` is an explicit trusted Work-completion operation,
not an inference result. It rejects outstanding unknown/provisional requests and
returns only the unused allocation to its original root pool. Request finalization
alone refunds to the still-open allocation. Closing is irreversible and idempotent;
it does not update dispatch registration or authorize another worker. Cancellation,
restart, and uncertain completion do not close allocations automatically. See
[BUDGETS.md](BUDGETS.md) for exact conservation and overrun treatment.

Billing evidence is separate from `Completion::usage`, whose legacy display
counters default to zero. Final accounting requires a complete `[DONE]` stream,
valid integer prompt and completion counts with a matching total, and inclusive
OpenRouter `usage.cost`. The inclusive cost accounts for cache charges rather than
silently repricing all prompt tokens as uncached. BYOK usage is not accepted as
inclusive billing evidence. Missing/malformed cost, inconsistent counts, stream
errors, and EOF without `[DONE]` remain `Unknown`. Malformed or conflicting usage
fails the request; later usage cannot overwrite that evidence and release the hold.
Decimal cost parsing uses serde_json's existing `arbitrary_precision` feature,
supports scientific notation, and rounds up to microUSD without an f64 conversion.
Overflow and unsupported representations remain unknown; exponent handling does
not allocate exponent-sized buffers.

Errors/timeouts and dropped futures preserve the unresolved hold and inference
slot, including cancellation after a response but before reconciliation completes.
The model library has no built-in request timeout; the daemon broker now requires
and enforces a deadline for every invocation. Direct library callers must still
enforce their own deadline. A
successful response without billing evidence can return its content
but does not release budget. Reconciliation failure returns an accounting error;
it does not authorize replay. Final evidence records actual input plus output tokens
and inclusive cost, releasing only unused bounds. Existing ledger overrun debt and
sticky admission pause still apply. No automatic expiry, zero-on-error refund,
provider bill lookup, or background reconciliation scanner is implemented.

## Release Gates

Campaigns remain disabled. Before enabling them, all of the following are required:

1. Connect the implemented private socketpair transport to host assignment lifecycle and subprocess bootstrap. Preserve isolated scoped authority, not prompts, environment variables, logs, broad existing IPC grants, or raw database access. Decide generation/revision migration, multi-policy grants, retention, and restart reauthorization. A local socket connection alone is not authorization.
2. Authorize the explicit admitted-funding choice and trusted allocation closure in the scoped transport. The internal atomic transfer is implemented; a raw admission ID is not request authorization. Existing stored admission IDs remain unchanged and serve as stable Work IDs, not Attempt or request IDs.
3. Carry the accounting context through every launched worker inference, retry, model-based compaction, and verifier request. The generic Ghost loop now supports the private accounted adapter, with real-model/real-redb localhost tests; selection at worker launch remains gated. Partial SSE, timeout, lost replies, redirects, retries, stale policy, and disconnect retain their accounting fences.
4. Validate conservative token/routing/pricing bounds and inclusive billing semantics against the supported provider contract, including caches, reasoning, BYOK exclusions, hidden upstream retries, and rate changes. Add authoritative late-usage recovery for unknown holds and failed reconciliation.
5. Implement trusted worker registration, lifecycle/recovery fencing, and admission dispatch integration before any transition to Running. No such startup or transition is installed here.

## Verification Scope

Model tests use the same reserve/dispatch/reconcile function as the HTTP boundary
with fake provider futures: denied requests never construct the provider, retries
create separate holds, timeout errors and dropped futures remain unresolved, and
the provider can acquire the fake store lock. Localhost fake HTTP/SSE tests also
exercise `chat_accounted` with a fake accountant: serialized `max_tokens`, pinned
routing with fallback disabled and required parameter support, usage inclusion,
valid billing, malformed/conflicting usage, EOF without `[DONE]`, a caller-enforced
deadline on a stalled request, and dropping after the response before reconciliation.
Unresolved holds block further reservation. Parser tests cover inclusive cache
cost, actual prompt/output counts, exact scientific notation, fractional-microUSD
rounding, malformed/missing usage, and arithmetic overflow. No external model
calls are made. Model-library timeout tests cover caller cancellation; daemon
broker tests additionally cover its mandatory deadline wrapper.

Permit tests cover missing/random-forged/cross-store credentials, mutations of every
identity/purpose/provider/pricing/bounds field, forged funding, unsupported generation
changes, stale replacement, revocation, late final usage and conflicts, shared retry
allowance, reservation idempotence versus concurrent exclusive dispatch claims,
restart invalidation and durable replay denial, cancellation before claim, and a
revoke/claim race. A revoked permit is tested through `Model::chat_accounted` and
denies synchronously before HTTP. Debug redaction and absence of nonce material in
request serialization and durable dispatch evidence are checked. Successful combined
permit-adapter/HTTP testing and private socketpair loop integration are implemented;
subprocess transport bootstrap remains a release gate.

Broker tests combine host-issued permits, registered admitted funding, the real
model HTTP/SSE implementation, and temporary `runtime.redb` files. They verify
durable exclusive claims before the fixture sees HTTP, no authority lock or writer
across HTTP, exact wire model/output/routing/usage fields, and absence of permit,
campaign, and provider-key material in provider JSON. Valid final token/cost evidence
and request records survive reopen; retries use fresh IDs within one allocation.
Denied identity/bounds/model, expired deadlines, replay, replacement-stale and
revoked permits do not reach HTTP. Partial SSE, malformed billing, redirects, lost
replies, and stalled ordinary/required-tool requests retain unknown holds across
reopen; old permits and claimed IDs remain fenced. A blocked-redb-writer test proves
the single-thread async executor can enforce its deadline while reservation waits,
and the later non-cancellable commit cannot dispatch HTTP.

Daemon tests also cover concurrent allowance, fresh retry receipts with stable work and
execution identity, scope denial, unknown holds surviving reopen, accurate final
usage, terminal conflict rejection, and protected verifier classification without
task creation. Overflowing usage leaves its hold unresolved; representable
overcharges are recorded without clamping, create debt, pause admissions across
pools, and survive reopening the database. Ghost tests verify the ordinary loop;
daemon tests additionally run that same generic loop with the private adapter and
a real tool. Only tests currently invoke the broker, not shipped worker launch.

Funded-adapter tests also cover rollback after all reservation/evidence writes,
admission conversion with exhausted root headroom, concurrent requests bounded
by one allocation, retry consumption across Attempt IDs, protected funding in both
directions, cross-funding receipt rejection, reopen, and exact root refund on
closure. Ledger tests cover provisional overruns in unequal token/cost dimensions,
debt charged once, cancellation without release, rejected unresolved closure, and
idempotent finalization/closure after restart.

See [ADMISSION.md](ADMISSION.md) and [BUDGETS.md](BUDGETS.md).
