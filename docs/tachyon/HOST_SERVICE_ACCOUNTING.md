# Host Service Accounting Seam

Internal Rust-only support exists in `runtime_store/model_accounting/services.rs`.
Authorized campaign launch now activates it only for an explicit strict `oversight`
manifest policy; see [CAMPAIGN_OVERSIGHT.md](CAMPAIGN_OVERSIGHT.md). The pipeline
approves that policy and funds oversight before granting the remaining Work
allowance to root Work. No service creates registered Work,
an admitted Work handle, a resident worker or an execution lease.

## Funding and Authority

`ServicePolicy` fixes `CampaignOversight`, the full token/cost allowance, exact
`RequestEstimate`, finite `max_requests`, and `timeout_ms` (1..=86_400_000).
The caller is trusted host policy code. Nothing derives authorization from model
output. An authorized campaign manifest is translated by the host, not passed as
model authority. The complete policy is persisted with its SHA-256 hash and
the allocation receipt binds that hash. Credentials are not persisted.

`host_authorize_service` atomically allocates the full allowance from the existing
campaign Work envelope. It neither creates money nor accesses Verification funds.
The separate service table binds a campaign/name to one immutable policy.
`FundAllocation` remains restricted to exact registered Work holds.

An opaque, nonserializable `ServicePermit` contains a private nonce and cancellation
lease. A process-local map holds the current authority. Explicit reauthorization
after reopen must supply the identical policy; it does not reset funds or counts.
In-process replacement requires `Some(&current_permit)`, even after revocation.
Dropping or calling `revoke()` on a permit cancels that lease and provider I/O.
Dropping an old replaced permit does not revoke its replacement.

## Pipeline Calls

Exact internal interfaces:

```rust
RuntimeStore::host_authorize_service(
    &self,
    campaign_id: &str,
    name: &str,
    policy: ServicePolicy,
    replace: Option<&ServicePermit>,
) -> tachyon_model::Result<ServicePermit>

ModelBroker::execute_service(
    &self,
    permit: &ServicePermit,
    request_id: &str,
    messages: &[tachyon_model::ChatMessage],
    on_delta: &mut (dyn FnMut(&str) + Send),
) -> tachyon_model::Result<tachyon_model::Completion>

ServicePermit::revoke(&self)
```

Example host wiring, with `estimate`, `allowance`, request limit and timeout
provided by explicit host approval, not invented defaults:

```rust
use crate::runtime_store::model_accounting::services::{
    ServicePolicy, ServicePurpose,
};

let policy = ServicePolicy {
    purpose: ServicePurpose::CampaignOversight,
    allowance,
    estimate,
    max_requests,
    timeout_ms,
};
let permit = tokio::task::spawn_blocking({
    let store = store.clone();
    let campaign_id = campaign_id.clone();
    move || store.host_authorize_service(&campaign_id, "oversight", policy, None)
}).await??;

// Use the host-owned ModelBroker configured for the approved model and output cap.
// New logical request IDs are host-generated and must survive pipeline retries.
let completion = broker.execute_service(
    &permit, &request_id, &messages, &mut |_: &str| {},
).await?;

// Host cancellation callback, or simply drop the permit when its owner exits.
permit.revoke();
```

The synchronous authorization function belongs on `spawn_blocking` when invoked
from async pipeline code. Dispatch runs storage on the blocking pool, never holds
a transaction across network awaits, and consumes only host model-call capacity.
It derives attempt identity and accounting configuration from authority and the
logical request ID. It accepts no worker permit, tools or caller-supplied reservation.

## Accounting Semantics

`Model::chat_accounted` reserves using `ReserveAllocated` and atomically records a
durable exclusive request claim before HTTP dispatch. Any repeated request ID is
denied, across replacements and restarts; retries need a new logical ID. One
unresolved request per service is allowed. Durable claimed-request count never
decreases, including unknown, cancelled and timed-out requests.

Final inclusive provider billing reconciles actual usage through the existing
ledger adapter. Missing/incomplete billing, provider failure, cancellation, timeout
or a dropped future retains the unknown hold and claim. Unknown is not final zero
and blocks further service requests. Revocation does not prevent already-observed
final billing from being committed. There is no automatic release, refill, retry
or activation here. The pipeline must not interpret a successful text completion
as proof that billing was final.

For an existing campaign launch, dispatch additionally requires the pipeline's
durable assessment claim, Running campaign status, and the original campaign
deadline. Descriptor-only host service tests without a launch retain their
explicit host-only authorization behavior.

Tests use real redb ledgers and loopback HTTP only, with no live provider costs.
