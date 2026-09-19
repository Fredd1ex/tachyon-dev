# Budgeted Campaign Oversight

Campaign oversight is opt-in host policy. An authorized `campaign run` with an
`oversight` object funds the named `oversight` service before root Work admission.
Omitting the object preserves the old manifest serialization/digest and creates no
oversight service, task, assessment or provider request. Model output cannot enable
it. The explicit standalone background assessment protocol remains available and
does not implicitly activate this daemon pipeline.
Absent oversight configuration means disabled oversight and no oversight model
credits spent, not a fabricated zero-cost assessment. Once authorized, the host
automatically assesses the supported durable triggers within the immutable budget.

## Configuration

This is a complete root-only manifest example. Replace the campaign ID/objective
with the exact Draft campaign identity, select the installed executable and
dedicated canonical directories, set an absolute future `deadline_ms`, and approve
the actual model/pricing bounds before use. Values below are illustrative, not a
provider price recommendation.

```json
{
  "schema_version": 1,
  "campaign_id": "campaign-00000000000000000000000000000000",
  "objective": "Publish one candidate",
  "executable": "/opt/tachyon/ghost",
  "workspace": "/tmp/campaign/work",
  "home": "/tmp/campaign/home",
  "deadline_ms": 2000000000000,
  "work_tokens": 200000,
  "work_cost_micro_usd": 200000,
  "verification_tokens": 10000,
  "verification_cost_micro_usd": 10000,
  "max_active_inferences": 3,
  "model": "host-approved-model",
  "pricing_revision": "operator-approved-v1",
  "max_request_bytes": 64000,
  "input_tokens": 16000,
  "output_tokens": 1024,
  "input_micro_usd_per_million": 1000000,
  "output_micro_usd_per_million": 1000000,
  "other_micro_usd": 0,
  "oversight": {
    "tokens": 40000,
    "cost_micro_usd": 40000,
    "max_assessments": 2,
    "timeout_ms": 20000,
    "conversation_id": "foreground"
  },
  "evaluator": {
    "argv": ["/usr/bin/true"],
    "timeout_ms": 1000,
    "output_bytes": 1024,
    "input_bytes": 1024,
    "max_attempts": 1,
    "max_total_command_ms": 1000
  }
}
```

The strict `oversight` schema accepts only:

| Field | Contract |
| --- | --- |
| `tokens` | Immutable Work-pool token allowance; must cover one approved request. |
| `cost_micro_usd` | Immutable Work-pool cost allowance; must cover one approved request. |
| `max_assessments` | Integer 1 through 64; includes failed, stale and unknown attempts. |
| `timeout_ms` | Integer 1 through 300000, further limited by the original campaign deadline. |
| `conversation_id` | Optional explicit destination, nonblank, at most 256 bytes, no control characters. Omission retains assessments internally only. |

Unknown fields, floats, negative bounds, and insufficient root remainder fail
validation. Fixed and dynamic child allowances plus oversight are subtracted from
the Work envelope before root admission. The remainder must cover the root's
minimum request. Verification's protected allowance is unchanged. The count is a
ceiling, not a promise: allowance, inference capacity, cancellation or an unknown
hold can prevent further HTTP dispatch.

There is no separate oversight model, endpoint, pricing or output-token override.
The same campaign `ModelBroker` calls `execute_service`; its immutable service
policy uses the campaign's approved estimate. It uses `RequestClass::Work` with
`CampaignOversight` purpose, no tools, no registered synthetic Work, and no resident
or execution lease. It shares host model capacity and the campaign ledger.

## Triggers And Ownership

The existing campaign task owns the permit and assessor future, not a background
process. There is one in-flight request and one durable coalesced pending cause set.

- Explicit authorized launch bootstraps one assessment.
- Logical Work terminal transitions journal `logical_work_terminal` in the host transaction.
- A new durable Finding journals `new_finding` in its insertion transaction.
- Work wait changes, transitions to awaiting verification/human acceptance, and Campaign todo writes in Blocked status journal `blocked_state`.
- Non-oversight reported Work tokens or cost crossing 25%, 50%, 75%, 90% or 100% of the Work envelope journal `budget_threshold`. High-water bands do not reset.
- An authorized explicit request journals `explicit_request`; repeated command IDs are idempotent.

Source notifications wake the owner. A one-second fallback checks only that
campaign's durable pending receipt; idle ticks do not scan evidence or monitor
tables. Canonical projections are obtained for claims and publication checks.
Progress text, periodic monitor timestamps,
and oversight's own usage do not themselves trigger assessments. Changes while a
request runs coalesce into the next snapshot. There is no automated retry of an
unknown request ID. A durable interrupted claim remains `claimed_unknown`; a
provider result without final billing becomes `unknown` and stops the assessor.

Ordinary evidence-review resume/continuation does not reactivate the assessor or
replace its permit. Reopening the daemon does not restart assessments. Retained
attempts remain inspectable. Enabling a previously disabled campaign or changing
its immutable policy requires a new authorized campaign, not an `assess` request.

## Snapshot And Publication

The host writer fences a canonical read transaction containing the todo first page
(at most 20, with partial-page cursor/revision), durable Campaign monitor projection,
Work result/generation/instruction versions, Finding versions and bounded source
summaries. Finding/candidate text is sanitized, bounded and explicitly untrusted.
At most 20 Work/Finding summaries are sent, alongside `evidence_total`; the omitted
evidence is not represented as read. Todo descriptions and the full plan are not
injected. At most 20 output references can name only supplied todo/evidence IDs.
Resource observations are typed numeric host projections for the exact campaign,
not model estimates or guessed links. Unknown observations remain unknown.

The semantic fence includes campaign identity/objective, todo scope revision, Work
results and acceptance state, findings, cancellation/wait state, and other budget
holds/reports/transfers. It excludes the oversight allocation and its own model
charges, not other spend. Raw ledger revision and sample clocks are not freshness
authority. The writer rechecks the fence before publication. Stale, malformed or
cancelled results do not publish; final provider billing is still reconciled.

Each durable attempt receipt retains its ID, input hash, bounded source references,
semantic revision, trigger causes, status, and accepted assessment when present.
The service ledger remains authoritative for final or unknown billing.
On owner shutdown, a service allocation with exclusively final reports is closed
through the existing ledger, releasing only unused allowance; root admission is
not enlarged. An unresolved request keeps its allocation and unknown hold intact.

## Conversation Delivery

Accepted assessments use the dedicated `PublishCampaignAssessment` command, never
a WorkResult. The current host has one foreground session, `foreground`: only an
explicit matching destination is delivered. Other destinations are retained and
are never substituted with the current conversation. Without a destination there
is no notification outbox entry.

`foreground` is the existing logical host destination, not a process/session UUID
or a configurable display name. An undelivered outbox entry can therefore retry
to a replacement Foreground process with that same logical destination. A new
model session does not retarget other conversation IDs or clear the outbox.
This is not routing among multiple isolated user conversations.
Already admitted publications recover from durable history rather than being
automatically re-injected as evidence into a fresh conversation checkpoint.

Foreground deterministically relays a summary labelled an **unverified background
assessment**, with source references. There is no second model request and no
turn-ID allocation. A busy turn's messages, pending reply and commit cursor remain
untouched. Typed assessment evidence is checkpointed separately, deduplicated by
assessment ID, and included only in a later relevant request (up to three matches;
at most 256 retained assessments). It is not treated as verified Work acceptance.

The publication identity is `<assessment-id>:published`. The host validates exact
destination, metadata and advisory text against its durable outbox; admission and
history enqueue are atomic. Replayed publications are suppressed. Delivery retries
after five seconds reuse that identity. Existing PriorityAttn rendering and
host-origin budget/blocked attention remain separate: model `attention` output
does not create attention authority or duplicate those notifications.
Host attention displays a separate standalone notice even during synthesis; it
does not cancel the active provider request or constitute a general safety-emergency
interrupt. Displayed and acknowledged receipts require explicit, exact-ID membership
and are separate from delivery; see [Durable Attention](../interaction/ATTENTION.md).

Campaign ledger reconciliation separately emits deterministic `BudgetBlocked`
attention for a durable admissions pause or known Work-envelope exhaustion,
including actual oversight charges. Mere service allowance/count exhaustion or
the 25% assessment trigger is not a campaign budget blocker. This producer works
without enabling oversight and never grants more budget.

## Operator Interface

```sh
tachyon campaign run manifest.json --unisolated-development
tachyon campaign assessments CAMPAIGN_ID
tachyon campaign assess CAMPAIGN_ID --command-id review-after-decision --unisolated-development
```

The corresponding same-user API requests are `CampaignAssessmentList { id }` and
`CampaignAssessmentRequest { id, command_id, unisolated_development }`, returning
`CampaignAssessments { records }`. Listing is read-only and starts no provider.
An explicit request cannot enable disabled oversight, change its destination,
increase its budget/count, or reopen an exhausted/unknown service.

Conversation also exposes linked-campaign `list`, `status`, `steer`, and `cancel`
through a host-authorized tool. It does not create launches or spending grants.
Status separates accepted/applied/delivered revisions, and cancellation intent
is durable and signalled by root/child owners before cleanup can be confirmed.
See [Conversation Campaign Control](../interaction/CAMPAIGN_OVERSIGHT.md#conversation-campaign-control).

See [HOST_SERVICE_ACCOUNTING.md](HOST_SERVICE_ACCOUNTING.md) for the underlying
permit, reservation, dispatch and final-billing contract.
