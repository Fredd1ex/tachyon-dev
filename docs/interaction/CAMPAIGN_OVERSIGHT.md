# Campaign Oversight

`campaign` is the third orchestrator role, registered for Background/Primary with
internal output. It is policy hosted in the existing background process, not a
new daemon, worker, periodic task, or allocation controller.

## Calling

The background JSON-lines stream accepts the original unwrapped work review
request, existing schedule requests, or a strict `CampaignAssessmentRequest`
from `tachyon_api::campaign_oversight`, identified by
`"kind":"campaign_assessment"`. Assessments share the existing four-request
concurrency bound. Empty input performs no inference.

An explicit caller supplies a request ID, campaign ID/revision, bounded objective
summary, campaign todo scope and typed list response, and campaign-scoped typed
monitor snapshot. Query the todo service for at most 20 relevant items and the
monitor service for read-only observations first. Do not attach a full plan or
raw ledgers. The host checks scopes and input bounds and projects only todo
IDs/titles/statuses, page/revision information, observation freshness and selected
exact resource counters into its provider-neutral `CampaignSnapshot`. A todo
page is not proof of the entire plan; descriptions and process inventories are
not sent to the model. Supplied observations are not independently authenticated
or refreshed by this invocation.

`assess_campaign` is the host invocation entry point. Registry resolution and
explicit host Todo/Monitor grants are required. Wire input cannot add grants.
The role declares narrow read schemas for future host-bound query execution,
but this initial path executes no model tools: it makes one model call with the
supplied read results, validates a typed `CampaignAssessment`, and returns a
correlated result or error. Missing credentials return `model_unavailable`;
there is no fabricated model assessment. The pure snapshot renderer works
without a model.

## Authority

Assessments contain a summary, findings, known todo references, blockers, and an
attention classification. They are advisory and internal. They do not accept or
verify work, authorize budgets, allocate resources, launch agents, or mutate
todos. No automatic model runs are scheduled by registration or monitoring.
Any follow-up todo proposal must be translated by a later host/operator into a
typed todo command with its own authority, command ID and revision checks; no
model-produced command is executed here. Live plan queries and mutating todo
operations are deliberately not implemented as another tool loop.
