# Campaign Oversight

`campaign` is the third orchestrator role, registered for Background/Primary with
internal output. Shared preparation and validation live in the provider-neutral
orchestrator crate. The explicit standalone host below remains supported.

For opt-in event-driven daemon ownership, immutable Work-pool service funding,
canonical snapshots, stale-result fencing and typed Conversation delivery, see
[Budgeted Campaign Oversight](../tachyon/CAMPAIGN_OVERSIGHT.md). That pipeline is
owned by CampaignService, not a new daemon, worker or allocation controller.

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

Assessments contain a summary, findings, supplied todo/evidence references, blockers, and an
attention classification. They are advisory and internal. They do not accept or
verify work, authorize budgets, allocate resources, launch agents, or mutate
todos. Registration or monitoring alone schedules no model runs. An explicitly
authorized oversight manifest does enable automatic, budget-bounded assessments
on durable host triggers through CampaignService; absence of that configuration
disables this pipeline and spends no oversight model credits.
Any follow-up todo proposal must be translated by a later host/operator into a
typed todo command with its own authority, command ID and revision checks; no
model-produced command is executed here. Live plan queries and mutating todo
operations are deliberately not implemented as another tool loop.

## Conversation Campaign Control

Conversation has a native `campaign` tool for existing authorized launches whose
stored `oversight.conversation_id` explicitly matches the logical foreground.
The same-user `ConversationCampaign` API carries host-attached interaction origin
metadata separately from strict model arguments. The daemon validates the logical
foreground against its explicit registry selection and checks the enabled
Conversation invocation's selected capability and schema. Display names, user
prose, Work names, and group names never establish links or authority.

Operations are `list`, `status`, `steer`, and `cancel`. An explicit campaign ID must
belong to that linked set. Status returns bounded Work pages (at most 32) with exact
IDs, short objective summaries, generation, accepted/applied/delivered revisions,
and cancellation state. `include_plan: true` attaches a bounded campaign todo page
only on request. Conversation todos otherwise remain standalone conversation scope.
Current routing uses one logical destination, `foreground`, not multiple isolated
user conversations. Linked status and oversight snapshots carry exact numeric
host state and explicit IDs; neither prose nor guessed associations establish
campaign membership, resource usage, or authority.

Steering uses a separate `command_id`, exact Work ID, expected revision, and only
the user's requested instructions. Cancellation uses an exact Work ID and generation
and includes that Work's descendants, never an inferred group. Identical command
replays are idempotent; changed payloads, including changed expected revisions,
conflict under the same command ID. Refresh status and use a new command ID after
a revision conflict. Receipts say **accepted**, not **applied**. Do not claim Done
until status confirms the accepted steering revision applied, or the cancelled
branch's cleanup is complete.

The root owner and child scheduler poll durable cancellation intent and signal
their existing execution handles, including active provider I/O and human waits.
Acceptance alone does not imply interruption or cleanup. Applied and delivered
revisions are actual host boundary state, not aliases for the accepted revision.
An identical mutation replay returns its original receipt even after later
revisions or terminal state; it does not create another mutation or spending grant.
Unknown argument fields (including scope/identity dictionaries) and non-string
instruction payloads fail closed. The public same-user socket is operator authority,
not a worker capability; origin metadata is logical routing, not a secret credential.

The host binds the operator control facade to the launch protocol's authoritative,
validated root enrollment. It may steer the root or an exact descendant without
expanding worker parent-only permissions. This route cannot create campaigns,
grant spending, resize groups, transfer budgets, or fabricate revised objectives.
The base Conversation prompt is unchanged; the additional selected tool schema
and ordering are deliberate feature changes.

Typed host commands are not accepted from raw user `AgentChat` text: the daemon
wraps it as `AcceptUserTurn` with host metadata. Strict argument decoding also
rejects model-supplied identity and scope extensions. Owner-only foreground
checkpoint permissions protect local persistence; they do not provide universal
raw-secret redaction. See [parallel acceptance](PARALLEL_ACCEPTANCE.md) for the
real-process coverage and its scripted-model/delegation-replay limits.
