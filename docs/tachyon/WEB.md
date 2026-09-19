# Host Web Retrieval

Tachyond provides bounded, model-mediated web reports through OpenRouter's server
tools. It does not fetch target pages locally, launch Ghost, spawn a worker, or
create a campaign for an ordinary Conversation lookup. Responses contain a report,
typed citations, bounded citation annotations, observation time, and retrieval
status. They are **not raw pages**, a crawler, or proof that every requested URL
was fetched. PDF URLs are forwarded unchanged. Retrieved content is untrusted.

## Credentials and Configuration

Only the existing OpenRouter credential is used: `OPENROUTER_API_KEY` or the
existing OS credential-store entry. There is no Exa key, second credential, or
everyday enable flag. `[web]` is optional and defaults to enabled. The service
requires the OpenRouter endpoint and a configured model. It resolves the existing
Conversation model (including shared model inheritance); `web.model` is used only
when neither has a configured model. It never chooses an arbitrary new model or
sets `provider.only` to `openrouter` for ordinary requests.

Dedicated `Model::web_lookup` requests contain only `openrouter:web_search` or
`openrouter:web_fetch` server tools and omit `parallel_tool_calls` entirely,
regardless of the native-function setting. Sending even `false` unnecessarily
adds that parameter to endpoint compatibility requirements when
`provider.require_parameters` is true. This omission does not select a different
model or force a provider switch: configured routing, `require_parameters: true`,
disabled fallbacks, Exa, and campaign provider pinning remain intact. The query is
sent to the configured model for a model-mediated retrieval report, not directly
to Exa by the host.

Ordinary inference (including native `websearch`/`webfetch` function calls and
standalone worker inference) still respects its configured native parallel-call
flag and existing serial controls. Server-side retrieval may run internally in
parallel, but `max_uses` and `max_tool_calls` still bound permitted uses, including
four-use fetches. All permitted uses remain reserved, not just one HTTP request;
observed counts do not prove per-URL coverage. Output/response limits, deadlines,
conservative token/fee bounds, and inclusive completed-usage accounting are unchanged.
This removes an unnecessary routing constraint; it does not establish the cause
of a particular production rejection or change an already-running binary.

The service is initialized when the daemon starts. No running daemon or installed
configuration is changed by this implementation. Conversation registers native
`websearch` and `webfetch`; managed Ghost registers them per assignment. The foreground
uses blocking-pool IPC so retrieval does not block independent conversation turns.
Each daemon-authored user-turn command includes a `web_availability` snapshot
(`available` and an optional generic `reason`) in its correlation metadata. The
snapshot reflects an initialized host service, not merely `[web].enabled`.
Foreground uses it for both tool advertisement and dispatch; missing legacy or
local metadata fails closed. It is not inserted into model prompts, does not
contain credentials, and grants no funding or authority to bypass host admission.
No readiness RPC, key lookup, or automatic search is performed per model call.

## Conversation Tools

Tool arguments are the shared `WebRequest` payload, without model, authentication,
funding, or host identity fields. Exact examples:

```json
{"query":"current Rust release","domains":["rust-lang.org"],"max_results":3}
```

```json
{"urls":["https://arxiv.org/html/2401.00001","https://arxiv.org/pdf/2401.00001"],"instruction":"Summarize the paper","follow_links":false}
```

Search requires `query`; domains are optional and results default to 3
(range 1-3). Fetch requires 1-4 URLs; instruction is optional and link
following accepts only false/null/absent. The shared
`WebRequest::from_tool_input(name, input)` binder supplies the internal transport
tag. A caller-supplied `kind` or unknown field is rejected. Original metadata and root turn identity are preserved; command,
request, and tool-call IDs use the native call ID within the daemon's root scope.
Identical retries preserve all identities and cannot obtain fresh funding.

The tool returns a deterministic `WebOutcome` JSON object containing
`tool_call_id`, `query`, `freshness`, `evidence_notice`, `omitted`, and `result`
(the bounded `WebResult` projection). Encoded output is at most 8192 bytes;
citations take priority over report prose; redundant annotations are omitted from
the foreground projection. Synthesis divides the available evidence budget across
results instead of letting the first report evict later sources. URLs are never
shortened into invented URLs; titles and excerpts are retained ahead of model
prose, with explicit omission counts when bounds require reduction. Source indices
are provider metadata, not source facts. Citation offsets describe original
provider text only, never redacted text; projections clear character offsets when
shortening the answer, and never slice text using those offsets. Full typed
results remain in the daemon record. A grounded report is not automatically
source-verified. `result.usage` contains a typed `WebUsage` with optional
`receipt_id`, `input_tokens`, `output_tokens`, and inclusive `cost_micro_usd`.
The daemon persists a stable receipt with the result. Foreground carries it in
`ToolOutput` and deduplicates reported token counts by receipt within each turn.
Missing values are null, not fabricated zero. Valid tokens may be reported while
cost is unknown; this does not release escrow or authorize another lookup.
The final cost is one inclusive charge: never add server fees again.

Optional settings in `~/.config/tachyon/config.toml`, shown with their defaults:

```toml
[web]
enabled = true
concurrency = 4
max_requests = 4
max_server_calls = 4
max_fetch_urls = 4
input_tokens = 65536
output_tokens = 2048
turn_tokens = 270336
turn_cost_micro_usd = 1000000
max_request_cost_micro_usd = 250000
server_call_micro_usd = 10000
timeout_secs = 60
# model = "your/already-authorized-model" # fallback only
# input_micro_usd_per_million = ...     # optional host-attested bounds
# output_micro_usd_per_million = ...    # configure both, or neither
```

Search is fixed to Exa fast, one use, at most three results and 2,000 characters
per result. Fetch is fixed to Exa, at most four supplied URLs, one reserved use per
URL, and 4,000 content tokens per use. The daemon reserves the sum of all permitted
uses across the root, not just the number of RPCs. Link following is unsupported.
The model adapter rejects obvious local/nonpublic URLs and credentials in URLs;
remote DNS, retrieval scope, and enforcement still belong to OpenRouter/Exa.

## Budget Semantics

The default $0.25/request and $1/root limits are **provisional local host escrow**,
not advertised provider prices, prepaid grants, or provider-enforced spending caps.
The default 10,000 microUSD/server-use value is a host fee-coverage ceiling, not a
claim that either tool costs $0.01. Verify current model rates and inclusive server
tool billing before relying on configured bounds. If both token-rate bounds are
configured, reservation uses integer arithmetic, rounding up the complete input
plus output bound, then adding the fee ceiling for every permitted use. Otherwise
it uses the larger of the provisional request escrow and the total fee ceiling.
Provider usage can exceed host bounds; such an outcome is recorded and blocks
further requests in that root. No exact provider tariff is inferred.
Provider references: [web search](https://openrouter.ai/docs/guides/features/server-tools/web-search)
and [web fetch](https://openrouter.ai/docs/guides/features/server-tools/web-fetch).

Reservations, immutable root policy/model, command identity, counters, usage, and
bounded results are persisted in `runtime.redb`'s `web_turns` table. The adapter's
strict `RequestUsage::Final` supplies inclusive `usage.cost`, rounded upward to
integer microUSD. Tool fees are **not added again** on reconciliation. Display
token counts are never treated as billing evidence. Missing, incomplete,
conflicting or malformed billing evidence retains unknown spend. A usable report
with unknown billing is returned as unverified (or partial when interrupted);
subsequent requests in that root
are denied. A new, explicitly established root turn has an independent allowance.

Conversation roots include the conversation ID, the host-owned foreground session
UUID, and the turn ID. The daemon creates the session UUID once when it spawns a
fresh foreground session and reads it from its registry for web admission; it is
not a tool argument or a per-call nonce. Calls and retries within the same session
and turn share counters and receipts even if correlation metadata changes. A new
session may reuse a turn number without inheriting an earlier session's hold.
Pre-session-key records remain untouched under their original keys, including
unknown holds; they cannot be safely attributed to a new session and are not
migrated or replayed there. This changes neither the public request schema nor
private-worker/campaign scopes. Deployment starts a fresh foreground session;
resuming an old active turn across this key change is not supported.

The current daemon always launches foreground with `--new-session`; it does not
resume a foreground checkpoint after restart. The flag is startup-only. In a
running foreground, `BeginConversation` (including `reset: true`) is ignored and
turn numbers only advance; worker restart/resume/replan APIs reject foreground.
The UUID lives in the host's `AgentInfo.session_id`, not the foreground's event
envelopes: logical event/conversation/actor identities remain unchanged.

Checkpoint loading without `--new-session` restores history and `next_commit`,
not active requests or a durable host accounting session. Ordinary standalone
input has no host web-availability metadata and cannot enable web tools. If a
future host supports checkpoint continuation, it must persist and restore the
same accounting session for that continuation. Generating a fresh UUID while
resuming an old turn would provide a new allowance and lose replay/hold matching
for that turn. This limitation applies to every such restart, not only upgrades.
Current daemon restarts intentionally create new sessions with independent
allowances; old consumed amounts and unknown holds remain in their original
records, not refunded or enforced as a global cross-session budget.

An exact completed request ID replays the stored result, including an unknown or
failed outcome. Changed content under an existing ID is rejected. In-flight or
crash-interrupted IDs never dispatch again. One unresolved request serializes its
root; unrelated roots can run concurrently. The four-slot ordinary/standalone
service rejects excess requests as busy rather than keeping an unbounded queue.
It is separate from foreground model scheduling and campaign model capacity.

All redb work is offloaded to the blocking pool. Provider I/O stays in cancellable
async futures with deadlines, without locks or transactions held. Public socket
disconnect/shutdown and private-channel disconnect drop provider futures, retaining
unresolved durable claims. There are no automatic retries or redirects. Results
are retained as bounded ledger reports; no raw-document resource descriptor is
claimed or exported.

A provider/transport/parser failure after complete SSE frames supplied bounded
observations returns a `Partial` report containing those observations and an
explicit failure notice. It never returns provider error bodies or executes
unexpected client tool calls. Even if a prior frame contained complete billing,
an interrupted stream reconciles as `Unknown`: observed token/use counts may be
shown, but no final cost or refund is inferred. The host persists and replays the
partial report with the same receipt while retaining its unknown-spend hold.
Failures before evidence still return errors. External cancellation, process
death, or a host deadline that drops the whole lookup can retain only the durable
claim, not an in-memory partial report; no incremental evidence journal is claimed.

## Public Consumer API

```rust
impl tachyon_client::Client {
    pub fn conversation_web(
        &mut self,
        metadata: tachyon_api::InteractionMetadata,
        command: tachyon_api::web::WebCommand,
    ) -> Result<tachyon_api::web::WebResult, tachyon_client::ClientError>;
}
```

The wire request is `ApiRequest::ConversationWeb { metadata, command }`; the reply
is `ApiResponse::ConversationWeb { command, result: Result<WebResult, String> }`.
Use the host-established metadata, logical `FOREGROUND_ID` as both conversation
and caller, and the same root turn ID in metadata and command. Do not generate a
new turn ID for each tool call. Each distinct operation needs a stable unique
request ID; retries must preserve the complete command. The public path uses the
existing same-user Unix peer check and requires a live logical foreground route.
Public direct dispatch cannot bypass that route. Use separate client connections
for independent concurrent calls. No credential or policy is accepted in tool
arguments.

```rust
WebCommand {
    command_id, caller_id, tool_call_id, turn_id, request_id,
    request: WebRequest::Search { query, domains: None, max_results: 3 },
}
// Or:
WebRequest::Fetch { urls, instruction: None, follow_links: None }
```

## Private Consumer API

```rust
impl tachyon_model::broker::BrokerClient {
    pub async fn web_lookup(
        &self,
        command: tachyon_api::web::WebCommand,
    ) -> tachyon_model::Result<tachyon_api::web::WebResult>;
}
```

This selects `agents::Request::WebSearch { command }` or `WebFetch { command }`.
Corresponding typed replies have the same variant, command and report. The broker
checks correlation, variant, answer/annotation bounds, citation shape and target
URLs; a mismatched reply fails the channel. `Control::WebSearch` and
`Control::WebFetch` describe these capabilities.

For noncampaign host services, the daemon's host-only entry point is
`WebService::serve_private(channel, source, root_turn, deadline)`. The host binds
source and root before giving the authenticated channel to a consumer; callers
cannot refresh their root allowance through that channel. It shares the ordinary
service's semaphore and durable ledger. No campaign, Work, or oversight allocation
is manufactured for these requests, and no database handle reaches the consumer.

Managed warm Ghost receives `web::WorkerAssignment`, a transport-only envelope
around `WorkRequest` with an optional `host_service: ServiceBootstrap`. The secret
is never included in durable Work records, checkpoints or provider prompts.
`WebService::assignment` issues a one-use private socket, verifies Ghost PID/UID,
authenticates its capability, and binds the original work/generation/assignment
until its deadline. Each lookup still has the host's 60-second default cap.
Warm reuse cannot carry the previous channel or tool registration into the next
assignment. Host cancellation, shutdown, replacement or Work end closes it.

This channel also proxies existing ordinary worker inference to the configured
worker model, without passing the OpenRouter environment key to Ghost. Ordinary
worker inference remains distinct from retrieval escrow and campaign funding;
its channel is bounded by the Work deadline and broker frame/request limits.
The campaign native broker and admitted Work allocation path are unchanged.
Plain local CLI Ghost without a daemon assignment advertises neither web tool.
See [Ghost web tools](../ghost/WEB.md) for native/Python examples.

Campaign access requires a new, explicit optional manifest field:

```json
"web": {
  "max_requests": 4,
  "max_server_calls": 4,
  "inference_provider": "actual-openrouter-inference-provider-slug"
}
```

This pins the campaign's existing model requests to the explicitly authorized
upstream provider. `openrouter` itself is not a valid inference-provider selector
for this grant. Old manifests have no web allowance. Merely listing web controls
does not grant funding or access. The allowance is campaign-wide, across workers
and retries, bounded again by host policy. The command caller must match the
permit's Work ID. Verification/compaction/oversight authority does not grant web.

Every campaign request uses `ModelBroker`, the same live Work permit and exact
admitted `RequestReservation`, `PermittedAccounting`, and campaign model capacity.
Input bounds must cover the host web bound, output must fit the adapter cap, and
existing `other_micro_usd` must cover every permitted use at the host fee ceiling.
Insufficient older grants are denied, never upgraded. The web manifest field does
not add money, tokens, Work, or a text-only oversight grant. Inclusive final cost
debits the existing Work allocation exactly once.

The accounted adapter also rejects fee bounds below 7,000 microUSD for its single
Exa fast search or 1,000 microUSD per permitted Exa fetch (4,000 for four URLs).
These fixed-tool fee floors do not replace higher host ceilings or cover model
tokens, including repeated inputs in the provider's internal tool loop.

## Model Handoff

`Model::web_lookup` preserves configured routing and returns `WebCompletion` with
`completion`, `result`, and `usage: RequestUsage`. Ordinary hosts must reserve
before invoking it, then persist reconciliation from `usage`. The accounted
variant retains its pre-HTTP accounting callback and explicit provider pinning;
both variants use the same strict billing parser. Errors/cancellation and partial
failure reports retain the host hold as unknown. Never derive cost from
`completion.usage`.

The adapter additionally exposes independently validated provider token counts
in `result.usage`; absent or malformed counts stay null. Only strict
`RequestUsage::Final` supplies a final inclusive cost. The host attaches the
receipt during durable reconciliation; replay preserves it.

Local-only tests cover search/fetch wire shape, one-key transport, citations,
public peer-route constraints, replay, durable caps, concurrent capacity,
disconnects, incomplete/unknown billing, private scope validation, revoked permits,
fee denial, and exact inclusive Work debits. They use no live provider credentials
or external paid requests.

The optional actual warm-process fixture can be run without provider credentials:

```sh
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_warm_ghost_assignment_services_without_credentials_or_browser
```

It uses a localhost provider and an empty Ghost credential environment across six
assignments in one `--chat` process. It checks forged-generation denial, cancellation
of a pending lookup with its hold retained, capacity release, a fresh warm assignment,
legacy daemon input, and clean exit after EOF. No browser is launched. The existing IPython proxy fixture remains
optional and requires an already installed IPython; no installation is performed.
