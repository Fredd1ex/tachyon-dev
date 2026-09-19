# Interaction Architecture

## Current Implementation

The structural refactor is complete, including bounded capability infrastructure:
the role registry, host runtime extraction, daemon messaging extraction, durable
todos and operational feed, read-only monitoring, optional TUI views, and explicit
campaign assessment. Explicitly authorized manifests can now activate the separate
[budgeted CampaignService oversight pipeline](../tachyon/CAMPAIGN_OVERSIGHT.md).

Foreground and background remain independent executable hosts, each constructing
its own Tokio runtime in `main.rs`. There is no new shared crate or combined host
loop. Package and binary names remain `tachyon-foreground` and
`tachyon-background`; daemon actor IDs remain `foreground` and `background`.
Existing wire identities, persisted paths, configuration keys, and ordinary chat
behavior are preserved; the new services have explicit typed request paths.

```text
crates/interaction/
  foreground/
    Cargo.toml
    src/
      main.rs          90-line entry point, arguments and startup wiring
      runtime.rs       configured model and fallible Conversation registry adapter
      intake.rs        stdin admission, routing and concurrent turn task spawning
      execution.rs     turn/model loop, answerability timing and fallbacks
      tools.rs         native dispatch, contextual filtering and service validation
      scheduling.rs    future-time interpretation and scheduler routing
      input.rs         typed commands and legacy input decoding
      turns.rs         evidence, ordered history and context snapshots
      checkpoints.rs   checkpoint format and ordered writer
      delegation.rs    daemon worker, memory and scheduling requests
      model.rs         conversation policy/model adapters
      streaming.rs     correlated events and stdout publication
  background/
    Cargo.toml
    src/
      main.rs          entry point, configuration/model setup and request wiring
      requests.rs      bounded concurrent request processing and output
      review.rs        model review, parsing, validation and failure decisions
      campaign.rs      explicit one-shot campaign snapshot assessment
      scheduling.rs    deterministic schedule request validation
crates/orchestrators/src/
  registry.rs          pure invocation dispatch, not execution or authorization
  agents/
    conversation/      prompts, tools and routing/answerability policy
    coordinator/       prompts and tools for coordination/review
    campaign/          internal campaign assessment policy and bounded snapshots
```

The registry lists exactly Conversation, Coordinator, and Campaign. Host lanes
are not new roles; registered capabilities neither grant permissions nor imply that every
invocation exposes every tool. See [roles.md](roles.md) for policy IDs, public
reexports, prompt fixtures and capability ordering.

Memory remains the storage service in `crates/memory`, with the existing
Conversation memory capability. There is no memory inference role or placeholder
policy directory. Memory storage and its ownership are unchanged.

Conversation policy asks the model to recall relevant stored preferences before
recommendations or preference-sensitive answers, including format constraints,
and before delegation when needed. Current explicit instructions override stored
preferences. Greetings and context-free factual questions do not require recall.
The existing native `memory` service handles recall; there is no new classifier
or automatic per-turn lookup. The host limits recall to once per turn and removes
recall from subsequent schemas. Successful, matching current-request recall items
also survive worker synthesis as bounded user context, not instructions.
Accepted-evidence follow-ups expose only memory, not new worker or web work;
required fresh-work turns can recall before delegation rather than forcing both
into the same batch. Neither path automatically invokes memory.
Offline scripted-provider tests verify outbound policy, schemas, service routing,
and result propagation. They do not prove that a live model will choose recall or
obey preferences; relevance and answer compliance remain model responsibilities.

Foreground `main.rs` owns argument parsing and startup wiring; the extracted
modules above own the runtime behavior. `runtime.rs` resolves Conversation,
Foreground lane, `Primary`, then renders configured identity/persona. Prompt or
resolution failure disables the model before intake can call it and uses the
existing configured-error/turn-publication path, without a default prompt fallback.
Tool selection and each dispatch authorization consult the registry and intersect
its invocation schemas and descriptor capabilities with the implemented host
tools: single/multiple delegation, memory, schedule, todo, campaign, websearch,
and webfetch. Native web dispatch uses `Client::conversation_web` with original
turn metadata and stable native call identities, not worker delegation. Host
`[web].enabled = false` removes schemas and denies dispatch. The daemon owns
root admission, fixed retrieval engine, credentials, and retained typed evidence.
The next completion consumes a bounded structured report directly; if worker
delegation also requires synthesis, only matching native web call/result IDs
contribute `WebOutcome` evidence to the synthesis brief.
Contextual narrowing and the existing execution guards remain host policy.

Background review resolves Coordinator, Background lane, `Review`, using the
configured persona. It exposes exactly the registry's `submit_work_review` schema,
never Coordinator primary lifecycle tools. Registry failure returns an internal
`Inconclusive`/`ModelUnavailable` decision with the original correlation IDs and
no provider call. Invalid-request and absent-model checks still precede resolution;
required-tool mode, parsing and timeout behavior are unchanged.

The descriptor is host policy metadata, not a generic dynamic execution engine.
Visibility does not publish output: Conversation retains user-facing publication
and reviews and campaign assessments remain internal advisory decisions.
Background `main.rs` retains startup guards, Tokio startup, configuration/model
construction and stdin/stdout wiring to request processing.

Tachyond still owns process launch and authority. Executables resolve beside
Tachyond, with `TACHYON_FOREGROUND_BIN` and `TACHYON_BACKGROUND_BIN` overrides;
Cargo output names and persisted state paths are unchanged. The TUI retains
ownership of presentation and session archive behavior.

## Implemented Services

### Foreground Turn Scheduling

Intake owns concurrent turn and modeled-notification tasks in a `JoinSet`.
Dependency waits do not hold the intake loop or a provider permit. A queued
`AnswerNow` turn skips the preceding turn's evidence assessment; dependent turns
may answer from a supported subset before their parent finishes. An insufficient
assessment waits for changed selected evidence or a terminal parent, not every
notification. Wait futures are created before state checks, preserving Tokio
`notify_waiters` delivery even before the future's first poll.

After answerability inference, execution rechecks the selected evidence, parent
terminal state and context epoch before applying its decision. Changed snapshots
are rebuilt; terminal turns cannot publish or commit late inference results.
`CancelConversation` closes outstanding history gaps, preserves already-published
independent replies, aborts owned tasks and their receipt timers, and checkpoints
the advanced cursor. Publication is serialized with cancellation separately from
the conversation-state lock. Campaign advisories deduplicate before publication
and retain only the newest observed revision per campaign.

Regression tests use a loopback HTTP fake provider with held response sockets,
not paid inference. They cover partial/subset evidence with a pending parent,
independent publication, insufficient-evidence wakeups, stale assessments,
cancellation and late replies, ordered history, and notification ordering.

Limits: routing still identifies dependency by the preceding numeric turn, not
an explicit classifier-selected parent; intervening notification turns and
multi-parent dependencies remain unsupported. `InterruptAndReplan` is still a
dependent routing decision, not an authoritative worker supersession command.
Conversation cancellation stops foreground futures, not daemon-owned work or
already-running blocking service calls. Work-result generation/revision authority
remains a daemon responsibility; foreground does not infer revocation from prose.
There is no new wire-level per-turn cancellation acknowledgement or persistent
in-flight task recovery. Conservative routing fallback can still wait when the
classifier cannot establish independence.

- [Daemon messaging](daemon-messaging.md) owns command envelopes,
  notifications/history projection, and agent/work subscriptions. Typed campaign
  advisories and [durable attention](ATTENTION.md) use deterministic host publication,
  not a second inference or a new bus. Attention can display during active synthesis
  without aborting the provider request.
- [Structured todos](TODOS.md) are daemon-owned redb records with exact scope
  authority, revision checks, idempotent receipts, and transactional persistent
  operational events. `Todo`, `TodoSnapshot`, and `OperationalSubscribe` operator
  endpoints and typed client adapters are implemented. This durable feed is not a
  retrofit of replay guarantees onto legacy transcript/agent/work streams.
- [Monitoring](../tachyond/MONITORING.md) implements `MonitorGet` and
  `MonitorSubscribe`, bounded shared sampling, and coalesced read-only snapshots.
  Native CPU/GPU charges are job wall time, not OS utilization; retained bytes are
  logical accounting, not measured disk usage. Unknown is not known zero. Durable,
  capacity, and registry source clocks do not form a global atomic snapshot.
  Runtime schema remains v1; monitor versions are not durable feed revisions.
- [Ghost todo/monitor adapters](../ghost/TODOS_MONITOR.md) are optional native and
  Python broker tools. Exact Work, Campaign, and availability grants are separate;
  registration, membership, and Resource authority do not grant access.
- [Operational views](../tachyon/OPERATIONAL_VIEWS.md) add optional read-only
  `TODO` and `RESOURCES` tabs with bounded caches. They do not copy operational
  records into the transcript or change default chat, history, selection, or
  response-copy behavior.
- [Campaign assessment](CAMPAIGN_OVERSIGHT.md) resolves Campaign on
  Background/Primary for an explicit `CampaignAssessmentRequest`. The caller
  supplies bounded todo and monitor snapshots. Despite declared read capabilities
  and required host grants, this path passes no tools to the model and executes
  none: it makes one model call and validates an internal advisory result. This
  standalone path does not fetch observations or mutate todos. The separate opt-in,
  budgeted CampaignService automatically assesses durable semantic triggers using
  canonical host snapshots; campaign oversight is not limited to one-shot callers.
- Conversation's native `campaign` tool lists, inspects, steers and cancels exact
  Work branches in explicitly linked authorized campaigns. Accepted, delivered and
  applied state remain distinct; durable root cancellation intent is polled and
  signalled to execution, not reported as immediate cleanup.

The base Conversation prompt is unchanged, but selected native `todo` and `campaign`
schemas are implemented. The todo tool binds only `current_conversation`; campaign
selectors remain denied even when the campaign tool can read a linked plan.
There is no default monitor tool or automatic full-plan injection. Todos are
durable structured records, not Markdown files; no plan-file watchers, export/sync,
automatic completion or automatic conversation rotation is implemented here.

Strict typed input and host-authored envelopes prevent user `AgentChat` text from
injecting host commands or metadata. Foreground checkpoints are owner-only (`0600`).
These are routing and file-permission protections, not universal raw-secret
redaction of user, tool, evidence or checkpoint content.

## Characterization Evidence

Foreground tests exercise successful `process_turn` execution using the real
`tachyon_model::Model` against a scripted localhost HTTP/SSE endpoint. The endpoint
accepts A's request but holds its response while independent B completes. Tests
observe B's typed delta and final publication, verify B remains pending outside
the committed checkpoint snapshot, then release A and verify exact assistant
contents in A/B commit order. The existing publication callback is threaded into
the model loop; production still uses the same stdout event publisher.

Separate localhost SSE tests exercise tool-free spoken synthesis from current
request/tool evidence and verify that the delegation adapter suppresses DSML
planning text across event boundaries. Explicit request handoffs control ordering;
timeouts are deadlock watchdogs, not timing assertions. Fixtures use a local dummy
key and no external provider, daemon, or runtime configuration. These tests cover
checkpoint snapshots and model adapters, not end-to-end daemon worker execution
or disk-writer durability. Existing wire/checkpoint fixtures and no-model failure
tests remain in place.

Host regression tests also inject missing, disabled, wrong-lane, unsupported and
render-failing registry bindings. A localhost review SSE fixture checks exact
review-only schema exposure and a successful correlated decision. Foreground
tests compare ordered primary schemas and memory/schedule contextual narrowing.

Final parent-run evidence reports `cargo test --workspace`,
`cargo check --workspace --all-targets`, and `cargo build --workspace` (debug)
passing. The two opt-in process fixtures and the active-root cancellation
regression were rerun individually according to earlier agent evidence. This is
supplied verification evidence, not execution by this documentation-only pass;
it does not claim all ignored tests ran. See [parallel acceptance](PARALLEL_ACCEPTANCE.md)
for exact commands, replay boundaries and remaining coverage gaps.

## Campaign Integration

The optional manifest oversight policy now funds a host-only service allocation
before root admission. CampaignService owns bounded assessment execution, durable
semantic triggers and input snapshots, stale-result fencing and an advisory outbox.
An explicit destination receives `PublishCampaignAssessment` without a second model
call or fabricated WorkResult. An assessment's attention classification is still
not authority for the separate PriorityAttn path. See the linked oversight contract
for the complete schema, supported triggers, retention and recovery boundaries.
