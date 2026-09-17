# Interaction Architecture

## Current Phase: Complete

The structural refactor is complete, including bounded capability infrastructure:
the role registry, host runtime extraction, daemon messaging extraction, durable
todos and operational feed, read-only monitoring, optional TUI views, and explicit
campaign assessment. This is not an automated campaign oversight pipeline.

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

Foreground `main.rs` owns argument parsing and startup wiring; the extracted
modules above own the runtime behavior. `runtime.rs` resolves Conversation,
Foreground lane, `Primary`, then renders configured identity/persona. Prompt or
resolution failure disables the model before intake can call it and uses the
existing configured-error/turn-publication path, without a default prompt fallback.
Tool selection and each dispatch authorization consult the registry and intersect
its invocation schemas and descriptor capabilities with the four implemented host
tools. Contextual narrowing and the existing execution guards remain host policy.

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

- [Daemon messaging](daemon-messaging.md) extracts command envelopes, existing
  notifications/history projection, and agent/work subscriptions. It adds no bus,
  campaign notification policy, or new urgent publication path.
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
  none: it makes one model call and validates an internal advisory result. It does
  not fetch or refresh observations, schedule periodic inference, or mutate todos.

Normal chat gains no new prompts, default todo/monitor tools, or automatic plan
injection. Todos are structured records, not user Markdown plan persistence;
there are no plan-file watchers, export/sync, or automatic completion semantics.

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

Final parent-run verification reported the full default test suite passing and
all 30 explicitly run ignored tests passing. This records the supplied parent-run
evidence, not a new test execution by this documentation-only pass; no default-suite
test count is asserted. Service and UI fixture coverage is documented in the
linked component contracts.

## Outstanding Integration

Campaign assessment lifecycle/caller integration is a separate follow-up, not an
incomplete structural refactor: a host pipeline still needs to select when to
gather authorized snapshots, submit assessments, and handle their results. There
is no automatic campaign-to-Conversation oversight wiring, assessment notification
policy, or new urgent user-facing path. Existing messaging does not supply those
decisions merely because an assessment carries an attention classification.
