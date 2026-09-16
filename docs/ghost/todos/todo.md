# Ghost Implementation TODO

Follow-up requirements agreed 2026-09-10, reconciled 2026-09-16.
[COMPLETION](../COMPLETION.md) is the canonical source-local completed/remaining
status and evidence command record. Checked items apply only to their stated
scope, not the whole original roadmap. Unchecked items are not implemented
guarantees. See [terminology](../TERMINOLOGY.md), [API](../API.md), and
[research architecture](../RESEARCH.md) for definitions and current boundaries.

## Swarms Before Interaction

- [x] Bounded local Ghost feature milestone implemented: Python browser/artifact
  adapters, unified scoped `ctx`, final-context stopping snapshots, production
  operator integration and root-only core Work controls. No missing required
  interface found in this narrowed scope; not entire original roadmap completion.
- [x] Pass the bounded local acceptance gate: default workspace tests, all 29
  opt-in daemon fixtures in parallel and serially, and the separate Python
  browser/artifact/scoped-context fixture. See
  [current results](../COMPLETION.md#current-local-acceptance-gate). Production
  release, isolation, and independent security gates remain separate.
- [x] Durable host-internal groups over the existing campaign admission/ledger:
  explicit Work specs, immutable creation replay, atomic shared reservations,
  host-configured total/depth/root limits, ancestor concurrency, revisioned resize,
  drain-on-shrink, bounded status/list, and queued versus active cancellation.
  See [GROUPS](../GROUPS.md) for opt-in configuration and exact boundaries.
- [x] Connect group claims to existing dispatch and terminal acknowledgements to
  internal execution/evaluation transitions. Unknown and unfinished registrations
  retain slots across restart; billing settlement alone never releases capacity.
- [x] Add an explicit trusted-host catalog scheduler with Tokio tick/run, bounded
  task ownership, private Ghost launches, deferred review polling, durable callback
  errors, shared cancellation signals and cleanup. Real root-cap-two/three-Work
  overlap, shrink/drain, cancellation, and restart-without-review-replay test passes.
- [x] Private parent wait safe-point/capacity handoff (all/any/count), bounded
  root/child command repair, deferred review polling and authorized spawn catalog
  mapping. Unknown outcomes remain fenced; this is not general process recovery.
- [x] Private allowlisted `agents` status/list/result/send/steer and scoped Work
  cancel/group status/resize use typed Rust broker controls and generated Python methods.
- [x] Authorized template and bounded dynamic `agents` spawn/group, delivered
  send/steer, parent wait and compact final results through native/Python controls.
   Public depth defaults to one and is explicitly bounded to eight with inherited
   profile allowlists; at most 31 child slots are shared across depths, not multiplied
   per parent. See [AGENTS](../AGENTS.md).
- [x] Core broker `work.status`, durable `work.ask` with CLI answers/capacity
  handoff, and `work.complete` as a proposal, not verified success. See [WORK](../WORK.md).
- [ ] Finish whole-target local resource, delegation, policy, context and workflow
  gaps in [COMPLETION](../COMPLETION.md#remaining-local-work).
- [ ] Complete swarm lifecycle end-to-end acceptance before interaction refactoring.

## Campaign Interaction

- [ ] After swarm completion and the harness/campaign contracts are ready, add a campaign-agent
  role in the interaction/orchestration layer. It produces compact, high-level
  campaign updates for the Conversation agent; it does not own worker loops,
  databases, permissions, or budgets. This is an explicitly requested deferred
  role, not permission to add permanent planner/reviewer/state agents.
- [ ] Define one owner for allocation decisions per group. Prevent the campaign
  agent and Background coordinator from issuing competing control decisions.
  Tachyond remains authoritative for admission, revisions, and lifecycle.
  Local fixed/model-proposed/deterministic resize ownership and manual takeover
  are already fenced; campaign-agent/Background integration remains deferred.
- [ ] Project progress, meaningful findings, verification state, stopping reasons,
  usage, and attention requests through Conversation. Coalesce updates and keep
  raw logs externally retrievable rather than inserting them into user chat.

## Ordinary Assistant Routing

Ordinary answers, bounded delegation and follow-up reuse have existing tests.
These unchecked items retain the broader requested interaction acceptance scope;
they do not mean those baseline capabilities are absent. Explicit local campaign
authorization is implemented separately from any future conversational routing.

- [ ] Preserve ordinary direct answers and bounded worker delegation without
  requiring a Research, Campaign, Python kernel, or campaign coordinator.
  Multiple independent lookups may run in parallel without becoming a campaign.
- [ ] Reuse relevant, sufficiently fresh accepted evidence for follow-up advice.
  Fetch new evidence only when required; do not invent an unrequested forecast.
- [ ] Define optional campaign intent as a typed orchestration request, not a
  requirement that users label every query general/research. Classification is
  advisory and must not authorize expenditure or cross-Research access.
- [ ] Require explicit user authorization or an already-authorized trusted policy
  before activating a persistent campaign and its resource envelope. A keyword
  such as "research" and a large fan-out are not authorization.
- [ ] Add scripted end-to-end weather, evidence-supported coat advice, and new-city
  lookup tests: ordinary work completes without implicit campaign records; a
  requested campaign cannot execute before admission. Record routing reasons to
  distinguish fresh retrieval, missing evidence, and evidence reuse.

## Steering And Durable Lifecycle

- [ ] Add TUI steering controls using typed commands with expected revisions.
  Distinguish accepted from applied instructions at safe execution boundaries.
- [ ] Separate steer-current-work, queue-follow-up, cancel-work, pause-campaign,
  and detach-client semantics. Specify descendant cancellation versus retention.
- [x] Explicit campaign jobs are daemon-owned independently of CLI connection
  lifetime, with durable status and attention. Full TUI reattach remains pending.
- [x] Persist explicit campaign authorization, Work/dispatch intent and unknown
  outcomes; provide opt-in staging recovery and authoritative billing/cleanup
  reconciliation without replay. See [RECOVERY](../RECOVERY.md).
- [x] Explicit `campaign continue` claims a fresh attempt/process for the same eligible
   stopped-unverified root, with exact checkpoint/state fencing and idempotent command
   replay. Restore logical resource/question/child-handle context and instructions;
   validate executable pin, launched binary/version and retained activation before
   inference. Keep original funds, usage and deadline. No kernel/cell replay, child
   scheduler restart or automatic continuation. See [CONTINUATION](../CONTINUATION.md).
- [x] Reconstruct conservative resident/execution occupancy from unknown local
   execution identities after recovery; release exact retained slots once after a
   committed operator cleanup receipt with no active task. Billing is not cleanup.
- [x] Retain bounded host stopping snapshots and validated informational worker
    `final_context`, with stale/unknown fallback and host-owned output mappings.
- [ ] Complete broader resumable context/reattach, arbitrary non-output handles and
    automatic orphan/provider evidence recovery beyond operator attestation. Never
    replay unknown side effects or promise Python variable restoration.

## Budgets And Verification

- [x] Internal host-only registered execution through private Ghost broker, bounded
  candidate collection, deterministic evaluator callback, durable review state and
  final-hold-only allocation closure. Separate protected verification reservation;
  real spawned Ghost/local fake HTTP tests. The explicit CLI command evaluator
  and bounded root/child repair are now also implemented; no registry auto-execution
  or new LLM review route is implied.

- [x] Connect explicit host-authorized CLI campaign admission/dispatch to private
  per-model-request reservations. Ordinary dispatch remains outside this envelope;
  metadata creation and chat do not activate campaigns. See [CAMPAIGNS](../CAMPAIGNS.md).
- [x] Keep stable Work identity and admission/funding across fresh bounded command
  attempts; root and opted-in child retries never reset campaign allowance.
- [ ] Bound pending-index scan time and dispatch callback duration, not only batch
  size, before using the admission store for high-concurrency scheduling.

- [x] Implement bounded command-gate repair for root and opted-in children, with
  retained snapshots, protected verification, exhaustion and unknown-billing fences.
- [ ] Integrate bounded ordinary Background review rework. A Background
  `Rework` decision becomes terminal failed work rather than another Ghost
  attempt. Preserve evidence and fencing, charge retries to the same allowance,
  and do not weaken acceptance or start unbudgeted retry chains.

- [x] Implement a single daemon-owned campaign ledger before new group/delegation
  APIs. Reserve before dispatch and reconcile actual usage; preserve unresolved
  reservations across retries, cancellation, and restart.
- [x] Define exact accounting units and conservative treatment of uncertain
  provider charges. Child allocations must not multiply root allowance.
- [x] Reserve protected command verification allowance; host-authorized bounds
  and insufficient/unknown funds fence continuation without a top-up.
- [ ] Complete the wider protected finalization and authorized allowance-change
  contract; no unbudgeted synthesis or summarization chain.
- [x] Evaluate exact immutable candidates with protected command configuration;
   missing verification means unverified, not successful by default.
- [x] Add strict `json_metrics` stdout contracts with finite host bounds and exact
   candidate/config evidence. `final_heldout` forbids repair, children and explicit
   continuation, but does not prove unseen data. See [VERIFICATION](../VERIFICATION.md).
- [x] Add root-only explicit human acceptance of one retained Ready artifact, with
   state/version-fenced accept/reject and idempotent host-attributed receipts.
   Waiting holds no worker/evaluator process or model call; unknown billing remains
   owed. No child/group acceptance, repair/continuation or independently authenticated
   human identity is implied; ordinary question answers cannot accept candidates.
- [x] Add read versions and conditional `edit`/`write` preconditions plus the
   version-required `workspace::apply_exact_patch` host helper. Native writers share
   per-target cross-process advisory locks through atomic rename/sync on Linux,
   not an OS CAS against uncooperative shell writers.
   See [workspace usage](../../../crates/ghost/src/harness/tools/workspace/usage.md).
- [x] Exercise the helper in a bounded test-local single-writer root/child workflow:
   real Ghost, offline failing Rust input, observed stale/fresh versions, retained
   trace/artifact retrieval, host question answer, immutable command verification
   and actual fake-response accounting. See [ACCEPTANCE](../ACCEPTANCE.md).
- [x] Production operator integrating writer: retained multi-file exact patches,
    root state/version fencing, advisory locks, durable before/after evidence and
    journal, explicit identical-plan recovery. See [INTEGRATION](../INTEGRATION.md).
- [x] Root-only coordinated `work.status/ask/complete` and CLI attention, without
    child templates or agents grants; read-only/simple lookups remain compatible.
- [ ] General git worktree/merge workflows, actual Tachyon-change acceptance with
    reviewable interventions and arbitrary multi-artifact candidate selection.
    The implemented single-candidate contract accepts a multi-file JSON patch;
    integration is not atomic across files or proof of correctness.
- [ ] Enforce independent bounds for logical work, active execution, model calls,
  resident kernels, CPU/GPU jobs, depth, deadlines, and storage. Waiting parents
  release active permits. Preserve foreground capacity and fair scheduling among
  simultaneous Research efforts.
  Running/inference/lifetime/depth/deadline and resident-count bounds plus wait
  handoff exist. Simultaneous public campaigns share configurable round-robin
  resident-process/active-execution/model-call caps. Broker native CPU jobs now
  share a separate nonblocking try-acquire/release ceiling through exec/browser
  runners and Rust-backed Python exec; direct Python OS calls remain outside it.
   Logical GPU admission, native CPU/GPU duration holds and restart accounting are
    implemented, as is shared retained artifact/trace/managed-snapshot storage
    admission. Arbitrary Python jobs, hard resident-memory/whole-filesystem quotas
    and measured foreground protection remain outside this local milestone.
   Resident/execution identity recovery and operator release are implemented above.
   See [CAMPAIGNS](../CAMPAIGNS.md#shared-host-admission).
- [x] Add fixed/model-proposed/deterministic allocation with durable controller
   fencing and manual takeover, resize-only for admitted explicit and dynamic groups,
   including nested groups with ancestor-capacity projection.
- [x] Finite host-selected Work/Verify/Pause/Stop runtime adapters with durable
   receipts and unchanged funding. See [POLICIES](../POLICIES.md).
- [ ] Allowance reallocation and broader autonomous policy; finite adapters do not
   invent objectives, permissions or funds.

## Cross-Research Access

- [ ] Default to scope-local access. Define explicit grants for shared artifacts,
  findings, traces, and workspaces, including issuer, recipient, allowed operations,
  expiry/revocation, and provenance. Possession of an ID is not permission.
- [ ] Validate grants on the server for search, retrieval, delegation, and result
  publication. Keep scoped indexes from leaking unauthorized metadata.
- [ ] Define revocation behavior for copied evidence and in-flight operations;
  permission revocation cannot erase information already observed.

## Retention And Recovery

- [x] Publish explicitly registered exact artifact bytes outside disposable
  workspaces; retain immutable versions, provenance, failed command attempts,
  scoped authored findings, registered documents and emitted tool traces.
- [x] Export bounded retained process spools before successful Work teardown, with
   checksummed scoped history pages and original live-handle mappings; retain bounded
   broker snapshots/model-result records. These are not full model conversations or
   uncapped output. See [COMPLETION](../COMPLETION.md#local-context-retention-boundary).
- [ ] Complete broader model history/handle retention and integrate destructive
   workspace cleanup with required-evidence checks. Durable trace capture cannot
   recover output discarded upstream; cancellation/export failure records a gap.
- [ ] Make archive/tombstone the default for logical removal. Keep undo/recovery
  references and separate explicit permanent purge from workspace deletion.
- [x] Shared root/campaign retained-storage admission for artifacts, traces/uploads
   and managed input snapshots; archive/restore markers preserve data and charges.
- [ ] Define safe purge/eviction policy and backup/restore beyond bounded admission.
  Do not promise indefinite lossless retention: raw output beyond configured
  spool limits is currently discarded and must be reported as such.
- [ ] If required evidence cannot be retained, block destructive cleanup or return
  a clear failure rather than silently deleting the only copy. Crash-test staged
  publication, metadata commit, reconciliation, and workspace removal.

## Security And Isolation

- [ ] Complete and validate Ghost interface features with deterministic local fixtures,
  then implement the deferred [security contract](../SECURITY.md) in the
  isolation/release phase.
  Target Firecracker: replaceable native/microVM execution, fail-closed VM-required
  profiles, scoped channels, quotas, workspace/artifact boundaries, image verification,
  non-root operation, and host-owned cancellation/orphan recovery without replay.
  Native execution is not isolated; GPU passthrough needs a separate threat model.
- [ ] After local interface validation, resolve VM-per-Work/group/campaign placement
  against isolation, sharing grants, attribution, boot cost, and failure scope.
  Design a guest supervisor running Rust-managed process units while the host owns
  scheduling and accountable observed resources. Guest worker concurrency must not
  replace independent host CPU/memory/disk/PID/network and aggregate ceilings;
  provision and account for helpers, paused/pool capacity, and reviewed GPU limits.
- [ ] Design narrow mutually authenticated instance/Work/generation-scoped transport
  and optional guest bridge; vsock is only a candidate, not host PID authentication.
  Keep inference credentials in the host broker, forbid arbitrary guest access to
  daemon sockets, and separate bounded priority control/cancellation from logs and
  artifact traffic. Preserve host-authorized send/steer delivery semantics.
- [ ] Define authenticated boot identity and bounded host configuration/bootstrap
  separate from model prompts. Review cold boot versus sanitized warm pools using
  validated images, fresh identities/grants, and no cross-Research secrets or state.
  Keep Firecracker control non-root and KVM/network/device privilege provisioning
  separate from normal operation; no final transport/provisioning stack is selected.
- [ ] Specify launch/pause/resume/snapshot separately from actual safe application
  checkpoints. Revalidate and fence restored execution, reconcile crash/orphan
  resource evidence and unknown billing, and never replay arbitrary side effects.
- [ ] Publish validated guest artifacts to authorized `~/Agents` destinations via
  host-owned staging as immutable exports with path/link/race/size validation and
  host-computed hashes, never whole-`~/Agents`/home mounts or arbitrary guest-selected
  host writes. Retain bounded live evidence/checkpoints before final completion so
  guest loss need not erase all progress; durable exports do not imply safe replay.
- [ ] Review any future vsock dependency/platform API and its unsafe-code boundary
  before adoption. Preserve and audit first-party `forbid(unsafe_code)`; do not
  select a stack now or claim dependencies/kernel/VMM code are free of unsafe code.
- [ ] Maintain targeted security review and threat modeling during feature work,
  with CI dependency/CVE and supply-chain checks, secret scanning/redaction,
  protocol fuzzing, permissions/cross-Research denial, DoS and VM escape testing,
  lifecycle failure injection, and backup/restore verification.
- [ ] After feature completion, obtain an independent security audit as a
  release-blocking gate for the completed research/isolation features. Record actual
  scope/results, remediate blockers, and retest. Audit execution and any verification
  of "no unsafe first-party code" remain separate pending tasks, not completed claims.

## Release

- [ ] Keep the docs' CURRENT/PROPOSED labels aligned with tested implementations.
- [x] Verify the requested 0.3.0 version bump, refreshed Cargo.lock, version
  reporting, and release build. This version does not mark all harness gates
  complete. Installation, promotion, and commits remain separately authorized.
