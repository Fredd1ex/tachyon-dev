# Local Verification And Human Acceptance

Current scope: a host-configured native command evaluator over exact Ready
ArtifactStore snapshots, with an explicit host-only fail/repair/verify coordinator.
One trusted call can run a failing Ghost candidate, start a fresh repair attempt,
and accept the corrected snapshot. Ordinary chat and generic recovery do not enter
this loop. Reopening unknown state never authorizes process or Python cell replay.

## Available APIs

- Library: `tachyond::verification::{CommandEvaluator, evaluate_command,
  CommandEvidence, CommandOutcome}` and `ArtifactStore::copy_ready`.
- Internal daemon Rust API: `ModelBroker::execute_campaign_command` wraps the
  existing one-shot `execute_campaign` lifecycle and retains its no-retry semantics.
- `ModelBroker::execute_campaign_command_loop` explicitly authorizes bounded
  continuation, selecting each attempt's single host-published candidate.
- Internal observation: `RuntimeStore::campaign_command_gate(campaign, work)`
  returns the immutable binding and bounded historical command evidence.
- Internal explicit stop: `host_finalize_command_rework(campaign, work)` ends
  pending rework and settles only when billing is final. It never launches work.

The explicit [campaign CLI manifest](CAMPAIGNS.md) activates this wrapper with
protected accounting. Its optional child catalog binds the same command gate to
each exact child with one attempt by default. Optional per-child/per-profile
`evaluator` bounds enable the same bounded repair loop without duplicating root
ownership. See [CAMPAIGNS](CAMPAIGNS.md) for the manifest shape and writable native
`coding` profile policy. No model argument may override these host bounds.
There is no model-accessible public IPC grant, UI action or automatic daemon-startup
activation. The library evaluator alone does not reserve campaign money; its
trusted caller must do that.

Example host-owned command configuration (not a model tool argument):

```rust
let config = CommandEvaluator {
    acceptance_mode: None,
    result_contract: Default::default(),
    metrics: Default::default(),
    allow_extra_metrics: false,
    stage: None,
    argv: vec!["/usr/bin/cmp".into(), "candidate".into(),
               "/opt/host-verifiers/expected-report".into()],
    cwd: ".".into(),
    timeout_ms: 5_000,
    output_bytes: 8_192,
    input_bytes: 1_048_576,
    max_attempts: 2,
    max_total_command_ms: 10_000,
};
// Set ExecutionPolicy.evaluator_id to format!("command:{}", config.config_hash()?).
// Supply None to select the collected candidate, plus a private staging root.
// An explicit Ready ArtifactRegistration adds an exact-version constraint.
```

The executable must be absolute. Arguments are passed directly, without shell
interpolation. `cwd` is `.` or a traversal-free relative directory under fresh
staging, never the mutable workspace. The staged input is named `candidate`.
The staging root must be canonical, host-owned, mode 0700, and separate from the
workspace. No artifact path is used to construct a staging destination. No archive
extraction, symlink following, or worker-selected command configuration occurs.

The wrapper binds the initial Work, objective, model Attempt ID, assignment,
generation, funding, snapshot selection policy, staging root, and config.
Every selected exact snapshot is retained alongside command evidence. The loop
retains the original policy, historical executions/snapshots/evidence, and current
attempt separately. It never overwrites a failed candidate with repaired bytes.
Changing any binding conflicts, including after restart; an existing ungated
execution cannot acquire a command gate retroactively. The config hash covers
argv, cwd, input/output caps, timeout, maximum attempts and total command wall
allowance. It does not hash the executable or its dependencies: the host must
keep evaluator binaries, expected inputs, and dependencies immutable/versioned.

## Snapshot Boundary

`copy_ready` requires the exact stored registration and
`Ready { version: sha256 }`. It checks the stored object's size/type and digest,
then checks the copied bytes again. The evaluator never falls back to reading a
workspace path. Workspace replacement, deletion or mutation after publication
does not affect these bytes. The candidate must identify the exact artifact ID
and matching Work/generation/assignment; the wrapper also checks Attempt ID.

The command wrapper now publishes explicit `ArtifactRegistered` events collected
after private Ghost termination, before invoking the evaluator. It discards worker
publication metadata and candidate ID claims, validates Work/generation/assignment
and worker envelope scope, and stamps the host Attempt and worker identity (rejecting
conflicting supplied identities). The original tool registration SHA-256 and size
are required, not recomputed expectations. Descriptor-relative no-follow reads
snapshot only explicitly registered canonical workspace-relative paths. Mutating
the source after exit either preserves the registered snapshot bytes or leaves the
candidate Unverified; it cannot silently verify replacement bytes.

Identical Pending/Ready registrations deduplicate after stripping publication state;
conflicting IDs or ambiguous paths fail closed. Publication permits at most 32 unique
registrations and 16 MiB total source bytes, with two concurrent blocking jobs and
the assignment deadline. No runtime-store transaction spans publication. A cancelled
blocking copy may finish storing bytes, but cannot install candidate references.

`WorkResult.candidate_refs` is an optional host-only resolved ID list. The original
`WorkOutcome::Completed.artifacts` strings remain paths for history and legacy
workers. Every claimed path must exactly match an explicit registration; there is
no normalization, basename matching, ID-as-path fallback, workspace scan, or automatic
snapshot of an arbitrary claimed file. The command gate requires exactly one resolved
ID and stages only that immutable Ready version. Missing registrations remain
Unverified. Legacy records deserialize with absent references; no replay or implicit
upgrade grants them command acceptance.

## Results And Limits

With the default `exit_success` contract, exit zero produces Pass;
nonzero/signal produces Fail after output collection and
observed process-group disappearance. Sending a kill signal alone, including an
unreaped descendant still in the group, leaves cleanup Unverified.
Timeout, SpawnFailure, invalid/missing snapshot, collection
failure, or unknown cleanup cannot produce Accepted. The campaign phase maps Pass
to Accepted and Fail to Rejected; other outcomes are Unverified. Finishing command
review also requires matching durable command evidence. Acceptance is only under
this configured test, not general correctness. A late accepted steering revision
invalidates review of an older candidate at the final transaction boundary.

Evidence retains config hash, artifact ID, candidate SHA-256, outcome, exit code,
elapsed milliseconds, bounded stdout/stderr bytes, truncation and a fixed host
diagnostic. Raw bytes avoid UTF-8 expansion beyond the output cap. On timeout,
partial output is discarded and a bounded diagnostic is retained. If the outer
campaign deadline/cancellation interrupts the callback, detailed evidence may be
absent; this never becomes acceptance or automatic evaluator replay.

Limits: at most 256 arguments / 32768 argument bytes, 16 MiB input, 32768 retained
output bytes, 300000 ms per command, and 1..8 configured attempts including the
initial one. The total command wall ceiling must cover all configured attempts
and cannot exceed 2400000 ms. The one-shot API starts at most one attempt; the
explicit loop starts at most the configured count. Historical measured command
time (rounded up) is deducted before the next evaluation. An outer timeout caps
the remaining total allowance, including cleanup; interrupted cleanup is not Pass.
The command deadline includes staging. TERM/KILL cleanup can add roughly 300 ms;
an outer campaign deadline may cut it short and leave only Unverified evidence.
No model calls are made by the evaluator. Protected verification allowance must
be nonzero in the wrapper; existing ledger usage is finalized at zero model
tokens/cost. Command wall time is a separate host policy bound, not model billing.

Environment is cleared, then only fixed PATH, LANG and staging HOME are supplied.
There is no inherited secret environment or custom credential injection. Output
and candidate data remain untrusted; do not feed feedback back as system authority.
Snapshot filesystem work runs on Tokio's blocking pool; process I/O is asynchronous.

## Structured Local Metrics

The campaign `evaluator` accepts these additive fields. All existing command
fields remain required; omitting the additions preserves serialized configuration
and command behavior, including the existing config hash.

```json
{
  "argv": ["/opt/host-verifiers/measure-report"],
  "timeout_ms": 5000,
  "output_bytes": 8192,
  "input_bytes": 1048576,
  "max_attempts": 2,
  "max_total_command_ms": 10000,
  "result_contract": "json_metrics",
  "metrics": {
    "accuracy": {"min": 0.95, "max": 1},
    "latency_ms": {"max": 100}
  },
  "allow_extra_metrics": false,
  "stage": "development"
}
```

The trusted command reads the same immutable staged `candidate` and emits, for
example, `{"accuracy":0.97,"latency_ms":80}` on stdout. Passing requires exit
zero, confirmed process-group cleanup, and all inclusive host bounds satisfied.
The measurement source is the configured local command applied to the retained
artifact ID and SHA-256, under the evidence `config_hash`. This is not a theorem,
an unseen-data claim, or an automatically inferred statistical confidence level.
There is no training runtime or model reviewer.

`metrics` requires 1..64 named constraints. Names are 1..64 ASCII alphanumeric,
underscore or hyphen bytes, not paths. Each constraint permits only `min` and
`max`, requires at least one finite numeric bound, and requires min <= max.
Comparison uses finite IEEE-754 binary64 values, not exact decimal arithmetic;
do not use it to compare integer identities or exact accounting amounts.
`exit_success` forbids nonempty metrics and `allow_extra_metrics:true`.
Unknown fields, result-contract names and stage names fail validation.

Only captured stdout is parsed, never stderr or a worker result string. Stdout
has `floor(output_bytes / 2)` bytes; stderr retains the remainder. Truncated
stdout fails even if its prefix is valid JSON. Exactly one JSON object is required,
with optional surrounding whitespace, unique keys and finite numeric values.
Arrays, nested objects, null, booleans, numeric strings, NaN/infinity, overflow,
duplicate keys and trailing junk fail. Extra keys fail unless
`allow_extra_metrics:true`; allowed extras must still be unique finite numbers.
Stderr truncation alone does not invalidate otherwise complete metric stdout.
Failure evidence names the missing or violated configured field, or gives a
bounded parse/extra-field diagnostic without echoing arbitrary untrusted keys.

Contract, bounds, extra-field policy and optional stage participate in the immutable
config hash. Retained historical candidates and versions remain separate; compare
measurements only with the same config hash and their exact retained candidate
references, never mutable workspace paths. The host still must keep the executable
and external measurement inputs immutable; the hash does not pin their bytes.

Optional `stage` is `development` or `final_heldout`. `final_heldout` requires
`max_attempts:1`, forbids child catalogs and explicit continuation, and never
enters a repair loop. It limits exposure within this campaign, not across separately
authorized campaigns or out-of-band command calls. It does not prove the host's
data was actually held out. An absent stage preserves existing behavior.

## Explicit Human Acceptance

Human acceptance is a separate **root-only, trusted same-user host route**. It
does not run an evaluator command or model reviewer. Configure the campaign's
`evaluator` exactly as follows (all other root campaign fields remain required):

```json
{
  "acceptance_mode": "human",
  "input_bytes": 1048576,
  "max_attempts": 1
}
```

`input_bytes` remains bounded to 1..16777216. `max_attempts` must be one. Omitted
`argv` is empty and omitted `timeout_ms`, `output_bytes`, and
`max_total_command_ms` are zero. Nonempty argv, nonzero command bounds, metrics,
nondefault result contracts, extra-metric permission and stages are forbidden.
There is no `result_contract:"human_acceptance"` spelling. Human manifests with
`children` or `allocation` are explicitly rejected before launching anything;
child/group human acceptance is not supported. Human decisions cannot start repair
or explicit continuation. Command and metric defaults are unchanged.

After Ghost terminates and the host publishes exactly one bounded Ready artifact,
the host validates the retained object's digest and persists `AwaitingAcceptance`
with the candidate ID, SHA-256, config hash, generation, assignment, canonical
instruction revision and original deadline. Missing or ambiguous candidates remain
Unverified. The workspace is never a fallback. Later workspace mutation or deletion
does not change the retained candidate being accepted.

```sh
tachyon campaign acceptance campaign-<id>
tachyon campaign accept campaign-<id> --candidate <artifact-id> \
  --candidate-sha256 <sha256> --expected-state <expected_state_sha256> \
  --command-id <unique-operator-command-id> --confirm
tachyon campaign reject campaign-<id> --candidate <artifact-id> \
  --candidate-sha256 <sha256> --expected-state <expected_state_sha256> \
  --command-id <unique-operator-command-id> --confirm
```

The observation command returns the one pending root request, the retained receipt,
or neither for invalidated/unavailable input. It starts no jobs. Read the retained
artifact through the existing scoped artifact API, not a mutable workspace path.
The exact typed requests used by the CLI are:

```json
{"cmd":"campaign_acceptance_get","campaign_id":"campaign-<id>"}
```

```json
{
  "cmd": "campaign_acceptance_decide",
  "campaign_id": "campaign-<id>",
  "command_id": "operator-decision-1",
  "candidate": "<artifact-id>",
  "candidate_sha256": "<64-lowercase-hex-digits>",
  "expected_state_sha256": "<64-lowercase-hex-digits>",
  "decision": "accept",
  "confirm": true
}
```

`decision` is `accept` or `reject`; all fields are required and unknown fields are
rejected. The state hash binds the stopped execution, immutable evaluator/snapshot
binding, exact admissions and latest root host revision. Decision handling rechecks
candidate references, version, state, deadline, admission identity and cancellation
under the serialized writer. Wrong references, wrong versions, stale hashes and
superseding steering of either root or verification Work fail closed. A late decision
never revives cancelled, expired, superseded or terminal Work. An ordinary
`work.ask`/attention answer cannot make
this transition; there is no worker tool exposing the decision method.

Exactly one decision receipt commits atomically with the result. It retains the
original request and payload, `source:"human"`, daemon effective `host_uid`, and
host timestamp. IDs are bounded nonsecret identifiers and hashes, not paths or
free-text explanations; attribution is supplied by the host, not request fields.
This records an explicit operator attestation within the existing trusted same-user
boundary, **not independently authenticated human identity**. Native same-UID
workers can access local host resources; `--confirm` is not a sandbox or a defense
against a malicious process with that authority.

Replay only the identical command ID and payload after a lost acknowledgement.
It returns the original receipt without new accounting or execution. Reusing the
ID with a changed payload conflicts, and another command cannot overwrite a
terminal decision. Acceptance produces campaign `accepted_human` and execution
`AcceptedHuman`, never command `Pass` or automated `Accepted` evidence. Rejection
produces `rejected` with a human-source receipt (CLI decision output
`rejected_human`); it never starts rework. No theorem, statistical confidence or
automated correctness claim is inferred from either decision.

While waiting there are no worker/evaluator processes, execution or resident
permits, or active model calls. A lightweight campaign owner monitors the original
deadline and cancellation. The protected verification allocation remains reserved
until a final decision or invalidation. Its nonspending evaluator accounting is
recorded as final zero independently of acceptance, so no inference reservation
is held merely to await a person. Real unknown provider usage remains an accounting
obligation; acceptance does not mark it zero or release its funds. Both allocations
close only when all associated usage is final.

Cancellation, expiry and newer host steering terminate the pending acceptance as
Unverified (campaign Cancelled for cancellation), retaining the original candidate
and a bounded diagnostic visible through `campaign inspect`. Clean daemon shutdown
cancels an owned wait. After an unclean restart the durable request remains, but no
process or inference is replayed and no waiting owner is implicitly started.
Acceptance queries, status/inspection and decisions apply expiry/revision fences;
cancel can finalize a retained wait even without a live owner.

Tests use native Ghost publication and a local Rust fake provider, with no external
credentials, Python runtime, or real daemon startup:

```sh
cargo test -p tachyon-api -p tachyon -p tachyond
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond \
  actual_ghost_human_acceptance_typed_dispatch_no_pending_model_or_execution -- --ignored
```

## Repair Lifecycle

A failed command with `max_attempts > 1` retains both original allocations after
all associated billing holds are Final. `ExecutionRecord.rework_pending` becomes
true, with the rejected candidate and bounded command feedback retained. Unknown
or provisional billing blocks this transition; unknown launch/review state never
authorizes rework. With limit one, rejection settles normally. Explicit host stop
allows final closure. Settlement alone never generates a retry or replenishes funds.

Only the explicit loop may compare-and-set a rejected, pending execution into a
fresh `ExecutingUnknown` attempt. The transaction checks final billing in both
allocations, remaining model and protected verification allowance, cancellation,
capacity, deadline, command wall allowance, and exact latest accepted/applied
revision. It archives the prior attempt and commits a fresh Attempt ID and increased
assignment ordinal before launch. Work, objective, campaign, admission generation,
dispatch/funding identity, model estimate and pricing remain unchanged.

The original dispatch-keyed process claim is never removed. Replacement process
claims use dispatch plus Attempt ID; verification receipts are likewise attempt
scoped. Replacement permits require the previous launch lease to be closed and
retain the same allocation. Each provider request still reserves and claims once
against remaining Work funds, not root headroom or the verification pool.

Ghost receives optional typed `WorkRequest.attempt` identity and host-generated
feedback. Feedback contains the previous candidate reference/hash, evaluator config
hash and observed result; stdout and stderr are each limited to 512 bytes for this
context. They remain untrusted data, not system instructions. Tool event identities
and `WorkResult.attempt_id` propagate the attempt; host collection checks both the
attempt and assignment fences. Each process starts a new transcript and kernel;
the shared workspace is available, but prior arbitrary Python cells are not replayed.
Current steering is reconstructed through the existing broker boundary. A newer
accepted revision racing verification makes the candidate Unverified, never Accepted.

Unknown process/review state, unknown billing, unapplied steering or unavailable
capacity parks rather than launches. The child scheduler polls known deferred
review/rework under its original deadline, retaining logical nonterminal status
while releasing running capacity. `agents.result` projects repairable rejection
as `rework_pending`, including while final billing is missing. A parent's bounded
wait can return partial outstanding results without authorizing another attempt.
Explicit host continuation may revisit a known rejected attempt after a blocker
clears; unknown attempts require separate authoritative host recovery. Cancellation,
insufficient remaining funds, deadline and total-command exhaustion stop continuation;
known rejection settles only when billing is final. Max-attempt exhaustion likewise
settles rejection while preserving all candidates. Additive fields default absent
or empty for persisted one-shot records; the original API still never retries.
New command bindings persist the initial caller/Work deadline so resume cannot extend it.

Dynamic inputs are prepared once, not recopied between repair processes. The same
workspace retains candidate outputs; original read-only input files and proposal
quotas are unchanged. Native write/edit reject replacement of read-only files.
Every artifact publication is still a fresh attempt-scoped immutable snapshot;
archived attempts retain their old bytes and exact-version history references.
`agents.result` selects the current execution by instruction revision, not an
attempt ordinal; use the retained history capability for older attempt snapshots.

## Native Trust

This is native trusted-evaluator execution, not a security sandbox. Read-only
copied inputs and a private directory do not isolate a hostile same-UID process.
The evaluator can access host files/network; commands that execute candidate code,
load candidate plugins, or source candidate scripts are not safe to configure.
Process groups cannot contain descendants that deliberately escape their group.
Ghost and verification share `tachyon_util::process` TERM/KILL supervision without
importing Ghost private code or adding a production Ghost dependency to tachyond.
Filesystem/CPU/RSS/network isolation and microVM execution remain separate work.
The Firecracker target and feature/local-fixture then isolation/release sequence
are documented in [SECURITY.md](SECURITY.md); no VM backend is implemented here.

## Local Checks

```sh
cargo test -p tachyond verification::tests
cargo test -p tachyond command_rework_pending
cargo test -p ghost harness::tools::exec::tests
cargo test --workspace
cargo build -p ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_execution_review_and_settlement -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_command_publishes_registered_candidate -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_command_loop -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_campaign_children_repair_final_results_and_exhaustion -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_scheduler_pending_repair_resize_cancel_and_total_bound -- --ignored
```

Command tests run real local subprocesses over Ready snapshots after workspace
mutation, cover rejection/spawn failure/timeout, invalid references, bounded
output, config hashing, pending rework, unknown billing, limit-one closure,
immutable policy conflicts, explicit stop, and reopen without replay. Shared Ghost
exec tests cover cancellation, descendant cleanup, dropped futures and secret
environment scrubbing. The existing ignored actual-Ghost test uses localhost fake
model HTTP and the original deterministic callback. The actual-Ghost command test
uses native exec and artifact tools to produce Pending registrations, publishes
them on the host, and verifies the exact candidate. It covers successful first-attempt
settlement, post-publication workspace mutation, stale registration hashes, and
missing registration. The command-loop test runs real fail/correct/pass and exhausted
two-attempt paths over localhost model HTTP. It also covers concurrent calls,
insufficient remaining funds, unknown billing, cancellation, first/replacement
process timeout and reopen without replay, evaluator config conflicts, revised
instruction delivery and steering racing final acceptance. No paid provider is used.
