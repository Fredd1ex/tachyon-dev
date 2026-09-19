# Parallel Interaction Acceptance

These are local, scripted acceptance fixtures, not evidence of model competence or
benchmark performance. They use production functions and processes for the
guarantees below. HTTP model responses, explicit host/operator inputs, and the
completed-delegation replay seed for the second fixture's main synthesis step are
scripted. This is not live end-to-end delegation for every part of the interaction.
No provider credentials, installed daemon, service restart, package
installation, system clipboard, or repository mutation by a worker is required.
Daemon unit-test builds send diagnostic JSONL to an anonymous temporary file;
production logging is unchanged.

## Fixtures

`parallel_acceptance_foreground_services` starts a real `tachyon-foreground`
process with an empty environment, temporary HOME/config/data/workspace, a fixed
noncredential API key, and an ephemeral loopback HTTP provider. Its private Unix
socket runs `handle_connection`, including peer authentication and typed daemon
dispatch. The schedule coordinator invokes the production Background schedule
validator, not an unconditional approval stub. Todo calls use the durable daemon
facade, not `TODO_SERVICE` or an in-memory implementation.

The host holds the first turn's second HTTP request after the foreground has
actually emitted `I'm looking into it.` and listed its empty plan. While that
request is held:

1. An independent question about `WorkerState` is classified and answered.
2. A reminder for tomorrow at 15:00 is committed through the typed schedule tool.
   The test inspects the stored reminder before releasing `Scheduled.`
3. A conversation todo passes through pending, in-progress, blocked, and completed
   using the real service's IDs, revisions, and authenticated actor.
4. Each independent final is published while the checkpoint cursor remains at 1.
   Releasing the first response commits all seven turns in admission order.
5. Campaign listings are unchanged, and no delegated Work is created by these
   simple paths.

`parallel_acceptance_campaign_attention_oversight` repeats that same interaction
path with a shared runtime store and an explicitly host-authorized campaign. The
manifest, funding, executable, and evaluator are supplied by the host fixture;
user prose is **not** treated as authorization. The actual campaign executor
launches actual Ghost root and child processes. It checks:

1. Workers are active and blocked at owned HTTP requests while independent
   conversation, scheduling, and todo turns complete.
2. A real retained worker trace supports an authored finding. Its durable trigger
   causes budgeted Campaign oversight, whose validated advisory is delivered via
   the actual outbox, foreground command decoder, and publication/history path.
3. The two assessment calls, including launch assessment, have exactly two final
   ledger charges of 16 tokens and 16 micro-USD each. A third assessment is not
   dispatched after the limit, including during subsequent work transitions.
4. Before the root's real `work.ask`, the foreground calls `spawn_agent` against an
   explicitly seeded completed-delegation replay record. Production idempotent
   dispatch and `WorkSubscribe` return its scripted evidence. The fixture verifies
   the next HTTP request has the exact dedicated synthesis prompt plus evidence
   guidance and no tools,
    then holds that request while attention is published as a separate standalone
    notice, without cancelling the model request. Typed operator acknowledgement
    requests reconnect to the private socket. Repeated display/ack requests and repeated publication frames do not
   add UI notices or history. Transport is at-least-once: the same attention frame
   may replay, but its identity and membership do not change. Reconnect's cached
   usage replay is distinguished from a new notice.
5. A new user turn drives the real foreground `campaign` tool: list linked campaigns,
   query exact Work IDs, steer the speculative child, poll status, and cancel a
   separate queued branch using its returned generation. The
   coordination receipt first has accepted revision 2/applied revision 1. After
   the actual Ghost model boundary it has accepted/applied revision 2, and the
   next model request contains the new instructions. No test-only steering ack
   is used. A second status query confirms application before the foreground says
   it applied; cancellation status confirms cleanup before it says cancelled.
   The root and speculative child's cancellation flags remain unchanged.
6. The root publishes its candidate and passes the real bounded command evaluator
   (`grep -qx 42 candidate`). The test checks the settled execution, successful
   exit code, evaluator configuration hash, immutable candidate hash, and retained
   artifact ID. The real collected work evidence and advisory reach the foreground
   checkpoint before its final answer is released.

HTTP request/socket ownership establishes the concurrency barriers. There are no
50 ms sleeps or timing-based success assertions in these fixtures. Storage-owner
completion is observed with bounded yielding loops; production services retain
their own polling intervals. Every wait has a timeout. Campaign shutdown and
foreground process cleanup run before temporary data is removed.

## UI Evidence

The campaign fixture captures the **daemon subscriber's** typed interaction
envelopes, including host-attached attention membership, rather than inventing a
parallel UI transcript. It compares the normalized capture with:

- `crates/tachyon-tui/tests/fixtures/parallel_acceptance.json`
- `crates/tachyon-tui/tests/fixtures/parallel_acceptance.screen.txt`
- `crates/tachyon-tui/tests/fixtures/parallel_acceptance.copy.txt`

The first file is consumed by `parallel_acceptance_ui_pipeline`. That test runs
user submission/reservation, turn qualification, event projection, attention
receipt persistence/reopen, turn grouping, the real conversation renderer, and
copy-key selection. It checks the first turn remains pending after the independent
answer and overlays; it checks one attention notice after replay/reopen. Rendering
runs at 54 and 120 columns with stable repeated redraws. The 120-column rendered
buffer and selected-cell copy text have checked-in expected snapshots. The copy
callback captures bytes without invoking the OS clipboard or file fallback.

Normalization changes timestamps, generated IDs, campaign IDs, and finding hash
references only. The rendered clock is fixed at zero and the turn icon is mapped
to ASCII `^`. TUI tests use default names rather than reading personal config.
The optional launch advisory is omitted: it may legitimately be stale if root
admission changes its input fence. Its assessment call is still counted and
charged; the finding-triggered advisory must be published.

## Run

Run from the repository root on Linux as an unprivileged user. Build explicitly;
the tests never invoke Cargo and never select an installed executable implicitly.
The paths must refer to this fresh build, not a previously installed version.
The source reads `FOREGROUND_TEST_BIN` and `GHOST_TEST_BIN`, not `FGO_TEST_BIN`.
Below `FGO_TEST_BIN` is only a shell convenience explicitly mapped to the real
foreground test variable. The active-root regression additionally requires an
existing IPython installation; these commands do not install it.

```sh
cargo build -p tachyon-foreground -p ghost
FGO_TEST_BIN="$PWD/target/debug/tachyon-foreground"
FOREGROUND_TEST_BIN="$FGO_TEST_BIN" \
GHOST_TEST_BIN="$PWD/target/debug/ghost" \
cargo test -p tachyond --bin tachyond parallel_acceptance_foreground_services -- --ignored
FOREGROUND_TEST_BIN="$FGO_TEST_BIN" \
GHOST_TEST_BIN="$PWD/target/debug/ghost" \
cargo test -p tachyond --bin tachyond parallel_acceptance_campaign_attention_oversight -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" \
cargo test -p tachyond --bin tachyond conversation_cancel_interrupts_active_root_without_fabricating_cleanup -- --ignored
cargo test -p tachyon-tui --lib parallel_acceptance
```

Each named daemon command explicitly runs its corresponding ignored fixture;
the root-cancellation regression is separate from the two parallel fixtures.
Without `--ignored`, these process tests are skipped. A broad default
`parallel_acceptance` filter also includes the imported schedule validator's
unit test; its success is not evidence that either process fixture ran. Missing
binary variables fail an explicitly requested process run rather than silently
passing it. No binary timestamp/freshness check is claimed.

Optional capture output goes only to an explicitly supplied, already-existing
directory. For example, create `/tmp/opencode/parallel-acceptance`, then add
`PARALLEL_ACCEPTANCE_CAPTURE_DIR=/tmp/opencode/parallel-acceptance` to the commands
above. Runtime captures never overwrite the checked-in expected fixtures.

Additional regression commands:

```sh
cargo test -p tachyon-tui --lib
cargo test -p tachyon-foreground
cargo test --workspace
cargo check --workspace --all-targets
cargo build --workspace
```

## Limits

- The independent service turns initially run during a held tool-follow-up request.
  Attention and conversational steering then run during the dedicated
  `synthesize_spoken_response` pass after `spawn_agent`. The completed delegation
  is a scripted replay record, not a newly launched legacy worker. Campaign root
  and child execution still use real Ghost processes. The scripted final response
  is not evidence that a model incorporated updates arriving after its request began.
- `WorkerState` is an independent conceptual question, not a model-mediated live
  worker-status lookup. Actual worker activity is asserted separately from the
  campaign service.
- Model interpretation is scripted, not evidence of natural-language competence.
  The Conversation-to-operator endpoint, typed selectors, real accepted/applied
  boundary, and rendered confirmation are covered. Raw receipts remain tool data,
  not an additional UI receipt panel.
- This does not boot the installed daemon/background service, emulate a terminal
  session over a PTY, or test actual clipboard delivery. The production schedule
  validator is called in a test coordinator thread.
- The checked-in capture covers interaction publications and the working
  acknowledgement, not the complete worker telemetry, monitor, and todo-panel
  feeds or every field of the live execution. Those have separate component tests.
- The command verifies only the scripted candidate's contents. It does not run a
  performance benchmark or establish that a substantive investigation succeeded.

## Recorded Verification

The ack/observer/synthesis regression review reran both opt-in parallel fixtures
with freshly built local foreground and Ghost binaries. Both passed after fixing
the exact synthesis-guidance assertion and scoping acknowledgment capture to turn
1 rather than the last acknowledging turn. The existing interaction JSON, rendered
screen, and copy snapshots were unchanged. Foreground (82), background (21), model
(31), TUI (151), daemon library (29), and daemon binary (339 passed, 33 ignored)
tests also passed. These are local fake-provider checks, not real-provider claims.
An initial combined host command timed out after compilation; the complete daemon
binary suite passed in its bounded rerun. Other ignored process fixtures were not
rerun as part of this review.

Final parent-run evidence reports the default `cargo test --workspace` suite,
`cargo check --workspace --all-targets`, and debug `cargo build --workspace` all
passing. Earlier agent evidence records the two opt-in process fixtures and the
active-root cancellation regression rerun individually with fresh local binaries,
and the TUI fixture passing. These are reported implementation/parent results,
not fresh executions by this documentation-only reconciliation.

Default-suite success does not mean ignored tests ran. No claim is made that all
33 ignored tests were executed, and no aggregate test arithmetic is asserted.
The individual commands above make the opt-in evidence reproducible.

An earlier foreground negative-input assertion failed during implementation;
the subsequent review restored assessment identity checks and reran the suite
successfully. That historical failure is not the final workspace result.

An isolation issue was discovered during development: earlier runs used the
existing daemon test logger, which could append fixture events to the normal
diagnostic log. The test-only temporary log sink above fixes subsequent runs.
Existing logs were not deleted or rewritten.

The acceptance boundary remains real foreground and Ghost processes, real typed
IPC and durable services, real command-based candidate review, and fake localhost
model HTTP responses. The main synthesis delegation seed is replay, not a newly
launched live delegation; passing fixtures do not establish model competence,
universal raw-secret redaction, or interruption of foreground inference by attention.
