# First-party safety audit

Snapshot: 2026-09-12. Scope is the 13 Cargo workspace packages under `crates/`,
including untracked first-party additions. Dependency sources, `.cargo`, registry,
`target`, external and third-party code are not part of the safety claim.
Lifecycle files were being edited concurrently; line references describe the
inspected snapshot, not a frozen revision.

## Rust

All 17 library/binary roots already had `#![forbid(unsafe_code)]`, including
`tachyond/src/lib.rs` and its artifact-store module. The separate Ghost
`broker_bootstrap` integration-test root was missing it and is now guarded.
The new regression-test root is also guarded. Cargo metadata reported no extra
bins, examples, benches or build scripts; the first-party build-script glob also
found none.

First-party Rust content searches found no `unsafe` keyword, unsafe block,
function, trait, impl, extern declaration, unsafe attribute, `no_mangle`,
`export_name`, `link_section`, or `include!` invocation. Matches for `unsafe`
as a substring were the `unsafe_code` lint attributes, not unsafe operations.
No first-party filenames containing `unsafe` existed before adding the policy
test. Filenames and comment/string matches are not evidence of unsafe code.
Ordinary Rust modules inherit the crate-root prohibition.

This does not mean the executable or dependency graph contains no unsafe Rust:
Tokio and other third-party crates use unsafe internally. Compiler checks cover
the selected target/features and expanded code, not every possible platform.

## Async Findings

Targeted follow-up after the resident wait lifecycle implementation, 2026-09-12:

- Fixed: scheduler `tick`, `reap`, `dispatch` and launch status checks offload
  bounded storage phases with `RuntimeStore::storage` / `spawn_blocking`.
  Completed outcomes remain in the scheduler until persistence succeeds.
- Fixed: execution initialization, evidence CAS, review claim, evaluation
  reconciliation and settlement run on blocking workers. Original transactions,
  expected-state comparisons and atomic commits are preserved. No transaction
  or authority guard crosses an await; evaluator and network futures remain on
  Tokio, with no nested `block_on` or whole-lifecycle blocking task.
- Fixed: native launch validation, permit issuance and durable launch claim run
  in one bounded blocking preparation phase. Post-channel status and terminal
  accounting also run off-thread. Resident wait suspend/resume retains its
  authority-lock CAS ordering on blocking workers; its error cancellation now
  offloads storage too.
- Fixed: normal native cleanup and `LaunchGuard::drop` kill the process group
  first, then close a per-launch `PermitLease` atomic fence without acquiring
  authority or storage locks. The lease exists before asynchronous preparation,
  so cancelled preparation cannot leave a late-issued live grant. Every grant
  authority check (inference, control, wait suspend/resume) checks this fence.
  No queued revocation task is needed. Closed historical grants remain for
  exact-capability replacement and late billing reconciliation; they are not
  fresh authority and cannot simply be deleted without losing those semantics.
- Preserved: reservation holds authority across the DB race. A claim that passed
  authorization before closure may still commit an unknown hold. The broker
  rechecks deadline and launch liveness after reservation before HTTP; dropping
  the private channel future cancels its provider I/O. Closure does not cancel
  an already-winning claim, prove zero spend, or refund unknown billing.

Residual limits:

- Started blocking storage jobs cannot be preempted. Writer waits and durable
  commits can outlive timeout, disconnect or cancellation, and can delay runtime
  shutdown. Phases are bounded in work, not wall-clock commit time. They never
  launch a process, evaluator or HTTP request themselves. Tokio's blocking pool
  is shared, not a dedicated storage admission queue.
- Synchronous host-only store APIs (including scheduler construction/outcome
  lookup and explicit host revocation) remain synchronous. Callers outside these
  async lifecycle paths must offload them when appropriate. Short launch-registry
  mutex scopes remain synchronous but do not hold a DB writer or cross awaits;
  the scheduler's potentially storage-contended catalog snapshot is offloaded.
- Private listener temporary-directory setup/cleanup and native process spawning
  remain synchronous filesystem/OS operations. Framing, socket I/O, timers and
  child waits are asynchronous. Cooperative evaluators must not block Tokio.
- Kill/reap guarantees here apply to the existing Linux native process-group
  launcher, not containment of hostile code that escapes that group. A future
  microVM backend needs a separate trusted VM kill/termination API and its own
  lifecycle validation. No VM implementation or broad new audit is included.

## Python

The only first-party Python source found is
`crates/ghost/src/harness/tools/python/kernel.py` (175 lines): the IPython-side
execution and native-tool bridge shim. Host runtime, scheduling, policy and
native tools remain Rust. No Python was added for this audit.

Python proxy method lists, owner-thread checks and `busy` flags are convenience
checks, not an isolation boundary. Cell code can introspect Python objects and
function globals (including the control socket), and IPython allows direct
Python/shell execution under the worker's OS permissions. Do not treat proxy
checks as containment of hostile code or its output as verified evidence.

Rust host verification does exist: `python/bridge.rs:244-273` checks request and
sequence IDs, call limits/deadlines, allowed operations, prior package require,
current package policy and dispatch through `registry.execute`. It cannot prove
that frames originated through an unmodified Python proxy or that a reported
cell success represents independently verified work. OS sandboxing and trusted
host evidence verification remain separate requirements.

## Regression commands

```sh
cargo test --offline --locked -p tachyon-api --test unsafe_policy
cargo check --offline --locked --workspace --all-targets
cargo clippy --offline --locked --workspace --all-targets -- -D unsafe_code -W clippy::await_holding_lock
```

The dependency-free policy test uses existing `serde_json` and Cargo metadata to
check every workspace target root, including future custom bins/build scripts,
without traversing dependencies. It intentionally requires the prohibition as
the first line, so comments/strings cannot satisfy it. Rust compilation, not a
naive token-search test, enforces the prohibition in active code. Clippy's lock
lint does not detect every synchronous blocking call in an async function.

Verification during the initial audit (before the lifecycle follow-up):

- Policy integration test: passed (1 test).
- `cargo clippy --offline --locked -p tachyon-api --all-targets -- -D unsafe_code -W clippy::await_holding_lock`:
  passed, with existing `large_enum_variant` and `derivable_impls` warnings.
- Workspace all-target check: blocked by concurrent lifecycle changes. The
  observed errors were missing `ModelBroker::wait_private` in `permits.rs:107`
  and missing `WorkLimits::max_resident` initializers in `coordination.rs:487`,
  `broker_tests/catalog_tests.rs:136` and `broker_tests/execution_tests.rs:64`.
  These were not repaired as part of this audit. Full-workspace Clippy was not
  run after the check failed.

Verification after the targeted lifecycle fixes (Cargo commands run sequentially):

- `cargo test --offline --locked -p tachyond -p tachyon-model -p ghost`: passed.
  Daemon: 160 tests; model: 24; Ghost: 122. Seven opt-in daemon tests are excluded
  from this default invocation.
- Fresh `cargo build --offline --locked -p ghost --bin ghost`, then
  `GHOST_TEST_BIN=$PWD/target/debug/ghost cargo test --offline --locked -p tachyond --bin tachyond -- --ignored --test-threads=1`:
  all seven real-Ghost localhost fixtures passed, including Python resident wait.
- `cargo check --offline --locked --workspace --all-targets`: passed; the initial
  concurrent-lifecycle blockers above no longer apply.
- `cargo clippy --offline --locked -p tachyond --all-targets -- -D unsafe_code -W clippy::await_holding_lock`:
  passed, with existing style warnings and no await-holding-lock diagnostic.
- New current-thread contention regressions cover scheduler/execution heartbeat
  under a deliberately held redb writer, group kill/Drop with authority and store
  locked, cancelled late launch preparation, and a late committed unknown model
  claim that cannot start HTTP after lease closure. The normal deadline cleanup
  regression also holds authority/storage until the worker has been killed.
  Test lock holders use watchdogs and are joined; the late preparation test drains
  through its durable claim rather than relying on detached shutdown cleanup.
