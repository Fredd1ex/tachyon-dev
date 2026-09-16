# Local Research Acceptance

## Scope

These are representative **scripted code-path acceptance tests**, not tests of
model competence. Rust localhost fake providers drive freshly built, actual Ghost
processes through the existing campaign launch fixture, broker, scheduler, tools,
ledger, artifact store and host command gate. No API credits, user credentials,
installed daemon, UI, Python kernel or network source retrieval are involved.
The fixture supplies a fake model directly to the internal launch entrypoint;
it does not test CLI authorization or provider credential resolution.

Source: `crates/tachyond/src/runtime_store/campaign_launch/tests/acceptance.rs`.
The dependency-free failing Rust input is checked in alongside it as
`acceptance_input.rs.txt`, then copied into disposable workspaces. Neither worker
edits this repository. This is representative repair/integration dogfooding, not
an autonomous Tachyon repository change or a real research benchmark.

## Run

Run from the repository root on a trusted Linux development host with an existing
Rust toolchain (`rustc` on PATH, local linker). There are no project dependencies
to download: workers and the host verifier use the local compiler's `rustc --test`.
Cargo's offline builds still require the repository's dependencies to be cached.

```sh
cargo build -p ghost --bin ghost --offline
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_dogfood --offline -- --ignored
cargo test -p tachyond actual_dogfood --offline
```

Both cross-package tests are intentionally `#[ignore]`: the final command compiles
them and reports them ignored, rather than accidentally running a stale installed
Ghost. The explicit run requires `GHOST_TEST_BIN`; rebuild it after source changes.
An explicit path is not itself proof of freshness. No installation, version bump,
restart, credentials setup or commit is part of this procedure.

Local validation on 2026-09-16: both explicit `actual_dogfood` tests passed with a
fresh debug Ghost. `cargo test -p tachyond --offline` passed 286 tests with 28
explicitly ignored, including these two, and no failures. `git diff --check`
passed. Counts describe this working-tree run, not a release or performance gate.

## Tested Steps

`actual_dogfood_sequential_rust_repair`:

1. Launch a real root Ghost against a finite Rust HTTP/SSE script.
2. Compile and run the copied test, observing `adds` fail with exit code 101.
3. Read the workspace's actual version. At the next model request, the host uses
   `apply_exact_patch` to add an integration revision before releasing the scripted
   response. This request boundary is the deterministic conflict barrier.
4. Attempt an edit with the observed old version, assert conflict and unchanged
   bytes, re-read, then use the newly observed version for the successful edit.
5. Compile/run again and assert exit zero and `1 passed`. Publish one Rust artifact
   containing the exact repaired source and a comment report with the exact small
   replacement patch. One combined artifact matches the command gate's single
   candidate contract; unrelated publications are not silently selected.
6. Independently compile/run the immutable `candidate` in host verification staging.
   Assert Accepted, settled execution, exact SHA-256 and matching command evidence.
   Change the disposable source artifact afterward and read the unchanged retained
   bytes back from the host store.
7. Assert nine actual fake-provider responses charge 63 tokens and 63 microUSD in
   the ledger. These are scripted usage units, not money paid or measured real usage.

`actual_dogfood_distributed_evidence_and_single_writer`:

1. Admit one approved child in a separate workspace and HOME with a separate copy
   of the input. Bound depth to one, running Work to one, resident Work to two and
   total admitted Work/verifier slots to four. Root wait releases its running slot.
2. Child observes the failing test, reads and version-edits its own copy, observes
   the passing test, publishes source/patch/report, and passes host verification.
3. Root waits, requests the accepted child result, lists scoped traces with a bounded
   literal query and reads the exact returned failure-trace reference. Compare the
   retrieved bytes and full trace digest to the child's actual observed tool output,
   not a host-seeded finding or invented transcript. Reads are capped at 1024 bytes;
   this does not claim full trace ingestion.
4. Root retrieves the child's immutable artifact through scoped `history`, checks
   its exact bytes, and runs the failing baseline in its own workspace. The host
   single writer deliberately advances the root version. A stale native edit fails;
   fresh read follows, then stale host-helper integration fails and fresh versioned
   `apply_exact_patch` integrates source from the retrieved child artifact. Child
   workspace versions are never reused as root versions.
5. Root observes its integrated tests passing, asks permission to publish via native
   `work.ask`, and receives exactly one host answer through the typed attention
   endpoint. The test waits for the durable pending question, answers it once, then
   asserts one retained question/answer. No model answer or worker-authored receipt
   substitutes for the host intervention. This is publication guidance, **not human
   acceptance** of a candidate; command verification remains authoritative.
6. Root synthesizes the retrieved child source/report plus its host integration
   revision, publishes, and passes independent host command verification. Assert
   both candidates' exact snapshots remain immutable after workspace mutation.
7. Assert 17 root and seven child provider responses charge exactly 168 tokens and
   168 microUSD, with final request holds and the original shared 3000-token Work
   and 10-token protected verification envelopes unchanged.

The script caps requests at 40, HTTP request bodies at 64 KiB and headers at
16 KiB. Campaign deadline is 60 seconds, outer execution wait 40 seconds, each
compiler/test command at most 10 seconds, verifier input/output at most 4 KiB,
and campaign retained storage at most 8 MiB. Test code uses request boundaries,
oneshot completion and durable-state observation with cooperative yielding, not
sleeps to manufacture ordering. Existing production scheduler polling is unchanged.
These are logical fixture bounds, not hard OS CPU/RSS/disk/process isolation.

## Remaining Gaps

- The production operator gate now implements bounded multi-file integration with
  version checks, retained snapshots and manual journal recovery; see
  [INTEGRATION](INTEGRATION.md). These earlier dogfood scripts still use a test-local
  adapter, not that production entrypoint. General merge/git worktree management,
  multi-file atomic commit and review/apply UI remain outside the local gate.
  Advisory native locks do not fence uncooperative shell writers.
- Root-only coordinated Work/attention is now implemented without child templates.
  Its separate actual-Ghost fixture is `campaign_launch/tests/work.rs`; the two
  dogfood scripts above do not establish that coverage by themselves.
- The command gate selects exactly one candidate. Separate patch/report/source
  bundles, user-facing evidence navigation, broad context reattach and coherent
  progress/steering flows still need explicit workflow design and acceptance.
- This does not test live-source research quality, autonomous root synthesis,
  real Tachyon code changes, large fan-out, foreground responsiveness, broad
  multiworker merge quality or real-provider failure/accounting behavior.
- Physical resource enforcement, broader retention/cleanup, cross-Research grants,
  allowance changes and the remaining interfaces in [COMPLETION](COMPLETION.md)
  are still pending. **No microVM backend is implemented here.** Native execution
  is same-user and unisolated; Firecracker, guest transport and the independent
  security audit remain deferred isolation/release gates.

## Real Benchmarks

Real-model comparisons require separate explicit authorization before any spend:
approve provider/model, credentials supplied by the operator, task/data sources,
workspace/network permissions, total cost/token/time and worker limits, stopping
rules, and evaluation policy. Use matched-budget sequential/distributed conditions,
predeclared acceptance criteria and held-out evaluation where appropriate. Report
failures, uncertainty, actual billed usage and reproducible evidence, not just wins.
Do not treat `GHOST_TEST_BIN`, local fixture success or a request for dogfooding as
authorization for a paid run. No real benchmark was run and no latency, throughput,
quality, efficiency or performance improvement is claimed by this gate.
