# Offline TUI Regressions

Run `cargo test -p tachyon-tui --offline` and `cargo check -p tachyon-tui --offline`.
PTY integration and synthetic timing benchmarks are explicitly ignored in normal
test runs. No production CLI option or fixture executable is shipped.

Unit tests live beside private production modules:

- `app/input.rs`: `Routing::dispatch`, also called by the terminal loop, exercises
  arrow press/repeat/release, capture-mode wheels, overlay draft/paste ownership,
  independent scrolling, and stacked Escape close-before-quit behavior.
- `app/scheduler.rs`: the production batch iterator accepts an injected elapsed
  clock internally. Tests check exact 256-event and 4ms boundaries, FIFO retention,
  and a queued final reply through production projection, rendering, and copy.
  The terminal loop uses the same poll/read iterator tested for 32-input yields,
  first-poll wait, zero-wait draining, tail retention, and I/O error propagation.
- `profiling.rs`: deterministic full-frame tests compare terminal cells, turn
  anchors, copied text, and layout/height-pass counters across earlier insertion,
  width/height changes, and warm redraws.
- `app/verification.rs`: an offline App constructor and a default regression through
  the complete `handle_input` method (decoded events, no terminal I/O). It checks
  40 arrows, captured scrolling, explicit Ctrl+O and Escape close-before-quit.

## Opt-In PTY Integration (Linux)

```sh
cargo test -p tachyon-tui --offline offline_pty_event_loop -- --ignored --nocapture
```

Requires `/dev/ptmx`, `setsid` and `stty`. Uses safe `nix::pty::openpty`, with `term`
enabled only as a dev dependency. The ignored `fixture_child` test is an internal
entrypoint in the libtest executable, not the production `run()` function. The
parent launches exactly that test in a separate session, connects only its stdio
to the PTY, and drains output concurrently. Parent/default tests never enable raw
mode, set environment variables globally, or read crossterm's global event queue.
Failure cleanup kills/reaps the child and joins the output drainer.

The fixture directly constructs App with default config and a temporary archive.
Status/clipboard request channels are disconnected; controls use a rejecting
executor; operational reads stay unselected; attention state is empty. A synthetic
daemon writes API-framed events to an explicitly injected Unix socket pair through
the real subscription reader/buffer and app reducer. It never calls a provider,
connects to a daemon socket path, launches daemon commands, or reads user archives.
Only the test observer and offline construction seams are `cfg(test)`; input,
rendering, scheduling, subscription cancellation, and teardown are production code.

Assertions:

- Initial 100x32 frame and native mouse mode; 40 encoded Up reports cross the
  32-input batch limit, each subtracting exactly 3 rows without opening an inspector.
  Down adds 3; an uncaptured SGR wheel report leaves chat unchanged.
- `/mouse` enables terminal capture; encoded SGR wheel Up/Down subtract/add 8 rows
  without opening an inspector. Ctrl+O opens the viewport anchor's inspector and
  its title is painted in a successfully submitted frame.
- A real 60x18 resize preserves the selected inspector. Captured wheel scrolls its
  content without changing chat position; Escape closes it, preserves chat position,
  and subsequent input/frame observations prove the process did not quit.
- Kernel window-size changes via `stty` and SIGWINCH decode as exact Resize events.
  Frames at 20x6 and restored 100x32 keep the chat turn anchor and detached reading.
  A pane survives shrinking to 20x6 and Escape hides it without quitting or moving
  the anchor. This tests manual hiding, not an automatic panel-hide policy.
- After more than 1,024 model revisions from a continuous socket flood, Ctrl+P
  opens help and produces a frame in less than 2 seconds. Escape closes help while
  revisions continue advancing. Typing `x` updates the draft and produces another
  frame in less than 2 seconds, without submitting it. The synthetic producer runs
  until cancellation; socket timeout is not accepted as successful shutdown.
- Ctrl+C exits successfully within 5 seconds, including production worker drain
  and a joined socket producer. The PTY termios equals its original value; output
  contains cursor-show, paste-disable, mouse-disable after capture-enable, and
  alternate-screen leave. Reopening the temp archive finds an applied socket reply.

State/frame assertions use a separate observation socket, not ANSI screenshot
parsing. The terminal bytes and actual termios are checked independently. Deadlines
are generous hang/responsiveness guards, not claims of 16ms/33ms latency. Probe I/O
adds overhead. Synthetic repeated arrows are trackpad-equivalent input bytes, not
hardware gesture testing. No real daemon startup/status discovery, provider,
destructive pane controls, terminal emulator presentation, or abrupt-crash recovery
is exercised. Teardown does not promise persistence of every event sent before quit.
The 4ms scheduler budget is cooperative, not preemption of a slow handler.

## Opt-In Profiling

```sh
cargo test -p tachyon-tui --offline --release synthetic_ -- --ignored --nocapture --test-threads=1
# Same debug fixture as the earlier documented transcript baseline:
cargo test -p tachyon-tui --offline app::profiling::synthetic_frames -- --ignored --nocapture
```

`synthetic_frames` retains the original transcript benchmark. `synthetic_app_frames`
measures production `App::draw`, including chat, composer and footer, on TestBackend
with the same 512-turn seed, cold/warm/scroll/alternating-width/stream scenarios.
It prints total, p50, p95 and maximum draw times, layout builds and height passes.
Only work-counter invariants are asserted; timings have no pass/fail threshold.
Both are offline, without a PTY or provider. Full App measurements exclude the
per-frame mutation/resize setup; the older benchmark includes it. Chat gets 28 of
32 rows in App versus 32 in the transcript-only benchmark. These are different
measurement scopes, not before/after speedup evidence. See
`docs/tachyon/TUI_REFACTOR.md` for measured results and limitations.
