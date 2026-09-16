### Execution
Use `exec` for bounded workspace processes. Prefer `argv` for direct execution.
Use native `read`/`grep` for straightforward inspection; Python fits retained
analysis and programmatic iteration. Combine process steps only when useful.

Supply exactly one of `argv` or `command`. `command` invokes the configured
non-login shell only when policy allows it. Set workspace-relative `cwd` per
call; shell directory changes do not persist to other calls. Timeouts and output
are bounded by policy. Check exit status, termination, stderr, and truncation;
do not treat partial output as proof of success.
Bound producer output as well as returned pages. For bulk extraction, sample
unknown structure first and count/report skipped inputs and errors. Use `ctx`
continuations for retained output; discarded output leaves a coverage gap.

The default `action=run` preserves direct invocation behavior. `action=start`
accepts the same invocation and immediately returns a pending `exec:...`
operation plus stdout/stderr `output:...` references. Start acknowledges a
supervisor task, not successful spawn. Use `action=status` or `action=wait` with
`operation` to observe completion, spawn failure, exit/signal, timeout, or cancel.
Wait defaults to 1000 ms and accepts `wait_ms` up to 30000; a wait timeout does
not stop the process. `action=cancel` requests termination; wait for `done=true`
to confirm cleanup. `action=output` accepts operation, stream (stdout default),
byte cursor (0 default), and limit (8192 default/max). `ctx` navigates the same
outputs. Output currently retains a prefix per stream, with discarded bytes
reported explicitly, not an unlimited log.

Async actions require a per-work registry. References cannot cross works and
do not survive restart. Work end cancels active operations. Storage reserves at
most min(policy max_exec_output_bytes, 64 MiB) per operation, split equally by
stream, with 128 MiB and 256 stream entries per work. Capacity is retained until
work end; exhaustion rejects new starts. Execution is local, not a sandbox.

Broker work shares host CPU/GPU-job permits. Pending may mean waiting for capacity;
the original timeout includes that wait. Release follows confirmed native cleanup,
not the start acknowledgement. Ordinary local exec is explicitly ungated.

### Workload Admission

`workload` optionally selects `{"class":"cpu"}` (default) or `{"class":"gpu"}`
for run/start. GPU requires an explicit host grant and takes no device IDs or count.
The host selects one logical device and sets `CUDA_VISIBLE_DEVICES` as a scoped
hint, not an OS access restriction. Broker-native CPU/GPU calls reserve their
bounded job wall time before spawn; budgets and profile timeouts may deny a call.
Python Rust-backed exec uses the same hook; arbitrary Python `!shell` does not.
