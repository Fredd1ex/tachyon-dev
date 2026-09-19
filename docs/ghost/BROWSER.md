# Ghost Browser

Ghost exposes a restricted agent-browser CLI with Lightpanda as its only engine.
It does not add Chromium, a browser chat model, WebMCP, plugins, or remote browser
providers. Browser content and linked instructions are untrusted input.
This is URL reading and browser automation, not a full web search engine.

## Reading and Help

Prefer `read URL --outline`, `read URL --filter TEXT`, or
`read URL --llms index --filter TEXT`, then retrieve one relevant page. Use the
rendered-tab `read` form for authenticated or client-rendered content. Use
`open`, `snapshot -i -c`, current refs, and fresh snapshots only when interaction
is needed. URLs must be explicit HTTP(S), without embedded credentials.

Use a supported command's `--help` for the compiled-in scoped command guide.
No skill discovery is required. Compatibility aliases `skills list` and
`skills get core` serve the same guide. The release
binary does not contain upstream's skill files, so Ghost supplies this local
bundle directly from compiled-in text without a CLI subprocess, scratch files,
provisioning, npm, network access, or runtime skill downloads. Supported commands'
`--help` also returns this scoped guidance, not upstream's unrestricted help. This is not an
unmodified upstream skill pack. Requests for multiple skills, `--all`, `--full`,
and filesystem skill paths are rejected. `--llms full` is also rejected.

Commands and flags are allowlisted. Config files discovered in the workspace or
user home cannot enable providers or plugins: each invocation supplies a private
empty config and fixed Lightpanda settings. Host domain/action restrictions are
preserved; model-provided overrides are blocked. Each work registry uses a private
session ID rather than adopting an existing host default session. Registry clones
share that work's session; separate works use different sessions. Direct calls
without a work registry fall back to the tool instance's private session ID.
Sessions have a 60-second daemon idle timeout. The tool uses the existing exec
runtime's deadline, cancellation, process-group cleanup, and output capture.
The operation ceiling is 60 seconds and 8 KiB captured output, including help.
Setup has a separate wait budget of at most 120 seconds. Both phases respect the
remaining assignment deadline and host policy; operations also respect the host
exec ceiling. Explicit read/wait timeouts are 1..55000ms and are clamped below
the remaining operation budget by the TERM grace plus one second. Repeated
`--timeout` flags are rejected. Numeric `wait MS` durations are also clamped:
upstream ignores a separate `--timeout` for that form. HTTP reads
default to at most 25000ms per request; multiple upstream discovery requests
still share Ghost's outer deadline. This replaces the former 20-second ceiling,
which could kill operations before upstream's default 25-second error response.

The native schema takes one quoted `args` string, not an argv array. Targeted
`get text/html/value/attr/title/url/count/box/styles` forms are validated; CDP URL
disclosure is not exposed. Known `goto`/`navigate`, `quit`/`exit`, and `scrollinto`
aliases canonicalize before validation. Unsupported engine/page operations still
fail without fallback; exposing `box` or `styles` does not promise layout fidelity.
Operation errors retain the original diagnostic and structured exec termination,
with a `browser_failure_kind` hint. Deadline/cancellation come from the runtime;
network and compatibility hints are conservative text-based classifications, not
proof of a remote cause. Missing executables and provisioning failures remain
`dependency_unavailable`; ordinary command failures do not claim tool absence.

Work completion/failure/cancellation awaits a scoped `close` via the generic tool
lifecycle hook. Dropping/replacing the last work registry schedules the same
cleanup. Only works that attempted session commands are recorded; untouched,
skills-only, help-only, unavailable, and rejected-input work does not launch a
cleanup CLI. A successful explicit `close` clears the record. Cleanup never uses
`close --all`, a default host session, or another engine. It retains the private
empty config and fixed Lightpanda environment until close completes, using a fresh
cancellation token and three-second execution deadline (five-second host ceiling).
An unsuccessful command is still recorded because it may have started a daemon.

## Provisioning

Startup, package discovery/activation, and interface/help requests do not provision
or start browser resources. The first actual browser command performs setup on a
blocking worker; failures return a structured dependency error without fallback.
Setup uses `TACHYON_HARNESS_TOOLS_DIR`, or the Tachyon data directory's `tools`
subdirectory. No root privileges, package manager, install script, or host PATH
browser discovery is required. `TACHYON_AGENT_BROWSER_BIN` and
`TACHYON_LIGHTPANDA_BIN`, when requested, must be absolute executable paths.
Invalid overrides fail closed; they are never replaced with managed or host
binaries. Overrides are operator-trusted executables, not a sandbox boundary.

The CLI remains pinned to **0.35.0**, including an exact version check. Managed
Lightpanda is pinned to tagged release **0.4.1** and mandatory published SHA-256
digests inspected on **2026-09-18**. Supported automatic Lightpanda targets remain
Linux x86_64 and macOS arm64. No runtime latest/nightly discovery or fallback occurs.
GitHub reports `immutable: false` for this release: the tag is not an upstream
immutability guarantee. The checksum fixes accepted bytes even if an artifact is
replaced; a missing or changed artifact fails closed.

Provenance: [official release metadata](https://api.github.com/repos/lightpanda-io/browser/releases/tags/0.4.1)
and [tagged release](https://github.com/lightpanda-io/browser/releases/tag/0.4.1).
The following are upstream GitHub `digest` fields, not locally computed hashes:

| Target / artifact | Asset ID | SHA-256 |
| --- | --- | --- |
| Linux x86_64 / `lightpanda-x86_64-linux` | `565737341` | `1d40801e72c0bc61b2cbd3f3562bcfc46de7b79e0568f33f686b64f2e587610a` |
| macOS arm64 / `lightpanda-aarch64-macos` | `565729192` | `99e67739ed8cf5b985af7cbfa7c76b2bab257b171b2dad21109bd74b4f3bb510` |

The pre-change Linux pin was exactly the screenshot's asset `551831859` with
digest `50533da8fb42505479cec086291695949c67169ff8840deb259a6bd253b6169b`.
Its [metadata endpoint](https://api.github.com/repos/lightpanda-io/browser/releases/assets/551831859)
returned HTTP 404 during this inspection. The current `latest` release metadata
points to mutable `nightly`, with Linux asset `571659731`, not that pinned asset.
The old macOS arm64 asset `551818190` metadata also returned HTTP 404.
This establishes an unavailable old pin and changed nightly assets, not the exact
time or reason for upstream deletion. Repeating setup could not repair that pin.
The old success-only cache retried setup after each failure and marked it retryable.

Failures now have a process-local negative cache under the same setup lock.
Its key includes canonical tools root and directory identity, overrides and their
executable and verification-stamp metadata (including non-executable files), version/engine/output policy,
and artifact URL/digest. Manual replacement or configuration/pin changes permit a
fresh check; unrelated roots are isolated. Only one failure is retained per canonical
tools root, not a history of configurations; changing configuration discards the old
entry, and success clears it. The number of distinct operator-selected roots is not
globally capped. Expired entries are pruned on setup access. Entries expire after one hour for
permanent failures, or 60 seconds for transient failures, and process restart
clears them. No failure state is persisted outside the process. Concurrent and
sequential unchanged calls reuse the failure instead of repeating installation.
HTTP 404, checksum failures, unsupported platforms, and invalid executables fail
without download retries. HTTP 5xx/429 and connection/timeouts get at most two
attempts per executable inside setup, separated by at most 100ms, sharing one
120-second network budget (including response bodies and retry delay), with a
five-second connect limit. Body read failures use the same transient retry policy;
local write, checksum, and version failures do not. Redirect hops are not separate
retry attempts. Hashing, syncing and the five-second staged version probe are outside
the network budget. After exhaustion, the host may approve a retry
after the cooldown; this does not schedule another model turn. Setup errors are
structured `dependency_unavailable` with `retryable: false`, including exhausted
transient errors. Download diagnostics omit response bodies and redirect URLs.

A filesystem lock serializes setup, with a 20-second lock acquisition deadline.
Downloads are size/time bounded, staged on
the destination filesystem, synced, checked for digest (Lightpanda) and executable
version, then atomically renamed. Failed stages leave the previous destination
intact; stale download files are discarded on retry. Version probes retain at most
4 KiB per output stream and use a shared five-second deadline for transient
`ETXTBSY` spawn retries, process polling, and nonblocking pipe capture. Both capture
threads exit by that deadline and are joined before returning; a pipe still open
at the deadline fails the probe even if the leader exited successfully. Probes
terminate their process groups, but cannot kill descendants that deliberately
escape into another group/session. Such descendants cannot hold capture threads
open indefinitely. This is not a hard real-time bound on OS process spawning,
scheduling, or reaping after `SIGKILL`. Verification stamps track executable
metadata (canonical path, device/inode, size, mode, mtime and ctime), the command
contract, output configuration, and managed Lightpanda digest policy. A valid
stamp skips both version subprocesses and full-binary hashing. Metadata, pin,
configuration, or managed/override policy changes invalidate it; managed bytes
are hashed again before execution and replaced through the same checked staging
path if stale. Setup returns paths without rewriting process-wide override
environment variables, so managed installs remain managed on later calls.
Managed pin metadata and lowercase digest syntax are validated before a stamp can
authorize reuse. A poisoned failure-cache mutex returns an error before provisioning,
not a panic or an unguarded install. Lightpanda's tagged
[`main.zig`](https://github.com/lightpanda-io/browser/blob/0.4.1/src/main.zig)
prints the bare build version; tagged
[`build.zig`](https://github.com/lightpanda-io/browser/blob/0.4.1/build.zig)
constructs it as a semantic version. The probe accepts `0.4.1` and version-shaped
development builds, not product-prefixed or unknown output. It is a liveness/shape
check, not an exact Lightpanda version gate: managed identity comes from the digest,
while overrides remain operator-trusted.
Setup does not launch a scratch browser;
actual engine/CDP compatibility is checked by the first browser command.

## Upstream Verification

Inspected the upstream repository, installation and Lightpanda engine docs,
plus `v0.35.0` CLI `main.rs`, `commands.rs`, `flags.rs`, and `skills.rs`:

- <https://github.com/vercel-labs/agent-browser>
- <https://agent-browser.dev/installation>
- <https://agent-browser.dev/engines/lightpanda>
- <https://github.com/vercel-labs/agent-browser/tree/v0.35.0/cli/src>
- <https://api.github.com/repos/lightpanda-io/browser/releases/latest>

The pin supports read filters, outlines, llms indexes, and skills list/get.
Neither the pinned dispatch nor current upstream installer supports
`agent-browser install lightpanda`: the installer downloads Chrome. Ghost never
invokes it. The Lightpanda engine documentation was fetched again on 2026-09-18
and still recommends manually installing nightly binaries and selecting
`--engine lightpanda` with an optional `--executable-path`. Ghost instead uses
the explicitly reviewed tagged release and digests above. The initial pin inspection
fetched metadata/docs only. Subsequent localhost verification below exercised the
managed executables and found an incompatibility that version probes cannot detect.

## Real Localhost Verification

On 2026-09-18, inspected pinned CLI commit
`585e740fcef069d74e21f0e88e8bf4ea7df34385`, current upstream README/skill stub,
and installed help/version output with cleared environment as a non-root user.
The installed CLI was 0.35.0, but the managed Lightpanda file was the stale
`1.0.0-nightly.8781+55fdb8794`. The existing setup hook replaced it with SHA-checked
0.4.1 and refreshed the verification stamp; no system packages or Tachyon binaries
were installed or restarted.

The actual 0.35.0/0.4.1 pair failed a localhost URL read before HTTP retrieval.
`cli/src/native/cdp/lightpanda.rs` passes `serve --host 127.0.0.1 --port PORT
--timeout 604800`, but 0.4.1 rejects `--timeout` with `UnknownOption`. Upstream's
`to_ai_friendly_error` sees the word `timeout` in the launch diagnostic and
rewrites it to "Operation timed out. The page may still be loading or the element
may not exist." This is a launch argument incompatibility, not a slow page.

Ghost now supplies a private executable adapter for this exact seven-argument
launch shape. It removes only the obsolete session-timeout flag and execs the
verified absolute Lightpanda path with the loopback host and numeric port. Other
shapes fail closed. The stable adapter path avoids changing upstream launch
identity between commands; its lifetime includes work-end close. The daemon's
60-second idle limit and scoped cleanup remain in force. No Chrome fallback,
arbitrary downloaded script, or additional browser option is introduced.
The adapter and its randomly named temporary directory have mode 0700. The
selected absolute executable path is shell-quoted; the port is quoted and checked
for ASCII digits. This is a pinned compatibility adapter, not a plugin interface.
Private modes do not isolate hostile same-UID/root processes, and the temporary
directory parent must be operator-trusted (including any configured `TMPDIR`).

Opt-in real test (uses temporary HOME/workspace, localhost fixture, no models):

```sh
GHOST_BROWSER_TEST_MANAGED=1 \
GHOST_BROWSER_TEST_BIN="$HOME/.local/share/tachyon/tools/agent-browser/bin/agent-browser" \
GHOST_LIGHTPANDA_TEST_BIN="$HOME/.local/share/tachyon/tools/lightpanda/lightpanda" \
cargo test -p ghost browser_installed_localhost_smoke --lib -- --ignored --nocapture
```

`GHOST_BROWSER_TEST_MANAGED=1` explicitly opts into the existing managed setup
hook and can provision/repair the pinned install. Omit it to test only existing
operator-selected executables. The test requires both binary paths; it never
silently skips missing executables. Reads, navigation, targeted DOM retrieval,
snapshots, interaction and close are exercised through the actual Ghost wrapper.
This is compatibility evidence on a controlled page, not a research benchmark
or proof that every external site works. Screenshots remain outside this text tool.

## Limits and Integration

The latency improvement is structural, not a benchmark: startup/help no longer
pay download, hash, or probe costs; unchanged commands pay filesystem lock,
metadata and stamp reads rather than subprocess preflight or full-binary hashes.
Concurrent first use serializes under the cross-process setup lock and rechecks
the cache before provisioning. Each command still launches the pinned CLI, and
the first real command still pays cold provisioning/engine startup costs.
The command deadline includes waiting for setup. Cancellation/timeout stops the
caller waiting, but an already-started blocking setup can finish downloading and
publishing under its lock; it does not start a browser. The blocking worker's
lock wait is independently bounded to 20 seconds, not tied to caller cancellation.
That is a lock acquisition limit, not a total setup deadline. A retry can reuse
the completed install.
The tools directory and verification stamps are operator-trusted, not a defense
against a local attacker who can rewrite both binaries and stamps. Metadata
checks are not continuous integrity monitoring or protection against replacement
between validation and execution.

The pinned CLI creates its daemon and sends configured engine launch options
before dispatching `read`. Thus upstream's browser-free URL-read description
does not guarantee browser-free execution with Ghost's fixed engine options.
Do not remove Lightpanda settings to exploit that path: it risks Chrome fallback.
No version bump or parallel HTTP retrieval implementation is included here.

The CLI release download is version-tag pinned, not independently checksum
pinned. Lightpanda digests authenticate bytes against the inspected GitHub
metadata, not a separate vendor signing key. These changes do not provide a
network/SSRF sandbox; domain restrictions remain host policy. Lightpanda's DOM
and CDP compatibility is incomplete, and unsupported pages fail without engine
fallback. Focused tests use fake executables, in-memory stages and loopback HTTP
fixtures through the real downloader/managed publish path (hash match/mismatch,
stale install rejection/repair, prior executable preservation, concurrent cached
first use, and local help),
without model calls. The separately opted-in real test above additionally covers
the managed browser pair against a loopback page.

Work-end tests use a fake CLI with detached local processes to verify scoped
close, success/failure/cancellation/drop, isolation, and idempotence. They do not
prove real agent-browser/Lightpanda teardown on their own. The real smoke also
checks successful close and removal of its scoped daemon PID file. On Linux it
also records the daemon and its child processes across all spawning threads,
checks their owner and the exact selected Lightpanda argv path, and verifies
those PIDs disappear after close. A failed/timed-out close is reported
to stderr; the daemon's idle timeout remains a fallback, not a guarantee against
broken upstream teardown. Drop-only cleanup requires a running Tokio runtime;
hosts must await `finish_work()` before shutdown. Abrupt host death and deliberately
escaped descendants are not contained. Direct adapter calls without `for_work`
retain their lower-level lifetime and require explicit close by the caller.
Native admission covers each CLI operation (including work-end close), not a
resident lease spanning the detached browser session. Setup's blocking probes and
downloads are outside that operation lease. These are not whole-process-tree CPU
or memory accounting guarantees.
