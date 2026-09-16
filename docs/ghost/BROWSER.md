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

`skills list` and `skills get core` serve a small Ghost-scoped skill. The release
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
The browser ceiling is 20 seconds and 8 KiB captured output, including skill help.

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
Lightpanda uses GitHub release asset IDs and published SHA-256 digests inspected
on 2026-09-09, rather than trusting a mutable nightly URL. Supported automatic
Lightpanda targets remain Linux x86_64 and macOS arm64. A deleted upstream asset
fails closed; Ghost will not silently fetch a replacement nightly.

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
invokes it. The Lightpanda documentation instead recommends manual nightly
binaries; Ghost pins their asset identity and digest before executing them.

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
not live downloads, real browser installs, or model calls.

Work-end tests use a fake CLI with detached local processes to verify scoped
close, success/failure/cancellation/drop, isolation, and idempotence. They do not
prove real agent-browser/Lightpanda teardown. A failed/timed-out close is reported
to stderr; the daemon's idle timeout remains a fallback, not a guarantee against
broken upstream teardown. Drop-only cleanup requires a running Tokio runtime;
hosts must await `finish_work()` before shutdown. Abrupt host death and deliberately
escaped descendants are not contained. Direct adapter calls without `for_work`
retain their lower-level lifetime and require explicit close by the caller.
