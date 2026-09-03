# Ghost v0.3 Tool Runtime Requirements

Status: **Accepted target specification** for the v0.3.0 Ghost Research Harness.

This specification was accepted during the v0.2.0 development cycle. It defines
the target runtime; it does not describe the current v0.2.0 implementation.

## Goal

Implement a minimal, fast, extensible, Rust-native tool layer for the Ghost
worker harness. Ghost remains execution-only. It must not contain conversation,
coordination, task scheduling, lifecycle authority, or durable-state policy.

Priorities, in order:

1. Small API surface.
2. Predictable structured results.
3. Bounded memory and model-context usage.
4. Cancellation and deadlines.
5. Fast common-case execution.
6. Workspace-aware security boundaries.
7. Easy addition of future tools.
8. No Python or Node runtime dependency for the P0 core.
9. `#![forbid(unsafe_code)]` in Tachyon-owned Rust code.

The P0 built-in set contains exactly eight tools: `read`, `write`, `edit`, `ls`,
`find`, `grep`, `exec`, and `artifact`. Existing `ipython` and `agent_browser`
tools complement this set and remain available through the same registry when
enabled by policy. They are not replaced, renamed, or counted as P0 built-ins.

The Rust-native core must build and pass its integration fixture without
Python, Node, or browser dependencies. Deployments may additionally provide
IPython for stateful analysis and `agent_browser` for web and literature
research. Failure to initialize an optional complementary tool must not disable
the built-in core.

## Complementary Research Tool Tracks

The following tools share `ToolRegistry`, `ToolContext`, policy enforcement,
result envelopes, output storage, telemetry, cancellation, and deadlines with
the built-in core. They are designed and accepted separately so their runtime
dependencies cannot weaken the core acceptance gate.

### Persistent IPython

IPython remains the stateful scientific-analysis environment. Its separate
hardening track should provide:

- one persistent kernel/session per eligible Ghost workspace;
- deterministic reset and recovery after timeout, cancellation, or process
  failure;
- bounded stdout/stderr and rich-display capture through `ToolOutputStore`;
- serializable checkpoints with explicit reporting for values that cannot be
  restored;
- the same scrubbed environment and workspace identity as `exec`;
- no provider or daemon credentials in the kernel environment;
- lifecycle cleanup owned by Tachyond; and
- policy control through a future `ExecutePython` capability.

The built-in `exec` tool handles general shell execution. IPython remains
available for persistent variables, notebooks, data analysis, and scientific
libraries rather than serving as the only filesystem or shell interface.

### Quick Web Retrieval

Add a lightweight tool, provisionally `quick_web`, for the common case where a
researcher needs bounded text or JSON from an HTTP(S) resource without browser
automation.

Its separate specification should cover URL policy, DNS/private-network rules,
redirect limits, content types, decompression limits, response size, citations,
robots/site policy where applicable, cancellation, deadlines, and external
output references. It should be Rust-native and avoid launching Lightpanda.

### Interactive Lightpanda Research

Evolve `agent_browser` into the interactive path for JavaScript rendering,
multi-step navigation, forms, downloads, and evidence that cannot be retrieved
through `quick_web`.

This path remains backed by supervised, headless Lightpanda sessions because
Lightpanda is a minimal, fast research engine. Ghost does not require or plan a
Chromium-class browser or visible browser UI for this tier. Its separate
hardening track should cover session reuse, startup health, per-workspace
profiles, bounded snapshots, download/artifact registration, cancellation,
deadline enforcement, crash recovery, and cleanup.

Research policy should prefer the cheapest sufficient path:

1. Use `quick_web` for direct bounded retrieval.
2. Escalate to interactive Lightpanda when rendering or interaction is needed.
3. Use persistent IPython for analysis of retrieved or local data.

The model recommends tool calls, but policy determines which complementary
capabilities are available. Failure in IPython or either web path must remain a
structured tool failure and must not disable unrelated tools.

## Shared Execution Rules

Every tool call receives an effective deadline equal to the earliest of:

- the enclosing `WorkRequest` deadline;
- the `ToolPolicy` maximum duration; and
- a valid, bounded tool-specific timeout supplied by the caller.

Every execution path must observe a cancellation token. A tool must not hold a
registry, policy, output-store, or process-state lock across an `.await`.

Tool input is decoded with unknown fields rejected. Numeric limits are clamped
to policy maxima. A zero timeout does not mean unlimited execution unless the
policy explicitly enables unbounded execution.

## P0 Built-In Tools

### 1. `read`

Input:

```json
{
  "path": "string",
  "offset": 1,
  "limit": 400
}
```

`offset` and `limit` are optional, one-based, and line-oriented.

Requirements:

- Resolve relative paths from the active tool `cwd`.
- Permit absolute paths only when their canonical target is in an allowed root.
- Support text files only in P0.
- Read incrementally; do not load arbitrary files into memory.
- Bound returned bytes and lines globally and by policy.
- Return continuation information when truncated.
- Report file size, returned line range, and total lines when known.
- Decode valid UTF-8 exactly. Invalid UTF-8 returns a structured binary or
  unsupported error; lossy decoding is not permitted.
- Do not follow a symlink whose resolved target escapes an allowed root.

Example metadata:

```json
{
  "path": "src/lib.rs",
  "size_bytes": 48210,
  "start_line": 1,
  "end_line": 400,
  "total_lines": 1200,
  "truncated": true
}
```

### 2. `write`

Input:

```json
{
  "path": "string",
  "content": "string",
  "create_parents": false
}
```

`create_parents` defaults to `false`; there is no model-dependent "clearly
specified" inference.

Requirements:

- Create or replace one file.
- Create parent directories only when `create_parents` is `true`.
- Validate the nearest existing parent for new targets, then revalidate the
  target before replacement.
- Use an adjacent temporary file, flush it, sync when policy requires it, and
  atomically rename where the platform/filesystem supports replacement.
- Preserve the original file if any pre-rename step fails.
- Enforce path scope and reject symlink escapes.
- Return bytes written and whether a file was created or replaced.
- Never silently overwrite a path outside an allowed root.

### 3. `edit`

Input:

```json
{
  "path": "string",
  "old": "string",
  "new": "string"
}
```

Requirements:

- Perform exact string replacement; no regular expressions in P0.
- Fail with `NotFound` when `old` occurs zero times.
- Fail with `AmbiguousEdit` when `old` occurs more than once.
- Do not guess which match was intended.
- Use the same atomic replacement and path checks as `write`.
- Preserve the original file on every failure.
- Return occurrence count, changed byte count, and a compact bounded summary.

An explicit `occurrence` selector may be added later. `hashline_edit` is out of
scope.

### 4. `ls`

Input:

```json
{
  "path": ".",
  "limit": 200
}
```

Both fields are optional. The default path is the active tool `cwd`.

Requirements:

- Use Rust filesystem APIs; do not spawn a shell.
- Never recurse.
- Sort deterministically by filename bytes after platform-normalized decoding.
- Return filename, entry type, and size when cheaply available.
- Do not follow symlinks for metadata that could escape the workspace.
- Stop at the effective result limit and return continuation metadata.

### 5. `find`

Input:

```json
{
  "pattern": "*.rs",
  "path": ".",
  "limit": 200,
  "hidden": false
}
```

Preferred backend:

1. Resolve `fd` or `fdfind` once and cache the executable path.
2. Invoke it directly with `tokio::process::Command` when available.
3. Otherwise use the native Rust backend.

Native fallback requirements:

- Respect `.gitignore` and standard ignore files.
- Support a documented glob subset consistently across both backends.
- Do not follow symlinks by default.
- Enforce allowed roots for the traversal start and every returned path.
- Produce deterministic sorted results.
- Stop traversal once the effective limit is reached.
- Observe cancellation and deadline checks during traversal.

Do not spawn a shell to run `fd`.

### 6. `grep`

Input:

```json
{
  "pattern": "struct ToolRegistry",
  "path": ".",
  "context": 2,
  "limit": 200,
  "fixed_string": false
}
```

Preferred backend:

1. Resolve `rg` once and cache the executable path.
2. Invoke it directly with `tokio::process::Command` when available.
3. Otherwise use the native Rust backend.

Requirements:

- Use regular expressions by default and literal matching in fixed-string mode.
- Define the supported native regex syntax and test parity for that subset.
- Respect `.gitignore` and skip binary files by default.
- Return line numbers and optional bounded context lines.
- Bound total matches, bytes, context lines, and per-line length.
- Stop work when limits, cancellation, or the deadline are reached.
- Return structured matches:

```json
{
  "path": "src/tools.rs",
  "line": 42,
  "text": "struct ToolRegistry {",
  "before": [],
  "after": []
}
```

Do not spawn a shell to run `rg`.

### 7. `exec`

Input:

```json
{
  "command": "cargo test --workspace",
  "cwd": ".",
  "timeout_ms": 120000
}
```

This tool receives the largest P0 test surface.

Requirements:

- Use `tokio::process::Command`.
- Use the configured shell because command strings may contain pipes and
  redirection. Invoke the shell directly, without a login/profile mode.
- Default `cwd` to the active Ghost workspace and enforce allowed roots.
- Default to 120 seconds unless the enclosing work or policy is stricter.
- Reject `timeout_ms = 0` unless unbounded execution is explicitly enabled.
- Observe cancellation and effective deadlines.
- Drain stdout and stderr concurrently.
- Emit bounded streaming `WorkEvent` updates where appropriate.
- Maintain bounded rolling head/tail buffers and spool larger complete output
  through `ToolOutputStore` when useful.
- Distinguish normal completion, non-zero exit, timeout, cancellation, spawn
  failure, and cleanup failure.
- Start the child in an isolated process group using safe standard-library or
  dependency APIs compatible with `forbid(unsafe_code)`.
- On timeout/cancellation, send cooperative TERM, wait a bounded grace period,
  then hard-kill the process group where supported.
- Reap children and report platforms where descendant cleanup is not
  guaranteed.
- Record command, cwd, duration, exit code, termination reason, and output
  truncation.

Environment:

- Begin from `env_clear()`.
- Copy only policy-approved values from `PATH`, `LANG`, `LC_*`, `TERM`, and
  `TMPDIR`.
- Set `HOME` to a workspace-owned directory, not the user's real home.
- Never pass provider credentials, daemon credentials, SSH agent sockets, or
  unrelated Tachyon secrets.

Until Microsandbox exists, `exec` is an explicitly documented host-risk.
Changing `cwd`, clearing environment variables, and validating paths do not
constrain arbitrary shell commands or network access.

### 8. `artifact`

Input:

```json
{
  "path": "results/report.json",
  "kind": "dataset",
  "description": "Normalized benchmark output"
}
```

Requirements:

- Require an existing path inside an allowed root.
- Gather bounded metadata and hash regular files incrementally.
- Treat directory hashing as a bounded manifest operation; defer or reject an
  unbounded tree rather than walking it implicitly.
- Emit an artifact registration request/event through a context-provided sink.
- Associate metadata with task ID, work ID, generation ID, and attempt ID when
  present.
- Keep large bytes on the filesystem or in an external artifact store.
- Store only metadata, provenance, and references in daemon-owned persistence.
- Ghost never writes Tachyond's database directly.

## Tool Architecture

The registry requires object-safe tools. Native `async fn` in a trait is not
object-safe for `Arc<dyn Tool>`, so use the project's stable boxed-future
pattern (or another explicitly object-safe stable-Rust pattern):

```rust
trait Tool: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn schema(&self) -> &ToolSchema;
    fn capabilities(&self) -> &'static [Capability];

    fn execute<'a>(
        &'a self,
        ctx: &'a ToolContext,
        input: serde_json::Value,
    ) -> ToolFuture<'a>;
}

type ToolFuture<'a> = Pin<
    Box<dyn Future<Output = Result<ToolResult, ToolError>> + Send + 'a>
>;
```

Schemas are created once when tools are constructed.

`ToolContext` contains runtime facts and enforcement handles, not agent policy
decisions:

```rust
struct ToolContext {
    workspace_root: PathBuf,
    cwd: PathBuf,
    task_id: TaskId,
    work_id: WorkId,
    generation_id: GenerationId,
    attempt_id: Option<AttemptId>,
    deadline: Instant,
    cancellation: CancellationToken,
    policy: Arc<ToolPolicy>,
    output_store: Arc<dyn ToolOutputStore>,
    event_sink: Arc<dyn ToolEventSink>,
}
```

Tool implementations must not import Conversation Agent or Background
Coordinator code.

## Tool Registry

```rust
struct ToolRegistry {
    tools: HashMap<ToolName, Arc<dyn Tool>>,
}
```

Required API:

- `register(tool)`
- `get(name)`
- `definitions(policy)`
- `execute(name, context, input)`

Requirements:

- Duplicate names are startup/configuration errors.
- Registration ends before the worker begins serving work; the registry is
  immutable afterward.
- The model receives only schemas enabled by the current policy.
- Registry execution performs common input, capability, deadline, telemetry,
  and result-bound checks.
- Future tools are registered without changing Ghost's reasoning loop.
- The main loop contains no tool-name dispatch match.

## Capabilities And Policy

Initial capabilities:

```text
ReadFilesystem
WriteFilesystem
ExecuteProcess
RegisterArtifact
```

Mapping:

| Tools | Capability |
| --- | --- |
| `read`, `ls`, `find`, `grep` | `ReadFilesystem` |
| `write`, `edit` | `WriteFilesystem` |
| `exec` | `ExecuteProcess` |
| `artifact` | `RegisterArtifact` |

`ToolPolicy` is daemon/configuration supplied and immutable for one assignment.
The model cannot grant itself capabilities or widen allowed roots, deadlines,
output limits, environment access, or execution privileges.

## Workspace Rules

Tachyond assigns every Ghost one explicit workspace root. A configurable
research root may default to `~/Agents`, producing roots such as
`~/Agents/project-name`; Ghost itself must not infer permission for the user's
home directory. Existing managed workspaces under Tachyon's data directory
remain valid explicit roots during migration.

Filesystem tools must:

1. Normalize lexical components before access.
2. Canonicalize existing targets.
3. Canonicalize the nearest existing parent of a new write target.
4. Reject traversal outside allowed roots.
5. Reject symlink escapes and revalidate before atomic replacement.
6. Never assume `/home/$USER`, `~/.ssh`, `~/.config`, credentials, or unrelated
   projects are allowed.
7. Return `ToolError::OutsideWorkspace` or `ToolError::PermissionDenied` rather
   than prose-only failures.

Path validation reduces accidental filesystem access but is not a hard sandbox
for `exec`. This limitation remains visible in code and documentation until
Microsandbox provides process, filesystem, and network isolation.

## Result Contract And Output Limits

Every tool returns one envelope:

```rust
struct ToolResult {
    content: String,
    is_error: bool,
    metadata: serde_json::Value,
    truncated: bool,
    continuation: Option<Continuation>,
    output_ref: Option<ToolOutputRef>,
}
```

Defaults:

```text
MAX_RETURN_BYTES = 1 MiB
MAX_RETURN_LINES = 2000
MAX_MODEL_CONTENT_BYTES = 64 KiB
```

`MAX_RETURN_BYTES` is a hard runtime/envelope ceiling, not permission to insert
1 MiB into model history. The model-facing cap is independently configurable
and defaults no higher than 64 KiB; deployments may retain Ghost's smaller
12,000-character context budget. The registry applies both limits.

For larger output:

- preserve a useful bounded head and tail;
- include an explicit omission marker;
- set `truncated = true`;
- store complete output externally when useful and policy permits;
- return `output_ref` for later bounded read/search access; and
- never insert complete external output into model context automatically.

These limits apply to `read`, `grep`, `exec`, `find`, and `ls`.

## Structured Errors

```rust
enum ToolErrorCode {
    InvalidInput,
    NotFound,
    PermissionDenied,
    OutsideWorkspace,
    UnsupportedBinary,
    AmbiguousEdit,
    Timeout,
    Cancelled,
    ProcessFailed,
    DependencyUnavailable,
    Io,
    Internal,
}

struct ToolError {
    code: ToolErrorCode,
    message: String,
    retryable: bool,
    metadata: serde_json::Value,
}
```

Failures are serialized through the common result path and are never encoded
only in prose. Internal errors must not expose secrets or unrestricted host
paths.

## External Binary Discovery

At startup or first use, resolve and cache:

- `rg`
- `fd`, then `fdfind`

Search the sanitized configured `PATH` once. Validate that the result is an
executable file. Never rescan on every call and never use a shell for discovery
or invocation.

Missing external binaries are not fatal:

```text
rg available -> grep uses rg
rg absent    -> grep uses native backend
fd available -> find uses fd
fd absent    -> find uses native backend
```

## Performance And Telemetry

- Do not spawn a shell for `read`, `write`, `edit`, `grep`, `find`, or `ls`.
- Only `exec` has shell semantics.
- Avoid whole-file reads for ranges and stop searches at effective limits.
- Drain external process streams asynchronously.
- Cache executable discovery and schemas.
- Keep allocations bounded and spool large outputs incrementally.
- Check cancellation/deadlines inside native traversal loops.

Emit per-tool telemetry through the event sink:

```text
tool_name
call_id
start/end/duration
bytes_in/bytes_out
truncated
success/error_code
backend (native/rg/fd/shell)
task_id/work_id/generation_id/attempt_id
```

Telemetry is structured operational evidence, not user memory and not direct
database writes by Ghost.

## Tests

Unit coverage:

- `read`: ranges, truncation, binary input, missing paths, UTF-8, and workspace
  escape.
- `write`: create, replace, parent creation, atomic failure preservation,
  denied paths, and symlink races where testable.
- `edit`: one match, no match, ambiguous match, and failure preservation.
- `ls`: deterministic ordering, limits, types, and symlinks.
- `grep`: regex, fixed strings, ignore files, context, limits, cancellation,
  and native/`rg` parity.
- `find`: glob subset, ignore files, limits, symlink behavior, cancellation,
  and native/`fd` parity.
- `exec`: success, non-zero exit, concurrent stdout/stderr, timeout,
  cancellation, TERM-to-KILL escalation, descendant cleanup, huge output,
  output references, cwd denial, zero timeout, and secret exclusion.
- `artifact`: registration, nonexistent path, bounded hashing, provenance, and
  denied paths.
- registry: duplicate names, immutable startup, capability filtering, unknown
  tools, schema caching, and common limit enforcement.

Required integration fixture:

1. Ghost lists a fixture repository.
2. It finds source files.
3. It greps for a symbol.
4. It reads the relevant range.
5. It edits one file.
6. It executes the test suite.
7. It registers the resulting artifact.

The integration test must not require Conversation Agent, Background
Coordinator, browser, Python, Node, or Memory Agent.

## Migration Sequence

1. Add shared result/error/policy/context types and an immutable registry.
2. Implement path resolution plus `read`, `ls`, `write`, and `edit`.
3. Implement native `find`/`grep`, then optional cached `fd`/`rg` backends.
4. Replace the current execution backend with bounded cancellable `exec`.
5. Add output storage/references, telemetry, and artifact event registration.
6. Move Ghost's model loop from its tool-name match to registry dispatch.
7. Pass the no-Python integration fixture and adversarial execution tests.
8. Adapt existing `ipython` and Lightpanda-backed `agent_browser`
   implementations to registry
   dispatch without making them dependencies of the built-in core.

Each slice must preserve typed work identity and daemon-owned lifecycle. No
slice may move scheduling, semantic verification, or persistence authority into
Ghost.

## Explicitly Out Of Scope

Do not implement in this runtime milestone:

- subagents;
- `hashline_edit`;
- LSP;
- MCP;
- plugin scripting runtimes;
- git-specific tools;
- Docker or microVM integration;
- implementing or replacing the existing persistent IPython environment;
- implementing or replacing the existing Lightpanda research adapter;
- database access from Ghost; or
- agent-to-agent messaging.

These are separate layers. Existing IPython and browser functionality remains
complementary and may be hardened independently; this milestone does not remove
or redesign either one.
