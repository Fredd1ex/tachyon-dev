# Ghost Terminology

Foundation draft, reviewed 2026-09-10. **CURRENT** means implemented in this
worktree, not necessarily released. **PROPOSED** means the approved vocabulary
or intended contract, not an available API. This document owns definitions;
[API.md](API.md) owns call boundaries and [RESEARCH.md](RESEARCH.md) owns rationale.

## Research And Execution

| Term | Canonical Meaning And Status |
|---|---|
| Research | Top-level durable investigation and eventual ownership boundary, **not Project**. **CURRENT:** immutable `Research` metadata in Tachyond. **PROPOSED:** ownership of campaigns, evidence, and accounting. A repository is not its identity. |
| Campaign | Bounded durable effort within one Research, with a specific objective and eventual stopping/resource constraints. **CURRENT:** immutable metadata with only `draft` status. **PROPOSED:** execution lifecycle and explicit budget authorization. |
| Group | **PROPOSED:** coordination subdivision within a campaign; groups may nest within that campaign, never become an alternative Research root. Membership is not a process topology or permission grant. |
| Work | Durable objective, distinct from the executor assigned to pursue it. **CURRENT:** `WorkRequest`/`WorkResult` identify logical assignments; persistence is still mixed with worker/task records. **PROPOSED:** independently durable objective identity across attempts and executor replacement. |
| Worker | Temporary executor of Work, typically a Ghost process; may be retained or reused. Its process lifetime, model context, and assignment are not the objective's lifetime. |
| Workspace | Filesystem working area, not a Research, campaign, or session. **CURRENT:** native tools use `ToolContext.cwd`; Python `%cd` is separate. A path names a location, not durable ownership. |
| WorkSession | **PROPOSED:** resumable logical execution context for Work, including conversation and explicit recovery references. Not a promise to preserve a live process. |
| PythonSession | Kernel-backed analysis state subordinate to a work scope. **CURRENT:** lazy, in-memory, one cell at a time per kernel; lost on kernel failure and closed at work end. Not a WorkSession or durable checkpoint. |
| Python Proxy | **CURRENT:** interpreter-local adapter built from Rust-supplied native schemas and method descriptors. Not an implementation of the capability or a permission grant; all hostcalls are authorized in Rust. See [PYTHON.md](PYTHON.md) for file responsibilities. |

**PROPOSED lifetime rules:** Research survives worker replacement, client detach,
and workspace deletion. Deleting a workspace can destroy unretained artifact
bytes; it must not silently delete Research metadata/evidence records. Cancelling
Work stops its authorized execution, whereas detaching a client only stops
observation. Neither action means deleting Research. Retention/deletion APIs and
descendant-cancellation policy still require explicit contracts.

## Tools And References

| Term | Canonical Meaning And Status |
|---|---|
| Capability | Discoverable interface such as `workspace`, `exec`, or `ctx`. **CURRENT naming mismatch:** the Rust `Capability` enum instead names permission categories such as `ReadFilesystem`. That enum requires a targeted rename, not a change to this definition. |
| Tool Package | Trusted host-supplied implementation, manifest, guidance, configuration, and tests for a capability. **CURRENT:** represented by `Package`. Activation selects guidance; it neither loads arbitrary code nor grants permission. |
| Permission | Host-authorized action or resource boundary. **CURRENT:** policy checks operation names and the effect categories currently named `Capability` at dispatch. Not OS sandbox enforcement. **PROPOSED:** explicit Research-scoped access and cross-Research grants. |
| Operation | Callable tool API, such as `grep` or `exec`. **CURRENT naming mismatch:** exec also calls a live execution handle `metadata.operation` (`exec:<uuid>`). Preserve that wire field; do not confuse it with the callable name. |
| Invocation | One call of an operation with arguments and execution context. A status/wait invocation observes an earlier execution; it does not restart it. |
| Process Reference | Handle to supervised execution, not a PID or path. **CURRENT:** exec's opaque operation handle is work-local and may precede process spawn. It is not durable across Ghost restart. |
| Output Reference | **CURRENT:** typed `{"id":"output:..."}` for bounded live-work output; usable by native `ctx` only in the owning scope. Durable workspace-store refs are a separate unsupported `ctx` surface. Possession alone grants no access. |
| Resource Reference | **PROPOSED:** typed handle to authorized content, including documents, output, traces, and artifacts. An output reference is one specialized form; a resource reference is not a filesystem path or permission grant. |

Durable record IDs, display names, filesystem paths, and ephemeral handles are
different namespaces. **CURRENT:** Research/Campaign IDs are server-generated;
titles are non-unique text. **PROPOSED:** all durable relationships use IDs, with
names only for display/lookup and explicit permission checks on reference use.

## Results And Provenance

These are **PROPOSED research records**, unless a current counterpart is noted.

| Term | Meaning |
|---|---|
| Attempt | One bounded pursuit of Work with recorded inputs, executor/configuration, and an outcome. A retry creates another attempt, not another objective. |
| Outcome | Execution disposition, not scientific validity. **CURRENT:** `WorkOutcome` has completed, blocked, failed, cancelled, and timed-out variants; exec completion separately requires inspecting exit/error details. |
| Finding | Attributed claim or useful constraint, including negative or unresolved results, linked to supporting and contradicting evidence. |
| Artifact | Immutable, versioned published object or file set with provenance. **CURRENT gap:** `artifact` hashes/registers an existing regular file; bytes remain mutable in the workspace. Registration does not yet satisfy immutable publication or durable byte retention. |
| Candidate | Versioned proposed solution, referring to exact artifacts and inherited findings; not an accepted result merely because it was produced. |
| Evaluation | Recorded application of an identified evaluator/protocol to an exact candidate version, with inputs, environment, outcomes, and limitations. |
| Evidence | Inspectable observations and evaluation records supporting or challenging a finding. Model confidence or a success exit code alone is insufficient. |
| Observation | Bounded view of an outcome or resource presented to a worker; the underlying content may be much larger. |
| Evidence Stage | Extent of evaluation completed, such as smoke checks, full evaluation, or replication. Task-defined and separate from score. Evidence maturity describes this concept; it is not another independent field. |
| Round | Optional strategy-defined research iteration. Never use `generation` for this: assignment generations fence obsolete workers, while instruction revisions order steering. |
| Lineage | Typed derivation and evidence relationships among attempts, candidates, artifacts, evaluations, and findings. Not merely chronological chat. |
| Trace | Execution/event history for diagnosis and attribution. A trace records what happened; it does not by itself establish causality or validity. |
| Checkpoint | Explicit versioned recoverable state with declared coverage and omissions. **CURRENT:** chat checkpoints and activation snapshots are distinct; production activation recovery and Python restoration are not implemented. No arbitrary pickle restoration guarantee. |

## Migration Boundary

Current source names remain authoritative for code/wire use:
[API types](../../crates/tachyon-api/src/types.rs),
[daemon records](../../crates/tachyond/src/main.rs), and
[runtime store](../../crates/tachyond/src/runtime_store.rs).
`Agent*` requests/`AgentInfo`, daemon `Task`, and persisted `RuntimeTaskRecord`
mix worker lifecycle with task metadata. The daemon's `WorkRecord` is an in-memory
assignment/review record, not the proposed independent durable Work ledger.
Ghost's generated `for_work` scope is an ephemeral resource boundary, not a
Research ID. Python's internal `Session` and chat `session.rs` are different.

Use canonical terms in new design prose, but retain existing serialized names,
fields, IDs, and paths. Any future separation needs a targeted persistence/wire
migration and tests; this foundation does not authorize broad renames or add
compatibility aliases speculatively.
