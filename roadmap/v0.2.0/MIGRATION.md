# Tachyon v0.2.0 Migration Record

## Slice 1: Orchestration Policy Boundary

Status: complete and behavior-preserving.

### Completed

- Renamed the domain source directory from `crates/orchestrator` to
  `crates/orchestrators`.
- Made `tachyon-orchestrator` a production dependency of Ghost.
- Moved Conversational Agent prompt policy into:
  - `crates/orchestrators/src/conversation/prompt.rs`
- Moved Background Coordinator prompt policy into:
  - `crates/orchestrators/src/background/prompt.rs`
- Moved conversation classification, answerability parsing, and deterministic
  publication/execution policy into:
  - `crates/orchestrators/src/conversation/policy.rs`
- Moved conversation/background capability sets into the orchestration crate.
  Ghost now adapts neutral capabilities to its temporary tool schemas.
- Created the worker-only harness directory:
  - `crates/ghost/src/harness/backend.rs`
  - `crates/ghost/src/harness/browser_setup.rs`
  - `crates/ghost/src/harness/prompt.rs`
- Kept worker prompt knowledge next to the concrete IPython and browser
  capabilities it describes.
- Moved deterministic policy tests to the component that owns the policy.

### Preserved

- Process topology and command-line flags.
- Tachyond/Ghost line protocol and event envelopes.
- Checkpoint paths and JSON format.
- Provider requests, streaming, tool schemas, and model behavior.
- Existing conversation publication and commit ordering.
- Existing worker lifecycle behavior.

### Remaining Coupling After Slice 1

- Ghost `main.rs` still hosts the live Conversation and Background-compatible
  loops while migration adapters are in place.
- `ghost/model.rs` still mixes provider transport, provider-neutral message
  types, conversation tool schemas, and worker tool schemas.
- `ghost/role.rs` remains a compatibility adapter for role config and concrete
  tool-schema construction.
- Conversation state, checkpointing, evidence matching, and synthesis calls
  still execute in the Ghost process.
- The Background Coordinator role is defined but is not yet a separately
  scheduled runtime component.
- Conversation still delegates workers directly through `BackgroundDelegate`.

This slice improves maintainability and establishes dependency direction. It is
not expected by itself to reduce the greeting token count or weather latency,
because the live model loop and prompt contents are intentionally unchanged.

## Verification

```text
cargo test -p tachyon-orchestrator
cargo test -p ghost
cargo build --workspace
```

All pass after this slice.

## Slice 2: Neutral Model Runtime

Status: complete and behavior-preserving.

### Completed

- Added `crates/tachyon-model` for provider-neutral messages, completions, token
  usage, tool calls, tool specifications, model configuration, and the current
  OpenRouter/OpenAI-compatible SSE transport.
- Replaced model-side configuration and credential lookup with explicit
  `ModelConfig` inputs. Tachyon-specific config, key resolution, debug flags,
  and log paths remain in the Ghost compatibility adapter.
- Moved routing and reasoning wire types into `tachyon-model`; `tachyon-util`
  reexports them for configuration compatibility.
- Moved IPython and Agent Browser schemas to
  `crates/ghost/src/harness/tools.rs` beside their worker capabilities.
- Moved temporary concrete Conversation/Background schemas out of transport
  code and into `crates/ghost/src/orchestration_tools.rs`. They remain a Ghost
  adapter only while Ghost hosts the live orchestration loop.
- Generalized required-tool streaming so the model transport streams a
  caller-selected tool argument and contains no knowledge of `respond`, DSML,
  delegation, or other Tachyon role policy.
- Kept DSML suppression in `ghost/interaction.rs`, where the user-visible
  Conversation stream is adapted.
- Added tests for selected routing serialization, SSE framing across UTF-8 and
  CRLF boundaries, incremental JSON argument decoding, exact tool selection,
  and DSML suppression at the Ghost boundary.

### Preserved

- Provider endpoint, authorization, request body fields, and SSE behavior.
- Context fitting, token usage collection, finish reasons, and tool-call
  accumulation.
- Required-tool behavior and incremental direct-response publication.
- Existing process topology, command-line flags, checkpoints, and daemon IPC.

### Verification

```text
cargo check --workspace --all-targets
cargo test --workspace
cargo build --workspace
```

All pass after this slice.

### Remaining Coupling After Slice 2

- Ghost `main.rs` still hosts Conversation, Background-compatible, and worker
  loops in one binary.
- `ghost/model.rs` still adapts Tachyon configuration and credentials into the
  shared model runtime.
- `ghost/orchestration_tools.rs` still builds concrete schemas from neutral
  orchestrator capabilities.
- Conversation state, checkpointing, evidence matching, synthesis, and event
  publication still execute in the Ghost process.
- The Background Coordinator is defined but is not yet an independently
  scheduled runtime component.
- Conversation still delegates workers directly through `BackgroundDelegate`.

## Slice 3A: Foreground Command Boundary

Status: complete and behavior-preserving.

### Completed

- Added versioned `InteractionCommandEnvelope` and `InteractionEventEnvelope`
  contracts in `crates/tachyon-api/src/interaction.rs`.
- Added typed foreground commands for user-turn acceptance, background updates,
  conversation lifecycle, and notifications.
- Added typed foreground events and structured task intents for the upcoming
  dedicated Interaction Manager runtime.
- Every envelope carries message, correlation, causation, conversation, turn,
  generation, protocol-version, and timestamp metadata.
- Tachyond now serializes `ForegroundChat` input as `AcceptUserTurn`, so
  multiline messages cross the subprocess boundary as one NDJSON command.
- Correlated `WorkerCompleted` envelopes are reinjected as
  `PublishBackgroundUpdate` rather than prefixed control strings.
- The current Conversation compatibility process accepts the typed commands and
  retains legacy line parsing for non-Conversation Ghost chat sessions.

### Verification

- Protocol round-trip tests cover multiline turns, metadata, task intents, and
  nested worker-event preservation.
- Ghost tests cover typed user-turn and worker-evidence decoding.
- Tachyond tests cover versioned command construction and newline-safe framing.

### Remaining Coupling After Slice 3A

- Ghost still owns the live Conversation loop and outbound event adapter.
- Outbound process streams still mix structured envelopes with legacy status
  lines, although user input and worker evidence are now typed.
- `InteractionEventEnvelope` and `InteractionIntent` are defined but are not yet
  the sole outbound publication mechanism.
- Background delegation still starts workers directly from the Conversation
  compatibility process.

## Slice 3B: Standalone Foreground Runtime

Status: complete and behavior-preserving.

### Completed

- Added the standalone `tachyon-foreground` crate and binary with no dependency
  on Ghost.
- Moved live Conversation turn state, ordered commits, checkpoints, evidence
  matching, classification, answerability, synthesis, delegation, streaming,
  timing, and usage publication out of Ghost.
- Moved conversation model adapters into `tachyon-foreground`; the
  `tachyon-orchestrator` policy crate is provider-neutral and does not depend on
  `tachyon-model`.
- Kept role-owned capability schemas provider-neutral in
  `crates/orchestrators/src/tools.rs`; Foreground and temporary Background
  compatibility adapt them to model transport schemas.
- Tachyond resolves `TACHYON_FOREGROUND_BIN` or the sibling
  `tachyon-foreground` binary and supervises it separately from workers.
- Replaced the abbreviated legacy runtime identity with the stable `foreground`
  ID.
  API requests are `ForegroundChat` and `ForegroundSubscribe`, and structured
  events use `Actor::Foreground`. Legacy serialized request/actor names remain
  decode aliases only.
- Removed the Conversation role and all Conversation runtime ownership from
  Ghost. Ghost now contains worker execution plus temporary Background
  compatibility.

### Preserved

- Foreground checkpoint path and JSON format.
- Typed stdin commands, structured event envelopes, turn correlation, worker
  result timeouts, and TUI rendering behavior.
- Model routing, prompts, required-tool behavior, DSML suppression/recovery,
  ordered commits, and concurrent independent turns.
- Worker process topology and Tachyond lifecycle authority.

### Verification

```text
cargo check --workspace --all-targets
cargo test --workspace
cargo build --workspace
cargo tree -p tachyon-foreground --depth 1
cargo tree -p tachyon-orchestrator --depth 1
```

All pass. The dependency trees confirm that Foreground does not depend on Ghost
and orchestration policy does not depend on model transport.

### Remaining Coupling After Slice 3B

- Foreground still directly issues `BackgroundDelegate` and synchronously
  collects worker evidence inside a turn.
- `InteractionEventEnvelope` and `InteractionIntent` are not yet the sole
  outbound Foreground contract.
- Output streams retain compatibility line markers alongside structured events.
- Background Coordinator compatibility remains in Ghost and is not independently
  scheduled.

## Recommended Slice 4

Make the typed asynchronous boundary authoritative:

1. Emit validated `InteractionEventEnvelope` and `InteractionIntent` values as
   the sole semantic Foreground output.
2. Convert task intents into daemon-owned durable commands without Foreground
   waiting on workers.
3. Schedule the Background Coordinator independently with a bounded queue and
   lower priority than Foreground.
4. Return normalized findings/notifications through typed background updates.
5. Remove Background compatibility from Ghost and then remove remaining line
   marker adapters after replay tests pass.

The redb durable-state migration follows stabilization of these command/event
types and ownership boundaries. It is required for the v0.4.0 kernel and uses
two independent stores: `runtime.redb` for agent/task management and
`user-memory.redb` for durable user memory. Markdown agent records are imported
once and removed from the authoritative runtime write path after cutover.
