# Current Architecture

## Runtime Ownership

```text
User
  |
  v
Tachyon CLI / TUI
  |
  | Unix socket IPC
  v
Tachyond
  |
  +-- tachyon-foreground (daemon-owned, id `foreground`)
  |
  +-- worker Ghost processes

Provider-neutral conversation and coordination policy lives in
`crates/orchestrators`. The live user-facing turn runtime is the standalone
`tachyon-foreground` process; Ghost no longer has a Conversation role.
```

### Tachyond

Tachyond is the runtime authority. It currently:

- Starts Foreground when the daemon starts.
- Starts worker Ghost processes.
- Tracks in-memory agent state.
- Routes chat and subscription requests. User turns and worker evidence cross
  the foreground subprocess boundary as versioned `InteractionCommandEnvelope`
  values.
- Owns process handles and worker workspaces.
- Publishes line-oriented events over IPC.

The lifecycle contract is defined in `docs/LIFECYCLE.md`. Semantic retention
decisions currently remain in orchestration policy; Tachyond is authoritative
for process signals, replacement, lease enforcement, and cleanup.

### Foreground

Foreground is a dedicated process started and supervised by Tachyond. It:

- Owns the user-facing conversation.
- Decides whether to answer, delegate, wait, or replan.
- Requests workers through Tachyond.
- Synthesizes worker results.
- Temporarily delegates worker requests directly while the Background
  Coordinator is extracted.

Foreground does not run worker tools directly. Its current model schemas are
`spawn_agent` and `spawn_agents`.

### Workers

Workers are Ghost processes started by Tachyond for focused work. They have
execution tools such as `ipython` and `agent_browser`. They do not
create or control other agents.

Workers may be one-shot or warm sessions. Warm workers remain in `waiting`
after a completed delegated task when orchestration policy retains their
session. Ghost reports semantic completion and Tachyond enforces lifecycle.

Retention defaults to keep-alive. The coordinator must explicitly release a
worker when its context, workspace, and artifacts are no longer worth keeping.
Tachyond may override this only for an explicit safety limit or shutdown.

### TUI

The TUI is a separate `tachyon-tui` client crate. It:

- Sends user messages through the daemon API.
- Subscribes to Foreground and worker output.
- Renders conversation and lifecycle state.
- Provides local presentation controls.

The TUI starts neither Foreground nor Ghost and makes no orchestration
decisions. It still accepts compatibility line markers while outbound event
normalization is completed.

The CLI and TUI share `tachyon-client` for IPC access but have separate
presentation and entry-point code.

## Current Protocol

The shared API is newline-delimited JSON over a Unix domain socket. It supports
daemon status, agent start/list/status/logs/stop/kill/restart, chat, and live
subscriptions. It also exposes daemon-owned `interrupt` and `resume` process
operations. `stop` uses SIGTERM, `interrupt` uses SIGINT, and `kill` uses
SIGKILL on Unix.

The streamed payload is represented as `EventStream` plus a string, with typed
JSON agent events being introduced inside the payload. The lifecycle protocol
must add stable session, turn, task, parent-task, actor, event-kind, and
timestamp fields.

The daemon-to-foreground input path is typed in
`tachyon-api/src/interaction.rs`. Each command carries protocol version,
message/correlation/causation IDs, conversation and optional turn identity,
generation, and timestamp. The stable daemon identity is `foreground`; API
requests are `ForegroundChat` and `ForegroundSubscribe`.

## Deferred Infrastructure

These are intentionally deferred until the interaction model is reliable:

- Capability portals and user approval.
- Firecracker or other hard isolation.
- Resource allocation and environment profiles.
- Webcam, microphone, and display access.
- Durable task recovery across daemon restarts.
