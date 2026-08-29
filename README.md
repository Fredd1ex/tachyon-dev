# Tachyon

A minimal, Rust-native runtime for secure autonomous agents. Tachyon manages
agents the way systemd manages services and Docker manages containers.

```text
User
  │  tachyon CLI / TUI
  │  IPC
  ▼
Tachyond  (persistent daemon — source of truth)
  │
  ├──▶ tachyon-foreground  (conversation)
  ├──▶ Ghost ──▶ Sandbox   (worker harness)
  └──▶ Ghost ──▶ Sandbox
```

## Components

- **`tachyon-foreground`** — owns user-facing turns, ordering, checkpoints,
  model streaming, and response synthesis.
- **`ghost`** — the worker harness: runs bounded model/tool loops using
  `ipython` and `agent_browser`.
- **`tachyond`** — the persistent daemon that owns agent lifecycle, workspaces,
  Foreground, workers, and the IPC/API. Only one instance may run at a time.
- **`tachyon`** — the CLI and interactive interface, a client of the daemon.
- **`tachyon-api`** — shared IPC wire types (requests/responses mirror the CLI
  1:1).

## Architecture

The CLI and the TUI talk to the daemon over the **same API, one-to-one**. The
request types in `tachyon-api` mirror the CLI commands exactly
(`start`→`AgentStart`, `list`→`AgentList`, `stop`→`AgentStop`, …), so any
client uses an identical interface.
Transport is newline-delimited JSON over a Unix domain socket.

```text
tachyon (CLI / TUI)  ──►  Unix socket (tachyon-api)
                                ▼
                           tachyond
                                │
                   ┌─────────────┴─────────────┐
                   ▼                           ▼
          tachyon-foreground                 ghost workers
                                                │
                                             sandboxes
```

Only **one `tachyond` instance** may run at a time, enforced with an exclusive
file lock — a stale pidfile can never block startup or spawn a duplicate.

## Status

The daemon, Foreground runtime, worker harness, IPC API, and interactive TUI are
functional. Interaction routing and concurrent worker execution are implemented
but still evolving. Structured events, durable task state, interruption,
resource management, and hard isolation remain incomplete. See
[`docs/STATUS.md`](docs/STATUS.md) for the implementation state and
[`PROJECT.md`](PROJECT.md) for the full target architecture.

See [`docs/README.md`](docs/README.md) for the documentation index.

## Build

```sh
cargo build --release
```

Requires stable Rust (workspace, async traits ≥ 1.75). On Arch Linux, see
[`docs/ARCHINSTALL.md`](docs/ARCHINSTALL.md) for the full dependency list
(rust, bash, python, git, ripgrep, fd, curl, firecracker, …) and a one-shot
`pacman -S` command.

## Usage

Run `tachyon` with no command to open the interactive interface. Every command
and subcommand has a `--help` screen describing its options. The interface's
bottom input accepts the same commands as the CLI (they go through the daemon
via the same API). `start "<task>"` subscribes to the agent's live output and
streams it into the chat as it's produced by the harness. Chat bubbles use
colored labels to identify the source: you, Foreground, and workers.
Agent data commands auto-start the daemon if it is down; `daemon` and
`providers` never spawn it implicitly.

```text
tachyon                      # interactive interface
tachyon start "<task>"       # create + start an agent
tachyon list                 # list agents (aliases: ps, ls)
tachyon status [<id>]        # all agents or one
tachyon cat <id>             # agent details (alias: inspect)
tachyon logs <id> [-f]       # follow logs
tachyon stop <id>            # graceful stop
tachyon kill <id>            # force kill
tachyon restart <id>         # restart
tachyon exec <id> -- <cmd>   # run a command in the agent
tachyon top                  # live resource usage

tachyon daemon status        # is the daemon running?
tachyon daemon start         # start it explicitly
tachyon daemon stop
tachyon daemon restart

tachyon providers            # show provider config
tachyon providers list
tachyon providers set-model <name>
tachyon providers get model
```

## Configuration

Provider/model settings live in `~/.config/tachyon/config.toml` (`0600` perms).
**API keys are never stored in the config file** — use `tachyon providers login`
to store them in the operating system credential store, or use the
`OPENROUTER_API_KEY` environment variable for ephemeral/CI use. `tachyon providers` shows
the effective key source and current model.

## Security

- **No root required** — runs as an unprivileged user. Runtime binaries,
  including `tachyon`, `tachyond`, `tachyon-foreground`, and `ghost`, refuse to
  start with effective UID 0 unless
  `TACHYON_ALLOW_ROOT=1` is set to explicitly override.
- No unsafe Rust — every crate is `#![forbid(unsafe_code)]`.
- The current local backend restricts agents to per-agent workspaces and strips
  common credential variables. This is a transitional guardrail, not a hard
  security boundary.
- Firecracker-backed hard isolation and capability portals are deferred.

## License

MIT
