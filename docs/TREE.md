# Project Tree

This is the current source layout. `PROJECT.md` describes the intended system;
this file describes where that system is implemented today.

```text
tachyon/
├── Cargo.toml                    # Rust workspace definition
├── PROJECT.md                    # Canonical product vision and requirements
├── README.md                     # Project overview and quick start
├── TODO.md                       # Current implementation backlog
├── v0.2.0/                       # Approved v0.2.0 architecture and migration records
│   ├── INVARIANTS.md             # Ownership, concurrency, and reliability rules
│   ├── ORCHESTRATION_PROTOCOL.md # Target contracts and migration sequence
│   ├── UI_PROTOCOL.md            # Runtime-derived TUI projections and behavior
│   ├── MIGRATION.md              # Completed slices and remaining coupling
│   └── DURABLE_STATE.md          # Deferred redb schema and migration rules
├── docs/
│   ├── README.md                 # Documentation index and maintenance rules
│   ├── TREE.md                   # This source tree guide
│   ├── ARCHITECTURE.md           # Runtime ownership and boundaries
│   ├── STATUS.md                 # Implemented, partial, and missing work
│   ├── MEMORY.md                 # Markdown-first Memory service design
│   ├── INTERACTION.md            # Conversation routing and concurrency
│   ├── HARNESS.md                # Ghost tools and execution behavior
│   ├── DAEMON.md                 # Tachyond ownership, IPC, and lifecycle
│   └── ARCHINSTALL.md             # Arch Linux installation notes
├── crates/
│   ├── memory/                    # Markdown-first durable memory service
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs            # Standalone Unix-socket service
│   │       ├── store.rs           # Categories, front matter, atomic task files
│   │       ├── protocol.rs        # Memory request/response types
│   │       └── lib.rs             # Memory crate exports
│   │
│   ├── orchestrators/             # LLM-role policy, prompts, and scheduling domain
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs             # Orchestration policy/domain exports
│   │       ├── capabilities.rs    # Neutral conversation/background capabilities
│   │       ├── conversation/
│   │       │   ├── mod.rs         # Conversation turns and capability policy
│   │       │   ├── prompt.rs      # Conversation and spoken-response prompts
│   │       │   └── policy.rs      # Routing, answerability, and execution policy
│   │       ├── background/
│   │       │   ├── mod.rs         # Background capability policy
│   │       │   └── prompt.rs      # Background Coordinator prompt
│   │       ├── attention.rs       # Priority and result delivery policy
│   │       ├── tasks.rs            # Durable task identity/state model
│   │       ├── scheduler.rs        # Dependency readiness rules
│   │       ├── control.rs          # Start/await/interrupt/release intents
│   │       └── tools.rs            # Provider-neutral role tool schemas
│   │
│   ├── tachyon-model/             # Shared model types and provider transport
│   │   ├── Cargo.toml
│   │   └── src/lib.rs             # Explicit model config, OpenRouter requests,
│   │                              # SSE framing, usage, messages, and tool calls
│   │
│   ├── tachyon-foreground/        # User-facing foreground runtime
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs            # Turn state, checkpoints, delegation, events
│   │       └── interaction.rs     # Conversation model/streaming adapters
│   │
│   ├── tachyon/                  # User-facing CLI and daemon lifecycle client
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs           # CLI entry point and subcommands
│   │       ├── daemon.rs         # Start/stop/status daemon helpers
│   │       ├── cli.rs            # CLI argument definitions
│   │       ├── config.rs         # CLI configuration commands
│   │       ├── providers.rs      # Provider/model configuration commands
│   │       ├── style.rs          # Terminal presentation helpers
│   │       └── lib.rs            # Shared CLI crate exports
│   │
│   ├── tachyon-client/            # Shared typed IPC client
│   │   ├── Cargo.toml
│   │   └── src/lib.rs             # Client requests and subscriptions
│   │
│   ├── tachyon-tui/               # Separate visual Tachyon client
│   │   ├── Cargo.toml
│   │   └── src/lib.rs             # TUI event loop, view model, rendering
│   │
│   ├── tachyond/                 # Persistent daemon and runtime authority
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs           # Registry, IPC server, process spawning,
│   │                              # subscriptions, and lifecycle handling
│   │
│   ├── ghost/                    # Worker harness plus Background compatibility
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs           # Worker/Background model loop and process adapter
│   │       ├── role.rs           # Config/tool-schema compatibility adapter
│   │       ├── model.rs          # Tachyon config-to-model compatibility adapter
│   │       ├── harness/
│   │       │   ├── mod.rs        # Worker-only harness exports
│   │       │   ├── backend.rs    # ExecutionBackend, Local, and persistent IPython
│   │       │   ├── browser_setup.rs # Agent Browser/Lightpanda provisioning
│   │       │   ├── prompt.rs     # Prompt tied to worker capabilities
│   │       │   └── tools.rs      # Worker-owned IPython/browser schemas
│   │       └── lib.rs             # Ghost module exports and errors
│   │
│   ├── tachyon-api/              # Shared daemon protocol types
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── types.rs          # ApiRequest, ApiResponse, agent states,
│   │       │                      # event stream types
│   │       ├── interaction.rs    # Versioned foreground commands/events
│   │       ├── transport.rs       # Newline-delimited JSON socket transport
│   │       └── lib.rs             # API crate exports
│   │
│   └── tachyon-util/             # Shared paths, config, guards, and locking
│       ├── Cargo.toml
│       └── src/
│           ├── daemon.rs         # Socket, pid, log, workspace, and lock paths
│           ├── config.rs         # Shared non-secret configuration
│           ├── guard.rs          # Root/launch safety checks
│           └── lib.rs             # Utility crate exports
│
└── target/                       # Cargo build output, not source
```

## Runtime Mapping

```text
tachyon-tui/src/lib.rs
        │
        │ typed client calls
        ▼
tachyon-client/src/lib.rs
        │
        ▼
tachyond/src/main.rs
        │
        ├── spawns tachyon-foreground --agent-id foreground
        ├── spawns ghost --chat/--task --role worker
        └── spawns tachyon-memory --root .../memory --socket .../memory.sock
                    │
                    ├── orchestrators/src/   # prompts and deterministic role policy
                    ├── tachyon-foreground/ # foreground state and model adapters
                    ├── tachyon-model/src/lib.rs # shared model runtime
                    ├── ghost/src/model.rs   # config compatibility adapter
                    └── ghost/src/harness/   # worker execution capabilities
```

## Important Current Boundary

Tachyond starts the standalone `tachyon-foreground` process under the stable
`foreground` identity and starts Ghost only for workers or temporary Background
compatibility. Role policy and provider-neutral schemas live in
`crates/orchestrators`; provider transport lives in `crates/tachyon-model`.
Foreground input is typed, but outbound streams still contain some legacy line
markers alongside structured events.

## Where To Add Future Features

| Feature | Primary location |
|---|---|
| Structured events | `crates/tachyon-api`, then `crates/tachyond` |
| Task persistence | `crates/tachyond` and `crates/tachyon-util` |
| Interrupt/await/release | `crates/tachyon-api`, `crates/tachyond`, Ghost control client |
| TUI visualizations | `crates/tachyon-tui/src/lib.rs` |
| Worker lifecycle policy | Background Coordinator plus Tachyond API enforcement |
| Portals | Tachyond policy/control plane |
| MicroVM backend | `crates/ghost/src/harness/backend.rs` and Tachyond runtime |
| Conversation/background prompts | `crates/orchestrators/src/*/prompt.rs` |
| Turn scheduling policy | `crates/orchestrators/src/conversation/policy.rs` |
| Worker capabilities | `crates/ghost/src/harness` |
| Interaction tests | Foreground runtime tests plus orchestration policy tests |
