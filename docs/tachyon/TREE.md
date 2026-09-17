# Project Tree

This is the current source layout. `PROJECT.md` describes the intended system;
this file describes where that system is implemented today.

```text
tachyon/
├── Cargo.toml                    # Rust workspace definition
├── PROJECT.md                    # Canonical product vision and requirements
├── README.md                     # Project overview and quick start
├── TODO.md                       # Current implementation backlog
├── roadmap/                      # Versioned targets and migration records
│   ├── README.md                 # Milestone index and critical path
│   ├── v0.2.0/                   # Architecture and migration contracts
│   └── v0.3.0/                   # Ghost research harness and tool runtime
├── docs/
│   ├── README.md                 # Documentation index and maintenance rules
│   ├── interaction/              # Host layout and descriptive role registry
│   ├── tachyon/                  # Shared and user-facing system documentation
│   │   ├── ARCHITECTURE.md       # Runtime ownership and boundaries
│   │   ├── INTERACTION.md        # Conversation routing and concurrency
│   │   ├── MEMORY.md             # Memory service design
│   │   ├── RELIABILITY.md        # Reliability and release gates
│   │   ├── STATUS.md             # Implemented, partial, and missing work
│   │   ├── ARCHINSTALL.md        # Arch Linux installation notes
│   │   └── TREE.md               # This source tree guide
│   ├── tachyond/                 # Daemon-owned runtime documentation
│   │   ├── DAEMON.md             # Tachyond ownership and IPC
│   │   ├── LIFECYCLE.md          # Worker lifetime and cleanup contracts
│   │   └── REATTACH.md           # Persistent worker reattachment
│   └── ghost/                    # Worker harness documentation
│       ├── HARNESS.md            # Ghost tools and execution behavior
│       └── SANDBOX.md            # Workspace access and isolation policy
├── crates/
│   ├── memory/                    # Curated redb-backed memory service
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs            # Standalone Unix-socket service
│   │       ├── store.rs           # Versioned facts, indexes, and revocations
│   │       ├── protocol.rs        # Memory request/response types
│   │       └── lib.rs             # Memory crate exports
│   │
│   ├── orchestrators/             # LLM-role policy, prompts, and scheduling domain
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs             # Orchestration policy/domain exports
│   │       ├── capabilities.rs    # Neutral conversation/background capabilities
│   │       ├── registry.rs        # Descriptive Conversation/Coordinator inventory
│   │       ├── agents/
│   │       │   ├── conversation/  # Prompt Markdown/renderers, tools, routing policy
│   │       │   └── coordinator/   # Prompt Markdown/renderers and tools
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
│   ├── interaction/               # Existing host crates, not a shared crate
│   │   ├── foreground/            # Package/bin: tachyon-foreground
│   │   │   ├── Cargo.toml
│   │   │   └── src/
│   │   │       ├── main.rs        # Turn/model loop and tool dispatch/validation
│   │   │       ├── input.rs       # Typed and legacy input decoding
│   │   │       ├── turns.rs       # Evidence, ordered history and snapshots
│   │   │       ├── checkpoints.rs # Checkpoint format and writer
│   │   │       ├── delegation.rs  # Daemon service adapters
│   │   │       ├── model.rs       # Conversation policy/model adapters
│   │   │       └── streaming.rs   # Event correlation and stdout publication
│   │   └── background/            # Package/bin: tachyon-background
│   │       ├── Cargo.toml
│   │       └── src/
│   │           ├── main.rs        # Startup, model configuration and wiring
│   │           ├── requests.rs    # Concurrent request processing and output
│   │           ├── review.rs      # Semantic review and decision validation
│   │           └── scheduling.rs  # Schedule request validation
│   │
│   ├── tachyon/                  # User-facing CLI and daemon lifecycle client
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs           # CLI entry point and subcommands
│   │       ├── daemon.rs         # Start/stop/status daemon helpers
│   │       ├── cli.rs            # CLI argument definitions
│   │       ├── config.rs         # CLI configuration commands
│   │       ├── data.rs           # Safe authoritative memory wiping
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
        ├── spawns tachyon-background
        ├── spawns ghost --chat/--task --role worker
        └── spawns tachyon-memory --root .../memory --socket .../memory.sock
                    │
                    ├── orchestrators/src/   # prompts and deterministic role policy
                    ├── interaction/foreground/ # foreground state and model adapters
                    ├── interaction/background/ # review and scheduling host
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
| Conversation/background prompts | `crates/orchestrators/src/agents/*/prompt.rs` and adjacent Markdown |
| Turn scheduling policy | `crates/orchestrators/src/agents/conversation/policy.rs` |
| Worker capabilities | `crates/ghost/src/harness` |
| Interaction tests | Foreground runtime tests plus orchestration policy tests |
