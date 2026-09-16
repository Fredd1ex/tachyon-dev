#![forbid(unsafe_code)]

//! Shared types for the Tachyon daemon IPC.
//!
//! The API request set mirrors the `tachyon` CLI commands one-to-one: every
//! user-facing action is an `ApiRequest`, and the daemon serves the same
//! interface. This keeps the CLI, TUI, and foreground runtime
//! in lock-step over a single protocol.

pub mod agents;
pub mod context;
pub mod continuation;
pub mod interaction;
pub mod transport;
pub mod types;

pub use interaction::*;
pub use types::*;

/// Stable daemon registry and process identity for the foreground runtime.
pub const FOREGROUND_ID: &str = "foreground";
/// Stable daemon-owned identity for the semantic result reviewer.
pub const BACKGROUND_ID: &str = "background";
/// Stable identity of the curated-memory service/agent boundary.
pub const MEMORY_ID: &str = "memory";
pub mod campaign;
pub mod integration;
pub mod work;
