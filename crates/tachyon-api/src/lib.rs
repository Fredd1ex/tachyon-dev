#![forbid(unsafe_code)]

//! Shared types for the Tachyon daemon IPC.
//!
//! The API request set mirrors the `tachyon` CLI commands one-to-one: every
//! user-facing action is an `ApiRequest`, and the daemon serves the same
//! interface. This keeps the CLI, TUI, and foreground runtime
//! in lock-step over a single protocol.

pub mod interaction;
pub mod transport;
pub mod types;

pub use interaction::*;
pub use types::*;

/// Stable daemon registry and process identity for the foreground runtime.
pub const FOREGROUND_ID: &str = "foreground";
