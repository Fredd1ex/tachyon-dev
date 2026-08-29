#![forbid(unsafe_code)]

//! Role policy and domain model for Tachyon's orchestration layer.
//!
//! Prompt policy, provider-neutral schemas, and deterministic orchestration
//! decisions live here. Runtime/model adapters belong to their role hosts.

pub mod attention;
pub mod background;
pub mod capabilities;
pub mod control;
pub mod conversation;
pub mod scheduler;
pub mod tasks;
pub mod tools;
