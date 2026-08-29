#![forbid(unsafe_code)]

//! Markdown-first durable memory for Tachyon.

pub mod protocol;
pub mod store;

pub use store::{MemoryError, MemoryStore, TaskDocument, TaskMetadata, TaskState};
