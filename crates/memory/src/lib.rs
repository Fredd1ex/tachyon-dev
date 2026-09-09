#![forbid(unsafe_code)]

//! Curated durable memory for Tachyon.

pub mod protocol;
pub mod store;

pub use store::{
    explicit_preference, MemoryError, MemoryMutationSource, MemoryRecord, MemoryStore,
    PreferenceObservation, PrimitiveRecord,
};
