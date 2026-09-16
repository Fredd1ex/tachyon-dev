#![forbid(unsafe_code)]

/// Host-owned immutable artifact publication; never passed into worker runtimes.
pub mod artifact_store;
pub mod retained_storage;
pub mod verification;
