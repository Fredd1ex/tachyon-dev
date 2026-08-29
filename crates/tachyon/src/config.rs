#![forbid(unsafe_code)]

//! Re-export of the shared Tachyon config. The CLI is the writer; the daemon,
//! Foreground runtime, and Ghost read it via `tachyon_util::config`.

pub use tachyon_util::config::{Config, Provider};
