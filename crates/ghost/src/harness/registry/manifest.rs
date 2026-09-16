#![forbid(unsafe_code)]

use crate::harness::runtime::Tool;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
    pub interface: &'static str,
    pub usage: &'static str,
    pub operations: &'static [&'static str],
}

/// Trusted host-supplied operations, not a plugin loader or permission grant.
/// Construction must not start external resources.
pub struct Package {
    pub manifest: Manifest,
    pub tools: Vec<Arc<dyn Tool>>,
}
