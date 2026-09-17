//! Ordered capability selection; schemas remain shared and host grants remain external.

use crate::capabilities::Capability;

pub const CAPABILITIES: &[Capability] = &[
    Capability::DelegateOne,
    Capability::DelegateMany,
    Capability::Memory,
    Capability::Schedule,
];

pub const DELEGATION_CAPABILITIES: &[Capability] =
    &[Capability::DelegateOne, Capability::DelegateMany];

pub fn primary() -> Vec<crate::tools::ToolSchema> {
    CAPABILITIES
        .iter()
        .map(|capability| capability.schema())
        .collect()
}
