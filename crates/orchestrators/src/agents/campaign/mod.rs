//! Pure campaign oversight policy; registration grants no runtime authority.
pub mod progress;
pub mod tools;

use crate::registry::{
    HostLane, InvocationBinding, InvocationKind, OutputVisibility, RoleDescriptor, RoleId,
};

pub const fn definition() -> RoleDescriptor {
    RoleDescriptor {
        id: RoleId::Campaign,
        stable_id: "campaign",
        purpose:
            "Assess campaign progress and flag operator attention without allocation authority.",
        capabilities: tools::CAPABILITIES,
        host_lane: HostLane::Background,
        output_visibility: OutputVisibility::Internal,
        enabled: true,
        invocations: &[InvocationBinding {
            kind: InvocationKind::Primary,
            prompt: |_| Ok(include_str!("prompt.md").to_owned()),
            tools: tools::primary,
        }],
    }
}
