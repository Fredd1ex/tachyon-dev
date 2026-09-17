//! Background Coordinator policy.

pub mod prompt;
pub mod tools;

pub use tools::CAPABILITIES;

use crate::registry::{
    HostLane, InvocationBinding, InvocationContext, InvocationKind, OutputVisibility,
    RegistryError, RoleDescriptor, RoleId,
};

pub const fn definition() -> RoleDescriptor {
    RoleDescriptor {
        id: RoleId::Coordinator,
        stable_id: RoleId::Coordinator.stable_id(),
        purpose: "Coordinate internal objectives and review worker evidence and lifecycle.",
        capabilities: CAPABILITIES,
        host_lane: HostLane::Background,
        output_visibility: OutputVisibility::Internal,
        enabled: true,
        invocations: &[
            InvocationBinding {
                kind: InvocationKind::Primary,
                prompt: primary,
                tools: tools::primary,
            },
            InvocationBinding {
                kind: InvocationKind::Review,
                prompt: review,
                tools: tools::review,
            },
        ],
    }
}

fn primary(context: InvocationContext<'_>) -> Result<String, RegistryError> {
    Ok(prompt::system_prompt(prompt::PromptContext {
        persona: context.persona,
    }))
}

fn review(context: InvocationContext<'_>) -> Result<String, RegistryError> {
    Ok(prompt::review_system_prompt(prompt::PromptContext {
        persona: context.persona,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_has_lifecycle_but_no_execution_capabilities() {
        let names: Vec<_> = CAPABILITIES
            .iter()
            .map(|capability| capability.tool_name())
            .collect();
        assert!(names.contains(&"spawn_agent"));
        assert!(names.contains(&"agent_replan"));
        assert!(!names.contains(&"respond"));
        assert!(!names.contains(&"ipython"));
        assert!(!names.contains(&"agent_browser"));
    }
}
