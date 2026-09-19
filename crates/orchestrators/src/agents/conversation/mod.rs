use serde::{Deserialize, Serialize};

use crate::tasks::TaskId;

pub mod policy;
pub mod prompt;
pub mod tools;

pub use tools::{CAPABILITIES, DELEGATION_CAPABILITIES};

use crate::registry::{
    HostLane, InvocationBinding, InvocationContext, InvocationKind, OutputVisibility,
    RegistryError, RoleDescriptor, RoleId,
};

pub const fn definition() -> RoleDescriptor {
    RoleDescriptor {
        id: RoleId::Conversation,
        display_name: "Conversation",
        stable_id: RoleId::Conversation.stable_id(),
        purpose: "Answer user dialogue and delegate requests requiring fresh work.",
        capabilities: CAPABILITIES,
        host_lane: HostLane::Foreground,
        output_visibility: OutputVisibility::UserFacing,
        enabled: true,
        invocations: &[InvocationBinding {
            kind: InvocationKind::Primary,
            prompt: primary,
            tools: tools::primary,
        }],
    }
}

fn primary(context: InvocationContext<'_>) -> Result<String, RegistryError> {
    let identity = context
        .identity
        .ok_or(RegistryError::MissingConversationIdentity)?;
    Ok(prompt::system_prompt(prompt::PromptContext {
        user_name: identity.user_name,
        conversation_name: identity.conversation_name,
        persona: context.persona,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::Capability;

    #[test]
    fn conversation_exposes_contextual_daemon_services() {
        assert!(CAPABILITIES.contains(&Capability::Memory));
        assert!(CAPABILITIES.contains(&Capability::Schedule));
        assert!(!DELEGATION_CAPABILITIES.contains(&Capability::Memory));
        assert!(!DELEGATION_CAPABILITIES.contains(&Capability::Schedule));
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConversationTurn {
    pub id: String,
    pub user_text: String,
    pub task_id: Option<TaskId>,
}

impl ConversationTurn {
    pub fn new(id: impl Into<String>, user_text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            user_text: user_text.into(),
            task_id: None,
        }
    }
}
