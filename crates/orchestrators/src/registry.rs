//! Pure invocation dispatch, not runtime execution or permission grants.
//! Role IDs are policy identities, not daemon wire actor IDs.

use crate::{agents, capabilities::Capability, tools::ToolSchema};

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub enum RoleId {
    Campaign,
    Conversation,
    Coordinator,
    Custom(&'static str),
}

impl RoleId {
    pub const fn stable_id(self) -> &'static str {
        match self {
            Self::Campaign => "campaign",
            Self::Conversation => "conversation",
            Self::Coordinator => "coordinator",
            Self::Custom(id) => id,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InvocationKind {
    Primary,
    Review,
}

#[derive(Debug, Clone, Copy)]
pub struct ConversationIdentity<'a> {
    pub user_name: &'a str,
    pub conversation_name: &'a str,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct InvocationContext<'a> {
    pub identity: Option<ConversationIdentity<'a>>,
    pub persona: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct InvocationBinding {
    pub kind: InvocationKind,
    pub prompt: fn(InvocationContext<'_>) -> Result<String, RegistryError>,
    pub tools: fn() -> Vec<ToolSchema>,
}

#[derive(Debug)]
pub struct RenderedInvocation {
    pub prompt: String,
    pub tools: Vec<ToolSchema>,
    pub output_visibility: OutputVisibility,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RegistryError {
    InvalidStableId(RoleId),
    DuplicateRole(RoleId),
    DuplicateInvocation(RoleId, InvocationKind),
    UnknownRole(RoleId),
    DisabledRole(RoleId),
    WrongHostLane(RoleId, HostLane),
    UnsupportedInvocation(RoleId, InvocationKind),
    MissingConversationIdentity,
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "role registry: {self:?}")
    }
}

impl std::error::Error for RegistryError {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HostLane {
    Foreground,
    Background,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OutputVisibility {
    UserFacing,
    Internal,
}

#[derive(Debug, Clone, Copy)]
pub struct RoleDescriptor {
    pub id: RoleId,
    pub stable_id: &'static str,
    pub display_name: &'static str,
    pub purpose: &'static str,
    pub capabilities: &'static [Capability],
    pub host_lane: HostLane,
    pub output_visibility: OutputVisibility,
    pub enabled: bool,
    pub invocations: &'static [InvocationBinding],
}

/// The single registration list, sorted by stable role ID. Memory is a storage
/// service, not an inference role.
pub const ROLES: &[RoleDescriptor] = &[
    agents::campaign::definition(),
    agents::conversation::definition(),
    agents::coordinator::definition(),
];

/// Validation checks registration integrity only; it does not authorize tools.
#[derive(Debug)]
pub struct Registry<'a> {
    roles: &'a [RoleDescriptor],
}

impl<'a> Registry<'a> {
    pub fn enabled_roles(&self) -> impl Iterator<Item = &RoleDescriptor> {
        self.roles.iter().filter(|role| role.enabled)
    }

    pub fn new(roles: &'a [RoleDescriptor]) -> Result<Self, RegistryError> {
        for (index, role) in roles.iter().enumerate() {
            if role.stable_id.is_empty() || role.stable_id != role.id.stable_id() {
                return Err(RegistryError::InvalidStableId(role.id));
            }
            if roles[..index]
                .iter()
                .any(|other| other.stable_id == role.stable_id)
            {
                return Err(RegistryError::DuplicateRole(role.id));
            }
            for (index, binding) in role.invocations.iter().enumerate() {
                if role.invocations[..index]
                    .iter()
                    .any(|other| other.kind == binding.kind)
                {
                    return Err(RegistryError::DuplicateInvocation(role.id, binding.kind));
                }
            }
        }
        Ok(Self { roles })
    }

    pub fn resolve(
        &self,
        id: RoleId,
        lane: HostLane,
        kind: InvocationKind,
    ) -> Result<ResolvedInvocation<'a>, RegistryError> {
        let role = self
            .roles
            .iter()
            .find(|role| role.stable_id == id.stable_id())
            .ok_or(RegistryError::UnknownRole(id))?;
        if !role.enabled {
            return Err(RegistryError::DisabledRole(id));
        }
        if role.host_lane != lane {
            return Err(RegistryError::WrongHostLane(id, lane));
        }
        let binding = role
            .invocations
            .iter()
            .find(|binding| binding.kind == kind)
            .ok_or(RegistryError::UnsupportedInvocation(id, kind))?;
        Ok(ResolvedInvocation { role, binding })
    }
}

#[derive(Debug)]
pub struct ResolvedInvocation<'a> {
    role: &'a RoleDescriptor,
    binding: &'a InvocationBinding,
}

impl ResolvedInvocation<'_> {
    pub fn descriptor(&self) -> &RoleDescriptor {
        self.role
    }

    pub fn render(
        &self,
        context: InvocationContext<'_>,
    ) -> Result<RenderedInvocation, RegistryError> {
        Ok(RenderedInvocation {
            prompt: (self.binding.prompt)(context)?,
            tools: (self.binding.tools)(),
            output_visibility: self.role.output_visibility,
        })
    }
}

pub fn builtin() -> Registry<'static> {
    Registry::new(ROLES).expect("built-in role definitions must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_is_unique_sorted_and_contains_only_shipped_roles() {
        assert_eq!(
            ROLES.iter().map(|role| role.id).collect::<Vec<_>>(),
            [RoleId::Campaign, RoleId::Conversation, RoleId::Coordinator]
        );
        assert_eq!(
            ROLES.iter().map(|role| role.stable_id).collect::<Vec<_>>(),
            ["campaign", "conversation", "coordinator"]
        );
        for pair in ROLES.windows(2) {
            assert!(pair[0].id < pair[1].id);
            assert!(pair[0].stable_id < pair[1].stable_id);
        }
        for role in ROLES {
            assert!(!role.display_name.is_empty());
            assert!(!role.purpose.is_empty());
        }
    }

    #[test]
    fn roles_keep_host_and_output_boundaries_separate() {
        assert_eq!(ROLES[0].host_lane, HostLane::Background);
        assert_eq!(ROLES[0].output_visibility, OutputVisibility::Internal);
        assert_eq!(ROLES[1].host_lane, HostLane::Foreground);
        assert_eq!(ROLES[1].output_visibility, OutputVisibility::UserFacing);
        assert_eq!(ROLES[2].host_lane, HostLane::Background);
        assert_eq!(ROLES[2].output_visibility, OutputVisibility::Internal);
    }

    #[test]
    fn ordered_tool_selections_and_legacy_exports_are_unchanged() {
        let names = |role: &RoleDescriptor| {
            role.capabilities
                .iter()
                .map(|cap| cap.tool_name())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(&ROLES[1]),
            [
                "spawn_agent",
                "spawn_agents",
                "memory",
                "schedule",
                "todo",
                "campaign",
                "websearch",
                "webfetch"
            ]
        );
        assert_eq!(
            names(&ROLES[2]),
            [
                "spawn_agent",
                "spawn_agents",
                "agent_inspect",
                "agent_await",
                "agent_release",
                "agent_retain",
                "agent_stage",
                "agent_replan"
            ]
        );
        assert_eq!(crate::conversation::CAPABILITIES, ROLES[1].capabilities);
        assert_eq!(crate::background::CAPABILITIES, ROLES[2].capabilities);
        assert_eq!(
            crate::conversation::DELEGATION_CAPABILITIES,
            &[Capability::DelegateOne, Capability::DelegateMany]
        );
    }
}
