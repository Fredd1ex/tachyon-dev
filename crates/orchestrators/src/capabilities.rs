//! Neutral capabilities requested by orchestration roles.

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Capability {
    Respond,
    DelegateOne,
    DelegateMany,
    Memory,
    Schedule,
    InspectWorker,
    AwaitWorker,
    ReleaseWorker,
    RetainWorker,
    StageWorker,
    ReplanWorker,
}

impl Capability {
    pub const fn tool_name(self) -> &'static str {
        match self {
            Self::Respond => "respond",
            Self::DelegateOne => "spawn_agent",
            Self::DelegateMany => "spawn_agents",
            Self::Memory => "memory",
            Self::Schedule => "schedule",
            Self::InspectWorker => "agent_inspect",
            Self::AwaitWorker => "agent_await",
            Self::ReleaseWorker => "agent_release",
            Self::RetainWorker => "agent_retain",
            Self::StageWorker => "agent_stage",
            Self::ReplanWorker => "agent_replan",
        }
    }
}
