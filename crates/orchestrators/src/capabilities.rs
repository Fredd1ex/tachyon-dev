//! Neutral capabilities requested by orchestration roles.

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Capability {
    Todo,
    Monitor,
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
    pub fn schema(self) -> crate::tools::ToolSchema {
        use crate::tools;
        match self {
            Self::Todo => crate::agents::campaign::tools::todo(),
            Self::Monitor => crate::agents::campaign::tools::monitor(),
            Self::Respond => tools::respond(),
            Self::DelegateOne => tools::spawn_agent(),
            Self::DelegateMany => tools::spawn_agents(),
            Self::Memory => tools::memory(),
            Self::Schedule => tools::schedule(),
            Self::InspectWorker => tools::agent_inspect(),
            Self::AwaitWorker => tools::agent_await(),
            Self::ReleaseWorker => tools::agent_release(),
            Self::RetainWorker => tools::agent_retain(),
            Self::StageWorker => tools::agent_stage(),
            Self::ReplanWorker => tools::agent_replan(),
        }
    }

    pub const fn tool_name(self) -> &'static str {
        match self {
            Self::Todo => "todo",
            Self::Monitor => "monitor",
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
