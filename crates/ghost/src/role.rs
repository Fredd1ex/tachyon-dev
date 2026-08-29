//! Central role profiles: model configuration, prompts, and tool permissions.

use crate::harness;
use crate::model::ToolSpec;
use tachyon_orchestrator::background;
use tachyon_orchestrator::capabilities::Capability;
use tachyon_util::config::{AgentConfig, Config};

type ToolFactory = fn() -> ToolSpec;

struct ToolContract {
    name: &'static str,
    build: ToolFactory,
}

const WORKER_TOOLS: &[ToolContract] = &[
    ToolContract {
        name: "ipython",
        build: harness::tools::ipython,
    },
    ToolContract {
        name: "agent_browser",
        build: harness::tools::agent_browser,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentRole {
    Worker,
    Background,
}

impl AgentRole {
    pub fn config(self, cfg: &Config) -> AgentConfig {
        match self {
            Self::Background => cfg.background_config(),
            Self::Worker => cfg.worker_config(),
        }
    }

    pub fn system_prompt(self, cfg: &Config) -> String {
        match self {
            Self::Background => {
                let configured = cfg.background_config();
                background::prompt::system_prompt(background::prompt::PromptContext {
                    persona: configured.persona.as_deref(),
                })
            }
            Self::Worker => {
                let configured = cfg.worker_config();
                harness::prompt::system_prompt(configured.persona.as_deref())
            }
        }
    }

    pub fn tools(self, _force_delegation: bool) -> Vec<ToolSpec> {
        match self {
            Self::Worker => WORKER_TOOLS
                .iter()
                .map(|contract| (contract.build)())
                .collect(),
            Self::Background => background::CAPABILITIES
                .iter()
                .copied()
                .map(orchestration_tool)
                .collect(),
        }
    }

    pub fn allows_tool(self, name: &str) -> bool {
        match self {
            Self::Worker => WORKER_TOOLS.iter().any(|contract| contract.name == name),
            Self::Background => background::CAPABILITIES
                .iter()
                .any(|capability| capability.tool_name() == name),
        }
    }
}

fn orchestration_tool(capability: Capability) -> ToolSpec {
    let schema = match capability {
        Capability::Respond => tachyon_orchestrator::tools::respond(),
        Capability::DelegateOne => tachyon_orchestrator::tools::spawn_agent(),
        Capability::DelegateMany => tachyon_orchestrator::tools::spawn_agents(),
        Capability::InspectWorker => tachyon_orchestrator::tools::agent_inspect(),
        Capability::AwaitWorker => tachyon_orchestrator::tools::agent_await(),
        Capability::ReleaseWorker => tachyon_orchestrator::tools::agent_release(),
        Capability::RetainWorker => tachyon_orchestrator::tools::agent_retain(),
        Capability::StageWorker => tachyon_orchestrator::tools::agent_stage(),
        Capability::ReplanWorker => tachyon_orchestrator::tools::agent_replan(),
    };
    ToolSpec::new(schema.name, schema.description, schema.parameters)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(role: AgentRole, force_delegation: bool) -> Vec<String> {
        role.tools(force_delegation)
            .into_iter()
            .map(|tool| tool.name)
            .collect()
    }

    #[test]
    fn role_tool_sets_preserve_harness_boundaries() {
        assert_eq!(
            names(AgentRole::Worker, false),
            ["ipython", "agent_browser"]
        );
        assert_eq!(
            names(AgentRole::Background, false),
            [
                "spawn_agent",
                "spawn_agents",
                "agent_inspect",
                "agent_await",
                "agent_release",
                "agent_retain",
                "agent_stage",
                "agent_replan",
            ]
        );
    }

    #[test]
    fn advertised_tools_and_permissions_share_one_contract() {
        let all_names = [
            "respond",
            "spawn_agent",
            "spawn_agents",
            "ipython",
            "agent_browser",
            "agent_inspect",
            "agent_await",
            "agent_release",
            "agent_retain",
            "agent_stage",
            "agent_replan",
        ];
        for role in [AgentRole::Background, AgentRole::Worker] {
            let advertised = names(role, false);
            for name in all_names {
                assert_eq!(
                    role.allows_tool(name),
                    advertised.iter().any(|advertised| advertised == name),
                    "permission mismatch for {role:?}/{name}"
                );
            }
        }
    }

    #[test]
    fn force_delegation_does_not_change_worker_tools() {
        assert_eq!(
            names(AgentRole::Worker, true),
            names(AgentRole::Worker, false)
        );
    }

    #[test]
    fn prompts_match_role_tool_boundaries() {
        let cfg = Config::default();
        let background = AgentRole::Background.system_prompt(&cfg);
        assert!(background.contains("`spawn_agent`"));
        assert!(background.contains("no local execution or browser tools"));
        assert!(!background.contains("`ipython`"));
        assert!(!background.contains("`agent_browser`"));
        assert!(background.contains("Never address the user"));

        let worker = AgentRole::Worker.system_prompt(&cfg);
        assert!(worker.contains("`ipython`"));
        assert!(worker.contains("`agent_browser`"));
        assert!(!worker.contains("`spawn_agent`"));
        assert!(worker.contains("assigned workspace"));
        assert!(worker.len() < 750);
    }

    #[test]
    fn prompts_preserve_identity_and_role_personas() {
        let cfg = Config {
            names: tachyon_util::config::Names {
                ..Default::default()
            },
            background: Some(AgentConfig {
                persona: Some("background persona".into()),
                ..Default::default()
            }),
            worker: Some(AgentConfig {
                persona: Some("worker persona".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(AgentRole::Background
            .system_prompt(&cfg)
            .ends_with("background persona"));
        assert!(AgentRole::Worker
            .system_prompt(&cfg)
            .ends_with("worker persona"));
    }
}
