//! Central role profiles for model configuration and prompts.

use crate::harness;
use tachyon_orchestrator::background;
use tachyon_util::config::{AgentConfig, Config};

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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!worker.contains("`ipython`"));
        assert!(!worker.contains("`agent_browser`"));
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
