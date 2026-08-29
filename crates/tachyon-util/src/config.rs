#![forbid(unsafe_code)]

//! Shared Tachyon config. The CLI, daemon, Foreground runtime, and Ghost harness
//! read this file. Only the CLI writes it.

use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub use tachyon_model::{ProviderRouting, Reasoning, RoutingPreferences, RoutingProfile};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<Provider>,
    #[serde(default, deserialize_with = "deserialize_model")]
    pub model: Model,
    #[serde(default)]
    pub names: Names,
    #[serde(default, alias = "orchestrator")]
    pub foreground: Foreground,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<AgentConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<AgentConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<AgentConfig>,
}

/// Role-specific overrides. Omitted fields inherit the legacy shared model
/// settings so existing configurations continue to behave predictably.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_length: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Reasoning>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persona: Option<String>,
}

impl AgentConfig {
    pub fn model(&self, fallback: &Model) -> Model {
        Model {
            name: self.model.clone().or_else(|| fallback.name.clone()),
            temperature: self.temperature.or(fallback.temperature),
            context_length: self.context_length.or(fallback.context_length),
            max_completion_tokens: self
                .max_completion_tokens
                .or(fallback.max_completion_tokens),
            parallel_tool_calls: self
                .parallel_tool_calls
                .unwrap_or(fallback.parallel_tool_calls),
            reasoning: self
                .reasoning
                .clone()
                .unwrap_or_else(|| fallback.reasoning.clone()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Model {
    /// OpenRouter model identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    /// Provider context window, used for UI usage percentages and validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_length: Option<u32>,
    /// Maximum number of completion tokens requested from the provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,
    /// Allow the provider to execute independent tool calls concurrently.
    #[serde(default = "default_parallel_tool_calls")]
    pub parallel_tool_calls: bool,
    #[serde(default)]
    pub reasoning: Reasoning,
}

fn default_parallel_tool_calls() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ModelConfigValue {
    Structured(Model),
    Legacy(String),
}

fn deserialize_model<'de, D>(deserializer: D) -> Result<Model, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match ModelConfigValue::deserialize(deserializer)? {
        ModelConfigValue::Structured(model) => model,
        ModelConfigValue::Legacy(name) => Model {
            name: Some(name),
            temperature: None,
            context_length: None,
            max_completion_tokens: None,
            parallel_tool_calls: default_parallel_tool_calls(),
            reasoning: Reasoning::default(),
        },
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Names {
    /// The name shown for the human user in the TUI/chat.
    #[serde(default = "default_user_name")]
    pub user: String,
    /// The name shown for the Conversational Agent.
    #[serde(default = "default_conversation_name")]
    pub conversation: String,
    /// Legacy V1 foreground name retained only when reading older config files.
    #[serde(default, rename = "orchestrator", skip_serializing)]
    pub legacy_foreground_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Foreground {
    /// Additional user-controlled personality guidance for the foreground.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persona: Option<String>,
}

fn default_user_name() -> String {
    "you".into()
}

fn default_conversation_name() -> String {
    "Conversational Agent".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Provider {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// OpenRouter request routing preferences shared by all agent roles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<ProviderRouting>,
}

impl Config {
    pub fn default_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("tachyon")
            .join("config.toml")
    }

    pub fn load() -> Self {
        Self::load_from(&Self::default_path())
    }

    pub fn load_from(path: &Path) -> Self {
        match fs::read_to_string(path) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let toml = toml::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(path, toml)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    /// Resolve the active API key for a provider without reading config files.
    /// Environment values take precedence over the operating system credential
    /// store, keeping temporary shell overrides possible without persisting them.
    /// Known test/placeholder values are treated as unset so the harness fails
    /// with a clear "no key" error instead of making authenticated calls with
    /// a fake credential.
    pub fn default_model() -> &'static str {
        "~deepseek/deepseek-v4-flash-latest"
    }

    pub fn active_model(&self) -> String {
        self.model
            .name
            .clone()
            .unwrap_or_else(|| Self::default_model().into())
    }

    pub fn temperature(&self) -> f32 {
        self.model.temperature.unwrap_or(0.2)
    }

    pub fn provider_base_url(&self) -> String {
        self.provider
            .as_ref()
            .and_then(|p| p.base_url.clone())
            .unwrap_or_else(|| "https://openrouter.ai/api/v1".into())
    }

    pub fn provider_routing(&self) -> Option<ProviderRouting> {
        self.provider
            .as_ref()
            .and_then(|provider| provider.routing.clone())
    }

    /// The configured name for the human user.
    pub fn user_name(&self) -> String {
        if self.names.user.is_empty() {
            default_user_name()
        } else {
            self.names.user.clone()
        }
    }

    /// The configured name for the Conversational Agent.
    pub fn conversation_name(&self) -> String {
        if !self.names.conversation.is_empty() {
            self.names.conversation.clone()
        } else if !self.names.legacy_foreground_name.is_empty() {
            self.names.legacy_foreground_name.clone()
        } else {
            default_conversation_name()
        }
    }

    pub fn foreground_persona(&self) -> Option<&str> {
        self.foreground.persona.as_deref()
    }

    pub fn conversation_config(&self) -> AgentConfig {
        self.conversation.clone().unwrap_or_else(|| AgentConfig {
            persona: self.foreground.persona.clone(),
            ..AgentConfig::default()
        })
    }

    pub fn background_config(&self) -> AgentConfig {
        self.background.clone().unwrap_or_default()
    }

    /// Worker settings are independent from the Background Coordinator.
    pub fn worker_config(&self) -> AgentConfig {
        self.worker.clone().unwrap_or_default()
    }

    /// Resolve the active API key for a provider **from the environment only**.
    /// API keys are never stored in the config file; secrets stay out of it.
    /// Known test/placeholder values are treated as unset so the harness fails
    /// with a clear "no key" error instead of making authenticated calls with
    /// a fake credential.
    pub fn resolve_key(&self, provider: &str) -> Option<String> {
        let env_var = match provider {
            "openrouter" => "OPENROUTER_API_KEY",
            _ => return None,
        };
        let candidate = std::env::var(env_var)
            .ok()
            .or_else(|| crate::credentials::openrouter_key())
            .map(|v| v.trim().to_string());
        candidate.filter(|k| !Self::is_placeholder_key(k))
    }

    /// True if a key looks like a placeholder/test value that should not be
    /// sent to the provider.
    fn is_placeholder_key(k: &str) -> bool {
        k.is_empty()
            || k.starts_with("sk-test")
            || k == "sk-test1234567890abcdef"
            || k.starts_with("placeholder")
            || k.contains("your-api-key")
    }

    /// True if the primary provider has a key available.
    pub fn provider_ready(&self) -> bool {
        self.resolve_key("openrouter").is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_profile_selects_its_preferences() {
        let routing = ProviderRouting {
            profile: RoutingProfile::Cost,
            cost: RoutingPreferences {
                order: Some(vec!["Makora".into()]),
                ..RoutingPreferences::default()
            },
            performance: RoutingPreferences {
                sort: Some("throughput".into()),
                ..RoutingPreferences::default()
            },
            ..ProviderRouting::default()
        };

        assert_eq!(
            routing.active_preferences().order.as_ref().unwrap(),
            &vec!["Makora".to_string()]
        );
        assert_eq!(routing.active_preferences().sort, None);
    }

    #[test]
    fn legacy_routing_is_used_when_selected_profile_is_empty() {
        let routing: ProviderRouting = toml::from_str("order = [\"DeepInfra\"]").unwrap();

        assert_eq!(
            routing.active_preferences().order.as_ref().unwrap(),
            &vec!["DeepInfra".to_string()]
        );
    }

    #[test]
    fn conversation_and_background_overrides_are_independent() {
        let cfg = Config {
            model: Model {
                name: Some("shared".into()),
                temperature: Some(0.2),
                ..Model::default()
            },
            conversation: Some(AgentConfig {
                model: Some("conversation-model".into()),
                temperature: Some(0.1),
                context_length: Some(32_768),
                persona: Some("spoken".into()),
                ..AgentConfig::default()
            }),
            background: Some(AgentConfig {
                model: Some("background-model".into()),
                temperature: Some(0.4),
                context_length: Some(65_536),
                persona: Some("focused".into()),
                ..AgentConfig::default()
            }),
            ..Config::default()
        };

        let conversation = cfg.conversation_config();
        let background = cfg.background_config();
        let conversation_model = conversation.model(&cfg.model);
        let background_model = background.model(&cfg.model);

        assert_eq!(
            conversation_model.name.as_deref(),
            Some("conversation-model")
        );
        assert_eq!(background_model.name.as_deref(), Some("background-model"));
        assert_eq!(conversation_model.temperature, Some(0.1));
        assert_eq!(background_model.temperature, Some(0.4));
        assert_eq!(conversation_model.context_length, Some(32_768));
        assert_eq!(background_model.context_length, Some(65_536));
        assert_eq!(conversation.persona.as_deref(), Some("spoken"));
        assert_eq!(background.persona.as_deref(), Some("focused"));
    }

    #[test]
    fn legacy_persona_only_falls_back_to_conversation() {
        let cfg = Config {
            foreground: Foreground {
                persona: Some("legacy persona".into()),
            },
            ..Config::default()
        };

        assert_eq!(
            cfg.conversation_config().persona.as_deref(),
            Some("legacy persona")
        );
        assert!(cfg.background_config().persona.is_none());
    }

    #[test]
    fn worker_overrides_are_independent_from_background() {
        let cfg = Config {
            background: Some(AgentConfig {
                model: Some("background-model".into()),
                context_length: Some(65_536),
                ..AgentConfig::default()
            }),
            worker: Some(AgentConfig {
                model: Some("worker-model".into()),
                context_length: Some(131_072),
                ..AgentConfig::default()
            }),
            ..Config::default()
        };

        assert_eq!(
            cfg.background_config().model.as_deref(),
            Some("background-model")
        );
        assert_eq!(cfg.worker_config().model.as_deref(), Some("worker-model"));
        assert_eq!(cfg.worker_config().context_length, Some(131_072));
    }

    #[test]
    fn missing_worker_config_does_not_inherit_background() {
        let cfg = Config {
            background: Some(AgentConfig {
                model: Some("background-model".into()),
                context_length: Some(65_536),
                ..AgentConfig::default()
            }),
            ..Config::default()
        };

        assert!(cfg.worker_config().model.is_none());
        assert!(cfg.worker_config().context_length.is_none());
    }
}
