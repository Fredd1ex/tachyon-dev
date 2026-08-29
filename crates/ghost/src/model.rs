//! Ghost compatibility adapter for the shared model runtime.

use tachyon_util::config::{AgentConfig, Config};

use crate::error::GhostError;

pub use tachyon_model::{
    ChatMessage, Completion, Content, Model, ModelError, Role, TokenUsage, ToolCall, ToolSpec,
};

pub type Result<T> = std::result::Result<T, GhostError>;

pub fn from_agent_config(agent: &AgentConfig) -> Result<Model> {
    let cfg = Config::load();
    let api_key = cfg.resolve_key("openrouter").ok_or(GhostError::NoApiKey)?;
    let configured = agent.model(&cfg.model);
    let model_name = configured
        .name
        .clone()
        .unwrap_or_else(|| Config::default_model().into());
    #[cfg(unix)]
    {
        use std::io::Write;
        let source = if std::env::var("OPENROUTER_API_KEY")
            .map(|value| !value.is_empty())
            .unwrap_or(false)
        {
            "environment"
        } else {
            "credential store"
        };
        let _ = std::io::stderr()
            .write_all(format!("[ghost] model: {model_name} ({source})\n").as_bytes());
    }
    Ok(Model::new(tachyon_model::ModelConfig {
        base_url: cfg.provider_base_url(),
        api_key,
        model: model_name,
        temperature: configured.temperature.unwrap_or(0.2),
        max_completion_tokens: configured.max_completion_tokens,
        context_length: configured.context_length,
        parallel_tool_calls: configured.parallel_tool_calls,
        reasoning: configured.reasoning,
        routing: cfg.provider_routing(),
        debug: std::env::var("TACHYON_DEBUG")
            .map(|value| value == "1" || value == "true")
            .unwrap_or(false),
        debug_log: Some(tachyon_util::daemon::logs_dir().join("debug-http.log")),
    }))
}
