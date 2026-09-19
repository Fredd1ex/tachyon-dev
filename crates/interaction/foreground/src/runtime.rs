//! Conversation role adapter and configured model construction.

use tachyon_model::{Model, ToolSpec};
use tachyon_orchestrator::registry::{
    self, ConversationIdentity, HostLane, InvocationContext, InvocationKind, Registry, RoleId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AgentRole {
    Conversation,
}

impl AgentRole {
    pub(super) fn config(
        self,
        cfg: &tachyon_util::config::Config,
    ) -> tachyon_util::config::AgentConfig {
        cfg.conversation_config()
    }

    pub(super) fn primary(
        self,
        registry: &Registry<'_>,
        cfg: &tachyon_util::config::Config,
    ) -> Result<tachyon_orchestrator::registry::RenderedInvocation, String> {
        let user_name = cfg.user_name();
        let conversation_name = cfg.conversation_name();
        let configured = cfg.conversation_config();
        let resolved = registry
            .resolve(
                RoleId::Conversation,
                HostLane::Foreground,
                InvocationKind::Primary,
            )
            .map_err(|error| error.to_string())?;
        let mut rendered = resolved
            .render(InvocationContext {
                identity: Some(ConversationIdentity {
                    user_name: &user_name,
                    conversation_name: &conversation_name,
                }),
                persona: configured.persona.as_deref(),
            })
            .map_err(|error| error.to_string())?;
        if rendered.output_visibility != registry::OutputVisibility::UserFacing {
            return Err("role registry: conversation output must be user-facing".into());
        }
        rendered.tools.retain(|tool| {
            matches!(
                tool.name.as_str(),
                "spawn_agent"
                    | "spawn_agents"
                    | "memory"
                    | "schedule"
                    | "todo"
                    | "campaign"
                    | "websearch"
                    | "webfetch"
            ) && (cfg.web.enabled || !matches!(tool.name.as_str(), "websearch" | "webfetch"))
                && resolved
                    .descriptor()
                    .capabilities
                    .iter()
                    .any(|cap| cap.tool_name() == tool.name)
        });
        Ok(rendered)
    }

    pub(super) fn tools(
        self,
        _force_delegation: bool,
        metadata: Option<&tachyon_api::InteractionMetadata>,
    ) -> Result<Vec<ToolSpec>, String> {
        Ok(self
            .registered_tools()?
            .iter()
            .filter(|tool| {
                metadata.is_some_and(|m| m.web_available())
                    || !matches!(tool.name.as_str(), "websearch" | "webfetch")
            })
            .cloned()
            .collect())
    }

    fn registered_tools(self) -> Result<&'static [ToolSpec], String> {
        // Built-in policy is immutable; authorization must not re-render prompts or schemas.
        static TOOLS: std::sync::OnceLock<Result<Vec<ToolSpec>, String>> =
            std::sync::OnceLock::new();
        TOOLS
            .get_or_init(|| {
                self.primary(&registry::builtin(), &Default::default())
                    .map(|rendered| {
                        rendered
                            .tools
                            .into_iter()
                            .map(|schema| {
                                ToolSpec::new(schema.name, schema.description, schema.parameters)
                            })
                            .collect()
                    })
            })
            .as_deref()
            .map_err(Clone::clone)
    }

    pub(super) fn allows_tool(
        self,
        name: &str,
        metadata: Option<&tachyon_api::InteractionMetadata>,
    ) -> Result<bool, String> {
        if matches!(name, "websearch" | "webfetch") && !metadata.is_some_and(|m| m.web_available())
        {
            return Ok(false);
        }
        Ok(self
            .registered_tools()?
            .iter()
            .any(|tool| tool.name == name))
    }
}

pub(super) fn from_agent_config(
    cfg: &tachyon_util::config::Config,
    agent: &tachyon_util::config::AgentConfig,
) -> Result<Model, String> {
    let api_key = cfg.resolve_key("openrouter").ok_or_else(|| {
        "no usable OpenRouter API key is available to the conversation runtime (environment or credential store)".to_string()
    })?;
    let configured = agent.model(&cfg.model);
    let model_name = configured
        .name
        .clone()
        .unwrap_or_else(|| tachyon_util::config::Config::default_model().into());
    eprintln!("[foreground] model: {model_name}");
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
        debug: std::env::var("TACHYON_DEBUG").is_ok_and(|value| value == "1" || value == "true"),
        debug_log: Some(tachyon_util::daemon::logs_dir().join("debug-http.log")),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_orchestrator::registry::{InvocationBinding, RegistryError};

    #[test]
    fn web_readiness_is_per_turn_fail_closed_and_matches_dispatch() {
        let role = AgentRole::Conversation;
        let mut metadata =
            tachyon_api::InteractionMetadata::new("message", "root", "foreground", 1);
        for available in [None, Some(true), Some(false), None, Some(true)] {
            metadata.web_availability = available.map(|available| tachyon_api::WebAvailability {
                available,
                reason: (!available).then(|| "Host service unavailable".into()),
            });
            let tools = role.tools(false, Some(&metadata)).unwrap();
            for name in ["websearch", "webfetch"] {
                assert_eq!(
                    tools.iter().any(|t| t.name == name),
                    available == Some(true)
                );
                assert_eq!(
                    role.allows_tool(name, Some(&metadata)).unwrap(),
                    available == Some(true)
                );
            }
            assert!(tools.iter().any(|t| t.name == "memory"));
        }
        assert!(!role.allows_tool("websearch", None).unwrap());
    }

    #[test]
    fn authorization_reuses_selection_and_does_not_share_mutable_turn_schemas() {
        let role = AgentRole::Conversation;
        assert!(std::ptr::eq(
            role.registered_tools().unwrap(),
            role.registered_tools().unwrap()
        ));
        let mut tools = role.tools(false, None).unwrap();
        tools.clear();
        for name in ["spawn_agent", "spawn_agents", "memory", "schedule"] {
            assert!(role.allows_tool(name, None).unwrap());
        }
        for name in [
            "agent_release",
            "agent_retain",
            "agent_stage",
            "agent_replan",
            "respond",
            "Memory",
        ] {
            assert!(!role.allows_tool(name, None).unwrap());
        }
    }

    #[test]
    fn primary_fails_closed_for_unavailable_bindings_and_render_errors() {
        let role = tachyon_orchestrator::agents::conversation::definition();
        // Bindings are static policy definitions, including injected failures.
        const BROKEN: &[InvocationBinding] = &[InvocationBinding {
            kind: InvocationKind::Primary,
            prompt: |_| Err(RegistryError::MissingConversationIdentity),
            tools: || panic!("tools must not render after prompt failure"),
        }];
        for roles in [
            vec![],
            vec![tachyon_orchestrator::registry::RoleDescriptor {
                enabled: false,
                ..role
            }],
            vec![tachyon_orchestrator::registry::RoleDescriptor {
                host_lane: HostLane::Background,
                ..role
            }],
            vec![tachyon_orchestrator::registry::RoleDescriptor {
                invocations: &[],
                ..role
            }],
            vec![tachyon_orchestrator::registry::RoleDescriptor {
                invocations: BROKEN,
                ..role
            }],
        ] {
            let registry = Registry::new(&roles).unwrap();
            assert!(AgentRole::Conversation
                .primary(&registry, &Default::default())
                .is_err());
        }
    }

    #[test]
    fn primary_preserves_ordered_schemas_and_intersects_host_capabilities() {
        let cfg = Default::default();
        let rendered = AgentRole::Conversation
            .primary(&registry::builtin(), &cfg)
            .unwrap();
        let expected = tachyon_orchestrator::agents::conversation::tools::primary();
        assert_eq!(rendered.tools.len(), expected.len());
        for (actual, expected) in rendered.tools.iter().zip(expected) {
            assert_eq!(actual.name, expected.name);
            assert_eq!(actual.description, expected.description);
            assert_eq!(actual.parameters, expected.parameters);
        }
        let roles = [tachyon_orchestrator::registry::RoleDescriptor {
            capabilities: &[tachyon_orchestrator::capabilities::Capability::Memory],
            ..tachyon_orchestrator::agents::conversation::definition()
        }];
        let registry = Registry::new(&roles).unwrap();
        let rendered = AgentRole::Conversation.primary(&registry, &cfg).unwrap();
        assert_eq!(
            rendered
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["memory"]
        );
        assert!(!AgentRole::Conversation
            .allows_tool("agent_release", None)
            .unwrap());

        use tachyon_orchestrator::capabilities::Capability;
        let roles = [tachyon_orchestrator::registry::RoleDescriptor {
            capabilities: &[
                Capability::Memory,
                Capability::Schedule,
                Capability::ReleaseWorker,
            ],
            invocations: &[InvocationBinding {
                kind: InvocationKind::Primary,
                prompt: |_| Ok("test prompt".into()),
                tools: || {
                    vec![
                        Capability::ReleaseWorker.schema(),
                        Capability::Memory.schema(),
                        Capability::DelegateOne.schema(),
                    ]
                },
            }],
            ..tachyon_orchestrator::agents::conversation::definition()
        }];
        let registry = Registry::new(&roles).unwrap();
        let rendered = AgentRole::Conversation.primary(&registry, &cfg).unwrap();
        // Lifecycle schemas cannot broaden this host; capabilities cannot invent absent schemas.
        assert_eq!(
            rendered
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["memory"]
        );
    }
}
