#![forbid(unsafe_code)]

use tachyon_orchestrator::{agents, registry::*, tools::ToolSchema};

fn fake_prompt(context: InvocationContext<'_>) -> Result<String, RegistryError> {
    Ok(format!(
        "test role: {}",
        context.persona.unwrap_or("default")
    ))
}

fn fake_tools() -> Vec<ToolSchema> {
    Vec::new()
}

const FAKE: RoleDescriptor = RoleDescriptor {
    id: RoleId::Custom("test-role"),
    stable_id: "test-role",
    display_name: "Test Role",
    purpose: "Prove registration needs no kernel dispatch changes.",
    capabilities: &[],
    host_lane: HostLane::Background,
    output_visibility: OutputVisibility::Internal,
    enabled: true,
    invocations: &[InvocationBinding {
        kind: InvocationKind::Primary,
        prompt: fake_prompt,
        tools: fake_tools,
    }],
};

#[test]
fn borrowed_registry_adds_disables_and_removes_roles_without_kernel_changes() {
    let roles = [agents::conversation::definition(), FAKE];
    let registry = Registry::new(&roles).unwrap();
    let result = registry
        .resolve(FAKE.id, HostLane::Background, InvocationKind::Primary)
        .unwrap()
        .render(InvocationContext {
            identity: None,
            persona: Some("custom"),
        })
        .unwrap();
    assert_eq!(result.prompt, "test role: custom");
    assert!(result.tools.is_empty());
    assert_eq!(result.output_visibility, OutputVisibility::Internal);

    let disabled = [RoleDescriptor {
        enabled: false,
        ..FAKE
    }];
    assert_eq!(
        Registry::new(&disabled)
            .unwrap()
            .resolve(FAKE.id, HostLane::Background, InvocationKind::Primary)
            .unwrap_err(),
        RegistryError::DisabledRole(FAKE.id)
    );
    assert_eq!(
        Registry::new(&roles[..1])
            .unwrap()
            .resolve(FAKE.id, HostLane::Background, InvocationKind::Primary)
            .unwrap_err(),
        RegistryError::UnknownRole(FAKE.id)
    );
}

#[test]
fn registration_rejects_duplicate_roles_invocations_and_inconsistent_ids() {
    assert_eq!(
        Registry::new(&[FAKE, FAKE]).unwrap_err(),
        RegistryError::DuplicateRole(FAKE.id)
    );
    let duplicate = RoleDescriptor {
        invocations: &[
            InvocationBinding {
                kind: InvocationKind::Primary,
                prompt: fake_prompt,
                tools: fake_tools,
            },
            InvocationBinding {
                kind: InvocationKind::Primary,
                prompt: fake_prompt,
                tools: fake_tools,
            },
        ],
        ..FAKE
    };
    assert_eq!(
        Registry::new(&[duplicate]).unwrap_err(),
        RegistryError::DuplicateInvocation(FAKE.id, InvocationKind::Primary)
    );
    assert_eq!(
        Registry::new(&[RoleDescriptor {
            stable_id: "wrong",
            ..FAKE
        }])
        .unwrap_err(),
        RegistryError::InvalidStableId(FAKE.id)
    );
    // Alternate typed spellings cannot bypass stable-ID uniqueness.
    let alias = RoleDescriptor {
        id: RoleId::Custom("conversation"),
        ..agents::conversation::definition()
    };
    assert_eq!(
        Registry::new(&[agents::conversation::definition(), alias]).unwrap_err(),
        RegistryError::DuplicateRole(alias.id)
    );
}

#[test]
fn custom_spelling_resolves_known_stable_id_without_bypassing_disabled_role() {
    let alias = RoleId::Custom("conversation");
    assert_eq!(
        builtin()
            .resolve(alias, HostLane::Foreground, InvocationKind::Primary)
            .unwrap()
            .descriptor()
            .id,
        RoleId::Conversation
    );
    let roles = [RoleDescriptor {
        id: alias,
        enabled: false,
        ..agents::conversation::definition()
    }];
    for id in [alias, RoleId::Conversation] {
        assert_eq!(
            Registry::new(&roles)
                .unwrap()
                .resolve(id, HostLane::Foreground, InvocationKind::Primary)
                .unwrap_err(),
            RegistryError::DisabledRole(id)
        );
    }
    let uppercase = RoleId::Custom("Conversation");
    assert_eq!(
        builtin()
            .resolve(uppercase, HostLane::Foreground, InvocationKind::Primary)
            .unwrap_err(),
        RegistryError::UnknownRole(uppercase)
    );
}

#[test]
fn resolution_fails_closed_without_role_lane_or_invocation_fallback() {
    let registry = builtin();
    assert_eq!(
        registry
            .resolve(FAKE.id, HostLane::Background, InvocationKind::Primary)
            .unwrap_err(),
        RegistryError::UnknownRole(FAKE.id)
    );
    for (id, wrong_lane) in [
        (RoleId::Conversation, HostLane::Background),
        (RoleId::Coordinator, HostLane::Foreground),
    ] {
        assert_eq!(
            registry
                .resolve(id, wrong_lane, InvocationKind::Primary)
                .unwrap_err(),
            RegistryError::WrongHostLane(id, wrong_lane)
        );
    }
    assert_eq!(
        registry
            .resolve(
                RoleId::Conversation,
                HostLane::Foreground,
                InvocationKind::Review
            )
            .unwrap_err(),
        RegistryError::UnsupportedInvocation(RoleId::Conversation, InvocationKind::Review)
    );
    assert_eq!(
        registry
            .resolve(
                RoleId::Conversation,
                HostLane::Foreground,
                InvocationKind::Primary
            )
            .unwrap()
            .render(InvocationContext::default())
            .unwrap_err(),
        RegistryError::MissingConversationIdentity
    );
}

#[test]
fn invocation_tools_match_original_schema_snapshots() {
    let context = InvocationContext {
        identity: Some(ConversationIdentity {
            user_name: "you",
            conversation_name: "Conversational Agent",
        }),
        persona: None,
    };
    for (id, lane, kind, visibility, fixture, names) in [
        (
            RoleId::Conversation,
            HostLane::Foreground,
            InvocationKind::Primary,
            OutputVisibility::UserFacing,
            "conversation",
            vec![
                "spawn_agent",
                "spawn_agents",
                "memory",
                "schedule",
                "todo",
                "campaign",
                "websearch",
                "webfetch",
            ],
        ),
        (
            RoleId::Coordinator,
            HostLane::Background,
            InvocationKind::Primary,
            OutputVisibility::Internal,
            "coordinator",
            vec![
                "spawn_agent",
                "spawn_agents",
                "agent_inspect",
                "agent_await",
                "agent_release",
                "agent_retain",
                "agent_stage",
                "agent_replan",
            ],
        ),
        (
            RoleId::Coordinator,
            HostLane::Background,
            InvocationKind::Review,
            OutputVisibility::Internal,
            "review",
            vec!["submit_work_review"],
        ),
    ] {
        let rendered = builtin()
            .resolve(id, lane, kind)
            .unwrap()
            .render(context)
            .unwrap();
        assert_eq!(rendered.output_visibility, visibility);
        assert_eq!(
            rendered
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            names
        );
        // Conversation appends Todo and linked campaign contracts; preserve the original
        // frozen schemas and prompt bytes rather than rewriting legacy goldens.
        let legacy_tools = if id == RoleId::Conversation {
            assert_eq!(
                &rendered.tools[5],
                &tachyon_orchestrator::conversation::tools::campaign()
            );
            assert_eq!(
                rendered.tools[6],
                tachyon_orchestrator::conversation::tools::web(false)
            );
            assert_eq!(
                rendered.tools[7],
                tachyon_orchestrator::conversation::tools::web(true)
            );
            assert_eq!(
                rendered.tools[4],
                tachyon_orchestrator::conversation::tools::todo()
            );
            &rendered.tools[..4]
        } else {
            &rendered.tools[..]
        };
        let schemas: Vec<_> = legacy_tools.iter().map(|tool| serde_json::json!({
            "name": tool.name, "description": tool.description, "parameters": tool.parameters,
        })).collect();
        let actual = format!("{}\n", serde_json::to_string_pretty(&schemas).unwrap());
        let path = format!(
            "{}/tests/fixtures/{fixture}-tools.json",
            env!("CARGO_MANIFEST_DIR")
        );
        if std::env::var_os("UPDATE_ORCHESTRATOR_SNAPSHOTS").is_some() {
            std::fs::write(&path, &actual).unwrap();
        }
        assert_eq!(actual.as_bytes(), std::fs::read(path).unwrap());
    }
    assert_eq!(
        agents::coordinator::tools::REVIEW_TOOL,
        "submit_work_review"
    );
}
