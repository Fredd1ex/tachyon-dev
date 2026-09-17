#![forbid(unsafe_code)]

#[path = "fixtures/prompts.rs"]
mod frozen;

use tachyon_orchestrator::registry::{
    self, ConversationIdentity, HostLane, InvocationContext, InvocationKind, RoleId,
};
use tachyon_orchestrator::{background::prompt as coordinator, conversation};

#[test]
fn conversation_rendered_bytes_match_original() {
    for (user_name, conversation_name) in [
        ("you", "Conversational Agent"),
        ("Freddie", "Jarvis"),
        ("{conversation_name}\nUser", "{user_name} Assistant"),
        ("", ""),
    ] {
        for persona in [
            None,
            Some("conversation persona"),
            Some(""),
            Some("  style\n{user_name}\n"),
        ] {
            let mut expected = format!(
                "You are {conversation_name}; the user is {user_name}. {}",
                frozen::CONVERSATION_BODY
            );
            if let Some(persona) = persona {
                expected.push_str(frozen::CONVERSATION_PERSONA);
                expected.push_str(persona);
            }
            let actual = conversation::prompt::system_prompt(conversation::prompt::PromptContext {
                user_name,
                conversation_name,
                persona,
            });
            assert_eq!(actual.as_bytes(), expected.as_bytes());
            let rendered = registry::builtin()
                .resolve(
                    RoleId::Conversation,
                    HostLane::Foreground,
                    InvocationKind::Primary,
                )
                .unwrap()
                .render(InvocationContext {
                    identity: Some(ConversationIdentity {
                        user_name,
                        conversation_name,
                    }),
                    persona,
                })
                .unwrap();
            assert_eq!(rendered.prompt.as_bytes(), expected.as_bytes());
        }
    }
}

#[test]
fn coordinator_rendered_bytes_match_original() {
    for persona in [
        None,
        Some("background persona"),
        Some(""),
        Some("  style\n{persona}\n"),
    ] {
        for (kind, render, base, heading) in [
            (
                InvocationKind::Primary,
                coordinator::system_prompt as fn(coordinator::PromptContext<'_>) -> String,
                frozen::COORDINATOR,
                frozen::COORDINATOR_PERSONA,
            ),
            (
                InvocationKind::Review,
                coordinator::review_system_prompt,
                frozen::REVIEW,
                frozen::REVIEW_PERSONA,
            ),
        ] {
            let mut expected = base.to_string();
            if let Some(persona) = persona {
                expected.push_str(heading);
                expected.push_str(persona);
            }
            assert_eq!(
                render(coordinator::PromptContext { persona }).as_bytes(),
                expected.as_bytes()
            );
            let rendered = registry::builtin()
                .resolve(RoleId::Coordinator, HostLane::Background, kind)
                .unwrap()
                .render(InvocationContext {
                    identity: None,
                    persona,
                })
                .unwrap();
            assert_eq!(rendered.prompt.as_bytes(), expected.as_bytes());
        }
    }
}

#[test]
fn auxiliary_prompt_bytes_match_original() {
    assert_eq!(
        conversation::prompt::SYNTHESIS_PROMPT.as_bytes(),
        frozen::SYNTHESIS.as_bytes()
    );
    assert_eq!(
        conversation::policy::CLASSIFICATION_PROMPT.as_bytes(),
        frozen::CLASSIFICATION.as_bytes()
    );
    assert_eq!(
        conversation::policy::ANSWERABILITY_PROMPT.as_bytes(),
        frozen::ANSWERABILITY.as_bytes()
    );
}
