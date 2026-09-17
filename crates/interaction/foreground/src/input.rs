//! Versioned commands and legacy foreground input decoding.

use tachyon_api::types::{AgentEvent, ContextCompactionCommand, EventEnvelope};
use tachyon_api::{
    InteractionCommand, InteractionCommandEnvelope, InteractionMetadata, RecoveredSession,
};

use super::{runtime::AgentRole, streaming::synthetic_interaction_metadata, turns::EvidenceRecord};

pub(super) enum ChatInput {
    User {
        text: String,
        metadata: InteractionMetadata,
    },
    Evidence(EvidenceRecord),
    Recovery(Vec<RecoveredSession>),
    Notification {
        text: String,
        metadata: InteractionMetadata,
        model: bool,
    },
    Compaction(ContextCompactionCommand),
    Ignore,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::types::Actor;
    use tachyon_api::{RecoveredSession, FOREGROUND_ID};
    fn interaction_metadata() -> tachyon_api::InteractionMetadata {
        tachyon_api::InteractionMetadata::new("command-1", "turn-1", FOREGROUND_ID, 1)
    }
    #[test]
    fn conversation_accepts_versioned_multiline_user_turns() {
        let line = serde_json::to_string(&InteractionCommandEnvelope {
            metadata: interaction_metadata(),
            command: InteractionCommand::AcceptUserTurn {
                text: "first line\nsecond line".into(),
            },
        })
        .unwrap();
        match decode_chat_input(&line, AgentRole::Conversation) {
            ChatInput::User { text, metadata } => {
                assert_eq!(text, "first line\nsecond line");
                assert_eq!(metadata.message_id, "command-1");
                assert_eq!(metadata.correlation_id, "turn-1");
            }
            _ => panic!("expected a user turn"),
        }
    }

    #[test]
    fn operational_recovery_never_becomes_a_user_turn() {
        let sessions = vec![RecoveredSession {
            session_id: "worker-1".into(),
            task_type: "research".into(),
            description: "compare sources".into(),
            state: tachyon_api::AgentState::Waiting,
        }];
        let line = serde_json::to_string(&InteractionCommandEnvelope {
            metadata: interaction_metadata(),
            command: InteractionCommand::RestoreOperationalState {
                sessions: sessions.clone(),
            },
        })
        .unwrap();
        match decode_chat_input(&line, AgentRole::Conversation) {
            ChatInput::Recovery(decoded) => assert_eq!(decoded, sessions),
            _ => panic!("expected operational recovery"),
        }
    }

    #[test]
    fn conversation_accepts_correlated_background_updates() {
        let event = EventEnvelope {
            event_id: 7,
            session_id: "worker-1".into(),
            conversation_id: Some(FOREGROUND_ID.into()),
            turn_id: Some("1".into()),
            task_id: Some("task-1".into()),
            parent_task_id: None,
            tool_call_id: Some("call-1".into()),
            actor: Actor::Worker {
                id: "worker-1".into(),
            },
            sequence: 1,
            occurred_at_ms: 1,
            kind: AgentEvent::WorkerCompleted {
                worker_id: "worker-1".into(),
                objective: "weather".into(),
                result: "rain".into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: false,
            },
        };
        let line = serde_json::to_string(&InteractionCommandEnvelope {
            metadata: interaction_metadata(),
            command: InteractionCommand::PublishBackgroundUpdate {
                event: event.clone(),
            },
        })
        .unwrap();
        match decode_chat_input(&line, AgentRole::Conversation) {
            ChatInput::Evidence(EvidenceRecord::Correlated(decoded)) => {
                assert_eq!(decoded, event)
            }
            _ => panic!("expected correlated evidence"),
        }
    }

    #[test]
    fn versioned_command_wire_bytes_preserve_host_context() {
        let wire = r#"{"protocol_version":1,"message_id":"command-1","correlation_id":"request-1","causation_id":null,"conversation_id":"conversation-1","turn_id":"2","generation":0,"occurred_at_ms":123,"cwd":"/host/workspace","command":"accept_user_turn","text":"first\nsecond"}"#;
        let envelope: InteractionCommandEnvelope = serde_json::from_str(wire).unwrap();
        assert_eq!(serde_json::to_string(&envelope).unwrap(), wire);
        let ChatInput::User { text, metadata } = decode_chat_input(wire, AgentRole::Conversation)
        else {
            panic!("expected user turn");
        };
        assert_eq!(text, "first\nsecond");
        assert_eq!(metadata.cwd.as_deref(), Some("/host/workspace"));
        assert_eq!(metadata.turn_id.as_deref(), Some("2"));
        assert!(matches!(
            decode_chat_input(
                &wire.replacen("\"protocol_version\":1", "\"protocol_version\":99", 1),
                AgentRole::Conversation
            ),
            ChatInput::Ignore
        ));
    }
}

pub(super) fn decode_event(data: &str) -> Option<AgentEvent> {
    serde_json::from_str::<EventEnvelope>(data)
        .map(|envelope| envelope.kind)
        .or_else(|_| serde_json::from_str::<AgentEvent>(data))
        .ok()
}

fn decode_evidence(data: &str) -> Option<EvidenceRecord> {
    serde_json::from_str::<EventEnvelope>(data)
        .map(EvidenceRecord::Correlated)
        .or_else(|_| serde_json::from_str::<AgentEvent>(data).map(EvidenceRecord::Legacy))
        .ok()
}

pub(super) fn decode_chat_input(line: &str, role: AgentRole) -> ChatInput {
    if let Ok(command) = serde_json::from_str::<ContextCompactionCommand>(line) {
        return ChatInput::Compaction(command);
    }
    if role == AgentRole::Conversation {
        if let Ok(envelope) = serde_json::from_str::<InteractionCommandEnvelope>(line) {
            if envelope.metadata.protocol_version != tachyon_api::INTERACTION_PROTOCOL_VERSION {
                return ChatInput::Ignore;
            }
            let metadata = envelope.metadata;
            return match envelope.command {
                InteractionCommand::AcceptUserTurn { text } => {
                    let text = text.trim().to_string();
                    if text.is_empty() {
                        ChatInput::Ignore
                    } else {
                        ChatInput::User { text, metadata }
                    }
                }
                InteractionCommand::PublishBackgroundUpdate { event } => {
                    ChatInput::Evidence(EvidenceRecord::Correlated(event))
                }
                InteractionCommand::RestoreOperationalState { sessions } => {
                    ChatInput::Recovery(sessions)
                }
                InteractionCommand::NotifyUser { text, model } => ChatInput::Notification {
                    text,
                    metadata,
                    model,
                },
                InteractionCommand::BeginConversation { .. }
                | InteractionCommand::CancelConversation { .. } => ChatInput::Ignore,
            };
        }
    }
    let text = line.trim().to_string();
    if text.is_empty() {
        ChatInput::Ignore
    } else if let Some(data) = text.strip_prefix("[daemon:evidence] ") {
        decode_evidence(data)
            .map(ChatInput::Evidence)
            .unwrap_or(ChatInput::Ignore)
    } else {
        ChatInput::User {
            text,
            metadata: synthetic_interaction_metadata(None),
        }
    }
}
