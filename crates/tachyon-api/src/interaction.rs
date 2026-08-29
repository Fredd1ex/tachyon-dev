//! Versioned protocol between Tachyond and the foreground Interaction Manager.

use serde::{Deserialize, Serialize};

use crate::{AgentState, EventEnvelope, LifetimeClass};

pub const INTERACTION_PROTOCOL_VERSION: u16 = 1;

/// Correlation metadata shared by foreground commands and events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InteractionMetadata {
    pub protocol_version: u16,
    pub message_id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub conversation_id: String,
    pub turn_id: Option<String>,
    pub generation: u64,
    pub occurred_at_ms: u64,
}

impl InteractionMetadata {
    pub fn new(
        message_id: impl Into<String>,
        correlation_id: impl Into<String>,
        conversation_id: impl Into<String>,
        occurred_at_ms: u64,
    ) -> Self {
        Self {
            protocol_version: INTERACTION_PROTOCOL_VERSION,
            message_id: message_id.into(),
            correlation_id: correlation_id.into(),
            causation_id: None,
            conversation_id: conversation_id.into(),
            turn_id: None,
            generation: 0,
            occurred_at_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InteractionCommandEnvelope {
    #[serde(flatten)]
    pub metadata: InteractionMetadata,
    #[serde(flatten)]
    pub command: InteractionCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum InteractionCommand {
    BeginConversation { reset: bool },
    AcceptUserTurn { text: String },
    CancelConversation { reason: String },
    PublishBackgroundUpdate { event: EventEnvelope },
    RestoreOperationalState { sessions: Vec<RecoveredSession> },
    NotifyUser { text: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveredSession {
    pub session_id: String,
    pub task_type: String,
    pub description: String,
    pub state: AgentState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InteractionEventEnvelope {
    #[serde(flatten)]
    pub metadata: InteractionMetadata,
    #[serde(flatten)]
    pub event: InteractionEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum InteractionEvent {
    UserTurnAccepted { text: String },
    ConversationDelta { text: String },
    ConversationFinished { text: String },
    ConversationIntentProduced { intents: Vec<InteractionIntent> },
    ForegroundRequestTimedOut { deadline_ms: u64 },
    UserVisibleNotificationPublished { text: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "intent", rename_all = "snake_case")]
pub enum InteractionIntent {
    StartTasks {
        tasks: Vec<TaskIntent>,
    },
    CancelTask {
        task_id: String,
    },
    SteerTask {
        task_id: String,
        instruction: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskIntent {
    pub objective: String,
    pub purpose: String,
    pub lifetime_class: LifetimeClass,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Actor, AgentEvent, FOREGROUND_ID};

    fn metadata() -> InteractionMetadata {
        InteractionMetadata {
            protocol_version: INTERACTION_PROTOCOL_VERSION,
            message_id: "command-7".into(),
            correlation_id: "interaction-3".into(),
            causation_id: Some("request-2".into()),
            conversation_id: FOREGROUND_ID.into(),
            turn_id: Some("3".into()),
            generation: 4,
            occurred_at_ms: 123,
        }
    }

    #[test]
    fn user_turn_command_round_trips_with_correlation_metadata() {
        let envelope = InteractionCommandEnvelope {
            metadata: metadata(),
            command: InteractionCommand::AcceptUserTurn {
                text: "first line\nsecond line".into(),
            },
        };
        let wire = serde_json::to_string(&envelope).unwrap();
        assert!(wire.contains(r#""command":"accept_user_turn""#));
        assert!(wire.contains(r#""protocol_version":1"#));
        assert_eq!(
            serde_json::from_str::<InteractionCommandEnvelope>(&wire).unwrap(),
            envelope
        );
    }

    #[test]
    fn background_update_preserves_the_correlated_worker_event() {
        let worker = EventEnvelope {
            event_id: 9,
            session_id: "worker-1".into(),
            conversation_id: Some(FOREGROUND_ID.into()),
            turn_id: Some("3".into()),
            task_id: Some("task-1".into()),
            parent_task_id: None,
            tool_call_id: Some("call-1".into()),
            actor: Actor::Worker {
                id: "worker-1".into(),
            },
            sequence: 2,
            occurred_at_ms: 124,
            kind: AgentEvent::WorkerCompleted {
                worker_id: "worker-1".into(),
                objective: "check weather".into(),
                result: "rain".into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: false,
            },
        };
        let envelope = InteractionCommandEnvelope {
            metadata: metadata(),
            command: InteractionCommand::PublishBackgroundUpdate {
                event: worker.clone(),
            },
        };
        let wire = serde_json::to_string(&envelope).unwrap();
        let decoded: InteractionCommandEnvelope = serde_json::from_str(&wire).unwrap();
        assert_eq!(decoded, envelope);
        assert!(matches!(
            decoded.command,
            InteractionCommand::PublishBackgroundUpdate { event } if event == worker
        ));
    }

    #[test]
    fn conversation_intents_round_trip_without_runtime_dependencies() {
        let envelope = InteractionEventEnvelope {
            metadata: metadata(),
            event: InteractionEvent::ConversationIntentProduced {
                intents: vec![InteractionIntent::StartTasks {
                    tasks: vec![TaskIntent {
                        objective: "compare forecasts".into(),
                        purpose: "weather".into(),
                        lifetime_class: LifetimeClass::Short,
                    }],
                }],
            },
        };
        let wire = serde_json::to_string(&envelope).unwrap();
        assert!(wire.contains(r#""event":"conversation_intent_produced""#));
        assert!(wire.contains(r#""intent":"start_tasks""#));
        assert_eq!(
            serde_json::from_str::<InteractionEventEnvelope>(&wire).unwrap(),
            envelope
        );
    }

    #[test]
    fn operational_recovery_is_a_typed_non_user_command() {
        let envelope = InteractionCommandEnvelope {
            metadata: metadata(),
            command: InteractionCommand::RestoreOperationalState {
                sessions: vec![RecoveredSession {
                    session_id: "worker-1".into(),
                    task_type: "research".into(),
                    description: "compare sources".into(),
                    state: AgentState::Waiting,
                }],
            },
        };
        let wire = serde_json::to_string(&envelope).unwrap();
        assert!(!wire.contains('\n'));
        assert!(wire.contains(r#""command":"restore_operational_state""#));
        assert_eq!(
            serde_json::from_str::<InteractionCommandEnvelope>(&wire).unwrap(),
            envelope
        );
    }
}
