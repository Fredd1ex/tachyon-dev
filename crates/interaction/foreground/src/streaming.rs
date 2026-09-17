//! Foreground event correlation and stdout publication.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use tachyon_api::types::{Actor, AgentEvent, EventEnvelope};
use tachyon_api::{InteractionEvent, InteractionEventEnvelope, InteractionMetadata, FOREGROUND_ID};

use crate::runtime::AgentRole;

pub(super) const QUEUED_TURN_ACKNOWLEDGEMENT: &str =
    "Let me pull that together and I'll get back to you shortly.";
pub(super) const CONCURRENT_TURN_ACKNOWLEDGEMENT: &str =
    "Absolutely - I'll handle that while I keep the other request moving.";

struct EventContext {
    session_id: String,
    conversation_id: Option<String>,
    actor: Actor,
}

static EVENT_CONTEXT: OnceLock<EventContext> = OnceLock::new();
static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static LEGACY_INPUT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(super) fn session_id() -> &'static str {
    EVENT_CONTEXT
        .get()
        .map(|context| context.session_id.as_str())
        .unwrap_or("foreground")
}

pub(super) fn emit_turn(turn: Option<u64>, message: String) {
    if let Some(rest) = message.strip_prefix("[status] ") {
        emit_event(status_event(turn, rest));
    } else if let Some(message) = message.strip_prefix("[foreground:error] ") {
        emit_event(AgentEvent::Error {
            turn,
            message: message.to_string(),
        });
    } else if let Some(rest) = message.strip_prefix("[tool:") {
        if let Some((id, rest)) = rest.split_once("] ") {
            let mut parts = rest.splitn(2, ' ');
            emit_event(AgentEvent::ToolStarted {
                turn,
                id: id.to_string(),
                name: parts.next().unwrap_or_default().to_string(),
                arguments: parts.next().unwrap_or_default().to_string(),
            });
        }
    }
    if let Some(turn) = turn {
        println!("[turn:{turn}] {message}");
    } else {
        println!("{message}");
    }
}

pub(super) fn status_event(turn: Option<u64>, status: &str) -> AgentEvent {
    let mut parts = status.splitn(2, ' ');
    AgentEvent::Status {
        turn,
        phase: parts.next().unwrap_or("working").to_string(),
        message: parts.next().unwrap_or_default().to_string(),
    }
}

pub(super) fn emit_queued_turn(turn: u64, acknowledgement: Option<&str>) {
    emit_turn(
        Some(turn),
        format!(
            "[status] queued {}",
            acknowledgement.unwrap_or(QUEUED_TURN_ACKNOWLEDGEMENT)
        ),
    );
}

pub(super) fn emit_turn_block(turn: Option<u64>, kind: &str, text: &str) {
    if kind == "[agent]" {
        emit_event(AgentEvent::Reply {
            turn,
            text: text.to_string(),
            final_reply: true,
        });
        if turn.is_some() {
            return;
        }
    } else if let Some(id) = kind
        .strip_prefix("[tool-result:")
        .and_then(|s| s.strip_suffix(']'))
    {
        emit_event(AgentEvent::ToolFinished {
            turn,
            id: id.to_string(),
            output: text.to_string(),
        });
    }
    for line in text.lines() {
        emit_turn(turn, format!("{kind} {line}"));
    }
    if text.is_empty() {
        emit_turn(turn, kind.to_string());
    }
}

pub(super) fn emit_acknowledgement(turn: Option<u64>, text: &str) {
    emit_turn(turn, format!("[status] working {text}"));
}

pub(super) fn emit_event(event: AgentEvent) {
    let sequence = EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let turn_id = event_turn(&event).map(|turn| turn.to_string());
    let tool_call_id = match &event {
        AgentEvent::ToolStarted { id, .. } | AgentEvent::ToolFinished { id, .. } => {
            Some(id.clone())
        }
        _ => None,
    };
    let task_id = match &event {
        AgentEvent::WorkerCompleted { worker_id, .. } => Some(worker_id.clone()),
        AgentEvent::WorkProgress { event } => Some(event.work_id.clone()),
        AgentEvent::WorkResult { result } => Some(result.work_id.clone()),
        _ => None,
    };
    let fallback = EventContext {
        session_id: format!("foreground-{}", std::process::id()),
        conversation_id: None,
        actor: Actor::System,
    };
    let context = EVENT_CONTEXT.get().unwrap_or(&fallback);
    let envelope = EventEnvelope {
        event_id: sequence,
        session_id: context.session_id.clone(),
        conversation_id: context.conversation_id.clone(),
        turn_id,
        task_id,
        parent_task_id: None,
        tool_call_id,
        actor: context.actor.clone(),
        sequence,
        occurred_at_ms: unix_now_ms(),
        kind: event,
    };
    if let Ok(data) = serde_json::to_string(&envelope) {
        println!("{data}");
    }
}

pub(super) fn emit_interaction_event(metadata: &InteractionMetadata, event: InteractionEvent) {
    let sequence = EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let envelope = interaction_event_envelope(metadata, event, sequence, unix_now_ms());
    if let Ok(data) = serde_json::to_string(&envelope) {
        println!("{data}");
    }
}

fn interaction_event_envelope(
    metadata: &InteractionMetadata,
    event: InteractionEvent,
    sequence: u64,
    occurred_at_ms: u64,
) -> InteractionEventEnvelope {
    let mut event_metadata = metadata.clone();
    event_metadata.protocol_version = tachyon_api::INTERACTION_PROTOCOL_VERSION;
    event_metadata.message_id = format!("interaction-event-{sequence}");
    event_metadata.causation_id = Some(metadata.message_id.clone());
    event_metadata.occurred_at_ms = occurred_at_ms;
    InteractionEventEnvelope {
        metadata: event_metadata,
        event,
    }
}

pub(super) fn synthetic_interaction_metadata(turn: Option<u64>) -> InteractionMetadata {
    let sequence = LEGACY_INPUT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let message_id = format!("legacy-input-{sequence}");
    let conversation_id = EVENT_CONTEXT
        .get()
        .and_then(|context| context.conversation_id.clone())
        .unwrap_or_else(|| FOREGROUND_ID.into());
    let mut metadata =
        InteractionMetadata::new(&message_id, &message_id, conversation_id, unix_now_ms());
    metadata.turn_id = turn.map(|turn| turn.to_string());
    metadata
}

pub(super) fn init_event_context(_role: AgentRole, agent_id: Option<&str>) {
    let session_id = agent_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("conversation-{}", std::process::id()));
    let actor = Actor::Foreground;
    let conversation_id = Some(session_id.clone());
    let _ = EVENT_CONTEXT.set(EventContext {
        session_id,
        conversation_id,
        actor,
    });
}

fn event_turn(event: &AgentEvent) -> Option<u64> {
    match event {
        AgentEvent::Usage { turn, .. }
        | AgentEvent::Status { turn, .. }
        | AgentEvent::Reply { turn, .. }
        | AgentEvent::ReplyDelta { turn, .. }
        | AgentEvent::ToolStarted { turn, .. }
        | AgentEvent::ToolFinished { turn, .. }
        | AgentEvent::WorkerStarted { turn, .. }
        | AgentEvent::MemorySaved { turn, .. }
        | AgentEvent::MemoryRecalled { turn, .. }
        | AgentEvent::MemoryMutation { turn, .. }
        | AgentEvent::ReminderScheduled { turn, .. }
        | AgentEvent::ReminderCancelled { turn, .. }
        | AgentEvent::ReminderFired { turn, .. }
        | AgentEvent::ScheduledTaskCreated { turn, .. }
        | AgentEvent::Error { turn, .. } => *turn,
        AgentEvent::Timing { turn, .. } => Some(*turn),
        AgentEvent::WorkerCompleted { .. }
        | AgentEvent::ContextCompacted { .. }
        | AgentEvent::ToolTelemetry { .. }
        | AgentEvent::ArtifactRegistered { .. }
        | AgentEvent::WorkCandidate { .. }
        | AgentEvent::WorkProgress { .. }
        | AgentEvent::WorkResult { .. }
        | AgentEvent::WorkerReleaseRequested { .. } => None,
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::usable_acknowledgement;
    #[test]
    fn queued_turn_acknowledgement_is_a_turn_correlated_status() {
        let event = status_event(Some(7), &format!("queued {QUEUED_TURN_ACKNOWLEDGEMENT}"));
        assert!(matches!(
            event,
            AgentEvent::Status {
                turn: Some(7),
                phase,
                message,
            } if phase == "queued" && message == QUEUED_TURN_ACKNOWLEDGEMENT
        ));
        assert!(!QUEUED_TURN_ACKNOWLEDGEMENT.contains("turn"));
        assert!(!QUEUED_TURN_ACKNOWLEDGEMENT.contains("queue"));
        assert!(usable_acknowledgement(CONCURRENT_TURN_ACKNOWLEDGEMENT).is_some());
    }

    #[test]
    fn finished_event_wire_bytes_are_stable() {
        let wire = r#"{"protocol_version":1,"message_id":"interaction-event-7","correlation_id":"request-1","causation_id":"command-1","conversation_id":"conversation-1","turn_id":"2","generation":0,"occurred_at_ms":123,"cwd":"/host/workspace","event":"conversation_finished","text":"first\nsecond"}"#;
        let mut metadata =
            InteractionMetadata::new("command-1", "request-1", "conversation-1", 100);
        metadata.turn_id = Some("2".into());
        metadata.cwd = Some("/host/workspace".into());
        let envelope = interaction_event_envelope(
            &metadata,
            InteractionEvent::ConversationFinished {
                text: "first\nsecond".into(),
            },
            7,
            123,
        );
        assert_eq!(serde_json::to_string(&envelope).unwrap(), wire);
        assert!(
            matches!(envelope.event, InteractionEvent::ConversationFinished { text } if text == "first\nsecond")
        );
    }
}
