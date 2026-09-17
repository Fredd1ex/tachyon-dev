use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tachyon_api::types::{
    AgentEvent as StructuredAgentEvent, HistoryKind, HistoryRole, ReminderInfo,
};
use tachyon_api::{
    InteractionCommand, InteractionCommandEnvelope, InteractionEvent, InteractionEventEnvelope,
    InteractionMetadata,
};

use crate::runtime_store::HistoryProjection;
use crate::{push_event, unix_now_ms, Registry, DAEMON_EVENT_SEQUENCE};

pub(crate) fn work_attention_notification(
    q: &tachyon_api::work::Attention,
) -> Result<String, String> {
    let text = format!("Work needs input: campaign={:?} work={:?} request={:?}. Use tachyon campaign attention list. Question: {:?}", q.campaign_id, q.work_id, q.request_id, q.question.chars().take(256).collect::<String>());
    super::encode_interaction_command(
        InteractionCommand::NotifyUser { text, model: false },
        None,
        None,
        None,
        None,
    )
}

pub(crate) fn emit_schedule_event(
    registry: &Arc<Mutex<Registry>>,
    conversation_id: String,
    turn: u64,
    kind: StructuredAgentEvent,
) {
    let sequence = DAEMON_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let envelope = tachyon_api::EventEnvelope {
        event_id: sequence,
        session_id: tachyon_api::BACKGROUND_ID.into(),
        conversation_id: Some(conversation_id),
        turn_id: Some(turn.to_string()),
        task_id: None,
        parent_task_id: None,
        tool_call_id: None,
        actor: tachyon_api::Actor::Background,
        sequence,
        occurred_at_ms: unix_now_ms(),
        kind,
    };
    if let Ok(data) = serde_json::to_string(&envelope) {
        push_event(
            registry,
            tachyon_api::FOREGROUND_ID,
            tachyon_api::EventStream::Stdout,
            &data,
        );
    }
}

pub(crate) fn encode_reminder_notification(reminder: &ReminderInfo) -> Result<String, String> {
    let metadata = InteractionMetadata {
        protocol_version: tachyon_api::INTERACTION_PROTOCOL_VERSION,
        message_id: format!("reminder-delivery-{}", reminder.id),
        cwd: None,
        correlation_id: reminder.id.clone(),
        causation_id: Some(reminder.id.clone()),
        conversation_id: reminder.conversation_id.clone(),
        turn_id: None,
        generation: 0,
        occurred_at_ms: unix_now_ms(),
    };
    serde_json::to_string(&InteractionCommandEnvelope {
        metadata,
        command: InteractionCommand::NotifyUser {
            text: reminder.text.clone(),
            model: true,
        },
    })
    .map_err(|error| format!("encode reminder notification: {error}"))
}

pub(crate) fn encode_scheduled_task_notification(
    task: &tachyon_api::ScheduledTaskInfo,
    result: &str,
) -> Result<String, String> {
    let metadata = InteractionMetadata {
        protocol_version: tachyon_api::INTERACTION_PROTOCOL_VERSION,
        message_id: format!("scheduled-task-delivery-{}", task.id),
        cwd: None,
        correlation_id: task.id.clone(),
        causation_id: Some(task.id.clone()),
        conversation_id: task.conversation_id.clone(),
        turn_id: None,
        generation: 0,
        occurred_at_ms: unix_now_ms(),
    };
    let context = format!(
        "A scheduled agent task has finished. Present its result to the user as a concise standalone update.\nObjective: {}\nResult:\n{}",
        task.objective, result
    );
    serde_json::to_string(&InteractionCommandEnvelope {
        metadata,
        command: InteractionCommand::NotifyUser {
            text: context,
            model: true,
        },
    })
    .map_err(|error| format!("encode scheduled task notification: {error}"))
}

fn history_projection(data: &str) -> Option<HistoryProjection> {
    let envelope = serde_json::from_str::<InteractionEventEnvelope>(data).ok()?;
    let (role, text) = match envelope.event {
        InteractionEvent::UserTurnAccepted { text } => (HistoryRole::User, text),
        InteractionEvent::ConversationFinished { text } => (HistoryRole::Assistant, text),
        InteractionEvent::UserVisibleNotificationPublished { text } => {
            (HistoryRole::Notification, text)
        }
        InteractionEvent::ConversationDelta { .. }
        | InteractionEvent::ConversationIntentProduced { .. }
        | InteractionEvent::ForegroundRequestTimedOut { .. } => return None,
    };
    if text.trim().is_empty() {
        return None;
    }
    Some(HistoryProjection {
        schema_version: 1,
        event_id: envelope.metadata.message_id,
        kind: HistoryKind::Conversation,
        conversation_id: envelope.metadata.conversation_id,
        turn_id: envelope.metadata.turn_id,
        occurred_at_ms: envelope.metadata.occurred_at_ms,
        role,
        text,
        task_id: None,
        task_state: None,
    })
}

pub(crate) fn project_pending_history(registry: &Arc<Mutex<Registry>>) -> Result<(), String> {
    let (runtime, history) = {
        let registry = registry.lock().unwrap();
        (
            registry
                .runtime_store
                .clone()
                .ok_or_else(|| "runtime store unavailable".to_string())?,
            registry.history_store.clone(),
        )
    };
    let Some(history) = history else {
        return Ok(());
    };
    for projection in runtime.pending_history()? {
        history.apply(&projection)?;
        runtime.acknowledge_history(&projection.event_id, unix_now_ms())?;
    }
    Ok(())
}

pub(crate) fn persist_interaction_history(registry: &Arc<Mutex<Registry>>, data: &str) {
    let Some(projection) = history_projection(data) else {
        return;
    };
    let runtime = registry.lock().unwrap().runtime_store.clone();
    let Some(runtime) = runtime else {
        eprintln!("tachyond: runtime store missing for history event");
        return;
    };
    if let Err(error) = runtime.enqueue_history(&projection) {
        eprintln!("tachyond: enqueue history {}: {error}", projection.event_id);
        return;
    }
    if let Err(error) = project_pending_history(registry) {
        eprintln!("tachyond: project history: {error}");
    }
}

pub(crate) fn acknowledge_reminder_notification(registry: &Arc<Mutex<Registry>>, data: &str) {
    let Ok(envelope) = serde_json::from_str::<InteractionEventEnvelope>(data) else {
        return;
    };
    if !matches!(
        envelope.event,
        InteractionEvent::UserVisibleNotificationPublished { .. }
    ) {
        return;
    }
    let store = registry.lock().unwrap().runtime_store.clone();
    let Some(store) = store else { return };
    if envelope
        .metadata
        .correlation_id
        .starts_with("scheduled-task-")
    {
        if let Err(error) =
            store.acknowledge_scheduled_task_notification(&envelope.metadata.correlation_id)
        {
            eprintln!(
                "tachyond: acknowledge scheduled task {}: {error}",
                envelope.metadata.correlation_id
            );
        }
        return;
    }
    if !envelope.metadata.correlation_id.starts_with("reminder-") {
        return;
    }
    match store.acknowledge_reminder_delivery(
        &envelope.metadata.correlation_id,
        envelope.metadata.occurred_at_ms,
    ) {
        Ok(reminder) => emit_schedule_event(
            registry,
            reminder.conversation_id.clone(),
            reminder.turn,
            StructuredAgentEvent::ReminderFired {
                turn: Some(reminder.turn),
                reminder_id: reminder.id,
            },
        ),
        Err(error) => eprintln!(
            "tachyond: acknowledge reminder {}: {error}",
            envelope.metadata.correlation_id
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{history_store::HistoryStore, runtime_store::RuntimeStore};
    use tachyon_api::{EventEnvelope, FOREGROUND_ID};

    #[test]
    fn work_attention_is_bounded_and_does_not_request_a_model() {
        let question = tachyon_api::work::Attention {
            campaign_id: "campaign-1".into(),
            work_id: "work-1".into(),
            generation: 2,
            instruction_revision: 3,
            request_id: "request-1".into(),
            question: "x".repeat(300),
            deadline_ms: 100,
            timeout_ms: 50,
            answer: None,
        };
        let wire = work_attention_notification(&question).unwrap();
        let envelope: InteractionCommandEnvelope = serde_json::from_str(&wire).unwrap();
        assert_eq!(
            envelope.metadata.correlation_id,
            envelope.metadata.message_id
        );
        assert_eq!(envelope.metadata.cwd, None);
        assert_eq!(envelope.command, InteractionCommand::NotifyUser {
            text: format!("Work needs input: campaign=\"campaign-1\" work=\"work-1\" request=\"request-1\". Use tachyon campaign attention list. Question: \"{}\"", "x".repeat(256)),
            model: false,
        });
    }

    #[test]
    fn history_projection_keeps_only_canonical_visible_messages() {
        let metadata = InteractionMetadata::new("message-1", "command-1", "conversation-1", 123);
        for (event, role) in [
            (
                InteractionEvent::UserTurnAccepted {
                    text: "hello".into(),
                },
                Some(HistoryRole::User),
            ),
            (
                InteractionEvent::ConversationFinished {
                    text: "answer".into(),
                },
                Some(HistoryRole::Assistant),
            ),
            (
                InteractionEvent::UserVisibleNotificationPublished {
                    text: "notice".into(),
                },
                Some(HistoryRole::Notification),
            ),
            (
                InteractionEvent::ConversationDelta {
                    text: "partial".into(),
                },
                None,
            ),
            (
                InteractionEvent::ConversationIntentProduced { intents: vec![] },
                None,
            ),
            (
                InteractionEvent::ForegroundRequestTimedOut { deadline_ms: 123 },
                None,
            ),
            (
                InteractionEvent::ConversationFinished { text: " \n".into() },
                None,
            ),
        ] {
            let wire = serde_json::to_string(&InteractionEventEnvelope {
                metadata: metadata.clone(),
                event,
            })
            .unwrap();
            let projection = history_projection(&wire);
            assert_eq!(projection.as_ref().map(|p| &p.role), role.as_ref());
            if let Some(projection) = projection {
                assert_eq!(projection.event_id, "message-1");
                assert_eq!(projection.conversation_id, "conversation-1");
                assert_eq!(projection.occurred_at_ms, 123);
                assert_eq!(projection.kind, HistoryKind::Conversation);
            }
        }
        assert!(history_projection("not json").is_none());
    }

    #[test]
    fn history_outbox_projects_and_acknowledges() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = Arc::new(RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap());
        let registry = Arc::new(Mutex::new(Registry {
            runtime_store: Some(runtime.clone()),
            ..Registry::default()
        }));
        let event = InteractionEventEnvelope {
            metadata: InteractionMetadata::new("message-1", "command-1", "conversation-1", 123),
            event: InteractionEvent::ConversationFinished {
                text: "answer".into(),
            },
        };
        let wire = serde_json::to_string(&event).unwrap();
        persist_interaction_history(&registry, &wire);
        assert_eq!(runtime.pending_history().unwrap().len(), 1);
        registry.lock().unwrap().history_store = Some(Arc::new(
            HistoryStore::open(&directory.path().join("history.redb")).unwrap(),
        ));
        project_pending_history(&registry).unwrap();
        assert!(runtime.pending_history().unwrap().is_empty());
        persist_interaction_history(&registry, &wire);
        assert!(runtime.pending_history().unwrap().is_empty());
    }

    #[test]
    fn reminder_notification_carries_stable_delivery_identity() {
        let reminder = ReminderInfo {
            id: "reminder-123-1".into(),
            conversation_id: FOREGROUND_ID.into(),
            turn: 1,
            text: "Your coffee is ready.".into(),
            created_at_ms: 123,
            due_at_ms: 60_123,
            status: tachyon_api::types::ReminderStatus::Delivering,
        };
        let encoded = encode_reminder_notification(&reminder).unwrap();
        let envelope: InteractionCommandEnvelope = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "message_id": "reminder-delivery-reminder-123-1",
                "correlation_id": "reminder-123-1",
                "causation_id": "reminder-123-1",
                "conversation_id": FOREGROUND_ID,
                "turn_id": null,
                "generation": 0,
                "occurred_at_ms": envelope.metadata.occurred_at_ms,
                "command": "notify_user",
                "text": "Your coffee is ready.",
                "model": true
            })
        );
        let retry: InteractionCommandEnvelope =
            serde_json::from_str(&encode_reminder_notification(&reminder).unwrap()).unwrap();
        assert_eq!(retry.metadata.message_id, envelope.metadata.message_id);
    }

    #[test]
    fn reminder_fired_is_published_once_after_delivery_commit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.redb");
        let store = Arc::new(RuntimeStore::open(&path).unwrap());
        store
            .create_reminder(
                "reminder-1",
                "source-1",
                "conversation-1",
                7,
                "ready",
                100,
                200,
            )
            .unwrap();
        let mut reg = Registry {
            runtime_store: Some(store.clone()),
            ..Registry::default()
        };
        reg.tasks.insert(
            FOREGROUND_ID.into(),
            crate::tests::task(FOREGROUND_ID, tachyon_api::AgentState::Running),
        );
        let events = reg.subscribe(FOREGROUND_ID).unwrap();
        let registry = Arc::new(Mutex::new(reg));
        let mut event = InteractionEventEnvelope {
            metadata: InteractionMetadata::new(
                "publication-1",
                "reminder-1",
                "conversation-1",
                201,
            ),
            event: InteractionEvent::UserVisibleNotificationPublished {
                text: "ready".into(),
            },
        };
        // Pending reminders cannot acknowledge a delivery that has not been claimed.
        acknowledge_reminder_notification(&registry, &serde_json::to_string(&event).unwrap());
        assert!(events.try_recv().is_err());
        assert_eq!(store.claim_due_reminders(200, 1).unwrap().len(), 1);
        event.event = InteractionEvent::ConversationFinished {
            text: "ready".into(),
        };
        acknowledge_reminder_notification(&registry, &serde_json::to_string(&event).unwrap());
        assert_eq!(store.active_reminders().unwrap().len(), 1);
        assert!(events.try_recv().is_err());
        event.event = InteractionEvent::UserVisibleNotificationPublished {
            text: "ready".into(),
        };
        let wire = serde_json::to_string(&event).unwrap();
        acknowledge_reminder_notification(&registry, &wire);
        let published: EventEnvelope =
            serde_json::from_str(&events.try_recv().unwrap().data).unwrap();
        assert!(store.active_reminders().unwrap().is_empty());
        assert_eq!(published.event_id, published.sequence);
        assert!(published.event_id >= 1 << 63);
        assert_eq!(published.conversation_id.as_deref(), Some("conversation-1"));
        assert_eq!(published.turn_id.as_deref(), Some("7"));
        assert!(
            matches!(published.kind, StructuredAgentEvent::ReminderFired { turn: Some(7), reminder_id } if reminder_id == "reminder-1")
        );
        acknowledge_reminder_notification(&registry, &wire);
        assert!(events.try_recv().is_err());
        drop(registry);
        drop(store);
        let reopened = RuntimeStore::open(&path).unwrap();
        assert!(reopened.active_reminders().unwrap().is_empty());
        assert!(reopened.claim_due_reminders(10_000, 1).unwrap().is_empty());
    }
}
