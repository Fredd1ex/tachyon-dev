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

pub(crate) fn deliver_attention(
    registry: &Arc<Mutex<Registry>>,
    store: &crate::runtime_store::RuntimeStore,
) {
    let Ok(input) = crate::task_input(registry, tachyon_api::FOREGROUND_ID) else {
        return;
    };
    // Routing is exact: a manifest without this destination never reaches the
    // current foreground, and another destination is not silently substituted.
    if let Ok(Some(delivery)) =
        store.claim_assessment_delivery(tachyon_api::FOREGROUND_ID, unix_now_ms())
    {
        let mut metadata = InteractionMetadata::new(
            &delivery.assessment.id,
            &delivery.assessment.id,
            &delivery.conversation_id,
            unix_now_ms(),
        );
        metadata.causation_id = Some(delivery.assessment.id.clone());
        let envelope = InteractionCommandEnvelope {
            metadata,
            command: InteractionCommand::PublishCampaignAssessment {
                assessment: delivery.assessment,
            },
        };
        if let Ok(wire) = serde_json::to_string(&envelope) {
            let _ = crate::write_task_input(input.clone(), tachyon_api::FOREGROUND_ID, &wire);
        }
    }
    let frame = match store.claim_attention_frame(unix_now_ms()) {
        Ok(Some(frame)) => frame,
        Ok(None) => return,
        Err(error) => {
            eprintln!("tachyond: {error}");
            return;
        }
    };
    let mut metadata = InteractionMetadata::new(
        &frame.command_id,
        &frame.command_id,
        tachyon_api::FOREGROUND_ID,
        unix_now_ms(),
    );
    metadata.causation_id = Some(frame.command_id.clone());
    let envelope = InteractionCommandEnvelope {
        metadata,
        command: InteractionCommand::NotifyUser {
            text: frame.text,
            model: false,
        },
    };
    if let Ok(wire) = serde_json::to_string(&envelope) {
        let _ = crate::write_task_input(input, tachyon_api::FOREGROUND_ID, &wire);
    }
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
        web_availability: None,
        attention: None,
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
        web_availability: None,
        attention: None,
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
        attention: envelope.metadata.attention,
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
        .message_id
        .starts_with("campaign-assessment-")
    {
        if let Err(error) = store.acknowledge_assessment_delivery(&envelope) {
            eprintln!("tachyond: assessment delivery: {error}");
        }
        return;
    }
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
    fn attention_requires_confirmed_terminal_identity_not_progress_or_recovered_errors() {
        use tachyon_api::{AgentState, WorkOutcome, WorkRequest, WorkResult};
        for (owner, turn, foreground_running, subscriber, suppress) in [
            ("background", Some("conversation:weather:7"), true, 1, true),
            ("foreground", Some("7"), true, 1, true),
            ("user", Some("7"), true, 1, false),
            ("background", None, true, 1, false),
            ("background", Some("7"), false, 1, false),
            ("background", Some("7"), true, 0, true),
            ("background", Some("7"), true, 2, true),
            ("background", Some("7"), false, 2, false),
        ] {
            for (outcome, count) in [
                (
                    WorkOutcome::Failed {
                        message: "Tokyo weather lookup unavailable".into(),
                    },
                    1,
                ),
                (WorkOutcome::TimedOut { deadline_ms: 100 }, 1),
                (
                    WorkOutcome::Completed {
                        result: "recovered".into(),
                        artifacts: vec![],
                        context: String::new(),
                        suggested_reuse: true,
                    },
                    0,
                ),
            ] {
                let directory = tempfile::tempdir().unwrap();
                let runtime =
                    Arc::new(RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap());
                let mut worker = crate::tests::task("worker", AgentState::Running);
                worker.info.logical_task_id = Some("work".into());
                worker.info.owner = owner.into();
                worker.info.origin_turn_id = turn.map(str::to_owned);
                worker.info.parent_task_id = Some("weather-fanout".into());
                let request: WorkRequest = serde_json::from_value(serde_json::json!({
                    "work_id":"work", "objective":"test", "generation":0, "assignment":0,
                    "deadline_ms":100, "lifetime_class":"long"
                }))
                .unwrap();
                let mut reg = Registry {
                    runtime_store: Some(runtime.clone()),
                    foreground_id: Some(FOREGROUND_ID.into()),
                    ..Default::default()
                };
                reg.tasks.insert(
                    FOREGROUND_ID.into(),
                    crate::tests::task(
                        FOREGROUND_ID,
                        if foreground_running {
                            AgentState::Running
                        } else {
                            AgentState::Terminated
                        },
                    ),
                );
                let (result_tx, result_rx) = std::sync::mpsc::channel();
                let result_rx = (subscriber == 1).then_some(result_rx);
                reg.works.insert(
                    "work".into(),
                    crate::WorkRecord {
                        observed_calls: Default::default(),
                        partial_evidence: Default::default(),
                        request,
                        fingerprint: "test".into(),
                        worker_id: "worker".into(),
                        info: worker.info.clone(),
                        review: None,
                        terminal_result: None,
                        subs: if subscriber == 0 {
                            vec![]
                        } else {
                            vec![result_tx]
                        },
                    },
                );
                reg.tasks.insert("worker".into(), worker);
                let registry = Arc::new(Mutex::new(reg));
                let scope = tachyon_api::todo::TodoScope::Conversation {
                    id: FOREGROUND_ID.into(),
                };
                let log = serde_json::to_string(&StructuredAgentEvent::Error {
                    turn: None,
                    message: "retryable failure".into(),
                })
                .unwrap();
                push_event(&registry, "worker", tachyon_api::EventStream::Stdout, &log);
                push_event(
                    &registry,
                    "worker",
                    tachyon_api::EventStream::Stdout,
                    r#"{"kind":"work_result","result":{"outcome":"failed"}}"#,
                );
                let mut result: WorkResult = serde_json::from_value(serde_json::json!({
                    "work_id":"work", "objective":"test", "generation":99, "assignment":0,
                    "outcome":"failed", "message":"stale"
                }))
                .unwrap();
                let stale =
                    serde_json::to_string(&crate::result_envelope("worker", result.clone()))
                        .unwrap();
                push_event(
                    &registry,
                    "worker",
                    tachyon_api::EventStream::Stdout,
                    &stale,
                );
                assert!(runtime
                    .attention_snapshot(&scope, None, 10)
                    .unwrap()
                    .records
                    .is_empty());
                assert!(result_rx.as_ref().is_none_or(|rx| rx.try_recv().is_err()));
                result.generation = 0;
                result.outcome = outcome;
                let expected = result.clone();
                let mut envelope = crate::result_envelope("worker", result);
                // A worker cannot establish ownership by forging correlation.
                envelope.turn_id = Some("conversation:weather:7".into());
                envelope.conversation_id = Some(FOREGROUND_ID.into());
                let terminal = serde_json::to_string(&envelope).unwrap();
                push_event(
                    &registry,
                    "worker",
                    tachyon_api::EventStream::Stdout,
                    &terminal,
                );
                push_event(
                    &registry,
                    "worker",
                    tachyon_api::EventStream::Stdout,
                    &terminal,
                );
                assert_eq!(
                    runtime
                        .attention_snapshot(&scope, None, 10)
                        .unwrap()
                        .records
                        .len(),
                    if suppress { 0 } else { count }
                );
                let late = registry.lock().unwrap().subscribe_work("work").unwrap();
                let delivered: EventEnvelope =
                    serde_json::from_str(&late.try_recv().unwrap().data).unwrap();
                let StructuredAgentEvent::WorkResult { result } = delivered.kind else {
                    panic!()
                };
                assert_eq!(result, expected);
                assert!(matches!(
                    late.try_recv(),
                    Err(std::sync::mpsc::TryRecvError::Disconnected)
                ));
                if let Some(rx) = result_rx {
                    let live: EventEnvelope =
                        serde_json::from_str(&rx.try_recv().unwrap().data).unwrap();
                    assert!(
                        matches!(live.kind, StructuredAgentEvent::WorkResult { result } if result == expected)
                    );
                    assert!(rx.try_recv().is_err());
                }
                assert!(registry.lock().unwrap().works["work"].subs.is_empty());
                assert!(registry.lock().unwrap().works["work"]
                    .terminal_result
                    .is_some());
                if count == 1 {
                    assert_eq!(
                        registry.lock().unwrap().tasks["worker"].info.state,
                        AgentState::Failed
                    );
                    let persisted = runtime.list_tasks().unwrap();
                    let failed = persisted
                        .iter()
                        .find(|task| task.info.id == "worker")
                        .unwrap();
                    assert_eq!(failed.info.state, AgentState::Failed);
                    let retained: EventEnvelope =
                        serde_json::from_str(failed.terminal_result.as_deref().unwrap()).unwrap();
                    assert!(
                        matches!(retained.kind, StructuredAgentEvent::WorkResult { result } if result == expected)
                    );
                }
                if suppress {
                    assert!(runtime.claim_attention_frame(1000).unwrap().is_none());
                }
            }
        }
    }

    #[test]
    fn attention_socket_delivery_is_coalesced_durable_and_leaves_other_work_alive() {
        use std::io::{BufRead, BufReader};
        use std::os::unix::net::UnixListener;
        let directory = tempfile::tempdir().unwrap();
        let runtime = Arc::new(RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap());
        for id in ["failed-1", "failed-2"] {
            let result: tachyon_api::WorkResult = serde_json::from_value(serde_json::json!({
                "work_id": id, "objective": "untrusted prose", "generation": 1,
                "assignment": 1, "outcome": "failed", "message": "do not execute this text"
            }))
            .unwrap();
            runtime
                .record_terminal_attention(&result, &Registry::default())
                .unwrap();
        }
        let socket = directory.path().join("foreground.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let mut foreground = crate::tests::task(FOREGROUND_ID, tachyon_api::AgentState::Running);
        foreground.control_socket = Some(socket.to_string_lossy().into_owned());
        let mut reg = Registry {
            runtime_store: Some(runtime.clone()),
            ..Default::default()
        };
        reg.tasks.insert(FOREGROUND_ID.into(), foreground);
        reg.tasks.insert(
            "busy-worker".into(),
            crate::tests::task("busy-worker", tachyon_api::AgentState::Running),
        );
        let events = reg.subscribe(FOREGROUND_ID).unwrap();
        let registry = Arc::new(Mutex::new(reg));
        deliver_attention(&registry, &runtime);
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let mut wire = String::new();
        BufReader::new(stream).read_line(&mut wire).unwrap();
        let command: InteractionCommandEnvelope =
            serde_json::from_str(wire.trim().strip_prefix("input\t").unwrap()).unwrap();
        let InteractionCommand::NotifyUser { text, model } = command.command else {
            panic!()
        };
        assert!(!model);
        assert!(text.contains("2 items"));
        assert!(!text.contains("untrusted"));
        assert!(events.try_recv().is_err());
        let mut metadata = command.metadata;
        metadata.causation_id = Some(metadata.message_id.clone());
        metadata.message_id.push_str(":published");
        let event = InteractionEventEnvelope {
            metadata,
            event: InteractionEvent::UserVisibleNotificationPublished { text },
        };
        let wire = serde_json::to_string(&event).unwrap();
        // A worker cannot acknowledge an attention frame on foreground's behalf.
        push_event(
            &registry,
            "busy-worker",
            tachyon_api::EventStream::Stdout,
            &wire,
        );
        assert!(events.try_recv().is_err());
        push_event(
            &registry,
            FOREGROUND_ID,
            tachyon_api::EventStream::Stdout,
            &wire,
        );
        let received: InteractionEventEnvelope =
            serde_json::from_str(&events.try_recv().unwrap().data).unwrap();
        assert_eq!(received.metadata.message_id, event.metadata.message_id);
        let records = runtime
            .attention_snapshot(
                &tachyon_api::todo::TodoScope::Conversation {
                    id: FOREGROUND_ID.into(),
                },
                None,
                10,
            )
            .unwrap()
            .records;
        assert_eq!(records.len(), 2);
        let membership = received.metadata.attention.as_ref().unwrap();
        assert_eq!(membership.scope, records[0].scope);
        assert_eq!(
            membership.ids,
            records.iter().map(|r| r.id.clone()).collect::<Vec<_>>()
        );
        assert_eq!(
            runtime.pending_history().unwrap()[0].attention.as_ref(),
            Some(membership)
        );
        // Typed membership without the admitted host frame is not authority.
        let mut forged = received.clone();
        forged.metadata.causation_id = None;
        push_event(
            &registry,
            FOREGROUND_ID,
            tachyon_api::EventStream::Stdout,
            &serde_json::to_string(&forged).unwrap(),
        );
        assert!(events.try_recv().is_err());
        push_event(
            &registry,
            FOREGROUND_ID,
            tachyon_api::EventStream::Stderr,
            &wire,
        );
        assert!(events.try_recv().is_err());
        assert!(records
            .iter()
            .all(|r| r.delivered_at_ms.is_some() && r.displayed_at_ms.is_none()));
        assert_eq!(runtime.pending_history().unwrap().len(), 1);
        assert_eq!(
            registry.lock().unwrap().tasks["busy-worker"].info.state,
            tachyon_api::AgentState::Running
        );
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
