//! Foreground event correlation and stdout publication.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use tachyon_api::types::{Actor, AgentEvent, EventEnvelope};
use tachyon_api::{InteractionEvent, InteractionEventEnvelope, InteractionMetadata, FOREGROUND_ID};

use crate::runtime::AgentRole;

tokio::task_local! {
    static TURN_FEEDBACK: TurnFeedback;
}

#[cfg(test)]
tokio::task_local! {
    pub(super) static CAPTURED_EVENTS: std::cell::RefCell<Vec<AgentEvent>>;
}

struct TurnFeedback {
    turn: u64,
    accepted_at: std::time::Instant,
    acknowledged: std::cell::Cell<bool>,
    contextual: std::cell::Cell<bool>,
    answering: std::cell::Cell<bool>,
}

/// The timer lives inside the admitted turn task, not in a detached task. Dropping
/// routing/execution also drops the timer, including on channel closure or abort.
pub(super) async fn with_turn_feedback<T>(
    turn: u64,
    accepted_at: std::time::Instant,
    delay: std::time::Duration,
    future: impl std::future::Future<Output = T>,
) -> T {
    TURN_FEEDBACK
        .scope(
            TurnFeedback {
                turn,
                accepted_at,
                acknowledged: std::cell::Cell::new(false),
                contextual: std::cell::Cell::new(false),
                answering: std::cell::Cell::new(false),
            },
            async move {
                tokio::pin!(future);
                let timer = tokio::time::sleep(delay.saturating_sub(accepted_at.elapsed()));
                tokio::pin!(timer);
                tokio::select! {
                    biased;
                    result = &mut future => result,
                    _ = &mut timer => {
                        emit_acknowledgement(Some(turn), "Working on that.");
                        future.await
                    }
                }
            },
        )
        .await
}

pub(super) fn acknowledgement_delay() -> std::time::Duration {
    std::time::Duration::from_millis(
        std::env::var("TACHYON_ACK_DELAY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(500)
            .min(60_000),
    )
}

pub(super) fn observe_answer(event: &InteractionEvent) {
    let _ = TURN_FEEDBACK.try_with(|state| {
        let stage = match event {
            InteractionEvent::ConversationDelta { text } if !text.trim().is_empty() => {
                if state.answering.replace(true) {
                    return;
                }
                "first_answer"
            }
            InteractionEvent::ConversationFinished { .. } => {
                state.answering.set(true);
                "completed"
            }
            _ => return,
        };
        emit_event(AgentEvent::Timing {
            turn: state.turn,
            stage: stage.into(),
            elapsed_ms: state.accepted_at.elapsed().as_millis() as u64,
        });
    });
}

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
        if TURN_FEEDBACK
            .try_with(|state| {
                turn == Some(state.turn) && (state.answering.get() || state.contextual.get())
            })
            .unwrap_or(false)
        {
            return;
        }
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
                identity: None,
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

pub(super) fn emit_queued_turn(turn: u64) {
    emit_turn(Some(turn), "[status] queued".into());
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
            identity: None,
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
    let allowed = TURN_FEEDBACK
        .try_with(|state| {
            if state.answering.get() || state.acknowledged.replace(true) {
                return false;
            }
            true
        })
        .unwrap_or(true);
    if !allowed {
        return;
    }
    emit_turn(turn, format!("[status] working {text}"));
    let _ = TURN_FEEDBACK.try_with(|state| {
        emit_event(AgentEvent::Timing {
            turn: state.turn,
            stage: "acknowledgement_published".into(),
            elapsed_ms: state.accepted_at.elapsed().as_millis() as u64,
        });
    });
}

/// Only host-confirmed pending dependencies may replace the generic receipt.
pub(super) fn emit_dependency_acknowledgement(turn: u64) {
    let allowed = TURN_FEEDBACK
        .try_with(|state| {
            if state.turn != turn || state.answering.get() || state.contextual.replace(true) {
                return false;
            }
            state.acknowledged.set(true);
            true
        })
        .unwrap_or(true);
    if !allowed {
        return;
    }
    emit_event(AgentEvent::Status {
        turn: Some(turn),
        phase: "working".into(),
        message: "I'm waiting for those results. I'll answer when I have enough information."
            .into(),
    });
    let _ = TURN_FEEDBACK.try_with(|state| {
        emit_event(AgentEvent::Timing {
            turn,
            stage: "contextual_acknowledgement_published".into(),
            elapsed_ms: state.accepted_at.elapsed().as_millis() as u64,
        });
    });
}

pub(super) fn emit_event(event: AgentEvent) {
    #[cfg(test)]
    let _ = CAPTURED_EVENTS.try_with(|events| events.borrow_mut().push(event.clone()));
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
    event_metadata.message_id = if (metadata.message_id.starts_with("attention-command-")
        || metadata.message_id.starts_with("campaign-assessment-"))
        && matches!(
            &event,
            InteractionEvent::UserVisibleNotificationPublished { .. }
        ) {
        format!("{}:published", metadata.message_id)
    } else {
        format!("{}:interaction-event-{sequence}", metadata.message_id)
    };
    event_metadata.causation_id = Some(metadata.message_id.clone());
    event_metadata.occurred_at_ms = occurred_at_ms;
    InteractionEventEnvelope {
        metadata: event_metadata,
        event,
    }
}

pub(super) fn synthetic_interaction_metadata(turn: Option<u64>) -> InteractionMetadata {
    let sequence = LEGACY_INPUT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let message_id = format!(
        "legacy-input-{}-{}-{sequence}",
        std::process::id(),
        unix_now_ms()
    );
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
    let session_id = std::env::var("TACHYON_FOREGROUND_SESSION_ID")
        .ok()
        .filter(|id| !id.is_empty())
        .or_else(|| agent_id.map(str::to_string))
        .unwrap_or_else(|| format!("conversation-{}", std::process::id()));
    let actor = Actor::Foreground;
    let conversation_id = Some(FOREGROUND_ID.into());
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

    #[test]
    fn accepted_turn_echoes_typed_submit_origin_and_original_host_command() {
        use tachyon_api::interaction_manager::CommandOrigin;
        for (command, turn) in [("client-a", "1"), ("client-b", "2")] {
            let mut metadata =
                InteractionMetadata::new(format!("host-{command}"), command, FOREGROUND_ID, 1);
            metadata.command_origin = Some(CommandOrigin {
                session_id: "daemon-host-session".into(),
                command_id: command.into(),
                host_message_id: metadata.message_id.clone(),
            });
            let input = tachyon_api::InteractionCommandEnvelope {
                metadata: metadata.clone(),
                command: tachyon_api::InteractionCommand::AcceptUserTurn {
                    text: "identical text".into(),
                },
            };
            let crate::input::ChatInput::User { text, mut metadata } =
                crate::input::decode_chat_input(
                    &serde_json::to_string(&input).unwrap(),
                    AgentRole::Conversation,
                )
            else {
                panic!("not a user turn")
            };
            metadata.turn_id = Some(turn.into());
            let event = interaction_event_envelope(
                &metadata,
                InteractionEvent::UserTurnAccepted { text },
                42,
                2,
            );
            assert_eq!(event.metadata.command_origin, input.metadata.command_origin);
            assert_eq!(
                event.metadata.causation_id.as_ref(),
                Some(&input.metadata.message_id)
            );
            assert_eq!(event.metadata.correlation_id, command);
            assert_eq!(event.metadata.turn_id.as_deref(), Some(turn));
        }
    }

    #[tokio::test]
    async fn answer_without_receipt_also_suppresses_late_contextual_status() {
        for event in [
            InteractionEvent::ConversationDelta {
                text: "answer".into(),
            },
            InteractionEvent::ConversationFinished {
                text: "answer".into(),
            },
        ] {
            CAPTURED_EVENTS
                .scope(Default::default(), async {
                    with_turn_feedback(
                        7,
                        std::time::Instant::now(),
                        std::time::Duration::from_millis(500),
                        async {
                            observe_answer(&event);
                            emit_dependency_acknowledgement(7);
                            emit_turn(Some(7), "[status] working stale progress".into());
                        },
                    )
                    .await;
                    CAPTURED_EVENTS.with(|events| {
                        assert!(!events
                            .borrow()
                            .iter()
                            .any(|event| matches!(event, AgentEvent::Status { .. })));
                    });
                })
                .await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn contextual_receipt_replaces_fallback_and_cannot_override_answer() {
        for delayed in [false, true] {
            CAPTURED_EVENTS.scope(Default::default(), async {
                with_turn_feedback(7, std::time::Instant::now(), std::time::Duration::from_millis(500), async {
                    if delayed {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    emit_dependency_acknowledgement(7);
                    emit_dependency_acknowledgement(7);
                    emit_acknowledgement(Some(7), "generic regression");
                    emit_turn(Some(7), "[status] working".into());
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    observe_answer(&InteractionEvent::ConversationDelta { text: "Bring a coat.".into() });
                    emit_dependency_acknowledgement(7);
                    emit_turn(Some(7), "[status] working late progress".into());
                    observe_answer(&InteractionEvent::ConversationFinished { text: "Bring a coat.".into() });
                    emit_dependency_acknowledgement(7);
                    emit_turn(Some(7), "[status] queued".into());
                }).await;
                CAPTURED_EVENTS.with(|events| {
                    let events = events.borrow();
                    let statuses = events.iter().filter(|event| matches!(event, AgentEvent::Status { .. })).collect::<Vec<_>>();
                    assert_eq!(statuses.len(), 1 + usize::from(delayed));
                    let wire = serde_json::to_value(statuses.last().unwrap()).unwrap();
                    assert_eq!(wire["turn"], 7);
                    assert_eq!(wire["phase"], "working");
                    assert_eq!(wire["message"], "I'm waiting for those results. I'll answer when I have enough information.");
                    assert!(!events.iter().any(|event| matches!(event, AgentEvent::Reply { .. } | AgentEvent::ReplyDelta { .. })));
                });
            }).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn receipt_is_once_and_fast_or_streaming_answers_suppress_it() {
        use std::time::{Duration, Instant};
        for mode in ["fast", "streaming", "pending", "tool"] {
            CAPTURED_EVENTS.scope(Default::default(), async {
                with_turn_feedback(7, Instant::now(), Duration::from_millis(500), async {
                    if mode == "fast" { return; }
                    if mode == "streaming" {
                        observe_answer(&InteractionEvent::ConversationDelta { text: "Hello".into() });
                    }
                    if mode == "tool" {
                        emit_acknowledgement(Some(7), "I'm looking into it.");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let count = CAPTURED_EVENTS.with(|events| events.borrow().iter().filter(|event| {
                        matches!(event, AgentEvent::Status { message, .. } if !message.is_empty())
                    }).count());
                    assert_eq!(count, usize::from(mode != "streaming"));
                    observe_answer(&InteractionEvent::ConversationFinished { text: "Done".into() });
                    emit_acknowledgement(Some(7), "must not appear after completion");
                }).await;
                CAPTURED_EVENTS.with(|events| {
                    let events = events.borrow();
                    let acks = events.iter().filter(|event| matches!(event,
                        AgentEvent::Timing { turn: 7, stage, .. } if stage == "acknowledgement_published"
                    )).count();
                    assert_eq!(acks, usize::from(matches!(mode, "pending" | "tool")));
                    assert!(!events.iter().any(|event| matches!(event, AgentEvent::Reply { .. } | AgentEvent::ReplyDelta { .. })));
                });
            }).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn parallel_receipts_and_answers_stay_in_their_own_turns() {
        let jobs = (1..=32).map(|turn| {
            tokio::spawn(async move {
                CAPTURED_EVENTS.scope(Default::default(), async {
                    let slow = turn % 2 == 0;
                    with_turn_feedback(
                        turn,
                        std::time::Instant::now(),
                        std::time::Duration::from_millis(500),
                        async {
                            tokio::time::sleep(std::time::Duration::from_millis(if slow { 1000 } else { 100 })).await;
                            observe_answer(&InteractionEvent::ConversationDelta { text: "answer".into() });
                            observe_answer(&InteractionEvent::ConversationFinished { text: "answer".into() });
                        },
                    ).await;
                    CAPTURED_EVENTS.with(|events| {
                        let events = events.borrow();
                        assert!(events.iter().all(|event| event_turn(event) == Some(turn)));
                        assert_eq!(events.iter().filter(|event| matches!(event, AgentEvent::Status { .. })).count(), usize::from(slow));
                        assert_eq!(events.iter().filter(|event| matches!(event, AgentEvent::Timing { stage, .. } if stage == "first_answer")).count(), 1);
                    });
                }).await;
            })
        }).collect::<Vec<_>>();
        for job in jobs {
            job.await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_pending_turn_drops_receipt_timer() {
        CAPTURED_EVENTS
            .scope(Default::default(), async {
                let future = with_turn_feedback(
                    8,
                    std::time::Instant::now(),
                    std::time::Duration::from_millis(500),
                    std::future::pending::<()>(),
                );
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(100), future)
                        .await
                        .is_err()
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                CAPTURED_EVENTS.with(|events| assert!(events.borrow().is_empty()));
            })
            .await;
    }

    #[tokio::test]
    async fn fast_http_answer_has_distinct_timings_without_receipt_or_history_changes() {
        use crate::{
            execution::process_turn, model::test_provider::LocalProvider, turns::ConversationState,
        };
        use std::{
            collections::BTreeMap,
            sync::{Arc, Mutex},
            time::{Duration, Instant},
        };
        use tachyon_model::{ChatMessage, Role, TokenUsage};
        use tachyon_orchestrator::conversation::policy::InteractionDecision;
        let mut provider = LocalProvider::start().await;
        let conversation = Arc::new(Mutex::new(ConversationState {
            messages: vec![ChatMessage::new(Role::System, "system")],
            assessments: vec![],
            evidence: vec![],
            pending: BTreeMap::new(),
            next_commit: 1,
            context_epoch: 0,
        }));
        let active = Arc::new(Mutex::new(BTreeMap::from([(1, "hello".into())])));
        let (checkpoints, _snapshots) = std::sync::mpsc::channel();
        let mut metadata = synthetic_interaction_metadata(Some(1));
        metadata.turn_id = Some("1".into());
        let accepted = Instant::now();
        let args = (
            1,
            "hello".into(),
            metadata,
            false,
            InteractionDecision::AnswerNow,
            None,
            TokenUsage::default(),
            accepted,
            Some(provider.model.clone()),
            None,
            conversation.clone(),
            active.clone(),
            Arc::new(tokio::sync::Notify::new()),
            checkpoints,
            AgentRole::Conversation,
            None,
        );
        let client = tokio::spawn(async move {
            CAPTURED_EVENTS
                .scope(Default::default(), async {
                    let published = Mutex::new(Vec::new());
                    with_turn_feedback(
                        1,
                        accepted,
                        Duration::from_millis(500),
                        process_turn(args, |metadata, event| {
                            assert_eq!(metadata.turn_id.as_deref(), Some("1"));
                            if matches!(event, InteractionEvent::ConversationFinished { .. }) {
                                CAPTURED_EVENTS.with(|events| {
                                    assert!(!events.borrow().iter().any(|event| matches!(event,
                                        AgentEvent::Timing { stage, .. } if stage == "completed")));
                                });
                            }
                            published.lock().unwrap().push(event);
                        }),
                    )
                    .await;
                    let events = CAPTURED_EVENTS.with(|events| events.borrow().clone());
                    (events, published.into_inner().unwrap())
                })
                .await
        });
        let (events, published) = tokio::time::timeout(Duration::from_secs(5), async {
            provider
                .requests
                .recv()
                .await
                .unwrap()
                .respond(&["Hello", " there."])
                .await;
            client.await.unwrap()
        })
        .await
        .unwrap();
        let timings = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::Timing {
                    turn: 1,
                    stage,
                    elapsed_ms,
                } => Some((stage.as_str(), *elapsed_ms)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let stage_time = |name| timings.iter().find(|(stage, _)| *stage == name).unwrap().1;
        assert!(stage_time("provider_first_output") <= stage_time("first_answer"));
        assert!(stage_time("first_answer") <= stage_time("completed"));
        assert_eq!(
            timings
                .iter()
                .filter(|(stage, _)| *stage == "completed")
                .count(),
            1
        );
        assert_eq!(
            published
                .iter()
                .filter(|event| matches!(event, InteractionEvent::ConversationFinished { .. }))
                .count(),
            1
        );
        assert!(!timings
            .iter()
            .any(|(stage, _)| *stage == "acknowledgement_published"));
        assert!(
            matches!(published.last(), Some(InteractionEvent::ConversationFinished { text }) if text == "Hello there.")
        );
        let history = conversation.lock().unwrap();
        assert_eq!(history.messages.len(), 3);
        assert_eq!(history.messages[1].plain(), "hello");
        assert_eq!(history.messages[2].plain(), "Hello there.");
        assert!(active.lock().unwrap().is_empty());
        drop(history);
        provider.shutdown().await;
    }

    #[tokio::test]
    async fn stalled_tool_arguments_report_output_and_receipt_without_planning() {
        use crate::model::{chat_with_delegation, test_provider::LocalProvider};
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use tachyon_model::{ChatMessage, Role, ToolSpec};
        let mut provider = LocalProvider::start().await;
        let model = provider.model.clone();
        let output_seen = Arc::new(tokio::sync::Notify::new());
        let notified = output_seen.clone();
        let (release, wait) = tokio::sync::oneshot::channel();
        let client = tokio::spawn(async move {
            CAPTURED_EVENTS.scope(Default::default(), async move {
                let accepted = Instant::now();
                let mut visible = String::new();
                let mut relay = |delta: &str| visible.push_str(delta);
                let messages = [ChatMessage::new(Role::User, "inspect")];
                let tools = [ToolSpec::new("spawn_agent", "inspect", serde_json::json!({"type":"object"}))];
                let completion = with_turn_feedback(42, accepted, Duration::from_millis(20), async {
                    let call = tachyon_model::with_output_observer(
                        chat_with_delegation(&model, &messages, &tools, true, &mut relay),
                        move || {
                            emit_event(AgentEvent::Timing { turn: 42, stage: "provider_first_output".into(), elapsed_ms: accepted.elapsed().as_millis() as u64 });
                            notified.notify_one();
                        },
                    );
                    tokio::pin!(call);
                    tokio::select! {
                        _ = &mut call => panic!("tool stream must remain pending"),
                        _ = tokio::time::sleep(Duration::from_millis(60)) => {}
                    }
                    CAPTURED_EVENTS.with(|events| {
                        let events = events.borrow();
                        assert!(events.iter().any(|event| matches!(event, AgentEvent::Status { turn: Some(42), message, .. } if message == "Working on that.")));
                        assert!(events.iter().all(|event| !format!("{event:?}").contains("PRIVATE")));
                    });
                    release.send(()).unwrap();
                    call.await.unwrap()
                }).await;
                assert!(visible.is_empty());
                assert_eq!(completion.tool_calls.len(), 1);
                CAPTURED_EVENTS.with(|events| {
                    let events = events.borrow();
                    let timings = events.iter().filter_map(|event| match event {
                        AgentEvent::Timing { turn: 42, stage, elapsed_ms } => Some((stage.as_str(), *elapsed_ms)),
                        _ => None,
                    }).collect::<Vec<_>>();
                    assert_eq!(timings.iter().filter(|(stage, _)| *stage == "provider_first_output").count(), 1);
                    assert_eq!(timings.iter().filter(|(stage, _)| *stage == "acknowledgement_published").count(), 1);
                    assert!(!timings.iter().any(|(stage, _)| *stage == "first_answer"));
                });
            }).await;
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut request = provider.requests.recv().await.unwrap();
            request.start_stream().await;
            // Role/empty/reasoning frames are not semantic output.
            request.send_delta(serde_json::json!({"role":"assistant", "content":"", "reasoning_content":"PRIVATE reasoning"})).await;
            request.send_delta(serde_json::json!({"tool_calls":[{"index":0,"id":"call-1","function":{"name":"spawn_agent","arguments":"{\"task\":"}}]})).await;
            output_seen.notified().await;
            wait.await.unwrap();
            request.send_delta(serde_json::json!({"content":"PRIVATE planning", "tool_calls":[{"index":0,"function":{"arguments":"\"inspect\"}"}}]})).await;
            request.finish_stream().await;
            client.await.unwrap();
        }).await.expect("local stalled stream deadlocked");
        provider.shutdown().await;
    }
    #[test]
    fn campaign_assessment_relay_identity_survives_retry_without_turn_injection() {
        let mut metadata = InteractionMetadata::new(
            "campaign-assessment-c-1",
            "campaign-assessment-c-1",
            FOREGROUND_ID,
            1,
        );
        metadata.causation_id = Some(metadata.message_id.clone());
        let publish = |sequence| {
            interaction_event_envelope(
                &metadata,
                InteractionEvent::UserVisibleNotificationPublished {
                    text: "advisory".into(),
                },
                sequence,
                3,
            )
        };
        let first = publish(1);
        let retry = publish(10);
        assert_eq!(first, retry);
        assert_eq!(
            first.metadata.message_id,
            "campaign-assessment-c-1:published"
        );
        assert_eq!(first.metadata.turn_id, None);
        assert_eq!(first.metadata.generation, 0);
        assert!(first.metadata.attention.is_none());
    }
    #[tokio::test]
    async fn attention_publication_is_stable_while_provider_task_is_pending() {
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let provider = tokio::spawn(async move { wait.await.unwrap() });
        let metadata = InteractionMetadata::new(
            "attention-command-1",
            "attention-command-1",
            FOREGROUND_ID,
            1,
        );
        let publish = |sequence| {
            interaction_event_envelope(
                &metadata,
                InteractionEvent::UserVisibleNotificationPublished {
                    text: "Work failed (2 items).".into(),
                },
                sequence,
                2,
            )
        };
        let first = publish(7);
        let replay = publish(1);
        assert_eq!(first.metadata.message_id, "attention-command-1:published");
        assert_eq!(first.metadata.message_id, replay.metadata.message_id);
        assert_eq!(
            first.metadata.causation_id.as_deref(),
            Some("attention-command-1")
        );
        assert_eq!(first.metadata.turn_id, None);
        assert!(!provider.is_finished());
        release.send(()).unwrap();
        provider.await.unwrap();
    }
    #[test]
    fn queued_turn_status_does_not_publish_an_immediate_receipt() {
        let event = status_event(Some(7), "queued");
        assert!(matches!(
            event,
            AgentEvent::Status {
                turn: Some(7),
                phase,
                message,
            } if phase == "queued" && message.is_empty()
        ));
    }

    #[test]
    fn finished_event_wire_bytes_are_stable() {
        let wire = r#"{"protocol_version":1,"message_id":"command-1:interaction-event-7","correlation_id":"request-1","causation_id":"command-1","conversation_id":"conversation-1","turn_id":"2","generation":0,"occurred_at_ms":123,"cwd":"/host/workspace","event":"conversation_finished","text":"first\nsecond"}"#;
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

    #[test]
    fn publication_identity_is_scoped_to_the_input_command() {
        let publish = |command| {
            interaction_event_envelope(
                &InteractionMetadata::new(command, "request-1", "conversation-1", 100),
                InteractionEvent::ConversationFinished {
                    text: "result".into(),
                },
                7,
                123,
            )
        };
        let first = publish("command-1");
        let second = publish("command-2");
        assert_eq!(first.metadata.message_id, "command-1:interaction-event-7");
        assert_eq!(second.metadata.message_id, "command-2:interaction-event-7");
        assert_eq!(first.metadata.causation_id.as_deref(), Some("command-1"));
        assert_eq!(second.metadata.causation_id.as_deref(), Some("command-2"));
        assert_eq!(
            first.metadata.correlation_id,
            second.metadata.correlation_id
        );
        assert_eq!(
            first.metadata.conversation_id,
            second.metadata.conversation_id
        );
        assert_eq!(first.metadata.turn_id, None);
        assert_eq!(second.metadata.turn_id, None);
    }
}
