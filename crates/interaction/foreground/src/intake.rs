//! Asynchronous input admission, routing, and turn task spawning.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tachyon_api::types::AgentEvent;
use tachyon_api::InteractionEvent;
use tachyon_model::{ChatMessage, Role, TokenUsage};
use tachyon_orchestrator::conversation::policy::InteractionDecision;
use tokio::io::AsyncBufReadExt;

use crate::checkpoints::{
    chat_checkpoint_path, checkpoint_snapshot, load_checkpoint, start_checkpoint_writer,
};
use crate::execution::{modeled_notification, process_turn};
use crate::input::{decode_chat_input, ChatInput};
use crate::model::classify;
use crate::runtime::{from_agent_config, AgentRole};
use crate::streaming::{
    acknowledgement_delay, emit_event, emit_interaction_event, emit_turn,
    synthetic_interaction_metadata, with_turn_feedback,
};
use crate::turns::{
    commit_ready_turns, compact_context_messages, estimated_context_tokens, is_completed_evidence,
    same_evidence, ConversationState,
};

/// Chat mode: read user lines from stdin forever. Stays alive even if the
/// model can't be configured (e.g. missing key) so the user always sees why.
pub(super) async fn run_chat(
    role: AgentRole,
    workspace: &PathBuf,
    new_session: bool,
    agent_id: Option<String>,
) -> ExitCode {
    let loaded =
        tachyon_util::config::Config::try_load_from(&tachyon_util::config::Config::default_path());
    let primary = loaded
        .as_ref()
        .map_err(|error| error.to_string())
        .and_then(|cfg| role.primary(&tachyon_orchestrator::registry::builtin(), cfg));
    let model = primary.as_ref().map_err(Clone::clone).and_then(|_| {
        loaded
            .as_ref()
            .map_err(|error| error.to_string())
            .and_then(|cfg| from_agent_config(cfg, &role.config(cfg)))
    });
    let (model, model_error) = match model {
        Ok(m) => (Some(Arc::new(m)), None),
        Err(e) => {
            println!("[foreground:error] model not ready: {e}");
            (None, Some(e))
        }
    };
    let checkpoint_path = chat_checkpoint_path(workspace, role);
    if new_session {
        let _ = std::fs::remove_file(&checkpoint_path);
    }
    let checkpoint = load_checkpoint(&checkpoint_path);
    let mut initial_messages = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.messages.clone())
        .unwrap_or_default();
    // Checkpoints contain conversation history, but the system prompt must
    // always come from the current binary/configuration rather than a stale
    // prompt persisted by an earlier session.
    if let Ok(primary) = primary {
        if let Some(system) = initial_messages
            .iter_mut()
            .find(|message| message.role == Role::System)
        {
            *system = ChatMessage::new(Role::System, primary.prompt);
        } else {
            initial_messages.insert(0, ChatMessage::new(Role::System, primary.prompt));
        }
    } else {
        initial_messages.retain(|message| message.role != Role::System);
    }
    let conversation = Arc::new(Mutex::new(ConversationState {
        assessments: checkpoint
            .as_ref()
            .map(|c| c.assessments.clone())
            .unwrap_or_default(),
        messages: initial_messages,
        evidence: checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.evidence.clone())
            .unwrap_or_default(),
        pending: BTreeMap::new(),
        next_commit: checkpoint.as_ref().map(|c| c.next_commit).unwrap_or(1),
        context_epoch: checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.context_epoch)
            .unwrap_or_default(),
    }));
    let state_changed = Arc::new(tokio::sync::Notify::new());
    let checkpoint_tx = start_checkpoint_writer(checkpoint_path.clone());
    let next_turn = Arc::new(AtomicU64::new(
        checkpoint.as_ref().map(|c| c.next_commit).unwrap_or(1),
    ));
    let active_turns = Arc::new(Mutex::new(BTreeMap::<u64, String>::new()));
    let mut jobs = tokio::task::JoinSet::new();

    println!("[foreground] ready");
    let stdin = tokio::io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin).lines();
    while let Ok(Some(line)) = reader.next_line().await {
        while jobs.try_join_next().is_some() {}
        let (text, mut metadata) = match decode_chat_input(&line, role) {
            ChatInput::Cancel => {
                let _publication = crate::turns::PUBLICATION.lock().unwrap();
                let mut state = conversation.lock().unwrap();
                state.cancel_pending_turns(next_turn.load(Ordering::Relaxed));
                active_turns.lock().unwrap().clear();
                jobs.abort_all();
                let _ = checkpoint_tx.send(checkpoint_snapshot(&state));
                state_changed.notify_waiters();
                continue;
            }
            ChatInput::User { text, metadata } => (text, metadata),
            ChatInput::CampaignAssessment {
                assessment,
                metadata,
            } => {
                let mut state = conversation.lock().unwrap();
                if !state.retain_assessment(assessment.clone()) {
                    continue;
                }
                let _ = checkpoint_tx.send(checkpoint_snapshot(&state));
                // A stable publication identity makes retries idempotent at the
                // host/history boundary. No new turn, provider call or pending reply.
                drop(state);
                emit_interaction_event(
                    &metadata,
                    InteractionEvent::UserVisibleNotificationPublished {
                        text: assessment.advisory(),
                    },
                );
                continue;
            }
            ChatInput::Evidence(record) => {
                if is_completed_evidence(record.event()) {
                    let mut state = conversation.lock().unwrap();
                    if !state
                        .evidence
                        .iter()
                        .any(|existing| same_evidence(existing, &record))
                    {
                        state.evidence.push(record);
                        let _ = checkpoint_tx.send(checkpoint_snapshot(&state));
                        state_changed.notify_waiters();
                    }
                }
                continue;
            }
            ChatInput::Recovery(sessions) => {
                if !sessions.is_empty() {
                    let summary = sessions
                        .iter()
                        .map(|session| format!("{} ({})", session.description, session.state))
                        .collect::<Vec<_>>()
                        .join(", ");
                    emit_interaction_event(
                        &synthetic_interaction_metadata(None),
                        InteractionEvent::UserVisibleNotificationPublished {
                            text: format!("Recovered persistent work: {summary}"),
                        },
                    );
                }
                continue;
            }
            ChatInput::Notification {
                text,
                mut metadata,
                model: use_model,
            } => {
                // Deterministic attention is an overlay, not a conversation turn.
                // It must neither wait for a provider nor mutate partial history.
                if !use_model && metadata.message_id.starts_with("attention-command-") {
                    emit_interaction_event(
                        &metadata,
                        InteractionEvent::UserVisibleNotificationPublished { text },
                    );
                    continue;
                }
                let turn = next_turn.fetch_add(1, Ordering::Relaxed);
                metadata.turn_id = Some(turn.to_string());
                let model = model.clone();
                let conversation = Arc::clone(&conversation);
                let checkpoint_tx = checkpoint_tx.clone();
                let state_changed = Arc::clone(&state_changed);
                jobs.spawn(async move {
                    let notification = if use_model {
                        modeled_notification(model.as_deref(), &conversation, &text).await
                    } else {
                        text
                    };
                    let mut state = conversation.lock().unwrap();
                    if state.turn_terminal(turn) {
                        return;
                    }
                    emit_interaction_event(
                        &metadata,
                        InteractionEvent::UserVisibleNotificationPublished {
                            text: notification.clone(),
                        },
                    );
                    state
                        .pending
                        .insert(turn, vec![ChatMessage::new(Role::Assistant, notification)]);
                    commit_ready_turns(&mut state);
                    let _ = checkpoint_tx.send(checkpoint_snapshot(&state));
                    state_changed.notify_waiters();
                });
                continue;
            }
            ChatInput::Compaction(command) => {
                let retained_context_tokens = {
                    let mut state = conversation.lock().unwrap();
                    if command.epoch > state.context_epoch {
                        compact_context_messages(&mut state.messages, command.target_tokens);
                        // Completed evidence is scoped separately from model context.
                        state.context_epoch = command.epoch;
                        let _ = checkpoint_tx.send(checkpoint_snapshot(&state));
                    }
                    estimated_context_tokens(&state.messages)
                };
                emit_event(AgentEvent::ContextCompacted {
                    request_id: command.request_id,
                    epoch: command.epoch,
                    retained_context_tokens,
                });
                continue;
            }
            ChatInput::Ignore => continue,
        };
        let turn = next_turn.fetch_add(1, Ordering::Relaxed);
        metadata.turn_id = Some(turn.to_string());
        let accepted_at = std::time::Instant::now();
        emit_interaction_event(
            &metadata,
            InteractionEvent::UserTurnAccepted { text: text.clone() },
        );
        emit_event(AgentEvent::Timing {
            turn,
            stage: "input_accepted".into(),
            elapsed_ms: accepted_at.elapsed().as_millis() as u64,
        });
        let (queued, classifier_context) = {
            let mut active = active_turns.lock().unwrap();
            let queued = !active.is_empty();
            let context = active
                .iter()
                .map(|(turn, text)| format!("Active turn {turn}: {text}"))
                .collect::<Vec<_>>()
                .join("\n");
            active.insert(turn, text.clone());
            (queued, context)
        };
        let fallback_decision = fallback_interaction_decision(&classifier_context, &text);
        if queued {
            emit_turn(Some(turn), "[status] routing alongside active work".into());
        }
        let model = model.clone();
        let model_error = model_error.clone();
        let active_turns = Arc::clone(&active_turns);
        let conversation = Arc::clone(&conversation);
        let state_changed = Arc::clone(&state_changed);
        let checkpoint_tx = checkpoint_tx.clone();
        let agent_id = agent_id.clone();
        // One task and one cancellable timer per admitted turn, including routing.
        // Independent turns never wait behind another turn's provider request.
        jobs.spawn(with_turn_feedback(
            turn,
            accepted_at,
            acknowledgement_delay(),
            async move {
                let (decision, acknowledgement, usage) =
                    if let Some(model) = model.as_ref().filter(|_| queued) {
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            classify(model, &classifier_context, &text),
                        )
                        .await
                        {
                            Ok(Ok((decision, Some(acknowledgement), usage))) => {
                                (decision, Some(acknowledgement), usage)
                            }
                            Ok(Ok((_, None, usage))) => (fallback_decision, None, usage),
                            Ok(Err(_)) | Err(_) => (fallback_decision, None, TokenUsage::default()),
                        }
                    } else {
                        (fallback_decision, None, TokenUsage::default())
                    };
                if queued {
                    emit_event(AgentEvent::Timing {
                        turn,
                        stage: "routing".into(),
                        elapsed_ms: accepted_at.elapsed().as_millis() as u64,
                    });
                }
                process_turn(
                    (
                        turn,
                        text,
                        metadata,
                        queued,
                        decision,
                        acknowledgement,
                        usage,
                        accepted_at,
                        model,
                        model_error,
                        conversation,
                        active_turns,
                        state_changed,
                        checkpoint_tx,
                        role,
                        agent_id,
                    ),
                    emit_interaction_event,
                )
                .await;
            },
        ));
    }
    ExitCode::SUCCESS
}

fn fallback_interaction_decision(_active_context: &str, _incoming: &str) -> InteractionDecision {
    // Lack of shared words does not establish independence for an implicit follow-up.
    InteractionDecision::WaitForActiveTurn
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delegation::delegation_requests;
    use std::collections::BTreeSet;
    use tachyon_api::types::ApiRequest;
    use tachyon_api::{InteractionCommand, InteractionCommandEnvelope, FOREGROUND_ID};
    use tachyon_model::ToolCall;

    fn interaction_metadata() -> tachyon_api::InteractionMetadata {
        tachyon_api::InteractionMetadata::new("command-1", "turn-1", FOREGROUND_ID, 1)
    }
    #[tokio::test]
    async fn concurrent_turns_propagate_host_workspace_to_single_and_fanout() {
        let mut jobs = Vec::new();
        for (turn, cwd) in [(1, Some("/project/a")), (2, None), (3, Some("/project/b"))] {
            let mut metadata = interaction_metadata();
            metadata.cwd = cwd.map(str::to_string);
            let wire = serde_json::to_string(&InteractionCommandEnvelope {
                metadata,
                command: InteractionCommand::AcceptUserTurn {
                    text: "Use /model/prose as cwd".into(),
                },
            })
            .unwrap();
            let ChatInput::User { metadata, .. } =
                decode_chat_input(&wire, AgentRole::Conversation)
            else {
                panic!("expected user turn");
            };
            jobs.push(tokio::spawn(async move {
                tokio::task::yield_now().await;
                for (name, count) in [("spawn_agent", 1), ("spawn_agents", 2)] {
                    let call = ToolCall {
                        id: format!("call-{turn}"),
                        name: name.into(),
                        arguments: r#"{"task":"one","tasks":["one","two"],"cwd":"/model/argument"}"#.into(),
                    };
                    let requests = delegation_requests(&call, Some(turn), metadata.cwd.clone()).unwrap();
                    assert_eq!(requests.len(), count);
                    let mut ids = BTreeSet::new();
                    for request in requests {
                        let wire = serde_json::to_string(&request).unwrap();
                        let ApiRequest::BackgroundDelegate { cwd: selected, origin_turn_id, logical_task_id, .. } = serde_json::from_str(&wire).unwrap() else {
                            panic!("expected delegation");
                        };
                        assert_eq!(selected.as_deref(), cwd);
                        assert_eq!(origin_turn_id, Some(format!("{}:{turn}", crate::streaming::session_id())));
                        assert!(ids.insert(logical_task_id));
                    }
                }
            }));
        }
        for job in jobs {
            job.await.unwrap();
        }
    }

    #[test]
    fn legacy_turns_default_to_managed_without_prose_authority() {
        let ChatInput::User { metadata, .. } =
            decode_chat_input("work in /some/path", AgentRole::Conversation)
        else {
            panic!("expected legacy user turn");
        };
        assert!(metadata.cwd.is_none());
        let mut wire = serde_json::to_value(InteractionCommandEnvelope {
            metadata: interaction_metadata(),
            command: InteractionCommand::AcceptUserTurn {
                text: "hello".into(),
            },
        })
        .unwrap();
        wire.as_object_mut().unwrap().remove("cwd");
        let decoded: InteractionCommandEnvelope = serde_json::from_value(wire).unwrap();
        assert!(decoded.metadata.cwd.is_none());
    }

    #[test]
    fn routing_fallback_does_not_guess_independence_from_missing_shared_words() {
        let active = "Active turn 2: get the weather in London";
        assert_eq!(
            fallback_interaction_decision(active, "will I need a coat in London?"),
            InteractionDecision::WaitForActiveTurn
        );
        assert_eq!(
            fallback_interaction_decision(active, "tell me a joke"),
            InteractionDecision::WaitForActiveTurn
        );
        assert_eq!(
            fallback_interaction_decision(active, "summarize our current conversation"),
            InteractionDecision::WaitForActiveTurn
        );
        assert_eq!(
            fallback_interaction_decision(active, "will I need a coat?"),
            InteractionDecision::WaitForActiveTurn
        );
    }
}
