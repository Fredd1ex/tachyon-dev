//! Conversation event reduction and explicit command receipts.
use super::model::items::{AssignmentKey, WorkDetail};
use super::tool_parts;
use tachyon_api::types::{Actor, AgentEvent, WorkOutcome};
use tachyon_api::{InteractionEvent, InteractionEventEnvelope, FOREGROUND_ID};

use super::services::control::{Completion, Output};
use super::{attention, find_or_create_thread, turn_activity, ItemKind, Thread};

pub(super) fn control(
    result: Completion,
    attention: &mut attention::State,
    threads: &mut Vec<Thread>,
) {
    let report = result.report();
    match result.output {
        Output::Interaction(receipt) => {
            let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
            if let (Some(origin), Some(accepted)) = (&receipt.origin, &receipt.accepted) {
                if origin.session_id == receipt.command.session_id
                    && origin.command_id == receipt.command.command_id
                {
                    let turn = super::session_archive::conversation_turn(
                        &receipt.command.conversation_id,
                        &accepted.turn_id,
                    );
                    super::services::interaction::bind(&mut threads[idx], origin, &turn);
                }
            }
            let streaming = threads[idx].streaming;
            threads[idx].add(ItemKind::System, report);
            threads[idx].streaming = streaming;
        }
        Output::Attention(output) => {
            let output = match output {
                Ok((records, _)) => Ok((records, report)),
                Err(_) => Err(report),
            };
            attention.complete(attention::ResultEvent::Command(output), threads)
        }
        Output::Message(output) => {
            let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
            let streaming = threads[idx].streaming;
            threads[idx].add(
                if output.is_ok() {
                    ItemKind::System
                } else {
                    ItemKind::Error
                },
                report,
            );
            threads[idx].streaming = streaming;
        }
    }
}

pub(super) fn accept_user_turn(thread: &mut Thread, text: &str, turn: Option<String>) {
    if turn.is_some()
        && thread
            .items
            .iter()
            .any(|item| item.kind == ItemKind::User && item.turn == turn)
    {
        return;
    }
    // Accept identical queued prompts in submission order, never from an archive.
    if let Some(index) = thread.items[thread.history_len..].iter().position(|item| {
        item.kind == ItemKind::User && item.turn.is_none() && item.text.trim() == text.trim()
    }) {
        let index = thread.history_len + index;
        thread.touch_structure();
        thread.items[index].turn = turn.clone();
        thread.items[index].revision = thread.revision;
        if let Some(pending_index) = thread.items[index + 1..]
            .iter_mut()
            .position(|item| item.kind == ItemKind::PendingReply && item.turn.is_none())
        {
            let pending = &mut thread.items[index + 1 + pending_index];
            pending.turn = turn;
            pending.revision = thread.revision;
        }
    } else if !thread
        .items
        .iter()
        .any(|item| item.kind == ItemKind::User && item.turn == turn && item.text == text)
    {
        thread.add_turn(ItemKind::User, text.to_owned(), turn);
    }
}

pub(super) fn apply_interaction_event(thread: &mut Thread, envelope: InteractionEventEnvelope) {
    let turn = envelope.metadata.turn_id;
    if let Some(turn) = turn.as_ref().filter(|_| {
        matches!(
            envelope.event,
            InteractionEvent::UserTurnAccepted { .. }
                | InteractionEvent::ConversationFinished { .. }
                | InteractionEvent::ForegroundRequestTimedOut { .. }
        )
    }) {
        let metrics = thread.metrics.entry(turn.clone()).or_default();
        match &envelope.event {
            InteractionEvent::UserTurnAccepted { .. } => {
                metrics
                    .accepted_at_ms
                    .get_or_insert(envelope.metadata.occurred_at_ms);
            }
            InteractionEvent::ConversationFinished { .. }
            | InteractionEvent::ForegroundRequestTimedOut { .. } => {
                metrics
                    .ended_at_ms
                    .get_or_insert(envelope.metadata.occurred_at_ms);
            }
            _ => {}
        }
        thread.touch();
        thread
            .metric_revisions
            .insert(turn.clone(), thread.revision);
    }
    match envelope.event {
        InteractionEvent::UserTurnAccepted { text } => accept_user_turn(thread, &text, turn),
        InteractionEvent::ConversationDelta { text } => {
            thread.add_reply_fragment(text, turn, false)
        }
        InteractionEvent::ConversationFinished { text } => thread.finish_reply(text, turn),
        InteractionEvent::ConversationIntentProduced { .. } => thread.touch(),
        InteractionEvent::ForegroundRequestTimedOut { deadline_ms } => turn_activity::reply(
            thread,
            turn,
            turn_activity::ReplyUpdate::Failed(format!("request timed out after {deadline_ms}ms")),
        ),
        InteractionEvent::UserVisibleNotificationPublished { text } => {
            thread.add_turn(ItemKind::Reply, text, turn)
        }
    }
}

#[cfg(test)]
pub(super) fn apply_agent_event(thread: &mut Thread, event: AgentEvent) {
    apply_correlated_agent_event(thread, event, None);
}

pub(super) fn projected_turn(turn: Option<u64>, envelope_turn: Option<&str>) -> Option<String> {
    envelope_turn
        .map(str::to_owned)
        .or_else(|| turn.map(|turn| turn.to_string()))
}

pub(super) fn apply_correlated_agent_event(
    thread: &mut Thread,
    event: AgentEvent,
    envelope_turn: Option<&str>,
) {
    match event {
        AgentEvent::Status {
            turn,
            phase,
            message,
        } => {
            let turn = projected_turn(turn, envelope_turn);
            if matches!(phase.as_str(), "queued" | "working") {
                thread.update_pending_reply_status(turn.clone(), &phase, &message);
            }
            thread.add_turn(ItemKind::System, format!("[{phase}] {message}"), turn);
        }
        AgentEvent::ReplyDelta { turn, text } => {
            thread.add_reply_fragment(text, projected_turn(turn, envelope_turn), false)
        }
        AgentEvent::Reply {
            turn,
            text,
            final_reply: _,
        } => thread.finish_reply(text, projected_turn(turn, envelope_turn)),
        AgentEvent::Timing {
            turn,
            stage,
            elapsed_ms,
        } => thread.add_turn(
            ItemKind::System,
            format!("[timing] {stage} {elapsed_ms}ms"),
            projected_turn(Some(turn), envelope_turn),
        ),
        AgentEvent::WorkerStarted {
            turn,
            worker_id,
            objective,
        } => thread.add_turn(
            ItemKind::Spawn,
            format!("worker {worker_id}: {objective}"),
            projected_turn(turn, envelope_turn),
        ),
        AgentEvent::Usage {
            turn: Some(turn),
            prompt_tokens,
            completion_tokens,
            total_tokens,
            ..
        } => {
            thread
                .usage
                .insert(turn, (prompt_tokens, completion_tokens, total_tokens));
        }
        AgentEvent::Usage { turn: None, .. } => {}
        AgentEvent::WorkerCompleted {
            worker_id,
            objective,
            result,
            ..
        } => {
            // A worker may be reused across turns. Missing host correlation is
            // not permission to attach a completion to its latest start.
            let turn = envelope_turn.map(str::to_owned);
            thread.add_turn(
                ItemKind::SpawnResult,
                format!("worker {worker_id}: {objective}\n{result}"),
                turn,
            );
        }
        AgentEvent::WorkCandidate { .. } => {}
        AgentEvent::ArtifactRegistered { artifact } => thread.add_turn(
            ItemKind::System,
            format!(
                "artifact {} ({} bytes, {}): {}",
                artifact.path, artifact.size_bytes, artifact.kind, artifact.description
            ),
            None,
        ),
        AgentEvent::WorkProgress { event } => thread.add_turn(
            ItemKind::System,
            format!("work {}: {:?}", event.work_id, event.kind),
            None,
        ),
        AgentEvent::WorkResult { result } => {
            let key = AssignmentKey {
                work_id: result.work_id.clone(),
                generation: result.generation,
                assignment: result.assignment,
            };
            let turn = envelope_turn.map(str::to_owned);
            for (slot, tool) in result.evidence.tools.into_iter().enumerate() {
                let existing = thread.items.iter().position(|item| {
                    item.work
                        .as_ref()
                        .is_some_and(|work| work.key == key && work.slot == Some(slot))
                        && item.turn == turn
                });
                // Legacy live events have no assignment key. Only claim a unique call
                // in the same turn, never a call already claimed by another assignment.
                let live = if existing.is_none()
                    && turn.is_some()
                    && tool.parent_call_id.is_none()
                    && tool.call_id.is_some()
                {
                    let candidates = thread
                        .items
                        .iter()
                        .enumerate()
                        .filter(|(_, item)| {
                            item.kind == ItemKind::Tool
                                && item.work.is_none()
                                && item.turn == turn
                                && item.tool_id == tool.call_id
                                && tool_parts(&item.text).0 == tool.tool_name
                        })
                        .map(|(index, _)| index)
                        .collect::<Vec<_>>();
                    (candidates.len() == 1).then(|| candidates[0])
                } else {
                    None
                };
                let index = existing.or(live).unwrap_or_else(|| {
                    thread.add_tool(
                        tool.tool_name.clone(),
                        tool.call_id.clone().unwrap_or_default(),
                        turn.clone(),
                    );
                    thread.items.len() - 1
                });
                thread.touch_structure();
                let item = &mut thread.items[index];
                item.text = tool.tool_name.clone();
                item.output = None;
                item.revision = thread.revision;
                item.work = Some(WorkDetail {
                    raw_open: item.work.as_ref().is_some_and(|work| work.raw_open),
                    key: key.clone(),
                    slot: Some(slot),
                    tool: Some(tool),
                    timing: None,
                    omitted: 0,
                });
            }
            let (kind, text) = match result.outcome {
                WorkOutcome::Completed { result: text, .. } => (ItemKind::SpawnResult, text),
                WorkOutcome::Blocked { reason } => (ItemKind::Error, format!("blocked: {reason}")),
                WorkOutcome::Failed { message } => (ItemKind::Error, message),
                WorkOutcome::Cancelled { reason } => {
                    (ItemKind::Error, format!("cancelled: {reason}"))
                }
                WorkOutcome::TimedOut { .. } => (ItemKind::Error, "timed out".into()),
            };
            let text = format!("work {}: {}\n{text}", result.work_id, result.objective);
            let existing = thread.items.iter().position(|item| {
                item.work
                    .as_ref()
                    .is_some_and(|work| work.key == key && work.slot.is_none())
                    && item.turn == turn
            });
            let index = existing.unwrap_or_else(|| {
                thread.add_turn(kind.clone(), text.clone(), turn);
                thread.items.len() - 1
            });
            thread.touch_structure();
            let item = &mut thread.items[index];
            item.kind = kind;
            item.text = text;
            item.revision = thread.revision;
            item.work = Some(WorkDetail {
                raw_open: false,
                key,
                slot: None,
                tool: None,
                timing: result.timing,
                omitted: result.evidence.omitted,
            });
        }
        AgentEvent::WorkerReleaseRequested { reason } => thread.add_turn(
            ItemKind::System,
            format!("worker release requested: {reason}"),
            None,
        ),
        AgentEvent::ToolStarted {
            turn,
            id,
            name,
            arguments,
            ..
        } => {
            let turn = projected_turn(turn, envelope_turn);
            let mut matches = thread.items.iter().filter(|item| {
                item.kind == ItemKind::Tool
                    && item.tool_id.as_deref() == Some(id.as_str())
                    && item.turn == turn
                    && tool_parts(&item.text).0 == name
            });
            if matches.next().is_some() && matches.next().is_none() {
                return;
            }
            thread.add_tool(format!("{name} {arguments}"), id, turn);
        }
        AgentEvent::ToolFinished {
            turn, id, output, ..
        } => thread.add_tool_result(id, output, projected_turn(turn, envelope_turn)),
        AgentEvent::ToolTelemetry {
            tool_name,
            duration_ms,
            success,
            truncated,
            bytes_out,
            error_code,
            ..
        } => {
            let outcome = if success { "complete" } else { "failed" };
            let truncation = if truncated { " · truncated" } else { "" };
            let error = error_code
                .map(|code| format!(" · {code}"))
                .unwrap_or_default();
            thread.add_turn(ItemKind::System,
                format!("[tool telemetry] {tool_name} {outcome} · {duration_ms}ms · {bytes_out} bytes{truncation}{error}"), envelope_turn.map(str::to_owned));
        }
        AgentEvent::ContextCompacted {
            epoch,
            retained_context_tokens,
            ..
        } => thread.add_turn(
            ItemKind::System,
            format!(
                "[context compacted] epoch {epoch} · {retained_context_tokens} tokens retained"
            ),
            envelope_turn.map(str::to_owned),
        ),
        AgentEvent::MemorySaved { .. }
        | AgentEvent::MemoryRecalled { .. }
        | AgentEvent::MemoryMutation { .. }
        | AgentEvent::ReminderScheduled { .. }
        | AgentEvent::ReminderCancelled { .. }
        | AgentEvent::ReminderFired { .. }
        | AgentEvent::ScheduledTaskCreated { .. } => {}
        AgentEvent::Error { turn, message } => turn_activity::reply(
            thread,
            projected_turn(turn, envelope_turn),
            turn_activity::ReplyUpdate::Failed(message),
        ),
    }
}

pub(super) fn apply_actor_event(
    thread: &mut Thread,
    mut event: AgentEvent,
    actor: &Actor,
    envelope_turn: Option<&str>,
) {
    if matches!(actor, Actor::Background) {
        if let AgentEvent::Status {
            turn,
            phase,
            message,
        } = event
        {
            thread.add_turn(
                ItemKind::System,
                format!("[Background][{phase}] {message}"),
                projected_turn(turn, envelope_turn),
            );
            return;
        }
        match &mut event {
            AgentEvent::Reply { text, .. } => *text = format!("[Background] {text}"),
            AgentEvent::ToolStarted { name, .. } => *name = format!("Background::{name}"),
            _ => {}
        }
    }
    apply_correlated_agent_event(thread, event, envelope_turn);
}

pub(in crate::app) mod metrics;

pub(in crate::app) mod raw_line;

mod events;
