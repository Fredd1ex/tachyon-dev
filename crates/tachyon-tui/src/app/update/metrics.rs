//! Correlated aggregate accounting reduction.
use crate::app::model::metrics::TokenTotals;
use crate::app::model::thread::{find_or_create_thread, Thread};
use crate::app::update::projected_turn;
use crate::app::{elapsed, session_archive, turn_activity};
use tachyon_api::types::{
    Actor, AgentEvent, EventEnvelope, MemoryMutationKind, MemoryMutationResult, WorkOutcome,
};
use tachyon_api::FOREGROUND_ID;

pub(in crate::app) fn qualify_event_turn(envelope: &mut EventEnvelope) {
    if let (Some(conversation), Some(turn)) = (&envelope.conversation_id, &envelope.turn_id) {
        if !turn.starts_with("conversation:") {
            envelope.turn_id = Some(session_archive::conversation_turn(conversation, turn));
        }
    }
}

pub(in crate::app) fn record_correlated_metrics(
    threads: &mut Vec<Thread>,
    envelope: &EventEnvelope,
) {
    let root = find_or_create_thread(threads, FOREGROUND_ID, true, None);
    threads[root].touch();
    let revision = threads[root].revision;
    elapsed::record(&mut threads[root], envelope);
    turn_activity::record(&mut threads[root], envelope);
    match (&envelope.actor, &envelope.kind) {
        (
            Actor::Foreground,
            AgentEvent::Timing {
                turn,
                stage,
                elapsed_ms,
            },
        ) => {
            let turn = projected_turn(Some(*turn), envelope.turn_id.as_deref()).unwrap();
            let metrics = threads[root].metrics.entry(turn.clone()).or_default();
            match stage.as_str() {
                "first_visible" => metrics.first_visible_ms = Some(*elapsed_ms),
                "completed" => metrics.completed_ms = Some(*elapsed_ms),
                _ => {}
            }
            threads[root].metric_revisions.insert(turn, revision);
        }
        (
            Actor::Foreground,
            AgentEvent::Usage {
                turn: Some(turn),
                prompt_tokens,
                completion_tokens,
                total_tokens,
                ..
            },
        ) => {
            let turn = projected_turn(Some(*turn), envelope.turn_id.as_deref()).unwrap();
            threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .self_usage = Some(token_totals(
                *prompt_tokens,
                *completion_tokens,
                *total_tokens,
            ));
            threads[root].metric_revisions.insert(turn, revision);
        }
        (
            Actor::Worker { id },
            AgentEvent::Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                ..
            },
        ) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let assignment = envelope
                .task_id
                .clone()
                .unwrap_or_else(|| format!("{}:{}", envelope.session_id, id));
            threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .worker_usage
                .insert(
                    assignment,
                    token_totals(*prompt_tokens, *completion_tokens, *total_tokens),
                );
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (
            _,
            AgentEvent::MemoryRecalled {
                preference_count,
                history_count,
                ..
            },
        ) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let memory = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .memory;
            memory.recalled_preferences = memory.recalled_preferences.max(*preference_count);
            memory.recalled_history = memory.recalled_history.max(*history_count);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::MemoryMutation { result, .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let memory = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .memory;
            match result {
                MemoryMutationResult::Applied { kind, .. } => match kind {
                    MemoryMutationKind::Remember => memory.saved = memory.saved.saturating_add(1),
                    MemoryMutationKind::Forget => {
                        memory.forgotten = memory.forgotten.saturating_add(1)
                    }
                    MemoryMutationKind::Correct => {
                        memory.corrected = memory.corrected.saturating_add(1)
                    }
                },
                MemoryMutationResult::Rejected { .. } | MemoryMutationResult::Unavailable => {
                    memory.failed = memory.failed.saturating_add(1)
                }
                MemoryMutationResult::Ignored | MemoryMutationResult::AlreadyApplied { .. } => {}
            }
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::ReminderScheduled { .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let schedule = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .schedule;
            schedule.scheduled = schedule.scheduled.saturating_add(1);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::ScheduledTaskCreated { .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let schedule = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .schedule;
            schedule.tasks_scheduled = schedule.tasks_scheduled.saturating_add(1);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::ReminderCancelled { .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let schedule = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .schedule;
            schedule.cancelled = schedule.cancelled.saturating_add(1);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::ReminderFired { .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let schedule = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .schedule;
            schedule.fired = schedule.fired.saturating_add(1);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::WorkResult { result })
            if envelope
                .turn_id
                .as_deref()
                .is_some_and(|turn| turn.starts_with("conversation:")) =>
        {
            let turn = envelope.turn_id.as_ref().unwrap();
            threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .worker_outcomes
                .insert(
                    format!(
                        "{}:{}:{}",
                        result.work_id, result.generation, result.assignment
                    ),
                    matches!(result.outcome, WorkOutcome::Completed { .. }),
                );
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        _ => {}
    }
}

pub(in crate::app) fn token_totals(prompt: u32, completion: u32, total: u32) -> TokenTotals {
    TokenTotals {
        prompt: u64::from(prompt),
        completion: u64::from(completion),
        total: u64::from(if total == 0 {
            prompt.saturating_add(completion)
        } else {
            total
        }),
    }
}
