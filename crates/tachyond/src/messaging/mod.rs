//! Interaction wire codecs, publication side effects, and in-memory subscriptions.
//! Sequence IDs are process-local identities, not durable subscription cursors.

mod commands;
mod notifications;
mod subscriptions;

pub(super) use commands::encode_interaction_command;
pub(super) use notifications::{
    acknowledge_reminder_notification, emit_schedule_event, encode_reminder_notification,
    encode_scheduled_task_notification, persist_interaction_history, project_pending_history,
    work_attention_notification,
};
pub(super) use subscriptions::{stream_agent, stream_work};
