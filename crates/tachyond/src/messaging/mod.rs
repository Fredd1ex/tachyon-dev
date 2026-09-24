//! Interaction wire codecs, publication side effects, and in-memory subscriptions.
//! Sequence IDs are process-local identities, not durable subscription cursors.

mod commands;
pub(crate) mod notifications;
mod subscriptions;

pub(super) use commands::encode_interaction_command;
pub(super) use notifications::{
    acknowledge_reminder_notification, deliver_attention, emit_schedule_event,
    encode_reminder_notification, encode_scheduled_task_notification, project_pending_history,
};
pub(super) use subscriptions::{stream_agent, stream_work};
