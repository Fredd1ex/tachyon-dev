use std::path::Path;

pub(crate) mod admission;
pub(crate) mod campaign_launch;
pub(crate) mod campaign_ledger;
pub(crate) mod coordination;
#[cfg(target_os = "linux")]
pub(crate) mod execution;
pub(crate) mod groups;
mod integration;
pub(crate) mod model_accounting;
mod research;
#[cfg(target_os = "linux")]
pub(crate) mod research_context;
#[cfg(target_os = "linux")]
pub(crate) mod scheduler;

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tachyon_api::types::{
    AgentInfo, ContextCompactionCommand, HistoryKind, HistoryRole, ReminderInfo, ReminderStatus,
    ScheduledTaskInfo, ScheduledTaskMode, ScheduledTaskStatus,
};

const SCHEMA_VERSION: u64 = 1;
const SCHEMA_VERSION_KEY: &str = "schema_version";
const NEXT_EVENT_SEQUENCE_KEY: &str = "next_event_sequence";

const METADATA: TableDefinition<&str, u64> = TableDefinition::new("metadata");
const TASKS: TableDefinition<&str, &[u8]> = TableDefinition::new("tasks");
const TASK_EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("task_events");
const COMMANDS: TableDefinition<&str, u64> = TableDefinition::new("commands");
const MIGRATIONS: TableDefinition<&str, u64> = TableDefinition::new("migrations");
const HISTORY_OUTBOX: TableDefinition<&str, &[u8]> = TableDefinition::new("history_outbox");
const HISTORY_OUTBOX_ACKS: TableDefinition<&str, u64> = TableDefinition::new("history_outbox_acks");
const CONTEXT_COMPACTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("context_compactions");
const REMINDERS: TableDefinition<&str, &[u8]> = TableDefinition::new("reminders");
const SCHEDULED_TASKS: TableDefinition<&str, &[u8]> = TableDefinition::new("scheduled_tasks");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HistoryProjection {
    pub schema_version: u32,
    pub event_id: String,
    #[serde(default)]
    pub kind: HistoryKind,
    pub conversation_id: String,
    pub turn_id: Option<String>,
    pub occurred_at_ms: u64,
    pub role: HistoryRole,
    pub text: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub task_state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RuntimeTaskRecord {
    pub schema_version: u32,
    pub updated_at_ms: u64,
    pub info: AgentInfo,
    pub depends_on: Vec<String>,
    pub generation: u64,
    pub assignment: u64,
    pub warm: bool,
    pub ready: bool,
    pub owner: Option<String>,
    pub last_used_secs: u64,
    pub control_socket: Option<String>,
    pub terminal_usage: Option<String>,
    pub terminal_result: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TaskTransitionRecord {
    schema_version: u32,
    sequence: u64,
    occurred_at_ms: u64,
    task_id: String,
    state: String,
    note: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContextCompactionRecord {
    schema_version: u32,
    request_id: String,
    agent_id: String,
    generation: u64,
    assignment: u64,
    epoch: u64,
    context_tokens: u32,
    context_window: u32,
    target_tokens: u32,
    usage_event_id: u64,
    requested_at_ms: u64,
    completed_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReminderRecord {
    schema_version: u32,
    source_event_id: String,
    info: ReminderInfo,
    delivery_attempts: u32,
    delivery_started_at_ms: Option<u64>,
    delivered_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScheduledTaskRecord {
    schema_version: u32,
    source_event_id: String,
    info: ScheduledTaskInfo,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    delivery_started_at_ms: Option<u64>,
    #[serde(default)]
    result_delivered: bool,
}

mod compute;
mod host_capacity;

pub(crate) struct RuntimeStore {
    pub(crate) retained: tachyond::retained_storage::RetainedStorage,
    compute: std::sync::Mutex<compute::State>,
    host_capacity: host_capacity::HostCapacity,
    trace_root: std::path::PathBuf,
    trace_limits: research_context::traces::TraceLimits,
    pub(crate) attention_notifications:
        std::sync::Mutex<std::collections::VecDeque<tachyon_api::work::Attention>>,
    database: std::sync::Arc<Database>,
    model_permits: std::sync::Mutex<model_accounting::PermitState>,
    #[cfg(target_os = "linux")]
    host_catalog: std::sync::Mutex<scheduler::HostCatalog>,
}

impl RuntimeStore {
    pub(crate) fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "create runtime database directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        let database = Database::create(path)
            .map_err(|error| format!("open runtime database {}: {error}", path.display()))?;
        let database = std::sync::Arc::new(database);
        let maximum = tachyon_util::config::Config::try_load_from(
            &tachyon_util::config::Config::default_path(),
        )
        .map_err(|e| e.to_string())?
        .campaign_resources
        .max_retained_storage_bytes;
        let store = Self {
            retained: tachyond::retained_storage::RetainedStorage::new(database.clone(), maximum)?,
            compute: Default::default(),
            host_capacity: host_capacity::HostCapacity::new(
                tachyon_util::config::Config::try_load_from(
                    &tachyon_util::config::Config::default_path(),
                )
                .map_err(|e| e.to_string())?
                .campaign_resources,
            )?,
            trace_root: path.with_extension("traces"),
            trace_limits: research_context::traces::TraceLimits::configured()?,
            attention_notifications: Default::default(),
            database,
            model_permits: Default::default(),
            #[cfg(target_os = "linux")]
            host_catalog: Default::default(),
        };
        store.initialize()?;
        store.adopt_retained_traces()?;
        store.adopt_retained_snapshots()?;
        Ok(store)
    }

    fn initialize(&self) -> Result<(), String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin runtime schema transaction: {error}"))?;
        {
            let mut metadata = write
                .open_table(METADATA)
                .map_err(|error| format!("open runtime metadata: {error}"))?;
            let found = metadata
                .get(SCHEMA_VERSION_KEY)
                .map_err(|error| format!("read runtime schema version: {error}"))?
                .map(|value| value.value());
            match found {
                Some(SCHEMA_VERSION) => {}
                Some(version) => {
                    return Err(format!(
                        "unsupported runtime database schema {version}; expected {SCHEMA_VERSION}"
                    ));
                }
                None => {
                    metadata
                        .insert(SCHEMA_VERSION_KEY, SCHEMA_VERSION)
                        .map_err(|error| format!("initialize runtime schema version: {error}"))?;
                    metadata
                        .insert(NEXT_EVENT_SEQUENCE_KEY, 1)
                        .map_err(|error| format!("initialize runtime event sequence: {error}"))?;
                }
            }
            write
                .open_table(TASKS)
                .map_err(|error| format!("create runtime tasks table: {error}"))?;
            write
                .open_table(TASK_EVENTS)
                .map_err(|error| format!("create runtime task events table: {error}"))?;
            write
                .open_table(COMMANDS)
                .map_err(|error| format!("create runtime commands table: {error}"))?;
            write
                .open_table(MIGRATIONS)
                .map_err(|error| format!("create runtime migrations table: {error}"))?;
            write
                .open_table(HISTORY_OUTBOX)
                .map_err(|error| format!("create history outbox table: {error}"))?;
            write
                .open_table(HISTORY_OUTBOX_ACKS)
                .map_err(|error| format!("create history outbox acknowledgements: {error}"))?;
            write
                .open_table(CONTEXT_COMPACTIONS)
                .map_err(|error| format!("create context compactions: {error}"))?;
            write
                .open_table(REMINDERS)
                .map_err(|error| format!("create reminders: {error}"))?;
            write
                .open_table(SCHEDULED_TASKS)
                .map_err(|error| format!("create scheduled tasks: {error}"))?;
            research::initialize(&write)?;
            write.open_table(compute::JOBS).map_err(|e| e.to_string())?;
            write
                .open_table(campaign_launch::LAUNCHES)
                .map_err(|e| e.to_string())?;
            campaign_ledger::initialize(&write)?;
            admission::initialize(&write)?;
            groups::initialize(&write)?;
            coordination::initialize(&write)?;
            #[cfg(target_os = "linux")]
            execution::initialize(&write)?;
        }
        write
            .commit()
            .map_err(|error| format!("commit runtime schema transaction: {error}"))
    }

    /// Atomically updates the task projection and appends its transition event.
    /// A command ID makes retries return the original event without rewriting.
    pub(crate) fn persist_task_transition(
        &self,
        task: &RuntimeTaskRecord,
        note: &str,
        command_id: Option<&str>,
    ) -> Result<u64, String> {
        let task_bytes = serde_json::to_vec(task)
            .map_err(|error| format!("encode runtime task {}: {error}", task.info.id))?;
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin runtime task transaction: {error}"))?;

        let existing_sequence = if let Some(command_id) = command_id {
            let commands = write
                .open_table(COMMANDS)
                .map_err(|error| format!("open runtime commands: {error}"))?;
            let sequence = commands
                .get(command_id)
                .map_err(|error| format!("read runtime command {command_id}: {error}"))?
                .map(|value| value.value());
            sequence
        } else {
            None
        };
        if let Some(sequence) = existing_sequence {
            return Ok(sequence);
        }

        let sequence = {
            let mut metadata = write
                .open_table(METADATA)
                .map_err(|error| format!("open runtime metadata: {error}"))?;
            let sequence = metadata
                .get(NEXT_EVENT_SEQUENCE_KEY)
                .map_err(|error| format!("read runtime event sequence: {error}"))?
                .map(|value| value.value())
                .unwrap_or(1);
            metadata
                .insert(NEXT_EVENT_SEQUENCE_KEY, sequence.saturating_add(1))
                .map_err(|error| format!("advance runtime event sequence: {error}"))?;
            sequence
        };
        let event = TaskTransitionRecord {
            schema_version: SCHEMA_VERSION as u32,
            sequence,
            occurred_at_ms: task.updated_at_ms,
            task_id: task.info.id.clone(),
            state: task.info.state.to_string(),
            note: note.to_string(),
        };
        let event_bytes = serde_json::to_vec(&event)
            .map_err(|error| format!("encode runtime task event: {error}"))?;
        let history_projection = HistoryProjection {
            schema_version: SCHEMA_VERSION as u32,
            event_id: format!("runtime-task-transition-{sequence:020}"),
            kind: HistoryKind::Task,
            conversation_id: String::new(),
            turn_id: None,
            occurred_at_ms: task.updated_at_ms,
            role: HistoryRole::Notification,
            text: format!("{}: {note}", task.info.task),
            task_id: Some(task.info.id.clone()),
            task_state: Some(task.info.state.to_string()),
        };
        let history_bytes = serde_json::to_vec(&history_projection)
            .map_err(|error| format!("encode task history projection: {error}"))?;
        {
            let mut tasks = write
                .open_table(TASKS)
                .map_err(|error| format!("open runtime tasks: {error}"))?;
            tasks
                .insert(task.info.id.as_str(), task_bytes.as_slice())
                .map_err(|error| format!("write runtime task {}: {error}", task.info.id))?;
        }
        {
            let mut events = write
                .open_table(TASK_EVENTS)
                .map_err(|error| format!("open runtime task events: {error}"))?;
            events
                .insert(sequence, event_bytes.as_slice())
                .map_err(|error| format!("append runtime task event {sequence}: {error}"))?;
        }
        {
            let acknowledgements = write
                .open_table(HISTORY_OUTBOX_ACKS)
                .map_err(|error| format!("open history acknowledgements: {error}"))?;
            let acknowledged = acknowledgements
                .get(history_projection.event_id.as_str())
                .map_err(|error| format!("read task history acknowledgement: {error}"))?
                .is_some();
            drop(acknowledgements);
            if !acknowledged {
                write
                    .open_table(HISTORY_OUTBOX)
                    .map_err(|error| format!("open history outbox: {error}"))?
                    .insert(
                        history_projection.event_id.as_str(),
                        history_bytes.as_slice(),
                    )
                    .map_err(|error| format!("enqueue task history projection: {error}"))?;
            }
        }
        if let Some(command_id) = command_id {
            let mut commands = write
                .open_table(COMMANDS)
                .map_err(|error| format!("open runtime commands: {error}"))?;
            commands
                .insert(command_id, sequence)
                .map_err(|error| format!("write runtime command {command_id}: {error}"))?;
        }
        write
            .commit()
            .map_err(|error| format!("commit runtime task transaction: {error}"))?;
        Ok(sequence)
    }

    pub(crate) fn list_tasks(&self) -> Result<Vec<RuntimeTaskRecord>, String> {
        let read = self
            .database
            .begin_read()
            .map_err(|error| format!("begin runtime task read: {error}"))?;
        let tasks = read
            .open_table(TASKS)
            .map_err(|error| format!("open runtime tasks: {error}"))?;
        let mut records = Vec::new();
        let entries = tasks
            .iter()
            .map_err(|error| format!("iterate runtime tasks: {error}"))?;
        for entry in entries {
            let (_, value) = entry.map_err(|error| format!("read runtime task entry: {error}"))?;
            let record = serde_json::from_slice(value.value())
                .map_err(|error| format!("decode runtime task entry: {error}"))?;
            records.push(record);
        }
        Ok(records)
    }

    pub(crate) fn enqueue_history(&self, projection: &HistoryProjection) -> Result<(), String> {
        let bytes = serde_json::to_vec(projection)
            .map_err(|error| format!("encode history outbox {}: {error}", projection.event_id))?;
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin history outbox transaction: {error}"))?;
        {
            let acknowledgements = write
                .open_table(HISTORY_OUTBOX_ACKS)
                .map_err(|error| format!("open history outbox acknowledgements: {error}"))?;
            if acknowledgements
                .get(projection.event_id.as_str())
                .map_err(|error| format!("read history acknowledgement: {error}"))?
                .is_some()
            {
                return Ok(());
            }
        }
        {
            let mut outbox = write
                .open_table(HISTORY_OUTBOX)
                .map_err(|error| format!("open history outbox: {error}"))?;
            if outbox
                .get(projection.event_id.as_str())
                .map_err(|error| format!("read history outbox entry: {error}"))?
                .is_none()
            {
                outbox
                    .insert(projection.event_id.as_str(), bytes.as_slice())
                    .map_err(|error| format!("write history outbox entry: {error}"))?;
            }
        }
        write
            .commit()
            .map_err(|error| format!("commit history outbox transaction: {error}"))
    }

    pub(crate) fn pending_history(&self) -> Result<Vec<HistoryProjection>, String> {
        let read = self
            .database
            .begin_read()
            .map_err(|error| format!("begin history outbox read: {error}"))?;
        let outbox = read
            .open_table(HISTORY_OUTBOX)
            .map_err(|error| format!("open history outbox: {error}"))?;
        let entries = outbox
            .iter()
            .map_err(|error| format!("iterate history outbox: {error}"))?;
        let mut pending = Vec::new();
        for entry in entries {
            let (_, value) =
                entry.map_err(|error| format!("read history outbox entry: {error}"))?;
            pending.push(
                serde_json::from_slice(value.value())
                    .map_err(|error| format!("decode history outbox entry: {error}"))?,
            );
        }
        pending.sort_by(|left: &HistoryProjection, right| {
            left.occurred_at_ms
                .cmp(&right.occurred_at_ms)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        Ok(pending)
    }

    pub(crate) fn acknowledge_history(
        &self,
        event_id: &str,
        projected_at_ms: u64,
    ) -> Result<(), String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin history acknowledgement transaction: {error}"))?;
        {
            let mut outbox = write
                .open_table(HISTORY_OUTBOX)
                .map_err(|error| format!("open history outbox: {error}"))?;
            outbox
                .remove(event_id)
                .map_err(|error| format!("remove history outbox entry {event_id}: {error}"))?;
        }
        {
            let mut acknowledgements = write
                .open_table(HISTORY_OUTBOX_ACKS)
                .map_err(|error| format!("open history outbox acknowledgements: {error}"))?;
            acknowledgements
                .insert(event_id, projected_at_ms)
                .map_err(|error| format!("acknowledge history event {event_id}: {error}"))?;
        }
        write
            .commit()
            .map_err(|error| format!("commit history acknowledgement: {error}"))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_context_usage(
        &self,
        agent_id: &str,
        generation: u64,
        assignment: u64,
        usage_event_id: u64,
        context_tokens: u32,
        context_window: u32,
        occurred_at_ms: u64,
    ) -> Result<Option<ContextCompactionCommand>, String> {
        if context_window == 0 {
            return Ok(None);
        }
        let write = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        if (context_tokens as u64) * 100 <= (context_window as u64) * 45 {
            write
                .open_table(CONTEXT_COMPACTIONS)
                .map_err(|error| error.to_string())?
                .remove(agent_id)
                .map_err(|error| error.to_string())?;
            write.commit().map_err(|error| error.to_string())?;
            return Ok(None);
        }
        if (context_tokens as u64) * 100 < (context_window as u64) * 65 {
            return Ok(None);
        }
        let existing = {
            let compactions = write
                .open_table(CONTEXT_COMPACTIONS)
                .map_err(|error| error.to_string())?;
            let exists = compactions
                .get(agent_id)
                .map_err(|error| error.to_string())?
                .is_some();
            exists
        };
        if existing {
            return Ok(None);
        }
        let epoch_key = format!("compaction_epoch:{agent_id}");
        let epoch = {
            let mut metadata = write
                .open_table(METADATA)
                .map_err(|error| error.to_string())?;
            let previous = metadata
                .get(epoch_key.as_str())
                .map_err(|error| error.to_string())?
                .map(|value| value.value())
                .unwrap_or(0);
            let epoch = previous.saturating_add(1);
            metadata
                .insert(epoch_key.as_str(), epoch)
                .map_err(|error| error.to_string())?;
            epoch
        };
        let command = ContextCompactionCommand {
            request_id: format!("compact:{agent_id}:{epoch}"),
            epoch,
            target_tokens: context_window.saturating_mul(45) / 100,
        };
        let record = ContextCompactionRecord {
            schema_version: 1,
            request_id: command.request_id.clone(),
            agent_id: agent_id.into(),
            generation,
            assignment,
            epoch,
            context_tokens,
            context_window,
            target_tokens: command.target_tokens,
            usage_event_id,
            requested_at_ms: occurred_at_ms,
            completed_at_ms: None,
        };
        let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        write
            .open_table(CONTEXT_COMPACTIONS)
            .map_err(|error| error.to_string())?
            .insert(agent_id, bytes.as_slice())
            .map_err(|error| error.to_string())?;
        write.commit().map_err(|error| error.to_string())?;
        Ok(Some(command))
    }

    pub(crate) fn complete_context_compaction(
        &self,
        agent_id: &str,
        request_id: &str,
        retained_context_tokens: u32,
        completed_at_ms: u64,
    ) -> Result<(), String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        let mut record = {
            let compactions = write
                .open_table(CONTEXT_COMPACTIONS)
                .map_err(|error| error.to_string())?;
            let value = compactions
                .get(agent_id)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("no pending compaction for {agent_id}"))?;
            serde_json::from_slice::<ContextCompactionRecord>(value.value())
                .map_err(|error| error.to_string())?
        };
        if record.request_id != request_id {
            return Err(format!("stale compaction acknowledgement {request_id}"));
        }
        if retained_context_tokens <= record.target_tokens {
            write
                .open_table(CONTEXT_COMPACTIONS)
                .map_err(|error| error.to_string())?
                .remove(agent_id)
                .map_err(|error| error.to_string())?;
            write.commit().map_err(|error| error.to_string())?;
            return Ok(());
        }
        record.completed_at_ms = Some(completed_at_ms);
        let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        write
            .open_table(CONTEXT_COMPACTIONS)
            .map_err(|error| error.to_string())?
            .insert(agent_id, bytes.as_slice())
            .map_err(|error| error.to_string())?;
        write.commit().map_err(|error| error.to_string())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_reminder(
        &self,
        id: &str,
        source_event_id: &str,
        conversation_id: &str,
        turn: u64,
        text: &str,
        created_at_ms: u64,
        due_at_ms: u64,
    ) -> Result<ReminderInfo, String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin reminder transaction: {error}"))?;
        {
            let mut reminders = write
                .open_table(REMINDERS)
                .map_err(|error| format!("open reminders: {error}"))?;
            if let Some(value) = reminders
                .get(id)
                .map_err(|error| format!("read reminder {id}: {error}"))?
            {
                let record: ReminderRecord = serde_json::from_slice(value.value())
                    .map_err(|error| format!("decode reminder {id}: {error}"))?;
                if record.source_event_id != source_event_id {
                    return Err(format!("reminder id {id} belongs to another request"));
                }
                return Ok(record.info);
            }
            let info = ReminderInfo {
                id: id.to_string(),
                conversation_id: conversation_id.to_string(),
                turn,
                text: text.to_string(),
                created_at_ms,
                due_at_ms,
                status: ReminderStatus::Pending,
            };
            let bytes = serde_json::to_vec(&ReminderRecord {
                schema_version: 1,
                source_event_id: source_event_id.to_string(),
                info: info.clone(),
                delivery_attempts: 0,
                delivery_started_at_ms: None,
                delivered_at_ms: None,
            })
            .map_err(|error| format!("encode reminder {id}: {error}"))?;
            reminders
                .insert(id, bytes.as_slice())
                .map_err(|error| format!("write reminder {id}: {error}"))?;
            drop(reminders);
            write
                .commit()
                .map_err(|error| format!("commit reminder {id}: {error}"))?;
            Ok(info)
        }
    }

    pub(crate) fn active_reminders(&self) -> Result<Vec<ReminderInfo>, String> {
        let read = self
            .database
            .begin_read()
            .map_err(|error| format!("begin reminder read: {error}"))?;
        let reminders = read
            .open_table(REMINDERS)
            .map_err(|error| format!("open reminders: {error}"))?;
        let mut active = Vec::new();
        for entry in reminders
            .iter()
            .map_err(|error| format!("iterate reminders: {error}"))?
        {
            let (_, value) = entry.map_err(|error| format!("read reminder entry: {error}"))?;
            let record: ReminderRecord = serde_json::from_slice(value.value())
                .map_err(|error| format!("decode reminder entry: {error}"))?;
            if matches!(
                record.info.status,
                ReminderStatus::Pending | ReminderStatus::Delivering
            ) {
                active.push(record.info);
            }
        }
        active.sort_by(|left, right| {
            left.due_at_ms
                .cmp(&right.due_at_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(active)
    }

    pub(crate) fn cancel_reminder(&self, id: &str) -> Result<ReminderInfo, String> {
        self.update_reminder(id, |record| {
            if record.info.status != ReminderStatus::Pending {
                return Err(format!(
                    "reminder {id} is already {}",
                    match record.info.status {
                        ReminderStatus::Delivering => "firing",
                        ReminderStatus::Delivered => "delivered",
                        ReminderStatus::Cancelled => "cancelled",
                        ReminderStatus::Pending => unreachable!(),
                    }
                ));
            }
            record.info.status = ReminderStatus::Cancelled;
            record.delivery_started_at_ms = None;
            Ok(())
        })
    }

    pub(crate) fn claim_due_reminders(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<ReminderInfo>, String> {
        const DELIVERY_RETRY_MS: u64 = 5_000;
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin due reminder transaction: {error}"))?;
        let mut claimed = Vec::new();
        {
            let mut reminders = write
                .open_table(REMINDERS)
                .map_err(|error| format!("open reminders: {error}"))?;
            let mut candidates = Vec::new();
            for entry in reminders
                .iter()
                .map_err(|error| format!("iterate reminders: {error}"))?
            {
                let (key, value) =
                    entry.map_err(|error| format!("read reminder entry: {error}"))?;
                let record: ReminderRecord = serde_json::from_slice(value.value())
                    .map_err(|error| format!("decode reminder entry: {error}"))?;
                let retryable = record.info.status == ReminderStatus::Delivering
                    && record
                        .delivery_started_at_ms
                        .is_some_and(|started| started.saturating_add(DELIVERY_RETRY_MS) <= now_ms);
                if record.info.due_at_ms <= now_ms
                    && (record.info.status == ReminderStatus::Pending || retryable)
                {
                    candidates.push((key.value().to_string(), record));
                }
            }
            candidates.sort_by(|(_, left), (_, right)| {
                left.info
                    .due_at_ms
                    .cmp(&right.info.due_at_ms)
                    .then_with(|| left.info.id.cmp(&right.info.id))
            });
            for (id, mut record) in candidates.into_iter().take(limit) {
                record.info.status = ReminderStatus::Delivering;
                record.delivery_attempts = record.delivery_attempts.saturating_add(1);
                record.delivery_started_at_ms = Some(now_ms);
                let bytes = serde_json::to_vec(&record)
                    .map_err(|error| format!("encode reminder {id}: {error}"))?;
                reminders
                    .insert(id.as_str(), bytes.as_slice())
                    .map_err(|error| format!("claim reminder {id}: {error}"))?;
                claimed.push(record.info);
            }
        }
        write
            .commit()
            .map_err(|error| format!("commit due reminders: {error}"))?;
        Ok(claimed)
    }

    pub(crate) fn release_reminder_delivery(&self, id: &str) -> Result<ReminderInfo, String> {
        self.update_reminder(id, |record| {
            if record.info.status == ReminderStatus::Delivering {
                record.info.status = ReminderStatus::Pending;
                record.delivery_started_at_ms = None;
            }
            Ok(())
        })
    }

    pub(crate) fn acknowledge_reminder_delivery(
        &self,
        id: &str,
        delivered_at_ms: u64,
    ) -> Result<ReminderInfo, String> {
        self.update_reminder(id, |record| {
            if record.info.status != ReminderStatus::Delivering {
                return Err(format!("reminder {id} is not awaiting delivery"));
            }
            record.info.status = ReminderStatus::Delivered;
            record.delivered_at_ms = Some(delivered_at_ms);
            record.delivery_started_at_ms = None;
            Ok(())
        })
    }

    fn update_reminder(
        &self,
        id: &str,
        update: impl FnOnce(&mut ReminderRecord) -> Result<(), String>,
    ) -> Result<ReminderInfo, String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin reminder update: {error}"))?;
        let info = {
            let mut reminders = write
                .open_table(REMINDERS)
                .map_err(|error| format!("open reminders: {error}"))?;
            let value = reminders
                .get(id)
                .map_err(|error| format!("read reminder {id}: {error}"))?
                .ok_or_else(|| format!("no such reminder: {id}"))?;
            let mut record: ReminderRecord = serde_json::from_slice(value.value())
                .map_err(|error| format!("decode reminder {id}: {error}"))?;
            drop(value);
            update(&mut record)?;
            let bytes = serde_json::to_vec(&record)
                .map_err(|error| format!("encode reminder {id}: {error}"))?;
            reminders
                .insert(id, bytes.as_slice())
                .map_err(|error| format!("write reminder {id}: {error}"))?;
            record.info
        };
        write
            .commit()
            .map_err(|error| format!("commit reminder {id}: {error}"))?;
        Ok(info)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_scheduled_task(
        &self,
        id: &str,
        source_event_id: &str,
        conversation_id: &str,
        turn: u64,
        objective: &str,
        mode: ScheduledTaskMode,
        created_at_ms: u64,
        due_at_ms: u64,
    ) -> Result<ScheduledTaskInfo, String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        let info = {
            let mut tasks = write
                .open_table(SCHEDULED_TASKS)
                .map_err(|error| format!("open scheduled tasks: {error}"))?;
            if let Some(value) = tasks.get(id).map_err(|error| error.to_string())? {
                let record: ScheduledTaskRecord = serde_json::from_slice(value.value())
                    .map_err(|error| format!("decode scheduled task {id}: {error}"))?;
                if record.source_event_id != source_event_id {
                    return Err(format!("scheduled task id {id} belongs to another request"));
                }
                return Ok(record.info);
            }
            let info = ScheduledTaskInfo {
                id: id.to_string(),
                conversation_id: conversation_id.to_string(),
                turn,
                objective: objective.to_string(),
                mode,
                created_at_ms,
                due_at_ms,
                status: ScheduledTaskStatus::Pending,
                work_id: None,
            };
            let bytes = serde_json::to_vec(&ScheduledTaskRecord {
                schema_version: 1,
                source_event_id: source_event_id.to_string(),
                info: info.clone(),
                result: None,
                delivery_started_at_ms: None,
                result_delivered: false,
            })
            .map_err(|error| format!("encode scheduled task {id}: {error}"))?;
            tasks
                .insert(id, bytes.as_slice())
                .map_err(|error| format!("write scheduled task {id}: {error}"))?;
            info
        };
        write.commit().map_err(|error| error.to_string())?;
        Ok(info)
    }

    pub(crate) fn scheduled_tasks(&self) -> Result<Vec<ScheduledTaskInfo>, String> {
        let read = self
            .database
            .begin_read()
            .map_err(|error| error.to_string())?;
        let tasks = read
            .open_table(SCHEDULED_TASKS)
            .map_err(|error| format!("open scheduled tasks: {error}"))?;
        let mut schedules = Vec::new();
        for entry in tasks.iter().map_err(|error| error.to_string())? {
            let (_, value) = entry.map_err(|error| error.to_string())?;
            let record: ScheduledTaskRecord =
                serde_json::from_slice(value.value()).map_err(|error| error.to_string())?;
            if matches!(
                record.info.status,
                ScheduledTaskStatus::Pending | ScheduledTaskStatus::Running
            ) {
                schedules.push(record.info);
            }
        }
        schedules.sort_by(|left, right| {
            left.due_at_ms
                .cmp(&right.due_at_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(schedules)
    }

    pub(crate) fn recover_scheduled_tasks(&self) -> Result<(), String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        {
            let mut tasks = write
                .open_table(SCHEDULED_TASKS)
                .map_err(|error| format!("open scheduled tasks: {error}"))?;
            let mut recovered = Vec::new();
            for entry in tasks.iter().map_err(|error| error.to_string())? {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let mut record: ScheduledTaskRecord =
                    serde_json::from_slice(value.value()).map_err(|error| error.to_string())?;
                if record.info.status == ScheduledTaskStatus::Running && record.result.is_none() {
                    record.info.status = ScheduledTaskStatus::Pending;
                    recovered.push((key.value().to_string(), record));
                }
            }
            for (id, record) in recovered {
                let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
                tasks
                    .insert(id.as_str(), bytes.as_slice())
                    .map_err(|error| error.to_string())?;
            }
        }
        write.commit().map_err(|error| error.to_string())
    }

    pub(crate) fn claim_ready_scheduled_tasks(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<ScheduledTaskInfo>, String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        let mut claimed = Vec::new();
        {
            let mut tasks = write
                .open_table(SCHEDULED_TASKS)
                .map_err(|error| format!("open scheduled tasks: {error}"))?;
            let mut candidates = Vec::new();
            for entry in tasks.iter().map_err(|error| error.to_string())? {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let record: ScheduledTaskRecord =
                    serde_json::from_slice(value.value()).map_err(|error| error.to_string())?;
                let ready = record.info.status == ScheduledTaskStatus::Pending
                    && (record.info.mode == ScheduledTaskMode::FinishBy
                        || record.info.due_at_ms <= now_ms);
                if ready {
                    candidates.push((key.value().to_string(), record));
                }
            }
            candidates.sort_by_key(|(_, record)| record.info.due_at_ms);
            for (id, mut record) in candidates.into_iter().take(limit) {
                record.info.status = ScheduledTaskStatus::Running;
                record.info.work_id = Some(format!("scheduled-work-{id}"));
                let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
                tasks
                    .insert(id.as_str(), bytes.as_slice())
                    .map_err(|error| error.to_string())?;
                claimed.push(record.info);
            }
        }
        write.commit().map_err(|error| error.to_string())?;
        Ok(claimed)
    }

    pub(crate) fn store_scheduled_task_result(
        &self,
        id: &str,
        result: &str,
        failed: bool,
    ) -> Result<ScheduledTaskInfo, String> {
        self.update_scheduled_task(id, |record| {
            record.result = Some(result.to_string());
            if failed {
                record.info.status = ScheduledTaskStatus::Failed;
            }
            Ok(())
        })
    }

    pub(crate) fn claim_scheduled_task_notifications(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<(ScheduledTaskInfo, String)>, String> {
        const DELIVERY_RETRY_MS: u64 = 5_000;
        let write = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        let mut claimed = Vec::new();
        {
            let mut tasks = write
                .open_table(SCHEDULED_TASKS)
                .map_err(|error| format!("open scheduled tasks: {error}"))?;
            let mut candidates = Vec::new();
            for entry in tasks.iter().map_err(|error| error.to_string())? {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let record: ScheduledTaskRecord =
                    serde_json::from_slice(value.value()).map_err(|error| error.to_string())?;
                let retryable = record
                    .delivery_started_at_ms
                    .is_none_or(|started| started.saturating_add(DELIVERY_RETRY_MS) <= now_ms);
                if record.result.is_some()
                    && !record.result_delivered
                    && matches!(
                        record.info.status,
                        ScheduledTaskStatus::Running | ScheduledTaskStatus::Failed
                    )
                    && retryable
                {
                    candidates.push((key.value().to_string(), record));
                }
            }
            candidates.sort_by_key(|(_, record)| record.info.due_at_ms);
            for (id, mut record) in candidates.into_iter().take(limit) {
                record.delivery_started_at_ms = Some(now_ms);
                let result = record.result.clone().unwrap_or_default();
                let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
                tasks
                    .insert(id.as_str(), bytes.as_slice())
                    .map_err(|error| error.to_string())?;
                claimed.push((record.info, result));
            }
        }
        write.commit().map_err(|error| error.to_string())?;
        Ok(claimed)
    }

    pub(crate) fn acknowledge_scheduled_task_notification(
        &self,
        id: &str,
    ) -> Result<ScheduledTaskInfo, String> {
        self.update_scheduled_task(id, |record| {
            if record.info.status == ScheduledTaskStatus::Running {
                record.info.status = ScheduledTaskStatus::Completed;
            }
            record.delivery_started_at_ms = None;
            record.result_delivered = true;
            Ok(())
        })
    }

    fn update_scheduled_task(
        &self,
        id: &str,
        update: impl FnOnce(&mut ScheduledTaskRecord) -> Result<(), String>,
    ) -> Result<ScheduledTaskInfo, String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        let info = {
            let mut tasks = write
                .open_table(SCHEDULED_TASKS)
                .map_err(|error| format!("open scheduled tasks: {error}"))?;
            let value = tasks
                .get(id)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("no such scheduled task: {id}"))?;
            let mut record: ScheduledTaskRecord =
                serde_json::from_slice(value.value()).map_err(|error| error.to_string())?;
            drop(value);
            update(&mut record)?;
            let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            tasks
                .insert(id, bytes.as_slice())
                .map_err(|error| error.to_string())?;
            record.info
        };
        write.commit().map_err(|error| error.to_string())?;
        Ok(info)
    }

    #[allow(dead_code)]
    pub(crate) fn mark_migration(
        &self,
        migration: &str,
        completed_at_ms: u64,
    ) -> Result<(), String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin runtime migration transaction: {error}"))?;
        {
            let mut migrations = write
                .open_table(MIGRATIONS)
                .map_err(|error| format!("open runtime migrations: {error}"))?;
            migrations
                .insert(migration, completed_at_ms)
                .map_err(|error| format!("mark runtime migration {migration}: {error}"))?;
        }
        write
            .commit()
            .map_err(|error| format!("commit runtime migration {migration}: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::types::{AgentState, LifetimeClass};

    fn task(id: &str, state: AgentState) -> RuntimeTaskRecord {
        RuntimeTaskRecord {
            schema_version: SCHEMA_VERSION as u32,
            updated_at_ms: 123,
            info: AgentInfo {
                id: id.into(),
                task: "test task".into(),
                state,
                pid: None,
                workspace: "/tmp/test".into(),
                created_secs: 1,
                retained: false,
                lease_until_secs: None,
                session_id: id.into(),
                lifetime_class: LifetimeClass::Short,
                purpose: "test".into(),
                owner: "tachyond".into(),
                last_activity_secs: 1,
                checkpoint_available: false,
                turns_used: 0,
                turn_budget: Some(3),
                task_type: "test".into(),
                description: String::new(),
                persistent: false,
                sandboxed: false,
                stage_until_secs: None,
                logical_task_id: None,
                origin_turn_id: None,
                parent_task_id: None,
                tool_call_id: None,
            },
            depends_on: Vec::new(),
            generation: 1,
            assignment: 1,
            warm: false,
            ready: true,
            owner: None,
            last_used_secs: 1,
            control_socket: None,
            terminal_usage: None,
            terminal_result: None,
        }
    }

    fn history(event_id: &str) -> HistoryProjection {
        HistoryProjection {
            schema_version: 1,
            event_id: event_id.into(),
            kind: HistoryKind::Conversation,
            conversation_id: "conversation-1".into(),
            turn_id: Some("1".into()),
            occurred_at_ms: 123,
            role: HistoryRole::User,
            text: "hello".into(),
            task_id: None,
            task_state: None,
        }
    }

    #[test]
    fn task_and_event_survive_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.redb");
        {
            let store = RuntimeStore::open(&path).unwrap();
            assert_eq!(
                store
                    .persist_task_transition(&task("a", AgentState::Running), "started", None)
                    .unwrap(),
                1
            );
        }
        let store = RuntimeStore::open(&path).unwrap();
        let read = store.database.begin_read().unwrap();
        let tasks = read.open_table(TASKS).unwrap();
        let bytes = tasks.get("a").unwrap().unwrap();
        let restored: RuntimeTaskRecord = serde_json::from_slice(bytes.value()).unwrap();
        assert_eq!(restored.info.id, "a");
        let events = read.open_table(TASK_EVENTS).unwrap();
        assert!(events.get(1).unwrap().is_some());
    }

    #[test]
    fn moved_runtime_database_reopens_blank() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.redb");
        {
            let store = RuntimeStore::open(&path).unwrap();
            store
                .persist_task_transition(&task("a", AgentState::Running), "started", None)
                .unwrap();
        }
        std::fs::rename(&path, directory.path().join("runtime.backup")).unwrap();
        let regenerated = RuntimeStore::open(&path).unwrap();
        assert!(regenerated.list_tasks().unwrap().is_empty());
        assert!(regenerated.pending_history().unwrap().is_empty());
    }

    #[test]
    fn command_id_makes_transition_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap();
        let first = store
            .persist_task_transition(&task("a", AgentState::Running), "started", Some("cmd-1"))
            .unwrap();
        let second = store
            .persist_task_transition(&task("a", AgentState::Completed), "done", Some("cmd-1"))
            .unwrap();
        assert_eq!(first, second);
        let read = store.database.begin_read().unwrap();
        let tasks = read.open_table(TASKS).unwrap();
        let bytes = tasks.get("a").unwrap().unwrap();
        let restored: RuntimeTaskRecord = serde_json::from_slice(bytes.value()).unwrap();
        assert_eq!(restored.info.state, AgentState::Running);
    }

    #[test]
    fn migration_markers_are_durable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.redb");
        RuntimeStore::open(&path)
            .unwrap()
            .mark_migration("markdown_tasks_v1", 456)
            .unwrap();
        let store = RuntimeStore::open(&path).unwrap();
        let read = store.database.begin_read().unwrap();
        let migrations = read.open_table(MIGRATIONS).unwrap();
        assert_eq!(
            migrations
                .get("markdown_tasks_v1")
                .unwrap()
                .map(|value| value.value()),
            Some(456)
        );
    }

    #[test]
    fn task_listing_restores_versioned_records() {
        let directory = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap();
        store
            .persist_task_transition(&task("b", AgentState::Waiting), "waiting", None)
            .unwrap();
        store
            .persist_task_transition(&task("a", AgentState::Running), "started", None)
            .unwrap();
        let mut records = store.list_tasks().unwrap();
        records.sort_by(|left, right| left.info.id.cmp(&right.info.id));
        assert_eq!(
            records
                .iter()
                .map(|record| record.info.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn history_outbox_replays_until_acknowledged() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.redb");
        {
            let store = RuntimeStore::open(&path).unwrap();
            store.enqueue_history(&history("event-1")).unwrap();
            store.enqueue_history(&history("event-1")).unwrap();
            assert_eq!(store.pending_history().unwrap(), [history("event-1")]);
        }
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(store.pending_history().unwrap(), [history("event-1")]);
        store.acknowledge_history("event-1", 456).unwrap();
        assert!(store.pending_history().unwrap().is_empty());
        store.enqueue_history(&history("event-1")).unwrap();
        assert!(store.pending_history().unwrap().is_empty());
    }

    #[test]
    fn task_transition_enqueues_chronological_history() {
        let directory = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap();
        store
            .persist_task_transition(&task("a", AgentState::Running), "started", None)
            .unwrap();

        let pending = store.pending_history().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].kind, HistoryKind::Task);
        assert_eq!(pending[0].task_id.as_deref(), Some("a"));
        assert_eq!(pending[0].task_state.as_deref(), Some("running"));
    }

    #[test]
    fn context_compaction_uses_threshold_and_hysteresis() {
        let directory = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap();
        assert!(store
            .observe_context_usage("foreground", 0, 0, 1, 649, 1_000, 100)
            .unwrap()
            .is_none());
        let first = store
            .observe_context_usage("foreground", 0, 0, 2, 650, 1_000, 101)
            .unwrap()
            .unwrap();
        assert_eq!(first.epoch, 1);
        assert_eq!(first.target_tokens, 450);
        assert!(store
            .observe_context_usage("foreground", 0, 0, 3, 700, 1_000, 103)
            .unwrap()
            .is_none());
        store
            .complete_context_compaction("foreground", &first.request_id, 450, 104)
            .unwrap();
        let second = store
            .observe_context_usage("foreground", 0, 0, 5, 700, 1_000, 105)
            .unwrap()
            .unwrap();
        assert_eq!(second.epoch, 2);
    }

    #[test]
    fn reminders_are_durable_claimed_acknowledged_and_cancelled() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.redb");
        {
            let store = RuntimeStore::open(&path).unwrap();
            let reminder = store
                .create_reminder(
                    "reminder-1",
                    "source-1",
                    "foreground",
                    1,
                    "Your coffee is ready.",
                    100,
                    200,
                )
                .unwrap();
            assert_eq!(reminder.status, ReminderStatus::Pending);
            assert!(store.claim_due_reminders(199, 10).unwrap().is_empty());
        }
        let store = RuntimeStore::open(&path).unwrap();
        let claimed = store.claim_due_reminders(200, 10).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, ReminderStatus::Delivering);
        assert!(store.claim_due_reminders(201, 10).unwrap().is_empty());
        let delivered = store
            .acknowledge_reminder_delivery("reminder-1", 202)
            .unwrap();
        assert_eq!(delivered.status, ReminderStatus::Delivered);
        assert!(store.active_reminders().unwrap().is_empty());

        store
            .create_reminder(
                "reminder-2",
                "source-2",
                "foreground",
                2,
                "Second reminder.",
                300,
                400,
            )
            .unwrap();
        let cancelled = store.cancel_reminder("reminder-2").unwrap();
        assert_eq!(cancelled.status, ReminderStatus::Cancelled);
        assert!(store.claim_due_reminders(500, 10).unwrap().is_empty());
    }

    #[test]
    fn scheduled_tasks_preserve_start_and_finish_deadline_semantics() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        store
            .create_scheduled_task(
                "scheduled-task-start",
                "source-start",
                "foreground",
                1,
                "check weather",
                ScheduledTaskMode::StartAt,
                100,
                500,
            )
            .unwrap();

        store
            .create_scheduled_task(
                "scheduled-task-finish",
                "source-finish",
                "foreground",
                2,
                "prepare forecast",
                ScheduledTaskMode::FinishBy,
                100,
                500,
            )
            .unwrap();

        let listed = store.scheduled_tasks().unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().any(|task| task.objective == "check weather"));

        let claimed = store.claim_ready_scheduled_tasks(200, 10).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].mode, ScheduledTaskMode::FinishBy);
        assert!(store
            .claim_ready_scheduled_tasks(499, 10)
            .unwrap()
            .is_empty());
        drop(store);

        let store = RuntimeStore::open(&path).unwrap();
        store.recover_scheduled_tasks().unwrap();
        let mut claimed = store.claim_ready_scheduled_tasks(500, 10).unwrap();
        claimed.sort_by(|left, right| left.id.cmp(&right.id));
        assert_eq!(claimed.len(), 2);
        let finish = claimed
            .iter()
            .find(|task| task.mode == ScheduledTaskMode::FinishBy)
            .unwrap();
        store
            .store_scheduled_task_result(&finish.id, "forecast ready", false)
            .unwrap();
        let notifications = store.claim_scheduled_task_notifications(501, 10).unwrap();
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].1, "forecast ready");
        let delivered = store
            .acknowledge_scheduled_task_notification(&finish.id)
            .unwrap();
        assert_eq!(delivered.status, ScheduledTaskStatus::Completed);
        let active = store.scheduled_tasks().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].mode, ScheduledTaskMode::StartAt);
        assert!(store
            .claim_scheduled_task_notifications(10_000, 10)
            .unwrap()
            .is_empty());
    }
}
