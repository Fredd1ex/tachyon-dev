use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tachyon_api::types::{AgentInfo, HistoryRole};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HistoryProjection {
    pub schema_version: u32,
    pub event_id: String,
    pub conversation_id: String,
    pub turn_id: Option<String>,
    pub occurred_at_ms: u64,
    pub role: HistoryRole,
    pub text: String,
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

pub(crate) struct RuntimeStore {
    database: Database,
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
        let store = Self { database };
        store.initialize()?;
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
            conversation_id: "conversation-1".into(),
            turn_id: Some("1".into()),
            occurred_at_ms: 123,
            role: HistoryRole::User,
            text: "hello".into(),
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
}
