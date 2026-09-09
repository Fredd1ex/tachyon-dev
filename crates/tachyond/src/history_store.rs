use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tachyon_api::types::{HistoryEntry, HistoryKind, HistoryRole};

use crate::runtime_store::HistoryProjection;

const SCHEMA_VERSION: u64 = 1;
const METADATA: TableDefinition<&str, u64> = TableDefinition::new("metadata");
const CONVERSATIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("conversations");
const MESSAGES: TableDefinition<&str, &[u8]> = TableDefinition::new("messages");
const MESSAGES_BY_CONVERSATION: TableDefinition<&str, &str> =
    TableDefinition::new("messages_by_conversation");
const ACTIVITY_BY_TIME: TableDefinition<&str, &str> = TableDefinition::new("activity_by_time");
const ACTIVITY_BY_DAY: TableDefinition<&str, &str> = TableDefinition::new("activity_by_day");
const SUMMARY_INTERVAL: u64 = 20;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ConversationRecord {
    schema_version: u32,
    conversation_id: String,
    started_at_ms: u64,
    updated_at_ms: u64,
    message_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HistoryMessage {
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

pub(crate) struct HistoryStore {
    database: Database,
}

impl HistoryStore {
    pub(crate) fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "create history database directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        let database = Database::create(path)
            .map_err(|error| format!("open history database {}: {error}", path.display()))?;
        let store = Self { database };
        store.initialize()?;
        Ok(store)
    }

    fn initialize(&self) -> Result<(), String> {
        let write = self
            .database
            .begin_write()
            .map_err(|error| format!("begin history schema transaction: {error}"))?;
        {
            let mut metadata = write
                .open_table(METADATA)
                .map_err(|error| format!("open history metadata: {error}"))?;
            let found = metadata
                .get("schema_version")
                .map_err(|error| format!("read history schema version: {error}"))?
                .map(|value| value.value());
            match found {
                Some(SCHEMA_VERSION) => {}
                Some(version) => {
                    return Err(format!(
                        "unsupported history database schema {version}; expected {SCHEMA_VERSION}"
                    ));
                }
                None => {
                    metadata
                        .insert("schema_version", SCHEMA_VERSION)
                        .map_err(|error| format!("initialize history schema: {error}"))?;
                }
            }
            write
                .open_table(CONVERSATIONS)
                .map_err(|error| error.to_string())?;
            write
                .open_table(MESSAGES)
                .map_err(|error| error.to_string())?;
            write
                .open_table(MESSAGES_BY_CONVERSATION)
                .map_err(|error| error.to_string())?;
            write
                .open_table(ACTIVITY_BY_TIME)
                .map_err(|error| error.to_string())?;
            write
                .open_table(ACTIVITY_BY_DAY)
                .map_err(|error| error.to_string())?;
        }
        write
            .commit()
            .map_err(|error| format!("commit history schema: {error}"))
    }

    pub(crate) fn apply(&self, projection: &HistoryProjection) -> Result<bool, String> {
        let message = HistoryMessage {
            schema_version: SCHEMA_VERSION as u32,
            event_id: projection.event_id.clone(),
            kind: projection.kind,
            conversation_id: projection.conversation_id.clone(),
            turn_id: projection.turn_id.clone(),
            occurred_at_ms: projection.occurred_at_ms,
            role: projection.role.clone(),
            text: projection.text.clone(),
            task_id: projection.task_id.clone(),
            task_state: projection.task_state.clone(),
        };
        let message_bytes = serde_json::to_vec(&message)
            .map_err(|error| format!("encode history message: {error}"))?;
        let write = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        let exists = {
            let messages = write
                .open_table(MESSAGES)
                .map_err(|error| error.to_string())?;
            let exists = messages
                .get(projection.event_id.as_str())
                .map_err(|error| error.to_string())?
                .is_some();
            exists
        };
        if exists {
            return Ok(false);
        }

        let conversation = (projection.kind == HistoryKind::Conversation)
            .then(|| {
                let conversations = write
                    .open_table(CONVERSATIONS)
                    .map_err(|error| error.to_string())?;
                let existing = conversations
                    .get(projection.conversation_id.as_str())
                    .map_err(|error| error.to_string())?;
                let conversation = if let Some(existing) = existing {
                    let mut record: ConversationRecord =
                        serde_json::from_slice(existing.value())
                            .map_err(|error| format!("decode conversation: {error}"))?;
                    record.updated_at_ms = record.updated_at_ms.max(projection.occurred_at_ms);
                    record.started_at_ms = record.started_at_ms.min(projection.occurred_at_ms);
                    record.message_count = record.message_count.saturating_add(1);
                    record
                } else {
                    ConversationRecord {
                        schema_version: SCHEMA_VERSION as u32,
                        conversation_id: projection.conversation_id.clone(),
                        started_at_ms: projection.occurred_at_ms,
                        updated_at_ms: projection.occurred_at_ms,
                        message_count: 1,
                    }
                };
                Ok::<_, String>(conversation)
            })
            .transpose()?;
        let temporal_key = temporal_key(projection.occurred_at_ms, &projection.event_id);
        let day_key = day_key(projection.occurred_at_ms, &projection.event_id);
        write
            .open_table(MESSAGES)
            .map_err(|error| error.to_string())?
            .insert(projection.event_id.as_str(), message_bytes.as_slice())
            .map_err(|error| error.to_string())?;
        if let Some(conversation) = conversation.as_ref() {
            let conversation_bytes = serde_json::to_vec(&conversation)
                .map_err(|error| format!("encode conversation: {error}"))?;
            write
                .open_table(CONVERSATIONS)
                .map_err(|error| error.to_string())?
                .insert(
                    projection.conversation_id.as_str(),
                    conversation_bytes.as_slice(),
                )
                .map_err(|error| error.to_string())?;
        }
        if projection.kind != HistoryKind::Task {
            let conversation_key = format!(
                "{}:{:020}:{}",
                projection.conversation_id, projection.occurred_at_ms, projection.event_id
            );
            write
                .open_table(MESSAGES_BY_CONVERSATION)
                .map_err(|error| error.to_string())?
                .insert(conversation_key.as_str(), projection.event_id.as_str())
                .map_err(|error| error.to_string())?;
        }
        write
            .open_table(ACTIVITY_BY_TIME)
            .map_err(|error| error.to_string())?
            .insert(temporal_key.as_str(), projection.event_id.as_str())
            .map_err(|error| error.to_string())?;
        write
            .open_table(ACTIVITY_BY_DAY)
            .map_err(|error| error.to_string())?
            .insert(day_key.as_str(), projection.event_id.as_str())
            .map_err(|error| error.to_string())?;
        write
            .commit()
            .map_err(|error| format!("commit history message: {error}"))?;
        if let Some(conversation) =
            conversation.filter(|conversation| conversation.message_count % SUMMARY_INTERVAL == 0)
        {
            let messages = self.recent_conversation_messages(
                &projection.conversation_id,
                SUMMARY_INTERVAL as usize,
            )?;
            let summary = interval_summary(&messages);
            self.apply(&HistoryProjection {
                schema_version: SCHEMA_VERSION as u32,
                event_id: format!(
                    "conversation-summary-{}-{:020}",
                    projection.conversation_id, conversation.message_count
                ),
                kind: HistoryKind::ConversationSummary,
                conversation_id: projection.conversation_id.clone(),
                turn_id: projection.turn_id.clone(),
                occurred_at_ms: projection.occurred_at_ms,
                role: HistoryRole::Notification,
                text: summary,
                task_id: None,
                task_state: None,
            })?;
        }
        Ok(true)
    }

    fn recent_conversation_messages(
        &self,
        conversation_id: &str,
        limit: usize,
    ) -> Result<Vec<HistoryMessage>, String> {
        let read = self
            .database
            .begin_read()
            .map_err(|error| error.to_string())?;
        let index = read
            .open_table(MESSAGES_BY_CONVERSATION)
            .map_err(|error| error.to_string())?;
        let messages = read
            .open_table(MESSAGES)
            .map_err(|error| error.to_string())?;
        let start = format!("{conversation_id}:");
        let end = format!("{conversation_id};");
        let mut result = Vec::new();
        for entry in index
            .range(start.as_str()..end.as_str())
            .map_err(|error| error.to_string())?
            .rev()
        {
            let (_, event_id) = entry.map_err(|error| error.to_string())?;
            let value = messages
                .get(event_id.value())
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "conversation index references a missing message".to_string())?;
            let message: HistoryMessage = serde_json::from_slice(value.value())
                .map_err(|error| format!("decode history message: {error}"))?;
            if message.kind == HistoryKind::Conversation {
                result.push(message);
                if result.len() == limit {
                    break;
                }
            }
        }
        result.reverse();
        Ok(result)
    }

    pub(crate) fn activity_between(
        &self,
        start_ms: u64,
        end_ms: u64,
        limit: usize,
    ) -> Result<Vec<HistoryEntry>, String> {
        let read = self
            .database
            .begin_read()
            .map_err(|error| error.to_string())?;
        let index = read
            .open_table(ACTIVITY_BY_TIME)
            .map_err(|error| error.to_string())?;
        let messages = read
            .open_table(MESSAGES)
            .map_err(|error| error.to_string())?;
        let start = temporal_key(start_ms, "");
        let end = temporal_key(end_ms, "");
        let entries = index
            .range(start.as_str()..end.as_str())
            .map_err(|error| error.to_string())?;
        let mut result = Vec::new();
        for entry in entries {
            let (_, event_id) = entry.map_err(|error| error.to_string())?;
            let value = messages
                .get(event_id.value())
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "history index references a missing message".to_string())?;
            let message: HistoryMessage = serde_json::from_slice(value.value())
                .map_err(|error| format!("decode history message: {error}"))?;
            result.push(HistoryEntry {
                event_id: message.event_id,
                kind: message.kind,
                conversation_id: message.conversation_id,
                turn_id: message.turn_id,
                occurred_at_ms: message.occurred_at_ms,
                role: message.role,
                text: message.text,
                task_id: message.task_id,
                task_state: message.task_state,
            });
            if result.len() >= limit {
                break;
            }
        }
        Ok(result)
    }

    pub(crate) fn recall_between(
        &self,
        start_ms: u64,
        end_ms: u64,
        query: &str,
        limit: usize,
    ) -> Result<Vec<HistoryEntry>, String> {
        let read = self
            .database
            .begin_read()
            .map_err(|error| error.to_string())?;
        let index = read
            .open_table(ACTIVITY_BY_TIME)
            .map_err(|error| error.to_string())?;
        let messages = read
            .open_table(MESSAGES)
            .map_err(|error| error.to_string())?;
        let start = temporal_key(start_ms, "");
        let end = temporal_key(end_ms, "");
        let query_tokens = search_tokens(query);
        let mut ranked = Vec::new();
        for entry in index
            .range(start.as_str()..end.as_str())
            .map_err(|error| error.to_string())?
            .rev()
            .take(200)
        {
            let (_, event_id) = entry.map_err(|error| error.to_string())?;
            let value = messages
                .get(event_id.value())
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "history index references a missing message".to_string())?;
            let message: HistoryMessage = serde_json::from_slice(value.value())
                .map_err(|error| format!("decode history message: {error}"))?;
            if message.text.trim().eq_ignore_ascii_case(query.trim()) {
                continue;
            }
            let candidate_tokens = search_tokens(&message.text);
            let score = query_tokens
                .iter()
                .filter(|token| candidate_tokens.contains(*token))
                .count();
            ranked.push((score, message));
        }
        ranked.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .cmp(left_score)
                .then_with(|| right.occurred_at_ms.cmp(&left.occurred_at_ms))
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        Ok(ranked
            .into_iter()
            .take(limit)
            .map(|(_, message)| HistoryEntry {
                event_id: message.event_id,
                kind: message.kind,
                conversation_id: message.conversation_id,
                turn_id: message.turn_id,
                occurred_at_ms: message.occurred_at_ms,
                role: message.role,
                text: message.text,
                task_id: message.task_id,
                task_state: message.task_state,
            })
            .collect())
    }
}

fn temporal_key(occurred_at_ms: u64, event_id: &str) -> String {
    format!("{occurred_at_ms:020}:{event_id}")
}

fn day_key(occurred_at_ms: u64, event_id: &str) -> String {
    format!("{:010}:{event_id}", occurred_at_ms / 86_400_000)
}

fn interval_summary(messages: &[HistoryMessage]) -> String {
    const MAX_SUMMARY_BYTES: usize = 4_000;
    let mut summary = format!(
        "Conversation interval summary ({} messages):",
        messages.len()
    );
    for message in messages {
        let role = match message.role {
            HistoryRole::User => "User",
            HistoryRole::Assistant => "Assistant",
            HistoryRole::Notification => "System",
        };
        let text = message
            .text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let remaining = MAX_SUMMARY_BYTES.saturating_sub(summary.len());
        if remaining <= role.len() + 4 {
            break;
        }
        let text = text
            .chars()
            .take(remaining - role.len() - 4)
            .collect::<String>();
        summary.push_str("\n- ");
        summary.push_str(role);
        summary.push_str(": ");
        summary.push_str(&text);
    }
    summary
}

fn search_tokens(text: &str) -> std::collections::BTreeSet<String> {
    const STOP_WORDS: [&str; 19] = [
        "about", "after", "before", "could", "doing", "from", "have", "history", "last", "past",
        "please", "remember", "that", "this", "what", "when", "were", "with", "worked",
    ];
    text.split(|character: char| !character.is_ascii_alphanumeric())
        .map(str::to_ascii_lowercase)
        .filter(|token| token.len() > 2 && !STOP_WORDS.contains(&token.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection(event_id: &str, occurred_at_ms: u64, text: &str) -> HistoryProjection {
        HistoryProjection {
            schema_version: 1,
            event_id: event_id.into(),
            kind: HistoryKind::Conversation,
            conversation_id: "conversation-1".into(),
            turn_id: Some("1".into()),
            occurred_at_ms,
            role: HistoryRole::User,
            text: text.into(),
            task_id: None,
            task_state: None,
        }
    }

    #[test]
    fn projection_is_idempotent_and_survives_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.redb");
        {
            let store = HistoryStore::open(&path).unwrap();
            assert!(store.apply(&projection("event-1", 100, "hello")).unwrap());
            assert!(!store.apply(&projection("event-1", 100, "changed")).unwrap());
        }
        let store = HistoryStore::open(&path).unwrap();
        let records = store.activity_between(0, 200, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].text, "hello");
    }

    #[test]
    fn temporal_index_returns_only_requested_window() {
        let directory = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(&directory.path().join("history.redb")).unwrap();
        store.apply(&projection("before", 99, "before")).unwrap();
        store.apply(&projection("inside", 100, "inside")).unwrap();
        store.apply(&projection("after", 200, "after")).unwrap();
        let records = store.activity_between(100, 200, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].text, "inside");
    }

    #[test]
    fn recall_ranks_matching_recent_history_and_excludes_the_query() {
        let directory = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(&directory.path().join("history.redb")).unwrap();
        store
            .apply(&projection(
                "rust",
                100,
                "Implemented Rust memory retrieval",
            ))
            .unwrap();
        store
            .apply(&projection("other", 200, "Discussed the weather"))
            .unwrap();
        store
            .apply(&projection("query", 300, "What did we do on Rust memory?"))
            .unwrap();
        let records = store
            .recall_between(0, 400, "What did we do on Rust memory?", 2)
            .unwrap();
        assert_eq!(records[0].event_id, "rust");
        assert!(records.iter().all(|entry| entry.event_id != "query"));
    }

    #[test]
    fn writes_a_deterministic_summary_every_twenty_messages() {
        let directory = tempfile::tempdir().unwrap();
        let store = HistoryStore::open(&directory.path().join("history.redb")).unwrap();
        for sequence in 1..=20 {
            store
                .apply(&projection(
                    &format!("event-{sequence}"),
                    sequence,
                    &format!("message {sequence}"),
                ))
                .unwrap();
        }

        let records = store.activity_between(0, 21, 30).unwrap();
        assert_eq!(records.len(), 21);
        let summary = records
            .iter()
            .find(|entry| entry.kind == HistoryKind::ConversationSummary)
            .unwrap();
        assert_eq!(
            summary.event_id,
            "conversation-summary-conversation-1-00000000000000000020"
        );
        assert!(summary.text.contains("message 1"));
        assert!(summary.text.contains("message 20"));
    }

    #[test]
    fn deleted_history_database_reopens_blank() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.redb");
        {
            let store = HistoryStore::open(&path).unwrap();
            store.apply(&projection("event", 100, "saved")).unwrap();
        }
        std::fs::remove_file(&path).unwrap();
        let regenerated = HistoryStore::open(&path).unwrap();
        assert!(regenerated.activity_between(0, 200, 10).unwrap().is_empty());
    }
}
