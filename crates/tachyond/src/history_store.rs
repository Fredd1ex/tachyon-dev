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
const COMMANDS: TableDefinition<&str, &[u8]> = TableDefinition::new("interaction_commands");
const ACCEPTED_TURNS: TableDefinition<&str, &str> =
    TableDefinition::new("interaction_accepted_turns");
const INTERACTION_PROJECTION: TableDefinition<&str, &[u8]> =
    TableDefinition::new("interaction_projection_v1");
const INTERACTION_CONTENT: TableDefinition<(&str, u64), &[u8]> =
    TableDefinition::new("interaction_content_v1");
const INTERACTION_FINAL_GENERATIONS: TableDefinition<&str, u64> =
    TableDefinition::new("interaction_final_generations_v1");

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention: Option<tachyon_api::attention::AttentionFrameMetadata>,
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
    pub(crate) fn remember_interaction_final(
        &self,
        event: &tachyon_api::InteractionEventEnvelope,
    ) -> Result<(), String> {
        if !matches!(
            event.event,
            tachyon_api::InteractionEvent::ConversationFinished { .. }
        ) {
            return Ok(());
        }
        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
        tx.open_table(INTERACTION_FINAL_GENERATIONS)
            .map_err(|e| e.to_string())?
            .insert(
                event.metadata.message_id.as_str(),
                event.metadata.generation,
            )
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }

    pub(crate) fn interaction_final_generations(
        &self,
        entries: &[HistoryEntry],
    ) -> Result<std::collections::BTreeMap<String, u64>, String> {
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        let table = tx
            .open_table(INTERACTION_FINAL_GENERATIONS)
            .map_err(|e| e.to_string())?;
        let mut generations = std::collections::BTreeMap::new();
        for entry in entries {
            if let Some(row) = table
                .get(entry.event_id.as_str())
                .map_err(|e| e.to_string())?
            {
                generations.insert(entry.event_id.clone(), row.value());
            }
        }
        Ok(generations)
    }
    pub(crate) fn interaction_checkpoint(
        &self,
    ) -> Result<tachyon_interaction_manager::Checkpoint, String> {
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        let table = tx
            .open_table(INTERACTION_PROJECTION)
            .map_err(|e| e.to_string())?;
        table
            .get("foreground")
            .map_err(|e| e.to_string())?
            .map(|row| serde_json::from_slice(row.value()).map_err(|e| e.to_string()))
            .transpose()
            .map(|value| value.unwrap_or_default())
    }

    pub(crate) fn save_interaction_checkpoint(
        &self,
        checkpoint: &tachyon_interaction_manager::Checkpoint,
    ) -> Result<(), String> {
        self.save_interaction_event(checkpoint, None)
    }

    pub(crate) fn save_interaction_event(
        &self,
        checkpoint: &tachyon_interaction_manager::Checkpoint,
        event: Option<&tachyon_api::InteractionEventEnvelope>,
    ) -> Result<(), String> {
        let bytes = serde_json::to_vec(checkpoint).map_err(|e| e.to_string())?;
        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
        if let Some(event) = event {
            use tachyon_api::InteractionEvent;
            if let Some(response) = checkpoint
                .projection
                .responses
                .iter()
                .find(|r| Some(&r.turn_id) == event.metadata.turn_id.as_ref())
            {
                let content = match &event.event {
                    InteractionEvent::ConversationDelta { text } => Some((
                        text,
                        response.answer_bytes.saturating_sub(text.len() as u64),
                        None,
                    )),
                    InteractionEvent::ConversationFinished { text } => {
                        Some((text, 0, Some(event.metadata.message_id.as_str())))
                    }
                    _ => None,
                };
                if let Some((text, offset, final_event)) = content {
                    let reference = tachyon_interaction_manager::answer_reference(
                        &response.turn_id,
                        response.generation,
                        final_event,
                    );
                    let mut table = tx
                        .open_table(INTERACTION_CONTENT)
                        .map_err(|e| e.to_string())?;
                    for (index, chunk) in text.as_bytes().chunks(65536).enumerate() {
                        table
                            .insert((reference.as_str(), offset + (index * 65536) as u64), chunk)
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
        }
        tx.open_table(INTERACTION_PROJECTION)
            .map_err(|e| e.to_string())?
            .insert("foreground", bytes.as_slice())
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }

    pub(crate) fn interaction_content(
        &self,
        reference: &str,
        offset: u64,
        limit: Option<usize>,
    ) -> Result<tachyon_api::interaction_manager::ContentPage, String> {
        let limit = limit.unwrap_or(65536);
        if reference.len() > 4096 || !(1..=65536).contains(&limit) {
            return Err("invalid content bounds".into());
        }
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        if let Some(event_id) = reference.strip_prefix("history:") {
            let table = tx.open_table(MESSAGES).map_err(|e| e.to_string())?;
            let row = table
                .get(event_id)
                .map_err(|e| e.to_string())?
                .ok_or("unknown history reference")?;
            let message: HistoryMessage =
                serde_json::from_slice(row.value()).map_err(|e| e.to_string())?;
            if offset > message.text.len() as u64 {
                return Err("content offset out of range".into());
            }
            let bytes: Vec<u8> = message
                .text
                .as_bytes()
                .iter()
                .skip(offset as usize)
                .take(limit)
                .copied()
                .collect();
            let next = offset + bytes.len() as u64;
            return Ok(tachyon_api::interaction_manager::ContentPage {
                reference: reference.into(),
                offset,
                bytes,
                next_offset: (next < message.text.len() as u64).then_some(next),
            });
        }
        let table = tx
            .open_table(INTERACTION_CONTENT)
            .map_err(|e| e.to_string())?;
        let last = table
            .range((reference, 0)..=(reference, u64::MAX))
            .map_err(|e| e.to_string())?
            .next_back()
            .transpose()
            .map_err(|e| e.to_string())?
            .ok_or("unknown content reference")?;
        let total = last.0.value().1 + last.1.value().len() as u64;
        if offset > total {
            return Err("content offset out of range".into());
        }
        let start = table
            .range((reference, 0)..=(reference, offset))
            .map_err(|e| e.to_string())?
            .next_back()
            .transpose()
            .map_err(|e| e.to_string())?
            .map(|(key, _)| key.value().1)
            .unwrap_or(0);
        let mut bytes = Vec::new();
        for row in table
            .range((reference, start)..=(reference, u64::MAX))
            .map_err(|e| e.to_string())?
        {
            let (key, chunk) = row.map_err(|e| e.to_string())?;
            let skip = offset.saturating_sub(key.value().1) as usize;
            bytes.extend(chunk.value().iter().skip(skip).take(limit - bytes.len()));
            if bytes.len() == limit {
                break;
            }
        }
        let next = offset + bytes.len() as u64;
        Ok(tachyon_api::interaction_manager::ContentPage {
            reference: reference.into(),
            offset,
            bytes,
            next_offset: (next < total).then_some(next),
        })
    }
    pub(crate) fn existing_command(
        &self,
        command: &tachyon_api::interaction_manager::Submit,
    ) -> Result<Option<tachyon_api::interaction_manager::Receipt>, String> {
        let read = self.database.begin_read().map_err(|e| e.to_string())?;
        let table = read.open_table(COMMANDS).map_err(|e| e.to_string())?;
        let key = serde_json::to_string(&(&command.session_id, &command.command_id))
            .map_err(|e| e.to_string())?;
        let receipt = table
            .get(key.as_str())
            .map_err(|e| e.to_string())?
            .map(|v| serde_json::from_slice::<tachyon_api::interaction_manager::Receipt>(v.value()))
            .transpose()
            .map_err(|e| e.to_string())?;
        if receipt.as_ref().is_some_and(|r| &r.command != command) {
            return Err("command ID reused with different payload".into());
        }
        Ok(receipt)
    }
    pub(crate) fn command_receipt(
        &self,
        command: &tachyon_api::interaction_manager::Submit,
        delivered: bool,
        origin: Option<&tachyon_api::interaction_manager::CommandOrigin>,
    ) -> Result<(tachyon_api::interaction_manager::Receipt, bool), String> {
        use tachyon_api::interaction_manager::{Admission, Receipt};
        if origin.is_some_and(|origin| {
            origin.session_id != command.session_id
                || origin.command_id != command.command_id
                || origin.host_message_id.is_empty()
        }) {
            return Err("invalid admitted command origin".into());
        }
        let write = self.database.begin_write().map_err(|e| e.to_string())?;
        let key = serde_json::to_string(&(&command.session_id, &command.command_id))
            .map_err(|e| e.to_string())?;
        let (receipt, fresh) = {
            let mut table = write.open_table(COMMANDS).map_err(|e| e.to_string())?;
            let previous = table
                .get(key.as_str())
                .map_err(|e| e.to_string())?
                .map(|v| serde_json::from_slice::<Receipt>(v.value()))
                .transpose()
                .map_err(|e| e.to_string())?;
            if previous.as_ref().is_some_and(|r| &r.command != command) {
                return Err("command ID reused with different payload".into());
            }
            let fresh = previous.is_none();
            let mut receipt = previous.unwrap_or(Receipt {
                command: command.clone(),
                admission: Admission::Uncertain,
                origin: origin.cloned(),
                accepted: None,
            });
            if delivered {
                receipt.admission = Admission::Delivered;
            }
            let bytes = serde_json::to_vec(&receipt).map_err(|e| e.to_string())?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| e.to_string())?;
            (receipt, fresh)
        };
        write.commit().map_err(|e| e.to_string())?;
        Ok((receipt, fresh))
    }

    /// Bind only a host acceptance authenticated against the original admission.
    /// History projection never writes this table or guesses by text/order.
    pub(crate) fn bind_accepted_turn(
        &self,
        event: &tachyon_api::InteractionEventEnvelope,
    ) -> Result<(), String> {
        use tachyon_api::interaction_manager::{AcceptedTurn, Receipt};
        let Some(origin) = &event.metadata.command_origin else {
            return Ok(());
        };
        let tachyon_api::InteractionEvent::UserTurnAccepted { text } = &event.event else {
            return Ok(());
        };
        let accepted = AcceptedTurn {
            turn_id: event
                .metadata
                .turn_id
                .clone()
                .ok_or("accepted turn missing identity")?,
            event_id: event.metadata.message_id.clone(),
        };
        let key = serde_json::to_string(&(&origin.session_id, &origin.command_id))
            .map_err(|e| e.to_string())?;
        let write = self.database.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write.open_table(COMMANDS).map_err(|e| e.to_string())?;
            let mut receipt: Receipt = {
                let value = table
                    .get(key.as_str())
                    .map_err(|e| e.to_string())?
                    .ok_or("acceptance has no admitted command")?;
                serde_json::from_slice(value.value()).map_err(|e| e.to_string())?
            };
            if receipt.origin.as_ref() != Some(origin)
                || receipt.command.text != *text
                || event.metadata.correlation_id != origin.command_id
                || event.metadata.causation_id.as_deref() != Some(&origin.host_message_id)
                || receipt.command.conversation_id != event.metadata.conversation_id
                || !accepted
                    .turn_id
                    .starts_with(&format!("{}:", origin.session_id))
            {
                return Err("acceptance does not match admitted origin".into());
            }
            if receipt
                .accepted
                .as_ref()
                .is_some_and(|previous| previous != &accepted)
            {
                return Err("command already bound to a different acceptance".into());
            }
            {
                let mut turns = write
                    .open_table(ACCEPTED_TURNS)
                    .map_err(|e| e.to_string())?;
                if turns
                    .get(accepted.turn_id.as_str())
                    .map_err(|e| e.to_string())?
                    .is_some_and(|previous| previous.value() != key)
                {
                    return Err("turn already bound to a different command".into());
                }
                turns
                    .insert(accepted.turn_id.as_str(), key.as_str())
                    .map_err(|e| e.to_string())?;
            }
            receipt.accepted = Some(accepted);
            let bytes = serde_json::to_vec(&receipt).map_err(|e| e.to_string())?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| e.to_string())?;
        }
        write.commit().map_err(|e| e.to_string())
    }

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
            write.open_table(COMMANDS).map_err(|e| e.to_string())?;
            write
                .open_table(INTERACTION_PROJECTION)
                .map_err(|e| e.to_string())?;
            write
                .open_table(INTERACTION_CONTENT)
                .map_err(|e| e.to_string())?;
            write
                .open_table(INTERACTION_FINAL_GENERATIONS)
                .map_err(|e| e.to_string())?;
            write
                .open_table(ACCEPTED_TURNS)
                .map_err(|e| e.to_string())?;
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
            attention: projection.attention.clone(),
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
                attention: None,
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

    pub(crate) fn recent_conversation_messages(
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
                attention: message.attention,
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
                attention: message.attention,
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
            attention: None,
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
