use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u64 = 1;
const METADATA: TableDefinition<&str, u64> = TableDefinition::new("metadata");
const MEMORIES: TableDefinition<&str, &[u8]> = TableDefinition::new("memories");
const SUBJECT_PREDICATE_INDEX: TableDefinition<&str, &str> =
    TableDefinition::new("subject_predicate_index");
const REVOCATIONS: TableDefinition<&str, u64> = TableDefinition::new("revocations");

#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    #[error("memory storage error: {0}")]
    Storage(String),
    #[error("memory serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid memory identifier: {0}")]
    InvalidIdentifier(String),
    #[error("memory not found: {0}")]
    NotFound(String),
    #[error("memory identifier conflicts with an existing record: {0}")]
    Conflict(String),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MemoryRecord {
    pub schema_version: u32,
    pub id: String,
    pub subject: String,
    pub predicate: String,
    pub value: String,
    pub provenance: String,
    pub confidence_millis: u16,
    pub sensitivity: String,
    pub consent: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub supersedes: Option<String>,
}

pub struct MemoryStore {
    database: Database,
}

impl MemoryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MemoryError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                MemoryError::Storage(format!("create {}: {error}", parent.display()))
            })?;
        }
        let database = Database::create(path).map_err(storage)?;
        let store = Self { database };
        store.initialize()?;
        Ok(store)
    }

    fn initialize(&self) -> Result<(), MemoryError> {
        let write = self.database.begin_write().map_err(storage)?;
        {
            let mut metadata = write.open_table(METADATA).map_err(storage)?;
            let found = metadata
                .get("schema_version")
                .map_err(storage)?
                .map(|value| value.value());
            match found {
                Some(SCHEMA_VERSION) => {}
                Some(version) => {
                    return Err(MemoryError::Storage(format!(
                        "unsupported memories schema {version}; expected {SCHEMA_VERSION}"
                    )));
                }
                None => {
                    metadata
                        .insert("schema_version", SCHEMA_VERSION)
                        .map_err(storage)?;
                }
            }
            write.open_table(MEMORIES).map_err(storage)?;
            write.open_table(SUBJECT_PREDICATE_INDEX).map_err(storage)?;
            write.open_table(REVOCATIONS).map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    pub fn put(&self, record: &MemoryRecord) -> Result<(), MemoryError> {
        validate(record)?;
        let bytes = serde_json::to_vec(record)?;
        let index_key = format!(
            "{}:{}:{:020}:{}",
            record.subject, record.predicate, record.updated_at_ms, record.id
        );
        let write = self.database.begin_write().map_err(storage)?;
        {
            let memories = write.open_table(MEMORIES).map_err(storage)?;
            let existing = memories
                .get(record.id.as_str())
                .map_err(storage)?
                .map(|value| value.value().to_vec());
            if let Some(existing) = existing {
                let existing: MemoryRecord = serde_json::from_slice(&existing)?;
                if existing == *record {
                    return Ok(());
                }
                return Err(MemoryError::Conflict(record.id.clone()));
            }
        }
        write
            .open_table(MEMORIES)
            .map_err(storage)?
            .insert(record.id.as_str(), bytes.as_slice())
            .map_err(storage)?;
        write
            .open_table(SUBJECT_PREDICATE_INDEX)
            .map_err(storage)?
            .insert(index_key.as_str(), record.id.as_str())
            .map_err(storage)?;
        write.commit().map_err(storage)
    }

    pub fn get(&self, id: &str) -> Result<MemoryRecord, MemoryError> {
        validate_identifier(id)?;
        let read = self.database.begin_read().map_err(storage)?;
        let revocations = read.open_table(REVOCATIONS).map_err(storage)?;
        if revocations.get(id).map_err(storage)?.is_some() {
            return Err(MemoryError::NotFound(id.into()));
        }
        let memories = read.open_table(MEMORIES).map_err(storage)?;
        let value = memories
            .get(id)
            .map_err(storage)?
            .ok_or_else(|| MemoryError::NotFound(id.into()))?;
        Ok(serde_json::from_slice(value.value())?)
    }

    pub fn list(&self) -> Result<Vec<MemoryRecord>, MemoryError> {
        let read = self.database.begin_read().map_err(storage)?;
        let memories = read.open_table(MEMORIES).map_err(storage)?;
        let revocations = read.open_table(REVOCATIONS).map_err(storage)?;
        let mut records = Vec::new();
        for entry in memories.iter().map_err(storage)? {
            let (id, value) = entry.map_err(storage)?;
            if revocations.get(id.value()).map_err(storage)?.is_none() {
                records.push(serde_json::from_slice(value.value())?);
            }
        }
        records.sort_by(|left: &MemoryRecord, right| left.id.cmp(&right.id));
        Ok(records)
    }

    pub fn revoke(&self, id: &str, revoked_at_ms: u64) -> Result<(), MemoryError> {
        validate_identifier(id)?;
        let write = self.database.begin_write().map_err(storage)?;
        {
            let memories = write.open_table(MEMORIES).map_err(storage)?;
            if memories.get(id).map_err(storage)?.is_none() {
                return Err(MemoryError::NotFound(id.into()));
            }
        }
        write
            .open_table(REVOCATIONS)
            .map_err(storage)?
            .insert(id, revoked_at_ms)
            .map_err(storage)?;
        write.commit().map_err(storage)
    }
}

fn storage(error: impl std::fmt::Display) -> MemoryError {
    MemoryError::Storage(error.to_string())
}

fn validate(record: &MemoryRecord) -> Result<(), MemoryError> {
    validate_identifier(&record.id)?;
    if record.schema_version != SCHEMA_VERSION as u32
        || record.subject.trim().is_empty()
        || record.predicate.trim().is_empty()
        || record.value.trim().is_empty()
        || record.provenance.trim().is_empty()
        || record.consent.trim().is_empty()
        || record.confidence_millis > 1000
    {
        return Err(MemoryError::InvalidIdentifier(record.id.clone()));
    }
    Ok(())
}

fn validate_identifier(id: &str) -> Result<(), MemoryError> {
    if id.is_empty() || id == "." || id == ".." || id.contains('/') || id.contains('\\') {
        return Err(MemoryError::InvalidIdentifier(id.into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str) -> MemoryRecord {
        MemoryRecord {
            schema_version: 1,
            id: id.into(),
            subject: "user".into(),
            predicate: "response_style".into(),
            value: "concise".into(),
            provenance: "explicit user statement in conversation-1 turn-2".into(),
            confidence_millis: 1000,
            sensitivity: "normal".into(),
            consent: "stated".into(),
            created_at_ms: 100,
            updated_at_ms: 100,
            expires_at_ms: None,
            supersedes: None,
        }
    }

    #[test]
    fn curated_memory_round_trips_and_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("memories.redb");
        MemoryStore::open(&path)
            .unwrap()
            .put(&record("preference-1"))
            .unwrap();
        let store = MemoryStore::open(&path).unwrap();
        assert_eq!(store.get("preference-1").unwrap(), record("preference-1"));
    }

    #[test]
    fn stable_ids_are_idempotent_but_cannot_be_overwritten() {
        let directory = tempfile::tempdir().unwrap();
        let store = MemoryStore::open(directory.path().join("memories.redb")).unwrap();
        let original = record("preference-1");
        store.put(&original).unwrap();
        store.put(&original).unwrap();
        let mut conflicting = original;
        conflicting.value = "verbose".into();
        assert!(matches!(
            store.put(&conflicting),
            Err(MemoryError::Conflict(_))
        ));
    }

    #[test]
    fn revoked_memory_is_excluded_from_reads() {
        let directory = tempfile::tempdir().unwrap();
        let store = MemoryStore::open(directory.path().join("memories.redb")).unwrap();
        store.put(&record("preference-1")).unwrap();
        store.revoke("preference-1", 200).unwrap();
        assert!(matches!(
            store.get("preference-1"),
            Err(MemoryError::NotFound(_))
        ));
        assert!(store.list().unwrap().is_empty());
    }
}
