use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tachyon_api::types::{
    MemoryCardinality, MemoryDescriptor, MemoryIntent, MemoryKind, MemoryMutationKind,
    MemoryMutationResult,
};

const SCHEMA_VERSION: u64 = 1;
const MEMORY_RECORD_VERSION: u32 = 2;
const METADATA: TableDefinition<&str, u64> = TableDefinition::new("metadata");
const MEMORIES: TableDefinition<&str, &[u8]> = TableDefinition::new("memories");
const SUBJECT_PREDICATE_INDEX: TableDefinition<&str, &str> =
    TableDefinition::new("subject_predicate_index");
const REVOCATIONS: TableDefinition<&str, u64> = TableDefinition::new("revocations");
const PRIMITIVES: TableDefinition<&str, &[u8]> = TableDefinition::new("primitive_catalog");

const PRIMITIVE_SEEDS: [(&str, &str, &str); 19] = [
    ("kind.fact", "kind", "Stable information about the subject."),
    (
        "kind.preference",
        "kind",
        "A choice or taste that may guide responses.",
    ),
    (
        "kind.constraint",
        "kind",
        "A boundary or requirement that must be respected.",
    ),
    (
        "kind.goal",
        "kind",
        "A durable outcome the subject wants to achieve.",
    ),
    ("kind.routine", "kind", "A repeated behavior or schedule."),
    (
        "kind.relationship",
        "kind",
        "A durable relationship between entities.",
    ),
    (
        "scope.global",
        "scope",
        "Applies across conversations and workspaces.",
    ),
    ("scope.project", "scope", "Applies to one project."),
    ("scope.workspace", "scope", "Applies to one workspace."),
    (
        "scope.conversation",
        "scope",
        "Applies only to one conversation.",
    ),
    (
        "cardinality.one",
        "cardinality",
        "Only one active value is expected.",
    ),
    (
        "cardinality.many",
        "cardinality",
        "Multiple active values may coexist.",
    ),
    (
        "namespace.personal",
        "namespace",
        "Personal profile and tastes.",
    ),
    (
        "namespace.interaction",
        "namespace",
        "Communication and response behavior.",
    ),
    ("namespace.work", "namespace", "Work, projects, and tools."),
    (
        "namespace.environment",
        "namespace",
        "Devices, software, and execution environment.",
    ),
    (
        "namespace.relationships",
        "namespace",
        "People, teams, and organizations.",
    ),
    (
        "namespace.location",
        "namespace",
        "Locale, timezone, and place information.",
    ),
    (
        "namespace.uncategorized",
        "namespace",
        "Compatible fallback for legacy records.",
    ),
];

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
    #[serde(default)]
    pub kind: MemoryKind,
    #[serde(default = "default_record_namespace")]
    pub namespace: String,
    #[serde(default = "default_record_scope")]
    pub scope: String,
    #[serde(default)]
    pub cardinality: MemoryCardinality,
    #[serde(default)]
    pub topics: Vec<String>,
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrimitiveRecord {
    pub schema_version: u32,
    pub id: String,
    pub family: String,
    pub description: String,
}

pub struct MemoryStore {
    database: Database,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreferenceObservation<'a> {
    pub event_id: &'a str,
    pub conversation_id: &'a str,
    pub turn_id: Option<&'a str>,
    pub text: &'a str,
    pub occurred_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryMutationSource<'a> {
    pub event_id: &'a str,
    pub conversation_id: &'a str,
    pub turn_id: u64,
    pub occurred_at_ms: u64,
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
            let mut primitives = write.open_table(PRIMITIVES).map_err(storage)?;
            for (id, family, description) in PRIMITIVE_SEEDS {
                let primitive = PrimitiveRecord {
                    schema_version: 1,
                    id: id.into(),
                    family: family.into(),
                    description: description.into(),
                };
                let bytes = serde_json::to_vec(&primitive)?;
                let existing = primitives
                    .get(id)
                    .map_err(storage)?
                    .map(|existing| existing.value().to_vec());
                if let Some(existing) = existing {
                    let existing: PrimitiveRecord = serde_json::from_slice(&existing)?;
                    if existing != primitive {
                        return Err(MemoryError::Conflict(id.into()));
                    }
                } else {
                    primitives.insert(id, bytes.as_slice()).map_err(storage)?;
                }
            }
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

    pub fn list_primitives(&self) -> Result<Vec<PrimitiveRecord>, MemoryError> {
        let read = self.database.begin_read().map_err(storage)?;
        let primitives = read.open_table(PRIMITIVES).map_err(storage)?;
        let mut records = Vec::new();
        for entry in primitives.iter().map_err(storage)? {
            let (_, value) = entry.map_err(storage)?;
            records.push(serde_json::from_slice(value.value())?);
        }
        records.sort_by(|left: &PrimitiveRecord, right| left.id.cmp(&right.id));
        Ok(records)
    }

    /// Return active, explicitly-consented preferences in deterministic
    /// relevance order. This intentionally keeps retrieval behind a small API
    /// so a lexical ranker can later be replaced by semantic search.
    pub fn recall_preferences(
        &self,
        query: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, MemoryError> {
        let query_tokens = search_tokens(query);
        let mut ranked = self
            .list()?
            .into_iter()
            .filter(|record| {
                record.subject == "user"
                    && matches!(record.consent.as_str(), "stated" | "explicit" | "approved")
                    && record.sensitivity == "normal"
                    && record.expires_at_ms.is_none_or(|expiry| expiry > now_ms)
            })
            .map(|record| {
                let candidate = search_tokens(&record.value);
                let score = query_tokens
                    .iter()
                    .filter(|token| candidate.contains(*token))
                    .count();
                (score, record)
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .cmp(left_score)
                .then_with(|| right.updated_at_ms.cmp(&left.updated_at_ms))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(ranked
            .into_iter()
            .take(limit)
            .map(|(_, record)| record)
            .collect())
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

    pub fn apply_intent(
        &self,
        intent: &MemoryIntent,
        source: MemoryMutationSource<'_>,
    ) -> Result<MemoryMutationResult, MemoryError> {
        match intent {
            MemoryIntent::Ignore => Ok(MemoryMutationResult::Ignored),
            MemoryIntent::Remember { descriptor, value } => {
                self.remember(descriptor, value, source)
            }
            MemoryIntent::Forget { target_ids } => self.forget(target_ids, source.occurred_at_ms),
            MemoryIntent::Correct {
                target_ids,
                descriptor,
                value,
            } => self.correct(target_ids, descriptor, value, source),
        }
    }

    fn remember(
        &self,
        descriptor: &MemoryDescriptor,
        value: &str,
        source: MemoryMutationSource<'_>,
    ) -> Result<MemoryMutationResult, MemoryError> {
        let value = normalize_value(value);
        if value.is_empty() || value.chars().count() > 1000 {
            return Ok(MemoryMutationResult::Rejected {
                reason: "memory value must contain 1 to 1000 characters".into(),
            });
        }
        let matching = self
            .list()?
            .into_iter()
            .filter(|record| {
                record.subject == "user"
                    && record.kind == descriptor.kind
                    && record.namespace == descriptor.namespace
                    && record.predicate == descriptor.relation
                    && record.scope == descriptor.scope
            })
            .collect::<Vec<_>>();
        if let Some(existing) = matching
            .iter()
            .find(|record| record.value.eq_ignore_ascii_case(&value))
        {
            return Ok(MemoryMutationResult::AlreadyApplied {
                kind: MemoryMutationKind::Remember,
                memory_id: existing.id.clone(),
            });
        }
        if descriptor.cardinality == MemoryCardinality::One && !matching.is_empty() {
            let target_ids = matching
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>();
            return self.correct(&target_ids, descriptor, &value, source);
        }
        let record = mutation_record(descriptor, &value, &source, None);
        match self.put(&record) {
            Ok(()) => Ok(MemoryMutationResult::Applied {
                kind: MemoryMutationKind::Remember,
                memory_id: record.id,
                replaced_memory_id: None,
            }),
            Err(MemoryError::Conflict(_)) => Ok(MemoryMutationResult::AlreadyApplied {
                kind: MemoryMutationKind::Remember,
                memory_id: record.id,
            }),
            Err(error) => Err(error),
        }
    }

    fn forget(
        &self,
        target_ids: &[String],
        occurred_at_ms: u64,
    ) -> Result<MemoryMutationResult, MemoryError> {
        if target_ids.is_empty() {
            return Ok(MemoryMutationResult::Rejected {
                reason: "forget requires at least one memory".into(),
            });
        }
        for target_id in target_ids {
            validate_identifier(target_id)?;
        }
        let write = self.database.begin_write().map_err(storage)?;
        {
            let memories = write.open_table(MEMORIES).map_err(storage)?;
            for target_id in target_ids {
                if memories.get(target_id.as_str()).map_err(storage)?.is_none() {
                    return Ok(MemoryMutationResult::Rejected {
                        reason: "a requested memory no longer exists".into(),
                    });
                }
            }
        }
        {
            let mut revocations = write.open_table(REVOCATIONS).map_err(storage)?;
            let mut changed = false;
            for target_id in target_ids {
                if revocations
                    .get(target_id.as_str())
                    .map_err(storage)?
                    .is_none()
                {
                    revocations
                        .insert(target_id.as_str(), occurred_at_ms)
                        .map_err(storage)?;
                    changed = true;
                }
            }
            if !changed {
                return Ok(MemoryMutationResult::AlreadyApplied {
                    kind: MemoryMutationKind::Forget,
                    memory_id: target_ids[0].clone(),
                });
            }
        }
        write.commit().map_err(storage)?;
        Ok(MemoryMutationResult::Applied {
            kind: MemoryMutationKind::Forget,
            memory_id: target_ids[0].clone(),
            replaced_memory_id: None,
        })
    }

    fn correct(
        &self,
        target_ids: &[String],
        descriptor: &MemoryDescriptor,
        value: &str,
        source: MemoryMutationSource<'_>,
    ) -> Result<MemoryMutationResult, MemoryError> {
        if target_ids.is_empty() {
            return Ok(MemoryMutationResult::Rejected {
                reason: "correction requires at least one memory".into(),
            });
        }
        for target_id in target_ids {
            validate_identifier(target_id)?;
        }
        let value = normalize_value(value);
        if value.is_empty() || value.chars().count() > 1000 {
            return Ok(MemoryMutationResult::Rejected {
                reason: "memory value must contain 1 to 1000 characters".into(),
            });
        }
        let replacement = mutation_record(descriptor, &value, &source, Some(target_ids[0].clone()));
        let replacement_bytes = serde_json::to_vec(&replacement)?;
        let index_key = format!(
            "{}:{}:{:020}:{}",
            replacement.subject, replacement.predicate, replacement.updated_at_ms, replacement.id
        );
        let write = self.database.begin_write().map_err(storage)?;
        let targets_exist = {
            let memories = write.open_table(MEMORIES).map_err(storage)?;
            let mut all_exist = true;
            for target_id in target_ids {
                all_exist &= memories.get(target_id.as_str()).map_err(storage)?.is_some();
            }
            all_exist
        };
        if !targets_exist {
            return Ok(MemoryMutationResult::Rejected {
                reason: "a requested memory no longer exists".into(),
            });
        }
        let all_targets_revoked = {
            let revocations = write.open_table(REVOCATIONS).map_err(storage)?;
            let mut all_revoked = true;
            for target_id in target_ids {
                all_revoked &= revocations
                    .get(target_id.as_str())
                    .map_err(storage)?
                    .is_some();
            }
            all_revoked
        };
        let replacement_exists = {
            let memories = write.open_table(MEMORIES).map_err(storage)?;
            let exists = memories
                .get(replacement.id.as_str())
                .map_err(storage)?
                .is_some();
            exists
        };
        if all_targets_revoked {
            return Ok(if replacement_exists {
                MemoryMutationResult::AlreadyApplied {
                    kind: MemoryMutationKind::Correct,
                    memory_id: replacement.id,
                }
            } else {
                MemoryMutationResult::Rejected {
                    reason: "the requested memory was already forgotten".into(),
                }
            });
        }
        if replacement_exists {
            return Ok(MemoryMutationResult::Rejected {
                reason: "the correction conflicts with an existing memory".into(),
            });
        }
        write
            .open_table(MEMORIES)
            .map_err(storage)?
            .insert(replacement.id.as_str(), replacement_bytes.as_slice())
            .map_err(storage)?;
        write
            .open_table(SUBJECT_PREDICATE_INDEX)
            .map_err(storage)?
            .insert(index_key.as_str(), replacement.id.as_str())
            .map_err(storage)?;
        {
            let mut revocations = write.open_table(REVOCATIONS).map_err(storage)?;
            for target_id in target_ids {
                revocations
                    .insert(target_id.as_str(), source.occurred_at_ms)
                    .map_err(storage)?;
            }
        }
        write.commit().map_err(storage)?;
        Ok(MemoryMutationResult::Applied {
            kind: MemoryMutationKind::Correct,
            memory_id: replacement.id,
            replaced_memory_id: Some(target_ids[0].clone()),
        })
    }
}

fn mutation_record(
    descriptor: &MemoryDescriptor,
    value: &str,
    source: &MemoryMutationSource<'_>,
    supersedes: Option<String>,
) -> MemoryRecord {
    let id_suffix = source
        .event_id
        .chars()
        .map(|character| match character {
            '/' | '\\' => '-',
            character => character,
        })
        .collect::<String>();
    MemoryRecord {
        schema_version: MEMORY_RECORD_VERSION,
        id: format!("preference-{id_suffix}"),
        subject: "user".into(),
        predicate: descriptor.relation.clone(),
        kind: descriptor.kind,
        namespace: descriptor.namespace.clone(),
        scope: descriptor.scope.clone(),
        cardinality: descriptor.cardinality,
        topics: descriptor.topics.clone(),
        value: value.into(),
        provenance: format!(
            "semantic user memory intent in {} turn {}",
            source.conversation_id, source.turn_id
        ),
        confidence_millis: 1000,
        sensitivity: "normal".into(),
        consent: "stated".into(),
        created_at_ms: source.occurred_at_ms,
        updated_at_ms: source.occurred_at_ms,
        expires_at_ms: None,
        supersedes,
    }
}

fn normalize_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn default_record_namespace() -> String {
    "uncategorized".into()
}

fn default_record_scope() -> String {
    "global".into()
}

/// Curate only direct preference statements. Conversation history is not
/// promoted unless the user used an explicit preference construction.
pub fn explicit_preference(observation: PreferenceObservation<'_>) -> Option<MemoryRecord> {
    let text = observation.text.trim();
    if text.is_empty() || text.chars().count() > 1000 {
        return None;
    }
    let normalized = text.to_ascii_lowercase();
    const MARKERS: [&str; 8] = [
        "i prefer ",
        "my preference is ",
        "i like ",
        "i dislike ",
        "please always ",
        "i always want ",
        "i want you to always ",
        "remember that i ",
    ];
    if !MARKERS.iter().any(|marker| normalized.contains(marker)) {
        return None;
    }
    let id_suffix = observation
        .event_id
        .chars()
        .map(|character| match character {
            '/' | '\\' => '-',
            character => character,
        })
        .collect::<String>();
    Some(MemoryRecord {
        schema_version: SCHEMA_VERSION as u32,
        id: format!("preference-{id_suffix}"),
        subject: "user".into(),
        predicate: "preference".into(),
        kind: MemoryKind::Preference,
        namespace: "uncategorized".into(),
        scope: "global".into(),
        cardinality: MemoryCardinality::Many,
        topics: Vec::new(),
        value: text.into(),
        provenance: format!(
            "explicit user statement in {} turn {}",
            observation.conversation_id,
            observation.turn_id.unwrap_or("unknown")
        ),
        confidence_millis: 1000,
        sensitivity: "normal".into(),
        consent: "stated".into(),
        created_at_ms: observation.occurred_at_ms,
        updated_at_ms: observation.occurred_at_ms,
        expires_at_ms: None,
        supersedes: None,
    })
}

fn search_tokens(text: &str) -> std::collections::BTreeSet<String> {
    text.split(|character: char| !character.is_ascii_alphanumeric())
        .map(str::to_ascii_lowercase)
        .filter(|token| token.len() > 2)
        .collect()
}

fn storage(error: impl std::fmt::Display) -> MemoryError {
    MemoryError::Storage(error.to_string())
}

fn validate(record: &MemoryRecord) -> Result<(), MemoryError> {
    validate_identifier(&record.id)?;
    if !matches!(record.schema_version, 1 | MEMORY_RECORD_VERSION)
        || record.subject.trim().is_empty()
        || record.predicate.trim().is_empty()
        || record.namespace.trim().is_empty()
        || record.scope.trim().is_empty()
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
            kind: MemoryKind::Preference,
            namespace: "interaction.response".into(),
            scope: "global".into(),
            cardinality: MemoryCardinality::One,
            topics: vec!["responses".into()],
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

    #[test]
    fn explicit_preferences_are_curated_but_ordinary_tasks_are_not() {
        let preference = explicit_preference(PreferenceObservation {
            event_id: "event/1",
            conversation_id: "conversation-1",
            turn_id: Some("2"),
            text: "I prefer concise answers.",
            occurred_at_ms: 100,
        })
        .unwrap();
        assert_eq!(preference.id, "preference-event-1");
        assert_eq!(preference.predicate, "preference");
        assert!(explicit_preference(PreferenceObservation {
            event_id: "event-2",
            conversation_id: "conversation-1",
            turn_id: Some("3"),
            text: "Add a unit test for database recovery.",
            occurred_at_ms: 200,
        })
        .is_none());
    }

    #[test]
    fn recall_is_bounded_ranked_and_policy_filtered() {
        let directory = tempfile::tempdir().unwrap();
        let store = MemoryStore::open(directory.path().join("memories.redb")).unwrap();
        let mut concise = record("concise");
        concise.predicate = "preference".into();
        concise.value = "I prefer concise Rust answers".into();
        let mut expired = record("expired");
        expired.predicate = "preference".into();
        expired.value = "I prefer verbose Rust answers".into();
        expired.expires_at_ms = Some(99);
        let mut sensitive = record("sensitive");
        sensitive.predicate = "preference".into();
        sensitive.sensitivity = "private".into();
        store.put(&concise).unwrap();
        store.put(&expired).unwrap();
        store.put(&sensitive).unwrap();

        let recalled = store.recall_preferences("Rust style", 100, 1).unwrap();
        assert_eq!(recalled, vec![concise]);
    }

    #[test]
    fn deleted_or_moved_memory_database_reopens_blank() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("memories.redb");
        {
            let store = MemoryStore::open(&path).unwrap();
            store.put(&record("preference-1")).unwrap();
        }
        std::fs::rename(&path, directory.path().join("memories.backup")).unwrap();
        let regenerated = MemoryStore::open(&path).unwrap();
        assert!(regenerated.list().unwrap().is_empty());
        let primitives = regenerated.list_primitives().unwrap();
        assert_eq!(primitives.len(), PRIMITIVE_SEEDS.len());
        assert!(primitives
            .iter()
            .any(|primitive| primitive.id == "kind.preference"));
        assert!(primitives
            .iter()
            .any(|primitive| primitive.id == "namespace.work"));
    }

    fn source<'a>(event_id: &'a str, at: u64) -> MemoryMutationSource<'a> {
        MemoryMutationSource {
            event_id,
            conversation_id: "conversation-1",
            turn_id: 2,
            occurred_at_ms: at,
        }
    }

    #[test]
    fn semantic_mutations_remember_deduplicate_forget_and_correct() {
        let directory = tempfile::tempdir().unwrap();
        let store = MemoryStore::open(directory.path().join("memories.redb")).unwrap();
        let remembered = store
            .apply_intent(
                &MemoryIntent::Remember {
                    descriptor: MemoryDescriptor::default(),
                    value: "likes pickles".into(),
                },
                source("one", 100),
            )
            .unwrap();
        assert!(matches!(
            remembered,
            MemoryMutationResult::Applied {
                kind: MemoryMutationKind::Remember,
                ..
            }
        ));
        assert!(matches!(
            store
                .apply_intent(
                    &MemoryIntent::Remember {
                        descriptor: MemoryDescriptor::default(),
                        value: " likes   pickles ".into(),
                    },
                    source("two", 200),
                )
                .unwrap(),
            MemoryMutationResult::AlreadyApplied { .. }
        ));

        let original_id = store.list().unwrap()[0].id.clone();
        let corrected = store
            .apply_intent(
                &MemoryIntent::Correct {
                    target_ids: vec![original_id.clone()],
                    descriptor: MemoryDescriptor::default(),
                    value: "dislikes pickles".into(),
                },
                source("three", 300),
            )
            .unwrap();
        assert!(matches!(
            corrected,
            MemoryMutationResult::Applied {
                kind: MemoryMutationKind::Correct,
                replaced_memory_id: Some(ref id),
                ..
            } if id == &original_id
        ));
        let active = store.list().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].value, "dislikes pickles");
        assert_eq!(active[0].supersedes.as_deref(), Some(original_id.as_str()));

        let corrected_id = active[0].id.clone();
        assert!(matches!(
            store
                .apply_intent(
                    &MemoryIntent::Forget {
                        target_ids: vec![corrected_id],
                    },
                    source("four", 400),
                )
                .unwrap(),
            MemoryMutationResult::Applied {
                kind: MemoryMutationKind::Forget,
                ..
            }
        ));
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    fn single_cardinality_remember_supersedes_the_active_value() {
        let directory = tempfile::tempdir().unwrap();
        let store = MemoryStore::open(directory.path().join("memories.redb")).unwrap();
        let descriptor = MemoryDescriptor {
            cardinality: MemoryCardinality::One,
            ..MemoryDescriptor::default()
        };
        store
            .apply_intent(
                &MemoryIntent::Remember {
                    descriptor: descriptor.clone(),
                    value: "likes pickles".into(),
                },
                source("one", 100),
            )
            .unwrap();
        let original_id = store.list().unwrap()[0].id.clone();

        let result = store
            .apply_intent(
                &MemoryIntent::Remember {
                    descriptor,
                    value: "dislikes pickles".into(),
                },
                source("two", 200),
            )
            .unwrap();

        assert!(matches!(
            result,
            MemoryMutationResult::Applied {
                kind: MemoryMutationKind::Correct,
                replaced_memory_id: Some(ref id),
                ..
            } if id == &original_id
        ));
        let active = store.list().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].value, "dislikes pickles");
    }

    #[test]
    fn semantic_forget_revokes_all_equivalent_selected_records_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let store = MemoryStore::open(directory.path().join("memories.redb")).unwrap();
        let mut first = record("old-one");
        first.predicate = "preference".into();
        first.value = "I like pickles".into();
        let mut second = record("old-two");
        second.predicate = "preference".into();
        second.value = "remember that I love pickles".into();
        store.put(&first).unwrap();
        store.put(&second).unwrap();

        let result = store
            .apply_intent(
                &MemoryIntent::Forget {
                    target_ids: vec![first.id, second.id],
                },
                source("forget", 500),
            )
            .unwrap();
        assert!(matches!(
            result,
            MemoryMutationResult::Applied {
                kind: MemoryMutationKind::Forget,
                ..
            }
        ));
        assert!(store.list().unwrap().is_empty());
    }
}
