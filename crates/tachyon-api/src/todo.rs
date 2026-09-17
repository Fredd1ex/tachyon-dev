//! Flat, daemon-owned todos. Caller authority and provenance are not wire input.
use crate::operational_events::OperationalWatermark;
use serde::{Deserialize, Serialize};

pub const TITLE_MAX_BYTES: usize = 512;
pub const DESCRIPTION_MAX_BYTES: usize = 16_384;
pub const ID_MAX_BYTES: usize = 256;
pub const LIST_MAX: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TodoScope {
    Conversation { id: String },
    Work { work_id: String },
    Campaign { campaign_id: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Blocked,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TodoActor {
    pub source: String,
    pub actor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Todo {
    pub schema_version: u32,
    pub id: String,
    pub scope: TodoScope,
    pub title: String,
    pub description: String,
    pub status: TodoStatus,
    /// Immutable, daemon-assigned scope sequence. Never client arithmetic.
    pub order_key: u64,
    pub revision: u64,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub created_by: TodoActor,
    pub updated_by: TodoActor,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TodoFilter {
    pub status: Option<TodoStatus>,
    /// Exact retrieval within this scope; at most 100 unique IDs.
    pub ids: Option<Vec<String>>,
}

/// Opaque to callers: echo this JSON object unchanged to continue a list.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TodoCursor {
    pub version: u32,
    pub instance_id: String,
    pub scope: TodoScope,
    pub filter: TodoFilter,
    pub scope_revision: u64,
    pub after_order_key: u64,
    pub after_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum TodoRequest {
    List {
        scope: TodoScope,
        #[serde(default)]
        filter: TodoFilter,
        limit: Option<usize>,
        cursor: Option<TodoCursor>,
    },
    Add {
        scope: TodoScope,
        command_id: String,
        /// Exact scope revision (zero for an empty scope).
        expected_revision: u64,
        title: String,
        #[serde(default)]
        description: String,
    },
    Update {
        scope: TodoScope,
        command_id: String,
        id: String,
        /// Exact record revision, not the scope revision.
        expected_revision: u64,
        title: Option<String>,
        description: Option<String>,
        status: Option<TodoStatus>,
    },
}

impl TodoRequest {
    pub fn scope(&self) -> &TodoScope {
        match self {
            Self::List { scope, .. } | Self::Add { scope, .. } | Self::Update { scope, .. } => {
                scope
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TodoResponse {
    List {
        todos: Vec<Todo>,
        scope_revision: u64,
        watermark: OperationalWatermark,
        next_cursor: Option<TodoCursor>,
    },
    Mutation {
        todo: Todo,
        scope_revision: u64,
        watermark: OperationalWatermark,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TodoError {
    Invalid { message: String },
    AuthorityDenied,
    NotFound,
    RevisionConflict { current_revision: u64 },
    CommandConflict,
    CursorStale,
    Storage { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_scope_and_host_only_provenance() {
        assert!(serde_json::from_str::<TodoScope>(
            r#"{"kind":"conversation","id":"c","work_id":"w"}"#
        )
        .is_err());
        assert!(serde_json::from_str::<TodoRequest>(r#"{"operation":"add","scope":{"kind":"conversation","id":"c"},"command_id":"x","expected_revision":0,"title":"t","actor":"spoof"}"#).is_err());
        let scope = TodoScope::Conversation {
            id: "ordinary".into(),
        };
        assert_eq!(
            serde_json::from_slice::<TodoScope>(&serde_json::to_vec(&scope).unwrap()).unwrap(),
            scope
        );
    }

    #[test]
    fn requests_cursors_errors_and_statuses_round_trip() {
        let scope = TodoScope::Conversation { id: "c".into() };
        for status in [
            TodoStatus::Pending,
            TodoStatus::InProgress,
            TodoStatus::Blocked,
            TodoStatus::Completed,
            TodoStatus::Cancelled,
        ] {
            let request = TodoRequest::Update {
                scope: scope.clone(),
                command_id: "cmd".into(),
                id: "todo".into(),
                expected_revision: u64::MAX,
                title: None,
                description: Some(String::new()),
                status: Some(status),
            };
            assert_eq!(
                serde_json::from_slice::<TodoRequest>(&serde_json::to_vec(&request).unwrap())
                    .unwrap(),
                request
            );
        }
        let cursor = TodoCursor {
            version: 1,
            instance_id: "database-uuid".into(),
            scope: scope.clone(),
            filter: TodoFilter {
                status: Some(TodoStatus::Blocked),
                ids: Some(vec!["todo".into()]),
            },
            scope_revision: 42,
            after_order_key: 7,
            after_id: "todo".into(),
        };
        let request = TodoRequest::List {
            scope,
            filter: cursor.filter.clone(),
            cursor: Some(cursor),
            limit: None,
        };
        assert_eq!(
            serde_json::from_slice::<TodoRequest>(&serde_json::to_vec(&request).unwrap()).unwrap(),
            request
        );
        let error = TodoError::CursorStale;
        assert_eq!(
            serde_json::from_slice::<TodoError>(&serde_json::to_vec(&error).unwrap()).unwrap(),
            error
        );
    }
}
