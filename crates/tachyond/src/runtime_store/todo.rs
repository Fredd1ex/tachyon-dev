//! Synchronous host facade. Offload calls on async hosts; no locks cross awaits.
#![allow(dead_code)]
use super::{operational_events as feed, RuntimeStore};
use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::ops::Bound::{Excluded, Included};
use tachyon_api::operational_events::{OperationalChange, OperationalEvent};
use tachyon_api::todo::*;

const RECORDS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("todos_v1");
const ORDER: TableDefinition<(&str, u64, &str), ()> = TableDefinition::new("todos_order_v1");
const SCOPES: TableDefinition<&str, u64> = TableDefinition::new("todo_scope_revisions_v1");
const RECEIPTS: TableDefinition<&str, &[u8]> = TableDefinition::new("todo_receipts_v1");

pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    tx.open_table(RECORDS).map_err(|e| e.to_string())?;
    tx.open_table(ORDER).map_err(|e| e.to_string())?;
    tx.open_table(SCOPES).map_err(|e| e.to_string())?;
    tx.open_table(RECEIPTS).map_err(|e| e.to_string())?;
    Ok(())
}

impl RuntimeStore {
    pub(crate) fn interaction_progress(
        &self,
        scope: &TodoScope,
    ) -> Result<tachyon_api::interaction_manager::Progress, String> {
        let read = || -> Result<_, TodoError> {
            let tx = self.database.begin_read().map_err(storage)?;
            let key = scope_key(scope)?;
            let revision = tx
                .open_table(SCOPES)
                .map_err(storage)?
                .get(key.as_str())
                .map_err(storage)?
                .map(|v| v.value())
                .unwrap_or(0);
            let mut progress = tachyon_api::interaction_manager::Progress {
                scope: scope.clone(),
                scope_revision: Some(revision),
                pending: 0,
                in_progress: 0,
                blocked: 0,
                completed: 0,
                cancelled: 0,
            };
            let records = tx.open_table(RECORDS).map_err(storage)?;
            for row in records
                .range((key.as_str(), "")..=(key.as_str(), "\u{10ffff}"))
                .map_err(storage)?
            {
                let (_, value) = row.map_err(storage)?;
                let todo: Todo = decode(value.value())?;
                match todo.status {
                    TodoStatus::Pending => progress.pending += 1,
                    TodoStatus::InProgress => progress.in_progress += 1,
                    TodoStatus::Blocked => progress.blocked += 1,
                    TodoStatus::Completed => progress.completed += 1,
                    TodoStatus::Cancelled => progress.cancelled += 1,
                }
            }
            Ok(progress)
        };
        read().map_err(|e| format!("todo progress: {e:?}"))
    }
}

/// Host-only, deliberately not deserializable. The host must explicitly grant
/// exactly one scope. Campaign membership alone never constructs a grant.
pub(crate) enum TodoAuthority {
    Bound {
        scope: TodoScope,
        actor: TodoActor,
    },
    Ghost {
        scope: TodoScope,
        work_id: String,
        campaign_id: String,
    },
}

pub(crate) struct TodoFacade<'a> {
    pub(super) store: &'a RuntimeStore,
    pub(super) scope: TodoScope,
    actor: TodoActor,
}

fn storage(e: impl std::fmt::Display) -> TodoError {
    TodoError::Storage {
        message: e.to_string(),
    }
}
fn invalid(message: &str) -> TodoError {
    TodoError::Invalid {
        message: message.into(),
    }
}
fn text(value: &str, max: usize) -> Result<(), TodoError> {
    if value.trim().is_empty() || value.len() > max || value.contains('\0') {
        return Err(invalid("blank, oversized, or NUL-containing text"));
    }
    Ok(())
}
fn description(value: &str) -> Result<(), TodoError> {
    if value.len() > DESCRIPTION_MAX_BYTES || value.contains('\0') {
        return Err(invalid("invalid description"));
    }
    Ok(())
}
fn scope_key(scope: &TodoScope) -> Result<String, TodoError> {
    text(
        match scope {
            TodoScope::Conversation { id } => id,
            TodoScope::Work { work_id } => work_id,
            TodoScope::Campaign { campaign_id } => campaign_id,
        },
        ID_MAX_BYTES,
    )?;
    serde_json::to_string(scope).map_err(storage)
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, TodoError> {
    serde_json::to_vec(value).map_err(storage)
}
fn decode<T: DeserializeOwned>(value: &[u8]) -> Result<T, TodoError> {
    serde_json::from_slice(value).map_err(storage)
}
fn record(value: &[u8], scope: &TodoScope, id: &str) -> Result<Todo, TodoError> {
    let todo: Todo = decode(value)?;
    if todo.schema_version != 1 || &todo.scope != scope || todo.id != id || todo.revision == 0 {
        return Err(storage("unsupported or invalid todo record"));
    }
    Ok(todo)
}

/// Bounded first-page projection in the caller's canonical snapshot. Descriptions
/// are not assessment input; retaining them would defeat the prompt byte bound.
pub(super) fn assessment_page_in(
    tx: &redb::ReadTransaction,
    campaign: &str,
) -> Result<TodoResponse, String> {
    let read = || -> Result<TodoResponse, TodoError> {
        let scope = TodoScope::Campaign {
            campaign_id: campaign.into(),
        };
        let key = scope_key(&scope)?;
        let revision = tx
            .open_table(SCOPES)
            .map_err(storage)?
            .get(key.as_str())
            .map_err(storage)?
            .map(|v| v.value())
            .unwrap_or(0);
        let watermark =
            feed::watermark(&tx.open_table(feed::METADATA).map_err(storage)?).map_err(storage)?;
        let order = tx.open_table(ORDER).map_err(storage)?;
        let records = tx.open_table(RECORDS).map_err(storage)?;
        let mut todos = Vec::new();
        for entry in order
            .range((key.as_str(), 0, "")..=(key.as_str(), u64::MAX, "\u{10ffff}"))
            .map_err(storage)?
            .take(21)
        {
            let (index, _) = entry.map_err(storage)?;
            let (_, _, id) = index.value();
            let value = records
                .get((key.as_str(), id))
                .map_err(storage)?
                .ok_or_else(|| storage("missing indexed todo"))?;
            let mut todo = record(value.value(), &scope, id)?;
            todo.description.clear();
            todos.push(todo);
        }
        let more = todos.len() > 20;
        todos.truncate(20);
        let next_cursor = more.then(|| {
            let last = todos.last().unwrap();
            TodoCursor {
                version: 1,
                instance_id: watermark.instance_id.clone(),
                scope: scope.clone(),
                filter: Default::default(),
                scope_revision: revision,
                after_order_key: last.order_key,
                after_id: last.id.clone(),
            }
        });
        Ok(TodoResponse::List {
            todos,
            scope_revision: revision,
            watermark,
            next_cursor,
        })
    };
    read().map_err(|e| format!("assessment todos: {e:?}"))
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    schema_version: u32,
    actor: TodoActor,
    request: TodoRequest,
    response: TodoResponse,
}

impl RuntimeStore {
    pub(crate) fn todos(&self, authority: TodoAuthority) -> Result<TodoFacade<'_>, TodoError> {
        let tx = self.database.begin_write().map_err(storage)?;
        let (scope, actor) = match authority {
            TodoAuthority::Bound { scope, actor } => (scope, actor),
            TodoAuthority::Ghost {
                scope,
                work_id,
                campaign_id,
            } => {
                text(&work_id, ID_MAX_BYTES)?;
                text(&campaign_id, ID_MAX_BYTES)?;
                let work = Self::admitted_work_in(&tx, &work_id)
                    .map_err(|_| TodoError::AuthorityDenied)?;
                if work.admission.campaign_id != campaign_id
                    || !match &scope {
                        TodoScope::Work { work_id: id } => id == &work_id,
                        TodoScope::Campaign { campaign_id: id } => id == &campaign_id,
                        TodoScope::Conversation { .. } => false,
                    }
                {
                    return Err(TodoError::AuthorityDenied);
                }
                Self::campaign_status_in(&tx, &campaign_id)
                    .map_err(|_| TodoError::AuthorityDenied)?;
                (
                    scope,
                    TodoActor {
                        source: "ghost".into(),
                        actor: work_id,
                    },
                )
            }
        };
        scope_key(&scope)?;
        text(&actor.source, ID_MAX_BYTES)?;
        text(&actor.actor, ID_MAX_BYTES)?;
        match &scope {
            TodoScope::Conversation { .. } => {}
            TodoScope::Work { work_id } => {
                let work =
                    Self::admitted_work_in(&tx, work_id).map_err(|_| TodoError::AuthorityDenied)?;
                Self::campaign_status_in(&tx, &work.admission.campaign_id)
                    .map_err(|_| TodoError::AuthorityDenied)?;
            }
            TodoScope::Campaign { campaign_id } => {
                Self::campaign_status_in(&tx, campaign_id)
                    .map_err(|_| TodoError::AuthorityDenied)?;
            }
        }
        // Abort the validation-only transaction before returning the facade.
        drop(tx);
        Ok(TodoFacade {
            store: self,
            scope,
            actor,
        })
    }
}

impl TodoFacade<'_> {
    pub(crate) fn execute(&self, request: TodoRequest) -> Result<TodoResponse, TodoError> {
        if request.scope() != &self.scope {
            return Err(TodoError::AuthorityDenied);
        }
        let key = scope_key(&self.scope)?;
        if let TodoRequest::List {
            filter,
            limit,
            cursor,
            ..
        } = &request
        {
            let limit = limit.unwrap_or(32);
            if !(1..=LIST_MAX).contains(&limit) {
                return Err(invalid("list limit must be 1..100"));
            }
            if let Some(ids) = &filter.ids {
                if ids.len() > LIST_MAX {
                    return Err(invalid("at most 100 IDs"));
                }
                let mut unique = std::collections::HashSet::new();
                for id in ids {
                    text(id, ID_MAX_BYTES)?;
                    if !unique.insert(id) {
                        return Err(invalid("duplicate ID"));
                    }
                }
            }
            let tx = self.store.database.begin_read().map_err(storage)?;
            let revision = tx
                .open_table(SCOPES)
                .map_err(storage)?
                .get(key.as_str())
                .map_err(storage)?
                .map(|r| r.value())
                .unwrap_or(0);
            let watermark = feed::watermark(&tx.open_table(feed::METADATA).map_err(storage)?)
                .map_err(storage)?;
            if let Some(cursor) = cursor {
                if cursor.version != 1 || cursor.scope != self.scope || &cursor.filter != filter {
                    return Err(invalid("cursor scope/filter/version mismatch"));
                }
                text(&cursor.after_id, ID_MAX_BYTES)?;
                if cursor.instance_id != watermark.instance_id || cursor.scope_revision != revision
                {
                    return Err(TodoError::CursorStale);
                }
            }
            let records = tx.open_table(RECORDS).map_err(storage)?;
            let mut todos = Vec::new();
            let after = cursor
                .as_ref()
                .map(|c| (c.after_order_key, c.after_id.as_str()));
            if let Some(ids) = &filter.ids {
                // Bounded exact lookups, not a scan across unrelated records.
                for id in ids {
                    if let Some(row) = records.get((key.as_str(), id.as_str())).map_err(storage)? {
                        let todo = record(row.value(), &self.scope, id)?;
                        if after.is_none_or(|a| (todo.order_key, todo.id.as_str()) > a)
                            && filter.status.is_none_or(|s| s == todo.status)
                        {
                            todos.push(todo);
                        }
                    }
                }
                todos.sort_by(|a, b| (a.order_key, &a.id).cmp(&(b.order_key, &b.id)));
            } else {
                let order = tx.open_table(ORDER).map_err(storage)?;
                let lower = match after {
                    Some((seq, id)) => Excluded((key.as_str(), seq, id)),
                    None => Included((key.as_str(), 0, "")),
                };
                for row in order
                    .range((lower, Included((key.as_str(), u64::MAX, "\u{10ffff}"))))
                    .map_err(storage)?
                {
                    let (index, _) = row.map_err(storage)?;
                    let (_, _, id) = index.value();
                    let row = records
                        .get((key.as_str(), id))
                        .map_err(storage)?
                        .ok_or_else(|| storage("missing indexed todo"))?;
                    let todo = record(row.value(), &self.scope, id)?;
                    if filter.status.is_none_or(|s| s == todo.status) {
                        todos.push(todo);
                    }
                    if todos.len() > limit {
                        break;
                    }
                }
            }
            let more = todos.len() > limit;
            todos.truncate(limit);
            let next_cursor = if more {
                todos.last().map(|last| TodoCursor {
                    version: 1,
                    instance_id: watermark.instance_id.clone(),
                    scope: self.scope.clone(),
                    filter: filter.clone(),
                    scope_revision: revision,
                    after_order_key: last.order_key,
                    after_id: last.id.clone(),
                })
            } else {
                None
            };
            return Ok(TodoResponse::List {
                todos,
                scope_revision: revision,
                watermark,
                next_cursor,
            });
        }
        let (command_id, expected_revision) = match &request {
            TodoRequest::Add {
                command_id,
                expected_revision,
                title,
                description: desc,
                ..
            } => {
                text(title, TITLE_MAX_BYTES)?;
                description(desc)?;
                (command_id, *expected_revision)
            }
            TodoRequest::Update {
                command_id,
                expected_revision,
                id,
                title,
                description: desc,
                status,
                ..
            } => {
                text(id, ID_MAX_BYTES)?;
                if let Some(title) = title {
                    text(title, TITLE_MAX_BYTES)?;
                }
                if let Some(desc) = desc {
                    description(desc)?;
                }
                if title.is_none() && desc.is_none() && status.is_none() {
                    return Err(invalid("empty update"));
                }
                (command_id, *expected_revision)
            }
            TodoRequest::List { .. } => unreachable!(),
        };
        text(command_id, ID_MAX_BYTES)?;
        let tx = self.store.database.begin_write().map_err(storage)?;
        // Replay precedes revision checks, so retries survive subsequent edits.
        if let Some(row) = tx
            .open_table(RECEIPTS)
            .map_err(storage)?
            .get(command_id.as_str())
            .map_err(storage)?
        {
            let receipt: Receipt = decode(row.value())?;
            if receipt.schema_version != 1 {
                return Err(storage("unsupported todo receipt schema"));
            }
            if receipt.request != request || receipt.actor != self.actor {
                return Err(TodoError::CommandConflict);
            }
            return Ok(receipt.response);
        }
        let revision = tx
            .open_table(SCOPES)
            .map_err(storage)?
            .get(key.as_str())
            .map_err(storage)?
            .map(|r| r.value())
            .unwrap_or(0);
        let next_revision = revision
            .checked_add(1)
            .ok_or_else(|| storage("scope revision exhausted"))?;
        let now = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(storage)?
                .as_millis(),
        )
        .map_err(storage)?;
        let (todo, added) = match &request {
            TodoRequest::Add {
                title, description, ..
            } => {
                if expected_revision != revision {
                    return Err(TodoError::RevisionConflict {
                        current_revision: revision,
                    });
                }
                (
                    Todo {
                        schema_version: 1,
                        id: uuid::Uuid::new_v4().to_string(),
                        scope: self.scope.clone(),
                        title: title.clone(),
                        description: description.clone(),
                        status: TodoStatus::Pending,
                        order_key: next_revision,
                        revision: 1,
                        created_ms: now,
                        updated_ms: now,
                        created_by: self.actor.clone(),
                        updated_by: self.actor.clone(),
                    },
                    true,
                )
            }
            TodoRequest::Update {
                id,
                title,
                description,
                status,
                ..
            } => {
                let table = tx.open_table(RECORDS).map_err(storage)?;
                let row = table
                    .get((key.as_str(), id.as_str()))
                    .map_err(storage)?
                    .ok_or(TodoError::NotFound)?;
                let mut todo = record(row.value(), &self.scope, id)?;
                if todo.revision != expected_revision {
                    return Err(TodoError::RevisionConflict {
                        current_revision: todo.revision,
                    });
                }
                if let Some(title) = title {
                    todo.title = title.clone();
                }
                if let Some(description) = description {
                    todo.description = description.clone();
                }
                if let Some(status) = status {
                    todo.status = *status;
                }
                todo.revision = todo
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| storage("todo revision exhausted"))?;
                todo.updated_ms = now.max(todo.updated_ms);
                todo.updated_by = self.actor.clone();
                (todo, false)
            }
            TodoRequest::List { .. } => unreachable!(),
        };
        let bytes = encode(&todo)?;
        if todo.status == TodoStatus::Blocked {
            if let TodoScope::Campaign { campaign_id } = &todo.scope {
                super::campaign_oversight::trigger_in(&tx, campaign_id, "blocked_state")
                    .map_err(storage)?;
            }
        }
        tx.open_table(RECORDS)
            .map_err(storage)?
            .insert((key.as_str(), todo.id.as_str()), bytes.as_slice())
            .map_err(storage)?;
        tx.open_table(ORDER)
            .map_err(storage)?
            .insert((key.as_str(), todo.order_key, todo.id.as_str()), ())
            .map_err(storage)?;
        tx.open_table(SCOPES)
            .map_err(storage)?
            .insert(key.as_str(), next_revision)
            .map_err(storage)?;
        let watermark =
            feed::watermark(&tx.open_table(feed::METADATA).map_err(storage)?).map_err(storage)?;
        let watermark = feed::append(
            &tx,
            OperationalEvent {
                schema_version: 1,
                watermark,
                scope: self.scope.clone(),
                scope_revision: next_revision,
                occurred_at_ms: todo.updated_ms,
                change: if added {
                    OperationalChange::TodoAdded { todo: todo.clone() }
                } else {
                    OperationalChange::TodoUpdated { todo: todo.clone() }
                },
            },
        )
        .map_err(storage)?;
        let response = TodoResponse::Mutation {
            todo,
            scope_revision: next_revision,
            watermark,
        };
        let bytes = encode(&Receipt {
            schema_version: 1,
            actor: self.actor.clone(),
            request: request.clone(),
            response: response.clone(),
        })?;
        tx.open_table(RECEIPTS)
            .map_err(storage)?
            .insert(command_id.as_str(), bytes.as_slice())
            .map_err(storage)?;
        tx.commit().map_err(storage)?;
        if let TodoResponse::Mutation {
            todo,
            scope_revision,
            watermark,
        } = &response
        {
            self.store.operational_committed(OperationalEvent {
                schema_version: 1,
                watermark: watermark.clone(),
                scope: todo.scope.clone(),
                scope_revision: *scope_revision,
                occurred_at_ms: todo.updated_ms,
                change: if added {
                    OperationalChange::TodoAdded { todo: todo.clone() }
                } else {
                    OperationalChange::TodoUpdated { todo: todo.clone() }
                },
            });
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;
    use std::sync::{Arc, Barrier};

    fn scope() -> TodoScope {
        TodoScope::Conversation {
            id: "ordinary-chat".into(),
        }
    }
    fn bound(store: &RuntimeStore, scope: TodoScope) -> TodoFacade<'_> {
        store
            .todos(TodoAuthority::Bound {
                scope,
                actor: TodoActor {
                    source: "foreground".into(),
                    actor: "host-user".into(),
                },
            })
            .unwrap()
    }
    fn add(command: &str, revision: u64) -> TodoRequest {
        TodoRequest::Add {
            scope: scope(),
            command_id: command.into(),
            expected_revision: revision,
            title: "A durable task".into(),
            description: String::new(),
        }
    }
    fn update(id: &str, command: &str, revision: u64) -> TodoRequest {
        TodoRequest::Update {
            scope: scope(),
            command_id: command.into(),
            id: id.into(),
            expected_revision: revision,
            title: None,
            description: None,
            status: Some(TodoStatus::Completed),
        }
    }
    fn list(cursor: Option<TodoCursor>) -> TodoRequest {
        TodoRequest::List {
            scope: scope(),
            filter: TodoFilter::default(),
            limit: Some(1),
            cursor,
        }
    }
    fn changed(response: TodoResponse) -> Todo {
        let TodoResponse::Mutation { todo, .. } = response else {
            panic!()
        };
        todo
    }
    fn counts(store: &RuntimeStore) -> (u64, u64, u64, u64) {
        let tx = store.database.begin_read().unwrap();
        (
            tx.open_table(RECORDS).unwrap().len().unwrap(),
            tx.open_table(ORDER).unwrap().len().unwrap(),
            tx.open_table(RECEIPTS).unwrap().len().unwrap(),
            tx.open_table(feed::EVENTS).unwrap().len().unwrap(),
        )
    }

    #[test]
    fn reopen_replays_before_revision_checks_and_conflicts_are_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let (first, second, id) = {
            let store = RuntimeStore::open(&path).unwrap();
            let facade = bound(&store, scope());
            let first = facade.execute(add("add", 0)).unwrap();
            let id = changed(first.clone()).id;
            uuid::Uuid::parse_str(&id).unwrap();
            let second = facade.execute(update(&id, "update", 1)).unwrap();
            (first, second, id)
        };
        let store = RuntimeStore::open(&path).unwrap();
        let facade = bound(&store, scope());
        assert_eq!(facade.execute(add("add", 0)).unwrap(), first);
        assert_eq!(facade.execute(update(&id, "update", 1)).unwrap(), second);
        let other_actor = store
            .todos(TodoAuthority::Bound {
                scope: scope(),
                actor: TodoActor {
                    source: "foreground".into(),
                    actor: "other-user".into(),
                },
            })
            .unwrap();
        assert_eq!(
            other_actor.execute(add("add", 0)),
            Err(TodoError::CommandConflict)
        );
        assert_eq!(
            facade.execute(add("add", 2)),
            Err(TodoError::CommandConflict)
        );
        assert_eq!(
            facade.execute(update(&id, "stale", 1)),
            Err(TodoError::RevisionConflict {
                current_revision: 2
            })
        );
        assert_eq!(counts(&store), (1, 1, 2, 2));
        let TodoResponse::List {
            todos,
            scope_revision,
            watermark,
            ..
        } = facade.execute(list(None)).unwrap()
        else {
            panic!()
        };
        assert_eq!(scope_revision, 2);
        assert_eq!(watermark.sequence, 2);
        assert_eq!(todos, vec![changed(second)]);
        assert_eq!(todos[0].created_by.source, "foreground");
        let tx = store.database.begin_read().unwrap();
        let events = tx.open_table(feed::EVENTS).unwrap();
        let event: OperationalEvent = decode(events.get(2).unwrap().unwrap().value()).unwrap();
        assert_eq!(event.watermark, watermark);
        assert_eq!(event.scope_revision, scope_revision);
        assert_eq!(
            event.change,
            OperationalChange::TodoUpdated {
                todo: todos[0].clone()
            }
        );
    }

    #[test]
    fn concurrent_updates_have_one_winner_and_no_partial_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let id = changed(bound(&store, scope()).execute(add("add", 0)).unwrap()).id;
        let barrier = Arc::new(Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|i| {
                let store = store.clone();
                let id = id.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let facade = bound(&store, scope());
                    barrier.wait();
                    facade.execute(update(&id, &format!("update-{i}"), 1))
                })
            })
            .collect();
        let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| **r
                    == Err(TodoError::RevisionConflict {
                        current_revision: 2
                    }))
                .count(),
            1
        );
        assert_eq!(counts(&store), (1, 1, 2, 2));
    }

    #[test]
    fn cursor_keysets_bind_generation_filter_scope_and_database() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let facade = bound(&store, scope());
        let a = changed(facade.execute(add("a", 0)).unwrap());
        let b = changed(facade.execute(add("b", 1)).unwrap());
        let TodoResponse::List {
            todos,
            next_cursor: Some(cursor),
            scope_revision,
            watermark,
        } = facade.execute(list(None)).unwrap()
        else {
            panic!()
        };
        assert_eq!(todos, vec![a.clone()]);
        assert_eq!((scope_revision, watermark.sequence), (2, 2));
        let TodoResponse::List {
            todos, next_cursor, ..
        } = facade.execute(list(Some(cursor.clone()))).unwrap()
        else {
            panic!()
        };
        assert_eq!(todos, vec![b.clone()]);
        assert!(next_cursor.is_none());
        let mut wrong = cursor.clone();
        wrong.filter.status = Some(TodoStatus::Pending);
        assert!(matches!(
            facade.execute(list(Some(wrong))),
            Err(TodoError::Invalid { .. })
        ));
        let mut wrong = cursor.clone();
        wrong.scope = TodoScope::Conversation { id: "other".into() };
        assert!(matches!(
            facade.execute(list(Some(wrong))),
            Err(TodoError::Invalid { .. })
        ));
        let mut wrong = cursor.clone();
        wrong.instance_id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            facade.execute(list(Some(wrong))),
            Err(TodoError::CursorStale)
        );
        facade.execute(update(&a.id, "update", 1)).unwrap();
        assert_eq!(
            facade.execute(list(Some(cursor))),
            Err(TodoError::CursorStale)
        );
        let TodoResponse::List { todos, .. } = facade
            .execute(TodoRequest::List {
                scope: scope(),
                filter: TodoFilter {
                    ids: Some(vec![b.id.clone(), "missing".into()]),
                    status: None,
                },
                limit: None,
                cursor: None,
            })
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(todos, vec![b]);
        assert!(matches!(
            facade.execute(TodoRequest::List {
                scope: scope(),
                filter: TodoFilter::default(),
                limit: Some(101),
                cursor: None
            }),
            Err(TodoError::Invalid { .. })
        ));
    }

    #[test]
    fn scope_authority_and_exact_ids_never_leak_across_conversations() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = changed(bound(&store, scope()).execute(add("a", 0)).unwrap()).id;
        let other = TodoScope::Conversation { id: "other".into() };
        let facade = bound(&store, other.clone());
        assert_eq!(facade.execute(list(None)), Err(TodoError::AuthorityDenied));
        let mut request = update(&id, "other-update", 1);
        if let TodoRequest::Update { scope, .. } = &mut request {
            *scope = other.clone();
        }
        assert_eq!(facade.execute(request), Err(TodoError::NotFound));
        let TodoResponse::List {
            todos,
            scope_revision,
            ..
        } = facade
            .execute(TodoRequest::List {
                scope: other,
                filter: TodoFilter {
                    ids: Some(vec![id]),
                    status: None,
                },
                limit: None,
                cursor: None,
            })
            .unwrap()
        else {
            panic!()
        };
        assert!(todos.is_empty());
        assert_eq!(scope_revision, 0);
        assert_eq!(counts(&store), (1, 1, 1, 1));
        for scope in [
            TodoScope::Work {
                work_id: "missing".into(),
            },
            TodoScope::Campaign {
                campaign_id: "missing".into(),
            },
        ] {
            assert!(matches!(
                store.todos(TodoAuthority::Bound {
                    scope,
                    actor: TodoActor {
                        source: "operator".into(),
                        actor: "local".into()
                    }
                }),
                Err(TodoError::AuthorityDenied)
            ));
        }
    }

    #[test]
    fn ghost_requires_exact_persisted_work_campaign_and_explicit_scope() {
        use super::super::admission::Admission;
        use super::super::campaign_ledger::{Envelope, Pool, Units};
        use tachyon_api::{ApiRequest, ApiResponse};
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: "r".into(),
                title: "r".into(),
                objective: "r".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: "c".into(),
                research_id: research.id,
                title: "c".into(),
                objective: "c".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        store
            .host_authorize_campaign_envelope(
                "grant",
                &campaign.id,
                Envelope {
                    work: Units {
                        tokens: 100,
                        cost_micro_usd: 100,
                    },
                    verification: Units::default(),
                    max_active_inferences: 2,
                },
            )
            .unwrap();
        store
            .admit_campaign_work(Admission {
                work_id: "work".into(),
                campaign_id: campaign.id.clone(),
                objective: "test".into(),
                instruction_revision: 1,
                generation: 1,
                pool: Pool::Work,
                upper_bound: Units {
                    tokens: 1,
                    cost_micro_usd: 1,
                },
            })
            .unwrap();
        let work_scope = TodoScope::Work {
            work_id: "work".into(),
        };
        let ghost = |scope, work_id: &str, campaign_id: &str| TodoAuthority::Ghost {
            scope,
            work_id: work_id.into(),
            campaign_id: campaign_id.into(),
        };
        let facade = store
            .todos(ghost(work_scope.clone(), "work", &campaign.id))
            .unwrap();
        assert_eq!(
            facade.execute(TodoRequest::List {
                scope: TodoScope::Campaign {
                    campaign_id: campaign.id.clone()
                },
                filter: TodoFilter::default(),
                limit: None,
                cursor: None
            }),
            Err(TodoError::AuthorityDenied)
        );
        for authority in [
            ghost(work_scope.clone(), "work", "wrong"),
            ghost(work_scope, "wrong", &campaign.id),
            ghost(scope(), "work", &campaign.id),
            ghost(
                TodoScope::Work {
                    work_id: "other".into(),
                },
                "work",
                &campaign.id,
            ),
        ] {
            assert!(matches!(
                store.todos(authority),
                Err(TodoError::AuthorityDenied)
            ));
        }
        assert!(store
            .todos(ghost(
                TodoScope::Campaign {
                    campaign_id: campaign.id.clone()
                },
                "work",
                &campaign.id
            ))
            .is_ok());
    }

    #[test]
    fn additive_schema_and_future_record_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        // Existing schema v1 database with none of the new additive tables.
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            tx.open_table(super::super::METADATA)
                .unwrap()
                .insert(super::super::SCHEMA_VERSION_KEY, 1)
                .unwrap();
            tx.commit().unwrap();
        }
        let store = RuntimeStore::open(&path).unwrap();
        let facade = bound(&store, scope());
        let TodoResponse::List {
            scope_revision,
            watermark,
            ..
        } = facade.execute(list(None)).unwrap()
        else {
            panic!()
        };
        assert_eq!((scope_revision, watermark.sequence), (0, 0));
        let mut todo = changed(facade.execute(add("a", 0)).unwrap());
        todo.schema_version = 2;
        let tx = store.database.begin_write().unwrap();
        let key = scope_key(&scope()).unwrap();
        tx.open_table(RECORDS)
            .unwrap()
            .insert(
                (key.as_str(), todo.id.as_str()),
                encode(&todo).unwrap().as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
        assert!(matches!(
            facade.execute(list(None)),
            Err(TodoError::Storage { .. })
        ));
        assert!(matches!(
            facade.execute(update(&todo.id, "bad", 1)),
            Err(TodoError::Storage { .. })
        ));
        assert_eq!(counts(&store), (1, 1, 1, 1));
        drop(facade);
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        let tx = store.database.begin_read().unwrap();
        assert_eq!(
            feed::watermark(&tx.open_table(feed::METADATA).unwrap())
                .unwrap()
                .instance_id,
            watermark.instance_id
        );
        assert_eq!(
            tx.open_table(super::super::METADATA)
                .unwrap()
                .get(super::super::SCHEMA_VERSION_KEY)
                .unwrap()
                .unwrap()
                .value(),
            1
        );
        drop(tx);
        let tx = store.database.begin_write().unwrap();
        tx.open_table(super::super::METADATA)
            .unwrap()
            .insert(super::super::SCHEMA_VERSION_KEY, 2)
            .unwrap();
        tx.commit().unwrap();
        drop(store);
        assert!(RuntimeStore::open(&path).is_err());
    }

    #[test]
    fn late_failure_rolls_back_record_index_scope_event_and_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let tx = store.database.begin_write().unwrap();
        let mut table = tx.open_table(feed::METADATA).unwrap();
        let mut watermark = feed::watermark(&table).unwrap();
        watermark.sequence = u64::MAX;
        table
            .insert("operational_v1", encode(&watermark).unwrap().as_slice())
            .unwrap();
        drop(table);
        tx.commit().unwrap();
        let facade = bound(&store, scope());
        assert!(matches!(
            facade.execute(add("late-failure", 0)),
            Err(TodoError::Storage { .. })
        ));
        assert_eq!(counts(&store), (0, 0, 0, 0));
        let TodoResponse::List {
            todos,
            scope_revision,
            watermark: actual,
            ..
        } = facade.execute(list(None)).unwrap()
        else {
            panic!()
        };
        assert!(todos.is_empty());
        assert_eq!(scope_revision, 0);
        assert_eq!(actual, watermark);
    }

    #[test]
    fn initial_database_and_snapshot_generation_are_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        // Legacy database has data but no schema marker yet.
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            tx.open_table(super::super::METADATA)
                .unwrap()
                .insert("legacy_marker", 7)
                .unwrap();
            tx.commit().unwrap();
        }
        let store = Arc::new(RuntimeStore::open(&path).unwrap());
        let tx = store.database.begin_read().unwrap();
        assert_eq!(
            tx.open_table(super::super::METADATA)
                .unwrap()
                .get("legacy_marker")
                .unwrap()
                .unwrap()
                .value(),
            7
        );
        drop(tx);
        let barrier = Arc::new(Barrier::new(2));
        let writer = {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let facade = bound(&store, scope());
                barrier.wait();
                for revision in 0..20 {
                    facade
                        .execute(add(&format!("a-{revision}"), revision))
                        .unwrap();
                }
            })
        };
        let facade = bound(&store, scope());
        barrier.wait();
        for _ in 0..50 {
            let TodoResponse::List {
                todos,
                scope_revision,
                watermark,
                ..
            } = facade
                .execute(TodoRequest::List {
                    scope: scope(),
                    filter: TodoFilter::default(),
                    limit: Some(100),
                    cursor: None,
                })
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(todos.len() as u64, scope_revision);
            assert_eq!(scope_revision, watermark.sequence);
            assert!(todos.iter().all(|t| t.order_key <= scope_revision));
        }
        writer.join().unwrap();
        assert_eq!(counts(&store), (20, 20, 20, 20));
    }
}
