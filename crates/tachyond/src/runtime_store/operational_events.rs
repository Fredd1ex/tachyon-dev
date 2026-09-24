//! Shared transaction seam: append in the mutation transaction, never after commit.
#![allow(dead_code)]
use redb::{ReadableTable, TableDefinition, WriteTransaction};
use tachyon_api::operational_events::{OperationalEvent, OperationalWatermark};

pub(super) const EVENTS: TableDefinition<u64, &[u8]> =
    TableDefinition::new("operational_events_v1");
pub(super) const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("feed_metadata");
const KEY: &str = "operational_v1";

pub(crate) type Subscriber = std::sync::Arc<dyn Fn(&OperationalEvent) + Send + Sync>;

impl super::RuntimeStore {
    pub(crate) fn subscribe_operational(&self, subscriber: Subscriber) {
        self.operational_subscribers
            .lock()
            .unwrap()
            .push(subscriber);
    }

    /// Only after the durable mutation commits. Never call subscribers under a
    /// redb write transaction or the subscriber-list mutex.
    pub(super) fn operational_committed(&self, event: OperationalEvent) {
        let subscribers = self.operational_subscribers.lock().unwrap().clone();
        for subscriber in subscribers {
            subscriber(&event);
        }
    }
}

pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    tx.open_table(EVENTS).map_err(err)?;
    let mut table = tx.open_table(METADATA).map_err(err)?;
    if table.get(KEY).map_err(err)?.is_none() {
        let bytes = serde_json::to_vec(&OperationalWatermark {
            instance_id: uuid::Uuid::new_v4().to_string(),
            sequence: 0,
        })
        .map_err(err)?;
        table.insert(KEY, bytes.as_slice()).map_err(err)?;
    }
    Ok(())
}

fn err(e: impl std::fmt::Display) -> String {
    format!("operational feed: {e}")
}

impl super::todo::TodoFacade<'_> {
    pub(crate) fn operational_batch(
        &self,
        after: &OperationalWatermark,
    ) -> Result<tachyon_api::operational_events::OperationalBatch, tachyon_api::todo::TodoError>
    {
        use tachyon_api::{operational_events::OperationalBatch, todo::TodoError};
        let storage = |message: String| TodoError::Storage { message };
        let tx = self
            .store
            .database
            .begin_read()
            .map_err(|e| storage(err(e)))?;
        let current =
            watermark(&tx.open_table(METADATA).map_err(|e| storage(err(e)))?).map_err(storage)?;
        if after.instance_id != current.instance_id || after.sequence > current.sequence {
            return Err(TodoError::CursorStale);
        }
        let mut batch = OperationalBatch {
            events: Vec::new(),
            watermark: after.clone(),
        };
        if after.sequence == current.sequence {
            return Ok(batch);
        }
        let table = tx.open_table(EVENTS).map_err(|e| storage(err(e)))?;
        // Bound scanned rows, not just matching events, so unrelated scopes cannot
        // turn one poll into an unbounded scan. Empty batches still advance.
        for row in table
            .range((after.sequence + 1)..=current.sequence)
            .map_err(|e| storage(err(e)))?
            .take(100)
        {
            let (sequence, bytes) = row.map_err(|e| storage(err(e)))?;
            let event: OperationalEvent =
                serde_json::from_slice(bytes.value()).map_err(|e| storage(err(e)))?;
            if event.schema_version != 1
                || event.watermark.instance_id != current.instance_id
                || event.watermark.sequence != sequence.value()
                || sequence.value() != batch.watermark.sequence + 1
            {
                return Err(storage(err("invalid durable event")));
            }
            batch.watermark = event.watermark.clone();
            if event.scope == self.scope {
                batch.events.push(event);
            }
        }
        if batch.watermark.sequence == after.sequence {
            return Err(storage(err("missing durable event")));
        }
        Ok(batch)
    }
}

pub(super) fn watermark(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<OperationalWatermark, String> {
    let row = table
        .get(KEY)
        .map_err(err)?
        .ok_or("missing feed identity")?;
    let value: OperationalWatermark = serde_json::from_slice(row.value()).map_err(err)?;
    uuid::Uuid::parse_str(&value.instance_id).map_err(err)?;
    Ok(value)
}

/// Assigns the next durable sequence and updates the shared watermark atomically.
pub(super) fn append(
    tx: &WriteTransaction,
    mut event: OperationalEvent,
) -> Result<OperationalWatermark, String> {
    let mut metadata = tx.open_table(METADATA).map_err(err)?;
    let mut mark = watermark(&metadata)?;
    mark.sequence = mark
        .sequence
        .checked_add(1)
        .ok_or("operational sequence exhausted")?;
    event.watermark = mark.clone();
    let bytes = serde_json::to_vec(&event).map_err(err)?;
    tx.open_table(EVENTS)
        .map_err(err)?
        .insert(mark.sequence, bytes.as_slice())
        .map_err(err)?;
    let bytes = serde_json::to_vec(&mark).map_err(err)?;
    metadata.insert(KEY, bytes.as_slice()).map_err(err)?;
    Ok(mark)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_store::{todo::TodoAuthority, RuntimeStore};
    use tachyon_api::todo::*;

    #[test]
    fn durable_replay_is_bounded_scope_filtered_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let scope = TodoScope::Conversation {
            id: "selected".into(),
        };
        let authority = |scope| TodoAuthority::Bound {
            scope,
            actor: TodoActor {
                source: "operator".into(),
                actor: "uid:1".into(),
            },
        };
        let initial;
        {
            let store = RuntimeStore::open(&path).unwrap();
            let tx = store.database.begin_read().unwrap();
            initial = watermark(&tx.open_table(METADATA).unwrap()).unwrap();
            drop(tx);
            let other = TodoScope::Conversation { id: "other".into() };
            let facade = store.todos(authority(other.clone())).unwrap();
            for i in 0..101 {
                facade
                    .execute(TodoRequest::Add {
                        scope: other.clone(),
                        command_id: format!("other-{i}"),
                        expected_revision: i,
                        title: "other".into(),
                        description: String::new(),
                    })
                    .unwrap();
            }
            store
                .todos(authority(scope.clone()))
                .unwrap()
                .execute(TodoRequest::Add {
                    scope: scope.clone(),
                    command_id: "selected".into(),
                    expected_revision: 0,
                    title: "selected".into(),
                    description: String::new(),
                })
                .unwrap();
        }
        let store = RuntimeStore::open(&path).unwrap();
        let facade = store.todos(authority(scope.clone())).unwrap();
        let first = facade.operational_batch(&initial).unwrap();
        assert!(first.events.is_empty());
        assert_eq!(first.watermark.sequence, 100);
        let second = facade.operational_batch(&first.watermark).unwrap();
        assert_eq!(second.watermark.sequence, 102);
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].scope, scope);
        let idle = facade.operational_batch(&second.watermark).unwrap();
        assert!(idle.events.is_empty());
        assert_eq!(idle.watermark, second.watermark);
        let mut future = second.watermark;
        future.sequence += 1;
        assert_eq!(
            facade.operational_batch(&future),
            Err(TodoError::CursorStale)
        );
    }
}
