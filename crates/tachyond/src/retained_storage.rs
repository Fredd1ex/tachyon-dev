//! Host-owned logical retained-byte admission. No transaction spans two stores.
use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const REFS: TableDefinition<(&str, &str, &str), &[u8]> = TableDefinition::new("retained_refs_v1");
const LIMITS: TableDefinition<&str, u64> = TableDefinition::new("retained_limits_v1");
pub const DEFAULT_LIMIT: u64 = 64 * 1024 * 1024 * 1024;
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

#[derive(Clone)]
pub struct RetainedStorage {
    database: Arc<Database>,
    maximum: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_limits_and_debt_survive_changed_root_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let ledger = RetainedStorage::new(Arc::new(Database::create(&path).unwrap()), 100).unwrap();
        ledger.configure("old-default", None).unwrap();
        ledger.configure("explicit", Some(20)).unwrap();
        ledger
            .reserve("explicit", "artifact", "a", 10, "hash")
            .unwrap();
        drop(ledger);
        let ledger = RetainedStorage::new(Arc::new(Database::create(&path).unwrap()), 5).unwrap();
        ledger.configure("old-default", None).unwrap();
        assert!(ledger.configure("explicit", None).is_err());
        assert!(ledger.configure("explicit", Some(21)).is_err());
        assert_eq!(
            ledger.summary("old-default").unwrap()["campaign_limit_bytes"],
            DEFAULT_LIMIT
        );
        assert_eq!(
            ledger.summary("explicit").unwrap()["campaign_limit_bytes"],
            20
        );
        assert_eq!(ledger.summary("explicit").unwrap()["root_debt_bytes"], 5);
        assert!(ledger
            .reserve("old-default", "trace", "b", 1, "hash")
            .is_err());
    }

    #[test]
    fn retained_overflow_debt_idempotency_and_ready_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = RetainedStorage::new(
            Arc::new(Database::create(dir.path().join("runtime.redb")).unwrap()),
            u64::MAX,
        )
        .unwrap();
        ledger.configure("campaign", Some(u64::MAX)).unwrap();
        let r = ledger
            .reserve("campaign", "artifact", "a", u64::MAX, "immutable")
            .unwrap();
        assert!(ledger
            .reserve("campaign", "trace", "b", 1, "other")
            .is_err());
        assert!(ledger.adopt("campaign", "trace", "b", 1, "other").is_err());
        ledger
            .reserve("campaign", "artifact", "a", u64::MAX, "immutable")
            .unwrap();
        assert!(ledger
            .reserve("campaign", "artifact", "a", 1, "immutable")
            .is_err());
        assert!(ledger
            .reserve("campaign", "artifact", "a", u64::MAX, "changed")
            .is_err());
        ledger.ready(&r, 3).unwrap();
        ledger.ready(&r, 3).unwrap();
        assert!(ledger.ready(&r, 2).is_err());
        assert_eq!(ledger.summary("campaign").unwrap()["root_charged_bytes"], 3);
        ledger
            .reserve("campaign", "trace", "b", 1, "other")
            .unwrap();
        assert_eq!(ledger.summary("campaign").unwrap()["root_charged_bytes"], 4);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub campaign: String,
    pub kind: String,
    pub id: String,
    pub expected: u64,
    pub identity: String,
    pub ready: Option<u64>,
}

impl RetainedStorage {
    /// Uses the caller's runtime read snapshot, never opens a second transaction.
    pub fn monitor_in(
        &self,
        tx: &redb::ReadTransaction,
        scopes: &[tachyon_api::monitor::MonitorScope],
    ) -> Result<Vec<tachyon_api::monitor::Observed<tachyon_api::monitor::Storage>>, String> {
        use tachyon_api::monitor::*;
        let mut output: Vec<_> = scopes
            .iter()
            .map(|scope| {
                if matches!(scope, MonitorScope::Work { .. }) {
                    Observed::Unknown
                } else {
                    Observed::Known(Storage::default())
                }
            })
            .collect();
        let limits = tx.open_table(LIMITS).map_err(err)?;
        for (scope, output) in scopes.iter().zip(&mut output) {
            if let Observed::Known(output) = output {
                output.limit_bytes = match scope {
                    MonitorScope::Host => Observed::Known(Decimal(self.maximum.into())),
                    MonitorScope::Campaign { campaign_id } => limits
                        .get(campaign_id.as_str())
                        .map_err(err)?
                        .map_or(Observed::Unknown, |v| {
                            Observed::Known(Decimal(v.value().into()))
                        }),
                    MonitorScope::Work { .. } => Observed::Unknown,
                };
            }
        }
        let table = tx.open_table(REFS).map_err(err)?;
        for row in table.iter().map_err(err)? {
            let (key, value) = row.map_err(err)?;
            let receipt: Receipt = serde_json::from_slice(value.value()).map_err(err)?;
            if key.value()
                != (
                    receipt.campaign.as_str(),
                    receipt.kind.as_str(),
                    receipt.id.as_str(),
                )
            {
                return Err("invalid retained receipt identity".into());
            }
            for (scope, output) in scopes.iter().zip(&mut output) {
                if !scope.matches(&receipt.campaign, None) {
                    continue;
                }
                let Observed::Known(output) = output else {
                    continue;
                };
                let (target, bytes) = match receipt.ready {
                    Some(n) => (&mut output.ready_bytes, n),
                    None => (&mut output.reserved_bytes, receipt.expected),
                };
                target.0 = target
                    .0
                    .checked_add(bytes.into())
                    .ok_or("monitor storage overflow")?;
            }
        }
        Ok(output)
    }
    pub fn same_authority(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.database, &other.database) && self.maximum == other.maximum
    }

    pub fn get(&self, campaign: &str, kind: &str, id: &str) -> Result<Option<Receipt>, String> {
        let tx = self.database.begin_read().map_err(err)?;
        let table = tx.open_table(REFS).map_err(err)?;
        table
            .get((campaign, kind, id))
            .map_err(err)?
            .map(|v| serde_json::from_slice(v.value()).map_err(err))
            .transpose()
    }
    pub fn new(database: Arc<Database>, maximum: u64) -> Result<Self, String> {
        if maximum == 0 {
            return Err("retained storage maximum must be positive".into());
        }
        let tx = database.begin_write().map_err(err)?;
        tx.open_table(REFS).map_err(err)?;
        tx.open_table(LIMITS).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(Self { database, maximum })
    }

    pub fn configure(&self, campaign: &str, limit: Option<u64>) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        self.configure_in(&tx, campaign, limit)?;
        tx.commit().map_err(err)
    }

    pub fn configure_in(
        &self,
        tx: &WriteTransaction,
        campaign: &str,
        limit: Option<u64>,
    ) -> Result<(), String> {
        let limit = limit.unwrap_or(DEFAULT_LIMIT);
        if limit == 0 {
            return Err("retained storage limit must be positive".into());
        }
        {
            let mut table = tx.open_table(LIMITS).map_err(err)?;
            let old = table.get(campaign).map_err(err)?.map(|v| v.value());
            if old.is_some_and(|v| v != limit) {
                return Err("immutable retained storage limit conflict".into());
            }
            table.insert(campaign, limit).map_err(err)?;
        }
        Ok(())
    }

    pub fn summary(&self, campaign: &str) -> Result<serde_json::Value, String> {
        let tx = self.database.begin_read().map_err(err)?;
        let table = tx.open_table(REFS).map_err(err)?;
        let mut root = 0u64;
        let mut local = 0u64;
        let mut unresolved = 0u64;
        for row in table.iter().map_err(err)? {
            let (key, value) = row.map_err(err)?;
            let r: Receipt = serde_json::from_slice(value.value()).map_err(err)?;
            let bytes = r.ready.unwrap_or(r.expected);
            root = root.checked_add(bytes).ok_or("retained storage overflow")?;
            if key.value().0 == campaign {
                local = local
                    .checked_add(bytes)
                    .ok_or("retained storage overflow")?;
                if r.ready.is_none() {
                    unresolved = unresolved
                        .checked_add(bytes)
                        .ok_or("retained storage overflow")?;
                }
            }
        }
        let limits = tx.open_table(LIMITS).map_err(err)?;
        let limit = limits
            .get(campaign)
            .map_err(err)?
            .map(|v| v.value())
            .unwrap_or(DEFAULT_LIMIT);
        Ok(
            serde_json::json!({"scope":"retained artifacts, traces and managed input snapshots; not whole filesystem",
            "root_limit_bytes":self.maximum,"root_charged_bytes":root,"campaign_limit_bytes":limit,
            "campaign_charged_bytes":local,"campaign_unresolved_bytes":unresolved,
            "root_debt_bytes":root.saturating_sub(self.maximum),"campaign_debt_bytes":local.saturating_sub(limit)}),
        )
    }

    pub fn reserve(
        &self,
        campaign: &str,
        kind: &str,
        id: &str,
        expected: u64,
        identity: &str,
    ) -> Result<Receipt, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let receipt = self.reserve_in(&tx, campaign, kind, id, expected, identity, false)?;
        tx.commit().map_err(err)?;
        Ok(receipt)
    }

    /// Census imports debt even above caps. Only trusted startup metadata calls this.
    pub fn adopt(
        &self,
        campaign: &str,
        kind: &str,
        id: &str,
        expected: u64,
        identity: &str,
    ) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        self.reserve_in(&tx, campaign, kind, id, expected, identity, true)?;
        tx.commit().map_err(err)
    }

    pub fn reserve_in(
        &self,
        tx: &WriteTransaction,
        campaign: &str,
        kind: &str,
        id: &str,
        expected: u64,
        identity: &str,
        adopt: bool,
    ) -> Result<Receipt, String> {
        let mut table = tx.open_table(REFS).map_err(err)?;
        if let Some(row) = table.get((campaign, kind, id)).map_err(err)? {
            let old: Receipt = serde_json::from_slice(row.value()).map_err(err)?;
            if old.expected != expected || old.identity != identity {
                return Err("retained reference conflict".into());
            }
            return Ok(old);
        }
        let mut root = 0u64;
        let mut local = 0u64;
        for row in table.iter().map_err(err)? {
            let (key, value) = row.map_err(err)?;
            let r: Receipt = serde_json::from_slice(value.value()).map_err(err)?;
            let bytes = r.ready.unwrap_or(r.expected);
            root = root.checked_add(bytes).ok_or("retained storage overflow")?;
            if key.value().0 == campaign {
                local = local
                    .checked_add(bytes)
                    .ok_or("retained storage overflow")?;
            }
        }
        let root = root
            .checked_add(expected)
            .ok_or("retained storage overflow")?;
        let local = local
            .checked_add(expected)
            .ok_or("retained storage overflow")?;
        let limits = tx.open_table(LIMITS).map_err(err)?;
        let limit = limits
            .get(campaign)
            .map_err(err)?
            .map(|v| v.value())
            .unwrap_or(DEFAULT_LIMIT);
        if !adopt && (root > self.maximum || local > limit) {
            return Err(
                "retained storage capacity exhausted; unresolved reservations remain charged"
                    .into(),
            );
        }
        let receipt = Receipt {
            campaign: campaign.into(),
            kind: kind.into(),
            id: id.into(),
            expected,
            identity: identity.into(),
            ready: None,
        };
        table
            .insert(
                (campaign, kind, id),
                serde_json::to_vec(&receipt).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        Ok(receipt)
    }

    /// Call only after the individual resource's immutable metadata is published.
    pub fn ready(&self, receipt: &Receipt, actual: u64) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        self.ready_in(&tx, receipt, actual)?;
        tx.commit().map_err(err)
    }

    pub fn ready_in(
        &self,
        tx: &WriteTransaction,
        receipt: &Receipt,
        actual: u64,
    ) -> Result<(), String> {
        let mut table = tx.open_table(REFS).map_err(err)?;
        let key = (
            receipt.campaign.as_str(),
            receipt.kind.as_str(),
            receipt.id.as_str(),
        );
        let mut old: Receipt = serde_json::from_slice(
            table
                .get(key)
                .map_err(err)?
                .ok_or("missing retained reservation")?
                .value(),
        )
        .map_err(err)?;
        if old.identity != receipt.identity
            || old.expected != receipt.expected
            || actual > old.expected
            || old.ready.is_some_and(|n| n != actual)
        {
            return Err("retained ready reconciliation conflict".into());
        }
        old.ready = Some(actual);
        table
            .insert(key, serde_json::to_vec(&old).map_err(err)?.as_slice())
            .map_err(err)?;
        Ok(())
    }
}
