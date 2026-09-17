//! Projection only: one durable read transaction for all requested scopes per tick.
use super::{admission, campaign_ledger, compute, research, RuntimeStore};
use tachyon_api::monitor::*;

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(super) fn add(value: &mut Decimal, n: u128) -> Result<(), String> {
    value.0 = value.0.checked_add(n).ok_or("monitor counter overflow")?;
    Ok(())
}

/// Keep only a bounded ordered page, even when scanning a large registry/table.
pub(crate) fn entry(page: &mut Registered, query: &MonitorQuery, row: RegisteredEntry) {
    page.total.0 += 1;
    if query.after.as_ref().is_some_and(|after| row.id <= *after) {
        return;
    }
    let index = page.entries.partition_point(|e| e.id < row.id);
    if index <= query.limit {
        page.entries.insert(index, row);
        page.entries.truncate(query.limit + 1);
    }
}
pub(crate) fn finish_page(page: &mut Registered, query: &MonitorQuery) {
    if page.entries.len() > query.limit {
        page.entries.truncate(query.limit);
        page.next_after = page.entries.last().map(|e| e.id.clone());
    }
}

impl RuntimeStore {
    pub(crate) fn monitor_sample(
        &self,
        queries: &[MonitorQuery],
    ) -> Result<Vec<Result<MonitorPayload, MonitorError>>, String> {
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        let sampled_at_ms = now_ms();
        let campaigns = tx
            .open_table(research::CAMPAIGNS)
            .map_err(|e| e.to_string())?;
        let mut output = Vec::with_capacity(queries.len());
        let mut allocations = Vec::with_capacity(queries.len());
        for query in queries {
            let campaign = match &query.scope {
                MonitorScope::Host => None,
                MonitorScope::Campaign { campaign_id } | MonitorScope::Work { campaign_id, .. } => {
                    Some(campaign_id)
                }
            };
            let exists = match campaign {
                Some(id) => campaigns
                    .get(id.as_str())
                    .map_err(|e| e.to_string())?
                    .is_some(),
                None => true,
            };
            let allocation = admission::monitor_allocation(&tx, &query.scope)?;
            let exists = exists
                && (!matches!(query.scope, MonitorScope::Work { .. }) || allocation.is_some());
            allocations.push(allocation);
            output.push(if exists {
                Ok(MonitorPayload {
                    durable: Durable {
                        sampled_at_ms,
                        ..Default::default()
                    },
                    capacities: Vec::new(),
                    registered: Registered {
                        sampled_at_ms,
                        ..Default::default()
                    },
                })
            } else {
                Err(MonitorError::NotFound)
            });
        }
        campaign_ledger::monitor_in(&tx, queries, &allocations, &mut output)?;
        compute::monitor_in(&tx, queries, &mut output)?;
        admission::monitor_in(&tx, queries, &mut output)?;
        let scopes: Vec<_> = queries.iter().map(|q| q.scope.clone()).collect();
        let storage = self.retained.monitor_in(&tx, &scopes)?;
        for ((query, payload), storage) in queries.iter().zip(&mut output).zip(storage) {
            if let Ok(payload) = payload {
                payload.durable.retained_storage = storage;
                if !matches!(query.scope, MonitorScope::Host) {
                    finish_page(&mut payload.registered, query);
                }
            }
        }
        drop(tx);
        // These clocks deliberately follow the read snapshot; no global atomicity is claimed.
        if queries
            .iter()
            .any(|q| matches!(q.scope, MonitorScope::Host))
        {
            let capacities = self.monitor_capacities();
            for (query, result) in queries.iter().zip(&mut output) {
                if matches!(query.scope, MonitorScope::Host) {
                    match (&capacities, result.as_mut()) {
                        (Ok(capacities), Ok(payload)) => payload.capacities = capacities.clone(),
                        (Err(_), _) => *result = Err(MonitorError::Unavailable),
                        _ => {}
                    }
                }
            }
        }
        Ok(output)
    }
}
