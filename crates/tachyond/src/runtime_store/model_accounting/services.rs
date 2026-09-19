//! Host-only campaign oversight funding. No worker capability or Work registration.
#[cfg(test)]
mod tests;
use super::*;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tachyon_model::{ChatMessage, Completion};
use tokio::{sync::watch, time::Instant};

pub(in crate::runtime_store) const SERVICES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("campaign_host_services");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServicePurpose {
    CampaignOversight,
}

/// Host-approved policy, never inferred from model output or worker content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServicePolicy {
    pub purpose: ServicePurpose,
    pub allowance: Units,
    pub estimate: RequestEstimate,
    pub max_requests: u64,
    pub timeout_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct ServiceRecord {
    schema_version: u32,
    campaign_id: String,
    name: String,
    policy: ServicePolicy,
    policy_sha256: String,
    requests: BTreeMap<String, String>,
}

#[derive(Default)]
pub(super) struct ServiceState {
    current: BTreeMap<String, ServiceGrant>,
}

struct ServiceGrant {
    nonce: uuid::Uuid,
    policy: ServicePolicy,
    campaign_id: String,
    cancelled: watch::Sender<bool>,
}

/// Private nonce and revocable lease. Not serializable, clonable or worker-facing.
pub(crate) struct ServicePermit {
    nonce: uuid::Uuid,
    allocation_id: String,
    cancelled: watch::Sender<bool>,
    timeout_ms: u64,
}

impl ServicePermit {
    /// Also usable from a host cancellation callback; interrupts active provider I/O.
    pub(crate) fn revoke(&self) {
        self.cancelled.send_replace(true);
    }
}

impl Drop for ServicePermit {
    fn drop(&mut self) {
        self.revoke();
    }
}

pub(in crate::runtime_store) fn service_id(campaign: &str, name: &str) -> String {
    format!(
        "service:{:x}",
        Sha256::digest(serde_json::to_vec(&(campaign, name)).unwrap())
    )
}

fn load(write: &WriteTransaction, id: &str) -> tachyon_model::Result<ServiceRecord> {
    let table = write.open_table(SERVICES).map_err(err)?;
    let value = table
        .get(id)
        .map_err(err)?
        .ok_or_else(|| err("unknown host service"))?;
    let record: ServiceRecord = serde_json::from_slice(value.value()).map_err(err)?;
    if record.schema_version != 1
        || service_id(&record.campaign_id, &record.name) != id
        || record.policy_sha256
            != format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&record.policy).map_err(err)?)
            )
    {
        return Err(err("invalid host service record"));
    }
    Ok(record)
}

impl RuntimeStore {
    /// Reserves the FULL allowance from the existing campaign Work envelope.
    /// Reopen requires this explicit host reauthorization with the identical policy.
    /// In-process replacement requires the current capability, even if revoked.
    pub(crate) fn host_authorize_service(
        &self,
        campaign_id: &str,
        name: &str,
        policy: ServicePolicy,
        replace: Option<&ServicePermit>,
    ) -> tachyon_model::Result<ServicePermit> {
        let (tokens, cost) = policy.estimate.upper_bound()?;
        if campaign_id.trim().is_empty()
            || campaign_id.len() > 256
            || name.trim().is_empty()
            || name.len() > 256
            || policy.max_requests == 0
            || policy.timeout_ms == 0
            || policy.timeout_ms > 86_400_000
            || tokens > policy.allowance.tokens
            || cost > policy.allowance.cost_micro_usd
        {
            return Err(err("invalid host service policy"));
        }
        let id = service_id(campaign_id, name);
        let mut state = self
            .model_permits
            .lock()
            .map_err(|_| err("authority unavailable"))?;
        if state.services.current.get(&id).map(|g| g.nonce) != replace.map(|p| p.nonce) {
            return Err(err("service replacement conflict"));
        }
        let write = self.database.begin_write().map_err(err)?;
        if write
            .open_table(super::super::admission::WORK)
            .map_err(err)?
            .get(id.as_str())
            .map_err(err)?
            .is_some()
        {
            return Err(err("host service identity conflicts with Work"));
        }
        let exists = write
            .open_table(SERVICES)
            .map_err(err)?
            .get(id.as_str())
            .map_err(err)?
            .is_some();
        if exists {
            let record = load(&write, &id)?;
            if record.policy != policy {
                return Err(err("immutable service policy conflict"));
            }
            let ledger = Self::campaign_ledger_in(&write, campaign_id).map_err(err)?;
            if ledger.allocations.get(&id) != Some(&false) || ledger.admissions_paused {
                return Err(err("service funding unavailable"));
            }
        } else {
            let hash = format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&policy).map_err(err)?)
            );
            Self::fund_host_service_in(&write, campaign_id, &id, policy.allowance, &hash)
                .map_err(err)?;
            let record = ServiceRecord {
                schema_version: 1,
                campaign_id: campaign_id.into(),
                name: name.into(),
                policy: policy.clone(),
                policy_sha256: hash,
                requests: BTreeMap::new(),
            };
            write
                .open_table(SERVICES)
                .map_err(err)?
                .insert(
                    id.as_str(),
                    serde_json::to_vec(&record).map_err(err)?.as_slice(),
                )
                .map_err(err)?;
        }
        write.commit().map_err(err)?;
        let nonce = uuid::Uuid::new_v4();
        let timeout_ms = policy.timeout_ms;
        let (cancelled, _) = watch::channel(false);
        if let Some(old) = state.services.current.insert(
            id.clone(),
            ServiceGrant {
                nonce,
                policy,
                campaign_id: campaign_id.into(),
                cancelled: cancelled.clone(),
            },
        ) {
            old.cancelled.send_replace(true);
        }
        Ok(ServicePermit {
            nonce,
            allocation_id: id,
            cancelled,
            timeout_ms,
        })
    }

    fn claim_service(
        &self,
        nonce: uuid::Uuid,
        id: &str,
        request_id: &str,
        request: &RequestReservation,
        deadline: Instant,
    ) -> tachyon_model::Result<String> {
        if request_id.trim().is_empty() || request_id.len() > 256 {
            return Err(err("invalid service request ID"));
        }
        let state = self
            .model_permits
            .lock()
            .map_err(|_| err("authority unavailable"))?;
        let grant = state
            .services
            .current
            .get(id)
            .ok_or_else(|| err("unknown service authority"))?;
        if grant.nonce != nonce
            || *grant.cancelled.borrow()
            || Instant::now() >= deadline
            || request != &reservation(&grant.campaign_id, id, request_id, &grant.policy)
        {
            return Err(err("invalid service authority"));
        }
        let write = self.database.begin_write().map_err(err)?;
        let mut record = load(&write, id)?;
        super::super::campaign_oversight::dispatch_in(&write, &record.campaign_id, request_id)
            .map_err(err)?;
        if record.policy != grant.policy
            || record.requests.contains_key(request_id)
            || record.requests.len() as u64 >= record.policy.max_requests
        {
            return Err(err("service request replay or count exhausted"));
        }
        let ledger = Self::campaign_ledger_in(&write, &record.campaign_id).map_err(err)?;
        if ledger
            .reservations
            .values()
            .any(|r| r.allocation.as_deref() == Some(id) && !matches!(r.usage, Usage::Final(_)))
        {
            return Err(err("service has an unresolved request"));
        }
        let receipt = format!("model:{}", uuid::Uuid::new_v4());
        let (tokens, cost_micro_usd) = request.estimate.upper_bound()?;
        Self::campaign_ledger_command_in(
            &write,
            &format!("reserve:{receipt}"),
            &record.campaign_id,
            LedgerCommand::ReserveAllocated {
                reservation_id: receipt.clone(),
                allocation_id: id.into(),
                pool: Pool::Work,
                reserved: Units {
                    tokens,
                    cost_micro_usd,
                },
            },
        )
        .map_err(err)?;
        write
            .open_table(REQUESTS)
            .map_err(err)?
            .insert(
                receipt.as_str(),
                serde_json::to_vec(&Record {
                    schema_version: 1,
                    request: request.clone(),
                    usage: RequestUsage::Unknown,
                    allocation_id: Some(id.into()),
                })
                .map_err(err)?
                .as_slice(),
            )
            .map_err(err)?;
        // The request entry is the durable exclusive dispatch claim, including unknown outcomes.
        record.requests.insert(request_id.into(), receipt.clone());
        write
            .open_table(SERVICES)
            .map_err(err)?
            .insert(id, serde_json::to_vec(&record).map_err(err)?.as_slice())
            .map_err(err)?;
        write.commit().map_err(err)?;
        Ok(receipt)
    }
}

fn reservation(
    campaign: &str,
    id: &str,
    request_id: &str,
    policy: &ServicePolicy,
) -> RequestReservation {
    RequestReservation {
        identity: WorkIdentity {
            campaign_id: campaign.into(),
            work_id: id.into(),
            attempt_id: request_id.into(),
            generation: 1,
            instruction_revision: 1,
            class: RequestClass::Work,
        },
        estimate: policy.estimate.clone(),
    }
}

struct ServiceAccounting {
    store: Arc<RuntimeStore>,
    nonce: uuid::Uuid,
    allocation_id: String,
    request_id: String,
    request: RequestReservation,
    deadline: Instant,
    cancelled: watch::Sender<bool>,
}

impl RequestAccounting for ServiceAccounting {
    fn reserve<'a>(&'a self, request: &'a RequestReservation) -> AccountingFuture<'a, String> {
        Box::pin(async move {
            if request != &self.request {
                return Err(err("service request mismatch"));
            }
            let (store, nonce, id, request_id, request, deadline) = (
                self.store.clone(),
                self.nonce,
                self.allocation_id.clone(),
                self.request_id.clone(),
                request.clone(),
                self.deadline,
            );
            let receipt = tokio::task::spawn_blocking(move || {
                store.claim_service(nonce, &id, &request_id, &request, deadline)
            })
            .await
            .map_err(err)??;
            if Instant::now() >= self.deadline || *self.cancelled.borrow() {
                return Err(err("service cancelled before dispatch"));
            }
            Ok(receipt)
        })
    }
    fn reconcile<'a>(&'a self, receipt: &'a str, usage: RequestUsage) -> AccountingFuture<'a, ()> {
        Box::pin(async move {
            let (store, request, id, receipt) = (
                self.store.clone(),
                self.request.clone(),
                self.allocation_id.clone(),
                receipt.to_owned(),
            );
            tokio::task::spawn_blocking(move || {
                DaemonAccounting {
                    store: &store,
                    authorized: request,
                }
                .reconcile_funded_sync(&receipt, usage, Some(&id))
            })
            .await
            .map_err(err)?
        })
    }
}

impl ModelBroker {
    /// Host-only, text-only oversight. No tools, worker transport, resident or execution lease.
    /// Configuration and attempt identity are constructed from authority, not caller policy.
    pub(crate) async fn execute_service(
        &self,
        permit: &ServicePermit,
        request_id: &str,
        messages: &[ChatMessage],
        on_delta: &mut (dyn FnMut(&str) + Send),
    ) -> tachyon_model::Result<Completion> {
        if request_id.trim().is_empty() || request_id.len() > 256 {
            return Err(err("invalid service request ID"));
        }
        let deadline = Instant::now() + Duration::from_millis(permit.timeout_ms);
        let (store, id, nonce, request_id) = (
            self.store.clone(),
            permit.allocation_id.clone(),
            permit.nonce,
            request_id.to_owned(),
        );
        let request_key = request_id.clone();
        let mut cancellation = permit.cancelled.subscribe();
        let snapshot = tokio::task::spawn_blocking(move || {
            let state = store
                .model_permits
                .lock()
                .map_err(|_| err("authority unavailable"))?;
            let grant = state
                .services
                .current
                .get(&id)
                .ok_or_else(|| err("unknown service"))?;
            if grant.nonce != nonce || *grant.cancelled.borrow() {
                return Err(err("revoked service"));
            }
            Ok::<_, ModelError>(reservation(
                &grant.campaign_id,
                &id,
                &request_key,
                &grant.policy,
            ))
        });
        let request = tokio::select! {
            biased;
            _ = cancellation.wait_for(|value| *value) => return Err(err("service revoked")),
            _ = tokio::time::sleep_until(deadline) => return Err(err("service authority deadline")),
            result = snapshot => result.map_err(err)??,
        };
        let accountant = ServiceAccounting {
            store: self.store.clone(),
            nonce: permit.nonce,
            allocation_id: permit.allocation_id.clone(),
            request_id,
            request: request.clone(),
            deadline,
            cancelled: permit.cancelled.clone(),
        };
        let context = AccountingContext {
            accountant: &accountant,
            request,
        };
        let mut cancelled = permit.cancelled.subscribe();
        tokio::select! {
            biased;
            _ = cancelled.wait_for(|value| *value) => Err(err("service revoked; outcome may be unknown")),
            _ = tokio::time::sleep_until(deadline) => Err(err("service deadline; outcome may be unknown")),
            result = async {
                let _capacity = self.store.host_capacity.model.acquire(&context.request.identity.campaign_id).await.map_err(err)?;
                self.model.chat_accounted(messages, None, None, on_delta, &context).await
            } => result,
        }
    }
}
