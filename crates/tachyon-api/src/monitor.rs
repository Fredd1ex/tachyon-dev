//! Read-only operator monitoring. Not an execution authority or durable event feed.
use serde::{Deserialize, Serialize};

pub const MAX_PAGE: usize = 100;

/// Exact wide counters are decimal strings on the wire, including zero.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Decimal(pub u128);
impl Serialize for Decimal {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_string())
    }
}
impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(serde::de::Error::custom("expected unsigned decimal string"));
        }
        value.parse().map(Self).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "knowledge", content = "value", rename_all = "snake_case")]
pub enum Observed<T> {
    #[default]
    Unknown,
    Known(T),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub enum MonitorScope {
    Host,
    Campaign {
        campaign_id: String,
    },
    Work {
        campaign_id: String,
        work_id: String,
    },
}
impl MonitorScope {
    pub fn matches(&self, campaign: &str, work: Option<&str>) -> bool {
        match self {
            Self::Host => true,
            Self::Campaign { campaign_id } => campaign_id == campaign,
            Self::Work {
                campaign_id,
                work_id,
            } => campaign_id == campaign && work == Some(work_id.as_str()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorQuery {
    pub scope: MonitorScope,
    pub after: Option<String>,
    pub limit: usize,
}
impl MonitorQuery {
    pub fn validate(&self) -> Result<(), MonitorError> {
        let valid = |s: &str| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control);
        let ids = match &self.scope {
            MonitorScope::Host => true,
            MonitorScope::Campaign { campaign_id } => valid(campaign_id),
            MonitorScope::Work {
                campaign_id,
                work_id,
            } => valid(campaign_id) && valid(work_id),
        };
        if !ids
            || self.limit == 0
            || self.limit > MAX_PAGE
            || self
                .after
                .as_deref()
                .is_some_and(|s| s.is_empty() || s.len() > 512 || s.chars().any(char::is_control))
        {
            return Err(MonitorError::InvalidQuery);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Amounts {
    pub tokens: Decimal,
    pub cost_micro_usd: Decimal,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Inference {
    /// Campaign-ledger coverage only, not unaccounted ordinary chat/model traffic.
    pub final_usage: Amounts,
    pub provisional_usage: Amounts,
    pub unresolved_reserved: Amounts,
    pub unknown_reports: Decimal,
    pub provisional_reports: Decimal,
    pub final_reports: Decimal,
    pub allocation_parents: Decimal,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolBudget {
    pub authorized: Amounts,
    /// Host/Campaign includes remaining allocation funding, not just request charges.
    pub committed: Amounts,
    pub available: Amounts,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Funding {
    pub work: PoolBudget,
    pub verification: PoolBudget,
    pub debt: Amounts,
    pub paused_ledgers: Observed<Decimal>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeJobs {
    /// Charged wall-time, NOT CPU utilization or measured CPU time.
    pub cpu_charged_wall_ms: Decimal,
    pub gpu_charged_wall_ms: Decimal,
    pub unresolved: Decimal,
    pub cpu_unresolved: Decimal,
    pub gpu_unresolved: Decimal,
    pub finalized: Decimal,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Storage {
    pub limit_bytes: Observed<Decimal>,
    /// Logical retained receipts, NOT measured disk usage.
    pub reserved_bytes: Decimal,
    pub ready_bytes: Decimal,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Durable {
    pub sampled_at_ms: u64,
    pub inference: Observed<Inference>,
    /// Root envelopes for Host/Campaign. Work allocation budget is not a new grant.
    pub funding: Observed<Funding>,
    pub native_jobs: NativeJobs,
    /// Receipts have campaign attribution only; Work storage is Unknown.
    pub retained_storage: Observed<Storage>,
    pub admitted_work: Decimal,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    pub resource: CapacityResource,
    pub sampled_at_ms: u64,
    pub limit: Decimal,
    pub held: Decimal,
    pub queued: Decimal,
    pub unresolved: Observed<Decimal>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityResource {
    Campaign,
    Resident,
    Execution,
    Model,
    Cpu,
    Gpu,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegisteredRole {
    Foreground,
    Background,
    Agent,
    MemoryService,
    OrdinaryWork,
    CampaignWork,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredEntry {
    pub id: String,
    pub role: RegisteredRole,
    pub state: String,
    /// No invented process identity for an in-process service.
    pub pid: Option<u32>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registered {
    pub sampled_at_ms: u64,
    pub total: Decimal,
    pub entries: Vec<RegisteredEntry>,
    pub next_after: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorPayload {
    pub durable: Durable,
    /// Host-only process-local observations; not globally atomic with durable data.
    pub capacities: Vec<Capacity>,
    pub registered: Registered,
}
impl MonitorPayload {
    pub fn same_values(&self, other: &Self) -> bool {
        let strip = |mut p: Self| {
            p.durable.sampled_at_ms = 0;
            p.registered.sampled_at_ms = 0;
            for c in &mut p.capacities {
                c.sampled_at_ms = 0;
            }
            p
        };
        strip(self.clone()) == strip(other.clone())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorVersion {
    /// New random identity each daemon start; unrelated to todo/event epochs.
    pub epoch: String,
    pub sequence: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorSnapshot {
    pub query: MonitorQuery,
    pub version: MonitorVersion,
    pub payload: Option<MonitorPayload>,
    /// A failed sample retains the last successful payload and its clocks.
    pub stale: Option<MonitorError>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorError {
    InvalidQuery,
    NotFound,
    Unavailable,
    ScopeLimit,
    SubscriberLimit,
    Stopped,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn monitor_fingerprint_ignores_all_source_clocks_not_values() {
        let a = MonitorPayload {
            durable: Durable::default(),
            registered: Registered::default(),
            capacities: vec![Capacity {
                resource: CapacityResource::Model,
                sampled_at_ms: 1,
                limit: Decimal(2),
                held: Decimal(0),
                queued: Decimal(0),
                unresolved: Observed::Known(Decimal(0)),
            }],
        };
        let mut b = a.clone();
        b.durable.sampled_at_ms = 20;
        b.registered.sampled_at_ms = 30;
        b.capacities[0].sampled_at_ms = 40;
        assert!(a.same_values(&b));
        b.capacities[0].unresolved = Observed::Unknown;
        assert!(!a.same_values(&b));
    }
    #[test]
    fn exact_decimal_and_unknown_zero() {
        let n = Decimal(u128::from(u64::MAX) + 1);
        assert_eq!(
            serde_json::to_string(&n).unwrap(),
            "\"18446744073709551616\""
        );
        assert_eq!(
            serde_json::from_str::<Decimal>(&serde_json::to_string(&n).unwrap()).unwrap(),
            n
        );
        assert!(serde_json::from_str::<Decimal>("0").is_err());
        assert_ne!(
            serde_json::to_value(Observed::<Decimal>::Unknown).unwrap(),
            serde_json::to_value(Observed::Known(Decimal(0))).unwrap()
        );
    }
}
