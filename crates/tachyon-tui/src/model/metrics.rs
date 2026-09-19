//! Per-turn accounting persisted in visit snapshots.
use std::collections::HashMap;

#[derive(Clone, Default, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct TokenTotals {
    pub(crate) prompt: u64,
    pub(crate) completion: u64,
    pub(crate) total: u64,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct TurnMetrics {
    #[serde(default)]
    pub(crate) accepted_at_ms: Option<u64>,
    #[serde(default)]
    pub(crate) ended_at_ms: Option<u64>,
    /// Explicit terminal failure, distinct from timing completion before publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) failure: Option<String>,
    #[serde(default)]
    pub(crate) worker_started_at_ms: HashMap<String, u64>,
    #[serde(default)]
    pub(crate) worker_ended_at_ms: HashMap<String, u64>,
    pub(crate) first_visible_ms: Option<u64>,
    pub(crate) completed_ms: Option<u64>,
    pub(crate) self_usage: Option<TokenTotals>,
    #[serde(default)]
    pub(crate) worker_usage: HashMap<String, TokenTotals>,
    #[serde(default)]
    pub(crate) worker_outcomes: HashMap<String, bool>,
    #[serde(default)]
    pub(crate) memory: MemoryTurnMetrics,
    #[serde(default)]
    pub(crate) schedule: ScheduleTurnMetrics,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct MemoryTurnMetrics {
    pub(crate) saved: u32,
    pub(crate) forgotten: u32,
    pub(crate) corrected: u32,
    pub(crate) failed: u32,
    pub(crate) recalled_preferences: u32,
    pub(crate) recalled_history: u32,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct ScheduleTurnMetrics {
    pub(crate) scheduled: u32,
    #[serde(default)]
    pub(crate) tasks_scheduled: u32,
    pub(crate) cancelled: u32,
    pub(crate) fired: u32,
}
