//! Bounded provider-neutral projections. No API dependency or raw ledgers.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignSnapshot {
    pub id: String,
    pub revision: u64,
    pub objective_summary: String,
    pub todo_revision: u64,
    pub todos: Vec<TodoSummary>,
    pub todos_partial: bool,
    pub resources: ResourceSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TodoSummary {
    pub id: String,
    pub title: String,
    pub status: TodoStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Blocked,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSnapshot {
    pub sampled_at_ms: Option<u64>,
    pub stale: bool,
    // Decimal strings preserve exact wide service counters; None means unknown.
    pub final_tokens: Option<String>,
    pub final_cost_micro_usd: Option<String>,
    pub unresolved_native_jobs: Option<String>,
}

impl CampaignSnapshot {
    pub fn render(&self) -> Result<String, &'static str> {
        if self.id.is_empty()
            || self.id.len() > 256
            || self.id.chars().any(char::is_control)
            || self.objective_summary.trim().is_empty()
            || self.objective_summary.len() > 4096
            || self.todos.len() > 20
            || self
                .todos
                .iter()
                .any(|t| t.id.is_empty() || t.id.len() > 256 || t.title.len() > 512)
            || [
                &self.resources.final_tokens,
                &self.resources.final_cost_micro_usd,
                &self.resources.unresolved_native_jobs,
            ]
            .iter()
            .any(|v| {
                v.as_ref().is_some_and(|s| {
                    s.is_empty()
                        || s.len() > 39
                        || !s.bytes().all(|b| b.is_ascii_digit())
                        || s.parse::<u128>().is_err()
                })
            })
        {
            return Err("invalid campaign snapshot");
        }
        serde_json::to_string(self).map_err(|_| "campaign snapshot encoding failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_render_is_deterministic_bounded_and_preserves_unknown() {
        let mut s = CampaignSnapshot {
            id: "c".into(),
            revision: 2,
            objective_summary: "Ship".into(),
            todo_revision: 0,
            todos: vec![],
            todos_partial: false,
            resources: ResourceSnapshot {
                sampled_at_ms: None,
                stale: true,
                final_tokens: None,
                final_cost_micro_usd: Some("0".into()),
                unresolved_native_jobs: None,
            },
        };
        assert_eq!(s.render().unwrap(), "{\"id\":\"c\",\"revision\":2,\"objective_summary\":\"Ship\",\"todo_revision\":0,\"todos\":[],\"todos_partial\":false,\"resources\":{\"sampled_at_ms\":null,\"stale\":true,\"final_tokens\":null,\"final_cost_micro_usd\":\"0\",\"unresolved_native_jobs\":null}}");
        s.objective_summary = "x".repeat(4097);
        assert!(s.render().is_err());
    }
}
