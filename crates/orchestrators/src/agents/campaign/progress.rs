//! Bounded provider-neutral projections. No API dependency or raw ledgers.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignSnapshot {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub evidence_total: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<tachyon_api::campaign_oversight::AssessmentEvidence>,
    pub id: String,
    pub revision: u64,
    pub objective_summary: String,
    pub todo_revision: u64,
    pub todos: Vec<TodoSummary>,
    pub todos_partial: bool,
    pub resources: ResourceSnapshot,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unresolved_tokens: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unresolved_cost_micro_usd: Option<String>,
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
            || self.triggers.len() > 8
            || self
                .triggers
                .iter()
                .any(|s| s.is_empty() || s.len() > 64 || s.chars().any(char::is_control))
            || self.evidence.len() > 20
            || self.evidence_total < self.evidence.len() as u64
            || self.evidence.iter().any(|e| {
                e.reference.is_empty() || e.reference.len() > 512 || e.summary.len() > 2048
            })
            || self
                .todos
                .iter()
                .any(|t| t.id.is_empty() || t.id.len() > 256 || t.title.len() > 512)
            || [
                &self.resources.final_tokens,
                &self.resources.final_cost_micro_usd,
                &self.resources.unresolved_native_jobs,
                &self.resources.unresolved_tokens,
                &self.resources.unresolved_cost_micro_usd,
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
            triggers: vec![],
            evidence_total: 0,
            evidence: vec![],
            id: "c".into(),
            revision: 2,
            objective_summary: "Ship".into(),
            todo_revision: 0,
            todos: vec![],
            todos_partial: false,
            resources: ResourceSnapshot {
                unresolved_tokens: None,
                unresolved_cost_micro_usd: None,
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
