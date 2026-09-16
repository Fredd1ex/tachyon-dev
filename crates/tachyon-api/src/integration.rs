//! Explicit operator integration, never worker authority.
use serde::{Deserialize, Serialize};

pub const MAX_PLAN_BYTES: usize = 65_536;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IntegrationPlan {
    pub command_id: String,
    pub work_id: String,
    pub artifact_id: String,
    pub artifact_sha256: String,
    /// Host root read versions, not child workspace versions.
    pub expected_versions: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchBundle {
    pub files: Vec<ExactEdit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactEdit {
    pub path: String,
    pub old: String,
    pub new: String,
}

impl IntegrationPlan {
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_PLAN_BYTES {
            return Err("integration plan exceeds 65536 bytes".into());
        }
        let plan: Self = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        plan.validate()?;
        Ok(plan)
    }
    pub fn validate(&self) -> Result<(), String> {
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > MAX_PLAN_BYTES {
            return Err("integration plan exceeds 65536 bytes".into());
        }
        if [&self.command_id, &self.work_id, &self.artifact_id]
            .iter()
            .any(|s| s.is_empty() || s.len() > 256)
            || self.artifact_sha256.len() != 64
            || !self.artifact_sha256.bytes().all(|c| c.is_ascii_hexdigit())
            || self.expected_versions.is_empty()
            || self.expected_versions.len() > 32
            || self
                .expected_versions
                .iter()
                .any(|(p, v)| p.len() > 4096 || !v.starts_with("stat-v1:") || v.len() != 72)
        {
            return Err("invalid integration plan".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_exact_plan_rejects_root_override_and_unknown_fields() {
        let mut value = serde_json::json!({
            "command_id":"operator-1", "work_id":"child", "artifact_id":"patch",
            "artifact_sha256":"a".repeat(64),
            "expected_versions":{"src/a":format!("stat-v1:{}", "b".repeat(64))}
        });
        let plan = IntegrationPlan::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(
            IntegrationPlan::parse(&serde_json::to_vec(&plan).unwrap()).unwrap(),
            plan
        );
        value["workspace"] = serde_json::json!("/tmp/override");
        assert!(IntegrationPlan::parse(&serde_json::to_vec(&value).unwrap()).is_err());
        assert!(IntegrationPlan::parse(&vec![b' '; MAX_PLAN_BYTES + 1]).is_err());
        assert!(serde_json::from_value::<PatchBundle>(serde_json::json!({
            "files":[{"path":"a", "old":"old", "new":"new", "delete":true}]
        }))
        .is_err());
    }
}
