//! Explicit local operator authorization. This is not a worker capability.
use crate::context::{ResourceKind, ResourceRef};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationRequest {
    pub schema_version: u32,
    pub command_id: String,
    pub campaign_id: String,
    pub checkpoint: ResourceRef,
    pub expected_state_sha256: String,
    pub expected_stopping_reason: StoppingReason,
    pub executable_sha256: String,
    pub ghost_version: String,
    pub instruction: String,
    pub goal: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoppingReason {
    StoppedUnverified,
}

/// Host-selected configuration requirements, never a permission grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationBootstrap {
    pub executable_sha256: String,
    pub ghost_version: String,
    pub packages: std::collections::BTreeMap<String, String>,
}

impl ContinuationBootstrap {
    pub fn validate_observed(&self, actual: &Self) -> Result<(), &'static str> {
        if actual.executable_sha256 != self.executable_sha256
            || actual.ghost_version != self.ghost_version
            || actual.packages.len() > 64
            || self
                .packages
                .iter()
                .any(|(name, version)| actual.packages.get(name) != Some(version))
        {
            return Err(
                "continuation configuration mismatch: executable, Ghost version or package version",
            );
        }
        Ok(())
    }
}

impl ContinuationRequest {
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > crate::campaign::MANIFEST_MAX_BYTES {
            return Err("continuation request exceeds 65536 bytes".into());
        }
        let request: Self = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), String> {
        let id = |s: &str| {
            !s.is_empty()
                && s.len() <= 256
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
        };
        let hash = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.schema_version != 1
            || !id(&self.command_id)
            || !id(&self.campaign_id)
            || !self.checkpoint.valid()
            || self.checkpoint.kind != ResourceKind::Trace
            || !hash(&self.checkpoint.id)
            || !hash(&self.checkpoint.version)
            || !hash(&self.expected_state_sha256)
            || !hash(&self.executable_sha256)
            || !id(&self.ghost_version)
            || self.instruction.trim().is_empty()
            || self.instruction.len() > 16384
            || self.instruction.contains('\0')
            || self
                .goal
                .as_ref()
                .is_some_and(|s| s.trim().is_empty() || s.len() > 16384 || s.contains('\0'))
            || serde_json::to_vec(self).map_err(|e| e.to_string())?.len()
                > crate::campaign::MANIFEST_MAX_BYTES
        {
            return Err("invalid bounded continuation request; exact checkpoint, state and executable hashes required".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_revalidates_known_versions_and_build_pin() {
        let expected = ContinuationBootstrap {
            executable_sha256: "a".repeat(64),
            ghost_version: "0.3.0".into(),
            packages: std::collections::BTreeMap::from([("ipython".into(), "0.3.0".into())]),
        };
        expected.validate_observed(&expected).unwrap();
        for field in ["build", "ghost", "known_package", "missing_package"] {
            let mut actual = expected.clone();
            match field {
                "build" => actual.executable_sha256.clear(),
                "ghost" => actual.ghost_version = "other".into(),
                "known_package" => {
                    actual.packages.insert("ipython".into(), "other".into());
                }
                _ => actual.packages.clear(),
            }
            assert!(expected
                .validate_observed(&actual)
                .unwrap_err()
                .contains("configuration mismatch"));
        }
    }

    #[test]
    fn strict_bounded_operator_request() {
        let docs = include_str!("../../../docs/ghost/CONTINUATION.md");
        let example = docs
            .split("```json\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap();
        ContinuationRequest::parse(example.as_bytes()).unwrap();
        let value = serde_json::json!({"schema_version":1,"command_id":"continue-1",
            "campaign_id":"campaign-1","checkpoint":{"kind":"trace","work_id":"root",
            "id":"a".repeat(64),"version":"b".repeat(64)},"expected_state_sha256":"c".repeat(64),
            "expected_stopping_reason":"stopped_unverified","executable_sha256":"d".repeat(64),
            "ghost_version":"0.1.0","instruction":"Continue the same objective","goal":null});
        assert!(ContinuationRequest::parse(&serde_json::to_vec(&value).unwrap()).is_ok());
        let wire = serde_json::json!({"cmd":"campaign_continue","id":"campaign-1","request":value});
        assert!(matches!(
            serde_json::from_value::<crate::types::ApiRequest>(wire).unwrap(),
            crate::types::ApiRequest::CampaignContinue {
                unisolated_development: false,
                ..
            }
        ));
        for field in ["budget", "deadline_ms", "permissions", "kernel_pickle"] {
            let mut invalid = value.clone();
            invalid[field] = 1.into();
            assert!(ContinuationRequest::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
        for field in [
            "checkpoint",
            "command_id",
            "expected_state_sha256",
            "expected_stopping_reason",
            "executable_sha256",
            "ghost_version",
        ] {
            let mut invalid = value.clone();
            invalid.as_object_mut().unwrap().remove(field);
            assert!(ContinuationRequest::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
        for instruction in [String::new(), "x".repeat(16385), "bad\0instruction".into()] {
            let mut invalid = value.clone();
            invalid["instruction"] = instruction.into();
            assert!(ContinuationRequest::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
    }
}
