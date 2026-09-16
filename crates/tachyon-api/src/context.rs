//! Read-only research resources. Scope is supplied by the authenticated host, never this request.
use serde::{Deserialize, Serialize};

pub const MAX_PAGE_BYTES: usize = 8192;
pub const MAX_SCAN: usize = 64;

/// Informational worker observations, never permissions or a kernel checkpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerContextMetadata {
    pub activated_packages: std::collections::BTreeMap<String, String>,
    pub known_output_handles: Vec<String>,
}

impl WorkerContextMetadata {
    pub fn valid(&self) -> bool {
        let text = |s: &str| !s.is_empty() && s.len() <= 256 && !s.contains('\0');
        self.activated_packages.len() <= 64
            && self
                .activated_packages
                .iter()
                .all(|(k, v)| text(k) && text(v))
            && self.known_output_handles.len() <= 256
            && self.known_output_handles.iter().all(|s| text(s))
            && self
                .known_output_handles
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.known_output_handles.len()
    }
}

/// Host-authored diagnostic evidence at a model boundary. Not executable state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkContextSnapshot {
    /// Absent on legacy and pre-inference diagnostic snapshots.
    #[serde(default)]
    pub stopping: Option<StoppingContext>,
    pub schema_version: u32,
    pub work_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub instruction_revision: u64,
    pub request_id: String,
    pub objective: String,
    pub worker_claims_informational_only: Option<WorkerContextMetadata>,
    pub pending_question_refs: Option<Vec<String>>,
    /// Available allocation after boundary reservations, not usage or a new grant.
    /// None means unavailable; Some(0) means exhausted.
    pub remaining_tokens: Option<u64>,
    pub remaining_cost_micro_usd: Option<u64>,
    pub selected_resource_refs: Vec<ResourceRef>,
    pub reattachable: bool,
}

/// Host observations at collection, not authority to replay work or restore a kernel.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoppingContext {
    pub phase: StoppingPhase,
    pub reason: SnapshotStoppingReason,
    pub accepted_instruction_revision: u64,
    pub applied_instruction_revision: Option<u64>,
    pub instruction_refs: Vec<InstructionContextRef>,
    pub logical_work_handles: Vec<String>,
    pub group_handles: Vec<GroupContextRef>,
    pub produced_resource_refs: Vec<ResourceRef>,
    pub activation_observation: ActivationObservation,
    /// Invalid bounds or live IDs without an owned retained export were discarded.
    #[serde(default)]
    pub worker_observations_filtered: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupContextRef {
    pub group_id: String,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionContextRef {
    pub command_id: String,
    pub sequence: u64,
    pub accepted_revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoppingPhase {
    EvidenceReady,
    Unverified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStoppingReason {
    CandidateCollected,
    StoppedUnverified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationObservation {
    Final,
    Stale,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRef {
    pub kind: ResourceKind,
    pub work_id: String,
    pub id: String,
    pub version: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Attempt,
    Finding,
    Artifact,
    Trace,
    Document,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub literal: Option<String>,
    pub after: Option<String>,
    pub limit: usize,
    pub since_ms: Option<u64>,
    pub version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Snapshot {
        query: Query,
    },
    Search {
        query: Query,
    },
    Attempts {
        query: Query,
    },
    Findings {
        query: Query,
    },
    Artifacts {
        query: Query,
    },
    Traces {
        query: Query,
    },
    Documents {
        query: Query,
    },
    Read {
        resource: ResourceRef,
        offset: u64,
        limit: usize,
    },
}

impl Request {
    pub fn validate(&self) -> Result<(), &'static str> {
        let valid = match self {
            Self::Read {
                resource, limit, ..
            } => resource.valid() && (1..=1024).contains(limit),
            Self::Search { query }
            | Self::Snapshot { query }
            | Self::Attempts { query }
            | Self::Findings { query }
            | Self::Traces { query }
            | Self::Documents { query }
            | Self::Artifacts { query } => {
                (1..=16).contains(&query.limit)
                    && query.literal.as_ref().is_none_or(|s| s.len() <= 256)
                    && query.after.as_ref().is_none_or(|s| s.len() <= 1024)
                    && query.version.as_ref().is_none_or(|s| s.len() <= 256)
            }
        };
        if valid {
            Ok(())
        } else {
            Err("invalid research resource bounds")
        }
    }
}

impl ResourceRef {
    pub fn valid(&self) -> bool {
        [&self.work_id, &self.id, &self.version]
            .iter()
            .all(|s| !s.is_empty() && s.len() <= 256)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub reference: ResourceRef,
    /// Unknown for legacy evidence; never inferred from a deadline or query time.
    pub occurred_at_ms: Option<u64>,
    pub data: serde_json::Value,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    pub resources: Vec<Resource>,
    pub next_cursor: Option<String>,
}

/// Authored interpretation, not a theorem or an automatic consequence of an exit code.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Finding {
    pub id: String,
    pub work_id: String,
    pub author: String,
    pub claim: String,
    pub conditions: String,
    pub evidence: Vec<ResourceRef>,
    pub parents: Vec<ResourceRef>,
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    #[test]
    fn metadata_cannot_supply_host_authority_and_unknown_is_not_zero() {
        for field in [
            "objective",
            "remaining_tokens",
            "instruction_revision",
            "work_id",
            "reattachable",
        ] {
            let mut value = serde_json::to_value(WorkerContextMetadata::default()).unwrap();
            value[field] = 0.into();
            assert!(serde_json::from_value::<WorkerContextMetadata>(value).is_err());
        }
        let mut metadata = WorkerContextMetadata::default();
        metadata.known_output_handles = vec!["same".into(); 2];
        assert!(!metadata.valid());
        for metadata in [
            WorkerContextMetadata {
                activated_packages: (0..65).map(|i| (format!("p{i}"), "1".into())).collect(),
                ..Default::default()
            },
            WorkerContextMetadata {
                activated_packages: [("x".repeat(257), "1".into())].into(),
                ..Default::default()
            },
            WorkerContextMetadata {
                activated_packages: [("artifact".into(), "x".repeat(257))].into(),
                ..Default::default()
            },
            WorkerContextMetadata {
                known_output_handles: (0..257).map(|i| format!("output:{i}")).collect(),
                ..Default::default()
            },
            WorkerContextMetadata {
                known_output_handles: vec!["x".repeat(257)],
                ..Default::default()
            },
        ] {
            assert!(!metadata.valid());
        }
        let snapshot = WorkContextSnapshot {
            stopping: None,
            schema_version: 1,
            work_id: "w".into(),
            attempt_id: "a".into(),
            generation: 1,
            instruction_revision: 1,
            request_id: "r".into(),
            objective: "objective".into(),
            worker_claims_informational_only: None,
            pending_question_refs: None,
            remaining_tokens: Some(0),
            remaining_cost_micro_usd: None,
            selected_resource_refs: vec![],
            reattachable: false,
        };
        let value = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(value["remaining_tokens"], 0);
        assert!(value["remaining_cost_micro_usd"].is_null());
        assert_eq!(snapshot, serde_json::from_value(value.clone()).unwrap());
        let mut legacy = value;
        legacy.as_object_mut().unwrap().remove("stopping");
        assert_eq!(snapshot, serde_json::from_value(legacy).unwrap());
    }
}
