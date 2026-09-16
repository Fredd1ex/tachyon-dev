//! Explicit local host authorization, never a conversational intent or worker grant.
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

pub const MANIFEST_MAX_BYTES: usize = 65_536;

/// Privileged operator evidence, never a worker billing report or replay grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationReceipt {
    pub schema_version: u32,
    pub command_id: String,
    pub campaign_id: String,
    pub expected_state_sha256: String,
    pub evidence_reference: String,
    pub records: Vec<RecoveryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecoveryRecord {
    NativeCleanup {
        work_id: String,
        attempt_id: String,
        generation: u64,
        lease_id: String,
        confirmation: CleanupConfirmation,
    },
    ModelUsage {
        work_id: String,
        attempt_id: String,
        generation: u64,
        instruction_revision: u64,
        reservation_id: String,
        allocation_id: String,
        request_id: String,
        provider: String,
        provider_request_id: String,
        input_tokens: u64,
        output_tokens: u64,
        cost_micro_usd: u64,
    },
    Cleanup {
        work_id: String,
        attempt_id: String,
        generation: u64,
        reservation_id: String,
        confirmation: CleanupConfirmation,
        outcome: CleanupOutcome,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupConfirmation {
    OperatorAttestsAllProcessesTerminated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupOutcome {
    Unverified,
}

impl ReconciliationReceipt {
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MANIFEST_MAX_BYTES {
            return Err("receipt exceeds 65536 bytes".into());
        }
        let receipt: Self = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), String> {
        let identifier = |s: &str| {
            !s.is_empty()
                && s.len() <= 256
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
        };
        if self.schema_version != 1
            || !identifier(&self.command_id)
            || !identifier(&self.campaign_id)
            || !identifier(&self.evidence_reference)
            || self.expected_state_sha256.len() != 64
            || !self
                .expected_state_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.records.is_empty()
            || self.records.len() > 256
            || serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > MANIFEST_MAX_BYTES
        {
            return Err("invalid bounded reconciliation receipt; use a nonsecret evidence identifier, not a URL or free text".into());
        }
        for record in &self.records {
            let valid = match record {
                RecoveryRecord::ModelUsage {
                    work_id,
                    attempt_id,
                    generation,
                    instruction_revision,
                    reservation_id,
                    allocation_id,
                    request_id,
                    provider,
                    provider_request_id,
                    input_tokens,
                    output_tokens,
                    ..
                } => {
                    [
                        work_id,
                        attempt_id,
                        reservation_id,
                        allocation_id,
                        request_id,
                        provider,
                        provider_request_id,
                    ]
                    .into_iter()
                    .all(|s| identifier(s))
                        && *generation > 0
                        && *instruction_revision > 0
                        && input_tokens.checked_add(*output_tokens).is_some()
                }
                RecoveryRecord::Cleanup {
                    work_id,
                    attempt_id,
                    generation,
                    reservation_id,
                    ..
                } => {
                    [work_id, attempt_id, reservation_id]
                        .into_iter()
                        .all(|s| identifier(s))
                        && *generation > 0
                }
                RecoveryRecord::NativeCleanup {
                    work_id,
                    attempt_id,
                    generation,
                    lease_id,
                    ..
                } => {
                    [work_id, attempt_id, lease_id]
                        .into_iter()
                        .all(|s| identifier(s))
                        && *generation > 0
                }
            };
            if !valid {
                return Err("invalid reconciliation target/usage".into());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignManifest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_storage_bytes: Option<u64>,
    pub schema_version: u32,
    pub campaign_id: String,
    pub objective: String,
    pub executable: PathBuf,
    pub workspace: PathBuf,
    pub home: PathBuf,
    pub deadline_ms: u64,
    pub work_tokens: u64,
    pub work_cost_micro_usd: u64,
    pub verification_tokens: u64,
    pub verification_cost_micro_usd: u64,
    pub max_active_inferences: u32,
    pub model: String,
    pub pricing_revision: String,
    pub max_request_bytes: u64,
    pub input_tokens: u64,
    pub output_tokens: u32,
    pub input_micro_usd_per_million: u64,
    pub output_micro_usd_per_million: u64,
    pub other_micro_usd: u64,
    pub evaluator: CampaignEvaluator,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub children: Option<CampaignChildren>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocation: Option<CampaignAllocation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute: Option<ComputeEnvelope>,
}

/// Aggregate native-job wall time, not CPU cycles or GPU utilization.
/// Every descendant spends from this one immutable root allowance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputeEnvelope {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub profiles: std::collections::BTreeMap<String, ComputeProfile>,
    pub cpu_job_ms: u64,
    #[serde(default)]
    pub gpu_job_ms: u64,
    #[serde(default)]
    pub max_gpu_jobs: usize,
    pub max_cpu_timeout_ms: u64,
    #[serde(default)]
    pub max_gpu_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputeProfile {
    #[serde(default)]
    pub max_gpu_jobs: usize,
    pub max_cpu_timeout_ms: u64,
    #[serde(default)]
    pub max_gpu_timeout_ms: u64,
}

impl ComputeEnvelope {
    pub fn validate(&self) -> Result<(), String> {
        if self.profiles.len() > 64
            || self.profiles.iter().any(|(id, p)| {
                id.is_empty()
                    || id.len() > 256
                    || p.max_gpu_jobs > self.max_gpu_jobs
                    || p.max_cpu_timeout_ms == 0
                    || p.max_cpu_timeout_ms > self.max_cpu_timeout_ms
                    || p.max_gpu_timeout_ms > self.max_gpu_timeout_ms
                    || (p.max_gpu_jobs > 0 && p.max_gpu_timeout_ms == 0)
            })
        {
            return Err("invalid compute profile bounds".into());
        }
        if self.max_cpu_timeout_ms == 0
            || self.max_cpu_timeout_ms > 86_400_000
            || self.max_gpu_timeout_ms > 86_400_000
            || self.max_gpu_jobs > 256
            || (self.max_gpu_jobs > 0 && (self.gpu_job_ms == 0 || self.max_gpu_timeout_ms == 0))
        {
            return Err("invalid native compute envelope/timeouts".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationMode {
    Fixed,
    ModelProposed,
    Deterministic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignAllocation {
    pub mode: AllocationMode,
    pub max_running: usize,
    #[serde(
        default = "default_allocation_actions",
        skip_serializing_if = "is_default_allocation_actions"
    )]
    pub allowed_actions: Vec<AllocationAction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signals: Vec<AllocationSignal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationAction {
    Reallocate,
    Resize,
    Work,
    Verify,
    Pause,
    Stop,
}

pub fn default_allocation_actions() -> Vec<AllocationAction> {
    vec![AllocationAction::Resize]
}

fn is_default_allocation_actions(actions: &[AllocationAction]) -> bool {
    actions == [AllocationAction::Resize]
}

/// Explicit host intent, not an inference from a scientific outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationSignal {
    pub command_id: String,
    pub group_id: String,
    pub expected_revision: u64,
    pub action: AllocationControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AllocationControl {
    /// Exact trusted-host authorization to move existing unused Work funds.
    Reallocate {
        source_work_id: String,
        source_generation: u64,
        target_work_id: String,
        target_generation: u64,
        tokens: u64,
        cost_micro_usd: u64,
        expected_ledger_revision: u64,
    },
    Work {
        template_id: String,
        parent_work_id: String,
        generation: u64,
        instruction_revision: u64,
    },
    Verify {
        work_id: String,
        generation: u64,
        instruction_revision: u64,
    },
    Pause,
    /// Cancellation intent for an exact existing branch, not graceful completion.
    Stop {
        work_id: String,
        generation: u64,
    },
}

impl CampaignAllocation {
    pub fn validate(&self) -> Result<(), String> {
        let id = |s: &str| !s.trim().is_empty() && s.len() <= 256 && !s.contains('\0');
        if !(1..=64).contains(&self.max_running)
            || self.allowed_actions.len() > 6
            || self
                .allowed_actions
                .iter()
                .enumerate()
                .any(|(i, a)| self.allowed_actions[..i].contains(a))
            || self.signals.len() > 32
            || (!self.signals.is_empty() && self.mode != AllocationMode::Deterministic)
        {
            return Err("invalid allocation action bounds/mode".into());
        }
        let mut commands = std::collections::BTreeSet::new();
        for signal in &self.signals {
            let action = match &signal.action {
                AllocationControl::Reallocate {
                    source_work_id,
                    source_generation,
                    target_work_id,
                    target_generation,
                    tokens,
                    cost_micro_usd,
                    ..
                } => {
                    if !id(source_work_id)
                        || !id(target_work_id)
                        || source_work_id == target_work_id
                        || *source_generation == 0
                        || *target_generation == 0
                        || (*tokens == 0 && *cost_micro_usd == 0)
                    {
                        return Err("invalid allocation transfer".into());
                    }
                    AllocationAction::Reallocate
                }
                AllocationControl::Work {
                    template_id,
                    parent_work_id,
                    generation,
                    instruction_revision,
                } => {
                    if !id(template_id)
                        || !id(parent_work_id)
                        || *generation == 0
                        || *instruction_revision == 0
                    {
                        return Err("invalid allocation catalog identity".into());
                    }
                    AllocationAction::Work
                }
                AllocationControl::Verify {
                    work_id,
                    generation,
                    instruction_revision,
                } => {
                    if !id(work_id) || *generation == 0 || *instruction_revision == 0 {
                        return Err("invalid allocation verification identity".into());
                    }
                    AllocationAction::Verify
                }
                AllocationControl::Pause => AllocationAction::Pause,
                AllocationControl::Stop {
                    work_id,
                    generation,
                } => {
                    if !id(work_id) || *generation == 0 {
                        return Err("invalid allocation branch identity".into());
                    }
                    AllocationAction::Stop
                }
            };
            if !id(&signal.command_id)
                || !id(&signal.group_id)
                || signal.expected_revision == 0
                || !commands.insert(&signal.command_id)
                || !self.allowed_actions.contains(&action)
            {
                return Err("invalid or unauthorized allocation signal".into());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignChildren {
    #[serde(
        default = "default_child_depth",
        skip_serializing_if = "is_default_child_depth"
    )]
    pub max_depth: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic: Option<DynamicChildren>,
    pub total_work: usize,
    pub max_running: usize,
    pub max_resident: usize,
    pub controls: Vec<crate::agents::Control>,
    pub history: bool,
    pub completion: ChildCompletion,
    pub templates: Vec<CampaignTemplate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicChildren {
    pub max_proposals: usize,
    pub profiles: Vec<ChildProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildProfile {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profile_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluator: Option<ChildEvaluator>,
    pub profile_id: String,
    pub max_proposals: usize,
    pub max_objective_bytes: usize,
    pub max_context_refs: usize,
    pub managed_root: PathBuf,
    pub inputs: Vec<ChildInput>,
    pub max_input_files: usize,
    pub max_input_bytes: u64,
    pub permissions: crate::types::WorkPermissions,
    pub work_tokens: u64,
    pub work_cost_micro_usd: u64,
    pub verification_tokens: u64,
    pub verification_cost_micro_usd: u64,
}

/// Exact file allowlist, not recursive traversal or glob expansion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildInput {
    pub root: PathBuf,
    pub files: Vec<InputFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputFile {
    pub path: PathBuf,
    pub sha256: String,
}

impl ChildProfile {
    pub fn template_id(&self, campaign: &str) -> String {
        format!("dynamic-{campaign}-{}", self.profile_id)
    }

    pub fn slots(&self, campaign: &str) -> CampaignTemplate {
        let template_id = self.template_id(campaign);
        CampaignTemplate {
            template_id: template_id.clone(),
            group_id: Some(template_id.clone()),
            max_running: 1,
            specs: (0..self.max_proposals)
                .map(|index| {
                    let id = format!("{template_id}-{index}");
                    let root = self.managed_root.join(&id);
                    CampaignChild {
                        evaluator: self.evaluator.clone(),
                        work_id: Some(id),
                        objective: "Host dynamic slot; not an executable objective".into(),
                        workspace: root.join("work"),
                        home: root.join("home"),
                        work_tokens: self.work_tokens,
                        work_cost_micro_usd: self.work_cost_micro_usd,
                        verification_tokens: self.verification_tokens,
                        verification_cost_micro_usd: self.verification_cost_micro_usd,
                    }
                })
                .collect(),
        }
    }
}

impl CampaignChildren {
    pub fn execution_templates(&self, campaign: &str) -> Vec<CampaignTemplate> {
        self.templates
            .iter()
            .cloned()
            .chain(
                self.dynamic
                    .iter()
                    .flat_map(|d| &d.profiles)
                    .map(|p| p.slots(campaign)),
            )
            .collect()
    }
}

fn default_child_depth() -> usize {
    1
}

fn is_default_child_depth(depth: &usize) -> bool {
    *depth == 1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildCompletion {
    CancelOutstanding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignTemplate {
    pub template_id: String,
    pub group_id: Option<String>,
    pub max_running: usize,
    pub specs: Vec<CampaignChild>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignChild {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluator: Option<ChildEvaluator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_id: Option<String>,
    pub objective: String,
    pub workspace: PathBuf,
    pub home: PathBuf,
    pub work_tokens: u64,
    pub work_cost_micro_usd: u64,
    pub verification_tokens: u64,
    pub verification_cost_micro_usd: u64,
}

impl CampaignChild {
    pub fn resolved_id(&self, campaign: &str, template: &str, index: usize) -> String {
        self.work_id
            .clone()
            .unwrap_or_else(|| format!("{campaign}-{template}-{index}"))
    }
}

/// Host-only repair bounds; executable, command and model remain campaign policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildEvaluator {
    pub max_attempts: u32,
    pub max_total_command_ms: u64,
}

#[cfg(test)]
mod tests {
    #[test]
    fn compute_envelope_is_optional_strict_integer_and_gpu_disabled_by_default() {
        let p: super::ComputeEnvelope =
            serde_json::from_str(r#"{"cpu_job_ms":1000,"max_cpu_timeout_ms":100}"#).unwrap();
        p.validate().unwrap();
        assert_eq!(p.gpu_job_ms, 0);
        assert_eq!(p.max_gpu_jobs, 0);
        assert!(p.profiles.is_empty());
        for json in [
            r#"{"cpu_job_ms":1.5,"max_cpu_timeout_ms":100}"#,
            r#"{"cpu_job_ms":1000,"max_cpu_timeout_ms":100,"device_ids":["0"]}"#,
            r#"{"cpu_job_ms":1000,"max_cpu_timeout_ms":0}"#,
            r#"{"cpu_job_ms":1000,"max_cpu_timeout_ms":100,"max_gpu_jobs":1}"#,
        ] {
            assert!(serde_json::from_str::<super::ComputeEnvelope>(json)
                .map_err(|e| e.to_string())
                .and_then(|p| p.validate())
                .is_err());
        }
    }
    use super::*;
    #[test]
    fn reconciliation_strict_receipts_and_serialized_authority_defaults() {
        let docs = include_str!("../../../docs/ghost/RECOVERY.md");
        let example = docs
            .split("```json\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap();
        let example = ReconciliationReceipt::parse(example.as_bytes()).unwrap();
        let example = serde_json::to_value(example).unwrap();
        for key in example["records"][0].as_object().unwrap().keys() {
            let mut invalid = example.clone();
            invalid["records"][0].as_object_mut().unwrap().remove(key);
            assert!(
                ReconciliationReceipt::parse(&serde_json::to_vec(&invalid).unwrap()).is_err(),
                "missing model field {key}"
            );
        }
        let mut invalid = example;
        invalid["records"][0]["worker_receipt"] = "trusted".into();
        assert!(ReconciliationReceipt::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
        let value = serde_json::json!({"schema_version":1,"command_id":"operator-1","campaign_id":"campaign-1",
            "expected_state_sha256":"0".repeat(64),"evidence_reference":"case-42","records":[{
                "kind":"cleanup","work_id":"work","attempt_id":"attempt","generation":1,"reservation_id":"dispatch-1",
                "confirmation":"operator_attests_all_processes_terminated","outcome":"unverified"}]});
        let receipt = ReconciliationReceipt::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
        for key in value.as_object().unwrap().keys() {
            let mut invalid = value.clone();
            invalid.as_object_mut().unwrap().remove(key);
            assert!(ReconciliationReceipt::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
        for key in value["records"][0].as_object().unwrap().keys() {
            let mut invalid = value.clone();
            invalid["records"][0].as_object_mut().unwrap().remove(key);
            assert!(ReconciliationReceipt::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
        for (key, replacement) in [
            ("outcome", "accepted"),
            ("confirmation", "I think it stopped"),
            ("provider", "worker"),
        ] {
            let mut invalid = value.clone();
            invalid["records"][0][key] = replacement.into();
            assert!(ReconciliationReceipt::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
        for reference in [
            "https://provider/receipt?key=secret",
            "operator says yes",
            "",
            "\n",
        ] {
            let mut invalid = value.clone();
            invalid["evidence_reference"] = reference.into();
            assert!(ReconciliationReceipt::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
        use crate::types::ApiRequest;
        let request = ApiRequest::CampaignReconcile {
            id: receipt.campaign_id.clone(),
            receipt,
            unisolated_development: true,
            confirm_authoritative: true,
        };
        let mut wire = serde_json::to_value(&request).unwrap();
        let roundtrip: ApiRequest = serde_json::from_value(wire.clone()).unwrap();
        assert!(matches!(
            roundtrip,
            ApiRequest::CampaignReconcile {
                unisolated_development: true,
                confirm_authoritative: true,
                ..
            }
        ));
        wire.as_object_mut()
            .unwrap()
            .remove("unisolated_development");
        wire.as_object_mut()
            .unwrap()
            .remove("confirm_authoritative");
        assert!(matches!(
            serde_json::from_value::<ApiRequest>(wire).unwrap(),
            ApiRequest::CampaignReconcile {
                unisolated_development: false,
                confirm_authoritative: false,
                ..
            }
        ));
        assert!(ReconciliationReceipt::parse(&vec![b' '; MANIFEST_MAX_BYTES + 1]).is_err());
    }

    fn fixture() -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1, "campaign_id": "campaign-00000000000000000000000000000000",
            "objective": "Publish one candidate", "executable": "/opt/tachyon/ghost",
            "workspace": "/tmp/campaign/work", "home": "/tmp/campaign/home", "deadline_ms": 10000,
            "work_tokens": 100, "work_cost_micro_usd": 100,
            "verification_tokens": 10, "verification_cost_micro_usd": 10, "max_active_inferences": 2,
            "model": "fixture", "pricing_revision": "fixture-v1", "max_request_bytes": 1000,
            "input_tokens": 20, "output_tokens": 10, "input_micro_usd_per_million": 1000000,
            "output_micro_usd_per_million": 1000000, "other_micro_usd": 0,
            "evaluator": {"argv": ["/usr/bin/true"], "timeout_ms": 100, "output_bytes": 1024,
                "input_bytes": 1024, "max_attempts": 1, "max_total_command_ms": 100}
        })
    }
    fn parse(value: &serde_json::Value) -> Result<CampaignManifest, String> {
        CampaignManifest::parse(&serde_json::to_vec(value).unwrap(), 1)
    }
    #[test]
    fn retained_storage_manifest_is_optional_positive_and_strict_integer() {
        let mut value = fixture();
        assert_eq!(parse(&value).unwrap().retained_storage_bytes, None);
        for invalid in [
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!("1024"),
        ] {
            value["retained_storage_bytes"] = invalid;
            assert!(parse(&value).is_err());
        }
        value["retained_storage_bytes"] = serde_json::json!(1024);
        assert_eq!(parse(&value).unwrap().retained_storage_bytes, Some(1024));
        let overflowing = serde_json::to_string(&value).unwrap().replace(
            "\"retained_storage_bytes\":1024",
            "\"retained_storage_bytes\":18446744073709551616",
        );
        assert!(CampaignManifest::parse(overflowing.as_bytes(), 1).is_err());
    }
    fn child_repair_bounds(value: &serde_json::Value, pointer: &str) {
        let original = parse(value).unwrap();
        for (policy, valid) in [
            (
                serde_json::json!({"max_attempts":2,"max_total_command_ms":200}),
                true,
            ),
            (
                serde_json::json!({"max_attempts":8,"max_total_command_ms":2400000}),
                true,
            ),
            (
                serde_json::json!({"max_attempts":0,"max_total_command_ms":200}),
                false,
            ),
            (
                serde_json::json!({"max_attempts":9,"max_total_command_ms":900}),
                false,
            ),
            (
                serde_json::json!({"max_attempts":2,"max_total_command_ms":199}),
                false,
            ),
            (
                serde_json::json!({"max_attempts":2,"max_total_command_ms":2400001}),
                false,
            ),
            (serde_json::json!({"max_attempts":2}), false),
            (
                serde_json::json!({"max_attempts":2,"max_total_command_ms":200,"model":"override"}),
                false,
            ),
            (
                serde_json::json!({"max_attempts":2,"max_total_command_ms":200,"argv":["/usr/bin/true"]}),
                false,
            ),
        ] {
            let mut changed = value.clone();
            changed.pointer_mut(pointer).unwrap()["evaluator"] = policy;
            let parsed = parse(&changed);
            assert_eq!(parsed.is_ok(), valid, "{changed}");
            if let Ok(parsed) = parsed {
                assert_eq!(
                    parse(&serde_json::to_value(&parsed).unwrap()).unwrap(),
                    parsed
                );
                assert_eq!(parsed.work_tokens, original.work_tokens);
                assert_eq!(parsed.verification_tokens, original.verification_tokens);
                assert_eq!(parsed.child_work_ids(), original.child_work_ids());
                assert!(parsed
                    .children
                    .unwrap()
                    .execution_templates(&parsed.campaign_id)[0]
                    .specs[0]
                    .evaluator
                    .is_some());
            }
        }
    }
    #[test]
    fn dynamic_profiles_are_finite_strict_and_share_the_root_envelope() {
        let mut v = fixture();
        v["max_active_inferences"] = 6.into();
        v["children"] = serde_json::json!({
            "total_work":6,"max_running":1,"max_resident":2,"controls":["spawn","group","wait"],
            "history":false,"completion":"cancel_outstanding","templates":[],
            "dynamic":{"max_proposals":2,"profiles":[{
                "profile_id":"inspect","max_proposals":2,"max_objective_bytes":1024,"max_context_refs":0,
                "managed_root":"/tmp/managed/children","max_input_files":2,"max_input_bytes":1024,
                "inputs":[{"root":"/tmp/approved/code","files":[{"path":"lib.rs","sha256":"0".repeat(64)}]}],
                "permissions":{"task_type":"coding_read_only","allow_exec":false,"allow_python":false},
                "work_tokens":30,"work_cost_micro_usd":30,"verification_tokens":2,"verification_cost_micro_usd":2
            }]}
        });
        let m = parse(&v).unwrap();
        assert_eq!(m.child_work_ids().len(), 2);
        assert_eq!(m.children.as_ref().unwrap().max_depth, 1);
        for depth in [0, 1, 2, 8, 9] {
            let mut nested = v.clone();
            nested["children"]["max_depth"] = depth.into();
            nested["children"]["dynamic"]["profiles"][0]["profile_ids"] =
                serde_json::json!(["inspect"]);
            assert_eq!(parse(&nested).is_ok(), (1..=8).contains(&depth));
            if let Ok(nested) = parse(&nested) {
                assert_eq!(nested.child_work_ids(), m.child_work_ids());
                assert_eq!(nested.work_tokens, m.work_tokens);
            }
        }
        for ids in [
            serde_json::json!(["unknown"]),
            serde_json::json!(["inspect", "inspect"]),
        ] {
            let mut invalid = v.clone();
            invalid["children"]["dynamic"]["profiles"][0]["profile_ids"] = ids;
            assert!(parse(&invalid).is_err());
        }
        child_repair_bounds(&v, "/children/dynamic/profiles/0");
        assert_eq!(parse(&serde_json::to_value(&m).unwrap()).unwrap(), m);
        for key in [
            "work_tokens",
            "work_cost_micro_usd",
            "verification_tokens",
            "verification_cost_micro_usd",
        ] {
            let mut invalid = v.clone();
            invalid["children"]["dynamic"]["profiles"][0][key] = invalid[key].clone();
            assert!(parse(&invalid).is_err(), "finite {key}");
        }
        for path in [
            "../secret",
            "/etc/passwd",
            ".env",
            "src/.git/config",
            "**/*",
            "src/../lib.rs",
        ] {
            let mut invalid = v.clone();
            invalid["children"]["dynamic"]["profiles"][0]["inputs"][0]["files"][0]["path"] =
                path.into();
            assert!(parse(&invalid).is_err(), "{path}");
        }
        for (key, value) in [
            ("max_proposals", 32),
            ("max_objective_bytes", 16385),
            ("max_context_refs", 1),
            ("max_input_files", 0),
        ] {
            let mut invalid = v.clone();
            invalid["children"]["dynamic"]["profiles"][0][key] = value.into();
            assert!(parse(&invalid).is_err(), "{key}");
        }
        let mut invalid = v.clone();
        invalid["children"]["dynamic"]["profiles"][0]["permissions"]["task_type"] =
            "coding_read_write".into();
        assert!(parse(&invalid).is_err());
        for key in [
            "max_depth",
            "model",
            "retry_cap",
            "environment",
            "objective",
        ] {
            let mut invalid = v.clone();
            invalid["children"]["dynamic"]["profiles"][0][key] = 1.into();
            assert!(parse(&invalid).is_err(), "no policy override {key}");
        }
    }

    #[test]
    fn explicit_children_roundtrip_bounds_and_no_policy_overrides() {
        let mut v = fixture();
        v["children"] = serde_json::json!({
            "total_work": 4, "max_running": 2, "max_resident": 2,
            "controls": ["spawn", "wait", "status"], "history": false,
            "completion": "cancel_outstanding", "templates": [{
                "template_id": "one", "group_id": null, "max_running": 1,
                "specs": [{"work_id": "child", "objective": "Publish a candidate",
                    "workspace": "/tmp/child/work", "home": "/tmp/child/home",
                    "work_tokens": 40, "work_cost_micro_usd": 40,
                    "verification_tokens": 2, "verification_cost_micro_usd": 2}]
            }]
        });
        v["max_active_inferences"] = 4.into();
        child_repair_bounds(&v, "/children/templates/0/specs/0");
        let m = parse(&v).unwrap();
        for mode in ["fixed", "model_proposed", "deterministic"] {
            let mut selected = v.clone();
            selected["allocation"] = serde_json::json!({"mode":mode,"max_running":2});
            let selected_manifest = parse(&selected).unwrap();
            assert_eq!(
                parse(&serde_json::to_value(&selected_manifest).unwrap()).unwrap(),
                selected_manifest
            );
            for cap in [0, 3, 65, 1000] {
                let mut invalid = selected.clone();
                invalid["allocation"]["max_running"] = cap.into();
                assert!(parse(&invalid).is_err());
            }
            selected["allocation"]["budget"] = 1.into();
            assert!(parse(&selected).is_err());
        }
        let mut selected = v.clone();
        selected["allocation"] = serde_json::json!({
            "mode":"deterministic", "max_running":2,
            "allowed_actions":["pause","stop"],
            "signals":[{"command_id":"stop-one", "group_id":"batch", "expected_revision":1,
                "action":{"kind":"stop", "work_id":"existing", "generation":1}}]
        });
        parse(&selected).unwrap();
        for (pointer, value) in [
            ("/allocation/allowed_actions", serde_json::json!(["resize"])),
            (
                "/allocation/allowed_actions",
                serde_json::json!(["stop", "stop"]),
            ),
            ("/allocation/allowed_actions", serde_json::json!(["work"])),
            ("/allocation/allowed_actions", serde_json::json!(["verify"])),
            ("/allocation/mode", serde_json::json!("fixed")),
            (
                "/allocation/signals/0/expected_revision",
                serde_json::json!(0),
            ),
            (
                "/allocation/signals/0/action/generation",
                serde_json::json!(0),
            ),
            (
                "/allocation/signals/0/action/work_id",
                serde_json::json!(""),
            ),
        ] {
            let mut invalid = selected.clone();
            *invalid.pointer_mut(pointer).unwrap() = value;
            assert!(parse(&invalid).is_err(), "{pointer}");
        }
        let signal = selected["allocation"]["signals"][0].clone();
        selected["allocation"]["signals"] = serde_json::json!([signal, signal]);
        assert!(parse(&selected).is_err());
        for action in [
            serde_json::json!({"kind":"work", "template_id":"approved", "parent_work_id":"parent", "generation":1, "instruction_revision":1}),
            serde_json::json!({"kind":"verify", "work_id":"existing", "generation":1, "instruction_revision":1}),
        ] {
            selected["allocation"]["allowed_actions"] = serde_json::json!(["work", "verify"]);
            selected["allocation"]["signals"] = serde_json::json!([{
                "command_id":"execution-action", "group_id":"batch", "expected_revision":1, "action":action
            }]);
            let parsed = parse(&selected).unwrap();
            assert_eq!(
                parse(&serde_json::to_value(parsed).unwrap())
                    .unwrap()
                    .allocation
                    .unwrap()
                    .signals
                    .len(),
                1
            );
            for field in ["objective", "budget", "evaluator_id"] {
                let mut invalid = selected.clone();
                invalid["allocation"]["signals"][0]["action"][field] = "override".into();
                assert!(parse(&invalid).is_err(), "{field}");
            }
            selected["allocation"]["signals"][0]["action"]["instruction_revision"] = 0.into();
            assert!(parse(&selected).is_err());
        }
        assert_eq!(parse(&serde_json::to_value(&m).unwrap()).unwrap(), m);
        for slots in [1, 2, 3] {
            let mut invalid = m.clone();
            invalid.max_active_inferences = slots;
            assert!(invalid.validate(1).is_err(), "admission holds: {slots}");
        }
        let mut serial = m.clone();
        serial.children.as_mut().unwrap().max_running = 1;
        serial.validate(1).unwrap();
        for work in [
            format!("{}-root", m.campaign_id),
            format!("{}-verification", m.campaign_id),
            m.campaign_id.clone(),
        ] {
            let mut invalid = m.clone();
            invalid.children.as_mut().unwrap().templates[0].specs[0].work_id = Some(work);
            assert!(
                invalid.validate(1).is_err(),
                "work/verifier identity collision"
            );
        }
        let mut namespaces = m.clone();
        let template = &mut namespaces.children.as_mut().unwrap().templates[0];
        template.template_id = "child".into();
        template.group_id = Some("child".into());
        namespaces.validate(1).unwrap();
        for key in [
            "model",
            "executable",
            "provider",
            "parent",
            "campaign_id",
            "evaluator",
        ] {
            let mut invalid = v.clone();
            invalid["children"]["templates"][0]["specs"][0][key] = "forbidden".into();
            assert!(parse(&invalid).is_err(), "{key}");
        }
        for path in [
            "/tmp/campaign/work",
            "/tmp/child/home",
            "/tmp/child/../work",
            "relative",
        ] {
            let mut invalid = v.clone();
            invalid["children"]["templates"][0]["specs"][0]["workspace"] = path.into();
            assert!(parse(&invalid).is_err(), "{path}");
        }
        for key in [
            "work_tokens",
            "work_cost_micro_usd",
            "verification_tokens",
            "verification_cost_micro_usd",
        ] {
            let mut invalid = v.clone();
            invalid["children"]["templates"][0]["specs"][0][key] = invalid[key].clone();
            assert!(parse(&invalid).is_err(), "root reserve: {key}");
        }
        for key in v["children"].as_object().unwrap().keys() {
            let mut invalid = v.clone();
            invalid["children"].as_object_mut().unwrap().remove(key);
            assert!(parse(&invalid).is_err(), "missing child policy {key}");
        }
        let old = parse(&fixture()).unwrap();
        assert!(old.children.is_none());
        assert_eq!(serde_json::to_value(old).unwrap(), fixture());
        v["children"]["templates"][0]["specs"][0]
            .as_object_mut()
            .unwrap()
            .remove("work_id");
        let generated = parse(&v).unwrap();
        assert_eq!(
            generated.child_work_ids(),
            vec![format!("{}-one-0", generated.campaign_id)]
        );
        assert_eq!(
            generated.child_work_ids(),
            parse(&serde_json::to_value(&generated).unwrap())
                .unwrap()
                .child_work_ids()
        );
        let mut other = generated.clone();
        other.campaign_id = "campaign-11111111111111111111111111111111".into();
        assert_ne!(other.child_work_ids(), generated.child_work_ids());
        let mut collision = generated.children.as_ref().unwrap().templates[0].clone();
        collision.template_id = "collision".into();
        collision.specs[0].work_id = Some(generated.child_work_ids()[0].clone());
        let mut invalid = generated;
        invalid.max_active_inferences = 6;
        invalid.children.as_mut().unwrap().total_work = 6;
        invalid.children.as_mut().unwrap().templates.push(collision);
        assert!(invalid.validate(1).is_err());
    }
    #[test]
    fn documented_manifests_validate_with_explicit_deadline_replacement() {
        let docs = include_str!("../../../docs/ghost/CAMPAIGNS.md");
        let mut count = 0;
        for block in docs.split("```json\n").skip(1) {
            let json = block.split("```").next().unwrap();
            let mut manifest: CampaignManifest = serde_json::from_str(json).unwrap();
            manifest.deadline_ms = 60000;
            manifest.validate(1).unwrap();
            count += 1;
        }
        assert_eq!(count, 2);
    }
    #[test]
    fn manifest_requires_every_bound_and_denies_unknown_fields() {
        let valid = fixture();
        assert!(parse(&valid).is_ok());
        for key in valid.as_object().unwrap().keys() {
            let mut v = valid.clone();
            v.as_object_mut().unwrap().remove(key);
            assert!(parse(&v).is_err(), "missing {key}");
        }
        for key in [
            "api_key",
            "provider",
            "base_url",
            "artifact_root",
            "allow_launch",
            "extra",
        ] {
            let mut v = valid.clone();
            v[key] = "forbidden".into();
            assert!(parse(&v).is_err());
        }
        let mut v = valid.clone();
        v["evaluator"]["extra"] = true.into();
        assert!(parse(&v).is_err());
        for key in valid["evaluator"].as_object().unwrap().keys() {
            let mut v = valid.clone();
            v["evaluator"].as_object_mut().unwrap().remove(key);
            assert!(parse(&v).is_err(), "missing evaluator {key}");
        }
        assert!(CampaignManifest::parse(&vec![b' '; MANIFEST_MAX_BYTES + 1], 0).is_err());
    }
    #[test]
    fn unsafe_roots_deadlines_and_overflow_are_denied_without_io() {
        for p in ["/", "/tmp", "relative", "/tmp/../work", "/tmp/work/./other"] {
            let mut v = fixture();
            v["workspace"] = p.into();
            assert!(parse(&v).is_err(), "{p}");
        }
        for key in [
            "deadline_ms",
            "work_tokens",
            "work_cost_micro_usd",
            "verification_tokens",
            "verification_cost_micro_usd",
            "input_tokens",
            "output_tokens",
            "max_request_bytes",
        ] {
            let mut v = fixture();
            v[key] = 0.into();
            assert!(parse(&v).is_err(), "{key}");
        }
        let mut v = fixture();
        v["input_tokens"] = u64::MAX.into();
        assert!(parse(&v).is_err());
        let mut v = fixture();
        v["home"] = v["workspace"].clone();
        assert!(parse(&v).is_err());
        for key in ["work_tokens", "work_cost_micro_usd"] {
            let mut v = fixture();
            v[key] = u64::MAX.into();
            assert!(parse(&v).is_err(), "overflowing {key}");
        }
        for executable in ["/", "/bin/../usr/bin/true", "/usr/bin/./true"] {
            let mut v = fixture();
            v["evaluator"]["argv"][0] = executable.into();
            assert!(parse(&v).is_err());
        }
    }

    #[test]
    fn metric_contract_is_strict_bounded_and_additive() {
        for bounds in [
            r#"{"min":0,"min":1}"#,
            r#"{"min":null,"min":1}"#,
            r#"{"min":1e999}"#,
            r#"{"max":-1e999}"#,
            r#"{"min":false}"#,
            r#"{"min":NaN}"#,
            r#"{"min":0,"unknown":1}"#,
        ] {
            let valid = serde_json::from_str::<MetricBounds>(bounds).is_ok_and(|bounds| {
                ResultContract::JsonMetrics
                    .validate(&[("score".into(), bounds)].into(), false)
                    .is_ok()
            });
            assert!(!valid, "{bounds}");
        }
        assert!(serde_json::from_str::<CampaignEvaluator>(r#"{"argv":["/usr/bin/true"],"timeout_ms":100,"output_bytes":1024,"input_bytes":1024,"max_attempts":1,"max_total_command_ms":100,"result_contract":"json_metrics","metrics":{"score":{"min":1},"score":{"min":0}}}"#).is_err());
        let old = fixture();
        assert_eq!(serde_json::to_value(parse(&old).unwrap()).unwrap(), old);
        let mut value = old.clone();
        value["evaluator"]["result_contract"] = "json_metrics".into();
        assert!(parse(&value).is_err());
        value["evaluator"]["metrics"] = serde_json::json!({"score":{"min":0.9,"max":1}});
        let manifest = parse(&value).unwrap();
        assert_eq!(
            parse(&serde_json::to_value(&manifest).unwrap()).unwrap(),
            manifest
        );
        for bounds in [
            serde_json::json!({}),
            serde_json::json!({"min":2,"max":1}),
            serde_json::json!({"min":"0.9"}),
            serde_json::json!({"min":0,"path":"/tmp/secret"}),
        ] {
            let mut invalid = value.clone();
            invalid["evaluator"]["metrics"]["score"] = bounds;
            assert!(parse(&invalid).is_err());
        }
        for name in ["", "../score", "https://host/key", "score.value"] {
            let mut invalid = value.clone();
            invalid["evaluator"]["metrics"] = serde_json::json!({name:{"min":0}});
            assert!(parse(&invalid).is_err());
        }
        for contract in ["human_acceptance", "unknown", "exit_success"] {
            let mut invalid = value.clone();
            invalid["evaluator"]["result_contract"] = contract.into();
            assert!(parse(&invalid).is_err());
        }
        value["evaluator"]["stage"] = "final_heldout".into();
        assert!(parse(&value).is_ok());
        value["evaluator"]["max_attempts"] = 2.into();
        value["evaluator"]["max_total_command_ms"] = 200.into();
        assert!(parse(&value).is_err());
        value["evaluator"]["stage"] = "development".into();
        assert!(parse(&value).is_ok());
        value["evaluator"]["acceptance_mode"] = "human".into();
        assert!(parse(&value).is_err());
    }

    #[test]
    fn human_contract_requires_explicit_root_only_nonexecuting_policy_and_strict_decision() {
        let mut value = fixture();
        value["evaluator"] =
            serde_json::json!({"acceptance_mode":"human","input_bytes":1024,"max_attempts":1});
        let manifest = parse(&value).unwrap();
        assert!(manifest.evaluator.argv.is_empty());
        assert_eq!(
            parse(&serde_json::to_value(&manifest).unwrap()).unwrap(),
            manifest
        );
        for (key, replacement) in [
            ("argv", serde_json::json!(["/usr/bin/true"])),
            ("timeout_ms", 1.into()),
            ("output_bytes", 1.into()),
            ("max_total_command_ms", 1.into()),
            ("max_attempts", 2.into()),
            ("result_contract", "json_metrics".into()),
            ("metrics", serde_json::json!({"score":{"min":0}})),
            ("stage", "development".into()),
            ("path", "/tmp/input".into()),
            ("api_key", "forbidden".into()),
        ] {
            let mut invalid = value.clone();
            invalid["evaluator"][key] = replacement;
            assert!(parse(&invalid).is_err(), "{key}");
        }
        value["children"] = serde_json::json!({"total_work":2,"max_running":1,"max_resident":2,"controls":["spawn"],"history":false,"completion":"cancel_outstanding","templates":[]});
        assert!(parse(&value).unwrap_err().contains("root-only"));
        let wire = serde_json::json!({"cmd":"campaign_acceptance_decide","campaign_id":"campaign-test","command_id":"operator-1","candidate":"artifact-1","candidate_sha256":"0".repeat(64),"expected_state_sha256":"1".repeat(64),"decision":"accept","confirm":true});
        let request: crate::types::ApiRequest = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(request).unwrap(), wire);
        for key in wire.as_object().unwrap().keys().filter(|key| *key != "cmd") {
            let mut invalid = wire.clone();
            invalid.as_object_mut().unwrap().remove(key);
            assert!(
                serde_json::from_value::<crate::types::ApiRequest>(invalid).is_err(),
                "{key}"
            );
        }
        for key in ["answer", "host_uid", "path", "budget", "api_key"] {
            let mut invalid = wire.clone();
            invalid[key] = "forbidden".into();
            assert!(
                serde_json::from_value::<crate::types::ApiRequest>(invalid).is_err(),
                "{key}"
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignEvaluator {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_mode: Option<AcceptanceMode>,
    #[serde(default, skip_serializing_if = "ResultContract::is_default")]
    pub result_contract: ResultContract,
    #[serde(
        default,
        skip_serializing_if = "std::collections::BTreeMap::is_empty",
        deserialize_with = "deserialize_metric_constraints"
    )]
    pub metrics: std::collections::BTreeMap<String, MetricBounds>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_extra_metrics: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<EvaluationStage>,
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub timeout_ms: u64,
    #[serde(default)]
    pub output_bytes: usize,
    pub input_bytes: u64,
    pub max_attempts: u32,
    #[serde(default)]
    pub max_total_command_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceMode {
    Human,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanDecision {
    Accept,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceQuery {
    pub campaign_id: String,
}

/// Privileged same-user attestation. No paths, free text, or worker-supplied attribution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceDecision {
    pub command_id: String,
    pub campaign_id: String,
    pub candidate: String,
    pub candidate_sha256: String,
    pub expected_state_sha256: String,
    pub decision: HumanDecision,
    pub confirm: bool,
}

impl AcceptanceDecision {
    pub fn validate(&self) -> Result<(), String> {
        let id = |s: &str| {
            !s.is_empty()
                && s.len() <= 256
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
        };
        let hash = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if !self.confirm
            || !id(&self.command_id)
            || !id(&self.campaign_id)
            || !id(&self.candidate)
            || !hash(&self.candidate_sha256)
            || !hash(&self.expected_state_sha256)
        {
            return Err("explicit --confirm, bounded identifiers and exact lowercase SHA-256 hashes required".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceRequest {
    pub campaign_id: String,
    pub work_id: String,
    pub candidate: String,
    pub candidate_sha256: String,
    pub config_hash: String,
    pub generation: u64,
    pub assignment: u64,
    pub instruction_revision: u64,
    pub deadline_ms: u64,
    pub expected_state_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceReceipt {
    pub request: AcceptanceRequest,
    pub decision: AcceptanceDecision,
    pub source: AcceptanceMode,
    pub host_uid: u32,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultContract {
    #[default]
    ExitSuccess,
    JsonMetrics,
}

impl ResultContract {
    pub fn is_default(&self) -> bool {
        *self == Self::ExitSuccess
    }

    pub fn validate(
        &self,
        metrics: &std::collections::BTreeMap<String, MetricBounds>,
        allow_extra: bool,
    ) -> Result<(), String> {
        if *self == Self::ExitSuccess {
            return if metrics.is_empty() && !allow_extra {
                Ok(())
            } else {
                Err("metrics require json_metrics result_contract".into())
            };
        }
        if metrics.is_empty() || metrics.len() > 64 {
            return Err("json_metrics requires 1..=64 host metric constraints".into());
        }
        for (name, bounds) in metrics {
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            {
                return Err(
                    "metric names must be bounded identifiers, not paths or credentials".into(),
                );
            }
            let finite = |n: &serde_json::Number| n.as_f64().is_some_and(f64::is_finite);
            if bounds.min.is_none() && bounds.max.is_none()
                || bounds.min.iter().chain(&bounds.max).any(|n| !finite(n))
                || bounds
                    .min
                    .as_ref()
                    .zip(bounds.max.as_ref())
                    .is_some_and(|(min, max)| min.as_f64() > max.as_f64())
            {
                return Err(format!("invalid finite inclusive bounds for metric {name}"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricBounds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<serde_json::Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<serde_json::Number>,
}

pub fn deserialize_metric_constraints<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<std::collections::BTreeMap<String, MetricBounds>, D::Error> {
    struct Constraints;
    impl<'de> serde::de::Visitor<'de> for Constraints {
        type Value = std::collections::BTreeMap<String, MetricBounds>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("unique host metric constraints")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut constraints = Self::Value::new();
            while let Some((name, bounds)) = map.next_entry::<String, MetricBounds>()? {
                if constraints.len() >= 64 || constraints.insert(name, bounds).is_some() {
                    return Err(serde::de::Error::custom(
                        "duplicate or excessive metric constraints",
                    ));
                }
            }
            Ok(constraints)
        }
    }
    deserializer.deserialize_map(Constraints)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationStage {
    Development,
    FinalHeldout,
}

impl CampaignManifest {
    pub fn child_work_ids(&self) -> Vec<String> {
        self.children
            .iter()
            .flat_map(|c| c.execution_templates(&self.campaign_id))
            .flat_map(|t| {
                t.specs
                    .iter()
                    .enumerate()
                    .map(|(index, s)| s.resolved_id(&self.campaign_id, &t.template_id, index))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn parse(bytes: &[u8], now_ms: u64) -> Result<Self, String> {
        if bytes.len() > MANIFEST_MAX_BYTES {
            return Err("manifest exceeds 65536 bytes".into());
        }
        let manifest: Self = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        manifest.validate(now_ms)?;
        Ok(manifest)
    }

    pub fn validate(&self, now_ms: u64) -> Result<(), String> {
        if self.retained_storage_bytes == Some(0) {
            return Err("retained_storage_bytes must be positive".into());
        }
        if let Some(compute) = &self.compute {
            compute.validate()?;
        }
        if let Some(a) = &self.allocation {
            a.validate()?;
            if !(1..=64).contains(&a.max_running)
                || self
                    .children
                    .as_ref()
                    .is_none_or(|c| a.max_running > c.max_running)
            {
                return Err(
                    "allocation requires children and a bounded own concurrency cap".into(),
                );
            }
        }
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > MANIFEST_MAX_BYTES {
            return Err("manifest exceeds 65536 bytes".into());
        }
        let text = |s: &str, max| !s.trim().is_empty() && s.len() <= max && !s.contains('\0');
        let path = |p: &Path| {
            p.is_absolute()
                && p.as_os_str().len() <= 4096
                && p.components().count() > 2
                && p.components()
                    .all(|c| matches!(c, Component::RootDir | Component::Normal(_)))
                && p.components().collect::<PathBuf>().as_os_str() == p.as_os_str()
        };
        let e = &self.evaluator;
        let human = e.acceptance_mode == Some(AcceptanceMode::Human);
        if human
            && (self.children.is_some()
                || self.allocation.is_some()
                || e.result_contract != ResultContract::ExitSuccess
                || !e.metrics.is_empty()
                || e.allow_extra_metrics
                || e.stage.is_some()
                || !e.argv.is_empty()
                || e.timeout_ms != 0
                || e.output_bytes != 0
                || e.max_total_command_ms != 0
                || e.max_attempts != 1)
        {
            return Err("human acceptance is root-only, one attempt, without command, metrics, stage or child policy".into());
        }
        e.result_contract
            .validate(&e.metrics, e.allow_extra_metrics)?;
        if e.stage == Some(EvaluationStage::FinalHeldout) && e.max_attempts != 1 {
            return Err("final_heldout requires max_attempts=1; no repair exposure loop".into());
        }
        if e.stage == Some(EvaluationStage::FinalHeldout) && self.children.is_some() {
            return Err("final_heldout is root-only to prevent repeated child exposure".into());
        }
        if self.schema_version != 1
            || !self.campaign_id.strip_prefix("campaign-").is_some_and(|s| {
                s.len() == 32
                    && s.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            || !text(&self.objective, 16384)
            || !text(&self.model, 256)
            || !text(&self.pricing_revision, 256)
            || ![&self.executable, &self.workspace, &self.home]
                .into_iter()
                .all(|p| path(p))
            || self.workspace.starts_with(&self.home)
            || self.home.starts_with(&self.workspace)
            || self.deadline_ms <= now_ms
            || self.deadline_ms - now_ms > 86_400_000
            || self.work_tokens == 0
            || self.work_cost_micro_usd == 0
            || self.verification_tokens == 0
            || self.verification_cost_micro_usd == 0
            || !(2..=64).contains(&self.max_active_inferences)
            || !(1..=16_777_216).contains(&self.max_request_bytes)
            || self.input_tokens == 0
            || self.output_tokens == 0
            || (!human
                && (e.argv.is_empty()
                    || e.argv.len() > 256
                    || !path(Path::new(&e.argv[0]))
                    || e.argv.iter().any(|s| s.contains('\0'))
                    || e.argv.iter().map(String::len).sum::<usize>() > 32768
                    || !(1..=300_000).contains(&e.timeout_ms)
                    || !(1..=32768).contains(&e.output_bytes)))
            || !(1..=16_777_216).contains(&e.input_bytes)
            || !(1..=8).contains(&e.max_attempts)
            || e.max_total_command_ms < e.timeout_ms.saturating_mul(u64::from(e.max_attempts))
            || e.max_total_command_ms > 2_400_000
        {
            return Err("invalid campaign manifest: explicit bounded policy and normalized dedicated absolute paths required".into());
        }
        let tokens = self
            .input_tokens
            .checked_add(u64::from(self.output_tokens))
            .ok_or("token overflow")?;
        self.work_tokens
            .checked_add(self.verification_tokens)
            .ok_or("campaign token envelope overflow")?;
        self.work_cost_micro_usd
            .checked_add(self.verification_cost_micro_usd)
            .ok_or("campaign cost envelope overflow")?;
        let cost = (u128::from(self.input_tokens) * u128::from(self.input_micro_usd_per_million)
            + u128::from(self.output_tokens) * u128::from(self.output_micro_usd_per_million))
        .div_ceil(1_000_000)
            + u128::from(self.other_micro_usd);
        if tokens > self.work_tokens || cost > u128::from(self.work_cost_micro_usd) {
            return Err("request upper bound exceeds work budget".into());
        }
        if let Some(children) = &self.children {
            for child in children
                .execution_templates(&self.campaign_id)
                .iter()
                .flat_map(|t| &t.specs)
            {
                if let Some(repair) = &child.evaluator {
                    if !(1..=8).contains(&repair.max_attempts)
                        || repair.max_total_command_ms
                            < e.timeout_ms.saturating_mul(u64::from(repair.max_attempts))
                        || repair.max_total_command_ms > 2_400_000
                    {
                        return Err("invalid host child evaluator bounds".into());
                    }
                }
            }
            use std::collections::BTreeSet;
            let id = |s: &str| {
                !s.is_empty()
                    && s.len() <= 160
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            };
            if let Some(dynamic) = &children.dynamic {
                let mut profiles = BTreeSet::new();
                if !(1..=31).contains(&dynamic.max_proposals)
                    || dynamic.profiles.is_empty()
                    || dynamic.profiles.len() > 16
                    || dynamic
                        .profiles
                        .iter()
                        .any(|p| !(1..=31).contains(&p.max_proposals))
                    || dynamic
                        .profiles
                        .iter()
                        .map(|p| p.max_proposals)
                        .sum::<usize>()
                        != dynamic.max_proposals
                {
                    return Err("invalid finite dynamic proposal pool".into());
                }
                for p in &dynamic.profiles {
                    if p.profile_ids.iter().collect::<BTreeSet<_>>().len() != p.profile_ids.len()
                        || p.profile_ids
                            .iter()
                            .any(|id| !dynamic.profiles.iter().any(|p| &p.profile_id == id))
                    {
                        return Err(
                            "inherited profile_ids require unique approved selectors".into()
                        );
                    }
                    if !id(&p.profile_id)
                        || p.profile_id.len() > 64
                        || !profiles.insert(&p.profile_id)
                        || !(1..=16384).contains(&p.max_objective_bytes)
                        || p.max_context_refs > 4
                        || p.max_context_refs > 0 && !children.history
                        || !path(&p.managed_root)
                        || !(1..=1024).contains(&p.max_input_files)
                        || !(1..=16_777_216).contains(&p.max_input_bytes)
                        || p.inputs.is_empty()
                        || p.inputs.len() > 16
                        || p.inputs.iter().map(|i| i.files.len()).sum::<usize>() > p.max_input_files
                    {
                        return Err("invalid dynamic profile bounds".into());
                    }
                    for input in &p.inputs {
                        let mut files = BTreeSet::new();
                        if !path(&input.root)
                            || input.files.is_empty()
                            || input.root.starts_with(&p.managed_root)
                            || p.managed_root.starts_with(&input.root)
                        {
                            return Err("invalid approved input root".into());
                        }
                        for file in &input.files {
                            if file.path.as_os_str().is_empty()
                                || file.path.as_os_str().len() > 1024
                                || !file
                                    .path
                                    .components()
                                    .all(|c| matches!(c, Component::Normal(_)))
                                || file.path.components().collect::<PathBuf>() != file.path
                                || !files.insert(&file.path)
                                || file.path.components().any(|c| {
                                    c.as_os_str().to_str().is_none_or(|s| {
                                        s.starts_with('.')
                                            || s.contains('*')
                                            || s.contains('?')
                                            || s.contains('[')
                                    })
                                })
                                || file.sha256.len() != 64
                                || !file
                                    .sha256
                                    .bytes()
                                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                            {
                                return Err("inputs require explicit non-hidden relative files and SHA-256 versions".into());
                            }
                        }
                    }
                }
                let roots: Vec<_> = dynamic.profiles.iter().map(|p| &p.managed_root).collect();
                let fixed: Vec<_> = children
                    .templates
                    .iter()
                    .flat_map(|t| &t.specs)
                    .flat_map(|s| [&s.workspace, &s.home])
                    .chain([&self.workspace, &self.home])
                    .collect();
                for (index, root) in roots.iter().enumerate() {
                    if roots[..index]
                        .iter()
                        .chain(&fixed)
                        .any(|other| root.starts_with(other) || other.starts_with(root))
                        || dynamic
                            .profiles
                            .iter()
                            .flat_map(|p| &p.inputs)
                            .any(|i| root.starts_with(&i.root) || i.root.starts_with(root))
                    {
                        return Err(
                            "managed roots must be disjoint from workspaces and inputs".into()
                        );
                    }
                }
            }
            let execution_templates = children.execution_templates(&self.campaign_id);
            let count: usize = execution_templates.iter().map(|t| t.specs.len()).sum();
            if execution_templates.is_empty()
                || !(1..=8).contains(&children.max_depth)
                || children.templates.len() > 32
                || count > 127
                || children.total_work < (count + 1) * 2
                || children.total_work > 256
                || !(1..=64).contains(&children.max_running)
                || children.max_running > children.total_work
                || !(2..=64).contains(&children.max_resident)
                || children.controls.is_empty()
                || children.controls.iter().collect::<BTreeSet<_>>().len()
                    != children.controls.len()
                || children
                    .controls
                    .contains(&crate::agents::Control::Resource)
            {
                return Err("invalid explicit child limits or control allowlist".into());
            }
            // Queued Work and verifier holds occupy inference slots until funded.
            // Leave a slot for the root's next request before it can enter wait.
            if u64::from(self.max_active_inferences) < (count as u64 + 1) * 2 {
                return Err("child admission holds must leave root inference capacity".into());
            }
            let mut ids = BTreeSet::from([
                format!("{}-root", self.campaign_id),
                format!("{}-verification", self.campaign_id),
            ]);
            let mut templates = BTreeSet::new();
            let mut groups = BTreeSet::new();
            let mut paths = vec![&self.workspace, &self.home];
            let mut remaining = [
                self.work_tokens,
                self.work_cost_micro_usd,
                self.verification_tokens,
                self.verification_cost_micro_usd,
            ];
            for t in &execution_templates {
                if !id(&t.template_id)
                    || !templates.insert(&t.template_id)
                    || !(1..=32).contains(&t.specs.len())
                    || t.group_id.is_none() && t.specs.len() != 1
                    || t.group_id
                        .as_ref()
                        .is_some_and(|g| !id(g) || !groups.insert(g))
                    || t.max_running == 0
                    || t.max_running > children.max_running
                {
                    return Err("invalid child template".into());
                }
                for (index, s) in t.specs.iter().enumerate() {
                    let work_id = s.resolved_id(&self.campaign_id, &t.template_id, index);
                    if s.work_id.as_deref().is_some_and(|w| !id(w))
                        || !ids.insert(work_id.clone())
                        || !ids.insert(format!("{work_id}-verification"))
                        || !text(&s.objective, 16384)
                        || tokens > s.work_tokens
                        || cost > u128::from(s.work_cost_micro_usd)
                    {
                        return Err("invalid child identity, objective or request budget".into());
                    }
                    for p in [&s.workspace, &s.home] {
                        if !path(p)
                            || paths
                                .iter()
                                .any(|other| p.starts_with(other) || other.starts_with(p))
                        {
                            return Err(
                                "child paths must be normalized and mutually disjoint".into()
                            );
                        }
                        paths.push(p);
                    }
                    for (remaining, allocation) in remaining.iter_mut().zip([
                        s.work_tokens,
                        s.work_cost_micro_usd,
                        s.verification_tokens,
                        s.verification_cost_micro_usd,
                    ]) {
                        if allocation == 0 {
                            return Err("child budgets must be positive".into());
                        }
                        *remaining = remaining
                            .checked_sub(allocation)
                            .ok_or("child allocations exceed root envelope")?;
                    }
                }
            }
            if remaining[0] < tokens
                || u128::from(remaining[1]) < cost
                || remaining[2] == 0
                || remaining[3] == 0
            {
                return Err(
                    "child allocations must leave protected root work and verification budgets"
                        .into(),
                );
            }
        }
        Ok(())
    }
}
