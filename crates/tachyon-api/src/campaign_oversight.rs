//! Explicit background oversight requests, not daemon commands or authority grants.
use crate::{monitor::MonitorSnapshot, todo::TodoResponse, WorkReviewDecision, WorkReviewRequest};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignAssessmentRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub evidence_total: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<AssessmentEvidence>,
    pub kind: CampaignRequestKind,
    pub request_id: String,
    pub campaign_id: String,
    pub revision: u64,
    pub objective_summary: String,
    pub todo_scope: crate::todo::TodoScope,
    pub todos: TodoResponse,
    pub monitor: MonitorSnapshot,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CampaignRequestKind {
    #[serde(rename = "campaign_assessment")]
    CampaignAssessment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyReviewRequest {
    #[serde(flatten)]
    pub request: WorkReviewRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BackgroundRequest {
    CampaignAssessment(CampaignAssessmentRequest),
    Review(LegacyReviewRequest),
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum BackgroundResponse {
    Review(WorkReviewDecision),
    CampaignAssessment(CampaignAssessmentResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignAssessment {
    pub summary: String,
    pub findings: Vec<String>,
    pub refs: Vec<String>,
    pub blockers: Vec<String>,
    pub attention: Attention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Attention {
    None,
    Operator,
    InsufficientEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssessmentEvidence {
    pub reference: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedCampaignAssessment {
    pub id: String,
    pub campaign_id: String,
    pub revision: u64,
    pub objective: String,
    pub assessment: CampaignAssessment,
    pub sources: Vec<AssessmentEvidence>,
}

impl PublishedCampaignAssessment {
    pub fn advisory(&self) -> String {
        let refs = if self.assessment.refs.is_empty() {
            "no source citations supplied".into()
        } else {
            self.assessment.refs.join(", ")
        };
        format!(
            "Campaign {} advisory (unverified background assessment, revision {}): {}\nSources: {}",
            self.campaign_id, self.revision, self.assessment.summary, refs
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssessmentRecord {
    pub request_id: String,
    pub input_sha256: String,
    pub evidence_refs: Vec<String>,
    pub revision: u64,
    pub triggers: Vec<String>,
    pub status: String,
    pub published: Option<PublishedCampaignAssessment>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CampaignAssessmentResponse {
    pub request_id: String,
    pub campaign_id: String,
    pub revision: u64,
    pub result: Result<CampaignAssessment, CampaignAssessmentError>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignAssessmentError {
    InvalidRequest,
    MissingCapabilities,
    RegistryUnavailable,
    ModelUnavailable,
    TimedOut,
    ProviderError,
    MalformedOutput,
}
