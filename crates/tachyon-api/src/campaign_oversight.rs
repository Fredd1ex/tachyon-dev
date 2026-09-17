//! Explicit background oversight requests, not daemon commands or authority grants.
use crate::{monitor::MonitorSnapshot, todo::TodoResponse, WorkReviewDecision, WorkReviewRequest};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignAssessmentRequest {
    pub kind: CampaignRequestKind,
    pub request_id: String,
    pub campaign_id: String,
    pub revision: u64,
    pub objective_summary: String,
    pub todo_scope: crate::todo::TodoScope,
    pub todos: TodoResponse,
    pub monitor: MonitorSnapshot,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignAssessment {
    pub summary: String,
    pub findings: Vec<String>,
    pub refs: Vec<String>,
    pub blockers: Vec<String>,
    pub attention: Attention,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Attention {
    None,
    Operator,
    InsufficientEvidence,
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
