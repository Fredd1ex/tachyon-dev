//! Core controls for the exact Work bound to a private broker session.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status {},
    Ask {
        request_id: String,
        question: String,
        timeout_ms: u64,
    },
    Complete {
        summary: String,
        candidate_refs: Vec<String>,
        unresolved_questions: Vec<String>,
    },
}

impl Request {
    pub fn validate(&self) -> Result<(), &'static str> {
        let text = |s: &str, max| !s.trim().is_empty() && s.len() <= max && !s.contains('\0');
        let valid = match self {
            Self::Status {} => true,
            Self::Ask {
                request_id,
                question,
                timeout_ms,
            } => {
                text(request_id, 256) && text(question, 4096) && (1..=300_000).contains(timeout_ms)
            }
            Self::Complete {
                summary,
                candidate_refs,
                unresolved_questions,
            } => {
                text(summary, 16384)
                    && candidate_refs.len() <= 16
                    && candidate_refs.iter().all(|s| text(s, 256))
                    && unresolved_questions.len() <= 16
                    && unresolved_questions.iter().all(|s| text(s, 4096))
            }
        };
        if valid {
            Ok(())
        } else {
            Err("invalid work control bounds")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionProposal {
    pub summary: String,
    pub candidate_refs: Vec<String>,
    pub unresolved_questions: Vec<String>,
    pub instruction_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attention {
    pub campaign_id: String,
    pub work_id: String,
    pub generation: u64,
    pub instruction_revision: u64,
    pub request_id: String,
    pub question: String,
    pub deadline_ms: u64,
    pub timeout_ms: u64,
    pub answer: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reply {
    Status {
        objective: String,
        phase: String,
        instruction_revision: u64,
        remaining_tokens: u64,
        remaining_cost_micro_usd: u64,
        pending_questions: Vec<Attention>,
    },
    Answer {
        request_id: String,
        answer: Option<String>,
        resumed: bool,
    },
    Proposed {
        proposal: CompletionProposal,
    },
    Denied,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn controls_cannot_claim_authority_or_verification_and_are_bounded() {
        for input in [
            json!({"action":"status","campaign_id":"other"}),
            json!({"action":"status","work_id":"other"}),
            json!({"action":"complete","summary":"done","candidate_refs":[],"unresolved_questions":[],"verified":true}),
            json!({"action":"ask","request_id":"q","question":"?","timeout_ms":1,"generation":5}),
        ] {
            assert!(serde_json::from_value::<Request>(input).is_err());
        }
        for timeout_ms in [0, 300001, u64::MAX] {
            assert!(Request::Ask {
                request_id: "q".into(),
                question: "?".into(),
                timeout_ms
            }
            .validate()
            .is_err());
        }
        assert!(Request::Ask {
            request_id: "q".into(),
            question: "x".repeat(4097),
            timeout_ms: 1
        }
        .validate()
        .is_err());
        assert!(Request::Complete {
            summary: "done".into(),
            candidate_refs: vec![],
            unresolved_questions: vec![]
        }
        .validate()
        .is_ok());
    }
}
