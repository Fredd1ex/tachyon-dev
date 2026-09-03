//! Deterministic policy for scheduling and executing conversation turns.

use serde::{Deserialize, Serialize};

pub const CLASSIFICATION_PROMPT: &str = "Return only `AnswerNow` if the message can be handled independently of active work; otherwise return `WaitForActiveTurn`.";

pub const ANSWERABILITY_PROMPT: &str = "Decide whether the request can be answered accurately from conversation and accepted evidence using ordinary reasoning. Choose `AnswerFromContext` for supported advice, interpretation, comparison, explanation, summary, or transformation. Choose `NeedsNewWork` when an essential fact is missing, contradictory, stale, or the request asks for newer, future, or different-scope information. Judge only what was asked: do not demand an unrequested forecast or additional detail. Call the required tool exactly once; emit no prose.";

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum InteractionDecision {
    AnswerNow,
    AttachToActiveTurn,
    InterruptAndReplan,
    WaitForActiveTurn,
}

impl InteractionDecision {
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim() {
            "AnswerNow" => Some(Self::AnswerNow),
            "AttachToActiveTurn" => Some(Self::AttachToActiveTurn),
            "InterruptAndReplan" => Some(Self::InterruptAndReplan),
            "WaitForActiveTurn" => Some(Self::WaitForActiveTurn),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum Answerability {
    AnswerFromContext,
    NeedsNewWork,
}

impl Answerability {
    pub fn parse(text: &str) -> Self {
        match text.trim() {
            "AnswerFromContext" => Self::AnswerFromContext,
            _ => Self::NeedsNewWork,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct ExecutionPolicy {
    pub answer_from_context: bool,
    pub force_delegation: bool,
}

pub fn publication_requires_dependency(queued: bool, decision: InteractionDecision) -> bool {
    queued && decision != InteractionDecision::AnswerNow
}

pub fn execution_policy(answerability: Option<Answerability>) -> ExecutionPolicy {
    match answerability {
        Some(Answerability::AnswerFromContext) => ExecutionPolicy {
            answer_from_context: true,
            force_delegation: false,
        },
        Some(Answerability::NeedsNewWork) => ExecutionPolicy {
            answer_from_context: false,
            force_delegation: true,
        },
        None => ExecutionPolicy::default(),
    }
}

pub fn follow_up_execution_policy(
    requires_dependency: bool,
    has_accepted_evidence: bool,
    answerability: Option<Answerability>,
) -> ExecutionPolicy {
    if has_accepted_evidence {
        return execution_policy(Some(answerability.unwrap_or(Answerability::NeedsNewWork)));
    }
    if requires_dependency {
        return execution_policy(Some(Answerability::NeedsNewWork));
    }
    ExecutionPolicy::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_independent_queued_turns_publish_immediately() {
        assert!(!publication_requires_dependency(
            true,
            InteractionDecision::AnswerNow
        ));
        for decision in [
            InteractionDecision::WaitForActiveTurn,
            InteractionDecision::AttachToActiveTurn,
            InteractionDecision::InterruptAndReplan,
        ] {
            assert!(publication_requires_dependency(true, decision));
            assert!(!publication_requires_dependency(false, decision));
        }
    }

    #[test]
    fn answerability_alone_controls_execution() {
        assert_eq!(
            execution_policy(Some(Answerability::AnswerFromContext)),
            ExecutionPolicy {
                answer_from_context: true,
                force_delegation: false,
            }
        );
        assert_eq!(
            execution_policy(Some(Answerability::NeedsNewWork)),
            ExecutionPolicy {
                answer_from_context: false,
                force_delegation: true,
            }
        );
        assert_eq!(execution_policy(None), ExecutionPolicy::default());
    }

    #[test]
    fn follow_up_policy_only_disables_tools_for_accepted_answerable_evidence() {
        assert_eq!(
            follow_up_execution_policy(false, true, Some(Answerability::AnswerFromContext)),
            ExecutionPolicy {
                answer_from_context: true,
                force_delegation: false,
            }
        );
        for policy in [
            follow_up_execution_policy(true, false, None),
            follow_up_execution_policy(false, true, None),
            follow_up_execution_policy(false, true, Some(Answerability::NeedsNewWork)),
        ] {
            assert!(policy.force_delegation);
            assert!(!policy.answer_from_context);
        }
        assert_eq!(
            follow_up_execution_policy(false, false, None),
            ExecutionPolicy::default()
        );
    }

    #[test]
    fn classifier_output_fails_closed() {
        assert_eq!(
            InteractionDecision::parse("AnswerNow"),
            Some(InteractionDecision::AnswerNow)
        );
        assert_eq!(InteractionDecision::parse("explanation"), None);
        assert_eq!(
            Answerability::parse("explanation"),
            Answerability::NeedsNewWork
        );
    }

    #[test]
    fn answerability_prompt_covers_supported_synthesis_and_new_evidence_boundaries() {
        for request in [
            "advice",
            "interpretation",
            "comparison",
            "explanation",
            "summary",
            "transformation",
        ] {
            assert!(ANSWERABILITY_PROMPT.contains(request));
        }
        for boundary in [
            "missing",
            "contradictory",
            "stale",
            "newer",
            "future",
            "different-scope",
        ] {
            assert!(ANSWERABILITY_PROMPT.contains(boundary));
        }
        assert!(ANSWERABILITY_PROMPT.contains("accepted evidence"));
        assert!(ANSWERABILITY_PROMPT.contains("unrequested forecast"));
        assert!(ANSWERABILITY_PROMPT.contains("additional detail"));
        assert!(ANSWERABILITY_PROMPT.contains("required tool exactly once"));
        assert!(ANSWERABILITY_PROMPT.len() <= 600);
    }
}
