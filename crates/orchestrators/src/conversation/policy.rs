//! Deterministic policy for scheduling and executing conversation turns.

use serde::{Deserialize, Serialize};

pub const CLASSIFICATION_PROMPT: &str = "Return exactly one enum value and nothing else: AnswerNow, AttachToActiveTurn, InterruptAndReplan, or WaitForActiveTurn.\n\nClassify the incoming message relative to the active turn and its context. AnswerNow means it can be answered independently without changing the active turn. AttachToActiveTurn means it supplies information or a refinement for the active turn. InterruptAndReplan means it changes the objective enough that the active turn should be stopped and reconsidered. WaitForActiveTurn means it should be handled after the active turn. Do not explain your choice.";

pub const ANSWERABILITY_PROMPT: &str = "Return exactly one value and nothing else: AnswerFromContext or NeedsNewWork.\n\nClassify whether the existing conversation and task evidence are sufficient to answer the incoming message accurately. Choose AnswerFromContext when the answer can be grounded in the available evidence, even if it requires a simple inference. Choose NeedsNewWork only when relevant evidence is missing, stale, or contradictory. Do not call tools and do not explain your choice.";

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
}
