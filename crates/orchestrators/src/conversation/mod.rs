use serde::{Deserialize, Serialize};

use crate::tasks::TaskId;

pub mod policy;
pub mod prompt;

use crate::capabilities::Capability;

pub const CAPABILITIES: &[Capability] = &[Capability::DelegateOne, Capability::DelegateMany];

pub const DELEGATION_CAPABILITIES: &[Capability] =
    &[Capability::DelegateOne, Capability::DelegateMany];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_exposes_only_delegation_tools() {
        assert_eq!(CAPABILITIES, DELEGATION_CAPABILITIES);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConversationTurn {
    pub id: String,
    pub user_text: String,
    pub task_id: Option<TaskId>,
}

impl ConversationTurn {
    pub fn new(id: impl Into<String>, user_text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            user_text: user_text.into(),
            task_id: None,
        }
    }
}
