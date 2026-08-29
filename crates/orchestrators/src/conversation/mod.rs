use serde::{Deserialize, Serialize};

use crate::tasks::TaskId;

pub mod policy;
pub mod prompt;

use crate::capabilities::Capability;

pub const CAPABILITIES: &[Capability] = &[
    Capability::Respond,
    Capability::DelegateOne,
    Capability::DelegateMany,
];

pub const DELEGATION_CAPABILITIES: &[Capability] =
    &[Capability::DelegateOne, Capability::DelegateMany];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_work_removes_direct_response() {
        assert_eq!(CAPABILITIES[0], Capability::Respond);
        assert_eq!(
            DELEGATION_CAPABILITIES,
            &[Capability::DelegateOne, Capability::DelegateMany]
        );
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
