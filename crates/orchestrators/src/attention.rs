use serde::{Deserialize, Serialize};

use crate::tasks::TaskId;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum AttentionPriority {
    Critical,
    High,
    Normal,
    Low,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum DeliveryMode {
    Interrupt,
    Wait,
    Merge,
    Notify,
    Silent,
    Discard,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AttentionItem {
    pub task_id: TaskId,
    pub priority: AttentionPriority,
    pub delivery: DeliveryMode,
}
