use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type TaskId = Uuid;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum TaskState {
    Ready,
    Running,
    Waiting,
    Paused,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Task {
    pub id: TaskId,
    pub objective: String,
    pub state: TaskState,
    pub depends_on: Vec<TaskId>,
}

impl Task {
    pub fn new(objective: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            objective: objective.into(),
            state: TaskState::Ready,
            depends_on: Vec::new(),
        }
    }
}
