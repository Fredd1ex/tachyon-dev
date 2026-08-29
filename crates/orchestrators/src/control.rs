use serde::{Deserialize, Serialize};

use crate::tasks::TaskId;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ControlRequest {
    Start { task_id: TaskId },
    Await { task_id: TaskId },
    Interrupt { task_id: TaskId },
    Resume { task_id: TaskId },
    Release { task_id: TaskId },
    Replan { task_id: TaskId },
}
