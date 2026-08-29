use serde::{Deserialize, Serialize};

use crate::store::TaskDocument;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum MemoryRequest {
    ReadTask { id: String },
    WriteTask { document: TaskDocument },
    ListTasks,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MemoryResponse {
    Task { document: TaskDocument },
    Tasks { documents: Vec<TaskDocument> },
    Ok,
    Error { message: String },
}
