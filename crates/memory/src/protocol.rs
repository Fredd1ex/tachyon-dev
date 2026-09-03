use serde::{Deserialize, Serialize};

use crate::store::MemoryRecord;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum MemoryRequest {
    Get { id: String },
    Put { record: MemoryRecord },
    List,
    Revoke { id: String, revoked_at_ms: u64 },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MemoryResponse {
    Memory { record: MemoryRecord },
    Memories { records: Vec<MemoryRecord> },
    Ok,
    Error { message: String },
}
