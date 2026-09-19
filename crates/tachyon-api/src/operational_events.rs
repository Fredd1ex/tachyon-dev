//! Durable operational feed identities. Subscription transport is separate.
use serde::{Deserialize, Serialize};

/// A bounded durable replay batch. Resume after `watermark`, including empty batches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationalBatch {
    pub events: Vec<OperationalEvent>,
    pub watermark: OperationalWatermark,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationalWatermark {
    pub instance_id: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationalEvent {
    pub schema_version: u32,
    pub watermark: OperationalWatermark,
    pub scope: crate::todo::TodoScope,
    pub scope_revision: u64,
    pub occurred_at_ms: u64,
    pub change: OperationalChange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationalChange {
    AttentionChanged {
        attention: crate::attention::Attention,
    },
    TodoAdded {
        todo: crate::todo::Todo,
    },
    TodoUpdated {
        todo: crate::todo::Todo,
    },
}
