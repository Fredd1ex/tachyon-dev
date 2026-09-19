//! Transcript records and persisted work correlation, independent of rendering.
use crate::app::attention;

#[derive(Clone, Debug, Hash, PartialEq)]
pub(crate) enum ItemKind {
    User,
    PendingReply,
    Reply,
    Tool,
    ToolResult,
    System,
    Spawn,
    SpawnResult,
    Error,
}

pub(crate) struct Item {
    pub(crate) attention: Option<attention::Notice>,
    pub(crate) work: Option<WorkDetail>,
    pub(crate) kind: ItemKind,
    pub(crate) text: String,
    /// Collapsed diagnostic body; never changes the copy payload.
    pub(crate) hidden: bool,
    /// Tool output stays on its call so the card is atomic.
    pub(crate) output: Option<String>,
    pub(crate) tool_id: Option<String>,
    pub(crate) turn: Option<String>,
    pub(crate) timestamp: u64,
    pub(crate) revision: u64,
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct AssignmentKey {
    pub(crate) work_id: String,
    pub(crate) generation: u64,
    pub(crate) assignment: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct WorkDetail {
    #[serde(skip)]
    pub(crate) raw_open: bool,
    pub(crate) key: AssignmentKey,
    pub(crate) slot: Option<usize>,
    pub(crate) tool: Option<tachyon_api::types::WorkToolEvidence>,
    pub(crate) timing: Option<tachyon_api::types::WorkTiming>,
    pub(crate) omitted: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_work_identity_survives_without_persisting_expansion() {
        let detail = WorkDetail {
            raw_open: true,
            key: AssignmentKey {
                work_id: "work".into(),
                generation: 7,
                assignment: 3,
            },
            slot: Some(2),
            tool: None,
            timing: None,
            omitted: 4,
        };
        let json = serde_json::to_string(&detail).unwrap();
        assert!(!json.contains("raw_open"));
        let restored: WorkDetail = serde_json::from_str(&json).unwrap();
        assert!(restored.key == detail.key);
        assert!(!restored.raw_open);
        assert_eq!(restored.slot, Some(2));
        assert_eq!(restored.omitted, 4);
    }
}
