//! Host-authored attention, separate from worker questions and progress.
use crate::{operational_events::OperationalWatermark, todo::TodoScope};
use serde::{Deserialize, Serialize};

/// Host-authored membership of one published notice, not model correlation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AttentionFrameMetadata {
    pub scope: TodoScope,
    #[serde(deserialize_with = "deserialize_frame_ids")]
    pub ids: Vec<String>,
}

fn deserialize_frame_ids<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    struct Ids;
    impl<'de> serde::de::Visitor<'de> for Ids {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("1..=32 unique attention IDs")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            let mut ids = Vec::new();
            while let Some(id) = seq.next_element::<String>()? {
                if ids.len() == 32 || id.is_empty() || id.len() > 256 || ids.contains(&id) {
                    return Err(serde::de::Error::custom("invalid attention frame IDs"));
                }
                ids.push(id);
            }
            if ids.is_empty() {
                return Err(serde::de::Error::custom("empty attention frame"));
            }
            Ok(ids)
        }
    }
    d.deserialize_seq(Ids)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttentionCategory {
    WorkFailed,
    WorkTimedOut,
    BudgetBlocked,
    Question,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttentionSeverity {
    Warning,
    Urgent,
}

impl AttentionCategory {
    /// Priority is host policy, never a model-supplied value.
    pub fn severity(self) -> AttentionSeverity {
        match self {
            Self::Question => AttentionSeverity::Warning,
            Self::WorkFailed | Self::WorkTimedOut | Self::BudgetBlocked => {
                AttentionSeverity::Urgent
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Attention {
    pub id: String,
    pub command_id: String,
    pub cause_id: String,
    pub scope: TodoScope,
    pub work_id: Option<String>,
    pub campaign_id: Option<String>,
    pub generation: u64,
    pub instruction_revision: u64,
    pub category: AttentionCategory,
    pub severity: AttentionSeverity,
    pub accepted_at_ms: u64,
    pub delivered_at_ms: Option<u64>,
    pub displayed_at_ms: Option<u64>,
    pub acknowledged_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttentionAcknowledgement {
    Displayed,
    Acknowledged,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AttentionSnapshot {
    pub records: Vec<Attention>,
    pub next_cursor: Option<String>,
    pub watermark: OperationalWatermark,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_membership_is_bounded_unique_and_nested() {
        let frame = AttentionFrameMetadata {
            scope: TodoScope::Campaign {
                campaign_id: "source".into(),
            },
            ids: (0..32).map(|n| format!("attention-{n}")).collect(),
        };
        let wire = serde_json::to_value(&frame).unwrap();
        assert_eq!(
            serde_json::from_value::<AttentionFrameMetadata>(wire.clone()).unwrap(),
            frame
        );
        for ids in [vec![], vec!["duplicate"; 2], vec!["id"; 33], vec![""]] {
            let mut invalid = wire.clone();
            invalid["ids"] = serde_json::json!(ids);
            assert!(serde_json::from_value::<AttentionFrameMetadata>(invalid).is_err());
        }
        let mut too_many = wire.clone();
        too_many["ids"] = serde_json::json!((0..33).map(|n| format!("id-{n}")).collect::<Vec<_>>());
        assert!(serde_json::from_value::<AttentionFrameMetadata>(too_many).is_err());
        let mut missing = wire;
        missing.as_object_mut().unwrap().remove("scope");
        assert!(serde_json::from_value::<AttentionFrameMetadata>(missing).is_err());
    }

    #[test]
    fn legacy_history_and_publication_keep_their_serialized_shape() {
        let history = serde_json::json!({
            "event_id": "old", "kind": "conversation", "conversation_id": "foreground",
            "turn_id": null, "occurred_at_ms": 1, "role": "notification", "text": "old notice",
            "task_id": null, "task_state": null
        });
        let entry: crate::HistoryEntry = serde_json::from_value(history.clone()).unwrap();
        assert!(entry.attention.is_none());
        assert_eq!(serde_json::to_value(entry).unwrap(), history);
        let publication = serde_json::json!({
            "protocol_version": 1, "message_id": "old:published", "correlation_id": "old",
            "causation_id": "old", "conversation_id": "foreground", "turn_id": null,
            "generation": 0, "occurred_at_ms": 1,
            "event": "user_visible_notification_published", "text": "old notice"
        });
        let mut event: crate::InteractionEventEnvelope =
            serde_json::from_value(publication.clone()).unwrap();
        assert!(event.metadata.attention.is_none());
        assert_eq!(serde_json::to_value(&event).unwrap(), publication);
        let frame = AttentionFrameMetadata {
            scope: TodoScope::Work {
                work_id: "host-work".into(),
            },
            ids: vec!["exact-record-id".into()],
        };
        event.metadata.attention = Some(frame.clone());
        let wire = serde_json::to_value(&event).unwrap();
        assert_eq!(wire["attention"], serde_json::to_value(frame).unwrap());
        assert_eq!(
            serde_json::from_value::<crate::InteractionEventEnvelope>(wire).unwrap(),
            event
        );
    }
}
