use std::sync::atomic::{AtomicU64, Ordering};

use tachyon_api::{
    InteractionCommand, InteractionCommandEnvelope, InteractionMetadata, FOREGROUND_ID,
};

static INTERACTION_COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn encode_interaction_command(
    command: InteractionCommand,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    turn_id: Option<String>,
    cwd: Option<String>,
) -> Result<String, String> {
    let sequence = INTERACTION_COMMAND_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let message_id = format!("interaction-command-{sequence}");
    let mut metadata = InteractionMetadata::new(
        &message_id,
        correlation_id.unwrap_or_else(|| message_id.clone()),
        FOREGROUND_ID,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0),
    );
    metadata.causation_id = causation_id;
    metadata.turn_id = turn_id;
    metadata.cwd = cwd;
    serde_json::to_string(&InteractionCommandEnvelope { metadata, command })
        .map_err(|error| format!("encode interaction command: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreground_commands_are_versioned_and_preserve_multiline_turns() {
        let before = crate::unix_now_ms();
        let wire = encode_interaction_command(
            InteractionCommand::AcceptUserTurn {
                text: "line one\nline two".into(),
            },
            Some("request-1".into()),
            Some("cause-1".into()),
            Some("4".into()),
            Some("/tmp/selected".into()),
        )
        .unwrap();
        assert!(!wire.contains('\n'));
        let decoded: InteractionCommandEnvelope = serde_json::from_str(&wire).unwrap();
        assert!(decoded
            .metadata
            .message_id
            .starts_with("interaction-command-"));
        assert!((before..=crate::unix_now_ms()).contains(&decoded.metadata.occurred_at_ms));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&wire).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "message_id": decoded.metadata.message_id,
                "correlation_id": "request-1",
                "causation_id": "cause-1",
                "conversation_id": FOREGROUND_ID,
                "turn_id": "4",
                "generation": 0,
                "occurred_at_ms": decoded.metadata.occurred_at_ms,
                "cwd": "/tmp/selected",
                "command": "accept_user_turn",
                "text": "line one\nline two"
            })
        );
    }

    #[test]
    fn absent_correlation_defaults_to_unique_message_id_and_cwd_is_omitted() {
        let encode = || {
            let wire = encode_interaction_command(
                InteractionCommand::BeginConversation { reset: false },
                None,
                None,
                None,
                None,
            )
            .unwrap();
            serde_json::from_str::<serde_json::Value>(&wire).unwrap()
        };
        let first = encode();
        let second = encode();
        assert_eq!(first["correlation_id"], first["message_id"]);
        assert_ne!(first["message_id"], second["message_id"]);
        assert!(first.get("cwd").is_none());
        assert!(first["causation_id"].is_null());
        assert!(first["turn_id"].is_null());
    }
}
