//! Chat checkpoint persistence and session history compaction.

use std::path::PathBuf;

use crate::model::{ChatMessage, Content, Role};
use crate::role::AgentRole;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ChatCheckpoint {
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub evidence: Vec<serde_json::Value>,
    #[serde(default = "initial_commit")]
    pub next_commit: u64,
    #[serde(default)]
    pub context_epoch: u64,
}

fn initial_commit() -> u64 {
    1
}

pub fn chat_checkpoint_path(workspace: &std::path::Path, role: AgentRole) -> PathBuf {
    let name = match role {
        AgentRole::Worker => "worker.json",
        AgentRole::Background => "background.json",
    };
    workspace.join(".tachyon").join(name)
}

pub fn load_chat_checkpoint(path: &std::path::Path) -> Option<ChatCheckpoint> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
}

pub fn write_chat_checkpoint(path: &std::path::Path, checkpoint: &ChatCheckpoint) {
    let Ok(data) = serde_json::to_vec_pretty(checkpoint) else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, data).is_ok() {
        let _ = std::fs::rename(tmp, path);
    }
}

pub fn compact_completed_history(messages: &mut Vec<ChatMessage>) {
    messages.retain(|message| match message.role {
        Role::System | Role::User => true,
        Role::Assistant => message
            .content
            .iter()
            .all(|content| matches!(content, Content::Text(_))),
        Role::Tool => false,
    });
}

pub fn estimated_context_tokens(messages: &[ChatMessage]) -> u32 {
    messages
        .iter()
        .map(|message| {
            serde_json::to_vec(message)
                .map(|encoded| (encoded.len() / 4 + 1) as u32)
                .unwrap_or_default()
        })
        .fold(0, u32::saturating_add)
}

pub fn compact_context_messages(messages: &mut Vec<ChatMessage>, target_tokens: u32) {
    if estimated_context_tokens(messages) <= target_tokens {
        return;
    }
    let mut retained = Vec::new();
    let mut used = 0_u32;
    if let Some(system) = messages.iter().find(|message| message.role == Role::System) {
        used = used.saturating_add(estimated_context_tokens(std::slice::from_ref(system)));
        retained.push((0, system.clone()));
    }
    for (index, message) in messages.iter().enumerate().rev() {
        if message.role == Role::System {
            continue;
        }
        let cost = estimated_context_tokens(std::slice::from_ref(message));
        if used.saturating_add(cost) <= target_tokens || retained.len() < 3 {
            retained.push((index.saturating_add(1), message.clone()));
            used = used.saturating_add(cost);
        }
    }
    retained.sort_by_key(|(index, _)| *index);
    *messages = retained.into_iter().map(|(_, message)| message).collect();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ToolCall;

    #[test]
    fn chat_checkpoint_paths_are_role_isolated() {
        let root = std::path::Path::new("/tmp/workspace");
        assert_eq!(
            chat_checkpoint_path(root, AgentRole::Worker),
            root.join(".tachyon/worker.json")
        );
        assert_eq!(
            chat_checkpoint_path(root, AgentRole::Background),
            root.join(".tachyon/background.json")
        );
    }

    #[test]
    fn checkpoint_roundtrip_preserves_fields_and_replaces_existing_file() {
        let workspace = tempfile::tempdir().unwrap();
        let path = chat_checkpoint_path(workspace.path(), AgentRole::Worker);
        let mut checkpoint = ChatCheckpoint {
            messages: vec![
                ChatMessage::new(Role::System, "system"),
                ChatMessage::new(Role::User, "objective"),
            ],
            evidence: vec![serde_json::json!({"path": "finding.txt", "nested": [1, true]})],
            next_commit: 42,
            context_epoch: 7,
        };
        for epoch in [7, 8] {
            checkpoint.context_epoch = epoch;
            write_chat_checkpoint(&path, &checkpoint);
            let expected = serde_json::json!({
                "messages": checkpoint.messages,
                "evidence": checkpoint.evidence,
                "next_commit": 42,
                "context_epoch": epoch,
            });
            assert_eq!(
                std::fs::read(&path).unwrap(),
                serde_json::to_vec_pretty(&checkpoint).unwrap()
            );
            let loaded = load_chat_checkpoint(&path).unwrap();
            assert_eq!(serde_json::to_value(loaded).unwrap(), expected);
            assert!(!path.with_extension("json.tmp").exists());
        }
    }

    #[test]
    fn checkpoint_load_preserves_defaults_and_ignores_invalid_files() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join("checkpoint.json");
        assert!(load_chat_checkpoint(&path).is_none());
        std::fs::write(&path, r#"{"messages":[]}"#).unwrap();
        let checkpoint = load_chat_checkpoint(&path).unwrap();
        assert!(checkpoint.messages.is_empty());
        assert!(checkpoint.evidence.is_empty());
        assert_eq!(checkpoint.next_commit, 1);
        assert_eq!(checkpoint.context_epoch, 0);
        for invalid in ["not json", "{}", r#"{"messages":[],"next_commit":"bad"}"#] {
            std::fs::write(&path, invalid).unwrap();
            assert!(load_chat_checkpoint(&path).is_none());
        }
    }

    #[test]
    fn completed_history_drops_tool_protocol_but_keeps_transcript() {
        let mut messages = vec![
            ChatMessage::new(Role::System, "system"),
            ChatMessage::new(Role::User, "objective"),
            ChatMessage {
                role: Role::Assistant,
                content: vec![Content::ToolCall(ToolCall {
                    id: "call-1".into(),
                    name: "ipython".into(),
                    arguments: "{}".into(),
                })],
            },
            ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: "call-1".into(),
                    output: "raw output".into(),
                }],
            },
            ChatMessage::new(Role::Assistant, "final finding"),
        ];
        compact_completed_history(&mut messages);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].plain(), "objective");
        assert_eq!(messages[2].plain(), "final finding");
    }

    #[test]
    fn token_estimation_uses_serialized_bytes_per_message() {
        let messages = vec![
            ChatMessage::new(Role::System, "system"),
            ChatMessage::new(Role::User, "objective"),
        ];
        let expected: u32 = messages
            .iter()
            .map(|message| (serde_json::to_vec(message).unwrap().len() / 4 + 1) as u32)
            .sum();
        assert_eq!(estimated_context_tokens(&messages), expected);
        assert_eq!(estimated_context_tokens(&[]), 0);
    }

    #[test]
    fn compaction_is_unchanged_at_budget() {
        let mut messages = vec![
            ChatMessage::new(Role::User, "objective"),
            ChatMessage::new(Role::System, "system"),
            ChatMessage::new(Role::System, "second system"),
        ];
        let before = serde_json::to_value(&messages).unwrap();
        let budget = estimated_context_tokens(&messages);
        compact_context_messages(&mut messages, budget);
        assert_eq!(serde_json::to_value(&messages).unwrap(), before);
        let mut empty = Vec::new();
        compact_context_messages(&mut empty, 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn compaction_keeps_first_system_and_latest_messages_above_budget() {
        let mut messages = vec![
            ChatMessage::new(Role::User, "old"),
            ChatMessage::new(Role::System, "first system"),
            ChatMessage::new(Role::System, "second system"),
            ChatMessage::new(Role::User, "latest objective"),
            ChatMessage::new(Role::Assistant, "latest answer"),
        ];
        compact_context_messages(&mut messages, 0);
        assert_eq!(
            messages.iter().map(ChatMessage::plain).collect::<Vec<_>>(),
            ["first system", "latest objective", "latest answer"]
        );
        assert!(estimated_context_tokens(&messages) > 0);
    }

    #[test]
    fn compaction_without_system_keeps_three_latest_messages() {
        let mut messages: Vec<_> = ["old", "one", "two", "three"]
            .into_iter()
            .map(|text| ChatMessage::new(Role::User, text))
            .collect();
        compact_context_messages(&mut messages, 0);
        assert_eq!(
            messages.iter().map(ChatMessage::plain).collect::<Vec<_>>(),
            ["one", "two", "three"]
        );
    }

    #[test]
    fn compaction_skips_expensive_history_but_keeps_older_messages_that_fit() {
        let mut messages = vec![
            ChatMessage::new(Role::System, "system"),
            ChatMessage::new(Role::User, "small"),
            ChatMessage::new(Role::Assistant, "x".repeat(1000)),
            ChatMessage::new(Role::User, "latest objective"),
            ChatMessage::new(Role::Assistant, "latest answer"),
        ];
        let expected = vec![
            messages[0].clone(),
            messages[1].clone(),
            messages[3].clone(),
            messages[4].clone(),
        ];
        compact_context_messages(&mut messages, estimated_context_tokens(&expected));
        assert_eq!(
            serde_json::to_value(&messages).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
    }
}
