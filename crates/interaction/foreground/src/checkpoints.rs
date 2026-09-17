//! Conversation checkpoint wire format and ordered background writer.

use std::path::PathBuf;
use tachyon_model::ChatMessage;

use super::{
    runtime::AgentRole,
    turns::{ConversationState, EvidenceRecord},
};

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct ConversationCheckpoint {
    pub(super) messages: Vec<ChatMessage>,
    #[serde(default)]
    pub(super) evidence: Vec<EvidenceRecord>,
    pub(super) next_commit: u64,
    #[serde(default)]
    pub(super) context_epoch: u64,
}

pub(super) fn chat_checkpoint_path(workspace: &PathBuf, role: AgentRole) -> PathBuf {
    let _ = role;
    workspace.join(".tachyon").join("conversation.json")
}

pub(super) fn load_checkpoint(path: &std::path::Path) -> Option<ConversationCheckpoint> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
}

pub(super) fn checkpoint_snapshot(conversation: &ConversationState) -> ConversationCheckpoint {
    ConversationCheckpoint {
        messages: conversation.messages.clone(),
        evidence: conversation.evidence.clone(),
        next_commit: conversation.next_commit,
        context_epoch: conversation.context_epoch,
    }
}

pub(super) fn start_checkpoint_writer(
    path: PathBuf,
) -> std::sync::mpsc::Sender<ConversationCheckpoint> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        while let Ok(checkpoint) = rx.recv() {
            write_checkpoint(&path, &checkpoint);
        }
    });
    tx
}

fn write_checkpoint(path: &std::path::Path, checkpoint: &ConversationCheckpoint) {
    let Ok(data) = serde_json::to_vec_pretty(&checkpoint) else {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_checkpoint_path_is_stable() {
        let workspace = PathBuf::from("/tmp/tachyon-foreground-checkpoint");
        let conversation = chat_checkpoint_path(&workspace, AgentRole::Conversation);
        assert!(conversation.ends_with(".tachyon/conversation.json"));
    }

    use std::collections::BTreeMap;
    use tachyon_model::Role;

    #[test]
    fn checkpoint_wire_bytes_and_legacy_defaults_are_stable() {
        let checkpoint = ConversationCheckpoint {
            messages: vec![ChatMessage::new(Role::User, "first\nsecond")],
            evidence: Vec::new(),
            next_commit: 7,
            context_epoch: 2,
        };
        assert_eq!(
            serde_json::to_string(&checkpoint).unwrap(),
            r#"{"messages":[{"role":"user","content":[{"kind":"text","value":"first\nsecond"}]}],"evidence":[],"next_commit":7,"context_epoch":2}"#
        );
        let legacy: ConversationCheckpoint =
            serde_json::from_str(r#"{"messages":[],"next_commit":4}"#).unwrap();
        assert!(legacy.evidence.is_empty());
        assert_eq!(legacy.context_epoch, 0);
        assert_eq!(legacy.next_commit, 4);
    }

    #[test]
    fn conversation_checkpoint_round_trips_messages() {
        let path = std::env::temp_dir().join(format!(
            "tachyon-conversation-checkpoint-{}.json",
            std::process::id()
        ));
        let conversation = ConversationState {
            messages: vec![
                ChatMessage::new(Role::System, "system"),
                ChatMessage::new(Role::User, "remember this"),
            ],
            evidence: Vec::new(),
            pending: BTreeMap::new(),
            next_commit: 3,
            context_epoch: 0,
        };
        write_checkpoint(&path, &checkpoint_snapshot(&conversation));
        let restored = load_checkpoint(&path).expect("checkpoint should load");
        assert_eq!(restored.next_commit, 3);
        assert_eq!(restored.messages.len(), 2);
        assert_eq!(restored.messages[1].plain(), "remember this");
        let _ = std::fs::remove_file(path);
    }
}
