//! Legacy wire-line classification and event identity adapters.
use crate::app::model::items::ItemKind;
use crate::app::model::thread::Thread;
use crate::app::transcript::text::is_worker_runtime_detail;
use crate::app::update::accept_user_turn;
use std::collections::HashSet;
use tachyon_api::types::EventEnvelope;
use tachyon_api::InteractionEventEnvelope;

/// Route a streamed line into the right kind of item.
pub(in crate::app) fn classify_line(t: &mut Thread, text: &str) {
    t.touch();
    let (turn, text) = if let Some(rest) = text.strip_prefix("[turn:") {
        if let Some((turn, rest)) = rest.split_once("] ") {
            (Some(turn.to_string()), rest)
        } else {
            (None, text)
        }
    } else if text.starts_with("[user]") {
        (None, text)
    } else {
        (None, text)
    };
    if text.starts_with("[user]") {
        // Reconcile the optimistic local message with the daemon-assigned turn
        // instead of retaining a second uncorrelated conversation history.
        let body = text["[user]".len()..].trim_start();
        accept_user_turn(t, body, turn);
        return;
    } else if let Some(rest) = text.strip_prefix("[tool-result:") {
        if let Some((id, output)) = rest.split_once("] ") {
            t.add_tool_result(id.to_string(), output.to_string(), turn);
        }
    } else if let Some(rest) = text.strip_prefix("[tool:") {
        if let Some((id, call)) = rest.split_once("] ") {
            t.add_tool(call.to_string(), id.to_string(), turn);
        }
    } else if text.starts_with("[tool-result]") {
        t.add_turn(
            ItemKind::ToolResult,
            text["[tool-result]".len()..].trim_start().to_string(),
            turn,
        );
    } else if text.starts_with("[tool]") {
        t.add_turn(
            ItemKind::Tool,
            text["[tool]".len()..].trim_start().to_string(),
            turn,
        );
    } else if text.starts_with("[daemon:spawn]") {
        let spec = &text["[daemon:spawn]".len()..].trim();
        t.add_turn(ItemKind::Spawn, format!("⇣ spawn: {spec}"), turn);
    } else if text.starts_with("[daemon:result]") {
        let rest = &text["[daemon:result]".len()..].trim_start();
        t.add_turn(ItemKind::SpawnResult, format!("⇡ worker → {rest}"), turn);
    } else if text.starts_with("[worker:start]") {
        t.add_turn(
            ItemKind::Spawn,
            format!(
                "spawned worker {}",
                text["[worker:start]".len()..].trim_start()
            ),
            turn,
        );
    } else if text.starts_with("[worker:result]") {
        t.add_turn(
            ItemKind::SpawnResult,
            text["[worker:result]".len()..].trim_start().to_string(),
            turn,
        );
    } else if text.starts_with("[daemon]") {
        t.add(
            ItemKind::System,
            text["[daemon]".len()..].trim_start().to_string(),
        );
    } else if text.starts_with("[ghost") {
        t.add(ItemKind::System, text.to_string());
    } else if text.starts_with("[agent]") {
        t.finish_reply(text["[agent]".len()..].trim_start().to_string(), turn);
    } else if text.starts_with("[status]") {
        t.add_turn(
            ItemKind::System,
            text["[status]".len()..].trim_start().to_string(),
            turn,
        );
    } else if text.starts_with("[text]") {
        t.add_reply_fragment(text["[text]".len()..].to_string(), turn, false);
    } else if text.starts_with("[text-break]") {
        t.add_reply_fragment(String::new(), turn, true);
    } else if text.starts_with('⚠') {
        if is_worker_runtime_detail(text) {
            t.add(ItemKind::System, text.to_string());
        } else {
            t.add(ItemKind::Error, text.to_string());
        }
    } else if !text.trim().is_empty() && !t.is_foreground {
        // Plain content glues onto whatever the thread was doing.
        t.add(ItemKind::System, text.to_string());
    }
}

pub(in crate::app) fn accept_event(
    seen: &mut HashSet<(String, u64)>,
    envelope: &EventEnvelope,
) -> bool {
    envelope.event_id == 0 || seen.insert((envelope.session_id.clone(), envelope.event_id))
}

pub(in crate::app) fn decode_interaction_event(data: &str) -> Option<InteractionEventEnvelope> {
    serde_json::from_str(data).ok()
}

pub(in crate::app) fn is_structured_legacy_marker(data: &str) -> bool {
    let data = data.trim_start();
    data.starts_with("[turn:")
        && (data.contains(" [status]")
            || data.contains(" [text]")
            || data.contains(" [text-break]")
            || data.contains(" [tool:")
            || data.contains(" [tool-result:")
            || data.contains(" [agent]"))
}
