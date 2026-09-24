//! Headless manager client. Never starts a daemon or retries admission.
use crate::cli::ChatArgs;
use std::collections::HashSet;
use std::io::{self, Write};
use tachyon_api::interaction_manager::{Admission, Frame, Submit};
use tachyon_api::InteractionEvent;
use tachyon_client::{Client, InteractionSubscription};

pub fn run(args: ChatArgs) -> Result<(), String> {
    let mut client = Client::connect().map_err(|e| e.to_string())?;
    let mut output = io::stdout().lock();
    // Attach before admission. The initial snapshot and replay cover fast replies.
    let follow = args.follow || args.text.is_none();
    let mut stream = if follow {
        Some(InteractionSubscription::open(None).map_err(|e| e.to_string())?)
    } else {
        None
    };
    if let Some(text) = args.text {
        let snapshot = client.interaction_snapshot().map_err(|e| e.to_string())?;
        let command = Submit {
            conversation_id: snapshot.conversation_id,
            session_id: args
                .session_id
                .or(snapshot.session_id)
                .ok_or("foreground unavailable")?,
            command_id: args
                .command_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            text,
            cwd: args.cwd,
        };
        // Print the exact recovery payload before touching admission transport.
        eprintln!(
            "interaction command (retain for reconciliation): {}",
            serde_json::to_string(&command).unwrap()
        );
        let receipt = client.interaction_submit(command.clone())
            .map_err(|e| format!("{e}; admission unknown; not retried. Reconcile the identical command printed above"))?;
        if receipt.command != command {
            return Err("mismatched receipt; admission unknown; not retried".into());
        }
        if args.json {
            writeln!(output, "{}", serde_json::to_string(&receipt).unwrap())
                .map_err(|e| e.to_string())?;
        } else {
            writeln!(
                output,
                "admission {:?} (not execution): {}",
                receipt.admission, receipt.command.command_id
            )
            .map_err(|e| e.to_string())?;
            if let Some(accepted) = &receipt.accepted {
                writeln!(
                    output,
                    "host accepted turn {} (event {}); not completion",
                    accepted.turn_id, accepted.event_id
                )
                .map_err(|e| e.to_string())?;
            }
        }
        output.flush().map_err(|e| e.to_string())?;
        if receipt.admission == Admission::Uncertain && receipt.accepted.is_none() {
            return Err(
                "admission uncertain; operator reconciliation required; not retried".into(),
            );
        }
    }
    let mut seen = HashSet::new();
    while let Some(subscription) = &mut stream {
        let frame = match subscription.recv() {
            Ok(frame) => frame,
            Err(tachyon_client::ClientError::InteractionGap) => {
                stream = Some(InteractionSubscription::open(None).map_err(|e| e.to_string())?);
                continue;
            }
            Err(error) => return Err(format!(
                "{error}; stream disconnected; attach again for canonical state (do not resubmit)"
            )),
        };
        print_frame(&mut output, &frame, args.json, &mut seen).map_err(|e| e.to_string())?;
        if matches!(frame, Frame::ResnapshotRequired { .. }) {
            eprintln!("interaction gap; reattaching for canonical response state");
            stream = Some(InteractionSubscription::open(None).map_err(|e| e.to_string())?);
        }
    }
    Ok(())
}

fn print_frame(
    output: &mut impl Write,
    frame: &Frame,
    json: bool,
    seen: &mut HashSet<String>,
) -> io::Result<()> {
    if json {
        serde_json::to_writer(&mut *output, frame)?;
        writeln!(output)?;
    } else {
        // Text mode publishes canonical messages, never unrecoverable token fragments.
        match frame {
            Frame::Snapshot { snapshot } => {
                for entry in &snapshot.history {
                    if seen.insert(entry.event_id.clone()) {
                        writeln!(output, "{:?}: {}", entry.role, entry.text)?;
                    }
                }
            }
            Frame::Update { update } => {
                if let Some(event) = &update.event {
                    let publication = match &event.event {
                        InteractionEvent::UserTurnAccepted { text } => Some(("User", text)),
                        InteractionEvent::ConversationFinished { text } => {
                            Some(("Assistant", text))
                        }
                        InteractionEvent::UserVisibleNotificationPublished { text } => {
                            Some(("Notification", text))
                        }
                        _ => None,
                    };
                    if let Some((role, text)) = publication {
                        if seen.insert(event.metadata.message_id.clone()) {
                            writeln!(output, "{role}: {text}")?;
                        }
                    }
                }
            }
            Frame::ResnapshotRequired { .. } => {}
        }
        let responses: Vec<_> = match frame {
            Frame::Snapshot { snapshot } => snapshot.projection.responses.iter().collect(),
            Frame::Update { update } => update
                .changes
                .iter()
                .filter_map(|change| match change {
                    tachyon_api::interaction_manager::ProjectionChange::Response { response } => {
                        Some(response)
                    }
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        for response in responses {
            let key = response.final_event_id.clone().unwrap_or_else(|| {
                format!(
                    "projection:{}:{}:{}",
                    response.turn_id, response.generation, response.revision
                )
            });
            if seen.insert(key) {
                let text = if !response.answer.is_empty() {
                    response.answer.as_str()
                } else if let Some(failure) = &response.failure {
                    &failure.message
                } else {
                    response.pending.as_deref().unwrap_or("")
                };
                writeln!(
                    output,
                    "Assistant [{} {:?}]: {text}",
                    response.turn_id, response.phase
                )?;
            }
        }
    }
    output.flush()
}
