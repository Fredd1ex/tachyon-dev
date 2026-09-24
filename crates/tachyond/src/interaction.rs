//! Daemon adapter: durable admission precedes transport; attach never drives work.
use super::*;
use tachyon_api::interaction_manager::{Frame, Receipt, Revision, Snapshot, Submit};

pub(crate) fn initialize(registry: &Arc<Mutex<Registry>>) {
    let (manager, history, runtime, initialized) = {
        let reg = registry.lock().unwrap();
        let (Some(history), Some(runtime)) = (reg.history_store.clone(), reg.runtime_store.clone())
        else {
            return;
        };
        (
            reg.interaction.clone(),
            history,
            runtime,
            reg.interaction_bound.clone(),
        )
    };
    initialized.get_or_init(|| {
        let weak_store = Arc::downgrade(&runtime);
        let weak_history = Arc::downgrade(&history);
        let weak_manager = Arc::downgrade(&manager);
        let (sender, receiver) = mpsc::sync_channel(128);
        let overflow = Arc::new(AtomicBool::new(false));
        let missed = overflow.clone();
        runtime.subscribe_operational(Arc::new(move |event| {
            if sender.try_send(event.scope.clone()).is_err() {
                missed.store(true, Ordering::Release);
            }
        }));
        std::thread::spawn(move || loop {
            let scope = receiver.recv_timeout(std::time::Duration::from_millis(100));
            let (Some(store), Some(history), Some(manager)) = (
                weak_store.upgrade(),
                weak_history.upgrade(),
                weak_manager.upgrade(),
            ) else {
                break;
            };
            let scopes = if overflow.swap(false, Ordering::AcqRel) {
                // Overflow coalesces wakeups, not accepted state. Refresh only
                // known host-linked scopes, never the entire operational history.
                manager
                    .lock()
                    .unwrap()
                    .projection()
                    .progress
                    .into_iter()
                    .map(|p| p.scope)
                    .collect()
            } else if let Ok(scope) = scope {
                vec![scope]
            } else {
                continue;
            };
            for scope in scopes {
                let relevant = match &scope {
                    tachyon_api::todo::TodoScope::Conversation { id } => id == FOREGROUND_ID,
                    tachyon_api::todo::TodoScope::Work { work_id } => manager
                        .lock()
                        .unwrap()
                        .projection()
                        .works
                        .iter()
                        .any(|work| &work.work_id == work_id),
                    tachyon_api::todo::TodoScope::Campaign { .. } => false,
                };
                if relevant {
                    match store.interaction_progress(&scope) {
                        Ok(progress) => update_projection(&manager, Some(&history), |manager| {
                            manager.progress(progress)
                        }),
                        Err(error) => {
                            eprintln!("tachyond: interaction todo progress: {error}");
                            manager.lock().unwrap().invalidate();
                        }
                    }
                }
            }
        });
        update_projection(&manager, Some(&history), |_| {});
        let mut scopes: Vec<_> = manager
            .lock()
            .unwrap()
            .projection()
            .works
            .into_iter()
            .map(|work| work.todo_scope)
            .collect();
        scopes.push(tachyon_api::todo::TodoScope::Conversation {
            id: FOREGROUND_ID.into(),
        });
        for scope in scopes {
            if let Ok(progress) = runtime.interaction_progress(&scope) {
                update_projection(&manager, Some(&history), |manager| {
                    manager.progress(progress)
                });
            }
        }
    });
}

/// No registry guard is held during reduction, persistence, or publication.
fn update_projection(
    manager: &Arc<Mutex<tachyon_interaction_manager::Manager>>,
    history: Option<&Arc<HistoryStore>>,
    reduce: impl FnOnce(&mut tachyon_interaction_manager::Manager),
) {
    let mut manager = manager.lock().unwrap();
    let Some(history) = history else {
        manager.invalidate();
        return;
    };
    let result = (|| {
        if !manager.restored {
            manager.restore(history.interaction_checkpoint()?);
        }
        let mut next = manager.clone();
        reduce(&mut next);
        if next.revision() != manager.revision() {
            history.save_interaction_checkpoint(next.checkpoint())?;
            *manager = next;
        }
        Ok::<_, String>(())
    })();
    if let Err(error) = result {
        eprintln!("tachyond: interaction projection: {error}");
        manager.invalidate();
    }
}

pub(crate) fn host_stopped(registry: &Arc<Mutex<Registry>>) {
    let (manager, history) = {
        let reg = registry.lock().unwrap();
        (reg.interaction.clone(), reg.history_store.clone())
    };
    update_projection(&manager, history.as_ref(), |manager| {
        manager.reconcile(None, &[], &Default::default())
    });
}

pub(crate) fn work(registry: &Arc<Mutex<Registry>>, worker: &str) {
    work_record(registry, worker, None);
}

pub(crate) fn work_record(registry: &Arc<Mutex<Registry>>, worker: &str, work_id: Option<&str>) {
    initialize(registry);
    use tachyon_api::interaction_manager::{Metrics, Work, WorkPhase};
    let (manager, history, runtime, work) = {
        let reg = registry.lock().unwrap();
        let task = reg.tasks.get(worker);
        let Some(record) = work_id
            .or_else(|| task.and_then(|task| task.info.logical_task_id.as_deref()))
            .and_then(|id| reg.works.get(id))
        else {
            return;
        };
        if record.worker_id != worker {
            return;
        }
        let info = task
            .filter(|task| {
                task.info.logical_task_id.as_deref() == Some(record.request.work_id.as_str())
                    && task.assignment == record.request.assignment
            })
            .map(|task| &task.info)
            .unwrap_or(&record.info);
        let result = record
            .terminal_result
            .as_deref()
            .and_then(decode_structured_event)
            .and_then(|event| {
                if let StructuredAgentEvent::WorkResult { result } = event {
                    Some(result)
                } else {
                    None
                }
            });
        let phase = match result.as_ref().map(|r| &r.outcome) {
            Some(WorkOutcome::Completed { .. }) => WorkPhase::Completed,
            Some(WorkOutcome::Blocked { .. }) => WorkPhase::Blocked,
            Some(WorkOutcome::Failed { .. }) => WorkPhase::Failed,
            Some(WorkOutcome::Cancelled { .. }) => WorkPhase::Cancelled,
            Some(WorkOutcome::TimedOut { .. }) => WorkPhase::TimedOut,
            None if record.review.is_some() => WorkPhase::Reviewing,
            None => match info.state {
                AgentState::Running => WorkPhase::Running,
                AgentState::Starting | AgentState::Waiting => WorkPhase::Waiting,
                _ => WorkPhase::Unknown,
            },
        };
        let work = Work {
            work_id: record.request.work_id.clone(),
            worker_id: worker.into(),
            origin_turn_id: record
                .info
                .origin_turn_id
                .as_deref()
                .map(|id| tachyon_api::interaction_manager::canonical_turn_id("", id))
                .filter(|id| {
                    id.contains(':') && !id.starts_with(':') && !id.starts_with("foreground:")
                }),
            generation: record.request.generation,
            assignment: record.request.assignment,
            attempt_id: record.request.attempt.as_ref().map(|a| a.id.clone()),
            revision: 0,
            title: record
                .request
                .objective
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .chars()
                .take(120)
                .collect(),
            phase,
            metrics: Metrics {
                timing: result.as_ref().and_then(|r| r.timing.clone()),
                observed_invocations: result
                    .as_ref()
                    .and_then(|r| r.evidence.observed_invocations),
                ..Default::default()
            },
            latest_tool: None,
            result_available: result.is_some(),
            candidate_refs: result
                .as_ref()
                .and_then(|r| r.candidate_refs.clone())
                .unwrap_or_default(),
            todo_scope: tachyon_api::todo::TodoScope::Work {
                work_id: record.request.work_id.clone(),
            },
        };
        (
            reg.interaction.clone(),
            reg.history_store.clone(),
            reg.runtime_store.clone(),
            work,
        )
    };
    let known = manager
        .lock()
        .unwrap()
        .projection()
        .progress
        .iter()
        .any(|p| p.scope == work.todo_scope);
    let scope = work.todo_scope.clone();
    update_projection(&manager, history.as_ref(), |manager| {
        manager.work(work);
    });
    if !known {
        if let Some(progress) = runtime.and_then(|store| store.interaction_progress(&scope).ok()) {
            update_projection(&manager, history.as_ref(), |manager| {
                manager.progress(progress)
            });
        }
    }
}

pub(crate) fn telemetry(registry: &Arc<Mutex<Registry>>, source: &str, data: &str) {
    let Ok(event) = serde_json::from_str::<EventEnvelope>(data) else {
        return;
    };
    let (manager, history, work_id) = {
        let reg = registry.lock().unwrap();
        let Some(task) = reg.tasks.get(source) else {
            return;
        };
        if source == FOREGROUND_ID {
            if event.session_id != task.info.session_id
                || event.conversation_id.as_deref() != Some(FOREGROUND_ID)
                || !matches!(event.actor, tachyon_api::Actor::Foreground)
                || !event
                    .turn_id
                    .as_ref()
                    .is_some_and(|turn| turn.starts_with(&format!("{}:", task.info.session_id)))
            {
                return;
            }
            (reg.interaction.clone(), reg.history_store.clone(), None)
        } else {
            let identity = match &event.kind {
                StructuredAgentEvent::ToolStarted {
                    identity: Some(identity),
                    ..
                }
                | StructuredAgentEvent::ToolFinished {
                    identity: Some(identity),
                    ..
                }
                | StructuredAgentEvent::ToolTelemetry { identity, .. } => identity,
                // Worker usage lacks an assignment fence. Never attribute it to warm reuse.
                _ => return,
            };
            let Some(record) = identity.work_id.as_ref().and_then(|id| reg.works.get(id)) else {
                return;
            };
            if event.session_id != task.info.session_id
                || record.worker_id != source
                || !matches!(&event.actor, tachyon_api::Actor::Worker { id } if id == source)
                || task.info.state != AgentState::Running
                || task.info.logical_task_id != identity.work_id
                || Some(task.generation) != identity.generation
                || Some(task.assignment) != identity.assignment
                || identity
                    .task_id
                    .as_ref()
                    .is_some_and(|id| Some(id) != identity.work_id.as_ref())
                || record.terminal_result.is_some()
                || record.review.is_some()
                || identity.generation != Some(record.request.generation)
                || identity.assignment != Some(record.request.assignment)
                || identity.attempt_id.as_ref() != record.request.attempt.as_ref().map(|a| &a.id)
                || event.turn_id != record.info.origin_turn_id
            {
                return;
            }
            (
                reg.interaction.clone(),
                reg.history_store.clone(),
                Some(record.request.work_id.clone()),
            )
        }
    };
    update_projection(&manager, history.as_ref(), |manager| {
        manager.telemetry(&event, work_id.as_deref())
    });
}

pub(crate) fn snapshot(registry: &Arc<Mutex<Registry>>) -> Result<Snapshot, String> {
    initialize(registry);
    let (manager, history, host) = {
        let reg = registry.lock().unwrap();
        (
            reg.interaction.clone(),
            reg.history_store.clone(),
            reg.tasks.get(FOREGROUND_ID).map(|t| t.info.clone()),
        )
    };
    project_pending_history(registry)?;
    let mut manager = manager.lock().unwrap();
    let history = history.ok_or("history unavailable")?;
    if !manager.restored {
        manager.restore(history.interaction_checkpoint()?);
    }
    let messages = history.recent_conversation_messages(FOREGROUND_ID, 200)?;
    let mut entries: Vec<_> = messages
        .into_iter()
        .map(|m| tachyon_api::HistoryEntry {
            attention: m.attention,
            event_id: m.event_id,
            kind: m.kind,
            conversation_id: m.conversation_id,
            turn_id: m.turn_id,
            occurred_at_ms: m.occurred_at_ms,
            role: m.role,
            text: m.text,
            task_id: m.task_id,
            task_state: m.task_state,
        })
        .collect();
    let mut next = manager.clone();
    next.reconcile(
        host.as_ref()
            .filter(|h| !h.state.is_terminal())
            .map(|h| h.session_id.as_str()),
        &entries,
        &history.interaction_final_generations(&entries)?,
    );
    if next.revision() != manager.revision() {
        history.save_interaction_checkpoint(next.checkpoint())?;
        *manager = next;
    }
    let page = manager.page(&manager.revision(), 0).expect("current page");
    let mut history_content = Vec::new();
    for entry in &mut entries {
        if entry.text.len() > 16384 {
            history_content.push(tachyon_api::interaction_manager::HistoryContent {
                event_id: entry.event_id.clone(),
                reference: format!("history:{}", entry.event_id),
                total_bytes: entry.text.len() as u64,
            });
            let mut end = 16384;
            while !entry.text.is_char_boundary(end) {
                end -= 1;
            }
            entry.text.truncate(end);
        }
    }
    Ok(Snapshot {
        revision: manager.revision(),
        conversation_id: FOREGROUND_ID.into(),
        session_id: host.as_ref().map(|h| h.session_id.clone()),
        host_state: host.map(|h| h.state),
        projection: page.projection,
        projection_next: page.next_offset,
        history: entries,
        history_content,
    })
}

pub(crate) fn publish(registry: &Arc<Mutex<Registry>>, data: &str) {
    let Ok(mut event) = serde_json::from_str::<tachyon_api::InteractionEventEnvelope>(data) else {
        return;
    };
    if event.metadata.protocol_version != tachyon_api::INTERACTION_PROTOCOL_VERSION
        || event.metadata.conversation_id != FOREGROUND_ID
    {
        return;
    }
    initialize(registry);
    let (manager, session, history, runtime) = {
        let reg = registry.lock().unwrap();
        let Some(task) = reg.tasks.get(FOREGROUND_ID) else {
            return;
        };
        (
            reg.interaction.clone(),
            task.info.session_id.clone(),
            reg.history_store.clone(),
            reg.runtime_store.clone(),
        )
    };
    if event
        .metadata
        .command_origin
        .as_ref()
        .is_some_and(|origin| origin.session_id != session)
    {
        return;
    }
    // Foreground turn counters reset. Namespace durable event and turn identities.
    // Host-admitted attention/assessment publications already have durable IDs
    // and may have been enqueued transactionally with their delivery receipt.
    if event.metadata.attention.is_none()
        && !event
            .metadata
            .message_id
            .starts_with("campaign-assessment-")
        && !event
            .metadata
            .message_id
            .starts_with(&format!("{session}:"))
    {
        event.metadata.message_id = format!("{session}:{}", event.metadata.message_id);
    }
    event.metadata.turn_id = event
        .metadata
        .turn_id
        .map(|turn| tachyon_api::interaction_manager::canonical_turn_id(&session, &turn));
    if event
        .metadata
        .turn_id
        .as_ref()
        .is_some_and(|turn| !turn.starts_with(&format!("{session}:")))
        && !matches!(
            event.event,
            tachyon_api::InteractionEvent::UserVisibleNotificationPublished { .. }
        )
    {
        return;
    }
    let mut manager = manager.lock().unwrap();
    let Some(history) = history else {
        manager.invalidate();
        return;
    };
    if !manager.restored {
        match history.interaction_checkpoint() {
            Ok(checkpoint) => manager.restore(checkpoint),
            Err(error) => {
                eprintln!("tachyond: interaction recovery failed: {error}");
                manager.invalidate();
                return;
            }
        }
    }
    if event.metadata.command_origin.is_some() {
        let result = history.bind_accepted_turn(&event);
        if let Err(error) = result {
            eprintln!("tachyond: interaction acceptance rejected: {error}");
            manager.invalidate();
            return;
        }
    }
    let wire = serde_json::to_string(&event).expect("serializable interaction event");
    let mut next = manager.clone();
    next.publish(session, event.clone());
    if next.revision() == manager.revision() {
        return;
    }
    let persisted = (|| {
        history.remember_interaction_final(&event)?;
        if let Some(projection) = crate::messaging::notifications::history_projection(&wire) {
            let runtime = runtime.as_ref().ok_or("runtime store missing")?;
            runtime.enqueue_history(&projection)?;
            history.apply(&projection)?;
            runtime.acknowledge_history(&projection.event_id, unix_now_ms())?;
        }
        Ok::<_, String>(())
    })();
    if let Err(error) = persisted {
        eprintln!("tachyond: interaction projection failed: {error}");
        manager.invalidate();
        return;
    }
    if let Err(error) = history.save_interaction_event(next.checkpoint(), Some(&event)) {
        eprintln!("tachyond: interaction checkpoint failed: {error}");
        manager.invalidate();
        return;
    }
    *manager = next;
}

pub(crate) fn submit(registry: &Arc<Mutex<Registry>>, command: &Submit) -> Result<Receipt, String> {
    if command.conversation_id != FOREGROUND_ID
        || command.command_id.is_empty()
        || command.command_id.len() > 128
        || command.text.trim().is_empty()
        || command.text.len() > 65536
    {
        return Err("invalid interaction command".into());
    }
    let history = registry
        .lock()
        .unwrap()
        .history_store
        .clone()
        .ok_or("history unavailable")?;
    if let Some(receipt) = history.existing_command(command)? {
        return Ok(receipt);
    }
    // A duplicate, including an uncertain admission after restart, is never resent.
    // Validate session before creating a receipt, but allow retrieval from old sessions.
    let (input, web_available) = {
        let reg = registry.lock().unwrap();
        let task = reg
            .tasks
            .get(FOREGROUND_ID)
            .ok_or("foreground unavailable")?;
        if task.info.session_id != command.session_id {
            return Err("stale foreground session".into());
        }
        if task.info.state.is_terminal() || reg.service_shutdown.load(Ordering::Acquire) {
            return Err("foreground unavailable".into());
        }
        let input = if let Some(socket) = &task.control_socket {
            TaskInput::Supervisor(socket.clone())
        } else {
            TaskInput::Pipe(task.stdin.clone().ok_or("foreground not writable")?)
        };
        (input, reg.web.is_some())
    };
    let cwd = command
        .cwd
        .as_deref()
        .map(tachyon_util::daemon::selected_workspace)
        .transpose()?
        .map(|p| p.to_string_lossy().into_owned());
    let wire = encode_interaction_command(
        InteractionCommand::AcceptUserTurn {
            text: command.text.clone(),
        },
        Some(command.command_id.clone()),
        None,
        None,
        cwd,
        web_available,
    )?;
    let mut envelope: tachyon_api::InteractionCommandEnvelope =
        serde_json::from_str(&wire).map_err(|e| e.to_string())?;
    let origin = tachyon_api::interaction_manager::CommandOrigin {
        session_id: command.session_id.clone(),
        command_id: command.command_id.clone(),
        host_message_id: envelope.metadata.message_id.clone(),
    };
    envelope.metadata.command_origin = Some(origin.clone());
    let wire = serde_json::to_string(&envelope).map_err(|e| e.to_string())?;
    let (receipt, fresh) = history.command_receipt(command, false, Some(&origin))?;
    if !fresh {
        return Ok(receipt);
    }
    if write_task_input(input, FOREGROUND_ID, &wire).is_err() {
        return Ok(receipt);
    }
    history
        .command_receipt(command, true, Some(&origin))
        .map(|(r, _)| r)
}

pub(crate) fn serve(
    writer: &mut UnixStream,
    registry: &Arc<Mutex<Registry>>,
    after: Option<Revision>,
) -> std::io::Result<()> {
    writer.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
    let (manager, shutdown) = {
        let reg = registry.lock().unwrap();
        (reg.interaction.clone(), reg.service_shutdown.clone())
    };
    let mut cursor = match after {
        Some(after) => after,
        None => {
            let snapshot = match snapshot(registry) {
                Ok(s) => s,
                Err(e) => return write_response(writer, &ApiResponse::error(e)),
            };
            let cursor = snapshot.revision.clone();
            write_response(
                writer,
                &ApiResponse::InteractionFrame {
                    frame: Frame::Snapshot { snapshot },
                },
            )?;
            cursor
        }
    };
    while !shutdown.load(Ordering::Acquire) {
        let batch = manager.lock().unwrap().replay(&cursor);
        match batch {
            Ok(updates) => {
                for update in updates {
                    cursor = update.revision.clone();
                    write_response(
                        writer,
                        &ApiResponse::InteractionFrame {
                            frame: Frame::Update { update },
                        },
                    )?;
                }
            }
            Err(current) => {
                write_response(
                    writer,
                    &ApiResponse::InteractionFrame {
                        frame: Frame::ResnapshotRequired { current },
                    },
                )?;
                return Ok(());
            }
        }
        let mut byte = [0];
        match nix::sys::socket::recv(
            std::os::fd::AsRawFd::as_raw_fd(writer),
            &mut byte,
            nix::sys::socket::MsgFlags::MSG_PEEK | nix::sys::socket::MsgFlags::MSG_DONTWAIT,
        ) {
            Ok(0) => return Ok(()),
            Err(nix::errno::Errno::EAGAIN) => {}
            Ok(_) => return Ok(()),
            Err(_) => return Ok(()),
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn setup() -> (tempfile::TempDir, Arc<Mutex<Registry>>, UnixListener) {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("host.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let mut host = crate::tests::task(FOREGROUND_ID, AgentState::Running);
        host.info.session_id = "session-one".into();
        host.control_socket = Some(socket.to_string_lossy().into_owned());
        let mut reg = Registry {
            history_store: Some(Arc::new(
                HistoryStore::open(&dir.path().join("history.redb")).unwrap(),
            )),
            runtime_store: Some(Arc::new(
                RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap(),
            )),
            ..Default::default()
        };
        reg.tasks.insert(FOREGROUND_ID.into(), host);
        (dir, Arc::new(Mutex::new(reg)), listener)
    }

    fn command() -> Submit {
        Submit {
            conversation_id: FOREGROUND_ID.into(),
            session_id: "session-one".into(),
            command_id: "command-one".into(),
            text: "hello".into(),
            cwd: None,
        }
    }

    fn event(n: u64) -> String {
        serde_json::to_string(&tachyon_api::InteractionEventEnvelope {
            metadata: tachyon_api::InteractionMetadata::new(
                format!("event-{n}"),
                "command-one",
                FOREGROUND_ID,
                n,
            ),
            event: tachyon_api::InteractionEvent::ConversationFinished {
                text: format!("answer {n}"),
            },
        })
        .unwrap()
    }

    fn turn_event(id: &str, kind: tachyon_api::InteractionEvent) -> String {
        let mut metadata = tachyon_api::InteractionMetadata::new(id, "command", FOREGROUND_ID, 1);
        metadata.turn_id = Some("7".into());
        serde_json::to_string(&tachyon_api::InteractionEventEnvelope {
            metadata,
            event: kind,
        })
        .unwrap()
    }

    #[test]
    fn prefinal_checkpoint_reopen_gap_and_large_content_are_headless() {
        use tachyon_api::InteractionEvent;
        let (_dir, reg, _listener) = setup();
        publish(
            &reg,
            &turn_event(
                "accepted",
                InteractionEvent::UserTurnAccepted {
                    text: "hello".into(),
                },
            ),
        );
        let initial = snapshot(&reg).unwrap();
        for n in 0..260 {
            publish(
                &reg,
                &turn_event(
                    &format!("delta-{n}"),
                    InteractionEvent::ConversationDelta {
                        text: "x".repeat(300),
                    },
                ),
            );
        }
        assert!(reg
            .lock()
            .unwrap()
            .interaction
            .lock()
            .unwrap()
            .replay(&initial.revision)
            .is_err());
        let before = snapshot(&reg).unwrap();
        let response = &before.projection.responses[0];
        assert_eq!(
            response.phase,
            tachyon_api::interaction_manager::ResponsePhase::Answering
        );
        assert_eq!(response.answer_bytes, 78000);
        assert_eq!(response.answer.len(), 65536);
        let history = reg.lock().unwrap().history_store.clone().unwrap();
        let page = history
            .interaction_content(response.answer_ref.as_ref().unwrap(), 65000, Some(13000))
            .unwrap();
        assert_eq!(page.bytes, vec![b'x'; 13000]);
        assert!(page.next_offset.is_none());
        // Replace only the process-local manager. Recovery uses the existing store.
        *reg.lock().unwrap().interaction.lock().unwrap() = Default::default();
        let restored = snapshot(&reg).unwrap();
        assert_eq!(restored.projection, before.projection);
        assert_ne!(restored.revision.epoch, before.revision.epoch);
        publish(
            &reg,
            &turn_event(
                "delta-259",
                InteractionEvent::ConversationDelta {
                    text: "x".repeat(300),
                },
            ),
        );
        assert_eq!(
            snapshot(&reg).unwrap().projection.responses[0].answer_bytes,
            78000
        );
        publish(
            &reg,
            &turn_event(
                "final",
                InteractionEvent::ConversationFinished {
                    text: "authoritative final".into(),
                },
            ),
        );
        let final_snapshot = snapshot(&reg).unwrap();
        assert_eq!(
            final_snapshot.projection.responses[0].answer,
            "authoritative final"
        );
        assert_eq!(
            final_snapshot.history.last().unwrap().text,
            "authoritative final"
        );
    }

    #[test]
    fn durable_final_repairs_checkpoint_and_new_session_interrupts_only_pending() {
        use tachyon_api::InteractionEvent;
        let (_dir, reg, _listener) = setup();
        publish(
            &reg,
            &turn_event(
                "accepted",
                InteractionEvent::UserTurnAccepted {
                    text: "hello".into(),
                },
            ),
        );
        publish(
            &reg,
            &turn_event(
                "delta",
                InteractionEvent::ConversationDelta {
                    text: "prefix".into(),
                },
            ),
        );
        let history = reg.lock().unwrap().history_store.clone().unwrap();
        let pending = history.interaction_checkpoint().unwrap();
        publish(
            &reg,
            &turn_event(
                "final",
                InteractionEvent::ConversationFinished {
                    text: "durable".into(),
                },
            ),
        );
        history.save_interaction_checkpoint(&pending).unwrap();
        *reg.lock().unwrap().interaction.lock().unwrap() = Default::default();
        reg.lock()
            .unwrap()
            .tasks
            .get_mut(FOREGROUND_ID)
            .unwrap()
            .info
            .session_id = "new-session".into();
        let recovered = snapshot(&reg).unwrap();
        assert_eq!(recovered.projection.responses[0].answer, "durable");
        assert_eq!(
            recovered.projection.responses[0].phase,
            tachyon_api::interaction_manager::ResponsePhase::Completed
        );
    }

    #[test]
    fn old_canonical_final_cannot_complete_new_attempt_on_snapshot() {
        use tachyon_api::InteractionEvent;
        let (_dir, reg, _listener) = setup();
        publish(
            &reg,
            &turn_event(
                "old-final",
                InteractionEvent::ConversationFinished { text: "old".into() },
            ),
        );
        let mut acceptance: tachyon_api::InteractionEventEnvelope =
            serde_json::from_str(&turn_event(
                "new-attempt",
                InteractionEvent::UserTurnAccepted {
                    text: "retry".into(),
                },
            ))
            .unwrap();
        acceptance.metadata.generation = 1;
        publish(&reg, &serde_json::to_string(&acceptance).unwrap());
        assert_eq!(
            snapshot(&reg).unwrap().projection.responses[0].phase,
            tachyon_api::interaction_manager::ResponsePhase::Accepted
        );
        acceptance.metadata.message_id = "new-final".into();
        acceptance.event = InteractionEvent::ConversationFinished { text: "new".into() };
        publish(&reg, &serde_json::to_string(&acceptance).unwrap());
        assert_eq!(
            snapshot(&reg).unwrap().projection.responses[0].answer,
            "new"
        );
    }

    #[test]
    fn todo_bus_updates_without_client_polling_or_registry_lock_inversion() {
        use tachyon_api::todo::*;
        let (_dir, reg, _listener) = setup();
        initialize(&reg);
        let runtime = reg.lock().unwrap().runtime_store.clone().unwrap();
        let scope = TodoScope::Conversation {
            id: FOREGROUND_ID.into(),
        };
        let facade = runtime
            .todos(crate::runtime_store::todo::TodoAuthority::Bound {
                scope: scope.clone(),
                actor: TodoActor {
                    source: "host".into(),
                    actor: "test".into(),
                },
            })
            .unwrap();
        let cursor = reg.lock().unwrap().interaction.lock().unwrap().revision();
        // A mutation caller may already own the registry: the bus must not acquire it.
        let guard = reg.lock().unwrap();
        let response = facade
            .execute(TodoRequest::Add {
                scope: scope.clone(),
                command_id: "add".into(),
                expected_revision: 0,
                title: "verify".into(),
                description: String::new(),
            })
            .unwrap();
        drop(guard);
        let manager = reg.lock().unwrap().interaction.clone();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let updates = loop {
            let updates = manager.lock().unwrap().replay(&cursor).unwrap();
            if !updates.is_empty() {
                break updates;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(updates.len(), 1);
        assert!(updates[0].event.is_none());
        let TodoResponse::Mutation { todo, .. } = response else {
            panic!()
        };
        facade
            .execute(TodoRequest::Update {
                scope: scope.clone(),
                command_id: "complete".into(),
                id: todo.id,
                expected_revision: todo.revision,
                title: None,
                description: None,
                status: Some(TodoStatus::Completed),
            })
            .unwrap();
        let projection = loop {
            let projection = manager.lock().unwrap().projection();
            if projection.progress[0].scope_revision == Some(2) {
                break projection;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        };
        let progress = &projection.progress[0];
        assert_eq!(progress.scope_revision, Some(2));
        assert_eq!(progress.pending, 0);
        assert_eq!(progress.completed, 1);
        assert!(projection.works.is_empty());
        let blocked = manager.lock().unwrap();
        for revision in 2..142 {
            facade
                .execute(TodoRequest::Add {
                    scope: scope.clone(),
                    command_id: format!("overflow-{revision}"),
                    expected_revision: revision,
                    title: "queued".into(),
                    description: String::new(),
                })
                .unwrap();
        }
        drop(blocked);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let projection = manager.lock().unwrap().projection();
            if projection.progress[0].scope_revision == Some(142) {
                assert_eq!(projection.progress[0].pending, 140);
                assert_eq!(projection.progress[0].completed, 1);
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn admitted_work_not_error_prose_is_the_only_work_source() {
        let (_dir, reg, _listener) = setup();
        let mut worker = crate::tests::task("worker", AgentState::Running);
        worker.info.session_id = "worker-session".into();
        worker.info.logical_task_id = Some("work".into());
        worker.info.origin_turn_id = Some("session-one:7".into());
        worker.assignment = 2;
        let generation = worker.generation;
        let info = worker.info.clone();
        let request = WorkRequest {
            context_refs: vec![],
            constraints: None,
            attempt: None,
            work_id: "work".into(),
            objective: "Read the repository\n and verify results".into(),
            generation: worker.generation,
            assignment: 2,
            deadline_ms: 999,
            lifetime_class: LifetimeClass::Short,
        };
        {
            let mut guard = reg.lock().unwrap();
            guard.tasks.insert("worker".into(), worker);
            guard.works.insert(
                "work".into(),
                WorkRecord {
                    observed_calls: Default::default(),
                    partial_evidence: Default::default(),
                    request,
                    fingerprint: "test".into(),
                    worker_id: "worker".into(),
                    info,
                    review: None,
                    terminal_result: None,
                    subs: vec![],
                },
            );
        }
        work(&reg, "worker");
        publish(
            &reg,
            &turn_event(
                "accepted-work-turn",
                tachyon_api::InteractionEvent::UserTurnAccepted {
                    text: "read files".into(),
                },
            ),
        );
        for assignment in [1, 2] {
            let event = EventEnvelope {
                event_id: assignment,
                sequence: assignment,
                session_id: "worker-session".into(),
                conversation_id: Some(FOREGROUND_ID.into()),
                turn_id: Some("session-one:7".into()),
                task_id: Some("work".into()),
                parent_task_id: None,
                tool_call_id: Some("call".into()),
                actor: tachyon_api::Actor::Worker {
                    id: "worker".into(),
                },
                occurred_at_ms: 1,
                kind: StructuredAgentEvent::ToolStarted {
                    turn: None,
                    id: "call".into(),
                    name: "read".into(),
                    arguments: "{}".into(),
                    identity: Some(tachyon_api::ToolTelemetryIdentity {
                        work_id: Some("work".into()),
                        task_id: Some("work".into()),
                        generation: Some(generation),
                        assignment: Some(assignment),
                        attempt_id: None,
                    }),
                },
            };
            telemetry(&reg, "worker", &serde_json::to_string(&event).unwrap());
        }
        let projection = snapshot(&reg).unwrap().projection;
        assert_eq!(projection.works.len(), 1);
        assert_eq!(projection.works[0].metrics.tools_started, 1);
        assert_eq!(
            projection.works[0].latest_tool.as_ref().unwrap().name,
            "read"
        );
        assert_eq!(projection.responses[0].work_ids, vec!["work"]);
        assert_eq!(projection.responses[0].work_counts.active, 1);
        assert_eq!(
            projection.works[0].origin_turn_id.as_deref(),
            Some("session-one:7")
        );
        assert_eq!(
            projection.works[0].title,
            "Read the repository and verify results"
        );
        assert_eq!(
            projection.works[0].phase,
            tachyon_api::interaction_manager::WorkPhase::Running
        );
        assert_eq!(
            projection
                .progress
                .iter()
                .find(|p| p.scope == projection.works[0].todo_scope)
                .unwrap()
                .scope_revision,
            Some(0)
        );
    }

    fn attach(
        registry: &Arc<Mutex<Registry>>,
    ) -> (
        BufReader<UnixStream>,
        std::thread::JoinHandle<std::io::Result<()>>,
    ) {
        let (server, mut client) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let registry = registry.clone();
        writeln!(
            client,
            "{}",
            serde_json::to_string(&ApiRequest::ForegroundSubscribe).unwrap()
        )
        .unwrap();
        let thread = std::thread::spawn(move || handle_connection(server, registry));
        (BufReader::new(client), thread)
    }

    fn frame(reader: &mut BufReader<UnixStream>) -> Frame {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        match serde_json::from_str(&line).unwrap() {
            ApiResponse::InteractionFrame { frame } => frame,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn headless_submit_two_subscribers_duplicate_and_detach() {
        let (_dir, reg, listener) = setup();
        let receipt = submit(&reg, &command()).unwrap();
        assert_eq!(
            receipt.admission,
            tachyon_api::interaction_manager::Admission::Delivered
        );
        let (stream, _) = listener.accept().unwrap();
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).unwrap();
        assert!(line.contains("accept_user_turn"));
        assert_eq!(submit(&reg, &command()).unwrap(), receipt);
        listener.set_nonblocking(true).unwrap();
        assert!(listener.accept().is_err());
        let mut collision = command();
        collision.text = "different".into();
        assert!(submit(&reg, &collision).is_err());
        let (mut one, thread_one) = attach(&reg);
        let (mut two, thread_two) = attach(&reg);
        assert!(matches!(frame(&mut one), Frame::Snapshot { .. }));
        assert!(matches!(frame(&mut two), Frame::Snapshot { .. }));
        publish(&reg, &event(1));
        let Frame::Update { update: a } = frame(&mut one) else {
            panic!()
        };
        let Frame::Update { update: b } = frame(&mut two) else {
            panic!()
        };
        assert_eq!(a.revision, b.revision);
        drop(one);
        thread_one.join().unwrap().unwrap();
        drop(two);
        thread_two.join().unwrap().unwrap();
        publish(&reg, &event(2));
        let snapshot = snapshot(&reg).unwrap();
        assert_eq!(snapshot.history.len(), 2);
        assert_eq!(snapshot.history[1].text, "answer 2");
        assert_eq!(snapshot.host_state, Some(AgentState::Running));
    }

    #[test]
    fn snapshot_handoff_replays_or_explicitly_gaps() {
        let (_dir, reg, _listener) = setup();
        let cursor = snapshot(&reg).unwrap().revision;
        publish(&reg, &event(1));
        let manager = reg.lock().unwrap().interaction.clone();
        assert_eq!(manager.lock().unwrap().replay(&cursor).unwrap().len(), 1);
        for n in 2..260 {
            publish(&reg, &event(n));
        }
        assert!(manager.lock().unwrap().replay(&cursor).is_err());
        let fresh = snapshot(&reg).unwrap();
        assert_eq!(fresh.history.last().unwrap().text, "answer 259");
        publish(&reg, &event(260));
        assert_eq!(
            manager
                .lock()
                .unwrap()
                .replay(&fresh.revision)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn uncertain_receipt_survives_reopen_without_resending() {
        let (dir, reg, listener) = setup();
        reg.lock()
            .unwrap()
            .history_store
            .as_ref()
            .unwrap()
            .command_receipt(&command(), false, None)
            .unwrap();
        reg.lock().unwrap().history_store = None;
        reg.lock().unwrap().history_store = Some(Arc::new(
            HistoryStore::open(&dir.path().join("history.redb")).unwrap(),
        ));
        reg.lock()
            .unwrap()
            .tasks
            .get_mut(FOREGROUND_ID)
            .unwrap()
            .info
            .session_id = "new-session".into();
        assert_eq!(
            submit(&reg, &command()).unwrap().admission,
            tachyon_api::interaction_manager::Admission::Uncertain
        );
        listener.set_nonblocking(true).unwrap();
        assert!(listener.accept().is_err());
        let mut stale = command();
        stale.command_id = "new-command".into();
        assert!(submit(&reg, &stale).is_err());
    }

    #[test]
    fn concurrent_duplicate_admission_delivers_one_command() {
        let (_dir, reg, listener) = setup();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let reg = reg.clone();
                std::thread::spawn(move || submit(&reg, &command()).unwrap())
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let _ = listener.accept().unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(listener.accept().is_err());
    }

    #[test]
    fn repeated_host_event_ids_do_not_collide_across_sessions() {
        let (_dir, reg, _listener) = setup();
        publish(&reg, &event(1));
        reg.lock()
            .unwrap()
            .tasks
            .get_mut(FOREGROUND_ID)
            .unwrap()
            .info
            .session_id = "session-two".into();
        publish(&reg, &event(1));
        let history = snapshot(&reg).unwrap().history;
        assert_eq!(history.len(), 2);
        assert_ne!(history[0].event_id, history[1].event_id);
    }

    #[test]
    fn simultaneous_identical_prompts_bind_by_origin_not_text_or_arrival_order() {
        let (_dir, reg, listener) = setup();
        let mut commands = vec![command(), command()];
        commands[1].command_id = "second-client".into();
        let threads: Vec<_> = commands
            .iter()
            .cloned()
            .map(|command| {
                let reg = reg.clone();
                std::thread::spawn(move || submit(&reg, &command).unwrap())
            })
            .collect();
        for thread in threads {
            assert!(thread.join().unwrap().accepted.is_none());
        }
        let mut inputs = Vec::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).unwrap();
            inputs.push(
                serde_json::from_str::<tachyon_api::InteractionCommandEnvelope>(
                    line.trim().strip_prefix("input\t").unwrap(),
                )
                .unwrap(),
            );
        }
        let (mut one, thread_one) = attach(&reg);
        let (mut two, thread_two) = attach(&reg);
        let _ = frame(&mut one);
        let _ = frame(&mut two);
        for (index, input) in inputs.iter().enumerate().rev() {
            let mut metadata = input.metadata.clone();
            metadata.causation_id = Some(metadata.message_id.clone());
            metadata.message_id.push_str(":accepted");
            metadata.turn_id = Some((index + 1).to_string());
            let accepted = tachyon_api::InteractionEventEnvelope {
                metadata,
                event: tachyon_api::InteractionEvent::UserTurnAccepted {
                    text: "hello".into(),
                },
            };
            publish(&reg, &serde_json::to_string(&accepted).unwrap());
            let Frame::Update { update } = frame(&mut one) else {
                panic!()
            };
            let Frame::Update { update: other } = frame(&mut two) else {
                panic!()
            };
            assert_eq!(update.event, other.event);
            let event = update.event.as_ref().unwrap();
            let origin = event.metadata.command_origin.as_ref().unwrap();
            let command = commands
                .iter()
                .find(|c| c.command_id == origin.command_id)
                .unwrap();
            let receipt = submit(&reg, command).unwrap();
            assert_eq!(receipt.origin.as_ref(), Some(origin));
            assert_eq!(
                receipt.accepted.as_ref().unwrap().turn_id,
                format!("session-one:{}", index + 1)
            );
            assert_eq!(
                receipt.accepted.as_ref().unwrap().event_id,
                event.metadata.message_id
            );
            publish(&reg, &serde_json::to_string(&accepted).unwrap());
            assert_eq!(submit(&reg, command).unwrap(), receipt);
        }
        drop(one);
        drop(two);
        thread_one.join().unwrap().unwrap();
        thread_two.join().unwrap().unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(listener.accept().is_err());
    }

    #[test]
    fn delayed_acceptance_binds_uncertain_receipt_durably_and_never_rebinds() {
        use tachyon_api::interaction_manager::{Admission, CommandOrigin};
        let (dir, reg, _listener) = setup();
        let origin = CommandOrigin {
            session_id: "session-one".into(),
            command_id: command().command_id,
            host_message_id: "original-host-command".into(),
        };
        let history = reg.lock().unwrap().history_store.clone().unwrap();
        history
            .command_receipt(&command(), false, Some(&origin))
            .unwrap();
        let mut accepted: tachyon_api::InteractionEventEnvelope =
            serde_json::from_str(&event(1)).unwrap();
        accepted.event = tachyon_api::InteractionEvent::UserTurnAccepted {
            text: "hello".into(),
        };
        accepted.metadata.command_origin = Some(origin.clone());
        accepted.metadata.turn_id = Some("1".into());
        accepted.metadata.causation_id = Some("wrong-host-command".into());
        publish(&reg, &serde_json::to_string(&accepted).unwrap());
        assert!(submit(&reg, &command()).unwrap().accepted.is_none());
        accepted.metadata.causation_id = Some(origin.host_message_id.clone());
        publish(&reg, &serde_json::to_string(&accepted).unwrap());
        let receipt = submit(&reg, &command()).unwrap();
        assert_eq!(receipt.admission, Admission::Uncertain);
        assert_eq!(receipt.accepted.as_ref().unwrap().turn_id, "session-one:1");
        // A late transport-status update must not overwrite the acceptance.
        assert_eq!(
            history
                .command_receipt(&command(), true, Some(&origin))
                .unwrap()
                .0
                .accepted,
            receipt.accepted
        );
        accepted.metadata.turn_id = Some("2".into());
        publish(&reg, &serde_json::to_string(&accepted).unwrap());
        assert_eq!(submit(&reg, &command()).unwrap().accepted, receipt.accepted);
        assert_eq!(snapshot(&reg).unwrap().history.len(), 1);
        let mut collision = command();
        collision.command_id = "other-command".into();
        let collision_origin = CommandOrigin {
            command_id: collision.command_id.clone(),
            host_message_id: "other-host-command".into(),
            ..origin.clone()
        };
        history
            .command_receipt(&collision, false, Some(&collision_origin))
            .unwrap();
        let mut competing = accepted.clone();
        competing.metadata.turn_id = Some("1".into());
        competing.metadata.command_origin = Some(collision_origin.clone());
        competing.metadata.correlation_id = collision_origin.command_id.clone();
        competing.metadata.causation_id = Some(collision_origin.host_message_id.clone());
        publish(&reg, &serde_json::to_string(&competing).unwrap());
        assert!(submit(&reg, &collision).unwrap().accepted.is_none());
        drop(history);
        reg.lock().unwrap().history_store = None;
        reg.lock().unwrap().history_store = Some(Arc::new(
            HistoryStore::open(&dir.path().join("history.redb")).unwrap(),
        ));
        reg.lock()
            .unwrap()
            .tasks
            .get_mut(FOREGROUND_ID)
            .unwrap()
            .info
            .session_id = "session-two".into();
        assert_eq!(submit(&reg, &command()).unwrap().accepted, receipt.accepted);
        // The old origin cannot be laundered into the new host session.
        publish(&reg, &serde_json::to_string(&accepted).unwrap());
        assert_eq!(snapshot(&reg).unwrap().history.len(), 1);
        let mut next = command();
        next.session_id = "session-two".into();
        let next_origin = submit(&reg, &next).unwrap().origin.unwrap();
        accepted.metadata.message_id = format!("{}:accepted", next_origin.host_message_id);
        accepted.metadata.causation_id = Some(next_origin.host_message_id.clone());
        accepted.metadata.command_origin = Some(next_origin);
        accepted.metadata.turn_id = Some("1".into());
        publish(&reg, &serde_json::to_string(&accepted).unwrap());
        assert_eq!(
            submit(&reg, &next).unwrap().accepted.unwrap().turn_id,
            "session-two:1"
        );
        assert_eq!(submit(&reg, &command()).unwrap().accepted, receipt.accepted);
        assert_eq!(snapshot(&reg).unwrap().history.len(), 2);
    }

    #[test]
    fn manager_raw_metrics_tools_and_worker_origins_share_exact_turn_key() {
        let (_dir, reg, _listener) = setup();
        for session in ["session-one", "session-two"] {
            reg.lock()
                .unwrap()
                .tasks
                .get_mut(FOREGROUND_ID)
                .unwrap()
                .info
                .session_id = session.into();
            let info = reg.lock().unwrap().tasks[FOREGROUND_ID].info.clone();
            let mut accepted: tachyon_api::InteractionEventEnvelope =
                serde_json::from_str(&event(1)).unwrap();
            accepted.metadata.turn_id = Some("7".into());
            let wire = correlate_event(&serde_json::to_string(&accepted).unwrap(), &info);
            publish(&reg, &wire);
            let turn = snapshot(&reg)
                .unwrap()
                .history
                .into_iter()
                .find(|h| h.event_id.starts_with(session))
                .unwrap()
                .turn_id
                .unwrap();
            assert_eq!(turn, format!("{session}:7"));
            for kind in [
                StructuredAgentEvent::Timing {
                    turn: 7,
                    stage: "input_accepted".into(),
                    elapsed_ms: 1,
                },
                StructuredAgentEvent::Usage {
                    turn: Some(7),
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                    context_tokens: 1,
                    context_window: None,
                },
                StructuredAgentEvent::ToolStarted {
                    turn: Some(7),
                    id: "call".into(),
                    name: "test".into(),
                    arguments: "{}".into(),
                    identity: None,
                },
            ] {
                let event = EventEnvelope {
                    event_id: 1,
                    session_id: FOREGROUND_ID.into(),
                    conversation_id: Some(FOREGROUND_ID.into()),
                    turn_id: Some("7".into()),
                    task_id: None,
                    parent_task_id: None,
                    tool_call_id: None,
                    actor: tachyon_api::Actor::Foreground,
                    sequence: 1,
                    occurred_at_ms: 1,
                    kind,
                };
                let wire = correlate_event(&serde_json::to_string(&event).unwrap(), &info);
                let correlated: EventEnvelope = serde_json::from_str(&wire).unwrap();
                assert_eq!(correlated.turn_id.as_ref(), Some(&turn));
                assert_eq!(correlated.session_id, session);
                assert_eq!(correlate_event(&wire, &info), wire);
                if matches!(event.kind, StructuredAgentEvent::Usage { .. }) {
                    let mut worker = crate::tests::task("worker", AgentState::Running).info;
                    worker.origin_turn_id = Some(turn.clone());
                    let correlated: EventEnvelope = serde_json::from_str(&correlate_event(
                        &serde_json::to_string(&event).unwrap(),
                        &worker,
                    ))
                    .unwrap();
                    assert_eq!(correlated.turn_id.as_ref(), Some(&turn));
                    assert_eq!(correlated.conversation_id.as_deref(), Some(FOREGROUND_ID));
                }
            }
        }
    }

    #[test]
    fn due_reminder_delivers_without_any_subscriber() {
        let (_dir, reg, listener) = setup();
        let store = reg.lock().unwrap().runtime_store.clone().unwrap();
        store
            .create_reminder("reminder", "source", FOREGROUND_ID, 1, "remember", 1, 2)
            .unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let scheduler = {
            let reg = reg.clone();
            let shutdown = shutdown.clone();
            std::thread::spawn(move || run_reminder_scheduler(reg, shutdown))
        };
        listener.set_nonblocking(true).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let stream = loop {
            if let Ok((stream, _)) = listener.accept() {
                break stream;
            }
            if std::time::Instant::now() > deadline {
                shutdown.store(true, Ordering::SeqCst);
                scheduler.join().unwrap();
                panic!("headless reminder not delivered");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let mut wire = String::new();
        BufReader::new(stream).read_line(&mut wire).unwrap();
        assert!(wire.contains("reminder-delivery-reminder"));
        shutdown.store(true, Ordering::SeqCst);
        scheduler.join().unwrap();
    }

    #[test]
    fn slow_consumer_does_not_hold_registry_or_manager() {
        let (_dir, reg, _listener) = setup();
        let (mut client, thread) = attach(&reg);
        assert!(matches!(frame(&mut client), Frame::Snapshot { .. }));
        let manager = reg.lock().unwrap().interaction.clone();
        let mut event: tachyon_api::InteractionEventEnvelope =
            serde_json::from_str(&event(1)).unwrap();
        event.event = tachyon_api::InteractionEvent::ConversationDelta {
            text: "x".repeat(128 * 1024),
        };
        for _ in 0..24 {
            manager
                .lock()
                .unwrap()
                .publish("session-one".into(), event.clone());
        }
        std::thread::sleep(Duration::from_millis(150));
        assert!(reg.try_lock().is_ok());
        assert!(manager.try_lock().is_ok());
        drop(client);
        let _ = thread.join().unwrap();
    }
}
