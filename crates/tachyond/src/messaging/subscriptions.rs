use std::os::unix::net::UnixStream;
use std::sync::{mpsc, Arc, Mutex};

use tachyon_api::types::{ApiResponse, EventStream};

use crate::{write_response, AgentEvent, Registry};

impl Registry {
    pub(crate) fn subscribe(&mut self, id: &str) -> Option<mpsc::Receiver<AgentEvent>> {
        let (tx, rx) = mpsc::channel();
        let task = self.tasks.get_mut(id)?;
        if let Some(usage) = &task.terminal_usage {
            let _ = tx.send(AgentEvent {
                stream: EventStream::Stdout,
                data: usage.clone(),
            });
        }
        if let Some(result) = &task.terminal_result {
            let _ = tx.send(AgentEvent {
                stream: EventStream::Stdout,
                data: result.clone(),
            });
        }
        task.subs.push(tx);
        Some(rx)
    }

    pub(crate) fn subscribe_work(&mut self, work_id: &str) -> Option<mpsc::Receiver<AgentEvent>> {
        let (tx, rx) = mpsc::channel();
        let work = self.works.get_mut(work_id)?;
        if let Some(result) = &work.terminal_result {
            let _ = tx.send(AgentEvent {
                stream: EventStream::Stdout,
                data: result.clone(),
            });
        } else {
            work.subs.push(tx);
        }
        Some(rx)
    }
}

/// Stream an agent's events to a subscriber until the agent ends.
pub(crate) fn stream_agent(
    writer: &mut UnixStream,
    id: &str,
    registry: Arc<Mutex<Registry>>,
) -> std::io::Result<()> {
    let rx = {
        let mut reg = registry.lock().unwrap();
        match reg.subscribe(id) {
            Some(rx) => rx,
            None => {
                return write_response(writer, &ApiResponse::error(format!("no such agent: {id}")))
            }
        }
    };

    while let Ok(ev) = rx.recv() {
        let resp = ApiResponse::Event {
            stream: ev.stream,
            data: ev.data,
        };
        if write_response(writer, &resp).is_err() {
            break;
        }
    }
    Ok(())
}

pub(crate) fn stream_work(
    writer: &mut UnixStream,
    work_id: &str,
    registry: Arc<Mutex<Registry>>,
) -> std::io::Result<()> {
    let rx = {
        let mut reg = registry.lock().unwrap();
        match reg.subscribe_work(work_id) {
            Some(rx) => rx,
            None => {
                return write_response(
                    writer,
                    &ApiResponse::error(format!("no such work: {work_id}")),
                )
            }
        }
    };
    while let Ok(event) = rx.recv() {
        if write_response(
            writer,
            &ApiResponse::Event {
                stream: event.stream,
                data: event.data,
            },
        )
        .is_err()
        {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{push_event, tests::task, WorkRecord};
    use tachyon_api::{
        AgentEvent as StructuredAgentEvent, AgentState, LifetimeClass, WorkOutcome, WorkRequest,
        WorkResult,
    };

    #[test]
    fn typed_work_result_is_terminal_once_and_replayed_by_work_id() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let mut worker = task("worker", AgentState::Running);
        worker.warm = true;
        worker.info.retained = true;
        worker.info.logical_task_id = Some("work-1".into());
        let info = worker.info.clone();
        let request = WorkRequest {
            context_refs: vec![],
            constraints: None,
            attempt: None,
            work_id: "work-1".into(),
            objective: "inspect".into(),
            generation: 0,
            assignment: 0,
            deadline_ms: 100,
            lifetime_class: LifetimeClass::Long,
        };
        let live = {
            let mut reg = registry.lock().unwrap();
            reg.tasks.insert("worker".into(), worker);
            reg.works.insert(
                request.work_id.clone(),
                WorkRecord {
                    request: request.clone(),
                    fingerprint: "fingerprint".into(),
                    worker_id: "worker".into(),
                    info,
                    review: None,
                    terminal_result: None,
                    subs: Vec::new(),
                },
            );
            reg.subscribe_work("work-1").unwrap()
        };
        let result = WorkResult {
            work_id: request.work_id.clone(),
            objective: request.objective.clone(),
            attempt_id: None,
            candidate_refs: None,
            final_context: None,
            generation: 0,
            instruction_revision: None,
            evidence: Default::default(),
            timing: None,
            assignment: 0,
            outcome: WorkOutcome::Completed {
                result: "first".into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: true,
            },
        };
        let event = serde_json::to_string(&StructuredAgentEvent::WorkResult {
            result: result.clone(),
        })
        .unwrap();
        push_event(&registry, "worker", EventStream::Stdout, &event);

        let duplicate = serde_json::to_string(&StructuredAgentEvent::WorkResult {
            result: WorkResult {
                outcome: WorkOutcome::Completed {
                    result: "second".into(),
                    artifacts: Vec::new(),
                    context: String::new(),
                    suggested_reuse: true,
                },
                ..result
            },
        })
        .unwrap();
        push_event(&registry, "worker", EventStream::Stdout, &duplicate);

        let mut reg = registry.lock().unwrap();
        assert_eq!(reg.tasks["worker"].info.turns_used, 1);
        let replay = reg.subscribe_work("work-1").unwrap();
        let replayed = replay.recv().unwrap();
        assert!(replayed.data.contains("first"));
        assert!(!replayed.data.contains("second"));
        assert_eq!(live.try_recv().unwrap().data, replayed.data);
        assert!(matches!(
            live.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert!(matches!(
            replay.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert!(reg.works["work-1"].subs.is_empty());
    }

    #[test]
    fn agent_subscription_replays_usage_before_result_and_remains_live() {
        let mut reg = Registry::default();
        let mut worker = task("worker", AgentState::Completed);
        worker.terminal_usage = Some("usage".into());
        worker.terminal_result = Some("result".into());
        reg.tasks.insert("worker".into(), worker);
        let rx = reg.subscribe("worker").unwrap();
        for expected in ["usage", "result"] {
            let event = rx.try_recv().unwrap();
            assert_eq!(event.stream, EventStream::Stdout);
            assert_eq!(event.data, expected);
        }
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(reg.tasks["worker"].subs.len(), 1);
        assert!(reg.subscribe("missing").is_none());
        assert!(reg.subscribe_work("missing").is_none());
    }

    #[test]
    fn stream_errors_keep_existing_wire_messages() {
        use std::io::{BufRead, BufReader};
        for (agent, expected) in [
            (true, "no such agent: missing"),
            (false, "no such work: missing"),
        ] {
            let (mut writer, reader) = UnixStream::pair().unwrap();
            let registry = Arc::new(Mutex::new(Registry::default()));
            if agent {
                stream_agent(&mut writer, "missing", registry).unwrap();
            } else {
                stream_work(&mut writer, "missing", registry).unwrap();
            }
            let mut line = String::new();
            BufReader::new(reader).read_line(&mut line).unwrap();
            assert_eq!(
                line,
                format!(
                    "{}\n",
                    serde_json::to_string(&ApiResponse::error(expected)).unwrap()
                )
            );
        }
    }
}
