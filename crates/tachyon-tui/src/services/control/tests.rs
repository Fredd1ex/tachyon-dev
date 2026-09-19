use super::{execute_with, Command, Output, Worker, CAPACITY};
use crate::app::{actions, attention, editor, scheduler, update, Thread};
use std::io::BufReader;
use std::os::unix::net::UnixStream;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use tachyon_api::transport::{read_request, read_response, write_request, write_response};
use tachyon_api::types::{AgentInfo, ApiRequest, ApiResponse};

fn info(id: &str) -> AgentInfo {
    serde_json::from_value(serde_json::json!({
        "id": id, "task": "fake", "state": "running", "pid": null,
        "workspace": "/fake", "created_secs": 1
    }))
    .unwrap()
}

#[test]
fn blocked_local_intake_keeps_input_and_frames_live_and_drains_fifo_without_retry() {
    let (mut client, mut server) = UnixStream::pair().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    server
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let (entered, started) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let mut reader = BufReader::new(server.try_clone().unwrap());
        let mut observed = Vec::new();
        for index in 0..CAPACITY {
            let req = read_request(&mut reader).unwrap();
            if index == 0 {
                assert!(
                    matches!(&req, ApiRequest::ForegroundChat { text, cwd: None } if text == "first")
                );
                entered.send(()).unwrap();
                gate.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            let response = match &req {
                ApiRequest::ForegroundChat { .. } => ApiResponse::Chat {
                    id: tachyon_api::FOREGROUND_ID.into(),
                },
                ApiRequest::AgentStop { id } | ApiRequest::AgentResume { id } => {
                    ApiResponse::Agent { info: info(id) }
                }
                _ => panic!("unexpected request"),
            };
            observed.push(serde_json::to_string(&req).unwrap());
            write_response(&mut server, &response).unwrap();
        }
        observed
    });
    let mut reader = BufReader::new(client.try_clone().unwrap());
    let mut worker = Worker::with_executor(move |command| {
        execute_with(command, |req, _| {
            write_request(&mut client, req).unwrap();
            read_response(&mut reader).map_err(|e| e.to_string())
        })
    })
    .unwrap();
    let mut threads = vec![Thread::new_foreground()];
    actions::submit_chat("/managed first", &mut threads, &mut worker).unwrap();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut draft = "next".to_string();
    let mut cursor = 4;
    editor::insert_at(&mut draft, &mut cursor, '!');
    assert_eq!(draft, "next!");
    assert!(scheduler::frame_due(true, true, Duration::from_millis(16)));
    let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(30, 3)).unwrap();
    terminal
        .draw(|f| f.render_widget(ratatui::widgets::Paragraph::new(draft.as_str()), f.area()))
        .unwrap();
    for index in 1..CAPACITY {
        let verb = if index % 2 == 0 { "resume" } else { "stop" };
        actions::handle_slash(&format!("{verb} agent-{index}"), &mut threads, &mut worker).unwrap();
    }
    let before = threads[0].items.len();
    assert!(actions::submit_chat(&draft, &mut threads, &mut worker)
        .unwrap_err()
        .contains("not accepted"));
    assert_eq!(threads[0].items.len(), before);
    assert_eq!(draft, "next!");
    assert_eq!(worker.pending(), CAPACITY);
    assert!(worker.poll().is_none());
    release.send(()).unwrap();
    let results = worker.shutdown();
    assert_eq!(results.len(), CAPACITY);
    assert_eq!(
        results.iter().map(|r| r.id).collect::<Vec<_>>(),
        (1..=CAPACITY as u64).collect::<Vec<_>>()
    );
    assert!(results
        .iter()
        .all(|r| matches!(&r.output, Output::Message(Ok(_)))));
    let observed = server.join().unwrap();
    for (index, req) in observed.iter().enumerate().skip(1) {
        assert!(req.contains(&format!("agent-{index}")));
    }
    assert_eq!(worker.pending(), 0);
    assert!(worker.shutdown().is_empty());
}

#[test]
fn unread_results_count_against_admission_bound() {
    let (done, rx) = mpsc::channel();
    let mut worker = Worker::with_executor(move |_| {
        done.send(()).unwrap();
        Output::Message(Ok("done".into()))
    })
    .unwrap();
    for index in 0..CAPACITY {
        worker
            .submit(index.to_string(), Command::Chat("fake".into()))
            .unwrap();
    }
    for _ in 0..CAPACITY {
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    assert!(worker
        .submit("extra".into(), Command::Chat("fake".into()))
        .is_err());
    assert_eq!(worker.shutdown().len(), CAPACITY);
}

#[test]
fn selection_switch_cannot_retarget_result_and_duplicate_generation_is_ignored() {
    let (entered, started) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let mut worker = Worker::with_executor(move |command| {
        let Command::Agent(ApiRequest::AgentStop { id }) = command else {
            panic!("wrong command")
        };
        assert_eq!(id, "original");
        entered.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(5)).unwrap();
        Output::Message(Ok("Terminated".into()))
    })
    .unwrap();
    let mut threads = vec![Thread::new_foreground()];
    let mut agents = std::collections::HashMap::from([("original".into(), info("original"))]);
    actions::pane_control("stop", 2, &agents, &mut threads, &mut worker).unwrap();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    agents.clear();
    agents.insert("new-selection".into(), info("new-selection"));
    release.send(()).unwrap();
    let result = worker.shutdown().pop().unwrap();
    let id = result.id;
    assert_eq!(result.label, "pane: stop original");
    let directory = tempfile::tempdir_in("/tmp/opencode").unwrap();
    let mut attention = attention::State::open(directory.path()).unwrap();
    threads[0].streaming = true;
    update::control(result, &mut attention, &mut threads);
    assert!(threads[0].streaming);
    assert!(threads[0]
        .items
        .last()
        .unwrap()
        .text
        .contains("stop original"));
    assert!(!threads[0]
        .items
        .last()
        .unwrap()
        .text
        .contains("new-selection"));
    assert_eq!(
        agents["new-selection"].state,
        tachyon_api::types::AgentState::Running
    );
    assert!(worker
        .accept(id, Output::Message(Ok("duplicate".into())))
        .is_none());
    assert!(worker
        .accept(id + 100, Output::Message(Ok("unknown generation".into())))
        .is_none());
}

#[test]
fn unknown_outcome_is_not_retried_and_later_commands_still_execute() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let mut worker = Worker::with_executor(move |command| {
        execute_with(command, |req, _| {
            observed
                .lock()
                .unwrap()
                .push(serde_json::to_string(req).unwrap());
            Err("connection lost after send; outcome unknown; not retried".into())
        })
    })
    .unwrap();
    let mut threads = vec![Thread::new_foreground()];
    actions::handle_slash("stop first", &mut threads, &mut worker).unwrap();
    actions::handle_slash("resume second", &mut threads, &mut worker).unwrap();
    let results = worker.shutdown();
    assert_eq!(calls.lock().unwrap().len(), 2);
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .all(|r| r.report().contains("outcome unknown")));
}

#[test]
fn drop_waits_for_running_and_queued_mutations() {
    let (entered, started) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let calls = Arc::new(Mutex::new(0));
    let observed = calls.clone();
    let mut worker = Worker::with_executor(move |_| {
        let mut count = observed.lock().unwrap();
        if *count == 0 {
            entered.send(()).unwrap();
            gate.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        *count += 1;
        Output::Message(Ok("completed".into()))
    })
    .unwrap();
    for index in 0..3 {
        worker
            .submit(
                index.to_string(),
                Command::Agent(ApiRequest::AgentStop {
                    id: index.to_string(),
                }),
            )
            .unwrap();
    }
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let (done, finished) = mpsc::channel();
    let join = std::thread::spawn(move || {
        drop(worker);
        done.send(()).unwrap();
    });
    assert!(finished.recv_timeout(Duration::from_millis(20)).is_err());
    release.send(()).unwrap();
    finished.recv_timeout(Duration::from_secs(5)).unwrap();
    join.join().unwrap();
    assert_eq!(*calls.lock().unwrap(), 3);
}

#[test]
fn mismatched_agent_and_scoped_attention_responses_do_not_apply() {
    let output = execute_with(
        Command::Agent(ApiRequest::AgentStop {
            id: "original".into(),
        }),
        |_, _| {
            Ok(ApiResponse::Agent {
                info: info("different"),
            })
        },
    );
    assert!(matches!(output, Output::Message(Err(error)) if error.contains("mismatched")));
    let directory = tempfile::tempdir_in("/tmp/opencode").unwrap();
    let attention = attention::State::open(directory.path()).unwrap();
    let requests = attention
        .command("attention campaign original")
        .unwrap()
        .unwrap();
    let output = execute_with(Command::Attention(requests), |request, _| {
        assert!(
            matches!(request, ApiRequest::AttentionList { scope: tachyon_api::todo::TodoScope::Campaign { campaign_id }, .. } if campaign_id == "original")
        );
        Ok(ApiResponse::AttentionList {
            snapshot: tachyon_api::attention::AttentionSnapshot {
                records: vec![tachyon_api::attention::Attention {
                    id: "same-id-wrong-scope".into(),
                    command_id: "command".into(),
                    cause_id: "cause".into(),
                    scope: tachyon_api::todo::TodoScope::Campaign {
                        campaign_id: "different".into(),
                    },
                    work_id: None,
                    campaign_id: Some("different".into()),
                    generation: 1,
                    instruction_revision: 1,
                    category: tachyon_api::attention::AttentionCategory::Question,
                    severity: tachyon_api::attention::AttentionSeverity::Warning,
                    accepted_at_ms: 1,
                    delivered_at_ms: None,
                    displayed_at_ms: None,
                    acknowledged_at_ms: None,
                }],
                next_cursor: None,
                watermark: tachyon_api::operational_events::OperationalWatermark {
                    instance_id: "fake".into(),
                    sequence: 1,
                },
            },
        })
    });
    assert!(
        matches!(output, Output::Attention(Err(error)) if error.contains("mismatched attention scope"))
    );
    assert!(attention.command("stop original").is_none());
    assert!(attention.command("ack unknown").unwrap().is_err());
}

#[test]
fn daemon_controls_are_owned_ordered_and_survive_an_earlier_executor_panic() {
    let ui_thread = std::thread::current().id();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let mut worker = Worker::with_executor(move |command| {
        assert_ne!(std::thread::current().id(), ui_thread);
        let Command::Daemon(action) = command else {
            panic!("wrong command")
        };
        observed.lock().unwrap().push(action.clone());
        if action == "stop" {
            panic!("fake service panic after a possible mutation");
        }
        Output::Message(Ok("request completed".into()))
    })
    .unwrap();
    actions::daemon_control("stop", &mut worker).unwrap();
    actions::daemon_control("start", &mut worker).unwrap();
    let results = worker.shutdown();
    assert_eq!(*calls.lock().unwrap(), ["stop", "start"]);
    assert_eq!(results.len(), 2);
    assert!(results[0].report().contains("outcome unknown; not retried"));
    assert!(results[1].report().contains("request completed"));
    assert!(actions::daemon_control("restart", &mut worker)
        .unwrap_err()
        .contains("not accepted"));
}
