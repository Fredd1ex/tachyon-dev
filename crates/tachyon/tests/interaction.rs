#![forbid(unsafe_code)]
//! Real CLI processes against a fake Unix-socket provider; no daemon or runtime.
use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tachyon_api::interaction_manager::{Admission, Frame, Receipt, Revision, Snapshot, Update};
use tachyon_api::transport::{read_request, write_response};
use tachyon_api::{
    ApiRequest, ApiResponse, InteractionEvent, InteractionEventEnvelope, InteractionMetadata,
};

fn accept(listener: &UnixListener) -> UnixStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match listener.accept() {
            Ok((socket, _)) => {
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                return socket;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "CLI did not connect");
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("{e}"),
        }
    }
}

fn snapshot() -> Frame {
    Frame::Snapshot {
        snapshot: Snapshot {
            history_content: Vec::new(),
            projection: Default::default(),
            projection_next: None,
            revision: Revision {
                epoch: "fake-daemon".into(),
                sequence: 0,
            },
            conversation_id: "foreground".into(),
            session_id: Some("host-session".into()),
            host_state: None,
            history: Vec::new(),
        },
    }
}

fn send(socket: &mut UnixStream, frame: Frame) {
    write_response(socket, &ApiResponse::InteractionFrame { frame }).unwrap();
}

#[test]
fn identical_headless_submissions_keep_reversed_authoritative_bindings() {
    use tachyon_api::interaction_manager::{AcceptedTurn, CommandOrigin};
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("state")).unwrap();
    let listener = UnixListener::bind(root.path().join("state/tachyond.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut clients = Vec::new();
    for (id, turn) in [("a", "host-session:2"), ("b", "host-session:1")] {
        let child = Command::new(env!("CARGO_BIN_EXE_tachyon"))
            .args([
                "chat",
                "same",
                "--follow",
                "--json",
                "--session-id",
                "host-session",
                "--command-id",
                id,
            ])
            .env("TACHYON_DATA_DIR", root.path())
            .env("TACHYON_ALLOW_ROOT", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut requests = accept(&listener);
        let mut stream = accept(&listener);
        assert!(matches!(
            read_request(&mut BufReader::new(stream.try_clone().unwrap())).unwrap(),
            ApiRequest::InteractionAttach { after: None }
        ));
        send(&mut stream, snapshot());
        let mut reader = BufReader::new(requests.try_clone().unwrap());
        assert!(matches!(
            read_request(&mut reader).unwrap(),
            ApiRequest::InteractionSnapshot
        ));
        send(&mut requests, snapshot());
        let ApiRequest::InteractionSubmit { command } = read_request(&mut reader).unwrap() else {
            panic!()
        };
        let receipt = Receipt {
            command,
            admission: if id == "b" {
                Admission::Uncertain
            } else {
                Admission::Delivered
            },
            origin: Some(CommandOrigin {
                session_id: "host-session".into(),
                command_id: id.into(),
                host_message_id: format!("input-{id}"),
            }),
            accepted: Some(AcceptedTurn {
                turn_id: turn.into(),
                event_id: format!("accepted-{id}"),
            }),
        };
        clients.push((child, requests, stream, receipt));
    }
    let mut sequence = 0;
    for index in [1, 0] {
        let receipt = clients[index].3.clone();
        // Acceptance can already be waiting on the dedicated stream when the
        // request socket finally returns its receipt.
        for event in [
            InteractionEvent::UserTurnAccepted {
                text: "same".into(),
            },
            InteractionEvent::ConversationFinished {
                text: format!("answer {}", receipt.command.command_id),
            },
        ] {
            sequence += 1;
            let id = if matches!(event, InteractionEvent::UserTurnAccepted { .. }) {
                receipt.accepted.as_ref().unwrap().event_id.clone()
            } else {
                format!("final-{}", receipt.command.command_id)
            };
            let mut metadata =
                InteractionMetadata::new(id, &receipt.command.command_id, "foreground", sequence);
            metadata.command_origin = receipt.origin.clone();
            metadata.turn_id = Some(receipt.accepted.as_ref().unwrap().turn_id.clone());
            let frame = Frame::Update {
                update: Update {
                    revision: Revision {
                        epoch: "fake-daemon".into(),
                        sequence,
                    },
                    session_id: "host-session".into(),
                    event: Some(InteractionEventEnvelope { metadata, event }),
                    changes: Vec::new(),
                },
            };
            for (_, _, stream, _) in &mut clients {
                send(stream, frame.clone());
            }
        }
        write_response(
            &mut clients[index].1,
            &ApiResponse::InteractionReceipt { receipt },
        )
        .unwrap();
    }
    for (child, requests, stream, receipt) in clients {
        drop(requests);
        drop(stream);
        let result = child.wait_with_output().unwrap();
        let records: Vec<serde_json::Value> = String::from_utf8(result.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records[0], serde_json::to_value(&receipt).unwrap());
        assert_eq!(
            records
                .iter()
                .filter(|r| r["update"]["event"]["event"] == "user_turn_accepted")
                .count(),
            2
        );
        assert_eq!(
            records
                .iter()
                .filter(|r| r["update"]["event"]["event"] == "conversation_finished")
                .count(),
            2
        );
    }
    assert!(listener.accept().is_err());
}

#[test]
fn unknown_or_uncertain_admission_never_retries_or_claims_completion() {
    for (uncertain, accepted) in [(false, false), (true, false), (true, true)] {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("state")).unwrap();
        let listener = UnixListener::bind(root.path().join("state/tachyond.sock")).unwrap();
        listener.set_nonblocking(true).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_tachyon"))
            .args(["chat", "hello", "--json"])
            .env("TACHYON_DATA_DIR", root.path())
            .env("TACHYON_ALLOW_ROOT", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut socket = accept(&listener);
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        assert!(matches!(
            read_request(&mut reader).unwrap(),
            ApiRequest::InteractionSnapshot
        ));
        send(&mut socket, snapshot());
        let ApiRequest::InteractionSubmit { command } = read_request(&mut reader).unwrap() else {
            panic!()
        };
        if uncertain {
            write_response(
                &mut socket,
                &ApiResponse::InteractionReceipt {
                    receipt: Receipt {
                        origin: accepted.then(|| tachyon_api::interaction_manager::CommandOrigin {
                            session_id: command.session_id.clone(),
                            command_id: command.command_id.clone(),
                            host_message_id: "input".into(),
                        }),
                        accepted: accepted.then(|| {
                            tachyon_api::interaction_manager::AcceptedTurn {
                                turn_id: "host-session:1".into(),
                                event_id: "accepted-1".into(),
                            }
                        }),
                        command: command.clone(),
                        admission: Admission::Uncertain,
                    },
                },
            )
            .unwrap();
        }
        drop(reader);
        drop(socket);
        let result = child.wait_with_output().unwrap();
        assert_eq!(result.status.success(), accepted);
        let error = String::from_utf8(result.stderr).unwrap();
        assert!(error.contains(&command.command_id));
        if accepted {
            let value: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
            assert_eq!(value["admission"], "uncertain");
            assert_eq!(value["accepted"]["turn_id"], "host-session:1");
        } else {
            assert!(error.contains("not retried"));
        }
        assert!(listener.accept().is_err());
    }
}

#[test]
fn attach_gap_resnapshots_without_submitting_and_text_deduplicates_canonical_ids() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("state")).unwrap();
    let listener = UnixListener::bind(root.path().join("state/tachyond.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_tachyon"))
        .args(["chat"])
        .env("TACHYON_DATA_DIR", root.path())
        .env("TACHYON_ALLOW_ROOT", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let _requests = accept(&listener);
    let mut stream = accept(&listener);
    assert!(matches!(
        read_request(&mut BufReader::new(stream.try_clone().unwrap())).unwrap(),
        ApiRequest::InteractionAttach { after: None }
    ));
    let mut canonical = snapshot();
    let Frame::Snapshot { snapshot } = &mut canonical else {
        panic!()
    };
    snapshot.history.push(tachyon_api::HistoryEntry {
        attention: None,
        event_id: "host:final".into(),
        kind: tachyon_api::HistoryKind::Conversation,
        conversation_id: "foreground".into(),
        turn_id: Some("host:1".into()),
        occurred_at_ms: 1,
        role: tachyon_api::HistoryRole::Assistant,
        text: "canonical once".into(),
        task_id: None,
        task_state: None,
    });
    send(&mut stream, canonical.clone());
    send(
        &mut stream,
        Frame::ResnapshotRequired {
            current: Revision {
                epoch: "new-daemon".into(),
                sequence: 0,
            },
        },
    );
    drop(stream);
    let mut stream = accept(&listener);
    assert!(matches!(
        read_request(&mut BufReader::new(stream.try_clone().unwrap())).unwrap(),
        ApiRequest::InteractionAttach { after: None }
    ));
    send(&mut stream, canonical);
    drop(stream);
    let result = child.wait_with_output().unwrap();
    assert!(!result.status.success());
    let text = String::from_utf8(result.stdout).unwrap();
    assert_eq!(text.matches("canonical once").count(), 1);
    assert!(String::from_utf8(result.stderr)
        .unwrap()
        .contains("reattaching"));
    assert!(listener.accept().is_err());
}

#[test]
fn two_headless_clients_share_canonical_stream_and_reconcile_identical_admission() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("state")).unwrap();
    let listener = UnixListener::bind(root.path().join("state/tachyond.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let cli = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_tachyon"))
            .args(args)
            .env("TACHYON_DATA_DIR", root.path())
            .env("TACHYON_ALLOW_ROOT", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let observer = cli(&["chat"]);
    let _observer_requests = accept(&listener);
    let mut observer_stream = accept(&listener);
    assert!(matches!(
        read_request(&mut BufReader::new(observer_stream.try_clone().unwrap())).unwrap(),
        ApiRequest::InteractionAttach { after: None }
    ));
    send(&mut observer_stream, snapshot());

    let submitter = cli(&[
        "chat",
        "hello",
        "--follow",
        "--json",
        "--command-id",
        "pending-1",
        "--session-id",
        "host-session",
    ]);
    let mut requests = accept(&listener);
    let mut stream = accept(&listener);
    assert!(matches!(
        read_request(&mut BufReader::new(stream.try_clone().unwrap())).unwrap(),
        ApiRequest::InteractionAttach { after: None }
    ));
    send(&mut stream, snapshot());
    let mut reader = BufReader::new(requests.try_clone().unwrap());
    assert!(matches!(
        read_request(&mut reader).unwrap(),
        ApiRequest::InteractionSnapshot
    ));
    send(&mut requests, snapshot());
    let ApiRequest::InteractionSubmit { command } = read_request(&mut reader).unwrap() else {
        panic!()
    };
    assert_eq!(command.session_id, "host-session");
    assert_eq!(command.command_id, "pending-1");
    let receipt = Receipt {
        origin: Some(tachyon_api::interaction_manager::CommandOrigin {
            session_id: command.session_id.clone(),
            command_id: command.command_id.clone(),
            host_message_id: "host-input".into(),
        }),
        accepted: Some(tachyon_api::interaction_manager::AcceptedTurn {
            turn_id: "host-session:1".into(),
            event_id: "host-session:event-0".into(),
        }),
        command: command.clone(),
        admission: Admission::Delivered,
    };
    write_response(
        &mut requests,
        &ApiResponse::InteractionReceipt {
            receipt: receipt.clone(),
        },
    )
    .unwrap();
    for (sequence, event) in [
        InteractionEvent::UserTurnAccepted {
            text: "hello".into(),
        },
        InteractionEvent::ConversationDelta {
            text: "partial".into(),
        },
        InteractionEvent::ConversationFinished {
            text: "canonical answer".into(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let mut metadata = InteractionMetadata::new(
            format!("host-session:event-{sequence}"),
            "request",
            "foreground",
            1,
        );
        metadata.turn_id = Some("host-session:1".into());
        metadata.command_origin = receipt.origin.clone();
        let frame = Frame::Update {
            update: Update {
                revision: Revision {
                    epoch: "fake-daemon".into(),
                    sequence: sequence as u64 + 1,
                },
                session_id: "host-session".into(),
                event: Some(InteractionEventEnvelope { metadata, event }),
                changes: Vec::new(),
            },
        };
        send(&mut stream, frame.clone());
        send(&mut observer_stream, frame);
    }
    drop(stream);
    drop(observer_stream);
    let observer = observer.wait_with_output().unwrap();
    let submitter = submitter.wait_with_output().unwrap();
    assert!(
        !observer.status.success(),
        "EOF reports recovery needed, not completion"
    );
    let text = String::from_utf8(observer.stdout).unwrap();
    assert_eq!(text.matches("canonical answer").count(), 1);
    assert!(!text.contains("partial"));
    let json = String::from_utf8(submitter.stdout).unwrap();
    let records: Vec<serde_json::Value> = json
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(records.len(), 5);
    assert_eq!(records[0]["command"]["command_id"], "pending-1");
    assert_eq!(
        records[4]["update"]["event"]["event"],
        "conversation_finished"
    );
    assert_eq!(records[4]["update"]["event"]["text"], "canonical answer");

    // Operator repeats the original identity, not a fresh command. No follow socket.
    let duplicate = cli(&[
        "chat",
        "hello",
        "--json",
        "--command-id",
        "pending-1",
        "--session-id",
        "host-session",
    ]);
    let mut socket = accept(&listener);
    let mut reader = BufReader::new(socket.try_clone().unwrap());
    assert!(matches!(
        read_request(&mut reader).unwrap(),
        ApiRequest::InteractionSnapshot
    ));
    send(&mut socket, snapshot());
    let ApiRequest::InteractionSubmit { command: repeated } = read_request(&mut reader).unwrap()
    else {
        panic!()
    };
    assert_eq!(repeated, command);
    write_response(&mut socket, &ApiResponse::InteractionReceipt { receipt }).unwrap();
    assert!(duplicate.wait_with_output().unwrap().status.success());
    assert!(listener.accept().is_err(), "no automatic retry connections");
}
