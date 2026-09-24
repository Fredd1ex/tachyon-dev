//! Linux PTY integration without fork/pre_exec/ioctl unsafe code or a shipped binary.
use super::*;
use nix::{
    pty::{openpty, Winsize},
    sys::{
        signal::{kill, Signal},
        termios::tcgetattr,
    },
    unistd::Pid,
};
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    os::unix::net::UnixListener,
    process::{Child, Command, Stdio},
    thread::{self, JoinHandle},
};
use tachyon_api::{
    transport::{read_request, write_response},
    types::ApiRequest,
};

const CHILD: &str = "app::verification::pty::fixture_child";
const TIMEOUT: Duration = Duration::from_secs(5);

#[test]
#[ignore = "private child entrypoint; launched only by offline_pty_event_loop"]
fn fixture_child() {
    let Some(root) = std::env::var_os("TACHYON_TUI_PTY_FIXTURE") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut probe = UnixStream::connect(root.join("probe.sock")).unwrap();
    probe.set_write_timeout(Some(TIMEOUT)).unwrap();
    let mut app = fixture(&root, 32);
    app.probe = Some(probe.try_clone().unwrap());
    let (reader, mut writer) = UnixStream::pair().unwrap();
    writer.set_write_timeout(Some(TIMEOUT)).unwrap();
    app.subscriptions.attach_fixture(reader).unwrap();
    let mut start = probe.try_clone().unwrap();
    let producer = thread::spawn(move || {
        let request = read_request(&mut BufReader::new(writer.try_clone().unwrap())).unwrap();
        assert!(matches!(
            request,
            ApiRequest::InteractionAttach { after: None }
        ));
        write_response(
            &mut writer,
            &ApiResponse::InteractionFrame {
                frame: tachyon_api::interaction_manager::Frame::Snapshot {
                    snapshot: tachyon_api::interaction_manager::Snapshot {
                        history_content: Vec::new(),
                        projection: Default::default(),
                        projection_next: None,
                        revision: tachyon_api::interaction_manager::Revision {
                            epoch: "offline".into(),
                            sequence: 0,
                        },
                        conversation_id: FOREGROUND_ID.into(),
                        session_id: Some("host".into()),
                        host_state: None,
                        history: Vec::new(),
                    },
                },
            },
        )
        .unwrap();
        let mut signal = [0];
        start.read_exact(&mut signal).unwrap();
        assert_eq!(signal, *b"f");
        let mut sent = 0u64;
        loop {
            let mut metadata = tachyon_api::InteractionMetadata::new(
                format!("event-{sent}"),
                "request",
                FOREGROUND_ID,
                sent,
            );
            metadata.turn_id = Some("host:31".into());
            if let Err(error) = write_response(
                &mut writer,
                &ApiResponse::InteractionFrame {
                    frame: tachyon_api::interaction_manager::Frame::Update {
                        update: tachyon_api::interaction_manager::Update {
                            changes: Vec::new(),
                            revision: tachyon_api::interaction_manager::Revision {
                                epoch: "offline".into(),
                                sequence: sent + 1,
                            },
                            session_id: "host".into(),
                            event: Some(tachyon_api::InteractionEventEnvelope {
                                metadata,
                                event: tachyon_api::InteractionEvent::ConversationFinished {
                                    text: format!("Offline socket update {sent}"),
                                },
                            }),
                        },
                    },
                },
            ) {
                assert!(
                    matches!(
                        error.kind(),
                        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                    ),
                    "producer stopped without socket cancellation: {error}"
                );
                break;
            }
            sent += 1;
        }
        sent
    });
    enable_raw_mode().unwrap();
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen).unwrap();
    app.mouse_capture.apply(&mut stdout).unwrap();
    stdout
        .execute(crossterm::event::EnableBracketedPaste)
        .unwrap();
    let terminal = Terminal::new(CrosstermBackend::new(stdout)).unwrap();
    app.event_loop(terminal).unwrap();
    assert!(!crossterm::terminal::is_raw_mode_enabled().unwrap());
    let sent = producer.join().unwrap();
    writeln!(
        probe,
        "{}",
        serde_json::json!({"event": "stopped", "sent": sent})
    )
    .unwrap();
}

// On assertion failure, do not leave an interactive child or a blocked PTY drainer.
struct Process {
    child: Child,
    slave: Option<File>,
    output: Option<JoinHandle<Vec<u8>>>,
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.slave.take();
        if let Some(output) = self.output.take() {
            let bytes = output.join().unwrap();
            if thread::panicking() {
                eprintln!("PTY output: {}", String::from_utf8_lossy(&bytes));
            }
        }
    }
}

fn next(reader: &mut BufReader<UnixStream>, predicate: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut line = String::new();
        assert!(
            reader.read_line(&mut line).unwrap() > 0,
            "fixture closed observation socket"
        );
        let state: Value = serde_json::from_str(&line).unwrap();
        if predicate(&state) {
            return state;
        }
        assert!(
            Instant::now() < deadline,
            "observation timeout; last state: {state}"
        );
    }
}

fn input(
    master: &mut File,
    reader: &mut BufReader<UnixStream>,
    bytes: &[u8],
    decoded: &str,
) -> Value {
    master.write_all(bytes).unwrap();
    next(reader, |s| s["event"].as_str().unwrap().contains(decoded))
}

fn resize(process: &Process, reader: &mut BufReader<UnixStream>, columns: u16, rows: u16) -> Value {
    assert!(Command::new("stty")
        .args(["cols", &columns.to_string(), "rows", &rows.to_string()])
        .stdin(Stdio::from(
            process.slave.as_ref().unwrap().try_clone().unwrap()
        ))
        .status()
        .unwrap()
        .success());
    kill(Pid::from_raw(process.child.id() as i32), Signal::SIGWINCH).unwrap();
    next(reader, |s| {
        s["event"] == format!("Resize({columns}, {rows})")
    });
    next(reader, |s| {
        s["event"] == "frame" && s["size"] == serde_json::json!([columns, rows])
    })
}

#[test]
#[ignore = "opt-in Linux openpty integration; no daemon/provider; needs setsid and stty"]
fn offline_pty_event_loop() {
    let root = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(root.path().join("probe.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let pty = openpty(
        Some(&Winsize {
            ws_row: 32,
            ws_col: 100,
            ws_xpixel: 0,
            ws_ypixel: 0,
        }),
        None,
    )
    .unwrap();
    let slave = File::from(pty.slave);
    let original = tcgetattr(&slave).unwrap();
    let mut master = File::from(pty.master);
    // setsid removes any inherited controlling terminal. Crossterm then uses the
    // PTY stdio, never the developer's terminal. No unsafe pre_exec is required.
    let child = Command::new("setsid")
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            CHILD,
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TACHYON_TUI_PTY_FIXTURE", root.path())
        .env("TERM", "xterm-256color")
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave.try_clone().unwrap()))
        .spawn()
        .unwrap();
    let mut drain = master.try_clone().unwrap();
    let output = thread::spawn(move || {
        let mut output = Vec::new();
        let mut bytes = [0; 8192];
        loop {
            match drain.read(&mut bytes) {
                Ok(0) => break,
                Ok(n) => {
                    output.extend_from_slice(&bytes[..n]);
                    if output.len() > 2 * 1024 * 1024 {
                        output.drain(..1024 * 1024);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.raw_os_error() == Some(nix::libc::EIO) => break,
                Err(error) => panic!("PTY read: {error}"),
            }
        }
        output
    });
    let mut process = Process {
        child,
        slave: Some(slave),
        output: Some(output),
    };
    let deadline = Instant::now() + TIMEOUT;
    let socket = loop {
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "child startup timeout");
                assert!(
                    process.child.try_wait().unwrap().is_none(),
                    "child exited before startup"
                );
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("accept: {error}"),
        }
    };
    socket.set_read_timeout(Some(TIMEOUT)).unwrap();
    let mut reader = BufReader::new(socket);
    let initial = next(&mut reader, |s| s["event"] == "frame");
    assert_eq!(initial["size"], serde_json::json!([100, 32]));
    assert_eq!(initial["capture"], false);
    let top = initial["top"].as_u64().unwrap();

    // Forty arrow reports cross the production 32-input batch limit. This is the
    // byte-level equivalent of a terminal translating uncaptured trackpad motion.
    master.write_all(b"\x1b[A".repeat(40).as_slice()).unwrap();
    for index in 1..=40 {
        let state = next(&mut reader, |s| {
            s["event"].as_str().unwrap().contains("code: Up")
        });
        assert_eq!(state["top"], top - 3 * index);
        assert!(state["trace"].is_null());
    }
    let down = input(&mut master, &mut reader, b"\x1b[B", "code: Down");
    assert_eq!(down["top"], top - 117);
    let ignored = input(&mut master, &mut reader, b"\x1b[<64;6;6M", "ScrollUp");
    assert_eq!(ignored["top"], down["top"]);
    let capture = input(&mut master, &mut reader, b"/mouse\r", "code: Enter");
    assert_eq!(capture["capture"], true);
    let wheel = input(&mut master, &mut reader, b"\x1b[<64;6;6M", "ScrollUp");
    assert_eq!(wheel["top"], top - 125);
    assert!(wheel["trace"].is_null());
    let wheel = input(&mut master, &mut reader, b"\x1b[<65;6;6M", "ScrollDown");
    assert_eq!(wheel["top"], top - 117);
    assert!(wheel["trace"].is_null());
    let chat = next(&mut reader, |s| s["event"] == "frame");
    let opened = input(&mut master, &mut reader, b"\x0f", "code: Char('o')");
    assert_eq!(opened["trace"], chat["trace"]);
    assert_eq!(opened["top"], chat["top"]);
    next(&mut reader, |s| {
        s["event"] == "frame" && s["inspector_painted"] == false
    });

    // Actual kernel window-size changes plus SIGWINCH, not injected Event::Resize.
    for (columns, rows) in [(20, 6), (100, 32)] {
        let resized = resize(&process, &mut reader, columns, rows);
        assert_eq!(resized["anchor"], chat["anchor"]);
        assert_eq!(resized["follow"], false);
        assert_eq!(resized["trace"], opened["trace"]);
    }
    let pane = input(&mut master, &mut reader, b"\t", "code: Tab");
    assert_eq!(pane["pane"], true);
    let tiny_pane = resize(&process, &mut reader, 20, 6);
    assert_eq!(tiny_pane["pane"], true);
    assert_eq!(tiny_pane["anchor"], chat["anchor"]);
    let hidden = input(&mut master, &mut reader, b"\x1b", "code: Esc");
    assert_eq!(hidden["pane"], false);
    let tiny_chat = next(&mut reader, |s| s["event"] == "frame" && s["pane"] == false);
    assert_eq!(tiny_chat["anchor"], chat["anchor"]);

    // The synthetic daemon floods API-framed socket events until cancellation.
    reader.get_mut().write_all(b"f").unwrap();
    let flooded = next(&mut reader, |s| {
        s["event"] == "frame"
            && s["revision"].as_u64().unwrap() > initial["revision"].as_u64().unwrap() + 1024
    });
    let start = Instant::now();
    let help = input(&mut master, &mut reader, b"\x10", "code: Char('p')");
    assert_eq!(help["help"], true);
    next(&mut reader, |s| s["event"] == "frame" && s["help"] == true);
    let input_latency = start.elapsed();
    assert!(
        input_latency < Duration::from_secs(2),
        "input-to-frame under flood"
    );
    let closed = input(&mut master, &mut reader, b"\x1b", "code: Esc");
    assert_eq!(closed["help"], false);
    next(&mut reader, |s| {
        s["event"] == "frame"
            && s["revision"].as_u64().unwrap() > flooded["revision"].as_u64().unwrap()
    });
    let start = Instant::now();
    let typed = input(&mut master, &mut reader, b"x", "code: Char('x')");
    assert_eq!(typed["draft"], "x");
    next(&mut reader, |s| s["event"] == "frame" && s["draft"] == "x");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "draft frame under flood"
    );
    let start = Instant::now();
    input(&mut master, &mut reader, b"\x03", "code: Char('c')");
    let stopped = next(&mut reader, |s| s["event"] == "stopped");
    assert!(stopped["sent"].as_u64().unwrap() > 1024);
    loop {
        if let Some(status) = process.child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(start.elapsed() < TIMEOUT, "child teardown timed out");
        thread::sleep(Duration::from_millis(5));
    }
    let teardown = start.elapsed();
    assert!(teardown < TIMEOUT, "joined teardown exceeded deadline");
    assert_eq!(
        tcgetattr(process.slave.as_ref().unwrap()).unwrap(),
        original
    );
    process.slave.take();
    let output = process.output.take().unwrap().join().unwrap();
    for sequence in [
        b"\x1b[?1049h".as_slice(),
        b"\x1b[?1006h",
        b"\x1b[?1006l",
        b"\x1b[?2004l",
        b"\x1b[?25h",
        b"\x1b[?1049l",
    ] {
        assert!(
            output.windows(sequence.len()).any(|w| w == sequence),
            "missing terminal sequence {sequence:?}"
        );
    }
    let enabled = output
        .windows(8)
        .rposition(|w| w == b"\x1b[?1006h")
        .unwrap();
    let disabled = output
        .windows(8)
        .rposition(|w| w == b"\x1b[?1006l")
        .unwrap();
    assert!(
        disabled > enabled,
        "capture must be disabled during teardown"
    );
    assert!(!root.path().join("tui-visits").exists());
    assert!(!root.path().join("tui-session.json").exists());
    assert!(!root.path().join("tui-attention.json").exists());
    eprintln!(
        "PTY: {} socket updates sent; input/frame {input_latency:?}; joined teardown {teardown:?}",
        stopped["sent"]
    );
}
