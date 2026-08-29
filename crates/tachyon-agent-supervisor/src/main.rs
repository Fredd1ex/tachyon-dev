#![forbid(unsafe_code)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use tachyon_api::types::{AgentEvent, EventEnvelope};

#[derive(Default)]
struct SupervisorState {
    clients: Vec<mpsc::Sender<String>>,
    ready_event: Option<String>,
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let socket = value(&args, "--socket")?;
    let ghost = value(&args, "--ghost")?;
    let id = value(&args, "--id")?;
    let cwd = value(&args, "--cwd")?;

    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    listener.set_nonblocking(true)?;
    std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    let mut child = Command::new(ghost)
        .args([
            "--chat",
            "--role",
            "worker",
            "--agent-id",
            &id,
            "--cwd",
            &cwd,
        ])
        .current_dir(&cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = child.id();
    std::fs::write(format!("{socket}.pid"), pid.to_string())?;
    let stdin = Arc::new(Mutex::new(child.stdin.take().expect("ghost stdin")));
    let state = Arc::new(Mutex::new(SupervisorState::default()));

    spawn_pump(child.stdout.take(), "stdout", Arc::clone(&state));
    spawn_pump(child.stderr.take(), "stderr", Arc::clone(&state));

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let stdin = Arc::clone(&stdin);
                let state = Arc::clone(&state);
                thread::spawn(move || handle_client(stream, stdin, state, pid));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => eprintln!("supervisor: accept: {error}"),
        }
        if let Some(status) = child.try_wait()? {
            state.lock().unwrap().ready_event = None;
            broadcast(&state, format!("exit\t{}", status.code().unwrap_or(-1)));
            break;
        }
        thread::sleep(std::time::Duration::from_millis(50));
    }
    let _ = std::fs::remove_file(&socket);
    let _ = std::fs::remove_file(format!("{socket}.pid"));
    Ok(())
}

fn spawn_pump<R: std::io::Read + Send + 'static>(
    stream: Option<R>,
    name: &str,
    state: Arc<Mutex<SupervisorState>>,
) {
    let Some(stream) = stream else { return };
    let name = name.to_string();
    thread::spawn(move || {
        for line in BufReader::new(stream).lines().map_while(Result::ok) {
            if name == "stdout" && is_ready_event(&line) {
                state.lock().unwrap().ready_event = Some(line.clone());
            }
            broadcast(&state, format!("{name}\t{line}"));
        }
    });
}

fn handle_client(
    stream: UnixStream,
    stdin: Arc<Mutex<std::process::ChildStdin>>,
    state: Arc<Mutex<SupervisorState>>,
    pid: u32,
) {
    let (tx, rx) = mpsc::channel();
    {
        let mut state = state.lock().unwrap();
        if let Some(ready) = &state.ready_event {
            let _ = tx.send(format!("stdout\t{ready}"));
        }
        state.clients.push(tx);
    }
    let mut writer = stream.try_clone().ok();
    thread::spawn(move || {
        if let Some(ref mut writer) = writer {
            for event in rx {
                let _ = writeln!(writer, "{event}");
                let _ = writer.flush();
            }
        }
    });
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    while reader
        .read_line(&mut line)
        .ok()
        .filter(|n| *n > 0)
        .is_some()
    {
        let command = line.trim_end().to_string();
        line.clear();
        if let Some(input) = command.strip_prefix("input\t") {
            state.lock().unwrap().ready_event = None;
            if let Ok(mut stdin) = stdin.lock() {
                let _ = writeln!(stdin, "{input}");
                let _ = stdin.flush();
            }
        } else if let Some(signal) = command.strip_prefix("signal\t") {
            let signal = match signal {
                "int" => Signal::SIGINT,
                "kill" => Signal::SIGKILL,
                _ => Signal::SIGTERM,
            };
            let _ = kill(Pid::from_raw(pid as i32), signal);
        }
    }
}

fn broadcast(state: &Arc<Mutex<SupervisorState>>, event: String) {
    state
        .lock()
        .unwrap()
        .clients
        .retain(|client| client.send(event.clone()).is_ok());
}

fn is_ready_event(data: &str) -> bool {
    serde_json::from_str::<EventEnvelope>(data)
        .map(|envelope| envelope.kind)
        .or_else(|_| serde_json::from_str::<AgentEvent>(data))
        .is_ok_and(|event| matches!(event, AgentEvent::Status { phase, .. } if phase == "ready"))
}

fn value(args: &[String], key: &str) -> std::io::Result<String> {
    args.windows(2)
        .find(|pair| pair[0] == key)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_typed_ready_status_is_latched() {
        let ready = serde_json::to_string(&AgentEvent::Status {
            turn: None,
            phase: "ready".into(),
            message: String::new(),
        })
        .unwrap();
        let working = serde_json::to_string(&AgentEvent::Status {
            turn: Some(1),
            phase: "working".into(),
            message: String::new(),
        })
        .unwrap();
        assert!(is_ready_event(&ready));
        assert!(!is_ready_event(&working));
        assert!(!is_ready_event("[ghost] ready"));
    }
}
