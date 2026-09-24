//! Ordered, non-retrying explicit commands. The bound includes unread results.
use std::collections::BTreeMap;
use std::io;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;
use std::time::Duration;

use tachyon_api::attention::Attention;
use tachyon_api::types::{ApiRequest, ApiResponse};
use tachyon_client::Client;

use super::super::attention;

const CAPACITY: usize = 16;

pub(in crate::app) enum Command {
    Agent(ApiRequest),
    Daemon(String),
    Submit(tachyon_api::interaction_manager::Submit),
    Attention(Vec<ApiRequest>),
}

pub(in crate::app) enum Output {
    Interaction(tachyon_api::interaction_manager::Receipt),
    Message(Result<String, String>),
    Attention(Result<(Vec<Attention>, String), String>),
}

pub(in crate::app) struct Completion {
    pub(in crate::app) id: u64,
    pub(in crate::app) label: String,
    pub(in crate::app) output: Output,
}

pub(in crate::app) struct Worker {
    sender: Option<SyncSender<(u64, Command)>>,
    results: Receiver<(u64, Output)>,
    join: Option<JoinHandle<()>>,
    pending: BTreeMap<u64, String>,
    generation: u64,
}

impl Worker {
    #[cfg(test)]
    pub(in crate::app) fn offline() -> io::Result<Self> {
        Self::with_executor(|_| panic!("offline fixture must not submit controls"))
    }

    pub(in crate::app) fn start() -> io::Result<Self> {
        Self::with_executor(execute)
    }

    fn with_executor(
        mut execute: impl FnMut(Command) -> Output + Send + 'static,
    ) -> io::Result<Self> {
        let (sender, requests) = mpsc::sync_channel(CAPACITY);
        let (out, results) = mpsc::sync_channel(CAPACITY);
        let join = std::thread::Builder::new()
            .name("tui-control".into())
            .spawn(move || {
                while let Ok((id, command)) = requests.recv() {
                    let output =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(command)))
                            .unwrap_or_else(|_| {
                                Output::Message(Err(
                                    "command worker panicked; outcome unknown; not retried".into(),
                                ))
                            });
                    // At most CAPACITY commands exist across requests, execution and results.
                    if out.send((id, output)).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            results,
            join: Some(join),
            pending: BTreeMap::new(),
            generation: 0,
        })
    }

    pub(in crate::app) fn submit(
        &mut self,
        label: String,
        command: Command,
    ) -> Result<u64, String> {
        if self.pending.len() == CAPACITY {
            return Err("Command queue full; not accepted or sent. Draft retained; submit again only when ready.".into());
        }
        let id = self
            .generation
            .checked_add(1)
            .ok_or("Command IDs exhausted; not accepted")?;
        let sender = self
            .sender
            .as_ref()
            .ok_or("Command worker stopped; not accepted")?;
        sender
            .try_send((id, command))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => "Command queue full; not accepted or sent",
                mpsc::TrySendError::Disconnected(_) => {
                    "Command worker unavailable; not accepted or sent"
                }
            })?;
        self.generation = id;
        self.pending.insert(id, label);
        Ok(id)
    }

    fn accept(&mut self, id: u64, output: Output) -> Option<Completion> {
        let label = self.pending.remove(&id)?;
        Some(Completion { id, label, output })
    }

    pub(in crate::app) fn poll(&mut self) -> Option<Completion> {
        while let Ok((id, output)) = self.results.try_recv() {
            if let Some(result) = self.accept(id, output) {
                return Some(result);
            }
        }
        None
    }

    pub(in crate::app) fn pending(&self) -> usize {
        self.pending.len()
    }

    pub(in crate::app) fn shutdown(&mut self) -> Vec<Completion> {
        self.sender.take();
        let failed = self.join.take().is_some_and(|join| join.join().is_err());
        let mut results = Vec::new();
        while let Some(result) = self.poll() {
            results.push(result);
        }
        for (id, label) in std::mem::take(&mut self.pending) {
            results.push(Completion {
                id,
                label,
                output: Output::Message(Err(format!(
                    "command worker {} without a result; outcome unknown; not retried",
                    if failed { "panicked" } else { "stopped" }
                ))),
            });
        }
        results
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        for result in self.shutdown() {
            // Early setup/error paths have no UI left to receive outcomes.
            eprintln!("{}", result.report());
        }
    }
}

impl Completion {
    pub(in crate::app) fn report(&self) -> String {
        if let Output::Interaction(receipt) = &self.output {
            return format!(
                "command #{} {}: transport {:?}; host acceptance {:?}; not completion; receipt: {}",
                self.id,
                self.label,
                receipt.admission,
                receipt.accepted,
                serde_json::to_string(receipt).unwrap()
            );
        }
        let text = match &self.output {
            Output::Interaction(_) => unreachable!(),
            Output::Message(Ok(text)) => text.as_str(),
            Output::Message(Err(error)) | Output::Attention(Err(error)) => error.as_str(),
            Output::Attention(Ok((_, text))) => text.as_str(),
        };
        format!("command #{} {}: {text}", self.id, self.label)
    }
}

fn execute(command: Command) -> Output {
    let mut client = None;
    execute_with(command, |request, timeout| {
        if client.is_none() {
            client = Some(Client::connect().map_err(|e| format!("not sent: {e}"))?);
        }
        client
            .as_mut()
            .unwrap()
            .request(request, timeout)
            .map_err(|e| format!("{e}; outcome may be unknown; not retried"))
    })
}

fn execute_with(
    command: Command,
    mut request: impl FnMut(&ApiRequest, Duration) -> Result<ApiResponse, String>,
) -> Output {
    if let Command::Submit(command) = command {
        let recovery = serde_json::to_string(&command).unwrap();
        return match request(&ApiRequest::InteractionSubmit { command: command.clone() }, Duration::from_secs(5)) {
            Ok(ApiResponse::InteractionReceipt { receipt }) if receipt.command == command => Output::Interaction(receipt),
            response => Output::Message(Err(format!("admission unknown; not retried ({response:?}); reconcile identical command, never a new ID: {recovery}"))),
        };
    }
    if let Command::Attention(requests) = command {
        return Output::Attention(attention::execute(requests, |req| {
            request(req, Duration::from_secs(5))
        }));
    }
    Output::Message((|| match command {
        Command::Daemon(action) => {
            let status = tachyon_cli_command()
                .arg("daemon")
                .arg(&action)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map_err(|e| format!("{e}; daemon command outcome may be unknown; not retried"))?;
            if status.success() {
                Ok("request completed".into())
            } else {
                Err(format!(
                    "daemon command exited with {status}; inspect daemon state before retrying"
                ))
            }
        }
        Command::Submit(_) => unreachable!(),
        Command::Agent(req) => {
            let id = match &req {
                ApiRequest::AgentAwait { id }
                | ApiRequest::AgentStop { id }
                | ApiRequest::AgentInterrupt { id }
                | ApiRequest::AgentKill { id }
                | ApiRequest::AgentRelease { id }
                | ApiRequest::AgentRestart { id }
                | ApiRequest::AgentResume { id }
                | ApiRequest::AgentReplan { id, .. } => id,
                _ => return Err("unsupported control request; not sent".into()),
            };
            match request(&req, Duration::from_secs(10))? {
                ApiResponse::Agent { info } if &info.id == id => Ok(format!("{:?}", info.state)),
                _ => Err("mismatched control response; outcome unknown; not retried".into()),
            }
        }
        Command::Attention(_) => unreachable!(),
    })())
}

fn tachyon_cli_command() -> std::process::Command {
    if let Ok(path) = std::env::var("TACHYON_CLI_BIN") {
        return std::process::Command::new(path);
    }
    if let Some(path) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|parent| parent.join("tachyon")))
        .filter(|path| path.is_file())
    {
        return std::process::Command::new(path);
    }
    std::process::Command::new("tachyon")
}

#[cfg(test)]
mod tests;
