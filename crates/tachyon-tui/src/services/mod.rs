//! Background status polling. UI requests never perform socket I/O or wait.
use super::TuiEvent;
use std::{io, sync::mpsc};
use tachyon_api::types::{AgentInfo, DaemonInfo, ScheduledTaskInfo};
use tachyon_client::Client;

pub(super) mod control;
mod event_buffer;
pub(super) mod history;
pub(super) mod subscriptions;

pub(super) struct Snapshot {
    pub(super) daemon: Option<DaemonInfo>,
    pub(super) agents: Vec<AgentInfo>,
    pub(super) schedules: Vec<ScheduledTaskInfo>,
}

pub(super) fn status_worker(out: mpsc::Sender<TuiEvent>) -> io::Result<mpsc::SyncSender<()>> {
    let (request, rx) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("tui-status".into())
        .spawn(move || {
            while rx.recv().is_ok() {
                let mut snapshot = Snapshot {
                    daemon: None,
                    agents: Vec::new(),
                    schedules: Vec::new(),
                };
                if let Ok(mut client) = Client::connect() {
                    snapshot.daemon = client.daemon_status().ok();
                    snapshot.agents = client.agent_list().unwrap_or_default();
                    snapshot.schedules = client.scheduled_task_list().unwrap_or_default();
                }
                if out.send(TuiEvent::Status(snapshot)).is_err() {
                    break;
                }
            }
        })?;
    Ok(request)
}
