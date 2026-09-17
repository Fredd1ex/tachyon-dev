#![forbid(unsafe_code)]

//! Tachyon's user-facing Conversation runtime.

mod checkpoints;
mod delegation;
mod execution;
mod input;
mod intake;
mod model;
mod runtime;
mod scheduling;
mod streaming;
mod tools;
mod turns;

use std::path::PathBuf;
use std::process::ExitCode;

use intake::run_chat;
use runtime::AgentRole;
use streaming::init_event_context;

fn main() -> ExitCode {
    if let Some(code) = tachyon_util::guard::guard_or_exit_code() {
        return ExitCode::from(code as u8);
    }
    let (cwd, agent_id, new_session) = parse_args();
    let role = AgentRole::Conversation;
    init_event_context(role, agent_id.as_deref());

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("tachyon-foreground: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async move {
        // The agent's workspace is its prison: all tool output lands here. If
        // the daemon gave us an explicit cwd (workspace), honour it; otherwise
        // fall back to a per-id workspace under ~/.local/share/tachyon.
        let workspace = match &cwd {
            Some(c) => PathBuf::from(c),
            None => {
                let id = agent_id.clone().unwrap_or_else(|| "anon".into());
                tachyon_util::daemon::workspaces_dir().join(id)
            }
        };
        println!("[foreground] workspace: {}", workspace.display());
        run_chat(role, &workspace, new_session, agent_id).await
    })
}

fn parse_args() -> (Option<String>, Option<String>, bool) {
    let args: Vec<String> = std::env::args().collect();
    let mut cwd: Option<String> = None;
    let mut agent_id: Option<String> = None;
    let mut new_session = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            // Accepted for compatibility with the pre-split daemon command.
            "--chat" => i += 1,
            "--cwd" => {
                if i + 1 < args.len() {
                    cwd = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--agent-id" => {
                if i + 1 < args.len() {
                    agent_id = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--role" => i += usize::from(i + 1 < args.len()) + 1,
            "--new-session" => {
                new_session = true;
                i += 1;
            }
            _ => i += 1,
        }
    }
    (cwd, agent_id, new_session)
}
