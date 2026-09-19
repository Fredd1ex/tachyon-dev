//! Resolve explicit commands against current selection before asynchronous execution.
use super::services::control::{Command, Worker};
use super::{find_or_create_thread, pane_agent_ids, ItemKind, Thread};
use std::collections::HashMap;
use tachyon_api::{
    types::{AgentInfo, ApiRequest},
    FOREGROUND_ID,
};

pub(super) fn foreground_workspace_request(
    text: String,
    cwd: std::io::Result<std::path::PathBuf>,
) -> Result<(String, Option<String>), String> {
    if let Some(text) = text.strip_prefix("/managed ") {
        return Ok((text.trim().to_string(), None));
    }
    let cwd = cwd.map_err(|error| format!("cannot select current workspace: {error}"))?;
    let cwd = cwd
        .into_os_string()
        .into_string()
        .map_err(|_| "current workspace path is not UTF-8".to_string())?;
    Ok((text, Some(cwd)))
}

fn agent(verb: &str, id: String) -> ApiRequest {
    match verb {
        "await" => ApiRequest::AgentAwait { id },
        "stop" => ApiRequest::AgentStop { id },
        "interrupt" => ApiRequest::AgentInterrupt { id },
        "kill" => ApiRequest::AgentKill { id },
        "release" => ApiRequest::AgentRelease { id },
        "restart" => ApiRequest::AgentRestart { id },
        "resume" => ApiRequest::AgentResume { id },
        _ => unreachable!("validated control verb"),
    }
}

pub(super) fn submit_chat(
    cmd: &str,
    threads: &mut Vec<Thread>,
    worker: &mut Worker,
) -> Result<u64, String> {
    let id = worker.submit("foreground intake".into(), Command::Chat(cmd.into()))?;
    let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
    threads[idx].add(
        ItemKind::User,
        cmd.strip_prefix("/managed ").unwrap_or(cmd).trim().into(),
    );
    threads[idx].reserve_reply();
    Ok(id)
}

pub(super) fn handle_slash(
    cmd: &str,
    threads: &mut Vec<Thread>,
    worker: &mut Worker,
) -> Result<(), String> {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let request = match parts.as_slice() {
        ["replan", id, task @ ..] if !task.is_empty() => Some(ApiRequest::AgentReplan {
            id: (*id).into(),
            task: task.join(" "),
        }),
        [verb @ ("await" | "stop" | "interrupt" | "kill" | "release" | "restart" | "resume"), id] => {
            Some(agent(verb, (*id).into()))
        }
        _ => None,
    };
    if let Some(request) = request {
        worker.submit(cmd.into(), Command::Agent(request))?;
        return Ok(());
    }
    let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
    if matches!(parts.as_slice(), ["help"] | []) {
        for line in [
            "/clear       toggle previous visits (keeps history)",
            "/managed TEXT  run this turn in managed agent workspaces",
            "/stop <id>  stop an agent",
            "/await <id>  show current agent state",
            "/interrupt <id>  interrupt an agent",
            "/kill <id>  kill an agent",
            "/release <id>  terminate and clean an agent",
            "/replan <id> <task>  replace an agent objective",
            "/restart <id>  restart an agent",
            "/resume <id>  resume an agent",
            "/exit       quit Tachyon",
            "/attention [conversation|campaign|work <id>]  list attention; /ack <id>  acknowledge",
        ] {
            threads[idx].add(ItemKind::System, line.into());
        }
    } else {
        threads[idx].add(ItemKind::Error, format!("unknown slash command /{cmd}"));
    }
    Ok(())
}

pub(super) fn pane_control(
    verb: &str,
    focus: usize,
    agent_infos: &HashMap<String, AgentInfo>,
    threads: &mut Vec<Thread>,
    worker: &mut Worker,
) -> Result<(), String> {
    if focus == 0 {
        return match verb {
            "stop" | "kill" => daemon_control("stop", worker),
            "restart" => daemon_control("restart", worker),
            _ => Ok(()),
        };
    }
    if focus == 1 {
        let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
        threads[idx].add(
            ItemKind::System,
            "can't manage the foreground from the pane".into(),
        );
    } else if let Some(id) = pane_agent_ids(agent_infos).get(focus - 2).cloned() {
        worker.submit(
            format!("pane: {verb} {id}"),
            Command::Agent(agent(verb, id)),
        )?;
    }
    Ok(())
}

pub(super) fn daemon_control(action: &str, worker: &mut Worker) -> Result<(), String> {
    worker.submit(format!("daemon {action}"), Command::Daemon(action.into()))?;
    Ok(())
}
