#![forbid(unsafe_code)]

use std::process::ExitCode;

use clap::Parser;
use tachyon::cli::{Cli, Command, DaemonAction, MemoryAction, ProviderAction};
use tachyon::style::{colored_glyph, palette, render};

use tachyon_api::types::{AgentState, ApiRequest, ApiResponse};
use tachyon_api::FOREGROUND_ID;
use tachyon_util::guard;

fn main() -> ExitCode {
    if let Some(code) = guard::guard_or_exit_code() {
        return ExitCode::from(code as u8);
    }

    let cli = Cli::parse();
    let p = palette();

    // Bare `tachyon` → interactive interface.
    if cli.command.is_none() {
        tachyon::daemon::ensure_running();
        daemon_key_check(&p);
        if let Err(e) = tachyon_tui::run() {
            eprintln!("tachyon: {e}");
            return ExitCode::FAILURE;
        }
        return ExitCode::SUCCESS;
    }

    let cmd = cli.command.expect("handled above");
    if let Command::Campaign { action } = cmd {
        return tachyon::campaign::run(action);
    }

    // Only agent commands auto-start the daemon. Local administration and
    // provider commands must not spawn it implicitly.
    if needs_ipc(&cmd) {
        tachyon::daemon::ensure_running();
        daemon_key_check(&p);
    }

    dispatch(cmd, &p)
}

/// True if a command needs an IPC connection to the daemon.
fn needs_ipc(cmd: &Command) -> bool {
    !matches!(
        cmd,
        Command::Daemon(_) | Command::Memory(_) | Command::Providers(_)
    ) && !matches!(cmd, Command::Restart(args) if args.id == "daemon")
}

/// All CLI commands are thin mirrors of `ApiRequest`s (one-to-one mapping).
fn dispatch(cmd: Command, p: &tachyon::style::Palette) -> ExitCode {
    // Daemon lifecycle and provider config don't need IPC for all actions.
    if !needs_ipc(&cmd) {
        match &cmd {
            // Accept the natural word order as an alias for `daemon restart`.
            Command::Restart(args) if args.id == "daemon" => return daemon_restart(p),
            Command::Daemon(args) => match &args.action {
                None => {}
                Some(DaemonAction::Status(args)) => {
                    // If the daemon isn't running, report that instead of a
                    // raw socket error.
                    if !tachyon::daemon::status() {
                        println!(
                            "{} {}",
                            colored_glyph(false),
                            render(&p.bad, "Tachyon daemon is not running")
                        );
                        println!(
                            "  {}",
                            render(&p.dim, "start it with: tachyon daemon start")
                        );
                        return ExitCode::FAILURE;
                    }
                    let mut client = match tachyon_client::Client::connect() {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("tachyon: {e}");
                            return ExitCode::FAILURE;
                        }
                    };
                    if args.wait {
                        return daemon_wait(&mut client, p);
                    }
                    return daemon_status(&mut client, p);
                }
                Some(DaemonAction::Start) => return daemon_start(p),
                Some(DaemonAction::Stop) => return daemon_stop(p),
                Some(DaemonAction::Restart) => return daemon_restart(p),
            },
            Command::Memory(args) => return memory_cmd(args, p),
            Command::Providers(_) => return providers_cmd(cmd, p),
            _ => {}
        }
    }

    let mut client = match tachyon_client::Client::connect() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("tachyon: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Convert the CLI command to its ApiRequest twin, then run it.
    let req: ApiRequest = match cmd {
        Command::Start(args) => {
            // `start` goes through the foreground: send the task to its chat,
            // which may spawn worker agents as needed.
            return start_via_foreground(&mut client, args.task, args.cwd, p);
        }
        Command::List(_) => ApiRequest::AgentList,
        Command::Status(args) => ApiRequest::AgentStatus { id: args.id },
        Command::Cat(args) => ApiRequest::AgentCat { id: args.id },
        Command::Logs(args) => ApiRequest::AgentLogs {
            id: args.id,
            follow: args.follow,
            lines: args.lines as u32,
        },
        Command::Stop(args) => ApiRequest::AgentStop { id: args.id },
        Command::Await(args) => ApiRequest::AgentAwait { id: args.id },
        Command::Release(args) => ApiRequest::AgentRelease { id: args.id },
        Command::Replan(args) => ApiRequest::AgentReplan {
            id: args.id,
            task: args.task,
        },
        Command::Interrupt(args) => ApiRequest::AgentInterrupt { id: args.id },
        Command::Kill(args) => ApiRequest::AgentKill { id: args.id },
        Command::Restart(args) => ApiRequest::AgentRestart { id: args.id },
        Command::Resume(args) => ApiRequest::AgentResume { id: args.id },
        Command::Exec(args) => ApiRequest::AgentExec {
            id: args.id,
            command: args.command,
        },
        Command::Attach(args) => ApiRequest::AgentAttach { id: args.id },
        Command::History(args) => ApiRequest::HistoryQuery {
            since_ms: args.since_ms,
            until_ms: args.until_ms,
            limit: args.limit,
        },
        Command::Top(_) => ApiRequest::Top,
        Command::Campaign { .. }
        | Command::Daemon(_)
        | Command::Memory(_)
        | Command::Providers(_) => {
            unreachable!("handled above")
        }
    };

    let resp = match client.request(&req, std::time::Duration::from_secs(60)) {
        Ok(r) => r,
        Err(e) => {
            println!("{}", render(&p.bad, e.to_string()));
            return ExitCode::FAILURE;
        }
    };
    print_response(resp, p);
    ExitCode::SUCCESS
}

/// `tachyon start <task>` delegates to the foreground. Send the task as a chat
/// message, subscribe to its stream, and print foreground/worker
/// activity until a final answer appears.
fn start_via_foreground(
    client: &mut tachyon_client::Client,
    task: String,
    cwd: Option<String>,
    p: &tachyon::style::Palette,
) -> ExitCode {
    let cwd = match cwd.map(|path| std::fs::canonicalize(path)).transpose() {
        Ok(cwd) => cwd.map(|path| path.to_string_lossy().into_owned()),
        Err(error) => {
            println!("{} {}", render(&p.bad, "workspace unavailable:"), error);
            return ExitCode::FAILURE;
        }
    };
    let cwd_hint = match &cwd {
        Some(c) => format!(" (cwd: {c})"),
        None => String::new(),
    };
    println!(
        "{} {}",
        render(&p.accent, format!("foreground handling:{cwd_hint}")),
        render(&p.dim, &task)
    );

    if let Err(e) = client.foreground_chat_with_cwd(task, cwd) {
        println!("{} {}", render(&p.bad, "failed to reach foreground:"), e);
        return ExitCode::FAILURE;
    }

    // Open a stream to the foreground and print activity until we see a
    // final `[agent]` / `[daemon:result]` answer.
    let mut sub = match tachyon_client::Subscription::open(FOREGROUND_ID) {
        Ok(s) => s,
        Err(e) => {
            println!("{} {}", render(&p.bad, "failed to subscribe:"), e);
            return ExitCode::FAILURE;
        }
    };

    // Give the daemon a moment to relay; stream for up to ~5 min.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    let mut done = false;
    while !done && std::time::Instant::now() < deadline {
        match sub.next() {
            Some(ApiResponse::Event { data, .. }) => {
                let line = data.trim().to_string();
                if line.is_empty() {
                    continue;
                }
                // Suppress token-spray relay lines; show meaningful markers.
                if line.starts_with("[text]") {
                    continue;
                }
                if line.starts_with("[stream]") {
                    continue;
                }
                // The task the user typed is already printed; skip its echo.
                if line.starts_with("[user]") {
                    continue;
                }
                println!("  {}", render(&p.dim, &line));
                if line.starts_with("[agent] ")
                    || line.starts_with("[ghost] answer")
                    || line.contains("[daemon:result]")
                    || line.starts_with("[ghost:error]")
                {
                    done = true;
                }
            }
            Some(_) => {}
            None => {
                println!("{}", render(&p.warn, "foreground stream closed"));
                break;
            }
        }
    }
    if !done {
        println!(
            "{}",
            render(&p.warn, "timed out waiting for the foreground")
        );
    }
    ExitCode::SUCCESS
}

fn print_response(resp: ApiResponse, p: &tachyon::style::Palette) {
    use ApiResponse::*;
    match resp {
        Ok { message } => {
            if let Some(m) = message {
                println!("{}", render(&p.good, m));
            }
        }
        Agent { info } => print_agent(&info, p),
        Agents { agents } if agents.is_empty() => {
            println!("{} {}", colored_glyph(false), render(&p.dim, "No agents."));
        }
        Agents { agents } => {
            for a in agents {
                print_agent(&a, p);
            }
        }
        Logs { id, lines } => {
            if lines.is_empty() {
                println!("{} {}", render(&p.dim, "No logs yet for"), id);
            } else {
                for l in lines {
                    println!("{l}");
                }
            }
        }
        History { entries } => {
            for entry in entries {
                println!(
                    "{}\t{:?}\t{}\t{}",
                    entry.occurred_at_ms, entry.role, entry.conversation_id, entry.text
                );
            }
        }
        Exec {
            exit_code,
            stdout,
            stderr,
            ..
        } => {
            if !stdout.is_empty() {
                print!("{stdout}");
                if !stdout.ends_with('\n') {
                    println!();
                }
            }
            if !stderr.is_empty() {
                eprint!("{stderr}");
            }
            match exit_code {
                Some(c) => println!("{} {}", render(&p.dim, "exit code:"), c),
                None => println!("{} {}", render(&p.dim, "exit code:"), "(none)"),
            }
        }
        Attach { output, .. } => println!("{output}"),
        _ => {}
    }
}

fn print_agent(a: &tachyon_client::api::AgentInfo, p: &tachyon::style::Palette) {
    let glyph = match a.state {
        AgentState::Running | AgentState::Starting => colored_glyph(true),
        AgentState::Waiting => render(&p.idle, "○"),
        AgentState::Completed => render(&p.good, "●"),
        AgentState::Failed => render(&p.bad, "●"),
        AgentState::Interrupted | AgentState::Terminated => render(&p.warn, "●"),
        AgentState::Released => render(&p.warn, "●"),
        AgentState::Staged => render(&p.warn, "◐"),
        AgentState::Created => render(&p.idle, "○"),
    };
    let state = match a.state {
        AgentState::Running => render(&p.good, a.state.to_string()),
        AgentState::Failed => render(&p.bad, a.state.to_string()),
        AgentState::Interrupted | AgentState::Terminated | AgentState::Released => {
            render(&p.warn, a.state.to_string())
        }
        AgentState::Staged => render(&p.warn, a.state.to_string()),
        s => render(&p.idle, s.to_string()),
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let age = now.saturating_sub(a.created_secs);
    let deadline = a
        .stage_until_secs
        .or(a.lease_until_secs)
        .map(|until| format!("ttl {}s", until.saturating_sub(now)))
        .unwrap_or_else(|| "ttl -".into());
    println!(
        "{glyph} {}  {}  {}  {}  {}  {}  {}",
        render(&p.accent, &a.id),
        state,
        render(&p.dim, format!("{age}s")),
        render(&p.dim, &a.task_type),
        render(&p.dim, deadline),
        if a.persistent {
            "persistent"
        } else {
            "session"
        },
        a.task,
    );
}

fn daemon_status(client: &mut tachyon_client::Client, p: &tachyon::style::Palette) -> ExitCode {
    match client.daemon_status() {
        Ok(info) => {
            println!(
                "{} {} ({})",
                colored_glyph(true),
                render(&p.good, "Tachyon daemon"),
                render(&p.dim, format!("pid {}", info.pid))
            );
            println!("  {} {}", render(&p.dim, "version:"), info.version);
            println!("  {} {}", render(&p.dim, "proto:"), info.proto_version);
            println!(
                "  {} {}",
                render(&p.dim, "provider:"),
                if info.provider_ready {
                    render(&p.good, "ready")
                } else {
                    render(&p.warn, "not configured")
                }
            );
            println!("  {} {}", render(&p.dim, "socket:"), info.socket);
            ExitCode::SUCCESS
        }
        Err(e) => {
            println!(
                "{} {}",
                colored_glyph(false),
                render(&p.bad, "daemon is not running")
            );
            println!("  {}", render(&p.dim, e.to_string()));
            ExitCode::FAILURE
        }
    }
}

fn daemon_wait(client: &mut tachyon_client::Client, p: &tachyon::style::Palette) -> ExitCode {
    // Poll the socket until the daemon answers.
    for _ in 0..100 {
        if client.daemon_status().is_ok() {
            return daemon_status(client, p);
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    println!("{}", render(&p.bad, "daemon did not come up in time"));
    ExitCode::FAILURE
}

fn daemon_start(p: &tachyon::style::Palette) -> ExitCode {
    if tachyon::daemon::status() {
        println!(
            "{} {}",
            colored_glyph(true),
            render(&p.good, "already running")
        );
    } else if let Some(pid) = tachyon::daemon::start() {
        println!(
            "{} {}",
            colored_glyph(true),
            render(&p.good, format!("daemon started (pid {pid})"))
        );
    } else {
        println!("{}", render(&p.bad, "failed to start daemon"));
        return ExitCode::FAILURE;
    }
    daemon_key_check(p);
    ExitCode::SUCCESS
}

/// After starting (or connecting to) the daemon, verify it can see an API key.
/// A key present in the *shell* but missing from the *daemon's* environment is
/// the #1 cause of "model is not configured" in the TUI.
fn daemon_key_check(p: &tachyon::style::Palette) {
    let shell_has = std::env::var("OPENROUTER_API_KEY")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);
    let mut daemon_ready = false;
    // A freshly spawned daemon needs a moment to bind its socket and read its
    // environment. Avoid reporting a false missing-key warning during that window.
    for attempt in 0..20 {
        daemon_ready = match tachyon_client::Client::connect().and_then(|mut c| c.daemon_status()) {
            Ok(info) => info.provider_ready,
            Err(_) => false,
        };
        if daemon_ready || !shell_has {
            break;
        }
        if attempt < 19 {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
    if shell_has && !daemon_ready {
        eprintln!(
            "{}",
            render(
                &p.bad,
                "WARNING: OPENROUTER_API_KEY is set in this shell but the daemon does not see it.\n  The daemon captures its environment at launch. Run:\n    tachyon daemon restart   (in this same shell, after exporting the key)"
            )
        );
    }
}

fn daemon_stop(p: &tachyon::style::Palette) -> ExitCode {
    match tachyon::daemon::stop() {
        Ok(()) => println!("{}", render(&p.warn, "daemon stopped")),
        Err(e) => {
            println!("{} {}", render(&p.bad, "failed to stop daemon:"), e);
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

fn daemon_restart(p: &tachyon::style::Palette) -> ExitCode {
    match tachyon::daemon::restart() {
        Some(pid) => println!(
            "{}",
            render(&p.good, format!("daemon restarted (pid {pid})"))
        ),
        None => {
            println!("{}", render(&p.bad, "failed to restart daemon"));
            return ExitCode::FAILURE;
        }
    }
    daemon_key_check(p);
    ExitCode::SUCCESS
}

fn memory_cmd(args: &tachyon::cli::MemoryArgs, p: &tachyon::style::Palette) -> ExitCode {
    match &args.action {
        MemoryAction::Wipe => {
            let path = tachyon::data::memory_path();
            if tachyon::daemon::status() {
                println!(
                    "{}",
                    render(&p.bad, "cannot wipe memory while the daemon is running")
                );
                println!("  stop it first with: tachyon daemon stop");
                return ExitCode::FAILURE;
            }
            match tachyon::data::erase(std::slice::from_ref(&path)) {
                Ok(results) => {
                    for (path, deleted) in results {
                        let status = if deleted { "deleted" } else { "already absent" };
                        println!("{} {}", render(&p.good, status), path.display());
                    }
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    println!("{}", render(&p.bad, error));
                    ExitCode::FAILURE
                }
            }
        }
    }
}

fn providers_cmd(cmd: Command, p: &tachyon::style::Palette) -> ExitCode {
    let Command::Providers(args) = cmd else {
        return ExitCode::FAILURE;
    };
    // Provider config lives in the shared config file, read by all components.
    let cfg = tachyon::config::Config::load();
    match args.action {
        None => list_providers(&cfg, p),
        Some(action) => match action {
            ProviderAction::List => list_providers(&cfg, p),
            ProviderAction::SetModel(m) => {
                let mut c = cfg;
                c.model.name = Some(m.model);
                let mut prov = c.provider.clone().unwrap_or_default();
                prov.name = "openrouter".into();
                prov.base_url = Some("https://openrouter.ai/api/v1".into());
                c.provider = Some(prov);
                match c.save(&tachyon::config::Config::default_path()) {
                    Ok(()) => println!("{}", render(&p.good, "Model set.")),
                    Err(e) => {
                        println!("{} {e}", render(&p.bad, "failed to save:"));
                        return ExitCode::FAILURE;
                    }
                }
                ExitCode::SUCCESS
            }
            ProviderAction::Login => match tachyon::providers::login() {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    println!("{} {error}", render(&p.bad, "failed to store credential:"));
                    ExitCode::FAILURE
                }
            },
            ProviderAction::Logout => match tachyon::providers::logout() {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    println!("{} {error}", render(&p.bad, "failed to remove credential:"));
                    ExitCode::FAILURE
                }
            },
            ProviderAction::Get(g) => {
                let v = match g.key.as_str() {
                    "model" => Some(cfg.active_model()),
                    "base_url" => Some(cfg.provider_base_url()),
                    _ => None,
                };
                match v {
                    Some(v) => {
                        println!("{v}");
                        ExitCode::SUCCESS
                    }
                    None => {
                        println!("{} {}", render(&p.dim, "not set:"), g.key);
                        ExitCode::FAILURE
                    }
                }
            }
        },
    }
}

fn list_providers(_cfg: &tachyon::config::Config, _p: &tachyon::style::Palette) -> ExitCode {
    tachyon::providers::list();
    ExitCode::SUCCESS
}
