#![forbid(unsafe_code)]

use clap::{Args, Parser, Subcommand};

/// Tachyon — a minimal runtime for autonomous agents.
///
/// Manages agents the way a service manager manages services. Agents run in
/// isolated sandboxes, execute tasks with bash, python, and a browser, and are
/// supervised by a persistent daemon.
///
/// Run `tachyon` with no command to open the interactive interface.
#[derive(Parser, Debug)]
#[command(name = "tachyon", version, about = "A minimal runtime for autonomous agents. Run `tachyon` with no command for the interactive interface.", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Create and start an agent for a task.
    ///
    /// The task is given to a harness which works autonomously using
    /// bash, python, and a browser. The agent runs in the background;
    /// use `tachyon logs <id>` or the TUI to follow its progress.
    #[command(alias = "new", alias = "create")]
    Start(StartArgs),

    /// List agents and their current state.
    ///
    /// Shows agent ids, states, tasks, and start times.
    #[command(alias = "ps", alias = "ls")]
    List(ListArgs),

    /// Show the status of all agents or a single agent.
    Status(StatusArgs),

    /// Show an agent's configuration and details.
    #[command(alias = "inspect")]
    Cat(CatArgs),

    /// Follow or show an agent's logs.
    Logs(LogsArgs),

    /// Gracefully stop an agent.
    Stop(IdArgs),

    /// Return the daemon-authoritative state of an agent.
    Await(IdArgs),

    /// Terminate and release an agent's process, task state, and workspace.
    Release(IdArgs),

    /// Replace an agent with a new durable objective.
    Replan(ReplanArgs),

    /// Force-stop an agent immediately.
    Kill(IdArgs),

    /// Interrupt an agent gracefully.
    Interrupt(IdArgs),

    /// Restart a stopped or failed agent.
    Restart(IdArgs),

    /// Resume an interrupted or stopped agent.
    Resume(IdArgs),

    /// Show live resource usage of running agents.
    Top(TopArgs),

    /// Run a command inside an agent's environment.
    Exec(ExecArgs),

    /// Attach to an agent's live output.
    Attach(IdArgs),

    /// Manage the Tachyon daemon.
    Daemon(DaemonArgs),

    /// Configure LLM providers and API keys.
    Providers(ProvidersArgs),
}

#[derive(Args, Debug)]
pub struct StartArgs {
    /// The task for the agent to perform.
    #[arg(required = true)]
    pub task: String,

    /// Working directory / repository for the agent.
    #[arg(short, long)]
    pub cwd: Option<String>,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    /// Show more detail for each agent.
    #[arg(short, long)]
    pub long: bool,
}

#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Agent id; show all agents when omitted.
    pub id: Option<String>,
}

#[derive(Args, Debug)]
pub struct CatArgs {
    /// The agent id to inspect.
    pub id: String,
}

#[derive(Args, Debug)]
pub struct LogsArgs {
    /// Follow log output as it is written.
    #[arg(short, long)]
    pub follow: bool,

    /// Number of lines to show.
    #[arg(short, long, default_value_t = 50)]
    pub lines: i32,

    /// The agent id to read logs for.
    pub id: String,
}

#[derive(Args, Debug)]
pub struct IdArgs {
    /// The agent id.
    pub id: String,
}

#[derive(Args, Debug)]
pub struct ReplanArgs {
    /// The agent id.
    pub id: String,
    /// The replacement objective.
    pub task: String,
}

#[derive(Args, Debug)]
pub struct TopArgs {
    /// Refresh interval in seconds.
    #[arg(short, long, default_value_t = 1)]
    pub interval: u64,
}

#[derive(Args, Debug)]
pub struct ExecArgs {
    /// The agent id to run the command in.
    pub id: String,

    /// Command and arguments to run (use `--` to separate from flags).
    #[arg(required = true, trailing_var_arg = true)]
    pub command: Vec<String>,
}

#[derive(Args, Debug)]
pub struct AttachArgs {
    /// The agent id to attach to.
    pub id: String,
}

#[derive(Args, Debug)]
pub struct DaemonArgs {
    /// Action to perform (default: status).
    #[command(subcommand)]
    pub action: Option<DaemonAction>,
}

#[derive(Subcommand, Debug)]
pub enum DaemonAction {
    /// Show whether the daemon is running and healthy.
    Status(DaemonStatusArgs),
    /// Start the daemon in the background.
    Start,
    /// Stop the daemon gracefully.
    Stop,
    /// Restart the daemon.
    Restart,
}

#[derive(Args, Debug)]
pub struct DaemonStatusArgs {
    /// Poll until the daemon is ready (e.g. after a start).
    #[arg(short, long)]
    pub wait: bool,
}

#[derive(Args, Debug)]
pub struct ProvidersArgs {
    /// Action to perform (default: interactive menu).
    #[command(subcommand)]
    pub action: Option<ProviderAction>,
}

#[derive(Subcommand, Debug)]
pub enum ProviderAction {
    /// Show the configured provider and whether a credential is available.
    List,
    /// Set the active model for a provider.
    SetModel(SetModelArgs),
    /// Store an OpenRouter key in the operating system credential store.
    Login,
    /// Remove the OpenRouter key from the operating system credential store.
    Logout,
    /// Print a config value (e.g. `model`, `base_url`).
    Get(GetArgs),
}

#[derive(Args, Debug)]
pub struct SetModelArgs {
    /// Model name (e.g. ~deepseek/deepseek-v4-flash-latest).
    pub model: String,
}

#[derive(Args, Debug)]
pub struct GetArgs {
    /// Config key, e.g. `model` or `base_url`.
    pub key: String,
}
