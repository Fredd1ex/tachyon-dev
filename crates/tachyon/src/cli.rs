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
    /// Submit to or attach to the foreground conversation. Requires a running daemon.
    Chat(ChatArgs),
    /// Explicit native local campaign control (NOT a sandbox).
    Campaign {
        #[command(subcommand)]
        action: CampaignAction,
    },
    /// Create and start an agent for a task.
    ///
    /// The task is given to a harness which works autonomously using
    /// bash, python, and a browser. The agent runs in the background;
    /// this command reports admission, not completion. Use `tachyon chat` or the
    /// TUI to follow the foreground conversation.
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

    /// Query canonical conversation history by Unix millisecond range.
    History(HistoryArgs),

    /// Manage Tachyon's curated memory store.
    Memory(MemoryArgs),

    /// Manage the Tachyon daemon.
    Daemon(DaemonArgs),

    /// Configure LLM providers and API keys.
    Providers(ProvidersArgs),
}

#[derive(Subcommand, Debug)]
pub enum CampaignAction {
    /// Archive an idle Campaign (or Research whose campaigns are all archived). Deletes nothing.
    Archive {
        id: String,
    },
    /// Restore retained metadata without restarting execution. Also accepts Research IDs.
    Restore {
        id: String,
    },
    /// Query the independent archive marker. Also accepts Research IDs.
    Retention {
        id: String,
    },
    /// Inspect the root's retained human acceptance request or receipt. Starts no jobs.
    Acceptance {
        id: String,
    },
    /// Explicit trusted same-user acceptance, not automated verification.
    Accept(AcceptanceArgs),
    /// Explicit trusted same-user rejection. Does not start a repair.
    Reject(AcceptanceArgs),
    /// List or answer durable Work questions through the same-user host API.
    Attention {
        #[command(subcommand)]
        action: AttentionAction,
    },
    /// Create inert research and campaign metadata; does not authorize execution.
    Create {
        title: String,
        objective: String,
    },
    /// Authorize a manifest read by this CLI. Requires a running daemon.
    Run {
        manifest: std::path::PathBuf,
        #[arg(long, required = true)]
        unisolated_development: bool,
    },
    Status {
        id: String,
    },
    /// Inspect retained staging and durable admissions without starting jobs.
    Inspect {
        id: String,
    },
    /// Read retained advisory assessments and attempt statuses; starts no model.
    Assessments {
        id: String,
    },
    /// Coalesce an explicit assessment request into an already enabled campaign.
    Assess {
        id: String,
        #[arg(long)]
        command_id: String,
        #[arg(long, required = true)]
        unisolated_development: bool,
    },
    /// Read root file versions for host inputs and operator approval. Starts no models.
    IntegrationSnapshot {
        id: String,
        #[arg(required = true)]
        paths: Vec<String>,
    },
    /// Apply one retained child patch bundle; retry the identical plan to recover.
    Integrate {
        id: String,
        plan: std::path::PathBuf,
        #[arg(long)]
        expected_state: String,
        #[arg(long, required = true)]
        confirm: bool,
    },
    /// Reapprove stored dynamic staging policy, never replay execution.
    Recover {
        id: String,
        #[arg(long, required = true)]
        unisolated_development: bool,
    },
    /// Apply independently obtained final billing/cleanup evidence, never replay.
    Reconcile {
        id: String,
        receipt: std::path::PathBuf,
        #[arg(long, required = true)]
        unisolated_development: bool,
        #[arg(long, required = true)]
        confirm_authoritative: bool,
    },
    Cancel {
        id: String,
    },
    /// Explicit fresh-process continuation from an exact retained checkpoint.
    Continue {
        id: String,
        request: std::path::PathBuf,
        #[arg(long, required = true)]
        unisolated_development: bool,
    },
    /// Continue only a durable ready verification state, never unknown execution.
    Resume {
        id: String,
        #[arg(long, required = true)]
        unisolated_development: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum AttentionAction {
    List {
        id: String,
        #[arg(long)]
        after: Option<String>,
        #[arg(long, default_value_t = 32)]
        limit: usize,
    },
    Answer {
        id: String,
        work_id: String,
        request_id: String,
        #[arg(long)]
        generation: u64,
        #[arg(long)]
        instruction_revision: u64,
        answer: String,
    },
}

#[derive(Args, Debug)]
pub struct AcceptanceArgs {
    pub id: String,
    #[arg(long)]
    pub candidate: String,
    #[arg(long)]
    pub candidate_sha256: String,
    #[arg(long)]
    pub expected_state: String,
    #[arg(long)]
    pub command_id: String,
    #[arg(long, required = true)]
    pub confirm: bool,
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
pub struct ChatArgs {
    /// Text to submit; omit to attach without submitting.
    pub text: Option<String>,
    /// Working directory for a new command.
    #[arg(short, long, requires = "text")]
    pub cwd: Option<String>,
    /// Stream manager frames after admission (attach always streams).
    #[arg(short, long)]
    pub follow: bool,
    /// Emit newline-delimited JSON frames and receipts instead of canonical text.
    #[arg(long)]
    pub json: bool,
    /// Recover an identical command's receipt; requires its original session ID.
    #[arg(long, requires_all = ["session_id", "text"])]
    pub command_id: Option<String>,
    /// Original host session, only for explicit receipt reconciliation.
    #[arg(long, requires = "command_id")]
    pub session_id: Option<String>,
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
    /// Read the retained terminal WorkResult. Requires a running daemon.
    #[arg(long)]
    pub result: bool,
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
pub struct HistoryArgs {
    /// Inclusive Unix timestamp in milliseconds.
    #[arg(long)]
    pub since_ms: u64,

    /// Exclusive Unix timestamp in milliseconds.
    #[arg(long)]
    pub until_ms: u64,

    /// Maximum records to return.
    #[arg(short, long, default_value_t = 100)]
    pub limit: u32,
}

#[derive(Args, Debug)]
pub struct MemoryArgs {
    #[command(subcommand)]
    pub action: MemoryAction,
}

#[derive(Subcommand, Debug)]
pub enum MemoryAction {
    /// Permanently wipe all curated memories.
    Wipe,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_attach_and_reconciliation_flags_are_explicit() {
        for args in [
            vec!["tachyon", "chat"],
            vec!["tachyon", "chat", "hello", "--follow", "--json"],
            vec![
                "tachyon",
                "chat",
                "hello",
                "--session-id",
                "host",
                "--command-id",
                "same",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_ok());
        }
        for args in [
            vec!["tachyon", "chat", "--cwd", "/tmp"],
            vec!["tachyon", "chat", "hello", "--command-id", "same"],
            vec![
                "tachyon",
                "chat",
                "--session-id",
                "host",
                "--command-id",
                "same",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn integration_cli_requires_confirmation_state_and_has_no_root_override() {
        let args = [
            "tachyon",
            "campaign",
            "integrate",
            "campaign-test",
            "plan.json",
            "--expected-state",
            "state",
            "--confirm",
        ];
        assert!(Cli::try_parse_from(args).is_ok());
        assert!(Cli::try_parse_from(&args[..7]).is_err());
        let mut missing_state = args.to_vec();
        missing_state.drain(5..7);
        assert!(Cli::try_parse_from(missing_state).is_err());
        let mut override_root = args.to_vec();
        override_root.extend(["--workspace", "/tmp"]);
        assert!(Cli::try_parse_from(override_root).is_err());
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "integration-snapshot",
            "campaign-test",
            "src/a",
            "src/b"
        ])
        .is_ok());
    }

    #[test]
    fn human_acceptance_cli_requires_exact_version_state_command_and_confirmation() {
        use tachyon_api::{campaign::HumanDecision, types::ApiRequest};
        for verb in ["accept", "reject"] {
            let hash = "a".repeat(64);
            let args = [
                "tachyon",
                "campaign",
                verb,
                "campaign-test",
                "--candidate",
                "artifact-1",
                "--candidate-sha256",
                &hash,
                "--expected-state",
                &hash,
                "--command-id",
                "operator-1",
                "--confirm",
            ];
            for missing in [4, 6, 8, 10, 12] {
                let mut invalid = args.to_vec();
                invalid.drain(missing..(missing + if missing == 12 { 1 } else { 2 }));
                assert!(Cli::try_parse_from(invalid).is_err());
            }
            let cli = Cli::try_parse_from(args).unwrap();
            let Some(Command::Campaign { action }) = cli.command else {
                panic!()
            };
            let (args, decision) = match action {
                CampaignAction::Accept(args) => (args, HumanDecision::Accept),
                CampaignAction::Reject(args) => (args, HumanDecision::Reject),
                _ => panic!(),
            };
            let ApiRequest::CampaignAcceptanceDecide(request) = args.request(decision).unwrap()
            else {
                panic!()
            };
            assert_eq!(request.decision, decision);
            assert!(request.confirm);
            assert_eq!(request.expected_state_sha256, hash);
            assert_eq!(request.candidate, "artifact-1");
        }
        assert!(
            Cli::try_parse_from(["tachyon", "campaign", "acceptance", "campaign-test"]).is_ok()
        );
    }

    #[test]
    fn retention_commands_are_explicit_and_have_no_purge() {
        for action in ["archive", "restore", "retention"] {
            assert!(Cli::try_parse_from(["tachyon", "campaign", action]).is_err());
            assert!(Cli::try_parse_from(["tachyon", "campaign", action, "campaign-id"]).is_ok());
            assert!(Cli::try_parse_from(["tachyon", "campaign", action, "research-id"]).is_ok());
        }
        assert!(
            Cli::try_parse_from(["tachyon", "campaign", "purge", "campaign-id", "--confirm"])
                .is_err()
        );
    }

    #[test]
    fn attention_commands_require_exact_answer_identity() {
        assert!(
            Cli::try_parse_from(["tachyon", "campaign", "attention", "list", "campaign"]).is_ok()
        );
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "attention",
            "answer",
            "campaign",
            "work",
            "question",
            "2"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "attention",
            "answer",
            "campaign",
            "work",
            "question",
            "--generation",
            "1",
            "--instruction-revision",
            "3",
            "2"
        ])
        .is_ok());
    }

    #[test]
    fn campaign_requires_explicit_native_execution_flag() {
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "continue",
            "campaign-id",
            "request.json"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "continue",
            "campaign-id",
            "request.json",
            "--unisolated-development"
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "continue",
            "campaign-id",
            "request.json",
            "--unisolated-development",
            "--budget",
            "100"
        ])
        .is_err());
        for flags in [
            vec![],
            vec!["--unisolated-development"],
            vec!["--confirm-authoritative"],
        ] {
            let mut args = vec![
                "tachyon",
                "campaign",
                "reconcile",
                "campaign-id",
                "receipt.json",
            ];
            args.extend(flags);
            assert!(Cli::try_parse_from(args).is_err());
        }
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "reconcile",
            "campaign-id",
            "receipt.json",
            "--unisolated-development",
            "--confirm-authoritative"
        ])
        .is_ok());
        assert!(Cli::try_parse_from(["tachyon", "campaign", "run", "missing.json"]).is_err());
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "run",
            "missing.json",
            "--unisolated-development"
        ])
        .is_ok());
        assert!(Cli::try_parse_from(["tachyon", "campaign", "status", "campaign-id"]).is_ok());
        assert!(Cli::try_parse_from(["tachyon", "campaign", "cancel", "campaign-id"]).is_ok());
        assert!(Cli::try_parse_from(["tachyon", "campaign", "inspect", "campaign-id"]).is_ok());
        assert!(Cli::try_parse_from(["tachyon", "campaign", "recover", "campaign-id"]).is_err());
        assert!(Cli::try_parse_from([
            "tachyon",
            "campaign",
            "recover",
            "campaign-id",
            "--unisolated-development"
        ])
        .is_ok());
    }

    #[test]
    fn memory_wipe_is_a_complete_command_without_confirmation_flags() {
        let cli = Cli::try_parse_from(["tachyon", "memory", "wipe"]).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Memory(MemoryArgs {
                action: MemoryAction::Wipe
            }))
        ));
    }
}
