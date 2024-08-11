use clap::Parser;
use concepts::{ExecutionId, FunctionFqn};
use std::path::PathBuf;

mod shadow {
    #![allow(clippy::needless_raw_string_hashes)]
    shadow_rs::shadow!(build);
}

#[derive(Parser, Debug)]
#[command(version = const_format::formatcp!("{} {}", shadow::build::PKG_VERSION, shadow::build::SHORT_COMMIT),
about = "Obelisk: deterministic backend", disable_version_flag = true, disable_help_subcommand = true)]
pub(crate) struct Args {
    #[command(subcommand)]
    pub(crate) command: Subcommand,

    /// Print version
    #[arg(short, long, action = clap::ArgAction::Version)]
    version: Option<bool>,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Subcommand {
    #[command(subcommand)]
    Daemon(Daemon),
    #[command()]
    Client(Client),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Daemon {
    Serve {
        /// Clean sqlite database and wasm cache
        #[arg(short, long)]
        clean: bool,
        #[arg(short, long)]
        machine_readable_logs: bool,
    },
}

#[derive(Debug, clap::Args)]
pub(crate) struct Client {
    /// Address of the obelisk server
    #[arg(short, long, default_value = "http://127.0.0.1:5005")]
    pub(crate) api_url: String,
    #[command(subcommand)]
    pub(crate) command: ClientSubcommand,
}

#[derive(Debug, clap::Subcommand)]

pub(crate) enum ClientSubcommand {
    #[command(subcommand)]
    Component(Component),
    #[command(subcommand)]
    Execution(Execution),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Component {
    /// Parse WASM file and output its metadata.
    Inspect {
        #[arg(required(true))]
        path: PathBuf,
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
    /// List components.
    List {
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Execution {
    Submit {
        /// Follow the stream of events until the execution finishes
        #[arg(short, long)]
        follow: bool,
        /// Enable full verbosity with `-vv`
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
        /// Function in the fully qualified format
        #[arg(value_name = "FUNCTION")]
        ffqn: FunctionFqn,
        /// Parameters encoded as an JSON array
        #[arg(value_name = "PARAMS")]
        params: String,
        // TODO: interactive
        // TODO: when: String,
        // TODO: poll?
    },
    Get {
        /// Follow the stream of events until the execution finishes
        #[arg(short, long)]
        follow: bool,
        execution_id: ExecutionId,
        /// Enable full verbosity with `-vv`
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
}
