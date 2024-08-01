use clap::Parser;
use concepts::{ComponentConfigHash, ExecutionId, FunctionFqn};
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
    Executor(Executor),
    #[command(subcommand)]
    Component(Component),
    #[command(subcommand)]
    Execution(Execution),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Executor {
    Serve {
        /// Clean sqlite database and wasm cache
        #[arg(short, long)]
        clean: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Component {
    /// Parse WASM file and output its metadata.
    Inspect {
        #[arg(required(true))]
        path: PathBuf,
        #[arg(short, long)]
        verbose: bool,
    },
    /// List components.
    List {
        // Show disabled components
        #[arg(short, long)]
        disabled: bool,
        /// Enable full verbosity with `-vv`
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
    /// Get metadata of a stored component.
    Get {
        /// Component id consisting of a prefix and a hash
        #[arg()]
        config_id: ComponentConfigHash,
        /// Enable full verbosity with `-vv`
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Execution {
    Schedule {
        /// Follow the stream of events until the execution finishes
        #[arg(short, long)]
        follow: bool,
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
