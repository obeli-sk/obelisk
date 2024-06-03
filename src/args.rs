use clap::Parser;
use concepts::{ComponentId, ExecutionId, FunctionFqn};
use std::path::PathBuf;

mod shaddow {
    #![allow(clippy::needless_raw_string_hashes)]
    shadow_rs::shadow!(build);
}

#[derive(Parser, Debug)]
#[command(version = const_format::formatcp!("{} {}", shaddow::build::PKG_VERSION, shaddow::build::SHORT_COMMIT),
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
        #[arg(short, long)]
        clean: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Component {
    /// Parse WASM file and output its metadata.
    Inspect {
        #[arg(required(true))]
        wasm_path: PathBuf,
        #[arg(short, long)]
        verbose: bool,
    },
    /// Load WASM file into the blob store and populate database.
    Add {
        /// Replace component(s) with overlapping exports
        #[arg(short, long)]
        replace: bool,
        #[arg(required(true))]
        wasm_path: PathBuf,
        // TODO: interactive configuration based on component type
    },
    /// List active components.
    List {
        /// Enable full verbosity with `-vv`
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
    /// Get metadata of a stored component.
    Get {
        /// Component id consisting of a prefix and a hash
        #[arg()]
        id: ComponentId,
        /// Enable full verbosity with `-vv`
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
    /// Delete the WASM from the blob store.
    Archive {
        /// Component id consisting of a prefix and a hash
        #[arg()]
        component_id: ComponentId,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Execution {
    Schedule {
        /// Force execution creation without name or parameter validation
        #[arg(long)]
        force: bool,
        /// Function in the fully qualified format
        #[arg(value_name = "FUNCTION")]
        ffqn: FunctionFqn,
        /// Parameters encoded as an JSON array
        #[arg(value_name = "PARAMETERS")]
        params: Vec<String>,
        // TODO: interactive
        // TODO: when: String,
        // TODO: poll?
    },
    Get {
        execution_id: ExecutionId,
        /// Enable full verbosity with `-vv`
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
}
