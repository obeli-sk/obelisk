use clap::Parser;
use concepts::{ComponentId, ExecutionId, FunctionFqn};
use std::path::PathBuf;

mod shaddow {
    #![allow(clippy::needless_raw_string_hashes)]
    shadow_rs::shadow!(build);
}

#[derive(Parser, Debug)]
#[command(version = const_format::formatcp!("{} {}", shaddow::build::PKG_VERSION, shaddow::build::SHORT_COMMIT), about = "Obelisk: deterministic backend")]
pub(crate) struct Args {
    #[command(subcommand)]
    pub(crate) command: Subcommand,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Subcommand {
    #[command(subcommand)]
    Server(Server),
    #[command(subcommand)]
    Component(Component),
    #[command(subcommand)]
    Exe(Exe),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Server {
    Run {
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
        #[arg(short, long)]
        /// Replace component(s) with overlapping exports
        replace: bool,
        #[arg(required(true))]
        wasm_path: PathBuf,
        // TODO: interactive configuration based on component type
    },
    /// List active components.
    List {
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
    /// Get metadata of a stored component.
    Get {
        #[arg()]
        // Component id consisting of a prefix and a hash
        id: ComponentId,
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
    /// Delete the WASM from the blob store.
    Archive {
        #[arg(short, long)] // , value_parser = clap::value_parser!(ComponentId)
        component_id: ComponentId,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Exe {
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
    Status {
        execution_id: ExecutionId,
    },
}
