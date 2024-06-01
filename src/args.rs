use clap::Parser;
use concepts::FunctionFqn;
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
    Inspect {
        #[arg(required(true))]
        wasm_path: String, // TODO: PathBuf
        #[arg(short, long)]
        verbose: bool,
    },
    Add {
        #[arg(short, long)]
        /// Replace component(s) with overlapping exports
        replace: bool,
        #[arg(required(true))]
        wasm_path: PathBuf,
        // TODO: interactive configuration based on component type
    },
    List {
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
    },
    Get {
        #[arg(short, long)]
        /// Function in the fully qualified format
        ffqn: String,
        #[arg(short, long)]
        // Component id consisting of a prefix and a hash
        id: String,
    },
    Archive {
        #[arg(short, long)]
        component_id: String, // TODO: ComponentId,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Exe {
    Schedule {
        #[arg(short, long)]
        /// Function in the fully qualified format
        ffqn: FunctionFqn,
        #[arg(short, long)]
        /// Parameters encoded as an JSON array
        params: Option<String>,
        // TODO: interactive
        // TODO: when: String,
        // TODO: poll?
    },
    List,
    Status {
        execution_id: String,
    },
}
