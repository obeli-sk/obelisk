use std::path::PathBuf;

use clap::Parser;
use concepts::FunctionFqn;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
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
    Archive {
        #[arg(short, long)]
        component_id: String, // TODO: ComponentId,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Exe {
    Schedule {
        #[arg(short, long)]
        ffqn: FunctionFqn,
        #[arg(short, long)]
        params: Option<String>,
        // interactive
        // when: String,
        #[arg(short, long)]
        verbose: bool,
    },
    List,
    Status {
        execution_id: String,
    },
}
