use clap::Parser;
use concepts::ComponentId;

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
        replace: bool, // if there is an overlap between existing exports
        #[arg(required(true))]
        wasm_path: String, // TODO: PathBuf
                           // TODO: interactive configuration based on component type
    },
    List,
    Archive {
        #[arg(short, long)]
        component_id: String, // TODO: ComponentId,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Exe {
    // Execution creation:
    // interactive search for ffqn showing param types and result, file name
    // enter parameters one by one, TODO: parameter names
    // submit execution
    // TODO: typecheck by the CLI
    // Return ExecutionId, poll the status unless finishes or Ctrl-C
    Schedule {
        // interactive
        // when: String,
        // function: String,
        // params: Vec<String>,
    },
    List,
    Status {
        execution_id: String,
    },
}
