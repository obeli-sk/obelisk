use clap::Parser;

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
    Worker(Worker),
    #[command(subcommand)]
    Function(Function),
    #[command(subcommand)]
    Exe(Exe),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Server {
    Run {
        #[arg(short, long)]
        clean: bool,
    },
    Shutdown {
        #[arg(short, long)]
        ungraceful: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Worker {
    Inspect {
        #[arg(required(true))]
        wasm_path: String, // TODO: PathBuf
        #[arg(short, long)]
        verbose: bool,
    },
    Add {
        #[arg(short, long)]
        replace: bool,
        #[arg(required(true))]
        wasm_path: String, // TODO: PathBuf
    },
    List,
    Remove {
        #[arg(num_args(1..), required(true))]
        hashes: Vec<String>,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Function {
    List,
    Validate {
        function: String,
        params: Vec<String>,
    },
    Schedule {
        when: String,
        function: String,
        params: Vec<String>,
    },
    Run {
        function: String,
        params: Vec<String>,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Exe {
    List,
    Status { execution_id: String },
}
