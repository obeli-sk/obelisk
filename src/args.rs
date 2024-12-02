use clap::Parser;
use concepts::{ExecutionId, FunctionFqn};
use std::path::PathBuf;

mod shadow {
    #![allow(clippy::needless_raw_string_hashes)]
    shadow_rs::shadow!(build);
}

#[derive(Parser, Debug)]
#[clap(name = "obelisk")]
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
    Server(Server),
    #[command()]
    Client(Client),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Server {
    Run {
        /// Clean the sqlite database
        #[arg(long)]
        clean_db: bool,
        /// Clean the codegen and OCI cache directories
        #[arg(long)]
        clean_cache: bool,
        /// Clean the codegen cache
        #[arg(long)]
        clean_codegen_cache: bool,
        #[arg(long, short)]
        config: Option<PathBuf>,
    },
    GenerateConfig,
    /// Read the configuration, compile the components, verify their imports and exit
    Verify {
        /// Clean the sqlite database
        #[arg(long)]
        clean_db: bool,
        /// Clean the codegen and OCI cache directories
        #[arg(long)]
        clean_cache: bool,
        /// Clean the codegen cache
        #[arg(long)]
        clean_codegen_cache: bool,
        #[arg(long, short)]
        config: Option<PathBuf>,
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
        #[arg(short, long)]
        extensions: bool,
        #[arg(short, long)]
        convert_core_module: bool,
    },
    /// List components.
    List {
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbosity: u8,
        #[arg(short, long)]
        extensions: bool,
    },
    /// Push a WASM file to an OCI registry.
    Push {
        /// WASM file to be pushed
        #[arg(required(true))]
        path: PathBuf,
        /// OCI reference. Example: docker.io/repo/image:tag
        #[arg(required(true))]
        image_name: oci_client::Reference,
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
