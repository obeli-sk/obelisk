use clap::{ArgGroup, Parser};
use concepts::{ComponentType, ExecutionId, FunctionFqn, prefixed_ulid::ExecutionIdDerived};
use std::path::PathBuf;

pub(crate) mod shadow {
    pub(crate) const PKG_VERSION: &str = env!("PKG_VERSION");
}

#[derive(Parser, Debug)]
#[clap(name = "obelisk")]
#[command
(
    version = const_format::formatcp!("{}", shadow::PKG_VERSION),
about = "Obelisk: deterministic workflow engine", disable_version_flag = true, disable_help_subcommand = true)]
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
    #[command(subcommand)]
    Generate(Generate),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Generate {
    /// Generate the Obelisk configuration schema in JSON schema format.
    ConfigSchema {
        /// Filename to write the schema to, defaults to `<stdout>`.
        output: Option<PathBuf>,
    },
    /// Generate extension WIT files that are automatically implemented by Obelisk
    /// based on the exported interfaces of the component.
    ExportedExtWits {
        #[arg(long, short)]
        /// One of `workflow`, `activity_wasm`, `activity_stub`, `webhook_endpoint`
        component_type: ComponentType,
        input_wit_directory: PathBuf,
        output_deps_directory: Option<PathBuf>,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Server {
    Run {
        /// Clean the sqlite database directory
        #[arg(long)]
        clean_sqlite_directory: bool,
        /// Clean the codegen and OCI cache directories
        #[arg(long)]
        clean_cache: bool,
        /// Clean the codegen cache directory
        #[arg(long)]
        clean_codegen_cache: bool,
        #[arg(long, short)]
        config: Option<PathBuf>,
    },
    GenerateConfig,
    /// Read the configuration, compile the components, verify their imports and exit
    Verify {
        /// Clean the codegen and OCI cache directories
        #[arg(long)]
        clean_cache: bool,
        /// Clean the codegen cache
        #[arg(long)]
        clean_codegen_cache: bool,
        #[arg(long, short)]
        config: Option<PathBuf>,
        #[arg(long, short)]
        ignore_missing_env_vars: bool,
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
        #[arg(required(true))]
        component_type: ComponentType,
        #[arg(short, long)]
        imports: bool,
        #[arg(short, long)]
        extensions: bool,
        #[arg(short, long)]
        convert_core_module: bool,
    },
    /// List components.
    List {
        #[arg(short, long)]
        imports: bool,
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
    /// Submit new execution and optionally follow its status stream until the it finishes.
    Submit {
        /// Function in the fully qualified format
        #[arg(value_name = "FUNCTION")]
        ffqn: FunctionFqn,
        /// Parameters encoded as an JSON
        #[arg(value_name = "PARAMS")]
        params: String,
        /// Follow the stream of events until the execution finishes
        #[arg(short, long)]
        follow: bool,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long, requires = "follow", conflicts_with = "json")]
        no_reconnect: bool,
        /// Print output as JSON
        #[arg(long)]
        json: bool,
    },
    Stub(Stub),
    Get {
        /// Follow the status stream until the execution finishes.
        #[arg(short, long)]
        follow: bool,
        execution_id: ExecutionId,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long, requires = "follow")]
        no_reconnect: bool,
    },
    GetJson {
        /// Follow the stream of events until the execution finishes.
        #[arg(short, long)]
        follow: bool,
        execution_id: ExecutionId,
    },
}

#[derive(Debug, clap::Args)]
#[command(group(
    ArgGroup::new("result")
        .args(&["return_value", "empty", "error"])
        .required(true)
        .multiple(false)
))]
pub(crate) struct Stub {
    /// Execution ID of the stub execution waiting for its return value.
    #[arg(value_name = "EXECUTION_ID")]
    pub(crate) execution_id: ExecutionIdDerived,

    /// Stub a return value encoded as JSON
    #[arg(long)]
    pub(crate) return_value: Option<String>,

    /// Stub an empty return value
    #[arg(long)]
    pub(crate) empty: bool,

    /// Stub an error
    #[arg(long)]
    pub(crate) error: bool,
}
