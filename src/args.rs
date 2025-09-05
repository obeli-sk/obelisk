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
    Extensions {
        /// One of `workflow`, `activity_wasm`, `activity_stub`, `webhook_endpoint`
        component_type: ComponentType,
        /// Path to the `wit` folder, containing the target world and possibly `deps` subfolder.
        input_wit_directory: PathBuf,
        /// Optional. If not set, WIT files will be written to the input directory.
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
        /// Path to the TOML configuration
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
        /// Path to the TOML configuration
        #[arg(long, short)]
        config: Option<PathBuf>,
        /// Do not verify existence of environment variables
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
        /// One of `workflow`, `activity_wasm`, `activity_stub`, `webhook_endpoint`
        #[arg(required(true))]
        component_type: ComponentType,

        /// Path to the WASM file
        #[arg(required(true))]
        path: PathBuf,

        /// Show component imports
        #[arg(short, long)]
        imports: bool,

        /// Show auto-generated export extensions
        #[arg(short, long)]
        extensions: bool,

        /// Attempt to convert Core WASM Module to a WASM Component
        #[arg(short, long)]
        convert_core_module: bool,
    },
    /// List components.
    List {
        /// Show component imports
        #[arg(short, long)]
        imports: bool,
        /// Show auto-generated export extensions
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
        #[arg(value_name = "function")]
        ffqn: FunctionFqn,
        /// Follow the stream of events until the execution finishes
        #[arg(short, long)]
        follow: bool,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long, requires = "follow", conflicts_with = "json")]
        no_reconnect: bool,
        /// Print output as JSON
        #[arg(long)]
        json: bool, // TODO: output=json|jsonl|plain
        /// Parameters for the function. Accepts one of the following formats:
        ///
        /// - A single argument containing a JSON array string (e.g., '["a", "b"]')
        ///
        /// - A single argument prefixed with '@' referencing a file that contains the JSON array
        ///
        /// - Multiple individual arguments following '--' (e.g., -- "a" @secondparam.json null 1)
        #[expect(clippy::doc_link_with_quotes)] // Intentional use of '
        #[arg(name = "parameters")]
        params: Vec<String>,
    },
    /// Write a return value or an execution error to an already created stubbed execution.
    Stub(Stub),
    /// Get the current state of an execution.
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

pub(crate) mod params {
    use clap::error::ErrorKind;
    use serde_json::Value;

    pub(crate) fn parse_params(params: Vec<String>) -> Result<Vec<u8>, clap::Error> {
        if params.is_empty() {
            Ok("[]".to_string().into_bytes()) // no params, does not matter if `--` was present.
        } else if params.len() == 1 && !dashdash() {
            let mut params = params;
            let json_array = params.pop().expect("checked that len == 1");
            // Single JSON Array, or a `@`-prefixed file containing the array.
            let json_array = if let Some(file_path) = json_array.strip_prefix('@') {
                std::fs::read_to_string(file_path).map_err(|err| {
                    clap::Error::raw(
                        ErrorKind::Io,
                        format!(
                            "parameter parsing failed: failed to read file '{file_path}': {err}"
                        ),
                    )
                })?
            } else {
                json_array
            };
            let json_value = serde_json::from_str(&json_array).map_err(|err| {
                clap::Error::raw(
                    ErrorKind::ValueValidation,
                    format!("Invalid JSON array for parameters: {err}"),
                )
            })?;
            let Value::Array(_) = &json_value else {
                return Err(clap::Error::raw(
                    ErrorKind::ValueValidation,
                    "Parameter provided as JSON must be a JSON array.",
                ));
            };
            Ok(json_value.to_string().into_bytes())
        } else {
            // Fallback to raw arguments. Each argument is interpreted as a JSON value or a file starting with `@` that contains the JSON.
            let mut parsed_params: Vec<Value> = Vec::new();
            for (idx, arg) in params.into_iter().enumerate() {
                let arg = if let Some(file_path) = arg.strip_prefix('@') {
                    std::fs::read_to_string(file_path).map_err(|err| {
                        clap::Error::raw(
                            ErrorKind::Io,
                            format!(
                                "{idx}-th parameter parsing failed: failed to read file '{file_path}': {err}"
                            ),
                        )
                    })?
                } else {
                    arg
                };
                let json = serde_json::from_str(&arg).map_err(|err| {
                    clap::Error::raw(
                        ErrorKind::ValueValidation,
                        format!("{idx}-th parameter parsing failed: cannot parse as JSON: {err}"),
                    )
                })?;
                parsed_params.push(json);
            }
            Ok(Value::Array(parsed_params).to_string().into_bytes())
        }
    }

    fn dashdash() -> bool {
        // Ambigous: Either the single JSON Array representing all parameters,
        // OR `-- "first-and-only-json-param"`
        let mut rev_arg_iter = std::env::args().rev();
        rev_arg_iter.next().expect("last arg must be present");
        let maybe_separator = rev_arg_iter.next().expect("last-1 arg must be present");
        maybe_separator == "--"
    }
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
