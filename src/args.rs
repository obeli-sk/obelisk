use crate::config::toml::ComponentLocationToml;
use clap::Parser;
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
    #[command(subcommand)]
    Execution(Execution),
    #[command(subcommand)]
    Component(Component),
    #[command(subcommand)]
    Generate(Generate),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Generate {
    /// Generate the Obelisk configuration schema in JSON schema format.
    #[cfg(debug_assertions)]
    ConfigSchema {
        /// Filename to write the schema to, defaults to <stdout>.
        output: Option<PathBuf>,
    },
    /// Generate extension WIT files that are automatically implemented by Obelisk
    /// based on the exported interfaces of the component.
    WitExtensions {
        #[arg(long, short)]
        force: bool,
        /// One of `workflow`, `activity_wasm`, `activity_stub`, `webhook_endpoint`
        component_type: ComponentType,
        /// Path to the `wit` folder, containing the target world and possibly `deps` subfolder.
        input_wit_directory: PathBuf,
        /// Directory where folders and WIT files will be written to.
        output_directory: PathBuf,
    },
    /// Generate Obelisk WIT files for given component type.
    WitSupport {
        /// One of `workflow`, `activity_wasm`, `activity_stub`, `webhook_endpoint`
        component_type: ComponentType,
        /// Directory where folders and WIT files will be written to.
        output_directory: PathBuf,
    },
    /// Generate WIT dependency folder based on activities and workflows found in provided TOML configuration.
    WitDeps {
        /// Path to the TOML configuration
        #[arg(long, short)]
        config: Option<PathBuf>,
        /// Directory where folders and WIT files will be written to.
        output_directory: PathBuf,
        /// Overwrite existing files.
        #[arg(long, short)]
        overwrite: bool,
    },
    Config {
        /// Filename to write the TOML to, defaults to <stdout>.
        output: Option<PathBuf>,
        /// Overwrite existing file.
        #[arg(long, short)]
        overwrite: bool,
    },
    ExecutionId,
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
        /// Ignore type checking errors
        #[arg(long, short)]
        suppress_type_checking_errors: bool,
    },
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
        /// Ignore type checking errors
        #[arg(long, short)]
        suppress_type_checking_errors: bool,
    },
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
        location: ComponentLocationToml,

        /// Show component imports
        #[arg(short, long)]
        imports: bool,

        /// Show auto-generated export extensions
        #[arg(short, long)]
        extensions: bool,

        /// Path to the TOML configuration
        #[arg(long, short)]
        config: Option<PathBuf>,
    },
    /// List components.
    List {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
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
    /// Add a component to the TOML configuration file.
    Add {
        /// One of `workflow`, `activity_wasm`, `activity_stub`, `webhook_endpoint`
        #[arg(required(true))]
        component_type: ComponentType,
        /// Path to the WASM file
        #[arg(required(true))]
        location: ComponentLocationToml,
        #[arg(long, short)]
        name: String,
        /// Path to the TOML configuration
        #[arg(long, short)]
        config: Option<PathBuf>,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Execution {
    /// Submit new execution and optionally follow its status stream until the it finishes.
    Submit {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        #[arg(short, long)]
        execution_id: Option<ExecutionId>,
        /// Function in the fully qualified format
        #[arg(value_name = "function")]
        ffqn: FunctionFqn,
        /// Follow the stream of events until the execution finishes
        #[arg(short, long)]
        follow: bool,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long, requires = "follow")]
        no_reconnect: bool,
        /// Output JSON in Web API format.
        #[arg(short, long)]
        json: bool,
        /// Accepted Parameter Formats:
        ///
        /// - JSON array string, e.g. '["first", "second", null, 1]'
        ///
        /// - File reference prefixed with @, e.g. @file.json (file must contain a valid JSON array)
        ///
        /// - Multiple arguments after --, e.g. -- '"first"' @secondparam.json null 1
        ///
        /// - For functions with no parameters: [] (JSON array variant) or no arguments.
        #[arg(name = "parameters")]
        params: Vec<String>,
    },
    /// Write a return value or an execution error to an already created stubbed execution.
    Stub(Stub),
    /// Get the current state of an execution.
    Get {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Follow the status stream until the execution finishes.
        #[arg(short, long)]
        follow: bool,
        execution_id: ExecutionId,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long, requires = "follow")]
        no_reconnect: bool,
    },
    Cancel(CancelCommand),
}

pub(crate) mod params {
    use clap::error::ErrorKind;
    use serde_json::Value;

    pub(crate) fn parse_params(params: Vec<String>) -> Result<Vec<serde_json::Value>, clap::Error> {
        if params.is_empty() {
            Ok(vec![]) // no params, does not matter if `--` was present.
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
            let Value::Array(params) = json_value else {
                return Err(clap::Error::raw(
                    ErrorKind::ValueValidation,
                    "Parameter provided as JSON must be a JSON array.",
                ));
            };
            Ok(params)
        } else {
            // Fallback to raw arguments. Each argument is interpreted as a JSON value or a file starting with `@` that contains the JSON.
            let mut parsed_params: Vec<Value> = Vec::new();
            for (idx, arg) in params.into_iter().enumerate() {
                let arg = if let Some(file_path) = arg.strip_prefix('@') {
                    std::fs::read_to_string(file_path).map_err(|err| {
                        clap::Error::raw(
                            ErrorKind::Io,
                            format!(
                                "{}-th parameter parsing failed: failed to read file '{file_path}': {err}",
                                idx + 1
                            ),
                        )
                    })?
                } else {
                    arg
                };
                let json = serde_json::from_str(&arg).map_err(|err| {
                    clap::Error::raw(
                        ErrorKind::ValueValidation,
                        format!(
                            "cannot parse {}-th parameter `{arg}` as JSON -  {err}",
                            idx + 1
                        ),
                    )
                })?;
                parsed_params.push(json);
            }
            Ok(parsed_params)
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
#[command()]
pub(crate) struct Stub {
    /// Address of the obelisk server
    #[arg(short, long, default_value = "http://127.0.0.1:5005")]
    pub(crate) api_url: String,
    /// Execution ID of the stub execution waiting for its return value.
    #[arg(value_name = "EXECUTION_ID")]
    pub(crate) execution_id: ExecutionIdDerived,

    /// Stub a return value encoded as JSON
    #[arg(value_name = "RETURN_VAL")]
    pub(crate) return_value: String,
}

#[derive(Debug, clap::Args)]
#[command()]
pub(crate) struct CancelCommand {
    /// Address of the obelisk server
    #[arg(short, long, default_value = "http://127.0.0.1:5005")]
    pub(crate) api_url: String,
    #[arg(value_name = "ID")]
    pub(crate) id: String,
}
