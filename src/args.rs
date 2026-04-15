use clap::Parser;
use concepts::{
    ComponentType, ExecutionId, FunctionFqn, FunctionFqnParseError,
    prefixed_ulid::{DeploymentId, ExecutionIdDerived},
};

/// Deployment TOML section names, used as the key in the deployment TOML file.
#[derive(
    Debug, Clone, Copy, strum::Display, strum::EnumString, serde::Serialize, serde::Deserialize,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub(crate) enum TomlComponentType {
    ActivityWasm,
    ActivityStub,
    ActivityExternal,
    ActivityJs,
    WorkflowWasm,
    WorkflowJs,
    WebhookEndpointWasm,
    WebhookEndpointJs,
    Cron,
}
use std::{path::PathBuf, str::FromStr};

fn parse_oci_reference(s: &str) -> Result<oci_client::Reference, String> {
    let s = s
        .strip_prefix("oci://")
        .ok_or_else(|| format!("OCI reference must start with `oci://`, got: {s}"))?;
    oci_client::Reference::from_str(s).map_err(|e| e.to_string())
}

/// A deployment source: either a path to a TOML file or an existing deployment ID.
#[derive(Debug, Clone)]
pub(crate) enum DeploymentSource {
    File(PathBuf),
    Id(DeploymentId),
}

impl FromStr for DeploymentSource {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(id) = s.parse::<DeploymentId>() {
            Ok(DeploymentSource::Id(id))
        } else {
            Ok(DeploymentSource::File(PathBuf::from(s)))
        }
    }
}

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
    /// Run or verify the Obelisk server.
    #[command(subcommand)]
    Server(Server),
    /// Submit, inspect, stub, cancel, pause, unpause, replay, or upgrade executions against a running server.
    #[command(subcommand)]
    Execution(Execution),
    /// Inspect components or add/push them to an OCI registry.
    #[command(subcommand)]
    Component(Component),
    /// Manage deployments.
    #[command(subcommand)]
    Deployment(Deployment),
    /// Generate configuration files and WIT artifacts.
    #[command(subcommand)]
    Generate(Generate),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Deployment {
    /// Upload a deployment TOML as a new deployment; print the new deployment ID.
    Submit {
        /// Path to the deployment TOML file.
        #[arg(
            value_name = "PATH",
            required_unless_present = "empty",
            conflicts_with = "empty"
        )]
        file: Option<PathBuf>,
        /// Submit an empty deployment with no components.
        #[arg(long)]
        empty: bool,
        /// Verify all environment variables before persisting.
        #[arg(long)]
        verify: bool,
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
    },
    /// Submit (if a file is given) and enqueue for next server restart.
    Enqueue {
        /// Path to a deployment TOML file, or an existing deployment ID.
        #[arg(
            value_name = "PATH|ID",
            required_unless_present = "empty",
            conflicts_with = "empty"
        )]
        source: Option<DeploymentSource>,
        /// Enqueue an empty deployment with no components.
        #[arg(long)]
        empty: bool,
        /// Verify all environment variables before enqueuing.
        #[arg(long)]
        verify: bool,
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
    },
    /// Submit (if a file is given) and hot-redeploy immediately. Fails if not possible.
    Apply {
        /// Path to a deployment TOML file, or an existing deployment ID.
        #[arg(
            value_name = "PATH|ID",
            required_unless_present = "empty",
            conflicts_with = "empty"
        )]
        source: Option<DeploymentSource>,
        /// Apply an empty deployment with no components.
        #[arg(long)]
        empty: bool,
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
    },
    /// List recent deployments.
    List {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
    },
    /// Show the full configuration of a deployment.
    Show {
        /// Deployment ID
        #[arg(value_name = "ID")]
        id: String,
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Generate {
    /// Generate the server configuration (server.toml) schema in JSON schema format.
    #[cfg(debug_assertions)]
    ServerConfigSchema {
        /// Filename to write the schema to, defaults to <stdout>.
        output: Option<PathBuf>,
    },
    /// Generate the deployment configuration (deployment.toml) schema in JSON schema format.
    #[cfg(debug_assertions)]
    DeploymentSchema {
        /// Filename to write the schema to, defaults to <stdout>.
        output: Option<PathBuf>,
    },
    /// Generate the canonical deployment schema (stored in the database) in JSON schema format.
    #[cfg(debug_assertions)]
    DeploymentCanonicalSchema {
        /// Filename to write the schema to, defaults to <stdout>.
        output: Option<PathBuf>,
    },
    /// Generate the database storage schema in JSON schema format.
    #[cfg(debug_assertions)]
    DbSchema {
        /// Filename to write the schema to, defaults to <stdout>.
        output: Option<PathBuf>,
    },
    /// Generate the `OpenAPI` schema in JSON format.
    #[cfg(debug_assertions)]
    OpenApiSchema {
        /// Filename to write the schema to, defaults to <stdout>.
        output: Option<PathBuf>,
    },
    /// Generate extension WIT files that are automatically implemented by Obelisk
    /// based on the exported interfaces of the component (e.g. `-schedule`, `-await-next` variants).
    WitExtensions {
        /// Overwrite existing files in the output directory.
        #[arg(long, short)]
        force: bool,
        /// Component type this WIT is for. One of `workflow`, `activity`, `activity_stub`, `webhook_endpoint`.
        component_type: ComponentType,
        /// Path to the `wit` folder containing the target world and, if present, a `deps` subfolder.
        input_wit_directory: PathBuf,
        /// Directory where folders and WIT files will be written to.
        output_directory: PathBuf,
    },
    /// Generate Obelisk's built-in support WIT files (host interfaces) for the given component type.
    WitSupport {
        /// Component type whose host interfaces to emit. One of `workflow`, `activity`, `activity_stub`, `webhook_endpoint`.
        component_type: ComponentType,
        /// Directory where folders and WIT files will be written to.
        output_directory: PathBuf,
        /// Overwrite existing files in the output directory.
        #[arg(long, short)]
        overwrite: bool,
    },
    /// Generate WIT dependency folder based on activities and workflows found in the deployment TOML.
    WitDeps {
        /// Path to the deployment TOML file.
        #[arg(long, short)]
        deployment: PathBuf,
        /// Directory where folders and WIT files will be written to.
        output_directory: PathBuf,
        /// Overwrite existing files.
        #[arg(long, short)]
        overwrite: bool,
    },
    /// Generate a default server.toml.
    ServerConfig {
        /// Filename to write the TOML to, defaults to `server.toml`.
        output: Option<PathBuf>,
        /// Overwrite existing file.
        #[arg(long, short)]
        overwrite: bool,
    },
    /// Generate a default deployment.toml.
    Deployment {
        /// Filename to write the TOML to, defaults to `deployment.toml`.
        output: Option<PathBuf>,
        /// Overwrite existing file.
        #[arg(long, short)]
        overwrite: bool,
    },
    /// Generate a fresh random execution ID and print it to stdout.
    ExecutionId,
    /// Print a prompt context for authoring an Obelisk application with a coding agent.
    ///
    /// Usage: obelisk generate prompt 'description of what to build' | claude
    ///
    /// Example:
    ///   obelisk generate prompt 'Create a daily task that monitors the GitHub organization "obeli-sk". Fetch the list of public repositories,
    ///     then in parallel fetch their dev-deps.txt files from the main branch. If a file exists and contains the line `obelisk someversion`,
    ///     ensure the same version is used throughout the organization. If not, send an alert via Postmark.'
    Prompt {
        /// Description of the application to build.
        description: String,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Server {
    /// Start the Obelisk server.
    Run {
        /// Delete the sqlite database directory before starting. Destroys all execution history.
        #[arg(long)]
        clean_sqlite_directory: bool,
        /// Delete both the codegen cache and the OCI image cache before starting.
        #[arg(long)]
        clean_cache: bool,
        /// Delete only the codegen cache before starting; OCI image cache is kept.
        #[arg(long)]
        clean_codegen_cache: bool,
        /// Path to the server configuration file (server.toml). If omitted, built-in defaults are used.
        #[arg(long)]
        server_config: Option<PathBuf>,
        /// Path to the deployment TOML file. If provided, the deployment is inserted and activated on startup,
        /// overriding any existing Enqueued or Active deployment in the database.
        #[arg(short, long, conflicts_with = "empty")]
        deployment: Option<PathBuf>,
        /// Start with an empty deployment, ignoring any Enqueued or Active deployment in the database.
        /// Useful for recovering from a faulty deployment: start empty, then push a new deployment or switch to existing via gRPC.
        #[arg(long)]
        empty: bool,
        /// Do not fail startup when a component's imports/exports fail type checking against the current deployment.
        #[arg(long, short)]
        suppress_type_checking_errors: bool,
    },
    /// Read the configuration, compile the components, verify their imports and exit without starting the server.
    Verify {
        /// Delete both the codegen cache and the OCI image cache before verifying.
        #[arg(long)]
        clean_cache: bool,
        /// Delete only the codegen cache before verifying; OCI image cache is kept.
        #[arg(long)]
        clean_codegen_cache: bool,
        /// Path to the server configuration file (server.toml). If omitted, built-in defaults are used.
        #[arg(long)]
        server_config: Option<PathBuf>,
        /// Path to the deployment TOML file. If omitted, the database's Enqueued deployment is used,
        /// falling back to the Active deployment. Errors if neither is found.
        #[arg(short, long)]
        deployment: Option<PathBuf>,
        /// Skip the check that every environment variable referenced by the deployment is set.
        #[arg(long, short)]
        ignore_missing_env_vars: bool,
        /// Do not fail when a component's imports/exports fail type checking against the deployment.
        #[arg(long, short)]
        suppress_type_checking_errors: bool,
        /// Skip opening the sqlite database and validating its schema.
        #[arg(long)]
        skip_db: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Component {
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
    /// Push a WASM component to an OCI registry with deployment metadata.
    Push {
        /// Component name in the input deployment TOML
        #[arg(required(true))]
        component_name: String,
        /// OCI reference with `oci://` prefix. Example: `oci://docker.io/repo/image:tag`
        #[arg(required(true), value_parser = parse_oci_reference)]
        oci: oci_client::Reference,
        /// Path to the input deployment TOML file.
        #[arg(long, short, required = true)]
        deployment: PathBuf,
    },
    /// Add a component to the deployment TOML configuration file from an OCI reference.
    Add {
        /// OCI reference with `oci://` prefix. Example: `oci://docker.io/repo/image:tag`
        #[arg(required(true), value_parser = parse_oci_reference)]
        location: oci_client::Reference,
        /// Component name in the target deployment TOML
        #[arg(required(true))]
        component_name: String,
        /// Path to the target deployment TOML file.
        #[arg(long, short, required = true)]
        deployment: PathBuf,
        /// Pin the location with the manifest digest (e.g. `image:tag@sha256:...`).
        #[arg(long)]
        locked: bool,
    },
}

#[derive(Debug, Clone)]
pub enum FunctionFqnOrShort {
    Ffqn(FunctionFqn),
    Short {
        ifc_name: String,
        function_name: String,
    }, // starts with `.../` prefix
}
impl FromStr for FunctionFqnOrShort {
    type Err = FunctionFqnParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        const PREFIX: &str = ".../";

        if let Some(rest) = input.strip_prefix(PREFIX) {
            let Some((ifc_name, fn_name)) = rest.split_once('.') else {
                return Err(FunctionFqnParseError::DelimiterNotFound(input.to_string()));
            };
            // Ensure exactly two parts
            if fn_name.contains('.') {
                return Err(FunctionFqnParseError::DelimiterFoundInFunctionName(
                    input.to_string(),
                ));
            }

            Ok(FunctionFqnOrShort::Short {
                ifc_name: ifc_name.to_string(),
                function_name: fn_name.to_string(),
            })
        } else {
            Ok(FunctionFqnOrShort::Ffqn(FunctionFqn::from_str(input)?))
        }
    }
}

/// Minimum log level for `execution logs`.
/// One of: `trace`, `debug`, `info`, `warn`, `error`, `off`.
#[derive(Debug, Clone, Copy, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum LogLevelArg {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    /// Disable structured log output. Use with `--show-streams`.
    Off,
}

/// Log stream type for `execution logs`.
/// One of: `stdout`, `stderr`.
#[derive(Debug, Clone, Copy, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum LogStreamTypeArg {
    Stdout,
    Stderr,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Execution {
    /// List recent executions.
    List {
        /// Address of the obelisk server.
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Filter by function FFQN prefix.
        /// Accepts any prefix of a fully-qualified function name, e.g.: `namespace:`.
        /// Short `.../ifc.fn` form is not supported.
        #[arg(long = "ffqn", value_name = "FFQN_PREFIX")]
        ffqn_prefix: Option<String>,
        /// Filter by execution id prefix.
        /// Useful to find all child executions if --show-derived flag is enabled.
        #[arg(long = "execution_id", short, value_name = "EXECUTION_ID_PREFIX")]
        execution_id_prefix: Option<String>,
        /// Include child (derived) executions spawned by workflows.
        /// By default only top-level executions are shown.
        #[arg(long)]
        show_derived: bool,
        /// Hide finished executions.
        #[arg(long)]
        hide_finished: bool,
        /// Number of executions to return.
        #[arg(long, default_value = "20")]
        limit: u16,
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
    },
    /// Fetch structured logs for an execution.
    Logs {
        /// Address of the obelisk server.
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Execution ID to fetch logs for.
        #[arg(value_name = "EXECUTION_ID")]
        execution_id: ExecutionId,
        /// Include logs from child (derived) executions.
        #[arg(long)]
        show_derived: bool,
        /// Minimum log level: trace, debug, info, warn, error, off.
        /// `off` disables structured log output entirely (use with --show-streams).
        #[arg(long, value_name = "LEVEL", default_value = "debug")]
        level: LogLevelArg,
        /// Include stdout/stderr stream output.
        #[arg(long)]
        show_streams: bool,
        /// Filter stream output by type (stdout, stderr). Repeatable. Only with --show-streams.
        #[arg(long, value_name = "TYPE", requires = "show_streams")]
        stream_type: Vec<LogStreamTypeArg>,
        /// Show the run ID in each log line.
        #[arg(long)]
        show_run_id: bool,
        /// Only show entries created after this timestamp (RFC3339, e.g. 2026-04-14T10:00:00Z).
        #[arg(long, value_name = "TIMESTAMP")]
        after: Option<String>,
        /// Poll for new log entries until the execution finishes.
        #[arg(long)]
        follow: bool,
        /// Number of log entries to return per request (default: 20, max: 200).
        #[arg(long, default_value = "20")]
        limit: u16,
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
    },
    /// Show the execution event log (history).
    Events {
        /// Address of the obelisk server.
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Execution ID.
        #[arg(value_name = "EXECUTION_ID")]
        execution_id: ExecutionId,
        /// Start from this event version (inclusive).
        #[arg(long, value_name = "VERSION")]
        from: Option<u32>,
        /// Number of events to return.
        #[arg(long, default_value = "20")]
        limit: u16,
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
    },
    /// Show join-set responses for an execution (child results, delay completions).
    Responses {
        /// Address of the obelisk server.
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Execution ID.
        #[arg(value_name = "EXECUTION_ID")]
        execution_id: ExecutionId,
        /// Start from this response cursor (inclusive).
        #[arg(long, value_name = "CURSOR")]
        from: Option<u32>,
        /// Number of responses to return.
        #[arg(long, default_value = "20")]
        limit: u16,
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
    },
    /// Submit a new execution and optionally follow its status stream until it finishes.
    Submit {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Use this explicit execution ID instead of having the server assign one. Useful for idempotency. See `obelisk generate execution-id`.
        #[arg(short, long)]
        execution_id: Option<ExecutionId>,
        /// Function to invoke, either as a fully qualified name (`ns:pkg/ifc.fn`)
        /// or shortened to `.../ifc.fn` when the interface name is unambiguous.
        #[arg(value_name = "function")]
        ffqn: FunctionFqnOrShort,
        /// Follow the stream of events until the execution finishes.
        #[arg(short, long)]
        follow: bool,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long, requires = "follow")]
        no_reconnect: bool,
        /// Output events as JSON in Web API format instead of human-readable text.
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
    /// Write a return value or an execution error to an already created stubbed execution,
    /// unblocking any parent workflow that is awaiting it.
    Stub(Stub),
    /// Get the current state of an execution, optionally following the event stream.
    Get {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Follow the status stream until the execution finishes.
        #[arg(short, long)]
        follow: bool,
        /// Execution ID to look up.
        execution_id: ExecutionId,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long, requires = "follow")]
        no_reconnect: bool,
    },
    /// Request cancellation of a running activity or pending delay.
    Cancel(CancelCommand),
    /// Pause a workflow execution, preventing it from being picked up until unpaused.
    Pause {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Execution ID to pause.
        execution_id: ExecutionId,
    },
    /// Resume a previously paused workflow execution.
    Unpause {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Execution ID to unpause.
        execution_id: ExecutionId,
    },
    /// Replay a workflow execution from its execution log, checking for non-determinism.
    Replay {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Execution ID to replay.
        execution_id: ExecutionId,
    },
    /// Upgrade a workflow execution to the current component version in the active deployment.
    ///
    /// Looks up the execution's FFQN, finds the component that exports it in the active
    /// deployment, and upgrades the execution from its current component digest to the new one.
    Upgrade {
        /// Address of the obelisk server
        #[arg(short, long, default_value = "http://127.0.0.1:5005")]
        api_url: String,
        /// Execution ID to upgrade.
        execution_id: ExecutionId,
        /// Skip the determinism check during upgrade.
        #[arg(long)]
        skip_determinism_check: bool,
    },
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
#[expect(clippy::doc_markdown)]
pub(crate) struct CancelCommand {
    /// Address of the obelisk server
    #[arg(short, long, default_value = "http://127.0.0.1:5005")]
    pub(crate) api_url: String,
    /// Execution id of an activity (E_01...) or a delay request (Delay_01...)
    #[arg(value_name = "ID")]
    pub(crate) id: String,
}
