use crate::config::secret_registry::API_TOKEN;
use clap::Parser;
use concepts::{
    ComponentType, ExecutionId, FunctionFqn, FunctionFqnParseError,
    prefixed_ulid::{
        DelayId, DeploymentId, ExecutionIdDerived, ExecutionIdTopLevel, NodeRunId, SystemEventId,
    },
};
use secrecy::SecretString;

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
    ActivityExec,
    ActivityVm,
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

fn parse_secret_string(value: &str) -> Result<SecretString, &'static str> {
    if value.is_empty() {
        Err("API token must not be empty")
    } else {
        Ok(SecretString::from(value.to_owned()))
    }
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
    pub(crate) const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
    // Short git commit, set by Nix builds.
    const GIT_SHA: &str = match option_env!("OBELISK_GIT_SHA") {
        Some(sha) => sha,
        None => "",
    };
    const GIT_SHA_OPEN: &str = if GIT_SHA.is_empty() { "" } else { " (" };
    const GIT_SHA_CLOSE: &str = if GIT_SHA.is_empty() { "" } else { ")" };
    pub(crate) const LONG_VERSION: &str =
        const_format::concatcp!(PKG_VERSION, GIT_SHA_OPEN, GIT_SHA, GIT_SHA_CLOSE);
}

#[derive(Parser, Debug)]
#[clap(name = "obelisk")]
#[command
(
    version = shadow::LONG_VERSION,
about = "Obelisk: deterministic workflow engine", disable_version_flag = true, disable_help_subcommand = true)]
pub(crate) struct Args {
    #[command(subcommand)]
    pub(crate) command: Subcommand,

    /// Print version
    #[arg(short, long, action = clap::ArgAction::Version)]
    version: Option<bool>,
}

/// Connection options shared by direct API commands.
#[derive(Debug, clap::Args)]
pub(crate) struct ClientConnection {
    /// Show HTTP requests and response statuses on stderr.
    #[arg(long, global = true)]
    pub(crate) verbose: bool,
    /// Address of the Obelisk server.
    #[arg(
        short,
        long,
        global = true,
        env = "OBELISK_API_URL",
        default_value = "http://127.0.0.1:5005"
    )]
    pub(crate) api_url: String,
    /// API token presented to the server as `Authorization: Bearer <token>`.
    /// Falls back to `$OBELISK_API_TOKEN`.
    #[arg(
        long,
        global = true,
        env = API_TOKEN,
        hide_env_values = true,
        value_name = "TOKEN",
        value_parser = parse_secret_string
    )]
    pub(crate) api_token: Option<SecretString>,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Subcommand {
    /// Manage apps through the Obelisk cloud control plane.
    Cloud(CloudArgs),
    /// Run or verify the Obelisk server.
    #[command(subcommand)]
    Server(Server),
    /// Submit, inspect, stub, cancel, pause, replay, persist backtraces, or upgrade executions.
    Execution(ExecutionArgs),
    /// Inspect components or add/push them to an OCI registry.
    Component(ComponentArgs),
    /// Manage deployments.
    Deployment(DeploymentArgs),
    /// Perform operator-only destructive maintenance.
    Admin(AdminArgs),
    /// Generate configuration files and WIT artifacts.
    #[command(subcommand)]
    Generate(Generate),
}

#[derive(Debug, clap::Args)]
pub(crate) struct ExecutionArgs {
    #[command(subcommand)]
    pub(crate) command: Execution,
    #[command(flatten)]
    pub(crate) token: ClientConnection,
}

#[derive(Debug, clap::Args)]
pub(crate) struct ComponentArgs {
    #[command(subcommand)]
    pub(crate) command: Component,
    #[command(flatten)]
    pub(crate) token: ClientConnection,
}

#[derive(Debug, clap::Args)]
pub(crate) struct DeploymentArgs {
    #[command(subcommand)]
    pub(crate) command: Deployment,
    #[command(flatten)]
    pub(crate) token: ClientConnection,
}

#[derive(Debug, clap::Args)]
pub(crate) struct AdminArgs {
    #[command(subcommand)]
    pub(crate) command: Admin,
    #[command(flatten)]
    pub(crate) token: ClientConnection,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Admin {
    #[command(subcommand)]
    Executions(AdminExecutions),
    #[command(subcommand)]
    Deployments(AdminDeployments),
    #[command(subcommand)]
    Events(AdminEvents),
    #[command(subcommand)]
    Storage(AdminStorage),
    /// Print the ID of the current node run.
    NodeRunId {
        /// Output as JSON instead of plain text.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum AdminEvents {
    /// Get one persisted system event by ID.
    Get {
        event_id: SystemEventId,
        #[arg(long)]
        json: bool,
    },
    /// List persisted operator-visible system events.
    List {
        /// Only show events emitted by this node run.
        #[arg(long)]
        node_run_id: Option<NodeRunId>,
        #[arg(long)]
        level: Option<String>,
        #[arg(long)]
        code: Option<String>,
        /// Only show events created at or after this RFC 3339 time.
        #[arg(long)]
        from: Option<chrono::DateTime<chrono::Utc>>,
        /// Only show events created before this RFC 3339 time.
        #[arg(long)]
        to: Option<chrono::DateTime<chrono::Utc>>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[arg(long)]
        json: bool,
    },
    /// Retain system events newer than the configured maximum age.
    Retain {
        #[arg(long, value_parser = parse_retention_age)]
        max_age: std::time::Duration,
        #[arg(long, default_value_t = 1000)]
        batch_size: u32,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum AdminStorage {
    /// Show persistent storage usage and record counts.
    Show {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum AdminExecutions {
    /// Delete one or more terminal top-level executions and their derived trees.
    Delete {
        #[arg(required = true, num_args = 1..)]
        execution_ids: Vec<ExecutionId>,
        /// Delete non-terminal trees unless their roots belong to the active deployment.
        #[arg(long)]
        force: bool,
        #[arg(long)]
        json: bool,
    },
    /// Retain top-level execution trees by count or age.
    Retain {
        #[arg(long, required_unless_present = "max_age", conflicts_with = "max_age")]
        count: Option<u32>,
        #[arg(long, value_parser = parse_retention_age)]
        max_age: Option<std::time::Duration>,
        #[arg(long, default_value_t = 100)]
        batch_size: u32,
        /// Delete non-terminal trees unless their roots belong to the active deployment.
        #[arg(long)]
        force: bool,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum AdminDeployments {
    /// Delete one or more inactive deployments.
    Delete {
        #[arg(required = true, num_args = 1..)]
        deployment_ids: Vec<DeploymentId>,
        #[arg(long)]
        delete_executions: bool,
        /// Delete non-terminal trees unless their roots belong to the active deployment.
        #[arg(long, requires = "delete_executions")]
        force: bool,
        #[arg(long)]
        json: bool,
    },
    /// Retain inactive deployments by count or age; active and enqueued deployments are kept.
    Retain {
        #[arg(long, required_unless_present = "max_age", conflicts_with = "max_age")]
        count: Option<u32>,
        #[arg(long, value_parser = parse_retention_age)]
        max_age: Option<std::time::Duration>,
        #[arg(long)]
        delete_executions: bool,
        /// Delete non-terminal trees unless their roots belong to the active deployment.
        #[arg(long, requires = "delete_executions")]
        force: bool,
        #[arg(long, default_value_t = 100)]
        batch_size: u32,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        json: bool,
    },
}

fn parse_retention_age(value: &str) -> Result<std::time::Duration, String> {
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(|| "duration must have a unit: s, m, h, or d".to_owned())?;
    let amount = value[..split]
        .parse::<u64>()
        .map_err(|_| "duration must start with a positive integer".to_owned())?;
    if amount == 0 {
        return Err("duration must be greater than zero".to_owned());
    }
    let multiplier = match &value[split..] {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => return Err("duration unit must be s, m, h, or d".to_owned()),
    };
    amount
        .checked_mul(multiplier)
        .map(std::time::Duration::from_secs)
        .ok_or_else(|| "duration is too large".to_owned())
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Deployment {
    /// Submit a deployment TOML; print the deployment ID.
    /// Reuses matching active content and description unless a deployment ID is supplied.
    Submit {
        /// Path to the deployment TOML file.
        #[arg(
            value_name = "PATH",
            default_value = "deployment.toml",
            default_value_if("empty", "true", None),
            conflicts_with = "empty"
        )]
        file: Option<PathBuf>,
        /// Submit an empty deployment with no components.
        #[arg(long)]
        empty: bool,
        /// Store a verified deployment while app or platform approval is pending.
        /// Allows unavailable values and approvals; activation still checks strictly.
        #[arg(long)]
        allow_unavailable_runtime_config: bool,
        /// Optional human-readable description.
        #[arg(long)]
        description: Option<String>,
        /// Optional client-supplied deployment ID for idempotent submission.
        ///
        /// If a deployment with this ID already exists and its content digest
        /// matches, the submission is a no-op; a digest mismatch is an error.
        /// Generate one with `obelisk generate deployment-id`.
        #[arg(long)]
        deployment_id: Option<DeploymentId>,
        /// Print the deployment ID as a JSON string.
        #[arg(long)]
        json: bool,
    },
    /// Submit and stage for cloud app apply, or enqueue for a standalone server restart.
    /// Reuses matching active content and description unless a deployment ID is supplied.
    Enqueue {
        /// Path to a deployment TOML file, or an existing deployment ID.
        #[arg(
            value_name = "PATH|ID",
            default_value = "deployment.toml",
            default_value_if("empty", "true", None),
            conflicts_with = "empty"
        )]
        source: Option<DeploymentSource>,
        /// Enqueue an empty deployment with no components.
        #[arg(long)]
        empty: bool,
        /// Submit and enqueue while app or platform approval is pending.
        /// Activation still checks strictly.
        #[arg(long)]
        allow_unavailable_runtime_config: bool,
        /// Optional human-readable description for a newly submitted deployment.
        #[arg(long)]
        description: Option<String>,
        /// Optional client-supplied deployment ID for idempotent submission.
        ///
        /// Only valid when submitting a file/empty deployment, not when `source`
        /// is an existing deployment ID. Generate one with
        /// `obelisk generate deployment-id`.
        #[arg(long)]
        deployment_id: Option<DeploymentId>,
        /// Print the deployment ID as a JSON string.
        #[arg(long)]
        json: bool,
    },
    /// Submit (if a file is given) and apply immediately. Fails if not possible.
    /// Reuses matching active content and description unless a deployment ID is supplied.
    Apply {
        /// Path to a deployment TOML file, or an existing deployment ID.
        #[arg(
            value_name = "PATH|ID",
            default_value = "deployment.toml",
            default_value_if("empty", "true", None),
            conflicts_with = "empty"
        )]
        source: Option<DeploymentSource>,
        /// Apply an empty deployment with no components.
        #[arg(long)]
        empty: bool,
        /// Optional human-readable description for a newly submitted deployment.
        #[arg(long)]
        description: Option<String>,
        /// Optional client-supplied deployment ID for idempotent submission.
        ///
        /// Only valid when submitting a file/empty deployment, not when `source`
        /// is an existing deployment ID. Generate one with
        /// `obelisk generate deployment-id`.
        #[arg(long)]
        deployment_id: Option<DeploymentId>,
        /// Print the deployment ID as a JSON string.
        #[arg(long)]
        json: bool,
    },
    /// List recent deployments.
    List {
        /// Output as JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
    },
    /// Print the ID of the currently active deployment.
    Active {
        /// Output as JSON instead of plain text.
        #[arg(long)]
        json: bool,
    },

    /// Show a deployment.
    ///
    /// By default prints the stored deployment manifest. Pass a FILE to print a single source
    /// file, or `--json` to render the stored TOML manifest as JSON.
    Show {
        /// Deployment ID
        #[arg(value_name = "ID")]
        id: DeploymentId,
        /// Print the content of a single source file, identified by its deployment-relative
        /// path as shown in the TOML (e.g. `scripts/run.sh`).
        #[arg(value_name = "FILE", conflicts_with = "json")]
        file: Option<String>,
        /// Print the stored TOML manifest as JSON instead of TOML.
        #[arg(long)]
        json: bool,
    },
    /// Retrieve a deployment as editable TOML without generated metadata, plus its source files.
    Pull {
        /// Deployment ID
        #[arg(value_name = "ID")]
        id: DeploymentId,
        /// Directory to write `deployment.toml` and source files into. Defaults to the
        /// current directory.
        #[arg(long, short)]
        output: Option<PathBuf>,
        /// Overwrite existing files in the output directory.
        #[arg(long, short)]
        force: bool,
        /// Keep generated content digests and JavaScript module metadata in `deployment.toml`.
        #[arg(long)]
        include_generated_metadata: bool,
    },
    /// Print the content digest of a deployment TOML, or of an existing deployment.
    /// Equal digests mean equal content, so tooling can skip resubmitting the active deployment.
    Digest {
        /// Path to a deployment TOML file, or an existing deployment ID.
        #[arg(value_name = "PATH|ID", default_value = "deployment.toml")]
        source: DeploymentSource,
        /// Print the digest as a JSON string.
        #[arg(long)]
        json: bool,
    },
    /// Alias for `server verify --skip-db --deployment <PATH>`.
    Verify(DeploymentVerifyArgs),
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub(crate) enum StarterTemplate {
    /// Web dashboard that checks URLs in parallel via a workflow and a retried activity.
    #[default]
    UrlHealthDashboard,
    /// One webhook calling one workflow calling one HTTP activity.
    Minimal,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Generate {
    /// Create a JS starter app in a new NAME directory, or in the current directory if unnamed.
    New {
        /// New directory and app name; defaults to the current directory with a derived slug.
        name: Option<String>,
        #[arg(long, value_enum, default_value_t)]
        template: StarterTemplate,
    },
    /// Calculate reviewed configuration digests for exec and secret-exposing VM activities.
    SecretConfigDigest {
        /// Path to the deployment TOML file.
        #[arg(long, short)]
        deployment: PathBuf,
        /// Limit output to this resolved component name.
        component_name: Option<String>,
        /// Output structured JSON.
        #[arg(short, long)]
        json: bool,
    },
    /// Generate extension WIT files that are automatically implemented by Obelisk
    /// based on the exported interfaces of the component (e.g. `-schedule`, `-await-next` variants).
    WitExtensions {
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
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
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
        /// Component type whose host interfaces to emit. One of `workflow`, `activity`, `activity_stub`, `webhook_endpoint`.
        component_type: ComponentType,
        /// Directory where folders and WIT files will be written to.
        output_directory: PathBuf,
        /// Overwrite existing files in the output directory.
        #[arg(long, short)]
        force: bool,
    },
    /// Generate WIT dependency folder based on activities and workflows found in the deployment TOML.
    WitDeps {
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
        /// Path to the deployment TOML file.
        #[arg(long, short)]
        deployment: PathBuf,
        /// Directory where folders and WIT files will be written to.
        output_directory: PathBuf,
        /// Overwrite existing files.
        #[arg(long, short)]
        force: bool,
        /// Skip local-path components.
        #[arg(long)]
        skip_local: bool,
        /// Remove obsolete WIT dependencies previously generated by Obelisk.
        #[arg(long)]
        prune: bool,
    },
    /// Generate a default server.toml.
    ServerConfig {
        /// Generate a single-party server config that enables exec activities.
        #[arg(long)]
        trusted: bool,
        /// Filename to write the TOML to, defaults to <stdout>.
        output: Option<PathBuf>,
        /// Overwrite existing file.
        #[arg(long, short)]
        force: bool,
    },
    /// Generate a default app.toml.
    AppConfig {
        /// Allow outbound HTTP from any component with a matching deployment rule.
        #[arg(long)]
        trusted: bool,
        /// Filename to write the TOML to, defaults to stdout.
        output: Option<PathBuf>,
        /// Overwrite existing file.
        #[arg(long, short)]
        force: bool,
    },
    // backcompat for 0.41 -> 0.42
    /// Split policy fields from an existing server.toml into app.toml.
    SplitConfig {
        /// Existing server.toml to update.
        #[arg(long)]
        server_config: PathBuf,
        /// Output app.toml, defaulting to a sibling of server.toml.
        #[arg(short, long)]
        app_config: Option<PathBuf>,
    },
    /// Generate a default deployment.toml.
    Deployment {
        /// Filename to write the TOML to, defaults to <stdout>.
        output: Option<PathBuf>,
        /// Overwrite existing file.
        #[arg(long, short)]
        force: bool,
    },
    /// Generate a fresh random execution ID and print it to stdout.
    ExecutionId {
        /// Output as JSON instead of plain text.
        #[arg(short, long)]
        json: bool,
    },
    /// Generate a fresh random deployment ID and print it to stdout.
    ///
    /// The ID can be passed to `deployment submit/enqueue/apply --deployment-id`
    /// for idempotent submission.
    DeploymentId {
        /// Output as JSON instead of plain text.
        #[arg(short, long)]
        json: bool,
    },
    /// Generate a random API token and print it to stdout.
    ///
    /// Stdout carries only the token, so it composes with client env injection:
    /// `OBELISK_API_TOKEN=$(obelisk generate token)`. When run interactively,
    /// the token's `api.token_hashes` entry is also printed to stderr.
    /// The plaintext token is printed once and never stored; revoke it by
    /// deleting its `api.token_hashes` entry and restarting the server.
    Token {
        /// Output the token and its `sha256:` hash as JSON instead of plain text.
        #[arg(short, long)]
        json: bool,
        /// Append the token's hash to `api.token_hashes` in this server.toml file.
        #[arg(short, long)]
        server_config: Option<PathBuf>,
    },
    /// Print a prompt context for authoring an Obelisk application with a coding agent.
    ///
    /// Usage: obelisk generate prompt description of what to build | claude
    ///
    /// Example:
    ///   obelisk generate prompt Create a daily task that monitors GitHub org obeli-sk | claude
    #[command(trailing_var_arg = true)]
    Prompt {
        /// Description of the application to build.
        #[arg(trailing_var_arg = true, required = true)]
        description: Vec<String>,
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
        #[arg(short, long)]
        server_config: Option<PathBuf>,
        /// App policy path; defaults to ./app.toml if present, otherwise built-in defaults (set `OBELISK_APP_NAME`).
        #[arg(short, long)]
        app_config: Option<PathBuf>,
        /// Path to the deployment TOML file. If provided, the deployment is inserted and activated on startup,
        /// overriding any existing Enqueued or Active deployment in the database.
        #[arg(short, long, conflicts_with = "empty")]
        deployment: Option<PathBuf>,
        /// Select an existing deployment from the database and verify it before activation.
        #[arg(long, conflicts_with_all = ["deployment", "empty", "description"])]
        deployment_id: Option<DeploymentId>,
        /// Start with an empty deployment, ignoring any Enqueued or Active deployment in the database.
        /// Useful for recovering from a faulty deployment: start empty, then push a new deployment or switch to existing via the API.
        #[arg(long)]
        empty: bool,
        /// Optional human-readable description for the deployment created by --deployment or --empty.
        #[arg(long)]
        description: Option<String>,
        /// Do not fail startup when a component's imports/exports fail type checking against the current deployment.
        #[arg(long)]
        suppress_type_checking_errors: bool,
        /// Accept unauthenticated requests on the API port. Dev/recovery override.
        #[arg(long, conflicts_with = "api_token")]
        no_auth: bool,

        /// API token required to access the API server as `Authorization: Bearer <token>`.
        /// Falls back to `$OBELISK_API_TOKEN`.
        #[arg(long,
            env = API_TOKEN,
            hide_env_values = true,
            value_name = "TOKEN",
            value_parser = parse_secret_string,
            conflicts_with = "no_auth",
        )]
        api_token: Option<SecretString>,
    },
    /// Read the last successfully committed app checkpoint without starting the server.
    Checkpoint {
        #[arg(short, long)]
        server_config: Option<PathBuf>,
    },
    /// Read the configuration, compile the components, verify their imports and exit without starting the server.
    Verify(VerifyArgs),
}

#[derive(Debug, clap::Args)]
#[expect(clippy::struct_excessive_bools)]
pub(crate) struct VerifyArgs {
    /// Delete both the codegen cache and the OCI image cache before verifying.
    #[arg(long)]
    pub(crate) clean_cache: bool,
    /// Delete only the codegen cache before verifying; OCI image cache is kept.
    #[arg(long)]
    pub(crate) clean_codegen_cache: bool,
    /// Path to the server configuration file (server.toml). If omitted, built-in defaults are used.
    #[arg(short, long)]
    pub(crate) server_config: Option<PathBuf>,
    /// App policy path; defaults to ./app.toml if present, otherwise built-in defaults (set `OBELISK_APP_NAME`).
    #[arg(short, long)]
    pub(crate) app_config: Option<PathBuf>,
    /// Path to the deployment TOML file. If omitted, the database's Enqueued deployment is used,
    /// falling back to the Active deployment. Errors if neither is found.
    #[arg(short, long)]
    pub(crate) deployment: Option<PathBuf>,
    /// Verify an existing deployment from the database without activating it.
    #[arg(long, conflicts_with_all = ["deployment", "skip_db"])]
    pub(crate) deployment_id: Option<DeploymentId>,
    /// Tolerate missing environment and secret values; declarations and permissions are still checked.
    #[arg(long)]
    pub(crate) allow_unavailable_runtime_config: bool,
    /// Do not fail when a component's imports/exports fail type checking against the deployment.
    #[arg(long)]
    pub(crate) suppress_type_checking_errors: bool,
    /// Skip opening the sqlite database and validating its schema. Requires `--deployment`,
    /// since with no database there is no active deployment to fall back to.
    #[arg(long, requires = "deployment")]
    pub(crate) skip_db: bool,
    #[command(flatten)]
    pub(crate) fixes: VerifyFixArgs,
}

#[derive(Debug, Default, Clone, clap::Args)]
#[expect(clippy::struct_excessive_bools)]
pub(crate) struct VerifyFixArgs {
    /// Apply all app repairs, including exec approvals and secret exposure grants. Does not change server permissions.
    #[arg(long, requires = "deployment")]
    pub(crate) fix: bool,
    /// Add missing app environment and secret declarations without supplying values.
    #[arg(long, requires = "deployment")]
    pub(crate) fix_app_declarations: bool,
    /// Add app outbound HTTP destinations and secret replacement permissions.
    #[arg(long, requires = "deployment")]
    pub(crate) fix_app_http_permissions: bool,
    /// Add app exec approval digests for the selected deployment.
    #[arg(long, requires = "deployment")]
    pub(crate) fix_app_exec_permissions: bool,
    /// Add app secret exposure grants for the selected deployment.
    #[arg(long, requires = "deployment")]
    pub(crate) fix_app_secret_exposure: bool,
    /// Authorize app exec approvals and the selected deployment in the explicit server configuration.
    #[arg(long, requires_all = ["deployment", "server_config"])]
    pub(crate) fix_server_exec_permissions: bool,
}

impl VerifyFixArgs {
    pub(crate) fn any(&self) -> bool {
        self.app_declarations() || self.app_http_permissions() || self.digest_repairs()
    }

    pub(crate) fn app_declarations(&self) -> bool {
        self.fix || self.fix_app_declarations
    }
    pub(crate) fn app_http_permissions(&self) -> bool {
        self.fix || self.fix_app_http_permissions
    }
    pub(crate) fn app_exec_permissions(&self) -> bool {
        self.fix || self.fix_app_exec_permissions
    }
    pub(crate) fn app_secret_exposure(&self) -> bool {
        self.fix || self.fix_app_secret_exposure
    }
    pub(crate) fn digest_repairs(&self) -> bool {
        self.app_exec_permissions()
            || self.app_secret_exposure()
            || self.fix_server_exec_permissions
    }
}

#[derive(Debug, clap::Args)]
pub(crate) struct DeploymentVerifyArgs {
    // Local verification ignores the inherited API connection.
    #[arg(long, hide = true)]
    pub(crate) api_url: Option<String>,
    /// Delete both the codegen cache and the OCI image cache before verifying.
    #[arg(long)]
    pub(crate) clean_cache: bool,
    /// Delete only the codegen cache before verifying; OCI image cache is kept.
    #[arg(long)]
    pub(crate) clean_codegen_cache: bool,
    /// Path to the local deployment TOML file.
    #[arg(short, long)]
    pub(crate) deployment: PathBuf,
    /// Do not fail when a component's imports/exports fail type checking against the deployment.
    #[arg(long)]
    pub(crate) suppress_type_checking_errors: bool,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Component {
    /// List components.
    List {
        /// Show component imports
        #[arg(short, long)]
        imports: bool,
        /// Output as JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
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
        location: oci_client::Reference,
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
    /// Disable structured log output.
    Off,
}

/// Log stream type for `execution logs`.
/// One of: `stdout`, `stderr`, `none`.
#[derive(Debug, Clone, Copy, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum LogStreamTypeArg {
    Stdout,
    Stderr,
    None,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Execution {
    /// List recent executions.
    List {
        /// Filter by function FFQN prefix.
        /// Accepts any prefix of a fully-qualified function name, e.g.: `namespace:`.
        /// Short `.../ifc.fn` form is not supported.
        #[arg(long = "ffqn", value_name = "FFQN_PREFIX")]
        ffqn_prefix: Option<String>,
        /// Filter by execution id prefix.
        /// Useful to find all child executions if --show-derived flag is enabled.
        #[arg(long = "execution-id", short, value_name = "EXECUTION_ID_PREFIX")]
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
        /// Execution ID to fetch logs for.
        #[arg(value_name = "EXECUTION_ID")]
        execution_id: ExecutionId,
        /// Include logs from child (derived) executions.
        #[arg(long)]
        show_derived: bool,
        /// Minimum log level: trace, debug, info, warn, error, off.
        /// `off` disables structured log output entirely.
        #[arg(long, value_name = "LEVEL", default_value = "debug")]
        level: LogLevelArg,
        /// Select which stream output to show: stdout, stderr, none.
        /// If not specified, both stdout and stderr are shown.
        /// Use `none` to hide all stream output.
        #[arg(long = "stream-type", value_name = "TYPE")]
        stream_type: Option<LogStreamTypeArg>,
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
        /// Output one JSON log record per line (NDJSON).
        #[arg(short, long)]
        json: bool,
    },
    /// Show the execution event log (history).
    Events {
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
        /// Use this explicit execution ID instead of having the server assign one. Useful for idempotency. See `obelisk generate execution-id`.
        #[arg(short, long)]
        execution_id: Option<ExecutionId>,
        /// Function to invoke, either as a fully qualified name (`ns:pkg/ifc.fn`)
        /// or shortened to `.../ifc.fn` when the interface name is unambiguous.
        #[arg(value_name = "FUNCTION")]
        ffqn: FunctionFqnOrShort,
        /// Follow the stream of events until the execution finishes.
        #[arg(short, long)]
        follow: bool,
        /// Follow logs until the execution finishes, then print its result.
        #[arg(long)]
        follow_logs: bool,
        /// Include logs from child (derived) executions while following logs.
        #[arg(long, requires = "follow_logs")]
        show_derived: bool,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long)]
        no_reconnect: bool,
        /// Create the execution in paused state so it won't run until explicitly unpaused or advanced.
        #[arg(long)]
        paused: bool,
        /// Output Web API JSON, using NDJSON when following.
        #[arg(short, long)]
        json: bool,
        /// Accepted Parameter Formats:
        ///
        /// - JSON array string, e.g. '["first", "second", null, 1]'
        ///
        /// - File reference prefixed with @, e.g. @file.json (file must contain a valid JSON array)
        ///
        /// - `-` to read a JSON array from stdin
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
    /// Get the current state of an execution, optionally following status changes.
    #[command(alias = "get")]
    Status {
        /// Follow the status stream until the execution finishes.
        #[arg(short, long)]
        follow: bool,
        /// Execution ID to look up.
        execution_id: ExecutionId,
        /// Do not attempt to reconnect on connection error while following the status stream.
        #[arg(long, requires = "follow")]
        no_reconnect: bool,
        /// Output JSON, using NDJSON when following.
        #[arg(short, long)]
        json: bool,
    },
    /// Get the final result of an execution.
    Result {
        /// Follow until the execution finishes and the final result is available.
        #[arg(short, long)]
        follow: bool,
        /// Execution ID to look up.
        execution_id: ExecutionId,
        /// Do not attempt to reconnect on connection error while following the execution.
        #[arg(long, requires = "follow")]
        no_reconnect: bool,
        /// Output JSON, using NDJSON when following.
        #[arg(short, long)]
        json: bool,
    },
    /// Request cancellation of a running activity or pending delay.
    Cancel(CancelCommand),
    /// Pause a workflow execution or a pending delay.
    Pause {
        /// Execution ID (`E_01`...) or delay ID (`Delay_01`...) to pause.
        #[arg(value_name = "ID")]
        id: ExecutionIdOrDelayId,
        /// Output the outcome as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Resume a previously paused workflow execution or delay.
    Unpause {
        /// Execution ID (`E_01`...) or delay ID (`Delay_01`...) to unpause.
        #[arg(value_name = "ID")]
        id: ExecutionIdOrDelayId,
        /// Output the outcome as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Replay a workflow execution from its execution log, checking for non-determinism.
    Replay {
        /// Execution ID to replay.
        execution_id: ExecutionId,
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
    },
    /// Replay a workflow and persist its call-site backtraces.
    PersistBacktraces {
        /// Execution ID whose backtraces should be persisted.
        execution_id: ExecutionId,
    },
    /// Advance a paused workflow execution by replaying it and applying the next captured writes.
    Advance {
        /// Execution ID to advance.
        execution_id: ExecutionId,
        /// Output as JSON instead of human-readable text.
        #[arg(short, long)]
        json: bool,
        /// Send only the first N captured writes from replay to advance.
        #[arg(long)]
        trim: Option<usize>,
        /// Pause both replayed child executions and delay requests when applying the advance.
        #[arg(short = 'p', long = "pause-all", visible_alias = "pause")]
        pause_all: bool,
        /// Rewrite replayed submitted executions so they are created paused when advance is applied.
        #[arg(long)]
        pause_submitted_executions: bool,
        /// Rewrite replayed delay requests so they are created paused when advance is applied.
        #[arg(long)]
        pause_delays: bool,
        /// Advance even if replay finishes with an execution failure, persisting it.
        #[arg(long)]
        force: bool,
    },
    /// Upgrade a workflow execution to the current component version in the active deployment.
    ///
    /// Looks up the execution's FFQN, finds the component that exports it in the active
    /// deployment, and upgrades the execution from its current component digest to the new one.
    Upgrade {
        /// Execution ID to upgrade.
        execution_id: ExecutionId,
        /// Skip the determinism check during upgrade.
        #[arg(long)]
        skip_determinism_check: bool,
        /// Output the outcome as JSON.
        #[arg(long)]
        json: bool,
    },
}

pub(crate) mod params {
    use clap::error::ErrorKind;
    use serde_json::Value;
    use std::io::Read as _;

    pub(crate) fn parse_params(params: Vec<String>) -> Result<Vec<serde_json::Value>, clap::Error> {
        match params.as_slice() {
            [] => Ok(vec![]),
            [stdin] if stdin == "-" => {
                let mut json_array = String::new();
                std::io::stdin()
                    .read_to_string(&mut json_array)
                    .map_err(|err| {
                        clap::Error::raw(
                            ErrorKind::Io,
                            format!("parameter parsing failed: failed to read stdin: {err}"),
                        )
                    })?;
                parse_json_array(&json_array)
            }
            [json_array] if !dashdash() => {
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
                    json_array.clone()
                };
                parse_json_array(&json_array)
            }
            _ => {
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
    }

    fn parse_json_array(json_array: &str) -> Result<Vec<Value>, clap::Error> {
        let json_value = serde_json::from_str(json_array).map_err(|err| {
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
    /// Execution ID of the stub execution waiting for its return value.
    #[arg(value_name = "EXECUTION_ID")]
    pub(crate) execution_id: ExecutionIdDerived,

    /// Stub a return value encoded as JSON
    #[arg(value_name = "RETURN_VAL")]
    pub(crate) return_value: String,
}

/// An ID that is either an execution ID or a delay ID.
#[derive(Debug, Clone)]
pub(crate) enum ExecutionIdOrDelayId {
    Execution(ExecutionId),
    Delay(DelayId),
}

impl std::str::FromStr for ExecutionIdOrDelayId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(execution_id) = s.parse::<ExecutionId>() {
            Ok(Self::Execution(execution_id))
        } else if let Ok(delay_id) = s.parse::<DelayId>() {
            Ok(Self::Delay(delay_id))
        } else {
            Err(format!(
                "cannot parse `{s}` as an execution ID or a delay ID"
            ))
        }
    }
}

#[derive(Debug, clap::Args)]
#[command()]
#[expect(clippy::doc_markdown)]
pub(crate) struct CancelCommand {
    /// Execution id of an activity (E_01...) or a delay request (Delay_01...)
    #[arg(value_name = "ID")]
    pub(crate) id: ExecutionIdOrDelayId,
    /// Output the outcome as JSON.
    #[arg(long)]
    pub(crate) json: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deployment_sources_default_to_project_file_without_overriding_empty_or_ids() {
        use clap::Parser as _;
        let parse = |prefix: &[&str], action: &str, suffix: &[&str]| {
            let args = Args::try_parse_from(
                prefix
                    .iter()
                    .copied()
                    .chain([action])
                    .chain(suffix.iter().copied()),
            )
            .unwrap();
            match args.command {
                Subcommand::Deployment(DeploymentArgs { command, .. })
                | Subcommand::Cloud(CloudArgs {
                    command:
                        Cloud::App(CloudAppTarget {
                            command: CloudApp::Deployment(command),
                            ..
                        }),
                    ..
                }) => command,
                _ => panic!("expected deployment"),
            }
        };
        for prefix in [
            vec!["obelisk", "deployment"],
            vec!["obelisk", "cloud", "app", "deployment"],
        ] {
            for action in ["submit", "enqueue", "apply"] {
                for suffix in [vec![], vec!["--empty"], vec!["other.toml"]] {
                    let command = parse(&prefix, action, &suffix);
                    let (path, empty) = match command {
                        Deployment::Submit { file, empty, .. } => (file, empty),
                        Deployment::Apply { source, empty, .. }
                        | Deployment::Enqueue { source, empty, .. } => (
                            source.map(|source| match source {
                                DeploymentSource::File(path) => path,
                                DeploymentSource::Id(_) => panic!("expected file"),
                            }),
                            empty,
                        ),
                        _ => panic!("expected submission"),
                    };
                    assert_eq!(empty, suffix == ["--empty"]);
                    assert_eq!(
                        path,
                        if empty {
                            None
                        } else {
                            Some(PathBuf::from(
                                suffix.first().copied().unwrap_or("deployment.toml"),
                            ))
                        }
                    );
                }
                assert!(
                    Args::try_parse_from(prefix.iter().copied().chain([
                        action,
                        "other.toml",
                        "--empty"
                    ]))
                    .is_err()
                );
                if action != "submit" {
                    let id = concepts::prefixed_ulid::DeploymentId::generate().to_string();
                    let command = parse(&prefix, action, &[&id]);
                    assert!(matches!(
                        command,
                        Deployment::Apply {
                            source: Some(DeploymentSource::Id(_)),
                            ..
                        } | Deployment::Enqueue {
                            source: Some(DeploymentSource::Id(_)),
                            ..
                        }
                    ));
                }
            }
        }
    }

    #[test]
    fn cli_command_tree_is_valid() {
        <Args as clap::CommandFactory>::command().debug_assert();
    }

    #[test]
    fn deployment_verify_accepts_only_deployment_inputs() {
        for prefix in [vec!["obelisk"], vec!["obelisk", "cloud", "app"]] {
            let args = Args::try_parse_from(prefix.into_iter().chain([
                "deployment",
                "verify",
                "-d",
                "deployment.toml",
                "--api-url",
                "https://cloud.example.test",
            ]))
            .unwrap();
            let verify = match args.command {
                Subcommand::Deployment(args) => {
                    assert_eq!(args.token.api_url, "https://cloud.example.test");
                    let Deployment::Verify(verify) = args.command else {
                        panic!()
                    };
                    verify
                }
                Subcommand::Cloud(args) => {
                    assert_eq!(args.api_url.as_deref(), Some("https://cloud.example.test"));
                    let Cloud::App(CloudAppTarget {
                        command: CloudApp::Deployment(Deployment::Verify(verify)),
                        ..
                    }) = args.command
                    else {
                        panic!()
                    };
                    verify
                }
                _ => panic!(),
            };
            assert_eq!(verify.deployment, PathBuf::from("deployment.toml"));
        }
    }

    #[test]
    fn deployment_verify_rejects_policy_and_repair_flags() {
        for flag in [
            "--app-config",
            "--server-config",
            "--fix",
            "--fix-app-exec-permissions",
            "--allow-unavailable-runtime-config",
        ] {
            assert!(
                Args::try_parse_from([
                    "obelisk",
                    "deployment",
                    "verify",
                    "-d",
                    "deployment.toml",
                    flag
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn direct_connection_flags_work_before_and_after_subcommands() {
        for command in [
            vec!["deployment", "--api-url", "http://example.test", "list"],
            vec!["deployment", "list", "--api-url", "http://example.test"],
        ] {
            let args = Args::try_parse_from(std::iter::once("obelisk").chain(command)).unwrap();
            let Subcommand::Deployment(args) = args.command else {
                panic!()
            };
            assert_eq!(args.token.api_url, "http://example.test");
        }
    }

    #[test]
    fn server_api_token_must_not_be_empty() {
        let err =
            Args::try_parse_from(["obelisk", "server", "run", "--api-token", ""]).unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(err.to_string().contains("API token must not be empty"));
    }

    #[test]
    fn execution_submit_follow_logs_accepts_no_reconnect() {
        let args = Args::try_parse_from([
            "obelisk",
            "execution",
            "submit",
            "--follow-logs",
            "--no-reconnect",
            "--show-derived",
            "example:pkg/interface.function",
        ])
        .unwrap();

        let Subcommand::Execution(ExecutionArgs {
            command:
                Execution::Submit {
                    follow,
                    follow_logs,
                    show_derived,
                    no_reconnect,
                    ..
                },
            ..
        }) = args.command
        else {
            panic!("expected execution submit");
        };
        assert!(!follow);
        assert!(follow_logs);
        assert!(no_reconnect);
        assert!(show_derived);

        let err = Args::try_parse_from([
            "obelisk",
            "execution",
            "submit",
            "--show-derived",
            "example:pkg/interface.function",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn admin_plural_delete_accepts_multiple_ids() {
        let first = ExecutionId::generate();
        let second = ExecutionId::generate();
        let args = Args::try_parse_from([
            "obelisk".to_owned(),
            "admin".to_owned(),
            "executions".to_owned(),
            "delete".to_owned(),
            first.to_string(),
            second.to_string(),
            "--force".to_owned(),
        ])
        .unwrap();

        let Subcommand::Admin(AdminArgs {
            command:
                Admin::Executions(AdminExecutions::Delete {
                    execution_ids,
                    force,
                    ..
                }),
            ..
        }) = args.command
        else {
            panic!("expected plural admin execution deletion");
        };
        assert_eq!(execution_ids, [first, second]);
        assert!(force);

        let first = DeploymentId::generate();
        let second = DeploymentId::generate();
        let args = Args::try_parse_from([
            "obelisk".to_owned(),
            "admin".to_owned(),
            "deployments".to_owned(),
            "delete".to_owned(),
            first.to_string(),
            second.to_string(),
            "--delete-executions".to_owned(),
            "--force".to_owned(),
        ])
        .unwrap();
        let Subcommand::Admin(AdminArgs {
            command:
                Admin::Deployments(AdminDeployments::Delete {
                    deployment_ids,
                    delete_executions,
                    force,
                    ..
                }),
            ..
        }) = args.command
        else {
            panic!("expected plural admin deployment deletion");
        };
        assert_eq!(deployment_ids, [first, second]);
        assert!(delete_executions);
        assert!(force);

        let args = Args::try_parse_from([
            "obelisk",
            "admin",
            "deployments",
            "retain",
            "--count",
            "10",
            "--delete-executions",
            "--force",
        ])
        .unwrap();
        let Subcommand::Admin(AdminArgs {
            command:
                Admin::Deployments(AdminDeployments::Retain {
                    count,
                    delete_executions,
                    force,
                    ..
                }),
            ..
        }) = args.command
        else {
            panic!("expected deployment retention");
        };
        assert_eq!(count, Some(10));
        assert!(delete_executions);
        assert!(force);

        assert!(
            Args::try_parse_from([
                "obelisk",
                "admin",
                "deployments",
                "retain",
                "--count",
                "10",
                "--force",
            ])
            .is_err()
        );
    }

    #[test]
    fn admin_retention_accepts_exactly_one_selector() {
        let args = Args::try_parse_from([
            "obelisk",
            "admin",
            "executions",
            "retain",
            "--max-age",
            "30d",
        ])
        .unwrap();
        let Subcommand::Admin(AdminArgs {
            command: Admin::Executions(AdminExecutions::Retain { count, max_age, .. }),
            ..
        }) = args.command
        else {
            panic!("expected execution retention");
        };
        assert_eq!(count, None);
        assert_eq!(max_age, Some(std::time::Duration::from_hours(30 * 24)));
        assert!(Args::try_parse_from(["obelisk", "admin", "executions", "retain"]).is_err());
        assert!(
            Args::try_parse_from([
                "obelisk",
                "admin",
                "executions",
                "retain",
                "--count",
                "10",
                "--max-age",
                "30d",
            ])
            .is_err()
        );
    }

    #[test]
    fn admin_singular_resource_group_is_rejected() {
        let err = Args::try_parse_from([
            "obelisk",
            "admin",
            "execution",
            "delete",
            &ExecutionId::generate().to_string(),
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }
}

#[derive(Debug, clap::Args)]
pub(crate) struct CloudArgs {
    /// Print machine-readable JSON instead of human-readable output.
    #[arg(long, global = true)]
    pub(crate) json: bool,
    /// Show detailed metadata, HTTP requests, and response statuses on stderr.
    #[arg(long, global = true)]
    pub(crate) verbose: bool,
    /// Cloud control plane URL.
    #[arg(long, global = true, env = "OBELISK_CLOUD_API_URL", default_value = "https://cloud.obeli.sk", value_parser = |value: &str| value.parse::<reqwest::Url>().map(|_| value.to_owned()))]
    pub(crate) api_url: Option<String>,
    /// Obelisk cloud apps' domain; app endpoints are `<app>.<domain>`, `api--<app>.<domain>` and `webui--<app>.<domain>`.
    #[arg(
        long,
        global = true,
        env = "OBELISK_CLOUD_APPS_DOMAIN",
        default_value = "obelisk.works"
    )]
    pub(crate) apps_domain: String,
    /// Cloud API bearer token.
    #[arg(long, global = true, env = "OBELISK_CLOUD_API_TOKEN", hide_env_values = true, value_parser = parse_secret_string)]
    pub(crate) api_token: Option<SecretString>,
    #[command(subcommand)]
    pub(crate) command: Cloud,
}

#[derive(Debug, clap::Args)]
pub(crate) struct CloudAppTarget {
    /// Selected cloud app name (4-20 characters, no double hyphens); defaults to `app_name` in ./app.toml.
    #[arg(long, global = true, env = "OBELISK_APP_NAME", value_name = "NAME")]
    pub(crate) app: Option<String>,
    #[command(subcommand)]
    pub(crate) command: CloudApp,
}

#[derive(Debug, clap::Args)]
pub(crate) struct CloudCommandId {
    /// Reuse this root execution ID when retrying the command.
    #[arg(long = "command-id", value_name = "EXECUTION_ID")]
    pub(crate) id: Option<ExecutionIdTopLevel>,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum Cloud {
    /// Create, inspect and revoke cloud bearer tokens.
    #[command(subcommand)]
    Token(CloudToken),
    /// List apps owned by the authenticated user.
    #[command(subcommand)]
    Apps(CloudApps),
    /// Manage an app and its deployments, executions and configuration.
    App(CloudAppTarget),
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum CloudApps {
    List,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum CloudToken {
    /// Create a token; defaults to account-wide access, including future apps.
    New {
        #[arg(long)]
        name: String,
        /// Save the server-issued token in a new private file. Lost responses require revocation and replacement.
        #[arg(long)]
        token_file: PathBuf,
        /// Restrict access to this app slug; repeat for multiple apps.
        #[arg(long, value_name = "APP_NAME", value_parser = clap::builder::NonEmptyStringValueParser::new())]
        scope: Vec<String>,
        /// Absolute expiry in RFC 3339 format.
        #[arg(long)]
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    },
    List,
    Get {
        id: String,
    },
    Revoke {
        id: String,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum CloudApp {
    /// Manage deployments through the selected app's API.
    #[command(subcommand)]
    Deployment(Deployment),
    /// Manage executions through the selected app's API.
    #[command(subcommand)]
    Execution(Execution),
    /// Inspect components or manage OCI artifacts.
    #[command(subcommand)]
    Component(Component),
    /// Perform operator-only destructive maintenance on the selected app.
    #[command(subcommand)]
    Admin(Admin),
    /// Create a stopped app; apply explicitly to start it.
    New {
        /// App name; falls back to --app, the policy file's `app_name`, then ./app.toml.
        name: Option<String>,
        /// App policy path; defaults to ./app.toml if present.
        #[arg(short = 'a', long)]
        app_config: Option<PathBuf>,
        /// Prompt for initial env vars and secrets; requires a terminal.
        #[arg(long)]
        fill: bool,
        /// Read initial env vars and secrets from same-named environment variables.
        #[arg(long, conflicts_with = "fill")]
        fill_non_interactive: bool,
        #[command(flatten)]
        command_id: CloudCommandId,
    },
    /// Inspect the app row.
    Get,
    /// Inspect management, Machine and target API health.
    Status,
    /// Stage app policy and cloud settings without restarting the VM.
    Stage {
        /// App policy path; defaults to ./app.toml.
        #[arg(short = 'a', long, default_value = "app.toml")]
        app_config: PathBuf,
        #[command(flatten)]
        command_id: CloudCommandId,
    },
    /// Publish the local project, creating or configuring the app and activating its deployment.
    Deploy {
        /// App configuration path.
        #[arg(short = 'a', long, default_value = "app.toml")]
        app_config: PathBuf,
        /// Deployment manifest path.
        #[arg(short = 'd', long, default_value = "deployment.toml")]
        deployment: PathBuf,
        /// Run `fill` first; otherwise missing required env vars and secrets are an error.
        #[arg(long)]
        fill: bool,
        /// Run `fill --non-interactive` first.
        #[arg(long, conflicts_with = "fill")]
        fill_non_interactive: bool,
    },
    /// Apply the staged configuration and the enqueued (or active) deployment.
    Apply {
        /// App policy path compared with the staged configuration; defaults to ./app.toml if present.
        #[arg(short = 'a', long)]
        app_config: Option<PathBuf>,
        /// Use the staged configuration even when the local app config differs.
        #[arg(long, conflicts_with = "app_config")]
        staged: bool,
        /// Run `fill` first; otherwise missing required env vars and secrets are an error.
        #[arg(long)]
        fill: bool,
        /// Run `fill --non-interactive` first.
        #[arg(long, conflicts_with = "fill")]
        fill_non_interactive: bool,
        /// Reapply even with a clear stale flag, including uncertain secret changes.
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        command_id: CloudCommandId,
    },
    Enable {
        #[command(flatten)]
        command_id: CloudCommandId,
    },
    Disable {
        #[command(flatten)]
        command_id: CloudCommandId,
    },
    Delete {
        #[command(flatten)]
        command_id: CloudCommandId,
    },
    /// Interactively stage declared env vars and secrets, offering process environment values.
    Fill {
        /// App policy path compared with the staged configuration; defaults to ./app.toml if present.
        #[arg(short = 'a', long)]
        app_config: Option<PathBuf>,
        /// Use the staged configuration even when the local app config differs.
        #[arg(long, conflicts_with = "app_config")]
        staged: bool,
        /// Stage values of same-named environment variables without prompting.
        #[arg(long)]
        non_interactive: bool,
    },
    #[command(subcommand)]
    Env(CloudEnv),
    #[command(subcommand)]
    Secrets(CloudSecrets),
    /// Show all endpoints, or select the API, webhook or Web UI URL.
    Endpoint {
        #[command(subcommand)]
        command: Option<CloudEndpoint>,
    },
}

#[derive(Debug, Clone, Copy, clap::Subcommand)]
pub(crate) enum CloudEndpoint {
    Api,
    Webhook,
    Webui,
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum CloudEnv {
    List,
    Set {
        name: String,
        value: String,
        #[command(flatten)]
        command_id: CloudCommandId,
    },
    Unset {
        name: String,
        #[command(flatten)]
        command_id: CloudCommandId,
    },
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum CloudSecrets {
    /// List metadata; secret values are never returned.
    List,
    Set {
        name: String,
        /// Read the value from a process environment variable.
        #[arg(long, conflicts_with = "stdin", required_unless_present = "stdin")]
        from_env: Option<String>,
        /// Read the exact value from stdin, including trailing newlines.
        #[arg(long, conflicts_with = "from_env")]
        stdin: bool,
    },
    Unset {
        name: String,
    },
}
