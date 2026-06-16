use super::{config_holder::PathPrefixes, env_var::EnvVarConfig};
use crate::args::TomlComponentType;
use crate::command::server::FrameFilesToSourceContent;
use crate::config::config_holder::{CACHE_DIR_PREFIX, DATA_DIR_PREFIX};
use crate::config::env_var::{
    EnvVarMissing, EnvVarsMissing, interpolate_env_vars_plaintext, interpolate_env_vars_secret,
};
use crate::config::toml::cron::CronComponentConfigToml;
use crate::config::wasm_cache_metadata_dir;
use crate::oci;
use anyhow::{Context, ensure};
use anyhow::{anyhow, bail};
use concepts::ContentDigest;
use concepts::ReturnType;
use concepts::component_id::Digest;
use concepts::{
    ComponentId, ComponentRetryConfig, ComponentType, FunctionFqn, StrVariant,
    component_id::ComponentDigest, prefixed_ulid::ExecutorId, storage::LogLevel,
};
use db_postgres::postgres_dao::{self, PostgresConfig};
use db_sqlite::sqlite_dao::SqliteConfig;
use hashbrown::HashMap;
use log::{LoggingConfig, LoggingStyle};
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use sha2::{Digest as _, Sha256};
use std::fmt::Display;
use std::str::FromStr;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tracing::{debug, instrument, warn};
use utils::wasm_tools::WasmComponent;
use wasm_workers::activity::activity_exec_worker::ExecProgram;
use wasm_workers::cron::cron_worker::CronOrOnce;
use wasm_workers::http_hooks::ConfigSectionHint;
use wasm_workers::http_request_policy::HostPatternError;
use wasm_workers::{
    activity::activity_worker::ActivityConfig,
    envvar::EnvVar,
    http_request_policy::{AllowedHostConfig, HostPattern, MethodsPattern, ReplacementLocation},
    std_output_stream::StdOutputConfig,
    workflow::workflow_worker::{
        DEFAULT_NON_BLOCKING_EVENT_BATCHING, JoinNextBlockingStrategy, WorkflowConfig,
    },
};
use webhook::{HttpServer, WebhookJsComponentConfigToml, WebhookWasmComponentConfigToml};

pub(crate) use deployment_config::config::{
    ActivityExecComponentConfigCanonical, ActivityExternalComponentConfigCanonical,
    ActivityExternalFileConfigToml, ActivityJsComponentConfigCanonical,
    ActivityStubComponentConfigCanonical, ActivityStubExtInlineConfigCanonical,
    ActivityStubFileConfigToml, ActivityWasmComponentConfigToml, AllowedHostToml,
    BacktraceSourceCanonical, BlockingStrategyConfigToml, ComponentBacktraceConfigCanonical,
    ComponentCommon, ComponentLocationToml, ComponentStdOutputToml, ConfigName,
    DeploymentCanonical, DurationConfig, DurationConfigOptional, ExecConfigToml, ExecSecretsToml,
    InflightSemaphore, JsParamToml, LockingStrategy, LogLevelToml, MethodsInput, MethodsInputStar,
    OCI_SCHEMA_PREFIX, ReplaceIn, ScriptLocationCanonical, Unlimited,
    WorkflowJsComponentConfigCanonical, WorkflowWasmComponentConfigCanonical,
    default_lock_extension, default_max_output_bytes, default_max_retries,
    default_retry_exp_backoff,
};

const DEFAULT_SQLITE_DIR_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}obelisk-sqlite", DATA_DIR_PREFIX);
const DEFAULT_SQLITE_DIR: &str = "obelisk-sqlite";
pub(crate) const SQLITE_FILE_NAME: &str = "obelisk.sqlite";
const DEFAULT_WASM_DIRECTORY_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}wasm", CACHE_DIR_PREFIX);
const DEFAULT_WASM_DIRECTORY: &str = "cache/wasm";
const DEFAULT_CODEGEN_CACHE_DIRECTORY_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}codegen", CACHE_DIR_PREFIX);
const DEFAULT_CODEGEN_CACHE_DIRECTORY: &str = "cache/codegen";

#[derive(Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeploymentToml {
    #[serde(default, rename = "activity_wasm")]
    pub(crate) activities_wasm: Vec<ActivityWasmComponentConfigToml>,
    #[serde(default, rename = "activity_stub")]
    pub(crate) activities_stub: Vec<ActivityStubComponentConfigToml>,
    #[serde(default, rename = "activity_external")]
    pub(crate) activities_external: Vec<ActivityExternalComponentConfigToml>,
    #[serde(default, rename = "activity_js")]
    pub(crate) activities_js: Vec<ActivityJsComponentConfigToml>,
    #[serde(default, rename = "activity_exec")]
    pub(crate) activities_exec: Vec<ActivityExecComponentConfigToml>,
    #[serde(default, rename = "workflow_wasm")]
    pub(crate) workflows_wasm: Vec<WorkflowWasmComponentConfigToml>,
    #[serde(default, rename = "workflow_js")]
    pub(crate) workflows_js: Vec<WorkflowJsComponentConfigToml>,
    #[serde(default, rename = "webhook_endpoint_wasm")]
    pub(crate) webhooks_wasm: Vec<WebhookWasmComponentConfigToml>,
    #[serde(default, rename = "webhook_endpoint_js")]
    pub(crate) webhooks_js: Vec<WebhookJsComponentConfigToml>,
    #[serde(default, rename = "cron")]
    pub(crate) crons: Vec<CronComponentConfigToml>,
}

/// A `DeploymentToml` that has passed name-uniqueness validation.
///
/// Components that support auto-derived names (`activity_js`, `activity_exec`,
/// `workflow_js`) are stored as `(Config, ConfigName)` tuples with the resolved name
/// pulled out of the `Option`.
#[derive(Default)]
pub(crate) struct DeploymentTomlValidated {
    pub(crate) activities_exec: Vec<(ActivityExecComponentConfigToml, ConfigName)>,
    pub(crate) activities_external: Vec<(ActivityExternalComponentConfigToml, ConfigName)>,
    pub(crate) activities_js: Vec<(ActivityJsComponentConfigToml, ConfigName)>,
    pub(crate) activities_stub: Vec<(ActivityStubComponentConfigToml, ConfigName)>,
    pub(crate) activities_wasm: Vec<ActivityWasmComponentConfigToml>,

    pub(crate) workflows_js: Vec<(WorkflowJsComponentConfigToml, ConfigName)>,
    pub(crate) workflows_wasm: Vec<WorkflowWasmComponentConfigToml>,

    pub(crate) webhooks_js: Vec<WebhookJsComponentConfigToml>,
    pub(crate) webhooks_wasm: Vec<WebhookWasmComponentConfigToml>,

    pub(crate) crons: Vec<CronComponentConfigToml>,

    pub(crate) component_names_to_types: hashbrown::HashMap<String, crate::args::TomlComponentType>,

    /// Canonicalized deployment directory, used to decide whether a script path is
    /// owned by the deployment (under this dir) or an external reference.
    pub(crate) deployment_dir: PathBuf,
}
impl DeploymentTomlValidated {
    pub(crate) async fn canonicalize(self) -> Result<DeploymentCanonical, anyhow::Error> {
        resolve_local_refs_to_canonical(self).await
    }
}

impl DeploymentToml {
    /// Expand `${DEPLOYMENT_DIR}/` prefixes in WASM component paths,
    /// verify that every component name is unique, and return a `DeploymentTomlValidated`
    /// that also carries the name→type index and the deployment directory.
    pub(crate) fn validate(
        mut self,
        deployment_dir: &std::path::Path,
    ) -> Result<DeploymentTomlValidated, anyhow::Error> {
        self.expand_deployment_dir_prefix(deployment_dir)?;
        self.normalize_oci_locations()?;

        // Build the name→type index and check for duplicates.
        let mut component_names_to_types = hashbrown::HashMap::new();
        // Add components with mandatory names
        let iter = self
            .activities_wasm
            .iter()
            .map(|c| (c.common.name.as_str(), TomlComponentType::ActivityWasm))
            .chain(
                self.workflows_wasm
                    .iter()
                    .map(|c| (c.common.name.as_str(), TomlComponentType::WorkflowWasm)),
            )
            .chain(self.webhooks_wasm.iter().map(|c| {
                (
                    c.common.name.as_str(),
                    TomlComponentType::WebhookEndpointWasm,
                )
            }))
            .chain(
                self.webhooks_js
                    .iter()
                    .map(|c| (c.name.as_str(), TomlComponentType::WebhookEndpointJs)),
            )
            .chain(
                self.crons
                    .iter()
                    .map(|c| (c.name.as_str(), TomlComponentType::Cron)),
            );

        for (name, component_type) in iter {
            if component_names_to_types
                .insert(name.to_string(), component_type)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }

        let activities_js = Self::resolve_names(self.activities_js);
        let activities_exec = Self::resolve_names(self.activities_exec);
        let activities_stub = Self::resolve_stub_names(self.activities_stub);
        let activities_external = Self::resolve_external_names(self.activities_external);
        let workflows_js = Self::resolve_names(self.workflows_js);

        // Add components with optional names (now resolved)

        for (_, name) in &activities_js {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityJs)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_exec {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityExec)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_stub {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityStub)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_external {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityExternal)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &workflows_js {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::WorkflowJs)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        Ok(DeploymentTomlValidated {
            activities_exec,
            activities_external,
            activities_js,
            activities_stub,
            activities_wasm: self.activities_wasm,

            workflows_js,
            workflows_wasm: self.workflows_wasm,

            webhooks_js: self.webhooks_js,
            webhooks_wasm: self.webhooks_wasm,

            crons: self.crons,

            component_names_to_types,
            deployment_dir: deployment_dir.to_path_buf(),
        })
    }

    // Resolve optional names from FFQN.
    fn resolve_names<T: HasOptionalNameAndFfqn>(configs: Vec<T>) -> Vec<(T, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = c
                    .config_name()
                    .cloned()
                    .unwrap_or_else(|| ConfigName::from_ffqn(c.ffqn()));
                (c, name)
            })
            .collect()
    }

    /// Resolve names for `ActivityStubComponentConfigToml` enum variants.
    /// File variants always have an explicit name; Inline variants may derive from FFQN.
    fn resolve_stub_names(
        configs: Vec<ActivityStubComponentConfigToml>,
    ) -> Vec<(ActivityStubComponentConfigToml, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = match &c {
                    ActivityStubComponentConfigToml::File(f) => f.common.name.clone(),
                    ActivityStubComponentConfigToml::Inline(i) => i
                        .name
                        .clone()
                        .unwrap_or_else(|| ConfigName::from_ffqn(&i.ffqn)),
                };
                (c, name)
            })
            .collect()
    }

    /// Resolve names for `ActivityExternalComponentConfigToml` enum variants.
    fn resolve_external_names(
        configs: Vec<ActivityExternalComponentConfigToml>,
    ) -> Vec<(ActivityExternalComponentConfigToml, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = match &c {
                    ActivityExternalComponentConfigToml::File(f) => f.common.name.clone(),
                    ActivityExternalComponentConfigToml::Inline(i) => i
                        .name
                        .clone()
                        .unwrap_or_else(|| ConfigName::from_ffqn(&i.ffqn)),
                };
                (c, name)
            })
            .collect()
    }

    /// Resolve a WASM component file path to an absolute path. A `${DEPLOYMENT_DIR}/<suffix>`
    /// path and a bare relative path are both anchored to the deployment directory and must
    /// stay within it (no `..` escape); an absolute path is left untouched (out-of-tree
    /// reference). This makes every relative path in a deployment.toml deployment-relative.
    fn expand_deployment_dir(
        s: &mut String,
        deployment_dir: &std::path::Path,
    ) -> anyhow::Result<()> {
        // A `${DEPLOYMENT_DIR}/x` path and a bare relative `x` are equivalent; only an
        // absolute path is left untouched (out-of-tree reference).
        let candidate = strip_deployment_dir_prefix(s).unwrap_or(s.as_str());
        if std::path::Path::new(candidate).is_absolute() {
            return Ok(());
        }
        let rel = normalize_owned_subpath(candidate)
            .with_context(|| format!("invalid deployment-relative path `{s}`"))?;
        *s = deployment_dir.join(rel).to_string_lossy().into_owned();
        Ok(())
    }

    /// Validate and normalize OCI references of WASM components so that the canonical
    /// form matches the previous `oci_client::Reference`-based serialization.
    fn normalize_oci_locations(&mut self) -> Result<(), anyhow::Error> {
        fn normalize(loc: &mut ComponentLocationToml) -> Result<(), anyhow::Error> {
            if let ComponentLocationToml::Oci(image) = loc {
                let reference = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow!("invalid OCI reference `{image}`: {e}"))?;
                *image = reference.to_string();
            }
            Ok(())
        }
        for c in &mut self.activities_wasm {
            normalize(&mut c.common.location)?;
        }
        for c in &mut self.activities_stub {
            if let ActivityStubComponentConfigToml::File(c) = c {
                normalize(&mut c.common.location)?;
            }
        }
        for c in &mut self.activities_external {
            if let ActivityExternalComponentConfigToml::File(c) = c {
                normalize(&mut c.common.location)?;
            }
        }
        for c in &mut self.workflows_wasm {
            normalize(&mut c.common.location)?;
        }
        for c in &mut self.webhooks_wasm {
            normalize(&mut c.common.location)?;
        }
        Ok(())
    }

    /// Expand `${DEPLOYMENT_DIR}` prefixes in WASM component paths (which are read lazily
    /// at runtime and therefore must be absolute in the canonical form), rejecting `..`
    /// escapes.
    fn expand_deployment_dir_prefix(
        &mut self,
        deployment_dir: &std::path::Path,
    ) -> anyhow::Result<()> {
        fn expand_loc(
            loc: &mut ComponentLocationToml,
            deployment_dir: &std::path::Path,
        ) -> anyhow::Result<()> {
            if let ComponentLocationToml::Path(p) = loc {
                DeploymentToml::expand_deployment_dir(p, deployment_dir)?;
            }
            Ok(())
        }
        for c in &mut self.activities_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        for c in &mut self.activities_stub {
            if let ActivityStubComponentConfigToml::File(c) = c {
                expand_loc(&mut c.common.location, deployment_dir)?;
            }
        }
        for c in &mut self.activities_external {
            if let ActivityExternalComponentConfigToml::File(c) = c {
                expand_loc(&mut c.common.location, deployment_dir)?;
            }
        }
        // Script (JS/exec) locations and backtrace sources are NOT expanded here. Their
        // `${DEPLOYMENT_DIR}` prefix and relative-vs-absolute classification are handled
        // when resolving to canonical (`resolve_script_toml_to_canonical` /
        // `resolve_backtrace_to_canonical`), so deployment-owned files (relative, mirrored)
        // are distinguished from external references (absolute).
        for c in &mut self.workflows_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        for c in &mut self.webhooks_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServerConfigToml {
    #[serde(default, rename = "obelisk-version")]
    pub(crate) obelisk_version: Option<String>,
    #[serde(default)]
    pub(crate) api: ApiConfig,
    #[serde(default)]
    pub(crate) database: DatabaseConfigToml,
    #[serde(default)]
    pub(crate) webui: WebUIConfig,
    #[serde(default)]
    pub(crate) external: ExternalServerConfig,
    #[serde(default, rename = "wasm")]
    pub(crate) wasm_global_config: WasmGlobalConfigToml,
    #[serde(default, rename = "workflows")]
    pub(crate) workflows_global_config: WorkflowsGlobalConfigToml,
    #[serde(default)]
    pub(crate) timers_watcher: TimersWatcherTomlConfig,
    #[serde(default)]
    pub(crate) cancel_watcher: CancelWatcherTomlConfig,
    #[cfg(feature = "otlp")]
    #[serde(default)]
    pub(crate) otlp: Option<otlp::OtlpConfig>,
    #[serde(default)]
    pub(crate) log: LoggingConfig,
    #[serde(default, rename = "http_server")]
    pub(crate) http_servers: Vec<HttpServer>,
}

/// Return a canonical JSON string of the deployment config for storage.
pub(crate) fn compute_config_json(deployment: &DeploymentCanonical) -> String {
    serde_json::to_string(deployment).expect("DeploymentCanonical is serializable")
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_api_listening_addr")]
    pub(crate) listening_addr: SocketAddr,
}
impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listening_addr: default_api_listening_addr(),
        }
    }
}
fn default_api_listening_addr() -> SocketAddr {
    "127.0.0.1:5005".parse().expect("valid default address")
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DatabaseConfigToml {
    Sqlite(SqliteConfigToml),
    Postgres(PostgresConfigToml),
}
impl DatabaseConfigToml {
    pub fn get_subscription_interruption(&self) -> Option<Duration> {
        match self {
            DatabaseConfigToml::Sqlite(_) => None,
            DatabaseConfigToml::Postgres(postgres_config_toml) => {
                postgres_config_toml.subscription_interruption.into()
            }
        }
    }
}
impl Default for DatabaseConfigToml {
    fn default() -> DatabaseConfigToml {
        DatabaseConfigToml::Sqlite(SqliteConfigToml::default())
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostgresConfigToml {
    host: String,
    user: String,
    password: String,
    db_name: String,
    /// Interrupts listening for notifications periodically, needed for Postgres with a local-only subscription mechanism.
    #[serde(default = "default_subscription_interruption")]
    pub subscription_interruption: DurationConfigOptional,
    #[serde(default)]
    provision_policy: PostgresProvisionPolicy,
}

impl PostgresConfigToml {
    pub fn as_config(&self) -> Result<PostgresConfig, anyhow::Error> {
        Ok(PostgresConfig {
            host: interpolate_env_vars_plaintext(&self.host)?,
            user: interpolate_env_vars_plaintext(&self.user)?,
            password: interpolate_env_vars_secret(&self.password)?,
            db_name: interpolate_env_vars_plaintext(&self.db_name)?,
        })
    }
    pub fn as_provision_policy(&self) -> postgres_dao::ProvisionPolicy {
        match self.provision_policy {
            PostgresProvisionPolicy::Never => postgres_dao::ProvisionPolicy::NeverCreate,
            PostgresProvisionPolicy::Auto => postgres_dao::ProvisionPolicy::Auto,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum PostgresProvisionPolicy {
    #[default]
    Never,
    /// Create database if it does not exist.
    Auto,
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct SqliteConfigToml {
    #[serde(default)]
    directory: Option<String>,
    #[serde(default = "default_sqlite_queue_capacity")]
    queue_capacity: usize,
    #[serde(default)]
    pragma: std::collections::HashMap<String, String>, // hashbrown is not supported by schemars
    #[serde(default)]
    metrics_threshold: Option<DurationConfig>,
}
impl Default for SqliteConfigToml {
    fn default() -> Self {
        Self {
            directory: None,
            queue_capacity: default_sqlite_queue_capacity(),
            pragma: std::collections::HashMap::default(),
            metrics_threshold: Option::default(),
        }
    }
}
impl SqliteConfigToml {
    pub(crate) async fn get_sqlite_dir(
        &self,
        path_prefixes: &PathPrefixes,
    ) -> Result<PathBuf, anyhow::Error> {
        let sqlite_file = self.directory.as_deref().unwrap_or_else(|| {
            if path_prefixes.project_dirs.is_some() {
                DEFAULT_SQLITE_DIR_IF_PROJECT_DIRS
            } else {
                DEFAULT_SQLITE_DIR
            }
        });
        path_prefixes
            .server_config_replace_path_prefix_mkdir(sqlite_file)
            .await
    }

    pub(crate) fn as_sqlite_config(&self) -> SqliteConfig {
        SqliteConfig {
            queue_capacity: self.queue_capacity,
            pragma_override: Some(self.pragma.clone().into_iter().collect()),
            metrics_threshold: self.metrics_threshold.map(Duration::from),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebUIConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_webui_listening_addr")]
    pub(crate) listening_addr: String,
}
impl Default for WebUIConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listening_addr: default_webui_listening_addr(),
        }
    }
}
fn default_webui_listening_addr() -> String {
    "127.0.0.1:8080".to_string()
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExternalServerConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_external_listening_addr")]
    pub(crate) listening_addr: SocketAddr,
}
impl Default for ExternalServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listening_addr: default_external_listening_addr(),
        }
    }
}
fn default_external_listening_addr() -> SocketAddr {
    "127.0.0.1:9090".parse().expect("valid default address")
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WasmGlobalConfigToml {
    #[serde(default)]
    pub(crate) codegen_cache: CodegenCache,
    #[serde(default)]
    pub(crate) backtrace: WasmGlobalBacktrace,
    #[serde(default)]
    cache_directory: Option<String>,
    #[serde(default)]
    pub(crate) allocator_config: WasmtimeAllocatorConfig,
    #[serde(default)]
    pub(crate) global_executor_instance_limiter: InflightSemaphore,
    #[serde(default)]
    pub(crate) global_webhook_instance_limiter: InflightSemaphore,
    #[serde(default)]
    pub(crate) fuel: ValueOrUnlimited<u64>,
    #[serde(default)]
    pub(crate) build_semaphore: ValueOrUnlimited<u64>,
    #[serde(default = "default_parallel_compilation")]
    pub(crate) parallel_compilation: bool,
    #[serde(default)]
    pub(crate) wasmtime_pooling_config: WasmtimePoolingAllocatorConfig,
    #[serde(default = "default_debug")]
    pub(crate) debug: bool,
}
impl Default for WasmGlobalConfigToml {
    fn default() -> Self {
        WasmGlobalConfigToml {
            codegen_cache: CodegenCache::default(),
            backtrace: WasmGlobalBacktrace::default(),
            cache_directory: Option::default(),
            allocator_config: WasmtimeAllocatorConfig::default(),
            global_executor_instance_limiter: InflightSemaphore::default(),
            global_webhook_instance_limiter: InflightSemaphore::default(),
            fuel: ValueOrUnlimited::default(),
            build_semaphore: ValueOrUnlimited::default(),
            parallel_compilation: default_parallel_compilation(),
            wasmtime_pooling_config: WasmtimePoolingAllocatorConfig::default(),
            debug: default_debug(),
        }
    }
}

impl WasmGlobalConfigToml {
    pub(crate) async fn get_wasm_cache_directory(
        &self,
        path_prefixes: &PathPrefixes,
    ) -> Result<PathBuf, anyhow::Error> {
        let wasm_directory = self.cache_directory.as_deref().unwrap_or_else(|| {
            if path_prefixes.project_dirs.is_some() {
                DEFAULT_WASM_DIRECTORY_IF_PROJECT_DIRS
            } else {
                DEFAULT_WASM_DIRECTORY
            }
        });
        path_prefixes
            .server_config_replace_path_prefix_mkdir(wasm_directory)
            .await
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WasmGlobalBacktrace {
    #[serde(default = "default_global_backtrace_persist")]
    pub(crate) persist: bool,
}

impl Default for WasmGlobalBacktrace {
    fn default() -> Self {
        Self {
            persist: default_global_backtrace_persist(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowsGlobalConfigToml {
    #[serde(default = "default_workflows_lock_extension_leeway")]
    pub(crate) lock_extension_leeway: DurationConfig,
}
impl Default for WorkflowsGlobalConfigToml {
    fn default() -> WorkflowsGlobalConfigToml {
        WorkflowsGlobalConfigToml {
            lock_extension_leeway: default_workflows_lock_extension_leeway(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodegenCache {
    #[serde(default = "default_codegen_enabled")]
    enabled: bool,
    #[serde(default)]
    directory: Option<String>,
}

impl Default for CodegenCache {
    fn default() -> Self {
        Self {
            enabled: default_codegen_enabled(),
            directory: None,
        }
    }
}

impl CodegenCache {
    pub(crate) async fn get_directory(
        &self,
        path_prefixes: &PathPrefixes,
    ) -> Result<Option<PathBuf>, anyhow::Error> {
        if self.enabled {
            let directory = self.directory.as_deref().unwrap_or_else(|| {
                if path_prefixes.project_dirs.is_some() {
                    DEFAULT_CODEGEN_CACHE_DIRECTORY_IF_PROJECT_DIRS
                } else {
                    DEFAULT_CODEGEN_CACHE_DIRECTORY
                }
            });
            path_prefixes
                .server_config_replace_path_prefix_mkdir(directory)
                .await
                .map(Some)
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimersWatcherTomlConfig {
    #[serde(default = "default_timers_watcher_enabled")]
    pub(crate) enabled: bool,

    #[serde(default = "default_timers_watcher_leeway")]
    // TODO: Add `derivative`,
    // #[derivative(Default(value = "default_timers_watcher_leeway()"))]
    pub(crate) leeway: DurationConfig,
    #[serde(default = "default_timers_watcher_tick_sleep")]
    pub(crate) tick_sleep: DurationConfig,
}
impl Default for TimersWatcherTomlConfig {
    fn default() -> Self {
        Self {
            enabled: default_timers_watcher_enabled(),
            leeway: default_timers_watcher_leeway(),
            tick_sleep: default_timers_watcher_tick_sleep(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancelWatcherTomlConfig {
    #[serde(default = "default_cancel_watcher_tick_sleep")]
    pub(crate) tick_sleep: DurationConfig,
}
impl Default for CancelWatcherTomlConfig {
    fn default() -> Self {
        Self {
            tick_sleep: default_cancel_watcher_tick_sleep(),
        }
    }
}

// Components

#[derive(Debug, Clone, Hash)]
pub(crate) struct ComponentCommonVerified {
    pub(crate) name: ConfigName,
    pub(crate) location: ComponentLocationToml,
}

pub(crate) trait ComponentLocationFetchExt {
    async fn fetch(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error>;
}

impl ComponentLocationFetchExt for ComponentLocationToml {
    /// Fetch wasm file and calculate its content digest.
    ///
    /// Read wasm file either from local fs, or pull from an OCI registry and cache it.
    async fn fetch(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
        use utils::sha256sum::calculate_sha256_file;

        debug!("Fetching {self:?}");
        let stopwatch = std::time::Instant::now();

        let (actual_digest, path) = match &self {
            ComponentLocationToml::Path(wasm_path) => {
                let wasm_path = PathBuf::from(wasm_path);
                if !wasm_path.exists() {
                    bail!("file does not exist: {wasm_path:?}");
                }
                let actual_digest = calculate_sha256_file(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
                (actual_digest, wasm_path)
            }
            ComponentLocationToml::Oci(image) => {
                let image = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow!("invalid OCI reference `{image}`: {e}"))?;
                let (digest, path, _, _) =
                    oci::pull_to_cache_dir(&image, wasm_cache_dir, metadata_dir)
                        .await
                        .context("try cleaning the cache directory with `--clean-cache`")?;
                (digest, path)
            }
        };
        let stopwatch = stopwatch.elapsed();
        debug!("Fetching done in {stopwatch:?}");
        Ok((actual_digest, path))
    }
}

/// Trait for config structs that have an optional `name` and a required `ffqn`.
trait HasOptionalNameAndFfqn {
    fn config_name(&self) -> Option<&ConfigName>;
    fn ffqn(&self) -> &FunctionFqn;
}

impl HasOptionalNameAndFfqn for ActivityJsComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for ActivityExecComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for WorkflowJsComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for ActivityStubExtInlineConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

/// Location of a JavaScript source file.
/// Supports local file paths and OCI registry references (`oci://...`).
/// On-disk format only; replaced by [`ScriptLocationCanonical`] before transmission and hash computation.
#[derive(Debug, Clone, Hash, JsonSchema, SerializeDisplay, DeserializeFromStr)]
#[schemars(with = "String")]
pub(crate) enum JsLocationToml {
    Path(String),
    Oci(oci_client::Reference),
}
impl Display for JsLocationToml {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsLocationToml::Path(p) => write!(f, "{p}"),
            JsLocationToml::Oci(r) => write!(f, "{OCI_SCHEMA_PREFIX}{r}"),
        }
    }
}
impl FromStr for JsLocationToml {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(location) = s.strip_prefix(OCI_SCHEMA_PREFIX) {
            Ok(JsLocationToml::Oci(
                oci_client::Reference::from_str(location)
                    .map_err(|e| anyhow::anyhow!("invalid OCI reference: {e}"))?,
            ))
        } else {
            Ok(JsLocationToml::Path(s.to_string()))
        }
    }
}

pub(crate) trait ComponentCommonFetchExt {
    async fn fetch(
        self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ComponentCommonVerified, ContentDigest, PathBuf), anyhow::Error>;
}

impl ComponentCommonFetchExt for ComponentCommon {
    async fn fetch(
        self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ComponentCommonVerified, ContentDigest, PathBuf), anyhow::Error> {
        let (content_digest, wasm_path) = self.location.fetch(wasm_cache_dir, metadata_dir).await?;

        let verified = ComponentCommonVerified {
            name: self.name,
            location: self.location,
        };
        Ok((verified, content_digest, wasm_path))
    }
}

fn locking_strategy_into_executor(value: LockingStrategy) -> executor::executor::LockingStrategy {
    match value {
        LockingStrategy::ByFfqns => executor::executor::LockingStrategy::ByFfqns,
        LockingStrategy::ByComponentDigest => {
            executor::executor::LockingStrategy::ByComponentDigest
        }
        LockingStrategy::Auto => executor::executor::LockingStrategy::Auto,
    }
}

pub(crate) trait ExecConfigTomlExt {
    fn into_exec_exec_config(
        self,
        component_id: ComponentId,
        task_limiter_global: Option<Arc<tokio::sync::Semaphore>>,
        retry_config: ComponentRetryConfig,
    ) -> Result<executor::executor::ExecConfig, anyhow::Error>;
}

impl ExecConfigTomlExt for ExecConfigToml {
    fn into_exec_exec_config(
        self,
        component_id: ComponentId,
        task_limiter_global: Option<Arc<tokio::sync::Semaphore>>,
        retry_config: ComponentRetryConfig,
    ) -> Result<executor::executor::ExecConfig, anyhow::Error> {
        Ok(executor::executor::ExecConfig {
            lock_expiry: self.lock_expiry.into(),
            tick_sleep: self.tick_sleep.into(),
            batch_size: self.batch_size,
            locking_strategy: locking_strategy(self.locking_strategy, component_id.component_type)?,
            component_id,
            task_limiter_global,
            task_limiter_local: self.instance_limiter.as_semaphore(),
            executor_id: ExecutorId::generate(),
            retry_config,
        })
    }
}

fn locking_strategy(
    locking_strategy_override: Option<LockingStrategy>,
    component_type: ComponentType,
) -> Result<executor::executor::LockingStrategy, anyhow::Error> {
    if component_type == ComponentType::Cron {
        ensure!(
            locking_strategy_override.is_none(),
            "locking strategy cannot be overridden for cron"
        );
        // needed for seed execution deduplication.
        return Ok(executor::executor::LockingStrategy::ByComponentDigest);
    }
    // Auto is only valid for workflows
    if component_type != ComponentType::Workflow
        && locking_strategy_override == Some(LockingStrategy::Auto)
    {
        bail!("Locking strategy `auto` is only available for workflows");
    }
    Ok(locking_strategy_override.map(locking_strategy_into_executor).unwrap_or_else(||
    match component_type {
        ComponentType::Activity => executor::executor::LockingStrategy::ByFfqns,
        ComponentType::Workflow => executor::executor::LockingStrategy::Auto,
        other => unreachable!(
            "unexpected type {other}, only workflows, activities, and crons expose locking strategy"
        ),
    }))
}

pub(crate) trait LogLevelTomlExt {
    fn into_log_level(self) -> Option<LogLevel>;
}
impl LogLevelTomlExt for LogLevelToml {
    fn into_log_level(self) -> Option<LogLevel> {
        match self {
            LogLevelToml::Off => None,
            LogLevelToml::Trace => Some(LogLevel::Trace),
            LogLevelToml::Debug => Some(LogLevel::Debug),
            LogLevelToml::Info => Some(LogLevel::Info),
            LogLevelToml::Warn => Some(LogLevel::Warn),
            LogLevelToml::Error => Some(LogLevel::Error),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityStubExtInlineConfigToml {
    /// Component name. Optional — defaults to `{ifc_name}.{function_name}` from `ffqn`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    #[serde(default)]
    pub(crate) params: Option<Vec<JsParamToml>>,
    #[serde(default)]
    pub(crate) return_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityStubComponentConfigToml {
    File(ActivityStubFileConfigToml),
    Inline(ActivityStubExtInlineConfigToml),
}
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityExternalComponentConfigToml {
    File(ActivityExternalFileConfigToml),
    Inline(ActivityStubExtInlineConfigToml),
}

#[derive(Debug)]
pub(crate) struct ActivityStubExtConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) component_id: ComponentId,
}

#[derive(Debug)]
pub(crate) struct ActivityStubExtInlineConfigVerified {
    pub(crate) component_id: ComponentId,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
}

#[derive(Debug)]
pub(crate) enum ActivityStubConfigVerified {
    File(ActivityStubExtConfigVerified),
    Inline(ActivityStubExtInlineConfigVerified),
}

pub(crate) trait ActivityStubComponentConfigCanonicalExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityStubConfigVerified, anyhow::Error>;
}

impl ActivityStubComponentConfigCanonicalExt for ActivityStubComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.name_str(), component_id))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityStubConfigVerified, anyhow::Error> {
        match self {
            Self::File(file) => {
                let (common, content_digest, wasm_path) =
                    file.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
                let component_id = ComponentId::new(
                    ComponentType::ActivityStub,
                    StrVariant::from(common.name),
                    ComponentDigest(content_digest.0),
                )?;
                Ok(ActivityStubConfigVerified::File(
                    ActivityStubExtConfigVerified {
                        wasm_path,
                        component_id,
                    },
                ))
            }
            Self::Inline(inline) => {
                let ffqn = inline.ffqn;
                let parsed_params = match inline.params {
                    None => {
                        vec![concepts::ParameterType {
                            type_wrapper: val_json::type_wrapper::TypeWrapper::List(Box::new(
                                val_json::type_wrapper::TypeWrapper::String,
                            )),
                            name: StrVariant::Static("params"),
                            wit_type: StrVariant::Static("list<string>"),
                        }]
                    }
                    Some(params) => params
                        .iter()
                        .map(|p| {
                            let tw = val_json::type_wrapper::parse_wit_type(&p.wit_type)
                                .map_err(|e| anyhow!("invalid param type `{}`: {e}", p.wit_type))?;
                            Ok(concepts::ParameterType {
                                type_wrapper: tw,
                                name: StrVariant::from(p.name.clone()),
                                wit_type: StrVariant::from(p.wit_type.clone()),
                            })
                        })
                        .collect::<Result<Vec<_>, anyhow::Error>>()?,
                };

                const DEFAULT_RETURN_TYPE: &str = "result<string, string>";
                let return_type_str = inline.return_type.as_deref().unwrap_or(DEFAULT_RETURN_TYPE);
                let return_type_tw = val_json::type_wrapper::parse_wit_type(return_type_str)
                    .map_err(|e| anyhow!("invalid return_type `{return_type_str}`: {e}"))?;
                let return_type = concepts::ReturnType::detect(
                    return_type_tw,
                    StrVariant::from(return_type_str.to_string()),
                );
                let return_type = match return_type {
                    ReturnType::Extendable(rt) => rt,
                    ReturnType::NonExtendable(_) => bail!(
                        "return_type must be `result`, `result<T>`, `result<T, string>`, or \
                         `result<T, variant {{ execution-failed, ... }}>`, got `{return_type_str}`"
                    ),
                };

                // Compute component digest: SHA256 of prefix + ffqn + params + return_type
                let mut hasher = Sha256::new();
                hasher.update(b"activity_stub_inline:");
                hasher.update(ffqn.to_string().as_bytes());
                for p in &parsed_params {
                    hasher.update(p.wit_type.as_ref().as_bytes());
                }
                hasher.update(return_type.wit_type.as_bytes());
                let hash: [u8; 32] = hasher.finalize().into();
                let component_digest = ComponentDigest(Digest(hash));

                let component_id = ComponentId::new(
                    ComponentType::ActivityStub,
                    StrVariant::from(inline.name),
                    component_digest,
                )?;

                Ok(ActivityStubConfigVerified::Inline(
                    ActivityStubExtInlineConfigVerified {
                        component_id,
                        ffqn,
                        params: parsed_params,
                        return_type,
                    },
                ))
            }
        }
    }
}
#[derive(Debug)]
pub(crate) enum ActivityExternalConfigVerified {
    File(ActivityStubExtConfigVerified),
    Inline(ActivityStubExtInlineConfigVerified),
}

pub(crate) trait ActivityExternalComponentConfigCanonicalExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityExternalConfigVerified, anyhow::Error>;
}

impl ActivityExternalComponentConfigCanonicalExt for ActivityExternalComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.name_str(), component_id))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityExternalConfigVerified, anyhow::Error> {
        match self {
            Self::File(file) => {
                let component_digest_override = file.component_digest;
                let (common, content_digest, wasm_path) =
                    file.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
                let component_digest =
                    component_digest_override.unwrap_or(ComponentDigest(content_digest.0));
                let component_id = ComponentId::new(
                    ComponentType::Activity,
                    StrVariant::from(common.name),
                    component_digest,
                )?;
                Ok(ActivityExternalConfigVerified::File(
                    ActivityStubExtConfigVerified {
                        wasm_path,
                        component_id,
                    },
                ))
            }
            Self::Inline(inline) => {
                let ffqn = inline.ffqn;
                let parsed_params = match inline.params {
                    None => {
                        vec![concepts::ParameterType {
                            type_wrapper: val_json::type_wrapper::TypeWrapper::List(Box::new(
                                val_json::type_wrapper::TypeWrapper::String,
                            )),
                            name: StrVariant::Static("params"),
                            wit_type: StrVariant::Static("list<string>"),
                        }]
                    }
                    Some(params) => params
                        .iter()
                        .map(|p| {
                            let tw = val_json::type_wrapper::parse_wit_type(&p.wit_type)
                                .map_err(|e| anyhow!("invalid param type `{}`: {e}", p.wit_type))?;
                            Ok(concepts::ParameterType {
                                type_wrapper: tw,
                                name: StrVariant::from(p.name.clone()),
                                wit_type: StrVariant::from(p.wit_type.clone()),
                            })
                        })
                        .collect::<Result<Vec<_>, anyhow::Error>>()?,
                };

                const DEFAULT_RETURN_TYPE: &str = "result<string, string>";
                let return_type_str = inline.return_type.as_deref().unwrap_or(DEFAULT_RETURN_TYPE);
                let return_type_tw = val_json::type_wrapper::parse_wit_type(return_type_str)
                    .map_err(|e| anyhow!("invalid return_type `{return_type_str}`: {e}"))?;
                let return_type = concepts::ReturnType::detect(
                    return_type_tw,
                    StrVariant::from(return_type_str.to_string()),
                );
                let return_type = match return_type {
                    ReturnType::Extendable(rt) => rt,
                    ReturnType::NonExtendable(_) => bail!(
                        "return_type must be `result`, `result<T>`, `result<T, string>`, or \
                         `result<T, variant {{ execution-failed, ... }}>`, got `{return_type_str}`"
                    ),
                };

                // Compute component digest: SHA256 of prefix + ffqn + params + return_type
                let mut hasher = Sha256::new();
                hasher.update(b"activity_external_inline:");
                hasher.update(ffqn.to_string().as_bytes());
                for p in &parsed_params {
                    hasher.update(p.wit_type.as_ref().as_bytes());
                }
                hasher.update(return_type.wit_type.as_bytes());
                let hash: [u8; 32] = hasher.finalize().into();
                let component_digest = ComponentDigest(Digest(hash));

                let component_id = ComponentId::new(
                    ComponentType::Activity,
                    StrVariant::from(inline.name),
                    component_digest,
                )?;

                Ok(ActivityExternalConfigVerified::Inline(
                    ActivityStubExtInlineConfigVerified {
                        component_id,
                        ffqn,
                        params: parsed_params,
                        return_type,
                    },
                ))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ActivityWasmConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityWasmConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }
}

pub(crate) trait ActivityWasmComponentConfigTomlExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityWasmConfigVerified, anyhow::Error>;
}

impl ActivityWasmComponentConfigTomlExt for ActivityWasmComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.common.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityWasmConfigVerified, anyhow::Error> {
        let (common, content_digest, wasm_path) =
            self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;

        let env_vars = resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars)?;
        let allowed_hosts = resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars)?;

        // Validate no collision between env_vars and secret env names
        validate_no_env_collision(&env_vars, &allowed_hosts)?;

        let component_digest = self
            .component_digest
            .unwrap_or(ComponentDigest(content_digest.0));
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(common.name),
            component_digest,
        )?;
        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            env_vars,
            fuel,
            allowed_hosts,
            config_section_hint: ConfigSectionHint::ActivityWasm,
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityWasmConfigVerified {
            wasm_path,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityJsComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the JavaScript source file.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<JsLocationToml>,
    /// Inline JavaScript source embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the JS source file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// Override the auto-computed component digest used for locking.
    /// If set, this value is used instead of the digest derived from the JS source, ffqn, and params.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the JS function.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    /// Defaults to no parameters.
    #[serde(default)]
    pub(crate) params: Vec<JsParamToml>,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default, rename = "allowed_host")]
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
    /// WIT return type. Defaults to `result`.
    /// Must be `result<T, string>` — the error type must be `string` since JS throws strings.
    #[serde(default)]
    pub(crate) return_type: Option<String>,
}
#[derive(Debug)]
pub(crate) struct ActivityJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>, // same for all JS activities
    pub(crate) js_source: String,
    pub(crate) js_file_name: String,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityJsConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }

    pub(crate) fn as_frame_sources(&self) -> FrameFilesToSourceContent {
        FrameFilesToSourceContent::from([(self.js_file_name.clone(), self.js_source.clone())])
    }
}

// --- activity_exec config ---

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityExecComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the exec script.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<JsLocationToml>,
    /// Inline script content embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the exec script.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the exec activity.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    #[serde(default)]
    pub(crate) params: Vec<JsParamToml>,
    /// WIT return type. Defaults to `result`.
    #[serde(default)]
    pub(crate) return_type: Option<String>,
    /// Override the auto-computed component digest used for locking.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    /// Maximum bytes collected from stdout to form the response.
    /// Exceeding the limit fails the execution.
    /// Not used when `return_type` is result (default), since the response carries no data.
    #[serde(default = "default_max_output_bytes")]
    pub(crate) max_output_bytes: u64,
    /// Secrets pushed to stdin. See `ExecSecretsToml`.
    #[serde(default)]
    pub(crate) secrets: Option<ExecSecretsToml>,
}

#[derive(Debug)]
pub(crate) struct ResolvedExecProgram {
    pub(crate) program: ExecProgram,
    pub(crate) source_bytes: Vec<u8>,
}

pub(crate) trait ActivityExecComponentConfigCanonicalExt {
    async fn resolve(
        &self,
        wasm_cache_dir: &std::path::Path,
    ) -> anyhow::Result<ResolvedExecProgram>;
    fn fetch_and_verify(
        self,
        resolved_program: ResolvedExecProgram,
        ignore_missing_env_vars: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<ActivityExecConfigVerified, anyhow::Error>;
}

impl ActivityExecComponentConfigCanonicalExt for ActivityExecComponentConfigCanonical {
    /// Resolve the canonical program to a form the worker can execute.
    async fn resolve(
        &self,
        wasm_cache_dir: &std::path::Path,
    ) -> anyhow::Result<ResolvedExecProgram> {
        match &self.location {
            ScriptLocationCanonical::Content { content, .. } => {
                if let Some(expected) = self.content_digest.as_ref() {
                    let hash: [u8; 32] = Sha256::digest(content.as_bytes()).into();
                    let actual = ContentDigest(Digest(hash));
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for inline exec content: expected {expected}, got {actual}"
                    );
                }
                Ok(ResolvedExecProgram {
                    program: ExecProgram::Inline(content.clone()),
                    source_bytes: content.as_bytes().to_vec(),
                })
            }
            ScriptLocationCanonical::Path { path } => {
                let full_path = PathBuf::from(path);
                let content = tokio::fs::read_to_string(&full_path)
                    .await
                    .with_context(|| format!("cannot read external exec file {full_path:?}"))?;
                if let Some(expected) = self.content_digest.as_ref() {
                    let hash: [u8; 32] = Sha256::digest(content.as_bytes()).into();
                    let actual = ContentDigest(Digest(hash));
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for external exec file {full_path:?}: expected {expected}, got {actual}"
                    );
                }
                Ok(ResolvedExecProgram {
                    program: ExecProgram::Inline(content.clone()),
                    source_bytes: content.into_bytes(),
                })
            }
            ScriptLocationCanonical::Oci { image } => {
                let oci_ref = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow!("invalid OCI reference `{image}`: {e}"))?;
                let exec_cache_dir = wasm_cache_dir.join("exec");
                tokio::fs::create_dir_all(&exec_cache_dir).await?;
                let metadata_dir = wasm_cache_metadata_dir(wasm_cache_dir);
                let result =
                    crate::oci::pull_exec_to_cache(&oci_ref, &exec_cache_dir, &metadata_dir)
                        .await?;
                if let Some(expected) = self.content_digest.as_ref() {
                    let actual = utils::sha256sum::calculate_sha256_file(&result.exec_path).await?;
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for OCI exec `{image}`: expected {expected}, got {actual}"
                    );
                }
                let source_bytes = tokio::fs::read(&result.exec_path).await.with_context(|| {
                    format!("cannot read cached exec file {:?}", result.exec_path)
                })?;
                Ok(ResolvedExecProgram {
                    program: ExecProgram::CachedFile(result.exec_path),
                    source_bytes,
                })
            }
        }
    }

    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    fn fetch_and_verify(
        self,
        resolved_program: ResolvedExecProgram,
        ignore_missing_env_vars: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<ActivityExecConfigVerified, anyhow::Error> {
        let parsed_params = self
            .params
            .iter()
            .map(|p| {
                let tw = val_json::type_wrapper::parse_wit_type(&p.wit_type)
                    .map_err(|e| anyhow!("invalid param type `{}`: {e}", p.wit_type))?;
                Ok(concepts::ParameterType {
                    type_wrapper: tw,
                    name: StrVariant::from(p.name.clone()),
                    wit_type: StrVariant::from(p.wit_type.clone()),
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;
        const DEFAULT_RETURN_TYPE: &str = "result";
        let return_type_str = self.return_type.as_deref().unwrap_or(DEFAULT_RETURN_TYPE);
        let return_type_tw = val_json::type_wrapper::parse_wit_type(return_type_str)
            .map_err(|e| anyhow!("invalid return_type `{return_type_str}`: {e}"))?;
        let return_type = concepts::ReturnType::detect(
            return_type_tw,
            StrVariant::from(return_type_str.to_string()),
        );
        let return_type = match return_type {
            ReturnType::Extendable(rt) => rt,
            ReturnType::NonExtendable(_) => bail!(
                "return_type must be `result`, `result<T>`, `result<T, string>`, or \
                 `result<T, variant {{ execution-failed, ... }}>`, got `{return_type_str}`"
            ),
        };
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"activity_exec:");
            hasher.update(&resolved_program.source_bytes);
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let env_vars = resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars)?;
        let resolved_secrets = if let Some(secrets) = self.secrets {
            let resolved = resolve_secret_env_vars(
                secrets
                    .env_vars
                    .into_iter()
                    .map(|secret| EnvVarConfig::KeyValue {
                        key: secret.name,
                        value: secret.value,
                    })
                    .collect(),
                ignore_missing_env_vars,
            )
            .map_err(|e| anyhow!("failed to resolve exec secrets: {e}"))?
            .into_iter()
            .collect::<indexmap::IndexMap<_, _>>();
            if resolved.is_empty() {
                None
            } else {
                Some(ResolvedExecSecrets { env_vars: resolved })
            }
        } else {
            None
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityExecConfigVerified {
            program: resolved_program.program,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            env_vars,
            max_output_bytes: self.max_output_bytes,
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            secrets: resolved_secrets,
            component_id: component_id.clone(),
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

/// Resolved secrets for exec activities. Secret values are stored in `SecretString`.
#[derive(derive_more::Debug)]
pub(crate) struct ResolvedExecSecrets {
    /// Resolved secret env vars: name → secret value.
    pub(crate) env_vars: indexmap::IndexMap<String, secrecy::SecretString>,
}

#[derive(Debug)]
pub(crate) struct ActivityExecConfigVerified {
    pub(crate) program: wasm_workers::activity::activity_exec_worker::ExecProgram,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) env_vars: Arc<[EnvVar]>,
    pub(crate) max_output_bytes: u64,
    pub(crate) forward_stdout: Option<StdOutputConfig>,
    pub(crate) forward_stderr: Option<StdOutputConfig>,
    pub(crate) secrets: Option<ResolvedExecSecrets>,
    pub(crate) component_id: ComponentId,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityExecConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.component_id
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowJsComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the JavaScript source file.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<JsLocationToml>,
    /// Inline JavaScript source embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the JS source file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// Override the auto-computed component digest used for locking.
    /// If set, this value is used instead of the digest derived from the JS source, ffqn, and params.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the JS workflow function.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    /// Defaults to no parameters.
    #[serde(default)]
    pub(crate) params: Vec<JsParamToml>,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    #[serde(default = "default_lock_extension")]
    pub(crate) lock_extension: bool,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    /// WIT return type. Defaults to `result`.
    /// Must be `result`, `result<T>`, `result<T, string>`, or
    /// `result<T, variant { execution-failed, ... }>`.
    #[serde(default)]
    pub(crate) return_type: Option<String>,
}

#[derive(Debug)]
pub(crate) struct WorkflowJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>, // same for all JS workflows
    pub(crate) js_source: String,
    pub(crate) js_file_name: String,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl WorkflowJsConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.workflow_config.component_id
    }

    pub(crate) fn frame_sources(
        js_file_name: String,
        js_source: String,
    ) -> FrameFilesToSourceContent {
        FrameFilesToSourceContent::from([(js_file_name, js_source)])
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowWasmComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    /// Override the auto-computed component digest used for locking.
    /// If set, this value is used instead of the content digest of the WASM file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    #[serde(default)]
    pub(crate) backtrace: ComponentBacktraceConfig,
    #[serde(default)]
    pub(crate) stub_wasi: bool,
    #[serde(default = "default_lock_extension")]
    pub(crate) lock_extension: bool,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
}

pub(crate) trait BlockingStrategyConfigTomlExt {
    fn into_blocking_strategy(self) -> JoinNextBlockingStrategy;
}
impl BlockingStrategyConfigTomlExt for BlockingStrategyConfigToml {
    fn into_blocking_strategy(self) -> JoinNextBlockingStrategy {
        use deployment_config::config::{
            BlockingStrategyAwaitConfig, BlockingStrategyConfigCustomized,
            BlockingStrategyConfigSimple,
        };
        match self {
            BlockingStrategyConfigToml::Tagged(BlockingStrategyConfigCustomized::Await(
                BlockingStrategyAwaitConfig {
                    non_blocking_event_batching,
                },
            )) => JoinNextBlockingStrategy::Await {
                non_blocking_event_batching,
            },
            BlockingStrategyConfigToml::Simple(BlockingStrategyConfigSimple::Interrupt) => {
                JoinNextBlockingStrategy::Interrupt
            }
            BlockingStrategyConfigToml::Simple(BlockingStrategyConfigSimple::Await) => {
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
                }
            }
        }
    }
}

/// Location of a WASM backtrace source file.
/// On-disk format only; resolved to `ComponentBacktraceConfigCanonical` before transmission
/// and hash computation.
///
/// Serialised as: `"path/to/file.ts"` → `Path`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
#[schemars(with = "String")]
pub(crate) enum BacktraceSourceLocation {
    /// Local file path — resolved at submit/startup time via `resolve_to_canonical`.
    Path(String),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentBacktraceConfig {
    #[serde(rename = "sources")]
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub(crate) frame_files_to_sources: HashMap<String, BacktraceSourceLocation>,
}
pub(crate) struct JsContent {
    pub(crate) source: String,
    pub(crate) file_name: String,
}

pub(crate) trait JsLocationCanonicalExt {
    async fn get_content(
        &self,
        wasm_cache_dir: &Path,
        expected_digest: Option<&ContentDigest>,
    ) -> anyhow::Result<JsContent>;
}

impl JsLocationCanonicalExt for ScriptLocationCanonical {
    /// Return the JS source content and file name.
    /// For `Content`, returns them directly (validating digest if provided).
    /// For `Path`, reads the external file at runtime (validating digest if provided).
    /// For `Oci`, pulls from the registry (or cache) under `wasm_cache_dir/js/`.
    async fn get_content(
        &self,
        wasm_cache_dir: &Path,
        expected_digest: Option<&ContentDigest>,
    ) -> anyhow::Result<JsContent> {
        match self {
            ScriptLocationCanonical::Content { content, file_name } => {
                if let Some(expected) = expected_digest {
                    let hash: [u8; 32] = Sha256::digest(content.as_bytes()).into();
                    let actual = ContentDigest(Digest(hash));
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for inline JS `{file_name}`: expected {expected}, got {actual}"
                    );
                }
                Ok(JsContent {
                    source: content.clone(),
                    file_name: file_name.clone(),
                })
            }
            ScriptLocationCanonical::Path { path } => {
                let full_path = PathBuf::from(path);
                let source = tokio::fs::read_to_string(&full_path)
                    .await
                    .with_context(|| format!("cannot read external JS file {full_path:?}"))?;
                if let Some(expected) = expected_digest {
                    let hash: [u8; 32] = Sha256::digest(source.as_bytes()).into();
                    let actual = ContentDigest(Digest(hash));
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for external JS file {full_path:?}: expected {expected}, got {actual}"
                    );
                }
                let file_name = full_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(path)
                    .to_string();
                Ok(JsContent { source, file_name })
            }
            ScriptLocationCanonical::Oci { image } => {
                let oci_ref = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow::anyhow!("invalid OCI reference in canonical form: {e}"))?;
                let js_cache_dir = wasm_cache_dir.join("js");
                tokio::fs::create_dir_all(&js_cache_dir)
                    .await
                    .with_context(|| {
                        format!("cannot create JS cache directory {js_cache_dir:?}")
                    })?;
                let metadata_dir = wasm_cache_metadata_dir(wasm_cache_dir);
                tokio::fs::create_dir_all(&metadata_dir)
                    .await
                    .with_context(|| {
                        format!("cannot create metadata directory {metadata_dir:?}")
                    })?;
                let crate::oci::JsCacheResult { js_path, .. } =
                    crate::oci::pull_js_to_cache(&oci_ref, &js_cache_dir, &metadata_dir)
                        .await
                        .with_context(|| format!("cannot pull JS from OCI: {image}"))?;
                if let Some(expected) = expected_digest {
                    let hash = utils::sha256sum::calculate_sha256_file(&js_path).await?;
                    ensure!(
                        *expected == hash,
                        "content digest mismatch for OCI JS `{image}`: expected {expected}, got {hash}"
                    );
                }
                let file_name = js_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("cached.js")
                    .to_string();
                let source = tokio::fs::read_to_string(&js_path)
                    .await
                    .with_context(|| format!("cannot read cached JS file {js_path:?}"))?;
                Ok(JsContent { source, file_name })
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct WorkflowConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) frame_files_to_sources: FrameFilesToSourceContent,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl WorkflowConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.workflow_config.component_id
    }
}

// Canonical component config types live in the `deployment-config` crate.

pub(crate) trait ActivityJsComponentConfigCanonicalExt {
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityJsConfigVerified, anyhow::Error>;
}

impl ActivityJsComponentConfigCanonicalExt for ActivityJsComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityJsConfigVerified, anyhow::Error> {
        let parsed_params = self
            .params
            .iter()
            .map(|p| {
                let tw = val_json::type_wrapper::parse_wit_type(&p.wit_type)
                    .map_err(|e| anyhow!("invalid param type `{}`: {e}", p.wit_type))?;
                Ok(concepts::ParameterType {
                    type_wrapper: tw,
                    name: StrVariant::from(p.name.clone()),
                    wit_type: StrVariant::from(p.wit_type.clone()),
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;
        let JsContent {
            source: js_source,
            file_name: js_file_name,
        } = self
            .location
            .get_content(&wasm_cache_dir, self.content_digest.as_ref())
            .await?;
        const DEFAULT_RETURN_TYPE: &str = "result";
        let return_type_str = self.return_type.as_deref().unwrap_or(DEFAULT_RETURN_TYPE);
        let return_type_tw = val_json::type_wrapper::parse_wit_type(return_type_str)
            .map_err(|e| anyhow!("invalid return_type `{return_type_str}`: {e}"))?;
        let return_type = concepts::ReturnType::detect(
            return_type_tw,
            StrVariant::from(return_type_str.to_string()),
        );
        let return_type = match return_type {
            ReturnType::Extendable(rt) => rt,
            ReturnType::NonExtendable(_) => bail!(
                "return_type must be `result`, `result<T>`, `result<T, string>`, or \
                 `result<T, variant {{ execution-failed, ... }}>`, got `{return_type_str}`"
            ),
        };
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"activity_js:");
            hasher.update(js_source.as_bytes());
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let env_vars = resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars)?;
        let allowed_hosts = resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars)?;
        validate_no_env_collision(&env_vars, &allowed_hosts)?;
        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            env_vars,
            fuel,
            allowed_hosts,
            config_section_hint: ConfigSectionHint::ActivityJs,
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityJsConfigVerified {
            wasm_path,
            js_source,
            js_file_name,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

pub(crate) trait WorkflowWasmComponentConfigCanonicalExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        global_backtrace_persist: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
    ) -> Result<WorkflowConfigVerified, anyhow::Error>;
}

impl WorkflowWasmComponentConfigCanonicalExt for WorkflowWasmComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.common.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        global_backtrace_persist: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
    ) -> Result<WorkflowConfigVerified, anyhow::Error> {
        let retry_exp_backoff = Duration::from(self.retry_exp_backoff);
        if retry_exp_backoff == Duration::ZERO {
            bail!(
                "invalid `retry_exp_backoff` setting for workflow `{}` - duration must not be zero",
                self.common.name
            );
        }
        let (common, content_digest, wasm_path) =
            self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
        let wasm_path = WasmComponent::convert_core_module_to_component(
            &wasm_path,
            &content_digest,
            &wasm_cache_dir,
        )
        .await?
        .unwrap_or(wasm_path);
        let component_digest = self
            .component_digest
            .unwrap_or(ComponentDigest(content_digest.0));
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(common.name),
            component_digest,
        )?;
        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            join_next_blocking_strategy: self.blocking_strategy.into_blocking_strategy(),
            backtrace_persist: global_backtrace_persist,
            stub_wasi: self.stub_wasi,
            fuel,
            lock_extension: self.lock_extension.then_some(self.exec.lock_expiry.into()),
            subscription_interruption,
        };
        let frame_files_to_sources = self.backtrace.into_frame_files();
        let retry_config = ComponentRetryConfig {
            max_retries: None,
            retry_exp_backoff,
        };
        Ok(WorkflowConfigVerified {
            wasm_path,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            frame_files_to_sources,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

pub(crate) trait WorkflowJsComponentConfigCanonicalExt {
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<WorkflowJsConfigVerified, anyhow::Error>;
}

impl WorkflowJsComponentConfigCanonicalExt for WorkflowJsComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<WorkflowJsConfigVerified, anyhow::Error> {
        let parsed_params = self
            .params
            .iter()
            .map(|p| {
                let tw = val_json::type_wrapper::parse_wit_type(&p.wit_type)
                    .map_err(|e| anyhow!("invalid param type `{}`: {e}", p.wit_type))?;
                Ok(concepts::ParameterType {
                    type_wrapper: tw,
                    name: StrVariant::from(p.name.clone()),
                    wit_type: StrVariant::from(p.wit_type.clone()),
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;
        let JsContent {
            source: js_source,
            file_name: js_file_name,
        } = self
            .location
            .get_content(&wasm_cache_dir, self.content_digest.as_ref())
            .await?;
        const DEFAULT_RETURN_TYPE: &str = "result";
        let return_type_str = self.return_type.as_deref().unwrap_or(DEFAULT_RETURN_TYPE);
        let return_type_tw = val_json::type_wrapper::parse_wit_type(return_type_str)
            .map_err(|e| anyhow!("invalid return_type `{return_type_str}`: {e}"))?;
        let return_type = concepts::ReturnType::detect(
            return_type_tw,
            StrVariant::from(return_type_str.to_string()),
        );
        let return_type = match return_type {
            ReturnType::Extendable(rt) => rt,
            ReturnType::NonExtendable(_) => bail!(
                "return_type must be `result`, `result<T>`, `result<T, string>`, or \
                 `result<T, variant {{ execution-failed, ... }}>`, got `{return_type_str}`"
            ),
        };
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"workflow_js:");
            hasher.update(js_source.as_bytes());
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            join_next_blocking_strategy: self.blocking_strategy.into_blocking_strategy(),
            backtrace_persist: false,
            stub_wasi: false,
            fuel: None,
            lock_extension: self.lock_extension.then_some(self.exec.lock_expiry.into()),
            subscription_interruption: None,
        };
        let retry_config = ComponentRetryConfig {
            max_retries: None,
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(WorkflowJsConfigVerified {
            wasm_path,
            js_source,
            js_file_name,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

/// Resolve a `DeploymentToml` to `DeploymentCanonical` by reading all local JS and backtrace
/// source files.
async fn resolve_local_refs_to_canonical(
    deployment: DeploymentTomlValidated,
) -> anyhow::Result<DeploymentCanonical> {
    let deployment_dir = deployment.deployment_dir.clone();
    let mut activities_js = Vec::with_capacity(deployment.activities_js.len());
    for (a, name) in deployment.activities_js {
        activities_js.push(ActivityJsComponentConfigCanonical {
            location: resolve_script_toml_to_canonical(
                a.location,
                a.content,
                format!("{name}.js"),
                &deployment_dir,
                a.content_digest.as_ref(),
            )
            .await?,
            name,
            content_digest: a.content_digest,
            component_digest: a.component_digest,
            ffqn: a.ffqn,
            params: a.params,
            exec: a.exec,
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars,
            allowed_hosts: a.allowed_hosts,
            return_type: a.return_type,
        });
    }

    let mut workflows_wasm = Vec::with_capacity(deployment.workflows_wasm.len());
    for w in deployment.workflows_wasm {
        workflows_wasm.push(WorkflowWasmComponentConfigCanonical {
            common: w.common,
            component_digest: w.component_digest,
            exec: w.exec,
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            backtrace: resolve_backtrace_to_canonical(&w.backtrace, &deployment_dir).await?,
            stub_wasi: w.stub_wasi,
            lock_extension: w.lock_extension,
            logs_store_min_level: w.logs_store_min_level,
        });
    }

    let mut workflows_js = Vec::with_capacity(deployment.workflows_js.len());
    for (w, name) in deployment.workflows_js {
        workflows_js.push(WorkflowJsComponentConfigCanonical {
            location: resolve_script_toml_to_canonical(
                w.location,
                w.content,
                format!("{name}.js"),
                &deployment_dir,
                w.content_digest.as_ref(),
            )
            .await?,
            name,
            content_digest: w.content_digest,
            component_digest: w.component_digest,
            ffqn: w.ffqn,
            params: w.params,
            exec: w.exec,
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            lock_extension: w.lock_extension,
            logs_store_min_level: w.logs_store_min_level,
            return_type: w.return_type,
        });
    }

    let mut webhooks_wasm = Vec::with_capacity(deployment.webhooks_wasm.len());
    for w in deployment.webhooks_wasm {
        webhooks_wasm.push(webhook::WebhookWasmComponentConfigCanonical {
            common: w.common,
            http_server: w.http_server,
            routes: w.routes,
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            env_vars: w.env_vars,
            backtrace: resolve_backtrace_to_canonical(&w.backtrace, &deployment_dir).await?,
            logs_store_min_level: w.logs_store_min_level,
            allowed_hosts: w.allowed_hosts,
        });
    }

    let mut webhooks_js = Vec::with_capacity(deployment.webhooks_js.len());
    for w in deployment.webhooks_js {
        webhooks_js.push(webhook::WebhookJsComponentConfigCanonical {
            location: resolve_script_toml_to_canonical(
                w.location,
                w.content,
                format!("{}.js", w.name),
                &deployment_dir,
                w.content_digest.as_ref(),
            )
            .await?,
            name: w.name,
            content_digest: w.content_digest,
            http_server: w.http_server,
            routes: w.routes,
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            logs_store_min_level: w.logs_store_min_level,
            env_vars: w.env_vars,
            allowed_hosts: w.allowed_hosts,
        });
    }

    let mut activities_exec = Vec::with_capacity(deployment.activities_exec.len());
    for (a, name) in deployment.activities_exec {
        let location = resolve_script_toml_to_canonical(
            a.location,
            a.content,
            name.to_string(),
            &deployment_dir,
            a.content_digest.as_ref(),
        )
        .await?;
        activities_exec.push(ActivityExecComponentConfigCanonical {
            name,
            location,
            content_digest: a.content_digest,
            ffqn: a.ffqn,
            params: a.params,
            return_type: a.return_type,
            component_digest: a.component_digest,
            exec: a.exec,
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars,
            max_output_bytes: a.max_output_bytes,
            secrets: a.secrets,
        });
    }

    // Build canonical stubs/externals with resolved names filled in.
    let activities_stub = deployment
        .activities_stub
        .into_iter()
        .map(|(c, name)| match c {
            ActivityStubComponentConfigToml::File(f) => {
                ActivityStubComponentConfigCanonical::File(f)
            }
            ActivityStubComponentConfigToml::Inline(i) => {
                ActivityStubComponentConfigCanonical::Inline(ActivityStubExtInlineConfigCanonical {
                    name,
                    ffqn: i.ffqn,
                    params: i.params,
                    return_type: i.return_type,
                })
            }
        })
        .collect();
    let activities_external = deployment
        .activities_external
        .into_iter()
        .map(|(c, name)| match c {
            ActivityExternalComponentConfigToml::File(f) => {
                ActivityExternalComponentConfigCanonical::File(f)
            }
            ActivityExternalComponentConfigToml::Inline(i) => {
                ActivityExternalComponentConfigCanonical::Inline(
                    ActivityStubExtInlineConfigCanonical {
                        name,
                        ffqn: i.ffqn,
                        params: i.params,
                        return_type: i.return_type,
                    },
                )
            }
        })
        .collect();

    Ok(DeploymentCanonical {
        activities_wasm: deployment.activities_wasm,
        activities_stub,
        activities_external,
        activities_js,
        activities_exec,
        workflows_wasm,
        workflows_js,
        webhooks_wasm,
        webhooks_js,
        crons: deployment.crons,
    })
}

/// Verify `bytes` against `expected` content digest, if one is set. No-op when unset
/// (digests are purely optional, user-supplied, and never auto-computed).
fn verify_content_digest(
    bytes: &[u8],
    expected: Option<&ContentDigest>,
    what: &str,
) -> anyhow::Result<()> {
    if let Some(expected) = expected {
        let hash: [u8; 32] = Sha256::digest(bytes).into();
        let actual = ContentDigest(Digest(hash));
        ensure!(
            *expected == actual,
            "content digest mismatch for {what}: expected {expected}, got {actual}"
        );
    }
    Ok(())
}

/// The literal prefix used to anchor a path at the deployment directory.
pub(crate) const DEPLOYMENT_DIR_PREFIX: &str = "${DEPLOYMENT_DIR}";

/// Strip an optional `${DEPLOYMENT_DIR}` (and following `/`) prefix, returning the remainder.
fn strip_deployment_dir_prefix(s: &str) -> Option<&str> {
    s.strip_prefix(DEPLOYMENT_DIR_PREFIX)
        .map(|rest| rest.strip_prefix('/').unwrap_or(rest))
}

/// Normalize a deployment-owned relative path to forward-slash form, rejecting anything
/// that would escape the deployment directory (`..`, absolute paths, drive prefixes).
/// Used both when canonicalizing and (defensively) when recreating files on export.
pub(crate) fn normalize_owned_subpath(rel: &str) -> anyhow::Result<String> {
    use std::path::Component;
    let mut parts: Vec<&str> = Vec::new();
    for comp in std::path::Path::new(rel).components() {
        match comp {
            Component::Normal(s) => parts.push(
                s.to_str()
                    .with_context(|| format!("non-UTF8 path component in `{rel}`"))?,
            ),
            Component::CurDir => {}
            Component::ParentDir => {
                bail!(
                    "path must not contain `..` (cannot escape the deployment directory): `{rel}`"
                )
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("path must be relative to the deployment directory: `{rel}`")
            }
        }
    }
    ensure!(!parts.is_empty(), "empty deployment-relative path: `{rel}`");
    Ok(parts.join("/"))
}

/// Mirror an absolute path into a safe relative subpath by dropping the root/drive prefix
/// and keeping the remaining components (e.g. `/home/u/p/src/lib.rs` → `home/u/p/src/lib.rs`).
/// Rejects `..`. Used to recreate absolute-sourced backtrace files self-contained under the
/// output directory (their content is captured in the canonical, so this round-trips).
fn mirror_path_as_relative(path: &str) -> anyhow::Result<String> {
    use std::path::Component;
    let mut parts: Vec<&str> = Vec::new();
    for comp in std::path::Path::new(path).components() {
        match comp {
            Component::Normal(s) => parts.push(
                s.to_str()
                    .with_context(|| format!("non-UTF8 path component in `{path}`"))?,
            ),
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            Component::ParentDir => {
                bail!("backtrace source path must not contain `..`: `{path}`")
            }
        }
    }
    ensure!(!parts.is_empty(), "empty backtrace source path: `{path}`");
    Ok(parts.join("/"))
}

/// Resolve a script source (JS or exec) TOML location to its canonical form.
///
/// - inline `content` → `Content { content, file_name: default_file_name }` (owned).
/// - a **relative** `Path` (bare, or `${DEPLOYMENT_DIR}/…`) → read + inline as `Content`,
///   with `file_name` preserving the deployment-relative subpath (owned). `..` escapes error.
/// - an **absolute** `Path` → `Path { path }` (external; read at runtime, not recreated).
/// - an `Oci` reference → `Oci { image }`.
///
/// When `content_digest` is set it is verified here against the relevant bytes (inline
/// content, the owned file, or the external file). `Oci` digests are verified at runtime.
async fn resolve_script_toml_to_canonical(
    location: Option<JsLocationToml>,
    content: Option<String>,
    default_file_name: String,
    deployment_dir: &Path,
    content_digest: Option<&ContentDigest>,
) -> anyhow::Result<ScriptLocationCanonical> {
    match (location, content) {
        (None, Some(content)) => {
            verify_content_digest(content.as_bytes(), content_digest, &default_file_name)?;
            Ok(ScriptLocationCanonical::Content {
                content,
                file_name: default_file_name,
            })
        }
        (Some(JsLocationToml::Path(path)), None) => {
            // A `${DEPLOYMENT_DIR}/…` prefix or any non-absolute path is deployment-owned;
            // an absolute path is an external reference.
            let owned_rel = if let Some(rest) = strip_deployment_dir_prefix(&path) {
                Some(normalize_owned_subpath(rest)?)
            } else if std::path::Path::new(&path).is_absolute() {
                None
            } else {
                Some(normalize_owned_subpath(&path)?)
            };
            if let Some(file_name) = owned_rel {
                let full_path = deployment_dir.join(&file_name);
                let content = tokio::fs::read_to_string(&full_path)
                    .await
                    .with_context(|| format!("cannot read script file {full_path:?}"))?;
                verify_content_digest(content.as_bytes(), content_digest, &file_name)?;
                Ok(ScriptLocationCanonical::Content { content, file_name })
            } else {
                // External reference: verify digest against its bytes if set.
                if content_digest.is_some() {
                    let bytes = tokio::fs::read(&path)
                        .await
                        .with_context(|| format!("cannot read external script file {path:?}"))?;
                    verify_content_digest(&bytes, content_digest, &path)?;
                }
                Ok(ScriptLocationCanonical::Path { path })
            }
        }
        (Some(JsLocationToml::Oci(reference)), None) => Ok(ScriptLocationCanonical::Oci {
            // `to_string()` == `whole()`; use `to_string()` to match WASM OCI normalization.
            image: reference.to_string(),
        }),
        (None, None) | (Some(_), Some(_)) => {
            bail!("exactly one of `location` or `content` must be set for script components")
        }
    }
}

async fn resolve_backtrace_to_canonical(
    backtrace: &ComponentBacktraceConfig,
    deployment_dir: &Path,
) -> anyhow::Result<ComponentBacktraceConfigCanonical> {
    let mut frame_files_to_sources = HashMap::new();
    for (key, location) in &backtrace.frame_files_to_sources {
        let BacktraceSourceLocation::Path(path) = location;
        // Classify the source path like a script: a relative path (bare or
        // `${DEPLOYMENT_DIR}/…`) is deployment-relative and its subpath is mirrored on
        // export; an absolute path is recreated self-contained under a root-stripped mirror.
        let (full_path, file_name) = if let Some(rest) = strip_deployment_dir_prefix(path) {
            let rel = normalize_owned_subpath(rest)?;
            (deployment_dir.join(&rel), rel)
        } else if std::path::Path::new(path).is_absolute() {
            // Captured content is recreated self-contained under a root-stripped mirror.
            (PathBuf::from(path), mirror_path_as_relative(path)?)
        } else {
            let rel = normalize_owned_subpath(path)?;
            (deployment_dir.join(&rel), rel)
        };
        if !full_path.exists() {
            warn!("Ignoring missing backtrace source - file does not exist: {full_path:?}");
            continue;
        }
        match tokio::fs::read_to_string(&full_path).await {
            Ok(content) => {
                frame_files_to_sources
                    .insert(key.clone(), BacktraceSourceCanonical { content, file_name });
            }
            Err(err) => {
                warn!("Cannot read backtrace source {full_path:?} - {err:?}");
            }
        }
    }
    Ok(ComponentBacktraceConfigCanonical {
        frame_files_to_sources,
    })
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WasmtimeAllocatorConfig {
    #[default]
    Auto,
    OnDemand,
    Pooling,
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WasmtimePoolingAllocatorConfig {
    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in linear memories.
    #[serde(default)]
    pooling_memory_keep_resident: Option<usize>,

    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in tables.
    #[serde(default)]
    pooling_table_keep_resident: Option<usize>,

    /// Enable memory protection keys for the pooling allocator; this can
    /// optimize the size of memory slots.
    #[serde(default)]
    memory_protection_keys: Option<bool>,

    /// The maximum number of WebAssembly instances which can be created
    /// with the pooling allocator.
    #[serde(default)]
    pooling_total_core_instances: Option<u32>,

    /// The maximum number of WebAssembly components which can be created
    /// with the pooling allocator.
    #[serde(default)]
    pooling_total_component_instances: Option<u32>,

    /// The maximum number of WebAssembly memories which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pooling_total_memories: Option<u32>,

    /// The maximum number of WebAssembly tables which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pooling_total_tables: Option<u32>,

    /// The maximum number of WebAssembly stacks which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pooling_total_stacks: Option<u32>,

    /// The maximum runtime size of each linear memory in the pooling
    /// allocator, in bytes.
    #[serde(default)]
    pooling_max_memory_size: Option<usize>,
}

impl From<WasmtimePoolingAllocatorConfig> for wasm_workers::engines::PoolingOptions {
    fn from(value: WasmtimePoolingAllocatorConfig) -> wasm_workers::engines::PoolingOptions {
        wasm_workers::engines::PoolingOptions {
            pooling_memory_keep_resident: value.pooling_memory_keep_resident,
            pooling_table_keep_resident: value.pooling_table_keep_resident,
            memory_protection_keys: value.memory_protection_keys,
            pooling_total_core_instances: value.pooling_total_core_instances,
            pooling_total_component_instances: value.pooling_total_component_instances,
            pooling_total_memories: value.pooling_total_memories,
            pooling_total_tables: value.pooling_total_tables,
            pooling_total_stacks: value.pooling_total_stacks,
            pooling_max_memory_size: value.pooling_max_memory_size,
        }
    }
}

#[cfg(feature = "otlp")]
pub(crate) mod otlp {
    use super::{Deserialize, log};
    use log::EnvFilter;
    use schemars::JsonSchema;

    #[derive(Debug, Deserialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct OtlpConfig {
        pub(crate) enabled: bool,
        #[serde(default)]
        pub(crate) level: EnvFilter,
        #[serde(default = "default_service_name")]
        pub(crate) service_name: String,
        #[serde(default = "default_otlp_endpoint")]
        pub(crate) otlp_endpoint: String,
    }

    fn default_service_name() -> String {
        "obelisk-server".to_string()
    }

    fn default_otlp_endpoint() -> String {
        // Default port as per https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md
        "http://localhost:4317".to_string()
    }
}

pub(crate) mod log {
    use crate::config::toml::default_console_enabled;

    use super::{Deserialize, JsonSchema, default_console_style};
    use serde_with::serde_as;
    use std::str::FromStr;

    #[derive(Debug, Deserialize, JsonSchema, Default, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct LoggingConfig {
        #[serde(default)]
        pub(crate) file: Option<AppenderRollingFile>,
        #[serde(default)]
        pub(crate) console: AppenderConsole,
    }

    #[derive(Debug, Deserialize, JsonSchema, Default, Copy, Clone)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum SpanConfig {
        /// spans are ignored (this is the default)
        #[default]
        None,
        /// one event when span is created
        New,
        /// one event per enter of a span
        Enter,
        /// one event per exit of a span
        Exit,
        /// one event when the span is dropped
        Close,
        /// one event per enter/exit of a span
        Active,
        /// events at all points (new, enter, exit, drop)
        Full,
    }

    impl From<SpanConfig> for tracing_subscriber::fmt::format::FmtSpan {
        fn from(value: SpanConfig) -> Self {
            match value {
                SpanConfig::None => Self::NONE,
                SpanConfig::New => Self::NEW,
                SpanConfig::Enter => Self::ENTER,
                SpanConfig::Exit => Self::EXIT,
                SpanConfig::Close => Self::CLOSE,
                SpanConfig::Active => Self::ACTIVE,
                SpanConfig::Full => Self::FULL,
            }
        }
    }

    #[derive(Debug, Deserialize, JsonSchema, Default, Clone)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LoggingStyle {
        #[default]
        Plain,
        PlainCompact,
        Json,
    }

    #[serde_as]
    #[derive(Debug, Deserialize, JsonSchema, Default, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderCommon {
        #[serde(default)]
        pub(crate) level: EnvFilter,
        #[serde(default)]
        pub(crate) span: SpanConfig,
        #[serde(default)]
        pub(crate) target: bool,
    }

    #[derive(Debug, serde_with::DeserializeFromStr, JsonSchema, Clone)]
    pub(crate) struct EnvFilter(
        #[schemars(with = "String")] pub(crate) tracing_subscriber::EnvFilter,
    );
    impl FromStr for EnvFilter {
        type Err = tracing_subscriber::filter::ParseError;

        fn from_str(directives: &str) -> Result<Self, Self::Err> {
            tracing_subscriber::EnvFilter::builder()
                .parse(directives)
                .map(Self)
        }
    }
    impl Default for EnvFilter {
        fn default() -> Self {
            Self::from_str("info,app=debug").expect("empty directive must not fail to parse")
        }
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
    pub(crate) enum AppenderConsoleWriter {
        #[default]
        Stderr,
        Stdout,
    }

    #[derive(Debug, Deserialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderConsole {
        #[serde(default = "default_console_enabled")]
        pub(crate) enabled: bool,
        #[serde(flatten, default)]
        pub(crate) common: AppenderCommon,
        #[serde(default = "default_console_style")]
        pub(crate) style: LoggingStyle,
        #[serde(default)]
        pub(crate) writer: AppenderConsoleWriter,
    }
    impl Default for AppenderConsole {
        fn default() -> Self {
            Self {
                enabled: default_console_enabled(),
                common: AppenderCommon::default(),
                style: default_console_style(),
                writer: AppenderConsoleWriter::default(),
            }
        }
    }

    #[derive(Debug, Deserialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderRollingFile {
        pub(crate) enabled: bool,
        #[serde(flatten, default)]
        pub(crate) common: AppenderCommon,
        pub(crate) directory: String,
        pub(crate) prefix: String,
        #[serde(default)]
        pub(crate) rotation: Rotation,
        #[serde(default)]
        pub(crate) style: LoggingStyle,
    }

    #[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Rotation {
        Minutely,
        Hourly,
        Daily,
        #[default]
        Never,
    }
    impl From<Rotation> for tracing_appender::rolling::Rotation {
        fn from(value: Rotation) -> Self {
            match value {
                Rotation::Minutely => Self::MINUTELY,
                Rotation::Hourly => Self::HOURLY,
                Rotation::Daily => Self::DAILY,
                Rotation::Never => Self::NEVER,
            }
        }
    }
}

pub(crate) trait ComponentStdOutputTomlExt {
    fn into_std_output_config(self) -> Option<StdOutputConfig>;
}
impl ComponentStdOutputTomlExt for ComponentStdOutputToml {
    fn into_std_output_config(self) -> Option<StdOutputConfig> {
        match self {
            ComponentStdOutputToml::None => None,
            ComponentStdOutputToml::Stdout => Some(StdOutputConfig::Stdout),
            ComponentStdOutputToml::Stderr => Some(StdOutputConfig::Stderr),
            ComponentStdOutputToml::Db => Some(StdOutputConfig::Db),
        }
    }
}

pub(crate) mod webhook {
    use super::{
        AllowedHostToml, ComponentBacktraceConfig, ComponentCommon, ComponentCommonFetchExt,
        ComponentStdOutputToml, ComponentStdOutputTomlExt, ConfigName, JsContent,
        JsLocationCanonicalExt, JsLocationToml, LogLevelTomlExt, resolve_allowed_hosts,
        resolve_env_vars_plaintext, validate_no_env_collision,
    };
    use crate::command::server::FrameFilesToSourceContent;
    use crate::config::{env_var::EnvVarConfig, toml::LogLevelToml};
    use anyhow::Context;
    use concepts::{
        ComponentId, ComponentType, ContentDigest, StrVariant,
        component_id::{ComponentDigest, Digest},
        storage::LogLevel,
    };
    pub(crate) use deployment_config::config::webhook::{
        WebhookJsComponentConfigCanonical, WebhookRoute, WebhookRouteDetail,
        WebhookWasmComponentConfigCanonical, default_external_server_name,
    };
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use sha2::{Digest as _, Sha256};
    use std::{
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };
    use tracing::instrument;
    use wasm_workers::{
        envvar::EnvVar, http_hooks::ConfigSectionHint, http_request_policy::AllowedHostConfig,
        std_output_stream::StdOutputConfig,
    };

    #[derive(Debug, Deserialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct HttpServer {
        pub(crate) name: ConfigName,
        pub(crate) listening_addr: SocketAddr,
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookWasmComponentConfigToml {
        #[serde(flatten)]
        pub(crate) common: ComponentCommon,
        #[serde(default = "default_external_server_name")]
        pub(crate) http_server: ConfigName,
        pub(crate) routes: Vec<WebhookRoute>,
        #[serde(default)]
        pub(crate) forward_stdout: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) forward_stderr: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) env_vars: Vec<EnvVarConfig>,
        #[serde(default)]
        pub(crate) backtrace: ComponentBacktraceConfig,
        #[serde(default)]
        pub(crate) logs_store_min_level: LogLevelToml,
        /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
        #[serde(default, rename = "allowed_host")]
        pub(crate) allowed_hosts: Vec<AllowedHostToml>,
    }

    #[derive(Debug)]
    pub(crate) struct WebhookWasmComponentConfigVerified {
        pub(crate) component_id: ComponentId,
        pub(crate) wasm_path: PathBuf,
        pub(crate) routes: Vec<WebhookRouteVerified>,
        pub(crate) forward_stdout: Option<StdOutputConfig>,
        pub(crate) forward_stderr: Option<StdOutputConfig>,
        pub(crate) env_vars: Arc<[EnvVar]>,
        pub(crate) frame_files_to_sources: FrameFilesToSourceContent,
        pub(crate) subscription_interruption: Option<Duration>,
        pub(crate) logs_store_min_level: Option<LogLevel>,
        pub(crate) allowed_hosts: Arc<[AllowedHostConfig]>,
        /// The TOML config section type for error messages
        pub(crate) config_section_hint: ConfigSectionHint,
    }

    #[derive(Debug)]
    pub(crate) struct WebhookRouteVerified {
        pub(crate) methods: Vec<http::Method>,
        pub(crate) route: String,
    }

    impl TryFrom<WebhookRoute> for WebhookRouteVerified {
        type Error = anyhow::Error;

        fn try_from(value: WebhookRoute) -> Result<Self, Self::Error> {
            Ok(match value {
                WebhookRoute::String(route) => Self {
                    methods: Vec::new(),
                    route,
                },
                WebhookRoute::WebhookRouteDetail(WebhookRouteDetail { methods, route }) => {
                    let methods = methods
                        .into_iter()
                        .map(|method| {
                            http::Method::from_bytes(method.as_bytes())
                                .with_context(|| format!("cannot parse route method `{method}`"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Self { methods, route }
                }
            })
        }
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookJsComponentConfigToml {
        pub(crate) name: ConfigName,
        /// Location of the JavaScript source file.
        /// Supports local file paths and OCI registry references (`oci://...`).
        #[serde(default)]
        pub(crate) location: Option<JsLocationToml>,
        /// Inline JavaScript source embedded in the TOML.
        /// Exactly one of `location` or `content` must be set.
        #[serde(default)]
        pub(crate) content: Option<String>,
        /// Content digest of the JS source file.
        #[serde(default)]
        #[schemars(with = "Option<String>")]
        pub(crate) content_digest: Option<ContentDigest>,
        /// The HTTP server to bind this webhook to.
        #[serde(default = "default_external_server_name")]
        pub(crate) http_server: ConfigName,
        /// Routes that this webhook responds to.
        pub(crate) routes: Vec<WebhookRoute>,
        #[serde(default)]
        pub(crate) forward_stdout: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) forward_stderr: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) logs_store_min_level: LogLevelToml,
        #[serde(default)]
        pub(crate) env_vars: Vec<EnvVarConfig>,
        /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
        #[serde(default, rename = "allowed_host")]
        pub(crate) allowed_hosts: Vec<AllowedHostToml>,
    }

    #[derive(Debug)]
    pub(crate) struct WebhookJsConfigVerified {
        pub(crate) wasm_path: Arc<Path>,
        pub(crate) component_id: ComponentId,
        pub(crate) js_source: String,
        pub(crate) js_file_name: String,
        pub(crate) routes: Vec<WebhookRouteVerified>,
        pub(crate) forward_stdout: Option<StdOutputConfig>,
        pub(crate) forward_stderr: Option<StdOutputConfig>,
        pub(crate) env_vars: Arc<[EnvVar]>,
        pub(crate) logs_store_min_level: Option<LogLevel>,
        pub(crate) allowed_hosts: Arc<[AllowedHostConfig]>,
        /// The TOML config section type for error messages
        pub(crate) config_section_hint: ConfigSectionHint,
    }

    impl WebhookJsConfigVerified {
        pub(crate) fn as_frame_sources(&self) -> FrameFilesToSourceContent {
            FrameFilesToSourceContent::from([(self.js_file_name.clone(), self.js_source.clone())])
        }
    }

    pub(crate) trait WebhookWasmComponentConfigCanonicalExt {
        async fn fetch_and_verify(
            self,
            wasm_cache_dir: Arc<Path>,
            metadata_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            subscription_interruption: Option<Duration>,
        ) -> Result<(ConfigName, WebhookWasmComponentConfigVerified), anyhow::Error>;
    }

    impl WebhookWasmComponentConfigCanonicalExt for WebhookWasmComponentConfigCanonical {
        #[instrument(skip_all, fields(component_name = self.common.name.as_str()), err)]
        async fn fetch_and_verify(
            self,
            wasm_cache_dir: Arc<Path>,
            metadata_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            subscription_interruption: Option<Duration>,
        ) -> Result<(ConfigName, WebhookWasmComponentConfigVerified), anyhow::Error> {
            let (common, content_digest, wasm_path) =
                self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
            let frame_files_to_sources = self.backtrace.into_frame_files();
            let component_id = ComponentId::new(
                ComponentType::WebhookEndpoint,
                StrVariant::from(common.name.clone()),
                ComponentDigest(content_digest.0),
            )?;
            let env_vars = resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars)?;
            let allowed_hosts = resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars)?;
            validate_no_env_collision(&env_vars, &allowed_hosts)?;
            Ok((
                common.name,
                WebhookWasmComponentConfigVerified {
                    component_id,
                    wasm_path,
                    routes: self
                        .routes
                        .into_iter()
                        .map(WebhookRouteVerified::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                    forward_stdout: self.forward_stdout.into_std_output_config(),
                    forward_stderr: self.forward_stderr.into_std_output_config(),
                    env_vars,
                    frame_files_to_sources,
                    subscription_interruption,
                    logs_store_min_level: self.logs_store_min_level.into_log_level(),
                    allowed_hosts,
                    config_section_hint: ConfigSectionHint::WebhookEndpointWasm,
                },
            ))
        }
    }

    pub(crate) trait WebhookJsComponentConfigCanonicalExt {
        async fn fetch_and_verify(
            self,
            wasm_path: Arc<Path>,
            wasm_cache_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
        ) -> Result<(ConfigName, WebhookJsConfigVerified), anyhow::Error>;
    }

    impl WebhookJsComponentConfigCanonicalExt for WebhookJsComponentConfigCanonical {
        #[instrument(skip_all, fields(component_name = self.name.as_str()))]
        async fn fetch_and_verify(
            self,
            wasm_path: Arc<Path>,
            wasm_cache_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
        ) -> Result<(ConfigName, WebhookJsConfigVerified), anyhow::Error> {
            let JsContent {
                source: js_source,
                file_name: js_file_name,
            } = self
                .location
                .get_content(&wasm_cache_dir, self.content_digest.as_ref())
                .await?;
            let mut hasher = Sha256::new();
            hasher.update(b"webhook_js:");
            hasher.update(js_source.as_bytes());
            let hash: [u8; 32] = hasher.finalize().into();
            let component_id = ComponentId::new(
                ComponentType::WebhookEndpoint,
                StrVariant::from(self.name.clone()),
                ComponentDigest(Digest(hash)),
            )?;
            let env_vars = resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars)?;
            let allowed_hosts = resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars)?;
            validate_no_env_collision(&env_vars, &allowed_hosts)?;
            Ok((
                self.name,
                WebhookJsConfigVerified {
                    wasm_path,
                    component_id,
                    js_file_name,
                    js_source,
                    routes: self
                        .routes
                        .into_iter()
                        .map(WebhookRouteVerified::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                    forward_stdout: self.forward_stdout.into_std_output_config(),
                    forward_stderr: self.forward_stderr.into_std_output_config(),
                    env_vars,
                    logs_store_min_level: self.logs_store_min_level.into_log_level(),
                    allowed_hosts,
                    config_section_hint: ConfigSectionHint::WebhookEndpointJs,
                },
            ))
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(untagged)]
pub(crate) enum ValueOrUnlimited<T> {
    Unlimited(Unlimited),
    Some(T),
}
impl<T> Default for ValueOrUnlimited<T> {
    fn default() -> Self {
        Self::Unlimited(Unlimited::Unlimited)
    }
}
impl<T> From<ValueOrUnlimited<T>> for Option<T> {
    fn from(value: ValueOrUnlimited<T>) -> Self {
        match value {
            ValueOrUnlimited::Some(val) => Some(val),
            ValueOrUnlimited::Unlimited(Unlimited::Unlimited) => None,
        }
    }
}

pub(crate) trait InflightSemaphoreExt {
    fn as_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>>;
}
impl InflightSemaphoreExt for InflightSemaphore {
    fn as_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        match self {
            InflightSemaphore::Unlimited(_) => None,
            InflightSemaphore::Some(permits) => Some(Arc::new(tokio::sync::Semaphore::new(
                usize::try_from(*permits).expect("usize >= u32"),
            ))),
        }
    }
}

// TODO: Move to env_var module// TODO: Move to env_var module
fn resolve_env_vars_plaintext(
    env_vars: Vec<EnvVarConfig>,
    ignore_missing: bool,
) -> Result<Arc<[EnvVar]>, EnvVarMissing> {
    env_vars
        .into_iter()
        .map(|env_var| match env_var {
            EnvVarConfig::KeyValue { key, value } => Ok(EnvVar {
                key,
                val: interpolate_env_vars_plaintext(&value)?,
            }),
            EnvVarConfig::Key(key) => match std::env::var(&key) {
                Ok(val) => Ok(EnvVar { key, val }),
                Err(_err) => {
                    if ignore_missing {
                        Ok(EnvVar {
                            key,
                            val: String::new(),
                        })
                    } else {
                        Err(EnvVarMissing(key))
                    }
                }
            },
        })
        .collect::<Result<_, _>>()
}

#[derive(Debug, thiserror::Error)]
enum ResolveAllowedHostsError {
    #[error(transparent)]
    HostPattern(#[from] HostPatternError),
    #[error(transparent)]
    EnvVarsMissing(#[from] EnvVarsMissing),
    #[error("cannot parse HTTP method `{0}`")]
    InvalidMethod(String),
    #[error("use `methods = \"*\"` to allow all methods, not `methods = [\"*\"]`")]
    InvalidMethodStar,
}

fn resolve_allowed_hosts(
    entries: Vec<AllowedHostToml>,
    ignore_missing_env_vars: bool,
) -> Result<Arc<[AllowedHostConfig]>, ResolveAllowedHostsError> {
    entries
        .into_iter()
        .filter_map(|entry| {
            // Convert MethodsInput to MethodsPattern
            let methods = match entry.methods {
                None => {
                    // Omitted methods: nothing allowed, warn and skip
                    warn!(
                        "allowed_host `{}` has no `methods` field - no requests will be allowed; \
                         use `methods = \"*\"` to allow all methods",
                        entry.pattern
                    );
                    return None;
                }
                Some(MethodsInput::Star(_)) => {
                    // `methods = "*"` - all methods allowed
                    MethodsPattern::AllMethods
                }
                Some(MethodsInput::List(list)) => {
                    if list.is_empty() {
                        // Empty list: nothing allowed, warn and skip
                        warn!(
                            "allowed_host `{}` has empty `methods = []` - no requests will be allowed",
                            entry.pattern
                        );
                        return None;
                    }
                    // Parse specific methods
                    match list
                        .into_iter()
                        .map(|m| {
                            http::Method::from_bytes(m.as_bytes()).map_err(|_| {
                                if m == "*" {
                                    ResolveAllowedHostsError::InvalidMethodStar
                                } else {
                                    ResolveAllowedHostsError::InvalidMethod(m)
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(methods) => MethodsPattern::Specific(methods),
                        Err(e) => return Some(Err(e)),
                    }
                }
            };

            let pattern_str = match interpolate_env_vars_plaintext(&entry.pattern) {
                Ok(s) => s,
                Err(EnvVarMissing(var)) => {
                    if ignore_missing_env_vars {
                        warn!(
                            "allowed_host pattern `{}` references missing env var `{var}`, skipping",
                            entry.pattern
                        );
                        return None;
                    }
                    return Some(Err(ResolveAllowedHostsError::EnvVarsMissing(
                        EnvVarsMissing(vec![var]),
                    )));
                }
            };
            let pattern = match HostPattern::parse_with_methods(&pattern_str, methods) {
                Ok(p) => p,
                Err(e) => return Some(Err(e.into())),
            };

            let (secret_env_mappings, replace_in) = if let Some(secrets) = entry.secrets {
                if secrets.env_vars.is_empty() {
                    warn!(
                        "allowed_host `{}` has empty `secrets.env_vars`",
                        entry.pattern
                    );
                }
                if secrets.replace_in.is_empty() {
                    warn!(
                        "allowed_host `{}` has empty `secrets.replace_in` - secrets will never be injected",
                        entry.pattern
                    );
                }
                if pattern.scheme.allows_unencrypted() {
                    warn!("secrets allowed for potentially unencrypted host `{pattern}`");
                }

                let env_mappings =
                    match resolve_secret_env_vars(secrets.env_vars, ignore_missing_env_vars) {
                        Ok(m) => m,
                        Err(e) => return Some(Err(e)),
                    };
                let replace_in = secrets
                    .replace_in
                    .into_iter()
                    .map(|r| match r {
                        ReplaceIn::Headers => ReplacementLocation::Headers,
                        ReplaceIn::Body => ReplacementLocation::Body,
                        ReplaceIn::Params => ReplacementLocation::Params,
                    })
                    .collect();
                (env_mappings, replace_in)
            } else {
                (Vec::new(), hashbrown::HashSet::new())
            };

            Some(Ok(AllowedHostConfig {
                pattern,
                secret_env_mappings,
                replace_in,
            }))
        })
        .collect::<Result<_, _>>()
}

fn resolve_secret_env_vars(
    env_vars: Vec<EnvVarConfig>,
    ignore_missing: bool,
) -> Result<Vec<(String, SecretString)>, ResolveAllowedHostsError> {
    let mut missing = vec![];
    let mut env_mappings = Vec::new();
    for env_var in env_vars {
        match env_var {
            EnvVarConfig::KeyValue { key, value } => match interpolate_env_vars_secret(&value) {
                Ok(real_value) => env_mappings.push((key, real_value)),
                Err(err) => missing.push(err.0),
            },
            EnvVarConfig::Key(key) => match std::env::var(&key) {
                Ok(val) => env_mappings.push((key, SecretString::from(val))),
                Err(_err) => missing.push(key),
            },
        }
    }
    if !missing.is_empty() && !ignore_missing {
        return Err(EnvVarsMissing(missing).into());
    }
    Ok(env_mappings)
}

fn validate_no_env_collision(
    env_vars: &[EnvVar],
    allowed_hosts: &[AllowedHostConfig],
) -> Result<(), anyhow::Error> {
    let env_var_keys: hashbrown::HashSet<_> = env_vars.iter().map(|e| e.key.as_str()).collect();
    for host in allowed_hosts {
        for (key, _) in &host.secret_env_mappings {
            ensure!(
                !env_var_keys.contains(key.as_str()),
                "secret env var `{key}` collides with an `env_vars` entry"
            );
        }
    }
    Ok(())
}

const fn default_true() -> bool {
    true
}
const fn default_parallel_compilation() -> bool {
    true
}
const fn default_debug() -> bool {
    false
}
const fn default_global_backtrace_persist() -> bool {
    true
}

const fn default_codegen_enabled() -> bool {
    true
}

const fn default_subscription_interruption() -> DurationConfigOptional {
    DurationConfigOptional::Seconds(1)
}
fn default_console_enabled() -> bool {
    true
}
fn default_console_style() -> LoggingStyle {
    LoggingStyle::PlainCompact
}

fn default_sqlite_queue_capacity() -> usize {
    SqliteConfig::default().queue_capacity
}
fn default_workflows_lock_extension_leeway() -> DurationConfig {
    DurationConfig::Milliseconds(100)
}
fn default_timers_watcher_enabled() -> bool {
    true
}
fn default_timers_watcher_leeway() -> DurationConfig {
    DurationConfig::Milliseconds(500)
}
fn default_timers_watcher_tick_sleep() -> DurationConfig {
    DurationConfig::Milliseconds(100)
}

fn default_cancel_watcher_tick_sleep() -> DurationConfig {
    DurationConfig::Seconds(1)
}

pub(crate) mod cron {
    use super::*;

    pub(crate) use deployment_config::config::cron::CronComponentConfigToml;

    #[derive(Debug)]
    pub(crate) struct CronConfigVerified {
        pub(crate) component_id: ComponentId,
        pub(crate) target_ffqn: FunctionFqn,
        pub(crate) params_json: Vec<serde_json::Value>,
        pub(crate) cron_schedule: CronOrOnce,
        pub(crate) exec_config: executor::executor::ExecConfig,
    }

    pub(crate) trait CronComponentConfigTomlExt {
        fn verify(self) -> Result<CronConfigVerified, anyhow::Error>;
    }

    impl CronComponentConfigTomlExt for CronComponentConfigToml {
        fn verify(self) -> Result<CronConfigVerified, anyhow::Error> {
            let name = self.name.to_string();
            let cron_schedule = if self.schedule == "@once" {
                CronOrOnce::Once
            } else {
                CronOrOnce::Cron(Box::new(
                    croner::Cron::new(&self.schedule).parse().with_context(|| {
                        format!(
                            "invalid cron expression `{}` for schedule `{name}`",
                            self.schedule
                        )
                    })?,
                ))
            };
            // Validate params JSON
            let serde_json::Value::Array(params_json) =
                serde_json::from_str::<serde_json::Value>(&self.params).with_context(|| {
                    format!(
                        "invalid JSON params for schedule `{name}`: `{}`",
                        self.params
                    )
                })?
            else {
                bail!("invalid params for schedule `{name}` - expected JSON array")
            };
            // Compute component digest from schedule config
            let mut hasher = Sha256::new();
            sha2::Digest::update(&mut hasher, name.as_bytes());
            sha2::Digest::update(&mut hasher, self.ffqn.to_string().as_bytes());
            sha2::Digest::update(&mut hasher, self.params.as_bytes());
            sha2::Digest::update(&mut hasher, self.schedule.as_bytes());
            let hash: [u8; 32] = sha2::Digest::finalize(hasher).into();
            let component_digest = ComponentDigest(Digest(hash));
            let component_id = ComponentId::new(
                ComponentType::Cron,
                StrVariant::from(name),
                component_digest,
            )?;
            let exec_config = self.exec.into_exec_exec_config(
                component_id.clone(),
                None, // no global instance limiter for crons
                ComponentRetryConfig::CRON,
            )?;
            Ok(CronConfigVerified {
                component_id,
                target_ffqn: self.ffqn,
                params_json,
                cron_schedule,
                exec_config,
            })
        }
    }
}

// --- Filesystem export: `DeploymentCanonical` -> `DeploymentToml` + side files ---

/// A source file to recreate next to an exported `deployment.toml`.
/// `rel_path` is relative to the output directory (a basename).
pub(crate) struct SideFile {
    pub(crate) rel_path: String,
    pub(crate) content: String,
}

/// The result of converting a stored `DeploymentCanonical` back to an editable deployment.
pub(crate) struct DeploymentExport {
    pub(crate) deployment_toml: DeploymentToml,
    /// Owned source files (and backtrace sources) to recreate under the output directory.
    pub(crate) side_files: Vec<SideFile>,
    /// Absolute paths of external (non-owned) script references that were emitted verbatim
    /// and are therefore not portable across machines.
    pub(crate) external_paths: Vec<String>,
}

/// Build a `${DEPLOYMENT_DIR}/<rel>` location hint (never an absolute path).
fn deployment_dir_hint(rel: &str) -> String {
    format!("{DEPLOYMENT_DIR_PREFIX}/{rel}")
}

fn path_basename(path: &str) -> String {
    std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(path)
        .to_string()
}

/// Accumulates side files, erroring on basename collisions with conflicting content.
#[derive(Default)]
struct SideFileCollector {
    files: Vec<SideFile>,
}
impl SideFileCollector {
    /// Register a side file. Identical re-registrations (same content) are deduplicated;
    /// a same-name file with different content is a hard error (it cannot be written to
    /// disk without one clobbering the other).
    fn push(&mut self, rel_path: String, content: String) -> anyhow::Result<()> {
        if let Some(existing) = self.files.iter().find(|f| f.rel_path == rel_path) {
            ensure!(
                existing.content == content,
                "cannot export deployment: multiple distinct source files would be written \
                 to `{rel_path}`"
            );
            return Ok(());
        }
        self.files.push(SideFile { rel_path, content });
        Ok(())
    }
}

/// Map a canonical script location to a TOML `location`, recreating owned content as a
/// side file (mirroring its deployment-relative subpath). Callers set `content` to `None`
/// since content is always externalized to a side file or referenced by path.
fn script_location_to_toml(
    location: ScriptLocationCanonical,
    files: &mut SideFileCollector,
    external_paths: &mut Vec<String>,
) -> anyhow::Result<Option<JsLocationToml>> {
    Ok(Some(match location {
        ScriptLocationCanonical::Content { content, file_name } => {
            files.push(file_name.clone(), content)?;
            JsLocationToml::Path(deployment_dir_hint(&file_name))
        }
        ScriptLocationCanonical::Path { path } => {
            // External reference: emit the absolute path verbatim (it cannot be expressed
            // as a portable `${DEPLOYMENT_DIR}` hint) and do not recreate the file.
            external_paths.push(path.clone());
            JsLocationToml::Path(path)
        }
        ScriptLocationCanonical::Oci { image } => JsLocationToml::Oci(
            oci_client::Reference::from_str(&image)
                .map_err(|e| anyhow!("invalid OCI reference in canonical form `{image}`: {e}"))?,
        ),
    }))
}

/// Rewrite a WASM component location to a `${DEPLOYMENT_DIR}` hint (Path) or pass through (Oci).
/// WASM bytes are never recreated; only the location string is rewritten.
fn wasm_location_to_toml(location: &ComponentLocationToml) -> ComponentLocationToml {
    match location {
        ComponentLocationToml::Path(p) => {
            ComponentLocationToml::Path(deployment_dir_hint(&path_basename(p)))
        }
        ComponentLocationToml::Oci(r) => ComponentLocationToml::Oci(r.clone()),
    }
}

/// Recreate backtrace sources as side files (mirroring their deployment-relative subpath),
/// pointing each frame-symbol key at a `${DEPLOYMENT_DIR}` hint.
fn backtrace_to_toml(
    backtrace: ComponentBacktraceConfigCanonical,
    files: &mut SideFileCollector,
) -> anyhow::Result<ComponentBacktraceConfig> {
    let mut frame_files_to_sources = HashMap::new();
    for (key, source) in backtrace.frame_files_to_sources {
        files.push(source.file_name.clone(), source.content)?;
        frame_files_to_sources.insert(
            key,
            BacktraceSourceLocation::Path(deployment_dir_hint(&source.file_name)),
        );
    }
    Ok(ComponentBacktraceConfig {
        frame_files_to_sources,
    })
}

/// Convert a stored `DeploymentCanonical` back into a re-submittable `DeploymentToml` plus
/// the source files to recreate. Locations are emitted as `${DEPLOYMENT_DIR}/<name>` hints
/// (never absolute paths); owned script content and workflow/webhook backtrace sources are
/// returned as side files. External-path scripts and WASM/OCI references are referenced but
/// not recreated.
pub(crate) fn deployment_canonical_to_toml(
    deployment: DeploymentCanonical,
) -> anyhow::Result<DeploymentExport> {
    let mut files = SideFileCollector::default();
    let mut external_paths: Vec<String> = Vec::new();

    let activities_wasm = deployment
        .activities_wasm
        .into_iter()
        .map(|mut c| {
            c.common.location = wasm_location_to_toml(&c.common.location);
            c
        })
        .collect();

    let activities_stub = deployment
        .activities_stub
        .into_iter()
        .map(|c| match c {
            ActivityStubComponentConfigCanonical::File(mut f) => {
                f.common.location = wasm_location_to_toml(&f.common.location);
                ActivityStubComponentConfigToml::File(f)
            }
            ActivityStubComponentConfigCanonical::Inline(i) => {
                ActivityStubComponentConfigToml::Inline(ActivityStubExtInlineConfigToml {
                    name: Some(i.name),
                    ffqn: i.ffqn,
                    params: i.params,
                    return_type: i.return_type,
                })
            }
        })
        .collect();

    let activities_external = deployment
        .activities_external
        .into_iter()
        .map(|c| match c {
            ActivityExternalComponentConfigCanonical::File(mut f) => {
                f.common.location = wasm_location_to_toml(&f.common.location);
                ActivityExternalComponentConfigToml::File(f)
            }
            ActivityExternalComponentConfigCanonical::Inline(i) => {
                ActivityExternalComponentConfigToml::Inline(ActivityStubExtInlineConfigToml {
                    name: Some(i.name),
                    ffqn: i.ffqn,
                    params: i.params,
                    return_type: i.return_type,
                })
            }
        })
        .collect();

    let mut activities_js = Vec::with_capacity(deployment.activities_js.len());
    for a in deployment.activities_js {
        let location = script_location_to_toml(a.location, &mut files, &mut external_paths)?;
        activities_js.push(ActivityJsComponentConfigToml {
            name: Some(a.name),
            location,
            content: None,
            content_digest: a.content_digest,
            component_digest: a.component_digest,
            ffqn: a.ffqn,
            params: a.params,
            exec: a.exec,
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars,
            allowed_hosts: a.allowed_hosts,
            return_type: a.return_type,
        });
    }

    let mut activities_exec = Vec::with_capacity(deployment.activities_exec.len());
    for a in deployment.activities_exec {
        let location = script_location_to_toml(a.location, &mut files, &mut external_paths)?;
        activities_exec.push(ActivityExecComponentConfigToml {
            name: Some(a.name),
            location,
            content: None,
            content_digest: a.content_digest,
            ffqn: a.ffqn,
            params: a.params,
            return_type: a.return_type,
            component_digest: a.component_digest,
            exec: a.exec,
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars,
            max_output_bytes: a.max_output_bytes,
            secrets: a.secrets,
        });
    }

    let mut workflows_wasm = Vec::with_capacity(deployment.workflows_wasm.len());
    for mut w in deployment.workflows_wasm {
        w.common.location = wasm_location_to_toml(&w.common.location);
        let backtrace = backtrace_to_toml(w.backtrace, &mut files)?;
        workflows_wasm.push(WorkflowWasmComponentConfigToml {
            common: w.common,
            component_digest: w.component_digest,
            exec: w.exec,
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            backtrace,
            stub_wasi: w.stub_wasi,
            lock_extension: w.lock_extension,
            logs_store_min_level: w.logs_store_min_level,
        });
    }

    let mut workflows_js = Vec::with_capacity(deployment.workflows_js.len());
    for w in deployment.workflows_js {
        let location = script_location_to_toml(w.location, &mut files, &mut external_paths)?;
        workflows_js.push(WorkflowJsComponentConfigToml {
            name: Some(w.name),
            location,
            content: None,
            content_digest: w.content_digest,
            component_digest: w.component_digest,
            ffqn: w.ffqn,
            params: w.params,
            exec: w.exec,
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            lock_extension: w.lock_extension,
            logs_store_min_level: w.logs_store_min_level,
            return_type: w.return_type,
        });
    }

    let mut webhooks_wasm = Vec::with_capacity(deployment.webhooks_wasm.len());
    for mut w in deployment.webhooks_wasm {
        w.common.location = wasm_location_to_toml(&w.common.location);
        let backtrace = backtrace_to_toml(w.backtrace, &mut files)?;
        webhooks_wasm.push(WebhookWasmComponentConfigToml {
            common: w.common,
            http_server: w.http_server,
            routes: w.routes,
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            env_vars: w.env_vars,
            backtrace,
            logs_store_min_level: w.logs_store_min_level,
            allowed_hosts: w.allowed_hosts,
        });
    }

    let mut webhooks_js = Vec::with_capacity(deployment.webhooks_js.len());
    for w in deployment.webhooks_js {
        let location = script_location_to_toml(w.location, &mut files, &mut external_paths)?;
        webhooks_js.push(WebhookJsComponentConfigToml {
            name: w.name,
            location,
            content: None,
            content_digest: w.content_digest,
            http_server: w.http_server,
            routes: w.routes,
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            logs_store_min_level: w.logs_store_min_level,
            env_vars: w.env_vars,
            allowed_hosts: w.allowed_hosts,
        });
    }

    let deployment_toml = DeploymentToml {
        activities_wasm,
        activities_stub,
        activities_external,
        activities_js,
        activities_exec,
        workflows_wasm,
        workflows_js,
        webhooks_wasm,
        webhooks_js,
        crons: deployment.crons,
    };
    Ok(DeploymentExport {
        deployment_toml,
        side_files: files.files,
        external_paths,
    })
}

#[cfg(test)]
mod tests {
    mod blocking_strategy {
        use super::super::*;
        use deployment_config::config::{
            BlockingStrategyAwaitConfig, BlockingStrategyConfigCustomized,
            BlockingStrategyConfigSimple, default_non_blocking_event_batching,
        };
        use serde::Deserialize;

        // Helper struct to deserialize into
        #[derive(Deserialize, Debug, PartialEq)]
        struct TestConfig {
            strategy: BlockingStrategyConfigToml,
        }

        #[test]
        fn deserialize_simple_interrupt() {
            let toml_str = r#"
strategy = "interrupt"
"#;
            let expected = TestConfig {
                strategy: BlockingStrategyConfigToml::Simple(
                    BlockingStrategyConfigSimple::Interrupt,
                ),
            };
            let actual: TestConfig =
                toml::from_str(toml_str).expect("Should parse interrupt string");
            assert_eq!(actual, expected);

            // Verify From impl result
            assert_eq!(
                actual.strategy.into_blocking_strategy(),
                JoinNextBlockingStrategy::Interrupt
            );
        }

        #[test]
        fn deserialize_simple_await() {
            let toml_str = r#"
strategy = "await"
"#;
            let expected = TestConfig {
                strategy: BlockingStrategyConfigToml::Simple(
                    BlockingStrategyConfigSimple::Await, // The default variant of Simple
                ),
            };
            let actual: TestConfig = toml::from_str(toml_str).expect("Should parse await string");
            assert_eq!(actual, expected);

            // Verify From impl result (uses default batching)
            assert_eq!(
                actual.strategy.into_blocking_strategy(),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING
                }
            );
        }

        #[test]
        fn deserialize_tagged_await_default_batching() {
            let toml_str = r#"
strategy = { kind = "await" }
"#;
            let expected = TestConfig {
                strategy: BlockingStrategyConfigToml::Tagged(
                    BlockingStrategyConfigCustomized::Await(BlockingStrategyAwaitConfig {
                        non_blocking_event_batching: default_non_blocking_event_batching(),
                    }),
                ),
            };
            let actual: TestConfig =
                toml::from_str(toml_str).expect("Should parse tagged await with default batching");
            assert_eq!(actual, expected);

            // Verify From impl result (uses default batching)
            assert_eq!(
                actual.strategy.into_blocking_strategy(),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING
                }
            );
        }

        #[test]
        fn deserialize_tagged_await_custom_batching() {
            let toml_str = r#"
strategy = { kind = "await", non_blocking_event_batching = 99 }
"#;
            let expected = TestConfig {
                strategy: BlockingStrategyConfigToml::Tagged(
                    BlockingStrategyConfigCustomized::Await(BlockingStrategyAwaitConfig {
                        non_blocking_event_batching: 99,
                    }),
                ),
            };
            let actual: TestConfig =
                toml::from_str(toml_str).expect("Should parse tagged await with custom batching");
            assert_eq!(actual, expected);

            // Verify From impl result (uses custom batching)
            assert_eq!(
                actual.strategy.into_blocking_strategy(),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 99
                }
            );
        }

        #[test]
        fn deserialize_invalid_string_should_fail() {
            let toml_str = r#"
strategy = "unknown"
"#;
            let result = toml::from_str::<TestConfig>(toml_str);
            assert!(result.is_err(), "Should fail on unknown string");
            // Check for a more specific error if needed, e.g., contains "unknown variant"
        }

        #[test]
        fn deserialize_invalid_kind_in_tagged_should_fail() {
            let toml_str = r#"
strategy = { kind = "interrupt", non_blocking_event_batching = 10 }
"#;
            let result = toml::from_str::<TestConfig>(toml_str);
            assert!(result.is_err(), "Should fail on invalid kind in map");
        }

        #[test]
        fn deserialize_invalid_structure_missing_kind_should_fail() {
            let toml_str = r#"
strategy = { name = "await", non_blocking_event_batching = 10 } # Missing 'kind'
"#;
            let result = toml::from_str::<TestConfig>(toml_str);
            // Fails `Tagged` because 'kind' is missing. Fails `Simple` because it's not a string.
            assert!(result.is_err(), "Should fail on map missing 'kind'");
        }

        #[test]
        fn deserialize_invalid_type_should_fail() {
            let toml_str = r"
strategy = 123
";
            let result = toml::from_str::<TestConfig>(toml_str);
            // Fails `Tagged` because not a map. Fails `Simple` because not a string.
            assert!(result.is_err(), "Should fail on incorrect type (integer)");
        }

        #[test]
        fn deserialize_tagged_await_with_extra_field_should_fail() {
            // TOML allows extra fields by default, Serde ignores them if not in the struct
            let toml_str = r#"
strategy = { kind = "await", non_blocking_event_batching = 25, extra_stuff = "hello" }
"#;
            let result = toml::from_str::<TestConfig>(toml_str);
            assert!(result.is_err(), "Should fail on `extra_stuff`");
        }
    }

    mod component_location {
        use super::super::*;

        #[test]
        fn parse_local_path() {
            let location: ComponentLocationToml = "./my-component.wasm".parse().unwrap();
            assert!(
                matches!(location, ComponentLocationToml::Path(p) if p == "./my-component.wasm")
            );
        }

        #[test]
        fn parse_oci_reference() {
            let location: ComponentLocationToml =
                "oci://ghcr.io/obeli-sk/obelisk:v0.34.1".parse().unwrap();
            assert!(matches!(location, ComponentLocationToml::Oci(_)));
        }
    }

    mod activity_stub {
        use super::super::*;

        #[test]
        fn deserialize_file_mode() {
            let toml_str = r#"
name = "my_stub"
location = "./stub.wasm"
"#;
            let stub: ActivityStubComponentConfigToml = toml::from_str(toml_str).unwrap();
            assert!(matches!(stub, ActivityStubComponentConfigToml::File(_)));
        }

        #[test]
        fn deserialize_inline_mode() {
            let toml_str = r#"
name = "my_stub"
ffqn = "ns:pkg/ifc.fn"
params = [{ name = "id", type = "u64" }]
return_type = "result<string, string>"
"#;
            let stub: ActivityStubComponentConfigToml = toml::from_str(toml_str).unwrap();
            assert!(matches!(stub, ActivityStubComponentConfigToml::Inline(_)));
        }

        #[test]
        fn reject_both_location_and_ffqn() {
            let toml_str = r#"
name = "my_stub"
location = "./stub.wasm"
ffqn = "ns:pkg/ifc.fn"
"#;
            toml::from_str::<ActivityStubComponentConfigToml>(toml_str).unwrap_err();
        }

        #[test]
        fn reject_neither_location_nor_ffqn() {
            let toml_str = r#"
name = "my_stub"
"#;
            toml::from_str::<ActivityStubComponentConfigToml>(toml_str).unwrap_err();
        }
    }

    mod activity_exec {
        use deployment_config::config::SecretEnvVarToml;
        use wasm_workers::activity::activity_exec_worker::ExecProgram;

        use super::super::*;

        fn exec_config_with_secret(value: &str) -> ActivityExecComponentConfigCanonical {
            ActivityExecComponentConfigCanonical {
                name: ConfigName::new(StrVariant::from("exec-test")).unwrap(),
                location: ScriptLocationCanonical::Content {
                    content: "#!/usr/bin/env bash\necho null\n".into(),
                    file_name: "exec-test".into(),
                },
                content_digest: None,
                ffqn: "testing:integration/exec-secret.expose".parse().unwrap(),
                params: vec![],
                return_type: Some("result<string, string>".into()),
                component_digest: None,
                exec: ExecConfigToml::default(),
                max_retries: default_max_retries(),
                retry_exp_backoff: default_retry_exp_backoff(),
                forward_stdout: ComponentStdOutputToml::default(),
                forward_stderr: ComponentStdOutputToml::default(),
                logs_store_min_level: LogLevelToml::default(),
                env_vars: vec![],
                max_output_bytes: default_max_output_bytes(),
                secrets: Some(ExecSecretsToml {
                    env_vars: vec![SecretEnvVarToml {
                        name: "MY_SECRET".into(),
                        value: value.into(),
                    }],
                }),
            }
        }

        fn exec_config_with_source(
            location: ScriptLocationCanonical,
            content_digest: Option<ContentDigest>,
        ) -> ActivityExecComponentConfigCanonical {
            ActivityExecComponentConfigCanonical {
                name: ConfigName::new(StrVariant::from("exec-test")).unwrap(),
                location,
                content_digest,
                ffqn: "testing:integration/exec-secret.expose".parse().unwrap(),
                params: vec![],
                return_type: Some("result<string, string>".into()),
                component_digest: None,
                exec: ExecConfigToml::default(),
                max_retries: default_max_retries(),
                retry_exp_backoff: default_retry_exp_backoff(),
                forward_stdout: ComponentStdOutputToml::default(),
                forward_stderr: ComponentStdOutputToml::default(),
                logs_store_min_level: LogLevelToml::default(),
                env_vars: vec![],
                max_output_bytes: default_max_output_bytes(),
                secrets: None,
            }
        }

        #[test]
        fn fetch_and_verify_activity_exec_secret_fails_when_missing_and_not_ignored() {
            let config = exec_config_with_secret("${MISSING_EXEC_SECRET}");
            let error = config
                .fetch_and_verify(
                    ResolvedExecProgram {
                        program: ExecProgram::Inline("#!/usr/bin/env bash\necho null\n".into()),
                        source_bytes: b"#!/usr/bin/env bash\necho null\n".to_vec(),
                    },
                    false,
                    None,
                )
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("failed to resolve exec secrets"),
                "unexpected error: {error}"
            );
            assert!(
                error.contains("MISSING_EXEC_SECRET"),
                "unexpected error: {error}"
            );
        }

        #[test]
        fn fetch_and_verify_activity_exec_secret_is_skipped_when_missing_and_ignored() {
            let config = exec_config_with_secret("${MISSING_EXEC_SECRET}");
            let verified = config
                .fetch_and_verify(
                    ResolvedExecProgram {
                        program: ExecProgram::Inline("#!/usr/bin/env bash\necho null\n".into()),
                        source_bytes: b"#!/usr/bin/env bash\necho null\n".to_vec(),
                    },
                    true,
                    None,
                )
                .unwrap();
            assert!(verified.secrets.is_none());
        }

        #[test]
        fn fetch_and_verify_activity_exec_hashes_resolved_source_not_oci_reference() {
            let source = b"#!/usr/bin/env bash\necho null\n".to_vec();
            let inline = exec_config_with_source(
                ScriptLocationCanonical::Content {
                    content: String::from_utf8(source.clone()).unwrap(),
                    file_name: "exec-test".into(),
                },
                None,
            );
            let oci = exec_config_with_source(
                ScriptLocationCanonical::Oci {
                    image: "registry.example.com/ns/exec:latest".into(),
                },
                None,
            );

            let inline_verified = inline
                .fetch_and_verify(
                    ResolvedExecProgram {
                        program: ExecProgram::Inline(String::from_utf8(source.clone()).unwrap()),
                        source_bytes: source.clone(),
                    },
                    true,
                    None,
                )
                .unwrap();
            let oci_verified = oci
                .fetch_and_verify(
                    ResolvedExecProgram {
                        program: ExecProgram::CachedFile(std::path::PathBuf::from(
                            "/tmp/fake-exec-script.sh",
                        )),
                        source_bytes: source,
                    },
                    true,
                    None,
                )
                .unwrap();

            assert_eq!(inline_verified.component_id, oci_verified.component_id);
        }

        #[tokio::test]
        async fn resolve_activity_exec_validates_inline_content_digest() {
            let config = exec_config_with_source(
                ScriptLocationCanonical::Content {
                    content: "#!/usr/bin/env bash\necho null\n".into(),
                    file_name: "exec-test".into(),
                },
                Some(
                    "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                        .parse()
                        .unwrap(),
                ),
            );
            let error = config
                .resolve(std::path::Path::new("/tmp"))
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("content digest mismatch"),
                "unexpected error: {error}"
            );
        }
    }

    mod script_location {
        use super::super::*;

        fn digest_of(bytes: &[u8]) -> ContentDigest {
            let hash: [u8; 32] = Sha256::digest(bytes).into();
            ContentDigest(Digest(hash))
        }

        #[tokio::test]
        async fn inline_content_becomes_owned() {
            let dir = tempfile::tempdir().unwrap();
            let location = resolve_script_toml_to_canonical(
                None,
                Some("export const x = 1;".to_string()),
                "foo.js".to_string(),
                dir.path(),
                None,
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationCanonical::Content { content, file_name }
                    if content == "export const x = 1;" && file_name == "foo.js"
            );
        }

        #[tokio::test]
        async fn relative_file_is_owned_and_mirrors_subpath() {
            let dir = tempfile::tempdir().unwrap();
            let sub = dir.path().join("scripts");
            std::fs::create_dir_all(&sub).unwrap();
            std::fs::write(sub.join("a.js"), "owned content").unwrap();

            // Bare relative path (implicit `${DEPLOYMENT_DIR}` prefix).
            let location = resolve_script_toml_to_canonical(
                Some(JsLocationToml::Path("scripts/a.js".to_string())),
                None,
                "ignored.js".to_string(),
                dir.path(),
                None,
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationCanonical::Content { content, file_name }
                    if content == "owned content" && file_name == "scripts/a.js"
            );
        }

        #[tokio::test]
        async fn explicit_deployment_dir_prefix_is_owned() {
            let dir = tempfile::tempdir().unwrap();
            let sub = dir.path().join("scripts");
            std::fs::create_dir_all(&sub).unwrap();
            std::fs::write(sub.join("a.js"), "owned content").unwrap();

            let location = resolve_script_toml_to_canonical(
                Some(JsLocationToml::Path(
                    "${DEPLOYMENT_DIR}/scripts/a.js".to_string(),
                )),
                None,
                "ignored.js".to_string(),
                dir.path(),
                None,
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationCanonical::Content { file_name, .. } if file_name == "scripts/a.js"
            );
        }

        #[tokio::test]
        async fn absolute_path_is_external() {
            let root = tempfile::tempdir().unwrap();
            let outside = root.path().join("outside.js");
            std::fs::write(&outside, "external content").unwrap();
            let abs = outside.to_string_lossy().into_owned();

            let location = resolve_script_toml_to_canonical(
                Some(JsLocationToml::Path(abs.clone())),
                None,
                "ignored.js".to_string(),
                root.path(),
                None,
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationCanonical::Path { path } if path == abs
            );
        }

        #[tokio::test]
        async fn parent_dir_escape_is_rejected() {
            let dir = tempfile::tempdir().unwrap();
            for raw in ["../escape.js", "${DEPLOYMENT_DIR}/../escape.js"] {
                let err = resolve_script_toml_to_canonical(
                    Some(JsLocationToml::Path(raw.to_string())),
                    None,
                    "ignored.js".to_string(),
                    dir.path(),
                    None,
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(err.contains("`..`"), "unexpected error for `{raw}`: {err}");
            }
        }

        #[tokio::test]
        async fn oci_becomes_oci() {
            let dir = tempfile::tempdir().unwrap();
            let reference =
                oci_client::Reference::from_str("docker.io/library/example:latest").unwrap();
            let location = resolve_script_toml_to_canonical(
                Some(JsLocationToml::Oci(reference)),
                None,
                "ignored.js".to_string(),
                dir.path(),
                None,
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationCanonical::Oci { image }
                    if image == "docker.io/library/example:latest"
            );
        }

        #[tokio::test]
        async fn content_digest_verified_at_submit() {
            let dir = tempfile::tempdir().unwrap();
            let content = "export const x = 1;";

            // Matching digest succeeds.
            resolve_script_toml_to_canonical(
                None,
                Some(content.to_string()),
                "foo.js".to_string(),
                dir.path(),
                Some(&digest_of(content.as_bytes())),
            )
            .await
            .expect("matching digest should pass");

            // Mismatching digest fails.
            let wrong = digest_of(b"different");
            let err = resolve_script_toml_to_canonical(
                None,
                Some(content.to_string()),
                "foo.js".to_string(),
                dir.path(),
                Some(&wrong),
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("content digest mismatch"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn external_file_content_digest_verified_at_submit() {
            let root = tempfile::tempdir().unwrap();
            let inner = root.path().join("inner");
            std::fs::create_dir_all(&inner).unwrap();
            let outside = root.path().join("outside.js");
            std::fs::write(&outside, "external content").unwrap();

            let wrong = digest_of(b"nope");
            let err = resolve_script_toml_to_canonical(
                Some(JsLocationToml::Path(outside.to_string_lossy().into_owned())),
                None,
                "ignored.js".to_string(),
                &inner,
                Some(&wrong),
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("content digest mismatch"),
                "unexpected error: {err}"
            );
        }
    }

    mod export {
        use super::super::*;

        #[test]
        fn script_content_emits_side_file_and_hint() {
            let mut files = SideFileCollector::default();
            let mut external = Vec::new();
            let loc = script_location_to_toml(
                ScriptLocationCanonical::Content {
                    content: "console.log(1)".to_string(),
                    file_name: "scripts/a.js".to_string(),
                },
                &mut files,
                &mut external,
            )
            .unwrap();
            assert_matches::assert_matches!(
                loc,
                Some(JsLocationToml::Path(p)) if p == "${DEPLOYMENT_DIR}/scripts/a.js"
            );
            assert_eq!(files.files.len(), 1);
            assert_eq!(files.files[0].rel_path, "scripts/a.js");
            assert_eq!(files.files[0].content, "console.log(1)");
            assert!(external.is_empty());
        }

        #[test]
        fn external_path_is_referenced_verbatim_without_side_file() {
            let mut files = SideFileCollector::default();
            let mut external = Vec::new();
            let loc = script_location_to_toml(
                ScriptLocationCanonical::Path {
                    path: "/abs/elsewhere/lib.js".to_string(),
                },
                &mut files,
                &mut external,
            )
            .unwrap();
            assert_matches::assert_matches!(
                loc,
                Some(JsLocationToml::Path(p)) if p == "/abs/elsewhere/lib.js"
            );
            assert!(files.files.is_empty());
            assert_eq!(external, vec!["/abs/elsewhere/lib.js".to_string()]);
        }

        #[test]
        fn oci_passthrough() {
            let mut files = SideFileCollector::default();
            let mut external = Vec::new();
            let loc = script_location_to_toml(
                ScriptLocationCanonical::Oci {
                    image: "docker.io/library/example:latest".to_string(),
                },
                &mut files,
                &mut external,
            )
            .unwrap();
            assert_matches::assert_matches!(
                loc,
                Some(JsLocationToml::Oci(r)) if r.to_string() == "docker.io/library/example:latest"
            );
            assert!(files.files.is_empty());
        }

        #[test]
        fn wasm_path_becomes_basename_hint() {
            let rewritten = wasm_location_to_toml(&ComponentLocationToml::Path(
                "/deployment/dir/component.wasm".to_string(),
            ));
            assert_matches::assert_matches!(
                rewritten,
                ComponentLocationToml::Path(p) if p == "${DEPLOYMENT_DIR}/component.wasm"
            );
            let oci = wasm_location_to_toml(&ComponentLocationToml::Oci(
                "docker.io/library/example:latest".to_string(),
            ));
            assert_matches::assert_matches!(
                oci,
                ComponentLocationToml::Oci(r) if r == "docker.io/library/example:latest"
            );
        }

        #[test]
        fn duplicate_basename_with_distinct_content_errors() {
            let mut files = SideFileCollector::default();
            files.push("a.js".to_string(), "one".to_string()).unwrap();
            // Same name, same content is deduplicated.
            files.push("a.js".to_string(), "one".to_string()).unwrap();
            assert_eq!(files.files.len(), 1);
            // Same name, different content is an error.
            let err = files
                .push("a.js".to_string(), "two".to_string())
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("multiple distinct source files"),
                "unexpected error: {err}"
            );
        }

        fn js_activity(
            name: &str,
            location: ScriptLocationCanonical,
        ) -> ActivityJsComponentConfigCanonical {
            ActivityJsComponentConfigCanonical {
                name: ConfigName::new(StrVariant::from(name.to_string())).unwrap(),
                location,
                content_digest: None,
                component_digest: None,
                ffqn: "ns:pkg/ifc.fn".parse().unwrap(),
                params: vec![],
                exec: ExecConfigToml::default(),
                max_retries: default_max_retries(),
                retry_exp_backoff: default_retry_exp_backoff(),
                forward_stdout: ComponentStdOutputToml::default(),
                forward_stderr: ComponentStdOutputToml::default(),
                logs_store_min_level: LogLevelToml::default(),
                env_vars: vec![],
                allowed_hosts: vec![],
                return_type: None,
            }
        }

        #[test]
        fn deployment_canonical_to_toml_end_to_end() {
            let mut deployment = DeploymentCanonical::default();
            deployment.activities_js.push(js_activity(
                "owned",
                ScriptLocationCanonical::Content {
                    content: "export const a = 1;".to_string(),
                    file_name: "scripts/owned.js".to_string(),
                },
            ));
            deployment.activities_js.push(js_activity(
                "external",
                ScriptLocationCanonical::Path {
                    path: "/somewhere/external.js".to_string(),
                },
            ));

            let export = deployment_canonical_to_toml(deployment).unwrap();

            // Only the owned script is recreated, mirroring its subpath.
            assert_eq!(export.side_files.len(), 1);
            assert_eq!(export.side_files[0].rel_path, "scripts/owned.js");
            // The external reference is reported and emitted verbatim.
            assert_eq!(
                export.external_paths,
                vec!["/somewhere/external.js".to_string()]
            );

            let toml_config = &export.deployment_toml;
            assert_eq!(toml_config.activities_js.len(), 2);
            for c in &toml_config.activities_js {
                assert!(c.content.is_none());
                assert!(c.name.is_some());
                assert_matches::assert_matches!(&c.location, Some(JsLocationToml::Path(_)));
            }

            // The result serializes to TOML.
            let serialized = toml::to_string_pretty(toml_config).unwrap();
            assert!(serialized.contains("[[activity_js]]"));
        }
    }

    mod backtrace {
        use super::super::*;

        #[test]
        fn wasm_deployment_dir_escape_rejected_but_subpath_ok() {
            let dir = std::path::Path::new("/dep");

            let mut escape = "${DEPLOYMENT_DIR}/../evil.wasm".to_string();
            let err = format!(
                "{:#}",
                DeploymentToml::expand_deployment_dir(&mut escape, dir).unwrap_err()
            );
            assert!(err.contains("`..`"), "unexpected error: {err}");

            let mut ok = "${DEPLOYMENT_DIR}/components/a.wasm".to_string();
            DeploymentToml::expand_deployment_dir(&mut ok, dir).unwrap();
            assert_eq!(ok, "/dep/components/a.wasm");

            // Bare relative paths are anchored to the deployment dir too.
            let mut bare = "components/a.wasm".to_string();
            DeploymentToml::expand_deployment_dir(&mut bare, dir).unwrap();
            assert_eq!(bare, "/dep/components/a.wasm");

            let mut bare_escape = "../evil.wasm".to_string();
            let err = format!(
                "{:#}",
                DeploymentToml::expand_deployment_dir(&mut bare_escape, dir).unwrap_err()
            );
            assert!(err.contains("`..`"), "unexpected error: {err}");

            // Absolute paths are left untouched (out-of-tree reference).
            let mut abs = "/other/a.wasm".to_string();
            DeploymentToml::expand_deployment_dir(&mut abs, dir).unwrap();
            assert_eq!(abs, "/other/a.wasm");
        }

        #[tokio::test]
        async fn canonical_retains_relative_subpath() {
            let dir = tempfile::tempdir().unwrap();
            let sub = dir.path().join("crates/foo/src");
            std::fs::create_dir_all(&sub).unwrap();
            std::fs::write(sub.join("lib.rs"), "SRC").unwrap();

            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources.insert(
                ".../src/lib.rs".to_string(),
                BacktraceSourceLocation::Path(
                    "${DEPLOYMENT_DIR}/crates/foo/src/lib.rs".to_string(),
                ),
            );
            let canon = resolve_backtrace_to_canonical(&bt, dir.path())
                .await
                .unwrap();
            let src = canon.frame_files_to_sources.get(".../src/lib.rs").unwrap();
            assert_eq!(src.content, "SRC");
            assert_eq!(src.file_name, "crates/foo/src/lib.rs");
        }

        #[tokio::test]
        async fn bare_relative_source_is_deployment_dir_relative() {
            // A bare relative backtrace source (no `${DEPLOYMENT_DIR}` prefix) is resolved
            // against the deployment dir, same as the explicit-prefix form.
            let dir = tempfile::tempdir().unwrap();
            let sub = dir.path().join("crates/foo/src");
            std::fs::create_dir_all(&sub).unwrap();
            std::fs::write(sub.join("lib.rs"), "SRC").unwrap();

            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources.insert(
                ".../src/lib.rs".to_string(),
                BacktraceSourceLocation::Path("crates/foo/src/lib.rs".to_string()),
            );
            let canon = resolve_backtrace_to_canonical(&bt, dir.path())
                .await
                .unwrap();
            let src = canon.frame_files_to_sources.get(".../src/lib.rs").unwrap();
            assert_eq!(src.content, "SRC");
            assert_eq!(src.file_name, "crates/foo/src/lib.rs");
        }

        #[tokio::test]
        async fn source_parent_dir_escape_is_rejected() {
            let dir = tempfile::tempdir().unwrap();
            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources.insert(
                "frame".to_string(),
                BacktraceSourceLocation::Path("${DEPLOYMENT_DIR}/../escape.rs".to_string()),
            );
            let err = format!(
                "{:#}",
                resolve_backtrace_to_canonical(&bt, dir.path())
                    .await
                    .unwrap_err()
            );
            assert!(err.contains("`..`"), "unexpected error: {err}");
        }

        #[tokio::test]
        async fn absolute_source_is_mirrored_root_stripped() {
            // An absolute backtrace source path is recreated self-contained under a
            // root-stripped relative mirror, so it never escapes the output directory.
            let dir = tempfile::tempdir().unwrap();
            let abs = dir.path().join("nested/lib.rs");
            std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
            std::fs::write(&abs, "SRC").unwrap();

            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources.insert(
                ".../src/lib.rs".to_string(),
                BacktraceSourceLocation::Path(abs.to_string_lossy().into_owned()),
            );
            // Deployment dir is unrelated to the absolute source.
            let other_dir = tempfile::tempdir().unwrap();
            let canon = resolve_backtrace_to_canonical(&bt, other_dir.path())
                .await
                .unwrap();
            let src = canon.frame_files_to_sources.get(".../src/lib.rs").unwrap();
            assert_eq!(src.content, "SRC");
            // Root/drive stripped, remaining components preserved, no leading separator.
            let expected = mirror_path_as_relative(&abs.to_string_lossy()).unwrap();
            assert_eq!(src.file_name, expected);
            assert!(!src.file_name.starts_with('/'));
            assert!(src.file_name.ends_with("nested/lib.rs"));
        }

        #[test]
        fn export_mirrors_subpaths_without_basename_collision() {
            // Two components whose frame keys share a basename (`lib.rs`) but live in
            // distinct subfolders: must produce two side files, not a collision error.
            let mut files = SideFileCollector::default();
            let mut bt = ComponentBacktraceConfigCanonical::default();
            bt.frame_files_to_sources.insert(
                "a/.../lib.rs".to_string(),
                BacktraceSourceCanonical {
                    content: "A".to_string(),
                    file_name: "fibo/workflow/src/lib.rs".to_string(),
                },
            );
            bt.frame_files_to_sources.insert(
                "b/.../lib.rs".to_string(),
                BacktraceSourceCanonical {
                    content: "B".to_string(),
                    file_name: "fibo/webhook/src/lib.rs".to_string(),
                },
            );
            let toml_bt = backtrace_to_toml(bt, &mut files).unwrap();
            assert_eq!(files.files.len(), 2);
            assert_eq!(toml_bt.frame_files_to_sources.len(), 2);
            for loc in toml_bt.frame_files_to_sources.values() {
                let BacktraceSourceLocation::Path(p) = loc;
                assert!(p.starts_with("${DEPLOYMENT_DIR}/"), "unexpected hint: {p}");
            }
        }
    }
}
