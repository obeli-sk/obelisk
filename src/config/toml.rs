use super::{config_holder::PathPrefixes, env_var::EnvVarConfig};
use crate::command::server::FrameFilesToSourceContent;
use crate::config::config_holder::{CACHE_DIR_PREFIX, DATA_DIR_PREFIX};
use crate::config::env_var::{
    EnvVarMissing, EnvVarsMissing, interpolate_env_vars_plaintext, interpolate_env_vars_secret,
};
use crate::config::toml::cron::CronComponentConfigToml;
use crate::oci;
use anyhow::{Context, ensure};
use anyhow::{anyhow, bail};
use concepts::ContentDigest;
use concepts::ReturnType;
use concepts::component_id::Digest;
use concepts::{
    ComponentId, ComponentRetryConfig, ComponentType, FunctionFqn, InvalidNameError, StrVariant,
    check_name, component_id::ComponentDigest, prefixed_ulid::ExecutorId, storage::LogLevel,
};
use db_postgres::postgres_dao::{self, PostgresConfig};
use db_sqlite::sqlite_dao::SqliteConfig;
use hashbrown::HashMap;
use log::{LoggingConfig, LoggingStyle};
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Deserializer, Serialize};
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
    pub(crate) workflows: Vec<WorkflowWasmComponentConfigToml>,
    #[serde(default, rename = "workflow_js")]
    pub(crate) workflows_js: Vec<WorkflowJsComponentConfigToml>,
    #[serde(default, rename = "webhook_endpoint_wasm")]
    pub(crate) webhooks: Vec<WebhookWasmComponentConfigToml>,
    #[serde(default, rename = "webhook_endpoint_js")]
    pub(crate) webhooks_js: Vec<WebhookJsComponentConfigToml>,
    #[serde(default, rename = "cron")]
    pub(crate) crons: Vec<CronComponentConfigToml>,
}

/// A `DeploymentToml` that has passed name-uniqueness validation.
///
/// Components that support auto-derived names (`activity_js`, `activity_exec`,
/// `workflow_js`) are stored as `(Config, ConfigName)` tuples with the resolved name
/// pulled out of the `Option`. The remaining component types stay inside `inner`.
#[derive(Default)]
pub(crate) struct DeploymentTomlValidated {
    pub(crate) inner: DeploymentToml,
    pub(crate) activities_js: Vec<(ActivityJsComponentConfigToml, ConfigName)>,
    pub(crate) activities_exec: Vec<(ActivityExecComponentConfigToml, ConfigName)>,
    pub(crate) activities_stub: Vec<(ActivityStubComponentConfigToml, ConfigName)>,
    pub(crate) activities_external: Vec<(ActivityExternalComponentConfigToml, ConfigName)>,
    pub(crate) workflows_js: Vec<(WorkflowJsComponentConfigToml, ConfigName)>,
    pub(crate) component_type_by_name: hashbrown::HashMap<String, crate::args::TomlComponentType>,
}

impl DeploymentToml {
    /// Consume `self`, expand `${DEPLOYMENT_DIR}/` prefixes in WASM component paths,
    /// verify that every component name is unique, and return a `DeploymentTomlValidated`
    /// that also carries the name→type index and the deployment directory.
    pub(crate) fn validate(
        mut self,
        deployment_dir: &std::path::Path,
    ) -> Result<DeploymentTomlValidated, anyhow::Error> {
        self.expand_deployment_dir_prefix(deployment_dir);
        // Resolve optional names from FFQN and drain vectors out of `self`.
        let activities_js = Self::resolve_names(std::mem::take(&mut self.activities_js))?;
        let activities_exec = Self::resolve_names(std::mem::take(&mut self.activities_exec))?;
        let activities_stub = Self::resolve_stub_names(std::mem::take(&mut self.activities_stub))?;
        let activities_external =
            Self::resolve_external_names(std::mem::take(&mut self.activities_external))?;
        let workflows_js = Self::resolve_names(std::mem::take(&mut self.workflows_js))?;
        // Build the name→type index and check for duplicates.
        let mut component_type_by_name = hashbrown::HashMap::new();
        // Components remaining in `inner` (all have mandatory names).
        for (name, component_type) in self.all_component_names_with_types() {
            if component_type_by_name
                .insert(name.to_string(), component_type)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        // Components with resolved names.
        use crate::args::TomlComponentType;
        for (_, name) in &activities_js {
            if component_type_by_name
                .insert(name.to_string(), TomlComponentType::ActivityJs)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_exec {
            if component_type_by_name
                .insert(name.to_string(), TomlComponentType::ActivityExec)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_stub {
            if component_type_by_name
                .insert(name.to_string(), TomlComponentType::ActivityStub)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_external {
            if component_type_by_name
                .insert(name.to_string(), TomlComponentType::ActivityExternal)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &workflows_js {
            if component_type_by_name
                .insert(name.to_string(), TomlComponentType::WorkflowJs)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        Ok(DeploymentTomlValidated {
            inner: self,
            activities_js,
            activities_exec,
            activities_stub,
            activities_external,
            workflows_js,
            component_type_by_name,
        })
    }

    fn resolve_names<T: HasOptionalNameAndFfqn>(
        configs: Vec<T>,
    ) -> Result<Vec<(T, ConfigName)>, anyhow::Error> {
        let mut resolved_names: hashbrown::HashMap<String, Vec<String>> = hashbrown::HashMap::new();
        for c in &configs {
            if c.config_name().is_none() {
                let derived = ConfigName::from_ffqn(c.ffqn());
                resolved_names
                    .entry(derived.0.as_ref().to_string())
                    .or_default()
                    .push(c.ffqn().to_string());
            }
        }
        for (name, ffqns) in &resolved_names {
            if ffqns.len() > 1 {
                bail!(
                    "multiple components derive the same name `{name}` from their FFQNs: {}. \
                     Please specify an explicit `name` for each of these components",
                    ffqns.join(", ")
                );
            }
        }

        Ok(configs
            .into_iter()
            .map(|c| {
                let name = c
                    .config_name()
                    .cloned()
                    .unwrap_or_else(|| ConfigName::from_ffqn(c.ffqn()));
                (c, name)
            })
            .collect())
    }

    /// Resolve names for `ActivityStubComponentConfigToml` enum variants.
    /// File variants always have an explicit name; Inline variants may derive from FFQN.
    fn resolve_stub_names(
        configs: Vec<ActivityStubComponentConfigToml>,
    ) -> Result<Vec<(ActivityStubComponentConfigToml, ConfigName)>, anyhow::Error> {
        // Check for clashes among auto-derived inline names.
        let mut auto_names: hashbrown::HashMap<String, Vec<String>> = hashbrown::HashMap::new();
        for c in &configs {
            if let ActivityStubComponentConfigToml::Inline(inline) = c
                && inline.name.is_none()
            {
                let derived = ConfigName::from_ffqn(&inline.ffqn);
                auto_names
                    .entry(derived.0.as_ref().to_string())
                    .or_default()
                    .push(inline.ffqn.to_string());
            }
        }
        for (name, ffqns) in &auto_names {
            if ffqns.len() > 1 {
                bail!(
                    "multiple components derive the same name `{name}` from their FFQNs: {}. \
                     Please specify an explicit `name` for each of these components",
                    ffqns.join(", ")
                );
            }
        }
        Ok(configs
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
            .collect())
    }

    /// Resolve names for `ActivityExternalComponentConfigToml` enum variants.
    fn resolve_external_names(
        configs: Vec<ActivityExternalComponentConfigToml>,
    ) -> Result<Vec<(ActivityExternalComponentConfigToml, ConfigName)>, anyhow::Error> {
        let mut auto_names: hashbrown::HashMap<String, Vec<String>> = hashbrown::HashMap::new();
        for c in &configs {
            if let ActivityExternalComponentConfigToml::Inline(inline) = c
                && inline.name.is_none()
            {
                let derived = ConfigName::from_ffqn(&inline.ffqn);
                auto_names
                    .entry(derived.0.as_ref().to_string())
                    .or_default()
                    .push(inline.ffqn.to_string());
            }
        }
        for (name, ffqns) in &auto_names {
            if ffqns.len() > 1 {
                bail!(
                    "multiple components derive the same name `{name}` from their FFQNs: {}. \
                     Please specify an explicit `name` for each of these components",
                    ffqns.join(", ")
                );
            }
        }
        Ok(configs
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
            .collect())
    }

    fn expand_deployment_dir(s: &mut String, is_dir: bool, deployment_dir: &std::path::Path) {
        if let Some(suffix) = s.strip_prefix("${DEPLOYMENT_DIR}") {
            // if is_dir, we allow suffix to be empty. Otherwise we expect `/`
            if is_dir && suffix.is_empty() {
                *s = deployment_dir.to_string_lossy().into_owned();
            } else if let Some(suffix) = suffix.strip_prefix("/") {
                *s = deployment_dir.join(suffix).to_string_lossy().into_owned();
            }
        }
    }

    /// Expand `${DEPLOYMENT_DIR}` prefixes in all local file paths.
    fn expand_deployment_dir_prefix(&mut self, deployment_dir: &std::path::Path) {
        let expand_loc = |loc: &mut ComponentLocationToml| {
            if let ComponentLocationToml::Path(p) = loc {
                Self::expand_deployment_dir(p, false, deployment_dir);
            }
        };
        let expand_backtrace = |bt: &mut ComponentBacktraceConfig| {
            for loc in bt.frame_files_to_sources.values_mut() {
                let BacktraceSourceLocation::Path(p) = loc;
                Self::expand_deployment_dir(p, false, deployment_dir);
            }
        };
        for c in &mut self.activities_wasm {
            expand_loc(&mut c.common.location);
        }
        for c in &mut self.activities_stub {
            if let ActivityStubComponentConfigToml::File(c) = c {
                expand_loc(&mut c.common.location);
            }
        }
        for c in &mut self.activities_external {
            if let ActivityExternalComponentConfigToml::File(c) = c {
                expand_loc(&mut c.common.location);
            }
        }
        for c in &mut self.activities_js {
            if let JsLocationToml::Path(p) = &mut c.location {
                Self::expand_deployment_dir(p, false, deployment_dir);
            }
        }
        for c in &mut self.activities_exec {
            if let ExecProgramToml::Include(p) = &mut c.program {
                Self::expand_deployment_dir(p, false, deployment_dir);
            }
            if let Some(cwd) = &mut c.cwd {
                Self::expand_deployment_dir(cwd, true, deployment_dir);
            }
        }
        for c in &mut self.workflows {
            expand_loc(&mut c.common.location);
            expand_backtrace(&mut c.backtrace);
        }
        for c in &mut self.workflows_js {
            if let JsLocationToml::Path(p) = &mut c.location {
                Self::expand_deployment_dir(p, false, deployment_dir);
            }
        }
        for c in &mut self.webhooks {
            expand_loc(&mut c.common.location);
            expand_backtrace(&mut c.backtrace);
        }
        for c in &mut self.webhooks_js {
            if let JsLocationToml::Path(p) = &mut c.location {
                Self::expand_deployment_dir(p, false, deployment_dir);
            }
        }
    }

    fn all_component_names_with_types(
        &self,
    ) -> impl Iterator<Item = (&str, crate::args::TomlComponentType)> {
        use crate::args::TomlComponentType;
        self.activities_wasm
            .iter()
            .map(|c| (c.common.name.0.as_ref(), TomlComponentType::ActivityWasm))
            // activities_stub, activities_external, activities_js, activities_exec,
            // workflows_js are handled separately via DeploymentTomlValidated's
            // resolved-name fields.
            .chain(
                self.workflows
                    .iter()
                    .map(|c| (c.common.name.0.as_ref(), TomlComponentType::WorkflowWasm)),
            )
            .chain(self.webhooks.iter().map(|c| {
                (
                    c.common.name.0.as_ref(),
                    TomlComponentType::WebhookEndpointWasm,
                )
            }))
            .chain(
                self.webhooks_js
                    .iter()
                    .map(|c| (c.name.0.as_ref(), TomlComponentType::WebhookEndpointJs)),
            )
            .chain(
                self.crons
                    .iter()
                    .map(|c| (c.name.0.as_ref(), TomlComponentType::Cron)),
            )
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
    #[serde(default = "default_cancel_watcher_enabled")]
    pub(crate) enabled: bool,
    #[serde(default = "default_cancel_watcher_tick_sleep")]
    pub(crate) tick_sleep: DurationConfig,
}
impl Default for CancelWatcherTomlConfig {
    fn default() -> Self {
        Self {
            enabled: default_cancel_watcher_enabled(),
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

#[derive(
    Debug, Clone, Hash, JsonSchema, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
#[serde(rename_all = "snake_case")]
#[schemars(with = "String")]
pub(crate) enum ComponentLocationToml {
    Path(String), // String because it can contain path prefix - $DEPLOYMENT_DIR/
    Oci(oci_client::Reference),
}
impl ComponentLocationToml {
    /// Fetch wasm file and calculate its content digest.
    ///
    /// Read wasm file either from local fs, or pull from an OCI registry and cache it.
    pub(crate) async fn fetch(
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
                let (digest, path, _, _) =
                    oci::pull_to_cache_dir(image, wasm_cache_dir, metadata_dir)
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
pub(crate) const OCI_SCHEMA_PREFIX: &str = "oci://";
impl FromStr for ComponentLocationToml {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(location) = s.strip_prefix(OCI_SCHEMA_PREFIX) {
            Ok(ComponentLocationToml::Oci(
                oci_client::Reference::from_str(location)
                    .map_err(|e| anyhow::anyhow!("invalid OCI reference: {e}"))?,
            ))
        } else {
            Ok(ComponentLocationToml::Path(s.to_string()))
        }
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

impl std::fmt::Display for ComponentLocationToml {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentLocationToml::Path(p) => write!(f, "{p}"),
            ComponentLocationToml::Oci(r) => write!(f, "{OCI_SCHEMA_PREFIX}{r}"),
        }
    }
}

/// Location of a JavaScript source file.
/// Supports local file paths and OCI registry references (`oci://...`).
/// On-disk format only; replaced by [`JsLocationCanonical`] before transmission and hash computation.
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

/// Activity, Webhook, Workflow or a Http server
#[derive(
    Debug,
    Clone,
    Hash,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::Into,
    JsonSchema,
    derive_more::Deref,
)]
#[display("{_0}")]
pub struct ConfigName(#[schemars(with = "String")] StrVariant);
impl ConfigName {
    pub fn new(name: StrVariant) -> Result<Self, InvalidNameError<ConfigName>> {
        Ok(Self(check_name(name, "_.-")?))
    }
}
impl<'de> Deserialize<'de> for ConfigName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name = String::deserialize(deserializer)?;
        ConfigName::new(StrVariant::from(name)).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for ConfigName {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}

impl ConfigName {
    /// Derive a `ConfigName` from a `FunctionFqn` using the short form `{ifc_name}.{function_name}`.
    pub(crate) fn from_ffqn(ffqn: &FunctionFqn) -> Self {
        let ifc_name = ffqn.ifc_fqn.ifc_name();
        let function_name: &str = &ffqn.function_name;
        // WIT identifiers are kebab-case ([a-z0-9-]), so the derived name
        // contains only [a-z0-9-.] — always valid for ConfigName.
        Self(StrVariant::from(format!("{ifc_name}.{function_name}")))
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentCommon {
    pub(crate) name: ConfigName,
    pub(crate) location: ComponentLocationToml,
}

impl ComponentCommon {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LockingStrategy {
    ByFfqns,
    ByComponentDigest,
}
impl From<LockingStrategy> for executor::executor::LockingStrategy {
    fn from(value: LockingStrategy) -> Self {
        match value {
            LockingStrategy::ByFfqns => executor::executor::LockingStrategy::ByFfqns,
            LockingStrategy::ByComponentDigest => {
                executor::executor::LockingStrategy::ByComponentDigest
            }
        }
    }
}
impl From<executor::executor::LockingStrategy> for LockingStrategy {
    fn from(value: executor::executor::LockingStrategy) -> Self {
        match value {
            executor::executor::LockingStrategy::ByFfqns => LockingStrategy::ByFfqns,
            executor::executor::LockingStrategy::ByComponentDigest => {
                LockingStrategy::ByComponentDigest
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecConfigToml {
    #[serde(default = "default_batch_size")]
    batch_size: u32,
    #[serde(default = "default_lock_expiry")]
    pub(crate) lock_expiry: DurationConfig,
    #[serde(default = "default_tick_sleep")]
    tick_sleep: DurationConfig,
    #[serde(default)]
    locking_strategy: Option<LockingStrategy>,
}

impl Default for ExecConfigToml {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            lock_expiry: default_lock_expiry(),
            tick_sleep: default_tick_sleep(),
            locking_strategy: None,
        }
    }
}

impl ExecConfigToml {
    pub(crate) fn into_exec_exec_config(
        self,
        component_id: ComponentId,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        retry_config: ComponentRetryConfig,
    ) -> Result<executor::executor::ExecConfig, anyhow::Error> {
        Ok(executor::executor::ExecConfig {
            lock_expiry: self.lock_expiry.into(),
            tick_sleep: self.tick_sleep.into(),
            batch_size: self.batch_size,
            locking_strategy: locking_strategy(self.locking_strategy, component_id.component_type)?,
            component_id,
            task_limiter: global_executor_instance_limiter,
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
    Ok(locking_strategy_override.map(executor::executor::LockingStrategy::from).unwrap_or_else(||
    match component_type {
        ComponentType::Activity => executor::executor::LockingStrategy::ByFfqns,
        ComponentType::Workflow => executor::executor::LockingStrategy::ByComponentDigest,
        other => unreachable!(
            "unexpected type {other}, only workflows, activities, and crons expose locking strategy"
        ),
    }))
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityWasmComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    /// Override the auto-computed component digest used for locking.
    /// If set, this value is used instead of the content digest of the WASM file.
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
    pub(crate) env_vars: Vec<EnvVarConfig>,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default, rename = "allowed_host")]
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LogLevelToml {
    Off,
    Trace,
    #[default]
    Debug,
    Info,
    Warn,
    Error,
}
impl From<LogLevelToml> for Option<LogLevel> {
    fn from(value: LogLevelToml) -> Self {
        match value {
            LogLevelToml::Off => None,
            LogLevelToml::Trace => Some(LogLevel::Trace),
            LogLevelToml::Debug => Some(LogLevel::Debug),
            LogLevelToml::Info => Some(LogLevel::Info),
            LogLevelToml::Warn => Some(LogLevel::Warn),
            LogLevelToml::Error => Some(LogLevel::Error),
        }
    }
}

/// Where in the outgoing request placeholders are replaced.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplaceIn {
    Headers,
    Body,
    Params,
}

/// Input for method restrictions in TOML configuration.
/// Supports both `methods = "*"` and `methods = ["GET", "POST"]` syntax.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum MethodsInput {
    /// All methods allowed (from `methods = "*"`).
    Star(MethodsInputStar),
    /// Specific methods list (from `methods = ["GET", "POST"]`).
    List(Vec<String>),
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct MethodsInputStar(
    #[serde(
        deserialize_with = "deserialize_star",
        serialize_with = "serialize_star"
    )]
    (),
);

fn deserialize_star<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s == "*" {
        Ok(())
    } else {
        Err(serde::de::Error::custom(format!(
            "expected \"*\", got \"{s}\""
        )))
    }
}

fn serialize_star<S: serde::Serializer>(_: &(), s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str("*")
}

/// An allowed outgoing HTTP host with optional method restrictions and secrets.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct AllowedHostToml {
    /// Host pattern (e.g. `"api.example.com"`, `"*.example.com"`, `"http://localhost:8080"`).
    pub pattern: String,
    /// Allowed HTTP methods.
    /// - Omit to allow nothing (warning emitted).
    /// - `methods = "*"` or `methods = ["*"]` to allow all methods.
    /// - `methods = ["GET", "POST"]` to allow specific methods.
    /// - `methods = []` to allow nothing (warning emitted).
    pub methods: Option<MethodsInput>,
    /// Optional secrets for this host.
    #[serde(default)]
    pub secrets: Option<AllowedHostSecretsToml>,
}

/// Secrets configuration for an allowed host.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct AllowedHostSecretsToml {
    /// Env vars using the same syntax as top-level `env_vars`.
    pub env_vars: Vec<EnvVarConfig>,
    /// Where in the request to perform replacement.
    /// Default: empty (no replacement — deny by default).
    #[serde(default)]
    pub replace_in: Vec<ReplaceIn>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityStubFileConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
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
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityExternalFileConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    /// Override the auto-computed component digest used for locking.
    /// If set, this value is used instead of the content digest of the WASM file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityExternalComponentConfigToml {
    File(ActivityExternalFileConfigToml),
    Inline(ActivityStubExtInlineConfigToml),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityStubExtInlineConfigCanonical {
    pub(crate) name: ConfigName,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    #[serde(default)]
    pub(crate) params: Option<Vec<JsParamToml>>,
    #[serde(default)]
    pub(crate) return_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityStubComponentConfigCanonical {
    File(ActivityStubFileConfigToml),
    Inline(ActivityStubExtInlineConfigCanonical),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityExternalComponentConfigCanonical {
    File(ActivityExternalFileConfigToml),
    Inline(ActivityStubExtInlineConfigCanonical),
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

impl ActivityStubComponentConfigCanonical {
    fn name_str(&self) -> &str {
        match self {
            Self::File(f) => f.common.name.0.as_ref(),
            Self::Inline(i) => i.name.0.as_ref(),
        }
    }

    #[instrument(skip_all, fields(component_name = self.name_str(), component_id))]
    pub(crate) async fn fetch_and_verify(
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

impl ActivityExternalComponentConfigCanonical {
    fn name_str(&self) -> &str {
        match self {
            Self::File(f) => f.common.name.0.as_ref(),
            Self::Inline(i) => i.name.0.as_ref(),
        }
    }

    #[instrument(skip_all, fields(component_name = self.name_str(), component_id))]
    pub(crate) async fn fetch_and_verify(
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

impl ActivityWasmComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref()))]
    pub(crate) async fn fetch_and_verify(
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
            forward_stdout: self.forward_stdout.into(),
            forward_stderr: self.forward_stderr.into(),
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
            logs_store_min_level: self.logs_store_min_level.into(),
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
    pub(crate) location: JsLocationToml,
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
/// A parameter declaration for a JS activity function.
#[derive(Debug, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct JsParamToml {
    /// Parameter name (used in WIT metadata).
    /// Stored in kebab-case for WIT compatibility.
    pub(crate) name: String,
    /// WIT type string, e.g. `string`, `u32`, `list<string>`, `option<u64>`.
    #[serde(rename = "type")]
    pub(crate) wit_type: String,
}
impl<'de> Deserialize<'de> for JsParamToml {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            name: String,
            #[serde(rename = "type")]
            wit_type: String,
        }
        let raw = Raw::deserialize(deserializer)?;
        let name = if raw.name.contains('_') {
            let kebab = raw.name.replace('_', "-");
            warn!(
                "param name `{}` contains '_', converting to kebab-case: `{kebab}`",
                raw.name
            );
            kebab
        } else {
            raw.name
        };
        Ok(JsParamToml {
            name,
            wit_type: raw.wit_type,
        })
    }
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

/// Program specification for exec activities (TOML input form).
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) enum ExecProgramToml {
    /// Explicit argv. The first element is the executable; remaining elements are fixed arguments.
    /// Activity params are JSON-serialized and appended as trailing args.
    /// Does not support `${DEPLOYMENT_DIR}/` prefix.
    #[serde(rename = "external")]
    External(Vec<String>),
    /// Inline script content. Written to a temp file at each execution.
    /// Activity params are appended as args.
    #[serde(rename = "inline")]
    Inline(String),
    /// File path to include. Resolved to `inline` at canonicalization time.
    /// Supports `${DEPLOYMENT_DIR}/` prefix.
    #[serde(rename = "include")]
    Include(String), // expanded in `expand_deployment_dir_prefix`
}

/// Program specification for exec activities (canonical/wire form).
/// `Include` has been resolved to `Inline`.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) enum ExecProgramCanonical {
    #[serde(rename = "external")]
    External(Vec<String>),
    #[serde(rename = "inline")]
    Inline(String),
}

/// Secret entry: resolved from environment variables at startup.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecretEnvVarToml {
    /// Name used to reference this secret in the `secrets.stdin` function.
    pub(crate) name: String,
    /// Value supporting `${VAR}` interpolation from host environment.
    #[serde(default)]
    pub(crate) value: String,
}

/// Secrets configuration for exec activities.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecSecretsToml {
    /// Secret entries resolved from environment variables.
    #[serde(default)]
    pub(crate) env_vars: Vec<SecretEnvVarToml>,
}

const fn default_max_output_bytes() -> u64 {
    4096
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityExecComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Program specification. Exactly one of `external`, `inline`, or `include`.
    pub(crate) program: ExecProgramToml,
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
    /// Working directory for the child process. Supports `${DEPLOYMENT_DIR}` expansion.
    /// Defaults to the server's working directory.
    #[serde(default)]
    pub(crate) cwd: Option<String>,
    /// Maximum bytes collected from stdout to form the response.
    /// Exceeding the limit fails the execution.
    /// Not used when `return_type` is result (default), since the response carries no data.
    #[serde(default = "default_max_output_bytes")]
    pub(crate) max_output_bytes: u64,
    /// Secrets pushed to stdin. See `ExecSecretsToml`.
    #[serde(default)]
    pub(crate) secrets: Option<ExecSecretsToml>,
}

/// Canonical form of `ActivityExecComponentConfigToml`.
#[derive(schemars::JsonSchema, Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityExecComponentConfigCanonical {
    pub(crate) name: ConfigName,
    pub(crate) program: ExecProgramCanonical,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<JsParamToml>,
    pub(crate) return_type: Option<String>,
    pub(crate) component_digest: Option<ComponentDigest>,
    pub(crate) exec: ExecConfigToml,
    pub(crate) max_retries: u32,
    pub(crate) retry_exp_backoff: DurationConfig,
    pub(crate) forward_stdout: ComponentStdOutputToml,
    pub(crate) forward_stderr: ComponentStdOutputToml,
    pub(crate) logs_store_min_level: LogLevelToml,
    pub(crate) env_vars: Vec<EnvVarConfig>,
    pub(crate) cwd: Option<String>, // Must be resolved.
    pub(crate) max_output_bytes: u64,
    pub(crate) secrets: Option<ExecSecretsToml>,
}

impl ActivityExecComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.name.0.as_ref()))]
    pub(crate) fn fetch_and_verify(
        self,
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
        let program_bytes = match &self.program {
            ExecProgramCanonical::External(argv) => argv.join("\0"),
            ExecProgramCanonical::Inline(script) => script.clone(),
        };
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"activity_exec:");
            hasher.update(program_bytes.as_bytes());
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
            program: self.program,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            env_vars,
            cwd: self.cwd,
            max_output_bytes: self.max_output_bytes,
            forward_stdout: self.forward_stdout.into(),
            forward_stderr: self.forward_stderr.into(),
            secrets: resolved_secrets,
            component_id: component_id.clone(),
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into(),
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
    pub(crate) program: ExecProgramCanonical,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) env_vars: Arc<[EnvVar]>,
    pub(crate) cwd: Option<String>,
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
    pub(crate) location: JsLocationToml,
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

    pub(crate) fn as_frame_sources(&self) -> FrameFilesToSourceContent {
        FrameFilesToSourceContent::from([(self.js_file_name.clone(), self.js_source.clone())])
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy, JsonSchema, PartialEq)]
#[serde(untagged)] // Try variants without needing a specific outer tag
pub(crate) enum BlockingStrategyConfigToml {
    // Try the more specific map format first
    Tagged(BlockingStrategyConfigCustomized),
    // If it's not the map format, try the simple string format
    Simple(BlockingStrategyConfigSimple),
}
impl Default for BlockingStrategyConfigToml {
    fn default() -> Self {
        Self::Simple(BlockingStrategyConfigSimple::default())
    }
}
// Enum to handle the tagged map case ({ kind = "await", ... })
#[derive(Debug, Deserialize, Serialize, Clone, Copy, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")] // Expects a map with "kind" field
pub(crate) enum BlockingStrategyConfigCustomized {
    Await(BlockingStrategyAwaitConfig),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockingStrategyAwaitConfig {
    #[serde(default = "default_non_blocking_event_batching")]
    non_blocking_event_batching: u32,
}
#[derive(Debug, Deserialize, Serialize, Clone, Copy, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BlockingStrategyConfigSimple {
    Interrupt,
    #[default]
    Await,
}
impl From<BlockingStrategyConfigToml> for JoinNextBlockingStrategy {
    fn from(input: BlockingStrategyConfigToml) -> Self {
        match input {
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
/// Canonical JS location — no local file paths.
/// Used for hash computation, wire format in deployment submission, and DB storage.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JsLocationCanonical {
    #[schemars(with = "String")]
    Content { content: String, file_name: String },
    /// OCI-sourced JS.
    #[schemars(with = "String")]
    Oci { image: String },
}

pub(crate) struct JsContent {
    pub(crate) source: String,
    pub(crate) file_name: String,
}

impl JsLocationCanonical {
    /// Return the JS source content and file name.
    /// For `Content`, returns them directly (validating digest if provided).
    /// For `Oci`, pulls from the registry (or cache) under `wasm_cache_dir/js/`.
    pub(crate) async fn get_content(
        &self,
        wasm_cache_dir: &Path,
        expected_digest: Option<&ContentDigest>,
    ) -> anyhow::Result<JsContent> {
        match self {
            JsLocationCanonical::Content { content, file_name } => {
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
            JsLocationCanonical::Oci { image } => {
                let oci_ref = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow::anyhow!("invalid OCI reference in canonical form: {e}"))?;
                let js_cache_dir = wasm_cache_dir.join("js");
                tokio::fs::create_dir_all(&js_cache_dir)
                    .await
                    .with_context(|| {
                        format!("cannot create JS cache directory {js_cache_dir:?}")
                    })?;
                let metadata_dir = wasm_cache_dir.join("metadata");
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

// Canonical component config types
// Used for wire format, hash computation, and DB storage.

#[derive(schemars::JsonSchema, Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct ComponentBacktraceConfigCanonical {
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub(crate) frame_files_to_sources: HashMap<String, String>,
}

impl ComponentBacktraceConfigCanonical {
    pub(crate) fn into_frame_files(self) -> FrameFilesToSourceContent {
        self.frame_files_to_sources
    }
}

/// Canonical form of `ActivityJsComponentConfigToml`.
#[derive(schemars::JsonSchema, Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityJsComponentConfigCanonical {
    pub(crate) name: ConfigName,
    pub(crate) location: JsLocationCanonical,
    pub(crate) content_digest: Option<ContentDigest>,
    pub(crate) component_digest: Option<ComponentDigest>,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<JsParamToml>,
    pub(crate) exec: ExecConfigToml,
    pub(crate) max_retries: u32,
    pub(crate) retry_exp_backoff: DurationConfig,
    pub(crate) forward_stdout: ComponentStdOutputToml,
    pub(crate) forward_stderr: ComponentStdOutputToml,
    pub(crate) logs_store_min_level: LogLevelToml,
    pub(crate) env_vars: Vec<EnvVarConfig>,
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
    pub(crate) return_type: Option<String>,
}

impl ActivityJsComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.name.0.as_ref()))]
    pub(crate) async fn fetch_and_verify(
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
            forward_stdout: self.forward_stdout.into(),
            forward_stderr: self.forward_stderr.into(),
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
            logs_store_min_level: self.logs_store_min_level.into(),
        })
    }
}

/// Canonical form of `WorkflowWasmComponentConfigToml`.
#[derive(schemars::JsonSchema, Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowWasmComponentConfigCanonical {
    pub(crate) common: ComponentCommon,
    pub(crate) component_digest: Option<ComponentDigest>,
    pub(crate) exec: ExecConfigToml,
    pub(crate) retry_exp_backoff: DurationConfig,
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    pub(crate) backtrace: ComponentBacktraceConfigCanonical,
    pub(crate) stub_wasi: bool,
    pub(crate) lock_extension: bool,
    pub(crate) logs_store_min_level: LogLevelToml,
}

impl WorkflowWasmComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref()))]
    pub(crate) async fn fetch_and_verify(
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
                self.common.name.0
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
            join_next_blocking_strategy: self.blocking_strategy.into(),
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
            logs_store_min_level: self.logs_store_min_level.into(),
        })
    }
}

/// Canonical form of `WorkflowJsComponentConfigToml`.
#[derive(schemars::JsonSchema, Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowJsComponentConfigCanonical {
    pub(crate) name: ConfigName,
    pub(crate) location: JsLocationCanonical,
    pub(crate) content_digest: Option<ContentDigest>,
    pub(crate) component_digest: Option<ComponentDigest>,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<JsParamToml>,
    pub(crate) exec: ExecConfigToml,
    pub(crate) retry_exp_backoff: DurationConfig,
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    pub(crate) logs_store_min_level: LogLevelToml,
    pub(crate) return_type: Option<String>,
    pub(crate) lock_extension: bool,
}

impl WorkflowJsComponentConfigCanonical {
    #[instrument(skip_all, fields(component_name = self.name.0.as_ref()))]
    pub(crate) async fn fetch_and_verify(
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
            join_next_blocking_strategy: self.blocking_strategy.into(),
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
            logs_store_min_level: self.logs_store_min_level.into(),
        })
    }
}

/// Canonical deployment configuration — no local file paths.
/// Used for hash computation, wire format, and DB storage.
#[derive(Debug, Deserialize, Serialize, Default, Clone, schemars::JsonSchema)]
pub(crate) struct DeploymentCanonical {
    pub(crate) activities_wasm: Vec<ActivityWasmComponentConfigToml>,
    pub(crate) activities_stub: Vec<ActivityStubComponentConfigCanonical>,
    pub(crate) activities_external: Vec<ActivityExternalComponentConfigCanonical>,
    pub(crate) activities_js: Vec<ActivityJsComponentConfigCanonical>,
    pub(crate) activities_exec: Vec<ActivityExecComponentConfigCanonical>,
    pub(crate) workflows: Vec<WorkflowWasmComponentConfigCanonical>,
    pub(crate) workflows_js: Vec<WorkflowJsComponentConfigCanonical>,
    pub(crate) webhooks: Vec<webhook::WebhookWasmComponentConfigCanonical>,
    pub(crate) webhooks_js: Vec<webhook::WebhookJsComponentConfigCanonical>,
    #[serde(default)]
    pub(crate) crons: Vec<CronComponentConfigToml>,
}

/// Resolve a `DeploymentToml` to `DeploymentCanonical` by reading all local JS and backtrace
/// source files.
pub(crate) async fn resolve_local_refs_to_canonical(
    deployment: &DeploymentTomlValidated,
) -> anyhow::Result<DeploymentCanonical> {
    let mut activities_js = Vec::with_capacity(deployment.activities_js.len());
    for (a, name) in &deployment.activities_js {
        activities_js.push(ActivityJsComponentConfigCanonical {
            name: name.clone(),
            location: resolve_js_to_canonical(&a.location).await?,
            content_digest: a.content_digest.clone(),
            component_digest: a.component_digest.clone(),
            ffqn: a.ffqn.clone(),
            params: a.params.clone(),
            exec: a.exec.clone(),
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars.clone(),
            allowed_hosts: a.allowed_hosts.clone(),
            return_type: a.return_type.clone(),
        });
    }

    let deployment_inner = &deployment.inner;
    let mut workflows = Vec::with_capacity(deployment_inner.workflows.len());
    for w in &deployment_inner.workflows {
        workflows.push(WorkflowWasmComponentConfigCanonical {
            common: w.common.clone(),
            component_digest: w.component_digest.clone(),
            exec: w.exec.clone(),
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            backtrace: resolve_backtrace_to_canonical(&w.backtrace).await,
            stub_wasi: w.stub_wasi,
            lock_extension: w.lock_extension,
            logs_store_min_level: w.logs_store_min_level,
        });
    }

    let mut workflows_js = Vec::with_capacity(deployment.workflows_js.len());
    for (w, name) in &deployment.workflows_js {
        workflows_js.push(WorkflowJsComponentConfigCanonical {
            name: name.clone(),
            location: resolve_js_to_canonical(&w.location).await?,
            content_digest: w.content_digest.clone(),
            component_digest: w.component_digest.clone(),
            ffqn: w.ffqn.clone(),
            params: w.params.clone(),
            exec: w.exec.clone(),
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            lock_extension: w.lock_extension,
            logs_store_min_level: w.logs_store_min_level,
            return_type: w.return_type.clone(),
        });
    }

    let mut webhooks = Vec::with_capacity(deployment_inner.webhooks.len());
    for w in &deployment_inner.webhooks {
        webhooks.push(webhook::WebhookWasmComponentConfigCanonical {
            common: w.common.clone(),
            http_server: w.http_server.clone(),
            routes: w.routes.clone(),
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            env_vars: w.env_vars.clone(),
            backtrace: resolve_backtrace_to_canonical(&w.backtrace).await,
            logs_store_min_level: w.logs_store_min_level,
            allowed_hosts: w.allowed_hosts.clone(),
        });
    }

    let mut webhooks_js = Vec::with_capacity(deployment_inner.webhooks_js.len());
    for w in &deployment_inner.webhooks_js {
        webhooks_js.push(webhook::WebhookJsComponentConfigCanonical {
            name: w.name.clone(),
            location: resolve_js_to_canonical(&w.location).await?,
            content_digest: w.content_digest.clone(),
            http_server: w.http_server.clone(),
            routes: w.routes.clone(),
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            logs_store_min_level: w.logs_store_min_level,
            env_vars: w.env_vars.clone(),
            allowed_hosts: w.allowed_hosts.clone(),
        });
    }

    let mut activities_exec = Vec::with_capacity(deployment.activities_exec.len());
    for (a, name) in &deployment.activities_exec {
        let program = match &a.program {
            ExecProgramToml::External(argv) => ExecProgramCanonical::External(argv.clone()),
            ExecProgramToml::Inline(script) => ExecProgramCanonical::Inline(script.clone()),
            ExecProgramToml::Include(path) => {
                let full_path = PathBuf::from(path);
                if !full_path.exists() {
                    bail!("include file does not exist: {full_path:?}");
                }
                let content = tokio::fs::read_to_string(&full_path)
                    .await
                    .with_context(|| format!("cannot read include file {full_path:?}"))?;
                ExecProgramCanonical::Inline(content)
            }
        };
        activities_exec.push(ActivityExecComponentConfigCanonical {
            name: name.clone(),
            program,
            ffqn: a.ffqn.clone(),
            params: a.params.clone(),
            return_type: a.return_type.clone(),
            component_digest: a.component_digest.clone(),
            exec: a.exec.clone(),
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars.clone(),
            cwd: a.cwd.clone(),
            max_output_bytes: a.max_output_bytes,
            secrets: a.secrets.clone(),
        });
    }

    // Build canonical stubs/externals with resolved names filled in.
    let activities_stub = deployment
        .activities_stub
        .iter()
        .map(|(c, name)| match c {
            ActivityStubComponentConfigToml::File(f) => {
                ActivityStubComponentConfigCanonical::File(f.clone())
            }
            ActivityStubComponentConfigToml::Inline(i) => {
                ActivityStubComponentConfigCanonical::Inline(ActivityStubExtInlineConfigCanonical {
                    name: name.clone(),
                    ffqn: i.ffqn.clone(),
                    params: i.params.clone(),
                    return_type: i.return_type.clone(),
                })
            }
        })
        .collect();
    let activities_external = deployment
        .activities_external
        .iter()
        .map(|(c, name)| match c {
            ActivityExternalComponentConfigToml::File(f) => {
                ActivityExternalComponentConfigCanonical::File(f.clone())
            }
            ActivityExternalComponentConfigToml::Inline(i) => {
                ActivityExternalComponentConfigCanonical::Inline(
                    ActivityStubExtInlineConfigCanonical {
                        name: name.clone(),
                        ffqn: i.ffqn.clone(),
                        params: i.params.clone(),
                        return_type: i.return_type.clone(),
                    },
                )
            }
        })
        .collect();

    Ok(DeploymentCanonical {
        activities_wasm: deployment_inner.activities_wasm.clone(),
        activities_stub,
        activities_external,
        activities_js,
        activities_exec,
        workflows,
        workflows_js,
        webhooks,
        webhooks_js,
        crons: deployment_inner.crons.clone(),
    })
}

async fn resolve_js_to_canonical(location: &JsLocationToml) -> anyhow::Result<JsLocationCanonical> {
    match location {
        JsLocationToml::Path(path) => {
            let file_name = std::path::Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(path)
                .to_string();
            let full_path = PathBuf::from(path);
            if !full_path.exists() {
                bail!("file does not exist: {full_path:?}");
            }
            let content = tokio::fs::read_to_string(&full_path)
                .await
                .with_context(|| format!("cannot read JS file {full_path:?}"))?;
            Ok(JsLocationCanonical::Content { content, file_name })
        }
        JsLocationToml::Oci(reference) => Ok(JsLocationCanonical::Oci {
            image: reference.whole(),
        }),
    }
}

async fn resolve_backtrace_to_canonical(
    backtrace: &ComponentBacktraceConfig,
) -> ComponentBacktraceConfigCanonical {
    let mut frame_files_to_sources = HashMap::new();
    for (key, location) in &backtrace.frame_files_to_sources {
        let BacktraceSourceLocation::Path(path) = location;
        let content = async {
            let full_path = PathBuf::from(path);
            if !full_path.exists() {
                warn!("Ignoring missing backtrace source - file does not exist: {full_path:?}");
                return None;
            }
            tokio::fs::read_to_string(&full_path)
                .await
                .inspect_err(|err| warn!("Cannot read backtrace source {full_path:?} - {err:?}"))
                .ok()
        }
        .await;
        if let Some(content) = content {
            frame_files_to_sources.insert(key.clone(), content);
        }
    }
    ComponentBacktraceConfigCanonical {
        frame_files_to_sources,
    }
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurationConfig {
    Milliseconds(u64),
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
}
impl From<DurationConfig> for Duration {
    fn from(value: DurationConfig) -> Self {
        match value {
            DurationConfig::Milliseconds(millis) => Duration::from_millis(millis),
            DurationConfig::Seconds(secs) => Duration::from_secs(secs),
            DurationConfig::Minutes(mins) => Duration::from_secs(mins * 60),
            DurationConfig::Hours(hrs) => Duration::from_secs(hrs * 60 * 60),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurationConfigOptional {
    None,
    Milliseconds(u64),
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
}
impl From<DurationConfigOptional> for Option<Duration> {
    fn from(value: DurationConfigOptional) -> Self {
        match value {
            DurationConfigOptional::None => None,
            DurationConfigOptional::Milliseconds(millis) => Some(Duration::from_millis(millis)),
            DurationConfigOptional::Seconds(secs) => Some(Duration::from_secs(secs)),
            DurationConfigOptional::Minutes(mins) => Some(Duration::from_secs(mins * 60)),
            DurationConfigOptional::Hours(hrs) => Some(Duration::from_secs(hrs * 60 * 60)),
        }
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentStdOutputToml {
    None,
    Stdout,
    Stderr,
    #[default]
    Db,
}
impl From<ComponentStdOutputToml> for Option<StdOutputConfig> {
    fn from(value: ComponentStdOutputToml) -> Self {
        match value {
            ComponentStdOutputToml::None => None,
            ComponentStdOutputToml::Stdout => Some(StdOutputConfig::Stdout),
            ComponentStdOutputToml::Stderr => Some(StdOutputConfig::Stderr),
            ComponentStdOutputToml::Db => Some(StdOutputConfig::Db),
        }
    }
}

pub(crate) mod webhook {
    use super::{
        AllowedHostToml, ComponentBacktraceConfig, ComponentBacktraceConfigCanonical,
        ComponentCommon, ComponentStdOutputToml, ConfigName, JsContent, JsLocationCanonical,
        JsLocationToml, resolve_allowed_hosts, resolve_env_vars_plaintext,
        validate_no_env_collision,
    };
    use crate::{
        command::server::FrameFilesToSourceContent,
        config::{env_var::EnvVarConfig, toml::LogLevelToml},
    };
    use anyhow::Context;
    use concepts::{
        ComponentId, ComponentType, ContentDigest, StrVariant,
        component_id::{ComponentDigest, Digest},
        storage::LogLevel,
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

    fn default_external_server_name() -> ConfigName {
        ConfigName::new(StrVariant::Static("external")).expect("valid name")
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

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(untagged)]
    pub(crate) enum WebhookRoute {
        String(String),
        WebhookRouteDetail(WebhookRouteDetail),
    }

    impl Default for WebhookRoute {
        fn default() -> Self {
            WebhookRoute::String(String::new())
        }
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookRouteDetail {
        // Empty means all methods.
        #[serde(default)]
        pub(crate) methods: Vec<String>,
        pub(crate) route: String,
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
        /// Supports local file paths.
        pub(crate) location: JsLocationToml,
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

    /// Canonical form of `WebhookWasmComponentConfigToml`.
    #[derive(Debug, Deserialize, Serialize, Clone, schemars::JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookWasmComponentConfigCanonical {
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
        pub(crate) backtrace: ComponentBacktraceConfigCanonical,
        #[serde(default)]
        pub(crate) logs_store_min_level: LogLevelToml,
        #[serde(default, rename = "allowed_host")]
        pub(crate) allowed_hosts: Vec<AllowedHostToml>,
    }

    impl WebhookWasmComponentConfigCanonical {
        #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref()), err)]
        pub(crate) async fn fetch_and_verify(
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
                    forward_stdout: self.forward_stdout.into(),
                    forward_stderr: self.forward_stderr.into(),
                    env_vars,
                    frame_files_to_sources,
                    subscription_interruption,
                    logs_store_min_level: self.logs_store_min_level.into(),
                    allowed_hosts,
                    config_section_hint: ConfigSectionHint::WebhookEndpointWasm,
                },
            ))
        }
    }

    /// Canonical form of `WebhookJsComponentConfigToml`.
    #[derive(Debug, Deserialize, Serialize, Clone, schemars::JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookJsComponentConfigCanonical {
        pub(crate) name: ConfigName,
        pub(crate) location: JsLocationCanonical,
        #[serde(default)]
        pub(crate) content_digest: Option<ContentDigest>,
        #[serde(default = "default_external_server_name")]
        pub(crate) http_server: ConfigName,
        pub(crate) routes: Vec<WebhookRoute>,
        #[serde(default)]
        pub(crate) forward_stdout: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) forward_stderr: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) logs_store_min_level: LogLevelToml,
        #[serde(default)]
        pub(crate) env_vars: Vec<EnvVarConfig>,
        #[serde(default, rename = "allowed_host")]
        pub(crate) allowed_hosts: Vec<AllowedHostToml>,
    }

    impl WebhookJsComponentConfigCanonical {
        #[instrument(skip_all, fields(component_name = self.name.0.as_ref()))]
        pub(crate) async fn fetch_and_verify(
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
                    forward_stdout: self.forward_stdout.into(),
                    forward_stderr: self.forward_stderr.into(),
                    env_vars,
                    logs_store_min_level: self.logs_store_min_level.into(),
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

// TODO: Unify with ValueOrUnlimited
#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(untagged)]
pub(crate) enum InflightSemaphore {
    Unlimited(Unlimited),
    Some(u32),
}
impl Default for InflightSemaphore {
    fn default() -> Self {
        Self::Unlimited(Unlimited::Unlimited)
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Unlimited {
    #[default]
    Unlimited,
}

impl InflightSemaphore {
    pub(crate) fn as_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        match self {
            InflightSemaphore::Unlimited(_) => None,
            InflightSemaphore::Some(permits) => Some(Arc::new(tokio::sync::Semaphore::new(
                usize::try_from(*permits).expect("usize >= u32"),
            ))),
        }
    }
}

// TODO: Move to env_var module
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

const fn default_max_retries() -> u32 {
    5
}

const fn default_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Milliseconds(100)
}

const fn default_non_blocking_event_batching() -> u32 {
    DEFAULT_NON_BLOCKING_EVENT_BATCHING
}

const fn default_batch_size() -> u32 {
    5
}

const fn default_lock_expiry() -> DurationConfig {
    DurationConfig::Seconds(1)
}

const fn default_tick_sleep() -> DurationConfig {
    DurationConfig::Milliseconds(200)
}

const fn default_lock_extension() -> bool {
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

fn default_cancel_watcher_enabled() -> bool {
    true
}
fn default_cancel_watcher_tick_sleep() -> DurationConfig {
    DurationConfig::Seconds(1)
}

pub(crate) mod cron {
    use super::*;

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct CronComponentConfigToml {
        pub(crate) name: ConfigName,
        /// Fully qualified function name of the target to invoke on each schedule tick.
        #[schemars(with = "String")]
        pub(crate) ffqn: FunctionFqn,
        /// JSON-encoded parameters to pass to the target function.
        #[serde(default = "default_schedule_params")]
        pub(crate) params: String,
        /// Cron expression or `@once`, `@daily`, `@hourly`, `@weekly`, `@monthly`, `@yearly`.
        pub(crate) schedule: String,
        #[serde(default)]
        pub(crate) exec: ExecConfigToml,
    }

    fn default_schedule_params() -> String {
        "[]".to_string()
    }

    #[derive(Debug)]
    pub(crate) struct CronConfigVerified {
        pub(crate) component_id: ComponentId,
        pub(crate) target_ffqn: FunctionFqn,
        pub(crate) params_json: Vec<serde_json::Value>,
        pub(crate) cron_schedule: CronOrOnce,
        pub(crate) exec_config: executor::executor::ExecConfig,
    }

    impl CronComponentConfigToml {
        pub(crate) fn verify(self) -> Result<CronConfigVerified, anyhow::Error> {
            let name = self.name.0.to_string();
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

#[cfg(test)]
mod tests {
    mod blocking_strategy {
        use super::super::*;
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
                JoinNextBlockingStrategy::from(actual.strategy),
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
                JoinNextBlockingStrategy::from(actual.strategy),
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
                JoinNextBlockingStrategy::from(actual.strategy),
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
                JoinNextBlockingStrategy::from(actual.strategy),
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
        use super::super::*;

        fn exec_config_with_secret(value: &str) -> ActivityExecComponentConfigCanonical {
            ActivityExecComponentConfigCanonical {
                name: ConfigName::new(StrVariant::from("exec-test")).unwrap(),
                program: ExecProgramCanonical::Inline("#!/usr/bin/env bash\necho null\n".into()),
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
                cwd: None,
                max_output_bytes: default_max_output_bytes(),
                secrets: Some(ExecSecretsToml {
                    env_vars: vec![SecretEnvVarToml {
                        name: "MY_SECRET".into(),
                        value: value.into(),
                    }],
                }),
            }
        }

        #[test]
        fn fetch_and_verify_activity_exec_secret_fails_when_missing_and_not_ignored() {
            let config = exec_config_with_secret("${MISSING_EXEC_SECRET}");
            let error = config
                .fetch_and_verify(false, None)
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
            let verified = config.fetch_and_verify(true, None).unwrap();
            assert!(verified.secrets.is_none());
        }
    }
}
