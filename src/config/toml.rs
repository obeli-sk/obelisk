use super::{
    ComponentLocation, ConfigStoreCommon, config_holder::PathPrefixes, env_var::EnvVarConfig,
};
use crate::config::config_holder::{CACHE_DIR_PREFIX, DATA_DIR_PREFIX};
use anyhow::{anyhow, bail};
use concepts::{
    ComponentId, ComponentRetryConfig, ComponentType, InvalidNameError, StrVariant, check_name,
    component_id::InputContentDigest, prefixed_ulid::ExecutorId,
};
use db_sqlite::sqlite_dao::SqliteConfig;
use log::{LoggingConfig, LoggingStyle};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tracing::{instrument, warn};
use utils::wasm_tools::WasmComponent;
use wasm_workers::{
    activity::activity_worker::{ActivityConfig, ActivityDirectoriesConfig, ProcessProvider},
    envvar::EnvVar,
    workflow::workflow_worker::{
        DEFAULT_NON_BLOCKING_EVENT_BATCHING, JoinNextBlockingStrategy, WorkflowConfig,
    },
};
use webhook::{HttpServer, WebhookComponentConfigToml};

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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigToml {
    pub(crate) api: ApiConfig,
    #[serde(default)]
    pub(crate) database: DatabaseConfigToml,
    #[serde(default)]
    pub(crate) webui: WebUIConfig,
    #[serde(default, rename = "wasm")]
    pub(crate) wasm_global_config: WasmGlobalConfigToml,
    #[serde(default, rename = "activities")]
    pub(crate) activities_global_config: ActivitiesGlobalConfigToml,
    #[serde(default, rename = "workflows")]
    pub(crate) workflows_global_config: WorkflowsGlobalConfigToml,
    #[serde(default)]
    pub(crate) timers_watcher: TimersWatcherTomlConfig,
    #[serde(default)]
    pub(crate) cancel_watcher: CancelWatcherTomlConfig,
    #[serde(default, rename = "activity_wasm")]
    pub(crate) activities_wasm: Vec<ActivityWasmComponentConfigToml>,
    #[serde(default, rename = "activity_stub")]
    pub(crate) activities_stub: Vec<ActivityStubExtComponentConfigToml>,
    #[serde(default, rename = "activity_external")]
    pub(crate) activities_external: Vec<ActivityStubExtComponentConfigToml>,
    #[serde(default, rename = "workflow")]
    pub(crate) workflows: Vec<WorkflowComponentConfigToml>,
    #[cfg(feature = "otlp")]
    #[serde(default)]
    pub(crate) otlp: Option<otlp::OtlpConfig>,
    #[serde(default)]
    pub(crate) log: LoggingConfig,
    #[serde(default, rename = "http_server")]
    pub(crate) http_servers: Vec<HttpServer>,
    #[serde(default, rename = "webhook_endpoint")]
    pub(crate) webhooks: Vec<WebhookComponentConfigToml>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiConfig {
    pub(crate) listening_addr: SocketAddr,
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
    pub host: String,
    pub user: String,
    pub password: String,
    pub db_name: String,
    /// Interrupts listening for notifications periodically, needed for Postgres with a local-only subscription mechanism.
    #[serde(default = "default_subscription_interruption")]
    pub subscription_interruption: DurationConfigOptional,
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
        path_prefixes.replace_path_prefix_mkdir(sqlite_file).await
    }

    pub(crate) fn as_sqlite_config(&self) -> SqliteConfig {
        SqliteConfig {
            queue_capacity: self.queue_capacity,
            pragma_override: Some(self.pragma.clone().into_iter().collect()),
            metrics_threshold: self.metrics_threshold.map(Duration::from),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebUIConfig {
    pub(crate) listening_addr: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
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
            .replace_path_prefix_mkdir(wasm_directory)
            .await
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivitiesGlobalConfigToml {
    directories: ActivitiesDirectoriesGlobalConfigToml,
}
impl ActivitiesGlobalConfigToml {
    pub(crate) fn get_directories(&self) -> Option<&ActivitiesDirectoriesGlobalConfigToml> {
        if self.directories.enabled {
            Some(&self.directories)
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivitiesDirectoriesGlobalConfigToml {
    #[serde(default = "default_activities_directories_enabled")]
    enabled: bool,
    #[serde(default = "default_activities_directories_parent_directory")]
    parent_directory: String,
    #[serde(default)]
    cleanup: ActivitiesDirectoriesCleanupConfigToml,
}
impl Default for ActivitiesDirectoriesGlobalConfigToml {
    fn default() -> Self {
        Self {
            enabled: default_activities_directories_enabled(),
            parent_directory: default_activities_directories_parent_directory(),
            cleanup: ActivitiesDirectoriesCleanupConfigToml::default(),
        }
    }
}
impl ActivitiesDirectoriesGlobalConfigToml {
    pub(crate) async fn get_parent_directory(
        &self,
        path_prefixes: &PathPrefixes,
    ) -> Result<Arc<Path>, anyhow::Error> {
        assert!(self.enabled); // see `ActivitiesGlobalConfigToml::get_directories`
        path_prefixes
            .replace_path_prefix_mkdir(&self.parent_directory)
            .await
            .map(Arc::from)
    }

    pub(crate) fn get_cleanup(&self) -> Option<ActivitiesDirectoriesCleanupConfigToml> {
        assert!(self.enabled); // see `ActivitiesGlobalConfigToml::get_directories`
        if self.cleanup.enabled {
            Some(self.cleanup)
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivitiesDirectoriesCleanupConfigToml {
    #[serde(default = "default_dir_cleanup_enabled")]
    pub(crate) enabled: bool,
    #[serde(default = "default_dir_cleanup_run_every")]
    pub(crate) run_every: DurationConfig,
    #[serde(default = "default_dir_cleanup_older_than")]
    pub(crate) older_than: DurationConfig,
}
impl Default for ActivitiesDirectoriesCleanupConfigToml {
    fn default() -> Self {
        Self {
            enabled: default_dir_cleanup_enabled(),
            run_every: default_dir_cleanup_run_every(),
            older_than: default_dir_cleanup_older_than(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
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
                .replace_path_prefix_mkdir(directory)
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

/// Activity, Webhook, Workflow or a Http server
#[derive(
    Debug, Clone, Hash, PartialEq, Eq, derive_more::Display, derive_more::Into, JsonSchema,
)]
#[display("{_0}")]
pub struct ConfigName(#[schemars(with = "String")] StrVariant);
impl ConfigName {
    pub fn new(name: StrVariant) -> Result<Self, InvalidNameError<ConfigName>> {
        Ok(Self(check_name(name, "_")?))
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentCommon {
    pub(crate) name: ConfigName,
    pub(crate) location: ComponentLocation,
}

impl ComponentCommon {
    /// Fetch wasm file, calculate its content digest, optionally compare with the expected `content_digest`.
    ///
    /// Read wasm file either from local fs or pull from an OCI registry and cache it if needed.
    /// If the `content_digest` is set, verify that it matches the calculated digest.
    /// Otherwise backfill the `content_digest`.
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
        path_prefixes: &PathPrefixes,
    ) -> Result<(ConfigStoreCommon, PathBuf), anyhow::Error> {
        let (actual_content_digest, wasm_path) = self
            .location
            .obtain_wasm(wasm_cache_dir, metadata_dir, path_prefixes)
            .await?;
        let verified = ConfigStoreCommon {
            name: self.name,
            location: self.location,
            content_digest: actual_content_digest,
        };
        Ok((verified, wasm_path))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LockingStrategy {
    ByFfqns,
    ByComponentId,
}
impl From<LockingStrategy> for executor::executor::LockingStrategy {
    fn from(value: LockingStrategy) -> Self {
        match value {
            LockingStrategy::ByFfqns => executor::executor::LockingStrategy::ByFfqns,
            LockingStrategy::ByComponentId => executor::executor::LockingStrategy::ByComponentId,
        }
    }
}
impl From<executor::executor::LockingStrategy> for LockingStrategy {
    fn from(value: executor::executor::LockingStrategy) -> Self {
        match value {
            executor::executor::LockingStrategy::ByFfqns => LockingStrategy::ByFfqns,
            executor::executor::LockingStrategy::ByComponentId => LockingStrategy::ByComponentId,
        }
    }
}
impl Default for LockingStrategy {
    fn default() -> Self {
        executor::executor::LockingStrategy::default().into()
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecConfigToml {
    #[serde(default = "default_batch_size")]
    batch_size: u32,
    #[serde(default = "default_lock_expiry")]
    lock_expiry: DurationConfig,
    #[serde(default = "default_tick_sleep")]
    tick_sleep: DurationConfig,
    #[serde(default)]
    locking_strategy: LockingStrategy,
}

impl Default for ExecConfigToml {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            lock_expiry: default_lock_expiry(),
            tick_sleep: default_tick_sleep(),
            locking_strategy: LockingStrategy::default(),
        }
    }
}

impl ExecConfigToml {
    pub(crate) fn into_exec_exec_config(
        self,
        component_id: ComponentId,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        retry_config: ComponentRetryConfig,
    ) -> executor::executor::ExecConfig {
        executor::executor::ExecConfig {
            lock_expiry: self.lock_expiry.into(),
            tick_sleep: self.tick_sleep.into(),
            batch_size: self.batch_size,
            component_id,
            task_limiter: global_executor_instance_limiter,
            executor_id: ExecutorId::generate(),
            retry_config,
            locking_strategy: self.locking_strategy.into(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityWasmComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) forward_stdout: StdOutput,
    #[serde(default)]
    pub(crate) forward_stderr: StdOutput,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    #[serde(default = "default_retry_on_err")]
    pub(crate) retry_on_err: bool,
    #[serde(default)]
    pub(crate) directories: ActivityDirectoriesConfigToml,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityStubExtComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
}
#[derive(Debug)]
pub(crate) struct ActivityStubExtConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) component_id: ComponentId,
}
impl ActivityStubExtComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref(), component_id))]
    pub(crate) async fn fetch_and_verify(
        self,
        component_type: ComponentType,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        path_prefixes: Arc<PathPrefixes>,
    ) -> Result<ActivityStubExtConfigVerified, anyhow::Error> {
        let (common, wasm_path) = self
            .common
            .fetch_and_verify(&wasm_cache_dir, &metadata_dir, &path_prefixes)
            .await?;
        let component_id = ComponentId::new(
            component_type,
            StrVariant::from(common.name),
            InputContentDigest(common.content_digest),
        )?;

        Ok(ActivityStubExtConfigVerified {
            wasm_path,
            component_id,
        })
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityDirectoriesConfigToml {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    reuse_on_retry: bool,
    #[serde(default)]
    process_provider: ActivityDirectoriesProcessProvider,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActivityDirectoriesProcessProvider {
    #[default]
    None,
    Native,
}
impl From<ActivityDirectoriesProcessProvider> for Option<ProcessProvider> {
    fn from(value: ActivityDirectoriesProcessProvider) -> Self {
        match value {
            ActivityDirectoriesProcessProvider::None => None,
            ActivityDirectoriesProcessProvider::Native => Some(ProcessProvider::Native),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ActivityWasmConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
}

impl ActivityWasmConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }
}

impl ActivityWasmComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref()))]
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        path_prefixes: Arc<PathPrefixes>,
        ignore_missing_env_vars: bool,
        parent_preopen_dir: Option<Arc<Path>>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityWasmConfigVerified, anyhow::Error> {
        let (common, wasm_path) = self
            .common
            .fetch_and_verify(&wasm_cache_dir, &metadata_dir, &path_prefixes)
            .await?;

        let env_vars = resolve_env_vars(self.env_vars, ignore_missing_env_vars)?;
        let directories_config = match (parent_preopen_dir, self.directories.enabled) {
            (Some(parent_preopen_dir), true) => Some(ActivityDirectoriesConfig {
                parent_preopen_dir,
                reuse_on_retry: self.directories.reuse_on_retry,
                process_provider: self.directories.process_provider.into(),
            }),
            (None, true) => {
                bail!(
                    "`directories.enabled` set to true for activity `{}` while the global setting `activities.directories.enabled` is false",
                    common.name.0
                );
            }
            (_, false) => None,
        };

        let component_id = ComponentId::new(
            ComponentType::ActivityWasm,
            StrVariant::from(common.name),
            InputContentDigest(common.content_digest),
        )?;
        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into(),
            forward_stderr: self.forward_stderr.into(),
            env_vars,
            retry_on_err: self.retry_on_err,
            directories_config,
            fuel,
        };
        let retry_config = ComponentRetryConfig {
            max_retries: self.max_retries,
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityWasmConfigVerified {
            wasm_path,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            ),
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    #[serde(default = "default_convert_core_module")]
    pub(crate) convert_core_module: bool,
    #[serde(default)]
    pub(crate) backtrace: WorkflowComponentBacktraceConfig,
    #[serde(default)]
    pub(crate) stub_wasi: bool,
    #[serde(default = "default_lock_extension")]
    lock_extension: DurationConfig,
}

#[derive(Debug, Deserialize, Clone, Copy, JsonSchema, PartialEq)]
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
#[derive(Debug, Deserialize, Clone, Copy, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")] // Expects a map with "kind" field
pub(crate) enum BlockingStrategyConfigCustomized {
    Await(BlockingStrategyAwaitConfig),
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockingStrategyAwaitConfig {
    #[serde(default = "default_non_blocking_event_batching")]
    non_blocking_event_batching: u32,
}
#[derive(Debug, Deserialize, Clone, Copy, JsonSchema, Default, PartialEq)]
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

type BacktraceFrameFilesToSourcesUnverified = hashbrown::HashMap<String, String>;
type BacktraceFrameFilesToSourcesVerified = hashbrown::HashMap<String, PathBuf>;

#[derive(Debug, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowComponentBacktraceConfig {
    #[serde(rename = "sources")]
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub(crate) frame_files_to_sources: BacktraceFrameFilesToSourcesUnverified,
}

#[derive(Debug)]
pub(crate) struct WorkflowConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) frame_files_to_sources: BacktraceFrameFilesToSourcesVerified,
}

fn verify_frame_files_to_sources(
    frame_files_to_sources: BacktraceFrameFilesToSourcesUnverified,
    path_prefixes: &PathPrefixes,
) -> BacktraceFrameFilesToSourcesVerified {
    frame_files_to_sources
        .into_iter()
        .filter_map(|(key, value)| {
            // Remove all entries where destination file is not found.
            match path_prefixes
                .replace_file_prefix_verify_exists(&value)
                .map(|value| (path_prefixes.replace_file_prefix_no_verify(&key), value)) // the key points to source path found in WASM
            {
                Ok((k, v)) => Some((k, v)),
                Err(err) => {
                    warn!("Ignoring missing backtrace source - {err:?}");
                    None
                }
            }
        })
        .collect()
}

impl WorkflowConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.workflow_config.component_id
    }
}

impl WorkflowComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref()))]
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        path_prefixes: Arc<PathPrefixes>,
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
        let (common, wasm_path) = self
            .common
            .fetch_and_verify(&wasm_cache_dir, &metadata_dir, &path_prefixes)
            .await?;
        let (wasm_path, _transformed_digest) = if self.convert_core_module {
            // TODO: Avoid this by maintainig a mapping file if necessary.
            WasmComponent::convert_core_module_to_component(&wasm_path, &wasm_cache_dir)
                .await?
                .unwrap_or((wasm_path, common.content_digest.clone()))
        } else {
            (wasm_path, common.content_digest.clone())
        };
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(common.name),
            InputContentDigest(common.content_digest),
        )?;

        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            join_next_blocking_strategy: self.blocking_strategy.into(),
            backtrace_persist: global_backtrace_persist,
            stub_wasi: self.stub_wasi,
            fuel,
            lock_extension: self.lock_extension.into(),
            subscription_interruption,
        };
        let frame_files_to_sources =
            verify_frame_files_to_sources(self.backtrace.frame_files_to_sources, &path_prefixes);
        let retry_config = ComponentRetryConfig {
            max_retries: u32::MAX,
            retry_exp_backoff,
        };
        Ok(WorkflowConfigVerified {
            wasm_path,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            ),
            frame_files_to_sources,
        })
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

    #[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
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
    use super::{Deserialize, JsonSchema, default_out_style};
    use serde_with::serde_as;
    use std::str::FromStr;

    #[derive(Debug, Deserialize, JsonSchema, Default)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct LoggingConfig {
        #[serde(default)]
        pub(crate) file: Option<AppenderRollingFile>,
        #[serde(default)]
        pub(crate) stdout: Option<AppenderOut>,
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

    #[derive(Debug, Deserialize, JsonSchema, Default)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LoggingStyle {
        #[default]
        Plain,
        PlainCompact,
        Json,
    }

    #[serde_as]
    #[derive(Debug, Deserialize, JsonSchema, Default)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderCommon {
        #[serde(default)]
        pub(crate) level: EnvFilter,
        #[serde(default)]
        pub(crate) span: SpanConfig,
        #[serde(default)]
        pub(crate) target: bool,
    }

    #[derive(Debug, serde_with::DeserializeFromStr, JsonSchema)]
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
            Self::from_str("info,app=trace").expect("empty directive must not fail to parse")
        }
    }

    #[derive(Debug, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderOut {
        pub(crate) enabled: bool,
        #[serde(flatten, default)]
        pub(crate) common: AppenderCommon,
        #[serde(default = "default_out_style")]
        pub(crate) style: LoggingStyle,
    }

    #[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StdOutput {
    #[default]
    None,
    Stdout,
    Stderr,
}
impl From<StdOutput> for Option<wasm_workers::std_output_stream::StdOutput> {
    fn from(value: StdOutput) -> Self {
        match value {
            StdOutput::None => None,
            StdOutput::Stdout => Some(wasm_workers::std_output_stream::StdOutput::Stdout),
            StdOutput::Stderr => Some(wasm_workers::std_output_stream::StdOutput::Stderr),
        }
    }
}

pub(crate) mod webhook {
    use super::{
        BacktraceFrameFilesToSourcesVerified, ComponentCommon, ConfigName, StdOutput,
        WorkflowComponentBacktraceConfig, resolve_env_vars, verify_frame_files_to_sources,
    };
    use crate::config::{config_holder::PathPrefixes, env_var::EnvVarConfig};
    use anyhow::Context;
    use concepts::{ComponentId, ComponentType, StrVariant, component_id::InputContentDigest};
    use schemars::JsonSchema;
    use serde::Deserialize;
    use std::{
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::Arc,
    };
    use tracing::instrument;
    use wasm_workers::envvar::EnvVar;

    #[derive(Debug, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct HttpServer {
        pub(crate) name: ConfigName,
        pub(crate) listening_addr: SocketAddr,
    }

    #[derive(Debug, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookComponentConfigToml {
        // TODO: Rename to WebhookComponentConfigToml
        #[serde(flatten)]
        pub(crate) common: ComponentCommon,
        pub(crate) http_server: ConfigName,
        pub(crate) routes: Vec<WebhookRoute>,
        #[serde(default)]
        pub(crate) forward_stdout: StdOutput,
        #[serde(default)]
        pub(crate) forward_stderr: StdOutput,
        #[serde(default)]
        pub(crate) env_vars: Vec<EnvVarConfig>,
        #[serde(default)]
        pub(crate) backtrace: WorkflowComponentBacktraceConfig,
    }

    impl WebhookComponentConfigToml {
        #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref()), err)]
        pub(crate) async fn fetch_and_verify(
            self,
            wasm_cache_dir: Arc<Path>,
            metadata_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            path_prefixes: Arc<PathPrefixes>,
        ) -> Result<(ConfigName /* name */, WebhookComponentVerified), anyhow::Error> {
            let (common, wasm_path) = self
                .common
                .fetch_and_verify(&wasm_cache_dir, &metadata_dir, &path_prefixes)
                .await?;
            let frame_files_to_sources = verify_frame_files_to_sources(
                self.backtrace.frame_files_to_sources,
                &path_prefixes,
            );
            let component_id = ComponentId::new(
                ComponentType::WebhookEndpoint,
                StrVariant::from(common.name.clone()),
                InputContentDigest(common.content_digest),
            )?;
            Ok((
                common.name, // TODO: remove, already in component id
                WebhookComponentVerified {
                    component_id,
                    wasm_path,
                    routes: self
                        .routes
                        .into_iter()
                        .map(WebhookRouteVerified::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                    forward_stdout: self.forward_stdout.into(),
                    forward_stderr: self.forward_stderr.into(),
                    env_vars: resolve_env_vars(self.env_vars, ignore_missing_env_vars)?,
                    frame_files_to_sources,
                },
            ))
        }
    }

    #[derive(Debug, Deserialize, JsonSchema)]
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

    #[derive(Debug, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookRouteDetail {
        // Empty means all methods.
        #[serde(default)]
        pub(crate) methods: Vec<String>,
        pub(crate) route: String,
    }

    #[derive(Debug)]
    pub(crate) struct WebhookComponentVerified {
        // TODO: WebhookComponentConfigVerified
        pub(crate) component_id: ComponentId,
        pub(crate) wasm_path: PathBuf,
        pub(crate) routes: Vec<WebhookRouteVerified>,
        pub(crate) forward_stdout: Option<wasm_workers::std_output_stream::StdOutput>,
        pub(crate) forward_stderr: Option<wasm_workers::std_output_stream::StdOutput>,
        pub(crate) env_vars: Arc<[EnvVar]>,
        pub(crate) frame_files_to_sources: BacktraceFrameFilesToSourcesVerified,
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
                                .with_context(|| format!("cannot parse route method `{method}`",))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Self { methods, route }
                }
            })
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
#[derive(Debug, Deserialize, JsonSchema)]
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

fn resolve_env_vars(
    env_vars: Vec<EnvVarConfig>,
    ignore_missing: bool,
) -> Result<Arc<[EnvVar]>, anyhow::Error> {
    env_vars
        .into_iter()
        .map(|EnvVarConfig { key, val }| match val {
            Some(val) => Ok(EnvVar { key, val }),
            None => match std::env::var(&key) {
                Ok(val) => Ok(EnvVar { key, val }),
                Err(err) => {
                    if ignore_missing {
                        Ok(EnvVar {
                            key,
                            val: String::new(),
                        })
                    } else {
                        Err(anyhow!(
                            "cannot get environment variable `{key}` from the host - {err}"
                        ))
                    }
                }
            },
        })
        .collect::<Result<_, _>>()
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

const fn default_retry_on_err() -> bool {
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

const fn default_convert_core_module() -> bool {
    true
}

const fn default_lock_extension() -> DurationConfig {
    DurationConfig::Seconds(1)
}

const fn default_subscription_interruption() -> DurationConfigOptional {
    DurationConfigOptional::Seconds(1)
}

fn default_out_style() -> LoggingStyle {
    LoggingStyle::PlainCompact
}

fn default_sqlite_queue_capacity() -> usize {
    SqliteConfig::default().queue_capacity
}
fn default_workflows_lock_extension_leeway() -> DurationConfig {
    DurationConfig::Milliseconds(100)
}
fn default_activities_directories_enabled() -> bool {
    false
}

fn default_activities_directories_parent_directory() -> String {
    "${TEMP_DIR}/obelisk".to_string()
}

fn default_dir_cleanup_run_every() -> DurationConfig {
    DurationConfig::Minutes(1)
}
fn default_dir_cleanup_older_than() -> DurationConfig {
    DurationConfig::Minutes(5)
}
fn default_dir_cleanup_enabled() -> bool {
    true
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
}
