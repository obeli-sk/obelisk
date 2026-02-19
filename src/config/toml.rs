use super::{config_holder::PathPrefixes, env_var::EnvVarConfig};
use crate::config::env_var::{
    EnvVarMissing, EnvVarsMissing, interpolate_env_vars_plaintext, interpolate_env_vars_secret,
};
use crate::github::{self, GH_SCHEMA_PREFIX, GitHubReleaseReference};
use crate::oci;
use crate::{
    config::config_holder::{CACHE_DIR_PREFIX, DATA_DIR_PREFIX},
    github::content_digest_to_wasm_file,
};
use anyhow::{Context, ensure};
use anyhow::{anyhow, bail};
use concepts::{
    ComponentId, ComponentRetryConfig, ComponentType, FunctionFqn, InvalidNameError, StrVariant,
    check_name, component_id::InputContentDigest, prefixed_ulid::ExecutorId, storage::LogLevel,
};
use concepts::{ContentDigest, prefixed_ulid::DeploymentId};
use db_postgres::postgres_dao::{self, PostgresConfig};
use db_sqlite::sqlite_dao::SqliteConfig;
use hashbrown::HashMap;
use log::{LoggingConfig, LoggingStyle};
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tracing::{info, instrument, trace, warn};
use utils::wasm_tools::WasmComponent;
use wasm_workers::http_request_policy::HostPatternError;
use wasm_workers::{
    activity::activity_worker::{ActivityConfig, ActivityDirectoriesConfig, ProcessProvider},
    envvar::EnvVar,
    http_request_policy::{AllowedHostConfig, HostPattern, ReplacementLocation},
    std_output_stream::StdOutputConfig,
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
    #[serde(default, rename = "obelisk-version")]
    pub(crate) obelisk_version: Option<String>,
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    deployment_id: Option<DeploymentId>,
    #[serde(default)]
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
    #[serde(default, rename = "activity_js")]
    pub(crate) activities_js: Vec<ActivityJsComponentConfigToml>,
    #[serde(default, rename = "workflow")]
    pub(crate) workflows: Vec<WorkflowComponentConfigToml>,
    #[serde(default, rename = "workflow_js")]
    pub(crate) workflows_js: Vec<WorkflowJsComponentConfigToml>,
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
impl ConfigToml {
    pub(crate) fn get_deployment_id(&self) -> DeploymentId {
        self.deployment_id.unwrap_or_else(DeploymentId::generate)
    }
}

#[derive(Debug, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiConfig {
    pub(crate) listening_addr: Option<SocketAddr>,
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

// Components

#[derive(Debug, Clone, Hash)]
pub(crate) struct ComponentCommonVerified {
    pub(crate) name: ConfigName,
    pub(crate) location: ComponentLocationToml,
    pub(crate) content_digest: ContentDigest,
}

#[derive(Debug, Clone, Hash, JsonSchema, serde_with::DeserializeFromStr)]
#[serde(rename_all = "snake_case")]
#[schemars(with = "String")]
pub(crate) enum ComponentLocationToml {
    Path(String), // String because it can contain path prefix
    Oci(
        // #[serde_as(as = "serde_with::DisplayFromStr")]
        oci_client::Reference,
    ),
    GitHub(GitHubReleaseReference),
}
impl ComponentLocationToml {
    /// Fetch wasm file, calculate its content digest.
    ///
    /// Read wasm file either from local fs, pull from an OCI registry, or pull from GitHub release and cache it.
    /// Calculate the `content_digest`. File is not converted from Core to Component format.
    /// If `expected_content_digest` is specified:
    /// - try to find it in cache instead of download. (We trust that the cache was not tampered with).
    /// - if downloaded, digests must match.
    pub(crate) async fn fetch(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
        path_prefixes: &PathPrefixes,
        expected_digest: Option<&ContentDigest>,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
        use utils::sha256sum::calculate_sha256_file;

        // Happy path: if content_digest is known and file exists in cache, return immediately
        if let Some(expected_digest) = expected_digest
            && let wasm_path = content_digest_to_wasm_file(wasm_cache_dir, expected_digest)
            && wasm_path.exists()
        {
            trace!("Using cached file for known content digest");
            return Ok((expected_digest.clone(), wasm_path));
        }

        let (actual_digest, path) = match &self {
            ComponentLocationToml::Path(wasm_path) => {
                let wasm_path = path_prefixes.replace_file_prefix_verify_exists(wasm_path)?;
                let actual_digest = calculate_sha256_file(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
                (actual_digest, wasm_path)
            }
            ComponentLocationToml::Oci(image) => {
                oci::pull_to_cache_dir(image, wasm_cache_dir, metadata_dir)
                    .await
                    .context("try cleaning the cache directory with `--clean-cache`")?
            }
            ComponentLocationToml::GitHub(github_ref) => {
                let (actual_digest, wasm_path) =
                    github::pull_to_cache_dir(github_ref, wasm_cache_dir)
                        .await
                        .context("try cleaning the cache directory with `--clean-cache`")?;
                // Suggest adding content_digest to config
                if expected_digest.is_none() {
                    info!(
                        r#"No content_digest specified for GitHub release component. Consider adding content_digest = "{}" to avoid refetching"#,
                        actual_digest.with_infix(":")
                    );
                }

                (actual_digest, wasm_path)
            }
        };
        if let Some(expected_digest) = expected_digest {
            ensure!(
                *expected_digest == actual_digest,
                "content digest mismatch: expected {expected_digest}, got {actual_digest}"
            );
        }
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
        } else if let Some(location) = s.strip_prefix(GH_SCHEMA_PREFIX) {
            Ok(ComponentLocationToml::GitHub(
                GitHubReleaseReference::from_str(location)?,
            ))
        } else {
            Ok(ComponentLocationToml::Path(s.to_string()))
        }
    }
}

/// Location of a JavaScript source file for JS activities.
/// Supports local file paths and `gh://` GitHub release references.
/// OCI references are not supported.
#[derive(Debug, Clone, Hash, JsonSchema, serde_with::DeserializeFromStr)]
#[schemars(with = "String")]
pub(crate) enum JsLocationToml {
    Path(String),
    GitHub(GitHubReleaseReference),
}

impl std::fmt::Display for JsLocationToml {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsLocationToml::Path(p) => write!(f, "{p}"),
            JsLocationToml::GitHub(gh) => write!(f, "{GH_SCHEMA_PREFIX}{gh}"),
        }
    }
}

impl FromStr for JsLocationToml {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(OCI_SCHEMA_PREFIX) {
            bail!(
                "OCI references are not supported for JS activities. Use a local file path or gh:// reference."
            );
        } else if let Some(location) = s.strip_prefix(GH_SCHEMA_PREFIX) {
            Ok(JsLocationToml::GitHub(GitHubReleaseReference::from_str(
                location,
            )?))
        } else {
            Ok(JsLocationToml::Path(s.to_string()))
        }
    }
}

impl JsLocationToml {
    /// Fetch the JS source file and return its content as a string.
    ///
    /// For local files, reads directly from the filesystem.
    /// For GitHub releases, downloads to `wasm_cache_dir` (reusing the existing cache),
    /// then reads the cached file.
    pub(crate) async fn read_to_string(
        &self,
        wasm_cache_dir: &Path,
        path_prefixes: &PathPrefixes,
        expected_digest: Option<&ContentDigest>,
    ) -> Result<String, anyhow::Error> {
        let file_path = match self {
            JsLocationToml::Path(path) => path_prefixes.replace_file_prefix_verify_exists(path)?,
            JsLocationToml::GitHub(github_ref) => {
                // Happy path: if content_digest is known and file exists in cache, use it
                if let Some(expected) = expected_digest {
                    let cached = content_digest_to_wasm_file(wasm_cache_dir, expected);
                    if cached.exists() {
                        trace!("Using cached JS source for known content digest");
                        return tokio::fs::read_to_string(&cached).await.with_context(|| {
                            format!("cannot read cached JS source file `{cached:?}`")
                        });
                    }
                }
                let (actual_digest, cached_path) =
                    github::pull_to_cache_dir(github_ref, wasm_cache_dir)
                        .await
                        .context("cannot fetch JS source from GitHub release")?;
                if let Some(expected) = expected_digest {
                    ensure!(
                        *expected == actual_digest,
                        "content digest mismatch for JS source: expected {expected}, got {actual_digest}"
                    );
                } else {
                    info!(
                        r#"No content_digest specified for GitHub release JS source. Consider adding content_digest = "{}" to avoid refetching"#,
                        actual_digest.with_infix(":")
                    );
                }
                cached_path
            }
        };
        let content = tokio::fs::read_to_string(&file_path)
            .await
            .with_context(|| format!("cannot read JS source file `{file_path:?}`"))?;
        Ok(content)
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
    pub(crate) location: ComponentLocationToml,
    /// Content digest of the WASM file.
    /// Recommended for GitHub releases to enable caching and reproducible builds.
    /// If the file is found in cache, the download is bypassed.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
}

impl ComponentCommon {
    async fn fetch(
        self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
        path_prefixes: &PathPrefixes,
    ) -> Result<(ComponentCommonVerified, PathBuf), anyhow::Error> {
        let (fetched_digest, wasm_path) = self
            .location
            .fetch(
                wasm_cache_dir,
                metadata_dir,
                path_prefixes,
                self.content_digest.as_ref(),
            )
            .await?;

        let verified = ComponentCommonVerified {
            name: self.name,
            location: self.location,
            content_digest: fetched_digest,
        };
        Ok((verified, wasm_path))
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
impl Default for LockingStrategy {
    fn default() -> Self {
        executor::executor::LockingStrategy::ByComponentDigest.into()
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
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    #[serde(default)]
    pub(crate) directories: ActivityDirectoriesConfigToml,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default)]
    pub(crate) allowed_host: Vec<AllowedHostToml>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
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
#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplaceIn {
    Headers,
    Body,
    Params,
}

/// An allowed outgoing HTTP host with optional method restrictions and secrets.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AllowedHostToml {
    /// Host pattern (e.g. `"api.example.com"`, `"*.example.com"`, `"http://localhost:8080"`).
    pub pattern: String,
    /// Allowed HTTP methods. Empty or omitted means all methods are allowed.
    #[serde(default)]
    pub methods: Vec<String>,
    /// Optional secrets for this host.
    #[serde(default)]
    pub secrets: Option<AllowedHostSecretsToml>,
}

/// Secrets configuration for an allowed host.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AllowedHostSecretsToml {
    /// Env vars using the same syntax as top-level `env_vars`.
    pub env_vars: Vec<EnvVarConfig>,
    /// Where in the request to perform replacement.
    /// Default: empty (no replacement — deny by default).
    #[serde(default)]
    pub replace_in: Vec<ReplaceIn>,
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
            .fetch(&wasm_cache_dir, &metadata_dir, &path_prefixes)
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
    pub(crate) logs_store_min_level: Option<LogLevel>,
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
            .fetch(&wasm_cache_dir, &metadata_dir, &path_prefixes)
            .await?;

        let env_vars = resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars)?;
        let allowed_hosts = resolve_allowed_hosts(self.allowed_host, ignore_missing_env_vars)?;

        // Validate no collision between env_vars and secret env names
        validate_no_env_collision(&env_vars, &allowed_hosts)?;

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
            directories_config,
            fuel,
            allowed_hosts,
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
            ),
            logs_store_min_level: self.logs_store_min_level.into(),
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityJsComponentConfigToml {
    pub(crate) name: ConfigName,
    /// Location of the JavaScript source file.
    /// Supports local file paths and `gh://` GitHub release references.
    /// OCI references are not supported for JS activities.
    pub(crate) location: JsLocationToml,
    /// Content digest of the JS source file.
    /// Recommended for GitHub releases to enable caching and reproducible builds.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    pub(crate) ffqn: String,
    /// Custom parameters for the JS function.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    /// If omitted, defaults to a single `(params: list<string>)` parameter.
    #[serde(default)]
    pub(crate) params: ParamsSpec,
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
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default)]
    pub(crate) allowed_host: Vec<AllowedHostToml>,
}

#[derive(Debug, Default, Deserialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParamsSpec {
    #[default]
    Default, // `(params: list<string>)`
    Inline(Vec<JsParamToml>),
    // TODO: Add a WIT folder location later
}

/// A parameter declaration for a JS activity function.
#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct JsParamToml {
    /// Parameter name (used in WIT metadata).
    pub(crate) name: String,
    /// WIT type string, e.g. `string`, `u32`, `list<string>`, `option<u64>`.
    #[serde(rename = "type")]
    pub(crate) wit_type: String,
}

#[derive(Debug)]
pub(crate) struct ActivityJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>, // same for all JS activities
    pub(crate) js_source: String,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityJsConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }
}

impl ActivityJsComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.name.0.as_ref()))]
    pub(crate) async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        path_prefixes: Arc<PathPrefixes>,
        ignore_missing_env_vars: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityJsConfigVerified, anyhow::Error> {
        let ffqn = FunctionFqn::from_str(&self.ffqn)
            .map_err(|e| anyhow!("invalid ffqn `{}`: {e}", self.ffqn))?;

        // Parse custom params or default to `params: list<string>`
        let parsed_params = match self.params {
            ParamsSpec::Default => {
                vec![concepts::ParameterType {
                    type_wrapper: val_json::type_wrapper::TypeWrapper::List(Box::new(
                        val_json::type_wrapper::TypeWrapper::String,
                    )),
                    name: StrVariant::Static("params"),
                    wit_type: StrVariant::Static("list<string>"),
                }]
            }
            ParamsSpec::Inline(params) => params
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

        let js_source = self
            .location
            .read_to_string(
                &wasm_cache_dir,
                &path_prefixes,
                self.content_digest.as_ref(),
            )
            .await?;

        // Compute content digest from source + ffqn + params
        use sha2::{Digest as _, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"activity_js:");
        hasher.update(js_source.as_bytes());
        hasher.update(self.ffqn.as_bytes());
        for p in &parsed_params {
            hasher.update(p.wit_type.as_ref().as_bytes());
        }
        let hash: [u8; 32] = hasher.finalize().into();
        let content_digest = concepts::ContentDigest(concepts::component_id::Digest(hash));

        let component_id = ComponentId::new(
            ComponentType::ActivityWasm,
            StrVariant::from(self.name),
            InputContentDigest(content_digest),
        )?;

        let allowed_hosts = resolve_allowed_hosts(self.allowed_host, ignore_missing_env_vars)?;

        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into(),
            forward_stderr: self.forward_stderr.into(),
            env_vars: Arc::from([]),
            directories_config: None,
            fuel,
            allowed_hosts,
        };

        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };

        Ok(ActivityJsConfigVerified {
            wasm_path,
            js_source,
            ffqn,
            params: parsed_params,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            ),
            logs_store_min_level: self.logs_store_min_level.into(),
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowJsComponentConfigToml {
    pub(crate) name: ConfigName,
    /// Location of the JavaScript source file.
    /// Supports local file paths and `gh://` GitHub release references.
    /// OCI references are not supported for JS workflows.
    pub(crate) location: JsLocationToml,
    /// Content digest of the JS source file.
    /// Recommended for GitHub releases to enable caching and reproducible builds.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    pub(crate) ffqn: String,
    /// Custom parameters for the JS workflow function.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    /// If omitted, defaults to a single `(params: list<string>)` parameter.
    #[serde(default)]
    pub(crate) params: ParamsSpec,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    #[serde(default = "default_lock_extension")]
    lock_extension: DurationConfig,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
}

#[derive(Debug)]
pub(crate) struct WorkflowJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>, // same for all JS workflows
    pub(crate) js_source: String,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl WorkflowJsConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.workflow_config.component_id
    }
}

impl WorkflowJsComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.name.0.as_ref()))]
    pub(crate) async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        path_prefixes: Arc<PathPrefixes>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<WorkflowJsConfigVerified, anyhow::Error> {
        let ffqn = FunctionFqn::from_str(&self.ffqn)
            .map_err(|e| anyhow!("invalid ffqn `{}`: {e}", self.ffqn))?;

        // Parse custom params or default to `params: list<string>`
        let parsed_params = match self.params {
            ParamsSpec::Default => {
                vec![concepts::ParameterType {
                    type_wrapper: val_json::type_wrapper::TypeWrapper::List(Box::new(
                        val_json::type_wrapper::TypeWrapper::String,
                    )),
                    name: StrVariant::Static("params"),
                    wit_type: StrVariant::Static("list<string>"),
                }]
            }
            ParamsSpec::Inline(params) => params
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

        let js_source = self
            .location
            .read_to_string(
                &wasm_cache_dir,
                &path_prefixes,
                self.content_digest.as_ref(),
            )
            .await?;

        // Compute content digest from source + ffqn + params
        use sha2::{Digest as _, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"workflow_js:");
        hasher.update(js_source.as_bytes());
        hasher.update(self.ffqn.as_bytes());
        for p in &parsed_params {
            hasher.update(p.wit_type.as_ref().as_bytes());
        }
        let hash: [u8; 32] = hasher.finalize().into();
        let content_digest = concepts::ContentDigest(concepts::component_id::Digest(hash));

        let component_id = ComponentId::new(
            ComponentType::Workflow, // Use Workflow type, not a separate WorkflowJs
            StrVariant::from(self.name),
            InputContentDigest(content_digest),
        )?;

        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            join_next_blocking_strategy: self.blocking_strategy.into(),
            backtrace_persist: false, // JS workflows don't support backtraces
            stub_wasi: false,
            fuel: None, // Fuel is controlled by the JS runtime
            lock_extension: self.lock_extension.into(),
            subscription_interruption: None,
        };

        let retry_config = ComponentRetryConfig {
            max_retries: None, // Workflows retry forever
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };

        Ok(WorkflowJsConfigVerified {
            wasm_path,
            js_source,
            ffqn,
            params: parsed_params,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            ),
            logs_store_min_level: self.logs_store_min_level.into(),
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
    #[serde(default)]
    pub(crate) backtrace: ComponentBacktraceConfig,
    #[serde(default)]
    pub(crate) stub_wasi: bool,
    #[serde(default = "default_lock_extension")]
    lock_extension: DurationConfig,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
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
#[derive(Debug, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentBacktraceConfig {
    #[serde(rename = "sources")]
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub(crate) frame_files_to_sources: HashMap<String, String>,
    #[serde(default)]
    pub(crate) ignore_component_digest: bool,
}
impl ComponentBacktraceConfig {
    fn verify(self, path_prefixes: &PathPrefixes) -> ComponentBacktraceConfigVerified {
        let frame_files_to_sources = self
            .frame_files_to_sources
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
            .collect();

        ComponentBacktraceConfigVerified {
            frame_files_to_sources,
            ignore_component_digest: self.ignore_component_digest,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ComponentBacktraceConfigVerified {
    pub(crate) frame_files_to_sources: HashMap<String, PathBuf>,
    pub(crate) ignore_component_digest: bool,
}

#[derive(Debug)]
pub(crate) struct WorkflowConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) backtrace_config: ComponentBacktraceConfigVerified,
    pub(crate) logs_store_min_level: Option<LogLevel>,
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
            .fetch(&wasm_cache_dir, &metadata_dir, &path_prefixes)
            .await?;
        let wasm_path = WasmComponent::convert_core_module_to_component(
            &wasm_path,
            &common.content_digest,
            &wasm_cache_dir,
        )
        .await?
        .unwrap_or(wasm_path); // None means the original is already Component

        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(common.name),
            InputContentDigest(common.content_digest), // NB: content digest belongs to the original (untransformed) file.
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
        let backtrace_config = self.backtrace.verify(&path_prefixes);
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
            ),
            backtrace_config,
            logs_store_min_level: self.logs_store_min_level.into(),
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
    use crate::config::toml::default_console_enabled;

    use super::{Deserialize, JsonSchema, default_console_style};
    use serde_with::serde_as;
    use std::str::FromStr;

    #[derive(Debug, Deserialize, JsonSchema, Default)]
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
            Self::from_str("info,app=debug").expect("empty directive must not fail to parse")
        }
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
    pub(crate) enum AppenderConsoleWriter {
        #[default]
        Stderr,
        Stdout,
    }

    #[derive(Debug, Deserialize, JsonSchema)]
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
        AllowedHostToml, ComponentBacktraceConfig, ComponentCommon, ComponentStdOutputToml,
        ConfigName, resolve_allowed_hosts, resolve_env_vars_plaintext, validate_no_env_collision,
    };
    use crate::config::{
        config_holder::PathPrefixes,
        env_var::EnvVarConfig,
        toml::{ComponentBacktraceConfigVerified, LogLevelToml},
    };
    use anyhow::Context;
    use concepts::{
        ComponentId, ComponentType, StrVariant, component_id::InputContentDigest, storage::LogLevel,
    };
    use schemars::JsonSchema;
    use serde::Deserialize;
    use std::{
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };
    use tracing::instrument;
    use wasm_workers::{
        envvar::EnvVar, http_request_policy::AllowedHostConfig, std_output_stream::StdOutputConfig,
    };

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
        #[serde(default)]
        pub(crate) allowed_host: Vec<AllowedHostToml>,
    }

    impl WebhookComponentConfigToml {
        #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref()), err)]
        pub(crate) async fn fetch_and_verify(
            self,
            wasm_cache_dir: Arc<Path>,
            metadata_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            path_prefixes: Arc<PathPrefixes>,
            subscription_interruption: Option<Duration>,
        ) -> Result<(ConfigName /* name */, WebhookComponentConfigVerified), anyhow::Error>
        {
            let (common, wasm_path) = self
                .common
                .fetch(&wasm_cache_dir, &metadata_dir, &path_prefixes)
                .await?;
            let backtrace_config = self.backtrace.verify(&path_prefixes);
            let component_id = ComponentId::new(
                ComponentType::WebhookEndpoint,
                StrVariant::from(common.name.clone()),
                InputContentDigest(common.content_digest),
            )?;
            let env_vars = resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars)?;
            let allowed_hosts = resolve_allowed_hosts(self.allowed_host, ignore_missing_env_vars)?;

            // Validate no collision between env_vars and secret env names
            validate_no_env_collision(&env_vars, &allowed_hosts)?;

            Ok((
                common.name, // TODO: remove, already in component id
                WebhookComponentConfigVerified {
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
                    backtrace_config,
                    subscription_interruption,
                    logs_store_min_level: self.logs_store_min_level.into(),
                    allowed_hosts,
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
    pub(crate) struct WebhookComponentConfigVerified {
        pub(crate) component_id: ComponentId,
        pub(crate) wasm_path: PathBuf,
        pub(crate) routes: Vec<WebhookRouteVerified>,
        pub(crate) forward_stdout: Option<StdOutputConfig>,
        pub(crate) forward_stderr: Option<StdOutputConfig>,
        pub(crate) env_vars: Arc<[EnvVar]>,
        pub(crate) backtrace_config: ComponentBacktraceConfigVerified,
        pub(crate) subscription_interruption: Option<Duration>,
        pub(crate) logs_store_min_level: Option<LogLevel>,
        pub(crate) allowed_hosts: Arc<[AllowedHostConfig]>,
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

// TODO: Move to env_var module
fn resolve_env_vars_plaintext(
    env_vars: Vec<EnvVarConfig>,
    ignore_missing: bool,
) -> Result<Arc<[EnvVar]>, EnvVarMissing> {
    env_vars
        .into_iter()
        .map(|EnvVarConfig { key, val }| match val {
            Some(val) => Ok(EnvVar {
                key,
                val: interpolate_env_vars_plaintext(&val)?,
            }),
            None => match std::env::var(&key) {
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
}

fn resolve_allowed_hosts(
    entries: Vec<AllowedHostToml>,
    ignore_missing_env_vars: bool,
) -> Result<Arc<[AllowedHostConfig]>, ResolveAllowedHostsError> {
    entries
        .into_iter()
        .map(|entry| {
            let methods = entry
                .methods
                .into_iter()
                .map(|m| {
                    http::Method::from_bytes(m.as_bytes())
                        .map_err(|_| ResolveAllowedHostsError::InvalidMethod(m))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let pattern = HostPattern::parse_with_methods(&entry.pattern, methods)?;

            let (secret_env_mappings, replace_in) = if let Some(secrets) = entry.secrets {
                if secrets.env_vars.is_empty() {
                    warn!("allowed_host `{}` has empty `secrets.env_vars`", entry.pattern);
                }
                if secrets.replace_in.is_empty() {
                    warn!(
                        "allowed_host `{}` has empty `secrets.replace_in` - secrets will never be injected",
                        entry.pattern
                    );
                }
                if pattern.scheme == "http" {
                    warn!("secrets allowed for unencrypted host `{pattern}`");
                }

                let env_mappings = resolve_secret_env_vars(
                    secrets.env_vars,
                    ignore_missing_env_vars,
                )?;
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

            Ok(AllowedHostConfig {
                pattern,
                secret_env_mappings,
                replace_in,
            })
        })
        .collect::<Result<_, _>>()
}

fn resolve_secret_env_vars(
    env_vars: Vec<EnvVarConfig>,
    ignore_missing: bool,
) -> Result<Vec<(String, SecretString)>, ResolveAllowedHostsError> {
    let mut missing = vec![];
    let mut env_mappings = Vec::new();
    for EnvVarConfig {
        key,
        val: toml_supplied_val,
    } in env_vars
    {
        match toml_supplied_val {
            Some(val) => match interpolate_env_vars_secret(&val) {
                Ok(real_value) => env_mappings.push((key, real_value)),
                Err(err) => missing.push(err.0),
            },
            None => match std::env::var(&key) {
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

const fn default_lock_extension() -> DurationConfig {
    DurationConfig::Seconds(1)
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

    mod component_location {
        use super::super::*;
        use crate::github::GitHubReleaseTag;

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

        #[test]
        fn parse_github_release_specific_tag() {
            let location: ComponentLocationToml = "gh://obeli-sk/obelisk@v0.34.1/my-component.wasm"
                .parse()
                .unwrap();
            match location {
                ComponentLocationToml::GitHub(ref gh_ref) => {
                    assert_eq!(gh_ref.owner, "obeli-sk");
                    assert_eq!(gh_ref.repo, "obelisk");
                    assert_eq!(
                        gh_ref.tag,
                        GitHubReleaseTag::Specific("v0.34.1".to_string())
                    );
                    assert_eq!(gh_ref.asset_name, "my-component.wasm");
                }
                _ => panic!("expected GitHub variant"),
            }
        }

        #[test]
        fn parse_github_release_latest() {
            let location: ComponentLocationToml = "gh://obeli-sk/obelisk@latest/my-component.wasm"
                .parse()
                .unwrap();
            match location {
                ComponentLocationToml::GitHub(ref gh_ref) => {
                    assert_eq!(gh_ref.tag, GitHubReleaseTag::Latest);
                }
                _ => panic!("expected GitHub variant"),
            }
        }

        #[test]
        fn parse_github_release_invalid() {
            let result: Result<ComponentLocationToml, _> = "gh://invalid-format".parse();
            assert!(result.is_err());
        }

        #[test]
        fn deserialize_component_common_with_content_digest() {
            let toml_str = r#"
name = "my_component"
location = "gh://owner/repo@v1.0.0/component.wasm"
content_digest = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
"#;
            let common: ComponentCommon = toml::from_str(toml_str).unwrap();
            assert!(common.content_digest.is_some());
        }

        #[test]
        fn deserialize_component_common_without_content_digest() {
            let toml_str = r#"
name = "my_component"
location = "gh://owner/repo@v1.0.0/component.wasm"
"#;
            let common: ComponentCommon = toml::from_str(toml_str).unwrap();
            assert!(common.content_digest.is_none());
        }
    }
}
