//! Server/runtime configuration (`obelisk.toml`): listeners, database, WASM engine
//! globals, watchers, allocator, and telemetry. Orthogonal to the deployment manifest.

use self::log::{LoggingConfig, LoggingStyle};
use crate::config::config_holder::{CACHE_DIR_PREFIX, DATA_DIR_PREFIX, PathPrefixes};
use crate::config::deployment::{
    AllowedHostToml, ConfigName, DurationConfig, DurationConfigOptional, InflightSemaphore,
    ValueOrUnlimited,
};
use crate::config::env_var::{interpolate_env_vars_plaintext, interpolate_env_vars_secret};
use crate::config::secret_registry::{SecretRegistry, SecretsToml};
use concepts::ContentDigest;
use concepts::component_id::Digest;
use db_postgres::postgres_dao::{self, PostgresConfig};
use db_sqlite::sqlite_dao::SqliteConfig;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Default, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServerConfigToml {
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) source_path: Option<PathBuf>,
    #[serde(default, rename = "obelisk-version")]
    pub(crate) obelisk_version: Option<String>,
    /// Operator-owned secret registry. Maps a logical secret name to a source
    /// (currently only `{ env = "VAR" }`). Env-backed secrets are resolved and
    /// their source variables wiped from the process environment at startup, before
    /// the tokio runtime starts. Deployments reference these names in `activity_exec`
    /// `secrets` and `allowed_host[].secrets`; they cannot interpolate them.
    #[serde(default)]
    pub(crate) secrets: SecretsToml,
    /// Permit deployments to run host processes through `activity_exec`.
    /// `false` denies all (default), `true` allows any, a map from exec activity
    /// names to `sha256:...` content digests allows only the named scripts.
    #[serde(default)]
    pub(crate) allow_exec_activities: AllowExecActivities,
    /// Operator-owned allowlist for component-originated HTTP requests.
    /// An empty allowlist denies every outbound request.
    #[serde(default)]
    pub(crate) outbound_http: OutboundHttpToml,
    /// Per-file size limit for deployment-owned blobs attached to a submit request.
    #[serde(default)]
    pub(crate) max_deployment_file_bytes: MaxDeploymentFileBytes,
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

#[derive(Debug, Default, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutboundHttpToml {
    /// Global outbound HTTP entries use the same grammar as deployment
    /// `allowed_host` entries.
    #[serde(default, rename = "allowed_host")]
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
}

/// Per-file size limit (in bytes) for deployment-owned blobs, defaulting to 20 MiB.
#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(transparent)]
pub(crate) struct MaxDeploymentFileBytes(pub(crate) u32);

impl Default for MaxDeploymentFileBytes {
    fn default() -> Self {
        Self(MAX_DEPLOYMENT_FILE_BYTES)
    }
}

/// Exec activity policy: deny all, allow any, or allow only named scripts whose
/// content digest matches the configured digest of the exact script text.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum AllowExecActivities {
    #[default]
    Deny,
    AllowAny,
    Allowlist(BTreeMap<String, ContentDigest>),
    // backcompat: 0.40.x accepted an unnamed list of content digests.
    LegacyAllowlist(Vec<ContentDigest>),
}

impl<'de> Deserialize<'de> for AllowExecActivities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct AllowExecActivitiesVisitor;
        impl<'de> serde::de::Visitor<'de> for AllowExecActivitiesVisitor {
            type Value = AllowExecActivities;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a boolean or a map from exec activity names to `sha256:...` content digests",
                )
            }

            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(if v {
                    AllowExecActivities::AllowAny
                } else {
                    AllowExecActivities::Deny
                })
            }

            // The `OBELISK__ALLOW_EXEC_ACTIVITIES` env override arrives as a string.
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v.parse::<bool>() {
                    Ok(v) => self.visit_bool(v),
                    Err(_) => Err(E::invalid_value(serde::de::Unexpected::Str(v), &self)),
                }
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut digests = Vec::new();
                while let Some(digest) = seq.next_element::<ContentDigest>()? {
                    digests.push(digest);
                }
                Ok(AllowExecActivities::LegacyAllowlist(digests))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut digests = BTreeMap::new();
                while let Some((name, digest)) = map.next_entry::<String, ContentDigest>()? {
                    if digests.insert(name.clone(), digest).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate exec activity name `{name}`"
                        )));
                    }
                }
                Ok(AllowExecActivities::Allowlist(digests))
            }
        }
        deserializer.deserialize_any(AllowExecActivitiesVisitor)
    }
}

impl JsonSchema for AllowExecActivities {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("AllowExecActivities")
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "anyOf": [
                {"type": "boolean"},
                {"type": "object", "additionalProperties": {"type": "string"}},
                // backcompat: 0.40.x accepted an unnamed list of content digests.
                {"type": "array", "items": {"type": "string"}}
            ]
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_api_listening_addr")]
    pub(crate) listening_addr: SocketAddr,
    /// Accepted API bearer tokens as `sha256:<hex>` digests of the token text.
    /// Only hashes of high-entropy random tokens are safe to commit: SHA-256 is fast and unsalted,
    /// so hashes of passwords or other guessable tokens are vulnerable to offline guessing.
    /// Tokens should contain at least 32 characters. Always generate an entry with
    /// `obelisk generate token`.
    #[serde(default)]
    pub(crate) token_hashes: Vec<Digest>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listening_addr: default_api_listening_addr(),
            token_hashes: Vec::new(),
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
    pub fn as_config(
        &self,
        secret_registry: &SecretRegistry,
    ) -> Result<PostgresConfig, anyhow::Error> {
        Ok(PostgresConfig {
            host: interpolate_env_vars_plaintext(&self.host, secret_registry)?,
            user: interpolate_env_vars_plaintext(&self.user, secret_registry)?,
            password: interpolate_env_vars_secret(&self.password, secret_registry)?,
            db_name: interpolate_env_vars_plaintext(&self.db_name, secret_registry)?,
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
        secret_registry: &SecretRegistry,
    ) -> Result<PathBuf, anyhow::Error> {
        let sqlite_file = self.directory.as_deref().unwrap_or_else(|| {
            if path_prefixes.project_dirs.is_some() {
                DEFAULT_SQLITE_DIR_IF_PROJECT_DIRS
            } else {
                DEFAULT_SQLITE_DIR
            }
        });
        path_prefixes
            .server_config_replace_path_prefix_mkdir(sqlite_file, secret_registry)
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
        secret_registry: &SecretRegistry,
    ) -> Result<PathBuf, anyhow::Error> {
        let wasm_directory = self.cache_directory.as_deref().unwrap_or_else(|| {
            if path_prefixes.project_dirs.is_some() {
                DEFAULT_WASM_DIRECTORY_IF_PROJECT_DIRS
            } else {
                DEFAULT_WASM_DIRECTORY
            }
        });
        path_prefixes
            .server_config_replace_path_prefix_mkdir(wasm_directory, secret_registry)
            .await
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowsGlobalConfigToml {
    /// Deprecated: set `lock_extension_leeway` on each `[[workflow_wasm]]` / `[[workflow_js]]`
    /// instead. When set, it overrides the per-workflow value for every workflow. Will be
    /// removed in 0.42.
    #[serde(default)]
    pub(crate) lock_extension_leeway: Option<DurationConfig>,
    /// Maximum number of captured writes a single replay pass returns. On reaching it, replay
    /// stops and returns that many writes as an advanceable prefix; advancing them and replaying
    /// again resumes from the persisted tip. Keeps a non-terminating workflow (e.g. an unresolved
    /// `joinNextTry` poll loop, whose replay never blocks) advanceable in bounded batches instead
    /// of collecting captured writes forever.
    #[serde(default = "default_max_replay_captured_writes")]
    pub(crate) max_replay_captured_writes: usize,
    /// Maximum number of history events a real workflow run may write before yielding.
    #[serde(default = "default_max_events_per_run")]
    #[schemars(range(min = 1))]
    pub(crate) max_events_per_run: usize,
    /// Number of newly written non-blocking events between database response refreshes.
    #[serde(default = "default_response_refresh_interval")]
    #[schemars(range(min = 1))]
    pub(crate) response_refresh_interval: usize,
}

impl Default for WorkflowsGlobalConfigToml {
    fn default() -> Self {
        Self {
            lock_extension_leeway: None,
            max_replay_captured_writes: default_max_replay_captured_writes(),
            max_events_per_run: default_max_events_per_run(),
            response_refresh_interval: default_response_refresh_interval(),
        }
    }
}

const fn default_max_replay_captured_writes() -> usize {
    100
}

const fn default_max_events_per_run() -> usize {
    100
}

const fn default_response_refresh_interval() -> usize {
    32
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
        secret_registry: &SecretRegistry,
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
                .server_config_replace_path_prefix_mkdir(directory, secret_registry)
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
    use super::{Deserialize, JsonSchema, default_console_enabled, default_console_style};
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

const fn default_true() -> bool {
    true
}

const fn default_parallel_compilation() -> bool {
    true
}

const fn default_debug() -> bool {
    false
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

// HTTP server declaration (referenced by ServerConfigToml)

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpServer {
    pub(crate) name: ConfigName,
    pub(crate) listening_addr: SocketAddr,
}

// Default on-disk locations and size limits for the server's data/cache directories.

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
pub(crate) const MAX_DEPLOYMENT_FILE_BYTES: u32 = 20 * 1024 * 1024; // 20MiB

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::deployment::{MethodsInput, ReplaceIn};

    mod outbound_http {
        use super::*;

        #[test]
        fn server_allowlist_uses_deployment_allowed_host_shape() {
            let config: ServerConfigToml = toml::from_str(
                r#"
                [secrets]
                API_KEY = { env = "API_KEY_SOURCE" }

                [[outbound_http.allowed_host]]
                pattern = "api.example.com"
                methods = ["POST"]
                request_url_regex = "^POST https://api\\.example\\.com/v1/"
                secrets = ["API_KEY"]
                replace_in = ["headers"]
                "#,
            )
            .unwrap();

            let entry = &config.outbound_http.allowed_hosts[0];
            assert_eq!(entry.pattern, "api.example.com");
            assert_eq!(entry.secrets, ["API_KEY"]);
            assert!(matches!(
                entry.methods,
                Some(MethodsInput::List(ref methods)) if methods.as_slice() == ["POST"]
            ));
            assert!(matches!(entry.replace_in.as_slice(), [ReplaceIn::Headers]));
        }

        #[test]
        fn omitted_server_allowlist_is_empty() {
            let config: ServerConfigToml = toml::from_str("").unwrap();
            assert!(config.outbound_http.allowed_hosts.is_empty());
        }
    }

    mod allow_exec_activities {
        use super::*;

        #[derive(serde::Deserialize, Debug)]
        struct TestConfig {
            #[serde(default)]
            allow: AllowExecActivities,
        }

        const DIGEST: &str =
            "sha256:abababababababababababababababababababababababababababababababab";

        #[test]
        fn deserialize_bool_map_and_legacy_digest_list() {
            let actual: TestConfig = toml::from_str("allow = true").unwrap();
            assert_eq!(AllowExecActivities::AllowAny, actual.allow);
            let actual: TestConfig = toml::from_str("allow = false").unwrap();
            assert_eq!(AllowExecActivities::Deny, actual.allow);
            let actual: TestConfig = toml::from_str("").unwrap();
            assert_eq!(AllowExecActivities::Deny, actual.allow);
            let actual: TestConfig =
                toml::from_str(&format!("[allow]\ngreet = \"{DIGEST}\"")).unwrap();
            assert_eq!(
                AllowExecActivities::Allowlist(BTreeMap::from([(
                    "greet".to_string(),
                    DIGEST.parse().unwrap()
                )])),
                actual.allow
            );
            let actual: TestConfig = toml::from_str(&format!("allow = [\"{DIGEST}\"]")).unwrap();
            assert_eq!(
                AllowExecActivities::LegacyAllowlist(vec![DIGEST.parse().unwrap()]),
                actual.allow
            );
        }

        #[test]
        fn deserialize_bool_string_as_sent_by_env_override() {
            // `OBELISK__ALLOW_EXEC_ACTIVITIES=true` reaches serde as a string.
            let actual: TestConfig = toml::from_str(r#"allow = "true""#).unwrap();
            assert_eq!(AllowExecActivities::AllowAny, actual.allow);
            toml::from_str::<TestConfig>(r#"allow = "yes""#).unwrap_err();
        }
    }
}
