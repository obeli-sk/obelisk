use super::{
    config_holder::PathPrefixes, env_var::EnvVarConfig, ComponentLocation, ConfigStoreCommon,
};
use anyhow::{anyhow, bail};
use concepts::{
    check_name, ComponentId, ComponentRetryConfig, ComponentType, ContentDigest, InvalidNameError,
    StrVariant,
};
use db_sqlite::sqlite_dao::SqliteConfig;
use log::{LoggingConfig, LoggingStyle};
use serde::{Deserialize, Deserializer};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tracing::{instrument, warn};
use util::{
    replace_file_prefix_no_verify, replace_file_prefix_verify_exists, replace_path_prefix_mkdir,
};
use utils::wasm_tools::WasmComponent;
use wasm_workers::{
    activity::activity_worker::ActivityConfig,
    envvar::EnvVar,
    workflow::workflow_worker::{JoinNextBlockingStrategy, WorkflowConfig},
};
use wasmtime::WasmBacktraceDetails;
use webhook::{HttpServer, WebhookComponentConfigToml};

// Path prefixes
const HOME_DIR_PREFIX: &str = "~/";
const DATA_DIR_PREFIX: &str = "${DATA_DIR}/";
const CACHE_DIR_PREFIX: &str = "${CACHE_DIR}/";
const CONFIG_DIR_PREFIX: &str = "${CONFIG_DIR}/";
const OBELISK_TOML_DIR_PREFIX: &str = "${OBELISK_TOML_DIR}/";

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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigToml {
    pub(crate) api: ApiConfig,
    #[serde(default)]
    pub(crate) sqlite: SqliteConfigToml,
    #[serde(default)]
    pub(crate) webui: WebUIConfig,
    #[serde(default, rename = "workflows")]
    pub(crate) workflows_global_config: WorkflowsGlobalConfigToml,
    #[serde(default)]
    wasm_cache_directory: Option<String>,
    #[serde(default)]
    pub(crate) codegen_cache: CodegenCache,
    #[serde(default, rename = "activity_wasm")]
    pub(crate) wasm_activities: Vec<ActivityComponentConfigToml>,
    #[serde(default, rename = "workflow")]
    pub(crate) workflows: Vec<WorkflowComponentConfigToml>,
    #[serde(default)]
    pub(crate) wasmtime_allocator_config: WasmtimeAllocatorConfig,
    #[serde(default)]
    pub(crate) wasmtime_pooling_config: WasmtimePoolingAllocatorConfig,
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
    pub(crate) async fn get_wasm_cache_directory(
        &self,
        path_prefixes: &PathPrefixes,
    ) -> Result<PathBuf, anyhow::Error> {
        let wasm_directory = self.wasm_cache_directory.as_deref().unwrap_or_else(|| {
            if path_prefixes.project_dirs.is_some() {
                DEFAULT_WASM_DIRECTORY_IF_PROJECT_DIRS
            } else {
                DEFAULT_WASM_DIRECTORY
            }
        });
        replace_path_prefix_mkdir(wasm_directory, path_prefixes).await
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiConfig {
    pub(crate) listening_addr: SocketAddr,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct SqliteConfigToml {
    #[serde(default)]
    directory: Option<String>,
    queue_capacity: Option<usize>,
    low_prio_threshold: Option<usize>,
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
        replace_path_prefix_mkdir(sqlite_file, path_prefixes).await
    }

    pub(crate) fn as_config(&self) -> SqliteConfig {
        let def = SqliteConfig::default();
        SqliteConfig {
            queue_capacity: self.queue_capacity.unwrap_or(def.queue_capacity),
            low_prio_threshold: self.low_prio_threshold.unwrap_or(def.low_prio_threshold),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebUIConfig {
    pub(crate) listening_addr: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowsGlobalConfigToml {
    #[serde(default)]
    pub(crate) backtrace: WorkflowsGlobalBacktrace,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowsGlobalBacktrace {
    #[serde(default = "default_global_backtrace_persist")]
    pub(crate) persist: bool,
}

impl WorkflowsGlobalBacktrace {
    pub(crate) fn persist_backtrace(&self) -> WasmBacktraceDetails {
        if self.persist {
            WasmBacktraceDetails::Enable
        } else {
            WasmBacktraceDetails::Disable
        }
    }
}
impl Default for WorkflowsGlobalBacktrace {
    fn default() -> Self {
        Self {
            persist: default_global_backtrace_persist(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodegenCache {
    #[serde(default = "default_codegen_enabled")]
    pub enabled: bool,
    #[serde(default)]
    directory: Option<String>,
}

impl Default for CodegenCache {
    fn default() -> Self {
        Self {
            enabled: true,
            directory: None,
        }
    }
}

impl CodegenCache {
    pub(crate) async fn get_directory(
        &self,
        path_prefixes: &PathPrefixes,
    ) -> Result<PathBuf, anyhow::Error> {
        let directory = self.directory.as_deref().unwrap_or_else(|| {
            if path_prefixes.project_dirs.is_some() {
                DEFAULT_CODEGEN_CACHE_DIRECTORY_IF_PROJECT_DIRS
            } else {
                DEFAULT_CODEGEN_CACHE_DIRECTORY
            }
        });
        replace_path_prefix_mkdir(directory, path_prefixes).await
    }
}

/// Activity, Webhook, Workflow or a Http server
#[derive(Debug, Clone, Hash, PartialEq, Eq, derive_more::Display, derive_more::Into)]
#[display("{_0}")]
pub struct ConfigName(StrVariant);
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

#[derive(Debug, Deserialize)]
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
    ) -> Result<(ConfigStoreCommon, PathBuf), anyhow::Error> {
        let (actual_content_digest, wasm_path) = self
            .location
            .obtain_wasm(wasm_cache_dir, metadata_dir)
            .await?;
        let verified = ConfigStoreCommon {
            name: self.name,
            location: self.location,
            content_digest: actual_content_digest,
        };
        Ok((verified, wasm_path))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecConfigToml {
    #[serde(default = "default_batch_size")]
    batch_size: u32,
    #[serde(default = "default_lock_expiry")]
    lock_expiry: DurationConfig,
    #[serde(default = "default_tick_sleep")]
    tick_sleep: DurationConfig,
    #[serde(default)]
    pub(crate) max_inflight_instances: InflightSemaphore,
}

impl Default for ExecConfigToml {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            lock_expiry: default_lock_expiry(),
            tick_sleep: default_tick_sleep(),
            max_inflight_instances: InflightSemaphore::default(),
        }
    }
}

impl ExecConfigToml {
    pub(crate) fn into_exec_exec_config(
        self,
        component_id: ComponentId,
    ) -> executor::executor::ExecConfig {
        executor::executor::ExecConfig {
            lock_expiry: self.lock_expiry.into(),
            tick_sleep: self.tick_sleep.into(),
            batch_size: self.batch_size,
            component_id,
            task_limiter: self.max_inflight_instances.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityComponentConfigToml {
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
}

#[derive(Debug)]
pub(crate) struct ActivityWasmConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) retry_config: ComponentRetryConfig,
    pub(crate) content_digest: ContentDigest,
}

impl ActivityWasmConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }
}

impl ActivityComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref(), component_id))]
    pub(crate) async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
    ) -> Result<ActivityWasmConfigVerified, anyhow::Error> {
        let component_id = ComponentId::new(
            ComponentType::ActivityWasm,
            StrVariant::from(self.common.name.clone()),
        )?;
        tracing::Span::current().record("component_id", tracing::field::display(&component_id));

        let (common, wasm_path) = self
            .common
            .fetch_and_verify(&wasm_cache_dir, &metadata_dir)
            .await?;

        let env_vars = resolve_env_vars(self.env_vars, ignore_missing_env_vars)?;
        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into(),
            forward_stderr: self.forward_stderr.into(),
            env_vars,
            retry_on_err: self.retry_on_err,
        };
        Ok(ActivityWasmConfigVerified {
            content_digest: common.content_digest,
            wasm_path,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(component_id),
            retry_config: ComponentRetryConfig {
                max_retries: self.max_retries,
                retry_exp_backoff: self.retry_exp_backoff.into(),
            },
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default = "default_strategy")]
    pub(crate) join_next_blocking_strategy: JoinNextBlockingStrategy,
    #[serde(default = "default_non_blocking_event_batching")]
    pub(crate) non_blocking_event_batching: u32,
    #[serde(default = "default_retry_on_trap")]
    pub(crate) retry_on_trap: bool,
    #[serde(default = "default_convert_core_module")]
    pub(crate) convert_core_module: bool,
    #[serde(default = "default_forward_unhandled_child_errors_in_completing_join_set_close")]
    pub(crate) forward_unhandled_child_errors_in_completing_join_set_close: bool,
    #[serde(default)]
    pub(crate) backtrace: ComponentBacktraceConfig,
}

type BacktraceFrameFilesToSourcesUnverified = hashbrown::HashMap<String, String>;
type BacktraceFrameFilesToSourcesVerified = hashbrown::HashMap<String, PathBuf>;

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentBacktraceConfig {
    #[serde(rename = "sources")]
    pub(crate) frame_files_to_sources: BacktraceFrameFilesToSourcesUnverified,
}

#[derive(Debug)]
pub(crate) struct WorkflowConfigVerified {
    pub(crate) content_digest: ContentDigest,
    pub(crate) wasm_path: PathBuf,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) retry_config: ComponentRetryConfig,
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
            match replace_file_prefix_verify_exists(&value, path_prefixes)
                .map(|value| (replace_file_prefix_no_verify(&key, path_prefixes), value))
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
    #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref(), component_id))]
    pub(crate) async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        path_prefixes: impl AsRef<PathPrefixes>,
    ) -> Result<WorkflowConfigVerified, anyhow::Error> {
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(self.common.name.clone()),
        )?;
        tracing::Span::current().record("component_id", tracing::field::display(&component_id));

        let retry_exp_backoff = Duration::from(self.retry_exp_backoff);
        if retry_exp_backoff == Duration::ZERO {
            bail!(
                "invalid `retry_exp_backoff` setting for workflow `{}` - duration must not be zero",
                self.common.name.0
            );
        }
        let (common, wasm_path) = self
            .common
            .fetch_and_verify(&wasm_cache_dir, &metadata_dir)
            .await?;
        let wasm_path = if self.convert_core_module {
            WasmComponent::convert_core_module_to_component(&wasm_path, &wasm_cache_dir)
                .await?
                .unwrap_or(wasm_path)
        } else {
            wasm_path
        };

        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            join_next_blocking_strategy: self.join_next_blocking_strategy,
            non_blocking_event_batching: self.non_blocking_event_batching,
            retry_on_trap: self.retry_on_trap,
            forward_unhandled_child_errors_in_join_set_close: self
                .forward_unhandled_child_errors_in_completing_join_set_close,
        };
        let frame_files_to_sources = verify_frame_files_to_sources(
            self.backtrace.frame_files_to_sources,
            path_prefixes.as_ref(),
        );
        Ok(WorkflowConfigVerified {
            content_digest: common.content_digest,
            wasm_path,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(component_id),
            retry_config: ComponentRetryConfig {
                max_retries: u32::MAX,
                retry_exp_backoff,
            },
            frame_files_to_sources,
        })
    }
}

#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WasmtimeAllocatorConfig {
    #[default]
    Auto,
    OnDemand,
    Pooling,
}

#[derive(Debug, Deserialize, Clone, Copy, Default)]
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
    use super::{log, Deserialize};
    use log::EnvFilter;

    #[derive(Debug, Deserialize)]
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

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurationConfig {
    Seconds(u64),
    Milliseconds(u64),
}

impl From<DurationConfig> for Duration {
    fn from(value: DurationConfig) -> Self {
        match value {
            DurationConfig::Milliseconds(millis) => Duration::from_millis(millis),
            DurationConfig::Seconds(secs) => Duration::from_secs(secs),
        }
    }
}

pub(crate) mod log {
    use super::{default_out_style, Deserialize};
    use serde_with::serde_as;
    use std::str::FromStr;

    #[derive(Debug, Deserialize, Default)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct LoggingConfig {
        #[serde(default)]
        pub(crate) file: Option<AppenderRollingFile>,
        #[serde(default)]
        pub(crate) stdout: Option<AppenderOut>,
    }

    #[derive(Debug, Deserialize, Default, Copy, Clone)]
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

    #[derive(Debug, Deserialize, Default)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LoggingStyle {
        #[default]
        Plain,
        PlainCompact,
        Json,
    }

    #[serde_as]
    #[derive(Debug, Deserialize, Default)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderCommon {
        #[serde(default)]
        pub(crate) level: EnvFilter,
        #[serde(default)]
        pub(crate) span: SpanConfig,
        #[serde(default)]
        pub(crate) target: bool,
    }

    #[derive(Debug, serde_with::DeserializeFromStr)]
    pub(crate) struct EnvFilter(pub(crate) tracing_subscriber::EnvFilter);
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

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderOut {
        pub(crate) enabled: bool,
        #[serde(flatten, default)]
        pub(crate) common: AppenderCommon,
        #[serde(default = "default_out_style")]
        pub(crate) style: LoggingStyle,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderRollingFile {
        pub(crate) enabled: bool,
        #[serde(flatten, default)]
        pub(crate) common: AppenderCommon,
        pub(crate) directory: String,
        pub(crate) prefix: String,
        pub(crate) rotation: Rotation,
        #[serde(default)]
        pub(crate) style: LoggingStyle,
    }

    #[derive(Debug, Deserialize, Clone, Copy)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Rotation {
        Minutely,
        Hourly,
        Daily,
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

#[derive(Debug, Deserialize, Clone, Copy, Default)]
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
        resolve_env_vars, verify_frame_files_to_sources, BacktraceFrameFilesToSourcesVerified,
        ComponentBacktraceConfig, ComponentCommon, ConfigName, InflightSemaphore, StdOutput,
    };
    use crate::config::{config_holder::PathPrefixes, env_var::EnvVarConfig};
    use anyhow::Context;
    use concepts::{ComponentId, ComponentType, ContentDigest, StrVariant};
    use serde::Deserialize;
    use std::{
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::Arc,
    };
    use tracing::instrument;
    use wasm_workers::envvar::EnvVar;

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct HttpServer {
        pub(crate) name: ConfigName,
        pub(crate) listening_addr: SocketAddr,
        #[serde(default)]
        pub(crate) max_inflight_requests: InflightSemaphore,
    }

    #[derive(Debug, Deserialize)]
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
        pub(crate) backtrace: ComponentBacktraceConfig,
    }

    impl WebhookComponentConfigToml {
        #[instrument(skip_all, fields(component_name = self.common.name.0.as_ref(), component_id), err)]
        pub(crate) async fn fetch_and_verify(
            self,
            wasm_cache_dir: Arc<Path>,
            metadata_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            path_prefixes: impl AsRef<PathPrefixes>,
        ) -> Result<WebhookComponentVerified, anyhow::Error> {
            let component_id = ComponentId::new(
                ComponentType::WebhookEndpoint,
                StrVariant::from(self.common.name.clone()),
            )?;
            tracing::Span::current().record("component_id", tracing::field::display(&component_id));

            let (common, wasm_path) = self
                .common
                .fetch_and_verify(&wasm_cache_dir, &metadata_dir)
                .await?;
            let frame_files_to_sources = verify_frame_files_to_sources(
                self.backtrace.frame_files_to_sources,
                path_prefixes.as_ref(),
            );

            Ok(WebhookComponentVerified {
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
                content_digest: common.content_digest,
                frame_files_to_sources,
            })
        }
    }

    #[derive(Debug, Deserialize)]
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

    #[derive(Debug, Deserialize)]
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
        pub(crate) content_digest: ContentDigest,
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

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Unlimited {
    #[default]
    Unlimited,
}

impl From<InflightSemaphore> for Option<Arc<tokio::sync::Semaphore>> {
    fn from(value: InflightSemaphore) -> Self {
        match value {
            InflightSemaphore::Unlimited(_) => None,
            InflightSemaphore::Some(permits) => Some(Arc::new(tokio::sync::Semaphore::new(
                usize::try_from(permits).expect("usize >= u32"),
            ))),
        }
    }
}

mod util {
    use crate::config::config_holder::PathPrefixes;
    use anyhow::bail;
    use std::path::PathBuf;
    use tracing::warn;

    use super::{
        CACHE_DIR_PREFIX, CONFIG_DIR_PREFIX, DATA_DIR_PREFIX, HOME_DIR_PREFIX,
        OBELISK_TOML_DIR_PREFIX,
    };

    pub(crate) fn replace_file_prefix_verify_exists(
        input_path: &str,
        path_prefixes: &PathPrefixes,
    ) -> Result<PathBuf, anyhow::Error> {
        let path = if let (Some(project_dirs), Some(base_dirs)) =
            (&path_prefixes.project_dirs, &path_prefixes.base_dirs)
        {
            if let Some(suffix) = input_path.strip_prefix(HOME_DIR_PREFIX) {
                base_dirs.home_dir().join(suffix)
            } else if let Some(suffix) = input_path.strip_prefix(DATA_DIR_PREFIX) {
                project_dirs.data_dir().join(suffix)
            } else if let Some(suffix) = input_path.strip_prefix(CACHE_DIR_PREFIX) {
                project_dirs.cache_dir().join(suffix)
            } else if let Some(suffix) = input_path.strip_prefix(CONFIG_DIR_PREFIX) {
                project_dirs.config_dir().join(suffix)
            } else if let Some(suffix) = input_path.strip_prefix(OBELISK_TOML_DIR_PREFIX) {
                path_prefixes.obelisk_toml_dir.join(suffix)
            } else {
                PathBuf::from(input_path)
            }
        } else {
            if input_path.starts_with(HOME_DIR_PREFIX)
                || input_path.starts_with(DATA_DIR_PREFIX)
                || input_path.starts_with(CACHE_DIR_PREFIX)
                || input_path.starts_with(CONFIG_DIR_PREFIX)
                || input_path.starts_with(OBELISK_TOML_DIR_PREFIX)
            {
                warn!("Not expanding prefix of `{input_path}`");
            }

            PathBuf::from(input_path)
        };
        if path.exists() {
            Ok(path)
        } else {
            bail!("file does not exist: {path:?}")
        }
    }
    pub(crate) fn replace_file_prefix_no_verify(
        input_path: &str,
        path_prefixes: &PathPrefixes,
    ) -> String {
        let path = if let (Some(project_dirs), Some(base_dirs)) =
            (&path_prefixes.project_dirs, &path_prefixes.base_dirs)
        {
            if let Some(suffix) = input_path.strip_prefix(HOME_DIR_PREFIX) {
                base_dirs.home_dir().join(suffix)
            } else if let Some(suffix) = input_path.strip_prefix(DATA_DIR_PREFIX) {
                project_dirs.data_dir().join(suffix)
            } else if let Some(suffix) = input_path.strip_prefix(CACHE_DIR_PREFIX) {
                project_dirs.cache_dir().join(suffix)
            } else if let Some(suffix) = input_path.strip_prefix(CONFIG_DIR_PREFIX) {
                project_dirs.config_dir().join(suffix)
            } else if let Some(suffix) = input_path.strip_prefix(OBELISK_TOML_DIR_PREFIX) {
                path_prefixes.obelisk_toml_dir.join(suffix)
            } else {
                PathBuf::from(input_path)
            }
        } else {
            if input_path.starts_with(HOME_DIR_PREFIX)
                || input_path.starts_with(DATA_DIR_PREFIX)
                || input_path.starts_with(CACHE_DIR_PREFIX)
                || input_path.starts_with(CONFIG_DIR_PREFIX)
                || input_path.starts_with(OBELISK_TOML_DIR_PREFIX)
            {
                warn!("Not expanding prefix of `{input_path}`");
            }

            PathBuf::from(input_path)
        };
        path.to_string_lossy().into_owned()
    }

    pub(crate) async fn replace_path_prefix_mkdir(
        dir: &str,
        path_prefixes: &PathPrefixes,
    ) -> Result<PathBuf, anyhow::Error> {
        let path = if let (Some(project_dirs), Some(base_dirs)) =
            (&path_prefixes.project_dirs, &path_prefixes.base_dirs)
        {
            if let Some(suffix) = dir.strip_prefix(HOME_DIR_PREFIX) {
                base_dirs.home_dir().join(suffix)
            } else if let Some(suffix) = dir.strip_prefix(DATA_DIR_PREFIX) {
                project_dirs.data_dir().join(suffix)
            } else if let Some(suffix) = dir.strip_prefix(CACHE_DIR_PREFIX) {
                project_dirs.cache_dir().join(suffix)
            } else if let Some(suffix) = dir.strip_prefix(CONFIG_DIR_PREFIX) {
                project_dirs.config_dir().join(suffix)
            } else if let Some(suffix) = dir.strip_prefix(OBELISK_TOML_DIR_PREFIX) {
                path_prefixes.obelisk_toml_dir.join(suffix)
            } else {
                PathBuf::from(dir)
            }
        } else {
            if dir.starts_with(HOME_DIR_PREFIX)
                || dir.starts_with(DATA_DIR_PREFIX)
                || dir.starts_with(CACHE_DIR_PREFIX)
                || dir.starts_with(CONFIG_DIR_PREFIX)
                || dir.starts_with(OBELISK_TOML_DIR_PREFIX)
            {
                warn!("Not expanding prefix of `{dir}`");
            }

            PathBuf::from(dir)
        };
        tokio::fs::create_dir_all(&path).await?;
        Ok(path)
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

const fn default_strategy() -> JoinNextBlockingStrategy {
    JoinNextBlockingStrategy::Await
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

const fn default_non_blocking_event_batching() -> u32 {
    100
}

const fn default_retry_on_trap() -> bool {
    false
}

const fn default_convert_core_module() -> bool {
    true
}

const fn default_forward_unhandled_child_errors_in_completing_join_set_close() -> bool {
    true
}

fn default_out_style() -> LoggingStyle {
    LoggingStyle::PlainCompact
}
