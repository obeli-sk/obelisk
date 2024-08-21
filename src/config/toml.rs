use super::{
    ComponentLocation, {ConfigStore, ConfigStoreCommon},
};
use anyhow::bail;
use concepts::{ComponentType, ContentDigest};
use config::{builder::AsyncState, ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use tracing::debug;
use wasm_workers::{
    activity_worker::ActivityConfig,
    workflow_worker::{JoinNextBlockingStrategy, WorkflowConfig},
};

const DATA_DIR_PREFIX: &str = "${DATA_DIR}/";
const CACHE_DIR_PREFIX: &str = "${CACHE_DIR}/";
const CONFIG_DIR_PREFIX: &str = "${CONFIG_DIR}/";
const DEFAULT_SQLITE_FILE_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}obelisk.sqlite", DATA_DIR_PREFIX);
const DEFAULT_SQLITE_FILE: &str = "obelisk.sqlite";
const DEFAULT_OCI_CONFIG_WASM_DIRECTORY_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}wasm", CACHE_DIR_PREFIX);
const DEFAULT_OCI_CONFIG_WASM_DIRECTORY: &str = "cache/wasm";
const DEFAULT_CODEGEN_CACHE_DIRECTORY_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}codegen", CACHE_DIR_PREFIX);
const DEFAULT_CODEGEN_CACHE_DIRECTORY: &str = "cache/codegen";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObeliskConfig {
    #[serde(default)]
    sqlite_file: Option<String>,
    #[serde(default = "default_api_listening_addr")]
    pub(crate) api_listening_addr: SocketAddr,
    #[serde(default)]
    pub(crate) oci: OciConfig,
    #[serde(default)]
    pub(crate) codegen_cache: CodegenCache,
    #[serde(default)]
    pub(crate) activity: Vec<Activity>,
    #[serde(default)]
    pub(crate) workflow: Vec<Workflow>,
    #[serde(default)]
    pub(crate) wasmtime_pooling_config: WasmtimePoolingConfig,
    #[cfg(feature = "otlp")]
    #[serde(default)]
    pub(crate) otlp_config: otlp::OtlpConfig,
}

async fn replace_path_prefix_mkdir(
    path: &str,
    project_dirs: Option<&ProjectDirs>,
    file_or_folder: FileOrFolder,
) -> Result<PathBuf, anyhow::Error> {
    let path = match project_dirs {
        Some(project_dirs) => {
            if let Some(suffix) = path.strip_prefix(DATA_DIR_PREFIX) {
                project_dirs.data_dir().join(suffix)
            } else if let Some(suffix) = path.strip_prefix(CACHE_DIR_PREFIX) {
                project_dirs.cache_dir().join(suffix)
            } else if let Some(suffix) = path.strip_prefix(CONFIG_DIR_PREFIX) {
                project_dirs.config_dir().join(suffix)
            } else {
                PathBuf::from(path)
            }
        }
        None => PathBuf::from(path),
    };
    if file_or_folder == FileOrFolder::Folder {
        tokio::fs::create_dir_all(&path).await?;
    } else if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(path)
}

#[derive(PartialEq, Eq)]
enum FileOrFolder {
    File,
    Folder,
}

impl ObeliskConfig {
    pub(crate) async fn get_sqlite_file(
        &self,
        project_dirs: Option<&ProjectDirs>,
    ) -> Result<PathBuf, anyhow::Error> {
        let sqlite_file = self.sqlite_file.as_deref().unwrap_or_else(|| {
            if project_dirs.is_some() {
                DEFAULT_SQLITE_FILE_IF_PROJECT_DIRS
            } else {
                DEFAULT_SQLITE_FILE
            }
        });
        replace_path_prefix_mkdir(sqlite_file, project_dirs, FileOrFolder::File).await
    }
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct OciConfig {
    #[serde(default)]
    wasm_directory: Option<String>,
}

impl OciConfig {
    pub(crate) async fn get_wasm_directory(
        &self,
        project_dirs: Option<&ProjectDirs>,
    ) -> Result<PathBuf, anyhow::Error> {
        let wasm_directory = self.wasm_directory.as_deref().unwrap_or_else(|| {
            if project_dirs.is_some() {
                DEFAULT_OCI_CONFIG_WASM_DIRECTORY_IF_PROJECT_DIRS
            } else {
                DEFAULT_OCI_CONFIG_WASM_DIRECTORY
            }
        });
        replace_path_prefix_mkdir(wasm_directory, project_dirs, FileOrFolder::Folder).await
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CodegenCache {
    #[serde(default = "default_true")]
    enabled: bool,
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
    pub(crate) async fn get_directory_if_enabled(
        &self,
        project_dirs: Option<&ProjectDirs>,
    ) -> Result<Option<PathBuf>, anyhow::Error> {
        if !self.enabled {
            return Ok(None);
        }
        let directory = self.directory.as_deref().unwrap_or_else(|| {
            if project_dirs.is_some() {
                DEFAULT_CODEGEN_CACHE_DIRECTORY_IF_PROJECT_DIRS
            } else {
                DEFAULT_CODEGEN_CACHE_DIRECTORY
            }
        });
        Ok(Some(
            replace_path_prefix_mkdir(directory, project_dirs, FileOrFolder::Folder).await?,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
enum ComponentEnabled {
    Enabled,
    Disabled,
}
impl From<ComponentEnabled> for bool {
    fn from(value: ComponentEnabled) -> Self {
        matches!(value, ComponentEnabled::Enabled)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentCommon {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    pub(crate) name: String,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: Option<ContentDigest>,
    #[serde(default)]
    pub(crate) exec: ExecConfig,
    #[serde(default = "default_max_retries")]
    pub(crate) default_max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) default_retry_exp_backoff: DurationConfig,
}

impl ComponentCommon {
    /// Fetch wasm file, calculate its content digest, optionally compare with the expected `content_digest`.
    ///
    /// Read wasm file either from local fs or pull from an OCI registry and cache it if needed.
    /// If the `content_digest` is set, verify that it matches the calculated digest.
    /// Otherwise backfill the `content_digest`.
    async fn verify_content_digest(
        self,
        r#type: ComponentType,
        wasm_cache_dir: impl AsRef<Path>,
    ) -> Result<(ConfigStoreCommon, PathBuf, ComponentEnabled), anyhow::Error> {
        let (actual_content_digest, wasm_path) = self.location.obtain_wasm(wasm_cache_dir).await?;
        if let Some(specified) = &self.content_digest {
            if *specified != actual_content_digest {
                bail!("Wrong content digest for {type} {name}, specified {specified} , actually got {actual_content_digest}",
                name = self.name)
            }
        }
        let verified = ConfigStoreCommon {
            name: self.name,
            location: self.location,
            content_digest: actual_content_digest,
            exec: super::ExecConfig {
                batch_size: self.exec.batch_size,
                lock_expiry: self.exec.lock_expiry.into(),
                tick_sleep: self.exec.tick_sleep.into(),
            },
            default_max_retries: self.default_max_retries,
            default_retry_exp_backoff: self.default_retry_exp_backoff.into(),
        };
        Ok((
            verified,
            wasm_path,
            if self.enabled {
                ComponentEnabled::Enabled
            } else {
                ComponentEnabled::Disabled
            },
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecConfig {
    #[serde(default = "default_batch_size")]
    batch_size: u32,
    #[serde(default = "default_lock_expiry")]
    lock_expiry: DurationConfig,
    #[serde(default = "default_tick_sleep")]
    tick_sleep: DurationConfig,
}

impl Default for ExecConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            lock_expiry: default_lock_expiry(),
            tick_sleep: default_tick_sleep(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Activity {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    #[serde(default = "default_true")]
    pub(crate) recycle_instances: bool,
}

#[derive(Debug)]
pub(crate) struct VerifiedActivityConfig {
    pub(crate) config_store: ConfigStore,
    pub(crate) wasm_path: PathBuf,
    pub(crate) enabled: bool,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
}

impl Activity {
    pub(crate) async fn verify_content_digest(
        self,
        wasm_cache_dir: impl AsRef<Path>,
    ) -> Result<VerifiedActivityConfig, anyhow::Error> {
        let (common, wasm_path, enabled) = self
            .common
            .verify_content_digest(ComponentType::WasmActivity, wasm_cache_dir)
            .await?;
        let exec_config = common.exec.clone();
        let config_store = ConfigStore::WasmActivityV1 {
            common,
            recycle_instances: self.recycle_instances,
        };
        let config_id = config_store.as_hash();
        let exec_config = exec_config.into_exec_exec_config(config_id.clone());
        let activity_config = ActivityConfig {
            config_id: config_id.clone(),
            recycle_instances: self.recycle_instances.into(),
        };
        Ok(VerifiedActivityConfig {
            config_store,
            wasm_path,
            enabled: enabled.into(),
            activity_config,
            exec_config,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Workflow {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    #[serde(default = "default_strategy")]
    pub(crate) join_next_blocking_strategy: JoinNextBlockingStrategy,
    #[serde(default = "default_child_retry_exp_backoff")]
    pub(crate) child_retry_exp_backoff: DurationConfig,
    #[serde(default = "default_child_max_retries")]
    pub(crate) child_max_retries: u32,
    #[serde(default = "default_non_blocking_event_batching")]
    pub(crate) non_blocking_event_batching: u32,
}

#[derive(Debug)]
pub(crate) struct VerifiedWorkflowConfig {
    pub(crate) config_store: ConfigStore,
    pub(crate) wasm_path: PathBuf,
    pub(crate) enabled: bool,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
}

impl Workflow {
    pub(crate) async fn verify_content_digest(
        self,
        wasm_cache_dir: impl AsRef<Path>,
    ) -> Result<VerifiedWorkflowConfig, anyhow::Error> {
        let (common, wasm_path, enabled) = self
            .common
            .verify_content_digest(ComponentType::WasmActivity, wasm_cache_dir)
            .await?;
        let exec_config = common.exec.clone();
        let config_store = ConfigStore::WasmWorkflowV1 {
            common,
            join_next_blocking_strategy: self.join_next_blocking_strategy,
            child_retry_exp_backoff: self.child_retry_exp_backoff.clone().into(),
            child_max_retries: self.child_max_retries,
            non_blocking_event_batching: self.non_blocking_event_batching,
        };
        let config_id = config_store.as_hash();
        let workflow_config = WorkflowConfig {
            config_id: config_id.clone(),
            join_next_blocking_strategy: self.join_next_blocking_strategy,
            child_retry_exp_backoff: self.child_retry_exp_backoff.into(),
            child_max_retries: self.child_max_retries,
            non_blocking_event_batching: self.non_blocking_event_batching,
        };
        let exec_config = exec_config.into_exec_exec_config(config_id);
        Ok(VerifiedWorkflowConfig {
            config_store,
            wasm_path,
            enabled: enabled.into(),
            workflow_config,
            exec_config,
        })
    }
}

pub(crate) struct ConfigHolder {
    paths: Vec<PathBuf>,
    pub(crate) project_dirs: Option<ProjectDirs>,
}

impl ConfigHolder {
    pub(crate) fn new(project_dirs: Option<ProjectDirs>) -> Self {
        let mut paths = Vec::new();
        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                let global_config = PathBuf::from("/etc/obelisk/obelisk.toml");
                debug!("Global config: {global_config:?} exists? {:?}", global_config.try_exists());
                paths.push(global_config);
            }
        }
        if let Some(project_dirs) = &project_dirs {
            let user_config_dir = project_dirs.config_dir();
            // Lin: /home/alice/.config/obelisk/
            // Win: C:\Users\Alice\AppData\Roaming\obelisk\obelisk\config\
            // Mac: /Users/Alice/Library/Application Support/com.obelisk.obelisk-App/
            let user_config = user_config_dir.join("obelisk.toml");
            debug!(
                "User config: {user_config:?} exists? {:?}",
                user_config.try_exists()
            );
            paths.push(user_config);
        }
        let workdir_config = PathBuf::from("obelisk.toml");
        debug!(
            "Workdir config: {workdir_config:?} exists? {:?}",
            workdir_config.try_exists()
        );
        paths.push(workdir_config);
        Self {
            paths,
            project_dirs,
        }
    }

    pub(crate) async fn load_config(&self) -> Result<ObeliskConfig, anyhow::Error> {
        Self::load_configs(&self.paths).await
    }

    async fn load_configs(paths: &[PathBuf]) -> Result<ObeliskConfig, anyhow::Error> {
        let mut builder = ConfigBuilder::<AsyncState>::default();
        for path in paths {
            builder = builder.add_source(
                File::from(path.as_ref())
                    .required(false)
                    .format(FileFormat::Toml),
            );
        }
        let settings = builder
            .add_source(Environment::with_prefix("obelisk"))
            .build()
            .await?;
        Ok(settings.try_deserialize()?)
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WasmtimePoolingConfig {
    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in linear memories.
    #[serde(default)]
    pub(crate) pooling_memory_keep_resident: Option<usize>,

    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in tables.
    #[serde(default)]
    pub(crate) pooling_table_keep_resident: Option<usize>,

    /// Enable memory protection keys for the pooling allocator; this can
    /// optimize the size of memory slots.
    #[serde(default)]
    pub(crate) memory_protection_keys: Option<bool>,

    /// The maximum number of WebAssembly instances which can be created
    /// with the pooling allocator.
    #[serde(default)]
    pub(crate) pooling_total_core_instances: Option<u32>,

    /// The maximum number of WebAssembly components which can be created
    /// with the pooling allocator.
    #[serde(default)]
    pub(crate) pooling_total_component_instances: Option<u32>,

    /// The maximum number of WebAssembly memories which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pub(crate) pooling_total_memories: Option<u32>,

    /// The maximum number of WebAssembly tables which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pub(crate) pooling_total_tables: Option<u32>,

    /// The maximum number of WebAssembly stacks which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pub(crate) pooling_total_stacks: Option<u32>,

    /// The maximum runtime size of each linear memory in the pooling
    /// allocator, in bytes.
    #[serde(default)]
    pub(crate) pooling_max_memory_size: Option<usize>,
}

impl From<WasmtimePoolingConfig> for wasm_workers::engines::PoolingOptions {
    fn from(value: WasmtimePoolingConfig) -> wasm_workers::engines::PoolingOptions {
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
    use super::*;

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct OtlpConfig {
        #[serde(default = "default_service_name")]
        pub(crate) service_name: String,
        #[serde(default = "default_otlp_endpoint")]
        pub(crate) otlp_endpoint: String,
    }

    impl Default for OtlpConfig {
        fn default() -> Self {
            Self {
                service_name: default_service_name(),
                otlp_endpoint: default_otlp_endpoint(),
            }
        }
    }

    fn default_service_name() -> String {
        "obelisk-server".to_string()
    }

    fn default_otlp_endpoint() -> String {
        // Endpoints per protocol https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md
        "http://localhost:4317".to_string()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurationConfig {
    Secs(u64),
    Millis(u64),
}

impl From<DurationConfig> for Duration {
    fn from(value: DurationConfig) -> Self {
        match value {
            DurationConfig::Millis(millis) => Duration::from_millis(millis),
            DurationConfig::Secs(secs) => Duration::from_secs(secs),
        }
    }
}

// https://github.com/serde-rs/serde/issues/368
const fn default_true() -> bool {
    true
}

const fn default_max_retries() -> u32 {
    5
}

const fn default_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Millis(100)
}

const fn default_strategy() -> JoinNextBlockingStrategy {
    JoinNextBlockingStrategy::Await
}

const fn default_child_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Millis(100)
}

const fn default_child_max_retries() -> u32 {
    5
}

const fn default_batch_size() -> u32 {
    5
}

const fn default_lock_expiry() -> DurationConfig {
    DurationConfig::Secs(1)
}

const fn default_tick_sleep() -> DurationConfig {
    DurationConfig::Millis(200)
}

const fn default_non_blocking_event_batching() -> u32 {
    100
}

fn default_api_listening_addr() -> SocketAddr {
    "127.0.0.1:5005".parse().unwrap()
}
