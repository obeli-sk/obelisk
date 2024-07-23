use super::{
    store::{ConfigStore, ConfigStoreCommon},
    ComponentLocation,
};
use crate::oci;
use anyhow::{bail, Context as _};
use concepts::{storage::ComponentToggle, ComponentType, ContentDigest};
use config::{builder::AsyncState, ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tracing::debug;
use wasm_workers::{
    activity_worker::ActivityConfig,
    workflow_worker::{JoinNextBlockingStrategy, WorkflowConfig},
};

const DATA_DIR_PREFIX: &str = "${DATA_DIR}/";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObeliskConfig {
    #[serde(default = "default_sqlite_file")]
    sqlite_file: String,
    #[serde(default)]
    pub(crate) activity: Vec<Activity>,
    #[serde(default)]
    pub(crate) workflow: Vec<Workflow>,
}

impl ObeliskConfig {
    pub(crate) async fn get_sqlite_file(
        &self,
        project_dirs: Option<&ProjectDirs>,
    ) -> Result<PathBuf, anyhow::Error> {
        let sqlite_path = match (self.sqlite_file.strip_prefix(DATA_DIR_PREFIX), project_dirs) {
            (None, _) => PathBuf::from(&self.sqlite_file),
            (Some(suffix), Some(project_dirs)) => project_dirs.data_dir().join(suffix),
            (Some(_), None) => bail!("cannot use {DATA_DIR_PREFIX} on this platform, please specify canonical path to `sqlite_file`"),
        };
        if let Some(parent) = sqlite_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(sqlite_path)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct OciConfig {
    #[serde(default = "default_oci_config_wasm_directory")]
    wasm_directory: String,
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
    ) -> Result<(ConfigStoreCommon, PathBuf, ComponentToggle), anyhow::Error> {
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
            exec: super::store::ExecConfig {
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
                ComponentToggle::Enabled
            } else {
                ComponentToggle::Disabled
            },
        ))
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecConfig {
    batch_size: u32,
    lock_expiry: DurationConfig,
    tick_sleep: DurationConfig,
}

impl Default for ExecConfig {
    fn default() -> Self {
        Self {
            batch_size: 5,
            lock_expiry: DurationConfig::Secs(1),
            tick_sleep: DurationConfig::Millis(200),
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
    pub(crate) enabled: ComponentToggle,
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
            enabled,
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
    #[serde(default = "default_true")]
    pub(crate) non_blocking_event_batching: bool,
}

#[derive(Debug)]
pub(crate) struct VerifiedWorkflowConfig {
    pub(crate) config_store: ConfigStore,
    pub(crate) wasm_path: PathBuf,
    pub(crate) enabled: ComponentToggle,
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
            non_blocking_event_batching: self.non_blocking_event_batching.into(),
        };
        let exec_config = exec_config.into_exec_exec_config(config_id);
        Ok(VerifiedWorkflowConfig {
            config_store,
            wasm_path,
            enabled,
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

// https://github.com/serde-rs/serde/issues/368
fn default_true() -> bool {
    true
}

fn default_sqlite_file() -> String {
    "obelisk.sqlite".to_string()
}

fn default_max_retries() -> u32 {
    5
}

fn default_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Millis(100)
}

fn default_strategy() -> JoinNextBlockingStrategy {
    JoinNextBlockingStrategy::Await
}

fn default_child_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Millis(100)
}

fn default_child_max_retries() -> u32 {
    5
}

fn default_oci_config_wasm_directory() -> String {
    format!("${{{CACHE_DIR_PREFIX}}}/wasm")
}
