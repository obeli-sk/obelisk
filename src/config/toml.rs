use super::{
    store::{ConfigStore, ConfigStoreCommon},
    ComponentLocation,
};
use crate::oci;
use anyhow::{bail, Context as _};
use concepts::{storage::ComponentToggle, ComponentType, ContentDigest};
use config::{builder::AsyncState, ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};
use tokio::{sync::mpsc, task::AbortHandle};
use tracing::{debug, error, info, trace, warn};
use wasm_workers::{
    activity_worker::ActivityConfig,
    workflow_worker::{JoinNextBlockingStrategy, WorkflowConfig},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObeliskConfig {
    #[serde(default = "default_sqlite_file")]
    pub(crate) sqlite_file: String,
    #[serde(default)]
    pub(crate) watch_component_changes: bool,
    #[serde(default)]
    pub(crate) activity: Vec<Activity>,
    #[serde(default)]
    pub(crate) workflow: Vec<Workflow>,
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
    ) -> Result<(ConfigStoreCommon, PathBuf, ComponentToggle), anyhow::Error> {
        let (actual_content_digest, wasm_path) = {
            match &self.location {
                ComponentLocation::File(wasm_path) => {
                    let wasm_path = wasm_path
                        .canonicalize()
                        .with_context(|| format!("cannot canonicalize file `{wasm_path:?}`"))?;
                    let content_digest = wasm_workers::component_detector::file_hash(&wasm_path)
                        .await
                        .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
                    (content_digest, wasm_path)
                }
                ComponentLocation::Oci(image) => oci::obtan_wasm_from_oci(&image).await?,
            }
        };
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
    ) -> Result<VerifiedActivityConfig, anyhow::Error> {
        let (common, wasm_path, enabled) = self
            .common
            .verify_content_digest(ComponentType::WasmActivity)
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

fn default_strategy() -> JoinNextBlockingStrategy {
    JoinNextBlockingStrategy::Await
}

fn default_child_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Millis(100)
}

fn default_child_max_retries() -> u32 {
    5
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
    ) -> Result<VerifiedWorkflowConfig, anyhow::Error> {
        let (common, wasm_path, enabled) = self
            .common
            .verify_content_digest(ComponentType::WasmActivity)
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
}

pub(crate) struct ConfigWatcher {
    pub(crate) rx: mpsc::Receiver<ObeliskConfig>,
    pub(crate) abort_handle: AbortHandle,
}

impl ConfigHolder {
    pub(crate) fn new() -> Self {
        let mut paths = Vec::new();
        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                paths.push(PathBuf::from("/etc/obelisk/obelisk.toml"));
            }
        }
        if let Some(proj_dirs) = ProjectDirs::from("com", "obelisk", "obelisk") {
            let user_config_dir = proj_dirs.config_dir();
            // Lin: /home/alice/.config/obelisk/
            // Win: C:\Users\Alice\AppData\Roaming\obelisk\obelisk\config\
            // Mac: /Users/Alice/Library/Application Support/com.obelisk.obelisk-App/
            let user_config = user_config_dir.join("obelisk.toml");
            debug!("User config: {user_config:?}");
            paths.push(user_config);
        }
        paths.push(PathBuf::from("obelisk.toml"));
        Self { paths }
    }

    pub(crate) async fn load_config(&self) -> Result<ObeliskConfig, anyhow::Error> {
        Self::load_configs(&self.paths).await
    }

    pub(crate) async fn load_config_watch_changes(
        &self,
    ) -> Result<(ObeliskConfig, Option<ConfigWatcher>), anyhow::Error> {
        let config = Self::load_configs(&self.paths).await?;
        let watcher = if config.watch_component_changes {
            Some(self.watch_component_changes()?)
        } else {
            None
        };
        Ok((config, watcher))
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

    fn watch_component_changes(&self) -> Result<ConfigWatcher, anyhow::Error> {
        let (external_tx, rx) = mpsc::channel(1);
        let (internal_tx, mut internal_rx) = mpsc::channel(1);
        let mut debouncer = new_debouncer(
            Duration::from_secs(1),
            move |res: DebounceEventResult| match res {
                Ok(events) => {
                    trace!("Sending config file change notification - {events:?}");
                    let _ = internal_tx.blocking_send(());
                }
                Err(e) => error!("Error while watching for config changes - {:?}", e),
            },
        )?;
        for path in &self.paths {
            if let Err(err) = debouncer
                .watcher()
                .watch(path.as_ref(), RecursiveMode::NonRecursive)
            {
                warn!("Not listening on configuration changes of {path:?} - {err:?}");
            }
        }
        let paths = self.paths.clone();
        let abort_handle = tokio::spawn(async move {
            while let Some(()) = internal_rx.recv().await {
                tokio::time::sleep(Duration::from_millis(100)).await;
                // even after debouncing there are two events received - Kind::Any, Kind::AnyContinuous
                while let Ok(()) = internal_rx.try_recv() {}
                info!("Updating the configuration");
                let config = match Self::load_configs(&paths).await {
                    Ok(config) => config,
                    Err(err) => {
                        warn!("Cannot read config change - {err:?}");
                        continue;
                    }
                };
                if let Err(_) = external_tx.send(config).await {
                    info!("Shutting down the watcher task");
                    break;
                }
            }
            drop(debouncer);
        })
        .abort_handle();
        Ok(ConfigWatcher { rx, abort_handle })
    }
}

// https://github.com/serde-rs/serde/issues/368
fn default_true() -> bool {
    true
}
