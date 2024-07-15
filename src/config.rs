use anyhow::{bail, Context as _};
use concepts::{ComponentId, ComponentType};
use config::{builder::AsyncState, ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};
use tokio::{sync::mpsc, task::AbortHandle};
use tracing::{debug, error, info, trace, warn};

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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Activity {
    pub(crate) name: String,
    #[serde(default = "get_true")]
    pub(crate) enabled: bool,
    pub(crate) location: ComponentLocation,
    // #[serde(rename = "content-digest")]
    pub(crate) content_digest: Option<String>,
}

impl Activity {
    pub(crate) async fn verify_content_digest(&self) -> Result<ComponentId, anyhow::Error> {
        verify_content_digest(
            &self.location,
            self.content_digest.as_deref(),
            &self.name,
            ComponentType::WasmActivity,
        )
        .await
    }
}

#[derive(Debug, Deserialize)]
pub(crate) enum ComponentLocation {
    File(PathBuf),
    Oci(Oci),
}

impl ComponentLocation {
    async fn calculate_content_digest(&self) -> Result<ComponentId, anyhow::Error> {
        match self {
            Self::File(wasm_path) => {
                let wasm_path = wasm_path
                    .canonicalize()
                    .with_context(|| format!("cannot canonicalize file `{wasm_path:?}`"))?;
                wasm_workers::component_detector::file_hash(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))
            }
            Self::Oci(_) => unimplemented!("calculate_content_digest for OCI not implemented yet"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Workflow {
    pub(crate) name: String,
    #[serde(default = "get_true")]
    pub(crate) enabled: bool,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: Option<String>,
}

async fn verify_content_digest(
    location: &ComponentLocation,
    specified_content_digest: Option<&str>,
    name: &str,
    r#type: ComponentType,
) -> Result<ComponentId, anyhow::Error> {
    let actual = location.calculate_content_digest().await?;
    if let Some(specified) = specified_content_digest {
        if *specified != actual.to_string() {
            bail!("Wrong content digest for {type} {name}, specified {specified} , actually got {actual}")
        }
    }
    Ok(actual)
}

impl Workflow {
    pub(crate) async fn verify_content_digest(&self) -> Result<ComponentId, anyhow::Error> {
        verify_content_digest(
            &self.location,
            self.content_digest.as_deref(),
            &self.name,
            ComponentType::WasmWorkflow,
        )
        .await
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Oci {
    String(String),
    Table(OciTable),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OciTable {
    pub(crate) registry: Option<String>,
    pub(crate) repository: String,
    pub(crate) tag: Option<String>,
    pub(crate) digest: Option<String>,
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
fn get_true() -> bool {
    true
}
