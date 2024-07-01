use config::{builder::AsyncState, Case, ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};
use tokio::sync::mpsc::{self};
use tracing::{debug, error, info, trace, warn};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default = "default_sqlite_file")]
    pub(crate) sqlite_file: String,
    #[serde(default)]
    pub(crate) watch_config_changes: bool,
    #[serde(default)]
    pub(crate) activity: Vec<Activity>,
    #[serde(default)]
    pub(crate) workflow: Vec<Workflow>,
}

fn default_sqlite_file() -> String {
    "obelisk.sqlite".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub(crate) struct Activity {
    pub(crate) name: String,
    pub(crate) file: String,
    #[serde(rename = "content-digest")]
    pub(crate) content_digest: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub(crate) struct Workflow {
    pub(crate) name: String,
    pub(crate) oci: Oci,
    pub(crate) enabled: Option<bool>,
    #[serde(rename = "content-digest")]
    pub(crate) content_digest: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Oci {
    String(String),
    Table(OciTable),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
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
            // Lin: /home/alice/.config/obelisk
            // Win: C:\Users\Alice\AppData\Roaming\obelisk\obelisk\config
            // Mac: /Users/Alice/Library/Application Support/com.obelisk.obelisk-App
            let user_config = user_config_dir.join("obelisk.toml");
            debug!("User config: {user_config:?}");
            paths.push(user_config);
        }
        paths.push(PathBuf::from("obelisk.toml"));
        Self { paths }
    }

    pub(crate) async fn load_config(&self) -> Result<Config, anyhow::Error> {
        Self::load_configs(&self.paths).await
    }

    pub(crate) async fn load_configs(paths: &[PathBuf]) -> Result<Config, anyhow::Error> {
        let mut builder = ConfigBuilder::<AsyncState>::default();
        for path in paths {
            builder = builder.add_source(
                File::from(path.as_ref())
                    .required(false)
                    .format(FileFormat::Toml),
            );
        }
        let settings = builder
            .add_source(Environment::with_prefix("obelisk").convert_case(Case::Kebab))
            .build()
            .await?;
        Ok(settings.try_deserialize()?)
    }

    /// Create new task with file watcher. Closing the receiver will shut down the task.
    pub(crate) async fn watch(
        self,
    ) -> Result<mpsc::Receiver<Result<Config, anyhow::Error>>, anyhow::Error> {
        let (external_tx, external_rx) = mpsc::channel(1);
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
        tokio::spawn(async move {
            while let Some(()) = internal_rx.recv().await {
                tokio::time::sleep(Duration::from_millis(100)).await;
                // even after debouncing there are two events received - Kind::Any, Kind::AnyContinuous
                while let Ok(()) = internal_rx.try_recv() {}
                info!("Updating the configuration");
                let config_res = Self::load_configs(&self.paths).await;
                if let Err(_) = external_tx.send(config_res).await {
                    info!("Shutting down the watcher task");
                    break;
                }
            }
            drop(debouncer);
        });
        Ok(external_rx)
    }
}
