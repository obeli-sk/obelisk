use super::toml::ObeliskConfig;
use anyhow::bail;
use config::{builder::AsyncState, ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use std::path::PathBuf;
use tracing::debug;

#[cfg(debug_assertions)]
const EXAMPLE_TOML: &[u8] = b"not supported in debug build";
#[cfg(not(debug_assertions))]
const EXAMPLE_TOML: &[u8] = include_bytes!("../../obelisk.toml");

pub(crate) struct ConfigHolder {
    paths: Vec<PathBuf>,
    pub(crate) project_dirs: Option<ProjectDirs>,
}

impl ConfigHolder {
    pub(crate) async fn generate_default_config() -> Result<PathBuf, anyhow::Error> {
        let path = PathBuf::from("obelisk.toml");
        if path.try_exists()? {
            bail!("file already exists: {path:?}");
        }
        tokio::fs::write(&path, EXAMPLE_TOML).await?;
        Ok(path)
    }

    pub(crate) fn new(project_dirs: Option<ProjectDirs>, config: Option<PathBuf>) -> Self {
        let paths = if let Some(config) = config {
            vec![config]
        } else {
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
            paths
        };
        Self {
            paths,
            project_dirs,
        }
    }

    pub(crate) async fn load_config(&self) -> Result<ObeliskConfig, anyhow::Error> {
        let mut builder = ConfigBuilder::<AsyncState>::default();
        let mut config_exists = false;
        for path in &self.paths {
            // if no config is specified, try to merge all 3 default locations. At least one must exist.
            config_exists |= path.is_file();
            builder = builder.add_source(
                File::from(path.as_ref())
                    .required(false)
                    .format(FileFormat::Toml),
            );
        }
        if !config_exists {
            bail!("config file not found: {:?}", self.paths);
        }
        let settings = builder
            .add_source(Environment::with_prefix("obelisk").separator("__"))
            .build()
            .await?;
        Ok(settings.try_deserialize()?)
    }
}
