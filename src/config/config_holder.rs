use super::toml::ConfigToml;
use anyhow::{Context as _, bail};
use config::{ConfigBuilder, Environment, File, FileFormat, builder::AsyncState};
use directories::{BaseDirs, ProjectDirs};
use std::path::{Path, PathBuf};
use tracing::debug;

// release: Include real file
#[cfg(not(debug_assertions))]
const EXAMPLE_TOML: &[u8] = include_bytes!("../../obelisk.toml");
#[cfg(debug_assertions)]
const EXAMPLE_TOML: &[u8] = b"not available in debug builds";

pub(crate) struct PathPrefixes {
    pub(crate) obelisk_toml_dir: PathBuf,
    pub(crate) project_dirs: Option<ProjectDirs>,
    pub(crate) base_dirs: Option<BaseDirs>,
}

pub(crate) struct ConfigHolder {
    obelisk_toml: PathBuf,
    pub(crate) path_prefixes: PathPrefixes,
}

impl ConfigHolder {
    pub(crate) async fn generate_default_config(obelisk_toml: &Path) -> Result<(), anyhow::Error> {
        if obelisk_toml.try_exists()? {
            bail!("file already exists: {obelisk_toml:?}");
        }
        tokio::fs::write(obelisk_toml, EXAMPLE_TOML).await?;
        Ok(())
    }

    fn guess_obelisk_toml(project_dirs: Option<&ProjectDirs>) -> Result<PathBuf, anyhow::Error> {
        // Guess the config file location based on the following priority:
        // 1. ./obelisk.toml
        let local = PathBuf::from("obelisk.toml");
        if local.try_exists().unwrap_or_default() {
            return Ok(local);
        }
        // 2. $CONFIG_DIR/obelisk/obelisk.toml

        if let Some(project_dirs) = &project_dirs {
            let user_config_dir = project_dirs.config_dir();
            // Lin: /home/alice/.config/obelisk/
            // Win: C:\Users\Alice\AppData\Roaming\obelisk\obelisk\config\
            // Mac: /Users/Alice/Library/Application Support/com.obelisk.obelisk-App/
            let user_config = user_config_dir.join("obelisk.toml");
            if user_config.try_exists().unwrap_or_default() {
                return Ok(user_config);
            }
        }

        // 3. /etc/obelisk/obelisk.toml
        let global_config = PathBuf::from("/etc/obelisk/obelisk.toml");
        if global_config.try_exists().unwrap_or_default() {
            return Ok(global_config);
        }
        bail!("cannot find `obelisk.toml` in any of the default locations");
    }

    pub(crate) fn new(
        project_dirs: Option<ProjectDirs>,
        base_dirs: Option<BaseDirs>,
        config: Option<PathBuf>,
    ) -> Result<Self, anyhow::Error> {
        let obelisk_toml = if let Some(config) = config {
            config
        } else {
            let found = Self::guess_obelisk_toml(project_dirs.as_ref())?;
            debug!("Using obelisk.toml: {:?}", found);
            found
        };
        Ok(Self {
            path_prefixes: PathPrefixes {
                obelisk_toml_dir: obelisk_toml
                    .canonicalize()
                    .with_context(|| {
                        format!(
                            "error while calling canonicalize on parent path of {obelisk_toml:?}"
                        )
                    })?
                    .parent()
                    .with_context(|| format!("error getting parent path of {obelisk_toml:?}"))?
                    .to_path_buf(),
                project_dirs,
                base_dirs,
            },
            obelisk_toml,
        })
    }

    pub(crate) async fn load_config(&self) -> Result<ConfigToml, anyhow::Error> {
        let mut builder = ConfigBuilder::<AsyncState>::default();
        builder = builder.add_source(
            File::from(self.obelisk_toml.as_ref())
                .required(true)
                .format(FileFormat::Toml),
        );
        let settings = builder
            .add_source(Environment::with_prefix("obelisk").separator("__"))
            .build()
            .await?;
        Ok(settings.try_deserialize()?)
    }
}
