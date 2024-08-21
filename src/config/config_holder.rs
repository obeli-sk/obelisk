use super::toml::ObeliskConfig;
use config::{builder::AsyncState, ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use std::path::PathBuf;
use tracing::debug;

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
