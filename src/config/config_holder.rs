use super::toml::{DeploymentToml, DeploymentTomlValidated, ServerConfigToml};
use anyhow::{Context as _, bail};
use config::{ConfigBuilder, Environment, File, FileFormat, builder::AsyncState};
use directories::{BaseDirs, ProjectDirs};
use std::path::{Path, PathBuf};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tracing::info;
use tracing::warn;

pub(crate) const OBELISK_HELP_SERVER_TOML: &str = include_str!("../../obelisk-help-server.toml");
pub(crate) const OBELISK_HELP_DEPLOYMENT_TOML: &str =
    include_str!("../../obelisk-help-deployment.toml");

// Path prefixes
const HOME_DIR_PREFIX: &str = "~/";
pub(crate) const DATA_DIR_PREFIX: &str = "${DATA_DIR}/";
pub(crate) const CACHE_DIR_PREFIX: &str = "${CACHE_DIR}/";
const CONFIG_DIR_PREFIX: &str = "${CONFIG_DIR}/";
const SERVER_CONFIG_DIR_PREFIX: &str = "${SERVER_CONFIG_DIR}/";
const TEMP_DIR_PREFIX: &str = "${TEMP_DIR}/";

#[derive(Clone)]
pub(crate) struct PathPrefixes {
    /// Directory containing server.toml; None when no --server-config was provided.
    pub(crate) server_config_dir: Option<PathBuf>,
    pub(crate) project_dirs: Option<ProjectDirs>,
    pub(crate) base_dirs: Option<BaseDirs>,
}

impl PathPrefixes {
    pub(crate) async fn server_config_replace_path_prefix_mkdir(
        &self,
        dir: &str,
    ) -> Result<PathBuf, anyhow::Error> {
        let path =
            if let (Some(project_dirs), Some(base_dirs)) = (&self.project_dirs, &self.base_dirs) {
                if let Some(suffix) = dir.strip_prefix(HOME_DIR_PREFIX) {
                    base_dirs.home_dir().join(suffix)
                } else if let Some(suffix) = dir.strip_prefix(DATA_DIR_PREFIX) {
                    project_dirs.data_dir().join(suffix)
                } else if let Some(suffix) = dir.strip_prefix(CACHE_DIR_PREFIX) {
                    project_dirs.cache_dir().join(suffix)
                } else if let Some(suffix) = dir.strip_prefix(CONFIG_DIR_PREFIX) {
                    project_dirs.config_dir().join(suffix)
                } else if let Some(suffix) = dir.strip_prefix(SERVER_CONFIG_DIR_PREFIX) {
                    if let Some(config_dir) = &self.server_config_dir {
                        config_dir.join(suffix)
                    } else {
                        warn!("Not expanding prefix of `{dir}`: no server config file");
                        PathBuf::from(dir)
                    }
                } else if let Some(suffix) = dir.strip_prefix(TEMP_DIR_PREFIX) {
                    std::env::temp_dir().join(suffix)
                } else {
                    PathBuf::from(dir)
                }
            } else {
                if dir.starts_with(HOME_DIR_PREFIX)
                    || dir.starts_with(DATA_DIR_PREFIX)
                    || dir.starts_with(CACHE_DIR_PREFIX)
                    || dir.starts_with(CONFIG_DIR_PREFIX)
                    || dir.starts_with(SERVER_CONFIG_DIR_PREFIX)
                {
                    warn!("Not expanding prefix of `{dir}`");
                }

                PathBuf::from(dir)
            };
        tokio::fs::create_dir_all(&path)
            .await
            .with_context(|| format!("cannot create directory {path:?}"))?;
        Ok(path)
    }
}

#[derive(Clone)]
pub(crate) struct ConfigHolder {
    config_source: Option<PathBuf>,
    pub(crate) path_prefixes: PathPrefixes,
}

impl ConfigHolder {
    pub(crate) async fn generate_default_server_config(
        dst: Option<PathBuf>,
        overwrite: bool,
    ) -> Result<PathBuf, anyhow::Error> {
        let dst = dst.unwrap_or(PathBuf::from("server.toml"));
        write_config_file(&dst, OBELISK_HELP_SERVER_TOML, overwrite).await?;
        Ok(dst)
    }

    pub(crate) async fn generate_default_deployment_config(
        dst: Option<PathBuf>,
        overwrite: bool,
    ) -> Result<PathBuf, anyhow::Error> {
        let dst = dst.unwrap_or(PathBuf::from("deployment.toml"));
        write_config_file(&dst, OBELISK_HELP_DEPLOYMENT_TOML, overwrite).await?;
        Ok(dst)
    }

    /// Create a `ConfigHolder` for server configuration.
    /// If `server_config` is `None`, all fields will use built-in defaults.
    /// If `server_config` is `Some(path)`, the file must exist.
    pub(crate) fn new(
        project_dirs: Option<ProjectDirs>,
        base_dirs: Option<BaseDirs>,
        server_config: Option<PathBuf>,
    ) -> Result<Self, anyhow::Error> {
        let server_config_dir = if let Some(path) = &server_config {
            let exists = path.try_exists().unwrap_or_default();
            if !exists {
                bail!("cannot find server config file {path:?}");
            }
            let canonical_parent = canonicalize_parent(path)
                .with_context(|| format!("cannot resolve parent of {path:?}"))?;
            Some(canonical_parent)
        } else {
            None
        };

        if let Some(path) = &server_config {
            info!("Using server configuration file {:?}", path);
        }

        Ok(Self {
            config_source: server_config,
            path_prefixes: PathPrefixes {
                server_config_dir,
                project_dirs,
                base_dirs,
            },
        })
    }

    pub(crate) async fn load_config(&self) -> Result<ServerConfigToml, anyhow::Error> {
        let mut builder = ConfigBuilder::<AsyncState>::default();
        if let Some(path) = &self.config_source {
            builder = builder.add_source(
                File::from(path.as_path())
                    .required(true)
                    .format(FileFormat::Toml),
            );
        }
        builder = builder.add_source(Environment::with_prefix("obelisk").separator("__"));
        let settings = builder.build().await?;
        Ok(settings.try_deserialize()?)
    }
}

/// Load and validate a deployment TOML file.
/// `${DEPLOYMENT_DIR}/` prefixes in WASM component paths are expanded to absolute paths.
pub(crate) async fn load_deployment_toml(
    deployment_toml: PathBuf,
) -> Result<DeploymentTomlValidated, anyhow::Error> {
    let exists = deployment_toml.try_exists().unwrap_or_default();
    if !exists {
        bail!("cannot find deployment file {deployment_toml:?}");
    }
    info!("Using deployment file {:?}", deployment_toml);
    let deployment_dir = canonicalize_parent(&deployment_toml)
        .with_context(|| format!("cannot resolve parent of {deployment_toml:?}"))?;
    let builder = ConfigBuilder::<AsyncState>::default().add_source(
        File::from(deployment_toml.as_path())
            .required(true)
            .format(FileFormat::Toml),
    );
    let settings = builder.build().await?;
    let deployment: DeploymentToml = settings
        .try_deserialize()
        .with_context(|| format!("cannot parse deployment file {deployment_toml:?}"))?;
    deployment
        .validate(&deployment_dir)
        .with_context(|| format!("invalid deployment file {deployment_toml:?}"))
}

fn canonicalize_parent(path: &Path) -> Result<PathBuf, anyhow::Error> {
    Ok(path
        .canonicalize()
        .with_context(|| format!("error calling canonicalize on {path:?}"))?
        .parent()
        .with_context(|| format!("error getting parent path of {path:?}"))?
        .to_path_buf())
}

async fn write_config_file(
    dst: &Path,
    contents: &str,
    overwrite: bool,
) -> Result<(), anyhow::Error> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true) // Always allow creating new files.
        .truncate(true) // Truncate existing files.
        .create_new(!overwrite) // if true, `create` is ignored, and only new file creation is allowed, meaning overwriting is disabled.
        .open(dst)
        .await
        .with_context(|| {
            format!(
                "cannot open {dst:?} for writing{}",
                if !overwrite {
                    ", try using `--overwrite`"
                } else {
                    ""
                }
            )
        })?;
    file.write_all(contents.as_bytes())
        .await
        .with_context(|| format!("cannot write to {dst:?}"))?;
    Ok(())
}
