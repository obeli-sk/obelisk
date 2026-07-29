use super::env_var::interpolate_path_template;
use super::secret_registry::SecretRegistry;
use super::toml::{DeploymentToml, ServerConfigToml};
use crate::config::toml::DeploymentResolved;
use crate::config::toml::DeploymentTomlValidated;
use anyhow::{Context as _, bail};
use config::{Config, ConfigBuilder, Environment, File, FileFormat, builder::AsyncState};
use directories::{BaseDirs, ProjectDirs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tracing::info;

pub(crate) const OBELISK_HELP_SERVER_TOML: &str = include_str!("../../obelisk-help-server.toml");
pub(crate) const OBELISK_HELP_DEPLOYMENT_TOML: &str =
    include_str!("../../obelisk-help-deployment.toml");

/// Leading `~/`, expanded to the user's home directory. Not `${}` interpolation syntax.
const HOME_DIR_PREFIX: &str = "~/";
// Default-path building blocks referencing the synthetic `${DATA_DIR}` / `${CACHE_DIR}` variables.
pub(crate) const DATA_DIR_PREFIX: &str = "${DATA_DIR}/";
pub(crate) const CACHE_DIR_PREFIX: &str = "${CACHE_DIR}/";

#[derive(Clone)]
pub(crate) struct PathPrefixes {
    /// Directory containing server.toml; None when no --server-config was provided.
    pub(crate) server_config_dir: Option<PathBuf>,
    pub(crate) project_dirs: Option<ProjectDirs>,
    pub(crate) base_dirs: Option<BaseDirs>,
    secret_registry: Arc<SecretRegistry>,
}

impl PathPrefixes {
    pub(crate) async fn server_config_replace_path_prefix_mkdir(
        &self,
        dir: &str,
    ) -> Result<PathBuf, anyhow::Error> {
        let path = PathBuf::from(self.interpolate_path(dir)?);
        tokio::fs::create_dir_all(&path)
            .await
            .with_context(|| format!("cannot create directory {path:?}"))?;
        Ok(path)
    }

    /// Resolve a server-config path field: a leading `~/` becomes the home directory, then
    /// synthetic path variables (`${DATA_DIR}` etc.) and process environment variables are
    /// interpolated, with synthetic names taking precedence.
    fn interpolate_path(&self, dir: &str) -> Result<String, anyhow::Error> {
        let dir = if let Some(suffix) = dir.strip_prefix(HOME_DIR_PREFIX) {
            let home = self
                .base_dirs
                .as_ref()
                .context("cannot expand `~/`: home directory is unavailable")?
                .home_dir();
            home.join(suffix).to_string_lossy().into_owned()
        } else {
            dir.to_owned()
        };
        interpolate_path_template(&dir, &self.synthetic_dirs(), self.secret_registry.as_ref())
    }

    /// Synthetic path variables and their values, or `None` when unavailable in this context.
    fn synthetic_dirs(&self) -> Vec<(&'static str, Option<String>)> {
        let to_string = |p: &Path| p.to_string_lossy().into_owned();
        let project_dirs = self.project_dirs.as_ref();
        vec![
            ("DATA_DIR", project_dirs.map(|p| to_string(p.data_dir()))),
            ("CACHE_DIR", project_dirs.map(|p| to_string(p.cache_dir()))),
            (
                "CONFIG_DIR",
                project_dirs.map(|p| to_string(p.config_dir())),
            ),
            (
                "SERVER_CONFIG_DIR",
                self.server_config_dir.as_deref().map(to_string),
            ),
            ("TEMP_DIR", Some(to_string(&std::env::temp_dir()))),
        ]
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
                secret_registry: Arc::new(SecretRegistry::empty()),
            },
        })
    }

    pub(crate) fn set_secret_registry(&mut self, secret_registry: Arc<SecretRegistry>) {
        self.path_prefixes.secret_registry = secret_registry;
    }

    pub(crate) async fn load_config(&self) -> Result<ServerConfigToml, anyhow::Error> {
        self.load_config_sync()
    }

    /// Load the complete server configuration before the async runtime starts.
    ///
    /// This uses the same file and `OBELISK__...` environment sources as
    /// [`Self::load_config`]. Server startup uses this synchronous variant so the
    /// exact config value that defines `[secrets]` is passed into the runtime after
    /// its env-backed sources have been wiped.
    pub(crate) fn load_config_sync(&self) -> Result<ServerConfigToml, anyhow::Error> {
        let mut builder = Config::builder();
        if let Some(path) = &self.config_source {
            builder = builder.add_source(
                File::from(path.as_path())
                    .required(true)
                    .format(FileFormat::Toml),
            );
        }
        builder = builder.add_source(Environment::with_prefix("obelisk").separator("__"));
        let settings = builder.build()?;
        Ok(settings.try_deserialize()?)
    }
}

pub(crate) async fn load_deployment_validated(
    deployment_toml: &Path,
) -> Result<DeploymentTomlValidated, anyhow::Error> {
    let exists = deployment_toml.try_exists().unwrap_or_default();
    if !exists {
        bail!("cannot find deployment file {deployment_toml:?}");
    }
    info!("Using deployment file {:?}", deployment_toml);
    let deployment_dir = canonicalize_parent(deployment_toml)
        .with_context(|| format!("cannot resolve parent of {deployment_toml:?}"))?;
    let builder = ConfigBuilder::<AsyncState>::default().add_source(
        File::from(deployment_toml)
            .required(true)
            .format(FileFormat::Toml),
    );
    let settings = builder.build().await?;
    let deployment: DeploymentToml = settings
        .try_deserialize()
        .with_context(|| format!("cannot parse deployment file {deployment_toml:?}"))?;
    deployment
        .validate(&deployment_dir)
        .with_context(|| format!("cannot validate {deployment_toml:?}"))
}

pub(crate) async fn load_deployment_canonical(
    deployment_toml: &Path,
) -> Result<DeploymentResolved, anyhow::Error> {
    load_deployment_validated(deployment_toml)
        .await?
        .canonicalize()
        .await
        .with_context(|| format!("cannot canonicalize {deployment_toml:?}"))
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
