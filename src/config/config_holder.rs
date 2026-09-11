use super::env_var::{
    StartupEnvVars, interpolate_path_template, interpolate_startup_path_template,
};
use super::secret_registry::SecretRegistry;
use super::server::ServerConfigToml;
use anyhow::{Context as _, bail};
use config::{Config, Environment, File, FileFormat};
use directories::{BaseDirs, ProjectDirs};
use std::path::{Path, PathBuf};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tracing::info;

pub(crate) const OBELISK_HELP_SERVER_TOML: &str = include_str!("../../server-help.toml");
pub(crate) const OBELISK_TRUSTED_SERVER_TOML: &str =
    include_str!("../../server-trusted-template.toml");
pub(crate) const OBELISK_HELP_DEPLOYMENT_TOML: &str = include_str!("../../deployment-help.toml");

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
}

impl PathPrefixes {
    pub(crate) fn resolve_server_path(
        &self,
        dir: &str,
        env_vars: &StartupEnvVars,
    ) -> Result<String, anyhow::Error> {
        let dir = self.expand_home(dir)?;
        interpolate_startup_path_template(&dir, &self.synthetic_dirs(), env_vars)
    }

    pub(crate) async fn server_config_replace_path_prefix_mkdir(
        &self,
        dir: &str,
        secret_registry: &SecretRegistry,
    ) -> Result<PathBuf, anyhow::Error> {
        let path = PathBuf::from(self.interpolate_path(dir, secret_registry)?);
        tokio::fs::create_dir_all(&path)
            .await
            .with_context(|| format!("cannot create directory {path:?}"))?;
        Ok(path)
    }

    /// Resolve a server-config path field: a leading `~/` becomes the home directory, then
    /// synthetic path variables (`${DATA_DIR}` etc.) and process environment variables are
    /// interpolated, with synthetic names taking precedence.
    fn interpolate_path(
        &self,
        dir: &str,
        secret_registry: &SecretRegistry,
    ) -> Result<String, anyhow::Error> {
        let dir = self.expand_home(dir)?;
        interpolate_path_template(&dir, &self.synthetic_dirs(), secret_registry)
    }

    fn expand_home(&self, dir: &str) -> Result<String, anyhow::Error> {
        Ok(if let Some(suffix) = dir.strip_prefix(HOME_DIR_PREFIX) {
            let home = self
                .base_dirs
                .as_ref()
                .context("cannot expand `~/`: home directory is unavailable")?
                .home_dir();
            home.join(suffix).to_string_lossy().into_owned()
        } else {
            dir.to_owned()
        })
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
    pub(crate) config_source: Option<PathBuf>,
    pub(crate) path_prefixes: PathPrefixes,
}

impl ConfigHolder {
    pub(crate) async fn generate_server_config(
        dst: PathBuf,
        trusted: bool,
        overwrite: bool,
    ) -> Result<PathBuf, anyhow::Error> {
        write_config_file(&dst, server_config_template(trusted), overwrite).await?;
        Ok(dst)
    }

    pub(crate) async fn generate_default_deployment_config(
        dst: PathBuf,
        overwrite: bool,
    ) -> Result<PathBuf, anyhow::Error> {
        write_config_file(&dst, OBELISK_HELP_DEPLOYMENT_TOML, overwrite).await?;
        Ok(dst)
    }

    /// Create a `ConfigHolder` for server configuration.
    /// If `config_source` is `None`, all fields will use built-in defaults.
    pub(crate) fn new(
        project_dirs: Option<ProjectDirs>,
        base_dirs: Option<BaseDirs>,
        config_source: Option<PathBuf>,
    ) -> Result<Self, anyhow::Error> {
        let server_config_dir = if let Some(path) = &config_source {
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

        if let Some(path) = &config_source {
            info!("Using server configuration file {:?}", path);
        }

        Ok(Self {
            config_source,
            path_prefixes: PathPrefixes {
                server_config_dir,
                project_dirs,
                base_dirs,
            },
        })
    }

    /// Load the complete server configuration before the async runtime starts.
    ///
    /// This uses the same file and `OBELISK__...` environment sources as
    /// [`Self::load_config`]. Server startup uses this synchronous variant so the
    /// exact config value that defines `[secrets]` is passed into the runtime after
    /// its env-backed sources have been wiped.
    pub(crate) fn load_config(&self) -> Result<ServerConfigToml, anyhow::Error> {
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
        let mut config: ServerConfigToml = settings.try_deserialize()?;
        config.source_path.clone_from(&self.config_source);
        Ok(config)
    }
}

pub(crate) fn server_config_template(trusted: bool) -> &'static str {
    if trusted {
        OBELISK_TRUSTED_SERVER_TOML
    } else {
        OBELISK_HELP_SERVER_TOML
    }
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

#[cfg(test)]
mod tests {
    use super::{OBELISK_TRUSTED_SERVER_TOML, ServerConfigToml};
    use crate::config::deployment::MethodsInput;
    use crate::config::server::AllowExecActivities;

    #[test]
    fn trusted_server_config_allows_exec_and_outbound_http_without_secrets() {
        let config: ServerConfigToml = toml::from_str(OBELISK_TRUSTED_SERVER_TOML).unwrap();

        assert_eq!(config.allow_exec_activities, AllowExecActivities::AllowAny);
        assert!(config.secrets.is_empty());
        let [host] = config.outbound_http.allowed_hosts.as_slice() else {
            panic!("expected one outbound HTTP host");
        };
        assert_eq!(host.pattern, "*://*:*");
        assert!(matches!(host.methods, Some(MethodsInput::Star(_))));
        assert!(host.secrets.is_empty());
        assert!(host.replace_in.is_empty());
    }
}
