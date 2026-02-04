use super::toml::ConfigToml;
use anyhow::{Context as _, bail};
use config::{ConfigBuilder, Environment, File, FileFormat, builder::AsyncState};
use directories::{BaseDirs, ProjectDirs};
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tracing::debug;
use tracing::info;
use tracing::warn;

/// Represents either a local file path or a remote URL for the config.
#[derive(Debug, Clone)]
pub(crate) enum ConfigSource {
    LocalFile(PathBuf),
    Url(String),
}

impl std::str::FromStr for ConfigSource {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("http://") || s.starts_with("https://") {
            Ok(ConfigSource::Url(s.to_string()))
        } else {
            Ok(ConfigSource::LocalFile(PathBuf::from(s)))
        }
    }
}

pub(crate) const OBELISK_HELP_TOML: &str = include_str!("../../obelisk-help.toml");
const OBELISK_GEN_TOML: &str = include_str!("../../obelisk-generate.toml");

// Path prefixes
const HOME_DIR_PREFIX: &str = "~/";
pub(crate) const DATA_DIR_PREFIX: &str = "${DATA_DIR}/";
pub(crate) const CACHE_DIR_PREFIX: &str = "${CACHE_DIR}/";
const CONFIG_DIR_PREFIX: &str = "${CONFIG_DIR}/";
const OBELISK_TOML_DIR_PREFIX: &str = "${OBELISK_TOML_DIR}/";
const TEMP_DIR_PREFIX: &str = "${TEMP_DIR}/";

pub(crate) struct PathPrefixes {
    pub(crate) obelisk_toml_dir: PathBuf,
    pub(crate) project_dirs: Option<ProjectDirs>,
    pub(crate) base_dirs: Option<BaseDirs>,
}

impl PathPrefixes {
    pub(crate) fn replace_file_prefix_verify_exists(
        &self,
        input_path: &str,
    ) -> Result<PathBuf, anyhow::Error> {
        let path =
            if let (Some(project_dirs), Some(base_dirs)) = (&self.project_dirs, &self.base_dirs) {
                if let Some(suffix) = input_path.strip_prefix(HOME_DIR_PREFIX) {
                    base_dirs.home_dir().join(suffix)
                } else if let Some(suffix) = input_path.strip_prefix(DATA_DIR_PREFIX) {
                    project_dirs.data_dir().join(suffix)
                } else if let Some(suffix) = input_path.strip_prefix(CACHE_DIR_PREFIX) {
                    project_dirs.cache_dir().join(suffix)
                } else if let Some(suffix) = input_path.strip_prefix(CONFIG_DIR_PREFIX) {
                    project_dirs.config_dir().join(suffix)
                } else if let Some(suffix) = input_path.strip_prefix(OBELISK_TOML_DIR_PREFIX) {
                    self.obelisk_toml_dir.join(suffix)
                } else {
                    PathBuf::from(input_path)
                }
            } else {
                if input_path.starts_with(HOME_DIR_PREFIX)
                    || input_path.starts_with(DATA_DIR_PREFIX)
                    || input_path.starts_with(CACHE_DIR_PREFIX)
                    || input_path.starts_with(CONFIG_DIR_PREFIX)
                    || input_path.starts_with(OBELISK_TOML_DIR_PREFIX)
                {
                    warn!("Not expanding prefix of `{input_path}`");
                }

                PathBuf::from(input_path)
            };
        if path.exists() {
            Ok(path)
        } else {
            bail!("file does not exist: {path:?}")
        }
    }
    pub(crate) fn replace_file_prefix_no_verify(&self, input_path: &str) -> String {
        let path =
            if let (Some(project_dirs), Some(base_dirs)) = (&self.project_dirs, &self.base_dirs) {
                if let Some(suffix) = input_path.strip_prefix(HOME_DIR_PREFIX) {
                    base_dirs.home_dir().join(suffix)
                } else if let Some(suffix) = input_path.strip_prefix(DATA_DIR_PREFIX) {
                    project_dirs.data_dir().join(suffix)
                } else if let Some(suffix) = input_path.strip_prefix(CACHE_DIR_PREFIX) {
                    project_dirs.cache_dir().join(suffix)
                } else if let Some(suffix) = input_path.strip_prefix(CONFIG_DIR_PREFIX) {
                    project_dirs.config_dir().join(suffix)
                } else if let Some(suffix) = input_path.strip_prefix(OBELISK_TOML_DIR_PREFIX) {
                    self.obelisk_toml_dir.join(suffix)
                } else {
                    PathBuf::from(input_path)
                }
            } else {
                if input_path.starts_with(HOME_DIR_PREFIX)
                    || input_path.starts_with(DATA_DIR_PREFIX)
                    || input_path.starts_with(CACHE_DIR_PREFIX)
                    || input_path.starts_with(CONFIG_DIR_PREFIX)
                    || input_path.starts_with(OBELISK_TOML_DIR_PREFIX)
                {
                    warn!("Not expanding prefix of `{input_path}`");
                }

                PathBuf::from(input_path)
            };
        path.to_string_lossy().into_owned()
    }

    pub(crate) async fn replace_path_prefix_mkdir(
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
                } else if let Some(suffix) = dir.strip_prefix(OBELISK_TOML_DIR_PREFIX) {
                    self.obelisk_toml_dir.join(suffix)
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
                    || dir.starts_with(OBELISK_TOML_DIR_PREFIX)
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

pub(crate) struct ConfigHolder {
    config_source: Option<ConfigSource>,
    pub(crate) path_prefixes: PathPrefixes,
}

impl ConfigHolder {
    pub(crate) async fn generate_default_config(
        dst: Option<PathBuf>,
        overwrite: bool,
    ) -> Result<PathBuf, anyhow::Error> {
        let contents = format!("{OBELISK_HELP_TOML}\n{OBELISK_GEN_TOML}");
        let dst = dst.unwrap_or(PathBuf::from("obelisk.toml"));

        let mut file = OpenOptions::new()
            .write(true)
            .create(true) // Always allow creating new files.
            .truncate(true) // Truncate existing files.
            .create_new(!overwrite) // if true, `create` is ignored, and only new file creation is allowed, meaning overwriting is disabled.
            .open(&dst)
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

        Ok(dst)
    }

    pub(crate) fn new(
        project_dirs: Option<ProjectDirs>,
        base_dirs: Option<BaseDirs>,
        config: Option<ConfigSource>,
        allow_missing: bool,
    ) -> Result<Self, anyhow::Error> {
        let config_source = if let Some(config) = config {
            Some(config)
        } else {
            let local = PathBuf::from("obelisk.toml");
            let exists = local.try_exists().unwrap_or_default();
            if !allow_missing && !exists {
                bail!("cannot find `obelisk.toml` in current directory");
            }
            if exists {
                info!("Using configuration file {:?}", local);
                Some(ConfigSource::LocalFile(local))
            } else {
                None
            }
        };

        let obelisk_toml_dir = match &config_source {
            None | Some(ConfigSource::Url(_)) => {
                // For URLs or no config, use current working directory
                std::env::current_dir().context("failed to get CWD")?
            }
            Some(ConfigSource::LocalFile(path)) => path
                .canonicalize()
                .with_context(|| {
                    format!("error while calling canonicalize on parent path of {path:?}")
                })?
                .parent()
                .with_context(|| format!("error getting parent path of {path:?}"))?
                .to_path_buf(),
        };

        Ok(Self {
            path_prefixes: PathPrefixes {
                obelisk_toml_dir,
                project_dirs,
                base_dirs,
            },
            config_source,
        })
    }

    pub(crate) async fn load_config(&self) -> Result<ConfigToml, anyhow::Error> {
        let mut builder = ConfigBuilder::<AsyncState>::default();
        match &self.config_source {
            Some(ConfigSource::LocalFile(path)) => {
                builder = builder.add_source(
                    File::from(path.as_path())
                        .required(true)
                        .format(FileFormat::Toml),
                );
            }
            Some(ConfigSource::Url(url)) => {
                info!("Fetching configuration from URL: {url}");
                let content = fetch_url_content(url)
                    .await
                    .with_context(|| format!("failed to fetch configuration from {url}"))?;
                debug!("Downloaded {content}");
                builder = builder.add_source(File::from_str(&content, FileFormat::Toml));
            }
            None => {}
        }
        let settings = builder
            .add_source(Environment::with_prefix("obelisk").separator("__"))
            .build()
            .await?;
        Ok(settings.try_deserialize()?)
    }
}

async fn fetch_url_content(url: &str) -> Result<String, anyhow::Error> {
    let client = reqwest::Client::builder()
        .build()
        .context("failed to build HTTP client")?;

    let response = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/toml, text/plain, */*")
        .send()
        .await
        .with_context(|| format!("failed to send request to {url}"))?;

    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_default();
        bail!("HTTP {status}: {error_body}");
    }

    // Check for obviously wrong content types that indicate an error page
    if let Some(content_type) = response.headers().get(reqwest::header::CONTENT_TYPE)
        && let Ok(ct) = content_type.to_str()
        && ct.starts_with("text/html")
    {
        bail!("server returned HTML instead of TOML (Content-Type: {ct})");
    }

    response
        .text()
        .await
        .with_context(|| format!("failed to read response body from {url}"))
}
