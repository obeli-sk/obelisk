use super::toml::ConfigToml;
use anyhow::{Context as _, bail};
use config::{ConfigBuilder, Environment, File, FileFormat, builder::AsyncState};
use directories::{BaseDirs, ProjectDirs};
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tracing::info;
use tracing::warn;

const OBELISK_HELP_TOML: &str = include_str!("../../obelisk-help.toml");
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
    obelisk_toml: Option<PathBuf>,
    pub(crate) path_prefixes: PathPrefixes,
}

impl ConfigHolder {
    pub(crate) async fn generate_default_config(
        dst: Option<PathBuf>,
        overwrite: bool,
    ) -> Result<(), anyhow::Error> {
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
        println!("Generated {dst:?}");

        Ok(())
    }

    pub(crate) fn new(
        project_dirs: Option<ProjectDirs>,
        base_dirs: Option<BaseDirs>,
        config: Option<PathBuf>,
        allow_missing: bool,
    ) -> Result<Self, anyhow::Error> {
        let obelisk_toml = if let Some(config) = config {
            Some(config)
        } else {
            let local = PathBuf::from("obelisk.toml");
            let exists = local.try_exists().unwrap_or_default();
            if !allow_missing && !exists {
                bail!("cannot find `obelisk.toml` in current directory");
            }
            if exists {
                info!("Using configuration file {:?}", local);
                Some(local)
            } else {
                None
            }
        };

        Ok(Self {
            path_prefixes: PathPrefixes {
                obelisk_toml_dir: match &obelisk_toml {
                    None => std::env::current_dir().context("failed to get CWD")?,
                    Some(obelisk_toml) => {
                        obelisk_toml.canonicalize().with_context(|| {
                            format!("error while calling canonicalize on parent path of {obelisk_toml:?}")
                        })?
                        .parent()
                        .with_context(|| format!("error getting parent path of {obelisk_toml:?}"))?
                        .to_path_buf()
                    }
                },
                project_dirs,
                base_dirs,
            },
            obelisk_toml,
        })
    }

    pub(crate) async fn load_config(&self) -> Result<ConfigToml, anyhow::Error> {
        let mut builder = ConfigBuilder::<AsyncState>::default();
        if let Some(obelisk_toml) = self.obelisk_toml.as_deref() {
            builder = builder.add_source(
                File::from(obelisk_toml)
                    .required(true)
                    .format(FileFormat::Toml),
            );
        }
        let settings = builder
            .add_source(Environment::with_prefix("obelisk").separator("__"))
            .build()
            .await?;
        Ok(settings.try_deserialize()?)
    }
}
