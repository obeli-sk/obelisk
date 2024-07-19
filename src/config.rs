use anyhow::{bail, Context as _};
use concepts::{prefixed_ulid::ConfigId, ComponentId, ComponentType};
use config::{builder::AsyncState, ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use serde::Deserialize;
use serde_with::serde_as;
use std::{path::PathBuf, time::Duration};
use tokio::{sync::mpsc, task::AbortHandle};
use tracing::{debug, error, info, trace, warn};
use wasm_workers::workflow_worker::JoinNextBlockingStrategy;

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

fn default_max_retries() -> u32 {
    5
}

fn default_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Millis(100)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentCommon {
    pub(crate) name: String,
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: Option<String>,
    #[serde(default = "ConfigId::generate")]
    pub(crate) config_id: ConfigId,
    #[serde(default)]
    pub(crate) exec: ExecConfig,
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: u32, // TODO: persist and use when creating an execution
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig, // TODO: persist and use when creating an execution
}

impl ComponentCommon {
    async fn verify_content_digest(
        &self,
        r#type: ComponentType,
    ) -> Result<(ComponentId, PathBuf), anyhow::Error> {
        let (actual, wasm_path) = self.location.calculate_content_digest().await?;
        if let Some(specified) = &self.content_digest {
            if *specified != actual.to_string() {
                bail!("Wrong content digest for {type} {name}, specified {specified} , actually got {actual}",
                    name = self.name)
            }
        }
        Ok((actual, wasm_path))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurationConfig {
    Secs(u64),
    Millis(u64),
}

impl From<DurationConfig> for Duration {
    fn from(value: DurationConfig) -> Self {
        match value {
            DurationConfig::Millis(millis) => Duration::from_millis(millis),
            DurationConfig::Secs(secs) => Duration::from_secs(secs),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecConfig {
    batch_size: u32,
    lock_expiry: DurationConfig,
    tick_sleep: DurationConfig,
}

impl ExecConfig {
    pub(crate) fn into_exec_exec_config(
        self,
        config_id: ConfigId,
    ) -> executor::executor::ExecConfig {
        executor::executor::ExecConfig {
            lock_expiry: self.lock_expiry.into(),
            tick_sleep: self.tick_sleep.into(),
            batch_size: self.batch_size,
            config_id,
        }
    }
}

impl Default for ExecConfig {
    fn default() -> Self {
        Self {
            batch_size: 5,
            lock_expiry: DurationConfig::Secs(1),
            tick_sleep: DurationConfig::Millis(200),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Activity {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    #[serde(default = "default_true")]
    pub(crate) recycle_instances: bool,
}

impl Activity {
    pub(crate) async fn verify_content_digest(
        &self,
    ) -> Result<(ComponentId, PathBuf), anyhow::Error> {
        self.common
            .verify_content_digest(ComponentType::WasmActivity)
            .await
    }
}

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentLocation {
    File(PathBuf),
    Oci(#[serde_as(as = "serde_with::DisplayFromStr")] oci_distribution::Reference),
}

impl ComponentLocation {
    async fn calculate_content_digest(&self) -> Result<(ComponentId, PathBuf), anyhow::Error> {
        match self {
            Self::File(wasm_path) => {
                let wasm_path = wasm_path
                    .canonicalize()
                    .with_context(|| format!("cannot canonicalize file `{wasm_path:?}`"))?;
                let component_id = wasm_workers::component_detector::file_hash(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;

                Ok((component_id, wasm_path))
            }
            Self::Oci(image) => oci::obtan_wasm_from_oci(&image).await,
        }
    }
}

mod oci {
    use std::path::PathBuf;

    use anyhow::{bail, ensure, Context};
    use concepts::ComponentId;
    use tracing::{debug, info};

    pub(crate) async fn obtan_wasm_from_oci(
        image: &oci_distribution::Reference,
    ) -> Result<(ComponentId, PathBuf), anyhow::Error> {
        let client = {
            let client =
                oci_distribution::Client::new(oci_distribution::client::ClientConfig::default());
            oci_wasm::WasmClient::new(client)
        };
        info!("Fetching metadata for {image}");
        let auth = get_oci_auth(&image)?;
        let (_oci_config, wasm_config, metadata_digest) =
            client.pull_manifest_and_config(&image, &auth).await?;
        wasm_config
            .component
            .context("image must contain a wasi component")?;
        ensure!(
            wasm_config.layer_digests.len() == 1,
            "expected single layer in {image}, got {layers}",
            layers = wasm_config.layer_digests.len()
        );
        let content_digest = wasm_config.layer_digests.first().expect("ensured already");

        // TODO: avoid the metadata fetch if we know the metadata digest
        debug!(
            "TODO: Saving metadata digest {metadata_digest} to content digest {content_digest:?}"
        );

        use directories::ProjectDirs;
        let project_dirs = ProjectDirs::from("com", "obelisk", "obelisk")
            .context("cannot obtain project directories")?;
        let cache_dir = project_dirs.cache_dir().join("wasm");
        tokio::fs::create_dir_all(&cache_dir)
            .await
            .with_context(|| format!("cannot create cache directory {cache_dir:?}"))?;
        let wasm_path = cache_dir.join(PathBuf::from(format!(
            "{content_digest}.wasm",
            content_digest = content_digest
                .strip_prefix("sha256:")
                .unwrap_or(content_digest)
        )));
        // Do not download if the file exists and matches the expected sha256 digest.
        match wasm_workers::component_detector::file_hash(&wasm_path).await {
            Ok(actual) if actual.to_string() == *content_digest => {
                debug!("Found in cache {wasm_path:?}");
            }
            _ => {
                info!("Pulling image to {wasm_path:?}");
                let data = client
                    // FIXME: do not download all layers at once to memory
                    .pull(&image, &auth)
                    .await
                    .with_context(|| format!("Unable to pull image {image}"))?;

                let data = {
                    assert_eq!(1, data.layers.len(), "ensured already");
                    data.layers
                        .into_iter()
                        .next()
                        .expect("ensured already")
                        .data
                };
                tokio::fs::write(&wasm_path, data)
                    .await
                    .with_context(|| format!("unable to write file {wasm_path:?}"))?;
            }
        }
        // Verify that the sha256 matches the actual download.
        let actual_hash = wasm_workers::component_detector::file_hash(&wasm_path)
            .await
            .with_context(|| {
                format!("failed to compute sha256 of the downloaded wasm file {wasm_path:?}")
            })?;
        ensure!(
        *content_digest == actual_hash.to_string() ,
        "sha256 digest mismatch for {image}, file {wasm_path:?}. Expected {content_digest}, got {actual_hash}"
    );

        Ok((actual_hash, wasm_path))
    }

    // TODO: move to `wasm_pkg_common`
    fn get_oci_auth(
        reference: &oci_distribution::Reference,
    ) -> Result<oci_distribution::secrets::RegistryAuth, anyhow::Error> {
        /// Translate the registry into a key for the auth lookup.
        fn get_docker_config_auth_key(reference: &oci_distribution::Reference) -> &str {
            match reference.resolve_registry() {
                "index.docker.io" => "https://index.docker.io/v1/", // Default registry uses this key.
                other => other, // All other registries are keyed by their domain name without the `https://` prefix or any path suffix.
            }
        }
        let server_url = get_docker_config_auth_key(reference);
        match docker_credential::get_credential(server_url) {
            Ok(docker_credential::DockerCredential::UsernamePassword(username, password)) => {
                return Ok(oci_distribution::secrets::RegistryAuth::Basic(
                    username, password,
                ));
            }
            Ok(docker_credential::DockerCredential::IdentityToken(_)) => {
                bail!("identity tokens not supported")
            }
            Err(err) => {
                debug!("Failed to look up OCI credentials with key `{server_url}`: {err}");
            }
        }
        Ok(oci_distribution::secrets::RegistryAuth::Anonymous)
    }
}

fn default_strategy() -> JoinNextBlockingStrategy {
    JoinNextBlockingStrategy::Await
}

fn default_child_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Millis(100)
}

fn default_child_max_retries() -> u32 {
    5
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Workflow {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    #[serde(default = "default_strategy")]
    pub(crate) join_next_blocking_strategy: JoinNextBlockingStrategy,
    #[serde(default = "default_child_retry_exp_backoff")]
    pub(crate) child_retry_exp_backoff: DurationConfig,
    #[serde(default = "default_child_max_retries")]
    pub(crate) child_max_retries: u32,
    #[serde(default = "default_true")]
    pub(crate) non_blocking_event_batching: bool,
}

impl Workflow {
    pub(crate) async fn verify_content_digest(
        &self,
    ) -> Result<(ComponentId, PathBuf), anyhow::Error> {
        self.common
            .verify_content_digest(ComponentType::WasmWorkflow)
            .await
    }
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
fn default_true() -> bool {
    true
}
