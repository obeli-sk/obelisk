use anyhow::{Context, bail, ensure};
use concepts::{ContentDigest, Digest};
use futures_util::TryFutureExt;
use oci_client::{
    Reference,
    errors::OciDistributionError,
    manifest::{OciDescriptor, OciImageManifest},
};
use oci_wasm::{ToConfig, WASM_MANIFEST_MEDIA_TYPE, WasmClient, WasmConfig};
use std::{
    future::Future,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, instrument, warn};
use utils::{sha256sum::calculate_sha256_file, wasm_tools::WasmComponent};

const OCI_CLIENT_RETRIES: u64 = 10;

fn digest_to_metadata_file(metadata_dir: &Path, metadata_file: &Digest) -> PathBuf {
    metadata_dir.join(format!(
        "{}_{}.txt",
        metadata_file.hash_type(),
        metadata_file.digest_base16()
    ))
}

#[instrument(skip_all)]
async fn metadata_to_content_digest(
    image: &Reference,
    metadata_dir: &Path,
) -> Result<Option<ContentDigest>, anyhow::Error> {
    // Recoverable errors, like reading inconsistent data from disk should be ignored. Image will be downloaded again.
    let metadata_digest = image
        .digest()
        .map(Digest::from_str)
        .transpose()
        .context("image digest specified in the image must be well-formed")?;
    if let Some(metadata_digest) = metadata_digest {
        let metadata_file = digest_to_metadata_file(metadata_dir, &metadata_digest);
        if metadata_file.is_file()
            && let Ok(content) = tokio::fs::read_to_string(&metadata_file).await
            && let Ok(digest) = ContentDigest::from_str(&content)
        {
            return Ok(Some(digest));
        }
    }
    Ok(None)
}

#[instrument(skip_all, fields(image = image.to_string()) err)]
pub(crate) async fn pull_to_cache_dir(
    image: &Reference,
    wasm_cache_dir: &Path,
    metadata_dir: &Path,
) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
    let client = WasmClientWithRetry::new(OCI_CLIENT_RETRIES);
    let auth = get_oci_auth(image)?;
    // If the `image` specifies the metadata digest, try to find the link in the `metadata_dir`.
    let pull_request = if let Some(content_digest) =
        metadata_to_content_digest(image, metadata_dir).await?
    {
        PullRequest::MatchedMappingFile { content_digest }
    } else {
        // Otherwise download metadata and return the first layer's digest.
        info!("Fetching metadata");
        let (layer_content_digest, layer, metadata_digest) = client
            .pull_manifest_and_config_with_retry(image, &auth)
            .await?;
        debug!("Fetched metadata digest {metadata_digest}");
        match image.digest() {
            None => warn!(
                "Consider adding metadata digest to component's `location.oci` configuration: {image}@{metadata_digest}"
            ),
            Some(specified) => {
                ensure!(
                    specified == metadata_digest,
                    "metadata digest specified in {image} must be respected by the oci client, got {metadata_digest}"
                );
            }
        }
        // Create new file in the metadata directory.
        let metadata_file =
            digest_to_metadata_file(metadata_dir, &Digest::from_str(&metadata_digest)?);
        tokio::fs::write(&metadata_file, layer_content_digest.to_string()).await?;
        PullRequest::GotMetadata {
            layer,
            layer_content_digest,
        }
    };
    let content_digest = pull_request.content_digest();
    let wasm_path = wasm_cache_dir.join(format!(
        "{hash_type}_{content_digest}.wasm",
        hash_type = content_digest.hash_type(),
        content_digest = content_digest.digest_base16(),
    ));

    if wasm_path.is_file() {
        // Verify that the local file matches the expected `content_digest`, otherwise download it again.
        match calculate_sha256_file(&wasm_path).await {
            Ok(actual_digest) if actual_digest == *content_digest => {
                return Ok((actual_digest, wasm_path));
            }
            Ok(wrong_digest) => {
                warn!(
                    "Wrong digest for {wasm_path:?}, expected: {content_digest}, actual: {wrong_digest}"
                );
            }
            Err(err) => {
                warn!("Cannot calculate digest for {wasm_path:?} - {err:?}");
            }
        }
    }
    info!("Pulling image to {wasm_path:?}");
    let content_digest = content_digest.clone();
    client
        .pull_with_retry(image, &auth, &wasm_path, pull_request)
        .await
        .with_context(|| format!("Unable to pull image {image}"))?;

    Ok((content_digest, wasm_path))
}

fn get_oci_auth(reference: &Reference) -> Result<oci_client::secrets::RegistryAuth, anyhow::Error> {
    /// Translate the registry into a key for the auth lookup.
    fn get_docker_config_auth_key(reference: &Reference) -> &str {
        match reference.resolve_registry() {
            "index.docker.io" => "https://index.docker.io/v1/", // Default registry uses this key.
            other => other, // All other registries are keyed by their domain name without the `https://` prefix or any path suffix.
        }
    }
    let server_url = get_docker_config_auth_key(reference);
    match docker_credential::get_credential(server_url) {
        Ok(docker_credential::DockerCredential::UsernamePassword(username, password)) => {
            return Ok(oci_client::secrets::RegistryAuth::Basic(username, password));
        }
        Ok(docker_credential::DockerCredential::IdentityToken(_)) => {
            bail!("identity tokens not supported")
        }
        Err(err) => {
            debug!("Failed to look up OCI credentials with key `{server_url}`: {err}");
        }
    }
    Ok(oci_client::secrets::RegistryAuth::Anonymous)
}

pub(crate) async fn push(wasm_path: PathBuf, reference: &Reference) -> Result<(), anyhow::Error> {
    if reference.digest().is_some() {
        bail!("cannot push a digest reference");
    }
    let wasm_path = {
        // Attempt to convert the core module to a component if needed.
        let output_parent = wasm_path
            .parent()
            .expect("direct parent of a file is never None");
        WasmComponent::convert_core_module_to_component(&wasm_path, output_parent)
            .await?
            .unwrap_or(wasm_path)
    };
    // Sanity check: Is it really a WASM Component?
    WasmComponent::verify_wasm(&wasm_path)?;
    debug!("Pushing...");

    let client = WasmClientWithRetry::new(OCI_CLIENT_RETRIES);
    let (conf, layer) = WasmConfig::from_component(&wasm_path, None)
        .await
        .context("Unable to parse component")?;
    let auth = get_oci_auth(reference)?;
    let resp = client
        .push(reference, &auth, layer, conf, None)
        .await
        .context("Unable to push image")?;

    if let Some(digest) = resp.manifest_url.rsplit("manifests/sha256:").next() {
        println!("{reference}@sha256:{digest}");
    } else {
        println!("{reference}");
    }
    Ok(())
}

struct WasmClientWithRetry {
    client: WasmClient,
    retries: u64,
}

impl WasmClientWithRetry {
    fn new(retries: u64) -> Self {
        Self {
            client: WasmClient::new(oci_client::Client::default()),
            retries,
        }
    }

    async fn retry<O, E: std::fmt::Debug, F: Future<Output = Result<O, E>>>(
        &self,
        what: impl Fn() -> F,
        reason: &'static str,
    ) -> Result<O, E> {
        let mut tries = 0;
        loop {
            match what().await {
                Ok(ok) => return Ok(ok),
                Err(err) if tries == self.retries => return Err(err),
                Err(err) => {
                    tries += 1;
                    let duration = Duration::from_secs(tries);
                    debug!("Error {reason} {err:?}");
                    warn!("Retrying after {duration:?}");
                    tokio::time::sleep(duration).await;
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn pull_manifest_and_config_with_retry(
        &self,
        image: &Reference,
        auth: &oci_client::secrets::RegistryAuth,
    ) -> anyhow::Result<(
        ContentDigest,
        OciDescriptor, /* layer */
        String,        /* metadata_digest */
    )> {
        self.retry(
            || async {
                let (mut manifest, wasm_config, metadata_digest) =
                    self.client.pull_manifest_and_config(image, auth).await?;

                let layer = manifest
                    .layers
                    .pop()
                    .expect("oci-wasm checks that Wasm components must have exactly one layer");
                if layer.media_type != oci_wasm::WASM_LAYER_MEDIA_TYPE {
                    return Err(OciDistributionError::IncompatibleLayerMediaTypeError(
                        layer.media_type.clone(),
                    )
                    .into());
                }
                let layer_content_digest = ContentDigest::from_str(&layer.digest)
                    .context("layer content digest must be well-formed")?;

                // Verify WASM Component
                wasm_config
                    .component
                    .context("image must contain a wasi component")?;
                Ok((layer_content_digest, layer, metadata_digest))
            },
            "calling pull_manifest_and_config",
        )
        .await
    }

    #[instrument(skip_all)]
    async fn pull_with_retry(
        &self,
        image: &Reference,
        auth: &oci_client::secrets::RegistryAuth,
        wasm_path: &Path,
        pull_request: PullRequest,
    ) -> anyhow::Result<()> {
        let requested_content_digest = pull_request.content_digest();
        let maybe_uninitialized_layer;
        let layer = match &pull_request {
            PullRequest::GotMetadata {
                layer,
                layer_content_digest: _,
            } => layer,
            PullRequest::MatchedMappingFile { content_digest } => {
                let (layer_content_digest, layer, _) = self
                    .pull_manifest_and_config_with_retry(image, auth)
                    .await?;
                maybe_uninitialized_layer = layer;
                ensure!(
                    layer_content_digest == *content_digest,
                    "oci client must have downloaded the requested image"
                );
                &maybe_uninitialized_layer
            }
        };
        self.retry(
            || self.pull(image, wasm_path, layer, requested_content_digest),
            "pulling the image",
        )
        .await
    }

    async fn pull(
        &self,
        image: &Reference,
        wasm_path: &Path,
        layer: &OciDescriptor,
        requested_content_digest: &ContentDigest,
    ) -> anyhow::Result<()> {
        debug!("Pulling image: {:?}", image);
        let oci_client = self.client.as_ref();
        {
            // Crashing while writing is safe: File will be discarded on next startup during "Verify file content digest"
            let file = tokio::fs::File::create(wasm_path).await?;
            let mut buffer = tokio::io::BufWriter::new(file);
            oci_client.pull_blob(image, &layer, &mut buffer).await?;
            buffer.flush().await?;
            // No `sync_all` for better performance. Starting after crash here would make digest mismatch and redownload the file.
        }
        let actual_content_digest = calculate_sha256_file(wasm_path).await?;
        ensure!(
            *requested_content_digest == actual_content_digest,
            "sha256 digest mismatch for {image}, file {wasm_path:?}. Expected {requested_content_digest}, got {actual_content_digest}"
        );
        Ok(())
    }

    #[instrument(skip_all)]
    async fn push(
        &self,
        image: &Reference,
        auth: &oci_client::secrets::RegistryAuth,
        component_layer: oci_client::client::ImageLayer,
        config: impl ToConfig,
        annotations: Option<std::collections::BTreeMap<String, String>>,
    ) -> anyhow::Result<oci_client::client::PushResponse> {
        let layers = vec![component_layer];
        let config = config.to_config()?;
        let mut manifest = OciImageManifest::build(&layers, &config, annotations);
        manifest.media_type = Some(WASM_MANIFEST_MEDIA_TYPE.to_string());
        self.retry(
            || {
                let config = config.clone();
                let manifest = manifest.clone();
                self.client
                    .as_ref()
                    .push(image, &layers, config, auth, Some(manifest))
                    .err_into()
            },
            "pushing the image",
        )
        .await
    }
}

enum PullRequest {
    MatchedMappingFile {
        content_digest: ContentDigest,
    },
    GotMetadata {
        layer: OciDescriptor,
        layer_content_digest: ContentDigest,
    },
}
impl PullRequest {
    fn content_digest(&self) -> &ContentDigest {
        match self {
            PullRequest::MatchedMappingFile { content_digest } => content_digest,
            PullRequest::GotMetadata {
                layer: _,
                layer_content_digest,
            } => layer_content_digest,
        }
    }
}
