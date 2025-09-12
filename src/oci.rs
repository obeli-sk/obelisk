use anyhow::{Context, bail, ensure};
use concepts::{ContentDigest, Digest};
use futures_util::TryFutureExt;
use oci_client::{Reference, manifest::OciImageManifest};
use oci_wasm::{ToConfig, WASM_MANIFEST_MEDIA_TYPE, WasmClient, WasmConfig};
use std::{
    future::Future,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use tracing::{debug, info, instrument, warn};
use utils::wasm_tools::WasmComponent;

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
        .context("image digest must be well formed")?;
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
    let content_digest = if let Some(content_digest) =
        metadata_to_content_digest(image, metadata_dir).await?
    {
        content_digest
    } else {
        info!("Fetching metadata for {image}");
        let (_oci_config, wasm_config, metadata_digest) =
            client.pull_manifest_and_config(image, &auth).await?;
        match image.digest() {
            None => warn!(
                "Consider adding metadata digest to component's `location.oci` configuration: {image}@{metadata_digest}"
            ),
            Some(specified) => {
                debug!("Fetched metadata digest {metadata_digest}");
                if specified != metadata_digest {
                    unreachable!(
                        "metadata digest mismatch. Specified: {image}\nActual: {metadata_digest}"
                    )
                }
            }
        }
        wasm_config
            .component
            .context("image must contain a wasi component")?;
        let content_digest = ContentDigest::from_str(
            wasm_config
                .layer_digests
                .first()
                .expect("layer length asserted in WasmClient"),
        )?;
        // Create new file in the metadata directory.
        let metadata_file =
            digest_to_metadata_file(metadata_dir, &Digest::from_str(&metadata_digest)?);
        tokio::fs::write(&metadata_file, content_digest.to_string()).await?;
        content_digest
    };
    let wasm_path = wasm_cache_dir.join(format!(
        "{hash_type}_{content_digest}.wasm",
        hash_type = content_digest.hash_type(),
        content_digest = content_digest.digest_base16(),
    ));
    // Do not download if the file exists. Content hash must match as it was verified before writing, see below.
    if wasm_path.is_file() {
        return Ok((content_digest, wasm_path));
    }
    info!("Pulling image to {wasm_path:?}");
    let data = client
        .pull(image, &auth)
        .await
        .with_context(|| format!("Unable to pull image {image}"))?;
    let data = data
        .layers
        .into_iter()
        .next()
        .expect("layer length asserted in WasmClient")
        .data;
    let actual_hash = calculate_sha256_mem(&data);
    ensure!(
        content_digest == actual_hash,
        "sha256 digest mismatch for {image}, file {wasm_path:?}. Expected {content_digest}, got {actual_hash}"
    );
    // Write only after verifying the hash.
    tokio::fs::write(&wasm_path, data)
        .await
        .with_context(|| format!("unable to write file {wasm_path:?}"))?;
    Ok((actual_hash, wasm_path))
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

#[must_use]
fn calculate_sha256_mem(data: &[u8]) -> ContentDigest {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::default();
    hasher.update(data);
    ContentDigest::new(
        concepts::HashType::Sha256,
        format!("{:x}", hasher.finalize()),
    )
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
    async fn pull_manifest_and_config(
        &self,
        image: &Reference,
        auth: &oci_client::secrets::RegistryAuth,
    ) -> anyhow::Result<(oci_client::manifest::OciImageManifest, WasmConfig, String)> {
        self.retry(
            || self.client.pull_manifest_and_config(image, auth),
            "calling pull_manifest_and_config",
        )
        .await
    }

    #[instrument(skip_all)]
    async fn pull(
        &self,
        image: &Reference,
        auth: &oci_client::secrets::RegistryAuth,
    ) -> anyhow::Result<oci_client::client::ImageData> {
        self.retry(|| self.client.pull(image, auth), "pulling the image")
            .await
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
