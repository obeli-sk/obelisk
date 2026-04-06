use crate::args::TomlComponentType;
use crate::config::toml::{AllowedHostToml, DurationConfig, JsParamToml, OCI_SCHEMA_PREFIX};
use crate::config::{content_digest_to_js_file, content_digest_to_wasm_file};
use anyhow::{Context, bail, ensure};
use concepts::FunctionFqn;
use concepts::{ContentDigest, component_id::Digest};
use futures_util::TryFutureExt;
use oci_client::{
    Reference,
    errors::OciDistributionError,
    manifest::{OciDescriptor, OciImageManifest},
};
use oci_wasm::{ToConfig, WASM_MANIFEST_MEDIA_TYPE, WasmClient, WasmConfig};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    future::Future,
    io::ErrorKind,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, instrument, warn};
use utils::{sha256sum::calculate_sha256_file, wasm_tools::WasmComponent};

pub const METADATA_ANNOTATION_KEY: &str = "obelisk.component_metadata:0.1.0";
pub const ACTIVITY_JS_CONFIG_ANNOTATION_KEY: &str = "obelisk.activity_js_config:0.1.0";
pub const JS_LAYER_MEDIA_TYPE: &str = "application/vnd.obelisk.js.v0+javascript";
const JS_CONFIG_MEDIA_TYPE: &str = "application/vnd.obelisk.js.config.v0+json";

struct LayerWithAnnotations {
    layer_content_digest: ContentDigest,
    layer: OciDescriptor,
    metadata_digest: String,
    manifest_annotations: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ComponentMetadataAnnotation {
    pub component_type: TomlComponentType,
    pub env_vars: Vec<String>,
    pub allowed_hosts: Vec<AllowedHostToml>,
    pub lock_duration: Option<DurationConfig>,
}

/// JS-specific configuration stored as an OCI manifest annotation.
/// Contains the information needed to reconstruct the WIT interface for a JS activity or workflow.
/// Not present for webhook JS components (no function signature).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsWitConfigAnnotation {
    pub ffqn: FunctionFqn,
    #[serde(default)]
    pub params: Vec<JsParamToml>,
    pub return_type: Option<String>,
}

pub(crate) struct JsCacheResult {
    pub(crate) js_path: PathBuf,
    pub(crate) manifest_digest: String,
    pub(crate) js_config: Option<JsWitConfigAnnotation>,
}

const OCI_CLIENT_RETRIES: u64 = 10;

// Content of this file is a sha sum of the downloaded WASM file.
fn digest_to_metadata_file(metadata_dir: &Path, metadata_file: &Digest) -> PathBuf {
    metadata_dir.join(format!("{}.txt", metadata_file.with_infix("_")))
}

async fn verify_cached_file(path: &Path, content_digest: &ContentDigest) -> Result<(), ()> {
    match calculate_sha256_file(&path).await {
        Ok(actual_digest) if actual_digest == *content_digest => Ok(()),
        Ok(wrong_digest) => {
            warn!(
                "Wrong digest for {path:?}, deleting the file. Expected: {content_digest}, actual: {wrong_digest}"
            );
            let _ = tokio::fs::remove_file(path).await;
            Err(())
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Err(()),
        Err(err) => {
            warn!("Cannot calculate digest for {path:?}, deleting the file - {err:?}");
            let _ = tokio::fs::remove_file(path).await;
            Err(())
        }
    }
}

#[instrument(skip_all, fields(image = image.to_string()) err)]
pub(crate) async fn pull_to_cache_dir(
    image: &Reference,
    wasm_cache_dir: &Path,
    metadata_dir: &Path,
) -> Result<
    (
        ContentDigest,
        PathBuf,
        String,
        Option<ComponentMetadataAnnotation>,
    ),
    anyhow::Error,
> {
    let client = WasmClientWithRetry::new(OCI_CLIENT_RETRIES);
    let auth = get_oci_auth(image)?;
    // Happy path: image's metadata digest mapping.txt -> content digest -> file -> verify hash
    // Recoverable errors, like reading inconsistent data, will be ignored. Image will be downloaded again.
    if let Some(manifest_digest) = image.digest()
        && let Ok(metadata_digest) = Digest::from_str(manifest_digest)
        && let metadata_file = digest_to_metadata_file(metadata_dir, &metadata_digest)
        && let Ok(content) = tokio::fs::read_to_string(&metadata_file).await
        && let Ok(content_digest) = ContentDigest::from_str(&content)
        && let wasm_path = content_digest_to_wasm_file(wasm_cache_dir, &content_digest)
        && let Ok(()) = verify_cached_file(&wasm_path, &content_digest).await
    {
        return Ok((content_digest, wasm_path, manifest_digest.to_string(), None));
    }
    // The mapping file will be recreated. We need to fetch metadata anyway for `layer`
    // and use that as the source of truth.

    info!("Fetching metadata");
    let (layer, content_digest, manifest_digest, component_metadata) = {
        let LayerWithAnnotations {
            layer_content_digest,
            layer,
            metadata_digest,
            manifest_annotations,
        } = client
            .pull_manifest_and_config_with_retry(image, &auth)
            .await?;
        debug!("Fetched manifest digest {metadata_digest}");
        if let Some(specified) = image.digest() {
            ensure!(
                specified == metadata_digest,
                "manifest digest specified in {image} must be respected by the oci client, got {metadata_digest}"
            );
        }
        // Create new file in the metadata directory.
        let metadata_file =
            digest_to_metadata_file(metadata_dir, &Digest::from_str(&metadata_digest)?);
        debug!("Writing WASM digest {layer_content_digest} to metadata file {metadata_file:?}");
        tokio::fs::write(&metadata_file, layer_content_digest.to_string()).await?;

        let comp_metadata = extract_component_metadata(manifest_annotations.as_ref());
        (layer, layer_content_digest, metadata_digest, comp_metadata)
    };
    let wasm_path = content_digest_to_wasm_file(wasm_cache_dir, &content_digest);
    if let Ok(()) = verify_cached_file(&wasm_path, &content_digest).await {
        return Ok((
            content_digest,
            wasm_path,
            manifest_digest.clone(),
            component_metadata,
        ));
    }
    info!("Pulling image to {wasm_path:?}");
    pull_blob_to_file(
        client.client.as_ref(),
        image,
        &wasm_path,
        &layer,
        &content_digest,
    )
    .await
    .with_context(|| format!("Unable to pull image {image}"))?;

    Ok((
        content_digest,
        wasm_path,
        manifest_digest.clone(),
        component_metadata,
    ))
}

/// Pull a JS image from OCI to the local JS cache directory.
#[instrument(skip_all, fields(image = image.to_string()) err)]
pub(crate) async fn pull_js_to_cache(
    image: &Reference,
    js_cache_dir: &Path,
    metadata_dir: &Path,
) -> Result<JsCacheResult, anyhow::Error> {
    let auth = get_oci_auth(image)?;
    let raw_client = oci_client::Client::default();

    // Happy path: manifest digest → content digest → cached file
    if let Some(manifest_digest) = image.digest()
        && let Ok(meta_digest) = Digest::from_str(manifest_digest)
        && let metadata_file = digest_to_metadata_file(metadata_dir, &meta_digest)
        && let Ok(content) = tokio::fs::read_to_string(&metadata_file).await
        && let Ok(content_digest) = ContentDigest::from_str(&content)
        && let js_path = content_digest_to_js_file(js_cache_dir, &content_digest)
        && let Ok(()) = verify_cached_file(&js_path, &content_digest).await
    {
        return Ok(JsCacheResult {
            js_path,
            manifest_digest: manifest_digest.to_string(),
            js_config: None,
        });
    }

    info!("Fetching JS metadata");
    let (manifest, manifest_digest, _config_str) = retry(
        || raw_client.pull_manifest_and_config(image, &auth),
        OCI_CLIENT_RETRIES,
        "calling pull_manifest_and_config for JS",
    )
    .await?;

    if let Some(specified) = image.digest() {
        ensure!(
            specified == manifest_digest,
            "manifest digest specified in {image} must be respected by the oci client, got {manifest_digest}"
        );
    }

    let layer = manifest
        .layers
        .into_iter()
        .next()
        .context("JS OCI image must have exactly one layer")?;
    ensure!(
        layer.media_type == JS_LAYER_MEDIA_TYPE,
        "expected JS layer media type {JS_LAYER_MEDIA_TYPE}, got {}",
        layer.media_type
    );
    let content_digest =
        ContentDigest::from_str(&layer.digest).context("JS layer digest must be well-formed")?;

    let metadata_file = digest_to_metadata_file(metadata_dir, &Digest::from_str(&manifest_digest)?);
    debug!("Writing JS digest {content_digest} to metadata file {metadata_file:?}");
    tokio::fs::write(&metadata_file, content_digest.to_string()).await?;

    let js_config = extract_js_config(manifest.annotations.as_ref());

    let js_path = content_digest_to_js_file(js_cache_dir, &content_digest);
    if let Ok(()) = verify_cached_file(&js_path, &content_digest).await {
        return Ok(JsCacheResult {
            js_path,
            manifest_digest,
            js_config,
        });
    }

    info!("Pulling JS source to {js_path:?}");
    let layer_desc = OciDescriptor {
        digest: layer.digest,
        size: layer.size,
        media_type: layer.media_type,
        ..Default::default()
    };
    pull_blob_to_file(&raw_client, image, &js_path, &layer_desc, &content_digest)
        .await
        .with_context(|| format!("Unable to pull JS image {image}"))?;

    Ok(JsCacheResult {
        js_path,
        manifest_digest,
        js_config,
    })
}

/// Pull only the manifest/config to extract metadata, without downloading the blob.
/// Works for both WASM and JS OCI images.
/// Returns `(component_metadata, js_config)`.
pub(crate) async fn pull_metadata(
    image: &Reference,
) -> Result<
    (
        Option<ComponentMetadataAnnotation>,
        Option<JsWitConfigAnnotation>,
    ),
    anyhow::Error,
> {
    let auth = get_oci_auth(image)?;
    let raw_client = oci_client::Client::default();
    info!("Fetching metadata");
    let (manifest, _manifest_digest, _config_str) = retry(
        || raw_client.pull_manifest_and_config(image, &auth),
        OCI_CLIENT_RETRIES,
        "calling pull_manifest_and_config",
    )
    .await?;
    let comp_metadata = extract_component_metadata(manifest.annotations.as_ref());
    let js_config = extract_js_config(manifest.annotations.as_ref());
    Ok((comp_metadata, js_config))
}

fn extract_component_metadata(
    annotations: Option<&BTreeMap<String, String>>,
) -> Option<ComponentMetadataAnnotation> {
    annotations
        .and_then(|m| m.get(METADATA_ANNOTATION_KEY))
        .and_then(|json| serde_json::from_str(json).ok())
}

fn extract_js_config(
    annotations: Option<&BTreeMap<String, String>>,
) -> Option<JsWitConfigAnnotation> {
    annotations
        .and_then(|m| m.get(ACTIVITY_JS_CONFIG_ANNOTATION_KEY))
        .and_then(|json| serde_json::from_str(json).ok())
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

pub(crate) async fn push(
    wasm_path: PathBuf,
    reference: &Reference,
    metadata: &ComponentMetadataAnnotation,
) -> Result<(), anyhow::Error> {
    if reference.digest().is_some() {
        bail!("cannot push a digest reference");
    }
    // Sanity check: Is it really a WASM Component?
    if WasmComponent::verify_wasm(&wasm_path).is_err() {
        // Attempt to convert the core module to a component
        let output_parent = wasm_path
            .parent()
            .expect("direct parent of a file is never None");
        let input_digest = calculate_sha256_file(&wasm_path).await?;
        WasmComponent::convert_core_module_to_component(&wasm_path, &input_digest, output_parent)
            .await?
            .context(
                "input file is not a WASM Component, and conversion from core module failed",
            )?;
    }
    debug!("Pushing...");
    let client = WasmClientWithRetry::new(OCI_CLIENT_RETRIES);
    let (conf, layer) = WasmConfig::from_component(&wasm_path, None)
        .await
        .context("Unable to parse component")?;
    let auth = get_oci_auth(reference)?;

    let annotations = BTreeMap::from([(
        METADATA_ANNOTATION_KEY.to_string(),
        serde_json::to_string(metadata)?,
    )]);
    let resp = client
        .push(reference, &auth, layer, conf, Some(annotations))
        .await
        .context("Unable to push image")?;

    if let Some(digest) = resp.manifest_url.rsplit("manifests/sha256:").next() {
        println!("{OCI_SCHEMA_PREFIX}{reference}@sha256:{digest}");
    } else {
        println!("{OCI_SCHEMA_PREFIX}{reference}");
    }
    Ok(())
}

/// Push a JS source file to an OCI registry.
/// `js_config` is `Some` for `activity_js` and `workflow_js` (carries `ffqn/params/return_type`),
/// and `None` for `webhook_endpoint_js` (no function signature).
pub(crate) async fn push_js(
    js_path: PathBuf,
    reference: &Reference,
    metadata: &ComponentMetadataAnnotation,
    js_config: Option<&JsWitConfigAnnotation>,
) -> Result<(), anyhow::Error> {
    if reference.digest().is_some() {
        bail!("cannot push a digest reference");
    }
    let js_source = tokio::fs::read_to_string(&js_path)
        .await
        .with_context(|| format!("cannot read JS file {js_path:?}"))?;

    let layer = oci_client::client::ImageLayer::new(
        js_source.into_bytes(),
        JS_LAYER_MEDIA_TYPE.to_string(),
        None,
    );

    // Minimal empty config blob
    let config = oci_client::client::Config {
        data: b"{}".as_slice().into(),
        media_type: JS_CONFIG_MEDIA_TYPE.to_string(),
        annotations: None,
    };

    let mut annotations = BTreeMap::new();
    annotations.insert(
        METADATA_ANNOTATION_KEY.to_string(),
        serde_json::to_string(metadata)?,
    );
    if let Some(js) = js_config {
        annotations.insert(
            ACTIVITY_JS_CONFIG_ANNOTATION_KEY.to_string(),
            serde_json::to_string(js)?,
        );
    }

    let layers = vec![layer];
    let mut manifest = OciImageManifest::build(&layers, &config, Some(annotations));
    // Use standard OCI manifest media type (not WASM-specific)
    manifest.media_type = Some("application/vnd.oci.image.manifest.v1+json".to_string());

    let auth = get_oci_auth(reference)?;
    let raw_client = oci_client::Client::default();
    let resp = retry(
        || {
            raw_client.push(
                reference,
                &layers,
                config.clone(),
                &auth,
                Some(manifest.clone()),
            )
        },
        OCI_CLIENT_RETRIES,
        "pushing JS image",
    )
    .await
    .context("Unable to push JS image")?;

    if let Some(digest) = resp.manifest_url.rsplit("manifests/sha256:").next() {
        println!("{OCI_SCHEMA_PREFIX}{reference}@sha256:{digest}");
    } else {
        println!("{OCI_SCHEMA_PREFIX}{reference}");
    }
    Ok(())
}

/// Pull a single blob layer to a local file, verifying the sha256 digest.
/// Writes atomically via a temp file to avoid partial reads.
async fn pull_blob_to_file(
    client: &oci_client::Client,
    image: &Reference,
    dest_path: &Path,
    layer: &OciDescriptor,
    requested_content_digest: &ContentDigest,
) -> anyhow::Result<()> {
    debug!("Pulling blob: {:?}", image);

    let dest_dir = dest_path
        .parent()
        .context("dest_path must have a parent directory")?;
    let temp_file = tempfile::NamedTempFile::new_in(dest_dir)?;
    let temp_path = temp_file.path().to_path_buf();
    temp_file.keep()?;
    {
        let file = tokio::fs::File::create(&temp_path).await?;
        let mut buffer = tokio::io::BufWriter::new(file);
        client.pull_blob(image, layer, &mut buffer).await?;
        buffer.flush().await?;
    }
    let actual_content_digest = calculate_sha256_file(&temp_path).await?;
    if *requested_content_digest != actual_content_digest {
        let _ = tokio::fs::remove_file(&temp_path).await;
        bail!(
            "sha256 digest mismatch for {image}, file {temp_path:?}. Expected {requested_content_digest}, got {actual_content_digest}"
        );
    }
    tokio::fs::rename(&temp_path, dest_path)
        .await
        .with_context(|| format!("cannot rename {temp_path:?} to {dest_path:?}"))?;
    Ok(())
}

/// Simple retry helper for async operations.
async fn retry<O, E: std::fmt::Debug, F: Future<Output = Result<O, E>>>(
    what: impl Fn() -> F,
    retries: u64,
    reason: &'static str,
) -> Result<O, E> {
    let mut tries = 0;
    loop {
        match what().await {
            Ok(ok) => return Ok(ok),
            Err(err) if tries == retries => return Err(err),
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
        retry(what, self.retries, reason).await
    }

    #[instrument(skip_all)]
    async fn pull_manifest_and_config_with_retry(
        &self,
        image: &Reference,
        auth: &oci_client::secrets::RegistryAuth,
    ) -> anyhow::Result<LayerWithAnnotations> {
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
                Ok(LayerWithAnnotations {
                    layer_content_digest,
                    layer,
                    metadata_digest,
                    manifest_annotations: manifest.annotations,
                })
            },
            "calling pull_manifest_and_config",
        )
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
