use anyhow::{bail, ensure, Context};
use concepts::ContentDigest;
use oci_distribution::Reference;
use oci_wasm::{WasmClient, WasmConfig};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use tracing::{debug, info, warn};

fn get_client() -> WasmClient {
    WasmClient::new(oci_distribution::Client::new(
        oci_distribution::client::ClientConfig::default(),
    ))
}

pub(crate) async fn obtain_wasm_from_oci(
    image: &Reference,
    wasm_cache_dir: impl AsRef<Path>,
) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
    let wasm_cache_dir = wasm_cache_dir.as_ref();
    let client = get_client();
    let auth = get_oci_auth(image)?;
    let oci_cache_mapping_file = wasm_cache_dir.join("metadata_to_content_digests.json");
    let mut oci_cache_mapping: hashbrown::HashMap<String, ContentDigest> = {
        tokio::fs::read_to_string(&oci_cache_mapping_file)
            .await
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok())
            .unwrap_or_default()
    };
    // Try to find the content digest using the json mapping if the `image` specifies the metadata digest.
    let content_digest = if let Some(content_digest) = image
        .digest()
        .and_then(|metadata_digest| oci_cache_mapping.get(metadata_digest))
    {
        content_digest.to_owned()
    } else {
        info!("Fetching metadata for {image}");
        // Workaround for bug somewhere in `WasmClient::pull_manifest_and_config`
        // metadata digest must be removed from Reference, otherwise no matter
        // what the pulled image contains, we get the specified digest.
        let image_without_digest = Reference::with_tag(
            image.registry().to_string(),
            image.repository().to_string(),
            image.tag().unwrap_or("latest").to_string(),
        );
        let (_oci_config, wasm_config, metadata_digest) = client
            .pull_manifest_and_config(&image_without_digest, &auth)
            .await?;
        match image.digest() {
            None => info!("Consider adding metadata digest to component's `location.oci` configuration: {image}@{metadata_digest}"),
            Some(specified) => {
                debug!("Fetched metadata digest {metadata_digest}");
                if specified != metadata_digest {
                    bail!("metadata digest mismatch. Specified:\n{image_without_digest}@{specified}\nActually got:\n{image_without_digest}@{metadata_digest}")
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
        // Update the mapping and persist.
        oci_cache_mapping.insert(metadata_digest, content_digest.clone());
        let json = serde_json::to_string(&oci_cache_mapping)?;
        if let Err(err) = tokio::fs::write(&oci_cache_mapping_file, json.as_bytes()).await {
            warn!("Cannot write {oci_cache_mapping_file:?} - {err:?}");
        }
        content_digest
    };
    let wasm_path = wasm_cache_dir.join(PathBuf::from(format!(
        "{hash_type}_{content_digest}.wasm",
        hash_type = content_digest.hash_type,
        content_digest = content_digest.hash_base16,
    )));
    // Do not download if the file exists and matches the expected sha256 digest.
    match wasm_workers::component_detector::file_hash(&wasm_path).await {
        Ok(actual) if actual == content_digest => {
            debug!("Found in cache {wasm_path:?}");
        }
        other => {
            if other.is_ok() {
                warn!("Wrong content digest, rewriting the cached file {wasm_path:?}");
            }
            info!("Pulling image to {wasm_path:?}");
            let data = client
                // FIXME: do not download all layers at once to memory, based on a size limit
                .pull(image, &auth)
                .await
                .with_context(|| format!("Unable to pull image {image}"))?;
            let data = data
                .layers
                .into_iter()
                .next()
                .expect("layer length asserted in WasmClient")
                .data;
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
    ensure!(content_digest == actual_hash,
        "sha256 digest mismatch for {image}, file {wasm_path:?}. Expected {content_digest}, got {actual_hash}");
    Ok((actual_hash, wasm_path))
}

fn get_oci_auth(
    reference: &Reference,
) -> Result<oci_distribution::secrets::RegistryAuth, anyhow::Error> {
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

pub(crate) async fn push(file: &PathBuf, reference: &Reference) -> Result<(), anyhow::Error> {
    if reference.digest().is_some() {
        bail!("cannot push a digest reference");
    }
    let client = get_client();
    let (conf, layer) = WasmConfig::from_component(file, None)
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
