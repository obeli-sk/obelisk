use anyhow::{Context as _, ensure};
use concepts::{ContentDigest, component_id::Digest};
use oci_client::Reference;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;

pub(crate) const RUNTIME_SHA256: &str =
    "04c542a0826e4280210874dadde8029be7810af7003c7fa6de5dd13eb531ea4f";
const RUNTIME_LOCATION: &str = embedded_assets::ACTIVITY_VM_RUNTIME_LOCATION;

pub(crate) async fn fetch(cache_root: &Path) -> anyhow::Result<PathBuf> {
    if let Some(path) = std::env::var_os("OBELISK_ACTIVITY_VM_RUNTIME") {
        let path = PathBuf::from(path);
        ensure!(
            verified_file(&path, RUNTIME_SHA256).await?,
            "OBELISK_ACTIVITY_VM_RUNTIME does not match the pinned runtime digest"
        );
        return Ok(path);
    }
    let runtime_dir = cache_root.join("activity-vm").join("runtimes");
    tokio::fs::create_dir_all(&runtime_dir).await?;
    let destination = runtime_dir.join(format!("sha256_{RUNTIME_SHA256}.wasm"));
    if verified_file(&destination, RUNTIME_SHA256).await? {
        return Ok(destination);
    }

    let reference = Reference::from_str(
        RUNTIME_LOCATION
            .trim()
            .strip_prefix("oci://")
            .context("activity VM runtime reference must start with `oci://`")?,
    )?;
    let content_digest = ContentDigest(Digest::from_str(&format!("sha256:{RUNTIME_SHA256}"))?);
    crate::oci::pull_artifact_layer_to_cache(
        &reference,
        "application/wasm",
        &content_digest,
        &destination,
    )
    .await?;
    Ok(destination)
}

async fn verified_file(path: &Path, expected: &str) -> anyhow::Result<bool> {
    let digest = ContentDigest(Digest::from_str(&format!("sha256:{expected}"))?);
    Ok(crate::oci::verify_cached_file(path, &digest).await.is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_an_unverified_cached_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.wasm");
        tokio::fs::write(&path, b"not the appliance").await.unwrap();
        assert!(!verified_file(&path, RUNTIME_SHA256).await.unwrap());
    }
}
