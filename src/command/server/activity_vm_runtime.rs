use anyhow::{Context as _, ensure};
use oci_client::Reference;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;

const RUNTIME_LOCATION: &str = embedded_assets::ACTIVITY_VM_RUNTIME_LOCATION;

pub(crate) async fn fetch(cache_root: &Path) -> anyhow::Result<PathBuf> {
    let reference = Reference::from_str(
        RUNTIME_LOCATION
            .trim()
            .strip_prefix("oci://")
            .context("activity VM runtime reference must start with `oci://`")?,
    )?;
    ensure!(
        reference.digest().is_some(),
        "activity VM runtime OCI reference must be pinned by manifest digest"
    );
    if let Some(path) = std::env::var_os("OBELISK_ACTIVITY_VM_RUNTIME") {
        let path = PathBuf::from(path);
        crate::oci::verify_artifact_layer(&reference, "application/wasm", &path)
            .await
            .context(
                "OBELISK_ACTIVITY_VM_RUNTIME does not match the runtime layer in the pinned OCI manifest",
            )?;
        Ok(path)
    } else {
        let runtime_dir = cache_root.join("activity-vm").join("runtimes");
        let (_, cached_path) =
            crate::oci::pull_artifact_layer_to_cache(&reference, "application/wasm", &runtime_dir)
                .await?;
        Ok(cached_path)
    }
}
