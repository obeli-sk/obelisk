use anyhow::{Context as _, ensure};
use oci_client::Reference;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;

const RUNTIME_LOCATION: &str = embedded_assets::ACTIVITY_VM_RUNTIME_LOCATION;
const RUNTIME_ARTIFACT_KIND: &str = "activity-vm-runtime.v1";

pub(crate) async fn fetch(cache_root: &Path) -> anyhow::Result<PathBuf> {
    let reference = Reference::from_str(
        RUNTIME_LOCATION
            .strip_prefix("oci://")
            .context("activity VM runtime reference must start with `oci://`")?,
    )?;
    ensure!(
        reference.digest().is_some(),
        "activity VM runtime OCI reference must be pinned by manifest digest"
    );
    let runtime_dir = cache_root.join("activity-vm").join("runtimes");
    let (_, cached_path) =
        crate::oci::pull_wasm_module_to_cache(&reference, RUNTIME_ARTIFACT_KIND, &runtime_dir)
            .await?;
    Ok(cached_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_workers::engines::{EngineConfig, Engines};

    #[tokio::test]
    async fn populate_activity_vm_codegen_cache() {
        test_utils::set_up();
        let workspace = PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap());
        let runtime = fetch(&workspace.join("test-wasm-cache")).await.unwrap();
        let engine =
            Engines::get_activity_vm_engine_test(EngineConfig::on_demand_testing()).unwrap();
        activity_vm_runner::compile(&engine, &runtime).unwrap();
    }
}
