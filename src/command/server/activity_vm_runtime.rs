use activity_vm_runner::RuntimeSource;
use anyhow::{Context as _, ensure};
use concepts::ContentDigest;
#[cfg(debug_assertions)]
use concepts::component_id::Digest;
use embedded_assets::{ACTIVITY_VM_BOCHS_WASM_RUNTIME_LOCATION, ACTIVITY_VM_QEMU_RUNTIME_LOCATION};
use oci_client::Reference;
#[cfg(debug_assertions)]
use sha2::{Digest as _, Sha256};
use std::path::Path;
#[cfg(debug_assertions)]
use std::path::PathBuf;
use std::str::FromStr as _;

use super::ActivityVmRuntimeMode;

const RUNTIME_ARTIFACT_KIND: &str = "activity-vm-runtime.v1";

pub(crate) async fn fetch(
    cache_root: &Path,
    mode: ActivityVmRuntimeMode,
) -> anyhow::Result<Option<RuntimeSource>> {
    match mode {
        ActivityVmRuntimeMode::Disabled => Ok(None),
        ActivityVmRuntimeMode::QemuTcg => {
            #[cfg(debug_assertions)]
            if let Some(bundle) = std::env::var_os("OBELISK_NATIVE_QEMU_BUNDLE").map(PathBuf::from)
            {
                tracing::warn!("Overriding native QEMU bundle with {bundle:?}");
                ensure!(bundle.is_dir(), "local native QEMU bundle is missing");
                let mut hasher = Sha256::new();
                for file in ["vm.state", "guest/machine.json", "qemu-path"] {
                    let digest = utils::sha256sum::calculate_sha256_file(bundle.join(file)).await?;
                    hasher.update(digest.0.0);
                }
                let digest = ContentDigest(Digest(hasher.finalize().into()));
                return Ok(Some(RuntimeSource::QemuNative { bundle, digest }));
            }
            let reference = Reference::from_str(
                ACTIVITY_VM_QEMU_RUNTIME_LOCATION
                    .trim()
                    .strip_prefix("oci://")
                    .context("native QEMU runtime reference must start with `oci://`")?,
            )?;
            ensure!(
                reference.digest().is_some(),
                "native QEMU runtime OCI reference must be pinned by manifest digest"
            );
            let digest = ContentDigest::from_str(reference.digest().unwrap())?;
            let bundle =
                crate::oci::pull_native_qemu_bundle_to_cache(&reference, cache_root).await?;
            Ok(Some(RuntimeSource::QemuNative { bundle, digest }))
        }
        ActivityVmRuntimeMode::BochsWasm => {
            #[cfg(debug_assertions)]
            if let Some(module) =
                std::env::var_os("OBELISK_ACTIVITY_VM_RUNTIME_MODULE").map(PathBuf::from)
            {
                tracing::warn!("Overriding activity-vm-runtime with {module:?}");
                ensure!(module.is_file(), "local activity VM module is missing");
                return Ok(Some(RuntimeSource::BochsWasm(module)));
            }
            let reference = Reference::from_str(
                ACTIVITY_VM_BOCHS_WASM_RUNTIME_LOCATION
                    .trim()
                    .strip_prefix("oci://")
                    .context("activity VM runtime reference must start with `oci://`")?,
            )?;
            ensure!(
                reference.digest().is_some(),
                "activity VM runtime OCI reference must be pinned by manifest digest"
            );
            let runtime_dir = cache_root.join("activity-vm").join("runtimes");
            let (_, cached_path) = crate::oci::pull_wasm_module_to_cache(
                &reference,
                RUNTIME_ARTIFACT_KIND,
                &runtime_dir,
            )
            .await?;
            Ok(Some(RuntimeSource::BochsWasm(cached_path)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use wasm_workers::engines::{EngineConfig, Engines};

    #[tokio::test]
    async fn populate_activity_vm_codegen_cache() {
        test_utils::set_up();
        let workspace = PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap());
        let runtime = fetch(
            &workspace.join("test-wasm-cache"),
            ActivityVmRuntimeMode::BochsWasm,
        )
        .await
        .unwrap()
        .unwrap();
        let engine =
            Engines::get_activity_vm_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let RuntimeSource::BochsWasm(wasm) = runtime else {
            unreachable!()
        };
        activity_vm_runner::compile(&engine, &wasm).unwrap();
    }

    #[tokio::test]
    async fn populate_activity_vm_qemu_cache() {
        test_utils::set_up();
        let workspace = PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap());
        fetch(
            &workspace.join("test-wasm-cache"),
            ActivityVmRuntimeMode::QemuTcg,
        )
        .await
        .unwrap();
    }
}
