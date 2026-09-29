use activity_vm_runner::RuntimeSource;
use anyhow::{Context as _, ensure};
use concepts::ContentDigest;
#[cfg(debug_assertions)]
use concepts::component_id::Digest;
use embedded_assets::{
    ACTIVITY_VM_BOCHS_WASM_RUNTIME_LOCATION, ACTIVITY_VM_FIRECRACKER_RUNTIME_LOCATION,
    ACTIVITY_VM_QEMU_KVM_RUNTIME_LOCATION, ACTIVITY_VM_QEMU_TCG_RUNTIME_LOCATION,
};
use oci_client::Reference;
#[cfg(debug_assertions)]
use sha2::{Digest as _, Sha256};
use std::path::Path;
use std::str::FromStr as _;

use super::ActivityVmRuntimeMode;

const RUNTIME_ARTIFACT_KIND: &str = "activity-vm-runtime.v1";

pub(crate) async fn fetch(
    cache_root: &Path,
    mode: ActivityVmRuntimeMode,
) -> anyhow::Result<Option<RuntimeSource>> {
    #[cfg(not(target_os = "linux"))]
    match mode {
        ActivityVmRuntimeMode::Firecracker => {
            anyhow::bail!("Firecracker is only supported on Linux")
        }
        ActivityVmRuntimeMode::QemuKvm => anyhow::bail!("QEMU KVM is only supported on Linux"),
        _ => {}
    }

    match mode {
        ActivityVmRuntimeMode::Disabled => Ok(None),
        ActivityVmRuntimeMode::Firecracker => {
            #[cfg(debug_assertions)]
            if let Some(bundle) =
                std::env::var_os("OBELISK_FIRECRACKER_BUNDLE").map(std::path::PathBuf::from)
            {
                tracing::warn!("Overriding Firecracker bundle with {bundle:?}");
                ensure!(bundle.is_dir(), "local Firecracker bundle is missing");
                check_firecracker_on_path(&bundle).await?;
                let mut hasher = Sha256::new();
                for file in [
                    "guest/vmlinux",
                    "guest/initramfs.cpio.gz",
                    "guest/machine.json",
                ] {
                    let digest = utils::sha256sum::calculate_sha256_file(bundle.join(file)).await?;
                    hasher.update(digest.0.0);
                }
                let digest = ContentDigest(Digest(hasher.finalize().into()));
                return Ok(Some(RuntimeSource::Firecracker { bundle, digest }));
            }
            let location = ACTIVITY_VM_FIRECRACKER_RUNTIME_LOCATION.trim();
            ensure!(
                !location.is_empty(),
                "the Firecracker runtime has no published release yet"
            );
            let reference = Reference::from_str(
                location
                    .strip_prefix("oci://")
                    .context("Firecracker runtime reference must start with `oci://`")?,
            )?;
            let digest =
                ContentDigest::from_str(reference.digest().context(
                    "Firecracker runtime OCI reference must be pinned by manifest digest",
                )?)?;
            let bundle =
                crate::oci::pull_firecracker_bundle_to_cache(&reference, cache_root).await?;
            check_firecracker_on_path(&bundle).await?;
            Ok(Some(RuntimeSource::Firecracker { bundle, digest }))
        }
        ActivityVmRuntimeMode::QemuTcg | ActivityVmRuntimeMode::QemuKvm => {
            let (location, accelerator) = match mode {
                ActivityVmRuntimeMode::QemuTcg => (ACTIVITY_VM_QEMU_TCG_RUNTIME_LOCATION, "tcg"),
                ActivityVmRuntimeMode::QemuKvm => (ACTIVITY_VM_QEMU_KVM_RUNTIME_LOCATION, "kvm"),
                _ => unreachable!(),
            };
            #[cfg(debug_assertions)]
            if let Some(bundle) =
                std::env::var_os("OBELISK_NATIVE_QEMU_BUNDLE").map(std::path::PathBuf::from)
            {
                tracing::warn!("Overriding native QEMU bundle with {bundle:?}");
                ensure!(bundle.is_dir(), "local native QEMU bundle is missing");
                check_native_qemu_on_path(&bundle, accelerator).await?;
                let mut hasher = Sha256::new();
                for file in ["vm.state", "guest/machine.json"] {
                    let digest = utils::sha256sum::calculate_sha256_file(bundle.join(file)).await?;
                    hasher.update(digest.0.0);
                }
                let digest = ContentDigest(Digest(hasher.finalize().into()));
                return Ok(Some(RuntimeSource::QemuNative { bundle, digest }));
            }
            ensure!(
                !location.trim().is_empty(),
                "native QEMU {accelerator} runtime reference is empty"
            );
            let reference = Reference::from_str(
                location
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
                crate::oci::pull_native_qemu_bundle_to_cache(&reference, cache_root, accelerator)
                    .await?;
            check_native_qemu_on_path(&bundle, accelerator).await?;
            Ok(Some(RuntimeSource::QemuNative { bundle, digest }))
        }
        ActivityVmRuntimeMode::BochsWasm => {
            #[cfg(debug_assertions)]
            if let Some(module) =
                std::env::var_os("OBELISK_ACTIVITY_VM_RUNTIME_MODULE").map(std::path::PathBuf::from)
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

async fn check_native_qemu_on_path(bundle: &Path, accelerator: &str) -> anyhow::Result<()> {
    let machine: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(bundle.join("guest/machine.json")).await?)?;
    let args = machine["args"]
        .as_array()
        .context("native QEMU machine has no arguments")?;
    let configured_accelerator = args
        .windows(2)
        .find(|pair| pair[0] == "-accel")
        .and_then(|pair| pair[1].as_str())
        .context("native QEMU machine has no accelerator")?;
    ensure!(
        configured_accelerator.split(',').next() == Some(accelerator),
        "native QEMU bundle uses {configured_accelerator}, expected {accelerator}"
    );
    if accelerator == "kvm" {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/kvm")
            .context("KVM requires read/write access to /dev/kvm")?;
    }
    let actual = qemu_version_on_path().await?;
    let expected = match tokio::fs::read_to_string(bundle.join("qemu-version.txt")).await {
        Ok(version) => version,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!("Native QEMU bundle has no qemu-version.txt");
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    if actual != expected.trim() {
        tracing::warn!(
            "Native QEMU version mismatch: snapshot expects {}, executable on PATH reports {actual}",
            expected.trim()
        );
    }
    Ok(())
}

async fn check_firecracker_on_path(bundle: &Path) -> anyhow::Result<()> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .context("Firecracker requires read/write access to /dev/kvm")?;
    let actual = first_line_of_version("firecracker").await?;
    let expected = tokio::fs::read_to_string(bundle.join("firecracker-version.txt")).await?;
    // Every VM cold boots, so a different Firecracker only needs to support the same config.
    if actual != expected.trim() {
        tracing::warn!(
            "Firecracker version mismatch: bundle was tested with {}, PATH has {actual}",
            expected.trim()
        );
    }
    first_line_of_version("mkfs.erofs").await?;
    Ok(())
}

async fn first_line_of_version(program: &str) -> anyhow::Result<String> {
    let output = tokio::process::Command::new(program)
        .arg("--version")
        .output()
        .await
        .with_context(|| format!("{program} must be available on PATH"))?;
    ensure!(
        output.status.success(),
        "{program} --version exited with {}",
        output.status
    );
    String::from_utf8(output.stdout)?
        .lines()
        .next()
        .map(str::to_owned)
        .with_context(|| format!("{program} --version printed nothing"))
}

async fn qemu_version_on_path() -> anyhow::Result<String> {
    let output = tokio::process::Command::new("qemu-system-x86_64")
        .arg("--version")
        .output()
        .await
        .context("qemu-system-x86_64 must be available on PATH")?;
    ensure!(
        output.status.success(),
        "QEMU --version exited with {}",
        output.status
    );
    String::from_utf8(output.stdout)?
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("QEMU emulator version "))
        .map(str::to_owned)
        .context("unexpected QEMU --version output")
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
    async fn populate_activity_vm_firecracker_cache() {
        test_utils::set_up();
        let workspace = PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap());
        let Some(RuntimeSource::Firecracker { bundle, .. }) = fetch(
            &workspace.join("test-wasm-cache"),
            ActivityVmRuntimeMode::Firecracker,
        )
        .await
        .unwrap() else {
            unreachable!()
        };
        assert!(bundle.join("guest/vmlinux").is_file());
    }

    #[tokio::test]
    async fn populate_activity_vm_qemu_cache() {
        test_utils::set_up();
        let workspace = PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap());
        let mode = if std::env::var("OBELISK_UNSTABLE_ACTIVITY_VM").as_deref() == Ok("qemu-kvm") {
            ActivityVmRuntimeMode::QemuKvm
        } else {
            ActivityVmRuntimeMode::QemuTcg
        };
        let Some(RuntimeSource::QemuNative { bundle, .. }) =
            fetch(&workspace.join("test-wasm-cache"), mode)
                .await
                .unwrap()
        else {
            unreachable!()
        };
        let expected = tokio::fs::read_to_string(bundle.join("qemu-version.txt"))
            .await
            .unwrap();
        assert_eq!(
            expected.trim(),
            qemu_version_on_path().await.unwrap(),
            "QEMU on PATH must match the version the pinned snapshot was built with"
        );
    }
}
