use anyhow::{Context as _, ensure};
use concepts::{ContentDigest, component_id::Digest};
use oci_client::{Reference, manifest::OciManifest};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr as _;

#[derive(Clone, Debug)]
pub(crate) enum ActivityVmRuntime {
    WasmModule(PathBuf),
    QemuBundle(QemuRuntimeBundle),
}

impl ActivityVmRuntime {
    pub(crate) fn module(&self) -> &Path {
        match self {
            Self::WasmModule(path) => path,
            Self::QemuBundle(bundle) => &bundle.module,
        }
    }

    pub(crate) fn qemu_config(
        &self,
    ) -> anyhow::Result<Option<activity_vm_runner::QemuRuntimeConfig>> {
        match self {
            Self::WasmModule(_) => Ok(None),
            Self::QemuBundle(bundle) => Ok(Some(activity_vm_runner::QemuRuntimeConfig {
                args: serde_json::from_slice(&std::fs::read(&bundle.args)?)
                    .context("QEMU runtime args.json must be a JSON string array")?,
                image_dir: bundle.image_dir.clone(),
            })),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QemuRuntimeBundle {
    module: PathBuf,
    args: PathBuf,
    image_dir: PathBuf,
}

const RUNTIME_LOCATION: &str = embedded_assets::ACTIVITY_VM_RUNTIME_LOCATION;
const RUNTIME_ARTIFACT_KIND: &str = "activity-vm-runtime.v1";
const TITLE: &str = "org.opencontainers.image.title";

pub(crate) async fn fetch(cache_root: &Path) -> anyhow::Result<ActivityVmRuntime> {
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

    let auth = crate::oci::get_oci_auth(&reference)?;
    let client = oci_client::Client::default();
    let (manifest, actual_digest) = client.pull_manifest(&reference, &auth).await?;
    ensure!(
        reference.digest() == Some(actual_digest.as_str()),
        "registry returned manifest {actual_digest}, expected {}",
        reference.digest().unwrap()
    );
    let is_qemu_bundle = match manifest {
        OciManifest::Image(ref manifest) => manifest.layers.iter().any(|layer| {
            layer
                .annotations
                .as_ref()
                .and_then(|a| a.get(TITLE))
                .is_some_and(|title| title == "qemu-system-x86_64.wasm")
        }),
        OciManifest::ImageIndex(_) => false,
    };
    if is_qemu_bundle {
        return fetch_qemu_bundle(&reference, cache_root, manifest, &client)
            .await
            .map(ActivityVmRuntime::QemuBundle);
    }

    let runtime_dir = cache_root.join("activity-vm").join("runtimes");
    let (_, cached_path) =
        crate::oci::pull_wasm_module_to_cache(&reference, RUNTIME_ARTIFACT_KIND, &runtime_dir)
            .await?;
    Ok(ActivityVmRuntime::WasmModule(cached_path))
}

async fn fetch_qemu_bundle(
    reference: &Reference,
    cache_root: &Path,
    manifest: OciManifest,
    client: &oci_client::Client,
) -> anyhow::Result<QemuRuntimeBundle> {
    let manifest_digest = reference.digest().expect("validated above");
    let final_dir = cache_root
        .join("activity-vm/qemu-runtimes")
        .join(manifest_digest.replace(':', "_"));
    if final_dir.join(".complete").is_file() {
        verify_sums(&final_dir)?;
        return Ok(bundle_paths(final_dir));
    }
    let OciManifest::Image(manifest) = manifest else {
        anyhow::bail!("QEMU runtime must not be an OCI index")
    };
    ensure!(
        manifest.artifact_type.as_deref() == Some("application/wasm"),
        "invalid QEMU artifact type"
    );
    ensure!(
        manifest.config.media_type == "application/vnd.oci.empty.v1+json",
        "invalid QEMU config media type"
    );
    let specs = [
        (
            "qemu-system-x86_64.wasm",
            "application/wasm",
            "qemu-system-x86_64.wasm",
        ),
        ("args.json", "application/json", "args.json"),
        (
            "image",
            "application/vnd.oci.image.layer.v1.tar",
            "image.tar.gz",
        ),
        ("SHA256SUMS", "text/plain", "SHA256SUMS"),
    ];
    ensure!(
        manifest.layers.len() == specs.len(),
        "QEMU bundle must have exactly four layers"
    );
    let parent = final_dir
        .parent()
        .context("QEMU runtime cache path has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let staging = tempfile::Builder::new()
        .prefix(".qemu-runtime-")
        .tempdir_in(parent)?;
    for (title, media_type, filename) in specs {
        let matches = manifest
            .layers
            .iter()
            .filter(|descriptor| {
                descriptor.media_type == media_type
                    && descriptor
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get(TITLE))
                        .is_some_and(|value| value == title)
            })
            .collect::<Vec<_>>();
        ensure!(
            matches.len() == 1,
            "QEMU bundle requires exactly one {title} layer"
        );
        let digest = ContentDigest(Digest::from_str(&matches[0].digest)?);
        crate::oci::pull_blob_to_file(
            client,
            reference,
            &staging.path().join(filename),
            matches[0],
            &digest,
            "QEMU runtime layer",
        )
        .await?;
    }
    let decoder = flate2::read::GzDecoder::new(std::fs::File::open(
        staging.path().join("image.tar.gz"),
    )?);
    let unpacked = staging.path().join("unpacked");
    super::qemu_bundle_cache::publish_image_layer(decoder, &unpacked)?;
    std::fs::rename(unpacked.join("image"), staging.path().join("image"))?;
    std::fs::remove_dir(unpacked)?;
    std::fs::remove_file(staging.path().join("image.tar.gz"))?;
    verify_sums(staging.path())?;
    std::fs::write(staging.path().join(".complete"), manifest_digest)?;
    let staging_path = staging.keep();
    match std::fs::rename(&staging_path, &final_dir) {
        Ok(()) => {}
        Err(_) if final_dir.join(".complete").is_file() => {
            verify_sums(&final_dir)?;
            std::fs::remove_dir_all(staging_path)?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(bundle_paths(final_dir))
}

fn bundle_paths(root: PathBuf) -> QemuRuntimeBundle {
    QemuRuntimeBundle {
        module: root.join("qemu-system-x86_64.wasm"),
        args: root.join("args.json"),
        image_dir: root.join("image"),
    }
}

fn verify_sums(root: &Path) -> anyhow::Result<()> {
    use sha2::{Digest as _, Sha256};
    let sums = std::fs::read_to_string(root.join("SHA256SUMS"))?;
    let mut declared = BTreeSet::new();
    for line in sums.lines() {
        let (expected, relative) = line.split_once("  ").context("invalid SHA256SUMS line")?;
        let relative = Path::new(relative);
        ensure!(
            !relative.is_absolute()
                && relative
                    .components()
                    .all(|part| matches!(part, Component::Normal(_))),
            "unsafe SHA256SUMS path {relative:?}"
        );
        ensure!(
            expected.len() == 64 && expected.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid SHA256SUMS digest"
        );
        ensure!(
            declared.insert(relative.to_owned()),
            "duplicate SHA256SUMS path {relative:?}"
        );
        let bytes = std::fs::read(root.join(relative))
            .with_context(|| format!("cannot read declared QEMU runtime file {relative:?}"))?;
        ensure!(
            format!("{:x}", Sha256::digest(bytes)) == expected.to_ascii_lowercase(),
            "checksum mismatch for {relative:?}"
        );
    }
    let mut actual = BTreeSet::new();
    collect_files(root, root, &mut actual)?;
    actual.remove(Path::new("SHA256SUMS"));
    actual.remove(Path::new(".complete"));
    ensure!(
        declared == actual,
        "SHA256SUMS file list does not match QEMU runtime contents"
    );
    Ok(())
}

fn collect_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeSet<PathBuf>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        ensure!(
            !file_type.is_symlink(),
            "QEMU runtime contains a symbolic link"
        );
        if file_type.is_dir() {
            collect_files(root, &entry.path(), files)?;
        } else if file_type.is_file() {
            files.insert(entry.path().strip_prefix(root)?.to_owned());
        } else {
            anyhow::bail!("QEMU runtime contains a special file");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_workers::engines::{EngineConfig, Engines};

    #[tokio::test]
    async fn populate_activity_vm_codegen_cache() {
        test_utils::set_up();
        let workspace = PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap());
        let runtime = fetch(&workspace.join("test-wasm-cache"))
            .await
            .unwrap_or_else(|error| panic!("{error:#}"));
        let engine =
            Engines::get_activity_vm_engine_test(EngineConfig::on_demand_testing()).unwrap();
        activity_vm_runner::compile(&engine, runtime.module()).unwrap();
    }
}
