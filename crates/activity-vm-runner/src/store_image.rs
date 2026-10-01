use super::MapDir;
use anyhow::{Context as _, ensure};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use wasmtime_wasi::FsPerms;

type ImageKey = (u64, Vec<(PathBuf, String)>);

/// Mappings of a deployed activity do not change, so each distinct set is packed once per process.
static STORE_IMAGES: std::sync::LazyLock<
    tokio::sync::Mutex<HashMap<ImageKey, Arc<tempfile::TempDir>>>,
> = std::sync::LazyLock::new(Default::default);

pub(super) async fn store_image(
    mkfs_erofs: &Path,
    mapdirs: &[MapDir],
) -> anyhow::Result<Arc<tempfile::TempDir>> {
    store_image_with_size(mkfs_erofs, mapdirs, 0).await
}

pub(super) async fn store_image_with_size(
    mkfs_erofs: &Path,
    mapdirs: &[MapDir],
    fixed_size: u64,
) -> anyhow::Result<Arc<tempfile::TempDir>> {
    let key = (
        fixed_size,
        mapdirs
            .iter()
            .map(|mapping| (mapping.host.clone(), mapping.guest.clone()))
            .collect::<Vec<_>>(),
    );
    let mut images = STORE_IMAGES.lock().await;
    if let Some(image) = images.get(&key) {
        return Ok(image.clone());
    }
    let dir = Arc::new(tempfile::tempdir()?);
    build_store_image(mkfs_erofs, mapdirs, &dir.path().join("store.img")).await?;
    if fixed_size != 0 {
        let image = dir.path().join("store.img");
        let file = std::fs::OpenOptions::new().write(true).open(&image)?;
        ensure!(
            file.metadata()?.len() <= fixed_size,
            "EROFS closure image exceeds the QEMU drive's fixed size"
        );
        file.set_len(fixed_size)?;
    }
    images.insert(key, dir.clone());
    Ok(dir)
}

/// Packs the mappings into a read-only erofs image that the guest mounts as its `/share`.
async fn build_store_image(
    mkfs_erofs: &Path,
    mapdirs: &[MapDir],
    image: &Path,
) -> anyhow::Result<()> {
    // Hardlinks need the staging directory on the same filesystem as the store.
    let share = match mapdirs.first().and_then(|mapping| mapping.host.parent()) {
        Some(parent) => tempfile::tempdir_in(parent)?,
        None => tempfile::tempdir()?,
    };
    for mapping in mapdirs {
        install_mapping(share.path(), mapping).await?;
    }
    tokio::fs::create_dir_all(share.path().join("nix/store")).await?;
    let output = tokio::process::Command::new(mkfs_erofs)
        .args(["--all-root", "-T0"])
        .arg(image)
        .arg(share.path())
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("starting {}", mkfs_erofs.display()))?;
    ensure!(
        output.status.success(),
        "mkfs.erofs failed with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

pub(super) async fn install_mapping(share: &Path, mapping: &MapDir) -> anyhow::Result<()> {
    ensure!(
        mapping.guest.starts_with('/') && !mapping.guest.split('/').any(|part| part == ".."),
        "unsafe activity VM guest mapping: {}",
        mapping.guest
    );
    let destination = share.join(mapping.guest.trim_start_matches('/'));
    tokio::fs::create_dir_all(destination.parent().context("mapping has no parent")?).await?;
    let source = mapping.host.clone();
    tokio::task::spawn_blocking(move || {
        let status = std::process::Command::new("cp")
            .args(["-a", "-l", "--"])
            .arg(&source)
            .arg(&destination)
            .status()?;
        ensure!(
            status.success(),
            "copying activity VM mapping {} failed: {status}",
            source.display()
        );
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        mapping.permissions == FsPerms::ReadOnly,
        "activity VM supports writable files only in its mailbox"
    );
    Ok(())
}
