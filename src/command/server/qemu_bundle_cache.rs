use anyhow::{Context as _, ensure};
use std::{
    collections::HashSet,
    io::Read,
    path::{Component, Path, PathBuf},
};

/// Extract an OCI image layer into a private sibling directory and publish it
/// atomically only after the complete archive has been validated.
pub(crate) fn publish_image_layer<R: Read>(reader: R, destination: &Path) -> anyhow::Result<()> {
    const MAX_ENTRIES: usize = 4096;
    const MAX_UNPACKED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
    let parent = destination
        .parent()
        .context("QEMU image destination must have a parent directory")?;
    ensure!(
        destination.file_name().is_some(),
        "QEMU image destination must name a directory"
    );
    std::fs::create_dir_all(parent).context("cannot create QEMU image cache directory")?;

    let staging = tempfile::Builder::new()
        .prefix(".qemu-image-")
        .tempdir_in(parent)
        .context("cannot create QEMU image staging directory")?;
    let mut archive = tar::Archive::new(reader);
    archive.set_preserve_ownerships(false);
    archive.set_preserve_permissions(false);
    let mut paths = HashSet::<PathBuf>::new();
    let mut unpacked_bytes = 0_u64;

    for entry in archive
        .entries()
        .context("cannot read QEMU image archive")?
    {
        let mut entry = entry.context("cannot read QEMU image archive entry")?;
        ensure!(
            paths.len() < MAX_ENTRIES,
            "QEMU image archive has too many entries"
        );
        unpacked_bytes = unpacked_bytes
            .checked_add(entry.size())
            .context("QEMU image archive size overflow")?;
        ensure!(
            unpacked_bytes <= MAX_UNPACKED_BYTES,
            "QEMU image archive is too large"
        );
        let path = entry
            .path()
            .context("QEMU image archive entry has an invalid path")?
            .into_owned();
        ensure!(
            !path.as_os_str().is_empty()
                && path
                    .components()
                    .all(|component| matches!(component, Component::Normal(_))),
            "QEMU image archive contains unsafe path `{}`",
            path.display()
        );
        ensure!(
            paths.insert(path.clone()),
            "QEMU image archive contains duplicate path `{}`",
            path.display()
        );

        let kind = entry.header().entry_type();
        ensure!(
            kind.is_file() || kind.is_dir(),
            "QEMU image archive contains unsupported entry `{}`",
            path.display()
        );
        ensure!(
            entry
                .unpack_in(staging.path())
                .with_context(|| format!("cannot extract `{}`", path.display()))?,
            "QEMU image archive entry escapes destination: `{}`",
            path.display()
        );
    }

    let staging_path = staging.keep();
    if let Err(error) = std::fs::rename(&staging_path, destination) {
        let _ = std::fs::remove_dir_all(&staging_path);
        return Err(error).context("cannot atomically publish QEMU image directory");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tar::{Builder, EntryType, Header};

    fn append(builder: &mut Builder<Vec<u8>>, path: &str, contents: &[u8]) {
        let mut header = Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(contents.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, path, contents)
            .expect("append test archive entry");
    }

    fn archive_with_entry(kind: EntryType) -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());
        let mut header = Header::new_gnu();
        header.set_entry_type(kind);
        header.set_mode(0o644);
        header.set_size(0);
        if kind.is_symlink() || kind.is_hard_link() {
            header.set_link_name("target").unwrap();
        }
        header.set_cksum();
        builder
            .append_data(&mut header, "unsafe", std::io::empty())
            .unwrap();
        builder.into_inner().unwrap()
    }

    fn traversal_archive() -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());
        append(&mut builder, "safe-name", b"escaped");
        let mut bytes = builder.into_inner().unwrap();

        bytes[..100].fill(0);
        bytes[..9].copy_from_slice(b"../escape");
        bytes[148..156].fill(b' ');
        let checksum: u64 = bytes[..512].iter().map(|byte| u64::from(*byte)).sum();
        bytes[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
        bytes
    }

    #[test]
    fn publishes_valid_layer_atomically() {
        let cache = tempfile::tempdir().unwrap();
        let destination = cache.path().join("image");
        let mut builder = Builder::new(Vec::new());
        append(&mut builder, "bzImage", b"kernel");
        append(&mut builder, "firmware/rom", b"firmware");
        let bytes = builder.into_inner().unwrap();

        publish_image_layer(Cursor::new(bytes), &destination).unwrap();

        assert_eq!(
            std::fs::read(destination.join("bzImage")).unwrap(),
            b"kernel"
        );
        assert_eq!(
            std::fs::read(destination.join("firmware/rom")).unwrap(),
            b"firmware"
        );
    }

    #[test]
    fn rejects_traversal_without_publishing() {
        let cache = tempfile::tempdir().unwrap();
        let destination = cache.path().join("image");

        let error =
            publish_image_layer(Cursor::new(traversal_archive()), &destination).unwrap_err();

        assert!(error.to_string().contains("unsafe path"), "{error:#}");
        assert!(!destination.exists());
        assert!(!cache.path().join("escape").exists());
    }

    #[test]
    fn rejects_links_devices_and_fifos_without_publishing() {
        let kinds = [
            EntryType::symlink(),
            EntryType::hard_link(),
            EntryType::character_special(),
            EntryType::block_special(),
            EntryType::fifo(),
        ];
        for kind in kinds {
            let cache = tempfile::tempdir().unwrap();
            let destination = cache.path().join("image");

            let error = publish_image_layer(Cursor::new(archive_with_entry(kind)), &destination)
                .unwrap_err();

            assert!(error.to_string().contains("unsupported entry"), "{error:#}");
            assert!(!destination.exists());
        }
    }

    #[test]
    fn rejects_duplicate_paths_without_publishing() {
        let cache = tempfile::tempdir().unwrap();
        let destination = cache.path().join("image");
        let mut builder = Builder::new(Vec::new());
        append(&mut builder, "duplicate", b"first");
        append(&mut builder, "duplicate", b"second");
        let bytes = builder.into_inner().unwrap();

        let error = publish_image_layer(Cursor::new(bytes), &destination).unwrap_err();

        assert!(error.to_string().contains("duplicate path"), "{error:#}");
        assert!(!destination.exists());
    }
}
