use anyhow::Context as _;
use std::collections::BTreeSet;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const LOCK: &str = ".obelisk-cache.lock";
const PINS: &str = ".obelisk-cache-pins";

#[derive(Debug)]
pub(super) struct Lease {
    _file: File,
    path: PathBuf,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn lock_file(root: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(LOCK))
}

pub(crate) async fn lock_for_use(root: PathBuf) -> io::Result<File> {
    tokio::task::spawn_blocking(move || {
        let file = lock_file(&root)?;
        file.lock_shared()?;
        Ok(file)
    })
    .await
    .map_err(io::Error::other)?
}

pub(super) fn pin(root: &Path, paths: &BTreeSet<PathBuf>) -> anyhow::Result<Lease> {
    let directory = root.join(PINS);
    fs::create_dir_all(&directory)?;
    let path = directory.join(concepts::prefixed_ulid::DeploymentId::generate().to_string());
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    file.lock()?;
    let relative: BTreeSet<_> = paths
        .iter()
        .filter_map(|p| p.strip_prefix(root).ok())
        .collect();
    serde_json::to_writer(&mut file, &relative)?;
    Ok(Lease { _file: file, path })
}

fn live_pins(root: &Path) -> anyhow::Result<BTreeSet<PathBuf>> {
    let mut paths = BTreeSet::new();
    let directory = match fs::read_dir(root.join(PINS)) {
        Ok(directory) => Some(directory),
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => return Err(err.into()),
    };
    if let Some(directory) = directory {
        for entry in directory {
            let path = entry?.path();
            match File::open(&path) {
                Ok(file) => match file.try_lock() {
                    Ok(()) => match fs::remove_file(path) {
                        Ok(()) => {}
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                        Err(err) => return Err(err.into()),
                    },
                    Err(fs::TryLockError::WouldBlock) => {
                        paths.extend(
                            serde_json::from_reader::<_, BTreeSet<PathBuf>>(file)?
                                .into_iter()
                                .map(|p| root.join(p)),
                        );
                    }
                    Err(fs::TryLockError::Error(err)) => return Err(err.into()),
                },
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
    }
    Ok(paths)
}

#[derive(Clone, Copy, Default)]
pub(super) struct Stats {
    pub deleted_entries: u64,
    pub deleted_bytes: u64,
    pub remaining_bytes: u64,
}

struct Entry {
    path: PathBuf,
    bytes: u64,
    last_used: SystemTime,
    protected: bool,
}

pub(super) fn record_use(paths: &BTreeSet<PathBuf>) {
    let now = SystemTime::now();
    for path in paths {
        let result = fs::symlink_metadata(path).and_then(|metadata| {
            if metadata.is_symlink() {
                Ok(())
            } else {
                File::open(path)?.set_times(FileTimes::new().set_modified(now))
            }
        });
        match result {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => tracing::debug!(?path, %err, "Cannot update cache last-use timestamp"),
        }
    }
}

pub(super) fn sweep(root: &Path, max_size: u64) -> anyhow::Result<Stats> {
    let lock = lock_file(root).with_context(|| format!("cannot open cache GC lock in {root:?}"))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(fs::TryLockError::WouldBlock) => return Ok(Stats::default()),
        Err(fs::TryLockError::Error(err)) => return Err(err.into()),
    }
    let protected = live_pins(root).context("cannot determine live cache pins")?;
    let mut entries = Vec::new();
    collect(root, root, &protected, &mut entries)?;
    let mut stats = Stats {
        remaining_bytes: entries.iter().map(|e| e.bytes).sum(),
        ..Stats::default()
    };
    record_use(
        &entries
            .iter()
            .filter(|e| e.protected)
            .map(|e| e.path.clone())
            .collect(),
    );
    if stats.remaining_bytes > max_size {
        entries.sort_unstable_by(|a, b| {
            a.last_used
                .cmp(&b.last_used)
                .then_with(|| a.path.cmp(&b.path))
        });
        let target = max_size / 10 * 7 + max_size % 10 * 7 / 10;
        for entry in entries {
            if stats.remaining_bytes <= target {
                break;
            }
            if !entry.protected {
                let deletion = (|| -> io::Result<()> {
                    let metadata = fs::symlink_metadata(&entry.path)?;
                    if metadata.is_dir() {
                        fs::remove_dir_all(&entry.path)
                    } else {
                        fs::remove_file(&entry.path)
                    }
                })();
                if skip_unavailable(&entry.path, deletion.map_err(Into::into))?.is_some() {
                    stats.deleted_entries += 1;
                    stats.deleted_bytes += entry.bytes;
                    stats.remaining_bytes -= entry.bytes;
                }
            }
        }
    }
    Ok(stats)
}

fn skip_unavailable<T>(path: &Path, result: anyhow::Result<T>) -> anyhow::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(err)
            if err.downcast_ref::<io::Error>().is_some_and(|err| {
                matches!(
                    err.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::NotFound
                )
            }) =>
        {
            tracing::debug!(?path, %err, "Skipping unavailable cache entry");
            Ok(None)
        }
        Err(err) => Err(err).with_context(|| format!("cannot process cache entry {path:?}")),
    }
}

fn collect(
    root: &Path,
    directory: &Path,
    protected: &BTreeSet<PathBuf>,
    entries: &mut Vec<Entry>,
) -> anyhow::Result<()> {
    if let Some(children) =
        skip_unavailable(directory, fs::read_dir(directory).map_err(Into::into))?
    {
        for child in children {
            if let Some(child) = skip_unavailable(directory, child.map_err(Into::into))? {
                let path = child.path();
                skip_unavailable(&path, collect_entry(root, &path, protected, entries))?;
            }
        }
    }
    Ok(())
}

fn collect_entry(
    root: &Path,
    path: &Path,
    protected: &BTreeSet<PathBuf>,
    entries: &mut Vec<Entry>,
) -> anyhow::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    let relative = path.strip_prefix(root)?;
    if matches!(relative.to_str(), Some(LOCK | PINS)) {
    } else if metadata.is_dir()
        && matches!(
            relative.to_str(),
            Some(
                "metadata"
                    | "js"
                    | "exec"
                    | "activity-vm"
                    | "activity-vm/entrypoints"
                    | "activity-vm/runtimes"
                    | "activity-vm/native-qemu"
                    | "activity-vm/firecracker"
            )
        )
    {
        collect(root, path, protected, entries)?;
    } else {
        // The Nix store has a shared manifest and completion markers, so evict it atomically.
        let mut pinned = protected.iter().any(|p| p.starts_with(path));
        if metadata.is_file() && path.extension().is_some_and(|ext| ext == "txt") {
            match fs::read_to_string(path) {
                Ok(content) => {
                    if let Ok(digest) = content.trim().parse::<concepts::ContentDigest>() {
                        let prefix = digest.0.with_infix("_");
                        pinned |= protected.iter().any(|p| {
                            p.file_name()
                                .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
                        });
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::InvalidData => {}
                Err(err) => return Err(err.into()),
            }
        }
        entries.push(Entry {
            bytes: tree_size(path)?,
            last_used: metadata.modified()?,
            protected: pinned,
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn tree_size(path: &Path) -> io::Result<u64> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() {
        fs::read_dir(path)?.try_fold(0, |size, child| Ok(size + tree_size(&child?.path())?))
    } else {
        Ok(metadata.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn write(path: &Path, seconds: u64) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, [0; 10]).unwrap();
        File::open(path)
            .unwrap()
            .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(seconds)))
            .unwrap();
    }

    #[test]
    fn eviction_uses_last_use_and_preserves_active_files_above_limit() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let active = root.join("exec/active");
        let reused = root.join("js/reused.js");
        let stale = root.join("old.wasm");
        write(&active, 0);
        write(&reused, 1);
        write(&stale, 2);
        record_use(&BTreeSet::from([reused.clone()]));
        let _lease = pin(root, &BTreeSet::from([active.clone()])).unwrap();
        let stats = sweep(root, 29).unwrap();
        assert_eq!(stats.deleted_bytes, 10);
        assert!(active.exists());
        assert!(reused.exists());
        assert!(!stale.exists());
        let stats = sweep(root, 1).unwrap();
        assert_eq!(stats.remaining_bytes, 10);
        assert!(active.exists());
        assert!(!reused.exists());
    }

    #[test]
    fn timestamp_failure_does_not_prevent_recording_other_entries() {
        let temp = tempfile::tempdir().unwrap();
        let blocker = temp.path().join("a-file");
        let invalid = blocker.join("entry");
        let reused = temp.path().join("z-reused.wasm");
        write(&blocker, 0);
        write(&reused, 0);
        assert!(fs::symlink_metadata(&invalid).is_err());
        record_use(&BTreeSet::from([invalid, reused.clone()]));
        assert!(fs::metadata(reused).unwrap().modified().unwrap() > UNIX_EPOCH);
    }

    #[test]
    #[cfg(unix)]
    fn permission_denied_entries_do_not_block_other_evictions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let unreadable = root.join("exec");
        let bundle = root.join("bundle/private");
        let undeletable = root.join("js");
        let mapping = root.join("metadata/unreadable.txt");
        let free = root.join("free.wasm");
        write(&unreadable.join("script"), 0);
        write(&bundle.join("image"), 0);
        write(&undeletable.join("script.js"), 0);
        write(&mapping, 0);
        write(&free, 0);
        for path in [&unreadable, &bundle, &mapping] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o000)).unwrap();
        }
        fs::set_permissions(&undeletable, fs::Permissions::from_mode(0o555)).unwrap();
        let permissions_enforced = fs::read_dir(&unreadable).is_err();
        let result = if permissions_enforced {
            Some(sweep(root, 0))
        } else {
            None
        };
        for path in [&unreadable, &bundle, &undeletable] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        fs::set_permissions(&mapping, fs::Permissions::from_mode(0o644)).unwrap();
        if let Some(result) = result {
            let stats = result.unwrap();
            assert_eq!(stats.deleted_entries, 1);
            assert_eq!(stats.deleted_bytes, 10);
            assert_eq!(stats.remaining_bytes, 10);
            assert!(!free.exists());
            assert!(unreadable.join("script").exists());
            assert!(bundle.join("image").exists());
            assert!(undeletable.join("script.js").exists());
            assert!(mapping.exists());
        } else {
            eprintln!(
                "Permission checks bypassed by the current user; skipping permission assertions"
            );
        }
    }

    #[test]
    fn active_metadata_and_whole_vm_artifacts_are_protected() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let digest = concepts::ContentDigest(concepts::component_id::Digest([1; 32]));
        let wasm = root.join(format!("{}.wasm", digest.0.with_infix("_")));
        write(&wasm, 0);
        let mapping = root.join("metadata/mapping.txt");
        fs::create_dir_all(mapping.parent().unwrap()).unwrap();
        fs::write(&mapping, digest.to_string()).unwrap();
        let runtime = root.join("activity-vm/native-qemu/old/guest/machine.json");
        let store = root.join("activity-vm/store/package/bin/tool");
        write(&runtime, 0);
        write(&store, 0);
        let manifest = root.join("activity-vm/store/.obelisk-activity-vm-manifest");
        write(&manifest, 0);
        let lease = pin(root, &BTreeSet::from([wasm.clone(), store.clone()])).unwrap();
        sweep(root, 0).unwrap();
        assert!(wasm.exists());
        assert!(mapping.exists());
        assert!(store.exists());
        assert!(manifest.exists());
        assert!(!root.join("activity-vm/native-qemu/old").exists());
        drop(lease);
        sweep(root, 0).unwrap();
        assert!(!root.join("activity-vm/store").exists());
    }

    #[cfg(unix)]
    #[test]
    fn shared_cache_preserves_live_leases_and_discards_stale_leases() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let active = root.join("exec/active");
        write(&active, 0);
        let lease = pin(root, &BTreeSet::from([active.clone()])).unwrap();
        let stale_path = root.join(PINS).join("crashed-process");
        fs::write(&stale_path, b"invalid stale data").unwrap();
        let stats = sweep(root, 0).unwrap();
        assert_eq!(stats.deleted_bytes, 0);
        assert!(active.exists());
        assert!(!stale_path.exists());
        drop(lease);
        let stats = sweep(root, 0).unwrap();
        assert_eq!(stats.deleted_bytes, 10);
        assert!(!active.exists());
    }

    #[tokio::test]
    async fn shared_cache_skips_eviction_during_verification() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let download = root.join("pending.wasm");
        write(&download, 0);
        let guard = lock_for_use(root.to_path_buf()).await.unwrap();
        sweep(root, 0).unwrap();
        assert!(download.exists());
        drop(guard);
        sweep(root, 0).unwrap();
        assert!(!download.exists());
    }

    #[cfg(unix)]
    #[test]
    fn eviction_does_not_follow_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let target = external.path().join("keep");
        write(&target, 0);
        std::os::unix::fs::symlink(external.path(), temp.path().join("link")).unwrap();
        sweep(temp.path(), 0).unwrap();
        assert!(target.exists());
        assert!(!temp.path().join("link").exists());
    }
}
