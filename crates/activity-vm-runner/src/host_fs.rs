use anyhow::{Context, ensure};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, symlink};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use wasmtime::{Caller, Extern, Linker};

use crate::{MapDir, VmState};

const KIND_FILE: i32 = 1;
const KIND_DIRECTORY: i32 = 2;
const KIND_SYMLINK: i32 = 3;

#[derive(Clone)]
struct Mount {
    host: PathBuf,
    guest: String,
    writable: bool,
}

pub(crate) struct HostFs {
    mounts: Vec<Mount>,
    files: Mutex<OpenFiles>,
    _scratch: tempfile::TempDir,
}

struct OpenFiles {
    next: i32,
    files: HashMap<i32, File>,
}

enum Resolved {
    Host { path: PathBuf, writable: bool },
    VirtualDirectory,
}

impl HostFs {
    pub(crate) fn new(mapdirs: &[MapDir]) -> anyhow::Result<Arc<Self>> {
        let scratch = tempfile::tempdir()?;
        let mut mounts = mapdirs
            .iter()
            .map(|mapdir| Mount {
                host: mapdir.host.clone(),
                guest: normalize_guest_mount(&mapdir.guest),
                writable: mapdir.permissions == wasmtime_wasi::FsPerms::ReadWrite,
            })
            .collect::<Vec<_>>();
        mounts.push(Mount {
            host: scratch.path().to_owned(),
            guest: "/".to_owned(),
            writable: true,
        });
        mounts.sort_unstable_by_key(|mount| std::cmp::Reverse(mount.guest.len()));
        Ok(Arc::new(Self {
            mounts,
            files: Mutex::new(OpenFiles {
                next: 16,
                files: HashMap::new(),
            }),
            _scratch: scratch,
        }))
    }

    fn resolve(&self, raw: &str) -> anyhow::Result<Resolved> {
        let guest = normalize_node_path(raw)?;
        for mount in &self.mounts {
            if mount.guest == "/" {
                continue;
            }
            if guest == mount.guest {
                return Ok(Resolved::Host {
                    path: mount.host.clone(),
                    writable: mount.writable,
                });
            }
            let relative = if mount.guest == "/" {
                guest.strip_prefix('/')
            } else {
                guest.strip_prefix(&format!("{}/", mount.guest))
            };
            if let Some(relative) = relative {
                return Ok(Resolved::Host {
                    path: mount.host.join(relative),
                    writable: mount.writable,
                });
            }
        }
        let prefix = if guest == "/" {
            "/".to_owned()
        } else {
            format!("{guest}/")
        };
        if self
            .mounts
            .iter()
            .any(|mount| mount.guest != "/" && mount.guest.starts_with(&prefix))
        {
            return Ok(Resolved::VirtualDirectory);
        }
        let root = self
            .mounts
            .iter()
            .find(|mount| mount.guest == "/")
            .context("missing host filesystem scratch mount")?;
        Ok(Resolved::Host {
            path: root.host.join(guest.trim_start_matches('/')),
            writable: true,
        })
    }

    fn entries(&self, raw: &str) -> anyhow::Result<Vec<(String, i32)>> {
        let guest = normalize_node_path(raw)?;
        let mut entries = BTreeMap::new();
        if let Resolved::Host { path, .. } = self.resolve(&guest)? {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                entries.insert(
                    entry.file_name().to_string_lossy().into_owned(),
                    kind(&entry.file_type()?),
                );
            }
        }
        let prefix = if guest == "/" {
            "/".to_owned()
        } else {
            format!("{guest}/")
        };
        for mount in &self.mounts {
            let Some(remainder) = mount.guest.strip_prefix(&prefix) else {
                continue;
            };
            let Some(child) = remainder.split('/').next() else {
                continue;
            };
            if !child.is_empty() {
                entries.entry(child.to_owned()).or_insert(KIND_DIRECTORY);
            }
        }
        Ok(entries.into_iter().collect())
    }

    fn host_path(&self, raw: &str) -> anyhow::Result<(PathBuf, bool)> {
        match self.resolve(raw)? {
            Resolved::Host { path, writable } => Ok((path, writable)),
            Resolved::VirtualDirectory => anyhow::bail!("path is a virtual directory"),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_host_path(&self, raw: &str) -> anyhow::Result<(PathBuf, bool)> {
        self.host_path(raw)
    }

    fn allocate(&self, file: File) -> i32 {
        let mut files = self.files.lock().expect("host filesystem mutex poisoned");
        let fd = files.next;
        files.next = files.next.checked_add(1).unwrap_or(16);
        files.files.insert(fd, file);
        fd
    }
}

pub(crate) fn add_to_linker(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "_wasmfs_node_get_mode",
        |mut caller: Caller<'_, VmState>, path: i32, output: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let path = read_string(caller, path)?;
                if std::env::var_os("OBELISK_QEMU_TRACE_HOSTFS").is_some() {
                    eprintln!("hostfs mode {path}");
                }
                let mode = match fs.resolve(&path)? {
                    Resolved::VirtualDirectory => 0o040_555,
                    Resolved::Host { path, writable } => {
                        let mut mode = std::fs::symlink_metadata(path)?.mode();
                        if !writable {
                            mode &= !0o222;
                        }
                        mode
                    }
                };
                write_u32(caller, output, mode)
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_stat_size",
        |mut caller: Caller<'_, VmState>, path: i32, output: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let path = read_string(caller, path)?;
                if std::env::var_os("OBELISK_QEMU_TRACE_HOSTFS").is_some() {
                    eprintln!("hostfs stat {path}");
                }
                let size = match fs.resolve(&path)? {
                    Resolved::VirtualDirectory => 0,
                    Resolved::Host { path, .. } => std::fs::symlink_metadata(path)?.size(),
                };
                let size = u32::try_from(size).context("host file exceeds WasmFS size limit")?;
                write_u32(caller, output, size)
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_fstat_size",
        |mut caller: Caller<'_, VmState>, fd: i32, output: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let files = fs.files.lock().expect("host filesystem mutex poisoned");
                let size = files
                    .files
                    .get(&fd)
                    .context("unknown host file descriptor")?
                    .metadata()?
                    .len();
                write_u32(caller, output, u32::try_from(size)?)
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_open",
        |mut caller: Caller<'_, VmState>, path: i32, mode: i32| -> i32 {
            let result = (|| -> anyhow::Result<i32> {
                let path = read_string(&mut caller, path)?;
                let mode = read_string(&mut caller, mode)?;
                let fs = caller.data().host_fs.clone();
                let (path, writable) = fs.host_path(&path)?;
                eprintln!("hostfs open {} mode={mode}", path.display());
                let mut options = OpenOptions::new();
                match mode.as_str() {
                    "r" => {
                        options.read(true);
                    }
                    "r+" if writable => {
                        options.read(true).write(true);
                    }
                    "w" if writable => {
                        options.write(true).create(true).truncate(true);
                    }
                    "a" if writable => {
                        options.append(true).create(true);
                    }
                    _ => anyhow::bail!("unsupported or read-only open mode {mode}"),
                }
                Ok(fs.allocate(options.open(path)?))
            })();
            result.unwrap_or_else(|error| -emscripten_errno(&error))
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_close",
        |caller: Caller<'_, VmState>, fd: i32| -> i32 {
            let fs = caller.data().host_fs.clone();
            if fs
                .files
                .lock()
                .expect("host filesystem mutex poisoned")
                .files
                .remove(&fd)
                .is_some()
            {
                0
            } else {
                8
            }
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_read",
        |mut caller: Caller<'_, VmState>,
         fd: i32,
         buffer: i32,
         len: i32,
         offset: i32,
         output: i32|
         -> i32 {
            host_result(&mut caller, |caller, fs| {
                let len = usize::try_from(len)?;
                let offset = u64::try_from(offset)?;
                let mut bytes = vec![0; len];
                let count = {
                    let files = fs.files.lock().expect("host filesystem mutex poisoned");
                    files
                        .files
                        .get(&fd)
                        .context("unknown host file descriptor")?
                        .read_at(&mut bytes, offset)?
                };
                write_memory(caller, buffer, &bytes[..count])?;
                write_u32(caller, output, u32::try_from(count)?)
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_write",
        |mut caller: Caller<'_, VmState>,
         fd: i32,
         buffer: i32,
         len: i32,
         offset: i32,
         output: i32|
         -> i32 {
            host_result(&mut caller, |caller, fs| {
                let bytes = read_memory(caller, buffer, usize::try_from(len)?)?;
                let count = {
                    let files = fs.files.lock().expect("host filesystem mutex poisoned");
                    files
                        .files
                        .get(&fd)
                        .context("unknown host file descriptor")?
                        .write_at(&bytes, u64::try_from(offset)?)?
                };
                write_u32(caller, output, u32::try_from(count)?)
            })
        },
    )?;
    add_path_mutations(linker)?;
    add_readdir(linker)?;
    Ok(())
}

fn add_path_mutations(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "_wasmfs_node_insert_file",
        |mut caller: Caller<'_, VmState>, path: i32, mode: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let (path, writable) = fs.host_path(&read_string(caller, path)?)?;
                ensure!(writable, "read-only host mount");
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(u32::try_from(mode)?)
                    .open(path)?;
                Ok(())
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_insert_directory",
        |mut caller: Caller<'_, VmState>, path: i32, _mode: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let (path, writable) = fs.host_path(&read_string(caller, path)?)?;
                ensure!(writable, "read-only host mount");
                std::fs::create_dir(path)?;
                Ok(())
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_unlink",
        |mut caller: Caller<'_, VmState>, path: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let (path, writable) = fs.host_path(&read_string(caller, path)?)?;
                ensure!(writable, "read-only host mount");
                std::fs::remove_file(path)?;
                Ok(())
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_rmdir",
        |mut caller: Caller<'_, VmState>, path: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let (path, writable) = fs.host_path(&read_string(caller, path)?)?;
                ensure!(writable, "read-only host mount");
                std::fs::remove_dir(path)?;
                Ok(())
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_truncate",
        |mut caller: Caller<'_, VmState>, path: i32, len: i64| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let (path, writable) = fs.host_path(&read_string(caller, path)?)?;
                ensure!(writable, "read-only host mount");
                OpenOptions::new()
                    .write(true)
                    .open(path)?
                    .set_len(u64::try_from(len)?)?;
                Ok(())
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_ftruncate",
        |caller: Caller<'_, VmState>, fd: i32, len: i64| -> i32 {
            let fs = caller.data().host_fs.clone();
            let result = fs
                .files
                .lock()
                .expect("host filesystem mutex poisoned")
                .files
                .get(&fd)
                .context("unknown host file descriptor")
                .and_then(|file| file.set_len(u64::try_from(len)?).map_err(Into::into));
            result.map_or_else(|error| emscripten_errno(&error), |()| 0)
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_rename",
        |mut caller: Caller<'_, VmState>, from: i32, to: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let (from, from_writable) = fs.host_path(&read_string(caller, from)?)?;
                let (to, to_writable) = fs.host_path(&read_string(caller, to)?)?;
                ensure!(from_writable && to_writable, "read-only host mount");
                std::fs::rename(from, to)?;
                Ok(())
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_symlink",
        |mut caller: Caller<'_, VmState>, target: i32, link: i32| -> i32 {
            host_result(&mut caller, |caller, fs| {
                let target = read_string(caller, target)?;
                let (link, writable) = fs.host_path(&read_string(caller, link)?)?;
                ensure!(writable, "read-only host mount");
                symlink(target, link)?;
                Ok(())
            })
        },
    )?;
    linker.func_wrap(
        "env",
        "_wasmfs_node_readlink",
        |mut caller: Caller<'_, VmState>, path: i32, output: i32, capacity: i32| -> i32 {
            let result = (|| -> anyhow::Result<i32> {
                let fs = caller.data().host_fs.clone();
                let (path, _) = fs.host_path(&read_string(&mut caller, path)?)?;
                let target = std::fs::read_link(path)?
                    .as_os_str()
                    .as_encoded_bytes()
                    .to_vec();
                let capacity = usize::try_from(capacity)?;
                ensure!(capacity > target.len(), "readlink buffer too small");
                write_memory(&mut caller, output, &target)?;
                write_memory(&mut caller, output + i32::try_from(target.len())?, &[0])?;
                Ok(i32::try_from(target.len())?)
            })();
            result.unwrap_or_else(|error| -emscripten_errno(&error))
        },
    )?;
    Ok(())
}

fn add_readdir(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "_wasmfs_node_readdir",
        |mut caller: Caller<'_, VmState>, path: i32, vector: i32| -> wasmtime::Result<i32> {
            let result = (|| -> anyhow::Result<()> {
                let path = read_string(&mut caller, path)?;
                let fs = caller.data().host_fs.clone();
                let entries = fs.entries(&path)?;
                let malloc = caller
                    .get_export("malloc")
                    .and_then(Extern::into_func)
                    .context("missing Emscripten malloc")?
                    .typed::<i32, i32>(&caller)?;
                let free = caller
                    .get_export("free")
                    .and_then(Extern::into_func)
                    .context("missing Emscripten free")?
                    .typed::<i32, ()>(&caller)?;
                let record = caller
                    .get_export("_wasmfs_node_record_dirent")
                    .and_then(Extern::into_func)
                    .context("missing WasmFS dirent callback")?
                    .typed::<(i32, i32, i32), ()>(&caller)?;
                for (name, kind) in entries {
                    let len = i32::try_from(name.len() + 1)?;
                    let pointer = malloc.call(&mut caller, len)?;
                    write_memory(&mut caller, pointer, name.as_bytes())?;
                    write_memory(&mut caller, pointer + len - 1, &[0])?;
                    record.call(&mut caller, (vector, pointer, kind))?;
                    free.call(&mut caller, pointer)?;
                }
                Ok(())
            })();
            Ok(result.map_or_else(|error| emscripten_errno(&error), |()| 0))
        },
    )?;
    Ok(())
}

fn host_result(
    caller: &mut Caller<'_, VmState>,
    operation: impl FnOnce(&mut Caller<'_, VmState>, Arc<HostFs>) -> anyhow::Result<()>,
) -> i32 {
    let fs = caller.data().host_fs.clone();
    operation(caller, fs).map_or_else(|error| emscripten_errno(&error), |()| 0)
}

fn normalize_guest_mount(path: &str) -> String {
    format!("/{}", path.trim_matches('/'))
}

fn normalize_node_path(path: &str) -> anyhow::Result<String> {
    let path = path.strip_prefix("./").unwrap_or(path);
    let mut normalized = PathBuf::from("/");
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => normalized.push(component),
            Component::ParentDir | Component::Prefix(_) => {
                anyhow::bail!("invalid host path {path}")
            }
        }
    }
    Ok(normalized.to_string_lossy().into_owned())
}

fn kind(file_type: &std::fs::FileType) -> i32 {
    if file_type.is_file() {
        KIND_FILE
    } else if file_type.is_dir() {
        KIND_DIRECTORY
    } else if file_type.is_symlink() {
        KIND_SYMLINK
    } else {
        0
    }
}

fn read_string(caller: &mut Caller<'_, VmState>, pointer: i32) -> anyhow::Result<String> {
    let start = usize::try_from(pointer)?;
    let mut bytes = Vec::new();
    for offset in 0..=64 * 1024 {
        let byte = read_memory(caller, i32::try_from(start + offset)?, 1)?[0];
        if byte == 0 {
            return Ok(String::from_utf8(bytes)?);
        }
        bytes.push(byte);
    }
    anyhow::bail!("unterminated or overlong host path")
}

fn read_memory(
    caller: &mut Caller<'_, VmState>,
    pointer: i32,
    len: usize,
) -> anyhow::Result<Vec<u8>> {
    let mut bytes = vec![0; len];
    let offset = usize::try_from(pointer)?;
    match caller.get_export("memory") {
        Some(Extern::Memory(memory)) => memory.read(caller, offset, &mut bytes)?,
        Some(Extern::SharedMemory(memory)) => {
            let source = memory
                .data()
                .get(offset..offset + len)
                .context("host read is out of bounds")?;
            for (output, input) in bytes.iter_mut().zip(source) {
                // SAFETY: the Emscripten caller owns these bytes for the duration of the import.
                *output = unsafe { input.get().read() };
            }
        }
        _ => anyhow::bail!("missing Emscripten memory export"),
    }
    Ok(bytes)
}

fn write_memory(
    caller: &mut Caller<'_, VmState>,
    pointer: i32,
    bytes: &[u8],
) -> anyhow::Result<()> {
    let offset = usize::try_from(pointer)?;
    match caller.get_export("memory") {
        Some(Extern::Memory(memory)) => memory.write(caller, offset, bytes)?,
        Some(Extern::SharedMemory(memory)) => {
            let target = memory
                .data()
                .get(offset..offset + bytes.len())
                .context("host write is out of bounds")?;
            for (output, input) in target.iter().zip(bytes) {
                // SAFETY: the Emscripten caller owns these bytes for the duration of the import.
                unsafe { output.get().write(*input) };
            }
        }
        _ => anyhow::bail!("missing Emscripten memory export"),
    }
    Ok(())
}

fn write_u32(caller: &mut Caller<'_, VmState>, pointer: i32, value: u32) -> anyhow::Result<()> {
    write_memory(caller, pointer, &value.to_le_bytes())
}

fn emscripten_errno(error: &anyhow::Error) -> i32 {
    let raw = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<std::io::Error>())
        .and_then(std::io::Error::raw_os_error);
    match raw {
        Some(1) => 63,
        Some(2) => 44,
        Some(9) => 8,
        Some(13) => 2,
        Some(17) => 20,
        Some(18) => 75,
        Some(20) => 54,
        Some(21) => 31,
        Some(22) => 28,
        Some(24) => 33,
        Some(27) => 22,
        Some(28) => 51,
        Some(30) => 69,
        Some(32) => 64,
        Some(36) => 37,
        Some(39) => 55,
        Some(40) => 32,
        _ => 29,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qemu_9p_mounts_keep_pack_store_and_proxy_separate() {
        let root = tempfile::tempdir().unwrap();
        let store = root.path().join("store");
        let proxy = root.path().join("proxy");
        let pack = root.path().join("pack");
        std::fs::create_dir_all(&store).unwrap();
        std::fs::create_dir_all(&proxy).unwrap();
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(store.join("closure"), b"nix").unwrap();
        std::fs::write(proxy.join("http-guest.sh"), b"proxy").unwrap();
        std::fs::write(pack.join("info"), b"args").unwrap();

        let fs = HostFs::new(&[
            MapDir::read_only(store.clone(), "/nix/store".into()),
            MapDir::read_write(proxy.clone(), "/obelisk-activity-vm-http".into()),
            MapDir::read_write(pack.clone(), "/pack".into()),
        ])
        .unwrap();

        assert_eq!(
            fs.host_path("/nix/store/closure").unwrap(),
            (store.join("closure"), false)
        );
        assert_eq!(
            fs.host_path("/obelisk-activity-vm-http/http-guest.sh")
                .unwrap(),
            (proxy.join("http-guest.sh"), true)
        );
        assert_eq!(
            fs.host_path("/pack/info").unwrap(),
            (pack.join("info"), true)
        );
        let root_entries = fs.entries("/").unwrap();
        assert!(
            root_entries
                .iter()
                .any(|entry| entry == &("nix".into(), KIND_DIRECTORY))
        );
        assert!(
            root_entries
                .iter()
                .any(|entry| entry == &("pack".into(), KIND_DIRECTORY))
        );
        assert!(
            root_entries
                .iter()
                .any(|entry| entry == &("obelisk-activity-vm-http".into(), KIND_DIRECTORY))
        );
    }

    #[test]
    fn qemu_9p_rejects_parent_traversal() {
        let fs = HostFs::new(&[]).unwrap();
        assert!(fs.host_path("/pack/../secret").is_err());
    }
}
