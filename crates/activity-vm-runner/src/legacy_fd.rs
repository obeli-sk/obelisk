use anyhow::{Context, ensure};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use wasmtime::{Caller, Extern, Linker, Module};

use crate::{MapDir, VmState};

const AT_FDCWD: i32 = -100;
const O_ACCMODE: i32 = 3;
const O_RDONLY: i32 = 0;
const O_CREAT: i32 = 64;
const O_EXCL: i32 = 128;
const O_TRUNC: i32 = 512;
const O_APPEND: i32 = 1024;

const ERRNO_BADF: i32 = 8;
const ERRNO_INVAL: i32 = 28;
const ERRNO_IO: i32 = 29;
const ERRNO_NOENT: i32 = 44;
const ERRNO_NOTCAPABLE: i32 = 76;

#[derive(Clone)]
struct Mount {
    host: PathBuf,
    guest: String,
    writable: bool,
}

struct Descriptor {
    file: File,
    writable: bool,
}

pub(crate) struct LegacyFdTable {
    mounts: Vec<Mount>,
    descriptors: Mutex<Descriptors>,
}

struct Descriptors {
    next: i32,
    files: HashMap<i32, Descriptor>,
}

impl LegacyFdTable {
    pub(crate) fn new(mapdirs: &[MapDir]) -> anyhow::Result<Arc<Self>> {
        let mut mounts = mapdirs
            .iter()
            .map(|mapdir| {
                Ok(Mount {
                    host: mapdir.host.canonicalize().with_context(|| {
                        format!("canonicalizing legacy mount {}", mapdir.host.display())
                    })?,
                    guest: normalize_absolute(&mapdir.guest)?,
                    writable: mapdir.permissions == wasmtime_wasi::FsPerms::ReadWrite,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        mounts.sort_unstable_by_key(|mount| std::cmp::Reverse(mount.guest.len()));
        Ok(Arc::new(Self {
            mounts,
            descriptors: Mutex::new(Descriptors {
                // Keep clear of stdio and the small descriptors Emscripten uses
                // for its signal/event pipes.
                next: 64,
                files: HashMap::new(),
            }),
        }))
    }

    fn resolve_existing(&self, raw: &str) -> anyhow::Result<(&Mount, PathBuf)> {
        let guest = normalize_absolute(raw)?;
        let mount = self
            .mounts
            .iter()
            .find(|mount| {
                guest == mount.guest
                    || guest
                        .strip_prefix(&mount.guest)
                        .is_some_and(|rest| rest.starts_with('/'))
            })
            .context("legacy path is outside a mounted directory")?;
        let relative = guest
            .strip_prefix(&mount.guest)
            .unwrap()
            .trim_start_matches('/');
        let path = mount.host.join(relative);
        let canonical = path
            .canonicalize()
            .with_context(|| format!("opening legacy path {guest}"))?;
        ensure!(
            canonical.starts_with(&mount.host),
            "legacy path escapes its mount"
        );
        Ok((mount, canonical))
    }

    fn open(&self, path: &str, flags: i32) -> Result<i32, i32> {
        if flags & (O_CREAT | O_EXCL | O_TRUNC | O_APPEND) != 0 {
            return Err(ERRNO_NOTCAPABLE);
        }
        if flags & O_ACCMODE != O_RDONLY {
            return Err(ERRNO_NOTCAPABLE);
        }
        let (mount, path) = self.resolve_existing(path).map_err(fs_errno)?;
        let file = File::open(path).map_err(|error| io_errno(&error))?;
        let mut descriptors = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        let fd = descriptors.next;
        descriptors.next = descriptors.next.checked_add(1).unwrap_or(64);
        descriptors.files.insert(
            fd,
            Descriptor {
                file,
                writable: mount.writable && flags & O_ACCMODE != O_RDONLY,
            },
        );
        Ok(fd)
    }

    fn close(&self, fd: i32) -> Result<(), i32> {
        self.descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .files
            .remove(&fd)
            .map(|_| ())
            .ok_or(ERRNO_BADF)
    }

    fn read(&self, fd: i32, output: &mut [u8]) -> Result<usize, i32> {
        self.descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .files
            .get_mut(&fd)
            .ok_or(ERRNO_BADF)?
            .file
            .read(output)
            .map_err(|error| io_errno(&error))
    }

    fn pread(&self, fd: i32, output: &mut [u8], offset: u64) -> Result<usize, i32> {
        use std::os::unix::fs::FileExt;
        self.descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .files
            .get(&fd)
            .ok_or(ERRNO_BADF)?
            .file
            .read_at(output, offset)
            .map_err(|error| io_errno(&error))
    }

    fn write(&self, fd: i32, input: &[u8]) -> Result<usize, i32> {
        let mut descriptors = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        let descriptor = descriptors.files.get_mut(&fd).ok_or(ERRNO_BADF)?;
        if !descriptor.writable {
            return Err(ERRNO_BADF);
        }
        descriptor
            .file
            .write(input)
            .map_err(|error| io_errno(&error))
    }

    fn seek(&self, fd: i32, offset: i64, whence: i32) -> Result<u64, i32> {
        let position = match whence {
            0 => SeekFrom::Start(u64::try_from(offset).map_err(|_| ERRNO_INVAL)?),
            1 => SeekFrom::Current(offset),
            2 => SeekFrom::End(offset),
            _ => return Err(ERRNO_INVAL),
        };
        self.descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .files
            .get_mut(&fd)
            .ok_or(ERRNO_BADF)?
            .file
            .seek(position)
            .map_err(|error| io_errno(&error))
    }
}

pub(crate) fn is_required(module: &Module) -> bool {
    module
        .imports()
        .any(|import| import.module() == "env" && import.name() == "__syscall_openat")
}

pub(crate) fn add_to_linker(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "__syscall_openat",
        |mut caller: Caller<'_, VmState>, dirfd: i32, path: i32, flags: i32, _mode: i32| {
            if dirfd != AT_FDCWD {
                return -ERRNO_NOTCAPABLE;
            }
            let path = match read_string(&mut caller, path) {
                Ok(path) => path,
                Err(errno) => return -errno,
            };
            caller
                .data()
                .legacy_fds
                .open(&path, flags)
                .unwrap_or_else(|errno| -errno)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_close",
        |caller: Caller<'_, VmState>, fd: i32| {
            caller.data().legacy_fds.close(fd).err().unwrap_or(0)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_read",
        |mut caller: Caller<'_, VmState>, fd: i32, iovs: i32, count: i32, output: i32| {
            vectored_io(&mut caller, fd, iovs, count, output, None, false)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_pread",
        |mut caller: Caller<'_, VmState>,
         fd: i32,
         iovs: i32,
         count: i32,
         offset: i64,
         output: i32| {
            let Ok(offset) = u64::try_from(offset) else {
                return ERRNO_INVAL;
            };
            vectored_io(&mut caller, fd, iovs, count, output, Some(offset), false)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_write",
        |mut caller: Caller<'_, VmState>, fd: i32, iovs: i32, count: i32, output: i32| {
            vectored_io(&mut caller, fd, iovs, count, output, None, true)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_seek",
        |mut caller: Caller<'_, VmState>, fd: i32, offset: i64, whence: i32, output: i32| {
            let table = caller.data().legacy_fds.clone();
            match table.seek(fd, offset, whence) {
                Ok(position) => write_memory(&mut caller, output, &position.to_le_bytes())
                    .err()
                    .unwrap_or(0),
                Err(errno) => errno,
            }
        },
    )?;
    Ok(())
}

fn vectored_io(
    caller: &mut Caller<'_, VmState>,
    fd: i32,
    iovs: i32,
    count: i32,
    output: i32,
    offset: Option<u64>,
    write: bool,
) -> i32 {
    let result = (|| -> Result<u32, i32> {
        let count = u32::try_from(count).map_err(|_| ERRNO_INVAL)?;
        let table = caller.data().legacy_fds.clone();
        let mut total = 0_u32;
        for index in 0..count {
            let iovec = iovs
                .checked_add(
                    i32::try_from(index)
                        .unwrap()
                        .checked_mul(8)
                        .ok_or(ERRNO_INVAL)?,
                )
                .ok_or(ERRNO_INVAL)?;
            let pointer = read_u32(caller, iovec)?;
            let length = read_u32(caller, iovec + 4)?;
            let mut bytes = vec![0; usize::try_from(length).map_err(|_| ERRNO_INVAL)?];
            let transferred = if write {
                read_memory(
                    caller,
                    i32::try_from(pointer).map_err(|_| ERRNO_INVAL)?,
                    &mut bytes,
                )?;
                table.write(fd, &bytes)?
            } else {
                let transferred = match offset {
                    Some(base) => table.pread(fd, &mut bytes, base + u64::from(total))?,
                    None => table.read(fd, &mut bytes)?,
                };
                write_memory(
                    caller,
                    i32::try_from(pointer).map_err(|_| ERRNO_INVAL)?,
                    &bytes[..transferred],
                )?;
                transferred
            };
            total = total
                .checked_add(u32::try_from(transferred).map_err(|_| ERRNO_INVAL)?)
                .ok_or(ERRNO_INVAL)?;
            if transferred < bytes.len() {
                break;
            }
        }
        Ok(total)
    })();
    match result {
        Ok(total) => write_memory(caller, output, &total.to_le_bytes())
            .err()
            .unwrap_or(0),
        Err(errno) => errno,
    }
}

fn normalize_absolute(raw: &str) -> anyhow::Result<String> {
    ensure!(raw.starts_with('/'), "legacy path is not absolute");
    let mut normalized = PathBuf::from("/");
    for component in Path::new(raw).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => normalized.push(component),
            Component::ParentDir => ensure!(normalized.pop(), "legacy path escapes root"),
            Component::Prefix(_) => anyhow::bail!("unsupported legacy path prefix"),
        }
    }
    Ok(normalized.to_string_lossy().into_owned())
}

fn memory(caller: &mut Caller<'_, VmState>) -> Result<Extern, i32> {
    caller.get_export("memory").ok_or(ERRNO_INVAL)
}

fn read_string(caller: &mut Caller<'_, VmState>, pointer: i32) -> Result<String, i32> {
    let memory = memory(caller)?;
    let memory = memory.into_memory().ok_or(ERRNO_INVAL)?;
    let data = memory.data(caller);
    let start = usize::try_from(pointer).map_err(|_| ERRNO_INVAL)?;
    let tail = data.get(start..).ok_or(ERRNO_INVAL)?;
    let end = tail.iter().position(|byte| *byte == 0).ok_or(ERRNO_INVAL)?;
    std::str::from_utf8(&tail[..end])
        .map(str::to_owned)
        .map_err(|_| ERRNO_INVAL)
}

fn read_u32(caller: &mut Caller<'_, VmState>, pointer: i32) -> Result<u32, i32> {
    let mut bytes = [0; 4];
    read_memory(caller, pointer, &mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_memory(
    caller: &mut Caller<'_, VmState>,
    pointer: i32,
    output: &mut [u8],
) -> Result<(), i32> {
    let memory = memory(caller)?.into_memory().ok_or(ERRNO_INVAL)?;
    memory
        .read(
            caller,
            usize::try_from(pointer).map_err(|_| ERRNO_INVAL)?,
            output,
        )
        .map_err(|_| ERRNO_INVAL)
}

fn write_memory(caller: &mut Caller<'_, VmState>, pointer: i32, input: &[u8]) -> Result<(), i32> {
    let memory = memory(caller)?.into_memory().ok_or(ERRNO_INVAL)?;
    memory
        .write(
            caller,
            usize::try_from(pointer).map_err(|_| ERRNO_INVAL)?,
            input,
        )
        .map_err(|_| ERRNO_INVAL)
}

fn io_errno(error: &std::io::Error) -> i32 {
    match error.kind() {
        std::io::ErrorKind::NotFound => ERRNO_NOENT,
        std::io::ErrorKind::PermissionDenied => ERRNO_NOTCAPABLE,
        std::io::ErrorKind::InvalidInput => ERRNO_INVAL,
        _ => ERRNO_IO,
    }
}

fn fs_errno(error: anyhow::Error) -> i32 {
    error
        .downcast_ref::<std::io::Error>()
        .map_or(ERRNO_NOTCAPABLE, io_errno)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use wasmtime::{Engine, Store};
    use wasmtime_wasi::{WasiCtxBuilder, p1};

    fn fixture() -> (tempfile::TempDir, Arc<LegacyFdTable>) {
        let pack = tempfile::tempdir().unwrap();
        File::create(pack.path().join("hello"))
            .unwrap()
            .write_all(b"hello world")
            .unwrap();
        let table = LegacyFdTable::new(&[MapDir::read_only(
            pack.path().to_owned(),
            "/pack".to_owned(),
        )])
        .unwrap();
        (pack, table)
    }

    #[test]
    fn shares_positioned_and_stream_io_on_one_descriptor() {
        let (_pack, table) = fixture();
        let fd = table.open("/pack/hello", O_RDONLY).unwrap();
        let mut bytes = [0; 5];
        assert_eq!(table.read(fd, &mut bytes).unwrap(), 5);
        assert_eq!(&bytes, b"hello");
        assert_eq!(table.pread(fd, &mut bytes, 6).unwrap(), 5);
        assert_eq!(&bytes, b"world");
        assert_eq!(table.seek(fd, 6, 0).unwrap(), 6);
        assert_eq!(table.read(fd, &mut bytes).unwrap(), 5);
        assert_eq!(&bytes, b"world");
        table.close(fd).unwrap();
        assert_eq!(table.read(fd, &mut bytes), Err(ERRNO_BADF));
    }

    #[test]
    fn rejects_writes_and_mutating_open_flags_on_read_only_pack() {
        let (_pack, table) = fixture();
        let fd = table.open("/pack/hello", O_RDONLY).unwrap();
        assert_eq!(table.write(fd, b"no"), Err(ERRNO_BADF));
        assert_eq!(table.open("/pack/hello", O_TRUNC), Err(ERRNO_NOTCAPABLE));
    }

    #[test]
    fn confines_paths_and_symlinks_to_the_mount() {
        use std::os::unix::fs::symlink;

        let (pack, table) = fixture();
        assert_eq!(
            table.open("/pack/../../etc/passwd", O_RDONLY),
            Err(ERRNO_NOTCAPABLE)
        );
        symlink("/etc/passwd", pack.path().join("escape")).unwrap();
        assert_eq!(table.open("/pack/escape", O_RDONLY), Err(ERRNO_NOTCAPABLE));
    }

    #[test]
    fn links_openat_and_preview1_fds_to_the_same_table() {
        let (pack, table) = fixture();
        let mapdirs = [MapDir::read_only(
            pack.path().to_owned(),
            "/pack".to_owned(),
        )];
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "__syscall_openat"
                    (func $openat (param i32 i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_pread"
                    (func $pread (param i32 i32 i32 i64 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_read"
                    (func $read (param i32 i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_write"
                    (func $write (param i32 i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_seek"
                    (func $seek (param i32 i64 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_close"
                    (func $close (param i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 0) "/pack/hello\00")
                (data (i32.const 32) "\80\00\00\00\05\00\00\00")
                (data (i32.const 40) "\90\00\00\00\05\00\00\00")
                (func (export "run") (result i32)
                    (local $fd i32)
                    i32.const -100 i32.const 0 i32.const 0 i32.const 0
                    call $openat local.tee $fd
                    i32.const 32 i32.const 1 i64.const 6 i32.const 24
                    call $pread
                    if (result i32)
                        i32.const 1
                    else
                        local.get $fd i64.const 0 i32.const 0 i32.const 16
                        call $seek drop
                        local.get $fd i32.const 40 i32.const 1 i32.const 28
                        call $read drop
                        local.get $fd i32.const 32 i32.const 1 i32.const 24
                        call $write
                        i32.const 8 i32.ne
                        if (result i32)
                            i32.const 2
                        else
                            local.get $fd call $close
                        end
                    end))"#,
        )
        .unwrap();
        let host_fs = crate::host_fs::HostFs::new(&mapdirs).unwrap();
        let mut store = Store::new(
            &engine,
            VmState {
                wasi: WasiCtxBuilder::new().build_p1(),
                qemu_jit: crate::qemu_jit::QemuJit::new(None),
                host_fs,
                legacy_fds: table,
                fiber_next: None,
                fiber_entries: HashMap::new(),
                poll_calls: 0,
            },
        );
        let mut linker = Linker::new(&engine);
        p1::add_to_linker_sync(&mut linker, |state: &mut VmState| &mut state.wasi).unwrap();
        linker.allow_shadowing(true);
        add_to_linker(&mut linker).unwrap();
        let instance = linker.instantiate(&mut store, &module).unwrap();
        assert_eq!(
            instance
                .get_typed_func::<(), i32>(&mut store, "run")
                .unwrap()
                .call(&mut store, ())
                .unwrap(),
            0
        );
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        assert_eq!(&memory.data(&store)[128..133], b"world");
        assert_eq!(&memory.data(&store)[144..149], b"hello");
        assert_eq!(&memory.data(&store)[28..32], &5_u32.to_le_bytes());
    }
}
