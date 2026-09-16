use anyhow::{Context, ensure};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
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
const ERRNO_NOTTY: i32 = 59;

const POLLIN: i16 = 1;
const POLLOUT: i16 = 4;
const POLLERR: i16 = 8;
const POLLHUP: i16 = 16;
const POLLNVAL: i16 = 32;

#[derive(Clone)]
struct Mount {
    host: PathBuf,
    guest: String,
    writable: bool,
}

enum Descriptor {
    File { file: File, writable: bool },
    TtyInput,
    TtyOutput,
    PipeRead(u32),
    PipeWrite(u32),
}

pub(crate) struct LegacyFdTable {
    mounts: Vec<Mount>,
    descriptors: Mutex<Descriptors>,
    readiness: Condvar,
}

struct Descriptors {
    next: i32,
    next_pipe: u32,
    entries: HashMap<i32, Descriptor>,
    pipes: HashMap<u32, Pipe>,
    tty_input: VecDeque<u8>,
    tty_output: Vec<u8>,
    status_flags: HashMap<i32, i32>,
}

struct Pipe {
    bytes: VecDeque<u8>,
    readers: usize,
    writers: usize,
}

#[derive(Clone, Copy)]
struct PollFd {
    fd: i32,
    events: i16,
    revents: i16,
}

struct LegacyStat {
    dev: u32,
    mode: u32,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    size: i64,
    blocks: u32,
    atime: i64,
    atime_nsec: u32,
    mtime: i64,
    mtime_nsec: u32,
    ctime: i64,
    ctime_nsec: u32,
    ino: u64,
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
                next_pipe: 0,
                entries: HashMap::from([
                    (0, Descriptor::TtyInput),
                    (1, Descriptor::TtyOutput),
                    (2, Descriptor::TtyOutput),
                ]),
                pipes: HashMap::new(),
                tty_input: VecDeque::new(),
                tty_output: Vec::new(),
                status_flags: HashMap::from([(0, 0), (1, 1), (2, 1)]),
            }),
            readiness: Condvar::new(),
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

    fn resolve_unfollowed(&self, raw: &str) -> anyhow::Result<(&Mount, PathBuf)> {
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
        let parent = path.parent().context("legacy path has no parent")?;
        let canonical_parent = parent
            .canonicalize()
            .with_context(|| format!("resolving parent of legacy path {guest}"))?;
        ensure!(
            canonical_parent.starts_with(&mount.host),
            "legacy path escapes its mount"
        );
        Ok((
            mount,
            canonical_parent.join(path.file_name().unwrap_or_default()),
        ))
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
        descriptors.entries.insert(
            fd,
            Descriptor::File {
                file,
                writable: mount.writable && flags & O_ACCMODE != O_RDONLY,
            },
        );
        descriptors.status_flags.insert(fd, flags);
        Ok(fd)
    }

    fn close(&self, fd: i32) -> Result<(), i32> {
        let mut state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        let descriptor = state.entries.remove(&fd).ok_or(ERRNO_BADF)?;
        state.status_flags.remove(&fd);
        let pipe_id = match descriptor {
            Descriptor::PipeRead(id) => {
                state.pipes.get_mut(&id).unwrap().readers -= 1;
                Some(id)
            }
            Descriptor::PipeWrite(id) => {
                state.pipes.get_mut(&id).unwrap().writers -= 1;
                Some(id)
            }
            _ => None,
        };
        if let Some(id) = pipe_id {
            let pipe = state.pipes.get(&id).unwrap();
            if pipe.readers == 0 && pipe.writers == 0 {
                state.pipes.remove(&id);
            }
        }
        drop(state);
        self.readiness.notify_all();
        Ok(())
    }

    fn read(&self, fd: i32, output: &mut [u8]) -> Result<usize, i32> {
        let mut state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        let descriptor = state.entries.get(&fd).ok_or(ERRNO_BADF)?;
        match descriptor {
            Descriptor::File { .. } => match state.entries.get_mut(&fd).unwrap() {
                Descriptor::File { file, .. } => {
                    file.read(output).map_err(|error| io_errno(&error))
                }
                _ => unreachable!(),
            },
            Descriptor::TtyInput => Ok(drain(&mut state.tty_input, output)),
            Descriptor::PipeRead(id) => {
                let id = *id;
                Ok(drain(&mut state.pipes.get_mut(&id).unwrap().bytes, output))
            }
            Descriptor::TtyOutput | Descriptor::PipeWrite(_) => Err(ERRNO_BADF),
        }
    }

    fn pread(&self, fd: i32, output: &mut [u8], offset: u64) -> Result<usize, i32> {
        use std::os::unix::fs::FileExt;
        match self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .entries
            .get(&fd)
            .ok_or(ERRNO_BADF)?
        {
            Descriptor::File { file, .. } => file
                .read_at(output, offset)
                .map_err(|error| io_errno(&error)),
            _ => Err(ERRNO_BADF),
        }
    }

    fn pwrite(&self, fd: i32, input: &[u8], offset: u64) -> Result<usize, i32> {
        use std::os::unix::fs::FileExt;
        match self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .entries
            .get(&fd)
            .ok_or(ERRNO_BADF)?
        {
            Descriptor::File {
                file,
                writable: true,
            } => file
                .write_at(input, offset)
                .map_err(|error| io_errno(&error)),
            _ => Err(ERRNO_BADF),
        }
    }

    fn write(&self, fd: i32, input: &[u8]) -> Result<usize, i32> {
        let mut descriptors = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        let descriptor = descriptors.entries.get(&fd).ok_or(ERRNO_BADF)?;
        let result = match descriptor {
            Descriptor::File {
                writable: false, ..
            }
            | Descriptor::TtyInput
            | Descriptor::PipeRead(_) => Err(ERRNO_BADF),
            Descriptor::File { .. } => match descriptors.entries.get_mut(&fd).unwrap() {
                Descriptor::File { file, .. } => {
                    file.write(input).map_err(|error| io_errno(&error))
                }
                _ => unreachable!(),
            },
            Descriptor::TtyOutput => {
                descriptors.tty_output.extend_from_slice(input);
                Ok(input.len())
            }
            Descriptor::PipeWrite(id) => {
                let id = *id;
                let pipe = descriptors.pipes.get_mut(&id).unwrap();
                if pipe.readers == 0 {
                    Err(ERRNO_BADF)
                } else {
                    pipe.bytes.extend(input);
                    Ok(input.len())
                }
            }
        };
        drop(descriptors);
        if result.is_ok() {
            self.readiness.notify_all();
        }
        result
    }

    fn seek(&self, fd: i32, offset: i64, whence: i32) -> Result<u64, i32> {
        let position = match whence {
            0 => SeekFrom::Start(u64::try_from(offset).map_err(|_| ERRNO_INVAL)?),
            1 => SeekFrom::Current(offset),
            2 => SeekFrom::End(offset),
            _ => return Err(ERRNO_INVAL),
        };
        match self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .entries
            .get_mut(&fd)
            .ok_or(ERRNO_BADF)?
        {
            Descriptor::File { file, .. } => file.seek(position).map_err(|error| io_errno(&error)),
            _ => Err(ERRNO_BADF),
        }
    }

    fn pipe(&self) -> (i32, i32) {
        let mut state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        let id = state.next_pipe;
        state.next_pipe = state.next_pipe.wrapping_add(1);
        let read_fd = allocate_fd(&mut state);
        let write_fd = allocate_fd(&mut state);
        state.pipes.insert(
            id,
            Pipe {
                bytes: VecDeque::new(),
                readers: 1,
                writers: 1,
            },
        );
        state.entries.insert(read_fd, Descriptor::PipeRead(id));
        state.entries.insert(write_fd, Descriptor::PipeWrite(id));
        state.status_flags.insert(read_fd, 0);
        state.status_flags.insert(write_fd, 1);
        (read_fd, write_fd)
    }

    fn duplicate(&self, fd: i32, minimum: i32) -> Result<i32, i32> {
        if minimum < 0 {
            return Err(ERRNO_INVAL);
        }
        let mut state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        let descriptor = match state.entries.get(&fd).ok_or(ERRNO_BADF)? {
            Descriptor::File { file, writable } => Descriptor::File {
                file: file.try_clone().map_err(|error| io_errno(&error))?,
                writable: *writable,
            },
            Descriptor::TtyInput => Descriptor::TtyInput,
            Descriptor::TtyOutput => Descriptor::TtyOutput,
            Descriptor::PipeRead(id) => Descriptor::PipeRead(*id),
            Descriptor::PipeWrite(id) => Descriptor::PipeWrite(*id),
        };
        let mut new_fd = minimum.max(0);
        while state.entries.contains_key(&new_fd) {
            new_fd = new_fd.checked_add(1).ok_or(ERRNO_INVAL)?;
        }
        match &descriptor {
            Descriptor::PipeRead(id) => state.pipes.get_mut(id).unwrap().readers += 1,
            Descriptor::PipeWrite(id) => state.pipes.get_mut(id).unwrap().writers += 1,
            _ => {}
        }
        let flags = *state.status_flags.get(&fd).unwrap_or(&0);
        state.entries.insert(new_fd, descriptor);
        state.status_flags.insert(new_fd, flags);
        state.next = state.next.max(new_fd.saturating_add(1));
        Ok(new_fd)
    }

    fn get_flags(&self, fd: i32) -> Result<i32, i32> {
        self.descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .status_flags
            .get(&fd)
            .copied()
            .ok_or(ERRNO_BADF)
    }

    fn add_flags(&self, fd: i32, flags: i32) -> Result<(), i32> {
        let mut state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        *state.status_flags.get_mut(&fd).ok_or(ERRNO_BADF)? |= flags;
        Ok(())
    }

    fn is_tty(&self, fd: i32) -> Result<bool, i32> {
        let state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        Ok(matches!(
            state.entries.get(&fd).ok_or(ERRNO_BADF)?,
            Descriptor::TtyInput | Descriptor::TtyOutput
        ))
    }

    fn bytes_available(&self, fd: i32) -> Result<u32, i32> {
        let state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        match state.entries.get(&fd).ok_or(ERRNO_BADF)? {
            Descriptor::TtyInput => Ok(state.tty_input.len() as u32),
            Descriptor::PipeRead(id) => Ok(state.pipes.get(id).unwrap().bytes.len() as u32),
            _ => Err(ERRNO_INVAL),
        }
    }

    fn fstat(&self, fd: i32) -> Result<LegacyStat, i32> {
        let state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        match state.entries.get(&fd).ok_or(ERRNO_BADF)? {
            Descriptor::File { file, .. } => file
                .metadata()
                .map(|meta| metadata_stat(&meta))
                .map_err(|error| io_errno(&error)),
            Descriptor::TtyInput | Descriptor::TtyOutput => Ok(synthetic_stat(0o020_666)),
            Descriptor::PipeRead(_) | Descriptor::PipeWrite(_) => Ok(synthetic_stat(0o010_600)),
        }
    }

    fn contains(&self, fd: i32) -> bool {
        self.descriptors
            .lock()
            .expect("legacy fd table mutex poisoned")
            .entries
            .contains_key(&fd)
    }

    fn stat(&self, path: &str, nofollow: bool) -> Result<LegacyStat, i32> {
        let (_, path) = if nofollow {
            self.resolve_unfollowed(path)
        } else {
            self.resolve_existing(path)
        }
        .map_err(fs_errno)?;
        let metadata = if nofollow {
            std::fs::symlink_metadata(path)
        } else {
            std::fs::metadata(path)
        }
        .map_err(|error| io_errno(&error))?;
        Ok(metadata_stat(&metadata))
    }

    fn access(&self, path: &str, mode: i32) -> Result<(), i32> {
        if mode & !7 != 0 {
            return Err(ERRNO_INVAL);
        }
        let (mount, path) = self.resolve_existing(path).map_err(fs_errno)?;
        let metadata = path.metadata().map_err(|error| io_errno(&error))?;
        use std::os::unix::fs::MetadataExt;
        if mode & 2 != 0 && !mount.writable {
            return Err(2);
        }
        if mode & 4 != 0 && metadata.mode() & 0o444 == 0 {
            return Err(2);
        }
        if mode & 1 != 0 && metadata.mode() & 0o111 == 0 {
            return Err(2);
        }
        Ok(())
    }

    fn readlink(&self, path: &str) -> Result<Vec<u8>, i32> {
        use std::os::unix::ffi::OsStrExt;
        let (_, path) = self.resolve_unfollowed(path).map_err(fs_errno)?;
        std::fs::read_link(path)
            .map(|target| target.as_os_str().as_bytes().to_vec())
            .map_err(|error| io_errno(&error))
    }

    fn poll(&self, fds: &mut [PollFd], timeout_ms: i32) -> usize {
        let deadline = u64::try_from(timeout_ms)
            .ok()
            .map(|ms| Instant::now() + Duration::from_millis(ms));
        let mut state = self
            .descriptors
            .lock()
            .expect("legacy fd table mutex poisoned");
        loop {
            let ready = update_revents(&state, fds);
            if ready != 0 || timeout_ms == 0 {
                return ready;
            }
            state = match deadline {
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return 0;
                    };
                    let (state, result) = self
                        .readiness
                        .wait_timeout(state, remaining)
                        .expect("legacy fd table mutex poisoned");
                    if result.timed_out() {
                        update_revents(&state, fds);
                        return fds.iter().filter(|fd| fd.revents != 0).count();
                    }
                    state
                }
                None => self
                    .readiness
                    .wait(state)
                    .expect("legacy fd table mutex poisoned"),
            };
        }
    }

    #[cfg(test)]
    fn push_tty_input(&self, input: &[u8]) {
        self.descriptors.lock().unwrap().tty_input.extend(input);
        self.readiness.notify_all();
    }

    #[cfg(test)]
    fn tty_output(&self) -> Vec<u8> {
        self.descriptors.lock().unwrap().tty_output.clone()
    }
}

fn allocate_fd(state: &mut Descriptors) -> i32 {
    let fd = state.next;
    state.next = state.next.checked_add(1).unwrap_or(64);
    fd
}

fn drain(queue: &mut VecDeque<u8>, output: &mut [u8]) -> usize {
    let count = output.len().min(queue.len());
    for slot in &mut output[..count] {
        *slot = queue.pop_front().unwrap();
    }
    count
}

fn update_revents(state: &Descriptors, fds: &mut [PollFd]) -> usize {
    for pollfd in fds.iter_mut() {
        let available = match state.entries.get(&pollfd.fd) {
            None => POLLNVAL,
            Some(Descriptor::File { writable, .. }) => POLLIN | if *writable { POLLOUT } else { 0 },
            Some(Descriptor::TtyInput) => {
                if state.tty_input.is_empty() {
                    0
                } else {
                    POLLIN
                }
            }
            Some(Descriptor::TtyOutput) => POLLOUT,
            Some(Descriptor::PipeRead(id)) => {
                let pipe = state.pipes.get(id).unwrap();
                (if pipe.bytes.is_empty() { 0 } else { POLLIN })
                    | (if pipe.writers == 0 { POLLHUP } else { 0 })
            }
            Some(Descriptor::PipeWrite(id)) => {
                let pipe = state.pipes.get(id).unwrap();
                if pipe.readers == 0 {
                    POLLERR | POLLHUP
                } else {
                    POLLOUT
                }
            }
        };
        pollfd.revents = available & (pollfd.events | POLLERR | POLLHUP | POLLNVAL);
    }
    fds.iter().filter(|fd| fd.revents != 0).count()
}

fn metadata_stat(metadata: &std::fs::Metadata) -> LegacyStat {
    use std::os::unix::fs::MetadataExt;
    LegacyStat {
        dev: metadata.dev() as u32,
        mode: metadata.mode(),
        nlink: metadata.nlink() as u32,
        uid: metadata.uid(),
        gid: metadata.gid(),
        rdev: metadata.rdev() as u32,
        size: metadata.size() as i64,
        blocks: metadata.blocks() as u32,
        atime: metadata.atime(),
        atime_nsec: metadata.atime_nsec() as u32,
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec() as u32,
        ctime: metadata.ctime(),
        ctime_nsec: metadata.ctime_nsec() as u32,
        ino: metadata.ino(),
    }
}

fn synthetic_stat(mode: u32) -> LegacyStat {
    LegacyStat {
        dev: 1,
        mode,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        size: 0,
        blocks: 0,
        atime: 0,
        atime_nsec: 0,
        mtime: 0,
        mtime_nsec: 0,
        ctime: 0,
        ctime_nsec: 0,
        ino: 0,
    }
}

fn encode_stat(stat: &LegacyStat) -> [u8; 96] {
    let mut output = [0; 96];
    output[0..4].copy_from_slice(&stat.dev.to_le_bytes());
    output[4..8].copy_from_slice(&stat.mode.to_le_bytes());
    output[8..12].copy_from_slice(&stat.nlink.to_le_bytes());
    output[12..16].copy_from_slice(&stat.uid.to_le_bytes());
    output[16..20].copy_from_slice(&stat.gid.to_le_bytes());
    output[20..24].copy_from_slice(&stat.rdev.to_le_bytes());
    output[24..32].copy_from_slice(&stat.size.to_le_bytes());
    output[32..36].copy_from_slice(&4096_u32.to_le_bytes());
    output[36..40].copy_from_slice(&stat.blocks.to_le_bytes());
    output[40..48].copy_from_slice(&stat.atime.to_le_bytes());
    output[48..52].copy_from_slice(&stat.atime_nsec.to_le_bytes());
    output[56..64].copy_from_slice(&stat.mtime.to_le_bytes());
    output[64..68].copy_from_slice(&stat.mtime_nsec.to_le_bytes());
    output[72..80].copy_from_slice(&stat.ctime.to_le_bytes());
    output[80..84].copy_from_slice(&stat.ctime_nsec.to_le_bytes());
    output[88..96].copy_from_slice(&stat.ino.to_le_bytes());
    output
}

pub(crate) fn is_required(module: &Module) -> bool {
    module
        .imports()
        .any(|import| import.module() == "env" && import.name() == "__syscall_openat")
}

pub(crate) fn add_to_linker(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "_emscripten_receive_on_main_thread_js",
        |mut caller: Caller<'_, VmState>,
         index: i32,
         _thread: i32,
         encoded_count: i32,
         args: i32|
         -> wasmtime::Result<f64> {
            let args = decode_proxy_args(&mut caller, encoded_count, args).map_err(|errno| {
                wasmtime::Error::msg(format!(
                    "decoding proxied Emscripten call {index}: errno {errno}"
                ))
            })?;
            let value = dispatch_proxy(&mut caller, index, &args).map_err(|message| {
                wasmtime::Error::msg(format!("proxied Emscripten call {index}: {message}"))
            })?;
            Ok(f64::from(value))
        },
    )?;
    linker.func_wrap(
        "env",
        "_mmap_js",
        |mut caller: Caller<'_, VmState>,
         length: i32,
         _protection: i32,
         _flags: i32,
         fd: i32,
         offset: i64,
         allocated: i32,
         address: i32| {
            let result = (|| -> Result<(), i32> {
                let length = u32::try_from(length).map_err(|_| ERRNO_INVAL)?;
                let offset = u64::try_from(offset).map_err(|_| ERRNO_INVAL)?;
                let aligned = length.checked_add(65_535).ok_or(ERRNO_INVAL)? & !65_535;
                let allocator = caller
                    .get_export("emscripten_builtin_memalign")
                    .and_then(Extern::into_func)
                    .ok_or(ERRNO_INVAL)?
                    .typed::<(i32, i32), i32>(&caller)
                    .map_err(|_| ERRNO_INVAL)?;
                let pointer = allocator
                    .call(&mut caller, (65_536, aligned as i32))
                    .map_err(|_| ERRNO_IO)?;
                if pointer == 0 {
                    return Err(48);
                }
                let mut bytes = vec![0; length as usize];
                let table = caller.data().legacy_fds.clone();
                table.pread(fd, &mut bytes, offset)?;
                write_memory(&mut caller, pointer, &bytes)?;
                write_memory(&mut caller, allocated, &1_u32.to_le_bytes())?;
                write_memory(&mut caller, address, &(pointer as u32).to_le_bytes())
            })();
            result.map_or_else(|errno| -errno, |()| 0)
        },
    )?;
    linker.func_wrap(
        "env",
        "_msync_js",
        |mut caller: Caller<'_, VmState>,
         address: i32,
         length: i32,
         _protection: i32,
         flags: i32,
         fd: i32,
         offset: i64| {
            sync_mapping(&mut caller, address, length, flags, fd, offset)
                .map_or_else(|errno| -errno, |()| 0)
        },
    )?;
    linker.func_wrap(
        "env",
        "_munmap_js",
        |mut caller: Caller<'_, VmState>,
         address: i32,
         length: i32,
         protection: i32,
         flags: i32,
         fd: i32,
         offset: i64| {
            if protection & 2 == 0 {
                0
            } else {
                sync_mapping(&mut caller, address, length, flags, fd, offset)
                    .map_or_else(|errno| -errno, |()| 0)
            }
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_fcntl64",
        |mut caller: Caller<'_, VmState>, fd: i32, command: i32, varargs: i32| {
            let table = caller.data().legacy_fds.clone();
            let result = (|| -> Result<i32, i32> {
                match command {
                    0 | 1030 => table.duplicate(fd, read_u32(&mut caller, varargs)? as i32),
                    1 | 2 | 6 | 7 => {
                        table.get_flags(fd)?;
                        Ok(0)
                    }
                    3 => table.get_flags(fd),
                    4 => {
                        table.add_flags(fd, read_u32(&mut caller, varargs)? as i32)?;
                        Ok(0)
                    }
                    5 => {
                        table.get_flags(fd)?;
                        let pointer = read_u32(&mut caller, varargs)? as i32;
                        write_memory(&mut caller, pointer, &2_i16.to_le_bytes())?;
                        Ok(0)
                    }
                    8 | 16 => Err(ERRNO_INVAL),
                    _ => Err(ERRNO_INVAL),
                }
            })();
            result.unwrap_or_else(|errno| -errno)
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_ioctl",
        |mut caller: Caller<'_, VmState>, fd: i32, operation: i32, varargs: i32| {
            let table = caller.data().legacy_fds.clone();
            let result = (|| -> Result<i32, i32> {
                if operation == 21531 {
                    let output = read_u32(&mut caller, varargs)? as i32;
                    let count = table.bytes_available(fd)?;
                    write_memory(&mut caller, output, &count.to_le_bytes())?;
                    return Ok(0);
                }
                if !table.is_tty(fd)? {
                    return Err(ERRNO_NOTTY);
                }
                match operation {
                    21505 => {
                        let output = read_u32(&mut caller, varargs)? as i32;
                        write_memory(&mut caller, output, &[0; 49])?;
                        Ok(0)
                    }
                    21506..=21512 | 21515 | 21524 => Ok(0),
                    21519 => {
                        let output = read_u32(&mut caller, varargs)? as i32;
                        write_memory(&mut caller, output, &0_u32.to_le_bytes())?;
                        Ok(0)
                    }
                    21520 => Err(ERRNO_INVAL),
                    21523 => {
                        let output = read_u32(&mut caller, varargs)? as i32;
                        let mut winsize = [0; 8];
                        winsize[0..2].copy_from_slice(&24_u16.to_le_bytes());
                        winsize[2..4].copy_from_slice(&80_u16.to_le_bytes());
                        write_memory(&mut caller, output, &winsize)?;
                        Ok(0)
                    }
                    _ => Err(ERRNO_INVAL),
                }
            })();
            result.unwrap_or_else(|errno| -errno)
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_fstat64",
        |mut caller: Caller<'_, VmState>, fd: i32, output: i32| {
            let table = caller.data().legacy_fds.clone();
            match table.fstat(fd) {
                Ok(stat) => write_memory(&mut caller, output, &encode_stat(&stat))
                    .map_or_else(|errno| -errno, |()| 0),
                Err(errno) => -errno,
            }
        },
    )?;
    for (name, nofollow) in [("__syscall_stat64", false), ("__syscall_lstat64", true)] {
        linker.func_wrap(
            "env",
            name,
            move |mut caller: Caller<'_, VmState>, path: i32, output: i32| {
                path_stat(&mut caller, path, output, nofollow)
            },
        )?;
    }
    linker.func_wrap(
        "env",
        "__syscall_newfstatat",
        |mut caller: Caller<'_, VmState>, dirfd: i32, path: i32, output: i32, flags: i32| {
            if dirfd != AT_FDCWD {
                return -ERRNO_NOTCAPABLE;
            }
            path_stat(&mut caller, path, output, flags & 256 != 0)
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_faccessat",
        |mut caller: Caller<'_, VmState>, dirfd: i32, path: i32, mode: i32, _flags: i32| {
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
                .access(&path, mode)
                .map_or_else(|errno| -errno, |()| 0)
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_readlinkat",
        |mut caller: Caller<'_, VmState>, dirfd: i32, path: i32, output: i32, size: i32| {
            if dirfd != AT_FDCWD {
                return -ERRNO_NOTCAPABLE;
            }
            let result = (|| -> Result<i32, i32> {
                let size = usize::try_from(size).map_err(|_| ERRNO_INVAL)?;
                if size == 0 {
                    return Err(ERRNO_INVAL);
                }
                let path = read_string(&mut caller, path)?;
                let target = caller.data().legacy_fds.readlink(&path)?;
                let length = size.min(target.len());
                write_memory(&mut caller, output, &target[..length])?;
                i32::try_from(length).map_err(|_| ERRNO_INVAL)
            })();
            result.unwrap_or_else(|errno| -errno)
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_statfs64",
        |mut caller: Caller<'_, VmState>, path: i32, _size: i32, output: i32| {
            let result = (|| -> Result<(), i32> {
                let path = read_string(&mut caller, path)?;
                caller
                    .data()
                    .legacy_fds
                    .resolve_existing(&path)
                    .map_err(fs_errno)?;
                write_statfs(&mut caller, output)
            })();
            result.map_or_else(|errno| -errno, |()| 0)
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_fstatfs64",
        |mut caller: Caller<'_, VmState>, fd: i32, _size: i32, output: i32| {
            if !caller.data().legacy_fds.contains(fd) {
                return -ERRNO_BADF;
            }
            write_statfs(&mut caller, output).map_or_else(|errno| -errno, |()| 0)
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_pipe",
        |mut caller: Caller<'_, VmState>, output: i32| {
            let (read_fd, write_fd) = caller.data().legacy_fds.pipe();
            let mut pair = [0; 8];
            pair[..4].copy_from_slice(&read_fd.to_le_bytes());
            pair[4..].copy_from_slice(&write_fd.to_le_bytes());
            write_memory(&mut caller, output, &pair).map_or_else(|errno| -errno, |()| 0)
        },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_poll",
        |mut caller: Caller<'_, VmState>, pointer: i32, count: i32, timeout_ms: i32| {
            let result = (|| -> Result<i32, i32> {
                let count = usize::try_from(count).map_err(|_| ERRNO_INVAL)?;
                let mut fds = Vec::with_capacity(count);
                for index in 0..count {
                    let address = pointer
                        .checked_add(
                            i32::try_from(index.checked_mul(8).ok_or(ERRNO_INVAL)?)
                                .map_err(|_| ERRNO_INVAL)?,
                        )
                        .ok_or(ERRNO_INVAL)?;
                    let mut bytes = [0; 8];
                    read_memory(&mut caller, address, &mut bytes)?;
                    fds.push(PollFd {
                        fd: i32::from_le_bytes(bytes[..4].try_into().unwrap()),
                        events: i16::from_le_bytes(bytes[4..6].try_into().unwrap()),
                        revents: 0,
                    });
                }
                let table = caller.data().legacy_fds.clone();
                let ready = table.poll(&mut fds, timeout_ms);
                for (index, pollfd) in fds.iter().enumerate() {
                    let address = pointer
                        .checked_add(
                            i32::try_from(index.checked_mul(8).ok_or(ERRNO_INVAL)?)
                                .map_err(|_| ERRNO_INVAL)?,
                        )
                        .and_then(|address| address.checked_add(6))
                        .ok_or(ERRNO_INVAL)?;
                    write_memory(&mut caller, address, &pollfd.revents.to_le_bytes())?;
                }
                i32::try_from(ready).map_err(|_| ERRNO_INVAL)
            })();
            result.unwrap_or_else(|errno| -errno)
        },
    )?;
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

fn decode_proxy_args(
    caller: &mut Caller<'_, VmState>,
    encoded_count: i32,
    pointer: i32,
) -> Result<Vec<i64>, i32> {
    if encoded_count < 0 || encoded_count % 2 != 0 {
        return Err(ERRNO_INVAL);
    }
    let count = usize::try_from(encoded_count / 2).map_err(|_| ERRNO_INVAL)?;
    let mut output = Vec::with_capacity(count);
    for index in 0..count {
        let address = pointer
            .checked_add(
                i32::try_from(index.checked_mul(16).ok_or(ERRNO_INVAL)?)
                    .map_err(|_| ERRNO_INVAL)?,
            )
            .ok_or(ERRNO_INVAL)?;
        let mut pair = [0; 16];
        read_memory(caller, address, &mut pair)?;
        let tag = i64::from_le_bytes(pair[..8].try_into().unwrap());
        let bits = u64::from_le_bytes(pair[8..].try_into().unwrap());
        output.push(if tag == 0 {
            f64::from_bits(bits) as i64
        } else {
            bits as i64
        });
    }
    Ok(output)
}

fn dispatch_proxy(
    caller: &mut Caller<'_, VmState>,
    index: i32,
    args: &[i64],
) -> Result<i32, String> {
    let arg = |index: usize| -> Result<i32, String> {
        i32::try_from(
            *args
                .get(index)
                .ok_or_else(|| format!("missing argument {index}"))?,
        )
        .map_err(|_| format!("argument {index} does not fit i32"))
    };
    let table = caller.data().legacy_fds.clone();
    let syscall = match index {
        9 => {
            let dirfd = arg(0)?;
            if dirfd != AT_FDCWD {
                Err(ERRNO_NOTCAPABLE)
            } else {
                let path = read_string(caller, arg(1)?).map_err(|errno| errno.to_string())?;
                table.access(&path, arg(2)?).map(|()| 0)
            }
        }
        13 => return Ok(fcntl_call(caller, arg(0)?, arg(1)?, arg(2)?)),
        14 => match table.fstat(arg(0)?) {
            Ok(stat) => write_memory(caller, arg(1)?, &encode_stat(&stat)).map(|()| 0),
            Err(errno) => Err(errno),
        },
        15 => {
            if !table.contains(arg(0)?) {
                Err(ERRNO_BADF)
            } else {
                write_statfs(caller, arg(2)?).map(|()| 0)
            }
        }
        23 => return Ok(ioctl_call(caller, arg(0)?, arg(1)?, arg(2)?)),
        27 => {
            if arg(0)? != AT_FDCWD {
                Err(ERRNO_NOTCAPABLE)
            } else {
                let path = read_string(caller, arg(1)?).map_err(|errno| errno.to_string())?;
                match table.stat(&path, arg(3)? & 256 != 0) {
                    Ok(stat) => write_memory(caller, arg(2)?, &encode_stat(&stat)).map(|()| 0),
                    Err(errno) => Err(errno),
                }
            }
        }
        28 => {
            if arg(0)? != AT_FDCWD {
                Err(ERRNO_NOTCAPABLE)
            } else {
                let path = read_string(caller, arg(1)?).map_err(|errno| errno.to_string())?;
                return Ok(table.open(&path, arg(2)?).unwrap_or_else(|errno| -errno));
            }
        }
        29 => {
            let (reader, writer) = table.pipe();
            let mut pair = [0; 8];
            pair[..4].copy_from_slice(&reader.to_le_bytes());
            pair[4..].copy_from_slice(&writer.to_le_bytes());
            write_memory(caller, arg(0)?, &pair).map(|()| 0)
        }
        30 => return Ok(poll_call(caller, arg(0)?, arg(1)?, arg(2)?)),
        // The synchronous Rust poll/read implementations wait on the shared
        // descriptor condvar directly, so the JS-only atomic wake helper does
        // not participate. Complete it immediately if the artifact queues it.
        31 => {
            let address = arg(0)?
                .checked_mul(4)
                .ok_or_else(|| "atomic index overflow".to_owned())?;
            write_memory(caller, address, &2_i32.to_le_bytes())
                .map_err(|errno| errno.to_string())?;
            return Ok(0);
        }
        32 => {
            if arg(0)? != AT_FDCWD {
                Err(ERRNO_NOTCAPABLE)
            } else {
                let path = read_string(caller, arg(1)?).map_err(|errno| errno.to_string())?;
                let target = table.readlink(&path).map_err(|errno| errno.to_string())?;
                let size =
                    usize::try_from(arg(3)?).map_err(|_| "negative readlink size".to_owned())?;
                if size == 0 {
                    Err(ERRNO_INVAL)
                } else {
                    let length = size.min(target.len());
                    write_memory(caller, arg(2)?, &target[..length])
                        .map_err(|errno| errno.to_string())?;
                    return Ok(length as i32);
                }
            }
        }
        45 => {
            return Ok(mmap_call(
                caller,
                arg(0)?,
                arg(3)?,
                *args
                    .get(4)
                    .ok_or_else(|| "missing mmap offset".to_owned())?,
                arg(5)?,
                arg(6)?,
            ));
        }
        50 => return Ok(table.close(arg(0)?).err().unwrap_or(0)),
        52 => {
            return Ok(vectored_io(
                caller,
                arg(0)?,
                arg(1)?,
                arg(2)?,
                arg(4)?,
                Some(args[3] as u64),
                false,
            ));
        }
        54 => {
            return Ok(vectored_io(
                caller,
                arg(0)?,
                arg(1)?,
                arg(2)?,
                arg(3)?,
                None,
                false,
            ));
        }
        55 => match table.seek(
            arg(0)?,
            args.get(1)
                .copied()
                .ok_or_else(|| "missing offset".to_owned())?,
            arg(2)?,
        ) {
            Ok(position) => write_memory(caller, arg(3)?, &position.to_le_bytes()).map(|()| 0),
            Err(errno) => Err(errno),
        },
        57 => {
            return Ok(vectored_io(
                caller,
                arg(0)?,
                arg(1)?,
                arg(2)?,
                arg(3)?,
                None,
                true,
            ));
        }
        _ => return Err("unsupported proxy table index".to_owned()),
    };
    Ok(syscall.unwrap_or_else(|errno| -errno))
}

fn fcntl_call(caller: &mut Caller<'_, VmState>, fd: i32, command: i32, varargs: i32) -> i32 {
    let table = caller.data().legacy_fds.clone();
    let result = (|| -> Result<i32, i32> {
        match command {
            0 | 1030 => table.duplicate(fd, read_u32(caller, varargs)? as i32),
            1 | 2 | 6 | 7 => {
                table.get_flags(fd)?;
                Ok(0)
            }
            3 => table.get_flags(fd),
            4 => {
                table.add_flags(fd, read_u32(caller, varargs)? as i32)?;
                Ok(0)
            }
            5 => {
                table.get_flags(fd)?;
                let pointer = read_u32(caller, varargs)? as i32;
                write_memory(caller, pointer, &2_i16.to_le_bytes())?;
                Ok(0)
            }
            _ => Err(ERRNO_INVAL),
        }
    })();
    result.unwrap_or_else(|errno| -errno)
}

fn ioctl_call(caller: &mut Caller<'_, VmState>, fd: i32, operation: i32, varargs: i32) -> i32 {
    let table = caller.data().legacy_fds.clone();
    let result = (|| -> Result<i32, i32> {
        if operation == 21531 {
            let output = read_u32(caller, varargs)? as i32;
            write_memory(caller, output, &table.bytes_available(fd)?.to_le_bytes())?;
            return Ok(0);
        }
        if !table.is_tty(fd)? {
            return Err(ERRNO_NOTTY);
        }
        match operation {
            21505 => {
                let output = read_u32(caller, varargs)? as i32;
                write_memory(caller, output, &[0; 49])?;
                Ok(0)
            }
            21506..=21512 | 21515 | 21524 => Ok(0),
            21519 => {
                let output = read_u32(caller, varargs)? as i32;
                write_memory(caller, output, &0_u32.to_le_bytes())?;
                Ok(0)
            }
            21520 => Err(ERRNO_INVAL),
            21523 => {
                let output = read_u32(caller, varargs)? as i32;
                let mut winsize = [0; 8];
                winsize[..2].copy_from_slice(&24_u16.to_le_bytes());
                winsize[2..4].copy_from_slice(&80_u16.to_le_bytes());
                write_memory(caller, output, &winsize)?;
                Ok(0)
            }
            _ => Err(ERRNO_INVAL),
        }
    })();
    result.unwrap_or_else(|errno| -errno)
}

fn poll_call(caller: &mut Caller<'_, VmState>, pointer: i32, count: i32, timeout_ms: i32) -> i32 {
    let result = (|| -> Result<i32, i32> {
        let count = usize::try_from(count).map_err(|_| ERRNO_INVAL)?;
        let mut fds = Vec::with_capacity(count);
        for index in 0..count {
            let address = pointer
                .checked_add(i32::try_from(index * 8).map_err(|_| ERRNO_INVAL)?)
                .ok_or(ERRNO_INVAL)?;
            let mut bytes = [0; 8];
            read_memory(caller, address, &mut bytes)?;
            fds.push(PollFd {
                fd: i32::from_le_bytes(bytes[..4].try_into().unwrap()),
                events: i16::from_le_bytes(bytes[4..6].try_into().unwrap()),
                revents: 0,
            });
        }
        let table = caller.data().legacy_fds.clone();
        let ready = table.poll(&mut fds, timeout_ms);
        for (index, fd) in fds.iter().enumerate() {
            let address = pointer
                .checked_add(i32::try_from(index * 8 + 6).map_err(|_| ERRNO_INVAL)?)
                .ok_or(ERRNO_INVAL)?;
            write_memory(caller, address, &fd.revents.to_le_bytes())?;
        }
        i32::try_from(ready).map_err(|_| ERRNO_INVAL)
    })();
    result.unwrap_or_else(|errno| -errno)
}

fn mmap_call(
    caller: &mut Caller<'_, VmState>,
    length: i32,
    fd: i32,
    offset: i64,
    allocated: i32,
    address: i32,
) -> i32 {
    let result = (|| -> Result<(), i32> {
        let length = u32::try_from(length).map_err(|_| ERRNO_INVAL)?;
        let offset = u64::try_from(offset).map_err(|_| ERRNO_INVAL)?;
        let aligned = length.checked_add(65_535).ok_or(ERRNO_INVAL)? & !65_535;
        let allocator = caller
            .get_export("emscripten_builtin_memalign")
            .and_then(Extern::into_func)
            .ok_or(ERRNO_INVAL)?
            .typed::<(i32, i32), i32>(&caller)
            .map_err(|_| ERRNO_INVAL)?;
        let pointer = allocator
            .call(&mut *caller, (65_536, aligned as i32))
            .map_err(|_| ERRNO_IO)?;
        if pointer == 0 {
            return Err(48);
        }
        let mut bytes = vec![0; length as usize];
        caller.data().legacy_fds.pread(fd, &mut bytes, offset)?;
        write_memory(caller, pointer, &bytes)?;
        write_memory(caller, allocated, &1_u32.to_le_bytes())?;
        write_memory(caller, address, &(pointer as u32).to_le_bytes())
    })();
    result.map_or_else(|errno| -errno, |()| 0)
}

fn path_stat(caller: &mut Caller<'_, VmState>, path: i32, output: i32, nofollow: bool) -> i32 {
    let result = (|| -> Result<(), i32> {
        let path = read_string(caller, path)?;
        let stat = caller.data().legacy_fds.stat(&path, nofollow)?;
        write_memory(caller, output, &encode_stat(&stat))
    })();
    result.map_or_else(|errno| -errno, |()| 0)
}

fn sync_mapping(
    caller: &mut Caller<'_, VmState>,
    address: i32,
    length: i32,
    flags: i32,
    fd: i32,
    offset: i64,
) -> Result<(), i32> {
    if flags & 2 != 0 {
        return Ok(());
    }
    let length = usize::try_from(length).map_err(|_| ERRNO_INVAL)?;
    let offset = u64::try_from(offset).map_err(|_| ERRNO_INVAL)?;
    let mut bytes = vec![0; length];
    read_memory(caller, address, &mut bytes)?;
    let table = caller.data().legacy_fds.clone();
    let written = table.pwrite(fd, &bytes, offset)?;
    if written == bytes.len() {
        Ok(())
    } else {
        Err(ERRNO_IO)
    }
}

fn write_statfs(caller: &mut Caller<'_, VmState>, output: i32) -> Result<(), i32> {
    let mut bytes = [0; 64];
    for (offset, value) in [
        (4, 4096_u32),
        (8, 1_000_000),
        (12, 500_000),
        (16, 500_000),
        (20, 1),
        (24, 1_000_000),
        (28, 42),
        (36, 255),
        (40, 4096),
        (44, 2),
    ] {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }
    write_memory(caller, output, &bytes)
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
    fn polls_tty_pipes_files_and_timeouts_in_one_namespace() {
        let (_pack, table) = fixture();
        let file = table.open("/pack/hello", O_RDONLY).unwrap();
        let (reader, writer) = table.pipe();
        let mut fds = [
            PollFd {
                fd: 0,
                events: POLLIN,
                revents: 0,
            },
            PollFd {
                fd: 1,
                events: POLLOUT,
                revents: 0,
            },
            PollFd {
                fd: file,
                events: POLLIN | POLLOUT,
                revents: 0,
            },
            PollFd {
                fd: reader,
                events: POLLIN,
                revents: 0,
            },
            PollFd {
                fd: 999,
                events: POLLIN,
                revents: 0,
            },
        ];
        assert_eq!(table.poll(&mut fds, 0), 3);
        assert_eq!(fds.map(|fd| fd.revents), [0, POLLOUT, POLLIN, 0, POLLNVAL]);

        table.push_tty_input(b"in");
        table.write(1, b"out").unwrap();
        table.write(writer, b"pipe").unwrap();
        assert_eq!(table.poll(&mut fds, 0), 5);
        assert_eq!(fds[0].revents, POLLIN);
        assert_eq!(fds[3].revents, POLLIN);
        assert_eq!(table.tty_output(), b"out");

        let mut input = [0; 4];
        assert_eq!(table.read(reader, &mut input).unwrap(), 4);
        assert_eq!(&input, b"pipe");
        table.close(writer).unwrap();
        fds[3].revents = 0;
        assert_eq!(table.poll(&mut fds[3..4], 0), 1);
        assert_eq!(fds[3].revents, POLLHUP);

        let started = Instant::now();
        let mut idle = [PollFd {
            fd: reader,
            events: POLLIN,
            revents: 0,
        }];
        // HUP is always reported, so use an empty stdin to exercise timeout.
        idle[0].fd = 0;
        table.read(0, &mut input).unwrap();
        assert_eq!(table.poll(&mut idle, 15), 0);
        assert!(started.elapsed() >= Duration::from_millis(10));
    }

    #[test]
    fn poll_wait_is_woken_by_pipe_output() {
        let (_pack, table) = fixture();
        let (reader, writer) = table.pipe();
        let producer = table.clone();
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            producer.write(writer, b"x").unwrap();
        });
        let mut fds = [PollFd {
            fd: reader,
            events: POLLIN,
            revents: 0,
        }];
        assert_eq!(table.poll(&mut fds, 1_000), 1);
        assert_eq!(fds[0].revents, POLLIN);
        thread.join().unwrap();
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

    #[test]
    fn links_pipe_and_poll_with_the_emscripten_abi_layout() {
        let (_pack, table) = fixture();
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "__syscall_pipe" (func $pipe (param i32) (result i32)))
                (import "env" "__syscall_poll" (func $poll (param i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_read"
                    (func $read (param i32 i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_write"
                    (func $write (param i32 i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_close"
                    (func $close (param i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 64) "xyz")
                (func (export "run") (result i32)
                    (local $reader i32) (local $writer i32)
                    i32.const 0 call $pipe
                    if i32.const 1 return end
                    i32.const 0 i32.load local.set $reader
                    i32.const 4 i32.load local.set $writer
                    i32.const 32 i32.const 64 i32.store
                    i32.const 36 i32.const 3 i32.store
                    local.get $writer i32.const 32 i32.const 1 i32.const 40 call $write
                    if i32.const 2 return end
                    i32.const 16 local.get $reader i32.store
                    i32.const 20 i32.const 1 i32.store16
                    i32.const 16 i32.const 1 i32.const 0 call $poll
                    i32.const 1 i32.ne if i32.const 3 return end
                    i32.const 22 i32.load16_s i32.const 1 i32.ne
                    if i32.const 4 return end
                    i32.const 48 i32.const 80 i32.store
                    i32.const 52 i32.const 3 i32.store
                    local.get $reader i32.const 48 i32.const 1 i32.const 56 call $read
                    if i32.const 5 return end
                    local.get $writer call $close drop
                    i32.const 16 i32.const 1 i32.const 0 call $poll drop
                    i32.const 22 i32.load16_s i32.const 16 i32.ne
                    if i32.const 6 return end
                    i32.const 80 i32.load8_u i32.const 120 i32.ne
                    if i32.const 7 return end
                    i32.const 82 i32.load8_u i32.const 122 i32.ne
                    if i32.const 8 return end
                    i32.const 0))"#,
        )
        .unwrap();
        let host_fs = crate::host_fs::HostFs::new(&[]).unwrap();
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
    }

    #[test]
    fn links_stat_access_readlink_and_statfs_with_legacy_layouts() {
        use std::os::unix::fs::symlink;

        let (pack, table) = fixture();
        symlink("hello", pack.path().join("link")).unwrap();
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "__syscall_openat"
                    (func $open (param i32 i32 i32 i32) (result i32)))
                (import "env" "__syscall_fstat64"
                    (func $fstat (param i32 i32) (result i32)))
                (import "env" "__syscall_newfstatat"
                    (func $stat (param i32 i32 i32 i32) (result i32)))
                (import "env" "__syscall_faccessat"
                    (func $access (param i32 i32 i32 i32) (result i32)))
                (import "env" "__syscall_readlinkat"
                    (func $readlink (param i32 i32 i32 i32) (result i32)))
                (import "env" "__syscall_fstatfs64"
                    (func $statfs (param i32 i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 0) "/pack/hello\00")
                (data (i32.const 32) "/pack/link\00")
                (func (export "run") (result i32)
                    (local $fd i32)
                    i32.const -100 i32.const 0 i32.const 0 i32.const 0
                    call $open local.tee $fd
                    i32.const 128 call $fstat
                    if i32.const 1 return end
                    i32.const 152 i64.load i64.const 11 i64.ne
                    if i32.const 2 return end
                    i32.const -100 i32.const 0 i32.const 224 i32.const 0 call $stat
                    if i32.const 3 return end
                    i32.const 248 i64.load i64.const 11 i64.ne
                    if i32.const 4 return end
                    i32.const -100 i32.const 0 i32.const 4 i32.const 0 call $access
                    if i32.const 5 return end
                    i32.const -100 i32.const 32 i32.const 400 i32.const 5 call $readlink
                    i32.const 5 i32.ne if i32.const 6 return end
                    i32.const 400 i32.load8_u i32.const 104 i32.ne
                    if i32.const 7 return end
                    local.get $fd i32.const 64 i32.const 500 call $statfs
                    if i32.const 8 return end
                    i32.const 504 i32.load i32.const 4096 i32.ne
                    if i32.const 9 return end
                    i32.const 0))"#,
        )
        .unwrap();
        let mapdirs = [MapDir::read_only(
            pack.path().to_owned(),
            "/pack".to_owned(),
        )];
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
        assert_eq!(
            &instance
                .get_memory(&mut store, "memory")
                .unwrap()
                .data(&store)[400..405],
            b"hello"
        );
    }

    #[test]
    fn links_fcntl_and_tty_pipe_ioctls() {
        let (_pack, table) = fixture();
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "__syscall_pipe" (func $pipe (param i32) (result i32)))
                (import "env" "__syscall_fcntl64"
                    (func $fcntl (param i32 i32 i32) (result i32)))
                (import "env" "__syscall_ioctl"
                    (func $ioctl (param i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_write"
                    (func $write (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 64) "xyz")
                (func (export "run") (result i32)
                    (local $reader i32) (local $writer i32) (local $duplicate i32)
                    i32.const 0 call $pipe drop
                    i32.const 0 i32.load local.set $reader
                    i32.const 4 i32.load local.set $writer
                    i32.const 16 i32.const 64 i32.store
                    i32.const 20 i32.const 3 i32.store
                    local.get $writer i32.const 16 i32.const 1 i32.const 24 call $write drop
                    local.get $reader i32.const 3 i32.const 0 call $fcntl
                    if i32.const 1 return end
                    i32.const 40 i32.const 100 i32.store
                    local.get $reader i32.const 0 i32.const 40 call $fcntl
                    local.tee $duplicate i32.const 100 i32.ne
                    if i32.const 2 return end
                    i32.const 44 i32.const 48 i32.store
                    local.get $duplicate i32.const 21531 i32.const 44 call $ioctl
                    if i32.const 3 return end
                    i32.const 48 i32.load i32.const 3 i32.ne
                    if i32.const 4 return end
                    i32.const 52 i32.const 56 i32.store
                    i32.const 1 i32.const 21523 i32.const 52 call $ioctl
                    if i32.const 5 return end
                    i32.const 56 i32.load16_u i32.const 24 i32.ne
                    if i32.const 6 return end
                    i32.const 58 i32.load16_u i32.const 80 i32.ne
                    if i32.const 7 return end
                    i32.const 0))"#,
        )
        .unwrap();
        let host_fs = crate::host_fs::HostFs::new(&[]).unwrap();
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
    }

    #[test]
    fn maps_legacy_files_through_the_guest_allocator() {
        let (_pack, table) = fixture();
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "__syscall_openat"
                    (func $open (param i32 i32 i32 i32) (result i32)))
                (import "env" "_mmap_js"
                    (func $mmap (param i32 i32 i32 i32 i64 i32 i32) (result i32)))
                (import "env" "_munmap_js"
                    (func $munmap (param i32 i32 i32 i32 i32 i64) (result i32)))
                (memory (export "memory") 2)
                (data (i32.const 0) "/pack/hello\00")
                (func (export "emscripten_builtin_memalign") (param i32 i32) (result i32)
                    i32.const 65536)
                (func (export "run") (result i32)
                    (local $fd i32)
                    i32.const -100 i32.const 0 i32.const 0 i32.const 0
                    call $open local.set $fd
                    i32.const 5 i32.const 1 i32.const 2 local.get $fd i64.const 6
                    i32.const 100 i32.const 104 call $mmap
                    if i32.const 1 return end
                    i32.const 100 i32.load i32.const 1 i32.ne
                    if i32.const 2 return end
                    i32.const 104 i32.load i32.const 65536 i32.ne
                    if i32.const 3 return end
                    i32.const 65536 i32.load8_u i32.const 119 i32.ne
                    if i32.const 4 return end
                    i32.const 65540 i32.load8_u i32.const 100 i32.ne
                    if i32.const 5 return end
                    i32.const 65536 i32.const 5 i32.const 0 i32.const 2
                    local.get $fd i64.const 6 call $munmap
                    if i32.const 6 return end
                    i32.const 0))"#,
        )
        .unwrap();
        let host_fs = crate::host_fs::HostFs::new(&[]).unwrap();
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
    }

    #[test]
    fn dispatches_tagged_main_thread_proxy_arguments() {
        let (_pack, table) = fixture();
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "_emscripten_receive_on_main_thread_js"
                    (func $proxy (param i32 i32 i32 i32) (result f64)))
                (memory (export "memory") 1)
                (data (i32.const 256) "/pack/hello\00")
                (func (export "run") (result i32)
                    (local $fd i32)
                    ;; Four non-BigInt arguments are encoded as tag=0,f64 payload.
                    i32.const 8 f64.const -100 f64.store
                    i32.const 24 f64.const 256 f64.store
                    i32.const 40 f64.const 0 f64.store
                    i32.const 56 f64.const 0 f64.store
                    i32.const 28 i32.const 123 i32.const 8 i32.const 0 call $proxy
                    i32.trunc_f64_s local.tee $fd
                    i32.const 0 i32.lt_s if i32.const 1 return end
                    ;; fstat(fd, 320), also through proxy index 14.
                    i32.const 72 local.get $fd f64.convert_i32_s f64.store
                    i32.const 88 f64.const 320 f64.store
                    i32.const 14 i32.const 123 i32.const 4 i32.const 64 call $proxy
                    i32.trunc_f64_s i32.const 0 i32.ne
                    if i32.const 2 return end
                    i32.const 344 i64.load i64.const 11 i64.ne
                    if i32.const 3 return end
                    i32.const 0))"#,
        )
        .unwrap();
        let host_fs = crate::host_fs::HostFs::new(&[]).unwrap();
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
    }
}
