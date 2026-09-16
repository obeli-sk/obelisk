use anyhow::{Context, bail, ensure};
use concepts::storage::http_client_trace::HttpClientTrace;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use wasm_workers::http_request_policy::HttpRequestPolicy;
use wasmtime::{
    Engine, Extern, ExternType, Instance, Linker, Memory, MemoryType, Module, Ref, SharedMemory,
    Store, Table, TableType,
};
use wasmtime_wasi::{
    FsPerms, WasiCtxBuilder, async_trait,
    cli::{IsTerminal, StdinStream},
    p1,
    p2::{InputStream, Pollable, StreamResult, pipe},
};

mod emscripten;
mod host_fs;
mod http_bridge;
mod legacy_fd;
mod legacy_wasi;
mod qemu_jit;

struct VmState {
    wasi: p1::WasiP1Ctx,
    qemu_jit: qemu_jit::QemuJit,
    host_fs: Arc<host_fs::HostFs>,
    legacy_fds: Arc<legacy_fd::LegacyFdTable>,
    fiber_next: Option<i32>,
    fiber_entries: Arc<Mutex<HashMap<i32, (i32, i32)>>>,
    poll_calls: u64,
    pthread_spawn: Option<PthreadSpawn>,
}

type PthreadSpawn = Arc<dyn Fn(i32, i32, i32, i32) -> wasmtime::Result<i32> + Send + Sync>;

struct EmscriptenRuntime {
    engine: Engine,
    module: Module,
    memory: SharedMemory,
    table_import: Option<(String, String, TableType)>,
    legacy_jit: bool,
    host_fs: Arc<host_fs::HostFs>,
    legacy_fds: Arc<legacy_fd::LegacyFdTable>,
    fiber_entries: Arc<Mutex<HashMap<i32, (i32, i32)>>>,
    mapdirs: Vec<MapDir>,
    arguments: Arc<[String]>,
    stdout: pipe::MemoryOutputPipe,
    stderr: pipe::MemoryOutputPipe,
    threads: Mutex<Vec<std::thread::JoinHandle<anyhow::Result<()>>>>,
    cancelled: Arc<AtomicBool>,
}

const QEMU_RESUME_INPUT: &[u8] = b"\x01ccont\n\x01c=\n";

/// A serial input which supplies the snapshot-resume handshake once and then
/// remains open. Returning `Closed` after the handshake makes QEMU's stdio
/// character device treat stdin as a hangup and terminate the VM.
struct ResumeInput {
    bytes: Arc<Mutex<bytes::Bytes>>,
}

impl ResumeInput {
    fn new() -> Self {
        Self {
            bytes: Arc::new(Mutex::new(bytes::Bytes::from_static(QEMU_RESUME_INPUT))),
        }
    }
}

#[async_trait]
impl InputStream for ResumeInput {
    fn read(&mut self, size: usize) -> StreamResult<bytes::Bytes> {
        let mut bytes = self.bytes.lock().expect("resume input mutex poisoned");
        let size = size.min(bytes.len());
        eprintln!(
            "QEMU resume p2 read requested={size} remaining={}",
            bytes.len()
        );
        Ok(bytes.split_to(size))
    }
}

#[async_trait]
impl Pollable for ResumeInput {
    async fn ready(&mut self) {
        if self
            .bytes
            .lock()
            .expect("resume input mutex poisoned")
            .is_empty()
        {
            std::future::pending::<()>().await;
        }
    }
}

impl IsTerminal for ResumeInput {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdinStream for ResumeInput {
    fn async_stream(&self) -> Box<dyn tokio::io::AsyncRead + Send + Sync> {
        Box::new(ResumeAsyncRead {
            bytes: self.bytes.clone(),
        })
    }

    fn p2_stream(&self) -> Box<dyn InputStream> {
        Box::new(Self {
            bytes: self.bytes.clone(),
        })
    }
}

struct ResumeAsyncRead {
    bytes: Arc<Mutex<bytes::Bytes>>,
}

impl tokio::io::AsyncRead for ResumeAsyncRead {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut bytes = self.bytes.lock().expect("resume input mutex poisoned");
        if bytes.is_empty() {
            return std::task::Poll::Pending;
        }
        let size = buffer.remaining().min(bytes.len());
        eprintln!(
            "QEMU resume p1 read requested={size} remaining={}",
            bytes.len()
        );
        buffer.put_slice(&bytes.split_to(size));
        std::task::Poll::Ready(Ok(()))
    }
}

impl qemu_jit::HasQemuJit for VmState {
    fn qemu_jit(&mut self) -> &mut qemu_jit::QemuJit {
        &mut self.qemu_jit
    }
}

pub struct VmOutput {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub serial: Vec<u8>,
    pub http_client_traces: Vec<HttpClientTrace>,
}

#[derive(Clone)]
pub struct MapDir {
    host: PathBuf,
    guest: String,
    permissions: FsPerms,
    qemu_store_image: Option<String>,
}

#[derive(Clone, Debug)]
pub struct QemuRuntimeConfig {
    pub args: Vec<String>,
    pub image_dir: PathBuf,
}

impl MapDir {
    #[must_use]
    pub fn read_only(host: PathBuf, guest: String) -> Self {
        Self {
            host,
            guest,
            permissions: FsPerms::ReadOnly,
            qemu_store_image: None,
        }
    }

    #[must_use]
    pub fn read_write(host: PathBuf, guest: String) -> Self {
        Self {
            host,
            guest,
            permissions: FsPerms::ReadWrite,
            qemu_store_image: None,
        }
    }

    pub fn qemu_store_squashfs(host: PathBuf) -> anyhow::Result<Self> {
        anyhow::ensure!(
            host.file_name()
                .is_some_and(|name| name == "store.squashfs"),
            "QEMU store image must be named store.squashfs"
        );
        let parent = host
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .context("QEMU store image must have a parent directory")?;
        Ok(Self {
            host: parent.to_owned(),
            guest: "/obelisk-activity-vm-store".to_owned(),
            permissions: FsPerms::ReadOnly,
            qemu_store_image: Some("obelisk-activity-vm-store/store.squashfs".to_owned()),
        })
    }
}

#[expect(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn execute(
    engine: &Engine,
    module: Module,
    mut mapdirs: Vec<MapDir>,
    qemu_runtime: Option<QemuRuntimeConfig>,
    guest_args: Vec<String>,
    mut env: HashMap<String, String>,
    stdin: Option<Vec<u8>>,
    policy: HttpRequestPolicy,
    cancelled: Arc<AtomicBool>,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
) -> anyhow::Result<VmOutput> {
    let started = Instant::now();
    tracing::debug!("Preparing activity VM execution");
    let is_qemu = qemu_memory_type(&module)?.is_some();
    let is_legacy_qemu = legacy_fd::is_required(&module);
    if let Some(runtime) = &qemu_runtime {
        ensure!(is_qemu, "QEMU runtime configuration requires a QEMU module");
        mapdirs.push(MapDir::read_only(
            runtime.image_dir.clone(),
            "/image".to_owned(),
        ));
    }
    let has_nix_store = mapdirs.iter().any(|mapdir| {
        mapdir.guest == "/nix/store"
            || mapdir.guest.starts_with("/nix/store/")
            || mapdir.qemu_store_image.is_some()
    });
    env.insert(
        "OBELISK_ACTIVITY_VM_NIX_STORE".to_owned(),
        if has_nix_store { "1" } else { "0" }.to_owned(),
    );
    let uses_qemu_pack = is_qemu && (qemu_runtime.is_some() || !is_legacy_qemu);
    let qemu_pack = uses_qemu_pack.then(tempfile::tempdir).transpose()?;
    let queue_temp = (!uses_qemu_pack).then(tempfile::tempdir).transpose()?;
    let queue = if let Some(pack) = &qemu_pack {
        let queue = pack.path().join("obelisk-activity-vm-http");
        tokio::fs::create_dir(&queue).await?;
        queue
    } else {
        queue_temp
            .as_ref()
            .context("missing activity VM queue")?
            .path()
            .to_owned()
    };
    tokio::fs::write(
        queue.join("http-guest.sh"),
        include_bytes!("../guest/http-guest.sh"),
    )
    .await?;
    for name in [
        "exit-code",
        "phase-command-start",
        "phase-guest-launcher",
        "phase-network-ready",
        "phase-store-mounted",
        "proxy.log",
        "stderr",
        "stdout",
    ] {
        tokio::fs::write(queue.join(name), []).await?;
    }
    if let Some(stdin) = stdin {
        tokio::fs::write(queue.join("stdin.json"), stdin).await?;
        env.insert(
            "OBELISK_ACTIVITY_VM_STDIN".to_owned(),
            "/obelisk-activity-vm-http/stdin.json".to_owned(),
        );
    }
    if !uses_qemu_pack {
        mapdirs.push(MapDir::read_write(
            queue.clone(),
            "/obelisk-activity-vm-http".to_owned(),
        ));
    }
    if let Some(pack) = &qemu_pack {
        tokio::fs::write(
            pack.path().join("info"),
            qemu_runtime_info(&mapdirs, &["obelisk-activity-vm-http"], &guest_args, &env)?,
        )
        .await?;
        mapdirs.push(MapDir::read_write(
            pack.path().to_owned(),
            "/pack".to_owned(),
        ));
    }
    let module_args = if let Some(runtime) = qemu_runtime {
        runtime.args
    } else if is_qemu && !is_legacy_qemu {
        qemu_args()?
    } else {
        guest_args
    };
    let traces = Arc::new(Mutex::new(Vec::new()));
    let broker = tokio::spawn(http_bridge::serve(queue.clone(), policy, traces.clone()));
    let (phase_logger_stop, phase_logger_stop_rx) = tokio::sync::oneshot::channel();
    let phase_logger = tokio::spawn(log_guest_phases(
        queue.clone(),
        started,
        phase_logger_stop_rx,
    ));
    let engine = engine.clone();
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Starting activity VM module"
    );
    let result = tokio::task::spawn_blocking(move || {
        run_module(
            &engine,
            &module,
            &mapdirs,
            &module_args,
            &env,
            max_stdout_bytes,
            max_stderr_bytes,
            cancelled,
        )
    })
    .await?;
    broker.abort();
    let _ = broker.await;
    let _ = phase_logger_stop.send(());
    let _ = phase_logger.await;
    let mut output = match result {
        Ok(output) => output,
        Err(error) => {
            eprintln!(
                "activity VM queue after failure: {:?}",
                directory_entries(&queue)
            );
            if let Some(pack) = &qemu_pack {
                eprintln!(
                    "activity VM pack after failure: {:?}",
                    directory_entries(pack.path())
                );
            }
            return Err(error);
        }
    };
    let has_guest_output =
        replace_output_from_guest_files(&mut output, &queue, max_stdout_bytes, max_stderr_bytes)
            .await?;
    if is_qemu {
        ensure!(
            has_guest_output,
            "QEMU guest did not publish output through the 9p queue"
        );
    }
    if is_qemu {
        if let Some(pack) = &qemu_pack
            && let Ok(console) = tokio::fs::read(pack.path().join("console.log")).await
        {
            output.stderr.extend(console);
        }
    }
    drop(qemu_pack);
    output
        .http_client_traces
        .clone_from(&traces.lock().expect("trace mutex poisoned"));
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        exit_code = output.exit_code,
        "Activity VM execution complete"
    );
    Ok(output)
}

fn directory_entries(path: &Path) -> Vec<String> {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}

async fn replace_output_from_guest_files(
    output: &mut VmOutput,
    queue: &Path,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
) -> anyhow::Result<bool> {
    let Some(stdout) = read_guest_output(queue.join("stdout"), max_stdout_bytes).await? else {
        return Ok(false);
    };
    let stderr = read_guest_output(queue.join("stderr"), max_stderr_bytes)
        .await?
        .unwrap_or_default();
    let mut diagnostics = stderr;
    diagnostics.append(&mut output.stderr);
    diagnostics.append(&mut output.stdout);
    output.stdout = stdout;
    output.stderr = diagnostics;
    if let Ok(exit_code) = tokio::fs::read_to_string(queue.join("exit-code")).await {
        output.exit_code = exit_code
            .trim()
            .parse()
            .context("parsing activity VM guest exit code")?;
    }
    Ok(true)
}

async fn read_guest_output(path: PathBuf, max_bytes: usize) -> anyhow::Result<Option<Vec<u8>>> {
    let file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let limit = u64::try_from(max_bytes)
        .expect("32 bit systems are unsupported")
        .saturating_add(1);
    let mut bytes = Vec::new();
    file.take(limit).read_to_end(&mut bytes).await?;
    Ok(Some(bytes))
}

fn qemu_runtime_info(
    mapdirs: &[MapDir],
    pack_mounts: &[&str],
    guest_args: &[String],
    env: &HashMap<String, String>,
) -> anyhow::Result<String> {
    let mut info = String::from("c:");
    for argument in guest_args {
        info.push(' ');
        info.push_str(&argument.replace('\\', "\\\\").replace(' ', "\\ "));
    }
    info.push('\n');
    for mapdir in mapdirs {
        ensure_runtime_info_line(&mapdir.guest)?;
        info.push_str(if mapdir.permissions == FsPerms::ReadOnly {
            "mr: "
        } else {
            "m: "
        });
        info.push_str(mapdir.guest.trim_start_matches('/'));
        info.push('\n');
    }
    for mount in pack_mounts {
        ensure_runtime_info_line(mount)?;
        info.push_str("p: ");
        info.push_str(mount.trim_start_matches('/'));
        info.push('\n');
    }
    for image in mapdirs
        .iter()
        .filter_map(|mapdir| mapdir.qemu_store_image.as_deref())
    {
        ensure_runtime_info_line(image)?;
        info.push_str("s: ");
        info.push_str(image);
        info.push('\n');
    }
    let mut env = env.iter().collect::<Vec<_>>();
    env.sort_unstable_by(|left, right| left.0.cmp(right.0));
    for (key, value) in env {
        ensure_runtime_info_line(key)?;
        info.push_str("env: ");
        info.push_str(key);
        info.push('=');
        info.push_str(&value.replace('\n', "\\\n"));
        info.push('\n');
    }
    Ok(info)
}

fn ensure_runtime_info_line(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.contains(['\r', '\n']),
        "activity VM runtime metadata contains a newline"
    );
    Ok(())
}

fn qemu_args() -> anyhow::Result<Vec<String>> {
    if let Some(arguments) = std::env::var_os("OBELISK_QEMU_ARGS_JSON") {
        return Ok(serde_json::from_str(&arguments.to_string_lossy())?);
    }
    Ok([
        "-display",
        "none",
        "-serial",
        "stdio",
        "-monitor",
        "none",
        "-incoming",
        "file:/image/vm.state",
        "-d",
        "cpu_reset,guest_errors",
        "-D",
        "/pack/qemu.log",
        "-nic",
        "none",
        "-m",
        "128M",
        "-cpu",
        "qemu64,+rdrand",
        "-device",
        "virtio-rng-pci",
        "-accel",
        "tcg,tb-size=500,thread=multi",
        "-smp",
        "1,sockets=1",
        "-L",
        "/image/",
        "-drive",
        "if=virtio,format=raw,file=/image/rootfs.bin",
        "-kernel",
        "/image/bzImage",
        "-append",
        "earlyprintk=ttyS0,115200n8 console=ttyS0,115200n8 slub_debug=F root=/dev/vda rootwait acpi=off ro virtio_net.napi_tx=false loglevel=7 QEMU_MODE=1 init=/sbin/tini -- /sbin/init",
        "-fsdev",
        "local,path=/,security_model=passthrough,id=wasi0",
        "-device",
        "virtio-9p-pci,fsdev=wasi0,mount_tag=wasi0,ioeventfd=off",
        "-fsdev",
        "local,path=/pack,security_model=none,id=wasi1",
        "-device",
        "virtio-9p-pci,fsdev=wasi1,mount_tag=wasi1,ioeventfd=off",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect())
}

async fn log_guest_phases(
    queue: PathBuf,
    started: Instant,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    const PHASES: &[(&str, &str)] = &[
        ("guest-launcher", "Linux reached the activity VM launcher"),
        ("store-mounted", "Nix store mapped"),
        ("network-ready", "Activity VM network bridge ready"),
        ("command-start", "Activity VM command starting"),
        ("store-mount-failed", "Nix store mapping failed"),
        ("network-failed", "Activity VM network bridge failed"),
    ];
    let mut observed = [false; PHASES.len()];
    loop {
        log_available_guest_phases(&queue, started, &mut observed, PHASES).await;
        if observed[..PHASES.len() - 1]
            .iter()
            .all(|observed| *observed)
        {
            return;
        }
        tokio::select! {
            _ = &mut stop => break,
            () = tokio::time::sleep(std::time::Duration::from_millis(2)) => {}
        }
    }
    log_available_guest_phases(&queue, started, &mut observed, PHASES).await;
}

async fn log_available_guest_phases(
    queue: &Path,
    started: Instant,
    observed: &mut [bool],
    phases: &[(&str, &str)],
) {
    for (index, (marker, message)) in phases.iter().enumerate() {
        if !observed[index]
            && tokio::fs::try_exists(queue.join(format!("phase-{marker}")))
                .await
                .unwrap_or(false)
        {
            observed[index] = true;
            let details = tokio::fs::read_to_string(queue.join(format!("phase-{marker}")))
                .await
                .unwrap_or_default();
            tracing::debug!(
                phase = *marker,
                elapsed_ms = started.elapsed().as_millis(),
                details = details.trim(),
                "{message}"
            );
        }
    }
}

#[expect(clippy::too_many_arguments)]
fn run_module(
    engine: &Engine,
    module: &Module,
    mapdirs: &[MapDir],
    guest_args: &[String],
    env: &HashMap<String, String>,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    cancelled: Arc<AtomicBool>,
) -> anyhow::Result<VmOutput> {
    let started = Instant::now();
    let stdout = pipe::MemoryOutputPipe::new(max_stdout_bytes.saturating_add(1));
    let stderr = pipe::MemoryOutputPipe::new(max_stderr_bytes);
    let mut wasi = WasiCtxBuilder::new();
    if qemu_memory_type(module)?.is_some() {
        wasi.stdin(ResumeInput::new());
    } else {
        wasi.stdin(pipe::ClosedInputStream);
    }
    wasi.stdout(stdout.clone())
        .stderr(stderr.clone())
        .arg("obelisk-activity-vm");
    for argument in guest_args {
        wasi.arg(argument);
    }
    for (key, value) in env {
        wasi.env(key, value);
    }
    for mapdir in mapdirs {
        wasi.preopened_dir(&mapdir.host, &mapdir.guest, mapdir.permissions)
            .map_err(|error| {
                anyhow::anyhow!(
                    "preopening {} as {}: {error:?}",
                    mapdir.host.display(),
                    mapdir.guest
                )
            })?;
    }
    let host_fs = host_fs::HostFs::new(mapdirs)?;
    let legacy_fds = legacy_fd::LegacyFdTable::new(mapdirs)?;
    if guest_args.iter().any(|argument| argument == "-incoming") {
        legacy_fds.seed_tty_input(QEMU_RESUME_INPUT);
    }
    let fiber_entries = Arc::new(Mutex::new(HashMap::new()));
    let mut store = Store::new(
        engine,
        VmState {
            wasi: wasi.build_p1(),
            qemu_jit: qemu_jit::QemuJit::new(None),
            host_fs: host_fs.clone(),
            legacy_fds: legacy_fds.clone(),
            fiber_next: None,
            fiber_entries: fiber_entries.clone(),
            poll_calls: 0,
            pthread_spawn: None,
        },
    );
    let mut linker: Linker<VmState> = Linker::new(engine);
    let qemu_memory_type = qemu_memory_type(module)?;
    if qemu_memory_type.is_none() {
        p1::add_to_linker_sync(&mut linker, |state: &mut VmState| &mut state.wasi)?;
    } else {
        let arguments = std::iter::once("obelisk-activity-vm".to_owned())
            .chain(guest_args.iter().cloned())
            .collect::<Arc<[_]>>();
        legacy_wasi::add_to_linker(&mut linker, stdout.clone(), stderr.clone(), arguments)?;
    }
    let mut emscripten_runtime = None;
    if let Some(memory_type) = qemu_memory_type {
        let memory = if memory_type.is_shared() {
            qemu_jit::QemuMemory::Shared(SharedMemory::new(engine, memory_type)?)
        } else {
            qemu_jit::QemuMemory::Plain(Memory::new(&mut store, memory_type)?)
        };
        store.data_mut().qemu_jit.memory = Some(memory.clone());
        linker.define(&mut store, "env", "memory", memory.as_extern())?;
        let table_import = qemu_table_import(module);
        if let Some((import_module, import_name, table_type)) = &table_import {
            let table = Table::new(&mut store, table_type.clone(), Ref::Func(None))?;
            linker.define(&mut store, import_module, import_name, table)?;
        }
        qemu_jit::add_to_linker(&mut linker)?;
        emscripten::add_invoke_wrappers(&mut linker, module)?;
        emscripten::add_longjmp(&mut linker)?;
        emscripten::add_ffi_call(&mut linker)?;
        emscripten::add_platform_services(&mut linker)?;
        emscripten::add_platform_shims(&mut linker, module)?;
        emscripten::add_main_thread_init(&mut linker)?;
        emscripten::add_fiber_swap(&mut linker)?;
        emscripten::add_mailbox_notify(&mut linker)?;
        emscripten::add_poll(&mut linker)?;
        host_fs::add_to_linker(&mut linker)?;
        if legacy_fd::is_required(module) {
            linker.allow_shadowing(true);
            legacy_fd::add_to_linker(&mut linker)?;
        }
        if let qemu_jit::QemuMemory::Shared(memory) = memory {
            let runtime = Arc::new(EmscriptenRuntime {
                engine: engine.clone(),
                module: module.clone(),
                memory,
                table_import,
                legacy_jit: module
                    .imports()
                    .any(|import| import.name() == "instantiate_wasm"),
                host_fs,
                legacy_fds,
                fiber_entries,
                mapdirs: mapdirs.to_vec(),
                arguments: std::iter::once("obelisk-activity-vm".to_owned())
                    .chain(guest_args.iter().cloned())
                    .collect(),
                stdout: stdout.clone(),
                stderr: stderr.clone(),
                threads: Mutex::new(Vec::new()),
                cancelled: cancelled.clone(),
            });
            let spawn = pthread_spawner(runtime.clone());
            store.data_mut().pthread_spawn = Some(spawn.clone());
            add_pthread_create(&mut linker, spawn)?;
            emscripten_runtime = Some(runtime);
        }
    }
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Activity VM linker prepared"
    );
    let epoch_cancelled = cancelled.clone();
    store.epoch_deadline_callback(move |_| {
        Ok(if epoch_cancelled.load(Ordering::Relaxed) {
            wasmtime::UpdateDeadline::Interrupt
        } else {
            wasmtime::UpdateDeadline::Continue(1)
        })
    });
    store.set_epoch_deadline(1);
    let instantiate_started = Instant::now();
    let instance = linker.instantiate(&mut store, module)?;
    tracing::debug!(
        elapsed_ms = instantiate_started.elapsed().as_millis(),
        total_elapsed_ms = started.elapsed().as_millis(),
        "Activity VM module instantiated"
    );
    let start = instance.get_typed_func::<(), ()>(&mut store, "_start").ok();
    if std::env::var_os("OBELISK_QEMU_PROBE_STDIN_POLL").is_some()
        && instance.get_func(&mut store, "__syscall_poll").is_some()
    {
        let malloc = instance.get_typed_func::<i32, i32>(&mut store, "malloc")?;
        let pollfd = malloc.call(&mut store, 8)?;
        let memory = instance
            .get_shared_memory(&mut store, "memory")
            .context("missing shared QEMU memory")?;
        for (cell, byte) in memory.data()[pollfd as usize..pollfd as usize + 8]
            .iter()
            .zip([0, 0, 0, 0, 1, 0, 0, 0])
        {
            // SAFETY: this allocation is private until __syscall_poll is called.
            unsafe { cell.get().write_volatile(byte) };
        }
        let ready = instance
            .get_typed_func::<(i32, i32, i32), i32>(&mut store, "__syscall_poll")?
            .call(&mut store, (pollfd, 1, 0))?;
        let mut result = [0_u8; 8];
        for (byte, cell) in result
            .iter_mut()
            .zip(&memory.data()[pollfd as usize..pollfd as usize + 8])
        {
            // SAFETY: __syscall_poll has finished writing this allocation.
            *byte = unsafe { cell.get().read_volatile() };
        }
        eprintln!("QEMU stdin poll probe ready={ready} pollfd={result:?}");
    }
    let call_started = Instant::now();
    if std::env::var_os("OBELISK_QEMU_TRACE_STDERR").is_some() {
        let stderr = stderr.clone();
        let stdout = stdout.clone();
        std::thread::spawn(move || {
            for _ in 0..60 {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let contents = stdout.contents();
                if !contents.is_empty() {
                    eprintln!("QEMU live stdout: {}", String::from_utf8_lossy(&contents));
                }
                let contents = stderr.contents();
                if !contents.is_empty() {
                    eprintln!("QEMU live stderr: {}", String::from_utf8_lossy(&contents));
                }
            }
        });
    }
    if std::env::var_os("OBELISK_QEMU_DEBUG_INTERRUPT").is_some() {
        let engine = engine.clone();
        let cancelled = cancelled.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(10));
            cancelled.store(true, Ordering::Relaxed);
            for _ in 0..30 {
                engine.increment_epoch();
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        });
    }
    let call_result = if let Some(start) = start {
        if instance
            .get_func(&mut store, "asyncify_get_state")
            .is_some()
        {
            emscripten::call_asyncify_root(&mut store, &instance, &start)
        } else {
            start.call(&mut store, ())
        }
    } else {
        call_emscripten_main(&mut store, &instance, guest_args)
            .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
    };
    eprintln!("QEMU root result: {call_result:?}");
    let mut serial = Vec::new();
    if let Some(runtime) = emscripten_runtime {
        if let Err(error) = join_pthreads(&runtime, mapdirs) {
            if is_shutdown_trap(&error) && guest_has_completed(&runtime, mapdirs) {
                eprintln!("QEMU pthread stopped after guest completion: {error:#}");
            } else {
                let console = runtime.legacy_fds.tty_output();
                return Err(error.context(format!(
                    "QEMU serial output:\n{}",
                    String::from_utf8_lossy(&console)
                )));
            }
        }
        serial = runtime.legacy_fds.tty_output();
    }
    let exit_code = match call_result {
        Ok(()) => 0,
        Err(error) => match error.downcast_ref::<wasmtime_wasi::I32Exit>() {
            Some(exit) => exit.0,
            None => bail!(
                "VM trapped: {error:?}\nguest stderr:\n{}",
                String::from_utf8_lossy(&stderr.contents())
            ),
        },
    };
    tracing::debug!(
        elapsed_ms = call_started.elapsed().as_millis(),
        total_elapsed_ms = started.elapsed().as_millis(),
        exit_code,
        "Activity VM module returned"
    );
    Ok(VmOutput {
        exit_code,
        stdout: stdout.contents().to_vec(),
        stderr: stderr.contents().to_vec(),
        serial,
        http_client_traces: Vec::new(),
    })
}

fn call_emscripten_main(
    store: &mut Store<VmState>,
    instance: &Instance,
    arguments: &[String],
) -> anyhow::Result<()> {
    instance
        .get_typed_func::<(), ()>(&mut *store, "__wasm_call_ctors")?
        .call(&mut *store, ())?;
    let malloc = instance.get_typed_func::<i32, i32>(&mut *store, "malloc")?;
    let memory = match store.data().qemu_jit.memory.as_ref() {
        Some(qemu_jit::QemuMemory::Shared(memory)) => memory.clone(),
        _ => bail!("missing Emscripten shared memory import"),
    };
    let mut argv = Vec::with_capacity(arguments.len() + 1);
    for argument in
        std::iter::once("obelisk-activity-vm").chain(arguments.iter().map(String::as_str))
    {
        let bytes = argument.as_bytes();
        let pointer = malloc.call(&mut *store, i32::try_from(bytes.len() + 1)?)?;
        write_shared_bytes(&memory, pointer, bytes)?;
        write_shared_bytes(&memory, pointer + i32::try_from(bytes.len())?, &[0])?;
        argv.push(pointer);
    }
    let argv_pointer = malloc.call(&mut *store, i32::try_from((argv.len() + 1) * 4)?)?;
    for (index, pointer) in argv.iter().chain(std::iter::once(&0)).enumerate() {
        write_shared_bytes(
            &memory,
            argv_pointer + i32::try_from(index * 4)?,
            &pointer.to_le_bytes(),
        )?;
    }
    let status = instance
        .get_typed_func::<(i32, i32), i32>(&mut *store, "_emscripten_proxy_main")?
        .call(&mut *store, (i32::try_from(argv.len())?, argv_pointer))?;
    anyhow::ensure!(status == 0, "Emscripten main returned {status}");
    Ok(())
}

fn write_shared_bytes(memory: &SharedMemory, pointer: i32, bytes: &[u8]) -> anyhow::Result<()> {
    let start = usize::try_from(pointer)?;
    let destination = memory
        .data()
        .get(start..start + bytes.len())
        .context("Emscripten memory write is out of bounds")?;
    for (destination, source) in destination.iter().zip(bytes) {
        // SAFETY: this initialization happens before the argument is published to the guest.
        unsafe { destination.get().write_volatile(*source) };
    }
    Ok(())
}

fn add_pthread_create(linker: &mut Linker<VmState>, spawn: PthreadSpawn) -> anyhow::Result<()> {
    emscripten::add_pthread_create(linker, move |pthread_ptr, attr, start_routine, arg| {
        spawn(pthread_ptr, attr, start_routine, arg)
    })
}

fn pthread_spawner(runtime: Arc<EmscriptenRuntime>) -> PthreadSpawn {
    Arc::new(move |pthread_ptr, _attr, start_routine, arg| {
        eprintln!("pthread create ptr={pthread_ptr:#x} start={start_routine:#x} arg={arg:#x}");
        if pthread_ptr <= 0 || start_routine < 0 {
            return Err(wasmtime::Error::msg("invalid Emscripten pthread request"));
        }
        let runtime = runtime.clone();
        let thread_runtime = runtime.clone();
        let handle = std::thread::Builder::new()
            .name(format!("activity-vm-pthread-{pthread_ptr:x}"))
            .spawn(move || {
                run_pthread(&thread_runtime, pthread_ptr, start_routine, arg).with_context(|| {
                    format!("Emscripten pthread {pthread_ptr:#x} start routine {start_routine:#x}")
                })
            })
            .map_err(|error| {
                wasmtime::Error::msg(format!("spawning Emscripten pthread: {error}"))
            })?;
        runtime
            .threads
            .lock()
            .expect("Emscripten thread mutex poisoned")
            .push(handle);
        Ok(0)
    })
}

fn guest_completion_exists(mapdirs: &[MapDir]) -> bool {
    mapdirs
        .iter()
        .find(|mapdir| mapdir.guest == "/obelisk-activity-vm-http")
        .is_some_and(|mapdir| {
            mapdir.host.join("stdout").is_file()
                && std::fs::metadata(mapdir.host.join("exit-code"))
                    .is_ok_and(|metadata| metadata.len() > 0)
        })
}

fn guest_has_completed(runtime: &EmscriptenRuntime, mapdirs: &[MapDir]) -> bool {
    guest_completion_exists(mapdirs)
        || runtime
            .legacy_fds
            .tty_output()
            .windows(b"activity-vm: command completed".len())
            .any(|window| window == b"activity-vm: command completed")
}

fn is_shutdown_trap(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<wasmtime::Trap>())
        .is_some_and(|trap| {
            matches!(
                trap,
                wasmtime::Trap::UnreachableCodeReached | wasmtime::Trap::Interrupt
            )
        })
}

fn join_pthreads(runtime: &EmscriptenRuntime, mapdirs: &[MapDir]) -> anyhow::Result<()> {
    let mut first_error = None;
    let mut stopping = false;
    loop {
        let handle = {
            let mut threads = runtime
                .threads
                .lock()
                .expect("Emscripten thread mutex poisoned");
            if threads.is_empty() {
                return match first_error {
                    Some(error)
                        if !(guest_has_completed(runtime, mapdirs) && is_shutdown_trap(&error)) =>
                    {
                        Err(error)
                    }
                    _ => Ok(()),
                };
            }
            threads
                .iter()
                .position(std::thread::JoinHandle::is_finished)
                .map(|index| threads.swap_remove(index))
        };
        if let Some(handle) = handle {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                    stopping = true;
                }
                Err(_) => {
                    first_error
                        .get_or_insert_with(|| anyhow::anyhow!("Emscripten pthread panicked"));
                    stopping = true;
                }
            }
        } else {
            stopping |= guest_has_completed(runtime, mapdirs);
            if stopping {
                runtime.cancelled.store(true, Ordering::Relaxed);
                runtime.engine.increment_epoch();
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

fn run_pthread(
    runtime: &Arc<EmscriptenRuntime>,
    pthread_ptr: i32,
    start_routine: i32,
    arg: i32,
) -> anyhow::Result<()> {
    eprintln!("pthread {pthread_ptr:#x}: instantiate start={start_routine:#x}");
    const PTHREAD_STACK_OFFSET: usize = 52;
    const PTHREAD_STACK_SIZE_OFFSET: usize = 56;

    let stack_high = read_shared_u32(&runtime.memory, pthread_ptr as usize + PTHREAD_STACK_OFFSET)?;
    let stack_size = read_shared_u32(
        &runtime.memory,
        pthread_ptr as usize + PTHREAD_STACK_SIZE_OFFSET,
    )?;
    let stack_low = stack_high
        .checked_sub(stack_size)
        .context("Emscripten pthread stack underflow")?;
    eprintln!(
        "pthread {pthread_ptr:#x}: stack {stack_low:#x}..{stack_high:#x} ({stack_size:#x} bytes)"
    );

    let mut wasi = WasiCtxBuilder::new();
    // QEMU's PROXY_TO_PTHREAD build executes main() on a worker instance.
    // The native snapshot builder leaves the guest blocked on this serial
    // byte; make it available to that worker as well as the bootstrap store.
    wasi.stdin(ResumeInput::new())
        .stdout(runtime.stdout.clone())
        .stderr(runtime.stderr.clone());
    for mapdir in &runtime.mapdirs {
        wasi.preopened_dir(&mapdir.host, &mapdir.guest, mapdir.permissions)
            .map_err(|error| {
                anyhow::anyhow!(
                    "preopening {} as {} for pthread: {error:?}",
                    mapdir.host.display(),
                    mapdir.guest
                )
            })?;
    }
    let wasi = wasi.build_p1();
    let memory = qemu_jit::QemuMemory::Shared(runtime.memory.clone());
    let mut store = Store::new(
        &runtime.engine,
        VmState {
            wasi,
            qemu_jit: qemu_jit::QemuJit::new(Some(memory.clone())),
            host_fs: runtime.host_fs.clone(),
            legacy_fds: runtime.legacy_fds.clone(),
            fiber_next: None,
            fiber_entries: runtime.fiber_entries.clone(),
            poll_calls: 0,
            pthread_spawn: Some(pthread_spawner(runtime.clone())),
        },
    );
    let mut linker = Linker::new(&runtime.engine);
    if legacy_fd::is_required(&runtime.module) {
        legacy_wasi::add_to_linker(
            &mut linker,
            runtime.stdout.clone(),
            runtime.stderr.clone(),
            runtime.arguments.clone(),
        )?;
    } else {
        p1::add_to_linker_sync(&mut linker, |state: &mut VmState| &mut state.wasi)?;
    }
    linker.define(&mut store, "env", "memory", memory.as_extern())?;
    if let Some((import_module, import_name, table_type)) = &runtime.table_import {
        let table = Table::new(&mut store, table_type.clone(), Ref::Func(None))?;
        linker.define(&mut store, import_module, import_name, table)?;
    }
    qemu_jit::add_to_linker(&mut linker)?;
    emscripten::add_invoke_wrappers(&mut linker, &runtime.module)?;
    emscripten::add_longjmp(&mut linker)?;
    emscripten::add_ffi_call(&mut linker)?;
    emscripten::add_platform_services(&mut linker)?;
    emscripten::add_platform_shims(&mut linker, &runtime.module)?;
    emscripten::add_main_thread_init(&mut linker)?;
    emscripten::add_fiber_swap(&mut linker)?;
    emscripten::add_mailbox_notify(&mut linker)?;
    emscripten::add_poll(&mut linker)?;
    host_fs::add_to_linker(&mut linker)?;
    if legacy_fd::is_required(&runtime.module) {
        linker.allow_shadowing(true);
        legacy_fd::add_to_linker(&mut linker)?;
    }
    add_pthread_create(
        &mut linker,
        store
            .data()
            .pthread_spawn
            .clone()
            .expect("pthread spawner is set"),
    )?;
    let cancelled = runtime.cancelled.clone();
    store.epoch_deadline_callback(move |_| {
        Ok(if cancelled.load(Ordering::Relaxed) {
            wasmtime::UpdateDeadline::Interrupt
        } else {
            wasmtime::UpdateDeadline::Continue(1)
        })
    });
    store.set_epoch_deadline(1);
    let instance = linker.instantiate(&mut store, &runtime.module)?;
    eprintln!("pthread {pthread_ptr:#x}: instantiated");

    instance
        .get_typed_func::<(i32, i32), ()>(&mut store, "emscripten_stack_set_limits")?
        .call(&mut store, (stack_high as i32, stack_low as i32))?;
    instance
        .get_typed_func::<i32, ()>(&mut store, "stackRestore")
        .or_else(|_| instance.get_typed_func::<i32, ()>(&mut store, "_emscripten_stack_restore"))?
        .call(&mut store, stack_high as i32)?;
    instance
        .get_typed_func::<(i32, i32, i32, i32, i32, i32), ()>(
            &mut store,
            "_emscripten_thread_init",
        )?
        .call(&mut store, (pthread_ptr, 0, 0, 1, 0, 0))?;
    let tls_base = instance
        .get_typed_func::<(), i32>(&mut store, "_emscripten_tls_init")?
        .call(&mut store, ())?;
    let tls_initdone = read_shared_u32(&runtime.memory, tls_base as usize + 4)?;
    // Mirrors Emscripten's worker bootstrap. Senders use this flag to choose
    // the asynchronous mailbox notification path for a live pthread.
    write_shared_u32(&runtime.memory, pthread_ptr as usize + 128, 1)?;
    eprintln!(
        "pthread {pthread_ptr:#x}: initialized tls_base={tls_base:#x} tls[4..8]={tls_initdone:#x}"
    );
    if runtime.legacy_jit {
        // Old artifacts do not initialize their fixed wasm32 TCG context as
        // part of the pthread entry point, so reproduce that setup here. New
        // batched-JIT artifacts own initialization inside their entry point;
        // calling the exported init_wasm32 before it deadlocks QEMU while the
        // main thread waits for vCPU startup.
        let malloc = instance.get_typed_func::<i32, i32>(&mut store, "malloc")?;
        let stack = malloc.call(&mut store, 640)?;
        let stack128 = malloc.call(&mut store, 640)?;
        write_shared_u32(&runtime.memory, tls_base as usize + 4, 1 << 16)?;
        write_shared_u32(&runtime.memory, tls_base as usize + 8, 0)?;
        write_shared_u32(&runtime.memory, tls_base as usize + 12, 8)?;
        write_shared_u32(&runtime.memory, tls_base as usize + 16, 16)?;
        write_shared_u32(&runtime.memory, tls_base as usize + 20, 1)?;
        write_shared_u32(&runtime.memory, tls_base as usize + 52, stack as u32)?;
        write_shared_u32(
            &runtime.memory,
            tls_base as usize + 60,
            (tls_base + 80) as u32,
        )?;
        write_shared_u32(&runtime.memory, tls_base as usize + 72, stack128 as u32)?;
        store.data_mut().qemu_jit.initialize_legacy(
            tls_base + 56,
            tls_base + 96,
            tls_base + 200_096,
        );
    }

    let table = instance
        .get_export(&mut store, "__indirect_function_table")
        .and_then(Extern::into_table)
        .context("Emscripten pthread function table is unavailable")?;
    eprintln!(
        "pthread {pthread_ptr:#x}: table size={} entry6761={}",
        table.size(&store),
        matches!(table.get(&mut store, 6761), Some(Ref::Func(Some(_))))
    );
    anyhow::ensure!(
        matches!(
            table.get(&mut store, start_routine as u64),
            Some(Ref::Func(Some(_)))
        ),
        "Emscripten pthread entry {start_routine} is not a function"
    );
    if std::env::var_os("OBELISK_QEMU_PROBE_STDIN_POLL").is_some() {
        let malloc = instance.get_typed_func::<i32, i32>(&mut store, "malloc")?;
        let pollfd = malloc.call(&mut store, 8)?;
        for (cell, byte) in runtime.memory.data()[pollfd as usize..pollfd as usize + 8]
            .iter()
            .zip([0, 0, 0, 0, 1, 0, 0, 0])
        {
            // SAFETY: this allocation is private until __syscall_poll is called.
            unsafe { cell.get().write_volatile(byte) };
        }
        let ready = instance
            .get_typed_func::<(i32, i32, i32), i32>(&mut store, "__syscall_poll")?
            .call(&mut store, (pollfd, 1, 0))?;
        let mut result = [0_u8; 8];
        for (byte, cell) in result
            .iter_mut()
            .zip(&runtime.memory.data()[pollfd as usize..pollfd as usize + 8])
        {
            // SAFETY: __syscall_poll has finished writing this allocation.
            *byte = unsafe { cell.get().read_volatile() };
        }
        eprintln!("pthread {pthread_ptr:#x}: stdin poll probe ready={ready} pollfd={result:?}");
    }
    eprintln!("pthread {pthread_ptr:#x}: entering start routine");
    let function = match table.get(&mut store, start_routine as u64) {
        Some(Ref::Func(Some(function))) => function,
        _ => unreachable!("pthread entry was validated above"),
    };
    let result = if instance
        .get_func(&mut store, "asyncify_get_state")
        .is_some()
    {
        emscripten::call_asyncify_pthread(&mut store, &instance, &function, arg)?
    } else {
        function.typed::<i32, i32>(&store)?.call(&mut store, arg)?
    };
    eprintln!("pthread {pthread_ptr:#x}: start routine returned {result}");
    let diagnostics = runtime.stderr.contents();
    if !diagnostics.is_empty() {
        eprintln!(
            "pthread {pthread_ptr:#x}: QEMU stderr at return: {}",
            String::from_utf8_lossy(&diagnostics)
        );
    }
    // Emscripten's Node worker keeps the runtime alive after the entry point
    // returns when noExitRuntime is set. QEMU relies on that worker message
    // loop, so do not run `_emscripten_thread_exit` here.
    Ok(())
}

fn read_shared_u32(memory: &SharedMemory, offset: usize) -> anyhow::Result<u32> {
    let bytes = memory
        .data()
        .get(offset..offset + 4)
        .context("Emscripten pthread metadata is out of bounds")?;
    Ok(u32::from_le_bytes([
        read_shared_byte(&bytes[0]),
        read_shared_byte(&bytes[1]),
        read_shared_byte(&bytes[2]),
        read_shared_byte(&bytes[3]),
    ]))
}

fn read_shared_byte(byte: &UnsafeCell<u8>) -> u8 {
    // SAFETY: pthread_create publishes this metadata before invoking the host import.
    unsafe { byte.get().read() }
}

fn write_shared_u32(memory: &SharedMemory, offset: usize, value: u32) -> anyhow::Result<()> {
    let bytes = memory
        .data()
        .get(offset..offset + 4)
        .context("Emscripten pthread metadata is out of bounds")?;
    for (destination, source) in bytes.iter().zip(value.to_le_bytes()) {
        // SAFETY: worker initialization owns its TLS fields before entry.
        unsafe { destination.get().write_volatile(source) };
    }
    Ok(())
}

fn qemu_memory_type(module: &Module) -> anyhow::Result<Option<MemoryType>> {
    let memory_type =
        module.imports().find_map(
            |import| match (import.module(), import.name(), import.ty()) {
                ("env", "memory", ExternType::Memory(memory_type)) => Some(memory_type),
                _ => None,
            },
        );
    Ok(memory_type)
}

fn qemu_table_import(module: &Module) -> Option<(String, String, TableType)> {
    module.imports().find_map(|import| match import.ty() {
        ExternType::Table(table_type) => Some((
            import.module().to_owned(),
            import.name().to_owned(),
            table_type,
        )),
        _ => None,
    })
}

pub fn compile(engine: &Engine, module_path: &Path) -> anyhow::Result<Module> {
    Module::from_file(engine, module_path)
        .map_err(|error| anyhow::anyhow!("loading VM runtime {}: {error:?}", module_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::Config;

    #[test]
    fn runs_a_preview1_module() {
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            wat::parse_str("(module (func (export \"_start\")))").unwrap(),
        )
        .unwrap();
        let output = run_module(
            &engine,
            &module,
            &[],
            &[],
            &HashMap::new(),
            1024,
            1024,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert_eq!(output.exit_code, 0);
    }

    #[test]
    fn legacy_wasi_exposes_runtime_arguments_clock_and_randomness() {
        let mut config = Config::new();
        config.shared_memory(true);
        let engine = Engine::new(&config).unwrap();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "memory" (memory 1 1 shared))
                (import "wasi_snapshot_preview1" "args_sizes_get"
                    (func $sizes (param i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "args_get"
                    (func $get (param i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "clock_time_get"
                    (func $clock (param i32 i64 i32) (result i32)))
                (import "wasi_snapshot_preview1" "random_get"
                    (func $random (param i32 i32) (result i32)))
                (func (export "run")
                    i32.const 0 i32.const 4 call $sizes drop
                    i32.const 8 i32.const 32 call $get drop
                    i32.const 0 i64.const 1 i32.const 52 call $clock drop
                    i32.const 64 i32.const 32 call $random drop))"#,
        )
        .unwrap();
        let memory = SharedMemory::new(&engine, MemoryType::shared(1, 1)).unwrap();
        let qemu_memory = qemu_jit::QemuMemory::Shared(memory.clone());
        let mut store = Store::new(
            &engine,
            VmState {
                wasi: WasiCtxBuilder::new().build_p1(),
                qemu_jit: qemu_jit::QemuJit::new(Some(qemu_memory.clone())),
                host_fs: host_fs::HostFs::new(&[]).unwrap(),
                legacy_fds: legacy_fd::LegacyFdTable::new(&[]).unwrap(),
                fiber_next: None,
                fiber_entries: Arc::new(Mutex::new(HashMap::new())),
                poll_calls: 0,
                pthread_spawn: None,
            },
        );
        let mut linker = Linker::new(&engine);
        linker
            .define(&mut store, "env", "memory", qemu_memory.as_extern())
            .unwrap();
        legacy_wasi::add_to_linker(
            &mut linker,
            pipe::MemoryOutputPipe::new(128),
            pipe::MemoryOutputPipe::new(128),
            Arc::from(["qemu".to_owned(), "--flag".to_owned(), "value".to_owned()]),
        )
        .unwrap();
        let instance = linker.instantiate(&mut store, &module).unwrap();
        instance
            .get_typed_func::<(), ()>(&mut store, "run")
            .unwrap()
            .call(&mut store, ())
            .unwrap();

        assert_eq!(read_shared_u32(&memory, 0).unwrap(), 3);
        assert_eq!(read_shared_u32(&memory, 4).unwrap(), 18);
        assert_eq!(read_shared_u32(&memory, 8).unwrap(), 32);
        assert_eq!(read_shared_u32(&memory, 12).unwrap(), 37);
        assert_eq!(read_shared_u32(&memory, 16).unwrap(), 44);
        let bytes = memory.data()[32..50]
            .iter()
            // SAFETY: the Wasm call has returned, so no guest thread accesses this memory.
            .map(|cell| unsafe { cell.get().read() })
            .collect::<Vec<_>>();
        assert_eq!(bytes, b"qemu\0--flag\0value\0");
        assert!(read_shared_u64(&memory, 52) > 0);
        assert!(
            read_shared_bytes(&memory, 64, 32)
                .iter()
                .any(|byte| *byte != 0)
        );
    }

    #[test]
    fn runs_an_emscripten_pthread_in_a_shared_instance() {
        let mut config = Config::new();
        config.shared_memory(true);
        let engine = Engine::new(&config).unwrap();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "memory" (memory 1 1 shared))
                (table (export "__indirect_function_table") 1 funcref)
                (func $thread (param $arg i32) (result i32)
                    i32.const 0 local.get $arg i32.store
                    local.get $arg i32.const 1 i32.add)
                (elem (i32.const 0) $thread)
                (func (export "emscripten_stack_set_limits") (param i32 i32))
                (func (export "_emscripten_stack_restore") (param i32))
                (func (export "_emscripten_thread_init")
                    (param i32 i32 i32 i32 i32 i32))
                (func (export "_emscripten_tls_init") (result i32) i32.const 0)
                ;; The host must not invoke this before the pthread entry.
                (func (export "init_wasm32") unreachable)
                (func (export "_emscripten_thread_exit") (param $result i32)
                    i32.const 4 local.get $result i32.store))"#,
        )
        .unwrap();
        let memory = SharedMemory::new(&engine, MemoryType::shared(1, 1)).unwrap();
        let pthread_ptr = 100_u32;
        write_shared_u32(&memory, pthread_ptr as usize + 52, 65_536);
        write_shared_u32(&memory, pthread_ptr as usize + 56, 4_096);
        let runtime = Arc::new(EmscriptenRuntime {
            engine,
            module,
            memory: memory.clone(),
            table_import: None,
            legacy_jit: false,
            host_fs: host_fs::HostFs::new(&[]).unwrap(),
            legacy_fds: legacy_fd::LegacyFdTable::new(&[]).unwrap(),
            fiber_entries: Arc::new(Mutex::new(HashMap::new())),
            mapdirs: Vec::new(),
            arguments: Arc::from([]),
            stdout: pipe::MemoryOutputPipe::new(1024),
            stderr: pipe::MemoryOutputPipe::new(1024),
            threads: Mutex::new(Vec::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
        });
        run_pthread(&runtime, pthread_ptr as i32, 0, 41).unwrap();
        assert_eq!(read_shared_u32(&memory, 0).unwrap(), 41);
        // noExitRuntime keeps the worker alive instead of calling
        // `_emscripten_thread_exit` after its entry point returns.
        assert_eq!(read_shared_u32(&memory, 4).unwrap(), 0);
    }

    #[test]
    fn switches_an_asyncify_fiber() {
        let mut config = Config::new();
        config.shared_memory(true);
        let engine = Engine::new(&config).unwrap();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "memory" (memory 1 1 shared))
                (import "env" "emscripten_fiber_swap" (func $swap (param i32 i32)))
                (export "memory" (memory 0))
                (global $state (mut i32) (i32.const 0))
                (data (i32.const 100) "\00\10\00\00\00\08\00\00\00\0c\00\00\00\00\00\00")
                (func (export "asyncify_get_state") (result i32) global.get $state)
                (func (export "asyncify_start_unwind") (param i32)
                    i32.const 1 global.set $state)
                (func (export "asyncify_stop_unwind")
                    i32.const 0 global.set $state)
                (func (export "asyncify_start_rewind") (param i32)
                    i32.const 2 global.set $state)
                (func (export "asyncify_stop_rewind")
                    i32.const 0 global.set $state)
                (func (export "emscripten_stack_get_current") (result i32)
                    i32.const 3072)
                (func (export "emscripten_stack_set_limits") (param i32 i32))
                (func (export "_emscripten_stack_restore") (param i32))
                (func (export "_start")
                    global.get $state i32.const 2 i32.eq
                    if
                        i32.const 64 i32.const 100 call $swap
                        i32.const 0 i32.const 42 i32.store
                    else
                        i32.const 64 i32.const 100 call $swap
                    end))"#,
        )
        .unwrap();
        let memory = SharedMemory::new(&engine, MemoryType::shared(1, 1)).unwrap();
        let mut store = Store::new(
            &engine,
            VmState {
                wasi: WasiCtxBuilder::new().build_p1(),
                qemu_jit: qemu_jit::QemuJit::new(None),
                host_fs: host_fs::HostFs::new(&[]).unwrap(),
                legacy_fds: legacy_fd::LegacyFdTable::new(&[]).unwrap(),
                fiber_next: None,
                fiber_entries: Arc::new(Mutex::new(HashMap::new())),
                poll_calls: 0,
                pthread_spawn: None,
            },
        );
        let mut linker = Linker::new(&engine);
        linker
            .define(&mut store, "env", "memory", memory.clone())
            .unwrap();
        emscripten::add_fiber_swap(&mut linker).unwrap();
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let start = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .unwrap();
        emscripten::call_asyncify_root(&mut store, &instance, &start).unwrap();
        assert_eq!(read_shared_u32(&memory, 0).unwrap(), 42);
        assert_eq!(read_shared_u32(&memory, 64 + 8).unwrap(), 3072);
    }

    #[test]
    fn writes_qemu_runtime_info_for_dynamic_9p_mounts() {
        let mapdirs = [
            MapDir::read_only(PathBuf::from("/host/store"), "/nix/store/abc".to_owned()),
            MapDir::read_write(PathBuf::from("/host/queue"), "/queue".to_owned()),
        ];
        let args = ["/bin/echo".to_owned(), "hello world".to_owned()];
        let env = HashMap::from([
            ("SECOND".to_owned(), "two".to_owned()),
            ("FIRST".to_owned(), "one".to_owned()),
        ]);
        assert_eq!(
            qemu_runtime_info(&mapdirs, &["runtime/queue"], &args, &env).unwrap(),
            "c: /bin/echo hello\\ world\nmr: nix/store/abc\nm: queue\np: runtime/queue\nenv: FIRST=one\nenv: SECOND=two\n"
        );
    }

    #[tokio::test]
    #[ignore = "requires a built QEMU activity VM runtime"]
    async fn qemu_running_snapshot_executes_literal_echo() {
        let runtime_dir = PathBuf::from(
            std::env::var_os("OBELISK_QEMU_RUNTIME_DIR")
                .expect("OBELISK_QEMU_RUNTIME_DIR must point at the QEMU runtime"),
        );
        let mut config = Config::new();
        config.shared_memory(true).epoch_interruption(true);
        let engine = Engine::new(&config).unwrap();
        let module = compile(&engine, &runtime_dir.join("qemu-system-x86_64.wasm")).unwrap();
        let args =
            serde_json::from_slice(&std::fs::read(runtime_dir.join("args.json")).unwrap()).unwrap();
        let output = execute(
            &engine,
            module,
            Vec::new(),
            Some(QemuRuntimeConfig {
                args,
                image_dir: runtime_dir.join("image"),
            }),
            vec!["/bin/echo".to_owned(), "Hello, world!".to_owned()],
            HashMap::new(),
            None,
            HttpRequestPolicy::default(),
            Arc::new(AtomicBool::new(false)),
            1024 * 1024,
            1024 * 1024,
        )
        .await
        .unwrap();

        assert_eq!(output.exit_code, 0);
        let serial = String::from_utf8_lossy(&output.serial);
        assert!(serial.contains("Hello, world!"), "serial output: {serial}");
    }

    #[test]
    fn writes_qemu_store_squashfs_after_read_only_mount() {
        let mapdir =
            MapDir::qemu_store_squashfs(PathBuf::from("/cache/activity/store.squashfs")).unwrap();

        assert_eq!(
            qemu_runtime_info(&[mapdir], &[], &[], &HashMap::new()).unwrap(),
            "c:\nmr: obelisk-activity-vm-store\ns: obelisk-activity-vm-store/store.squashfs\n"
        );
    }

    #[test]
    fn rejects_ambiguous_qemu_store_image_path() {
        assert!(MapDir::qemu_store_squashfs(PathBuf::from("store.squashfs")).is_err());
        assert!(MapDir::qemu_store_squashfs(PathBuf::from("/cache/store.img")).is_err());
    }

    fn write_shared_u32(memory: &SharedMemory, offset: usize, value: u32) {
        for (destination, source) in memory.data()[offset..offset + 4]
            .iter()
            .zip(value.to_le_bytes())
        {
            // SAFETY: the test does not run a Wasm instance concurrently.
            unsafe { destination.get().write(source) };
        }
    }

    fn read_shared_u64(memory: &SharedMemory, offset: usize) -> u64 {
        u64::from_le_bytes(read_shared_bytes(memory, offset, 8).try_into().unwrap())
    }

    fn read_shared_bytes(memory: &SharedMemory, offset: usize, length: usize) -> Vec<u8> {
        memory.data()[offset..offset + length]
            .iter()
            // SAFETY: tests do not access this memory concurrently.
            .map(|cell| unsafe { cell.get().read() })
            .collect()
    }
}
