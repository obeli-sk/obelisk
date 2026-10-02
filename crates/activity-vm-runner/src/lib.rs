use anyhow::{Context as _, bail};
use concepts::ContentDigest;
use concepts::storage::LogStreamType;
use concepts::storage::http_client_trace::HttpClientTrace;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeekExt as _};
use wasm_workers::http_request_policy::HttpRequestPolicy;
use wasm_workers::store_limits::StoreMemoryLimiter;
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::p1::WasiP1Ctx;
use wasmtime_wasi::{FsPerms, WasiCtxBuilder, p1, p2::pipe};

#[cfg(target_os = "linux")]
mod firecracker;
mod http_bridge;
mod native_qemu;
mod store_image;

#[cfg(target_os = "linux")]
pub use firecracker::validate as validate_firecracker;
pub use native_qemu::{validate_guest_cpus, validate_guest_memory};

#[cfg(not(target_os = "linux"))]
pub fn validate_firecracker(_bundle: &Path, _memory: u64, _cpus: u32) -> anyhow::Result<()> {
    bail!("Firecracker is only supported on Linux")
}

/// Guest RAM set by the Bochs runtime's `bochsrc`; Bochs cannot change it per activity.
pub const BOCHS_GUEST_MEMORY: u64 = 512 << 20;

pub struct VmOutput {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
}

/// Receives guest and emulator output while the VM runs.
pub type OutputSink = Arc<dyn Fn(LogStreamType, &[u8]) + Send + Sync>;

#[derive(Clone, Debug)]
pub enum RuntimeSource {
    BochsWasm(PathBuf),
    QemuNative {
        bundle: PathBuf,
        digest: ContentDigest,
    },
    Firecracker {
        bundle: PathBuf,
        digest: ContentDigest,
    },
}

impl RuntimeSource {
    #[must_use]
    pub fn path(&self) -> &Path {
        match self {
            Self::BochsWasm(path) => path,
            Self::QemuNative { bundle, .. } | Self::Firecracker { bundle, .. } => bundle,
        }
    }
}

pub enum RuntimeBackend {
    BochsWasm {
        engine: Arc<Engine>,
        module: Module,
    },
    QemuNative {
        bundle: PathBuf,
        /// Total guest RAM.
        guest_memory: u64,
        guest_cpus: u32,
    },
    Firecracker {
        bundle: PathBuf,
        /// Total guest RAM.
        guest_memory: u64,
        guest_cpus: u32,
    },
}

#[derive(Clone)]
pub struct MapDir {
    host: PathBuf,
    guest: String,
    permissions: FsPerms,
}

impl MapDir {
    #[must_use]
    pub fn read_only(host: PathBuf, guest: String) -> Self {
        Self {
            host,
            guest,
            permissions: FsPerms::ReadOnly,
        }
    }

    #[must_use]
    pub fn read_write(host: PathBuf, guest: String) -> Self {
        Self {
            host,
            guest,
            permissions: FsPerms::ReadWrite,
        }
    }
}

/// Sending `stop` stops and reaps the VM before the returned future completes.
#[expect(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn execute(
    backend: &RuntimeBackend,
    mut mapdirs: Vec<MapDir>,
    guest_args: Vec<String>,
    mut env: HashMap<String, String>,
    stdin: Option<Vec<u8>>,
    policy: HttpRequestPolicy,
    http_client_traces: Arc<Mutex<Vec<HttpClientTrace>>>,
    output_sink: OutputSink,
    max_stdout_bytes: usize,
    memory: Option<u64>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<VmOutput> {
    let (engine, module) = match backend {
        RuntimeBackend::QemuNative {
            bundle,
            guest_memory,
            guest_cpus,
        } => {
            return native_qemu::execute(
                bundle,
                *guest_memory,
                *guest_cpus,
                mapdirs,
                guest_args,
                env,
                stdin,
                policy,
                http_client_traces,
                output_sink,
                max_stdout_bytes,
                stop,
            )
            .await;
        }
        #[cfg(target_os = "linux")]
        RuntimeBackend::Firecracker {
            bundle,
            guest_memory,
            guest_cpus,
        } => {
            return firecracker::execute(
                bundle,
                *guest_memory,
                *guest_cpus,
                mapdirs,
                guest_args,
                env,
                stdin,
                policy,
                http_client_traces,
                output_sink,
                max_stdout_bytes,
                stop,
            )
            .await;
        }
        #[cfg(not(target_os = "linux"))]
        RuntimeBackend::Firecracker { .. } => bail!("Firecracker is only supported on Linux"),
        RuntimeBackend::BochsWasm { engine, module } => (engine, module),
    };
    let started = Instant::now();
    tracing::debug!("Preparing activity VM execution");
    let queue = tempfile::tempdir()?;
    tokio::fs::write(
        queue.path().join("http-guest.sh"),
        include_bytes!("../guest/http-guest.sh"),
    )
    .await?;
    if let Some(stdin) = stdin {
        tokio::fs::write(queue.path().join("stdin.json"), stdin).await?;
        env.insert(
            "OBELISK_ACTIVITY_VM_STDIN".to_owned(),
            "/obelisk-activity-vm-http/stdin.json".to_owned(),
        );
    }
    mapdirs.push(MapDir::read_write(
        queue.path().to_owned(),
        "/obelisk-activity-vm-http".to_owned(),
    ));
    let _broker = AbortOnDrop(tokio::spawn(http_bridge::serve(
        queue.path().to_owned(),
        policy,
        http_client_traces,
    )));
    let stdout = pipe::MemoryOutputPipe::new(max_stdout_bytes.saturating_add(1));
    let stderr = pipe::MemoryOutputPipe::new(16 * 1024 * 1024);
    let tail = OutputTail::spawn(
        queue.path().to_owned(),
        vec![stdout.clone(), stderr.clone()],
        output_sink,
    );
    let execution = run_until_activity_completes(
        engine,
        module,
        &mapdirs,
        &guest_args,
        &env,
        stdout.clone(),
        stderr.clone(),
        memory,
        queue.path(),
        started,
    );
    let result = tokio::select! {
        result = execution => result,
        _ = &mut stop => Ok(0),
    };
    tail.finish().await;
    let exit_code = result?;
    let output = guest_result(
        queue.path(),
        max_stdout_bytes,
        VmOutput {
            exit_code,
            stdout: stdout.contents().to_vec(),
        },
    )
    .await?;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        exit_code = output.exit_code,
        "Activity VM execution complete"
    );
    Ok(output)
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Bounds the emulator console output kept in memory for error messages.
const CONSOLE_CAPTURE_BYTES: usize = 64 * 1024;
const OUTPUT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Forwards output as the guest writes it, so it survives the VM being dropped mid-run.
struct OutputTail {
    stop: tokio::sync::oneshot::Sender<()>,
    task: AbortOnDrop<()>,
}

impl OutputTail {
    /// Tails the guest's `stdout` and `stderr` files and the in-memory console pipes.
    fn spawn(queue: PathBuf, consoles: Vec<pipe::MemoryOutputPipe>, sink: OutputSink) -> Self {
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut files = [
                (queue.join("stdout"), LogStreamType::StdOut, 0),
                (queue.join("stderr"), LogStreamType::StdErr, 0),
            ];
            let mut consoles = consoles
                .into_iter()
                .map(|pipe| (pipe, 0))
                .collect::<Vec<_>>();
            loop {
                let stopping = tokio::select! {
                    () = tokio::time::sleep(OUTPUT_POLL_INTERVAL) => false,
                    _ = &mut stopped => true,
                };
                for (path, stream, offset) in &mut files {
                    if let Err(error) = forward_appended(path, *stream, offset, &sink).await {
                        tracing::debug!(path = %path.display(), "Cannot tail activity VM output: {error}");
                    }
                }
                for (console, offset) in &mut consoles {
                    let contents = console.contents();
                    if let Some(appended) = contents.get(*offset..)
                        && !appended.is_empty()
                    {
                        sink(LogStreamType::StdErr, appended);
                        *offset = contents.len();
                    }
                }
                if stopping {
                    return;
                }
            }
        });
        Self {
            stop,
            task: AbortOnDrop(task),
        }
    }

    /// Forwards everything written so far.
    async fn finish(self) {
        let _ = self.stop.send(());
        let mut task = self.task;
        let _ = (&mut task.0).await;
    }
}

/// Reopens by path because Firecracker replaces the file instead of appending to it.
async fn forward_appended(
    path: &Path,
    stream: LogStreamType,
    offset: &mut u64,
    sink: &OutputSink,
) -> std::io::Result<()> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    file.seek(std::io::SeekFrom::Start(*offset)).await?;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        sink(stream, &buffer[..count]);
        *offset += u64::try_from(count).expect("buffer length fits in u64");
    }
}

/// Forwards emulator output as diagnostics, keeping a bounded copy for error messages.
fn spawn_console_reader(
    mut pipe: impl AsyncRead + Unpin + Send + 'static,
    sink: OutputSink,
) -> tokio::task::JoinHandle<Vec<u8>> {
    tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        let mut captured = Vec::new();
        while let Ok(count) = pipe.read(&mut buffer).await
            && count > 0
        {
            let chunk = &buffer[..count];
            tracing::debug!(console = %String::from_utf8_lossy(chunk), "Activity VM console output");
            sink(LogStreamType::StdErr, chunk);
            let room = CONSOLE_CAPTURE_BYTES.saturating_sub(captured.len());
            captured.extend_from_slice(&chunk[..count.min(room)]);
        }
        captured
    })
}

/// Stops the VM once the guest marks the activity complete, skipping the emulated shutdown.
#[expect(clippy::too_many_arguments)]
async fn run_until_activity_completes(
    engine: &Engine,
    module: &Module,
    mapdirs: &[MapDir],
    guest_args: &[String],
    env: &HashMap<String, String>,
    stdout: pipe::MemoryOutputPipe,
    stderr: pipe::MemoryOutputPipe,
    memory: Option<u64>,
    queue: &Path,
    started: Instant,
) -> anyhow::Result<i32> {
    let mut phase_logger = AbortOnDrop(tokio::spawn(log_guest_phases(queue.to_owned(), started)));
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Starting activity VM module"
    );
    tokio::select! {
        exit_code = run_module(engine, module, mapdirs, guest_args, env, stdout, stderr, memory) => {
            let exit_code = exit_code?;
            tracing::debug!(
                elapsed_ms = started.elapsed().as_millis(),
                runtime_exit_code = exit_code,
                "Activity VM module returned"
            );
            Ok(exit_code)
        }
        Ok(activity_completed_at) = &mut phase_logger.0 => {
            tracing::debug!(
                activity_completed_ms = activity_completed_at.as_millis(),
                vm_termination_ms = started.elapsed()
                    .saturating_sub(activity_completed_at)
                    .as_millis(),
                "Activity VM stopped after activity completion"
            );
            // The guest writes its exit code to the queue, overriding this one.
            Ok(0)
        }
    }
}

const PHASES: &[(&str, &str)] = &[
    ("guest-launcher", "Linux reached the activity VM launcher"),
    ("store-mounted", "Nix store mapped"),
    ("network-ready", "Activity VM network bridge ready"),
    ("command-start", "Activity VM command starting"),
    ("store-mount-failed", "Nix store mapping failed"),
    ("network-failed", "Activity VM network bridge failed"),
    ("activity-complete", "Activity VM command completed"),
];

/// Returns once the `activity-complete` phase, which must be last in `PHASES`, is observed.
async fn log_guest_phases(queue: PathBuf, started: Instant) -> Duration {
    let mut observed = [false; PHASES.len()];
    loop {
        log_available_guest_phases(&queue, started, &mut observed).await;
        if observed[PHASES.len() - 1] {
            return started.elapsed();
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn log_available_guest_phases(queue: &Path, started: Instant, observed: &mut [bool]) {
    for (index, (marker, message)) in PHASES.iter().enumerate() {
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

/// The emulator's linear memory holds the guest's emulated RAM, so `activities.vm_bochs.memory`
/// is enforced on this store like any other wasm slot.
struct VmStore {
    wasi: WasiP1Ctx,
    memory_limiter: StoreMemoryLimiter,
}

#[expect(clippy::too_many_arguments)]
async fn run_module(
    engine: &Engine,
    module: &Module,
    mapdirs: &[MapDir],
    guest_args: &[String],
    env: &HashMap<String, String>,
    stdout: pipe::MemoryOutputPipe,
    stderr: pipe::MemoryOutputPipe,
    memory: Option<u64>,
) -> anyhow::Result<i32> {
    let started = Instant::now();
    let mut linker = Linker::new(engine);
    p1::add_to_linker_async(&mut linker, |data: &mut VmStore| &mut data.wasi)?;
    let pre = linker.instantiate_pre(module)?;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Activity VM linker prepared"
    );
    let mut wasi = WasiCtxBuilder::new();
    wasi.stdin(pipe::ClosedInputStream)
        .stdout(stdout)
        .stderr(stderr)
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
    let mut store = Store::new(
        engine,
        VmStore {
            wasi: wasi.build_p1(),
            memory_limiter: StoreMemoryLimiter::new(memory),
        },
    );
    store.limiter(|data| &mut data.memory_limiter);
    // Same as regular activities: yield to tokio on every epoch tick so the caller can drop us.
    store.epoch_deadline_callback(|_| {
        Ok(wasmtime::UpdateDeadline::YieldCustom(
            1,
            Box::pin(tokio::task::yield_now()),
        ))
    });
    store.set_epoch_deadline(1);
    let instance = pre.instantiate_async(&mut store).await?;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Activity VM module instantiated"
    );
    let start = instance.get_typed_func::<(), ()>(&mut store, "_start")?;
    match start.call_async(&mut store, ()).await {
        Ok(()) => Ok(0),
        Err(error) => match error.downcast_ref::<wasmtime_wasi::I32Exit>() {
            Some(exit) => Ok(exit.0),
            None => bail!("VM trapped: {error:?}"),
        },
    }
}

/// Prefers the activity's own stdout and exit code over the console's.
async fn guest_result(
    queue: &Path,
    max_stdout_bytes: usize,
    console: VmOutput,
) -> anyhow::Result<VmOutput> {
    let Some(stdout) = read_guest_output(queue.join("stdout"), max_stdout_bytes).await? else {
        return Ok(console);
    };
    let exit_code = match tokio::fs::read_to_string(queue.join("exit-code")).await {
        Ok(exit_code) => exit_code
            .trim()
            .parse()
            .context("parsing activity VM guest exit code")?,
        Err(_) => console.exit_code,
    };
    Ok(VmOutput { exit_code, stdout })
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

pub fn compile(engine: &Engine, module_path: &Path) -> anyhow::Result<Module> {
    Module::from_file(engine, module_path)
        .map_err(|error| anyhow::anyhow!("loading VM runtime {}: {error:?}", module_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch_engine() -> Engine {
        let mut config = wasmtime::Config::new();
        config.epoch_interruption(true);
        Engine::new(&config).unwrap()
    }

    fn module(engine: &Engine, wat: &str) -> Module {
        Module::new(engine, wat::parse_str(wat).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn runs_a_preview1_module() {
        let engine = epoch_engine();
        let module = module(&engine, "(module (func (export \"_start\")))");
        let exit_code = run_module(
            &engine,
            &module,
            &[],
            &[],
            &HashMap::new(),
            pipe::MemoryOutputPipe::new(1024),
            pipe::MemoryOutputPipe::new(1024),
            None,
        )
        .await
        .unwrap();
        assert_eq!(exit_code, 0);
    }

    #[tokio::test]
    async fn activity_completion_interrupts_the_vm() {
        let engine = epoch_engine();
        let _epoch_ticker = wasm_workers::epoch_ticker::EpochTicker::spawn_new(
            vec![engine.weak()],
            Duration::from_millis(1),
        );
        let module = module(
            &engine,
            r#"(module
            (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
            (memory (export "memory") 1)
            (data (i32.const 8) "started")
            (func (export "_start")
                (i32.store (i32.const 0) (i32.const 8))
                (i32.store (i32.const 4) (i32.const 7))
                (drop (call $write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 16)))
                (loop br 0)))"#,
        );
        let queue = tempfile::tempdir().unwrap();
        let marker = queue.path().join("phase-activity-complete");
        let stdout = pipe::MemoryOutputPipe::new(1024);
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let tail = OutputTail::spawn(
            queue.path().to_owned(),
            vec![stdout.clone()],
            Arc::new(move |_, bytes| started_tx.send(bytes.to_vec()).unwrap()),
        );
        let signal = tokio::spawn(async move {
            assert_eq!(started_rx.recv().await.unwrap(), b"started");
            tokio::fs::write(marker, b"0").await.unwrap();
        });

        let exit_code = tokio::time::timeout(
            Duration::from_secs(10),
            run_until_activity_completes(
                &engine,
                &module,
                &[],
                &[],
                &HashMap::new(),
                stdout,
                pipe::MemoryOutputPipe::new(1024),
                None,
                queue.path(),
                Instant::now(),
            ),
        )
        .await
        .expect("VM must stop after activity completion")
        .unwrap();
        signal.await.unwrap();
        tail.finish().await;
        assert_eq!(exit_code, 0);
    }

    #[tokio::test]
    async fn guest_files_override_console_output_and_exit_code() {
        let queue = tempfile::tempdir().unwrap();
        tokio::fs::write(queue.path().join("stdout"), b"\"result\"\n")
            .await
            .unwrap();
        tokio::fs::write(queue.path().join("exit-code"), b"7\n")
            .await
            .unwrap();
        let console = VmOutput {
            exit_code: 0,
            stdout: b"serial console\n".to_vec(),
        };

        let output = guest_result(queue.path(), 1024, console).await.unwrap();

        assert_eq!(output.exit_code, 7);
        assert_eq!(output.stdout, b"\"result\"\n");
    }

    #[tokio::test]
    async fn absent_guest_files_preserve_console_output() {
        let queue = tempfile::tempdir().unwrap();
        let console = VmOutput {
            exit_code: 3,
            stdout: b"stdout".to_vec(),
        };

        let output = guest_result(queue.path(), 1024, console).await.unwrap();

        assert_eq!(output.exit_code, 3);
        assert_eq!(output.stdout, b"stdout");
    }

    #[tokio::test]
    async fn output_is_forwarded_while_the_guest_runs() {
        let queue = tempfile::tempdir().unwrap();
        let (forwarded_tx, mut forwarded_rx) = tokio::sync::mpsc::unbounded_channel();
        let sink: OutputSink = Arc::new(move |stream, bytes: &[u8]| {
            forwarded_tx.send((stream, bytes.to_vec())).unwrap();
        });
        let console = pipe::MemoryOutputPipe::new(1024);
        let tail = OutputTail::spawn(queue.path().to_owned(), vec![console.clone()], sink);
        let stderr = queue.path().join("stderr");
        tokio::fs::write(&stderr, b"before hang\n").await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(10), forwarded_rx.recv())
            .await
            .expect("stderr must be forwarded before the guest finishes")
            .unwrap();
        assert_eq!(first, (LogStreamType::StdErr, b"before hang\n".to_vec()));

        // Firecracker replaces the whole file; only the appended part must be forwarded.
        let replacement = queue.path().join("stderr.tmp");
        tokio::fs::write(&replacement, b"before hang\nafter\n")
            .await
            .unwrap();
        tokio::fs::rename(replacement, &stderr).await.unwrap();
        tokio::fs::write(queue.path().join("stdout"), b"\"result\"\n")
            .await
            .unwrap();
        wasmtime_wasi::p2::OutputStream::write(
            &mut console.clone(),
            bytes::Bytes::from_static(b"console\n"),
        )
        .unwrap();
        tail.finish().await;

        let mut forwarded = vec![first];
        while let Some(chunk) = forwarded_rx.recv().await {
            forwarded.push(chunk);
        }
        forwarded.sort_by_key(|(stream, bytes)| (*stream as u8, bytes.clone()));
        assert_eq!(
            forwarded,
            [
                (LogStreamType::StdOut, b"\"result\"\n".to_vec()),
                (LogStreamType::StdErr, b"after\n".to_vec()),
                (LogStreamType::StdErr, b"before hang\n".to_vec()),
                (LogStreamType::StdErr, b"console\n".to_vec()),
            ]
        );
    }
}
