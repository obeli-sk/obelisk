use anyhow::{Context as _, bail};
use concepts::storage::http_client_trace::HttpClientTrace;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt as _;
use wasm_workers::http_request_policy::HttpRequestPolicy;
use wasm_workers::store_limits::StoreMemoryLimiter;
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::p1::WasiP1Ctx;
use wasmtime_wasi::{FsPerms, WasiCtxBuilder, p1, p2::pipe};

mod http_bridge;

pub struct VmOutput {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
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

/// Dropping the returned future stops the VM, the same way a regular activity is cancelled.
#[expect(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn execute(
    engine: &Engine,
    module: Module,
    mut mapdirs: Vec<MapDir>,
    guest_args: Vec<String>,
    mut env: HashMap<String, String>,
    stdin: Option<Vec<u8>>,
    policy: HttpRequestPolicy,
    http_client_traces: Arc<Mutex<Vec<HttpClientTrace>>>,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    memory: Option<u64>,
) -> anyhow::Result<VmOutput> {
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
    let stderr = pipe::MemoryOutputPipe::new(max_stderr_bytes);
    let exit_code = run_until_activity_completes(
        engine,
        &module,
        &mapdirs,
        &guest_args,
        &env,
        stdout.clone(),
        stderr.clone(),
        memory,
        queue.path(),
        started,
    )
    .await?;
    let mut output = VmOutput {
        exit_code,
        stdout: stdout.contents().to_vec(),
        stderr: stderr.contents().to_vec(),
    };
    replace_output_from_guest_files(
        &mut output,
        queue.path(),
        max_stdout_bytes,
        max_stderr_bytes,
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

async fn replace_output_from_guest_files(
    output: &mut VmOutput,
    queue: &Path,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
) -> anyhow::Result<()> {
    let Some(stdout) = read_guest_output(queue.join("stdout"), max_stdout_bytes).await? else {
        return Ok(());
    };
    let stderr = read_guest_output(queue.join("stderr"), max_stderr_bytes)
        .await?
        .unwrap_or_default();

    // The VM has one serial console, so retain any emulator or init output as diagnostics.
    // Activity output itself travels through the 9p control directory and remains separated.
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
    Ok(())
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
        let module = module(&engine, "(module (func (export \"_start\") (loop br 0)))");
        let queue = tempfile::tempdir().unwrap();
        let marker = queue.path().join("phase-activity-complete");
        // The delay only makes the guest loop likely to be running; the outcome does not depend on it.
        let signal = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
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
                pipe::MemoryOutputPipe::new(1024),
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
        assert_eq!(exit_code, 0);
    }

    #[tokio::test]
    async fn guest_files_separate_output_and_override_exit_code() {
        let queue = tempfile::tempdir().unwrap();
        tokio::fs::write(queue.path().join("stdout"), b"\"result\"\n")
            .await
            .unwrap();
        tokio::fs::write(queue.path().join("stderr"), b"guest diagnostic\n")
            .await
            .unwrap();
        tokio::fs::write(queue.path().join("exit-code"), b"7\n")
            .await
            .unwrap();
        let mut output = VmOutput {
            exit_code: 0,
            stdout: b"serial console\n".to_vec(),
            stderr: b"emulator diagnostic\n".to_vec(),
        };

        replace_output_from_guest_files(&mut output, queue.path(), 1024, 1024)
            .await
            .unwrap();

        assert_eq!(output.exit_code, 7);
        assert_eq!(output.stdout, b"\"result\"\n");
        assert_eq!(
            output.stderr,
            b"guest diagnostic\nemulator diagnostic\nserial console\n"
        );
    }

    #[tokio::test]
    async fn absent_guest_files_preserve_wasi_output() {
        let queue = tempfile::tempdir().unwrap();
        let mut output = VmOutput {
            exit_code: 3,
            stdout: b"stdout".to_vec(),
            stderr: b"stderr".to_vec(),
        };

        replace_output_from_guest_files(&mut output, queue.path(), 1024, 1024)
            .await
            .unwrap();

        assert_eq!(output.exit_code, 3);
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");
    }
}
