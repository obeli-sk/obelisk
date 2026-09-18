use anyhow::{Context as _, bail};
use concepts::storage::http_client_trace::HttpClientTrace;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt as _;
use wasm_workers::http_request_policy::HttpRequestPolicy;
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::{FsPerms, WasiCtxBuilder, p1, p2::pipe};

mod http_bridge;

pub struct VmOutput {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub http_client_traces: Vec<HttpClientTrace>,
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

#[expect(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn execute(
    engine: &Engine,
    module: Module,
    mut mapdirs: Vec<MapDir>,
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
    let traces = Arc::new(Mutex::new(Vec::new()));
    let broker = tokio::spawn(http_bridge::serve(
        queue.path().to_owned(),
        policy,
        traces.clone(),
    ));
    let activity_completed = Arc::new(AtomicBool::new(false));
    let (phase_logger_stop, phase_logger_stop_rx) = tokio::sync::oneshot::channel();
    let phase_logger = tokio::spawn(log_guest_phases(
        queue.path().to_owned(),
        started,
        phase_logger_stop_rx,
        activity_completed.clone(),
        engine.clone(),
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
            &guest_args,
            &env,
            max_stdout_bytes,
            max_stderr_bytes,
            cancelled,
            activity_completed,
        )
    })
    .await?;
    broker.abort();
    let _ = broker.await;
    let vm_returned_at = started.elapsed();
    let _ = phase_logger_stop.send(());
    if let Ok(Some(activity_completed_at)) = phase_logger.await {
        tracing::debug!(
            activity_completed_ms = activity_completed_at.as_millis(),
            vm_termination_ms = vm_returned_at
                .saturating_sub(activity_completed_at)
                .as_millis(),
            "Activity VM termination measured"
        );
    }
    let mut output = result?;
    replace_output_from_guest_files(
        &mut output,
        queue.path(),
        max_stdout_bytes,
        max_stderr_bytes,
    )
    .await?;
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

async fn log_guest_phases(
    queue: PathBuf,
    started: Instant,
    mut stop: tokio::sync::oneshot::Receiver<()>,
    activity_completed: Arc<AtomicBool>,
    engine: Engine,
) -> Option<Duration> {
    const PHASES: &[(&str, &str)] = &[
        ("guest-launcher", "Linux reached the activity VM launcher"),
        ("store-mounted", "Nix store mapped"),
        ("network-ready", "Activity VM network bridge ready"),
        ("command-start", "Activity VM command starting"),
        ("activity-complete", "Activity VM command completed"),
        ("store-mount-failed", "Nix store mapping failed"),
        ("network-failed", "Activity VM network bridge failed"),
    ];
    let mut observed = [false; PHASES.len()];
    let mut activity_completed_at = None;
    loop {
        log_available_guest_phases(&queue, started, &mut observed, PHASES).await;
        if observed[4] && activity_completed_at.is_none() {
            activity_completed_at = Some(started.elapsed());
            activity_completed.store(true, Ordering::Release);
            engine.increment_epoch();
        }
        if observed[..5].iter().all(|observed| *observed) {
            return activity_completed_at;
        }
        tokio::select! {
            _ = &mut stop => break,
            () = tokio::time::sleep(std::time::Duration::from_millis(2)) => {}
        }
    }
    log_available_guest_phases(&queue, started, &mut observed, PHASES).await;
    if observed[4] && activity_completed_at.is_none() {
        activity_completed_at = Some(started.elapsed());
        activity_completed.store(true, Ordering::Release);
        engine.increment_epoch();
    }
    activity_completed_at
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
    activity_completed: Arc<AtomicBool>,
) -> anyhow::Result<VmOutput> {
    let started = Instant::now();
    let mut linker = Linker::new(engine);
    p1::add_to_linker_sync(&mut linker, |ctx| ctx)?;
    let pre = linker.instantiate_pre(module)?;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Activity VM linker prepared"
    );
    let stdout = pipe::MemoryOutputPipe::new(max_stdout_bytes.saturating_add(1));
    let stderr = pipe::MemoryOutputPipe::new(max_stderr_bytes);
    let mut wasi = WasiCtxBuilder::new();
    wasi.stdin(pipe::ClosedInputStream)
        .stdout(stdout.clone())
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
    let mut store = Store::new(engine, wasi.build_p1());
    let cancelled_for_deadline = cancelled.clone();
    let activity_completed_for_deadline = activity_completed.clone();
    store.epoch_deadline_callback(move |_| {
        Ok(
            if cancelled_for_deadline.load(Ordering::Relaxed)
                || activity_completed_for_deadline.load(Ordering::Acquire)
            {
                wasmtime::UpdateDeadline::Interrupt
            } else {
                wasmtime::UpdateDeadline::Continue(1)
            },
        )
    });
    store.set_epoch_deadline(1);
    let instantiate_started = Instant::now();
    let instance = pre.instantiate(&mut store)?;
    tracing::debug!(
        elapsed_ms = instantiate_started.elapsed().as_millis(),
        total_elapsed_ms = started.elapsed().as_millis(),
        "Activity VM module instantiated"
    );
    let start = instance.get_typed_func::<(), ()>(&mut store, "_start")?;
    let call_started = Instant::now();
    let exit_code = match start.call(&mut store, ()) {
        Ok(()) => 0,
        Err(error)
            if activity_completed.load(Ordering::Acquire)
                && !cancelled.load(Ordering::Relaxed)
                && error.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::Interrupt) =>
        {
            0
        }
        Err(error) => match error.downcast_ref::<wasmtime_wasi::I32Exit>() {
            Some(exit) => exit.0,
            None => bail!("VM trapped: {error:?}"),
        },
    };
    tracing::debug!(
        elapsed_ms = call_started.elapsed().as_millis(),
        total_elapsed_ms = started.elapsed().as_millis(),
        runtime_exit_code = exit_code,
        "Activity VM module returned"
    );
    Ok(VmOutput {
        exit_code,
        stdout: stdout.contents().to_vec(),
        stderr: stderr.contents().to_vec(),
        http_client_traces: Vec::new(),
    })
}

pub fn compile(engine: &Engine, module_path: &Path) -> anyhow::Result<Module> {
    Module::from_file(engine, module_path)
        .map_err(|error| anyhow::anyhow!("loading VM runtime {}: {error:?}", module_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert_eq!(output.exit_code, 0);
    }

    #[test]
    fn activity_completion_interrupts_the_vm() {
        let mut config = wasmtime::Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).unwrap();
        let module = Module::new(
            &engine,
            wat::parse_str("(module (func (export \"_start\") (loop br 0)))").unwrap(),
        )
        .unwrap();
        let activity_completed = Arc::new(AtomicBool::new(false));
        let signal = activity_completed.clone();
        let signal_engine = engine.clone();
        let signal_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            signal.store(true, Ordering::Release);
            signal_engine.increment_epoch();
        });

        let output = run_module(
            &engine,
            &module,
            &[],
            &[],
            &HashMap::new(),
            1024,
            1024,
            Arc::new(AtomicBool::new(false)),
            activity_completed,
        )
        .unwrap();
        signal_thread.join().unwrap();
        assert_eq!(output.exit_code, 0);
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
            http_client_traces: Vec::new(),
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
            http_client_traces: Vec::new(),
        };

        replace_output_from_guest_files(&mut output, queue.path(), 1024, 1024)
            .await
            .unwrap();

        assert_eq!(output.exit_code, 3);
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");
    }
}
