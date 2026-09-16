use anyhow::{Context, bail};
use concepts::storage::http_client_trace::HttpClientTrace;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;
use wasm_workers::http_request_policy::HttpRequestPolicy;
use wasmtime::{
    Engine, Extern, ExternType, Linker, Memory, MemoryType, Module, Ref, SharedMemory, Store,
};
use wasmtime_wasi::{FsPerms, WasiCtxBuilder, p1, p2::pipe};

mod emscripten;
mod host_fs;
mod http_bridge;
mod legacy_fd;
mod qemu_jit;

struct VmState {
    wasi: p1::WasiP1Ctx,
    qemu_jit: qemu_jit::QemuJit,
    host_fs: Arc<host_fs::HostFs>,
    legacy_fds: Arc<legacy_fd::LegacyFdTable>,
    fiber_next: Option<i32>,
    fiber_entries: HashMap<i32, (i32, i32)>,
    poll_calls: u64,
    pthread_spawn: Option<PthreadSpawn>,
}

type PthreadSpawn = Arc<dyn Fn(i32, i32, i32, i32) -> wasmtime::Result<i32> + Send + Sync>;

struct EmscriptenRuntime {
    engine: Engine,
    module: Module,
    memory: SharedMemory,
    host_fs: Arc<host_fs::HostFs>,
    legacy_fds: Arc<legacy_fd::LegacyFdTable>,
    mapdirs: Vec<MapDir>,
    stdout: pipe::MemoryOutputPipe,
    stderr: pipe::MemoryOutputPipe,
    threads: Mutex<Vec<std::thread::JoinHandle<anyhow::Result<()>>>>,
    cancelled: Arc<AtomicBool>,
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
    let is_qemu = qemu_memory_type(&module)?.is_some();
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
    let qemu_pack = if is_qemu {
        let pack = tempfile::tempdir()?;
        tokio::fs::write(
            pack.path().join("info"),
            qemu_runtime_info(&mapdirs, &guest_args, &env)?,
        )
        .await?;
        mapdirs.push(MapDir::read_write(
            pack.path().to_owned(),
            "/pack".to_owned(),
        ));
        Some(pack)
    } else {
        None
    };
    let module_args = if is_qemu { qemu_args()? } else { guest_args };
    let traces = Arc::new(Mutex::new(Vec::new()));
    let broker = tokio::spawn(http_bridge::serve(
        queue.path().to_owned(),
        policy,
        traces.clone(),
    ));
    let (phase_logger_stop, phase_logger_stop_rx) = tokio::sync::oneshot::channel();
    let phase_logger = tokio::spawn(log_guest_phases(
        queue.path().to_owned(),
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
    let mut output = result?;
    if is_qemu {
        if let Ok(console) = tokio::fs::read(queue.path().join("console.log")).await {
            output.stdout = console;
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

fn qemu_runtime_info(
    mapdirs: &[MapDir],
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
        "chardev:activity-serial",
        "-chardev",
        "file,id=activity-serial,path=/pack/console.log",
        "-monitor",
        "none",
        "-incoming",
        "file:/image/vm.state",
        "-d",
        "cpu_reset,guest_errors",
        "-D",
        "/pack/qemu.log",
        "-net",
        "nic,model=e1000",
        "-m",
        "128M",
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
        "-virtfs",
        "local,path=/,mount_tag=wasi0,security_model=passthrough,id=wasi0",
        "-virtfs",
        "local,path=/pack,mount_tag=wasi1,security_model=passthrough,id=wasi1",
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
    let host_fs = host_fs::HostFs::new(mapdirs)?;
    let legacy_fds = legacy_fd::LegacyFdTable::new(mapdirs)?;
    let mut store = Store::new(
        engine,
        VmState {
            wasi: wasi.build_p1(),
            qemu_jit: qemu_jit::QemuJit::new(None),
            host_fs: host_fs.clone(),
            legacy_fds: legacy_fds.clone(),
            fiber_next: None,
            fiber_entries: HashMap::new(),
            poll_calls: 0,
            pthread_spawn: None,
        },
    );
    let mut linker: Linker<VmState> = Linker::new(engine);
    p1::add_to_linker_sync(&mut linker, |state: &mut VmState| &mut state.wasi)?;
    let mut emscripten_runtime = None;
    if let Some(memory_type) = qemu_memory_type(module)? {
        let memory = if memory_type.is_shared() {
            qemu_jit::QemuMemory::Shared(SharedMemory::new(engine, memory_type)?)
        } else {
            qemu_jit::QemuMemory::Plain(Memory::new(&mut store, memory_type)?)
        };
        store.data_mut().qemu_jit.memory = Some(memory.clone());
        linker.define(&mut store, "env", "memory", memory.as_extern())?;
        qemu_jit::add_to_linker(&mut linker)?;
        emscripten::add_invoke_wrappers(&mut linker, module)?;
        emscripten::add_longjmp(&mut linker)?;
        emscripten::add_ffi_call(&mut linker)?;
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
                host_fs,
                legacy_fds,
                mapdirs: mapdirs.to_vec(),
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
    let start = instance.get_typed_func::<(), ()>(&mut store, "_start")?;
    let call_started = Instant::now();
    if std::env::var_os("OBELISK_QEMU_TRACE_STDERR").is_some() {
        let stderr = stderr.clone();
        std::thread::spawn(move || {
            for _ in 0..60 {
                std::thread::sleep(std::time::Duration::from_secs(1));
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
            engine.increment_epoch();
        });
    }
    let call_result = emscripten::call_asyncify_root(&mut store, &instance, &start);
    eprintln!("QEMU root result: {call_result:?}");
    if let Some(runtime) = emscripten_runtime {
        join_pthreads(&runtime)?;
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
        http_client_traces: Vec::new(),
    })
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
            .spawn(move || run_pthread(&thread_runtime, pthread_ptr, start_routine, arg))
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

fn join_pthreads(runtime: &EmscriptenRuntime) -> anyhow::Result<()> {
    let mut failures = Vec::new();
    loop {
        let handle = runtime
            .threads
            .lock()
            .expect("Emscripten thread mutex poisoned")
            .pop();
        let Some(handle) = handle else {
            if failures.is_empty() {
                return Ok(());
            }
            anyhow::bail!("Emscripten pthread failures:\n{}", failures.join("\n\n"));
        };
        while !handle.is_finished() {
            std::thread::sleep(std::time::Duration::from_secs(2));
            let stderr = runtime.stderr.contents();
            if !stderr.is_empty() {
                eprintln!("QEMU stderr: {}", String::from_utf8_lossy(&stderr));
            }
        }
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("{error:?}")),
            Err(_) => failures.push("Emscripten pthread panicked".to_owned()),
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
    wasi.stdin(pipe::ClosedInputStream)
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
            fiber_entries: HashMap::new(),
            poll_calls: 0,
            pthread_spawn: Some(pthread_spawner(runtime.clone())),
        },
    );
    let mut linker = Linker::new(&runtime.engine);
    p1::add_to_linker_sync(&mut linker, |state: &mut VmState| &mut state.wasi)?;
    linker.define(&mut store, "env", "memory", memory.as_extern())?;
    qemu_jit::add_to_linker(&mut linker)?;
    emscripten::add_invoke_wrappers(&mut linker, &runtime.module)?;
    emscripten::add_longjmp(&mut linker)?;
    emscripten::add_ffi_call(&mut linker)?;
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
        .get_typed_func::<i32, ()>(&mut store, "_emscripten_stack_restore")?
        .call(&mut store, stack_high as i32)?;
    instance
        .get_typed_func::<(i32, i32, i32, i32, i32, i32), ()>(
            &mut store,
            "_emscripten_thread_init",
        )?
        .call(&mut store, (pthread_ptr, 0, 0, 1, 0, 0))?;
    let _ = instance
        .get_typed_func::<(), i32>(&mut store, "_emscripten_tls_init")?
        .call(&mut store, ())?;
    eprintln!("pthread {pthread_ptr:#x}: initialized");

    let table = instance
        .get_export(&mut store, "__indirect_function_table")
        .and_then(Extern::into_table)
        .context("Emscripten pthread function table is unavailable")?;
    let function = match table.get(&mut store, start_routine as u64) {
        Some(Ref::Func(Some(function))) => function,
        _ => anyhow::bail!("Emscripten pthread entry {start_routine} is not a function"),
    };
    eprintln!("pthread {pthread_ptr:#x}: entering start routine");
    let result = emscripten::call_asyncify_pthread(&mut store, &instance, &function, arg)?;
    eprintln!("pthread {pthread_ptr:#x}: start routine returned {result}");
    let diagnostics = runtime.stderr.contents();
    if !diagnostics.is_empty() {
        eprintln!(
            "pthread {pthread_ptr:#x}: QEMU stderr at return: {}",
            String::from_utf8_lossy(&diagnostics)
        );
    }
    instance
        .get_typed_func::<i32, ()>(&mut store, "_emscripten_thread_exit")?
        .call(&mut store, result)?;
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

fn qemu_memory_type(module: &Module) -> anyhow::Result<Option<MemoryType>> {
    let memory_type = module
        .imports()
        .find_map(
            |import| match (import.module(), import.name(), import.ty()) {
                ("env", "memory", ExternType::Memory(memory_type)) => Some(memory_type),
                _ => None,
            },
        );
    Ok(memory_type)
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
            host_fs: host_fs::HostFs::new(&[]).unwrap(),
            legacy_fds: legacy_fd::LegacyFdTable::new(&[]).unwrap(),
            mapdirs: Vec::new(),
            stdout: pipe::MemoryOutputPipe::new(1024),
            stderr: pipe::MemoryOutputPipe::new(1024),
            threads: Mutex::new(Vec::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
        });
        run_pthread(&runtime, pthread_ptr as i32, 0, 41).unwrap();
        assert_eq!(read_shared_u32(&memory, 0).unwrap(), 41);
        assert_eq!(read_shared_u32(&memory, 4).unwrap(), 42);
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
                fiber_entries: HashMap::new(),
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
            qemu_runtime_info(&mapdirs, &args, &env).unwrap(),
            "c: /bin/echo hello\\ world\nmr: nix/store/abc\nm: queue\nenv: FIRST=one\nenv: SECOND=two\n"
        );
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
}
