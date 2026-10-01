use super::{
    AbortOnDrop, MapDir, VmOutput, http_bridge, log_guest_phases, native_qemu,
    replace_output_from_guest_files, store_image,
};
use anyhow::{Context as _, bail, ensure};
use concepts::storage::http_client_trace::HttpClientTrace;
use nix::sys::inotify::{AddWatchFlags, InitFlags, Inotify};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixListener;
use wasm_workers::http_request_policy::HttpRequestPolicy;

/// Guest-initiated vsock connections to this port reach the host at `<uds_path>_<port>`.
const MAILBOX_PORT: u32 = 1024;
/// Suffix of files the mirror is still writing; neither side forwards them.
const MIRROR_TMP: &str = ".mbtmp";

#[derive(Deserialize)]
struct Machine {
    boot_args: String,
    max_cpus: u32,
    min_memory_mib: u64,
}

struct Bundle {
    firecracker: PathBuf,
    mkfs_erofs: PathBuf,
    kernel: PathBuf,
    initrd: PathBuf,
    machine: Machine,
}

impl Bundle {
    fn load(bundle: &Path) -> anyhow::Result<Self> {
        let guest = bundle.join("guest");
        let machine = std::fs::read(guest.join("machine.json"))
            .context("cannot read the Firecracker bundle's machine.json")?;
        Ok(Self {
            // Taken from PATH like native QEMU; a cold boot does not need the build's exact version.
            firecracker: PathBuf::from("firecracker"),
            mkfs_erofs: PathBuf::from("mkfs.erofs"),
            kernel: guest.join("vmlinux"),
            initrd: guest.join("initramfs.cpio.gz"),
            machine: serde_json::from_slice(&machine)?,
        })
    }

    fn memory_mib(&self, memory: u64) -> anyhow::Result<u64> {
        let mib = memory.div_ceil(1 << 20);
        ensure!(
            mib >= self.machine.min_memory_mib,
            "activity_vm `memory` must be at least {} MiB",
            self.machine.min_memory_mib
        );
        Ok(mib)
    }

    fn check_cpus(&self, cpus: u32) -> anyhow::Result<()> {
        ensure!(
            (1..=self.machine.max_cpus).contains(&cpus),
            "activity_vm `cpus` must be between 1 and {}",
            self.machine.max_cpus
        );
        Ok(())
    }
}

pub fn validate(bundle: &Path, memory: u64, cpus: u32) -> anyhow::Result<()> {
    let bundle = Bundle::load(bundle)?;
    bundle.memory_mib(memory)?;
    bundle.check_cpus(cpus)
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn execute(
    bundle: &Path,
    guest_memory: u64,
    guest_cpus: u32,
    mapdirs: Vec<MapDir>,
    guest_args: Vec<String>,
    mut env: HashMap<String, String>,
    stdin: Option<Vec<u8>>,
    policy: HttpRequestPolicy,
    traces: Arc<Mutex<Vec<HttpClientTrace>>>,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
) -> anyhow::Result<VmOutput> {
    let started = Instant::now();
    let bundle = Bundle::load(bundle)?;
    let memory_mib = bundle.memory_mib(guest_memory)?;
    bundle.check_cpus(guest_cpus)?;

    let work = tempfile::tempdir()?;
    let queue = tempfile::tempdir()?;
    let store_image = store_image::store_image(&bundle.mkfs_erofs, &mapdirs).await?;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Firecracker store image ready"
    );

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
    // The guest runs `run.sh` as soon as it arrives, so it is mirrored last.
    let run_script = native_qemu::invocation_script(&guest_args, &env)?;
    let _broker = AbortOnDrop(tokio::spawn(http_bridge::serve(
        queue.path().to_owned(),
        policy,
        traces,
    )));

    let vsock = work.path().join("v.sock");
    let listener = UnixListener::bind(work.path().join(format!("v.sock_{MAILBOX_PORT}")))?;
    let config = serde_json::json!({
        "boot-source": {
            "kernel_image_path": bundle.kernel,
            "initrd_path": bundle.initrd,
            "boot_args": bundle.machine.boot_args,
        },
        "drives": [{
            "drive_id": "store",
            "path_on_host": store_image.path().join("store.img"),
            "is_root_device": false,
            "is_read_only": true,
        }],
        "machine-config": { "vcpu_count": guest_cpus, "mem_size_mib": memory_mib },
        "vsock": { "guest_cid": 3, "uds_path": vsock },
    });
    let config_path = work.path().join("vm.json");
    tokio::fs::write(&config_path, serde_json::to_vec(&config)?).await?;
    let mut child = tokio::process::Command::new(&bundle.firecracker)
        .arg("--no-api")
        .arg("--config-file")
        .arg(&config_path)
        .args(["--level", "Error"])
        .current_dir(work.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("starting {}", bundle.firecracker.display()))?;
    let mut stdout_pipe = child
        .stdout
        .take()
        .context("Firecracker serial output is missing")?;
    let mut stderr_pipe = child
        .stderr
        .take()
        .context("Firecracker stderr is missing")?;
    let stdout_reader = tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        let mut output = Vec::new();
        while let Ok(count) = stdout_pipe.read(&mut buffer).await
            && count > 0
        {
            tracing::debug!(serial = %String::from_utf8_lossy(&buffer[..count]), "Firecracker serial output");
            if output.len() < 64 * 1024 {
                output.extend_from_slice(&buffer[..count.min(64 * 1024 - output.len())]);
            }
        }
        output
    });
    let stderr_reader = tokio::spawn(async move {
        let mut stderr = Vec::new();
        stderr_pipe.read_to_end(&mut stderr).await.map(|_| stderr)
    });

    let connection = tokio::select! {
        accepted = tokio::time::timeout(Duration::from_secs(15), listener.accept()) => {
            accepted.context("Firecracker guest did not connect its mailbox within 15 seconds")??.0
        }
        exited = child.wait() => {
            let status = exited?;
            let stderr = stderr_reader.await??;
            let serial = stdout_reader.await?;
            bail!(
                "Firecracker exited before the guest connected: {status}\n{}{}",
                String::from_utf8_lossy(&stderr),
                String::from_utf8_lossy(&serial)
            );
        }
    };
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Firecracker guest mailbox connected"
    );
    let (read, write) = connection.into_split();
    let received = Arc::new(Mutex::new(HashSet::new()));
    let _to_host = AbortOnDrop(tokio::spawn(mirror_to_host(
        read,
        queue.path().to_owned(),
        received.clone(),
    )));
    let _to_guest = AbortOnDrop(tokio::spawn(mirror_to_guest(
        write,
        queue.path().to_owned(),
        received,
        run_script,
    )));

    let mut phases = AbortOnDrop(tokio::spawn(log_guest_phases(
        queue.path().to_owned(),
        started,
    )));
    let exit_code = tokio::select! {
        completed = &mut phases.0 => {
            completed.context("Firecracker phase observer failed")?;
            child.start_kill()?;
            child.wait().await?;
            0
        }
        exited = child.wait() => {
            let status = exited?;
            bail!("Firecracker exited before activity completion: {status}");
        }
    };
    let stderr = stderr_reader.await??;
    let serial = stdout_reader.await?;
    let mut output = VmOutput {
        exit_code,
        stdout: Vec::new(),
        stderr: [stderr, serial].concat(),
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
        "Firecracker activity complete"
    );
    Ok(output)
}

struct InotifyFd(Inotify);

impl std::os::fd::AsRawFd for InotifyFd {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsFd as _;
        self.0.as_fd().as_raw_fd()
    }
}

/// Mailbox names are lowercase by protocol.
#[expect(clippy::case_sensitive_file_extension_comparisons)]
fn is_forwarded(name: &str) -> bool {
    !name.ends_with(".tmp") && !name.ends_with(MIRROR_TMP) && !name.ends_with(".working")
}

async fn write_frame(
    stream: &mut tokio::net::unix::OwnedWriteHalf,
    name: &str,
    data: &[u8],
) -> anyhow::Result<()> {
    let mut header = Vec::with_capacity(10 + name.len());
    header.extend_from_slice(&u16::try_from(name.len())?.to_le_bytes());
    header.extend_from_slice(&u64::try_from(data.len())?.to_le_bytes());
    header.extend_from_slice(name.as_bytes());
    stream.write_all(&header).await?;
    stream.write_all(data).await?;
    Ok(())
}

/// Writes each guest file atomically, in the order the guest sent them.
async fn mirror_to_host(
    mut stream: tokio::net::unix::OwnedReadHalf,
    queue: PathBuf,
    received: Arc<Mutex<HashSet<String>>>,
) -> anyhow::Result<()> {
    loop {
        let mut header = [0_u8; 10];
        stream.read_exact(&mut header).await?;
        let name_len = usize::from(u16::from_le_bytes([header[0], header[1]]));
        let data_len = u64::from_le_bytes(header[2..].try_into().expect("8 bytes"));
        let mut name = vec![0_u8; name_len];
        stream.read_exact(&mut name).await?;
        let name = String::from_utf8(name)?;
        ensure!(
            !name.is_empty() && !name.contains('/') && name != "." && name != "..",
            "invalid mailbox file name from guest: {name:?}"
        );
        let mut data = Vec::new();
        (&mut stream).take(data_len).read_to_end(&mut data).await?;
        ensure!(
            data.len() as u64 == data_len,
            "guest mailbox closed mid-file"
        );
        let temporary = queue.join(format!("{name}{MIRROR_TMP}"));
        tokio::fs::write(&temporary, data).await?;
        received.lock().unwrap().insert(name.clone());
        tokio::fs::rename(temporary, queue.join(name)).await?;
    }
}

/// Sends the initial files, then every host file as inotify reports it, preserving write order.
async fn mirror_to_guest(
    mut stream: tokio::net::unix::OwnedWriteHalf,
    queue: PathBuf,
    received: Arc<Mutex<HashSet<String>>>,
    run_script: String,
) -> anyhow::Result<()> {
    let inotify = Inotify::init(InitFlags::IN_NONBLOCK | InitFlags::IN_CLOEXEC)?;
    inotify.add_watch(
        &queue,
        AddWatchFlags::IN_CLOSE_WRITE | AddWatchFlags::IN_MOVED_TO,
    )?;
    let inotify = AsyncFd::new(InotifyFd(inotify))?;
    let mut initial = std::fs::read_dir(&queue)?
        .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    initial.sort();
    for name in initial {
        let data = tokio::fs::read(queue.join(&name)).await?;
        write_frame(&mut stream, &name, &data).await?;
    }
    write_frame(&mut stream, "run.sh", run_script.as_bytes()).await?;
    loop {
        let mut guard = inotify.readable().await?;
        let events = match guard.get_inner().0.read_events() {
            Ok(events) => events,
            Err(nix::errno::Errno::EAGAIN) => {
                guard.clear_ready();
                Vec::new()
            }
            Err(error) => return Err(error.into()),
        };
        for event in events {
            if let Some(name) = event.name.as_deref().and_then(std::ffi::OsStr::to_str)
                && is_forwarded(name)
                && !received.lock().unwrap().contains(name)
            {
                // A reader may have consumed it already; nothing to forward then.
                if let Ok(data) = tokio::fs::read(queue.join(name)).await {
                    write_frame(&mut stream, name, &data).await?;
                }
            }
        }
    }
}
