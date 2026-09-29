use super::{
    AbortOnDrop, MapDir, VmOutput, http_bridge, log_guest_phases, replace_output_from_guest_files,
};
use anyhow::{Context as _, bail, ensure};
use concepts::storage::http_client_trace::HttpClientTrace;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use wasm_workers::http_request_policy::HttpRequestPolicy;
use wasmtime_wasi::FsPerms;

#[derive(Deserialize)]
struct Machine {
    ram: String,
    #[serde(default)]
    hotplug: Option<Hotplug>,
    #[serde(default)]
    cpu_hotplug: Option<CpuHotplug>,
    args: Vec<String>,
}

/// The snapshot has one vCPU; the host hot-adds the rest after restore.
#[derive(Deserialize)]
struct CpuHotplug {
    max: u32,
}

/// A virtio-mem device in the snapshot, empty until the host plugs memory after restore.
#[derive(Deserialize)]
struct Hotplug {
    device: String,
    max_bytes: u64,
    block_bytes: u64,
}

impl Machine {
    fn load(guest: &Path) -> anyhow::Result<Self> {
        let path = guest.join("machine.json");
        let bytes =
            std::fs::read(&path).with_context(|| format!("cannot read {}", path.display()))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn base_bytes(&self) -> anyhow::Result<u64> {
        let (number, shift) = match self.ram.as_bytes().last() {
            Some(b'M') => (&self.ram[..self.ram.len() - 1], 20),
            Some(b'G') => (&self.ram[..self.ram.len() - 1], 30),
            _ => bail!("unsupported native QEMU `ram`: {}", self.ram),
        };
        Ok(number.parse::<u64>()? << shift)
    }

    /// Bytes to plug so the guest has at least `memory`, rounded up to the device block size.
    fn plug_bytes(&self, memory: u64) -> anyhow::Result<u64> {
        let base = self.base_bytes()?;
        ensure!(
            memory >= base,
            "activity_vm `memory` must be at least the runtime's base RAM of {} MiB",
            base >> 20
        );
        if memory == base {
            return Ok(0);
        }
        let hotplug = self.hotplug.as_ref().context(
            "this native QEMU runtime cannot change guest RAM; update the runtime bundle",
        )?;
        let plug = (memory - base).div_ceil(hotplug.block_bytes) * hotplug.block_bytes;
        ensure!(
            plug <= hotplug.max_bytes,
            "activity_vm `memory` exceeds the runtime's limit of {} MiB",
            (base + hotplug.max_bytes) >> 20
        );
        Ok(plug)
    }

    fn check_cpus(&self, cpus: u32) -> anyhow::Result<()> {
        ensure!(cpus >= 1, "activity_vm `cpus` must be at least 1");
        if cpus == 1 {
            return Ok(());
        }
        let max = self
            .cpu_hotplug
            .as_ref()
            .context("this native QEMU runtime cannot add vCPUs; update the runtime bundle")?
            .max;
        ensure!(
            cpus <= max,
            "activity_vm `cpus` exceeds the runtime's limit of {max}"
        );
        Ok(())
    }
}

/// Checks at deployment time that the bundle can give the guest `memory` bytes of RAM.
pub fn validate_guest_memory(bundle: &Path, memory: u64) -> anyhow::Result<()> {
    Machine::load(&bundle.join("guest"))?.plug_bytes(memory)?;
    Ok(())
}

/// Checks at deployment time that the bundle can give the guest `cpus` vCPUs.
pub fn validate_guest_cpus(bundle: &Path, cpus: u32) -> anyhow::Result<()> {
    Machine::load(&bundle.join("guest"))?.check_cpus(cpus)
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
    let guest = bundle.join("guest");
    let machine = Machine::load(&guest)?;
    let plug = machine.plug_bytes(guest_memory)?;
    machine.check_cpus(guest_cpus)?;
    let snapshot = bundle.join("vm.state");
    ensure!(snapshot.is_file(), "native QEMU snapshot is missing");

    let share = match mapdirs.first().and_then(|mapping| mapping.host.parent()) {
        Some(parent) => tempfile::tempdir_in(parent)?,
        None => tempfile::tempdir()?,
    };
    let queue = tempfile::tempdir()?;
    let control = tempfile::tempdir()?;
    let qmp_socket = control.path().join("qmp.sock");
    for mapping in &mapdirs {
        install_mapping(share.path(), mapping).await?;
    }
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Native QEMU mappings staged"
    );
    tokio::fs::create_dir_all(share.path().join("nix/store")).await?;
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
    tokio::fs::write(
        queue.path().join("run.sh"),
        invocation_script(&guest_args, &env)?,
    )
    .await?;
    let _broker = AbortOnDrop(tokio::spawn(http_bridge::serve(
        queue.path().to_owned(),
        policy,
        traces,
    )));

    let args = machine
        .args
        .iter()
        .map(|arg| {
            arg.replace("{pack}", &guest.to_string_lossy())
                .replace("{share}", &share.path().to_string_lossy())
                .replace("{queue}", &queue.path().to_string_lossy())
                .replace("{ram}", &machine.ram)
        })
        .collect::<Vec<_>>();
    let mut child = tokio::process::Command::new("qemu-system-x86_64")
        .args(args)
        .arg("-incoming")
        .arg(format!("file:{}", snapshot.display()))
        .arg("-qmp")
        .arg(format!("unix:{},server,nowait", qmp_socket.display()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("starting native QEMU")?;
    let mut stderr_pipe = child
        .stderr
        .take()
        .context("native QEMU stderr is missing")?;
    let mut stdout_pipe = child
        .stdout
        .take()
        .context("native QEMU serial output is missing")?;
    let stdout_reader = tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        let mut output = Vec::new();
        while let Ok(count) = stdout_pipe.read(&mut buffer).await {
            if count == 0 {
                break;
            }
            tracing::debug!(serial = %String::from_utf8_lossy(&buffer[..count]), "Native QEMU serial output");
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
    let mut qmp = tokio::time::timeout(Duration::from_secs(15), wait_until_running(&qmp_socket))
        .await
        .context("native QEMU did not restore its snapshot within 15 seconds")??;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Native QEMU snapshot restored"
    );
    if plug > 0 {
        let hotplug = machine.hotplug.as_ref().expect("checked by plug_bytes");
        tokio::time::timeout(
            Duration::from_secs(60),
            qmp.plug(&format!("/machine/peripheral/{}", hotplug.device), plug),
        )
        .await
        .context("native QEMU guest did not plug its memory within 60 seconds")??;
        tracing::debug!(
            elapsed_ms = started.elapsed().as_millis(),
            plug_mib = plug >> 20,
            "Native QEMU guest memory plugged"
        );
    }
    if guest_cpus > 1 {
        qmp.add_cpus(guest_cpus - 1).await?;
        tracing::debug!(
            elapsed_ms = started.elapsed().as_millis(),
            guest_cpus,
            "Native QEMU guest vCPUs added"
        );
    }
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    // Above one vCPU, `init` waits for ACPI to register them all and onlines them.
    let cpus = if guest_cpus > 1 {
        format!(" {guest_cpus}")
    } else {
        String::new()
    };
    let clock = format!("{}.{:09}{cpus}\n", now.as_secs(), now.subsec_nanos());
    child
        .stdin
        .take()
        .context("native QEMU serial input is missing")?
        .write_all(clock.as_bytes())
        .await?;

    let mut phases = AbortOnDrop(tokio::spawn(log_guest_phases(
        queue.path().to_owned(),
        started,
    )));
    let exit_code = tokio::select! {
        completed = &mut phases.0 => {
            completed.context("native QEMU phase observer failed")?;
            child.start_kill()?;
            child.wait().await?;
            0
        }
        exited = child.wait() => {
            let status = exited?;
            bail!("native QEMU exited before activity completion: {status}");
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
        "Native QEMU activity complete"
    );
    Ok(output)
}

struct Qmp {
    lines: tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    write: tokio::net::unix::OwnedWriteHalf,
}

impl Qmp {
    async fn execute(
        &mut self,
        command: &str,
        arguments: Option<serde_json::Value>,
    ) -> anyhow::Result<serde_json::Value> {
        let mut request = serde_json::json!({ "execute": command });
        if let Some(arguments) = arguments {
            request["arguments"] = arguments;
        }
        let mut line = serde_json::to_vec(&request)?;
        line.push(b'\n');
        self.write.write_all(&line).await?;
        Ok(self.read().await?["return"].take())
    }

    async fn read(&mut self) -> anyhow::Result<serde_json::Value> {
        loop {
            let line = self
                .lines
                .next_line()
                .await?
                .context("native QEMU QMP socket closed")?;
            let reply: serde_json::Value = serde_json::from_str(&line)?;
            if let Some(error) = reply.get("error") {
                bail!("native QEMU QMP error: {error}");
            }
            if reply.get("return").is_some() || reply.get("QMP").is_some() {
                return Ok(reply);
            }
        }
    }

    /// The guest driver onlines memory as it plugs it, so a matching `size` means usable RAM.
    async fn plug(&mut self, path: &str, bytes: u64) -> anyhow::Result<()> {
        self.execute(
            "qom-set",
            Some(serde_json::json!({ "path": path, "property": "requested-size", "value": bytes })),
        )
        .await?;
        let get = serde_json::json!({ "path": path, "property": "size" });
        while self.execute("qom-get", Some(get.clone())).await? != bytes {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }

    async fn add_cpus(&mut self, count: u32) -> anyhow::Result<()> {
        let slots = self.execute("query-hotpluggable-cpus", None).await?;
        let mut free = slots
            .as_array()
            .context("unexpected `query-hotpluggable-cpus` reply")?
            .iter()
            .filter(|slot| slot.get("qom-path").is_none())
            .collect::<Vec<_>>();
        free.sort_by_key(|slot| slot["props"]["socket-id"].as_u64());
        ensure!(
            free.len() >= count as usize,
            "native QEMU has only {} free vCPU slots",
            free.len()
        );
        for (index, slot) in free.into_iter().take(count as usize).enumerate() {
            let mut arguments = slot["props"].clone();
            arguments["driver"] = slot["type"].clone();
            arguments["id"] = format!("cpu{}", index + 1).into();
            self.execute("device_add", Some(arguments)).await?;
        }
        Ok(())
    }
}

async fn wait_until_running(socket: &Path) -> anyhow::Result<Qmp> {
    let stream = loop {
        match tokio::net::UnixStream::connect(socket).await {
            Ok(stream) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let (read, write) = stream.into_split();
    let mut qmp = Qmp {
        lines: BufReader::new(read).lines(),
        write,
    };
    qmp.read().await?;
    qmp.execute("qmp_capabilities", None).await?;
    while qmp.execute("query-status", None).await?["status"] != "running" {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(qmp)
}

async fn install_mapping(share: &Path, mapping: &MapDir) -> anyhow::Result<()> {
    ensure!(
        mapping.guest.starts_with('/') && !mapping.guest.split('/').any(|part| part == ".."),
        "unsafe native QEMU guest mapping: {}",
        mapping.guest
    );
    let destination = share.join(mapping.guest.trim_start_matches('/'));
    tokio::fs::create_dir_all(destination.parent().context("mapping has no parent")?).await?;
    let source = mapping.host.clone();
    tokio::task::spawn_blocking(move || {
        let status = std::process::Command::new("cp")
            .args(["-a", "-l", "--"])
            .arg(&source)
            .arg(&destination)
            .status()?;
        ensure!(
            status.success(),
            "copying native QEMU mapping {} failed: {status}",
            source.display()
        );
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        mapping.permissions == FsPerms::ReadOnly,
        "native QEMU supports writable files only in its mailbox"
    );
    Ok(())
}

fn invocation_script(args: &[String], env: &HashMap<String, String>) -> anyhow::Result<String> {
    ensure!(
        args.first().is_some_and(|arg| arg == "-no-stdin"),
        "unexpected VM invocation"
    );
    let mut script = String::from("#!/bin/sh\nset -eu\n");
    for (key, value) in env {
        ensure!(
            key.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_alphabetic() || byte == b'_' || (index > 0 && byte.is_ascii_digit())
            }),
            "invalid VM environment name: {key}"
        );
        script.push_str("export ");
        script.push_str(key);
        script.push('=');
        script.push_str(&quote(value));
        script.push('\n');
    }
    for arg in &args[1..] {
        if !script.ends_with('\n') {
            script.push(' ');
        }
        script.push_str(&quote(arg));
    }
    script.push('\n');
    Ok(script)
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine(hotplug: bool) -> Machine {
        Machine {
            ram: "256M".to_owned(),
            cpu_hotplug: hotplug.then_some(CpuHotplug { max: 16 }),
            hotplug: hotplug.then(|| Hotplug {
                device: "vmem".to_owned(),
                max_bytes: 16 << 30,
                block_bytes: 2 << 20,
            }),
            args: Vec::new(),
        }
    }

    #[test]
    fn plug_bytes_is_the_difference_rounded_up_to_blocks() {
        let machine = machine(true);
        assert_eq!(0, machine.plug_bytes(256 << 20).unwrap());
        assert_eq!(2 << 20, machine.plug_bytes((256 << 20) + 1).unwrap());
        assert_eq!(256 << 20, machine.plug_bytes(512 << 20).unwrap());
        assert_eq!(
            16 << 30,
            machine.plug_bytes((16 << 30) + (256 << 20)).unwrap()
        );
    }

    #[test]
    fn plug_bytes_rejects_sizes_outside_the_bundle() {
        let machine = machine(true);
        assert!(machine.plug_bytes(128 << 20).is_err());
        assert!(machine.plug_bytes((16 << 30) + (258 << 20)).is_err());
    }

    #[test]
    fn bundle_without_hotplug_only_accepts_its_base_ram() {
        let machine = machine(false);
        assert_eq!(0, machine.plug_bytes(256 << 20).unwrap());
        let error = machine.plug_bytes(1 << 30).unwrap_err().to_string();
        assert!(error.contains("update the runtime bundle"), "{error}");
    }

    #[test]
    fn check_cpus_accepts_one_up_to_the_bundle_limit() {
        let machine = machine(true);
        assert!(machine.check_cpus(0).is_err());
        machine.check_cpus(1).unwrap();
        machine.check_cpus(16).unwrap();
        assert!(machine.check_cpus(17).is_err());
        let machine = self::machine(false);
        machine.check_cpus(1).unwrap();
        let error = machine.check_cpus(2).unwrap_err().to_string();
        assert!(error.contains("update the runtime bundle"), "{error}");
    }
}
