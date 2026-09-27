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
    args: Vec<String>,
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn execute(
    bundle: &Path,
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
    let machine: Machine =
        serde_json::from_slice(&tokio::fs::read(guest.join("machine.json")).await?)?;
    let qemu = tokio::fs::read_to_string(bundle.join("qemu-path")).await?;
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
    let mut child = tokio::process::Command::new(qemu.trim())
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
    tokio::time::timeout(Duration::from_secs(15), wait_until_running(&qmp_socket))
        .await
        .context("native QEMU did not restore its snapshot within 15 seconds")??;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Native QEMU snapshot restored"
    );
    child
        .stdin
        .take()
        .context("native QEMU serial input is missing")?
        .write_all(b"\n")
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

async fn wait_until_running(socket: &Path) -> anyhow::Result<()> {
    let stream = loop {
        match tokio::net::UnixStream::connect(socket).await {
            Ok(stream) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    read_qmp(&mut lines).await?;
    write
        .write_all(b"{\"execute\":\"qmp_capabilities\"}\n")
        .await?;
    read_qmp(&mut lines).await?;
    loop {
        write.write_all(b"{\"execute\":\"query-status\"}\n").await?;
        let reply = read_qmp(&mut lines).await?;
        if reply["return"]["status"] == "running" {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn read_qmp(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
) -> anyhow::Result<serde_json::Value> {
    loop {
        let line = lines
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
