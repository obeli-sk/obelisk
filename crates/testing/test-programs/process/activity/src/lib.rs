use std::time::Duration;

use anyhow::{Context as _, bail, ensure};
use exports::testing::process::process::Guest;
use futures_concurrency::future::Join as _;
use obelisk::activity::process::{self as process_support};
use wstd::io::{AsyncInputStream, AsyncOutputStream, AsyncPollable, Cursor};
use wstd::runtime::block_on;
use wstd::wasip2::io::streams::InputStream;

wit_bindgen::generate!({
     world: "any:any/any",
       with: {
       "wasi:io/error@0.2.3": wstd::wasip2::io::error,
       "wasi:io/poll@0.2.3": wstd::wasip2::io::poll,
       "wasi:io/streams@0.2.3": wstd::wasip2::io::streams,
       "obelisk:activity/process@1.0.0": generate,
       "testing:process/process": generate,
   },
});

struct Component;
export!(Component);

fn touch() -> Result<(), anyhow::Error> {
    // Idempotently create a file so that the activity works with `reuse_on_retry`.
    let proc = process_support::spawn(
        "touch",
        &process_support::SpawnOptions {
            args: vec!["touched".to_string()],
            environment: vec![],
            current_working_directory: None,
            stdin: process_support::Stdio::Discard,
            stdout: process_support::Stdio::Discard,
            stderr: process_support::Stdio::Discard,
        },
    )?;
    println!("Waiting for {}", proc.id());
    let sub = proc.subscribe_wait();
    sub.block();
    println!("Done waiting for {}", proc.id());
    let exit_status = proc.wait()?;
    ensure!(exit_status == Some(0));

    let entries = ls();
    if entries != vec!["./touched"] {
        bail!("unexpected file list: {entries:?}");
    }
    Ok(())
}

fn ls() -> Vec<String> {
    let mut entries = std::fs::read_dir(".")
        .expect("read_dir failed")
        .map(|res| res.map(|e| e.path().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>, _>>()
        .expect("iterating over folder failed");
    entries.sort();
    println!("ls:");
    println!("{entries:?}");
    entries
}

async fn stdio() -> Result<(), anyhow::Error> {
    println!("Spawning bash");
    let proc = process_support::spawn(
        "bash",
        &process_support::SpawnOptions {
            args: vec![
                "-c".to_string(),
                format!("echo -n stderr >&2; read input; echo -n \"hello $input\""),
            ],
            environment: vec![],
            current_working_directory: None,
            stdin: process_support::Stdio::Pipe,
            stdout: process_support::Stdio::Pipe,
            stderr: process_support::Stdio::Pipe,
        },
    )?;
    println!("Taking std streams");
    let stdin = proc.take_stdin().expect("first `take_stdin` must succeed");
    let stdout = proc
        .take_stdout()
        .expect("first `take_stdout` must succeed");
    let stderr = proc
        .take_stderr()
        .expect("first `take_stderr` must succeed");

    // Write to child's stdin first
    {
        let mut stdin = AsyncOutputStream::new(stdin);
        wstd::io::copy(Cursor::new(b"wasi!\n"), &mut stdin).await?;
        stdin.flush().await?;
    }

    println!("Waiting for {}", proc.id());
    let sub = proc.subscribe_wait();
    sub.block();
    println!("Done waiting for {}", proc.id());
    let exit_status = proc.wait()?;
    ensure!(exit_status == Some(0));

    let stdout = stream_to_string(stdout).await?;
    println!("Got {stdout}");
    ensure!(stdout == "hello wasi!");

    let stderr = stream_to_string(stderr).await?;
    ensure!(stderr == "stderr");

    Ok(())
}

async fn stream_to_string(stream: InputStream) -> Result<String, anyhow::Error> {
    let mut buffer = Cursor::new(Vec::new());
    let stream = AsyncInputStream::new(stream);
    wstd::io::copy(stream, &mut buffer).await?;
    let output = buffer.into_inner();
    let output = String::from_utf8_lossy(&output).into_owned();
    Ok(output)
}

fn kill() -> Result<(), anyhow::Error> {
    let proc = process_support::spawn(
        "sleep",
        &process_support::SpawnOptions {
            args: vec!["10".to_string()],
            environment: vec![],
            current_working_directory: None,
            stdin: process_support::Stdio::Pipe,
            stdout: process_support::Stdio::Pipe,
            stderr: process_support::Stdio::Pipe,
        },
    )?;
    proc.kill()?;
    println!("Waiting for {}", proc.id());
    let sub = proc.subscribe_wait();
    sub.block();
    println!("Done waiting for {}", proc.id());
    let exit_status = proc.wait()?;
    ensure!(exit_status.is_none());
    Ok(())
}

async fn exec_sleep() -> Result<u32, anyhow::Error> {
    // Spawn bash that spawns sleep, detached from bash. If process groups are used, the sleep process should be killed.
    let proc = process_support::spawn(
        "bash",
        &process_support::SpawnOptions {
            args: vec![
                "-c".to_string(),
                "nohup sleep 100 > /dev/null 2>&1 & echo -n $!".to_string(),
            ],
            environment: environment()?,
            current_working_directory: None,
            stdin: process_support::Stdio::Pipe,
            stdout: process_support::Stdio::Pipe,
            stderr: process_support::Stdio::Pipe,
        },
    )?;

    let child_stdout = proc
        .take_stdout()
        .expect("first `take_stdout` must succeed");

    let sleep_pid = stream_to_string(child_stdout).await?;
    println!("Got pid: {sleep_pid}");
    Ok(sleep_pid.parse()?)
}

async fn subscribe_wait() -> Result<(), anyhow::Error> {
    const SCRIPT: &str = r"
i=0
while true; do
  echo $i
  ((i++))
  sleep 0.1
done
    ";
    let proc = process_support::spawn(
        "bash",
        &process_support::SpawnOptions {
            args: vec!["-c".to_string(), SCRIPT.to_string()],
            environment: environment()?,
            current_working_directory: None,
            stdin: process_support::Stdio::Discard,
            stdout: process_support::Stdio::Pipe,
            stderr: process_support::Stdio::Pipe,
        },
    )?;
    let child_stdout = AsyncInputStream::new(
        proc.take_stdout()
            .expect("first `take_stdout` must succeed"),
    );
    let current_stdout = AsyncOutputStream::new(wstd::wasip2::cli::stdout::get_stdout());

    let child_stderr = AsyncInputStream::new(
        proc.take_stderr()
            .expect("first `take_stderr` must succeed"),
    );
    let current_stderr = AsyncOutputStream::new(wstd::wasip2::cli::stderr::get_stderr());
    println!("Waiting");

    let stdout_fut = wstd::io::copy(child_stdout, current_stdout);
    let stderr_fut = wstd::io::copy(child_stderr, current_stderr);
    let wait_fut = AsyncPollable::new(proc.subscribe_wait()).wait_for();
    let kill_fut = async {
        wstd::task::sleep(Duration::from_millis(500).into()).await;
        println!("Killing");
        proc.kill()?;
        Ok::<_, anyhow::Error>(())
    };

    let (stdout, stderr, _wait, _kill) = (stdout_fut, stderr_fut, wait_fut, kill_fut).join().await;
    let exit_code = proc.wait()?;
    println!("Done waiting for {} - {exit_code:?}", proc.id());
    stdout?;
    stderr?;
    Ok(())
}

fn environment() -> Result<Vec<(String, String)>, anyhow::Error> {
    let path = std::env::var("PATH").context("`PATH` not found")?;
    Ok(vec![("PATH".to_string(), path)])
}

impl Guest for Component {
    fn touch() -> Result<(), String> {
        self::touch().map_err(|err| err.to_string())
    }

    fn stdio() -> Result<(), String> {
        block_on(async move { self::stdio().await.map_err(|err| err.to_string()) })
    }

    fn kill() -> Result<(), String> {
        self::kill().map_err(|err| err.to_string())
    }

    fn exec_sleep() -> Result<u32, String> {
        block_on(async move { self::exec_sleep().await.map_err(|err| err.to_string()) })
    }

    fn subscribe_wait() -> Result<(), String> {
        block_on(async move { self::subscribe_wait().await.map_err(|err| err.to_string()) })
    }
}
