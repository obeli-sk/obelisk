use anyhow::{bail, ensure};
use exports::testing::process::process::Guest;
use obelisk::activity::process_support::{SpawnOptions, Stdio};
use wstd::io::{AsyncInputStream, AsyncOutputStream, Cursor};
use wstd::runtime::block_on;

wit_bindgen::generate!({
     world: "any:any/any",
       with: {
       "wasi:io/error@0.2.3": wasi::io::error,
       "wasi:io/poll@0.2.3": wasi::io::poll,
       "wasi:io/streams@0.2.3": wasi::io::streams,
       "obelisk:activity/process-support@1.0.0": generate,
       "testing:process/process": generate,
   },
});

struct Component;
export!(Component);

fn touch() -> Result<(), anyhow::Error> {
    // Idempotently create a file so that the activity works with `reuse-on-retry`.
    let proc = obelisk::activity::process_support::spawn(
        "touch",
        &SpawnOptions {
            args: vec!["touched".to_string()],
            environment: vec![],
            current_working_directory: None,
            stdin: Stdio::Discard,
            stdout: Stdio::Discard,
            stderr: Stdio::Discard,
        },
    )?;
    println!("Waiting for {}", proc.id());
    let exit_code = proc.wait()?;
    ensure!(exit_code == Some(0));

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
    let proc = obelisk::activity::process_support::spawn(
        "bash",
        &SpawnOptions {
            args: vec![
                "-c".to_string(),
                format!("echo -n stderr >&2; read input; echo -n \"hello $input\""),
            ],
            environment: vec![],
            current_working_directory: None,
            stdin: Stdio::Pipe,
            stdout: Stdio::Pipe,
            stderr: Stdio::Pipe,
        },
    )?;
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
    let exit_code = proc.wait()?;
    ensure!(exit_code == Some(0));

    let to_str = |stream| async move {
        let mut buffer = Cursor::new(Vec::new());
        let stream = AsyncInputStream::new(stream);
        wstd::io::copy(stream, &mut buffer).await?;
        let output = buffer.into_inner();
        let output = String::from_utf8_lossy(&output).into_owned();
        Ok::<_, anyhow::Error>(output)
    };

    let stdout = to_str(stdout).await?;
    println!("Got {stdout}");
    ensure!(stdout == "hello wasi!");

    let stderr = to_str(stderr).await?;
    ensure!(stderr == "stderr");

    Ok(())
}

fn kill() -> Result<(), anyhow::Error> {
    let proc = obelisk::activity::process_support::spawn(
        "sleep",
        &SpawnOptions {
            args: vec!["10".to_string()],
            environment: vec![],
            current_working_directory: None,
            stdin: Stdio::Pipe,
            stdout: Stdio::Pipe,
            stderr: Stdio::Pipe,
        },
    )?;
    proc.kill()?;
    let status = proc.wait()?;
    println!("status: {status:?}");
    ensure!(status.is_none());
    Ok(())
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
}
