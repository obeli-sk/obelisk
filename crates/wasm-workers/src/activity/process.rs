use crate::activity::activity_ctx_process::process_support_outer::v1_0_0::obelisk::activity::process as process_support;
use std::path::{Path, PathBuf};
use std::process::Stdio as StdProcessStdio;
use tracing::{debug, info, warn};
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream, StdinStream as _, StdoutStream as _};

#[derive(derive_more::Debug)]
pub enum HostChildProcess {
    Native {
        id: u64,
        #[expect(dead_code)]
        command: String, // For logging/debugging purposes
        #[debug(skip)]
        child: tokio::process::Child,
        // Store the handles for piped streams before they are converted and taken.
        #[debug(skip)]
        stdin: Option<tokio::process::ChildStdin>,
        #[debug(skip)]
        stdout: Option<tokio::process::ChildStdout>,
        #[debug(skip)]
        stderr: Option<tokio::process::ChildStderr>,
    },
}

impl HostChildProcess {
    pub(crate) fn spawn_native(
        command: String,
        options: &process_support::SpawnOptions,
        preopened_dir: &Path,
    ) -> Result<process_support::ChildProcess, process_support::SpawnError> {
        debug!(
            "Host attempting to spawn: command='{}', args={:?}, env_vars_count={}, cwd={:?}",
            command,
            options.args,
            options.environment.len(),
            options.current_working_directory
        );

        let mut cmd = tokio::process::Command::new(&command);
        cmd.args(&options.args);

        // Start with clean env, then add specified vars.
        cmd.env_clear();
        for (key, value) in &options.environment {
            cmd.env(key, value);
        }

        // Current Working Directory - Use what guest specifies or fallback to the preopened directory.
        if let Some(cwd_str) = &options.current_working_directory {
            let cwd_path = PathBuf::from(cwd_str);
            if !cwd_path.is_dir() {
                return Err(process_support::SpawnError::CwdNotFound);
            }
            cmd.current_dir(cwd_path);
        } else {
            cmd.current_dir(preopened_dir);
        }

        // Stdio Handling
        cmd.stdin(match options.stdin {
            process_support::Stdio::Pipe => StdProcessStdio::piped(),
            process_support::Stdio::Discard => StdProcessStdio::null(),
        });
        cmd.stdout(match options.stdout {
            process_support::Stdio::Pipe => StdProcessStdio::piped(),
            process_support::Stdio::Discard => StdProcessStdio::null(),
        });
        cmd.stderr(match options.stderr {
            process_support::Stdio::Pipe => StdProcessStdio::piped(),
            process_support::Stdio::Discard => StdProcessStdio::null(),
        });

        match cmd.spawn() {
            Ok(mut child) => {
                let child_stdin = match options.stdin {
                    process_support::Stdio::Pipe => child.stdin.take(),
                    process_support::Stdio::Discard => None,
                };
                let child_stdout = match options.stdout {
                    process_support::Stdio::Pipe => child.stdout.take(),
                    process_support::Stdio::Discard => None,
                };
                let child_stderr = match options.stderr {
                    process_support::Stdio::Pipe => child.stderr.take(),
                    process_support::Stdio::Discard => None,
                };

                let child_process = HostChildProcess::Native {
                    id: u64::from(child.id().expect("child has not been polled to completion")),
                    child,
                    command,
                    stdin: child_stdin,
                    stdout: child_stdout,
                    stderr: child_stderr,
                };
                info!("Spawned {child_process:?}");
                Ok(child_process)
            }
            Err(err) => {
                warn!("Host error: Failed to spawn process '{command}': {err:?}");
                let spawn_error = match err.kind() {
                    std::io::ErrorKind::NotFound => process_support::SpawnError::CommandNotFound,
                    std::io::ErrorKind::PermissionDenied => {
                        process_support::SpawnError::PermissionDenied
                    }
                    _ => process_support::SpawnError::GenericError,
                };
                Err(spawn_error)
            }
        }
    }

    pub(crate) fn id(&self) -> u64 {
        match self {
            Self::Native { id, .. } => *id,
        }
    }

    pub(crate) fn take_stdin(&mut self) -> Option<DynOutputStream> {
        match self {
            Self::Native { stdin, .. } => stdin.take().map(|stream| {
                wasmtime_wasi::p2::AsyncStdoutStream::new(
                    wasmtime_wasi::p2::pipe::AsyncWriteStream::new(1024, stream),
                )
                .stream()
            }),
        }
    }

    pub(crate) fn take_stdout(&mut self) -> Option<DynInputStream> {
        match self {
            Self::Native { stdout, .. } => stdout.take().map(|stream| {
                wasmtime_wasi::p2::AsyncStdinStream::new(
                    wasmtime_wasi::p2::pipe::AsyncReadStream::new(stream),
                )
                .stream()
            }),
        }
    }

    pub(crate) fn take_stderr(&mut self) -> Option<DynInputStream> {
        match self {
            Self::Native { stderr, .. } => stderr.take().map(|stream| {
                wasmtime_wasi::p2::AsyncStdinStream::new(
                    wasmtime_wasi::p2::pipe::AsyncReadStream::new(stream),
                )
                .stream()
            }),
        }
    }

    pub(crate) async fn wait(&mut self) -> Result<Option<i32>, process_support::ProcessError> {
        match self {
            Self::Native { child, .. } => {
                #[cfg_attr(madsim, allow(deprecated))]
                let wait_result = child.wait().await;
                match wait_result {
                    Ok(exit_status) => Ok(exit_status.code()),
                    Err(wait_err) => {
                        warn!("Host error: Waiting on process {self:?} failed: {wait_err:?}");
                        Err(process_support::ProcessError::OsError)
                    }
                }
            }
        }
    }

    pub(crate) async fn kill(&mut self) -> Result<(), process_support::ProcessError> {
        match self {
            Self::Native { child, .. } => match child.kill().await {
                Ok(()) => Ok(()),
                Err(err) => {
                    warn!(
                        "Host error: Killing process {self:?}: kind: {kind},  {err:?}",
                        kind = err.kind()
                    );
                    let process_error = match err.kind() {
                        std::io::ErrorKind::PermissionDenied => {
                            process_support::ProcessError::PermissionDenied
                        }
                        _ => process_support::ProcessError::OsError,
                    };
                    Err(process_error)
                }
            },
        }
    }
}
