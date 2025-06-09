use super::activity_worker::ProcessProvider;
use crate::activity::activity_ctx_process::process_support_outer::v1_0_0::obelisk::activity::process as process_support;
use std::path::{Path, PathBuf};
use std::process::Stdio as StdProcessStdio;
use tracing::{debug, error, info, trace, warn};
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream, StdinStream as _, StdoutStream as _};

#[derive(derive_more::Debug)]
pub struct HostChildProcess {
    #[expect(dead_code)] // For logging/debugging purposes
    provider: ProcessProvider,
    id: u32,
    #[expect(dead_code)] // For logging/debugging purposes
    command: String,
    #[debug(skip)]
    child: tokio::process::Child,
    // Store the handles for piped streams before they are converted and taken.
    #[debug(skip)]
    stdin: Option<tokio::process::ChildStdin>,
    #[debug(skip)]
    stdout: Option<tokio::process::ChildStdout>,
    #[debug(skip)]
    stderr: Option<tokio::process::ChildStderr>,
}

impl Drop for HostChildProcess {
    fn drop(&mut self) {
        self.clean();
    }
}

impl HostChildProcess {
    pub(crate) fn spawn(
        provider: ProcessProvider,
        command: String,
        options: &process_support::SpawnOptions,
        preopened_dir: &Path,
    ) -> Result<process_support::ChildProcess, process_support::SpawnError> {
        debug!(
            "Host attempting to spawn: {provider:?} command='{command}', args={:?}, env_vars_count={}, cwd={:?}",
            options.args,
            options.environment.len(),
            options.current_working_directory
        );
        let mut cmd = match provider {
            ProcessProvider::Native => {
                let mut cmd = tokio::process::Command::new(&command);
                cmd.args(&options.args);
                #[cfg(unix)]
                cmd.process_group(0);
                cmd.kill_on_drop(true);
                cmd
            }
        };

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

                let child_process = HostChildProcess {
                    provider,
                    id: child
                        .id()
                        .ok_or_else(||{
                        error!("pid is None - should not happen since child has not been polled to completion");
                        process_support::SpawnError::GenericError
                    })?,
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

    #[cfg(unix)]
    fn clean(&self) {
        let Ok(pid) = i32::try_from(self.id) else {
            warn!(
                "Failed to send SIGTERM to process group {} - cannot cast to i32",
                self.id
            );
            return;
        };
        #[cfg_attr(madsim, allow(deprecated))]
        tokio::task::spawn_blocking(move || {
            trace!("Attempting to kill process group with PGID: {pid}");
            unsafe {
                // Send SIGTERM to the entire process group
                // The negative sign before pgid_to_kill is crucial
                let result = libc::kill(-pid, libc::SIGTERM);
                if result == 0 {
                    debug!("Successfully sent SIGTERM to process group {pid}",);
                } else {
                    let last_error = std::io::Error::last_os_error();
                    if let Some(3) = last_error.raw_os_error() {
                        trace!("Ignoring no such process error: {last_error:?}");
                    } else {
                        warn!("Failed to send SIGTERM to process group {pid}: {last_error:?}",);
                    }
                }
            }
        });
    }
    #[cfg(not(unix))]
    fn clean(&self) {}

    pub(crate) fn id(&self) -> u32 {
        self.id
    }

    pub(crate) fn take_stdin(&mut self) -> Option<DynOutputStream> {
        self.stdin.take().map(|stream| {
            wasmtime_wasi::p2::AsyncStdoutStream::new(
                wasmtime_wasi::p2::pipe::AsyncWriteStream::new(1024, stream),
            )
            .stream()
        })
    }

    pub(crate) fn take_stdout(&mut self) -> Option<DynInputStream> {
        self.stdout.take().map(|stream| {
            wasmtime_wasi::p2::AsyncStdinStream::new(wasmtime_wasi::p2::pipe::AsyncReadStream::new(
                stream,
            ))
            .stream()
        })
    }

    pub(crate) fn take_stderr(&mut self) -> Option<DynInputStream> {
        self.stderr.take().map(|stream| {
            wasmtime_wasi::p2::AsyncStdinStream::new(wasmtime_wasi::p2::pipe::AsyncReadStream::new(
                stream,
            ))
            .stream()
        })
    }

    pub(crate) fn try_wait(&mut self) -> Result<Option<i32>, process_support::WaitError> {
        #[cfg_attr(madsim, allow(deprecated))]
        let wait_result = self.child.try_wait();
        match wait_result {
            Ok(Some(exit_status)) => Ok(exit_status.code()),
            Ok(None) => Err(process_support::WaitError::WouldBlock),
            Err(wait_err) => {
                warn!("Host error: Waiting on process {self:?} failed: {wait_err:?}");
                Err(process_support::WaitError::OsError)
            }
        }
    }

    pub(crate) async fn kill(&mut self) -> Result<(), process_support::KillError> {
        match self.child.kill().await {
            Ok(()) => Ok(()),
            Err(err) => {
                warn!(
                    "Host error: Killing process {self:?}: kind: {kind},  {err:?}",
                    kind = err.kind()
                );
                Err(process_support::KillError::OsError)
            }
        }
    }
}

#[async_trait::async_trait]
impl wasmtime_wasi::p2::Pollable for HostChildProcess {
    async fn ready(&mut self) {
        let exit_status = self.child.wait().await;
        debug!("Exit status: {exit_status:?}");
    }
}
