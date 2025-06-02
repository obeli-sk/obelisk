use super::{
    activity_ctx::ActivityCtx,
    activity_host_exports::process_support_outer::v1_0_0::obelisk::activity::process_support,
};
use crate::activity::activity_host_exports::process_support_outer::HostChildProcess;
use concepts::time::ClockFn;
use std::{path::PathBuf, process::Stdio as StdProcessStdio};
use tracing::{debug, info, warn};
use wasmtime::component::Resource;
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream, StdinStream, StdoutStream};
use wasmtime_wasi_io::IoView as _;

impl<C: ClockFn> process_support::Host for ActivityCtx<C> {
    async fn spawn(
        &mut self,
        command: String,
        options: process_support::SpawnOptions,
    ) -> wasmtime::Result<
        Result<Resource<process_support::ChildProcess>, process_support::SpawnError>,
    > {
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
                return Ok(Err(process_support::SpawnError::CwdNotFound));
            }
            cmd.current_dir(cwd_path);
        } else {
            cmd.current_dir(
                self.preopened_dir
                    .as_deref()
                    .expect("process api must only be linked if preopened dir is enabled"),
            );
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
                    id: u64::from(child.id().expect("child has not been polled to completion")),
                    child,
                    command_str: command.clone(),
                    stdin: child_stdin,
                    stdout: child_stdout,
                    stderr: child_stderr,
                };
                info!("Spawned {child_process:?}");

                match self.table().push(child_process) {
                    Ok(resource) => Ok(Ok(resource)),
                    Err(err) => {
                        warn!(
                            "Host error: Failed to push HostChildProcess to resource table: {err:?}",
                        );
                        Ok(Err(process_support::SpawnError::InternalError))
                    }
                }
            }
            Err(err) => {
                warn!("Host error: Failed to spawn process '{command}': {err:?}");
                let spawn_error = match err.kind() {
                    std::io::ErrorKind::NotFound => process_support::SpawnError::CommandNotFound,
                    std::io::ErrorKind::PermissionDenied => {
                        process_support::SpawnError::PermissionDenied
                    }
                    _ => process_support::SpawnError::InternalError,
                };
                Ok(Err(spawn_error))
            }
        }
    }
}

// Implement methods for the `child-process` resource
impl<C: ClockFn> process_support::HostChildProcess for ActivityCtx<C> {
    async fn id(&mut self, self_handle: Resource<HostChildProcess>) -> wasmtime::Result<u64> {
        let child_process = self.table().get(&self_handle)?;
        Ok(child_process.id)
    }

    async fn take_stdin(
        &mut self,
        self_handle: Resource<HostChildProcess>,
    ) -> wasmtime::Result<Option<Resource<DynOutputStream>>> {
        let host_child_process = self.table().get_mut(&self_handle)?;
        if let Some(child_stdin) = host_child_process.stdin.take() {
            let child_stdin = wasmtime_wasi::p2::AsyncStdoutStream::new(
                wasmtime_wasi::p2::pipe::AsyncWriteStream::new(1024, child_stdin),
            )
            .stream();
            Ok(Some(self.table().push(child_stdin)?))
        } else {
            Ok(None)
        }
    }

    async fn take_stdout(
        &mut self,
        self_handle: Resource<HostChildProcess>,
    ) -> wasmtime::Result<Option<Resource<DynInputStream>>> {
        let host_child_process = self.table().get_mut(&self_handle)?;
        if let Some(child_stdout) = host_child_process.stdout.take() {
            let child_stdout = wasmtime_wasi::p2::AsyncStdinStream::new(
                wasmtime_wasi::p2::pipe::AsyncReadStream::new(child_stdout),
            )
            .stream();
            Ok(Some(self.table().push(child_stdout)?))
        } else {
            Ok(None)
        }
    }

    async fn take_stderr(
        &mut self,
        self_handle: Resource<HostChildProcess>,
    ) -> wasmtime::Result<Option<Resource<DynInputStream>>> {
        let host_child_process = self.table().get_mut(&self_handle)?;
        if let Some(child_stdout) = host_child_process.stderr.take() {
            let child_stdout = wasmtime_wasi::p2::AsyncStdinStream::new(
                wasmtime_wasi::p2::pipe::AsyncReadStream::new(child_stdout),
            )
            .stream();
            Ok(Some(self.table().push(child_stdout)?))
        } else {
            Ok(None)
        }
    }

    async fn wait(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<Result<Option<i32>, process_support::ProcessError>> {
        let child_process = self.table().get_mut(&self_handle)?;

        #[cfg_attr(madsim, allow(deprecated))]
        let wait_result = child_process.child.wait().await;

        match wait_result {
            Ok(exit_status) => Ok(Ok(exit_status.code())),
            Err(wait_err) => {
                warn!("Host error: Waiting on process {child_process:?} failed: {wait_err:?}");
                Ok(Err(process_support::ProcessError::OsError))
            }
        }
    }

    async fn kill(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<Result<(), process_support::ProcessError>> {
        let child_process = self.table().get_mut(&self_handle)?;

        match child_process.child.kill().await {
            Ok(()) => Ok(Ok(())),
            Err(err) => {
                warn!(
                    "Host error: Killing process {child_process:?}: kind: {kind},  {err:?}",
                    kind = err.kind()
                );
                let process_error = match err.kind() {
                    std::io::ErrorKind::PermissionDenied => {
                        process_support::ProcessError::PermissionDenied
                    }
                    _ => process_support::ProcessError::OsError,
                };
                Ok(Err(process_error))
            }
        }
    }

    async fn drop(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<()> {
        let _child_process = self.table().delete(self_handle)?;
        Ok(())
    }
}
