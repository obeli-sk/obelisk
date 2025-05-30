use super::{
    activity_ctx::ActivityCtx,
    activity_host_exports::process_support_outer::v1_0_0::obelisk::activity::process_support,
};
use crate::activity::activity_host_exports::process_support_outer::HostChildProcess;
use concepts::time::ClockFn;
use std::{
    path::PathBuf,
    process::Stdio as StdProcessStdio,
    sync::{Arc, Mutex},
};
use tracing::{debug, info, warn};
use wasmtime::component::Resource;

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

        let mut cmd = std::process::Command::new(&command);
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
            Ok(mut child_handle) => {
                let child_stdin = match options.stdin {
                    process_support::Stdio::Pipe => child_handle.stdin.take(),
                    process_support::Stdio::Discard => None,
                };
                let child_stdout = match options.stdout {
                    process_support::Stdio::Pipe => child_handle.stdout.take(),
                    process_support::Stdio::Discard => None,
                };
                let child_stderr = match options.stderr {
                    process_support::Stdio::Pipe => child_handle.stderr.take(),
                    process_support::Stdio::Discard => None,
                };

                let child_process = HostChildProcess {
                    id: u64::from(child_handle.id()),
                    child: Arc::new(Mutex::new(child_handle)),
                    command_str: command.clone(),
                    stdin: child_stdin,
                    stdout: child_stdout,
                    stderr: child_stderr,
                };
                info!("Spawned {child_process:?}");

                match self.table.push(child_process) {
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

impl<C: ClockFn> process_support::HostChildProcess for ActivityCtx<C> {
    async fn id(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<u64> {
        let child_process = self.table.get(&self_handle)?;
        Ok(child_process.id)
    }

    async fn wait(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<Result<Option<i32>, process_support::ProcessError>> {
        let child_process = self.table.get(&self_handle)?;
        let child_arc = Arc::clone(&child_process.child); // Clone Arc to move into spawn_blocking

        #[cfg_attr(madsim, allow(deprecated))]
        let wait_result =
            tokio::task::spawn_blocking(move || child_arc.lock().unwrap().wait()).await;

        match wait_result {
            Ok(Ok(exit_status)) => {
                // spawn_blocking Ok, wait Ok
                Ok(Ok(exit_status.code()))
            }
            Ok(Err(wait_err)) => {
                // spawn_blocking Ok, wait Err
                warn!("Host error: Waiting on process {child_process:?} failed: {wait_err:?}");
                Ok(Err(process_support::ProcessError::OsError))
            }
            Err(spawn_blocking_err) => {
                warn!(
                    "Host error: Tokio task join error for process wait {child_process:?}: {spawn_blocking_err:?}"
                );
                Ok(Err(process_support::ProcessError::InternalError))
            }
        }
    }

    async fn kill(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<Result<(), process_support::ProcessError>> {
        let child_process = self.table.get(&self_handle)?;
        let mut child_guard = child_process.child.lock().unwrap();

        match child_guard.kill() {
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
        let _child_process = self.table.delete(self_handle)?;
        Ok(())
    }
}
