//! Exec activity worker that spawns native processes via `tokio::process::Command`.
//!
//! The child process communicates its result via exit code and stdout:
//! - Exit 0: stdout contains the ok-variant JSON matching `return_type`.
//! - Exit non-zero: stdout contains the err-variant JSON matching `return_type`.

use super::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use crate::envvar::EnvVar;
use crate::std_output_stream::{StdOutputConfig, StdOutputConfigWithSender};
use async_trait::async_trait;
use concepts::storage::LogInfoAppendRow;
use concepts::{
    ComponentType, FunctionFqn, FunctionMetadata, PackageIfcFns, ParameterType,
    ReturnTypeExtendable,
};
use executor::worker::{
    FatalError, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use secrecy::{ExposeSecret, SecretString};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};
use utils::wasm_tools::WasmComponent;

/// Configuration for an exec activity program.
#[derive(Debug, Clone)]
pub enum ExecProgram {
    /// Explicit argv. First element is executable, rest are fixed args.
    External(Vec<String>),
    /// Inline script content. Written to a temp file at each execution.
    Inline(String),
}

/// Compiled exec activity. No WASM engine needed.
pub struct ActivityExecWorkerCompiled {
    program: ExecProgram,
    user_ffqn: FunctionFqn,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    env_vars: Arc<[EnvVar]>,
    cwd: Option<String>,
    max_output_bytes: u64,
    forward_stdout: Option<StdOutputConfig>,
    forward_stderr: Option<StdOutputConfig>,
    /// Pre-computed stdin content from resolved secrets. Written to the child's stdin pipe.
    stdin_content: Option<SecretString>,
    user_wasm_component: WasmComponent,
}

impl ActivityExecWorkerCompiled {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        program: ExecProgram,
        user_ffqn: FunctionFqn,
        user_params: Vec<ParameterType>,
        user_return_type: ReturnTypeExtendable,
        env_vars: Arc<[EnvVar]>,
        cwd: Option<String>,
        max_output_bytes: u64,
        forward_stdout: Option<StdOutputConfig>,
        forward_stderr: Option<StdOutputConfig>,
        stdin_content: Option<SecretString>,
    ) -> Result<Self, utils::wasm_tools::DecodeError> {
        let user_wasm_component = WasmComponent::new_from_fn_signature(
            &user_ffqn,
            &user_params,
            &user_return_type,
            ComponentType::Activity,
            "exec-activity",
        )?;
        Ok(Self {
            program,
            user_ffqn,
            user_params,
            user_return_type,
            env_vars,
            cwd,
            max_output_bytes,
            forward_stdout,
            forward_stderr,
            stdin_content,
            user_wasm_component,
        })
    }

    #[must_use]
    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        self.user_wasm_component.exported_functions(true)
    }

    #[must_use]
    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        self.user_wasm_component.exports_hierarchy_ext()
    }

    #[must_use]
    pub fn wit(&self) -> String {
        self.user_wasm_component.wit()
    }

    #[must_use]
    pub fn into_worker(
        self,
        cancel_registry: CancelRegistry,
        log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
        _logs_storage_config: Option<LogStrageConfig>,
    ) -> ActivityExecWorker {
        let stdout_config = StdOutputConfigWithSender::new(
            self.forward_stdout,
            log_forwarder_sender,
            concepts::storage::LogStreamType::StdOut,
        );
        let stderr_config = StdOutputConfigWithSender::new(
            self.forward_stderr,
            log_forwarder_sender,
            concepts::storage::LogStreamType::StdErr,
        );
        ActivityExecWorker {
            program: self.program,
            user_ffqn: self.user_ffqn,
            user_params: self.user_params,
            user_return_type: self.user_return_type,
            env_vars: self.env_vars,
            cwd: self.cwd,
            max_output_bytes: self.max_output_bytes,
            forward_stdout: stdout_config,
            forward_stderr: stderr_config,
            stdin_content: self.stdin_content,
            cancel_registry,
            user_exports_noext: self.user_wasm_component.exported_functions(false).to_vec(),
        }
    }
}

pub struct ActivityExecWorker {
    program: ExecProgram,
    #[allow(dead_code)]
    user_ffqn: FunctionFqn,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    env_vars: Arc<[EnvVar]>,
    cwd: Option<String>,
    max_output_bytes: u64,
    forward_stdout: Option<StdOutputConfigWithSender>,
    forward_stderr: Option<StdOutputConfigWithSender>,
    stdin_content: Option<SecretString>,
    cancel_registry: CancelRegistry,
    user_exports_noext: Vec<FunctionMetadata>,
}

/// Read from `reader` in chunks, streaming each chunk to `forwarder`,
/// while accumulating the full output (up to `capture_limit` bytes).
/// Capturing can be turned off by setting `capture_limit` to zero.
async fn read_and_stream(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    capture_limit: u64,
    forwarder: Option<&StdOutputConfigWithSender>,
    ctx: &WorkerContext,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut buf = Vec::with_capacity(capture_limit.min(8192) as usize);
    let mut chunk = [0u8; 4096];
    let mut exceeded = false;
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        // Forward to log storage.
        if let Some(fwd) = forwarder {
            forward_output(fwd, &chunk[..n], ctx);
        }
        // Accumulate for result capture unless turned off.
        if !exceeded && capture_limit > 0 {
            let space = (capture_limit as usize).saturating_sub(buf.len());
            if space > 0 {
                let to_capture = n.min(space);
                buf.extend_from_slice(&chunk[..to_capture]);
            }
            if buf.len() as u64 >= capture_limit && n > space {
                exceeded = true;
            }
        }
    }
    if capture_limit == 0 {
        assert!(!exceeded);
    }
    Ok((buf, exceeded))
}

#[async_trait]
impl Worker for ActivityExecWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        let version = ctx.version.clone();

        let mut param_args: Vec<String> = Vec::new();
        let _temp_file_guard: Option<tempfile::TempPath>;
        let mut cmd = match &self.program {
            ExecProgram::External(argv) => {
                let Some((cmd, specified_args)) = argv.split_first() else {
                    return Err(WorkerError::FatalError(
                        FatalError::CannotInstantiate {
                            reason: "external program argv is empty".to_string(),
                            detail: None,
                        },
                        version,
                    ));
                };
                param_args.extend_from_slice(specified_args); // Execution args come afterwards.
                tokio::process::Command::new(cmd)
            }
            ExecProgram::Inline(script) => {
                // Write script to temp file preserving shebang.
                let mut tmp = tempfile::Builder::new()
                    .prefix("obelisk-exec-")
                    .tempfile()
                    .map_err(|e| {
                        WorkerError::FatalError(
                            FatalError::CannotInstantiate {
                                reason: "failed to create temp file for inline script".to_string(),
                                detail: Some(e.to_string()),
                            },
                            version.clone(),
                        )
                    })?;
                use std::io::Write;
                tmp.write_all(script.as_bytes()).map_err(|e| {
                    WorkerError::FatalError(
                        FatalError::CannotInstantiate {
                            reason: "failed to write inline script".to_string(),
                            detail: Some(e.to_string()),
                        },
                        version.clone(),
                    )
                })?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    tmp.as_file()
                        .set_permissions(std::fs::Permissions::from_mode(0o755))
                        .map_err(|e| {
                            WorkerError::FatalError(
                                FatalError::CannotInstantiate {
                                    reason: "failed to make temp file executable".to_string(),
                                    detail: Some(e.to_string()),
                                },
                                version.clone(),
                            )
                        })?;
                }
                let temp_path = tmp.into_temp_path(); // Close the file handle.
                let cmd = tokio::process::Command::new(&temp_path);
                _temp_file_guard = Some(temp_path);
                cmd
            }
        };

        {
            // Serialize each user parameter as a JSON string for command-line args.
            let json_params = ctx
                .params
                .as_json_values()
                .expect("params come from database, not wasmtime");
            assert_eq!(
                self.user_params.len(),
                json_params.len(),
                "type checked in Params::from_json_values"
            );
            param_args.extend(json_params.iter().map(|v| {
                serde_json::to_string(v).expect("serde_json::Value must be serializable")
            }));
        }
        cmd.args(param_args);

        // Clean environment + configured env vars.
        cmd.env_clear();
        for env_var in self.env_vars.iter() {
            cmd.env(&env_var.key, &env_var.val);
        }

        // Set cwd if configured.
        if let Some(cwd) = &self.cwd {
            cmd.current_dir(cwd);
        }

        // Process group and kill_on_drop.
        #[cfg(unix)]
        cmd.process_group(0);
        cmd.kill_on_drop(true);

        // Capture stdout/stderr, optionally pipe stdin.
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        if self.stdin_content.is_some() {
            cmd.stdin(std::process::Stdio::piped());
        }

        // Spawn the child process.
        trace!("Spawning {cmd:?}");
        let mut child = cmd.spawn().map_err(|e| {
            WorkerError::FatalError(
                FatalError::CannotInstantiate {
                    reason: "failed to spawn child process".to_string(),
                    detail: Some(e.to_string()),
                },
                version.clone(),
            )
        })?;

        // Write stdin content if configured (e.g. resolved secrets).
        if let Some(ref stdin_content) = self.stdin_content {
            use tokio::io::AsyncWriteExt;
            let mut child_stdin = child.stdin.take().expect("stdin was piped");
            child_stdin
                .write_all(stdin_content.expose_secret().as_bytes())
                .await
                .map_err(|e| {
                    WorkerError::FatalError(
                        FatalError::CannotInstantiate {
                            reason: "failed to write to child stdin".to_string(),
                            detail: Some(e.to_string()),
                        },
                        version.clone(),
                    )
                })?;
            // Drop stdin to signal EOF so the child can proceed.
            drop(child_stdin);
        }

        let mut child_stdout = child.stdout.take().expect("stdout was piped");
        let mut child_stderr = child.stderr.take().expect("stderr was piped");

        // Register cancellation token.
        let cancel_token = self
            .cancel_registry
            .obtain_cancellation_token(ctx.execution_id.clone());

        // Skip stdout collection when return_type is `result` (unit ok and err variants).
        let max_stdout_bytes = if self.user_return_type.type_wrapper_tl.is_result_of_units() {
            0
        } else {
            self.max_output_bytes
        };
        let result = tokio::select! {
            biased;
            _ = cancel_token => {
                // Kill the child on cancellation.
                let _ = child.kill().await;
                return Err(WorkerError::FatalError(
                    FatalError::Cancelled,
                    version,
                ));
            }
            result = async {
                // Read stdout/stderr concurrently, streaming to log forwarder as chunks arrive.
                let stdout_fut = read_and_stream(
                    &mut child_stdout,
                    max_stdout_bytes,
                    self.forward_stdout.as_ref(),
                    &ctx,
                );
                let stderr_fut = read_and_stream(
                    &mut child_stderr,
                    0, // stderr is only streamed to logs, not captured
                    self.forward_stderr.as_ref(),
                    &ctx,
                );
                let (stdout_result, stderr_result) = tokio::join!(stdout_fut, stderr_fut);
                let (mut stdout_bytes, mut stdout_exceeded) = stdout_result?;
                let _ = stderr_result?;
                let exit_code = child.wait().await?.code().unwrap_or(-1);
                // If the unit type was requested, return empty response.
                if exit_code == 0 && self.user_return_type.type_wrapper_tl.ok.is_none()
                    || exit_code != 0 && self.user_return_type.type_wrapper_tl.err.is_none()
                {
                    stdout_exceeded = false;
                    stdout_bytes = Vec::new();
                }
                Ok::<_, std::io::Error>((stdout_bytes, stdout_exceeded, exit_code))
            } => {
                result.map_err(|e| {
                    WorkerError::FatalError(
                        FatalError::CannotInstantiate {
                            reason: "I/O error during child process execution".to_string(),
                            detail: Some(e.to_string()),
                        },
                        version.clone(),
                    )
                })?
            }
        };

        let (stdout_bytes, stdout_exceeded, exit_code) = result;

        // Check output size limit.
        if stdout_exceeded {
            return Err(WorkerError::FatalError(
                FatalError::CannotInstantiate {
                    reason: format!(
                        "stdout exceeded max_output_bytes limit of {} bytes",
                        self.max_output_bytes
                    ),
                    detail: None,
                },
                version,
            ));
        }

        debug!(
            exit_code,
            stdout_len = stdout_bytes.len(),
            "Child process finished"
        );
        let stdout = String::from_utf8_lossy(&stdout_bytes);
        let parsed = if stdout.trim().is_empty() {
            None
        } else {
            Some(serde_json::from_str::<serde_json::Value>(&stdout).map_err(|e| {
                WorkerError::FatalError(
                    FatalError::ResultParsingError(
                        concepts::ResultParsingError::ResultParsingErrorFromVal(
                            concepts::ResultParsingErrorFromVal::TypeCheckError(format!(
                                "failed to parse stdout as JSON on exit {exit_code}: {e}, stdout: `{stdout}`"
                            )),
                        ),
                    ),
                    version.clone(),
                )
            })?)
        };

        let retval = if exit_code == 0 {
            crate::js_worker_utils::map_ok_variant(parsed, &self.user_return_type, version.clone())?
        } else {
            crate::js_worker_utils::map_err_variant(
                parsed,
                &self.user_return_type,
                version.clone(),
            )?
        };
        Ok(WorkerResultOk::RunFinished {
            retval,
            version,
            http_client_traces: None,
        })
    }
}

fn forward_output(config: &StdOutputConfigWithSender, output: &[u8], ctx: &WorkerContext) {
    if output.is_empty() {
        return;
    }
    match config {
        StdOutputConfigWithSender::Stdout => {
            use std::io::Write;
            let _ = std::io::stdout().write_all(output);
        }
        StdOutputConfigWithSender::Stderr => {
            use std::io::Write;
            let _ = std::io::stderr().write_all(output);
        }
        StdOutputConfigWithSender::Db {
            sender,
            forwarding_from,
        } => {
            let log_entry = concepts::storage::LogEntry::Stream {
                created_at: chrono::Utc::now(),
                payload: output.to_vec(),
                stream_type: *forwarding_from,
            };
            let row = LogInfoAppendRow {
                execution_id: ctx.execution_id.clone(),
                run_id: ctx.locked_event.run_id,
                log_entry,
            };
            if let Err(err) = sender.try_send(row) {
                warn!("Failed to forward output to DB: {err}");
            }
        }
    }
}
