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

/// Read up to `limit` bytes from `reader`, returning the bytes read.
/// If the stream exceeds `limit`, returns the bytes read so far + a flag.
async fn read_limited(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    limit: u64,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut buf = Vec::with_capacity(limit.min(8192) as usize);
    let mut limited = reader.take(limit + 1);
    limited.read_to_end(&mut buf).await?;
    if buf.len() as u64 > limit {
        buf.truncate(
            usize::try_from(limit)
                .expect("u64 must fit in usize - 32-bit platforms are not supported"),
        );
        Ok((buf, true))
    } else {
        Ok((buf, false))
    }
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

        // 4. Clean environment + configured env vars.
        cmd.env_clear();
        for env_var in self.env_vars.iter() {
            cmd.env(&env_var.key, &env_var.val);
        }

        // 5. Set cwd if configured.
        if let Some(cwd) = &self.cwd {
            cmd.current_dir(cwd);
        }

        // 6. Process group and kill_on_drop.
        #[cfg(unix)]
        cmd.process_group(0);
        cmd.kill_on_drop(true);

        // 7. Capture stdout/stderr, optionally pipe stdin.
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        if self.stdin_content.is_some() {
            cmd.stdin(std::process::Stdio::piped());
        }

        // 8. Spawn the child process.
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

        // 8b. Write stdin content if configured (e.g. resolved secrets).
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

        // 9. Register cancellation token.
        let cancel_token = self
            .cancel_registry
            .obtain_cancellation_token(ctx.execution_id.clone());

        // 10. Read stdout/stderr concurrently with cancellation support.
        let max_bytes = self.max_output_bytes;
        let result = tokio::select! {
            biased;
            _ = cancel_token => {
                // Kill the child on cancellation.
                let _ = child.kill().await;
                return Err(WorkerError::FatalError(
                    FatalError::CannotInstantiate {
                        reason: "execution cancelled".to_string(),
                        detail: None,
                    },
                    version,
                ));
            }
            result = async {
                let stdout_fut = read_limited(&mut child_stdout, max_bytes);
                let stderr_fut = read_limited(&mut child_stderr, max_bytes);
                let (stdout_result, stderr_result) = tokio::join!(stdout_fut, stderr_fut);
                let (stdout_bytes, stdout_exceeded) = stdout_result?;
                let (stderr_bytes, stderr_exceeded) = stderr_result?;
                let status = child.wait().await?;
                Ok::<_, std::io::Error>((stdout_bytes, stdout_exceeded, stderr_bytes, stderr_exceeded, status))
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

        let (stdout_bytes, stdout_exceeded, stderr_bytes, stderr_exceeded, status) = result;

        // 11. Check output size limits.
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
        if stderr_exceeded {
            return Err(WorkerError::FatalError(
                FatalError::CannotInstantiate {
                    reason: format!(
                        "stderr exceeded max_output_bytes limit of {} bytes",
                        self.max_output_bytes
                    ),
                    detail: None,
                },
                version,
            ));
        }

        let stdout_str = String::from_utf8_lossy(&stdout_bytes);
        let stderr_str = String::from_utf8_lossy(&stderr_bytes);

        // 12. Forward stdout/stderr per config.
        if let Some(ref config) = self.forward_stdout {
            forward_output(config, &stdout_str, &ctx);
        }
        if let Some(ref config) = self.forward_stderr {
            forward_output(config, &stderr_str, &ctx);
        }

        debug!(
            exit_code = status.code(),
            stdout_len = stdout_bytes.len(),
            stderr_len = stderr_bytes.len(),
            "Child process finished"
        );

        // 13. Map exit code + stdout to result.
        let exit_code = status.code().unwrap_or(-1);
        if exit_code == 0 {
            // Ok path: stdout is the ok-variant JSON.
            let stdout_trimmed = stdout_str.trim();
            if stdout_trimmed.is_empty() {
                // Empty stdout → JSON null (for void ok types like `result`)
                let ok_val = serde_json::Value::Null;
                let retval = crate::js_worker_utils::map_js_ok_to_user_retval(
                    &ok_val,
                    &self.user_return_type,
                    version.clone(),
                )?;
                Ok(WorkerResultOk::RunFinished {
                    retval,
                    version,
                    http_client_traces: None,
                })
            } else {
                let ok_val: serde_json::Value = serde_json::from_str(stdout_trimmed)
                    .map_err(|e| {
                        WorkerError::FatalError(
                            FatalError::ResultParsingError(
                                concepts::ResultParsingError::ResultParsingErrorFromVal(
                                    concepts::ResultParsingErrorFromVal::TypeCheckError(format!(
                                        "failed to parse stdout as JSON on exit 0: {e}, stdout: `{stdout_trimmed}`"
                                    )),
                                ),
                            ),
                            version.clone(),
                        )
                    })?;
                let retval = crate::js_worker_utils::map_js_ok_to_user_retval(
                    &ok_val,
                    &self.user_return_type,
                    version.clone(),
                )?;
                Ok(WorkerResultOk::RunFinished {
                    retval,
                    version,
                    http_client_traces: None,
                })
            }
        } else {
            // Err path: stdout is the err-variant JSON.
            let stdout_trimmed = stdout_str.trim();
            if stdout_trimmed.is_empty() {
                // Empty stdout on non-zero exit → JSON null for err
                let retval = crate::js_worker_utils::map_js_throw_to_user_err(
                    "null",
                    &self.user_return_type,
                    version.clone(),
                )?;
                Ok(WorkerResultOk::RunFinished {
                    retval,
                    version,
                    http_client_traces: None,
                })
            } else {
                // stdout should already be valid JSON; pass it through as-is to map_js_throw_to_user_err
                // which expects the raw JSON string.
                let retval = crate::js_worker_utils::map_js_throw_to_user_err(
                    stdout_trimmed,
                    &self.user_return_type,
                    version.clone(),
                )?;
                Ok(WorkerResultOk::RunFinished {
                    retval,
                    version,
                    http_client_traces: None,
                })
            }
        }
    }
}

fn forward_output(config: &StdOutputConfigWithSender, output: &str, ctx: &WorkerContext) {
    if output.is_empty() {
        return;
    }
    match config {
        StdOutputConfigWithSender::Stdout => {
            print!("{output}");
        }
        StdOutputConfigWithSender::Stderr => {
            eprint!("{output}");
        }
        StdOutputConfigWithSender::Db {
            sender,
            forwarding_from,
        } => {
            let log_entry = concepts::storage::LogEntry::Stream {
                created_at: chrono::Utc::now(),
                payload: output.as_bytes().to_vec(),
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
