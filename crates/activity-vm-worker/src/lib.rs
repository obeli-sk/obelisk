use activity_vm_runner::MapDir;
use async_trait::async_trait;
use concepts::{
    ComponentType, FunctionFqn, FunctionMetadata, PackageIfcFns, ParameterType,
    ReturnTypeExtendable,
};
use executor::worker::{
    FatalError, RunFinished, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use secrecy::ExposeSecret as _;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use utils::wasm_tools::WasmComponent;
use wasm_workers::activity::cancel_registry::CancelRegistry;
use wasm_workers::std_output_stream::{StdOutputConfig, StdOutputConfigWithSender};
use wasmtime::{Engine, Module};
use worker_common::{ExecSecrets, ProcessHttpPolicySpec, SecretResolver};

pub struct ActivityVmWorkerCompiled {
    module: Module,
    engine: Arc<Engine>,
    mapdirs: Vec<MapDir>,
    qemu_runtime: Option<activity_vm_runner::QemuRuntimeConfig>,
    guest_args: Vec<String>,
    policy_spec: ProcessHttpPolicySpec,
    secrets: Option<ExecSecrets>,
    exposed_secrets: Vec<String>,
    params_via_stdin: bool,
    env: HashMap<String, String>,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    max_output_bytes: u64,
    forward_stdout: Option<StdOutputConfig>,
    forward_stderr: Option<StdOutputConfig>,
    user_wasm_component: WasmComponent,
}

impl ActivityVmWorkerCompiled {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        module: Module,
        engine: Arc<Engine>,
        mapdirs: Vec<MapDir>,
        qemu_runtime: Option<activity_vm_runner::QemuRuntimeConfig>,
        guest_args: Vec<String>,
        policy_spec: ProcessHttpPolicySpec,
        secrets: Option<ExecSecrets>,
        exposed_secrets: Vec<String>,
        params_via_stdin: bool,
        env: HashMap<String, String>,
        ffqn: &FunctionFqn,
        user_params: Vec<ParameterType>,
        user_return_type: ReturnTypeExtendable,
        max_output_bytes: u64,
        forward_stdout: Option<StdOutputConfig>,
        forward_stderr: Option<StdOutputConfig>,
        authored_component: Option<WasmComponent>,
    ) -> anyhow::Result<Self> {
        let user_wasm_component = match authored_component {
            Some(component) => component,
            None => WasmComponent::new_from_fn_signature(
                ffqn,
                &user_params,
                &user_return_type,
                ComponentType::Activity,
                "vm-activity",
            )?,
        };
        Ok(Self {
            module,
            engine,
            mapdirs,
            qemu_runtime,
            guest_args,
            policy_spec,
            secrets,
            exposed_secrets,
            params_via_stdin,
            env,
            user_params,
            user_return_type,
            max_output_bytes,
            forward_stdout,
            forward_stderr,
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
        log_forwarder_sender: &tokio::sync::mpsc::Sender<concepts::storage::LogInfoAppendRow>,
    ) -> ActivityVmWorker {
        ActivityVmWorker {
            module: self.module,
            engine: self.engine,
            mapdirs: self.mapdirs,
            qemu_runtime: self.qemu_runtime,
            guest_args: self.guest_args,
            policy_spec: self.policy_spec,
            secrets: self.secrets,
            exposed_secrets: self.exposed_secrets,
            params_via_stdin: self.params_via_stdin,
            env: self.env,
            user_params: self.user_params,
            user_return_type: self.user_return_type,
            max_output_bytes: self.max_output_bytes,
            forward_stdout: StdOutputConfigWithSender::new(
                self.forward_stdout,
                log_forwarder_sender,
                concepts::storage::LogStreamType::StdOut,
            ),
            forward_stderr: StdOutputConfigWithSender::new(
                self.forward_stderr,
                log_forwarder_sender,
                concepts::storage::LogStreamType::StdErr,
            ),
            user_exports_noext: self.user_wasm_component.exported_functions(false).to_vec(),
            cancel_registry,
        }
    }
}

pub struct ActivityVmWorker {
    module: Module,
    engine: Arc<Engine>,
    mapdirs: Vec<MapDir>,
    qemu_runtime: Option<activity_vm_runner::QemuRuntimeConfig>,
    guest_args: Vec<String>,
    policy_spec: ProcessHttpPolicySpec,
    secrets: Option<ExecSecrets>,
    exposed_secrets: Vec<String>,
    params_via_stdin: bool,
    env: HashMap<String, String>,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    max_output_bytes: u64,
    forward_stdout: Option<StdOutputConfigWithSender>,
    forward_stderr: Option<StdOutputConfigWithSender>,
    user_exports_noext: Vec<FunctionMetadata>,
    cancel_registry: CancelRegistry,
}

#[async_trait]
impl Worker for ActivityVmWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        let started = Instant::now();
        tracing::debug!("Starting activity VM worker run");
        let version = ctx.version.clone();
        let json_params = ctx
            .params
            .as_json_values()
            .expect("params come from the database, not Wasmtime");
        assert_eq!(self.user_params.len(), json_params.len());

        let empty_resolver = EmptyResolver;
        let resolver = self
            .secrets
            .as_ref()
            .map(|secrets| secrets.resolver.as_ref())
            .unwrap_or(&empty_resolver);
        let (policy, placeholders) = wasm_workers::policy_builder::build_process_http_policy(
            self.policy_spec.clone(),
            resolver,
        )
        .map_err(|error| cannot_instantiate("cannot build VM HTTP policy", error, &version))?;
        let mut env = self.env.clone();
        env.extend(placeholders);
        for name in &self.exposed_secrets {
            let value = resolver.secret_lookup(name).ok_or_else(|| {
                cannot_instantiate(
                    "cannot expose VM secret",
                    anyhow::anyhow!("secret `{name}` is not available"),
                    &version,
                )
            })?;
            env.insert(name.clone(), value.expose_secret().to_owned());
        }

        let mut guest_args = self.guest_args.clone();
        if !self.params_via_stdin {
            guest_args.extend(json_params.iter().map(|value| {
                serde_json::to_string(value).expect("serde_json::Value is serializable")
            }));
        }
        let stdin = if self.params_via_stdin {
            let mut input = serde_json::Map::new();
            input.insert("params".to_owned(), json_params.to_vec().into());
            Some(serde_json::to_vec(&input).expect("JSON map serialization cannot fail"))
        } else {
            None
        };

        let max_stdout = usize::try_from(self.max_output_bytes)
            .expect("32 bit systems are unsupported")
            .max(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let execution = activity_vm_runner::execute(
            &self.engine,
            self.module.clone(),
            self.mapdirs.clone(),
            self.qemu_runtime.clone(),
            guest_args,
            env,
            stdin,
            policy,
            cancelled.clone(),
            max_stdout,
            16 * 1024 * 1024,
        );
        tokio::pin!(execution);
        let cancellation = self
            .cancel_registry
            .activity_obtain_cancellation_token(ctx.execution_id.clone());
        let output = tokio::select! {
            result = &mut execution => {
                result.map_err(|error| cannot_instantiate("VM execution failed", error, &version))?
            }
            _ = cancellation => {
                cancelled.store(true, Ordering::Relaxed);
                self.engine.increment_epoch();
                let _ = execution.await;
                return Err(WorkerError::FatalError(FatalError::Cancelled, version));
            }
        };
        tracing::debug!(
            elapsed_ms = started.elapsed().as_millis(),
            "Activity VM runner returned to worker"
        );

        forward(self.forward_stdout.as_ref(), &output.stdout, &ctx);
        forward(self.forward_stderr.as_ref(), &output.stderr, &ctx);
        let output_variant_is_unit =
            output_variant_is_unit(output.exit_code, &self.user_return_type);
        if !output_variant_is_unit && output.stdout.len() > max_stdout {
            return Err(cannot_instantiate(
                "VM stdout exceeded max_output_bytes",
                anyhow::anyhow!("limit is {} bytes", self.max_output_bytes),
                &version,
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed = if output_variant_is_unit || stdout.trim().is_empty() {
            None
        } else {
            Some(serde_json::from_str(&stdout).map_err(|error| {
                WorkerError::FatalError(
                    FatalError::ResultParsingError(
                        concepts::ResultParsingError::ResultParsingErrorFromVal(
                            concepts::ResultParsingErrorFromVal::TypeCheckError(format!(
                                "failed to parse VM stdout as JSON on exit {}: {error}",
                                output.exit_code
                            )),
                        ),
                    ),
                    version.clone(),
                )
            })?)
        };
        let retval = if output.exit_code == 0 {
            wasm_workers::js_worker_utils::map_ok_variant(
                parsed,
                &self.user_return_type,
                version.clone(),
            )?
        } else {
            wasm_workers::js_worker_utils::map_err_variant(
                parsed,
                &self.user_return_type,
                version.clone(),
            )?
        };
        Ok(WorkerResultOk::RunFinished(RunFinished {
            retval,
            version,
            http_client_traces: Some(output.http_client_traces),
        }))
    }
}

#[derive(Debug)]
struct EmptyResolver;

impl SecretResolver for EmptyResolver {
    fn secret_lookup(&self, _name: &str) -> Option<secrecy::SecretString> {
        None
    }
}

fn cannot_instantiate(
    reason: &str,
    error: impl std::fmt::Display,
    version: &concepts::storage::Version,
) -> WorkerError {
    WorkerError::FatalError(
        FatalError::CannotInstantiate {
            reason: reason.to_owned(),
            detail: Some(error.to_string()),
        },
        version.clone(),
    )
}

fn output_variant_is_unit(exit_code: i32, return_type: &ReturnTypeExtendable) -> bool {
    if exit_code == 0 {
        return_type.type_wrapper_tl.ok.is_none()
    } else {
        return_type.type_wrapper_tl.err.is_none()
    }
}

fn forward(config: Option<&StdOutputConfigWithSender>, bytes: &[u8], ctx: &WorkerContext) {
    use std::io::Write as _;
    match config {
        Some(StdOutputConfigWithSender::Stdout) => {
            let _ = std::io::stdout().write_all(bytes);
        }
        Some(StdOutputConfigWithSender::Stderr) => {
            let _ = std::io::stderr().write_all(bytes);
        }
        Some(StdOutputConfigWithSender::Db {
            sender,
            forwarding_from,
        }) => {
            let _ = sender.try_send(concepts::storage::LogInfoAppendRow {
                execution_id: ctx.execution_id.clone(),
                run_id: ctx.locked_event.run_id,
                log_entry: concepts::storage::LogEntry::Stream {
                    created_at: chrono::Utc::now(),
                    payload: bytes.to_vec(),
                    stream_type: *forwarding_from,
                },
            });
        }
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::output_variant_is_unit;
    use concepts::ReturnType;

    #[test]
    fn result_of_units_ignores_non_json_stdout() {
        let type_wrapper = val_json::type_wrapper::parse_wit_type("result").unwrap();
        let ReturnType::Extendable(return_type) = ReturnType::detect(
            type_wrapper,
            concepts::StrVariant::from("result".to_owned()),
        ) else {
            panic!("result must be an extendable return type");
        };

        assert!(output_variant_is_unit(0, &return_type));
        assert!(output_variant_is_unit(1, &return_type));
    }
}
