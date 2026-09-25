use activity_vm_runner::MapDir;
use async_trait::async_trait;
use concepts::storage::http_client_trace::HttpClientTrace;
use concepts::time::{ClockFn, Sleep};
use concepts::{
    ComponentType, FunctionFqn, FunctionMetadata, PackageIfcFns, ParameterType,
    ReturnTypeExtendable,
};
use executor::worker::{
    FatalError, RunFinished, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use secrecy::ExposeSecret as _;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
    memory: Option<u64>,
}

impl ActivityVmWorkerCompiled {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        module: Module,
        engine: Arc<Engine>,
        mapdirs: Vec<MapDir>,
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
        memory: Option<u64>,
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
            memory,
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
        clock_fn: Box<dyn ClockFn>,
        sleep: Arc<dyn Sleep>,
    ) -> ActivityVmWorker {
        ActivityVmWorker {
            module: self.module,
            engine: self.engine,
            mapdirs: self.mapdirs,
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
            memory: self.memory,
            clock_fn,
            sleep,
        }
    }
}

pub struct ActivityVmWorker {
    module: Module,
    engine: Arc<Engine>,
    mapdirs: Vec<MapDir>,
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
    memory: Option<u64>,
    clock_fn: Box<dyn ClockFn>,
    sleep: Arc<dyn Sleep>,
}

#[async_trait]
impl Worker for ActivityVmWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        let started = Instant::now();
        let started_at = self.clock_fn.now();
        tracing::debug!("Starting activity VM worker run");
        let version = ctx.version.clone();
        let lock_expires_at = ctx.locked_event.lock_expires_at;
        let Ok(deadline_duration) = (lock_expires_at - started_at).to_std() else {
            tracing::info!(execution_deadline = %lock_expires_at, %started_at,
                "Timed out - started_at later than execution_deadline");
            return Err(WorkerError::TemporaryTimeout {
                http_client_traces: None,
                version,
            });
        };
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
        // Verification rejects absent required secrets, so a missing one is an absent optional.
        for name in &self.exposed_secrets {
            if let Some(value) = resolver.secret_lookup(name) {
                env.insert(name.clone(), value.expose_secret().to_owned());
            }
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
        let cancellation = self
            .cancel_registry
            .activity_obtain_cancellation_token(ctx.execution_id.clone());
        let mut execution_interrupt_watcher = ctx.execution_interrupt_watcher.clone();
        let http_client_traces = Arc::new(Mutex::new(Vec::new()));
        let execution = activity_vm_runner::execute(
            &self.engine,
            self.module.clone(),
            self.mapdirs.clone(),
            guest_args,
            env,
            stdin,
            policy,
            http_client_traces.clone(),
            max_stdout,
            16 * 1024 * 1024,
            self.memory,
        );
        // Dropping the execution stops the VM, like in the regular activity worker.
        let output = tokio::select! {
            result = execution => {
                result.map_err(|error| cannot_instantiate("VM execution failed", error, &version))?
            }
            () = self.sleep.sleep(deadline_duration) => {
                tracing::info!(duration = ?started.elapsed(), "Run timed out");
                return Err(WorkerError::TemporaryTimeout {
                    http_client_traces: Some(take_traces(&http_client_traces)),
                    version,
                });
            }
            _ = cancellation => {
                return Err(WorkerError::FatalError(FatalError::Cancelled, version));
            }
            _ = execution_interrupt_watcher.changed() => {
                return Err(WorkerError::ExecutionYielded {
                    version,
                    reason: executor::worker::ExecutionYieldReason::ExecutorClosing,
                });
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
            http_client_traces: Some(take_traces(&http_client_traces)),
        }))
    }
}

fn take_traces(traces: &Mutex<Vec<HttpClientTrace>>) -> Vec<HttpClientTrace> {
    std::mem::take(&mut traces.lock().expect("trace mutex poisoned"))
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
    use super::*;
    use concepts::component_id::COMPONENT_DIGEST_DUMMY;
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutionId, ExecutorId, RunId};
    use concepts::storage::{Locked, Version};
    use concepts::time::{Now, TokioSleep};
    use concepts::{ComponentRetryConfig, ExecutionMetadata, Params, ReturnType, StrVariant};
    use std::time::Duration;
    use wasm_workers::epoch_ticker::EpochTicker;

    fn return_type(wit: &str) -> ReturnTypeExtendable {
        let type_wrapper = val_json::type_wrapper::parse_wit_type(wit).unwrap();
        let ReturnType::Extendable(return_type) =
            ReturnType::detect(type_wrapper, StrVariant::from(wit.to_owned()))
        else {
            panic!("{wit} must be an extendable return type");
        };
        return_type
    }

    #[test]
    fn result_of_units_ignores_non_json_stdout() {
        let return_type = return_type("result");

        assert!(output_variant_is_unit(0, &return_type));
        assert!(output_variant_is_unit(1, &return_type));
    }

    #[tokio::test]
    async fn lock_expiry_stops_the_vm() {
        let mut config = wasmtime::Config::new();
        config.epoch_interruption(true);
        let engine = Arc::new(Engine::new(&config).unwrap());
        let _epoch_ticker = EpochTicker::spawn_new(vec![engine.weak()], Duration::from_millis(1));
        // Stands in for the emulator: a guest that never finishes.
        let module = Module::new(
            &engine,
            wat::parse_str("(module (func (export \"_start\") (loop br 0)))").unwrap(),
        )
        .unwrap();
        let ffqn = FunctionFqn::new_static("testing:vm/hang", "run");
        let worker = ActivityVmWorkerCompiled::new(
            module,
            engine,
            Vec::new(),
            Vec::new(),
            ProcessHttpPolicySpec {
                component: Vec::new(),
                global: Vec::new(),
            },
            None,
            Vec::new(),
            false,
            HashMap::new(),
            &ffqn,
            Vec::new(),
            return_type("result<string, string>"),
            1024,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (log_sender, _log_receiver) = tokio::sync::mpsc::channel(1);
        let worker = worker.into_worker(
            CancelRegistry::new(),
            &log_sender,
            Now.clone_box(),
            Arc::new(TokioSleep),
        );
        let component_id = concepts::ComponentId::new(
            ComponentType::Activity,
            StrVariant::Static("vm"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap();
        let (_close_tx, execution_interrupt_watcher) = tokio::sync::watch::channel(false);
        let ctx = WorkerContext {
            execution_id: ExecutionId::generate(),
            metadata: ExecutionMetadata::empty(),
            component_digest: component_id.component_digest.clone(),
            ffqn,
            params: Params::empty(),
            event_history: Vec::new(),
            responses: Vec::new(),
            parent: None,
            version: Version::new(0),
            can_be_retried: false,
            worker_span: tracing::info_span!("vm_test"),
            locked_event: Locked {
                component_id,
                executor_id: ExecutorId::generate(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                run_id: RunId::generate(),
                lock_expires_at: Now.now() + chrono::Duration::milliseconds(100),
                retry_config: ComponentRetryConfig::ZERO,
            },
            execution_interrupt_watcher,
            instance_permit: None,
        };

        let result = tokio::time::timeout(Duration::from_secs(10), worker.run(ctx))
            .await
            .expect("lock expiry must stop the VM");
        assert!(
            matches!(result, Err(WorkerError::TemporaryTimeout { .. })),
            "{result:?}"
        );
    }
}
