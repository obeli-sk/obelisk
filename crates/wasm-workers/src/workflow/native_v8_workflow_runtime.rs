#![allow(clippy::needless_pass_by_value)]

use super::workflow_ctx::{NativeJoinNextTryError, WorkflowCtx};
use super::workflow_runtime::{RuntimePrepareError, WorkflowInvocation, WorkflowRuntime};
use super::workflow_worker::{CallFuncResult, RunError};
use crate::js_imports::NamedFnImport;
use crate::v8_pool::V8Pool;
use async_trait::async_trait;
use chrono::{TimeZone as _, Utc};
use concepts::storage::ResponseSubscriptionEnd;
use concepts::storage::{HistoryEventScheduleAt, LogLevel};
use concepts::{
    ComponentId, FunctionFqn, IfcFqnName, JoinSetId, Params, ResultParsingError,
    ResultParsingErrorFromVal, ReturnTypeExtendable, SupportedFunctionReturnValue, TrapKind,
};
use deno_core::{
    JsRuntime, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleLoader,
    ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType, OpState, PollEventLoopOptions,
    ResolutionKind, RuntimeOptions, op2, resolve_import,
};
use deno_error::JsErrorBox;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::Duration;

pub(crate) struct NativeV8WorkflowRuntime {
    entry_path: String,
    files: BTreeMap<String, String>,
    return_type: ReturnTypeExtendable,
    resolved_imports: HashMap<IfcFqnName, Vec<NamedFnImport>>,
    v8_pool: V8Pool,
}

impl NativeV8WorkflowRuntime {
    pub(crate) fn new(
        entry_path: String,
        files: BTreeMap<String, String>,
        return_type: ReturnTypeExtendable,
        resolved_imports: HashMap<IfcFqnName, Vec<NamedFnImport>>,
        v8_pool: V8Pool,
    ) -> Self {
        Self {
            entry_path,
            files,
            return_type,
            resolved_imports,
            v8_pool,
        }
    }
}

struct NativeV8Invocation {
    workflow_ctx: WorkflowCtx,
    entry_path: String,
    files: BTreeMap<String, String>,
    params: Params,
    return_type: ReturnTypeExtendable,
    resolved_imports: HashMap<IfcFqnName, Vec<NamedFnImport>>,
    v8_pool: V8Pool,
}

#[async_trait]
impl WorkflowRuntime for NativeV8WorkflowRuntime {
    async fn prepare(
        &self,
        workflow_ctx: WorkflowCtx,
        _component_id: &ComponentId,
        _ffqn: &FunctionFqn,
        params: &Params,
        _fuel: Option<u64>,
    ) -> Result<Box<dyn WorkflowInvocation>, RuntimePrepareError> {
        Ok(Box::new(NativeV8Invocation {
            workflow_ctx,
            entry_path: self.entry_path.clone(),
            files: self.files.clone(),
            params: params.clone(),
            return_type: self.return_type.clone(),
            resolved_imports: self.resolved_imports.clone(),
            v8_pool: self.v8_pool.clone(),
        }))
    }
}

enum NativeV8Failure {
    CannotInstantiate(String),
    ResultParsing(String),
    Trap(String),
    Host(super::workflow_ctx::WorkflowFunctionError),
}

#[async_trait]
impl WorkflowInvocation for NativeV8Invocation {
    async fn invoke(self: Box<Self>, _assigned_fuel: Option<u64>) -> CallFuncResult {
        let NativeV8Invocation {
            workflow_ctx,
            entry_path,
            files,
            params,
            return_type,
            resolved_imports,
            v8_pool,
        } = *self;
        let interruption = workflow_ctx.native_interruption();
        let handle = tokio::runtime::Handle::current();
        let max_heap_size = v8_pool.max_heap_size();
        let (isolate_tx, isolate_rx) = tokio::sync::oneshot::channel();
        let mut task = tokio::spawn(async move {
            let mut workflow_ctx = workflow_ctx;

            v8_pool
                .execute_for(crate::v8_pool::V8Workload::Workflow, move || async move {
                    let result = execute(
                        ExecuteArgs {
                            entry_path: &entry_path,
                            files: &files,
                            params: &params,
                            return_type: &return_type,
                            resolved_imports: &resolved_imports,
                        },
                        &mut workflow_ctx,
                        MainRuntimeHandle(handle),
                        isolate_tx,
                        max_heap_size,
                    )
                    .await;
                    (result, workflow_ctx)
                })
                .await
                .expect("native V8 workflow pool stopped")
        });
        let mut termination_guard = isolate_rx.await.ok().map(TerminationGuard::new);
        let result = match interruption {
            Some(Ok(interruption)) => tokio::select! {
                result = &mut task => result,
                reason = interruption => {
                    if let Some(guard) = &termination_guard {
                        guard.handle.terminate_execution();
                    }
                    let mut result = task.await;
                    if let Ok((failure, _)) = &mut result {
                        *failure = Err(NativeV8Failure::Host(interruption_error(reason)));
                    }
                    result
                }
            },
            Some(Err(reason)) => {
                if let Some(guard) = &termination_guard {
                    guard.handle.terminate_execution();
                }
                let mut result = task.await;
                if let Ok((failure, _)) = &mut result {
                    *failure = Err(NativeV8Failure::Host(interruption_error(reason)));
                }
                result
            }
            None => task.await,
        };
        if let Some(guard) = &mut termination_guard {
            guard.armed = false;
        }

        match result {
            Ok((Ok(retval), workflow_ctx)) => Ok((retval, workflow_ctx)),
            Ok((Err(failure), workflow_ctx)) => match failure {
                NativeV8Failure::CannotInstantiate(reason) => {
                    Err(RunError::CannotInstantiate(reason, Box::new(workflow_ctx)))
                }
                NativeV8Failure::ResultParsing(reason) => Err(RunError::ResultParsingError(
                    ResultParsingError::ResultParsingErrorFromVal(
                        ResultParsingErrorFromVal::TypeCheckError(reason),
                    ),
                    Box::new(workflow_ctx),
                )),
                NativeV8Failure::Trap(reason) => Err(RunError::Trap {
                    reason,
                    detail: None,
                    workflow_ctx: Box::new(workflow_ctx),
                    kind: TrapKind::Trap,
                }),
                NativeV8Failure::Host(err) => Err(RunError::WorkerPartialResult(
                    err.into_worker_partial_result(workflow_ctx.version().clone()),
                    Box::new(workflow_ctx),
                )),
            },
            Err(err) => panic!("native V8 workflow task panicked: {err}"),
        }
    }
}

fn interruption_error(
    reason: ResponseSubscriptionEnd,
) -> super::workflow_ctx::WorkflowFunctionError {
    use super::deadline_tracker::InterruptKind;
    use super::workflow_ctx::WorkflowFunctionError;
    match reason {
        ResponseSubscriptionEnd::LockDeadlineReached => WorkflowFunctionError::LockExpired,
        ResponseSubscriptionEnd::ExecutorClosing => {
            WorkflowFunctionError::Interrupt(InterruptKind::ExecutorClosing)
        }
        ResponseSubscriptionEnd::ExecutionUpdated => {
            WorkflowFunctionError::Interrupt(InterruptKind::PauseOrCancel)
        }
        ResponseSubscriptionEnd::PollIntervalElapsed => unreachable!("unbounded interruption wait"),
    }
}

struct TerminationGuard {
    handle: deno_core::v8::IsolateHandle,
    armed: bool,
}

impl TerminationGuard {
    fn new(handle: deno_core::v8::IsolateHandle) -> Self {
        Self {
            handle,
            armed: true,
        }
    }
}

impl Drop for TerminationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.handle.terminate_execution();
        }
    }
}

struct HostState {
    workflow_ctx: usize,
    handle: MainRuntimeHandle,
    join_sets: Vec<Option<JoinSetId>>,
}

#[derive(Clone)]
struct MainRuntimeHandle(tokio::runtime::Handle);

impl MainRuntimeHandle {
    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        let _guard = self.0.enter();
        futures_lite::future::block_on(future)
    }
}

impl HostState {
    fn context(&mut self) -> &mut WorkflowCtx {
        // SAFETY: execute owns WorkflowCtx for the lifetime of the JsRuntime and drops V8 first.
        unsafe { &mut *(self.workflow_ctx as *mut WorkflowCtx) }
    }

    fn call<T>(
        &mut self,
        f: impl FnOnce(
            &mut WorkflowCtx,
            &MainRuntimeHandle,
        ) -> Result<T, super::workflow_ctx::WorkflowFunctionError>,
    ) -> Result<T, JsErrorBox> {
        let handle = self.handle.clone();
        f(self.context(), &handle).map_err(|err| {
            if is_runtime_control_flow(&err) {
                self.context().set_native_host_error(err.clone());
            }
            JsErrorBox::generic(err.to_string())
        })
    }
}

#[op2]
#[serde]
fn op_obelisk_host(
    state: &mut OpState,
    #[serde] request: serde_json::Value,
) -> Result<serde_json::Value, JsErrorBox> {
    let op = request
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| JsErrorBox::type_error("host operation is missing"))?;
    let args = request.get("args").cloned().unwrap_or(Value::Null);
    let host = state.borrow_mut::<HostState>();
    match op {
        "executionIdCurrent" => Ok(json!(host.context().native_execution_id_current())),
        "executionIdGenerate" => {
            let id = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle
                    .block_on(ctx.execution_id_generate(backtrace))
                    .map_err(anyhow_to_workflow_error)
            })?;
            Ok(json!(id.to_string()))
        }
        "now" => {
            let datetime = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle
                    .block_on(ctx.sleep_named(HistoryEventScheduleAt::Now, None, backtrace))
                    .map_err(anyhow_to_workflow_error)?
                    .map_err(|()| {
                        super::workflow_ctx::WorkflowFunctionError::ConstraintViolation(
                            "sleep was cancelled".into(),
                        )
                    })
            })?;
            Ok(json!(datetime_to_millis(&datetime)))
        }
        "createJoinSet" => {
            let name = args.get("name").and_then(Value::as_str).map(str::to_owned);
            let id = host.call(|ctx, handle| handle.block_on(ctx.native_join_set_create(name)))?;
            let index = host.join_sets.len();
            host.join_sets.push(Some(id));
            Ok(json!(index))
        }
        "lastId" => {
            let id = join_set(host, &args)?;
            Ok(host
                .context()
                .native_join_set_last_id(&id)
                .map_or(Value::Null, Value::String))
        }
        "joinSetId" => Ok(json!(join_set(host, &args)?.to_string())),
        "call" => {
            let target = string_arg(&args, "target")?
                .parse::<FunctionFqn>()
                .map_err(|err| JsErrorBox::type_error(format!("invalid function name: {err}")))?;
            let params = serde_json::to_string(args.get("params").unwrap_or(&Value::Null))
                .map_err(JsErrorBox::from_err)?;
            let outcome = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle
                    .block_on(ctx.call_json(target, params, backtrace))
                    .map_err(anyhow_to_workflow_error)?
                    .map_err(|err| {
                        super::workflow_ctx::WorkflowFunctionError::ConstraintViolation(
                            format!("call failed: {err:?}").into(),
                        )
                    })
            })?;
            let child_id = host.context().native_last_direct_call_id();
            Ok(outcome_envelope_with_id_and_kind(
                outcome,
                child_id,
                host.context(),
            ))
        }
        "submit" => {
            let join_set_id = join_set(host, &args)?;
            let target = string_arg(&args, "target")?
                .parse::<FunctionFqn>()
                .map_err(|err| JsErrorBox::type_error(format!("invalid function name: {err}")))?;
            let params = serde_json::to_string(args.get("params").unwrap_or(&Value::Null))
                .map_err(JsErrorBox::from_err)?;
            let execution_id = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle
                    .block_on(ctx.submit_json(join_set_id, target, params, backtrace))
                    .map_err(anyhow_to_workflow_error)?
                    .map_err(|err| {
                        super::workflow_ctx::WorkflowFunctionError::ConstraintViolation(
                            format!("submit failed: {err:?}").into(),
                        )
                    })
            })?;
            Ok(json!(execution_id.id))
        }
        "submitDelay" => {
            let join_set_id = join_set(host, &args)?;
            let schedule = schedule_arg(&args, "schedule")?;
            let delay_id = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle.block_on(ctx.submit_delay(join_set_id, schedule, backtrace))
            })?;
            Ok(json!(delay_id.id))
        }
        "joinNext" => {
            let join_set_id = join_set(host, &args)?;
            let outcome = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle.block_on(ctx.join_next(join_set_id.clone(), backtrace))
            })?;
            Ok(match outcome {
                Ok(outcome) => {
                    let child_id = host.context().native_join_set_last_id(&join_set_id);
                    outcome_envelope_with_id_and_kind(outcome, child_id, host.context())
                }
                Err(_) => json!({"exhausted": true}),
            })
        }
        "joinNextFor" => {
            let join_set_id = join_set(host, &args)?;
            let target = string_arg(&args, "target")?
                .parse::<FunctionFqn>()
                .map_err(|err| JsErrorBox::type_error(format!("invalid function name: {err}")))?;
            let handle = host.handle.clone();
            let backtrace = host.context().native_backtrace();
            let outcome = handle
                .block_on(
                    host.context()
                        .join_next_for(join_set_id.clone(), target, backtrace),
                )
                .map_err(|err| {
                    if is_runtime_control_flow(&err) {
                        host.context().set_native_host_error(err.clone());
                    }
                    JsErrorBox::generic(err.to_string())
                })?;
            Ok(match outcome {
                Ok(outcome) => {
                    let child_id = host.context().native_join_set_last_id(&join_set_id);
                    outcome_envelope_with_id_and_kind(outcome, child_id, host.context())
                }
                Err(crate::workflow::host_exports::latest::obelisk::workflow::workflow_support::JoinNextForError::AllProcessed) => {
                    json!({"allProcessed": true})
                }
                Err(crate::workflow::host_exports::latest::obelisk::workflow::workflow_support::JoinNextForError::FunctionMismatch(mismatch)) => {
                    let actual_target = mismatch.actual_function.map(|function| {
                        format!("{}.{}", function.interface_name, function.function_name)
                    });
                    let actual_id = match mismatch.actual_id {
                        crate::workflow::host_exports::latest::obelisk::types::execution::ResponseId::ExecutionId(id) => id.id,
                        crate::workflow::host_exports::latest::obelisk::types::execution::ResponseId::DelayId(id) => id.id,
                    };
                    json!({"mismatch": {"actualTarget": actual_target, "actualId": actual_id}})
                }
            })
        }
        "joinNextTry" => {
            let join_set_id = join_set(host, &args)?;
            let outcome = host.call(|ctx, handle| {
                handle.block_on(ctx.native_join_next_try(join_set_id.clone()))
            })?;
            Ok(match outcome {
                Ok(outcome) => {
                    let child_id = host.context().native_join_set_last_id(&join_set_id);
                    outcome_envelope_with_id_and_kind(outcome, child_id, host.context())
                }
                Err(NativeJoinNextTryError::Pending) => json!({"pending": true}),
                Err(NativeJoinNextTryError::AllProcessed) => json!({"exhausted": true}),
            })
        }
        "sleep" => {
            let schedule = schedule_arg(&args, "schedule")?;
            let name = args.get("name").and_then(Value::as_str).map(str::to_owned);
            let datetime = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle
                    .block_on(ctx.sleep_named(schedule, name, backtrace))
                    .map_err(anyhow_to_workflow_error)?
                    .map_err(|()| {
                        super::workflow_ctx::WorkflowFunctionError::ConstraintViolation(
                            "sleep was cancelled".into(),
                        )
                    })
            })?;
            Ok(json!(datetime_to_millis(&datetime)))
        }
        "randomU64" | "randomU64Inclusive" => {
            let min = u64_arg(&args, "min")?;
            let max = u64_arg(&args, "max")?;
            let inclusive = op == "randomU64Inclusive";
            let value = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                let result = if inclusive {
                    handle.block_on(ctx.random_u64_inclusive(min, max, backtrace.clone()))
                } else {
                    handle.block_on(ctx.random_u64_exclusive(min, max, backtrace))
                };
                result.map_err(anyhow_to_workflow_error)
            })?;
            Ok(json!(value))
        }
        "randomString" => {
            let min = u64_arg(&args, "min")?
                .try_into()
                .map_err(|_| JsErrorBox::range_error("min is too large"))?;
            let max = u64_arg(&args, "max")?
                .try_into()
                .map_err(|_| JsErrorBox::range_error("max is too large"))?;
            let value = host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle
                    .block_on(ctx.random_string(min, max, backtrace))
                    .map_err(anyhow_to_workflow_error)
            })?;
            Ok(json!(value))
        }
        "schedule" => {
            let execution_id = string_arg(&args, "executionId")?
                .parse::<concepts::ExecutionId>()
                .map_err(|err| JsErrorBox::type_error(format!("invalid execution id: {err}")))?;
            let target = string_arg(&args, "target")?
                .parse::<FunctionFqn>()
                .map_err(|err| JsErrorBox::type_error(format!("invalid function name: {err}")))?;
            let params = serde_json::to_string(args.get("params").unwrap_or(&Value::Null))
                .map_err(JsErrorBox::from_err)?;
            let schedule = schedule_arg(&args, "schedule")?;
            host.call(|ctx, handle| {
                handle.block_on(ctx.native_schedule_json(execution_id, target, params, schedule))
            })?;
            Ok(Value::Null)
        }
        "stub" => {
            let execution_id = string_arg(&args, "executionId")?.to_owned();
            let retval = string_arg(&args, "resultJson")?.to_owned();
            host.call(|ctx, handle| handle.block_on(ctx.native_stub_json(execution_id, retval)))?;
            Ok(Value::Null)
        }
        "close" => {
            let index = index_arg(&args)?;
            let id = host
                .join_sets
                .get_mut(index)
                .and_then(Option::take)
                .ok_or_else(|| JsErrorBox::generic("join set is closed"))?;
            host.call(|ctx, handle| {
                let backtrace = ctx.native_backtrace();
                handle
                    .block_on(ctx.join_set_close(&id, backtrace))
                    .map_err(anyhow_to_workflow_error)
            })?;
            Ok(Value::Null)
        }
        "log" => {
            let level = args.get("level").and_then(Value::as_str).unwrap_or("info");
            let message = args
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let level = match level {
                "trace" => LogLevel::Trace,
                "debug" => LogLevel::Debug,
                "warn" => LogLevel::Warn,
                "error" => LogLevel::Error,
                _ => LogLevel::Info,
            };
            host.context().native_log(level, message.to_owned());
            Ok(Value::Null)
        }
        _ => Err(JsErrorBox::generic(format!(
            "unsupported workflow host operation: {op}"
        ))),
    }
}

deno_core::extension!(obelisk_v8, ops = [op_obelisk_host]);

struct ExecuteArgs<'a> {
    entry_path: &'a str,
    files: &'a BTreeMap<String, String>,
    params: &'a Params,
    return_type: &'a ReturnTypeExtendable,
    resolved_imports: &'a HashMap<IfcFqnName, Vec<NamedFnImport>>,
}

async fn execute(
    args: ExecuteArgs<'_>,
    workflow_ctx: &mut WorkflowCtx,
    handle: MainRuntimeHandle,
    isolate_tx: tokio::sync::oneshot::Sender<deno_core::v8::IsolateHandle>,
    max_heap_size: usize,
) -> Result<SupportedFunctionReturnValue, NativeV8Failure> {
    let ExecuteArgs {
        entry_path,
        files,
        params,
        return_type,
        resolved_imports,
    } = args;
    let loader = Rc::new(InMemoryModuleLoader::new(files, resolved_imports));
    let mut runtime = JsRuntime::new(RuntimeOptions {
        module_loader: Some(loader.clone()),
        extensions: vec![obelisk_v8::init()],
        create_params: Some(deno_core::v8::CreateParams::default().heap_limits(0, max_heap_size)),
        ..Default::default()
    });
    let _ = isolate_tx.send(runtime.v8_isolate().thread_safe_handle());
    runtime.op_state().borrow_mut().put(HostState {
        workflow_ctx: std::ptr::from_mut(workflow_ctx) as usize,
        handle: handle.clone(),
        join_sets: Vec::new(),
    });

    let params = params
        .as_json_values()
        .ok_or_else(|| NativeV8Failure::ResultParsing("parameters are not JSON values".into()))?;
    let entry = loader.specifier_for_path(entry_path).ok_or_else(|| {
        NativeV8Failure::CannotInstantiate("JavaScript entry module was not found".into())
    })?;
    let main = ModuleSpecifier::parse("obelisk-main:run")
        .map_err(|err| NativeV8Failure::CannotInstantiate(err.to_string()))?;
    let source = format!(
        "import 'obelisk:workflow@1.0.0'; import workflow from {}; try {{ globalThis.__obeliskResult = {{ ok: true, value: await workflow(...{}) }}; }} catch (error) {{ globalThis.__obeliskResult = {{ ok: false, absent: error === undefined || (error instanceof Error && error.value === undefined), value: error instanceof Error && 'value' in error ? error.value : error }}; }}",
        serde_json::to_string(entry.as_str()).expect("URL must serialize"),
        serde_json::to_string(&params).expect("parameters must serialize")
    );
    let evaluated = async {
        let id = runtime.load_main_es_module_from_code(&main, source).await?;
        let evaluation = runtime.mod_evaluate(id);
        runtime
            .run_event_loop(PollEventLoopOptions::default())
            .await?;
        evaluation.await
    }
    .await;
    if let Err(err) = evaluated {
        if let Some(host_err) = workflow_ctx.take_native_host_error() {
            return Err(NativeV8Failure::Host(host_err));
        }
        let reason = err.to_string();
        if reason.contains("does not provide an export named 'default'") {
            return Err(NativeV8Failure::CannotInstantiate(format!(
                "JavaScript entry module has no default export: {reason}"
            )));
        }
        return Err(NativeV8Failure::Trap(reason));
    }
    if let Some(host_err) = workflow_ctx.take_native_host_error() {
        return Err(NativeV8Failure::Host(host_err));
    }
    let value = runtime
        .execute_script("obelisk:result", "globalThis.__obeliskResult")
        .map_err(|err| NativeV8Failure::Trap(err.to_string()))?;
    let envelope = {
        deno_core::scope!(scope, runtime);
        let local = deno_core::v8::Local::new(scope, value);
        deno_core::serde_v8::from_v8::<Value>(scope, local)
            .map_err(|err| NativeV8Failure::ResultParsing(err.to_string()))?
    };
    let ok = envelope.get("ok").and_then(Value::as_bool).unwrap_or(false);
    let absent = envelope
        .get("absent")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let value = (!absent).then(|| envelope.get("value").cloned().unwrap_or(Value::Null));
    let mapped = if ok {
        crate::js_worker_utils::map_ok_variant_fatal(
            value,
            return_type,
            concepts::storage::Version(0),
        )
    } else {
        crate::js_worker_utils::map_err_variant_fatal(
            value,
            return_type,
            concepts::storage::Version(0),
        )
    };
    mapped.map_err(|(err, _)| NativeV8Failure::ResultParsing(err.to_string()))
}

struct InMemoryModuleLoader {
    sources: HashMap<String, String>,
    paths: HashMap<String, ModuleSpecifier>,
}

impl InMemoryModuleLoader {
    fn new(
        files: &BTreeMap<String, String>,
        imports: &HashMap<IfcFqnName, Vec<NamedFnImport>>,
    ) -> Self {
        let mut sources = HashMap::new();
        let mut paths = HashMap::new();
        for (path, source) in files {
            let specifier = ModuleSpecifier::parse(&format!("file:///obelisk/{path}"))
                .expect("generated module URL must parse");
            sources.insert(specifier.to_string(), source.clone());
            paths.insert(path.clone(), specifier);
        }
        sources.insert("obelisk:workflow@1.0.0".into(), WORKFLOW_MODULE.into());
        sources.insert(
            "obelisk:workflow-dynamic@1.0.0".into(),
            DYNAMIC_MODULE.into(),
        );
        for (specifier, functions) in imports {
            sources.insert(
                specifier.to_string(),
                import_module_source(&specifier.to_string(), functions),
            );
        }
        Self { sources, paths }
    }

    fn specifier_for_path(&self, path: &str) -> Option<&ModuleSpecifier> {
        self.paths.get(path)
    }
}

impl ModuleLoader for InMemoryModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, JsErrorBox> {
        if self.sources.contains_key(specifier) {
            ModuleSpecifier::parse(specifier).map_err(JsErrorBox::from_err)
        } else {
            resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)
        }
    }

    fn load(
        &self,
        specifier: &ModuleSpecifier,
        _referrer: Option<&ModuleLoadReferrer>,
        _options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        let result = self
            .sources
            .get(specifier.as_str())
            .cloned()
            .ok_or_else(|| JsErrorBox::generic(format!("module not found: {specifier}")))
            .map(|source| {
                ModuleSource::new(
                    ModuleType::JavaScript,
                    ModuleSourceCode::String(source.into()),
                    specifier,
                    None,
                )
            });
        ModuleLoadResponse::Sync(result)
    }
}

fn import_module_source(specifier: &str, functions: &[NamedFnImport]) -> String {
    use boa_common::imports::{EXT_SUFFIX, SCHEDULE_SUFFIX, STUB_SUFFIX, strip_specifier_suffix};
    use std::fmt::Write as _;
    let schedule_base = strip_specifier_suffix(specifier, SCHEDULE_SUFFIX);
    let ext_base = strip_specifier_suffix(specifier, EXT_SUFFIX);
    let stub_base = strip_specifier_suffix(specifier, STUB_SUFFIX);
    functions
        .iter()
        .enumerate()
        .fold(String::new(), |mut acc, (index, function)| {
            let (target, body) = if let Some(base) = &schedule_base {
                let name = function
                    .wit_name
                    .strip_suffix("-schedule")
                    .expect("validated schedule import");
                (
                    format!("{base}.{name}"),
                    "(...args) => scheduleTarget(TARGET, args)".to_string(),
                )
            } else if let Some(base) = &ext_base {
                if let Some(name) = function.wit_name.strip_suffix("-submit") {
                    (
                        format!("{base}.{name}"),
                        "(joinSet, ...params) => joinSet.__submitTarget(TARGET, params)"
                            .to_string(),
                    )
                } else if let Some(name) = function.wit_name.strip_suffix("-await-next") {
                    (
                        format!("{base}.{name}"),
                        "(joinSet) => joinSet.__joinNextFor(TARGET, NAME)".to_string(),
                    )
                } else {
                    (
                        String::new(),
                        "() => { throw new Error('native V8 get proxy is not implemented'); }"
                            .to_string(),
                    )
                }
            } else if let Some(base) = &stub_base {
                let name = function
                    .wit_name
                    .strip_suffix("-stub")
                    .expect("validated stub import");
                (
                    format!("{base}.{name}"),
                    "(executionId, result) => host('stub', { executionId, resultJson: JSON.stringify(result) })".to_string(),
                )
            } else {
                (
                    format!("{specifier}.{}", function.wit_name),
                    "(...params) => unwrapHost(host('call', { target: TARGET, params }))"
                        .to_string(),
                )
            };
            let binding = format!("__obeliskImport{index}");
            writeln!(
                acc,
                "const {binding}Target = {}; const {binding} = {}; export {{ {binding} as {} }};",
                serde_json::to_string(&target).unwrap(),
                body.replace("TARGET", &format!("{binding}Target")).replace(
                    "NAME",
                    &serde_json::to_string(&function.js_name).unwrap(),
                ),
                function.js_name
            )
            .unwrap();
            acc
        })
}

fn is_runtime_control_flow(err: &super::workflow_ctx::WorkflowFunctionError) -> bool {
    use super::workflow_ctx::WorkflowFunctionError as Error;
    matches!(
        err,
        Error::NondeterminismDetected(_)
            | Error::InterruptDbUpdated
            | Error::DbError(_)
            | Error::LockExpired
            | Error::Interrupt(_)
            | Error::ReplayInterrupt
    )
}

fn outcome_envelope(outcome: Result<Option<String>, Option<String>>) -> Value {
    match outcome {
        Ok(Some(json)) => {
            json!({"ok": serde_json::from_str::<Value>(&json).unwrap_or(Value::Null)})
        }
        Ok(None) => json!({"ok": Value::Null}),
        Err(Some(json)) => {
            json!({"throw": serde_json::from_str::<Value>(&json).unwrap_or(Value::Null)})
        }
        Err(None) => json!({"throwUndefined": true}),
    }
}

fn outcome_envelope_with_id_and_kind(
    outcome: Result<Option<String>, Option<String>>,
    response_id: Option<String>,
    workflow_ctx: &WorkflowCtx,
) -> Value {
    let mut envelope = outcome_envelope(outcome);
    if let Value::Object(object) = &mut envelope {
        let is_cancelled = object.contains_key("throwUndefined");
        let delay_id = response_id
            .as_deref()
            .filter(|id| id.parse::<concepts::prefixed_ulid::DelayId>().is_ok())
            .map(str::to_owned);
        let failure_kind = response_id
            .as_deref()
            .and_then(|id| workflow_ctx.native_child_failure_kind(id));
        if let Some(delay_id) = &delay_id {
            object.insert("delayId".into(), Value::String(delay_id.clone()));
        } else if let Some(response_id) = response_id {
            object.insert("childId".into(), Value::String(response_id));
        }
        object.insert(
            "failureKind".into(),
            if delay_id.is_some() && is_cancelled {
                Value::String("cancelled".into())
            } else {
                failure_kind.map_or(Value::Null, |kind| Value::String(kind.into()))
            },
        );
        if let Some(delay_id) = delay_id.filter(|_| is_cancelled) {
            object.insert(
                "message".into(),
                Value::String(format!("delay {delay_id} cancelled")),
            );
        }
    }
    envelope
}

fn index_arg(args: &Value) -> Result<usize, JsErrorBox> {
    args.get("index")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| JsErrorBox::type_error("join set index is missing"))
}

fn join_set(host: &HostState, args: &Value) -> Result<JoinSetId, JsErrorBox> {
    host.join_sets
        .get(index_arg(args)?)
        .and_then(Clone::clone)
        .ok_or_else(|| JsErrorBox::generic("join set is closed"))
}

fn string_arg<'a>(args: &'a Value, name: &str) -> Result<&'a str, JsErrorBox> {
    args.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| JsErrorBox::type_error(format!("{name} must be a string")))
}

fn u64_arg(args: &Value, name: &str) -> Result<u64, JsErrorBox> {
    args.get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| JsErrorBox::type_error(format!("{name} must be a non-negative integer")))
}

fn schedule_arg(args: &Value, name: &str) -> Result<HistoryEventScheduleAt, JsErrorBox> {
    let value = args.get(name).unwrap_or(&Value::Null);
    if value.is_null() {
        return Ok(HistoryEventScheduleAt::Now);
    }
    let object = value
        .as_object()
        .ok_or_else(|| JsErrorBox::type_error("schedule must be an object"))?;
    let present = [
        "milliseconds",
        "seconds",
        "minutes",
        "hours",
        "days",
        "atMillis",
    ]
    .into_iter()
    .filter(|key| object.contains_key(*key))
    .collect::<Vec<_>>();
    if present.len() > 1 {
        return Err(JsErrorBox::type_error(format!(
            "schedule object has multiple keys ({})",
            present.join(", ")
        )));
    }
    for (key, multiplier) in [
        ("milliseconds", 1_u64),
        ("seconds", 1_000),
        ("minutes", 60_000),
        ("hours", 3_600_000),
        ("days", 86_400_000),
    ] {
        if let Some(number) = object.get(key).and_then(Value::as_u64) {
            return Ok(HistoryEventScheduleAt::In(Duration::from_millis(
                number.saturating_mul(multiplier),
            )));
        }
    }
    if let Some(millis) = object.get("atMillis").and_then(Value::as_i64) {
        let datetime = Utc
            .timestamp_millis_opt(millis)
            .single()
            .ok_or_else(|| JsErrorBox::type_error("schedule Date is invalid"))?;
        return Ok(HistoryEventScheduleAt::At(datetime));
    }
    if let Some(at) = object.get("at").and_then(Value::as_object) {
        let seconds = at
            .get("seconds")
            .and_then(Value::as_i64)
            .ok_or_else(|| JsErrorBox::type_error("invalid absolute schedule"))?;
        let nanos = at
            .get("nanoseconds")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .try_into()
            .map_err(|_| JsErrorBox::type_error("invalid absolute schedule"))?;
        let datetime = Utc
            .timestamp_opt(seconds, nanos)
            .single()
            .ok_or_else(|| JsErrorBox::type_error("invalid absolute schedule"))?;
        return Ok(HistoryEventScheduleAt::At(datetime));
    }
    Err(JsErrorBox::type_error(
        "schedule object has no recognized key",
    ))
}

/// Convert a WIT `Datetime` into JavaScript epoch milliseconds. Timestamp
/// seconds always fit in `i64`, and sub-millisecond precision is dropped since
/// `Date` truncates to integer milliseconds anyway.
fn datetime_to_millis(
    datetime: &crate::workflow::host_exports::latest::obelisk::types::time::Datetime,
) -> i64 {
    datetime.seconds.cast_signed() * 1_000 + i64::from(datetime.nanoseconds) / 1_000_000
}

fn anyhow_to_workflow_error(err: wasmtime::Error) -> super::workflow_ctx::WorkflowFunctionError {
    err.downcast::<super::workflow_ctx::WorkflowFunctionError>()
        .unwrap_or_else(|err| {
            super::workflow_ctx::WorkflowFunctionError::ConstraintViolation(err.to_string().into())
        })
}

const WORKFLOW_MODULE: &str = r"
const host = (op, args = {}) => Deno.core.ops.op_obelisk_host({ op, args });
export class ChildError extends Error { constructor(value, options = {}) { super(options.message ?? 'child execution failed'); this.name = 'ChildError'; this.value = value; this.childId = options.childId; this.delayId = options.delayId; this.failureKind = options.failureKind; this.cancelled = options.cancelled ?? false; } }
export const ChildExecutionError = ChildError;
export class JoinSetExhaustedError extends Error { constructor(message = 'JoinSetEmpty: all responses processed') { super(message); this.name = 'JoinSetExhaustedError'; this.code = 'OBELISK_JOIN_SET_EXHAUSTED'; } }
const nativeDate = globalThis.Date;
globalThis.Date = class Date extends nativeDate { constructor(...args) { super(...(args.length ? args : [host('now')])); } static now() { return host('now'); } };
Math.random = () => randomU64(0, 1000000) / 1000000;
const format = value => typeof value === 'string' ? value : (() => { try { return JSON.stringify(value); } catch { return String(value); } })();
globalThis.console = Object.fromEntries(['trace', 'debug', 'info', 'log', 'warn', 'error'].map(level => [level, (...values) => host('log', { level: level === 'log' ? 'info' : level, message: values.map(format).join(' ') })]));
const scheduleValue = value => value instanceof nativeDate ? { atMillis: value.getTime() } : value;
export const executionIdCurrent = () => host('executionIdCurrent');
export const executionIdGenerate = () => host('executionIdGenerate');
export const call = (target, params) => unwrapHost(host('call', { target, params }));
export const schedule = (executionId, target, params, schedule) => host('schedule', { executionId, target, params, schedule: scheduleValue(schedule) });
export const sleep = (schedule, name) => new nativeDate(host('sleep', { schedule: scheduleValue(schedule), name }));
export const randomU64 = (min, max) => host('randomU64', { min, max });
export const randomU64Inclusive = (min, max) => host('randomU64Inclusive', { min, max });
export const randomString = (min, max) => host('randomString', { min, max });
export const stub = (executionId, result) => host('stub', { executionId, resultJson: JSON.stringify(result) });
export function createJoinSet(options) {
  const index = host('createJoinSet', typeof options === 'string' ? { name: options } : options ?? {});
  return {
    __index: index,
    get lastId() { return host('lastId', { index }); },
    id() { return host('joinSetId', { index }); },
    submit(target, params = []) { return host('submit', { index, target, params }); },
    __submitTarget(target, params) { return host('submit', { index, target, params }); },
    submitDelay(schedule) { return host('submitDelay', { index, schedule: scheduleValue(schedule) }); },
    joinNext() { const result = host('joinNext', { index }); if (result.exhausted) throw new JoinSetExhaustedError(); return unwrapHost(result); },
    __joinNextFor(target, name) { const result = host('joinNextFor', { index, target }); if (result.allProcessed) throw new JoinSetExhaustedError(); if (result.mismatch) { const actual = result.mismatch.actualTarget ? ` came from ${result.mismatch.actualTarget}` : ' was a delay'; throw new Error(`${name} failed on ${this.id()}: expected a response from ${target}, but the next response ${result.mismatch.actualId}${actual}`); } return unwrapHost(result); },
    joinNextTry() { const result = host('joinNextTry', { index }); if (result.pending) return undefined; if (result.exhausted) throw new JoinSetExhaustedError(); return unwrapHost(result); },
    close() { return host('close', { index }); },
  };
}
export function unwrapHost(result) {
  const options = { childId: result.childId, delayId: result.delayId, failureKind: result.failureKind, cancelled: result.failureKind === 'cancelled', message: result.message };
  if (result.throwUndefined) throw new ChildError(undefined, options);
  if (Object.hasOwn(result, 'throw')) throw new ChildError(result.throw, options);
  return result.ok;
}
globalThis.host = host;
globalThis.unwrapHost = unwrapHost;
globalThis.scheduleTarget = (target, args) => { const executionId = executionIdGenerate(); schedule(executionId, target, args.slice(1), args[0]); return executionId; };
";

const DYNAMIC_MODULE: &str = r"
export const call = (target, params) => unwrapHost(host('call', { target, params }));
export const schedule = (executionId, target, params, scheduleAt) => host('schedule', { executionId, target, params, schedule: scheduleAt });
";
