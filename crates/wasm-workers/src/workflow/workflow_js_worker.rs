//! JS workflow worker that wraps a `WorkflowWorker` running the Boa WASM component.
//!
//! The Boa component exports `obelisk-workflow:workflow-js-runtime/execute.run(fn-name, js-code, params-json) -> result<result<string, string>, js-runtime-error>`.
//! This wrapper translates the user's typed interface `func(params) -> result<T, E>`
//! into calls to the Boa component, deserializing the JSON-encoded ok string as the configured type.

use super::workflow_worker::{
    AdvanceResponse, ReplayError, WorkflowWorker, WorkflowWorkerCompiled,
};
use crate::activity::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use crate::workflow::deadline_tracker::DeadlineTrackerFactory;
use crate::workflow::replay_db_proxy::InternalCapturedWrite;
use crate::workflow::workflow_worker::{AdvanceError, ReplayAdvanceable, ReplayResponse};
use async_trait::async_trait;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::http_client_trace::HttpClientTrace;
use concepts::storage::{CapturedDbWrite, DbPool, Version};
use concepts::time::ClockFn;
use concepts::{
    ComponentId, ComponentType, ExecutionFailureKind, ExecutionId, FinishedExecutionError,
    FunctionFqn, FunctionMetadata, FunctionRegistry, PackageIfcFns, ParameterType, Params,
    ResultParsingError, ResultParsingErrorFromVal, ReturnTypeExtendable,
    SupportedFunctionReturnValue,
};
use executor::worker::{
    FatalError, RunFinished, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};
use utils::wasm_tools::WasmComponent;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};
use wasmtime::Engine;

/// Compiled JS workflow. Holds the compiled Boa WASM component + JS source + user FFQN.
pub struct WorkflowJsWorkerCompiled {
    inner: WorkflowWorkerCompiled,
    js_source: String,
    js_file_name: String,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    /// User interface parsed from synthesized WIT — provides exports, extensions, and WIT text.
    user_wasm_component: WasmComponent,
}

impl WorkflowJsWorkerCompiled {
    pub fn new(
        inner: WorkflowWorkerCompiled,
        js_source: String,
        js_file_name: String,
        user_ffqn: &FunctionFqn,
        user_params: Vec<ParameterType>,
        user_return_type: ReturnTypeExtendable,
    ) -> Result<Self, utils::wasm_tools::DecodeError> {
        let user_wasm_component = WasmComponent::new_from_fn_signature(
            user_ffqn,
            &user_params,
            &user_return_type,
            ComponentType::Workflow,
            "js-workflow",
        )?;
        Ok(Self {
            inner,
            js_source,
            js_file_name,
            user_params,
            user_return_type,
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
    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        self.inner.imported_functions()
    }

    /// Return WIT text describing the user interface including extension packages.
    /// Returns `None` if WIT generation fails (should not happen for valid configs).
    #[must_use]
    pub fn wit(&self) -> String {
        self.user_wasm_component.wit()
    }

    pub fn link(
        self,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Result<WorkflowJsWorkerLinked, crate::WasmFileError> {
        // Resolve JS imports against the function registry before linking.
        // This validates named imports and resolves namespace imports (`import *`).
        // Parse errors in JS source are caught here early rather than at runtime.
        let resolved_imports = resolve_js_imports(&self.js_source, fn_registry.as_ref())
            .map_err(|e| crate::WasmFileError::linking_error("JS import resolution", e))?;

        let linked = self.inner.link(fn_registry)?;
        Ok(WorkflowJsWorkerLinked {
            inner: linked,
            js_source: self.js_source,
            js_file_name: self.js_file_name,
            user_params: self.user_params,
            user_return_type: self.user_return_type,
            user_exports_noext: self.user_wasm_component.exported_functions(false).to_vec(),
            resolved_imports,
        })
    }
}

pub struct WorkflowJsWorkerLinked {
    inner: super::workflow_worker::WorkflowWorkerLinked,
    js_source: String,
    js_file_name: String,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    user_exports_noext: Vec<FunctionMetadata>,
    /// Resolved imports: specifier → [(`js_name`, `wit_name`)].
    resolved_imports: HashMap<String, Vec<(String, String)>>,
}

impl WorkflowJsWorkerLinked {
    pub fn into_worker(
        self,
        deployment_id: DeploymentId,
        db_pool: Arc<dyn DbPool>,
        deadline_factory: Arc<dyn DeadlineTrackerFactory>,
        cancel_registry: CancelRegistry,
        logs_storage_config: Option<LogStrageConfig>,
    ) -> WorkflowJsWorker {
        let inner = self.inner.into_worker(
            deployment_id,
            db_pool,
            deadline_factory,
            cancel_registry,
            logs_storage_config,
        );
        WorkflowJsWorker {
            inner,
            js_source: self.js_source,
            js_file_name: self.js_file_name,
            user_params: self.user_params,
            user_return_type: self.user_return_type,
            user_exports_noext: self.user_exports_noext,
            resolved_imports: self.resolved_imports,
        }
    }
}

pub struct WorkflowJsWorker {
    inner: WorkflowWorker,
    js_source: String,
    js_file_name: String,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    user_exports_noext: Vec<FunctionMetadata>,
    /// Resolved imports: specifier → [(`js_name`, `wit_name`)].
    resolved_imports: HashMap<String, Vec<(String, String)>>,
}

use crate::js_imports::resolve_js_imports;

impl WorkflowJsWorker {
    fn boa_invocation(
        params: &Params,
        js_source: String,
        js_file_name: Option<String>,
        resolved_imports: &HashMap<String, Vec<(String, String)>>,
    ) -> (FunctionFqn, Params) {
        let json_params = params
            .as_json_values()
            .expect("params come from database, not wasmtime");
        let params_json_list: Vec<serde_json::Value> = json_params
            .iter()
            .map(|v| {
                serde_json::Value::String(
                    serde_json::to_string(v).expect("serde_json::Value must be serializable"),
                )
            })
            .collect();

        // Serialize resolved imports as list<tuple<string, list<tuple<string, string>>>>
        let imports_json: Vec<serde_json::Value> = resolved_imports
            .iter()
            .map(|(specifier, funcs)| {
                let funcs_json: Vec<serde_json::Value> = funcs
                    .iter()
                    .map(|(js_name, wit_name)| {
                        serde_json::Value::Array(vec![
                            serde_json::Value::String(js_name.clone()),
                            serde_json::Value::String(wit_name.clone()),
                        ])
                    })
                    .collect();
                serde_json::Value::Array(vec![
                    serde_json::Value::String(specifier.clone()),
                    serde_json::Value::Array(funcs_json),
                ])
            })
            .collect();

        let ffqn =
            FunctionFqn::new_static_tuple(("obelisk-workflow:workflow-js-runtime/execute", "run"));
        let boa_params: Arc<[serde_json::Value]> = Arc::from([
            serde_json::Value::String(js_source),
            serde_json::Value::Array(params_json_list),
            js_file_name.map_or(serde_json::Value::Null, serde_json::Value::String),
            serde_json::Value::Array(imports_json),
        ]);
        let params = Params::from_json_values(
            boa_params,
            [
                &TypeWrapper::String,
                &TypeWrapper::List(Box::new(TypeWrapper::String)),
                &TypeWrapper::Option(Box::new(TypeWrapper::String)),
                &TypeWrapper::List(Box::new(TypeWrapper::Tuple(Box::new([
                    TypeWrapper::String,
                    TypeWrapper::List(Box::new(TypeWrapper::Tuple(Box::new([
                        TypeWrapper::String,
                        TypeWrapper::String,
                    ])))),
                ])))),
            ]
            .into_iter(),
        )
        .expect("types checked at compile time");
        (ffqn, params)
    }
}

#[async_trait]
impl Worker for WorkflowJsWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    async fn run(&self, mut ctx: WorkerContext) -> WorkerResult {
        assert_eq!(
            self.user_params.len(),
            ctx.params
                .as_json_values()
                .expect("params come from database, not wasmtime")
                .len(),
            "type checked in Params::from_json_values"
        );
        (ctx.ffqn, ctx.params) = Self::boa_invocation(
            &ctx.params,
            self.js_source.clone(),
            Some(self.js_file_name.clone()),
            &self.resolved_imports,
        );

        let inner_worker_ok = self.inner.run(ctx).await?;
        debug!("Workflow worker returned {inner_worker_ok:?}");

        match inner_worker_ok {
            WorkerResultOk::DbUpdatedByWorkerOrWatcher => {
                Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher)
            }
            WorkerResultOk::RunFinished(RunFinished {
                retval,
                version,
                http_client_traces,
            }) => transform_to_outer_result(
                retval,
                version,
                http_client_traces,
                &self.user_return_type,
            )
            .map(WorkerResultOk::RunFinished)
            .map_err(|(err, version)| WorkerError::FatalError(err, version)),
        }
    }
}

/// Transform `result<result<string, string>, js-runtime-error>` returned by `workflow-js-runtime`
/// to user specified `user_return_type`.
fn transform_to_outer_result(
    retval: SupportedFunctionReturnValue,
    version: Version,
    http_client_traces: Option<Vec<HttpClientTrace>>,
    user_return_type: &ReturnTypeExtendable,
) -> Result<RunFinished, (FatalError, Version)> {
    match retval {
        SupportedFunctionReturnValue::Ok(Some(WastValWithType {
            r#type:
                TypeWrapper::Result {
                    ok: Some(ok_type),
                    err: Some(err_type),
                },
            value: WastVal::Result(Ok(Some(ok_val))),
        })) => {
            assert!(*ok_type == TypeWrapper::String && *err_type == TypeWrapper::String);
            let WastVal::String(ok_val) = *ok_val else {
                unreachable!("ok type is String, so value must be WastVal::String")
            };
            let Ok(ok_val) = serde_json::from_str(&ok_val) else {
                unreachable!("workflow-js-runtime always sends JSON-encoded string")
            };
            let retval = crate::js_worker_utils::map_ok_variant_fatal(
                Some(ok_val),
                user_return_type,
                version.clone(),
            )?;
            Ok(RunFinished {
                retval,
                version,
                http_client_traces,
            })
        }

        SupportedFunctionReturnValue::Ok(Some(WastValWithType {
            r#type:
                TypeWrapper::Result {
                    ok: Some(ok_type),
                    err: Some(err_type),
                },
            value: WastVal::Result(Err(Some(err_val))),
        })) => {
            assert!(*ok_type == TypeWrapper::String && *err_type == TypeWrapper::String);
            let WastVal::String(err_val) = *err_val else {
                unreachable!("err type is String, so value must be WastVal::String")
            };
            let Ok(err_val) = serde_json::from_str(&err_val) else {
                unreachable!("workflow-js-runtime always sends JSON-encoded string")
            };
            let retval = crate::js_worker_utils::map_err_variant_fatal(
                Some(err_val),
                user_return_type,
                version.clone(),
            )?;
            Ok(RunFinished {
                retval,
                version,
                http_client_traces,
            })
        }

        SupportedFunctionReturnValue::Err(Some(js_runtime_err)) => {
            // Map JsRuntimeError variants to appropriate WorkerError
            let WastVal::Variant(variant_name, payload) = &js_runtime_err.value else {
                unreachable!("expected Variant for js-runtime-error")
            };
            let name = variant_name.as_snake_str();
            match name {
                "wrong_return_type" | "wrong_thrown_type" => {
                    let reason = if let Some(payload) = payload
                        && let WastVal::String(s) = payload.as_ref()
                    {
                        s.clone()
                    } else {
                        unreachable!("both variants have string payload")
                    };

                    Err((
                        FatalError::ResultParsingError(
                            ResultParsingError::ResultParsingErrorFromVal(
                                ResultParsingErrorFromVal::TypeCheckError(reason),
                            ),
                        ),
                        version,
                    ))
                }
                "module_parse_error" | "no_default_export" => {
                    let detail = payload.as_ref().and_then(|p| {
                        if let WastVal::String(s) = p.as_ref() {
                            Some(s.clone())
                        } else {
                            None
                        }
                    });
                    Err((
                        FatalError::CannotInstantiate {
                            reason: format!("js-runtime-error: {name}"),
                            detail,
                        },
                        version,
                    ))
                }
                "execution_failed" => {
                    // This variant is returned when a workflow function fails,
                    // e.g., when joinNext returns an error from a child execution.
                    // We propagate this as an ExecutionError.
                    Ok(RunFinished {
                        retval: SupportedFunctionReturnValue::ExecutionError(
                            FinishedExecutionError {
                                kind: ExecutionFailureKind::Uncategorized,
                                reason: Some("js-runtime execution-failed".to_string()),
                                detail: None,
                            },
                        ),
                        version,
                        http_client_traces,
                    })
                }
                _ => unreachable!("unexpected js-runtime-error variant: {name}"),
            }
        }

        retval @ SupportedFunctionReturnValue::ExecutionError(_) => Ok(RunFinished {
            retval,
            version,
            http_client_traces,
        }),

        other => unreachable!("unexpected SupportedFunctionReturnValue: {other:?}"),
    }
}

fn transform_to_append_finished(
    retval: SupportedFunctionReturnValue,
    version: &Version,
    user_return_type: &ReturnTypeExtendable,
) -> (SupportedFunctionReturnValue, Option<FatalError>) {
    let (retval, version_obtained, fatal_error) =
        match transform_to_outer_result(retval, version.clone(), None, user_return_type) {
            Ok(RunFinished {
                retval, version, ..
            }) => (retval, version, None),
            Err((fatal_error, version)) => {
                let retval = SupportedFunctionReturnValue::ExecutionError(
                    FinishedExecutionError::from(&fatal_error),
                );
                (retval, version, Some(fatal_error))
            }
        };
    assert_eq!(*version, version_obtained);
    (retval, fatal_error)
}

impl WorkflowJsWorker {
    /// Replay a JS workflow execution for debugging/verification.
    ///
    /// This function recreates the workflow execution from the database log,
    /// transforming the context to call the workflow-js-runtime just like the
    /// regular `run` method does.
    #[allow(clippy::too_many_arguments)]
    pub async fn replay(
        deployment_id: DeploymentId,
        component_id: ComponentId,
        wasmtime_component: wasmtime::component::Component,
        exim: &utils::wasm_tools::ExIm,
        engine: Arc<Engine>,
        fn_registry: Arc<dyn FunctionRegistry>,
        real_db_pool: Arc<dyn DbPool>,
        execution_id: ExecutionId,
        logs_storage_config: Option<LogStrageConfig>,
        clock_fn: Box<dyn ClockFn>,
        js_source: String,
        user_return_type: &ReturnTypeExtendable,
    ) -> Result<ReplayResponse, ReplayError> {
        let db_conn = real_db_pool
            .connection()
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let already_finished_result = log.as_finished_result();
        let resolved_imports =
            resolve_js_imports(&js_source, fn_registry.as_ref()).unwrap_or_default();
        let (ffqn, params) = Self::boa_invocation(log.params(), js_source, None, &resolved_imports);

        let (captured_writes, mut fatal_error) = WorkflowWorker::capture_replay_writes_from_log(
            deployment_id,
            component_id,
            wasmtime_component,
            exim,
            engine,
            fn_registry,
            real_db_pool,
            execution_id,
            logs_storage_config,
            clock_fn,
            log,
            ffqn,
            params,
        )
        .await?;
        // Remove side effects, unwrapping user retval or fatal error.
        let captured_writes: Vec<_> = captured_writes
            .into_iter()
            .map(|internal_write| {
                let write = internal_write.write;
                match write {
                    CapturedDbWrite::AppendFinished {
                        execution_id,
                        version,
                        current_time,
                        retval, // workflow-js-runtime WASM result
                    } => {
                        let (retval, fatal_error_from_wit) =
                            transform_to_append_finished(retval, &version, user_return_type);
                        if fatal_error_from_wit.is_some() {
                            // TODO: can both fatal errors be present?
                            fatal_error = fatal_error_from_wit;
                        }
                        CapturedDbWrite::AppendFinished {
                            execution_id,
                            version,
                            current_time,
                            retval,
                        }
                    }
                    _ => write,
                }
            })
            .collect();

        WorkflowWorker::replay_response(captured_writes, fatal_error, already_finished_result)
    }

    /// Advance a paused JS workflow by one interrupt boundary.
    #[allow(clippy::too_many_arguments)]
    pub async fn advance(
        deployment_id: DeploymentId,
        component_id: ComponentId,
        wasmtime_component: wasmtime::component::Component,
        exim: &utils::wasm_tools::ExIm,
        engine: Arc<Engine>,
        fn_registry: Arc<dyn FunctionRegistry>,
        cancel_registry: CancelRegistry,
        real_db_pool: Arc<dyn DbPool>,
        execution_id: ExecutionId,
        logs_storage_config: Option<LogStrageConfig>,
        clock_fn: Box<dyn ClockFn>,
        js_source: String,
        user_return_type: &ReturnTypeExtendable,
        requested: ReplayAdvanceable,
    ) -> Result<AdvanceResponse, AdvanceError> {
        info!("Advance to requested {requested:?}");
        let db_conn = real_db_pool
            .connection()
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        if requested.captured_writes.is_empty() {
            return Err(crate::workflow::workflow_worker::AdvanceError::NoWrites);
        }
        if let Some(expected_version) = requested.starting_version()
            && log.next_version != *expected_version
        {
            return Err(AdvanceError::VersionMismatch {
                expected: log.next_version,
            });
        }

        let old_version = log.next_version.clone();
        let resolved_imports =
            resolve_js_imports(&js_source, fn_registry.as_ref()).unwrap_or_default();
        let (ffqn, params) = Self::boa_invocation(log.params(), js_source, None, &resolved_imports);
        let log_forwarder_sender = logs_storage_config
            .as_ref()
            .map(|config| &config.log_sender);
        let (mut fresh_replay, _fatal_error) = WorkflowWorker::capture_replay_writes_from_log(
            deployment_id,
            component_id,
            wasmtime_component,
            exim,
            engine,
            fn_registry,
            real_db_pool,
            execution_id,
            logs_storage_config.clone(),
            clock_fn,
            log,
            ffqn,
            params,
        )
        .await?;
        if let Some(InternalCapturedWrite {
            write:
                CapturedDbWrite::AppendFinished {
                    retval, version, ..
                },
            ..
        }) = fresh_replay.last_mut()
        {
            let (retval_transformed, _fatal_error_from_wit) =
                transform_to_append_finished(retval.clone(), version, user_return_type);
            *retval = retval_transformed;
        }
        WorkflowWorker::advance_from_log(
            &*db_conn,
            &cancel_registry,
            log_forwarder_sender,
            requested,
            fresh_replay,
            old_version,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RunnableComponent;
    use crate::activity::activity_worker::test::compile_activity;
    use crate::activity::activity_worker::tests::new_activity_fibo;
    use crate::engines::{EngineConfig, Engines};
    use crate::testing_fn_registry::TestingFnRegistry;
    use crate::workflow::deadline_tracker::DeadlineTrackerFactoryTokio;
    use crate::workflow::workflow_worker::{JoinNextBlockingStrategy, WorkflowConfig};
    use assert_matches::assert_matches;
    use chrono::DateTime;
    use concepts::component_id::{COMPONENT_DIGEST_DUMMY, ComponentDigest};
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, DelayId, ExecutorId, RunId};
    use concepts::storage::{
        CapturedDbWrite, CreateRequest, DbConnectionTest, DbPool, DbPoolCloseable,
        ExecutionRequest, HistoryEvent, JoinSetRequest, JoinSetResponse, Locked, LogEntry,
        LogInfoAppendRow, LogLevel, PendingState, PendingStateFinished, PendingStateFinishedError,
        PendingStateFinishedResultKind, PendingStatePendingAt, Version,
    };
    use concepts::time::{ClockFn, Now, TokioSleep};
    use concepts::{
        ComponentRetryConfig, ComponentType, ExecutionId, ExecutionMetadata, StrVariant,
        TypeWrapperTopLevel,
    };
    use db_mem::inmemory_dao::InMemoryPool;
    use db_tests::Database;
    use executor::executor::{ExecConfig, ExecTask, LockingStrategy};
    use executor::worker::{WorkerContext, WorkerError, WorkerResultOk};
    use executor::{expired_timers_watcher, worker::Worker};
    use insta::assert_json_snapshot;
    use rstest::rstest;
    use serde_json::json;
    use std::str::FromStr as _;
    use std::time::Duration;
    use test_db_macro::expand_enum_database;
    use test_utils::sim_clock::SimClock;
    use test_utils::{ExecutionLogSanitized, redact_component_digest};
    use tokio::sync::mpsc;
    use tracing::{info, info_span};
    use utils::sha256sum::calculate_sha256_file;
    use val_json::wast_val::WastVal;
    use wasmtime::Engine;

    const FIBO_10_OUTPUT: u64 = 55;

    fn drain_forwarded_log_messages(
        receiver: &mut mpsc::Receiver<LogInfoAppendRow>,
    ) -> Vec<String> {
        let mut messages = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(LogInfoAppendRow {
                    log_entry: LogEntry::Log { message, .. },
                    ..
                }) => messages.push(message),
                Ok(_) => {}
                Err(
                    tokio::sync::mpsc::error::TryRecvError::Empty
                    | tokio::sync::mpsc::error::TryRecvError::Disconnected,
                ) => break,
            }
        }
        messages
    }

    fn default_return_type() -> ReturnTypeExtendable {
        ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::String)),
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("result<string, string>"),
        }
    }

    fn new_js_workflow_worker_with_return_type(
        js_source: &str,
        user_ffqn: &FunctionFqn,
        return_type: ReturnTypeExtendable,
    ) -> Arc<dyn Worker> {
        let engine = Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let cancel_registry = CancelRegistry::new();
        let clock_fn: Box<dyn ClockFn> = Now.clone_box();

        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap();

        // Compile the Boa WASM component
        let wasm_path = workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME;
        let runnable_component =
            RunnableComponent::new(wasm_path, &engine, component_id.component_type).unwrap();

        let config = WorkflowConfig {
            component_id: component_id.clone(),
            join_next_blocking_strategy: JoinNextBlockingStrategy::Interrupt,
            backtrace_persist: false,
            stub_wasi: false,
            fuel: None,
            lock_extension: Some(Duration::from_secs(1)),
            subscription_interruption: None,
        };

        let compiled = WorkflowWorkerCompiled::new_with_config(
            runnable_component.clone(),
            config,
            engine,
            clock_fn.clone_box(),
        )
        .unwrap();

        let js_compiled = WorkflowJsWorkerCompiled::new(
            compiled,
            js_source.to_string(),
            String::new(),
            user_ffqn,
            vec![ParameterType {
                type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
                name: StrVariant::Static("params"),
                wit_type: StrVariant::Static("list<string>"),
            }],
            return_type,
        )
        .unwrap();

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        let linked = js_compiled.link(fn_registry).unwrap();

        let db_pool = Arc::new(InMemoryPool::new());
        let deadline_factory = Arc::new(DeadlineTrackerFactoryTokio::new(Duration::ZERO, clock_fn));

        Arc::new(linked.into_worker(
            DEPLOYMENT_ID_DUMMY,
            db_pool,
            deadline_factory,
            cancel_registry,
            None,
        ))
    }

    fn new_js_workflow_worker(js_source: &str, user_ffqn: &FunctionFqn) -> Arc<dyn Worker> {
        new_js_workflow_worker_with_return_type(js_source, user_ffqn, default_return_type())
    }

    /// Like `new_js_workflow_worker` but returns the Result from `link()` for testing error cases.
    fn try_link_js_workflow_worker(
        js_source: &str,
        user_ffqn: &FunctionFqn,
    ) -> Result<WorkflowJsWorkerLinked, crate::WasmFileError> {
        let engine = Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let clock_fn: Box<dyn ClockFn> = Now.clone_box();

        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap();

        let wasm_path = workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME;
        let runnable_component =
            RunnableComponent::new(wasm_path, &engine, component_id.component_type).unwrap();

        let config = WorkflowConfig {
            component_id,
            join_next_blocking_strategy: JoinNextBlockingStrategy::Interrupt,
            backtrace_persist: false,
            stub_wasi: false,
            fuel: None,
            lock_extension: None,
            subscription_interruption: None,
        };

        let compiled =
            WorkflowWorkerCompiled::new_with_config(runnable_component, config, engine, clock_fn)
                .unwrap();

        let js_compiled = WorkflowJsWorkerCompiled::new(
            compiled,
            js_source.to_string(),
            String::new(),
            user_ffqn,
            vec![ParameterType {
                type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
                name: StrVariant::Static("params"),
                wit_type: StrVariant::Static("list<string>"),
            }],
            default_return_type(),
        )
        .unwrap();

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        js_compiled.link(fn_registry)
    }

    fn make_worker_context(ffqn: FunctionFqn, params: &[String]) -> WorkerContext {
        // The user function signature is: func(params: list<string>) -> result<string, string>
        // So we wrap the params in a list
        let params_json: Vec<serde_json::Value> = vec![json!(params)];
        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap();
        WorkerContext {
            execution_id: ExecutionId::generate(),
            metadata: ExecutionMetadata::empty(),
            ffqn,
            params: Params::from_json_values_test(params_json),
            event_history: Vec::new(),
            responses: Vec::new(),
            version: Version::new(0),
            can_be_retried: true,
            worker_span: info_span!("js_workflow_test"),
            locked_event: Locked {
                component_id,
                executor_id: ExecutorId::generate(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                run_id: RunId::generate(),
                lock_expires_at: chrono::Utc::now() + chrono::Duration::seconds(60),
                retry_config: ComponentRetryConfig::WORKFLOW,
            },
            executor_close_watcher: tokio::sync::watch::channel(false).1,
        }
    }

    fn extract_string(val: &WastVal) -> String {
        match val {
            WastVal::String(s) => s.clone(),
            other => panic!("expected string, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn workflow_js_simple_return() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            export default function hello() {
                return "hello world";
            }
        "#;

        let worker = new_js_workflow_worker(js_source, &ffqn);
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "hello world");
    }

    #[tokio::test]
    async fn async_fn_should_fail() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            export default async function hello() {
                return "hello world";
            }
        "#;

        let worker = new_js_workflow_worker(js_source, &ffqn);
        let ctx = make_worker_context(ffqn, &[]);

        let err = worker.run(ctx).await.unwrap_err();
        assert_matches!(
            err,
            WorkerError::FatalError(
                FatalError::ResultParsingError(ResultParsingError::ResultParsingErrorFromVal(
                    ResultParsingErrorFromVal::TypeCheckError(reason),
                )),
                _version,
            ) => {
                assert_eq!(
                    "failed to type check the ok variant value `{}` as type string - invalid type: map, expected value matching \"string\" at line 1 column 2",
                    reason,
                );
            }
        );
    }

    #[tokio::test]
    async fn workflow_js_with_params() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "greet");
        let js_source = r#"
            export default function greet(params) {
                let name = params[0];
                let greeting = params[1];
                return greeting + ", " + name + "!";
            }
        "#;

        let worker = new_js_workflow_worker(js_source, &ffqn);
        let ctx = make_worker_context(ffqn, &["World".to_string(), "Hello".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "Hello, World!");
    }

    #[tokio::test]
    async fn workflow_js_with_throw_string() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fail");
        let js_source = r#"
            export default function fail() {
                throw "something went wrong";
            }
        "#;

        let worker = new_js_workflow_worker(js_source, &ffqn);
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        // For result<string, string>, a throw becomes Err
        let err_val = assert_matches!(retval, SupportedFunctionReturnValue::Err(err) => err);
        let err_val = err_val.expect("should have err value");
        assert_eq!(extract_string(&err_val.value), "something went wrong");
    }

    #[tokio::test]
    async fn workflow_js_syntax_error_should_fail_to_link() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "broken");
        let js_source = r"
            export default function broken( {
                return 'this has a syntax error';
            }
        ";

        match try_link_js_workflow_worker(js_source, &ffqn) {
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("parse error"), "error: {msg}");
            }
            Ok(_) => panic!("linking should fail for JS with syntax errors"),
        }
    }

    #[tokio::test]
    async fn workflow_js_no_default_export() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "missing");
        let js_source = r"
            export function some_other_function() {
                return 'hello';
            }
        ";

        let worker = new_js_workflow_worker(js_source, &ffqn);
        let ctx = make_worker_context(ffqn, &[]);

        let err = worker.run(ctx).await.unwrap_err();
        assert_matches!(
            err,
            WorkerError::FatalError(
                FatalError::CannotInstantiate { reason, detail: _ },
                _version,
            ) => {
                assert!(reason.contains("no_default_export"), "reason: {reason}");
            }
        );
    }

    // ==================== Workflow function tests ====================

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    async fn compile_js_workflow_worker(
        js_source: &str,
        user_ffqn: &FunctionFqn,
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        fn_registry: Arc<dyn FunctionRegistry>,
        workflow_engine: Arc<Engine>,
    ) -> (WorkflowJsWorker, concepts::ComponentId, RunnableComponent) {
        let wasm_path = workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME;
        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            ComponentDigest(calculate_sha256_file(wasm_path).await.unwrap().0),
        )
        .unwrap();

        let runnable_component =
            RunnableComponent::new(wasm_path, &workflow_engine, component_id.component_type)
                .unwrap();

        let config = WorkflowConfig {
            component_id: component_id.clone(),
            join_next_blocking_strategy: JoinNextBlockingStrategy::Interrupt,
            backtrace_persist: false,
            stub_wasi: false,
            fuel: None,
            lock_extension: None,
            subscription_interruption: None,
        };

        let compiled = WorkflowWorkerCompiled::new_with_config(
            runnable_component.clone(),
            config,
            workflow_engine,
            clock_fn.clone_box(),
        )
        .unwrap();

        let js_compiled = WorkflowJsWorkerCompiled::new(
            compiled,
            js_source.to_string(),
            String::new(),
            user_ffqn,
            vec![ParameterType {
                type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
                name: StrVariant::Static("params"),
                wit_type: StrVariant::Static("list<string>"),
            }],
            default_return_type(),
        )
        .unwrap();

        let linked = js_compiled.link(fn_registry).unwrap();

        let deadline_factory = Arc::new(DeadlineTrackerFactoryTokio::new(Duration::ZERO, clock_fn));

        (
            linked.into_worker(
                DEPLOYMENT_ID_DUMMY,
                db_pool,
                deadline_factory,
                CancelRegistry::new(),
                None,
            ),
            component_id,
            runnable_component,
        )
    }

    fn new_js_workflow_exec_task(
        worker: WorkflowJsWorker,
        clock_fn: Box<dyn ClockFn>,
        db_pool: Arc<dyn DbPool>,
    ) -> ExecTask {
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            component_id: worker.inner.config.component_id.clone(),
            task_limiter_global: None,
            task_limiter_local: None,
            executor_id: ExecutorId::generate(),
            retry_config: ComponentRetryConfig::WORKFLOW,
            locking_strategy: LockingStrategy::ByComponentDigest,
        };
        ExecTask::new_all_ffqns_test(Arc::new(worker), exec_config, clock_fn, db_pool)
    }

    fn new_js_workflow_exec_task_with_close_watcher(
        worker: WorkflowJsWorker,
        clock_fn: Box<dyn ClockFn>,
        db_pool: Arc<dyn DbPool>,
        close_watcher: tokio::sync::watch::Receiver<bool>,
    ) -> ExecTask {
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            component_id: worker.inner.config.component_id.clone(),
            task_limiter_global: None,
            task_limiter_local: None,
            executor_id: ExecutorId::generate(),
            retry_config: ComponentRetryConfig::WORKFLOW,
            locking_strategy: LockingStrategy::ByComponentDigest,
        };
        ExecTask::new_all_ffqns_test_with_close_watcher(
            Arc::new(worker),
            exec_config,
            clock_fn,
            db_pool,
            close_watcher,
        )
    }

    /// Test: joinNextTry successfully finds a delay response after it expires
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_join_next_try_found(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_delay(params) {
            const js = obelisk.createJoinSet();
            const delayId = js.submitDelay({ milliseconds: 10 });
            obelisk.sleep({ milliseconds: 20 });
            const response = js.joinNextTry();
            const afterResponse = js.joinNextTry();
            return JSON.stringify({
                responseType: response.type,
                responseOk: response.ok,
                afterStatus: afterResponse.status
            });
        }";

        let harness =
            JsWorkflowTestHarness::with_no_activities(db_pool, js_source, "test-delay").await;
        harness.tick().await; // blocks on sleep
        harness.advance_time(Duration::from_millis(30)).await;
        harness.tick().await; // completes

        let result = harness.get_result_json().await;
        assert_eq!(json!("delay"), result["responseType"]);
        assert_eq!(json!(true), result["responseOk"]);
        assert_eq!(json!("allProcessed"), result["afterStatus"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: JS workflow exercises all workflow-support APIs
    /// - createJoinSet (with and without name)
    /// - joinSet.submit (calls fibo activity)
    /// - joinSet.joinNext
    /// - joinSet.joinNextTry
    /// - obelisk.getResult
    /// - obelisk.randomU64, randomU64Inclusive, randomString
    /// - joinSet.submitDelay
    /// - joinSet.close
    /// - console logging
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_all_apis(
        database: Database,
        #[values(false, true)] activity_should_win: bool,
        #[values(false, true)] explicit_close: bool,
    ) {
        let (_guard, db_pool, db_close) = database.set_up().await;
        Box::pin(workflow_js_all_apis_inner(
            db_pool.clone(),
            activity_should_win,
            explicit_close,
        ))
        .await;
        db_close.close().await;
    }

    async fn workflow_js_all_apis_inner(
        db_pool: Arc<dyn DbPool>,
        activity_should_win: bool,
        explicit_close: bool,
    ) {
        test_utils::set_up();
        let sim_clock = SimClock::epoch();

        // JS code that exercises all workflow-support APIs
        let js_source = r"
        export default function test_all_apis(params) {
            console.log('Starting comprehensive API test');
            const explicit_close = params[0] === 'true';

            /* Test random functions */
            const rand1 = obelisk.randomU64(0, 10);
            const rand2 = obelisk.randomU64Inclusive(1, 10);
            const randStr = obelisk.randomString(5, 10);
            console.debug('Random values:', Number(rand1), Number(rand2), randStr);

            /* Test createJoinSet (unnamed) */
            const js1 = obelisk.createJoinSet();
            console.log('Created unnamed join set:', js1.id());

            /* Test createJoinSet (named) */
            const js2 = obelisk.createJoinSet({ name: 'my-named-set' });
            console.log('Created named join set:', js2.id());

            /* Test joinNextTry on empty join set - should return allProcessed */
            const tryEmpty = js2.joinNextTry();
            console.log('joinNextTry on empty:', JSON.stringify(tryEmpty));

            /* Submit fibo(10) activity call */
            const fiboFfqn = 'testing:fibo/fibo.fibo';
            const execId = js1.submit(fiboFfqn, [10]);
            console.log('Submitted fibo(10), execId:', execId);

            /* Submit a delay */
            const delayId = js1.submitDelay({ milliseconds: 100 });
            console.log('Submitted delay, delayId:', delayId);

            /* Test joinNextTry before any response is ready - should return pending */
            const tryPending = js1.joinNextTry();
            console.log('joinNextTry pending:', JSON.stringify(tryPending));

            /* Join next - should get fibo result first (activity completes before delay) */
            const response1 = js1.joinNext();
            console.log('joinNext response 1:', JSON.stringify(response1));

            /* Get the fibo result */
            let fiboResult = null;
            let loser = null;
            if (response1.type === 'execution') {
                const result = obelisk.getResult(response1.id);
                console.log('Got fibo result:', JSON.stringify(result));
                fiboResult = result.ok;
                loser = delayId;
            } else {
                loser = execId;
            }
            if (explicit_close) {
                js1.close();
                js2.close();
            }
            console.log('all done');
            return JSON.stringify({
                rand1InRange: rand1 >= 0n && rand1 < 10n,
                rand2InRange: rand2 >= 1n && rand2 <= 10n,
                randStrLenOk: randStr.length >= 5 && randStr.length < 10,
                fiboResult: fiboResult,
                response1Type: response1.type,
                loser,
                joinNextTryEmpty: tryEmpty.status,
                joinNextTryPending: tryPending.status
            });
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test-all-apis");

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
        ]);

        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;

        let workflow_exec =
            new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params = Params::from_json_values_test(vec![json!(vec![if explicit_close {
            "true"
        } else {
            "false"
        }])]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params,
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        info!("Step 1: Run workflow until blocked by join set (waiting for activity)");
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );

        assert_matches!(
            db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap()
                .pending_state,
            PendingState::BlockedByJoinSet(..)
        );

        if activity_should_win {
            info!("Step 2: Run activity to complete the fibo(10) child execution");
            let activity_exec = new_activity_fibo(
                db_pool.clone(),
                sim_clock.clone_box(),
                TokioSleep,
                LockingStrategy::ByComponentDigest,
            )
            .await;
            let executed_activities = activity_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await;
            assert_eq!(1, executed_activities.len());
        } else {
            info!("Step 2: Move time forward to complete the delay");
            sim_clock.move_time_forward(Duration::from_millis(200));
            {
                let timer =
                    expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
                        .await
                        .unwrap();
                assert_eq!(1, timer.expired_async_timers);
            }
        }

        info!("Step 3: Resume workflow - should process the winner");
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );

        info!("Step 4: Resume workflow - should close the join set");
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );

        let res = db_connection
            .get_finished_result(&execution_id)
            .await
            .unwrap();
        info!("Got result: {res:?}");

        // Verify results
        let ok_val = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val);
        let json_str = assert_matches!(&ok_val.value, WastVal::String(s) => s);
        let result: serde_json::Value = serde_json::from_str(json_str).unwrap();

        assert_eq!(
            json!(true),
            result["rand1InRange"],
            "rand1 should be in range [0, 100)"
        );
        assert_eq!(
            json!(true),
            result["rand2InRange"],
            "rand2 should be in range [1, 10]"
        );
        assert_eq!(
            json!(true),
            result["randStrLenOk"],
            "random string length should be in range [5, 10)"
        );
        assert_eq!(
            json!("allProcessed"),
            result["joinNextTryEmpty"],
            "joinNextTry on empty join set should return allProcessed"
        );
        assert_eq!(
            json!("pending"),
            result["joinNextTryPending"],
            "joinNextTry before response ready should return pending"
        );
        if activity_should_win {
            assert_eq!(
                json!("execution"),
                result["response1Type"],
                "first response should be execution"
            );
            assert_eq!(
                json!(FIBO_10_OUTPUT),
                result["fiboResult"],
                "fibo(10) should be 55"
            );
        } else {
            assert_eq!(
                json!("delay"),
                result["response1Type"],
                "first response should be delay"
            );
        }
        // check that the loser is cancelled.
        if activity_should_win {
            let delay_id = DelayId::from_str(result["loser"].as_str().unwrap()).unwrap();
            let responses = db_connection.get(&execution_id).await.unwrap().responses;
            let resp = responses
                .iter()
                .find_map(|resp| {
                    if let JoinSetResponse::DelayFinished {
                        delay_id: found,
                        result,
                    } = &resp.event.event.event
                        && *found == delay_id
                    {
                        Some(result)
                    } else {
                        None
                    }
                })
                .unwrap();
            assert!(resp.is_err());
        } else {
            let child_id = ExecutionId::from_str(result["loser"].as_str().unwrap()).unwrap();
            let state = db_connection
                .get_pending_state(&child_id)
                .await
                .unwrap()
                .pending_state;
            assert_matches!(
                state,
                PendingState::Finished(PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind::Err(
                        PendingStateFinishedError::ExecutionFailure(
                            ExecutionFailureKind::Cancelled
                        )
                    ),
                    ..
                })
            );
        }

        let stopwatch = std::time::Instant::now();
        let (log_sender, mut log_storage_recv) = mpsc::channel(100);
        WorkflowJsWorker::replay(
            DeploymentId::generate(),
            workflow_exec.config.component_id.clone(),
            runnable_component.wasmtime_component,
            &runnable_component.wasm_component.exim,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            execution_id,
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender,
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();
        info!("Replayed in {:?}", stopwatch.elapsed());
        // Nothing should be added to logs
        let mut buffer = Vec::new();
        let received = log_storage_recv.recv_many(&mut buffer, 100).await;
        assert_eq!(0, received, "expected no new messages, got {buffer:?}");
    }

    // ==================== JS Workflow test harness ====================

    /// What activities to register for the test.
    enum TestActivities {
        None,
        Stub,
    }

    /// Helper for running JS workflow tests with reduced boilerplate.
    struct JsWorkflowTestHarness {
        workflow_exec: ExecTask,
        execution_id: ExecutionId,
        db_connection: Box<dyn DbConnectionTest>,
        sim_clock: SimClock,
    }

    impl JsWorkflowTestHarness {
        /// Create harness with stub activity registered.
        async fn with_stub_activity(
            db_pool: Arc<dyn DbPool>,
            js_source: &str,
            fn_name: &'static str,
        ) -> Self {
            Self::new(db_pool, js_source, fn_name, TestActivities::Stub).await
        }

        /// Create harness with no activities registered.
        async fn with_no_activities(
            db_pool: Arc<dyn DbPool>,
            js_source: &str,
            fn_name: &'static str,
        ) -> Self {
            Self::new(db_pool, js_source, fn_name, TestActivities::None).await
        }

        async fn new(
            db_pool: Arc<dyn DbPool>,
            js_source: &str,
            fn_name: &'static str,
            activities: TestActivities,
        ) -> Self {
            use crate::activity::activity_worker::test::compile_activity_stub;

            let sim_clock = SimClock::epoch();
            let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", fn_name);

            let components = match activities {
                TestActivities::None => vec![],
                TestActivities::Stub => vec![
                    compile_activity_stub(
                        test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY,
                    )
                    .await,
                ],
            };
            let fn_registry: Arc<dyn FunctionRegistry> =
                TestingFnRegistry::new_from_components(components);

            let workflow_engine =
                Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let (worker, component_id, _runnable_component) = compile_js_workflow_worker(
                js_source,
                &user_ffqn,
                db_pool.clone(),
                sim_clock.clone_box(),
                fn_registry,
                workflow_engine,
            )
            .await;

            let workflow_exec =
                new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());

            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection_test().await.unwrap();

            let params = Params::from_json_values_test(vec![json!(Vec::<String>::new())]);
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: user_ffqn,
                    params,
                    parent: None,
                    metadata: ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id,
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    scheduled_by: None,
                    paused: false,
                })
                .await
                .unwrap();

            Self {
                workflow_exec,
                execution_id,
                db_connection,
                sim_clock,
            }
        }

        async fn tick(&self) {
            self.workflow_exec
                .tick_test_await(self.sim_clock.now(), RunId::generate())
                .await;
        }

        /// Move time forward and process expired timers.
        async fn advance_time(&self, duration: Duration) {
            self.sim_clock.move_time_forward(duration);
            expired_timers_watcher::tick_test(self.db_connection.as_ref(), self.sim_clock.now())
                .await
                .unwrap();
        }

        async fn get_result_json(&self) -> serde_json::Value {
            let res = self
                .db_connection
                .get_finished_result(&self.execution_id)
                .await
                .unwrap();
            let ok_val = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val);
            let json_str = assert_matches!(&ok_val.value, WastVal::String(s) => s);
            serde_json::from_str(json_str).unwrap()
        }
    }

    // ==================== Workflow tests ====================

    /// Test: JS workflow uses `obelisk.stub()` to stub an `activity_stub` execution.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-param']);
            obelisk.stub(execId, {'ok': 'stubbed-result-42'});
            const response = js.joinNext();
            const result = obelisk.getResult(execId);
            return JSON.stringify({
                responseType: response.type,
                responseOk: response.ok,
                result: result
            });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub").await;
        harness.tick().await; // submit, stub, block on joinNext
        harness.tick().await; // resume, complete

        let result = harness.get_result_json().await;
        assert_eq!(json!("execution"), result["responseType"]);
        assert_eq!(json!(true), result["responseOk"]);
        assert_eq!(json!("stubbed-result-42"), result["result"]["ok"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stub with error response (`result<string>` has no error type, so err is null).
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_with_error(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_err(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-param']);
            obelisk.stub(execId, {'err': null}); // result<string> has no error type
            const response = js.joinNext();
            const result = obelisk.getResult(execId);
            return JSON.stringify({ responseOk: response.ok, result: result });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-err").await;
        harness.tick().await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(json!(false), result["responseOk"]);
        assert_eq!(json!(null), result["result"]["err"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stub non-existent execution returns error.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_execution_not_found(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_not_found(params) {
            try {
                obelisk.stub('E_00000000000000000000000000.n:fake_1', {'ok': 'x'});
                return JSON.stringify({ error: 'expected-error-but-got-none' });
            } catch (e) {
                return JSON.stringify({ errorType: 'stub-error', errorMessage: e.message });
            }
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-not-found")
                .await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(json!("stub-error"), result["errorType"]);
        let error_msg = result["errorMessage"].as_str().unwrap();
        assert!(
            error_msg.contains("NotFound") || error_msg.contains("ExecutionNotFound"),
            "Expected 'NotFound' in error message, got: {error_msg}"
        );
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stubbing the same value twice succeeds (idempotent).
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_same_value_twice(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_twice(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-param']);
            obelisk.stub(execId, {'ok': 'same-value'});
            obelisk.stub(execId, {'ok': 'same-value'}); // same value - should succeed
            const response = js.joinNext();
            const result = obelisk.getResult(execId);
            return JSON.stringify({ responseOk: response.ok, result: result });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-twice").await;
        harness.tick().await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(json!(true), result["responseOk"]);
        assert_eq!(json!("same-value"), result["result"]["ok"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stub `noret` function (returns `result` with no ok/err payloads).
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_noret(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_noret(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.noret', []);
            obelisk.stub(execId, {'ok': null}); // result has no payload
            const response = js.joinNext();
            const result = obelisk.getResult(execId);
            return JSON.stringify({ responseOk: response.ok, result: result });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-noret").await;
        harness.tick().await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(json!(true), result["responseOk"]);
        assert_eq!(json!(null), result["result"]["ok"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stub conflict - second stub with different value must fail.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_conflict(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_conflict(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-param']);
            obelisk.stub(execId, {'ok': 'first-value'});
            try {
                obelisk.stub(execId, {'ok': 'different-value'}); // must fail
                return JSON.stringify({ error: 'expected-conflict-but-stub-succeeded' });
            } catch (e) {
                const response = js.joinNext();
                const result = obelisk.getResult(execId);
                return JSON.stringify({
                    conflictDetected: true,
                    errorMessage: e.message,
                    responseOk: response.ok,
                    result: result
                });
            }
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-conflict")
                .await;
        harness.tick().await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(true),
            result["conflictDetected"],
            "Expected conflict, got: {result}"
        );
        let error_msg = result["errorMessage"].as_str().unwrap();
        assert!(
            error_msg.contains("Conflict"),
            "Expected 'Conflict' in error, got: {error_msg}"
        );
        assert_eq!(json!(true), result["responseOk"]);
        assert_eq!(json!("first-value"), result["result"]["ok"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: `obelisk.executionIdGenerate()` and `obelisk.schedule()` to schedule a top-level execution.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_schedule(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        // scheduleAt is optional - omitting it defaults to 'now'
        let js_source = r"
        export default function test_schedule(params) {
            const execId = obelisk.executionIdGenerate();
            obelisk.schedule(execId, 'testing:stub-activity/activity.foo', ['scheduled-param']);
            return JSON.stringify({ scheduledExecutionId: execId });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool.clone(), js_source, "test-schedule")
                .await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        let scheduled_exec_id_str = result["scheduledExecutionId"]
            .as_str()
            .expect("scheduledExecutionId should be a string");

        // Verify the scheduled execution was created in the database
        let scheduled_exec_id =
            ExecutionId::from_str(scheduled_exec_id_str).expect("should parse execution ID");
        let db_connection = db_pool.connection_test().await.unwrap();
        let create_request = db_connection
            .get_create_request(&scheduled_exec_id)
            .await
            .expect("scheduled execution should exist");

        assert_eq!(
            create_request.ffqn,
            FunctionFqn::new_static("testing:stub-activity/activity", "foo")
        );
        drop(harness);
        drop(db_connection);
        db_close.close().await;
    }

    #[tokio::test]
    async fn void_result_return_null() {
        // `result` (no ok, no err): `return null` → Ok(None)
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "do-work");
        let js_source = r"
            export default function do_work() {
                return null;
            }
        ";

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: None,
                err: None,
            },
            wit_type: StrVariant::Static("result"),
        };

        let worker = new_js_workflow_worker_with_return_type(js_source, &ffqn, return_type);
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        assert_matches!(retval, SupportedFunctionReturnValue::Ok(None));
    }

    #[tokio::test]
    async fn void_result_throw_null() {
        // `result` (no ok, no err): `throw null` → Err(None)
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "do-work");
        let js_source = r"
            export default function do_work() {
                throw null;
            }
        ";

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: None,
                err: None,
            },
            wit_type: StrVariant::Static("result"),
        };

        let worker = new_js_workflow_worker_with_return_type(js_source, &ffqn, return_type);
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        assert_matches!(retval, SupportedFunctionReturnValue::Err(None));
    }

    /// When the executor signals close while a JS workflow worker is running,
    /// the worker must write an `Unlocked` event and exit.
    ///
    /// Test steps:
    /// 1. Spawn the busy workflow worker task via `tick_test` (returns immediately
    ///    with a handle to the in-progress worker task).
    /// 2. Send the executor-close signal.
    /// 3. Join the worker task.
    /// 4. Assert the execution log contains an `Unlocked` event.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn executor_close_writes_unlocked_event(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
            export default function busy(params) {
                for (let i = 0; i < 30; i++) {
                    obelisk.sleep({ milliseconds: 300 });
                }
                return 'done';
            }
        ";
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "busy");

        let (close_sender, close_receiver) = tokio::sync::watch::channel(false);

        let sim_clock = SimClock::epoch();
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (worker, component_id, _) = compile_js_workflow_worker(
            js_source,
            &ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry,
            workflow_engine,
        )
        .await;

        let exec_task = new_js_workflow_exec_task_with_close_watcher(
            worker,
            sim_clock.clone_box(),
            db_pool.clone(),
            close_receiver,
        );

        // Register the execution so the DB is ready for the Unlocked append.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        // Spawn the busy worker task. tick_test returns as soon as the task is
        // spawned — the worker itself runs concurrently.
        let progress = exec_task
            .tick_test(sim_clock.now(), RunId::generate())
            .await;

        // Signal the executor to close. The worker bails on the next apply()
        // call (i.e. at the very first obelisk.sleep) and writes Unlocked.
        close_sender.send(true).unwrap();

        // Join the worker task.
        progress.wait_for_tasks().await;

        // Verify the Unlocked event is present in the execution log.
        let log = db_connection.get(&execution_id).await.unwrap();
        let has_unlocked = log
            .events
            .iter()
            .any(|e| matches!(e.event, ExecutionRequest::Unlocked { .. }));
        assert!(
            has_unlocked,
            "expected Unlocked event in execution log, got: {:?}",
            log.events
        );
        drop(db_connection);
        db_close.close().await;
    }

    /// Test: `Math.random()` returns a value in [0, 1) and is deterministic (replay
    /// produces identical output because random values are replayed from the event log).
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_math_random(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_math_random(params) {
            const r1 = Math.random();
            const r2 = Math.random();
            const r3 = Math.random();
            return JSON.stringify({
                r1, r2, r3,
                allInRange: r1 >= 0 && r1 < 1 && r2 >= 0 && r2 < 1 && r3 >= 0 && r3 < 1,
                notAllZero: r1 !== 0 || r2 !== 0 || r3 !== 0
            });
        }";

        let harness = JsWorkflowTestHarness::with_no_activities(
            db_pool.clone(),
            js_source,
            "test-math-random",
        )
        .await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(true),
            result["allInRange"],
            "all values must be in [0, 1): {result}"
        );
        assert_eq!(
            json!(true),
            result["notAllZero"],
            "values should not all be zero: {result}"
        );

        // Execution log must contain Persist events — one per Math.random() call
        let log = db_pool
            .connection_test()
            .await
            .unwrap()
            .get(&harness.execution_id)
            .await
            .unwrap();
        assert!(
            log.events.iter().any(|e| matches!(
                e.event,
                ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::Persist { .. }
                }
            )),
            "expected at least one Persist event for Math.random()"
        );
        drop(harness);
        db_close.close().await;
    }

    /// Test: `Date.now()` returns the current simulated clock time.
    /// - `advance_time(42ms)` → clock=42ms (no timers yet)
    /// - `tick()` → workflow creates `sleep_bt(Now)` with `expires_at=42ms`, yields
    /// - `advance_time(ZERO)` → fires the timer (42ms ≤ 42ms)
    /// - `tick()` → workflow resumes, `sleep_bt` returns 42ms, `Date.now()` = 42ms
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_date_now(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_date_now(params) {
            const now = Date.now();
            return JSON.stringify({ now });
        }";

        let harness =
            JsWorkflowTestHarness::with_no_activities(db_pool.clone(), js_source, "test-date-now")
                .await;
        // Put the clock at 42 ms before the workflow first runs so that the
        // sleep_bt(Now) call schedules its wakeup at t=42 ms.
        harness.advance_time(Duration::from_millis(42)).await;
        harness.tick().await; // workflow yields at sleep_bt(Now), expires_at=42ms
        harness.advance_time(Duration::ZERO).await; // fire the timer (42ms ≤ 42ms)
        harness.tick().await; // workflow resumes, sleep returns 42ms

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(42),
            result["now"],
            "Date.now() should return the simulated clock time (42ms): {result}"
        );

        // Execution log must contain a DelayRequest event — Date.now() uses
        // the internal sleep_bt(Now) which creates a JoinSetRequest::DelayRequest
        let db_conn = db_pool.connection_test().await.unwrap();
        let log = db_conn.get(&harness.execution_id).await.unwrap();
        assert!(
            log.events.iter().any(|e| matches!(
                e.event,
                ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        request: JoinSetRequest::DelayRequest { .. },
                        ..
                    }
                }
            )),
            "expected a JoinSetRequest::DelayRequest event for Date.now()"
        );
        drop(harness);
        drop(db_conn);
        db_close.close().await;
    }

    /// Test: `obelisk.sleep()` returns a `Date` object representing the wake-up time.
    /// - `advance_time(42ms)` → clock=42ms
    /// - `tick()` → workflow calls `sleep({ milliseconds: 100 })`, yields at `expires_at=142ms`
    /// - `advance_time(100ms)` → fires the timer (142ms ≤ 142ms)
    /// - `tick()` → workflow resumes; sleep returns a Date whose `.getTime()` == 142
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_sleep_returns_date(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_sleep_date(params) {
            const wakeUp = obelisk.sleep({ milliseconds: 100 });
            return JSON.stringify({ ms: wakeUp.getTime() });
        }";

        let harness = JsWorkflowTestHarness::with_no_activities(
            db_pool.clone(),
            js_source,
            "test-sleep-date",
        )
        .await;
        harness.advance_time(Duration::from_millis(42)).await;
        harness.tick().await; // workflow yields at sleep, expires_at=142ms
        harness.advance_time(Duration::from_millis(100)).await; // fire the timer
        harness.tick().await; // workflow resumes, sleep returns Date(142ms)

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(142),
            result["ms"],
            "sleep() should return a Date whose getTime() equals the wake-up ms: {result}"
        );
        drop(harness);
        db_close.close().await;
    }

    /// Test: replay of a JS workflow that creates a join set and returns.
    /// Replays at three stages:
    /// 1. Just after creation (no events in DB) — preview returns `JoinSetCreate` + finished result.
    /// 2. After `JoinSetCreate` event is manually inserted but execution is not finished — preview returns finished result.
    /// 3. After the execution is fully done — replay returns finished result, no `next_events`.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_replay_create_join_set(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r#"
        export default function test_replay(params) {
            const js = obelisk.createJoinSet();
            return "done-" + js.id();
        }"#;

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test-replay");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;

        let workflow_exec =
            new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params = Params::from_json_values_test(vec![json!(Vec::<String>::new())]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params,
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        // --- Stage 1: Replay on just-created execution (no events in DB) ---
        let (log_sender, _log_recv) = mpsc::channel(100);
        let replay = WorkflowJsWorker::replay(
            DeploymentId::generate(),
            component_id.clone(),
            runnable_component.wasmtime_component.clone(),
            &runnable_component.wasm_component.exim,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            execution_id.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender: log_sender.clone(),
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        info!("Stage 1 replay: {replay:?}");
        assert!(
            replay.get_return_value().is_some(),
            "workflow should complete during preview (non-blocking events only)"
        );
        let next_events = replay.history_events();
        assert!(
            !next_events.is_empty(),
            "preview should contain at least the JoinSetCreate event"
        );
        assert_matches!(&next_events[0], HistoryEvent::JoinSetCreate { .. });

        // --- Stage 2: Manually insert JoinSetCreate event, execution not finished ---
        // Extract the JoinSetCreate event from the replay preview and insert it.
        use concepts::storage::AppendRequest;
        let join_set_create_event = next_events[0].clone();
        db_connection
            .append_batch(
                sim_clock.now(),
                vec![AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionRequest::HistoryEvent {
                        event: join_set_create_event.clone(),
                    },
                }],
                execution_id.clone(),
                Version::new(1),
            )
            .await
            .unwrap();

        let replay2 = WorkflowJsWorker::replay(
            DeploymentId::generate(),
            component_id.clone(),
            runnable_component.wasmtime_component.clone(),
            &runnable_component.wasm_component.exim,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            execution_id.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender: log_sender.clone(),
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();

        let replay2 = assert_matches!(replay2, ReplayResponse::Advanceable(replay2) => replay2);
        info!("Stage 2 replay: {replay2:?}");
        assert!(
            replay2.get_return_value().is_some(),
            "workflow should complete even with partial event log (JoinSetCreate already present)"
        );
        // The JoinSetCreate is already in the log, so it should not appear as a next_event.
        assert!(
            !replay2
                .history_events()
                .iter()
                .any(|e| matches!(e, HistoryEvent::JoinSetCreate { .. })),
            "JoinSetCreate should not appear in next_events since it's already in the log"
        );

        // --- Stage 3: Execute the workflow fully, then replay ---
        // Tick the execution (which already has JoinSetCreate in the log) to completion.
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );

        let res = db_connection
            .get_finished_result(&execution_id)
            .await
            .unwrap();
        let ok_val = assert_matches!(&res, SupportedFunctionReturnValue::Ok(Some(val)) => val);
        let result_str = assert_matches!(&ok_val.value, WastVal::String(s) => s);
        assert!(
            result_str.starts_with("done-"),
            "unexpected result: {result_str}"
        );

        let replay3 = WorkflowJsWorker::replay(
            DeploymentId::generate(),
            component_id.clone(),
            runnable_component.wasmtime_component.clone(),
            &runnable_component.wasm_component.exim,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            execution_id.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender: log_sender.clone(),
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();

        let result = assert_matches!(replay3, ReplayResponse::Finished { result } => result);
        info!("Stage 3 replay result: {result:?}");
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_replay_call_stub(database: Database) {
        use crate::activity::activity_worker::test::compile_activity_stub;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function call_stub() {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-input']);
            obelisk.stub(execId, { 'ok': 'hello' });
            const response = js.joinNext();
            if (!response.ok) throw 'stub failed';
            return obelisk.getResult(execId).ok;
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "call-stub");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;

        let workflow_exec =
            new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());

        let execution_id = ExecutionId::from_parts(0, 0);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        // replay on just-created execution
        let (log_sender, _log_recv) = mpsc::channel(100);
        let replay = WorkflowJsWorker::replay(
            DeploymentId::generate(),
            component_id.clone(),
            runnable_component.wasmtime_component.clone(),
            &runnable_component.wasm_component.exim,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            execution_id.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender: log_sender.clone(),
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        // With FlushedCache interruption, replay stops after the first cache flush.
        // The first flush covers JoinSetCreate and child execution submit.
        assert_eq!(2, replay.history_events().len());
        insta::with_settings!({
            prepend_module_to_snapshot => false},
            {
                assert_json_snapshot!(replay.history_events());
            }
        );

        // Tick - should end with JoinNext
        // Tick the execution (which already has JoinSetCreate in the log) to completion.
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );
        // Replay should return the return value
        let replay = WorkflowJsWorker::replay(
            DeploymentId::generate(),
            component_id.clone(),
            runnable_component.wasmtime_component.clone(),
            &runnable_component.wasm_component.exim,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            execution_id.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender: log_sender.clone(),
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        let retval = replay
            .get_return_value()
            .expect("retval should be computed");
        let retval = assert_matches!(retval, SupportedFunctionReturnValue::Ok(Some(WastValWithType{ r#type: _, value })) => value);
        assert_eq!(WastVal::String("hello".into()), *retval);
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[case::full(None, "full", 0)]
    #[case::trimmed_to_1(Some(1), "trimmed_to_1", 1)]
    #[tokio::test]
    async fn workflow_js_advance_call_stub(
        database: Database,
        #[case] trim_to: Option<usize>,
        #[case] snapshot_suffix: &str,
        #[case] execution_idx: u16,
    ) {
        use crate::activity::activity_worker::test::compile_activity_stub;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function call_stub() {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-input']);
            obelisk.stub(execId, { 'ok': 'hello' });
            const response = js.joinNext();
            if (!response.ok) throw 'stub failed';
            return obelisk.getResult(execId).ok;
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "call-stub");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;

        let db_connection = db_pool.connection_test().await.unwrap();

        let (log_sender, _log_recv) = mpsc::channel(100);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: concepts::storage::LogLevel::Debug,
            log_sender: log_sender.clone(),
        });

        let deployment_id = DeploymentId::from_parts(0, 0);
        let create_paused_execution = |execution_id: ExecutionId| CreateRequest {
            created_at: sim_clock.now(),
            execution_id,
            ffqn: user_ffqn.clone(),
            params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
            parent: None,
            metadata: ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            component_id: component_id.clone(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            scheduled_by: None,
            paused: true,
        };

        let execution_id = ExecutionId::from_parts(0, execution_idx.into());
        db_connection
            .create(create_paused_execution(execution_id.clone()))
            .await
            .unwrap();
        let result = workflow_js_step_execution_until_finished(
            &*db_connection,
            WorkflowJsAdvanceHarness {
                deployment_id,
                component_id,
                runnable_component,
                workflow_engine,
                fn_registry,
                db_pool,
                execution_id,
                logs_storage_config,
                js_source: js_source.to_string(),
                sim_clock,
                idle_action: None,
            },
            snapshot_suffix,
            16,
            trim_to,
        )
        .await;

        assert_matches!(
            result,
            SupportedFunctionReturnValue::Ok(Some(WastValWithType { value: WastVal::String(s), .. }))
                if s == "hello"
        );

        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[case::full(None, "submit_cancel_full", 10)]
    #[case::trimmed_to_1(Some(1), "submit_cancel_trimmed_to_1", 11)]
    #[tokio::test]
    async fn workflow_js_advance_submit_without_await_cancels_child_activity(
        database: Database,
        #[case] trim_to: Option<usize>,
        #[case] test_name: &str,
        #[case] execution_idx: u16,
    ) {
        use concepts::ExecutionFailureKind;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function submit_and_return() {
            const js = obelisk.createJoinSet();
            js.submit('testing:fibo/fibo.fibo', [10]);
            return 'done';
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "submit-and-return");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;

        let db_connection = db_pool.connection_test().await.unwrap();

        let deployment_id = DeploymentId::from_parts(0, 0);
        let execution_id = ExecutionId::from_parts(0, execution_idx.into());
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn.clone(),
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let result = workflow_js_step_execution_until_finished(
            &*db_connection,
            WorkflowJsAdvanceHarness {
                deployment_id,
                component_id,
                runnable_component,
                workflow_engine,
                fn_registry,
                db_pool: db_pool.clone(),
                execution_id: execution_id.clone(),
                logs_storage_config: None,
                js_source: js_source.to_string(),
                sim_clock,
                idle_action: None,
            },
            test_name,
            16,
            trim_to,
        )
        .await;

        assert_matches!(
            result,
            SupportedFunctionReturnValue::Ok(Some(WastValWithType { value: WastVal::String(s), .. }))
                if s == "done"
        );

        let log = db_connection.get(&execution_id).await.unwrap();
        assert_eq!(1, log.responses.len(), "join set should be auto-closed");
        let result = assert_matches!(
            &log.responses[0].event.event.event,
            JoinSetResponse::ChildExecutionFinished { result, .. } => result
        );
        let err = assert_matches!(result, SupportedFunctionReturnValue::ExecutionError(err) => err);
        assert_matches!(err.kind, ExecutionFailureKind::Cancelled);

        drop(db_connection);
        db_close.close().await;
    }

    #[tokio::test]
    async fn advance_forwards_captured_application_logs() {
        test_utils::set_up();

        let js_source = r"
        export default function () {
            console.info('before sleep');
            obelisk.sleep({ milliseconds: 10 });
            return 'done';
        }";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "log-then-sleep");
        let sim_clock = SimClock::epoch();
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let (log_sender, mut log_recv) = mpsc::channel(16);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: LogLevel::Debug,
            log_sender,
        });

        let replay = WorkflowJsWorker::replay(
            DeploymentId::generate(),
            component_id.clone(),
            runnable_component.wasmtime_component.clone(),
            &runnable_component.wasm_component.exim,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            execution_id.clone(),
            logs_storage_config.clone(),
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();
        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        assert_matches!(
            log_recv.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );

        WorkflowJsWorker::advance(
            DeploymentId::generate(),
            component_id,
            runnable_component.wasmtime_component,
            &runnable_component.wasm_component.exim,
            workflow_engine,
            fn_registry,
            CancelRegistry::new(),
            db_pool,
            execution_id,
            logs_storage_config,
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
            replay,
        )
        .await
        .unwrap();

        assert_matches!(
            log_recv.try_recv(),
            Ok(LogInfoAppendRow {
                log_entry: LogEntry::Log {
                    level: LogLevel::Info,
                    message,
                    ..
                },
                ..
            }) if message == "before sleep"
        );
        assert_matches!(
            log_recv.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty
                | tokio::sync::mpsc::error::TryRecvError::Disconnected)
        );
    }

    #[tokio::test]
    async fn advance_trimmed_writes_forward_only_prefix_logs() {
        test_utils::set_up();

        let js_source = r"
        export default function () {
            console.info('before create');
            const js = obelisk.createJoinSet();
            console.info('before delay');
            js.submitDelay({ milliseconds: 10 });
            console.info('before join');
            js.joinNext();
            return 'done';
        }";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "trimmed-log-prefix");
        let sim_clock = SimClock::epoch();
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let (log_sender, mut log_recv) = mpsc::channel(16);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: LogLevel::Debug,
            log_sender,
        });

        let mut forwarded = Vec::new();
        for expected_len in [3_usize, 2, 1] {
            let replay = WorkflowJsWorker::replay(
                DeploymentId::generate(),
                component_id.clone(),
                runnable_component.wasmtime_component.clone(),
                &runnable_component.wasm_component.exim,
                workflow_engine.clone(),
                fn_registry.clone(),
                db_pool.clone(),
                execution_id.clone(),
                logs_storage_config.clone(),
                sim_clock.clone_box(),
                js_source.to_string(),
                &default_return_type(),
            )
            .await
            .unwrap();
            let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
            assert_eq!(replay.captured_writes.len(), expected_len);

            WorkflowJsWorker::advance(
                DeploymentId::generate(),
                component_id.clone(),
                runnable_component.wasmtime_component.clone(),
                &runnable_component.wasm_component.exim,
                workflow_engine.clone(),
                fn_registry.clone(),
                CancelRegistry::new(),
                db_pool.clone(),
                execution_id.clone(),
                logs_storage_config.clone(),
                sim_clock.clone_box(),
                js_source.to_string(),
                &default_return_type(),
                replay.truncate_to(1),
            )
            .await
            .unwrap();

            forwarded.extend(drain_forwarded_log_messages(&mut log_recv));
        }

        assert_eq!(
            forwarded,
            vec!["before create", "before delay", "before join"]
        );
        let replay = WorkflowJsWorker::replay(
            DeploymentId::generate(),
            component_id,
            runnable_component.wasmtime_component,
            &runnable_component.wasm_component.exim,
            workflow_engine,
            fn_registry,
            db_pool,
            execution_id,
            logs_storage_config,
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();
        assert_matches!(replay, ReplayResponse::Blocked);
    }

    #[derive(Clone)]
    struct WorkflowJsAdvanceHarness {
        deployment_id: DeploymentId,
        component_id: ComponentId,
        runnable_component: RunnableComponent,
        workflow_engine: Arc<Engine>,
        fn_registry: Arc<dyn FunctionRegistry>,
        db_pool: Arc<dyn DbPool>,
        execution_id: ExecutionId,
        logs_storage_config: Option<LogStrageConfig>,
        js_source: String,
        sim_clock: SimClock,
        idle_action: Option<WorkflowJsAdvanceIdleAction>,
    }

    #[allow(dead_code)]
    #[derive(Clone, Copy)]
    enum WorkflowJsAdvanceIdleAction {
        TickFiboActivity,
    }

    async fn workflow_js_step_execution_until_finished(
        db_connection: &dyn DbConnectionTest,
        harness: WorkflowJsAdvanceHarness,
        test_name: &str,
        max_steps: usize,
        trim_to: Option<usize>,
    ) -> SupportedFunctionReturnValue {
        let mut steps = 0;
        let mut saw_trimmed_preview = false;
        loop {
            harness
                .sim_clock
                .move_time_forward(Duration::from_millis(100));
            let replay = WorkflowJsWorker::replay(
                harness.deployment_id,
                harness.component_id.clone(),
                harness.runnable_component.wasmtime_component.clone(),
                &harness.runnable_component.wasm_component.exim,
                harness.workflow_engine.clone(),
                harness.fn_registry.clone(),
                harness.db_pool.clone(),
                harness.execution_id.clone(),
                harness.logs_storage_config.clone(),
                harness.sim_clock.clone_box(),
                harness.js_source.clone(),
                &default_return_type(),
            )
            .await
            .unwrap();
            let replay = match replay {
                ReplayResponse::Advanceable(replay) => replay,
                ReplayResponse::Finished {
                    result: finished_result,
                } => {
                    assert!(
                        steps > 0,
                        "step-through harness must execute at least one replay+advance round",
                    );
                    if trim_to.is_some() {
                        assert!(
                            saw_trimmed_preview,
                            "test must exercise trimmed replay writes"
                        );
                    }
                    return finished_result;
                }
                ReplayResponse::Blocked => match harness.idle_action {
                    Some(WorkflowJsAdvanceIdleAction::TickFiboActivity) => {
                        let activity_exec = new_activity_fibo(
                            harness.db_pool.clone(),
                            harness.sim_clock.clone_box(),
                            TokioSleep,
                            LockingStrategy::ByComponentDigest,
                        )
                        .await;
                        activity_exec
                            .tick_test_await(harness.sim_clock.now(), RunId::generate())
                            .await;
                        continue;
                    }
                    None => panic!(
                        "replay must be advanceable or finished while stepping paused JS workflow"
                    ),
                },
            };

            steps += 1;
            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_replay_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    assert_json_snapshot!(
                        redact_component_digest(serde_json::to_value(&replay).unwrap())
                    );
                }
            );

            let requested = match trim_to {
                Some(trim_to) => replay.truncate_to(trim_to),
                None => replay.clone(),
            };
            saw_trimmed_preview |= requested.captured_writes.len() < replay.captured_writes.len();
            harness
                .sim_clock
                .move_time_forward(Duration::from_millis(100));

            let advance = WorkflowJsWorker::advance(
                harness.deployment_id,
                harness.component_id.clone(),
                harness.runnable_component.wasmtime_component.clone(),
                &harness.runnable_component.wasm_component.exim,
                harness.workflow_engine.clone(),
                harness.fn_registry.clone(),
                CancelRegistry::new(),
                harness.db_pool.clone(),
                harness.execution_id.clone(),
                harness.logs_storage_config.clone(),
                harness.sim_clock.clone_box(),
                harness.js_source.clone(),
                &default_return_type(),
                requested.clone(),
            )
            .await
            .unwrap();
            assert_eq!(advance.finished, requested.get_return_value().cloned());

            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_advance_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    assert_json_snapshot!(
                        json!({
                            "finished": advance.finished.is_some(),
                            "requested_captured_writes_len": requested.captured_writes.len(),
                            "replayed_captured_writes_len": replay.captured_writes.len(),
                        })
                    );
                }
            );

            let log = db_connection.get(&harness.execution_id).await.unwrap();
            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_log_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    assert_json_snapshot!(ExecutionLogSanitized::from(log));
                }
            );

            if let Ok(finished_result) = db_connection
                .get_finished_result(&harness.execution_id)
                .await
            {
                assert!(
                    steps > 0,
                    "step-through harness must execute at least one replay+advance round",
                );
                if trim_to.is_some() {
                    assert!(
                        saw_trimmed_preview,
                        "test must exercise trimmed replay writes",
                    );
                }
                return finished_result;
            }

            assert!(
                steps < max_steps,
                "execution did not finish after {steps} replay+advance steps",
            );
        }
    }

    #[tokio::test]
    async fn advance_paused_js_workflow_can_pause_new_child_execution() {
        test_utils::set_up();

        let js_source = "
            export default function (_params) {
                const js = obelisk.createJoinSet();
                js.submit('testing:fibo/fibo.fibo', [10]);
                js.joinNext();
                return 'done';
            }
        ";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "pause-child");
        let sim_clock = SimClock::epoch();
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;
        let db_connection = db_pool.connection_test().await.unwrap();
        let execution_id = ExecutionId::from_parts(0, 9002);
        let created_at = sim_clock.now();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn.clone(),
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let deployment_id = DeploymentId::from_parts(0, 0);
        sim_clock.move_time_forward(Duration::from_millis(100));
        let replay = WorkflowJsWorker::replay(
            deployment_id,
            component_id.clone(),
            runnable_component.wasmtime_component.clone(),
            &runnable_component.wasm_component.exim,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            execution_id.clone(),
            None,
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        let mut requested = replay.clone();
        let replayed_child_created_at = requested
            .captured_writes
            .iter()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution { child_req, .. } => {
                    child_req.first().map(|child| child.created_at)
                }
                _ => None,
            })
            .expect("replay should create a child execution");
        assert_eq!(replayed_child_created_at, sim_clock.now());
        let child_execution_id = requested
            .captured_writes
            .iter_mut()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time,
                    batch,
                    child_req,
                    ..
                } => {
                    *current_time = DateTime::UNIX_EPOCH; // modifying time in the advance request should have no effect.
                    for req in batch.iter_mut() {
                        req.created_at = DateTime::UNIX_EPOCH;
                    }
                    for child in child_req.iter_mut() {
                        child.paused = true;
                        child.created_at = DateTime::UNIX_EPOCH;
                        child.scheduled_at = DateTime::UNIX_EPOCH;
                    }
                    child_req.first().map(|child| child.execution_id.clone())
                }
                _ => None,
            })
            .expect("replay should create a child execution");
        sim_clock.move_time_forward(Duration::from_millis(100));

        let advance = WorkflowJsWorker::advance(
            deployment_id,
            component_id,
            runnable_component.wasmtime_component,
            &runnable_component.wasm_component.exim,
            workflow_engine,
            fn_registry,
            CancelRegistry::new(),
            db_pool.clone(),
            execution_id,
            None,
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
            requested,
        )
        .await
        .unwrap();

        assert_eq!(advance.finished, None);
        assert_matches!(
            db_connection
                .get_pending_state(&child_execution_id)
                .await
                .unwrap()
                .pending_state,
            PendingState::Paused(_)
        );
        let create_event = db_connection
            .get_execution_event(&child_execution_id, &Version::new(0))
            .await
            .unwrap();
        // Check that the current system time is used.
        assert_eq!(create_event.created_at, sim_clock.now());
        let ExecutionRequest::Created { scheduled_at, .. } = create_event.event else {
            panic!("child execution log must start with Created");
        };
        assert_eq!(scheduled_at, sim_clock.now());
    }

    #[tokio::test]
    async fn advance_paused_js_workflow_uses_server_time_for_relative_schedule() {
        test_utils::set_up();

        let js_source = r"
            export default function (_params) {
                const execId = obelisk.executionIdGenerate();
                obelisk.schedule(
                    execId,
                    'testing:stub-activity/activity.foo',
                    ['scheduled-param'],
                    { minutes: 5 },
                );
                return execId;
            }
        ";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "schedule-relative");
        let sim_clock = SimClock::epoch();
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            crate::activity::activity_worker::test::compile_activity_stub(
                test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY,
            )
            .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        )
        .await;
        let db_connection = db_pool.connection_test().await.unwrap();
        let execution_id = ExecutionId::from_parts(0, 9003);
        let created_at = sim_clock.now();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn.clone(),
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let deployment_id = DeploymentId::from_parts(0, 0);
        sim_clock.move_time_forward(Duration::from_millis(100));
        let replay = WorkflowJsWorker::replay(
            deployment_id,
            component_id.clone(),
            runnable_component.wasmtime_component.clone(),
            &runnable_component.wasm_component.exim,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            execution_id.clone(),
            None,
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
        )
        .await
        .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        let mut requested = replay.clone();
        let replayed_child_scheduled_at = requested
            .captured_writes
            .iter()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution { child_req, .. } => {
                    child_req.first().map(|child| child.scheduled_at)
                }
                _ => None,
            })
            .expect("replay should create a scheduled execution");
        assert_eq!(
            replayed_child_scheduled_at,
            sim_clock.now() + chrono::TimeDelta::minutes(5)
        );
        let scheduled_execution_id = requested
            .captured_writes
            .iter_mut()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time,
                    batch,
                    child_req,
                    ..
                } => {
                    *current_time = DateTime::UNIX_EPOCH; // modifying time in the advance request should have no effect.
                    for req in batch.iter_mut() {
                        req.created_at = DateTime::UNIX_EPOCH;
                    }
                    for child in child_req.iter_mut() {
                        child.created_at = DateTime::UNIX_EPOCH;
                        child.scheduled_at = DateTime::UNIX_EPOCH;
                    }
                    child_req.first().map(|child| child.execution_id.clone())
                }
                _ => None,
            })
            .expect("replay should create a scheduled execution");
        sim_clock.move_time_forward(Duration::from_millis(100));

        let advance = WorkflowJsWorker::advance(
            deployment_id,
            component_id,
            runnable_component.wasmtime_component,
            &runnable_component.wasm_component.exim,
            workflow_engine,
            fn_registry,
            CancelRegistry::new(),
            db_pool.clone(),
            execution_id,
            None,
            sim_clock.clone_box(),
            js_source.to_string(),
            &default_return_type(),
            requested,
        )
        .await
        .unwrap();

        advance
            .finished
            .expect("main execution should have finished");
        let expected_scheduled_at = sim_clock.now() + chrono::TimeDelta::minutes(5);
        assert_matches!(
            db_connection
                .get_pending_state(&scheduled_execution_id)
                .await
                .unwrap()
                .pending_state,
            PendingState::PendingAt(PendingStatePendingAt { scheduled_at, .. }) if scheduled_at == expected_scheduled_at
        );
        let create_event = db_connection
            .get_execution_event(&scheduled_execution_id, &Version::new(0))
            .await
            .unwrap();
        assert_eq!(create_event.created_at, sim_clock.now());
        let ExecutionRequest::Created { scheduled_at, .. } = create_event.event else {
            panic!("scheduled execution log must start with Created");
        };
        assert_eq!(scheduled_at, expected_scheduled_at);
    }
}
