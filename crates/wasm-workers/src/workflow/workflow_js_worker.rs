//! JS workflow worker that wraps a `WorkflowWorker` running the Boa WASM component.
//!
//! The Boa component exports `obelisk-workflow:workflow-js-runtime/execute.run(fn-name, js-code, params-json) -> result<result<string, string>, js-runtime-error>`.
//! This wrapper translates the user's typed interface `func(params) -> result<T, E>`
//! into calls to the Boa component, deserializing the JSON-encoded ok string as the configured type.

use super::workflow_worker::{ReplayError, WorkflowConfig, WorkflowWorker, WorkflowWorkerCompiled};
use crate::activity::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use crate::workflow::deadline_tracker::{DeadlineTrackerFactory, DeadlineTrackerFactoryForReplay};
use crate::workflow::workflow_ctx::ReplayKind;
use async_trait::async_trait;
use concepts::prefixed_ulid::{DeploymentId, ExecutorId, RunId};
use concepts::storage::{DbConnection, DbPool, Locked};
use concepts::time::{ClockFn, ConstClock};
use concepts::{
    ComponentId, ComponentRetryConfig, ComponentType, ExecutionFailureKind, ExecutionId,
    ExecutionMetadata, FinishedExecutionError, FunctionFqn, FunctionMetadata, FunctionRegistry,
    PackageIfcFns, ParameterType, Params, ResultParsingError, ResultParsingErrorFromVal,
    ReturnTypeExtendable, SupportedFunctionReturnValue,
};
use db_mem::inmemory_dao::InMemoryPool;
use executor::worker::{
    FatalError, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use std::sync::Arc;
use tracing::{Span, debug};
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
        let linked = self.inner.link(fn_registry)?;
        Ok(WorkflowJsWorkerLinked {
            inner: linked,
            js_source: self.js_source,
            js_file_name: self.js_file_name,
            user_params: self.user_params,
            user_return_type: self.user_return_type,
            user_exports_noext: self.user_wasm_component.exported_functions(false).to_vec(),
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
}

#[async_trait]
impl Worker for WorkflowJsWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    async fn run(&self, mut ctx: WorkerContext) -> WorkerResult {
        // Serialize each user parameter individually as a JSON string.
        let json_params = ctx
            .params
            .as_json_values()
            .expect("params come from database, not wasmtime"); // TODO: Extract ParamsInternal
        assert_eq!(
            self.user_params.len(),
            json_params.len(),
            "type checked in Params::from_json_values"
        );
        let params_json_list: Vec<serde_json::Value> = json_params
            .iter()
            .map(|v| {
                serde_json::Value::String(
                    serde_json::to_string(v).expect("serde_json::Value must be serializable"),
                )
            })
            .collect();

        // Rewrite context to call workflow-js-runtime
        ctx.ffqn =
            FunctionFqn::new_static_tuple(("obelisk-workflow:workflow-js-runtime/execute", "run"));
        // run: func(js-code: string, params-json: list<string>, js-file-name: option<string>) -> result<result<string, string>, js-runtime-error>
        let boa_params: Arc<[serde_json::Value]> = Arc::from([
            serde_json::Value::String(self.js_source.clone()), // js-code: string
            serde_json::Value::Array(params_json_list),        // params-json: list<string>
            serde_json::Value::String(self.js_file_name.clone()), // js-file-name: option<string>
        ]);
        ctx.params = Params::from_json_values(
            boa_params,
            [
                &TypeWrapper::String,                                // js-code: string
                &TypeWrapper::List(Box::new(TypeWrapper::String)),   // params-json: list<string>
                &TypeWrapper::Option(Box::new(TypeWrapper::String)), // js-file-name: option<string>
            ]
            .into_iter(),
        )
        .expect("types checked at compile time");

        let inner_worker_ok = self.inner.run(ctx).await?;
        debug!("Workflow worker returned {inner_worker_ok:?}");

        match inner_worker_ok {
            WorkerResultOk::DbUpdatedByWorkerOrWatcher => {
                Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher)
            }
            WorkerResultOk::RunFinished {
                retval,
                version,
                http_client_traces,
            } => {
                match retval {
                    SupportedFunctionReturnValue::Ok(Some(WastValWithType {
                        r#type:
                            TypeWrapper::Result {
                                ok: Some(ok_type),
                                err: Some(err_type),
                            },
                        value: WastVal::Result(Ok(Some(ok_val))),
                    })) if *ok_type == TypeWrapper::String && *err_type == TypeWrapper::String => {
                        let WastVal::String(ok_val) = *ok_val else {
                            unreachable!("ok type is String, so value must be WastVal::String")
                        };
                        let Ok(ok_val) = serde_json::from_str(&ok_val) else {
                            unreachable!("workflow-js-runtime always sends JSON-encoded string")
                        };
                        let retval = crate::js_worker_utils::map_js_ok_to_user_retval(
                            Some(&ok_val),
                            &self.user_return_type,
                            version.clone(),
                        )?;
                        Ok(WorkerResultOk::RunFinished {
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
                    })) if *ok_type == TypeWrapper::String && *err_type == TypeWrapper::String => {
                        let WastVal::String(thrown) = *err_val else {
                            unreachable!("err type is String, so value must be WastVal::String")
                        };
                        let retval = crate::js_worker_utils::map_js_throw_to_user_err(
                            &thrown,
                            &self.user_return_type,
                            version.clone(),
                        )?;
                        Ok(WorkerResultOk::RunFinished {
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

                                Err(WorkerError::FatalError(
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
                                Err(WorkerError::FatalError(
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
                                Ok(WorkerResultOk::RunFinished {
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

                    retval @ SupportedFunctionReturnValue::ExecutionError(_) => {
                        Ok(WorkerResultOk::RunFinished {
                            retval,
                            version,
                            http_client_traces,
                        })
                    }

                    other => unreachable!("unexpected SupportedFunctionReturnValue: {other:?}"),
                }
            }
        }
    }
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
        db_conn: &dyn DbConnection,
        execution_id: ExecutionId,
        logs_storage_config: Option<LogStrageConfig>,
        js_source: String,
    ) -> Result<(), ReplayError> {
        let clock_fn = ConstClock(chrono::DateTime::from_timestamp_nanos(0));

        let config = WorkflowConfig {
            join_next_blocking_strategy:
                super::workflow_worker::JoinNextBlockingStrategy::Interrupt,
            backtrace_persist: false,
            lock_extension: None,
            subscription_interruption: None,
            component_id,
            stub_wasi: true, // no harm, stub it in any case
            fuel: None,
        };

        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;

        // Transform the stored params to the workflow-js-runtime format
        let _original_ffqn = log.ffqn().clone();
        let original_params = log.params().clone();

        // Build the transformed params for the JS runtime
        // Serialize each stored parameter individually as a JSON string
        let json_params = original_params
            .as_json_values()
            .expect("params come from database");
        let params_json_list: Vec<serde_json::Value> = json_params
            .iter()
            .map(|v| {
                serde_json::Value::String(
                    serde_json::to_string(v).expect("serde_json::Value must be serializable"),
                )
            })
            .collect();

        let boa_ffqn =
            FunctionFqn::new_static_tuple(("obelisk-workflow:workflow-js-runtime/execute", "run"));
        let boa_params: Arc<[serde_json::Value]> = Arc::from([
            serde_json::Value::String(js_source),
            serde_json::Value::Array(params_json_list),
            serde_json::Value::Null, // no backtrace is going to be persisted anyway
        ]);
        let transformed_params = Params::from_json_values(
            boa_params,
            [
                &TypeWrapper::String,                                // js-code: string
                &TypeWrapper::List(Box::new(TypeWrapper::String)),   // params-json: list<string>
                &TypeWrapper::Option(Box::new(TypeWrapper::String)), // js-file-name: option<string>
            ]
            .into_iter(),
        )
        .expect("types checked at compile time");
        let (_executor_close_sender, executor_close_watcher) = tokio::sync::watch::channel(false); // TODO: consider using current exec's watcher
        let replay_kind = if log.is_finished() {
            ReplayKind::Finished
        } else {
            ReplayKind::Unfinished
        };
        let ctx = WorkerContext {
            execution_id,
            metadata: ExecutionMetadata::empty(),
            ffqn: boa_ffqn,
            params: transformed_params,
            event_history: log.event_history().collect(),
            responses: log.responses,
            version: log.next_version,
            can_be_retried: true,
            worker_span: Span::current(),
            locked_event: Locked {
                component_id: config.component_id.clone(),
                deployment_id,
                executor_id: ExecutorId::generate(),
                run_id: RunId::generate(),
                lock_expires_at: clock_fn.now(),
                retry_config: ComponentRetryConfig::WORKFLOW,
            },
            executor_close_watcher,
        };

        let compiled = WorkflowWorkerCompiled::new_with_config_inner(
            wasmtime_component,
            exim,
            config,
            engine,
            clock_fn.clone_box(),
        )?;
        let linked = compiled.link(fn_registry)?;
        let db_pool = Arc::new(InMemoryPool::new());
        let worker = linked.into_worker(
            deployment_id,
            db_pool,
            Arc::new(DeadlineTrackerFactoryForReplay {}),
            CancelRegistry::new(),
            logs_storage_config,
        );
        worker.replay_internal(ctx, Some(replay_kind)).await
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
    use concepts::component_id::{COMPONENT_DIGEST_DUMMY, ComponentDigest};
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, DelayId, ExecutorId, RunId};
    use concepts::storage::{
        CreateRequest, DbConnectionTest, DbPool, DbPoolCloseable, ExecutionRequest, HistoryEvent,
        JoinSetRequest, JoinSetResponse, Locked, PendingState, PendingStateFinished,
        PendingStateFinishedError, PendingStateFinishedResultKind, Version,
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
    use rstest::rstest;
    use serde_json::json;
    use std::str::FromStr as _;
    use std::time::Duration;
    use test_db_macro::expand_enum_database;
    use test_utils::sim_clock::SimClock;
    use tokio::sync::mpsc;
    use tracing::{info, info_span};
    use utils::sha256sum::calculate_sha256_file;
    use val_json::wast_val::WastVal;
    use wasmtime::Engine;

    const FIBO_10_OUTPUT: u64 = 55;

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
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
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
                    "failed to type check the return value `{}` as type string - invalid type: map, expected value matching \"string\" at line 1 column 2",
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
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
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
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        // For result<string, string>, a throw becomes Err
        let err_val = assert_matches!(retval, SupportedFunctionReturnValue::Err(err) => err);
        let err_val = err_val.expect("should have err value");
        assert_eq!(extract_string(&err_val.value), "something went wrong");
    }

    #[tokio::test]
    async fn workflow_js_syntax_error_should_fail_to_instantiate() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "broken");
        let js_source = r"
            export default function broken( {
                return 'this has a syntax error';
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
                assert!(reason.contains("module_parse_error"), "reason: {reason}");
            }
        );
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
            task_limiter: None,
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
            task_limiter: None,
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
        workflow_js_all_apis_inner(db_pool.clone(), activity_should_win, explicit_close).await;
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
            db_connection.as_ref(),
            execution_id,
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender,
            }),
            js_source.to_string(),
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
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
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
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
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

        db_close.close().await;
    }
}
