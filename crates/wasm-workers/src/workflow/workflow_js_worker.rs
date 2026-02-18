//! JS workflow worker that wraps a `WorkflowWorker` running the Boa WASM component.
//!
//! The Boa component exports `obelisk-workflow:workflow-js-runtime/execute.run(fn-name, js-code, params-json) -> result<result<string, string>, js-runtime-error>`.
//! This wrapper translates the user's typed interface `func(params: list<string>) -> result<string, string>`
//! into calls to the Boa component.

use super::workflow_worker::{ReplayError, WorkflowConfig, WorkflowWorker, WorkflowWorkerCompiled};
use crate::activity::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use crate::workflow::deadline_tracker::{DeadlineTrackerFactory, DeadlineTrackerFactoryForReplay};
use async_trait::async_trait;
use concepts::prefixed_ulid::{DeploymentId, ExecutorId, RunId};
use concepts::storage::{DbConnection, DbPool, Locked};
use concepts::time::{ClockFn, ConstClock};
use concepts::{
    ComponentId, ComponentRetryConfig, ExecutionFailureKind, ExecutionId, ExecutionMetadata,
    FinishedExecutionError, FunctionFqn, FunctionMetadata, FunctionRegistry, PackageIfcFns,
    ParameterType, ParameterTypes, Params, ResultParsingError, ResultParsingErrorFromVal,
    ReturnType, StrVariant, SupportedFunctionReturnValue,
};
use db_mem::inmemory_dao::InMemoryPool;
use executor::worker::{
    FatalError, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use indexmap::IndexMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{Span, debug};
use utils::wasm_tools::ExIm;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};
use wasmtime::Engine;

/// Compiled JS workflow. Holds the compiled Boa WASM component + JS source + user FFQN.
pub struct WorkflowJsWorkerCompiled {
    inner: WorkflowWorkerCompiled,
    js_source: String,
    user_ffqn: FunctionFqn,
    user_params: Vec<ParameterType>,
    user_exports_noext: Vec<FunctionMetadata>,
    user_exports_ext: Vec<FunctionMetadata>,
    user_exports_hierarchy_ext: Vec<PackageIfcFns>,
}

impl WorkflowJsWorkerCompiled {
    #[must_use]
    pub fn new(
        inner: WorkflowWorkerCompiled,
        js_source: String,
        user_ffqn: FunctionFqn,
        user_params: Vec<ParameterType>,
    ) -> Self {
        let fn_metadata = make_fn_metadata(user_ffqn.clone(), &user_params);
        let fn_metadata_ext = make_fn_metadata_ext(&fn_metadata);
        let hierarchy = make_exports_hierarchy(&fn_metadata, &fn_metadata_ext);
        Self {
            inner,
            js_source,
            user_ffqn,
            user_params,
            user_exports_noext: vec![fn_metadata],
            user_exports_ext: fn_metadata_ext,
            user_exports_hierarchy_ext: hierarchy,
        }
    }

    #[must_use]
    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        &self.user_exports_ext
    }

    #[must_use]
    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        &self.user_exports_hierarchy_ext
    }

    #[must_use]
    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        self.inner.imported_functions()
    }

    pub fn link(
        self,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Result<WorkflowJsWorkerLinked, crate::WasmFileError> {
        let linked = self.inner.link(fn_registry)?;
        Ok(WorkflowJsWorkerLinked {
            inner: linked,
            js_source: self.js_source,
            user_ffqn: self.user_ffqn,
            user_params: self.user_params,
            user_exports_noext: self.user_exports_noext,
        })
    }
}

pub struct WorkflowJsWorkerLinked {
    inner: super::workflow_worker::WorkflowWorkerLinked,
    js_source: String,
    user_ffqn: FunctionFqn,
    user_params: Vec<ParameterType>,
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
            user_ffqn: self.user_ffqn,
            user_params: self.user_params,
            user_exports_noext: self.user_exports_noext,
        }
    }
}

pub struct WorkflowJsWorker {
    inner: WorkflowWorker,
    js_source: String,
    user_ffqn: FunctionFqn,
    user_params: Vec<ParameterType>,
    user_exports_noext: Vec<FunctionMetadata>,
}

#[async_trait]
impl Worker for WorkflowJsWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    // Return result<string, string> or a WorkerError mapped from `JsRuntimeError`
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
            // Copied from workflow_js_runtime_builder::exports::obelisk_workflow::workflow_js_runtime::execute::RUN
            FunctionFqn::new_static_tuple(("obelisk-workflow:workflow-js-runtime/execute", "run"));
        let boa_params: Arc<[serde_json::Value]> = Arc::from([
            serde_json::Value::String(self.user_ffqn.function_name.to_string()),
            serde_json::Value::String(self.js_source.clone()),
            serde_json::Value::Array(params_json_list),
        ]);
        ctx.params = Params::from_json_values(
            boa_params,
            [
                &TypeWrapper::String,
                &TypeWrapper::String,
                &TypeWrapper::List(Box::new(TypeWrapper::String)),
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
            WorkerResultOk::Finished {
                retval,
                version,
                http_client_traces,
            } => {
                match retval {
                    SupportedFunctionReturnValue::Ok {
                        ok:
                            Some(WastValWithType {
                                r#type:
                                    TypeWrapper::Result {
                                        ok: Some(ok_type),
                                        err: Some(_),
                                    },
                                value: WastVal::Result(Ok(Some(ok_val))),
                            }),
                    } => {
                        // js runtime returned {"ok": {"ok":"some string"}}
                        assert_eq!(TypeWrapper::String, *ok_type);
                        Ok(WorkerResultOk::Finished {
                            retval: SupportedFunctionReturnValue::Ok {
                                ok: Some(WastValWithType {
                                    r#type: *ok_type,
                                    value: *ok_val,
                                }),
                            },
                            version,
                            http_client_traces,
                        })
                    }

                    SupportedFunctionReturnValue::Ok {
                        ok:
                            Some(WastValWithType {
                                r#type:
                                    TypeWrapper::Result {
                                        ok: Some(_),
                                        err: Some(err_type),
                                    },
                                value: WastVal::Result(Err(Some(err_val))),
                            }),
                    } => {
                        // js runtime returned {"ok":{"err":"some string"}}
                        assert_eq!(TypeWrapper::String, *err_type);
                        Ok(WorkerResultOk::Finished {
                            retval: SupportedFunctionReturnValue::Err {
                                err: Some(WastValWithType {
                                    r#type: *err_type,
                                    value: *err_val,
                                }),
                            },
                            version,
                            http_client_traces,
                        })
                    }

                    SupportedFunctionReturnValue::Err {
                        err: Some(js_runtime_err),
                    } => {
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
                            "cannot_declare_function" | "function_not_found" => {
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
                                Ok(WorkerResultOk::Finished {
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
                        Ok(WorkerResultOk::Finished {
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
        exim: &ExIm,
        engine: Arc<Engine>,
        fn_registry: Arc<dyn FunctionRegistry>,
        db_conn: &dyn DbConnection,
        execution_id: ExecutionId,
        logs_storage_config: Option<LogStrageConfig>,
        js_source: String,
        user_ffqn: FunctionFqn,
    ) -> Result<(), ReplayError> {
        let clock_fn = ConstClock(chrono::DateTime::from_timestamp_nanos(0));

        let config = WorkflowConfig {
            join_next_blocking_strategy:
                super::workflow_worker::JoinNextBlockingStrategy::Interrupt,
            backtrace_persist: false,
            lock_extension: Duration::ZERO,
            subscription_interruption: None,
            component_id,
            stub_wasi: true,
            fuel: None,
        };

        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let is_finished = log.is_finished();

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
            serde_json::Value::String(user_ffqn.function_name.to_string()),
            serde_json::Value::String(js_source),
            serde_json::Value::Array(params_json_list),
        ]);
        let transformed_params = Params::from_json_values(
            boa_params,
            [
                &TypeWrapper::String,
                &TypeWrapper::String,
                &TypeWrapper::List(Box::new(TypeWrapper::String)),
            ]
            .into_iter(),
        )
        .expect("types checked at compile time");

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
        worker
            .run_internal(ctx, is_finished)
            .await
            .map(|_| ())
            .map_err(ReplayError::from)
    }
}

/// Create the `FunctionMetadata` for the user's JS workflow function with the given parameters.
/// Return type is always `result<string, string>`.
fn make_fn_metadata(ffqn: FunctionFqn, params: &[ParameterType]) -> FunctionMetadata {
    let return_type_wrapper = TypeWrapper::Result {
        ok: Some(Box::new(TypeWrapper::String)),
        err: Some(Box::new(TypeWrapper::String)),
    };
    FunctionMetadata {
        ffqn,
        parameter_types: ParameterTypes(params.to_vec()),
        return_type: ReturnType::detect(
            return_type_wrapper,
            StrVariant::Static("result<string, string>"),
        ),
        extension: None,
        submittable: true,
    }
}

/// Generate extension functions (submit, await-next, schedule, get) from the primary function.
fn make_fn_metadata_ext(primary: &FunctionMetadata) -> Vec<FunctionMetadata> {
    // For now, just return the primary function.
    // Extension functions will be generated by the ExIm::decode machinery
    // when the ComponentConfig is registered.
    vec![primary.clone()]
}

fn make_exports_hierarchy(
    primary: &FunctionMetadata,
    _ext: &[FunctionMetadata],
) -> Vec<PackageIfcFns> {
    let mut fns = IndexMap::new();
    fns.insert(primary.ffqn.function_name.clone(), primary.clone());
    vec![PackageIfcFns {
        ifc_fqn: primary.ffqn.ifc_fqn.clone(),
        extension: false,
        fns,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RunnableComponent;
    use crate::activity::activity_worker::tests::{compile_activity, new_activity_fibo};
    use crate::engines::{EngineConfig, Engines};
    use crate::testing_fn_registry::TestingFnRegistry;
    use crate::workflow::deadline_tracker::DeadlineTrackerFactoryTokio;
    use crate::workflow::workflow_worker::{JoinNextBlockingStrategy, WorkflowConfig};
    use assert_matches::assert_matches;
    use concepts::component_id::{CONTENT_DIGEST_DUMMY, InputContentDigest};
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, DelayId, ExecutorId, RunId};
    use concepts::storage::{
        CreateRequest, DbPool, DbPoolCloseable, JoinSetResponse, Locked, PendingState,
        PendingStateFinished, PendingStateFinishedError, PendingStateFinishedResultKind, Version,
    };
    use concepts::time::{ClockFn, Now, TokioSleep};
    use concepts::{
        ComponentRetryConfig, ComponentType, ExecutionId, ExecutionMetadata, StrVariant,
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

    fn new_js_workflow_worker(js_source: &str, user_ffqn: FunctionFqn) -> Arc<dyn Worker> {
        let engine = Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let cancel_registry = CancelRegistry::new();
        let clock_fn: Box<dyn ClockFn> = Now.clone_box();

        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            InputContentDigest(CONTENT_DIGEST_DUMMY),
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
            lock_extension: Duration::from_secs(1),
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
            user_ffqn,
            vec![ParameterType {
                type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
                name: StrVariant::Static("params"),
                wit_type: StrVariant::Static("list<string>"),
            }],
        );

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        let linked = js_compiled.link(fn_registry).unwrap();

        let db_pool = Arc::new(InMemoryPool::new());
        let deadline_factory = Arc::new(DeadlineTrackerFactoryTokio {
            leeway: Duration::ZERO,
            clock_fn,
        });

        Arc::new(linked.into_worker(
            DEPLOYMENT_ID_DUMMY,
            db_pool,
            deadline_factory,
            cancel_registry,
            None,
        ))
    }

    fn make_worker_context(ffqn: FunctionFqn, params: &[String]) -> WorkerContext {
        // The user function signature is: func(params: list<string>) -> result<string, string>
        // So we wrap the params in a list
        let params_json: Vec<serde_json::Value> = vec![json!(params)];
        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            InputContentDigest(CONTENT_DIGEST_DUMMY),
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
        }
    }

    fn extract_string(val: &WastVal) -> String {
        match val {
            WastVal::String(s) => s.clone(),
            other => panic!("expected string, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn js_workflow_simple_return() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            function hello() {
                return "hello world";
            }
        "#;

        let worker = new_js_workflow_worker(js_source, ffqn.clone());
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "hello world");
    }

    #[tokio::test]
    async fn js_workflow_with_params() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "greet");
        let js_source = r#"
            function greet(params) {
                let name = params[0];
                let greeting = params[1];
                return greeting + ", " + name + "!";
            }
        "#;

        let worker = new_js_workflow_worker(js_source, ffqn.clone());
        let ctx = make_worker_context(ffqn, &["World".to_string(), "Hello".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "Hello, World!");
    }

    #[tokio::test]
    async fn js_workflow_with_throw_string() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fail");
        let js_source = r#"
            function fail() {
                throw "something went wrong";
            }
        "#;

        let worker = new_js_workflow_worker(js_source, ffqn.clone());
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        // For result<string, string>, a throw becomes Err
        let err_val = assert_matches!(retval, SupportedFunctionReturnValue::Err { err } => err);
        let err_val = err_val.expect("should have err value");
        assert_eq!(extract_string(&err_val.value), "something went wrong");
    }

    #[tokio::test]
    async fn js_workflow_syntax_error_should_fail_to_instantiate() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "broken");
        let js_source = r"
            function broken( {
                return 'this has a syntax error';
            }
        ";

        let worker = new_js_workflow_worker(js_source, ffqn.clone());
        let ctx = make_worker_context(ffqn, &[]);

        let err = worker.run(ctx).await.unwrap_err();
        assert_matches!(
            err,
            WorkerError::FatalError(
                FatalError::CannotInstantiate { reason, detail: _ },
                _version,
            ) => {
                assert!(reason.contains("cannot_declare_function"), "reason: {reason}");
            }
        );
    }

    #[tokio::test]
    async fn js_workflow_function_not_found_should_fail_to_instantiate() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "missing");
        let js_source = r"
            function some_other_function() {
                return 'hello';
            }
        ";

        let worker = new_js_workflow_worker(js_source, ffqn.clone());
        let ctx = make_worker_context(ffqn, &[]);

        let err = worker.run(ctx).await.unwrap_err();
        assert_matches!(
            err,
            WorkerError::FatalError(
                FatalError::CannotInstantiate { reason, detail: _ },
                _version,
            ) => {
                assert!(reason.contains("function_not_found"), "reason: {reason}");
            }
        );
    }

    // ==================== Workflow function tests ====================

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    async fn compile_js_workflow_worker(
        js_source: &str,
        user_ffqn: FunctionFqn,
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        fn_registry: Arc<dyn FunctionRegistry>,
        workflow_engine: Arc<Engine>,
    ) -> (WorkflowJsWorker, concepts::ComponentId, RunnableComponent) {
        let wasm_path = workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME;
        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            InputContentDigest(calculate_sha256_file(wasm_path).await.unwrap()),
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
            lock_extension: Duration::ZERO,
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
            user_ffqn,
            vec![ParameterType {
                type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
                name: StrVariant::Static("params"),
                wit_type: StrVariant::Static("list<string>"),
            }],
        );

        let linked = js_compiled.link(fn_registry).unwrap();

        let deadline_factory = Arc::new(DeadlineTrackerFactoryTokio {
            leeway: Duration::ZERO,
            clock_fn,
        });

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

    /// Test: joinNextTry successfully finds a delay response after it expires
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn js_workflow_join_next_try_found(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;
        let sim_clock = SimClock::epoch();

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test_delay");
        let js_source = r"function test_delay(params) {
            const js = obelisk.createJoinSet();

            /* Submit a short delay */
            const delayId = js.submitDelay({ milliseconds: 10 });

            /* Wait for it to expire using sleep */
            obelisk.sleep({ milliseconds: 20 });

            /* Now joinNextTry should find the response */
            const response = js.joinNextTry();

            /* After processing, joinNextTry should return allProcessed */
            const afterResponse = js.joinNextTry();

            return JSON.stringify({
                responseType: response.type,
                responseOk: response.ok,
                afterStatus: afterResponse.status
            });
        }";

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (worker, component_id, _runnable_component) = compile_js_workflow_worker(
            js_source,
            user_ffqn.clone(),
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
                ffqn: user_ffqn.clone(),
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

        // Run until blocked by sleep
        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        // Move time forward to expire the sleep
        sim_clock.move_time_forward(Duration::from_millis(30));
        expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
            .await
            .unwrap();

        // Resume workflow - should complete
        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        let res = db_connection
            .get_finished_result(&execution_id)
            .await
            .unwrap();

        let ok_val =
            assert_matches!(res, SupportedFunctionReturnValue::Ok { ok: Some(val) } => val);
        let json_str = assert_matches!(&ok_val.value, WastVal::String(s) => s);
        let result: serde_json::Value = serde_json::from_str(json_str).unwrap();

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
    async fn js_workflow_all_apis(
        database: Database,
        #[values(false, true)] activity_should_win: bool,
        #[values(false, true)] explicit_close: bool,
    ) {
        let (_guard, db_pool, db_close) = database.set_up().await;
        js_workflow_all_apis_inner(db_pool.clone(), activity_should_win, explicit_close).await;
        db_close.close().await;
    }

    async fn js_workflow_all_apis_inner(
        db_pool: Arc<dyn DbPool>,
        activity_should_win: bool,
        explicit_close: bool,
    ) {
        test_utils::set_up();
        let sim_clock = SimClock::epoch();

        // JS code that exercises all workflow-support APIs
        let js_source = r"function test_all_apis(params) {
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

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test_all_apis");

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
        ]);

        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            user_ffqn.clone(),
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
                ffqn: user_ffqn.clone(),
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
        let ok_val =
            assert_matches!(res, SupportedFunctionReturnValue::Ok { ok: Some(val) } => val);
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
            user_ffqn,
        )
        .await
        .unwrap();
        info!("Replayed in {:?}", stopwatch.elapsed());
        // Nothing should be added to logs
        let mut buffer = Vec::new();
        let received = log_storage_recv.recv_many(&mut buffer, 100).await;
        assert_eq!(0, received, "expected no new messages, got {buffer:?}");
    }
}
