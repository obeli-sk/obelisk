//! JS workflow worker that wraps a `WorkflowWorker` running the Boa WASM component.
//!
//! The Boa component exports `obelisk-workflow:workflow-js-runtime/execute.run(fn-name, js-code, params-json) -> result<result<string, string>, js-runtime-error>`.
//! This wrapper translates the user's typed interface `func(params: list<string>) -> result<string, string>`
//! into calls to the Boa component.

use super::workflow_worker::{WorkflowWorker, WorkflowWorkerCompiled};
use crate::activity::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use crate::workflow::deadline_tracker::DeadlineTrackerFactory;
use async_trait::async_trait;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::DbPool;
use concepts::{
    ExecutionFailureKind, FinishedExecutionError, FunctionFqn, FunctionMetadata, FunctionRegistry,
    PackageIfcFns, ParameterType, ParameterTypes, Params, ResultParsingError,
    ResultParsingErrorFromVal, ReturnType, StrVariant, SupportedFunctionReturnValue,
};
use executor::worker::{
    FatalError, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use indexmap::IndexMap;
use std::sync::Arc;
use tracing::debug;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};

/// Compiled JS workflow. Holds the compiled Boa WASM component + JS source + user FFQN.
pub struct WorkflowJsWorkerCompiled {
    inner: WorkflowWorkerCompiled,
    js_source: String,
    user_ffqn: FunctionFqn,
    user_exports_noext: Vec<FunctionMetadata>,
    user_exports_ext: Vec<FunctionMetadata>,
    user_exports_hierarchy_ext: Vec<PackageIfcFns>,
}

impl WorkflowJsWorkerCompiled {
    #[must_use]
    pub fn new(inner: WorkflowWorkerCompiled, js_source: String, user_ffqn: FunctionFqn) -> Self {
        let fn_metadata = make_fn_metadata(user_ffqn.clone());
        let fn_metadata_ext = make_fn_metadata_ext(&fn_metadata);
        let hierarchy = make_exports_hierarchy(&fn_metadata, &fn_metadata_ext);
        Self {
            inner,
            js_source,
            user_ffqn,
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
            user_exports_noext: self.user_exports_noext,
        })
    }
}

pub struct WorkflowJsWorkerLinked {
    inner: super::workflow_worker::WorkflowWorkerLinked,
    js_source: String,
    user_ffqn: FunctionFqn,
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
            user_exports_noext: self.user_exports_noext,
        }
    }
}

pub struct WorkflowJsWorker {
    inner: WorkflowWorker,
    js_source: String,
    user_ffqn: FunctionFqn,
    user_exports_noext: Vec<FunctionMetadata>,
}

#[async_trait]
impl Worker for WorkflowJsWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    // Return result<string, string> or a WorkerError mapped from `JsRuntimeError`
    async fn run(&self, mut ctx: WorkerContext) -> WorkerResult {
        // Expecting a single parameter of type `list<string>`.
        assert_eq!(
            1,
            ctx.params.len(),
            "type checked in Params::from_json_values"
        );
        let json_params = ctx
            .params
            .as_json_values()
            .expect("params come from database, not wasmtime"); // TODO: Extract ParamsInternal
        let list_of_strings = json_params.first().expect("checked above");
        assert!(
            list_of_strings.is_array(),
            "params must have been type checked to adhere to `list<string>` when `Params` was constructed"
        );

        let params_json_str =
            serde_json::to_string(list_of_strings).expect("serde_json::Value must be serializable");

        // Rewrite context to call workflow-js-runtime
        ctx.ffqn =
            // Copied from workflow_js_runtime_builder::exports::obelisk_workflow::workflow_js_runtime::execute::RUN
            FunctionFqn::new_static_tuple(("obelisk-workflow:workflow-js-runtime/execute", "run"));
        let boa_params: Arc<[serde_json::Value]> = Arc::from([
            serde_json::Value::String(self.user_ffqn.function_name.to_string()),
            serde_json::Value::String(self.js_source.clone()),
            serde_json::Value::String(params_json_str),
        ]);
        ctx.params = Params::from_json_values(
            boa_params,
            [
                &TypeWrapper::String,
                &TypeWrapper::String,
                &TypeWrapper::String,
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

/// Create the `FunctionMetadata` for the user's function:
/// `func(params: list<string>) -> result<string, string>`
fn make_fn_metadata(ffqn: FunctionFqn) -> FunctionMetadata {
    let param_type = ParameterType {
        type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
        name: StrVariant::Static("params"),
        wit_type: StrVariant::Static("list<string>"),
    };
    let return_type_wrapper = TypeWrapper::Result {
        ok: Some(Box::new(TypeWrapper::String)),
        err: Some(Box::new(TypeWrapper::String)),
    };
    FunctionMetadata {
        ffqn,
        parameter_types: ParameterTypes(vec![param_type]),
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
    use crate::engines::{EngineConfig, Engines};
    use crate::testing_fn_registry::TestingFnRegistry;
    use crate::workflow::deadline_tracker::DeadlineTrackerFactoryTokio;
    use crate::workflow::workflow_worker::WorkflowConfig;
    use assert_matches::assert_matches;
    use concepts::component_id::{CONTENT_DIGEST_DUMMY, InputContentDigest};
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId};
    use concepts::storage::{Locked, Version};
    use concepts::time::{ClockFn, Now};
    use concepts::{
        ComponentRetryConfig, ComponentType, ExecutionId, ExecutionMetadata, StrVariant,
    };
    use db_mem::inmemory_dao::InMemoryPool;
    use executor::worker::{WorkerContext, WorkerError, WorkerResultOk};
    use serde_json::json;
    use std::time::Duration;
    use tracing::info_span;
    use val_json::wast_val::WastVal;

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
        let runnable =
            RunnableComponent::new(wasm_path, &engine, component_id.component_type).unwrap();

        let config = WorkflowConfig {
            component_id: component_id.clone(),
            join_next_blocking_strategy:
                super::super::workflow_worker::JoinNextBlockingStrategy::Interrupt,
            backtrace_persist: false,
            stub_wasi: false,
            fuel: None,
            lock_extension: Duration::from_secs(1),
            subscription_interruption: None,
        };

        let compiled =
            WorkflowWorkerCompiled::new_with_config(runnable, config, engine, clock_fn.clone_box())
                .unwrap();

        let js_compiled = WorkflowJsWorkerCompiled::new(compiled, js_source.to_string(), user_ffqn);

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
}
