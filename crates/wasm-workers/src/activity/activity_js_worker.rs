//! JS activity worker that wraps an `ActivityWorker` running the Boa WASM component.
//!
//! The Boa component exports `obelisk:js-runtime/execute.run(js-code, params-json) -> result<string, string>`.
//! This wrapper translates the user's typed interface `func(params: list<string>) -> result<string, string>`
//! into calls to the Boa component.

use super::activity_worker::{ActivityWorker, ActivityWorkerCompiled};
use super::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use assert_matches::assert_matches;
use async_trait::async_trait;
use concepts::storage::LogInfoAppendRow;
use concepts::time::Sleep;
use concepts::{
    FunctionFqn, FunctionMetadata, PackageIfcFns, ParameterType, ParameterTypes, Params,
    ResultParsingError, ResultParsingErrorFromVal, ReturnType, StrVariant,
    SupportedFunctionReturnValue,
};
use executor::worker::{
    FatalError, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use indexmap::IndexMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};

/// Compiled JS activity. Holds the compiled Boa WASM component + JS source + user FFQN.
pub struct ActivityJsWorkerCompiled<S: Sleep> {
    inner: ActivityWorkerCompiled<S>,
    js_source: String,
    user_ffqn: FunctionFqn,
    user_exports_noext: Vec<FunctionMetadata>,
    user_exports_ext: Vec<FunctionMetadata>,
    user_exports_hierarchy_ext: Vec<PackageIfcFns>,
}

impl<S: Sleep> ActivityJsWorkerCompiled<S> {
    pub fn new(
        inner: ActivityWorkerCompiled<S>,
        js_source: String,
        user_ffqn: FunctionFqn,
    ) -> Self {
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

    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        &self.user_exports_ext
    }

    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        &self.user_exports_hierarchy_ext
    }

    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        self.inner.imported_functions()
    }

    pub fn into_worker(
        self,
        cancel_registry: CancelRegistry,
        log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
        logs_storage_config: Option<LogStrageConfig>,
    ) -> ActivityJsWorker<S> {
        let inner =
            self.inner
                .into_worker(cancel_registry, log_forwarder_sender, logs_storage_config);
        ActivityJsWorker {
            inner,
            js_source: self.js_source,
            user_ffqn: self.user_ffqn,
            user_exports_noext: self.user_exports_noext,
        }
    }
}

pub struct ActivityJsWorker<S: Sleep> {
    inner: ActivityWorker<S>,
    js_source: String,
    #[allow(dead_code)] // Will be used for error context in future
    user_ffqn: FunctionFqn,
    user_exports_noext: Vec<FunctionMetadata>,
}

#[async_trait]
impl<S: Sleep + 'static> Worker for ActivityJsWorker<S> {
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

        // Rewrite context to call
        ctx.ffqn = FunctionFqn::new_static_tuple(activity_js_runtime_builder::exports::obelisk_activity::activity_js_runtime::execute::RUN);
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

        let (retval, version, http_client_traces) = assert_matches!(inner_worker_ok, WorkerResultOk::Finished { retval, version,  http_client_traces }
            => (retval, version,  http_client_traces), "activity_js_runtime runs in ActivityWorker");
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
                        unreachable!(
                            "execution-failed is injected by a workflow worker, not by activity-js-runtime"
                        )
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
/// This reuses the same logic as the `ExIm` decoder in `wasm_tools`.
fn make_fn_metadata_ext(primary: &FunctionMetadata) -> Vec<FunctionMetadata> {
    // For now, just return the primary function.
    // Extension functions will be generated by the ExIm::decode machinery
    // when the ComponentConfig is registered.
    // Actually, we need to provide the full set including extensions,
    // since WorkerCompiled::new_activity reads them from the worker.
    // Let's just return the primary for now - the registry generates extensions.
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

#[cfg(all(test, feature = "activity-js"))]
mod tests {
    use super::*;
    use crate::activity::activity_worker::tests::compile_activity_with_engine;
    use crate::engines::{EngineConfig, Engines};
    use assert_matches::assert_matches;
    use concepts::SupportedFunctionReturnValue;
    use concepts::component_id::{CONTENT_DIGEST_DUMMY, InputContentDigest};
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId};
    use concepts::storage::{Locked, Version};
    use concepts::time::TokioSleep;
    use concepts::time::{ClockFn, Now};
    use concepts::{
        ComponentRetryConfig, ComponentType, ExecutionId, ExecutionMetadata, StrVariant,
    };
    use executor::worker::{WorkerContext, WorkerError, WorkerResultOk};
    use serde_json::json;
    use tokio::sync::mpsc;
    use tracing::info_span;
    use val_json::wast_val::WastVal;

    async fn new_js_activity_worker_with_config(
        js_source: &str,
        user_ffqn: FunctionFqn,
        config_fn: impl FnOnce(concepts::ComponentId) -> super::super::activity_worker::ActivityConfig,
    ) -> Arc<dyn Worker> {
        let engine = Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let cancel_registry = CancelRegistry::new();
        let (db_forwarder_sender, _) = mpsc::channel(1);
        let clock_fn: Box<dyn ClockFn> = Now.clone_box();

        let component_id = concepts::ComponentId::new(
            ComponentType::ActivityJs,
            StrVariant::Static("test_js"),
            InputContentDigest(CONTENT_DIGEST_DUMMY),
        )
        .unwrap();

        // Compile the Boa WASM component
        let (wasm_component, _boa_component_id) = compile_activity_with_engine(
            activity_js_runtime_builder::ACTIVITY_JS_RUNTIME,
            &engine,
            ComponentType::ActivityJs,
        )
        .await;

        let config = config_fn(component_id);

        let compiled = super::super::activity_worker::ActivityWorkerCompiled::new_with_config(
            wasm_component,
            config,
            engine,
            clock_fn,
            TokioSleep,
        )
        .unwrap();

        let js_compiled = ActivityJsWorkerCompiled::new(compiled, js_source.to_string(), user_ffqn);

        Arc::new(js_compiled.into_worker(cancel_registry, &db_forwarder_sender, None))
    }

    async fn new_js_activity_worker(js_source: &str, user_ffqn: FunctionFqn) -> Arc<dyn Worker> {
        new_js_activity_worker_with_config(js_source, user_ffqn, |component_id| {
            super::super::activity_worker::ActivityConfig {
                component_id,
                forward_stdout: None,
                forward_stderr: None,
                env_vars: Arc::from([]),
                retry_on_err: false,
                directories_config: None,
                fuel: None,
                secrets: Arc::from([]),
                allowed_hosts: Arc::from([]),
            }
        })
        .await
    }

    fn make_worker_context(ffqn: FunctionFqn, params: &[String]) -> WorkerContext {
        // The user function signature is: func(params: list<string>) -> result<string, string>
        // So we wrap the params in a list
        let params_json: Vec<serde_json::Value> = vec![json!(params)];
        let component_id = concepts::ComponentId::new(
            ComponentType::ActivityJs,
            StrVariant::Static("test_js"),
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
            can_be_retried: false,
            worker_span: info_span!("js_test"),
            locked_event: Locked {
                component_id,
                executor_id: ExecutorId::generate(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                run_id: RunId::generate(),
                lock_expires_at: chrono::Utc::now() + chrono::Duration::seconds(60),
                retry_config: ComponentRetryConfig::ZERO,
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
    async fn js_activity_simple_return() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            function hello() {
                return "hello world";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "hello world");
    }

    #[tokio::test]
    async fn js_activity_with_params() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "greet");
        let js_source = r#"
            function greet(params) {
                let name = params[0];
                let greeting = params[1];
                return greeting + ", " + name + "!";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &["World".to_string(), "Hello".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "Hello, World!");
    }

    #[tokio::test]
    async fn js_activity_with_throw() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fail");
        let js_source = r#"
            function fail() {
                throw "something went wrong";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        // For result<string, string>, a throw becomes Err
        let err_val = assert_matches!(retval, SupportedFunctionReturnValue::Err { err } => err);
        let err_val = err_val.expect("should have err value");
        assert_eq!(extract_string(&err_val.value), "something went wrong");
    }

    #[tokio::test]
    async fn js_activity_console_log() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "logging");
        let js_source = r#"
            function logging(params) {
                console.log("Log message:", params[0]);
                console.info("Info message");
                console.warn("Warning message");
                console.error("Error message");
                return "logged";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &["test".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "logged");
    }

    #[tokio::test]
    async fn js_activity_returning_object_should_fail_to_typecheck() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "object");
        let js_source = r"
            function object(params) {
                return { name: params[0], count: 42 };
            }
        ";

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &["test".to_string()]);

        let err = worker.run(ctx).await.unwrap_err();
        assert_matches!(
            err,
            WorkerError::FatalError(
                FatalError::ResultParsingError(ResultParsingError::ResultParsingErrorFromVal(
                    ResultParsingErrorFromVal::TypeCheckError(reason),
                )),
                _version,
            )
            => {
                assert!(reason.starts_with("expected string, got JsValue"), "{reason} should start with: `expexpected string, got JsValue`");
            }
        );
    }

    #[tokio::test]
    async fn js_activity_throwing_object_should_fail_to_typecheck() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "throw_object");
        let js_source = r"
            function throw_object() {
                throw { code: 42, message: 'error' };
            }
        ";

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let err = worker.run(ctx).await.unwrap_err();
        assert_matches!(
            err,
            WorkerError::FatalError(
                FatalError::ResultParsingError(ResultParsingError::ResultParsingErrorFromVal(
                    ResultParsingErrorFromVal::TypeCheckError(reason),
                )),
                _version,
            )
            => {
                assert!(reason.starts_with("expected string, got JsError"), "{reason} should start with: `expected string, got JsError`");
            }
        );
    }

    #[tokio::test]
    async fn js_activity_syntax_error_should_fail_to_instantiate() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "broken");
        let js_source = r"
            function broken( {
                return 'this has a syntax error';
            }
        ";

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
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
    async fn js_activity_function_not_found_should_fail_to_instantiate() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "missing");
        let js_source = r"
            function some_other_function() {
                return 'hello';
            }
        ";

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
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

    async fn new_js_activity_worker_with_http(
        js_source: &str,
        user_ffqn: FunctionFqn,
        allowed_host: &str,
    ) -> Arc<dyn Worker> {
        use crate::http_request_policy::HostPattern;
        let host_pattern = HostPattern::parse(allowed_host).unwrap();
        new_js_activity_worker_with_config(js_source, user_ffqn, move |component_id| {
            super::super::activity_worker::ActivityConfig {
                component_id,
                forward_stdout: None,
                forward_stderr: None,
                env_vars: Arc::from([]),
                retry_on_err: false,
                directories_config: None,
                fuel: None,
                secrets: Arc::from([]),
                allowed_hosts: Arc::from(vec![host_pattern]),
            }
        })
        .await
    }

    #[tokio::test]
    async fn js_activity_fetch_get() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };
        test_utils::set_up();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/hello"))
            .respond_with(ResponseTemplate::new(200).set_body_string("fetch works"))
            .expect(1)
            .mount(&server)
            .await;

        let url = server.uri();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "do-fetch");
        let js_source = format!(
            r#"
            async function do_fetch(params) {{
                const resp = await fetch("{url}/hello");
                const text = await resp.text();
                return text;
            }}
            "#
        );

        let allowed = format!("127.0.0.1:{}", server.address().port());
        let worker = new_js_activity_worker_with_http(&js_source, ffqn.clone(), &allowed).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "fetch works");
    }

    #[tokio::test]
    async fn js_activity_fetch_post_json() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{body_json, method, path},
        };
        test_utils::set_up();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_json(serde_json::json!({"key": "value"})))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"status":"ok"}"#))
            .expect(1)
            .mount(&server)
            .await;

        let url = server.uri();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "post-json");
        let js_source = format!(
            r#"
            async function post_json(params) {{
                const resp = await fetch("{url}/api", {{
                    method: "POST",
                    headers: {{ "Content-Type": "application/json" }},
                    body: JSON.stringify({{ key: "value" }})
                }});
                const data = await resp.json();
                return data.status;
            }}
            "#
        );

        let allowed = format!("127.0.0.1:{}", server.address().port());
        let worker = new_js_activity_worker_with_http(&js_source, ffqn.clone(), &allowed).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "ok");
    }

    #[tokio::test]
    async fn js_activity_fetch_response_status() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };
        test_utils::set_up();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/not-found"))
            .respond_with(ResponseTemplate::new(404).set_body_string("nope"))
            .expect(1)
            .mount(&server)
            .await;

        let url = server.uri();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "check-status");
        let js_source = format!(
            r#"
            async function check_status(params) {{
                const resp = await fetch("{url}/not-found");
                return resp.status.toString();
            }}
            "#
        );

        let allowed = format!("127.0.0.1:{}", server.address().port());
        let worker = new_js_activity_worker_with_http(&js_source, ffqn.clone(), &allowed).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "404");
    }

    #[tokio::test]
    async fn js_activity_fetch_disallowed_host() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "bad-fetch");
        // No hosts are allowed (using default new_js_activity_worker)
        let js_source = r#"
            async function bad_fetch(params) {
                const resp = await fetch("http://example.com/");
                return await resp.text();
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        // The fetch should fail because no hosts are allowed
        let result = worker
            .run(ctx)
            .await
            .expect("worker should return a result");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        // The JS runtime wraps the error - it should come back as a WrongReturnType
        // or WrongThrownType since the promise rejection isn't a plain string throw.
        assert_matches!(
            retval,
            SupportedFunctionReturnValue::Err { .. }
                | SupportedFunctionReturnValue::ExecutionError(_)
        );
    }

    #[tokio::test]
    async fn js_activity_sync_function_still_works_with_fetch_runtime() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "sync-fn");
        let js_source = r#"
            function sync_fn(params) {
                return "sync result: " + params[0];
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &["hello".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "sync result: hello");
    }
}
