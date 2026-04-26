//! JS activity worker that wraps an `ActivityWorker` running the Boa WASM component.
//!
//! The Boa component exports `obelisk:js-runtime/execute.run(js-code, params-json) -> result<string, string>`.
//! This wrapper translates the user's typed interface `func(params) -> result<T, string>`
//! into calls to the Boa component, deserializing the JSON-encoded ok string as the configured type.

use super::activity_worker::{ActivityWorker, ActivityWorkerCompiled};
use super::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use assert_matches::assert_matches;
use async_trait::async_trait;
use concepts::storage::LogInfoAppendRow;
use concepts::{
    ComponentType, FunctionFqn, FunctionMetadata, PackageIfcFns, ParameterType, Params,
    ResultParsingError, ResultParsingErrorFromVal, ReturnTypeExtendable,
    SupportedFunctionReturnValue,
};
use executor::worker::{
    FatalError, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;
use utils::wasm_tools::WasmComponent;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};

/// Compiled JS activity. Holds the compiled Boa WASM component + JS source + user FFQN.
pub struct ActivityJsWorkerCompiled {
    inner: ActivityWorkerCompiled,
    js_source: String,
    user_ffqn: FunctionFqn,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    /// User interface parsed from synthesized WIT — provides exports, extensions, and WIT text.
    user_wasm_component: WasmComponent,
}

impl ActivityJsWorkerCompiled {
    pub fn new(
        inner: ActivityWorkerCompiled,
        js_source: String,
        user_ffqn: FunctionFqn,
        user_params: Vec<ParameterType>,
        user_return_type: ReturnTypeExtendable,
    ) -> Result<Self, utils::wasm_tools::DecodeError> {
        let user_wasm_component = WasmComponent::new_from_fn_signature(
            &user_ffqn,
            &user_params,
            &user_return_type,
            ComponentType::Activity,
            "js-activity",
        )?;
        Ok(Self {
            inner,
            js_source,
            user_ffqn,
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
    #[must_use]
    pub fn wit(&self) -> String {
        self.user_wasm_component.wit()
    }

    #[must_use]
    pub fn into_worker(
        self,
        cancel_registry: CancelRegistry,
        log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
        logs_storage_config: Option<LogStrageConfig>,
    ) -> ActivityJsWorker {
        let inner =
            self.inner
                .into_worker(cancel_registry, log_forwarder_sender, logs_storage_config);
        ActivityJsWorker {
            inner,
            js_source: self.js_source,
            user_ffqn: self.user_ffqn,
            user_params: self.user_params,
            user_return_type: self.user_return_type,
            user_exports_noext: self.user_wasm_component.exported_functions(false).to_vec(),
        }
    }
}

pub struct ActivityJsWorker {
    inner: ActivityWorker,
    js_source: String,
    #[allow(dead_code)] // Will be used for error context in future
    user_ffqn: FunctionFqn,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    user_exports_noext: Vec<FunctionMetadata>,
}

#[async_trait]
impl Worker for ActivityJsWorker {
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

        // Rewrite context to call
        ctx.ffqn =
            FunctionFqn::new_static_tuple(("obelisk-activity:activity-js-runtime/execute", "run"));
        // run: func(js-code: string, params-json: list<string>) -> result<result<string, string>, js-runtime-error>
        let boa_params: Arc<[serde_json::Value]> = Arc::from([
            serde_json::Value::String(self.js_source.clone()),
            serde_json::Value::Array(params_json_list),
        ]);
        ctx.params = Params::from_json_values(
            boa_params,
            [
                &TypeWrapper::String,
                &TypeWrapper::List(Box::new(TypeWrapper::String)),
            ]
            .into_iter(),
        )
        .expect("types checked at compile time");

        let inner_worker_ok = self.inner.run(ctx).await?;
        debug!("Activity worker returned {inner_worker_ok:?}");

        let (retval, version, http_client_traces) = assert_matches!(inner_worker_ok, WorkerResultOk::RunFinished { retval, version,  http_client_traces }
            => (retval, version,  http_client_traces), "activity_js_runtime runs in ActivityWorker");
        match retval {
            SupportedFunctionReturnValue::Ok(Some(WastValWithType {
                r#type:
                    TypeWrapper::Result {
                        ok: Some(ok_type),   // String
                        err: Some(err_type), // err string was not sent
                    },
                value: WastVal::Result(Ok(Some(ok_val))), // activity-js-runtime returned {"ok": {"ok": "<json-encoded value>"}}
            })) if *ok_type == TypeWrapper::String && *err_type == TypeWrapper::String => {
                let WastVal::String(ok_val) = *ok_val else {
                    unreachable!("ok type is String, so value must be WastVal::String")
                };
                let Ok(ok_val) = serde_json::from_str(&ok_val) else {
                    unreachable!("activity-js-runtime always sends JSON-encoded string")
                };
                let retval = crate::js_worker_utils::map_js_ok_to_user_retval(
                    &ok_val,
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
                value: WastVal::Result(Err(Some(err_val))), // js runtime returned {"ok":{"err":"some string"}}
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
                        unreachable!(
                            "execution-failed is injected by a workflow worker, not by activity-js-runtime"
                        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::activity_worker::test::compile_activity_with_engine;
    use crate::activity::activity_worker::tests::TestRetryBehavior;
    use crate::engines::{EngineConfig, Engines};
    use crate::http_hooks::ConfigSectionHint;
    use assert_matches::assert_matches;
    use concepts::component_id::COMPONENT_DIGEST_DUMMY;
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId};
    use concepts::storage::{Locked, Version};
    use concepts::time::{ClockFn, Now, TokioSleep};
    use concepts::{
        ComponentRetryConfig, ComponentType, ExecutionId, ExecutionMetadata, StrVariant,
    };
    use concepts::{SupportedFunctionReturnValue, TypeWrapperTopLevel};
    use executor::worker::{WorkerContext, WorkerError, WorkerResultOk};
    use serde_json::json;
    use tokio::sync::mpsc;
    use tracing::info_span;
    use val_json::wast_val::WastVal;

    fn default_return_type() -> ReturnTypeExtendable {
        ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::String)),
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("result<string, string>"),
        }
    }

    struct JsWorkerBuilder {
        js_source: String,
        user_ffqn: FunctionFqn,
        user_params: Vec<ParameterType>,
        user_return_type: ReturnTypeExtendable,
        allowed_hosts: Vec<crate::http_request_policy::AllowedHostConfig>,
        logs_storage_config: Option<crate::component_logger::LogStrageConfig>,
    }

    impl JsWorkerBuilder {
        fn new(js_source: &str, user_ffqn: FunctionFqn) -> Self {
            Self {
                js_source: js_source.to_string(),
                user_ffqn,
                user_params: vec![ParameterType {
                    type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
                    name: StrVariant::Static("params"),
                    wit_type: StrVariant::Static("list<string>"),
                }],
                user_return_type: default_return_type(),
                allowed_hosts: Vec::new(),
                logs_storage_config: None,
            }
        }

        fn with_params(mut self, params: Vec<ParameterType>) -> Self {
            self.user_params = params;
            self
        }

        fn with_return_type(mut self, return_type: ReturnTypeExtendable) -> Self {
            self.user_return_type = return_type;
            self
        }

        fn with_allowed_host(mut self, host: &str) -> Self {
            use crate::http_request_policy::{AllowedHostConfig, HostPattern, MethodsPattern};
            let pattern =
                HostPattern::parse_with_methods(host, MethodsPattern::AllMethods).unwrap();
            self.allowed_hosts.push(AllowedHostConfig {
                pattern,
                secret_env_mappings: Vec::new(),
                replace_in: hashbrown::HashSet::new(),
            });
            self
        }

        fn with_logs(mut self, config: crate::component_logger::LogStrageConfig) -> Self {
            self.logs_storage_config = Some(config);
            self
        }

        async fn build(self) -> Arc<dyn Worker> {
            let engine =
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let cancel_registry = CancelRegistry::new();
            let (db_forwarder_sender, _) = mpsc::channel(1);
            let clock_fn: Box<dyn ClockFn> = Now.clone_box();

            let component_id = concepts::ComponentId::new(
                ComponentType::Activity,
                StrVariant::Static("test_js"),
                COMPONENT_DIGEST_DUMMY,
            )
            .unwrap();

            let (wasm_component, _boa_component_id) = compile_activity_with_engine(
                activity_js_runtime_builder::ACTIVITY_JS_RUNTIME,
                &engine,
                ComponentType::Activity,
            )
            .await;

            let config = super::super::activity_worker::ActivityConfig {
                component_id,
                forward_stdout: None,
                forward_stderr: None,
                env_vars: Arc::from([]),
                fuel: None,
                allowed_hosts: Arc::from(self.allowed_hosts),
                config_section_hint: ConfigSectionHint::ActivityJs,
            };

            let compiled = super::super::activity_worker::ActivityWorkerCompiled::new_with_config(
                wasm_component,
                config,
                engine,
                clock_fn,
                std::sync::Arc::new(TokioSleep),
            )
            .unwrap();

            let js_compiled = ActivityJsWorkerCompiled::new(
                compiled,
                self.js_source,
                self.user_ffqn,
                self.user_params,
                self.user_return_type,
            )
            .unwrap();

            Arc::new(js_compiled.into_worker(
                cancel_registry,
                &db_forwarder_sender,
                self.logs_storage_config,
            ))
        }
    }

    async fn new_js_activity_worker(js_source: &str, user_ffqn: FunctionFqn) -> Arc<dyn Worker> {
        JsWorkerBuilder::new(js_source, user_ffqn).build().await
    }

    async fn new_js_activity_worker_custom_params(
        js_source: &str,
        user_ffqn: FunctionFqn,
        user_params: Vec<ParameterType>,
    ) -> Arc<dyn Worker> {
        JsWorkerBuilder::new(js_source, user_ffqn)
            .with_params(user_params)
            .build()
            .await
    }

    fn make_worker_context(ffqn: FunctionFqn, params: &[String]) -> WorkerContext {
        // The user function signature is: func(params: list<string>) -> result<string, string>
        // So we wrap the params in a list
        let params_json: Vec<serde_json::Value> = vec![json!(params)];
        let component_id = concepts::ComponentId::new(
            ComponentType::Activity,
            StrVariant::Static("test_js"),
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
            executor_close_watcher: tokio::sync::watch::channel(false).1,
        }
    }

    fn make_worker_context_custom(
        ffqn: FunctionFqn,
        params_json: Vec<serde_json::Value>,
    ) -> WorkerContext {
        let component_id = concepts::ComponentId::new(
            ComponentType::Activity,
            StrVariant::Static("test_js"),
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
    async fn simple_anon_export() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            export default function() {
                return "hello world";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "hello world");
    }

    #[tokio::test]
    async fn simple_with_name() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            export default function simple() {
                return "hello world";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "hello world");
    }

    #[tokio::test]
    async fn async_simple() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            export default async function async_simple() {
                return "hello world";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "hello world");
    }

    #[tokio::test]
    async fn with_params() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "greet");
        let js_source = r#"
            export default function greet(params) {
                let name = params[0];
                let greeting = params[1];
                return greeting + ", " + name + "!";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &["World".to_string(), "Hello".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "Hello, World!");
    }

    #[tokio::test]
    async fn with_throw_string() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fail");
        let js_source = r#"
            export default function fail() {
                throw "something went wrong";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        // For result<string, string>, a throw becomes Err
        let err_val = assert_matches!(retval, SupportedFunctionReturnValue::Err(err) => err);
        let err_val = err_val.expect("should have err value");
        assert_eq!(extract_string(&err_val.value), "something went wrong");
    }

    #[tokio::test]
    async fn with_throw_error_object() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fail");
        let js_source = r#"
            export default function fail() {
                throw new Error("something went wrong");
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        // For result<string, string>, a throw becomes Err
        let err_val = assert_matches!(retval, SupportedFunctionReturnValue::Err(err) => err);
        let err_val = err_val.expect("should have err value");
        assert_eq!(extract_string(&err_val.value), "something went wrong");
    }

    #[tokio::test]
    async fn console_log() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "logging");
        let js_source = r#"
            export default function logging(params) {
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
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "logged");
    }

    #[tokio::test]
    async fn returning_object_should_fail_to_typecheck() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "object");
        let js_source = r"
            export default function object(params) {
                return { name: params[0], count: 42 };
            }
        ";

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &["test".to_string()]);

        // The JS object is JSON-serialized successfully, but fails to deserialize as result<string, string>
        // because the JSON object `{"name":"test","count":42}` is not a string.
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
                assert_eq!(
                    r#"failed to type check the return value `{"count":42,"name":"test"}` as `string`: invalid type: map, expected value matching "string" at line 1 column 1"#,
                    reason);
            }
        );
    }

    #[tokio::test]
    async fn throwing_object_should_fail_to_typecheck() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "throw-object");
        let js_source = r"
            export default function throw_object() {
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
                assert!(
                    reason.contains("failed to type check thrown value"),
                    "{reason} should mention type check failure"
                );
            }
        );
    }

    #[tokio::test]
    async fn no_default_export() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r"
            export function hello() {
                return 'notice not default';
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
                assert!(reason.contains("no_default_export"), "reason: {reason}");
            }
        );
    }

    #[tokio::test]
    async fn syntax_error_should_fail_to_instantiate() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "broken");
        let js_source = r"
            export default function broken( {
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
                assert!(reason.contains("module_parse_error"), "reason: {reason}");
            }
        );
    }

    async fn new_js_activity_worker_with_http(
        js_source: &str,
        user_ffqn: FunctionFqn,
        allowed_host: &str,
    ) -> Arc<dyn Worker> {
        JsWorkerBuilder::new(js_source, user_ffqn)
            .with_allowed_host(allowed_host)
            .build()
            .await
    }

    #[tokio::test]
    async fn fetch_get() {
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
            export default async function do_fetch(params) {{
                const resp = await fetch("{url}/hello");
                const text = await resp.text();
                return text;
            }}
            "#
        );

        let allowed = format!("http://127.0.0.1:{}", server.address().port());
        let worker = new_js_activity_worker_with_http(&js_source, ffqn.clone(), &allowed).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "fetch works");
    }

    #[tokio::test]
    async fn fetch_post_json() {
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
            export default async function post_json(params) {{
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

        let allowed = format!("http://127.0.0.1:{}", server.address().port());
        let worker = new_js_activity_worker_with_http(&js_source, ffqn.clone(), &allowed).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "ok");
    }

    #[tokio::test]
    async fn fetch_response_status() {
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
            export default async function check_status(params) {{
                const resp = await fetch("{url}/not-found");
                return "status:" + resp.status;
            }}
            "#
        );

        let allowed = format!("http://127.0.0.1:{}", server.address().port());
        let worker = new_js_activity_worker_with_http(&js_source, ffqn.clone(), &allowed).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "status:404");
    }

    #[tokio::test]
    async fn fetch_disallowed_host() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "bad-fetch");
        // No hosts are allowed
        let js_source = r#"
            export default async function bad_fetch(params) {
                const resp = await fetch("http://example.com/");
                return await resp.text();
            }
        "#;
        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let err_val = assert_matches!(retval, SupportedFunctionReturnValue::Err(err) => err);
        let err_str = err_val.expect("should have error value");
        let msg = extract_string(&err_str.value);
        assert_eq!("ErrorCode::HttpRequestDenied", msg);
    }

    #[tokio::test]
    async fn sync_function_still_works_with_fetch_runtime() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "sync-fn");
        let js_source = r#"
            export default function sync_fn(params) {
                return "sync result: " + params[0];
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &["hello".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "sync result: hello");
    }

    #[tokio::test]
    async fn custom_params_string_and_u32() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "greet-n-times");
        let js_source = r#"
            export default function greet_n_times(name, count) {
                let parts = [];
                for (let i = 0; i < count; i++) {
                    parts.push("Hello, " + name + "!");
                }
                return parts.join(" ");
            }
        "#;

        let user_params = vec![
            ParameterType {
                type_wrapper: TypeWrapper::String,
                name: StrVariant::Static("name"),
                wit_type: StrVariant::Static("string"),
            },
            ParameterType {
                type_wrapper: TypeWrapper::U32,
                name: StrVariant::Static("count"),
                wit_type: StrVariant::Static("u32"),
            },
        ];

        let worker =
            new_js_activity_worker_custom_params(js_source, ffqn.clone(), user_params).await;
        let ctx = make_worker_context_custom(ffqn, vec![json!("World"), json!(3)]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(
            extract_string(&ok_val.value),
            "Hello, World! Hello, World! Hello, World!"
        );
    }

    #[tokio::test]
    async fn custom_params_no_params() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "no-args");
        let js_source = r#"
            export default function no_args() {
                return "no args works";
            }
        "#;

        let worker = new_js_activity_worker_custom_params(js_source, ffqn.clone(), vec![]).await;
        let ctx = make_worker_context_custom(ffqn, vec![]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "no args works");
    }

    #[tokio::test]
    async fn custom_params_list_and_bool() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "format-list");
        let js_source = r#"
            export default function format_list(items, uppercase) {
                let result = items.join(", ");
                if (uppercase) {
                    result = result.toUpperCase();
                }
                return result;
            }
        "#;

        let user_params = vec![
            ParameterType {
                type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
                name: StrVariant::Static("items"),
                wit_type: StrVariant::Static("list<string>"),
            },
            ParameterType {
                type_wrapper: TypeWrapper::Bool,
                name: StrVariant::Static("uppercase"),
                wit_type: StrVariant::Static("bool"),
            },
        ];

        let worker =
            new_js_activity_worker_custom_params(js_source, ffqn.clone(), user_params).await;
        let ctx = make_worker_context_custom(
            ffqn,
            vec![json!(["apple", "banana", "cherry"]), json!(true)],
        );

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "APPLE, BANANA, CHERRY");
    }

    #[tokio::test]
    async fn set_timeout_basic() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "delayed");
        let js_source = r#"
            export default async function delayed() {
                return new Promise((resolve) => {
                    setTimeout(() => {
                        resolve("delayed result");
                    }, 50);
                });
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let start = std::time::Instant::now();
        let result = worker.run(ctx).await.expect("worker should succeed");
        let elapsed = start.elapsed();

        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "delayed result");

        // Verify that we actually waited for the timeout (at least 50ms)
        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "expected at least 50ms delay, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn set_timeout_multiple() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "multi-timeout");
        let js_source = r#"
            export default async function multi_timeout() {
                let results = [];

                await new Promise((resolve) => {
                    setTimeout(() => {
                        results.push("first");
                        resolve();
                    }, 30);
                });

                await new Promise((resolve) => {
                    setTimeout(() => {
                        results.push("second");
                        resolve();
                    }, 30);
                });

                return results.join(", ");
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, &[]);

        let start = std::time::Instant::now();
        let result = worker.run(ctx).await.expect("worker should succeed");
        let elapsed = start.elapsed();

        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "first, second");

        // Both timeouts should have been honored (at least 60ms total)
        assert!(
            elapsed >= std::time::Duration::from_millis(60),
            "expected at least 60ms delay, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn set_timeout_orphaned_abandoned() {
        use crate::component_logger::LogStrageConfig;
        use concepts::storage::LogLevel;

        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "orphan-timer");
        // This function logs immediately, then starts an orphaned timer that would log.
        // The orphaned timer's log should never appear.
        let js_source = r#"
            export default async function orphan_timer() {
                console.log("immediate log");

                // Start a timer but don't await it - this should be abandoned
                setTimeout(() => {
                    console.log("orphaned timer log");
                }, 100);

                // Return immediately
                return "done";
            }
        "#;

        // Set up log capture
        let (log_sender, mut log_receiver) = mpsc::channel(10);
        let logs_storage_config = LogStrageConfig {
            min_level: LogLevel::Trace,
            log_sender,
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_logs(logs_storage_config)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");

        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "done");

        // Collect all log messages
        log_receiver.close();
        let mut messages = Vec::new();
        while let Some(log_row) = log_receiver.recv().await {
            if let concepts::storage::LogEntry::Log { message, .. } = log_row.log_entry {
                messages.push(message);
            }
        }

        // The immediate log should be present
        assert!(
            messages.iter().any(|m| m.contains("immediate log")),
            "expected 'immediate log' message, got: {messages:?}"
        );

        // The orphaned timer log should NOT be present
        assert!(
            !messages.iter().any(|m| m.contains("orphaned timer log")),
            "orphaned timer should have been abandoned, but found its log message: {messages:?}"
        );
    }

    /// Creates a JS activity worker with allowed host configuration for HTTP tests.
    /// The JS function should have signature: `function(url: string) -> result<string, string>`
    async fn new_js_activity_worker_for_retry_test(
        js_source: &str,
        user_ffqn: FunctionFqn,
        listener: &std::net::TcpListener,
    ) -> Arc<dyn Worker> {
        let server_address = listener
            .local_addr()
            .expect("Failed to get server address.");
        let allowed_host = format!("http://127.0.0.1:{port}", port = server_address.port());

        let user_params = vec![ParameterType {
            type_wrapper: TypeWrapper::String,
            name: StrVariant::Static("url"),
            wit_type: StrVariant::Static("string"),
        }];

        JsWorkerBuilder::new(js_source, user_ffqn)
            .with_params(user_params)
            .with_allowed_host(&allowed_host)
            .build()
            .await
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn fetch_get_retry_on_fallible_err(
        #[values(TestRetryBehavior::SucceedOnRetry,TestRetryBehavior::Fail { expected_retry_err: "wrong status code: 404" })]
        test_retry_behavior: TestRetryBehavior,
        #[values(
            executor::executor::LockingStrategy::ByFfqns,
            executor::executor::LockingStrategy::ByComponentDigest
        )]
        locking_strategy: executor::executor::LockingStrategy,
    ) {
        use crate::activity::activity_worker::tests::run_http_get_retry_test;

        test_utils::set_up();

        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fetch-get");
        // JS that throws on non-2xx status codes (similar to the Rust http_get activity)
        let js_source = r#"
            export default async function fetch_get(url) {
                const resp = await fetch(url);
                if (resp.status >= 400) {
                    throw "wrong status code: " + resp.status;
                }
                const text = await resp.text();
                return text;
            }
        "#;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let worker =
            new_js_activity_worker_for_retry_test(js_source, ffqn.clone(), &listener).await;

        run_http_get_retry_test(
            listener,
            worker,
            ffqn,
            |uri| Params::from_json_values_test(vec![json!(format!("{uri}/"))]),
            locking_strategy,
            "wrong status code: 500",
            test_retry_behavior,
        )
        .await;
    }

    #[tokio::test]
    async fn typed_return_u32() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "get-count");
        let js_source = r"
            export default function get_count() {
                return 42;
            }
        ";

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::U32)),
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("result<u32, string>"),
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_return_type(return_type)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let wvt = assert_matches!(retval, SupportedFunctionReturnValue::Ok(Some(wvt)) => wvt);
        assert_eq!(wvt.r#type, TypeWrapper::U32);
        assert_eq!(wvt.value, WastVal::U32(42));
    }

    #[tokio::test]
    async fn typed_return_list_string() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "get-tags");
        let js_source = r#"
            export default function get_tags() {
                return ["alpha", "beta", "gamma"];
            }
        "#;

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::List(Box::new(TypeWrapper::String)))),
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("result<list<string>, string>"),
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_return_type(return_type)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let wvt = assert_matches!(retval, SupportedFunctionReturnValue::Ok(Some(wvt)) => wvt);
        assert_eq!(wvt.r#type, TypeWrapper::List(Box::new(TypeWrapper::String)));
        assert_eq!(
            wvt.value,
            WastVal::List(vec![
                WastVal::String("alpha".to_string()),
                WastVal::String("beta".to_string()),
                WastVal::String("gamma".to_string()),
            ])
        );
    }

    #[tokio::test]
    async fn typed_return_throw_with_typed_return() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "might-fail");
        let js_source = r#"
            export default function might_fail() {
                throw "computation failed";
            }
        "#;

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::U32)),
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("result<u32, string>"),
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_return_type(return_type)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let err_wvt = assert_matches!(retval, SupportedFunctionReturnValue::Err(Some(wvt)) => wvt);
        assert_eq!(err_wvt.r#type, TypeWrapper::String);
        assert_eq!(
            err_wvt.value,
            WastVal::String("computation failed".to_string())
        );
    }

    #[tokio::test]
    async fn retval_ok_none_should_accept_null() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "ok-none");
        let js_source = r"
            export default function ok_none() {
                return null;
            }
        ";

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: None,
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("result<_, string>"),
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_return_type(return_type)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        assert_matches!(retval, SupportedFunctionReturnValue::Ok(None));
    }

    #[tokio::test]
    async fn typed_throw_variant_err() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "might-fail");
        let js_source = r#"
            export default function might_fail() {
                throw "not_found";
            }
        "#;

        use val_json::type_wrapper::TypeKey;
        use val_json::type_wrapper::indexmap::IndexMap;
        let cases: IndexMap<TypeKey, Option<TypeWrapper>> = [
            (TypeKey::new_kebab("execution-failed"), None),
            (TypeKey::new_kebab("not-found"), None),
        ]
        .into_iter()
        .collect();
        let err_variant = TypeWrapper::Variant(cases.clone());

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::U32)),
                err: Some(Box::new(err_variant.clone())),
            },
            wit_type: StrVariant::Static("result<u32, variant { execution-failed, not-found }>"),
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_return_type(return_type)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let err_wvt = assert_matches!(retval, SupportedFunctionReturnValue::Err(Some(wvt)) => wvt);
        assert_eq!(err_wvt.r#type, err_variant);
        assert_eq!(
            err_wvt.value,
            WastVal::Variant(val_json::wast_val::ValKey::new_snake("not_found"), None)
        );
    }

    #[tokio::test]
    async fn typed_throw_err_none_is_fatal() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "no-err");
        let js_source = r#"
            export default function no_err() {
                throw "oops";
            }
        "#;

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::U32)),
                err: None,
            },
            wit_type: StrVariant::Static("result<u32>"),
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_return_type(return_type)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let err = worker.run(ctx).await.unwrap_err();
        let reason = assert_matches!(
            err,
            WorkerError::FatalError(
                FatalError::ResultParsingError(ResultParsingError::ResultParsingErrorFromVal(
                    ResultParsingErrorFromVal::TypeCheckError(reason),
                )),
                _version,
            ) => reason
        );
        assert_eq!(
            reason,
            "thrown value type check failed: return type is `result<u32>` (no error type), expected `throw null`, got `\"oops\"`"
        );
    }

    #[tokio::test]
    async fn typed_throw_null_void_err() {
        // `throw null` with `result<string>` (err=None) → Err(None)
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "null-throw");
        let js_source = r"
            export default function null_throw() {
                throw null;
            }
        ";

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::String)),
                err: None,
            },
            wit_type: StrVariant::Static("result<string>"),
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_return_type(return_type)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        assert_matches!(retval, SupportedFunctionReturnValue::Err(None));
    }

    #[tokio::test]
    async fn typed_return_enum() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "get-status");
        let js_source = r#"
            export default function get_status() {
                return "running";
            }
        "#;

        use val_json::type_wrapper::TypeKey;
        use val_json::type_wrapper::indexmap::IndexSet;
        let cases: IndexSet<TypeKey> = ["pending", "running", "done"]
            .iter()
            .map(|s| TypeKey::new_kebab(*s))
            .collect();

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::Enum(cases.clone()))),
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("result<enum { pending, running, done }, string>"),
        };

        let worker = JsWorkerBuilder::new(js_source, ffqn.clone())
            .with_return_type(return_type)
            .build()
            .await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished { retval, .. } => retval);
        let wvt = assert_matches!(retval, SupportedFunctionReturnValue::Ok(Some(wvt)) => wvt);
        assert_eq!(wvt.r#type, TypeWrapper::Enum(cases));
        assert_eq!(
            wvt.value,
            WastVal::Enum(val_json::wast_val::ValKey::new_snake("running"))
        );
    }
}
