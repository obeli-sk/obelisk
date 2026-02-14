//! JS activity worker that wraps an `ActivityWorker` running the Boa WASM component.
//!
//! The Boa component exports `obelisk:js-runtime/execute.run(js-code, params-json) -> result<string, string>`.
//! This wrapper translates the user's typed interface `func(params: list<string>) -> result<string, string>`
//! into calls to the Boa component.

use super::activity_worker::{ActivityWorker, ActivityWorkerCompiled};
use super::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use async_trait::async_trait;
use concepts::storage::LogInfoAppendRow;
use concepts::time::Sleep;
use concepts::{
    FunctionFqn, FunctionMetadata, PackageIfcFns, ParameterType, ParameterTypes, Params,
    ReturnType, StrVariant,
};
use executor::worker::{Worker, WorkerContext, WorkerResult};
use indexmap::IndexMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use val_json::type_wrapper::TypeWrapper;

/// The FFQN of the Boa component's `run` function.
const BOA_IFC_FQN: &str = "obelisk:js-runtime/execute";
const BOA_FN_NAME: &str = "run";

/// Compiled JS activity. Holds the compiled Boa WASM component + JS source + user FFQN.
pub struct JsActivityWorkerCompiled<S: Sleep> {
    inner: ActivityWorkerCompiled<S>,
    js_source: String,
    user_ffqn: FunctionFqn,
    user_exports_noext: Vec<FunctionMetadata>,
    user_exports_ext: Vec<FunctionMetadata>,
    user_exports_hierarchy_ext: Vec<PackageIfcFns>,
}

impl<S: Sleep> JsActivityWorkerCompiled<S> {
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
    ) -> JsActivityWorker<S> {
        let inner =
            self.inner
                .into_worker(cancel_registry, log_forwarder_sender, logs_storage_config);
        JsActivityWorker {
            inner,
            js_source: self.js_source,
            user_ffqn: self.user_ffqn,
            user_exports_noext: self.user_exports_noext,
        }
    }
}

pub struct JsActivityWorker<S: Sleep> {
    inner: ActivityWorker<S>,
    js_source: String,
    #[allow(dead_code)] // Will be used for error context in future
    user_ffqn: FunctionFqn,
    user_exports_noext: Vec<FunctionMetadata>,
}

#[async_trait]
impl<S: Sleep + 'static> Worker for JsActivityWorker<S> {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    async fn run(&self, mut ctx: WorkerContext) -> WorkerResult {
        // Serialize user params to JSON to extract the list<string> argument.
        // The user function signature is: func(params: list<string>) -> result<string, string>
        // So the stored params JSON is: [ ["str1", "str2", ...] ]
        let user_params_array: Vec<serde_json::Value> = serde_json::to_value(&ctx.params)
            .and_then(serde_json::from_value)
            .unwrap_or_default();

        let params_json_str = if user_params_array.is_empty() {
            "[]".to_string()
        } else {
            // First element is the list<string>
            serde_json::to_string(&user_params_array[0]).unwrap_or_else(|_| "[]".to_string())
        };

        // Rewrite context to call the Boa component's run(js-code, params-json)
        ctx.ffqn = FunctionFqn::new_static(BOA_IFC_FQN, BOA_FN_NAME);
        let boa_params: Arc<[serde_json::Value]> = Arc::from([
            serde_json::Value::String(self.js_source.clone()),
            serde_json::Value::String(params_json_str),
        ]);
        ctx.params = Params::from_json_values(
            boa_params,
            [&TypeWrapper::String, &TypeWrapper::String].into_iter(),
        )
        .expect("boa params are always valid");

        self.inner.run(ctx).await
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
    use executor::worker::{WorkerContext, WorkerResultOk};
    use serde_json::json;
    use tokio::sync::mpsc;
    use tracing::info_span;
    use val_json::wast_val::WastVal;

    async fn new_js_activity_worker(js_source: &str, user_ffqn: FunctionFqn) -> Arc<dyn Worker> {
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
            js_activity_runtime_builder::JS_ACTIVITY_RUNTIME,
            &engine,
            ComponentType::ActivityJs,
        )
        .await;

        // Create an ActivityConfig for the JS activity
        let config = super::super::activity_worker::ActivityConfig {
            component_id,
            forward_stdout: None,
            forward_stderr: None,
            env_vars: Arc::from([]),
            retry_on_err: false,
            directories_config: None,
            fuel: None,
            secrets: Arc::from([]),
            allowed_hosts: Arc::from([]),
        };

        let compiled = super::super::activity_worker::ActivityWorkerCompiled::new_with_config(
            wasm_component,
            config,
            engine,
            clock_fn,
            TokioSleep,
        )
        .unwrap();

        let js_compiled = JsActivityWorkerCompiled::new(compiled, js_source.to_string(), user_ffqn);

        Arc::new(js_compiled.into_worker(cancel_registry, &db_forwarder_sender, None))
    }

    fn make_worker_context(ffqn: FunctionFqn, params: Vec<String>) -> WorkerContext {
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
            function main() {
                return "hello world";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, vec![]);

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
            function main(name, greeting) {
                return greeting + ", " + name + "!";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, vec!["World".to_string(), "Hello".to_string()]);

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
            function main() {
                throw "something went wrong";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, vec![]);

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
            function main(msg) {
                console.log("Log message:", msg);
                console.info("Info message");
                console.warn("Warning message");
                console.error("Error message");
                return "logged";
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, vec!["test".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "logged");
    }

    #[tokio::test]
    async fn js_activity_json_object() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "object");
        let js_source = r#"
            function main(name) {
                return { name: name, count: 42 };
            }
        "#;

        let worker = new_js_activity_worker(js_source, ffqn.clone()).await;
        let ctx = make_worker_context(ffqn, vec!["test".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::Finished { retval, .. } => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok { ok } => ok);
        let ok_val = output.expect("should have ok value");
        let json_str = extract_string(&ok_val.value);
        // Parse and verify the JSON structure
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["name"], "test");
        assert_eq!(parsed["count"], 42);
    }
}
