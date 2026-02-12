use concepts::{ComponentType, FunctionFqn, FunctionMetadata, StrVariant};
use std::{error::Error, fmt::Debug, path::Path};
use tracing::{debug, trace};
use tracing_error::SpanTrace;
use utils::wasm_tools::{self, DecodeError, ExIm, WasmComponent};

pub mod activity;
pub mod component_logger;
pub mod engines;
pub mod epoch_ticker;
pub mod http_request_policy;
pub mod log_db_forwarder;
pub mod preopens_cleaner;
pub mod registry;
pub mod std_output_stream;
#[cfg(any(test, feature = "test"))]
pub mod testing_fn_registry;
pub mod webhook;
pub mod workflow;

#[derive(thiserror::Error, Debug)]
pub enum WasmFileError {
    #[error("cannot decode: {0}")]
    DecodeError(
        #[from]
        #[source]
        wasm_tools::DecodeError,
    ),
    #[error("linking error - {reason}, details: {err}")]
    LinkingError {
        reason: StrVariant,
        #[source]
        err: Box<dyn Error + Send + Sync>,
        context: SpanTrace,
    },
}
impl WasmFileError {
    pub fn linking_error(
        reason: impl Into<StrVariant>,
        error: impl Into<Box<dyn Error + Send + Sync>>,
    ) -> WasmFileError {
        WasmFileError::LinkingError {
            reason: reason.into(),
            err: error.into(),
            context: SpanTrace::capture(),
        }
    }
}

pub mod envvar {
    #[derive(Clone, derive_more::Debug)]
    pub struct EnvVar {
        pub key: String,
        #[debug(skip)]
        pub val: String,
    }
}

#[derive(derive_more::Debug, Clone)]
pub struct RunnableComponent {
    #[debug(skip)]
    pub wasmtime_component: wasmtime::component::Component,
    pub wasm_component: WasmComponent,
}
impl RunnableComponent {
    pub fn new<P: AsRef<Path>>(
        wasm_path: P,
        engine: &wasmtime::Engine,
        component_type: ComponentType,
    ) -> Result<Self, DecodeError> {
        let wasm_path = wasm_path.as_ref();
        let wasm_component = WasmComponent::new(wasm_path, component_type)?;
        trace!("Decoding using wasmtime");
        let wasmtime_component = {
            let stopwatch = std::time::Instant::now();
            let wasmtime_component = wasmtime::component::Component::from_file(engine, wasm_path)
                .map_err(|err| {
                DecodeError::new_with_source(
                    format!("cannot parse {wasm_path:?} using wasmtime"),
                    err,
                )
            })?;
            debug!("Parsed with wasmtime in {:?}", stopwatch.elapsed());
            wasmtime_component
        };
        Ok(Self {
            wasmtime_component,
            wasm_component,
        })
    }

    pub fn index_exported_functions(
        wasmtime_component: &wasmtime::component::Component,
        exim: &ExIm,
    ) -> Result<
        hashbrown::HashMap<FunctionFqn, wasmtime::component::ComponentExportIndex>,
        DecodeError,
    > {
        let mut exported_ffqn_to_index = hashbrown::HashMap::new();
        for FunctionMetadata { ffqn, .. } in exim.get_exports(false) {
            let Some(ifc_export_index) = wasmtime_component.get_export_index(None, &ffqn.ifc_fqn)
            else {
                return Err(DecodeError::new_without_source(format!(
                    "cannot find exported interface {ffqn}"
                )));
            };
            let Some(fn_export_index) =
                wasmtime_component.get_export_index(Some(&ifc_export_index), &ffqn.function_name)
            else {
                return Err(DecodeError::new_without_source(format!(
                    "cannot find exported function {ffqn}"
                )));
            };
            exported_ffqn_to_index.insert(ffqn.clone(), fn_export_index);
        }
        Ok(exported_ffqn_to_index)
    }
}

#[cfg(test)]
pub(crate) mod tests {

    mod populate_codegen_cache {
        use crate::{
            activity::activity_worker::tests::compile_activity,
            workflow::workflow_worker::tests::compile_workflow,
        };

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
            test_programs_dir_activity_builder::TEST_PROGRAMS_DIR_ACTIVITY,
            test_programs_process_activity_builder::TEST_PROGRAMS_PROCESS_ACTIVITY,
            ])]
        #[tokio::test]
        async fn activity(wasm_path: &str) {
            compile_activity(wasm_path).await;
        }

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            ])]
        #[tokio::test]
        async fn workflow(wasm_path: &str) {
            compile_workflow(wasm_path).await;
        }

        #[cfg(feature = "boa-unstable")]
        #[rstest::rstest(wasm_path => [
            test_programs_adhoc_js_workflow_builder::TEST_PROGRAMS_ADHOC_JS_WORKFLOW
            ])]
        #[tokio::test]
        async fn workflow_adhoc(wasm_path: &str) {
            compile_workflow(wasm_path).await;
        }

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_webhook_builder::TEST_PROGRAMS_FIBO_WEBHOOK
            ])]
        #[test]
        fn webhook(wasm_path: &str) {
            crate::webhook::webhook_trigger::tests::nosim::compile_webhook(wasm_path);
        }
    }
}
