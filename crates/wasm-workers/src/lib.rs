use concepts::{ComponentType, FunctionFqn, FunctionMetadata, StrVariant};
use std::{error::Error, fmt::Debug, path::Path};
use tracing::{debug, error, trace};
use utils::wasm_tools::{self, DecodeError, WasmComponent};

pub mod activity;
mod component_logger;
pub mod engines;
pub mod epoch_ticker;
pub mod preopens_cleaner;
pub mod std_output_stream;
#[cfg(any(test, feature = "test"))]
pub mod testing_fn_registry;
pub mod webhook;
pub mod workflow;

#[derive(thiserror::Error, Debug)]
pub enum WasmFileError {
    #[error("cannot read WASM file: {0}")]
    CannotReadComponent(wasmtime::Error),
    #[error("cannot decode: {0}")]
    DecodeError(#[from] wasm_tools::DecodeError),
    #[error("linking error - {context}, details: {err}")]
    LinkingError {
        context: StrVariant,
        err: Box<dyn Error + Send + Sync>,
    },
}

pub mod envvar {
    #[derive(Clone, derive_more::Debug)]
    pub struct EnvVar {
        pub key: String,
        #[debug(skip)]
        pub val: String,
    }
}

#[derive(derive_more::Debug)]
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
                error!("Cannot parse {wasm_path:?} using wasmtime - {err:?}");
                DecodeError::CannotReadComponent { source: err }
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
        &self,
    ) -> Result<
        hashbrown::HashMap<FunctionFqn, wasmtime::component::ComponentExportIndex>,
        DecodeError,
    > {
        let mut exported_ffqn_to_index = hashbrown::HashMap::new();
        for FunctionMetadata { ffqn, .. } in self.wasm_component.exim.get_exports(false) {
            let Some(ifc_export_index) = self
                .wasmtime_component
                .get_export_index(None, &ffqn.ifc_fqn)
            else {
                error!("Cannot find exported interface `{}`", ffqn.ifc_fqn);
                return Err(DecodeError::CannotReadComponentWithReason {
                    reason: format!("cannot find exported interface {ffqn}"),
                });
            };
            let Some(fn_export_index) = self
                .wasmtime_component
                .get_export_index(Some(&ifc_export_index), &ffqn.function_name)
            else {
                error!("Cannot find exported function {ffqn}");
                return Err(DecodeError::CannotReadComponentWithReason {
                    reason: format!("cannot find exported function {ffqn}"),
                });
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
        #[test]
        fn fibo(wasm_path: &str) {
            compile_activity(wasm_path);
        }

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            ])]
        #[test]
        fn workflow(wasm_path: &str) {
            compile_workflow(wasm_path);
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
