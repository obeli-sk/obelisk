use concepts::StrVariant;
use std::{error::Error, fmt::Debug, path::PathBuf};
use utils::wasm_tools::{self};

pub mod activity_worker;
pub mod component_detector;
pub mod engines;
pub mod epoch_ticker;
mod event_history;
mod workflow_ctx;
pub mod workflow_worker;

#[derive(thiserror::Error, Debug)]
pub enum WasmFileError {
    #[error("cannot read wasm component from `{0}` - {1}")]
    CannotReadComponent(PathBuf, wasmtime::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(PathBuf, wasm_tools::DecodeError),
    #[error("cannot link `{file}` - {reason}, details: {err}")]
    LinkingError {
        file: PathBuf,
        reason: StrVariant,
        err: Box<dyn Error + Send + Sync>,
    },
    #[error("no exported interfaces")]
    NoExportedInterfaces,
    #[error("mixed workflows and activities")]
    MixedWorkflowsAndActivities,
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::component_detector::ComponentDetector;
    use async_trait::async_trait;
    use concepts::{ComponentId, FunctionFqn, FunctionMetadata, FunctionRegistry, ParameterTypes};
    use std::{path::Path, sync::Arc};

    pub(crate) struct TestingFnRegistry(
        hashbrown::HashMap<FunctionFqn, (FunctionMetadata, ComponentId)>,
    );

    #[async_trait]
    impl FunctionRegistry for TestingFnRegistry {
        async fn get_by_exported_function(
            &self,
            ffqn: &FunctionFqn,
        ) -> Option<(FunctionMetadata, ComponentId)> {
            self.0.get(ffqn).cloned()
        }
    }

    pub(crate) fn fn_registry_dummy(ffqns: &[FunctionFqn]) -> Arc<dyn FunctionRegistry> {
        let component_id = ComponentId::empty();
        let mut map = hashbrown::HashMap::new();
        for ffqn in ffqns {
            map.insert(
                ffqn.clone(),
                (
                    (ffqn.clone(), ParameterTypes::default(), None),
                    component_id.clone(),
                ),
            );
        }
        Arc::new(TestingFnRegistry(map))
    }

    pub(crate) async fn fn_registry_parsing_wasm(
        wasm_paths: &[impl AsRef<Path>],
    ) -> Arc<dyn FunctionRegistry> {
        let mut map = hashbrown::HashMap::new();
        for wasm_path in wasm_paths {
            let wasm_path = wasm_path.as_ref();
            let component_id = crate::component_detector::file_hash(wasm_path)
                .await
                .unwrap();
            let engine = ComponentDetector::get_engine();
            let detected = ComponentDetector::new(wasm_path, &engine).unwrap();
            for (ffqn, params, res) in detected.exports {
                map.insert(ffqn.clone(), ((ffqn, params, res), component_id.clone()));
            }
        }
        Arc::new(TestingFnRegistry(map))
    }
}
