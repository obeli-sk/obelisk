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
    use concepts::{
        ComponentConfigHash, ComponentType, ContentDigest, FunctionFqn, FunctionMetadata,
        FunctionRegistry, ParameterTypes,
    };
    use std::{path::Path, sync::Arc};
    use utils::wasm_tools::WasmComponent;

    pub(crate) struct TestingFnRegistry(
        hashbrown::HashMap<FunctionFqn, (FunctionMetadata, ComponentConfigHash)>,
    );

    #[async_trait]
    impl FunctionRegistry for TestingFnRegistry {
        async fn get_by_exported_function(
            &self,
            ffqn: &FunctionFqn,
        ) -> Option<(FunctionMetadata, ComponentConfigHash)> {
            self.0.get(ffqn).cloned()
        }
    }

    pub(crate) fn fn_registry_dummy(ffqns: &[FunctionFqn]) -> Arc<dyn FunctionRegistry> {
        let component_id = ComponentConfigHash::dummy();
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
            let component_type = if wasm_path.to_str().unwrap().ends_with("activity.wasm") {
                ComponentType::WasmActivity
            } else if wasm_path.to_str().unwrap().ends_with("workflow.wasm") {
                ComponentType::WasmWorkflow
            } else {
                panic!("cannot determine type automatically from file {wasm_path:?}")
            };
            let config_id = ComponentConfigHash {
                component_type,
                config_hash: ContentDigest::empty(), // this mismatch is intentional, any hash would do
            };
            for (ffqn, params, res) in
                WasmComponent::new(wasm_path, &ComponentDetector::get_engine())
                    .unwrap()
                    .exported_functions()
            {
                map.insert(ffqn.clone(), ((ffqn, params, res), config_id.clone()));
            }
        }
        Arc::new(TestingFnRegistry(map))
    }
}
