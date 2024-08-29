use concepts::StrVariant;
use std::{error::Error, fmt::Debug, path::PathBuf};
use utils::wasm_tools::{self};

pub mod activity_worker;
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
}

#[cfg(test)]
pub(crate) mod tests {
    use async_trait::async_trait;
    use concepts::{
        ComponentConfigHash, FunctionFqn, FunctionMetadata, FunctionRegistry, ParameterTypes,
    };
    use std::sync::Arc;

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
                    FunctionMetadata {
                        ffqn: ffqn.clone(),
                        parameter_types: ParameterTypes::default(),
                        return_type: None,
                    },
                    component_id.clone(),
                ),
            );
        }
        Arc::new(TestingFnRegistry(map))
    }
}
