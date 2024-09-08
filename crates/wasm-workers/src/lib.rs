use concepts::StrVariant;
use std::{error::Error, fmt::Debug};
use utils::wasm_tools::{self};

mod activity_ctx;
pub mod activity_worker;
pub mod engines;
pub mod epoch_ticker;
mod event_history;
pub mod webhook_trigger;
mod workflow_ctx;
pub mod workflow_worker;

#[derive(thiserror::Error, Debug)]
pub enum WasmFileError {
    #[error("cannot read WASM file: {0}")]
    CannotReadComponent(wasmtime::Error),
    #[error("cannot decode: {0}")]
    DecodeError(wasm_tools::DecodeError),
    #[error("linking error - {context}, details: {err}")]
    LinkingError {
        context: StrVariant,
        err: Box<dyn Error + Send + Sync>,
    },
}

#[cfg(test)]
pub(crate) mod tests {
    use async_trait::async_trait;
    use concepts::{
        ConfigId, ComponentRetryConfig, FunctionFqn, FunctionMetadata, FunctionRegistry,
        ParameterTypes,
    };
    use std::sync::Arc;

    pub(crate) struct TestingFnRegistry(
        hashbrown::HashMap<
            FunctionFqn,
            (FunctionMetadata, ConfigId, ComponentRetryConfig),
        >,
    );

    #[async_trait]
    impl FunctionRegistry for TestingFnRegistry {
        async fn get_by_exported_function(
            &self,
            ffqn: &FunctionFqn,
        ) -> Option<(FunctionMetadata, ConfigId, ComponentRetryConfig)> {
            self.0.get(ffqn).cloned()
        }
    }

    pub(crate) fn fn_registry_dummy(ffqns: &[FunctionFqn]) -> Arc<dyn FunctionRegistry> {
        let component_id = ConfigId::dummy();
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
                    ComponentRetryConfig::default(),
                ),
            );
        }
        Arc::new(TestingFnRegistry(map))
    }
}
