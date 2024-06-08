use concepts::StrVariant;
use std::{error::Error, path::PathBuf, sync::Arc};
use utils::wasm_tools::{self};
use wasmtime::Engine;

pub mod activity_worker;
pub mod component_detector;
pub mod epoch_ticker;
mod event_history;
mod workflow_ctx;
pub mod workflow_worker;

#[derive(Clone)]
pub struct EngineConfig {
    pub allocation_strategy: wasmtime::InstanceAllocationStrategy,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::pooling(),
        }
    }
}

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

pub struct Engines {
    pub activity_engine: Arc<Engine>,
    pub workflow_engine: Arc<Engine>,
}

impl Engines {
    #[must_use]
    pub fn new(engine_config: EngineConfig) -> Self {
        Engines {
            activity_engine: activity_worker::get_activity_engine(engine_config.clone()),
            workflow_engine: workflow_worker::get_workflow_engine(engine_config),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::component_detector::ComponentDetector;
    use chrono::{DateTime, Utc};
    use concepts::{
        storage::{Component, ComponentWithMetadata, DbConnection},
        ComponentId, ComponentType, FunctionFqn, ParameterTypes,
    };
    use std::path::Path;

    pub(crate) async fn component_add_dummy<DB: DbConnection>(
        db_connection: &DB,
        created_at: DateTime<Utc>,
        ffqn: FunctionFqn,
    ) {
        db_connection
            .component_add(
                created_at,
                ComponentWithMetadata {
                    component: Component {
                        component_id: ComponentId::new(
                            concepts::HashType::Sha256,
                            ulid::Ulid::new().to_string(),
                        ),
                        component_type: ComponentType::WasmActivity,
                        config: serde_json::Value::String(String::new()),
                        file_name: String::new(),
                    },
                    exports: vec![(ffqn, ParameterTypes::default(), None)],
                    imports: vec![],
                },
                true,
            )
            .await
            .unwrap()
            .unwrap();
    }

    pub(crate) async fn component_add_real<DB: DbConnection>(
        db_connection: &DB,
        created_at: DateTime<Utc>,
        wasm_path: impl AsRef<Path>,
    ) {
        let wasm_path = wasm_path.as_ref();
        let file_name = wasm_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let component_id = crate::component_detector::hash(wasm_path).unwrap();
        let engine = ComponentDetector::get_engine();
        let detected = ComponentDetector::new(wasm_path, &engine).unwrap();
        let config = serde_json::Value::String("fake, not deserialized in tests".to_string());
        let component = ComponentWithMetadata {
            component: Component {
                component_id,
                component_type: detected.component_type,
                config,
                file_name,
            },
            exports: detected.exports,
            imports: detected.imports,
        };
        db_connection
            .component_add(created_at, component, true)
            .await
            .unwrap()
            .unwrap();
    }
}
