use concepts::{StrVariant};
use std::{error::Error, path::PathBuf, sync::Arc};
use utils::wasm_tools::{self};
use wasmtime::{Engine};

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
