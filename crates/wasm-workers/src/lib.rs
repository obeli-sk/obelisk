use concepts::{FunctionMetadata, StrVariant};
use std::{error::Error, ops::Deref, sync::Arc};
use tracing::{info, instrument, trace};
use utils::wasm_tools::{self, ExIm};
use wasmtime::{component::Component, Engine};

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
    CannotReadComponent(StrVariant, wasmtime::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(StrVariant, wasm_tools::DecodeError),
    #[error("cannot link `{file}` - {reason}, details: {err}")]
    LinkingError {
        file: StrVariant,
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

#[derive(derivative::Derivative)]
#[derivative(Debug)]
struct WasmComponent {
    #[derivative(Debug = "ignore")]
    component: Component,
    exim: ExIm,
}

impl WasmComponent {
    #[instrument(skip_all, fields(%wasm_path))]
    fn new(wasm_path: &StrVariant, engine: &Engine) -> Result<Self, WasmFileError> {
        info!("Reading");
        let component =
            Component::from_file(engine, wasm_path.deref()).map_err(|err: wasmtime::Error| {
                WasmFileError::CannotReadComponent(wasm_path.clone(), err)
            })?;
        let exim = wasm_tools::decode(&component, engine)
            .map_err(|err| WasmFileError::DecodeError(wasm_path.clone(), err))?;
        trace!(?exim, "Exports and imports");
        Ok(Self { component, exim })
    }

    fn exported_functions(&self) -> impl Iterator<Item = FunctionMetadata> + '_ {
        self.exim.exported_functions()
    }

    fn imported_functions(&self) -> impl Iterator<Item = FunctionMetadata> + '_ {
        self.exim.imported_functions()
    }
}
