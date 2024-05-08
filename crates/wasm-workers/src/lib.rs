use concepts::StrVariant;
use std::{error::Error, ops::Deref};
use tracing::{info, instrument, trace};
use utils::wasm_tools::{self, ExIm};
use wasmtime::{component::Component, Engine};

pub mod activity_worker;
pub mod auto_worker;
pub mod epoch_ticker;
mod event_history;
mod workflow_ctx;
pub mod workflow_worker;

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
        err: Box<dyn Error>,
    },
}

#[derive(derivative::Derivative)]
#[derivative(Debug)]
struct WasmComponent {
    wasm_path: StrVariant,
    #[derivative(Debug = "ignore")]
    component: Component,
    exim: ExIm,
}

impl WasmComponent {
    #[instrument(skip_all, fields(%wasm_path))]
    fn new(wasm_path: StrVariant, engine: &Engine) -> Result<Self, WasmFileError> {
        info!("Reading");
        let component =
            Component::from_file(engine, wasm_path.deref()).map_err(|err: wasmtime::Error| {
                WasmFileError::CannotReadComponent(wasm_path.clone(), err)
            })?;
        let exim = wasm_tools::decode(&component, engine)
            .map_err(|err| WasmFileError::DecodeError(wasm_path.clone(), err))?;
        trace!(?exim, "Exports and imports");
        Ok(Self {
            wasm_path,
            component,
            exim,
        })
    }
}
