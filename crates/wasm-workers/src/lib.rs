use std::error::Error;

use concepts::StrVariant;
use utils::wasm_tools;

pub mod activity_worker;
pub mod epoch_ticker;
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
    #[error("cannot open `{0}` - {1}")]
    CannotOpen(StrVariant, std::io::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(StrVariant, wasm_tools::DecodeError),
    #[error("cannot decode metadata `{0}` - {1}")]
    FunctionMetadataError(StrVariant, wasm_tools::FunctionMetadataError),
    #[error("cannot link `{file}` - {reason}, details: {err}")]
    LinkingError {
        file: StrVariant,
        reason: StrVariant,
        err: Box<dyn Error>,
    },
    #[error("cannot compile `{0}` - {1}")]
    CompilationError(StrVariant, Box<dyn Error>),
}
