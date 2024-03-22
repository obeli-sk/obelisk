use std::{borrow::Cow, error::Error};

use utils::wasm_tools;

pub mod activity_worker;
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
    CannotOpen(Cow<'static, str>, std::io::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(Cow<'static, str>, wasm_tools::DecodeError),
    #[error("cannot decode metadata `{0}` - {1}")]
    FunctionMetadataError(Cow<'static, str>, wasm_tools::FunctionMetadataError),
    #[error("cannot link `{file}` - {reason}, details: {err}")]
    LinkingError {
        file: Cow<'static, str>,
        reason: Cow<'static, str>,
        err: Box<dyn Error>,
    },
    #[error("cannot compile `{0}` - {1}")]
    CompilationError(Cow<'static, str>, Box<dyn Error>),
}
