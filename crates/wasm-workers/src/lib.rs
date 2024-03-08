use std::sync::Arc;

use tracing_unwrap::ResultExt;
use wasmtime::Engine;

pub mod activity_worker;

pub struct EngineConfig {
    pub allocation_strategy: wasmtime::InstanceAllocationStrategy,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(
                wasmtime::PoolingAllocationConfig::default(),
            ),
        }
    }
}

pub fn activity_engine(config: EngineConfig) -> Arc<Engine> {
    let mut wasmtime_config = wasmtime::Config::new();
    // TODO: limit execution with epoch_interruption
    wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
    wasmtime_config.wasm_component_model(true);
    wasmtime_config.async_support(true);
    wasmtime_config.allocation_strategy(config.allocation_strategy);
    Arc::new(Engine::new(&wasmtime_config).unwrap_or_log())
}