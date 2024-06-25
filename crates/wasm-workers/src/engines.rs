use crate::{activity_worker, workflow_worker};
use std::{error::Error, fmt::Debug, sync::Arc};
use wasmtime::Engine;

#[derive(Clone)]
pub struct EngineConfig {
    pub allocation_strategy: wasmtime::InstanceAllocationStrategy,
}

impl Debug for EngineConfig {
    // Workaround for missing debug in InstanceAllocationStrategy
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.allocation_strategy {
            wasmtime::InstanceAllocationStrategy::OnDemand => f
                .debug_struct("EngineConfig")
                .field("allocation_strategy", &"OnDemand")
                .finish(),
            wasmtime::InstanceAllocationStrategy::Pooling(polling) => f
                .debug_struct("EngineConfig")
                .field("allocation_strategy", &"Polling")
                .field("polling_config", &polling)
                .finish(),
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::OnDemand,
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct EngineConfigError(Box<dyn Error + Send + Sync>);

// Copied from wasmtime/crates/cli-flags
#[derive(PartialEq, Clone, Default)]
pub struct OptimizeOptions {
    /// Byte size of the guard region after dynamic memories are allocated
    // pub dynamic_memory_guard_size: Option<u64>,

    /// Force using a "static" style for all wasm memories
    // pub static_memory_forced: Option<bool>,

    /// Maximum size in bytes of wasm memory before it becomes dynamically
    /// relocatable instead of up-front-reserved.
    // pub static_memory_maximum_size: Option<u64>,

    /// Byte size of the guard region after static memories are allocated
    // pub static_memory_guard_size: Option<u64>,

    /// Bytes to reserve at the end of linear memory for growth for dynamic
    /// memories.
    // pub dynamic_memory_reserved_for_growth: Option<u64>,

    /// Indicates whether an unmapped region of memory is placed before all
    /// linear memories.
    // pub guard_before_linear_memory: Option<bool>,

    /// Whether to initialize tables lazily, so that instantiation is
    /// fast but indirect calls are a little slower. If no, tables are
    /// initialized eagerly from any active element segments that apply to
    /// them during instantiation. (default: yes)
    // pub table_lazy_init: Option<bool>,

    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in linear memories.
    pub pooling_memory_keep_resident: Option<usize>,

    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in tables.
    pub pooling_table_keep_resident: Option<usize>,

    /// Enable memory protection keys for the pooling allocator; this can
    /// optimize the size of memory slots.
    pub memory_protection_keys: Option<bool>,

    /// The maximum number of WebAssembly instances which can be created
    /// with the pooling allocator.
    pub pooling_total_core_instances: Option<u32>,

    /// The maximum number of WebAssembly components which can be created
    /// with the pooling allocator.
    pub pooling_total_component_instances: Option<u32>,

    /// The maximum number of WebAssembly memories which can be created with
    /// the pooling allocator.
    pub pooling_total_memories: Option<u32>,

    /// The maximum number of WebAssembly tables which can be created with
    /// the pooling allocator.
    pub pooling_total_tables: Option<u32>,

    /// The maximum number of WebAssembly stacks which can be created with
    /// the pooling allocator.
    pub pooling_total_stacks: Option<u32>,

    /// The maximum runtime size of each linear memory in the pooling
    /// allocator, in bytes.
    pub pooling_max_memory_size: Option<usize>,
}

impl EngineConfig {
    // Adapted from https://github.com/bytecodealliance/wasmtime/pull/8610
    pub fn detect_allocation_strategy(opts: OptimizeOptions) -> Result<Self, EngineConfigError> {
        const BITS_TO_TEST: u32 = 42;
        let mut config = wasmtime::Config::new();
        config.wasm_memory64(true);
        config.static_memory_maximum_size(1 << BITS_TO_TEST);
        let engine = Engine::new(&config).map_err(|err| EngineConfigError(err.into()))?;
        let mut store = wasmtime::Store::new(&engine, ());
        // NB: the maximum size is in wasm pages to take out the 16-bits of wasm
        // page size here from the maximum size.
        let ty = wasmtime::MemoryType::new64(0, Some(1 << (BITS_TO_TEST - 16)));
        let allocation_strategy = if wasmtime::Memory::new(&mut store, ty).is_ok() {
            let mut cfg = wasmtime::PoolingAllocationConfig::default();
            if let Some(size) = opts.pooling_memory_keep_resident {
                cfg.linear_memory_keep_resident(size);
            }
            if let Some(size) = opts.pooling_table_keep_resident {
                cfg.table_keep_resident(size);
            }
            if let Some(limit) = opts.pooling_total_core_instances {
                cfg.total_core_instances(limit);
            }
            if let Some(limit) = opts.pooling_total_component_instances {
                cfg.total_component_instances(limit);
            }
            if let Some(limit) = opts.pooling_total_memories {
                cfg.total_memories(limit);
            }
            if let Some(limit) = opts.pooling_total_tables {
                cfg.total_tables(limit);
            }
            if let Some(limit) = opts.pooling_total_stacks {
                cfg.total_stacks(limit);
            }
            if let Some(limit) = opts.pooling_max_memory_size {
                cfg.max_memory_size(limit);
            }
            if let Some(enable) = opts.memory_protection_keys {
                if enable {
                    cfg.memory_protection_keys(wasmtime::MpkEnabled::Enable);
                }
            }
            wasmtime::InstanceAllocationStrategy::Pooling(cfg)
        } else {
            wasmtime::InstanceAllocationStrategy::OnDemand
        };
        Ok(Self {
            allocation_strategy,
        })
    }
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
