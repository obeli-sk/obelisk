use std::{error::Error, fmt::Debug, path::Path, sync::Arc};
use tracing::{debug, warn};
use wasmtime::Engine;

#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("uncagegorized engine creation error - {0}")]
    Uncategorized(Box<dyn Error + Send + Sync>),
    #[error("error configuring the codegen cache")]
    CodegenCache(Box<dyn Error + Send + Sync>),
}

// Copied from wasmtime/crates/cli-flags
#[derive(PartialEq, Clone, Default, Debug)]
pub struct PoolingOptions {
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

#[derive(Clone)]
pub struct EngineConfig<'a> {
    pub allocation_strategy: wasmtime::InstanceAllocationStrategy,
    pub cache_config_path: Option<&'a Path>,
}

impl Debug for EngineConfig<'_> {
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

impl EngineConfig<'_> {
    #[cfg(test)]
    pub(crate) fn on_demand_no_cache() -> Self {
        Self {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::OnDemand,
            cache_config_path: None,
        }
    }
}

pub struct Engines {
    pub activity_engine: Arc<Engine>,
    pub workflow_engine: Arc<Engine>,
}

impl Engines {
    pub(crate) fn get_activity_engine(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        wasmtime_config.wasm_component_model(true);
        wasmtime_config.async_support(true);
        wasmtime_config.allocation_strategy(config.allocation_strategy);
        wasmtime_config.epoch_interruption(true);
        if let Some(cache_config_path) = config.cache_config_path {
            wasmtime_config
                .cache_config_load(cache_config_path)
                .map_err(|err| EngineError::CodegenCache(err.into()))?;
        }
        Ok(Arc::new(
            Engine::new(&wasmtime_config).map_err(|err| EngineError::Uncategorized(err.into()))?,
        ))
    }

    pub(crate) fn get_workflow_engine(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_backtrace(true);
        wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Disable);
        wasmtime_config.wasm_component_model(true);
        wasmtime_config.async_support(true);
        wasmtime_config.allocation_strategy(config.allocation_strategy);
        wasmtime_config.epoch_interruption(true);
        if let Some(cache_config_path) = config.cache_config_path {
            wasmtime_config
                .cache_config_load(cache_config_path)
                .map_err(|err| EngineError::CodegenCache(err.into()))?;
        }
        Ok(Arc::new(
            Engine::new(&wasmtime_config).map_err(|err| EngineError::Uncategorized(err.into()))?,
        ))
    }

    fn on_demand_with_cache(cache_config_path: Option<&Path>) -> Result<Self, EngineError> {
        let engine_config = EngineConfig {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::OnDemand,
            cache_config_path,
        };
        Ok(Engines {
            activity_engine: Self::get_activity_engine(engine_config.clone())?,
            workflow_engine: Self::get_workflow_engine(engine_config)?,
        })
    }

    fn pooling(
        opts: &PoolingOptions,
        cache_config_path: Option<&Path>,
    ) -> Result<Self, EngineError> {
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
        let allocation_strategy = wasmtime::InstanceAllocationStrategy::Pooling(cfg.clone());
        let engine_config = EngineConfig {
            allocation_strategy,
            cache_config_path,
        };
        Ok(Engines {
            activity_engine: Self::get_activity_engine(engine_config.clone())?,
            workflow_engine: Self::get_workflow_engine(engine_config)?,
        })
    }

    pub fn auto_detect_allocator(
        pooling_opts: &PoolingOptions,
        cache_config_path: Option<&Path>,
    ) -> Result<Self, EngineError> {
        Self::pooling(pooling_opts, cache_config_path).or_else(|err| {
            warn!("Falling back to on-demand allocator - {err}");
            debug!("{err:?}");
            Self::on_demand_with_cache(cache_config_path)
        })
    }
}
