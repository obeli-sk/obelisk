use std::{error::Error, fmt::Debug, path::PathBuf, sync::Arc};
use tracing::{debug, warn};
use wasmtime::{Cache, CacheConfig, Engine, EngineWeak, WasmBacktraceDetails};

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

#[derive(Clone, Debug)]
pub enum PoolingConfig {
    OnDemand,
    Pooling(PoolingOptions),
    PoolingWithFallback(PoolingOptions),
}
impl PoolingConfig {
    fn strategy(&self) -> wasmtime::InstanceAllocationStrategy {
        match self {
            PoolingConfig::OnDemand => wasmtime::InstanceAllocationStrategy::OnDemand,
            PoolingConfig::Pooling(opts) | PoolingConfig::PoolingWithFallback(opts) => {
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
                if let Some(enable) = opts.memory_protection_keys
                    && enable
                {
                    cfg.memory_protection_keys(wasmtime::Enabled::Auto);
                }
                wasmtime::InstanceAllocationStrategy::Pooling(cfg)
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub pooling_config: PoolingConfig,
    pub codegen_cache_dir: Option<PathBuf>,
    pub consume_fuel: bool,
    pub parallel_compilation: bool,
    pub debug: bool,
}

impl EngineConfig {
    #[cfg(test)]
    #[must_use]
    pub fn on_demand_testing() -> Self {
        let workspace_dir = PathBuf::from(
            std::env::var("CARGO_WORKSPACE_DIR").expect("CARGO_WORKSPACE_DIR must be set"),
        );
        let codegen_cache = workspace_dir.join("test-codegen-cache");
        Self {
            pooling_config: PoolingConfig::OnDemand,
            codegen_cache_dir: Some(codegen_cache),
            consume_fuel: false,
            parallel_compilation: true,
            debug: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn pooling_nocache_testing(opts: PoolingOptions) -> Self {
        Self {
            pooling_config: PoolingConfig::Pooling(opts),
            codegen_cache_dir: None,
            consume_fuel: false,
            parallel_compilation: true,
            debug: false,
        }
    }
}

#[derive(Clone)]
pub struct Engines {
    pub activity_engine: Arc<Engine>,
    pub webhook_engine: Arc<Engine>,
    pub workflow_engine: Arc<Engine>,
}

impl Engines {
    #[must_use]
    pub fn weak_refs(&self) -> Vec<EngineWeak> {
        vec![
            self.activity_engine.weak(),
            self.workflow_engine.weak(),
            self.workflow_engine.weak(),
        ]
    }

    fn configure_common(
        mut dst_wasmtime_config: wasmtime::Config,
        config: EngineConfig,
    ) -> Result<Arc<Engine>, EngineError> {
        dst_wasmtime_config.wasm_component_model(true);
        dst_wasmtime_config.async_support(true);

        dst_wasmtime_config.wasm_backtrace_details(WasmBacktraceDetails::Enable);
        dst_wasmtime_config.epoch_interruption(true);
        dst_wasmtime_config.consume_fuel(config.consume_fuel);

        if config.debug {
            dst_wasmtime_config.debug_info(true);
            dst_wasmtime_config.cranelift_opt_level(wasmtime::OptLevel::None);
        }

        dst_wasmtime_config.parallel_compilation(config.parallel_compilation);

        dst_wasmtime_config.allocation_strategy(config.pooling_config.strategy());
        if let Some(codegen_cache_dir) = config.codegen_cache_dir {
            let mut cache_config = CacheConfig::new();
            cache_config.with_directory(codegen_cache_dir);
            let cache =
                Cache::new(cache_config).map_err(|err| EngineError::CodegenCache(err.into()))?;
            dst_wasmtime_config.cache(Some(cache));
        }
        Engine::new(&dst_wasmtime_config)
            .map(Arc::new)
            .map_err(|err| EngineError::Uncategorized(err.into()))
    }

    pub(crate) fn get_webhook_engine(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        Self::configure_common(wasmtime::Config::new(), config)
    }

    #[cfg(test)]
    pub fn get_activity_engine_test(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        Self::get_activity_engine_internal(config)
    }

    fn get_activity_engine_internal(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        Self::configure_common(wasmtime::Config::new(), config)
    }

    #[cfg(test)]
    pub fn get_workflow_engine_test(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        Self::get_workflow_engine_internal(config)
    }
    fn get_workflow_engine_internal(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        let mut wasmtime_config = wasmtime::Config::new();
        // Make sure the runtime is deterministic when using `simd` or `relaxed_simd`.
        // https://bytecodealliance.zulipchat.com/#narrow/channel/206238-general/topic/Determinism.20of.20Wasm.20SIMD.20in.20Wasmtime
        wasmtime_config.cranelift_nan_canonicalization(true);
        wasmtime_config.relaxed_simd_deterministic(true);
        Self::configure_common(wasmtime_config, config)
    }

    pub fn new(engine_config: EngineConfig) -> Result<Self, EngineError> {
        let res: Result<_, EngineError> = (|engine_config: &EngineConfig| {
            Ok(Engines {
                activity_engine: Self::get_activity_engine_internal(engine_config.clone())?,
                webhook_engine: Self::get_webhook_engine(engine_config.clone())?,
                workflow_engine: Self::get_workflow_engine_internal(engine_config.clone())?,
            })
        })(&engine_config);
        let res = if let Err(err) = &res
            && matches!(
                engine_config.pooling_config,
                PoolingConfig::PoolingWithFallback(_)
            ) {
            warn!("Falling back to on-demand allocator - {err}");
            debug!("{err:?}");
            let engine_config = EngineConfig {
                pooling_config: PoolingConfig::OnDemand,
                codegen_cache_dir: engine_config.codegen_cache_dir,
                consume_fuel: engine_config.consume_fuel,
                parallel_compilation: engine_config.parallel_compilation,
                debug: engine_config.debug,
            };
            Self::new(engine_config)
        } else {
            res
        };
        res
    }
}
