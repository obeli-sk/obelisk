use std::{error::Error, fmt::Debug, path::Path, rc::Rc, sync::Arc};
use tempfile::NamedTempFile;
use tracing::{debug, instrument, trace, warn};
use wasmtime::{Engine, EngineWeak, WasmBacktraceDetails};

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
pub struct EngineConfig {
    pooling_opts: Option<PoolingOptions>,
    cache_config_file: Option<Rc<NamedTempFile>>,
}

impl EngineConfig {
    fn strategy(&self) -> wasmtime::InstanceAllocationStrategy {
        if let Some(opts) = &self.pooling_opts {
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
        }
    }

    #[cfg(test)]
    pub(crate) async fn on_demand_testing() -> Self {
        use std::path::PathBuf;
        let codegen_cache = PathBuf::from("test-codegen-cache");
        std::fs::create_dir_all(&codegen_cache).unwrap();
        let temp_file = Engines::write_codegen_config(Some(&codegen_cache))
            .await
            .unwrap()
            .unwrap();
        std::fs::create_dir_all(&codegen_cache).unwrap();
        Self {
            pooling_opts: None,
            cache_config_file: Some(temp_file),
        }
    }
    #[cfg(not(madsim))]
    #[cfg(test)]
    pub(crate) fn pooling_nocache_testing(opts: PoolingOptions) -> Self {
        Self {
            pooling_opts: Some(opts),
            cache_config_file: None,
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
        mut wasmtime_config: wasmtime::Config,
        config: EngineConfig,
    ) -> Result<Arc<Engine>, EngineError> {
        wasmtime_config.wasm_component_model(true);
        wasmtime_config.async_support(true);

        wasmtime_config.allocation_strategy(config.strategy());
        if let Some(cache_config) = config.cache_config_file {
            wasmtime_config
                .cache_config_load(cache_config.path())
                .map_err(|err| EngineError::CodegenCache(err.into()))?;
        }
        Engine::new(&wasmtime_config)
            .map(Arc::new)
            .map_err(|err| EngineError::Uncategorized(err.into()))
    }

    pub(crate) fn get_webhook_engine(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_backtrace_details(WasmBacktraceDetails::Enable);
        wasmtime_config.epoch_interruption(true);
        Self::configure_common(wasmtime_config, config)
    }

    pub(crate) fn get_activity_engine(config: EngineConfig) -> Result<Arc<Engine>, EngineError> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_backtrace_details(WasmBacktraceDetails::Enable);
        wasmtime_config.epoch_interruption(true);
        Self::configure_common(wasmtime_config, config)
    }

    pub(crate) fn get_workflow_engine(
        config: EngineConfig,
        backtrace_capture: WasmBacktraceDetails,
    ) -> Result<Arc<Engine>, EngineError> {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_backtrace_details(backtrace_capture);
        wasmtime_config.epoch_interruption(true);
        // Make sure the runtime is deterministic when using `simd` or `relaxed_simd`.
        // https://bytecodealliance.zulipchat.com/#narrow/channel/206238-general/topic/Determinism.20of.20Wasm.20SIMD.20in.20Wasmtime
        wasmtime_config.cranelift_nan_canonicalization(true);
        wasmtime_config.relaxed_simd_deterministic(true);
        Self::configure_common(wasmtime_config, config)
    }

    #[instrument(skip_all)]
    pub fn on_demand(
        cache_config_file: Option<Rc<NamedTempFile>>,
        workflow_capture_backtrace: WasmBacktraceDetails,
    ) -> Result<Self, EngineError> {
        let engine_config = EngineConfig {
            pooling_opts: None,
            cache_config_file,
        };
        Ok(Engines {
            activity_engine: Self::get_activity_engine(engine_config.clone())?,
            webhook_engine: Self::get_webhook_engine(engine_config.clone())?,
            workflow_engine: Self::get_workflow_engine(engine_config, workflow_capture_backtrace)?,
        })
    }

    #[instrument(skip_all)]
    pub fn pooling(
        opts: PoolingOptions,
        cache_config_file: Option<Rc<NamedTempFile>>,
        workflow_capture_backtrace: WasmBacktraceDetails,
    ) -> Result<Self, EngineError> {
        let engine_config = EngineConfig {
            pooling_opts: Some(opts),
            cache_config_file,
        };
        Ok(Engines {
            activity_engine: Self::get_activity_engine(engine_config.clone())?,
            webhook_engine: Self::get_webhook_engine(engine_config.clone())?,
            workflow_engine: Self::get_workflow_engine(engine_config, workflow_capture_backtrace)?,
        })
    }

    pub fn auto_detect_allocator(
        pooling_opts: PoolingOptions,
        cache_config: Option<Rc<NamedTempFile>>,
        workflow_capture_backtrace: WasmBacktraceDetails,
    ) -> Result<Self, EngineError> {
        Self::pooling(
            pooling_opts,
            cache_config.clone(),
            workflow_capture_backtrace,
        )
        .or_else(|err| {
            warn!("Falling back to on-demand allocator - {err}");
            debug!("{err:?}");
            Self::on_demand(cache_config, workflow_capture_backtrace)
        })
    }

    // TODO: Create a wasmtime issue requesting a better cache config API
    #[instrument(skip_all)]
    pub async fn write_codegen_config(
        codegen_cache: Option<&Path>,
    ) -> Result<Option<Rc<NamedTempFile>>, std::io::Error> {
        #[cfg(not(madsim))]
        async fn write(file: &NamedTempFile, content: String) -> Result<(), std::io::Error> {
            tokio::fs::write(file, content).await
        }
        #[cfg(madsim)]
        #[expect(clippy::unused_async)]
        async fn write(mut file: &NamedTempFile, content: String) -> Result<(), std::io::Error> {
            use std::io::Write;
            writeln!(file, "{content}")
        }
        Ok(if let Some(codegen_cache) = codegen_cache {
            let codegen_cache_config_file = tempfile::NamedTempFile::new()?;
            debug!("Setting codegen cache to {codegen_cache:?}");
            let codegen_cache = codegen_cache.canonicalize()?;
            let content = format!(
                r"[cache]
enabled = true
directory = {codegen_cache:?}
"
            );
            write(&codegen_cache_config_file, content).await?;
            trace!(
                "Wrote temporary cache config to {:?}",
                codegen_cache_config_file.path()
            );
            Some(Rc::new(codegen_cache_config_file))
        } else {
            None
        })
    }
}
