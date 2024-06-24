use concepts::StrVariant;
use std::{error::Error, fmt::Debug, path::PathBuf, sync::Arc};
use utils::wasm_tools::{self};
use wasmtime::Engine;

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

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct EngineConfigError(Box<dyn Error + Send + Sync>);

// Copied from wasmtime's cli-flags package.
#[derive(PartialEq, Clone, Default)]
pub struct OptimizeOptions {
    /// Optimization level of generated code (0-2, s; default: 2)
    pub opt_level: Option<wasmtime::OptLevel>,

    /// Byte size of the guard region after dynamic memories are allocated
    pub dynamic_memory_guard_size: Option<u64>,

    /// Force using a "static" style for all wasm memories
    pub static_memory_forced: Option<bool>,

    /// Maximum size in bytes of wasm memory before it becomes dynamically
    /// relocatable instead of up-front-reserved.
    pub static_memory_maximum_size: Option<u64>,

    /// Byte size of the guard region after static memories are allocated
    pub static_memory_guard_size: Option<u64>,

    /// Bytes to reserve at the end of linear memory for growth for dynamic
    /// memories.
    pub dynamic_memory_reserved_for_growth: Option<u64>,

    /// Indicates whether an unmapped region of memory is placed before all
    /// linear memories.
    pub guard_before_linear_memory: Option<bool>,

    /// Whether to initialize tables lazily, so that instantiation is
    /// fast but indirect calls are a little slower. If no, tables are
    /// initialized eagerly from any active element segments that apply to
    /// them during instantiation. (default: yes)
    pub table_lazy_init: Option<bool>,

    /// Enable the pooling allocator, in place of the on-demand allocator.
    pub pooling_allocator: Option<bool>,

    /// The number of decommits to do per batch. A batch size of 1
    /// effectively disables decommit batching. (default: 1)
    pub pooling_decommit_batch_size: Option<u32>,

    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in linear memories.
    pub pooling_memory_keep_resident: Option<usize>,

    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in tables.
    pub pooling_table_keep_resident: Option<usize>,

    /// Enable memory protection keys for the pooling allocator; this can
    /// optimize the size of memory slots.
    pub memory_protection_keys: Option<bool>,

    /// Configure attempting to initialize linear memory via a
    /// copy-on-write mapping (default: yes)
    pub memory_init_cow: Option<bool>,

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

    /// Whether to enable call-indirect caching.
    pub cache_call_indirects: Option<bool>,

    /// The maximum call-indirect cache slot count.
    ///
    /// One slot is allocated per indirect callsite; if the module
    /// has more indirect callsites than this limit, then the
    /// first callsites in linear order in the code section, up to
    /// the limit, will receive a cache slot.
    pub max_call_indirect_cache_slots: Option<usize>,
}

impl OptimizeOptions {
    fn from_env() -> Self {
        Self::default() // FIXME
    }
}

impl EngineConfig {
    pub fn new() -> Result<Self, EngineConfigError> {
        Ok(Self {
            allocation_strategy: Self::use_pooling_allocator_by_default()?,
        })
    }

    // Adapted from https://github.com/bytecodealliance/wasmtime/pull/8610
    fn use_pooling_allocator_by_default(
    ) -> Result<wasmtime::InstanceAllocationStrategy, EngineConfigError> {
        let opts = OptimizeOptions::from_env();
        const BITS_TO_TEST: u32 = 42;
        let mut config = wasmtime::Config::new();
        config.wasm_memory64(true);
        config.static_memory_maximum_size(1 << BITS_TO_TEST);
        let engine = Engine::new(&config).map_err(|err| EngineConfigError(err.into()))?;
        let mut store = wasmtime::Store::new(&engine, ());
        // NB: the maximum size is in wasm pages to take out the 16-bits of wasm
        // page size here from the maximum size.
        let ty = wasmtime::MemoryType::new64(0, Some(1 << (BITS_TO_TEST - 16)));
        if wasmtime::Memory::new(&mut store, ty).is_ok() {
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
            // if let Some(limit) = opts.pooling_max_memory_size {
            //     cfg.max_memory_size(limit);
            // }
            if let Some(enable) = opts.memory_protection_keys {
                if enable {
                    cfg.memory_protection_keys(wasmtime::MpkEnabled::Enable);
                }
            }
            Ok(wasmtime::InstanceAllocationStrategy::Pooling(cfg))
        } else {
            Ok(wasmtime::InstanceAllocationStrategy::OnDemand)
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
    #[error("no exported interfaces")]
    NoExportedInterfaces,
    #[error("mixed workflows and activities")]
    MixedWorkflowsAndActivities,
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

#[cfg(test)]
pub(crate) mod tests {
    use crate::component_detector::ComponentDetector;
    use chrono::{DateTime, Utc};
    use concepts::{
        storage::{Component, ComponentWithMetadata, DbConnection},
        ComponentId, ComponentType, FunctionFqn, ParameterTypes,
    };
    use std::path::Path;

    pub(crate) async fn component_add_dummy<DB: DbConnection>(
        db_connection: &DB,
        created_at: DateTime<Utc>,
        ffqn: FunctionFqn,
    ) {
        db_connection
            .component_add(
                created_at,
                ComponentWithMetadata {
                    component: Component {
                        component_id: ComponentId::empty(),
                        component_type: ComponentType::WasmActivity,
                        config: serde_json::Value::String(String::new()),
                        file_name: String::new(),
                    },
                    exports: vec![(ffqn, ParameterTypes::default(), None)],
                    imports: vec![],
                },
                true,
            )
            .await
            .unwrap();
    }

    pub(crate) async fn component_add_real<DB: DbConnection>(
        db_connection: &DB,
        created_at: DateTime<Utc>,
        wasm_path: impl AsRef<Path>,
    ) {
        let wasm_path = wasm_path.as_ref();
        let file_name = wasm_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let component_id = crate::component_detector::file_hash(wasm_path).unwrap();
        let engine = ComponentDetector::get_engine();
        let detected = ComponentDetector::new(wasm_path, &engine).unwrap();
        let config = serde_json::Value::String("fake, not deserialized in tests".to_string());
        let component = ComponentWithMetadata {
            component: Component {
                component_id,
                component_type: detected.component_type,
                config,
                file_name,
            },
            exports: detected.exports,
            imports: detected.imports,
        };
        db_connection
            .component_add(created_at, component, true)
            .await
            .unwrap();
    }
}
