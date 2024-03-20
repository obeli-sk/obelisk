use std::sync::Arc;

use wasmtime::Store;

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

#[derive(Clone, Debug)]
pub enum RecycleInstancesSetting {
    Enable,
    Disable,
}

pub(crate) type MaybeRecycledInstances<C> =
    Option<Arc<std::sync::Mutex<Vec<(wasmtime::component::Instance, Store<C>)>>>>;

impl RecycleInstancesSetting {
    pub(crate) fn instantiate<C>(&self) -> MaybeRecycledInstances<C> {
        match self {
            Self::Enable => Some(Default::default()),
            Self::Disable => None,
        }
    }
}
