use super::ComponentLocation;
use concepts::ComponentType;
use concepts::{ComponentConfigHash, ContentDigest};
use sha2::Digest as _;
use sha2::Sha256;
use std::time::Duration;
use wasm_workers::workflow_worker::JoinNextBlockingStrategy;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ExecConfig {
    pub(crate) batch_size: u32,
    pub(crate) lock_expiry: Duration,
    pub(crate) tick_sleep: Duration,
}
impl ExecConfig {
    pub(crate) fn into_exec_exec_config(
        self,
        config_id: ComponentConfigHash,
    ) -> executor::executor::ExecConfig {
        executor::executor::ExecConfig {
            lock_expiry: self.lock_expiry,
            tick_sleep: self.tick_sleep,
            batch_size: self.batch_size,
            config_id,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ConfigStoreCommon {
    pub(crate) name: String,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: ContentDigest,
    pub(crate) exec: ExecConfig,
    pub(crate) default_max_retries: u32,
    pub(crate) default_retry_exp_backoff: Duration,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum ConfigStore {
    WasmActivityV1 {
        common: ConfigStoreCommon,
        recycle_instances: bool,
    },
    WasmWorkflowV1 {
        common: ConfigStoreCommon,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        child_retry_exp_backoff: Duration,
        child_max_retries: u32,
        non_blocking_event_batching: bool,
    },
}

impl ConfigStore {
    fn as_component_type(&self) -> ComponentType {
        match self {
            Self::WasmActivityV1 { .. } => ComponentType::WasmActivity,
            Self::WasmWorkflowV1 { .. } => ComponentType::WasmWorkflow,
        }
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            Self::WasmActivityV1 { common, .. } | Self::WasmWorkflowV1 { common, .. } => {
                &common.name
            }
        }
    }

    pub(crate) fn as_hash(&self) -> ComponentConfigHash {
        let json = serde_json::to_string(&self).unwrap();
        let hash_base16 = format!("{:x}", Sha256::digest(json.as_bytes()));
        ComponentConfigHash {
            component_type: self.as_component_type(),
            config_hash: ContentDigest::new(concepts::HashType::Sha256, hash_base16),
        }
    }
}
