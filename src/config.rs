pub(crate) mod config_holder;
pub(crate) mod toml;

use crate::oci;
use anyhow::Context;
use concepts::ComponentConfigHash;
use concepts::ContentDigest;
use concepts::{ComponentType, FunctionMetadata};
use serde_with::serde_as;
use sha2::Digest as _;
use sha2::Sha256;
use std::path::{Path, PathBuf};
use std::time::Duration;
use wasm_workers::workflow_worker::JoinNextBlockingStrategy;

#[derive(Debug, Clone)]
pub struct Component {
    pub config_id: ComponentConfigHash, // Unique identifier computed from `config` field.
    pub config_store: ConfigStore,
    pub exports: Vec<FunctionMetadata>,
    pub imports: Vec<FunctionMetadata>,
}

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ConfigStoreCommon {
    pub(crate) name: String,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: ContentDigest,
    pub(crate) exec: ExecConfig,
    pub(crate) default_max_retries: u32,
    pub(crate) default_retry_exp_backoff: Duration,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
        non_blocking_event_batching: u32,
    },
}

impl ConfigStore {
    fn as_component_type(&self) -> ComponentType {
        match self {
            Self::WasmActivityV1 { .. } => ComponentType::WasmActivity,
            Self::WasmWorkflowV1 { .. } => ComponentType::WasmWorkflow,
        }
    }

    pub(crate) fn common(&self) -> &ConfigStoreCommon {
        match self {
            Self::WasmActivityV1 { common, .. } | Self::WasmWorkflowV1 { common, .. } => common,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.common().name
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

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentLocation {
    Path(PathBuf),
    Oci(#[serde_as(as = "serde_with::DisplayFromStr")] oci_distribution::Reference),
}

impl ComponentLocation {
    pub(crate) async fn obtain_wasm(
        &self,
        wasm_cache_dir: impl AsRef<Path>,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
        match self {
            ComponentLocation::Path(wasm_path) => {
                let wasm_path = wasm_path
                    .canonicalize()
                    .with_context(|| format!("cannot canonicalize file `{wasm_path:?}`"))?;
                let content_digest = wasm_workers::component_detector::file_hash(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
                Ok((content_digest, wasm_path))
            }
            ComponentLocation::Oci(image) => oci::pull_to_cache_dir(image, wasm_cache_dir).await,
        }
    }
}
