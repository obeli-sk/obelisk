pub(crate) mod config_holder;
pub(crate) mod toml;

use crate::oci;
use anyhow::Context;
use concepts::ComponentConfigHash;
use concepts::ContentDigest;
use concepts::{ComponentType, FunctionMetadata};
use serde_with::serde_as;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::instrument;
use wasm_workers::workflow_worker::JoinNextBlockingStrategy;

#[derive(Debug, Clone)]
pub struct Component {
    pub config_id: ComponentConfigHash, // Unique identifier computed from `config` field.
    pub config_store: ConfigStore,
    pub exports: Vec<FunctionMetadata>,
    pub imports: Vec<FunctionMetadata>,
}

#[derive(Debug, Clone, Hash, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Hash)]
pub(crate) struct ConfigStoreCommon {
    pub(crate) name: String,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: ContentDigest,
    pub(crate) exec: ExecConfig,
}

#[derive(Debug, Clone, Hash)]
pub(crate) enum ConfigStore {
    WasmActivityV1 {
        common: ConfigStoreCommon,
        default_max_retries: u32,
        default_retry_exp_backoff: Duration,
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

    pub(crate) fn default_max_retries(&self) -> u32 {
        if let Self::WasmActivityV1 {
            default_max_retries,
            ..
        } = self
        {
            *default_max_retries
        } else {
            0
        }
    }

    pub(crate) fn default_retry_exp_backoff(&self) -> Duration {
        if let Self::WasmActivityV1 {
            default_retry_exp_backoff,
            ..
        } = self
        {
            *default_retry_exp_backoff
        } else {
            Duration::ZERO
        }
    }

    pub(crate) fn as_hash(&self) -> ComponentConfigHash {
        let hash = {
            use std::hash::Hash as _;
            use std::hash::Hasher as _;
            let mut hasher = std::hash::DefaultHasher::new();
            self.hash(&mut hasher);
            format!("{:x}", hasher.finish())
        };
        ComponentConfigHash {
            component_type: self.as_component_type(),
            hash,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Hash, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentLocation {
    Path(PathBuf),
    Oci(#[serde_as(as = "serde_with::DisplayFromStr")] oci_distribution::Reference),
}

impl ComponentLocation {
    #[instrument(skip_all)]
    pub(crate) async fn obtain_wasm(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
        match self {
            ComponentLocation::Path(wasm_path) => {
                let wasm_path = wasm_path
                    .canonicalize()
                    .with_context(|| format!("cannot canonicalize file `{wasm_path:?}`"))?;
                // Future optimization: If the sha256 is in the config file and wasm is already in the cache dir, do not recalculate it.
                let content_digest = Self::calculate_sha256_file(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
                Ok((content_digest, wasm_path))
            }
            ComponentLocation::Oci(image) => {
                oci::pull_to_cache_dir(image, wasm_cache_dir, metadata_dir)
                    .await
                    .context("try cleaning the cache directory with `--clean-cache`")
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn calculate_sha256_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<ContentDigest, std::io::Error> {
        use sha2::Digest;
        use tokio::io::AsyncReadExt;
        let mut file = tokio::fs::File::open(path).await?;
        let mut hasher = sha2::Sha256::default();
        let mut buf = [0; 4096];
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(ContentDigest::new(
            concepts::HashType::Sha256,
            format!("{:x}", hasher.finalize()),
        ))
    }
}
