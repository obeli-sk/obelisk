pub(crate) mod config_holder;
pub(crate) mod toml;

use crate::oci;
use anyhow::Context;
use concepts::ConfigId;
use concepts::ContentDigest;
use concepts::{ComponentType, FunctionMetadata};
use serde_with::serde_as;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::instrument;
use wasm_workers::workflow_worker::JoinNextBlockingStrategy;

#[derive(Debug, Clone)]
pub struct Component {
    // Identifier for given configuration.
    // Uniqueness is not guaranteed.
    // The id is not persisted, only appears in logs and traces and gRPC responses.
    pub config_id: ConfigId,
    pub config_store: ConfigStore,
    pub imports: Vec<FunctionMetadata>,
    pub exports: Option<Vec<FunctionMetadata>>, // None for webhooks
}

#[derive(Debug, Clone, Hash)]
pub(crate) struct ConfigStoreCommon {
    pub(crate) name: String,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: ContentDigest,
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
        child_retry_exp_backoff: Option<Duration>,
        child_max_retries: Option<u32>,
        non_blocking_event_batching: u32,
    },
    WebhookV1 {
        common: ConfigStoreCommon,
    },
}

impl ConfigStore {
    fn as_component_type(&self) -> ComponentType {
        match self {
            Self::WasmActivityV1 { .. } => ComponentType::WasmActivity,
            Self::WasmWorkflowV1 { .. } => ComponentType::Workflow,
            Self::WebhookV1 { .. } => ComponentType::Webhook,
        }
    }

    pub(crate) fn common(&self) -> &ConfigStoreCommon {
        match self {
            Self::WasmActivityV1 { common, .. }
            | Self::WasmWorkflowV1 { common, .. }
            | Self::WebhookV1 { common } => common,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.common().name
    }

    pub(crate) fn default_max_retries(&self) -> u32 {
        match self {
            Self::WasmActivityV1 {
                default_max_retries,
                ..
            } => *default_max_retries,
            Self::WasmWorkflowV1 { .. } => 0,
            Self::WebhookV1 { .. } => unreachable!("webhooks have no retries"),
        }
    }

    pub(crate) fn default_retry_exp_backoff(&self) -> Duration {
        match self {
            Self::WasmActivityV1 {
                default_retry_exp_backoff,
                ..
            } => *default_retry_exp_backoff,
            Self::WasmWorkflowV1 { .. } => Duration::ZERO,
            Self::WebhookV1 { .. } => unreachable!("webhooks have no retries"),
        }
    }

    pub(crate) fn config_id(&self) -> ConfigId {
        let hash = {
            use std::hash::Hash as _;
            use std::hash::Hasher as _;
            let mut hasher = std::hash::DefaultHasher::new();
            self.hash(&mut hasher);
            format!("{:x}", hasher.finish())
        };
        ConfigId {
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
