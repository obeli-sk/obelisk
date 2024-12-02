pub(crate) mod config_holder;
pub(crate) mod toml;

use crate::oci;
use anyhow::Context;
use concepts::ComponentRetryConfig;
use concepts::ConfigId;
use concepts::ConfigIdType;
use concepts::ContentDigest;
use concepts::FunctionMetadata;
use concepts::PackageIfcFns;
use concepts::StrVariant;
use serde_with::serde_as;
use std::path::{Path, PathBuf};
use tracing::instrument;
use utils::sha256sum::calculate_sha256_file;

/// Holds information about components, used for gRPC services like `ListComponents`
#[derive(Debug, Clone)]
pub(crate) struct ComponentConfig {
    pub(crate) config_id: ConfigId,
    pub(crate) imports: Vec<FunctionMetadata>,
    pub(crate) importable: Option<ComponentConfigImportable>,
    pub(crate) content_digest: ContentDigest,
}

#[derive(Debug, Clone)]
pub(crate) struct ComponentConfigImportable {
    // Workflows or Activities, not Webhooks
    pub(crate) exports_ext: Vec<FunctionMetadata>,
    pub(crate) exports_hierarchy_ext: Vec<PackageIfcFns>,
    pub(crate) retry_config: ComponentRetryConfig,
}

#[derive(Debug, Clone, Hash)]
pub(crate) struct ConfigStoreCommon {
    pub(crate) name: String,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: ContentDigest,
}

/// Create an identifier for given configuration.
/// Uniqueness of the hash part may not
pub(crate) fn config_id(
    config_id_type: ConfigIdType,
    hash: u64,
    name: StrVariant,
) -> Result<ConfigId, anyhow::Error> {
    let hash = StrVariant::from(format!("{hash:x}"));
    Ok(ConfigId::new(config_id_type, name, hash)?)
}

#[serde_as]
#[derive(Debug, Clone, Hash, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentLocation {
    Path(PathBuf),
    Oci(#[serde_as(as = "serde_with::DisplayFromStr")] oci_client::Reference),
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
                let content_digest = calculate_sha256_file(&wasm_path)
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
}
