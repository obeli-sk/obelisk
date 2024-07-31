use anyhow::Context;
use concepts::ContentDigest;
use serde_with::serde_as;
use std::path::{Path, PathBuf};

use crate::oci;

pub(crate) mod store;
pub(crate) mod toml;

#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
            ComponentLocation::Oci(image) => oci::obtain_wasm_from_oci(image, wasm_cache_dir).await,
        }
    }
}
