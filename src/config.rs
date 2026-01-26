pub(crate) mod config_holder;
pub(crate) mod env_var;
pub(crate) mod toml;

use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use concepts::ContentDigest;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_with::serde_as;
use toml::ConfigName;

use crate::{config::config_holder::PathPrefixes, oci};

#[derive(Debug, Clone, Hash)]
pub(crate) struct ConfigStoreCommon {
    pub(crate) name: ConfigName,
    pub(crate) location: ComponentLocationToml,
    pub(crate) content_digest: ContentDigest,
}

#[serde_as]
#[derive(Debug, Clone, Hash, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentLocationToml {
    Path(String), // String because it can contain path prefix
    Oci(
        #[serde_as(as = "serde_with::DisplayFromStr")]
        #[schemars(with = "String")]
        oci_client::Reference,
    ),
}
impl ComponentLocationToml {
    /// Fetch wasm file, calculate its content digest.
    ///
    /// Read wasm file either from local fs or pull from an OCI registry and cache it.
    /// Calculate the `content_digest`. File is not converted from Core to Component format.
    pub(crate) async fn fetch(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
        path_prefixes: &PathPrefixes,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
        use utils::sha256sum::calculate_sha256_file;

        match &self {
            ComponentLocationToml::Path(wasm_path) => {
                let wasm_path = path_prefixes.replace_file_prefix_verify_exists(wasm_path)?;
                // Future optimization: If the content digest is specified in TOML and wasm is already in the cache dir, do not recalculate it.
                let content_digest = calculate_sha256_file(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
                Ok((content_digest, wasm_path))
            }
            ComponentLocationToml::Oci(image) => {
                oci::pull_to_cache_dir(image, wasm_cache_dir, metadata_dir)
                    .await
                    .context("try cleaning the cache directory with `--clean-cache`")
            }
        }
    }
}

impl FromStr for ComponentLocationToml {
    type Err = oci_client::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(location) = s.strip_prefix("oci://") {
            Ok(ComponentLocationToml::Oci(oci_client::Reference::from_str(
                location,
            )?))
        } else {
            Ok(ComponentLocationToml::Path(s.to_string()))
        }
    }
}
