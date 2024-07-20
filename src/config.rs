use serde_with::serde_as;
use std::path::PathBuf;

pub(crate) mod store;
pub(crate) mod toml;

#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentLocation {
    File(PathBuf),
    Oci(#[serde_as(as = "serde_with::DisplayFromStr")] oci_distribution::Reference),
}
