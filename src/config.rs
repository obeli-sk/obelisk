pub(crate) mod config_holder;
pub(crate) mod env_var;
pub(crate) mod toml;

use concepts::ContentDigest;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_with::serde_as;
use toml::ConfigName;

#[derive(Debug, Clone, Hash)]
pub(crate) struct ConfigStoreCommon {
    pub(crate) name: ConfigName,
    pub(crate) location: ComponentLocation,
    pub(crate) content_digest: ContentDigest,
}

#[serde_as]
#[derive(Debug, Clone, Hash, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentLocation {
    Path(String),
    Oci(
        #[serde_as(as = "serde_with::DisplayFromStr")]
        #[schemars(with = "String")]
        oci_client::Reference,
    ),
}
