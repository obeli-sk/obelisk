pub(crate) mod config_holder;
pub(crate) mod env_var;
pub(crate) mod toml;

use concepts::ComponentId;
use concepts::ContentDigest;
use concepts::FunctionMetadata;
use concepts::PackageIfcFns;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_with::serde_as;
use toml::ConfigName;

/// Holds information about components, used for gRPC services like `ListComponents`
#[derive(Debug, Clone)]
pub(crate) struct ComponentConfig {
    pub(crate) component_id: ComponentId,
    pub(crate) imports: Vec<FunctionMetadata>,
    pub(crate) workflow_or_activity_config: Option<ComponentConfigImportable>,
    pub(crate) wit: Option<String>,
}

#[derive(Debug, Clone)]
// Workflows or Activities (WASM, stub, external), but not Webhooks
pub(crate) struct ComponentConfigImportable {
    pub(crate) exports_ext: Vec<FunctionMetadata>,
    pub(crate) exports_hierarchy_ext: Vec<PackageIfcFns>,
}

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
