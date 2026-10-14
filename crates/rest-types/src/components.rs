use concepts::{
    ComponentId, FunctionExtension, FunctionFqn, FunctionMetadata, ParameterType,
    storage::{
        ComponentFileRole, DeploymentComponentFileDetail, PersistedFunctionMetadata,
        PersistedParameterType,
    },
};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Component configuration with optional imports/exports
#[derive(Serialize, Deserialize, ToSchema)]
pub struct ComponentConfig {
    /// Component identifier
    #[schema(value_type = Object)]
    pub component_id: ComponentId,
    /// Deployment-owned files used by this component.
    pub files: Vec<ComponentFileRef>,
    /// Imported functions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imports: Option<Vec<FunctionMetadataLite>>,
    /// Exported functions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exports: Option<Vec<FunctionMetadataLite>>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct ComponentFileRef {
    pub file: ComponentFile,
    #[schema(value_type = String)]
    pub role: ComponentFileRole,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct ComponentFile {
    pub path: String,
    pub digest: String,
    pub size: u64,
}

impl From<DeploymentComponentFileDetail> for ComponentFileRef {
    fn from(value: DeploymentComponentFileDetail) -> Self {
        Self {
            file: ComponentFile {
                path: value.file.path,
                digest: value.file.digest.to_string(),
                size: value.file.size,
            },
            role: value.role,
        }
    }
}

/// Lightweight function metadata
#[derive(Serialize, Deserialize, derive_more::Display, ToSchema)]
#[display("{ffqn}: func({}) -> {return_type}", parameter_types.iter().join(", "))]
pub struct FunctionMetadataLite {
    /// Fully qualified function name
    #[schema(value_type = String)]
    pub ffqn: FunctionFqn,
    /// Parameter types
    pub parameter_types: Vec<ParameterTypeLite>,
    /// Return type as WIT string
    pub return_type: String,
    /// Function extension type if any
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub extension: Option<FunctionExtension>,
    /// Externally submittable: primary functions + `-schedule` extended, but no activity stubs
    pub submittable: bool,
}

impl From<FunctionMetadata> for FunctionMetadataLite {
    fn from(value: FunctionMetadata) -> Self {
        FunctionMetadataLite {
            ffqn: value.ffqn,
            parameter_types: value
                .parameter_types
                .0
                .into_iter()
                .map(ParameterTypeLite::from)
                .collect(),
            return_type: value.return_type.wit_type().to_string(),
            extension: value.extension,
            submittable: value.submittable,
        }
    }
}

impl From<PersistedFunctionMetadata> for FunctionMetadataLite {
    fn from(value: PersistedFunctionMetadata) -> Self {
        FunctionMetadataLite {
            ffqn: value.ffqn,
            parameter_types: value
                .parameter_types
                .into_iter()
                .map(ParameterTypeLite::from)
                .collect(),
            return_type: value.return_type,
            extension: value.extension,
            submittable: value.submittable,
        }
    }
}

/// Parameter type information
#[derive(Serialize, Deserialize, derive_more::Display, ToSchema)]
#[display("{name}: {wit_type}")]
pub struct ParameterTypeLite {
    /// Parameter name
    pub name: String,
    /// WIT type representation
    pub wit_type: String,
}

impl From<ParameterType> for ParameterTypeLite {
    fn from(value: ParameterType) -> Self {
        ParameterTypeLite {
            name: value.name.to_string(),
            wit_type: value.wit_type.to_string(),
        }
    }
}

impl From<PersistedParameterType> for ParameterTypeLite {
    fn from(value: PersistedParameterType) -> Self {
        ParameterTypeLite {
            name: value.name,
            wit_type: value.wit_type,
        }
    }
}
