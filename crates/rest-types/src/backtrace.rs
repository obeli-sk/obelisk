use concepts::{
    ExecutionId,
    storage::{BacktraceInfo, SourceMapping, Version, VersionType, WasmBacktrace},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Serializable version of `BacktraceInfo`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BacktraceInfoSer {
    #[schema(value_type = String)]
    pub execution_id: ExecutionId,
    #[schema(value_type = Object)]
    pub component_id: concepts::ComponentId,
    #[schema(value_type = String)]
    pub version_min_including: VersionType,
    #[schema(value_type = String)]
    pub version_max_excluding: VersionType,
    #[schema(value_type = Object)]
    pub wasm_backtrace: WasmBacktrace,
}

impl From<BacktraceInfo> for BacktraceInfoSer {
    fn from(value: BacktraceInfo) -> Self {
        BacktraceInfoSer {
            execution_id: value.execution_id,
            component_id: value.component_id,
            version_min_including: value.version_min_including.0,
            version_max_excluding: value.version_max_excluding.0,
            wasm_backtrace: value.wasm_backtrace,
        }
    }
}

impl TryFrom<BacktraceInfoSer> for BacktraceInfo {
    type Error = String;

    fn try_from(value: BacktraceInfoSer) -> Result<Self, Self::Error> {
        Ok(Self {
            execution_id: value.execution_id,
            component_id: value.component_id,
            version_min_including: Version::new(value.version_min_including),
            version_max_excluding: Version::new(value.version_max_excluding),
            wasm_backtrace: value.wasm_backtrace,
        })
    }
}

/// A source file registered for a component.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ComponentSourceSer {
    /// File key accepted by the source endpoints. Suffix keys keep their `.../` prefix.
    pub file: String,
    pub digest: String,
}

impl From<SourceMapping> for ComponentSourceSer {
    fn from(value: SourceMapping) -> Self {
        let file = if value.is_suffix {
            format!("...{}", value.frame_key)
        } else {
            value.frame_key
        };
        Self {
            file,
            digest: value.digest.to_string(),
        }
    }
}
