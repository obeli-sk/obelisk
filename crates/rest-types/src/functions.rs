use concepts::{FunctionExtension, FunctionFqn};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Function output information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FunctionOutput {
    /// Fully qualified function name
    #[schema(value_type = String)]
    pub ffqn: FunctionFqn,
    /// Extension type if any
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub extension: Option<FunctionExtension>,
}
