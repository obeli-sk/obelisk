use chrono::{DateTime, Utc};
use concepts::{
    ComponentType, ExecutionId, FinishedExecutionFailure, FunctionFqn,
    SupportedFunctionReturnValue,
    component_id::ComponentDigest,
    prefixed_ulid::DeploymentId,
    storage::{
        ExecutionEvent, ExecutionWithState, PendingState, ResponseCursor, ResponseWithCursor,
        Version,
    },
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use val_json::wast_val::WastVal;

/// Execution with current state information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionWithStateSer {
    /// Unique execution identifier
    #[schema(value_type = String, example = "E_01JKXYZ123456789ABCDEFGHIJ")]
    pub execution_id: ExecutionId,
    /// Fully qualified function name
    #[schema(value_type = String, example = "my-pkg:my-ifc/my-fn")]
    pub ffqn: FunctionFqn,
    /// Current pending state of the execution
    #[schema(value_type = Object)]
    pub pending_state: PendingState,
    /// When the execution was created
    pub created_at: DateTime<Utc>,
    /// When the execution was first scheduled
    pub first_scheduled_at: DateTime<Utc>,
    /// Component content digest
    #[schema(value_type = String)]
    pub component_digest: ComponentDigest,
    /// Type of component (workflow, activity, webhook)
    #[schema(value_type = String)]
    pub component_type: ComponentType,
    /// Deployment that created this execution
    #[schema(value_type = String, example = "Dep_01JKXYZ123456789ABCDEFGHIJ")]
    pub deployment_id: DeploymentId,
}

impl From<ExecutionWithState> for ExecutionWithStateSer {
    fn from(value: ExecutionWithState) -> Self {
        let ExecutionWithState {
            execution_id,
            ffqn,
            pending_state,
            created_at,
            first_scheduled_at,
            component_digest,
            component_type,
            deployment_id,
        } = value;
        ExecutionWithStateSer {
            execution_id,
            ffqn,
            pending_state,
            created_at,
            first_scheduled_at,
            component_digest,
            component_type,
            deployment_id,
        }
    }
}

/// Execution list item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionListItemSer {
    /// Opaque cursor of this item, valid for the `order` it was listed with
    pub cursor: String,
    #[serde(flatten)]
    pub execution: ExecutionWithStateSer,
}

/// Response containing execution events
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionEventsResponse {
    /// List of execution events
    #[schema(value_type = Vec<Object>)]
    pub events: Vec<ExecutionEvent>,
    /// Maximum version in the response
    #[schema(value_type = u32)]
    pub max_version: Version,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionEventsAndResponsesResponse {
    #[schema(value_type = Vec<Object>)]
    pub events: Vec<ExecutionEvent>,
    #[schema(value_type = Vec<Object>)]
    pub responses: Vec<ResponseWithCursor>,
    pub current_status: ExecutionWithStateSer,
    #[schema(value_type = u32)]
    pub max_version: Version,
    #[schema(value_type = u32)]
    pub max_cursor: ResponseCursor,
}

/// Response containing execution responses
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionResponsesResponse {
    /// List of responses with cursors
    #[schema(value_type = Vec<Object>)]
    pub responses: Vec<ResponseWithCursor>,
    /// Maximum cursor in the response
    #[schema(value_type = u32)]
    pub max_cursor: ResponseCursor,
    /// Cursor reached in the requested direction
    #[schema(value_type = u32)]
    pub scan_cursor: ResponseCursor,
}

/// Lossy representation of `SupportedFunctionReturnValue`. Does not contain WIT type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetVal {
    /// Successful result value
    #[schema(value_type = Option<Object>)]
    Ok(Option<serde_json::Value>),
    /// Error result (WIT result's error variant)
    #[schema(value_type = Option<Object>)]
    Err(Option<serde_json::Value>),
    /// Execution failed
    #[schema(value_type = Object)]
    ExecutionFailed(FinishedExecutionFailure),
}

impl From<SupportedFunctionReturnValue> for RetVal {
    fn from(value: SupportedFunctionReturnValue) -> RetVal {
        match value {
            SupportedFunctionReturnValue::Ok(val_with_type) => {
                RetVal::Ok(val_with_type.map(|it| wast_val_to_json(&it.value)))
            }
            SupportedFunctionReturnValue::Err(val_with_type) => {
                RetVal::Err(val_with_type.map(|it| wast_val_to_json(&it.value)))
            }
            SupportedFunctionReturnValue::ExecutionFailure(err) => RetVal::ExecutionFailed(err),
        }
    }
}

/// Payload for submitting a new execution
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, ToSchema)]
pub struct ExecutionSubmitPayload {
    /// Fully qualified function name to execute
    #[schema(value_type = String, example = "my-pkg:my-ifc/my-fn")]
    pub ffqn: FunctionFqn,
    /// Function parameters as JSON values
    pub params: Vec<serde_json::Value>,
    /// If true, create the execution in paused state.
    #[serde(default)]
    pub paused: bool,
}

/// Payload for upgrading an execution to a new component version
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionUpgradePayload {
    /// Old component digest to upgrade from
    #[schema(value_type = String)]
    pub old: ComponentDigest,
    /// New component digest to upgrade to
    #[schema(value_type = String)]
    pub new: ComponentDigest,
    /// Skip determinism check during upgrade
    #[serde(default)]
    pub skip_determinism_check: bool,
}

fn wast_val_to_json(value: &WastVal) -> serde_json::Value {
    serde_json::to_value(value).expect("WastVal must be JSON serializable")
}
