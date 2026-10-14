use chrono::{DateTime, Utc};
use concepts::{
    ExecutionId,
    prefixed_ulid::{DeploymentId, NodeRunId, SystemEventId},
    storage,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SystemEventsResponse {
    pub events: Vec<SystemEventResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SystemEventResponse {
    #[schema(value_type = String)]
    pub event_id: SystemEventId,
    #[schema(value_type = String)]
    pub node_run_id: NodeRunId,
    // backcompat: 0.42.0-rc.3 event responses had no policy digest.
    pub app_config_digest: Option<String>,
    pub created_at: DateTime<Utc>,
    pub level: String,
    pub code: String,
    pub message: String,
    #[schema(value_type = Option<String>)]
    pub execution_id: Option<ExecutionId>,
    #[schema(value_type = Option<String>)]
    pub deployment_id: Option<DeploymentId>,
    pub details: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct StorageStatusResponse {
    pub database_bytes: Option<u64>,
    pub execution_count: u64,
    pub deployment_count: u64,
    pub system_event_count: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RetainSystemEventsRequest {
    pub max_age_seconds: u64,
    pub batch_size: u32,
    pub dry_run: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RetainSystemEventsResponse {
    /// Events deleted, or that would be deleted when `dry_run` is true.
    pub deleted: u64,
    pub has_more: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteResponse {
    pub deleted: bool,
    pub already_deleted: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteDeploymentResponse {
    pub deleted: bool,
    pub already_deleted: bool,
    pub deleted_execution_trees: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CleanupRequest {
    pub retain_count: Option<u32>,
    pub max_age_seconds: Option<u64>,
    pub batch_size: u32,
    #[serde(default)]
    pub force_non_terminal: bool,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RetainDeploymentsRequest {
    pub retain_count: Option<u32>,
    pub max_age_seconds: Option<u64>,
    pub batch_size: u32,
    #[serde(default)]
    pub delete_executions: bool,
    #[serde(default)]
    pub force_non_terminal: bool,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CleanupResponse {
    pub deleted_execution_trees: u64,
    pub deleted_deployments: u64,
    pub retained: u64,
    pub blocked_non_terminal: u64,
    pub blocked_by_execution_reference: u64,
    pub has_more: bool,
}

impl From<storage::CleanupResult> for CleanupResponse {
    fn from(value: storage::CleanupResult) -> Self {
        Self {
            deleted_execution_trees: value.deleted_execution_trees,
            deleted_deployments: value.deleted_deployments,
            retained: value.retained,
            blocked_non_terminal: value.blocked_non_terminal,
            blocked_by_execution_reference: value.blocked_by_execution_reference,
            has_more: value.has_more,
        }
    }
}
