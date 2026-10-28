use chrono::{DateTime, Utc};
use concepts::{
    ExecutionId,
    prefixed_ulid::{DeploymentId, ExecutionIdTopLevel, NodeRunId, SystemEventId},
    storage,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SystemEventsResponse {
    pub events: Vec<SystemEventResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SystemEventResponse {
    #[schema(value_type = String)]
    pub event_id: SystemEventId,
    #[schema(value_type = String)]
    pub node_run_id: NodeRunId,
    // backcompat: 0.42.0-rc.3 event responses had no policy digest.
    pub app_config_digest: Option<String>,
    pub created_at: DateTime<Utc>,
    #[schema(value_type = String)]
    pub level: storage::SystemEventLevel,
    pub code: String,
    pub message: String,
    #[schema(value_type = Option<String>)]
    pub execution_id: Option<ExecutionId>,
    #[schema(value_type = Option<String>)]
    pub deployment_id: Option<DeploymentId>,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StorageStatusResponse {
    pub database_bytes: Option<u64>,
    pub execution_count: u64,
    pub deployment_count: u64,
    pub system_event_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RetainSystemEventsRequest {
    pub max_age_seconds: u64,
    pub batch_size: u32,
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RetainSystemEventsResponse {
    /// Events deleted, or that would be deleted when `dry_run` is true.
    pub deleted: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeleteResponse {
    pub deleted: bool,
    pub already_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeleteDeploymentResponse {
    pub deleted: bool,
    pub already_deleted: bool,
    pub deleted_execution_trees: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CleanupRequest {
    pub retain_count: Option<u32>,
    pub max_age_seconds: Option<u64>,
    pub batch_size: u32,
    #[serde(default)]
    pub force_non_terminal: bool,
    #[serde(default)]
    pub dry_run: bool,
    // backcompat: 0.43.0-rc.11 execution retention requests had no cursor.
    #[serde(default)]
    pub cursor: Option<ExecutionRetentionCursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionRetentionCursor {
    /// Ordering timestamp: `created_at` for count retention, `updated_at` for age retention.
    pub timestamp: DateTime<Utc>,
    #[schema(value_type = String)]
    pub execution_id: ExecutionIdTopLevel,
}

impl From<storage::ExecutionRetentionCursor> for ExecutionRetentionCursor {
    fn from(value: storage::ExecutionRetentionCursor) -> Self {
        Self {
            timestamp: value.timestamp,
            execution_id: value.execution_id,
        }
    }
}

impl From<ExecutionRetentionCursor> for storage::ExecutionRetentionCursor {
    fn from(value: ExecutionRetentionCursor) -> Self {
        Self {
            timestamp: value.timestamp,
            execution_id: value.execution_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentRetentionCursor {
    /// Ordering timestamp: `created_at` for count retention, `inactive_at` for age retention.
    pub timestamp: DateTime<Utc>,
    #[schema(value_type = String)]
    pub deployment_id: DeploymentId,
}

impl From<storage::DeploymentRetentionCursor> for DeploymentRetentionCursor {
    fn from(value: storage::DeploymentRetentionCursor) -> Self {
        Self {
            timestamp: value.timestamp,
            deployment_id: value.deployment_id,
        }
    }
}

impl From<DeploymentRetentionCursor> for storage::DeploymentRetentionCursor {
    fn from(value: DeploymentRetentionCursor) -> Self {
        Self {
            timestamp: value.timestamp,
            deployment_id: value.deployment_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
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
    // backcompat: 0.43.0-rc.11 retention requests had no cursor.
    /// Resume a deployment scan with the same retention policy and options.
    #[serde(default)]
    pub cursor: Option<DeploymentRetentionCursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CleanupResponse {
    pub deleted_execution_trees: u64,
    pub deleted_deployments: u64,
    pub retained: u64,
    pub blocked_non_terminal: u64,
    pub blocked_by_execution_reference: u64,
    pub has_more: bool,
    // backcompat: 0.43.0-rc.11 cleanup responses had no deployment cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_deployment_cursor: Option<DeploymentRetentionCursor>,
    // backcompat: 0.43.0-rc.11 cleanup responses had no execution cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_execution_cursor: Option<ExecutionRetentionCursor>,
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
            next_deployment_cursor: value
                .next_deployment_cursor
                .filter(|_| value.has_more)
                .map(Into::into),
            next_execution_cursor: value
                .next_execution_cursor
                .filter(|_| value.has_more)
                .map(Into::into),
        }
    }
}
