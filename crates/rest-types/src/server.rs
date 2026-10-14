use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DeploymentId, NodeRunId};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ServerInfoResponse {
    pub app_name: String,
    pub obelisk_version: String,
    #[schema(value_type = String)]
    pub deployment_id: DeploymentId,
    #[schema(value_type = String)]
    pub node_run_id: NodeRunId,
    pub app_config_digest: String,
    pub started_at: DateTime<Utc>,
}
