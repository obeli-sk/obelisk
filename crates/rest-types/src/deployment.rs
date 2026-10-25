use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::{DeploymentFileRecord, DeploymentRecord, DeploymentStatus};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentComponentType {
    WorkflowWasm,
    WorkflowJs,
    ActivityWasm,
    ActivityJs,
    ActivityExec,
    ActivityVm,
    ActivityStub,
    ActivityExternal,
    WebhookEndpointWasm,
    WebhookEndpointJs,
    Cron,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentComponentCount {
    pub component_type: DeploymentComponentType,
    pub count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentComponentSummary {
    pub components: Vec<DeploymentComponentCount>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentStatusSer {
    Inactive,
    Enqueued,
    Active,
}

impl From<&DeploymentStatus> for DeploymentStatusSer {
    fn from(s: &DeploymentStatus) -> Self {
        match s {
            DeploymentStatus::Inactive => Self::Inactive,
            DeploymentStatus::Enqueued => Self::Enqueued,
            DeploymentStatus::Active => Self::Active,
        }
    }
}
/// Deployment state with execution counts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentStateSer {
    /// Deployment identifier
    #[schema(value_type = String, example = "Dep_01JKXYZ123456789ABCDEFGHIJ")]
    pub deployment_id: DeploymentId,
    /// Optional human-readable deployment description
    pub description: Option<String>,
    /// Content digest = `sha256(deployment_toml)`
    pub digest: String,
    /// Deployment lifecycle status
    pub status: DeploymentStatusSer,
    /// When this deployment was submitted
    pub created_at: DateTime<Utc>,
    /// When this deployment was last active; None if never active
    pub last_active_at: Option<DateTime<Utc>>,
    // backcompat: 0.42.0-rc.3 deployment responses had no policy digest.
    pub last_active_app_config_digest: Option<String>,
    /// Number of locked executions
    pub locked: u32,
    /// Number of pending executions
    pub pending: u32,
    /// Number of scheduled executions
    pub scheduled: u32,
    /// Number of blocked executions
    pub blocked: u32,
    /// Number of paused executions, regardless of the underlying state
    pub paused: u32,
    /// Number of executions with cancellation requested; teardown in progress
    pub cancelling: u32,
    /// Number of executions finished successfully
    pub finished_ok: u32,
    /// Number of executions finished with the `err` variant of the result type
    pub finished_error: u32,
    /// Number of executions finished with an execution failure
    pub finished_execution_failure: u32,
    /// Component counts by manifest section type. Absent when not requested or unavailable.
    pub component_summary: Option<DeploymentComponentSummary>,
    /// Processed deployment manifest, present when requested.
    pub deployment_toml: Option<String>,
}

/// A deployment-owned file the manifest references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FileRefSer {
    /// Deployment-relative path.
    pub path: String,
    /// Content digest ("sha256:...").
    pub digest: String,
    /// Byte length of the referenced blob.
    pub size: u64,
}

impl From<&DeploymentFileRecord> for FileRefSer {
    fn from(f: &DeploymentFileRecord) -> Self {
        Self {
            path: f.path.clone(),
            digest: f.digest.to_string(),
            size: f.size,
        }
    }
}

/// Deployment details with config
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentRecordSer {
    #[schema(value_type = String)]
    pub deployment_id: DeploymentId,
    pub description: Option<String>,
    /// Content digest = `sha256(deployment_toml)`
    pub digest: String,
    pub status: DeploymentStatusSer,
    pub created_at: DateTime<Utc>,
    pub last_active_at: Option<DateTime<Utc>>,
    // backcompat: 0.42.0-rc.3 deployment responses had no policy digest.
    pub last_active_app_config_digest: Option<String>,
    /// Processed deployment manifest (`deployment.toml`), stored byte-for-byte
    pub deployment_toml: String,
    /// Files the manifest references.
    pub files: Vec<FileRefSer>,
}

impl From<&DeploymentRecord> for DeploymentRecordSer {
    fn from(r: &DeploymentRecord) -> Self {
        Self {
            deployment_id: r.deployment_id,
            description: r.description.clone(),
            digest: r.digest.to_string(),
            status: DeploymentStatusSer::from(&r.status),
            created_at: r.created_at,
            last_active_at: r.last_active_at,
            last_active_app_config_digest: r.last_active_app_config_digest.clone(),
            deployment_toml: r.deployment_toml.clone(),
            files: r.files.iter().map(FileRefSer::from).collect(),
        }
    }
}

/// Request payload for submitting a new deployment
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentSubmitPayload {
    /// Processed deployment manifest (`deployment.toml`). Referenced file blobs must
    /// be uploaded separately after the server returns `missing_digests`.
    pub deployment_toml: String,
    /// Optional human-readable deployment description
    pub description: Option<String>,
    /// Store the deployment while app or platform approval is pending. Tolerates
    /// unavailable values and approvals; activation still checks strictly.
    #[serde(default)]
    pub allow_unavailable_runtime_config: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentSubmitResponse {
    pub deployment_id: String,
}

/// A single file-set problem found while validating a submit package.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FileIssue {
    pub section: String,
    pub component_name: Option<String>,
    pub field_path: String,
    pub path: Option<String>,
    pub digest: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DigestMismatch {
    pub file: FileIssue,
    pub supplied_digest: String,
    pub actual_digest: String,
}

/// Structured `409` body describing why a submit package was rejected. No
/// deployment is stored when this is returned. A client retries the same submit
/// with the `missing_files` blobs attached.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SubmitPackageErrorBody {
    pub missing_digest_fields: Vec<FileIssue>,
    pub missing_files: Vec<FileIssue>,
    pub unexpected_files: Vec<FileIssue>,
    pub digest_mismatches: Vec<DigestMismatch>,
    pub oversized_files: Vec<FileIssue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MissingRuntimeConfigErrorBody {
    pub error: String,
    #[serde(default)]
    pub public_env: Vec<String>,
    pub optional_public_env: Vec<String>,
    pub required_public_env: Vec<String>,
    pub secrets: Vec<String>,
    /// Subset of `secrets` whose every reference is optional.
    #[serde(default)]
    pub optional_secrets: Vec<String>,
    /// Registered as optional and unset, yet referenced as required.
    #[serde(default)]
    pub unset_secrets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct GenericErrorBody {
    pub err: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum DeploymentSubmitErrorBody {
    InvalidConfig(GenericErrorBody),
    MissingRuntimeConfig(MissingRuntimeConfigErrorBody),
}

pub use crate::lifecycle::{
    DeploymentSwitchOutcome, DeploymentSwitchPayload, DeploymentSwitchResponse,
};
