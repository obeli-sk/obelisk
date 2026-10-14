use crate::backtrace::BacktraceInfoSer;
use chrono::{DateTime, Utc};
use concepts::{
    ExecutionId, FinishedExecutionFailure, JoinSetId, SupportedFunctionReturnValue,
    storage::{PendingState, Version},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// A serializable mirror of `CreateRequest` from concepts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CreateRequestSer {
    pub created_at: DateTime<Utc>,
    pub execution_id: String,
    pub ffqn: String,
    #[schema(value_type = Vec<Object>)]
    pub params: concepts::Params,
    pub parent_execution_id: Option<String>,
    pub parent_join_set_id: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    #[schema(value_type = Object)]
    pub component_id: concepts::ComponentId,
    pub deployment_id: String,
    #[schema(value_type = Object)]
    pub metadata: concepts::ExecutionMetadata,
    pub scheduled_by: Option<String>,
    pub paused: bool,
    #[serde(default = "concepts::persisted_value::legacy_unlimited_persisted_value_size")]
    pub max_persisted_value_size_bytes: u64,
}

impl From<concepts::storage::CreateRequest> for CreateRequestSer {
    fn from(r: concepts::storage::CreateRequest) -> Self {
        let (parent_execution_id, parent_join_set_id) = r
            .parent
            .map(|(execution_id, join_set_id)| {
                (
                    Some(execution_id.to_string()),
                    Some(join_set_id.to_string()),
                )
            })
            .unwrap_or((None, None));
        Self {
            created_at: r.created_at,
            execution_id: r.execution_id.to_string(),
            ffqn: r.ffqn.to_string(),
            params: r.params,
            parent_execution_id,
            parent_join_set_id,
            scheduled_at: r.scheduled_at,
            component_id: r.component_id,
            deployment_id: r.deployment_id.to_string(),
            metadata: r.metadata,
            scheduled_by: r.scheduled_by.map(|execution_id| execution_id.to_string()),
            paused: r.paused,
            max_persisted_value_size_bytes: r.max_persisted_value_size_bytes,
        }
    }
}

impl TryFrom<CreateRequestSer> for concepts::storage::CreateRequest {
    type Error = String;

    fn try_from(value: CreateRequestSer) -> Result<Self, Self::Error> {
        let parent = match (value.parent_execution_id, value.parent_join_set_id) {
            (Some(execution_id), Some(join_set_id)) => Some((
                execution_id
                    .parse()
                    .map_err(|err| format!("invalid parent_execution_id - {err}"))?,
                join_set_id
                    .parse::<JoinSetId>()
                    .map_err(|err| format!("invalid parent_join_set_id - {err}"))?,
            )),
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => {
                return Err(
                    "parent_execution_id and parent_join_set_id must be both set or both omitted"
                        .to_string(),
                );
            }
        };

        Ok(Self {
            created_at: value.created_at,
            execution_id: value
                .execution_id
                .parse()
                .map_err(|err| format!("invalid execution_id - {err}"))?,
            ffqn: value
                .ffqn
                .parse()
                .map_err(|err| format!("invalid ffqn - {err}"))?,
            params: value.params,
            parent,
            scheduled_at: value.scheduled_at,
            component_id: value.component_id,
            deployment_id: value
                .deployment_id
                .parse()
                .map_err(|err| format!("invalid deployment_id - {err}"))?,
            metadata: value.metadata,
            scheduled_by: value
                .scheduled_by
                .map(|execution_id| {
                    execution_id
                        .parse()
                        .map_err(|err| format!("invalid scheduled_by - {err}"))
                })
                .transpose()?,
            paused: value.paused,
            max_persisted_value_size_bytes: value.max_persisted_value_size_bytes,
        })
    }
}

/// A serializable mirror of `AppendResponseToExecution` from concepts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StubResponseSer {
    pub parent_execution_id: String,
    pub created_at: DateTime<Utc>,
    pub join_set_id: String,
    pub child_execution_id: String,
    pub finished_version: u32,
    #[schema(value_type = Object)]
    pub result: SupportedFunctionReturnValue,
}

impl From<concepts::storage::AppendResponseToExecution> for StubResponseSer {
    fn from(r: concepts::storage::AppendResponseToExecution) -> Self {
        Self {
            parent_execution_id: r.parent_execution_id.to_string(),
            created_at: r.created_at,
            join_set_id: r.join_set_id.to_string(),
            child_execution_id: r.child_execution_id.to_string(),
            finished_version: r.finished_version.0,
            result: r.result,
        }
    }
}

impl TryFrom<StubResponseSer> for concepts::storage::AppendResponseToExecution {
    type Error = String;

    fn try_from(value: StubResponseSer) -> Result<Self, Self::Error> {
        let child_execution_id = value
            .child_execution_id
            .parse::<ExecutionId>()
            .map_err(|err| format!("invalid child_execution_id - {err}"))?;
        let ExecutionId::Derived(child_execution_id) = child_execution_id else {
            return Err("child_execution_id must be a derived execution id".to_string());
        };

        Ok(Self {
            parent_execution_id: value
                .parent_execution_id
                .parse()
                .map_err(|err| format!("invalid parent_execution_id - {err}"))?,
            created_at: value.created_at,
            join_set_id: value
                .join_set_id
                .parse()
                .map_err(|err| format!("invalid join_set_id - {err}"))?,
            child_execution_id,
            finished_version: Version::new(value.finished_version),
            result: value.result,
        })
    }
}

/// A write captured during replay, produced by the replay endpoint and applied by the advance endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CapturedWriteSer {
    Append {
        execution_id: String,
        version: u32,
        #[schema(value_type = Object)]
        event: concepts::storage::AppendRequest,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        backtraces: Vec<BacktraceInfoSer>,
    },
    AppendBatch {
        #[schema(value_type = Vec<Object>)]
        events: Vec<concepts::storage::AppendRequest>,
        execution_id: String,
        version: u32,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        backtraces: Vec<BacktraceInfoSer>,
    },
    AppendBatchCreateNewExecution {
        #[schema(value_type = Vec<Object>)]
        events: Vec<concepts::storage::AppendRequest>,
        execution_id: String,
        version: u32,
        child_requests: Vec<CreateRequestSer>,
        backtraces: Vec<BacktraceInfoSer>,
    },
    AppendBatchWithDelayResponse {
        #[schema(value_type = Vec<Object>)]
        events: Vec<concepts::storage::AppendRequest>,
        execution_id: String,
        version: u32,
        join_set_id: String,
        delay_id: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        backtraces: Vec<BacktraceInfoSer>,
    },
    AppendStubResponse {
        execution_id: String,
        version: u32,
        #[schema(value_type = Vec<Object>)]
        events: Vec<concepts::storage::AppendRequest>,
        response: StubResponseSer,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        backtraces: Vec<BacktraceInfoSer>,
    },
    AppendFinished {
        execution_id: String,
        version: u32,
        #[schema(value_type = Object)]
        retval: SupportedFunctionReturnValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_execution_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_join_set_id: Option<String>,
    },
}

impl From<concepts::storage::CapturedDbWrite> for CapturedWriteSer {
    fn from(w: concepts::storage::CapturedDbWrite) -> Self {
        use concepts::storage::CapturedDbWrite;
        match w {
            CapturedDbWrite::Append {
                execution_id,
                version,
                req,
                backtraces,
            } => CapturedWriteSer::Append {
                execution_id: execution_id.to_string(),
                version: version.0,
                event: req,
                backtraces: backtraces.into_iter().map(BacktraceInfoSer::from).collect(),
            },
            CapturedDbWrite::AppendBatch {
                current_time: _,
                batch,
                execution_id,
                version,
                backtraces,
            } => CapturedWriteSer::AppendBatch {
                events: batch,
                execution_id: execution_id.to_string(),
                version: version.0,
                backtraces: backtraces.into_iter().map(BacktraceInfoSer::from).collect(),
            },
            CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time: _,
                batch,
                execution_id,
                version,
                child_req,
                backtraces,
            } => CapturedWriteSer::AppendBatchCreateNewExecution {
                events: batch,
                execution_id: execution_id.to_string(),
                version: version.0,
                child_requests: child_req.into_iter().map(CreateRequestSer::from).collect(),
                backtraces: backtraces.into_iter().map(BacktraceInfoSer::from).collect(),
            },
            CapturedDbWrite::AppendBatchWithDelayResponse {
                current_time: _,
                batch,
                execution_id,
                version,
                join_set_id,
                delay_id,
                backtraces,
            } => CapturedWriteSer::AppendBatchWithDelayResponse {
                events: batch,
                execution_id: execution_id.to_string(),
                version: version.0,
                join_set_id: join_set_id.to_string(),
                delay_id: delay_id.to_string(),
                backtraces: backtraces.into_iter().map(BacktraceInfoSer::from).collect(),
            },
            CapturedDbWrite::AppendStubResponse {
                events,
                response,
                current_time: _,
                backtraces,
            } => CapturedWriteSer::AppendStubResponse {
                execution_id: events.execution_id.to_string(),
                version: events.version.0,
                events: events.batch,
                response: StubResponseSer::from(response),
                backtraces: backtraces.into_iter().map(BacktraceInfoSer::from).collect(),
            },
            CapturedDbWrite::AppendFinished {
                execution_id,
                version,
                retval,
                current_time: _,
                parent,
            } => CapturedWriteSer::AppendFinished {
                execution_id: execution_id.to_string(),
                version: version.0,
                retval,
                parent_execution_id: parent.as_ref().map(|(id, _)| id.to_string()),
                parent_join_set_id: parent.map(|(_, js)| js.to_string()),
            },
        }
    }
}

impl TryFrom<CapturedWriteSer> for concepts::storage::CapturedDbWrite {
    type Error = String;

    fn try_from(value: CapturedWriteSer) -> Result<Self, Self::Error> {
        match value {
            CapturedWriteSer::Append {
                execution_id,
                version,
                event,
                backtraces,
            } => Ok(Self::Append {
                execution_id: execution_id
                    .parse()
                    .map_err(|err| format!("invalid execution_id - {err}"))?,
                version: Version::new(version),
                req: event,
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendBatch {
                events,
                execution_id,
                version,
                backtraces,
            } => Ok(Self::AppendBatch {
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                batch: events,
                execution_id: execution_id
                    .parse()
                    .map_err(|err| format!("invalid execution_id - {err}"))?,
                version: Version::new(version),
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendBatchCreateNewExecution {
                events,
                execution_id,
                version,
                child_requests,
                backtraces,
            } => Ok(Self::AppendBatchCreateNewExecution {
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                batch: events,
                execution_id: execution_id
                    .parse()
                    .map_err(|err| format!("invalid execution_id - {err}"))?,
                version: Version::new(version),
                child_req: child_requests
                    .into_iter()
                    .map(concepts::storage::CreateRequest::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendBatchWithDelayResponse {
                events,
                execution_id,
                version,
                join_set_id,
                delay_id,
                backtraces,
            } => Ok(Self::AppendBatchWithDelayResponse {
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                batch: events,
                execution_id: execution_id
                    .parse()
                    .map_err(|err| format!("invalid execution_id - {err}"))?,
                version: Version::new(version),
                join_set_id: join_set_id
                    .parse()
                    .map_err(|err| format!("invalid join_set_id - {err}"))?,
                delay_id: delay_id
                    .parse()
                    .map_err(|err| format!("invalid delay_id - {err}"))?,
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendStubResponse {
                execution_id,
                version,
                events,
                response,
                backtraces,
            } => Ok(Self::AppendStubResponse {
                events: concepts::storage::AppendEventsToExecution {
                    execution_id: execution_id
                        .parse()
                        .map_err(|err| format!("invalid execution_id - {err}"))?,
                    version: Version::new(version),
                    batch: events,
                },
                response: response.try_into()?,
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendFinished {
                execution_id,
                version,
                retval,
                parent_execution_id,
                parent_join_set_id,
            } => {
                let parent = match (parent_execution_id, parent_join_set_id) {
                    (Some(exec_id), Some(js_id)) => Some((
                        exec_id
                            .parse()
                            .map_err(|err| format!("invalid parent_execution_id - {err}"))?,
                        js_id
                            .parse()
                            .map_err(|err| format!("invalid parent_join_set_id - {err}"))?,
                    )),
                    (None, None) => None,
                    _ => {
                        return Err(
                            "parent_execution_id and parent_join_set_id must both be set or both be unset"
                                .to_string(),
                        );
                    }
                };
                Ok(Self::AppendFinished {
                    execution_id: execution_id
                        .parse()
                        .map_err(|err| format!("invalid execution_id - {err}"))?,
                    version: Version::new(version),
                    retval,
                    current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                    parent,
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ReplayResponseSer {
    #[serde(flatten)]
    pub outcome: ReplayOutcomeSer,
    /// Number of persisted history events supplied to the workflow replay.
    pub replayed_event_count: u64,
    /// Time spent executing the workflow replay after loading its execution log.
    pub replay_duration_ms: u64,
    /// Highest persisted execution-event version included in the replay log. Response records use
    /// a separate cursor and are not represented by this version.
    pub replay_version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplayOutcomeSer {
    Advanceable {
        captured_writes: Vec<CapturedWriteSer>,
    },
    Finished {
        retval: serde_json::Value, // SupportedFunctionReturnValue -> RetVal
    },
    Blocked,
    ReplayFailed {
        error: String,
        /// Structured, sanitized reason for the replay failure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(value_type = Option<Object>)]
        failure: Option<FinishedExecutionFailure>,
        captured_writes: Vec<CapturedWriteSer>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AdvanceRequestSer {
    pub captured_writes: Vec<CapturedWriteSer>,
    /// Capture and persist fresh call-site backtraces while advancing.
    #[serde(default)]
    pub persist_backtrace: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdvanceResponseSer {
    Finished {
        value: crate::executions::RetVal,
    },
    InProgress {
        #[schema(value_type = Object)]
        pending_state: PendingState,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdvanceErrorSer {
    VersionMismatch { expected: u32 },
    ReplayMismatch,
    Transient(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PersistBacktracesResponseSer {
    /// Number of newly persisted backtraces (already-persisted ones are skipped).
    pub persisted_backtrace_count: u32,
}
