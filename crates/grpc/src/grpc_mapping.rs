use crate::grpc_gen::{self, execution_event::history_event, result_kind};
use chrono::DateTime;
use concepts::{
    ComponentId, ComponentRetryConfig, ComponentType, ExecutionFailureKind, ExecutionId,
    ExecutionMetadata, FinishedExecutionFailure, FunctionFqn, StrVariant,
    SupportedFunctionReturnValue,
    component_id::{ComponentDigest, Digest},
    prefixed_ulid::{DelayId, DeploymentId, ExecutorId, RunId},
    storage::{
        AppendEventsToExecution, AppendRequest, AppendResponseToExecution, BacktraceInfo,
        CancelOutcome, CapturedDbWrite, ChildExecutionRequestError, ComponentUpgradeOutcome,
        ComponentUpgradeReason, CreateRequest, DbErrorGeneric, DbErrorRead, DbErrorWrite,
        DbErrorWriteNonRetriable, ExecutionEvent, ExecutionListPagination, ExecutionRequest,
        ExecutionWithState, HistoryEvent, HistoryEventScheduleAt, JoinSetRequest, Locked, LockedBy,
        LogEntry, LogEntryRow, LogLevel, LogStreamType, Pagination, PendingState,
        PendingStateBlockedByJoinSet, PendingStateFinished, PendingStateFinishedError,
        PendingStateFinishedResultKind, PendingStateLocked, PendingStatePendingAt, PersistKind,
        ScheduleRequestError, StubError, Unlocked, Version, VersionParseError,
        http_client_trace::HttpClientTrace,
    },
};
use concepts::{JoinSetId, JoinSetKind};
use serde::de::DeserializeOwned;
use std::{borrow::Borrow, collections::HashMap, str::FromStr};
use tracing::warn;
use val_json::{type_wrapper::parse_wit_type, wast_val_ser::deserialize_slice};

impl<T: Borrow<ExecutionId>> From<T> for grpc_gen::ExecutionId {
    fn from(value: T) -> Self {
        Self {
            id: value.borrow().to_string(),
        }
    }
}

impl From<DelayId> for grpc_gen::DelayId {
    fn from(value: DelayId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}
impl TryFrom<grpc_gen::DelayId> for DelayId {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::DelayId) -> Result<Self, Self::Error> {
        DelayId::from_str(&value.id).map_err(|err| {
            tonic::Status::invalid_argument(format!("DelayId cannot be parsed - {err}"))
        })
    }
}

impl From<JoinSetId> for grpc_gen::JoinSetId {
    fn from(join_set_id: JoinSetId) -> Self {
        (&join_set_id).into()
    }
}
impl From<&JoinSetId> for grpc_gen::JoinSetId {
    fn from(join_set_id: &JoinSetId) -> Self {
        Self {
            kind: grpc_gen::join_set_id::JoinSetKind::from(join_set_id.kind) as i32,
            name: join_set_id.name.to_string(),
        }
    }
}

impl From<JoinSetKind> for grpc_gen::join_set_id::JoinSetKind {
    fn from(value: JoinSetKind) -> Self {
        match value {
            JoinSetKind::OneOff => grpc_gen::join_set_id::JoinSetKind::OneOff,
            JoinSetKind::Named => grpc_gen::join_set_id::JoinSetKind::Named,
            JoinSetKind::Generated => grpc_gen::join_set_id::JoinSetKind::Generated,
        }
    }
}

impl TryFrom<grpc_gen::join_set_id::JoinSetKind> for JoinSetKind {
    type Error = ();

    fn try_from(value: grpc_gen::join_set_id::JoinSetKind) -> Result<JoinSetKind, Self::Error> {
        match value {
            grpc_gen::join_set_id::JoinSetKind::Unspecified => Err(()),
            grpc_gen::join_set_id::JoinSetKind::OneOff => Ok(JoinSetKind::OneOff),
            grpc_gen::join_set_id::JoinSetKind::Named => Ok(JoinSetKind::Named),
            grpc_gen::join_set_id::JoinSetKind::Generated => Ok(JoinSetKind::Generated),
        }
    }
}

impl TryFrom<grpc_gen::JoinSetId> for JoinSetId {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::JoinSetId) -> Result<Self, Self::Error> {
        let kind = grpc_gen::join_set_id::JoinSetKind::try_from(value.kind)
            .map_err(|_| tonic::Status::invalid_argument("join_set_id.kind cannot be parsed"))?;
        let kind = JoinSetKind::try_from(kind)
            .map_err(|()| tonic::Status::invalid_argument("join_set_id.kind cannot be parsed"))?;
        JoinSetId::new(kind, StrVariant::from(value.name))
            .map_err(|err| tonic::Status::invalid_argument(format!("join_set_id invalid - {err}")))
    }
}

impl From<RunId> for grpc_gen::RunId {
    fn from(value: RunId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<ComponentRetryConfig> for grpc_gen::ComponentRetryConfig {
    fn from(value: ComponentRetryConfig) -> Self {
        Self {
            max_retries: value.max_retries,
            retry_exp_backoff: Some(
                prost_wkt_types::Duration::try_from(value.retry_exp_backoff)
                    .expect("retry_exp_backoff must fit protobuf Duration"),
            ),
        }
    }
}

impl TryFrom<grpc_gen::ComponentRetryConfig> for ComponentRetryConfig {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::ComponentRetryConfig) -> Result<Self, Self::Error> {
        let retry_exp_backoff = value
            .retry_exp_backoff
            .argument_must_exist("retry_exp_backoff")?
            .try_into()
            .map_err(|err| {
                tonic::Status::invalid_argument(format!(
                    "retry_exp_backoff cannot be parsed - {err}"
                ))
            })?;
        Ok(Self {
            max_retries: value.max_retries,
            retry_exp_backoff,
        })
    }
}

impl From<DeploymentId> for grpc_gen::DeploymentId {
    fn from(value: DeploymentId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}
impl TryFrom<grpc_gen::DeploymentId> for DeploymentId {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::DeploymentId) -> Result<Self, Self::Error> {
        DeploymentId::from_str(&value.id)
            .map_err(|_| tonic::Status::invalid_argument("DeploymentId cannot be parsed"))
    }
}

impl From<ComponentId> for grpc_gen::ComponentId {
    fn from(value: ComponentId) -> Self {
        Self {
            component_type: grpc_gen::ComponentType::from(value.component_type).into(),
            name: value.name.to_string(),
            digest: Some(value.component_digest.into()),
        }
    }
}

impl From<ComponentDigest> for grpc_gen::ContentDigest {
    fn from(value: ComponentDigest) -> Self {
        grpc_gen::ContentDigest::from(&value)
    }
}
impl From<&ComponentDigest> for grpc_gen::ContentDigest {
    fn from(value: &ComponentDigest) -> Self {
        grpc_gen::ContentDigest {
            digest: value.to_string(),
        }
    }
}
impl TryFrom<grpc_gen::ContentDigest> for ComponentDigest {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::ContentDigest) -> Result<Self, Self::Error> {
        let digest = Digest::from_str(&value.digest).map_err(|parse_err| {
            warn!("`Digest` cannot be parsed - {parse_err:?}");
            tonic::Status::invalid_argument(format!("`Digest` cannot be parsed - {parse_err}"))
        })?;
        Ok(ComponentDigest(digest))
    }
}

impl TryFrom<grpc_gen::ComponentId> for ComponentId {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::ComponentId) -> Result<ComponentId, Self::Error> {
        let component_type =
            grpc_gen::ComponentType::try_from(value.component_type).map_err(|parse_err| {
                warn!("`component_type` cannot be parsed - {parse_err:?}");
                tonic::Status::invalid_argument(format!(
                    "`component_type` cannot be parsed - {parse_err}"
                ))
            })?;
        let component_type = ComponentType::try_from(component_type)?;
        let Some(input_digest) = value.digest else {
            return Err(tonic::Status::invalid_argument(
                "`input_digest` is mandatory",
            ));
        };
        let input_digest = ComponentDigest::try_from(input_digest)?;
        ComponentId::new(component_type, value.name.into(), input_digest).map_err(|parse_err| {
            warn!("`name` is invalid - {parse_err:?}");
            tonic::Status::invalid_argument(format!("name cannot be parsed - {parse_err}"))
        })
    }
}

impl TryFrom<grpc_gen::ExecutionId> for ExecutionId {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::ExecutionId) -> Result<Self, Self::Error> {
        value.id.parse().map_err(|parse_err| {
            tonic::Status::invalid_argument(format!("ExecutionId cannot be parsed - {parse_err}"))
        })
    }
}

pub trait TonicServerOptionExt<T> {
    fn argument_must_exist(self, argument: &str) -> Result<T, tonic::Status>;

    fn entity_must_exist(self) -> Result<T, tonic::Status>;

    fn must_exist(self, what: &'static str) -> Result<T, tonic::Status>;
}

impl<T> TonicServerOptionExt<T> for Option<T> {
    fn argument_must_exist(self, argument: &str) -> Result<T, tonic::Status> {
        self.ok_or_else(|| {
            tonic::Status::invalid_argument(format!("argument `{argument}` must exist"))
        })
    }

    fn entity_must_exist(self) -> Result<T, tonic::Status> {
        self.ok_or_else(|| tonic::Status::not_found("entity not found"))
    }

    fn must_exist(self, what: &'static str) -> Result<T, tonic::Status> {
        self.ok_or_else(|| tonic::Status::not_found(format!("{what} not found")))
    }
}

pub trait TonicServerResultExt<T> {
    fn to_status(self) -> Result<T, tonic::Status>;
}

impl<T> TonicServerResultExt<T> for Result<T, VersionParseError> {
    fn to_status(self) -> Result<T, tonic::Status> {
        self.map_err(|err| tonic::Status::invalid_argument(err.to_string()))
    }
}

impl<T> TonicServerResultExt<T> for Result<T, DbErrorWrite> {
    fn to_status(self) -> Result<T, tonic::Status> {
        self.map_err(|err| db_error_write_to_status(&err))
    }
}

impl<T> TonicServerResultExt<T> for Result<T, DbErrorRead> {
    fn to_status(self) -> Result<T, tonic::Status> {
        self.map_err(|err| db_error_read_to_status(&err))
    }
}
impl<T> TonicServerResultExt<T> for Result<T, DbErrorGeneric> {
    fn to_status(self) -> Result<T, tonic::Status> {
        self.map_err(|db_err| {
            warn!("Got db error {db_err:?}");
            tonic::Status::internal("database error".to_string())
        })
    }
}

pub fn db_error_write_to_status(db_err: &DbErrorWrite) -> tonic::Status {
    match db_err {
        DbErrorWrite::NotFound => tonic::Status::not_found("entity not found"),
        DbErrorWrite::NonRetriable(
            DbErrorWriteNonRetriable::ValidationFailed(reason)
            | DbErrorWriteNonRetriable::IllegalState { reason, .. },
        ) => tonic::Status::invalid_argument(reason.to_string()),
        _ => {
            warn!("Got db error {db_err:?}");
            tonic::Status::internal("database error".to_string())
        }
    }
}

pub fn db_error_read_to_status(db_err: &DbErrorRead) -> tonic::Status {
    if *db_err == DbErrorRead::NotFound {
        tonic::Status::not_found("entity not found")
    } else {
        warn!("Got db error {db_err:?}");
        tonic::Status::internal("database error".to_string())
    }
}

impl TryFrom<grpc_gen::FunctionName> for FunctionFqn {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::FunctionName) -> Result<Self, Self::Error> {
        FunctionFqn::try_from_tuple(&value.interface_name, &value.function_name).map_err(
            |parse_err| {
                warn!("{parse_err:?}");
                tonic::Status::invalid_argument(format!(
                    "FunctionName cannot be parsed - {parse_err}"
                ))
            },
        )
    }
}

impl<T: Borrow<FunctionFqn>> From<T> for grpc_gen::FunctionName {
    fn from(ffqn: T) -> Self {
        let ffqn = ffqn.borrow();
        grpc_gen::FunctionName {
            interface_name: ffqn.ifc_fqn.to_string(),
            function_name: ffqn.function_name.to_string(),
        }
    }
}

impl From<ComponentType> for grpc_gen::ComponentType {
    fn from(value: ComponentType) -> Self {
        match value {
            ComponentType::Activity => grpc_gen::ComponentType::Activity,
            ComponentType::ActivityStub => grpc_gen::ComponentType::ActivityStub,
            ComponentType::Workflow => grpc_gen::ComponentType::Workflow,
            ComponentType::WebhookEndpoint => grpc_gen::ComponentType::WebhookEndpoint,
            ComponentType::Cron => grpc_gen::ComponentType::Cron,
        }
    }
}
impl TryFrom<grpc_gen::ComponentType> for ComponentType {
    type Error = tonic::Status;
    fn try_from(value: grpc_gen::ComponentType) -> Result<Self, Self::Error> {
        match value {
            grpc_gen::ComponentType::Unspecified => Err(tonic::Status::invalid_argument(
                "`ComponentType` must be specified",
            )),
            grpc_gen::ComponentType::Activity => Ok(ComponentType::Activity),
            grpc_gen::ComponentType::ActivityStub => Ok(ComponentType::ActivityStub),
            grpc_gen::ComponentType::Workflow => Ok(ComponentType::Workflow),
            grpc_gen::ComponentType::WebhookEndpoint => Ok(ComponentType::WebhookEndpoint),
            grpc_gen::ComponentType::Cron => Ok(ComponentType::Cron),
        }
    }
}

impl From<&ExecutionWithState> for grpc_gen::ExecutionStatus {
    fn from(execution_with_state: &ExecutionWithState) -> grpc_gen::ExecutionStatus {
        use grpc_gen::execution_status::{
            BlockedByJoinSet, Cancelling, Finished, Locked, Paused, PendingAt, Status,
        };
        grpc_gen::ExecutionStatus {
            component_digest: Some(grpc_gen::ContentDigest::from(
                &execution_with_state.component_digest,
            )),
            status: Some(match &execution_with_state.pending_state {
                PendingState::Locked(PendingStateLocked {
                    locked_by:
                        LockedBy {
                            executor_id: _,
                            run_id,
                        },
                    lock_expires_at,
                }) => Status::Locked(Locked {
                    run_id: Some((*run_id).into()),
                    lock_expires_at: Some((*lock_expires_at).into()),
                }),
                PendingState::PendingAt(PendingStatePendingAt {
                    scheduled_at,
                    last_lock: _,
                }) => Status::PendingAt(PendingAt {
                    scheduled_at: Some((*scheduled_at).into()),
                }),
                PendingState::BlockedByJoinSet(PendingStateBlockedByJoinSet {
                    join_set_id,
                    lock_expires_at,
                    closing,
                }) => Status::BlockedByJoinSet(BlockedByJoinSet {
                    join_set_id: Some(join_set_id.into()),
                    lock_expires_at: Some((*lock_expires_at).into()),
                    closing: *closing,
                }),
                PendingState::Finished(PendingStateFinished {
                    version: _,
                    finished_at,
                    result_kind,
                }) => Status::Finished(Finished {
                    finished_at: Some((*finished_at).into()),
                    result_kind: grpc_gen::ResultKind::from(*result_kind).into(),
                }),
                PendingState::Paused(..) => Status::Paused(Paused {}),
                PendingState::Cancelling(..) => Status::Cancelling(Cancelling {}),
            }),
        }
    }
}

impl From<ExecutionWithState> for grpc_gen::ExecutionSummary {
    fn from(value: ExecutionWithState) -> Self {
        grpc_gen::ExecutionSummary {
            current_status: Some((&value).into()),
            execution_id: Some(value.execution_id.into()),
            function_name: Some(value.ffqn.into()),
            created_at: Some(value.created_at.into()),
            first_scheduled_at: Some(value.first_scheduled_at.into()),
            component_digest: Some(value.component_digest.into()),
            deployment_id: Some(value.deployment_id.into()),
            component_type: grpc_gen::ComponentType::from(value.component_type).into(),
        }
    }
}

impl From<PendingStateFinishedResultKind> for grpc_gen::ResultKind {
    fn from(result_kind: PendingStateFinishedResultKind) -> Self {
        (&result_kind).into()
    }
}
impl From<&PendingStateFinishedResultKind> for grpc_gen::ResultKind {
    fn from(result_kind: &PendingStateFinishedResultKind) -> Self {
        match result_kind {
            PendingStateFinishedResultKind::Ok => grpc_gen::ResultKind {
                value: Some(result_kind::Value::Ok(result_kind::Ok {})),
            },
            PendingStateFinishedResultKind::Err(PendingStateFinishedError::ExecutionFailure(
                kind,
            )) => grpc_gen::ResultKind {
                value: Some(result_kind::Value::ExecutionFailureKind(
                    grpc_gen::ExecutionFailureKind::from(kind).into(),
                )),
            },
            PendingStateFinishedResultKind::Err(PendingStateFinishedError::Error) => {
                grpc_gen::ResultKind {
                    value: Some(result_kind::Value::Error(result_kind::Error {})),
                }
            }
        }
    }
}
#[derive(Debug)]
pub enum ResultKindConversionError {
    MissingValue,   // buggy producer
    UnknownVariant, // newer producer
}

impl TryFrom<grpc_gen::ResultKind> for PendingStateFinishedResultKind {
    type Error = ResultKindConversionError;

    fn try_from(result: grpc_gen::ResultKind) -> Result<Self, Self::Error> {
        use grpc_gen::result_kind::Value;

        match result.value {
            Some(Value::Ok(_)) => Ok(PendingStateFinishedResultKind::Ok),

            Some(Value::ExecutionFailureKind(kind)) => Ok(PendingStateFinishedResultKind::Err(
                PendingStateFinishedError::ExecutionFailure(
                    ExecutionFailureKind::try_from(
                        grpc_gen::ExecutionFailureKind::try_from(kind)
                            .map_err(|_| ResultKindConversionError::UnknownVariant)?,
                    )
                    .map_err(|_unspecified| ResultKindConversionError::MissingValue)?,
                ),
            )),

            Some(Value::Error(_)) => Ok(PendingStateFinishedResultKind::Err(
                PendingStateFinishedError::Error,
            )),

            None => Err(ResultKindConversionError::MissingValue),
        }
    }
}

impl From<ExecutionFailureKind> for grpc_gen::ExecutionFailureKind {
    fn from(value: ExecutionFailureKind) -> Self {
        (&value).into()
    }
}
impl From<&ExecutionFailureKind> for grpc_gen::ExecutionFailureKind {
    fn from(value: &ExecutionFailureKind) -> Self {
        match value {
            ExecutionFailureKind::TimedOut => grpc_gen::ExecutionFailureKind::TimedOut,
            ExecutionFailureKind::NondeterminismDetected => {
                grpc_gen::ExecutionFailureKind::NondeterminismDetected
            }
            ExecutionFailureKind::OutOfFuel => grpc_gen::ExecutionFailureKind::OutOfFuel,
            ExecutionFailureKind::Cancelled => grpc_gen::ExecutionFailureKind::Cancelled,
            ExecutionFailureKind::Uncategorized => grpc_gen::ExecutionFailureKind::Uncategorized,
        }
    }
}
impl TryFrom<grpc_gen::ExecutionFailureKind> for ExecutionFailureKind {
    type Error = ();
    fn try_from(value: grpc_gen::ExecutionFailureKind) -> Result<Self, Self::Error> {
        match value {
            grpc_gen::ExecutionFailureKind::Unspecified => Err(()),
            grpc_gen::ExecutionFailureKind::TimedOut => Ok(ExecutionFailureKind::TimedOut),

            grpc_gen::ExecutionFailureKind::NondeterminismDetected => {
                Ok(ExecutionFailureKind::NondeterminismDetected)
            }

            grpc_gen::ExecutionFailureKind::OutOfFuel => Ok(ExecutionFailureKind::OutOfFuel),

            grpc_gen::ExecutionFailureKind::Cancelled => Ok(ExecutionFailureKind::Cancelled),

            grpc_gen::ExecutionFailureKind::Uncategorized => {
                Ok(ExecutionFailureKind::Uncategorized)
            }
        }
    }
}

pub fn convert_length(l: u32) -> Result<u16, tonic::Status> {
    u16::try_from(l).map_err(|_| tonic::Status::invalid_argument("`length` must be an u8"))
}

impl TryFrom<grpc_gen::list_executions_request::Pagination> for ExecutionListPagination {
    type Error = tonic::Status;

    fn try_from(
        pagination: grpc_gen::list_executions_request::Pagination,
    ) -> Result<Self, Self::Error> {
        use grpc_gen::list_executions_request::Pagination as GPagination;
        use grpc_gen::list_executions_request::{
            Cursor as OuterCursor, NewerThan, OlderThan, cursor::Cursor as InnerCursor,
        };

        Ok(match pagination {
            GPagination::NewerThan(NewerThan {
                length,
                cursor,
                including_cursor,
            }) => match cursor {
                Some(OuterCursor {
                    cursor: Some(InnerCursor::ExecutionId(id)),
                }) => ExecutionListPagination::ExecutionId(Pagination::NewerThan {
                    length: convert_length(length)?,
                    cursor: Some(ExecutionId::try_from(id)?),
                    including_cursor,
                }),
                Some(OuterCursor {
                    cursor: Some(InnerCursor::CreatedAt(timestamp)),
                }) => ExecutionListPagination::CreatedBy(Pagination::NewerThan {
                    length: convert_length(length)?,
                    cursor: Some(timestamp.into()),
                    including_cursor,
                }),
                _ => ExecutionListPagination::CreatedBy(Pagination::NewerThan {
                    length: convert_length(length)?,
                    cursor: None,
                    including_cursor,
                }),
            },
            GPagination::OlderThan(OlderThan {
                length,
                cursor,
                including_cursor,
            }) => match cursor {
                Some(OuterCursor {
                    cursor: Some(InnerCursor::ExecutionId(id)),
                }) => ExecutionListPagination::ExecutionId(Pagination::OlderThan {
                    length: convert_length(length)?,
                    cursor: Some(ExecutionId::try_from(id)?),
                    including_cursor,
                }),
                Some(OuterCursor {
                    cursor: Some(InnerCursor::CreatedAt(timestamp)),
                }) => ExecutionListPagination::CreatedBy(Pagination::OlderThan {
                    length: convert_length(length)?,
                    cursor: Some(timestamp.into()),
                    including_cursor,
                }),
                _ => ExecutionListPagination::CreatedBy(Pagination::OlderThan {
                    length: convert_length(length)?,
                    cursor: None,
                    including_cursor,
                }),
            },
        })
    }
}

pub fn to_any<T: serde::Serialize>(
    serializable: T,
    uri: String,
) -> Result<prost_wkt_types::Any, serde_json::Error> {
    Ok(prost_wkt_types::Any {
        type_url: uri,
        value: serde_json::to_string(&serializable)?.into_bytes(),
    })
}

fn from_any<T: DeserializeOwned>(
    any: &prost_wkt_types::Any,
    what: &'static str,
) -> Result<T, tonic::Status> {
    serde_json::from_slice(&any.value).map_err(|err| {
        tonic::Status::invalid_argument(format!("cannot decode `{what}` from Any payload - {err}"))
    })
}

fn metadata_to_grpc_map(metadata: &ExecutionMetadata) -> HashMap<String, String> {
    serde_json::from_value(
        serde_json::to_value(metadata).expect("ExecutionMetadata must serialize"),
    )
    .unwrap_or_default()
}

fn metadata_from_grpc_map(
    metadata: HashMap<String, String>,
) -> Result<ExecutionMetadata, tonic::Status> {
    serde_json::from_value(serde_json::to_value(metadata).expect("HashMap must serialize"))
        .map_err(|err| tonic::Status::invalid_argument(format!("cannot decode `metadata` - {err}")))
}

impl From<SupportedFunctionReturnValue> for grpc_gen::SupportedFunctionResult {
    fn from(finished_result: SupportedFunctionReturnValue) -> Self {
        let (value, wit_type_inline) = match finished_result {
            SupportedFunctionReturnValue::Ok(val_with_type) => {
                let wit_type_inline = val_with_type
                    .as_ref()
                    .map(|val_with_type| val_with_type.r#type.to_string());
                (
                    grpc_gen::supported_function_result::Value::Ok(
                        grpc_gen::supported_function_result::OkPayload {
                            return_value: val_with_type
                                .map(|val_with_type| {
                                    to_any(
                                        val_with_type.value,
                                        "urn:obelisk:json:retval:TBD".to_string(),
                                    )
                                })
                                .transpose()
                                .expect("SupportedFunctionReturnValue must be JSON-serializable"),
                        },
                    ),
                    wit_type_inline,
                )
            }
            SupportedFunctionReturnValue::Err(val_with_type) => {
                let wit_type_inline = val_with_type
                    .as_ref()
                    .map(|val_with_type| val_with_type.r#type.to_string());
                (
                    grpc_gen::supported_function_result::Value::Error(
                        grpc_gen::supported_function_result::ErrorPayload {
                            return_value: val_with_type
                                .map(|val_with_type| {
                                    to_any(
                                        val_with_type.value,
                                        "urn:obelisk:json:retval:TBD".to_string(),
                                    )
                                })
                                .transpose()
                                .expect("SupportedFunctionReturnValue must be JSON-serializable"),
                        },
                    ),
                    wit_type_inline,
                )
            }
            SupportedFunctionReturnValue::ExecutionFailure(FinishedExecutionFailure {
                kind,
                reason,
                detail,
            }) => (
                grpc_gen::supported_function_result::Value::ExecutionFailure(
                    grpc_gen::supported_function_result::ExecutionFailure {
                        reason,
                        detail,
                        kind: grpc_gen::ExecutionFailureKind::from(kind).into(),
                    },
                ),
                None,
            ),
        };
        grpc_gen::SupportedFunctionResult {
            value: Some(value),
            wit_type_inline,
        }
    }
}

fn schedule_at_to_grpc(
    scheduled_at: HistoryEventScheduleAt,
) -> grpc_gen::execution_event::history_event::schedule::ScheduledAt {
    grpc_gen::execution_event::history_event::schedule::ScheduledAt {
        variant: match scheduled_at {
            HistoryEventScheduleAt::Now => Some(
                grpc_gen::execution_event::history_event::schedule::scheduled_at::Variant::Now(
                    grpc_gen::execution_event::history_event::schedule::scheduled_at::Now {},
                ),
            ),
            HistoryEventScheduleAt::At(at) => Some(
                grpc_gen::execution_event::history_event::schedule::scheduled_at::Variant::At(
                    grpc_gen::execution_event::history_event::schedule::scheduled_at::At {
                        at: Some(prost_wkt_types::Timestamp::from(at)),
                    },
                ),
            ),
            HistoryEventScheduleAt::In(r#in) => Some(
                grpc_gen::execution_event::history_event::schedule::scheduled_at::Variant::In(
                    grpc_gen::execution_event::history_event::schedule::scheduled_at::In {
                        r#in: prost_wkt_types::Duration::try_from(r#in).ok(),
                    },
                ),
            ),
        },
    }
}

fn delay_schedule_at_to_grpc(
    scheduled_at: HistoryEventScheduleAt,
) -> grpc_gen::execution_event::history_event::join_set_request::delay_request::ScheduledAt {
    grpc_gen::execution_event::history_event::join_set_request::delay_request::ScheduledAt {
        variant: match scheduled_at {
            HistoryEventScheduleAt::Now => Some(
                grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::Variant::Now(
                    grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::Now {},
                ),
            ),
            HistoryEventScheduleAt::At(at) => Some(
                grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::Variant::At(
                    grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::At {
                        at: Some(prost_wkt_types::Timestamp::from(at)),
                    },
                ),
            ),
            HistoryEventScheduleAt::In(r#in) => Some(
                grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::Variant::In(
                    grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::In {
                        r#in: prost_wkt_types::Duration::try_from(r#in).ok(),
                    },
                ),
            ),
        },
    }
}

fn schedule_at_from_grpc(
    scheduled_at: grpc_gen::execution_event::history_event::schedule::ScheduledAt,
) -> Result<HistoryEventScheduleAt, tonic::Status> {
    match scheduled_at
        .variant
        .argument_must_exist("scheduled_at.variant")?
    {
        grpc_gen::execution_event::history_event::schedule::scheduled_at::Variant::Now(_) => {
            Ok(HistoryEventScheduleAt::Now)
        }
        grpc_gen::execution_event::history_event::schedule::scheduled_at::Variant::At(at) => Ok(
            HistoryEventScheduleAt::At(at.at.argument_must_exist("scheduled_at.at")?.into()),
        ),
        grpc_gen::execution_event::history_event::schedule::scheduled_at::Variant::In(r#in) => {
            let duration = r#in
                .r#in
                .argument_must_exist("scheduled_at.in")?
                .try_into()
                .map_err(|err| {
                    tonic::Status::invalid_argument(format!(
                        "scheduled_at.in cannot be parsed - {err}"
                    ))
                })?;
            Ok(HistoryEventScheduleAt::In(duration))
        }
    }
}

fn delay_schedule_at_from_grpc(
    scheduled_at: grpc_gen::execution_event::history_event::join_set_request::delay_request::ScheduledAt,
) -> Result<HistoryEventScheduleAt, tonic::Status> {
    match scheduled_at.variant.argument_must_exist("scheduled_at.variant")? {
        grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::Variant::Now(_) => {
            Ok(HistoryEventScheduleAt::Now)
        }
        grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::Variant::At(at) => Ok(
            HistoryEventScheduleAt::At(at.at.argument_must_exist("scheduled_at.at")?.into()),
        ),
        grpc_gen::execution_event::history_event::join_set_request::delay_request::scheduled_at::Variant::In(r#in) => {
            let duration = r#in
                .r#in
                .argument_must_exist("scheduled_at.in")?
                .try_into()
                .map_err(|err| {
                    tonic::Status::invalid_argument(format!(
                        "scheduled_at.in cannot be parsed - {err}"
                    ))
                })?;
            Ok(HistoryEventScheduleAt::In(duration))
        }
    }
}

fn http_client_trace_from_grpc(
    value: grpc_gen::HttpClientTrace,
) -> Result<HttpClientTrace, tonic::Status> {
    let finished_at = value.finished_at.map(Into::into);
    let status = match value.result {
        Some(grpc_gen::http_client_trace::Result::Status(status)) => {
            Some(Ok(u16::try_from(status).map_err(|_| {
                tonic::Status::invalid_argument("http status out of range")
            })?))
        }
        Some(grpc_gen::http_client_trace::Result::Error(err)) => Some(Err(err)),
        None => None,
    };
    let resp = match (finished_at, status) {
        (Some(finished_at), Some(status)) => {
            Some(concepts::storage::http_client_trace::ResponseTrace {
                finished_at,
                status,
            })
        }
        (None, None) => None,
        _ => {
            return Err(tonic::Status::invalid_argument(
                "http client trace must have both finished_at and result, or neither",
            ));
        }
    };
    Ok(HttpClientTrace {
        req: concepts::storage::http_client_trace::RequestTrace {
            sent_at: value.sent_at.argument_must_exist("sent_at")?.into(),
            uri: value.uri,
            method: value.method,
        },
        resp,
    })
}

impl TryFrom<grpc_gen::SupportedFunctionResult> for SupportedFunctionReturnValue {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::SupportedFunctionResult) -> Result<Self, Self::Error> {
        match value.value {
            Some(grpc_gen::supported_function_result::Value::Ok(
                grpc_gen::supported_function_result::OkPayload { return_value: None },
            )) => Ok(SupportedFunctionReturnValue::Ok(None)),
            Some(grpc_gen::supported_function_result::Value::Error(
                grpc_gen::supported_function_result::ErrorPayload { return_value: None },
            )) => Ok(SupportedFunctionReturnValue::Err(None)),
            Some(grpc_gen::supported_function_result::Value::ExecutionFailure(
                grpc_gen::supported_function_result::ExecutionFailure {
                    kind,
                    reason,
                    detail,
                },
            )) => {
                let kind = grpc_gen::ExecutionFailureKind::try_from(kind)
                    .map_err(|_| tonic::Status::invalid_argument("invalid execution failure kind"))
                    .and_then(|kind| {
                        ExecutionFailureKind::try_from(kind).map_err(|()| {
                            tonic::Status::invalid_argument("invalid execution failure kind")
                        })
                    })?;
                Ok(SupportedFunctionReturnValue::ExecutionFailure(
                    FinishedExecutionFailure {
                        kind,
                        reason,
                        detail,
                    },
                ))
            }
            Some(grpc_gen::supported_function_result::Value::Ok(
                grpc_gen::supported_function_result::OkPayload {
                    return_value: Some(return_value),
                },
            )) => {
                let wit_type_inline = value.wit_type_inline.ok_or_else(|| {
                    tonic::Status::invalid_argument(
                        "missing `wit_type_inline` for non-empty ok SupportedFunctionResult",
                    )
                })?;
                let wit_type = parse_wit_type(&wit_type_inline).map_err(|err| {
                    tonic::Status::invalid_argument(format!(
                        "cannot parse `wit_type_inline` for ok SupportedFunctionResult - {err}"
                    ))
                })?;
                let value = deserialize_slice(&return_value.value, wit_type).map_err(|err| {
                    tonic::Status::invalid_argument(format!(
                        "cannot decode ok return_value using `wit_type_inline` - {err}"
                    ))
                })?;
                Ok(SupportedFunctionReturnValue::Ok(Some(value)))
            }
            Some(grpc_gen::supported_function_result::Value::Error(
                grpc_gen::supported_function_result::ErrorPayload {
                    return_value: Some(return_value),
                },
            )) => {
                let wit_type_inline = value.wit_type_inline.ok_or_else(|| {
                    tonic::Status::invalid_argument(
                        "missing `wit_type_inline` for non-empty error SupportedFunctionResult",
                    )
                })?;
                let wit_type = parse_wit_type(&wit_type_inline).map_err(|err| {
                    tonic::Status::invalid_argument(format!(
                        "cannot parse `wit_type_inline` for error SupportedFunctionResult - {err}"
                    ))
                })?;
                let value = deserialize_slice(&return_value.value, wit_type).map_err(|err| {
                    tonic::Status::invalid_argument(format!(
                        "cannot decode error return_value using `wit_type_inline` - {err}"
                    ))
                })?;
                Ok(SupportedFunctionReturnValue::Err(Some(value)))
            }
            None => Err(tonic::Status::invalid_argument(
                "`SupportedFunctionResult.value` must exist",
            )),
        }
    }
}

pub fn from_execution_event_to_grpc(event: ExecutionEvent) -> grpc_gen::ExecutionEvent {
    grpc_gen::ExecutionEvent {
        created_at: Some(prost_wkt_types::Timestamp::from(event.created_at)),
        version: event.version.0,
        backtrace_id: event.backtrace_id.map(|v| v.0),
        event: Some(match event.event {
            ExecutionRequest::Created {
                ffqn,
                params,
                parent,
                scheduled_at,
                component_id,
                deployment_id,
                metadata,
                scheduled_by,
            } => grpc_gen::execution_event::Event::Created(grpc_gen::execution_event::Created {
                params: Some(
                    to_any(params, format!("urn:obelisk:json:params:{ffqn}"))
                        .expect("Params must be JSON-serializable"),
                ),
                function_name: Some(grpc_gen::FunctionName::from(ffqn)),
                scheduled_at: Some(prost_wkt_types::Timestamp::from(scheduled_at)),
                component_id: Some(component_id.into()),
                deployment_id: Some(deployment_id.into()),
                parent_execution_id: parent
                    .as_ref()
                    .map(|(id, _)| grpc_gen::ExecutionId { id: id.to_string() }),
                parent_join_set_id: parent.map(|(_, js)| js.into()),
                metadata: metadata_to_grpc_map(&metadata),
                scheduled_by: scheduled_by.map(|id| grpc_gen::ExecutionId { id: id.to_string() }),
            }),
            ExecutionRequest::Locked(Locked {
                component_id,
                deployment_id,
                executor_id,
                run_id,
                lock_expires_at,
                retry_config,
            }) => grpc_gen::execution_event::Event::Locked(grpc_gen::execution_event::Locked {
                component_id: Some(component_id.into()),
                deployment_id: Some(deployment_id.into()),
                run_id: run_id.to_string(),
                executor_id: executor_id.to_string(),
                retry_config: Some(retry_config.into()),
                lock_expires_at: Some(prost_wkt_types::Timestamp::from(lock_expires_at)),
            }),
            ExecutionRequest::Unlocked(unlocked) => grpc_gen::execution_event::Event::Unlocked(
                grpc_gen::execution_event::Unlocked {
                    reason: unlocked.reason.to_string(),
                    backoff_expires_at: Some(prost_wkt_types::Timestamp::from(
                        unlocked.unlocked_at,
                    )),
                },
            ),
            ExecutionRequest::ComponentUpgradeFinished {
                component_digest,
                deployment_id,
                outcome,
            } => grpc_gen::execution_event::Event::ComponentUpgradeFinished(
                grpc_gen::execution_event::ComponentUpgradeFinished {
                    component_digest: Some(component_digest.into()),
                    deployment_id: Some(deployment_id.into()),
                    outcome: Some(match outcome {
                        ComponentUpgradeOutcome::Success { reason } => {
                            grpc_gen::execution_event::component_upgrade_finished::Outcome::Success(
                                grpc_gen::execution_event::component_upgrade_finished::Success {
                                    reason: Some(match reason {
                                        ComponentUpgradeReason::Auto => {
                                            grpc_gen::execution_event::component_upgrade_finished::success::Reason::Auto(
                                                grpc_gen::execution_event::component_upgrade_finished::Auto {},
                                            )
                                        }
                                        ComponentUpgradeReason::Manual { force } => {
                                            grpc_gen::execution_event::component_upgrade_finished::success::Reason::Manual(
                                                grpc_gen::execution_event::component_upgrade_finished::Manual { force },
                                            )
                                        }
                                    }),
                                },
                            )
                        }
                        ComponentUpgradeOutcome::Failed { reason } => {
                            grpc_gen::execution_event::component_upgrade_finished::Outcome::Failed(
                                grpc_gen::execution_event::component_upgrade_finished::Failed {
                                    reason: reason.to_string(),
                                },
                            )
                        }
                    }),
                },
            ),
            ExecutionRequest::TemporarilyFailed {
                backoff_expires_at,
                reason,
                detail,
                http_client_traces,
            } => grpc_gen::execution_event::Event::TemporarilyFailed(
                grpc_gen::execution_event::TemporarilyFailed {
                    reason: reason.to_string(),
                    detail,
                    backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                    http_client_traces: http_client_traces
                        .unwrap_or_default()
                        .into_iter()
                        .map(grpc_gen::HttpClientTrace::from)
                        .collect(),
                },
            ),
            ExecutionRequest::TemporarilyTimedOut {
                backoff_expires_at,
                http_client_traces,
            } => grpc_gen::execution_event::Event::TemporarilyTimedOut(
                grpc_gen::execution_event::TemporarilyTimedOut {
                    backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                    http_client_traces: http_client_traces
                        .unwrap_or_default()
                        .into_iter()
                        .map(grpc_gen::HttpClientTrace::from)
                        .collect(),
                },
            ),
            ExecutionRequest::Finished {
                retval: result,
                http_client_traces,
            } => grpc_gen::execution_event::Event::Finished(grpc_gen::execution_event::Finished {
                value: Some(grpc_gen::SupportedFunctionResult::from(result)),
                http_client_traces: http_client_traces
                    .unwrap_or_default()
                    .into_iter()
                    .map(grpc_gen::HttpClientTrace::from)
                    .collect(),
            }),
            ExecutionRequest::Paused => {
                grpc_gen::execution_event::Event::Paused(grpc_gen::execution_event::Paused {})
            }
            ExecutionRequest::Unpaused => {
                grpc_gen::execution_event::Event::Unpaused(grpc_gen::execution_event::Unpaused {})
            }
            ExecutionRequest::CancellationRequested => {
                grpc_gen::execution_event::Event::CancellationRequested(
                    grpc_gen::execution_event::CancellationRequested {},
                )
            }
            ExecutionRequest::HistoryEvent { event } => {
                grpc_gen::execution_event::Event::HistoryVariant(history_event_to_grpc(event))
            }
        }),
    }
}

impl TryFrom<grpc_gen::ExecutionEvent> for ExecutionEvent {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::ExecutionEvent) -> Result<Self, Self::Error> {
        let event = match value.event.argument_must_exist("event")? {
            grpc_gen::execution_event::Event::Created(created) => {
                let ffqn: FunctionFqn = created
                    .function_name
                    .argument_must_exist("function_name")?
                    .try_into()?;
                let params = from_any(&created.params.argument_must_exist("params")?, "Params")?;
                let parent = match (created.parent_execution_id, created.parent_join_set_id) {
                    (Some(parent_execution_id), Some(parent_join_set_id)) => Some((
                        parent_execution_id.try_into()?,
                        parent_join_set_id.try_into()?,
                    )),
                    (None, None) => None,
                    _ => {
                        return Err(tonic::Status::invalid_argument(
                            "`parent_execution_id` and `parent_join_set_id` must either both exist or both be absent",
                        ));
                    }
                };
                ExecutionRequest::Created {
                    ffqn,
                    params,
                    parent,
                    scheduled_at: created
                        .scheduled_at
                        .argument_must_exist("scheduled_at")?
                        .into(),
                    component_id: created
                        .component_id
                        .argument_must_exist("component_id")?
                        .try_into()?,
                    deployment_id: created
                        .deployment_id
                        .argument_must_exist("deployment_id")?
                        .try_into()?,
                    metadata: metadata_from_grpc_map(created.metadata)?,
                    scheduled_by: created.scheduled_by.map(TryInto::try_into).transpose()?,
                }
            }
            grpc_gen::execution_event::Event::Locked(locked) => ExecutionRequest::Locked(Locked {
                component_id: locked
                    .component_id
                    .argument_must_exist("component_id")?
                    .try_into()?,
                executor_id: ExecutorId::from_str(&locked.executor_id).map_err(|err| {
                    tonic::Status::invalid_argument(format!("executor_id cannot be parsed - {err}"))
                })?,
                deployment_id: locked
                    .deployment_id
                    .argument_must_exist("deployment_id")?
                    .try_into()?,
                run_id: RunId::from_str(&locked.run_id).map_err(|err| {
                    tonic::Status::invalid_argument(format!("run_id cannot be parsed - {err}"))
                })?,
                lock_expires_at: locked
                    .lock_expires_at
                    .argument_must_exist("lock_expires_at")?
                    .into(),
                retry_config: locked
                    .retry_config
                    .argument_must_exist("retry_config")?
                    .try_into()?,
            }),
            grpc_gen::execution_event::Event::Unlocked(unlocked) => {
                ExecutionRequest::Unlocked(Unlocked {
                    unlocked_at: unlocked
                        .backoff_expires_at
                        .argument_must_exist("backoff_expires_at")?
                        .into(),
                    reason: StrVariant::from(unlocked.reason),
                })
            }
            grpc_gen::execution_event::Event::ComponentUpgradeFinished(upgraded) => {
                ExecutionRequest::ComponentUpgradeFinished {
                    component_digest: upgraded
                        .component_digest
                        .argument_must_exist("component_digest")?
                        .try_into()?,
                    deployment_id: upgraded
                        .deployment_id
                        .argument_must_exist("deployment_id")?
                        .try_into()?,
                    outcome: match upgraded.outcome.argument_must_exist("outcome")? {
                        grpc_gen::execution_event::component_upgrade_finished::Outcome::Success(success) => {
                            ComponentUpgradeOutcome::Success {
                                reason: match success.reason.argument_must_exist("reason")? {
                                    grpc_gen::execution_event::component_upgrade_finished::success::Reason::Auto(_) => {
                                        ComponentUpgradeReason::Auto
                                    }
                                    grpc_gen::execution_event::component_upgrade_finished::success::Reason::Manual(manual) => {
                                        ComponentUpgradeReason::Manual { force: manual.force }
                                    }
                                },
                            }
                        }
                        grpc_gen::execution_event::component_upgrade_finished::Outcome::Failed(failed) => {
                            ComponentUpgradeOutcome::Failed {
                                reason: StrVariant::from(failed.reason),
                            }
                        }
                    },
                }
            }
            grpc_gen::execution_event::Event::TemporarilyFailed(failed) => {
                ExecutionRequest::TemporarilyFailed {
                    backoff_expires_at: failed
                        .backoff_expires_at
                        .argument_must_exist("backoff_expires_at")?
                        .into(),
                    reason: StrVariant::from(failed.reason),
                    detail: failed.detail,
                    http_client_traces: optional_http_client_traces_from_grpc(
                        failed.http_client_traces,
                    )?,
                }
            }
            grpc_gen::execution_event::Event::TemporarilyTimedOut(timed_out) => {
                ExecutionRequest::TemporarilyTimedOut {
                    backoff_expires_at: timed_out
                        .backoff_expires_at
                        .argument_must_exist("backoff_expires_at")?
                        .into(),
                    http_client_traces: optional_http_client_traces_from_grpc(
                        timed_out.http_client_traces,
                    )?,
                }
            }
            grpc_gen::execution_event::Event::Finished(finished) => ExecutionRequest::Finished {
                retval: finished.value.argument_must_exist("value")?.try_into()?,
                http_client_traces: optional_http_client_traces_from_grpc(
                    finished.http_client_traces,
                )?,
            },
            grpc_gen::execution_event::Event::HistoryVariant(event) => {
                ExecutionRequest::HistoryEvent {
                    event: history_event_from_grpc(event)?,
                }
            }
            grpc_gen::execution_event::Event::Paused(_) => ExecutionRequest::Paused,
            grpc_gen::execution_event::Event::Unpaused(_) => ExecutionRequest::Unpaused,
            grpc_gen::execution_event::Event::CancellationRequested(_) => {
                ExecutionRequest::CancellationRequested
            }
        };

        Ok(ExecutionEvent {
            created_at: value.created_at.argument_must_exist("created_at")?.into(),
            event,
            backtrace_id: value.backtrace_id.map(Version::new),
            version: Version::new(value.version),
        })
    }
}

fn history_event_from_grpc(
    value: grpc_gen::execution_event::HistoryEvent,
) -> Result<HistoryEvent, tonic::Status> {
    Ok(match value.event.argument_must_exist("event")? {
        history_event::Event::Persist(persist) => {
            let kind = match persist.kind.argument_must_exist("kind")?.variant.argument_must_exist("kind.variant")? {
                history_event::persist::persist_kind::Variant::RandomString(random) => {
                    PersistKind::RandomString {
                        min_length: random.min_length,
                        max_length_exclusive: random.max_length_exclusive,
                    }
                }
                history_event::persist::persist_kind::Variant::RandomU64(random) => {
                    PersistKind::RandomU64 {
                        min: random.min,
                        max_inclusive: random.max_inclusive,
                    }
                }
                history_event::persist::persist_kind::Variant::ExecutionId(_) => {
                    PersistKind::ExecutionId
                }
            };
            HistoryEvent::Persist {
                value: persist.data.argument_must_exist("data")?.value,
                kind,
            }
        }
        history_event::Event::JoinSetCreated(created) => HistoryEvent::JoinSetCreate {
            join_set_id: created.join_set_id.argument_must_exist("join_set_id")?.try_into()?,
        },
        history_event::Event::JoinSetRequest(req) => HistoryEvent::JoinSetRequest {
            join_set_id: req.join_set_id.argument_must_exist("join_set_id")?.try_into()?,
            request: match req.join_set_request.argument_must_exist("join_set_request")? {
                history_event::join_set_request::JoinSetRequest::DelayRequest(delay) => {
                    JoinSetRequest::DelayRequest {
                        delay_id: delay.delay_id.argument_must_exist("delay_id")?.try_into()?,
                        expires_at: delay.expires_at.argument_must_exist("expires_at")?.into(),
                        schedule_at: delay_schedule_at_from_grpc(
                            delay.scheduled_at.argument_must_exist("scheduled_at")?,
                        )?,
                        paused: delay.paused,
                    }
                }
                history_event::join_set_request::JoinSetRequest::ChildExecutionRequest(child) => {
                    let child_execution_id = match child
                        .child_execution_id
                        .argument_must_exist("child_execution_id")?
                        .try_into()?
                    {
                        ExecutionId::Derived(derived) => derived,
                        ExecutionId::TopLevel(_) => {
                            return Err(tonic::Status::invalid_argument(
                                "child_execution_id must be a derived execution id",
                            ))
                        }
                    };
                    JoinSetRequest::ChildExecutionRequest {
                        child_execution_id,
                        target_ffqn: child
                            .function_name
                            .argument_must_exist("function_name")?
                            .try_into()?,
                        params: from_any(&child.params.argument_must_exist("params")?, "Params")?,
                        result: match child.result.argument_must_exist("result")? {
                            grpc_gen::execution_event::history_event::join_set_request::child_execution_request::Result::Ok(_) => Ok(()),
                            grpc_gen::execution_event::history_event::join_set_request::child_execution_request::Result::Error(err) => Err(match grpc_gen::execution_event::history_event::join_set_request::child_execution_request::error::Kind::try_from(err.kind).map_err(|_| tonic::Status::invalid_argument("invalid child_execution_request.error.kind"))? {
                                grpc_gen::execution_event::history_event::join_set_request::child_execution_request::error::Kind::FunctionNotFound => ChildExecutionRequestError::FunctionNotFound,
                                grpc_gen::execution_event::history_event::join_set_request::child_execution_request::error::Kind::TypeCheckError => ChildExecutionRequestError::TypeCheckError(
                                    err.detail.ok_or_else(|| tonic::Status::invalid_argument("missing child_execution_request.error.detail"))?,
                                ),
                                grpc_gen::execution_event::history_event::join_set_request::child_execution_request::error::Kind::Unspecified => {
                                    return Err(tonic::Status::invalid_argument("invalid child_execution_request.error.kind"))
                                }
                            }),
                        },
                    }
                }
            },
        },
        history_event::Event::JoinNext(join_next) => HistoryEvent::JoinNext {
            join_set_id: join_next.join_set_id.argument_must_exist("join_set_id")?.try_into()?,
            run_expires_at: join_next
                .run_expires_at
                .argument_must_exist("run_expires_at")?
                .into(),
            requested_ffqn: join_next.function.map(TryInto::try_into).transpose()?,
            closing: join_next.closing,
        },
        history_event::Event::JoinNextTooMany(too_many) => HistoryEvent::JoinNextTooMany {
            join_set_id: too_many.join_set_id.argument_must_exist("join_set_id")?.try_into()?,
            requested_ffqn: too_many.function.map(TryInto::try_into).transpose()?,
        },
        history_event::Event::JoinNextTry(join_next_try) => HistoryEvent::JoinNextTry {
            join_set_id: join_next_try.join_set_id.argument_must_exist("join_set_id")?.try_into()?,
            outcome: match grpc_gen::execution_event::history_event::join_next_try::Outcome::try_from(join_next_try.outcome)
                .map_err(|_| tonic::Status::invalid_argument("invalid join_next_try.outcome"))?
            {
                grpc_gen::execution_event::history_event::join_next_try::Outcome::Found => {
                    concepts::storage::JoinNextTryOutcome::Found
                }
                grpc_gen::execution_event::history_event::join_next_try::Outcome::Pending => {
                    concepts::storage::JoinNextTryOutcome::Pending
                }
                grpc_gen::execution_event::history_event::join_next_try::Outcome::AllProcessed => {
                    concepts::storage::JoinNextTryOutcome::AllProcessed
                }
            },
        },
        history_event::Event::Schedule(schedule) => HistoryEvent::Schedule {
            execution_id: schedule.execution_id.argument_must_exist("execution_id")?.try_into()?,
            schedule_at: schedule_at_from_grpc(
                schedule.scheduled_at.argument_must_exist("scheduled_at")?,
            )?,
            result: match schedule.result.argument_must_exist("result")? {
                grpc_gen::execution_event::history_event::schedule::Result::Ok(_) => Ok(()),
                grpc_gen::execution_event::history_event::schedule::Result::Error(err) => Err(
                    match grpc_gen::execution_event::history_event::schedule::error::Kind::try_from(err.kind)
                        .map_err(|_| tonic::Status::invalid_argument("invalid schedule.error.kind"))?
                    {
                        grpc_gen::execution_event::history_event::schedule::error::Kind::FunctionNotFound => {
                            ScheduleRequestError::FunctionNotFound
                        }
                        grpc_gen::execution_event::history_event::schedule::error::Kind::TypeCheckError => {
                            ScheduleRequestError::TypeCheckError(
                                err.detail.ok_or_else(|| tonic::Status::invalid_argument("missing schedule.error.detail"))?,
                            )
                        }
                        grpc_gen::execution_event::history_event::schedule::error::Kind::Unspecified => {
                            return Err(tonic::Status::invalid_argument("invalid schedule.error.kind"))
                        }
                    },
                ),
            },
        },
        history_event::Event::Stub(stub) => HistoryEvent::Stub {
            target_execution_id: match stub
                .execution_id
                .argument_must_exist("execution_id")?
                .try_into()?
            {
                ExecutionId::Derived(derived) => derived,
                ExecutionId::TopLevel(_) => {
                    return Err(tonic::Status::invalid_argument(
                        "stub execution_id must be a derived execution id",
                    ))
                }
            },
            retval_hash: stub.retval_hash.parse().map_err(|err| {
                tonic::Status::invalid_argument(format!("invalid retval_hash - {err}"))
            })?,
            result: match stub.result.argument_must_exist("result")? {
                grpc_gen::execution_event::history_event::stub::Result::Ok(_) => Ok(()),
                grpc_gen::execution_event::history_event::stub::Result::Error(err) => Err(
                    match grpc_gen::execution_event::history_event::stub::error::Kind::try_from(err.kind)
                        .map_err(|_| tonic::Status::invalid_argument("invalid stub.error.kind"))?
                    {
                        grpc_gen::execution_event::history_event::stub::error::Kind::ExecutionNotFound => {
                            StubError::ExecutionNotFound
                        }
                        grpc_gen::execution_event::history_event::stub::error::Kind::TypeCheckError => {
                            StubError::TypeCheckError(
                                err.detail.ok_or_else(|| tonic::Status::invalid_argument("missing stub.error.detail"))?,
                            )
                        }
                        grpc_gen::execution_event::history_event::stub::error::Kind::Conflict => {
                            StubError::Conflict
                        }
                        grpc_gen::execution_event::history_event::stub::error::Kind::Unspecified => {
                            return Err(tonic::Status::invalid_argument("invalid stub.error.kind"))
                        }
                    },
                ),
            },
        },
    })
}

fn optional_http_client_traces_from_grpc(
    traces: Vec<grpc_gen::HttpClientTrace>,
) -> Result<Option<Vec<HttpClientTrace>>, tonic::Status> {
    if traces.is_empty() {
        Ok(None)
    } else {
        traces
            .into_iter()
            .map(http_client_trace_from_grpc)
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }
}

pub fn history_event_to_grpc(event: HistoryEvent) -> grpc_gen::execution_event::HistoryEvent {
    grpc_gen::execution_event::HistoryEvent {
        event: Some(match event {
            HistoryEvent::Persist { value, kind } => {
                history_event::Event::Persist(history_event::Persist {
                    data: Some(prost_wkt_types::Any {
                        type_url: "unknown".to_string(),
                        value,
                    }),
                    kind: Some(history_event::persist::PersistKind {
                        variant: Some(match kind {
                            PersistKind::RandomU64 { min, max_inclusive } => {
                                history_event::persist::persist_kind::Variant::RandomU64(
                                    history_event::persist::persist_kind::RandomU64 {
                                        min,
                                        max_inclusive,
                                    },
                                )
                            }
                            PersistKind::RandomString {
                                min_length,
                                max_length_exclusive,
                            } => {
                                history_event::persist::persist_kind::Variant::RandomString(
                                    history_event::persist::persist_kind::RandomString {
                                        min_length,
                                        max_length_exclusive,
                                    },
                                )
                            }
                            PersistKind::ExecutionId => {
                                history_event::persist::persist_kind::Variant::ExecutionId(
                                    history_event::persist::persist_kind::ExecutionId {},
                                )
                            }
                        }),
                    }),
                })
            }
            HistoryEvent::JoinSetCreate { join_set_id } => {
                history_event::Event::JoinSetCreated(history_event::JoinSetCreated {
                    join_set_id: Some(join_set_id.into()),
                })
            }
            HistoryEvent::JoinSetRequest {
                join_set_id,
                request,
            } => history_event::Event::JoinSetRequest(history_event::JoinSetRequest {
                join_set_id: Some(join_set_id.into()),
                join_set_request: match request {
                    JoinSetRequest::DelayRequest {
                        delay_id,
                        expires_at,
                        schedule_at,
                        paused,
                    } => Some(
                        history_event::join_set_request::JoinSetRequest::DelayRequest(
                            history_event::join_set_request::DelayRequest {
                                delay_id: Some(delay_id.into()),
                                expires_at: Some(prost_wkt_types::Timestamp::from(expires_at)),
                                scheduled_at: Some(delay_schedule_at_to_grpc(schedule_at)),
                                paused,
                            },
                        ),
                    ),
                    JoinSetRequest::ChildExecutionRequest {
                        child_execution_id,
                        params,
                        target_ffqn,
                        result,
                    } => Some(
                        history_event::join_set_request::JoinSetRequest::ChildExecutionRequest(
                            history_event::join_set_request::ChildExecutionRequest {
                                child_execution_id: Some(grpc_gen::ExecutionId {
                                    id: child_execution_id.to_string(),
                                }),
                                function_name: Some(grpc_gen::FunctionName::from(target_ffqn)),
                                params: Some(
                                    to_any(
                                        params,
                                        "urn:obelisk:json:params:child-execution-request"
                                            .to_string(),
                                    )
                                    .expect("Params must be JSON-serializable"),
                                ),
                                result: Some(match result {
                                    Ok(()) => grpc_gen::execution_event::history_event::join_set_request::child_execution_request::Result::Ok(
                                        grpc_gen::execution_event::history_event::join_set_request::child_execution_request::Ok {},
                                    ),
                                    Err(ChildExecutionRequestError::FunctionNotFound) => grpc_gen::execution_event::history_event::join_set_request::child_execution_request::Result::Error(
                                        grpc_gen::execution_event::history_event::join_set_request::child_execution_request::Error {
                                            kind: grpc_gen::execution_event::history_event::join_set_request::child_execution_request::error::Kind::FunctionNotFound.into(),
                                            detail: None,
                                        },
                                    ),
                                    Err(ChildExecutionRequestError::TypeCheckError(err)) => grpc_gen::execution_event::history_event::join_set_request::child_execution_request::Result::Error(
                                        grpc_gen::execution_event::history_event::join_set_request::child_execution_request::Error {
                                            kind: grpc_gen::execution_event::history_event::join_set_request::child_execution_request::error::Kind::TypeCheckError.into(),
                                            detail: Some(err),
                                        },
                                    ),
                                }),
                            },
                        ),
                    ),
                },
            }),
            HistoryEvent::JoinNext {
                join_set_id,
                run_expires_at,
                closing,
                requested_ffqn,
            } => history_event::Event::JoinNext(history_event::JoinNext {
                join_set_id: Some(join_set_id.into()),
                run_expires_at: Some(prost_wkt_types::Timestamp::from(run_expires_at)),
                closing,
                function: requested_ffqn.map(Into::into),
            }),
            HistoryEvent::JoinNextTooMany {
                join_set_id,
                requested_ffqn,
            } => history_event::Event::JoinNextTooMany(history_event::JoinNextTooMany {
                join_set_id: Some(join_set_id.into()),
                function: requested_ffqn.map(Into::into),
            }),
            HistoryEvent::JoinNextTry {
                join_set_id,
                outcome,
            } => {
                let outcome = match outcome {
                    concepts::storage::JoinNextTryOutcome::Found => {
                        history_event::join_next_try::Outcome::Found
                    }
                    concepts::storage::JoinNextTryOutcome::Pending => {
                        history_event::join_next_try::Outcome::Pending
                    }
                    concepts::storage::JoinNextTryOutcome::AllProcessed => {
                        history_event::join_next_try::Outcome::AllProcessed
                    }
                };
                history_event::Event::JoinNextTry(history_event::JoinNextTry {
                    join_set_id: Some(join_set_id.into()),
                    outcome: outcome.into(),
                })
            }
            HistoryEvent::Schedule {
                execution_id,
                schedule_at: scheduled_at,
                result,
            } => history_event::Event::Schedule(history_event::Schedule {
                execution_id: Some(grpc_gen::ExecutionId {
                    id: execution_id.to_string(),
                }),
                scheduled_at: Some(schedule_at_to_grpc(scheduled_at)),
                result: Some(match result {
                    Ok(()) => grpc_gen::execution_event::history_event::schedule::Result::Ok(
                        grpc_gen::execution_event::history_event::schedule::Ok {},
                    ),
                    Err(ScheduleRequestError::FunctionNotFound) => {
                        grpc_gen::execution_event::history_event::schedule::Result::Error(
                            grpc_gen::execution_event::history_event::schedule::Error {
                                kind: grpc_gen::execution_event::history_event::schedule::error::Kind::FunctionNotFound.into(),
                                detail: None,
                            },
                        )
                    }
                    Err(ScheduleRequestError::TypeCheckError(err)) => {
                        grpc_gen::execution_event::history_event::schedule::Result::Error(
                            grpc_gen::execution_event::history_event::schedule::Error {
                                kind: grpc_gen::execution_event::history_event::schedule::error::Kind::TypeCheckError.into(),
                                detail: Some(err),
                            },
                        )
                    }
                }),
            }),
            HistoryEvent::Stub {
                target_execution_id,
                result,
                retval_hash,
            } => history_event::Event::Stub(history_event::Stub {
                execution_id: Some(ExecutionId::Derived(target_execution_id).into()),
                retval_hash: retval_hash.to_string(),
                result: Some(match result {
                    Ok(()) => grpc_gen::execution_event::history_event::stub::Result::Ok(
                        grpc_gen::execution_event::history_event::stub::Ok {},
                    ),
                    Err(StubError::ExecutionNotFound) => {
                        grpc_gen::execution_event::history_event::stub::Result::Error(
                            grpc_gen::execution_event::history_event::stub::Error {
                                kind: grpc_gen::execution_event::history_event::stub::error::Kind::ExecutionNotFound.into(),
                                detail: None,
                            },
                        )
                    }
                    Err(StubError::TypeCheckError(err)) => {
                        grpc_gen::execution_event::history_event::stub::Result::Error(
                            grpc_gen::execution_event::history_event::stub::Error {
                                kind: grpc_gen::execution_event::history_event::stub::error::Kind::TypeCheckError.into(),
                                detail: Some(err),
                            },
                        )
                    }
                    Err(StubError::Conflict) => {
                        grpc_gen::execution_event::history_event::stub::Result::Error(
                            grpc_gen::execution_event::history_event::stub::Error {
                                kind: grpc_gen::execution_event::history_event::stub::error::Kind::Conflict.into(),
                                detail: None,
                            },
                        )
                    }
                }),
            }),
        }),
    }
}

pub mod response {
    use super::grpc_gen;
    use concepts::{
        ExecutionId,
        storage::{JoinSetResponse, JoinSetResponseEventOuter, ResponseWithCursor},
    };
    use prost_wkt_types::Timestamp;

    impl From<ResponseWithCursor> for grpc_gen::ResponseWithCursor {
        fn from(response: ResponseWithCursor) -> Self {
            Self {
                event: Some(response.event.into()),
                cursor: response.cursor.0,
            }
        }
    }

    impl From<JoinSetResponseEventOuter> for grpc_gen::JoinSetResponseEvent {
        fn from(event: JoinSetResponseEventOuter) -> Self {
            let response = match event.event.event {
                JoinSetResponse::DelayFinished { delay_id, result } => {
                    grpc_gen::join_set_response_event::Response::DelayFinished(
                        grpc_gen::join_set_response_event::DelayFinished {
                            delay_id: Some(delay_id.into()),
                            success: result.is_ok(),
                        },
                    )
                }
                JoinSetResponse::ChildExecutionFinished {
                    child_execution_id,
                    finished_version: _,
                    result,
                } => grpc_gen::join_set_response_event::Response::ChildExecutionFinished(
                    grpc_gen::join_set_response_event::ChildExecutionFinished {
                        child_execution_id: Some(ExecutionId::Derived(child_execution_id).into()),
                        value: Some(result.into()),
                    },
                ),
            };

            Self {
                created_at: Some(Timestamp::from(event.created_at)),
                join_set_id: Some(event.event.join_set_id.into()),
                response: Some(response),
            }
        }
    }
}

mod backtrace {
    use crate::grpc_gen;
    use crate::grpc_mapping::TonicServerOptionExt;
    use concepts::storage::{BacktraceInfo, WasmBacktrace};
    use concepts::{ComponentId, ExecutionId};

    impl From<concepts::storage::FrameInfo> for grpc_gen::FrameInfo {
        fn from(frame: concepts::storage::FrameInfo) -> Self {
            Self {
                module: frame.module,
                func_name: frame.func_name,
                symbols: frame.symbols.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl From<concepts::storage::FrameSymbol> for grpc_gen::FrameSymbol {
        fn from(symbol: concepts::storage::FrameSymbol) -> Self {
            Self {
                func_name: symbol.func_name,
                file: symbol.file,
                line: symbol.line,
                col: symbol.col,
            }
        }
    }

    impl From<WasmBacktrace> for grpc_gen::WasmBacktrace {
        fn from(backtrace: WasmBacktrace) -> Self {
            Self {
                version_min_including: 0,
                version_max_excluding: 0,
                frames: backtrace.frames.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl From<BacktraceInfo> for grpc_gen::CapturedBacktrace {
        fn from(backtrace: BacktraceInfo) -> Self {
            Self {
                execution_id: Some(backtrace.execution_id.into()),
                component_id: Some(backtrace.component_id.into()),
                wasm_backtrace: Some(grpc_gen::WasmBacktrace {
                    version_min_including: backtrace.version_min_including.0,
                    version_max_excluding: backtrace.version_max_excluding.0,
                    frames: backtrace
                        .wasm_backtrace
                        .frames
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                }),
            }
        }
    }

    impl TryFrom<grpc_gen::CapturedBacktrace> for BacktraceInfo {
        type Error = tonic::Status;

        fn try_from(value: grpc_gen::CapturedBacktrace) -> Result<Self, Self::Error> {
            let execution_id: ExecutionId = value
                .execution_id
                .argument_must_exist("execution_id")?
                .try_into()?;
            let component_id: ComponentId = value
                .component_id
                .argument_must_exist("component_id")?
                .try_into()?;
            let wasm_backtrace = value.wasm_backtrace.argument_must_exist("wasm_backtrace")?;
            Ok(BacktraceInfo {
                execution_id,
                component_id,
                version_min_including: concepts::storage::Version::new(
                    wasm_backtrace.version_min_including,
                ),
                version_max_excluding: concepts::storage::Version::new(
                    wasm_backtrace.version_max_excluding,
                ),
                wasm_backtrace: WasmBacktrace {
                    frames: wasm_backtrace
                        .frames
                        .into_iter()
                        .map(|frame| concepts::storage::FrameInfo {
                            module: frame.module,
                            func_name: frame.func_name,
                            symbols: frame
                                .symbols
                                .into_iter()
                                .map(|symbol| concepts::storage::FrameSymbol {
                                    func_name: symbol.func_name,
                                    file: symbol.file,
                                    line: symbol.line,
                                    col: symbol.col,
                                })
                                .collect(),
                        })
                        .collect(),
                },
            })
        }
    }
}

impl From<HttpClientTrace> for grpc_gen::HttpClientTrace {
    fn from(value: HttpClientTrace) -> Self {
        Self {
            sent_at: Some(value.req.sent_at.into()),
            uri: value.req.uri,
            method: value.req.method,
            finished_at: value.resp.as_ref().map(|resp| resp.finished_at.into()),
            result: value.resp.map(|resp| match resp.status {
                Ok(status) => grpc_gen::http_client_trace::Result::Status(u32::from(status)),
                Err(err) => grpc_gen::http_client_trace::Result::Error(err),
            }),
        }
    }
}

impl From<CancelOutcome> for grpc_gen::cancel_activity_response::CancelActivityOutcome {
    fn from(value: CancelOutcome) -> Self {
        match value {
            CancelOutcome::Cancelled => {
                grpc_gen::cancel_activity_response::CancelActivityOutcome::Cancelled
            }
            CancelOutcome::AlreadyFinished => {
                grpc_gen::cancel_activity_response::CancelActivityOutcome::AlreadyFinished
            }
            CancelOutcome::AlreadyCancelling => {
                unreachable!("cancel_activity never yields AlreadyCancelling")
            }
        }
    }
}

impl From<CancelOutcome> for grpc_gen::cancel_execution_response::CancelExecutionOutcome {
    fn from(value: CancelOutcome) -> Self {
        match value {
            CancelOutcome::Cancelled => {
                grpc_gen::cancel_execution_response::CancelExecutionOutcome::CancellationRequested
            }
            CancelOutcome::AlreadyFinished => {
                grpc_gen::cancel_execution_response::CancelExecutionOutcome::AlreadyFinished
            }
            CancelOutcome::AlreadyCancelling => {
                grpc_gen::cancel_execution_response::CancelExecutionOutcome::AlreadyCancelling
            }
        }
    }
}

impl From<CancelOutcome> for grpc_gen::cancel_delay_response::CancelDelayOutcome {
    fn from(value: CancelOutcome) -> Self {
        match value {
            CancelOutcome::Cancelled => {
                grpc_gen::cancel_delay_response::CancelDelayOutcome::Cancelled
            }
            CancelOutcome::AlreadyFinished => {
                grpc_gen::cancel_delay_response::CancelDelayOutcome::AlreadyFinished
            }
            CancelOutcome::AlreadyCancelling => {
                unreachable!("cancel_delay never yields AlreadyCancelling")
            }
        }
    }
}

impl TryFrom<grpc_gen::LogLevel> for LogLevel {
    type Error = ();

    fn try_from(
        value: grpc_gen::LogLevel,
    ) -> Result<Self, <Self as TryFrom<grpc_gen::LogLevel>>::Error> {
        match value {
            grpc_gen::LogLevel::Unspecified => Err(()),
            grpc_gen::LogLevel::Trace => Ok(LogLevel::Trace),
            grpc_gen::LogLevel::Debug => Ok(LogLevel::Debug),
            grpc_gen::LogLevel::Info => Ok(LogLevel::Info),
            grpc_gen::LogLevel::Warn => Ok(LogLevel::Warn),
            grpc_gen::LogLevel::Error => Ok(LogLevel::Error),
        }
    }
}

impl TryFrom<grpc_gen::LogStreamType> for LogStreamType {
    type Error = ();

    fn try_from(value: grpc_gen::LogStreamType) -> Result<Self, Self::Error> {
        match value {
            grpc_gen::LogStreamType::Unspecified => Err(()),
            grpc_gen::LogStreamType::Stdout => Ok(LogStreamType::StdOut),
            grpc_gen::LogStreamType::Stderr => Ok(LogStreamType::StdErr),
        }
    }
}

impl From<LogLevel> for grpc_gen::LogLevel {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Trace => grpc_gen::LogLevel::Trace,
            LogLevel::Debug => grpc_gen::LogLevel::Debug,
            LogLevel::Info => grpc_gen::LogLevel::Info,
            LogLevel::Warn => grpc_gen::LogLevel::Warn,
            LogLevel::Error => grpc_gen::LogLevel::Error,
        }
    }
}

impl From<LogStreamType> for grpc_gen::LogStreamType {
    fn from(value: LogStreamType) -> Self {
        match value {
            LogStreamType::StdOut => grpc_gen::LogStreamType::Stdout,
            LogStreamType::StdErr => grpc_gen::LogStreamType::Stderr,
        }
    }
}

impl From<LogEntryRow> for grpc_gen::list_logs_response::LogEntry {
    fn from(value: LogEntryRow) -> Self {
        use grpc_gen::list_logs_response::log_entry;

        let created_at = Some(value.log_entry.created_at().into());

        let entry = match value.log_entry {
            LogEntry::Log {
                created_at: _,
                level,
                message,
            } => Some(log_entry::Entry::Log(log_entry::LogVariant {
                level: grpc_gen::LogLevel::from(level).into(),
                message,
            })),
            LogEntry::Stream {
                created_at: _,
                payload,
                stream_type,
            } => Some(log_entry::Entry::Stream(log_entry::StreamVariant {
                payload,
                stream_type: grpc_gen::LogStreamType::from(stream_type).into(),
            })),
        };

        grpc_gen::list_logs_response::LogEntry {
            created_at,
            entry,
            run_id: Some(value.run_id.into()),
            execution_id: Some(value.execution_id.into()),
        }
    }
}

fn append_request_to_grpc_event(
    req: concepts::storage::AppendRequest,
    version: u32,
) -> grpc_gen::ExecutionEvent {
    from_execution_event_to_grpc(ExecutionEvent {
        created_at: req.created_at,
        event: req.event,
        backtrace_id: None,
        version: Version::new(version),
    })
}

fn append_requests_to_grpc_events(
    batch: Vec<concepts::storage::AppendRequest>,
    start_version: u32,
) -> Vec<grpc_gen::ExecutionEvent> {
    batch
        .into_iter()
        .enumerate()
        .map(|(i, req)| {
            append_request_to_grpc_event(
                req,
                start_version + u32::try_from(i).expect("unsupported number of batch events > u32"),
            )
        })
        .collect()
}

fn create_request_to_grpc(
    req: concepts::storage::CreateRequest,
) -> grpc_gen::CreateExecutionRequest {
    grpc_gen::CreateExecutionRequest {
        execution_id: Some(grpc_gen::ExecutionId {
            id: req.execution_id.to_string(),
        }),
        function_name: Some(grpc_gen::FunctionName::from(req.ffqn)),
        params: Some(
            to_any(req.params, "urn:obelisk:json:params".to_string())
                .expect("Params must be JSON-serializable"),
        ),
        scheduled_at: Some(prost_wkt_types::Timestamp::from(req.scheduled_at)),
        component_id: Some(req.component_id.clone().into()),
        deployment_id: Some(req.deployment_id.into()),
        parent_execution_id: req
            .parent
            .as_ref()
            .map(|(id, _)| grpc_gen::ExecutionId { id: id.to_string() }),
        parent_join_set_id: req.parent.clone().map(|(_, js)| js.into()),
        created_at: Some(prost_wkt_types::Timestamp::from(req.created_at)),
        metadata: metadata_to_grpc_map(&req.metadata),
        paused: req.paused,
        scheduled_by: req
            .scheduled_by
            .map(|id| grpc_gen::ExecutionId { id: id.to_string() }),
    }
}

impl TryFrom<grpc_gen::CreateExecutionRequest> for CreateRequest {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::CreateExecutionRequest) -> Result<Self, Self::Error> {
        let ffqn: FunctionFqn = value
            .function_name
            .argument_must_exist("function_name")?
            .try_into()?;
        let params = from_any(&value.params.argument_must_exist("params")?, "Params")?;
        let parent = match (value.parent_execution_id, value.parent_join_set_id) {
            (Some(parent_execution_id), Some(parent_join_set_id)) => Some((
                parent_execution_id.try_into()?,
                parent_join_set_id
                    .try_into()
                    .map_err(|_| tonic::Status::invalid_argument("join_set_id cannot be parsed"))?,
            )),
            (None, None) => None,
            _ => {
                return Err(tonic::Status::invalid_argument(
                    "`parent_execution_id` and `parent_join_set_id` must either both exist or both be absent",
                ));
            }
        };
        Ok(CreateRequest {
            created_at: value.created_at.argument_must_exist("created_at")?.into(),
            execution_id: value
                .execution_id
                .argument_must_exist("execution_id")?
                .try_into()?,
            ffqn,
            params,
            parent,
            scheduled_at: value
                .scheduled_at
                .argument_must_exist("scheduled_at")?
                .into(),
            component_id: value
                .component_id
                .argument_must_exist("component_id")?
                .try_into()?,
            deployment_id: value
                .deployment_id
                .argument_must_exist("deployment_id")?
                .try_into()?,
            metadata: metadata_from_grpc_map(value.metadata)?,
            scheduled_by: value.scheduled_by.map(TryInto::try_into).transpose()?,
            paused: value.paused,
        })
    }
}

fn append_request_from_grpc_event(
    event: grpc_gen::ExecutionEvent,
) -> Result<AppendRequest, tonic::Status> {
    let event: ExecutionEvent = event.try_into()?;
    Ok(AppendRequest {
        created_at: event.created_at,
        event: event.event,
    })
}

pub fn captured_write_to_grpc(write: CapturedDbWrite) -> grpc_gen::CapturedWrite {
    grpc_gen::CapturedWrite {
        write: Some(match write {
            CapturedDbWrite::Append {
                execution_id,
                version,
                req,
                backtraces,
            } => grpc_gen::captured_write::Write::Append(grpc_gen::captured_write::Append {
                execution_id: Some(grpc_gen::ExecutionId {
                    id: execution_id.to_string(),
                }),
                version: version.0,
                event: Some(append_request_to_grpc_event(req, version.0)),
                backtraces: backtraces.into_iter().map(Into::into).collect(),
            }),
            CapturedDbWrite::AppendBatch {
                current_time: _,
                batch,
                execution_id,
                version,
                backtraces,
            } => grpc_gen::captured_write::Write::AppendBatch(
                grpc_gen::captured_write::AppendBatch {
                    events: append_requests_to_grpc_events(batch, version.0),
                    execution_id: Some(grpc_gen::ExecutionId {
                        id: execution_id.to_string(),
                    }),
                    version: version.0,
                    backtraces: backtraces.into_iter().map(Into::into).collect(),
                },
            ),
            CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time: _,
                batch,
                execution_id,
                version,
                child_req,
                backtraces,
            } => grpc_gen::captured_write::Write::AppendBatchCreateNewExecution(
                grpc_gen::captured_write::AppendBatchCreateNewExecution {
                    events: append_requests_to_grpc_events(batch, version.0),
                    execution_id: Some(grpc_gen::ExecutionId {
                        id: execution_id.to_string(),
                    }),
                    version: version.0,
                    child_requests: child_req.into_iter().map(create_request_to_grpc).collect(),
                    backtraces: backtraces.into_iter().map(Into::into).collect(),
                },
            ),
            CapturedDbWrite::AppendStubResponse {
                events,
                response,
                current_time: _,
                backtraces,
            } => grpc_gen::captured_write::Write::AppendStubResponse(
                grpc_gen::captured_write::AppendStubResponse {
                    execution_id: Some(grpc_gen::ExecutionId {
                        id: events.execution_id.to_string(),
                    }),
                    version: events.version.0,
                    events: append_requests_to_grpc_events(events.batch, events.version.0),
                    parent_execution_id: Some(grpc_gen::ExecutionId {
                        id: response.parent_execution_id.to_string(),
                    }),
                    join_set_id: Some(response.join_set_id.clone().into()),
                    child_execution_id: Some(grpc_gen::ExecutionId {
                        id: response.child_execution_id.to_string(),
                    }),
                    result: Some(grpc_gen::SupportedFunctionResult::from(
                        response.result.clone(),
                    )),
                    finished_version: response.finished_version.0,
                    backtraces: backtraces.into_iter().map(Into::into).collect(),
                },
            ),
            CapturedDbWrite::AppendFinished {
                execution_id,
                version,
                retval,
                current_time: _,
                parent,
            } => {
                let (parent_execution_id, parent_join_set_id) = match parent {
                    Some((exec_id, join_set_id)) => (
                        Some(grpc_gen::ExecutionId {
                            id: exec_id.to_string(),
                        }),
                        Some(grpc_gen::JoinSetId::from(join_set_id)),
                    ),
                    None => (None, None),
                };
                grpc_gen::captured_write::Write::AppendFinished(
                    grpc_gen::captured_write::AppendFinished {
                        execution_id: Some(grpc_gen::ExecutionId {
                            id: execution_id.to_string(),
                        }),
                        version: version.0,
                        event: Some(grpc_gen::execution_event::Finished {
                            value: Some(grpc_gen::SupportedFunctionResult::from(retval)),
                            http_client_traces: Vec::new(),
                        }),
                        parent_execution_id,
                        parent_join_set_id,
                    },
                )
            }
        }),
    }
}

pub fn captured_write_from_grpc(
    write: grpc_gen::CapturedWrite,
) -> Result<CapturedDbWrite, tonic::Status> {
    match write.write.argument_must_exist("write")? {
        grpc_gen::captured_write::Write::Append(append) => Ok(CapturedDbWrite::Append {
            execution_id: append
                .execution_id
                .argument_must_exist("execution_id")?
                .try_into()?,
            version: Version::new(append.version),
            req: append_request_from_grpc_event(append.event.argument_must_exist("event")?)?,
            backtraces: append
                .backtraces
                .into_iter()
                .map(BacktraceInfo::try_from)
                .collect::<Result<Vec<BacktraceInfo>, _>>()?,
        }),
        grpc_gen::captured_write::Write::AppendBatch(batch) => Ok(CapturedDbWrite::AppendBatch {
            current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
            batch: batch
                .events
                .into_iter()
                .map(append_request_from_grpc_event)
                .collect::<Result<Vec<_>, _>>()?,
            execution_id: batch
                .execution_id
                .argument_must_exist("execution_id")?
                .try_into()?,
            version: Version::new(batch.version),
            backtraces: batch
                .backtraces
                .into_iter()
                .map(BacktraceInfo::try_from)
                .collect::<Result<Vec<BacktraceInfo>, _>>()?,
        }),
        grpc_gen::captured_write::Write::AppendBatchCreateNewExecution(batch) => {
            Ok(CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                batch: batch
                    .events
                    .into_iter()
                    .map(append_request_from_grpc_event)
                    .collect::<Result<Vec<_>, _>>()?,
                execution_id: batch
                    .execution_id
                    .argument_must_exist("execution_id")?
                    .try_into()?,
                version: Version::new(batch.version),
                child_req: batch
                    .child_requests
                    .into_iter()
                    .map(CreateRequest::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                backtraces: batch
                    .backtraces
                    .into_iter()
                    .map(BacktraceInfo::try_from)
                    .collect::<Result<Vec<BacktraceInfo>, _>>()?,
            })
        }
        grpc_gen::captured_write::Write::AppendStubResponse(batch) => {
            let execution_id = batch
                .execution_id
                .argument_must_exist("execution_id")?
                .try_into()?;
            Ok(CapturedDbWrite::AppendStubResponse {
                events: AppendEventsToExecution {
                    execution_id,
                    version: Version::new(batch.version),
                    batch: batch
                        .events
                        .into_iter()
                        .map(append_request_from_grpc_event)
                        .collect::<Result<Vec<_>, _>>()?,
                },
                response: AppendResponseToExecution {
                    parent_execution_id: batch
                        .parent_execution_id
                        .argument_must_exist("parent_execution_id")?
                        .try_into()?,
                    created_at: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                    join_set_id: batch
                        .join_set_id
                        .argument_must_exist("join_set_id")?
                        .try_into()?,
                    child_execution_id: match batch
                        .child_execution_id
                        .argument_must_exist("child_execution_id")?
                        .try_into()?
                    {
                        ExecutionId::Derived(derived) => derived,
                        ExecutionId::TopLevel(_) => {
                            return Err(tonic::Status::invalid_argument(
                                "child_execution_id must be a derived execution id",
                            ));
                        }
                    },
                    finished_version: Version::new(batch.finished_version),
                    result: batch.result.argument_must_exist("result")?.try_into()?,
                },
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                backtraces: batch
                    .backtraces
                    .into_iter()
                    .map(BacktraceInfo::try_from)
                    .collect::<Result<Vec<BacktraceInfo>, _>>()?,
            })
        }
        grpc_gen::captured_write::Write::AppendFinished(append_finished) => {
            let finished = append_finished.event.argument_must_exist("event")?;
            let parent = match (
                append_finished.parent_execution_id,
                append_finished.parent_join_set_id,
            ) {
                (Some(parent_execution_id), Some(parent_join_set_id)) => Some((
                    parent_execution_id.try_into()?,
                    parent_join_set_id.try_into()?,
                )),
                (None, None) => None,
                _ => {
                    return Err(tonic::Status::invalid_argument(
                        "parent_execution_id and parent_join_set_id must both be set or both be unset",
                    ));
                }
            };
            Ok(CapturedDbWrite::AppendFinished {
                execution_id: append_finished
                    .execution_id
                    .argument_must_exist("execution_id")?
                    .try_into()?,
                version: Version::new(append_finished.version),
                retval: finished.value.argument_must_exist("value")?.try_into()?,
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                parent,
            })
        }
    }
}
