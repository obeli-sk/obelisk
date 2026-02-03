use crate::grpc_gen::{self, execution_event::history_event, result_kind};
use concepts::{
    ComponentId, ComponentType, ContentDigest, ExecutionFailureKind, ExecutionId,
    FinishedExecutionError, FunctionFqn, SupportedFunctionReturnValue,
    component_id::{Digest, InputContentDigest},
    prefixed_ulid::{DelayId, DeploymentId, RunId},
    storage::{
        CancelOutcome, DbErrorGeneric, DbErrorRead, DbErrorWrite, ExecutionEvent,
        ExecutionListPagination, ExecutionRequest, ExecutionWithState, HistoryEvent,
        HistoryEventScheduleAt, JoinSetRequest, Locked, LockedBy, LogEntry, LogEntryRow, LogLevel,
        LogStreamType, Pagination, PendingState, PendingStateBlockedByJoinSet,
        PendingStateFinished, PendingStateFinishedError, PendingStateFinishedResultKind,
        PendingStateLocked, PendingStatePendingAt, VersionParseError,
        http_client_trace::HttpClientTrace,
    },
};
use concepts::{JoinSetId, JoinSetKind};
use std::{borrow::Borrow, str::FromStr};
use tracing::warn;

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

impl From<RunId> for grpc_gen::RunId {
    fn from(value: RunId) -> Self {
        Self {
            id: value.to_string(),
        }
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
            digest: Some(value.input_digest.into()),
        }
    }
}

impl From<InputContentDigest> for grpc_gen::ContentDigest {
    fn from(value: InputContentDigest) -> Self {
        grpc_gen::ContentDigest::from(&value)
    }
}
impl From<&InputContentDigest> for grpc_gen::ContentDigest {
    fn from(value: &InputContentDigest) -> Self {
        grpc_gen::ContentDigest {
            digest: value.to_string(),
        }
    }
}
impl TryFrom<grpc_gen::ContentDigest> for InputContentDigest {
    type Error = tonic::Status;

    fn try_from(value: grpc_gen::ContentDigest) -> Result<Self, Self::Error> {
        let digest = Digest::from_str(&value.digest).map_err(|parse_err| {
            warn!("`Digest` cannot be parsed - {parse_err:?}");
            tonic::Status::invalid_argument(format!("`Digest` cannot be parsed - {parse_err}"))
        })?;
        Ok(InputContentDigest(ContentDigest(digest)))
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
        let input_digest = InputContentDigest::try_from(input_digest)?;
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
    if matches!(*db_err, DbErrorWrite::NotFound) {
        tonic::Status::not_found("entity not found")
    } else {
        warn!("Got db error {db_err:?}");
        tonic::Status::internal("database error".to_string())
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
            ComponentType::ActivityWasm => grpc_gen::ComponentType::ActivityWasm,
            ComponentType::ActivityStub => grpc_gen::ComponentType::ActivityStub,
            ComponentType::ActivityExternal => grpc_gen::ComponentType::ActivityExternal,
            ComponentType::Workflow => grpc_gen::ComponentType::Workflow,
            ComponentType::WebhookEndpoint => grpc_gen::ComponentType::WebhookEndpoint,
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
            grpc_gen::ComponentType::ActivityWasm => Ok(ComponentType::ActivityWasm),
            grpc_gen::ComponentType::ActivityStub => Ok(ComponentType::ActivityStub),
            grpc_gen::ComponentType::ActivityExternal => Ok(ComponentType::ActivityExternal),
            grpc_gen::ComponentType::Workflow => Ok(ComponentType::Workflow),
            grpc_gen::ComponentType::WebhookEndpoint => Ok(ComponentType::WebhookEndpoint),
        }
    }
}

impl From<&ExecutionWithState> for grpc_gen::ExecutionStatus {
    fn from(execution_with_state: &ExecutionWithState) -> grpc_gen::ExecutionStatus {
        use grpc_gen::execution_status::{
            BlockedByJoinSet, Finished, Locked, Paused, PendingAt, Status,
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
                PendingState::Finished {
                    finished:
                        PendingStateFinished {
                            version: _,
                            finished_at,
                            result_kind,
                        },
                } => Status::Finished(Finished {
                    finished_at: Some((*finished_at).into()),
                    result_kind: grpc_gen::ResultKind::from(*result_kind).into(),
                }),
                PendingState::Paused => Status::Paused(Paused {}),
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

impl From<SupportedFunctionReturnValue> for grpc_gen::SupportedFunctionResult {
    fn from(finished_result: SupportedFunctionReturnValue) -> Self {
        let value = match finished_result {
            SupportedFunctionReturnValue::Ok { ok: val_with_type } => {
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
                )
            }
            SupportedFunctionReturnValue::Err { err: val_with_type } => {
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
                )
            }
            SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError {
                kind,
                reason,
                detail,
            }) => grpc_gen::supported_function_result::Value::ExecutionFailure(
                grpc_gen::supported_function_result::ExecutionFailure {
                    reason,
                    detail,
                    kind: grpc_gen::ExecutionFailureKind::from(kind).into(),
                },
            ),
        };
        grpc_gen::SupportedFunctionResult { value: Some(value) }
    }
}

pub fn from_execution_event_to_grpc(event: ExecutionEvent) -> grpc_gen::ExecutionEvent {
    grpc_gen::ExecutionEvent {
            created_at: Some(prost_wkt_types::Timestamp::from(event.created_at)),
            version: event.version.0,
            backtrace_id: event.backtrace_id.map(|v|v.0),
            event: Some(match event.event {
                ExecutionRequest::Created {
                    ffqn,
                    params,
                    parent: _,
                    scheduled_at,
                    component_id,
                    deployment_id,
                    metadata: _,
                    scheduled_by,
                } => grpc_gen::execution_event::Event::Created(grpc_gen::execution_event::Created {
                    params: Some(to_any(
                        params,
                        format!("urn:obelisk:json:params:{ffqn}"),
                    ).expect("Params must be JSON-serializable")),
                    function_name: Some(grpc_gen::FunctionName::from(ffqn)),
                    scheduled_at: Some(prost_wkt_types::Timestamp::from(scheduled_at)),
                    component_id: Some(component_id.into()),
                    deployment_id: Some(deployment_id.into()),
                    scheduled_by: scheduled_by.map(|id| grpc_gen::ExecutionId { id: id.to_string() }),
                }),
                ExecutionRequest::Locked(Locked{
                    component_id,
                    deployment_id,
                    executor_id: _,
                    run_id,
                    lock_expires_at,
                    retry_config: _,
                }) => grpc_gen::execution_event::Event::Locked(grpc_gen::execution_event::Locked {
                    component_id: Some(component_id.into()),
                    deployment_id: Some(deployment_id.into()),
                    run_id: run_id.to_string(),
                    lock_expires_at: Some(prost_wkt_types::Timestamp::from(lock_expires_at)),
                }),
                ExecutionRequest::Unlocked { backoff_expires_at, reason } => {
                    grpc_gen::execution_event::Event::Unlocked(grpc_gen::execution_event::Unlocked {
                        backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                        reason: reason.to_string(),
                    })
                },
                ExecutionRequest::TemporarilyFailed {
                    backoff_expires_at,
                    reason,
                    detail,
                    http_client_traces
                } => grpc_gen::execution_event::Event::TemporarilyFailed(grpc_gen::execution_event::TemporarilyFailed {
                    reason: reason.to_string(),
                    detail,
                    backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                    http_client_traces: http_client_traces.unwrap_or_default().into_iter().map(grpc_gen::HttpClientTrace::from).collect(),
                }),
                ExecutionRequest::TemporarilyTimedOut { backoff_expires_at, http_client_traces } => {
                    grpc_gen::execution_event::Event::TemporarilyTimedOut(grpc_gen::execution_event::TemporarilyTimedOut {
                        backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                        http_client_traces: http_client_traces
                            .unwrap_or_default()
                            .into_iter()
                            .map(grpc_gen::HttpClientTrace::from)
                            .collect(),
                    })
                },
                ExecutionRequest::Finished { result, http_client_traces } => grpc_gen::execution_event::Event::Finished(grpc_gen::execution_event::Finished {
                    value: Some(
                        grpc_gen::SupportedFunctionResult::from(result)
                    ),
                    http_client_traces: http_client_traces.unwrap_or_default().into_iter().map(grpc_gen::HttpClientTrace::from).collect(),

                }),
                ExecutionRequest::Paused  => {
                    grpc_gen::execution_event::Event::Paused(grpc_gen::execution_event::Paused{})
                },
                ExecutionRequest::Unpaused => grpc_gen::execution_event::Event::Unpaused(grpc_gen::execution_event::Unpaused{}),
                ExecutionRequest::HistoryEvent { event } => grpc_gen::execution_event::Event::HistoryVariant(grpc_gen::execution_event::HistoryEvent {
                    event: Some(match event {
                        HistoryEvent::Persist { value, kind } => history_event::Event::Persist(history_event::Persist {
                            data: Some(prost_wkt_types::Any {
                                type_url: "unknown".to_string(),
                                value,
                            }),
                            kind: Some(history_event::persist::PersistKind { variant: Some(match kind {
                                concepts::storage::PersistKind::RandomU64 { .. } =>
                                history_event::persist::persist_kind::Variant::RandomU64(
                                    history_event::persist::persist_kind::RandomU64 {  }),
                                concepts::storage::PersistKind::RandomString { .. } =>
                                history_event::persist::persist_kind::Variant::RandomString(
                                    history_event::persist::persist_kind::RandomString {  }),
                            }) })
                        }),
                        HistoryEvent::JoinSetCreate { join_set_id } =>
                            history_event::Event::JoinSetCreated(history_event::JoinSetCreated {
                                join_set_id: Some(join_set_id.into()),
                            }),
                        HistoryEvent::JoinSetRequest { join_set_id, request } => history_event::Event::JoinSetRequest(history_event::JoinSetRequest {
                            join_set_id: Some(join_set_id.into()),
                            join_set_request: match request {
                                JoinSetRequest::DelayRequest { delay_id, expires_at, .. } => {
                                    Some(history_event::join_set_request::JoinSetRequest::DelayRequest(
                                        history_event::join_set_request::DelayRequest {
                                            delay_id: Some(delay_id.into()),
                                            expires_at: Some(prost_wkt_types::Timestamp::from(expires_at)),
                                        }
                                    ))
                                }
                                JoinSetRequest::ChildExecutionRequest { child_execution_id, params: _, target_ffqn: _ } => {
                                    Some(history_event::join_set_request::JoinSetRequest::ChildExecutionRequest(
                                        history_event::join_set_request::ChildExecutionRequest {
                                            child_execution_id: Some(grpc_gen::ExecutionId { id: child_execution_id.to_string() }),
                                        }
                                    ))
                                }
                            },
                        }),
                        HistoryEvent::JoinNext { join_set_id, run_expires_at, closing, requested_ffqn:_ } => history_event::Event::JoinNext(history_event::JoinNext {
                            join_set_id: Some(join_set_id.into()),
                            run_expires_at: Some(prost_wkt_types::Timestamp::from(run_expires_at)),
                            closing,
                        }),
                        HistoryEvent::JoinNextTooMany { join_set_id, requested_ffqn } => history_event::Event::JoinNextTooMany(history_event::JoinNextTooMany {
                            join_set_id: Some(join_set_id.into()),
                            function: requested_ffqn.map(Into::into)
                        }),
                        HistoryEvent::Schedule { execution_id, schedule_at: scheduled_at } => history_event::Event::Schedule(history_event::Schedule {
                            execution_id: Some(grpc_gen::ExecutionId { id: execution_id.to_string() }),
                            scheduled_at: Some(history_event::schedule::ScheduledAt {
                                variant: match scheduled_at {
                                    HistoryEventScheduleAt::Now => Some(history_event::schedule::scheduled_at::Variant::Now(history_event::schedule::scheduled_at::Now {})),
                                    HistoryEventScheduleAt::At( at) => Some(history_event::schedule::scheduled_at::Variant::At(history_event::schedule::scheduled_at::At {
                                        at: Some(prost_wkt_types::Timestamp::from(at)),
                                    })),
                                    HistoryEventScheduleAt::In (r#in ) => Some(history_event::schedule::scheduled_at::Variant::In(history_event::schedule::scheduled_at::In {
                                        r#in: prost_wkt_types::Duration::try_from(r#in).ok(),
                                    })),
                                },
                            }),
                        }),
                        HistoryEvent::Stub {  target_execution_id, persist_result: target_result, .. } => history_event::Event::Stub(history_event::Stub {
                            execution_id: Some(ExecutionId::Derived(target_execution_id).into()),
                            success: target_result.is_ok()
                        }),
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

impl From<CancelOutcome> for grpc_gen::cancel_response::CancelOutcome {
    fn from(value: CancelOutcome) -> Self {
        match value {
            CancelOutcome::Cancelled => grpc_gen::cancel_response::CancelOutcome::Cancelled,
            CancelOutcome::AlreadyFinished => {
                grpc_gen::cancel_response::CancelOutcome::AlreadyFinished
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
        }
    }
}
