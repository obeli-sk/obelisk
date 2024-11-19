use crate::command::grpc::{self};
use anyhow::anyhow;
use concepts::{
    prefixed_ulid::{JoinSetId, RunId},
    storage::{
        DbError, ExecutionEvent, ExecutionEventInner, ExecutionListPagination, HistoryEvent,
        HistoryEventScheduledAt, JoinSetRequest, Pagination, PendingState, PendingStateFinished,
        PendingStateFinishedError, PendingStateFinishedResultKind, SpecificError, VersionType,
    },
    ConfigId, ConfigIdType, ExecutionId, FinishedExecutionError, FinishedExecutionResult,
    FunctionFqn, SupportedFunctionReturnValue,
};
use std::borrow::Borrow;
use tracing::error;

impl<T: Borrow<ExecutionId>> From<T> for grpc::ExecutionId {
    fn from(value: T) -> Self {
        Self {
            id: value.borrow().to_string(),
        }
    }
}

impl From<JoinSetId> for grpc::JoinSetId {
    fn from(value: JoinSetId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<RunId> for grpc::RunId {
    fn from(value: RunId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<ConfigId> for grpc::ConfigId {
    fn from(value: ConfigId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl TryFrom<grpc::ExecutionId> for ExecutionId {
    type Error = tonic::Status;

    fn try_from(value: grpc::ExecutionId) -> Result<Self, Self::Error> {
        value.id.parse().map_err(|parse_err| {
            tonic::Status::invalid_argument(format!("ExecutionId cannot be parsed - {parse_err}"))
        })
    }
}

pub trait TonicServerOptionExt<T> {
    fn argument_must_exist(self, argument: &str) -> Result<T, tonic::Status>;
}

impl<T> TonicServerOptionExt<T> for Option<T> {
    fn argument_must_exist(self, argument: &str) -> Result<T, tonic::Status> {
        self.ok_or_else(|| {
            tonic::Status::invalid_argument(format!("argument `{argument}` must exist"))
        })
    }
}

pub trait TonicClientResultExt<T> {
    fn to_anyhow(self) -> Result<tonic::Response<T>, anyhow::Error>;
}

impl<T> TonicClientResultExt<T> for Result<tonic::Response<T>, tonic::Status> {
    fn to_anyhow(self) -> Result<tonic::Response<T>, anyhow::Error> {
        self.map_err(|err| {
            let msg = err.message();
            if msg.is_empty() {
                anyhow!("{err}")
            } else {
                anyhow!("{msg}")
            }
        })
    }
}

pub trait TonicServerResultExt<T> {
    fn to_status(self) -> Result<T, tonic::Status>;
}

impl<T> TonicServerResultExt<T> for Result<T, DbError> {
    fn to_status(self) -> Result<T, tonic::Status> {
        self.map_err(|err| db_error_to_status(&err))
    }
}

pub fn db_error_to_status(db_err: &DbError) -> tonic::Status {
    if matches!(db_err, DbError::Specific(SpecificError::NotFound)) {
        tonic::Status::not_found("entity not found")
    } else {
        error!("Got db error {db_err:?}");
        tonic::Status::internal(format!("database error: {db_err}"))
    }
}

impl<T> TonicServerResultExt<T> for Result<T, anyhow::Error> {
    fn to_status(self) -> Result<T, tonic::Status> {
        self.map_err(|err| {
            error!("{err:?}");
            tonic::Status::internal(format!("{err}"))
        })
    }
}

impl TryFrom<grpc::FunctionName> for FunctionFqn {
    type Error = tonic::Status;

    fn try_from(value: grpc::FunctionName) -> Result<Self, Self::Error> {
        FunctionFqn::try_from_tuple(&value.interface_name, &value.function_name).map_err(
            |parse_err| {
                tonic::Status::invalid_argument(format!(
                    "FunctionName cannot be parsed - {parse_err}"
                ))
            },
        )
    }
}

impl<T: Borrow<FunctionFqn>> From<T> for grpc::FunctionName {
    fn from(ffqn: T) -> Self {
        let ffqn = ffqn.borrow();
        grpc::FunctionName {
            interface_name: ffqn.ifc_fqn.to_string(),
            function_name: ffqn.function_name.to_string(),
        }
    }
}

impl From<ConfigIdType> for grpc::ComponentType {
    fn from(value: ConfigIdType) -> Self {
        grpc::ComponentType::from_str_name(&value.to_string().to_uppercase()).unwrap()
    }
}

impl From<PendingState> for grpc::ExecutionStatus {
    fn from(pending_state: PendingState) -> grpc::ExecutionStatus {
        use grpc::execution_status::{BlockedByJoinSet, Finished, Locked, PendingAt, Status};
        grpc::ExecutionStatus {
            status: Some(match pending_state {
                PendingState::Locked {
                    executor_id: _,
                    run_id,
                    lock_expires_at,
                } => Status::Locked(Locked {
                    run_id: Some(run_id.into()),
                    lock_expires_at: Some(lock_expires_at.into()),
                }),
                PendingState::PendingAt { scheduled_at } => Status::PendingAt(PendingAt {
                    scheduled_at: Some(scheduled_at.into()),
                }),
                PendingState::BlockedByJoinSet {
                    join_set_id,
                    lock_expires_at,
                    closing,
                } => Status::BlockedByJoinSet(BlockedByJoinSet {
                    join_set_id: Some(join_set_id.into()),
                    lock_expires_at: Some(lock_expires_at.into()),
                    closing,
                }),
                PendingState::Finished {
                    finished:
                        PendingStateFinished {
                            version: _,
                            finished_at,
                            result_kind,
                        },
                } => Status::Finished(Finished {
                    finished_at: Some(finished_at.into()),
                    result_kind: grpc::ResultKind::from(result_kind).into(),
                }),
            }),
        }
    }
}

impl From<PendingStateFinishedResultKind> for grpc::ResultKind {
    fn from(result_kind: PendingStateFinishedResultKind) -> Self {
        match result_kind {
            PendingStateFinishedResultKind(Ok(())) => grpc::ResultKind::Ok,
            PendingStateFinishedResultKind(Err(PendingStateFinishedError::Timeout)) => {
                grpc::ResultKind::Timeout
            }
            PendingStateFinishedResultKind(Err(
                PendingStateFinishedError::NondeterminismDetected,
            )) => grpc::ResultKind::NondeterminismDetected,
            PendingStateFinishedResultKind(Err(PendingStateFinishedError::ExecutionFailure)) => {
                grpc::ResultKind::ExecutionFailure
            }
            PendingStateFinishedResultKind(Err(PendingStateFinishedError::FallibleError)) => {
                grpc::ResultKind::FallibleError
            }
        }
    }
}

impl TryFrom<grpc::list_executions_request::Pagination> for ExecutionListPagination {
    type Error = tonic::Status;

    fn try_from(
        pagination: grpc::list_executions_request::Pagination,
    ) -> Result<Self, Self::Error> {
        use grpc::list_executions_request::Pagination as GPagination;
        use grpc::list_executions_request::{
            cursor::Cursor as InnerCursor, Cursor as OuterCursor, NewerThan, OlderThan,
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
                    length,
                    cursor: Some(ExecutionId::try_from(id)?),
                    including_cursor,
                }),
                Some(OuterCursor {
                    cursor: Some(InnerCursor::CreatedAt(timestamp)),
                }) => ExecutionListPagination::CreatedBy(Pagination::NewerThan {
                    length,
                    cursor: Some(timestamp.into()),
                    including_cursor,
                }),
                _ => ExecutionListPagination::CreatedBy(Pagination::NewerThan {
                    length,
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
                    length,
                    cursor: Some(ExecutionId::try_from(id)?),
                    including_cursor,
                }),
                Some(OuterCursor {
                    cursor: Some(InnerCursor::CreatedAt(timestamp)),
                }) => ExecutionListPagination::CreatedBy(Pagination::OlderThan {
                    length,
                    cursor: Some(timestamp.into()),
                    including_cursor,
                }),
                _ => ExecutionListPagination::CreatedBy(Pagination::OlderThan {
                    length,
                    cursor: None,
                    including_cursor,
                }),
            },
        })
    }
}

pub(crate) fn to_any<T: serde::Serialize>(
    serializable: T,
    uri: String,
) -> Option<prost_wkt_types::Any> {
    serde_json::to_string(&serializable)
        .inspect_err(|ser_err| {
            error!(
                "Cannot serialize {:?} - {ser_err:?}",
                std::any::type_name::<T>()
            );
        })
        .ok()
        .map(|res| prost_wkt_types::Any {
            type_url: uri,
            value: res.into_bytes(),
        })
}

pub(crate) fn from_finished_result_to_grpc_result_detail(
    finished_result: FinishedExecutionResult,
    ffqn: &FunctionFqn,
) -> grpc::ResultDetail {
    let value = match finished_result {
        Ok(SupportedFunctionReturnValue::None) => {
            grpc::result_detail::Value::Ok(grpc::result_detail::Ok { return_value: None })
        }
        Ok(SupportedFunctionReturnValue::InfallibleOrResultOk(val_with_type)) => {
            grpc::result_detail::Value::Ok(grpc::result_detail::Ok {
                return_value: to_any(
                    val_with_type.value,
                    format!("urn:obelisk:json:params:{ffqn}"),
                ),
            })
        }
        Ok(SupportedFunctionReturnValue::FallibleResultErr(val_with_type)) => {
            grpc::result_detail::Value::FallibleError(grpc::result_detail::FallibleError {
                return_value: to_any(
                    val_with_type.value,
                    format!("urn:obelisk:json:params:{ffqn}"),
                ),
            })
        }
        Err(FinishedExecutionError::PermanentTimeout) => {
            grpc::result_detail::Value::Timeout(grpc::result_detail::Timeout {})
        }
        Err(FinishedExecutionError::PermanentFailure(reason)) => {
            grpc::result_detail::Value::ExecutionFailure(grpc::result_detail::ExecutionFailure {
                reason: reason.to_string(),
            })
        }
        Err(FinishedExecutionError::NondeterminismDetected(reason)) => {
            grpc::result_detail::Value::NondeterminismDetected(
                grpc::result_detail::NondeterminismDetected {
                    reason: reason.to_string(),
                },
            )
        }
    };
    grpc::ResultDetail { value: Some(value) }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn from_execution_event_to_grpc(
    event: ExecutionEvent,
    version: VersionType,
    ffqn: &FunctionFqn,
) -> grpc::ExecutionEvent {
    grpc::ExecutionEvent {
            created_at: Some(prost_wkt_types::Timestamp::from(event.created_at)),
            version,
            event: Some(match event.event {
                ExecutionEventInner::Created {
                    ffqn,
                    params,
                    parent: _,
                    scheduled_at,
                    retry_exp_backoff: _,
                    max_retries: _,
                    config_id,
                    metadata: _,
                    scheduled_by,
                } => grpc::execution_event::Event::Created(grpc::execution_event::Created {
                    params: to_any(
                        params,
                        format!("urn:obelisk:json:params:{ffqn}"),
                    ),
                    function_name: Some(grpc::FunctionName::from(ffqn)),
                    scheduled_at: Some(prost_wkt_types::Timestamp::from(scheduled_at)),
                    config_id: config_id.to_string(),
                    scheduled_by: scheduled_by.map(|id| grpc::ExecutionId { id: id.to_string() }),
                }),
                ExecutionEventInner::Locked {
                    config_id,
                    executor_id: _,
                    run_id,
                    lock_expires_at,
                } => grpc::execution_event::Event::Locked(grpc::execution_event::Locked {
                    config_id: config_id.to_string(),
                    run_id: run_id.to_string(),
                    lock_expires_at: Some(prost_wkt_types::Timestamp::from(lock_expires_at)),
                }),
                ExecutionEventInner::Unlocked => grpc::execution_event::Event::Unlocked(grpc::execution_event::Unlocked {}),
                ExecutionEventInner::IntermittentlyFailed {
                    backoff_expires_at,
                    reason,
                } => grpc::execution_event::Event::Failed(grpc::execution_event::IntermittentlyFailed {
                    reason: reason.to_string(),
                    backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                }),
                ExecutionEventInner::IntermittentTimedOut { backoff_expires_at } => grpc::execution_event::Event::TimedOut(grpc::execution_event::IntermittentlyTimedOut {
                    backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                }),
                ExecutionEventInner::Finished { result } => grpc::execution_event::Event::Finished(grpc::execution_event::Finished {
                    result_detail: Some(
                        from_finished_result_to_grpc_result_detail(result, ffqn)
                    ),
                }),
                ExecutionEventInner::HistoryEvent { event } => grpc::execution_event::Event::HistoryVariant(grpc::execution_event::HistoryEvent {
                    event: Some(match event {
                        HistoryEvent::Persist { value } => grpc::execution_event::history_event::Event::Persist(grpc::execution_event::history_event::Persist {
                            data: Some(prost_wkt_types::Any {
                                type_url: "unknown".to_string(),
                                value,
                            }),
                        }),
                        HistoryEvent::JoinSet { join_set_id } => grpc::execution_event::history_event::Event::JoinSetCreated(grpc::execution_event::history_event::JoinSetCreated {
                            join_set_id: Some(grpc::JoinSetId { id: join_set_id.to_string() }),
                        }),
                        HistoryEvent::JoinSetRequest { join_set_id, request } => grpc::execution_event::history_event::Event::JoinSetRequest(grpc::execution_event::history_event::JoinSetRequest {
                            join_set_id: Some(grpc::JoinSetId { id: join_set_id.to_string() }),
                            join_set_request: match request {
                                JoinSetRequest::DelayRequest { delay_id, expires_at } => {
                                    Some(grpc::execution_event::history_event::join_set_request::JoinSetRequest::DelayRequest(
                                        grpc::execution_event::history_event::join_set_request::DelayRequest {
                                            delay_id: Some(grpc::DelayId { id: delay_id.to_string() }),
                                            expires_at: Some(prost_wkt_types::Timestamp::from(expires_at)),
                                        }
                                    ))
                                }
                                JoinSetRequest::ChildExecutionRequest { child_execution_id } => {
                                    Some(grpc::execution_event::history_event::join_set_request::JoinSetRequest::ChildExecutionRequest(
                                        grpc::execution_event::history_event::join_set_request::ChildExecutionRequest {
                                            child_execution_id: Some(grpc::ExecutionId { id: child_execution_id.to_string() }),
                                        }
                                    ))
                                }
                            },
                        }),
                        HistoryEvent::JoinNext { join_set_id, run_expires_at, closing } => grpc::execution_event::history_event::Event::JoinNext(grpc::execution_event::history_event::JoinNext {
                            join_set_id: Some(grpc::JoinSetId { id: join_set_id.to_string() }),
                            run_expires_at: Some(prost_wkt_types::Timestamp::from(run_expires_at)),
                            closing,
                        }),
                        HistoryEvent::Schedule { execution_id, scheduled_at } => grpc::execution_event::history_event::Event::Schedule(grpc::execution_event::history_event::Schedule {
                            execution_id: Some(grpc::ExecutionId { id: execution_id.to_string() }),
                            scheduled_at: Some(grpc::execution_event::history_event::schedule::ScheduledAt {
                                variant: match scheduled_at {
                                    HistoryEventScheduledAt::Now => Some(grpc::execution_event::history_event::schedule::scheduled_at::Variant::Now(grpc::execution_event::history_event::schedule::scheduled_at::Now {})),
                                    HistoryEventScheduledAt::At( at) => Some(grpc::execution_event::history_event::schedule::scheduled_at::Variant::At(grpc::execution_event::history_event::schedule::scheduled_at::At {
                                        at: Some(prost_wkt_types::Timestamp::from(at)),
                                    })),
                                    HistoryEventScheduledAt::In (r#in ) => Some(grpc::execution_event::history_event::schedule::scheduled_at::Variant::In(grpc::execution_event::history_event::schedule::scheduled_at::In {
                                        r#in: prost_wkt_types::Duration::try_from(r#in).ok(),
                                    })),
                                },
                            }),
                        }),
                    }),
                }),
            }),
        }
}
