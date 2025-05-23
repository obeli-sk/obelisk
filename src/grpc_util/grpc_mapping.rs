use crate::command::grpc::{self};
use concepts::{
    ClosingStrategy, ComponentId, ComponentType, ExecutionId, FinishedExecutionError,
    FinishedExecutionResult, FunctionFqn, SupportedFunctionReturnValue,
    prefixed_ulid::{DelayId, RunId},
    storage::{
        DbError, ExecutionEvent, ExecutionEventInner, ExecutionListPagination, HistoryEvent,
        HistoryEventScheduledAt, JoinSetRequest, Pagination, PendingState, PendingStateFinished,
        PendingStateFinishedError, PendingStateFinishedResultKind, SpecificError, VersionType,
        http_client_trace::HttpClientTrace,
    },
};
use concepts::{JoinSetId, JoinSetKind};
use std::borrow::Borrow;
use tracing::error;

impl<T: Borrow<ExecutionId>> From<T> for grpc::ExecutionId {
    fn from(value: T) -> Self {
        Self {
            id: value.borrow().to_string(),
        }
    }
}

impl From<DelayId> for grpc::DelayId {
    fn from(value: DelayId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<JoinSetId> for grpc::JoinSetId {
    fn from(join_set_id: JoinSetId) -> Self {
        Self {
            kind: grpc::join_set_id::JoinSetKind::from(join_set_id.kind) as i32,
            name: join_set_id.name.to_string(),
        }
    }
}

impl From<JoinSetKind> for grpc::join_set_id::JoinSetKind {
    fn from(value: JoinSetKind) -> Self {
        match value {
            JoinSetKind::OneOff => grpc::join_set_id::JoinSetKind::OneOff,
            JoinSetKind::Named => grpc::join_set_id::JoinSetKind::Named,
            JoinSetKind::Generated => grpc::join_set_id::JoinSetKind::Generated,
        }
    }
}

impl From<grpc::join_set_id::JoinSetKind> for JoinSetKind {
    fn from(value: grpc::join_set_id::JoinSetKind) -> Self {
        match value {
            grpc::join_set_id::JoinSetKind::OneOff => JoinSetKind::OneOff,
            grpc::join_set_id::JoinSetKind::Named => JoinSetKind::Named,
            grpc::join_set_id::JoinSetKind::Generated => JoinSetKind::Generated,
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

impl From<ComponentId> for grpc::ComponentId {
    fn from(value: ComponentId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl TryFrom<grpc::ComponentId> for ComponentId {
    type Error = tonic::Status;

    fn try_from(value: grpc::ComponentId) -> Result<Self, Self::Error> {
        value.id.parse().map_err(|parse_err| {
            error!("{parse_err:?}");
            tonic::Status::invalid_argument(format!("ComponentId cannot be parsed - {parse_err}"))
        })
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
    #[expect(clippy::result_large_err)]
    fn argument_must_exist(self, argument: &str) -> Result<T, tonic::Status>;

    #[expect(clippy::result_large_err)]
    fn entity_must_exist(self) -> Result<T, tonic::Status>;
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
}

pub trait TonicServerResultExt<T> {
    #[expect(clippy::result_large_err)]
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
                error!("{parse_err:?}");
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

impl From<ComponentType> for grpc::ComponentType {
    fn from(value: ComponentType) -> Self {
        match value {
            ComponentType::ActivityWasm => grpc::ComponentType::ActivityWasm,
            ComponentType::Workflow => grpc::ComponentType::Workflow,
            ComponentType::WebhookEndpoint => grpc::ComponentType::WebhookEndpoint,
        }
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
            PendingStateFinishedResultKind(Err(PendingStateFinishedError::ExecutionFailure)) => {
                grpc::ResultKind::ExecutionFailure
            }
            PendingStateFinishedResultKind(Err(PendingStateFinishedError::FallibleError)) => {
                grpc::ResultKind::FallibleError
            }
            PendingStateFinishedResultKind(Err(
                PendingStateFinishedError::UnhandledChildExecutionError,
            )) => grpc::ResultKind::UnhandledChildExecutionError,
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

impl From<FinishedExecutionResult> for grpc::ResultDetail {
    fn from(finished_result: FinishedExecutionResult) -> Self {
        let value = match finished_result {
            Ok(SupportedFunctionReturnValue::None) => {
                grpc::result_detail::Value::Ok(grpc::result_detail::Ok { return_value: None })
            }
            Ok(SupportedFunctionReturnValue::InfallibleOrResultOk(val_with_type)) => {
                grpc::result_detail::Value::Ok(grpc::result_detail::Ok {
                    return_value: to_any(
                        val_with_type.value,
                        "urn:obelisk:json:result:TBD".to_string(),
                    ),
                })
            }
            Ok(SupportedFunctionReturnValue::FallibleResultErr(val_with_type)) => {
                grpc::result_detail::Value::FallibleError(grpc::result_detail::FallibleError {
                    return_value: to_any(
                        val_with_type.value,
                        "urn:obelisk:json:result:TBD".to_string(),
                    ),
                })
            }
            Err(FinishedExecutionError::PermanentTimeout) => {
                grpc::result_detail::Value::Timeout(grpc::result_detail::Timeout {})
            }
            Err(FinishedExecutionError::PermanentFailure {
                reason_full,
                kind: _, // TODO Add PermanentFailureKind
                detail,
                reason_inner: _,
            }) => grpc::result_detail::Value::ExecutionFailure(
                grpc::result_detail::ExecutionFailure {
                    reason: reason_full,
                    detail,
                },
            ),
            Err(FinishedExecutionError::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            }) => grpc::result_detail::Value::UnhandledChildExecutionError(
                grpc::result_detail::UnhandledChildExecutionError {
                    child_execution_id: Some(ExecutionId::Derived(child_execution_id).into()),
                    root_cause_id: Some(ExecutionId::Derived(root_cause_id).into()),
                },
            ),
        };
        grpc::ResultDetail { value: Some(value) }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn from_execution_event_to_grpc(
    event: ExecutionEvent,
    version: VersionType,
) -> grpc::ExecutionEvent {
    grpc::ExecutionEvent {
            created_at: Some(prost_wkt_types::Timestamp::from(event.created_at)),
            version,
            backtrace_id: event.backtrace_id.map(|v|v.0),
            event: Some(match event.event {
                ExecutionEventInner::Created {
                    ffqn,
                    params,
                    parent: _,
                    scheduled_at,
                    retry_exp_backoff: _,
                    max_retries: _,
                    component_id,
                    metadata: _,
                    scheduled_by,
                } => grpc::execution_event::Event::Created(grpc::execution_event::Created {
                    params: to_any(
                        params,
                        format!("urn:obelisk:json:params:{ffqn}"),
                    ),
                    function_name: Some(grpc::FunctionName::from(ffqn)),
                    scheduled_at: Some(prost_wkt_types::Timestamp::from(scheduled_at)),
                    component_id: Some(component_id.into()),
                    scheduled_by: scheduled_by.map(|id| grpc::ExecutionId { id: id.to_string() }),
                }),
                ExecutionEventInner::Locked {
                    component_id,
                    executor_id: _,
                    run_id,
                    lock_expires_at,
                } => grpc::execution_event::Event::Locked(grpc::execution_event::Locked {
                    component_id: Some(component_id.into()),
                    run_id: run_id.to_string(),
                    lock_expires_at: Some(prost_wkt_types::Timestamp::from(lock_expires_at)),
                }),
                ExecutionEventInner::Unlocked { backoff_expires_at, reason } => {
                    grpc::execution_event::Event::Unlocked(grpc::execution_event::Unlocked {
                        backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                        reason: reason.to_string(),
                    })
                },
                ExecutionEventInner::TemporarilyFailed {
                    backoff_expires_at,
                    reason_full,
                    reason_inner: _,
                    detail,
                    http_client_traces
                } => grpc::execution_event::Event::TemporarilyFailed(grpc::execution_event::TemporarilyFailed {
                    reason: reason_full.to_string(),
                    detail,
                    backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                    http_client_traces: http_client_traces.unwrap_or_default().into_iter().map(grpc::HttpClientTrace::from).collect(),
                }),
                ExecutionEventInner::TemporarilyTimedOut { backoff_expires_at, http_client_traces } => {
                    grpc::execution_event::Event::TemporarilyTimedOut(grpc::execution_event::TemporarilyTimedOut {
                        backoff_expires_at: Some(prost_wkt_types::Timestamp::from(backoff_expires_at)),
                        http_client_traces: http_client_traces
                            .unwrap_or_default()
                            .into_iter()
                            .map(grpc::HttpClientTrace::from)
                            .collect(),
                    })
                },
                ExecutionEventInner::Finished { result, http_client_traces } => grpc::execution_event::Event::Finished(grpc::execution_event::Finished {
                    result_detail: Some(
                        grpc::ResultDetail::from(result)
                    ),
                    http_client_traces: http_client_traces.unwrap_or_default().into_iter().map(grpc::HttpClientTrace::from).collect(),

                }),
                ExecutionEventInner::HistoryEvent { event } => grpc::execution_event::Event::HistoryVariant(grpc::execution_event::HistoryEvent {
                    event: Some(match event {
                        HistoryEvent::Persist { value, kind } => grpc::execution_event::history_event::Event::Persist(grpc::execution_event::history_event::Persist {
                            data: Some(prost_wkt_types::Any {
                                type_url: "unknown".to_string(),
                                value,
                            }),
                            kind: Some(grpc::execution_event::history_event::persist::PersistKind { variant: Some(match kind {
                                concepts::storage::PersistKind::RandomU64 { .. } =>
                                grpc::execution_event::history_event::persist::persist_kind::Variant::RandomU64(
                                    grpc::execution_event::history_event::persist::persist_kind::RandomU64 {  }),
                                concepts::storage::PersistKind::RandomString { .. } =>
                                grpc::execution_event::history_event::persist::persist_kind::Variant::RandomString(
                                    grpc::execution_event::history_event::persist::persist_kind::RandomString {  }),
                            }) })
                        }),
                        HistoryEvent::JoinSetCreate { join_set_id, closing_strategy } =>
                            grpc::execution_event::history_event::Event::JoinSetCreated(grpc::execution_event::history_event::JoinSetCreated {
                                join_set_id: Some(join_set_id.into()),
                                closing_strategy: match closing_strategy {
                                    ClosingStrategy::Complete => grpc::execution_event::history_event::join_set_created::ClosingStrategy::Complete.into()
                                }
                            }),
                        HistoryEvent::JoinSetRequest { join_set_id, request } => grpc::execution_event::history_event::Event::JoinSetRequest(grpc::execution_event::history_event::JoinSetRequest {
                            join_set_id: Some(join_set_id.into()),
                            join_set_request: match request {
                                JoinSetRequest::DelayRequest { delay_id, expires_at } => {
                                    Some(grpc::execution_event::history_event::join_set_request::JoinSetRequest::DelayRequest(
                                        grpc::execution_event::history_event::join_set_request::DelayRequest {
                                            delay_id: Some(delay_id.into()),
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
                            join_set_id: Some(join_set_id.into()),
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

pub mod response {
    use crate::command::grpc;
    use concepts::{
        ExecutionId,
        storage::{JoinSetResponse, JoinSetResponseEventOuter, ResponseWithCursor},
    };
    use prost_wkt_types::Timestamp;

    impl From<ResponseWithCursor> for grpc::ResponseWithCursor {
        fn from(response: ResponseWithCursor) -> Self {
            Self {
                event: Some(response.event.into()),
                cursor: response.cursor,
            }
        }
    }

    impl From<JoinSetResponseEventOuter> for grpc::JoinSetResponseEvent {
        fn from(event: JoinSetResponseEventOuter) -> Self {
            let response = match event.event.event {
                JoinSetResponse::DelayFinished { delay_id } => {
                    grpc::join_set_response_event::Response::DelayFinished(
                        grpc::join_set_response_event::DelayFinished {
                            delay_id: Some(delay_id.into()),
                        },
                    )
                }
                JoinSetResponse::ChildExecutionFinished {
                    child_execution_id,
                    finished_version: _,
                    result,
                } => grpc::join_set_response_event::Response::ChildExecutionFinished(
                    grpc::join_set_response_event::ChildExecutionFinished {
                        child_execution_id: Some(ExecutionId::Derived(child_execution_id).into()),
                        result_detail: Some(result.into()),
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
    use crate::command::grpc;

    impl From<concepts::storage::FrameInfo> for grpc::FrameInfo {
        fn from(frame: concepts::storage::FrameInfo) -> Self {
            Self {
                module: frame.module,
                func_name: frame.func_name,
                symbols: frame.symbols.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl From<concepts::storage::FrameSymbol> for grpc::FrameSymbol {
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

impl From<HttpClientTrace> for grpc::HttpClientTrace {
    fn from(value: HttpClientTrace) -> Self {
        Self {
            sent_at: Some(value.req.sent_at.into()),
            uri: value.req.uri,
            method: value.req.method,
            finished_at: value.resp.as_ref().map(|resp| resp.finished_at.into()),
            result: value.resp.map(|resp| match resp.status {
                Ok(status) => grpc::http_client_trace::Result::Status(u32::from(status)),
                Err(err) => grpc::http_client_trace::Result::Error(err),
            }),
        }
    }
}
