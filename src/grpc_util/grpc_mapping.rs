use crate::command::grpc::{self};
use anyhow::anyhow;
use concepts::{
    prefixed_ulid::{JoinSetId, RunId},
    storage::{
        DbError, Pagination, PendingState, PendingStateFinished, PendingStateFinishedError,
        PendingStateFinishedResultKind, SpecificError,
    },
    ConfigId, ConfigIdType, ExecutionId, FunctionFqn,
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

impl TryFrom<grpc::list_executions_request::Pagination> for Pagination<ExecutionId> {
    type Error = tonic::Status;
    fn try_from(value: grpc::list_executions_request::Pagination) -> Result<Self, Self::Error> {
        Ok(match value {
            grpc::list_executions_request::Pagination::OlderThan(
                grpc::list_executions_request::OlderThan {
                    previous,
                    cursor: Some(cursor),
                    including_cursor,
                },
            ) => Pagination::OlderThan {
                previous,
                cursor: Some(ExecutionId::try_from(cursor)?),
                including_cursor,
            },
            grpc::list_executions_request::Pagination::Latest(
                grpc::list_executions_request::Latest { latest: previous },
            )
            | grpc::list_executions_request::Pagination::OlderThan(
                grpc::list_executions_request::OlderThan {
                    previous,
                    cursor: None,
                    including_cursor: _,
                },
            ) => Pagination::OlderThan {
                previous,
                cursor: None,
                including_cursor: false,
            },
            grpc::list_executions_request::Pagination::NewerThan(
                grpc::list_executions_request::NewerThan {
                    next,
                    cursor: Some(cursor),
                    including_cursor,
                },
            ) => Pagination::NewerThan {
                next,
                cursor: Some(ExecutionId::try_from(cursor)?),
                including_cursor,
            },
            grpc::list_executions_request::Pagination::Oldest(
                grpc::list_executions_request::Oldest { oldest: next },
            )
            | grpc::list_executions_request::Pagination::NewerThan(
                grpc::list_executions_request::NewerThan {
                    next,
                    cursor: None,
                    including_cursor: _,
                },
            ) => Pagination::NewerThan {
                next,
                cursor: None,
                including_cursor: false,
            },
        })
    }
}
