use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn};
use concepts::{Params, SupportedFunctionResult};

mod executor;
mod storage;

#[cfg(test)]
mod testing;

mod worker {
    use self::storage::inmemory_dao::{
        api::Version, EventHistory, ExecutionEvent, ExecutionEventInner, ExecutorName,
    };
    use super::*;

    #[async_trait]
    pub trait Worker<ID: ExecutionId> {
        async fn run(
            &self,
            execution_id: ID,
            params: Params,
            event_history: Vec<EventHistory<ID>>,
            version: Version,
            lock_expires_at: DateTime<Utc>,
        ) -> Result<Version, DbError>;
    }

    #[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
    pub enum DbConnectionError {
        #[error("send error")]
        SendError,
        #[error("receive error")]
        RecvError,
    }

    #[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
    pub enum RowSpecificError {
        #[error("validation failed: {0}")]
        ValidationFailed(&'static str),
        #[error("version mismatch")]
        VersionMismatch,
        #[error("not found")]
        NotFound,
    }

    #[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
    pub enum DbError {
        #[error(transparent)]
        Connection(DbConnectionError),
        #[error(transparent)]
        RowSpecific(RowSpecificError),
    }

    pub type PendingExecution<ID> = (ID, Version, Params, Option<DateTime<Utc>>);
    pub type ExecutionHistory<ID> = (Vec<ExecutionEvent<ID>>, Version);
    pub type LockResponse<ID> = (Vec<EventHistory<ID>>, Version);
    pub type AppendResponse = Version;

    #[async_trait]
    pub trait DbConnection<ID: ExecutionId> {
        // TODO: fetch_and_lock
        async fn fetch_pending(
            &self,
            batch_size: usize,
            expiring_before: DateTime<Utc>,
            ffqns: Vec<FunctionFqn>,
        ) -> Result<Vec<PendingExecution<ID>>, DbConnectionError>;

        /// Specialized `append` which does not require a version.
        async fn create(
            &self,
            execution_id: ID,
            created_at: DateTime<Utc>,
            ffqn: FunctionFqn,
            params: Params,
            parent: Option<ID>,
            scheduled_at: Option<DateTime<Utc>>,
        ) -> Result<AppendResponse, DbError> {
            let event = ExecutionEvent {
                created_at,
                event: ExecutionEventInner::Created {
                    ffqn,
                    params,
                    parent,
                    scheduled_at,
                },
            };
            self.append(execution_id, Version::default(), event).await
        }

        /// Specialized `append` which returns the event history.
        async fn lock(
            &self,
            created_at: DateTime<Utc>,
            execution_id: ID,
            version: Version,
            executor_name: ExecutorName,
            expires_at: DateTime<Utc>,
        ) -> Result<LockResponse<ID>, DbError>;

        async fn append(
            &self,
            execution_id: ID,
            version: Version,
            event: ExecutionEvent<ID>,
        ) -> Result<AppendResponse, DbError>;

        async fn get(&self, execution_id: ID) -> Result<ExecutionHistory<ID>, DbError>;
    }

    #[derive(Debug)]
    pub(crate) enum MaybeReplayResponse<E: ExecutionId> {
        ReplayResponse(ReplayResponse<E>),
        MissingResponse,
    }

    #[derive(Debug)]
    pub(crate) enum ReplayResponse<E: ExecutionId> {
        Completed,
        CompletedWithResult {
            child_execution_id: E,
            result: FinishedExecutionResult<E>,
        },
    }
}

type FinishedExecutionResult<ID> = Result<SupportedFunctionResult, FinishedExecutionError<ID>>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum FinishedExecutionError<ID: ExecutionId> {
    #[error("permanent timeout")]
    PermanentTimeout,
    // TODO PermanentFailure when error retries are implemented
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(String),
    #[error("uncategorized error")]
    UncategorizedError, //TODO: add reason?
    #[error("cancelled, reason: `{0}`")]
    Cancelled(String),
    #[error("continuing as {execution_id}")]
    ContinueAsNew {
        // TODO: Move to the OK part of the result
        execution_id: ID,
    },
    #[error("cancelled and starting {execution_id}")]
    CancelledWithNew { execution_id: ID },
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum ExecutionStatusInfo<ID: ExecutionId> {
    Pending,
    Enqueued,
    DelayedUntil(DateTime<Utc>),
    Blocked,
    IntermittentTimeout(DateTime<Utc>),
    Finished(FinishedExecutionResult<ID>),
}
impl<ID: ExecutionId> ExecutionStatusInfo<ID> {
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished(_))
    }
}

pub(crate) mod time {
    use chrono::DateTime;
    use chrono::Utc;

    cfg_if::cfg_if! {
        if #[cfg(all(test, madsim))] {
            pub(crate) fn now() -> DateTime<Utc> {
                DateTime::from(madsim::time::TimeHandle::current().now_time())
            }
        } else {
            pub(crate) fn now() -> DateTime<Utc> {
                Utc::now()
            }
        }
    }
}
