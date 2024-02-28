pub(crate) mod inmemory_dao;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::Params;
use concepts::{ExecutionId, FunctionFqn};

use crate::FinishedExecutionResult;

pub type Version = usize;

pub type ExecutorName = Arc<String>;

#[derive(Clone, Debug, derive_more::Display, PartialEq, Eq)]
#[display(fmt = "{event}")]
pub(crate) struct ExecutionEvent<ID: ExecutionId> {
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) event: ExecutionEventInner<ID>,
}

#[derive(Clone, Debug, derive_more::Display, PartialEq, Eq)]
pub(crate) enum ExecutionEventInner<ID: ExecutionId> {
    /// Created by an external system or a scheduler when requesting a child execution or
    /// an executor when continuing as new `FinishedExecutionError`::`ContinueAsNew`,`CancelledWithNew` .
    // After optional expiry(`scheduled_at`) interpreted as pending.
    #[display(fmt = "Created({ffqn})")]
    Created {
        // execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
        parent: Option<ID>,
        scheduled_at: Option<DateTime<Utc>>,
    },
    // Created by an executor.
    // Either immediately followed by an execution request by an executor or
    // after expiry immediately followed by WaitingForExecutor by a scheduler.
    #[display(fmt = "Locked")]
    Locked {
        executor_name: ExecutorName,
        expires_at: DateTime<Utc>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentFailure")]
    IntermittentFailure {
        expires_at: DateTime<Utc>,
        reason: String,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentTimeout")]
    IntermittentTimeout { expires_at: DateTime<Utc> },
    // Created by the executor holding last lock.
    // Processed by a scheduler if a parent execution needs to be notified,
    // also when
    #[display(fmt = "Finished")]
    Finished { result: FinishedExecutionResult<ID> },
    // Created by an external system or a scheduler during a race.
    // Processed by the executor holding the last Lock.
    // Imediately followed by Finished by a scheduler.
    #[display(fmt = "CancelRequest")]
    CancelRequest,

    #[display(fmt = "EventHistory({event})")]
    EventHistory { event: EventHistory<ID> },
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display)]
pub(crate) enum EventHistory<ID: ExecutionId> {
    // Created by the executor holding last lock.
    // Interpreted as lock being ended.
    Yield,
    #[display(fmt = "Persist")]
    // Created by the executor holding last lock.
    // Does not block the execution
    Persist {
        value: Vec<u8>,
    },
    // Created by the executor holding last lock.
    // Does not block the execution
    JoinSet {
        joinset_id: ID,
    },
    // Created by an executor
    // Processed by a scheduler
    // Later followed by DelayFinished
    #[display(fmt = "DelayedUntilAsyncRequest({joinset_id})")]
    DelayedUntilAsyncRequest {
        joinset_id: ID,
        delay_id: ID,
        expires_at: DateTime<Utc>,
    },
    // Created by an executor
    // Processed by a scheduler - new execution must be scheduled
    // Immediately followed by ChildExecutionRequested
    #[display(fmt = "ChildExecutionAsyncRequest({joinset_id})")]
    ChildExecutionAsyncRequest {
        joinset_id: ID,
        child_execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
    },
    // Created by a scheduler right after.
    // Processed by other schedulers
    ChildExecutionRequested {
        child_execution_id: ID,
    },
    // Created by the executor.
    // Executor continues without blocking.
    JoinNextFetched {
        joinset_id: ID,
    },
    #[display(fmt = "EventHistoryAsyncResponse({joinset_id})")]
    EventHistoryAsyncResponse {
        joinset_id: ID,
        response: EventHistoryAsyncResponse<ID>,
    },
    // Created by the executor.
    // Execution is blocked, until the next response of the
    // joinset arrives. After that, a scheduler issues `WaitingForExecutor`.
    JoinNextBlocking {
        joinset_id: ID,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EventHistoryAsyncResponse<ID: ExecutionId> {
    // Created by a scheduler sometime after DelayedUntilAsyncRequest.
    DelayFinishedAsyncResponse {
        delay_id: ID,
    },
    // Created by a scheduler sometime after ChildExecutionRequested.
    ChildExecutionAsyncResponse {
        child_execution_id: ID,
        result: FinishedExecutionResult<ID>,
    },
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

pub type AppendResponse = Version;
pub type PendingExecution<ID> = (ID, Version, Params, Option<DateTime<Utc>>);
pub type ExecutionHistory<ID> = (Vec<ExecutionEvent<ID>>, Version);
pub type LockResponse<ID> = (Vec<EventHistory<ID>>, Version);
pub type LockPendingResponse<ID> = Vec<(
    ID,
    Version,
    Params,
    Vec<EventHistory<ID>>,
    Option<DateTime<Utc>>,
)>;

#[async_trait]
pub trait DbConnection<ID: ExecutionId> {
    async fn lock_pending(
        &self,
        batch_size: usize,
        fetch_expiring_before: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        lock_created_at: DateTime<Utc>,
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse<ID>, DbConnectionError>;

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
    ) -> Result<AppendResponse, DbError>;

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
