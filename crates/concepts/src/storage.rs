use crate::prefixed_ulid::DelayId;
use crate::prefixed_ulid::ExecutorId;
use crate::prefixed_ulid::JoinSetId;
use crate::prefixed_ulid::RunId;
use crate::ExecutionId;
use crate::FinishedExecutionResult;
use crate::FunctionFqn;
use crate::Params;
use crate::StrVariant;
use crate::SupportedFunctionResult;
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use std::time::Duration;
use strum::IntoStaticStr;
use tracing::debug;
use tracing::trace;

/// Remote client representation of the execution journal.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionLog {
    pub execution_id: ExecutionId,
    pub events: Vec<ExecutionEvent>,
    pub version: Version,
    pub pending_state: PendingState,
}

impl ExecutionLog {
    fn already_tried_count(&self) -> u32 {
        u32::try_from(
            self.events
                .iter()
                .filter(|event| event.event.is_intermittent_event())
                .count(),
        )
        .unwrap()
    }

    #[must_use]
    pub fn can_be_retried_after(&self) -> Option<Duration> {
        let already_tried_count = self.already_tried_count();
        if already_tried_count < self.max_retries() + 1 {
            // TODO: Add test for number of retries
            let duration = self.retry_exp_backoff() * 2_u32.saturating_pow(already_tried_count);
            Some(duration)
        } else {
            None
        }
    }

    #[must_use]
    pub fn retry_exp_backoff(&self) -> Duration {
        assert_matches!(self.events.first(), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { retry_exp_backoff, .. },
            ..
        }) => *retry_exp_backoff)
    }

    #[must_use]
    pub fn max_retries(&self) -> u32 {
        assert_matches!(self.events.first(), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { max_retries, .. },
            ..
        }) => *max_retries)
    }

    #[must_use]
    pub fn ffqn(&self) -> &FunctionFqn {
        assert_matches!(self.events.first(), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { ffqn, .. },
            ..
        }) => ffqn)
    }

    #[must_use]
    pub fn parent(&self) -> Option<(ExecutionId, JoinSetId)> {
        assert_matches!(self.events.first(), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { parent, .. },
            ..
        }) => *parent)
    }

    #[must_use]
    pub fn last_event(&self) -> &ExecutionEvent {
        self.events.last().expect("must contain at least one event")
    }

    #[must_use]
    pub fn finished_result(&self) -> Option<&FinishedExecutionResult> {
        if let ExecutionEvent {
            event: ExecutionEventInner::Finished { result, .. },
            ..
        } = self.last_event()
        {
            Some(result)
        } else {
            None
        }
    }

    pub fn event_history(&self) -> impl Iterator<Item = HistoryEvent> + '_ {
        self.events.iter().filter_map(|event| {
            if let ExecutionEventInner::HistoryEvent { event: eh, .. } = &event.event {
                Some(eh.clone())
            } else {
                None
            }
        })
    }

    pub fn join_set_requests(
        &self,
        join_set_id: JoinSetId,
    ) -> impl Iterator<Item = &JoinSetRequest> {
        self.events
            .iter()
            .filter_map(move |event| match &event.event {
                ExecutionEventInner::HistoryEvent {
                    event:
                        HistoryEvent::JoinSetRequest {
                            join_set_id: found,
                            request,
                        },
                    ..
                } if join_set_id == *found => Some(request),
                _ => None,
            })
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, derive_more::Display, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct Version(pub usize);
impl Version {
    #[must_use]
    pub fn new(arg: usize) -> Self {
        Self(arg)
    }
}

#[derive(
    Clone, Debug, derive_more::Display, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[display(fmt = "{event}")]
pub struct ExecutionEvent {
    // TODO: Rename to ExecutionEventRow
    pub created_at: DateTime<Utc>,
    pub event: ExecutionEventInner,
}

pub const DUMMY_CREATED: ExecutionEventInner = ExecutionEventInner::Created {
    ffqn: FunctionFqn::new_static("", ""),
    params: Params::Empty,
    parent: None,
    scheduled_at: None,
    retry_exp_backoff: Duration::ZERO,
    max_retries: 0,
};
pub const DUMMY_HISTORY_EVENT: ExecutionEventInner = ExecutionEventInner::HistoryEvent {
    event: HistoryEvent::Yield,
};
pub const DUMMY_INTERMITTENT_TIMEOUT: ExecutionEventInner =
    ExecutionEventInner::IntermittentTimeout {
        expires_at: DateTime::from_timestamp_nanos(0),
    };
pub const DUMMY_INTERMITTENT_FAILURE: ExecutionEventInner =
    ExecutionEventInner::IntermittentFailure {
        expires_at: DateTime::from_timestamp_nanos(0),
        reason: StrVariant::Static(""),
    };

#[derive(
    Clone,
    Debug,
    derive_more::Display,
    PartialEq,
    Eq,
    arbitrary::Arbitrary,
    Serialize,
    Deserialize,
    IntoStaticStr,
)]
pub enum ExecutionEventInner {
    // TODO: Rename to ExecutionEvent
    /// Created by an external system or a scheduler when requesting a child execution or
    /// an executor when continuing as new `FinishedExecutionError`::`ContinueAsNew`,`CancelledWithNew` .
    // After optional expiry(`scheduled_at`) interpreted as pending.
    #[display(fmt = "Created({ffqn}, `{scheduled_at:?}`)")]
    Created {
        ffqn: FunctionFqn,
        #[arbitrary(default)]
        params: Params,
        parent: Option<(ExecutionId, JoinSetId)>,
        scheduled_at: Option<DateTime<Utc>>,
        retry_exp_backoff: Duration,
        max_retries: u32,
    },
    // Created by an executor.
    // Either immediately followed by an execution request by an executor or
    // after expiry immediately followed by WaitingForExecutor by a scheduler.
    #[display(fmt = "Locked(`{lock_expires_at}`, {executor_id})")]
    Locked {
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentFailure(`{expires_at}`)")]
    IntermittentFailure {
        expires_at: DateTime<Utc>,
        #[arbitrary(value = StrVariant::Static("reason"))]
        reason: StrVariant,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentTimeout(`{expires_at}`)")]
    IntermittentTimeout { expires_at: DateTime<Utc> },
    // Created by the executor holding last lock.
    // Processed by a scheduler if a parent execution needs to be notified,
    // also when
    #[display(fmt = "Finished")]
    Finished {
        #[arbitrary(value = Ok(SupportedFunctionResult::None))]
        result: FinishedExecutionResult,
    },
    // Created by an external system or a scheduler during a race.
    // Processed by the executor holding the last Lock.
    // Imediately followed by Finished by a scheduler.
    #[display(fmt = "CancelRequest")]
    CancelRequest,

    #[display(fmt = "HistoryEvent({event})")]
    HistoryEvent { event: HistoryEvent },
}

impl ExecutionEventInner {
    #[must_use]
    pub fn is_intermittent_event(&self) -> bool {
        matches!(
            self,
            Self::IntermittentFailure { .. } | Self::IntermittentTimeout { .. }
        )
    }

    #[must_use]
    // TODO: Remove if finished_child_watcher is implemented
    pub fn appendable_without_version(&self) -> bool {
        matches!(
            self,
            Self::HistoryEvent {
                event: HistoryEvent::JoinSetResponse {
                    response: JoinSetResponse::ChildExecutionFinished { .. },
                    ..
                }
            }
        )
    }

    #[must_use]
    pub fn variant(&self) -> &'static str {
        Into::<&'static str>::into(self)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary, Serialize, Deserialize,
)]
pub enum HistoryEvent {
    /// Must be created by the executor in [`PendingState::Locked`].
    /// Returns execution to [`PendingState::PendingNow`] state.
    Yield,
    #[display(fmt = "Persist")]
    /// Must be created by the executor in [`PendingState::Locked`].
    Persist { value: Vec<u8> },
    /// Must be created by the executor in [`PendingState::Locked`].
    #[display(fmt = "JoinSet({join_set_id})")]
    JoinSet {
        join_set_id: JoinSetId,
        // TODO: add JoinSetKind (unordered, ordered)
    },
    #[display(fmt = "JoinSetRequest({join_set_id})")]
    JoinSetRequest {
        join_set_id: JoinSetId,
        request: JoinSetRequest,
    },
    /// Moves the execution to [`PendingState::PendingNow`] if it is currently blocked on `JoinNextBlocking`.
    #[display(fmt = "AsyncResponse({join_set_id}, {response})")]
    JoinSetResponse {
        join_set_id: JoinSetId,
        response: JoinSetResponse,
    },
    /// Must be created by the executor in [`PendingState::Locked`].
    /// Pending state is set to [`PendingState::BlockedByJoinSet`].
    /// When the response arrives at `resp_time`:
    /// The execution is [`PendingState::PendingAt`]`(max(resp_time, lock_expires_at)`, so that the
    /// original executor can continue. After the expiry any executor can continue without
    /// marking the execution as timed out.
    #[display(fmt = "JoinNext({join_set_id})")]
    JoinNext {
        join_set_id: JoinSetId,
        /// Set to a future time if the executor is keeping the execution warm waiting for the result.
        lock_expires_at: DateTime<Utc>,
    },
}

#[derive(
    Clone, Debug, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary, Serialize, Deserialize,
)]
pub enum JoinSetRequest {
    // Must be created by the executor in `PendingState::Locked`.
    #[display(fmt = "DelayRequest({delay_id})")]
    DelayRequest {
        delay_id: DelayId,
        expires_at: DateTime<Utc>,
    },
    // Must be created by the executor in `PendingState::Locked`.
    #[display(fmt = "ChildExecutionRequest({child_execution_id})")]
    ChildExecutionRequest { child_execution_id: ExecutionId },
}

#[derive(
    Clone, Debug, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary, Serialize, Deserialize,
)]
pub enum JoinSetResponse {
    DelayFinished {
        delay_id: DelayId,
    },
    #[display(fmt = "ChildExecutionAsyncResponse({child_execution_id})")]
    ChildExecutionFinished {
        child_execution_id: ExecutionId,
        #[arbitrary(value = Ok(SupportedFunctionResult::None))]
        result: FinishedExecutionResult,
    },
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum DbConnectionError {
    #[error("send error")]
    SendError,
    #[error("receive error")]
    RecvError,
    #[error("timeout")]
    Timeout,
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum SpecificError {
    #[error("validation failed: {0}")]
    ValidationFailed(StrVariant),
    #[error("version mismatch")]
    VersionMismatch,
    #[error("not found")]
    NotFound,
    #[error("consistency error: `{0}`")]
    ConsistencyError(StrVariant),
}

#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
pub enum DbError {
    #[error(transparent)]
    Connection(#[from] DbConnectionError),
    #[error(transparent)]
    Specific(SpecificError),
}

pub type AppendResponse = Version;
pub type PendingExecution = (ExecutionId, Version, Params, Option<DateTime<Utc>>);
pub type LockResponse = (Vec<HistoryEvent>, Version);

#[derive(Debug, Clone)]
pub struct LockedExecution {
    pub execution_id: ExecutionId,
    pub run_id: RunId,
    pub version: Version,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub event_history: Vec<HistoryEvent>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub retry_exp_backoff: Duration,
    pub max_retries: u32,
}
pub type LockPendingResponse = Vec<LockedExecution>;
pub type AppendBatchResponse = Version;

#[derive(Debug, Clone, derive_more::Display, Serialize, Deserialize)]
#[display(fmt = "{event}")]
pub struct AppendRequest {
    pub created_at: DateTime<Utc>,
    pub event: ExecutionEventInner,
}

pub type AppendBatch = Vec<AppendRequest>;

#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub created_at: DateTime<Utc>,
    pub execution_id: ExecutionId,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub parent: Option<(ExecutionId, JoinSetId)>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub retry_exp_backoff: Duration,
    pub max_retries: u32,
}

impl From<CreateRequest> for ExecutionEventInner {
    fn from(value: CreateRequest) -> Self {
        Self::Created {
            ffqn: value.ffqn,
            params: value.params,
            parent: value.parent,
            scheduled_at: value.scheduled_at,
            retry_exp_backoff: value.retry_exp_backoff,
            max_retries: value.max_retries,
        }
    }
}

pub trait DbPool<DB: DbConnection>: Send + Sync + Clone {
    fn connection(&self) -> DB;
}

#[async_trait]
pub trait DbConnection: Send + Sync {
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError>;

    /// Specialized `append` which returns the event history.
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError>;

    /// Append a single event to an existing execution log
    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Option<Version>,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError>;

    /// Append one or more events to an existing execution log
    async fn append_batch(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError>;

    /// Append one or more events to the parent execution log, and create new child execution log.
    async fn append_batch_create_child(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        child_req: CreateRequest,
    ) -> Result<AppendBatchResponse, DbError>;

    async fn append_batch_respond_to_parent(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        parent: (ExecutionId, AppendRequest),
    ) -> Result<AppendBatchResponse, DbError>;

    /// Get execution log.
    async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionLog, DbError>;

    /// Get currently expired locks and async timers (delay requests)
    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError>;

    /// Create a new execution log
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError>;

    async fn wait_for_finished_result(
        &self,
        execution_id: ExecutionId,
        timeout: Option<Duration>,
    ) -> Result<FinishedExecutionResult, DbError> {
        let execution_log = self
            .wait_for_pending_state(execution_id, PendingState::Finished, timeout)
            .await?;
        Ok(execution_log
            .finished_result()
            .expect("pending state was checked")
            .clone())
    }

    async fn wait_for_pending_state(
        &self,
        execution_id: ExecutionId,
        expected_pending_state: PendingState,
        timeout: Option<Duration>,
    ) -> Result<ExecutionLog, DbError> {
        trace!(%execution_id, "Waiting for {expected_pending_state}");
        let fut = async move {
            loop {
                let execution_log = self.get(execution_id).await?;
                if execution_log.pending_state == expected_pending_state {
                    debug!(%execution_id, "Found: {expected_pending_state}");
                    return Ok(execution_log);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        if let Some(timeout) = timeout {
            tokio::select! {
                res = fut => res,
                () = tokio::time::sleep(timeout) => Err(DbError::Connection(DbConnectionError::Timeout))
            }
        } else {
            fut.await
        }
    }
}

pub async fn wait_for_pending_state_fn<T: Debug>(
    db_connection: &dyn DbConnection,
    execution_id: ExecutionId,
    prdicate: impl Fn(ExecutionLog) -> Option<T> + Send,
    timeout: Option<Duration>,
) -> Result<T, DbError> {
    trace!(%execution_id, "Waiting for predicate");
    let fut = async move {
        loop {
            let execution_log = db_connection.get(execution_id).await?;
            if let Some(t) = prdicate(execution_log) {
                debug!(%execution_id, "Found: {t:?}");
                return Ok(t);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    if let Some(timeout) = timeout {
        tokio::select! {
            res = fut => res,
            () = tokio::time::sleep(timeout) => Err(DbError::Connection(DbConnectionError::Timeout))
        }
    } else {
        fut.await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpiredTimer {
    Lock {
        execution_id: ExecutionId,
        version: Version,
        already_retried_count: u32,
        max_retries: u32,
        retry_exp_backoff: Duration,
    },
    AsyncDelay {
        execution_id: ExecutionId,
        version: Version,
        join_set_id: JoinSetId,
        delay_id: DelayId,
    },
}

#[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PendingState {
    PendingNow,
    #[display(fmt = "Locked(`{lock_expires_at}`, {executor_id}, {run_id})")]
    Locked {
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
    },
    #[display(fmt = "PendingAt(`{scheduled_at}`)")]
    PendingAt {
        scheduled_at: DateTime<Utc>,
    }, // e.g. created with a schedule, intermittent timeout/failure
    #[display(fmt = "BlockedByJoinSet({join_set_id},`{lock_expires_at}`)")]
    /// Caused by [`HistoryEvent::JoinNext`]
    BlockedByJoinSet {
        join_set_id: JoinSetId,
        /// See [`HistoryEvent::JoinNext::lock_expires_at`].
        lock_expires_at: DateTime<Utc>,
    },
    Finished,
}

impl PendingState {
    pub fn can_append_lock(
        &self,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockKind, SpecificError> {
        if lock_expires_at <= created_at {
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "invalid expiry date",
            )));
        }
        match self {
            PendingState::PendingNow => Ok(LockKind::CreatingNewLock), // ok to lock
            PendingState::PendingAt { scheduled_at } => {
                if *scheduled_at <= created_at {
                    // pending now, ok to lock
                    Ok(LockKind::CreatingNewLock)
                } else {
                    Err(SpecificError::ValidationFailed(StrVariant::Static(
                        "cannot lock, not yet pending",
                    )))
                }
            }
            PendingState::Locked {
                executor_id: current_pending_state_executor_id,
                run_id: current_pending_state_run_id,
                ..
            } => {
                if executor_id == *current_pending_state_executor_id
                    && run_id == *current_pending_state_run_id
                {
                    // Original executor is extending the lock.
                    Ok(LockKind::Extending)
                } else {
                    Err(SpecificError::ValidationFailed(StrVariant::Static(
                        "cannot lock, already locked",
                    )))
                }
            }
            PendingState::BlockedByJoinSet { .. } => Err(SpecificError::ValidationFailed(
                StrVariant::Static("cannot append Locked event when in BlockedByJoinSet state"),
            )),
            PendingState::Finished => Err(SpecificError::ValidationFailed(StrVariant::Static(
                "already finished",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    Extending,
    CreatingNewLock,
}
