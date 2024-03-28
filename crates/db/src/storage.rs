pub mod inmemory_dao;

use self::journal::PendingState;
use crate::ExecutionHistory;
use crate::FinishedExecutionResult;
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::DelayId;
use concepts::prefixed_ulid::ExecutorId;
use concepts::prefixed_ulid::JoinSetId;
use concepts::prefixed_ulid::RunId;
use concepts::ExecutionId;
use concepts::FunctionFqn;
use concepts::Params;
use concepts::SupportedFunctionResult;
use std::borrow::Cow;
use std::time::Duration;
use tracing_unwrap::OptionExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
pub struct Version(usize);
impl Version {
    #[cfg(any(test, feature = "test"))]
    pub fn new(arg: usize) -> Self {
        Self(arg)
    }
}

#[derive(Clone, Debug, derive_more::Display, PartialEq, Eq)]
#[display(fmt = "{event}")]
pub struct ExecutionEvent {
    pub created_at: DateTime<Utc>,
    pub event: ExecutionEventInner,
}

#[derive(Clone, Debug, derive_more::Display, PartialEq, Eq, arbitrary::Arbitrary)]
pub enum ExecutionEventInner {
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
        #[arbitrary(value = Cow::Borrowed("reason"))]
        reason: Cow<'static, str>,
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
    fn appendable_only_in_lock(&self) -> bool {
        match self {
            Self::Locked { .. }
            | Self::IntermittentFailure { .. }
            | Self::IntermittentTimeout { .. }
            | Self::Finished { .. } => true,
            Self::HistoryEvent { event } => event.appendable_only_in_lock(),
            _ => false,
        }
    }

    pub fn is_retry(&self) -> bool {
        matches!(
            self,
            Self::IntermittentFailure { .. } | Self::IntermittentTimeout { .. }
        )
    }

    fn appendable_without_version(&self) -> bool {
        matches!(
            self,
            Self::HistoryEvent {
                event: HistoryEvent::AsyncResponse { .. }
            } | Self::Created { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary)]
pub enum HistoryEvent {
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    // Returns execution to `PotentiallyPending::PendingNow` state.
    Yield,
    #[display(fmt = "Persist")]
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    Persist {
        value: Vec<u8>,
    },
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    JoinSet {
        join_set_id: JoinSetId,
    },
    // JoinSet entry that will be unblocked by DelayFinishedAsyncResponse.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    #[display(fmt = "DelayedUntilAsyncRequest({join_set_id})")]
    DelayedUntilAsyncRequest {
        join_set_id: JoinSetId,
        delay_id: DelayId,
        expires_at: DateTime<Utc>,
    },
    // JoinSet entry that will be unblocked by ChildExecutionRequested.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    #[display(fmt = "ChildExecutionAsyncRequest({join_set_id})")]
    ChildExecutionAsyncRequest {
        join_set_id: JoinSetId,
        child_execution_id: ExecutionId,
    },
    // Execution continues without blocking as the next pending response is in the journal.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    JoinNextFetched {
        join_set_id: JoinSetId,
    },
    // Moves the execution to `PotentiallyPending::PendingNow` if it is currently blocked on `JoinNextBlocking`.
    #[display(fmt = "AsyncResponse({join_set_id}, {response})")]
    AsyncResponse {
        join_set_id: JoinSetId,
        response: AsyncResponse,
    },
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    // Execution is `PotentiallyPending::BlockedByJoinSet` until the next response of the JoinSet arrives.
    JoinNextBlocking {
        join_set_id: JoinSetId,
    },
}

impl HistoryEvent {
    fn appendable_only_in_lock(&self) -> bool {
        !matches!(self, Self::AsyncResponse { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary)]
pub enum AsyncResponse {
    // Created by a scheduler sometime after DelayedUntilAsyncRequest.
    DelayFinishedAsyncResponse {
        delay_id: DelayId,
    },
    // Created by an executor after ChildExecutionRequested.
    #[display(fmt = "ChildExecutionAsyncResponse({child_execution_id})")]
    ChildExecutionAsyncResponse {
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
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum SpecificError {
    #[error("validation failed: {0}")]
    ValidationFailed(Cow<'static, str>),
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
pub type CleanupExpiredLocks = usize; // number of expired locks
pub type AppendBatchResponse = AppendResponse;

#[derive(Debug, Clone, derive_more::Display)]
#[display(fmt = "{event}")]
pub struct AppendRequest {
    pub created_at: DateTime<Utc>,
    pub event: ExecutionEventInner,
}

#[async_trait]
pub trait DbConnection: Send + 'static + Clone + Send + Sync {
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbConnectionError>;

    /// Specialized `append` which does not require a version.
    async fn create(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        parent: Option<(ExecutionId, JoinSetId)>,
        scheduled_at: Option<DateTime<Utc>>,
        retry_exp_backoff: Duration,
        max_retries: u32,
    ) -> Result<AppendResponse, DbError> {
        let event = ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
        };
        self.append(execution_id, None, AppendRequest { event, created_at })
            .await
    }

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

    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Option<Version>,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError>;

    async fn append_batch(
        &self,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Option<Version>,
    ) -> Result<AppendBatchResponse, DbError>;

    async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionHistory, DbError>;

    async fn wait_for_finished_result(
        // FIXME timeout
        &self,
        execution_id: ExecutionId,
    ) -> Result<FinishedExecutionResult, DbError> {
        let ExecutionHistory { mut events, .. } = self
            .wait_for_pending_state(execution_id, PendingState::Finished)
            .await?;
        Ok(
            assert_matches!(events.pop().expect_or_log("must not be empty"),
            ExecutionEvent { event: ExecutionEventInner::Finished { result, .. } ,..} => result),
        )
    }

    async fn wait_for_pending_state(
        // FIXME timeout
        &self,
        execution_id: ExecutionId,
        expected_pending_state: PendingState,
    ) -> Result<ExecutionHistory, DbError> {
        loop {
            let execution_history = self.get(execution_id.clone()).await?;
            if execution_history.pending_state == expected_pending_state {
                return Ok(execution_history);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn cleanup_expired_locks(
        &self,
        now: DateTime<Utc>,
    ) -> Result<CleanupExpiredLocks, DbConnectionError>;
}

pub mod journal {
    use super::{ExecutionEvent, ExecutionEventInner, ExecutionId, HistoryEvent};
    use crate::storage::{ExecutionHistory, SpecificError, Version};
    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use concepts::{
        prefixed_ulid::{ExecutorId, JoinSetId, RunId},
        FunctionFqn, Params,
    };
    use std::{borrow::Cow, collections::VecDeque, time::Duration};
    use tracing_unwrap::OptionExt;

    #[derive(Debug)]
    pub(crate) struct ExecutionJournal {
        execution_id: ExecutionId,
        pub(crate) pending_state: PendingState,
        execution_events: VecDeque<ExecutionEvent>,
    }

    impl ExecutionJournal {
        pub(crate) fn new(
            execution_id: ExecutionId,
            ffqn: FunctionFqn,
            params: Params,
            scheduled_at: Option<DateTime<Utc>>,
            parent: Option<(ExecutionId, JoinSetId)>,
            created_at: DateTime<Utc>,
            retry_exp_backoff: Duration,
            max_retries: u32,
        ) -> Self {
            let pending_state = match scheduled_at {
                Some(pending_at) => PendingState::PendingAt(pending_at),
                None => PendingState::PendingNow,
            };
            let event = ExecutionEvent {
                event: ExecutionEventInner::Created {
                    ffqn,
                    params,
                    scheduled_at,
                    parent,
                    retry_exp_backoff,
                    max_retries,
                },
                created_at,
            };
            Self {
                execution_id,
                pending_state,
                execution_events: VecDeque::from([event]),
            }
        }

        pub(crate) fn len(&self) -> usize {
            self.execution_events.len()
        }

        pub(crate) fn ffqn(&self) -> &FunctionFqn {
            match self.execution_events.get(0).unwrap() {
                ExecutionEvent {
                    event: ExecutionEventInner::Created { ffqn, .. },
                    ..
                } => ffqn,
                _ => panic!("first event must be `Created`"),
            }
        }

        pub(crate) fn version(&self) -> Version {
            Version(self.execution_events.len())
        }

        pub(crate) fn execution_id(&self) -> &ExecutionId {
            &self.execution_id
        }

        pub(crate) fn append(
            &mut self,
            created_at: DateTime<Utc>,
            event: ExecutionEventInner,
        ) -> Result<Version, SpecificError> {
            if self.pending_state == PendingState::Finished {
                return Err(SpecificError::ValidationFailed(Cow::Borrowed(
                    "already finished",
                )));
            }

            if let ExecutionEventInner::Locked {
                executor_id,
                lock_expires_at,
                run_id,
            } = &event
            {
                if *lock_expires_at <= created_at {
                    return Err(SpecificError::ValidationFailed(Cow::Borrowed(
                        "invalid expiry date",
                    )));
                }
                match &self.pending_state {
                    PendingState::PendingNow => {} // ok to lock
                    PendingState::PendingAt(pending_start) => {
                        if *pending_start <= created_at {
                            // pending now, ok to lock
                        } else {
                            return Err(SpecificError::ValidationFailed(Cow::Owned(format!(
                                "cannot append {event} event, not yet pending"
                            ))));
                        }
                    }
                    PendingState::Locked {
                        executor_id: currently_locked_by,
                        lock_expires_at,
                        run_id: current_run_id,
                    } => {
                        if executor_id == currently_locked_by && run_id == current_run_id {
                            // we allow extending the lock
                        } else if *lock_expires_at <= created_at {
                            // we allow locking after the old lock expired
                        } else {
                            return Err(SpecificError::ValidationFailed(Cow::Owned(format!(
                                "cannot append {event}, already in {}",
                                self.pending_state
                            ))));
                        }
                    }
                    PendingState::BlockedByJoinSet => {
                        return Err(SpecificError::ValidationFailed(Cow::Borrowed(
                            "cannot append Locked event when in BlockedByJoinSet state",
                        )));
                    }
                    PendingState::Finished => {
                        unreachable!() // handled at the beginning of the function
                    }
                }
            }
            let locked_now = matches!(self.pending_state, PendingState::Locked { lock_expires_at,.. } if lock_expires_at > created_at);
            if locked_now && !event.appendable_only_in_lock() {
                return Err(SpecificError::ValidationFailed(Cow::Owned(format!(
                    "cannot append {event} event in {}",
                    self.pending_state
                ))));
            }
            self.execution_events
                .push_back(ExecutionEvent { event, created_at });
            // update the state
            self.pending_state = self.calculate_pending_state();
            Ok(self.version())
        }

        fn calculate_pending_state(&self) -> PendingState {
            self.execution_events
                .iter()
                .enumerate()
                .rev()
                .find_map(|(idx, event)| match (idx, &event.event) {
                    (
                        _,
                        ExecutionEventInner::Created {
                            scheduled_at: None, ..
                        },
                    ) => Some(PendingState::PendingNow),

                    (
                        _,
                        ExecutionEventInner::Created {
                            scheduled_at: Some(scheduled_at),
                            ..
                        },
                    ) => Some(PendingState::PendingAt(scheduled_at.clone())),

                    (_, ExecutionEventInner::Finished { .. }) => Some(PendingState::Finished),

                    (
                        _,
                        ExecutionEventInner::Locked {
                            executor_id,
                            lock_expires_at,
                            run_id,
                        },
                    ) => Some(PendingState::Locked {
                        executor_id: executor_id.clone(),
                        lock_expires_at: *lock_expires_at,
                        run_id: run_id.clone(),
                    }),

                    (_, ExecutionEventInner::IntermittentFailure { expires_at, .. }) => {
                        Some(PendingState::PendingAt(*expires_at))
                    }

                    (_, ExecutionEventInner::IntermittentTimeout { expires_at, .. }) => {
                        Some(PendingState::PendingAt(*expires_at))
                    }

                    (
                        _,
                        ExecutionEventInner::HistoryEvent {
                            event: HistoryEvent::Yield { .. },
                            ..
                        },
                    ) => Some(PendingState::PendingNow),
                    (
                        idx,
                        ExecutionEventInner::HistoryEvent {
                            event:
                                HistoryEvent::JoinNextBlocking {
                                    join_set_id: expected_join_set_id,
                                    ..
                                },
                            ..
                        },
                    ) => {
                        // pending if this event is followed by an async response
                        if self
                            .execution_events
                            .iter()
                            .skip(idx + 1)
                            .find(|event| {
                                matches!(event, ExecutionEvent {
                            event:
                                ExecutionEventInner::HistoryEvent{event:
                                    HistoryEvent::AsyncResponse { join_set_id, .. },
                                .. },
                        .. }
                        if expected_join_set_id == join_set_id)
                            })
                            .is_some()
                        {
                            Some(PendingState::PendingNow)
                        } else {
                            Some(PendingState::BlockedByJoinSet)
                        }
                    }
                    _ => None,
                })
                .expect_or_log("journal must begin with Created event")
        }

        pub(crate) fn event_history(&self) -> Vec<HistoryEvent> {
            self.execution_events
                .iter()
                .filter_map(|event| {
                    if let ExecutionEventInner::HistoryEvent { event: eh, .. } = &event.event {
                        Some(eh.clone())
                    } else {
                        None
                    }
                })
                .collect()
        }

        pub(crate) fn retry_exp_backoff(&self) -> Duration {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { retry_exp_backoff, .. },
                ..
            }) => *retry_exp_backoff)
        }

        pub(crate) fn max_retries(&self) -> u32 {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { max_retries, .. },
                ..
            }) => *max_retries)
        }

        pub(crate) fn params(&self) -> Params {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { params, .. },
                ..
            }) => params.clone())
        }

        pub fn as_execution_history(&self) -> ExecutionHistory {
            ExecutionHistory {
                execution_id: self.execution_id.clone(),
                events: self.execution_events.iter().cloned().collect(),
                version: self.version(),
                pending_state: self.pending_state.clone(),
            }
        }

        pub(crate) fn can_be_retried_after(&self) -> Option<Duration> {
            crate::can_be_retried_after(
                self.execution_events.iter(),
                self.max_retries(),
                self.retry_exp_backoff(),
            )
        }

        pub(crate) fn truncate(&mut self, len: usize) {
            self.execution_events.truncate(len);
            self.pending_state = self.calculate_pending_state();
        }
    }

    #[derive(Debug, Clone, derive_more::Display, PartialEq, Eq)]
    pub enum PendingState {
        PendingNow,
        #[display(fmt = "Locked(`{lock_expires_at}`,{executor_id})")]
        Locked {
            executor_id: ExecutorId,
            run_id: RunId,
            lock_expires_at: DateTime<Utc>,
        },
        #[display(fmt = "PendingAt(`{_0}`)")]
        PendingAt(DateTime<Utc>), // e.g. created with a schedule, intermittent timeout/failure
        BlockedByJoinSet,
        Finished,
    }
}
