use self::journal::PendingState;
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
use std::fmt::Debug;
use std::time::Duration;
use tracing::debug;
use tracing::trace;

/// Remote client representation of the execution journal.
#[derive(Debug)]
pub struct ExecutionHistory {
    pub execution_id: ExecutionId,
    pub events: Vec<ExecutionEvent>,
    pub version: Version,
    pub pending_state: PendingState,
}

impl ExecutionHistory {
    fn already_retried_count(&self) -> u32 {
        u32::try_from(
            self.events
                .iter()
                .filter(|event| event.event.is_retry())
                .count(),
        )
        .unwrap()
    }

    #[must_use]
    pub fn can_be_retried_after(&self) -> Option<Duration> {
        let already_retried_count = self.already_retried_count();
        if already_retried_count < self.max_retries() {
            let duration = self.retry_exp_backoff() * 2_u32.saturating_pow(already_retried_count);
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
    pub fn params(&self) -> Params {
        assert_matches!(self.events.first(), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { params, .. },
            ..
        }) => params.clone())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
pub struct Version(usize);
impl Version {
    #[must_use]
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
    pub fn appendable_only_in_lock(&self) -> bool {
        match self {
            Self::Locked { .. } // only if the lock is being extended by the same executor
            | Self::IntermittentFailure { .. }
            | Self::IntermittentTimeout { .. }
            | Self::Finished { .. } => true,
            Self::HistoryEvent { event } => event.appendable_only_in_lock(),
            _ => false,
        }
    }

    #[must_use]
    pub fn is_retry(&self) -> bool {
        matches!(
            self,
            Self::IntermittentFailure { .. } | Self::IntermittentTimeout { .. }
        )
    }

    #[must_use]
    pub fn appendable_without_version(&self) -> bool {
        matches!(
            self,
            Self::HistoryEvent {
                event: HistoryEvent::JoinSetResponse {
                    response: JoinSetResponse::ChildExecutionFinished { .. },
                    ..
                }
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
    // Execution continues without blocking as the next pending response is in the journal.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    JoinNextFetched {
        join_set_id: JoinSetId,
    },
    // Moves the execution to `PotentiallyPending::PendingNow` if it is currently blocked on `JoinNextBlocking`.
    #[display(fmt = "AsyncResponse({join_set_id}, {response})")]
    JoinSetResponse {
        join_set_id: JoinSetId,
        response: JoinSetResponse,
    },
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    // Execution is `PotentiallyPending::BlockedByJoinSet` until the next response of the JoinSet arrives.
    JoinNextBlocking {
        join_set_id: JoinSetId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary)]
pub enum JoinSetRequest {
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    #[display(fmt = "DelayRequest({delay_id})")]
    DelayRequest {
        delay_id: DelayId,
        expires_at: DateTime<Utc>,
    },
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    #[display(fmt = "ChildExecutionRequest({child_execution_id})")]
    ChildExecutionRequest { child_execution_id: ExecutionId },
}

impl HistoryEvent {
    fn appendable_only_in_lock(&self) -> bool {
        !matches!(self, Self::JoinSetResponse { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary)]
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

    #[allow(clippy::too_many_arguments)]
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
        self.append(execution_id, None, AppendRequest { created_at, event })
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
        &self,
        execution_id: ExecutionId,
        timeout: Option<Duration>,
    ) -> Result<FinishedExecutionResult, DbError> {
        let execution_history = self
            .wait_for_pending_state(execution_id, PendingState::Finished, timeout)
            .await?;
        Ok(execution_history
            .finished_result()
            .expect("pending state was checked")
            .clone())
    }

    async fn wait_for_pending_state(
        &self,
        execution_id: ExecutionId,
        expected_pending_state: PendingState,
        timeout: Option<Duration>,
    ) -> Result<ExecutionHistory, DbError> {
        trace!(%execution_id, "Waiting for {expected_pending_state}");
        let fut = async move {
            loop {
                let execution_history = self.get(execution_id).await?;
                if execution_history.pending_state == expected_pending_state {
                    debug!(%execution_id, "Found: {expected_pending_state}");
                    return Ok(execution_history);
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

    async fn wait_for_pending_state_fn<T: Debug>(
        &self,
        execution_id: ExecutionId,
        prdicate: impl Fn(ExecutionHistory) -> Option<T> + Send,
        timeout: Option<Duration>,
    ) -> Result<T, DbError> {
        trace!(%execution_id, "Waiting for predicate");
        let fut = async move {
            loop {
                let execution_history = self.get(execution_id).await?;
                if let Some(t) = prdicate(execution_history) {
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

    async fn get_expired_timers(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbConnectionError>;
}

#[derive(Debug, Clone)]
pub enum ExpiredTimer {
    Lock {
        execution_id: ExecutionId,
        version: Version,
        already_retried_count: u32,
        max_retries: u32,
        retry_exp_backoff: Duration,
    },
    AsyncTimer {
        execution_id: ExecutionId,
        version: Version,
        join_set_id: JoinSetId,
        delay_id: DelayId,
    },
}

pub mod journal {
    use super::{ExecutionEvent, ExecutionEventInner, ExecutionId, HistoryEvent};
    use crate::storage::{ExecutionHistory, SpecificError, Version};
    use crate::{
        prefixed_ulid::{ExecutorId, JoinSetId, RunId},
        FunctionFqn, Params, StrVariant,
    };
    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use std::{collections::VecDeque, sync::Arc, time::Duration};

    #[derive(Debug)]
    pub struct ExecutionJournal {
        pub execution_id: ExecutionId,
        pub pending_state: PendingState,
        pub execution_events: VecDeque<ExecutionEvent>,
    }

    impl ExecutionJournal {
        #[allow(clippy::too_many_arguments)]
        #[must_use]
        pub fn new(
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

        #[must_use]
        pub fn len(&self) -> usize {
            self.execution_events.len()
        }

        #[must_use]
        pub fn is_empty(&self) -> bool {
            self.execution_events.is_empty()
        }

        #[must_use]
        pub fn ffqn(&self) -> &FunctionFqn {
            match self.execution_events.front().unwrap() {
                ExecutionEvent {
                    event: ExecutionEventInner::Created { ffqn, .. },
                    ..
                } => ffqn,
                _ => panic!("first event must be `Created`"),
            }
        }

        #[must_use]
        pub fn version(&self) -> Version {
            Version(self.execution_events.len())
        }

        #[must_use]
        pub fn execution_id(&self) -> ExecutionId {
            self.execution_id
        }

        pub fn append(
            &mut self,
            created_at: DateTime<Utc>,
            event: ExecutionEventInner,
        ) -> Result<Version, SpecificError> {
            if self.pending_state == PendingState::Finished {
                return Err(SpecificError::ValidationFailed(StrVariant::Static(
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
                    return Err(SpecificError::ValidationFailed(StrVariant::Static(
                        "invalid expiry date",
                    )));
                }
                match &self.pending_state {
                    PendingState::PendingNow => {} // ok to lock
                    PendingState::PendingAt(pending_start) => {
                        if *pending_start <= created_at {
                            // pending now, ok to lock
                        } else {
                            return Err(SpecificError::ValidationFailed(StrVariant::Arc(
                                Arc::from(format!("cannot append {event} event, not yet pending")),
                            )));
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
                            return Err(SpecificError::ValidationFailed(StrVariant::Arc(
                                Arc::from(format!(
                                    "cannot append {event}, already in {}",
                                    self.pending_state
                                )),
                            )));
                        }
                    }
                    PendingState::BlockedByJoinSet { .. } => {
                        return Err(SpecificError::ValidationFailed(StrVariant::Static(
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
                return Err(SpecificError::ValidationFailed(StrVariant::Arc(Arc::from(
                    format!("cannot append {event} event in {}", self.pending_state),
                ))));
            }
            self.execution_events
                .push_back(ExecutionEvent { created_at, event });
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
                        }
                        | ExecutionEventInner::HistoryEvent {
                            event: HistoryEvent::Yield { .. },
                            ..
                        },
                    ) => Some(PendingState::PendingNow),

                    (
                        _,
                        ExecutionEventInner::Created {
                            scheduled_at: Some(scheduled_at),
                            ..
                        },
                    ) => Some(PendingState::PendingAt(*scheduled_at)),

                    (_, ExecutionEventInner::Finished { .. }) => Some(PendingState::Finished),

                    (
                        _,
                        ExecutionEventInner::Locked {
                            executor_id,
                            lock_expires_at,
                            run_id,
                        },
                    ) => Some(PendingState::Locked {
                        executor_id: *executor_id,
                        lock_expires_at: *lock_expires_at,
                        run_id: *run_id,
                    }),

                    (
                        _,
                        ExecutionEventInner::IntermittentFailure { expires_at, .. }
                        | ExecutionEventInner::IntermittentTimeout { expires_at, .. },
                    ) => Some(PendingState::PendingAt(*expires_at)),

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
                        if self.execution_events.iter().skip(idx + 1).any(|event| {
                            matches!(event, ExecutionEvent {
                                event:
                                    ExecutionEventInner::HistoryEvent{event:
                                        HistoryEvent::JoinSetResponse { join_set_id, .. },
                                    .. },
                                .. }
                                if expected_join_set_id == join_set_id)
                        }) {
                            Some(PendingState::PendingNow)
                        } else {
                            Some(PendingState::BlockedByJoinSet {
                                join_set_id: *expected_join_set_id,
                            })
                        }
                    }
                    _ => None,
                })
                .expect("journal must begin with Created event")
        }

        pub fn event_history(&self) -> impl Iterator<Item = HistoryEvent> + '_ {
            self.execution_events.iter().filter_map(|event| {
                if let ExecutionEventInner::HistoryEvent { event: eh, .. } = &event.event {
                    Some(eh.clone())
                } else {
                    None
                }
            })
        }

        #[must_use]
        pub fn retry_exp_backoff(&self) -> Duration {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { retry_exp_backoff, .. },
                ..
            }) => *retry_exp_backoff)
        }

        #[must_use]
        pub fn max_retries(&self) -> u32 {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { max_retries, .. },
                ..
            }) => *max_retries)
        }

        #[must_use]
        pub fn already_retried_count(&self) -> u32 {
            u32::try_from(
                self.execution_events
                    .iter()
                    .filter(|event| event.event.is_retry())
                    .count(),
            )
            .unwrap()
        }

        #[must_use]
        pub fn params(&self) -> Params {
            assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { params, .. },
                ..
            }) => params.clone())
        }

        #[must_use]
        pub fn as_execution_history(&self) -> ExecutionHistory {
            ExecutionHistory {
                execution_id: self.execution_id,
                events: self.execution_events.iter().cloned().collect(),
                version: self.version(),
                pending_state: self.pending_state,
            }
        }

        pub fn truncate(&mut self, len: usize) {
            self.execution_events.truncate(len);
            self.pending_state = self.calculate_pending_state();
        }
    }

    #[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq)]
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
        BlockedByJoinSet {
            join_set_id: JoinSetId,
        },
        Finished,
    }
}
