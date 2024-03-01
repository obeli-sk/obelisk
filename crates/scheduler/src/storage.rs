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
    #[display(fmt = "Created({ffqn}, `{scheduled_at:?}`)")]
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
    #[display(fmt = "Locked(`{expires_at}`, `{executor_name}`)")]
    Locked {
        executor_name: ExecutorName,
        expires_at: DateTime<Utc>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentFailure(`{expires_at}`)")]
    IntermittentFailure {
        expires_at: DateTime<Utc>,
        reason: String,
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
    Finished { result: FinishedExecutionResult<ID> },
    // Created by an external system or a scheduler during a race.
    // Processed by the executor holding the last Lock.
    // Imediately followed by Finished by a scheduler.
    #[display(fmt = "CancelRequest")]
    CancelRequest,

    #[display(fmt = "EventHistory({event})")]
    EventHistory { event: EventHistory<ID> },
}

impl<ID: ExecutionId> ExecutionEventInner<ID> {
    fn appendable_only_in_lock(&self) -> bool {
        match self {
            Self::Locked { .. }
            | Self::IntermittentFailure { .. }
            | Self::IntermittentTimeout { .. }
            | Self::Finished { .. } => true,
            Self::EventHistory { event } => event.appendable_only_in_lock(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display)]
pub(crate) enum EventHistory<ID: ExecutionId> {
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
        joinset_id: ID,
    },
    // Joinset entry that will be unblocked by DelayFinishedAsyncResponse.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    #[display(fmt = "DelayedUntilAsyncRequest({joinset_id})")]
    DelayedUntilAsyncRequest {
        joinset_id: ID,
        delay_id: ID,
        expires_at: DateTime<Utc>,
    },
    // Joinset entry that will be unblocked by ChildExecutionRequested.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    #[display(fmt = "ChildExecutionAsyncRequest({joinset_id})")]
    ChildExecutionAsyncRequest {
        joinset_id: ID,
        child_execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
    },
    // Execution continues without blocking as the next pending response is in the journal.
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    JoinNextFetched {
        joinset_id: ID,
    },
    // Moves the execution to `PotentiallyPending::PendingNow` if it is currently blocked on `JoinNextBlocking`.
    #[display(fmt = "EventHistoryAsyncResponse({joinset_id})")]
    EventHistoryAsyncResponse {
        joinset_id: ID,
        response: EventHistoryAsyncResponse<ID>,
    },
    // Must be created by the executor in `PotentiallyPending::Locked` state.
    // Execution is `PotentiallyPending::BlockedByJoinSet` until the next response of the joinset arrives.
    JoinNextBlocking {
        joinset_id: ID,
    },
}

impl<ID: ExecutionId> EventHistory<ID> {
    fn appendable_only_in_lock(&self) -> bool {
        !matches!(self, Self::EventHistoryAsyncResponse { .. })
    }
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
pub trait DbConnection<ID: ExecutionId>: Send + 'static {
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
        created_at: DateTime<Utc>,
        execution_id: ID,
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
        created_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        event: ExecutionEventInner<ID>,
    ) -> Result<AppendResponse, DbError>;

    async fn get(&self, execution_id: ID) -> Result<ExecutionHistory<ID>, DbError>;
}

pub(crate) mod journal {
    use super::{EventHistory, ExecutionEvent, ExecutionEventInner, ExecutorName};
    use crate::storage::{ExecutionHistory, RowSpecificError, Version};
    use chrono::{DateTime, Utc};
    use concepts::{ExecutionId, FunctionFqn, Params};
    use std::collections::VecDeque;
    use tracing_unwrap::OptionExt;

    #[derive(Debug)]
    pub(crate) struct ExecutionJournal<ID: ExecutionId> {
        execution_id: ID,
        events: VecDeque<ExecutionEvent<ID>>,
    }

    impl<ID: ExecutionId> ExecutionJournal<ID> {
        pub(crate) fn new(
            execution_id: ID,
            ffqn: FunctionFqn,
            params: Params,
            scheduled_at: Option<DateTime<Utc>>,
            parent: Option<ID>,
            created_at: DateTime<Utc>,
        ) -> Self {
            let event = ExecutionEvent {
                event: ExecutionEventInner::Created {
                    ffqn,
                    params,
                    scheduled_at,
                    parent,
                },
                created_at,
            };
            Self {
                execution_id,
                events: VecDeque::from([event]),
            }
        }

        pub(crate) fn len(&self) -> usize {
            self.events.len()
        }

        pub(crate) fn created_at(&self) -> DateTime<Utc> {
            self.events.iter().rev().next().unwrap_or_log().created_at
        }

        pub(crate) fn ffqn(&self) -> &FunctionFqn {
            match self.events.get(0) {
                Some(ExecutionEvent {
                    event: ExecutionEventInner::Created { ffqn, .. },
                    ..
                }) => ffqn,
                _ => panic!("first event must be `Created`"),
            }
        }

        pub(crate) fn version(&self) -> Version {
            self.events.len()
        }

        pub(crate) fn id(&self) -> &ID {
            &self.execution_id
        }

        pub(crate) fn validate_push(
            &mut self,
            created_at: DateTime<Utc>,
            event: ExecutionEventInner<ID>,
        ) -> Result<(), RowSpecificError> {
            let pending_state = self.pending_state();
            if pending_state == PendingState::Finished {
                return Err(RowSpecificError::ValidationFailed("already finished"));
            }

            if let ExecutionEventInner::Locked {
                executor_name,
                expires_at,
            } = &event
            {
                if *expires_at <= created_at {
                    return Err(RowSpecificError::ValidationFailed("invalid expiry date"));
                }
                match &pending_state {
                    PendingState::PendingNow => {} // ok to lock
                    PendingState::PendingAfterExpiry(pending_start) => {
                        if *pending_start <= created_at {
                            // pending now, ok to lock
                        } else {
                            return Err(RowSpecificError::ValidationFailed(
                                "cannot append {event} event, not yet pending",
                            ));
                        }
                    }
                    PendingState::Locked {
                        executor_name: locked_by,
                        expires_at,
                    } => {
                        if executor_name == locked_by {
                            // we allow extending the lock
                        } else if *expires_at <= created_at {
                            // we allow locking after the old lock expired
                        } else {
                            return Err(RowSpecificError::ValidationFailed(
                                "cannot append {event}, already in {pending_state} state",
                            ));
                        }
                    }
                    PendingState::BlockedByJoinSet => {
                        return Err(RowSpecificError::ValidationFailed(
                            "cannot append Locked event when in BlockedByJoinSet state",
                        ));
                    }
                    PendingState::Finished => {
                        unreachable!() // handled at the beginning of the function
                    }
                }
            }
            let locked_now = matches!(pending_state, PendingState::Locked { expires_at,.. } if expires_at > created_at);
            if !event.appendable_only_in_lock() {
                return Err(RowSpecificError::ValidationFailed(
                    "cannot append {event} event in {pending_state} state",
                ));
            }
            self.events.push_back(ExecutionEvent { event, created_at });
            Ok(())
        }

        pub(crate) fn event_history(&self) -> Vec<EventHistory<ID>> {
            self.events
                .iter()
                .filter_map(|event| {
                    if let ExecutionEventInner::EventHistory { event: eh, .. } = &event.event {
                        Some(eh.clone())
                    } else {
                        None
                    }
                })
                .collect()
        }

        pub(crate) fn params(&self) -> Params {
            match self.events.front().expect_or_log("must be not empty") {
                ExecutionEvent {
                    event: ExecutionEventInner::Created { params, .. },
                    ..
                } => Some(params),
                _ => None,
            }
            .expect_or_log("first event must be `Created`")
            .clone()
        }

        pub(crate) fn as_execution_history(&self) -> ExecutionHistory<ID> {
            (self.events.iter().cloned().collect(), self.version())
        }

        pub(crate) fn pending_state(&self) -> PendingState {
            self.events
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
                    ) => Some(PendingState::PendingAfterExpiry(scheduled_at.clone())),

                    (_, ExecutionEventInner::Finished { .. }) => Some(PendingState::Finished),

                    (
                        _,
                        ExecutionEventInner::Locked {
                            executor_name,
                            expires_at,
                        },
                    ) => Some(PendingState::Locked {
                        executor_name: executor_name.clone(),
                        expires_at: *expires_at,
                    }),

                    (_, ExecutionEventInner::IntermittentFailure { expires_at, .. }) => {
                        Some(PendingState::PendingAfterExpiry(*expires_at))
                    }

                    (_, ExecutionEventInner::IntermittentTimeout { expires_at, .. }) => {
                        Some(PendingState::PendingAfterExpiry(*expires_at))
                    }

                    (
                        _,
                        ExecutionEventInner::EventHistory {
                            event: EventHistory::Yield { .. },
                            ..
                        },
                    ) => Some(PendingState::PendingNow),
                    (
                        idx,
                        ExecutionEventInner::EventHistory {
                            event:
                                EventHistory::JoinNextBlocking {
                                    joinset_id: expected_join_set_id,
                                    ..
                                },
                            ..
                        },
                    ) => {
                        // pending if this event is followed by an async response
                        if self
                            .events
                            .iter()
                            .skip(idx + 1)
                            .find(|event| {
                                matches!(event, ExecutionEvent {
                                event:
                                    ExecutionEventInner::EventHistory{event:
                                        EventHistory::EventHistoryAsyncResponse { joinset_id, .. },
                                    .. },
                            .. }
                            if expected_join_set_id == joinset_id)
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
    }

    #[derive(Debug, derive_more::Display, PartialEq, Eq)]
    pub(crate) enum PendingState {
        PendingNow,
        #[display(fmt = "Locked(`{expires_at}`,`{executor_name}`)")]
        Locked {
            executor_name: ExecutorName,
            expires_at: DateTime<Utc>,
        },
        #[display(fmt = "PendingAfterExpiry(`{_0}`)")]
        PendingAfterExpiry(DateTime<Utc>), // e.g. created with a schedule, intermittent timeout/failure
        BlockedByJoinSet,
        Finished,
    }
}
