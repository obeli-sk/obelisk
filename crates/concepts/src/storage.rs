use crate::ClosingStrategy;
use crate::ComponentId;
use crate::ExecutionId;
use crate::ExecutionMetadata;
use crate::FinishedExecutionResult;
use crate::FunctionFqn;
use crate::JoinSetId;
use crate::Params;
use crate::StrVariant;
use crate::prefixed_ulid::DelayId;
use crate::prefixed_ulid::ExecutionIdDerived;
use crate::prefixed_ulid::ExecutorId;
use crate::prefixed_ulid::RunId;
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::TimeDelta;
use chrono::{DateTime, Utc};
use http_client_trace::HttpClientTrace;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoStaticStr;
use tracing::error;

/// Remote client representation of the execution journal.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionLog {
    pub execution_id: ExecutionId,
    pub events: Vec<ExecutionEvent>,
    pub responses: Vec<JoinSetResponseEventOuter>,
    pub next_version: Version, // Is not advanced once in Finished state
    pub pending_state: PendingState,
}

impl ExecutionLog {
    #[must_use]
    pub fn can_be_retried_after(
        temporary_event_count: u32,
        max_retries: u32,
        retry_exp_backoff: Duration,
    ) -> Option<Duration> {
        // If max_retries == u32::MAX, wrapping is OK after this succeeds - we want to retry forever.
        if temporary_event_count <= max_retries {
            // TODO: Add test for number of retries
            let duration = retry_exp_backoff * 2_u32.saturating_pow(temporary_event_count - 1);
            Some(duration)
        } else {
            None
        }
    }

    #[must_use]
    pub fn compute_retry_duration_when_retrying_forever(
        temporary_event_count: u32,
        retry_exp_backoff: Duration,
    ) -> Duration {
        Self::can_be_retried_after(temporary_event_count, u32::MAX, retry_exp_backoff)
            .expect("`max_retries` set to MAX must never return None")
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
        }) => parent.clone())
    }

    #[must_use]
    pub fn last_event(&self) -> &ExecutionEvent {
        self.events.last().expect("must contain at least one event")
    }

    #[must_use]
    pub fn into_finished_result(mut self) -> Option<FinishedExecutionResult> {
        if let ExecutionEvent {
            event: ExecutionEventInner::Finished { result, .. },
            ..
        } = self.events.pop().expect("must contain at least one event")
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

    #[cfg(feature = "test")]
    #[must_use]
    pub fn find_join_set_request(&self, join_set_id: &JoinSetId) -> Option<&JoinSetRequest> {
        self.events
            .iter()
            .find_map(move |event| match &event.event {
                ExecutionEventInner::HistoryEvent {
                    event:
                        HistoryEvent::JoinSetRequest {
                            join_set_id: found,
                            request,
                        },
                    ..
                } if *join_set_id == *found => Some(request),
                _ => None,
            })
    }
}

pub type VersionType = u32;
#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    derive_more::Into,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct Version(pub VersionType);
impl Version {
    #[must_use]
    pub fn new(arg: VersionType) -> Self {
        Self(arg)
    }
}

#[derive(
    Clone, Debug, derive_more::Display, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[display("{event}")]
pub struct ExecutionEvent {
    pub created_at: DateTime<Utc>,
    pub event: ExecutionEventInner,
    pub backtrace_id: Option<Version>,
}

/// Moves the execution to [`PendingState::PendingNow`] if it is currently blocked on `JoinNextBlocking`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JoinSetResponseEventOuter {
    pub created_at: DateTime<Utc>,
    pub event: JoinSetResponseEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JoinSetResponseEvent {
    pub join_set_id: JoinSetId,
    pub event: JoinSetResponse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(tag = "type")]
pub enum JoinSetResponse {
    DelayFinished {
        delay_id: DelayId,
    },
    ChildExecutionFinished {
        child_execution_id: ExecutionIdDerived,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = Version(2)))]
        finished_version: Version,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = Ok(crate::SupportedFunctionReturnValue::None)))]
        result: FinishedExecutionResult,
    },
}

pub const DUMMY_CREATED: ExecutionEventInner = ExecutionEventInner::Created {
    ffqn: FunctionFqn::new_static("", ""),
    params: Params::empty(),
    parent: None,
    scheduled_at: DateTime::from_timestamp_nanos(0),
    retry_exp_backoff: Duration::ZERO,
    max_retries: 0,
    component_id: ComponentId::dummy_activity(),
    metadata: ExecutionMetadata::empty(),
    scheduled_by: None,
};
pub const DUMMY_HISTORY_EVENT: ExecutionEventInner = ExecutionEventInner::HistoryEvent {
    event: HistoryEvent::JoinSetCreate {
        join_set_id: JoinSetId {
            kind: crate::JoinSetKind::OneOff,
            name: StrVariant::empty(),
        },
        closing_strategy: ClosingStrategy::Complete,
    },
};
pub const DUMMY_TEMPORARILY_TIMED_OUT: ExecutionEventInner =
    ExecutionEventInner::TemporarilyTimedOut {
        backoff_expires_at: DateTime::from_timestamp_nanos(0),
        http_client_traces: None,
    };
pub const DUMMY_TEMPORARILY_FAILED: ExecutionEventInner = ExecutionEventInner::TemporarilyFailed {
    backoff_expires_at: DateTime::from_timestamp_nanos(0),
    reason_full: StrVariant::empty(),
    reason_inner: StrVariant::empty(),
    detail: None,
    http_client_traces: None,
};

#[derive(
    Clone,
    derive_more::Debug,
    derive_more::Display,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    IntoStaticStr,
)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[allow(clippy::large_enum_variant)]
pub enum ExecutionEventInner {
    /// Created by an external system or a scheduler when requesting a child execution or
    /// an executor when continuing as new `FinishedExecutionError`::`ContinueAsNew`,`CancelledWithNew` .
    /// The execution is [`PendingState::PendingAt`]`(scheduled_at)`.
    #[display("Created({ffqn}, `{scheduled_at}`)")]
    Created {
        ffqn: FunctionFqn,
        #[cfg_attr(any(test, feature = "test"), arbitrary(default))]
        #[debug(skip)]
        params: Params,
        parent: Option<(ExecutionId, JoinSetId)>,
        scheduled_at: DateTime<Utc>,
        retry_exp_backoff: Duration,
        max_retries: u32,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = ComponentId::dummy_activity()))]
        component_id: ComponentId,
        #[cfg_attr(any(test, feature = "test"), arbitrary(default))]
        metadata: ExecutionMetadata,
        scheduled_by: Option<ExecutionId>,
    },
    // Created by an executor.
    // Either immediately followed by an execution request by an executor or
    // after expiry immediately followed by WaitingForExecutor by a scheduler.
    #[display("Locked(`{lock_expires_at}`, {component_id})")]
    Locked {
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = ComponentId::dummy_activity()))]
        component_id: ComponentId,
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
    },
    /// Returns execution to [`PendingState::PendingNow`] state
    /// without timing out. This can happen when the executor is running
    /// out of resources like [`WorkerError::LimitReached`] or when
    /// the executor is shutting down.
    #[display("Unlocked(`{backoff_expires_at}`)")]
    Unlocked {
        backoff_expires_at: DateTime<Utc>,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = StrVariant::Static("reason")))]
        reason: StrVariant,
    },
    // Created by the executor holding the lock.
    // After expiry interpreted as pending.
    #[display("TemporarilyFailed(`{backoff_expires_at}`)")]
    TemporarilyFailed {
        backoff_expires_at: DateTime<Utc>,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = StrVariant::Static("reason")))]
        reason_full: StrVariant,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = StrVariant::Static("reason inner")))]
        reason_inner: StrVariant,
        detail: Option<String>,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = None))]
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },
    // Created by the executor holding last lock.
    // After expiry interpreted as pending.
    #[display("TemporarilyTimedOut(`{backoff_expires_at}`)")]
    TemporarilyTimedOut {
        backoff_expires_at: DateTime<Utc>,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = None))]
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler if a parent execution needs to be notified,
    // also when
    #[display("Finished")]
    Finished {
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = Ok(crate::SupportedFunctionReturnValue::None)))]
        result: FinishedExecutionResult,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = None))]
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },

    #[display("HistoryEvent({event})")]
    HistoryEvent { event: HistoryEvent },
}

impl ExecutionEventInner {
    #[must_use]
    pub fn is_temporary_event(&self) -> bool {
        matches!(
            self,
            Self::TemporarilyFailed { .. } | Self::TemporarilyTimedOut { .. }
        )
    }

    #[must_use]
    pub fn variant(&self) -> &'static str {
        Into::<&'static str>::into(self)
    }

    #[must_use]
    pub fn join_set_id(&self) -> Option<&JoinSetId> {
        match self {
            Self::Created {
                parent: Some((_parent_id, join_set_id)),
                ..
            } => Some(join_set_id),
            Self::HistoryEvent {
                event:
                    HistoryEvent::JoinSetCreate { join_set_id, .. }
                    | HistoryEvent::JoinSetRequest { join_set_id, .. }
                    | HistoryEvent::JoinNext { join_set_id, .. },
            } => Some(join_set_id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(tag = "type")]
pub enum PersistKind {
    #[display("RandomU64({min}, {max_inclusive})")]
    RandomU64 { min: u64, max_inclusive: u64 },
    #[display("RandomString({min_length}, {max_length_exclusive})")]
    RandomString {
        min_length: u64,
        max_length_exclusive: u64,
    },
}

#[must_use]
pub fn from_u64_to_bytes(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

#[must_use]
pub fn from_bytes_to_u64(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}

#[derive(
    derive_more::Debug, Clone, PartialEq, Eq, derive_more::Display, Serialize, Deserialize,
)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(tag = "type")]
/// Must be created by the executor in [`PendingState::Locked`].
pub enum HistoryEvent {
    /// Persist a generated pseudorandom value.
    #[display("Persist")]
    Persist {
        #[debug(skip)]
        value: Vec<u8>, // Only stored for nondeterminism checks. TODO: Consider using a hashed value or just the intention.
        kind: PersistKind,
    },
    #[display("JoinSetCreate({join_set_id})")]
    JoinSetCreate {
        join_set_id: JoinSetId,
        closing_strategy: ClosingStrategy,
    },
    #[display("JoinSetRequest({join_set_id}, {request})")]
    JoinSetRequest {
        join_set_id: JoinSetId,
        request: JoinSetRequest,
    },
    /// Sets the pending state to [`PendingState::BlockedByJoinSet`].
    /// When the response arrives at `resp_time`:
    /// The execution is [`PendingState::PendingAt`]`(max(resp_time, lock_expires_at)`, so that the
    /// original executor can continue. After the expiry any executor can continue without
    /// marking the execution as timed out.
    #[display("JoinNext({join_set_id})")]
    JoinNext {
        join_set_id: JoinSetId,
        /// Set to a future time if the worker is keeping the execution invocation warm waiting for the result.
        /// The pending status will be kept in Locked state until `run_expires_at`.
        run_expires_at: DateTime<Utc>,
        /// Set to a specific function when calling `-await-next` extension function, used for
        /// determinism checks.
        requested_ffqn: Option<FunctionFqn>,
        /// Closing request must never set `requested_ffqn` and is ignored by determinism checks.
        closing: bool,
    },
    /// Records the fact that a join set was awaited more times than its submission count.
    #[display("JoinNextTooMany({join_set_id})")]
    JoinNextTooMany {
        join_set_id: JoinSetId,
        /// Set to a specific function when calling `-await-next` extension function, used for
        /// determinism checks.
        requested_ffqn: Option<FunctionFqn>,
    },
    #[display("Schedule({execution_id}, {schedule_at})")]
    Schedule {
        execution_id: ExecutionId,
        schedule_at: HistoryEventScheduleAt, // Stores intention to schedule an execution at a date/offset
    },
    #[display("Stub({target_execution_id})")]
    Stub {
        target_execution_id: ExecutionIdDerived,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = Ok(crate::SupportedFunctionReturnValue::None)))]
        result: FinishedExecutionResult, // Only stored for nondeterminism checks. TODO: Consider using a hashed value.
        persist_result: Result<(), ()>, // Is the Row (target_execution_id,Version:1) what is expected?
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
pub enum HistoryEventScheduleAt {
    Now,
    At(DateTime<Utc>),
    #[display("In({_0:?})")]
    In(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScheduleAtConversionError {
    #[error("source duration value is out of range")]
    OutOfRangeError,
}

impl HistoryEventScheduleAt {
    pub fn as_date_time(
        &self,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, ScheduleAtConversionError> {
        match self {
            Self::Now => Ok(now),
            Self::At(date_time) => Ok(*date_time),
            Self::In(duration) => {
                let time_delta = TimeDelta::from_std(*duration)
                    .map_err(|_| ScheduleAtConversionError::OutOfRangeError)?;
                now.checked_add_signed(time_delta)
                    .ok_or(ScheduleAtConversionError::OutOfRangeError)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(tag = "type")]
pub enum JoinSetRequest {
    // Must be created by the executor in `PendingState::Locked`.
    #[display("DelayRequest({delay_id}, expires_at: `{expires_at}`, schedule_at: `{schedule_at}`)")]
    DelayRequest {
        delay_id: DelayId,
        expires_at: DateTime<Utc>,
        schedule_at: HistoryEventScheduleAt,
    },
    // Must be created by the executor in `PendingState::Locked`.
    #[display("ChildExecutionRequest({child_execution_id})")]
    ChildExecutionRequest {
        child_execution_id: ExecutionIdDerived,
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
    ValidationFailed(StrVariant),
    #[error("version mismatch")]
    VersionMismatch {
        appending_version: Version,
        expected_version: Version,
    },
    #[error("version missing")]
    VersionMissing,
    #[error("not found")]
    NotFound,
    #[error("consistency error: `{0}`")]
    ConsistencyError(StrVariant),
    #[error("{0}")]
    GenericError(StrVariant),
}

#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
pub enum DbError {
    #[error(transparent)]
    Connection(#[from] DbConnectionError),
    #[error(transparent)]
    Specific(#[from] SpecificError),
}

pub type AppendResponse = Version;
pub type PendingExecution = (ExecutionId, Version, Params, Option<DateTime<Utc>>);
pub type LockResponse = (Vec<HistoryEvent>, Version);

#[derive(Debug, Clone)]
pub struct LockedExecution {
    pub execution_id: ExecutionId,
    pub metadata: ExecutionMetadata,
    pub run_id: RunId,
    pub version: Version,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub event_history: Vec<HistoryEvent>,
    pub responses: Vec<JoinSetResponseEventOuter>,
    pub scheduled_at: DateTime<Utc>,
    pub retry_exp_backoff: Duration,
    pub max_retries: u32,
    pub parent: Option<(ExecutionId, JoinSetId)>,
    pub temporary_event_count: u32,
}

pub type LockPendingResponse = Vec<LockedExecution>;
pub type AppendBatchResponse = Version;

#[derive(Debug, Clone, derive_more::Display, Serialize, Deserialize)]
#[display("{event}")]
pub struct AppendRequest {
    pub created_at: DateTime<Utc>,
    pub event: ExecutionEventInner,
}

#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub created_at: DateTime<Utc>,
    pub execution_id: ExecutionId,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub parent: Option<(ExecutionId, JoinSetId)>,
    pub scheduled_at: DateTime<Utc>,
    pub retry_exp_backoff: Duration,
    pub max_retries: u32,
    pub component_id: ComponentId,
    pub metadata: ExecutionMetadata,
    pub scheduled_by: Option<ExecutionId>,
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
            component_id: value.component_id,
            metadata: value.metadata,
            scheduled_by: value.scheduled_by,
        }
    }
}

#[async_trait]
pub trait DbPool: Send + Sync {
    fn connection(&self) -> Box<dyn DbConnection>;
    fn is_closing(&self) -> bool;
    async fn close(&self) -> Result<(), DbError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("client timeout")]
    Timeout,
    #[error(transparent)]
    DbError(#[from] DbError),
}

#[async_trait]
pub trait DbConnection: Send + Sync {
    #[expect(clippy::too_many_arguments)]
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
    ) -> Result<LockPendingResponse, DbError>;

    /// Specialized `append` which returns the event history.
    #[expect(clippy::too_many_arguments)]
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError>;

    /// Append a single event to an existing execution log
    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError>;

    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbError>;

    /// Append one or more events to an existing execution log
    async fn append_batch(
        &self,
        current_time: DateTime<Utc>, // not persisted, can be used for unblocking `subscribe_to_pending`
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError>;

    /// Append one or more events to the parent execution log, and create zero or more child execution logs.
    async fn append_batch_create_new_execution(
        &self,
        current_time: DateTime<Utc>, // not persisted, can be used for unblocking `subscribe_to_pending`
        batch: Vec<AppendRequest>,   // must not contain `ExecutionEventInner::Created` events
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbError>;

    async fn append_batch_respond_to_parent(
        &self,
        execution_id: ExecutionIdDerived,
        current_time: DateTime<Utc>, // not persisted, can be used for unblocking `subscribe_to_pending`
        batch: Vec<AppendRequest>,
        version: Version,
        parent_execution_id: ExecutionId,
        parent_response_event: JoinSetResponseEventOuter,
    ) -> Result<AppendBatchResponse, DbError>;

    #[cfg(feature = "test")]
    /// Get execution log.
    async fn get(&self, execution_id: &ExecutionId) -> Result<ExecutionLog, DbError>;

    async fn list_execution_events(
        &self,
        execution_id: &ExecutionId,
        since: &Version,
        max_length: VersionType,
        include_backtrace_id: bool,
    ) -> Result<Vec<ExecutionEvent>, DbError>;

    /// Get a single event without `backtrace_id`
    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbError>;

    async fn get_create_request(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbError> {
        let execution_event = self
            .get_execution_event(execution_id, &Version::new(0))
            .await?;
        if let ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
            component_id,
            metadata,
            scheduled_by,
        } = execution_event.event
        {
            Ok(CreateRequest {
                created_at: execution_event.created_at,
                execution_id: execution_id.clone(),
                ffqn,
                params,
                parent,
                scheduled_at,
                retry_exp_backoff,
                max_retries,
                component_id,
                metadata,
                scheduled_by,
            })
        } else {
            error!(%execution_id, "Execution log must start with creation");
            Err(DbError::Specific(SpecificError::ConsistencyError(
                StrVariant::Static("execution log must start with creation"),
            )))
        }
    }

    async fn get_finished_result(
        &self,
        execution_id: &ExecutionId,
        finished: PendingStateFinished,
    ) -> Result<Option<FinishedExecutionResult>, DbError> {
        let last_event = self
            .get_execution_event(execution_id, &Version::new(finished.version))
            .await?;
        if let ExecutionEventInner::Finished { result, .. } = last_event.event {
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    async fn get_pending_state(&self, execution_id: &ExecutionId) -> Result<PendingState, DbError>;

    /// Get currently expired locks and async timers (delay requests)
    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError>;

    /// Create a new execution log
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError>;

    /// Get notified when a new response arrives.
    /// Parameter `start_idx` must be at most be equal to current size of responses in the execution log.
    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError>;

    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout: Option<Duration>,
    ) -> Result<FinishedExecutionResult, ClientError>;

    /// Best effort for blocking while there are no pending executions.
    /// Return immediately if there are pending notifications at `pending_at_or_sooner`.
    /// Implementation must return not later than at expiry date, which is: `pending_at_or_sooner` + `max_wait`.
    /// Timers that expire until the expiry date can be disregarded.
    /// Databases that do not support subscriptions should wait for `max_wait`.
    async fn wait_for_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        max_wait: Duration,
    );

    async fn append_backtrace(&self, append: BacktraceInfo) -> Result<(), DbError>;

    async fn append_backtrace_batch(&self, batch: Vec<BacktraceInfo>) -> Result<(), DbError>;

    /// Used by gRPC only.
    /// Get the latest backtrace if version is not set.
    async fn get_backtrace(
        &self,
        execution_id: &ExecutionId,
        filter: BacktraceFilter,
    ) -> Result<BacktraceInfo, DbError>;

    /// Returns executions sorted in descending order.
    /// Used by gRPC only.
    async fn list_executions(
        &self,
        ffqn: Option<FunctionFqn>,
        top_level_only: bool,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbError>;

    /// Returns responses of an execution ordered as they arrived,
    /// enabling matching each `JoinNext` to its corresponding response.
    /// Used by gRPC only.
    async fn list_responses(
        &self,
        execution_id: &ExecutionId,
        pagination: Pagination<u32>,
    ) -> Result<Vec<ResponseWithCursor>, DbError>;
}

#[derive(Clone)]
pub enum BacktraceFilter {
    First,
    Last,
    Specific(Version),
}

pub struct BacktraceInfo {
    pub execution_id: ExecutionId,
    pub component_id: ComponentId,
    pub version_min_including: Version,
    pub version_max_excluding: Version,
    pub wasm_backtrace: WasmBacktrace,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WasmBacktrace {
    pub frames: Vec<FrameInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FrameInfo {
    pub module: String,
    pub func_name: String,
    pub symbols: Vec<FrameSymbol>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FrameSymbol {
    pub func_name: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub col: Option<u32>,
}

mod wasm_backtrace {
    use super::{FrameInfo, FrameSymbol, WasmBacktrace};

    impl WasmBacktrace {
        pub fn maybe_from(backtrace: &wasmtime::WasmBacktrace) -> Option<Self> {
            if backtrace.frames().is_empty() {
                None
            } else {
                Some(Self {
                    frames: backtrace.frames().iter().map(FrameInfo::from).collect(),
                })
            }
        }
    }

    impl From<&wasmtime::FrameInfo> for FrameInfo {
        fn from(frame: &wasmtime::FrameInfo) -> Self {
            let module_name = frame.module().name().unwrap_or("<unknown>").to_string();
            let mut func_name = String::new();
            wasmtime_environ::demangle_function_name_or_index(
                &mut func_name,
                frame.func_name(),
                frame.func_index() as usize,
            )
            .expect("writing to string must succeed");
            Self {
                module: module_name,
                func_name,
                symbols: frame
                    .symbols()
                    .iter()
                    .map(std::convert::Into::into)
                    .collect(),
            }
        }
    }

    impl From<&wasmtime::FrameSymbol> for FrameSymbol {
        fn from(symbol: &wasmtime::FrameSymbol) -> Self {
            let func_name = symbol.name().map(|name| {
                let mut writer = String::new();
                wasmtime_environ::demangle_function_name(&mut writer, name)
                    .expect("writing to string must succeed");
                writer
            });

            Self {
                func_name,
                file: symbol.file().map(ToString::to_string),
                line: symbol.line(),
                col: symbol.column(),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResponseWithCursor {
    pub event: JoinSetResponseEventOuter,
    pub cursor: u32,
}

pub struct ExecutionWithState {
    pub execution_id: ExecutionId,
    pub ffqn: FunctionFqn,
    pub pending_state: PendingState,
    pub created_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
}

pub enum ExecutionListPagination {
    CreatedBy(Pagination<Option<DateTime<Utc>>>),
    ExecutionId(Pagination<Option<ExecutionId>>),
}

#[derive(Debug, Clone, Copy)]
pub enum Pagination<T> {
    NewerThan {
        length: u32,
        cursor: T,
        including_cursor: bool,
    },
    OlderThan {
        length: u32,
        cursor: T,
        including_cursor: bool,
    },
}
impl<T> Pagination<T> {
    pub fn length(&self) -> u32 {
        match self {
            Pagination::NewerThan { length, .. } | Pagination::OlderThan { length, .. } => *length,
        }
    }
    pub fn rel(&self) -> &'static str {
        match self {
            Pagination::NewerThan {
                including_cursor: false,
                ..
            } => ">",
            Pagination::NewerThan {
                including_cursor: true,
                ..
            } => ">=",
            Pagination::OlderThan {
                including_cursor: false,
                ..
            } => "<",
            Pagination::OlderThan {
                including_cursor: true,
                ..
            } => "<=",
        }
    }
    pub fn is_desc(&self) -> bool {
        matches!(self, Pagination::OlderThan { .. })
    }
}

#[cfg(feature = "test")]
pub async fn wait_for_pending_state_fn<T: Debug>(
    db_connection: &dyn DbConnection,
    execution_id: &ExecutionId,
    predicate: impl Fn(ExecutionLog) -> Option<T> + Send,
    timeout: Option<Duration>,
) -> Result<T, ClientError> {
    tracing::trace!(%execution_id, "Waiting for predicate");
    let fut = async move {
        loop {
            let execution_log = db_connection.get(execution_id).await?;
            if let Some(t) = predicate(execution_log) {
                tracing::debug!(%execution_id, "Found: {t:?}");
                return Ok(t);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    if let Some(timeout) = timeout {
        tokio::select! { // future's liveness: Dropping the loser immediately.
            res = fut => res,
            () = tokio::time::sleep(timeout) => Err(ClientError::Timeout)
        }
    } else {
        fut.await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpiredTimer {
    Lock {
        execution_id: ExecutionId,
        locked_at_version: Version,
        version: Version, // Next version
        /// As the execution may still be running, this represents the number of temporary failures + timeouts prior to this execution.
        temporary_event_count: u32,
        max_retries: u32,
        retry_exp_backoff: Duration,
        parent: Option<(ExecutionId, JoinSetId)>,
    },
    Delay {
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        delay_id: DelayId,
    },
}

#[derive(Debug, Clone, derive_more::Display, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PendingState {
    #[display("Locked(`{lock_expires_at}`, {executor_id}, {run_id})")]
    Locked {
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
    },
    #[display("PendingAt(`{scheduled_at}`)")]
    PendingAt { scheduled_at: DateTime<Utc> }, // e.g. created with a schedule, temporary timeout/failure
    #[display("BlockedByJoinSet({join_set_id},`{lock_expires_at}`)")]
    /// Caused by [`HistoryEvent::JoinNext`]
    BlockedByJoinSet {
        join_set_id: JoinSetId,
        /// See [`HistoryEvent::JoinNext::lock_expires_at`].
        lock_expires_at: DateTime<Utc>,
        /// Blocked by closing of the join set
        closing: bool,
    },
    #[display("Finished({finished})")]
    Finished { finished: PendingStateFinished },
}

#[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq, Serialize, Deserialize)]
#[display("{result_kind}")]
pub struct PendingStateFinished {
    pub version: VersionType, // not Version since it must be Copy
    pub finished_at: DateTime<Utc>,
    pub result_kind: PendingStateFinishedResultKind,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde_with::SerializeDisplay, serde_with::DeserializeFromStr,
)]
pub struct PendingStateFinishedResultKind(pub Result<(), PendingStateFinishedError>);
const OK_VARIANT: &str = "ok";
impl FromStr for PendingStateFinishedResultKind {
    type Err = strum::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == OK_VARIANT {
            Ok(PendingStateFinishedResultKind(Ok(())))
        } else {
            let err = PendingStateFinishedError::from_str(s)?;
            Ok(PendingStateFinishedResultKind(Err(err)))
        }
    }
}

impl Display for PendingStateFinishedResultKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Ok(()) => write!(f, "{OK_VARIANT}"),
            Err(err) => write!(f, "{err}"),
        }
    }
}

impl From<&FinishedExecutionResult> for PendingStateFinishedResultKind {
    fn from(result: &FinishedExecutionResult) -> Self {
        match result {
            Ok(supported_fn_return_value) => {
                supported_fn_return_value.as_pending_state_finished_result()
            }
            Err(err) => PendingStateFinishedResultKind(Err(err.as_pending_state_finished_error())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::Display)]
#[cfg_attr(test, derive(strum::VariantArray))]
#[strum(serialize_all = "snake_case")]
pub enum PendingStateFinishedError {
    Timeout,
    UnhandledChildExecutionError,
    ExecutionFailure,
    FallibleError,
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
                if executor_id == *current_pending_state_executor_id // if the executor_id is same, component_id must be same as well
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
            PendingState::Finished { .. } => Err(SpecificError::ValidationFailed(
                StrVariant::Static("already finished"),
            )),
        }
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        matches!(self, PendingState::Finished { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    Extending,
    CreatingNewLock,
}

pub mod http_client_trace {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct HttpClientTrace {
        pub req: RequestTrace,
        pub resp: Option<ResponseTrace>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RequestTrace {
        pub sent_at: DateTime<Utc>,
        pub uri: String,
        pub method: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ResponseTrace {
        pub finished_at: DateTime<Utc>,
        pub status: Result<u16, String>,
    }
}

#[cfg(test)]
mod tests {
    use super::HistoryEventScheduleAt;
    use super::JoinSetResponse;
    use super::PendingStateFinished;
    use super::PendingStateFinishedError;
    use super::PendingStateFinishedResultKind;
    use crate::ExecutionId;
    use crate::FinishedExecutionResult;
    use crate::SupportedFunctionReturnValue;
    use crate::storage::Version;
    use assert_matches::assert_matches;
    use chrono::DateTime;
    use chrono::Datelike;
    use chrono::Utc;
    use rstest::rstest;
    use serde_json::json;
    use std::str::FromStr;
    use std::time::Duration;
    use strum::VariantArray as _;
    use val_json::type_wrapper::TypeWrapper;
    use val_json::wast_val::WastVal;
    use val_json::wast_val::WastValWithType;

    #[rstest(expected => [
        PendingStateFinishedResultKind(Result::Ok(())),
        PendingStateFinishedResultKind(Result::Err(PendingStateFinishedError::Timeout)),
    ])]
    #[test]
    fn serde_pending_state_finished_result_kind_should_work(
        expected: PendingStateFinishedResultKind,
    ) {
        let ser = serde_json::to_string(&expected).unwrap();
        let actual: PendingStateFinishedResultKind = serde_json::from_str(&ser).unwrap();
        assert_eq!(expected, actual);
    }

    #[rstest(result_kind => [
        PendingStateFinishedResultKind(Result::Ok(())),
        PendingStateFinishedResultKind(Result::Err(PendingStateFinishedError::Timeout)),
    ])]
    #[test]
    fn serde_pending_state_finished_should_work(result_kind: PendingStateFinishedResultKind) {
        let expected = PendingStateFinished {
            version: 0,
            finished_at: Utc::now(),
            result_kind,
        };

        let ser = serde_json::to_string(&expected).unwrap();
        let actual: PendingStateFinished = serde_json::from_str(&ser).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn join_set_deser_with_result_ok_option_none_should_work() {
        let json = json!({
            "type": "ChildExecutionFinished",
            "child_execution_id": "E_01JGKY3WWV7Z24NP9BJF90JZHB.g:gg_1",
            "finished_version": 2,
            "result": {
                "Ok": {
                    "InfallibleOrResultOk": {
                        "type": {
                            "result": {
                                "ok": {
                                    "option": "string"
                                },
                                "err": "string"
                            }
                        },
                        "value": {
                            "ok": null
                        }
                    }
                }
            }
        });
        let actual: JoinSetResponse = serde_json::from_value(json).unwrap();
        let (child_execution_id, finished_version, wast_val_with_type) = assert_matches!(
            actual,
            JoinSetResponse::ChildExecutionFinished {
                child_execution_id,
                finished_version,
                result: FinishedExecutionResult::Ok(SupportedFunctionReturnValue::InfallibleOrResultOk(wast_val_with_type))
            } => (child_execution_id, finished_version, wast_val_with_type)
        );
        assert_eq!(
            ExecutionId::from_str("E_01JGKY3WWV7Z24NP9BJF90JZHB.g:gg_1").unwrap(),
            ExecutionId::Derived(child_execution_id)
        );
        assert_eq!(Version(2), finished_version);

        let expected = WastValWithType {
            r#type: TypeWrapper::Result {
                ok: Some(Box::new(TypeWrapper::Option(Box::new(TypeWrapper::String)))),
                err: Some(Box::new(TypeWrapper::String)),
            },
            value: WastVal::Result(Ok(Some(Box::new(WastVal::Option(None))))),
        };
        assert_eq!(expected, wast_val_with_type);
    }

    #[test]
    fn verify_pending_state_finished_result_kind_serde() {
        let variants: Vec<_> = PendingStateFinishedError::VARIANTS
            .iter()
            .map(|var| PendingStateFinishedResultKind(Err(*var)))
            .chain(std::iter::once(PendingStateFinishedResultKind(Ok(()))))
            .collect();
        let ser = serde_json::to_string_pretty(&variants).unwrap();
        insta::assert_snapshot!(ser);
        let deser: Vec<PendingStateFinishedResultKind> = serde_json::from_str(&ser).unwrap();
        assert_eq!(variants, deser);
    }

    #[test]
    fn as_date_time_should_work_with_duration_u32_max_secs() {
        let duration = Duration::from_secs(u64::from(u32::MAX));
        let schedule_at = HistoryEventScheduleAt::In(duration);
        let resolved = schedule_at.as_date_time(DateTime::UNIX_EPOCH).unwrap();
        assert_eq!(2106, resolved.year());
    }

    const MILLIS_PER_SEC: i64 = 1000;
    const TIMEDELTA_MAX_SECS: i64 = i64::MAX / MILLIS_PER_SEC;

    #[test]
    fn as_date_time_should_fail_on_duration_secs_greater_than_i64_max() {
        // Fails on duration -> timedelta conversion, but a smaller duration can fail on datetime + timedelta
        let duration = Duration::from_secs(
            u64::try_from(TIMEDELTA_MAX_SECS).expect("positive number must not fail") + 1,
        );
        let schedule_at = HistoryEventScheduleAt::In(duration);
        schedule_at.as_date_time(DateTime::UNIX_EPOCH).unwrap_err();
    }
}
