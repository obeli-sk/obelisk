use crate::prefixed_ulid::DelayId;
use crate::prefixed_ulid::ExecutorId;
use crate::prefixed_ulid::JoinSetId;
use crate::prefixed_ulid::RunId;
use crate::ConfigId;
use crate::ExecutionId;
use crate::ExecutionMetadata;
use crate::FinishedExecutionResult;
use crate::FunctionFqn;
use crate::Params;
use crate::StrVariant;
use crate::SupportedFunctionReturnValue;
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use derive_more::FromStrError;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::ops::Deref as _;
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
        intermittent_event_count: u32,
        max_retries: u32,
        retry_exp_backoff: Duration,
    ) -> Option<Duration> {
        // If max_retries == u32::MAX, wrapping is OK after this succeeds - we want to retry forever.
        if intermittent_event_count <= max_retries {
            // TODO: Add test for number of retries
            let duration = retry_exp_backoff * 2_u32.saturating_pow(intermittent_event_count - 1);
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
        }) => parent.clone())
    }

    #[must_use]
    pub fn last_event(&self) -> &ExecutionEvent {
        self.events.last().expect("must contain at least one event")
    }

    #[must_use]
    pub fn into_finished_result(mut self) -> Option<FinishedExecutionResult> {
        if let ExecutionEvent {
            event: ExecutionEventInner::Finished { result },
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

pub type VersionType = u32;
#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
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

#[derive(Clone, Debug, PartialEq, Eq, arbitrary::Arbitrary, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JoinSetResponse {
    DelayFinished {
        delay_id: DelayId,
    },
    ChildExecutionFinished {
        child_execution_id: ExecutionId,
        #[arbitrary(value = Ok(SupportedFunctionReturnValue::None))]
        result: FinishedExecutionResult,
    },
}

impl JoinSetResponse {
    #[must_use]
    pub fn delay_id(&self) -> Option<DelayId> {
        if let JoinSetResponse::DelayFinished { delay_id } = self {
            Some(*delay_id)
        } else {
            None
        }
    }

    #[must_use]
    pub fn child_execution_id(&self) -> Option<ExecutionId> {
        if let JoinSetResponse::ChildExecutionFinished {
            child_execution_id, ..
        } = self
        {
            Some(child_execution_id.clone())
        } else {
            None
        }
    }
}

pub const DUMMY_CREATED: ExecutionEventInner = ExecutionEventInner::Created {
    ffqn: FunctionFqn::new_static("", ""),
    params: Params::empty(),
    parent: None,
    scheduled_at: DateTime::from_timestamp_nanos(0),
    retry_exp_backoff: Duration::ZERO,
    max_retries: 0,
    config_id: ConfigId::dummy_activity(),
    metadata: ExecutionMetadata::empty(),
    scheduled_by: None,
};
pub const DUMMY_HISTORY_EVENT: ExecutionEventInner = ExecutionEventInner::HistoryEvent {
    event: HistoryEvent::JoinSet {
        join_set_id: JoinSetId::from_parts(0, 0),
    },
};
pub const DUMMY_INTERMITTENT_TIMEOUT: ExecutionEventInner =
    ExecutionEventInner::IntermittentTimedOut {
        backoff_expires_at: DateTime::from_timestamp_nanos(0),
    };
pub const DUMMY_INTERMITTENT_FAILURE: ExecutionEventInner =
    ExecutionEventInner::IntermittentlyFailed {
        backoff_expires_at: DateTime::from_timestamp_nanos(0),
        reason: StrVariant::empty(),
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
#[allow(clippy::large_enum_variant)]
pub enum ExecutionEventInner {
    /// Created by an external system or a scheduler when requesting a child execution or
    /// an executor when continuing as new `FinishedExecutionError`::`ContinueAsNew`,`CancelledWithNew` .
    /// The execution is [`PendingState::PendingAt`]`(scheduled_at)`.
    #[display("Created({ffqn}, `{scheduled_at}`)")]
    Created {
        ffqn: FunctionFqn,
        #[arbitrary(default)]
        params: Params,
        parent: Option<(ExecutionId, JoinSetId)>,
        scheduled_at: DateTime<Utc>,
        retry_exp_backoff: Duration,
        max_retries: u32,
        #[arbitrary(value = ConfigId::dummy_activity())]
        config_id: ConfigId,
        #[arbitrary(default)]
        metadata: ExecutionMetadata,
        scheduled_by: Option<ExecutionId>,
    },
    // Created by an executor.
    // Either immediately followed by an execution request by an executor or
    // after expiry immediately followed by WaitingForExecutor by a scheduler.
    #[display("Locked(`{lock_expires_at}`, {config_id})")]
    Locked {
        #[arbitrary(value = ConfigId::dummy_activity())]
        config_id: ConfigId,
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
    },
    /// Returns execution to [`PendingState::PendingNow`] state
    /// without timing out. This can happen when the executor is running
    /// out of resources like [`WorkerError::LimitReached`] or when
    /// the executor is shutting down.
    Unlocked,
    // Created by the executor holding the lock.
    // After expiry interpreted as pending.
    #[display("IntermittentlyFailed(`{backoff_expires_at}`)")]
    IntermittentlyFailed {
        backoff_expires_at: DateTime<Utc>,
        #[arbitrary(value = StrVariant::Static("reason"))]
        reason: StrVariant,
    },
    // Created by the executor holding last lock.
    // After expiry interpreted as pending.
    #[display("IntermittentlyTimedOut(`{backoff_expires_at}`)")]
    IntermittentTimedOut { backoff_expires_at: DateTime<Utc> },
    // Created by the executor holding last lock.
    // Processed by a scheduler if a parent execution needs to be notified,
    // also when
    #[display("Finished")]
    Finished {
        #[arbitrary(value = Ok(SupportedFunctionReturnValue::None))]
        result: FinishedExecutionResult,
    },

    #[display("HistoryEvent({event})")]
    HistoryEvent { event: HistoryEvent },
}

impl ExecutionEventInner {
    #[must_use]
    pub fn is_intermittent_event(&self) -> bool {
        matches!(
            self,
            Self::IntermittentlyFailed { .. } | Self::IntermittentTimedOut { .. }
        )
    }

    #[must_use]
    pub fn variant(&self) -> &'static str {
        Into::<&'static str>::into(self)
    }

    #[must_use]
    pub fn join_set_id(&self) -> Option<JoinSetId> {
        match self {
            Self::Created {
                parent: Some((_parent_id, join_set_id)),
                ..
            } => Some(*join_set_id),
            Self::HistoryEvent {
                event:
                    HistoryEvent::JoinSet { join_set_id }
                    | HistoryEvent::JoinSetRequest { join_set_id, .. }
                    | HistoryEvent::JoinNext { join_set_id, .. },
            } => Some(*join_set_id),
            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary, Serialize, Deserialize,
)]
#[serde(tag = "type")]
pub enum HistoryEvent {
    #[display("Persist")]
    /// Must be created by the executor in [`PendingState::Locked`].
    Persist { value: Vec<u8> },
    /// Must be created by the executor in [`PendingState::Locked`].
    #[display("JoinSet({join_set_id})")]
    JoinSet {
        join_set_id: JoinSetId,
        // TODO: add JoinSetKind (unordered, ordered)
    },
    #[display("JoinSetRequest({join_set_id}, {request})")]
    JoinSetRequest {
        join_set_id: JoinSetId,
        request: JoinSetRequest,
    },
    /// Must be created by the executor in [`PendingState::Locked`].
    /// Pending state is set to [`PendingState::BlockedByJoinSet`].
    /// When the response arrives at `resp_time`:
    /// The execution is [`PendingState::PendingAt`]`(max(resp_time, lock_expires_at)`, so that the
    /// original executor can continue. After the expiry any executor can continue without
    /// marking the execution as timed out.
    #[display("JoinNext({join_set_id})")]
    JoinNext {
        join_set_id: JoinSetId,
        /// Set to a future time if the worker is keeping the execution invocation warm waiting for the result.
        run_expires_at: DateTime<Utc>,
        /// Is the joinset being closed?
        closing: bool,
    },
    #[display("Schedule({execution_id}, {scheduled_at})")]
    Schedule {
        execution_id: ExecutionId,
        scheduled_at: HistoryEventScheduledAt,
    },
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    derive_more::Display,
    arbitrary::Arbitrary,
    Serialize,
    Deserialize,
)]
pub enum HistoryEventScheduledAt {
    Now,
    At(DateTime<Utc>),
    #[display("In({_0:?})")]
    In(Duration),
}
impl HistoryEventScheduledAt {
    #[must_use]
    pub fn as_date_time(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Self::Now => now,
            Self::At(date_time) => *date_time,
            Self::In(duration) => now + *duration,
        }
    }
}
impl TryFrom<&wasmtime::component::Val> for HistoryEventScheduledAt {
    type Error = &'static str;

    fn try_from(scheduled_at: &wasmtime::component::Val) -> Result<Self, Self::Error> {
        use wasmtime::component::Val;
        let Val::Variant(variant, val) = scheduled_at else {
            return Err("wrong type");
        };
        match (variant.as_str(), val) {
            ("now", None) => Ok(HistoryEventScheduledAt::Now),
            ("in", Some(duration)) => {
                if let &Val::Variant(key, value) = &duration.deref() {
                    let duration = match (key.as_str(), value.as_deref()) {
                        ("milliseconds", Some(Val::U64(value))) => Duration::from_millis(*value),
                        ("seconds", Some(Val::U64(value))) => Duration::from_secs(*value),
                        ("minutes", Some(Val::U64(value))) => Duration::from_secs(*value * 60),
                        ("hours", Some(Val::U64(value))) => Duration::from_secs(*value * 60 * 60),
                        ("days", Some(Val::U64(value))) => {
                            Duration::from_secs(*value * 60 * 60 * 24)
                        },
                        _ => return Err(
                            "cannot convert `scheduled-at`, `in` variant: Allowed keys: `milliseconds`(U64), `seconds`(U64), `minutes`(U32), `hours`(U32), `days`(U32)",
                        )
                    };
                    Ok(HistoryEventScheduledAt::In(duration))
                } else {
                    todo!()
                }
            }
            ("at", Some(date_time)) if matches!(date_time.deref(), Val::Record(_)) => {
                let date_time =
                    assert_matches!(date_time.deref(), Val::Record(keys_vals) => keys_vals)
                        .iter()
                        .map(|(k, v)| (k.as_str(), v))
                        .collect::<HashMap<_, _>>();
                let seconds = date_time.get("seconds");
                let nanos = date_time.get("nanoseconds");
                match (date_time.len(), seconds, nanos) {
                    (2, Some(Val::U64(seconds)), Some(Val::U32(nanos))) => {
                        let Ok(seconds) = i64::try_from(*seconds) else {
                            return Err("cannot convert `scheduled-at`, cannot convert seconds from u64 to i64");
                        };
                        let Some(date_time) = DateTime::from_timestamp(seconds, *nanos) else {
                            return Err("cannot convert `scheduled-at`, cannot convert seconds and nanos to DateTime");
                        };
                        Ok(HistoryEventScheduledAt::At(date_time))
                    }
                    _ => {
                        Err(
                            "cannot convert `scheduled-at`, `at` variant: record must have exactly two keys: `seconds`(U64), `nanoseconds`(U32)",
                        )
                    }
                }
            }
            _ => Err("cannot convert `scheduled-at` variant, expected one of `now`, `in`, `at`"),
        }
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, derive_more::Display, arbitrary::Arbitrary, Serialize, Deserialize,
)]
#[serde(tag = "type")]
pub enum JoinSetRequest {
    // Must be created by the executor in `PendingState::Locked`.
    #[display("DelayRequest({delay_id}, expires_at: `{expires_at}`)")]
    DelayRequest {
        delay_id: DelayId,
        expires_at: DateTime<Utc>,
    },
    // Must be created by the executor in `PendingState::Locked`.
    #[display("ChildExecutionRequest({child_execution_id})")]
    ChildExecutionRequest { child_execution_id: ExecutionId },
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
    pub intermittent_event_count: u32,
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
    pub config_id: ConfigId,
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
            config_id: value.config_id,
            metadata: value.metadata,
            scheduled_by: value.scheduled_by,
        }
    }
}

#[async_trait]
pub trait DbPool<DB: DbConnection>: Send + Sync + Clone {
    fn connection(&self) -> DB;
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
        config_id: ConfigId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError>;

    /// Specialized `append` which returns the event history.
    #[expect(clippy::too_many_arguments)]
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        config_id: ConfigId,
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
        execution_id: ExecutionId,
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
    ) -> Result<Vec<ExecutionEvent>, DbError>;

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
            config_id,
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
                config_id,
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
        if let ExecutionEventInner::Finished { result } = last_event.event {
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
    /// Return imediately if there are pending notifications at `pending_at_or_sooner`.
    /// Implementation must return not later than at expiry date, which is: `pending_at_or_sooner` + `max_wait`.
    /// Timers that expire until the expiry date can be disregarded.
    /// Databases that do not support subscriptions should wait for `max_wait`.
    // FIXME: Rename to wait_for_pending
    async fn subscribe_to_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        max_wait: Duration,
    );

    /// Returns executions sorted in descending order.
    /// Used by gRPC only.
    async fn list_executions(
        &self,
        ffqn: Option<FunctionFqn>,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbError>;
}

pub struct ExecutionWithState {
    pub execution_id: ExecutionId,
    pub ffqn: FunctionFqn,
    pub pending_state: PendingState,
    pub created_at: DateTime<Utc>,
}

pub enum ExecutionListPagination {
    CreatedBy(Pagination<DateTime<Utc>>),
    ExecutionId(Pagination<ExecutionId>),
}

#[derive(Debug, Clone, Copy)]
pub enum Pagination<T> {
    NewerThan {
        length: u32,
        cursor: Option<T>,
        including_cursor: bool,
    },
    OlderThan {
        length: u32,
        cursor: Option<T>,
        including_cursor: bool,
    },
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
        version: Version,
        /// As the execution may still be running, this represents the number of intermittent failures prior to this execution.
        intermittent_event_count: u32,
        max_retries: u32,
        retry_exp_backoff: Duration,
        parent: Option<(ExecutionId, JoinSetId)>,
    },
    AsyncDelay {
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        delay_id: DelayId,
    },
}

#[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PendingState {
    #[display("Locked(`{lock_expires_at}`, {executor_id}, {run_id})")]
    Locked {
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
    },
    #[display("PendingAt(`{scheduled_at}`)")]
    PendingAt { scheduled_at: DateTime<Utc> }, // e.g. created with a schedule, intermittent timeout/failure
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
const OK_VARIANT: &str = "Ok";
impl FromStr for PendingStateFinishedResultKind {
    type Err = FromStrError;

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

#[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq, derive_more::FromStr)]
#[display("{_0}")]
pub enum PendingStateFinishedError {
    Timeout,
    NondeterminismDetected,
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
                if executor_id == *current_pending_state_executor_id // if the executor_id is same, config_id must be same as well
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

#[cfg(test)]
mod tests {
    use super::PendingStateFinished;
    use super::PendingStateFinishedError;
    use super::PendingStateFinishedResultKind;
    use chrono::Utc;
    use rstest::rstest;

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
}
