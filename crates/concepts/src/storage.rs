use crate::ComponentId;
use crate::ComponentRetryConfig;
use crate::ComponentType;
use crate::ExecutionFailureKind;
use crate::ExecutionId;
use crate::ExecutionMetadata;
use crate::FinishedExecutionError;
use crate::FunctionFqn;
use crate::JoinSetId;
use crate::Params;
use crate::StrVariant;
use crate::SupportedFunctionReturnValue;
use crate::component_id::InputContentDigest;
use crate::prefixed_ulid::DelayId;
use crate::prefixed_ulid::DeploymentId;
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
use std::panic::Location;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;
use tracing::instrument;
use tracing_error::SpanTrace;

// Shared between databases. TODO: Extract to db-common
pub const STATE_PENDING_AT: &str = "pending_at";
pub const STATE_BLOCKED_BY_JOIN_SET: &str = "blocked_by_join_set";
pub const STATE_LOCKED: &str = "locked";
pub const STATE_FINISHED: &str = "finished";
pub const HISTORY_EVENT_TYPE_JOIN_NEXT: &str = "join_next"; // Serialization tag of `HistoryEvent::JoinNext`

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ExecutionLog {
    pub execution_id: ExecutionId,
    pub events: Vec<ExecutionEvent>,
    pub responses: Vec<ResponseWithCursor>,
    pub next_version: Version, // Is not advanced once in Finished state
    pub pending_state: PendingState, // reflecting the current state
    pub component_digest: InputContentDigest, // reflecting the current state
    pub component_type: ComponentType,
    pub deployment_id: DeploymentId, // reflecting the current state
}

impl ExecutionLog {
    /// Return some duration after which the execution will be retried.
    /// Return `None` if no more retries are allowed.
    #[must_use]
    pub fn can_be_retried_after(
        temporary_event_count: u32,
        max_retries: Option<u32>,
        retry_exp_backoff: Duration,
    ) -> Option<Duration> {
        // If max_retries == None, wrapping is OK after this succeeds - we want to retry forever.
        if temporary_event_count <= max_retries.unwrap_or(u32::MAX) {
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
        Self::can_be_retried_after(temporary_event_count, None, retry_exp_backoff)
            .expect("`max_retries` set to MAX must never return None")
    }

    #[must_use]
    pub fn ffqn(&self) -> &FunctionFqn {
        assert_matches!(self.events.first(), Some(ExecutionEvent {
            event: ExecutionRequest::Created { ffqn, .. },
            ..
        }) => ffqn)
    }

    #[must_use]
    pub fn params(&self) -> &Params {
        assert_matches!(self.events.first(), Some(ExecutionEvent {
            event: ExecutionRequest::Created { params, .. },
            ..
        }) => params)
    }

    #[must_use]
    pub fn parent(&self) -> Option<(ExecutionId, JoinSetId)> {
        assert_matches!(self.events.first(), Some(ExecutionEvent {
            event: ExecutionRequest::Created { parent, .. },
            ..
        }) => parent.clone())
    }

    #[must_use]
    pub fn last_event(&self) -> &ExecutionEvent {
        self.events.last().expect("must contain at least one event")
    }

    #[must_use]
    pub fn as_finished_result(&self) -> Option<SupportedFunctionReturnValue> {
        if let ExecutionEvent {
            event: ExecutionRequest::Finished { result, .. },
            ..
        } = self.events.last().expect("must contain at least one event")
        {
            Some(result.clone())
        } else {
            None
        }
    }

    pub fn event_history(&self) -> impl Iterator<Item = (HistoryEvent, Version)> + '_ {
        self.events.iter().filter_map(|event| {
            if let ExecutionRequest::HistoryEvent { event: eh, .. } = &event.event {
                Some((eh.clone(), event.version.clone()))
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
                ExecutionRequest::HistoryEvent {
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
    pub fn new(arg: VersionType) -> Version {
        Version(arg)
    }

    #[must_use]
    pub fn increment(&self) -> Version {
        Version(self.0 + 1)
    }
}
impl TryFrom<i64> for Version {
    type Error = VersionParseError;
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        VersionType::try_from(value)
            .map(Version::new)
            .map_err(|_| VersionParseError)
    }
}
impl From<Version> for usize {
    fn from(value: Version) -> Self {
        usize::try_from(value.0).expect("16 bit systems are unsupported")
    }
}
impl From<&Version> for usize {
    fn from(value: &Version) -> Self {
        usize::try_from(value.0).expect("16 bit systems are unsupported")
    }
}

#[derive(Debug, thiserror::Error)]
#[error("version must be u32")]
pub struct VersionParseError;

#[derive(
    Clone, Debug, derive_more::Display, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[display("{event}")]
pub struct ExecutionEvent {
    pub created_at: DateTime<Utc>,
    pub event: ExecutionRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace_id: Option<Version>,
    pub version: Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize /* webapi */)]
pub struct ResponseCursor(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Serialize /* webapi */)]
pub struct ResponseWithCursor {
    pub event: JoinSetResponseEventOuter,
    pub cursor: ResponseCursor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize /* webapi */)]
pub struct JoinSetResponseEventOuter {
    pub created_at: DateTime<Utc>,
    pub event: JoinSetResponseEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JoinSetResponseEvent {
    pub join_set_id: JoinSetId,
    pub event: JoinSetResponse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, derive_more::Display)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JoinSetResponse {
    #[display("delay {}: {delay_id}", if result.is_ok() { "finished" } else { "cancelled"})]
    DelayFinished {
        delay_id: DelayId,
        result: Result<(), ()>,
    },
    #[display("{result}: {child_execution_id}")] // execution completed..
    ChildExecutionFinished {
        child_execution_id: ExecutionIdDerived,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = Version(2)))]
        finished_version: Version,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = crate::SUPPORTED_RETURN_VALUE_OK_EMPTY))]
        result: SupportedFunctionReturnValue,
    },
}

pub const DUMMY_CREATED: ExecutionRequest = ExecutionRequest::Created {
    ffqn: FunctionFqn::new_static("", ""),
    params: Params::empty(),
    parent: None,
    scheduled_at: DateTime::from_timestamp_nanos(0),
    component_id: ComponentId::dummy_activity(),
    deployment_id: DeploymentId::from_parts(0, 0),
    metadata: ExecutionMetadata::empty(),
    scheduled_by: None,
};
pub const DUMMY_HISTORY_EVENT: ExecutionRequest = ExecutionRequest::HistoryEvent {
    event: HistoryEvent::JoinSetCreate {
        join_set_id: JoinSetId {
            kind: crate::JoinSetKind::OneOff,
            name: StrVariant::empty(),
        },
    },
};

#[derive(
    Clone, derive_more::Debug, derive_more::Display, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum ExecutionRequest {
    #[display("Created({ffqn}, `{scheduled_at}`)")]
    Created {
        ffqn: FunctionFqn,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = Params::empty()))]
        #[debug(skip)]
        params: Params,
        parent: Option<(ExecutionId, JoinSetId)>,
        scheduled_at: DateTime<Utc>,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = ComponentId::dummy_activity()))]
        component_id: ComponentId,
        deployment_id: DeploymentId,
        #[cfg_attr(any(test, feature = "test"), arbitrary(default))]
        metadata: ExecutionMetadata,
        scheduled_by: Option<ExecutionId>,
    },
    Locked(Locked),
    /// Returns execution to [`PendingState::PendingAt`] state at the specified time.
    /// This can happen when:
    /// - executor is running out of resources like [`WorkerError::LimitReached`]
    /// - executor is shutting down
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
        reason: StrVariant,
        detail: Option<String>,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = None))]
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },
    // Created by the executor holding the lock.
    // After expiry interpreted as pending.
    #[display("TemporarilyTimedOut(`{backoff_expires_at}`)")]
    TemporarilyTimedOut {
        backoff_expires_at: DateTime<Utc>,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = None))]
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },
    // Created by the executor holding the lock.
    #[display("Finished")]
    Finished {
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = crate::SUPPORTED_RETURN_VALUE_OK_EMPTY))]
        result: SupportedFunctionReturnValue,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = None))]
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },

    #[display("HistoryEvent({event})")]
    HistoryEvent {
        event: HistoryEvent,
    },
    #[display("Paused")]
    Paused,
    #[display("Unpaused")]
    Unpaused,
}

impl ExecutionRequest {
    #[must_use]
    pub fn is_temporary_event(&self) -> bool {
        matches!(
            self,
            Self::TemporarilyFailed { .. } | Self::TemporarilyTimedOut { .. }
        )
    }

    /// String representation of `ExecutionRequest`, used in execution log table to fetch events of certain type, e.g. `created` + `history_event`.
    #[must_use]
    pub const fn variant(&self) -> &'static str {
        match self {
            ExecutionRequest::Created { .. } => "created",
            ExecutionRequest::Locked(_) => "locked",
            ExecutionRequest::Unlocked { .. } => "unlocked",
            ExecutionRequest::TemporarilyFailed { .. } => "temporarily_failed",
            ExecutionRequest::TemporarilyTimedOut { .. } => "temporarily_timed_out",
            ExecutionRequest::Finished { .. } => "finished",
            ExecutionRequest::HistoryEvent { .. } => "history_event",
            ExecutionRequest::Paused => "paused",
            ExecutionRequest::Unpaused => "unpaused",
        }
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

#[derive(
    Clone, derive_more::Debug, derive_more::Display, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[display("Locked(`{lock_expires_at}`, {component_id})")]
pub struct Locked {
    #[cfg_attr(any(test, feature = "test"), arbitrary(value = ComponentId::dummy_activity()))]
    pub component_id: ComponentId,
    pub executor_id: ExecutorId,
    pub deployment_id: DeploymentId,
    pub run_id: RunId,
    pub lock_expires_at: DateTime<Utc>,
    #[cfg_attr(any(test, feature = "test"), arbitrary(value = ComponentRetryConfig::ZERO))]
    pub retry_config: ComponentRetryConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(tag = "type", rename_all = "snake_case")]
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
#[serde(tag = "type", rename_all = "snake_case")]
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
    JoinSetCreate { join_set_id: JoinSetId },
    #[display("JoinSetRequest({request})")]
    // join_set_id is part of ExecutionId or DelayId in the `request`
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
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = crate::SUPPORTED_RETURN_VALUE_OK_EMPTY))]
        result: SupportedFunctionReturnValue, // Only stored for nondeterminism checks. TODO: Consider using a hashed value.
        persist_result: Result<(), ()>, // Does the row (target_execution_id,Version:1) match the proposed `result`?
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum HistoryEventScheduleAt {
    Now,
    #[display("At(`{_0}`)")]
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
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JoinSetRequest {
    // Must be created by the executor in `PendingState::Locked`.
    #[display("DelayRequest({delay_id}, expires_at: `{expires_at}`, schedule_at: `{schedule_at}`)")]
    DelayRequest {
        delay_id: DelayId,
        expires_at: DateTime<Utc>,
        schedule_at: HistoryEventScheduleAt,
    },
    // Must be created by the executor in `PendingState::Locked`.
    #[display("ChildExecutionRequest({child_execution_id}, {target_ffqn}, params: {params})")]
    ChildExecutionRequest {
        child_execution_id: ExecutionIdDerived,
        target_ffqn: FunctionFqn,
        #[cfg_attr(any(test, feature = "test"), arbitrary(value = Params::empty()))]
        params: Params,
    },
}

/// Error that is not specific to an execution.
#[derive(Debug, Clone, thiserror::Error, derive_more::PartialEq, derive_more::Eq)]
pub enum DbErrorGeneric {
    #[error("database error: {reason}")]
    Uncategorized {
        reason: StrVariant,
        #[eq(skip)]
        #[partial_eq(skip)]
        context: SpanTrace,
        #[eq(skip)]
        #[partial_eq(skip)]
        #[source]
        source: Option<Arc<dyn std::error::Error + Send + Sync>>,
        loc: &'static Location<'static>,
    },
    #[error("database was closed")]
    Close,
}

#[derive(thiserror::Error, Clone, Debug, derive_more::PartialEq, derive_more::Eq)]
pub enum DbErrorWriteNonRetriable {
    #[error("validation failed: {0}")]
    ValidationFailed(StrVariant),
    #[error("conflict")]
    Conflict,
    #[error("illegal state: {reason}")]
    IllegalState {
        reason: StrVariant,
        #[eq(skip)]
        #[partial_eq(skip)]
        context: SpanTrace,
        #[eq(skip)]
        #[partial_eq(skip)]
        #[source]
        source: Option<Arc<dyn std::error::Error + Send + Sync>>,
        loc: &'static Location<'static>,
    },
    #[error("version conflict: expected: {expected}, got: {requested}")]
    VersionConflict {
        expected: Version,
        requested: Version,
    },
}

/// Write error tied to an execution
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum DbErrorWrite {
    #[error("cannot write - row not found")]
    NotFound,
    #[error("non-retriable error: {0}")]
    NonRetriable(#[from] DbErrorWriteNonRetriable),
    #[error(transparent)]
    Generic(#[from] DbErrorGeneric),
}

/// Read error tied to an execution
#[derive(Debug, Clone, thiserror::Error, PartialEq)]
pub enum DbErrorRead {
    #[error("cannot read - row not found")]
    NotFound,
    #[error(transparent)]
    Generic(#[from] DbErrorGeneric),
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum DbErrorReadWithTimeout {
    #[error("timeout")]
    Timeout(TimeoutOutcome),
    #[error(transparent)]
    DbErrorRead(#[from] DbErrorRead),
}

// Represents next version after successfuly appended to execution log.
// TODO: Convert to struct with next_version
pub type AppendResponse = Version;
pub type PendingExecution = (ExecutionId, Version, Params, Option<DateTime<Utc>>);

#[derive(Debug, Clone)]
pub struct LockedExecution {
    pub execution_id: ExecutionId,
    pub next_version: Version,
    pub metadata: ExecutionMetadata,
    pub locked_event: Locked,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub event_history: Vec<(HistoryEvent, Version)>,
    pub responses: Vec<ResponseWithCursor>,
    pub parent: Option<(ExecutionId, JoinSetId)>,
    pub intermittent_event_count: u32,
}

pub type LockPendingResponse = Vec<LockedExecution>;
pub type AppendBatchResponse = Version;

#[derive(Debug, Clone, derive_more::Display, Serialize, Deserialize)]
#[display("{event}")]
pub struct AppendRequest {
    pub created_at: DateTime<Utc>,
    pub event: ExecutionRequest,
}

#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub created_at: DateTime<Utc>,
    pub execution_id: ExecutionId,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub parent: Option<(ExecutionId, JoinSetId)>,
    pub scheduled_at: DateTime<Utc>,
    pub component_id: ComponentId,
    pub deployment_id: DeploymentId,
    pub metadata: ExecutionMetadata,
    pub scheduled_by: Option<ExecutionId>,
}

impl From<CreateRequest> for ExecutionRequest {
    fn from(value: CreateRequest) -> Self {
        Self::Created {
            ffqn: value.ffqn,
            params: value.params,
            parent: value.parent,
            scheduled_at: value.scheduled_at,
            component_id: value.component_id,
            deployment_id: value.deployment_id,
            metadata: value.metadata,
            scheduled_by: value.scheduled_by,
        }
    }
}

#[async_trait]
pub trait DbPool: Send + Sync {
    async fn db_exec_conn(&self) -> Result<Box<dyn DbExecutor>, DbErrorGeneric>;

    async fn connection(&self) -> Result<Box<dyn DbConnection>, DbErrorGeneric>;

    async fn external_api_conn(&self) -> Result<Box<dyn DbExternalApi>, DbErrorGeneric>;

    #[cfg(feature = "test")]
    async fn connection_test(&self) -> Result<Box<dyn DbConnectionTest>, DbErrorGeneric>;
}

#[async_trait]
pub trait DbPoolCloseable {
    async fn close(&self);
}

#[derive(Clone, Debug)]
pub struct AppendEventsToExecution {
    pub execution_id: ExecutionId,
    pub version: Version,
    pub batch: Vec<AppendRequest>,
}

#[derive(Clone, Debug)]
pub struct AppendResponseToExecution {
    pub parent_execution_id: ExecutionId,
    pub created_at: DateTime<Utc>,
    pub join_set_id: JoinSetId,
    pub child_execution_id: ExecutionIdDerived,
    pub finished_version: Version,
    pub result: SupportedFunctionReturnValue,
}

#[async_trait]
pub trait DbExecutor: Send + Sync {
    #[expect(clippy::too_many_arguments)]
    async fn lock_pending_by_ffqns(
        &self,
        batch_size: u32,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        deployment_id: DeploymentId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
        retry_config: ComponentRetryConfig,
    ) -> Result<LockPendingResponse, DbErrorWrite>;

    #[expect(clippy::too_many_arguments)]
    async fn lock_pending_by_component_digest(
        &self,
        batch_size: u32,
        pending_at_or_sooner: DateTime<Utc>,
        component_id: &ComponentId,
        deployment_id: DeploymentId,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
        retry_config: ComponentRetryConfig,
    ) -> Result<LockPendingResponse, DbErrorWrite>;

    #[cfg(feature = "test")]
    #[expect(clippy::too_many_arguments)]
    async fn lock_one(
        &self,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        deployment_id: DeploymentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        retry_config: ComponentRetryConfig,
    ) -> Result<LockedExecution, DbErrorWrite>;

    /// Append a single event to an existing execution log.
    /// The request cannot contain `ExecutionEventInner::Created`.
    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbErrorWrite>;

    /// Append a batch of events to an existing execution log, and append a response to a parent execution.
    /// The batch cannot contain `ExecutionEventInner::Created`.
    async fn append_batch_respond_to_parent(
        &self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>, // not persisted, can be used for unblocking `subscribe_to_pending`
    ) -> Result<AppendBatchResponse, DbErrorWrite>;

    /// Notification mechainism with no strict guarantees for waiting while there are no pending executions.
    /// Return immediately if there are pending notifications at `pending_at_or_sooner`.
    /// Otherwise wait until `timeout_fut` resolves.
    /// Delay requests that expire between `pending_at_or_sooner` and timeout can be disregarded.
    async fn wait_for_pending_by_ffqn(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    );

    /// Notification mechainism with no strict guarantees for waiting while there are no pending executions.
    /// Return immediately if there are pending notifications at `pending_at_or_sooner`.
    /// Otherwise wait until `timeout_fut` resolves.
    /// Delay requests that expire between `pending_at_or_sooner` and timeout can be disregarded.
    async fn wait_for_pending_by_component_digest(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        component_digest: &InputContentDigest,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    );

    async fn cancel_activity_with_retries(
        &self,
        execution_id: &ExecutionId,
        cancelled_at: DateTime<Utc>,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        let mut retries = 5;
        loop {
            let res = self.cancel_activity(execution_id, cancelled_at).await;
            if res.is_ok() || retries == 0 {
                return res;
            }
            retries -= 1;
        }
    }

    /// Get last event. Impls may set `ExecutionEvent::backtrace_id` to `None`.
    async fn get_last_execution_event(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<ExecutionEvent, DbErrorRead>;

    async fn cancel_activity(
        &self,
        execution_id: &ExecutionId,
        cancelled_at: DateTime<Utc>,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        debug!("Determining cancellation state of {execution_id}");

        let last_event = self
            .get_last_execution_event(execution_id)
            .await
            .map_err(DbErrorWrite::from)?;
        if let ExecutionRequest::Finished {
            result:
                SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError {
                    kind: ExecutionFailureKind::Cancelled,
                    ..
                }),
            ..
        } = last_event.event
        {
            return Ok(CancelOutcome::Cancelled);
        } else if matches!(last_event.event, ExecutionRequest::Finished { .. }) {
            debug!("Not cancelling, {execution_id} is already finished");
            return Ok(CancelOutcome::AlreadyFinished);
        }
        let finished_version = last_event.version.increment();
        let child_result = SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError {
            reason: None,
            kind: ExecutionFailureKind::Cancelled,
            detail: None,
        });
        let cancel_request = AppendRequest {
            created_at: cancelled_at,
            event: ExecutionRequest::Finished {
                result: child_result.clone(),
                http_client_traces: None,
            },
        };
        debug!("Cancelling activity {execution_id} at {finished_version}");
        if let ExecutionId::Derived(execution_id) = execution_id {
            let (parent_execution_id, join_set_id) = execution_id.split_to_parts();
            let child_execution_id = ExecutionId::Derived(execution_id.clone());
            self.append_batch_respond_to_parent(
                AppendEventsToExecution {
                    execution_id: child_execution_id,
                    version: finished_version.clone(),
                    batch: vec![cancel_request],
                },
                AppendResponseToExecution {
                    parent_execution_id,
                    created_at: cancelled_at,
                    join_set_id: join_set_id.clone(),
                    child_execution_id: execution_id.clone(),
                    finished_version,
                    result: child_result,
                },
                cancelled_at,
            )
            .await?;
        } else {
            self.append(execution_id.clone(), finished_version, cancel_request)
                .await?;
        }
        debug!("Cancelled {execution_id}");
        Ok(CancelOutcome::Cancelled)
    }
}

pub enum AppendDelayResponseOutcome {
    Success,
    AlreadyFinished,
    AlreadyCancelled,
}

#[derive(Debug, Clone, Default)]
pub struct ListExecutionsFilter {
    pub ffqn_prefix: Option<String>,
    pub show_derived: bool,
    pub hide_finished: bool,
    pub execution_id_prefix: Option<String>,
    pub component_digest: Option<InputContentDigest>,
    pub deployment_id: Option<DeploymentId>,
}

#[async_trait]
pub trait DbExternalApi: DbConnection {
    /// Get the latest backtrace if version is not set.
    async fn get_backtrace(
        &self,
        execution_id: &ExecutionId,
        filter: BacktraceFilter,
    ) -> Result<BacktraceInfo, DbErrorRead>;

    /// Returns executions sorted in descending order.
    async fn list_executions(
        &self,
        filter: ListExecutionsFilter,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbErrorGeneric>;

    async fn list_execution_events(
        &self,
        execution_id: &ExecutionId,
        since: &Version,
        max_length: VersionType,
        include_backtrace_id: bool,
    ) -> Result<Vec<ExecutionEvent>, DbErrorRead>;

    /// Returns responses of an execution ordered as they arrived,
    /// enabling matching each `JoinNext` to its corresponding response.
    async fn list_responses(
        &self,
        execution_id: &ExecutionId,
        pagination: Pagination<u32>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorRead>;

    async fn list_execution_events_responses(
        &self,
        execution_id: &ExecutionId,
        req_since: &Version,
        req_max_length: VersionType,
        req_include_backtrace_id: bool,
        resp_pagination: Pagination<VersionType>,
    ) -> Result<ExecutionWithStateRequestsResponses, DbErrorRead>;

    async fn upgrade_execution_component(
        &self,
        execution_id: &ExecutionId,
        old: &InputContentDigest,
        new: &InputContentDigest,
    ) -> Result<(), DbErrorWrite>;

    async fn list_logs(
        &self,
        execution_id: &ExecutionId,
        filter: LogFilter,
        pagination: Pagination<u32>,
    ) -> Result<ListLogsResponse, DbErrorRead>;

    async fn list_deployment_states(
        &self,
        current_time: DateTime<Utc>,
        pagination: Pagination<Option<DeploymentId>>,
    ) -> Result<Vec<DeploymentState>, DbErrorRead>;

    /// Pause an execution. Only pending executions can be paused.
    async fn pause_execution(
        &self,
        execution_id: &ExecutionId,
        paused_at: DateTime<Utc>,
    ) -> Result<AppendResponse, DbErrorWrite>;

    /// Unpause an execution. Only paused executions can be unpaused.
    async fn unpause_execution(
        &self,
        execution_id: &ExecutionId,
        unpaused_at: DateTime<Utc>,
    ) -> Result<AppendResponse, DbErrorWrite>;
}
pub const LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH: u16 = 20;
pub const LIST_DEPLOYMENT_STATES_DEFAULT_PAGINATION: Pagination<Option<DeploymentId>> =
    Pagination::OlderThan {
        length: LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH,
        cursor: None,
        including_cursor: false,
    };

pub struct DeploymentState {
    pub deployment_id: DeploymentId,
    pub locked: u32,
    // In `PendingAt` state, scheduled to present or past
    pub pending: u32,
    // In `PendingAt` state, scheduled into the future
    pub scheduled: u32,
    pub blocked: u32,
    pub finished: u32,
}
impl DeploymentState {
    #[must_use]
    pub fn new(deployment_id: DeploymentId) -> Self {
        DeploymentState {
            deployment_id,
            locked: 0,
            pending: 0,
            scheduled: 0,
            blocked: 0,
            finished: 0,
        }
    }
}

#[derive(Debug)]
pub struct ListLogsResponse {
    pub items: Vec<LogEntryRow>,
    pub next_page: Pagination<u32>, // Newer logs can always arrive e.g. via replay
    pub prev_page: Option<Pagination<u32>>, // None if we are already at the beginning
}

#[derive(Debug)]
pub struct LogFilter {
    show_logs: bool,
    show_streams: bool,
    levels: Vec<LogLevel>, // Only applied if `show_logs` = true, empty means return all levels.
    stream_types: Vec<LogStreamType>, // Only applied if `show_streams` = true, empty means return all stream types.
}
impl LogFilter {
    // Constructor for logs only
    #[must_use]
    pub fn show_logs(levels: Vec<LogLevel>) -> LogFilter {
        LogFilter {
            show_logs: true,
            show_streams: false,
            levels,
            stream_types: Vec::new(),
        }
    }
    // Constructor for streams only
    #[must_use]
    pub fn show_streams(stream_types: Vec<LogStreamType>) -> LogFilter {
        LogFilter {
            show_logs: false,
            show_streams: true,
            levels: Vec::new(),
            stream_types,
        }
    }
    // Constructor for both logs and streams
    #[must_use]
    pub fn show_combined(levels: Vec<LogLevel>, stream_types: Vec<LogStreamType>) -> LogFilter {
        LogFilter {
            show_logs: true,
            show_streams: true,
            levels,
            stream_types,
        }
    }
    // Getters
    #[must_use]
    pub fn should_show_logs(&self) -> bool {
        self.show_logs
    }
    #[must_use]
    pub fn should_show_streams(&self) -> bool {
        self.show_streams
    }
    #[must_use]
    pub fn levels(&self) -> &Vec<LogLevel> {
        &self.levels
    }
    #[must_use]
    pub fn stream_types(&self) -> &Vec<LogStreamType> {
        &self.stream_types
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionWithStateRequestsResponses {
    pub execution_with_state: ExecutionWithState,
    pub events: Vec<ExecutionEvent>,
    pub responses: Vec<ResponseWithCursor>,
}

#[async_trait]
pub trait DbConnection: DbExecutor {
    /// Get execution log.
    async fn get(&self, execution_id: &ExecutionId) -> Result<ExecutionLog, DbErrorRead>;

    async fn append_delay_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        delay_id: DelayId,
        outcome: Result<(), ()>, // Successfully finished - `Ok(())` or cancelled - `Err(())`
    ) -> Result<AppendDelayResponseOutcome, DbErrorWrite>;

    /// Append a batch of events to an existing execution log, and append a response to a parent execution.
    /// The batch cannot contain `ExecutionEventInner::Created`.
    async fn append_batch(
        &self,
        current_time: DateTime<Utc>, // not persisted, can be used for unblocking `subscribe_to_pending`
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbErrorWrite>;

    /// Append one or more events to the parent execution log, and create zero or more child execution logs.
    /// The batch cannot contain `ExecutionEventInner::Created`.
    async fn append_batch_create_new_execution(
        &self,
        current_time: DateTime<Utc>, // not persisted, can be used for unblocking `subscribe_to_pending`
        batch: Vec<AppendRequest>,   // must not contain `ExecutionEventInner::Created` events
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
        backtraces: Vec<BacktraceInfo>,
    ) -> Result<AppendBatchResponse, DbErrorWrite>;

    /// Get a single event specified by version. Impls may set `ExecutionEvent::backtrace_id` to `None`.
    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbErrorRead>;

    #[instrument(skip(self))]
    async fn get_create_request(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbErrorRead> {
        let execution_event = self
            .get_execution_event(execution_id, &Version::new(0))
            .await?;
        if let ExecutionRequest::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            component_id,
            deployment_id,
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
                component_id,
                deployment_id,
                metadata,
                scheduled_by,
            })
        } else {
            Err(DbErrorRead::Generic(DbErrorGeneric::Uncategorized {
                reason: "execution log must start with creation".into(),
                context: SpanTrace::capture(),
                source: None,
                loc: Location::caller(),
            }))
        }
    }

    async fn get_pending_state(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<ExecutionWithState, DbErrorRead>;

    /// Get currently expired locks and async timers (delay requests)
    async fn get_expired_timers(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbErrorGeneric>;

    /// Create a new execution log
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbErrorWrite>;

    /// Notification mechainism with no strict guarantees for getting notified when a new response arrives.
    /// Parameter `start_idx` must be at most be equal to current size of responses in the execution log.
    /// If no response arrives immediately and `interrupt_after` resolves, `DbErrorReadWithTimeout::Timeout` is returned.
    /// Implementations with no pubsub support should use polling.
    /// Callers are expected to call this function in a loop with a reasonable timeout
    /// to support less stellar implementations.
    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        last_response: ResponseCursor,
        timeout_fut: Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorReadWithTimeout>;

    /// Notification mechainism with no strict guarantees for getting the finished result.
    /// Implementations with no pubsub support should use polling.
    /// Callers are expected to call this function in a loop with a reasonable timeout
    /// to support less stellar implementations.
    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout_fut: Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>>,
        // TODO: camcel fut
    ) -> Result<SupportedFunctionReturnValue, DbErrorReadWithTimeout>;

    async fn append_backtrace(&self, append: BacktraceInfo) -> Result<(), DbErrorWrite>;

    async fn append_backtrace_batch(&self, batch: Vec<BacktraceInfo>) -> Result<(), DbErrorWrite>;

    async fn append_log(&self, row: LogInfoAppendRow) -> Result<(), DbErrorWrite>;

    async fn append_log_batch(&self, batch: &[LogInfoAppendRow]) -> Result<(), DbErrorWrite>;
}

#[derive(Clone, Debug)]
pub struct LogInfoAppendRow {
    pub execution_id: ExecutionId,
    pub run_id: RunId,
    pub log_entry: LogEntry,
}

#[derive(Debug, Clone)]
pub struct LogEntryRow {
    pub cursor: u32,
    pub run_id: RunId,
    pub log_entry: LogEntry,
}

#[derive(Debug, Clone)]
pub enum LogEntry {
    Log {
        created_at: DateTime<Utc>,
        level: LogLevel,
        message: String,
    },
    Stream {
        created_at: DateTime<Utc>,
        payload: Vec<u8>,
        stream_type: LogStreamType,
    },
}
impl LogEntry {
    #[must_use]
    pub fn created_at(&self) -> DateTime<Utc> {
        match self {
            LogEntry::Log { created_at, .. } | LogEntry::Stream { created_at, .. } => *created_at,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::TryFrom, strum::EnumIter)]
#[try_from(repr)]
#[repr(u8)]
pub enum LogLevel {
    Trace = 1,
    Debug,
    Info,
    Warn,
    Error,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::TryFrom, strum::EnumIter)]
#[try_from(repr)]
#[repr(u8)]
pub enum LogStreamType {
    StdOut = 1,
    StdErr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutOutcome {
    Timeout,
    Cancel,
}

#[cfg(feature = "test")]
#[async_trait]
pub trait DbConnectionTest: DbConnection {
    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbErrorWrite>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled,
    AlreadyFinished,
}

#[instrument(skip(db_connection))]
pub async fn stub_execution(
    db_connection: &dyn DbConnection,
    execution_id: ExecutionIdDerived,
    parent_execution_id: ExecutionId,
    join_set_id: JoinSetId,
    created_at: DateTime<Utc>,
    return_value: SupportedFunctionReturnValue,
) -> Result<(), DbErrorWrite> {
    let stub_finished_version = Version::new(1); // Stub activities have no execution log except Created event.
    // Attempt to write to `execution_id` and its parent, ignoring the possible conflict error on this tx
    let write_attempt = {
        let finished_req = AppendRequest {
            created_at,
            event: ExecutionRequest::Finished {
                result: return_value.clone(),
                http_client_traces: None,
            },
        };
        db_connection
            .append_batch_respond_to_parent(
                AppendEventsToExecution {
                    execution_id: ExecutionId::Derived(execution_id.clone()),
                    version: stub_finished_version.clone(),
                    batch: vec![finished_req],
                },
                AppendResponseToExecution {
                    parent_execution_id,
                    created_at,
                    join_set_id,
                    child_execution_id: execution_id.clone(),
                    finished_version: stub_finished_version.clone(),
                    result: return_value.clone(),
                },
                created_at,
            )
            .await
    };
    if let Err(write_attempt) = write_attempt {
        // Check that the expected value is in the database
        debug!("Stub write attempt failed - {write_attempt:?}");

        let found = db_connection
            .get_execution_event(&ExecutionId::Derived(execution_id), &stub_finished_version)
            .await?; // Not found at this point should not happen, unless the previous write failed. Will be retried.
        match found.event {
            ExecutionRequest::Finished {
                result: found_result,
                ..
            } if return_value == found_result => {
                // Same value has already be written, RPC is successful.
                Ok(())
            }
            ExecutionRequest::Finished { .. } => Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::Conflict,
            )),
            _other => Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::IllegalState {
                    reason: "unexpected execution event at stubbed execution".into(),
                    context: SpanTrace::capture(),
                    source: None,
                    loc: Location::caller(),
                },
            )),
        }
    } else {
        Ok(())
    }
}

pub async fn cancel_delay(
    db_connection: &dyn DbConnection,
    delay_id: DelayId,
    created_at: DateTime<Utc>,
) -> Result<CancelOutcome, DbErrorWrite> {
    let (parent_execution_id, join_set_id) = delay_id.split_to_parts();
    db_connection
        .append_delay_response(
            created_at,
            parent_execution_id,
            join_set_id,
            delay_id,
            Err(()), // Mark as cancelled.
        )
        .await
        .map(|ok| match ok {
            AppendDelayResponseOutcome::Success | AppendDelayResponseOutcome::AlreadyCancelled => {
                CancelOutcome::Cancelled
            }
            AppendDelayResponseOutcome::AlreadyFinished => CancelOutcome::AlreadyFinished,
        })
}

#[derive(Clone, Debug)]
pub enum BacktraceFilter {
    First,
    Last,
    Specific(Version),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BacktraceInfo {
    pub execution_id: ExecutionId,
    pub component_id: ComponentId,
    pub version_min_including: Version,
    pub version_max_excluding: Version,
    pub wasm_backtrace: WasmBacktrace,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WasmBacktrace {
    pub frames: Vec<FrameInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrameInfo {
    pub module: String,
    pub func_name: String,
    pub symbols: Vec<FrameSymbol>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, derive_more::Display)]
#[display("{execution_id} {pending_state} {component_digest}")]
pub struct ExecutionWithState {
    pub execution_id: ExecutionId,
    pub ffqn: FunctionFqn,
    pub pending_state: PendingState,
    pub created_at: DateTime<Utc>,
    pub first_scheduled_at: DateTime<Utc>,
    pub component_digest: InputContentDigest,
    pub component_type: ComponentType,
    pub deployment_id: DeploymentId,
}

#[derive(Debug, Clone)]
pub enum ExecutionListPagination {
    CreatedBy(Pagination<Option<DateTime<Utc>>>),
    ExecutionId(Pagination<Option<ExecutionId>>),
}
impl Default for ExecutionListPagination {
    fn default() -> ExecutionListPagination {
        ExecutionListPagination::CreatedBy(Pagination::OlderThan {
            length: 20,
            cursor: None,
            including_cursor: false, // does not matter when `cursor` is not specified
        })
    }
}
impl ExecutionListPagination {
    #[must_use]
    pub fn length(&self) -> u16 {
        match self {
            ExecutionListPagination::CreatedBy(pagination) => pagination.length(),
            ExecutionListPagination::ExecutionId(pagination) => pagination.length(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pagination<T> {
    NewerThan {
        length: u16,
        cursor: T,
        including_cursor: bool,
    },
    OlderThan {
        length: u16,
        cursor: T,
        including_cursor: bool,
    },
}
impl<T: Clone> Pagination<T> {
    pub fn length(&self) -> u16 {
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

    pub fn asc_or_desc(&self) -> &'static str {
        if self.is_asc() { "asc" } else { "desc" }
    }

    pub fn is_asc(&self) -> bool {
        !self.is_desc()
    }

    pub fn cursor(&self) -> &T {
        match self {
            Pagination::NewerThan { cursor, .. } | Pagination::OlderThan { cursor, .. } => cursor,
        }
    }

    #[must_use]
    pub fn invert(&self) -> Self {
        match self {
            Pagination::NewerThan {
                length,
                cursor,
                including_cursor,
            } => Pagination::OlderThan {
                length: *length,
                cursor: cursor.clone(),
                including_cursor: !including_cursor,
            },
            Pagination::OlderThan {
                length,
                cursor,
                including_cursor,
            } => Pagination::NewerThan {
                length: *length,
                cursor: cursor.clone(),
                including_cursor: !including_cursor,
            },
        }
    }
}

#[cfg(feature = "test")]
pub async fn wait_for_pending_state_fn<T: Debug>(
    db_connection: &dyn DbConnectionTest,
    execution_id: &ExecutionId,
    predicate: impl Fn(ExecutionLog) -> Option<T> + Send,
    timeout: Option<Duration>,
) -> Result<T, DbErrorReadWithTimeout> {
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
            () = tokio::time::sleep(timeout) => Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Timeout))
        }
    } else {
        fut.await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpiredTimer {
    Lock(ExpiredLock),
    Delay(ExpiredDelay),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredLock {
    pub execution_id: ExecutionId,
    // Version of last `Locked` event, used to detect whether the execution made progress.
    pub locked_at_version: Version,
    pub next_version: Version,
    /// As the execution may still be running, this represents the number of intermittent failures + timeouts prior to this execution.
    pub intermittent_event_count: u32,
    pub max_retries: Option<u32>,
    pub retry_exp_backoff: Duration,
    pub locked_by: LockedBy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredDelay {
    pub execution_id: ExecutionId,
    pub join_set_id: JoinSetId,
    pub delay_id: DelayId,
}

#[derive(Debug, Clone, derive_more::Display, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PendingState {
    /// Caused by [`ExecutionRequest::Locked`].
    Locked(PendingStateLocked),

    #[display("PendingAt(`{scheduled_at}`)")]
    PendingAt {
        scheduled_at: DateTime<Utc>,
        /// `last_lock` is needed for lock extension.
        last_lock: Option<LockedBy>,
    },

    /// Caused by [`HistoryEvent::JoinNext`]
    #[display("BlockedByJoinSet({join_set_id},`{lock_expires_at}`)")]
    BlockedByJoinSet {
        join_set_id: JoinSetId,
        /// See [`HistoryEvent::JoinNext::lock_expires_at`].
        lock_expires_at: DateTime<Utc>,
        /// Blocked by closing of the join set
        closing: bool,
    },

    #[display("Paused")]
    Paused,

    #[display("Finished({finished})")]
    Finished {
        #[serde(flatten)]
        finished: PendingStateFinished,
    },
}

#[derive(Debug, Clone, derive_more::Display, PartialEq, Eq, Serialize)]
#[display("Locked(`{lock_expires_at}`, {}, {})", locked_by.executor_id, locked_by.run_id)]
pub struct PendingStateLocked {
    pub locked_by: LockedBy,
    pub lock_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockedBy {
    pub executor_id: ExecutorId,
    pub run_id: RunId,
}
impl From<&Locked> for LockedBy {
    fn from(value: &Locked) -> Self {
        LockedBy {
            executor_id: value.executor_id,
            run_id: value.run_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingStateFinished {
    pub version: VersionType, // not Version since it must be Copy
    pub finished_at: DateTime<Utc>,
    pub result_kind: PendingStateFinishedResultKind,
}
impl Display for PendingStateFinished {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.result_kind {
            PendingStateFinishedResultKind::Ok => write!(f, "ok"),
            PendingStateFinishedResultKind::Err(err) => write!(f, "{err}"),
        }
    }
}

// This is not a Result so that it can be customized for serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingStateFinishedResultKind {
    Ok,
    Err(PendingStateFinishedError),
}
impl PendingStateFinishedResultKind {
    pub fn as_result(&self) -> Result<(), &PendingStateFinishedError> {
        match self {
            PendingStateFinishedResultKind::Ok => Ok(()),
            PendingStateFinishedResultKind::Err(err) => Err(err),
        }
    }
}

impl From<&SupportedFunctionReturnValue> for PendingStateFinishedResultKind {
    fn from(result: &SupportedFunctionReturnValue) -> Self {
        result.as_pending_state_finished_result()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, derive_more::Display)]
#[serde(rename_all = "snake_case")]
pub enum PendingStateFinishedError {
    #[display("execution terminated: {_0}")]
    ExecutionFailure(ExecutionFailureKind),
    #[display("execution completed with an error")]
    Error,
}

impl PendingState {
    #[instrument(skip(self))]
    pub fn can_append_lock(
        &self,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockKind, DbErrorWriteNonRetriable> {
        if lock_expires_at <= created_at {
            return Err(DbErrorWriteNonRetriable::ValidationFailed(
                "invalid expiry date".into(),
            ));
        }
        match self {
            PendingState::PendingAt {
                scheduled_at,
                last_lock,
            } => {
                if *scheduled_at <= created_at {
                    // pending now, ok to lock
                    Ok(LockKind::CreatingNewLock)
                } else if let Some(LockedBy {
                    executor_id: last_executor_id,
                    run_id: last_run_id,
                }) = last_lock
                    && executor_id == *last_executor_id
                    && run_id == *last_run_id
                {
                    // Original executor is extending the lock.
                    Ok(LockKind::Extending)
                } else {
                    Err(DbErrorWriteNonRetriable::ValidationFailed(
                        "cannot lock, not yet pending".into(),
                    ))
                }
            }
            PendingState::Locked(PendingStateLocked {
                locked_by:
                    LockedBy {
                        executor_id: current_pending_state_executor_id,
                        run_id: current_pending_state_run_id,
                    },
                lock_expires_at: _,
            }) => {
                if executor_id == *current_pending_state_executor_id
                    && run_id == *current_pending_state_run_id
                {
                    // Original executor is extending the lock.
                    Ok(LockKind::Extending)
                } else {
                    Err(DbErrorWriteNonRetriable::IllegalState {
                        reason: "cannot lock, already locked".into(),
                        context: SpanTrace::capture(),
                        source: None,
                        loc: Location::caller(),
                    })
                }
            }
            PendingState::BlockedByJoinSet { .. } => Err(DbErrorWriteNonRetriable::IllegalState {
                reason: "cannot append Locked event when in BlockedByJoinSet state".into(),
                context: SpanTrace::capture(),
                source: None,
                loc: Location::caller(),
            }),
            PendingState::Finished { .. } => Err(DbErrorWriteNonRetriable::IllegalState {
                reason: "already finished".into(),
                context: SpanTrace::capture(),
                source: None,
                loc: Location::caller(),
            }),
            PendingState::Paused => Err(DbErrorWriteNonRetriable::IllegalState {
                reason: "cannot lock, execution is paused".into(),
                context: SpanTrace::capture(),
                source: None,
                loc: Location::caller(),
            }),
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
    use super::PendingStateFinished;
    use super::PendingStateFinishedError;
    use super::PendingStateFinishedResultKind;
    use crate::ExecutionFailureKind;
    use crate::SupportedFunctionReturnValue;
    use chrono::DateTime;
    use chrono::Datelike;
    use chrono::Utc;
    use insta::assert_snapshot;
    use rstest::rstest;
    use std::time::Duration;
    use val_json::type_wrapper::TypeWrapper;
    use val_json::wast_val::WastVal;
    use val_json::wast_val::WastValWithType;

    #[rstest(expected => [
        PendingStateFinishedResultKind::Ok,
        PendingStateFinishedResultKind::Err(PendingStateFinishedError::ExecutionFailure(ExecutionFailureKind::TimedOut)),
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
        PendingStateFinishedResultKind::Ok,
        PendingStateFinishedResultKind::Err(PendingStateFinishedError::ExecutionFailure(ExecutionFailureKind::TimedOut)),
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
        let expected = SupportedFunctionReturnValue::Ok {
            ok: Some(WastValWithType {
                r#type: TypeWrapper::Result {
                    ok: Some(Box::new(TypeWrapper::Option(Box::new(TypeWrapper::String)))),
                    err: Some(Box::new(TypeWrapper::String)),
                },
                value: WastVal::Result(Ok(Some(Box::new(WastVal::Option(None))))),
            }),
        };
        let json = serde_json::to_string(&expected).unwrap();
        assert_snapshot!(json);

        let actual: SupportedFunctionReturnValue = serde_json::from_str(&json).unwrap();

        assert_eq!(expected, actual);
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
