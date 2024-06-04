use crate::prefixed_ulid::DelayId;
use crate::prefixed_ulid::ExecutorId;
use crate::prefixed_ulid::JoinSetId;
use crate::prefixed_ulid::RunId;
use crate::ComponentId;
use crate::ComponentType;
use crate::ExecutionId;
use crate::FinishedExecutionResult;
use crate::FunctionFqn;
use crate::FunctionMetadata;
use crate::Params;
use crate::StrVariant;
use crate::SupportedFunctionResult;
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoStaticStr;
use tracing::debug;
use tracing::trace;

/// Remote client representation of the execution journal.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionLog {
    pub execution_id: ExecutionId,
    pub events: Vec<ExecutionEvent>,
    pub responses: Vec<JoinSetResponseEventOuter>,
    pub version: Version,
    pub pending_state: PendingState,
}

impl ExecutionLog {
    #[must_use]
    pub fn can_be_retried_after(
        intermittent_event_count: u32,
        max_retries: u32,
        retry_exp_backoff: Duration,
    ) -> Option<Duration> {
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
        }) => *parent)
    }

    #[must_use]
    pub fn last_event(&self) -> &ExecutionEvent {
        self.events.last().expect("must contain at least one event")
    }

    #[must_use]
    pub fn finished_result(&self) -> Option<&FinishedExecutionResult> {
        if let ExecutionEvent {
            event: ExecutionEventInner::Finished { result },
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
        #[arbitrary(value = Ok(SupportedFunctionResult::None))]
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
            Some(*child_execution_id)
        } else {
            None
        }
    }
}

pub const DUMMY_CREATED: ExecutionEventInner = ExecutionEventInner::Created {
    ffqn: FunctionFqn::new_static("", ""),
    params: Params::default(),
    parent: None,
    scheduled_at: DateTime::from_timestamp_nanos(0),
    retry_exp_backoff: Duration::ZERO,
    max_retries: 0,
};
pub const DUMMY_HISTORY_EVENT: ExecutionEventInner = ExecutionEventInner::HistoryEvent {
    event: HistoryEvent::JoinSet {
        join_set_id: JoinSetId::from_parts(0, 0),
    },
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
// TODO: Rename to ExecutionEvent
pub enum ExecutionEventInner {
    /// Created by an external system or a scheduler when requesting a child execution or
    /// an executor when continuing as new `FinishedExecutionError`::`ContinueAsNew`,`CancelledWithNew` .
    // After optional expiry(`scheduled_at`) interpreted as pending.
    #[display(fmt = "Created({ffqn}, `{scheduled_at}`)")]
    Created {
        ffqn: FunctionFqn,
        #[arbitrary(default)]
        params: Params,
        parent: Option<(ExecutionId, JoinSetId)>,
        scheduled_at: DateTime<Utc>,
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
    /// Returns execution to [`PendingState::PendingNow`] state
    /// without timing out. This can happen when the executor is running
    /// out of resources like [`WorkerError::LimitReached`] or when
    /// the executor is shutting down.
    Unlocked,
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentFailure(`{expires_at}`)")]
    IntermittentFailure {
        //TODO: Rename to IntermittentlyFailed
        expires_at: DateTime<Utc>,
        #[arbitrary(value = StrVariant::Static("reason"))]
        reason: StrVariant,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentTimeout(`{expires_at}`)")]
    IntermittentTimeout { expires_at: DateTime<Utc> }, // TODO: Rename to IntermittentlyTimeouted
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
    CancelRequest, // TODO Rename to CancelRequested

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
    #[display(fmt = "Persist")]
    /// Must be created by the executor in [`PendingState::Locked`].
    Persist { value: Vec<u8> },
    /// Must be created by the executor in [`PendingState::Locked`].
    #[display(fmt = "JoinSet({join_set_id})")]
    JoinSet {
        join_set_id: JoinSetId,
        // TODO: add JoinSetKind (unordered, ordered)
    },
    #[display(fmt = "JoinSetRequest({join_set_id}, {request})")]
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
#[serde(tag = "type")]
pub enum JoinSetRequest {
    // Must be created by the executor in `PendingState::Locked`.
    #[display(fmt = "DelayRequest({delay_id}, expires_at: `{expires_at}`)")]
    DelayRequest {
        delay_id: DelayId,
        expires_at: DateTime<Utc>,
    },
    // Must be created by the executor in `PendingState::Locked`.
    #[display(fmt = "ChildExecutionRequest({child_execution_id})")]
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
#[display(fmt = "{event}")]
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

#[async_trait]
pub trait DbPool<DB: DbConnection>: Send + Sync + Clone {
    fn connection(&self) -> DB;
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
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
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
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError>;

    /// Append one or more events to the parent execution log, and create zero or more child execution logs.
    async fn append_batch_create_child(
        &self,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbError>;

    async fn append_batch_respond_to_parent(
        &self,
        execution_id: ExecutionId,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        version: Version,
        parent_execution_id: ExecutionId,
        parent_response_event: JoinSetResponseEvent,
    ) -> Result<AppendBatchResponse, DbError>;

    /// Get execution log.
    async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionLog, DbError>;

    /// Get currently expired locks and async timers (delay requests)
    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError>;

    /// Create a new execution log
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError>;

    /// Get notified when a new response arrives.
    /// Parameter `start_idx` must be at most be equal to current size of responses in the execution log.
    async fn subscribe_to_next_responses(
        &self,
        execution_id: ExecutionId,
        start_idx: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError>;

    async fn wait_for_finished_result(
        &self,
        execution_id: ExecutionId,
        timeout: Option<Duration>,
    ) -> Result<FinishedExecutionResult, ClientError> {
        let execution_log = self
            .wait_for_pending_state(execution_id, PendingState::Finished, timeout)
            .await?;
        Ok(execution_log
            .finished_result()
            .expect("pending state was checked")
            .clone())
    }

    /// Best effort for subscribe to pending executions.
    /// Return imediately if there are pending notifications at `pending_at_or_sooner`.
    /// Implementation must return not later than at expiry date, which is: `pending_at_or_sooner` + `max_wait`.
    /// Timers that expire until the expiry date can be disregarded.
    /// Databases that do not support subscriptions should wait for `max_wait`.
    async fn subscribe_to_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        max_wait: Duration,
    );

    async fn wait_for_pending_state(
        &self,
        execution_id: ExecutionId,
        expected_pending_state: PendingState,
        timeout: Option<Duration>,
    ) -> Result<ExecutionLog, ClientError> {
        trace!(%execution_id, "Waiting for {expected_pending_state}");
        let fut = async move {
            loop {
                let execution_log = self.get(execution_id).await?;
                if execution_log.pending_state == expected_pending_state {
                    debug!(%execution_id, "Found: {expected_pending_state}");
                    return Ok(execution_log);
                }
                tokio::time::sleep(Duration::from_millis(100)).await; // TODO: Switch to subscription-based approach
            }
        };

        if let Some(timeout) = timeout {
            tokio::select! {
                res = fut => res,
                () = tokio::time::sleep(timeout) => Err(ClientError::Timeout)
            }
        } else {
            fut.await
        }
    }

    /// Append a component. If the component is set to `active` and there are
    /// components with overlapping exports, the operation will not append the
    /// component and the inner result will contain the list of components that
    /// must be deactivated first.
    async fn component_add(
        &self,
        created_at: DateTime<Utc>,
        component: ComponentWithMetadata,
        active: bool,
    ) -> Result<Result<(), Vec<ComponentId>>, DbError>;

    async fn list_components(&self, active: bool) -> Result<Vec<Component>, DbError>;

    /// Get component and its activation state.
    async fn component_get_metadata(
        &self,
        component_id: ComponentId,
    ) -> Result<(ComponentWithMetadata, bool), DbError>;

    /// Find exported function in the active component list.
    async fn get_exported_function(&self, ffqn: FunctionFqn) -> Result<FunctionMetadata, DbError>;

    async fn component_deactivate(&self, component_id: ComponentId) -> Result<(), DbError>;

    async fn component_activate(&self, component_id: ComponentId) -> Result<(), DbError>;

    /*


    async fn archive_component(&self, component_id: ComponentId);



    async fn list_ffqns(
        &self,
    ) -> Vec<(
        FunctionFqn,
        ParameterTypes,
        ResultType,
        String, /* file name */
        ComponentType,
    )>;
    */
}

#[derive(Debug, Clone)]
pub struct Component {
    pub component_id: ComponentId,
    pub component_type: ComponentType, // Defines the schema for `config`
    pub config: serde_json::Value,     // Out of persistence scope
    pub file_name: String,             // Additional identifier without path
}

#[derive(Debug, Clone)]
pub struct ComponentWithMetadata {
    pub component: Component,
    pub exports: Vec<FunctionMetadata>,
    pub imports: Vec<FunctionMetadata>,
}

pub async fn wait_for_pending_state_fn<T: Debug>(
    db_connection: &dyn DbConnection,
    execution_id: ExecutionId,
    predicate: impl Fn(ExecutionLog) -> Option<T> + Send,
    timeout: Option<Duration>,
) -> Result<T, ClientError> {
    trace!(%execution_id, "Waiting for predicate");
    let fut = async move {
        loop {
            let execution_log = db_connection.get(execution_id).await?;
            if let Some(t) = predicate(execution_log) {
                debug!(%execution_id, "Found: {t:?}");
                return Ok(t);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    if let Some(timeout) = timeout {
        tokio::select! {
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
