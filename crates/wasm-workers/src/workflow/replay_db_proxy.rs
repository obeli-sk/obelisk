//! A database proxy for replay that captures all appended `HistoryEvent`s in memory
//! instead of persisting them. This allows `replay_internal` to collect the events
//! that the workflow would produce next.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::component_id::ComponentDigest;
use concepts::prefixed_ulid::{DelayId, ExecutorId, RunId};
use concepts::storage::{
    AppendBatchResponse, AppendEventsToExecution, AppendRequest, AppendResponse,
    AppendResponseToExecution, BacktraceInfo, CreateRequest, DbConnection, DbErrorGeneric,
    DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbExecutor, DbPool, ExecutionEvent,
    ExecutionLog, ExecutionRequest, ExecutionWithState, ExpiredTimer, HistoryEvent,
    LockPendingResponse, LogInfoAppendRow, ResponseCursor, ResponseWithCursor, TimeoutOutcome,
    Version,
};
use concepts::{
    ComponentId, ComponentRetryConfig, ExecutionId, FunctionFqn, JoinSetId,
    SupportedFunctionReturnValue,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Shared buffer that accumulates `HistoryEvent`s during replay.
#[derive(Debug, Default, Clone)]
pub(crate) struct ReplayEventCollector {
    events: Arc<Mutex<Vec<HistoryEvent>>>,
}

impl ReplayEventCollector {
    pub(crate) fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Extract collected events, consuming the buffer.
    pub(crate) fn take_events(&self) -> Vec<HistoryEvent> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
}

/// A `DbPool` that returns `ReplayDbConnection` instances.
/// Read operations are passed through to the real database pool.
pub(crate) struct ReplayDbPool {
    execution_id: ExecutionId,
    collector: ReplayEventCollector,
    starting_version: Version,
    real_pool: Arc<dyn DbPool>,
}

impl ReplayDbPool {
    pub(crate) fn new(
        execution_id: ExecutionId,
        collector: ReplayEventCollector,
        starting_version: Version,
        real_pool: Arc<dyn DbPool>,
    ) -> Self {
        Self {
            execution_id,
            collector,
            starting_version,
            real_pool,
        }
    }
}

#[async_trait]
impl DbPool for ReplayDbPool {
    async fn db_exec_conn(&self) -> Result<Box<dyn DbExecutor>, DbErrorGeneric> {
        Ok(Box::new(ReplayDbConnection {
            execution_id: self.execution_id.clone(),
            collector: self.collector.clone(),
            version: self.starting_version.clone(),
            real_connection: self.real_pool.connection().await?,
        }))
    }

    async fn connection(&self) -> Result<Box<dyn DbConnection>, DbErrorGeneric> {
        Ok(Box::new(ReplayDbConnection {
            execution_id: self.execution_id.clone(),
            collector: self.collector.clone(),
            version: self.starting_version.clone(),
            real_connection: self.real_pool.connection().await?,
        }))
    }

    async fn external_api_conn(
        &self,
    ) -> Result<Box<dyn concepts::storage::DbExternalApi>, DbErrorGeneric> {
        unimplemented!("ReplayDbPool does not support external_api_conn")
    }

    #[cfg(feature = "test")]
    async fn connection_test(
        &self,
    ) -> Result<Box<dyn concepts::storage::DbConnectionTest>, DbErrorGeneric> {
        unimplemented!("ReplayDbPool does not support connection_test")
    }
}

/// A `DbConnection` that captures history events instead of persisting them.
/// Read operations are delegated to the real database connection.
struct ReplayDbConnection {
    execution_id: ExecutionId,
    collector: ReplayEventCollector,
    version: Version,
    real_connection: Box<dyn DbConnection>,
}

impl ReplayDbConnection {
    fn collect_events_from_batch(&self, batch: &[AppendRequest]) {
        let mut events = self.collector.events.lock().unwrap();
        for req in batch {
            if let ExecutionRequest::HistoryEvent { event } = &req.event {
                events.push(event.clone());
            }
        }
    }
}

fn next_version(curr_version: &Version, batch_size: usize) -> Version {
    Version::new(
        curr_version.0
            + u32::try_from(batch_size).expect("batch size is always just a couple of items"),
    )
}

#[async_trait]
impl DbExecutor for ReplayDbConnection {
    async fn lock_pending_by_ffqns(
        &self,
        _batch_size: u32,
        _pending_at_or_sooner: DateTime<Utc>,
        _ffqns: Arc<[FunctionFqn]>,
        _created_at: DateTime<Utc>,
        _component_id: ComponentId,
        _deployment_id: concepts::prefixed_ulid::DeploymentId,
        _executor_id: ExecutorId,
        _lock_expires_at: DateTime<Utc>,
        _run_id: RunId,
        _retry_config: ComponentRetryConfig,
    ) -> Result<LockPendingResponse, DbErrorWrite> {
        unimplemented!("not used during replay")
    }

    async fn lock_pending_by_component_digest(
        &self,
        _batch_size: u32,
        _pending_at_or_sooner: DateTime<Utc>,
        _component_id: &ComponentId,
        _deployment_id: concepts::prefixed_ulid::DeploymentId,
        _created_at: DateTime<Utc>,
        _executor_id: ExecutorId,
        _lock_expires_at: DateTime<Utc>,
        _run_id: RunId,
        _retry_config: ComponentRetryConfig,
    ) -> Result<LockPendingResponse, DbErrorWrite> {
        unimplemented!("not used during replay")
    }

    #[cfg(feature = "test")]
    async fn lock_one(
        &self,
        _created_at: DateTime<Utc>,
        _component_id: ComponentId,
        _deployment_id: concepts::prefixed_ulid::DeploymentId,
        _execution_id: &ExecutionId,
        _run_id: RunId,
        _version: Version,
        _executor_id: ExecutorId,
        _lock_expires_at: DateTime<Utc>,
        _retry_config: ComponentRetryConfig,
    ) -> Result<concepts::storage::LockedExecution, DbErrorWrite> {
        unimplemented!("not used during replay")
    }

    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        if let ExecutionRequest::HistoryEvent { event } = &req.event {
            self.collector.events.lock().unwrap().push(event.clone());
        }
        Ok(Version::new(version.0 + 1))
    }

    async fn append_batch_respond_to_parent(
        &self,
        events: AppendEventsToExecution,
        _response: AppendResponseToExecution,
        _current_time: DateTime<Utc>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        assert_eq!(self.execution_id, events.execution_id);
        self.collect_events_from_batch(&events.batch);
        Ok(next_version(&self.version, events.batch.len()))
    }

    async fn wait_for_pending_by_ffqn(
        &self,
        _pending_at_or_sooner: DateTime<Utc>,
        _ffqns: Arc<[FunctionFqn]>,
        _timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        // no-op for replay
    }

    async fn wait_for_pending_by_component_digest(
        &self,
        _pending_at_or_sooner: DateTime<Utc>,
        _component_digest: &ComponentDigest,
        _timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        // no-op for replay
    }

    async fn get_last_execution_event(
        &self,
        _activity_execution_id: &ExecutionId,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        // During replay, cancel_activity calls this to check if the child is already finished.
        // Return a "Cancelled" finished event so cancel_activity returns CancelOutcome::Cancelled.
        // `EventHistory::join_set_close` does not care about value of `CancelOutcome`.
        Ok(ExecutionEvent {
            created_at: DateTime::UNIX_EPOCH,
            event: ExecutionRequest::Finished {
                retval: SupportedFunctionReturnValue::ExecutionError(
                    concepts::FinishedExecutionError {
                        reason: None,
                        kind: concepts::ExecutionFailureKind::Cancelled,
                        detail: None,
                    },
                ),
                http_client_traces: None,
            },
            backtrace_id: None,
            version: Version::new(0),
        })
    }
}

#[async_trait]
impl DbConnection for ReplayDbConnection {
    async fn get(&self, execution_id: &ExecutionId) -> Result<ExecutionLog, DbErrorRead> {
        // Allow getting the current state of a stubbed execution
        self.real_connection.get(execution_id).await
    }

    async fn append_delay_response(
        &self,
        _created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        _join_set_id: JoinSetId,
        _delay_id: DelayId,
        outcome: Result<(), ()>,
    ) -> Result<concepts::storage::AppendDelayResponseOutcome, DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        assert!(outcome.is_err(), "replay can only request to cancel delays");
        Ok(concepts::storage::AppendDelayResponseOutcome::AlreadyCancelled)
    }

    async fn append_batch(
        &self,
        _current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        self.collect_events_from_batch(&batch);
        Ok(next_version(&version, batch.len()))
    }

    async fn append_batch_create_new_execution(
        &self,
        _current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        _child_req: Vec<CreateRequest>,
        _backtraces: Vec<BacktraceInfo>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        self.collect_events_from_batch(&batch);
        Ok(next_version(&version, batch.len()))
    }

    // Needed for stubbed executions
    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        self.real_connection
            .get_execution_event(execution_id, version)
            .await
    }

    // Needed for stubbed executions
    async fn get_pending_state(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<ExecutionWithState, DbErrorRead> {
        self.real_connection.get_pending_state(execution_id).await
    }

    async fn get_expired_timers(
        &self,
        _at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbErrorGeneric> {
        unimplemented!("not used during replay")
    }

    async fn create(&self, _req: CreateRequest) -> Result<AppendResponse, DbErrorWrite> {
        unimplemented!("not used during replay")
    }

    async fn subscribe_to_next_responses(
        &self,
        _execution_id: &ExecutionId,
        _last_response: ResponseCursor,
        _timeout_fut: Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorReadWithTimeout> {
        // During replay, there are no new responses to subscribe to.
        Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Timeout))
    }

    async fn wait_for_finished_result(
        &self,
        _execution_id: &ExecutionId,
        _timeout_fut: Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>>,
    ) -> Result<SupportedFunctionReturnValue, DbErrorReadWithTimeout> {
        unimplemented!("not used during replay")
    }

    async fn append_backtrace(&self, _append: BacktraceInfo) -> Result<(), DbErrorWrite> {
        // Silently discard backtraces during replay
        Ok(())
    }

    async fn append_backtrace_batch(&self, _batch: Vec<BacktraceInfo>) -> Result<(), DbErrorWrite> {
        // Silently discard backtraces during replay
        Ok(())
    }

    async fn append_log(&self, _row: LogInfoAppendRow) -> Result<(), DbErrorWrite> {
        // Silently discard logs during replay
        Ok(())
    }

    async fn append_log_batch(&self, _batch: &[LogInfoAppendRow]) -> Result<(), DbErrorWrite> {
        // Silently discard logs during replay
        Ok(())
    }
}
