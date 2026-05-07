//! A workflow database proxy for replay that captures all appended `HistoryEvent`s in memory
//! instead of persisting them. This allows `replay_internal` to collect the events
//! that the workflow would produce next.

use super::caching_db_connection::{CacheableDbEvent, WorkflowDbConnection};
use crate::activity::cancel_registry::CancelRegistry;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ExecutionId,
    prefixed_ulid::DelayId,
    storage::{
        self, AppendBatchResponse, AppendEventsToExecution, AppendRequest,
        AppendResponseToExecution, BacktraceInfo, CancelOutcome, CreateRequest, DbConnection,
        DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, ExecutionEvent, ExecutionRequest,
        HistoryEvent, ResponseCursor, ResponseWithCursor, TimeoutOutcome, Version,
    },
};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// A captured database write operation with all arguments needed to replay it
/// against the real database.
#[derive(Debug, Clone)]
pub(crate) enum CapturedDbWrite {
    Append {
        execution_id: ExecutionId,
        version: Version,
        req: AppendRequest,
    },
    AppendBatch {
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    },
    AppendBatchCreateNewExecution {
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
        backtraces: Vec<BacktraceInfo>,
    },
    AppendBatchRespondToParent {
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
    },
}

/// Shared buffer that accumulates `HistoryEvent`s and full write operations during replay.
#[derive(Debug, Default, Clone)]
pub(crate) struct ReplayEventCollector {
    events: Arc<Mutex<Vec<HistoryEvent>>>,
    writes: Arc<Mutex<Vec<CapturedDbWrite>>>,
}

impl ReplayEventCollector {
    pub(crate) fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            writes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Extract collected events, consuming the buffer.
    pub(crate) fn take_events(&self) -> Vec<HistoryEvent> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }

    /// Extract collected write operations, consuming the buffer.
    pub(crate) fn take_writes(&self) -> Vec<CapturedDbWrite> {
        std::mem::take(&mut *self.writes.lock().unwrap())
    }
}

/// A `WorkflowDbConnection` that captures history events instead of persisting them.
/// Read operations are delegated to the real database connection.
pub(crate) struct ReplayWorkflowDbConnection {
    execution_id: ExecutionId,
    collector: ReplayEventCollector,
    version: Version,
    real_connection: Box<dyn DbConnection>,
}

impl ReplayWorkflowDbConnection {
    pub(crate) fn new(
        execution_id: ExecutionId,
        collector: ReplayEventCollector,
        version: Version,
        real_connection: Box<dyn DbConnection>,
    ) -> Self {
        Self {
            execution_id,
            collector,
            version,
            real_connection,
        }
    }

    fn collect_history_event(&self, req: &AppendRequest) {
        if let ExecutionRequest::HistoryEvent { event } = &req.event {
            self.collector.events.lock().unwrap().push(event.clone());
        }
    }

    fn collect_history_events_from_batch(&self, batch: &[AppendRequest]) {
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

/// Extract the `AppendRequest` and `Version` from a `CacheableDbEvent`, collecting
/// the history event and recording the appropriate `CapturedDbWrite`.
fn cacheable_event_parts(
    event: CacheableDbEvent,
) -> (
    AppendRequest,
    Version,
    Option<CreateRequest>,
    Option<BacktraceInfo>,
) {
    match event {
        CacheableDbEvent::SubmitChildExecution {
            request,
            version,
            child_req,
            backtrace,
        }
        | CacheableDbEvent::Schedule {
            request,
            version,
            child_req,
            backtrace,
        } => (request, version, Some(child_req), backtrace),
        CacheableDbEvent::SubmitChildExecutionError {
            request,
            version,
            backtrace,
        }
        | CacheableDbEvent::ScheduleError {
            request,
            version,
            backtrace,
        }
        | CacheableDbEvent::JoinSetCreate {
            request,
            version,
            backtrace,
        }
        | CacheableDbEvent::Persist {
            request,
            version,
            backtrace,
        }
        | CacheableDbEvent::SubmitDelay {
            request,
            version,
            backtrace,
        }
        | CacheableDbEvent::JoinNextTry {
            request,
            version,
            backtrace,
        } => (request, version, None, backtrace),
    }
}

#[async_trait]
impl WorkflowDbConnection for ReplayWorkflowDbConnection {
    fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    fn version(&self) -> &Version {
        &self.version
    }

    async fn append_non_blocking(
        &mut self,
        non_blocking_event: CacheableDbEvent,
        _called_at: DateTime<Utc>,
    ) -> Result<(), DbErrorWrite> {
        let (request, version, child_req, backtrace) = cacheable_event_parts(non_blocking_event);
        self.collect_history_event(&request);
        if let Some(child_req) = child_req {
            self.collector.writes.lock().unwrap().push(
                CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time: _called_at,
                    batch: vec![request],
                    execution_id: self.execution_id.clone(),
                    version: version.clone(),
                    child_req: vec![child_req],
                    backtraces: backtrace.into_iter().collect(),
                },
            );
        } else {
            self.collector
                .writes
                .lock()
                .unwrap()
                .push(CapturedDbWrite::Append {
                    execution_id: self.execution_id.clone(),
                    version: version.clone(),
                    req: request,
                });
        }
        self.version = Version::new(version.0 + 1);
        Ok(())
    }

    async fn append_blocking(
        &mut self,
        execution_id: ExecutionId,
        req: AppendRequest,
        _called_at: DateTime<Utc>,
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        self.collect_history_event(&req);
        let version = self.version.clone();
        self.collector
            .writes
            .lock()
            .unwrap()
            .push(CapturedDbWrite::Append {
                execution_id,
                version: version.clone(),
                req,
            });
        self.version = Version::new(version.0 + 1);
        Ok(())
    }

    async fn append_batch(
        &mut self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        self.collect_history_events_from_batch(&batch);
        let version = self.version.clone();
        let next = next_version(&version, batch.len());
        self.collector
            .writes
            .lock()
            .unwrap()
            .push(CapturedDbWrite::AppendBatch {
                current_time,
                batch,
                execution_id,
                version,
            });
        self.version = next;
        Ok(())
    }

    async fn append_batch_create_new_execution(
        &mut self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        child_req: Vec<CreateRequest>,
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        self.collect_history_events_from_batch(&batch);
        let version = self.version.clone();
        let next = next_version(&version, batch.len());
        self.collector.writes.lock().unwrap().push(
            CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time,
                batch,
                execution_id,
                version,
                child_req,
                backtraces: Vec::new(),
            },
        );
        self.version = next;
        Ok(())
    }

    async fn append_batch_respond_to_parent(
        &mut self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        assert_eq!(self.execution_id, events.execution_id);
        self.collect_history_events_from_batch(&events.batch);
        let next = next_version(&self.version, events.batch.len());
        self.collector
            .writes
            .lock()
            .unwrap()
            .push(CapturedDbWrite::AppendBatchRespondToParent {
                events,
                response,
                current_time,
            });
        self.version = next.clone();
        Ok(next)
    }

    async fn get_create_request(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbErrorRead> {
        self.real_connection.get_create_request(execution_id).await
    }

    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        self.real_connection
            .get_execution_event(execution_id, version)
            .await
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

    async fn flush_non_blocking_event_cache(
        &mut self,
        _current_time: DateTime<Utc>,
    ) -> Result<(), DbErrorWrite> {
        // No caching in replay — nothing to flush.
        Ok(())
    }

    async fn cancel_activity(
        &mut self,
        _cancel_registry: &CancelRegistry,
        _execution_id: &ExecutionId,
        _cancelled_at: DateTime<Utc>,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        // During replay, always return Cancelled.
        // `EventHistory::join_set_close` does not care about the value of `CancelOutcome`.
        Ok(CancelOutcome::Cancelled)
    }

    async fn cancel_delay(
        &mut self,
        _delay_id: DelayId,
        _cancelled_at: DateTime<Utc>,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        // During replay, always return Cancelled.
        Ok(CancelOutcome::Cancelled)
    }
}
