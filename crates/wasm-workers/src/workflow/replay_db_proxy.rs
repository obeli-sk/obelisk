//! A workflow database proxy for replay that captures all write operations in memory
//! instead of persisting them. This allows `replay_internal` to collect the operations
//! that the workflow would produce next.

use super::caching_db_connection::{CacheableDbEvent, WorkflowDbConnection};
use crate::{
    activity::cancel_registry::CancelRegistry,
    workflow::{caching_db_connection::FlushOutcome, event_history::DbErrorWriteOrReplayInterrupt},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ExecutionId,
    prefixed_ulid::DelayId,
    storage::{
        self, AppendBatchResponse, AppendEventsToExecution, AppendRequest,
        AppendResponseToExecution, BacktraceInfo, CancelOutcome, CapturedDbWrite, CreateRequest,
        DbConnection, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbErrorWriteNonRetriable,
        ExecutionEvent, ExecutionRequest, ResponseCursor, ResponseWithCursor, TimeoutOutcome,
        Version,
    },
};
use std::pin::Pin;
use std::{any::Any, future::Future};

#[derive(Debug, Default)]
struct ReplayEventCollector {
    writes: Vec<CapturedDbWrite>,
}

impl ReplayEventCollector {
    fn into_writes(self) -> Vec<CapturedDbWrite> {
        self.writes
    }
}

pub(crate) async fn apply_writes(
    conn: &dyn DbConnection,
    actual: Vec<CapturedDbWrite>,
    version: Version,
) -> Result<Version, DbErrorWrite> {
    let mut new_version = version;
    for write in &actual {
        if let Some(v) = apply_captured_write(write, conn).await? {
            new_version = v;
        }
    }
    Ok(new_version)
}

async fn apply_captured_write(
    write: &CapturedDbWrite,
    conn: &dyn DbConnection,
) -> Result<Option<Version>, DbErrorWrite> {
    match write.clone() {
        CapturedDbWrite::Append {
            execution_id,
            version,
            req,
        } => conn.append(execution_id, version, req).await.map(Some),
        CapturedDbWrite::AppendBatch {
            current_time,
            batch,
            execution_id,
            version,
        } => conn
            .append_batch(current_time, batch, execution_id, version)
            .await
            .map(Some),
        CapturedDbWrite::AppendBatchCreateNewExecution {
            current_time,
            batch,
            execution_id,
            version,
            child_req,
            backtraces,
        } => conn
            .append_batch_create_new_execution(
                current_time,
                batch,
                execution_id,
                version,
                child_req,
                backtraces,
            )
            .await
            .map(Some),
        CapturedDbWrite::AppendStubResponse {
            events,
            response,
            current_time,
        } => {
            conn.append_batch_respond_to_parent(events, response, current_time)
                .await?;
            Ok(None)
        }
    }
}

/// A `WorkflowDbConnection` that captures write operations instead of persisting them.
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
        version: Version,
        real_connection: Box<dyn DbConnection>,
    ) -> Self {
        Self {
            execution_id,
            collector: ReplayEventCollector::default(),
            version,
            real_connection,
        }
    }
    pub(crate) fn into_writes(self) -> Vec<CapturedDbWrite> {
        self.collector.into_writes()
    }

    /// Push a captured write and advance the version.
    pub(crate) fn push_write(&mut self, write: CapturedDbWrite) {
        self.collector.writes.push(write);
    }

    pub(crate) fn version(&self) -> &Version {
        &self.version
    }

    pub(crate) fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
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
    fn as_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

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
        if let Some(child_req) = child_req {
            self.collector
                .writes
                .push(CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time: _called_at,
                    batch: vec![request],
                    execution_id: self.execution_id.clone(),
                    version: version.clone(),
                    child_req: vec![child_req],
                    backtraces: backtrace.into_iter().collect(),
                });
        } else {
            self.collector.writes.push(CapturedDbWrite::Append {
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
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        let version = self.version.clone();
        self.collector.writes.push(CapturedDbWrite::Append {
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
        let version = self.version.clone();
        let next = next_version(&version, batch.len());
        self.collector.writes.push(CapturedDbWrite::AppendBatch {
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
        let version = self.version.clone();
        let next = next_version(&version, batch.len());
        self.collector
            .writes
            .push(CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time,
                batch,
                execution_id,
                version,
                child_req,
                backtraces: Vec::new(),
            });
        self.version = next;
        Ok(())
    }

    async fn append_stub_response(
        &mut self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
    ) -> Result<AppendBatchResponse, DbErrorWriteOrReplayInterrupt> {
        let target_execution_id = &events.execution_id;
        // Query the database first.
        let stub_finished_version = Version::new(1); // Stub activities have no execution log except Created event.
        if let Ok(found_stub) = self
            .real_connection
            .get_execution_event(target_execution_id, &stub_finished_version)
            .await
        {
            if matches!(found_stub.event, ExecutionRequest::Finished { .. }) {
                // First stub write sends AlreadyFinished, caller will do the retval comparison.
                return Err(DbErrorWriteOrReplayInterrupt::DbError(
                    DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::AlreadyFinished),
                ));
            }
            // This must not be a stub execution.
            return Err(DbErrorWriteOrReplayInterrupt::DbError(
                DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict),
            ));
        }

        self.collector
            .writes
            .push(CapturedDbWrite::AppendStubResponse {
                events,
                response,
                current_time,
            });
        // no idea about the response
        Err(DbErrorWriteOrReplayInterrupt::ReplayInterrupt)
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
    ) -> Result<FlushOutcome, DbErrorWrite> {
        if self.collector.writes.is_empty() {
            Ok(FlushOutcome::Noop)
        } else {
            Ok(FlushOutcome::FlushedCache)
        }
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
