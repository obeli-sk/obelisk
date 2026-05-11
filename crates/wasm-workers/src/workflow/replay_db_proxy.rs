//! A workflow database proxy for replay that captures all write operations in memory
//! instead of persisting them. This allows `replay_internal` to collect the operations
//! that the workflow would produce next.

use super::caching_db_connection::{CacheableDbEvent, WorkflowDbConnection};
use crate::workflow::host_exports::response_id::ResponseId;
use crate::workflow::replay_advance::JoinSetCloseCancellations;
use crate::{
    activity::cancel_registry::CancelRegistry,
    workflow::{
        caching_db_connection::FlushOutcome, event_history::DbErrorWriteOrReplayInterrupt,
        replay_advance::is_closing_join_next,
    },
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ExecutionId,
    storage::{
        self, AppendBatchResponse, AppendEventsToExecution, AppendRequest,
        AppendResponseToExecution, BacktraceInfo, CapturedDbWrite, CreateRequest, DbConnection,
        DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbErrorWriteNonRetriable,
        ExecutionEvent, ExecutionRequest, ResponseCursor, ResponseWithCursor, TimeoutOutcome,
        Version,
    },
};
use std::pin::Pin;
use std::{any::Any, future::Future};
use tracing::{debug, trace};

#[derive(Debug, Clone)]
pub(crate) struct InternalCapturedWrite {
    pub(crate) public: CapturedDbWrite,
    pub(crate) cancellations: Option<JoinSetCloseCancellations>,
}

#[derive(Debug, Clone)]
pub(crate) struct InternalReplayResponse {
    pub(crate) captured_writes: Vec<InternalCapturedWrite>,
}

#[derive(Debug, Default)]
struct ReplayEventCollector {
    writes: Vec<InternalCapturedWrite>,
}

impl ReplayEventCollector {
    fn into_writes(self) -> InternalReplayResponse {
        InternalReplayResponse {
            captured_writes: self.writes,
        }
    }

    fn push_public_write(&mut self, public: CapturedDbWrite) {
        self.writes.push(InternalCapturedWrite {
            public,
            cancellations: None,
        });
    }

    fn push_public_write_with_cancellations(
        &mut self,
        public: CapturedDbWrite,
        cancellations: Option<JoinSetCloseCancellations>,
    ) {
        self.writes.push(InternalCapturedWrite {
            public,
            cancellations,
        });
    }
}

pub(crate) async fn apply_writes(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    actual: Vec<InternalCapturedWrite>,
    old_version: Version,
) -> Result<Version, DbErrorWrite> {
    let mut version = old_version; // In case `actual` is empty, just return the old version.
    for write in &actual {
        if let Some(v) = apply_captured_write(write, conn, cancel_registry).await? {
            version = v;
        }
    }
    Ok(version)
}

async fn apply_captured_write(
    write: &InternalCapturedWrite,
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
) -> Result<Option<Version>, DbErrorWrite> {
    if let Some(cancellations) = &write.cancellations {
        for response_id in cancellations.iterate_in_cancellation_order() {
            match response_id {
                ResponseId::ChildExecutionId(execution_id) => {
                    let res = cancel_registry
                        .cancel_activity(
                            conn,
                            &ExecutionId::Derived(execution_id.clone()),
                            cancellations.cancelled_at,
                        )
                        .await;
                    if let Err(err) = res {
                        debug!("Ignoring failure to cancel activity {execution_id} - {err:?}");
                    }
                }
                ResponseId::DelayId(delay_id) => {
                    let res =
                        storage::cancel_delay(conn, delay_id.clone(), cancellations.cancelled_at)
                            .await;
                    if let Err(err) = res {
                        // This means that the watcher expired the delay in the mean time.
                        trace!("Ignoring failure to cancel {delay_id} - {err:?}");
                    }
                }
            }
        }
    }

    match write.public.clone() {
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
    pub(crate) fn into_writes(self) -> InternalReplayResponse {
        self.collector.into_writes()
    }

    /// Push a captured write and advance the version.
    pub(crate) fn push_write(&mut self, write: CapturedDbWrite) {
        self.collector.push_public_write(write);
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
        assert!(
            !is_closing_join_next(&request),
            "closing join next is not appended using `append_non_blocking`"
        );

        if let Some(child_req) = child_req {
            self.collector
                .push_public_write(CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time: _called_at,
                    batch: vec![request],
                    execution_id: self.execution_id.clone(),
                    version: version.clone(),
                    child_req: vec![child_req],
                    backtraces: backtrace.into_iter().collect(),
                });
        } else {
            self.collector.push_public_write(CapturedDbWrite::Append {
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
        self.collector.push_public_write(CapturedDbWrite::Append {
            execution_id,
            version: version.clone(),
            req,
        });
        self.version = Version::new(version.0 + 1);
        Ok(())
    }

    async fn append_join_set_close(
        &mut self,
        _cancel_registry: &CancelRegistry,
        execution_id: ExecutionId,
        req: AppendRequest,
        cancellations: Option<JoinSetCloseCancellations>,
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        assert!(
            is_closing_join_next(&req),
            "append_join_set_close must append JoinNext(closing=true)"
        );
        // only CachingDbConnection can assert the flush outcome as here `flush_non_blocking_event_cache` is stateless.
        let version = self.version.clone();

        self.collector.push_public_write_with_cancellations(
            CapturedDbWrite::Append {
                execution_id,
                version: version.clone(),
                req,
            },
            cancellations,
        );
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
        for request in &batch {
            assert!(
                !is_closing_join_next(request),
                "closing join next is not appended using `append_batch`"
            );
        }
        self.collector
            .push_public_write(CapturedDbWrite::AppendBatch {
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
        for request in &batch {
            assert!(
                !is_closing_join_next(request),
                "closing join next is not appended using `append_batch_create_new_execution`"
            );
        }
        self.collector
            .push_public_write(CapturedDbWrite::AppendBatchCreateNewExecution {
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
            .push_public_write(CapturedDbWrite::AppendStubResponse {
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
            // any event here must be non-blocking as flush is called before a blocking event.
            Ok(FlushOutcome::Noop)
        } else {
            Ok(FlushOutcome::FlushedCache)
        }
    }
}
