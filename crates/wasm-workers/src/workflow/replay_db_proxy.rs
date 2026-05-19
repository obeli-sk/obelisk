//! A workflow database proxy for replay that captures all write operations in memory
//! instead of persisting them. This allows `replay_internal` to collect the operations
//! that the workflow would produce next.

use super::caching_db_connection::{CacheableDbEvent, WorkflowDbConnection};
use crate::workflow::host_exports::response_id::ResponseId;
use crate::workflow::replay_advance::JoinSetCloseCancellations;
use crate::{
    activity::cancel_registry::CancelRegistry,
    workflow::{event_history::UpsertStubOrReplayInterrupt, replay_advance::is_closing_join_next},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::storage::DbErrorStubResponse;
use concepts::{
    ComponentId, ExecutionId, JoinSetId,
    prefixed_ulid::ExecutionIdDerived,
    storage::{
        self, AppendEventsToExecution, AppendRequest, AppendResponseToExecution, BacktraceInfo,
        CapturedDbWrite, CreateRequest, DbConnection, DbErrorRead, DbErrorReadWithTimeout,
        DbErrorWrite, DbErrorWriteNonRetriable, ExecutionRequest, LogInfoAppendRow, ResponseCursor,
        ResponseWithCursor, TimeoutOutcome, Version,
    },
};
use std::pin::Pin;
use std::{any::Any, future::Future};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

#[derive(Debug, Clone)]
pub(crate) struct InternalCapturedWrite {
    pub(crate) write: CapturedDbWrite,
    cancellations: Option<JoinSetCloseCancellations>,
    pub(crate) logs: Vec<LogInfoAppendRow>,
}

#[cfg(test)]
impl InternalCapturedWrite {
    pub(crate) fn new_for_test(write: CapturedDbWrite, logs: Vec<LogInfoAppendRow>) -> Self {
        Self {
            write,
            cancellations: None,
            logs,
        }
    }
}

#[derive(Debug, Default)]
struct ReplayEventCollector {
    preview: Vec<InternalCapturedWrite>,
    // Logs emitted during replay belong to the next captured write, which
    // corresponds to the next user-code step that would be persisted.
    pending_logs: Vec<LogInfoAppendRow>,
}

impl ReplayEventCollector {
    fn into_writes(self) -> Vec<InternalCapturedWrite> {
        if !self.pending_logs.is_empty() {
            let pending_messages: Vec<_> = self
                .pending_logs
                .iter()
                .map(|row| format!("{:?}", row.log_entry))
                .collect();
            warn!(
                pending_logs = self.pending_logs.len(),
                ?pending_messages,
                "Replay finished with pending application logs that were not attached to a captured write"
            );
        }
        debug_assert!(
            self.pending_logs.is_empty(),
            "replay must not finish with unattached application logs: {:?}",
            self.pending_logs
        );
        self.preview
    }

    fn push_write(&mut self, write: CapturedDbWrite) {
        self.preview.push(InternalCapturedWrite {
            write,
            cancellations: None,
            logs: std::mem::take(&mut self.pending_logs),
        });
    }

    fn push_write_with_cancellations(
        &mut self,
        write: CapturedDbWrite,
        cancellations: Option<JoinSetCloseCancellations>,
    ) {
        self.preview.push(InternalCapturedWrite {
            write,
            cancellations,
            logs: std::mem::take(&mut self.pending_logs),
        });
    }

    fn push_log(&mut self, row: LogInfoAppendRow) {
        self.pending_logs.push(row);
    }
}

pub(crate) async fn apply_writes(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    log_forwarder_sender: Option<&mpsc::Sender<LogInfoAppendRow>>,
    actual: Vec<InternalCapturedWrite>,
    old_version: Version,
) -> Result<Version, DbErrorWrite> {
    let mut version = old_version; // In case `actual` is empty, just return the old version.
    for write in &actual {
        if let Some(v) = apply_captured_write(write, conn, cancel_registry).await? {
            version = v;
        }
        if let Some(log_forwarder_sender) = log_forwarder_sender {
            for row in &write.logs {
                if let Err(err) = log_forwarder_sender.try_send(row.clone()) {
                    warn!("Dropping captured workflow log message: {err:?}");
                }
            }
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

    match write.write.clone() {
        CapturedDbWrite::Append {
            execution_id,
            version,
            req,
            backtraces,
        } => {
            let result = conn.append(execution_id, version, req).await?;
            if !backtraces.is_empty() {
                conn.append_backtrace_batch(backtraces).await?;
            }
            Ok(Some(result))
        }
        CapturedDbWrite::AppendBatch {
            current_time,
            batch,
            execution_id,
            version,
            backtraces,
        } => {
            let result = conn
                .append_batch(current_time, batch, execution_id, version)
                .await?;
            if !backtraces.is_empty() {
                conn.append_backtrace_batch(backtraces).await?;
            }
            Ok(Some(result))
        }
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
            backtraces: _,
        } => {
            let mut batch = events.batch;
            let req = batch.pop().expect("stub batch must have exactly one item");
            debug_assert!(batch.is_empty(), "stub batch must have exactly one item");
            let ExecutionId::Derived(derived_id) = events.execution_id else {
                panic!("stub execution_id must be derived");
            };
            conn.upsert_stub_response(derived_id, events.version, req, response, current_time)
                .await
                .map_err(|err| match err {
                    DbErrorStubResponse::StubConflict => {
                        DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict)
                    }
                    DbErrorStubResponse::Write(db_err) => db_err,
                })?;
            Ok(None)
        }
        CapturedDbWrite::AppendFinished {
            execution_id,
            version,
            current_time,
            retval,
            parent,
        } => {
            if let Some((parent_execution_id, parent_join_set)) = parent {
                let ExecutionId::Derived(child_execution_id) = execution_id.clone() else {
                    panic!("AppendFinished with parent must have a derived execution_id")
                };
                let events = AppendEventsToExecution {
                    execution_id,
                    version: version.clone(),
                    batch: vec![AppendRequest {
                        created_at: current_time,
                        event: ExecutionRequest::Finished {
                            retval: retval.clone(),
                            http_client_traces: None,
                        },
                    }],
                };
                let response = AppendResponseToExecution {
                    parent_execution_id,
                    created_at: current_time,
                    join_set_id: parent_join_set,
                    child_execution_id,
                    finished_version: version,
                    result: retval,
                };
                conn.append_batch_respond_to_parent(events, response, current_time)
                    .await
                    .map(Some)
            } else {
                conn.append(
                    execution_id,
                    version,
                    AppendRequest {
                        created_at: current_time,
                        event: ExecutionRequest::Finished {
                            retval,
                            http_client_traces: None,
                        },
                    },
                )
                .await
                .map(Some)
            }
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
    parent: Option<(ExecutionId, JoinSetId)>,
}

impl ReplayWorkflowDbConnection {
    pub(crate) fn new(
        execution_id: ExecutionId,
        version: Version,
        real_connection: Box<dyn DbConnection>,
        parent: Option<(ExecutionId, JoinSetId)>,
    ) -> Self {
        Self {
            execution_id,
            collector: ReplayEventCollector::default(),
            version,
            real_connection,
            parent,
        }
    }
    pub(crate) fn into_writes(self) -> Vec<InternalCapturedWrite> {
        self.collector.into_writes()
    }

    /// Push a captured write and advance the version.
    pub(crate) fn push_write(&mut self, write: CapturedDbWrite) {
        self.collector.push_write(write);
    }

    pub(crate) fn push_log(&mut self, row: LogInfoAppendRow) {
        self.collector.push_log(row);
    }

    pub(crate) fn version(&self) -> &Version {
        &self.version
    }

    pub(crate) fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    pub(crate) fn parent(&self) -> Option<(ExecutionId, JoinSetId)> {
        self.parent.clone()
    }
}

fn make_backtrace(
    execution_id: &ExecutionId,
    component_id: &ComponentId,
    version: &Version,
    next_version: &Version,
    wasm_backtrace: Option<storage::WasmBacktrace>,
) -> Vec<BacktraceInfo> {
    wasm_backtrace
        .map(|wasm_backtrace| BacktraceInfo {
            execution_id: execution_id.clone(),
            component_id: component_id.clone(),
            version_min_including: version.clone(),
            version_max_excluding: next_version.clone(),
            wasm_backtrace,
        })
        .into_iter()
        .collect()
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

    fn capture_application_log(&mut self, row: LogInfoAppendRow) -> bool {
        self.push_log(row);
        true
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
                .push_write(CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time: _called_at,
                    batch: vec![request],
                    execution_id: self.execution_id.clone(),
                    version: version.clone(),
                    child_req: vec![child_req],
                    backtraces: backtrace.into_iter().collect(),
                });
        } else {
            self.collector.push_write(CapturedDbWrite::Append {
                execution_id: self.execution_id.clone(),
                version: version.clone(),
                req: request,
                backtraces: backtrace.into_iter().collect(),
            });
        }
        self.version = Version::new(version.0 + 1);
        Ok(())
    }

    async fn append_blocking(
        &mut self,
        execution_id: ExecutionId,
        req: AppendRequest,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        let version = self.version.clone();
        let next_version = Version::new(version.0 + 1);
        let backtraces = make_backtrace(
            &execution_id,
            component_id,
            &version,
            &next_version,
            wasm_backtrace,
        );
        self.collector.push_write(CapturedDbWrite::Append {
            execution_id,
            version: version.clone(),
            req,
            backtraces,
        });
        self.version = next_version;
        Ok(())
    }

    async fn append_join_set_close(
        &mut self,
        _cancel_registry: &CancelRegistry,
        execution_id: ExecutionId,
        req: AppendRequest,
        cancellations: Option<JoinSetCloseCancellations>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        assert_eq!(self.execution_id, execution_id);
        assert!(
            is_closing_join_next(&req),
            "append_join_set_close must append JoinNext(closing=true)"
        );
        // only CachingDbConnection can assert the flush outcome as here `flush_non_blocking_event_cache` is stateless.
        let version = self.version.clone();
        let next_version = Version::new(version.0 + 1);
        let backtraces = make_backtrace(
            &execution_id,
            component_id,
            &version,
            &next_version,
            wasm_backtrace,
        );

        self.collector.push_write_with_cancellations(
            CapturedDbWrite::Append {
                execution_id,
                version: version.clone(),
                req,
                backtraces,
            },
            cancellations,
        );
        self.version = next_version;
        Ok(())
    }

    async fn append_batch(
        &mut self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
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
        let backtraces =
            make_backtrace(&execution_id, component_id, &version, &next, wasm_backtrace);
        self.collector.push_write(CapturedDbWrite::AppendBatch {
            current_time,
            batch,
            execution_id,
            version,
            backtraces,
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
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
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
        let backtraces =
            make_backtrace(&execution_id, component_id, &version, &next, wasm_backtrace);
        self.collector
            .push_write(CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time,
                batch,
                execution_id,
                version,
                child_req,
                backtraces,
            });
        self.version = next;
        Ok(())
    }

    async fn upsert_stub_response(
        &mut self,
        execution_id: ExecutionIdDerived,
        version: Version,
        req: AppendRequest,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), UpsertStubOrReplayInterrupt> {
        let execution_id = ExecutionId::Derived(execution_id);
        // Query the database first.
        let stub_finished_version = Version::new(1); // Stub activities have no execution log except Created event.
        if let Ok(found_stub) = self
            .real_connection
            .get_execution_event(&execution_id, &stub_finished_version)
            .await
        {
            match found_stub.event {
                ExecutionRequest::Finished { retval, .. } if retval == response.result => {
                    // Same value already written — idempotent success.
                    return Ok(());
                }
                ExecutionRequest::Finished { .. } => {
                    // Different value — conflict.
                    return Err(UpsertStubOrReplayInterrupt::StubConflict);
                }
                _ => {
                    // This must not be a stub execution.
                    return Err(UpsertStubOrReplayInterrupt::DbError(
                        DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict),
                    ));
                }
            }
        }

        let next = next_version(&version, 1);
        let backtraces = make_backtrace(
            &self.execution_id,
            component_id,
            &self.version,
            &next,
            wasm_backtrace,
        );
        self.collector
            .push_write(CapturedDbWrite::AppendStubResponse {
                events: AppendEventsToExecution {
                    execution_id,
                    version,
                    batch: vec![req],
                },
                response,
                current_time,
                backtraces,
            });
        // no idea about the response
        Err(UpsertStubOrReplayInterrupt::ReplayInterrupt)
    }

    // Replay assumes it is the only writer to its own and its children's exec log so it can read uncomitted `CreateRequest` from memory.
    async fn get_stub_create_request(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbErrorRead> {
        if let Some(create_req) =
            self.collector
                .preview
                .iter()
                .rev()
                .find_map(|captured| match &captured.write {
                    CapturedDbWrite::AppendBatchCreateNewExecution { child_req, .. } => child_req
                        .iter()
                        .find(|create_req| &create_req.execution_id == execution_id)
                        .cloned(),
                    _ => None,
                })
        {
            return Ok(create_req);
        }
        self.real_connection.get_create_request(execution_id).await
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
        // noop
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::caching_db_connection::{CachingBuffer, CachingDbConnection};
    use crate::workflow::workflow_worker::JoinNextBlockingStrategy;
    use chrono::Utc;
    use concepts::storage::DbPool;
    use concepts::{FunctionFqn, Params};
    use db_mem::inmemory_dao::InMemoryPool;
    use rstest::rstest;
    use std::sync::Arc;

    enum ConnectionMode {
        Caching,
        Replay,
    }

    #[rstest]
    #[case::caching(ConnectionMode::Caching)]
    #[case::replay(ConnectionMode::Replay)]
    #[tokio::test]
    async fn get_stub_create_request_reads_from_captured_writes(#[case] mode: ConnectionMode) {
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let real_connection = db_pool.connection().await.unwrap();
        let parent_execution_id = ExecutionId::from_parts(0, 0);
        let child_execution_id = ExecutionId::from_parts(0, 1);
        let created_at = Utc::now();
        let parent_version = real_connection
            .create(CreateRequest {
                created_at,
                execution_id: parent_execution_id.clone(),
                ffqn: FunctionFqn::new_static(
                    "testing:integration/workflow-call-stub",
                    "call-stub",
                ),
                params: Params::from_json_values_test(vec![serde_json::json!(123_u64)]),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: ComponentId::dummy_workflow(),
                deployment_id: concepts::prefixed_ulid::DeploymentId::generate(),
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();
        let create_request = CreateRequest {
            created_at,
            execution_id: child_execution_id.clone(),
            ffqn: FunctionFqn::new_static("testing:integration/stubs", "my-stub"),
            params: Params::from_json_values_test(vec![serde_json::json!(123_u64)]),
            parent: Some((
                parent_execution_id.clone(),
                concepts::JoinSetId::new(
                    concepts::JoinSetKind::Generated,
                    concepts::StrVariant::Static("1"),
                )
                .unwrap(),
            )),
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: created_at,
            component_id: ComponentId::dummy_activity(),
            deployment_id: concepts::prefixed_ulid::DeploymentId::generate(),
            scheduled_by: None,
            paused: false,
        };
        let mut connection: Box<dyn WorkflowDbConnection> = match mode {
            ConnectionMode::Caching => Box::new(CachingDbConnection::new(
                real_connection,
                parent_execution_id.clone(),
                CachingBuffer::new(JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 100,
                }),
                parent_version,
            )),
            ConnectionMode::Replay => Box::new(ReplayWorkflowDbConnection::new(
                parent_execution_id.clone(),
                parent_version,
                real_connection,
                None, // test helper, no parent
            )),
        };
        connection
            .append_batch_create_new_execution(
                created_at,
                vec![AppendRequest {
                    created_at,
                    event: concepts::storage::ExecutionRequest::HistoryEvent {
                        event: concepts::storage::HistoryEvent::Persist {
                            value: vec![1],
                            kind: concepts::storage::PersistKind::ExecutionId,
                        },
                    },
                }],
                parent_execution_id,
                vec![create_request.clone()],
                None,
                &ComponentId::dummy_activity(),
            )
            .await
            .unwrap();

        let found = connection
            .get_stub_create_request(&child_execution_id)
            .await
            .unwrap();

        assert_eq!(found, create_request);
    }
}
