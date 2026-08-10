use super::workflow_worker::JoinNextBlockingStrategy;
use crate::{
    activity::cancel_registry::CancelRegistry,
    workflow::{
        event_history::UpsertStubOrReplayInterrupt,
        replay_advance::{JoinSetCloseCancellations, is_closing_join_next},
    },
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ExecutionId, JoinSetId,
    prefixed_ulid::{DelayId, ExecutionIdDerived},
    storage::{
        self, AppendRequest, AppendResponseToExecution, BacktraceInfo, CreateRequest, DbConnection,
        DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, LogInfoAppendRow, ResponseCursor,
        ResponseWithCursor, TimeoutOutcome, Version,
    },
};
use db_common::JoinSetResponseId;
use std::pin::Pin;
use std::{any::Any, future::Future};
use tracing::{debug, instrument, warn};

#[async_trait]
pub(crate) trait WorkflowDbConnection: Send + Any {
    fn as_any(self: Box<Self>) -> Box<dyn Any>;

    fn execution_id(&self) -> &ExecutionId;

    fn try_defer_application_log(&mut self, row: LogInfoAppendRow) -> bool;

    async fn append_backtrace(&mut self, backtrace: BacktraceInfo) -> Result<(), DbErrorWrite>;

    async fn append_non_blocking(
        &mut self,
        non_blocking_event: CacheableDbEvent,
        called_at: DateTime<Utc>,
    ) -> Result<(), DbErrorWrite>;

    // Caller must trigger flushing before this call.
    async fn append_blocking(
        &mut self,
        version: Version,
        execution_id: ExecutionId,
        req: AppendRequest,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite>;

    #[expect(clippy::too_many_arguments)]
    async fn append_join_set_close(
        &mut self,
        version: Version,
        cancel_registry: &CancelRegistry,
        execution_id: ExecutionId,
        req: AppendRequest,
        cancellations: Option<JoinSetCloseCancellations>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite>;

    async fn append_batch(
        &mut self,
        version: Version,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite>;

    #[expect(clippy::too_many_arguments)]
    async fn append_batch_with_delay_response(
        &mut self,
        version: Version,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        delay_id: DelayId,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite>;

    #[expect(clippy::too_many_arguments)]
    async fn append_batch_create_new_execution(
        &mut self,
        version: Version,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        child_req: Vec<CreateRequest>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite>;

    // `stub_backtrace` is display-only, keyed to the parent's future stub-event version so persist skips it.
    async fn upsert_stub_response(
        &mut self,
        execution_id: ExecutionIdDerived,
        version: Version,
        req: AppendRequest,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
        stub_backtrace: Option<BacktraceInfo>,
    ) -> Result<(), UpsertStubOrReplayInterrupt>;

    // Part of writing stub response: start with this read, then attempt to write the response in `EventHistory::append_to_db_non_blocking`.
    async fn get_stub_create_request(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbErrorRead>;

    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        last_response: ResponseCursor,
        timeout_fut: Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorReadWithTimeout>;

    async fn flush_non_blocking_event_cache(
        &mut self,
        current_time: DateTime<Utc>,
    ) -> Result<(), DbErrorWrite>;
}

pub(crate) struct CachingDbConnection {
    db_connection: Box<dyn DbConnection>,
    execution_id: ExecutionId,
    pub(crate) caching_buffer: Option<CachingBuffer>,
}
impl CachingDbConnection {
    pub(crate) fn new(
        db_connection: Box<dyn DbConnection>,
        execution_id: ExecutionId,
        caching_buffer: Option<CachingBuffer>,
    ) -> CachingDbConnection {
        CachingDbConnection {
            db_connection,
            execution_id,
            caching_buffer,
        }
    }
}

pub(crate) enum CacheableDbEvent {
    SubmitChildExecution {
        request: AppendRequest,
        version: Version,
        child_req: CreateRequest,
        backtrace: Option<BacktraceInfo>,
    },
    /// `SubmitChildExecution` where the intent failed (function not found or params parsing error).
    /// Only persists the history event, no child execution created.
    SubmitChildExecutionError {
        request: AppendRequest,
        version: Version,
        backtrace: Option<BacktraceInfo>,
    },
    Schedule {
        request: AppendRequest,
        version: Version,
        child_req: CreateRequest,
        backtrace: Option<BacktraceInfo>,
    },
    /// Schedule where the intent failed (function not found or params parsing error).
    /// Only persists the history event, no child execution created.
    ScheduleError {
        request: AppendRequest,
        version: Version,
        backtrace: Option<BacktraceInfo>,
    },
    JoinSetCreate {
        request: AppendRequest,
        version: Version,
        backtrace: Option<BacktraceInfo>,
    },
    Persist {
        request: AppendRequest,
        version: Version,
        backtrace: Option<BacktraceInfo>,
    },
    SubmitDelay {
        request: AppendRequest,
        version: Version,
        backtrace: Option<BacktraceInfo>,
    },
    JoinNextTry {
        request: AppendRequest,
        version: Version,
        backtrace: Option<BacktraceInfo>,
    },
}

pub(crate) struct CachingBuffer {
    pub(crate) non_blocking_event_batch_size: usize,
    pub(crate) non_blocking_event_batch: Vec<CacheableDbEvent>,
}
impl CachingBuffer {
    pub(crate) fn new(
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) -> Option<CachingBuffer> {
        let non_blocking_event_batch_size = match join_next_blocking_strategy {
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching,
            } => non_blocking_event_batching as usize,
            JoinNextBlockingStrategy::Interrupt => 0,
        };
        if non_blocking_event_batch_size == 0 {
            None
        } else {
            Some(CachingBuffer {
                non_blocking_event_batch_size,
                non_blocking_event_batch: Vec::with_capacity(non_blocking_event_batch_size),
            })
        }
    }
}

#[async_trait]
impl WorkflowDbConnection for CachingDbConnection {
    fn as_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    fn try_defer_application_log(&mut self, _row: LogInfoAppendRow) -> bool {
        false
    }

    async fn append_backtrace(&mut self, _backtrace: BacktraceInfo) -> Result<(), DbErrorWrite> {
        unreachable!("CachingDbConnection never captures backtraces")
    }

    async fn append_non_blocking(
        &mut self,
        non_blocking_event: CacheableDbEvent,
        called_at: DateTime<Utc>,
    ) -> Result<(), DbErrorWrite> {
        if let Some(caching_buffer) = &mut self.caching_buffer {
            caching_buffer
                .non_blocking_event_batch
                .push(non_blocking_event);
            self.flush_non_blocking_event_cache_if_full(called_at)
                .await?;
        } else {
            // No caching_buffer here, so no flushing before the write.
            match non_blocking_event {
                CacheableDbEvent::Schedule {
                    request,
                    version,
                    child_req,
                    backtrace: _,
                }
                | CacheableDbEvent::SubmitChildExecution {
                    request,
                    version,
                    child_req,
                    backtrace: _,
                } => {
                    let next_version = self
                        .db_connection
                        .append_batch_create_new_execution(
                            called_at,
                            vec![request],
                            self.execution_id.clone(),
                            version.clone(),
                            vec![child_req],
                            vec![],
                        )
                        .await?;
                    assert_eq!(version.increment(), next_version);
                }
                CacheableDbEvent::JoinSetCreate {
                    request,
                    version,
                    backtrace: _,
                }
                | CacheableDbEvent::Persist {
                    request,
                    version,
                    backtrace: _,
                }
                | CacheableDbEvent::SubmitDelay {
                    request,
                    version,
                    backtrace: _,
                }
                | CacheableDbEvent::JoinNextTry {
                    request,
                    version,
                    backtrace: _,
                }
                | CacheableDbEvent::ScheduleError {
                    request,
                    version,
                    backtrace: _,
                }
                | CacheableDbEvent::SubmitChildExecutionError {
                    request,
                    version,
                    backtrace: _,
                } => {
                    let next_version = self
                        .db_connection
                        .append(self.execution_id.clone(), version.clone(), request)
                        .await?;
                    assert_eq!(version.increment(), next_version);
                }
            }
        }
        Ok(())
    }

    // Caller must trigger flushing before this call.
    async fn append_blocking(
        &mut self,
        version: Version,
        execution_id: ExecutionId,
        req: AppendRequest,
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        self.flush_non_blocking_event_cache(req.created_at).await?;
        let next_version = self
            .db_connection
            .append(execution_id, version.clone(), req)
            .await?;
        assert_eq!(version.increment(), next_version);
        Ok(())
    }

    async fn append_batch(
        &mut self,
        version: Version,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        self.flush_non_blocking_event_cache(current_time).await?;
        self.db_connection
            .append_batch(current_time, batch, execution_id, version)
            .await?;
        Ok(())
    }

    async fn append_batch_with_delay_response(
        &mut self,
        version: Version,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        delay_id: DelayId,
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        self.flush_non_blocking_event_cache(current_time).await?;
        self.db_connection
            .append_batch_with_delay_response(
                current_time,
                batch,
                execution_id,
                version,
                join_set_id,
                delay_id,
            )
            .await?;
        Ok(())
    }

    async fn append_join_set_close(
        &mut self,
        version: Version,
        cancel_registry: &CancelRegistry,
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
        self.flush_non_blocking_event_cache(req.created_at).await?;

        // Activities and delays are cancelled in reverse order of creation.
        if let Some(cancellations) = cancellations {
            for response_id in cancellations.iterate_in_cancellation_order() {
                match response_id {
                    JoinSetResponseId::ChildExecutionId(child_execution_id_derived) => {
                        let res = cancel_registry
                            .cancel_activity(
                                self.db_connection.as_ref(),
                                &ExecutionId::Derived(child_execution_id_derived.clone()),
                                cancellations.cancelled_at,
                            )
                            .await;
                        if let Err(err) = res {
                            debug!(
                                "Ignoring failure to cancel activity {child_execution_id_derived} - {err:?}"
                            );
                        }
                    }
                    JoinSetResponseId::DelayId(delay_id) => {
                        let res = storage::cancel_delay(
                            self.db_connection.as_ref(),
                            delay_id.clone(),
                            cancellations.cancelled_at,
                        )
                        .await;
                        if let Err(err) = res {
                            debug!("Ignoring failure to cancel delay {delay_id} - {err:?}");
                        }
                    }
                }
            }
            // Signal cancellable children; the driver drives their close and the
            // `Cancelled` response wakes our await.
            for child_id in cancellations.cancellable_child_ids() {
                let res = self
                    .db_connection
                    .cancel_workflow_with_retries(
                        &ExecutionId::Derived(child_id.clone()),
                        cancellations.cancelled_at,
                    )
                    .await;
                if let Err(err) = res {
                    debug!("Ignoring failure to signal cancellable child {child_id} - {err:?}");
                }
            }
        }

        self.append_blocking(version, execution_id, req, wasm_backtrace, component_id)
            .await
    }

    async fn append_batch_create_new_execution(
        &mut self,
        version: Version,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        child_req: Vec<CreateRequest>,
        _wasm_backtrace: Option<storage::WasmBacktrace>,
        _component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        self.flush_non_blocking_event_cache(current_time).await?;
        let expected_next_version =
            Version(version.0 + u32::try_from(batch.len()).expect("max 3 won't overflow"));
        let next_version = self
            .db_connection
            .append_batch_create_new_execution(
                current_time,
                batch,
                execution_id,
                version,
                child_req,
                vec![],
            )
            .await?;
        assert_eq!(next_version, expected_next_version); // must hold, assumed when creating the backtrace `version_max_excluding`

        Ok(())
    }

    async fn upsert_stub_response(
        &mut self,
        execution_id: ExecutionIdDerived,
        version: Version,
        req: AppendRequest,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
        _stub_backtrace: Option<BacktraceInfo>,
    ) -> Result<(), UpsertStubOrReplayInterrupt> {
        // This write bypasses the cache (it must return the conflict result
        // immediately), so flush first to keep it ordered after any buffered write.
        // Without this a self-fulfilled stub, whose child `submit` is still buffered,
        // creates the child here and again when the buffer flushes.
        self.flush_non_blocking_event_cache(current_time)
            .await
            .map_err(UpsertStubOrReplayInterrupt::DbError)?;
        self.db_connection
            .upsert_stub_response(execution_id, version, req, response, current_time)
            .await
            .map_err(|err| match err {
                concepts::storage::DbErrorStubResponse::StubConflict => {
                    UpsertStubOrReplayInterrupt::StubConflict
                }
                concepts::storage::DbErrorStubResponse::Write(db_err) => {
                    UpsertStubOrReplayInterrupt::DbError(db_err)
                }
            })
    }

    async fn get_stub_create_request(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbErrorRead> {
        if let Some(caching_buffer) = &self.caching_buffer
            && let Some(found) = caching_buffer
                .non_blocking_event_batch
                .iter()
                .find_map(|event| match event {
                    CacheableDbEvent::SubmitChildExecution {
                        request,
                        version,
                        child_req,
                        backtrace,
                    } if child_req.execution_id == *execution_id => Some(child_req.clone()),
                    _ => None,
                })
        {
            return Ok(found);
        }

        self.db_connection.get_create_request(execution_id).await
    }

    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        last_response: ResponseCursor,
        timeout_fut: Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorReadWithTimeout> {
        self.db_connection
            .subscribe_to_next_responses(execution_id, last_response, timeout_fut)
            .await
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
    async fn flush_non_blocking_event_cache(
        &mut self,
        current_time: DateTime<Utc>,
    ) -> Result<(), DbErrorWrite> {
        if let Some(caching_buffer) = &mut self.caching_buffer
            && !caching_buffer.non_blocking_event_batch.is_empty()
        {
            debug!("Flushing the non-blocking event cache started");
            let mut batches = Vec::with_capacity(caching_buffer.non_blocking_event_batch.len());
            let mut childs = Vec::with_capacity(caching_buffer.non_blocking_event_batch.len());
            let mut first_version = None;
            for non_blocking in caching_buffer.non_blocking_event_batch.drain(..) {
                match non_blocking {
                    CacheableDbEvent::SubmitChildExecution {
                        request,
                        version,
                        child_req,
                        backtrace: _,
                    }
                    | CacheableDbEvent::Schedule {
                        request,
                        version,
                        child_req,
                        backtrace: _,
                    } => {
                        if first_version.is_none() {
                            first_version.replace(version);
                        }
                        childs.push(child_req);
                        batches.push(request);
                    }
                    CacheableDbEvent::JoinSetCreate {
                        request,
                        version,
                        backtrace: _,
                    }
                    | CacheableDbEvent::Persist {
                        request,
                        version,
                        backtrace: _,
                    }
                    | CacheableDbEvent::SubmitDelay {
                        request,
                        version,
                        backtrace: _,
                    }
                    | CacheableDbEvent::JoinNextTry {
                        request,
                        version,
                        backtrace: _,
                    }
                    | CacheableDbEvent::ScheduleError {
                        request,
                        version,
                        backtrace: _,
                    }
                    | CacheableDbEvent::SubmitChildExecutionError {
                        request,
                        version,
                        backtrace: _,
                    } => {
                        if first_version.is_none() {
                            first_version.replace(version);
                        }
                        batches.push(request);
                    }
                }
            }
            assert!(!batches.is_empty());
            self.db_connection
                .append_batch_create_new_execution(
                    current_time,
                    batches,
                    self.execution_id.clone(),
                    first_version.expect("checked that !non_blocking_event_batch.is_empty()"),
                    childs,
                    vec![],
                )
                .await?;

            debug!("Flushing the non-blocking event cache finished");
        }
        Ok(())
    }
}

impl Drop for CachingDbConnection {
    fn drop(&mut self) {
        if let Some(caching_buffer) = &self.caching_buffer
            && !caching_buffer.non_blocking_event_batch.is_empty()
        {
            warn!(
                execution_id = %self.execution_id,
                cache_len = caching_buffer.non_blocking_event_batch.len(),
                "CachingDbConnection dropped with non-empty cache"
            );
        }
    }
}

impl CachingDbConnection {
    async fn flush_non_blocking_event_cache_if_full(
        &mut self,
        current_time: DateTime<Utc>,
    ) -> Result<(), DbErrorWrite> {
        if let Some(caching_buffer) = &self.caching_buffer {
            let too_many = caching_buffer.non_blocking_event_batch.len()
                >= caching_buffer.non_blocking_event_batch_size;
            if too_many {
                self.flush_non_blocking_event_cache(current_time).await?;
            }
        }
        Ok(())
    }
}
