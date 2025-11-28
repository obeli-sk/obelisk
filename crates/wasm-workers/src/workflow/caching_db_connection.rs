use super::workflow_worker::JoinNextBlockingStrategy;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ExecutionId,
    storage::{
        self, AppendBatchResponse, AppendEventsToExecution, AppendRequest,
        AppendResponseToExecution, BacktraceInfo, CreateRequest, DbConnection, DbErrorRead,
        DbErrorReadWithTimeout, DbErrorWrite, ExecutionEvent, JoinSetResponseEventOuter, Version,
    },
};
use std::pin::Pin;
use tracing::{debug, instrument};

pub(crate) struct CachingDbConnection {
    pub(crate) db_connection: Box<dyn DbConnection>,
    pub(crate) execution_id: ExecutionId,
    pub(crate) caching_buffer: Option<CachingBuffer>,
}
pub(crate) enum CacheableDbEvent {
    SubmitChildExecution {
        request: AppendRequest,
        version: Version,
        child_req: CreateRequest,
        backtrace: Option<BacktraceInfo>,
    },
    Schedule {
        request: AppendRequest,
        version: Version,
        child_req: CreateRequest,
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

impl CachingDbConnection {
    pub(crate) async fn append_non_blocking(
        &mut self,
        non_blocking_event: CacheableDbEvent,
        called_at: DateTime<Utc>,
        version: &mut Version,
    ) -> Result<(), DbErrorWrite> {
        if let Some(caching_buffer) = &mut self.caching_buffer {
            let next_version = Version::new(version.0 + 1);
            caching_buffer
                .non_blocking_event_batch
                .push(non_blocking_event);
            self.flush_non_blocking_event_cache_if_full(called_at)
                .await?;
            *version = next_version;
        } else {
            // No caching_buffer here, so no flushing before the write.
            let next_version = match non_blocking_event {
                CacheableDbEvent::Schedule {
                    request,
                    version,
                    child_req,
                    backtrace,
                }
                | CacheableDbEvent::SubmitChildExecution {
                    request,
                    version,
                    child_req,
                    backtrace,
                } => {
                    let next_version = self
                        .db_connection
                        .append_batch_create_new_execution(
                            called_at,
                            vec![request],
                            self.execution_id.clone(),
                            version.clone(),
                            vec![child_req],
                        )
                        .await?;
                    if let Some(backtrace) = backtrace {
                        let _ = self
                            .db_connection
                            .append_backtrace(backtrace)
                            .await
                            .inspect_err(|err| {
                                debug!("Ignoring error while appending backtrace: {err:?}");
                            });
                    }
                    next_version
                }
                CacheableDbEvent::JoinSetCreate {
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
                } => {
                    let next_version = self
                        .db_connection
                        .append(self.execution_id.clone(), version.clone(), request)
                        .await?;

                    if let Some(backtrace) = backtrace {
                        let _ = self
                            .db_connection
                            .append_backtrace(backtrace)
                            .await
                            .inspect_err(|err| {
                                debug!("Ignoring error while appending backtrace: {err:?}");
                            });
                    }
                    next_version
                }
            };
            *version = next_version;
        }
        Ok(())
    }

    pub(crate) async fn append_blocking(
        &mut self,
        execution_id: ExecutionId,
        version: &mut Version,
        req: AppendRequest,
        called_at: DateTime<Utc>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        self.flush_non_blocking_event_cache(called_at).await?;
        let next_version = self
            .db_connection
            .append(execution_id, version.clone(), req)
            .await?;
        self.persist_backtrace_blocking(version, &next_version, wasm_backtrace, component_id)
            .await;
        *version = next_version;
        Ok(())
    }

    pub(crate) async fn append_batch(
        &mut self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: &mut Version,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        self.flush_non_blocking_event_cache(current_time).await?;
        let next_version = self
            .db_connection
            .append_batch(current_time, batch, execution_id, version.clone())
            .await?;
        self.persist_backtrace_blocking(version, &next_version, wasm_backtrace, component_id)
            .await;
        *version = next_version;
        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn append_batch_create_new_execution(
        &mut self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: &mut Version,
        child_req: Vec<CreateRequest>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) -> Result<(), DbErrorWrite> {
        self.flush_non_blocking_event_cache(current_time).await?;
        let next_version = self
            .db_connection
            .append_batch_create_new_execution(
                current_time,
                batch,
                execution_id,
                version.clone(),
                child_req,
            )
            .await?;
        self.persist_backtrace_blocking(version, &next_version, wasm_backtrace, component_id)
            .await;
        *version = next_version;
        Ok(())
    }

    pub(crate) async fn append_batch_respond_to_parent(
        &mut self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        self.flush_non_blocking_event_cache(current_time).await?;
        self.db_connection
            .append_batch_respond_to_parent(events, response, current_time)
            .await
    }

    pub(crate) async fn get_create_request(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbErrorRead> {
        self.db_connection.get_create_request(execution_id).await
    }

    pub(crate) async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        self.db_connection
            .get_execution_event(execution_id, version)
            .await
    }

    pub(crate) async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: usize,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbErrorReadWithTimeout> {
        self.db_connection
            .subscribe_to_next_responses(execution_id, start_idx, timeout_fut)
            .await
    }

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

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
    pub(crate) async fn flush_non_blocking_event_cache(
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
            let mut backtraces = Vec::with_capacity(caching_buffer.non_blocking_event_batch.len());
            for non_blocking in caching_buffer.non_blocking_event_batch.drain(..) {
                match non_blocking {
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
                    } => {
                        if first_version.is_none() {
                            first_version.replace(version);
                        }
                        childs.push(child_req);
                        batches.push(request);
                        if let Some(backtrace) = backtrace {
                            backtraces.push(backtrace);
                        }
                    }
                    CacheableDbEvent::JoinSetCreate {
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
                    } => {
                        if first_version.is_none() {
                            first_version.replace(version);
                        }
                        batches.push(request);
                        if let Some(backtrace) = backtrace {
                            backtraces.push(backtrace);
                        }
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
                )
                .await?;
            if !backtraces.is_empty() {
                let _ = self
                    .db_connection
                    .append_backtrace_batch(backtraces)
                    .await
                    .inspect_err(|err| debug!("Ignoring error while appending backtrace: {err:?}"));
            }
            debug!("Flushing the non-blocking event cache finished");
        }
        Ok(())
    }

    async fn persist_backtrace_blocking(
        &mut self,
        version: &Version,
        next_version: &Version,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: &ComponentId,
    ) {
        if let Some(wasm_backtrace) = wasm_backtrace {
            assert_eq!(
                self.caching_buffer
                    .as_ref()
                    .map(|caching_buffer| caching_buffer.non_blocking_event_batch.len())
                    .unwrap_or_default(),
                0,
                "persist_backtrace_blocking must be called only after flushing `non_blocking_event_batch`"
            );

            let _ = self
                .db_connection
                .append_backtrace(BacktraceInfo {
                    execution_id: self.execution_id.clone(),
                    component_id: component_id.clone(),
                    version_min_including: version.clone(),
                    version_max_excluding: next_version.clone(),
                    wasm_backtrace,
                })
                .await
                .inspect_err(|err| debug!("Ignoring error while appending backtrace: {err:?}"));
        }
    }
}
