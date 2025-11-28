use std::pin::Pin;

use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ExecutionId,
    storage::{
        self, AppendBatchResponse, AppendEventsToExecution, AppendRequest, AppendResponse,
        AppendResponseToExecution, BacktraceInfo, CreateRequest, DbConnection, DbErrorRead,
        DbErrorReadWithTimeout, DbErrorWrite, ExecutionEvent, JoinSetResponseEventOuter, Version,
    },
};
use tracing::{debug, instrument};

use super::workflow_worker::JoinNextBlockingStrategy;

pub(crate) struct CachingDbConnection {
    pub(crate) db_connection: Box<dyn DbConnection>,
    pub(crate) execution_id: ExecutionId,
    pub(crate) caching_buffer: Option<CachingBuffer>,
}
pub(crate) enum NonBlockingCache {
    SubmitChildExecution {
        batch: Vec<AppendRequest>,
        version: Version,
        child_req: CreateRequest,
        backtrace: Option<BacktraceInfo>,
    },
    Schedule {
        batch: Vec<AppendRequest>,
        version: Version,
        child_req: CreateRequest,
        backtrace: Option<BacktraceInfo>,
    },
}

pub(crate) struct CachingBuffer {
    pub(crate) non_blocking_event_batch_size: usize,
    pub(crate) non_blocking_event_batch: Vec<NonBlockingCache>,
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
        non_blocking_event: NonBlockingCache,
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
            let next_version = match non_blocking_event {
                NonBlockingCache::Schedule {
                    batch,
                    version,
                    child_req,
                    backtrace,
                }
                | NonBlockingCache::SubmitChildExecution {
                    batch,
                    version,
                    child_req,
                    backtrace,
                } => {
                    let next_version = self
                        .db_connection
                        .append_batch_create_new_execution(
                            called_at,
                            batch,
                            self.execution_id.clone(),
                            version.clone(),
                            vec![child_req],
                        )
                        .await?;
                    if let Some(backtrace) = backtrace
                        && let Err(err) = self.db_connection.append_backtrace(backtrace).await
                    {
                        debug!("Ignoring error while appending backtrace: {err:?}");
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
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbErrorWrite> {
        self.db_connection.append(execution_id, version, req).await
    }

    pub(crate) async fn append_batch(
        &self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        self.db_connection
            .append_batch(current_time, batch, execution_id, version)
            .await
    }

    pub(crate) async fn append_batch_create_new_execution(
        &self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        self.db_connection
            .append_batch_create_new_execution(
                current_time,
                batch,
                execution_id,
                version,
                child_req,
            )
            .await
    }

    pub(crate) async fn append_batch_respond_to_parent(
        &self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
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

    pub(crate) async fn flush_non_blocking_event_cache_if_full(
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
            let mut wasm_backtraces =
                Vec::with_capacity(caching_buffer.non_blocking_event_batch.len());
            for non_blocking in caching_buffer.non_blocking_event_batch.drain(..) {
                match non_blocking {
                    NonBlockingCache::SubmitChildExecution {
                        batch,
                        version,
                        child_req,
                        backtrace,
                    }
                    | NonBlockingCache::Schedule {
                        batch,
                        version,
                        child_req,
                        backtrace,
                    } => {
                        if first_version.is_none() {
                            first_version.replace(version);
                        }
                        childs.push(child_req);
                        assert!(!batch.is_empty());
                        batches.extend(batch);
                        if let Some(backtrace) = backtrace {
                            wasm_backtraces.push(backtrace);
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
            if !wasm_backtraces.is_empty() {
                let res = self
                    .db_connection
                    .append_backtrace_batch(wasm_backtraces)
                    .await;
                if let Err(err) = res {
                    debug!("Ignoring error while appending backtrace: {err:?}");
                }
            }
            debug!("Flushing the non-blocking event cache finished");
        }
        Ok(())
    }

    pub(crate) async fn persist_backtrace_blocking(
        &mut self,
        version: &Version,
        next_version: &Version,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        component_id: ComponentId,
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

            if let Err(err) = self
                .db_connection
                .append_backtrace(BacktraceInfo {
                    execution_id: self.execution_id.clone(),
                    component_id,
                    version_min_including: version.clone(),
                    version_max_excluding: next_version.clone(),
                    wasm_backtrace,
                })
                .await
            {
                debug!("Ignoring error while appending backtrace: {err:?}");
            }
        }
    }
}
