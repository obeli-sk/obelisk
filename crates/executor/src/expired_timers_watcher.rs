use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ExecutorId;
use db::{
    storage::{
        AppendRequest, AsyncResponse, DbConnection, DbConnectionError, ExecutionEventInner,
        ExpiredTimer, HistoryEvent,
    },
    FinishedExecutionError,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::AbortHandle;
use tracing::{debug, info, info_span, instrument, trace, warn, Instrument};
use utils::time::ClockFn;

#[derive(Debug, Clone)]
pub struct Config<C: ClockFn> {
    pub tick_sleep: Duration,
    pub clock_fn: C,
}

pub struct Task<DB: DbConnection> {
    pub(crate) db_connection: DB,
}

#[allow(dead_code)] // FIXME: add test
#[derive(Debug)]
pub(crate) struct TickProgress {
    pub(crate) expired_locks: usize,
    pub(crate) expired_async_timers: usize,
}

pub struct TaskHandle {
    executor_id: ExecutorId,
    is_closing: Arc<AtomicBool>,
    abort_handle: AbortHandle,
}

impl TaskHandle {
    #[instrument(skip_all, fields(executor_id = %self.executor_id))]
    pub async fn close(&self) {
        trace!("Gracefully closing");
        self.is_closing.store(true, Ordering::Relaxed);
        while !self.abort_handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        info!("Gracefully closed");
    }
}

impl Drop for TaskHandle {
    #[instrument(skip_all, fields(executor_id = %self.executor_id))]
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!("Aborting the task");
        self.abort_handle.abort();
    }
}

impl<DB: DbConnection> Task<DB> {
    pub fn spawn_new<C: ClockFn + 'static>(db_connection: DB, config: Config<C>) -> TaskHandle {
        let executor_id = ExecutorId::generate();
        let span = info_span!("lock_watcher",
            executor = %executor_id,
        );
        let is_closing = Arc::new(AtomicBool::default());
        let is_closing_inner = is_closing.clone();
        let tick_sleep = config.tick_sleep;
        let abort_handle = tokio::spawn(
            async move {
                info!("Spawned expired lock watcher");
                let task = Self { db_connection };
                let mut old_err = None;
                loop {
                    let res = task.tick((config.clock_fn)()).await;
                    Self::log_err_if_new(res, &mut old_err);
                    if is_closing_inner.load(Ordering::Relaxed) {
                        return;
                    }
                    tokio::time::sleep(tick_sleep).await;
                }
            }
            .instrument(span),
        )
        .abort_handle();
        TaskHandle {
            executor_id,
            is_closing,
            abort_handle,
        }
    }

    fn log_err_if_new(
        res: Result<TickProgress, DbConnectionError>,
        old_err: &mut Option<DbConnectionError>,
    ) {
        match (res, &old_err) {
            (Ok(_), _) => {
                *old_err = None;
            }
            (Err(err), Some(old)) if err == *old => {}
            (Err(err), _) => {
                warn!("Tick failed: {err:?}");
                *old_err = Some(err);
            }
        }
    }

    #[instrument(skip_all)]
    pub(crate) async fn tick(
        &self,
        executed_at: DateTime<Utc>,
    ) -> Result<TickProgress, DbConnectionError> {
        let mut expired_locks = 0;
        let mut expired_async_timers = 0;
        for expired_timer in self.db_connection.get_expired_timers(executed_at).await? {
            match expired_timer {
                ExpiredTimer::Lock {
                    execution_id,
                    version,
                    already_retried_count,
                    max_retries,
                    retry_exp_backoff,
                } => {
                    let event = if already_retried_count < max_retries {
                        let duration =
                            retry_exp_backoff * 2_u32.saturating_pow(already_retried_count);
                        let expires_at = executed_at + duration;
                        debug!(%execution_id, "Retrying execution with expired lock after {duration:?} at {expires_at}");
                        ExecutionEventInner::IntermittentTimeout { expires_at }
                    } else {
                        info!(%execution_id, "Marking execution with expired lock as permanently timed out");
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::PermanentTimeout),
                        }
                    };
                    let res = self
                        .db_connection
                        .append(
                            execution_id,
                            Some(version),
                            AppendRequest {
                                created_at: executed_at,
                                event,
                            },
                        )
                        .await;
                    if let Err(err) = res {
                        debug!(%execution_id, "Failed to update expired lock - {err:?}");
                    } else {
                        expired_locks += 1;
                    }
                }
                ExpiredTimer::AsyncTimer {
                    execution_id,
                    version,
                    join_set_id,
                    delay_id,
                } => {
                    let event = ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::AsyncResponse {
                            join_set_id,
                            response: AsyncResponse::DelayFinishedAsyncResponse { delay_id },
                        },
                    };
                    debug!(%execution_id, %join_set_id, %delay_id, "Appending DelayFinishedAsyncResponse");
                    let res = self
                        .db_connection
                        .append(
                            execution_id,
                            Some(version),
                            AppendRequest {
                                created_at: executed_at,
                                event,
                            },
                        )
                        .await;
                    if let Err(err) = res {
                        debug!(%execution_id, %join_set_id, %delay_id, "Failed to update expired async timer - {err:?}");
                    } else {
                        expired_async_timers += 1;
                    }
                }
            }
        }
        Ok(TickProgress {
            expired_locks,
            expired_async_timers,
        })
    }
}
