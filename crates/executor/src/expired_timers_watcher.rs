use crate::executor::Append;
use crate::executor::ChildFinishedResponse;
use chrono::{DateTime, Utc};
use concepts::storage::AppendRequest;
use concepts::storage::DbConnection;
use concepts::storage::DbError;
use concepts::storage::DbPool;
use concepts::storage::ExecutionLog;
use concepts::storage::JoinSetResponseEvent;
use concepts::{
    storage::{ExecutionEventInner, ExpiredTimer, JoinSetResponse},
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
use tracing::Level;
use tracing::{debug, error, info, instrument, trace, warn};
use utils::time::ClockFn;

#[derive(Debug, Clone)]
pub struct TimersWatcherConfig<C: ClockFn> {
    pub tick_sleep: Duration,
    pub clock_fn: C,
    pub leeway: Duration,
}

#[expect(dead_code)]
#[derive(Debug)]
pub(crate) struct TickProgress {
    pub(crate) expired_locks: usize,
    pub(crate) expired_async_timers: usize,
}

pub struct TaskHandle {
    is_closing: Arc<AtomicBool>,
    abort_handle: AbortHandle,
}

impl TaskHandle {
    #[instrument(level = Level::DEBUG, skip_all, name = "expired_timers_watcher.close")]
    pub async fn close(&self) {
        trace!("Gracefully closing");
        self.is_closing.store(true, Ordering::Relaxed);
        while !self.abort_handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        debug!("Gracefully closed expired_timers_watcher");
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!("Aborting the expired_timers_watcher");
        self.abort_handle.abort();
    }
}

pub fn spawn_new<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    db_pool: P,
    config: TimersWatcherConfig<C>,
) -> TaskHandle {
    let is_closing = Arc::new(AtomicBool::default());
    let tick_sleep = config.tick_sleep;
    let abort_handle = tokio::spawn({
        let is_closing = is_closing.clone();
        async move {
            debug!("Spawned expired_timers_watcher");
            let mut old_err = None;
            while !is_closing.load(Ordering::Relaxed) {
                let executed_at = config.clock_fn.now() - config.leeway;
                let res = tick(db_pool.connection(), executed_at).await;
                log_err_if_new(res, &mut old_err);
                tokio::time::sleep(tick_sleep).await;
            }
        }
    })
    .abort_handle();
    TaskHandle {
        is_closing,
        abort_handle,
    }
}

fn log_err_if_new(res: Result<TickProgress, DbError>, old_err: &mut Option<DbError>) {
    match (res, &old_err) {
        (Ok(_), _) => {
            *old_err = None;
        }
        (Err(err), Some(old)) if err == *old => {}
        (Err(err), _) => {
            error!("Tick failed: {err:?}");
            *old_err = Some(err);
        }
    }
}

#[instrument(level = Level::TRACE, skip_all)]
pub(crate) async fn tick<DB: DbConnection + 'static>(
    db_connection: DB,
    executed_at: DateTime<Utc>,
) -> Result<TickProgress, DbError> {
    let mut expired_locks = 0;
    let mut expired_async_timers = 0;
    for expired_timer in db_connection.get_expired_timers(executed_at).await? {
        match expired_timer {
            ExpiredTimer::Lock {
                execution_id,
                version,
                temporary_event_count,
                max_retries,
                retry_exp_backoff,
                parent,
            } => {
                let append = if let Some(duration) = ExecutionLog::can_be_retried_after(
                    temporary_event_count + 1,
                    max_retries,
                    retry_exp_backoff,
                ) {
                    let backoff_expires_at = executed_at + duration;
                    debug!(%execution_id, "Retrying execution with expired lock after {duration:?} at {backoff_expires_at}");
                    Append {
                        created_at: executed_at,
                        primary_event: AppendRequest {
                            created_at: executed_at,
                            event: ExecutionEventInner::TemporarilyTimedOut { backoff_expires_at },
                        },
                        execution_id: execution_id.clone(),
                        version,
                        child_finished: None,
                    }
                } else {
                    info!(%execution_id, "Marking execution with expired lock as permanently timed out");
                    let finished_exec_result = Err(FinishedExecutionError::PermanentTimeout);
                    let child_finished = parent.map(|(parent_execution_id, parent_join_set)| {
                        ChildFinishedResponse {
                            parent_execution_id,
                            parent_join_set,
                            result: finished_exec_result.clone(),
                        }
                    });
                    Append {
                        created_at: executed_at,
                        primary_event: AppendRequest {
                            created_at: executed_at,
                            event: ExecutionEventInner::Finished {
                                result: finished_exec_result,
                            },
                        },
                        execution_id: execution_id.clone(),
                        version,
                        child_finished,
                    }
                };
                let res = append.append(&db_connection).await;
                if let Err(err) = res {
                    debug!(%execution_id, "Failed to update expired lock - {err:?}");
                } else {
                    expired_locks += 1;
                }
            }
            ExpiredTimer::AsyncDelay {
                execution_id,
                join_set_id,
                delay_id,
            } => {
                let event = JoinSetResponse::DelayFinished { delay_id };
                debug!(%execution_id, %join_set_id, %delay_id, "Appending DelayFinishedAsyncResponse");
                let res = db_connection
                    .append_response(
                        executed_at,
                        execution_id.clone(),
                        JoinSetResponseEvent { join_set_id, event },
                    )
                    .await;
                if let Err(err) = res {
                    debug!(%execution_id, %delay_id, "Failed to update expired async timer - {err:?}");
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
