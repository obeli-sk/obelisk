use crate::AbortOnDropHandle;
use crate::executor::Append;
use crate::executor::ChildFinishedResponse;
use chrono::{DateTime, Utc};
use concepts::StrVariant;
use concepts::storage::AppendRequest;
use concepts::storage::DbConnection;
use concepts::storage::DbError;
use concepts::storage::DbPool;
use concepts::storage::ExecutionLog;
use concepts::storage::JoinSetResponseEvent;
use concepts::time::ClockFn;
use concepts::{
    FinishedExecutionError,
    storage::{ExecutionEventInner, ExpiredTimer, JoinSetResponse},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tracing::Level;
use tracing::warn;
use tracing::{debug, info, instrument};

#[derive(Debug, Clone)]
pub struct TimersWatcherConfig<C: ClockFn> {
    pub tick_sleep: Duration,
    pub clock_fn: C,
    pub leeway: Duration, // A short duration that will be subtracted from now() so that a hot workflow can win.
}

#[derive(Debug, PartialEq)]
pub struct TickProgress {
    pub expired_locks: usize,
    pub expired_async_timers: usize,
}

pub fn spawn_new<C: ClockFn + 'static>(
    db_pool: Arc<dyn DbPool>,
    config: TimersWatcherConfig<C>,
) -> AbortOnDropHandle {
    info!("Spawning expired_timers_watcher");
    let is_closing = Arc::new(AtomicBool::default());
    let tick_sleep = config.tick_sleep;
    AbortOnDropHandle::new(
        tokio::spawn({
            let is_closing = is_closing.clone();
            async move {
                let mut old_err = None;
                while !is_closing.load(Ordering::Relaxed) {
                    let executed_at = config.clock_fn.now() - config.leeway;
                    let res = tick(db_pool.connection().as_ref(), executed_at).await;
                    log_err_if_new(res, &mut old_err);
                    tokio::time::sleep(tick_sleep).await;
                }
            }
        })
        .abort_handle(),
    )
}

fn log_err_if_new(res: Result<TickProgress, DbError>, old_err: &mut Option<DbError>) {
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

#[cfg(feature = "test")]
pub async fn tick_test(
    db_connection: &dyn DbConnection,
    executed_at: DateTime<Utc>,
) -> Result<TickProgress, DbError> {
    tick(db_connection, executed_at).await
}

#[instrument(level = Level::TRACE, skip_all)]
pub(crate) async fn tick(
    db_connection: &dyn DbConnection,
    executed_at: DateTime<Utc>,
) -> Result<TickProgress, DbError> {
    let mut expired_locks = 0;
    let mut expired_async_timers = 0;
    for expired_timer in db_connection.get_expired_timers(executed_at).await? {
        match expired_timer {
            ExpiredTimer::Lock {
                execution_id,
                locked_at_version,
                version,
                temporary_event_count,
                max_retries,
                retry_exp_backoff,
                parent,
            } => {
                let append = if max_retries == u32::MAX && locked_at_version.0 + 1 < version.0 {
                    // Workflow that made progress is unlocked and immediately available for locking.
                    debug!(%execution_id, "Unlocking workflow execution");
                    Append {
                        created_at: executed_at,
                        primary_event: AppendRequest {
                            created_at: executed_at,
                            event: ExecutionEventInner::Unlocked {
                                backoff_expires_at: executed_at,
                                reason: StrVariant::Static("made progress"),
                            },
                        },
                        execution_id: execution_id.clone(),
                        version,
                        child_finished: None,
                    }
                } else if let Some(duration) = ExecutionLog::can_be_retried_after(
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
                            event: ExecutionEventInner::TemporarilyTimedOut {
                                backoff_expires_at,
                                http_client_traces: None,
                            },
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
                                http_client_traces: None,
                            },
                        },
                        execution_id: execution_id.clone(),
                        version,
                        child_finished,
                    }
                };
                let res = append.append(db_connection).await;
                if let Err(err) = res {
                    debug!(%execution_id, "Failed to update expired lock - {err:?}");
                } else {
                    expired_locks += 1;
                }
            }
            ExpiredTimer::Delay {
                execution_id,
                join_set_id,
                delay_id,
            } => {
                debug!(%execution_id, %join_set_id, %delay_id, "Appending DelayFinishedAsyncResponse");
                let event = JoinSetResponse::DelayFinished { delay_id };
                let res = db_connection
                    .append_response(
                        executed_at,
                        execution_id.clone(),
                        JoinSetResponseEvent { join_set_id, event },
                    )
                    .await;
                if let Err(err) = res {
                    debug!(%execution_id, "Failed to update expired async timer - {err:?}");
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
