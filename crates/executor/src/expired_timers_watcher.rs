use crate::executor::Append;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ExecutorId;
use concepts::storage::DbConnection;
use concepts::storage::DbError;
use concepts::storage::JoinSetResponseEvent;
use concepts::FinishedExecutionResult;
use concepts::SupportedFunctionReturnValue;
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
use tracing::Span;
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument};
use utils::time::ClockFn;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::WastVal;
use val_json::wast_val::WastValWithType;

#[derive(Debug, Clone)]
pub struct TimersWatcherConfig<C: ClockFn> {
    pub tick_sleep: Duration,
    pub clock_fn: C,
}

pub struct TimersWatcherTask<DB: DbConnection> {
    pub(crate) db_connection: DB, // FIXME: DbPool
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct TickProgress {
    pub(crate) expired_locks: usize,
    pub(crate) expired_async_timers: usize,
}

pub struct TaskHandle {
    is_closing: Arc<AtomicBool>,
    abort_handle: AbortHandle,
    span: Span,
}

impl TaskHandle {
    #[instrument(skip_all, parent = &self.span)]
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
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        self.span.in_scope(|| {
            warn!("Aborting the task");
            self.abort_handle.abort();
        });
    }
}

impl<DB: DbConnection + 'static> TimersWatcherTask<DB> {
    pub fn spawn_new<C: ClockFn + 'static>(
        db_connection: DB,
        config: TimersWatcherConfig<C>,
    ) -> TaskHandle {
        let executor_id = ExecutorId::generate();
        let span = info_span!(parent: None, "expired_timers_watcher",
            executor = %executor_id,
        );
        let is_closing = Arc::new(AtomicBool::default());
        let is_closing_inner = is_closing.clone();
        let tick_sleep = config.tick_sleep;
        let abort_handle = tokio::spawn(
            async move {
                info!("Spawned");
                let task = Self { db_connection };
                let mut old_err = None;
                loop {
                    let executed_at = (config.clock_fn)();
                    let res = task.tick(executed_at).await;
                    Self::log_err_if_new(res, &mut old_err);
                    if is_closing_inner.load(Ordering::Relaxed) {
                        return;
                    }
                    tokio::time::sleep(tick_sleep).await;
                }
            }
            .instrument(span.clone()),
        )
        .abort_handle();
        TaskHandle {
            is_closing,
            abort_handle,
            span,
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

    #[instrument(level = Level::DEBUG, skip_all)]
    pub(crate) async fn tick(&self, executed_at: DateTime<Utc>) -> Result<TickProgress, DbError> {
        let mut expired_locks = 0;
        let mut expired_async_timers = 0;
        for expired_timer in self.db_connection.get_expired_timers(executed_at).await? {
            match expired_timer {
                ExpiredTimer::Lock {
                    execution_id,
                    version,
                    intermittent_event_count,
                    max_retries,
                    retry_exp_backoff,
                    parent,
                    return_type,
                } => {
                    let append = if intermittent_event_count < max_retries {
                        let duration =
                            retry_exp_backoff * 2_u32.saturating_pow(intermittent_event_count);
                        let expires_at = executed_at + duration;
                        debug!(%execution_id, "Retrying execution with expired lock after {duration:?} at {expires_at}");
                        Append {
                            created_at: executed_at,
                            primary_event: ExecutionEventInner::IntermittentTimeout { expires_at }, // not converting for clarity
                            execution_id,
                            version,
                            parent: None,
                        }
                    } else {
                        info!(%execution_id, "Marking execution with expired lock as permanently timed out");
                        // Try to convert to SupportedFunctionResult::Fallible
                        let finished_exec_result = convert_permanent_timeout(return_type);
                        let parent = parent.map(|(p, j)| (p, j, finished_exec_result.clone()));
                        Append {
                            created_at: executed_at,
                            primary_event: ExecutionEventInner::Finished {
                                result: finished_exec_result,
                            },
                            execution_id,
                            version,
                            parent,
                        }
                    };
                    let res = append.append(&self.db_connection).await;
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
                    let res = self
                        .db_connection
                        .append_response(
                            executed_at,
                            execution_id,
                            JoinSetResponseEvent { join_set_id, event },
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

fn convert_permanent_timeout(return_type: Option<TypeWrapper>) -> FinishedExecutionResult {
    match return_type {
        Some(return_type @ TypeWrapper::Result { ok: _, err: None }) => {
            Ok(SupportedFunctionReturnValue::Fallible(WastValWithType {
                r#type: return_type,
                value: WastVal::Result(Err(None)),
            }))
        }
        Some(TypeWrapper::Result {
            ok,
            err: Some(err_type),
        }) if matches!(err_type.as_ref(), TypeWrapper::String) => {
            Ok(SupportedFunctionReturnValue::Fallible(WastValWithType {
                r#type: TypeWrapper::Result {
                    ok,
                    err: Some(err_type),
                },
                value: WastVal::Result(Err(Some(WastVal::String("timeout".to_string()).into()))),
            }))
        }
        _ => Err(FinishedExecutionError::PermanentTimeout),
    }
}
