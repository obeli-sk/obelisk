use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ExecutorId;
use db::storage::{DbConnection, DbConnectionError};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::AbortHandle;
use tracing::{info, info_span, instrument, trace, warn, Instrument};

#[derive(Debug, Clone)]
pub struct Config<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> {
    pub tick_sleep: Duration,
    pub clock_fn: C,
}

pub struct Task<DB: DbConnection> {
    pub(crate) db_connection: DB,
}

#[derive(Debug)]
pub(crate) struct TickProgress {
    pub(crate) expired_locks: usize,
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
    pub fn spawn_new<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static>(
        db_connection: DB,
        config: Config<C>,
    ) -> TaskHandle {
        let executor_id = ExecutorId::generate();
        let span = info_span!("lock_watcher",
            executor = %executor_id,
        );
        let is_closing: Arc<AtomicBool> = Default::default();
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
            abort_handle,
            is_closing,
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
        Ok(TickProgress {
            expired_locks: self
                .db_connection
                .cleanup_expired_locks(executed_at)
                .await?,
        })
    }
}
