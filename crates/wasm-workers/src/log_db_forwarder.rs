use concepts::storage::{DbConnection, DbErrorWrite, DbPool, LogInfoAppendRow};
use executor::AbortOnDropHandle;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::{Instrument, Level, debug, info_span, instrument, trace, warn};

const RECV_MANY_LIMIT: usize = 500; // max items per tx
const DB_ERR_DELAY_MS: u64 = 100;

pub fn spawn_new(
    db_pool: Arc<dyn DbPool>,
    mut receiver: mpsc::Receiver<LogInfoAppendRow>,
) -> AbortOnDropHandle {
    AbortOnDropHandle::new(
        tokio::spawn({
            async move {
                debug!("Spawned log db forwarder");
                let mut old_err = None;
                let mut buffer = Vec::with_capacity(RECV_MANY_LIMIT);
                loop {
                    let res = db_pool.connection().await;
                    let res = match res {
                        Ok(conn) => tick(conn.as_ref(), &mut receiver, &mut buffer).await,
                        Err(err) => Err(TickError::DbErrorWrite(err.into())),
                    };
                    let res = match res {
                        Ok(ok) => Ok(ok),
                        Err(TickError::Closed) => {
                            debug!("Channel closed");
                            return;
                        }
                        Err(TickError::DbErrorWrite(err)) => Err(err),
                    };
                    log_err_if_new(res, &mut old_err);
                    if old_err.is_some() {
                        tokio::time::sleep(Duration::from_millis(DB_ERR_DELAY_MS)).await;
                    }
                }
            }
            .instrument(info_span!(parent: None, "log_db_forwarder"))
        })
        .abort_handle(),
    )
}

#[derive(Debug, PartialEq)]
pub struct TickProgress {
    forwarded: usize,
}

#[derive(Clone, Debug, thiserror::Error)]
enum TickError {
    #[error("closed")]
    Closed,
    #[error(transparent)]
    DbErrorWrite(#[from] DbErrorWrite),
}

#[instrument(level = Level::TRACE, skip_all)]
async fn tick(
    db_connection: &dyn DbConnection,
    receiver: &mut mpsc::Receiver<LogInfoAppendRow>,
    buffer: &mut Vec<LogInfoAppendRow>,
) -> Result<TickProgress, TickError> {
    let forwarded = receiver.recv_many(buffer, RECV_MANY_LIMIT).await;
    if forwarded > 0 {
        let stopwatch = Instant::now();
        db_connection.append_log_batch(buffer).await?;
        buffer.clear();
        let stopwatch = stopwatch.elapsed();
        trace!("Appended {forwarded} log rows in {stopwatch:?}");
        Ok(TickProgress { forwarded })
    } else {
        Err(TickError::Closed)
    }
}

fn log_err_if_new(res: Result<TickProgress, DbErrorWrite>, old_err: &mut Option<DbErrorWrite>) {
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
