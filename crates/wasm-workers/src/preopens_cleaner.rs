use concepts::{
    ExecutionId,
    storage::{DbErrorRead, DbPool, PendingState, PendingStateFinished},
    time::{ClockFn, Sleep},
};
use executor::AbortOnDropHandle;
use std::{error::Error, ffi::OsStr, path::Path, str::FromStr, sync::Arc, time::Duration};
use tracing::{debug, info, trace, warn};

pub struct PreopensCleaner<S: Sleep, C: ClockFn> {
    delete_older_than: Duration,
    parent_preopen_dir: Arc<Path>,
    sleep_duration: Duration,
    sleep: S,
    clock_fn: C,
    db_pool: Arc<dyn DbPool>,
}

impl<S: Sleep + 'static, C: ClockFn + 'static> PreopensCleaner<S, C> {
    pub fn spawn_task(
        delete_older_than: Duration,
        parent_preopen_dir: Arc<Path>,
        sleep_duration: Duration,
        sleep: S,
        clock_fn: C,
        db_pool: Arc<dyn DbPool>,
    ) -> AbortOnDropHandle {
        info!("Spawning preopened dir cleaner");
        let this = PreopensCleaner {
            delete_older_than,
            parent_preopen_dir,
            sleep_duration,
            sleep,
            clock_fn,
            db_pool,
        };
        AbortOnDropHandle::new(
            tokio::task::spawn(async move {
                loop {
                    this.sleep.sleep(this.sleep_duration).await;
                    let res = this.tick().await;
                    if let Err(err) = res {
                        warn!("Cleaning old preopened directories failed - {err:?}");
                    }
                }
            })
            .abort_handle(),
        )
    }

    async fn tick(&self) -> Result<(), Box<dyn Error>> {
        trace!("tick started");
        let mut subfolders = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.parent_preopen_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                subfolders.push(entry.file_name());
            }
        }
        let delete_older_than = self.clock_fn.now() - self.delete_older_than;
        trace!(
            "Scanning {} subfolders for finished before {delete_older_than}",
            subfolders.len()
        );
        for subfolder in subfolders {
            if let Ok(execution_id) = ExecutionId::from_str(&subfolder.to_string_lossy()) {
                match self
                    .db_pool
                    .connection()
                    .get_pending_state(&execution_id)
                    .await
                {
                    Ok(PendingState::Finished {
                        finished: PendingStateFinished { finished_at, .. },
                    }) if finished_at < delete_older_than => {
                        // finished => delete after specified time
                        self.delete(&subfolder, "finished").await;
                    }
                    Err(DbErrorRead::NotFound) => {
                        // not found => delete
                        self.delete(&subfolder, "not found").await;
                    }
                    Ok(_) => {} // execution still running => ignore
                    Err(err) => warn!("Skipping subfolder {execution_id} - database error {err:?}"),
                }
            } else {
                // Unparseable execution id - skip
                warn!("Skipping subfolder {subfolder:?} - is not an execution id");
            }
        }
        trace!("tick finished");
        Ok(())
    }

    async fn delete(&self, subfolder: &OsStr, reason: &'static str) {
        debug!("Deleting {subfolder:?} - {reason}");
        match tokio::fs::remove_dir_all(self.parent_preopen_dir.join(subfolder)).await {
            Ok(()) => debug!("Deleting {subfolder:?} done"),
            Err(err) => warn!("Deleting {subfolder:?} failed - {err:?}"),
        }
    }
}
