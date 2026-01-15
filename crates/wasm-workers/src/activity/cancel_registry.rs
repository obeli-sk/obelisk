use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::ExecutionId,
    storage::{CancelOutcome, DbConnection, DbErrorGeneric, DbErrorWrite, DbPool},
};
use executor::AbortOnDropHandle;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tracing::{Instrument, debug, info_span, warn};

pub const CANCEL_RETRIES: u8 = 5;

#[derive(Clone)]
/// All currently running activities in this process.
/// Activity worker tasks register themselves and listen on cancellation token.
/// RPCs can trigger `cancel` which writes the new state to db and triggers the cancellation token.
pub struct CancelRegistry {
    tokens: Arc<Mutex<hashbrown::HashMap<ExecutionId, oneshot::Sender<()>>>>,
}

impl Default for CancelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelRegistry {
    #[must_use]
    pub fn new() -> CancelRegistry {
        CancelRegistry {
            tokens: Arc::default(),
        }
    }

    pub fn spawn_cancel_watcher(
        &self,
        db_pool: Arc<dyn DbPool>,
        sleep_duration: Duration,
    ) -> AbortOnDropHandle {
        let clone = self.clone();
        AbortOnDropHandle::new(
            tokio::spawn({
                async move {
                    debug!("Spawned the cancel watcher");
                    let mut old_err = None;
                    loop {
                        let res = db_pool.connection().await;
                        let res = match res {
                            Ok(conn) => {
                                clone.tick(conn.as_ref()).await;
                                Ok(())
                            }
                            Err(err) => Err(err),
                        };
                        log_err_if_new(res, &mut old_err);
                        tokio::time::sleep(sleep_duration).await;
                    }
                }
                .instrument(info_span!(parent: None, "cancel_watcher"))
            })
            .abort_handle(),
        )
    }

    async fn tick(&self, db_connection: &dyn DbConnection) {
        let execution_ids: Vec<_> = {
            let guard = self.tokens.lock().unwrap();
            guard.keys().cloned().collect()
        };
        let mut finished = Vec::new();
        for execution_id in execution_ids {
            if let Ok(execution_with_state) = db_connection.get_pending_state(&execution_id).await
                && execution_with_state.pending_state.is_finished()
            {
                finished.push(execution_id);
            }
        }
        if !finished.is_empty() {
            let mut guard = self.tokens.lock().unwrap();
            for execution_id in finished {
                if let Some(sender) = guard.remove(&execution_id) {
                    let _ = sender.send(());
                }
            }
        }
    }

    pub(crate) fn obtain_cancellation_token(
        &self,
        execution_id: ExecutionId,
    ) -> oneshot::Receiver<()> {
        // Cleanup old entries.
        let mut guard = self.tokens.lock().unwrap();
        guard.retain(|_key, sender| !sender.is_closed());

        let (sender, receiver) = oneshot::channel();
        guard.insert(execution_id, sender);
        receiver
    }

    /// It is the responsibility of the caller to check that the execution belongs to an activity!
    pub async fn cancel(
        &self,
        db_connection: &dyn DbConnection,
        execution_id: &ExecutionId,
        cancelled_at: DateTime<Utc>,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        // Sending the signal is best effort, the activity might be locked but the token might not be obtained yet.
        let sender = {
            let mut guard = self.tokens.lock().unwrap();
            guard.remove(execution_id)
        };
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
        db_connection
            .cancel_activity_with_retries(execution_id, cancelled_at)
            .await
    }
}

fn log_err_if_new(res: Result<(), DbErrorGeneric>, old_err: &mut Option<DbErrorGeneric>) {
    match (res, &old_err) {
        (Ok(()), _) => {
            *old_err = None;
        }
        (Err(err), Some(old)) if err == *old => {}
        (Err(err), _) => {
            warn!("Tick failed: {err:?}");
            *old_err = Some(err);
        }
    }
}
