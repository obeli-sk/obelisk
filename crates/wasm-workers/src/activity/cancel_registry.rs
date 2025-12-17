use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::ExecutionId,
    storage::{CancelOutcome, DbConnection, DbErrorWrite, DbPool},
};
use executor::AbortOnDropHandle;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tracing::info;

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
        info!("Spawning cancel_watcher");
        let clone = self.clone();
        AbortOnDropHandle::new(
            tokio::spawn({
                async move {
                    loop {
                        clone.tick(db_pool.connection().as_ref()).await;
                        tokio::time::sleep(sleep_duration).await;
                    }
                }
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
            if let Ok(pending_state) = db_connection.get_pending_state(&execution_id).await
                && pending_state.is_finished()
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
