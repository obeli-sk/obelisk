use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::ExecutionId,
    storage::{CancelOutcome, DbConnection, DbErrorWrite},
};
use executor::AbortOnDropHandle;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tracing::{Instrument, debug, info, info_span};

#[derive(Clone)]
/// All currently running activities in this process.
/// Activity worker tasks register themselves and listen on interruption token.
/// Cancel RPCs and workflow workers call `cancel_activity`
/// which writes the new state to db (no matter whether registered or not)
/// and triggers the interruption token.
pub struct CancelRegistry {
    tokens: Arc<Mutex<hashbrown::HashMap<ExecutionId, ActivityInfo>>>,
}

struct ActivityInfo {
    interrupt_sender: oneshot::Sender<()>,
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

    pub fn spawn_cancel_watcher(&self, sleep_duration: Duration) -> AbortOnDropHandle {
        let clone = self.clone();
        AbortOnDropHandle::new(
            tokio::spawn({
                async move {
                    debug!("Spawned the cancel watcher");
                    loop {
                        clone.tick();
                        tokio::time::sleep(sleep_duration).await;
                    }
                }
                .instrument(info_span!(parent: None, "cancel_watcher"))
            })
            .abort_handle(),
        )
    }

    fn tick(&self) {
        let mut guard = self.tokens.lock().unwrap();
        guard.retain(|_exe, info| !info.interrupt_sender.is_closed());
    }

    pub(crate) fn activity_obtain_interrupt_token(
        &self,
        execution_id: ExecutionId,
    ) -> oneshot::Receiver<()> {
        let mut guard = self.tokens.lock().unwrap();
        let (interrupt_sender, receiver) = oneshot::channel();
        guard.insert(execution_id, ActivityInfo { interrupt_sender });
        receiver
    }

    /// Best-effort local interrupt for an activity currently running in this process.
    /// Unlike `cancel_activity`, this does not write terminal cancellation state to the DB.
    /// Noop if the execution is not an activity tracked by this registry.
    fn interrupt_running_activity(&self, execution_id: &ExecutionId) {
        let info = {
            let mut guard = self.tokens.lock().unwrap();
            guard.remove(execution_id)
        };
        if let Some(info) = info {
            let _ = info.interrupt_sender.send(());
        }
    }

    /// It is the responsibility of the caller to check that the execution belongs to an activity!
    pub async fn cancel_activity(
        &self,
        db_connection: &dyn DbConnection,
        execution_id: &ExecutionId,
        cancelled_at: DateTime<Utc>,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        info!(%execution_id, "Cancelling activity");
        let outcome = db_connection
            .cancel_activity_with_retries(execution_id, cancelled_at)
            .await?;
        if outcome == CancelOutcome::Cancelled {
            // Sending the signal is best effort, the activity might not be registered yet.
            self.interrupt_running_activity(execution_id);
        }
        Ok(outcome)
    }
}
