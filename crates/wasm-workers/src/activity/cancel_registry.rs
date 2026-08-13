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
use tokio::sync::{oneshot, watch};
use tracing::{Instrument, debug, info, info_span};

#[derive(Clone)]
/// All currently running activities and workflows in this process.
/// Activity worker tasks register themselves and listen on a cancellation token;
/// cancel RPCs and workflow workers call `cancel_activity`, which writes durable
/// cancellation intent to db (whether registered or not) and triggers the token.
/// Workflow worker tasks register a per-execution interrupt watch (pruned by `tick`
/// once the run drops its receiver); the pause and cancel RPCs call
/// `signal_workflow_interrupt` after their durable write. The write is what takes
/// effect (it dooms the run's next db append); the signal only stops the run early so
/// it stops burning CPU, returning `DbUpdatedByWorkerOrWatcher` without appending.
pub struct CancelRegistry {
    activity_cancellation_tokens: Arc<Mutex<hashbrown::HashMap<ExecutionId, ActivityInfo>>>,
    running_workflows: Arc<Mutex<hashbrown::HashMap<ExecutionId, watch::Sender<bool>>>>,
}

struct ActivityInfo {
    cancellation_sender: oneshot::Sender<()>,
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
            activity_cancellation_tokens: Arc::default(),
            running_workflows: Arc::default(),
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
        self.activity_cancellation_tokens
            .lock()
            .unwrap()
            .retain(|_exe, info| !info.cancellation_sender.is_closed());
        // A workflow's receiver lives in its deadline tracker for the run's
        // lifetime, so `is_closed` prunes the entry once the run ends/traps.
        self.running_workflows
            .lock()
            .unwrap()
            .retain(|_exe, sender| !sender.is_closed());
    }

    pub(crate) fn activity_obtain_cancellation_token(
        &self,
        execution_id: ExecutionId,
    ) -> oneshot::Receiver<()> {
        let mut guard = self.activity_cancellation_tokens.lock().unwrap();
        let (cancellation_sender, receiver) = oneshot::channel();
        guard.insert(execution_id, ActivityInfo { cancellation_sender });
        receiver
    }

    /// Register a locked workflow run so `signal_workflow_interrupt` can interrupt it
    /// same-node. The returned receiver is threaded into the deadline tracker; the
    /// entry is pruned by `tick` once the run drops it (completion, trap, or panic).
    #[must_use]
    pub fn register_running_workflow(&self, execution_id: ExecutionId) -> watch::Receiver<bool> {
        let (sender, receiver) = watch::channel(false);
        self.running_workflows
            .lock()
            .unwrap()
            .insert(execution_id, sender);
        receiver
    }

    /// Best-effort interrupt of a locally-running workflow after its durable pause or
    /// cancel write. A no-op if the workflow is not running in this process (another
    /// node, or not currently locked).
    pub fn signal_workflow_interrupt(&self, execution_id: &ExecutionId) {
        if let Some(sender) = self.running_workflows.lock().unwrap().get(execution_id) {
            let _ = sender.send(true);
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
        if outcome == CancelOutcome::CancelRequested {
            // Sending the signal is best effort, the activity might not be registered yet.
            let info = {
                let mut guard = self.activity_cancellation_tokens.lock().unwrap();
                guard.remove(execution_id)
            };
            if let Some(info) = info {
                let _ = info.cancellation_sender.send(());
            }
        }
        Ok(outcome)
    }
}
