use chrono::{DateTime, Utc};
use concepts::{
    ExecutionFailureKind, FinishedExecutionError, SupportedFunctionReturnValue,
    prefixed_ulid::ExecutionId,
    storage::{
        AppendEventsToExecution, AppendRequest, AppendResponseToExecution, CancelOutcome,
        DbConnection, DbErrorWrite, ExecutionEventInner,
    },
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tracing::debug;

pub const CANCEL_RETRIES: u8 = 5;

#[derive(Clone)]
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
        retries: u8,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        // Sending the signal is best effort, the activity might be locked but the token might not be obtained yet.
        let sender = {
            let mut guard = self.tokens.lock().unwrap();
            guard.remove(execution_id)
        };
        if let Some(sender) = sender
            && let Ok(()) = sender.send(())
        {
            // Give a chance to the executor to write the result
            tokio::time::sleep(Duration::from_millis(100)).await; // TODO: make configurable.
        }
        Self::cancel_activity_with_retries(db_connection, execution_id, cancelled_at, retries).await
    }

    async fn cancel_activity_with_retries(
        db_connection: &dyn DbConnection,
        execution_id: &ExecutionId,
        cancelled_at: DateTime<Utc>,
        mut retries: u8,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        loop {
            let res = Self::cancel_activity(db_connection, execution_id, cancelled_at).await;
            if res.is_ok() || retries == 0 {
                return res;
            }
            retries -= 1;
        }
    }

    async fn cancel_activity(
        db_connection: &dyn DbConnection,
        execution_id: &ExecutionId,
        cancelled_at: DateTime<Utc>,
    ) -> Result<CancelOutcome, DbErrorWrite> {
        debug!("Determining cancellation state of {execution_id}");

        let last_event = db_connection
            .get_last_execution_event(execution_id)
            .await
            .map_err(DbErrorWrite::from)?;
        if let ExecutionEventInner::Finished {
            result:
                SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError {
                    kind: ExecutionFailureKind::Cancelled,
                    ..
                }),
            ..
        } = last_event.event
        {
            return Ok(CancelOutcome::Cancelled);
        } else if matches!(last_event.event, ExecutionEventInner::Finished { .. }) {
            debug!("Not cancelling, {execution_id} is already finished");
            return Ok(CancelOutcome::AlreadyFinished);
        }
        let finished_version = last_event.version.increment();
        let child_result = SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError {
            reason: None,
            kind: ExecutionFailureKind::Cancelled,
            detail: None,
        });
        let cancel_request = AppendRequest {
            created_at: cancelled_at,
            event: ExecutionEventInner::Finished {
                result: child_result.clone(),
                http_client_traces: None,
            },
        };
        debug!("Cancelling activity {execution_id} at {finished_version}");
        if let ExecutionId::Derived(execution_id) = execution_id {
            let (parent_execution_id, join_set_id) = execution_id.split_to_parts();
            let child_execution_id = ExecutionId::Derived(execution_id.clone());
            db_connection
                .append_batch_respond_to_parent(
                    AppendEventsToExecution {
                        execution_id: child_execution_id,
                        version: finished_version.clone(),
                        batch: vec![cancel_request],
                    },
                    AppendResponseToExecution {
                        parent_execution_id,
                        created_at: cancelled_at,
                        join_set_id: join_set_id.clone(),
                        child_execution_id: execution_id.clone(),
                        finished_version,
                        result: child_result,
                    },
                    cancelled_at,
                )
                .await?;
        } else {
            db_connection
                .append(execution_id.clone(), finished_version, cancel_request)
                .await?;
        }
        debug!("Cancelled {execution_id}");
        Ok(CancelOutcome::Cancelled)
    }
}
