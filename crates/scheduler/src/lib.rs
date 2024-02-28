use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::ExecutionId;
use concepts::{Params, SupportedFunctionResult};

mod executor;
mod storage;

#[cfg(test)]
mod testing;

mod worker {
    use self::storage::{DbError, EventHistory, Version};
    use super::*;

    #[async_trait]
    pub trait Worker<ID: ExecutionId> {
        async fn run(
            &self,
            execution_id: ID,
            params: Params,
            event_history: Vec<EventHistory<ID>>,
            version: Version,
            lock_expires_at: DateTime<Utc>,
        ) -> Result<Version, DbError>;
    }
}

type FinishedExecutionResult<ID> = Result<SupportedFunctionResult, FinishedExecutionError<ID>>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum FinishedExecutionError<ID: ExecutionId> {
    #[error("permanent timeout")]
    PermanentTimeout,
    // TODO PermanentFailure when error retries are implemented
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(String),
    #[error("uncategorized error")]
    UncategorizedError, //TODO: add reason?
    #[error("cancelled, reason: `{0}`")]
    Cancelled(String),
    #[error("continuing as {execution_id}")]
    ContinueAsNew {
        // TODO: Move to the OK part of the result
        execution_id: ID,
    },
    #[error("cancelled and starting {execution_id}")]
    CancelledWithNew { execution_id: ID },
}

pub(crate) mod time {
    use chrono::DateTime;
    use chrono::Utc;

    cfg_if::cfg_if! {
        if #[cfg(all(test, madsim))] {
            pub(crate) fn now() -> DateTime<Utc> {
                if madsim::rand::random() {
                    madsim::time::advance(std::time::Duration::from_millis(1));
                }
                DateTime::from(madsim::time::TimeHandle::current().now_time())
            }
            pub(crate) fn now_tokio_instant() -> tokio::time::Instant {
                madsim::time::Instant::now()
            }
        } else {
            pub(crate) fn now() -> DateTime<Utc> {
                Utc::now()
            }
            pub(crate) fn now_tokio_instant() -> tokio::time::Instant {
                tokio::time::Instant::now()
            }
        }
    }
}
