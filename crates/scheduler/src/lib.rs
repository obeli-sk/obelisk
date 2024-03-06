use std::borrow::Cow;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::ExecutionId;
use concepts::{Params, SupportedFunctionResult};

mod executor;
mod storage;

#[cfg(test)]
mod testing;

mod worker {
    use std::{borrow::Cow, error::Error};

    use self::storage::{HistoryEvent, Version};
    use super::*;
    use concepts::FunctionFqn;

    pub type WorkerResult = Result<(SupportedFunctionResult, Version), (WorkerError, Version)>;

    #[derive(Debug, thiserror::Error)]
    pub enum WorkerError {
        #[error("intermittent error: `{reason}`, {err:?}")]
        IntermittentError {
            reason: Cow<'static, str>,
            err: Box<dyn Error + Send>,
        },
        // #[error("intermittent timeout")]
        // IntermittentTimeout,
        #[error(transparent)]
        FatalError(#[from] FatalError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum FatalError {
        #[error("non-determinism detected: `{0}`")]
        NonDeterminismDetected(Cow<'static, str>),
    }

    #[async_trait]
    pub trait Worker<ID: ExecutionId> {
        async fn run(
            &mut self,
            execution_id: ID,
            ffqn: FunctionFqn,
            params: Params,
            event_history: Vec<HistoryEvent<ID>>,
            version: Version,
            lock_expires_at: DateTime<Utc>,
        ) -> WorkerResult;
    }
}

type FinishedExecutionResult<ID> = Result<SupportedFunctionResult, FinishedExecutionError<ID>>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum FinishedExecutionError<ID: ExecutionId> {
    #[error("permanent timeout")]
    PermanentTimeout,
    // TODO PermanentFailure when error retries are implemented
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(Cow<'static, str>),
    #[error("uncategorized error: `{0}`")]
    UncategorizedError(Cow<'static, str>), // intermittent failure that is not retried
    #[error("cancelled, reason: `{0}`")]
    Cancelled(Cow<'static, str>),
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
