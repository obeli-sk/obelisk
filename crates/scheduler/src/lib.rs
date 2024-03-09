use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::ExecutionId;
use concepts::{Params, SupportedFunctionResult};
use std::borrow::Cow;

pub mod executor;
pub mod storage;

pub mod worker {
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
    pub trait Worker<ID: ExecutionId>: Clone + valuable::Valuable {
        async fn run(
            &self,
            execution_id: ID,
            ffqn: FunctionFqn,
            params: Params,
            event_history: Vec<HistoryEvent<ID>>,
            version: Version,
            execution_deadline: DateTime<Utc>,
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
