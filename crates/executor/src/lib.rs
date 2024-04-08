use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::storage::HistoryEvent;
use concepts::storage::Version;
use concepts::ExecutionId;
use concepts::{FunctionFqn, ParamsParsingError, ResultParsingError};
use concepts::{Params, SupportedFunctionResult};
use std::error::Error;

pub mod executor;
pub mod expired_timers_watcher;

pub mod worker {
    use super::{
        async_trait, DateTime, Error, ExecutionId, FunctionFqn, HistoryEvent, Params,
        ParamsParsingError, ResultParsingError, SupportedFunctionResult, Utc, Version,
    };
    use concepts::{storage::DbError, StrVariant};

    #[derive(Debug, thiserror::Error)]
    pub enum WorkerError {
        #[error("intermittent error: {reason} - `{err}`")]
        IntermittentError {
            reason: StrVariant,
            err: Box<dyn Error + Send + Sync>,
        },
        #[error("Limit reached: {0}")]
        LimitReached(String),
        #[error("intermittent timeout")]
        IntermittentTimeout,
        #[error(transparent)]
        FatalError(#[from] FatalError),
        #[error("child execution request")]
        ChildExecutionRequest,
        #[error("sleep request")]
        DelayRequest,
        #[error(transparent)]
        DbError(DbError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum FatalError {
        #[error("non-determinism detected: `{0}`")]
        NonDeterminismDetected(StrVariant),
        #[error(transparent)]
        ParamsParsingError(ParamsParsingError),
        #[error(transparent)]
        ResultParsingError(ResultParsingError),
    }

    pub type WorkerResult = Result<(SupportedFunctionResult, Version), (WorkerError, Version)>;

    // FIXME: Version should not always be returned. Either the worker does not
    // write to db, or takes care of all its writes.
    #[async_trait]
    pub trait Worker: valuable::Valuable + Send + Sync + 'static {
        async fn run(
            &self,
            execution_id: ExecutionId,
            ffqn: FunctionFqn,
            params: Params,
            event_history: Vec<HistoryEvent>,
            version: Version,
            execution_deadline: DateTime<Utc>,
        ) -> WorkerResult;

        fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn>;
    }
}
