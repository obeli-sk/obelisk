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
            version: Version,
        },
        #[error("Limit reached: {0}")]
        LimitReached(String, Version),
        #[error("intermittent timeout")]
        IntermittentTimeout,
        #[error("fatal error: {0}")]
        FatalError(FatalError, Version),
        #[error("child execution request")]
        ChildExecutionRequest, // TODO: Move to OK part of `WorkerResult`
        #[error("sleep request")]
        DelayRequest, // TODO: Move to OK part of `WorkerResult`
        #[error(transparent)]
        DbError(DbError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum FatalError {
        #[error("non-determinism detected: `{0}`")]
        NonDeterminismDetected(StrVariant),
        #[error("parameters cannot be parsed: {0}")]
        ParamsParsingError(ParamsParsingError), // FIXME: Add FFQN
        #[error("result cannot be parsed: {0}")]
        ResultParsingError(ResultParsingError), // FIXME: Add FFQN
    }

    pub type WorkerResult = Result<(SupportedFunctionResult, Version), WorkerError>;

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

        fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn>; // FIXME: Rename to `exported_functions`
    }
}
