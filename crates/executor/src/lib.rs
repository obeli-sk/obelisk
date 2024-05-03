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
        #[error("intermittent error: {reason}")]
        IntermittentError {
            reason: StrVariant,
            err: Option<Box<dyn Error + Send + Sync>>,
            version: Version,
        },
        #[error("Limit reached: {0}")]
        LimitReached(String, Version),
        #[error("intermittent timeout")]
        IntermittentTimeout,
        #[error("fatal error: {0}")]
        FatalError(FatalError, Version),
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

    #[must_use]
    #[derive(Debug)]
    pub enum WorkerResult {
        Ok(SupportedFunctionResult, Version),
        ChildExecutionRequest,
        DelayRequest,
        Err(WorkerError),
    }

    #[derive(Debug)]
    pub struct WorkerContext {
        pub execution_id: ExecutionId,
        pub ffqn: FunctionFqn,
        pub params: Params,
        pub event_history: Vec<HistoryEvent>,
        pub version: Version,
        pub execution_deadline: DateTime<Utc>,
        pub can_be_retried: bool,
    }

    #[async_trait]
    pub trait Worker: valuable::Valuable + Send + Sync + 'static {
        async fn run(&self, ctx: WorkerContext) -> WorkerResult;

        fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn>; // FIXME: Rename to `exported_functions`
    }
}
