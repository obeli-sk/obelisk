use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::storage::HistoryEvent;
use concepts::storage::Version;
use concepts::ExecutionId;
use concepts::FunctionMetadata;
use concepts::{
    storage::{DbError, JoinSetResponseEvent},
    FinishedExecutionError, StrVariant,
};
use concepts::{FunctionFqn, ParamsParsingError, ResultParsingError};
use concepts::{Params, SupportedFunctionReturnValue};
use std::error::Error;

#[async_trait]
pub trait Worker: Send + Sync + 'static {
    async fn run(&self, ctx: WorkerContext) -> WorkerResult;

    fn exported_functions(&self) -> &[FunctionMetadata];

    fn imported_functions(&self) -> &[FunctionMetadata];
}

#[must_use]
#[derive(Debug)]
pub enum WorkerResult {
    Ok(SupportedFunctionReturnValue, Version),
    ChildExecutionRequest,
    DelayRequest,
    Err(WorkerError),
}

#[derive(Debug)]
pub struct WorkerContext {
    pub execution_id: ExecutionId,
    pub correlation_id: StrVariant,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub event_history: Vec<HistoryEvent>,
    pub responses: Vec<JoinSetResponseEvent>,
    pub version: Version,
    pub execution_deadline: DateTime<Utc>,
    pub can_be_retried: bool,
}

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
    ParamsParsingError(ParamsParsingError),
    #[error("result cannot be parsed: {0}")]
    ResultParsingError(ResultParsingError),
    #[error("child finished with an execution error: {0}")]
    ChildExecutionError(FinishedExecutionError),
    #[error("uncategorized error: {0}")]
    UncategorizedError(&'static str),
}
