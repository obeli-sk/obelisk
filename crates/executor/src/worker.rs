use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::RunId;
use concepts::storage::HistoryEvent;
use concepts::storage::Version;
use concepts::ExecutionId;
use concepts::ExecutionMetadata;
use concepts::FunctionMetadata;
use concepts::{
    storage::{DbError, JoinSetResponseEvent},
    FinishedExecutionError, StrVariant,
};
use concepts::{FunctionFqn, ParamsParsingError, ResultParsingError};
use concepts::{Params, SupportedFunctionReturnValue};
use tracing::Span;

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
    DbUpdatedByWorker,
    Err(WorkerError),
}

#[derive(Debug)]
pub struct WorkerContext {
    pub execution_id: ExecutionId,
    pub run_id: RunId,
    pub metadata: ExecutionMetadata,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub event_history: Vec<HistoryEvent>,
    pub responses: Vec<JoinSetResponseEvent>,
    pub version: Version,
    pub execution_deadline: DateTime<Utc>,
    pub can_be_retried: bool,
    pub worker_span: Span,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    // retriable errors
    #[error("temporary error: {reason}")]
    TemporaryError {
        reason: StrVariant,
        detail: Option<String>,
        version: Version,
    },
    #[error("Limit reached: {0}")]
    LimitReached(String, Version),
    #[error("temporary timeout")]
    TemporaryTimeout,
    #[error(transparent)]
    DbError(DbError),
    // non-retriable errors
    #[error("fatal error: {0}")]
    FatalError(FatalError, Version),
}

#[derive(Debug, thiserror::Error)]
pub enum FatalError {
    #[error("nondeterminism detected: `{0}`")]
    NondeterminismDetected(StrVariant),
    #[error("parameters cannot be parsed: {0}")]
    ParamsParsingError(ParamsParsingError),
    #[error("result cannot be parsed: {0}")]
    ResultParsingError(ResultParsingError),
    #[error("child finished with an execution error: {0}")]
    ChildExecutionError(FinishedExecutionError),
    #[error("uncategorized error: {reason} - {detail}")]
    UncategorizedError { reason: String, detail: String },
}
