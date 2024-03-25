use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::ExecutionId;
use concepts::{prefixed_ulid::JoinSetId, FunctionFqn, ParamsParsingError, ResultParsingError};
use concepts::{Params, SupportedFunctionResult};
use db::storage::HistoryEvent;
use db::storage::Version;
use std::borrow::Cow;
use std::error::Error;

pub mod executor;

pub mod worker {

    use super::*;

    pub type WorkerResult = Result<(SupportedFunctionResult, Version), (WorkerError, Version)>;

    #[derive(Debug, thiserror::Error)]
    pub enum WorkerError {
        #[error("intermittent error: `{reason}`, {err:?}")]
        IntermittentError {
            reason: Cow<'static, str>,
            err: Box<dyn Error + Send + Sync>,
        },
        #[error("Limit reached: {0}")]
        LimitReached(String),
        #[error("intermittent timeout")]
        IntermittentTimeout,
        #[error(transparent)]
        FatalError(#[from] FatalError),
        #[error("interrupt")]
        Interrupt(ChildExecutionRequest),
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct ChildExecutionRequest {
        pub new_join_set_id: JoinSetId,
        pub child_execution_id: ExecutionId,
        pub ffqn: FunctionFqn,
        pub params: Params,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum FatalError {
        #[error("non-determinism detected: `{0}`")]
        NonDeterminismDetected(Cow<'static, str>),
        #[error("not found")]
        NotFound,
        #[error(transparent)]
        ParamsParsingError(ParamsParsingError),
        #[error(transparent)]
        ResultParsingError(ResultParsingError),
    }

    #[async_trait]
    pub trait Worker: Clone + valuable::Valuable + Send + Sync + 'static {
        async fn run(
            &self,
            execution_id: ExecutionId,
            ffqn: FunctionFqn,
            params: Params,
            event_history: Vec<HistoryEvent>,
            version: Version,
            execution_deadline: DateTime<Utc>,
        ) -> WorkerResult;
    }
}
