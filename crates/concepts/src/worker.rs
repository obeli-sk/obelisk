use crate::storage::HistoryEvent;
use crate::storage::Version;
use crate::ExecutionId;
use crate::{prefixed_ulid::JoinSetId, FunctionFqn, ParamsParsingError, ResultParsingError};
use crate::{Params, SupportedFunctionResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::error::Error;

use crate::{prefixed_ulid::DelayId, StrVariant};

pub type WorkerResult = Result<(SupportedFunctionResult, Version), (WorkerError, Version)>;

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
    IntermittentTimeout { epoch_based: bool },
    #[error(transparent)]
    FatalError(#[from] FatalError),
    #[error("child execution request")]
    ChildExecutionRequest(ChildExecutionRequest),
    #[error("sleep request")]
    SleepRequest(SleepRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildExecutionRequest {
    pub new_join_set_id: JoinSetId,
    pub child_execution_id: ExecutionId,
    pub ffqn: FunctionFqn,
    pub params: Params,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SleepRequest {
    pub new_join_set_id: JoinSetId,
    pub delay_id: DelayId,
    pub expires_at: DateTime<Utc>,
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

#[async_trait]
pub trait Worker: Clone + valuable::Valuable + Send + Sync + 'static {
    // FIXME: Remove Clone
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
