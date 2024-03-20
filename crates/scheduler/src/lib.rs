use crate::storage::ExecutionEventInner;
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::ExecutionId;
use concepts::{Params, SupportedFunctionResult};
use std::borrow::Cow;
use std::time::Duration;
use storage::journal::PendingState;
use storage::{ExecutionEvent, ExecutionIdStr, Version};

pub mod executor;
pub mod storage;

pub mod worker {
    use std::{borrow::Cow, error::Error};

    use self::storage::{HistoryEvent, Version};
    use super::*;
    use concepts::{
        prefixed_ulid::{ActivityId, JoinSetId},
        FunctionFqn, ParamsParsingError, ResultParsingError,
    };

    pub type WorkerResult = Result<(SupportedFunctionResult, Version), (WorkerError, Version)>;

    #[derive(Debug, thiserror::Error)]
    pub enum WorkerError {
        #[error("intermittent error: `{reason}`, {err:?}")]
        IntermittentError {
            reason: Cow<'static, str>,
            err: Box<dyn Error + Send>,
        },
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
        pub child_execution_id: ActivityId, // TODO: unify with WorkflowId as ExecutionId
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
    pub trait Worker<ID: ExecutionId>: Clone + valuable::Valuable + Send + Sync + 'static {
        async fn run(
            &self,
            execution_id: ID,
            ffqn: FunctionFqn,
            params: Params,
            event_history: Vec<HistoryEvent>,
            version: Version,
            execution_deadline: DateTime<Utc>,
        ) -> WorkerResult;
    }
}

#[derive(Debug)]
pub struct ExecutionHistory {
    execution_events: Vec<ExecutionEvent>,
    version: Version,
    pending_state: PendingState,
}

impl ExecutionHistory {
    pub fn can_be_retried_after(&self) -> Option<Duration> {
        can_be_retried_after(
            self.execution_events.iter(),
            self.max_retries(),
            self.retry_exp_backoff(),
        )
    }

    pub fn retry_exp_backoff(&self) -> Duration {
        assert_matches!(self.execution_events.get(0), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { retry_exp_backoff, .. },
            ..
        }) => *retry_exp_backoff)
    }

    pub fn max_retries(&self) -> u32 {
        assert_matches!(self.execution_events.get(0), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { max_retries, .. },
            ..
        }) => *max_retries)
    }

    pub fn params(&self) -> Params {
        assert_matches!(self.execution_events.get(0), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { params, .. },
            ..
        }) => params.clone())
    }

    pub fn last_event(&self) -> &ExecutionEvent {
        self.execution_events
            .last()
            .expect("must contain at least one event")
    }
}

fn can_be_retried_after<'a>(
    iter: impl Iterator<Item = &'a ExecutionEvent>,
    max_retries: u32,
    retry_exp_backoff: Duration,
) -> Option<Duration> {
    let already_retried_count = iter.filter(|event| event.event.is_retry()).count() as u32;
    if already_retried_count < max_retries {
        let duration = retry_exp_backoff * 2_u32.saturating_pow(already_retried_count);
        Some(duration)
    } else {
        None
    }
}

pub type FinishedExecutionResult = Result<SupportedFunctionResult, FinishedExecutionError>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum FinishedExecutionError {
    #[error("permanent timeout")]
    PermanentTimeout,
    // TODO PermanentFailure when error retries are implemented
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(Cow<'static, str>),
    #[error("uncategorized error: `{0}`")]
    PermanentFailure(Cow<'static, str>), // intermittent failure that is not retried
    #[error("cancelled, reason: `{0}`")]
    Cancelled(Cow<'static, str>),
    #[error("continuing as {execution_id}")]
    ContinueAsNew {
        // TODO: Move to the OK part of the result
        execution_id: ExecutionIdStr,
    },
}
