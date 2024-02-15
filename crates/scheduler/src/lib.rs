use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn};
use concepts::{Params, SupportedFunctionResult};
use std::time::Duration;

mod memory;

mod worker {

    use super::*;
    /// Worker commands sent to the worker executor.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum WorkerCommand<E: ExecutionId> {
        DelayFor(Duration),
        ExecuteBlocking {
            ffqn: FunctionFqn,
            params: Params,
            child_execution_id: E,
        },
        PublishResult(SupportedFunctionResult),
    }

    #[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
    pub enum WorkerError {
        #[error("worker timed out")]
        Timeout,
        #[error("non-determinism detected, reason: `{0}`")]
        NonDeterminismDetected(String),
        #[error("worker failed")]
        Uncategorized, // Panic, cancellation
    }

    #[async_trait]
    pub trait Worker<S: WorkerStore<E>, E: ExecutionId> {
        async fn run(
            &self,
            workflow_id: E,
            params: Params,
            store: S,
        ) -> Result<WorkerCommand<E>, WorkerError>;
    }

    #[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
    #[error("non-determinism detected, reason: `{0}`")]
    pub(crate) struct NonDeterminismError(pub(crate) String);

    impl From<NonDeterminismError> for WorkerError {
        fn from(value: NonDeterminismError) -> Self {
            Self::NonDeterminismDetected(value.0)
        }
    }

    #[derive(Debug)]
    pub(crate) enum MaybeReplayResponse<E: ExecutionId> {
        ReplayResponse(ReplayResponse<E>),
        MissingResponse,
    }

    #[derive(Debug)]
    pub(crate) enum ReplayResponse<E: ExecutionId> {
        Completed,
        CompletedWithResult {
            child_execution_id: E,
            result: FinishedExecutionResult,
        },
    }

    pub(crate) trait WorkerStore<E: ExecutionId> {
        fn next_id(&mut self) -> Result<E, NonDeterminismError>;

        fn next_event(
            &mut self,
            command: &WorkerCommand<E>,
        ) -> Result<MaybeReplayResponse<E>, NonDeterminismError>;
    }

    pub(crate) trait WriteableWorkerStore<E: ExecutionId>:
        WorkerStore<E> + Clone + Default + Send + 'static
    {
        fn restart(&mut self);

        fn persist_child_result(&mut self, child_execution_id: E, result: FinishedExecutionResult);

        fn persist_delay_passed(&mut self, duration: Duration);
    }
}

type FinishedExecutionResult = Result<SupportedFunctionResult, FinishedExecutionError>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum FinishedExecutionError {
    #[error("permanent timeout")]
    PermanentTimeout,
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(String),
    #[error("uncategorized error")]
    UncategorizedError,
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum ExecutionStatusInfo {
    Pending,
    Enqueued,
    DelayedUntil(DateTime<Utc>),
    Blocked,
    IntermittentTimeout(DateTime<Utc>),
    Finished(FinishedExecutionResult),
}
impl ExecutionStatusInfo {
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished(_))
    }
}
