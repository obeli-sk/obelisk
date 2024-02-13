use std::{fmt::Display, hash::Hash};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{Params, SupportedFunctionResult};

mod memory;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCommand {
    PublishResult(SupportedFunctionResult),
    EnqueueNow,
    DelayUntil(DateTime<Utc>),
}

pub type WorkerExecutionResult = Result<WorkerCommand, WorkerError>;
pub type RunId = ulid::Ulid; // TODO

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum WorkerError {
    #[error("worker timed out")]
    Timeout,
    #[error("worker failed")]
    Uncategorized,
}

#[async_trait]
pub trait Worker<S, E: ExecutionId> {
    async fn run(&self, workflow_id: E, params: Params, store: S) -> WorkerExecutionResult;
}

pub trait ExecutionId: Clone + Hash + Display + Eq + PartialEq + Send + 'static {}

impl<T> ExecutionId for T where T: Clone + Hash + Display + Eq + PartialEq + Send + 'static {}

type FinishedExecutionResult = Result<SupportedFunctionResult, FinishedExecutionError>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum FinishedExecutionError {
    #[error("permanent timeout")]
    PermanentTimeout,
    #[error("uncategorized error")]
    UncategorizedError,
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum ExecutionStatusInfo {
    Pending,
    Enqueued,
    DelayedUntil(DateTime<Utc>),
    Finished(FinishedExecutionResult),
}
impl ExecutionStatusInfo {
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Finished(_))
    }
}
