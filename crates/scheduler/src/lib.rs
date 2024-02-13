use std::{fmt::Display, hash::Hash};

use async_trait::async_trait;
use concepts::{Params, SupportedFunctionResult};

mod memory;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartialResult {
    FinalResult(SupportedFunctionResult),
    PartialProgress,
}

pub type ExecutionResult = Result<PartialResult, WorkerError>;
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
    async fn run(&self, workflow_id: E, params: Params, store: S) -> ExecutionResult;
}

pub trait ExecutionId: Clone + Hash + Display + Eq + PartialEq + Send + 'static {}

impl<T> ExecutionId for T where T: Clone + Hash + Display + Eq + PartialEq + Send + 'static {}
