use async_trait::async_trait;
use concepts::{workflow_id::WorkflowId, Params, SupportedFunctionResult};

mod memory;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartialResult {
    FinalResult(SupportedFunctionResult),
    PartialProgress,
}

pub type ExecutionResult = Result<PartialResult, WorkerError>;
pub type ExecutionId = ulid::Ulid;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum WorkerError {
    #[error("worker timed out")]
    Timeout,
    #[error("worker failed")]
    Uncategorized,
}

#[async_trait]
pub trait Worker<S> {
    async fn run<'a>(
        &'a self,
        workflow_id: WorkflowId,
        params: Params,
        store: tokio::sync::MutexGuard<'a, S>,
    ) -> ExecutionResult;
}
