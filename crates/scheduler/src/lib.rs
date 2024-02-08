use async_trait::async_trait;
use concepts::{workflow_id::WorkflowId, Params, SupportedFunctionResult};

mod memory;

pub type ExecutionResult = Result<SupportedFunctionResult, WorkerError>;
pub type ExecutionId = ulid::Ulid;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum WorkerError {
    #[error("worker timed out: {workflow_id}")]
    Timeout { workflow_id: WorkflowId },
}

#[async_trait]
pub trait Worker {
    async fn run(&self, workflow_id: WorkflowId, params: Params) -> ExecutionResult;
}
