use async_trait::async_trait;
use concepts::{workflow_id::WorkflowId, SupportedFunctionResult};
use tokio::sync::oneshot;

mod memory;

pub type ExecutionResult = Result<SupportedFunctionResult, WorkerError>;
pub type ExecutionId = ulid::Ulid;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum WorkerError {
    #[error("worker timed out: {workflow_id}")]
    Timeout { workflow_id: WorkflowId },
}

#[async_trait]
pub trait Worker {
    async fn run(
        &self,
        workflow_id: WorkflowId,
        params: Vec<wasmtime::component::Val>,
    ) -> ExecutionResult;
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueError {
    #[error("queue is full")]
    Full {
        workflow_id: WorkflowId,
        params: Vec<wasmtime::component::Val>,
    },
    #[error("queue is closed")]
    Closed {
        workflow_id: WorkflowId,
        params: Vec<wasmtime::component::Val>,
    },
}

pub trait QueueWriter {
    fn enqueue(
        &self,
        workflow_id: WorkflowId,
        params: Vec<wasmtime::component::Val>,
    ) -> Result<oneshot::Receiver<ExecutionResult>, EnqueueError>;
}
