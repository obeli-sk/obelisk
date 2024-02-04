use crate::activity::ActivityRequest;
use concepts::{workflow_id::WorkflowId, FunctionFqn};

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("[{workflow_id}] workflow {fqn} not found")]
    NotFound {
        workflow_id: WorkflowId,
        fqn: FunctionFqn,
    },
    #[error("[{workflow_id},{run_id}] workflow {workflow_fqn} encountered non deterministic execution, reason: `{reason}`")]
    NonDeterminismDetected {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] activity failed, workflow {workflow_fqn}, activity {activity_fqn}, reason: `{reason}`")]
    ActivityFailed {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        activity_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] workflow limit reached, workflow {workflow_fqn}, reason: `{reason}`")]
    LimitReached {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] activity limit reached, workflow {workflow_fqn}, activity {activity_fqn}, reason: `{reason}`")]
    ActivityLimitReached {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        activity_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] activity not found, workflow {workflow_fqn}, activity {activity_fqn}")]
    ActivityNotFound {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        activity_fqn: FunctionFqn,
    },
    #[error("[{workflow_id}] workflow {workflow_fqn} cannot be scheduled: `{reason}`")]
    SchedulingError {
        workflow_id: WorkflowId,
        workflow_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] {workflow_fqn} encountered an unknown error: `{source:?}`")]
    UnknownError {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        // #[backtrace]
        source: anyhow::Error,
    },
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum ActivityFailed {
    // TODO: add run_id
    #[error("[{workflow_id}] limit reached for activity {activity_fqn} - `{reason}`")]
    LimitReached {
        workflow_id: WorkflowId,
        activity_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id}] activity {activity_fqn} not found")]
    NotFound {
        workflow_id: WorkflowId,
        activity_fqn: FunctionFqn,
    },
    #[error("[{workflow_id}] activity {activity_fqn} failed - {reason}")]
    Other {
        workflow_id: WorkflowId,
        activity_fqn: FunctionFqn,
        reason: String,
    },
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum WorkflowFailed {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(String),
    #[error(transparent)]
    ActivityFailed(ActivityFailed),
    #[error("limit reached: `{0}`")]
    LimitReached(String),
    #[error("unknown error: `{0:?}`")]
    UnknownError(anyhow::Error),
}

impl WorkflowFailed {
    pub(crate) fn into_execution_error(
        self,
        workflow_fqn: FunctionFqn,
        workflow_id: WorkflowId,
        run_id: u64,
    ) -> ExecutionError {
        match self {
            WorkflowFailed::ActivityFailed(ActivityFailed::Other {
                workflow_id: id,
                activity_fqn,
                reason,
            }) => {
                assert_eq!(id, workflow_id);
                ExecutionError::ActivityFailed {
                    workflow_id,
                    run_id,
                    workflow_fqn,
                    activity_fqn,
                    reason,
                }
            }
            WorkflowFailed::ActivityFailed(ActivityFailed::LimitReached {
                workflow_id: id,
                activity_fqn,
                reason,
            }) => {
                assert_eq!(id, workflow_id);
                ExecutionError::ActivityLimitReached {
                    workflow_id,
                    run_id,
                    workflow_fqn,
                    activity_fqn,
                    reason,
                }
            }
            WorkflowFailed::ActivityFailed(ActivityFailed::NotFound {
                workflow_id: id,
                activity_fqn,
            }) => {
                assert_eq!(id, workflow_id);
                ExecutionError::ActivityNotFound {
                    workflow_id,
                    run_id,
                    workflow_fqn,
                    activity_fqn,
                }
            }
            WorkflowFailed::NonDeterminismDetected(reason) => {
                ExecutionError::NonDeterminismDetected {
                    workflow_id,
                    run_id,
                    workflow_fqn,
                    reason,
                }
            }
            WorkflowFailed::LimitReached(reason) => ExecutionError::LimitReached {
                workflow_id,
                run_id,
                workflow_fqn,
                reason,
            },
            WorkflowFailed::UnknownError(source) => ExecutionError::UnknownError {
                workflow_id,
                run_id,
                workflow_fqn,
                source,
            },
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum HostFunctionError {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(String),
    #[error("interrupt: {fqn}", fqn = request.activity_fqn)]
    Interrupt { request: ActivityRequest },
    #[error(transparent)]
    ActivityFailed(#[from] ActivityFailed),
}
