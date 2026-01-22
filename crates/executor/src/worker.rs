use async_trait::async_trait;
use concepts::ExecutionFailureKind;
use concepts::ExecutionId;
use concepts::ExecutionMetadata;
use concepts::FunctionMetadata;
use concepts::TrapKind;
use concepts::storage::DbErrorWrite;
use concepts::storage::HistoryEvent;
use concepts::storage::Locked;
use concepts::storage::ResponseWithCursor;
use concepts::storage::Version;
use concepts::storage::http_client_trace::HttpClientTrace;
use concepts::{FinishedExecutionError, StrVariant};
use concepts::{FunctionFqn, ParamsParsingError, ResultParsingError};
use concepts::{Params, SupportedFunctionReturnValue};
use tracing::Span;

#[async_trait]
pub trait Worker: Send + Sync + 'static {
    async fn run(&self, ctx: WorkerContext) -> WorkerResult;

    // List exported functions without extensions.
    // Used by executor.
    fn exported_functions_noext(&self) -> &[FunctionMetadata];
}

#[must_use]
#[derive(Debug)]
pub enum WorkerResult {
    // FIXME: Use Result<WorkerResultOk, WorkerError>
    Ok(
        SupportedFunctionReturnValue,
        Version,
        Option<Vec<HttpClientTrace>>,
    ),
    // If no write occured, the watcher will timeout the execution and retry after backoff which avoids the busy loop
    DbUpdatedByWorkerOrWatcher,
    Err(WorkerError),
}

#[derive(Debug)]
pub enum WorkerResultOk {
    DbUpdatedByWorkerOrWatcher,
    Finished {
        retval: SupportedFunctionReturnValue,
        version: Version,
        traces: Option<Vec<HttpClientTrace>>,
    },
}
impl From<WorkerResultOk> for WorkerResult {
    fn from(value: WorkerResultOk) -> Self {
        match value {
            WorkerResultOk::DbUpdatedByWorkerOrWatcher => WorkerResult::DbUpdatedByWorkerOrWatcher,
            WorkerResultOk::Finished {
                retval,
                version,
                traces,
            } => WorkerResult::Ok(retval, version, traces),
        }
    }
}

#[derive(Debug)]
pub struct WorkerContext {
    pub execution_id: ExecutionId,
    pub metadata: ExecutionMetadata,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub event_history: Vec<(HistoryEvent, Version)>,
    pub responses: Vec<ResponseWithCursor>,
    pub version: Version,
    pub can_be_retried: bool,
    pub worker_span: Span,
    pub locked_event: Locked,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    // retriable errors
    // Used by activity worker
    #[error("activity {trap_kind}: {reason}")]
    ActivityTrap {
        reason: String,
        trap_kind: TrapKind,
        detail: Option<String>,
        version: Version,
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },
    #[error("{reason}")]
    ActivityPreopenedDirError {
        reason: String,
        detail: String,
        version: Version,
    },
    // Used by activity worker, must not be returned when retries are exhausted.
    #[error("activity returned error")]
    ActivityReturnedError {
        detail: Option<String>,
        version: Version,
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },
    /// Resources are exhausted.
    /// Executor must mark the execution as Unlocked.
    /// This event does not increase temporary event count.
    #[error("limit reached: {reason}")]
    LimitReached { reason: String, version: Version },
    // Used by activity worker, best effort. If this is not persisted, the expired timers watcher will append it.
    #[error("temporary timeout")]
    TemporaryTimeout {
        http_client_traces: Option<Vec<HttpClientTrace>>,
        version: Version,
    },
    #[error(transparent)]
    DbError(DbErrorWrite),
    // non-retriable errors
    #[error("fatal error: {0}")]
    FatalError(FatalError, Version),
}

#[derive(Debug, thiserror::Error)]
pub enum FatalError {
    // Used by workflow worker
    #[error("nondeterminism detected")]
    NondeterminismDetected { detail: String },
    // Used by activity worker, workflow worker
    #[error(transparent)]
    ParamsParsingError(ParamsParsingError),
    // Used by activity worker, workflow worker
    #[error("cannot instantiate: {reason}")]
    CannotInstantiate { reason: String, detail: String },
    // Used by activity worker, workflow worker
    #[error(transparent)]
    ResultParsingError(ResultParsingError),
    /// Used when workflow cannot call an imported function, either a child execution or a function from workflow-support.
    #[error("error calling imported function {ffqn} : {reason}")]
    ImportedFunctionCallError {
        ffqn: FunctionFqn,
        reason: StrVariant,
        detail: Option<String>,
    },
    /// Workflow trap if `retry_on_trap` is disabled.
    #[error("workflow {trap_kind}: {reason}")]
    WorkflowTrap {
        reason: String,
        trap_kind: TrapKind,
        detail: Option<String>,
    },
    #[error("out of fuel: {reason}")]
    OutOfFuel { reason: String },
    #[error("constraint violation: {reason}")]
    ConstraintViolation { reason: StrVariant },
    // Applies to activities.
    #[error("cancelled")]
    Cancelled,
}

impl From<FatalError> for FinishedExecutionError {
    fn from(err: FatalError) -> Self {
        let reason_generic = err.to_string(); // Override with err's reason if no information is lost.
        match err {
            FatalError::NondeterminismDetected { detail } => FinishedExecutionError {
                reason: None,
                kind: ExecutionFailureKind::NondeterminismDetected,
                detail: Some(detail),
            },
            FatalError::OutOfFuel { reason } => FinishedExecutionError {
                reason: Some(reason),
                kind: ExecutionFailureKind::OutOfFuel,
                detail: None,
            },
            FatalError::ParamsParsingError(err) => FinishedExecutionError {
                reason: Some(reason_generic),
                kind: ExecutionFailureKind::Uncategorized,
                detail: err.detail(),
            },
            FatalError::CannotInstantiate { reason: _, detail } => FinishedExecutionError {
                reason: Some(reason_generic),
                kind: ExecutionFailureKind::Uncategorized,
                detail: Some(detail),
            },
            FatalError::ResultParsingError(_) | FatalError::ConstraintViolation { reason: _ } => {
                FinishedExecutionError {
                    reason: Some(reason_generic),
                    kind: ExecutionFailureKind::Uncategorized,
                    detail: None,
                }
            }
            FatalError::ImportedFunctionCallError { detail, .. }
            | FatalError::WorkflowTrap { detail, .. } => FinishedExecutionError {
                reason: Some(reason_generic),
                kind: ExecutionFailureKind::Uncategorized,
                detail,
            },
            FatalError::Cancelled => FinishedExecutionError {
                kind: ExecutionFailureKind::Cancelled,
                reason: None,
                detail: None,
            },
        }
    }
}
