use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::ExecutionId;
use concepts::ExecutionMetadata;
use concepts::FunctionMetadata;
use concepts::PermanentFailureKind;
use concepts::TrapKind;
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::prefixed_ulid::RunId;
use concepts::storage::HistoryEvent;
use concepts::storage::Version;
use concepts::storage::http_client_trace::HttpClientTrace;
use concepts::{
    FinishedExecutionError, StrVariant,
    storage::{DbError, JoinSetResponseEvent},
};
use concepts::{FunctionFqn, ParamsParsingError, ResultParsingError};
use concepts::{Params, SupportedFunctionReturnValue};
use tracing::Span;

#[async_trait]
pub trait Worker: Send + Sync + 'static {
    async fn run(&self, ctx: WorkerContext) -> WorkerResult;

    // List exported functions without extensions.
    // Used by executor.
    // TODO: Rename to `exported_functions_noext`
    fn exported_functions(&self) -> &[FunctionMetadata];
}

#[must_use]
#[derive(Debug)]
pub enum WorkerResult {
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
pub struct WorkerContext {
    pub execution_id: ExecutionId,
    pub run_id: RunId,
    pub metadata: ExecutionMetadata,
    pub ffqn: FunctionFqn,
    pub params: Params,
    pub event_history: Vec<HistoryEvent>,
    pub responses: Vec<JoinSetResponseEvent>,
    pub version: Version,
    pub execution_deadline: DateTime<Utc>,
    pub can_be_retried: bool,
    pub worker_span: Span,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    // retriable errors
    // Used by activity worker
    #[error("activity {trap_kind}: {reason}")]
    ActivityTrap {
        reason: String,
        trap_kind: TrapKind,
        detail: String,
        version: Version,
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },
    #[error("{reason_kind}")]
    ActivityPreopenedDirError {
        reason_kind: &'static str,
        reason_inner: String,
        version: Version,
    },
    // Used by activity worker, must not be returned when retries are exhausted.
    #[error("activity returned error")]
    ActivityReturnedError {
        detail: Option<String>,
        version: Version,
        http_client_traces: Option<Vec<HttpClientTrace>>,
    },
    // Resources are exhausted, retry after a delay as Unlocked, without increasing temporary event count.
    #[error("limit reached: {reason}")]
    LimitReached { reason: String, version: Version },
    // Used by activity worker, best effort. If this is not persisted, the expired timers watcher will append it.
    #[error("temporary timeout")]
    TemporaryTimeout {
        http_client_traces: Option<Vec<HttpClientTrace>>,
        version: Version,
    },
    #[error(transparent)]
    DbError(DbError),
    // non-retriable errors
    #[error("fatal error: {0}")]
    FatalError(FatalError, Version),
}

#[derive(Debug, thiserror::Error)]
pub enum FatalError {
    /// Used by workflow worker when directly called child execution fails.
    #[error("child finished with an execution error: {child_execution_id}")]
    UnhandledChildExecutionError {
        child_execution_id: ExecutionIdDerived,
        root_cause_id: ExecutionIdDerived,
    },
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
        detail: String,
    },
    /// Workflow attempted to create a join set with the same name twice, or with an invalid character.
    #[error("{reason}")]
    JoinSetNameError { reason: String },
}

impl From<FatalError> for FinishedExecutionError {
    fn from(value: FatalError) -> Self {
        let reason_full = value.to_string();
        match value {
            FatalError::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            } => FinishedExecutionError::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            },
            FatalError::NondeterminismDetected { detail } => {
                FinishedExecutionError::PermanentFailure {
                    reason_inner: reason_full.clone(),
                    reason_full,
                    kind: PermanentFailureKind::NondeterminismDetected,
                    detail: Some(detail),
                }
            }
            FatalError::ParamsParsingError(params_parsing_error) => {
                FinishedExecutionError::PermanentFailure {
                    reason_inner: reason_full.to_string(),
                    reason_full,
                    kind: PermanentFailureKind::ParamsParsingError,
                    detail: params_parsing_error.detail(),
                }
            }
            FatalError::CannotInstantiate {
                detail,
                reason: reason_inner,
                ..
            } => FinishedExecutionError::PermanentFailure {
                reason_inner,
                reason_full,
                kind: PermanentFailureKind::CannotInstantiate,
                detail: Some(detail),
            },
            FatalError::ResultParsingError(_) => FinishedExecutionError::PermanentFailure {
                reason_inner: reason_full.to_string(),
                reason_full,
                kind: PermanentFailureKind::ResultParsingError,
                detail: None,
            },
            FatalError::ImportedFunctionCallError {
                detail,
                reason: reason_inner,
                ..
            } => FinishedExecutionError::PermanentFailure {
                reason_inner: reason_inner.to_string(),
                reason_full,
                kind: PermanentFailureKind::ImportedFunctionCallError,
                detail,
            },
            FatalError::WorkflowTrap {
                detail,
                reason: reason_inner,
                ..
            } => FinishedExecutionError::PermanentFailure {
                reason_inner,
                reason_full,
                kind: PermanentFailureKind::WorkflowTrap,
                detail: Some(detail),
            },
            FatalError::JoinSetNameError { reason } => FinishedExecutionError::PermanentFailure {
                reason_inner: reason,
                reason_full,
                kind: PermanentFailureKind::JoinSetNameError,
                detail: None,
            },
        }
    }
}
