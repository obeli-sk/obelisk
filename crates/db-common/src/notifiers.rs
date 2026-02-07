//! Notification types for database events.

use chrono::{DateTime, Utc};
use concepts::{
    ExecutionId, FunctionFqn, SupportedFunctionReturnValue, component_id::InputContentDigest,
    storage::ResponseWithCursor,
};

/// Notification data for when an execution becomes pending.
#[derive(Debug)]
pub struct NotifierPendingAt {
    pub scheduled_at: DateTime<Utc>,
    pub ffqn: FunctionFqn,
    pub component_input_digest: InputContentDigest,
}

/// Notification data for when an execution finishes.
#[derive(Debug)]
pub struct NotifierExecutionFinished {
    pub execution_id: ExecutionId,
    pub retval: SupportedFunctionReturnValue,
}

/// Aggregated notifications to send after an append operation.
#[derive(Debug, Default)]
pub struct AppendNotifier {
    pub pending_at: Option<NotifierPendingAt>,
    pub execution_finished: Option<NotifierExecutionFinished>,
    pub response: Option<(ExecutionId, ResponseWithCursor)>,
}
