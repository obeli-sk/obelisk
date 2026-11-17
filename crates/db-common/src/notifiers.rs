//! Notification types for database events.

use concepts::{
    ExecutionId, FunctionFqn, SupportedFunctionReturnValue, component_id::ComponentDigest,
    storage::ResponseWithCursor,
};
use jiff::Timestamp;

/// Notification data for when an execution becomes pending.
#[derive(Debug)]
pub struct NotifierPendingAt {
    pub scheduled_at: Timestamp,
    pub ffqn: FunctionFqn,
    pub component_input_digest: ComponentDigest,
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
