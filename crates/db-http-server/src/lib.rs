use chrono::{DateTime, Utc};
use concepts::component_id::ComponentDigest;
use concepts::prefixed_ulid::{DelayId, DeploymentId, ExecutionIdDerived, ExecutorId, RunId};
use concepts::storage::{
    AppendBatchResponse, AppendDelayResponseOutcome, AppendEventsToExecution, AppendRequest,
    AppendResponse, AppendResponseToExecution, BacktraceFilter, BacktraceInfo, CancelOutcome,
    CasGcResult, CleanupResult, ComponentMetadataRecord, ComponentUpgradeReason, CreateRequest,
    DbPool, DeleteDeploymentResult, DeleteExecutionTreeResult, DeploymentComponentDetail,
    DeploymentComponentFileRecord, DeploymentComponentRecord, DeploymentExecutionCounts,
    DeploymentFileRecord, DeploymentRecord, DeploymentState, EnqueueOutcome, ExecutionEvent,
    ExecutionEventBounds, ExecutionGcResult, ExecutionListPagination, ExecutionLog,
    ExecutionWithState, ExecutionWithStateRequestsResponses, ExpiredTimer, HttpPolicyEventIds,
    ListExecutionEventsResponse, ListExecutionsFilter, ListLogsResponse, ListResponsesResponse,
    LockPendingResponse, LogCursor, LogFilter, LogInfoAppendRow, Pagination, RetentionPolicy,
    StorageStatus, SystemEvent, SystemEventFilter, SystemEventRetentionResult, Version,
    VersionType,
};
use concepts::{
    ComponentId, ComponentRetryConfig, ContentDigest, ExecutionId, FunctionFqn, JoinSetId,
};
use std::num::NonZeroU16;
use std::sync::Arc;

#[cfg(feature = "test")]
use concepts::storage::{JoinSetResponseEvent, LockedExecution};

mod server;
use db_http::{LONG_POLL_MILLIS, REQUEST_TIMEOUT, error};
pub use server::StorageServer;

macro_rules! owned_type {
    ((&str)) => { String };
    ((&[$t:ty])) => { Vec<$t> };
    ((&$t:ty)) => { $t };
    ((Option<&str>)) => { Option<String> };
    ((Option<&$t:ty>)) => { Option<$t> };
    (($t:ty)) => { $t };
}
macro_rules! borrowed_arg {
    ((&str), $arg:ident) => {
        &$arg
    };
    ((&[$t:ty]), $arg:ident) => {
        &$arg
    };
    ((&$t:ty), $arg:ident) => {
        &$arg
    };
    ((Option<&str>), $arg:ident) => {
        $arg.as_deref()
    };
    ((Option<&$t:ty>), $arg:ident) => {
        $arg.as_ref()
    };
    (($t:ty), $arg:ident) => {
        $arg
    };
}
macro_rules! checked_call {
    (append_system_event_with_cas, $connection:ident, $event:expr, $content:expr) => {{
        let event = $event;
        let content = $content;
        if event.cas_digest.as_ref() == Some(&concepts::cas::content_digest(&content)) {
            $connection.append_system_event_with_cas(event, content).await
        } else {
            Err(server::invalid_argument("system event CAS digest must match its content"))
        }
    }};
    (append_batch, $connection:ident, $at:expr, $batch:expr $(, $arg:expr)*) => {
        checked_call!(@batch append_batch, $connection, $at, $batch $(, $arg)*)
    };
    (append_batch_with_delay_response, $connection:ident, $at:expr, $batch:expr $(, $arg:expr)*) => {
        checked_call!(@batch append_batch_with_delay_response, $connection, $at, $batch $(, $arg)*)
    };
    (append_batch_create_new_execution, $connection:ident, $at:expr, $batch:expr $(, $arg:expr)*) => {
        checked_call!(@batch append_batch_create_new_execution, $connection, $at, $batch $(, $arg)*)
    };
    (@batch $method:ident, $connection:ident, $at:expr, $batch:expr $(, $arg:expr)*) => {{
        let batch = $batch;
        if batch.is_empty() {
            Err(server::invalid_argument("append batch must not be empty"))
        } else {
            $connection.$method($at, batch $(, $arg)*).await
        }
    }};
    (insert_deployment_with_components, $connection:ident, $record:expr $(, $arg:expr)*) => {{
        let record = $record;
        if record.status == concepts::storage::DeploymentStatus::Inactive && record.last_active_at.is_none() {
            $connection.insert_deployment_with_components(record $(, $arg)*).await
        } else {
            Err(server::invalid_argument("new deployments must be inactive and have no activation timestamp"))
        }
    }};
    ($method:ident, $connection:ident $(, $arg:expr)*) => {
        $connection.$method($($arg),*).await
    };
}

macro_rules! rpc_interface {
    ($interface:ident, $connection:ident; $( $(#[$attr:meta])* fn $method:ident($($arg:ident: ($($ty:tt)*)),* $(,)?) -> $out:ty, $err:ty; )* @extras { $($extra:item)* }) => {
        #[allow(non_snake_case)]
        async fn $connection(pool: &dyn DbPool, method: &str, body: &[u8]) -> Result<axum::response::Response, server::ProtocolError> {
            match method {
                $( $(#[$attr])*
                    stringify!($method) => {
                        let ($($arg,)*): ($(owned_type!(($($ty)*)),)*) = serde_json::from_slice(body)?;
                        let connection = pool.$connection().await;
                        let result: Result<$out, error::WireError> = match connection {
                            Ok(connection) => checked_call!($method, connection $(, borrowed_arg!(($($ty)*), $arg))*).map_err(error::WireError::from),
                            Err(err) => Err(err.into()),
                        };
                        Ok(server::encode(result))
                    }
                )*
                _ => Err(server::ProtocolError::UnknownOperation),
            }
        }
    };
}
db_http::rpc_methods!(rpc_interface);
