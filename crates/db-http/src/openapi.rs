use super::{
    AppendBatchResponse, AppendDelayResponseOutcome, AppendEventsToExecution, AppendRequest,
    AppendResponse, AppendResponseToExecution, Arc, BacktraceFilter, BacktraceInfo, CancelOutcome,
    CasGcResult, CleanupResult, ComponentDigest, ComponentId, ComponentMetadataRecord,
    ComponentRetryConfig, ComponentUpgradeReason, ContentDigest, CreateRequest, DateTime, DelayId,
    DeleteDeploymentResult, DeleteExecutionTreeResult, DeploymentComponentDetail,
    DeploymentComponentFileRecord, DeploymentComponentRecord, DeploymentExecutionCounts,
    DeploymentFileRecord, DeploymentId, DeploymentRecord, DeploymentState, EnqueueOutcome,
    ExecutionEvent, ExecutionEventBounds, ExecutionGcResult, ExecutionId, ExecutionIdDerived,
    ExecutionListPagination, ExecutionLog, ExecutionWithState, ExecutionWithStateRequestsResponses,
    ExecutorId, ExpiredTimer, FunctionFqn, HttpPolicyEventIds, JoinSetId,
    ListExecutionEventsResponse, ListExecutionsFilter, ListLogsResponse, ListResponsesResponse,
    LockPendingResponse, LogCursor, LogFilter, LogInfoAppendRow, NonZeroU16, Pagination,
    RetentionPolicy, RunId, StorageStatus, SystemEvent, SystemEventFilter,
    SystemEventRetentionResult, Utc, Version, VersionType,
};
use crate::error::WireError;
use schemars::{JsonSchema, SchemaGenerator, generate::SchemaSettings};
use serde_json::{Map, Value, json};

fn operation<Request: JsonSchema, Response: JsonSchema>(
    generator: &mut SchemaGenerator,
    interface: &str,
    method: &str,
) -> Value {
    json!({
        "operationId": format!("{interface}_{method}"),
        "tags": [interface],
        "requestBody": {"required": true, "content": {"application/json": {
            "schema": generator.subschema_for::<Request>()
        }}},
        "responses": {
            "200": {"description": "Typed database result; Err may follow a committed write and must not be replayed automatically.",
                "content": {"application/json": {"schema": generator.subschema_for::<Result<Response, WireError>>()}}},
            "400": {"description": "Malformed arguments"},
            "401": {"description": "Missing or invalid bearer token"},
            "404": {"description": "Unknown operation or protocol version"},
            "413": {"description": "Request body exceeds 128 MiB"},
            "503": {"description": "Request or long-poll capacity exhausted"},
            "504": {"description": "Request exceeded the 60-second deadline; a write may have committed"}
        }
    })
}

#[must_use]
pub fn schema() -> Value {
    let mut settings = SchemaSettings::draft2020_12();
    settings.definitions_path = "/components/schemas".into();
    let mut generator = settings.into_generator();
    let mut paths = Map::new();
    macro_rules! schema_method {
        (#[cfg(feature = "test")] $($rest:tt)*) => {};
        ($interface:ident, $method:ident, [$($arg:ident),*], ($($ty:tt)*), $out:ty) => {
            let mut operation = operation::<($($ty)*), $out>(&mut generator, stringify!($interface), stringify!($method));
            operation["x-argument-names"] = json!([$(stringify!($arg)),*]);
            paths.insert(format!("/v1/{}/{}", stringify!($interface), stringify!($method)), json!({"post": operation}));
        };
    }
    macro_rules! rpc_interface {
        (DbConnectionTest, $($rest:tt)*) => {};
        ($interface:ident, $connection:ident; $( $(#[$($attr:tt)*])* fn $method:ident($($arg:ident: ($($ty:tt)*)),* $(,)?) -> $out:ty, $err:ty; )* @extras { $($extra:item)* }) => {
            $(schema_method!($(#[$($attr)*])* $interface, $method, [$($arg),*], ($($($ty)*,)*), $out);)*
        };
    }
    crate::rpc_methods!(rpc_interface);
    use concepts::SupportedFunctionReturnValue;
    use concepts::storage::{ResponseCursor, ResponseWithCursor};
    paths.insert("/v1/Notifications/pending_ffqn".into(), json!({"post": operation::<(DateTime<Utc>, Arc<[FunctionFqn]>, Option<ComponentDigest>), ()>(&mut generator, "Notifications", "pending_ffqn")}));
    paths.insert("/v1/Notifications/pending_digest".into(), json!({"post": operation::<(DateTime<Utc>, ComponentDigest), ()>(&mut generator, "Notifications", "pending_digest")}));
    paths.insert("/v1/Notifications/responses".into(), json!({"post": operation::<(ExecutionId, ResponseCursor, u64), Vec<ResponseWithCursor>>(&mut generator, "Notifications", "responses")}));
    paths.insert("/v1/Notifications/finished".into(), json!({"post": operation::<(ExecutionId, u64), SupportedFunctionReturnValue>(&mut generator, "Notifications", "finished")}));
    let binary = json!({"type": "string", "format": "binary"});
    let mut upload = operation::<(), ContentDigest>(&mut generator, "Cas", "write_blob");
    upload["requestBody"] =
        json!({"required": true, "content": {"application/octet-stream": {"schema": binary}}});
    paths.insert("/v1/blobs".into(), json!({"post": upload}));
    paths.insert("/v1/blobs/{digest}".into(), json!({
        "parameters": [{"name": "digest", "in": "path", "required": true, "schema": generator.subschema_for::<ContentDigest>()}],
        "get": {"operationId": "Cas_read_blob", "tags": ["Cas"], "responses": {
            "200": {"description": "Blob bytes", "content": {"application/octet-stream": {"schema": binary}}},
            "400": {"description": "Invalid digest"}, "401": {"description": "Invalid bearer token"},
            "404": {"description": "Blob absent"}, "500": {"description": "Database read failed"},
            "503": {"description": "Storage unavailable"}, "504": {"description": "Request deadline exceeded"}
        }},
        "head": {"operationId": "Cas_contains_blob", "tags": ["Cas"], "responses": {
            "200": {"description": "Blob exists"}, "400": {"description": "Invalid digest"},
            "401": {"description": "Invalid bearer token"}, "404": {"description": "Blob absent"},
            "500": {"description": "Database read failed"}, "503": {"description": "Storage unavailable"},
            "504": {"description": "Request deadline exceeded"}
        }}
    }));
    json!({
        "openapi": "3.1.0",
        "info": {"title": "Obelisk storage API", "version": "1", "description": "Authenticated operation-level storage protocol. Positional JSON arguments and typed Result responses. Notification polls last at most 30 seconds; responses and finished take a maximum wait in milliseconds as their last argument."},
        "security": [{"bearerAuth": []}],
        "paths": paths,
        "components": {"securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}}, "schemas": generator.definitions()}
    })
}
