use crate::{
    command::server::{self, PreparedDirs, ServerVerified, SubmitError, SubmitOutcome},
    server::web_api_server::{
        backtrace::{execution_backtrace, execution_backtrace_source},
        components::{component_wit, components_list},
        deployment::{
            get_current_deployment_id, get_deployment, list_deployments, submit_deployment,
            switch_deployment,
        },
        functions::{function_wit, functions_list},
    },
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing,
};
use axum_accept::AcceptExtractor;
use axum_extra::extract::Query;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentType, ExecutionId, FinishedExecutionFailure, FunctionFqn, JoinSetId,
    SupportedFunctionReturnValue,
    component_id::ComponentDigest,
    prefixed_ulid::{DelayId, DeploymentId, ExecutionIdDerived},
    storage::{
        self, BacktraceFilter, CancelOutcome, DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout,
        DbErrorWrite, DbErrorWriteNonRetriable, DbPool, ExecutionEvent, ExecutionListPagination,
        ExecutionRequest, ExecutionWithState, FunctionNameFilter, ListExecutionsFilter,
        LogInfoAppendRow, Pagination, PendingState, PendingStateFinishedError,
        PendingStateFinishedResultKind, ResponseCursor, ResponseWithCursor, TimeoutOutcome,
        Version, VersionType,
    },
    time::{ClockFn as _, Now, Sleep as _},
};
use http::{StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::{fmt::Write as _, time::Duration};
use tokio::{
    select,
    sync::{mpsc, watch},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument as _, Span, debug, info, info_span, instrument, trace, warn};
use utoipa::{IntoParams, OpenApi, ToSchema};
use val_json::{wast_val::WastVal, wast_val_ser::deserialize_value};
use wasm_workers::{
    activity::cancel_registry::CancelRegistry, registry::ReplayWorker,
    webhook::webhook_registry::WebhookRegistry, workflow::workflow_worker::AdvanceError,
};

#[derive(Clone)]
pub(crate) struct WebApiState {
    pub(crate) server_verified: ServerVerified,
    pub(crate) deployment_ctx: crate::command::server::DeploymentContextHandle,
    pub(crate) db_pool: Arc<dyn DbPool>,
    pub(crate) cancel_registry: CancelRegistry,
    pub(crate) termination_watcher: watch::Receiver<()>,
    pub(crate) subscription_interruption: Option<Duration>,
    pub(crate) log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
    pub(crate) prepared_dirs: PreparedDirs,
    pub(crate) webhook_registry: Arc<WebhookRegistry>,
}

/// `OpenAPI` documentation for the Obelisk REST API
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Obelisk REST API",
        description = "REST API for the Obelisk deterministic workflow engine",
        version = "1.0.0"
    ),
    tags(
        (name = "executions", description = "Execution management"),
        (name = "components", description = "Component management"),
        (name = "functions", description = "Function management"),
        (name = "deployments", description = "Deployment management"),
        (name = "delays", description = "Delay management")
    ),
    paths(
        execution_id_generate,
        delay_cancel,
        delay_pause,
        delay_unpause,
        executions_list,
        execution_cancel,
        execution_pause,
        execution_unpause,
        execution_events,
        logs::execution_logs,
        execution_responses,
        execution_status_get,
        execution_stub,
        execution_get_retval,
        execution_submit_put,
        execution_submit_post,
        execution_replay,
        execution_advance,
        execution_upgrade,
        backtrace::execution_backtrace,
        backtrace::execution_backtrace_source,
        components::component_wit,
        components::components_list,
        functions::functions_list,
        functions::function_wit,
        deployment::list_deployments,
        deployment::get_current_deployment_id,
        deployment::get_deployment,
        deployment::submit_deployment,
        deployment::switch_deployment,
    ),
    components(schemas(
        PaginationDirectionSortedFromLatest,
        PaginationDirectionSortedFromOldest,
        ExecutionWithStateSer,
        ExecutionEventsResponse,
        ExecutionResponsesResponse,
        ExecutionStubPayload,
        RetVal,
        ReplayResponseSer,
        AdvanceRequestSer,
        AdvanceResponseSer,
        ExecutionSubmitPayload,
        ExecutionUpgradePayload,
        logs::LogEntryRowSer,
        logs::LogEntrySer,
        logs::LogLevelSer,
        logs::LogStreamTypeSer,
        logs::LogLevelParam,
        logs::LogStreamTypeParam,
        components::ComponentConfig,
        components::FunctionMetadataLite,
        components::ParameterTypeLite,
        functions::FunctionOutput,
        deployment::DeploymentStateSer,
        backtrace::BacktraceInfoSer,
    ))
)]
pub(crate) struct ApiDoc;

/// Return the `OpenAPI` JSON schema
#[utoipa::path(
    get,
    path = "/openapi.json",
    responses(
        (status = 200, description = "OpenAPI JSON schema")
    )
)]
async fn openapi_json() -> impl IntoResponse {
    pretty_json_response(StatusCode::OK, &ApiDoc::openapi())
}

fn pretty_json_response<T: Serialize>(status: StatusCode, value: &T) -> Response {
    let body = serde_json::to_vec_pretty(value).expect("JSON response serialization must succeed");
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    response
}

pub(crate) fn app_router(state: WebApiState) -> Router {
    Router::new()
        .route("/openapi.json", routing::get(openapi_json))
        .nest("/v1", v1_router())
        .with_state(Arc::new(state))
}

fn v1_router() -> Router<Arc<WebApiState>> {
    Router::new()
        .route("/components", routing::get(components_list))
        .route("/components/{digest}/wit", routing::get(component_wit))
        .route("/functions", routing::get(functions_list))
        .route("/functions/wit", routing::get(function_wit))
        .route("/delays/{delay-id}/cancel", routing::put(delay_cancel))
        .route("/delays/{delay-id}/pause", routing::put(delay_pause))
        .route("/delays/{delay-id}/unpause", routing::put(delay_unpause))
        .route("/execution-id", routing::get(execution_id_generate))
        .route("/executions", routing::get(executions_list))
        .route("/executions", routing::post(execution_submit_post))
        .route(
            "/executions/{execution-id}/cancel",
            routing::put(execution_cancel),
        )
        .route(
            "/executions/{execution-id}/pause",
            routing::put(execution_pause),
        )
        .route(
            "/executions/{execution-id}/unpause",
            routing::put(execution_unpause),
        )
        .route(
            "/executions/{execution-id}/events",
            routing::get(execution_events),
        )
        .route(
            "/executions/{execution-id}/logs",
            routing::get(logs::execution_logs),
        )
        .route(
            "/executions/{execution-id}/replay",
            routing::put(execution_replay),
        )
        .route(
            "/executions/{execution-id}/advance",
            routing::put(execution_advance),
        )
        .route(
            "/executions/{execution-id}/responses",
            routing::get(execution_responses),
        )
        .route(
            "/executions/{execution-id}/status",
            routing::get(execution_status_get),
        )
        .route(
            "/executions/{execution-id}/stub",
            routing::put(execution_stub),
        )
        .route(
            "/executions/{execution-id}",
            routing::get(execution_get_retval),
        )
        .route(
            "/executions/{execution-id}",
            routing::put(execution_submit_put),
        )
        .route(
            "/executions/{execution-id}/upgrade",
            routing::put(execution_upgrade),
        )
        .route("/deployments", routing::get(list_deployments))
        .route("/deployments", routing::post(submit_deployment))
        .route("/deployments/{deployment-id}", routing::get(get_deployment))
        .route(
            "/deployments/{deployment-id}/switch",
            routing::put(switch_deployment),
        )
        .route("/deployment-id", routing::get(get_current_deployment_id))
        .route(
            "/executions/{execution-id}/backtrace",
            routing::get(execution_backtrace),
        )
        .route(
            "/executions/{execution-id}/backtrace/source",
            routing::get(execution_backtrace_source),
        )
}

/// Generate a new execution ID
#[utoipa::path(
    get,
    path = "/v1/execution-id",
    tag = "executions",
    responses(
        (status = 200, description = "Generated execution ID", body = String)
    )
)]
async fn execution_id_generate(_: State<Arc<WebApiState>>, accept: AcceptHeader) -> Response {
    let id = ExecutionId::generate();
    match accept {
        AcceptHeader::Json => pretty_json_response(StatusCode::OK, &id),
        AcceptHeader::Text => id.to_string().into_response(),
    }
}

/// Cancel a delay
#[utoipa::path(
    put,
    path = "/v1/delays/{delay_id}/cancel",
    tag = "delays",
    params(
        ("delay_id" = String, Path, description = "Delay ID to cancel")
    ),
    responses(
        (status = 200, description = "Delay cancelled"),
        (status = 409, description = "Already finished")
    )
)]
#[instrument(skip_all, fields(delay_id))]
async fn delay_cancel(
    Path(delay_id): Path<DelayId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let executed_at = Now.now();
    let outcome = storage::cancel_delay(conn.as_ref(), delay_id, executed_at)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse::from_cancel_outcome(outcome, accept).into_response())
}

/// Pause a delay
#[utoipa::path(
    put,
    path = "/v1/delays/{delay_id}/pause",
    tag = "delays",
    params(
        ("delay_id" = String, Path, description = "Delay ID to pause")
    ),
    responses(
        (status = 200, description = "Delay paused"),
        (status = 404, description = "Delay not found")
    )
)]
#[instrument(skip_all, fields(delay_id))]
async fn delay_pause(
    Path(delay_id): Path<DelayId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    conn.pause_delay(&delay_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "paused".to_string(),
        accept,
    }
    .into_response())
}

/// Unpause a delay
#[utoipa::path(
    put,
    path = "/v1/delays/{delay_id}/unpause",
    tag = "delays",
    params(
        ("delay_id" = String, Path, description = "Delay ID to unpause")
    ),
    responses(
        (status = 200, description = "Delay unpaused"),
        (status = 404, description = "Delay not found")
    )
)]
#[instrument(skip_all, fields(delay_id))]
async fn delay_unpause(
    Path(delay_id): Path<DelayId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    conn.unpause_delay(&delay_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "unpaused".to_string(),
        accept,
    }
    .into_response())
}

#[derive(Deserialize, Debug, IntoParams)]
#[into_params(parameter_in = Query)]
struct ExecutionsListParams {
    /// Filter by function FQN prefix
    ffqn_prefix: Option<String>,
    /// Show derived executions (child executions spawned by workflows)
    #[serde(default)]
    show_derived: bool,
    /// Hide finished executions
    #[serde(default)]
    hide_finished: bool,
    /// Filter by execution ID prefix
    execution_id_prefix: Option<String>,
    /// Filter by component digest
    #[serde(default)]
    #[param(value_type = Option<String>)]
    component_digest: Option<ComponentDigest>,
    /// Filter by deployment ID
    #[serde(default)]
    #[param(value_type = Option<String>)]
    deployment_id: Option<DeploymentId>,
    // pagination
    /// Pagination cursor (`DateTime` or `ExecutionId`)
    #[param(value_type = Option<String>)]
    cursor: Option<ExecutionListCursorDeser>,
    /// Number of items to return
    length: Option<u16>,
    /// Include the cursor item in results
    #[serde(default)]
    including_cursor: bool,
    /// Pagination direction
    #[serde(default)]
    direction: PaginationDirectionSortedFromLatest,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, ToSchema)]
#[serde(rename_all = "snake_case")]
enum PaginationDirectionSortedFromLatest {
    /// Fetch items older than cursor
    #[default] // Default = last few items from newest
    Older,
    /// Fetch items newer than cursor
    Newer,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, ToSchema)]
#[serde(rename_all = "snake_case")]
enum PaginationDirectionSortedFromOldest {
    /// Fetch items older than cursor
    Older,
    /// Fetch items newer than cursor
    #[default] // Default = start from 0
    Newer,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum ExecutionListCursorDeser {
    CreatedBy(DateTime<Utc>),
    ExecutionId(ExecutionId),
}
/// Execution with current state information
#[derive(Serialize, ToSchema)]
pub struct ExecutionWithStateSer {
    /// Unique execution identifier
    #[schema(value_type = String, example = "E_01JKXYZ123456789ABCDEFGHIJ")]
    pub execution_id: ExecutionId,
    /// Fully qualified function name
    #[schema(value_type = String, example = "my-pkg:my-ifc/my-fn")]
    pub ffqn: FunctionFqn,
    /// Current pending state of the execution
    #[schema(value_type = Object)]
    pub pending_state: PendingState,
    /// When the execution was created
    pub created_at: DateTime<Utc>,
    /// When the execution was first scheduled
    pub first_scheduled_at: DateTime<Utc>,
    /// Component content digest
    #[schema(value_type = String)]
    pub component_digest: ComponentDigest,
    /// Type of component (workflow, activity, webhook)
    #[schema(value_type = String)]
    pub component_type: ComponentType,
    /// Deployment that created this execution
    #[schema(value_type = String, example = "Dep_01JKXYZ123456789ABCDEFGHIJ")]
    pub deployment_id: DeploymentId,
}
impl From<ExecutionWithState> for ExecutionWithStateSer {
    fn from(value: ExecutionWithState) -> Self {
        let ExecutionWithState {
            execution_id,
            ffqn,
            pending_state,
            created_at,
            first_scheduled_at,
            component_digest,
            component_type,
            deployment_id,
        } = value;
        ExecutionWithStateSer {
            execution_id,
            ffqn,
            pending_state,
            created_at,
            first_scheduled_at,
            component_digest,
            component_type,
            deployment_id,
        }
    }
}

/// List executions with filtering and pagination
#[utoipa::path(
    get,
    path = "/v1/executions",
    tag = "executions",
    params(ExecutionsListParams),
    responses(
        (status = 200, description = "List of executions", body = Vec<ExecutionWithStateSer>)
    )
)]
#[instrument(skip_all)]
async fn executions_list(
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionsListParams>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let default_pagination = ExecutionListPagination::default();
    let pagination = {
        let ExecutionsListParams {
            cursor,
            length,
            including_cursor,
            direction,
            ..
        } = params;
        let length = length.unwrap_or(default_pagination.length());
        match cursor {
            Some(ExecutionListCursorDeser::CreatedBy(cursor)) => {
                ExecutionListPagination::CreatedBy(match direction {
                    PaginationDirectionSortedFromLatest::Older => Pagination::OlderThan {
                        length,
                        cursor: Some(cursor),
                        including_cursor,
                    },
                    PaginationDirectionSortedFromLatest::Newer => Pagination::NewerThan {
                        length,
                        cursor: Some(cursor),
                        including_cursor,
                    },
                })
            }
            Some(ExecutionListCursorDeser::ExecutionId(cursor)) => {
                ExecutionListPagination::ExecutionId(match direction {
                    PaginationDirectionSortedFromLatest::Older => Pagination::OlderThan {
                        length,
                        cursor: Some(cursor),
                        including_cursor,
                    },
                    PaginationDirectionSortedFromLatest::Newer => Pagination::NewerThan {
                        length,
                        cursor: Some(cursor),
                        including_cursor,
                    },
                })
            }
            None => ExecutionListPagination::CreatedBy(
                // CreatedBy because it is the current default
                match direction {
                    PaginationDirectionSortedFromLatest::Older => Pagination::OlderThan {
                        length,
                        cursor: None,
                        including_cursor, // does not matter
                    },

                    PaginationDirectionSortedFromLatest::Newer => Pagination::NewerThan {
                        length,
                        cursor: None,
                        including_cursor, // does not matter
                    },
                },
            ),
        }
    };

    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;

    let filter = ListExecutionsFilter {
        function_name_filter: {
            // Map `ffqn_prefix` to a FunctionName.
            // If this is a package name with a version, the search will not find anything as the
            // FFQN contains the version behind the interface.
            params.ffqn_prefix.map(FunctionNameFilter::FunctionName)
        },
        show_derived: params.show_derived,
        hide_finished: params.hide_finished,
        execution_id_prefix: params.execution_id_prefix,
        component_digest: params.component_digest,
        deployment_id: params.deployment_id,
    };

    let executions = conn
        .list_executions(filter, pagination)
        .await
        .map_err(|err| ErrorWrapper(err, accept))?;
    Ok(match accept {
        AcceptHeader::Text => {
            let mut output = String::new();
            for execution in executions {
                writeln!(
                    &mut output,
                    "{id} `{pending_state}` {ffqn} `{first_scheduled_at}`",
                    id = execution.execution_id,
                    ffqn = execution.ffqn,
                    pending_state = execution.pending_state,
                    first_scheduled_at = execution.first_scheduled_at,
                )
                .expect("writing to string");
            }
            output.into_response()
        }
        AcceptHeader::Json => {
            let executions: Vec<_> = executions
                .into_iter()
                .map(ExecutionWithStateSer::from)
                .collect();
            pretty_json_response(StatusCode::OK, &executions)
        }
    })
}

/// Cancel an activity execution
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/cancel",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID to cancel")
    ),
    responses(
        (status = 200, description = "Execution cancelled"),
        (status = 409, description = "Already finished"),
        (status = 422, description = "Not an activity")
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_cancel(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let create_req = conn
        .get_create_request(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    // Must verify that this is an activity
    if !create_req.component_id.component_type.is_activity() {
        return Err(HttpResponse {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: "cancelled execution must be an activity".to_string(),
            accept,
        });
    }
    let executed_at = Now.now();
    let outcome = state
        .cancel_registry
        .cancel_activity(conn.as_ref(), &execution_id, executed_at)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse::from_cancel_outcome(outcome, accept).into_response())
}

#[instrument(skip_all, fields(execution_id))]
/// Pause an execution
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/pause",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID to pause")
    ),
    responses(
        (status = 200, description = "Execution paused")
    )
)]
async fn execution_pause(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    info!("Pausing execution");
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let paused_at = Now.now();
    conn.pause_execution(&execution_id, paused_at)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    // No need to distinguish between component types, only activities are tracked in the cancel registry.
    state
        .cancel_registry
        .interrupt_running_activity(&execution_id);
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "paused".to_string(),
        accept,
    }
    .into_response())
}

/// Unpause an execution
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/unpause",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID to unpause")
    ),
    responses(
        (status = 200, description = "Execution unpaused")
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_unpause(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    info!("Unpausing execution");
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let unpaused_at = Now.now();
    conn.unpause_execution(&execution_id, unpaused_at)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "unpaused".to_string(),
        accept,
    }
    .into_response())
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ExecutionEventsParams {
    /// Version cursor for pagination
    version: Option<VersionType>,
    /// Number of events to return
    length: Option<u16>,
    /// Include the cursor item in results
    #[serde(default)]
    including_cursor: bool,
    /// Pagination direction
    #[serde(default)]
    direction: PaginationDirectionSortedFromOldest,
    /// Include backtrace IDs in events
    #[serde(default)]
    include_backtrace_id: bool,
}

/// Response containing execution events
#[derive(Serialize, ToSchema)]
struct ExecutionEventsResponse {
    /// List of execution events
    #[schema(value_type = Vec<Object>)]
    events: Vec<ExecutionEvent>,
    /// Maximum version in the response
    #[schema(value_type = u32)]
    max_version: Version,
}
/// Get execution events (history)
#[utoipa::path(
    get,
    path = "/v1/executions/{execution_id}/events",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID"),
        ExecutionEventsParams
    ),
    responses(
        (status = 200, description = "Execution events", body = ExecutionEventsResponse)
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_events(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionEventsParams>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    const DEFAULT_LENGTH: u16 = 20;
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let length = params.length.unwrap_or(DEFAULT_LENGTH);
    let pagination = match params.direction {
        PaginationDirectionSortedFromOldest::Older => Pagination::OlderThan {
            length,
            cursor: params.version.unwrap_or(VersionType::MAX),
            including_cursor: params.including_cursor,
        },
        PaginationDirectionSortedFromOldest::Newer => Pagination::NewerThan {
            length,
            cursor: params.version.unwrap_or(0),
            including_cursor: params.including_cursor,
        },
    };
    let result = conn
        .list_execution_events(&execution_id, pagination, params.include_backtrace_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(match accept {
        AcceptHeader::Json => pretty_json_response(
            StatusCode::OK,
            &ExecutionEventsResponse {
                events: result.events,
                max_version: result.max_version,
            },
        ),
        AcceptHeader::Text => {
            let mut output = String::new();
            for event in result.events {
                writeln!(
                    &mut output,
                    "{version} `{created_at}` {event}",
                    version = event.version,
                    created_at = event.created_at,
                    event = event.event,
                )
                .expect("writing to string");
            }
            output.into_response()
        }
    })
}

mod logs {
    use super::*;
    use base64::{Engine as _, prelude::BASE64_STANDARD};
    use chrono::{DateTime, Utc};
    use concepts::{
        prefixed_ulid::RunId,
        storage::{LogEntry, LogEntryRow, LogFilter, LogLevel, LogStreamType},
    };
    use std::fmt::Display;

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    #[expect(clippy::struct_excessive_bools)]
    pub(crate) struct ExecutionLogsParams {
        /// Filter by log levels
        #[serde(default)]
        #[param(value_type = Vec<String>)]
        level: Vec<LogLevelParam>,
        /// Filter by stream types (stdout, stderr)
        #[serde(default)]
        #[param(value_type = Vec<String>)]
        stream_type: Vec<LogStreamTypeParam>,
        /// Include log entries
        #[serde(default = "default_true")]
        show_logs: bool,
        /// Include stream entries (stdout/stderr)
        #[serde(default = "default_true")]
        show_streams: bool,
        /// Include logs from all derived executions
        #[serde(default)]
        show_derived: bool,
        /// Include the run ID in text output
        #[serde(default)]
        show_run_id: bool,

        // pagination
        /// Cursor for pagination (`DateTime`, opaque)
        cursor: Option<DateTime<Utc>>,
        /// Number of entries to return
        length: Option<u16>,
        /// Include the cursor item in results
        #[serde(default)]
        including_cursor: bool,
        /// Pagination direction
        #[serde(default)]
        direction: PaginationDirectionSortedFromOldest,
    }

    fn default_true() -> bool {
        true
    }

    #[derive(Debug, Deserialize, Clone, Copy, ToSchema)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LogLevelParam {
        Trace,
        Debug,
        Info,
        Warn,
        Error,
    }

    impl From<LogLevelParam> for LogLevel {
        fn from(value: LogLevelParam) -> Self {
            match value {
                LogLevelParam::Trace => LogLevel::Trace,
                LogLevelParam::Debug => LogLevel::Debug,
                LogLevelParam::Info => LogLevel::Info,
                LogLevelParam::Warn => LogLevel::Warn,
                LogLevelParam::Error => LogLevel::Error,
            }
        }
    }

    #[derive(Debug, Deserialize, Clone, Copy, ToSchema)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LogStreamTypeParam {
        Stdout,
        Stderr,
    }

    impl From<LogStreamTypeParam> for LogStreamType {
        fn from(value: LogStreamTypeParam) -> Self {
            match value {
                LogStreamTypeParam::Stdout => LogStreamType::StdOut,
                LogStreamTypeParam::Stderr => LogStreamType::StdErr,
            }
        }
    }

    /// Log entry row with cursor
    #[derive(Serialize, ToSchema)]
    pub(crate) struct LogEntryRowSer {
        /// Cursor position (`DateTime`, opaque)
        pub cursor: String,
        /// Run ID that produced this log
        #[schema(value_type = String)]
        pub run_id: RunId,
        /// Execution ID that produced this log
        pub execution_id: String,
        /// Log entry details
        #[serde(flatten)]
        pub info: LogEntrySer,
    }

    /// Log entry content
    #[derive(Serialize, ToSchema)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub(crate) enum LogEntrySer {
        /// Structured log message
        Log {
            created_at: DateTime<Utc>,
            level: LogLevelSer,
            message: String,
        },
        /// Raw stream output (stdout/stderr)
        Stream {
            created_at: DateTime<Utc>,
            /// Base64 encoded payload
            payload: String,
            stream_type: LogStreamTypeSer,
        },
    }

    impl From<LogEntryRow> for LogEntryRowSer {
        fn from(row: LogEntryRow) -> Self {
            Self {
                cursor: row.cursor.to_rfc3339(),
                run_id: row.run_id,
                execution_id: row.execution_id.to_string(),
                info: match row.log_entry {
                    LogEntry::Log {
                        created_at,
                        level,
                        message,
                    } => LogEntrySer::Log {
                        created_at,
                        level: level.into(),
                        message,
                    },
                    LogEntry::Stream {
                        created_at,
                        payload,
                        stream_type,
                    } => LogEntrySer::Stream {
                        created_at,
                        payload: BASE64_STANDARD.encode(payload),
                        stream_type: stream_type.into(),
                    },
                },
            }
        }
    }

    #[derive(serde::Serialize, ToSchema)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LogLevelSer {
        Trace,
        Debug,
        Info,
        Warn,
        Error,
    }

    impl Display for LogLevelSer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.pad(match self {
                Self::Trace => "TRACE",
                Self::Debug => "DEBUG",
                Self::Info => "INFO",
                Self::Warn => "WARN",
                Self::Error => "ERROR",
            })
        }
    }

    impl From<LogLevel> for LogLevelSer {
        fn from(value: LogLevel) -> Self {
            match value {
                LogLevel::Trace => Self::Trace,
                LogLevel::Debug => Self::Debug,
                LogLevel::Info => Self::Info,
                LogLevel::Warn => Self::Warn,
                LogLevel::Error => Self::Error,
            }
        }
    }

    #[derive(serde::Serialize, ToSchema)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LogStreamTypeSer {
        Stdout,
        Stderr,
    }

    impl Display for LogStreamTypeSer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.pad(match self {
                Self::Stdout => "STDOUT",
                Self::Stderr => "STDERR",
            })
        }
    }

    impl From<LogStreamType> for LogStreamTypeSer {
        fn from(value: LogStreamType) -> Self {
            match value {
                LogStreamType::StdOut => Self::Stdout,
                LogStreamType::StdErr => Self::Stderr,
            }
        }
    }

    /// Get execution logs
    #[utoipa::path(
        get,
        path = "/v1/executions/{execution_id}/logs",
        tag = "executions",
        params(
            ("execution_id" = String, Path, description = "Execution ID"),
            ExecutionLogsParams
        ),
        responses(
            (status = 200, description = "Execution logs", body = Vec<LogEntryRowSer>)
        )
    )]
    #[instrument(skip_all, fields(execution_id))]
    pub(crate) async fn execution_logs(
        Path(execution_id): Path<ExecutionId>,
        state: State<Arc<WebApiState>>,
        Query(params): Query<ExecutionLogsParams>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        const DEFAULT_LENGTH: u16 = 20;
        const MAX_LENGTH_INCLUSIVE: u16 = 200;

        let filter = match (params.show_logs, params.show_streams) {
            (true, true) => LogFilter::show_combined(
                params.level.into_iter().map(Into::into).collect(),
                params.stream_type.into_iter().map(Into::into).collect(),
            ),
            (true, false) => {
                LogFilter::show_logs(params.level.into_iter().map(Into::into).collect())
            }
            (false, true) => {
                LogFilter::show_streams(params.stream_type.into_iter().map(Into::into).collect())
            }
            (false, false) => {
                return Err(HttpResponse {
                    status: StatusCode::BAD_REQUEST,
                    message: "at least one of `show_logs`, `show_streams` must be set".to_string(),
                    accept,
                });
            }
        };

        let length = MAX_LENGTH_INCLUSIVE.min(params.length.unwrap_or(DEFAULT_LENGTH));

        let pagination = match params.direction {
            PaginationDirectionSortedFromOldest::Older => Pagination::OlderThan {
                length,
                cursor: params.cursor.unwrap_or_else(Utc::now),
                including_cursor: params.including_cursor,
            },
            PaginationDirectionSortedFromOldest::Newer => Pagination::NewerThan {
                length,
                cursor: params.cursor.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
                including_cursor: params.including_cursor,
            },
        };

        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let result = conn
            .list_logs(&execution_id, params.show_derived, filter, pagination)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        Ok(match accept {
            AcceptHeader::Json => {
                let items: Vec<LogEntryRowSer> =
                    result.items.into_iter().map(LogEntryRowSer::from).collect();
                pretty_json_response(StatusCode::OK, &items)
            }
            AcceptHeader::Text => {
                let mut output = String::new();
                struct PrefixId<'a>(bool, &'a dyn Display);
                impl Display for PrefixId<'_> {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        if self.0 {
                            write!(f, "{} ", self.1)
                        } else {
                            Ok(())
                        }
                    }
                }
                for log in result.items {
                    match log.log_entry {
                        LogEntry::Log {
                            created_at,
                            level,
                            message,
                        } => {
                            let level = LogLevelSer::from(level);
                            writeln!(
                                &mut output,
                                "{created_at} [{level:<6}] {run_id}{exec_id}{message}",
                                created_at = created_at.format("%Y-%m-%dT%H:%M:%S%.9fZ"),
                                run_id = PrefixId(params.show_run_id, &log.run_id),
                                exec_id = PrefixId(params.show_derived, &log.execution_id),
                            )
                            .expect("writing to string");
                        }
                        LogEntry::Stream {
                            created_at,
                            payload,
                            stream_type,
                        } => {
                            let stream_type = LogStreamTypeSer::from(stream_type);
                            let payload_utf8 = String::from_utf8_lossy(&payload);
                            writeln!(
                                &mut output,
                                "{created_at} [{stream_type:<6}] {run_id}{exec_id}{payload_utf8}",
                                created_at = created_at.format("%Y-%m-%dT%H:%M:%S%.9fZ"),
                                run_id = PrefixId(params.show_run_id, &log.run_id),
                                exec_id = PrefixId(params.show_derived, &log.execution_id),
                            )
                            .expect("writing to string");
                        }
                    }
                }
                output.into_response()
            }
        })
    }
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ExecutionResponsesParams {
    /// Cursor for pagination
    cursor: Option<u32>,
    /// Number of responses to return
    length: Option<u16>,
    /// Include the cursor item in results
    #[serde(default)]
    including_cursor: bool,
    /// Pagination direction
    #[serde(default)]
    direction: PaginationDirectionSortedFromOldest,
}

/// Response containing execution responses
#[derive(Serialize, ToSchema)]
struct ExecutionResponsesResponse {
    /// List of responses with cursors
    #[schema(value_type = Vec<Object>)]
    responses: Vec<ResponseWithCursor>,
    /// Maximum cursor in the response
    #[schema(value_type = u32)]
    max_cursor: ResponseCursor,
}
/// Get execution responses (join set responses)
#[utoipa::path(
    get,
    path = "/v1/executions/{execution_id}/responses",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID"),
        ExecutionResponsesParams
    ),
    responses(
        (status = 200, description = "Execution responses", body = ExecutionResponsesResponse)
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_responses(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionResponsesParams>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    const DEFAULT_LENGTH: u16 = 20;
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let length = params.length.unwrap_or(DEFAULT_LENGTH);
    let pagination = match params.direction {
        PaginationDirectionSortedFromOldest::Older => Pagination::OlderThan {
            length,
            cursor: params.cursor.unwrap_or(u32::MAX),
            including_cursor: params.including_cursor,
        },
        PaginationDirectionSortedFromOldest::Newer => Pagination::NewerThan {
            length,
            cursor: params.cursor.unwrap_or(0),
            including_cursor: params.including_cursor,
        },
    };
    let result = conn
        .list_responses(&execution_id, pagination)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;

    Ok(match accept {
        AcceptHeader::Json => pretty_json_response(
            StatusCode::OK,
            &ExecutionResponsesResponse {
                responses: result.responses,
                max_cursor: result.max_cursor,
            },
        ),
        AcceptHeader::Text => {
            let mut output = String::new();
            for response in result.responses {
                writeln!(
                    &mut output,
                    "{cursor} `{created_at}` {join_set_id} {resp}",
                    cursor = response.cursor,
                    created_at = response.event.created_at,
                    join_set_id = response.event.event.join_set_id,
                    resp = response.event.event.event,
                )
                .expect("writing to string");
            }
            output.into_response()
        }
    })
}

/// Get execution status
#[utoipa::path(
    get,
    path = "/v1/executions/{execution_id}/status",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID")
    ),
    responses(
        (status = 200, description = "Execution status", body = ExecutionWithStateSer)
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_status_get(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let execution_with_state = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?
        .get_pending_state(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(match accept {
        AcceptHeader::Json => pretty_json_response(
            StatusCode::OK,
            &ExecutionWithStateSer::from(execution_with_state),
        ),
        AcceptHeader::Text => {
            format_execution_status_text(&execution_with_state.pending_state).into_response()
        }
    })
}

fn format_execution_status_text(pending_state: &PendingState) -> String {
    match pending_state {
        PendingState::Locked(_) => "Locked".to_string(),
        PendingState::PendingAt(pending) => format!("Pending at {}", pending.scheduled_at),
        PendingState::BlockedByJoinSet(blocked) => format!(
            "Blocked by {}{}",
            blocked.join_set_id,
            if blocked.closing { " (closing)" } else { "" }
        ),
        PendingState::Paused(_) => "Paused".to_string(),
        PendingState::Finished(finished) => match finished.result_kind {
            PendingStateFinishedResultKind::Ok => "Finished: OK".to_string(),
            PendingStateFinishedResultKind::Err(PendingStateFinishedError::Error) => {
                "Finished: Error".to_string()
            }
            PendingStateFinishedResultKind::Err(PendingStateFinishedError::ExecutionFailure(
                kind,
            )) => format!("Finished: Execution failure ({kind})"),
        },
    }
}

/// Payload for stubbing an execution result
#[derive(Deserialize, ToSchema)]
struct ExecutionStubPayload(
    /// The return value to stub
    serde_json::Value,
);

/// Stub an execution result (for testing/debugging)
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/stub",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Derived execution ID to stub")
    ),
    request_body = ExecutionStubPayload,
    responses(
        (status = 200, description = "Execution stubbed"),
        (status = 422, description = "Invalid stub")
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_stub(
    Path(execution_id): Path<ExecutionIdDerived>,
    state: State<Arc<WebApiState>>,
    Json(ExecutionStubPayload(return_value)): Json<ExecutionStubPayload>,
) -> Result<Response, HttpResponse> {
    let accept = AcceptHeader::Json;
    let (parent_execution_id, join_set_id) = execution_id.split_to_parts();
    let component_registry_ro = {
        let ctx = state.deployment_ctx.read().await;
        ctx.component_registry_ro.clone()
    };
    // Get FFQN
    let db_connection = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let ffqn = db_connection
        .get_create_request(&ExecutionId::Derived(execution_id.clone()))
        .await
        .map_err(|err| ErrorWrapper(err, accept))?
        .ffqn;

    // Check that ffqn exists
    let Some((_component_id, fn_metadata)) =
        component_registry_ro.find_by_exported_ffqn_stub(&ffqn)
    else {
        return Err(HttpResponse {
            status: StatusCode::NOT_FOUND,
            message: "function not found".to_string(),
            accept,
        });
    };
    let created_at = Now.now();

    // Type check `return_value`
    let return_value = {
        let type_wrapper = fn_metadata.return_type.type_wrapper();
        let return_value = match deserialize_value(&return_value, type_wrapper) {
            Ok(wast_val_with_type) => wast_val_with_type,
            Err(err) => {
                return Err(HttpResponse {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    message: format!(
                        "cannot deserialize return value according to its type - {err}"
                    ),
                    accept,
                });
            }
        };
        SupportedFunctionReturnValue::from_wast_val_with_type(return_value)
            .expect("checked that ffqn is no-ext, return type must be Compatible")
    };
    storage::stub_execution(
        db_connection.as_ref(),
        execution_id,
        parent_execution_id,
        join_set_id,
        created_at,
        return_value,
    )
    .await
    .map_err(|err| ErrorWrapper(err, accept))?;

    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "stubbed".to_string(),
        accept,
    }
    .into_response())
}

/// Lossy representation of `SupportedFunctionReturnValue`. Does not contain WIT type.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum RetVal {
    /// Successful result value
    #[schema(value_type = Option<Object>)]
    Ok(Option<WastVal>),
    /// Error result (WIT result's error variant)
    #[schema(value_type = Option<Object>)]
    Error(Option<WastVal>),
    /// Execution failed
    #[schema(value_type = Object)]
    ExecutionFailure(FinishedExecutionFailure),
}
impl From<SupportedFunctionReturnValue> for RetVal {
    fn from(value: SupportedFunctionReturnValue) -> RetVal {
        match value {
            SupportedFunctionReturnValue::Ok(val_with_type) => {
                RetVal::Ok(val_with_type.map(|it| it.value))
            }
            SupportedFunctionReturnValue::Err(val_with_type) => {
                RetVal::Error(val_with_type.map(|it| it.value))
            }
            SupportedFunctionReturnValue::ExecutionFailure(err) => RetVal::ExecutionFailure(err),
        }
    }
}

/// A serializable mirror of `CreateRequest` from concepts.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct CreateRequestSer {
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) execution_id: String,
    pub(crate) ffqn: String,
    #[schema(value_type = Vec<Object>)]
    pub(crate) params: concepts::Params,
    pub(crate) parent_execution_id: Option<String>,
    pub(crate) parent_join_set_id: Option<String>,
    pub(crate) scheduled_at: DateTime<Utc>,
    #[schema(value_type = Object)]
    pub(crate) component_id: concepts::ComponentId,
    pub(crate) deployment_id: String,
    #[schema(value_type = Object)]
    pub(crate) metadata: concepts::ExecutionMetadata,
    pub(crate) scheduled_by: Option<String>,
    pub(crate) paused: bool,
}

impl From<concepts::storage::CreateRequest> for CreateRequestSer {
    fn from(r: concepts::storage::CreateRequest) -> Self {
        let (parent_execution_id, parent_join_set_id) = r
            .parent
            .map(|(execution_id, join_set_id)| {
                (
                    Some(execution_id.to_string()),
                    Some(join_set_id.to_string()),
                )
            })
            .unwrap_or((None, None));
        Self {
            created_at: r.created_at,
            execution_id: r.execution_id.to_string(),
            ffqn: r.ffqn.to_string(),
            params: r.params,
            parent_execution_id,
            parent_join_set_id,
            scheduled_at: r.scheduled_at,
            component_id: r.component_id,
            deployment_id: r.deployment_id.to_string(),
            metadata: r.metadata,
            scheduled_by: r.scheduled_by.map(|execution_id| execution_id.to_string()),
            paused: r.paused,
        }
    }
}

impl TryFrom<CreateRequestSer> for concepts::storage::CreateRequest {
    type Error = String;

    fn try_from(value: CreateRequestSer) -> Result<Self, Self::Error> {
        let parent = match (value.parent_execution_id, value.parent_join_set_id) {
            (Some(execution_id), Some(join_set_id)) => Some((
                execution_id
                    .parse()
                    .map_err(|err| format!("invalid parent_execution_id - {err}"))?,
                join_set_id
                    .parse::<JoinSetId>()
                    .map_err(|err| format!("invalid parent_join_set_id - {err}"))?,
            )),
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => {
                return Err(
                    "parent_execution_id and parent_join_set_id must be both set or both omitted"
                        .to_string(),
                );
            }
        };

        Ok(Self {
            created_at: value.created_at,
            execution_id: value
                .execution_id
                .parse()
                .map_err(|err| format!("invalid execution_id - {err}"))?,
            ffqn: value
                .ffqn
                .parse()
                .map_err(|err| format!("invalid ffqn - {err}"))?,
            params: value.params,
            parent,
            scheduled_at: value.scheduled_at,
            component_id: value.component_id,
            deployment_id: value
                .deployment_id
                .parse()
                .map_err(|err| format!("invalid deployment_id - {err}"))?,
            metadata: value.metadata,
            scheduled_by: value
                .scheduled_by
                .map(|execution_id| {
                    execution_id
                        .parse()
                        .map_err(|err| format!("invalid scheduled_by - {err}"))
                })
                .transpose()?,
            paused: value.paused,
        })
    }
}

/// A serializable mirror of `AppendResponseToExecution` from concepts.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct StubResponseSer {
    pub(crate) parent_execution_id: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) join_set_id: String,
    pub(crate) child_execution_id: String,
    pub(crate) finished_version: u32,
    #[schema(value_type = Object)]
    pub(crate) result: SupportedFunctionReturnValue,
}

impl From<concepts::storage::AppendResponseToExecution> for StubResponseSer {
    fn from(r: concepts::storage::AppendResponseToExecution) -> Self {
        Self {
            parent_execution_id: r.parent_execution_id.to_string(),
            created_at: r.created_at,
            join_set_id: r.join_set_id.to_string(),
            child_execution_id: r.child_execution_id.to_string(),
            finished_version: r.finished_version.0,
            result: r.result,
        }
    }
}

impl TryFrom<StubResponseSer> for concepts::storage::AppendResponseToExecution {
    type Error = String;

    fn try_from(value: StubResponseSer) -> Result<Self, Self::Error> {
        let child_execution_id = value
            .child_execution_id
            .parse::<ExecutionId>()
            .map_err(|err| format!("invalid child_execution_id - {err}"))?;
        let ExecutionId::Derived(child_execution_id) = child_execution_id else {
            return Err("child_execution_id must be a derived execution id".to_string());
        };

        Ok(Self {
            parent_execution_id: value
                .parent_execution_id
                .parse()
                .map_err(|err| format!("invalid parent_execution_id - {err}"))?,
            created_at: value.created_at,
            join_set_id: value
                .join_set_id
                .parse()
                .map_err(|err| format!("invalid join_set_id - {err}"))?,
            child_execution_id,
            finished_version: Version::new(value.finished_version),
            result: value.result,
        })
    }
}

/// A serializable captured database write operation.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum CapturedWriteSer {
    Append {
        execution_id: String,
        version: u32,
        #[schema(value_type = Object)]
        event: concepts::storage::AppendRequest,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        backtraces: Vec<backtrace::BacktraceInfoSer>,
    },
    AppendBatch {
        #[schema(value_type = Vec<Object>)]
        events: Vec<concepts::storage::AppendRequest>,
        execution_id: String,
        version: u32,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        backtraces: Vec<backtrace::BacktraceInfoSer>,
    },
    AppendBatchCreateNewExecution {
        #[schema(value_type = Vec<Object>)]
        events: Vec<concepts::storage::AppendRequest>,
        execution_id: String,
        version: u32,
        child_requests: Vec<CreateRequestSer>,
        backtraces: Vec<backtrace::BacktraceInfoSer>,
    },
    AppendStubResponse {
        execution_id: String,
        version: u32,
        #[schema(value_type = Vec<Object>)]
        events: Vec<concepts::storage::AppendRequest>,
        response: StubResponseSer,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        backtraces: Vec<backtrace::BacktraceInfoSer>,
    },
    AppendFinished {
        execution_id: String,
        version: u32,
        #[schema(value_type = Object)]
        retval: SupportedFunctionReturnValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_execution_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_join_set_id: Option<String>,
    },
}

impl From<concepts::storage::CapturedDbWrite> for CapturedWriteSer {
    fn from(w: concepts::storage::CapturedDbWrite) -> Self {
        use concepts::storage::CapturedDbWrite;
        match w {
            CapturedDbWrite::Append {
                execution_id,
                version,
                req,
                backtraces,
            } => CapturedWriteSer::Append {
                execution_id: execution_id.to_string(),
                version: version.0,
                event: req,
                backtraces: backtraces
                    .into_iter()
                    .map(backtrace::BacktraceInfoSer::from)
                    .collect(),
            },
            CapturedDbWrite::AppendBatch {
                current_time: _,
                batch,
                execution_id,
                version,
                backtraces,
            } => CapturedWriteSer::AppendBatch {
                events: batch,
                execution_id: execution_id.to_string(),
                version: version.0,
                backtraces: backtraces
                    .into_iter()
                    .map(backtrace::BacktraceInfoSer::from)
                    .collect(),
            },
            CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time: _,
                batch,
                execution_id,
                version,
                child_req,
                backtraces,
            } => CapturedWriteSer::AppendBatchCreateNewExecution {
                events: batch,
                execution_id: execution_id.to_string(),
                version: version.0,
                child_requests: child_req.into_iter().map(CreateRequestSer::from).collect(),
                backtraces: backtraces
                    .into_iter()
                    .map(backtrace::BacktraceInfoSer::from)
                    .collect(),
            },
            CapturedDbWrite::AppendStubResponse {
                events,
                response,
                current_time: _,
                backtraces,
            } => CapturedWriteSer::AppendStubResponse {
                execution_id: events.execution_id.to_string(),
                version: events.version.0,
                events: events.batch,
                response: StubResponseSer::from(response),
                backtraces: backtraces
                    .into_iter()
                    .map(backtrace::BacktraceInfoSer::from)
                    .collect(),
            },
            CapturedDbWrite::AppendFinished {
                execution_id,
                version,
                retval,
                current_time: _,
                parent,
            } => CapturedWriteSer::AppendFinished {
                execution_id: execution_id.to_string(),
                version: version.0,
                retval,
                parent_execution_id: parent.as_ref().map(|(id, _)| id.to_string()),
                parent_join_set_id: parent.map(|(_, js)| js.to_string()),
            },
        }
    }
}

impl TryFrom<CapturedWriteSer> for concepts::storage::CapturedDbWrite {
    type Error = String;

    fn try_from(value: CapturedWriteSer) -> Result<Self, Self::Error> {
        match value {
            CapturedWriteSer::Append {
                execution_id,
                version,
                event,
                backtraces,
            } => Ok(Self::Append {
                execution_id: execution_id
                    .parse()
                    .map_err(|err| format!("invalid execution_id - {err}"))?,
                version: Version::new(version),
                req: event,
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendBatch {
                events,
                execution_id,
                version,
                backtraces,
            } => Ok(Self::AppendBatch {
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                batch: events,
                execution_id: execution_id
                    .parse()
                    .map_err(|err| format!("invalid execution_id - {err}"))?,
                version: Version::new(version),
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendBatchCreateNewExecution {
                events,
                execution_id,
                version,
                child_requests,
                backtraces,
            } => Ok(Self::AppendBatchCreateNewExecution {
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                batch: events,
                execution_id: execution_id
                    .parse()
                    .map_err(|err| format!("invalid execution_id - {err}"))?,
                version: Version::new(version),
                child_req: child_requests
                    .into_iter()
                    .map(concepts::storage::CreateRequest::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendStubResponse {
                execution_id,
                version,
                events,
                response,
                backtraces,
            } => Ok(Self::AppendStubResponse {
                events: concepts::storage::AppendEventsToExecution {
                    execution_id: execution_id
                        .parse()
                        .map_err(|err| format!("invalid execution_id - {err}"))?,
                    version: Version::new(version),
                    batch: events,
                },
                response: response.try_into()?,
                current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                backtraces: backtraces
                    .into_iter()
                    .map(concepts::storage::BacktraceInfo::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            CapturedWriteSer::AppendFinished {
                execution_id,
                version,
                retval,
                parent_execution_id,
                parent_join_set_id,
            } => {
                let parent = match (parent_execution_id, parent_join_set_id) {
                    (Some(exec_id), Some(js_id)) => Some((
                        exec_id
                            .parse()
                            .map_err(|err| format!("invalid parent_execution_id - {err}"))?,
                        js_id
                            .parse()
                            .map_err(|err| format!("invalid parent_join_set_id - {err}"))?,
                    )),
                    (None, None) => None,
                    _ => {
                        return Err(
                            "parent_execution_id and parent_join_set_id must both be set or both be unset"
                                .to_string(),
                        );
                    }
                };
                Ok(Self::AppendFinished {
                    execution_id: execution_id
                        .parse()
                        .map_err(|err| format!("invalid execution_id - {err}"))?,
                    version: Version::new(version),
                    retval,
                    current_time: DateTime::UNIX_EPOCH, // will be replaced in `advance`
                    parent,
                })
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ReplayResponseSer {
    Advanceable {
        captured_writes: Vec<CapturedWriteSer>,
    },
    Finished {
        retval: serde_json::Value, // SupportedFunctionReturnValue -> RetVal
    },
    Blocked,
    ReplayFailed {
        error: String,
        captured_writes: Vec<CapturedWriteSer>,
    },
}

impl From<wasm_workers::workflow::workflow_worker::ReplayResponse> for ReplayResponseSer {
    fn from(value: wasm_workers::workflow::workflow_worker::ReplayResponse) -> Self {
        match value {
            wasm_workers::workflow::workflow_worker::ReplayResponse::Advanceable(replay) => {
                Self::Advanceable {
                    captured_writes: replay
                        .captured_writes
                        .into_iter()
                        .map(CapturedWriteSer::from)
                        .collect(),
                }
            }
            wasm_workers::workflow::workflow_worker::ReplayResponse::Finished { result } => {
                Self::Finished {
                    retval: serde_json::to_value(RetVal::from(result))
                        .expect("supported retval must be JSON serializable"),
                }
            }
            wasm_workers::workflow::workflow_worker::ReplayResponse::Blocked => Self::Blocked,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct AdvanceRequestSer {
    pub(crate) captured_writes: Vec<CapturedWriteSer>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AdvanceResponseSer {
    Finished {
        value: RetVal,
    },
    InProgress {
        #[schema(value_type = Object)]
        pending_state: PendingState,
    },
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AdvanceErrorSer {
    VersionMismatch { expected: u32 },
    ReplayMismatch,
    Transient(String),
}

/// Get execution return value
#[utoipa::path(
    get,
    path = "/v1/executions/{execution_id}",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID"),
        ExecutionFollowParam
    ),
    responses(
        (status = 200, description = "Execution result", body = RetVal),
        (status = 425, description = "Not finished yet")
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_get_retval(
    Path(execution_id): Path<ExecutionId>,
    Query(params): Query<ExecutionFollowParam>,
    state: State<Arc<WebApiState>>,
) -> Result<http::Response<Body>, HttpResponse> {
    let last_event = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, AcceptHeader::Json))?
        .get_last_execution_event(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, AcceptHeader::Json))?;

    if let ExecutionRequest::Finished { retval, .. } = last_event.event {
        let retval = RetVal::from(retval);

        Ok(pretty_json_response(StatusCode::OK, &retval))
    } else if params.follow {
        Ok(stream_execution_response(
            execution_id,
            &state,
            StatusCode::OK,
            state.subscription_interruption,
        ))
    } else {
        Ok(HttpResponse {
            status: StatusCode::TOO_EARLY,
            message: "not finished yet".to_string(),
            accept: AcceptHeader::Json,
        }
        .into_response())
    }
}

/// Payload for submitting a new execution
#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub(crate) struct ExecutionSubmitPayload {
    /// Fully qualified function name to execute
    #[schema(value_type = String, example = "my-pkg:my-ifc/my-fn")]
    pub(crate) ffqn: FunctionFqn,
    /// Function parameters as JSON values
    pub(crate) params: Vec<serde_json::Value>,
    /// If true, create the execution in paused state.
    #[serde(default)]
    pub(crate) paused: bool,
}

#[derive(Deserialize, Debug, IntoParams)]
#[into_params(parameter_in = Query)]
struct ExecutionFollowParam {
    /// If true, stream the result when execution finishes
    #[serde(default)]
    follow: bool,
}

/// Submit an execution with a specific ID
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID"),
        ExecutionFollowParam
    ),
    request_body = ExecutionSubmitPayload,
    responses(
        (status = 200, description = "Execution submitted", body = RetVal),
        (status = 409, description = "Conflict")
    )
)]
async fn execution_submit_put(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionFollowParam>,
    accept: AcceptHeader,
    Json(payload): Json<ExecutionSubmitPayload>,
) -> Result<http::Response<Body>, HttpResponse> {
    execution_submit(execution_id, state, payload, params.follow, accept).await
}

/// Submit a new execution (auto-generated ID)
#[utoipa::path(
    post,
    path = "/v1/executions",
    tag = "executions",
    params(ExecutionFollowParam),
    request_body = ExecutionSubmitPayload,
    responses(
        (status = 200, description = "Execution submitted", body = RetVal)
    )
)]
async fn execution_submit_post(
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
    Query(params): Query<ExecutionFollowParam>,
    Json(payload): Json<ExecutionSubmitPayload>,
) -> Result<http::Response<Body>, HttpResponse> {
    let execution_id = ExecutionId::generate();
    execution_submit(execution_id, state, payload, params.follow, accept).await
}

#[instrument(skip_all, fields(execution_id))]
async fn execution_submit(
    execution_id: ExecutionId,
    state: State<Arc<WebApiState>>,
    payload: ExecutionSubmitPayload,
    follow: bool,
    accept: AcceptHeader,
) -> Result<http::Response<Body>, HttpResponse> {
    let (deployment_id, component_registry_ro) = {
        let ctx = state.deployment_ctx.read().await;
        (ctx.deployment_id, ctx.component_registry_ro.clone())
    };
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let res = server::submit(
        deployment_id,
        conn.as_ref(),
        execution_id.clone(),
        payload.ffqn,
        payload.params,
        payload.paused,
        &component_registry_ro,
    )
    .await
    .map_err(|err| ErrorWrapper(err, accept))?;
    let status = match res {
        SubmitOutcome::Created => StatusCode::CREATED,
        SubmitOutcome::ExistsWithSameParameters => StatusCode::OK,
    };
    if follow {
        Ok(stream_execution_response(
            execution_id,
            &state,
            status,
            state.subscription_interruption,
        ))
    } else {
        Ok(HttpResponse {
            status,
            message: execution_id.to_string(),
            accept,
        }
        .into_response())
    }
}

/// Wait until the execution finishes, return `RetVal` as JSON.
fn stream_execution_response(
    execution_id: ExecutionId,
    state: &WebApiState,
    status: StatusCode,
    subscription_interruption: Option<Duration>,
) -> http::Response<Body> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let trace_id = server::gen_trace_id();
    let span = info_span!("stream_execution_response", trace_id, %execution_id);
    tokio::spawn(
        stream_execution_response_task(
            execution_id,
            state.db_pool.clone(),
            tx,
            state.termination_watcher.clone(),
            subscription_interruption,
        )
        .instrument(span),
    );
    // Send response headers immediately.
    let stream = ReceiverStream::new(rx);
    let mut response = (status, Body::from_stream(stream)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    response
}

async fn stream_execution_response_task(
    execution_id: ExecutionId,
    db_pool: Arc<dyn DbPool>,
    tx: mpsc::Sender<Result<Bytes, std::io::Error>>,
    server_termination_watcher: watch::Receiver<()>,
    subscription_interruption: Option<Duration>,
) {
    debug!("Started streaming execution response");
    let db_connection = match db_pool.connection().await {
        Ok(ok) => ok,
        Err(err) => {
            warn!("Cannot obtain connection - {err:?}");
            return;
        }
    };
    let sleep = concepts::time::TokioSleep;
    let timeout_factory = {
        let tx = tx.clone();
        move || {
            let subscription_interruption = subscription_interruption.unwrap_or(Duration::MAX);
            let sleep = sleep.clone();
            let tx = tx.clone();
            let mut server_termination_watcher = server_termination_watcher.clone();
            Box::pin(async move {
                select! {
                    () = tx.closed() => {
                        debug!("Client disconnected");
                        TimeoutOutcome::Cancel
                    }
                    () = sleep.sleep(subscription_interruption) => TimeoutOutcome::Timeout,
                    _ = server_termination_watcher.changed() => TimeoutOutcome::Cancel,
                }
            })
        }
    };

    loop {
        let timeout = timeout_factory();
        let res = db_connection
            .wait_for_finished_result(&execution_id, Some(timeout))
            .await;
        match res {
            Ok(result) => {
                trace!("Finished ok");
                let result = RetVal::from(result);
                let result = serde_json::to_vec_pretty(&result)
                    .expect("serialization of already stored retval cannot fail");
                let _ = tx.try_send(Ok(Bytes::from(result))); // Ignore if the remote side is closed.
                debug!("Sent execution result");
                return;
            }
            Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Timeout)) => {
                trace!("Timeout triggers resubscribing");
            }
            Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Cancel)) => {
                debug!("Connection closed, not waiting for result");
                return;
            }
            Err(DbErrorReadWithTimeout::DbErrorRead(err)) => {
                warn!("Database error: {err:?}");
                return;
            }
        }
    }
}

/// Replay an execution (re-run from execution log)
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/replay",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID to replay")
    ),
    responses(
        (status = 200, description = "Execution replayed", body = ReplayResponseSer),
        (status = 404, description = "Not found"),
        (status = StatusCode::CONFLICT, description = "Replay failed", body = ReplayResponseSer)
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_replay(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let ser = replay_execution_internal(&state, &execution_id, accept).await?;
    let status = if matches!(ser, ReplayResponseSer::ReplayFailed { .. }) {
        StatusCode::CONFLICT
    } else {
        StatusCode::OK
    };
    Ok(match accept {
        AcceptHeader::Json => pretty_json_response(status, &ser),
        AcceptHeader::Text => {
            let body = match ser {
                ReplayResponseSer::Advanceable { captured_writes } => {
                    format!("outcome: advanceable, {} writes", captured_writes.len())
                }
                ReplayResponseSer::Finished { retval } => {
                    format!("outcome: finished\nresult: {retval}")
                }
                ReplayResponseSer::Blocked => "outcome: blocked".to_string(),
                ReplayResponseSer::ReplayFailed {
                    error,
                    captured_writes,
                } => {
                    format!(
                        "outcome: replay_failed, error: {error}, {} writes",
                        captured_writes.len()
                    )
                }
            };
            (status, body).into_response()
        }
    })
}

/// Advance a paused execution using replay-captured writes.
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/advance",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID to advance")
    ),
    request_body = AdvanceRequestSer,
    responses(
        (status = 200, description = "Execution advance outcome", body = AdvanceResponseSer),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Not found"),
        (status = 422, description = "Advance failed", body = AdvanceErrorSer)
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_advance(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
    Json(payload): Json<AdvanceRequestSer>,
) -> Result<Response, HttpResponse> {
    let replay_worker = get_replay_target(&state, &execution_id, accept).await?;

    let captured_writes = wasm_workers::workflow::workflow_worker::ReplayAdvanceable {
        captured_writes: payload
            .captured_writes
            .into_iter()
            .map(concepts::storage::CapturedDbWrite::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|message| HttpResponse {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                message,
                accept,
            })?,
    };
    if captured_writes.is_empty() {
        return Err(HttpResponse::bad_request(
            accept,
            "`captured_writes` must not be empty".to_string(),
        ));
    }

    let advance_res = match &replay_worker {
        ReplayWorker::Js(worker) => worker.advance(execution_id.clone(), captured_writes).await,
        ReplayWorker::Wasm(worker) => worker.advance(execution_id.clone(), captured_writes).await,
    };

    let advance_response = match advance_res {
        Ok(ok) => ok,
        Err(err) => {
            info!("Advance failed: {err:?}");
            let error = match err {
                AdvanceError::NoWrites => {
                    unreachable!("sent 401")
                }
                AdvanceError::VersionMismatch { expected } => AdvanceErrorSer::VersionMismatch {
                    expected: expected.0,
                },
                AdvanceError::ReplayMismatch => AdvanceErrorSer::ReplayMismatch,
                AdvanceError::DbError(db_err) => {
                    return Err(ErrorWrapper(db_err, accept).into());
                }
                err @ (AdvanceError::ExecutorClosing | AdvanceError::LimitReached { .. }) => {
                    AdvanceErrorSer::Transient(err.to_string())
                }
            };
            let response = match accept {
                AcceptHeader::Json => {
                    pretty_json_response(StatusCode::UNPROCESSABLE_ENTITY, &error)
                }
                AcceptHeader::Text => {
                    let text = match error {
                        AdvanceErrorSer::VersionMismatch { expected } => {
                            format!("error: version_mismatch\nexpected: {expected}")
                        }
                        AdvanceErrorSer::ReplayMismatch => "error: replay_mismatch".to_string(),
                        AdvanceErrorSer::Transient(err) => format!("transient error: {err}"),
                    };
                    (StatusCode::UNPROCESSABLE_ENTITY, text).into_response()
                }
            };
            return Ok(response);
        }
    };

    let response = if let Some(finished) = advance_response.finished {
        AdvanceResponseSer::Finished {
            value: RetVal::from(finished),
        }
    } else {
        let execution_with_state = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?
            .get_pending_state(&execution_id)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        AdvanceResponseSer::InProgress {
            pending_state: execution_with_state.pending_state,
        }
    };
    match &response {
        AdvanceResponseSer::Finished { value } => Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &response),
            AcceptHeader::Text => format!(
                "success:\n{}",
                serde_json::to_string_pretty(&value).expect("retval must be JSON serializable")
            )
            .into_response(),
        }),
        AdvanceResponseSer::InProgress { pending_state } => Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &response),
            AcceptHeader::Text => {
                format!("success, current state: {pending_state}").into_response()
            }
        }),
    }
}

async fn get_replay_target(
    state: &Arc<WebApiState>,
    execution_id: &ExecutionId,
    accept: AcceptHeader,
) -> Result<ReplayWorker, HttpResponse> {
    let (component_registry_ro, replay_workers) = {
        let ctx = state.deployment_ctx.read().await;
        (
            ctx.component_registry_ro.clone(),
            ctx.replay_workers.clone(),
        )
    };
    let conn = state
        .db_pool
        .connection()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let create_req = conn.get_create_request(execution_id).await.map_err(|e| {
        if e == DbErrorRead::NotFound {
            HttpResponse::not_found(accept, "execution")
        } else {
            ErrorWrapper(e, accept).into()
        }
    })?;
    let Some((component_id, fn_metadata)) =
        component_registry_ro.find_by_exported_ffqn_submittable(&create_req.ffqn)
    else {
        return Err(HttpResponse::not_found(accept, "component"));
    };
    if fn_metadata.extension.is_some() {
        return Err(HttpResponse::bad_request(
            accept,
            "function must not be an extension".to_string(),
        ));
    }
    Span::current().record("component_id", tracing::field::display(&component_id));
    let (_component_id, replay_worker) = replay_workers
        .get(&component_id.component_digest)
        .ok_or_else(|| HttpResponse::not_found(accept, "replay worker"))?;
    Ok(replay_worker.clone())
}

async fn replay_execution_internal(
    state: &Arc<WebApiState>,
    execution_id: &ExecutionId,
    accept: AcceptHeader,
) -> Result<ReplayResponseSer, HttpResponse> {
    let replay_worker = get_replay_target(state, execution_id, accept).await?;

    let map_replay_err =
        |err: wasm_workers::workflow::workflow_worker::ReplayError| -> Result<ReplayResponseSer, HttpResponse> {
            use wasm_workers::workflow::workflow_worker::ReplayError;
            match err {
                ReplayError::ReplayFailed {
                    err,
                    captured_writes,
                } => Ok(ReplayResponseSer::ReplayFailed {
                    error: err.to_string(),
                    captured_writes: captured_writes
                        .into_iter()
                        .map(CapturedWriteSer::from)
                        .collect(),
                }),
                other => Err(HttpResponse {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    message: format!("Replay error: {other}"),
                    accept,
                }),
            }
        };

    let replay_res = match &replay_worker {
        ReplayWorker::Js(worker) => worker.replay(execution_id.clone()).await,
        ReplayWorker::Wasm(worker) => worker.replay(execution_id.clone()).await,
    };
    match replay_res {
        Ok(response) => Ok(ReplayResponseSer::from(response)),
        Err(err) => map_replay_err(err),
    }
}

/// Payload for upgrading an execution to a new component version
#[derive(Deserialize, ToSchema)]
struct ExecutionUpgradePayload {
    /// Old component digest to upgrade from
    #[schema(value_type = String)]
    pub old: ComponentDigest,
    /// New component digest to upgrade to
    #[schema(value_type = String)]
    pub new: ComponentDigest,
    /// Skip determinism check during upgrade
    #[serde(default)]
    pub skip_determinism_check: bool,
}

/// Upgrade an execution to a new component version
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/upgrade",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID to upgrade")
    ),
    request_body = ExecutionUpgradePayload,
    responses(
        (status = 200, description = "Execution upgraded"),
        (status = 404, description = "Not found"),
        (status = 422, description = "Upgrade failed")
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_upgrade(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
    Json(payload): Json<ExecutionUpgradePayload>,
) -> Result<Response, HttpResponse> {
    if !payload.skip_determinism_check {
        let replay_workers = {
            let ctx = state.deployment_ctx.read().await;
            ctx.replay_workers.clone()
        };
        let (_component_id, replay_worker) = replay_workers
            .get(&payload.new)
            .ok_or_else(|| HttpResponse::not_found(accept, Some("new component")))?;
        let replay_res = match replay_worker {
            ReplayWorker::Js(worker) => worker.replay(execution_id.clone()).await,
            ReplayWorker::Wasm(worker) => worker.replay(execution_id.clone()).await,
        };
        if let Err(err) = replay_res {
            info!("Replay failed: {err:?}");
            return Err(HttpResponse {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                message: format!("Replay failed: {err}"),
                accept,
            });
        }
    }
    state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?
        .upgrade_execution_component(
            &execution_id,
            &payload.old,
            &payload.new,
            concepts::storage::ComponentUpgradeReason::Manual {
                force: payload.skip_determinism_check,
            },
        )
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "upgraded".to_string(),
        accept,
    }
    .into_response())
}

pub(crate) mod components {
    use crate::server::web_api_server::{HttpResponse, pretty_json_response};

    use super::{
        AcceptHeader, Arc, Deserialize, FunctionFqn, IntoParams, IntoResponse, Query, Response,
        Serialize, State, StatusCode, ToSchema, WebApiState,
    };
    use axum::extract::Path;
    use concepts::{
        ComponentId, ComponentType, FunctionExtension, FunctionMetadata, ParameterType,
        component_id::ComponentDigest,
        prefixed_ulid::DeploymentId,
        storage::{DeploymentComponentDetail, PersistedFunctionMetadata, PersistedParameterType},
    };
    use itertools::Itertools;
    use std::fmt::{Debug, Write as _};

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct ComponentsListParams {
        /// Filter by deployment ID
        #[param(value_type = Option<String>)]
        deployment_id: Option<DeploymentId>,
        /// Filter by component type (workflow, activity, webhook)
        #[param(value_type = Option<String>)]
        r#type: Option<ComponentType>,
        /// Filter by component name
        name: Option<String>,
        /// Filter by component digest
        #[param(value_type = Option<String>)]
        digest: Option<ComponentDigest>,
        /// Filter by function
        #[param(value_type = Option<String>)]
        ffqn: Option<FunctionFqn>,
        /// Include exports in response
        #[serde(default)]
        exports: bool,
        /// Include imports in response
        #[serde(default)]
        imports: bool,
        /// Include extension functions
        #[serde(default)]
        extensions: bool,
        /// If set to true, show only submittable exports
        submittable: Option<bool>,
    }

    #[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct ComponentWitParams {
        /// Filter by deployment ID
        #[param(value_type = Option<String>)]
        deployment_id: Option<DeploymentId>,
    }

    /// Get WIT definition for a component
    #[utoipa::path(
        get,
        path = "/v1/components/{digest}/wit",
        tag = "components",
        params(
            ("digest" = String, Path, description = "Component content digest"),
            ComponentWitParams,
        ),
        responses(
            (status = 200, description = "WIT definition", body = String),
            (status = 204, description = "No WIT available"),
            (status = 404, description = "Component not found")
        )
    )]
    pub(crate) async fn component_wit(
        Path(digest): Path<ComponentDigest>,
        state: State<Arc<WebApiState>>,
        Query(params): Query<ComponentWitParams>,
    ) -> Result<Response, HttpResponse> {
        let deployment_id = if let Some(deployment_id) = params.deployment_id {
            deployment_id
        } else {
            state.deployment_ctx.read().await.deployment_id
        };
        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|err| HttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: err.to_string(),
                accept: AcceptHeader::Text,
            })?;
        let wit = conn
            .get_deployment_component_wit(deployment_id, &digest)
            .await
            .map_err(|err| HttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: err.to_string(),
                accept: AcceptHeader::Text,
            })?;
        match wit {
            Some(wit) => Ok(wit.into_response()),
            None => Err(HttpResponse::not_found(
                AcceptHeader::Text,
                Some("component"),
            )),
        }
    }

    /// List components
    #[utoipa::path(
        get,
        path = "/v1/components",
        tag = "components",
        params(ComponentsListParams),
        responses(
            (status = 200, description = "List of components", body = Vec<ComponentConfig>)
        )
    )]
    pub(crate) async fn components_list(
        state: State<Arc<WebApiState>>,
        Query(params): Query<ComponentsListParams>,
        accept: AcceptHeader,
    ) -> Response {
        let deployment_id = if let Some(deployment_id) = params.deployment_id {
            deployment_id
        } else {
            state.deployment_ctx.read().await.deployment_id
        };
        let mut components = match state.db_pool.external_api_conn().await {
            Ok(conn) => match conn.list_deployment_components(deployment_id).await {
                Ok(components) => components,
                Err(err) => {
                    return HttpResponse {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        message: err.to_string(),
                        accept,
                    }
                    .into_response();
                }
            },
            Err(err) => {
                return HttpResponse {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: err.to_string(),
                    accept,
                }
                .into_response();
            }
        };

        if let Some(name) = params.name {
            components.retain(|c| c.component_id.name.as_ref() == name);
        }
        if let Some(digest) = params.digest {
            components.retain(|c| c.component_id.component_digest == digest);
        }
        if let Some(ffqn) = params.ffqn {
            components.retain(|c| {
                filtered_exports(c, params.extensions)
                    .iter()
                    .find(|fn_meta| fn_meta.ffqn == ffqn)
                    .is_some()
            });
        }
        if let Some(ty) = params.r#type {
            components.retain(|c| c.component_id.component_type == ty);
        }
        let components: Vec<_> = components
            .into_iter()
            .map(|c| {
                let exports = if params.exports {
                    let mut exports = Vec::new();
                    for export in filtered_exports(&c, params.extensions)
                        .into_iter()
                        .filter(|e| {
                            if let Some(submittable) = params.submittable {
                                e.submittable == submittable
                            } else {
                                true
                            }
                        })
                    {
                        exports.push(FunctionMetadataLite::from(export));
                    }

                    Some(exports)
                } else {
                    None
                };

                ComponentConfig {
                    component_id: c.component_id,
                    imports: if params.imports {
                        Some(
                            c.imports
                                .into_iter()
                                .map(FunctionMetadataLite::from)
                                .collect(),
                        )
                    } else {
                        None
                    },
                    exports,
                }
            })
            .collect();

        match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &components),
            AcceptHeader::Text => {
                let mut output = String::new();
                for component in components {
                    writeln!(
                        output,
                        "{} {}",
                        component.component_id, component.component_id.component_digest
                    )
                    .expect("writing to string");
                    if let Some(fns) = component.exports {
                        writeln!(output, " exports:").expect("writing to string");
                        for func in fns {
                            writeln!(output, "  {func}").expect("writing to string");
                        }
                    }
                    if let Some(fns) = component.imports {
                        writeln!(output, " imports:").expect("writing to string");
                        for func in fns {
                            writeln!(output, "  {func}").expect("writing to string");
                        }
                    }
                }
                output.into_response()
            }
        }
    }

    /// Component configuration with optional imports/exports
    #[derive(Serialize, ToSchema)]
    pub(crate) struct ComponentConfig {
        /// Component identifier
        #[schema(value_type = Object)]
        component_id: ComponentId,
        /// Imported functions
        #[serde(skip_serializing_if = "Option::is_none")]
        imports: Option<Vec<FunctionMetadataLite>>,
        /// Exported functions
        #[serde(skip_serializing_if = "Option::is_none")]
        exports: Option<Vec<FunctionMetadataLite>>,
    }

    /// Lightweight function metadata
    #[derive(serde::Serialize, derive_more::Display, ToSchema)]
    #[display("{ffqn}: func({}) -> {return_type}", parameter_types.iter().join(", "))]
    pub(crate) struct FunctionMetadataLite {
        /// Fully qualified function name
        #[schema(value_type = String)]
        ffqn: FunctionFqn,
        /// Parameter types
        parameter_types: Vec<ParameterTypeLite>,
        /// Return type as WIT string
        return_type: String,
        /// Function extension type if any
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(value_type = Option<String>)]
        extension: Option<FunctionExtension>,
        /// Externally submittable: primary functions + `-schedule` extended, but no activity stubs
        submittable: bool,
    }
    impl From<FunctionMetadata> for FunctionMetadataLite {
        fn from(value: FunctionMetadata) -> Self {
            FunctionMetadataLite {
                ffqn: value.ffqn,
                parameter_types: value
                    .parameter_types
                    .0
                    .into_iter()
                    .map(ParameterTypeLite::from)
                    .collect(),
                return_type: value.return_type.wit_type().to_string(),
                extension: value.extension,
                submittable: value.submittable,
            }
        }
    }
    impl From<PersistedFunctionMetadata> for FunctionMetadataLite {
        fn from(value: PersistedFunctionMetadata) -> Self {
            FunctionMetadataLite {
                ffqn: value.ffqn,
                parameter_types: value
                    .parameter_types
                    .into_iter()
                    .map(ParameterTypeLite::from)
                    .collect(),
                return_type: value.return_type,
                extension: value.extension,
                submittable: value.submittable,
            }
        }
    }

    /// Parameter type information
    #[derive(serde::Serialize, derive_more::Display, ToSchema)]
    #[display("{name}: {wit_type}")]
    pub(crate) struct ParameterTypeLite {
        /// Parameter name
        name: String,
        /// WIT type representation
        wit_type: String,
    }

    impl From<ParameterType> for ParameterTypeLite {
        fn from(value: ParameterType) -> Self {
            ParameterTypeLite {
                name: value.name.to_string(),
                wit_type: value.wit_type.to_string(),
            }
        }
    }
    impl From<PersistedParameterType> for ParameterTypeLite {
        fn from(value: PersistedParameterType) -> Self {
            ParameterTypeLite {
                name: value.name,
                wit_type: value.wit_type,
            }
        }
    }

    fn filtered_exports(
        component: &DeploymentComponentDetail,
        extensions: bool,
    ) -> Vec<PersistedFunctionMetadata> {
        let mut exports = component.exports.clone();
        if !extensions {
            exports.retain(|fn_metadata| !fn_metadata.ffqn.ifc_fqn.is_extension());
        }
        exports
    }
}

mod functions {
    use super::{
        AcceptHeader, Arc, Deserialize, IntoParams, IntoResponse, Query, Response, State,
        StatusCode, ToSchema, WebApiState, pretty_json_response,
    };
    use concepts::{FunctionExtension, FunctionFqn, FunctionRegistry};
    use std::fmt::Write as _;

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct FunctionsListParams {
        /// Include extension functions
        #[serde(default)]
        extensions: bool,
    }

    /// List all functions
    #[utoipa::path(
        get,
        path = "/v1/functions",
        tag = "functions",
        params(FunctionsListParams),
        responses(
            (status = 200, description = "List of functions", body = Vec<FunctionOutput>)
        )
    )]
    pub(crate) async fn functions_list(
        state: State<Arc<WebApiState>>,
        Query(params): Query<FunctionsListParams>,
        accept: AcceptHeader,
    ) -> Response {
        let component_registry_ro = state
            .deployment_ctx
            .read()
            .await
            .component_registry_ro
            .clone();
        let all_exports = component_registry_ro.all_exports();

        let functions: Vec<FunctionOutput> = all_exports
            .iter()
            .filter(|pkg_ifc| params.extensions || !pkg_ifc.extension)
            .flat_map(|pkg_ifc| pkg_ifc.fns.values())
            .map(|fn_metadata| FunctionOutput {
                ffqn: fn_metadata.ffqn.clone(),
                extension: fn_metadata.extension,
            })
            .collect();

        match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &functions),
            AcceptHeader::Text => {
                let mut output = String::new();
                for func in functions {
                    writeln!(output, "{}", func.ffqn).expect("writing to string");
                }
                output.into_response()
            }
        }
    }

    /// Function output information
    #[derive(serde::Serialize, ToSchema)]
    pub(crate) struct FunctionOutput {
        /// Fully qualified function name
        #[schema(value_type = String)]
        ffqn: FunctionFqn,
        /// Extension type if any
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(value_type = Option<String>)]
        extension: Option<FunctionExtension>,
    }

    use super::HttpResponse;

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct FunctionWitParams {
        /// Fully qualified function name
        #[param(value_type = String)]
        ffqn: FunctionFqn,
    }

    /// Get WIT definition for a function
    #[utoipa::path(
        get,
        path = "/v1/functions/wit",
        tag = "functions",
        params(FunctionWitParams),
        responses(
            (status = 200, description = "WIT definition", body = String),
            (status = 404, description = "Function not found")
        )
    )]
    pub(crate) async fn function_wit(
        Query(params): Query<FunctionWitParams>,
        state: State<Arc<WebApiState>>,
    ) -> Result<Response, HttpResponse> {
        let ffqn = params.ffqn;
        let component_registry_ro = state
            .deployment_ctx
            .read()
            .await
            .component_registry_ro
            .clone();
        // Find the component that exports this function
        let Some((component_id, _fn_metadata)) = component_registry_ro.find_by_exported_ffqn(&ffqn)
        else {
            return Err(HttpResponse::not_found(
                AcceptHeader::Text,
                Some("function"),
            ));
        };

        // Get the WIT for this component
        let wit = component_registry_ro
            .get_wit(&component_id.component_digest)
            .expect("if function is found, component must be found");

        // Print just the interface with the single function
        match crate::wit_printer::print_interface_with_single_fn(wit, &ffqn) {
            Ok(output) => Ok(output.into_response()),
            Err(e) => Err(HttpResponse {
                status: http::StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to print WIT: {e}"),
                accept: AcceptHeader::Text,
            }),
        }
    }
}

mod deployment {
    use crate::{
        command::server::SwitchDeploymentAction,
        server::web_api_server::{
            AcceptHeader, ErrorWrapper, HttpResponse, WebApiState, pretty_json_response,
        },
    };
    use axum::{
        Json,
        extract::{Path, Query, State},
        response::{IntoResponse, Response},
    };
    use chrono::{DateTime, Utc};
    use concepts::prefixed_ulid::DeploymentId;
    use concepts::storage::Pagination;
    use concepts::storage::{
        DeploymentRecord, DeploymentState, DeploymentStatus, LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH,
    };
    use http::StatusCode;
    use serde::{Deserialize, Serialize};
    use std::fmt::Write as _;
    use std::sync::Arc;
    use tracing::{info, instrument};
    use utoipa::{IntoParams, ToSchema};

    #[derive(Debug, Serialize, ToSchema)]
    #[serde(rename_all = "snake_case")]
    pub enum DeploymentStatusSer {
        Inactive,
        Enqueued,
        Active,
    }

    impl From<&DeploymentStatus> for DeploymentStatusSer {
        fn from(s: &DeploymentStatus) -> Self {
            match s {
                DeploymentStatus::Inactive => Self::Inactive,
                DeploymentStatus::Enqueued => Self::Enqueued,
                DeploymentStatus::Active => Self::Active,
            }
        }
    }
    /// Deployment state with execution counts
    #[derive(Debug, Serialize, ToSchema)]
    pub struct DeploymentStateSer {
        /// Deployment identifier
        #[schema(value_type = String, example = "Dep_01JKXYZ123456789ABCDEFGHIJ")]
        pub deployment_id: DeploymentId,
        /// Deployment lifecycle status
        pub status: DeploymentStatusSer,
        /// When this deployment was submitted
        pub created_at: DateTime<Utc>,
        /// When this deployment was last active; None if never active
        pub last_active_at: Option<DateTime<Utc>>,
        /// Number of locked executions
        pub locked: u32,
        /// Number of pending executions
        pub pending: u32,
        /// Number of scheduled executions
        pub scheduled: u32,
        /// Number of blocked executions
        pub blocked: u32,
        /// Number of finished executions
        pub finished: u32,
    }

    impl DeploymentStateSer {
        fn from(deployment_state: &DeploymentState) -> Self {
            Self {
                deployment_id: deployment_state.deployment_id,
                status: DeploymentStatusSer::from(&deployment_state.status),
                created_at: deployment_state.created_at,
                last_active_at: deployment_state.last_active_at,
                locked: deployment_state.locked,
                pending: deployment_state.pending,
                scheduled: deployment_state.scheduled,
                blocked: deployment_state.blocked,
                finished: deployment_state.finished,
            }
        }
    }
    #[derive(Debug, Deserialize, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct ListDeploymentsParams {
        /// Cursor for pagination (deployment ID)
        #[serde(default)]
        #[param(value_type = Option<String>)]
        cursor_from: Option<DeploymentId>,
        /// Number of items to return
        length: Option<u16>,
        /// Include the cursor item in results
        #[serde(default)]
        including_cursor: bool,
    }

    /// List deployments
    #[utoipa::path(
        get,
        path = "/v1/deployments",
        tag = "deployments",
        params(ListDeploymentsParams),
        responses(
            (status = 200, description = "List of deployments", body = Vec<DeploymentStateSer>)
        )
    )]
    #[instrument(skip_all)]
    pub(crate) async fn list_deployments(
        state: State<Arc<WebApiState>>,
        Query(params): Query<ListDeploymentsParams>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;
        let pagination = Pagination::OlderThan {
            length: params
                .length
                .unwrap_or(LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH),
            cursor: params.cursor_from,
            including_cursor: params.including_cursor,
        };
        let states = conn
            .list_deployment_states(Utc::now(), pagination, false)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let states: Vec<DeploymentStateSer> = states
            .into_iter()
            .map(|dep| DeploymentStateSer::from(&dep))
            .collect();

        Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &states),
            AcceptHeader::Text => {
                let mut output = String::new();
                for s in states {
                    writeln!(
                        &mut output,
                        "{} locked={} pending={} scheduled={} blocked={} finished={}",
                        s.deployment_id, s.locked, s.pending, s.scheduled, s.blocked, s.finished,
                    )
                    .expect("writing to string");
                }
                output.into_response()
            }
        })
    }

    /// Get current deployment ID
    #[utoipa::path(
        get,
        path = "/v1/deployment-id",
        tag = "deployments",
        responses(
            (status = 200, description = "Current deployment ID", body = String)
        )
    )]
    pub(crate) async fn get_current_deployment_id(
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let deployment_id = state.deployment_ctx.read().await.deployment_id;
        Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &deployment_id),
            AcceptHeader::Text => deployment_id.to_string().into_response(),
        })
    }

    /// Deployment details with config
    #[derive(Debug, Serialize, ToSchema)]
    pub struct DeploymentRecordSer {
        #[schema(value_type = String)]
        pub deployment_id: DeploymentId,
        pub status: DeploymentStatusSer,
        pub created_at: DateTime<Utc>,
        pub last_active_at: Option<DateTime<Utc>>,
        pub config_json: String,
    }

    impl From<&DeploymentRecord> for DeploymentRecordSer {
        fn from(r: &DeploymentRecord) -> Self {
            Self {
                deployment_id: r.deployment_id,
                status: DeploymentStatusSer::from(&r.status),
                created_at: r.created_at,
                last_active_at: r.last_active_at,
                config_json: r.config_json.clone(),
            }
        }
    }

    /// Get a specific deployment by ID
    #[utoipa::path(
        get,
        path = "/v1/deployments/{deployment_id}",
        tag = "deployments",
        params(
            ("deployment_id" = String, Path, description = "Deployment ID")
        ),
        responses(
            (status = 200, description = "Deployment details", body = DeploymentRecordSer),
            (status = 404, description = "Deployment not found")
        )
    )]
    #[instrument(skip_all, fields(deployment_id))]
    pub(crate) async fn get_deployment(
        Path(deployment_id): Path<DeploymentId>,
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;
        let record = conn
            .get_deployment(deployment_id)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?
            .ok_or_else(|| HttpResponse::not_found(accept, Some("deployment")))?;

        let ser = DeploymentRecordSer::from(&record);
        Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &ser),
            AcceptHeader::Text => {
                let mut output = String::new();
                writeln!(
                    &mut output,
                    "{} status={} created={} last_active={} config={}",
                    ser.deployment_id,
                    match ser.status {
                        DeploymentStatusSer::Inactive => "inactive",
                        DeploymentStatusSer::Enqueued => "enqueued",
                        DeploymentStatusSer::Active => "active",
                    },
                    ser.created_at.to_rfc3339(),
                    ser.last_active_at
                        .map(|t| t.to_rfc3339())
                        .unwrap_or_default(),
                    ser.config_json,
                )
                .expect("writing to string");
                output.into_response()
            }
        })
    }

    /// Request payload for submitting a new deployment
    #[derive(Deserialize, ToSchema)]
    pub struct DeploymentSubmitPayload {
        /// Deployment config as JSON string
        pub config_json: String,
        /// Verify all environment variables before persisting
        #[serde(default)]
        pub verify: bool,
    }

    /// Submit a new deployment
    #[utoipa::path(
        post,
        path = "/v1/deployments",
        tag = "deployments",
        request_body = DeploymentSubmitPayload,
        responses(
            (status = 200, description = "Deployment submitted", body = String),
            (status = 400, description = "Invalid config"),
            (status = 409, description = "Validation failed")
        )
    )]
    #[instrument(skip_all)]
    pub(crate) async fn submit_deployment(
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
        Json(payload): Json<DeploymentSubmitPayload>,
    ) -> Result<Response, HttpResponse> {
        let mut termination_watcher = state.termination_watcher.clone();
        let result = Box::pin(crate::command::server::submit_deployment(
            state.server_verified.clone(),
            &payload.config_json,
            payload.verify,
            Some("web-api".to_string()),
            &state.prepared_dirs,
            state.db_pool.clone(),
            &mut termination_watcher,
        ))
        .await
        .map_err(|err| HttpResponse {
            status: StatusCode::BAD_REQUEST,
            message: format!("{err:#}"),
            accept,
        })?;
        Ok(HttpResponse {
            status: StatusCode::OK,
            message: result.to_string(),
            accept,
        }
        .into_response())
    }

    /// Request payload for switching deployment
    #[derive(Deserialize, ToSchema)]
    pub struct DeploymentSwitchPayload {
        /// Verify config before switching
        #[serde(default)]
        pub verify: bool,
        /// Hot redeploy without restart
        #[serde(default)]
        pub hot_redeploy: bool,
    }

    /// Switch the active deployment
    #[utoipa::path(
        put,
        path = "/v1/deployments/{deployment_id}/switch",
        tag = "deployments",
        params(
            ("deployment_id" = String, Path, description = "Deployment ID to switch to")
        ),
        request_body = DeploymentSwitchPayload,
        responses(
            (status = 200, description = "Deployment switched or enqueued", body = String),
            (status = 404, description = "Deployment not found"),
            (status = 409, description = "Validation or switch failed")
        )
    )]
    #[instrument(skip_all, fields(deployment_id))]
    pub(crate) async fn switch_deployment(
        Path(deployment_id): Path<DeploymentId>,
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
        Json(payload): Json<DeploymentSwitchPayload>,
    ) -> Result<Response, HttpResponse> {
        let mut termination_watcher = state.termination_watcher.clone();
        tracing::Span::current().record("deployment_id", tracing::field::display(&deployment_id));
        let outcome = Box::pin(crate::command::server::switch_deployment(
            state.server_verified.clone(),
            deployment_id,
            SwitchDeploymentAction::new(payload.hot_redeploy, payload.verify),
            &state.prepared_dirs,
            state.db_pool.clone(),
            &mut termination_watcher,
            &state.deployment_ctx,
            &state.webhook_registry,
            state.cancel_registry.clone(),
            state.log_forwarder_sender.clone(),
        ))
        .await
        .map_err(|err| match err {
            crate::command::server::SwitchError::NotFound => {
                HttpResponse::not_found(accept, Some("deployment"))
            }
            crate::command::server::SwitchError::Other(e) => HttpResponse {
                status: StatusCode::BAD_REQUEST,
                message: format!("{e:#}"),
                accept,
            },
        })?;
        info!(%deployment_id, "Deployment switch outcome: {outcome}");
        let message = match outcome {
            crate::command::server::SwitchOutcome::Switched => "switched",
            crate::command::server::SwitchOutcome::RestartRequired => "restart_required",
        };
        Ok(HttpResponse {
            status: StatusCode::OK,
            message: message.to_string(),
            accept,
        }
        .into_response())
    }
}

mod backtrace {
    use super::*;

    /// Selector for which backtrace version to retrieve: "first", "last" (default), or a version number
    #[derive(Debug, Clone)]
    pub(crate) enum BacktraceVersionQuery {
        First,
        Last,
        Specific(Version),
    }

    impl TryFrom<String> for BacktraceVersionQuery {
        type Error = String;
        fn try_from(s: String) -> Result<Self, String> {
            match s.as_str() {
                "first" => Ok(BacktraceVersionQuery::First),
                "last" => Ok(BacktraceVersionQuery::Last),
                v => {
                    let n: VersionType = v
                        .parse()
                        .map_err(|_| format!("invalid version value `{v}`"))?;
                    Ok(BacktraceVersionQuery::Specific(Version(n)))
                }
            }
        }
    }

    impl<'de> serde::Deserialize<'de> for BacktraceVersionQuery {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let s = String::deserialize(d)?;
            BacktraceVersionQuery::try_from(s).map_err(serde::de::Error::custom)
        }
    }

    fn parse_version(
        version: Option<String>,
        accept: AcceptHeader,
    ) -> Result<BacktraceFilter, HttpResponse> {
        match version {
            None => Ok(BacktraceFilter::Last),
            Some(s) => match BacktraceVersionQuery::try_from(s) {
                Ok(BacktraceVersionQuery::First) => Ok(BacktraceFilter::First),
                Ok(BacktraceVersionQuery::Last) => Ok(BacktraceFilter::Last),
                Ok(BacktraceVersionQuery::Specific(v)) => Ok(BacktraceFilter::Specific(v)),
                Err(msg) => Err(HttpResponse {
                    status: StatusCode::BAD_REQUEST,
                    message: msg,
                    accept,
                }),
            },
        }
    }

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct BacktraceParams {
        /// Which backtrace version to retrieve: "first", "last" (default), or a version number
        version: Option<String>,
    }

    /// Serializable version of `BacktraceInfo`
    #[derive(Debug, Serialize, Deserialize, ToSchema)]
    pub(crate) struct BacktraceInfoSer {
        #[schema(value_type = String)]
        pub execution_id: ExecutionId,
        #[schema(value_type = Object)]
        pub component_id: concepts::ComponentId,
        #[schema(value_type = String)]
        pub version_min_including: VersionType,
        #[schema(value_type = String)]
        pub version_max_excluding: VersionType,
        #[schema(value_type = Object)]
        pub wasm_backtrace: concepts::storage::WasmBacktrace,
    }

    impl From<concepts::storage::BacktraceInfo> for BacktraceInfoSer {
        fn from(value: concepts::storage::BacktraceInfo) -> Self {
            BacktraceInfoSer {
                execution_id: value.execution_id,
                component_id: value.component_id,
                version_min_including: value.version_min_including.0,
                version_max_excluding: value.version_max_excluding.0,
                wasm_backtrace: value.wasm_backtrace,
            }
        }
    }

    impl TryFrom<BacktraceInfoSer> for concepts::storage::BacktraceInfo {
        type Error = String;

        fn try_from(value: BacktraceInfoSer) -> Result<Self, Self::Error> {
            Ok(Self {
                execution_id: value.execution_id,
                component_id: value.component_id,
                version_min_including: Version::new(value.version_min_including),
                version_max_excluding: Version::new(value.version_max_excluding),
                wasm_backtrace: value.wasm_backtrace,
            })
        }
    }

    /// Get execution backtrace
    #[utoipa::path(
        get,
        path = "/v1/executions/{execution_id}/backtrace",
        tag = "executions",
        params(
            ("execution_id" = String, Path, description = "Execution ID"),
            BacktraceParams
        ),
        responses(
            (status = 200, description = "Execution backtrace", body = BacktraceInfoSer),
            (status = 404, description = "Not found")
        )
    )]
    #[instrument(skip_all, fields(execution_id))]
    pub(crate) async fn execution_backtrace(
        Path(execution_id): Path<ExecutionId>,
        state: State<Arc<WebApiState>>,
        Query(params): Query<BacktraceParams>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let filter = parse_version(params.version, accept)?;

        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let info = conn
            .get_backtrace(&execution_id, filter)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let info_ser = BacktraceInfoSer::from(info);
        Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &info_ser),
            AcceptHeader::Text => {
                let mut output = String::new();
                writeln!(&mut output, "execution_id: {}", info_ser.execution_id)
                    .expect("writing to string");
                writeln!(&mut output, "component_id: {}", info_ser.component_id)
                    .expect("writing to string");
                writeln!(
                    &mut output,
                    "version: {}..{}",
                    info_ser.version_min_including, info_ser.version_max_excluding
                )
                .expect("writing to string");
                for frame in &info_ser.wasm_backtrace.frames {
                    writeln!(&mut output, "  {}:{}", frame.module, frame.func_name)
                        .expect("writing to string");
                    for sym in &frame.symbols {
                        if let (Some(file), Some(line), Some(col)) = (&sym.file, sym.line, sym.col)
                        {
                            writeln!(
                                &mut output,
                                "    {} ({}:{}:{})",
                                sym.func_name.as_deref().unwrap_or("??"),
                                file,
                                line,
                                col
                            )
                            .expect("writing to string");
                        } else if let Some(func) = &sym.func_name {
                            writeln!(&mut output, "    {func}").expect("writing to string");
                        }
                    }
                }
                output.into_response()
            }
        })
    }

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct BacktraceSourceParams {
        /// File path to retrieve (supports suffix matching if not exact)
        file: String,
        /// Which backtrace version to use for component lookup: "first", "last" (default), or a version number
        version: Option<String>,
    }

    /// Get source file for a backtrace frame
    #[utoipa::path(
        get,
        path = "/v1/executions/{execution_id}/backtrace/source",
        tag = "executions",
        params(
            ("execution_id" = String, Path, description = "Execution ID"),
            BacktraceSourceParams
        ),
        responses(
            (status = 200, description = "Source file content", body = String),
            (status = 404, description = "Not found")
        )
    )]
    #[instrument(skip_all, fields(execution_id))]
    pub(crate) async fn execution_backtrace_source(
        Path(execution_id): Path<ExecutionId>,
        state: State<Arc<WebApiState>>,
        Query(params): Query<BacktraceSourceParams>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let filter = parse_version(params.version, accept)?;

        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let backtrace_info = conn
            .get_backtrace(&execution_id, filter)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let content = conn
            .get_source_file(&backtrace_info.component_id.component_digest, &params.file)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let Some(content) = content else {
            return Err(HttpResponse::not_found(accept, "source file"));
        };

        Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &content),
            AcceptHeader::Text => content.into_response(),
        })
    }
}

#[derive(AcceptExtractor, Clone, Copy, Default)]
pub(crate) enum AcceptHeader {
    #[accept(mediatype = "text/plain")]
    #[default]
    Text,
    #[accept(mediatype = "application/json")]
    Json,
}

struct ErrorWrapper<E>(E, AcceptHeader);

pub(crate) struct HttpResponse {
    status: StatusCode,
    message: String,
    accept: AcceptHeader,
}
impl HttpResponse {
    fn from_cancel_outcome(outcome: CancelOutcome, accept: AcceptHeader) -> Self {
        match outcome {
            CancelOutcome::Cancelled => HttpResponse {
                status: StatusCode::OK,
                message: "cancelled".to_string(),
                accept,
            },
            CancelOutcome::AlreadyFinished => HttpResponse {
                status: StatusCode::CONFLICT,
                message: "already finished".to_string(),
                accept,
            },
        }
    }

    fn not_found(accept: AcceptHeader, what: impl Into<Option<&'static str>>) -> Self {
        HttpResponse {
            status: StatusCode::NOT_FOUND,
            message: if let Some(what) = what.into() {
                format!("{what} not found")
            } else {
                "not found".to_string()
            },
            accept,
        }
    }

    fn bad_request(accept: AcceptHeader, message: String) -> Self {
        HttpResponse {
            status: StatusCode::BAD_REQUEST,
            message,
            accept,
        }
    }
}

impl IntoResponse for HttpResponse {
    fn into_response(self) -> Response {
        match self.accept {
            AcceptHeader::Json => pretty_json_response(
                self.status,
                &if self.status.is_success() {
                    json!({ "ok": self.message })
                } else {
                    json!({ "err": self.message })
                },
            ),
            AcceptHeader::Text => (self.status, self.message).into_response(),
        }
    }
}
impl From<ErrorWrapper<DbErrorGeneric>> for HttpResponse {
    #[track_caller]
    fn from(value: ErrorWrapper<DbErrorGeneric>) -> Self {
        let err = value.0;
        let accept = value.1;
        warn!("{err:?}");
        HttpResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "database error".to_string(),
            accept,
        }
    }
}
impl From<ErrorWrapper<DbErrorRead>> for HttpResponse {
    #[track_caller]
    fn from(value: ErrorWrapper<DbErrorRead>) -> Self {
        let accept = value.1;
        match value.0 {
            DbErrorRead::NotFound => HttpResponse::not_found(accept, None),
            DbErrorRead::Generic(err) => HttpResponse::from(ErrorWrapper(err, accept)),
        }
    }
}
impl From<ErrorWrapper<DbErrorWriteNonRetriable>> for HttpResponse {
    #[track_caller]
    fn from(value: ErrorWrapper<DbErrorWriteNonRetriable>) -> Self {
        let err = value.0;
        let accept = value.1;
        if err == DbErrorWriteNonRetriable::Conflict {
            HttpResponse {
                status: StatusCode::CONFLICT,
                message: "conflict".to_string(),
                accept,
            }
        } else {
            let loc = std::panic::Location::caller();
            let (loc_file, loc_line) = (loc.file(), loc.line());

            warn!(loc_file, loc_line, "{err:?}");
            HttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: "database error".to_string(),
                accept,
            }
        }
    }
}
impl From<ErrorWrapper<DbErrorWrite>> for HttpResponse {
    #[track_caller]
    fn from(value: ErrorWrapper<DbErrorWrite>) -> Self {
        let accept = value.1;
        match value.0 {
            DbErrorWrite::NotFound => HttpResponse::not_found(accept, None),
            DbErrorWrite::Generic(err) => HttpResponse::from(ErrorWrapper(err, accept)),
            DbErrorWrite::NonRetriable(err) => HttpResponse::from(ErrorWrapper(err, accept)),
        }
    }
}
impl From<ErrorWrapper<SubmitError>> for HttpResponse {
    #[track_caller]
    fn from(value: ErrorWrapper<SubmitError>) -> Self {
        let accept = value.1;
        match value.0 {
            err @ SubmitError::Conflict => HttpResponse {
                status: StatusCode::CONFLICT,
                message: err.to_string(),
                accept,
            },
            SubmitError::FunctionNotFound => HttpResponse::not_found(accept, Some("ffqn")),
            SubmitError::DbErrorWrite(db_error_write) => {
                HttpResponse::from(ErrorWrapper(db_error_write, accept))
            }
            err @ (SubmitError::ExecutionIdMustBeTopLevel | SubmitError::ParamsInvalid(_)) => {
                HttpResponse {
                    status: StatusCode::BAD_REQUEST,
                    message: err.to_string(),
                    accept,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::format_execution_status_text;
    use chrono::{DateTime, Utc};
    use concepts::{
        ExecutionFailureKind, SupportedFunctionReturnValue,
        storage::{
            ExecutionRequest, PendingState, PendingStateFinished, PendingStateFinishedError,
            PendingStateFinishedResultKind,
        },
    };

    #[test]
    fn text_status_for_finished_ok_and_error_is_explicit() {
        assert_eq!(
            format_execution_status_text(&PendingState::Finished(PendingStateFinished {
                version: 1,
                finished_at: parse_dt("2026-01-01T00:00:00Z"),
                result_kind: PendingStateFinishedResultKind::Ok,
            })),
            "Finished: OK"
        );
        assert_eq!(
            format_execution_status_text(&PendingState::Finished(PendingStateFinished {
                version: 1,
                finished_at: parse_dt("2026-01-01T00:00:00Z"),
                result_kind: PendingStateFinishedResultKind::Err(PendingStateFinishedError::Error),
            })),
            "Finished: Error"
        );
    }

    #[test]
    fn text_status_for_finished_execution_failure_is_explicit() {
        assert_eq!(
            format_execution_status_text(&PendingState::Finished(PendingStateFinished {
                version: 1,
                finished_at: parse_dt("2026-01-01T00:00:00Z"),
                result_kind: PendingStateFinishedResultKind::Err(
                    PendingStateFinishedError::ExecutionFailure(
                        ExecutionFailureKind::Uncategorized,
                    ),
                ),
            })),
            "Finished: Execution failure (Uncategorized)"
        );
    }

    #[test]
    fn finished_event_display_is_explicit() {
        assert_eq!(
            ExecutionRequest::Finished {
                retval: SupportedFunctionReturnValue::Err(None),
                http_client_traces: None,
            }
            .to_string(),
            "Finished: Error"
        );
        assert_eq!(
            ExecutionRequest::Finished {
                retval: SupportedFunctionReturnValue::ExecutionFailure(
                    concepts::FinishedExecutionFailure {
                        kind: ExecutionFailureKind::Uncategorized,
                        reason: None,
                        detail: None,
                    },
                ),
                http_client_traces: None,
            }
            .to_string(),
            "Finished: Execution failure (Uncategorized)"
        );
    }

    fn parse_dt(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }
}
