#![expect(clippy::needless_for_each)] // for #[openapi] annotation
use crate::{
    command::server::{self, SubmitError, SubmitOutcome},
    server::web_api_server::{
        components::{component_wit, components_list},
        deployment::{get_current_deployment_id, list_deployments},
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
    ComponentType, ExecutionId, FinishedExecutionError, FunctionFqn, SupportedFunctionReturnValue,
    component_id::InputContentDigest,
    prefixed_ulid::{DelayId, DeploymentId, ExecutionIdDerived},
    storage::{
        self, CancelOutcome, DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite,
        DbErrorWriteNonRetriable, DbPool, ExecutionEvent, ExecutionListPagination,
        ExecutionRequest, ExecutionWithState, ListExecutionsFilter, LogInfoAppendRow, Pagination,
        PendingState, ResponseCursor, ResponseWithCursor, TimeoutOutcome, Version, VersionType,
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
use tracing::{Instrument as _, Span, debug, info_span, instrument, trace, warn};
use utoipa::{IntoParams, OpenApi, ToSchema};
use val_json::{wast_val::WastVal, wast_val_ser::deserialize_value};
use wasm_workers::{
    activity::cancel_registry::CancelRegistry, component_logger::LogStrageConfig, engines::Engines,
    registry::ComponentConfigRegistryRO, workflow::workflow_js_worker::WorkflowJsWorker,
    workflow::workflow_worker::WorkflowWorker,
};

#[derive(Clone)]
pub(crate) struct WebApiState {
    pub(crate) deployment_id: DeploymentId,
    pub(crate) db_pool: Arc<dyn DbPool>,
    pub(crate) component_registry_ro: ComponentConfigRegistryRO,
    pub(crate) cancel_registry: CancelRegistry,
    pub(crate) termination_watcher: watch::Receiver<()>,
    pub(crate) subscription_interruption: Option<Duration>,
    pub(crate) engines: Engines,
    pub(crate) log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
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
        execution_upgrade,
        components::component_wit,
        components::components_list,
        functions::functions_list,
        functions::function_wit,
        deployment::list_deployments,
        deployment::get_current_deployment_id,
    ),
    components(schemas(
        PaginationDirection,
        ExecutionWithStateSer,
        ExecutionEventsResponse,
        ExecutionResponsesResponse,
        ExecutionStubPayload,
        RetVal,
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
    Json(ApiDoc::openapi())
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
        .route("/deployment-id", routing::get(get_current_deployment_id))
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
        AcceptHeader::Json => Json(id).into_response(),
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
    component_digest: Option<InputContentDigest>,
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
    direction: PaginationDirection,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, ToSchema)]
#[serde(rename_all = "snake_case")]
enum PaginationDirection {
    /// Fetch items older than cursor
    Older,
    /// Fetch items newer than cursor
    #[default] // everything is newer than 0
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
    pub component_digest: InputContentDigest,
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
                    PaginationDirection::Older => Pagination::OlderThan {
                        length,
                        cursor: Some(cursor),
                        including_cursor,
                    },
                    PaginationDirection::Newer => Pagination::NewerThan {
                        length,
                        cursor: Some(cursor),
                        including_cursor,
                    },
                })
            }
            Some(ExecutionListCursorDeser::ExecutionId(cursor)) => {
                ExecutionListPagination::ExecutionId(match direction {
                    PaginationDirection::Older => Pagination::OlderThan {
                        length,
                        cursor: Some(cursor),
                        including_cursor,
                    },
                    PaginationDirection::Newer => Pagination::NewerThan {
                        length,
                        cursor: Some(cursor),
                        including_cursor,
                    },
                })
            }
            None => ExecutionListPagination::CreatedBy(
                // CreatedBy because it is the current default
                match direction {
                    PaginationDirection::Older => Pagination::OlderThan {
                        length,
                        cursor: None,
                        including_cursor, // does not matter
                    },

                    PaginationDirection::Newer => Pagination::NewerThan {
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
        ffqn_prefix: params.ffqn_prefix,
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
            Json(executions).into_response()
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
        .cancel(conn.as_ref(), &execution_id, executed_at)
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
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let paused_at = Now.now();
    conn.pause_execution(&execution_id, paused_at)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
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
    direction: PaginationDirection,
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
        PaginationDirection::Older => Pagination::OlderThan {
            length,
            cursor: params.version.unwrap_or(VersionType::MAX),
            including_cursor: params.including_cursor,
        },
        PaginationDirection::Newer => Pagination::NewerThan {
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
        AcceptHeader::Json => Json(ExecutionEventsResponse {
            events: result.events,
            max_version: result.max_version,
        })
        .into_response(),
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
    use concepts::{
        prefixed_ulid::RunId,
        storage::{LogEntry, LogEntryRow, LogFilter, LogLevel, LogStreamType},
    };

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
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

        // pagination
        /// Cursor for pagination
        cursor: Option<u32>,
        /// Number of entries to return
        length: Option<u16>,
        /// Include the cursor item in results
        #[serde(default)]
        including_cursor: bool,
        /// Pagination direction
        #[serde(default)]
        direction: PaginationDirection,
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
        /// Cursor position
        pub cursor: u32,
        /// Run ID that produced this log
        #[schema(value_type = String)]
        pub run_id: RunId,
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
                cursor: row.cursor,
                run_id: row.run_id,
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

    #[derive(serde::Serialize, derive_more::Display, ToSchema)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LogLevelSer {
        #[display("TRACE")]
        Trace,
        #[display("DEBUG")]
        Debug,
        #[display("INFO")]
        Info,
        #[display("WARN")]
        Warn,
        #[display("ERROR")]
        Error,
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

    #[derive(serde::Serialize, derive_more::Display, ToSchema)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LogStreamTypeSer {
        #[display("STDOUT")]
        Stdout,
        #[display("STDERR")]
        Stderr,
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
            PaginationDirection::Older => Pagination::OlderThan {
                length,
                cursor: params.cursor.unwrap_or(0),
                including_cursor: params.including_cursor,
            },
            PaginationDirection::Newer => Pagination::NewerThan {
                length,
                cursor: params.cursor.unwrap_or(0),
                including_cursor: params.including_cursor,
            },
        };

        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let result = conn
            .list_logs(&execution_id, filter, pagination)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        Ok(match accept {
            AcceptHeader::Json => {
                let items: Vec<LogEntryRowSer> =
                    result.items.into_iter().map(LogEntryRowSer::from).collect();
                Json(items).into_response()
            }
            AcceptHeader::Text => {
                let mut output = String::new();
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
                                "{cursor} {run_id} `{created_at}` [{level}] {message}",
                                cursor = log.cursor,
                                run_id = log.run_id,
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
                                "{cursor} {run_id} `{created_at}` [{stream_type}] {payload_utf8}",
                                cursor = log.cursor,
                                run_id = log.run_id,
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
    direction: PaginationDirection,
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
        PaginationDirection::Older => Pagination::OlderThan {
            length,
            cursor: params.cursor.unwrap_or(u32::MAX),
            including_cursor: params.including_cursor,
        },
        PaginationDirection::Newer => Pagination::NewerThan {
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
        AcceptHeader::Json => Json(ExecutionResponsesResponse {
            responses: result.responses,
            max_cursor: result.max_cursor,
        })
        .into_response(),
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
        AcceptHeader::Json => {
            Json(ExecutionWithStateSer::from(execution_with_state)).into_response()
        }
        AcceptHeader::Text => execution_with_state.to_string().into_response(),
    })
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
    let Some((_component_id, fn_metadata)) = state
        .component_registry_ro
        .find_by_exported_ffqn_stub(&ffqn)
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

/// Return value from a finished execution
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum RetVal {
    /// Successful result value
    #[schema(value_type = Option<Object>)]
    Ok(Option<WastVal>),
    /// Error result (WIT result's err variant)
    #[schema(value_type = Option<Object>)]
    Err(Option<WastVal>),
    /// Execution failed with an error
    #[schema(value_type = Object)]
    ExecutionError(FinishedExecutionError),
}
impl From<SupportedFunctionReturnValue> for RetVal {
    fn from(value: SupportedFunctionReturnValue) -> RetVal {
        match value {
            SupportedFunctionReturnValue::Ok { ok: val_with_type } => {
                RetVal::Ok(val_with_type.map(|it| it.value))
            }
            SupportedFunctionReturnValue::Err { err: val_with_type } => {
                RetVal::Err(val_with_type.map(|it| it.value))
            }
            SupportedFunctionReturnValue::ExecutionError(err) => RetVal::ExecutionError(err),
        }
    }
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

    if let ExecutionRequest::Finished { result, .. } = last_event.event {
        let result = RetVal::from(result);
        Ok(Json(result).into_response())
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
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let res = server::submit(
        state.deployment_id,
        conn.as_ref(),
        execution_id.clone(),
        payload.ffqn,
        payload.params,
        &state.component_registry_ro,
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
                let result = serde_json::to_vec(&result)
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
        (status = 200, description = "Execution replayed"),
        (status = 404, description = "Not found"),
        (status = 422, description = "Replay failed")
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_replay(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let conn = state
        .db_pool
        .connection()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    // Find the execution's ffqn.
    let create_req = conn.get_create_request(&execution_id).await.map_err(|e| {
        if e == DbErrorRead::NotFound {
            HttpResponse::not_found(accept, "execution")
        } else {
            ErrorWrapper(e, accept).into()
        }
    })?;
    // Check that ffqn exists
    let Some((component_id, _fn_metadata)) = state
        .component_registry_ro
        .find_by_exported_ffqn_submittable(&create_req.ffqn)
    else {
        return Err(HttpResponse::not_found(accept, "component"));
    };
    Span::current().record("component_id", tracing::field::display(component_id));

    let (component_id, replay_info) = state
        .component_registry_ro
        .get_workflow_replay_info(&component_id.input_digest)
        .expect("digest taken from found component id");

    let logs_storage_config = replay_info
        .logs_store_min_level
        .map(|min_level| LogStrageConfig {
            min_level,
            log_sender: state.log_forwarder_sender.clone(),
        });

    let replay_res = if let Some(js_info) = &replay_info.js_workflow_info {
        WorkflowJsWorker::replay(
            state.deployment_id,
            component_id.clone(),
            replay_info.runnable_component.wasmtime_component.clone(),
            &replay_info.runnable_component.wasm_component.exim,
            state.engines.workflow_engine.clone(),
            Arc::new(state.component_registry_ro.clone()),
            conn.as_ref(),
            execution_id.clone(),
            logs_storage_config,
            js_info.js_source.clone(),
            js_info.user_ffqn.clone(),
        )
        .await
    } else {
        WorkflowWorker::replay(
            state.deployment_id,
            component_id.clone(),
            replay_info.runnable_component.wasmtime_component.clone(),
            &replay_info.runnable_component.wasm_component.exim,
            state.engines.workflow_engine.clone(),
            Arc::new(state.component_registry_ro.clone()),
            conn.as_ref(),
            execution_id.clone(),
            logs_storage_config,
        )
        .await
    };
    if let Err(err) = replay_res {
        debug!("Replay failed: {err:?}");
        return Err(HttpResponse {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: format!("Replay failed: {err}"),
            accept,
        });
    }
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "replayed".to_string(),
        accept,
    }
    .into_response())
}

/// Payload for upgrading an execution to a new component version
#[derive(Deserialize, ToSchema)]
struct ExecutionUpgradePayload {
    /// Old component digest to upgrade from
    #[schema(value_type = String)]
    pub old: InputContentDigest,
    /// New component digest to upgrade to
    #[schema(value_type = String)]
    pub new: InputContentDigest,
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
        let (component_id, replay_info) = state
            .component_registry_ro
            .get_workflow_replay_info(&payload.new)
            .ok_or_else(|| HttpResponse::not_found(accept, Some("new component")))?;

        let logs_storage_config =
            replay_info
                .logs_store_min_level
                .map(|min_level| LogStrageConfig {
                    min_level,
                    log_sender: state.log_forwarder_sender.clone(),
                });
        let conn = state
            .db_pool
            .connection()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let replay_res = if let Some(js_info) = &replay_info.js_workflow_info {
            WorkflowJsWorker::replay(
                state.deployment_id,
                component_id.clone(),
                replay_info.runnable_component.wasmtime_component.clone(),
                &replay_info.runnable_component.wasm_component.exim,
                state.engines.workflow_engine.clone(),
                Arc::new(state.component_registry_ro.clone()),
                conn.as_ref(),
                execution_id.clone(),
                logs_storage_config,
                js_info.js_source.clone(),
                js_info.user_ffqn.clone(),
            )
            .await
        } else {
            WorkflowWorker::replay(
                state.deployment_id,
                component_id.clone(),
                replay_info.runnable_component.wasmtime_component.clone(),
                &replay_info.runnable_component.wasm_component.exim,
                state.engines.workflow_engine.clone(),
                Arc::new(state.component_registry_ro.clone()),
                conn.as_ref(),
                execution_id.clone(),
                logs_storage_config,
            )
            .await
        };
        if let Err(err) = replay_res {
            debug!("Replay failed: {err:?}");
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
        .upgrade_execution_component(&execution_id, &payload.old, &payload.new)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: if payload.old == payload.new && !payload.skip_determinism_check {
            "replayed"
        } else {
            "upgraded"
        }
        .to_string(),
        accept,
    }
    .into_response())
}

pub(crate) mod components {
    use crate::server::web_api_server::HttpResponse;

    use super::{
        AcceptHeader, Arc, Deserialize, FunctionFqn, IntoParams, IntoResponse, Json, Query,
        Response, Serialize, State, ToSchema, WebApiState,
    };
    use axum::extract::Path;
    use concepts::{
        ComponentId, ComponentType, FunctionExtension, FunctionMetadata, ParameterType,
        component_id::InputContentDigest,
    };
    use http::StatusCode;
    use itertools::Itertools;
    use std::fmt::{Debug, Write as _};

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct ComponentsListParams {
        /// Filter by component type (workflow, activity, webhook)
        #[param(value_type = Option<String>)]
        r#type: Option<ComponentType>,
        /// Filter by component name
        name: Option<String>,
        /// Filter by component digest
        #[param(value_type = Option<String>)]
        digest: Option<InputContentDigest>,
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

    /// Get WIT definition for a component
    #[utoipa::path(
        get,
        path = "/v1/components/{digest}/wit",
        tag = "components",
        params(
            ("digest" = String, Path, description = "Component digest")
        ),
        responses(
            (status = 200, description = "WIT definition", body = String),
            (status = 204, description = "No WIT available"),
            (status = 404, description = "Component not found")
        )
    )]
    pub(crate) async fn component_wit(
        Path(digest): Path<InputContentDigest>,
        state: State<Arc<WebApiState>>,
    ) -> Result<Response, HttpResponse> {
        let Some(wit) = state.component_registry_ro.get_wit(&digest) else {
            return Err(HttpResponse::not_found(
                AcceptHeader::Text,
                Some("component"),
            ));
        };
        Ok(if let Some(wit) = wit {
            wit.to_string().into_response()
        } else {
            (StatusCode::NO_CONTENT, "").into_response()
        })
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
        let mut components = state.component_registry_ro.list(params.extensions);

        if let Some(name) = params.name {
            components.retain(|c| c.component_id.name.as_ref() == name);
        }
        if let Some(digest) = params.digest {
            components.retain(|c| c.component_id.input_digest == digest);
        }
        if let Some(ty) = params.r#type {
            components.retain(|c| c.component_id.component_type == ty);
        }
        let components: Vec<_> = components
            .into_iter()
            .map(|c| {
                let exports = if params.exports {
                    let mut exports = Vec::new();
                    for export in c
                        .workflow_or_activity_config
                        .into_iter()
                        .flat_map(|c| c.exports_ext)
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
            AcceptHeader::Json => Json(components).into_response(),
            AcceptHeader::Text => {
                let mut output = String::new();
                for component in components {
                    writeln!(
                        output,
                        "{} {}",
                        component.component_id, component.component_id.input_digest
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
}

mod functions {
    use super::{
        AcceptHeader, Arc, Deserialize, IntoParams, IntoResponse, Json, Query, Response, State,
        ToSchema, WebApiState,
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
        let all_exports = state.component_registry_ro.all_exports();

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
            AcceptHeader::Json => Json(functions).into_response(),
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
        // Find the component that exports this function
        let Some((component_id, _fn_metadata)) =
            state.component_registry_ro.find_by_exported_ffqn(&ffqn)
        else {
            return Err(HttpResponse::not_found(
                AcceptHeader::Text,
                Some("function"),
            ));
        };

        // Get the WIT for this component
        let wit = state
            .component_registry_ro
            .get_wit(&component_id.input_digest)
            .expect("if function is found, component must be found");
        let Some(wit) = wit else {
            return Err(HttpResponse::not_found(AcceptHeader::Text, Some("wit")));
        };

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
    use crate::server::web_api_server::{AcceptHeader, ErrorWrapper, HttpResponse, WebApiState};
    use axum::{
        Json,
        extract::{Query, State},
        response::{IntoResponse, Response},
    };
    use chrono::Utc;
    use concepts::{
        prefixed_ulid::DeploymentId,
        storage::{DeploymentState, LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH, Pagination},
    };
    use serde::{Deserialize, Serialize};
    use std::{fmt::Write as _, sync::Arc};
    use tracing::instrument;
    use utoipa::{IntoParams, ToSchema};

    /// Deployment state with execution counts
    #[derive(Debug, Serialize, ToSchema)]
    pub struct DeploymentStateSer {
        /// Deployment identifier
        #[schema(value_type = String, example = "Dep_01JKXYZ123456789ABCDEFGHIJ")]
        pub deployment_id: DeploymentId,
        /// Whether this is the current deployment
        pub current: bool,
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
        fn from(deployment_state: &DeploymentState, current_deployment_id: DeploymentId) -> Self {
            Self {
                deployment_id: deployment_state.deployment_id,
                current: deployment_state.deployment_id == current_deployment_id,
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
        let mut states = conn
            .list_deployment_states(Utc::now(), pagination)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        if crate::server::should_add_current_deployment(&pagination, state.deployment_id, &states) {
            states.insert(0, DeploymentState::new(state.deployment_id));
        }

        let states: Vec<DeploymentStateSer> = states
            .into_iter()
            .map(|dep| DeploymentStateSer::from(&dep, state.deployment_id))
            .collect();

        Ok(match accept {
            AcceptHeader::Json => Json(states).into_response(),
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
        Ok(match accept {
            AcceptHeader::Json => Json(state.deployment_id).into_response(),
            AcceptHeader::Text => state.deployment_id.to_string().into_response(),
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
}

impl IntoResponse for HttpResponse {
    fn into_response(self) -> Response {
        match self.accept {
            AcceptHeader::Json => (
                self.status,
                Json(if self.status.is_success() {
                    json!({ "ok": self.message })
                } else {
                    json!({ "err": self.message })
                }),
            )
                .into_response(),
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
