use crate::{
    command::server::{self, ComponentConfigRegistryRO, SubmitError, SubmitOutcome},
    server::web_api_server::components::{component_wit, components_list},
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
    ExecutionId, FinishedExecutionError, FunctionFqn, SupportedFunctionReturnValue,
    component_id::InputContentDigest,
    prefixed_ulid::{DelayId, ExecutionIdDerived},
    storage::{
        self, CancelOutcome, DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite,
        DbErrorWriteNonRetriable, DbPool, ExecutionListPagination, ExecutionRequest,
        ExecutionWithState, ListExecutionsFilter, Pagination, PendingState, TimeoutOutcome,
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
use tracing::{Instrument as _, debug, info_span, trace, warn};
use val_json::{wast_val::WastVal, wast_val_ser::deserialize_value};
use wasm_workers::activity::cancel_registry::CancelRegistry;

#[derive(Clone)]
pub(crate) struct WebApiState {
    pub(crate) db_pool: Arc<dyn DbPool>,
    pub(crate) component_registry_ro: ComponentConfigRegistryRO,
    pub(crate) cancel_registry: CancelRegistry,
    pub(crate) termination_watcher: watch::Receiver<()>,
    pub(crate) subscription_interruption: Option<Duration>,
}

pub(crate) fn app_router(state: WebApiState) -> Router {
    Router::new()
        .nest("/v1", v1_router())
        .with_state(Arc::new(state))
}

fn v1_router() -> Router<Arc<WebApiState>> {
    Router::new()
        .route("/components", routing::get(components_list))
        .route("/components/{digest}/wit", routing::get(component_wit))
        .route("/delays/{delay-id}/cancel", routing::put(delay_cancel))
        .route("/execution-id", routing::get(execution_id_generate))
        .route("/executions", routing::get(executions_list))
        .route("/executions", routing::post(execution_submit_post))
        .route(
            "/executions/{execution-id}/cancel",
            routing::put(execution_cancel),
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
}

async fn execution_id_generate(_: State<Arc<WebApiState>>, accept: AcceptHeader) -> Response {
    let id = ExecutionId::generate();
    match accept {
        AcceptHeader::Json => Json(id).into_response(),
        AcceptHeader::Text => id.to_string().into_response(),
    }
}

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

#[derive(Deserialize, Debug)]
struct ExecutionsListParams {
    ffqn_prefix: Option<String>,
    #[serde(default)]
    show_derived: bool,
    #[serde(default)]
    hide_finished: bool,
    execution_id_prefix: Option<String>,
    // pagination
    cursor: Option<ExecutionListCursorDeser>,
    length: Option<u16>,
    #[serde(default)]
    including_cursor: bool,
    #[serde(default)]
    direction: PaginationDirection,
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum PaginationDirection {
    Older,
    #[default] // everything is newer than 0
    Newer,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum ExecutionListCursorDeser {
    CreatedBy(DateTime<Utc>),
    ExecutionId(ExecutionId),
}

async fn executions_list(
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionsListParams>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    #[derive(Serialize)]
    pub struct ExecutionWithStateSer {
        pub execution_id: ExecutionId,
        pub ffqn: FunctionFqn,
        pub pending_state: PendingState,
        pub created_at: DateTime<Utc>,
        pub first_scheduled_at: DateTime<Utc>,
        pub component_digest: InputContentDigest,
    }
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
                .map(
                    |ExecutionWithState {
                         execution_id,
                         ffqn,
                         pending_state,
                         created_at,
                         first_scheduled_at,
                         component_digest,
                     }| ExecutionWithStateSer {
                        execution_id,
                        ffqn,
                        pending_state,
                        created_at,
                        first_scheduled_at,
                        component_digest,
                    },
                )
                .collect();
            Json(executions).into_response()
        }
    })
}

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

#[derive(Debug, Deserialize)]
struct ExecutionEventsParams {
    #[serde(default)]
    version_from: VersionType,
    length: Option<u8>,
    #[serde(default)]
    include_backtrace_id: bool,
}
async fn execution_events(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionEventsParams>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    const DEFAULT_LENGTH: u8 = 20;
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let events = conn
        .list_execution_events(
            &execution_id,
            &Version(params.version_from),
            params.length.unwrap_or(DEFAULT_LENGTH).into(),
            params.include_backtrace_id,
        )
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(match accept {
        AcceptHeader::Json => Json(events).into_response(),
        AcceptHeader::Text => {
            let mut output = String::new();
            for event in events {
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

    #[derive(Deserialize, Debug)]
    pub(crate) struct ExecutionLogsParams {
        #[serde(default)]
        level: Vec<LogLevelParam>,
        #[serde(default)]
        stream_type: Vec<LogStreamTypeParam>,
        #[serde(default = "default_true")]
        show_logs: bool,
        #[serde(default = "default_true")]
        show_streams: bool,

        // pagination
        cursor: Option<u32>,
        length: Option<u16>,
        #[serde(default)]
        including_cursor: bool,
        #[serde(default)]
        direction: PaginationDirection,
    }

    fn default_true() -> bool {
        true
    }

    #[derive(Debug, Deserialize, Clone, Copy)]
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

    #[derive(Debug, Deserialize, Clone, Copy)]
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

    #[derive(Serialize)]
    struct LogEntryRowSer {
        pub cursor: u32,
        pub run_id: RunId,
        #[serde(flatten)]
        pub info: LogEntrySer,
    }

    #[derive(Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum LogEntrySer {
        Log {
            created_at: DateTime<Utc>,
            level: LogLevelSer,
            message: String,
        },
        Stream {
            created_at: DateTime<Utc>,
            payload: String, // Base64 encoded
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

    #[derive(serde::Serialize, derive_more::Display)]
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

    #[derive(serde::Serialize, derive_more::Display)]
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

#[derive(Debug, Deserialize)]
struct ExecutionResponsesParams {
    #[serde(default)]
    cursor_from: u32,
    length: Option<u16>,
    #[serde(default)]
    including_cursor: bool,
}
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
    let responses = conn
        .list_responses(
            &execution_id,
            Pagination::NewerThan {
                length: params.length.unwrap_or(DEFAULT_LENGTH),
                cursor: params.cursor_from,
                including_cursor: params.including_cursor,
            },
        )
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;

    Ok(match accept {
        AcceptHeader::Json => Json(responses).into_response(),
        AcceptHeader::Text => {
            let mut output = String::new();
            for response in responses {
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
        AcceptHeader::Json => Json(execution_with_state).into_response(),
        AcceptHeader::Text => execution_with_state.to_string().into_response(),
    })
}

#[derive(Deserialize)]
struct ExecutionStubPayload(serde_json::Value);

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

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum RetVal {
    Ok(Option<WastVal>),
    Err(Option<WastVal>), // Mirrors `err` variant of WIT `result`.
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

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ExecutionSubmitPayload {
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct ExecutionFollowParam {
    #[serde(default)]
    follow: bool,
}

async fn execution_submit_put(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionFollowParam>,
    accept: AcceptHeader,
    Json(payload): Json<ExecutionSubmitPayload>,
) -> Result<http::Response<Body>, HttpResponse> {
    execution_submit(execution_id, state, payload, params.follow, accept).await
}

async fn execution_submit_post(
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
    Query(params): Query<ExecutionFollowParam>,
    Json(payload): Json<ExecutionSubmitPayload>,
) -> Result<http::Response<Body>, HttpResponse> {
    let execution_id = ExecutionId::generate();
    execution_submit(execution_id, state, payload, params.follow, accept).await
}

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

#[derive(Deserialize)]
struct ExecutionUpgradePayload {
    pub old: InputContentDigest,
    pub new: InputContentDigest,
}

async fn execution_upgrade(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Json(payload): Json<ExecutionUpgradePayload>,
) -> Result<Response, HttpResponse> {
    state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, AcceptHeader::Json))?
        .upgrade_execution_component(&execution_id, &payload.old, &payload.new)
        .await
        .map_err(|e| ErrorWrapper(e, AcceptHeader::Json))?;
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "upgraded".to_string(),
        accept: AcceptHeader::Json,
    }
    .into_response())
}

pub(crate) mod components {
    use crate::server::web_api_server::HttpResponse;

    use super::{
        AcceptHeader, Arc, Deserialize, FunctionFqn, IntoResponse, Json, Query, Response,
        Serialize, State, WebApiState,
    };
    use axum::extract::Path;
    use concepts::{
        ComponentId, ComponentType, FunctionExtension, FunctionMetadata, ParameterType,
        component_id::InputContentDigest,
    };
    use http::StatusCode;
    use itertools::Itertools;
    use std::fmt::{Debug, Write as _};

    #[derive(Deserialize, Debug)]
    pub(crate) struct ComponentsListParams {
        r#type: Option<ComponentType>,
        name: Option<String>,
        digest: Option<InputContentDigest>,
        #[serde(default)]
        exports: bool,
        #[serde(default)]
        imports: bool,
        #[serde(default)]
        extensions: bool,
        /// If set to true, show only submittable exports.
        submittable: Option<bool>,
    }

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

    #[derive(Serialize)]
    pub(crate) struct ComponentConfig {
        component_id: ComponentId,
        #[serde(skip_serializing_if = "Option::is_none")]
        imports: Option<Vec<FunctionMetadataLite>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        exports: Option<Vec<FunctionMetadataLite>>,
    }

    #[derive(serde::Serialize, derive_more::Display)]
    #[display("{ffqn}: func({}) -> {return_type}", parameter_types.iter().join(", "))]
    struct FunctionMetadataLite {
        ffqn: FunctionFqn,
        parameter_types: Vec<ParameterTypeLite>,
        return_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
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

    #[derive(serde::Serialize, derive_more::Display)]
    #[display("{name}: {wit_type}")]
    struct ParameterTypeLite {
        name: String,
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

#[derive(AcceptExtractor, Clone, Copy)]
pub(crate) enum AcceptHeader {
    #[accept(mediatype = "text/plain")]
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

    fn not_found(accept: AcceptHeader, what: Option<&str>) -> Self {
        HttpResponse {
            status: StatusCode::NOT_FOUND,
            message: if let Some(what) = what {
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
