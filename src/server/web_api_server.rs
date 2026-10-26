use crate::server::request_cancellation;
use crate::{
    command::server::{self, PreparedDirs, ServerVerified, SubmitError, SubmitOutcome},
    server::web_api_server::{
        backtrace::{execution_backtrace, execution_backtrace_source},
        components::{component_wit, components_list},
        functions::{function_wit, functions_list},
    },
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, State},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing,
};
use axum_accept::AcceptExtractor;
use axum_extra::extract::Query;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentType, ExecutionId, FinishedExecutionFailure, FunctionFqn, JoinSetId,
    SupportedFunctionReturnValue,
    component_id::ComponentDigest,
    prefixed_ulid::{DelayId, DeploymentId, ExecutionIdDerived, NodeRunId, SystemEventId},
    storage::{
        self, BacktraceFilter, CancelOutcome, DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout,
        DbErrorWrite, DbErrorWriteNonRetriable, DbExternalApi, DbPool, DelayCancelOutcome,
        ExecutionEvent, ExecutionListPagination, ExecutionRequest, ExecutionStateFilter,
        FunctionNameFilter, ListExecutionsFilter, Pagination, PendingState,
        PendingStateFinishedError, PendingStateFinishedResultKind, TimeoutOutcome, Version,
        VersionType,
    },
    time::{ClockFn as _, Now, Sleep as _},
};
use http::HeaderMap;
use http::{StatusCode, header};
use rest_types::{
    backtrace::BacktraceInfoSer,
    executions::{
        ExecutionEventsAndResponsesResponse, ExecutionEventsResponse, ExecutionResponsesResponse,
        ExecutionSubmitPayload, ExecutionUpgradePayload, ExecutionWithStateSer, RetVal,
    },
    replay::{
        AdvanceErrorSer, AdvanceRequestSer, AdvanceResponseSer, CapturedWriteSer,
        PersistBacktracesResponseSer, ReplayOutcomeSer, ReplayResponseSer,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::convert::Infallible;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    select,
    sync::{mpsc, watch},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument as _, Span, debug, info, info_span, instrument, trace, warn};
use utoipa::{IntoParams, OpenApi, ToSchema};
use val_json::wast_val_ser::deserialize_value;
use wasm_workers::{
    activity::cancel_registry::CancelRegistry,
    registry::ReplayWorker,
    workflow::workflow_worker::{AdvanceError, BacktraceCapture},
};

#[derive(Clone)]
pub(crate) struct WebApiState {
    pub(crate) server_verified: ServerVerified,
    pub(crate) deployment_ctx: crate::command::server::DeploymentContextHandle,
    pub(crate) db_pool: Arc<dyn DbPool>,
    pub(crate) cancel_registry: CancelRegistry,
    pub(crate) termination_watcher: watch::Receiver<()>,
    pub(crate) subscription_interruption: Option<Duration>,
    pub(crate) prepared_dirs: PreparedDirs,
    pub(crate) deployment_switch_manager: crate::command::server::DeploymentSwitchManagerHandle,
}

async fn until_terminated<T>(
    state: &WebApiState,
    accept: AcceptHeader,
    work: impl std::future::Future<Output = T>,
) -> Result<T, HttpResponse> {
    request_cancellation::until_terminated(state.termination_watcher.clone(), work)
        .await
        .map_err(|_| HttpResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "server is shutting down".to_string(),
            accept,
        })
}

/// `OpenAPI` documentation for the Obelisk REST API
#[derive(OpenApi)]
#[openapi(
    modifiers(&LifecycleContract),
    info(
        title = "Obelisk REST API",
        description = "REST API for the Obelisk deterministic workflow engine. Structured resources and actions use JSON. Text remains available for WIT documents, source files, IDs, and app-config digests. Execution logs include raw stdout/stderr payloads in JSON.",
        version = "1.0.0"
    ),
    tags(
        (name = "executions", description = "Execution management"),
        (name = "components", description = "Component management"),
        (name = "functions", description = "Function management"),
        (name = "deployments", description = "Deployment management"),
        (name = "server", description = "Running server identity"),
        (name = "app policy", description = "Running app policy"),
        (name = "admin", description = "Operator-only destructive maintenance"),
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
        execution_event_bounds_batch,
        execution_status_batch,
        execution_events_and_responses,
        logs::execution_logs,
        execution_responses,
        execution_status_get,
        execution_stub,
        execution_get_retval,
        execution_submit_put,
        execution_submit_post,
        execution_replay,
        execution_advance,
        execution_persist_backtraces,
        execution_upgrade,
        backtrace::execution_backtrace,
        backtrace::execution_backtrace_source,
        backtrace::component_source,
        components::component_wit,
        components::components_list,
        functions::functions_list,
        functions::function_wit,
        deployment::list,
        deployment::current,
        deployment::get,
        deployment::submit_post,
        deployment::submit_put,
        deployment::get_file,
        app_config::get,
        app_config::digest,
        admin::delete_execution_tree,
        admin::retain_executions,
        admin::delete_deployment,
        admin::retain_deployments,
        admin::list_system_events,
        admin::get_system_event,
        admin::storage_status,
        admin::node_run_id,
        admin::retain_system_events,
    ),
    components(schemas(
        PaginationDirectionSortedFromLatest,
        PaginationDirectionSortedFromOldest,
        ExecutionWithStateSer,
        ExecutionEventsResponse,
        ExecutionEventBoundsBatchRequest,
        ExecutionEventBoundsBatchItem,
        ExecutionStatusBatchRequest,
        ExecutionStatusBatchItem,
        ExecutionEventsAndResponsesResponse,
        ExecutionResponsesResponse,
        ExecutionStubPayload,
        RetVal,
        ReplayResponseSer,
        AdvanceRequestSer,
        AdvanceResponseSer,
        PersistBacktracesResponseSer,
        ExecutionSubmitPayload,
        ExecutionUpgradePayload,
        rest_types::logs::LogEntryRowSer,
        rest_types::logs::LogEntrySer,
        rest_types::logs::LogLevelSer,
        rest_types::logs::LogStreamTypeSer,
        logs::LogLevelParam,
        logs::LogStreamTypeParam,
        rest_types::components::ComponentConfig,
        rest_types::components::FunctionMetadataLite,
        rest_types::components::ParameterTypeLite,
        rest_types::functions::FunctionOutput,
        rest_types::deployment::DeploymentStateSer,
        rest_types::deployment::DeploymentSubmitResponse,
        rest_types::admin::DeleteResponse,
        rest_types::admin::DeleteDeploymentResponse,
        rest_types::admin::CleanupRequest,
        rest_types::admin::RetainDeploymentsRequest,
        rest_types::admin::CleanupResponse,
        rest_types::admin::SystemEventsResponse,
        rest_types::admin::StorageStatusResponse,
        rest_types::admin::RetainSystemEventsRequest,
        rest_types::admin::RetainSystemEventsResponse,
        rest_types::deployment::DeploymentSubmitErrorBody,
        app_config::AppConfigResponse,
        rest_types::deployment::GenericErrorBody,
        rest_types::deployment::SubmitPackageErrorBody,
        rest_types::deployment::MissingRuntimeConfigErrorBody,
        rest_types::deployment::FileIssue,
        rest_types::deployment::DigestMismatch,
        BacktraceInfoSer,
    ))
)]
pub(crate) struct ApiDoc;

struct LifecycleContract;

impl utoipa::Modify for LifecycleContract {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let contract = rest_types::lifecycle::openapi();
        for path in contract.paths.paths.keys() {
            assert!(
                !openapi.paths.paths.contains_key(path),
                "lifecycle paths must come from OpenAPI"
            );
        }
        openapi.merge(contract);
    }
}

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
    let max_transport_message_size_bytes = state.server_verified.max_transport_message_size_bytes();
    Router::new()
        .route("/openapi.json", routing::get(openapi_json))
        .nest("/v1", v1_router(max_transport_message_size_bytes))
        .with_state(Arc::new(state))
}

#[expect(deprecated)]
fn v1_router(max_transport_message_size_bytes: usize) -> Router<Arc<WebApiState>> {
    Router::new()
        .route("/components", routing::get(components_list))
        .route("/components/{digest}/wit", routing::get(component_wit))
        .route(
            "/components/{digest}/source",
            routing::get(backtrace::component_source),
        )
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
            "/executions/events/batch",
            routing::post(execution_event_bounds_batch),
        )
        .route(
            "/executions/status/batch",
            routing::post(execution_status_batch),
        )
        .route(
            "/executions/{execution-id}/events-and-responses",
            routing::get(execution_events_and_responses),
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
            "/executions/{execution-id}/backtrace/persist",
            routing::put(execution_persist_backtraces),
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
        .route("/deployments", routing::get(deployment::list))
        .route("/deployments", routing::post(deployment::submit_post))
        .route(
            "/deployments/{deployment-id}",
            routing::get(deployment::get).put(deployment::submit_put),
        )
        .nest("/admin", admin_router())
        .route("/files/{digest}", routing::get(deployment::get_file))
        .route(
            "/deployments/{deployment-id}/switch",
            routing::put(deployment::switch),
        )
        .route("/deployment-id", routing::get(deployment::current))
        .route("/app-policy", routing::get(app_config::policy))
        .route("/app-config", routing::get(app_config::get))
        .route("/app-config-digest", routing::get(app_config::digest))
        .route("/info", routing::get(server_info::get))
        .route(
            "/executions/{execution-id}/backtrace",
            routing::get(execution_backtrace),
        )
        .route(
            "/executions/{execution-id}/backtrace/source",
            routing::get(execution_backtrace_source),
        )
        .layer(DefaultBodyLimit::max(max_transport_message_size_bytes))
}

#[expect(deprecated)]
fn admin_router() -> Router<Arc<WebApiState>> {
    Router::new()
        .route(
            "/executions/{execution-id}",
            routing::delete(admin::delete_execution_tree),
        )
        .route(
            "/executions/retain",
            routing::post(admin::retain_executions),
        )
        .route(
            "/deployments/{deployment-id}",
            routing::delete(admin::delete_deployment),
        )
        .route(
            "/deployments/retain",
            routing::post(admin::retain_deployments),
        )
        .route("/system-events", routing::get(admin::list_system_events))
        .route(
            "/system-events/{event-id}",
            routing::get(admin::get_system_event),
        )
        .route(
            "/system-events/retain",
            routing::post(admin::retain_system_events),
        )
        .route("/storage", routing::get(admin::storage_status))
        .route("/node-run-id", routing::get(admin::node_run_id))
}

pub(crate) mod app_config {
    use super::*;
    #[derive(Serialize, ToSchema)]
    pub(crate) struct AppConfigResponse<'a> {
        #[schema(value_type = String)]
        pub(crate) app_config_digest: &'a crate::config::app::AppConfigDigest,
        #[schema(value_type = rest_types::lifecycle::AppPolicySchema)]
        pub(crate) policy: &'a crate::config::app::AppPolicyV1,
    }

    #[derive(AcceptExtractor, Clone, Copy, Default)]
    pub(crate) enum AppConfigAcceptHeader {
        #[accept(mediatype = "application/json")]
        #[default]
        Json,
        #[accept(mediatype = "application/toml")]
        Toml,
    }

    fn json_or_toml(accept: AppConfigAcceptHeader, body: &impl Serialize) -> Response {
        match accept {
            AppConfigAcceptHeader::Json => (StatusCode::OK, Json(body)).into_response(),
            AppConfigAcceptHeader::Toml => match toml::to_string_pretty(body) {
                Ok(toml) => (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/toml")],
                    toml,
                )
                    .into_response(),
                Err(err) => HttpResponse {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: format!("cannot serialize app policy as TOML: {err}"),
                    accept: AcceptHeader::Json,
                }
                .into_response(),
            },
        }
    }

    pub(crate) async fn policy(
        State(state): State<Arc<WebApiState>>,
        accept: AppConfigAcceptHeader,
    ) -> Response {
        match accept {
            AppConfigAcceptHeader::Json => {
                let policy =
                    rest_types::lifecycle::AppPolicyV1::from(&state.server_verified.app_policy);
                json_or_toml(accept, &policy)
            }
            AppConfigAcceptHeader::Toml => json_or_toml(accept, &state.server_verified.app_policy),
        }
    }

    #[utoipa::path(get, path = "/v1/app-config", tag = "app policy", responses((status = 200, content((AppConfigResponse = "application/json"), (String = "application/toml")))))]
    // backcompat: 0.43, delete this endpoint after 0.43 in favor of `GET /v1/app-policy`.
    #[deprecated(note = "use `GET /v1/app-policy` and `app_config_digest` from `GET /v1/info`")]
    pub(crate) async fn get(
        State(state): State<Arc<WebApiState>>,
        accept: AppConfigAcceptHeader,
    ) -> Response {
        let body = AppConfigResponse {
            app_config_digest: &state.server_verified.app_config_digest,
            policy: &state.server_verified.app_policy,
        };
        json_or_toml(accept, &body)
    }

    #[utoipa::path(get, path = "/v1/app-config-digest", tag = "app policy", responses((status = 200, content((String = "text/plain"), (String = "application/json")))))]
    // backcompat: 0.43, delete this endpoint after 0.43 in favor of `GET /v1/info`.
    #[deprecated(note = "use `app_config_digest` from `GET /v1/info`")]
    pub(crate) async fn digest(
        State(state): State<Arc<WebApiState>>,
        accept: TextDefaultAcceptHeader,
    ) -> Response {
        let digest = &state.server_verified.app_config_digest;
        match accept.into() {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, digest),
            AcceptHeader::Text => digest.to_string().into_response(),
        }
    }
}

pub(crate) mod server_info {
    use super::{Arc, DateTime, Response, State, StatusCode, WebApiState, pretty_json_response};
    use crate::args::shadow::PKG_VERSION;
    use rest_types::server::ServerInfoResponse;

    pub(crate) async fn get(State(state): State<Arc<WebApiState>>) -> Response {
        let node_run_id = concepts::storage::initialize_node_run_id();
        let started_at = i64::try_from(node_run_id.timestamp_part())
            .ok()
            .and_then(DateTime::from_timestamp_millis)
            .expect("node run ID timestamp is generated from the current time");
        let body = ServerInfoResponse {
            app_name: state.server_verified.app_name.clone(),
            app_env_prefix: state.server_verified.app_env_prefix.clone(),
            obelisk_version: PKG_VERSION.to_string(),
            deployment_id: state.deployment_ctx.read().await.deployment_id.to_string(),
            node_run_id: node_run_id.to_string(),
            app_config_digest: state.server_verified.app_config_digest.to_string(),
            started_at,
        };
        pretty_json_response(StatusCode::OK, &body)
    }
}

pub(crate) mod admin {
    use super::*;
    use concepts::storage::{DeleteDeploymentResult, DeleteExecutionTreeResult, SystemEventCode};
    use rest_types::admin::{
        CleanupRequest, CleanupResponse, DeleteDeploymentResponse, DeleteResponse,
        RetainDeploymentsRequest, RetainSystemEventsRequest, RetainSystemEventsResponse,
        StorageStatusResponse, SystemEventResponse, SystemEventsResponse,
    };

    #[derive(Debug, Default, Deserialize, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct SystemEventsQuery {
        #[param(value_type = Option<String>)]
        node_run_id: Option<NodeRunId>,
        level: Option<String>,
        code: Option<String>,
        #[param(value_type = Option<String>)]
        deployment_id: Option<DeploymentId>,
        #[param(value_type = Option<String>)]
        before: Option<SystemEventId>,
        /// Inclusive lower bound of `created_at` (RFC 3339, millisecond precision).
        created_from: Option<DateTime<Utc>>,
        /// Exclusive upper bound of `created_at` (RFC 3339, millisecond precision).
        created_to: Option<DateTime<Utc>>,
        limit: Option<u32>,
    }

    #[utoipa::path(get, path = "/v1/admin/system-events", tag = "admin", params(SystemEventsQuery), responses((status = 200, body = SystemEventsResponse)))]
    pub(crate) async fn list_system_events(
        Query(query): Query<SystemEventsQuery>,
        State(state): State<Arc<WebApiState>>,
    ) -> Result<Response, HttpResponse> {
        let level = query
            .level
            .as_deref()
            .map(|level| match level {
                "debug" => Ok(storage::SystemEventLevel::Debug),
                "info" => Ok(storage::SystemEventLevel::Info),
                "warning" => Ok(storage::SystemEventLevel::Warning),
                "error" => Ok(storage::SystemEventLevel::Error),
                _ => Err(precondition("level must be debug, info, warning, or error")),
            })
            .transpose()?;
        let limit = query.limit.unwrap_or(100);
        if limit == 0 || limit > 1000 {
            return Err(precondition("limit must be between 1 and 1000"));
        }
        let mut events = state
            .db_pool
            .admin_conn()
            .await
            .map_err(|err| ErrorWrapper(err, AcceptHeader::Json))?
            .list_system_events(storage::SystemEventFilter {
                event_id: None,
                node_run_id: query.node_run_id,
                level,
                code: query.code,
                deployment_id: query.deployment_id,
                before_event_id: query.before,
                created_from: query.created_from,
                created_to: query.created_to,
                limit,
            })
            .await
            .map_err(|err| ErrorWrapper(err, AcceptHeader::Json))?;
        crate::server::system_event_writer::hydrate_cas_details(
            state.db_pool.as_ref(),
            &mut events,
        )
        .await;
        let next_cursor =
            (events.len() == limit as usize).then(|| events.last().unwrap().event_id.to_string());
        let events = events
            .into_iter()
            .map(|event| {
                let message = event.message().to_owned();
                SystemEventResponse {
                    event_id: event.event_id,
                    node_run_id: event.node_run_id,
                    app_config_digest: event.app_config_digest,
                    created_at: event.created_at,
                    level: event.level,
                    code: event.code,
                    message,
                    execution_id: event.execution_id,
                    deployment_id: event.deployment_id,
                    details: event.details,
                }
            })
            .collect();
        Ok(pretty_json_response(
            StatusCode::OK,
            &SystemEventsResponse {
                events,
                next_cursor,
            },
        ))
    }

    #[utoipa::path(get, path = "/v1/admin/system-events/{event_id}", tag = "admin", responses((status = 200, body = SystemEventResponse), (status = 404)))]
    pub(crate) async fn get_system_event(
        Path(event_id): Path<SystemEventId>,
        State(state): State<Arc<WebApiState>>,
    ) -> Result<Response, HttpResponse> {
        let event = state
            .db_pool
            .admin_conn()
            .await
            .map_err(|err| ErrorWrapper(err, AcceptHeader::Json))?
            .get_system_event(event_id)
            .await
            .map_err(|err| ErrorWrapper(err, AcceptHeader::Json))?
            .ok_or_else(|| HttpResponse::not_found(AcceptHeader::Json, "system event"))?;
        let mut events = [event];
        crate::server::system_event_writer::hydrate_cas_details(
            state.db_pool.as_ref(),
            &mut events,
        )
        .await;
        let event = events.into_iter().next().unwrap();
        let message = event.message().to_owned();
        let response = SystemEventResponse {
            event_id: event.event_id,
            node_run_id: event.node_run_id,
            app_config_digest: event.app_config_digest,
            created_at: event.created_at,
            level: event.level,
            code: event.code,
            message,
            execution_id: event.execution_id,
            deployment_id: event.deployment_id,
            details: event.details,
        };
        Ok(pretty_json_response(StatusCode::OK, &response))
    }

    #[utoipa::path(get, path = "/v1/admin/storage", tag = "admin", responses((status = 200, body = StorageStatusResponse)))]
    pub(crate) async fn storage_status(
        State(state): State<Arc<WebApiState>>,
    ) -> Result<Response, HttpResponse> {
        let status = state
            .db_pool
            .admin_conn()
            .await
            .map_err(|err| ErrorWrapper(err, AcceptHeader::Json))?
            .get_storage_status()
            .await
            .map_err(|err| ErrorWrapper(err, AcceptHeader::Json))?;
        Ok(pretty_json_response(
            StatusCode::OK,
            &StorageStatusResponse {
                database_bytes: status.database_bytes,
                execution_count: status.execution_count,
                deployment_count: status.deployment_count,
                system_event_count: status.system_event_count,
            },
        ))
    }

    #[utoipa::path(
        get,
        path = "/v1/admin/node-run-id",
        tag = "admin",
        responses((
            status = 200,
            content((String = "text/plain"), (String = "application/json"))
        ))
    )]
    // backcompat: 0.43, delete this endpoint after 0.43 in favor of `GET /v1/info`.
    #[deprecated(note = "use `GET /v1/info`")]
    pub(crate) async fn node_run_id(accept: TextDefaultAcceptHeader) -> Response {
        let id = concepts::storage::initialize_node_run_id();
        match accept.into() {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &id),
            AcceptHeader::Text => id.to_string().into_response(),
        }
    }

    #[utoipa::path(post, path = "/v1/admin/system-events/retain", tag = "admin", request_body = RetainSystemEventsRequest, responses((status = 200, body = RetainSystemEventsResponse)))]
    pub(crate) async fn retain_system_events(
        State(state): State<Arc<WebApiState>>,
        Json(request): Json<RetainSystemEventsRequest>,
    ) -> Result<Response, HttpResponse> {
        validate_batch_size(request.batch_size)?;
        if request.max_age_seconds == 0 {
            return Err(precondition("max_age_seconds must be greater than zero"));
        }
        let age = chrono::Duration::from_std(Duration::from_secs(request.max_age_seconds))
            .map_err(|_| precondition("max_age_seconds is too large"))?;
        let cutoff = Utc::now()
            .checked_sub_signed(age)
            .ok_or_else(|| precondition("max_age_seconds is too large"))?;
        let result = until_terminated(&state, AcceptHeader::Json, async {
            state
                .db_pool
                .admin_conn()
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))?
                .retain_system_events(cutoff, request.batch_size, request.dry_run)
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))
        })
        .await??;
        Ok(pretty_json_response(
            StatusCode::OK,
            &RetainSystemEventsResponse {
                deleted: result.deleted,
                has_more: result.has_more,
            },
        ))
    }

    fn validate_batch_size(batch_size: u32) -> Result<(), HttpResponse> {
        if batch_size == 0 || batch_size > 10_000 {
            Err(HttpResponse {
                status: StatusCode::BAD_REQUEST,
                message: "batch_size must be between 1 and 10000".into(),
                accept: AcceptHeader::Json,
            })
        } else {
            Ok(())
        }
    }

    fn precondition(message: impl Into<String>) -> HttpResponse {
        HttpResponse {
            status: StatusCode::PRECONDITION_FAILED,
            message: message.into(),
            accept: AcceptHeader::Json,
        }
    }

    fn retention_policy(
        retain_count: Option<u32>,
        max_age_seconds: Option<u64>,
    ) -> Result<storage::RetentionPolicy, HttpResponse> {
        match (retain_count, max_age_seconds) {
            (Some(count), None) => Ok(storage::RetentionPolicy::Count(count)),
            (None, Some(seconds)) if seconds > 0 => {
                let age = chrono::Duration::from_std(std::time::Duration::from_secs(seconds))
                    .map_err(|_| precondition("max_age_seconds is too large"))?;
                let cutoff = chrono::Utc::now()
                    .checked_sub_signed(age)
                    .ok_or_else(|| precondition("max_age_seconds is too large"))?;
                Ok(storage::RetentionPolicy::CreatedAtOrAfter(cutoff))
            }
            (None, Some(_)) => Err(precondition("max_age_seconds must be greater than zero")),
            _ => Err(precondition(
                "exactly one of retain_count or max_age_seconds is required",
            )),
        }
    }

    #[derive(Debug, Default, Deserialize, IntoParams)]
    pub(crate) struct DeleteExecutionQuery {
        #[serde(default)]
        force_non_terminal: bool,
    }

    #[utoipa::path(delete, path = "/v1/admin/executions/{execution_id}", tag = "admin", params(DeleteExecutionQuery), responses((status = 200, body = DeleteResponse)))]
    pub(crate) async fn delete_execution_tree(
        Path(execution_id): Path<ExecutionId>,
        Query(query): Query<DeleteExecutionQuery>,
        State(state): State<Arc<WebApiState>>,
    ) -> Result<Response, HttpResponse> {
        if !execution_id.is_top_level() {
            return Err(precondition(format!(
                "execution must be top-level; top-level execution is {}",
                execution_id.get_top_level()
            )));
        }
        crate::server::system_event_writer::record(
            state.db_pool.as_ref(),
            SystemEventCode::AdminExecutionDeleteStarted,
            Some(execution_id.clone()),
            None,
            json!({"force_non_terminal": query.force_non_terminal}),
        )
        .await;
        let outcome = until_terminated(&state, AcceptHeader::Json, async {
            state
                .db_pool
                .admin_conn()
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))?
                .delete_execution_tree(&execution_id, query.force_non_terminal)
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))
        })
        .await??;
        let response = match outcome {
            DeleteExecutionTreeResult::Deleted => DeleteResponse {
                deleted: true,
                already_deleted: false,
            },
            DeleteExecutionTreeResult::AlreadyDeleted => DeleteResponse {
                deleted: false,
                already_deleted: true,
            },
            DeleteExecutionTreeResult::NonTerminal => {
                return Err(precondition("execution tree is not terminal"));
            }
            DeleteExecutionTreeResult::ActiveDeployment => {
                return Err(precondition(
                    "non-terminal execution tree root belongs to the active deployment",
                ));
            }
        };
        crate::server::system_event_writer::record(
            state.db_pool.as_ref(),
            SystemEventCode::AdminExecutionDeleteCompleted,
            Some(execution_id),
            None,
            json!({"deleted": response.deleted, "already_deleted": response.already_deleted}),
        )
        .await;
        Ok(pretty_json_response(StatusCode::OK, &response))
    }

    #[utoipa::path(post, path = "/v1/admin/executions/retain", tag = "admin", request_body = CleanupRequest, responses((status = 200, body = CleanupResponse)))]
    pub(crate) async fn retain_executions(
        State(state): State<Arc<WebApiState>>,
        Json(request): Json<CleanupRequest>,
    ) -> Result<Response, HttpResponse> {
        validate_batch_size(request.batch_size)?;
        let retention = retention_policy(request.retain_count, request.max_age_seconds)?;
        crate::server::system_event_writer::record(state.db_pool.as_ref(), SystemEventCode::AdminExecutionRetainStarted, None, None, json!({"retain_count": request.retain_count, "max_age_seconds": request.max_age_seconds, "batch_size": request.batch_size, "force_non_terminal": request.force_non_terminal, "dry_run": request.dry_run})).await;
        let result = until_terminated(&state, AcceptHeader::Json, async {
            state
                .db_pool
                .admin_conn()
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))?
                .retain_executions(
                    retention,
                    request.batch_size,
                    request.force_non_terminal,
                    request.dry_run,
                )
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))
        })
        .await??;
        crate::server::system_event_writer::record(
            state.db_pool.as_ref(),
            SystemEventCode::AdminExecutionRetainCompleted,
            None,
            None,
            serde_json::to_value(CleanupResponse::from(result)).unwrap(),
        )
        .await;
        Ok(pretty_json_response(
            StatusCode::OK,
            &CleanupResponse::from(result),
        ))
    }

    #[derive(Debug, Default, Deserialize, IntoParams)]
    pub(crate) struct DeleteDeploymentQuery {
        #[serde(default)]
        delete_executions: bool,
        #[serde(default)]
        force_non_terminal: bool,
    }

    #[utoipa::path(delete, path = "/v1/admin/deployments/{deployment_id}", tag = "admin", params(DeleteDeploymentQuery), responses((status = 200, body = DeleteDeploymentResponse)))]
    pub(crate) async fn delete_deployment(
        Path(deployment_id): Path<DeploymentId>,
        Query(query): Query<DeleteDeploymentQuery>,
        State(state): State<Arc<WebApiState>>,
    ) -> Result<Response, HttpResponse> {
        if query.force_non_terminal && !query.delete_executions {
            return Err(precondition(
                "force_non_terminal requires delete_executions",
            ));
        }
        crate::server::system_event_writer::record(
            state.db_pool.as_ref(),
            SystemEventCode::AdminDeploymentDeleteStarted,
            None,
            Some(deployment_id),
            json!({
                "delete_executions": query.delete_executions,
                "force_non_terminal": query.force_non_terminal,
            }),
        )
        .await;
        let outcome = until_terminated(&state, AcceptHeader::Json, async {
            state
                .db_pool
                .admin_conn()
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))?
                .delete_deployment(
                    deployment_id,
                    query.delete_executions,
                    query.force_non_terminal,
                )
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))
        })
        .await??;
        let response = match outcome {
            DeleteDeploymentResult::Deleted {
                deleted_execution_trees,
            } => DeleteDeploymentResponse {
                deleted: true,
                already_deleted: false,
                deleted_execution_trees,
            },
            DeleteDeploymentResult::AlreadyDeleted => DeleteDeploymentResponse {
                deleted: false,
                already_deleted: true,
                deleted_execution_trees: 0,
            },
            DeleteDeploymentResult::Active => {
                return Err(precondition("active deployment cannot be deleted"));
            }
            DeleteDeploymentResult::Enqueued => {
                return Err(precondition("enqueued deployment cannot be deleted"));
            }
            DeleteDeploymentResult::Referenced { execution_trees } => {
                return Err(precondition(format!(
                    "deployment is referenced by {execution_trees} execution tree(s)"
                )));
            }
            DeleteDeploymentResult::ReferencedByNonTerminal { execution_trees } => {
                return Err(precondition(format!(
                    "deployment is referenced by non-terminal executions in {execution_trees} tree(s)"
                )));
            }
            DeleteDeploymentResult::ReferencedByActiveDeployment { execution_trees } => {
                return Err(precondition(format!(
                    "non-terminal execution roots in {execution_trees} tree(s) belong to the active deployment"
                )));
            }
        };
        crate::server::system_event_writer::record(
            state.db_pool.as_ref(),
            SystemEventCode::AdminDeploymentDeleteCompleted,
            None,
            Some(deployment_id),
            serde_json::to_value(&response).expect("cleanup response must serialize"),
        )
        .await;
        Ok(pretty_json_response(StatusCode::OK, &response))
    }

    #[utoipa::path(post, path = "/v1/admin/deployments/retain", tag = "admin", request_body = RetainDeploymentsRequest, responses((status = 200, body = CleanupResponse)))]
    pub(crate) async fn retain_deployments(
        State(state): State<Arc<WebApiState>>,
        Json(request): Json<RetainDeploymentsRequest>,
    ) -> Result<Response, HttpResponse> {
        validate_batch_size(request.batch_size)?;
        let retention = retention_policy(request.retain_count, request.max_age_seconds)?;
        if request.force_non_terminal && !request.delete_executions {
            return Err(precondition(
                "force_non_terminal requires delete_executions",
            ));
        }
        crate::server::system_event_writer::record(
            state.db_pool.as_ref(),
            SystemEventCode::AdminDeploymentRetainStarted,
            None,
            None,
            json!({
                "retain_count": request.retain_count,
                "max_age_seconds": request.max_age_seconds,
                "batch_size": request.batch_size,
                "delete_executions": request.delete_executions,
                "force_non_terminal": request.force_non_terminal,
                "dry_run": request.dry_run,
            }),
        )
        .await;
        let result = until_terminated(&state, AcceptHeader::Json, async {
            state
                .db_pool
                .admin_conn()
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))?
                .retain_deployments(
                    retention,
                    request.batch_size,
                    request.delete_executions,
                    request.force_non_terminal,
                    request.dry_run,
                )
                .await
                .map_err(|err| HttpResponse::from(ErrorWrapper(err, AcceptHeader::Json)))
        })
        .await??;
        crate::server::system_event_writer::record(
            state.db_pool.as_ref(),
            SystemEventCode::AdminDeploymentRetainCompleted,
            None,
            None,
            serde_json::to_value(CleanupResponse::from(result))
                .expect("cleanup response must serialize"),
        )
        .await;
        Ok(pretty_json_response(
            StatusCode::OK,
            &CleanupResponse::from(result),
        ))
    }
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
// backcompat: 0.43, delete this endpoint after 0.43 in favor of local ID generation.
#[deprecated(note = "generate execution IDs locally, e.g. `obelisk generate execution-id`")]
async fn execution_id_generate(
    _: State<Arc<WebApiState>>,
    accept: TextDefaultAcceptHeader,
) -> Response {
    let accept = accept.into();
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
    Ok(HttpResponse::from_delay_cancel_outcome(outcome, accept).into_response())
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
    /// Exact package scope, for example `ns:pkg`
    package: Option<String>,
    /// Exact interface scope, for example `ns:pkg/ifc`
    interface: Option<String>,
    /// Exact function name
    function: Option<String>,
    /// Match any of these states
    #[serde(default)]
    state: Vec<ExecutionStateParam>,
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
    #[param(minimum = 1)]
    length: Option<u16>,
    /// Include the cursor item in results
    #[serde(default)]
    including_cursor: bool,
    /// Pagination direction
    #[serde(default)]
    direction: PaginationDirectionSortedFromLatest,
}

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum ExecutionStateParam {
    Locked,
    Pending,
    Scheduled,
    Blocked,
    Paused,
    Cancelling,
    Finished,
    FinishedOk,
    FinishedError,
    FinishedExecutionFailure,
}

impl ExecutionStateParam {
    fn into_filter(self, now: DateTime<Utc>) -> ExecutionStateFilter {
        match self {
            Self::Locked => ExecutionStateFilter::Locked,
            Self::Pending => ExecutionStateFilter::Pending { now },
            Self::Scheduled => ExecutionStateFilter::Scheduled { now },
            Self::Blocked => ExecutionStateFilter::Blocked,
            Self::Paused => ExecutionStateFilter::Paused,
            Self::Cancelling => ExecutionStateFilter::Cancelling,
            Self::Finished => ExecutionStateFilter::Finished,
            Self::FinishedOk => ExecutionStateFilter::FinishedOk,
            Self::FinishedError => ExecutionStateFilter::FinishedError,
            Self::FinishedExecutionFailure => ExecutionStateFilter::FinishedExecutionFailure,
        }
    }
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

fn nonzero_page_length(length: u16, accept: AcceptHeader) -> Result<NonZeroU16, HttpResponse> {
    NonZeroU16::new(length).ok_or_else(|| {
        HttpResponse::bad_request(accept, "`length` must be greater than zero".to_string())
    })
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum ExecutionListCursorDeser {
    CreatedBy(DateTime<Utc>),
    ExecutionId(ExecutionId),
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
    let scopes = [
        params.ffqn_prefix.is_some(),
        params.package.is_some(),
        params.interface.is_some(),
        params.function.is_some(),
    ];
    if scopes.into_iter().filter(|set| *set).count() > 1 {
        return Err(HttpResponse::bad_request(
            accept,
            "choose one function scope".to_string(),
        ));
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
        let length = nonzero_page_length(length.unwrap_or(default_pagination.length()), accept)?;
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

    let now = Utc::now();
    let filter = ListExecutionsFilter {
        function_name_filter: params
            .package
            .map(FunctionNameFilter::PackageName)
            .or_else(|| params.interface.map(FunctionNameFilter::InterfaceName))
            .or_else(|| params.function.map(FunctionNameFilter::FunctionName))
            .or_else(|| params.ffqn_prefix.map(FunctionNameFilter::Prefix)),
        show_derived: params.show_derived,
        hide_finished: params.hide_finished,
        execution_id_prefix: params.execution_id_prefix,
        component_digest: params.component_digest,
        deployment_id: params.deployment_id,
        state_filters: params
            .state
            .into_iter()
            .map(|state| state.into_filter(now))
            .collect(),
    };

    let executions = conn
        .list_executions(filter, pagination)
        .await
        .map_err(|err| ErrorWrapper(err, accept))?;
    Ok({
        let executions: Vec<_> = executions
            .into_iter()
            .map(ExecutionWithStateSer::from)
            .collect();
        pretty_json_response(StatusCode::OK, &executions)
    })
}

/// Cancel an activity or cancellable workflow execution
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/cancel",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID to cancel")
    ),
    responses(
        (status = 200, description = "Cancellation requested"),
        (status = 409, description = "Already finished or already cancelling"),
        (status = 422, description = "Not an activity or cancellable workflow")
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
    let executed_at = Now.now();
    let outcome = match create_req.component_id.component_type {
        component_type if component_type.is_activity() => {
            state
                .cancel_registry
                .request_activity_cancellation(conn.as_ref(), &execution_id, executed_at)
                .await
        }
        ComponentType::Workflow => {
            let outcome = conn
                .append_cancel_workflow_requested_with_retries(&execution_id, executed_at)
                .await;
            if outcome.is_ok() {
                // Best-effort CPU saver: the write above already cancels it; a locally-running
                // workflow would otherwise burn CPU until its next db append fails on that write.
                state
                    .cancel_registry
                    .signal_workflow_interrupt(&execution_id);
            }
            outcome
        }
        _ => {
            return Err(HttpResponse {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                message: "cancelled execution must be an activity or cancellable workflow"
                    .to_string(),
                accept,
            });
        }
    }
    .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse::from_cancel_execution_outcome(outcome, accept).into_response())
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
    // Best-effort CPU saver: the write above already pauses it; a locally-running
    // workflow would otherwise burn CPU until its next db append fails on that write.
    state
        .cancel_registry
        .signal_workflow_interrupt(&execution_id);
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
    #[param(minimum = 1)]
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

#[derive(Deserialize, ToSchema)]
struct ExecutionEventBoundsBatchRequest {
    #[schema(value_type = Vec<String>)]
    execution_ids: Vec<ExecutionId>,
}

#[derive(Serialize, ToSchema)]
struct ExecutionEventBoundsBatchItem {
    #[schema(value_type = String)]
    execution_id: ExecutionId,
    #[schema(value_type = Object)]
    created: ExecutionEvent,
    #[schema(value_type = Option<Object>)]
    finished: Option<ExecutionEvent>,
}

#[utoipa::path(
    post,
    path = "/v1/executions/events/batch",
    tag = "executions",
    request_body = ExecutionEventBoundsBatchRequest,
    responses((status = 200, body = Vec<ExecutionEventBoundsBatchItem>))
)]
async fn execution_event_bounds_batch(
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
    Json(request): Json<ExecutionEventBoundsBatchRequest>,
) -> Result<Response, HttpResponse> {
    if request.execution_ids.is_empty() {
        return Err(HttpResponse {
            status: StatusCode::BAD_REQUEST,
            message: "execution_ids must contain at least one ID".to_string(),
            accept,
        });
    }
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let bounds = conn
        .get_execution_event_bounds_batch(&request.execution_ids)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let response: Vec<_> = bounds
        .into_iter()
        .map(|row| ExecutionEventBoundsBatchItem {
            execution_id: row.execution_id,
            created: row.created,
            finished: row.finished,
        })
        .collect();
    Ok(pretty_json_response(StatusCode::OK, &response))
}

/// Most executions one status batch may list.
const STATUS_BATCH_MAX_IDS: usize = 200;

#[derive(Deserialize, ToSchema)]
struct ExecutionStatusBatchRequest {
    /// At most 200 executions, duplicates are ignored. Missing executions are left out of the
    /// response.
    #[schema(value_type = Vec<String>)]
    execution_ids: Vec<ExecutionId>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ExecutionStatusBatchParams {
    /// Stream SSE `statuses` events: first all listed executions, then only the ones whose
    /// status or `max_version` changed. Missing executions are left out of the first event and
    /// are not followed. Finished executions are no longer followed and the stream ends when
    /// none are left.
    #[serde(default)]
    follow: bool,
}

#[derive(Clone, PartialEq, Serialize, ToSchema)]
struct ExecutionStatusBatchItem {
    #[serde(flatten)]
    status: ExecutionWithStateSer,
    #[schema(value_type = u32)]
    max_version: Version,
}

impl From<concepts::storage::ExecutionStateWithVersion> for ExecutionStatusBatchItem {
    fn from(state: concepts::storage::ExecutionStateWithVersion) -> Self {
        Self {
            status: state.execution_with_state.into(),
            max_version: state.max_version,
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/executions/status/batch",
    tag = "executions",
    params(ExecutionStatusBatchParams),
    request_body = ExecutionStatusBatchRequest,
    responses((status = 200, body = Vec<ExecutionStatusBatchItem>))
)]
async fn execution_status_batch(
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionStatusBatchParams>,
    headers: HeaderMap,
    Json(request): Json<ExecutionStatusBatchRequest>,
) -> Result<Response, HttpResponse> {
    let accept = streaming_accept(&headers, params.follow)?;
    let mut seen = std::collections::HashSet::new();
    let execution_ids: Vec<_> = request
        .execution_ids
        .into_iter()
        .filter(|id| seen.insert(id.clone()))
        .collect();
    if execution_ids.is_empty() || execution_ids.len() > STATUS_BATCH_MAX_IDS {
        return Err(HttpResponse::bad_request(
            accept,
            format!("execution_ids must contain 1 to {STATUS_BATCH_MAX_IDS} IDs"),
        ));
    }
    if params.follow {
        let source = StatusBatchFollow {
            execution_ids,
            last_sent: std::collections::HashMap::new(),
            first: true,
        };
        return follow_response(&state, source, accept).await;
    }
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let states = conn
        .get_pending_state_batch(&execution_ids)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let response: Vec<_> = states
        .into_iter()
        .map(ExecutionStatusBatchItem::from)
        .collect();
    Ok(pretty_json_response(StatusCode::OK, &response))
}

struct StatusBatchFollow {
    /// Executions not yet seen finished.
    execution_ids: Vec<ExecutionId>,
    last_sent: std::collections::HashMap<ExecutionId, ExecutionStatusBatchItem>,
    /// The first event is sent even when empty, so a client learns which executions are missing.
    first: bool,
}

#[async_trait::async_trait]
impl FollowSource for StatusBatchFollow {
    async fn next_page(&mut self, conn: &dyn DbExternalApi) -> Result<FollowPage, DbErrorRead> {
        let states = conn.get_pending_state_batch(&self.execution_ids).await?;
        self.execution_ids.retain(|id| {
            states
                .iter()
                .any(|state| state.execution_with_state.execution_id == *id)
        });
        let changed: Vec<_> = states
            .into_iter()
            .map(ExecutionStatusBatchItem::from)
            .filter(|item| self.last_sent.get(&item.status.execution_id) != Some(item))
            .collect();
        for item in &changed {
            if matches!(item.status.pending_state, PendingState::Finished(_)) {
                self.execution_ids
                    .retain(|id| *id != item.status.execution_id);
                self.last_sent.remove(&item.status.execution_id);
            } else {
                self.last_sent
                    .insert(item.status.execution_id.clone(), item.clone());
            }
        }
        // Statuses have no position to resume from, a reconnect gets all of them again.
        let event = (self.first || !changed.is_empty()).then(|| {
            Event::default()
                .event("statuses")
                .data(serde_json::to_string(&changed).expect("statuses serialize"))
        });
        self.first = false;
        Ok(FollowPage {
            event,
            full: false,
            finished: false,
            exhausted: self.execution_ids.is_empty(),
        })
    }
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ExecutionEventsAndResponsesParams {
    #[serde(default)]
    version_from: VersionType,
    /// With `follow`, the total for the whole stream, unlimited by default.
    events_length: Option<u16>,
    #[serde(default)]
    include_backtrace_id: bool,
    #[serde(default)]
    responses_cursor_from: VersionType,
    /// With `follow`, the total for the whole stream, unlimited by default.
    responses_length: Option<u16>,
    #[serde(default)]
    responses_including_cursor: bool,
    /// Stream SSE `events_and_responses` events until the execution finishes or either
    /// length is reached. The first event is always sent, later ones only with new rows.
    #[serde(default)]
    follow: bool,
}

#[utoipa::path(
    get,
    path = "/v1/executions/{execution_id}/events-and-responses",
    tag = "executions",
    params(("execution_id" = String, Path), ExecutionEventsAndResponsesParams),
    responses((status = 200, body = ExecutionEventsAndResponsesResponse))
)]
async fn execution_events_and_responses(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Query(params): Query<ExecutionEventsAndResponsesParams>,
    headers: HeaderMap,
) -> Result<Response, HttpResponse> {
    let accept = streaming_accept(&headers, params.follow)?;
    if params.follow {
        let (version_from, responses_cursor, responses_including_cursor) =
            match last_event_id(&headers) {
                Some(id) => {
                    let (version, cursor) = id
                        .split_once(':')
                        .and_then(|(version, cursor)| {
                            Some((version.parse().ok()?, cursor.parse().ok()?))
                        })
                        .ok_or_else(|| invalid_last_event_id(accept))?;
                    (version, cursor, false)
                }
                None => (
                    params.version_from,
                    params.responses_cursor_from,
                    params.responses_including_cursor,
                ),
            };
        let source = EventsAndResponsesFollow {
            execution_id,
            version_from,
            include_backtrace_id: params.include_backtrace_id,
            responses_cursor,
            responses_including_cursor,
            events_budget: follow_budget(params.events_length, accept)?,
            responses_budget: follow_budget(params.responses_length, accept)?,
            first: true,
        };
        return follow_response(&state, source, accept).await;
    }
    let events_length = nonzero_page_length(params.events_length.unwrap_or(20), accept)?;
    let responses_length = nonzero_page_length(params.responses_length.unwrap_or(20), accept)?;
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let result = conn
        .list_execution_events_responses(
            &execution_id,
            &Version::new(params.version_from),
            events_length,
            params.include_backtrace_id,
            Pagination::NewerThan {
                length: responses_length,
                cursor: params.responses_cursor_from,
                including_cursor: params.responses_including_cursor,
            },
        )
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(pretty_json_response(
        StatusCode::OK,
        &ExecutionEventsAndResponsesResponse {
            events: result.events,
            responses: result.responses,
            current_status: result.execution_with_state.into(),
            max_version: result.max_version,
            max_cursor: result.max_cursor,
        },
    ))
}
struct EventsAndResponsesFollow {
    execution_id: ExecutionId,
    version_from: VersionType,
    include_backtrace_id: bool,
    responses_cursor: VersionType,
    responses_including_cursor: bool,
    events_budget: FollowBudget,
    responses_budget: FollowBudget,
    first: bool,
}

#[async_trait::async_trait]
impl FollowSource for EventsAndResponsesFollow {
    async fn next_page(&mut self, conn: &dyn DbExternalApi) -> Result<FollowPage, DbErrorRead> {
        let events_length = self.events_budget.page_length();
        let responses_length = self.responses_budget.page_length();
        let result = conn
            .list_execution_events_responses(
                &self.execution_id,
                &Version::new(self.version_from),
                events_length,
                self.include_backtrace_id,
                Pagination::NewerThan {
                    length: responses_length,
                    cursor: self.responses_cursor,
                    including_cursor: self.responses_including_cursor,
                },
            )
            .await?;
        let full = result.events.len() >= usize::from(events_length.get())
            || result.responses.len() >= usize::from(responses_length.get());
        let finished = matches!(
            result.execution_with_state.pending_state,
            PendingState::Finished(_)
        );
        if let Some(last) = result.events.last() {
            self.version_from = last.version.0 + 1;
        }
        if let Some(last) = result.responses.last() {
            self.responses_cursor = last.cursor.0;
            self.responses_including_cursor = false;
        }
        let events_exhausted = self.events_budget.consume(result.events.len());
        let responses_exhausted = self.responses_budget.consume(result.responses.len());
        let send = std::mem::take(&mut self.first)
            || !result.events.is_empty()
            || !result.responses.is_empty();
        let event = send.then(|| {
            json_event(
                "events_and_responses",
                &format!("{}:{}", self.version_from, self.responses_cursor),
                &ExecutionEventsAndResponsesResponse {
                    events: result.events,
                    responses: result.responses,
                    current_status: result.execution_with_state.into(),
                    max_version: result.max_version,
                    max_cursor: result.max_cursor,
                },
            )
        });
        Ok(FollowPage {
            event,
            full,
            finished,
            exhausted: events_exhausted || responses_exhausted,
        })
    }
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
    let length = nonzero_page_length(params.length.unwrap_or(DEFAULT_LENGTH), accept)?;
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
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
    Ok(pretty_json_response(
        StatusCode::OK,
        &ExecutionEventsResponse {
            events: result.events,
            max_version: result.max_version,
        },
    ))
}

pub(crate) mod logs {
    use super::*;
    use base64::{Engine as _, prelude::BASE64_STANDARD};
    use chrono::{DateTime, Utc};
    use concepts::storage::{LogCursor, LogFilter, LogLevel, LogStreamType};
    use rest_types::logs::LogEntryRowSer;

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

        // pagination
        /// Opaque cursor for pagination
        cursor: Option<String>,
        /// Only include entries created after this timestamp.
        after: Option<DateTime<Utc>>,
        /// Number of entries to return. With `follow`, the total for the whole stream,
        /// unlimited by default.
        #[param(minimum = 1)]
        length: Option<u16>,
        /// Include the cursor item in results
        #[serde(default)]
        including_cursor: bool,
        /// Pagination direction
        #[serde(default)]
        direction: PaginationDirectionSortedFromOldest,
        /// Stream new entries as SSE `logs` events until the execution finishes.
        #[serde(default)]
        follow: bool,
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
        headers: HeaderMap,
    ) -> Result<Response, HttpResponse> {
        const DEFAULT_LENGTH: u16 = 20;
        const MAX_LENGTH_INCLUSIVE: u16 = 200;

        let accept = streaming_accept(&headers, params.follow)?;
        let cursor = params
            .cursor
            .as_deref()
            .map(|cursor| {
                decode_cursor(cursor).ok_or_else(|| {
                    HttpResponse::bad_request(accept, "invalid log cursor".to_string())
                })
            })
            .transpose()?;

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
        }
        .with_created_bounds(params.after, None);

        if params.follow {
            follow_requires_newer(params.direction, accept)?;
            let (cursor, including_cursor) = match last_event_id(&headers) {
                Some(id) => (
                    decode_cursor(id).ok_or_else(|| invalid_last_event_id(accept))?,
                    false,
                ),
                None => (
                    cursor.unwrap_or(LogCursor(i64::MIN)),
                    params.including_cursor,
                ),
            };
            let source = LogsFollow {
                execution_id,
                show_derived: params.show_derived,
                filter,
                cursor,
                including_cursor,
                budget: follow_budget(params.length, accept)?,
            };
            return follow_response(&state, source, accept).await;
        }

        let length = nonzero_page_length(
            MAX_LENGTH_INCLUSIVE.min(params.length.unwrap_or(DEFAULT_LENGTH)),
            accept,
        )?;

        let pagination = match params.direction {
            PaginationDirectionSortedFromOldest::Older => Pagination::OlderThan {
                length,
                cursor: cursor.unwrap_or(LogCursor(i64::MAX)),
                including_cursor: params.including_cursor,
            },
            PaginationDirectionSortedFromOldest::Newer => Pagination::NewerThan {
                length,
                cursor: cursor.unwrap_or(LogCursor(i64::MIN)),
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

        Ok({
            let items: Vec<LogEntryRowSer> =
                result.items.into_iter().map(LogEntryRowSer::from).collect();
            pretty_json_response(StatusCode::OK, &items)
        })
    }

    fn decode_cursor(cursor: &str) -> Option<LogCursor> {
        BASE64_STANDARD
            .decode(cursor)
            .ok()
            .and_then(|decoded| serde_json::from_slice::<LogCursor>(&decoded).ok())
    }

    struct LogsFollow {
        execution_id: ExecutionId,
        show_derived: bool,
        filter: LogFilter,
        cursor: LogCursor,
        including_cursor: bool,
        budget: FollowBudget,
    }

    #[async_trait::async_trait]
    impl FollowSource for LogsFollow {
        async fn next_page(&mut self, conn: &dyn DbExternalApi) -> Result<FollowPage, DbErrorRead> {
            let finished = is_finished(conn, &self.execution_id).await?;
            let length = self.budget.page_length();
            let result = conn
                .list_logs(
                    &self.execution_id,
                    self.show_derived,
                    self.filter.clone(),
                    Pagination::NewerThan {
                        length,
                        cursor: self.cursor,
                        including_cursor: self.including_cursor,
                    },
                )
                .await?;
            let full = result.items.len() >= usize::from(length.get());
            if let Some(last) = result.items.last() {
                self.cursor = last.cursor;
                self.including_cursor = false;
            }
            let exhausted = self.budget.consume(result.items.len());
            let items: Vec<LogEntryRowSer> =
                result.items.into_iter().map(LogEntryRowSer::from).collect();
            let event = items
                .last()
                .map(|last| json_event("logs", &last.cursor, &items));
            Ok(FollowPage {
                event,
                full,
                finished,
                exhausted,
            })
        }
    }
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ExecutionResponsesParams {
    /// Filter by an exact join-set ID
    join_set: Option<String>,
    /// Cursor for pagination
    cursor: Option<u32>,
    /// Maximum number of responses to return. With `follow`, the total for the whole stream,
    /// unlimited by default.
    #[param(minimum = 1)]
    length: Option<u16>,
    /// Include the cursor item in results
    #[serde(default)]
    including_cursor: bool,
    /// Pagination direction
    #[serde(default)]
    direction: PaginationDirectionSortedFromOldest,
    /// Stream new responses as SSE `responses` events until the execution finishes.
    #[serde(default)]
    follow: bool,
}

fn parse_join_set_filter(join_set: &str) -> Result<JoinSetId, String> {
    join_set
        .parse()
        .map_err(|err: concepts::JoinSetIdParseError| err.to_string())
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
    headers: HeaderMap,
) -> Result<Response, HttpResponse> {
    const DEFAULT_LENGTH: u16 = 20;
    let accept = streaming_accept(&headers, params.follow)?;
    let join_set = params
        .join_set
        .as_deref()
        .map(parse_join_set_filter)
        .transpose()
        .map_err(|err| HttpResponse::bad_request(accept, err))?;
    if params.follow {
        follow_requires_newer(params.direction, accept)?;
        let (cursor, including_cursor) = match last_event_id(&headers) {
            Some(id) => (
                id.parse().map_err(|_| invalid_last_event_id(accept))?,
                false,
            ),
            None => (params.cursor.unwrap_or(0), params.including_cursor),
        };
        let source = ResponsesFollow {
            execution_id,
            join_set,
            cursor,
            including_cursor,
            budget: follow_budget(params.length, accept)?,
        };
        return follow_response(&state, source, accept).await;
    }
    let length = nonzero_page_length(params.length.unwrap_or(DEFAULT_LENGTH), accept)?;
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
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
        .list_responses_filtered(&execution_id, pagination, join_set.as_ref())
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;

    Ok(pretty_json_response(
        StatusCode::OK,
        &ExecutionResponsesResponse {
            responses: result.responses,
            max_cursor: result.max_cursor,
            scan_cursor: result.scan_cursor,
        },
    ))
}

struct ResponsesFollow {
    execution_id: ExecutionId,
    join_set: Option<JoinSetId>,
    cursor: u32,
    including_cursor: bool,
    budget: FollowBudget,
}

#[async_trait::async_trait]
impl FollowSource for ResponsesFollow {
    async fn next_page(&mut self, conn: &dyn DbExternalApi) -> Result<FollowPage, DbErrorRead> {
        let finished = is_finished(conn, &self.execution_id).await?;
        let length = self.budget.page_length();
        let result = conn
            .list_responses_filtered(
                &self.execution_id,
                Pagination::NewerThan {
                    length,
                    cursor: self.cursor,
                    including_cursor: self.including_cursor,
                },
                self.join_set.as_ref(),
            )
            .await?;
        let full = result.responses.len() >= usize::from(length.get());
        // The scan cursor also skips responses outside of the join set filter.
        self.cursor = self.cursor.max(result.scan_cursor.0);
        self.including_cursor = false;
        let exhausted = self.budget.consume(result.responses.len());
        let event = (!result.responses.is_empty()).then(|| {
            json_event(
                "responses",
                &self.cursor.to_string(),
                &ExecutionResponsesResponse {
                    responses: result.responses,
                    max_cursor: result.max_cursor,
                    scan_cursor: result.scan_cursor,
                },
            )
        });
        Ok(FollowPage {
            event,
            full,
            finished,
            exhausted,
        })
    }
}

/// Get execution status
#[utoipa::path(
    get,
    path = "/v1/executions/{execution_id}/status",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID"),
        StatusFollowParams
    ),
    responses(
        (status = 200, description = "Execution status", body = ExecutionWithStateSer)
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_status_get(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Query(params): Query<StatusFollowParams>,
    headers: HeaderMap,
) -> Result<Response, HttpResponse> {
    let accept = streaming_accept(&headers, params.follow)?;
    let execution_with_state = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?
        .get_pending_state(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    if params.follow {
        let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(1);
        let db_pool = state.db_pool.clone();
        let mut termination = state.termination_watcher.clone();
        let last_event_id = last_event_id(&headers).map(str::to_owned);
        utils::spawn::spawn_named("stream_execution_status", async move {
            let mut last_sent_id = last_event_id;
            let mut current = execution_with_state;
            loop {
                let data = serde_json::to_string(&ExecutionWithStateSer::from(current.clone()))
                    .expect("status serializes");
                let id = Sha256::digest(data.as_bytes()).iter().fold(
                    String::with_capacity(64),
                    |mut out, byte| {
                        use std::fmt::Write as _;
                        write!(out, "{byte:02x}").expect("writing to string");
                        out
                    },
                );
                let finished_id = format!("{id}:finished");
                if last_sent_id.as_deref() != Some(&id)
                    && last_sent_id.as_deref() != Some(&finished_id)
                {
                    if tx
                        .send(Ok(Event::default().event("status").id(&id).data(data)))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    last_sent_id = Some(id);
                }
                if let PendingState::Finished(finished) = &current.pending_state {
                    if params.send_finished_status && last_sent_id.as_deref() != Some(&finished_id)
                    {
                        let Ok(conn) = db_pool.external_api_conn().await else {
                            break;
                        };
                        let Ok(created) = conn.get_create_request(&execution_id).await else {
                            break;
                        };
                        let Ok(last) = conn.get_last_execution_event(&execution_id).await else {
                            break;
                        };
                        if let ExecutionRequest::Finished { retval, .. } = last.event {
                            let data = json!({
                                "created_at": current.created_at,
                                "scheduled_at": created.scheduled_at,
                                "finished_at": finished.finished_at,
                                "value": RetVal::from(retval),
                            });
                            if tx
                                .send(Ok(Event::default()
                                    .event("finished_status")
                                    .id(&finished_id)
                                    .data(data.to_string())))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    break;
                }
                select! {
                    () = tx.closed() => break,
                    _ = termination.changed() => break,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                let Ok(conn) = db_pool.external_api_conn().await else {
                    break;
                };
                let Ok(next) = conn.get_pending_state(&execution_id).await else {
                    break;
                };
                current = next;
            }
        });
        return Ok(sse_response(rx));
    }
    Ok(pretty_json_response(
        StatusCode::OK,
        &ExecutionWithStateSer::from(execution_with_state),
    ))
}

pub(crate) fn format_execution_status_text(pending_state: &PendingState) -> String {
    match pending_state {
        PendingState::Locked(_) => "Locked".to_string(),
        PendingState::PendingAt(pending) => format!("Pending at {}", pending.scheduled_at),
        PendingState::BlockedByJoinSet(blocked) => format!(
            "Blocked by {}{}",
            blocked.join_set_id,
            if blocked.closing { " (closing)" } else { "" }
        ),
        PendingState::Paused(_) => "Paused".to_string(),
        PendingState::Cancelling(_) => "Cancelling".to_string(),
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
    let create_request = db_connection
        .get_create_request(&ExecutionId::Derived(execution_id.clone()))
        .await
        .map_err(|err| ErrorWrapper(err, accept))?;
    let ffqn = create_request.ffqn;
    let max_persisted_value_size_bytes = create_request.max_persisted_value_size_bytes;

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
    let value_limit =
        concepts::persisted_value::EncodedSizeLimit::new(max_persisted_value_size_bytes)
            .unwrap_or(concepts::persisted_value::EncodedSizeLimit::LEGACY_UNLIMITED);
    if let Err(exceeded) = value_limit.validate(&return_value) {
        concepts::persisted_value::report_rejection(
            Some(&ExecutionId::Derived(execution_id.clone())),
            Some(&ffqn),
            concepts::persisted_value::PersistedValueClass::Stub,
            concepts::persisted_value::PersistedValueOrigin::Rest,
            exceeded,
        );
        return Err(HttpResponse {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: format!(
                "stub result exceeds the {max_persisted_value_size_bytes}-byte persisted value limit"
            ),
            accept,
        });
    }

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
    if let Err(exceeded) = value_limit.validate(&return_value) {
        concepts::persisted_value::report_rejection(
            Some(&ExecutionId::Derived(execution_id.clone())),
            Some(&ffqn),
            concepts::persisted_value::PersistedValueClass::Stub,
            concepts::persisted_value::PersistedValueOrigin::Rest,
            exceeded,
        );
        return Err(HttpResponse {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: format!(
                "stub result exceeds the {max_persisted_value_size_bytes}-byte persisted value limit"
            ),
            accept,
        });
    }
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

fn replay_response_ser(
    value: wasm_workers::workflow::workflow_worker::MeasuredReplayResponse,
) -> ReplayResponseSer {
    use wasm_workers::workflow::workflow_worker::ReplayResponse;
    let outcome = match value.response {
        ReplayResponse::Advanceable(replay) => ReplayOutcomeSer::Advanceable {
            captured_writes: replay
                .captured_writes
                .into_iter()
                .map(CapturedWriteSer::from)
                .collect(),
        },
        ReplayResponse::Finished { result } => ReplayOutcomeSer::Finished {
            retval: serde_json::to_value(RetVal::from(result))
                .expect("supported retval must be JSON serializable"),
        },
        ReplayResponse::Blocked => ReplayOutcomeSer::Blocked,
    };
    replay_response_with_measurements(outcome, value.measurements)
}

fn replay_response_with_measurements(
    outcome: ReplayOutcomeSer,
    measurements: wasm_workers::workflow::workflow_worker::ReplayMeasurements,
) -> ReplayResponseSer {
    ReplayResponseSer {
        outcome,
        replayed_event_count: measurements.replayed_event_count,
        replay_duration_ms: measurements
            .replay_duration
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
        replay_version: measurements.replay_version,
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
    headers: HeaderMap,
) -> Result<http::Response<Body>, HttpResponse> {
    let accept = streaming_accept(&headers, params.follow)?;
    let last_event = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?
        .get_last_execution_event(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;

    if params.follow {
        Ok(stream_execution_response(
            execution_id,
            &state,
            StatusCode::OK,
            state.subscription_interruption,
            headers
                .get("last-event-id")
                .and_then(|value| value.to_str().ok()),
        ))
    } else if let ExecutionRequest::Finished { retval, .. } = last_event.event {
        let retval = RetVal::from(retval);

        Ok(pretty_json_response(StatusCode::OK, &retval))
    } else {
        Ok(HttpResponse {
            status: StatusCode::TOO_EARLY,
            message: "not finished yet".to_string(),
            accept,
        }
        .into_response())
    }
}

#[derive(Deserialize, Debug, IntoParams)]
#[into_params(parameter_in = Query)]
struct ExecutionFollowParam {
    /// If true, stream the result when execution finishes
    #[serde(default)]
    follow: bool,
}

#[derive(Deserialize, Debug, IntoParams)]
#[into_params(parameter_in = Query)]
struct StatusFollowParams {
    #[serde(default)]
    follow: bool,
    #[serde(default)]
    send_finished_status: bool,
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
    headers: HeaderMap,
    Json(payload): Json<ExecutionSubmitPayload>,
) -> Result<http::Response<Body>, HttpResponse> {
    let accept = streaming_accept(&headers, params.follow)?;
    let last_event_id = last_event_id(&headers).map(str::to_owned);
    execution_submit(
        execution_id,
        state,
        payload,
        params.follow,
        accept,
        last_event_id.as_deref(),
    )
    .await
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
    headers: HeaderMap,
    Query(params): Query<ExecutionFollowParam>,
    Json(payload): Json<ExecutionSubmitPayload>,
) -> Result<http::Response<Body>, HttpResponse> {
    let accept = streaming_accept(&headers, params.follow)?;
    let execution_id = ExecutionId::generate();
    let last_event_id = last_event_id(&headers).map(str::to_owned);
    execution_submit(
        execution_id,
        state,
        payload,
        params.follow,
        accept,
        last_event_id.as_deref(),
    )
    .await
}

fn streaming_accept(headers: &HeaderMap, follow: bool) -> Result<AcceptHeader, HttpResponse> {
    let mediatypes = axum_accept::parse_mediatypes(headers).map_err(|error| {
        let (status, message) = error.status_and_message();
        HttpResponse {
            status,
            message,
            accept: AcceptHeader::Json,
        }
    })?;
    if mediatypes.is_empty()
        || mediatypes.iter().any(|mt| {
            matches!(
                (mt.ty.as_str(), mt.subty.as_str()),
                ("*", "*") | ("application", "json" | "*")
            ) || (follow
                && matches!(
                    (mt.ty.as_str(), mt.subty.as_str()),
                    ("text", "event-stream" | "*")
                ))
        })
    {
        Ok(AcceptHeader::Json)
    } else {
        Err(HttpResponse {
            status: StatusCode::NOT_ACCEPTABLE,
            message: "Accept header must allow application/json or text/event-stream".to_string(),
            accept: AcceptHeader::Json,
        })
    }
}

fn sse_response(rx: mpsc::Receiver<Result<Event, Infallible>>) -> Response {
    Sse::new(ReceiverStream::new(rx))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("heartbeat"),
        )
        .into_response()
}

fn last_event_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
}

const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(1);
const FOLLOW_PAGE_LENGTH: u16 = 200;

/// Remaining number of items a follow stream may send, `None` means unlimited.
#[derive(Clone, Copy, Debug)]
struct FollowBudget(Option<u16>);

impl FollowBudget {
    fn page_length(self) -> NonZeroU16 {
        let length = self.0.map_or(FOLLOW_PAGE_LENGTH, |remaining| {
            remaining.min(FOLLOW_PAGE_LENGTH)
        });
        NonZeroU16::new(length).expect("an exhausted budget ends the stream")
    }

    /// Returns true when the budget is used up.
    fn consume(&mut self, items: usize) -> bool {
        let Some(remaining) = &mut self.0 else {
            return false;
        };
        *remaining = remaining.saturating_sub(u16::try_from(items).unwrap_or(u16::MAX));
        *remaining == 0
    }
}

fn follow_budget(length: Option<u16>, accept: AcceptHeader) -> Result<FollowBudget, HttpResponse> {
    length
        .map(|length| nonzero_page_length(length, accept).map(NonZeroU16::get))
        .transpose()
        .map(FollowBudget)
}

fn follow_requires_newer(
    direction: PaginationDirectionSortedFromOldest,
    accept: AcceptHeader,
) -> Result<(), HttpResponse> {
    match direction {
        PaginationDirectionSortedFromOldest::Newer => Ok(()),
        PaginationDirectionSortedFromOldest::Older => Err(HttpResponse::bad_request(
            accept,
            "`follow` requires `direction=newer`".to_string(),
        )),
    }
}

fn invalid_last_event_id(accept: AcceptHeader) -> HttpResponse {
    HttpResponse::bad_request(accept, "invalid Last-Event-ID".to_string())
}

/// One poll of a followed endpoint.
struct FollowPage {
    event: Option<Event>,
    /// The page was full, more rows may be ready without waiting.
    full: bool,
    /// The execution was finished before the page was read.
    finished: bool,
    /// The requested length was reached.
    exhausted: bool,
}

#[async_trait::async_trait]
trait FollowSource: Send + 'static {
    async fn next_page(&mut self, conn: &dyn DbExternalApi) -> Result<FollowPage, DbErrorRead>;
}

/// Read the first page inline so that errors like a missing execution become HTTP errors,
/// then keep polling in a task. The stream ends when the budget is used up, or with the
/// first non-full page read at least one poll interval after the execution finished, which
/// leaves time for rows written right after the finish, such as forwarded logs.
async fn follow_response(
    state: &WebApiState,
    mut source: impl FollowSource,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let conn = state
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    let mut page = source
        .next_page(conn.as_ref())
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    drop(conn);
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(1);
    let db_pool = state.db_pool.clone();
    let mut termination = state.termination_watcher.clone();
    utils::spawn::spawn_named(
        "follow_execution",
        async move {
            let mut finished = false;
            loop {
                if let Some(event) = page.event.take()
                    && tx.send(Ok(event)).await.is_err()
                {
                    break;
                }
                if page.exhausted || (finished && !page.full) {
                    break;
                }
                finished |= page.finished;
                if !page.full {
                    select! {
                        () = tx.closed() => break,
                        _ = termination.changed() => break,
                        () = tokio::time::sleep(FOLLOW_POLL_INTERVAL) => {}
                    }
                }
                let Ok(conn) = db_pool.external_api_conn().await else {
                    break;
                };
                match source.next_page(conn.as_ref()).await {
                    Ok(next) => page = next,
                    Err(err) => {
                        debug!("Ending follow stream - {err:?}");
                        break;
                    }
                }
            }
        }
        .in_current_span(),
    );
    Ok(sse_response(rx))
}

fn json_event(name: &'static str, id: &str, data: &impl Serialize) -> Event {
    Event::default()
        .event(name)
        .id(id)
        .data(serde_json::to_string(data).expect("follow page serializes"))
}

async fn is_finished(
    conn: &dyn DbExternalApi,
    execution_id: &ExecutionId,
) -> Result<bool, DbErrorRead> {
    Ok(matches!(
        conn.get_pending_state(execution_id).await?.pending_state,
        PendingState::Finished(_)
    ))
}

#[instrument(skip_all, fields(execution_id))]
async fn execution_submit(
    execution_id: ExecutionId,
    state: State<Arc<WebApiState>>,
    payload: ExecutionSubmitPayload,
    follow: bool,
    accept: AcceptHeader,
    last_event_id: Option<&str>,
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
        state.server_verified.max_persisted_value_size_bytes(),
        concepts::persisted_value::PersistedValueOrigin::Rest,
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
            last_event_id,
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
    last_event_id: Option<&str>,
) -> http::Response<Body> {
    if last_event_id == Some("1") {
        let mut response = (status, Body::empty()).into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("text/event-stream"),
        );
        return response;
    }
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let trace_id = server::gen_trace_id();
    let span = info_span!("stream_execution_response", trace_id, %execution_id);
    utils::spawn::spawn_named(
        "stream_execution_response",
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
        header::HeaderValue::from_static("text/event-stream"),
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
            let subscription_interruption = subscription_interruption
                .unwrap_or(Duration::MAX)
                .min(Duration::from_secs(15));
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
                let result = serde_json::to_string(&result)
                    .expect("serialization of already stored retval cannot fail");
                let _ = tx
                    .send(Ok(Bytes::from(format!(
                        "event: result\nid: 1\ndata: {result}\n\n"
                    ))))
                    .await;
                debug!("Sent execution result");
                return;
            }
            Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Timeout)) => {
                trace!("Timeout triggers resubscribing");
                if tx
                    .send(Ok(Bytes::from_static(b": heartbeat\n\n")))
                    .await
                    .is_err()
                {
                    return;
                }
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

/// Replay an execution. An advanceable (unfinished and unblocked) execution produces captured writes for the advance endpoint; backtraces on captured writes are display-only.
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
    let ser = Box::pin(replay_execution_internal(&state, &execution_id, accept)).await?;
    let status = if matches!(&ser.outcome, ReplayOutcomeSer::ReplayFailed { .. }) {
        StatusCode::CONFLICT
    } else {
        StatusCode::OK
    };
    Ok(pretty_json_response(status, &ser))
}

/// Advance a paused execution by applying captured writes from a prior replay; set `persist_backtrace` to persist fresh backtraces.
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

    let backtrace_capture = if payload.persist_backtrace {
        BacktraceCapture::NewEventsOnly
    } else {
        BacktraceCapture::Disabled
    };
    let advance_res = Box::pin(until_terminated(
        &state,
        accept,
        replay_worker.advance(execution_id.clone(), captured_writes, backtrace_capture),
    ))
    .await?;

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
            let response = pretty_json_response(StatusCode::UNPROCESSABLE_ENTITY, &error);
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
            .map_err(|e| HttpResponse::from(ErrorWrapper(e, accept)))?
            .get_pending_state(&execution_id)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        AdvanceResponseSer::InProgress {
            pending_state: execution_with_state.pending_state,
        }
    };
    Ok(pretty_json_response(StatusCode::OK, &response))
}

/// Replay an execution and persist its call-site backtraces (idempotent).
#[utoipa::path(
    put,
    path = "/v1/executions/{execution_id}/backtrace/persist",
    tag = "executions",
    params(
        ("execution_id" = String, Path, description = "Execution ID whose backtraces to persist")
    ),
    responses(
        (status = 200, description = "Backtraces persisted", body = PersistBacktracesResponseSer),
        (status = 404, description = "Not found"),
        (status = 422, description = "Replay failed")
    )
)]
#[instrument(skip_all, fields(execution_id))]
async fn execution_persist_backtraces(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let replay_worker = get_replay_target(&state, &execution_id, accept).await?;
    let persisted_backtrace_count = Box::pin(until_terminated(
        &state,
        accept,
        replay_worker.persist_backtraces(execution_id.clone()),
    ))
    .await?
    .map_err(|err| HttpResponse {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        message: format!("Replay error: {err}"),
        accept,
    })?;
    let persisted_backtrace_count =
        u32::try_from(persisted_backtrace_count).map_err(|_| HttpResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "too many backtraces persisted".to_string(),
            accept,
        })?;
    let ser = PersistBacktracesResponseSer {
        persisted_backtrace_count,
    };
    Ok(pretty_json_response(StatusCode::OK, &ser))
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
                    measurements,
                } => {
                    let failure = FinishedExecutionFailure::from(err.as_ref());
                    Ok(replay_response_with_measurements(
                        ReplayOutcomeSer::ReplayFailed {
                            error: err.to_string(),
                            failure: Some(failure),
                            captured_writes: captured_writes
                                .into_iter()
                                .map(CapturedWriteSer::from)
                                .collect(),
                        },
                        *measurements,
                    ))
                }
                other => Err(HttpResponse {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    message: format!("Replay error: {other}"),
                    accept,
                }),
            }
        };

    // Capture backtraces for the newly produced writes only; the already-persisted prefix
    // does not need them (and re-deriving them for a long log is expensive).
    let replay_res = Box::pin(until_terminated(
        state,
        accept,
        replay_worker.replay(execution_id.clone(), BacktraceCapture::NewEventsOnly),
    ))
    .await?;
    match replay_res {
        Ok(response) => Ok(replay_response_ser(response)),
        Err(err) => map_replay_err(err),
    }
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
        // no backtrace capture on upgrade
        let replay_res = Box::pin(until_terminated(
            &state,
            accept,
            replay_worker.replay(execution_id.clone(), BacktraceCapture::Disabled),
        ))
        .await?;
        if let Err(err) = replay_res {
            info!("Replay failed: {err:?}");
            return Err(HttpResponse {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                message: format!("Replay failed: {err}"),
                accept,
            });
        }
    }
    until_terminated(&state, accept, async {
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
            .map_err(|e| HttpResponse::from(ErrorWrapper(e, accept)))
    })
    .await??;
    Ok(HttpResponse {
        status: StatusCode::OK,
        message: "upgraded".to_string(),
        accept,
    }
    .into_response())
}

pub(crate) mod components {
    use crate::server::web_api_server::{HttpResponse, pretty_json_response};
    use rest_types::components::{ComponentConfig, FunctionMetadataLite};

    use super::{
        AcceptHeader, Arc, Deserialize, FunctionFqn, IntoParams, IntoResponse, Query, Response,
        State, StatusCode, WebApiState,
    };
    use axum::extract::Path;
    use concepts::{
        ComponentType,
        component_id::ComponentDigest,
        prefixed_ulid::DeploymentId,
        storage::{DeploymentComponentDetail, PersistedFunctionMetadata},
    };

    use std::fmt::Debug;

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
                    files: c.files.into_iter().map(Into::into).collect(),
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

        pretty_json_response(StatusCode::OK, &components)
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
        StatusCode, WebApiState, pretty_json_response,
    };
    use concepts::{FunctionFqn, FunctionRegistry};
    use rest_types::functions::FunctionOutput;

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
        _accept: AcceptHeader,
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

        pretty_json_response(StatusCode::OK, &functions)
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

pub(crate) mod deployment {
    use crate::{
        command::server::{self, RuntimeConfigAvailability, SwitchDeploymentAction},
        config::deployment::strip_generated_deployment_metadata,
        server::{
            deployment_summary,
            web_api_server::{
                AcceptHeader, ErrorWrapper, HttpResponse, PaginationDirectionSortedFromLatest,
                TextDefaultAcceptHeader, WebApiState, nonzero_page_length, pretty_json_response,
            },
        },
    };
    use http::header;
    use rest_types::deployment::{
        DeploymentRecordSer, DeploymentStateSer, DeploymentStatusSer, DeploymentSubmitErrorBody,
        DeploymentSubmitPayload, DeploymentSubmitResponse, DeploymentSwitchOutcome,
        DeploymentSwitchPayload, DeploymentSwitchResponse, DigestMismatch, FileIssue,
        MissingRuntimeConfigErrorBody, SubmitPackageErrorBody,
    };

    /// Map the REST `allow_unavailable_runtime_config` flag to the runtime config policy.
    fn runtime_config_availability_from_bool(allow_unavailable: bool) -> RuntimeConfigAvailability {
        if allow_unavailable {
            RuntimeConfigAvailability::AllowUnavailable
        } else {
            RuntimeConfigAvailability::Strict
        }
    }
    use axum::{
        Json,
        extract::{Path, Query, State},
        response::{IntoResponse, Response},
    };
    use chrono::Utc;
    use concepts::prefixed_ulid::DeploymentId;
    use concepts::storage::Pagination;
    use concepts::storage::{
        DeploymentExecutionCounts, DeploymentState, LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH,
    };
    use http::StatusCode;
    use serde::Deserialize;
    use std::sync::Arc;
    use tracing::{info, instrument};
    use utoipa::IntoParams;

    fn deployment_state_ser(
        deployment_state: &DeploymentState,
        include_component_summary: bool,
        include_deployment_toml: bool,
    ) -> DeploymentStateSer {
        let component_summary = include_component_summary
            .then(|| {
                deployment_state
                    .deployment_toml
                    .as_deref()
                    .and_then(deployment_summary::deployment_component_summary)
            })
            .flatten();
        DeploymentStateSer {
            deployment_id: deployment_state.deployment_id,
            description: deployment_state.description.clone(),
            digest: deployment_state.digest.to_string(),
            status: DeploymentStatusSer::from(&deployment_state.status),
            created_at: deployment_state.created_at,
            last_active_at: deployment_state.last_active_at,
            last_active_app_config_digest: deployment_state.last_active_app_config_digest.clone(),
            locked: deployment_state.locked,
            pending: deployment_state.pending,
            scheduled: deployment_state.scheduled,
            blocked: deployment_state.blocked,
            paused: deployment_state.paused,
            cancelling: deployment_state.cancelling,
            finished_ok: deployment_state.finished_ok,
            finished_error: deployment_state.finished_error,
            finished_execution_failure: deployment_state.finished_execution_failure,
            component_summary,
            deployment_toml: include_deployment_toml
                .then(|| deployment_state.deployment_toml.clone())
                .flatten(),
        }
    }

    #[derive(Debug, Deserialize, IntoParams)]
    #[into_params(parameter_in = Query)]
    #[expect(clippy::struct_excessive_bools)]
    pub(crate) struct ListDeploymentsParams {
        /// Cursor for pagination (deployment ID)
        #[serde(default)]
        #[param(value_type = Option<String>)]
        cursor_from: Option<DeploymentId>,
        #[serde(default)]
        direction: PaginationDirectionSortedFromLatest,
        /// Number of items to return
        #[param(minimum = 1)]
        length: Option<u16>,
        /// Include the cursor item in results
        #[serde(default)]
        including_cursor: bool,
        /// Count child executions in the summary buckets as well
        #[serde(default)]
        include_derived: bool,
        /// Include component counts by manifest section type
        #[serde(default)]
        include_component_summary: bool,
        #[serde(default)]
        include_deployment_toml: bool,
        #[serde(default = "default_include_execution_counts")]
        include_execution_counts: bool,
    }

    fn default_include_execution_counts() -> bool {
        true
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
    pub(crate) async fn list(
        state: State<Arc<WebApiState>>,
        Query(params): Query<ListDeploymentsParams>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let length = nonzero_page_length(
            params
                .length
                .unwrap_or(LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH),
            accept,
        )?;
        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;
        let pagination = match params.direction {
            PaginationDirectionSortedFromLatest::Older => Pagination::OlderThan {
                length,
                cursor: params.cursor_from,
                including_cursor: params.including_cursor,
            },
            PaginationDirectionSortedFromLatest::Newer => Pagination::NewerThan {
                length,
                cursor: params.cursor_from,
                including_cursor: params.including_cursor,
            },
        };
        let states = conn
            .list_deployment_states(
                Utc::now(),
                pagination,
                params.include_component_summary || params.include_deployment_toml,
                if params.include_execution_counts {
                    DeploymentExecutionCounts::Count {
                        include_derived: params.include_derived,
                    }
                } else {
                    DeploymentExecutionCounts::Skip
                },
            )
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let states: Vec<DeploymentStateSer> = states
            .into_iter()
            .map(|dep| {
                deployment_state_ser(
                    &dep,
                    params.include_component_summary,
                    params.include_deployment_toml,
                )
            })
            .collect();

        Ok(pretty_json_response(StatusCode::OK, &states))
    }

    /// Get current deployment ID
    #[utoipa::path(
        get,
        path = "/v1/deployment-id",
        tag = "deployments",
        responses((
            status = 200,
            description = "Current deployment ID",
            content((String = "text/plain"), (String = "application/json"))
        ))
    )]
    // backcompat: 0.43, delete this endpoint after 0.43 in favor of `GET /v1/info`.
    #[deprecated(note = "use `deployment_id` from `GET /v1/info`")]
    pub(crate) async fn current(
        state: State<Arc<WebApiState>>,
        accept: TextDefaultAcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let accept = accept.into();
        let deployment_id = state.deployment_ctx.read().await.deployment_id;
        Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &deployment_id),
            AcceptHeader::Text => deployment_id.to_string().into_response(),
        })
    }

    #[derive(Debug, Default, Deserialize, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct GetDeploymentParams {
        /// Include server-generated metadata in `deployment_toml`. Omission preserves the stored server view.
        include_generated_metadata: Option<bool>,
    }

    /// Get a specific deployment by ID
    #[utoipa::path(
        get,
        path = "/v1/deployments/{deployment_id}",
        tag = "deployments",
        params(
            ("deployment_id" = String, Path, description = "Deployment ID"),
            GetDeploymentParams
        ),
        responses(
            (status = 200, description = "Deployment details", body = DeploymentRecordSer),
            (status = 404, description = "Deployment not found")
        )
    )]
    #[instrument(skip_all, fields(deployment_id))]
    pub(crate) async fn get(
        Path(deployment_id): Path<DeploymentId>,
        Query(params): Query<GetDeploymentParams>,
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;
        let mut record = conn
            .get_deployment(deployment_id)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?
            .ok_or_else(|| HttpResponse::not_found(accept, Some("deployment")))?;
        if params.include_generated_metadata == Some(false) {
            record.deployment_toml = strip_generated_deployment_metadata(&record.deployment_toml)
                .map_err(|err| HttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("cannot clean deployment manifest: {err:#}"),
                accept,
            })?;
        }

        let ser = DeploymentRecordSer::from(&record);
        Ok(pretty_json_response(StatusCode::OK, &ser))
    }

    impl From<&server::SubmitFileIssue> for FileIssue {
        fn from(issue: &server::SubmitFileIssue) -> Self {
            FileIssue {
                section: issue.section.clone(),
                component_name: issue.component_name.clone(),
                field_path: issue.field_path.clone(),
                path: issue.path.clone(),
                digest: issue.digest.clone(),
                message: issue.message.clone(),
            }
        }
    }

    impl From<&server::SubmitPackageError> for SubmitPackageErrorBody {
        fn from(pkg: &server::SubmitPackageError) -> Self {
            SubmitPackageErrorBody {
                missing_digest_fields: pkg.missing_digest_fields.iter().map(Into::into).collect(),
                missing_files: pkg.missing_files.iter().map(Into::into).collect(),
                unexpected_files: pkg.unexpected_files.iter().map(Into::into).collect(),
                digest_mismatches: pkg
                    .digest_mismatches
                    .iter()
                    .map(|m| DigestMismatch {
                        file: (&m.file).into(),
                        supplied_digest: m.supplied_digest.clone(),
                        actual_digest: m.actual_digest.clone(),
                    })
                    .collect(),
                oversized_files: pkg.oversized_files.iter().map(Into::into).collect(),
            }
        }
    }

    /// Parsed inputs for a submit request, from either a JSON body or a multipart package.
    struct SubmitInputs {
        deployment_toml: String,
        description: Option<String>,
        allow_unavailable_runtime_config: bool,
        files: Vec<server::SuppliedFile>,
    }

    async fn parse_multipart_submit(
        mut multipart: axum::extract::Multipart,
        accept: AcceptHeader,
    ) -> Result<SubmitInputs, HttpResponse> {
        let bad = |message: String| HttpResponse {
            status: StatusCode::BAD_REQUEST,
            message,
            accept,
        };
        let mut deployment_toml = None;
        let mut description = None;
        let mut allow_unavailable_runtime_config = false;
        let mut files = Vec::new();
        while let Some(field) = multipart
            .next_field()
            .await
            .map_err(|err| bad(format!("invalid multipart body: {err}")))?
        {
            let name = field.name().unwrap_or_default().to_string();
            let file_name = field.file_name().map(str::to_string);
            match name.as_str() {
                "deployment_toml" => {
                    deployment_toml =
                        Some(field.text().await.map_err(|err| {
                            bad(format!("invalid `deployment_toml` field: {err}"))
                        })?);
                }
                "description" => {
                    description = Some(
                        field
                            .text()
                            .await
                            .map_err(|err| bad(format!("invalid `description` field: {err}")))?,
                    );
                }
                "allow_unavailable_runtime_config" => {
                    let value = field.text().await.map_err(|err| {
                        bad(format!(
                            "invalid `allow_unavailable_runtime_config` field: {err}"
                        ))
                    })?;
                    allow_unavailable_runtime_config = value.trim() == "true";
                }
                // Any other part is a file blob. `name` is the claimed digest (or `file`).
                _ => {
                    let supplied_digest = (name != "file").then_some(name);
                    let path = file_name.unwrap_or_default();
                    let content = field
                        .bytes()
                        .await
                        .map_err(|err| bad(format!("cannot read file blob `{path}`: {err}")))?
                        .to_vec();
                    files.push(server::SuppliedFile {
                        path,
                        supplied_digest,
                        content,
                    });
                }
            }
        }
        Ok(SubmitInputs {
            deployment_toml: deployment_toml
                .ok_or_else(|| bad("missing `deployment_toml` field".to_string()))?,
            description,
            allow_unavailable_runtime_config,
            files,
        })
    }

    /// Submit a new deployment.
    ///
    /// Accepts either an `application/json` body (no attached file blobs; referenced
    /// files must already exist in the CAS) or a `multipart/form-data` package whose
    /// parts carry the manifest and the deployment-owned file blobs. On an incomplete
    /// package the server stores nothing and returns a `409` `SubmitPackageErrorBody`
    /// listing the missing files to attach and retry.
    #[utoipa::path(
        post,
        path = "/v1/deployments",
        tag = "deployments",
        request_body = DeploymentSubmitPayload,
        responses(
            (status = 201, description = "Deployment submitted", body = DeploymentSubmitResponse),
            (status = 400, description = "Invalid config or unregistered secrets", body = DeploymentSubmitErrorBody),
            (status = 409, description = "Incomplete or invalid package", body = SubmitPackageErrorBody)
        )
    )]
    #[instrument(skip_all)]
    pub(crate) async fn submit_post(
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
        request: axum::extract::Request,
    ) -> Result<Response, HttpResponse> {
        let deployment_id = DeploymentId::generate();
        submit(deployment_id, true, state, accept, request).await
    }

    /// Submit a deployment with a client-supplied ID.
    #[utoipa::path(
        put,
        path = "/v1/deployments/{deployment_id}",
        tag = "deployments",
        params(("deployment_id" = String, Path, description = "Deployment ID")),
        request_body = DeploymentSubmitPayload,
        responses(
            (status = 204, description = "Deployment submitted"),
            (status = 400, description = "Invalid config or unregistered secrets", body = DeploymentSubmitErrorBody),
            (status = 409, description = "Deployment ID or package conflict", body = SubmitPackageErrorBody)
        )
    )]
    #[instrument(skip_all, fields(%deployment_id))]
    pub(crate) async fn submit_put(
        Path(deployment_id): Path<DeploymentId>,
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
        request: axum::extract::Request,
    ) -> Result<Response, HttpResponse> {
        submit(deployment_id, false, state, accept, request).await
    }

    async fn submit(
        deployment_id: DeploymentId,
        return_id: bool,
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
        request: axum::extract::Request,
    ) -> Result<Response, HttpResponse> {
        use axum::extract::FromRequest as _;

        let is_multipart = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("multipart/form-data"));

        let inputs = if is_multipart {
            let multipart = axum::extract::Multipart::from_request(request, &*state)
                .await
                .map_err(|err| HttpResponse {
                    status: StatusCode::BAD_REQUEST,
                    message: format!("invalid multipart request: {err}"),
                    accept,
                })?;
            parse_multipart_submit(multipart, accept).await?
        } else {
            let Json(payload) = Json::<DeploymentSubmitPayload>::from_request(request, &*state)
                .await
                .map_err(|err| HttpResponse {
                    status: StatusCode::BAD_REQUEST,
                    message: format!("invalid JSON body: {err}"),
                    accept,
                })?;
            SubmitInputs {
                deployment_toml: payload.deployment_toml,
                description: payload.description,
                allow_unavailable_runtime_config: payload.allow_unavailable_runtime_config,
                files: Vec::new(),
            }
        };

        let mut termination_watcher = state.termination_watcher.clone();
        let result = super::until_terminated(
            &state,
            accept,
            Box::pin(crate::command::server::submit_deployment(
                state.server_verified.clone(),
                &inputs.deployment_toml,
                runtime_config_availability_from_bool(inputs.allow_unavailable_runtime_config),
                Some("web-api".to_string()),
                inputs.description,
                deployment_id,
                &state.prepared_dirs,
                inputs.files,
                state.db_pool.clone(),
                &mut termination_watcher,
                state.deployment_switch_manager.clone(),
            )),
        )
        .await?;

        match result {
            Ok(_) => {}
            Err(server::SubmitDeploymentError::Busy) => {
                return Err(HttpResponse {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    message: "another deployment submit or switch is already running".to_string(),
                    accept,
                });
            }
            Err(server::SubmitDeploymentError::Conflict(err)) => {
                return Err(HttpResponse {
                    status: StatusCode::CONFLICT,
                    message: format!("{err:#}"),
                    accept,
                });
            }
            Err(server::SubmitDeploymentError::MissingRuntimeConfig(missing)) => {
                return Ok(pretty_json_response(
                    StatusCode::BAD_REQUEST,
                    &MissingRuntimeConfigErrorBody {
                        error: "missing_runtime_config".to_string(),
                        public_env: missing.public_env.into_iter().collect(),
                        optional_public_env: missing.optional_public_env.into_iter().collect(),
                        required_public_env: missing.required_public_env.into_iter().collect(),
                        secrets: missing.secrets.into_iter().collect(),
                        optional_secrets: missing.optional_secrets.into_iter().collect(),
                        unset_secrets: missing.unset_secrets.into_iter().collect(),
                    },
                ));
            }
            Err(server::SubmitDeploymentError::Other(err)) => {
                return Err(HttpResponse {
                    status: StatusCode::BAD_REQUEST,
                    message: format!("{err:#}"),
                    accept,
                });
            }
            Err(server::SubmitDeploymentError::Package(pkg)) => {
                let body = SubmitPackageErrorBody::from(&pkg);
                return Ok(pretty_json_response(StatusCode::CONFLICT, &body));
            }
        }
        if return_id {
            let mut response = pretty_json_response(
                StatusCode::CREATED,
                &DeploymentSubmitResponse {
                    deployment_id: deployment_id.to_string(),
                },
            );
            response.headers_mut().insert(
                header::LOCATION,
                format!("/v1/deployments/{deployment_id}")
                    .parse()
                    .expect("deployment location must be a valid header value"),
            );
            Ok(response)
        } else {
            Ok(StatusCode::NO_CONTENT.into_response())
        }
    }

    /// Fetch a deployment file blob by content digest.
    #[utoipa::path(
        get,
        path = "/v1/files/{digest}",
        tag = "deployments",
        params(("digest" = String, Path, description = "Content digest (sha256:...)")),
        responses(
            (status = 200, description = "Blob bytes", body = Vec<u8>),
            (status = 404, description = "Blob not found")
        )
    )]
    #[instrument(skip_all)]
    pub(crate) async fn get_file(
        Path(digest): Path<String>,
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let digest: concepts::ContentDigest = digest.parse().map_err(|err| HttpResponse {
            status: StatusCode::BAD_REQUEST,
            message: format!("invalid digest: {err}"),
            accept,
        })?;
        let cas = state
            .db_pool
            .cas_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;
        let content = cas
            .read_blob(&digest)
            .await
            .map_err(|err| HttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("cannot read file: {err}"),
                accept,
            })?
            .ok_or_else(|| HttpResponse::not_found(accept, Some("file")))?;
        Ok(content.into_response())
    }

    #[instrument(skip_all, fields(deployment_id))]
    pub(crate) async fn switch(
        Path(deployment_id): Path<DeploymentId>,
        state: State<Arc<WebApiState>>,
        accept: AcceptHeader,
        Json(payload): Json<DeploymentSwitchPayload>,
    ) -> Result<Response, HttpResponse> {
        tracing::Span::current().record("deployment_id", tracing::field::display(&deployment_id));
        let action = if payload.apply {
            if payload.allow_unavailable_runtime_config {
                return Err(HttpResponse::bad_request(
                    accept,
                    "`allow_unavailable_runtime_config = true` not allowed with `apply = true`"
                        .to_string(),
                ));
            }
            SwitchDeploymentAction::Activate
        } else {
            SwitchDeploymentAction::Enqueue(runtime_config_availability_from_bool(
                payload.allow_unavailable_runtime_config,
            ))
        };
        let outcome = crate::command::server::switch_deployment(
            state.deployment_switch_manager.clone(),
            deployment_id,
            action,
        )
        .await
        .map_err(|err| match err {
            crate::command::server::SwitchError::Busy => HttpResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "another deployment submit or switch is already running".to_string(),
                accept,
            },
            crate::command::server::SwitchError::NotFound => {
                HttpResponse::not_found(accept, Some("deployment"))
            }
            crate::command::server::SwitchError::MissingRuntimeConfig(e) => HttpResponse {
                status: StatusCode::BAD_REQUEST,
                message: e.to_string(),
                accept,
            },
            crate::command::server::SwitchError::Other(e) => HttpResponse {
                status: StatusCode::BAD_REQUEST,
                message: format!("{e:#}"),
                accept,
            },
        })?;
        info!(%deployment_id, "Deployment switch outcome: {outcome}");
        let ok = match outcome {
            crate::command::server::SwitchOutcome::Switched => DeploymentSwitchOutcome::Switched,
            crate::command::server::SwitchOutcome::RestartRequired => {
                DeploymentSwitchOutcome::RestartRequired
            }
        };
        Ok(pretty_json_response(
            StatusCode::OK,
            &DeploymentSwitchResponse { ok },
        ))
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
        Ok(pretty_json_response(StatusCode::OK, &info_ser))
    }

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct BacktraceSourceParams {
        /// File path to retrieve (supports suffix matching if not exact)
        file: String,
        /// Which backtrace version to use for component lookup: "first", "last" (default), or a version number
        version: Option<String>,
    }

    #[derive(Deserialize, Debug, IntoParams)]
    #[into_params(parameter_in = Query)]
    pub(crate) struct ComponentSourceParams {
        file: String,
    }

    #[utoipa::path(
        get,
        path = "/v1/components/{digest}/source",
        tag = "components",
        params(("digest" = String, Path), ComponentSourceParams),
        responses((status = 200, body = String), (status = 404))
    )]
    pub(crate) async fn component_source(
        Path(digest): Path<ComponentDigest>,
        state: State<Arc<WebApiState>>,
        Query(params): Query<ComponentSourceParams>,
        accept: TextDefaultAcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let accept = accept.into();
        let conn = state
            .db_pool
            .external_api_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;
        let digest = conn
            .resolve_source_digest(&digest, &params.file)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?
            .ok_or_else(|| HttpResponse::not_found(accept, "source file"))?;
        let cas = state
            .db_pool
            .cas_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;
        let content = cas
            .read_blob(&digest)
            .await
            .map_err(|e| HttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("cannot read source file: {e}"),
                accept,
            })?
            .ok_or_else(|| HttpResponse::not_found(accept, "source file"))?;
        let content = String::from_utf8(content).map_err(|_| HttpResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "backtrace source is not valid UTF-8".to_string(),
            accept,
        })?;
        Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &content),
            AcceptHeader::Text => content.into_response(),
        })
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
        accept: TextDefaultAcceptHeader,
    ) -> Result<Response, HttpResponse> {
        let accept = accept.into();
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

        let digest = conn
            .resolve_source_digest(&backtrace_info.component_id.component_digest, &params.file)
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;

        let Some(digest) = digest else {
            return Err(HttpResponse::not_found(accept, "source file"));
        };

        let cas = state
            .db_pool
            .cas_conn()
            .await
            .map_err(|e| ErrorWrapper(e, accept))?;
        let content = cas
            .read_blob(&digest)
            .await
            .map_err(|err| HttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("cannot read source file: {err}"),
                accept,
            })?
            .ok_or_else(|| HttpResponse::not_found(accept, "source file"))?;
        let content = String::from_utf8(content).map_err(|_| HttpResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "backtrace source is not valid UTF-8".to_string(),
            accept,
        })?;

        Ok(match accept {
            AcceptHeader::Json => pretty_json_response(StatusCode::OK, &content),
            AcceptHeader::Text => content.into_response(),
        })
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) enum AcceptHeader {
    #[default]
    Json,
    Text,
}

#[derive(AcceptExtractor, Default)]
enum JsonAcceptHeader {
    #[accept(mediatype = "application/json")]
    #[default]
    Json,
}

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for AcceptHeader {
    type Rejection = axum_accept::AcceptRejection;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        JsonAcceptHeader::from_request_parts(parts, state).await?;
        Ok(Self::Json)
    }
}

#[derive(AcceptExtractor, Clone, Copy, Default)]
pub(crate) enum TextDefaultAcceptHeader {
    #[accept(mediatype = "application/json")]
    Json,
    #[accept(mediatype = "text/plain")]
    #[default]
    Text,
}

impl From<TextDefaultAcceptHeader> for AcceptHeader {
    fn from(value: TextDefaultAcceptHeader) -> Self {
        match value {
            TextDefaultAcceptHeader::Json => Self::Json,
            TextDefaultAcceptHeader::Text => Self::Text,
        }
    }
}

struct ErrorWrapper<E>(E, AcceptHeader);

pub(crate) struct HttpResponse {
    status: StatusCode,
    message: String,
    accept: AcceptHeader,
}
impl HttpResponse {
    fn from_cancel_execution_outcome(outcome: CancelOutcome, accept: AcceptHeader) -> Self {
        match outcome {
            CancelOutcome::CancelRequested => HttpResponse {
                status: StatusCode::OK,
                message: "cancellation requested".to_string(),
                accept,
            },
            CancelOutcome::AlreadyFinished => HttpResponse {
                status: StatusCode::CONFLICT,
                message: "already finished".to_string(),
                accept,
            },
            CancelOutcome::AlreadyCancelling => HttpResponse {
                status: StatusCode::CONFLICT,
                message: "already cancelling".to_string(),
                accept,
            },
        }
    }

    fn from_delay_cancel_outcome(outcome: DelayCancelOutcome, accept: AcceptHeader) -> Self {
        match outcome {
            DelayCancelOutcome::Cancelled => HttpResponse {
                status: StatusCode::OK,
                message: "cancelled".to_string(),
                accept,
            },
            DelayCancelOutcome::AlreadyFinished => HttpResponse {
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
                    rest_types::MessageBody::Ok { ok: self.message }
                } else {
                    rest_types::MessageBody::Err { err: self.message }
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
        match err {
            DbErrorWriteNonRetriable::ValidationFailed(reason)
            | DbErrorWriteNonRetriable::IllegalState { reason, .. } => HttpResponse {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                message: reason.to_string(),
                accept,
            },
            DbErrorWriteNonRetriable::Conflict => HttpResponse {
                status: StatusCode::CONFLICT,
                message: "conflict".to_string(),
                accept,
            },
            err => {
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
            err @ SubmitError::ValueTooLarge { .. } => HttpResponse {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                message: err.to_string(),
                accept,
            },
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
    use super::{
        AcceptHeader, RetVal, TextDefaultAcceptHeader, admin, format_execution_status_text,
        nonzero_page_length, parse_join_set_filter,
    };
    use chrono::{DateTime, Utc};
    use concepts::{
        ExecutionFailureKind, SupportedFunctionReturnValue,
        prefixed_ulid::NodeRunId,
        storage::{
            ExecutionRequest, PendingState, PendingStateFinished, PendingStateFinishedError,
            PendingStateFinishedResultKind,
        },
    };

    #[tokio::test]
    #[expect(deprecated)]
    async fn node_run_id_defaults_to_text_and_supports_json() {
        let text_response = admin::node_run_id(TextDefaultAcceptHeader::Text).await;
        assert_eq!(
            text_response.headers()[http::header::CONTENT_TYPE],
            "text/plain; charset=utf-8"
        );
        let text = axum::body::to_bytes(text_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text_id: NodeRunId = std::str::from_utf8(&text).unwrap().parse().unwrap();

        let json_response = admin::node_run_id(TextDefaultAcceptHeader::Json).await;
        assert_eq!(
            json_response.headers()[http::header::CONTENT_TYPE],
            "application/json"
        );
        let json = axum::body::to_bytes(json_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_id: NodeRunId = serde_json::from_slice(&json).unwrap();

        assert_eq!(text_id, json_id);
    }

    #[test]
    fn pagination_length_must_be_nonzero() {
        let Ok(length) = nonzero_page_length(1, AcceptHeader::Json) else {
            panic!("positive page length must be accepted");
        };
        assert_eq!(1, length.get());
        assert_eq!(
            http::StatusCode::BAD_REQUEST,
            nonzero_page_length(0, AcceptHeader::Json)
                .unwrap_err()
                .status
        );
    }

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
    fn execution_failure_retval_has_canonical_field() {
        let failure = concepts::FinishedExecutionFailure {
            kind: ExecutionFailureKind::TimedOut,
            reason: Some("timed out".to_string()),
            detail: None,
        };

        assert_eq!(
            serde_json::to_value(RetVal::from(
                SupportedFunctionReturnValue::ExecutionFailure(failure.clone())
            ))
            .unwrap(),
            serde_json::json!({
                "execution_failed": failure,
            })
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

    #[test]
    fn response_join_set_filter_requires_canonical_id() {
        parse_join_set_filter("n:session-name").unwrap();
        parse_join_set_filter("session-name").unwrap_err();
    }

    fn parse_dt(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }
}
