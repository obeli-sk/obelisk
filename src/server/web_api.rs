use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
    routing,
};
use axum_accept::AcceptExtractor;
use concepts::{
    ExecutionId, FinishedExecutionError, FunctionFqn, SupportedFunctionReturnValue,
    prefixed_ulid::DelayId,
    storage::{
        self, CancelOutcome, DbErrorRead, DbErrorWrite, DbErrorWriteNonRetriable, DbPool,
        ExecutionEventInner,
    },
    time::{ClockFn as _, Now},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use val_json::wast_val::WastVal;
use wasm_workers::activity::cancel_registry::CancelRegistry;

use crate::{
    command::server::{self, ComponentConfigRegistryRO, SubmitError},
    server::web_api::components::components_list,
};

#[derive(Clone)]
pub(crate) struct WebApiState {
    pub(crate) db_pool: Arc<dyn DbPool>,
    pub(crate) component_registry_ro: ComponentConfigRegistryRO,
    pub(crate) cancel_registry: CancelRegistry,
}

pub(crate) fn app_router(state: WebApiState) -> Router {
    Router::new()
        .nest("/v1", v1_router())
        .with_state(Arc::new(state))
}

fn v1_router() -> Router<Arc<WebApiState>> {
    Router::new()
        .route("/delays/{delay-id}/cancel", routing::put(delay_cancel))
        .route("/execution-id", routing::get(execution_id_generate))
        .route(
            "/executions/{execution-id}/cancel",
            routing::put(execution_cancel),
        )
        .route(
            "/executions/{execution-id}/status",
            routing::get(execution_status_get),
        )
        .route("/executions/{execution-id}", routing::get(execution_get))
        .route("/executions/{execution-id}", routing::put(execution_submit))
        .route("/components", routing::get(components_list))
}

async fn execution_id_generate(_: State<Arc<WebApiState>>, accept: AcceptHeader) -> Response {
    let id = ExecutionId::generate();
    match accept {
        AcceptHeader::Json => Json(json!(id)).into_response(),
        AcceptHeader::Text => id.to_string().into_response(),
    }
}

async fn delay_cancel(
    Path(delay_id): Path<DelayId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let conn = state.db_pool.connection();
    let executed_at = Now.now();
    let outcome = storage::cancel_delay(conn.as_ref(), delay_id, executed_at)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(HttpResponse::from_cancel_outcome(outcome, accept).into_response())
}

async fn execution_cancel(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let conn = state.db_pool.connection();
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

async fn execution_status_get(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: AcceptHeader,
) -> Result<Response, HttpResponse> {
    let pending_state = state
        .db_pool
        .connection()
        .get_pending_state(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(match accept {
        AcceptHeader::Json => Json(json!(pending_state)).into_response(),
        AcceptHeader::Text => pending_state.to_string().into_response(),
    })
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum RetVal {
    Ok(Option<WastVal>),
    Err(Option<WastVal>),
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

async fn execution_get(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
) -> Result<Response, HttpResponse> {
    let last_event = state
        .db_pool
        .connection()
        .get_last_execution_event(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, AcceptHeader::Json))?;
    Ok(
        if let ExecutionEventInner::Finished { result, .. } = last_event.event {
            let result = RetVal::from(result);
            Json(json!(result)).into_response()
        } else {
            HttpResponse {
                status: StatusCode::TOO_EARLY,
                message: "not finished yet".to_string(),
                accept: AcceptHeader::Json,
            }
            .into_response()
        },
    )
}

#[derive(Deserialize, Debug)]
struct ExecutionPutPayload {
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
}

async fn execution_submit(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    Json(payload): Json<ExecutionPutPayload>,
) -> Response {
    match server::submit(
        state.db_pool.connection().as_ref(),
        execution_id,
        payload.ffqn,
        payload.params,
        &state.component_registry_ro,
    )
    .await
    {
        Ok(()) => HttpResponse {
            status: StatusCode::CREATED,
            message: "created".to_string(),
            accept: AcceptHeader::Json,
        }
        .into_response(),
        Err(SubmitError::DbErrorWrite(DbErrorWrite::NonRetriable(
            DbErrorWriteNonRetriable::Conflict,
        ))) => HttpResponse {
            status: StatusCode::CONFLICT,
            message: "already exists".to_string(),
            accept: AcceptHeader::Json,
        }
        .into_response(),
        Err(err) => HttpResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
            accept: AcceptHeader::Json,
        }
        .into_response(),
    }
}

pub(crate) mod components {
    use super::{
        AcceptHeader, Arc, Deserialize, FunctionFqn, IntoResponse, Json, Query, Response,
        Serialize, State, WebApiState, json,
    };
    use concepts::{
        ComponentId, ComponentType, FunctionExtension, FunctionMetadata, ParameterType,
    };
    use std::fmt::Write as _;

    #[derive(Deserialize, Debug)]
    pub(crate) struct ComponentsListParams {
        r#type: Option<ComponentType>,
        name: Option<String>,
        digest: Option<String>,
        exports: Option<bool>,
        imports: Option<bool>,
        extensions: Option<bool>,
        submittable: Option<bool>,
    }

    pub(crate) async fn components_list(
        state: State<Arc<WebApiState>>,
        Query(params): Query<ComponentsListParams>,
        accept: AcceptHeader,
    ) -> Response {
        let extensions = params.extensions.unwrap_or_default();
        let mut components = state.component_registry_ro.list(extensions);

        if let Some(name) = params.name {
            components.retain(|c| c.component_id.name.as_ref() == name);
        }
        if let Some(digest) = params.digest {
            components
                .retain(|c| c.component_id.input_digest.digest_base16_without_prefix() == digest);
        }
        if let Some(ty) = params.r#type {
            components.retain(|c| c.component_id.component_type == ty);
        }
        let exports = params.exports.unwrap_or_default();
        let imports = params.imports.unwrap_or_default();
        let components: Vec<_> = components
            .into_iter()
            .map(|c| {
                let (exports, exports_ext) = if exports {
                    let mut exports = Vec::new();
                    let mut exports_ext = Vec::new();
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
                        if export.extension.is_none() {
                            exports.push(FunctionMetadataLite::from(export));
                        } else if extensions {
                            exports_ext.push(FunctionMetadataLite::from(export));
                        }
                    }

                    (
                        Some(exports),
                        if extensions { Some(exports_ext) } else { None },
                    )
                } else {
                    (None, None)
                };

                ComponentConfig {
                    component_id: c.component_id,
                    imports: if imports {
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
                    exports_ext,
                }
            })
            .collect();

        match accept {
            AcceptHeader::Json => Json(json!(components)).into_response(),
            AcceptHeader::Text => {
                let mut output = String::new();
                for component in components {
                    writeln!(output, "{}", component.component_id).expect("writing to string");
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
        #[serde(skip_serializing_if = "Option::is_none")]
        exports_ext: Option<Vec<FunctionMetadataLite>>,
    }

    #[derive(serde::Serialize)]
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

    #[derive(serde::Serialize)]
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

struct HttpResponse {
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
impl From<ErrorWrapper<DbErrorRead>> for HttpResponse {
    fn from(value: ErrorWrapper<DbErrorRead>) -> Self {
        let (status, message) = match value.0 {
            DbErrorRead::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            DbErrorRead::Generic(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
        };
        HttpResponse {
            status,
            message,
            accept: value.1,
        }
    }
}
impl From<ErrorWrapper<DbErrorWrite>> for HttpResponse {
    fn from(value: ErrorWrapper<DbErrorWrite>) -> Self {
        let (status, message) = match value.0 {
            DbErrorWrite::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            DbErrorWrite::Generic(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
            DbErrorWrite::NonRetriable(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };
        HttpResponse {
            status,
            message,
            accept: value.1,
        }
    }
}
