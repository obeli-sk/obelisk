use axum::{
    Json, Router,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing,
};
use axum_accept::AcceptExtractor;
use concepts::{
    ExecutionId, FinishedExecutionError, FunctionFqn, SupportedFunctionReturnValue,
    storage::{DbErrorRead, DbErrorWrite, DbErrorWriteNonRetriable, DbPool, ExecutionEventInner},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use val_json::wast_val::WastVal;

use crate::command::server::{self, ComponentConfigRegistryRO, SubmitError};

#[derive(Clone)]
pub(crate) struct WebApiState {
    pub(crate) db_pool: Arc<dyn DbPool>,
    pub(crate) component_registry_ro: ComponentConfigRegistryRO,
}

pub(crate) fn app_router(state: WebApiState) -> Router {
    Router::new()
        .nest("/v1", v1_router())
        .with_state(Arc::new(state))
}

fn v1_router() -> Router<Arc<WebApiState>> {
    Router::new()
        .route("/execution-id", routing::get(execution_id_generate))
        .route(
            "/executions/{execution-id}/status",
            routing::get(execution_status_get),
        )
        .route("/executions/{execution-id}", routing::get(execution_get))
        .route("/executions/{execution-id}", routing::put(execution_put))
}

async fn execution_id_generate(_: State<Arc<WebApiState>>, accept: ExecutionIdAccept) -> Response {
    let id = ExecutionId::generate();
    match accept {
        ExecutionIdAccept::Json => Json(json!(id)).into_response(),
        ExecutionIdAccept::Text => id.to_string().into_response(),
    }
}

async fn execution_status_get(
    Path(execution_id): Path<ExecutionId>,
    state: State<Arc<WebApiState>>,
    accept: ExecutionIdAccept,
) -> Result<Response, ErrorWrapper<DbErrorRead>> {
    let pending_state = state
        .db_pool
        .connection()
        .get_pending_state(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, accept))?;
    Ok(match accept {
        ExecutionIdAccept::Json => Json(json!(pending_state)).into_response(),
        ExecutionIdAccept::Text => pending_state.to_string().into_response(),
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
) -> Result<Response, ErrorWrapper<DbErrorRead>> {
    let last_event = state
        .db_pool
        .connection()
        .get_last_execution_event(&execution_id)
        .await
        .map_err(|e| ErrorWrapper(e, ExecutionIdAccept::Json))?;
    Ok(
        if let ExecutionEventInner::Finished { result, .. } = last_event.event {
            let result = RetVal::from(result);
            Json(json!(result)).into_response()
        } else {
            (
                StatusCode::TOO_EARLY,
                Json(json!({"error":"not finished yet"})),
            )
                .into_response()
        },
    )
}

#[derive(Deserialize)]
struct ExecutionPutPayload {
    ffqn: FunctionFqn,
    params: Vec<serde_json::Value>,
}

async fn execution_put(
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
        Ok(()) => (StatusCode::CREATED, Json(json!({ "ok": "created" }))).into_response(),
        Err(SubmitError::DbErrorWrite(DbErrorWrite::NonRetriable(
            DbErrorWriteNonRetriable::Conflict,
        ))) => (
            StatusCode::CONFLICT,
            Json(json!({ "err": "already exists" })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "err": err.to_string() })),
        )
            .into_response(),
    }
}

#[derive(AcceptExtractor, Clone, Copy)]
enum ExecutionIdAccept {
    #[accept(mediatype = "text/plain")]
    Text,
    #[accept(mediatype = "application/json")]
    Json,
}

struct ErrorWrapper<E>(E, ExecutionIdAccept);

impl IntoResponse for ErrorWrapper<DbErrorRead> {
    fn into_response(self) -> Response {
        let (status, message) = match self.0 {
            DbErrorRead::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            DbErrorRead::Generic(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };
        match self.1 {
            ExecutionIdAccept::Json => (status, Json(json!({ "error": message }))).into_response(),
            ExecutionIdAccept::Text => (status, message).into_response(),
        }
    }
}
