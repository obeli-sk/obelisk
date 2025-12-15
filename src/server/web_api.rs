use axum::{
    Json, Router,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing,
};
use axum_accept::AcceptExtractor;
use concepts::{
    ExecutionId,
    storage::{DbErrorRead, DbPool},
};
use http::StatusCode;
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct WebApiState {
    pub(crate) db_pool: Arc<dyn DbPool>,
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
            routing::get(get_execution_status),
        )
}

async fn execution_id_generate(_: State<Arc<WebApiState>>, accept: ExecutionIdAccept) -> Response {
    let id = ExecutionId::generate();
    match accept {
        ExecutionIdAccept::Json => Json(json!(id)).into_response(),
        ExecutionIdAccept::Text => id.to_string().into_response(),
    }
}

async fn get_execution_status(
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

#[derive(AcceptExtractor, Clone, Copy)]
enum ExecutionIdAccept {
    #[accept(mediatype = "application/json")]
    Json,
    #[accept(mediatype = "text/plain")]
    Text,
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
