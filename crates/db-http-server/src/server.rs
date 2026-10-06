use crate::{LONG_POLL_MILLIS, error::WireError};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use chrono::{DateTime, Utc};
use concepts::{
    ExecutionId, FunctionFqn,
    component_id::ComponentDigest,
    storage::{DbPool, ResponseCursor, ResponseSubscriptionEnd, TimeoutOutcome},
};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use std::{io, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::{
    net::TcpListener,
    sync::{Mutex, Semaphore, watch},
    task::JoinHandle,
};

const BODY_LIMIT: usize = 128 * 1024 * 1024;
const MAX_LONG_POLLS: usize = 128;

#[derive(Clone)]
struct ServerState {
    pool: Arc<dyn DbPool>,
    authorization: SecretString,
    shutdown: watch::Receiver<bool>,
    subscriptions: Arc<Semaphore>,
    requests: Arc<Semaphore>,
}

pub struct StorageServer {
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<io::Result<()>>>>,
}

impl StorageServer {
    pub fn start(
        listener: TcpListener,
        pool: Arc<dyn DbPool>,
        token: &SecretString,
    ) -> Result<Self, io::Error> {
        if token.expose_secret().is_empty() || token.expose_secret().contains(['\r', '\n']) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "storage token must be nonempty and contain no newlines",
            ));
        }
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let state = ServerState {
            pool,
            authorization: SecretString::from(format!("Bearer {}", token.expose_secret())),
            shutdown: shutdown_rx.clone(),
            subscriptions: Arc::new(Semaphore::new(MAX_LONG_POLLS)),
            requests: Arc::new(Semaphore::new(256)),
        };
        let router = Router::new()
            .route("/v1/{interface}/{operation}", post(operation))
            .route("/v1/blobs", post(write_blob))
            .route(
                "/v1/blobs/{digest}",
                axum::routing::get(read_blob).head(contains_blob),
            )
            .layer(DefaultBodyLimit::max(BODY_LIMIT))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                authenticate,
            ))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.changed().await;
                })
                .await
        });
        Ok(Self {
            shutdown,
            task: Mutex::new(Some(task)),
        })
    }

    pub async fn close(&self) -> Result<(), io::Error> {
        self.shutdown.send_replace(true);
        if let Some(task) = self.task.lock().await.take() {
            task.await.map_err(io::Error::other)??;
        }
        Ok(())
    }
}
impl Drop for StorageServer {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProtocolError {
    #[error("unknown storage operation")]
    UnknownOperation,
    #[error("invalid storage message: {0}")]
    Json(#[from] serde_json::Error),
}
impl IntoResponse for ProtocolError {
    fn into_response(self) -> Response {
        match self {
            Self::UnknownOperation => StatusCode::NOT_FOUND.into_response(),
            Self::Json(_) => StatusCode::BAD_REQUEST.into_response(),
        }
    }
}

async fn authenticate(
    State(state): State<ServerState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let supplied = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .map_or(&[][..], |value| value.as_bytes());
    if !bool::from(supplied.ct_eq(state.authorization.expose_secret().as_bytes())) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(_permit) = state.requests.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    tokio::time::timeout(crate::REQUEST_TIMEOUT, next.run(request))
        .await
        .unwrap_or_else(|_| StatusCode::GATEWAY_TIMEOUT.into_response())
}

pub(crate) fn invalid_argument(reason: &'static str) -> concepts::storage::DbErrorWrite {
    concepts::storage::DbErrorWriteNonRetriable::ValidationFailed(reason.into()).into()
}

pub(crate) fn encode(value: impl Serialize) -> Response {
    Json(value).into_response()
}

async fn operation(
    State(mut state): State<ServerState>,
    Path((interface, method)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Response {
    let result = match interface.as_str() {
        "DbExecutor" => crate::db_exec_conn(state.pool.as_ref(), &method, &body).await,
        "DbConnection" => crate::connection(state.pool.as_ref(), &method, &body).await,
        "DbExternalApi" => crate::external_api_conn(state.pool.as_ref(), &method, &body).await,
        "DbAdmin" => crate::admin_conn(state.pool.as_ref(), &method, &body).await,
        "CasGc" => crate::cas_gc_conn(state.pool.as_ref(), &method, &body).await,
        #[cfg(feature = "test")]
        "DbConnectionTest" => crate::connection_test(state.pool.as_ref(), &method, &body).await,
        "Notifications" => {
            let Ok(_permit) = state.subscriptions.clone().try_acquire_owned() else {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            };
            tokio::select! {
                result = notifications(state.pool.as_ref(), &method, &body) => result,
                _ = state.shutdown.changed() => Ok(encode(Err::<(), _>(WireError::Closed))),
            }
        }
        _ => Err(ProtocolError::UnknownOperation),
    };
    match result {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn notifications(
    pool: &dyn DbPool,
    method: &str,
    body: &[u8],
) -> Result<Response, ProtocolError> {
    let conn = match pool.connection().await {
        Ok(conn) => conn,
        Err(err) => return Ok(encode(Err::<(), _>(WireError::from(err)))),
    };
    match method {
        "pending_ffqn" => {
            let (at, ffqns, digest): (DateTime<Utc>, Arc<[FunctionFqn]>, Option<ComponentDigest>) =
                serde_json::from_slice(body)?;
            conn.wait_for_pending_by_ffqn(
                at,
                ffqns,
                digest,
                Box::pin(tokio::time::sleep(Duration::from_millis(LONG_POLL_MILLIS))),
            )
            .await;
            Ok(encode(Ok::<(), WireError>(())))
        }
        "pending_digest" => {
            let (at, digest): (DateTime<Utc>, ComponentDigest) = serde_json::from_slice(body)?;
            conn.wait_for_pending_by_component_digest(
                at,
                &digest,
                Box::pin(tokio::time::sleep(Duration::from_millis(LONG_POLL_MILLIS))),
            )
            .await;
            Ok(encode(Ok::<(), WireError>(())))
        }
        "responses" => {
            let (id, cursor, millis): (ExecutionId, ResponseCursor, u64) =
                serde_json::from_slice(body)?;
            let result = conn
                .subscribe_to_next_responses(
                    &id,
                    cursor,
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(millis.min(LONG_POLL_MILLIS)))
                            .await;
                        ResponseSubscriptionEnd::PollIntervalElapsed
                    }),
                )
                .await
                .map_err(WireError::from);
            Ok(encode(result))
        }
        "finished" => {
            let (id, millis): (ExecutionId, u64) = serde_json::from_slice(body)?;
            let result = conn
                .wait_for_finished_result(
                    &id,
                    Some(Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(millis.min(LONG_POLL_MILLIS)))
                            .await;
                        TimeoutOutcome::Timeout
                    })),
                )
                .await
                .map_err(WireError::from);
            Ok(encode(result))
        }
        _ => Err(ProtocolError::UnknownOperation),
    }
}

async fn write_blob(State(state): State<ServerState>, body: axum::body::Bytes) -> Response {
    let result = match state.pool.cas_conn().await {
        Ok(conn) => conn.write_blob(&body).await.map_err(WireError::from),
        Err(err) => Err(err.into()),
    };
    Json(result).into_response()
}

async fn read_blob(
    State(state): State<ServerState>,
    Path(digest): Path<concepts::ContentDigest>,
) -> Response {
    let Ok(conn) = state.pool.cas_conn().await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match conn.read_blob(&digest).await {
        Ok(Some(bytes)) => (
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn contains_blob(
    State(state): State<ServerState>,
    Path(digest): Path<concepts::ContentDigest>,
) -> Response {
    let Ok(conn) = state.pool.cas_conn().await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match conn.contains_blob(&digest).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
