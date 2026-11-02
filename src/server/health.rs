use axum::{Router, extract::State, http::StatusCode, routing::get};
use concepts::storage::DbPool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::warn;

/// Serves `GET /health` without authentication and outside of the idle detector's activity tracking,
/// so that a platform probe neither needs the API token nor keeps the server from going idle.
pub(crate) fn router(db_pool: Arc<dyn DbPool>) -> Router {
    Router::new()
        .route("/health", get(health))
        .with_state(db_pool)
}

pub(crate) async fn serve(listener: TcpListener, db_pool: Arc<dyn DbPool>) -> std::io::Result<()> {
    axum::serve(listener, router(db_pool)).await
}

async fn health(State(db_pool): State<Arc<dyn DbPool>>) -> (StatusCode, &'static str) {
    let read = match db_pool.external_api_conn().await {
        Ok(conn) => conn
            .get_app_checkpoint()
            .await
            .map_err(|err| err.to_string()),
        Err(err) => Err(err.to_string()),
    };
    match read {
        Ok(_) => (StatusCode::OK, "ok\n"),
        Err(err) => {
            warn!("Health check failed: {err}");
            (StatusCode::SERVICE_UNAVAILABLE, "unavailable\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use concepts::storage::DbPoolCloseable;
    use db_sqlite::sqlite_dao::{SqliteConfig, SqlitePool};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_reads_database() {
        test_utils::set_up();
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = SqlitePool::new(file.path(), SqliteConfig::default())
            .await
            .unwrap();
        let app = router(Arc::new(db.clone()));
        let request = || {
            axum::http::Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap()
        };
        let response = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        db.close().await;
        let response = app.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
