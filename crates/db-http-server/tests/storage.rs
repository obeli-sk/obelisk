use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    routing::post,
};
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ComponentRetryConfig, ExecutionId, ExecutionMetadata, FunctionFqn, Params,
    SUPPORTED_RETURN_VALUE_OK_EMPTY,
    prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId},
    storage::*,
};
use db_http::HttpPool;
use db_sqlite::sqlite_dao::{SqliteConfig, SqlitePool};
use obeli_sk_db_http_server::StorageServer;
use secrecy::SecretString;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const FFQN: FunctionFqn = FunctionFqn::new_static("test:storage/operations", "run");
fn now() -> DateTime<Utc> {
    DateTime::from_timestamp_nanos(0)
}
fn create(id: ExecutionId) -> CreateRequest {
    CreateRequest {
        created_at: now(),
        execution_id: id,
        ffqn: FFQN,
        params: Params::empty(),
        parent: None,
        scheduled_at: now(),
        component_id: ComponentId::dummy_activity(),
        deployment_id: DEPLOYMENT_ID_DUMMY,
        metadata: ExecutionMetadata::empty(),
        scheduled_by: None,
        paused: false,
        max_persisted_value_size_bytes: 1_000_000,
    }
}
fn client() -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    db_http::client().unwrap()
}
async fn server(path: &std::path::Path) -> (StorageServer, HttpPool, Arc<SqlitePool>, String) {
    let pool = Arc::new(
        SqlitePool::new(path, SqliteConfig::default())
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = SecretString::from("test-storage-token");
    let service = StorageServer::start(listener, pool.clone(), &token).unwrap();
    let client = HttpPool::new(&url, token, client()).unwrap();
    (service, client, pool, url)
}
async fn claim(pool: &HttpPool, batch_size: u32) -> Result<Vec<LockedExecution>, DbErrorWrite> {
    pool.db_exec_conn()
        .await
        .unwrap()
        .lock_pending_by_ffqns(
            batch_size,
            now(),
            Arc::from([FFQN]),
            now(),
            ComponentId::dummy_activity(),
            DEPLOYMENT_ID_DUMMY,
            ExecutorId::generate(),
            now() + chrono::Duration::hours(1),
            RunId::generate(),
            ComponentRetryConfig::ZERO,
        )
        .await
}

#[tokio::test]
async fn independent_nodes_claim_once_and_reconnect_after_storage_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("storage.sqlite");
    let (service, first, sqlite, url) = server(&path).await;
    let second = HttpPool::new(&url, SecretString::from("test-storage-token"), client()).unwrap();
    let conn = first.connection().await.unwrap();
    for _ in 0..16 {
        conn.create(create(ExecutionId::generate())).await.unwrap();
    }
    let (a, b) = tokio::join!(claim(&first, 8), claim(&second, 8));
    let mut claimed = Vec::new();
    for result in [a, b] {
        match result {
            Ok(executions) => claimed.extend(executions),
            Err(DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::VersionConflict {
                ..
            })) => {}
            Err(DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::IllegalState {
                reason,
                ..
            })) if reason.to_string() == "cannot lock, already locked" => {}
            Err(err) => panic!("unexpected claim failure: {err}"),
        }
    }
    claimed.extend(claim(&second, 16).await.unwrap());
    assert_eq!(16, claimed.len());
    let ids: std::collections::HashSet<_> = claimed
        .iter()
        .map(|execution| &execution.execution_id)
        .collect();
    assert_eq!(16, ids.len());
    let id = claimed[0].execution_id.clone();
    let before = conn.get(&id).await.unwrap();
    first.close().await;
    assert!(matches!(
        first.connection().await,
        Err(DbErrorGeneric::Close)
    ));
    assert_eq!(
        before,
        second.connection().await.unwrap().get(&id).await.unwrap()
    );
    service.close().await.unwrap();
    sqlite.close().await;
    let sqlite = Arc::new(
        SqlitePool::new(&path, SqliteConfig::default())
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind(url.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    let service = StorageServer::start(
        listener,
        sqlite.clone(),
        &SecretString::from("test-storage-token"),
    )
    .unwrap();
    assert_eq!(
        before,
        second.connection().await.unwrap().get(&id).await.unwrap()
    );
    service.close().await.unwrap();
    sqlite.close().await;
}

#[tokio::test]
async fn remote_notifications_and_shutdown_preserve_caller_deadlines() {
    let dir = tempfile::tempdir().unwrap();
    let (service, reader, sqlite, url) = server(&dir.path().join("storage.sqlite")).await;
    let writer = HttpPool::new(&url, SecretString::from("test-storage-token"), client()).unwrap();
    let pending = reader.clone();
    let wait = tokio::spawn(async move {
        pending
            .db_exec_conn()
            .await
            .unwrap()
            .wait_for_pending_by_ffqn(
                now(),
                Arc::from([FFQN]),
                None,
                Box::pin(std::future::pending()),
            )
            .await;
    });
    let id = ExecutionId::generate();
    writer
        .connection()
        .await
        .unwrap()
        .create(create(id.clone()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .unwrap()
        .unwrap();
    let conn = reader.connection().await.unwrap();
    assert!(matches!(
        conn.wait_for_finished_result(
            &id,
            Some(Box::pin(std::future::ready(TimeoutOutcome::Cancel)))
        )
        .await,
        Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Cancel))
    ));
    let locked = claim(&writer, 1).await.unwrap().remove(0);
    let waiting = reader.clone();
    let waiting_id = id.clone();
    let finished = tokio::spawn(async move {
        waiting
            .connection()
            .await
            .unwrap()
            .wait_for_finished_result(&waiting_id, None)
            .await
            .unwrap()
    });
    writer
        .connection()
        .await
        .unwrap()
        .append(
            id.clone(),
            locked.next_version,
            AppendRequest {
                created_at: now(),
                event: ExecutionRequest::Finished {
                    retval: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                    http_client_traces: None,
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(
        SUPPORTED_RETURN_VALUE_OK_EMPTY,
        tokio::time::timeout(Duration::from_secs(2), finished)
            .await
            .unwrap()
            .unwrap()
    );
    let other = ExecutionId::generate();
    writer
        .connection()
        .await
        .unwrap()
        .create(create(other.clone()))
        .await
        .unwrap();
    let waiting = reader.clone();
    let subscription = tokio::spawn(async move {
        waiting
            .connection()
            .await
            .unwrap()
            .wait_for_finished_result(&other, None)
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::timeout(Duration::from_secs(2), service.close())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        subscription.await.unwrap(),
        Err(DbErrorReadWithTimeout::DbErrorRead(DbErrorRead::Generic(
            DbErrorGeneric::Close
        )))
    ));
    sqlite.close().await;
}

#[derive(Clone)]
struct Proxy {
    upstream: String,
    calls: Arc<AtomicUsize>,
}
async fn lose_reply(
    State(proxy): State<Proxy>,
    uri: Uri,
    mut headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    proxy.calls.fetch_add(1, Ordering::Relaxed);
    headers.remove(axum::http::header::HOST);
    let response = client()
        .post(format!("{}{}", proxy.upstream, uri.path()))
        .headers(headers)
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let _ = response.bytes().await.unwrap();
    StatusCode::BAD_GATEWAY
}

#[tokio::test]
async fn a_lost_committed_reply_is_reported_without_replaying_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let (service, direct, sqlite, upstream) = server(&dir.path().join("storage.sqlite")).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let proxy = Router::new().fallback(post(lose_reply)).with_state(Proxy {
        upstream,
        calls: calls.clone(),
    });
    let task = tokio::spawn(async move {
        axum::serve(listener, proxy).await.unwrap();
    });
    let through_proxy =
        HttpPool::new(&url, SecretString::from("test-storage-token"), client()).unwrap();
    let id = ExecutionId::generate();
    let error = through_proxy
        .connection()
        .await
        .unwrap()
        .create(create(id.clone()))
        .await
        .unwrap_err();
    assert!(matches!(error, DbErrorWrite::Generic(_)));
    assert_eq!(1, calls.load(Ordering::Relaxed));
    assert_eq!(
        1,
        direct
            .connection()
            .await
            .unwrap()
            .get(&id)
            .await
            .unwrap()
            .events
            .len()
    );
    task.abort();
    service.close().await.unwrap();
    sqlite.close().await;
}

#[tokio::test]
async fn protocol_rejects_unauthorized_unknown_and_malformed_requests() {
    let dir = tempfile::tempdir().unwrap();
    let (service, pool, sqlite, url) = server(&dir.path().join("storage.sqlite")).await;
    let client = client();
    let response = client
        .post(format!("{url}/v1/DbAdmin/get_storage_status"))
        .body("invalid json")
        .send()
        .await
        .unwrap();
    assert_eq!(StatusCode::UNAUTHORIZED, response.status());
    for (path, body, status) in [
        ("v2/DbAdmin/get_storage_status", "[]", StatusCode::NOT_FOUND),
        ("v1/DbAdmin/no_such_operation", "[]", StatusCode::NOT_FOUND),
        ("v1/DbConnection/create", "[]", StatusCode::BAD_REQUEST),
    ] {
        let response = client
            .post(format!("{url}/{path}"))
            .bearer_auth("test-storage-token")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(status, response.status());
    }
    let error = pool
        .connection()
        .await
        .unwrap()
        .append_batch(now(), Vec::new(), ExecutionId::generate(), Version::new(0))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::ValidationFailed(_))
    ));
    let record = DeploymentRecord {
        deployment_id: concepts::prefixed_ulid::DeploymentId::generate(),
        description: None,
        digest: DeploymentRecord::compute_digest(""),
        created_at: now(),
        last_active_at: None,
        last_active_app_config_digest: None,
        status: DeploymentStatus::Active,
        deployment_toml: String::new(),
        obelisk_version: "test".to_owned(),
        created_by: None,
        files: Vec::new(),
    };
    let error = pool
        .external_api_conn()
        .await
        .unwrap()
        .insert_deployment_with_components(record, Vec::new(), Vec::new(), Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::ValidationFailed(_))
    ));
    pool.verify().await.unwrap();
    service.close().await.unwrap();
    sqlite.close().await;
}

#[tokio::test]
async fn blobs_use_binary_transport_and_system_events_keep_their_cas_digest() {
    let dir = tempfile::tempdir().unwrap();
    let (service, pool, sqlite, url) = server(&dir.path().join("storage.sqlite")).await;
    let cas = pool.cas_conn().await.unwrap();
    let bytes: Vec<u8> = (0..=255).cycle().take(128 * 1024).collect();
    let digest = cas.write_blob(&bytes).await.unwrap();
    assert_eq!(digest, cas.write_blob(&bytes).await.unwrap());
    assert!(cas.contains_blob(&digest).await.unwrap());
    assert_eq!(Some(bytes.clone()), cas.read_blob(&digest).await.unwrap());
    let response = client()
        .get(format!("{url}/v1/blobs/{digest}"))
        .bearer_auth("test-storage-token")
        .send()
        .await
        .unwrap();
    assert_eq!(
        "application/octet-stream",
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap()
    );
    assert_eq!(bytes, response.bytes().await.unwrap());
    let absent = concepts::cas::content_digest(b"absent");
    assert!(!cas.contains_blob(&absent).await.unwrap());
    assert_eq!(None, cas.read_blob(&absent).await.unwrap());
    let admin = pool.admin_conn().await.unwrap();
    let mut event = SystemEvent::new(
        SystemEventCode::ServerStartupCompleted,
        None,
        None,
        serde_json::json!({}),
    )
    .unwrap();
    let error = admin
        .append_system_event_with_cas(event.clone(), bytes.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::ValidationFailed(_))
    ));
    event.cas_digest = Some(digest.clone());
    let event_id = event.event_id;
    admin
        .append_system_event_with_cas(event, bytes)
        .await
        .unwrap();
    assert_eq!(
        Some(digest),
        admin
            .get_system_event(event_id)
            .await
            .unwrap()
            .unwrap()
            .cas_digest
    );
    service.close().await.unwrap();
    sqlite.close().await;
}
