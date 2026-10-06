//! Local SQL-over-HTTP v3 test server using the real `TursoDB` engine.
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::Mutex;

type SqlResult<T> = turso::Result<T>;
struct Stream {
    conn: turso::Connection,
    stored: HashMap<i64, String>,
}
struct ServerState {
    directory: std::path::PathBuf,
    databases: Mutex<HashMap<String, turso::Database>>,
    streams: Mutex<HashMap<String, Arc<Mutex<Stream>>>>,
    next: AtomicU64,
    lost_commits: Mutex<HashSet<String>>,
    truncated_begins: Mutex<HashSet<String>>,
    expired_statements: Mutex<HashMap<String, String>>,
    expired_commits: Mutex<HashSet<String>>,
    failed_statements: Mutex<HashMap<String, String>>,
    commits: Mutex<HashMap<String, u64>>,
    idle_aborts: Mutex<HashMap<String, u64>>,
}

pub struct TestServer {
    pub url: String,
    state: Arc<ServerState>,
    task: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}
impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl TestServer {
    pub async fn truncate_next_begin_response(&self, database: &str) {
        self.state
            .truncated_begins
            .lock()
            .await
            .insert(database.to_owned());
    }
    pub async fn lose_next_commit_response(&self, database: &str) {
        self.state
            .lost_commits
            .lock()
            .await
            .insert(database.to_owned());
    }
    pub async fn expire_next_statement(&self, database: &str, prefix: &str) {
        self.state
            .expired_statements
            .lock()
            .await
            .insert(database.to_owned(), prefix.to_owned());
    }
    pub async fn expire_next_commit(&self, database: &str) {
        self.state
            .expired_commits
            .lock()
            .await
            .insert(database.to_owned());
    }
    pub async fn fail_next_statement_response(&self, database: &str, fragment: &str) {
        self.state
            .failed_statements
            .lock()
            .await
            .insert(database.to_owned(), fragment.to_owned());
    }
    pub async fn commit_sequence_count(&self, database: &str) -> u64 {
        self.state
            .commits
            .lock()
            .await
            .get(database)
            .copied()
            .unwrap_or(0)
    }
    pub async fn idle_abort_count(&self, database: &str) -> u64 {
        self.state
            .idle_aborts
            .lock()
            .await
            .get(database)
            .copied()
            .unwrap_or(0)
    }
    pub async fn start() -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(ServerState {
            directory: directory.path().to_owned(),
            databases: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            lost_commits: Mutex::new(HashSet::new()),
            truncated_begins: Mutex::new(HashSet::new()),
            expired_statements: Mutex::new(HashMap::new()),
            expired_commits: Mutex::new(HashSet::new()),
            failed_statements: Mutex::new(HashMap::new()),
            commits: Mutex::new(HashMap::new()),
            idle_aborts: Mutex::new(HashMap::new()),
        });
        let app = Router::new()
            .route("/{db}/v3/pipeline", post(pipeline))
            .route("/{db}/v3/cursor", post(cursor))
            .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                tracing::error!(%error,"test SQL server stopped");
            }
        });
        Ok(Self {
            url,
            state,
            task,
            _directory: directory,
        })
    }
}
impl ServerState {
    async fn take_failed_statement(&self, database: &str, sql: &str) -> bool {
        let mut faults = self.failed_statements.lock().await;
        let matched = faults
            .get(database)
            .is_some_and(|fragment| sql.contains(fragment));
        if matched {
            faults.remove(database);
        }
        matched
    }
    async fn take_expired_statement(&self, database: &str, sql: &str) -> bool {
        let mut faults = self.expired_statements.lock().await;
        let matched = faults
            .get(database)
            .is_some_and(|prefix| sql.trim_start().starts_with(prefix));
        if matched {
            faults.remove(database);
        }
        matched
    }
    async fn rollback_for_idle(
        &self,
        database: &str,
        conn: &turso::Connection,
    ) -> SqlResult<Value> {
        conn.execute("ROLLBACK", ()).await?;
        *self
            .idle_aborts
            .lock()
            .await
            .entry(database.to_owned())
            .or_default() += 1;
        Err(idle_abort())
    }
    async fn stream(
        &self,
        db: &str,
        baton: Option<&str>,
    ) -> Result<(String, Arc<Mutex<Stream>>), (StatusCode, String)> {
        if let Some(baton) = baton {
            return self
                .streams
                .lock()
                .await
                .get(baton)
                .cloned()
                .map(|s| (baton.to_owned(), s))
                .ok_or_else(|| (StatusCode::NOT_FOUND, "stream not found".to_owned()));
        }
        if db.is_empty() || !db.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err((StatusCode::BAD_REQUEST, "invalid database name".to_owned()));
        }
        let mut databases = self.databases.lock().await;
        if !databases.contains_key(db) {
            let database = turso::Builder::new_local(
                self.directory
                    .join(format!("{db}.db"))
                    .to_str()
                    .expect("temporary path is UTF-8"),
            )
            .build()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            if db.starts_with("mvcc_") {
                database
                    .connect()
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
                    .pragma_update("journal_mode", "'mvcc'")
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }
            databases.insert(db.to_owned(), database);
        }
        let conn = databases[db]
            .connect()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let baton = format!("{}", self.next.fetch_add(1, Ordering::Relaxed));
        let stream = Arc::new(Mutex::new(Stream {
            conn,
            stored: HashMap::new(),
        }));
        self.streams
            .lock()
            .await
            .insert(baton.clone(), stream.clone());
        Ok((baton, stream))
    }
}
fn idle_abort() -> turso::Error {
    turso::Error::Error("SQLite error: interactive transaction was rolled back because the stream was idle for too long; retry the transaction".to_owned())
}
fn error(error: &turso::Error) -> Value {
    let code = match error {
        turso::Error::Busy(_) => "SQLITE_BUSY",
        turso::Error::BusySnapshot(_) => "SQLITE_BUSY_SNAPSHOT",
        turso::Error::Constraint(_) => "SQLITE_CONSTRAINT",
        _ => "SQLITE_ERROR",
    };
    json!({"message":error.to_string(),"code":code})
}
fn decode(value: &Value) -> SqlResult<turso::Value> {
    Ok(match value["type"].as_str() {
        Some("null") => turso::Value::Null,
        Some("integer") => turso::Value::Integer(
            value["value"]
                .as_str()
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| turso::Error::Misuse("invalid integer".into()))?,
        ),
        Some("float") => turso::Value::Real(
            value["value"]
                .as_f64()
                .ok_or_else(|| turso::Error::Misuse("invalid float".into()))?,
        ),
        Some("text") => turso::Value::Text(value["value"].as_str().unwrap_or_default().to_owned()),
        Some("blob") => turso::Value::Blob(
            STANDARD
                .decode(value["base64"].as_str().unwrap_or_default())
                .map_err(|e| turso::Error::Misuse(e.to_string()))?,
        ),
        _ => return Err(turso::Error::Misuse("invalid argument".into())),
    })
}
fn encode(value: turso::Value) -> Value {
    match value {
        turso::Value::Null => json!({"type":"null"}),
        turso::Value::Integer(v) => json!({"type":"integer","value":v.to_string()}),
        turso::Value::Real(v) => json!({"type":"float","value":v}),
        turso::Value::Text(v) => json!({"type":"text","value":v}),
        turso::Value::Blob(v) => json!({"type":"blob","base64":STANDARD.encode(v)}),
    }
}
async fn execute(stream: &Stream, stmt: &Value) -> SqlResult<Value> {
    let sql = stmt["sql"]
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| {
            stmt["sql_id"]
                .as_i64()
                .and_then(|id| stream.stored.get(&id).cloned())
        })
        .ok_or_else(|| turso::Error::Misuse("missing SQL".into()))?;
    let mut statement = stream.conn.prepare(&sql).await?;
    let params = if let Some(named) = stmt["named_args"].as_array().filter(|a| !a.is_empty()) {
        turso::params::Params::Named(
            named
                .iter()
                .map(|arg| {
                    Ok((
                        std::borrow::Cow::Owned(
                            arg["name"].as_str().unwrap_or_default().to_owned(),
                        ),
                        decode(&arg["value"])?,
                    ))
                })
                .collect::<SqlResult<_>>()?,
        )
    } else {
        turso::params::Params::Positional(
            stmt["args"]
                .as_array()
                .map(|args| args.iter().map(decode).collect::<SqlResult<_>>())
                .transpose()?
                .unwrap_or_default(),
        )
    };
    let mut rows = statement.query(params).await?;
    let cols = rows
        .column_names()
        .into_iter()
        .map(|name| json!({"name":name,"decltype":null}))
        .collect::<Vec<_>>();
    let mut values = Vec::new();
    while let Some(row) = rows.next().await? {
        if stmt["want_rows"].as_bool().unwrap_or(true) {
            let mut result = Vec::new();
            for i in 0..row.column_count() {
                result.push(encode(row.get_value(i)?));
            }
            values.push(result);
        }
    }
    drop(rows);
    Ok(
        json!({"cols":cols,"rows":values,"affected_row_count":statement.n_change(),"last_insert_rowid":stream.conn.last_insert_rowid().to_string()}),
    )
}
fn condition(
    cond: &Value,
    results: &[Option<Value>],
    errors: &[Option<Value>],
    autocommit: bool,
) -> bool {
    let step = cond["step"]
        .as_u64()
        .and_then(|step| usize::try_from(step).ok())
        .unwrap_or(usize::MAX);
    match cond["type"].as_str() {
        Some("ok") => results.get(step).is_some_and(Option::is_some),
        Some("error") => errors.get(step).is_some_and(Option::is_some),
        Some("not") => !condition(&cond["cond"], results, errors, autocommit),
        Some("and") => cond["conds"]
            .as_array()
            .is_some_and(|v| v.iter().all(|c| condition(c, results, errors, autocommit))),
        Some("or") => cond["conds"]
            .as_array()
            .is_some_and(|v| v.iter().any(|c| condition(c, results, errors, autocommit))),
        Some("is_autocommit") => autocommit,
        _ => cond.is_null(),
    }
}
async fn batch(stream: &Stream, batch: &Value) -> SqlResult<Value> {
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for step in batch["steps"]
        .as_array()
        .ok_or_else(|| turso::Error::Misuse("missing batch steps".into()))?
    {
        if condition(
            &step["condition"],
            &results,
            &errors,
            stream.conn.is_autocommit()?,
        ) {
            match execute(stream, &step["stmt"]).await {
                Ok(result) => {
                    results.push(Some(result));
                    errors.push(None);
                }
                Err(e) => {
                    results.push(None);
                    errors.push(Some(error(&e)));
                }
            }
        } else {
            results.push(None);
            errors.push(None);
        }
    }
    Ok(json!({"step_results":results,"step_errors":errors}))
}
async fn request(stream: &mut Stream, req: &Value) -> SqlResult<Value> {
    let kind = req["type"].as_str().unwrap_or_default();
    Ok(match kind {
        "execute" => json!({"type":kind,"result":execute(stream,&req["stmt"]).await?}),
        "batch" => json!({"type":kind,"result":batch(stream,&req["batch"]).await?}),
        "sequence" => {
            stream
                .conn
                .execute_batch(req["sql"].as_str().unwrap_or_default())
                .await?;
            json!({"type":kind})
        }
        "get_autocommit" => json!({"type":kind,"is_autocommit":stream.conn.is_autocommit()?}),
        "store_sql" => {
            stream.stored.insert(
                req["sql_id"].as_i64().unwrap_or_default(),
                req["sql"].as_str().unwrap_or_default().to_owned(),
            );
            json!({"type":kind})
        }
        "close_sql" => {
            stream
                .stored
                .remove(&req["sql_id"].as_i64().unwrap_or_default());
            json!({"type":kind})
        }
        "close" => {
            if !stream.conn.is_autocommit()? {
                stream.conn.execute("ROLLBACK", ()).await?;
            }
            json!({"type":kind})
        }
        _ => return Err(turso::Error::Misuse(format!("unsupported request {kind}"))),
    })
}
async fn pipeline(
    State(state): State<Arc<ServerState>>,
    Path(db): Path<String>,
    Json(req): Json<Value>,
) -> Response {
    let (baton, stream) = match state.stream(&db, req["baton"].as_str()).await {
        Ok(s) => s,
        Err(r) => return r.into_response(),
    };
    let mut stream = stream.lock().await;
    let mut results = Vec::new();
    let mut closed = false;
    for req in req["requests"].as_array().unwrap_or(&Vec::new()) {
        closed |= req["type"] == "close";
        let is_commit = req["type"] == "sequence"
            && req["sql"]
                .as_str()
                .is_some_and(|sql| sql.trim().eq_ignore_ascii_case("COMMIT"));
        if is_commit {
            *state.commits.lock().await.entry(db.clone()).or_default() += 1;
        }
        let sql = req["stmt"]["sql"].as_str().unwrap_or_default();
        if state.take_failed_statement(&db, sql).await {
            if !stream.conn.is_autocommit().unwrap_or(true) {
                let _ = stream.conn.execute("ROLLBACK", ()).await;
            }
            return (
                StatusCode::BAD_GATEWAY,
                "injected statement response failure",
            )
                .into_response();
        }
        let expired = (is_commit && state.expired_commits.lock().await.remove(&db))
            || state.take_expired_statement(&db, sql).await;
        let result = if expired {
            state.rollback_for_idle(&db, &stream.conn).await
        } else {
            request(&mut stream, req).await
        };
        results.push(match result {
            Ok(response) => json!({"type":"ok","response":response}),
            Err(e) => json!({"type":"error","error":error(&e)}),
        });
    }
    let committed = req["requests"].as_array().is_some_and(|requests| {
        requests.iter().any(|r| {
            r["type"] == "sequence"
                && r["sql"]
                    .as_str()
                    .is_some_and(|s| s.trim().eq_ignore_ascii_case("COMMIT"))
        })
    });
    if committed && state.lost_commits.lock().await.remove(&db) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "injected lost commit response",
        )
            .into_response();
    }
    let begun = req["requests"].as_array().is_some_and(|requests| {
        requests.iter().any(|r| {
            r["type"] == "sequence"
                && r["sql"]
                    .as_str()
                    .is_some_and(|s| s.trim().eq_ignore_ascii_case("BEGIN IMMEDIATE"))
        })
    });
    if begun && state.truncated_begins.lock().await.remove(&db) {
        return (StatusCode::OK, "{\"injected_truncated_begin_response\":").into_response();
    }
    if closed {
        state.streams.lock().await.remove(&baton);
    }
    Json(json!({"baton":if closed{None}else{Some(baton)},"base_url":null,"results":results}))
        .into_response()
}
async fn cursor(
    State(state): State<Arc<ServerState>>,
    Path(db): Path<String>,
    Json(req): Json<Value>,
) -> Response {
    let (baton, stream) = match state.stream(&db, req["baton"].as_str()).await {
        Ok(s) => s,
        Err(r) => return r.into_response(),
    };
    let stream = stream.lock().await;
    let mut lines = vec![json!({"baton":baton,"base_url":null}).to_string()];
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for (index, step) in req["batch"]["steps"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .enumerate()
    {
        if condition(
            &step["condition"],
            &results,
            &errors,
            stream.conn.is_autocommit().unwrap_or(true),
        ) {
            let sql = step["stmt"]["sql"].as_str().unwrap_or_default();
            if state.take_failed_statement(&db, sql).await {
                if !stream.conn.is_autocommit().unwrap_or(true) {
                    let _ = stream.conn.execute("ROLLBACK", ()).await;
                }
                return (
                    StatusCode::BAD_GATEWAY,
                    "injected statement response failure",
                )
                    .into_response();
            }
            let result = if state.take_expired_statement(&db, sql).await {
                state.rollback_for_idle(&db, &stream.conn).await
            } else {
                execute(&stream, &step["stmt"]).await
            };
            match result {
                Ok(result) => {
                    lines.push(
                        json!({"type":"step_begin","step":index,"cols":result["cols"]}).to_string(),
                    );
                    for row in result["rows"].as_array().unwrap_or(&Vec::new()) {
                        lines.push(json!({"type":"row","row":row}).to_string());
                    }
                    lines.push(json!({"type":"step_end","affected_row_count":result["affected_row_count"],"last_insert_rowid":result["last_insert_rowid"]}).to_string());
                    results.push(Some(result));
                    errors.push(None);
                }
                Err(e) => {
                    let error = error(&e);
                    lines.push(json!({"type":"step_error","step":index,"error":error}).to_string());
                    results.push(None);
                    errors.push(Some(error));
                }
            }
        } else {
            results.push(None);
            errors.push(None);
        }
    }
    (
        [("content-type", "application/json")],
        format!("{}\n", lines.join("\n")),
    )
        .into_response()
}
