use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{DelayId, ExecutorId, JoinSetId, PrefixedUlid, RunId},
    storage::{
        AppendBatchResponse, AppendRequest, AppendResponse, ClientError, CreateRequest,
        DbConnection, DbConnectionError, DbError, DbPool, ExecutionEvent, ExecutionEventInner,
        ExpiredTimer, HistoryEvent, JoinSetRequest, JoinSetResponse, JoinSetResponseEvent,
        JoinSetResponseEventOuter, LockPendingResponse, LockResponse, LockedExecution, Pagination,
        PendingState, PendingStateFinished, PendingStateFinishedResultKind, SpecificError, Version,
        DUMMY_CREATED, DUMMY_HISTORY_EVENT, DUMMY_INTERMITTENT_FAILURE, DUMMY_INTERMITTENT_TIMEOUT,
    },
    ConfigId, ExecutionId, FinishedExecutionResult, FunctionFqn, StrVariant,
};
use derivative::Derivative;
use rusqlite::{
    named_params,
    types::{FromSql, FromSqlError},
    Connection, OpenFlags, OptionalExtension, Transaction,
};
use std::{
    cmp::max,
    collections::VecDeque,
    error::Error,
    fmt::Debug,
    path::Path,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::{debug, debug_span, error, info, instrument, trace, warn, Level, Span};

#[derive(Debug, Clone, Copy)]
struct DelayReq {
    join_set_id: JoinSetId,
    delay_id: DelayId,
    expires_at: DateTime<Utc>,
}

const PRAGMA: &str = r"
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = true;
PRAGMA busy_timeout = 1000;
PRAGMA cache_size = 10000;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 2147483648;
PRAGMA page_size = 8192;
";

// TODO metadata table with current schema version, migrations

/// Stores execution history. Append only.
const CREATE_TABLE_T_EXECUTION_LOG: &str = r"
CREATE TABLE IF NOT EXISTS t_execution_log (
    execution_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    json_value JSONB NOT NULL,
    version INTEGER NOT NULL,
    variant TEXT NOT NULL,
    join_set_id TEXT,
    history_event_type TEXT GENERATED ALWAYS AS (json_value->>'$.HistoryEvent.event.type') STORED,
    PRIMARY KEY (execution_id, version)
);
";

/// Stores child execution return values. Append only.
const CREATE_TABLE_T_JOIN_SET_RESPONSE: &str = r"
CREATE TABLE IF NOT EXISTS t_join_set_response (
    created_at TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    json_value JSONB NOT NULL,
    join_set_id TEXT NOT NULL,
    delay_id TEXT,
    child_execution_id TEXT,
    PRIMARY KEY (execution_id, join_set_id, delay_id, child_execution_id)
);
";
const CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_join_set_response_execution_id_created_at ON t_join_set_response (execution_id, created_at);
";

/// Stores executions in `PendingState`
/// State to column mapping:
/// `PendingAt`:
/// `BlockedByJoinSet`:     `join_set_id`, `join_set_closing`
/// `Locked`:               `executor_id`, `run_id`, `intermittent_event_count`, `max_retries`, `retry_exp_backoff_millis`, Option<`parent_execution_id`>, Option<`parent_join_set_id`>, `return_type`
/// `Finished` :            `result_kind`, `next_version` is not bumped, points to the finished version.
//FIXME: Remove `return_type`
const CREATE_TABLE_T_STATE: &str = r"
CREATE TABLE IF NOT EXISTS t_state (
    execution_id TEXT NOT NULL,
    next_version INTEGER NOT NULL,
    pending_expires_finished TEXT NOT NULL,
    ffqn TEXT NOT NULL,

    join_set_id TEXT,
    join_set_closing INTEGER,

    executor_id TEXT,
    run_id TEXT,
    intermittent_event_count INTEGER,
    max_retries INTEGER,
    retry_exp_backoff_millis INTEGER,
    parent_execution_id TEXT,
    parent_join_set_id TEXT,
    return_type TEXT,

    result_kind TEXT,

    PRIMARY KEY (execution_id)
)
";
const IDX_T_STATE_LOCK_PENDING: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending ON t_state (ffqn, pending_expires_finished);
";
const IDX_T_STATE_EXPIRED_TIMERS: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_expired_timers ON t_state (pending_expires_finished) WHERE executor_id IS NOT NULL;
";
const IDX_T_STATE_EXECUTION_ID: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_execution_id ON t_state (execution_id);
";
const IDX_T_STATE_FFQN: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_ffqn ON t_state (ffqn);
";

/// Represents [`ExpiredTimer::AsyncDelay`] . Rows are deleted when the delay is processed.
const CREATE_TABLE_T_DELAY: &str = r"
CREATE TABLE IF NOT EXISTS t_delay (
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,
    delay_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (execution_id, join_set_id, delay_id)
)
";

#[expect(clippy::needless_pass_by_value)]
fn convert_err(err: rusqlite::Error) -> DbError {
    if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
        DbError::Specific(SpecificError::NotFound)
    } else {
        error!(backtrace = %std::backtrace::Backtrace::capture(), "Sqlite error {err:?}");
        DbError::Specific(SpecificError::GenericError(StrVariant::Static(
            "sqlite error",
        )))
    }
}

fn parsing_err(err: impl Into<Box<dyn Error>>) -> DbError {
    let err = err.into();
    error!(backtrace = %std::backtrace::Backtrace::capture(), "Validation failed - {err:?}");
    DbError::Specific(SpecificError::ValidationFailed(StrVariant::Arc(Arc::from(
        err.to_string(),
    ))))
}

type ResponseSubscribers = Arc<
    std::sync::Mutex<hashbrown::HashMap<ExecutionId, oneshot::Sender<JoinSetResponseEventOuter>>>,
>;

struct PrefixedUlidWrapper<T: 'static>(PrefixedUlid<T>);
type ExecutorIdW = PrefixedUlidWrapper<concepts::prefixed_ulid::prefix::Exr>;
type JoinSetIdW = PrefixedUlidWrapper<concepts::prefixed_ulid::prefix::JoinSet>;
type ExecutionIdW = PrefixedUlidWrapper<concepts::prefixed_ulid::prefix::E>;
type RunIdW = PrefixedUlidWrapper<concepts::prefixed_ulid::prefix::Run>;
type DelayIdW = PrefixedUlidWrapper<concepts::prefixed_ulid::prefix::Delay>;

impl<T: 'static> FromSql for PrefixedUlidWrapper<T> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let str = value.as_str()?;
        let str = str.parse::<PrefixedUlid<T>>().map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert identifier of type:`{type}`, value:`{str}` - {err:?}",
                r#type = std::any::type_name::<T>()
            );
            FromSqlError::InvalidType
        })?;
        Ok(Self(str))
    }
}

struct JsonWrapper<T>(T);
impl<T: serde::de::DeserializeOwned + 'static> FromSql for JsonWrapper<T> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = serde_json::Value::column_result(value)?;
        let value = serde_json::from_value::<T>(value).map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert JSON to type:`{type}` - {err:?}",
                r#type = std::any::type_name::<T>()
            );
            FromSqlError::InvalidType
        })?;
        Ok(Self(value))
    }
}

struct FromStrWrapper<T: FromStr>(T);
impl<T: FromStr<Err = D>, D: Debug> FromSql for FromStrWrapper<T> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = String::column_result(value)?;
        let value = T::from_str(&value).map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert string `{value}` to type:`{type}` - {err:?}",
                r#type = std::any::type_name::<T>()
            );
            FromSqlError::InvalidType
        })?;
        Ok(Self(value))
    }
}

#[derive(Debug)]
struct CombinedStateDTO {
    ffqn: String,
    pending_expires_finished: DateTime<Utc>,
    executor_id: Option<ExecutorId>,
    run_id: Option<RunId>,
    join_set_id: Option<JoinSetId>,
    join_set_closing: Option<bool>,
    result_kind: Option<PendingStateFinishedResultKind>,
}

#[derive(Debug)]
struct CombinedState {
    ffqn: FunctionFqn,
    pending_state: PendingState,
    next_version: Version,
}

struct PendingAt {
    scheduled_at: DateTime<Utc>,
    ffqn: FunctionFqn,
}

#[derive(Default)]
struct IndexUpdated {
    intermittent_event_count: Option<u32>,
    pending_at: Option<PendingAt>,
}

impl CombinedState {
    fn new(dto: &CombinedStateDTO, next_version: Version) -> Result<Self, DbError> {
        let pending_state = match dto {
            CombinedStateDTO {
                ffqn: _,
                pending_expires_finished: scheduled_at,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } => Ok(PendingState::PendingAt {
                scheduled_at: *scheduled_at,
            }),
            CombinedStateDTO {
                ffqn: _,
                pending_expires_finished: lock_expires_at,
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } => Ok(PendingState::Locked {
                executor_id: *executor_id,
                run_id: *run_id,
                lock_expires_at: *lock_expires_at,
            }),
            CombinedStateDTO {
                ffqn: _,
                pending_expires_finished: lock_expires_at,
                executor_id: None,
                run_id: None,
                join_set_id: Some(join_set_id),
                join_set_closing: Some(join_set_closing),
                result_kind: None,
            } => Ok(PendingState::BlockedByJoinSet {
                join_set_id: *join_set_id,
                closing: *join_set_closing,
                lock_expires_at: *lock_expires_at,
            }),
            CombinedStateDTO {
                ffqn: _,
                pending_expires_finished: finished_at,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: Some(result_kind),
            } => Ok(PendingState::Finished {
                finished: PendingStateFinished {
                    finished_at: *finished_at,
                    version: next_version.0,
                    result_kind: *result_kind,
                },
            }),
            _ => {
                error!("Cannot deserialize pending state from  {dto:?}");
                Err(DbError::Specific(SpecificError::ConsistencyError(
                    StrVariant::Static("invalid `t_state`"),
                )))
            }
        }?;
        Ok(Self {
            ffqn: FunctionFqn::from_str(&dto.ffqn).map_err(|parse_err| {
                error!("Error parsing ffqn of {dto:?} - {parse_err:?}");
                DbError::Specific(SpecificError::ConsistencyError(StrVariant::Static(
                    "invalid ffqn value in `t_state`",
                )))
            })?,
            pending_state,
            next_version,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandPriority {
    // Write, Shutdown
    High,
    // Read
    Medium,
    // Lock scan
    Low,
}

#[derive(derivative::Derivative)]
#[derivative(Debug)]
enum ThreadCommand {
    Func {
        #[derivative(Debug = "ignore")]
        func: Box<dyn FnOnce(&mut Connection) + Send>,
        priority: CommandPriority,
    },
    Shutdown(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct SqlitePool {
    shutdown: Arc<AtomicBool>,
    command_tx: tokio::sync::mpsc::Sender<ThreadCommand>,
    response_subscribers: ResponseSubscribers,
    ffqn_to_pending_subscription: Arc<Mutex<hashbrown::HashMap<FunctionFqn, mpsc::Sender<()>>>>,
}

#[async_trait]
impl DbPool<SqlitePool> for SqlitePool {
    fn connection(&self) -> SqlitePool {
        self.clone()
    }

    fn is_closing(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    async fn close(&self) -> Result<(), DbError> {
        let res = self
            .shutdown
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        if res.is_err() {
            return Err(DbError::Specific(SpecificError::GenericError(
                StrVariant::Static("already closed"),
            )));
        }
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let res = self
            .command_tx
            .send(ThreadCommand::Shutdown(shutdown_tx))
            .await;
        if let Err(err) = res {
            error!("Cannot send the shutdown message - {err:?}");
            return Err(DbError::Specific(SpecificError::GenericError(
                StrVariant::Static("cannot send the shutdown message"),
            )));
        }
        shutdown_rx.await.map_err(|err| {
            error!("Cannot wait for shutdown - {err:?}");
            DbError::Specific(SpecificError::GenericError(StrVariant::Static(
                "cannot wait for shutdown",
            )))
        })?;
        Ok(())
    }
}

#[derive(Debug, Derivative, Clone, Copy)]
#[derivative(Default)]
pub struct SqliteConfig {
    #[derivative(Default(value = "100"))]
    pub queue_capacity: usize,
    #[derivative(Default(value = "100"))]
    pub low_prio_threshold: usize,
}

impl SqlitePool {
    #[instrument(level = Level::DEBUG, skip_all, name = "sqlite_new")]
    pub async fn new<P: AsRef<Path>>(path: P, config: SqliteConfig) -> Result<Self, DbError> {
        let path = path.as_ref().to_owned();
        let (init_tx, init_rx) = oneshot::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(config.queue_capacity);
        {
            let shutdown = shutdown.clone();
            let path = path.clone();
            std::thread::spawn(move || {
                let mut conn = match Connection::open_with_flags(path, OpenFlags::default()) {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("Cannot open the connection - {err:?}");
                        init_tx
                            .send(Err(err))
                            .expect("sending init error must succeed");
                        return;
                    }
                };
                let init = |init_tx: oneshot::Sender<_>, pragma| {
                    trace!("Executing `{pragma}`");
                    if let Err(err) = conn.execute(pragma, []) {
                        error!("Cannot run `{pragma}` - {err:?}");
                        init_tx
                            .send(Err(err))
                            .expect("sending init error must succeed");
                        panic!("initialization error")
                    }
                    init_tx
                };
                if let Err(err) = conn.pragma_update(None, "journal_mode", "wal") {
                    error!("Cannot set journal_mode - {err:?}");
                    init_tx
                        .send(Err(err))
                        .expect("sending init error must succeed");
                    return;
                }
                let init_tx = init(init_tx, PRAGMA);
                let init_tx = init(init_tx, CREATE_TABLE_T_EXECUTION_LOG);
                let init_tx = init(init_tx, CREATE_TABLE_T_JOIN_SET_RESPONSE);
                let init_tx = init(init_tx, CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID);
                let init_tx = init(init_tx, CREATE_TABLE_T_STATE);
                let init_tx = init(init_tx, IDX_T_STATE_LOCK_PENDING);
                let init_tx = init(init_tx, IDX_T_STATE_EXPIRED_TIMERS);
                let init_tx = init(init_tx, IDX_T_STATE_EXECUTION_ID);
                let init_tx = init(init_tx, IDX_T_STATE_FFQN);
                let init_tx = init(init_tx, CREATE_TABLE_T_DELAY);
                init_tx.send(Ok(())).expect("sending init OK must succeed");
                let mut vec: Vec<ThreadCommand> = Vec::with_capacity(config.queue_capacity);
                loop {
                    vec.clear();
                    vec.push(
                        command_rx
                            .blocking_recv()
                            .expect("command channel must be open"),
                    );
                    while let Ok(more) = command_rx.try_recv() {
                        vec.push(more);
                    }
                    if shutdown.load(Ordering::SeqCst) {
                        // Ignore everything except the shutdown command
                        for item in &mut vec {
                            if matches!(item, ThreadCommand::Shutdown(_)) {
                                let item = std::mem::replace(
                                    item,
                                    ThreadCommand::Shutdown(oneshot::channel().0),
                                );
                                let callback = assert_matches!(item, ThreadCommand::Shutdown(callback) => callback);
                                callback.send(()).expect("shutdown callback must be open");
                                return;
                            }
                        }
                    }

                    let mut execute = |expected: CommandPriority| {
                        let mut processed = 0;
                        for item in &mut vec {
                            if matches!(item, ThreadCommand::Func { priority, .. } if *priority == expected )
                            {
                                let item = std::mem::replace(
                                    item,
                                    ThreadCommand::Shutdown(oneshot::channel().0),
                                );
                                let func =
                                    assert_matches!(item, ThreadCommand::Func{func, ..} => func);
                                func(&mut conn);
                                processed += 1;
                            }
                        }
                        processed
                    };
                    let processed =
                        execute(CommandPriority::High) + execute(CommandPriority::Medium);
                    if processed < config.low_prio_threshold {
                        execute(CommandPriority::Low);
                    }
                }
            })
        };
        match init_rx.await {
            Ok(Ok(())) => {}
            other => {
                error!("Initialization error - {other:?}");
                return Err(DbError::Specific(SpecificError::GenericError(
                    StrVariant::Static("initialization error"),
                )));
            }
        }
        info!("Sqlite database location: {path:?}");
        Ok(Self {
            shutdown,
            command_tx,
            response_subscribers: Arc::default(),
            ffqn_to_pending_subscription: Arc::default(),
        })
    }

    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn transaction_write<F, T>(&self, func: F) -> Result<T, DbError>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static,
    {
        self.transaction(func, true).await
    }

    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn transaction_read<F, T>(&self, func: F) -> Result<T, DbError>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static,
    {
        self.transaction(func, false).await
    }

    /// Invokes the provided function wrapping a new [`rusqlite::Transaction`] that is committed automatically.
    async fn transaction<F, T>(&self, func: F, write: bool) -> Result<T, DbError>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let parent_span = Span::current();

        self.command_tx
            .send(ThreadCommand::Func {
                priority: if write {
                    CommandPriority::High
                } else {
                    CommandPriority::Medium
                },
                func: Box::new(move |conn| {
                    let res = debug_span!(parent: &parent_span, "tx_begin").in_scope(|| {
                        conn.transaction_with_behavior(if write {
                            rusqlite::TransactionBehavior::Immediate
                        } else {
                            rusqlite::TransactionBehavior::Deferred
                        })
                        .map_err(convert_err)
                    });
                    let res = res.and_then(|mut transaction| {
                        parent_span.in_scope(|| func(&mut transaction).map(|ok| (ok, transaction)))
                    });
                    let res = res.and_then(|(ok, transaction)| {
                        debug_span!(parent: &parent_span, "tx_commit")
                            .in_scope(|| transaction.commit().map(|()| ok).map_err(convert_err))
                    });
                    _ = tx.send(res);
                }),
            })
            .await
            .map_err(|_send_err| DbError::Connection(DbConnectionError::SendError))?;
        rx.await
            .map_err(|_recv_err| DbError::Connection(DbConnectionError::RecvError))?
    }

    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn conn_low_prio<F, T>(&self, func: F) -> Result<T, DbError>
    where
        F: FnOnce(&Connection) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static + Default,
    {
        let (tx, rx) = oneshot::channel();
        let span = tracing::debug_span!("tx_function");
        self.command_tx
            .send(ThreadCommand::Func {
                priority: CommandPriority::Low,
                func: Box::new(move |conn| {
                    _ = tx.send(span.in_scope(|| func(conn)));
                }),
            })
            .await
            .map_err(|_send_err| DbError::Connection(DbConnectionError::SendError))?;
        match rx.await {
            Ok(res) => res,
            Err(_recv_err) => Ok(T::default()), // Dropped computation because of other priorities..
        }
    }

    fn fetch_created_event(
        conn: &Connection,
        execution_id: ExecutionId,
    ) -> Result<CreateRequest, DbError> {
        let mut stmt = conn
            .prepare(
                "SELECT created_at, json_value FROM t_execution_log WHERE \
            execution_id = :execution_id AND (variant = :variant)",
            )
            .map_err(convert_err)?;
        let (created_at, event) = stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":variant": DUMMY_CREATED.variant(),
                },
                |row| {
                    let created_at = row.get("created_at")?;
                    let event = serde_json::from_value::<ExecutionEventInner>(
                        row.get::<_, serde_json::Value>("json_value")?,
                    )
                    .map(|event| (created_at, event))
                    .map_err(|serde| {
                        error!("cannot deserialize `Created` event: {row:?} - `{serde:?}`");
                        parsing_err(serde)
                    });
                    Ok(event)
                },
            )
            .map_err(convert_err)??;
        if let ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
            config_id,
            return_type,
            metadata,
            topmost_parent,
        } = event
        {
            Ok(CreateRequest {
                created_at,
                execution_id,
                ffqn,
                params,
                parent,
                scheduled_at,
                retry_exp_backoff,
                max_retries,
                config_id,
                return_type,
                metadata,
                topmost_parent,
            })
        } else {
            error!("Cannt match `Created` event - {event:?}");
            Err(DbError::Specific(SpecificError::ConsistencyError(
                StrVariant::Static("Cannot deserialize `Created` event"),
            )))
        }
    }

    fn count_intermittent_events(
        conn: &Connection,
        execution_id: ExecutionId,
    ) -> Result<u32, DbError> {
        let mut stmt = conn.prepare(
            "SELECT COUNT(*) as count FROM t_execution_log WHERE execution_id = :execution_id AND (variant = :v1 OR variant = :v2)",
        ).map_err(convert_err)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":v1": DUMMY_INTERMITTENT_TIMEOUT.variant(),
                ":v2": DUMMY_INTERMITTENT_FAILURE.variant(),
            },
            |row| row.get("count"),
        )
        .map_err(convert_err)
    }

    fn get_current_version(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<Version, DbError> {
        let mut stmt = tx.prepare(
            "SELECT version FROM t_execution_log WHERE execution_id = :execution_id ORDER BY version DESC LIMIT 1",
        ).map_err(convert_err)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                let version: usize = row.get("version")?;
                Ok(Version::new(version))
            },
        )
        .map_err(convert_err)
    }

    fn get_next_version(tx: &Transaction, execution_id: ExecutionId) -> Result<Version, DbError> {
        Self::get_current_version(tx, execution_id).map(|ver| Version::new(ver.0 + 1))
    }

    fn check_expected_next_and_appending_version(
        expected_version: &Version,
        appending_version: &Version,
    ) -> Result<(), DbError> {
        if *expected_version != *appending_version {
            return Err(DbError::Specific(SpecificError::VersionMismatch {
                appending_version: appending_version.clone(),
                expected_version: expected_version.clone(),
            }));
        }
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip_all, fields(execution_id = %req.execution_id))]
    fn create_inner(
        tx: &Transaction,
        req: CreateRequest,
    ) -> Result<(AppendResponse, Option<PendingAt>), DbError> {
        debug!("create_inner");
        let version = Version::new(0);
        let execution_id = req.execution_id;
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx.prepare(
                "INSERT INTO t_execution_log (execution_id, created_at, version, json_value, variant, join_set_id ) \
                VALUES (:execution_id, :created_at, :version, :json_value, :variant, :join_set_id)").map_err(convert_err)?;
        let ffqn = req.ffqn.clone();
        let created_at = req.created_at;
        let scheduled_at = req.scheduled_at;
        let event = ExecutionEventInner::from(req);
        let event_ser = serde_json::to_string(&event)
            .map_err(|err| {
                error!("Cannot serialize {event:?} - {err:?}");
                rusqlite::Error::ToSqlConversionFailure(err.into())
            })
            .map_err(convert_err)?;
        stmt.execute(named_params! {
            ":execution_id": &execution_id_str,
            ":created_at": created_at,
            ":version": version.0,
            ":json_value": event_ser,
            ":variant": event.variant(),
            ":join_set_id": event.join_set_id().map(|join_set_id| join_set_id.to_string()),
        })
        .map_err(convert_err)?;
        let next_version = Version::new(version.0 + 1);
        let pending_state = PendingState::PendingAt { scheduled_at };
        let index_updated = Self::update_state(
            tx,
            execution_id,
            pending_state,
            None, // no current version
            &next_version,
            Some(ffqn),
        )?;
        Ok((next_version, index_updated.pending_at))
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %pending_state, %next_version, purge, ?ffqn))]
    fn update_state(
        tx: &Transaction,
        execution_id: ExecutionId,
        pending_state: PendingState,
        expected_current_version_or_create: Option<&Version>, // None iif creating new execution
        next_version: &Version,
        ffqn: Option<FunctionFqn>, // will be fetched from `Created` if required
    ) -> Result<IndexUpdated, DbError> {
        debug!("update_index");
        let execution_id_str = execution_id.to_string();

        if let Some(expected_current_version) = expected_current_version_or_create {
            let mut stmt = tx
                .prepare_cached("DELETE FROM t_state WHERE execution_id = :execution_id AND next_version = :expected_version")
                .map_err(convert_err)?;
            let deleted = stmt
                .execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":expected_version": expected_current_version.0,
                })
                .map_err(convert_err)?;
            if deleted != 1 {
                error!(backtrace = %std::backtrace::Backtrace::capture(), "Failed updating state to {pending_state:?}");
                return Err(DbError::Specific(SpecificError::ConsistencyError(
                    "updating state failed".into(),
                )));
            }
        } else {
            // Creating
            assert_matches!(pending_state, PendingState::PendingAt { .. });
        }
        let ffqn = if let Some(ffqn) = ffqn {
            ffqn
        } else {
            Self::fetch_created_event(tx, execution_id)?.ffqn
        };
        match pending_state {
            PendingState::PendingAt { scheduled_at } => {
                debug!("Setting state `Pending(`{scheduled_at:?}`)");

                let mut stmt =  tx.prepare(
                        "INSERT INTO t_state (execution_id, next_version, pending_expires_finished, ffqn) \
                        VALUES (:execution_id, :next_version, :pending_expires_finished, :ffqn)",
                    ).map_err(convert_err)?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": next_version.0,
                    ":pending_expires_finished": scheduled_at,
                    ":ffqn": ffqn.to_string(),
                })
                .map_err(convert_err)?;
                Ok(IndexUpdated {
                    intermittent_event_count: None,
                    pending_at: Some(PendingAt { scheduled_at, ffqn }),
                })
            }
            PendingState::Locked {
                lock_expires_at,
                executor_id,
                run_id,
            } => {
                debug!(
                    %executor_id,
                    %run_id, "Setting state `Locked({next_version}, {lock_expires_at})`"
                );
                let intermittent_event_count = Self::count_intermittent_events(tx, execution_id)?;
                let create_req = Self::fetch_created_event(tx, execution_id)?;
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO t_state \
                    (execution_id, next_version, pending_expires_finished, ffqn, executor_id, run_id, intermittent_event_count, max_retries, retry_exp_backoff_millis, parent_execution_id, parent_join_set_id, return_type) \
                    VALUES \
                    (:execution_id, :next_version, :pending_expires_finished, :ffqn, :executor_id, :run_id, :intermittent_event_count, :max_retries, :retry_exp_backoff_millis, :parent_execution_id, :parent_join_set_id, :return_type)",
                ).map_err(convert_err)?;
                let return_type = create_req
                    .return_type
                    .map(|return_type| {
                        serde_json::to_string(&return_type).map_err(|err| {
                            error!("Cannot serialize {return_type:?} - {err:?}");
                            rusqlite::Error::ToSqlConversionFailure(err.into())
                        })
                    })
                    .transpose()
                    .map_err(convert_err)?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": next_version.0, // If the lock expires, this version will be used by `expired_timers_watcher`.
                    ":pending_expires_finished": lock_expires_at,
                    ":ffqn": ffqn.to_string(),
                    ":executor_id": executor_id.to_string(),
                    ":run_id": run_id.to_string(),
                    ":intermittent_event_count": intermittent_event_count,
                    ":max_retries": create_req.max_retries,
                    ":retry_exp_backoff_millis": u64::try_from(create_req.retry_exp_backoff.as_millis()).unwrap(),
                    ":parent_execution_id": create_req.parent.map(|(pid, _) | pid.to_string()),
                    ":parent_join_set_id": create_req.parent.map(|(_, join_set_id)| join_set_id.to_string()),
                    ":return_type": return_type,
                }).map_err(convert_err)?;
                Ok(IndexUpdated {
                    intermittent_event_count: Some(intermittent_event_count),
                    pending_at: None,
                })
            }
            PendingState::BlockedByJoinSet {
                join_set_id,
                lock_expires_at,
                closing: join_set_closing,
            } => {
                debug!(%join_set_id, "Setting state `BlockedByJoinSet({next_version},{lock_expires_at})`");
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO t_state (execution_id, next_version, pending_expires_finished, ffqn, join_set_id, join_set_closing) \
                                  VALUES (:execution_id, :next_version, :pending_expires_finished, :ffqn, :join_set_id, :join_set_closing)",
                ).map_err(convert_err)?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": next_version.0,
                    ":pending_expires_finished": lock_expires_at,
                    ":ffqn": ffqn.to_string(),
                    ":join_set_id": join_set_id.to_string(),
                    ":join_set_closing": join_set_closing,
                })
                .map_err(convert_err)?;
                Ok(IndexUpdated::default())
            }
            PendingState::Finished {
                finished:
                    PendingStateFinished {
                        version: finished_version,
                        finished_at,
                        result_kind,
                    },
            } => {
                debug!("Setting state `Finished`");
                let mut stmt = tx
                    .prepare_cached(
                        // the old next_version == finished_version
                        "INSERT INTO t_state (execution_id, next_version, pending_expires_finished, ffqn, result_kind) \
                                      VALUES (:execution_id, :next_version, :pending_expires_finished, :ffqn, :result_kind)",
                    )
                    .map_err(convert_err)?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": finished_version,
                    ":pending_expires_finished": finished_at,
                    ":ffqn": ffqn.to_string(),
                    ":result_kind": result_kind.to_string(),
                })
                .map_err(convert_err)?;
                Ok(IndexUpdated::default())
            }
        }
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %next_version, purge))]
    fn bump_state_next_version(
        tx: &Transaction,
        execution_id: ExecutionId,
        next_version: &Version,
        delay_req: Option<DelayReq>,
    ) -> Result<(), DbError> {
        debug!("update_index_version");
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx.prepare_cached(
            "UPDATE t_state SET next_version = :next_version WHERE execution_id = :execution_id AND next_version = :next_version - 1",
        ).map_err(convert_err)?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id_str,
                ":next_version": next_version.0,
            })
            .map_err(convert_err)?;
        if updated != 1 {
            return Err(DbError::Specific(SpecificError::NotFound));
        }
        if let Some(DelayReq {
            join_set_id,
            delay_id,
            expires_at,
        }) = delay_req
        {
            debug!("Inserting delay to `t_delay`");
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO t_delay (execution_id, join_set_id, delay_id, expires_at) \
                VALUES \
                (:execution_id, :join_set_id, :delay_id, :expires_at)",
                )
                .map_err(convert_err)?;
            stmt.execute(named_params! {
                ":execution_id": execution_id_str,
                ":join_set_id": join_set_id.to_string(),
                ":delay_id": delay_id.to_string(),
                ":expires_at": expires_at,
            })
            .map_err(convert_err)?;
        }
        Ok(())
    }

    fn get_combined_state(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<CombinedState, DbError> {
        let mut stmt = tx.prepare(
            "SELECT ffqn, next_version, pending_expires_finished, executor_id, run_id, join_set_id, join_set_closing, result_kind FROM t_state WHERE \
        execution_id = :execution_id",
        ).map_err(convert_err)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                Ok(CombinedState::new(
                    &CombinedStateDTO {
                        ffqn: row.get("ffqn")?,
                        pending_expires_finished: row
                            .get::<_, DateTime<Utc>>("pending_expires_finished")?,
                        executor_id: row
                            .get::<_, Option<ExecutorIdW>>("executor_id")?
                            .map(|w| w.0),
                        run_id: row.get::<_, Option<RunIdW>>("run_id")?.map(|w| w.0),
                        join_set_id: row
                            .get::<_, Option<JoinSetIdW>>("join_set_id")?
                            .map(|w| w.0),
                        join_set_closing: row.get::<_, Option<bool>>("join_set_closing")?,
                        result_kind: row
                            .get::<_, Option<FromStrWrapper<PendingStateFinishedResultKind>>>(
                                "result_kind",
                            )?
                            .map(|wrapper| wrapper.0),
                    },
                    Version::new(row.get("next_version")?),
                ))
            },
        )
        .map_err(convert_err)?
    }

    #[expect(clippy::too_many_lines)]
    fn list_executions(
        read_tx: &Transaction,
        ffqn: Option<FunctionFqn>,
        pagination: Pagination<ExecutionId>,
    ) -> Result<Vec<(ExecutionId, FunctionFqn, PendingState)>, DbError> {
        let mut where_vec: Vec<String> = vec![];

        let mut params: Vec<(&'static str, &dyn rusqlite::ToSql)> = vec![];
        let limit;
        let mut limit_desc = false;
        let ffqn_string: String;
        let execution_id_string: String;
        if let Some(ffqn) = ffqn {
            where_vec.push("ffqn = :ffqn".to_string());
            ffqn_string = ffqn.to_string();
            params.push((":ffqn", &ffqn_string));
        }
        match pagination {
            Pagination::FirstAfter {
                first,
                after: None,
                including_cursor: _,
            } => limit = first,
            Pagination::FirstAfter {
                first,
                after: Some(after),
                including_cursor,
            } => {
                limit = first;
                execution_id_string = after.to_string();
                where_vec.push(format!(
                    "execution_id {rel} :execution_id",
                    rel = if including_cursor { ">=" } else { ">" }
                ));
                params.push((":execution_id", &execution_id_string));
            }
            Pagination::LastBefore {
                last,
                before: None,
                including_cursor: _,
            } => {
                limit_desc = true;
                limit = last;
            }
            Pagination::LastBefore {
                last,
                before: Some(before),
                including_cursor,
            } => {
                limit_desc = true;
                limit = last;
                execution_id_string = before.to_string();
                where_vec.push(format!(
                    "execution_id {rel} :execution_id",
                    rel = if including_cursor { "<=" } else { "<" }
                ));
                params.push((":execution_id", &execution_id_string));
            }
        }
        let where_str = if where_vec.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_vec.join(" AND "))
        };

        read_tx.prepare(
            &format!("SELECT execution_id, ffqn, next_version, pending_expires_finished, executor_id, run_id, join_set_id, join_set_closing, result_kind \
                FROM t_state {where_str} ORDER BY execution_id {desc} LIMIT {limit}",
                desc = if limit_desc { "DESC" } else { "" } )
        ).map_err(convert_err)?
        .query_map::<_, &[(&'static str, &dyn rusqlite::ToSql)], _>(params.as_ref(), |row| {
            let execution_id_res = row
                .get::<_, String>("execution_id")?
                .parse::<ExecutionId>()
                .map_err(parsing_err);
            let combined_state_res = CombinedState::new(
                &CombinedStateDTO {
                    ffqn: row.get("ffqn")?,
                    pending_expires_finished: row
                        .get::<_, DateTime<Utc>>("pending_expires_finished")?,
                    executor_id: row
                        .get::<_, Option<ExecutorIdW>>("executor_id")?
                        .map(|w| w.0),
                    run_id: row.get::<_, Option<RunIdW>>("run_id")?.map(|w| w.0),
                    join_set_id: row
                        .get::<_, Option<JoinSetIdW>>("join_set_id")?
                        .map(|w| w.0),
                    join_set_closing: row.get::<_, Option<bool>>("join_set_closing")?,
                    result_kind: row
                        .get::<_, Option<FromStrWrapper<PendingStateFinishedResultKind>>>(
                            "result_kind",
                        )?
                        .map(|wrapper| wrapper.0),
                },
                Version::new(row.get("next_version")?),
            );
            Ok(combined_state_res.and_then(|combined_state| {
                execution_id_res.map(|execution_id| {
                    (
                        execution_id,
                        combined_state.ffqn,
                        combined_state.pending_state,
                    )
                })
            }))
        })
        .map_err(convert_err)?
        .collect::<Result<Vec<Result<_, _>>, _>>()
        .map_err(convert_err)?
        .into_iter()
        .collect()
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %run_id, %executor_id))]
    #[expect(clippy::too_many_arguments)]
    fn lock_inner(
        tx: &Transaction,
        created_at: DateTime<Utc>,
        config_id: ConfigId,
        execution_id: ExecutionId,
        run_id: RunId,
        appending_version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockedExecution, DbError> {
        debug!("lock_inner");
        let CombinedState {
            ffqn,
            pending_state,
            next_version: expected_version,
        } = Self::get_combined_state(tx, execution_id)?;
        pending_state
            .can_append_lock(created_at, executor_id, run_id, lock_expires_at)
            .map_err(DbError::Specific)?;
        Self::check_expected_next_and_appending_version(&expected_version, &appending_version)?;

        // Append to `execution_log` table.
        let event = ExecutionEventInner::Locked {
            config_id,
            executor_id,
            lock_expires_at,
            run_id,
        };
        let event_ser = serde_json::to_string(&event)
            .map_err(|err| {
                error!("Cannot serialize {event:?} - {err:?}");
                rusqlite::Error::ToSqlConversionFailure(err.into())
            })
            .map_err(convert_err)?;
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO t_execution_log \
            (execution_id, created_at, json_value, version, variant) \
            VALUES \
            (:execution_id, :created_at, :json_value, :version, :variant)",
            )
            .map_err(convert_err)?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": created_at,
            ":json_value": event_ser,
            ":version": appending_version.0,
            ":variant": event.variant(),
        })
        .map_err(convert_err)?;
        // Update `t_state`
        let pending_state = PendingState::Locked {
            executor_id,
            run_id,
            lock_expires_at,
        };

        let next_version = Version::new(appending_version.0 + 1);
        let intermittent_event_count = Self::update_state(
            tx,
            execution_id,
            pending_state,
            Some(&Version(next_version.0 - 1)),
            &next_version,
            Some(ffqn),
        )?
        .intermittent_event_count
        .expect("intermittent_event_count must be set");
        // Fetch event_history and `Created` event to construct the response.
        let mut events = tx
            .prepare(
                "SELECT json_value FROM t_execution_log WHERE \
                execution_id = :execution_id AND (variant = :v1 OR variant = :v2) \
                ORDER BY version",
            )
            .map_err(convert_err)?
            .query_map(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":v1": DUMMY_CREATED.variant(),
                    ":v2": DUMMY_HISTORY_EVENT.variant(),
                },
                |row| {
                    let event = serde_json::from_value::<ExecutionEventInner>(
                        row.get::<_, serde_json::Value>("json_value")?,
                    )
                    .map_err(|serde| {
                        error!("Cannot deserialize {row:?} - {serde:?}");
                        parsing_err(serde)
                    });
                    Ok(event)
                },
            )
            .map_err(convert_err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(convert_err)?
            .into_iter()
            .collect::<Result<VecDeque<_>, _>>()?;
        let Some(ExecutionEventInner::Created {
            ffqn,
            params,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
            parent,
            metadata,
            topmost_parent,
            ..
        }) = events.pop_front()
        else {
            error!("Execution log must contain at least `Created` event");
            return Err(DbError::Specific(SpecificError::ConsistencyError(
                StrVariant::Static("execution log must contain `Created` event"),
            )));
        };
        let responses = Self::get_responses(tx, execution_id)?;
        let event_history = events
            .into_iter()
            .map(|event| {
                if let ExecutionEventInner::HistoryEvent { event } = event {
                    Ok(event)
                } else {
                    error!("Rows can only contain `Created` and `HistoryEvent` event kinds");
                    Err(DbError::Specific(SpecificError::ConsistencyError(
                        StrVariant::Static(
                            "Rows can only contain `Created` and `HistoryEvent` event kinds",
                        ),
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LockedExecution {
            execution_id,
            metadata,
            run_id,
            version: next_version,
            ffqn,
            params,
            event_history,
            responses,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
            parent,
            intermittent_event_count,
            topmost_parent,
        })
    }

    fn count_join_next(
        tx: &Transaction,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
    ) -> Result<u64, DbError> {
        let mut stmt = tx.prepare(
            "SELECT COUNT(*) as count FROM t_execution_log WHERE execution_id = :execution_id AND join_set_id = :join_set_id \
            AND history_event_type = :join_next",
        ).map_err(convert_err)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":join_set_id": join_set_id.to_string(),
                ":join_next": "JoinNext",
            },
            |row| row.get("count"),
        )
        .map_err(convert_err)
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id))]
    fn append(
        tx: &Transaction,
        execution_id: ExecutionId,
        req: &AppendRequest,
        appending_version: Version,
    ) -> Result<(AppendResponse, Option<PendingAt>), DbError> {
        assert!(!matches!(req.event, ExecutionEventInner::Created { .. }));
        if let ExecutionEventInner::Locked {
            config_id,
            executor_id,
            run_id,
            lock_expires_at,
        } = &req.event
        {
            let locked = Self::lock_inner(
                tx,
                req.created_at,
                config_id.clone(),
                execution_id,
                *run_id,
                appending_version,
                *executor_id,
                *lock_expires_at,
            )?;
            return Ok((locked.version, None));
        }

        let combined_state = Self::get_combined_state(tx, execution_id)?;
        if combined_state.pending_state.is_finished() {
            debug!("Execution is already finished");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("execution is already finished"),
            )));
        }

        Self::check_expected_next_and_appending_version(
            &combined_state.next_version,
            &appending_version,
        )?;
        let event_ser = serde_json::to_string(&req.event)
            .map_err(|err| {
                error!("Cannot serialize {:?} - {err:?}", req.event);
                rusqlite::Error::ToSqlConversionFailure(err.into())
            })
            .map_err(convert_err)?;

        let mut stmt = tx.prepare(
                    "INSERT INTO t_execution_log (execution_id, created_at, json_value, version, variant, join_set_id) \
                    VALUES (:execution_id, :created_at, :json_value, :version, :variant, :join_set_id)").map_err(convert_err)?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": req.created_at,
            ":json_value": event_ser,
            ":version": appending_version.0,
            ":variant": req.event.variant(),
            ":join_set_id": req.event.join_set_id().map(|join_set_id | join_set_id.to_string()),
        })
        .map_err(convert_err)?;
        // Calculate current pending state
        let mut next_version = Version(appending_version.0 + 1);
        let index_action = match &req.event {
            ExecutionEventInner::Created { .. } => {
                unreachable!("handled in the caller")
            }

            ExecutionEventInner::Locked { .. } => {
                unreachable!("handled above")
            }
            ExecutionEventInner::IntermittentFailure { expires_at, .. }
            | ExecutionEventInner::IntermittentTimeout { expires_at } => {
                IndexAction::PendingStateChanged(PendingState::PendingAt {
                    scheduled_at: *expires_at,
                })
            }
            ExecutionEventInner::Finished { result } => {
                next_version = appending_version.clone();
                IndexAction::PendingStateChanged(PendingState::Finished {
                    finished: PendingStateFinished {
                        version: appending_version.0,
                        finished_at: req.created_at,
                        result_kind: PendingStateFinishedResultKind::from(result),
                    },
                })
            }
            ExecutionEventInner::Unlocked => {
                IndexAction::PendingStateChanged(PendingState::PendingAt {
                    scheduled_at: req.created_at,
                })
            }
            ExecutionEventInner::HistoryEvent {
                event:
                    HistoryEvent::JoinSet { .. }
                    | HistoryEvent::JoinSetRequest {
                        request: JoinSetRequest::ChildExecutionRequest { .. },
                        ..
                    }
                    | HistoryEvent::Persist { .. }
                    | HistoryEvent::Schedule { .. },
            } => IndexAction::NoPendingStateChange(None),

            ExecutionEventInner::HistoryEvent {
                event:
                    HistoryEvent::JoinSetRequest {
                        join_set_id,
                        request:
                            JoinSetRequest::DelayRequest {
                                delay_id,
                                expires_at,
                            },
                    },
            } => IndexAction::NoPendingStateChange(Some(DelayReq {
                join_set_id: *join_set_id,
                delay_id: *delay_id,
                expires_at: *expires_at,
            })),

            ExecutionEventInner::HistoryEvent {
                event:
                    HistoryEvent::JoinNext {
                        join_set_id,
                        lock_expires_at,
                        closing,
                    },
            } => {
                // Did the response arrive already?
                let join_next_count = Self::count_join_next(tx, execution_id, *join_set_id)?;
                let nth_response =
                    Self::nth_response(tx, execution_id, *join_set_id, join_next_count - 1)?; // Skip n-1 rows
                trace!("join_next_count: {join_next_count}, nth_response: {nth_response:?}");
                assert!(join_next_count > 0);
                if let Some(JoinSetResponseEventOuter {
                    created_at: nth_created_at,
                    ..
                }) = nth_response
                {
                    // No need to block
                    let scheduled_at = max(*lock_expires_at, nth_created_at);
                    IndexAction::PendingStateChanged(PendingState::PendingAt { scheduled_at })
                } else {
                    IndexAction::PendingStateChanged(PendingState::BlockedByJoinSet {
                        join_set_id: *join_set_id,
                        lock_expires_at: *lock_expires_at,
                        closing: *closing,
                    })
                }
            }
        };

        let pending_at = match index_action {
            IndexAction::PendingStateChanged(pending_state) => {
                let expected_current_version = Version(
                    if let PendingState::Finished {
                        finished:
                            PendingStateFinished {
                                version: finished_version,
                                ..
                            },
                    } = &pending_state
                    {
                        assert_eq!(*finished_version, next_version.0); // `next_state` must not be bumped.
                        next_version.0
                    } else {
                        next_version.0 - 1
                    },
                );

                Self::update_state(
                    tx,
                    execution_id,
                    pending_state,
                    Some(&expected_current_version),
                    &next_version,
                    None,
                )?
                .pending_at
            }
            IndexAction::NoPendingStateChange(delay_req) => {
                Self::bump_state_next_version(tx, execution_id, &next_version, delay_req)?;
                None
            }
        };
        Ok((next_version, pending_at))
    }

    fn append_response(
        tx: &Transaction,
        execution_id: ExecutionId,
        req: &JoinSetResponseEventOuter,
        response_subscribers: &ResponseSubscribers,
    ) -> Result<
        (
            Option<oneshot::Sender<JoinSetResponseEventOuter>>,
            Option<PendingAt>,
        ),
        DbError,
    > {
        let mut stmt = tx.prepare(
            "INSERT INTO t_join_set_response (execution_id, created_at, json_value, join_set_id, delay_id, child_execution_id) \
                    VALUES (:execution_id, :created_at, :json_value, :join_set_id, :delay_id, :child_execution_id)",
        ).map_err(convert_err)?;
        let join_set_id = req.event.join_set_id;
        let event_ser = serde_json::to_string(&req.event.event)
            .map_err(|err| {
                error!("Cannot serialize {:?} - {err:?}", req.event.event);
                rusqlite::Error::ToSqlConversionFailure(err.into())
            })
            .map_err(convert_err)?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": req.created_at,
            ":json_value": event_ser,
            ":join_set_id": join_set_id.to_string(),
            ":delay_id": req.event.event.delay_id().map(|id| id.to_string()),
            ":child_execution_id": req.event.event.child_execution_id().map(|id|id.to_string()),
        })
        .map_err(convert_err)?;

        // if the execution is going to be unblocked by this response...
        let previous_pending_state = Self::get_combined_state(tx, execution_id)?.pending_state;
        let pendnig_at = match previous_pending_state {
            PendingState::BlockedByJoinSet {
                join_set_id: found_join_set_id,
                lock_expires_at, // Set to a future time if the worker is keeping the execution warm waiting for the result.
                closing: _,
            } if join_set_id == found_join_set_id => {
                // update the pending state.
                let pending_state = PendingState::PendingAt {
                    scheduled_at: lock_expires_at, // TODO: test this
                };
                let next_version = Self::get_next_version(tx, execution_id)?;
                Self::update_state(
                    tx,
                    execution_id,
                    pending_state,
                    Some(&next_version), // not changing the version
                    &next_version,
                    None,
                )?
                .pending_at
            }
            _ => None,
        };
        if let JoinSetResponseEvent {
            join_set_id,
            event: JoinSetResponse::DelayFinished { delay_id },
        } = req.event
        {
            debug!(%join_set_id, %delay_id, "Deleting from `t_delay`");
            let mut stmt =
                tx.prepare_cached("DELETE FROM t_delay WHERE execution_id = :execution_id AND join_set_id = :join_set_id AND delay_id = :delay_id").map_err(convert_err)?;
            stmt.execute(named_params! {
                ":execution_id": execution_id.to_string(),
                ":join_set_id": join_set_id.to_string(),
                ":delay_id": delay_id.to_string(),
            })
            .map_err(convert_err)?;
        }
        Ok((
            response_subscribers.lock().unwrap().remove(&execution_id),
            pendnig_at,
        ))
    }

    #[cfg(feature = "test")]
    fn get(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<concepts::storage::ExecutionLog, DbError> {
        let mut stmt = tx
            .prepare(
                "SELECT created_at, json_value FROM t_execution_log WHERE \
                        execution_id = :execution_id ORDER BY version",
            )
            .map_err(convert_err)?;
        let events = stmt
            .query_map(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| {
                    let created_at = row.get("created_at")?;
                    let event = serde_json::from_value::<ExecutionEventInner>(
                        row.get::<_, serde_json::Value>("json_value")?,
                    )
                    .map(|event| ExecutionEvent { created_at, event })
                    .map_err(|serde| {
                        error!("Cannot deserialize {row:?} - {serde:?}");
                        parsing_err(serde)
                    });
                    Ok(event)
                },
            )
            .map_err(convert_err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(convert_err)?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        if events.is_empty() {
            return Err(DbError::Specific(SpecificError::NotFound));
        }
        let combined_state = Self::get_combined_state(tx, execution_id)?;
        let responses = Self::get_responses(tx, execution_id)?;
        Ok(concepts::storage::ExecutionLog {
            execution_id,
            events,
            responses,
            next_version: combined_state.next_version, // In case of finished, this will be the already last version
            pending_state: combined_state.pending_state,
        })
    }

    fn get_execution_event(
        tx: &Transaction,
        execution_id: ExecutionId,
        version: usize,
    ) -> Result<ExecutionEvent, DbError> {
        let mut stmt = tx
            .prepare(
                "SELECT created_at, json_value FROM t_execution_log WHERE \
                        execution_id = :execution_id AND version = :version",
            )
            .map_err(convert_err)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":version": version,
            },
            |row| {
                let created_at = row.get("created_at")?;
                let event = serde_json::from_value::<ExecutionEventInner>(
                    row.get::<_, serde_json::Value>("json_value")?,
                )
                .map(|event| ExecutionEvent { created_at, event })
                .map_err(|serde| {
                    error!("Cannot deserialize {row:?} - {serde:?}");
                    parsing_err(serde)
                });
                Ok(event)
            },
        )
        .map_err(convert_err)?
    }

    fn get_responses(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError> {
        tx.prepare(
            "SELECT created_at, join_set_id, json_value FROM t_join_set_response WHERE \
                execution_id = :execution_id ORDER BY created_at",
        )
        .map_err(convert_err)?
        .query_map(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                let created_at: DateTime<Utc> = row.get("created_at")?;
                let join_set_id = row.get::<_, JoinSetIdW>("join_set_id")?.0;
                let event = row.get::<_, JsonWrapper<JoinSetResponse>>("json_value")?.0;
                Ok(JoinSetResponseEventOuter {
                    event: JoinSetResponseEvent { join_set_id, event },
                    created_at,
                })
            },
        )
        .map_err(convert_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(convert_err)
    }

    fn nth_response(
        tx: &Transaction,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        skip_rows: u64,
    ) -> Result<Option<JoinSetResponseEventOuter>, DbError> {
        tx.prepare(
            "SELECT created_at, join_set_id, json_value FROM t_join_set_response WHERE \
            execution_id = :execution_id AND join_set_id = :join_set_id ORDER BY created_at
            LIMIT 1 OFFSET :offset",
        )
        .map_err(convert_err)?
        .query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":join_set_id": join_set_id.to_string(),
                ":offset": skip_rows,
            },
            |row| {
                let created_at: DateTime<Utc> = row.get("created_at")?;
                let join_set_id = row.get::<_, JoinSetIdW>("join_set_id")?.0;
                let event = row.get::<_, JsonWrapper<JoinSetResponse>>("json_value")?.0;
                Ok(JoinSetResponseEventOuter {
                    event: JoinSetResponseEvent { join_set_id, event },
                    created_at,
                })
            },
        )
        .optional()
        .map_err(convert_err)
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn get_responses_with_offset(
        tx: &Transaction,
        execution_id: ExecutionId,
        skip_rows: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError> {
        tx.prepare(
            "SELECT created_at, join_set_id, json_value FROM t_join_set_response WHERE \
            execution_id = :execution_id ORDER BY created_at
            LIMIT -1 OFFSET :offset",
        )
        .map_err(convert_err)?
        .query_map(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":offset": skip_rows,
            },
            |row| {
                let created_at: DateTime<Utc> = row.get("created_at")?;
                let join_set_id = row.get::<_, JoinSetIdW>("join_set_id")?.0;
                let event = row.get::<_, JsonWrapper<JoinSetResponse>>("json_value")?.0;
                Ok(JoinSetResponseEventOuter {
                    event: JoinSetResponseEvent { join_set_id, event },
                    created_at,
                })
            },
        )
        .map_err(convert_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(convert_err)
    }

    fn get_pending(
        conn: &Connection,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: &[FunctionFqn],
    ) -> Result<Vec<(ExecutionId, Version)>, DbError> {
        let mut execution_ids_versions = Vec::with_capacity(batch_size);

        for ffqn in ffqns {
            // Select executions in PendingAt or Locked after expiry.
            // Finished and BlockedByJoinSet must be excluded.
            let mut stmt = conn
                .prepare(
                    "SELECT execution_id, next_version FROM t_state WHERE \
                            pending_expires_finished <= :pending_expires_finished AND ffqn = :ffqn AND result_kind IS NULL AND join_set_id IS NULL \
                            ORDER BY pending_expires_finished LIMIT :batch_size",
                )
                .map_err(convert_err)?;
            let execs_and_versions = stmt
                .query_map(
                    named_params! {
                        ":pending_expires_finished": pending_at_or_sooner,
                        ":ffqn": ffqn.to_string(),
                        ":batch_size": batch_size - execution_ids_versions.len(),
                    },
                    |row| {
                        let execution_id =
                            row.get::<_, String>("execution_id")?.parse::<ExecutionId>();
                        let version = Version::new(row.get::<_, usize>("next_version")?);
                        Ok(execution_id.map(|e| (e, version)))
                    },
                )
                .map_err(convert_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(convert_err)?
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(parsing_err)?;
            execution_ids_versions.extend(execs_and_versions);
            if execution_ids_versions.len() == batch_size {
                // Prioritieze lowering of db requests, although ffqns later in the list might get starved.
                break;
            }
        }
        Ok(execution_ids_versions)
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn notify_pending(&self, pending_at: PendingAt, created_at: DateTime<Utc>) {
        Self::notify_pending_locked(
            pending_at,
            created_at,
            &self.ffqn_to_pending_subscription.lock().unwrap(),
        );
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn notify_pending_all(
        &self,
        pending_ats: impl Iterator<Item = PendingAt>,
        created_at: DateTime<Utc>,
    ) {
        let ffqn_to_pending_subscription = self.ffqn_to_pending_subscription.lock().unwrap();
        for pending_at in pending_ats {
            Self::notify_pending_locked(pending_at, created_at, &ffqn_to_pending_subscription);
        }
    }

    fn notify_pending_locked(
        pending_at: PendingAt,
        created_at: DateTime<Utc>,
        ffqn_to_pending_subscription: &std::sync::MutexGuard<
            hashbrown::HashMap<FunctionFqn, mpsc::Sender<()>>,
        >,
    ) {
        match pending_at {
            PendingAt { scheduled_at, ffqn } if scheduled_at <= created_at => {
                if let Some(subscription) = ffqn_to_pending_subscription.get(&ffqn) {
                    debug!("Notifying pending subscriber");
                    let _ = subscription.try_send(());
                }
            }
            _ => {}
        }
    }
}

enum IndexAction {
    PendingStateChanged(PendingState),
    NoPendingStateChange(Option<DelayReq>),
}

#[async_trait]
impl DbConnection for SqlitePool {
    #[instrument(level = Level::DEBUG, skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError> {
        debug!("create");
        trace!(?req, "create");
        let created_at = req.created_at;
        let (version, pending_at) = self
            .transaction_write(move |tx| Self::create_inner(tx, req))
            .await?;
        if let Some(pending_at) = pending_at {
            self.notify_pending(pending_at, created_at);
        }
        Ok(version)
    }

    #[instrument(level = Level::TRACE, skip(self))]
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        created_at: DateTime<Utc>,
        config_id: ConfigId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError> {
        trace!("lock_pending");
        let execution_ids_versions = self
            .conn_low_prio(move |conn| {
                Self::get_pending(conn, batch_size, pending_at_or_sooner, &ffqns)
            })
            .await?;
        if execution_ids_versions.is_empty() {
            Ok(vec![])
        } else {
            debug!("Locking {execution_ids_versions:?}");
            self.transaction_write(move |tx| {
                let mut locked_execs = Vec::with_capacity(execution_ids_versions.len());
                // Append lock
                for (execution_id, version) in execution_ids_versions {
                    match Self::lock_inner(
                        tx,
                        created_at,
                        config_id.clone(),
                        execution_id,
                        RunId::generate(),
                        version,
                        executor_id,
                        lock_expires_at,
                    ) {
                        Ok(locked) => locked_execs.push(locked),
                        Err(err) => {
                            warn!("Locking row {execution_id} failed - {err:?}");
                        }
                    }
                }
                Ok(locked_execs)
            })
            .await
        }
    }

    /// Specialized `append` which returns the event history.
    #[instrument(level = Level::DEBUG, skip(self))]
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        config_id: ConfigId,
        execution_id: ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        debug!("lock");
        self.transaction_write(move |tx| {
            let locked = Self::lock_inner(
                tx,
                created_at,
                config_id,
                execution_id,
                run_id,
                version,
                executor_id,
                lock_expires_at,
            )?;
            Ok((locked.event_history, locked.version))
        })
        .await
        .map_err(DbError::from)
    }

    #[instrument(level = Level::DEBUG, skip(self, req))]
    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        debug!(%req, "append");
        trace!(?req, "append");
        // Disallow `Created` event
        let created_at = req.created_at;
        if let ExecutionEventInner::Created { .. } = req.event {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("Cannot append `Created` event - use `create` instead"),
            )));
        }
        let (version, pending_at) = self
            .transaction_write(move |tx| Self::append(tx, execution_id, &req, version))
            .await
            .map_err(DbError::from)?;
        if let Some(pending_at) = pending_at {
            self.notify_pending(pending_at, created_at);
        }
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch))]
    async fn append_batch(
        &self,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError> {
        debug!("append_batch");
        trace!(?batch, "append_batch");
        if batch.is_empty() {
            error!("Empty batch request");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("empty batch request"),
            )));
        }
        if batch
            .iter()
            .any(|event| matches!(event, ExecutionEventInner::Created { .. }))
        {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("Cannot append `Created` event - use `create` instead"),
            )));
        }
        let (version, pending_at) = self
            .transaction_write(move |tx| {
                let mut version = version;
                let mut pending_at = None;
                for event in batch {
                    (version, pending_at) = Self::append(
                        tx,
                        execution_id,
                        &AppendRequest { created_at, event },
                        version,
                    )?;
                }
                Ok((version, pending_at))
            })
            .await
            .map_err(DbError::from)?;
        if let Some(pending_at) = pending_at {
            self.notify_pending(pending_at, created_at);
        }
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch, child_req))]
    async fn append_batch_create_new_execution(
        &self,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbError> {
        debug!("append_batch_create_new_execution");
        trace!(?batch, ?child_req, "append_batch_create_new_execution");
        if batch.is_empty() {
            error!("Empty batch request");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("empty batch request"),
            )));
        }
        if batch
            .iter()
            .any(|event| matches!(event, ExecutionEventInner::Created { .. }))
        {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("Cannot append `Created` event - use `create` instead"),
            )));
        }

        #[instrument(level = Level::TRACE, skip_all)]
        fn append_batch_create_new_execution_inner(
            tx: &mut rusqlite::Transaction,
            created_at: DateTime<Utc>,
            batch: Vec<ExecutionEventInner>,
            execution_id: ExecutionId,
            mut version: Version,
            child_req: Vec<CreateRequest>,
        ) -> Result<(Version, Vec<PendingAt>), DbError> {
            let mut pending_at = None;
            for event in batch {
                (version, pending_at) = SqlitePool::append(
                    tx,
                    execution_id,
                    &AppendRequest { created_at, event },
                    version,
                )?;
            }
            let mut pending_ats = Vec::new();
            if let Some(pending_at) = pending_at {
                pending_ats.push(pending_at);
            }
            for child_req in child_req {
                let (_, pending_at) = SqlitePool::create_inner(tx, child_req)?;
                if let Some(pending_at) = pending_at {
                    pending_ats.push(pending_at);
                }
            }
            Ok((version, pending_ats))
        }

        let (version, pending_ats) = self
            .transaction_write(move |tx| {
                append_batch_create_new_execution_inner(
                    tx,
                    created_at,
                    batch,
                    execution_id,
                    version,
                    child_req,
                )
            })
            .await
            .map_err(DbError::from)?;
        self.notify_pending_all(pending_ats.into_iter(), created_at);
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch, parent_response_event))]
    async fn append_batch_respond_to_parent(
        &self,
        execution_id: ExecutionId,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        version: Version,
        parent_execution_id: ExecutionId,
        parent_response_event: JoinSetResponseEvent,
    ) -> Result<AppendBatchResponse, DbError> {
        debug!("append_batch_respond_to_parent");
        if execution_id == parent_execution_id {
            // Pending state would be wrong
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static(
                    "Parameters `execution_id` and `parent_execution_id` cannot be the same",
                ),
            )));
        }
        trace!(
            ?batch,
            ?parent_response_event,
            "append_batch_respond_to_parent"
        );
        if batch.is_empty() {
            error!("Empty batch request");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("empty batch request"),
            )));
        }
        if batch
            .iter()
            .any(|event| matches!(event, ExecutionEventInner::Created { .. }))
        {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("Cannot append `Created` event - use `create` instead"),
            )));
        }
        let response_subscribers = self.response_subscribers.clone();
        let event = JoinSetResponseEventOuter {
            created_at,
            event: parent_response_event,
        };
        let (version, response_subscriber, pending_ats) = {
            let event = event.clone();
            self.transaction_write(move |tx| {
                let mut version = version;
                let mut pending_at_child = None;
                for event in batch {
                    (version, pending_at_child) = Self::append(
                        tx,
                        execution_id,
                        &AppendRequest { created_at, event },
                        version,
                    )?;
                }

                let (response_subscriber, pending_at_parent) =
                    Self::append_response(tx, parent_execution_id, &event, &response_subscribers)?;
                Ok((
                    version,
                    response_subscriber,
                    vec![pending_at_child, pending_at_parent],
                ))
            })
            .await
            .map_err(DbError::from)?
        };
        if let Some(response_subscriber) = response_subscriber {
            let notified = response_subscriber.send(event);
            debug!("Notifying response subscriber: {notified:?}");
        }
        self.notify_pending_all(pending_ats.into_iter().flatten(), created_at);
        Ok(version)
    }

    #[cfg(feature = "test")]
    #[instrument(level = Level::DEBUG, skip(self))]
    async fn get(
        &self,
        execution_id: ExecutionId,
    ) -> Result<concepts::storage::ExecutionLog, DbError> {
        trace!("get");
        self.transaction_read(move |tx| Self::get(tx, execution_id))
            .await
            .map_err(DbError::from)
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn subscribe_to_next_responses(
        &self,
        execution_id: ExecutionId,
        start_idx: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError> {
        debug!("next_responses");
        let response_subscribers = self.response_subscribers.clone();
        let resp_or_receiver = self
            .transaction_write(move |tx| {
                let responses = Self::get_responses_with_offset(tx, execution_id, start_idx)?;
                if responses.is_empty() {
                    // cannot race as we have the transaction write lock
                    let (sender, receiver) = oneshot::channel();
                    response_subscribers
                        .lock()
                        .unwrap()
                        .insert(execution_id, sender);
                    Ok(itertools::Either::Right(receiver))
                } else {
                    Ok(itertools::Either::Left(responses))
                }
            })
            .await
            .map_err(DbError::from)?;
        match resp_or_receiver {
            itertools::Either::Left(resp) => Ok(resp),
            itertools::Either::Right(receiver) => receiver
                .await
                .map(|resp| vec![resp])
                .map_err(|_| DbError::Connection(DbConnectionError::RecvError)),
        }
    }

    #[instrument(level = Level::DEBUG, skip(self, response_event), fields(join_set_id = %response_event.join_set_id))]
    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbError> {
        debug!("append_response");
        let response_subscribers = self.response_subscribers.clone();
        let event = JoinSetResponseEventOuter {
            created_at,
            event: response_event,
        };
        let (response_subscriber, pending_at) = {
            let event = event.clone();
            self.transaction_write(move |tx| {
                Self::append_response(tx, execution_id, &event, &response_subscribers)
            })
            .await
            .map_err(DbError::from)?
        };
        if let Some(response_subscriber) = response_subscriber {
            debug!("Notifying response subscriber");
            let _ = response_subscriber.send(event);
        }
        if let Some(pending_at) = pending_at {
            self.notify_pending(pending_at, created_at);
        }
        Ok(())
    }

    /// Get currently expired locks and async timers (delay requests)
    #[instrument(level = Level::TRACE, skip(self))]
    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError> {
        trace!("get_expired_timers");
        self.conn_low_prio(
            move |conn| {
                let mut expired_timers = conn.prepare(
                    "SELECT execution_id, join_set_id, delay_id FROM t_delay WHERE expires_at <= :at",
                ).map_err(convert_err)?.query_map(
                        named_params! {
                            ":at": at,
                        },
                        |row| {
                            let execution_id = row.get::<_, ExecutionIdW>("execution_id")?.0;
                            let join_set_id = row.get::<_, JoinSetIdW>("join_set_id")?.0;
                            let delay_id = row.get::<_, DelayIdW>("delay_id")?.0;
                            Ok(ExpiredTimer::AsyncDelay { execution_id, join_set_id, delay_id })
                        },
                    ).map_err(convert_err)?
                    .collect::<Result<Vec<_>, _>>().map_err(convert_err)?
                    ;

                    expired_timers.extend(conn.prepare(
                        "SELECT execution_id, next_version, intermittent_event_count, max_retries, retry_exp_backoff_millis, parent_execution_id, parent_join_set_id, return_type FROM t_state \
                        WHERE pending_expires_finished <= :at AND executor_id IS NOT NULL",
                    ).map_err(convert_err)?.query_map(
                            named_params! {
                                ":at": at,
                            },
                            |row| {
                                let execution_id = row.get::<_, ExecutionIdW>("execution_id")?.0;
                                let version = Version::new(row.get::<_, usize>("next_version")?);

                                let intermittent_event_count = row.get::<_, u32>("intermittent_event_count")?;
                                let max_retries = row.get::<_, u32>("max_retries")?;
                                let retry_exp_backoff = Duration::from_millis(row.get::<_, u64>("retry_exp_backoff_millis")?);
                                let parent_execution_id = row.get::<_, Option<ExecutionIdW>>("parent_execution_id")?.map(|it|it.0);
                                let parent_join_set_id = row.get::<_, Option<JoinSetIdW>>("parent_join_set_id")?.map(|it|it.0);

                                Ok(ExpiredTimer::Lock { execution_id, version, intermittent_event_count, max_retries,
                                    retry_exp_backoff, parent: parent_execution_id.and_then(|pexe| parent_join_set_id.map(|pjs| (pexe, pjs)))})
                            },
                        ).map_err(convert_err)?
                        .collect::<Result<Vec<_>, _>>().map_err(convert_err)?
                    );

                if !expired_timers.is_empty() {
                    debug!("get_expired_timers found {expired_timers:?}");
                }
                Ok(expired_timers)
            }
        )
        .await
        .map_err(DbError::from)
    }

    #[instrument(level = Level::TRACE, skip(self))]
    async fn subscribe_to_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        max_wait: Duration,
    ) {
        let sleep_until = Instant::now() + max_wait;
        let (sender, mut receiver) = mpsc::channel(1);
        {
            let mut ffqn_to_pending_subscription =
                self.ffqn_to_pending_subscription.lock().unwrap();
            for ffqn in ffqns.as_ref() {
                ffqn_to_pending_subscription.insert(ffqn.clone(), sender.clone());
            }
        }
        let Ok(execution_ids_versions) = self
            .conn_low_prio({
                let ffqns = ffqns.clone();
                move |conn| Self::get_pending(conn, 1, pending_at_or_sooner, ffqns.as_ref())
            })
            .await
        else {
            tokio::time::sleep_until(sleep_until).await;
            return;
        };
        if !execution_ids_versions.is_empty() {
            return;
        }
        tokio::select! { // future's liveness: Dropping the loser immediately.
            _ = receiver.recv() => {} // Got results eventually
            () = tokio::time::sleep_until(sleep_until) => {} // Timeout
        }
    }

    async fn wait_for_finished_result(
        &self,
        execution_id: ExecutionId,
        timeout: Option<Duration>,
    ) -> Result<FinishedExecutionResult, ClientError> {
        let execution_result = {
            let fut = async move {
                loop {
                    if let Some(execution_result) = self
                        .transaction_read(move |tx| {
                            let pending_state =
                                Self::get_combined_state(tx, execution_id)?.pending_state;
                            if let PendingState::Finished { finished } = pending_state {
                                let event =
                                    Self::get_execution_event(tx, execution_id, finished.version)?;
                                if let ExecutionEventInner::Finished { result } = event.event {
                                    Ok(Some(result))
                                } else {
                                    error!("Mismatch, expected Finished row: {event:?} based on t_state {finished}");
                                    Err(DbError::Specific(SpecificError::ConsistencyError(
                                        StrVariant::Static(
                                            "cannot get finished event based on t_state version",
                                        ),
                                    )))
                                }
                            } else {
                                Ok(None)
                            }
                        })
                        .await?
                    {
                        return Ok(execution_result);
                    }
                    // FIXME: change to subscription based approach
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };

            if let Some(timeout) = timeout {
                tokio::select! { // future's liveness: Dropping the loser immediately.
                    res = fut => res,
                    () = tokio::time::sleep(timeout) => Err(ClientError::Timeout)
                }
            } else {
                fut.await
            }
        }?;
        Ok(execution_result)
    }

    async fn get_execution_event(
        &self,
        execution_id: ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbError> {
        let version = version.0;
        self.transaction_read(move |tx| Self::get_execution_event(tx, execution_id, version))
            .await
    }

    async fn get_pending_state(&self, execution_id: ExecutionId) -> Result<PendingState, DbError> {
        Ok(self
            .transaction_read(move |tx| Self::get_combined_state(tx, execution_id))
            .await?
            .pending_state)
    }

    async fn list_executions(
        &self,
        ffqn: Option<FunctionFqn>,
        pagination: Pagination<ExecutionId>,
    ) -> Result<Vec<(ExecutionId, FunctionFqn, PendingState)>, DbError> {
        Ok(self
            .transaction_read(move |tx| Self::list_executions(tx, ffqn, pagination))
            .await?)
    }
}

#[cfg(any(test, feature = "tempfile"))]
pub mod tempfile {
    use super::{SqliteConfig, SqlitePool};
    use tempfile::NamedTempFile;

    pub async fn sqlite_pool() -> (SqlitePool, Option<NamedTempFile>) {
        if let Ok(path) = std::env::var("SQLITE_FILE") {
            (
                SqlitePool::new(path, SqliteConfig::default())
                    .await
                    .unwrap(),
                None,
            )
        } else {
            let file = NamedTempFile::new().unwrap();
            let path = file.path();
            (
                SqlitePool::new(path, SqliteConfig::default())
                    .await
                    .unwrap(),
                Some(file),
            )
        }
    }
}
