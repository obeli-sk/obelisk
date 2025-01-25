use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{DelayId, ExecutorId, JoinSetId, PrefixedUlid, RunId},
    storage::{
        AppendBatchResponse, AppendRequest, AppendResponse, ClientError, CreateRequest,
        DbConnection, DbConnectionError, DbError, DbPool, ExecutionEvent, ExecutionEventInner,
        ExecutionListPagination, ExecutionWithState, ExpiredTimer, HistoryEvent, JoinSetRequest,
        JoinSetResponse, JoinSetResponseEvent, JoinSetResponseEventOuter, LockPendingResponse,
        LockResponse, LockedExecution, Pagination, PendingState, PendingStateFinished,
        PendingStateFinishedResultKind, ResponseWithCursor, SpecificError, Version, VersionType,
        DUMMY_CREATED, DUMMY_HISTORY_EVENT, DUMMY_TEMPORARILY_FAILED, DUMMY_TEMPORARILY_TIMED_OUT,
    },
    ComponentId, ExecutionId, FinishedExecutionResult, FunctionFqn, StrVariant,
};
use hdrhistogram::{Counter, Histogram};
use rusqlite::{
    named_params,
    types::{FromSql, FromSqlError},
    Connection, OpenFlags, OptionalExtension, Params, ToSql, Transaction,
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
/*
mmap_size = 128MB - Set the global memory map so all processes can share some data
https://www.sqlite.org/pragma.html#pragma_mmap_size
https://www.sqlite.org/mmap.html

journal_size_limit = 64 MB - limit on the WAL file to prevent unlimited growth
https://www.sqlite.org/pragma.html#pragma_journal_size_limit

Inspired by https://github.com/rails/rails/pull/49349
*/
const PRAGMA: &str = r"
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = true;
PRAGMA busy_timeout = 1000;
PRAGMA cache_size = 10000;
PRAGMA temp_store = MEMORY;
PRAGMA page_size = 8192;
PRAGMA mmap_size = 134217728;
PRAGMA journal_size_limit = 67108864;
";
const PRAGMA_JOURNAL_MODE: &str = "journal_mode";
const PRAGMA_JOURNAL_MODE_WAL: &str = "wal";

const CREATE_TABLE_T_METADATA: &str = r"
CREATE TABLE IF NOT EXISTS t_metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
";
const T_METADATA_SINGLETON: u32 = 1;
const T_METADATA_EXPECTED_SCHEMA_VERSION: u32 = 1;

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
// Used in `fetch_created` and `get_execution_event`
const CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VERSION: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_version  ON t_execution_log (execution_id, version);
";
// Used in `lock_inner` to filter by execution ID and variant (created or event history)
const CREATE_INDEX_IDX_T_EXECUTION_ID_EXECUTION_ID_VARIANT: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_variant  ON t_execution_log (execution_id, variant);
";

/// Stores child execution return values. Append only.
const CREATE_TABLE_T_JOIN_SET_RESPONSE: &str = r"
CREATE TABLE IF NOT EXISTS t_join_set_response (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    json_value JSONB NOT NULL,
    join_set_id TEXT NOT NULL,
    delay_id TEXT,
    child_execution_id TEXT,
    UNIQUE (execution_id, join_set_id, delay_id, child_execution_id)
);
";
// Used when querying for the next response
const CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID_ID: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_join_set_response_execution_id_id ON t_join_set_response (execution_id, id);
";

/// Stores executions in `PendingState`
/// `state` to column mapping:
/// `PendingAt`:
/// `BlockedByJoinSet`:     `join_set_id`, `join_set_closing`
/// `Locked`:               `executor_id`, `run_id`, `temporary_event_count`, `max_retries`, `retry_exp_backoff_millis`, Option<`parent_execution_id`>, Option<`parent_join_set_id`>
/// `Finished` :            `result_kind`, `next_version` is not bumped, points to the finished version.
const CREATE_TABLE_T_STATE: &str = r"
CREATE TABLE IF NOT EXISTS t_state (
    execution_id TEXT NOT NULL,
    next_version INTEGER NOT NULL,
    pending_expires_finished TEXT NOT NULL,
    ffqn TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,

    join_set_id TEXT,
    join_set_closing INTEGER,

    executor_id TEXT,
    run_id TEXT,
    temporary_event_count INTEGER,
    max_retries INTEGER,
    retry_exp_backoff_millis INTEGER,
    parent_execution_id TEXT,
    parent_join_set_id TEXT,

    result_kind TEXT,

    PRIMARY KEY (execution_id)
)
";
const STATE_PENDING_AT: &str = "PendingAt";
const STATE_BLOCKED_BY_JOIN_SET: &str = "BlockedByJoinSet";
const STATE_LOCKED: &str = "Locked";
const STATE_FINISHED: &str = "Finished";

const IDX_T_STATE_LOCK_PENDING: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending ON t_state (state, pending_expires_finished, ffqn);
";
const IDX_T_STATE_EXPIRED_TIMERS: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_expired_timers ON t_state (pending_expires_finished) WHERE executor_id IS NOT NULL;
";
const IDX_T_STATE_EXECUTION_ID: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_execution_id ON t_state (execution_id);
";
// For `list_executions` by ffqn
const IDX_T_STATE_FFQN: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_ffqn ON t_state (ffqn);
";
// For `list_executions` by creation date
const IDX_T_STATE_CREATED_AT: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_created_at ON t_state (created_at);
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
impl<T: serde::de::DeserializeOwned + 'static + Debug> FromSql for JsonWrapper<T> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = match value {
            rusqlite::types::ValueRef::Text(value) | rusqlite::types::ValueRef::Blob(value) => {
                Ok(value)
            }
            other => {
                error!(
                    backtrace = %std::backtrace::Backtrace::capture(),
                    "Unexpected type when conveting to JSON - expected Text or Blob, got type `{other:?}`",
                );
                Err(FromSqlError::InvalidType)
            }
        }?;
        let value = serde_json::from_slice::<T>(value).map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert JSON value `{value:?}` to type:`{type}` - {err:?}",
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
    state: String,
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
    temporary_event_count: Option<u32>,
    pending_at: Option<PendingAt>,
}

impl CombinedState {
    fn new(dto: &CombinedStateDTO, next_version: Version) -> Result<Self, DbError> {
        let pending_state = match dto {
            CombinedStateDTO {
                state,
                ffqn: _,
                pending_expires_finished: scheduled_at,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } if state == STATE_PENDING_AT => Ok(PendingState::PendingAt {
                scheduled_at: *scheduled_at,
            }),
            CombinedStateDTO {
                state,
                ffqn: _,
                pending_expires_finished: lock_expires_at,
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } if state == STATE_LOCKED => Ok(PendingState::Locked {
                executor_id: *executor_id,
                run_id: *run_id,
                lock_expires_at: *lock_expires_at,
            }),
            CombinedStateDTO {
                state,
                ffqn: _,
                pending_expires_finished: lock_expires_at,
                executor_id: None,
                run_id: None,
                join_set_id: Some(join_set_id),
                join_set_closing: Some(join_set_closing),
                result_kind: None,
            } if state == STATE_BLOCKED_BY_JOIN_SET => Ok(PendingState::BlockedByJoinSet {
                join_set_id: *join_set_id,
                closing: *join_set_closing,
                lock_expires_at: *lock_expires_at,
            }),
            CombinedStateDTO {
                state,
                ffqn: _,
                pending_expires_finished: finished_at,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: Some(result_kind),
            } if state == STATE_FINISHED => Ok(PendingState::Finished {
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

#[derive(derive_more::Debug)]
enum ThreadCommand {
    Func {
        #[debug(skip)]
        func: Box<dyn FnOnce(&mut Connection) + Send>,
        priority: CommandPriority,
        sent_at: Instant,
        name: &'static str,
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

#[derive(Debug, Clone, Copy)]
pub struct SqliteConfig {
    pub queue_capacity: usize,
    pub low_prio_threshold: usize,
}
impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100,
            low_prio_threshold: 100,
        }
    }
}

impl SqlitePool {
    fn init_thread(path: &Path) -> Connection {
        // No need to log errors - propagate error messages via panic.
        // It is OK to panic here - spawned on blocking thread pool, panic will error the initialization.
        fn execute<P: Params>(conn: &Connection, sql: &str, params: P) {
            conn.execute(sql, params)
                .unwrap_or_else(|err| panic!("cannot run `{sql}` - {err:?}"));
        }

        let conn = Connection::open_with_flags(path, OpenFlags::default())
            .unwrap_or_else(|err| panic!("cannot open the connection - {err:?}"));

        conn.pragma_update(None, PRAGMA_JOURNAL_MODE, PRAGMA_JOURNAL_MODE_WAL)
            .unwrap_or_else(|err| panic!("cannot set journal_mode - {err:?}"));
        execute(&conn, PRAGMA, []);

        // t_metadata
        execute(&conn, CREATE_TABLE_T_METADATA, []);
        // Insert row if not exists.
        execute(
            &conn,
            &format!(
                "INSERT INTO t_metadata (id, schema_version, created_at) VALUES
                    ({T_METADATA_SINGLETON}, {T_METADATA_EXPECTED_SCHEMA_VERSION}, ?) ON CONFLICT DO NOTHING"
            ),
            [Utc::now()],
        );
        // Fail on unexpected `schema_version`.
        let actual_version = conn
            .prepare("SELECT schema_version FROM t_metadata")
            .unwrap_or_else(|err| panic!("cannot select schema version - {err:?}"))
            .query_row([], |row| row.get::<_, u32>("schema_version"));

        let actual_version = actual_version.unwrap_or_else(|err| {
            panic!("Cannot read the schema version - {err:?}");
        });
        assert!(actual_version == T_METADATA_EXPECTED_SCHEMA_VERSION,
            "wrong schema version, expected {T_METADATA_EXPECTED_SCHEMA_VERSION}, got {actual_version}");

        // t_execution_log
        execute(&conn, CREATE_TABLE_T_EXECUTION_LOG, []);
        execute(
            &conn,
            CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VERSION,
            [],
        );
        execute(
            &conn,
            CREATE_INDEX_IDX_T_EXECUTION_ID_EXECUTION_ID_VARIANT,
            [],
        );
        // t_join_set_response
        execute(&conn, CREATE_TABLE_T_JOIN_SET_RESPONSE, []);
        execute(
            &conn,
            CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID_ID,
            [],
        );
        // t_state
        execute(&conn, CREATE_TABLE_T_STATE, []);
        execute(&conn, IDX_T_STATE_LOCK_PENDING, []);
        execute(&conn, IDX_T_STATE_EXPIRED_TIMERS, []);
        execute(&conn, IDX_T_STATE_EXECUTION_ID, []);
        execute(&conn, IDX_T_STATE_FFQN, []);
        execute(&conn, IDX_T_STATE_CREATED_AT, []);
        // t_delay
        execute(&conn, CREATE_TABLE_T_DELAY, []);
        conn
    }

    fn connection_rpc(
        mut conn: Connection,
        shutdown: &Arc<AtomicBool>,
        mut command_rx: mpsc::Receiver<ThreadCommand>,
        config: SqliteConfig,
    ) {
        const METRIC_DUMPING_TRESHOLD: usize = 0; // 0 to disable histogram dumping
        let mut vec: Vec<ThreadCommand> = Vec::with_capacity(config.queue_capacity);
        // measure how long it takes to receive the `ThreadCommand`. 1us-1s
        let mut send_hist = Histogram::<u32>::new_with_bounds(1, 1_000_000, 3).unwrap();
        let mut func_histograms = hashbrown::HashMap::new();
        let mut metric_dumping_counter = 0;
        let print_histogram = |name, histogram: &Histogram<u32>, trailing_coma| {
            print!(
                "\"{name}\": {mean}, \"{name}_len\": {len}, \"{name}_meanlen\": {meanlen} {coma}",
                mean = histogram.mean(),
                len = histogram.len(),
                meanlen = histogram.mean() * histogram.len().as_f64(),
                coma = if trailing_coma { "," } else { "" }
            );
        };
        loop {
            vec.clear();
            metric_dumping_counter += 1;
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
                        let item =
                            std::mem::replace(item, ThreadCommand::Shutdown(oneshot::channel().0));
                        let callback =
                            assert_matches!(item, ThreadCommand::Shutdown(callback) => callback);
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
                        let item =
                            std::mem::replace(item, ThreadCommand::Shutdown(oneshot::channel().0));
                        let (func, sent_at, name) = assert_matches!(item, ThreadCommand::Func{func, sent_at, name, ..} => (func, sent_at, name));
                        let sent_at = sent_at.elapsed();
                        let started_at = Instant::now();
                        func(&mut conn);
                        let started_at = started_at.elapsed();
                        processed += 1;
                        // update hdr metrics
                        send_hist
                            .record(u64::from(sent_at.subsec_micros()))
                            .unwrap();
                        func_histograms
                            .entry(name)
                            .or_insert_with(|| {
                                Histogram::<u32>::new_with_bounds(1, 1_000_000, 3).unwrap()
                            })
                            .record(u64::from(started_at.subsec_micros()))
                            .unwrap();
                    }
                }
                processed
            };
            let processed = execute(CommandPriority::High) + execute(CommandPriority::Medium);
            if processed < config.low_prio_threshold {
                execute(CommandPriority::Low);
            }

            if metric_dumping_counter == METRIC_DUMPING_TRESHOLD {
                print!("{{");
                metric_dumping_counter = 0;
                func_histograms.iter_mut().for_each(|(name, h)| {
                    print_histogram(*name, h, true);
                    h.clear();
                });
                print_histogram("send", &send_hist, false);
                send_hist.clear();
                println!("}}");
            }
        } // Loop until shutdown is set to true.
    }

    #[instrument(level = Level::DEBUG, skip_all, name = "sqlite_new")]
    pub async fn new<P: AsRef<Path>>(path: P, config: SqliteConfig) -> Result<Self, DbError> {
        let path = path.as_ref().to_owned();
        let shutdown = Arc::new(AtomicBool::new(false));
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(config.queue_capacity);
        {
            let init_task = {
                let path = path.clone();
                #[cfg_attr(madsim, allow(deprecated))]
                tokio::task::spawn_blocking(move || Self::init_thread(&path)).await
            };
            let conn = match init_task {
                Ok(conn) => conn,
                Err(err) => {
                    error!("Initialization error - {err:?}");
                    return Err(DbError::Specific(SpecificError::GenericError(
                        StrVariant::Static("initialization error"),
                    )));
                }
            };
            let shutdown = shutdown.clone();
            std::thread::spawn(move || Self::connection_rpc(conn, &shutdown, command_rx, config));
        };
        info!("Sqlite database location: {path:?}");
        Ok(Self {
            shutdown,
            command_tx,
            response_subscribers: Arc::default(),
            ffqn_to_pending_subscription: Arc::default(),
        })
    }

    #[instrument(level = Level::TRACE, skip_all, fields(name))]
    pub async fn transaction_write<F, T>(&self, func: F, name: &'static str) -> Result<T, DbError>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static,
    {
        self.transaction(func, true, name).await
    }

    #[instrument(level = Level::TRACE, skip_all, fields(name))]
    pub async fn transaction_read<F, T>(&self, func: F, name: &'static str) -> Result<T, DbError>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static,
    {
        self.transaction(func, false, name).await
    }

    /// Invokes the provided function wrapping a new [`rusqlite::Transaction`] that is committed automatically.
    async fn transaction<F, T>(
        &self,
        func: F,
        write: bool,
        name: &'static str,
    ) -> Result<T, DbError>
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
                    let res = debug_span!(parent: &parent_span, "tx_begin", name).in_scope(|| {
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
                        debug_span!(parent: &parent_span, "tx_commit", name)
                            .in_scope(|| transaction.commit().map(|()| ok).map_err(convert_err))
                    });
                    _ = tx.send(res);
                }),
                sent_at: Instant::now(),
                name,
            })
            .await
            .map_err(|_send_err| DbError::Connection(DbConnectionError::SendError))?;
        rx.await
            .map_err(|_recv_err| DbError::Connection(DbConnectionError::RecvError))?
    }

    #[instrument(level = Level::TRACE, skip_all, fields(name))]
    pub async fn conn_low_prio<F, T>(&self, func: F, name: &'static str) -> Result<T, DbError>
    where
        F: FnOnce(&Connection) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static + Default,
    {
        let (tx, rx) = oneshot::channel();
        let span = tracing::trace_span!("tx_function");
        self.command_tx
            .send(ThreadCommand::Func {
                priority: CommandPriority::Low,
                func: Box::new(move |conn| {
                    _ = tx.send(span.in_scope(|| func(conn)));
                }),
                sent_at: Instant::now(),
                name,
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
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbError> {
        let mut stmt = conn
            .prepare(
                "SELECT created_at, json_value FROM t_execution_log WHERE \
            execution_id = :execution_id AND version = 0",
            )
            .map_err(convert_err)?;
        let (created_at, event) = stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| {
                    let created_at = row.get("created_at")?;
                    let event = row
                        .get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                        .map(|event| (created_at, event.0))
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
            component_id,
            metadata,
            scheduled_by,
        } = event
        {
            Ok(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn,
                params,
                parent,
                scheduled_at,
                retry_exp_backoff,
                max_retries,
                component_id,
                metadata,
                scheduled_by,
            })
        } else {
            error!("Cannt match `Created` event - {event:?}");
            Err(DbError::Specific(SpecificError::ConsistencyError(
                StrVariant::Static("Cannot deserialize `Created` event"),
            )))
        }
    }

    fn count_temporary_events(
        conn: &Connection,
        execution_id: &ExecutionId,
    ) -> Result<u32, DbError> {
        let mut stmt = conn.prepare(
            "SELECT COUNT(*) as count FROM t_execution_log WHERE execution_id = :execution_id AND (variant = :v1 OR variant = :v2)",
        ).map_err(convert_err)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":v1": DUMMY_TEMPORARILY_TIMED_OUT.variant(),
                ":v2": DUMMY_TEMPORARILY_FAILED.variant(),
            },
            |row| row.get("count"),
        )
        .map_err(convert_err)
    }

    fn get_current_version(
        tx: &Transaction,
        execution_id: &ExecutionId,
    ) -> Result<Version, DbError> {
        let mut stmt = tx.prepare(
            "SELECT version FROM t_execution_log WHERE execution_id = :execution_id ORDER BY version DESC LIMIT 1",
        ).map_err(convert_err)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                let version: VersionType = row.get("version")?;
                Ok(Version::new(version))
            },
        )
        .map_err(convert_err)
    }

    fn get_next_version(tx: &Transaction, execution_id: &ExecutionId) -> Result<Version, DbError> {
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
    ) -> Result<(AppendResponse, PendingAt), DbError> {
        debug!("create_inner");

        let version = Version::new(0);
        let execution_id = req.execution_id.clone();
        let execution_id_str = execution_id.to_string();
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
        tx.prepare(
                "INSERT INTO t_execution_log (execution_id, created_at, version, json_value, variant, join_set_id ) \
                VALUES (:execution_id, :created_at, :version, :json_value, :variant, :join_set_id)")
        .map_err(convert_err)?
        .execute(named_params! {
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
        let pending_at = {
            let scheduled_at = assert_matches!(pending_state, PendingState::PendingAt { scheduled_at } => scheduled_at);
            debug!("Creating with `Pending(`{scheduled_at:?}`)");
            tx.prepare(
                "INSERT INTO t_state (created_at, scheduled_at, state, execution_id, next_version, pending_expires_finished, ffqn) \
                VALUES (:created_at, :scheduled_at, :state, :execution_id, :next_version, :pending_expires_finished, :ffqn)",
            )
            .map_err(convert_err)?
            .execute(named_params! {
                ":created_at": created_at,
                ":scheduled_at": scheduled_at,
                ":state": STATE_PENDING_AT,
                ":execution_id": execution_id.to_string(),
                ":next_version": next_version.0,
                ":pending_expires_finished": scheduled_at,
                ":ffqn": ffqn.to_string(),
            })
            .map_err(convert_err)?;
            PendingAt { scheduled_at, ffqn }
        };
        Ok((next_version, pending_at))
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%execution_id, %pending_state, %next_version, purge, ffqn = ?ffqn_when_creating))]
    fn update_state(
        tx: &Transaction,
        execution_id: &ExecutionId,
        pending_state: PendingState,
        expected_current_version: &Version,
        next_version: &Version,
        ffqn_when_creating: Option<FunctionFqn>, // or fetched lazily
    ) -> Result<IndexUpdated, DbError> {
        debug!("update_index");

        match pending_state {
            PendingState::PendingAt { scheduled_at } => {
                debug!("Setting state `Pending(`{scheduled_at:?}`)");
                let updated = tx
                    .prepare(
                        "UPDATE t_state SET \
                        state=:state, next_version = :next_version, pending_expires_finished = :pending_expires_finished, \
                        join_set_id = NULL,join_set_closing=NULL, \
                        executor_id = NULL,run_id = NULL,temporary_event_count = NULL, \
                        max_retries = NULL,retry_exp_backoff_millis = NULL,parent_execution_id = NULL,parent_join_set_id = NULL, \
                        result_kind = NULL \
                        WHERE execution_id = :execution_id AND next_version = :expected_current_version",
                    )
                    .map_err(convert_err)?
                    .execute(named_params! {
                        ":next_version": next_version.0,
                        ":pending_expires_finished": scheduled_at,
                        ":state": STATE_PENDING_AT,

                        ":execution_id": execution_id.to_string(),
                        ":expected_current_version": expected_current_version.0,

                    })
                    .map_err(convert_err)?;
                if updated != 1 {
                    error!(backtrace = %std::backtrace::Backtrace::capture(), "Failed updating state to {pending_state:?}");
                    return Err(DbError::Specific(SpecificError::ConsistencyError(
                        "updating state failed".into(),
                    )));
                }
                let ffqn = if let Some(ffqn) = ffqn_when_creating {
                    ffqn
                } else {
                    Self::fetch_created_event(tx, execution_id)?.ffqn
                };
                Ok(IndexUpdated {
                    temporary_event_count: None,
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
                let temporary_event_count = Self::count_temporary_events(tx, execution_id)?;
                let create_req = Self::fetch_created_event(tx, execution_id)?;

                let updated = tx
                    .prepare(
                        "UPDATE t_state SET \
                        state=:state, next_version = :next_version, pending_expires_finished = :pending_expires_finished, \
                        join_set_id = NULL,join_set_closing=NULL, \
                        executor_id = :executor_id, run_id = :run_id, temporary_event_count = :temporary_event_count, \
                        max_retries = :max_retries, retry_exp_backoff_millis = :retry_exp_backoff_millis, \
                        parent_execution_id = :parent_execution_id, parent_join_set_id = :parent_join_set_id, \
                        result_kind = NULL \
                        WHERE execution_id = :execution_id AND next_version = :expected_current_version",
                    )
                    .map_err(convert_err)?
                    .execute(named_params! {
                        ":next_version": next_version.0,
                        ":pending_expires_finished": lock_expires_at,
                        ":state": STATE_LOCKED,

                        ":executor_id": executor_id.to_string(),
                        ":run_id": run_id.to_string(),
                        ":temporary_event_count": temporary_event_count,
                        ":max_retries": create_req.max_retries,
                        ":retry_exp_backoff_millis": u64::try_from(create_req.retry_exp_backoff.as_millis()).unwrap(),
                        ":parent_execution_id": create_req.parent.as_ref().map(|(pid, _) | pid.to_string()),
                        ":parent_join_set_id": create_req.parent.as_ref().map(|(_, join_set_id)| join_set_id.to_string()),

                        ":execution_id": execution_id.to_string(),
                        ":expected_current_version": expected_current_version.0,

                    })
                    .map_err(convert_err)?;
                if updated != 1 {
                    error!(backtrace = %std::backtrace::Backtrace::capture(), "Failed updating state to {pending_state:?}");
                    return Err(DbError::Specific(SpecificError::ConsistencyError(
                        "updating state failed".into(),
                    )));
                }

                Ok(IndexUpdated {
                    temporary_event_count: Some(temporary_event_count),
                    pending_at: None,
                })
            }
            PendingState::BlockedByJoinSet {
                join_set_id,
                lock_expires_at,
                closing: join_set_closing,
            } => {
                debug!(%join_set_id, "Setting state `BlockedByJoinSet({next_version},{lock_expires_at})`");
                let updated = tx
                    .prepare(
                        "UPDATE t_state SET \
                        state=:state, next_version = :next_version, pending_expires_finished = :pending_expires_finished, \
                        join_set_id = :join_set_id, join_set_closing=:join_set_closing, \
                        executor_id = NULL,run_id = NULL,temporary_event_count = NULL, \
                        max_retries = NULL,retry_exp_backoff_millis = NULL,parent_execution_id = NULL,parent_join_set_id = NULL, \
                        result_kind = NULL \
                        WHERE execution_id = :execution_id AND next_version = :expected_current_version",
                    )
                    .map_err(convert_err)?
                    .execute(named_params! {
                        ":next_version": next_version.0,
                        ":pending_expires_finished": lock_expires_at,
                        ":state": STATE_BLOCKED_BY_JOIN_SET,

                        ":join_set_id": join_set_id.to_string(),
                        ":join_set_closing": join_set_closing,

                        ":execution_id": execution_id.to_string(),
                        ":expected_current_version": expected_current_version.0,

                    })
                    .map_err(convert_err)?;
                if updated != 1 {
                    error!(backtrace = %std::backtrace::Backtrace::capture(), "Failed updating state to {pending_state:?}");
                    return Err(DbError::Specific(SpecificError::ConsistencyError(
                        "updating state failed".into(),
                    )));
                }

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
                assert_eq!(finished_version, expected_current_version.0);
                let updated = tx
                    .prepare(
                        "UPDATE t_state SET \
                        state=:state, next_version = :next_version, pending_expires_finished = :pending_expires_finished, \
                        join_set_id = NULL,join_set_closing=NULL, \
                        executor_id = NULL,run_id = NULL,temporary_event_count = NULL, \
                        max_retries = NULL,retry_exp_backoff_millis = NULL,parent_execution_id = NULL,parent_join_set_id = NULL, \
                        result_kind = :result_kind \
                        WHERE execution_id = :execution_id AND next_version = :expected_current_version",
                    )
                    .map_err(convert_err)?
                    .execute(named_params! {
                        ":next_version": finished_version,
                        ":pending_expires_finished": finished_at,
                        ":state": STATE_FINISHED,

                        ":result_kind": result_kind.to_string(),

                        ":execution_id": execution_id.to_string(),
                        ":expected_current_version": expected_current_version.0,

                    })
                    .map_err(convert_err)?;
                if updated != 1 {
                    error!(backtrace = %std::backtrace::Backtrace::capture(), "Failed updating state to {pending_state:?}");
                    return Err(DbError::Specific(SpecificError::ConsistencyError(
                        "updating state failed".into(),
                    )));
                }

                Ok(IndexUpdated::default())
            }
        }
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %next_version, purge))]
    fn bump_state_next_version(
        tx: &Transaction,
        execution_id: &ExecutionId,
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
        execution_id: &ExecutionId,
    ) -> Result<CombinedState, DbError> {
        let mut stmt = tx.prepare(
            "SELECT state, ffqn, next_version, pending_expires_finished, executor_id, run_id, join_set_id, join_set_closing, result_kind FROM t_state WHERE \
            execution_id = :execution_id",
        ).map_err(convert_err)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                Ok(CombinedState::new(
                    &CombinedStateDTO {
                        state: row.get("state")?,
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
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbError> {
        struct StatementModifier {
            where_vec: Vec<String>,
            params: Vec<(&'static str, Box<dyn rusqlite::ToSql>)>,
            limit: u32,
            limit_desc: bool,
        }
        fn paginate<T: rusqlite::ToSql + 'static>(
            pagination: Pagination<Option<T>>,
            column: &str,
        ) -> StatementModifier {
            let mut where_vec: Vec<String> = vec![];
            let mut params: Vec<(&'static str, Box<dyn rusqlite::ToSql>)> = vec![];
            let limit = pagination.length();
            let limit_desc = pagination.is_desc();
            let rel = pagination.rel();
            match pagination {
                Pagination::NewerThan {
                    cursor: Some(cursor),
                    ..
                }
                | Pagination::OlderThan {
                    cursor: Some(cursor),
                    ..
                } => {
                    where_vec.push(format!("{column} {rel} :cursor"));
                    params.push((":cursor", Box::new(cursor)));
                }
                _ => {}
            }
            StatementModifier {
                where_vec,
                params,
                limit,
                limit_desc,
            }
        }
        let mut statement_mod = match pagination {
            ExecutionListPagination::CreatedBy(pagination) => paginate(pagination, "created_at"),
            ExecutionListPagination::ExecutionId(pagination) => {
                paginate(pagination, "execution_id")
            }
        };

        if let Some(ffqn) = ffqn {
            statement_mod.where_vec.push("ffqn = :ffqn".to_string());
            statement_mod
                .params
                .push((":ffqn", Box::new(ffqn.to_string())));
        }

        let where_str = if statement_mod.where_vec.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", statement_mod.where_vec.join(" AND "))
        };
        let sql = format!("SELECT created_at, scheduled_at, state, execution_id, ffqn, next_version, \
            pending_expires_finished, executor_id, run_id, join_set_id, join_set_closing, result_kind \
            FROM t_state {where_str} ORDER BY created_at {desc} LIMIT {limit}",
            desc = if statement_mod.limit_desc { "DESC" } else { "" },
            limit = statement_mod.limit
        );
        let mut vec: Vec<_> = read_tx
            .prepare(&sql)
            .map_err(convert_err)?
            .query_map::<_, &[(&'static str, &dyn rusqlite::ToSql)], _>(
                statement_mod
                    .params
                    .iter()
                    .map(|(key, value)| (*key, value.as_ref()))
                    .collect::<Vec<_>>()
                    .as_ref(),
                |row| {
                    let execution_id_res = row
                        .get::<_, String>("execution_id")?
                        .parse::<ExecutionId>()
                        .map_err(parsing_err);
                    let created_at = row.get("created_at")?;
                    let scheduled_at = row.get("scheduled_at")?;
                    let combined_state_res = CombinedState::new(
                        &CombinedStateDTO {
                            state: row.get("state")?,
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
                        execution_id_res.map(|execution_id| ExecutionWithState {
                            execution_id,
                            ffqn: combined_state.ffqn,
                            pending_state: combined_state.pending_state,
                            created_at,
                            scheduled_at,
                        })
                    }))
                },
            )
            .map_err(convert_err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(convert_err)?
            .into_iter()
            .collect::<Result<_, _>>()?;

        if !statement_mod.limit_desc {
            // the list must be sorted in descending order
            vec.reverse();
        }
        Ok(vec)
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %run_id, %executor_id))]
    #[expect(clippy::too_many_arguments)]
    fn lock_inner(
        tx: &Transaction,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        execution_id: &ExecutionId,
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
            component_id,
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
        let temporary_event_count = Self::update_state(
            tx,
            execution_id,
            pending_state,
            &Version(next_version.0 - 1),
            &next_version,
            Some(ffqn),
        )?
        .temporary_event_count
        .expect("temporary_event_count must be set");
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
                    let event = row
                        .get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                        .map(|wrapper| wrapper.0)
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
            ..
        }) = events.pop_front()
        else {
            error!("Execution log must contain at least `Created` event");
            return Err(DbError::Specific(SpecificError::ConsistencyError(
                StrVariant::Static("execution log must contain `Created` event"),
            )));
        };
        let responses = Self::list_responses(tx, execution_id, None)?
            .into_iter()
            .map(|resp| resp.event)
            .collect();
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
            execution_id: execution_id.clone(),
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
            temporary_event_count,
        })
    }

    fn count_join_next(
        tx: &Transaction,
        execution_id: &ExecutionId,
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
        execution_id: &ExecutionId,
        req: &AppendRequest,
        appending_version: Version,
    ) -> Result<(AppendResponse, Option<PendingAt>), DbError> {
        assert!(!matches!(req.event, ExecutionEventInner::Created { .. }));
        if let ExecutionEventInner::Locked {
            component_id,
            executor_id,
            run_id,
            lock_expires_at,
        } = &req.event
        {
            let locked = Self::lock_inner(
                tx,
                req.created_at,
                component_id.clone(),
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
            ExecutionEventInner::TemporarilyFailed {
                backoff_expires_at, ..
            }
            | ExecutionEventInner::TemporarilyTimedOut { backoff_expires_at }
            | ExecutionEventInner::Unlocked {
                backoff_expires_at, ..
            } => IndexAction::PendingStateChanged(PendingState::PendingAt {
                scheduled_at: *backoff_expires_at,
            }),
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
                        run_expires_at,
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
                    let scheduled_at = max(*run_expires_at, nth_created_at);
                    IndexAction::PendingStateChanged(PendingState::PendingAt { scheduled_at })
                } else {
                    IndexAction::PendingStateChanged(PendingState::BlockedByJoinSet {
                        join_set_id: *join_set_id,
                        lock_expires_at: *run_expires_at,
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
                    &expected_current_version,
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
        execution_id: &ExecutionId,
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
                    &next_version, // not changing the version
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
            response_subscribers.lock().unwrap().remove(execution_id),
            pendnig_at,
        ))
    }

    #[cfg(feature = "test")]
    fn get(
        tx: &Transaction,
        execution_id: &ExecutionId,
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
                    let event = row
                        .get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                        .map(|event| ExecutionEvent {
                            created_at,
                            event: event.0,
                        })
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
        let responses = Self::list_responses(tx, execution_id, None)?
            .into_iter()
            .map(|resp| resp.event)
            .collect();
        Ok(concepts::storage::ExecutionLog {
            execution_id: execution_id.clone(),
            events,
            responses,
            next_version: combined_state.next_version, // In case of finished, this will be the already last version
            pending_state: combined_state.pending_state,
        })
    }

    fn list_execution_events(
        tx: &Transaction,
        execution_id: &ExecutionId,
        version_min: VersionType,
        version_max_excluding: VersionType,
    ) -> Result<Vec<ExecutionEvent>, DbError> {
        tx
        .prepare(
            "SELECT created_at, json_value FROM t_execution_log WHERE \
            execution_id = :execution_id AND version >= :version_min AND version < :version_max_excluding
            ORDER BY version",
        )
        .map_err(convert_err)?
        .query_map(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":version_min": version_min,
                ":version_max_excluding": version_max_excluding
            },
            |row| {
                let created_at = row.get("created_at")?;
                let event =
                    row.get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                .map(|event| ExecutionEvent { created_at, event: event.0 })
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
        .collect::<Result<Vec<_>, _>>()
    }

    fn get_execution_event(
        tx: &Transaction,
        execution_id: &ExecutionId,
        version: VersionType,
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
                let event = row
                    .get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                    .map(|event| ExecutionEvent {
                        created_at,
                        event: event.0,
                    })
                    .map_err(|serde| {
                        error!("Cannot deserialize {row:?} - {serde:?}");
                        parsing_err(serde)
                    });
                Ok(event)
            },
        )
        .map_err(convert_err)?
    }

    fn list_responses(
        tx: &Transaction,
        execution_id: &ExecutionId,
        pagination: Option<Pagination<u32>>,
    ) -> Result<Vec<ResponseWithCursor>, DbError> {
        let mut params: Vec<(&'static str, Box<dyn rusqlite::ToSql>)> = vec![];
        let mut sql =
            "SELECT id, created_at, join_set_id, json_value FROM t_join_set_response WHERE \
            execution_id = :execution_id"
                .to_string();
        let limit = match &pagination {
            Some(
                pagination @ (Pagination::NewerThan { cursor, .. }
                | Pagination::OlderThan { cursor, .. }),
            ) => {
                params.push((":cursor", Box::new(cursor)));
                sql.push_str(&format!(" AND id {rel} :cursor", rel = pagination.rel(),));
                Some(pagination.length())
            }
            None => None,
        };
        sql.push_str(" ORDER BY id");
        if pagination.as_ref().is_some_and(Pagination::is_desc) {
            sql.push_str(" DESC");
        }
        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        params.push((":execution_id", Box::new(execution_id.to_string())));
        tx.prepare(&sql)
            .map_err(convert_err)?
            .query_map::<_, &[(&'static str, &dyn ToSql)], _>(
                params
                    .iter()
                    .map(|(key, value)| (*key, value.as_ref()))
                    .collect::<Vec<_>>()
                    .as_ref(),
                |row| {
                    let cursor = row.get("id")?;
                    let created_at: DateTime<Utc> = row.get("created_at")?;
                    let join_set_id = row.get::<_, JoinSetIdW>("join_set_id")?.0;
                    let event = row.get::<_, JsonWrapper<JoinSetResponse>>("json_value")?.0;
                    Ok(ResponseWithCursor {
                        cursor,
                        event: JoinSetResponseEventOuter {
                            event: JoinSetResponseEvent { join_set_id, event },
                            created_at,
                        },
                    })
                },
            )
            .map_err(convert_err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(convert_err)
    }

    fn nth_response(
        tx: &Transaction,
        execution_id: &ExecutionId,
        join_set_id: JoinSetId,
        skip_rows: u64,
    ) -> Result<Option<JoinSetResponseEventOuter>, DbError> {
        tx.prepare(
            "SELECT created_at, join_set_id, json_value FROM t_join_set_response WHERE \
            execution_id = :execution_id AND join_set_id = :join_set_id ORDER BY id
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

    // TODO(perf): Instead of OFFSET an per-execution sequential ID could improve the read performance.
    #[instrument(level = Level::TRACE, skip_all)]
    fn get_responses_with_offset(
        tx: &Transaction,
        execution_id: &ExecutionId,
        skip_rows: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError> {
        tx.prepare(
            "SELECT created_at, join_set_id, json_value FROM t_join_set_response WHERE \
            execution_id = :execution_id ORDER BY id
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
            // Select executions in PendingAt.
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT execution_id, next_version FROM t_state WHERE \
                            state = \"{STATE_PENDING_AT}\" AND \
                            pending_expires_finished <= :pending_expires_finished AND ffqn = :ffqn \
                            ORDER BY pending_expires_finished LIMIT :batch_size"
                ))
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
                        let version = Version::new(row.get::<_, VersionType>("next_version")?);
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
    fn notify_pending(&self, pending_at: PendingAt, current_time: DateTime<Utc>) {
        Self::notify_pending_locked(
            pending_at,
            current_time,
            &self.ffqn_to_pending_subscription.lock().unwrap(),
        );
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn notify_pending_all(
        &self,
        pending_ats: impl Iterator<Item = PendingAt>,
        current_time: DateTime<Utc>,
    ) {
        let ffqn_to_pending_subscription = self.ffqn_to_pending_subscription.lock().unwrap();
        for pending_at in pending_ats {
            Self::notify_pending_locked(pending_at, current_time, &ffqn_to_pending_subscription);
        }
    }

    fn notify_pending_locked(
        pending_at: PendingAt,
        current_time: DateTime<Utc>,
        ffqn_to_pending_subscription: &std::sync::MutexGuard<
            hashbrown::HashMap<FunctionFqn, mpsc::Sender<()>>,
        >,
    ) {
        match pending_at {
            PendingAt { scheduled_at, ffqn } if scheduled_at <= current_time => {
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
            .transaction_write(move |tx| Self::create_inner(tx, req), "create")
            .await?;
        self.notify_pending(pending_at, created_at);
        Ok(version)
    }

    #[instrument(level = Level::TRACE, skip(self))]
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError> {
        trace!("lock_pending");
        let execution_ids_versions = self
            .conn_low_prio(
                move |conn| Self::get_pending(conn, batch_size, pending_at_or_sooner, &ffqns),
                "get_pending",
            )
            .await?;
        if execution_ids_versions.is_empty() {
            Ok(vec![])
        } else {
            debug!("Locking {execution_ids_versions:?}");
            self.transaction_write(
                move |tx| {
                    let mut locked_execs = Vec::with_capacity(execution_ids_versions.len());
                    // Append lock
                    for (execution_id, version) in execution_ids_versions {
                        match Self::lock_inner(
                            tx,
                            created_at,
                            component_id.clone(),
                            &execution_id,
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
                },
                "lock_pending",
            )
            .await
        }
    }

    /// Specialized `append` which returns the event history.
    #[instrument(level = Level::DEBUG, skip(self))]
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        debug!("lock");
        let execution_id = execution_id.clone();
        self.transaction_write(
            move |tx| {
                let locked = Self::lock_inner(
                    tx,
                    created_at,
                    component_id,
                    &execution_id,
                    run_id,
                    version,
                    executor_id,
                    lock_expires_at,
                )?;
                Ok((locked.event_history, locked.version))
            },
            "lock_inner",
        )
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
            panic!("Cannot append `Created` event - use `create` instead");
        }
        let (version, pending_at) = self
            .transaction_write(
                move |tx| Self::append(tx, &execution_id, &req, version),
                "append",
            )
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
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError> {
        debug!("append_batch");
        trace!(?batch, "append_batch");
        assert!(!batch.is_empty(), "Empty batch request");
        if batch.iter().any(|append_request| {
            matches!(append_request.event, ExecutionEventInner::Created { .. })
        }) {
            panic!("Cannot append `Created` event - use `create` instead");
        }
        let (version, pending_at) = self
            .transaction_write(
                move |tx| {
                    let mut version = version;
                    let mut pending_at = None;
                    for append_request in batch {
                        (version, pending_at) =
                            Self::append(tx, &execution_id, &append_request, version)?;
                    }
                    Ok((version, pending_at))
                },
                "append_batch",
            )
            .await
            .map_err(DbError::from)?;
        if let Some(pending_at) = pending_at {
            self.notify_pending(pending_at, current_time);
        }
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch, child_req))]
    async fn append_batch_create_new_execution(
        &self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbError> {
        debug!("append_batch_create_new_execution");
        trace!(?batch, ?child_req, "append_batch_create_new_execution");
        assert!(!batch.is_empty(), "Empty batch request");
        if batch.iter().any(|append_request| {
            matches!(append_request.event, ExecutionEventInner::Created { .. })
        }) {
            panic!("Cannot append `Created` event - use `create` instead");
        }

        #[instrument(level = Level::TRACE, skip_all)]
        fn append_batch_create_new_execution_inner(
            tx: &mut rusqlite::Transaction,
            batch: Vec<AppendRequest>,
            execution_id: &ExecutionId,
            mut version: Version,
            child_req: Vec<CreateRequest>,
        ) -> Result<(Version, Vec<PendingAt>), DbError> {
            let mut pending_at = None;
            for append_request in batch {
                (version, pending_at) =
                    SqlitePool::append(tx, execution_id, &append_request, version)?;
            }
            let mut pending_ats = Vec::new();
            if let Some(pending_at) = pending_at {
                pending_ats.push(pending_at);
            }
            for child_req in child_req {
                let (_, pending_at) = SqlitePool::create_inner(tx, child_req)?;
                pending_ats.push(pending_at);
            }
            Ok((version, pending_ats))
        }

        let (version, pending_ats) = self
            .transaction_write(
                move |tx| {
                    append_batch_create_new_execution_inner(
                        tx,
                        batch,
                        &execution_id,
                        version,
                        child_req,
                    )
                },
                "append_batch_create_new_execution_inner",
            )
            .await
            .map_err(DbError::from)?;
        self.notify_pending_all(pending_ats.into_iter(), current_time);
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch, parent_response_event))]
    async fn append_batch_respond_to_parent(
        &self,
        execution_id: ExecutionId,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        version: Version,
        parent_execution_id: ExecutionId,
        parent_response_event: JoinSetResponseEventOuter,
    ) -> Result<AppendBatchResponse, DbError> {
        debug!("append_batch_respond_to_parent");
        if execution_id == parent_execution_id {
            // Pending state would be wrong.
            // This is not a panic because it depends on DB state.
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
        assert!(!batch.is_empty(), "Empty batch request");
        if batch.iter().any(|append_request| {
            matches!(append_request.event, ExecutionEventInner::Created { .. })
        }) {
            panic!("Cannot append `Created` event - use `create` instead");
        }
        let response_subscribers = self.response_subscribers.clone();
        let (version, response_subscriber, pending_ats) = {
            let event = parent_response_event.clone();
            self.transaction_write(
                move |tx| {
                    let mut version = version;
                    let mut pending_at_child = None;
                    for append_request in batch {
                        (version, pending_at_child) =
                            Self::append(tx, &execution_id, &append_request, version)?;
                    }

                    let (response_subscriber, pending_at_parent) = Self::append_response(
                        tx,
                        &parent_execution_id,
                        &event,
                        &response_subscribers,
                    )?;
                    Ok((
                        version,
                        response_subscriber,
                        vec![pending_at_child, pending_at_parent],
                    ))
                },
                "append_batch_respond_to_parent",
            )
            .await
            .map_err(DbError::from)?
        };
        if let Some(response_subscriber) = response_subscriber {
            let notified = response_subscriber.send(parent_response_event);
            debug!("Notifying response subscriber: {notified:?}");
        }
        self.notify_pending_all(pending_ats.into_iter().flatten(), current_time);
        Ok(version)
    }

    #[cfg(feature = "test")]
    #[instrument(level = Level::DEBUG, skip(self))]
    async fn get(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<concepts::storage::ExecutionLog, DbError> {
        trace!("get");
        let execution_id = execution_id.clone();
        self.transaction_read(move |tx| Self::get(tx, &execution_id), "get")
            .await
            .map_err(DbError::from)
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn list_execution_events(
        &self,
        execution_id: &ExecutionId,
        since: &Version,
        max_length: VersionType,
    ) -> Result<Vec<ExecutionEvent>, DbError> {
        let execution_id = execution_id.clone();
        let since = since.0;
        self.transaction_read(
            move |tx| Self::list_execution_events(tx, &execution_id, since, since + max_length),
            "get",
        )
        .await
        .map_err(DbError::from)
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError> {
        debug!("next_responses");
        let execution_id = execution_id.clone();
        let response_subscribers = self.response_subscribers.clone();
        let resp_or_receiver = self
            .transaction_write(
                move |tx| {
                    let responses = Self::get_responses_with_offset(tx, &execution_id, start_idx)?;
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
                },
                "subscribe_to_next_responses",
            )
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
            self.transaction_write(
                move |tx| Self::append_response(tx, &execution_id, &event, &response_subscribers),
                "append_response",
            )
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
                            let execution_id = row.get("execution_id")?;
                            let join_set_id = row.get::<_, JoinSetIdW>("join_set_id")?.0;
                            let delay_id = row.get::<_, DelayIdW>("delay_id")?.0;
                            Ok(ExpiredTimer::AsyncDelay { execution_id, join_set_id, delay_id })
                        },
                    ).map_err(convert_err)?
                    .collect::<Result<Vec<_>, _>>().map_err(convert_err)?
                    ;
                    // Extend with expired locks
                    expired_timers.extend(conn.prepare(&format!(
                        "SELECT execution_id, next_version, temporary_event_count, max_retries, retry_exp_backoff_millis, parent_execution_id, parent_join_set_id FROM t_state \
                        WHERE pending_expires_finished <= :at AND state = \"{STATE_LOCKED}\"")
                    ).map_err(convert_err)?
                    .query_map(
                            named_params! {
                                ":at": at,
                            },
                            |row| {
                                let execution_id = row.get("execution_id")?;
                                let version = Version::new(row.get::<_, VersionType>("next_version")?);

                                let temporary_event_count = row.get::<_, u32>("temporary_event_count")?;
                                let max_retries = row.get::<_, u32>("max_retries")?;
                                let retry_exp_backoff = Duration::from_millis(row.get::<_, u64>("retry_exp_backoff_millis")?);
                                let parent_execution_id = row.get::<_, Option<ExecutionId>>("parent_execution_id")?;
                                let parent_join_set_id = row.get::<_, Option<JoinSetIdW>>("parent_join_set_id")?.map(|it|it.0);

                                Ok(ExpiredTimer::Lock { execution_id, version, temporary_event_count, max_retries,
                                    retry_exp_backoff, parent: parent_execution_id.and_then(|pexe| parent_join_set_id.map(|pjs| (pexe, pjs)))})
                            },
                        ).map_err(convert_err)?
                        .collect::<Result<Vec<_>, _>>().map_err(convert_err)?
                    );

                if !expired_timers.is_empty() {
                    debug!("get_expired_timers found {expired_timers:?}");
                }
                Ok(expired_timers)
            }, "get_expired_timers"
        )
        .await
        .map_err(DbError::from)
    }

    #[instrument(level = Level::TRACE, skip(self))]
    async fn wait_for_pending(
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
        let execution_ids_versions = match self
            .conn_low_prio(
                {
                    let ffqns = ffqns.clone();
                    move |conn| Self::get_pending(conn, 1, pending_at_or_sooner, ffqns.as_ref())
                },
                "subscribe_to_pending",
            )
            .await
        {
            Ok(ok) => ok,
            Err(err) => {
                trace!("Ignoring error and waiting in for timeout - {err:?}");
                tokio::time::sleep_until(sleep_until).await;
                return;
            }
        };
        if !execution_ids_versions.is_empty() {
            trace!("Not waiting, database already contains new pending executions");
            return;
        }
        tokio::select! { // future's liveness: Dropping the loser immediately.
            _ = receiver.recv() => {
                trace!("Received a notification");
            }
            () = tokio::time::sleep_until(sleep_until) => {
                trace!("Did not receive a notification");
            }
        }
    }

    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout: Option<Duration>,
    ) -> Result<FinishedExecutionResult, ClientError> {
        let execution_result = {
            let fut = async move {
                loop {
                    let execution_id = execution_id.clone();
                    if let Some(execution_result) = self
                        .transaction_read(move |tx| {
                            let pending_state =
                                Self::get_combined_state(tx, &execution_id)?.pending_state;
                            if let PendingState::Finished { finished } = pending_state {
                                let event =
                                    Self::get_execution_event(tx, &execution_id, finished.version)?;
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
                        }, "wait_for_finished_result")
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
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbError> {
        let version = version.0;
        let execution_id = execution_id.clone();
        self.transaction_read(
            move |tx| Self::get_execution_event(tx, &execution_id, version),
            "get_execution_event",
        )
        .await
    }

    async fn get_pending_state(&self, execution_id: &ExecutionId) -> Result<PendingState, DbError> {
        let execution_id = execution_id.clone();
        Ok(self
            .transaction_read(
                move |tx| Self::get_combined_state(tx, &execution_id),
                "get_pending_state",
            )
            .await?
            .pending_state)
    }

    async fn list_executions(
        &self,
        ffqn: Option<FunctionFqn>,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbError> {
        Ok(self
            .transaction_read(
                move |tx| Self::list_executions(tx, ffqn, pagination),
                "list_executions",
            )
            .await?)
    }

    async fn list_responses(
        &self,
        execution_id: &ExecutionId,
        pagination: Pagination<u32>,
    ) -> Result<Vec<ResponseWithCursor>, DbError> {
        let execution_id = execution_id.clone();
        Ok(self
            .transaction_read(
                move |tx| Self::list_responses(tx, &execution_id, Some(pagination)),
                "list_executions",
            )
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
