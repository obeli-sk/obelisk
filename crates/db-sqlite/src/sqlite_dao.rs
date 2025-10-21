use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ExecutionId, FunctionFqn, JoinSetId, SupportedFunctionReturnValue,
    prefixed_ulid::{DelayId, ExecutionIdDerived, ExecutorId, RunId},
    storage::{
        AppendBatchResponse, AppendRequest, AppendResponse, BacktraceFilter, BacktraceInfo,
        CreateRequest, DUMMY_CREATED, DUMMY_HISTORY_EVENT, DbConnection, DbErrorGeneric,
        DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbErrorWritePermanent, DbExecutor,
        DbPool, DbPoolCloseable, ExecutionEvent, ExecutionEventInner, ExecutionListPagination,
        ExecutionWithState, ExpiredTimer, HistoryEvent, JoinSetRequest, JoinSetResponse,
        JoinSetResponseEvent, JoinSetResponseEventOuter, LockPendingResponse, LockedExecution,
        Pagination, PendingState, PendingStateFinished, PendingStateFinishedResultKind,
        ResponseWithCursor, Version, VersionType,
    },
};
use conversions::{FromStrWrapper, JsonWrapper, consistency_db_err, consistency_rusqlite};
use conversions::{
    intermittent_err_generic, prepare_err_generic, result_err_generic, result_err_read,
};
use hashbrown::HashMap;
use hdrhistogram::{Counter, Histogram};
use rusqlite::{
    CachedStatement, Connection, OpenFlags, OptionalExtension, Params, ToSql, Transaction,
    named_params,
};
use std::{
    cmp::max,
    collections::VecDeque,
    fmt::Debug,
    path::Path,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use std::{fmt::Write as _, pin::Pin};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::{Level, Span, debug, debug_span, error, info, instrument, trace, warn};

#[derive(Debug, thiserror::Error)]
#[error("initialization error")]
pub struct InitializationError;

#[derive(Debug, Clone)]
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

const PRAGMA: [[&str; 2]; 10] = [
    ["journal_mode", "wal"],
    ["synchronous", "FULL"],
    ["foreign_keys", "true"],
    ["busy_timeout", "1000"],
    ["cache_size", "10000"], // number of pages
    ["temp_store", "MEMORY"],
    ["page_size", "8192"], // 8 KB
    ["mmap_size", "134217728"],
    ["journal_size_limit", "67108864"],
    ["integrity_check", ""],
];

// Append only
const CREATE_TABLE_T_METADATA: &str = r"
CREATE TABLE IF NOT EXISTS t_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_version INTEGER NOT NULL,
    created_at TEXT NOT NULL
) STRICT
";
const T_METADATA_EXPECTED_SCHEMA_VERSION: u32 = 2;

/// Stores execution history. Append only.
const CREATE_TABLE_T_EXECUTION_LOG: &str = r"
CREATE TABLE IF NOT EXISTS t_execution_log (
    execution_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    json_value TEXT NOT NULL,
    version INTEGER NOT NULL,
    variant TEXT NOT NULL,
    join_set_id TEXT,
    history_event_type TEXT GENERATED ALWAYS AS (json_value->>'$.HistoryEvent.event.type') STORED,
    PRIMARY KEY (execution_id, version)
) STRICT
";
// Used in `fetch_created` and `get_execution_event`
const CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VERSION: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_version  ON t_execution_log (execution_id, version);
";
// Used in `lock_inner` to filter by execution ID and variant (created or event history)
const CREATE_INDEX_IDX_T_EXECUTION_ID_EXECUTION_ID_VARIANT: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_variant  ON t_execution_log (execution_id, variant);
";

/// Stores child execution return values for the parent (`execution_id`). Append only.
/// For `JoinSetResponse::DelayFinished`, column `delay_id` must not be null.
/// For `JoinSetResponse::ChildExecutionFinished`, column `child_execution_id`,`finished_version`
/// must not be null.
const CREATE_TABLE_T_JOIN_SET_RESPONSE: &str = r"
CREATE TABLE IF NOT EXISTS t_join_set_response (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,

    delay_id TEXT,

    child_execution_id TEXT,
    finished_version INTEGER,

    UNIQUE (execution_id, join_set_id, delay_id, child_execution_id)
) STRICT
";
// Used when querying for the next response
const CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID_ID: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_join_set_response_execution_id_id ON t_join_set_response (execution_id, id);
";

/// Stores executions in `PendingState`
/// `state` to column mapping:
/// `PendingAt`:            (nothing but required columns)
/// `BlockedByJoinSet`:     `join_set_id`, `join_set_closing`
/// `Locked`:               `executor_id`, `run_id`
/// `Finished` :            `result_kind`.
const CREATE_TABLE_T_STATE: &str = r"
CREATE TABLE IF NOT EXISTS t_state (
    execution_id TEXT NOT NULL,
    is_top_level INTEGER NOT NULL,
    corresponding_version INTEGER NOT NULL,
    pending_expires_finished TEXT NOT NULL,
    ffqn TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    intermittent_event_count INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    retry_exp_backoff_millis INTEGER NOT NULL,

    join_set_id TEXT,
    join_set_closing INTEGER,

    executor_id TEXT,
    run_id TEXT,

    result_kind TEXT,

    PRIMARY KEY (execution_id)
) STRICT
";
const STATE_PENDING_AT: &str = "PendingAt";
const STATE_BLOCKED_BY_JOIN_SET: &str = "BlockedByJoinSet";
const STATE_LOCKED: &str = "Locked";
const STATE_FINISHED: &str = "Finished";
const HISTORY_EVENT_TYPE_JOIN_NEXT: &str = "JoinNext";

// TODO: partial indexes
const IDX_T_STATE_LOCK_PENDING: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending ON t_state (state, pending_expires_finished, ffqn);
";
const IDX_T_STATE_EXPIRED_TIMERS: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_expired_timers ON t_state (pending_expires_finished) WHERE executor_id IS NOT NULL;
";
const IDX_T_STATE_EXECUTION_ID_IS_TOP_LEVEL: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_state_execution_id_is_root ON t_state (execution_id, is_top_level);
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
) STRICT
";

// Append only.
const CREATE_TABLE_T_BACKTRACE: &str = r"
CREATE TABLE IF NOT EXISTS t_backtrace (
    execution_id TEXT NOT NULL,
    component_id TEXT NOT NULL,
    version_min_including INTEGER NOT NULL,
    version_max_excluding INTEGER NOT NULL,
    wasm_backtrace TEXT NOT NULL,
    PRIMARY KEY (execution_id, version_min_including, version_max_excluding)
) STRICT
";
// Index for searching backtraces by execution_id and version
const IDX_T_BACKTRACE_EXECUTION_ID_VERSION: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_backtrace_execution_id_version ON t_backtrace (execution_id, version_min_including, version_max_excluding);
";
mod conversions {

    use concepts::storage::{DbErrorGeneric, DbErrorRead};
    use rusqlite::types::{FromSql, FromSqlError};
    use std::{fmt::Debug, str::FromStr};
    use tracing::error;

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn prepare_err_generic(err: rusqlite::Error) -> DbErrorGeneric {
        error!(backtrace = %std::backtrace::Backtrace::capture(), "Sqlite error {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn intermittent_err_generic(err: rusqlite::Error) -> DbErrorGeneric {
        error!(backtrace = %std::backtrace::Backtrace::capture(), "Sqlite error {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn result_err_generic(err: rusqlite::Error) -> DbErrorGeneric {
        error!(backtrace = %std::backtrace::Backtrace::capture(), "Sqlite error {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
    pub(crate) fn result_err_read(err: rusqlite::Error) -> DbErrorRead {
        if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
            DbErrorRead::NotFound
        } else {
            result_err_generic(err).into()
        }
    }

    pub(crate) struct JsonWrapper<T>(pub(crate) T);
    impl<T: serde::de::DeserializeOwned + 'static + Debug> FromSql for JsonWrapper<T> {
        fn column_result(
            value: rusqlite::types::ValueRef<'_>,
        ) -> rusqlite::types::FromSqlResult<Self> {
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

    pub(crate) struct FromStrWrapper<T: FromStr>(pub(crate) T);
    impl<T: FromStr<Err = D>, D: Debug> FromSql for FromStrWrapper<T> {
        fn column_result(
            value: rusqlite::types::ValueRef<'_>,
        ) -> rusqlite::types::FromSqlResult<Self> {
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

    // Used as a wrapper for `FromSqlError::Other`
    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    pub(crate) struct OtherError(&'static str);
    pub(crate) fn consistency_rusqlite(input: &'static str) -> rusqlite::Error {
        FromSqlError::other(OtherError(input)).into()
    }

    pub(crate) fn consistency_db_err(input: &'static str) -> DbErrorGeneric {
        DbErrorGeneric::Uncategorized(input.into())
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
    corresponding_version: Version,
}
impl CombinedState {
    fn get_next_version_assert_not_finished(&self) -> Version {
        assert!(!self.pending_state.is_finished());
        self.corresponding_version.increment()
    }

    #[cfg(feature = "test")]
    fn get_next_version_or_finished(&self) -> Version {
        if self.pending_state.is_finished() {
            self.corresponding_version.clone()
        } else {
            self.corresponding_version.increment()
        }
    }
}

#[derive(Debug)]
struct PendingAt {
    scheduled_at: DateTime<Utc>,
    ffqn: FunctionFqn,
}

impl CombinedState {
    fn new(
        dto: &CombinedStateDTO,
        corresponding_version: Version,
    ) -> Result<Self, rusqlite::Error> {
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
                join_set_id: join_set_id.clone(),
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
                    version: corresponding_version.0,
                    result_kind: *result_kind,
                },
            }),
            _ => {
                error!("Cannot deserialize pending state from  {dto:?}");
                Err(consistency_rusqlite("invalid `t_state`"))
            }
        }?;
        Ok(Self {
            ffqn: FunctionFqn::from_str(&dto.ffqn).map_err(|parse_err| {
                error!("Error parsing ffqn of {dto:?} - {parse_err:?}");
                consistency_rusqlite("invalid ffqn value in `t_state`")
            })?,
            pending_state,
            corresponding_version,
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
    Shutdown,
    Dummy,
}

#[derive(Clone)]
pub struct SqlitePool(SqlitePoolInner);

#[derive(Clone)]
struct SqlitePoolInner {
    shutdown_requested: Arc<AtomicBool>,
    shutdown_finished: Arc<AtomicBool>,
    command_tx: tokio::sync::mpsc::Sender<ThreadCommand>,
    response_subscribers:
        Arc<Mutex<HashMap<ExecutionId, (oneshot::Sender<JoinSetResponseEventOuter>, u64)>>>,
    ffqn_to_pending_subscription: Arc<Mutex<HashMap<FunctionFqn, (mpsc::Sender<()>, u64)>>>,
    join_handle: Option<Arc<std::thread::JoinHandle<()>>>, // always Some, Optional for swapping in drop.
}

#[async_trait]
impl DbPoolCloseable for SqlitePool {
    async fn close(self) {
        self.0.shutdown_requested.store(true, Ordering::Release);
        // Unblock the thread's blocking_recv. If the capacity is reached, the next processed message will trigger shutdown.
        let _ = self.0.command_tx.try_send(ThreadCommand::Shutdown);
        while !self.0.shutdown_finished.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

#[async_trait]
impl DbPool for SqlitePool {
    fn connection(&self) -> Box<dyn DbConnection> {
        Box::new(self.clone())
    }
}
impl Drop for SqlitePool {
    fn drop(&mut self) {
        let arc = self.0.join_handle.take().expect("join_handle was set");
        if let Ok(join_handle) = Arc::try_unwrap(arc) {
            // Last holder
            if !join_handle.is_finished() {
                if !self.0.shutdown_finished.load(Ordering::Acquire) {
                    // Best effort to shut down the sqlite thread.
                    let backtrace = std::backtrace::Backtrace::capture();
                    warn!("SqlitePool was not closed properly - {backtrace}");
                    self.0.shutdown_requested.store(true, Ordering::Release);
                    // Unblock the thread's blocking_recv. If the capacity is reached, the next processed message will trigger shutdown.
                    let _ = self.0.command_tx.try_send(ThreadCommand::Shutdown);
                    // Not joining the thread, drop might be called from async context.
                    // We are shutting down the server anyway.
                } else {
                    // The thread set `shutdown_finished` as its last operation.
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqliteConfig {
    pub queue_capacity: usize,
    pub low_prio_threshold: usize,
    pub pragma_override: Option<HashMap<String, String>>,
    pub metrics_threshold: Option<Duration>,
}
impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100,
            low_prio_threshold: 100,
            pragma_override: None,
            metrics_threshold: None,
        }
    }
}

impl SqlitePool {
    fn init_thread(
        path: &Path,
        mut pragma_override: HashMap<String, String>,
    ) -> Result<Connection, InitializationError> {
        fn execute<P: Params>(
            conn: &Connection,
            sql: &str,
            params: P,
        ) -> Result<(), InitializationError> {
            conn.execute(sql, params).map(|_| ()).map_err(|err| {
                error!("Cannot run `{sql}` - {err:?}");
                InitializationError
            })
        }
        fn pragma_update(
            conn: &Connection,
            name: &str,
            value: &str,
        ) -> Result<(), InitializationError> {
            if value.is_empty() {
                debug!("Querying PRAGMA {name}");
                conn.pragma_query(None, name, |row| {
                    debug!("{row:?}");
                    Ok(())
                })
                .map_err(|err| {
                    error!("cannot update pragma `{name}`=`{value}` - {err:?}");
                    InitializationError
                })
            } else {
                debug!("Setting PRAGMA {name}={value}");
                conn.pragma_update(None, name, value).map_err(|err| {
                    error!("cannot update pragma `{name}`=`{value}` - {err:?}");
                    InitializationError
                })
            }
        }

        let conn = Connection::open_with_flags(path, OpenFlags::default()).map_err(|err| {
            error!("cannot open the connection - {err:?}");
            InitializationError
        })?;

        for [pragma_name, default_value] in PRAGMA {
            let pragma_value = pragma_override
                .remove(pragma_name)
                .unwrap_or_else(|| default_value.to_string());
            pragma_update(&conn, pragma_name, &pragma_value)?;
        }
        // drain the rest overrides
        for (pragma_name, pragma_value) in pragma_override.drain() {
            pragma_update(&conn, &pragma_name, &pragma_value)?;
        }

        // t_metadata
        execute(&conn, CREATE_TABLE_T_METADATA, [])?;
        // Insert row if not exists.
        execute(
            &conn,
            &format!(
                "INSERT INTO t_metadata (schema_version, created_at) VALUES
                    ({T_METADATA_EXPECTED_SCHEMA_VERSION}, ?) ON CONFLICT DO NOTHING"
            ),
            [Utc::now()],
        )?;
        // Fail on unexpected `schema_version`.
        let actual_version = conn
            .prepare("SELECT schema_version FROM t_metadata ORDER BY id DESC LIMIT 1")
            .map_err(|err| {
                error!("cannot select schema version - {err:?}");
                InitializationError
            })?
            .query_row([], |row| row.get::<_, u32>("schema_version"));

        let actual_version = actual_version.map_err(|err| {
            error!("Cannot read the schema version - {err:?}");
            InitializationError
        })?;
        if actual_version != T_METADATA_EXPECTED_SCHEMA_VERSION {
            error!(
                "wrong schema version, expected {T_METADATA_EXPECTED_SCHEMA_VERSION}, got {actual_version}"
            );
            return Err(InitializationError);
        }

        // t_execution_log
        execute(&conn, CREATE_TABLE_T_EXECUTION_LOG, [])?;
        execute(
            &conn,
            CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VERSION,
            [],
        )?;
        execute(
            &conn,
            CREATE_INDEX_IDX_T_EXECUTION_ID_EXECUTION_ID_VARIANT,
            [],
        )?;
        // t_join_set_response
        execute(&conn, CREATE_TABLE_T_JOIN_SET_RESPONSE, [])?;
        execute(
            &conn,
            CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID_ID,
            [],
        )?;
        // t_state
        execute(&conn, CREATE_TABLE_T_STATE, [])?;
        execute(&conn, IDX_T_STATE_LOCK_PENDING, [])?;
        execute(&conn, IDX_T_STATE_EXPIRED_TIMERS, [])?;
        execute(&conn, IDX_T_STATE_EXECUTION_ID_IS_TOP_LEVEL, [])?;
        execute(&conn, IDX_T_STATE_FFQN, [])?;
        execute(&conn, IDX_T_STATE_CREATED_AT, [])?;
        // t_delay
        execute(&conn, CREATE_TABLE_T_DELAY, [])?;
        // t_backtrace
        execute(&conn, CREATE_TABLE_T_BACKTRACE, [])?;
        execute(&conn, IDX_T_BACKTRACE_EXECUTION_ID_VERSION, [])?;
        Ok(conn)
    }

    fn connection_rpc(
        mut conn: Connection,
        shutdown_requested: &AtomicBool,
        shutdown_finished: &AtomicBool,
        mut command_rx: mpsc::Receiver<ThreadCommand>,
        queue_capacity: usize,
        low_prio_threshold: usize,
        metrics_threshold: Option<Duration>,
    ) {
        let mut vec: Vec<ThreadCommand> = Vec::with_capacity(queue_capacity);
        // measure how long it takes to receive the `ThreadCommand`. 1us-1s
        let mut send_hist = Histogram::<u32>::new_with_bounds(1, 1_000_000, 3).unwrap();
        let mut func_histograms = HashMap::new();
        let print_histogram = |name, histogram: &Histogram<u32>, trailing_coma| {
            print!(
                "\"{name}\": {mean}, \"{name}_len\": {len}, \"{name}_meanlen\": {meanlen} {coma}",
                mean = histogram.mean(),
                len = histogram.len(),
                meanlen = histogram.mean() * histogram.len().as_f64(),
                coma = if trailing_coma { "," } else { "" }
            );
        };
        let mut metrics_instant = std::time::Instant::now();
        loop {
            vec.clear();
            if let Some(item) = command_rx.blocking_recv() {
                vec.push(item);
            } else {
                debug!("command_rx was closed");
                break;
            }
            while let Ok(more) = command_rx.try_recv() {
                vec.push(more);
            }
            // Did we receive Shutdown in the batch?
            let shutdown_found = vec
                .iter()
                .any(|item| matches!(item, ThreadCommand::Shutdown));
            if shutdown_found || shutdown_requested.load(Ordering::Acquire) {
                debug!("Recveived shutdown before processing the batch");
                break;
            }

            let mut execute = |expected: CommandPriority| {
                let mut processed = 0;
                for item in &mut vec {
                    if matches!(item, ThreadCommand::Func { priority, .. } if *priority == expected )
                    {
                        let item = std::mem::replace(item, ThreadCommand::Dummy); // get owned item
                        let (func, sent_at, name) = assert_matches!(item, ThreadCommand::Func{func, sent_at, name, ..} => (func, sent_at, name));
                        let sent_latency = sent_at.elapsed();
                        let started_at = Instant::now();
                        func(&mut conn);
                        let func_duration = started_at.elapsed();
                        processed += 1;
                        // update hdr metrics
                        if let Ok(value) = u32::try_from(sent_latency.as_micros()) {
                            send_hist
                                .record(u64::from(value))
                                .expect("already cast to u32");
                        }
                        if let Ok(value) = u32::try_from(func_duration.as_micros()) {
                            func_histograms
                                .entry(name)
                                .or_insert_with(|| {
                                    Histogram::<u32>::new_with_bounds(1, 1_000_000, 3).unwrap()
                                })
                                .record(u64::from(value))
                                .expect("already cast to u32");
                        }
                    }
                    if shutdown_requested.load(Ordering::Acquire) {
                        // recheck after every function
                        debug!("Recveived shutdown during processing of the batch");
                        break;
                    }
                }
                processed
            };
            let processed = execute(CommandPriority::High) + execute(CommandPriority::Medium);
            if processed < low_prio_threshold {
                execute(CommandPriority::Low);
            }

            if let Some(metrics_threshold) = metrics_threshold
                && metrics_instant.elapsed() > metrics_threshold
            {
                print!("{{");
                func_histograms.iter_mut().for_each(|(name, h)| {
                    print_histogram(*name, h, true);
                    h.clear();
                });
                print_histogram("send_latency", &send_hist, false);
                send_hist.clear();
                println!("}}");
                metrics_instant = std::time::Instant::now();
            }
        } // Loop until shutdown is set to true.
        debug!("Closing command thread");
        shutdown_finished.store(true, Ordering::Release);
    }

    #[instrument(level = Level::DEBUG, skip_all, name = "sqlite_new")]
    pub async fn new<P: AsRef<Path>>(
        path: P,
        config: SqliteConfig,
    ) -> Result<Self, InitializationError> {
        let path = path.as_ref().to_owned();

        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let shutdown_finished = Arc::new(AtomicBool::new(false));

        let (command_tx, command_rx) = tokio::sync::mpsc::channel(config.queue_capacity);
        info!("Sqlite database location: {path:?}");
        let join_handle = {
            // Initialize the `Connection`.
            let init_task = {
                tokio::task::spawn_blocking(move || {
                    Self::init_thread(&path, config.pragma_override.unwrap_or_default())
                })
                .await
            };
            let conn = match init_task {
                Ok(res) => res?,
                Err(join_err) => {
                    error!("Initialization panic - {join_err:?}");
                    return Err(InitializationError);
                }
            };
            let shutdown_requested = shutdown_requested.clone();
            let shutdown_finished = shutdown_finished.clone();
            // Start the RPC thread.
            std::thread::spawn(move || {
                Self::connection_rpc(
                    conn,
                    &shutdown_requested,
                    &shutdown_finished,
                    command_rx,
                    config.queue_capacity,
                    config.low_prio_threshold,
                    config.metrics_threshold,
                );
            })
        };
        Ok(SqlitePool(SqlitePoolInner {
            shutdown_requested,
            shutdown_finished,
            command_tx,
            response_subscribers: Arc::default(),
            ffqn_to_pending_subscription: Arc::default(),
            join_handle: Some(Arc::new(join_handle)),
        }))
    }

    #[instrument(level = Level::TRACE, skip_all, fields(name))]
    pub async fn transaction_write<F, T, E>(
        &self,
        func: F,
        name: &'static str,
    ) -> Result<Result<T, E>, DbErrorGeneric /* only close variant */>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<DbErrorGeneric> + Send + 'static,
    {
        self.transaction(func, true, name).await
    }

    #[instrument(level = Level::TRACE, skip_all, fields(name))]
    pub async fn transaction_read<F, T, E>(
        &self,
        func: F,
        name: &'static str,
    ) -> Result<Result<T, E>, DbErrorGeneric /* only close variant */>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<DbErrorGeneric> + Send + 'static,
    {
        self.transaction(func, false, name).await
    }

    /// Invokes the provided function wrapping a new [`rusqlite::Transaction`] that is committed automatically.
    async fn transaction<F, T, E>(
        &self,
        func: F,
        write: bool,
        name: &'static str,
    ) -> Result<Result<T, E>, DbErrorGeneric /* only close variant */>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<DbErrorGeneric> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let parent_span = Span::current();

        self.0
            .command_tx
            .send(ThreadCommand::Func {
                priority: if write {
                    CommandPriority::High
                } else {
                    CommandPriority::Medium
                },
                func: Box::new(move |conn| {
                    let tx_begin =
                        debug_span!(parent: &parent_span, "tx_begin", name).in_scope(|| {
                            conn.transaction_with_behavior(if write {
                                rusqlite::TransactionBehavior::Immediate
                            } else {
                                rusqlite::TransactionBehavior::Deferred
                            })
                            .map_err(result_err_generic)
                            .map_err(E::from)
                        });
                    let tx_apply = tx_begin.and_then(|mut transaction| {
                        parent_span.in_scope(|| func(&mut transaction).map(|ok| (ok, transaction)))
                    });
                    let tx_commit = tx_apply.and_then(|(ok, transaction)| {
                        debug_span!(parent: &parent_span, "tx_commit", name).in_scope(|| {
                            transaction
                                .commit()
                                .map(|()| ok)
                                .map_err(result_err_generic)
                                .map_err(E::from)
                        })
                    });
                    tx.send(tx_commit)
                        .unwrap_or_else(|_| unreachable!("outer fn waits forever for the result"));
                }),
                sent_at: Instant::now(),
                name,
            })
            .await
            .map_err(|_send_err| DbErrorGeneric::Close)?;
        rx.await.map_err(|_recv_err| DbErrorGeneric::Close)
    }

    #[instrument(level = Level::TRACE, skip_all, fields(name))]
    pub async fn conn_low_prio<F, T>(
        &self,
        func: F,
        name: &'static str,
    ) -> Result<T, DbErrorGeneric>
    where
        F: FnOnce(&Connection) -> Result<T, DbErrorGeneric> + Send + 'static,
        T: Send + 'static + Default,
    {
        let (tx, rx) = oneshot::channel();
        let span = tracing::trace_span!("tx_function");
        self.0
            .command_tx
            .send(ThreadCommand::Func {
                priority: CommandPriority::Low,
                func: Box::new(move |conn| {
                    tx.send(span.in_scope(|| func(conn)))
                        .unwrap_or_else(|_| unreachable!("outer fn waits forever for the result"));
                }),
                sent_at: Instant::now(),
                name,
            })
            .await
            .map_err(|_send_err| DbErrorGeneric::Close)?;
        match rx.await {
            Ok(res) => res,
            Err(_recv_err) => Ok(T::default()), // Dropped computation because of other priorities..
        }
    }

    fn fetch_created_event(
        conn: &Connection,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbErrorRead> {
        let mut stmt = conn
            .prepare(
                "SELECT created_at, json_value FROM t_execution_log WHERE \
            execution_id = :execution_id AND version = 0",
            )
            .map_err(prepare_err_generic)?;
        let (created_at, event) = stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| {
                    let created_at = row.get("created_at")?;
                    let event = row
                        .get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                        .map_err(|serde| {
                            error!("cannot deserialize `Created` event: {row:?} - `{serde:?}`");
                            consistency_rusqlite("cannot deserialize `Created` event")
                        })?;
                    Ok((created_at, event.0))
                },
            )
            .map_err(result_err_generic)?;
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
            error!("Row with version=0 must be a `Created` event - {event:?}");
            Err(consistency_db_err("expected `Created` event").into())
        }
    }

    fn check_expected_next_and_appending_version(
        expected_version: &Version,
        appending_version: &Version,
    ) -> Result<(), DbErrorWrite> {
        if *expected_version != *appending_version {
            debug!(
                "Version conflict - expected: {expected_version:?}, appending: {appending_version:?}"
            );
            return Err(DbErrorWrite::Permanent(
                DbErrorWritePermanent::CannotWrite {
                    reason: "version conflict".into(),
                    expected_version: Some(expected_version.clone()),
                },
            ));
        }
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip_all, fields(execution_id = %req.execution_id))]
    fn create_inner(
        tx: &Transaction,
        req: CreateRequest,
    ) -> Result<(AppendResponse, PendingAt), DbErrorWrite> {
        debug!("create_inner");

        let version = Version::default();
        let execution_id = req.execution_id.clone();
        let execution_id_str = execution_id.to_string();
        let ffqn = req.ffqn.clone();
        let created_at = req.created_at;
        let scheduled_at = req.scheduled_at;
        let max_retries = req.max_retries;
        let backoff_millis =
            u64::try_from(req.retry_exp_backoff.as_millis()).expect("backoff too big");
        let event = ExecutionEventInner::from(req);
        let event_ser = serde_json::to_string(&event).map_err(|err| {
            error!("Cannot serialize {event:?} - {err:?}");
            DbErrorWritePermanent::ValidationFailed("parameter serialization error".into())
        })?;
        tx.prepare(
                "INSERT INTO t_execution_log (execution_id, created_at, version, json_value, variant, join_set_id ) \
                VALUES (:execution_id, :created_at, :version, :json_value, :variant, :join_set_id)")
        .map_err(prepare_err_generic)?
        .execute(named_params! {
            ":execution_id": &execution_id_str,
            ":created_at": created_at,
            ":version": version.0,
            ":json_value": event_ser,
            ":variant": event.variant(),
            ":join_set_id": event.join_set_id().map(std::string::ToString::to_string),
        })
        .map_err(result_err_generic)?;
        let pending_state = PendingState::PendingAt { scheduled_at };
        let pending_at = {
            let scheduled_at = assert_matches!(pending_state, PendingState::PendingAt { scheduled_at } => scheduled_at);
            debug!("Creating with `Pending(`{scheduled_at:?}`)");
            tx.prepare(
                r"
                INSERT INTO t_state (
                    execution_id,
                    is_top_level,
                    corresponding_version,
                    pending_expires_finished,
                    ffqn,
                    state,
                    created_at,
                    updated_at,
                    scheduled_at,
                    intermittent_event_count,
                    max_retries,
                    retry_exp_backoff_millis
                    )
                VALUES (
                    :execution_id,
                    :is_top_level,
                    :corresponding_version,
                    :pending_expires_finished,
                    :ffqn,
                    :state,
                    :created_at,
                    CURRENT_TIMESTAMP,
                    :scheduled_at,
                    0,
                    :max_retries,
                    :retry_exp_backoff_millis
                    )
                ",
            )
            .map_err(prepare_err_generic)?
            .execute(named_params! {
                ":execution_id": execution_id.to_string(),
                ":is_top_level": execution_id.is_top_level(),
                ":corresponding_version": version.0,
                ":pending_expires_finished": scheduled_at,
                ":ffqn": ffqn.to_string(),
                ":state": STATE_PENDING_AT,
                ":created_at": created_at,
                ":scheduled_at": scheduled_at,
                ":max_retries": max_retries,
                ":retry_exp_backoff_millis": backoff_millis,
            })
            .map_err(result_err_generic)?;
            PendingAt { scheduled_at, ffqn }
        };
        let next_version = Version::new(version.0 + 1);
        Ok((next_version, pending_at))
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%execution_id, %scheduled_at, %corresponding_version))]
    fn update_state_pending_after_response_appended(
        tx: &Transaction,
        execution_id: &ExecutionId,
        scheduled_at: DateTime<Utc>,     // Changing to state PendingAt
        corresponding_version: &Version, // t_execution_log is not be changed
    ) -> Result<PendingAt, DbErrorWrite> {
        debug!("Setting t_state to Pending(`{scheduled_at:?}`) after response appended");
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx
            .prepare_cached(
                r"
                UPDATE t_state
                SET
                    corresponding_version = :corresponding_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,

                    join_set_id = NULL,
                    join_set_closing = NULL,

                    executor_id = NULL,
                    run_id = NULL,

                    result_kind = NULL
                WHERE execution_id = :execution_id
            ",
            )
            .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id_str,
                ":corresponding_version": corresponding_version.0,
                ":pending_expires_finished": scheduled_at,
                ":state": STATE_PENDING_AT,
            })
            .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }
        Ok(PendingAt {
            scheduled_at,
            ffqn: Self::fetch_created_event(tx, execution_id)?.ffqn,
        })
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%execution_id, %scheduled_at, %appending_version))]
    fn update_state_pending_after_event_appended(
        tx: &Transaction,
        execution_id: &ExecutionId,
        appending_version: &Version,
        scheduled_at: DateTime<Utc>, // Changing to state PendingAt
        intermittent_failure: bool,
    ) -> Result<(AppendResponse, PendingAt), DbErrorWrite> {
        debug!("Setting t_state to Pending(`{scheduled_at:?}`) after event appended");
        // If response idx is unknown, use 0. This distinguishs the two paths.
        // JoinNext will always send an actual idx.
        let mut stmt = tx
            .prepare_cached(
                r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,
                    intermittent_event_count = intermittent_event_count + :intermittent_delta,

                    join_set_id = NULL,
                    join_set_closing = NULL,

                    executor_id = NULL,
                    run_id = NULL,

                    result_kind = NULL
                WHERE execution_id = :execution_id;
            ",
            )
            .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id.to_string(),
                ":appending_version": appending_version.0,
                ":pending_expires_finished": scheduled_at,
                ":state": STATE_PENDING_AT,
                ":intermittent_delta": i32::from(intermittent_failure) // 0 or 1
            })
            .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }
        Ok((
            appending_version.increment(),
            PendingAt {
                scheduled_at,
                ffqn: Self::fetch_created_event(tx, execution_id)?.ffqn,
            },
        ))
    }

    fn update_state_locked_get_intermittent_event_count(
        tx: &Transaction,
        execution_id: &ExecutionId,
        executor_id: ExecutorId,
        run_id: RunId,
        lock_expires_at: DateTime<Utc>,
        appending_version: &Version,
    ) -> Result<u32, DbErrorWrite> {
        debug!("Setting t_state to Locked(`{lock_expires_at:?}`)");
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx
            .prepare_cached(
                r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,


                    join_set_id = NULL,
                    join_set_closing = NULL,

                    executor_id = :executor_id,
                    run_id = :run_id,

                    result_kind = NULL
                WHERE execution_id = :execution_id
            ",
            )
            .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id_str,
                ":appending_version": appending_version.0,
                ":pending_expires_finished": lock_expires_at,
                ":state": STATE_LOCKED,
                ":executor_id": executor_id.to_string(),
                ":run_id": run_id.to_string(),
            })
            .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }

        // fetch intermittent event count from the just-inserted row.
        let intermittent_event_count = tx
            .prepare(
                "SELECT intermittent_event_count FROM t_state WHERE execution_id = :execution_id",
            )
            .map_err(prepare_err_generic)?
            .query_row(
                named_params! {
                    ":execution_id": execution_id_str,
                },
                |row| {
                    let intermittent_event_count = row.get("intermittent_event_count")?;
                    Ok(intermittent_event_count)
                },
            )
            .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

        Ok(intermittent_event_count)
    }

    fn update_state_blocked(
        tx: &Transaction,
        execution_id: &ExecutionId,
        appending_version: &Version,
        // BlockedByJoinSet fields
        join_set_id: &JoinSetId,
        lock_expires_at: DateTime<Utc>,
        join_set_closing: bool,
    ) -> Result<
        AppendResponse, // next version
        DbErrorWrite,
    > {
        debug!("Setting t_state to BlockedByJoinSet(`{join_set_id}`)");
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx
            .prepare_cached(
                r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,

                    join_set_id = :join_set_id,
                    join_set_closing = :join_set_closing,

                    executor_id = NULL,
                    run_id = NULL,

                    result_kind = NULL
                WHERE execution_id = :execution_id
            ",
            )
            .map_err(prepare_err_generic)?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id_str,
                ":appending_version": appending_version.0,
                ":pending_expires_finished": lock_expires_at,
                ":state": STATE_BLOCKED_BY_JOIN_SET,
                ":join_set_id": join_set_id,
                ":join_set_closing": join_set_closing,
            })
            .map_err(result_err_generic)?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }
        Ok(appending_version.increment())
    }

    fn update_state_finished(
        tx: &Transaction,
        execution_id: &ExecutionId,
        appending_version: &Version,
        // Finished fields
        finished_at: DateTime<Utc>,
        result_kind: PendingStateFinishedResultKind,
    ) -> Result<(), DbErrorWrite> {
        debug!("Setting t_state to Finished");
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx
            .prepare_cached(
                r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,

                    join_set_id = NULL,
                    join_set_closing = NULL,

                    executor_id = NULL,
                    run_id = NULL,

                    result_kind = :result_kind
                WHERE execution_id = :execution_id
            ",
            )
            .map_err(prepare_err_generic)?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id_str,
                ":appending_version": appending_version.0,
                ":pending_expires_finished": finished_at,
                ":state": STATE_FINISHED,
                ":result_kind": result_kind.to_string(),
            })
            .map_err(result_err_generic)?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }
        Ok(())
    }

    // Upon appending new event to t_execution_log, copy the previous t_state with changed appending_version and created_at.
    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %appending_version))]
    fn bump_state_next_version(
        tx: &Transaction,
        execution_id: &ExecutionId,
        appending_version: &Version,
        delay_req: Option<DelayReq>,
    ) -> Result<AppendResponse /* next version */, DbErrorWrite> {
        debug!("update_index_version");
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx
            .prepare_cached(
                r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    updated_at = CURRENT_TIMESTAMP
                WHERE execution_id = :execution_id
            ",
            )
            .map_err(prepare_err_generic)?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id_str,
                ":appending_version": appending_version.0,
            })
            .map_err(result_err_generic)?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
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
                .map_err(prepare_err_generic)?;
            stmt.execute(named_params! {
                ":execution_id": execution_id_str,
                ":join_set_id": join_set_id.to_string(),
                ":delay_id": delay_id.to_string(),
                ":expires_at": expires_at,
            })
            .map_err(result_err_generic)?;
        }
        Ok(appending_version.increment())
    }

    fn get_combined_state(
        tx: &Transaction,
        execution_id: &ExecutionId,
    ) -> Result<CombinedState, DbErrorRead> {
        let mut stmt = tx
            .prepare(
                r"
                SELECT
                    state, ffqn, corresponding_version, pending_expires_finished, executor_id, run_id,
                    join_set_id, join_set_closing, result_kind FROM t_state
                WHERE
                    execution_id = :execution_id
                ",
            )
            .map_err(prepare_err_generic)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                CombinedState::new(
                    &CombinedStateDTO {
                        state: row.get("state")?,
                        ffqn: row.get("ffqn")?,
                        pending_expires_finished: row
                            .get::<_, DateTime<Utc>>("pending_expires_finished")?,
                        executor_id: row.get::<_, Option<ExecutorId>>("executor_id")?,
                        run_id: row.get::<_, Option<RunId>>("run_id")?,
                        join_set_id: row.get::<_, Option<JoinSetId>>("join_set_id")?,
                        join_set_closing: row.get::<_, Option<bool>>("join_set_closing")?,
                        result_kind: row
                            .get::<_, Option<FromStrWrapper<PendingStateFinishedResultKind>>>(
                                "result_kind",
                            )?
                            .map(|wrapper| wrapper.0),
                    },
                    Version::new(row.get("corresponding_version")?),
                )
            },
        )
        .map_err(result_err_read)
    }

    fn list_executions(
        read_tx: &Transaction,
        ffqn: Option<FunctionFqn>,
        top_level_only: bool,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbErrorGeneric> {
        struct StatementModifier {
            where_vec: Vec<String>,
            params: Vec<(&'static str, Box<dyn rusqlite::ToSql>)>,
            limit: u32,
            limit_desc: bool,
        }
        fn paginate<T: rusqlite::ToSql + 'static>(
            pagination: Pagination<Option<T>>,
            column: &str,
            top_level_only: bool,
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
            if top_level_only {
                where_vec.push("is_top_level=true".to_string());
            }
            StatementModifier {
                where_vec,
                params,
                limit,
                limit_desc,
            }
        }
        let mut statement_mod = match pagination {
            ExecutionListPagination::CreatedBy(pagination) => {
                paginate(pagination, "created_at", top_level_only)
            }
            ExecutionListPagination::ExecutionId(pagination) => {
                paginate(pagination, "execution_id", top_level_only)
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
        let sql = format!(
            "SELECT created_at, scheduled_at, state, execution_id, ffqn, corresponding_version, \
            pending_expires_finished, executor_id, run_id, join_set_id, join_set_closing, result_kind \
            FROM t_state {where_str} ORDER BY created_at {desc} LIMIT {limit}",
            desc = if statement_mod.limit_desc { "DESC" } else { "" },
            limit = statement_mod.limit
        );
        let mut vec: Vec<_> = read_tx
            .prepare(&sql)
            .map_err(prepare_err_generic)?
            .query_map::<_, &[(&'static str, &dyn rusqlite::ToSql)], _>(
                statement_mod
                    .params
                    .iter()
                    .map(|(key, value)| (*key, value.as_ref()))
                    .collect::<Vec<_>>()
                    .as_ref(),
                |row| {
                    let execution_id = row.get::<_, ExecutionId>("execution_id")?;
                    let created_at = row.get("created_at")?;
                    let scheduled_at = row.get("scheduled_at")?;
                    let combined_state = CombinedState::new(
                        &CombinedStateDTO {
                            state: row.get("state")?,
                            ffqn: row.get("ffqn")?,
                            pending_expires_finished: row
                                .get::<_, DateTime<Utc>>("pending_expires_finished")?,
                            executor_id: row.get::<_, Option<ExecutorId>>("executor_id")?,
                            run_id: row.get::<_, Option<RunId>>("run_id")?,
                            join_set_id: row.get::<_, Option<JoinSetId>>("join_set_id")?,
                            join_set_closing: row.get::<_, Option<bool>>("join_set_closing")?,
                            result_kind: row
                                .get::<_, Option<FromStrWrapper<PendingStateFinishedResultKind>>>(
                                    "result_kind",
                                )?
                                .map(|wrapper| wrapper.0),
                        },
                        Version::new(row.get("corresponding_version")?),
                    )?;
                    Ok(ExecutionWithState {
                        execution_id,
                        ffqn: combined_state.ffqn,
                        pending_state: combined_state.pending_state,
                        created_at,
                        scheduled_at,
                    })
                },
            )
            .map_err(intermittent_err_generic)?
            .collect::<Vec<Result<_, _>>>()
            .into_iter()
            .filter_map(|row| match row {
                Ok(row) => Some(row),
                Err(err) => {
                    warn!("Skipping row - {err:?}");
                    None
                }
            })
            .collect();

        if !statement_mod.limit_desc {
            // the list must be sorted in descending order
            vec.reverse();
        }
        Ok(vec)
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %run_id, %executor_id))]
    #[expect(clippy::too_many_arguments)]
    fn lock_single_execution(
        tx: &Transaction,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        appending_version: &Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockedExecution, DbErrorWrite> {
        debug!("lock_inner");
        let combined_state = Self::get_combined_state(tx, execution_id)?;
        combined_state.pending_state.can_append_lock(
            created_at,
            executor_id,
            run_id,
            lock_expires_at,
        )?;
        let expected_version = combined_state.get_next_version_assert_not_finished();
        Self::check_expected_next_and_appending_version(&expected_version, appending_version)?;

        // Append to `execution_log` table.
        let event = ExecutionEventInner::Locked {
            component_id,
            executor_id,
            lock_expires_at,
            run_id,
        };
        let event_ser = serde_json::to_string(&event).map_err(|err| {
            warn!("Cannot serialize {event:?} - {err:?}");
            DbErrorWritePermanent::ValidationFailed("parameter serialization error".into())
        })?;
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO t_execution_log \
            (execution_id, created_at, json_value, version, variant) \
            VALUES \
            (:execution_id, :created_at, :json_value, :version, :variant)",
            )
            .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": created_at,
            ":json_value": event_ser,
            ":version": appending_version.0,
            ":variant": event.variant(),
        })
        .map_err(|err| {
            warn!("Cannot lock execution - {err:?}");
            DbErrorWritePermanent::CannotWrite {
                reason: "cannot lock execution".into(),
                expected_version: None,
            }
        })?;

        // Update `t_state`
        let responses = Self::list_responses(tx, execution_id, None)?;
        let responses = responses.into_iter().map(|resp| resp.event).collect();
        trace!("Responses: {responses:?}");

        let intermittent_event_count = Self::update_state_locked_get_intermittent_event_count(
            tx,
            execution_id,
            executor_id,
            run_id,
            lock_expires_at,
            appending_version,
        )?;
        // Fetch event_history and `Created` event to construct the response.
        let mut events = tx
            .prepare(
                "SELECT json_value FROM t_execution_log WHERE \
                execution_id = :execution_id AND (variant = :v1 OR variant = :v2) \
                ORDER BY version",
            )
            .map_err(prepare_err_generic)?
            .query_map(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":v1": DUMMY_CREATED.variant(),
                    ":v2": DUMMY_HISTORY_EVENT.variant(),
                },
                |row| {
                    row.get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                        .map(|wrapper| wrapper.0)
                        .map_err(|serde| {
                            error!("Cannot deserialize {row:?} - {serde:?}");
                            consistency_rusqlite("cannot deserialize json value")
                        })
                },
            )
            .map_err(result_err_generic)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(result_err_generic)?
            .into_iter()
            .collect::<VecDeque<_>>();
        let Some(ExecutionEventInner::Created {
            ffqn,
            params,
            retry_exp_backoff,
            max_retries,
            parent,
            metadata,
            ..
        }) = events.pop_front()
        else {
            error!("Execution log must contain at least `Created` event");
            return Err(consistency_db_err("execution log must contain `Created` event").into());
        };

        let event_history = events
            .into_iter()
            .map(|event| {
                if let ExecutionEventInner::HistoryEvent { event } = event {
                    Ok(event)
                } else {
                    error!("Rows can only contain `Created` and `HistoryEvent` event kinds");
                    Err(consistency_db_err(
                        "rows can only contain `Created` and `HistoryEvent` event kinds",
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LockedExecution {
            execution_id: execution_id.clone(),
            metadata,
            run_id,
            next_version: appending_version.increment(),
            ffqn,
            params,
            event_history,
            responses,
            retry_exp_backoff,
            max_retries,
            parent,
            intermittent_event_count,
        })
    }

    fn count_join_next(
        tx: &Transaction,
        execution_id: &ExecutionId,
        join_set_id: &JoinSetId,
    ) -> Result<u64, DbErrorRead> {
        let mut stmt = tx.prepare(
            "SELECT COUNT(*) as count FROM t_execution_log WHERE execution_id = :execution_id AND join_set_id = :join_set_id \
            AND history_event_type = :join_next",
        ).map_err(prepare_err_generic)?;
        Ok(stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":join_set_id": join_set_id.to_string(),
                    ":join_next": HISTORY_EVENT_TYPE_JOIN_NEXT,
                },
                |row| row.get("count"),
            )
            .map_err(result_err_generic)?)
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id))]
    #[expect(clippy::needless_return)]
    fn append(
        tx: &Transaction,
        execution_id: &ExecutionId,
        req: &AppendRequest,
        appending_version: Version,
    ) -> Result<
        (
            AppendResponse,
            Option<PendingAt>, // Notify subscribers listening on ffqn
        ),
        DbErrorWrite,
    > {
        if matches!(req.event, ExecutionEventInner::Created { .. }) {
            return Err(DbErrorWrite::Permanent(
                DbErrorWritePermanent::ValidationFailed(
                    "cannot append `Created` event - use `create` instead".into(),
                ),
            ));
        }
        if matches!(req.event, ExecutionEventInner::Locked { .. }) {
            return Err(DbErrorWrite::Permanent(
                DbErrorWritePermanent::ValidationFailed(
                    "cannot append `Locked` event - use `lock_*` instead".into(),
                ),
            ));
        }

        let combined_state = Self::get_combined_state(tx, execution_id)?;
        if combined_state.pending_state.is_finished() {
            debug!("Execution is already finished");
            return Err(DbErrorWrite::Permanent(
                DbErrorWritePermanent::CannotWrite {
                    reason: "already finished".into(),
                    expected_version: None,
                },
            ));
        }

        Self::check_expected_next_and_appending_version(
            &combined_state.get_next_version_assert_not_finished(),
            &appending_version,
        )?;
        let event_ser = serde_json::to_string(&req.event).map_err(|err| {
            error!("Cannot serialize {:?} - {err:?}", req.event);
            DbErrorWritePermanent::ValidationFailed("parameter serialization error".into())
        })?;

        let mut stmt = tx.prepare(
                    "INSERT INTO t_execution_log (execution_id, created_at, json_value, version, variant, join_set_id) \
                    VALUES (:execution_id, :created_at, :json_value, :version, :variant, :join_set_id)")
                    .map_err(prepare_err_generic)?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": req.created_at,
            ":json_value": event_ser,
            ":version": appending_version.0,
            ":variant": req.event.variant(),
            ":join_set_id": req.event.join_set_id().map(std::string::ToString::to_string),
        })
        .map_err(result_err_generic)?;
        // Calculate current pending state

        match &req.event {
            ExecutionEventInner::Created { .. } => {
                unreachable!("handled in the caller")
            }
            ExecutionEventInner::Locked { .. } => {
                unreachable!("handled above")
            }
            ExecutionEventInner::TemporarilyFailed {
                backoff_expires_at, ..
            }
            | ExecutionEventInner::TemporarilyTimedOut {
                backoff_expires_at, ..
            } => {
                let (next_version, pending_at) = Self::update_state_pending_after_event_appended(
                    tx,
                    execution_id,
                    &appending_version,
                    *backoff_expires_at,
                    true, // an intermittent failure
                )?;
                return Ok((next_version, Some(pending_at)));
            }
            ExecutionEventInner::Unlocked {
                backoff_expires_at, ..
            } => {
                let (next_version, pending_at) = Self::update_state_pending_after_event_appended(
                    tx,
                    execution_id,
                    &appending_version,
                    *backoff_expires_at,
                    false, // not an intermittent failure
                )?;
                return Ok((next_version, Some(pending_at)));
            }
            ExecutionEventInner::Finished { result, .. } => {
                Self::update_state_finished(
                    tx,
                    execution_id,
                    &appending_version,
                    req.created_at,
                    PendingStateFinishedResultKind::from(result),
                )?;
                return Ok((
                    appending_version,
                    None, // No subscriber notified, not PendingAt event.
                ));
            }
            ExecutionEventInner::HistoryEvent {
                event:
                    HistoryEvent::JoinSetCreate { .. }
                    | HistoryEvent::JoinSetRequest {
                        request: JoinSetRequest::ChildExecutionRequest { .. },
                        ..
                    }
                    | HistoryEvent::Persist { .. }
                    | HistoryEvent::Schedule { .. }
                    | HistoryEvent::Stub { .. }
                    | HistoryEvent::JoinNextTooMany { .. },
            } => {
                return Ok((
                    Self::bump_state_next_version(tx, execution_id, &appending_version, None)?,
                    None, // No subscriber notified, not PendingAt event.
                ));
            }

            ExecutionEventInner::HistoryEvent {
                event:
                    HistoryEvent::JoinSetRequest {
                        join_set_id,
                        request:
                            JoinSetRequest::DelayRequest {
                                delay_id,
                                expires_at,
                                ..
                            },
                    },
            } => {
                return Ok((
                    Self::bump_state_next_version(
                        tx,
                        execution_id,
                        &appending_version,
                        Some(DelayReq {
                            join_set_id: join_set_id.clone(),
                            delay_id: delay_id.clone(),
                            expires_at: *expires_at,
                        }),
                    )?,
                    None, // No subscriber notified, not PendingAt event.
                ));
            }

            ExecutionEventInner::HistoryEvent {
                event:
                    HistoryEvent::JoinNext {
                        join_set_id,
                        run_expires_at,
                        closing,
                        requested_ffqn: _,
                    },
            } => {
                // Did the response arrive already?
                let join_next_count = Self::count_join_next(tx, execution_id, join_set_id)?;
                let nth_response =
                    Self::nth_response(tx, execution_id, join_set_id, join_next_count - 1)?; // Skip n-1 rows
                trace!("join_next_count: {join_next_count}, nth_response: {nth_response:?}");
                assert!(join_next_count > 0);
                if let Some(ResponseWithCursor {
                    event:
                        JoinSetResponseEventOuter {
                            created_at: nth_created_at,
                            ..
                        },
                    cursor: _,
                }) = nth_response
                {
                    let scheduled_at = max(*run_expires_at, nth_created_at); // No need to block
                    let (next_version, pending_at) =
                        Self::update_state_pending_after_event_appended(
                            tx,
                            execution_id,
                            &appending_version,
                            scheduled_at,
                            false, // not an intermittent failure
                        )?;
                    return Ok((next_version, Some(pending_at)));
                }
                return Ok((
                    Self::update_state_blocked(
                        tx,
                        execution_id,
                        &appending_version,
                        join_set_id,
                        *run_expires_at,
                        *closing,
                    )?,
                    None, // No subscriber notified, not PendingAt event.
                ));
            }
        }
    }

    fn append_response(
        tx: &Transaction,
        execution_id: &ExecutionId,
        req: JoinSetResponseEventOuter,
        response_subscribers: &Arc<
            Mutex<HashMap<ExecutionId, (oneshot::Sender<JoinSetResponseEventOuter>, u64)>>,
        >,
    ) -> Result<Option<PendingAt>, DbErrorWrite> {
        let mut stmt = tx.prepare(
            "INSERT INTO t_join_set_response (execution_id, created_at, join_set_id, delay_id, child_execution_id, finished_version) \
                    VALUES (:execution_id, :created_at, :join_set_id, :delay_id, :child_execution_id, :finished_version)",
        ).map_err(prepare_err_generic)?;
        let join_set_id = &req.event.join_set_id;
        let delay_id = match &req.event.event {
            JoinSetResponse::DelayFinished { delay_id } => Some(delay_id.to_string()),
            JoinSetResponse::ChildExecutionFinished { .. } => None,
        };
        let (child_execution_id, finished_version) = match &req.event.event {
            JoinSetResponse::ChildExecutionFinished {
                child_execution_id,
                finished_version,
                result: _,
            } => (
                Some(child_execution_id.to_string()),
                Some(finished_version.0),
            ),
            JoinSetResponse::DelayFinished { .. } => (None, None),
        };

        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": req.created_at,
            ":join_set_id": join_set_id.to_string(),
            ":delay_id": delay_id,
            ":child_execution_id": child_execution_id,
            ":finished_version": finished_version,
        })
        .map_err(result_err_generic)?;

        // if the execution is going to be unblocked by this response...
        let combined_state = Self::get_combined_state(tx, execution_id)?;
        debug!("previous_pending_state: {combined_state:?}");
        let pendnig_at = if let PendingState::BlockedByJoinSet {
            join_set_id: found_join_set_id,
            lock_expires_at, // Set to a future time if the worker is keeping the execution warm waiting for the result.
            closing: _,
        } = combined_state.pending_state
            && *join_set_id == found_join_set_id
        {
            // PendingAt should be set to current time if called from expired_timers_watcher,
            // or to a future time if the execution is hot.
            let scheduled_at = max(lock_expires_at, req.created_at);
            // TODO: Add diff test
            // Unblock the state.
            let pending_at = Self::update_state_pending_after_response_appended(
                tx,
                execution_id,
                scheduled_at,
                &combined_state.corresponding_version, // not changing the version
            )?;
            Some(pending_at)
        } else {
            None
        };
        if let JoinSetResponseEvent {
            join_set_id,
            event: JoinSetResponse::DelayFinished { delay_id },
        } = &req.event
        {
            debug!(%join_set_id, %delay_id, "Deleting from `t_delay`");
            let mut stmt =
                tx.prepare_cached("DELETE FROM t_delay WHERE execution_id = :execution_id AND join_set_id = :join_set_id AND delay_id = :delay_id")
                .map_err(prepare_err_generic)?;
            stmt.execute(named_params! {
                ":execution_id": execution_id.to_string(),
                ":join_set_id": join_set_id.to_string(),
                ":delay_id": delay_id.to_string(),
            })
            .map_err(result_err_generic)?;
        }
        if let Some((sender, _tag)) = response_subscribers.lock().unwrap().remove(execution_id) {
            debug!("Notifying response subscriber");
            let _ = sender.send(req);
        }

        Ok(pendnig_at)
    }

    fn append_backtrace(
        tx: &Transaction,
        backtrace_info: &BacktraceInfo,
    ) -> Result<(), DbErrorWrite> {
        let backtrace = serde_json::to_string(&backtrace_info.wasm_backtrace).map_err(|err| {
            warn!(
                "Cannot serialize backtrace {:?} - {err:?}",
                backtrace_info.wasm_backtrace
            );
            DbErrorWritePermanent::ValidationFailed("cannot serialize backtrace".into())
        })?;
        let mut stmt = tx
            .prepare(
                "INSERT INTO t_backtrace (execution_id, component_id, version_min_including, version_max_excluding, wasm_backtrace) \
                    VALUES (:execution_id, :component_id, :version_min_including, :version_max_excluding, :wasm_backtrace)",
            )
            .map_err(prepare_err_generic)?;
        stmt.execute(named_params! {
            ":execution_id": backtrace_info.execution_id.to_string(),
            ":component_id": backtrace_info.component_id.to_string(),
            ":version_min_including": backtrace_info.version_min_including.0,
            ":version_max_excluding": backtrace_info.version_max_excluding.0,
            ":wasm_backtrace": backtrace,
        })
        .map_err(result_err_generic)?;
        Ok(())
    }

    #[cfg(feature = "test")]
    fn get(
        tx: &Transaction,
        execution_id: &ExecutionId,
    ) -> Result<concepts::storage::ExecutionLog, DbErrorRead> {
        let mut stmt = tx
            .prepare(
                "SELECT created_at, json_value FROM t_execution_log WHERE \
                        execution_id = :execution_id ORDER BY version",
            )
            .map_err(prepare_err_generic)?;
        let events = stmt
            .query_map(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| {
                    let created_at = row.get("created_at")?;
                    let event = row
                        .get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                        .map_err(|serde| {
                            error!("Cannot deserialize {row:?} - {serde:?}");
                            consistency_rusqlite("cannot deserialize event")
                        })?
                        .0;

                    Ok(ExecutionEvent {
                        created_at,
                        event,
                        backtrace_id: None,
                    })
                },
            )
            .map_err(result_err_read)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(result_err_read)?;
        if events.is_empty() {
            return Err(DbErrorRead::NotFound);
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
            next_version: combined_state.get_next_version_or_finished(), // In case of finished, this will be the already last version
            pending_state: combined_state.pending_state,
        })
    }

    fn list_execution_events(
        tx: &Transaction,
        execution_id: &ExecutionId,
        version_min: VersionType,
        version_max_excluding: VersionType,
        include_backtrace_id: bool,
    ) -> Result<Vec<ExecutionEvent>, DbErrorRead> {
        let select = if include_backtrace_id {
            "SELECT
                log.created_at,
                log.json_value,
                -- Select version_min_including from backtrace if a match is found, otherwise NULL
                bt.version_min_including AS backtrace_id
            FROM
                t_execution_log AS log
            LEFT OUTER JOIN -- Use LEFT JOIN to keep all logs even if no backtrace matches
                t_backtrace AS bt ON log.execution_id = bt.execution_id
                                -- Check if the log's version falls within the backtrace's range
                                AND log.version >= bt.version_min_including
                                AND log.version < bt.version_max_excluding
            WHERE
                log.execution_id = :execution_id
                AND log.version >= :version_min
                AND log.version < :version_max_excluding
            ORDER BY
                log.version;"
        } else {
            "SELECT
                created_at, json_value, NULL as backtrace_id
            FROM t_execution_log WHERE
                execution_id = :execution_id AND version >= :version_min AND version < :version_max_excluding
            ORDER BY version"
        };
        tx.prepare(select)
            .map_err(prepare_err_generic)?
            .query_map(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":version_min": version_min,
                    ":version_max_excluding": version_max_excluding
                },
                |row| {
                    let created_at = row.get("created_at")?;
                    let backtrace_id = row
                        .get::<_, Option<VersionType>>("backtrace_id")?
                        .map(Version::new);
                    let event = row
                        .get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                        .map(|event| ExecutionEvent {
                            created_at,
                            event: event.0,
                            backtrace_id,
                        })
                        .map_err(|serde| {
                            error!("Cannot deserialize {row:?} - {serde:?}");
                            consistency_rusqlite("cannot deserialize")
                        })?;
                    Ok(event)
                },
            )
            .map_err(result_err_read)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(result_err_read)
    }

    fn get_execution_event(
        tx: &Transaction,
        execution_id: &ExecutionId,
        version: VersionType,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let mut stmt = tx
            .prepare(
                "SELECT created_at, json_value FROM t_execution_log WHERE \
                        execution_id = :execution_id AND version = :version",
            )
            .map_err(prepare_err_generic)?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":version": version,
            },
            |row| {
                let created_at = row.get("created_at")?;
                let event = row
                    .get::<_, JsonWrapper<ExecutionEventInner>>("json_value")
                    .map_err(|serde| {
                        error!("Cannot deserialize {row:?} - {serde:?}");
                        consistency_rusqlite("cannot deserialize event")
                    })?;

                Ok(ExecutionEvent {
                    created_at,
                    event: event.0,
                    backtrace_id: None,
                })
            },
        )
        .map_err(result_err_read)
    }

    fn list_responses(
        tx: &Transaction,
        execution_id: &ExecutionId,
        pagination: Option<Pagination<u32>>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorRead> {
        // TODO: Add test
        let mut params: Vec<(&'static str, Box<dyn rusqlite::ToSql>)> = vec![];
        let mut sql = "SELECT \
            r.id, r.created_at, r.join_set_id,  r.delay_id, r.child_execution_id, r.finished_version, l.json_value \
            FROM t_join_set_response r LEFT OUTER JOIN t_execution_log l ON r.child_execution_id = l.execution_id \
            WHERE \
            r.execution_id = :execution_id \
            AND ( r.finished_version = l.version OR r.child_execution_id IS NULL ) \
            "
        .to_string();
        let limit = match &pagination {
            Some(
                pagination @ (Pagination::NewerThan { cursor, .. }
                | Pagination::OlderThan { cursor, .. }),
            ) => {
                params.push((":cursor", Box::new(cursor)));
                write!(sql, " AND r.id {rel} :cursor", rel = pagination.rel()).unwrap();
                Some(pagination.length())
            }
            None => None,
        };
        sql.push_str(" ORDER BY id");
        if pagination.as_ref().is_some_and(Pagination::is_desc) {
            sql.push_str(" DESC");
        }
        if let Some(limit) = limit {
            write!(sql, " LIMIT {limit}").unwrap();
        }
        params.push((":execution_id", Box::new(execution_id.to_string())));
        tx.prepare(&sql)
            .map_err(prepare_err_generic)?
            .query_map::<_, &[(&'static str, &dyn ToSql)], _>(
                params
                    .iter()
                    .map(|(key, value)| (*key, value.as_ref()))
                    .collect::<Vec<_>>()
                    .as_ref(),
                Self::parse_response_with_cursor,
            )
            .map_err(intermittent_err_generic)?
            .collect::<Result<Vec<_>, rusqlite::Error>>()
            .map_err(result_err_read)
    }

    fn parse_response_with_cursor(
        row: &rusqlite::Row<'_>,
    ) -> Result<ResponseWithCursor, rusqlite::Error> {
        let id = row.get("id")?;
        let created_at: DateTime<Utc> = row.get("created_at")?;
        let join_set_id = row.get::<_, JoinSetId>("join_set_id")?;
        let event = match (
            row.get::<_, Option<DelayId>>("delay_id")?,
            row.get::<_, Option<ExecutionIdDerived>>("child_execution_id")?,
            row.get::<_, Option<VersionType>>("finished_version")?,
            row.get::<_, Option<JsonWrapper<ExecutionEventInner>>>("json_value")?,
        ) {
            (Some(delay_id), None, None, None) => JoinSetResponse::DelayFinished { delay_id },
            (
                None,
                Some(child_execution_id),
                Some(finished_version),
                Some(JsonWrapper(ExecutionEventInner::Finished { result, .. })),
            ) => JoinSetResponse::ChildExecutionFinished {
                child_execution_id,
                finished_version: Version(finished_version),
                result,
            },
            (delay, child, finished, result) => {
                error!(
                    "Invalid row in t_join_set_response {id} - {:?} {child:?} {finished:?} {:?}",
                    delay,
                    result.map(|it| it.0)
                );
                return Err(consistency_rusqlite("invalid row in t_join_set_response"));
            }
        };
        Ok(ResponseWithCursor {
            cursor: id,
            event: JoinSetResponseEventOuter {
                event: JoinSetResponseEvent { join_set_id, event },
                created_at,
            },
        })
    }

    fn nth_response(
        tx: &Transaction,
        execution_id: &ExecutionId,
        join_set_id: &JoinSetId,
        skip_rows: u64,
    ) -> Result<Option<ResponseWithCursor>, DbErrorRead> {
        // TODO: Add test
        tx
            .prepare(
                "SELECT r.id, r.created_at, r.join_set_id, \
                    r.delay_id, \
                    r.child_execution_id, r.finished_version, l.json_value \
                    FROM t_join_set_response r LEFT OUTER JOIN t_execution_log l ON r.child_execution_id = l.execution_id \
                    WHERE \
                    r.execution_id = :execution_id AND r.join_set_id = :join_set_id AND \
                    (
                    r.finished_version = l.version \
                    OR \
                    r.child_execution_id IS NULL \
                    ) \
                    ORDER BY id \
                    LIMIT 1 OFFSET :offset",
            )
            .map_err(prepare_err_generic)?
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":join_set_id": join_set_id.to_string(),
                    ":offset": skip_rows,
                },
                Self::parse_response_with_cursor,
            )
            .optional()
            .map_err(result_err_read)
    }

    // TODO(perf): Instead of OFFSET an per-execution sequential ID could improve the read performance.
    #[instrument(level = Level::TRACE, skip_all)]
    fn get_responses_with_offset(
        tx: &Transaction,
        execution_id: &ExecutionId,
        skip_rows: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbErrorRead> {
        // TODO: Add test
        tx.prepare(
            "SELECT r.id, r.created_at, r.join_set_id, \
            r.delay_id, \
            r.child_execution_id, r.finished_version, l.json_value \
            FROM t_join_set_response r LEFT OUTER JOIN t_execution_log l ON r.child_execution_id = l.execution_id \
            WHERE \
            r.execution_id = :execution_id AND \
            ( \
            r.finished_version = l.version \
            OR r.child_execution_id IS NULL \
            ) \
            ORDER BY id \
            LIMIT -1 OFFSET :offset",
        )
        .map_err(prepare_err_generic)?
        .query_map(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":offset": skip_rows,
            },
            Self::parse_response_with_cursor,
        )
        .map_err(result_err_read)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(result_err_read)
        .map(|resp| resp.into_iter().map(|vec| vec.event).collect())
    }

    fn get_pending_of_single_ffqn(
        mut stmt: CachedStatement,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqn: &FunctionFqn,
    ) -> Result<Vec<(ExecutionId, Version)>, ()> {
        stmt.query_map(
            named_params! {
                ":pending_expires_finished": pending_at_or_sooner,
                ":ffqn": ffqn.to_string(),
                ":batch_size": batch_size,
            },
            |row| {
                let execution_id = row.get::<_, String>("execution_id")?.parse::<ExecutionId>();
                let next_version =
                    Version::new(row.get::<_, VersionType>("corresponding_version")?).increment();
                Ok(execution_id.map(|exe| (exe, next_version)))
            },
        )
        .map_err(|err| {
            warn!("Ignoring consistency error {err:?}");
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            warn!("Ignoring consistency error {err:?}");
        })?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            warn!("Ignoring consistency error {err:?}");
        })
    }

    /// Get executions and their next versions
    fn get_pending(
        conn: &Connection,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: &[FunctionFqn],
    ) -> Result<Vec<(ExecutionId, Version)>, DbErrorGeneric> {
        let mut execution_ids_versions = Vec::with_capacity(batch_size);
        for ffqn in ffqns {
            // Select executions in PendingAt.
            let stmt = conn
                .prepare_cached(&format!(
                    r#"
                    SELECT execution_id, corresponding_version FROM t_state WHERE
                    state = "{STATE_PENDING_AT}" AND
                    pending_expires_finished <= :pending_expires_finished AND ffqn = :ffqn
                    ORDER BY pending_expires_finished LIMIT :batch_size
                    "#
                ))
                .map_err(|err| DbErrorGeneric::Uncategorized(err.to_string().into()))?;

            if let Ok(execs_and_versions) = Self::get_pending_of_single_ffqn(
                stmt,
                batch_size - execution_ids_versions.len(),
                pending_at_or_sooner,
                ffqn,
            ) {
                execution_ids_versions.extend(execs_and_versions);
                if execution_ids_versions.len() == batch_size {
                    // Prioritieze lowering of db requests, although ffqns later in the list might get starved.
                    break;
                }
                // consistency errors are ignored since we want to return at least some rows.
            }
        }
        Ok(execution_ids_versions)
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn notify_pending(&self, pending_at: PendingAt, current_time: DateTime<Utc>) {
        Self::notify_pending_locked(
            pending_at,
            current_time,
            &self.0.ffqn_to_pending_subscription.lock().unwrap(),
        );
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn notify_pending_all(
        &self,
        pending_ats: impl Iterator<Item = PendingAt>,
        current_time: DateTime<Utc>,
    ) {
        let ffqn_to_pending_subscription = self.0.ffqn_to_pending_subscription.lock().unwrap();
        for pending_at in pending_ats {
            Self::notify_pending_locked(pending_at, current_time, &ffqn_to_pending_subscription);
        }
    }

    fn notify_pending_locked(
        pending_at: PendingAt,
        current_time: DateTime<Utc>,
        ffqn_to_pending_subscription: &std::sync::MutexGuard<
            HashMap<FunctionFqn, (mpsc::Sender<()>, u64)>,
        >,
    ) {
        match pending_at {
            PendingAt { scheduled_at, ffqn } if scheduled_at <= current_time => {
                if let Some((subscription, _)) = ffqn_to_pending_subscription.get(&ffqn) {
                    debug!("Notifying pending subscriber");
                    let _ = subscription.try_send(());
                }
            }
            _ => {}
        }
    }

    #[instrument(level = Level::TRACE, skip_all)]
    fn append_batch_create_new_execution_inner(
        tx: &mut rusqlite::Transaction,
        batch: Vec<AppendRequest>,
        execution_id: &ExecutionId,
        mut version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<(Version, Vec<PendingAt>), DbErrorWrite> {
        let mut pending_at = None;
        for append_request in batch {
            (version, pending_at) = Self::append(tx, execution_id, &append_request, version)?;
        }
        let mut pending_ats = Vec::new();
        if let Some(pending_at) = pending_at {
            pending_ats.push(pending_at);
        }
        for child_req in child_req {
            let (_, pending_at) = Self::create_inner(tx, child_req)?;
            pending_ats.push(pending_at);
        }
        Ok((version, pending_ats))
    }
}

#[async_trait]
impl DbExecutor for SqlitePool {
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
        run_id: RunId,
    ) -> Result<LockPendingResponse, DbErrorGeneric> {
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
                        match Self::lock_single_execution(
                            tx,
                            created_at,
                            component_id.clone(),
                            &execution_id,
                            run_id,
                            &version,
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
            .await?
        }
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn lock_one(
        &self,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockedExecution, DbErrorWrite> {
        debug!(%execution_id, "lock_extend");
        let execution_id = execution_id.clone();
        self.transaction_write(
            move |tx| {
                Self::lock_single_execution(
                    tx,
                    created_at,
                    component_id,
                    &execution_id,
                    run_id,
                    &version,
                    executor_id,
                    lock_expires_at,
                )
            },
            "lock_inner",
        )
        .await?
    }

    #[instrument(level = Level::DEBUG, skip(self, req))]
    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbErrorWrite> {
        debug!(%req, "append");
        trace!(?req, "append");
        // Disallow `Created` event
        let created_at = req.created_at;
        if let ExecutionEventInner::Created { .. } = req.event {
            panic!("cannot append `Created` event - use `create` instead");
        }
        let (version, pending_at) = self
            .transaction_write(
                move |tx| Self::append(tx, &execution_id, &req, version),
                "append",
            )
            .await??;
        if let Some(pending_at) = pending_at {
            self.notify_pending(pending_at, created_at);
        }
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch, parent_response_event))]
    async fn append_batch_respond_to_parent(
        &self,
        execution_id: ExecutionIdDerived,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        version: Version,
        parent_execution_id: ExecutionId,
        parent_response_event: JoinSetResponseEventOuter,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        debug!("append_batch_respond_to_parent");
        let execution_id = ExecutionId::Derived(execution_id);
        if execution_id == parent_execution_id {
            // Pending state would be wrong.
            // This is not a panic because it depends on DB state.
            return Err(DbErrorWrite::Permanent(
                DbErrorWritePermanent::ValidationFailed(
                    "Parameters `execution_id` and `parent_execution_id` cannot be the same".into(),
                ),
            ));
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
            panic!("cannot append `Created` event - use `create` instead");
        }
        let response_subscribers = self.0.response_subscribers.clone();
        let (version, pending_ats) = {
            self.transaction_write(
                move |tx| {
                    let mut version = version;
                    let mut pending_at_child = None;
                    for append_request in batch {
                        (version, pending_at_child) =
                            Self::append(tx, &execution_id, &append_request, version)?;
                    }

                    let pending_at_parent = Self::append_response(
                        tx,
                        &parent_execution_id,
                        parent_response_event,
                        &response_subscribers,
                    )?;
                    Ok::<_, DbErrorWrite>((version, vec![pending_at_child, pending_at_parent]))
                },
                "append_batch_respond_to_parent",
            )
            .await??
        };
        self.notify_pending_all(pending_ats.into_iter().flatten(), current_time);
        Ok(version)
    }

    #[instrument(level = Level::TRACE, skip(self, timeout_fut))]
    async fn wait_for_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        let unique_tag: u64 = rand::random();
        let (sender, mut receiver) = mpsc::channel(1);
        {
            let mut ffqn_to_pending_subscription =
                self.0.ffqn_to_pending_subscription.lock().unwrap();
            for ffqn in ffqns.as_ref() {
                ffqn_to_pending_subscription.insert(ffqn.clone(), (sender.clone(), unique_tag));
            }
        }
        async {
            let Ok(execution_ids_versions) = self
                .conn_low_prio(
                    {
                        let ffqns = ffqns.clone();
                        move |conn| Self::get_pending(conn, 1, pending_at_or_sooner, ffqns.as_ref())
                    },
                    "subscribe_to_pending",
                )
                .await
            else {
                trace!(
                    "Ignoring get_pending error and waiting in for timeout to avoid executor repolling too soon"
                );
                timeout_fut.await;
                return;
            };
            if !execution_ids_versions.is_empty() {
                trace!("Not waiting, database already contains new pending executions");
                return;
            }
            tokio::select! { // future's liveness: Dropping the loser immediately.
                _ = receiver.recv() => {
                    trace!("Received a notification");
                }
                () = timeout_fut => {
                }
            }
        }.await;
        // Clean up ffqn_to_pending_subscription in any case
        {
            let mut ffqn_to_pending_subscription =
                self.0.ffqn_to_pending_subscription.lock().unwrap();
            for ffqn in ffqns.as_ref() {
                match ffqn_to_pending_subscription.remove(ffqn) {
                    Some((_, tag)) if tag == unique_tag => {
                        // Cleanup OK.
                    }
                    Some(other) => {
                        // Reinsert foreign sender.
                        ffqn_to_pending_subscription.insert(ffqn.clone(), other);
                    }
                    None => {
                        // Value was replaced and cleaned up already.
                    }
                }
            }
        }
    }
}

#[async_trait]
impl DbConnection for SqlitePool {
    #[instrument(level = Level::DEBUG, skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbErrorWrite> {
        debug!("create");
        trace!(?req, "create");
        let created_at = req.created_at;
        let (version, pending_at) = self
            .transaction_write(move |tx| Self::create_inner(tx, req), "create")
            .await??;
        self.notify_pending(pending_at, created_at);
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip(self, batch))]
    async fn append_batch(
        &self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        debug!("append_batch");
        trace!(?batch, "append_batch");
        assert!(!batch.is_empty(), "Empty batch request");
        if batch.iter().any(|append_request| {
            matches!(append_request.event, ExecutionEventInner::Created { .. })
        }) {
            panic!("cannot append `Created` event - use `create` instead");
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
                    Ok::<_, DbErrorWrite>((version, pending_at))
                },
                "append_batch",
            )
            .await??;
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
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        debug!("append_batch_create_new_execution");
        trace!(?batch, ?child_req, "append_batch_create_new_execution");
        assert!(!batch.is_empty(), "Empty batch request");
        if batch.iter().any(|append_request| {
            matches!(append_request.event, ExecutionEventInner::Created { .. })
        }) {
            panic!("cannot append `Created` event - use `create` instead");
        }

        let (version, pending_ats) = self
            .transaction_write(
                move |tx| {
                    Self::append_batch_create_new_execution_inner(
                        tx,
                        batch,
                        &execution_id,
                        version,
                        child_req,
                    )
                },
                "append_batch_create_new_execution_inner",
            )
            .await??;
        self.notify_pending_all(pending_ats.into_iter(), current_time);
        Ok(version)
    }

    #[cfg(feature = "test")]
    #[instrument(level = Level::DEBUG, skip(self))]
    async fn get(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<concepts::storage::ExecutionLog, DbErrorRead> {
        trace!("get");
        let execution_id = execution_id.clone();
        self.transaction_read(move |tx| Self::get(tx, &execution_id), "get")
            .await?
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn list_execution_events(
        &self,
        execution_id: &ExecutionId,
        since: &Version,
        max_length: VersionType,
        include_backtrace_id: bool,
    ) -> Result<Vec<ExecutionEvent>, DbErrorRead> {
        let execution_id = execution_id.clone();
        let since = since.0;
        self.transaction_read(
            move |tx| {
                Self::list_execution_events(
                    tx,
                    &execution_id,
                    since,
                    since + max_length,
                    include_backtrace_id,
                )
            },
            "get",
        )
        .await?
    }

    #[instrument(level = Level::DEBUG, skip(self, timeout_fut))]
    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: usize,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbErrorReadWithTimeout> {
        debug!("next_responses");
        let unique_tag: u64 = rand::random();
        let execution_id = execution_id.clone();

        let cleanup = || {
            let mut guard = self.0.response_subscribers.lock().unwrap();
            match guard.remove(&execution_id) {
                Some((_, tag)) if tag == unique_tag => {} // Cleanup OK.
                Some(other) => {
                    // Reinsert foreign sender.
                    guard.insert(execution_id.clone(), other);
                }
                None => {} // Value was replaced and cleaned up already, or notification was sent.
            }
        };

        let response_subscribers = self.0.response_subscribers.clone();
        let resp_or_receiver = {
            let execution_id = execution_id.clone();
            self.transaction_write(
                move |tx| {
                    let responses = Self::get_responses_with_offset(tx, &execution_id, start_idx)?;
                    if responses.is_empty() {
                        // cannot race as we have the transaction write lock
                        let (sender, receiver) = oneshot::channel();
                        response_subscribers
                            .lock()
                            .unwrap()
                            .insert(execution_id, (sender, unique_tag));
                        Ok::<_, DbErrorReadWithTimeout>(itertools::Either::Right(receiver))
                    } else {
                        Ok(itertools::Either::Left(responses))
                    }
                },
                "subscribe_to_next_responses",
            )
            .await
        }
        .map_err(DbErrorReadWithTimeout::from)
        .flatten()
        .inspect_err(|_| {
            cleanup();
        })?;
        match resp_or_receiver {
            itertools::Either::Left(resp) => Ok(resp), // no added subscription
            itertools::Either::Right(receiver) => {
                let res = async move {
                    tokio::select! {
                        resp = receiver => {
                            let resp = resp.map_err(|_| DbErrorGeneric::Close)?;
                            Ok(vec![resp])
                        }
                        () = timeout_fut => Err(DbErrorReadWithTimeout::Timeout),
                    }
                }
                .await;
                cleanup();
                res
            }
        }
    }

    #[instrument(level = Level::DEBUG, skip(self, response_event), fields(join_set_id = %response_event.join_set_id))]
    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbErrorWrite> {
        debug!("append_response");
        let response_subscribers = self.0.response_subscribers.clone();
        let event = JoinSetResponseEventOuter {
            created_at,
            event: response_event,
        };
        let pending_at = self
            .transaction_write(
                move |tx| Self::append_response(tx, &execution_id, event, &response_subscribers),
                "append_response",
            )
            .await??;
        if let Some(pending_at) = pending_at {
            self.notify_pending(pending_at, created_at);
        }
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_backtrace(&self, append: BacktraceInfo) -> Result<(), DbErrorWrite> {
        debug!("append_backtrace");
        self.transaction_write(
            move |tx| Self::append_backtrace(tx, &append),
            "append_backtrace",
        )
        .await?
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_backtrace_batch(&self, batch: Vec<BacktraceInfo>) -> Result<(), DbErrorWrite> {
        debug!("append_backtrace_batch");
        self.transaction_write(
            move |tx| {
                for append in batch {
                    Self::append_backtrace(tx, &append)?;
                }
                Ok(())
            },
            "append_backtrace",
        )
        .await?
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn get_backtrace(
        &self,
        execution_id: &ExecutionId,
        filter: BacktraceFilter,
    ) -> Result<BacktraceInfo, DbErrorRead> {
        debug!("get_last_backtrace");
        let execution_id = execution_id.clone();

        self.transaction_read(
            move |tx| {
                let select = "SELECT component_id, version_min_including, version_max_excluding, wasm_backtrace FROM t_backtrace \
                                WHERE execution_id = :execution_id";
                let mut params: Vec<(&'static str, Box<dyn rusqlite::ToSql>)> = vec![(":execution_id", Box::new(execution_id.to_string()))];
                let select = match filter {
                    BacktraceFilter::Specific(version) =>{
                        params.push((":version", Box::new(version.0)));
                        format!("{select} AND version_min_including <= :version AND version_max_excluding > :version")
                    },
                    BacktraceFilter::First => format!("{select} ORDER BY version_min_including LIMIT 1"),
                    BacktraceFilter::Last => format!("{select} ORDER BY version_min_including DESC LIMIT 1")
               };
                tx
                    .prepare(&select)
                    .map_err(prepare_err_generic)?
                    .query_row::<_, &[(&'static str, &dyn ToSql)], _>(
                        params
                            .iter()
                            .map(|(key, value)| (*key, value.as_ref()))
                            .collect::<Vec<_>>()
                            .as_ref(),
                    |row| {
                        Ok(BacktraceInfo {
                            execution_id: execution_id.clone(),
                            component_id: row.get::<_, FromStrWrapper<_> >("component_id")?.0,
                            version_min_including: Version::new(row.get::<_, VersionType>("version_min_including")?),
                            version_max_excluding: Version::new(row.get::<_, VersionType>("version_max_excluding")?),
                            wasm_backtrace: row.get::<_, JsonWrapper<_>>("wasm_backtrace")?.0,
                        })
                    },
                )
                .map_err(result_err_read)
            },
            "get_last_backtrace",
        ).await?
    }

    /// Get currently expired delays and locks.
    #[instrument(level = Level::TRACE, skip(self))]
    async fn get_expired_timers(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbErrorGeneric> {
        self.conn_low_prio(
            move |conn| {
                let mut expired_timers = conn.prepare(
                    "SELECT execution_id, join_set_id, delay_id FROM t_delay WHERE expires_at <= :at",
                ).map_err(prepare_err_generic)?
                .query_map(
                        named_params! {
                            ":at": at,
                        },
                        |row| {
                            let execution_id = row.get("execution_id")?;
                            let join_set_id = row.get::<_, JoinSetId>("join_set_id")?;
                            let delay_id = row.get::<_, DelayId>("delay_id")?;
                            Ok(ExpiredTimer::Delay { execution_id, join_set_id, delay_id })
                        },
                    ).map_err(result_err_generic)?
                    .collect::<Result<Vec<_>, _>>().map_err(result_err_generic)?;
                // Extend with expired locks
                let expired = conn.prepare(&format!(r#"
                    SELECT execution_id, corresponding_version, intermittent_event_count, max_retries, retry_exp_backoff_millis
                    FROM t_state
                    WHERE pending_expires_finished <= :at AND state = "{STATE_LOCKED}"
                    "#
                )
                ).map_err(prepare_err_generic)?
                .query_map(
                        named_params! {
                            ":at": at,
                        },
                        |row| {
                            let execution_id = row.get("execution_id")?;
                            let locked_at_version = Version::new(row.get("corresponding_version")?);
                            let next_version = locked_at_version.increment();
                            let intermittent_event_count = row.get("intermittent_event_count")?;
                            let max_retries = row.get("max_retries")?;
                            let retry_exp_backoff_millis = row.get("retry_exp_backoff_millis")?;
                            let parent = if let ExecutionId::Derived(derived) = &execution_id {
                                derived.split_to_parts().inspect_err(|err| error!("cannot split execution {execution_id} to parts: {err:?}")).ok()
                            } else {
                                None
                            };
                            Ok(ExpiredTimer::Lock { execution_id, locked_at_version, next_version, intermittent_event_count,
                                max_retries,
                                retry_exp_backoff: Duration::from_millis(retry_exp_backoff_millis),
                                parent})
                        }
                    ).map_err(result_err_generic)?
                    .collect::<Result<Vec<_>, _>>().map_err(result_err_generic)?;
                expired_timers.extend(expired);
                if !expired_timers.is_empty() {
                    debug!("get_expired_timers found {expired_timers:?}");
                }
                Ok(expired_timers)
            }, "get_expired_timers"
        )
        .await
    }

    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout_fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ) -> Result<SupportedFunctionReturnValue, DbErrorReadWithTimeout> {
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
                                if let ExecutionEventInner::Finished { result, ..} = event.event {
                                    Ok(Some(result))
                                } else {
                                    error!("Mismatch, expected Finished row: {event:?} based on t_state {finished}");
                                    Err(DbErrorReadWithTimeout::from(consistency_db_err(
                                        "cannot get finished event based on t_state version"
                                    )))
                                }
                            } else {
                                Ok(None)
                            }
                        }, "wait_for_finished_result")
                        .await??
                    {
                        return Ok(execution_result);
                    }
                    // TODO: change to subscription based approach
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };

            if let Some(timeout_fut) = timeout_fut {
                tokio::select! { // future's liveness: Dropping the loser immediately.
                    res = fut => res,
                    () = timeout_fut => Err(DbErrorReadWithTimeout::Timeout)
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
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let version = version.0;
        let execution_id = execution_id.clone();
        self.transaction_read(
            move |tx| Self::get_execution_event(tx, &execution_id, version),
            "get_execution_event",
        )
        .await?
    }

    async fn get_pending_state(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<PendingState, DbErrorRead> {
        let execution_id = execution_id.clone();
        Ok(self
            .transaction_read(
                move |tx| Self::get_combined_state(tx, &execution_id),
                "get_pending_state",
            )
            .await??
            .pending_state)
    }

    async fn list_executions(
        &self,
        ffqn: Option<FunctionFqn>,
        top_level_only: bool,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbErrorGeneric> {
        self.transaction_read(
            move |tx| Self::list_executions(tx, ffqn, top_level_only, pagination),
            "list_executions",
        )
        .await?
    }

    async fn list_responses(
        &self,
        execution_id: &ExecutionId,
        pagination: Pagination<u32>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorRead> {
        let execution_id = execution_id.clone();
        self.transaction_read(
            move |tx| Self::list_responses(tx, &execution_id, Some(pagination)),
            "list_executions",
        )
        .await?
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
