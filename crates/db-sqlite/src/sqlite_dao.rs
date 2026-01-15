use crate::{histograms::Histograms, sqlite_dao::conversions::to_generic_error};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentId, ComponentRetryConfig, ExecutionId, FunctionFqn, JoinSetId, StrVariant,
    SupportedFunctionReturnValue,
    component_id::InputContentDigest,
    prefixed_ulid::{DelayId, ExecutionIdDerived, ExecutorId, RunId},
    storage::{
        AppendBatchResponse, AppendDelayResponseOutcome, AppendEventsToExecution, AppendRequest,
        AppendResponse, AppendResponseToExecution, BacktraceFilter, BacktraceInfo, CreateRequest,
        DUMMY_CREATED, DUMMY_HISTORY_EVENT, DbConnection, DbErrorGeneric, DbErrorRead,
        DbErrorReadWithTimeout, DbErrorWrite, DbErrorWriteNonRetriable, DbExecutor, DbExternalApi,
        DbPool, DbPoolCloseable, ExecutionEvent, ExecutionListPagination, ExecutionRequest,
        ExecutionWithState, ExecutionWithStateRequestsResponses, ExpiredDelay, ExpiredLock,
        ExpiredTimer, HISTORY_EVENT_TYPE_JOIN_NEXT, HistoryEvent, JoinSetRequest, JoinSetResponse,
        JoinSetResponseEvent, JoinSetResponseEventOuter, ListExecutionsFilter, LockPendingResponse,
        Locked, LockedBy, LockedExecution, LogFilter, LogInfo, LogInfoAppendRow, LogInfoRow,
        LogLevel, LogStreamType, Pagination, PendingState, PendingStateFinished,
        PendingStateFinishedResultKind, PendingStateLocked, ResponseWithCursor,
        STATE_BLOCKED_BY_JOIN_SET, STATE_FINISHED, STATE_LOCKED, STATE_PENDING_AT, TimeoutOutcome,
        Version, VersionType,
    },
};
use conversions::{JsonWrapper, consistency_db_err, consistency_rusqlite};
use hashbrown::HashMap;
use rusqlite::{
    CachedStatement, Connection, OpenFlags, OptionalExtension, Params, ToSql, Transaction,
    TransactionBehavior, named_params, types::ToSqlOutput,
};
use std::{
    cmp::max,
    collections::VecDeque,
    fmt::Debug,
    ops::DerefMut,
    panic::Location,
    path::Path,
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
use tracing::{Level, Span, debug, error, info, instrument, trace, warn};
use tracing_error::SpanTrace;

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
const T_METADATA_EXPECTED_SCHEMA_VERSION: u32 = 5;

/// Stores execution history. Append only.
const CREATE_TABLE_T_EXECUTION_LOG: &str = r"
CREATE TABLE IF NOT EXISTS t_execution_log (
    execution_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    json_value TEXT NOT NULL,
    version INTEGER NOT NULL,
    variant TEXT NOT NULL,
    join_set_id TEXT,
    history_event_type TEXT GENERATED ALWAYS AS (json_value->>'$.history_event.event.type') STORED,
    PRIMARY KEY (execution_id, version)
) STRICT
";
// Used in `fetch_created` and `get_execution_event`
const CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VERSION: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_version  ON t_execution_log (execution_id, version);
";
// Used in `lock_inner` to filter by execution ID and variant (created or event history)
const CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VARIANT: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_variant  ON t_execution_log (execution_id, variant);
";

// Used in `count_join_next`
const CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_JOIN_SET: &str = const_format::formatcp!(
    "CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_join_set  ON t_execution_log (execution_id, join_set_id, history_event_type) WHERE history_event_type=\"{}\";",
    HISTORY_EVENT_TYPE_JOIN_NEXT
);

/// Stores child execution return values for the parent (`execution_id`). Append only.
/// For `JoinSetResponse::DelayFinished`, columns `delay_id`,`delay_success` must not not null.
/// For `JoinSetResponse::ChildExecutionFinished`, columns `child_execution_id`,`finished_version`
/// must not be null.
const CREATE_TABLE_T_JOIN_SET_RESPONSE: &str = r"
CREATE TABLE IF NOT EXISTS t_join_set_response (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,

    delay_id TEXT,
    delay_success INTEGER,

    child_execution_id TEXT,
    finished_version INTEGER,

    UNIQUE (execution_id, join_set_id, delay_id, child_execution_id)
) STRICT
";
// Used when querying for the next response
const CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID_ID: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_join_set_response_execution_id_id ON t_join_set_response (execution_id, id);
";
// Child execution id must be unique.
const CREATE_INDEX_IDX_JOIN_SET_RESPONSE_UNIQUE_CHILD_ID: &str = r"
CREATE UNIQUE INDEX IF NOT EXISTS idx_join_set_response_unique_child_id
ON t_join_set_response (child_execution_id) WHERE child_execution_id IS NOT NULL;
";
// Delay id must be unique.
const CREATE_INDEX_IDX_JOIN_SET_RESPONSE_UNIQUE_DELAY_ID: &str = r"
CREATE UNIQUE INDEX IF NOT EXISTS idx_join_set_response_unique_delay_id
ON t_join_set_response (delay_id) WHERE delay_id IS NOT NULL;
";

/// Stores executions in `PendingState`
/// `state` to column mapping:
/// `PendingAt`:            (nothing but required columns), optionally contains all `Locked` columns.
/// `Locked`:               `max_retries`, `retry_exp_backoff_millis`, `last_lock_version`, `executor_id`, `run_id`
/// `BlockedByJoinSet`:     `join_set_id`, `join_set_closing`
/// `Finished` :            `result_kind`.
///
/// `pending_expires_finished` - either pending at, lock expires at or finished at based on state.
///
const CREATE_TABLE_T_STATE: &str = r"
CREATE TABLE IF NOT EXISTS t_state (
    execution_id TEXT NOT NULL,
    is_top_level INTEGER NOT NULL,
    corresponding_version INTEGER NOT NULL,
    ffqn TEXT NOT NULL,
    created_at TEXT NOT NULL,
    component_id_input_digest BLOB NOT NULL,
    first_scheduled_at TEXT NOT NULL,

    pending_expires_finished TEXT NOT NULL,
    state TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    intermittent_event_count INTEGER NOT NULL,

    max_retries INTEGER,
    retry_exp_backoff_millis INTEGER,
    last_lock_version INTEGER,
    executor_id TEXT,
    run_id TEXT,

    join_set_id TEXT,
    join_set_closing INTEGER,

    result_kind TEXT,

    PRIMARY KEY (execution_id)
) STRICT
";

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

/// Stores logs and std stream output of execution runs. Append only.
/// Logs have `level` and `message` null.
/// Std streams have `stream_type`, `payload` not null.
const CREATE_TABLE_T_LOG: &str = r"
CREATE TABLE IF NOT EXISTS t_log (
    id INTEGER PRIMARY KEY,
    execution_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    level INTEGER,
    message TEXT,
    stream_type INTEGER,
    payload BLOB
) STRICT
";
const IDX_T_LOG_EXECUTION_ID_RUN_ID_CREATED_AT: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_log_execution_id_run_id_created_at ON t_log (execution_id, run_id, created_at);
";
const IDX_T_LOG_EXECUTION_ID_CREATED_AT: &str = r"
CREATE INDEX IF NOT EXISTS idx_t_log_execution_id_created_at ON t_log (execution_id, created_at);
";

#[derive(Debug, thiserror::Error, Clone)]
enum RusqliteError {
    #[error("not found")]
    NotFound,
    #[error("generic: {reason}")]
    Generic {
        reason: StrVariant,
        context: SpanTrace,
        source: Option<Arc<dyn std::error::Error + Send + Sync>>,
        loc: &'static Location<'static>,
    },
    #[error("close")]
    Close,
}

mod conversions {

    use super::RusqliteError;
    use concepts::{
        StrVariant,
        storage::{DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite},
    };
    use rusqlite::{
        ToSql,
        types::{FromSql, FromSqlError},
    };
    use std::{fmt::Debug, panic::Location, sync::Arc};
    use tracing::error;
    use tracing_error::SpanTrace;

    impl From<rusqlite::Error> for RusqliteError {
        // The LTX returns this, capture inner location.
        #[track_caller]
        fn from(err: rusqlite::Error) -> Self {
            if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
                RusqliteError::NotFound
            } else {
                RusqliteError::Generic {
                    reason: err.to_string().into(),
                    context: SpanTrace::capture(),
                    source: Some(Arc::new(err)),
                    loc: Location::caller(),
                }
            }
        }
    }

    // Manual conversion, as this function ignores the NotFound.
    #[track_caller]
    pub fn to_generic_error(err: RusqliteError) -> DbErrorGeneric {
        if let RusqliteError::Close = err {
            DbErrorGeneric::Close
        } else {
            DbErrorGeneric::Uncategorized {
                reason: err.to_string().into(),
                context: SpanTrace::capture(),
                source: Some(Arc::new(err)),
                loc: Location::caller(),
            }
        }
    }

    impl From<RusqliteError> for DbErrorRead {
        fn from(err: RusqliteError) -> Self {
            if matches!(err, RusqliteError::NotFound) {
                Self::NotFound
            } else {
                to_generic_error(err).into()
            }
        }
    }
    impl From<RusqliteError> for DbErrorReadWithTimeout {
        fn from(err: RusqliteError) -> Self {
            Self::from(DbErrorRead::from(err))
        }
    }
    impl From<RusqliteError> for DbErrorWrite {
        fn from(err: RusqliteError) -> Self {
            if matches!(err, RusqliteError::NotFound) {
                Self::NotFound
            } else {
                to_generic_error(err).into()
            }
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
    impl<T: serde::ser::Serialize + Debug> ToSql for JsonWrapper<T> {
        fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
            let string = serde_json::to_string(&self.0).map_err(|err| {
                error!(
                    "Cannot serialize {value:?} of type `{type}` - {err:?}",
                    value = self.0,
                    r#type = std::any::type_name::<T>()
                );
                rusqlite::Error::ToSqlConversionFailure(Box::new(err))
            })?;
            Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Text(string),
            ))
        }
    }

    // Used as a wrapper for `FromSqlError::Other`
    #[derive(Debug, thiserror::Error)]
    #[error("{reason}")]
    pub(crate) struct OtherError {
        reason: StrVariant,
        loc: &'static Location<'static>,
    }

    #[track_caller]
    pub(crate) fn consistency_rusqlite(reason: impl Into<StrVariant>) -> rusqlite::Error {
        FromSqlError::other(OtherError {
            reason: reason.into(),
            loc: Location::caller(),
        })
        .into()
    }

    #[track_caller]
    pub(crate) fn consistency_db_err(reason: impl Into<StrVariant>) -> DbErrorGeneric {
        DbErrorGeneric::Uncategorized {
            reason: reason.into(),
            context: SpanTrace::capture(),
            source: None,
            loc: Location::caller(),
        }
    }
}

#[derive(Debug)]
struct CombinedStateDTO {
    execution_id: ExecutionId,
    state: String,
    ffqn: FunctionFqn,
    component_digest: InputContentDigest,
    created_at: DateTime<Utc>,
    first_scheduled_at: DateTime<Utc>,
    pending_expires_finished: DateTime<Utc>,
    // Locked:
    last_lock_version: Option<Version>,
    executor_id: Option<ExecutorId>,
    run_id: Option<RunId>,
    // Blocked by join set:
    join_set_id: Option<JoinSetId>,
    join_set_closing: Option<bool>,
    // Finished:
    result_kind: Option<PendingStateFinishedResultKind>,
}
#[derive(Debug)]
struct CombinedState {
    execution_with_state: ExecutionWithState,
    corresponding_version: Version,
}
impl CombinedState {
    fn get_next_version_assert_not_finished(&self) -> Version {
        assert!(!self.execution_with_state.pending_state.is_finished());
        self.corresponding_version.increment()
    }

    #[cfg(feature = "test")]
    fn get_next_version_or_finished(&self) -> Version {
        if self.execution_with_state.pending_state.is_finished() {
            self.corresponding_version.clone()
        } else {
            self.corresponding_version.increment()
        }
    }
}

#[derive(Debug)]
struct NotifierPendingAt {
    scheduled_at: DateTime<Utc>,
    ffqn: FunctionFqn,
    component_input_digest: InputContentDigest,
}

#[derive(Debug)]
struct NotifierExecutionFinished {
    execution_id: ExecutionId,
    retval: SupportedFunctionReturnValue,
}

#[derive(Debug, Default)]
struct AppendNotifier {
    pending_at: Option<NotifierPendingAt>,
    execution_finished: Option<NotifierExecutionFinished>,
    response: Option<(ExecutionId, JoinSetResponseEventOuter)>,
}

impl CombinedState {
    fn new(dto: CombinedStateDTO, corresponding_version: Version) -> Result<Self, rusqlite::Error> {
        let execution_with_state = match dto {
            // Pending - just created
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } if state == STATE_PENDING_AT => ExecutionWithState {
                component_digest,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::PendingAt {
                    scheduled_at,
                    last_lock: None,
                },
            },
            // Pending, previously locked
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } if state == STATE_PENDING_AT => ExecutionWithState {
                component_digest,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::PendingAt {
                    scheduled_at,
                    last_lock: Some(LockedBy {
                        executor_id,
                        run_id,
                    }),
                },
            },
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                pending_expires_finished: lock_expires_at,
                last_lock_version: Some(_),
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
            } if state == STATE_LOCKED => ExecutionWithState {
                component_digest,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Locked(PendingStateLocked {
                    locked_by: LockedBy {
                        executor_id,
                        run_id,
                    },
                    lock_expires_at,
                }),
            },
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                pending_expires_finished: lock_expires_at,
                last_lock_version: None,
                executor_id: _,
                run_id: _,
                join_set_id: Some(join_set_id),
                join_set_closing: Some(join_set_closing),
                result_kind: None,
            } if state == STATE_BLOCKED_BY_JOIN_SET => ExecutionWithState {
                component_digest,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::BlockedByJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing: join_set_closing,
                    lock_expires_at,
                },
            },
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                pending_expires_finished: finished_at,
                last_lock_version: None,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: Some(result_kind),
            } if state == STATE_FINISHED => ExecutionWithState {
                component_digest,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Finished {
                    finished: PendingStateFinished {
                        finished_at,
                        version: corresponding_version.0,
                        result_kind,
                    },
                },
            },
            _ => {
                error!("Cannot deserialize pending state from  {dto:?}");
                return Err(consistency_rusqlite("invalid `t_state`"));
            }
        };
        Ok(Self {
            execution_with_state,
            corresponding_version,
        })
    }
}

#[derive(derive_more::Debug)]
struct LogicalTx {
    #[debug(skip)]
    func: Box<dyn FnMut(&mut Transaction) + Send>,
    sent_at: Instant,
    func_name: &'static str,
    #[debug(skip)]
    commit_ack_sender: oneshot::Sender<Result<(), RusqliteError>>,
}

#[derive(derive_more::Debug)]
enum ThreadCommand {
    LogicalTx(LogicalTx),
    Shutdown,
}

#[derive(Clone)]
pub struct SqlitePool(SqlitePoolInner);

type ResponseSubscribers =
    Arc<Mutex<HashMap<ExecutionId, (oneshot::Sender<JoinSetResponseEventOuter>, u64)>>>;
type PendingSubscribers = Arc<Mutex<PendingFfqnSubscribersHolder>>;
type ExecutionFinishedSubscribers =
    Mutex<HashMap<ExecutionId, HashMap<u64, oneshot::Sender<SupportedFunctionReturnValue>>>>;

#[derive(Default)]
struct PendingFfqnSubscribersHolder {
    by_ffqns: HashMap<FunctionFqn, (mpsc::Sender<()>, u64)>,
    by_component: HashMap<InputContentDigest /* input digest */, (mpsc::Sender<()>, u64)>,
}
impl PendingFfqnSubscribersHolder {
    fn notify(&self, notifier: &NotifierPendingAt) {
        if let Some((subscription, _)) = self.by_ffqns.get(&notifier.ffqn) {
            debug!("Notifying pending subscriber by ffqn");
            // Does not block
            let _ = subscription.try_send(());
        }
        if let Some((subscription, _)) = self.by_component.get(&notifier.component_input_digest) {
            debug!("Notifying pending subscriber by component");
            // Does not block
            let _ = subscription.try_send(());
        }
    }

    fn insert_ffqn(&mut self, ffqn: FunctionFqn, value: (mpsc::Sender<()>, u64)) {
        self.by_ffqns.insert(ffqn, value);
    }

    fn remove_ffqn(&mut self, ffqn: &FunctionFqn) -> Option<(mpsc::Sender<()>, u64)> {
        self.by_ffqns.remove(ffqn)
    }

    fn insert_by_component(
        &mut self,
        input_content_digest: InputContentDigest,
        value: (mpsc::Sender<()>, u64),
    ) {
        self.by_component.insert(input_content_digest, value);
    }

    fn remove_by_component(
        &mut self,
        input_content_digest: &InputContentDigest,
    ) -> Option<(mpsc::Sender<()>, u64)> {
        self.by_component.remove(input_content_digest)
    }
}

#[derive(Clone)]
struct SqlitePoolInner {
    shutdown_requested: Arc<AtomicBool>,
    shutdown_finished: Arc<AtomicBool>,
    command_tx: tokio::sync::mpsc::Sender<ThreadCommand>,
    response_subscribers: ResponseSubscribers,
    pending_subscribers: PendingSubscribers,
    execution_finished_subscribers: Arc<ExecutionFinishedSubscribers>,
    join_handle: Option<Arc<std::thread::JoinHandle<()>>>, // always Some, Optional for swapping in drop.
}

#[async_trait]
impl DbPoolCloseable for SqlitePool {
    async fn close(&self) {
        debug!("Sqlite is closing");
        self.0.shutdown_requested.store(true, Ordering::Release);
        // Unblock the thread's blocking_recv. If the capacity is reached, the next processed message will trigger shutdown.
        let _ = self.0.command_tx.try_send(ThreadCommand::Shutdown);
        while !self.0.shutdown_finished.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        debug!("Sqlite was closed");
    }
}

#[async_trait]
impl DbPool for SqlitePool {
    async fn db_exec_conn(&self) -> Result<Box<dyn DbExecutor>, DbErrorGeneric> {
        if self.0.shutdown_requested.load(Ordering::Acquire) {
            return Err(DbErrorGeneric::Close);
        }
        Ok(Box::new(self.clone()))
    }

    async fn connection(&self) -> Result<Box<dyn DbConnection>, DbErrorGeneric> {
        if self.0.shutdown_requested.load(Ordering::Acquire) {
            return Err(DbErrorGeneric::Close);
        }
        Ok(Box::new(self.clone()))
    }
    async fn external_api_conn(&self) -> Result<Box<dyn DbExternalApi>, DbErrorGeneric> {
        if self.0.shutdown_requested.load(Ordering::Acquire) {
            return Err(DbErrorGeneric::Close);
        }
        Ok(Box::new(self.clone()))
    }
    #[cfg(feature = "test")]
    async fn connection_test(
        &self,
    ) -> Result<Box<dyn concepts::storage::DbConnectionTest>, DbErrorGeneric> {
        if self.0.shutdown_requested.load(Ordering::Acquire) {
            return Err(DbErrorGeneric::Close);
        }
        Ok(Box::new(self.clone()))
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
    pub pragma_override: Option<HashMap<String, String>>,
    pub metrics_threshold: Option<Duration>,
}
impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 100,
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
        fn conn_execute<P: Params>(
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
        {
            conn_execute(&conn, CREATE_TABLE_T_METADATA, [])?;
            // Insert row if not exists.

            let actual_version = conn
                .prepare("SELECT schema_version FROM t_metadata ORDER BY id DESC LIMIT 1")
                .map_err(|err| {
                    error!("cannot select schema version - {err:?}");
                    InitializationError
                })?
                .query_row([], |row| row.get::<_, u32>("schema_version"))
                .optional()
                .map_err(|err| {
                    error!("Cannot read the schema version - {err:?}");
                    InitializationError
                })?;

            match actual_version {
                None => conn_execute(
                    &conn,
                    &format!(
                        "INSERT INTO t_metadata (schema_version, created_at) VALUES
                            ({T_METADATA_EXPECTED_SCHEMA_VERSION}, ?) ON CONFLICT DO NOTHING"
                    ),
                    [Utc::now()],
                )?,
                Some(actual_version) => {
                    // Fail on unexpected `schema_version`.
                    if actual_version != T_METADATA_EXPECTED_SCHEMA_VERSION {
                        error!(
                            "wrong schema version, expected {T_METADATA_EXPECTED_SCHEMA_VERSION}, got {actual_version}"
                        );
                        return Err(InitializationError);
                    }
                }
            }
        }

        // t_execution_log
        conn_execute(&conn, CREATE_TABLE_T_EXECUTION_LOG, [])?;
        conn_execute(
            &conn,
            CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VERSION,
            [],
        )?;
        conn_execute(
            &conn,
            CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_VARIANT,
            [],
        )?;
        conn_execute(
            &conn,
            CREATE_INDEX_IDX_T_EXECUTION_LOG_EXECUTION_ID_JOIN_SET,
            [],
        )?;
        // t_join_set_response
        conn_execute(&conn, CREATE_TABLE_T_JOIN_SET_RESPONSE, [])?;
        conn_execute(
            &conn,
            CREATE_INDEX_IDX_T_JOIN_SET_RESPONSE_EXECUTION_ID_ID,
            [],
        )?;
        conn_execute(
            &conn,
            CREATE_INDEX_IDX_JOIN_SET_RESPONSE_UNIQUE_CHILD_ID,
            [],
        )?;
        conn_execute(
            &conn,
            CREATE_INDEX_IDX_JOIN_SET_RESPONSE_UNIQUE_DELAY_ID,
            [],
        )?;
        // t_state
        conn_execute(&conn, CREATE_TABLE_T_STATE, [])?;
        conn_execute(&conn, IDX_T_STATE_LOCK_PENDING, [])?;
        conn_execute(&conn, IDX_T_STATE_EXPIRED_TIMERS, [])?;
        conn_execute(&conn, IDX_T_STATE_EXECUTION_ID_IS_TOP_LEVEL, [])?;
        conn_execute(&conn, IDX_T_STATE_FFQN, [])?;
        conn_execute(&conn, IDX_T_STATE_CREATED_AT, [])?;
        // t_delay
        conn_execute(&conn, CREATE_TABLE_T_DELAY, [])?;
        // t_backtrace
        conn_execute(&conn, CREATE_TABLE_T_BACKTRACE, [])?;
        conn_execute(&conn, IDX_T_BACKTRACE_EXECUTION_ID_VERSION, [])?;
        // t_log
        conn_execute(&conn, CREATE_TABLE_T_LOG, [])?;
        conn_execute(&conn, IDX_T_LOG_EXECUTION_ID_RUN_ID_CREATED_AT, [])?;
        conn_execute(&conn, IDX_T_LOG_EXECUTION_ID_CREATED_AT, [])?;
        Ok(conn)
    }

    fn connection_rpc(
        mut conn: Connection,
        shutdown_requested: &AtomicBool,
        shutdown_finished: &AtomicBool,
        mut command_rx: mpsc::Receiver<ThreadCommand>,
        metrics_threshold: Option<Duration>,
    ) {
        let mut histograms = Histograms::new(metrics_threshold);
        while Self::tick(
            &mut conn,
            shutdown_requested,
            &mut command_rx,
            &mut histograms,
        )
        .is_ok()
        {
            // Loop until shutdown is set to true.
        }
        debug!("Closing command thread");
        shutdown_finished.store(true, Ordering::Release);
    }

    fn tick(
        conn: &mut Connection,
        shutdown_requested: &AtomicBool,
        command_rx: &mut mpsc::Receiver<ThreadCommand>,
        histograms: &mut Histograms,
    ) -> Result<(), ()> {
        let mut ltx_list = Vec::new();
        // Wait for first logical tx.
        let mut ltx = match command_rx.blocking_recv() {
            Some(ThreadCommand::LogicalTx(ltx)) => ltx,
            Some(ThreadCommand::Shutdown) => {
                debug!("Shutdown message received");
                return Err(());
            }
            None => {
                debug!("command_rx was closed");
                return Err(());
            }
        };
        let all_fns_start = std::time::Instant::now();
        let mut physical_tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|begin_err| {
                error!("Cannot open transaction, closing sqlite - {begin_err:?}");
            })?;
        Self::ltx_apply_to_tx(&mut ltx, &mut physical_tx, histograms);
        ltx_list.push(ltx);

        while let Ok(more) = command_rx.try_recv() {
            let mut ltx = match more {
                ThreadCommand::Shutdown => {
                    debug!("Shutdown message received");
                    return Err(());
                }
                ThreadCommand::LogicalTx(ltx) => ltx,
            };

            Self::ltx_apply_to_tx(&mut ltx, &mut physical_tx, histograms);
            ltx_list.push(ltx);
        }
        histograms.record_all_fns(all_fns_start.elapsed());

        {
            // Commit
            if shutdown_requested.load(Ordering::Relaxed) {
                debug!("Recveived shutdown during processing of the batch");
                return Err(());
            }
            let commit_result = {
                let now = std::time::Instant::now();
                let commit_result = physical_tx.commit().map_err(RusqliteError::from);
                histograms.record_commit(now.elapsed());
                commit_result
            };
            if commit_result.is_ok() || ltx_list.len() == 1 {
                // Happy path - just send the commit ACK.
                for ltx in ltx_list {
                    // Ignore the result: ThreadCommand producer timed out before awaiting the ack.
                    let _ = ltx.commit_ack_sender.send(commit_result.clone());
                }
            } else {
                warn!(
                    "Bulk transaction failed, committing {} transactions serially",
                    ltx_list.len()
                );
                // Bulk transaction failed. Replay each ltx in its own physical transaction.
                // TODO: Use SAVEPOINT + RELEASE SAVEPOINT, ROLLBACK savepoint instead.
                for ltx in ltx_list {
                    Self::ltx_commit_single(ltx, conn, shutdown_requested, histograms)?;
                }
            }
        }
        histograms.print_if_elapsed();
        Ok(())
    }

    fn ltx_commit_single(
        mut ltx: LogicalTx,
        conn: &mut Connection,
        shutdown_requested: &AtomicBool,
        histograms: &mut Histograms,
    ) -> Result<(), ()> {
        let mut physical_tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|begin_err| {
                error!("Cannot open transaction, closing sqlite - {begin_err:?}");
            })?;
        Self::ltx_apply_to_tx(&mut ltx, &mut physical_tx, histograms);
        if shutdown_requested.load(Ordering::Relaxed) {
            debug!("Recveived shutdown during processing of the batch");
            return Err(());
        }
        let commit_result = {
            let now = std::time::Instant::now();
            let commit_result = physical_tx.commit().map_err(RusqliteError::from);
            histograms.record_commit(now.elapsed());
            commit_result
        };
        // Ignore the result: ThreadCommand producer timed out before awaiting the ack.
        let _ = ltx.commit_ack_sender.send(commit_result);
        Ok(())
    }

    fn ltx_apply_to_tx(
        ltx: &mut LogicalTx,
        physical_tx: &mut Transaction,
        histograms: &mut Histograms,
    ) {
        let sent_latency = ltx.sent_at.elapsed();
        let started_at = Instant::now();
        (ltx.func)(physical_tx);
        histograms.record_command(sent_latency, ltx.func_name, started_at.elapsed());
    }

    #[instrument(skip_all, name = "sqlite_new")]
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
                    config.metrics_threshold,
                );
            })
        };
        Ok(SqlitePool(SqlitePoolInner {
            shutdown_requested,
            shutdown_finished,
            command_tx,
            response_subscribers: Arc::default(),
            pending_subscribers: Arc::default(),
            join_handle: Some(Arc::new(join_handle)),
            execution_finished_subscribers: Arc::default(),
        }))
    }

    /// Invokes the provided function wrapping a new [`rusqlite::Transaction`] that is committed automatically.
    async fn transaction<F, T, E>(&self, mut func: F, func_name: &'static str) -> Result<T, E>
    where
        F: FnMut(&mut rusqlite::Transaction) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<RusqliteError> + Send + 'static,
    {
        let fn_res: Arc<std::sync::Mutex<Option<_>>> = Arc::default();
        let (commit_ack_sender, commit_ack_receiver) = oneshot::channel();
        let current_span = Span::current();
        let thread_command_func = {
            let fn_res = fn_res.clone();
            ThreadCommand::LogicalTx(LogicalTx {
                func: Box::new(move |tx| {
                    let _guard = current_span.enter();
                    let func_res = func(tx);
                    *fn_res.lock().unwrap() = Some(func_res);
                }),
                sent_at: Instant::now(),
                func_name,
                commit_ack_sender,
            })
        };
        self.0
            .command_tx
            .send(thread_command_func)
            .await
            .map_err(|_send_err| RusqliteError::Close)?;

        // Wait for commit ack, then get the retval from the mutex.
        match commit_ack_receiver.await {
            Ok(Ok(())) => {
                let mut guard = fn_res.lock().unwrap();
                std::mem::take(guard.deref_mut()).expect("ltx must have been run at least once")
            }
            Ok(Err(rusqlite_err)) => Err(E::from(rusqlite_err)),
            Err(_) => Err(E::from(RusqliteError::Close)),
        }
    }

    fn fetch_created_event(
        conn: &Connection,
        execution_id: &ExecutionId,
    ) -> Result<CreateRequest, DbErrorRead> {
        let mut stmt = conn.prepare(
            "SELECT created_at, json_value FROM t_execution_log WHERE \
            execution_id = :execution_id AND version = 0",
        )?;
        let (created_at, event) = stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                let created_at = row.get("created_at")?;
                let event = row
                    .get::<_, JsonWrapper<ExecutionRequest>>("json_value")
                    .map_err(|serde| {
                        error!("cannot deserialize `Created` event: {row:?} - `{serde:?}`");
                        consistency_rusqlite("cannot deserialize `Created` event")
                    })?;
                Ok((created_at, event.0))
            },
        )?;
        if let ExecutionRequest::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
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
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::VersionConflict {
                    expected: expected_version.clone(),
                    requested: appending_version.clone(),
                },
            ));
        }
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip_all, fields(execution_id = %req.execution_id))]
    fn create_inner(
        tx: &Transaction,
        req: CreateRequest,
    ) -> Result<(AppendResponse, AppendNotifier), DbErrorWrite> {
        trace!("create_inner");

        let version = Version::default();
        let execution_id = req.execution_id.clone();
        let execution_id_str = execution_id.to_string();
        let ffqn = req.ffqn.clone();
        let created_at = req.created_at;
        let scheduled_at = req.scheduled_at;
        let component_id = req.component_id.clone();
        let event = ExecutionRequest::from(req);
        let event_ser = serde_json::to_string(&event).map_err(|err| {
            error!("Cannot serialize {event:?} - {err:?}");
            DbErrorWriteNonRetriable::ValidationFailed("parameter serialization error".into())
        })?;
        tx.prepare(
                "INSERT INTO t_execution_log (execution_id, created_at, version, json_value, variant, join_set_id ) \
                VALUES (:execution_id, :created_at, :version, :json_value, :variant, :join_set_id)")
        ?
        .execute(named_params! {
            ":execution_id": &execution_id_str,
            ":created_at": created_at,
            ":version": version.0,
            ":json_value": event_ser,
            ":variant": event.variant(),
            ":join_set_id": event.join_set_id().map(std::string::ToString::to_string),
        })
        ?;
        let pending_at = {
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
                    component_id_input_digest,
                    updated_at,
                    first_scheduled_at,
                    intermittent_event_count
                    )
                VALUES (
                    :execution_id,
                    :is_top_level,
                    :corresponding_version,
                    :pending_expires_finished,
                    :ffqn,
                    :state,
                    :created_at,
                    :component_id_input_digest,
                    CURRENT_TIMESTAMP,
                    :first_scheduled_at,
                    0
                    )
                ",
            )?
            .execute(named_params! {
                ":execution_id": execution_id.to_string(),
                ":is_top_level": execution_id.is_top_level(),
                ":corresponding_version": version.0,
                ":pending_expires_finished": scheduled_at,
                ":ffqn": ffqn.to_string(),
                ":state": STATE_PENDING_AT,
                ":created_at": created_at,
                ":component_id_input_digest": component_id.input_digest,
                ":first_scheduled_at": scheduled_at,
            })?;
            AppendNotifier {
                pending_at: Some(NotifierPendingAt {
                    scheduled_at,
                    ffqn,
                    component_input_digest: component_id.input_digest,
                }),
                execution_finished: None,
                response: None,
            }
        };
        let next_version = Version::new(version.0 + 1);
        Ok((next_version, pending_at))
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%execution_id, %scheduled_at, %corresponding_version))]
    fn update_state_pending_after_response_appended(
        tx: &Transaction,
        execution_id: &ExecutionId,
        scheduled_at: DateTime<Utc>,     // Changing to state PendingAt
        corresponding_version: &Version, // t_execution_log is not changed
        component_input_digest: InputContentDigest,
    ) -> Result<AppendNotifier, DbErrorWrite> {
        debug!("Setting t_state to Pending(`{scheduled_at:?}`) after response appended");
        let mut stmt = tx
            .prepare_cached(
                r"
                UPDATE t_state
                SET
                    corresponding_version = :corresponding_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,

                    last_lock_version = NULL,

                    join_set_id = NULL,
                    join_set_closing = NULL,

                    result_kind = NULL
                WHERE execution_id = :execution_id
            ",
            )
            .map_err(|err| DbErrorGeneric::Uncategorized {
                reason: err.to_string().into(),
                context: SpanTrace::capture(),
                source: Some(Arc::new(err)),
                loc: Location::caller(),
            })?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id,
                ":corresponding_version": corresponding_version.0,
                ":pending_expires_finished": scheduled_at,
                ":state": STATE_PENDING_AT,
            })
            .map_err(|err| DbErrorGeneric::Uncategorized {
                reason: err.to_string().into(),
                context: SpanTrace::capture(),
                source: Some(Arc::new(err)),
                loc: Location::caller(),
            })?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }
        Ok(AppendNotifier {
            pending_at: Some(NotifierPendingAt {
                scheduled_at,
                ffqn: Self::fetch_created_event(tx, execution_id)?.ffqn,
                component_input_digest,
            }),
            execution_finished: None,
            response: None,
        })
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%execution_id, %scheduled_at, %appending_version))]
    fn update_state_pending_after_event_appended(
        tx: &Transaction,
        execution_id: &ExecutionId,
        appending_version: &Version,
        scheduled_at: DateTime<Utc>, // Changing to state PendingAt
        intermittent_failure: bool,
        component_input_digest: InputContentDigest,
    ) -> Result<(AppendResponse, AppendNotifier), DbErrorWrite> {
        debug!("Setting t_state to Pending(`{scheduled_at:?}`) after event appended");
        // If response idx is unknown, use 0. This distinguishes the two paths.
        // JoinNext will always send an actual idx.
        let mut stmt = tx.prepare_cached(
            r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,
                    intermittent_event_count = intermittent_event_count + :intermittent_delta,

                    last_lock_version = NULL,

                    join_set_id = NULL,
                    join_set_closing = NULL,

                    result_kind = NULL
                WHERE execution_id = :execution_id;
            ",
        )?;
        let updated = stmt
            .execute(named_params! {
                ":execution_id": execution_id.to_string(),
                ":appending_version": appending_version.0,
                ":pending_expires_finished": scheduled_at,
                ":state": STATE_PENDING_AT,
                ":intermittent_delta": i32::from(intermittent_failure) // 0 or 1
            })
            .map_err(DbErrorWrite::from)?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }
        Ok((
            appending_version.increment(),
            AppendNotifier {
                pending_at: Some(NotifierPendingAt {
                    scheduled_at,
                    ffqn: Self::fetch_created_event(tx, execution_id)?.ffqn,
                    component_input_digest,
                }),
                execution_finished: None,
                response: None,
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
        retry_config: ComponentRetryConfig,
    ) -> Result<u32, DbErrorWrite> {
        debug!("Setting t_state to Locked(`{lock_expires_at:?}`)");
        let backoff_millis =
            i64::try_from(retry_config.retry_exp_backoff.as_millis()).map_err(|err| {
                DbErrorGeneric::Uncategorized {
                    reason: "backoff too big".into(),
                    context: SpanTrace::capture(),
                    source: Some(Arc::new(err)),
                    loc: Location::caller(),
                }
            })?; // Keep equal to Postgres' BIGINT = i64
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx.prepare_cached(
            r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,

                    max_retries = :max_retries,
                    retry_exp_backoff_millis = :retry_exp_backoff_millis,
                    last_lock_version = :appending_version,
                    executor_id = :executor_id,
                    run_id = :run_id,

                    join_set_id = NULL,
                    join_set_closing = NULL,

                    result_kind = NULL
                WHERE execution_id = :execution_id
            ",
        )?;
        let updated = stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":appending_version": appending_version.0,
            ":pending_expires_finished": lock_expires_at,
            ":state": STATE_LOCKED,
            ":max_retries": retry_config.max_retries,
            ":retry_exp_backoff_millis": backoff_millis,
            ":executor_id": executor_id.to_string(),
            ":run_id": run_id.to_string(),
        })?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }

        // fetch intermittent event count from the just-inserted row.
        let intermittent_event_count = tx
            .prepare(
                "SELECT intermittent_event_count FROM t_state WHERE execution_id = :execution_id",
            )?
            .query_row(
                named_params! {
                    ":execution_id": execution_id_str,
                },
                |row| {
                    let intermittent_event_count = row.get("intermittent_event_count")?;
                    Ok(intermittent_event_count)
                },
            )?;

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
        let mut stmt = tx.prepare_cached(
            r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,

                    last_lock_version = NULL,

                    join_set_id = :join_set_id,
                    join_set_closing = :join_set_closing,

                    result_kind = NULL
                WHERE execution_id = :execution_id
            ",
        )?;
        let updated = stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":appending_version": appending_version.0,
            ":pending_expires_finished": lock_expires_at,
            ":state": STATE_BLOCKED_BY_JOIN_SET,
            ":join_set_id": join_set_id,
            ":join_set_closing": join_set_closing,
        })?;
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
        let mut stmt = tx.prepare_cached(
            r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    pending_expires_finished = :pending_expires_finished,
                    state = :state,
                    updated_at = CURRENT_TIMESTAMP,

                    last_lock_version = NULL,
                    executor_id = NULL,
                    run_id = NULL,

                    join_set_id = NULL,
                    join_set_closing = NULL,

                    result_kind = :result_kind
                WHERE execution_id = :execution_id
            ",
        )?;

        let updated = stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":appending_version": appending_version.0,
            ":pending_expires_finished": finished_at,
            ":state": STATE_FINISHED,
            ":result_kind": JsonWrapper(result_kind),
        })?;
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
        let mut stmt = tx.prepare_cached(
            r"
                UPDATE t_state
                SET
                    corresponding_version = :appending_version,
                    updated_at = CURRENT_TIMESTAMP
                WHERE execution_id = :execution_id
            ",
        )?;
        let updated = stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":appending_version": appending_version.0,
        })?;
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
            let mut stmt = tx.prepare_cached(
                "INSERT INTO t_delay (execution_id, join_set_id, delay_id, expires_at) \
                VALUES \
                (:execution_id, :join_set_id, :delay_id, :expires_at)",
            )?;
            stmt.execute(named_params! {
                ":execution_id": execution_id_str,
                ":join_set_id": join_set_id.to_string(),
                ":delay_id": delay_id.to_string(),
                ":expires_at": expires_at,
            })?;
        }
        Ok(appending_version.increment())
    }

    fn get_combined_state(
        tx: &Transaction,
        execution_id: &ExecutionId,
    ) -> Result<CombinedState, DbErrorRead> {
        let mut stmt = tx.prepare(
            r"
                SELECT
                    created_at, first_scheduled_at,
                    state, ffqn, component_id_input_digest, corresponding_version, pending_expires_finished,
                    last_lock_version, executor_id, run_id,
                    join_set_id, join_set_closing,
                    result_kind
                    FROM t_state
                WHERE
                    execution_id = :execution_id
                ",
        )?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                let created_at: DateTime<Utc> = row.get("created_at")?;
                let first_scheduled_at: DateTime<Utc> = row.get("first_scheduled_at")?;
                let ffqn = row.get("ffqn")?;
                CombinedState::new(
                    CombinedStateDTO {
                        execution_id: execution_id.clone(),
                        created_at,
                        first_scheduled_at,
                        component_digest: row.get("component_id_input_digest")?,
                        state: row.get("state")?,
                        ffqn,
                        pending_expires_finished: row
                            .get::<_, DateTime<Utc>>("pending_expires_finished")?,
                        last_lock_version: row
                            .get::<_, Option<VersionType>>("last_lock_version")?
                            .map(Version::new),
                        executor_id: row.get::<_, Option<ExecutorId>>("executor_id")?,
                        run_id: row.get::<_, Option<RunId>>("run_id")?,
                        join_set_id: row.get::<_, Option<JoinSetId>>("join_set_id")?,
                        join_set_closing: row.get::<_, Option<bool>>("join_set_closing")?,
                        result_kind: row
                            .get::<_, Option<JsonWrapper<PendingStateFinishedResultKind>>>(
                                "result_kind",
                            )?
                            .map(|wrapper| wrapper.0),
                    },
                    Version::new(row.get("corresponding_version")?),
                )
            },
        )
        .map_err(DbErrorRead::from)
    }

    fn list_executions(
        read_tx: &Transaction,
        filter: &ListExecutionsFilter,
        pagination: &ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, RusqliteError> {
        #[derive(Debug)]
        struct StatementModifier<'a> {
            where_vec: Vec<String>,
            params: Vec<(&'static str, ToSqlOutput<'a>)>,
            limit: u32,
            limit_desc: bool,
        }

        fn paginate<'a, T: rusqlite::ToSql + 'static>(
            pagination: &'a Pagination<Option<T>>,
            column: &str,
            filter: &ListExecutionsFilter,
        ) -> Result<StatementModifier<'a>, RusqliteError> {
            let mut where_vec: Vec<String> = vec![];
            let mut params: Vec<(&'static str, ToSqlOutput<'a>)> = vec![];
            let limit = pagination.length();
            let limit_desc = pagination.is_desc();
            match pagination {
                Pagination::NewerThan {
                    cursor: Some(cursor),
                    ..
                }
                | Pagination::OlderThan {
                    cursor: Some(cursor),
                    ..
                } => {
                    where_vec.push(format!("{column} {rel} :cursor", rel = pagination.rel()));
                    let cursor = cursor.to_sql().map_err(|err| {
                        error!("Possible program error - cannot convert cursor to sql - {err:?}");
                        RusqliteError::Generic {
                            reason: "cannot convert cursor to sql".into(),
                            context: SpanTrace::capture(),
                            source: Some(Arc::new(err)),
                            loc: Location::caller(),
                        }
                    })?;
                    params.push((":cursor", cursor));
                }
                _ => {}
            }
            if !filter.show_derived {
                where_vec.push("is_top_level=true".to_string());
            }
            Ok(StatementModifier {
                where_vec,
                params,
                limit: u32::from(limit),
                limit_desc,
            })
        }

        let mut statement_mod = match pagination {
            ExecutionListPagination::CreatedBy(pagination) => {
                paginate(pagination, "created_at", filter)?
            }
            ExecutionListPagination::ExecutionId(pagination) => {
                paginate(pagination, "execution_id", filter)?
            }
        };

        let ffqn_temporary;
        if let Some(ffqn_prefix) = &filter.ffqn_prefix {
            statement_mod.where_vec.push("ffqn LIKE :ffqn".to_string());
            ffqn_temporary = format!("{ffqn_prefix}%"); // added %
            let ffqn = ffqn_temporary
                .to_sql()
                .expect("string conversion never fails");
            statement_mod.params.push((":ffqn", ffqn));
        }

        if filter.hide_finished {
            statement_mod
                .where_vec
                .push(format!("state != '{STATE_FINISHED}'"));
        }
        let prefix_temporary;
        if let Some(prefix) = &filter.execution_id_prefix {
            statement_mod
                .where_vec
                .push("execution_id LIKE :prefix".to_string());
            prefix_temporary = format!("{prefix}%"); // concat %
            statement_mod.params.push((
                ":prefix",
                prefix_temporary
                    .to_sql()
                    .expect("string conversion never fails"),
            ));
        }

        let where_str = if statement_mod.where_vec.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", statement_mod.where_vec.join(" AND "))
        };
        let sql = format!(
            r"
            SELECT created_at, first_scheduled_at, component_id_input_digest,
            state, execution_id, ffqn, corresponding_version, pending_expires_finished,
            last_lock_version, executor_id, run_id,
            join_set_id, join_set_closing,
            result_kind
            FROM t_state {where_str} ORDER BY created_at {desc} LIMIT {limit}
            ",
            desc = if statement_mod.limit_desc { "DESC" } else { "" },
            limit = statement_mod.limit,
        );
        let mut vec: Vec<_> = read_tx
            .prepare(&sql)?
            .query_map::<_, &[(&'static str, ToSqlOutput)], _>(
                statement_mod
                    .params
                    .into_iter()
                    .collect::<Vec<_>>()
                    .as_ref(),
                |row| {
                    let execution_id = row.get::<_, ExecutionId>("execution_id")?;
                    let component_digest: InputContentDigest =
                        row.get("component_id_input_digest")?;
                    let created_at = row.get("created_at")?;
                    let first_scheduled_at = row.get("first_scheduled_at")?;
                    let ffqn: FunctionFqn = row.get("ffqn")?;

                    let combined_state = CombinedState::new(
                        CombinedStateDTO {
                            execution_id,
                            created_at,
                            first_scheduled_at,
                            component_digest,
                            state: row.get("state")?,
                            ffqn,
                            pending_expires_finished: row
                                .get::<_, DateTime<Utc>>("pending_expires_finished")?,
                            executor_id: row.get::<_, Option<ExecutorId>>("executor_id")?,

                            last_lock_version: row
                                .get::<_, Option<VersionType>>("last_lock_version")?
                                .map(Version::new),
                            run_id: row.get::<_, Option<RunId>>("run_id")?,
                            join_set_id: row.get::<_, Option<JoinSetId>>("join_set_id")?,
                            join_set_closing: row.get::<_, Option<bool>>("join_set_closing")?,
                            result_kind: row
                                .get::<_, Option<JsonWrapper<PendingStateFinishedResultKind>>>(
                                    "result_kind",
                                )?
                                .map(|wrapper| wrapper.0),
                        },
                        Version::new(row.get("corresponding_version")?),
                    )?;
                    Ok(combined_state.execution_with_state)
                },
            )?
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

    fn list_responses(
        tx: &Transaction,
        execution_id: &ExecutionId,
        pagination: Option<Pagination<u32>>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorRead> {
        // TODO: Add test
        let mut params: Vec<(&'static str, Box<dyn rusqlite::ToSql>)> = vec![];
        let mut sql = "SELECT \
            r.id, r.created_at, r.join_set_id,  r.delay_id, r.delay_success, r.child_execution_id, r.finished_version, l.json_value \
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
        tx.prepare(&sql)?
            .query_map::<_, &[(&'static str, &dyn ToSql)], _>(
                params
                    .iter()
                    .map(|(key, value)| (*key, value.as_ref()))
                    .collect::<Vec<_>>()
                    .as_ref(),
                Self::parse_response_with_cursor,
            )?
            .collect::<Result<Vec<_>, rusqlite::Error>>()
            .map_err(DbErrorRead::from)
    }

    fn parse_response_with_cursor(
        row: &rusqlite::Row<'_>,
    ) -> Result<ResponseWithCursor, rusqlite::Error> {
        let id = row.get("id")?;
        let created_at: DateTime<Utc> = row.get("created_at")?;
        let join_set_id = row.get::<_, JoinSetId>("join_set_id")?;
        let event = match (
            row.get::<_, Option<DelayId>>("delay_id")?,
            row.get::<_, Option<bool>>("delay_success")?,
            row.get::<_, Option<ExecutionIdDerived>>("child_execution_id")?,
            row.get::<_, Option<VersionType>>("finished_version")?,
            row.get::<_, Option<JsonWrapper<ExecutionRequest>>>("json_value")?,
        ) {
            (Some(delay_id), Some(delay_success), None, None, None) => {
                JoinSetResponse::DelayFinished {
                    delay_id,
                    result: delay_success.then_some(()).ok_or(()),
                }
            }
            (
                None,
                None,
                Some(child_execution_id),
                Some(finished_version),
                Some(JsonWrapper(ExecutionRequest::Finished { result, .. })),
            ) => JoinSetResponse::ChildExecutionFinished {
                child_execution_id,
                finished_version: Version(finished_version),
                result,
            },
            (delay, delay_success, child, finished, result) => {
                error!(
                    "Invalid row in t_join_set_response {id} - {delay:?} {delay_success:?} {child:?} {finished:?} {:?}",
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

    #[instrument(level = Level::TRACE, skip(tx))]
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
        retry_config: ComponentRetryConfig,
    ) -> Result<LockedExecution, DbErrorWrite> {
        debug!("lock_single_execution");
        let combined_state = Self::get_combined_state(tx, execution_id)?;
        combined_state
            .execution_with_state
            .pending_state
            .can_append_lock(created_at, executor_id, run_id, lock_expires_at)?;
        let expected_version = combined_state.get_next_version_assert_not_finished();
        Self::check_expected_next_and_appending_version(&expected_version, appending_version)?;

        // Append to `execution_log` table.
        let locked_event = Locked {
            component_id,
            executor_id,
            lock_expires_at,
            run_id,
            retry_config,
        };
        let event = ExecutionRequest::Locked(locked_event.clone());
        let event_ser = serde_json::to_string(&event).map_err(|err| {
            warn!("Cannot serialize {event:?} - {err:?}");
            DbErrorWriteNonRetriable::ValidationFailed("parameter serialization error".into())
        })?;
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO t_execution_log \
            (execution_id, created_at, json_value, version, variant) \
            VALUES \
            (:execution_id, :created_at, :json_value, :version, :variant)",
            )
            .map_err(|err| DbErrorGeneric::Uncategorized {
                reason: err.to_string().into(),
                context: SpanTrace::capture(),
                source: Some(Arc::new(err)),
                loc: Location::caller(),
            })?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": created_at,
            ":json_value": event_ser,
            ":version": appending_version.0,
            ":variant": event.variant(),
        })
        .map_err(|err| {
            DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::IllegalState {
                reason: "cannot lock".into(),
                context: SpanTrace::capture(),
                source: Some(Arc::new(err)),
                loc: Location::caller(),
            })
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
            retry_config,
        )?;
        // Fetch event_history and `Created` event to construct the response.
        let mut events = tx
            .prepare(
                "SELECT json_value, version FROM t_execution_log WHERE \
                execution_id = :execution_id AND (variant = :variant1 OR variant = :variant2) \
                ORDER BY version",
            )?
            .query_map(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":variant1": DUMMY_CREATED.variant(),
                    ":variant2": DUMMY_HISTORY_EVENT.variant(),
                },
                |row| {
                    let created_at_fake = DateTime::from_timestamp_nanos(0); // not used, only the inner event and version
                    let event = row
                        .get::<_, JsonWrapper<ExecutionRequest>>("json_value")
                        .map_err(|serde| {
                            error!("Cannot deserialize {row:?} - {serde:?}");
                            consistency_rusqlite("cannot deserialize event")
                        })?
                        .0;
                    let version = Version(row.get("version")?);

                    Ok(ExecutionEvent {
                        created_at: created_at_fake,
                        event,
                        backtrace_id: None,
                        version,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<VecDeque<_>>();
        let Some(ExecutionRequest::Created {
            ffqn,
            params,
            parent,
            metadata,
            ..
        }) = events.pop_front().map(|outer| outer.event)
        else {
            return Err(consistency_db_err("execution log must contain `Created` event").into());
        };

        let event_history = events
            .into_iter()
            .map(|ExecutionEvent { event, version, .. }| {
                if let ExecutionRequest::HistoryEvent { event } = event {
                    Ok((event, version))
                } else {
                    Err(consistency_db_err(
                        "rows can only contain `Created` and `HistoryEvent` event kinds",
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(LockedExecution {
            execution_id: execution_id.clone(),
            metadata,
            next_version: appending_version.increment(),
            ffqn,
            params,
            event_history,
            responses,
            parent,
            intermittent_event_count,
            locked_event,
        })
    }

    fn count_join_next(
        tx: &Transaction,
        execution_id: &ExecutionId,
        join_set_id: &JoinSetId,
    ) -> Result<u32, DbErrorRead> {
        let mut stmt = tx.prepare(
            "SELECT COUNT(*) as count FROM t_execution_log WHERE execution_id = :execution_id AND join_set_id = :join_set_id \
            AND history_event_type = :join_next",
        )?;
        Ok(stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":join_set_id": join_set_id.to_string(),
                ":join_next": HISTORY_EVENT_TYPE_JOIN_NEXT,
            },
            |row| row.get::<_, u32>("count"),
        )?)
    }

    fn nth_response(
        tx: &Transaction,
        execution_id: &ExecutionId,
        join_set_id: &JoinSetId,
        skip_rows: u32,
    ) -> Result<Option<ResponseWithCursor>, DbErrorRead> {
        // TODO: Add test
        tx
            .prepare(
                "SELECT r.id, r.created_at, r.join_set_id, \
                    r.delay_id, r.delay_success, \
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
            ?
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":join_set_id": join_set_id.to_string(),
                    ":offset": skip_rows,
                },
                Self::parse_response_with_cursor,
            )
            .optional()
            .map_err(DbErrorRead::from)
    }

    #[instrument(level = Level::TRACE, skip_all, fields(%execution_id, %appending_version))]
    #[expect(clippy::needless_return)]
    fn append(
        tx: &Transaction,
        execution_id: &ExecutionId,
        req: AppendRequest,
        appending_version: Version,
    ) -> Result<(AppendResponse, AppendNotifier), DbErrorWrite> {
        if matches!(req.event, ExecutionRequest::Created { .. }) {
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::ValidationFailed(
                    "cannot append `Created` event - use `create` instead".into(),
                ),
            ));
        }
        if let AppendRequest {
            event:
                ExecutionRequest::Locked(Locked {
                    component_id,
                    executor_id,
                    run_id,
                    lock_expires_at,
                    retry_config,
                }),
            created_at,
        } = req
        {
            return Self::lock_single_execution(
                tx,
                created_at,
                component_id,
                execution_id,
                run_id,
                &appending_version,
                executor_id,
                lock_expires_at,
                retry_config,
            )
            .map(|locked_execution| (locked_execution.next_version, AppendNotifier::default()));
        }

        let combined_state = Self::get_combined_state(tx, execution_id)?;
        if combined_state
            .execution_with_state
            .pending_state
            .is_finished()
        {
            debug!("Execution is already finished");
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::IllegalState {
                    reason: "already finished".into(),
                    context: SpanTrace::capture(),
                    source: None,
                    loc: Location::caller(),
                },
            ));
        }

        Self::check_expected_next_and_appending_version(
            &combined_state.get_next_version_assert_not_finished(),
            &appending_version,
        )?;
        let event_ser = serde_json::to_string(&req.event).map_err(|err| {
            error!("Cannot serialize {:?} - {err:?}", req.event);
            DbErrorWriteNonRetriable::ValidationFailed("parameter serialization error".into())
        })?;

        let mut stmt = tx.prepare(
                    "INSERT INTO t_execution_log (execution_id, created_at, json_value, version, variant, join_set_id) \
                    VALUES (:execution_id, :created_at, :json_value, :version, :variant, :join_set_id)")
                    ?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": req.created_at,
            ":json_value": event_ser,
            ":version": appending_version.0,
            ":variant": req.event.variant(),
            ":join_set_id": req.event.join_set_id().map(std::string::ToString::to_string),
        })?;
        // Calculate current pending state

        match &req.event {
            ExecutionRequest::Created { .. } => {
                unreachable!("handled in the caller")
            }

            ExecutionRequest::Locked { .. } => {
                unreachable!("handled above")
            }

            ExecutionRequest::TemporarilyFailed {
                backoff_expires_at, ..
            }
            | ExecutionRequest::TemporarilyTimedOut {
                backoff_expires_at, ..
            } => {
                let (next_version, notifier) = Self::update_state_pending_after_event_appended(
                    tx,
                    execution_id,
                    &appending_version,
                    *backoff_expires_at,
                    true, // an intermittent failure
                    combined_state.execution_with_state.component_digest,
                )?;
                return Ok((next_version, notifier));
            }

            ExecutionRequest::Unlocked {
                backoff_expires_at, ..
            } => {
                let (next_version, notifier) = Self::update_state_pending_after_event_appended(
                    tx,
                    execution_id,
                    &appending_version,
                    *backoff_expires_at,
                    false, // not an intermittent failure
                    combined_state.execution_with_state.component_digest,
                )?;
                return Ok((next_version, notifier));
            }

            ExecutionRequest::Finished { result, .. } => {
                Self::update_state_finished(
                    tx,
                    execution_id,
                    &appending_version,
                    req.created_at,
                    PendingStateFinishedResultKind::from(result),
                )?;
                return Ok((
                    appending_version,
                    AppendNotifier {
                        pending_at: None,
                        execution_finished: Some(NotifierExecutionFinished {
                            execution_id: execution_id.clone(),
                            retval: result.clone(),
                        }),
                        response: None,
                    },
                ));
            }

            ExecutionRequest::HistoryEvent {
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
                    AppendNotifier::default(),
                ));
            }

            ExecutionRequest::HistoryEvent {
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
                    AppendNotifier::default(),
                ));
            }

            ExecutionRequest::HistoryEvent {
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
                    let (next_version, notifier) = Self::update_state_pending_after_event_appended(
                        tx,
                        execution_id,
                        &appending_version,
                        scheduled_at,
                        false, // not an intermittent failure
                        combined_state.execution_with_state.component_digest,
                    )?;
                    return Ok((next_version, notifier));
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
                    AppendNotifier::default(),
                ));
            }
        }
    }

    fn append_response(
        tx: &Transaction,
        execution_id: &ExecutionId,
        response_outer: JoinSetResponseEventOuter,
    ) -> Result<AppendNotifier, DbErrorWrite> {
        let mut stmt = tx.prepare(
            "INSERT INTO t_join_set_response (execution_id, created_at, join_set_id, delay_id, delay_success, child_execution_id, finished_version) \
                    VALUES (:execution_id, :created_at, :join_set_id, :delay_id, :delay_success, :child_execution_id, :finished_version)",
        )?;
        let join_set_id = &response_outer.event.join_set_id;
        let (delay_id, delay_success) = match &response_outer.event.event {
            JoinSetResponse::DelayFinished { delay_id, result } => {
                (Some(delay_id.to_string()), Some(result.is_ok()))
            }
            JoinSetResponse::ChildExecutionFinished { .. } => (None, None),
        };
        let (child_execution_id, finished_version) = match &response_outer.event.event {
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
            ":created_at": response_outer.created_at,
            ":join_set_id": join_set_id.to_string(),
            ":delay_id": delay_id,
            ":delay_success": delay_success,
            ":child_execution_id": child_execution_id,
            ":finished_version": finished_version,
        })?;

        // if the execution is going to be unblocked by this response...
        let combined_state = Self::get_combined_state(tx, execution_id)?;
        debug!("previous_pending_state: {combined_state:?}");
        let mut notifier = if let PendingState::BlockedByJoinSet {
            join_set_id: found_join_set_id,
            lock_expires_at, // Set to a future time if the worker is keeping the execution warm waiting for the result.
            closing: _,
        } = combined_state.execution_with_state.pending_state
            && *join_set_id == found_join_set_id
        {
            // PendingAt should be set to current time if called from expired_timers_watcher,
            // or to a future time if the execution is hot.
            let scheduled_at = max(lock_expires_at, response_outer.created_at);
            // TODO: Add diff test
            // Unblock the state.
            Self::update_state_pending_after_response_appended(
                tx,
                execution_id,
                scheduled_at,
                &combined_state.corresponding_version, // not changing the version
                combined_state.execution_with_state.component_digest,
            )?
        } else {
            AppendNotifier::default()
        };
        if let JoinSetResponseEvent {
            join_set_id,
            event:
                JoinSetResponse::DelayFinished {
                    delay_id,
                    result: _,
                },
        } = &response_outer.event
        {
            debug!(%join_set_id, %delay_id, "Deleting from `t_delay`");
            let mut stmt =
                tx.prepare_cached("DELETE FROM t_delay WHERE execution_id = :execution_id AND join_set_id = :join_set_id AND delay_id = :delay_id")
                ?;
            stmt.execute(named_params! {
                ":execution_id": execution_id.to_string(),
                ":join_set_id": join_set_id.to_string(),
                ":delay_id": delay_id.to_string(),
            })?;
        }
        notifier.response = Some((execution_id.clone(), response_outer));
        Ok(notifier)
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
            DbErrorWriteNonRetriable::ValidationFailed("cannot serialize backtrace".into())
        })?;
        let mut stmt = tx
            .prepare(
                "INSERT INTO t_backtrace (execution_id, component_id, version_min_including, version_max_excluding, wasm_backtrace) \
                    VALUES (:execution_id, :component_id, :version_min_including, :version_max_excluding, :wasm_backtrace)",
            )
            ?;
        stmt.execute(named_params! {
            ":execution_id": backtrace_info.execution_id.to_string(),
            ":component_id": JsonWrapper(&backtrace_info.component_id),
            ":version_min_including": backtrace_info.version_min_including.0,
            ":version_max_excluding": backtrace_info.version_max_excluding.0,
            ":wasm_backtrace": backtrace,
        })?;
        Ok(())
    }

    fn append_log(tx: &Transaction, row: &LogInfoAppendRow) -> Result<(), DbErrorWrite> {
        let mut stmt = tx.prepare(
            "INSERT INTO t_log (
            execution_id,
            run_id,
            created_at,
            level,
            message,
            stream_type,
            payload
        ) VALUES (
            :execution_id,
            :run_id,
            :created_at,
            :level,
            :message,
            :stream_type,
            :payload
        )",
        )?;

        match &row.log_info {
            LogInfo::Log {
                created_at,
                level,
                message,
            } => {
                stmt.execute(named_params! {
                    ":execution_id": row.execution_id,
                    ":run_id": row.run_id,
                    ":created_at": created_at,
                    ":level": *level as u8,
                    ":message": message,
                    ":stream_type": Option::<u8>::None,
                    ":payload": Option::<Vec<u8>>::None,
                })?;
            }
            LogInfo::Stream {
                created_at,
                payload,
                stream_type,
            } => {
                stmt.execute(named_params! {
                    ":execution_id": row.execution_id,
                    ":run_id": row.run_id,
                    ":created_at": created_at,
                    ":level": Option::<u8>::None,
                    ":message": Option::<String>::None,
                    ":stream_type": *stream_type as u8,
                    ":payload": payload,
                })?;
            }
        }

        Ok(())
    }

    #[cfg(feature = "test")]
    fn get(
        tx: &Transaction,
        execution_id: &ExecutionId,
    ) -> Result<concepts::storage::ExecutionLog, DbErrorRead> {
        let mut stmt = tx.prepare(
            "SELECT created_at, json_value, version FROM t_execution_log WHERE \
                        execution_id = :execution_id ORDER BY version",
        )?;
        let events = stmt
            .query_map(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| {
                    let created_at = row.get("created_at")?;
                    let event = row
                        .get::<_, JsonWrapper<ExecutionRequest>>("json_value")
                        .map_err(|serde| {
                            error!("Cannot deserialize {row:?} - {serde:?}");
                            consistency_rusqlite("cannot deserialize event")
                        })?
                        .0;
                    let version = Version(row.get("version")?);

                    Ok(ExecutionEvent {
                        created_at,
                        event,
                        backtrace_id: None,
                        version,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
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
            pending_state: combined_state.execution_with_state.pending_state,
            component_digest: combined_state.execution_with_state.component_digest,
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
                log.version as version,
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
                created_at, json_value, NULL as backtrace_id, version
            FROM t_execution_log WHERE
                execution_id = :execution_id AND version >= :version_min AND version < :version_max_excluding
            ORDER BY version"
        };
        tx.prepare(select)?
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
                    let version = Version(row.get("version")?);

                    let event = row
                        .get::<_, JsonWrapper<ExecutionRequest>>("json_value")
                        .map(|event| ExecutionEvent {
                            created_at,
                            event: event.0,
                            backtrace_id,
                            version,
                        })
                        .map_err(|serde| {
                            error!("Cannot deserialize {row:?} - {serde:?}");
                            consistency_rusqlite("cannot deserialize")
                        })?;
                    Ok(event)
                },
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbErrorRead::from)
    }

    fn get_execution_event(
        tx: &Transaction,
        execution_id: &ExecutionId,
        version: VersionType,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let mut stmt = tx.prepare(
            "SELECT created_at, json_value, version FROM t_execution_log WHERE \
                        execution_id = :execution_id AND version = :version",
        )?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":version": version,
            },
            |row| {
                let created_at = row.get("created_at")?;
                let event = row
                    .get::<_, JsonWrapper<ExecutionRequest>>("json_value")
                    .map_err(|serde| {
                        error!("Cannot deserialize {row:?} - {serde:?}");
                        consistency_rusqlite("cannot deserialize event")
                    })?;
                let version = Version(row.get("version")?);

                Ok(ExecutionEvent {
                    created_at,
                    event: event.0,
                    backtrace_id: None,
                    version,
                })
            },
        )
        .map_err(DbErrorRead::from)
    }

    fn get_last_execution_event(
        tx: &Transaction,
        execution_id: &ExecutionId,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let mut stmt = tx.prepare(
            "SELECT created_at, json_value, version FROM t_execution_log WHERE \
                        execution_id = :execution_id ORDER BY version DESC",
        )?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                let created_at = row.get("created_at")?;
                let event = row
                    .get::<_, JsonWrapper<ExecutionRequest>>("json_value")
                    .map_err(|serde| {
                        error!("Cannot deserialize {row:?} - {serde:?}");
                        consistency_rusqlite("cannot deserialize event")
                    })?;
                let version = Version(row.get("version")?);
                Ok(ExecutionEvent {
                    created_at,
                    event: event.0,
                    backtrace_id: None,
                    version: version.clone(),
                })
            },
        )
        .map_err(DbErrorRead::from)
    }

    fn delay_response(
        tx: &Transaction,
        execution_id: &ExecutionId,
        delay_id: &DelayId,
    ) -> Result<Option<bool>, DbErrorRead> {
        // TODO: Add test
        tx.prepare(
            "SELECT delay_success \
                    FROM t_join_set_response \
                    WHERE \
                    execution_id = :execution_id AND delay_id = :delay_id
                    ",
        )?
        .query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":delay_id": delay_id.to_string(),
            },
            |row| {
                let delay_success = row.get::<_, bool>("delay_success")?;
                Ok(delay_success)
            },
        )
        .optional()
        .map_err(DbErrorRead::from)
    }

    // TODO(perf): Instead of OFFSET an per-execution sequential ID could improve the read performance.
    #[instrument(level = Level::TRACE, skip_all)]
    fn get_responses_with_offset(
        tx: &Transaction,
        execution_id: &ExecutionId,
        skip_rows: u32,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbErrorRead> {
        // TODO: Add test
        tx.prepare(
            "SELECT r.id, r.created_at, r.join_set_id, \
            r.delay_id, r.delay_success, \
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
        ?
        .query_map(
            named_params! {
                ":execution_id": execution_id.to_string(),
                ":offset": skip_rows,
            },
            Self::parse_response_with_cursor,
        )
        ?
        .collect::<Result<Vec<_>, _>>()
        .map(|resp| resp.into_iter().map(|vec| vec.event).collect())
        .map_err(DbErrorRead::from)
    }

    fn get_pending_of_single_ffqn(
        mut stmt: CachedStatement,
        batch_size: u32,
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
                let execution_id = row.get::<_, ExecutionId>("execution_id")?;
                let next_version =
                    Version::new(row.get::<_, VersionType>("corresponding_version")?).increment();
                Ok((execution_id, next_version))
            },
        )
        .map_err(|err| {
            warn!("Ignoring consistency error {err:?}");
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            warn!("Ignoring consistency error {err:?}");
        })
    }

    /// Get executions and their next versions
    fn get_pending_by_ffqns(
        conn: &Connection,
        batch_size: u32,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: &[FunctionFqn],
    ) -> Result<Vec<(ExecutionId, Version)>, RusqliteError> {
        let batch_size = usize::try_from(batch_size).expect("16 bit systems are unsupported");
        let mut execution_ids_versions = Vec::with_capacity(batch_size);
        for ffqn in ffqns {
            // Select executions in PendingAt.
            let stmt = conn.prepare_cached(&format!(
                r#"
                    SELECT execution_id, corresponding_version FROM t_state WHERE
                    state = "{STATE_PENDING_AT}" AND
                    pending_expires_finished <= :pending_expires_finished AND ffqn = :ffqn
                    ORDER BY pending_expires_finished LIMIT :batch_size
                    "#
            ))?;

            if let Ok(execs_and_versions) = Self::get_pending_of_single_ffqn(
                stmt,
                u32::try_from(batch_size - execution_ids_versions.len())
                    .expect("u32 - anything must fit to u32"),
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

    fn get_pending_by_component_input_digest(
        conn: &Connection,
        batch_size: u32,
        pending_at_or_sooner: DateTime<Utc>,
        input_digest: &InputContentDigest,
    ) -> Result<Vec<(ExecutionId, Version)>, RusqliteError> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"
                SELECT execution_id, corresponding_version FROM t_state WHERE
                state = "{STATE_PENDING_AT}" AND
                pending_expires_finished <= :pending_expires_finished AND
                component_id_input_digest = :component_id_input_digest
                ORDER BY pending_expires_finished LIMIT :batch_size
                "#
        ))?;

        stmt.query_map(
            named_params! {
                ":pending_expires_finished": pending_at_or_sooner,
                ":component_id_input_digest": input_digest,
                ":batch_size": batch_size,
            },
            |row| {
                let execution_id = row.get::<_, ExecutionId>("execution_id")?;
                let next_version =
                    Version::new(row.get::<_, VersionType>("corresponding_version")?).increment();
                Ok((execution_id, next_version))
            },
        )?
        .collect::<Result<Vec<_>, _>>()
        .map_err(RusqliteError::from)
    }

    // Must be called after write transaction for a correct happens-before relationship.
    #[instrument(level = Level::TRACE, skip_all)]
    fn notify_all(&self, notifiers: Vec<AppendNotifier>, current_time: DateTime<Utc>) {
        let (pending_ats, finished_execs, responses) = {
            let (mut pending_ats, mut finished_execs, mut responses) =
                (Vec::new(), Vec::new(), Vec::new());
            for notifier in notifiers {
                if let Some(pending_at) = notifier.pending_at {
                    pending_ats.push(pending_at);
                }
                if let Some(finished) = notifier.execution_finished {
                    finished_execs.push(finished);
                }
                if let Some(response) = notifier.response {
                    responses.push(response);
                }
            }
            (pending_ats, finished_execs, responses)
        };

        // Notify pending_at subscribers.
        if !pending_ats.is_empty() {
            let guard = self.0.pending_subscribers.lock().unwrap();
            for pending_at in pending_ats {
                Self::notify_pending_locked(&pending_at, current_time, &guard);
            }
        }
        // Notify execution finished subscribers.
        // Every NotifierExecutionFinished value belongs to a different execution, since only `append(Finished)` can produce `NotifierExecutionFinished`.
        if !finished_execs.is_empty() {
            let mut guard = self.0.execution_finished_subscribers.lock().unwrap();
            for finished in finished_execs {
                if let Some(listeners_of_exe_id) = guard.remove(&finished.execution_id) {
                    for (_tag, sender) in listeners_of_exe_id {
                        // Sending while holding the lock but the oneshot sender does not block.
                        // If `wait_for_finished_result` happens after the append, it would receive the finished value instead.
                        let _ = sender.send(finished.retval.clone());
                    }
                }
            }
        }
        // Notify response subscribers.
        if !responses.is_empty() {
            let mut guard = self.0.response_subscribers.lock().unwrap();
            for (execution_id, response) in responses {
                if let Some((sender, _)) = guard.remove(&execution_id) {
                    let _ = sender.send(response);
                }
            }
        }
    }

    fn notify_pending_locked(
        notifier: &NotifierPendingAt,
        current_time: DateTime<Utc>,
        ffqn_to_pending_subscription: &std::sync::MutexGuard<PendingFfqnSubscribersHolder>,
    ) {
        // No need to remove here, cleanup is handled by the caller.
        if notifier.scheduled_at <= current_time {
            ffqn_to_pending_subscription.notify(notifier);
        }
    }

    fn upgrade_execution_component(
        tx: &Transaction,
        execution_id: &ExecutionId,
        old: &InputContentDigest,
        new: &InputContentDigest,
    ) -> Result<(), DbErrorWrite> {
        debug!("Updating t_state to component {new}");
        let mut stmt = tx.prepare_cached(
            r"
                UPDATE t_state
                SET
                    updated_at = CURRENT_TIMESTAMP,
                    component_id_input_digest = :new
                WHERE
                    execution_id = :execution_id AND
                    component_id_input_digest = :old
            ",
        )?;
        let updated = stmt.execute(named_params! {
            ":execution_id": execution_id,
            ":old": old,
            ":new": new,
        })?;
        if updated != 1 {
            return Err(DbErrorWrite::NotFound);
        }
        Ok(())
    }

    fn list_logs_tx(
        tx: &Transaction,
        execution_id: &ExecutionId,
        filter: &LogFilter,
        pagination: &Pagination<u32>,
    ) -> Result<Vec<LogInfoRow>, DbErrorRead> {
        let mut query = String::from(
            "SELECT id, run_id, created_at, level, message, stream_type, payload
         FROM t_log
         WHERE execution_id = :execution_id",
        );

        let length = pagination.length();
        let mut params = vec![
            (":execution_id", &execution_id as &dyn rusqlite::ToSql),
            (":cursor", pagination.cursor() as &dyn rusqlite::ToSql),
            (":length", &length as &dyn rusqlite::ToSql),
        ];

        if let Some(run_id) = &filter.run_id {
            query.push_str(" AND run_id = :run_id");
            params.push((":run_id", run_id));
        }

        // Filter by min_level (logs only)
        let min_level_temporary;
        if let Some(min_level) = filter.min_level {
            query.push_str(" AND level IS NOT NULL AND level >= :min_level");
            min_level_temporary = min_level as u8;
            params.push((":min_level", &min_level_temporary));
        }

        // Pagination
        write!(&mut query, " AND id {} :cursor", pagination.rel()).expect("writing to string");

        // Ordering
        query.push_str(" ORDER BY id ");
        query.push_str(if pagination.is_desc() { "DESC" } else { "ASC" });

        // Limit
        query.push_str(" LIMIT :length");

        let mut stmt = tx.prepare(&query)?;

        let logs = stmt
            .query_map(params.as_slice(), |row| {
                let cursor = row.get("id")?;
                let created_at: DateTime<Utc> = row.get("created_at")?;
                let run_id = row.get("run_id")?;
                let level: Option<u8> = row.get("level")?;
                let message: Option<String> = row.get("message")?;
                let stream_type: Option<u8> = row.get("stream_type")?;
                let payload: Option<Vec<u8>> = row.get("payload")?;

                let log_info = match (level, message, stream_type, payload) {
                    (Some(lvl), Some(msg), None, None) => LogInfo::Log {
                        created_at,
                        level: LogLevel::try_from(lvl).map_err(|_| {
                            consistency_rusqlite(format!(
                                "cannot convert {lvl} to LogLevel , id: {cursor}"
                            ))
                        })?,
                        message: msg,
                    },
                    (None, None, Some(stype), Some(pl)) => LogInfo::Stream {
                        created_at,
                        stream_type: LogStreamType::try_from(stype).map_err(|_| {
                            consistency_rusqlite(format!(
                                "cannot convert {stype} to LogStreamType , id: {cursor}"
                            ))
                        })?,
                        payload: pl,
                    },
                    _ => {
                        return Err(consistency_rusqlite(format!(
                            "invalid t_log row id:{cursor}"
                        )));
                    }
                };
                Ok(LogInfoRow {
                    cursor,
                    run_id,
                    log_info,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(logs)
    }
}

#[async_trait]
impl DbExecutor for SqlitePool {
    #[instrument(level = Level::TRACE, skip(self))]
    async fn lock_pending_by_ffqns(
        &self,
        batch_size: u32,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
        retry_config: ComponentRetryConfig,
    ) -> Result<LockPendingResponse, DbErrorGeneric> {
        let execution_ids_versions = self
            .transaction(
                move |conn| {
                    Self::get_pending_by_ffqns(conn, batch_size, pending_at_or_sooner, &ffqns)
                },
                "lock_pending_by_ffqns_get",
            )
            .await
            .map_err(to_generic_error)?;
        if execution_ids_versions.is_empty() {
            Ok(vec![])
        } else {
            debug!("Locking {execution_ids_versions:?}");
            self.transaction(
                move |tx| {
                    let mut locked_execs = Vec::with_capacity(execution_ids_versions.len());
                    // Append lock
                    for (execution_id, version) in &execution_ids_versions {
                        match Self::lock_single_execution(
                            tx,
                            created_at,
                            component_id.clone(),
                            execution_id,
                            run_id,
                            version,
                            executor_id,
                            lock_expires_at,
                            retry_config,
                        ) {
                            Ok(locked) => locked_execs.push(locked),
                            Err(err) => {
                                warn!("Locking row {execution_id} failed - {err:?}");
                            }
                        }
                    }
                    Ok(locked_execs)
                },
                "lock_pending_by_ffqns_one",
            )
            .await
            .map_err(to_generic_error)
        }
    }

    #[instrument(level = Level::TRACE, skip(self))]
    async fn lock_pending_by_component_id(
        &self,
        batch_size: u32,
        pending_at_or_sooner: DateTime<Utc>,
        component_id: &ComponentId,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
        retry_config: ComponentRetryConfig,
    ) -> Result<LockPendingResponse, DbErrorGeneric> {
        let component_id = component_id.clone();
        let execution_ids_versions = self
            .transaction(
                {
                    let component_id = component_id.clone();
                    move |conn| {
                        Self::get_pending_by_component_input_digest(
                            conn,
                            batch_size,
                            pending_at_or_sooner,
                            &component_id.input_digest,
                        )
                    }
                },
                "lock_pending_by_component_id_get",
            )
            .await
            .map_err(to_generic_error)?;
        if execution_ids_versions.is_empty() {
            Ok(vec![])
        } else {
            debug!("Locking {execution_ids_versions:?}");
            self.transaction(
                move |tx| {
                    let mut locked_execs = Vec::with_capacity(execution_ids_versions.len());
                    // Append lock
                    for (execution_id, version) in &execution_ids_versions {
                        match Self::lock_single_execution(
                            tx,
                            created_at,
                            component_id.clone(),
                            execution_id,
                            run_id,
                            version,
                            executor_id,
                            lock_expires_at,
                            retry_config,
                        ) {
                            Ok(locked) => locked_execs.push(locked),
                            Err(err) => {
                                warn!("Locking row {execution_id} failed - {err:?}");
                            }
                        }
                    }
                    Ok(locked_execs)
                },
                "lock_pending_by_component_id_one",
            )
            .await
            .map_err(to_generic_error)
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
        retry_config: ComponentRetryConfig,
    ) -> Result<LockedExecution, DbErrorWrite> {
        debug!(%execution_id, "lock_one");
        let execution_id = execution_id.clone();
        self.transaction(
            move |tx| {
                Self::lock_single_execution(
                    tx,
                    created_at,
                    component_id.clone(),
                    &execution_id,
                    run_id,
                    &version,
                    executor_id,
                    lock_expires_at,
                    retry_config,
                )
            },
            "lock_inner",
        )
        .await
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
        let created_at = req.created_at;
        let (version, notifier) = self
            .transaction(
                move |tx| Self::append(tx, &execution_id, req.clone(), version.clone()),
                "append",
            )
            .await?;
        self.notify_all(vec![notifier], created_at);
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_batch_respond_to_parent(
        &self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        current_time: DateTime<Utc>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        debug!("append_batch_respond_to_parent");
        if events.execution_id == response.parent_execution_id {
            // Pending state would be wrong.
            // This is not a panic because it depends on DB state.
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::ValidationFailed(
                    "Parameters `execution_id` and `parent_execution_id` cannot be the same".into(),
                ),
            ));
        }
        if events.batch.is_empty() {
            error!("Batch cannot be empty");
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::ValidationFailed("batch cannot be empty".into()),
            ));
        }
        let (version, notifiers) = {
            self.transaction(
                move |tx| {
                    let mut version = events.version.clone();
                    let mut notifier_of_child = None;
                    for append_request in &events.batch {
                        let (v, n) = Self::append(
                            tx,
                            &events.execution_id,
                            append_request.clone(),
                            version,
                        )?;
                        version = v;
                        notifier_of_child = Some(n);
                    }

                    let pending_at_parent = Self::append_response(
                        tx,
                        &response.parent_execution_id,
                        JoinSetResponseEventOuter {
                            created_at: response.created_at,
                            event: JoinSetResponseEvent {
                                join_set_id: response.join_set_id.clone(),
                                event: JoinSetResponse::ChildExecutionFinished {
                                    child_execution_id: response.child_execution_id.clone(),
                                    finished_version: response.finished_version.clone(),
                                    result: response.result.clone(),
                                },
                            },
                        },
                    )?;
                    Ok::<_, DbErrorWrite>((
                        version,
                        vec![
                            notifier_of_child.expect("checked that the batch is not empty"),
                            pending_at_parent,
                        ],
                    ))
                },
                "append_batch_respond_to_parent",
            )
            .await?
        };
        self.notify_all(notifiers, current_time);
        Ok(version)
    }

    // Supports only one subscriber (executor) per ffqn.
    // A new subscriber replaces the old one, which will eventually time out, which is fine.
    #[instrument(level = Level::TRACE, skip(self, timeout_fut))]
    async fn wait_for_pending_by_ffqn(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        let unique_tag: u64 = rand::random();
        let (sender, mut receiver) = mpsc::channel(1); // senders must use `try_send`
        {
            let mut pending_subscribers = self.0.pending_subscribers.lock().unwrap();
            for ffqn in ffqns.as_ref() {
                pending_subscribers.insert_ffqn(ffqn.clone(), (sender.clone(), unique_tag));
            }
        }
        async {
            let Ok(execution_ids_versions) = self
                .transaction(
                    {
                        let ffqns = ffqns.clone();
                        move |conn| Self::get_pending_by_ffqns(conn, 1, pending_at_or_sooner, ffqns.as_ref())
                    },
                    "get_pending_by_ffqns",
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
            let mut pending_subscribers = self.0.pending_subscribers.lock().unwrap();
            for ffqn in ffqns.as_ref() {
                match pending_subscribers.remove_ffqn(ffqn) {
                    Some((_, tag)) if tag == unique_tag => {
                        // Cleanup OK.
                    }
                    Some(other) => {
                        // Reinsert foreign sender.
                        pending_subscribers.insert_ffqn(ffqn.clone(), other);
                    }
                    None => {
                        // Value was replaced and cleaned up already.
                    }
                }
            }
        }
    }

    // Supports only one subscriber (executor) per component id.
    // A new subscriber replaces the old one, which will eventually time out, which is fine.
    #[instrument(level = Level::DEBUG, skip(self, timeout_fut))]
    async fn wait_for_pending_by_component_id(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        component_id: &ComponentId,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        let unique_tag: u64 = rand::random();
        let (sender, mut receiver) = mpsc::channel(1); // senders must use `try_send`
        {
            let mut pending_subscribers = self.0.pending_subscribers.lock().unwrap();
            pending_subscribers.insert_by_component(
                component_id.input_digest.clone(),
                (sender.clone(), unique_tag),
            );
        }
        async {
            let Ok(execution_ids_versions) = self
                .transaction(
                    {
                        let input_digest = component_id.input_digest.clone();
                        move |conn| Self::get_pending_by_component_input_digest(conn, 1, pending_at_or_sooner, &input_digest)
                    },
                    "get_pending_by_component_input_digest",
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
            let mut pending_subscribers = self.0.pending_subscribers.lock().unwrap();

            match pending_subscribers.remove_by_component(&component_id.input_digest) {
                Some((_, tag)) if tag == unique_tag => {
                    // Cleanup OK.
                }
                Some(other) => {
                    // Reinsert foreign sender.
                    pending_subscribers
                        .insert_by_component(component_id.input_digest.clone(), other);
                }
                None => {
                    // Value was replaced and cleaned up already.
                }
            }
        }
    }

    async fn get_last_execution_event(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let execution_id = execution_id.clone();
        self.transaction(
            move |tx| Self::get_last_execution_event(tx, &execution_id),
            "get_last_execution_event",
        )
        .await
    }
}

#[async_trait]
impl DbExternalApi for SqlitePool {
    #[instrument(skip(self))]
    async fn get_backtrace(
        &self,
        execution_id: &ExecutionId,
        filter: BacktraceFilter,
    ) -> Result<BacktraceInfo, DbErrorRead> {
        debug!("get_backtrace");
        let execution_id = execution_id.clone();

        self.transaction(
            move |tx| {
                let select = "SELECT component_id, version_min_including, version_max_excluding, wasm_backtrace FROM t_backtrace \
                                WHERE execution_id = :execution_id";
                let mut params: Vec<(&'static str, Box<dyn rusqlite::ToSql>)> = vec![(":execution_id", Box::new(execution_id.to_string()))];
                let select = match &filter {
                    BacktraceFilter::Specific(version) =>{
                        params.push((":version", Box::new(version.0)));
                        format!("{select} AND version_min_including <= :version AND version_max_excluding > :version")
                    },
                    BacktraceFilter::First => format!("{select} ORDER BY version_min_including LIMIT 1"),
                    BacktraceFilter::Last => format!("{select} ORDER BY version_min_including DESC LIMIT 1")
                };
                tx
                    .prepare(&select)
                    ?
                    .query_row::<_, &[(&'static str, &dyn ToSql)], _>(
                        params
                            .iter()
                            .map(|(key, value)| (*key, value.as_ref()))
                            .collect::<Vec<_>>()
                            .as_ref(),
                    |row| {
                        Ok(BacktraceInfo {
                            execution_id: execution_id.clone(),
                            component_id: row.get::<_, JsonWrapper<_> >("component_id")?.0,
                            version_min_including: Version::new(row.get::<_, VersionType>("version_min_including")?),
                            version_max_excluding: Version::new(row.get::<_, VersionType>("version_max_excluding")?),
                            wasm_backtrace: row.get::<_, JsonWrapper<_>>("wasm_backtrace")?.0,
                        })
                    },
                ).map_err(DbErrorRead::from)
            },
            "get_last_backtrace",
        ).await
    }

    #[instrument(skip(self))]
    async fn list_executions(
        &self,
        filter: ListExecutionsFilter,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbErrorGeneric> {
        self.transaction(
            move |tx| Self::list_executions(tx, &filter, &pagination),
            "list_executions",
        )
        .await
        .map_err(to_generic_error)
    }

    #[instrument(skip(self))]
    async fn list_execution_events(
        &self,
        execution_id: &ExecutionId,
        since: &Version,
        max_length: VersionType,
        include_backtrace_id: bool,
    ) -> Result<Vec<ExecutionEvent>, DbErrorRead> {
        let execution_id = execution_id.clone();
        let since = since.0;
        self.transaction(
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
        .await
    }

    #[instrument(skip(self))]
    async fn list_responses(
        &self,
        execution_id: &ExecutionId,
        pagination: Pagination<u32>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorRead> {
        let execution_id = execution_id.clone();
        self.transaction(
            move |tx| Self::list_responses(tx, &execution_id, Some(pagination)),
            "list_responses",
        )
        .await
    }

    #[instrument(skip(self))]
    async fn list_execution_events_responses(
        &self,
        execution_id: &ExecutionId,
        req_since: &Version,
        req_max_length: VersionType,
        req_include_backtrace_id: bool,
        resp_pagination: Pagination<u32>,
    ) -> Result<ExecutionWithStateRequestsResponses, DbErrorRead> {
        let execution_id = execution_id.clone();
        let req_since = req_since.0;
        self.transaction(
            move |tx| {
                let combined_state = Self::get_combined_state(tx, &execution_id)?;
                let events = Self::list_execution_events(
                    tx,
                    &execution_id,
                    req_since,
                    req_since + req_max_length,
                    req_include_backtrace_id,
                )?;
                let responses = Self::list_responses(tx, &execution_id, Some(resp_pagination))?;
                Ok(ExecutionWithStateRequestsResponses {
                    execution_with_state: combined_state.execution_with_state,
                    events,
                    responses,
                })
            },
            "list_execution_events_responses",
        )
        .await
    }

    #[instrument(skip(self))]
    async fn upgrade_execution_component(
        &self,
        execution_id: &ExecutionId,
        old: &InputContentDigest,
        new: &InputContentDigest,
    ) -> Result<(), DbErrorWrite> {
        let execution_id = execution_id.clone();
        let old = old.clone();
        let new = new.clone();
        self.transaction(
            move |tx| Self::upgrade_execution_component(tx, &execution_id, &old, &new),
            "upgrade_execution_component",
        )
        .await
    }

    #[instrument(skip(self))]
    async fn list_logs(
        &self,
        execution_id: &ExecutionId,
        filter: LogFilter,
        pagination: Pagination<u32>,
    ) -> Result<Vec<LogInfoRow>, DbErrorRead> {
        let execution_id = execution_id.clone();
        self.transaction(
            move |tx| Self::list_logs_tx(tx, &execution_id, &filter, &pagination),
            "list_logs",
        )
        .await
    }
}

#[async_trait]
impl DbConnection for SqlitePool {
    #[instrument(level = Level::DEBUG, skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbErrorWrite> {
        debug!("create");
        trace!(?req, "create");
        let created_at = req.created_at;
        let (version, notifier) = self
            .transaction(move |tx| Self::create_inner(tx, req.clone()), "create")
            .await?;
        self.notify_all(vec![notifier], created_at);
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

        let (version, notifier) = self
            .transaction(
                move |tx| {
                    let mut version = version.clone();
                    let mut notifier = None;
                    for append_request in &batch {
                        let (v, n) =
                            Self::append(tx, &execution_id, append_request.clone(), version)?;
                        version = v;
                        notifier = Some(n);
                    }
                    Ok::<_, DbErrorWrite>((
                        version,
                        notifier.expect("checked that the batch is not empty"),
                    ))
                },
                "append_batch",
            )
            .await?;

        self.notify_all(vec![notifier], current_time);
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

        let (version, notifiers) = self
            .transaction(
                move |tx| {
                    let mut notifier = None;
                    let mut version = version.clone();
                    for append_request in &batch {
                        let (v, n) =
                            Self::append(tx, &execution_id, append_request.clone(), version)?;
                        version = v;
                        notifier = Some(n);
                    }
                    let mut notifiers = Vec::new();
                    notifiers.push(notifier.expect("checked that the batch is not empty"));

                    for child_req in &child_req {
                        let (_, notifier) = Self::create_inner(tx, child_req.clone())?;
                        notifiers.push(notifier);
                    }
                    Ok::<_, DbErrorWrite>((version, notifiers))
                },
                "append_batch_create_new_execution_inner",
            )
            .await?;
        self.notify_all(notifiers, current_time);
        Ok(version)
    }

    // Supports only one subscriber per execution id.
    // A new call will overwrite the old subscriber, the old one will end
    // with a timeout, which is fine.
    #[instrument(level = Level::DEBUG, skip(self, timeout_fut))]
    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: u32,
        timeout_fut: Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>,
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
            self.transaction(
                move |tx| {
                    let responses = Self::get_responses_with_offset(tx, &execution_id, start_idx)?;
                    if responses.is_empty() {
                        // cannot race as we have the transaction write lock
                        let (sender, receiver) = oneshot::channel();
                        response_subscribers
                            .lock()
                            .unwrap()
                            .insert(execution_id.clone(), (sender, unique_tag));
                        Ok::<_, DbErrorReadWithTimeout>(itertools::Either::Right(receiver))
                    } else {
                        Ok(itertools::Either::Left(responses))
                    }
                },
                "subscribe_to_next_responses",
            )
            .await
        }
        .inspect_err(|_| {
            cleanup();
        })?;
        match resp_or_receiver {
            itertools::Either::Left(resp) => Ok(resp), // no need for cleanup
            itertools::Either::Right(receiver) => {
                let res = tokio::select! {
                    resp = receiver => {
                        match resp {
                            Ok(resp) => Ok(vec![resp]),
                            Err(_) => Err(DbErrorReadWithTimeout::from(DbErrorGeneric::Close)),
                        }
                    }
                    outcome = timeout_fut => Err(DbErrorReadWithTimeout::Timeout(outcome)),
                };
                cleanup();
                res
            }
        }
    }

    // Supports multiple subscribers.
    #[instrument(level = Level::DEBUG, skip(self, timeout_fut))]
    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout_fut: Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>>,
    ) -> Result<SupportedFunctionReturnValue, DbErrorReadWithTimeout> {
        let unique_tag: u64 = rand::random();
        let execution_id = execution_id.clone();
        let execution_finished_subscription = self.0.execution_finished_subscribers.clone();

        let cleanup = || {
            let mut guard = self.0.execution_finished_subscribers.lock().unwrap();
            if let Some(subscribers) = guard.get_mut(&execution_id) {
                subscribers.remove(&unique_tag);
            }
        };

        let resp_or_receiver = {
            let execution_id = execution_id.clone();
            self.transaction(move |tx| {
                let pending_state =
                    Self::get_combined_state(tx, &execution_id)?.execution_with_state.pending_state;
                if let PendingState::Finished { finished, .. } = pending_state {
                    let event =
                        Self::get_execution_event(tx, &execution_id, finished.version)?;
                    if let ExecutionRequest::Finished { result, ..} = event.event {
                        Ok(itertools::Either::Left(result))
                    } else {
                        error!("Mismatch, expected Finished row: {event:?} based on t_state {finished}");
                        Err(DbErrorReadWithTimeout::from(consistency_db_err(
                            "cannot get finished event based on t_state version"
                        )))
                    }
                } else {
                    // Cannot race with the notifier as we have the transaction write lock:
                    // Either the finished event was appended previously, thus `itertools::Either::Left` was selected,
                    // or we end up here. If this tx fails, the cleanup will remove this entry.
                    let (sender, receiver) = oneshot::channel();
                    let mut guard = execution_finished_subscription.lock().unwrap();
                    guard.entry(execution_id.clone()).or_default().insert(unique_tag, sender);
                    Ok(itertools::Either::Right(receiver))
                }
            }, "wait_for_finished_result")
            .await
        }
        .inspect_err(|_| {
            // This cleanup can race with the notification sender, since both are running after a transaction was finished.
            // If the notification sender wins, it removes our oneshot sender and puts a value in it, cleanup will not find the unique tag.
            // If cleanup wins, it simply removes the oneshot sender.
            cleanup();
        })?;

        let timeout_fut = timeout_fut.unwrap_or_else(|| Box::pin(std::future::pending()));
        match resp_or_receiver {
            itertools::Either::Left(resp) => Ok(resp), // no need for cleanup
            itertools::Either::Right(receiver) => {
                let res = tokio::select! {
                    resp = receiver => {
                        match resp {
                            Ok(retval) => Ok(retval),
                            Err(_recv_err) => Err(DbErrorGeneric::Close.into())
                        }
                    }
                    outcome = timeout_fut => Err(DbErrorReadWithTimeout::Timeout(outcome)),
                };
                cleanup();
                res
            }
        }
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%join_set_id, %execution_id))]
    async fn append_delay_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        delay_id: DelayId,
        result: Result<(), ()>,
    ) -> Result<AppendDelayResponseOutcome, DbErrorWrite> {
        debug!("append_delay_response");
        let event = JoinSetResponseEventOuter {
            created_at,
            event: JoinSetResponseEvent {
                join_set_id,
                event: JoinSetResponse::DelayFinished {
                    delay_id: delay_id.clone(),
                    result,
                },
            },
        };
        let res = self
            .transaction(
                {
                    let execution_id = execution_id.clone();
                    move |tx| Self::append_response(tx, &execution_id, event.clone())
                },
                "append_delay_response",
            )
            .await;
        match res {
            Ok(notifier) => {
                self.notify_all(vec![notifier], created_at);
                Ok(AppendDelayResponseOutcome::Success)
            }
            Err(DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict)) => {
                let delay_success = self
                    .transaction(
                        move |tx| Self::delay_response(tx, &execution_id, &delay_id),
                        "delay_response",
                    )
                    .await?;
                match delay_success {
                    Some(true) => Ok(AppendDelayResponseOutcome::AlreadyFinished),
                    Some(false) => Ok(AppendDelayResponseOutcome::AlreadyCancelled),
                    None => Err(DbErrorWrite::Generic(DbErrorGeneric::Uncategorized {
                        reason: "insert failed yet select did not find the response".into(),
                        context: SpanTrace::capture(),
                        source: None,
                        loc: Location::caller(),
                    })),
                }
            }
            Err(err) => Err(err),
        }
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_backtrace(&self, append: BacktraceInfo) -> Result<(), DbErrorWrite> {
        trace!("append_backtrace");
        self.transaction(
            move |tx| Self::append_backtrace(tx, &append),
            "append_backtrace",
        )
        .await
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_backtrace_batch(&self, batch: Vec<BacktraceInfo>) -> Result<(), DbErrorWrite> {
        trace!("append_backtrace_batch");
        self.transaction(
            move |tx| {
                for append in &batch {
                    Self::append_backtrace(tx, append)?;
                }
                Ok(())
            },
            "append_backtrace_batch",
        )
        .await
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_log(&self, row: LogInfoAppendRow) -> Result<(), DbErrorWrite> {
        trace!("append_log");
        self.transaction(move |tx| Self::append_log(tx, &row), "append_log")
            .await
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn append_log_batch(&self, batch: &[LogInfoAppendRow]) -> Result<(), DbErrorWrite> {
        trace!("append_log_batch");
        let batch = Vec::from(batch);
        self.transaction(
            move |tx| {
                for row in &batch {
                    Self::append_log(tx, &row)?;
                }
                Ok(())
            },
            "append_log_batch",
        )
        .await
    }

    /// Get currently expired delays and locks.
    #[instrument(level = Level::TRACE, skip(self))]
    async fn get_expired_timers(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbErrorGeneric> {
        self.transaction(
            move |conn| {
                let mut expired_timers = conn.prepare(
                    "SELECT execution_id, join_set_id, delay_id FROM t_delay WHERE expires_at <= :at",
                )?
                .query_map(
                        named_params! {
                            ":at": at,
                        },
                        |row| {
                            let execution_id = row.get("execution_id")?;
                            let join_set_id = row.get::<_, JoinSetId>("join_set_id")?;
                            let delay_id = row.get::<_, DelayId>("delay_id")?;
                            let delay = ExpiredDelay { execution_id, join_set_id, delay_id };
                            Ok(ExpiredTimer::Delay(delay))
                        },
                    )?
                    .collect::<Result<Vec<_>, _>>()?;
                // Extend with expired locks
                let expired = conn.prepare(&format!(r#"
                    SELECT execution_id, last_lock_version, corresponding_version, intermittent_event_count, max_retries, retry_exp_backoff_millis,
                    executor_id, run_id
                    FROM t_state
                    WHERE pending_expires_finished <= :at AND state = "{STATE_LOCKED}"
                    "#
                )
                )?
                .query_map(
                        named_params! {
                            ":at": at,
                        },
                        |row| {
                            let execution_id = row.get("execution_id")?;
                            let locked_at_version = Version::new(row.get("last_lock_version")?);
                            let next_version = Version::new(row.get("corresponding_version")?).increment();
                            let intermittent_event_count = row.get("intermittent_event_count")?;
                            let max_retries = row.get("max_retries")?;
                            let retry_exp_backoff_millis = u64::from(row.get::<_, u32>("retry_exp_backoff_millis")?);
                            let executor_id = row.get("executor_id")?;
                            let run_id = row.get("run_id")?;
                            let lock = ExpiredLock {
                                execution_id,
                                locked_at_version,
                                next_version,
                                intermittent_event_count,
                                max_retries,
                                retry_exp_backoff: Duration::from_millis(retry_exp_backoff_millis),
                                locked_by: LockedBy { executor_id, run_id },
                            };
                            Ok(ExpiredTimer::Lock(lock))
                        }
                    )?
                    .collect::<Result<Vec<_>, _>>()?;
                expired_timers.extend(expired);
                if !expired_timers.is_empty() {
                    debug!("get_expired_timers found {expired_timers:?}");
                }
                Ok(expired_timers)
            }, "get_expired_timers"
        )
        .await
        .map_err(to_generic_error)
    }

    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let version = version.0;
        let execution_id = execution_id.clone();
        self.transaction(
            move |tx| Self::get_execution_event(tx, &execution_id, version),
            "get_execution_event",
        )
        .await
    }

    async fn get_pending_state(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<ExecutionWithState, DbErrorRead> {
        let execution_id = execution_id.clone();
        Ok(self
            .transaction(
                move |tx| Self::get_combined_state(tx, &execution_id),
                "get_pending_state",
            )
            .await?
            .execution_with_state)
    }
}

#[cfg(feature = "test")]
#[async_trait]
impl concepts::storage::DbConnectionTest for SqlitePool {
    #[instrument(level = Level::DEBUG, skip(self, response_event), fields(join_set_id = %response_event.join_set_id))]
    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbErrorWrite> {
        debug!("append_response");
        let event = JoinSetResponseEventOuter {
            created_at,
            event: response_event,
        };
        let notifier = self
            .transaction(
                move |tx| Self::append_response(tx, &execution_id, event.clone()),
                "append_response",
            )
            .await?;
        self.notify_all(vec![notifier], created_at);
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip(self))]
    async fn get(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<concepts::storage::ExecutionLog, DbErrorRead> {
        trace!("get");
        let execution_id = execution_id.clone();
        self.transaction(move |tx| Self::get(tx, &execution_id), "get")
            .await
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
