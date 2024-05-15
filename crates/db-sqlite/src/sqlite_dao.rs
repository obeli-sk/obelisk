#![allow(clippy::all, dead_code)]
use async_sqlite::{rusqlite::named_params, ClientBuilder, JournalMode, Pool, PoolBuilder};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{DelayId, ExecutorId, JoinSetId, PrefixedUlid, RunId},
    storage::{
        AppendBatchResponse, AppendRequest, AppendResponse, CreateRequest, DbConnection, DbError,
        DbPool, ExecutionEvent, ExecutionEventInner, ExecutionLog, ExpiredTimer, HistoryEvent,
        JoinSetRequest, JoinSetResponse, JoinSetResponseEvent, JoinSetResponseEventOuter,
        LockPendingResponse, LockResponse, LockedExecution, PendingState, SpecificError, Version,
        DUMMY_CREATED, DUMMY_HISTORY_EVENT, DUMMY_INTERMITTENT_FAILURE, DUMMY_INTERMITTENT_TIMEOUT,
    },
    ExecutionId, FunctionFqn, StrVariant,
};
use rusqlite::{
    types::{FromSql, FromSqlError},
    Connection, OptionalExtension, Transaction,
};
use std::{cmp::max, collections::VecDeque, path::Path, sync::Arc, time::Duration};
use tracing::{debug, error, info, instrument, trace, warn, Span};

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
PRAGMA cache_size = 1000000000;
";

// TODO metadata table with current schema version, migrations

/// Stores execution history.
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
"; // TODO: index and order by created at

/// Stores executions in state: `Pending`, `PendingAt`, `Locked`, and delay requests.
/// State to column mapping:
/// `PendingAt`:  `ffqn`
/// `Locked`:                `executor_id`, `run_id`, `intermittent_event_count`, `max_retries`, `retry_exp_backoff_millis`, Option<`parent_execution_id`>, Option<`parent_join_set_id`>
/// `BlockedByJoinSet`:      `join_set_id`
const CREATE_TABLE_T_STATE: &str = r"
CREATE TABLE IF NOT EXISTS t_state (
    execution_id TEXT NOT NULL,
    next_version INTEGER NOT NULL,
    pending_or_expires_at TEXT NOT NULL,

    ffqn TEXT,
    join_set_id TEXT,
    executor_id TEXT,
    run_id TEXT,
    intermittent_event_count INTEGER,
    max_retries INTEGER,
    retry_exp_backoff_millis INTEGER,
    parent_execution_id TEXT,
    parent_join_set_id TEXT,
    PRIMARY KEY (execution_id, join_set_id)
)
"; // TODO: index by `pending_at` + `ffqn`

/// Stores delay requests.
/// State to column mapping:
const CREATE_TABLE_T_DELAY: &str = r"
CREATE TABLE IF NOT EXISTS t_delay (
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,
    delay_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (execution_id, join_set_id, delay_id)
)
";

#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    #[error(transparent)]
    Sqlite(#[from] async_sqlite::Error),
    #[error("parsing error - `{0}`")]
    Parsing(StrVariant),
    #[error(transparent)]
    DbError(#[from] DbError),
}

impl From<SqliteError> for DbError {
    fn from(err: SqliteError) -> Self {
        if let SqliteError::DbError(err) = err {
            err
        } else {
            error!(backtrace = %std::backtrace::Backtrace::capture(), "Validation failed - {err:?}");
            DbError::Specific(SpecificError::ValidationFailed(StrVariant::Arc(Arc::from(
                err.to_string(),
            ))))
        }
    }
}

impl From<rusqlite::Error> for SqliteError {
    fn from(value: rusqlite::Error) -> Self {
        Self::from(async_sqlite::Error::from(value))
    }
}

#[derive(Clone)]
pub struct SqlitePool {
    pool: Pool,
}

struct PrefixedUlidWrapper<T: 'static>(PrefixedUlid<T>);
type ExecutorIdW = PrefixedUlidWrapper<concepts::prefixed_ulid::prefix::Exr>;
type ConfigIdW = PrefixedUlidWrapper<concepts::prefixed_ulid::prefix::Conf>;
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

#[async_trait]
impl DbPool<SqlitePool> for SqlitePool {
    fn connection(&self) -> SqlitePool {
        self.clone()
    }

    async fn close(&self) -> Result<(), StrVariant> {
        self.pool
            .close()
            .await
            .map_err(|err| StrVariant::Arc(Arc::from(err.to_string())))
    }
}

impl SqlitePool {
    #[instrument(skip_all)]
    async fn init(pool: &Pool) -> Result<(), SqliteError> {
        pool.conn_with_err_and_span(
            |conn| {
                trace!("Executing `PRAGMA`");
                conn.execute(PRAGMA, [])?;
                trace!("Executing `CREATE_TABLE_T_EXECUTION_LOG`");
                conn.execute(CREATE_TABLE_T_EXECUTION_LOG, [])?;
                trace!("Executing `CREATE_TABLE_T_JOIN_SET_RESPONSE`");
                conn.execute(CREATE_TABLE_T_JOIN_SET_RESPONSE, [])?;
                trace!("Executing `CREATE_TABLE_T_STATE`");
                conn.execute(CREATE_TABLE_T_STATE, [])?;
                trace!("Executing `CREATE_TABLE_T_DELAY`");
                conn.execute(CREATE_TABLE_T_DELAY, [])?;
                info!("Done setting up sqlite");
                Ok::<_, SqliteError>(())
            },
            Span::current(),
        )
        .await?;
        Ok(())
    }

    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, SqliteError> {
        // Work around a race condition when creating a new database file returns "Database Busy" on one of the threads.
        // https://github.com/ryanfowler/async-sqlite/issues/10
        let client = ClientBuilder::new()
            .path(&path)
            .journal_mode(JournalMode::Wal)
            .open()
            .await?;
        client.close().await?;
        let pool = PoolBuilder::new()
            .path(path)
            .journal_mode(JournalMode::Wal)
            .open()
            .await?;
        Self::init(&pool).await?;
        Ok(Self { pool })
    }

    fn fetch_created_event(
        conn: &Connection,
        execution_id: ExecutionId,
    ) -> Result<CreateRequest, SqliteError> {
        let mut stmt = conn.prepare(
            "SELECT created_at, json_value FROM t_execution_log WHERE \
            execution_id = :execution_id AND (variant = :variant)",
        )?;
        let (created_at, event) = stmt.query_row(
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
                    SqliteError::Parsing(StrVariant::Static("cannot deserialize"))
                });
                Ok(event)
            },
        )??;
        if let ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
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
            })
        } else {
            error!("Cannt match `Created` event - {event:?}");
            Err(SqliteError::Parsing(StrVariant::Static(
                "Cannot deserialize `Created` event",
            )))
        }
    }

    fn count_intermittent_events(
        conn: &Connection,
        execution_id: ExecutionId,
    ) -> Result<u32, SqliteError> {
        let mut stmt = conn.prepare(
            "SELECT COUNT(*) as count FROM t_execution_log WHERE execution_id = :execution_id AND (variant = :v1 OR variant = :v2)",
        )?;
        Ok(stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":v1": DUMMY_INTERMITTENT_TIMEOUT.variant(),
                    ":v2": DUMMY_INTERMITTENT_FAILURE.variant(),
                },
                |row| row.get("count"),
            )
            .map_err(async_sqlite::Error::Rusqlite)?)
    }

    fn get_current_version(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<Version, SqliteError> {
        let mut stmt = tx.prepare(
            "SELECT version FROM t_execution_log WHERE execution_id = :execution_id ORDER BY version DESC LIMIT 1",
        )?;
        Ok(stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| {
                    let version: usize = row.get("version")?;
                    Ok(Version::new(version))
                },
            )
            .map_err(async_sqlite::Error::Rusqlite)?)
    }

    fn get_next_version(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<Version, SqliteError> {
        Self::get_current_version(tx, execution_id).map(|ver| Version::new(ver.0 + 1))
    }

    fn check_expected_next_and_appending_version(
        expected_version: &Version,
        appending_version: &Version,
    ) -> Result<(), SqliteError> {
        if *expected_version != *appending_version {
            return Err(SqliteError::DbError(DbError::Specific(
                SpecificError::VersionMismatch {
                    appending_version: appending_version.clone(),
                    expected_version: expected_version.clone(),
                },
            )));
        }
        Ok(())
    }

    fn check_next_version(
        tx: &Transaction,
        execution_id: ExecutionId,
        appending_version: &Version,
    ) -> Result<(), SqliteError> {
        let expected_version = Self::get_next_version(tx, execution_id)?;
        Self::check_expected_next_and_appending_version(&expected_version, appending_version)
    }

    #[instrument(skip_all, fields(execution_id = %req.execution_id))]
    fn create_inner(tx: &Transaction, req: CreateRequest) -> Result<AppendResponse, SqliteError> {
        debug!("create_inner");
        let version = Version::new(0);
        let execution_id = req.execution_id;
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx.prepare(
                "INSERT INTO t_execution_log (execution_id, created_at, version, json_value, variant, join_set_id ) \
                VALUES (:execution_id, :created_at, :version, :json_value, :variant, :join_set_id)")?;
        let ffqn = req.ffqn.clone();
        let created_at = req.created_at;
        let scheduled_at = req.scheduled_at;
        let event = ExecutionEventInner::from(req);
        stmt.execute(named_params! {
            ":execution_id": &execution_id_str,
            ":created_at": created_at,
            ":version": version.0,
            ":json_value": serde_json::to_value(&event).unwrap(),
            ":variant": event.variant(),
            ":join_set_id": event.join_set_id().map(|join_set_id| join_set_id.to_string()),
        })?;
        let next_version = Version::new(version.0 + 1);
        let pending_state = PendingState::PendingAt { scheduled_at };
        Self::update_index(
            tx,
            execution_id,
            &pending_state,
            &next_version,
            false,
            Some(ffqn),
        )?;
        Ok(next_version)
    }

    #[instrument(skip_all, fields(%execution_id, %pending_state, %next_version, purge, ?ffqn))]
    fn update_index(
        tx: &Transaction,
        execution_id: ExecutionId,
        pending_state: &PendingState,
        next_version: &Version,
        purge: bool,               // should the execution be deleted from `t_state`
        ffqn: Option<FunctionFqn>, // will be fetched from `Created` if required
    ) -> Result<Option<u32>, SqliteError> {
        debug!("update_index");
        let execution_id_str = execution_id.to_string();
        if purge {
            let mut stmt =
                tx.prepare_cached("DELETE FROM t_state WHERE execution_id = :execution_id")?;
            stmt.execute(named_params! {
                ":execution_id": execution_id_str,
            })?;
        }
        match pending_state {
            PendingState::PendingAt { .. } => {
                let scheduled_at = if let PendingState::PendingAt { scheduled_at } = pending_state {
                    Some(*scheduled_at)
                } else {
                    None
                };
                debug!("Setting state `Pending(`{scheduled_at:?}`)");
                let mut stmt = tx.prepare(
                    "INSERT INTO t_state (execution_id, next_version, pending_or_expires_at, ffqn) \
                    VALUES (:execution_id, :next_version, :pending_or_expires_at, :ffqn)",
                )?;
                let ffqn = if let Some(ffqn) = ffqn {
                    ffqn
                } else {
                    Self::fetch_created_event(tx, execution_id)?.ffqn
                };
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": next_version.0,
                    ":pending_or_expires_at": scheduled_at.unwrap_or_default(),
                    ":ffqn": ffqn.to_string()
                })?;
                Ok(None)
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
                (execution_id, next_version, pending_or_expires_at, executor_id, run_id, intermittent_event_count, max_retries, retry_exp_backoff_millis, parent_execution_id, parent_join_set_id) \
                VALUES \
                (:execution_id, :next_version, :pending_or_expires_at, :executor_id, :run_id, :intermittent_event_count, :max_retries, :retry_exp_backoff_millis, :parent_execution_id, :parent_join_set_id)",
                )?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": next_version.0, // If the lock expires, this version will be used by `expired_timers_watcher`.
                    ":pending_or_expires_at": lock_expires_at,
                    ":executor_id": executor_id.to_string(),
                    ":run_id": run_id.to_string(),
                    ":intermittent_event_count": intermittent_event_count,
                    ":max_retries": create_req.max_retries,
                    ":retry_exp_backoff_millis": u64::try_from(create_req.retry_exp_backoff.as_millis()).unwrap(),
                    ":parent_execution_id": create_req.parent.map(|(pid, _) | pid.to_string()),
                    ":parent_join_set_id": create_req.parent.map(|(_, join_set_id)| join_set_id.to_string()),

                })?;
                Ok(Some(intermittent_event_count))
            }
            PendingState::BlockedByJoinSet {
                join_set_id,
                lock_expires_at,
            } => {
                debug!(%join_set_id, "Setting state `BlockedByJoinSet({next_version},{lock_expires_at})`");
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO t_state (execution_id, next_version, pending_or_expires_at, join_set_id) \
                VALUES \
                (:execution_id, :next_version, :pending_or_expires_at, :join_set_id)",
                )?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": next_version.0, // If the lock expires, this version will be used by `expired_timers_watcher`.
                    ":pending_or_expires_at": lock_expires_at,
                    ":join_set_id": join_set_id.to_string(),
                })?;
                Ok(None)
            }
            PendingState::Finished => Ok(None),
        }
    }

    #[instrument(skip_all, fields(%execution_id, %next_version, purge))]
    fn update_index_next_version(
        tx: &Transaction,
        execution_id: ExecutionId,
        next_version: &Version,
        delay_req: Option<DelayReq>,
    ) -> Result<(), SqliteError> {
        debug!("update_index_version");
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx.prepare_cached(
            "UPDATE t_state SET next_version = :next_version WHERE execution_id = :execution_id AND next_version = :current_version",
        )?;
        stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":next_version": next_version.0,
            ":current_version": next_version.0 - 1,
        })?;
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
        Ok(())
    }

    // Map `t_state` into one of: `PendingAt`, `Locked`, `BlockedByJoinSet`. State `Finished` is merged into not found, represented as None.
    fn get_pending_state(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<Option<(PendingState, Version)>, SqliteError> {
        #[derive(Debug)]
        struct CombinedState {
            next_version: Version,
            pending_or_expires_at: DateTime<Utc>,
            executor_id: Option<ExecutorId>,
            run_id: Option<RunId>,
            join_set_id: Option<JoinSetId>,
        }

        impl CombinedState {
            fn into_pending_state(self) -> Result<(PendingState, Version), String> {
                match self {
                    CombinedState {
                        next_version,
                        pending_or_expires_at,
                        executor_id: None,
                        run_id: None,
                        join_set_id: None,
                    } => Ok((
                        PendingState::PendingAt {
                            scheduled_at: pending_or_expires_at,
                        },
                        next_version,
                    )), // merges PendingNow into PendingAt(default)
                    CombinedState {
                        next_version,
                        pending_or_expires_at,
                        executor_id: Some(executor_id),
                        run_id: Some(run_id),
                        join_set_id: None,
                    } => Ok((
                        PendingState::Locked {
                            executor_id,
                            run_id,
                            lock_expires_at: pending_or_expires_at,
                        },
                        next_version,
                    )),
                    CombinedState {
                        next_version,
                        pending_or_expires_at,
                        executor_id: None,
                        run_id: None,
                        join_set_id: Some(join_set_id),
                    } => Ok((
                        PendingState::BlockedByJoinSet {
                            join_set_id,
                            lock_expires_at: pending_or_expires_at,
                        },
                        next_version,
                    )),
                    _ => Err(format!("invalid state: {self:?}")),
                }
            }
        }

        let mut stmt = tx.prepare(
            "SELECT next_version, pending_or_expires_at, ffqn, executor_id, run_id, join_set_id FROM t_state WHERE \
        execution_id = :execution_id",
        )?;
        stmt.query_row(
            named_params! {
                ":execution_id": execution_id.to_string(),
            },
            |row| {
                Ok(CombinedState {
                    next_version: Version::new(row.get("next_version")?),
                    pending_or_expires_at: row.get::<_, DateTime<Utc>>("pending_or_expires_at")?,
                    executor_id: row
                        .get::<_, Option<ExecutorIdW>>("executor_id")?
                        .map(|w| w.0),
                    run_id: row.get::<_, Option<RunIdW>>("run_id")?.map(|w| w.0),
                    join_set_id: row
                        .get::<_, Option<JoinSetIdW>>("join_set_id")?
                        .map(|w| w.0),
                }
                .into_pending_state()
                .map_err(|string| {
                    error!("Cannot deserialize pending state from  {row:?} - {string}");
                    SqliteError::Parsing(StrVariant::Arc(Arc::from(string)))
                }))
            },
        )
        .optional()
        .map_err(async_sqlite::Error::Rusqlite)
        .map_err(SqliteError::Sqlite)?
        .transpose()
    }

    #[instrument(skip_all, fields(%execution_id, %run_id, %executor_id))]
    fn lock_inner(
        tx: &Transaction,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        appending_version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockedExecution, SqliteError> {
        debug!("lock_inner");
        Self::check_next_version(tx, execution_id, &appending_version)?;
        let (pending_state, _) = Self::get_pending_state(tx, execution_id)?
            .ok_or(DbError::Specific(SpecificError::NotFound))?;
        pending_state
            .can_append_lock(created_at, executor_id, run_id, lock_expires_at)
            .map_err(DbError::Specific)?;

        // Append to `execution_log` table.
        let event = ExecutionEventInner::Locked {
            executor_id,
            lock_expires_at,
            run_id,
        };
        let mut stmt = tx.prepare_cached(
            "INSERT INTO t_execution_log \
            (execution_id, created_at, json_value, version, variant) \
            VALUES \
            (:execution_id, :created_at, :json_value, :version, :variant)",
        )?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": created_at,
            ":json_value": serde_json::to_value(&event).unwrap(),
            ":version": appending_version.0,
            ":variant": event.variant(),
        })?;
        // Update `t_state`
        let pending_state = PendingState::Locked {
            executor_id,
            run_id,
            lock_expires_at,
        };

        let next_version = Version::new(appending_version.0 + 1);
        let intermittent_event_count = Self::update_index(
            tx,
            execution_id,
            &pending_state,
            &next_version,
            true,
            None, // No need for ffqn as we are not constructing PendingState::Pending
        )?
        .expect("intermittent_event_count must be set");
        // Fetch event_history and `Created` event to construct the response.
        let mut events = tx
            .prepare(
                "SELECT json_value FROM t_execution_log WHERE \
                execution_id = :execution_id AND (variant = :v1 OR variant = :v2) \
                ORDER BY version",
            )?
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
                        SqliteError::Parsing(StrVariant::Arc(Arc::from(serde.to_string())))
                    });
                    Ok(event)
                },
            )?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<VecDeque<_>, _>>()?;
        let Some(ExecutionEventInner::Created {
            ffqn,
            params,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
            parent,
            ..
        }) = events.pop_front()
        else {
            error!("Execution log must contain at least `Created` event");
            return Err(SqliteError::DbError(DbError::Specific(
                SpecificError::ConsistencyError(StrVariant::Static(
                    "execution log must contain `Created` event",
                )),
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
                    Err(SqliteError::DbError(DbError::Specific(
                        SpecificError::ConsistencyError(StrVariant::Static(
                            "Rows can only contain `Created` and `HistoryEvent` event kinds",
                        )),
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LockedExecution {
            execution_id,
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
        })
    }

    fn count_join_next(
        tx: &Transaction,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
    ) -> Result<u64, SqliteError> {
        let mut stmt = tx.prepare(
            "SELECT COUNT(*) as count FROM t_execution_log WHERE execution_id = :execution_id AND join_set_id = :join_set_id \
            AND history_event_type = :join_next",
        )?;
        Ok(stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":join_set_id": join_set_id.to_string(),
                    ":join_next": "JoinNext",
                },
                |row| row.get("count"),
            )
            .map_err(async_sqlite::Error::Rusqlite)?)
    }

    #[allow(clippy::too_many_lines)]
    fn append(
        tx: &Transaction,
        execution_id: ExecutionId,
        req: &AppendRequest,
        appending_version: Version,
    ) -> Result<AppendResponse, SqliteError> {
        enum IndexAction {
            PendingStateChanged(PendingState),
            NoPendingStateChange(Option<DelayReq>),
        }
        impl IndexAction {
            fn pending_state(&self) -> Option<&PendingState> {
                if let IndexAction::PendingStateChanged(pending_state) = self {
                    Some(pending_state)
                } else {
                    None
                }
            }
        }
        let (_pending_state, expected_version) = Self::get_pending_state(tx, execution_id)?.ok_or(
            SqliteError::DbError(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("execution does not exist or is already finished"),
            ))),
        )?;
        Self::check_expected_next_and_appending_version(&expected_version, &appending_version)?;

        let mut stmt = tx.prepare(
                    "INSERT INTO t_execution_log (execution_id, created_at, json_value, version, variant, join_set_id) \
                    VALUES (:execution_id, :created_at, :json_value, :version, :variant, :join_set_id)")?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": req.created_at,
            ":json_value": serde_json::to_value(&req.event).unwrap(),
            ":version": appending_version.0,
            ":variant": req.event.variant(),
            ":join_set_id": req.event.join_set_id().map(|join_set_id | join_set_id.to_string()),
        })?;
        // Calculate current pending state

        let index_action = match &req.event {
            ExecutionEventInner::Created { .. } => {
                unreachable!("handled in the caller")
            }

            ExecutionEventInner::Locked {
                executor_id,
                run_id,
                lock_expires_at,
            } => {
                let locked = Self::lock_inner(
                    &tx,
                    req.created_at,
                    execution_id,
                    *run_id,
                    appending_version,
                    *executor_id,
                    *lock_expires_at,
                )?;
                return Ok(locked.version);
            }
            ExecutionEventInner::IntermittentFailure { expires_at, .. }
            | ExecutionEventInner::IntermittentTimeout { expires_at } => {
                IndexAction::PendingStateChanged(PendingState::PendingAt {
                    scheduled_at: *expires_at,
                })
            }
            ExecutionEventInner::Finished { .. } => {
                IndexAction::PendingStateChanged(PendingState::Finished)
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
                    | HistoryEvent::Persist { .. },
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
                    })
                }
            }

            ExecutionEventInner::CancelRequest => {
                // FIXME
                IndexAction::NoPendingStateChange(None)
            }
        };
        let next_version = Version(appending_version.0 + 1);
        match index_action {
            IndexAction::PendingStateChanged(pending_state) => {
                Self::update_index(&tx, execution_id, &pending_state, &next_version, true, None)?;
            }
            IndexAction::NoPendingStateChange(delay_req) => {
                Self::update_index_next_version(&tx, execution_id, &next_version, delay_req)?;
            }
        };
        Ok(next_version)
    }

    #[allow(clippy::too_many_lines)]
    fn append_response(
        tx: &Transaction,
        execution_id: ExecutionId,
        req: &JoinSetResponseEventOuter,
    ) -> Result<(), SqliteError> {
        let mut stmt = tx.prepare(
            "INSERT INTO t_join_set_response (execution_id, created_at, json_value, join_set_id, delay_id, child_execution_id) \
                    VALUES (:execution_id, :created_at, :json_value, :join_set_id, :delay_id, :child_execution_id)",
        )?;
        stmt.execute(named_params! {
            ":execution_id": execution_id.to_string(),
            ":created_at": req.created_at,
            ":json_value": serde_json::to_value(&req.event.event).unwrap(),
            ":join_set_id": req.event.join_set_id.to_string(),
            ":delay_id": req.event.event.delay_id().map(|id| id.to_string()),
            ":child_execution_id": req.event.event.child_execution_id().map(|id|id.to_string()),
        })?;

        // if the execution is going to be unblocked by this response...
        let previous_pending_state = Self::get_pending_state(tx, execution_id)?.map(|it| it.0);
        match previous_pending_state {
            Some(PendingState::BlockedByJoinSet {
                lock_expires_at,
                join_set_id: found_join_set_id,
                ..
            }) if req.event.join_set_id == found_join_set_id => {
                // update the pending state.
                let pending_state = PendingState::PendingAt {
                    scheduled_at: lock_expires_at,
                };
                let next_version = Self::get_next_version(tx, execution_id)?;
                Self::update_index(tx, execution_id, &pending_state, &next_version, true, None)?;
            }
            _ => {}
        }
        if let JoinSetResponseEvent {
            join_set_id,
            event: JoinSetResponse::DelayFinished { delay_id },
        } = req.event
        {
            trace!(%join_set_id, %delay_id, "Deleting from `t_delay`");
            let mut stmt =
                tx.prepare_cached("DELETE FROM t_delay WHERE execution_id = :execution_id AND join_set_id = :join_set_id AND delay_id = :delay_id")?;
            stmt.execute(named_params! {
                ":execution_id": execution_id.to_string(),
                ":join_set_id": join_set_id.to_string(),
                ":delay_id": delay_id.to_string(),
            })?;
        }
        Ok(())
    }

    fn get_responses(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<Vec<JoinSetResponseEventOuter>, SqliteError> {
        tx.prepare(
            "SELECT created_at, join_set_id, json_value FROM t_join_set_response WHERE \
                execution_id = :execution_id ORDER BY created_at",
        )?
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
        )?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| SqliteError::Sqlite(err.into()))
    }

    fn nth_response(
        tx: &Transaction,
        execution_id: ExecutionId,
        join_set_id: JoinSetId,
        skip_rows: u64,
    ) -> Result<Option<JoinSetResponseEventOuter>, SqliteError> {
        tx.prepare(
            "SELECT created_at, join_set_id, json_value FROM t_join_set_response WHERE \
            execution_id = :execution_id AND join_set_id = :join_set_id ORDER BY created_at
            LIMIT 1 OFFSET :offset",
        )?
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
        .map_err(|err| SqliteError::Sqlite(err.into()))
    }
}

#[async_trait]
impl DbConnection for SqlitePool {
    #[instrument(skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError> {
        debug!("create");
        trace!(?req, "create");
        self.pool
            .transaction_write_with_span(move |tx| Self::create_inner(tx, req), Span::current())
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self))]
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError> {
        let execution_ids_versions = self
            .pool
            .conn_with_err_and_span::<_, _, SqliteError>(
                move |conn| {
                    let mut execution_ids_versions = Vec::with_capacity(batch_size);
                    // TODO: The locked executions should be sorted by pending date, otherwise ffqns later in the list might get starved.
                    for ffqn in ffqns {
                        let mut stmt = conn.prepare(
                            "SELECT execution_id, next_version FROM t_state WHERE \
                            pending_or_expires_at <= :pending_or_expires_at AND ffqn = :ffqn \
                            ORDER BY pending_or_expires_at LIMIT :batch_size",
                        )?;
                        let execs_and_versions = stmt
                            .query_map(
                                named_params! {
                                    ":pending_or_expires_at": pending_at_or_sooner,
                                    ":ffqn": ffqn.to_string(),
                                    ":batch_size": batch_size - execution_ids_versions.len(),
                                },
                                |row| {
                                    let execution_id = row
                                        .get::<_, String>("execution_id")?
                                        .parse::<ExecutionId>();
                                    let version =
                                        Version::new(row.get::<_, usize>("next_version")?);
                                    Ok(execution_id.map(|e| (e, version)))
                                },
                            )?
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|str| SqliteError::Parsing(str))?;
                        execution_ids_versions.extend(execs_and_versions);
                        if execution_ids_versions.len() == batch_size {
                            break;
                        }
                    }
                    Ok(execution_ids_versions)
                },
                Span::current(),
            )
            .await?;
        if execution_ids_versions.is_empty() {
            Ok(vec![])
        } else {
            debug!("Locking {execution_ids_versions:?}");
            self.pool
                .transaction_write_with_span::<_, _, SqliteError>(
                    move |tx| {
                        let mut locked_execs = Vec::with_capacity(execution_ids_versions.len());
                        // Append lock
                        for (execution_id, version) in execution_ids_versions {
                            let locked = Self::lock_inner(
                                &tx,
                                created_at,
                                execution_id,
                                RunId::generate(),
                                version,
                                executor_id,
                                lock_expires_at,
                            )?;
                            locked_execs.push(locked);
                        }
                        Ok(locked_execs)
                    },
                    Span::current(),
                )
                .await
                .map_err(DbError::from)
        }
    }

    /// Specialized `append` which returns the event history.
    #[instrument(skip(self))]
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        debug!("lock");
        self.pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    let locked = Self::lock_inner(
                        &tx,
                        created_at,
                        execution_id,
                        run_id,
                        version,
                        executor_id,
                        lock_expires_at,
                    )?;
                    Ok((locked.event_history, locked.version))
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self, req))]
    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        debug!(%req, "append");
        trace!(?req, "append");
        // Disallow `Created` event
        if let ExecutionEventInner::Created { .. } = req.event {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("Cannot append `Created` event - use `create` instead"),
            )));
        }
        self.pool
            .transaction_write_with_span(
                move |tx| Self::append(tx, execution_id, &req, version),
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self, batch))]
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
        self.pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    let mut version = version;
                    for event in batch {
                        version = Self::append(
                            tx,
                            execution_id,
                            &AppendRequest { created_at, event },
                            version,
                        )?;
                    }
                    Ok(version)
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self, batch, child_req))]
    async fn append_batch_create_child(
        &self,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        execution_id: ExecutionId,
        mut version: Version,
        child_req: CreateRequest,
    ) -> Result<AppendBatchResponse, DbError> {
        debug!("append_batch_create_child");
        trace!(?batch, ?child_req, "append_batch_create_child");
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
        self.pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    for event in batch {
                        version = Self::append(
                            tx,
                            execution_id,
                            &AppendRequest { created_at, event },
                            version,
                        )?;
                    }
                    Self::create_inner(tx, child_req)?;
                    Ok(version)
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self, batch, parent_response_event))]
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
        self.pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    let mut version = version;
                    for event in batch {
                        version = Self::append(
                            tx,
                            execution_id,
                            &AppendRequest { created_at, event },
                            version,
                        )?;
                    }
                    Self::append_response(
                        tx,
                        parent_execution_id,
                        &JoinSetResponseEventOuter {
                            created_at,
                            event: parent_response_event,
                        },
                    )?;
                    Ok(version)
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self))]
    async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionLog, DbError> {
        trace!("get");
        self.pool
            .transaction_read_with_span::<_, _, SqliteError>(
                move |tx| {
                    let mut stmt = tx.prepare(
                        "SELECT created_at, json_value FROM t_execution_log WHERE \
                        execution_id = :execution_id ORDER BY version",
                    )?;
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
                                    SqliteError::Parsing(StrVariant::Arc(Arc::from(
                                        serde.to_string(),
                                    )))
                                });
                                Ok(event)
                            },
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;
                    let pending_state = Self::get_pending_state(&tx, execution_id)?
                        .map_or(PendingState::Finished, |it| it.0); // Execution exists and was not found in `t_state` => Finished
                    let version = Version::new(events.len());
                    let responses = Self::get_responses(&tx, execution_id)?;
                    Ok(ExecutionLog {
                        execution_id,
                        events,
                        responses,
                        version,
                        pending_state,
                    })
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self, response_event))]
    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbError> {
        trace!("append_response");
        self.pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    Self::append_response(
                        &tx,
                        execution_id,
                        &JoinSetResponseEventOuter {
                            created_at,
                            event: response_event,
                        },
                    )
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    /// Get currently expired locks and async timers (delay requests)
    #[instrument(skip(self))]
    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError> {
        self.pool.conn_with_err_and_span::<_, _, SqliteError>(
            move |conn| {
                let mut expired_timers = conn.prepare(
                    "SELECT execution_id, join_set_id, delay_id FROM t_delay WHERE expires_at <= :at",
                )?.query_map(
                        named_params! {
                            ":at": at,
                        },
                        |row| {
                            let execution_id = row.get::<_, ExecutionIdW>("execution_id")?.0;
                            let join_set_id = row.get::<_, JoinSetIdW>("join_set_id")?.0;
                            let delay_id = row.get::<_, DelayIdW>("delay_id")?.0;
                            Ok(ExpiredTimer::AsyncDelay { execution_id, join_set_id, delay_id })
                        },
                    )?
                    .collect::<Result<Vec<_>, _>>()?
                    ;

                    expired_timers.extend(conn.prepare(
                        "SELECT execution_id, next_version, intermittent_event_count, max_retries, retry_exp_backoff_millis, parent_execution_id, parent_join_set_id FROM t_state \
                        WHERE pending_or_expires_at <= :at AND executor_id IS NOT NULL",
                    )?.query_map(
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
                                    retry_exp_backoff, parent: parent_execution_id.and_then(|pexe| parent_join_set_id.map(|pjs| (pexe, pjs))) })
                            },
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                    );

                if !expired_timers.is_empty() {
                    debug!("get_expired_timers found {expired_timers:?}");
                }
                Ok(expired_timers)
            },
            Span::current(),
        )
        .await
        .map_err(DbError::from)
    }
}

#[cfg(any(test, feature = "tempfile"))]
pub mod tempfile {
    use super::SqlitePool;
    use tempfile::NamedTempFile;

    pub async fn sqlite_pool() -> (SqlitePool, Option<NamedTempFile>) {
        if let Ok(path) = std::env::var("SQLITE_FILE") {
            (SqlitePool::new(path).await.unwrap(), None)
        } else {
            let file = NamedTempFile::new().unwrap();
            let path = file.path();
            (SqlitePool::new(path).await.unwrap(), Some(file))
        }
    }
}

#[cfg(all(test, not(madsim)))] // async-sqlite attempts to spawn a system thread in simulation
mod tests {
    use crate::sqlite_dao::tempfile::sqlite_pool;
    use concepts::storage::DbPool;

    #[tokio::test]
    async fn check_sqlite_version() {
        let (pool, _guard) = sqlite_pool().await;
        let version = pool
            .pool
            .conn(|conn| conn.query_row("SELECT SQLITE_VERSION()", [], |row| row.get(0)))
            .await;
        let version: String = version.unwrap();
        assert_eq!("3.45.0", version);
        pool.close().await.unwrap();
    }
}
