#![allow(clippy::all, dead_code)]
use async_sqlite::{rusqlite::named_params, ClientBuilder, JournalMode, Pool, PoolBuilder};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{DelayId, ExecutorId, JoinSetId, RunId},
    storage::{
        AppendBatch, AppendBatchResponse, AppendRequest, AppendResponse, CreateRequest,
        DbConnection, DbError, DbPool, ExecutionEvent, ExecutionEventInner, ExecutionLog,
        ExpiredTimer, HistoryEvent, JoinSetRequest, LockKind, LockPendingResponse, LockResponse,
        LockedExecution, PendingState, SpecificError, Version, DUMMY_CREATED, DUMMY_HISTORY_EVENT,
        DUMMY_INTERMITTENT_FAILURE, DUMMY_INTERMITTENT_TIMEOUT,
    },
    ExecutionId, FunctionFqn, StrVariant,
};
use rusqlite::{Connection, OptionalExtension, Transaction};
use std::{cmp::max, collections::VecDeque, path::Path, sync::Arc};
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
    pending_state TEXT,
    PRIMARY KEY (execution_id, version)
);
";

/// Stores executions in `Pending` state
const CREATE_TABLE_T_PENDING: &str = r"
CREATE TABLE IF NOT EXISTS t_pending (
    execution_id TEXT NOT NULL,
    next_version INTEGER NOT NULL,
    pending_at TEXT NOT NULL,
    ffqn TEXT NOT NULL,
    PRIMARY KEY (execution_id)
)
"; // TODO: index by `pending_at` + `ffqn`

/// Stores executions in `Locked` state, as well as delay requests.
const CREATE_TABLE_T_LOCKED: &str = r"
CREATE TABLE IF NOT EXISTS t_locked (
    execution_id TEXT NOT NULL,
    next_version INTEGER NOT NULL,
    join_set_id TEXT,
    delay_id TEXT,
    lock_expires_at TEXT NOT NULL,
    PRIMARY KEY (execution_id, join_set_id, delay_id)
)
"; // TODO: index by `lock_expires_at`

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
        match err {
            SqliteError::DbError(err) => err,
            _ => DbError::Specific(SpecificError::ValidationFailed(StrVariant::Arc(Arc::from(
                err.to_string(),
            )))),
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
                trace!("Executing `CREATE_TABLE_T_PENDING`");
                conn.execute(CREATE_TABLE_T_PENDING, [])?;
                trace!("Executing `CREATE_TABLE_T_LOCKED`");
                conn.execute(CREATE_TABLE_T_LOCKED, [])?;
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

    fn check_next_version(
        tx: &Transaction,
        execution_id: ExecutionId,
        appending_version: &Version,
    ) -> Result<(), SqliteError> {
        let expected_version = Self::get_next_version(tx, execution_id)?;
        if expected_version != *appending_version {
            return Err(SqliteError::DbError(DbError::Specific(
                SpecificError::VersionMismatch {
                    appending_version: appending_version.clone(),
                    expected_version,
                },
            )));
        }
        Ok(())
    }

    #[instrument(skip_all, fields(execution_id = %req.execution_id))]
    fn create_inner(tx: &Transaction, req: CreateRequest) -> Result<AppendResponse, SqliteError> {
        debug!("create_inner");
        let version = Version::new(0);
        let execution_id = req.execution_id;
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx.prepare(
                "INSERT INTO t_execution_log (execution_id, created_at, version, json_value, variant, pending_state, join_set_id ) \
                VALUES (:execution_id, :created_at, :version, :json_value, :variant, :pending_state, :join_set_id)")?;
        let ffqn = req.ffqn.clone();
        let created_at = req.created_at;
        let scheduled_at = req.scheduled_at;
        let pending_state = scheduled_at.map_or(PendingState::PendingNow, |scheduled_at| {
            PendingState::PendingAt { scheduled_at }
        });
        let event = ExecutionEventInner::from(req);
        stmt.execute(named_params! {
            ":execution_id": &execution_id_str,
            ":created_at": created_at,
            ":version": version.0,
            ":json_value": serde_json::to_value(&event).unwrap(),
            ":variant": event.variant(),
            ":pending_state": serde_json::to_value(&pending_state).unwrap(),
            ":join_set_id": event.join_set_id().map(|join_set_id| join_set_id.to_string()),
        })?;
        let next_version = Version::new(version.0 + 1);
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

    #[instrument(skip_all, fields(%execution_id, %pending_state, %next_version, purge))]
    fn update_index(
        tx: &Transaction,
        execution_id: ExecutionId,
        pending_state: &PendingState,
        next_version: &Version,
        purge: bool, // should the execution be deleted from `pending` and `locked`
        ffqn: Option<FunctionFqn>, // will be fetched from `Created` if required
    ) -> Result<(), SqliteError> {
        debug!("update_index");
        let execution_id_str = execution_id.to_string();
        if purge {
            let mut stmt =
                tx.prepare_cached("DELETE FROM t_pending WHERE execution_id = :execution_id")?;
            stmt.execute(named_params! {
                ":execution_id": execution_id_str,
            })?;

            let mut stmt =
                tx.prepare_cached("DELETE FROM t_locked WHERE execution_id = :execution_id")?;
            stmt.execute(named_params! {
                ":execution_id": execution_id_str,
            })?;
        }
        match pending_state {
            PendingState::PendingNow | PendingState::PendingAt { .. } => {
                let scheduled_at = if let PendingState::PendingAt { scheduled_at } = pending_state {
                    Some(*scheduled_at)
                } else {
                    None
                };
                debug!("Inserting to `t_pending` with schedule: `{scheduled_at:?}`");
                let mut stmt = tx.prepare(
                    "INSERT INTO t_pending (execution_id, next_version, pending_at, ffqn) \
                    VALUES (:execution_id, :next_version, :pending_at, :ffqn)",
                )?;
                let ffqn = if let Some(ffqn) = ffqn {
                    ffqn
                } else {
                    Self::fetch_created_event(tx, execution_id)?.ffqn
                };
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": next_version.0,
                    ":pending_at": scheduled_at.unwrap_or_default(),
                    ":ffqn": ffqn.to_string()
                })?;
                Ok(())
            }
            PendingState::Locked {
                lock_expires_at, ..
            } => {
                debug!("Inserting to `t_locked`");
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO t_locked (execution_id, next_version, lock_expires_at) \
                VALUES \
                (:execution_id, :next_version, :lock_expires_at)",
                )?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": next_version.0, // If the lock expires, this version will be used by `expired_timers_watcher`.
                    ":lock_expires_at": lock_expires_at,
                })?;
                Ok(())
            }
            PendingState::BlockedByJoinSet { .. } | PendingState::Finished => Ok(()),
        }
    }

    #[instrument(skip_all, fields(%execution_id, %next_version, purge))]
    fn update_index_no_pending_state_change(
        tx: &Transaction,
        execution_id: ExecutionId,
        next_version: &Version,
        delay_req: Option<DelayReq>,
    ) -> Result<(), SqliteError> {
        debug!("update_index_version");
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx.prepare_cached(
            "UPDATE t_pending SET next_version = :next_version WHERE execution_id = :execution_id AND next_version = :current_version",
        )?;
        stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":next_version": next_version.0,
            ":current_version": next_version.0 - 1,
        })?;
        let mut stmt =
            tx.prepare_cached("UPDATE t_locked SET next_version = :next_version WHERE execution_id = :execution_id AND next_version = :current_version")?;
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
            debug!("Inserting delay to `t_locked`");
            let mut stmt = tx.prepare_cached(
                "INSERT INTO t_locked (execution_id, next_version, join_set_id, delay_id, lock_expires_at) \
                VALUES \
                (:execution_id, :next_version, :join_set_id, :delay_id, :lock_expires_at)",
            )?;
            stmt.execute(named_params! {
                ":execution_id": execution_id_str,
                ":next_version": next_version.0, // If the lock expires, this version will be used by `expired_timers_watcher`.
                ":join_set_id": join_set_id.to_string(),
                ":delay_id": delay_id.to_string(),
                ":lock_expires_at": expires_at,
            })?;
        }
        Ok(())
    }

    #[instrument(skip_all, fields(%execution_id))]
    fn current_pending_state(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<PendingState, SqliteError> {
        let mut stmt = tx.prepare(
            "SELECT pending_state FROM t_execution_log WHERE \
        execution_id = :execution_id AND pending_state IS NOT NULL \
        ORDER BY version DESC LIMIT 1",
        )?;
        let pending_state = stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| row.get::<_, serde_json::Value>("pending_state"),
            )
            .optional()
            .map_err(async_sqlite::Error::Rusqlite)?
            .ok_or(SqliteError::DbError(DbError::Specific(
                // Every execution created must have at least Created pending state.
                SpecificError::NotFound,
            )))?;
        serde_json::from_value::<PendingState>(pending_state).map_err(|serde| {
            error!("Cannot parse `pending_state` - {serde:?}");
            SqliteError::Parsing(StrVariant::Arc(Arc::from(serde.to_string())))
        })
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
        let pending_state = Self::current_pending_state(tx, execution_id)?;
        let lock_kind = pending_state
            .can_append_lock(created_at, executor_id, run_id, lock_expires_at)
            .map_err(DbError::Specific)?;

        // Remove from `pending` table, unless we are extending the lock.
        let execution_id_str = execution_id.to_string();
        match lock_kind {
            LockKind::CreatingNewLock => {
                let mut stmt = tx.prepare_cached(
                    "DELETE FROM t_pending WHERE execution_id = :execution_id AND next_version = :next_version",
                )?;
                let deleted = stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": appending_version.0, // This must be the version requested.
                })?;
                if deleted != 1 {
                    error!("Locking failed while in {pending_state}: expected to delete one `t_pending` row, actual number: {deleted}");
                    return Err(SqliteError::DbError(DbError::Specific(
                        SpecificError::ConsistencyError(StrVariant::Static(
                            "consistency error in `t_pending` table - locking failed",
                        )),
                    )));
                }
            }
            LockKind::Extending => {
                let mut stmt = tx.prepare_cached(
                    "DELETE FROM t_locked WHERE execution_id = :execution_id AND next_version = :next_version",
                )?;
                let deleted = stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":next_version": appending_version.0, // This must be the version requested.
                })?;
                if deleted != 1 {
                    error!("Locking failed: expected to delete one `t_locked` row, actual number: {deleted}");
                    return Err(SqliteError::DbError(DbError::Specific(
                        SpecificError::ConsistencyError(StrVariant::Static(
                            "consistency error in `t_locked` table- locking failed",
                        )),
                    )));
                }
            }
        }
        // Fetch event_history and `Created` event to construct the response.
        let mut stmt = tx.prepare(
            "SELECT json_value FROM t_execution_log WHERE \
            execution_id = :execution_id AND (variant = :v1 OR variant = :v2) \
            ORDER BY version",
        )?;
        let mut events = stmt
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
        let intermittent_event_count = Self::count_intermittent_events(tx, execution_id)?;
        // Append to `execution_log` table.
        let event = ExecutionEventInner::Locked {
            executor_id,
            lock_expires_at,
            run_id,
        };
        let mut stmt = tx.prepare_cached(
            "INSERT INTO t_execution_log \
            (execution_id, created_at, json_value, version, variant, pending_state) \
            VALUES \
            (:execution_id, :created_at, :json_value, :version, :variant, :pending_state)",
        )?;
        let pending_state = PendingState::Locked {
            executor_id,
            run_id,
            lock_expires_at,
        };
        stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":created_at": created_at,
            ":json_value": serde_json::to_value(&event).unwrap(),
            ":version": appending_version.0,
            ":variant": event.variant(),
            ":pending_state": serde_json::to_value(&pending_state).unwrap(),
        })?;
        let next_version = Version::new(appending_version.0 + 1);
        Self::update_index(tx, execution_id, &pending_state, &next_version, false, None)?;
        Ok(LockedExecution {
            execution_id,
            run_id,
            version: next_version,
            ffqn,
            params,
            event_history,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
            parent,
            intermittent_event_count,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn append(
        tx: &Transaction,
        execution_id: ExecutionId,
        req: AppendRequest,
        appending_version: Option<Version>,
    ) -> Result<AppendResponse, SqliteError> {
        enum IndexAction {
            PendingStateChanged(PendingState, Version),
            NoPendingStateChange(Option<DelayReq>, Version),
        }
        impl IndexAction {
            fn pending_state(&self) -> Option<&PendingState> {
                if let IndexAction::PendingStateChanged(pending_state, _) = self {
                    Some(pending_state)
                } else {
                    None
                }
            }
            fn version(&self) -> &Version {
                match self {
                    Self::PendingStateChanged(_, version)
                    | Self::NoPendingStateChange(_, version) => version,
                }
            }
            fn next_version(&self) -> Version {
                Version(self.version().0 + 1)
            }
        }
        // Appending after `Finished` could be allowed and ignored.
        let found_pending_state = Self::current_pending_state(&tx, execution_id)?;
        if found_pending_state == PendingState::Finished {
            error!("Cannot append request received in Finished state: {req:?}");
            return Err(
                DbError::Specific(SpecificError::ValidationFailed(StrVariant::Static(
                    "already finished",
                )))
                .into(),
            );
        }

        match (req.event, appending_version) {
            (ExecutionEventInner::Created { .. }, _) => {
                unreachable!("handled in the caller")
            }
            (
                ExecutionEventInner::Locked {
                    executor_id,
                    run_id,
                    lock_expires_at,
                },
                Some(version),
            ) => {
                let locked = Self::lock_inner(
                    &tx,
                    req.created_at,
                    execution_id,
                    run_id,
                    version,
                    executor_id,
                    lock_expires_at,
                )?;
                Ok(locked.version)
            }
            (event, None) if !event.appendable_without_version() => {
                error!("Attempted to append an event without version: {event:?}");
                return Err(DbError::Specific(SpecificError::VersionMissing).into());
            }
            (event, appending_version) => {
                if let Some(appending_version) = &appending_version {
                    Self::check_next_version(&tx, execution_id, appending_version)?;
                }
                let index_action = {
                    match (&event, appending_version) {
                        (
                            ExecutionEventInner::IntermittentFailure { expires_at, .. }
                            | ExecutionEventInner::IntermittentTimeout { expires_at },
                            Some(appending_version),
                        ) => IndexAction::PendingStateChanged(
                            PendingState::PendingAt {
                                scheduled_at: *expires_at,
                            },
                            appending_version,
                        ),
                        (ExecutionEventInner::Finished { .. }, Some(appending_version)) => {
                            IndexAction::PendingStateChanged(
                                PendingState::Finished,
                                appending_version,
                            )
                        }
                        (ExecutionEventInner::Unlocked, Some(appending_version)) => {
                            IndexAction::PendingStateChanged(
                                PendingState::PendingNow,
                                appending_version,
                            )
                        }
                        (
                            ExecutionEventInner::HistoryEvent {
                                event:
                                    HistoryEvent::JoinSet { .. }
                                    | HistoryEvent::JoinSetRequest {
                                        request: JoinSetRequest::ChildExecutionRequest { .. },
                                        ..
                                    }
                                    | HistoryEvent::Persist { .. },
                            },
                            Some(appending_version),
                        ) => IndexAction::NoPendingStateChange(None, appending_version),
                        (
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
                            },
                            Some(appending_version),
                        ) => IndexAction::NoPendingStateChange(
                            Some(DelayReq {
                                join_set_id: *join_set_id,
                                delay_id: *delay_id,
                                expires_at: *expires_at,
                            }),
                            appending_version,
                        ),
                        (
                            ExecutionEventInner::HistoryEvent {
                                event:
                                    HistoryEvent::JoinNext {
                                        join_set_id,
                                        lock_expires_at,
                                    },
                            },
                            Some(appending_version),
                        ) => IndexAction::PendingStateChanged(
                            PendingState::BlockedByJoinSet {
                                join_set_id: *join_set_id,
                                lock_expires_at: *lock_expires_at,
                            },
                            appending_version,
                        ),
                        (
                            ExecutionEventInner::HistoryEvent {
                                event: HistoryEvent::JoinSetResponse { join_set_id, .. },
                            },
                            appending_version,
                        ) => {
                            let appending_version =
                                if let Some(appending_version) = appending_version {
                                    appending_version
                                } else {
                                    Self::get_next_version(&tx, execution_id)?
                                };
                            // Was the previous event with `pending_state` a JoinNext with this join set?
                            // If a `response_forwarder` would send responses together with new pending state, the select would not be needed.
                            match found_pending_state {
                                PendingState::BlockedByJoinSet {
                                    join_set_id: found_join_set_id,
                                    lock_expires_at,
                                } if found_join_set_id == *join_set_id => {
                                    // Unblocking the execution
                                    let scheduled_at = max(req.created_at, lock_expires_at);
                                    // The original executor can continue until the lock expires, but the execution is not marked as timed out
                                    IndexAction::PendingStateChanged(
                                        PendingState::PendingAt { scheduled_at },
                                        appending_version,
                                    )
                                }
                                _ => IndexAction::NoPendingStateChange(None, appending_version),
                            }
                        }
                        (ExecutionEventInner::CancelRequest, Some(appending_version)) => {
                            //TODO
                            IndexAction::NoPendingStateChange(None, appending_version)
                        }
                        (
                            ExecutionEventInner::Locked { .. }
                            | ExecutionEventInner::Created { .. },
                            _,
                        ) => unreachable!("handled above"),
                        (event, appending_version) => {
                            error!("Unsupported combination {event:?}, {appending_version:?}");
                            return Err(DbError::Specific(SpecificError::ValidationFailed(
                                StrVariant::Static("unsupported combination (event, version)"),
                            ))
                            .into());
                        }
                    }
                };
                let mut stmt = tx.prepare(
                    "INSERT INTO t_execution_log (execution_id, created_at, json_value, version, variant, join_set_id, pending_state) \
                    VALUES (:execution_id, :created_at, :json_value, :version, :variant, :join_set_id, :pending_state)")?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":created_at": req.created_at,
                    ":json_value": serde_json::to_value(&event).unwrap(),
                    ":version": index_action.version().0,
                    ":variant": event.variant(),
                    ":join_set_id": event.join_set_id().map(|join_set_id | join_set_id.to_string()),
                    ":pending_state": index_action.pending_state().map(|pending_state| serde_json::to_value(&pending_state).unwrap()),
                })?;
                let next_version = index_action.next_version();
                match index_action {
                    IndexAction::PendingStateChanged(pending_state, _) => {
                        Self::update_index(
                            &tx,
                            execution_id,
                            &pending_state,
                            &next_version,
                            true,
                            None,
                        )?;
                    }
                    IndexAction::NoPendingStateChange(delay_req, _) => {
                        Self::update_index_no_pending_state_change(
                            &tx,
                            execution_id,
                            &next_version,
                            delay_req,
                        )?;
                    }
                };
                Ok(next_version)
            }
        }
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
        trace!("lock_pending");
        let execution_ids_versions = self
            .pool
            .conn_with_err_and_span::<_, _, SqliteError>(
                move |conn| {
                    let mut execution_ids_versions = Vec::with_capacity(batch_size);
                    // TODO: The locked executions should be sorted by pending date, otherwise ffqns later in the list might get starved.
                    for ffqn in ffqns {
                        let mut stmt = conn.prepare(
                            "SELECT execution_id, next_version FROM t_pending WHERE \
                            pending_at <= :pending_at AND ffqn = :ffqn \
                            ORDER BY pending_at LIMIT :batch_size",
                        )?;
                        let execs_and_versions = stmt
                            .query_map(
                                named_params! {
                                    ":pending_at": pending_at_or_sooner,
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
        version: Option<Version>,
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
                move |tx| Self::append(tx, execution_id, req, version),
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self, batch))]
    async fn append_batch(
        &self,
        batch: AppendBatch,
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
            .any(|event| matches!(event.event, ExecutionEventInner::Created { .. }))
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
                    for req in batch {
                        version = Self::append(tx, execution_id, req, Some(version))?;
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
        batch: AppendBatch,
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
            .any(|event| matches!(event.event, ExecutionEventInner::Created { .. }))
        {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("Cannot append `Created` event - use `create` instead"),
            )));
        }
        self.pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    for req in batch {
                        version = Self::append(tx, execution_id, req, Some(version))?;
                    }
                    Self::create_inner(tx, child_req)?;
                    Ok(version)
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip(self, batch, parent))]
    async fn append_batch_respond_to_parent(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        parent: (ExecutionId, AppendRequest),
    ) -> Result<AppendBatchResponse, DbError> {
        debug!("append_batch_respond_to_parent");
        trace!(?batch, ?parent, "append_batch_respond_to_parent");
        if batch.is_empty() {
            error!("Empty batch request");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("empty batch request"),
            )));
        }
        if batch
            .iter()
            .any(|event| matches!(event.event, ExecutionEventInner::Created { .. }))
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
                    for req in batch {
                        version = Self::append(tx, execution_id, req, Some(version))?;
                    }
                    Self::append(tx, parent.0, parent.1, None)?;
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
            .conn_with_err_and_span::<_, _, SqliteError>(
                move |tx| {
                    let mut stmt = tx.prepare(
                        "SELECT created_at, json_value, pending_state FROM t_execution_log WHERE \
                        execution_id = :execution_id ORDER BY version",
                    )?;
                    let events_and_pending_state = stmt
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
                                let pending_state =
                                    row.get::<_, Option<serde_json::Value>>("pending_state")?;

                                Ok(event.map(|event| (event, pending_state)))
                            },
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;
                    let mut pending_state = None;
                    let mut events = Vec::with_capacity(events_and_pending_state.len());
                    for (event, pending) in events_and_pending_state {
                        events.push(event);
                        if let Some(pending) = pending {
                            pending_state = Some(pending);
                        }
                    }
                    let pending_state = match pending_state
                        .map(|pending_state| serde_json::from_value::<PendingState>(pending_state))
                    {
                        Some(Ok(pending_state)) => Ok(pending_state),
                        None => {
                            error!("Execution log must have at least one pending state");
                            Err(SqliteError::DbError(DbError::Specific(
                                SpecificError::ConsistencyError(StrVariant::Static(
                                    "execution log must have at least one pending state",
                                )),
                            )))
                        }
                        Some(Err(serde)) => {
                            error!("Cannot parse pending state: `{serde:?}` - {events:?}");
                            Err(SqliteError::Parsing(StrVariant::Static(
                                "cannot parse pending state",
                            )))
                        }
                    }?;
                    let version = Version::new(events.len());
                    Ok(ExecutionLog {
                        execution_id,
                        events,
                        version,
                        pending_state,
                    })
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    /// Get currently expired locks and async timers (delay requests)
    #[instrument(skip(self))]
    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError> {
        trace!("get_expired_timers");
        self.pool.conn_with_err_and_span::<_, _, SqliteError>(
            move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT execution_id, next_version, join_set_id, delay_id FROM t_locked WHERE lock_expires_at <= :at",
                )?;
                let query_res = stmt
                    .query_map(
                        named_params! {
                            ":at": at,
                        },
                        |row| {
                            let execution_id = row.get::<_, String>("execution_id")?.parse::<ExecutionId>();
                            let version = Version::new(row.get::<_, usize>("next_version")?);
                            let join_set_id = row.get::<_, Option<String>>("join_set_id")?.map(|str| str.parse::<JoinSetId>());
                            let delay_id = row.get::<_, Option<String>>("delay_id")?.map(|str| str.parse::<DelayId>());
                            Ok((execution_id, version, join_set_id, delay_id))
                        },
                    )?
                    .collect::<Result<Vec<_>, _>>()?
                    ;
                let mut expired_timers: Vec<ExpiredTimer> = Vec::new();
                for(execution_id, version, join_set_id, delay_id) in query_res {
                    let execution_id = execution_id.map_err(|str| SqliteError::Parsing(str))?;
                    let expired_timer = match (join_set_id, delay_id) {
                        (Some(join_set_id), Some(delay_id)) => {
                            let join_set_id = join_set_id.map_err(|str| SqliteError::Parsing(str))?;
                            let delay_id = delay_id.map_err(|str| SqliteError::Parsing(str))?;
                            ExpiredTimer::AsyncDelay { execution_id, version, join_set_id, delay_id }
                        }
                        (None, None) => {
                            let created = Self::fetch_created_event(conn, execution_id)?;
                            let intermittent_event_count = Self::count_intermittent_events(conn, execution_id)?;
                            ExpiredTimer::Lock { execution_id, version, intermittent_event_count, max_retries: created.max_retries,
                                retry_exp_backoff: created.retry_exp_backoff, parent: created.parent }
                        }
                        _ => {
                            error!("invalid combination of `join_set_id`, `delay_id`");
                            return Err(SqliteError::DbError(DbError::Specific(
                                SpecificError::ConsistencyError(StrVariant::Static("invalid combination of `join_set_id`, `delay_id`")))));
                        }
                    };
                    expired_timers.push(expired_timer);
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
