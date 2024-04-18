#![allow(clippy::all, dead_code)]

use async_sqlite::{rusqlite::named_params, ClientBuilder, JournalMode, Pool, PoolBuilder};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{ExecutorId, RunId},
    storage::{
        AppendBatch, AppendBatchCreateChildResponse, AppendBatchResponse, AppendRequest,
        AppendResponse, CreateRequest, DbConnection, DbConnectionError, DbError, ExecutionEvent,
        ExecutionEventInner, ExecutionLog, ExpiredTimer, HistoryEvent, LockKind,
        LockPendingResponse, LockResponse, LockedExecution, PendingState, SpecificError, Version,
        DUMMY_CREATED, DUMMY_HISTORY_EVENT,
    },
    ExecutionId, FunctionFqn, StrVariant,
};
use rusqlite::{OptionalExtension, Transaction};
use std::{cmp::max, collections::VecDeque, path::Path, sync::Arc};
use tracing::{debug, error, info, instrument, trace, warn, Span};

const PRAGMA: &str = r"
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = true;
PRAGMA busy_timeout = 1000;
";

// TODO metadata table with current app version

const CREATE_TABLE_EXECUTION_LOG: &str = r"
CREATE TABLE IF NOT EXISTS execution_log (
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

const CREATE_TABLE_PENDING: &str = r"
CREATE TABLE IF NOT EXISTS pending (
    execution_id TEXT NOT NULL,
    expected_next_version INTEGER NOT NULL,
    pending_at TEXT NOT NULL,
    ffqn TEXT NOT NULL,
    PRIMARY KEY (execution_id)
)
"; // TODO: index by `pending_at` + `ffqn`

const CREATE_TABLE_LOCKED: &str = r"
CREATE TABLE IF NOT EXISTS locked (
    execution_id TEXT NOT NULL,
    expected_next_version INTEGER NOT NULL,
    join_set_id TEXT,
    delay_id TEXT,
    lock_expires_at TEXT NOT NULL,
    PRIMARY KEY (execution_id)
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

pub struct SqlitePool {
    pool: Pool,
}

impl SqlitePool {
    #[instrument(skip_all)]
    async fn init(pool: &Pool) -> Result<(), SqliteError> {
        pool.conn_with_err_and_span(
            |conn| {
                trace!("Executing `PRAGMA`");
                conn.execute(PRAGMA, [])?;
                trace!("Executing `CREATE_TABLE_EXECUTION_LOG`");
                conn.execute(CREATE_TABLE_EXECUTION_LOG, [])?;
                trace!("Executing `CREATE_TABLE_PENDING`");
                conn.execute(CREATE_TABLE_PENDING, [])?;
                trace!("Executing `CREATE_TABLE_LOCKED`");
                conn.execute(CREATE_TABLE_LOCKED, [])?;
                debug!("Done setting up sqlite");
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

    pub async fn close(&self) -> Result<(), SqliteError> {
        Ok(self.pool.close().await?)
    }

    fn current_version(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<Version, SqliteError> {
        let mut stmt = tx.prepare(
            "SELECT version FROM execution_log WHERE execution_id = :execution_id ORDER BY version DESC LIMIT 1",
        )?;
        Ok(stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| {
                    let version: usize = row.get(0)?;
                    Ok(Version::new(version))
                },
            )
            .map_err(async_sqlite::Error::Rusqlite)?)
    }

    fn check_expected_next_version(
        tx: &Transaction,
        execution_id: ExecutionId,
        expected_next_version: &Version,
    ) -> Result<(), SqliteError> {
        if Self::current_version(tx, execution_id).map(|ver| Version::new(ver.0 + 1))?
            != *expected_next_version
        {
            return Err(SqliteError::DbError(DbError::Specific(
                SpecificError::VersionMismatch,
            )));
        }
        Ok(())
    }

    #[instrument(skip_all, fields(execution_id = %req.execution_id))]
    fn create(tx: &Transaction, req: CreateRequest) -> Result<AppendResponse, SqliteError> {
        trace!("create");
        let version = Version::new(0);
        let execution_id = req.execution_id;
        let execution_id_str = execution_id.to_string();
        let mut stmt = tx.prepare(
                "INSERT INTO execution_log (execution_id, created_at, version, json_value, variant, pending_state ) \
                VALUES (:execution_id, :created_at, :version, :json_value, :variant, :pending_state)")?;
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

    fn update_index(
        tx: &Transaction,
        execution_id: ExecutionId,
        pending_state: &PendingState,
        next_version: &Version,
        purge: bool, // should the execution be deleted from `pending` and `locked`
        ffqn: Option<FunctionFqn>, // will be fetched from `Created` if required
    ) -> Result<(), SqliteError> {
        let execution_id_str = execution_id.to_string();
        if purge {
            let mut stmt =
                tx.prepare_cached("DELETE FROM pending WHERE execution_id = :execution_id")?;
            stmt.execute(named_params! {
                ":execution_id": execution_id_str,
            })?;

            let mut stmt =
                tx.prepare_cached("DELETE FROM locked WHERE execution_id = :execution_id")?;
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
                let mut stmt = tx.prepare(
                    "INSERT INTO pending (execution_id, expected_next_version, pending_at, ffqn) \
                    VALUES (:execution_id, :expected_next_version, :pending_at, :ffqn)",
                )?;
                let ffqn = if let Some(ffqn) = ffqn {
                    Ok(ffqn)
                } else {
                    let mut stmt = tx.prepare(
                        "SELECT json_value FROM execution_log WHERE \
                        execution_id = :execution_id AND (variant = :variant)",
                    )?;
                    let created = stmt.query_row(
                        named_params! {
                            ":execution_id": execution_id.to_string(),
                            ":variant": DUMMY_CREATED.variant(),
                        },
                        |row| {
                            let event = serde_json::from_value::<ExecutionEventInner>(
                                row.get::<_, serde_json::Value>(0)?,
                            )
                            .map_err(|serde| {
                                error!("cannot deserialize `Created` event: {row:?} - `{serde:?}`");
                                SqliteError::Parsing(StrVariant::Static("cannot deserialize"))
                            });
                            Ok(event)
                        },
                    )??;
                    if let ExecutionEventInner::Created { ffqn, .. } = created {
                        Ok(ffqn)
                    } else {
                        error!("Cannt match `Created` event - {created:?}");
                        Err(SqliteError::Parsing(StrVariant::Static(
                            "Cannot deserialize `Created` event",
                        )))
                    }
                }?;

                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":expected_next_version": next_version.0,
                    ":pending_at": scheduled_at.unwrap_or_default(),
                    ":ffqn": ffqn.to_string()
                })?;
                Ok(())
            }
            PendingState::Locked {
                lock_expires_at, ..
            } => {
                // Add to the `locked` table.
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO locked (execution_id, expected_next_version, lock_expires_at) \
                VALUES \
                (:execution_id, :expected_next_version, :lock_expires_at)",
                )?;
                stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":expected_next_version": next_version.0, // If the lock expires, this version will be used by `expired_timers_watcher`.
                    ":lock_expires_at": lock_expires_at,
                })?;
                Ok(())
            }
            PendingState::BlockedByJoinSet { .. } | PendingState::Finished => Ok(()),
        }
    }

    #[instrument(skip_all, fields(%execution_id))]
    fn current_pending_state(
        tx: &Transaction,
        execution_id: ExecutionId,
    ) -> Result<PendingState, SqliteError> {
        let mut stmt = tx.prepare(
            "SELECT pending_state FROM execution_log WHERE \
        execution_id = :execution_id AND pending_state IS NOT NULL \
        ORDER BY version DESC LIMIT 1",
        )?;
        let pending_state = stmt
            .query_row(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                },
                |row| row.get::<_, serde_json::Value>(0),
            )
            .optional()
            .map_err(async_sqlite::Error::Rusqlite)?
            .ok_or(SqliteError::DbError(DbError::Specific(
                // Every execution created must have at least Created pending state.
                SpecificError::NotFound,
            )))?;
        serde_json::from_value::<PendingState>(pending_state)
            .map_err(|serde| SqliteError::Parsing(StrVariant::Arc(Arc::from(serde.to_string()))))
    }

    #[instrument(skip_all, fields(%execution_id, %run_id, %executor_id))]
    fn lock(
        tx: &Transaction,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        appending_version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockedExecution, SqliteError> {
        trace!("lock");
        Self::check_expected_next_version(tx, execution_id, &appending_version)?;
        let pending_state = Self::current_pending_state(tx, execution_id)?;
        let lock_kind = pending_state
            .can_append_lock(created_at, executor_id, run_id, lock_expires_at)
            .map_err(DbError::Specific)?;

        // Remove from `pending` table, unless we are extending the lock.
        let execution_id_str = execution_id.to_string();
        match lock_kind {
            LockKind::CreatingNewLock => {
                let mut stmt = tx.prepare_cached(
            "DELETE FROM pending WHERE execution_id = :execution_id AND expected_next_version = :expected_next_version",
        )?;
                let deleted = stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":expected_next_version": appending_version.0, // This must be the version requested.
                })?;
                if deleted != 1 {
                    info!("Locking failed: expected to delete one `pending` row, actual number: {deleted}");
                    return Err(SqliteError::DbError(DbError::Specific(
                        SpecificError::VersionMismatch,
                    )));
                }
            }
            LockKind::Extending => {
                let mut stmt = tx.prepare_cached(
                    "DELETE FROM locked WHERE execution_id = :execution_id AND expected_next_version = :expected_next_version",
                )?;
                let deleted = stmt.execute(named_params! {
                    ":execution_id": execution_id_str,
                    ":expected_next_version": appending_version.0, // This must be the version requested.
                })?;
                if deleted != 1 {
                    info!("Locking failed: expected to delete one `locked` row, actual number: {deleted}");
                    return Err(SqliteError::DbError(DbError::Specific(
                        SpecificError::VersionMismatch,
                    )));
                }
            }
        }
        // Fetch event_history and `Created` event to construct the response.
        let mut stmt = tx.prepare(
            "SELECT json_value FROM execution_log WHERE \
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
                        row.get::<_, serde_json::Value>(0)?,
                    )
                    .map_err(|serde| {
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
        // Append to `execution_log` table.
        let event = ExecutionEventInner::Locked {
            executor_id,
            lock_expires_at,
            run_id,
        };
        let mut stmt = tx.prepare_cached(
            "INSERT INTO execution_log \
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
        })
    }

    #[allow(clippy::too_many_lines)]
    fn append(
        tx: &Transaction,
        execution_id: ExecutionId,
        req: AppendRequest,
        appending_version: Option<Version>,
    ) -> Result<AppendResponse, SqliteError> {
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
                let locked = Self::lock(
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
                Err(DbError::Specific(SpecificError::VersionMismatch))?
            }
            (event, appending_version) => {
                let appending_version = if let Some(appending_version) = appending_version {
                    Self::check_expected_next_version(&tx, execution_id, &appending_version)?;
                    appending_version
                } else {
                    Self::current_version(&tx, execution_id)?
                };
                let (pending_state, join_set_id) = {
                    match event {
                        ExecutionEventInner::IntermittentFailure { expires_at, .. }
                        | ExecutionEventInner::IntermittentTimeout { expires_at } => (
                            Some(PendingState::PendingAt {
                                scheduled_at: expires_at,
                            }),
                            None,
                        ),
                        ExecutionEventInner::Finished { .. } => {
                            (Some(PendingState::Finished), None)
                        }
                        ExecutionEventInner::HistoryEvent {
                            event: HistoryEvent::Yield,
                        } => (Some(PendingState::PendingNow), None),
                        ExecutionEventInner::HistoryEvent {
                            event:
                                HistoryEvent::JoinSet { join_set_id }
                                | HistoryEvent::JoinSetRequest { join_set_id, .. },
                        } => (None, Some(join_set_id)),
                        ExecutionEventInner::HistoryEvent {
                            event: HistoryEvent::Persist { .. },
                        } => (None, None),
                        ExecutionEventInner::HistoryEvent {
                            event:
                                HistoryEvent::JoinNext {
                                    join_set_id,
                                    lock_expires_at,
                                },
                        } => (
                            Some(PendingState::BlockedByJoinSet {
                                join_set_id,
                                lock_expires_at,
                            }),
                            Some(join_set_id),
                        ),
                        ExecutionEventInner::HistoryEvent {
                            event: HistoryEvent::JoinSetResponse { join_set_id, .. },
                        } => {
                            // Was the previous event with `pending_state` a JoinNext with this join set?
                            // FIXME: Get rid of this db call: Do not allow appending child responses without version,
                            // have a watcher that notifies unprocessed child responses, send next pending state inside of JoinSetResponse
                            let found_pending_state =
                                Self::current_pending_state(&tx, execution_id)?;
                            let pending_state = match found_pending_state {
                                PendingState::BlockedByJoinSet {
                                    join_set_id: found_join_set_id,
                                    lock_expires_at,
                                } if found_join_set_id == join_set_id => {
                                    // Unblocking the execution
                                    let scheduled_at = max(req.created_at, lock_expires_at);
                                    // The original executor can continue until the lock expires, but the execution is not marked as timed out
                                    Some(PendingState::PendingAt { scheduled_at })
                                }
                                _ => None,
                            };
                            (pending_state, Some(join_set_id))
                        }
                        ExecutionEventInner::CancelRequest => todo!(),
                        ExecutionEventInner::Locked { .. }
                        | ExecutionEventInner::Created { .. } => unreachable!("handled above"),
                    }
                };
                let mut stmt = tx.prepare(
                    "INSERT INTO execution_log (execution_id, created_at, json_value, version, variant, join_set_id, pending_state) \
                    VALUES (:execution_id, :created_at, :json_value, :version, :variant, :join_set_id, :pending_state)")?;
                stmt.execute(named_params!{
                    ":execution_id": execution_id.to_string(),
                    ":created_at": req.created_at,
                    ":json_value": serde_json::to_value(&event).unwrap(),
                    ":version": appending_version.0,
                    ":variant": event.variant(),
                    ":join_set_id": join_set_id.map(|join_set_id | join_set_id.to_string()),
                    ":pending_state": pending_state.map(|pending_state| serde_json::to_value(&pending_state).unwrap()),
                })?;
                let next_version = Version::new(appending_version.0 + 1);
                if let Some(pending_state) = pending_state {
                    Self::update_index(
                        &tx,
                        execution_id,
                        &pending_state,
                        &next_version,
                        true,
                        None,
                    )?;
                }
                Ok(next_version)
            }
        }
    }
}

#[async_trait]
impl DbConnection for SqlitePool {
    #[instrument(skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError> {
        self.pool
            .transaction_write_with_span(move |tx| Self::create(tx, req), Span::current())
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip_all, fields(%executor_id))]
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
        Ok(self
            .pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    trace!("tx started");
                    let mut execution_ids_versions = Vec::with_capacity(batch_size);
                    // TODO: The locked executions should be sorted by pending date, otherwise ffqns later in the list might get starved.
                    for ffqn in ffqns {
                        let mut stmt = tx.prepare(
                            "SELECT execution_id, expected_next_version FROM pending WHERE \
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
                                    let execution_id =
                                        row.get::<_, String>(0)?.parse::<ExecutionId>();
                                    let version = Version::new(row.get::<_, usize>(1)?);
                                    Ok(execution_id.map(|e| (e, version)))
                                },
                            )?
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|str| SqliteError::Parsing(StrVariant::Static(str)))?;
                        execution_ids_versions.extend(execs_and_versions);
                        if execution_ids_versions.len() == batch_size {
                            break;
                        }
                    }
                    let mut locked_execs = Vec::with_capacity(execution_ids_versions.len());
                    // Append lock
                    for (execution_id, version) in execution_ids_versions {
                        let locked = Self::lock(
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
                    trace!("tx committed");
                    Ok(locked_execs)
                },
                Span::current(),
            )
            .await?)
    }

    /// Specialized `append` which returns the event history.
    #[instrument(skip_all, fields(%execution_id, %run_id, %executor_id))]
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        self.pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    let locked = Self::lock(
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

    #[instrument(skip_all, fields(%execution_id))]
    async fn append(
        &self,
        execution_id: ExecutionId,
        appending_version: Option<Version>,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        // Disallow `Created` event
        if let ExecutionEventInner::Created { .. } = req.event {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("Cannot append `Created` event - use `create` instead"),
            )));
        }
        self.pool
            .transaction_write_with_span(
                move |tx| Self::append(tx, execution_id, req, appending_version),
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    async fn append_batch(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError> {
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

    async fn append_batch_create_child(
        &self,
        parent_req: (AppendBatch, ExecutionId, Version),
        child_req: CreateRequest,
    ) -> Result<AppendBatchCreateChildResponse, DbError> {
        if parent_req.0.is_empty() {
            error!("Empty batch request");
            return Err(DbError::Specific(SpecificError::ValidationFailed(
                StrVariant::Static("empty batch request"),
            )));
        }
        if parent_req
            .0
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
                    let mut parent_version = parent_req.2;
                    for req in parent_req.0 {
                        parent_version = Self::append(tx, parent_req.1, req, Some(parent_version))?;
                    }
                    let child_version = Self::create(tx, child_req)?;
                    Ok((parent_version, child_version))
                },
                Span::current(),
            )
            .await
            .map_err(DbError::from)
    }

    #[instrument(skip_all, fields(%execution_id))]
    async fn append_batch_respond_to_parent(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        parent: (ExecutionId, AppendRequest),
    ) -> Result<AppendBatchResponse, DbError> {
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

    #[instrument(skip_all, fields(%execution_id))]
    async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionLog, DbError> {
        self.pool
            .transaction_write_with_span::<_, _, SqliteError>(
                move |tx| {
                    let mut stmt = tx.prepare(
                        "SELECT created_at, json_value FROM execution_log WHERE \
                        execution_id = :execution_id ORDER BY version",
                    )?;
                    let events = stmt
                        .query_map(
                            named_params! {
                                ":execution_id": execution_id.to_string(),
                            },
                            |row| {
                                let created_at = row.get(0)?;
                                let event = serde_json::from_value::<ExecutionEventInner>(
                                    row.get::<_, serde_json::Value>(1)?,
                                )
                                .map(|event| ExecutionEvent { created_at, event })
                                .map_err(|serde| {
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
                    let version = Version::new(events.len());
                    let pending_state = Self::current_pending_state(tx, execution_id)?;
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
    async fn get_expired_timers(
        &self,
        _at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbConnectionError> {
        todo!()
    }
}

#[cfg(all(test, not(madsim)))] // async-sqlite attempts to spawn a system thread in simulation
mod tests {
    use super::SqlitePool;
    use db_tests::db_test_stubs::lifecycle;
    use tempfile::NamedTempFile;
    use test_utils::set_up;

    async fn pool() -> (SqlitePool, Option<NamedTempFile>) {
        if let Ok(path) = std::env::var("SQLITE_FILE") {
            (SqlitePool::new(path).await.unwrap(), None)
        } else {
            let file = NamedTempFile::new().unwrap();
            let path = file.path();
            (SqlitePool::new(path).await.unwrap(), Some(file))
        }
    }

    #[tokio::test]
    async fn check_sqlite_version() {
        test_utils::set_up();
        let (pool, _guard) = pool().await;
        let version = pool
            .pool
            .conn(|conn| conn.query_row("SELECT SQLITE_VERSION()", [], |row| row.get(0)))
            .await;
        let version: String = version.unwrap();
        assert_eq!("3.45.0", version);
        pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_lifecycle() {
        set_up();
        let (pool, _guard) = pool().await;

        lifecycle(&pool).await;
        pool.close().await.unwrap();
    }
}
