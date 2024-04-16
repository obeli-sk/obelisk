#![allow(clippy::all, dead_code)]

use async_sqlite::{rusqlite::named_params, ClientBuilder, JournalMode, Pool, PoolBuilder};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{ExecutorId, RunId},
    storage::{
        AppendBatch, AppendBatchResponse, AppendRequest, AppendResponse, AppendTxResponse,
        CreateRequest, DbConnection, DbConnectionError, DbError, ExecutionEventInner, ExecutionLog,
        ExpiredTimer, LockPendingResponse, LockResponse, LockedExecution, SpecificError, Version,
        DUMMPY_HISTORY_EVENT, DUMMY_CREATED,
    },
    ExecutionId, FunctionFqn, StrVariant,
};
use rusqlite::Transaction;
use std::{collections::VecDeque, path::Path, sync::Arc};
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
            "SELECT version FROM execution_log WHERE \
        execution_id = :execution_id AND version IS NOT NULL \
        ORDER BY version DESC LIMIT 1",
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
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, SqliteError> {
        trace!("create");
        self.pool.transaction_write_with_span::<_,_,SqliteError>(move |tx| {
            let version = Version::new(0);
            let execution_id = req.execution_id.to_string();
            let mut stmt = tx.prepare(
                "INSERT INTO execution_log (execution_id, created_at, version, json_value, variant ) \
                VALUES (:execution_id, :created_at, :version, :json_value, :variant)")?;
            let ffqn_str = req.ffqn.to_string();
            let created_at = req.created_at;
            let scheduled_at = req.scheduled_at;
            let event = ExecutionEventInner::from(req);
            stmt.execute(named_params!{
                ":execution_id": &execution_id,
                ":created_at": created_at,
                ":version": version.0,
                ":json_value": serde_json::to_value(&event).unwrap(),
                ":variant": event.variant(),
            })?;
            let next_expected_version = Version::new(version.0 + 1);
            let mut stmt = tx.prepare(
                "INSERT INTO pending (execution_id, expected_next_version, pending_at, ffqn) \
                VALUES (:execution_id, :expected_next_version, :pending_at, :ffqn)")?;
            stmt.execute(named_params!{
                ":execution_id": &execution_id,
                ":expected_next_version": next_expected_version.0,
                ":pending_at": scheduled_at.unwrap_or_default(),
                ":ffqn": ffqn_str
            })?;
            Ok(next_expected_version)
        }, Span::current(),)
        .await
    }

    fn update_index(tx: &Transaction, execution_id: ExecutionId) -> Result<(), SqliteError> {
        let execution_id_str = execution_id.to_string();
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

        Ok(())
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
        // Remove from `pending` table.
        let execution_id_str = execution_id.to_string();
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
        // Add to the `locked` table.
        let mut stmt = tx.prepare_cached(
            "INSERT INTO locked (execution_id, expected_next_version, lock_expires_at) \
            VALUES \
            (:execution_id, :expected_next_version, :lock_expires_at)",
        )?;
        let returned_version = Version::new(appending_version.0 + 1);
        stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":expected_next_version": returned_version.0, // If the lock expires, this version will be used by `expired_timers_watcher`.
            ":lock_expires_at": lock_expires_at,
        })?;
        // Fetch `execution_log`
        let mut stmt = tx.prepare(
            "SELECT json_value FROM execution_log WHERE \
        execution_id = :execution_id AND (variant = :v1 OR variant = :v2) \
        ORDER BY created_at",
        )?;
        let mut events = stmt
            .query_map(
                named_params! {
                    ":execution_id": execution_id.to_string(),
                    ":v1": DUMMY_CREATED.variant(),
                    ":v2": DUMMPY_HISTORY_EVENT.variant(),
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
            .map(|event| match event {
                ExecutionEventInner::HistoryEvent { event } => Ok(event),
                _ => {
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
            (execution_id, created_at, json_value, version, variant) \
            VALUES \
            (:execution_id, :created_at, :json_value, :version, :variant)",
        )?;
        stmt.execute(named_params! {
            ":execution_id": execution_id_str,
            ":created_at": created_at,
            ":json_value": serde_json::to_value(&event).unwrap(),
            ":version": appending_version.0,
            ":variant": event.variant()
        })?;
        Ok(LockedExecution {
            execution_id,
            run_id,
            version: returned_version,
            ffqn,
            params,
            event_history,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
        })
    }
}

#[async_trait]
impl DbConnection for SqlitePool {
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
        Ok(self
            .pool
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
            .await?)
    }

    #[instrument(skip_all, fields(%execution_id))]
    async fn append(
        &self,
        execution_id: ExecutionId,
        appending_version: Option<Version>,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        let created_at = req.created_at;
        match (req.event, appending_version) {
            (ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
        }, None) =>
        {
            let req = CreateRequest {
                created_at,
                execution_id,
                ffqn,
                params,
                parent,
                scheduled_at,
                retry_exp_backoff,
                max_retries,
            };
            return Ok(self.create(req).await?);
        },
        (ExecutionEventInner::Created{..}, Some(_)) => {
            warn!("Attempted to append `Created` with version");
            Err(DbError::Specific(SpecificError::VersionMismatch))
        },

        (ExecutionEventInner::Locked { executor_id, run_id, lock_expires_at }, Some(version)) => {
            self.lock(created_at, execution_id, run_id, version, executor_id, lock_expires_at).await.map(|locked|locked.1)
        }
        (event, None) if !event.appendable_without_version() => {
            warn!("Attempted to append an event without version: {event:?}");
            Err(DbError::Specific(SpecificError::VersionMismatch))
        }
        (event, appending_version) => Ok(self
            .pool
            .transaction_write_with_span::<_,_,SqliteError>(move |tx| {
                let appending_version = if let Some(appending_version) = appending_version {
                    Self::check_expected_next_version(&tx, execution_id, &appending_version)?;
                    appending_version
                } else {
                    Self::current_version(&tx, execution_id)?
                };
                let mut stmt = tx.prepare(
                    "INSERT INTO execution_log (execution_id, created_at, json_value, version, variant) \
                    VALUES (:execution_id, :created_at, :json_value, :version, :variant)")?;
                stmt.execute(named_params!{
                    ":execution_id": execution_id.to_string(),
                    ":created_at": created_at,
                    ":json_value": serde_json::to_value(&event).unwrap(),
                    ":version": appending_version.0,
                    ":variant": event.variant(),
                })?;
                Self::update_index(&tx, execution_id)?;
                Ok(Version::new(appending_version.0 + 1))
            }, Span::current(),).await?)
        }
    }

    async fn append_batch(
        &self,
        _batch: AppendBatch,
        _execution_id: ExecutionId,
        _version: Option<Version>,
    ) -> Result<AppendBatchResponse, DbError> {
        todo!()
    }

    async fn append_tx(
        &self,
        _items: Vec<(AppendBatch, ExecutionId, Option<Version>)>,
    ) -> Result<AppendTxResponse, DbError> {
        todo!()
    }

    async fn get(&self, _execution_id: ExecutionId) -> Result<ExecutionLog, DbError> {
        todo!()
    }

    /// Get currently expired locks and async timers (delay requests)
    async fn get_expired_timers(
        &self,
        _at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbConnectionError> {
        todo!()
    }
}

#[cfg(all(test, not(madsim)))] // attempt to spawn a system thread in simulation
mod tests {
    use super::SqlitePool;
    use concepts::{storage::CreateRequest, ExecutionId, Params};
    use db_tests::db_test_stubs::{lifecycle, SOME_FFQN};
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use test_utils::set_up;
    use utils::time::now;

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
    async fn append() {
        set_up();
        let (pool, _guard) = pool().await;
        pool.create(CreateRequest {
            created_at: now(),
            execution_id: ExecutionId::generate(),
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();
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
