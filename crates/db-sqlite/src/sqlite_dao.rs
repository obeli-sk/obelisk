#![allow(clippy::all, dead_code)]

use async_sqlite::{ClientBuilder, JournalMode, Pool, PoolBuilder};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{ExecutorId, RunId},
    storage::{
        AppendBatch, AppendBatchResponse, AppendRequest, AppendResponse, AppendTxResponse,
        DbConnection, DbConnectionError, DbError, ExecutionLog, ExpiredTimer, LockPendingResponse,
        LockResponse, Version,
    },
    ExecutionId, FunctionFqn,
};
use std::{ops::Deref, path::Path};
use tracing::{debug, trace};

const PRAGMA: &str = r"
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = true;
PRAGMA busy_timeout = 1000;
";

const EXECUTION_LOG_INIT: &str = r"
CREATE TABLE IF NOT EXISTS execution_log (
    execution_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    json_value JSONB NOT NULL,
    version INTEGER,
    pending_at INTEGER,
    PRIMARY KEY (execution_id)
);
";

pub struct SqlitePool {
    pool: Pool,
}

impl SqlitePool {
    async fn init(pool: &Pool) -> Result<(), async_sqlite::Error> {
        pool.conn(|conn| {
            trace!("Executing `PRAGMA`");
            conn.execute(PRAGMA, [])?;
            trace!("Executing `EXECUTION_LOG_INIT`");
            conn.execute(EXECUTION_LOG_INIT, [])?;
            debug!("Done setting up sqlite");
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, async_sqlite::Error> {
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

    pub async fn close(&self) -> Result<(), async_sqlite::Error> {
        self.pool.close().await
    }
}

#[async_trait]
impl DbConnection for SqlitePool {
    async fn lock_pending(
        &self,
        _batch_size: usize,
        _pending_at_or_sooner: DateTime<Utc>,
        _ffqns: Vec<FunctionFqn>,
        _created_at: DateTime<Utc>,
        _executor_id: ExecutorId,
        _lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbConnectionError> {
        todo!()
    }

    /// Specialized `append` which returns the event history.
    async fn lock(
        &self,
        _created_at: DateTime<Utc>,
        _execution_id: ExecutionId,
        _run_id: RunId,
        _version: Version,
        _executor_id: ExecutorId,
        _lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        todo!()
    }

    async fn append(
        &self,
        _execution_id: ExecutionId,
        _version: Option<Version>,
        _req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        todo!()
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

impl Deref for SqlitePool {
    type Target = Pool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

#[cfg(all(test, not(madsim)))] // attempt to spawn a system thread in simulation
mod tests {
    use super::SqlitePool;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn check_sqlite_version() {
        test_utils::set_up();
        let file;
        let pool = {
            if let Ok(path) = std::env::var("SQLITE_FILE") {
                SqlitePool::new(path).await.unwrap()
            } else {
                file = NamedTempFile::new().unwrap();
                let path = file.path();
                SqlitePool::new(path).await.unwrap()
            }
        };
        let version = pool
            .conn(|conn| conn.query_row("SELECT SQLITE_VERSION()", [], |row| row.get(0)))
            .await;
        let version: String = version.unwrap();
        assert_eq!("3.45.0", version);
        pool.close().await.unwrap();
    }
}
