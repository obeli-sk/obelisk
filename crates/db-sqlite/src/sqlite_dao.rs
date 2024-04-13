#![allow(clippy::all, dead_code)]

use async_sqlite::{JournalMode, Pool, PoolBuilder};
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

const PRAGMA: &str = r"
PRAGMA synchronous = NORMAL;
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
            conn.execute(PRAGMA, [])?;
            conn.execute(EXECUTION_LOG_INIT, [])?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, async_sqlite::Error> {
        let pool = PoolBuilder::new()
            .path(path)
            .journal_mode(JournalMode::Wal)
            .open()
            .await?;
        Self::init(&pool).await?;
        Ok(Self { pool })
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
    use tempfile::NamedTempFile;

    use super::SqlitePool;

    #[tokio::test]
    async fn check_sqlite_version() {
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
        let version: String = pool
            .conn(|conn| conn.query_row("SELECT SQLITE_VERSION()", [], |row| row.get(0)))
            .await
            .unwrap();
        assert_eq!("3.45.0", version);
    }
}
