use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{ExecutorId, RunId},
    storage::{
        AppendBatch, AppendBatchResponse, AppendRequest, AppendResponse, CreateRequest,
        DbConnection, DbError, DbPool, ExecutionLog, ExpiredTimer, LockPendingResponse,
        LockResponse, Version,
    },
    ExecutionId, FunctionFqn, StrVariant,
};
use db_mem::inmemory_dao::{DbTask, DbTaskHandle, InMemoryPool};
use db_sqlite::sqlite_dao::SqlitePool;
use tempfile::NamedTempFile;

pub enum Database {
    Memory,
    Sqlite,
}

pub enum DbGuard {
    Memory(DbTaskHandle),
    Sqlite(Option<NamedTempFile>),
}

impl DbGuard {
    pub async fn close(self, db_pool: DbPoolEnum) {
        drop(db_pool);
        match self {
            DbGuard::Memory(mut db_task) => {
                db_task.close().await;
            }
            DbGuard::Sqlite(_) => {}
        }
    }
}

impl Database {
    pub async fn set_up(self) -> (DbGuard, DbPoolEnum) {
        match self {
            Database::Memory => {
                let db_task = DbTask::spawn_new(1);
                let db_pool = db_task.pool().unwrap();
                (DbGuard::Memory(db_task), DbPoolEnum::Memory(db_pool))
            }
            Database::Sqlite => {
                use db_sqlite::sqlite_dao::tempfile::sqlite_pool;
                let (db_pool, guard) = sqlite_pool().await;
                (DbGuard::Sqlite(guard), DbPoolEnum::Sqlite(db_pool))
            }
        }
    }
}

#[derive(Clone)]
pub enum DbPoolEnum {
    Memory(InMemoryPool),
    Sqlite(SqlitePool),
}

#[async_trait]
impl DbPool<DbConnectionProxy> for DbPoolEnum {
    fn connection(&self) -> DbConnectionProxy {
        match self {
            DbPoolEnum::Memory(pool) => DbConnectionProxy(Box::new(pool.connection())),
            DbPoolEnum::Sqlite(pool) => DbConnectionProxy(Box::new(pool.connection())),
        }
    }

    async fn close(&self) -> Result<(), StrVariant> {
        match self {
            DbPoolEnum::Memory(pool) => pool.close().await,
            DbPoolEnum::Sqlite(pool) => pool.close().await,
        }
    }
}

pub struct DbConnectionProxy(Box<dyn DbConnection>);
#[async_trait]
impl DbConnection for DbConnectionProxy {
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError> {
        self.0.create(req).await
    }

    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError> {
        self.0
            .lock_pending(
                batch_size,
                pending_at_or_sooner,
                ffqns,
                created_at,
                executor_id,
                lock_expires_at,
            )
            .await
    }

    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError> {
        self.0.get_expired_timers(at).await
    }

    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        self.0
            .lock(
                created_at,
                execution_id,
                run_id,
                version,
                executor_id,
                lock_expires_at,
            )
            .await
    }

    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Option<Version>,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        self.0.append(execution_id, version, req).await
    }

    async fn append_batch(
        &self,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0.append_batch(batch, execution_id, version).await
    }

    async fn append_batch_create_child(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        child_req: CreateRequest,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .append_batch_create_child(batch, execution_id, version, child_req)
            .await
    }

    async fn append_batch_respond_to_parent(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        parent: (ExecutionId, AppendRequest),
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .append_batch_respond_to_parent(batch, execution_id, version, parent)
            .await
    }

    async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionLog, DbError> {
        self.0.get(execution_id).await
    }
}
