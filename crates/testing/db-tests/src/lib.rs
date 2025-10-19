use async_trait::async_trait;
use concepts::FunctionFqn;
use concepts::storage::DbError;
use concepts::storage::DbExecutor;
use concepts::storage::DbPool;
use concepts::storage::DbPoolCloseable;
use concepts::time::TokioSleep;
use db_mem::inmemory_dao::InMemoryPool;
use db_sqlite::sqlite_dao::SqlitePool;
use std::sync::Arc;
use tempfile::NamedTempFile;

pub const SOME_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");

#[derive(Clone, Copy, Debug)]
pub enum Database {
    Memory,
    Sqlite,
}

pub enum DbGuard {
    Memory,
    Sqlite(Option<NamedTempFile>),
}

impl Database {
    pub async fn set_up(
        self,
    ) -> (
        DbGuard,
        Arc<dyn DbPool>,
        Arc<dyn DbExecutor>,
        DbPoolCloseableWrapper,
    ) {
        match self {
            Database::Memory => {
                let mem_db = InMemoryPool::new();
                let exec = mem_db.db_executor();
                let closeable = DbPoolCloseableWrapper::Memory(mem_db.clone());
                (DbGuard::Memory, Arc::new(mem_db), exec, closeable)
            }
            Database::Sqlite => {
                use db_sqlite::sqlite_dao::tempfile::sqlite_pool;
                let (sqlite, guard) = sqlite_pool().await;
                let closeable = DbPoolCloseableWrapper::Sqlite(sqlite.clone());
                (
                    DbGuard::Sqlite(guard),
                    Arc::new(sqlite.clone()),
                    Arc::new(sqlite),
                    closeable,
                )
            }
        }
    }
}

pub enum DbPoolCloseableWrapper {
    Memory(InMemoryPool),
    Sqlite(SqlitePool<TokioSleep>),
}

#[async_trait]
impl DbPoolCloseable for DbPoolCloseableWrapper {
    async fn close(self) -> Result<(), DbError> {
        match self {
            DbPoolCloseableWrapper::Memory(db) => db.close().await,
            DbPoolCloseableWrapper::Sqlite(db) => db.close().await,
        }
    }
}
