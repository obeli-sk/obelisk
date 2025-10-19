use concepts::FunctionFqn;
use concepts::storage::DbExecutor;
use concepts::storage::DbPool;
use db_mem::inmemory_dao::InMemoryPool;
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
    pub async fn set_up(self) -> (DbGuard, Arc<dyn DbPool>, Arc<dyn DbExecutor>) {
        match self {
            Database::Memory => {
                let mem = InMemoryPool::new();
                let exec = mem.db_executor();
                (DbGuard::Memory, Arc::new(mem), exec)
            }
            Database::Sqlite => {
                use db_sqlite::sqlite_dao::tempfile::sqlite_pool;
                let (db_pool, guard) = sqlite_pool().await;
                (
                    DbGuard::Sqlite(guard),
                    Arc::new(db_pool.clone()),
                    Arc::new(db_pool),
                )
            }
        }
    }
}
