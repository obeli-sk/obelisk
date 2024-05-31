use concepts::FunctionFqn;
use db_mem::inmemory_dao::InMemoryPool;
use tempfile::NamedTempFile;

pub mod db_proxy;
pub use db_proxy::DbConnectionProxy;
pub use db_proxy::DbPoolEnum;

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
    pub async fn set_up(self) -> (DbGuard, DbPoolEnum) {
        match self {
            Database::Memory => (DbGuard::Memory, DbPoolEnum::Memory(InMemoryPool::new())),
            Database::Sqlite => {
                use db_sqlite::sqlite_dao::tempfile::sqlite_pool;
                let (db_pool, guard) = sqlite_pool().await;
                (DbGuard::Sqlite(guard), DbPoolEnum::Sqlite(db_pool))
            }
        }
    }
}
