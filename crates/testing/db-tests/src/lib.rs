use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{ExecutorId, RunId},
    storage::{
        AppendBatchResponse, AppendRequest, AppendResponse, Component, ComponentWithMetadata,
        CreateRequest, DbConnection, DbError, DbPool, ExecutionEventInner, ExecutionLog,
        ExpiredTimer, JoinSetResponseEvent, JoinSetResponseEventOuter, LockPendingResponse,
        LockResponse, Version,
    },
    ComponentId, ExecutionId, FunctionFqn,
};
use db_mem::inmemory_dao::InMemoryPool;
use db_sqlite::sqlite_dao::SqlitePool;
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
