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
    ComponentId, ExecutionId, FunctionFqn, FunctionMetadata,
};
use db_mem::inmemory_dao::InMemoryPool;
use db_sqlite::sqlite_dao::SqlitePool;

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

    async fn close(&self) -> Result<(), DbError> {
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
        ffqns: Arc<[FunctionFqn]>,
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
        version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        self.0.append(execution_id, version, req).await
    }

    async fn append_batch(
        &self,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .append_batch(created_at, batch, execution_id, version)
            .await
    }

    async fn append_batch_create_child(
        &self,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .append_batch_create_child(created_at, batch, execution_id, version, child_req)
            .await
    }

    async fn append_batch_respond_to_parent(
        &self,
        execution_id: ExecutionId,
        created_at: DateTime<Utc>,
        batch: Vec<ExecutionEventInner>,
        version: Version,
        parent_execution_id: ExecutionId,
        parent_response_event: JoinSetResponseEvent,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .append_batch_respond_to_parent(
                execution_id,
                created_at,
                batch,
                version,
                parent_execution_id,
                parent_response_event,
            )
            .await
    }

    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbError> {
        self.0
            .append_response(created_at, execution_id, response_event)
            .await
    }

    async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionLog, DbError> {
        self.0.get(execution_id).await
    }

    async fn subscribe_to_next_responses(
        &self,
        execution_id: ExecutionId,
        start_idx: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError> {
        self.0
            .subscribe_to_next_responses(execution_id, start_idx)
            .await
    }

    async fn subscribe_to_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        max_wait: Duration,
    ) {
        self.0
            .subscribe_to_pending(pending_at_or_sooner, ffqns, max_wait)
            .await;
    }

    async fn append_component(
        &self,
        created_at: DateTime<Utc>,
        component: ComponentWithMetadata,
        replace: bool,
    ) -> Result<Vec<ComponentId>, DbError> {
        self.0
            .append_component(created_at, component, replace)
            .await
    }

    async fn list_components(&self, active: bool) -> Result<Vec<Component>, DbError> {
        self.0.list_components(active).await
    }

    async fn get_component_metadata(
        &self,
        component_id: ComponentId,
    ) -> Result<ComponentWithMetadata, DbError> {
        self.0.get_component_metadata(component_id).await
    }

    async fn get_exported_function(&self, ffqn: FunctionFqn) -> Result<FunctionMetadata, DbError> {
        self.0.get_exported_function(ffqn).await
    }

    async fn component_deactivate(&self, id: ComponentId) -> Result<(), DbError> {
        self.0.component_deactivate(id).await
    }

    async fn component_activate(&self, id: ComponentId) -> Result<(), DbError> {
        self.0.component_activate(id).await
    }
}
