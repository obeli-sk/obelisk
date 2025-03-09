use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{
    prefixed_ulid::{ExecutionIdDerived, ExecutorId, RunId},
    storage::{
        AppendBacktrace, AppendBatchResponse, AppendRequest, AppendResponse, ClientError,
        CreateRequest, DbConnection, DbError, DbPool, ExecutionEvent, ExecutionListPagination,
        ExecutionLog, ExecutionWithState, ExpiredTimer, JoinSetResponseEvent,
        JoinSetResponseEventOuter, LockPendingResponse, LockResponse, Pagination, PendingState,
        ResponseWithCursor, Version, VersionType, WasmBacktrace,
    },
    time::TokioSleep,
    ComponentId, ExecutionId, FinishedExecutionResult, FunctionFqn,
};
use db_mem::inmemory_dao::InMemoryPool;
use db_sqlite::sqlite_dao::SqlitePool;

#[derive(Clone)]
pub enum DbPoolEnum {
    Memory(InMemoryPool),
    Sqlite(SqlitePool<TokioSleep>),
}

#[async_trait]
impl DbPool<DbConnectionProxy> for DbPoolEnum {
    fn connection(&self) -> DbConnectionProxy {
        match self {
            DbPoolEnum::Memory(pool) => DbConnectionProxy(Arc::new(pool.connection())),
            DbPoolEnum::Sqlite(pool) => DbConnectionProxy(Arc::new(pool.connection())),
        }
    }

    fn is_closing(&self) -> bool {
        match self {
            DbPoolEnum::Memory(pool) => pool.is_closing(),
            DbPoolEnum::Sqlite(pool) => pool.is_closing(),
        }
    }

    async fn close(&self) -> Result<(), DbError> {
        match self {
            DbPoolEnum::Memory(pool) => pool.close().await,
            DbPoolEnum::Sqlite(pool) => pool.close().await,
        }
    }
}

#[derive(Clone)]
pub struct DbConnectionProxy(Arc<dyn DbConnection>);
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
        component_id: ComponentId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError> {
        self.0
            .lock_pending(
                batch_size,
                pending_at_or_sooner,
                ffqns,
                created_at,
                component_id,
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
        component_id: ComponentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        self.0
            .lock(
                created_at,
                component_id,
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
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .append_batch(current_time, batch, execution_id, version)
            .await
    }

    async fn append_batch_create_new_execution(
        &self,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .append_batch_create_new_execution(
                current_time,
                batch,
                execution_id,
                version,
                child_req,
            )
            .await
    }

    async fn append_batch_respond_to_parent(
        &self,
        execution_id: ExecutionIdDerived,
        current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        version: Version,
        parent_execution_id: ExecutionId,
        parent_response_event: JoinSetResponseEventOuter,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .append_batch_respond_to_parent(
                execution_id,
                current_time,
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

    async fn get(&self, execution_id: &ExecutionId) -> Result<ExecutionLog, DbError> {
        self.0.get(execution_id).await
    }

    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError> {
        self.0
            .subscribe_to_next_responses(execution_id, start_idx)
            .await
    }

    async fn wait_for_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        max_wait: Duration,
    ) {
        self.0
            .wait_for_pending(pending_at_or_sooner, ffqns, max_wait)
            .await;
    }
    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout: Option<Duration>,
    ) -> Result<FinishedExecutionResult, ClientError> {
        self.0.wait_for_finished_result(execution_id, timeout).await
    }

    async fn get_pending_state(&self, execution_id: &ExecutionId) -> Result<PendingState, DbError> {
        self.0.get_pending_state(execution_id).await
    }

    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbError> {
        self.0.get_execution_event(execution_id, version).await
    }

    async fn append_backtrace(&self, batch: AppendBacktrace) -> Result<(), DbError> {
        self.0.append_backtrace(batch).await
    }

    async fn append_backtrace_batch(&self, batch: Vec<AppendBacktrace>) -> Result<(), DbError> {
        self.0.append_backtrace_batch(batch).await
    }

    async fn get_last_backtrace(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<WasmBacktrace, DbError> {
        self.0.get_last_backtrace(execution_id).await
    }

    async fn list_executions(
        &self,
        ffqn: Option<FunctionFqn>,
        pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbError> {
        self.0.list_executions(ffqn, pagination).await
    }

    async fn list_execution_events(
        &self,
        execution_id: &ExecutionId,
        since: &Version,
        max_length: VersionType,
    ) -> Result<Vec<ExecutionEvent>, DbError> {
        self.0
            .list_execution_events(execution_id, since, max_length)
            .await
    }

    async fn list_responses(
        &self,
        execution_id: &ExecutionId,
        pagination: Pagination<u32>,
    ) -> Result<Vec<ResponseWithCursor>, DbError> {
        self.0.list_responses(execution_id, pagination).await
    }
}
