use crate::{
    storage::{
        DbConnection, DbConnectionError, DbError, ExecutionEvent, ExecutionEventInner,
        ExecutorName, HistoryEvent, Version,
    },
    time::{now, now_tokio_instant},
    worker::{FatalError, Worker, WorkerError},
    FinishedExecutionError,
};
use chrono::{DateTime, TimeDelta, Utc};
use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::AbortHandle;
use tracing::{debug, enabled, info, info_span, instrument, trace, warn, Instrument, Level};

pub mod activity_worker;

pub struct ExecTask<ID: ExecutionId, DB: DbConnection<ID>, W: Worker<ID>> {
    db_connection: DB,
    worker: W,
    config: ExecConfig,
    _phantom_data: PhantomData<fn(ID) -> ID>,
}

#[derive(Debug)]
struct ExecTickRequest {
    executed_at: DateTime<Utc>,
}

#[derive(Debug)]
struct ExecutionProgress<ID: ExecutionId> {
    execution_id: ID,
    result: Result<Version, DbError>,
}

#[derive(Debug, Clone)]
pub struct ExecConfig {
    ffqns: Vec<FunctionFqn>,
    executor_name: ExecutorName,
    lock_expiry: Duration,
    max_tick_sleep: Duration,
    batch_size: usize,
    retry_exp_backoff: TimeDelta,
    max_retries: u32,
}

impl<ID: ExecutionId, DB: DbConnection<ID> + Sync, W: Worker<ID> + Send + Sync + 'static>
    ExecTask<ID, DB, W>
{
    pub fn spawn_new(db_connection: DB, worker: W, config: ExecConfig) -> ExecutorTaskHandle {
        let span = info_span!("executor", executor_name = %config.executor_name);
        let is_closing: Arc<AtomicBool> = Default::default();
        let is_closing_inner = is_closing.clone();
        let abort_handle = tokio::spawn(
            async move {
                let mut task = Self {
                    db_connection,
                    worker,
                    config,
                    _phantom_data: PhantomData,
                };
                let mut old_err = None;
                loop {
                    let sleep_until = now_tokio_instant() + task.config.max_tick_sleep;
                    let res = task.tick(ExecTickRequest { executed_at: now() }).await;
                    match (res, &old_err) {
                        (Ok(_), _) => {
                            old_err = None;
                        }
                        (Err(err), Some(old)) if err == *old => {}
                        (Err(err), _) => {
                            warn!("Tick failed: {err:?}");
                            old_err = Some(err);
                        }
                    }
                    if is_closing_inner.load(Ordering::Relaxed) {
                        return;
                    }
                    tokio::time::sleep_until(sleep_until).await;
                }
            }
            .instrument(span),
        )
        .abort_handle();
        ExecutorTaskHandle {
            abort_handle,
            is_closing,
        }
    }

    #[instrument(skip_all)]
    async fn tick(
        &mut self,
        request: ExecTickRequest,
    ) -> Result<Vec<ExecutionProgress<ID>>, DbConnectionError> {
        let lock_expires_at = request.executed_at + self.config.lock_expiry;
        let locked = self
            .db_connection
            .lock_pending(
                self.config.batch_size,
                request.executed_at, // fetch expiring before now
                self.config.ffqns.clone(),
                request.executed_at, // lock created at - now
                self.config.executor_name.clone(),
                lock_expires_at,
            )
            .await?;
        let mut executions = Vec::new();
        for (execution_id, version, ffqn, params, event_history, _) in locked {
            // TODO: concurrency
            let result = self
                .run_execution(
                    execution_id.clone(),
                    version,
                    ffqn,
                    params,
                    event_history,
                    lock_expires_at,
                )
                .await;
            executions.push(ExecutionProgress {
                execution_id,
                result,
            });
        }
        Ok(executions)
    }

    #[instrument(skip_all, fields(%execution_id, %ffqn))]
    async fn run_execution(
        &mut self,
        execution_id: ID,
        version: Version,
        ffqn: FunctionFqn,
        params: Params,
        event_history: Vec<HistoryEvent<ID>>,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<Version, DbError> {
        if enabled!(Level::TRACE) {
            trace!(?params, version, "Starting");
        } else {
            debug!("Starting");
        }
        let result = self
            .worker
            .run(
                execution_id.clone(),
                ffqn,
                params,
                event_history,
                version,
                lock_expires_at,
            )
            .await;
        if enabled!(Level::TRACE) {
            trace!(?result, version, "Finished");
        } else {
            debug!("Finished");
        }
        // Map the WorkerError to an intermittent or a permanent failure.
        match self
            .worker_result_to_execution_event(&execution_id, result)
            .await
        {
            Ok((event, version)) => {
                self.db_connection
                    .append(now(), execution_id.clone(), version, event)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn worker_result_to_execution_event(
        &self,
        execution_id: &ID,
        result: Result<(SupportedFunctionResult, Version), (WorkerError, Version)>,
    ) -> Result<(ExecutionEventInner<ID>, Version), DbError> {
        match result {
            Ok((result, version)) => Ok((
                ExecutionEventInner::Finished { result: Ok(result) },
                version,
            )),
            Err((err, new_version)) => {
                debug!("Execution failed: {err:?}");
                let (event_history, received_version) =
                    self.db_connection.get(execution_id.clone()).await?;
                if received_version != new_version {
                    return Err(DbError::RowSpecific(
                        crate::storage::RowSpecificError::VersionMismatch,
                    ));
                }
                match err {
                    WorkerError::IntermittentError { reason, err: _ } => {
                        if let Some(expires_at) = self.can_be_retried(&event_history) {
                            debug!("Retrying execution at {expires_at}");
                            Ok((
                                ExecutionEventInner::IntermittentFailure { expires_at, reason },
                                new_version,
                            ))
                        } else {
                            info!("Marking execution as permanently failed");
                            Ok((
                                ExecutionEventInner::Finished {
                                    result: Err(FinishedExecutionError::UncategorizedError(reason)),
                                },
                                new_version,
                            ))
                        }
                    }
                    WorkerError::FatalError(FatalError::NonDeterminismDetected(reason)) => Ok((
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::NonDeterminismDetected(reason)),
                        },
                        new_version,
                    )),
                }
            }
        }
    }

    fn can_be_retried(&self, event_history: &Vec<ExecutionEvent<ID>>) -> Option<DateTime<Utc>> {
        let already_retried_count = event_history
            .iter()
            .filter(|event| event.event.is_retry())
            .count() as u32;
        if already_retried_count < self.config.max_retries {
            let duration =
                self.config.retry_exp_backoff * 2_i32.saturating_pow(already_retried_count);
            let expires_at = crate::time::now() + duration;
            Some(expires_at)
        } else {
            None
        }
    }
}

pub struct ExecutorTaskHandle {
    is_closing: Arc<AtomicBool>,
    abort_handle: AbortHandle,
}

impl ExecutorTaskHandle {
    pub async fn close(&self) {
        trace!("Gracefully closing");
        self.is_closing.store(true, Ordering::Relaxed);
        while !self.abort_handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        info!("Gracefully closed");
    }
}

impl Drop for ExecutorTaskHandle {
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!("Aborting the task");
        self.abort_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::{
            inmemory_dao::{tests::TickBasedDbConnection, DbTask},
            DbConnection, ExecutionEvent, ExecutionEventInner, HistoryEvent,
        },
        time::now,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, Params, SupportedFunctionResult};
    use std::{future::Future, sync::Arc};
    use tracing::info;
    use tracing_unwrap::{OptionExt, ResultExt};

    fn set_up() {
        crate::testing::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    struct SimpleWorker<DB: DbConnection<WorkflowId> + Send> {
        db_connection: DB,
    }

    #[async_trait]
    impl<DB: DbConnection<WorkflowId> + Sync> Worker<WorkflowId> for SimpleWorker<DB> {
        async fn run(
            &mut self,
            _execution_id: WorkflowId,
            ffqn: FunctionFqn,
            _params: Params,
            events: Vec<HistoryEvent<WorkflowId>>,
            version: Version,
            _lock_expires_at: DateTime<Utc>,
        ) -> Result<(SupportedFunctionResult, Version), (WorkerError, Version)> {
            assert_eq!(SOME_FFQN, ffqn);
            assert!(events.is_empty());
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok((SupportedFunctionResult::None, version))
        }
    }

    async fn tick_fn<DB: DbConnection<WorkflowId> + Clone + Sync>(
        db_connection: DB,
        config: ExecConfig,
    ) {
        let mut executor = ExecTask {
            db_connection: db_connection.clone(),
            worker: SimpleWorker {
                db_connection: db_connection.clone(),
            },
            config,
            _phantom_data: Default::default(),
        };
        let _ = executor.tick(ExecTickRequest { executed_at: now() }).await;
    }

    #[tokio::test]
    async fn stochastic_execute_tick_based() {
        set_up();
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            executor_name: Arc::new("exec1".to_string()),
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            max_tick_sleep: Duration::from_millis(500),
            max_retries: 0,
            retry_exp_backoff: TimeDelta::zero(),
        };
        let db_task = Arc::new(std::sync::Mutex::new(DbTask::new()));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        execute(db_connection, exec_config, tick_fn).await;
    }

    #[tokio::test]
    async fn stochastic_execute_executor_tick_db_task() {
        set_up();
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            executor_name: Arc::new("exec1".to_string()),
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            max_tick_sleep: Duration::from_millis(500),
            max_retries: 0,
            retry_exp_backoff: TimeDelta::zero(),
        };
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().unwrap_or_log();
        execute(db_connection, exec_config, tick_fn).await;
        db_task.close().await;
    }

    #[tokio::test]
    async fn stochastic_execute_task_based() {
        set_up();
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            executor_name: Arc::new("exec1".to_string()),
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            max_tick_sleep: Duration::from_millis(500),
            max_retries: 0,
            retry_exp_backoff: TimeDelta::zero(),
        };

        let mut db_task = DbTask::spawn_new(1);
        let worker = SimpleWorker {
            db_connection: db_task.as_db_connection().unwrap_or_log(),
        };
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            worker,
            exec_config.clone(),
        );
        execute(
            db_task.as_db_connection().unwrap_or_log(),
            exec_config,
            |_, _| tokio::time::sleep(Duration::from_secs(1)),
        )
        .await;
        exec_task.close().await;
        db_task.close().await;
    }

    async fn execute<
        DB: DbConnection<WorkflowId> + Clone + Sync,
        T: FnMut(DB, ExecConfig) -> F,
        F: Future<Output = ()>,
    >(
        db_connection: DB,
        exec_config: ExecConfig,
        mut tick: T,
    ) {
        info!("Now: {}", now());
        tick(db_connection.clone(), exec_config.clone()).await;
        // Create an execution
        let execution_id = WorkflowId::generate();
        db_connection
            .create(
                now(),
                execution_id.clone(),
                SOME_FFQN.to_owned(),
                Params::default(),
                None,
                None,
            )
            .await
            .unwrap_or_log();

        // execute!
        let requested_execution_at = now();
        tick(db_connection.clone(), exec_config.clone()).await;
        // check that DB contains the result.
        let (history, _) = db_connection.get(execution_id).await.unwrap_or_log();
        assert_eq!(3, history.len(), "Unexpected {history:?}");
        assert_matches!(
            history[0],
            ExecutionEvent {
                event: ExecutionEventInner::Created { .. },
                ..
            }
        );
        assert_matches!(
            history[1],
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                ..
            }
        );
        assert_matches!(history[2], ExecutionEvent {
            created_at: executed_at,
            event: ExecutionEventInner::Finished {
                result: Ok(SupportedFunctionResult::None),
            },
        } if executed_at >= requested_execution_at);
    }
}
