use crate::{
    storage::{
        DbConnection, DbConnectionError, DbError, ExecutionEvent, ExecutionEventInner,
        ExecutorName, HistoryEvent, Version,
    },
    time::{now, now_tokio_instant},
    worker::{FatalError, Worker, WorkerError, WorkerResult},
    FinishedExecutionError,
};
use chrono::{DateTime, TimeDelta, Utc};
use concepts::{ExecutionId, FunctionFqn, Params};
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

pub struct ExecTask<ID: ExecutionId, DB: DbConnection<ID>, W: Worker<ID>> {
    db_connection: DB,
    worker: W,
    config: ExecConfig,
    task_limiter: Arc<tokio::sync::Semaphore>,
    _phantom_data: PhantomData<fn(ID) -> ID>,
}

#[derive(Debug)]
struct ExecTickRequest {
    executed_at: DateTime<Utc>,
}

struct ExecutionProgress<ID: ExecutionId> {
    execution_id: ID,
    abort_handle: AbortHandle,
}

#[derive(Debug, Clone)]
pub struct ExecConfig {
    ffqns: Vec<FunctionFqn>,
    executor_name: ExecutorName,
    lock_expiry: Duration,
    max_tick_sleep: Duration,
    batch_size: u32,
    retry_exp_backoff: TimeDelta,
    max_retries: u32,
}

impl<ID: ExecutionId, DB: DbConnection<ID> + Sync, W: Worker<ID> + Send + Sync + 'static>
    ExecTask<ID, DB, W>
{
    pub fn spawn_new(
        db_connection: DB,
        worker: W,
        config: ExecConfig,
        task_limiter: Arc<tokio::sync::Semaphore>,
    ) -> ExecutorTaskHandle {
        let span = info_span!("executor", executor_name = %config.executor_name);
        let is_closing: Arc<AtomicBool> = Default::default();
        let is_closing_inner = is_closing.clone();
        let abort_handle = tokio::spawn(
            async move {
                let mut task = Self {
                    db_connection,
                    worker,
                    config,
                    task_limiter,
                    _phantom_data: PhantomData,
                };
                let mut old_err = None;
                loop {
                    let sleep_until = now_tokio_instant() + task.config.max_tick_sleep;
                    let res = task.tick(ExecTickRequest { executed_at: now() }).await;
                    Self::log_err_if_new(res, &mut old_err);
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

    fn log_err_if_new(
        res: Result<Vec<ExecutionProgress<ID>>, DbConnectionError>,
        old_err: &mut Option<DbConnectionError>,
    ) {
        match (res, &old_err) {
            (Ok(_), _) => {
                *old_err = None;
            }
            (Err(err), Some(old)) if err == *old => {}
            (Err(err), _) => {
                warn!("Tick failed: {err:?}");
                *old_err = Some(err);
            }
        }
    }

    async fn acquire_task_permits(&self) -> Vec<tokio::sync::OwnedSemaphorePermit> {
        let mut locks = Vec::new();
        for _ in 0..self.config.batch_size {
            if let Ok(permit) = self.task_limiter.clone().acquire_owned().await {
                locks.push(permit);
            }
        }
        locks
    }

    #[instrument(skip_all)]
    async fn tick(
        &mut self,
        request: ExecTickRequest,
    ) -> Result<Vec<ExecutionProgress<ID>>, DbConnectionError> {
        let lock_expires_at = request.executed_at + self.config.lock_expiry;
        let permits = self.acquire_task_permits().await;
        if permits.is_empty() {
            return Ok(vec![]);
        }
        let locked_executions = self
            .db_connection
            .lock_pending(
                permits.len(),
                request.executed_at, // fetch expiring before now
                self.config.ffqns.clone(),
                request.executed_at, // lock created at - now
                self.config.executor_name.clone(),
                lock_expires_at,
            )
            .await?
            .into_iter()
            .zip(permits);

        let mut executions = Vec::new();
        for ((execution_id, version, ffqn, params, event_history, _), permit) in locked_executions {
            // TODO: timeouts, clean shutdown
            let abort_handle = {
                let execution_id = execution_id.clone();
                let worker = self.worker.clone();
                let db_connection = self.db_connection.clone();
                let max_retries = self.config.max_retries;
                let retry_exp_backoff = self.config.retry_exp_backoff;
                let span = info_span!("worker", %execution_id, %ffqn,);
                tokio::spawn(
                    async move {
                        if let Err(err) = Self::run_worker(
                            worker,
                            db_connection,
                            execution_id,
                            version,
                            ffqn,
                            params,
                            event_history,
                            lock_expires_at,
                            max_retries,
                            retry_exp_backoff,
                        )
                        .await
                        {
                            info!("Execution failed: {err:?}");
                        }
                        drop(permit);
                    }
                    .instrument(span),
                )
                .abort_handle()
            };
            executions.push(ExecutionProgress {
                execution_id,
                abort_handle,
            });
        }
        Ok(executions)
    }

    #[instrument(skip_all, fields(%execution_id, %ffqn))]
    async fn run_worker(
        worker: W,
        db_connection: DB,
        execution_id: ID,
        version: Version,
        ffqn: FunctionFqn,
        params: Params,
        event_history: Vec<HistoryEvent<ID>>,
        lock_expires_at: DateTime<Utc>,
        max_retries: u32,
        retry_exp_backoff: TimeDelta,
    ) -> Result<Version, DbError> {
        if enabled!(Level::TRACE) {
            trace!(?params, version, "Starting");
        } else {
            debug!("Starting");
        }
        let worker_result = worker
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
            trace!(?worker_result, version, "Finished");
        } else {
            debug!("Finished");
        }
        // Map the WorkerError to an intermittent or a permanent failure.
        match Self::worker_result_to_execution_event(
            &db_connection,
            &execution_id,
            worker_result,
            max_retries,
            retry_exp_backoff,
        )
        .await
        {
            Ok((event, version)) => {
                db_connection
                    .append(now(), execution_id.clone(), version, event)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn worker_result_to_execution_event(
        db_connection: &DB,
        execution_id: &ID,
        worker_result: WorkerResult,
        max_retries: u32,
        retry_exp_backoff: TimeDelta,
    ) -> Result<(ExecutionEventInner<ID>, Version), DbError> {
        match worker_result {
            Ok((result, version)) => Ok((
                ExecutionEventInner::Finished { result: Ok(result) },
                version,
            )),
            Err((err, new_version)) => {
                debug!("Execution failed: {err:?}");
                let (event_history, received_version) =
                    db_connection.get(execution_id.clone()).await?; // This can come from a cache
                if received_version != new_version {
                    return Err(DbError::RowSpecific(
                        crate::storage::RowSpecificError::VersionMismatch,
                    ));
                }
                match err {
                    WorkerError::IntermittentError { reason, err: _ } => {
                        if let Some(expires_at) =
                            Self::can_be_retried(&event_history, max_retries, retry_exp_backoff)
                        {
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

    fn can_be_retried(
        event_history: &Vec<ExecutionEvent<ID>>,
        max_retries: u32,
        retry_exp_backoff: TimeDelta,
    ) -> Option<DateTime<Utc>> {
        let already_retried_count = event_history
            .iter()
            .filter(|event| event.event.is_retry())
            .count() as u32;
        if already_retried_count < max_retries {
            let duration = retry_exp_backoff * 2_i32.saturating_pow(already_retried_count);
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
        worker::WorkerResult,
    };
    use anyhow::anyhow;
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, Params, SupportedFunctionResult};
    use indexmap::IndexMap;
    use std::{borrow::Cow, future::Future, sync::Arc};
    use tracing::info;
    use tracing_unwrap::{OptionExt, ResultExt};

    fn set_up() {
        crate::testing::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");
    type SimpleWorkerResultMap =
        Arc<std::sync::Mutex<IndexMap<Version, (Vec<HistoryEvent<WorkflowId>>, WorkerResult)>>>;

    #[derive(Clone)]
    struct SimpleWorker<DB: DbConnection<WorkflowId> + Send> {
        db_connection: DB,
        worker_results_rev: SimpleWorkerResultMap,
    }

    #[async_trait]
    impl<DB: DbConnection<WorkflowId> + Sync> Worker<WorkflowId> for SimpleWorker<DB> {
        async fn run(
            &self,
            _execution_id: WorkflowId,
            ffqn: FunctionFqn,
            _params: Params,
            events: Vec<HistoryEvent<WorkflowId>>,
            version: Version,
            _lock_expires_at: DateTime<Utc>,
        ) -> WorkerResult {
            assert_eq!(SOME_FFQN, ffqn);
            let (expected_version, (expected_eh, worker_result)) = self
                .worker_results_rev
                .lock()
                .unwrap_or_log()
                .pop()
                .unwrap_or_log();
            assert_eq!(expected_version, version);
            assert_eq!(expected_eh, events);
            worker_result
        }
    }

    async fn tick_fn<DB: DbConnection<WorkflowId> + Sync>(
        db_connection: DB,
        config: ExecConfig,
        worker_results_rev: SimpleWorkerResultMap,
    ) {
        let mut executor = ExecTask {
            db_connection: db_connection.clone(),
            worker: SimpleWorker {
                db_connection: db_connection.clone(),
                worker_results_rev,
            },
            config,
            task_limiter: Arc::new(tokio::sync::Semaphore::new(1)),
            _phantom_data: Default::default(),
        };
        let mut execution_progress_vec = executor
            .tick(ExecTickRequest { executed_at: now() })
            .await
            .unwrap_or_log();
        loop {
            execution_progress_vec.retain(|progress| !progress.abort_handle.is_finished());
            if execution_progress_vec.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
        let worker_results_rev = {
            let finished_result: WorkerResult = Ok((SupportedFunctionResult::None, 2));
            let mut worker_results_rev = IndexMap::from([(2, (vec![], finished_result))]);
            worker_results_rev.reverse();
            Arc::new(std::sync::Mutex::new(worker_results_rev))
        };
        let execution_history =
            execute(db_connection, exec_config, worker_results_rev, tick_fn).await;
        assert_matches!(
            execution_history[2],
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: _,
            }
        );
    }

    #[tokio::test]
    async fn stochastic_execute_executor_tick_based_db_task_based() {
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
        let worker_results_rev = {
            let finished_result: WorkerResult = Ok((SupportedFunctionResult::None, 2));
            let mut worker_results_rev = IndexMap::from([(2, (vec![], finished_result))]);
            worker_results_rev.reverse();
            Arc::new(std::sync::Mutex::new(worker_results_rev))
        };
        let execution_history =
            execute(db_connection, exec_config, worker_results_rev, tick_fn).await;
        db_task.close().await;
        assert_matches!(
            execution_history[2],
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: _,
            }
        );
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
        let worker_results_rev = {
            let finished_result: WorkerResult = Ok((SupportedFunctionResult::None, 2));
            let mut worker_results_rev = IndexMap::from([(2, (vec![], finished_result))]);
            worker_results_rev.reverse();
            Arc::new(std::sync::Mutex::new(worker_results_rev))
        };
        let worker = SimpleWorker {
            db_connection: db_task.as_db_connection().unwrap_or_log(),
            worker_results_rev: worker_results_rev.clone(),
        };
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            worker,
            exec_config.clone(),
            Arc::new(tokio::sync::Semaphore::new(1)),
        );
        let execution_history = execute(
            db_task.as_db_connection().unwrap_or_log(),
            exec_config,
            worker_results_rev,
            |_, _, _| tokio::time::sleep(Duration::from_secs(1)),
        )
        .await;
        exec_task.close().await;
        db_task.close().await;
        assert_matches!(
            execution_history[2],
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: _,
            }
        );
    }

    async fn execute<
        DB: DbConnection<WorkflowId> + Clone + Sync,
        T: FnMut(DB, ExecConfig, SimpleWorkerResultMap) -> F,
        F: Future<Output = ()>,
    >(
        db_connection: DB,
        exec_config: ExecConfig,
        worker_results_rev: SimpleWorkerResultMap,
        mut tick: T,
    ) -> Vec<ExecutionEvent<WorkflowId>> {
        info!("Now: {}", now());
        tick(
            db_connection.clone(),
            exec_config.clone(),
            worker_results_rev.clone(),
        )
        .await;
        // Create an execution
        let execution_id = WorkflowId::generate();
        let created_at = now();
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                SOME_FFQN.to_owned(),
                Params::default(),
                None,
                None,
            )
            .await
            .unwrap_or_log();

        // execute!
        tick(
            db_connection.clone(),
            exec_config.clone(),
            worker_results_rev.clone(),
        )
        .await;
        let (execution_history, _) = db_connection.get(execution_id).await.unwrap_or_log();
        // check that DB contains Created and Locked events.
        assert_matches!(
            execution_history[0],
            ExecutionEvent {
                event: ExecutionEventInner::Created { .. },
                created_at: actually_created_at,
            }
            if created_at == actually_created_at
        );
        let last_created_at = assert_matches!(
            execution_history[1],
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: updated_at
            } if created_at <= updated_at
             => updated_at
        );
        assert_matches!(execution_history[2], ExecutionEvent {
            event: _,
            created_at: executed_at,
        } if executed_at >= last_created_at);
        execution_history
    }

    #[tokio::test]
    async fn stochastic_test_failure_retry_success() {
        set_up();
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            executor_name: Arc::new("exec1".to_string()),
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            max_tick_sleep: Duration::from_millis(500),
            max_retries: 1,
            retry_exp_backoff: TimeDelta::zero(),
        };

        let mut db_task = DbTask::spawn_new(1);
        let worker_results_rev = {
            let failed_result: WorkerResult = Err((
                WorkerError::IntermittentError {
                    reason: Cow::Borrowed("fail"),
                    err: anyhow!("").into(),
                },
                2,
            ));
            let finished_result: WorkerResult = Ok((SupportedFunctionResult::None, 4));

            let mut worker_results_rev = IndexMap::new();
            worker_results_rev.insert(2, (vec![], failed_result));
            worker_results_rev.insert(4, (vec![], finished_result));
            worker_results_rev.reverse();
            Arc::new(std::sync::Mutex::new(worker_results_rev))
        };
        let worker = SimpleWorker {
            db_connection: db_task.as_db_connection().unwrap_or_log(),
            worker_results_rev: worker_results_rev.clone(),
        };
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            worker,
            exec_config.clone(),
            Arc::new(tokio::sync::Semaphore::new(1)),
        );
        let execution_history = execute(
            db_task.as_db_connection().unwrap_or_log(),
            exec_config,
            worker_results_rev,
            |_, _, _| tokio::time::sleep(Duration::from_secs(1)),
        )
        .await;
        exec_task.close().await;
        db_task.close().await;
        assert_matches!(
            &execution_history[2],
            ExecutionEvent {
                event: ExecutionEventInner::IntermittentFailure {
                    reason,
                    expires_at: _
                },
                created_at: _,
            } if *reason == Cow::Borrowed("fail")
        );
        assert_matches!(
            execution_history[3],
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: _
            }
        );
        assert_matches!(
            execution_history[4],
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: _,
            }
        );
    }
}
