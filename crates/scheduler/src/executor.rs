use crate::worker::{FatalError, Worker, WorkerError, WorkerResult};
use chrono::{DateTime, Utc};
use concepts::{prefixed_ulid::ExecutorId, ExecutionId, FunctionFqn, Params};
use db::{
    storage::{
        AppendRequest, AsyncResponse, DbConnection, DbConnectionError, DbError,
        ExecutionEventInner, ExecutorName, HistoryEvent, SpecificError, Version,
    },
    ExecutionHistory, FinishedExecutionError,
};
use derivative::Derivative;
use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::AbortHandle;
use tracing::{debug, info, info_span, instrument, trace, warn, Instrument};
use utils::time::now;

#[derive(Debug, Clone)]
pub struct ExecConfig<C: Fn() -> DateTime<Utc>> {
    pub ffqns: Vec<FunctionFqn>,
    pub lock_expiry: Duration,
    pub lock_expiry_leeway: Duration,
    pub tick_sleep: Duration,
    pub batch_size: u32,
    pub cleanup_expired_locks: bool,
    pub clock_fn: C, // Used for obtaining current time when the execution finishes.
}

pub struct ExecTask<DB: DbConnection, W: Worker, C: Fn() -> DateTime<Utc> + Send + Clone> {
    db_connection: DB,
    worker: W,
    config: ExecConfig<C>,
    task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    executor_name: ExecutorName,
}

#[derive(Debug)]
struct ExecTickRequest {
    executed_at: DateTime<Utc>,
}

#[allow(dead_code)] // allowed for testing
#[derive(Derivative, Default)]
#[derivative(Debug)]
struct ExecutionProgress {
    #[derivative(Debug = "ignore")]
    executions: Vec<(ExecutionId, AbortHandle)>,
    expired_locks: Option<usize>,
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

impl<DB: DbConnection, W: Worker, C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static>
    ExecTask<DB, W, C>
{
    pub fn spawn_new(
        db_connection: DB,
        worker: W,
        config: ExecConfig<C>,
        task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> ExecutorTaskHandle {
        let executor_id = ExecutorId::generate();
        let span = info_span!("executor",
            executor = %executor_id,
            worker = worker.as_value()
        );
        let is_closing: Arc<AtomicBool> = Default::default();
        let is_closing_inner = is_closing.clone();
        let tick_sleep = config.tick_sleep;
        let abort_handle = tokio::spawn(
            async move {
                info!("Spawned executor");
                let task = Self {
                    db_connection,
                    worker,
                    config,
                    task_limiter,
                    executor_name: Arc::new(executor_id.to_string()),
                };
                let mut old_err = None;
                loop {
                    let res = task.tick(ExecTickRequest { executed_at: now() }).await;
                    Self::log_err_if_new(res, &mut old_err);
                    if is_closing_inner.load(Ordering::Relaxed) {
                        return;
                    }
                    tokio::time::sleep(tick_sleep).await;
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
        res: Result<ExecutionProgress, DbConnectionError>,
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

    fn acquire_task_permits(&self) -> Vec<Option<tokio::sync::OwnedSemaphorePermit>> {
        match &self.task_limiter {
            Some(task_limiter) => {
                let mut locks = Vec::new();
                for _ in 0..self.config.batch_size {
                    if let Ok(permit) = task_limiter.clone().try_acquire_owned() {
                        locks.push(Some(permit));
                    } else {
                        break;
                    }
                }
                locks
            }
            None => {
                let mut vec = Vec::with_capacity(self.config.batch_size as usize);
                for _ in 0..self.config.batch_size {
                    vec.push(None);
                }
                vec
            }
        }
    }

    #[instrument(skip_all)]
    async fn tick(&self, request: ExecTickRequest) -> Result<ExecutionProgress, DbConnectionError> {
        let locked_executions = {
            let mut permits = self.acquire_task_permits();
            if permits.is_empty() {
                return Ok(ExecutionProgress::default());
            }
            let lock_expires_at = request.executed_at + self.config.lock_expiry;
            let locked_executions = self
                .db_connection
                .lock_pending(
                    permits.len(),       // batch size
                    request.executed_at, // fetch expiring before now
                    self.config.ffqns.clone(),
                    request.executed_at, // created at
                    self.executor_name.clone(),
                    lock_expires_at,
                )
                .await?;
            // Drop permits if too many were allocated.
            while permits.len() > locked_executions.len() {
                permits.pop();
            }
            assert_eq!(permits.len(), locked_executions.len());
            locked_executions.into_iter().zip(permits)
        };
        let execution_deadline =
            request.executed_at + self.config.lock_expiry - self.config.lock_expiry_leeway;

        let mut executions = Vec::new();
        for (locked_execution, permit) in locked_executions {
            let execution_id = locked_execution.execution_id.clone();
            let join_handle = {
                let execution_id = execution_id.clone();
                let worker = self.worker.clone();
                let db_connection = self.db_connection.clone();
                let clock_fn = self.config.clock_fn.clone();
                let run_id = locked_execution.run_id;
                let span =
                    info_span!("worker", %execution_id, %run_id, ffqn = %locked_execution.ffqn,);
                // TODO: wait for termination of all spawned tasks in `close`.
                tokio::spawn(
                    async move {
                        let res = Self::run_worker(
                            worker,
                            db_connection,
                            execution_id,
                            locked_execution.version,
                            locked_execution.ffqn,
                            locked_execution.params,
                            locked_execution.event_history,
                            execution_deadline,
                            clock_fn,
                        )
                        .await;
                        if let Err(err) = res {
                            info!("Execution failed: {err:?}");
                        }
                        drop(permit);
                    }
                    .instrument(span),
                )
            };
            executions.push((execution_id, join_handle.abort_handle()));
        }
        let expired_locks = if self.config.cleanup_expired_locks {
            self.db_connection
                .cleanup_expired_locks(request.executed_at)
                .await
                .inspect_err(|err| warn!("Failed to clean up expired locks: {err:?}"))
                .ok()
        } else {
            None
        };
        Ok(ExecutionProgress {
            executions,
            expired_locks,
        })
    }

    #[instrument(skip_all, fields(%execution_id, %ffqn))]
    async fn run_worker(
        worker: W,
        db_connection: DB,
        execution_id: ExecutionId,
        version: Version,
        ffqn: FunctionFqn,
        params: Params,
        event_history: Vec<HistoryEvent>,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
    ) -> Result<(), DbError> {
        trace!(%version, ?params, ?event_history, "Starting");
        let worker_result = worker
            .run(
                execution_id.clone(),
                ffqn,
                params,
                event_history,
                version,
                execution_deadline,
            )
            .await;
        trace!(?worker_result, "Finished");
        let execution_history = db_connection.get(execution_id.clone()).await?;
        match Self::worker_result_to_execution_event(
            execution_id.clone(),
            worker_result,
            execution_history,
            clock_fn(),
        ) {
            Ok(append) => {
                db_connection
                    .append_batch(
                        append.primary_events,
                        append.execution_id,
                        Some(append.version),
                    )
                    .await?;
                if let Some((secondary_id, secondary_append_request)) = append.secondary {
                    db_connection
                        .append(secondary_id, None, secondary_append_request)
                        .await?;
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Map the WorkerError to an intermittent or a permanent failure.
    fn worker_result_to_execution_event(
        execution_id: ExecutionId,
        worker_result: WorkerResult,
        execution_history: ExecutionHistory,
        created_at: DateTime<Utc>,
    ) -> Result<Append, DbError> {
        Ok(match worker_result {
            Ok((result, new_version)) => {
                let finished_res;
                let event = if let Some(exec_err) = result.fallible_err() {
                    info!("Execution finished with an error result");
                    let reason =
                        Cow::Owned(format!("Execution returned error result: `{exec_err:?}`"));
                    if let Some(duration) = execution_history.can_be_retried_after() {
                        let expires_at = created_at + duration;
                        debug!("Retrying failed execution after {duration:?} at {expires_at}");
                        finished_res = None;
                        ExecutionEventInner::IntermittentFailure { expires_at, reason }
                    } else {
                        info!("Permanently failed");
                        finished_res = Some(Err(FinishedExecutionError::PermanentFailure(
                            reason.clone(),
                        )));
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::PermanentFailure(reason)),
                        }
                    }
                } else {
                    info!("Execution finished successfully");
                    finished_res = Some(Ok(result.clone()));
                    ExecutionEventInner::Finished { result: Ok(result) }
                };
                let finished_event = AppendRequest { created_at, event };
                let secondary = if let (Some(finished_res), Some((parent_id, join_set_id))) =
                    (finished_res, execution_history.parent())
                {
                    Some((
                        parent_id,
                        AppendRequest {
                            created_at,
                            event: ExecutionEventInner::HistoryEvent {
                                event: HistoryEvent::AsyncResponse {
                                    join_set_id,
                                    response: AsyncResponse::ChildExecutionAsyncResponse {
                                        child_execution_id: execution_id.clone(),
                                        result: finished_res,
                                    },
                                },
                            },
                        },
                    ))
                } else {
                    None
                };
                Append {
                    primary_events: vec![finished_event],
                    execution_id,
                    version: new_version,
                    secondary,
                }
            }
            Err((err, new_version)) => {
                if matches!(err, WorkerError::Interrupt(_)) {
                    // logged later
                } else {
                    debug!("Execution failed: {err:?}");
                }
                if execution_history.version != new_version {
                    return Err(DbError::Specific(SpecificError::VersionMismatch));
                }
                let event = match err {
                    WorkerError::Interrupt(request) => {
                        let join_set_id = request.new_join_set_id.clone();
                        let join = AppendRequest {
                            created_at,
                            event: ExecutionEventInner::HistoryEvent {
                                event: HistoryEvent::JoinSet {
                                    join_set_id: join_set_id.clone(),
                                },
                            },
                        };
                        let child_exec_async_req = AppendRequest {
                            created_at,
                            event: ExecutionEventInner::HistoryEvent {
                                event: HistoryEvent::ChildExecutionAsyncRequest {
                                    join_set_id: join_set_id.clone(),
                                    child_execution_id: request.child_execution_id.into(),
                                },
                            },
                        };
                        let block = AppendRequest {
                            created_at,
                            event: ExecutionEventInner::HistoryEvent {
                                event: HistoryEvent::JoinNextBlocking {
                                    join_set_id: join_set_id.clone(),
                                },
                            },
                        };
                        let child_execution_id = ExecutionId::generate();
                        info!(%child_execution_id, "Interrupted, scheduling child execution");
                        let child_exec = AppendRequest {
                            created_at,
                            event: ExecutionEventInner::Created {
                                ffqn: request.ffqn,
                                params: request.params,
                                parent: Some((execution_id.clone(), join_set_id)),
                                scheduled_at: None,
                                retry_exp_backoff: execution_history.retry_exp_backoff(),
                                max_retries: execution_history.max_retries(),
                            },
                        };
                        return Ok(Append {
                            primary_events: vec![join, child_exec_async_req, block],
                            execution_id,
                            version: new_version,
                            secondary: Some((child_execution_id, child_exec)),
                        });
                    }
                    WorkerError::IntermittentError { reason, err: _ } => {
                        if let Some(duration) = execution_history.can_be_retried_after() {
                            let expires_at = created_at + duration;
                            debug!("Retrying failed execution after {duration:?} at {expires_at}");
                            ExecutionEventInner::IntermittentFailure { expires_at, reason }
                        } else {
                            info!("Permanently failed");
                            ExecutionEventInner::Finished {
                                result: Err(FinishedExecutionError::PermanentFailure(reason)),
                            }
                        }
                    }
                    WorkerError::LimitReached(reason) => {
                        warn!("Limit reached: {reason}, yielding");
                        ExecutionEventInner::HistoryEvent {
                            event: HistoryEvent::Yield,
                        }
                    }
                    WorkerError::IntermittentTimeout { .. } => {
                        if let Some(duration) = execution_history.can_be_retried_after() {
                            let expires_at = created_at + duration;
                            debug!(
                                "Retrying timed out execution after {duration:?} at {expires_at}"
                            );
                            ExecutionEventInner::IntermittentTimeout { expires_at }
                        } else {
                            info!("Permanently timed out");
                            ExecutionEventInner::Finished {
                                result: Err(FinishedExecutionError::PermanentTimeout),
                            }
                        }
                    }
                    WorkerError::FatalError(FatalError::NonDeterminismDetected(reason)) => {
                        info!("Non-determinism detected");
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::NonDeterminismDetected(reason)),
                        }
                    }
                    WorkerError::FatalError(FatalError::NotFound) => {
                        info!("Not found");
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::PermanentFailure(Cow::Borrowed(
                                "not found",
                            ))),
                        }
                    }
                    WorkerError::FatalError(FatalError::ParamsParsingError(err)) => {
                        info!("Error parsing parameters");
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::PermanentFailure(Cow::Owned(
                                format!("error parsing parameters: {err:?}"),
                            ))),
                        }
                    }
                    WorkerError::FatalError(FatalError::ResultParsingError(err)) => {
                        info!("Error parsing result");
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::PermanentFailure(Cow::Owned(
                                format!("error parsing result: {err:?}"),
                            ))),
                        }
                    }
                };
                Append {
                    primary_events: vec![AppendRequest { created_at, event }],
                    execution_id,
                    version: new_version,
                    secondary: None,
                }
            }
        })
    }
}

struct Append {
    primary_events: Vec<AppendRequest>,
    execution_id: ExecutionId,
    version: Version,
    secondary: Option<(ExecutionId, AppendRequest)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::WorkerResult;
    use anyhow::anyhow;
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{FunctionFqnStr, Params, SupportedFunctionResult};
    use db::storage::{
        inmemory_dao::{tick::TickBasedDbConnection, DbTask},
        journal::PendingState,
        DbConnection, ExecutionEvent, ExecutionEventInner, HistoryEvent,
    };
    use indexmap::IndexMap;
    use std::{borrow::Cow, fmt::Debug, future::Future, sync::Arc};
    use tracing_unwrap::{OptionExt, ResultExt};
    use utils::time::now;

    fn set_up() {
        test_utils::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");
    type SimpleWorkerResultMap =
        Arc<std::sync::Mutex<IndexMap<Version, (Vec<HistoryEvent>, WorkerResult)>>>;

    #[derive(Clone, Debug)]
    struct SimpleWorker {
        worker_results_rev: SimpleWorkerResultMap,
    }

    impl valuable::Valuable for SimpleWorker {
        fn as_value(&self) -> valuable::Value<'_> {
            "SimpleWorker".as_value()
        }

        fn visit(&self, _visit: &mut dyn valuable::Visit) {}
    }

    #[async_trait]
    impl Worker for SimpleWorker {
        async fn run(
            &self,
            _execution_id: ExecutionId,
            ffqn: FunctionFqn,
            _params: Params,
            eh: Vec<HistoryEvent>,
            version: Version,
            _execution_deadline: DateTime<Utc>,
        ) -> WorkerResult {
            assert_eq!(SOME_FFQN, ffqn);
            let (expected_version, (expected_eh, worker_result)) = self
                .worker_results_rev
                .lock()
                .unwrap_or_log()
                .pop()
                .unwrap_or_log();
            trace!(%expected_version, %version, ?expected_eh, ?eh, "Running SimpleWorker");
            assert_eq!(expected_version, version);
            assert_eq!(expected_eh, eh);
            worker_result
        }
    }

    async fn tick_fn<
        DB: DbConnection,
        W: Worker + Debug,
        C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static,
    >(
        db_connection: DB,
        config: ExecConfig<C>,
        worker: W,
        executed_at: DateTime<Utc>,
    ) {
        trace!("Ticking with {worker:?}");
        let executor = ExecTask {
            db_connection: db_connection.clone(),
            worker,
            config,
            task_limiter: None,
            executor_name: Arc::new("SimpleWorker".to_string()),
        };
        let mut execution_progress = executor
            .tick(ExecTickRequest { executed_at })
            .await
            .unwrap_or_log();
        loop {
            execution_progress
                .executions
                .retain(|(_, abort_handle)| !abort_handle.is_finished());
            if execution_progress.executions.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn execute_tick_based() {
        set_up();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(100),
            tick_sleep: Duration::ZERO,
            cleanup_expired_locks: false,
            clock_fn: clock_fn.clone(),
        };
        let db_connection = TickBasedDbConnection {
            db_task: Arc::new(std::sync::Mutex::new(DbTask::new())),
        };
        let worker_results_rev = {
            let finished_result: WorkerResult =
                Ok((SupportedFunctionResult::None, Version::new(2)));
            let mut worker_results_rev =
                IndexMap::from([(Version::new(2), (vec![], finished_result))]);
            worker_results_rev.reverse();
            Arc::new(std::sync::Mutex::new(worker_results_rev))
        };
        let execution_history = create_and_tick(
            created_at,
            db_connection,
            exec_config,
            SimpleWorker { worker_results_rev },
            0,
            created_at,
            tick_fn,
        )
        .await;
        assert_matches!(
            execution_history.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: finished_at,
            } if created_at == *finished_at
        );
    }

    #[tokio::test]
    async fn stochastic_execute_executor_tick_based_db_task_based() {
        set_up();
        let clock_fn = || now();
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(100),
            tick_sleep: Duration::from_millis(100),
            cleanup_expired_locks: false,
            clock_fn: clock_fn.clone(),
        };
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");
        let worker_results_rev = {
            let finished_result: WorkerResult =
                Ok((SupportedFunctionResult::None, Version::new(2)));
            let mut worker_results_rev =
                IndexMap::from([(Version::new(2), (vec![], finished_result))]);
            worker_results_rev.reverse();
            Arc::new(std::sync::Mutex::new(worker_results_rev))
        };
        let created_at = now();
        let execution_history = create_and_tick(
            created_at,
            db_connection,
            exec_config,
            SimpleWorker { worker_results_rev },
            0,
            now(),
            tick_fn,
        )
        .await;
        db_task.close().await;
        assert_matches!(
            execution_history.events.get(2).unwrap(),
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
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(100),
            tick_sleep: Duration::ZERO,
            cleanup_expired_locks: false,
            clock_fn: || now(),
        };

        let mut db_task = DbTask::spawn_new(1);
        let worker_results_rev = {
            let finished_result: WorkerResult =
                Ok((SupportedFunctionResult::None, Version::new(2)));
            let mut worker_results_rev =
                IndexMap::from([(Version::new(2), (vec![], finished_result))]);
            worker_results_rev.reverse();
            Arc::new(std::sync::Mutex::new(worker_results_rev))
        };
        let worker = SimpleWorker {
            worker_results_rev: worker_results_rev.clone(),
        };
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            worker.clone(),
            exec_config.clone(),
            None,
        );
        let created_at = now();
        let execution_history = create_and_tick(
            created_at,
            db_task.as_db_connection().unwrap_or_log(),
            exec_config,
            worker,
            0,
            now(),
            |_, _, _, _| tokio::time::sleep(Duration::from_secs(1)),
        )
        .await;
        exec_task.close().await;
        db_task.close().await;
        assert_matches!(
            execution_history.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: _,
            }
        );
    }

    async fn create_and_tick<
        DB: DbConnection,
        W: Worker,
        C: Fn() -> DateTime<Utc> + Send + Clone + 'static,
        T: FnMut(DB, ExecConfig<C>, W, DateTime<Utc>) -> F,
        F: Future<Output = ()>,
    >(
        created_at: DateTime<Utc>,
        db_connection: DB,
        exec_config: ExecConfig<C>,
        worker: W,
        max_retries: u32,
        executed_at: DateTime<Utc>,
        mut tick: T,
    ) -> ExecutionHistory {
        // Create an execution
        let execution_id = ExecutionId::generate();
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                SOME_FFQN.to_owned(),
                Params::default(),
                None,
                None,
                Duration::ZERO,
                max_retries,
            )
            .await
            .unwrap_or_log();
        // execute!
        tick(
            db_connection.clone(),
            exec_config.clone(),
            worker,
            executed_at,
        )
        .await;
        let execution_history = db_connection.get(execution_id).await.unwrap_or_log();
        debug!("Execution history after tick: {execution_history:?}");
        // check that DB contains Created and Locked events.
        assert_matches!(
            execution_history.events.get(0).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Created { .. },
                created_at: actually_created_at,
            }
            if created_at == *actually_created_at
        );
        let locked_at = assert_matches!(
            execution_history.events.get(1).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: locked_at
            } if created_at <= *locked_at
             => *locked_at
        );
        assert_matches!(execution_history.events.get(2).unwrap(), ExecutionEvent {
            event: _,
            created_at: executed_at,
        } if *executed_at >= locked_at);
        execution_history
    }

    #[tokio::test]
    async fn worker_error_should_trigger_an_execution_retry() {
        set_up();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(100),
            tick_sleep: Duration::ZERO,
            cleanup_expired_locks: false,
            clock_fn: clock_fn,
        };
        let db_connection = TickBasedDbConnection {
            db_task: Arc::new(std::sync::Mutex::new(DbTask::new())),
        };
        let worker = SimpleWorker {
            worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(2),
                (
                    vec![],
                    Err((
                        WorkerError::IntermittentError {
                            reason: Cow::Borrowed("fail"),
                            err: anyhow!("").into(),
                        },
                        Version::new(2),
                    )),
                ),
            )]))),
        };
        let execution_history = create_and_tick(
            created_at,
            db_connection.clone(),
            exec_config.clone(),
            worker,
            1,
            created_at,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_history.events.len());
        assert_matches!(
            &execution_history.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::IntermittentFailure {
                    reason,
                    expires_at: _ // TODO: check
                },
                created_at: at,
            } if *reason == Cow::Borrowed("fail") && *at == created_at
        );
        // tick again to finish the execution
        let worker = SimpleWorker {
            worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(4),
                (vec![], Ok((SupportedFunctionResult::None, Version::new(4)))),
            )]))),
        };
        tick_fn(db_connection.clone(), exec_config, worker, created_at).await;
        let execution_history = db_connection
            .get(execution_history.execution_id)
            .await
            .unwrap_or_log();
        debug!("Execution history after second tick: {execution_history:?}");
        assert_matches!(
            execution_history.events.get(3).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: at
            } if *at == created_at
        );
        assert_matches!(
            execution_history.events.get(4).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: finished_at,
            } if *finished_at == created_at
        );
    }

    #[tokio::test]
    async fn execution_returning_err_should_be_retried() {
        set_up();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(100),
            tick_sleep: Duration::ZERO,
            cleanup_expired_locks: false,
            clock_fn,
        };
        let db_connection = TickBasedDbConnection {
            db_task: Arc::new(std::sync::Mutex::new(DbTask::new())),
        };
        let worker = SimpleWorker {
            worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(2),
                (
                    vec![],
                    Ok((
                        SupportedFunctionResult::Fallible(
                            val_json::wast_val::WastVal::Result(Err(None)),
                            Err(()),
                        ),
                        Version::new(2),
                    )),
                ),
            )]))),
        };
        let execution_history = create_and_tick(
            created_at,
            db_connection.clone(),
            exec_config.clone(),
            worker,
            1,
            created_at,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_history.events.len());
        let reason = assert_matches!(
            &execution_history.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::IntermittentFailure {
                    reason,
                    expires_at: _
                },
                created_at: at,
            } if *at == created_at
             => reason.to_string()
        );
        assert_eq!("Execution returned error result: `None`", reason);
        // tick again to finish the execution
        let worker = SimpleWorker {
            worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(4),
                (
                    vec![],
                    Ok((
                        SupportedFunctionResult::Fallible(
                            val_json::wast_val::WastVal::Result(Ok(None)),
                            Ok(()),
                        ),
                        Version::new(4),
                    )),
                ),
            )]))),
        };
        tick_fn(db_connection.clone(), exec_config, worker, created_at).await;
        let execution_history = db_connection
            .get(execution_history.execution_id)
            .await
            .unwrap_or_log();
        debug!("Execution history after second tick: {execution_history:?}");

        assert_eq!(5, execution_history.events.len());
        assert_matches!(
            execution_history.events.get(3).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: locked_at
            }  if *locked_at == created_at
        );
        assert_matches!(
            execution_history.events.get(4).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::Fallible(
                        val_json::wast_val::WastVal::Result(Ok(None)),
                        Ok(())
                    )),
                },
                created_at: finished_at,
            } if *finished_at == created_at
        );
    }

    #[derive(Clone, Debug)]
    struct SleepyWorker {
        duration: Duration,
        result: SupportedFunctionResult,
    }

    impl valuable::Valuable for SleepyWorker {
        fn as_value(&self) -> valuable::Value<'_> {
            "SleepyWorker".as_value()
        }

        fn visit(&self, _visit: &mut dyn valuable::Visit) {}
    }

    #[async_trait]
    impl Worker for SleepyWorker {
        async fn run(
            &self,
            _execution_id: ExecutionId,
            _ffqn: FunctionFqn,
            _params: Params,
            _events: Vec<HistoryEvent>,
            version: Version,
            _execution_deadline: DateTime<Utc>,
        ) -> WorkerResult {
            tokio::time::sleep(self.duration).await;
            Ok((self.result.clone(), version))
        }
    }

    #[tokio::test]
    async fn hanging_lock_should_be_cleaned_and_execution_retried() {
        set_up();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_millis(100),
            lock_expiry_leeway: Duration::from_millis(10),
            tick_sleep: Duration::ZERO,
            cleanup_expired_locks: true,
            clock_fn,
        };
        let db_connection = TickBasedDbConnection {
            db_task: Arc::new(std::sync::Mutex::new(DbTask::new())),
        };
        let worker = SleepyWorker {
            duration: exec_config.lock_expiry + exec_config.lock_expiry_leeway * 2,
            result: SupportedFunctionResult::None,
        };
        // Create an execution
        let execution_id = ExecutionId::generate();
        let timeout_duration = Duration::from_millis(300);
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                SOME_FFQN.to_owned(),
                Params::default(),
                None,
                None,
                timeout_duration,
                1,
            )
            .await
            .unwrap_or_log();

        let executor = ExecTask {
            db_connection: db_connection.clone(),
            worker,
            config: exec_config.clone(),
            task_limiter: None,
            executor_name: Arc::new("SimpleWorker".to_string()),
        };
        let mut first_execution_progress = executor
            .tick(ExecTickRequest {
                executed_at: created_at,
            })
            .await
            .unwrap_or_log();
        assert_eq!(1, first_execution_progress.executions.len());
        // Started hanging, wait for lock expiry.
        tokio::time::sleep(exec_config.lock_expiry + exec_config.lock_expiry_leeway).await;
        // cleanup should be called
        let now_after_first_lock_expiry = now();
        {
            debug!(now = %now_after_first_lock_expiry, "Expecting an expired lock");
            let cleanup_progress = executor
                .tick(ExecTickRequest {
                    executed_at: now_after_first_lock_expiry,
                })
                .await
                .unwrap_or_log();
            assert!(cleanup_progress.executions.is_empty());
            assert_eq!(Some(1), cleanup_progress.expired_locks);
        }
        assert!(!first_execution_progress
            .executions
            .pop()
            .unwrap()
            .1
            .is_finished());

        let execution_history = db_connection
            .get(execution_id.clone())
            .await
            .unwrap_or_log();
        let expected_first_timeout_expiry = now_after_first_lock_expiry + timeout_duration;
        assert_matches!(
            &execution_history.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::IntermittentTimeout { expires_at },
                created_at: at,
            } if *at == now_after_first_lock_expiry && *expires_at == expected_first_timeout_expiry
        );
        assert_eq!(
            PendingState::PendingAt(expected_first_timeout_expiry),
            execution_history.pending_state
        );
        tokio::time::sleep(timeout_duration).await;
        let now_after_first_timeout = now();
        debug!(now = %now_after_first_timeout, "Second execution should hang again and result in a permanent timeout");

        let mut second_execution_progress = executor
            .tick(ExecTickRequest {
                executed_at: now_after_first_timeout,
            })
            .await
            .unwrap_or_log();
        assert_eq!(1, second_execution_progress.executions.len());

        // Started hanging, wait for lock expiry.
        tokio::time::sleep(exec_config.lock_expiry + exec_config.lock_expiry_leeway).await;
        // cleanup should be called
        let now_after_second_lock_expiry = now();
        debug!(now = %now_after_second_lock_expiry, "Expecting the second lock to be expired");
        {
            let cleanup_progress = executor
                .tick(ExecTickRequest {
                    executed_at: now_after_second_lock_expiry,
                })
                .await
                .unwrap_or_log();
            assert!(cleanup_progress.executions.is_empty());
            assert_eq!(Some(1), cleanup_progress.expired_locks);
        }
        assert!(!second_execution_progress
            .executions
            .pop()
            .unwrap()
            .1
            .is_finished());
    }
}
