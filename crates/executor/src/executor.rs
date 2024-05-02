use crate::worker::{FatalError, Worker, WorkerError, WorkerResult};
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::JoinSetId;
use concepts::storage::{DbPool, ExecutionLog, LockedExecution};
use concepts::FinishedExecutionResult;
use concepts::{prefixed_ulid::ExecutorId, ExecutionId, FunctionFqn, StrVariant};
use concepts::{
    storage::{
        AppendRequest, DbConnection, DbError, ExecutionEventInner, HistoryEvent, JoinSetResponse,
        Version,
    },
    FinishedExecutionError,
};
use derivative::Derivative;
use std::marker::PhantomData;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::AbortHandle;
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument};
use utils::time::ClockFn;

#[derive(Debug, Clone)]
pub struct ExecConfig<C: ClockFn> {
    pub ffqns: Vec<FunctionFqn>,
    pub lock_expiry: Duration,
    pub tick_sleep: Duration,
    pub batch_size: u32,
    pub clock_fn: C, // Used for obtaining current time when the execution finishes.
}

pub struct ExecTask<W: Worker, C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    worker: Arc<W>,
    config: ExecConfig<C>,
    db_pool: P,
    task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    executor_id: ExecutorId,
    phantom_data: PhantomData<DB>,
}

#[derive(Derivative, Default)]
#[derivative(Debug)]
struct ExecutionProgress {
    #[derivative(Debug = "ignore")]
    #[allow(dead_code)] // allowed for testing
    executions: Vec<(ExecutionId, AbortHandle)>,
}

pub struct ExecutorTaskHandle {
    executor_id: ExecutorId,
    is_closing: Arc<AtomicBool>,
    abort_handle: AbortHandle,
}

impl ExecutorTaskHandle {
    #[instrument(skip_all, fields(executor_id = %self.executor_id))]
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
    #[instrument(skip_all, fields(executor_id = %self.executor_id))]
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!("Aborting the task");
        self.abort_handle.abort();
    }
}

impl<W: Worker, C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>
    ExecTask<W, C, DB, P>
{
    pub fn spawn_new(
        worker: Arc<W>,
        config: ExecConfig<C>,
        db_pool: P,
        task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> ExecutorTaskHandle {
        let executor_id = ExecutorId::generate();
        let span = info_span!("executor",
            executor = %executor_id,
            worker = worker.as_value()
        );
        let is_closing = Arc::new(AtomicBool::default());
        let is_closing_inner = is_closing.clone();
        let tick_sleep = config.tick_sleep;
        let abort_handle = tokio::spawn(
            async move {
                info!(ffqns = ?config.ffqns, "Spawned executor");
                let clock_fn = config.clock_fn.clone();
                let task = Self {
                    worker,
                    config,
                    task_limiter,
                    executor_id,
                    db_pool,
                    phantom_data: PhantomData,
                };
                let mut old_err = None;
                loop {
                    let res = task.tick(clock_fn()).await;
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
            executor_id,
            is_closing,
            abort_handle,
        }
    }

    fn log_err_if_new(res: Result<ExecutionProgress, DbError>, old_err: &mut Option<DbError>) {
        match (res, &old_err) {
            (Ok(_), _) => {
                *old_err = None;
            }
            (Err(err), Some(old)) if err == *old => {}
            (Err(err), _) => {
                error!("Tick failed: {err:?}");
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
    async fn tick(&self, executed_at: DateTime<Utc>) -> Result<ExecutionProgress, DbError> {
        let locked_executions = {
            let db_connection = self.db_pool.connection();
            let mut permits = self.acquire_task_permits();
            if permits.is_empty() {
                return Ok(ExecutionProgress::default());
            }
            let lock_expires_at = executed_at + self.config.lock_expiry;
            let locked_executions = db_connection
                .lock_pending(
                    permits.len(), // batch size
                    executed_at,   // fetch expiring before now
                    self.config.ffqns.clone(),
                    executed_at, // created at
                    self.executor_id,
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
        let execution_deadline = executed_at + self.config.lock_expiry;

        let mut executions = Vec::new();
        for (locked_execution, permit) in locked_executions {
            let execution_id = locked_execution.execution_id;
            let join_handle = {
                let worker = self.worker.clone();
                let db_pool = self.db_pool.clone();
                let clock_fn = self.config.clock_fn.clone();
                let run_id = locked_execution.run_id;
                let span =
                    info_span!("worker", %execution_id, %run_id, ffqn = %locked_execution.ffqn,);
                // TODO: wait for termination of all spawned tasks in `close`.
                tokio::spawn(
                    async move {
                        let res = Self::run_worker(
                            worker,
                            db_pool,
                            execution_id,
                            execution_deadline,
                            clock_fn,
                            locked_execution,
                        )
                        .await;
                        if let Err(err) = res {
                            error!("Updating execution failed: {err:?}");
                        }
                        drop(permit);
                    }
                    .instrument(span),
                )
            };
            executions.push((execution_id, join_handle.abort_handle()));
        }
        Ok(ExecutionProgress { executions })
    }

    #[instrument(skip_all, fields(%execution_id, ffqn = %locked_execution.ffqn))]
    async fn run_worker(
        worker: Arc<W>,
        db_pool: P,
        execution_id: ExecutionId,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
        locked_execution: LockedExecution,
    ) -> Result<(), DbError> {
        trace!(
            version = %locked_execution.version,
            params = ?locked_execution.params,
            event_history = ?locked_execution.event_history,
            "Worker::run starting"
        );
        let worker_result = worker
            .run(
                execution_id,
                locked_execution.ffqn,
                locked_execution.params,
                locked_execution.event_history,
                locked_execution.version,
                execution_deadline,
            )
            .await;
        trace!(?worker_result, "Worker::run finished");
        match Self::worker_result_to_execution_event(
            execution_id,
            worker_result,
            clock_fn(),
            locked_execution.retry_exp_backoff,
            locked_execution.parent,
            locked_execution.max_retries,
            locked_execution.intermittent_event_count,
        ) {
            Ok(Some(append)) => {
                trace!("Appending {append:?}");
                append.append(&db_pool.connection()).await
            }
            Ok(None) => Ok(()),
            Err(err) => Err(err),
        }
    }

    // FIXME: On a slow execution: race between `expired_timers_watcher` this if retry_exp_backoff is 0.
    /// Map the `WorkerError` to an intermittent or a permanent failure.
    #[allow(clippy::too_many_lines)]
    fn worker_result_to_execution_event(
        execution_id: ExecutionId,
        worker_result: WorkerResult,
        result_obtained_at: DateTime<Utc>,
        retry_exp_backoff: Duration,
        parent: Option<(ExecutionId, JoinSetId)>,
        max_retries: u32,
        mut intermittent_event_count: u32,
    ) -> Result<Option<Append>, DbError> {
        Ok(match worker_result {
            Ok((result, new_version)) => {
                let finished_res;
                // FIXME: interpret fallible result only in activity worker
                let primary_event = if let Some(exec_err) = result.fallible_err() {
                    intermittent_event_count += 1;
                    info!(
                        intermittent_event_count,
                        "Execution finished with an error result"
                    );
                    let reason = StrVariant::Arc(Arc::from(format!(
                        "Execution returned error result: `{exec_err:?}`"
                    )));
                    if let Some(duration) = ExecutionLog::can_be_retried_after(
                        intermittent_event_count,
                        max_retries,
                        retry_exp_backoff,
                    ) {
                        let expires_at = result_obtained_at + duration;
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
                let parent = parent.and_then(|(p, j)| finished_res.map(|r| (p, j, r)));
                Some(Append {
                    created_at: result_obtained_at,
                    primary_event,
                    execution_id,
                    version: new_version,
                    parent,
                })
            }
            Err(err) => {
                let (primary_event, parent, version) = match err {
                    WorkerError::ChildExecutionRequest | WorkerError::DelayRequest => {
                        return Ok(None);
                    }
                    WorkerError::IntermittentTimeout => {
                        info!("Intermittent timeout");
                        // Will be updated by `expired_timers_watcher`.
                        return Ok(None);
                    }
                    WorkerError::DbError(db_error) => {
                        info!("Worker encountered db error: {db_error:?}");
                        // FIXME: What to do? ExecutionEventInner::IntermittentFailure
                        return Err(db_error);
                    }
                    WorkerError::IntermittentError {
                        reason,
                        err: _,
                        version,
                    } => {
                        intermittent_event_count += 1;
                        info!(intermittent_event_count, "Execution finished with an error");
                        if let Some(duration) = ExecutionLog::can_be_retried_after(
                            intermittent_event_count,
                            max_retries,
                            retry_exp_backoff,
                        ) {
                            let expires_at = result_obtained_at + duration;
                            debug!("Retrying failed execution after {duration:?} at {expires_at}");
                            (
                                ExecutionEventInner::IntermittentFailure { expires_at, reason },
                                None,
                                version,
                            )
                        } else {
                            info!("Permanently failed");
                            let result = Err(FinishedExecutionError::PermanentFailure(reason));
                            let parent = parent.map(|(p, j)| (p, j, result.clone()));
                            (ExecutionEventInner::Finished { result }, parent, version)
                        }
                    }
                    WorkerError::LimitReached(reason, new_version) => {
                        warn!("Limit reached: {reason}, unlocking");
                        (ExecutionEventInner::Unlocked, None, new_version)
                    }
                    WorkerError::FatalError(
                        FatalError::NonDeterminismDetected(reason),
                        version,
                    ) => {
                        info!("Non-determinism detected");
                        let result = Err(FinishedExecutionError::NonDeterminismDetected(reason));
                        let parent = parent.map(|(p, j)| (p, j, result.clone()));
                        (ExecutionEventInner::Finished { result }, parent, version)
                    }
                    WorkerError::FatalError(FatalError::ParamsParsingError(err), version) => {
                        info!("Error parsing parameters");
                        let result =
                            Err(FinishedExecutionError::PermanentFailure(StrVariant::Arc(
                                Arc::from(format!("error parsing parameters: {err:?}")),
                            )));
                        let parent = parent.map(|(p, j)| (p, j, result.clone()));
                        (ExecutionEventInner::Finished { result }, parent, version)
                    }
                    WorkerError::FatalError(FatalError::ResultParsingError(err), version) => {
                        info!("Error parsing result");
                        let result = Err(FinishedExecutionError::PermanentFailure(
                            StrVariant::Arc(Arc::from(format!("error parsing result: {err:?}"))),
                        ));
                        let parent = parent.map(|(p, j)| (p, j, result.clone()));
                        (ExecutionEventInner::Finished { result }, parent, version)
                    }
                };
                Some(Append {
                    created_at: result_obtained_at,
                    primary_event,
                    execution_id,
                    version,
                    parent,
                })
            }
        })
    }
}

#[derive(Debug)]
pub(crate) struct Append {
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) primary_event: ExecutionEventInner,
    pub(crate) execution_id: ExecutionId,
    pub(crate) version: Version,
    pub(crate) parent: Option<(ExecutionId, JoinSetId, FinishedExecutionResult)>,
}

impl Append {
    pub(crate) async fn append(self, db_connection: &impl DbConnection) -> Result<(), DbError> {
        let primary_append = vec![AppendRequest {
            created_at: self.created_at,
            event: self.primary_event,
        }];
        if let Some((parent_id, join_set_id, result)) = self.parent {
            db_connection
                .append_batch_respond_to_parent(
                    primary_append,
                    self.execution_id,
                    self.version,
                    (
                        parent_id,
                        AppendRequest {
                            created_at: self.created_at,
                            event: ExecutionEventInner::HistoryEvent {
                                event: HistoryEvent::JoinSetResponse {
                                    join_set_id,
                                    response: JoinSetResponse::ChildExecutionFinished {
                                        child_execution_id: self.execution_id,
                                        result,
                                    },
                                },
                            },
                        },
                    ),
                )
                .await?;
        } else {
            db_connection
                .append_batch(primary_append, self.execution_id, self.version)
                .await?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
pub mod simple_worker {
    use crate::worker::{Worker, WorkerResult};
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use concepts::{
        storage::{HistoryEvent, Version},
        ExecutionId, FunctionFqn, Params,
    };
    use indexmap::IndexMap;
    use std::sync::Arc;
    use tracing::trace;

    pub(crate) const SOME_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");
    pub(crate) const SOME_FFQN_PTR: &FunctionFqn = &SOME_FFQN;

    pub type SimpleWorkerResultMap =
        Arc<std::sync::Mutex<IndexMap<Version, (Vec<HistoryEvent>, WorkerResult)>>>;

    #[derive(Clone, Debug)]
    pub struct SimpleWorker {
        pub worker_results_rev: SimpleWorkerResultMap,
    }

    impl SimpleWorker {
        #[must_use]
        pub fn with_single_result(res: WorkerResult) -> Self {
            Self {
                worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                    Version::new(2),
                    (vec![], res),
                )]))),
            }
        }
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
            _ffqn: FunctionFqn,
            _params: Params,
            eh: Vec<HistoryEvent>,
            version: Version,
            _execution_deadline: DateTime<Utc>,
        ) -> WorkerResult {
            let (expected_version, (expected_eh, worker_result)) =
                self.worker_results_rev.lock().unwrap().pop().unwrap();
            trace!(%expected_version, %version, ?expected_eh, ?eh, "Running SimpleWorker");
            assert_eq!(expected_version, version);
            assert_eq!(expected_eh, eh);
            worker_result
        }

        fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
            Some(SOME_FFQN_PTR).into_iter()
        }
    }
}

#[cfg(test)]
mod tests {
    use self::simple_worker::{SimpleWorker, SOME_FFQN_PTR};
    use super::*;
    use crate::{expired_timers_watcher, worker::WorkerResult};
    use anyhow::anyhow;
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::storage::CreateRequest;
    use concepts::storage::{
        DbConnection, ExecutionEvent, ExecutionEventInner, HistoryEvent, PendingState,
    };
    use concepts::{Params, SupportedFunctionResult};
    use db_mem::inmemory_dao::DbTask;
    use indexmap::IndexMap;
    use simple_worker::SOME_FFQN;
    use std::{fmt::Debug, future::Future, ops::Deref, sync::Arc};
    use test_utils::set_up;
    use test_utils::sim_clock::SimClock;
    use utils::time::now;
    use val_json::type_wrapper::TypeWrapper;
    use val_json::wast_val::{WastVal, WastValWithType};

    async fn tick_fn<
        W: Worker + Debug,
        C: ClockFn + 'static,
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
    >(
        config: ExecConfig<C>,
        db_pool: P,
        worker: Arc<W>,
        executed_at: DateTime<Utc>,
    ) -> ExecutionProgress {
        trace!("Ticking with {worker:?}");
        let executor = ExecTask {
            worker,
            config,
            db_pool,
            task_limiter: None,
            executor_id: ExecutorId::generate(),
            phantom_data: PhantomData,
        };
        let mut execution_progress = executor.tick(executed_at).await.unwrap();
        loop {
            execution_progress
                .executions
                .retain(|(_, abort_handle)| !abort_handle.is_finished());
            if execution_progress.executions.is_empty() {
                return execution_progress;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn execute_simple_lifecycle_tick_based_mem() {
        let mut db_task = DbTask::spawn_new(1);
        let pool = db_task.pool().unwrap();
        execute_simple_lifecycle_tick_based(pool).await;
        db_task.close().await;
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn execute_simple_lifecycle_tick_based_sqlite() {
        use db_sqlite::sqlite_dao::tempfile::sqlite_pool;

        let (pool, _guard) = sqlite_pool().await;
        execute_simple_lifecycle_tick_based(pool.clone()).await;
        pool.close().await.unwrap();
    }

    async fn execute_simple_lifecycle_tick_based<
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
    >(
        pool: P,
    ) {
        set_up();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::from_millis(100),
            clock_fn,
        };

        let execution_log = create_and_tick(
            CreateAndTickConfig {
                created_at,
                max_retries: 0,
                executed_at: created_at,
                retry_exp_backoff: Duration::ZERO,
            },
            pool,
            exec_config,
            Arc::new(SimpleWorker::with_single_result(Ok((
                SupportedFunctionResult::None,
                Version::new(2),
            )))),
            tick_fn,
        )
        .await;
        assert_matches!(
            execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: _,
            }
        );
    }

    #[tokio::test]
    async fn stochastic_execute_simple_lifecycle_task_based_mem() {
        set_up();
        let created_at = now();
        let clock_fn = move || created_at;
        let mut db_task = DbTask::spawn_new(1);
        let db_pool = db_task.pool().unwrap();
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn,
        };

        let worker = Arc::new(SimpleWorker::with_single_result(Ok((
            SupportedFunctionResult::None,
            Version::new(2),
        ))));
        let exec_task =
            ExecTask::spawn_new(worker.clone(), exec_config.clone(), db_pool.clone(), None);

        let execution_log = create_and_tick(
            CreateAndTickConfig {
                created_at,
                max_retries: 0,
                executed_at: created_at,
                retry_exp_backoff: Duration::ZERO,
            },
            db_pool,
            exec_config,
            worker,
            |_, _, _, _| async {
                tokio::time::sleep(Duration::from_secs(1)).await; // non deterministic if not run in madsim
                ExecutionProgress::default()
            },
        )
        .await;
        exec_task.close().await;
        db_task.close().await;
        assert_matches!(
            execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: _,
            }
        );
    }

    struct CreateAndTickConfig {
        created_at: DateTime<Utc>,
        max_retries: u32,
        executed_at: DateTime<Utc>,
        retry_exp_backoff: Duration,
    }

    async fn create_and_tick<
        W: Worker,
        C: ClockFn,
        DB: DbConnection,
        P: DbPool<DB>,
        T: FnMut(ExecConfig<C>, P, Arc<W>, DateTime<Utc>) -> F,
        F: Future<Output = ExecutionProgress>,
    >(
        config: CreateAndTickConfig,
        db_pool: P,
        exec_config: ExecConfig<C>,
        worker: Arc<W>,
        mut tick: T,
    ) -> ExecutionLog {
        // Create an execution
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at: config.created_at,
                execution_id,
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: config.retry_exp_backoff,
                max_retries: config.max_retries,
            })
            .await
            .unwrap();
        // execute!
        tick(exec_config, db_pool.clone(), worker, config.executed_at).await;
        let execution_log = db_connection.get(execution_id).await.unwrap();
        debug!("Execution history after tick: {execution_log:?}");
        // check that DB contains Created and Locked events.
        assert_matches!(
            execution_log.events.first().unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Created { .. },
                created_at: actually_created_at,
            }
            if config.created_at == *actually_created_at
        );
        let locked_at = assert_matches!(
            execution_log.events.get(1).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: locked_at
            } if config.created_at <= *locked_at
            => *locked_at
        );
        assert_matches!(execution_log.events.get(2).unwrap(), ExecutionEvent {
            event: _,
            created_at: executed_at,
        } if *executed_at >= locked_at);
        execution_log
    }

    #[tokio::test]
    async fn worker_error_should_trigger_an_execution_retry() {
        set_up();
        let sim_clock = SimClock::new(now());
        let mut db_task = DbTask::spawn_new(1);
        let db_pool = db_task.pool().unwrap();
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn: sim_clock.clock_fn(),
        };
        let worker = Arc::new(SimpleWorker::with_single_result(Err(
            WorkerError::IntermittentError {
                reason: StrVariant::Static("fail"),
                err: anyhow!("").into(),
                version: Version::new(2),
            },
        )));
        let retry_exp_backoff = Duration::from_millis(100);
        debug!(now = %sim_clock.now(), "Creating an execution that should fail");
        let execution_log = create_and_tick(
            CreateAndTickConfig {
                created_at: sim_clock.now(),
                max_retries: 1,
                executed_at: sim_clock.now(),
                retry_exp_backoff,
            },
            db_pool.clone(),
            exec_config.clone(),
            worker,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_log.events.len());
        {
            let (reason, at, expires_at) = assert_matches!(
                &execution_log.events.get(2).unwrap(),
                ExecutionEvent {
                    event: ExecutionEventInner::IntermittentFailure {
                        reason,
                        expires_at,
                    },
                    created_at: at,
                }
                => (reason, *at, *expires_at)
            );
            assert_eq!("fail", reason.deref());
            assert_eq!(at, sim_clock.now());
            assert_eq!(sim_clock.now() + retry_exp_backoff, expires_at);
        }
        let worker = Arc::new(SimpleWorker {
            worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(4),
                (vec![], Ok((SupportedFunctionResult::None, Version::new(4)))),
            )]))),
        });
        // noop until `retry_exp_backoff` expires
        assert!(tick_fn(
            exec_config.clone(),
            db_pool.clone(),
            worker.clone(),
            sim_clock.now(),
        )
        .await
        .executions
        .is_empty());
        // tick again to finish the execution
        sim_clock.move_time_forward(retry_exp_backoff);
        tick_fn(exec_config, db_pool.clone(), worker, sim_clock.now()).await;
        let execution_log = {
            let db_connection = db_pool.connection();
            db_connection.get(execution_log.execution_id).await.unwrap()
        };
        debug!(now = %sim_clock.now(), "Execution history after second tick: {execution_log:?}");
        assert_matches!(
            execution_log.events.get(3).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: at
            } if *at == sim_clock.now()
        );
        assert_matches!(
            execution_log.events.get(4).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: finished_at,
            } if *finished_at == sim_clock.now()
        );
        drop(db_pool);
        db_task.close().await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn execution_returning_err_should_be_retried() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_pool = db_task.pool().unwrap();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn,
        };

        let worker = Arc::new(SimpleWorker::with_single_result(Ok((
            SupportedFunctionResult::Fallible(WastValWithType {
                r#type: TypeWrapper::Result {
                    ok: None,
                    err: None,
                },
                value: WastVal::Result(Err(None)),
            }),
            Version::new(2),
        ))));
        let execution_log = create_and_tick(
            CreateAndTickConfig {
                created_at,
                max_retries: 1,
                executed_at: created_at,
                retry_exp_backoff: Duration::ZERO,
            },
            db_pool.clone(),
            exec_config.clone(),
            worker,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_log.events.len());
        let reason = assert_matches!(
            &execution_log.events.get(2).unwrap(),
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
        let worker = Arc::new(SimpleWorker {
            worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(4),
                (
                    vec![],
                    Ok((
                        SupportedFunctionResult::Fallible(WastValWithType {
                            r#type: TypeWrapper::Result {
                                ok: None,
                                err: None,
                            },
                            value: WastVal::Result(Ok(None)),
                        }),
                        Version::new(4),
                    )),
                ),
            )]))),
        });
        tick_fn(exec_config, db_pool.clone(), worker, created_at).await;
        let execution_log = {
            let db_connection = db_pool.connection();
            db_connection.get(execution_log.execution_id).await.unwrap()
        };
        debug!("Execution history after second tick: {execution_log:?}");

        assert_eq!(5, execution_log.events.len());
        assert_matches!(
            execution_log.events.get(3).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: locked_at
            }  if *locked_at == created_at
        );
        let (ok, err) = assert_matches!(
            execution_log.events.get(4).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::Fallible(
                        WastValWithType {
                            r#type: TypeWrapper::Result {
                                ok,
                                err,
                            },
                            value:WastVal::Result(Ok(None)),
                        }
                    )),
                },
                created_at: finished_at,
            } if *finished_at == created_at
            => (ok, err)
        );
        assert_eq!(None, *ok);
        assert_eq!(None, *err);
        drop(db_pool);
        db_task.close().await;
    }

    #[tokio::test]
    async fn err_activity_should_not_be_retried_if_no_retries_are_set() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_pool = db_task.pool().unwrap();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn,
        };
        let worker = Arc::new(SimpleWorker::with_single_result(Ok((
            SupportedFunctionResult::Fallible(WastValWithType {
                r#type: TypeWrapper::Result {
                    ok: None,
                    err: None,
                },
                value: WastVal::Result(Err(Some(WastVal::String("myerror".to_string()).into()))),
            }),
            Version::new(2),
        ))));
        let execution_log = create_and_tick(
            CreateAndTickConfig {
                created_at,
                max_retries: 0,
                executed_at: created_at,
                retry_exp_backoff: Duration::ZERO,
            },
            db_pool.clone(),
            exec_config.clone(),
            worker,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_log.events.len());
        let reason = assert_matches!(
            &execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished{
                    result: Err(FinishedExecutionError::PermanentFailure(reason))
                },
                created_at: at,
            } if *at == created_at
            => reason.to_string()
        );
        assert_eq!(
            "Execution returned error result: `Some(String(\"myerror\"))`",
            reason
        );
        drop(db_pool);
        db_task.close().await;
    }

    #[tokio::test]
    async fn worker_error_should_not_be_retried_if_no_retries_are_set() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_pool = db_task.pool().unwrap();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn,
        };
        let worker = Arc::new(SimpleWorker::with_single_result(WorkerResult::Err(
            WorkerError::IntermittentError {
                reason: StrVariant::Static("error reason"),
                err: anyhow!("myerror").into(),
                version: Version::new(2),
            },
        )));
        let execution_log = create_and_tick(
            CreateAndTickConfig {
                created_at,
                max_retries: 0,
                executed_at: created_at,
                retry_exp_backoff: Duration::ZERO,
            },
            db_pool.clone(),
            exec_config.clone(),
            worker,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_log.events.len());
        let reason = assert_matches!(
            &execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished{
                    result: Err(FinishedExecutionError::PermanentFailure(reason))
                },
                created_at: at,
            } if *at == created_at
            => reason.to_string()
        );
        assert_eq!("error reason", reason);
        drop(db_pool);
        db_task.close().await;
    }

    // TODO child_execution_permanently_failed_should_notify_parent
    // TODO child_execution_permanently_timed_out_should_notify_parent

    // TODO: Fatal errors should be propagated to the topmost parent

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

        fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
            Some(SOME_FFQN_PTR).into_iter()
        }
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn hanging_lock_should_be_cleaned_and_execution_retried() {
        set_up();
        let sim_clock = SimClock::new(now());
        let mut db_task = DbTask::spawn_new(1);
        let db_pool = db_task.pool().unwrap();
        let lock_expiry = Duration::from_millis(100);
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry,
            tick_sleep: Duration::ZERO,
            clock_fn: sim_clock.clock_fn(),
        };

        let timers_watcher = expired_timers_watcher::TimersWatcherTask {
            db_connection: db_pool.connection(),
        };

        let worker = Arc::new(SleepyWorker {
            duration: lock_expiry + Duration::from_millis(1), // sleep more than allowed by the lock expiry
            result: SupportedFunctionResult::None,
        });
        // Create an execution
        let execution_id = ExecutionId::generate();
        let timeout_duration = Duration::from_millis(300);
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id,
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: timeout_duration,
                max_retries: 1,
            })
            .await
            .unwrap();

        let executor = ExecTask {
            worker,
            config: exec_config,
            db_pool: db_pool.clone(),
            task_limiter: None,
            executor_id: ExecutorId::generate(),
            phantom_data: PhantomData,
        };
        let mut first_execution_progress = executor.tick(sim_clock.now()).await.unwrap();
        assert_eq!(1, first_execution_progress.executions.len());
        // Started hanging, wait for lock expiry.
        sim_clock.move_time_forward(lock_expiry);
        // cleanup should be called
        let now_after_first_lock_expiry = sim_clock.now();
        {
            debug!(now = %now_after_first_lock_expiry, "Expecting an expired lock");
            let cleanup_progress = executor.tick(now_after_first_lock_expiry).await.unwrap();
            assert!(cleanup_progress.executions.is_empty());
        }
        {
            let expired_locks = timers_watcher
                .tick(now_after_first_lock_expiry)
                .await
                .unwrap()
                .expired_locks;
            assert_eq!(1, expired_locks);
        }
        assert!(!first_execution_progress
            .executions
            .pop()
            .unwrap()
            .1
            .is_finished());

        let execution_log = db_connection.get(execution_id).await.unwrap();
        let expected_first_timeout_expiry = now_after_first_lock_expiry + timeout_duration;
        assert_matches!(
            &execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::IntermittentTimeout { expires_at },
                created_at: at,
            } if *at == now_after_first_lock_expiry && *expires_at == expected_first_timeout_expiry
        );
        assert_eq!(
            PendingState::PendingAt {
                scheduled_at: expected_first_timeout_expiry
            },
            execution_log.pending_state
        );
        sim_clock.move_time_forward(timeout_duration);
        let now_after_first_timeout = sim_clock.now();
        debug!(now = %now_after_first_timeout, "Second execution should hang again and result in a permanent timeout");

        let mut second_execution_progress = executor.tick(now_after_first_timeout).await.unwrap();
        assert_eq!(1, second_execution_progress.executions.len());

        // Started hanging, wait for lock expiry.
        sim_clock.move_time_forward(lock_expiry);
        // cleanup should be called
        let now_after_second_lock_expiry = sim_clock.now();
        debug!(now = %now_after_second_lock_expiry, "Expecting the second lock to be expired");
        {
            let cleanup_progress = executor.tick(now_after_second_lock_expiry).await.unwrap();
            assert!(cleanup_progress.executions.is_empty());
        }
        {
            let expired_locks = timers_watcher
                .tick(now_after_second_lock_expiry)
                .await
                .unwrap()
                .expired_locks;
            assert_eq!(1, expired_locks);
        }
        assert!(!second_execution_progress
            .executions
            .pop()
            .unwrap()
            .1
            .is_finished());

        drop(db_connection);
        drop(db_pool);
        drop(executor);
        drop(timers_watcher);
        db_task.close().await;
    }

    // TODO: same for IntermittentTimeout - should be handled by TimersWatcherTask. Count retries.
}
