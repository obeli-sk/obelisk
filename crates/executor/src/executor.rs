use crate::worker::{FatalError, Worker, WorkerContext, WorkerError, WorkerResult};
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::JoinSetId;
use concepts::storage::{DbPool, ExecutionLog, JoinSetResponseEvent, LockedExecution};
use concepts::{prefixed_ulid::ExecutorId, ExecutionId, FunctionFqn, StrVariant};
use concepts::{
    storage::{DbConnection, DbError, ExecutionEventInner, JoinSetResponse, Version},
    FinishedExecutionError,
};
use concepts::{ConfigId, FinishedExecutionResult, FunctionMetadata};
use derivative::Derivative;
use std::marker::PhantomData;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument, Level, Span};
use utils::time::ClockFn;

#[derive(Debug, Clone)]
pub struct ExecConfig {
    pub lock_expiry: Duration,
    pub tick_sleep: Duration,
    pub batch_size: u32,
    pub config_id: ConfigId,
    pub task_limiter: Option<Arc<tokio::sync::Semaphore>>,
}

pub struct ExecTask<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    worker: Arc<dyn Worker>,
    config: ExecConfig,
    clock_fn: C, // Used for obtaining current time when the execution finishes.
    db_pool: P,
    executor_id: ExecutorId,
    phantom_data: PhantomData<DB>,
    ffqns: Arc<[FunctionFqn]>,
}

#[derive(Derivative, Default)]
#[derivative(Debug)]
pub struct ExecutionProgress {
    #[derivative(Debug = "ignore")]
    #[allow(dead_code)]
    executions: Vec<(ExecutionId, JoinHandle<()>)>,
}

impl ExecutionProgress {
    #[cfg(feature = "test")]
    pub async fn wait_for_tasks(self) -> Result<usize, tokio::task::JoinError> {
        let execs = self.executions.len();
        for (_, handle) in self.executions {
            handle.await?;
        }
        Ok(execs)
    }
}

pub struct ExecutorTaskHandle {
    is_closing: Arc<AtomicBool>,
    abort_handle: AbortHandle,
    config_id: ConfigId,
    executor_id: ExecutorId,
}

impl ExecutorTaskHandle {
    #[instrument(level = Level::DEBUG, name = "executor.close", skip_all, fields(executor_id= %self.executor_id, config_id=%self.config_id))]
    pub async fn close(&self) {
        trace!("Gracefully closing");
        self.is_closing.store(true, Ordering::Relaxed);
        while !self.abort_handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        debug!("Gracefully closed");
    }
}

impl Drop for ExecutorTaskHandle {
    #[instrument(level = Level::DEBUG, name = "executor.drop", skip_all, fields(executor_id= %self.executor_id, config_id=%self.config_id))]
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!(executor_id= %self.executor_id, config_id=%self.config_id, "Aborting the executor task");
        self.abort_handle.abort();
    }
}

pub(crate) fn extract_ffqns(worker: &dyn Worker) -> Arc<[FunctionFqn]> {
    worker
        .exported_functions()
        .iter()
        .map(|FunctionMetadata { ffqn, .. }| ffqn.clone())
        .collect::<Arc<_>>()
}

impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> ExecTask<C, DB, P> {
    #[cfg(feature = "test")]
    pub fn new(
        worker: Arc<dyn Worker>,
        config: ExecConfig,
        clock_fn: C,
        db_pool: P,
        ffqns: Arc<[FunctionFqn]>,
    ) -> Self {
        Self {
            worker,
            config,
            executor_id: ExecutorId::generate(),
            db_pool,
            phantom_data: PhantomData,
            ffqns,
            clock_fn,
        }
    }

    pub fn spawn_new(
        worker: Arc<dyn Worker>,
        config: ExecConfig,
        clock_fn: C,
        db_pool: P,
        executor_id: ExecutorId,
    ) -> ExecutorTaskHandle {
        let is_closing = Arc::new(AtomicBool::default());
        let is_closing_inner = is_closing.clone();
        let ffqns = extract_ffqns(worker.as_ref());
        let config_id = config.config_id.clone();
        let abort_handle = tokio::spawn(async move {
            debug!(%executor_id, config_id = %config.config_id, "Spawned executor");
            let task = Self {
                worker,
                config,
                executor_id,
                db_pool,
                phantom_data: PhantomData,
                ffqns: ffqns.clone(),
                clock_fn: clock_fn.clone(),
            };
            loop {
                let _ = task.tick(clock_fn.now()).await;
                let executed_at = clock_fn.now();
                task.db_pool
                    .connection()
                    .subscribe_to_pending(executed_at, ffqns.clone(), task.config.tick_sleep)
                    .await;
                if is_closing_inner.load(Ordering::Relaxed) {
                    return;
                }
            }
        })
        .abort_handle();
        ExecutorTaskHandle {
            is_closing,
            abort_handle,
            config_id,
            executor_id,
        }
    }

    fn acquire_task_permits(&self) -> Vec<Option<tokio::sync::OwnedSemaphorePermit>> {
        match &self.config.task_limiter {
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

    #[cfg(feature = "test")]
    pub async fn tick2(&self, executed_at: DateTime<Utc>) -> Result<ExecutionProgress, ()> {
        self.tick(executed_at).await
    }

    #[instrument(level = Level::DEBUG, name = "executor.tick" skip_all, fields(executor_id = %self.executor_id, config_id = %self.config.config_id))]
    async fn tick(&self, executed_at: DateTime<Utc>) -> Result<ExecutionProgress, ()> {
        let locked_executions = {
            let mut permits = self.acquire_task_permits();
            if permits.is_empty() {
                return Ok(ExecutionProgress::default());
            }
            let db_connection = self.db_pool.connection();
            let lock_expires_at = executed_at + self.config.lock_expiry;
            let locked_executions = db_connection
                .lock_pending(
                    permits.len(), // batch size
                    executed_at,   // fetch expiring before now
                    self.ffqns.clone(),
                    executed_at, // created at
                    self.config.config_id.clone(),
                    self.executor_id,
                    lock_expires_at,
                )
                .await
                .map_err(|err| {
                    warn!(executor_id = %self.executor_id, config_id = %self.config.config_id, "lock_pending error {err:?}");
                })?;
            // Drop permits if too many were allocated.
            while permits.len() > locked_executions.len() {
                permits.pop();
            }
            assert_eq!(permits.len(), locked_executions.len());
            locked_executions.into_iter().zip(permits)
        };
        let execution_deadline = executed_at + self.config.lock_expiry;

        let mut executions = Vec::with_capacity(locked_executions.len());
        for (locked_execution, permit) in locked_executions {
            let execution_id = locked_execution.execution_id;
            let join_handle = {
                let worker = self.worker.clone();
                let db_pool = self.db_pool.clone();
                let clock_fn = self.clock_fn.clone();
                let run_id = locked_execution.run_id;
                let worker_span = info_span!(parent: None, "worker",
                    %execution_id, %run_id, ffqn = %locked_execution.ffqn, executor_id = %self.executor_id, config_id = %self.config.config_id);
                locked_execution.metadata.enrich(&worker_span);
                tokio::spawn({
                    let worker_span2 = worker_span.clone();
                    async move {
                        let res = Self::run_worker(
                            worker,
                            db_pool,
                            execution_deadline,
                            clock_fn,
                            locked_execution,
                            worker_span2,
                        )
                        .await;
                        if let Err(db_error) = res {
                            error!("Execution will be timed out not writing `{db_error:?}`");
                        }
                        drop(permit);
                    }
                    .instrument(worker_span)
                })
            };
            executions.push((execution_id, join_handle));
        }
        Ok(ExecutionProgress { executions })
    }

    async fn run_worker(
        worker: Arc<dyn Worker>,
        db_pool: P,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
        locked_execution: LockedExecution,
        worker_span: Span,
    ) -> Result<(), DbError> {
        debug!("Worker::run starting");
        trace!(
            version = %locked_execution.version,
            params = ?locked_execution.params,
            event_history = ?locked_execution.event_history,
            "Worker::run starting"
        );
        let can_be_retried = ExecutionLog::can_be_retried_after(
            locked_execution.intermittent_event_count + 1,
            locked_execution.max_retries,
            locked_execution.retry_exp_backoff,
        );
        let ctx = WorkerContext {
            execution_id: locked_execution.execution_id,
            metadata: locked_execution.metadata,
            ffqn: locked_execution.ffqn,
            params: locked_execution.params,
            event_history: locked_execution.event_history,
            responses: locked_execution
                .responses
                .into_iter()
                .map(|outer| outer.event)
                .collect(),
            version: locked_execution.version,
            execution_deadline,
            can_be_retried: can_be_retried.is_some(),
            run_id: locked_execution.run_id,
            worker_span,
            topmost_parent: locked_execution.topmost_parent,
        };
        let worker_result = worker.run(ctx).await;
        trace!(?worker_result, "Worker::run finished");
        let result_obtained_at = clock_fn.now();
        match Self::worker_result_to_execution_event(
            locked_execution.execution_id,
            worker_result,
            result_obtained_at,
            locked_execution.parent,
            can_be_retried,
        )? {
            Some(append) => {
                let db_connection = db_pool.connection();
                trace!("Appending {append:?}");
                append.clone().append(&db_connection).await
            }
            None => Ok(()),
        }
    }

    // FIXME: On a slow execution: race between `expired_timers_watcher` this if retry_exp_backoff is 0.
    /// Map the `WorkerError` to an intermittent or a permanent failure.
    #[expect(clippy::too_many_lines)]
    fn worker_result_to_execution_event(
        execution_id: ExecutionId,
        worker_result: WorkerResult,
        result_obtained_at: DateTime<Utc>,
        parent: Option<(ExecutionId, JoinSetId)>,
        can_be_retried: Option<Duration>,
    ) -> Result<Option<Append>, DbError> {
        Ok(match worker_result {
            WorkerResult::Ok(result, new_version) => {
                info!(
                    "Execution finished: {}",
                    result.as_pending_state_finished_result()
                );
                let parent = parent.map(|(p, j)| (p, j, Ok(result.clone())));
                let primary_event = ExecutionEventInner::Finished { result: Ok(result) };
                Some(Append {
                    created_at: result_obtained_at,
                    primary_event,
                    execution_id,
                    version: new_version,
                    parent,
                })
            }
            WorkerResult::DbUpdatedByWorker => None,
            WorkerResult::Err(err) => {
                let (primary_event, parent, version) = match err {
                    WorkerError::IntermittentTimeout => {
                        info!("Intermittent timeout");
                        // Will be updated by `expired_timers_watcher`.
                        return Ok(None);
                    }
                    WorkerError::DbError(db_error) => {
                        return Err(db_error);
                    }
                    WorkerError::IntermittentError {
                        reason,
                        err: _,
                        version,
                    } => {
                        if let Some(duration) = can_be_retried {
                            let expires_at = result_obtained_at + duration;
                            debug!("Retrying failed execution after {duration:?} at {expires_at}");
                            (
                                ExecutionEventInner::IntermittentFailure { expires_at, reason },
                                None,
                                version,
                            )
                        } else {
                            info!("Permanently failed - {reason}");
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
                        FatalError::NondeterminismDetected(reason),
                        version,
                    ) => {
                        info!("Nondeterminism detected - {reason}");
                        let result = Err(FinishedExecutionError::NondeterminismDetected(reason));
                        let parent = parent.map(|(p, j)| (p, j, result.clone()));
                        (ExecutionEventInner::Finished { result }, parent, version)
                    }
                    WorkerError::FatalError(FatalError::ParamsParsingError(err), version) => {
                        info!("Error parsing parameters - {err:?}");
                        let result =
                            Err(FinishedExecutionError::PermanentFailure(StrVariant::Arc(
                                Arc::from(format!("error parsing parameters: `{err}`")),
                            )));
                        let parent = parent.map(|(p, j)| (p, j, result.clone()));
                        (ExecutionEventInner::Finished { result }, parent, version)
                    }
                    WorkerError::FatalError(FatalError::ResultParsingError(err), version) => {
                        info!("Error parsing result - {err:?}");
                        let result = Err(FinishedExecutionError::PermanentFailure(
                            StrVariant::Arc(Arc::from(format!("error parsing result: `{err}`"))),
                        ));
                        let parent = parent.map(|(p, j)| (p, j, result.clone()));
                        (ExecutionEventInner::Finished { result }, parent, version)
                    }
                    WorkerError::FatalError(FatalError::ChildExecutionError(err), version) => {
                        info!("Child finished with an execution error - {err:?}");
                        let result = Err(FinishedExecutionError::PermanentFailure(
                            StrVariant::Arc(Arc::from(format!(
                                "child finished with an execution error: `{err}`"
                            ))),
                        ));
                        let parent = parent.map(|(p, j)| (p, j, result.clone()));
                        (ExecutionEventInner::Finished { result }, parent, version)
                    }
                    WorkerError::FatalError(FatalError::UncategorizedError(err), version) => {
                        info!("Uncategorized error - {err:?}");
                        let result = Err(FinishedExecutionError::PermanentFailure(
                            StrVariant::Arc(Arc::from(format!("uncategorized error: `{err}`"))),
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

#[derive(Debug, Clone)]
pub(crate) struct Append {
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) primary_event: ExecutionEventInner,
    pub(crate) execution_id: ExecutionId,
    pub(crate) version: Version,
    pub(crate) parent: Option<(ExecutionId, JoinSetId, FinishedExecutionResult)>,
}

impl Append {
    pub(crate) async fn append(self, db_connection: &impl DbConnection) -> Result<(), DbError> {
        if let Some((parent_id, join_set_id, result)) = self.parent {
            db_connection
                .append_batch_respond_to_parent(
                    self.execution_id,
                    self.created_at,
                    vec![self.primary_event],
                    self.version,
                    parent_id,
                    JoinSetResponseEvent {
                        join_set_id,
                        event: JoinSetResponse::ChildExecutionFinished {
                            child_execution_id: self.execution_id,
                            result,
                        },
                    },
                )
                .await?;
        } else {
            db_connection
                .append_batch(
                    self.created_at,
                    vec![self.primary_event],
                    self.execution_id,
                    self.version,
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
pub mod simple_worker {
    use crate::worker::{Worker, WorkerContext, WorkerResult};
    use async_trait::async_trait;
    use concepts::{
        storage::{HistoryEvent, Version},
        FunctionFqn, FunctionMetadata, ParameterTypes,
    };
    use indexmap::IndexMap;
    use std::sync::Arc;
    use tracing::trace;

    pub(crate) const FFQN_SOME: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");
    pub type SimpleWorkerResultMap =
        Arc<std::sync::Mutex<IndexMap<Version, (Vec<HistoryEvent>, WorkerResult)>>>;

    #[derive(Clone, Debug)]
    pub struct SimpleWorker {
        pub worker_results_rev: SimpleWorkerResultMap,
        pub ffqn: FunctionFqn,
        exported: [FunctionMetadata; 1],
    }

    impl SimpleWorker {
        #[must_use]
        pub fn with_single_result(res: WorkerResult) -> Self {
            Self::with_worker_results_rev(Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(2),
                (vec![], res),
            )]))))
        }

        #[must_use]
        pub fn with_ffqn(self, ffqn: FunctionFqn) -> Self {
            Self {
                worker_results_rev: self.worker_results_rev,
                exported: [FunctionMetadata {
                    ffqn: ffqn.clone(),
                    parameter_types: ParameterTypes::default(),
                    return_type: None,
                }],
                ffqn,
            }
        }

        #[must_use]
        pub fn with_worker_results_rev(worker_results_rev: SimpleWorkerResultMap) -> Self {
            Self {
                worker_results_rev,
                ffqn: FFQN_SOME,
                exported: [FunctionMetadata {
                    ffqn: FFQN_SOME,
                    parameter_types: ParameterTypes::default(),
                    return_type: None,
                }],
            }
        }
    }

    #[async_trait]
    impl Worker for SimpleWorker {
        async fn run(&self, ctx: WorkerContext) -> WorkerResult {
            let (expected_version, (expected_eh, worker_result)) =
                self.worker_results_rev.lock().unwrap().pop().unwrap();
            trace!(%expected_version, version = %ctx.version, ?expected_eh, eh = ?ctx.event_history, "Running SimpleWorker");
            assert_eq!(expected_version, ctx.version);
            assert_eq!(expected_eh, ctx.event_history);
            worker_result
        }

        fn exported_functions(&self) -> &[FunctionMetadata] {
            &self.exported
        }

        fn imported_functions(&self) -> &[FunctionMetadata] {
            &[]
        }
    }
}

#[cfg(test)]
mod tests {
    use self::simple_worker::SimpleWorker;
    use super::*;
    use crate::{expired_timers_watcher, worker::WorkerResult};
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::storage::{CreateRequest, JoinSetRequest};
    use concepts::storage::{
        DbConnection, ExecutionEvent, ExecutionEventInner, HistoryEvent, PendingState,
    };
    use concepts::{FunctionMetadata, ParameterTypes, Params, SupportedFunctionReturnValue};
    use db_tests::Database;
    use indexmap::IndexMap;
    use simple_worker::FFQN_SOME;
    use std::{fmt::Debug, future::Future, ops::Deref, sync::Arc};
    use test_utils::set_up;
    use test_utils::sim_clock::{ConstClock, SimClock};
    use utils::time::Now;

    pub(crate) const FFQN_CHILD: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn-child");

    async fn tick_fn<
        W: Worker + Debug,
        C: ClockFn + 'static,
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
    >(
        config: ExecConfig,
        clock_fn: C,
        db_pool: P,
        worker: Arc<W>,
        executed_at: DateTime<Utc>,
    ) -> ExecutionProgress {
        trace!("Ticking with {worker:?}");
        let ffqns = super::extract_ffqns(worker.as_ref());
        let executor = ExecTask::new(worker, config, clock_fn, db_pool, ffqns);
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
        let created_at = Now.now();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        execute_simple_lifecycle_tick_based(db_pool.clone(), ConstClock(created_at)).await;
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn execute_simple_lifecycle_tick_based_sqlite() {
        let created_at = Now.now();
        let (_guard, db_pool) = Database::Sqlite.set_up().await;
        execute_simple_lifecycle_tick_based(db_pool.clone(), ConstClock(created_at)).await;
        db_pool.close().await.unwrap();
    }

    async fn execute_simple_lifecycle_tick_based<
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
        C: ClockFn + 'static,
    >(
        pool: P,
        clock_fn: C,
    ) {
        set_up();
        let created_at = clock_fn.now();
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::from_millis(100),
            config_id: ConfigId::dummy_activity(),
            task_limiter: None,
        };

        let execution_log = create_and_tick(
            CreateAndTickConfig {
                execution_id: ExecutionId::generate(),
                created_at,
                max_retries: 0,
                executed_at: created_at,
                retry_exp_backoff: Duration::ZERO,
            },
            clock_fn,
            pool,
            exec_config,
            Arc::new(SimpleWorker::with_single_result(WorkerResult::Ok(
                SupportedFunctionReturnValue::None,
                Version::new(2),
            ))),
            tick_fn,
        )
        .await;
        assert_matches!(
            execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionReturnValue::None),
                },
                created_at: _,
            }
        );
    }

    #[tokio::test]
    async fn stochastic_execute_simple_lifecycle_task_based_mem() {
        set_up();
        let created_at = Now.now();
        let clock_fn = ConstClock(created_at);
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            config_id: ConfigId::dummy_activity(),
            task_limiter: None,
        };

        let worker = Arc::new(SimpleWorker::with_single_result(WorkerResult::Ok(
            SupportedFunctionReturnValue::None,
            Version::new(2),
        )));
        let exec_task = ExecTask::spawn_new(
            worker.clone(),
            exec_config.clone(),
            clock_fn,
            db_pool.clone(),
            ExecutorId::generate(),
        );

        let execution_log = create_and_tick(
            CreateAndTickConfig {
                execution_id: ExecutionId::generate(),
                created_at,
                max_retries: 0,
                executed_at: created_at,
                retry_exp_backoff: Duration::ZERO,
            },
            clock_fn,
            db_pool.clone(),
            exec_config,
            worker,
            |_, _, _, _, _| async {
                tokio::time::sleep(Duration::from_secs(1)).await; // non deterministic if not run in madsim
                ExecutionProgress::default()
            },
        )
        .await;
        exec_task.close().await;
        db_pool.close().await.unwrap();
        assert_matches!(
            execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionReturnValue::None),
                },
                created_at: _,
            }
        );
    }

    struct CreateAndTickConfig {
        execution_id: ExecutionId,
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
        T: FnMut(ExecConfig, C, P, Arc<W>, DateTime<Utc>) -> F,
        F: Future<Output = ExecutionProgress>,
    >(
        config: CreateAndTickConfig,
        clock_fn: C,
        db_pool: P,
        exec_config: ExecConfig,
        worker: Arc<W>,
        mut tick: T,
    ) -> ExecutionLog {
        // Create an execution
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at: config.created_at,
                execution_id: config.execution_id,
                ffqn: FFQN_SOME,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: config.created_at,
                retry_exp_backoff: config.retry_exp_backoff,
                max_retries: config.max_retries,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: config.execution_id,
            })
            .await
            .unwrap();
        // execute!
        tick(exec_config, clock_fn, db_pool, worker, config.executed_at).await;
        let execution_log = db_connection.get(config.execution_id).await.unwrap();
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

    #[expect(clippy::too_many_lines)]
    #[tokio::test]
    async fn worker_error_should_trigger_an_execution_retry() {
        set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            config_id: ConfigId::dummy_activity(),
            task_limiter: None,
        };
        let worker = Arc::new(SimpleWorker::with_single_result(WorkerResult::Err(
            WorkerError::IntermittentError {
                reason: StrVariant::Static("fail"),
                err: None,
                version: Version::new(2),
            },
        )));
        let retry_exp_backoff = Duration::from_millis(100);
        debug!(now = %sim_clock.now(), "Creating an execution that should fail");
        let execution_log = create_and_tick(
            CreateAndTickConfig {
                execution_id: ExecutionId::generate(),
                created_at: sim_clock.now(),
                max_retries: 1,
                executed_at: sim_clock.now(),
                retry_exp_backoff,
            },
            sim_clock.clone(),
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
        let worker = Arc::new(SimpleWorker::with_worker_results_rev(Arc::new(
            std::sync::Mutex::new(IndexMap::from([(
                Version::new(4),
                (
                    vec![],
                    WorkerResult::Ok(SupportedFunctionReturnValue::None, Version::new(4)),
                ),
            )])),
        )));
        // noop until `retry_exp_backoff` expires
        assert!(tick_fn(
            exec_config.clone(),
            sim_clock.clone(),
            db_pool.clone(),
            worker.clone(),
            sim_clock.now(),
        )
        .await
        .executions
        .is_empty());
        // tick again to finish the execution
        sim_clock.move_time_forward(retry_exp_backoff).await;
        tick_fn(
            exec_config,
            sim_clock.clone(),
            db_pool.clone(),
            worker,
            sim_clock.now(),
        )
        .await;
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
                    result: Ok(SupportedFunctionReturnValue::None),
                },
                created_at: finished_at,
            } if *finished_at == sim_clock.now()
        );
        db_pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn worker_error_should_not_be_retried_if_no_retries_are_set() {
        set_up();
        let created_at = Now.now();
        let clock_fn = ConstClock(created_at);
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            config_id: ConfigId::dummy_activity(),
            task_limiter: None,
        };
        let worker = Arc::new(SimpleWorker::with_single_result(WorkerResult::Err(
            WorkerError::IntermittentError {
                reason: StrVariant::Static("error reason"),
                err: None,
                version: Version::new(2),
            },
        )));
        let execution_log = create_and_tick(
            CreateAndTickConfig {
                execution_id: ExecutionId::generate(),
                created_at,
                max_retries: 0,
                executed_at: created_at,
                retry_exp_backoff: Duration::ZERO,
            },
            clock_fn,
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
        db_pool.close().await.unwrap();
    }

    #[rstest::rstest(
        worker_error => [
            WorkerError::IntermittentError {
                reason: StrVariant::Static("error reason"),
                err: None,
                version: Version::new(2),
            },
            WorkerError::IntermittentTimeout,
        ]
    )]
    #[tokio::test]
    async fn child_execution_permanently_failed_should_notify_parent(worker_error: WorkerError) {
        use concepts::storage::JoinSetResponseEventOuter;

        const LOCK_EXPIRY: Duration = Duration::from_secs(1);
        set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;

        let parent_worker = Arc::new(SimpleWorker::with_single_result(
            WorkerResult::DbUpdatedByWorker,
        ));
        let parent_execution_id = ExecutionId::generate();
        db_pool
            .connection()
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: parent_execution_id,
                ffqn: FFQN_SOME,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: parent_execution_id,
            })
            .await
            .unwrap();
        tick_fn(
            ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_EXPIRY,
                tick_sleep: Duration::ZERO,
                config_id: ConfigId::dummy_activity(),
                task_limiter: None,
            },
            sim_clock.clone(),
            db_pool.clone(),
            parent_worker,
            sim_clock.now(),
        )
        .await;

        let join_set_id = JoinSetId::generate();
        let child_execution_id = ExecutionId::generate();
        // executor does not append anything, this should have been written by the worker:
        {
            let child = CreateRequest {
                created_at: sim_clock.now(),
                execution_id: child_execution_id,
                ffqn: FFQN_CHILD,
                params: Params::empty(),
                parent: Some((parent_execution_id, join_set_id)),
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: parent_execution_id,
            };
            let join_set = ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSet { join_set_id },
            };
            let child_exec_req = ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                },
            };
            let join_next = ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at: sim_clock.now(),
                    closing: false,
                },
            };
            db_pool
                .connection()
                .append_batch_create_new_execution(
                    sim_clock.now(),
                    vec![join_set, child_exec_req, join_next],
                    parent_execution_id,
                    Version::new(2),
                    vec![child],
                )
                .await
                .unwrap();
        }
        let expected_child_err = match worker_error {
            WorkerError::IntermittentError { .. } => {
                FinishedExecutionError::PermanentFailure(StrVariant::Static("error reason"))
            }
            WorkerError::IntermittentTimeout => FinishedExecutionError::PermanentTimeout,
            worker_error => {
                unreachable!("unexpected {worker_error}")
            }
        };

        let child_worker = Arc::new(
            SimpleWorker::with_single_result(WorkerResult::Err(worker_error)).with_ffqn(FFQN_CHILD),
        );

        // execute the child
        tick_fn(
            ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_EXPIRY,
                tick_sleep: Duration::ZERO,
                config_id: ConfigId::dummy_activity(),
                task_limiter: None,
            },
            sim_clock.clone(),
            db_pool.clone(),
            child_worker,
            sim_clock.now(),
        )
        .await;
        if expected_child_err == FinishedExecutionError::PermanentTimeout {
            // In case of timeout, let the timers watcher handle it
            sim_clock.move_time_forward(LOCK_EXPIRY).await;
            expired_timers_watcher::tick(db_pool.connection(), sim_clock.now())
                .await
                .unwrap();
        }
        let child_log = db_pool.connection().get(child_execution_id).await.unwrap();
        assert!(child_log.pending_state.is_finished());
        assert_eq!(
            ExecutionEventInner::Finished {
                result: Err(expected_child_err)
            },
            child_log.last_event().event
        );
        let parent_log = db_pool.connection().get(parent_execution_id).await.unwrap();
        assert_matches!(
            parent_log.pending_state,
            PendingState::PendingAt {
                scheduled_at
            } if scheduled_at == sim_clock.now(),
            "parent should be back to pending"
        );
        let (found_join_set_id, found_child_execution_id, found_result) = assert_matches!(
            parent_log.responses.last(),
            Some(JoinSetResponseEventOuter{
                created_at: at,
                event: JoinSetResponseEvent{
                    join_set_id: found_join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished{
                        child_execution_id: found_child_execution_id,
                        result: found_result,
                    }
                }
            })
             if *at == sim_clock.now()
            => (*found_join_set_id, *found_child_execution_id, found_result)
        );
        assert_eq!(join_set_id, found_join_set_id);
        assert_eq!(child_execution_id, found_child_execution_id);
        assert!(found_result.is_err());

        db_pool.close().await.unwrap();
    }

    #[derive(Clone, Debug)]
    struct SleepyWorker {
        duration: Duration,
        result: SupportedFunctionReturnValue,
        exported: [FunctionMetadata; 1],
    }

    #[async_trait]
    impl Worker for SleepyWorker {
        async fn run(&self, ctx: WorkerContext) -> WorkerResult {
            tokio::time::sleep(self.duration).await;
            WorkerResult::Ok(self.result.clone(), ctx.version)
        }

        fn exported_functions(&self) -> &[FunctionMetadata] {
            &self.exported
        }

        fn imported_functions(&self) -> &[FunctionMetadata] {
            &[]
        }
    }

    #[expect(clippy::too_many_lines)]
    #[tokio::test]
    async fn hanging_lock_should_be_cleaned_and_execution_retried() {
        set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let lock_expiry = Duration::from_millis(100);
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry,
            tick_sleep: Duration::ZERO,
            config_id: ConfigId::dummy_activity(),
            task_limiter: None,
        };

        let worker = Arc::new(SleepyWorker {
            duration: lock_expiry + Duration::from_millis(1), // sleep more than allowed by the lock expiry
            result: SupportedFunctionReturnValue::None,
            exported: [FunctionMetadata {
                ffqn: FFQN_SOME,
                parameter_types: ParameterTypes::default(),
                return_type: None,
            }],
        });
        // Create an execution
        let execution_id = ExecutionId::generate();
        let timeout_duration = Duration::from_millis(300);
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id,
                ffqn: FFQN_SOME,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: timeout_duration,
                max_retries: 1,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();

        let ffqns = super::extract_ffqns(worker.as_ref());
        let executor = ExecTask::new(
            worker,
            exec_config,
            sim_clock.clone(),
            db_pool.clone(),
            ffqns,
        );
        let mut first_execution_progress = executor.tick(sim_clock.now()).await.unwrap();
        assert_eq!(1, first_execution_progress.executions.len());
        // Started hanging, wait for lock expiry.
        sim_clock.move_time_forward(lock_expiry).await;
        // cleanup should be called
        let now_after_first_lock_expiry = sim_clock.now();
        {
            debug!(now = %now_after_first_lock_expiry, "Expecting an expired lock");
            let cleanup_progress = executor.tick(now_after_first_lock_expiry).await.unwrap();
            assert!(cleanup_progress.executions.is_empty());
        }
        {
            let expired_locks =
                expired_timers_watcher::tick(db_pool.connection(), now_after_first_lock_expiry)
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
        sim_clock.move_time_forward(timeout_duration).await;
        let now_after_first_timeout = sim_clock.now();
        debug!(now = %now_after_first_timeout, "Second execution should hang again and result in a permanent timeout");

        let mut second_execution_progress = executor.tick(now_after_first_timeout).await.unwrap();
        assert_eq!(1, second_execution_progress.executions.len());

        // Started hanging, wait for lock expiry.
        sim_clock.move_time_forward(lock_expiry).await;
        // cleanup should be called
        let now_after_second_lock_expiry = sim_clock.now();
        debug!(now = %now_after_second_lock_expiry, "Expecting the second lock to be expired");
        {
            let cleanup_progress = executor.tick(now_after_second_lock_expiry).await.unwrap();
            assert!(cleanup_progress.executions.is_empty());
        }
        {
            let expired_locks =
                expired_timers_watcher::tick(db_pool.connection(), now_after_second_lock_expiry)
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
        drop(executor);
        db_pool.close().await.unwrap();
    }
}
