use crate::worker::{FatalError, Worker, WorkerError, WorkerResult};
use chrono::{DateTime, Utc};
use concepts::storage::ExecutionHistory;
use concepts::{prefixed_ulid::ExecutorId, ExecutionId, FunctionFqn, Params, StrVariant};
use concepts::{
    storage::{
        AppendRequest, DbConnection, DbConnectionError, DbError, ExecutionEventInner, HistoryEvent,
        JoinSetResponse, SpecificError, Version,
    },
    FinishedExecutionError,
};
use derivative::Derivative;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::AbortHandle;
use tracing::{debug, info, info_span, instrument, trace, warn, Instrument};
use utils::time::ClockFn;

#[derive(Debug, Clone)]
pub struct ExecConfig<C: ClockFn> {
    pub ffqns: Vec<FunctionFqn>,
    pub lock_expiry: Duration,
    pub tick_sleep: Duration,
    pub batch_size: u32,
    pub clock_fn: C, // Used for obtaining current time when the execution finishes.
}

pub struct ExecTask<DB: DbConnection, W: Worker, C: ClockFn> {
    db_connection: DB,
    worker: Arc<W>,
    config: ExecConfig<C>,
    task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    executor_id: ExecutorId,
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

impl<DB: DbConnection, W: Worker, C: ClockFn + 'static> ExecTask<DB, W, C> {
    pub fn spawn_new(
        db_connection: DB,
        worker: Arc<W>,
        config: ExecConfig<C>,
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
                    db_connection,
                    worker,
                    config,
                    task_limiter,
                    executor_id,
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
    async fn tick(
        &self,
        executed_at: DateTime<Utc>,
    ) -> Result<ExecutionProgress, DbConnectionError> {
        let locked_executions = {
            let mut permits = self.acquire_task_permits();
            if permits.is_empty() {
                return Ok(ExecutionProgress::default());
            }
            let lock_expires_at = executed_at + self.config.lock_expiry;
            let locked_executions = self
                .db_connection
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
        Ok(ExecutionProgress { executions })
    }

    #[instrument(skip_all, fields(%execution_id, %ffqn))]
    #[allow(clippy::too_many_arguments)]
    async fn run_worker(
        worker: Arc<W>,
        db_connection: DB,
        execution_id: ExecutionId,
        version: Version,
        ffqn: FunctionFqn,
        params: Params,
        event_history: Vec<HistoryEvent>,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
    ) -> Result<(), DbError> {
        trace!(%version, ?params, ?event_history, "Worker::run starting");
        let worker_result = worker
            .run(
                execution_id,
                ffqn,
                params,
                event_history,
                version,
                execution_deadline,
            )
            .await;
        trace!(?worker_result, "Worker::run finished");
        let execution_history = db_connection.get(execution_id).await?;
        match Self::worker_result_to_execution_event(
            execution_id,
            worker_result,
            &execution_history,
            clock_fn(),
        ) {
            Ok(Some(append)) => {
                db_connection
                    .append_batch(
                        append.primary_events,
                        append.execution_id,
                        Some(append.version),
                    )
                    .await?;
                if let Some((secondary_id, secondary_append_request)) =
                    append.async_resp_to_other_execution
                {
                    db_connection
                        .append(secondary_id, None, secondary_append_request)
                        .await?;
                }
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Map the `WorkerError` to an intermittent or a permanent failure.
    #[allow(clippy::too_many_lines)]
    fn worker_result_to_execution_event(
        execution_id: ExecutionId,
        worker_result: WorkerResult,
        execution_history: &ExecutionHistory,
        result_obtained_at: DateTime<Utc>,
    ) -> Result<Option<Append>, DbError> {
        Ok(match worker_result {
            Ok((result, new_version)) => {
                let finished_res;
                let event = if let Some(exec_err) = result.fallible_err() {
                    info!("Execution finished with an error result");
                    let reason = StrVariant::Arc(Arc::from(format!(
                        "Execution returned error result: `{exec_err:?}`"
                    )));
                    if let Some(duration) = execution_history.can_be_retried_after() {
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
                let finished_event = AppendRequest {
                    created_at: result_obtained_at,
                    event,
                };
                let secondary = if let (Some(finished_res), Some((parent_id, join_set_id))) =
                    (finished_res, execution_history.parent())
                {
                    Some((
                        parent_id,
                        AppendRequest {
                            created_at: result_obtained_at,
                            event: ExecutionEventInner::HistoryEvent {
                                event: HistoryEvent::JoinSetResponse {
                                    join_set_id,
                                    response: JoinSetResponse::ChildExecutionFinished {
                                        child_execution_id: execution_id,
                                        result: finished_res,
                                    },
                                },
                            },
                        },
                    ))
                } else {
                    None
                };
                Some(Append {
                    primary_events: vec![finished_event],
                    execution_id,
                    version: new_version,
                    async_resp_to_other_execution: secondary,
                })
            }
            Err((err, new_version)) => {
                if execution_history.version != new_version {
                    return Err(DbError::Specific(SpecificError::VersionMismatch));
                }
                let event = match err {
                    WorkerError::ChildExecutionRequest | WorkerError::DelayRequest => {
                        return Ok(None);
                    }
                    WorkerError::IntermittentError { reason, err: _ } => {
                        if let Some(duration) = execution_history.can_be_retried_after() {
                            let expires_at = result_obtained_at + duration;
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
                            let expires_at = result_obtained_at + duration;
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
                    WorkerError::DbError(db_error) => {
                        info!("Worker encountered db error: {db_error:?}");
                        return Err(db_error);
                    }
                    WorkerError::FatalError(FatalError::NonDeterminismDetected(reason)) => {
                        info!("Non-determinism detected");
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::NonDeterminismDetected(reason)),
                        }
                    }
                    WorkerError::FatalError(FatalError::ParamsParsingError(err)) => {
                        info!("Error parsing parameters");
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::PermanentFailure(StrVariant::Arc(
                                Arc::from(format!("error parsing parameters: {err:?}")),
                            ))),
                        }
                    }
                    WorkerError::FatalError(FatalError::ResultParsingError(err)) => {
                        info!("Error parsing result");
                        ExecutionEventInner::Finished {
                            result: Err(FinishedExecutionError::PermanentFailure(StrVariant::Arc(
                                Arc::from(format!("error parsing result: {err:?}")),
                            ))),
                        }
                    }
                };
                Some(Append {
                    primary_events: vec![AppendRequest {
                        created_at: result_obtained_at,
                        event,
                    }],
                    execution_id,
                    version: new_version,
                    async_resp_to_other_execution: None,
                })
            }
        })
    }
}

struct Append {
    primary_events: Vec<AppendRequest>,
    execution_id: ExecutionId,
    version: Version,
    async_resp_to_other_execution: Option<(ExecutionId, AppendRequest)>,
}

#[cfg(any(test, feature = "test"))]
pub mod simple_worker {
    use super::{
        trace, Arc, DateTime, ExecutionId, FunctionFqn, HistoryEvent, Params, Utc, Version, Worker,
        WorkerResult,
    };
    use async_trait::async_trait;
    use indexmap::IndexMap;

    pub(crate) const SOME_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");
    pub(crate) const SOME_FFQN_PTR: &FunctionFqn = &SOME_FFQN;

    pub type SimpleWorkerResultMap =
        Arc<std::sync::Mutex<IndexMap<Version, (Vec<HistoryEvent>, WorkerResult)>>>;

    #[derive(Clone, Debug)]
    pub struct SimpleWorker {
        pub worker_results_rev: SimpleWorkerResultMap,
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
    use concepts::storage::{
        journal::PendingState, DbConnection, ExecutionEvent, ExecutionEventInner, HistoryEvent,
    };
    use concepts::{Params, SupportedFunctionResult};
    use db::inmemory_dao::{tick::TickBasedDbConnection, DbTask};
    use indexmap::IndexMap;
    use simple_worker::SOME_FFQN;
    use std::{fmt::Debug, future::Future, ops::Deref, sync::Arc};
    use test_utils::sim_clock::SimClock;
    use utils::time::now;

    fn set_up() {
        test_utils::set_up();
    }

    async fn tick_fn<DB: DbConnection, W: Worker + Debug, C: ClockFn + 'static>(
        db_connection: DB,
        config: ExecConfig<C>,
        worker: Arc<W>,
        executed_at: DateTime<Utc>,
    ) -> ExecutionProgress {
        trace!("Ticking with {worker:?}");
        let executor = ExecTask {
            db_connection: db_connection.clone(),
            worker,
            config,
            task_limiter: None,
            executor_id: ExecutorId::generate(),
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
    async fn execute_tick_based() {
        set_up();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn,
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
            Arc::new(SimpleWorker { worker_results_rev }),
            0,
            created_at,
            Duration::ZERO,
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
    async fn execute_executor_tick_based_db_task_based() {
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
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect("must be open");
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
            Arc::new(SimpleWorker { worker_results_rev }),
            0,
            created_at,
            Duration::ZERO,
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
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn,
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
        let worker = Arc::new(SimpleWorker {
            worker_results_rev: worker_results_rev.clone(),
        });
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap(),
            worker.clone(),
            exec_config.clone(),
            None,
        );

        let execution_history = create_and_tick(
            created_at,
            db_task.as_db_connection().unwrap(),
            exec_config,
            worker,
            0,
            created_at,
            Duration::ZERO,
            |_, _, _, _| async {
                tokio::time::sleep(Duration::from_secs(1)).await; // non deterministic if not run in madsim
                ExecutionProgress::default()
            },
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

    #[allow(clippy::too_many_arguments)]
    async fn create_and_tick<
        DB: DbConnection,
        W: Worker,
        C: ClockFn,
        T: FnMut(DB, ExecConfig<C>, Arc<W>, DateTime<Utc>) -> F,
        F: Future<Output = ExecutionProgress>,
    >(
        created_at: DateTime<Utc>,
        db_connection: DB,
        exec_config: ExecConfig<C>,
        worker: Arc<W>,
        max_retries: u32,
        executed_at: DateTime<Utc>,
        retry_exp_backoff: Duration,
        mut tick: T,
    ) -> ExecutionHistory {
        // Create an execution
        let execution_id = ExecutionId::generate();
        db_connection
            .create(
                created_at,
                execution_id,
                SOME_FFQN,
                Params::default(),
                None,
                None,
                retry_exp_backoff,
                max_retries,
            )
            .await
            .unwrap();
        // execute!
        tick(
            db_connection.clone(),
            exec_config.clone(),
            worker,
            executed_at,
        )
        .await;
        let execution_history = db_connection.get(execution_id).await.unwrap();
        debug!("Execution history after tick: {execution_history:?}");
        // check that DB contains Created and Locked events.
        assert_matches!(
            execution_history.events.first().unwrap(),
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
        let sim_clock = SimClock::new(now());
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn: sim_clock.clock_fn(),
        };
        let db_connection = TickBasedDbConnection {
            db_task: Arc::new(std::sync::Mutex::new(DbTask::new())),
        };
        let worker = Arc::new(SimpleWorker {
            worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(2),
                (
                    vec![],
                    Err((
                        WorkerError::IntermittentError {
                            reason: StrVariant::Static("fail"),
                            err: anyhow!("").into(),
                        },
                        Version::new(2),
                    )),
                ),
            )]))),
        });
        let retry_exp_backoff = Duration::from_millis(100);
        debug!(now = %sim_clock.now(), "Creating an execution that should fail");
        let execution_history = create_and_tick(
            sim_clock.now(),
            db_connection.clone(),
            exec_config.clone(),
            worker,
            1,
            sim_clock.now(),
            retry_exp_backoff,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_history.events.len());
        assert_matches!(
            &execution_history.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::IntermittentFailure {
                    reason,
                    expires_at,
                },
                created_at: at,
            } if reason.deref() == "fail" && *at == sim_clock.now() && *expires_at == sim_clock.now() + retry_exp_backoff
        );
        let worker = Arc::new(SimpleWorker {
            worker_results_rev: Arc::new(std::sync::Mutex::new(IndexMap::from([(
                Version::new(4),
                (vec![], Ok((SupportedFunctionResult::None, Version::new(4)))),
            )]))),
        });
        // noop until `retry_exp_backoff` expires
        assert!(tick_fn(
            db_connection.clone(),
            exec_config.clone(),
            worker.clone(),
            sim_clock.now(),
        )
        .await
        .executions
        .is_empty());
        // tick again to finish the execution
        sim_clock.sleep(retry_exp_backoff);
        tick_fn(db_connection.clone(), exec_config, worker, sim_clock.now()).await;
        let execution_history = db_connection
            .get(execution_history.execution_id)
            .await
            .unwrap();
        debug!(now = %sim_clock.now(), "Execution history after second tick: {execution_history:?}");
        assert_matches!(
            execution_history.events.get(3).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: at
            } if *at == sim_clock.now()
        );
        assert_matches!(
            execution_history.events.get(4).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
                created_at: finished_at,
            } if *finished_at == sim_clock.now()
        );
    }

    #[tokio::test]
    async fn execution_returning_err_should_be_retried() {
        set_up();
        let created_at = now();
        let clock_fn = move || created_at;
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn,
        };
        let db_connection = TickBasedDbConnection {
            db_task: Arc::new(std::sync::Mutex::new(DbTask::new())),
        };
        let worker = Arc::new(SimpleWorker {
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
        });
        let execution_history = create_and_tick(
            created_at,
            db_connection.clone(),
            exec_config.clone(),
            worker,
            1,
            created_at,
            Duration::ZERO,
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
        let worker = Arc::new(SimpleWorker {
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
        });
        tick_fn(db_connection.clone(), exec_config, worker, created_at).await;
        let execution_history = db_connection
            .get(execution_history.execution_id)
            .await
            .unwrap();
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

        fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
            Some(SOME_FFQN_PTR).into_iter()
        }
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn hanging_lock_should_be_cleaned_and_execution_retried() {
        set_up();
        let sim_clock = SimClock::new(now());
        let exec_config = ExecConfig {
            ffqns: vec![SOME_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_millis(100),
            tick_sleep: Duration::ZERO,
            clock_fn: sim_clock.clock_fn(),
        };
        let db_connection = TickBasedDbConnection {
            db_task: Arc::new(std::sync::Mutex::new(DbTask::new())),
        };
        let lock_watcher = expired_timers_watcher::Task {
            db_connection: db_connection.clone(),
        };

        let worker = Arc::new(SleepyWorker {
            duration: exec_config.lock_expiry + Duration::from_millis(1), // sleep more than allowed by the lock expiry
            result: SupportedFunctionResult::None,
        });
        // Create an execution
        let execution_id = ExecutionId::generate();
        let timeout_duration = Duration::from_millis(300);
        db_connection
            .create(
                sim_clock.now(),
                execution_id,
                SOME_FFQN,
                Params::default(),
                None,
                None,
                timeout_duration,
                1,
            )
            .await
            .unwrap();

        let executor = ExecTask {
            db_connection: db_connection.clone(),
            worker,
            config: exec_config.clone(),
            task_limiter: None,
            executor_id: ExecutorId::generate(),
        };
        let mut first_execution_progress = executor.tick(sim_clock.now()).await.unwrap();
        assert_eq!(1, first_execution_progress.executions.len());
        // Started hanging, wait for lock expiry.
        sim_clock.sleep(exec_config.lock_expiry);
        // cleanup should be called
        let now_after_first_lock_expiry = sim_clock.now();
        {
            debug!(now = %now_after_first_lock_expiry, "Expecting an expired lock");
            let cleanup_progress = executor.tick(now_after_first_lock_expiry).await.unwrap();
            assert!(cleanup_progress.executions.is_empty());
        }
        {
            let expired_locks = lock_watcher
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

        let execution_history = db_connection.get(execution_id).await.unwrap();
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
        sim_clock.sleep(timeout_duration);
        let now_after_first_timeout = sim_clock.now();
        debug!(now = %now_after_first_timeout, "Second execution should hang again and result in a permanent timeout");

        let mut second_execution_progress = executor.tick(now_after_first_timeout).await.unwrap();
        assert_eq!(1, second_execution_progress.executions.len());

        // Started hanging, wait for lock expiry.
        sim_clock.sleep(exec_config.lock_expiry);
        // cleanup should be called
        let now_after_second_lock_expiry = sim_clock.now();
        debug!(now = %now_after_second_lock_expiry, "Expecting the second lock to be expired");
        {
            let cleanup_progress = executor.tick(now_after_second_lock_expiry).await.unwrap();
            assert!(cleanup_progress.executions.is_empty());
        }
        {
            let expired_locks = lock_watcher
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
    }
}
