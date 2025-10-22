use crate::worker::{Worker, WorkerContext, WorkerError, WorkerResult};
use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::RunId;
use concepts::storage::{
    AppendEventsToExecution, AppendRequest, AppendResponseToExecution, DbErrorGeneric,
    DbErrorWrite, DbExecutor, ExecutionLog, JoinSetResponseEvent, JoinSetResponseEventOuter,
    LockedExecution,
};
use concepts::time::{ClockFn, Sleep};
use concepts::{ComponentId, FunctionMetadata, StrVariant, SupportedFunctionReturnValue};
use concepts::{ExecutionId, FunctionFqn, prefixed_ulid::ExecutorId};
use concepts::{
    FinishedExecutionError,
    storage::{ExecutionEventInner, JoinSetResponse, Version},
};
use concepts::{JoinSetId, PermanentFailureKind};
use std::fmt::Display;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{Instrument, Level, Span, debug, error, info, info_span, instrument, trace, warn};

#[derive(Debug, Clone)]
pub struct ExecConfig {
    pub lock_expiry: Duration,
    pub tick_sleep: Duration,
    pub batch_size: u32,
    pub component_id: ComponentId,
    pub task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    pub executor_id: ExecutorId,
}

pub struct ExecTask<C: ClockFn> {
    worker: Arc<dyn Worker>,
    config: ExecConfig,
    clock_fn: C, // Used for obtaining current time when the execution finishes.
    db_exec: Arc<dyn DbExecutor>,
    ffqns: Arc<[FunctionFqn]>,
}

#[derive(derive_more::Debug, Default)]
pub struct ExecutionProgress {
    #[debug(skip)]
    #[allow(dead_code)]
    executions: Vec<(ExecutionId, JoinHandle<()>)>,
}

impl ExecutionProgress {
    #[cfg(feature = "test")]
    pub async fn wait_for_tasks(self) -> Vec<ExecutionId> {
        let mut vec = Vec::new();
        for (exe, join_handle) in self.executions {
            vec.push(exe);
            join_handle.await.unwrap();
        }
        vec
    }
}

#[derive(derive_more::Debug)]
pub struct ExecutorTaskHandle {
    #[debug(skip)]
    is_closing: Arc<AtomicBool>,
    #[debug(skip)]
    abort_handle: AbortHandle,
    component_id: ComponentId,
    executor_id: ExecutorId,
}

impl ExecutorTaskHandle {
    #[instrument(level = Level::DEBUG, name = "executor.close", skip_all, fields(executor_id = %self.executor_id, component_id = %self.component_id))]
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
    #[instrument(level = Level::DEBUG, name = "executor.drop", skip_all, fields(executor_id = %self.executor_id, component_id = %self.component_id))]
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!("Aborting the executor task");
        self.abort_handle.abort();
    }
}

#[cfg(feature = "test")]
pub fn extract_exported_ffqns_noext_test(worker: &dyn Worker) -> Arc<[FunctionFqn]> {
    extract_exported_ffqns_noext(worker)
}

fn extract_exported_ffqns_noext(worker: &dyn Worker) -> Arc<[FunctionFqn]> {
    worker
        .exported_functions()
        .iter()
        .map(|FunctionMetadata { ffqn, .. }| ffqn.clone())
        .collect::<Arc<_>>()
}

impl<C: ClockFn + 'static> ExecTask<C> {
    #[cfg(feature = "test")]
    pub fn new(
        worker: Arc<dyn Worker>,
        config: ExecConfig,
        clock_fn: C,
        db_exec: Arc<dyn DbExecutor>,
        ffqns: Arc<[FunctionFqn]>,
    ) -> Self {
        Self {
            worker,
            config,
            clock_fn,
            db_exec,
            ffqns,
        }
    }

    #[cfg(feature = "test")]
    pub fn new_all_ffqns_test(
        worker: Arc<dyn Worker>,
        config: ExecConfig,
        clock_fn: C,
        db_exec: Arc<dyn DbExecutor>,
    ) -> Self {
        let ffqns = extract_exported_ffqns_noext(worker.as_ref());
        Self {
            worker,
            config,
            clock_fn,
            db_exec,
            ffqns,
        }
    }

    pub fn spawn_new(
        worker: Arc<dyn Worker>,
        config: ExecConfig,
        clock_fn: C,
        db_exec: Arc<dyn DbExecutor>,
        sleep: impl Sleep + 'static,
    ) -> ExecutorTaskHandle {
        let is_closing = Arc::new(AtomicBool::default());
        let is_closing_inner = is_closing.clone();
        let ffqns = extract_exported_ffqns_noext(worker.as_ref());
        let component_id = config.component_id.clone();
        let executor_id = config.executor_id;
        let abort_handle = tokio::spawn(async move {
            debug!(executor_id = %config.executor_id, component_id = %config.component_id, "Spawned executor");
            let task = ExecTask {
                worker,
                config,
                db_exec,
                ffqns: ffqns.clone(),
                clock_fn: clock_fn.clone(),
            };
            while !is_closing_inner.load(Ordering::Relaxed) {
                let _ = task.tick(clock_fn.now(), RunId::generate()).await;

                task.db_exec
                    .wait_for_pending(clock_fn.now(), ffqns.clone(), {
                        let sleep = sleep.clone();
                        Box::pin(async move { sleep.sleep(task.config.tick_sleep).await })})
                    .await;
            }
        })
        .abort_handle();
        ExecutorTaskHandle {
            is_closing,
            abort_handle,
            component_id,
            executor_id,
        }
    }

    fn acquire_task_permits(&self) -> Vec<Option<tokio::sync::OwnedSemaphorePermit>> {
        if let Some(task_limiter) = &self.config.task_limiter {
            let mut locks = Vec::new();
            for _ in 0..self.config.batch_size {
                if let Ok(permit) = task_limiter.clone().try_acquire_owned() {
                    locks.push(Some(permit));
                } else {
                    break;
                }
            }
            locks
        } else {
            let mut vec = Vec::with_capacity(self.config.batch_size as usize);
            for _ in 0..self.config.batch_size {
                vec.push(None);
            }
            vec
        }
    }

    #[cfg(feature = "test")]
    pub async fn tick_test(&self, executed_at: DateTime<Utc>, run_id: RunId) -> ExecutionProgress {
        self.tick(executed_at, run_id).await.unwrap()
    }

    #[cfg(feature = "test")]
    pub async fn tick_test_await(
        &self,
        executed_at: DateTime<Utc>,
        run_id: RunId,
    ) -> Vec<ExecutionId> {
        self.tick(executed_at, run_id)
            .await
            .unwrap()
            .wait_for_tasks()
            .await
    }

    #[instrument(level = Level::TRACE, name = "executor.tick" skip_all, fields(executor_id = %self.config.executor_id, component_id = %self.config.component_id))]
    async fn tick(
        &self,
        executed_at: DateTime<Utc>,
        run_id: RunId,
    ) -> Result<ExecutionProgress, DbErrorGeneric> {
        let locked_executions = {
            let mut permits = self.acquire_task_permits();
            if permits.is_empty() {
                return Ok(ExecutionProgress::default());
            }
            let lock_expires_at = executed_at + self.config.lock_expiry;
            let locked_executions = self
                .db_exec
                .lock_pending(
                    permits.len(), // batch size
                    executed_at,   // fetch expiring before now
                    self.ffqns.clone(),
                    executed_at, // created at
                    self.config.component_id.clone(),
                    self.config.executor_id,
                    lock_expires_at,
                    run_id,
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

        let mut executions = Vec::with_capacity(locked_executions.len());
        for (locked_execution, permit) in locked_executions {
            let execution_id = locked_execution.execution_id.clone();
            let join_handle = {
                let worker = self.worker.clone();
                let db = self.db_exec.clone();
                let clock_fn = self.clock_fn.clone();
                let run_id = locked_execution.run_id;
                let worker_span = info_span!(parent: None, "worker",
                    "otel.name" = format!("worker {}", locked_execution.ffqn),
                    %execution_id, %run_id, ffqn = %locked_execution.ffqn, executor_id = %self.config.executor_id, component_id = %self.config.component_id);
                locked_execution.metadata.enrich(&worker_span);
                tokio::spawn({
                    let worker_span2 = worker_span.clone();
                    async move {
                        let _permit = permit;
                        let res = Self::run_worker(
                            worker,
                            db.as_ref(),
                            execution_deadline,
                            clock_fn,
                            locked_execution,
                            worker_span2,
                        )
                        .await;
                        if let Err(db_error) = res {
                            error!("Got db error `{db_error:?}`, expecting watcher to mark execution as timed out");
                        }
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
        db_exec: &dyn DbExecutor,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
        locked_execution: LockedExecution,
        worker_span: Span,
    ) -> Result<(), DbErrorWrite> {
        debug!("Worker::run starting");
        trace!(
            version = %locked_execution.next_version,
            params = ?locked_execution.params,
            event_history = ?locked_execution.event_history,
            "Worker::run starting"
        );
        let can_be_retried = ExecutionLog::can_be_retried_after(
            locked_execution.intermittent_event_count + 1,
            locked_execution.max_retries,
            locked_execution.retry_exp_backoff,
        );
        let unlock_expiry_on_limit_reached =
            ExecutionLog::compute_retry_duration_when_retrying_forever(
                locked_execution.intermittent_event_count + 1,
                locked_execution.retry_exp_backoff,
            );
        let ctx = WorkerContext {
            execution_id: locked_execution.execution_id.clone(),
            metadata: locked_execution.metadata,
            ffqn: locked_execution.ffqn,
            params: locked_execution.params,
            event_history: locked_execution.event_history,
            responses: locked_execution
                .responses
                .into_iter()
                .map(|outer| outer.event)
                .collect(),
            version: locked_execution.next_version,
            execution_deadline,
            can_be_retried: can_be_retried.is_some(),
            run_id: locked_execution.run_id,
            worker_span,
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
            unlock_expiry_on_limit_reached,
        )? {
            Some(append) => {
                trace!("Appending {append:?}");
                append.append(db_exec).await
            }
            None => Ok(()),
        }
    }

    /// Map the `WorkerError` to an temporary or a permanent failure.
    fn worker_result_to_execution_event(
        execution_id: ExecutionId,
        worker_result: WorkerResult,
        result_obtained_at: DateTime<Utc>,
        parent: Option<(ExecutionId, JoinSetId)>,
        can_be_retried: Option<Duration>,
        unlock_expiry_on_limit_reached: Duration,
    ) -> Result<Option<Append>, DbErrorWrite> {
        Ok(match worker_result {
            WorkerResult::Ok(result, new_version, http_client_traces) => {
                info!(
                    "Execution finished: {}",
                    result.as_pending_state_finished_result()
                );
                let child_finished =
                    parent.map(
                        |(parent_execution_id, parent_join_set)| ChildFinishedResponse {
                            parent_execution_id,
                            parent_join_set,
                            result: result.clone(),
                        },
                    );
                let primary_event = AppendRequest {
                    created_at: result_obtained_at,
                    event: ExecutionEventInner::Finished {
                        result,
                        http_client_traces,
                    },
                };

                Some(Append {
                    created_at: result_obtained_at,
                    primary_event,
                    execution_id,
                    version: new_version,
                    child_finished,
                })
            }
            WorkerResult::DbUpdatedByWorkerOrWatcher => None,
            WorkerResult::Err(err) => {
                let reason_full = err.to_string(); // WorkerError.display() usually contains a variant specific prefix + inner `reason`.
                let (primary_event, child_finished, version) = match err {
                    WorkerError::TemporaryTimeout {
                        http_client_traces,
                        version,
                    } => {
                        if let Some(duration) = can_be_retried {
                            let backoff_expires_at = result_obtained_at + duration;
                            info!(
                                "Temporary timeout, retrying after {duration:?} at {backoff_expires_at}"
                            );
                            (
                                ExecutionEventInner::TemporarilyTimedOut {
                                    backoff_expires_at,
                                    http_client_traces,
                                },
                                None,
                                version,
                            )
                        } else {
                            info!("Permanent timeout");
                            let result = SupportedFunctionReturnValue::ExecutionError(
                                FinishedExecutionError::PermanentTimeout,
                            );
                            let child_finished =
                                parent.map(|(parent_execution_id, parent_join_set)| {
                                    ChildFinishedResponse {
                                        parent_execution_id,
                                        parent_join_set,
                                        result: result.clone(),
                                    }
                                });
                            (
                                ExecutionEventInner::Finished {
                                    result,
                                    http_client_traces,
                                },
                                child_finished,
                                version,
                            )
                        }
                    }
                    WorkerError::DbError(db_error) => {
                        return Err(db_error);
                    }
                    WorkerError::ActivityTrap {
                        reason: reason_inner,
                        trap_kind,
                        detail,
                        version,
                        http_client_traces,
                    } => retry_or_fail(
                        result_obtained_at,
                        parent,
                        can_be_retried,
                        reason_full,
                        reason_inner,
                        trap_kind, // short reason like `trap` for logs
                        detail,
                        version,
                        http_client_traces,
                    ),
                    WorkerError::ActivityPreopenedDirError {
                        reason_kind,
                        reason_inner,
                        version,
                    } => retry_or_fail(
                        result_obtained_at,
                        parent,
                        can_be_retried,
                        reason_full,
                        reason_inner,
                        reason_kind, // short reason for logs
                        None,        // detail
                        version,
                        None, // http_client_traces
                    ),
                    WorkerError::ActivityReturnedError {
                        detail,
                        version,
                        http_client_traces,
                    } => {
                        let duration = can_be_retried.expect(
                            "ActivityReturnedError must not be returned when retries are exhausted",
                        );
                        let expires_at = result_obtained_at + duration;
                        debug!("Retrying ActivityReturnedError after {duration:?} at {expires_at}");
                        (
                            ExecutionEventInner::TemporarilyFailed {
                                backoff_expires_at: expires_at,
                                reason_full: StrVariant::Static("activity returned error"), // is same as the variant's display message.
                                reason_inner: StrVariant::Static("activity returned error"),
                                detail, // contains the backtrace
                                http_client_traces,
                            },
                            None,
                            version,
                        )
                    }
                    WorkerError::LimitReached {
                        reason: inner_reason,
                        version: new_version,
                    } => {
                        let expires_at = result_obtained_at + unlock_expiry_on_limit_reached;
                        warn!(
                            "Limit reached: {inner_reason}, unlocking after {unlock_expiry_on_limit_reached:?} at {expires_at}"
                        );
                        (
                            ExecutionEventInner::Unlocked {
                                backoff_expires_at: expires_at,
                                reason: StrVariant::from(reason_full),
                            },
                            None,
                            new_version,
                        )
                    }
                    WorkerError::FatalError(fatal_error, version) => {
                        info!("Fatal worker error - {fatal_error:?}");
                        let result = SupportedFunctionReturnValue::ExecutionError(
                            FinishedExecutionError::from(fatal_error),
                        );
                        let child_finished =
                            parent.map(|(parent_execution_id, parent_join_set)| {
                                ChildFinishedResponse {
                                    parent_execution_id,
                                    parent_join_set,
                                    result: result.clone(),
                                }
                            });
                        (
                            ExecutionEventInner::Finished {
                                result,
                                http_client_traces: None,
                            },
                            child_finished,
                            version,
                        )
                    }
                };
                Some(Append {
                    created_at: result_obtained_at,
                    primary_event: AppendRequest {
                        created_at: result_obtained_at,
                        event: primary_event,
                    },
                    execution_id,
                    version,
                    child_finished,
                })
            }
        })
    }
}

#[expect(clippy::too_many_arguments)]
fn retry_or_fail(
    result_obtained_at: DateTime<Utc>,
    parent: Option<(ExecutionId, JoinSetId)>,
    can_be_retried: Option<Duration>,
    reason_full: String,
    reason_inner: String,
    err_kind_for_logs: impl Display,
    detail: Option<String>,
    version: Version,
    http_client_traces: Option<Vec<concepts::storage::http_client_trace::HttpClientTrace>>,
) -> (ExecutionEventInner, Option<ChildFinishedResponse>, Version) {
    if let Some(duration) = can_be_retried {
        let expires_at = result_obtained_at + duration;
        debug!(
            "Retrying activity with `{err_kind_for_logs}` execution after {duration:?} at {expires_at}"
        );
        (
            ExecutionEventInner::TemporarilyFailed {
                backoff_expires_at: expires_at,
                reason_full: StrVariant::from(reason_full),
                reason_inner: StrVariant::from(reason_inner),
                detail,
                http_client_traces,
            },
            None,
            version,
        )
    } else {
        info!("Activity with `{err_kind_for_logs}` marked as permanent failure - {reason_inner}");
        let result = SupportedFunctionReturnValue::ExecutionError(
            FinishedExecutionError::PermanentFailure {
                reason_inner,
                reason_full,
                kind: PermanentFailureKind::ActivityTrap,
                detail,
            },
        );

        let child_finished =
            parent.map(
                |(parent_execution_id, parent_join_set)| ChildFinishedResponse {
                    parent_execution_id,
                    parent_join_set,
                    result: result.clone(),
                },
            );
        (
            ExecutionEventInner::Finished {
                result,
                http_client_traces,
            },
            child_finished,
            version,
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChildFinishedResponse {
    pub(crate) parent_execution_id: ExecutionId,
    pub(crate) parent_join_set: JoinSetId,
    pub(crate) result: SupportedFunctionReturnValue,
}

#[derive(Debug, Clone)]
pub(crate) struct Append {
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) primary_event: AppendRequest,
    pub(crate) execution_id: ExecutionId,
    pub(crate) version: Version,
    pub(crate) child_finished: Option<ChildFinishedResponse>,
}

impl Append {
    pub(crate) async fn append(self, db_exec: &dyn DbExecutor) -> Result<(), DbErrorWrite> {
        if let Some(child_finished) = self.child_finished {
            assert_matches!(
                &self.primary_event,
                AppendRequest {
                    event: ExecutionEventInner::Finished { .. },
                    ..
                }
            );
            let derived = assert_matches!(self.execution_id.clone(), ExecutionId::Derived(derived) => derived);
            let events = AppendEventsToExecution {
                execution_id: self.execution_id,
                version: self.version.clone(),
                batch: vec![self.primary_event],
            };
            let response = AppendResponseToExecution {
                parent_execution_id: child_finished.parent_execution_id,
                parent_response_event: JoinSetResponseEventOuter {
                    created_at: self.created_at,
                    event: JoinSetResponseEvent {
                        join_set_id: child_finished.parent_join_set,
                        event: JoinSetResponse::ChildExecutionFinished {
                            child_execution_id: derived,
                            // Since self.primary_event is a finished event, the version will remain the same.
                            finished_version: self.version,
                            result: child_finished.result,
                        },
                    },
                },
            };

            db_exec
                .append_batch_respond_to_parent(events, response, self.created_at)
                .await?;
        } else {
            db_exec
                .append(self.execution_id, self.version, self.primary_event)
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
        FunctionFqn, FunctionMetadata, ParameterTypes, RETURN_TYPE_DUMMY,
        storage::{HistoryEvent, Version},
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
                    return_type: RETURN_TYPE_DUMMY,
                    extension: None,
                    submittable: true,
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
                    return_type: RETURN_TYPE_DUMMY,
                    extension: None,
                    submittable: true,
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
    }
}

#[cfg(test)]
mod tests {
    use self::simple_worker::SimpleWorker;
    use super::*;
    use crate::{expired_timers_watcher, worker::WorkerResult};
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::storage::DbPoolCloseable;
    use concepts::storage::{CreateRequest, DbConnection, JoinSetRequest};
    use concepts::storage::{ExecutionEvent, ExecutionEventInner, HistoryEvent, PendingState};
    use concepts::time::Now;
    use concepts::{
        ClosingStrategy, FunctionMetadata, JoinSetKind, ParameterTypes, Params, RETURN_TYPE_DUMMY,
        SUPPORTED_RETURN_VALUE_OK_EMPTY, StrVariant, SupportedFunctionReturnValue, TrapKind,
    };
    use db_tests::Database;
    use indexmap::IndexMap;
    use simple_worker::FFQN_SOME;
    use std::{fmt::Debug, future::Future, ops::Deref, sync::Arc};
    use test_utils::set_up;
    use test_utils::sim_clock::{ConstClock, SimClock};

    pub(crate) const FFQN_CHILD: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn-child");

    async fn tick_fn<W: Worker + Debug, C: ClockFn + 'static>(
        config: ExecConfig,
        clock_fn: C,
        db_exec: Arc<dyn DbExecutor>,
        worker: Arc<W>,
        executed_at: DateTime<Utc>,
    ) -> Vec<ExecutionId> {
        trace!("Ticking with {worker:?}");
        let ffqns = super::extract_exported_ffqns_noext(worker.as_ref());
        let executor = ExecTask::new(worker, config, clock_fn, db_exec, ffqns);
        executor
            .tick_test_await(executed_at, RunId::generate())
            .await
    }

    #[tokio::test]
    async fn execute_simple_lifecycle_tick_based_mem() {
        let created_at = Now.now();
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        execute_simple_lifecycle_tick_based(
            db_pool.connection().as_ref(),
            db_exec.clone(),
            ConstClock(created_at),
        )
        .await;
        db_close.close().await;
    }

    #[tokio::test]
    async fn execute_simple_lifecycle_tick_based_sqlite() {
        let created_at = Now.now();
        let (_guard, db_pool, db_exec, db_close) = Database::Sqlite.set_up().await;
        execute_simple_lifecycle_tick_based(
            db_pool.connection().as_ref(),
            db_exec.clone(),
            ConstClock(created_at),
        )
        .await;
        db_close.close().await;
    }

    async fn execute_simple_lifecycle_tick_based<C: ClockFn + 'static>(
        db_connection: &dyn DbConnection,
        db_exec: Arc<dyn DbExecutor>,
        clock_fn: C,
    ) {
        set_up();
        let created_at = clock_fn.now();
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::from_millis(100),
            component_id: ComponentId::dummy_activity(),
            task_limiter: None,
            executor_id: ExecutorId::generate(),
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
            db_connection,
            db_exec,
            exec_config,
            Arc::new(SimpleWorker::with_single_result(WorkerResult::Ok(
                SUPPORTED_RETURN_VALUE_OK_EMPTY,
                Version::new(2),
                None,
            ))),
            tick_fn,
        )
        .await;
        assert_matches!(
            execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: SupportedFunctionReturnValue::Ok { ok: None },
                    http_client_traces: None
                },
                created_at: _,
                backtrace_id: None,
            }
        );
    }

    #[tokio::test]
    async fn execute_simple_lifecycle_task_based_mem() {
        set_up();
        let created_at = Now.now();
        let clock_fn = ConstClock(created_at);
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            component_id: ComponentId::dummy_activity(),
            task_limiter: None,
            executor_id: ExecutorId::generate(),
        };

        let worker = Arc::new(SimpleWorker::with_single_result(WorkerResult::Ok(
            SUPPORTED_RETURN_VALUE_OK_EMPTY,
            Version::new(2),
            None,
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
            db_pool.connection().as_ref(),
            db_exec,
            exec_config,
            worker,
            tick_fn,
        )
        .await;
        assert_matches!(
            execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: SupportedFunctionReturnValue::Ok { ok: None },
                    http_client_traces: None
                },
                created_at: _,
                backtrace_id: None,
            }
        );
        db_close.close().await;
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
        T: FnMut(ExecConfig, C, Arc<dyn DbExecutor>, Arc<W>, DateTime<Utc>) -> F,
        F: Future<Output = Vec<ExecutionId>>,
    >(
        config: CreateAndTickConfig,
        clock_fn: C,
        db_connection: &dyn DbConnection,
        db_exec: Arc<dyn DbExecutor>,
        exec_config: ExecConfig,
        worker: Arc<W>,
        mut tick: T,
    ) -> ExecutionLog {
        // Create an execution
        db_connection
            .create(CreateRequest {
                created_at: config.created_at,
                execution_id: config.execution_id.clone(),
                ffqn: FFQN_SOME,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: config.created_at,
                retry_exp_backoff: config.retry_exp_backoff,
                max_retries: config.max_retries,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        // execute!
        tick(exec_config, clock_fn, db_exec, worker, config.executed_at).await;
        let execution_log = db_connection.get(&config.execution_id).await.unwrap();
        debug!("Execution history after tick: {execution_log:?}");
        // check that DB contains Created and Locked events.
        assert_matches!(
            execution_log.events.first().unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Created { .. },
                created_at: actually_created_at,
                backtrace_id: None,
            }
            if config.created_at == *actually_created_at
        );
        let locked_at = assert_matches!(
            execution_log.events.get(1).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: locked_at,
                backtrace_id: None,
            } if config.created_at <= *locked_at
            => *locked_at
        );
        assert_matches!(execution_log.events.get(2).unwrap(), ExecutionEvent {
            event: _,
            created_at: executed_at,
            backtrace_id: None,
        } if *executed_at >= locked_at);
        execution_log
    }

    #[tokio::test]
    async fn activity_trap_should_trigger_an_execution_retry() {
        set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            component_id: ComponentId::dummy_activity(),
            task_limiter: None,
            executor_id: ExecutorId::generate(),
        };
        let expected_reason = "error reason";
        let expected_detail = "error detail";
        let worker = Arc::new(SimpleWorker::with_single_result(WorkerResult::Err(
            WorkerError::ActivityTrap {
                reason: expected_reason.to_string(),
                trap_kind: concepts::TrapKind::Trap,
                detail: Some(expected_detail.to_string()),
                version: Version::new(2),
                http_client_traces: None,
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
            db_pool.connection().as_ref(),
            db_exec.clone(),
            exec_config.clone(),
            worker,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_log.events.len());
        {
            let (reason_full, reason_inner, detail, at, expires_at) = assert_matches!(
                &execution_log.events.get(2).unwrap(),
                ExecutionEvent {
                    event: ExecutionEventInner::TemporarilyFailed {
                        reason_inner,
                        reason_full,
                        detail,
                        backoff_expires_at,
                        http_client_traces: None,
                    },
                    created_at: at,
                    backtrace_id: None,
                }
                => (reason_full, reason_inner, detail, *at, *backoff_expires_at)
            );
            assert_eq!(expected_reason, reason_inner.deref());
            assert_eq!(
                format!("activity trap: {expected_reason}"),
                reason_full.deref()
            );
            assert_eq!(Some(expected_detail), detail.as_deref());
            assert_eq!(at, sim_clock.now());
            assert_eq!(sim_clock.now() + retry_exp_backoff, expires_at);
        }
        let worker = Arc::new(SimpleWorker::with_worker_results_rev(Arc::new(
            std::sync::Mutex::new(IndexMap::from([(
                Version::new(4),
                (
                    vec![],
                    WorkerResult::Ok(SUPPORTED_RETURN_VALUE_OK_EMPTY, Version::new(4), None),
                ),
            )])),
        )));
        // noop until `retry_exp_backoff` expires
        assert!(
            tick_fn(
                exec_config.clone(),
                sim_clock.clone(),
                db_exec.clone(),
                worker.clone(),
                sim_clock.now(),
            )
            .await
            .is_empty()
        );
        // tick again to finish the execution
        sim_clock.move_time_forward(retry_exp_backoff);
        tick_fn(
            exec_config,
            sim_clock.clone(),
            db_exec.clone(),
            worker,
            sim_clock.now(),
        )
        .await;
        let execution_log = {
            let db_connection = db_pool.connection();
            db_connection
                .get(&execution_log.execution_id)
                .await
                .unwrap()
        };
        debug!(now = %sim_clock.now(), "Execution history after second tick: {execution_log:?}");
        assert_matches!(
            execution_log.events.get(3).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                created_at: at,
                backtrace_id: None,
            } if *at == sim_clock.now()
        );
        assert_matches!(
            execution_log.events.get(4).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: SupportedFunctionReturnValue::Ok{ok:None},
                    http_client_traces: None
                },
                created_at: finished_at,
                backtrace_id: None,
            } if *finished_at == sim_clock.now()
        );
        db_close.close().await;
    }

    #[tokio::test]
    async fn activity_trap_should_not_be_retried_if_no_retries_are_set() {
        set_up();
        let created_at = Now.now();
        let clock_fn = ConstClock(created_at);
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            component_id: ComponentId::dummy_activity(),
            task_limiter: None,
            executor_id: ExecutorId::generate(),
        };

        let expected_reason = "error reason";
        let expected_detail = "error detail";
        let worker = Arc::new(SimpleWorker::with_single_result(WorkerResult::Err(
            WorkerError::ActivityTrap {
                reason: expected_reason.to_string(),
                trap_kind: concepts::TrapKind::Trap,
                detail: Some(expected_detail.to_string()),
                version: Version::new(2),
                http_client_traces: None,
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
            db_pool.connection().as_ref(),
            db_exec,
            exec_config.clone(),
            worker,
            tick_fn,
        )
        .await;
        assert_eq!(3, execution_log.events.len());
        let (reason, kind, detail) = assert_matches!(
            &execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::Finished{
                    result: SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError::PermanentFailure{reason_inner, kind, detail, reason_full:_}),
                    http_client_traces: None
                },
                created_at: at,
                backtrace_id: None,
            } if *at == created_at
            => (reason_inner, kind, detail)
        );
        assert_eq!(expected_reason, *reason);
        assert_eq!(Some(expected_detail), detail.as_deref());
        assert_eq!(PermanentFailureKind::ActivityTrap, *kind);

        db_close.close().await;
    }

    #[tokio::test]
    async fn child_execution_permanently_failed_should_notify_parent_permanent_failure() {
        let worker_error = WorkerError::ActivityTrap {
            reason: "error reason".to_string(),
            trap_kind: TrapKind::Trap,
            detail: Some("detail".to_string()),
            version: Version::new(2),
            http_client_traces: None,
        };
        let expected_child_err = FinishedExecutionError::PermanentFailure {
            reason_full: "activity trap: error reason".to_string(),
            reason_inner: "error reason".to_string(),
            kind: PermanentFailureKind::ActivityTrap,
            detail: Some("detail".to_string()),
        };
        child_execution_permanently_failed_should_notify_parent(
            WorkerResult::Err(worker_error),
            expected_child_err,
        )
        .await;
    }

    // TODO: Add test for WorkerError::TemporaryTimeout

    #[tokio::test]
    async fn child_execution_permanently_failed_handled_by_watcher_should_notify_parent_timeout() {
        let expected_child_err = FinishedExecutionError::PermanentTimeout;
        child_execution_permanently_failed_should_notify_parent(
            WorkerResult::DbUpdatedByWorkerOrWatcher,
            expected_child_err,
        )
        .await;
    }

    async fn child_execution_permanently_failed_should_notify_parent(
        worker_result: WorkerResult,
        expected_child_err: FinishedExecutionError,
    ) {
        use concepts::storage::JoinSetResponseEventOuter;
        const LOCK_EXPIRY: Duration = Duration::from_secs(1);

        set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;

        let parent_worker = Arc::new(SimpleWorker::with_single_result(
            WorkerResult::DbUpdatedByWorkerOrWatcher,
        ));
        let parent_execution_id = ExecutionId::generate();
        db_pool
            .connection()
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: parent_execution_id.clone(),
                ffqn: FFQN_SOME,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        tick_fn(
            ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_EXPIRY,
                tick_sleep: Duration::ZERO,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
            },
            sim_clock.clone(),
            db_exec.clone(),
            parent_worker,
            sim_clock.now(),
        )
        .await;

        let join_set_id = JoinSetId::new(JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let child_execution_id = parent_execution_id.next_level(&join_set_id);
        // executor does not append anything, this should have been written by the worker:
        {
            let child = CreateRequest {
                created_at: sim_clock.now(),
                execution_id: ExecutionId::Derived(child_execution_id.clone()),
                ffqn: FFQN_CHILD,
                params: Params::empty(),
                parent: Some((parent_execution_id.clone(), join_set_id.clone())),
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            };
            let current_time = sim_clock.now();
            let join_set = AppendRequest {
                created_at: current_time,
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetCreate {
                        join_set_id: join_set_id.clone(),
                        closing_strategy: ClosingStrategy::Complete,
                    },
                },
            };
            let child_exec_req = AppendRequest {
                created_at: current_time,
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id: join_set_id.clone(),
                        request: JoinSetRequest::ChildExecutionRequest {
                            child_execution_id: child_execution_id.clone(),
                        },
                    },
                },
            };
            let join_next = AppendRequest {
                created_at: current_time,
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinNext {
                        join_set_id: join_set_id.clone(),
                        run_expires_at: sim_clock.now(),
                        closing: false,
                        requested_ffqn: Some(FFQN_CHILD),
                    },
                },
            };
            db_pool
                .connection()
                .append_batch_create_new_execution(
                    current_time,
                    vec![join_set, child_exec_req, join_next],
                    parent_execution_id.clone(),
                    Version::new(2),
                    vec![child],
                )
                .await
                .unwrap();
        }

        let child_worker =
            Arc::new(SimpleWorker::with_single_result(worker_result).with_ffqn(FFQN_CHILD));

        // execute the child
        tick_fn(
            ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_EXPIRY,
                tick_sleep: Duration::ZERO,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
            },
            sim_clock.clone(),
            db_exec.clone(),
            child_worker,
            sim_clock.now(),
        )
        .await;
        if matches!(expected_child_err, FinishedExecutionError::PermanentTimeout) {
            // In case of timeout, let the timers watcher handle it
            sim_clock.move_time_forward(LOCK_EXPIRY);
            expired_timers_watcher::tick(db_pool.connection().as_ref(), sim_clock.now())
                .await
                .unwrap();
        }
        let child_log = db_pool
            .connection()
            .get(&ExecutionId::Derived(child_execution_id.clone()))
            .await
            .unwrap();
        assert!(child_log.pending_state.is_finished());
        assert_eq!(
            Version(2),
            child_log.next_version,
            "created = 0, locked = 1, with_single_result = 2"
        );
        assert_eq!(
            ExecutionEventInner::Finished {
                result: SupportedFunctionReturnValue::ExecutionError(expected_child_err),
                http_client_traces: None
            },
            child_log.last_event().event
        );
        let parent_log = db_pool
            .connection()
            .get(&parent_execution_id)
            .await
            .unwrap();
        assert_matches!(
            parent_log.pending_state,
            PendingState::PendingAt {
                scheduled_at
            } if scheduled_at == sim_clock.now(),
            "parent should be back to pending"
        );
        let (found_join_set_id, found_child_execution_id, child_finished_version, found_result) = assert_matches!(
            parent_log.responses.last(),
            Some(JoinSetResponseEventOuter{
                created_at: at,
                event: JoinSetResponseEvent{
                    join_set_id: found_join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: found_child_execution_id,
                        finished_version,
                        result: found_result,
                    }
                }
            })
             if *at == sim_clock.now()
            => (found_join_set_id, found_child_execution_id, finished_version, found_result)
        );
        assert_eq!(join_set_id, *found_join_set_id);
        assert_eq!(child_execution_id, *found_child_execution_id);
        assert_eq!(child_log.next_version, *child_finished_version);
        assert_matches!(
            found_result,
            SupportedFunctionReturnValue::ExecutionError(_)
        );

        db_close.close().await;
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
            WorkerResult::Ok(self.result.clone(), ctx.version, None)
        }

        fn exported_functions(&self) -> &[FunctionMetadata] {
            &self.exported
        }
    }

    #[tokio::test]
    async fn hanging_lock_should_be_cleaned_and_execution_retried() {
        set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        let lock_expiry = Duration::from_millis(100);
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry,
            tick_sleep: Duration::ZERO,
            component_id: ComponentId::dummy_activity(),
            task_limiter: None,
            executor_id: ExecutorId::generate(),
        };

        let worker = Arc::new(SleepyWorker {
            duration: lock_expiry + Duration::from_millis(1), // sleep more than allowed by the lock expiry
            result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
            exported: [FunctionMetadata {
                ffqn: FFQN_SOME,
                parameter_types: ParameterTypes::default(),
                return_type: RETURN_TYPE_DUMMY,
                extension: None,
                submittable: true,
            }],
        });
        // Create an execution
        let execution_id = ExecutionId::generate();
        let timeout_duration = Duration::from_millis(300);
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_SOME,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: timeout_duration,
                max_retries: 1,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let ffqns = super::extract_exported_ffqns_noext(worker.as_ref());
        let executor = ExecTask::new(
            worker,
            exec_config,
            sim_clock.clone(),
            db_exec.clone(),
            ffqns,
        );
        let mut first_execution_progress = executor
            .tick(sim_clock.now(), RunId::generate())
            .await
            .unwrap();
        assert_eq!(1, first_execution_progress.executions.len());
        // Started hanging, wait for lock expiry.
        sim_clock.move_time_forward(lock_expiry);
        // cleanup should be called
        let now_after_first_lock_expiry = sim_clock.now();
        {
            debug!(now = %now_after_first_lock_expiry, "Expecting an expired lock");
            let cleanup_progress = executor
                .tick(now_after_first_lock_expiry, RunId::generate())
                .await
                .unwrap();
            assert!(cleanup_progress.executions.is_empty());
        }
        {
            let expired_locks = expired_timers_watcher::tick(
                db_pool.connection().as_ref(),
                now_after_first_lock_expiry,
            )
            .await
            .unwrap()
            .expired_locks;
            assert_eq!(1, expired_locks);
        }
        assert!(
            !first_execution_progress
                .executions
                .pop()
                .unwrap()
                .1
                .is_finished()
        );

        let execution_log = db_connection.get(&execution_id).await.unwrap();
        let expected_first_timeout_expiry = now_after_first_lock_expiry + timeout_duration;
        assert_matches!(
            &execution_log.events.get(2).unwrap(),
            ExecutionEvent {
                event: ExecutionEventInner::TemporarilyTimedOut { backoff_expires_at, .. },
                created_at: at,
                backtrace_id: None,
            } if *at == now_after_first_lock_expiry && *backoff_expires_at == expected_first_timeout_expiry
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

        let mut second_execution_progress = executor
            .tick(now_after_first_timeout, RunId::generate())
            .await
            .unwrap();
        assert_eq!(1, second_execution_progress.executions.len());

        // Started hanging, wait for lock expiry.
        sim_clock.move_time_forward(lock_expiry);
        // cleanup should be called
        let now_after_second_lock_expiry = sim_clock.now();
        debug!(now = %now_after_second_lock_expiry, "Expecting the second lock to be expired");
        {
            let cleanup_progress = executor
                .tick(now_after_second_lock_expiry, RunId::generate())
                .await
                .unwrap();
            assert!(cleanup_progress.executions.is_empty());
        }
        {
            let expired_locks = expired_timers_watcher::tick(
                db_pool.connection().as_ref(),
                now_after_second_lock_expiry,
            )
            .await
            .unwrap()
            .expired_locks;
            assert_eq!(1, expired_locks);
        }
        assert!(
            !second_execution_progress
                .executions
                .pop()
                .unwrap()
                .1
                .is_finished()
        );

        drop(db_connection);
        drop(executor);
        db_close.close().await;
    }
}
