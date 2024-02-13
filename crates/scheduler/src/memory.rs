use crate::{
    ExecutionId, ExecutionStatusInfo, FinishedExecutionError, FinishedExecutionResult, Worker,
    WorkerCommand, WorkerError, WorkerExecutionResult,
};
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Utc};
use concepts::{FunctionFqn, Params};
use indexmap::IndexMap;
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinSet},
};
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument, Level};
use tracing_unwrap::{OptionExt, ResultExt};

/// Executor disconnect detection:
/// This implementation relies on oneshot channels, which simulate a TCP connection in a way.
/// When the oneshot channel is closed, we know we need to re-enqueue the execution.
/// An alternate implementation might exist where we would use
/// versioning, health checks and timeouts to detect when an executor got stuck or disconnected.

// Done:
// Execution insertion, enqueueing, executing on a worker.
// Timeouts (permanent)
// Handling executor abortion (re-enqueueing)
// Enqueueing in the background when a queue is full during insertion.
// Handling of panics in workers

// TODO:
// * feat: WorkerCommand::DelayUntil
// * feat: dependent executions: workflow-activity, parent-child workflows
// * refactor: remove old db, runtime
// * fix: consistency between `inflight_executions` and `finished_executions`
// * feat: retries on timeouts
// * feat: retries on errors
// * feat: schedule the execution into the future
// * feat: retries with exponential backoff
// * feat: execution id, regenerate between retries
// * feat: execution history - 1.pending, 2. enqueued..
// * resil: limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.
// * resil: Fairness: reenqueue should push the execution to the end of `inflight_executions`
// * perf: wake up the listener by insertion and executor by pushing a workflow id to an mpsc channel.
#[derive(Debug)]
struct QueueEntry<S: Debug, E: ExecutionId> {
    execution_id: E,
    params: Params,
    store: S,
    executor_db_sender: oneshot::Sender<WorkerExecutionResult>,
}

#[derive(Debug)]
struct InflightExecution<S> {
    ffqn: FunctionFqn,
    params: Params,
    store: S,
    db_client_sender: Option<oneshot::Sender<FinishedExecutionResult>>, // Hack: Always present so it can be taken and used by the listener.
    // FIXME: Move to InflightExecutionStatus::Enqueued
    executor_db_receiver: Option<oneshot::Receiver<WorkerExecutionResult>>, // None if this entry needs to be submitted to the mpmc channel.
    status: InflightExecutionStatus,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, derive_more::Display)]
enum InflightExecutionStatus {
    Pending,
    Enqueued,
    DelayedUntil(DateTime<Utc>),
    // #[display(fmt = "IntermittentTimeout({retry_index})")]
    // IntermittentTimeout {
    //     retry_index: usize,
    // },
}

#[derive(Debug)]
pub struct InMemoryDatabase<S: Debug, E: ExecutionId> {
    queue_capacity: usize,
    // Single writer: `insert`. Read by db listener(reenqueue) and `spawn_executor`(receiver).
    db_to_executor_mpmc_queues: Arc<
        std::sync::Mutex<
            HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
        >,
    >,
    // Written by both `insert` and the listener (update status).
    inflight_executions: Arc<std::sync::Mutex<IndexMap<E, InflightExecution<S>>>>,
    // Written by the listener.
    finished_executions: Arc<std::sync::Mutex<HashMap<E, FinishedExecutionResult>>>,
    listener: AbortHandle,
}

impl<S: Clone + Debug + Send + 'static, E: ExecutionId> InMemoryDatabase<S, E> {
    pub fn spawn_new(queue_capacity: usize) -> Self {
        let inflight_executions: Arc<std::sync::Mutex<IndexMap<E, InflightExecution<S>>>> =
            Default::default();
        let db_to_executor_mpmc_queues: Arc<
            std::sync::Mutex<
                HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
            >,
        > = Default::default();
        let finished_executions: Arc<std::sync::Mutex<HashMap<E, FinishedExecutionResult>>> =
            Default::default();
        let listener = Self::spawn_listener(
            inflight_executions.clone(),
            db_to_executor_mpmc_queues.clone(),
            finished_executions.clone(),
        );
        Self {
            queue_capacity,
            db_to_executor_mpmc_queues,
            inflight_executions,
            finished_executions,
            listener,
        }
    }

    pub fn get_execution_status(&self, execution_id: &E) -> Option<ExecutionStatusInfo> {
        if let Some(res) = self
            .finished_executions
            .lock()
            .unwrap_or_log()
            .get(execution_id)
        {
            return Some(ExecutionStatusInfo::Finished(res.clone()));
        };
        match self
            .inflight_executions
            .lock()
            .unwrap_or_log()
            .get(execution_id)
            .map(|found| found.status)
        {
            Some(InflightExecutionStatus::Pending) => Some(ExecutionStatusInfo::Pending),
            Some(InflightExecutionStatus::Enqueued) => Some(ExecutionStatusInfo::Enqueued),
            Some(InflightExecutionStatus::DelayedUntil(delay)) => {
                Some(ExecutionStatusInfo::DelayedUntil(delay))
            }
            None => None,
        }
    }

    #[instrument(skip_all, fields(%ffqn, %execution_id))]
    pub fn insert_or_schedule(
        &self,
        ffqn: FunctionFqn,
        execution_id: E,
        params: Params,
        store: S,
        delayed_until: Option<DateTime<Utc>>,
    ) -> oneshot::Receiver<FinishedExecutionResult> {
        // make sure the mpmc channel is created, obtain its sender
        let db_to_executor_mpmc_sender = self
            .db_to_executor_mpmc_queues
            .lock()
            .unwrap_or_log()
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .0
            .clone();

        let (db_client_sender, db_client_receiver) = oneshot::channel();
        // Attempt to enqueue the execution.
        let submitted = {
            let (executor_db_receiver, status) = if let Some(delay) = delayed_until {
                (None, InflightExecutionStatus::DelayedUntil(delay))
            } else {
                Self::attempt_to_enqueue(
                    db_to_executor_mpmc_sender,
                    execution_id.clone(),
                    params.clone(),
                    store.clone(),
                    None,
                )
            };
            InflightExecution {
                ffqn,
                params,
                executor_db_receiver,
                status,
                db_client_sender: Some(db_client_sender),
                store,
            }
        };
        // Save the execution
        self.inflight_executions
            .lock()
            .unwrap_or_log()
            .insert(execution_id, submitted);

        db_client_receiver
    }

    fn attempt_to_enqueue(
        db_to_executor_mpmc_sender: Sender<QueueEntry<S, E>>,
        execution_id: E,
        params: Params,
        store: S,
        old_status: Option<InflightExecutionStatus>,
    ) -> (
        Option<oneshot::Receiver<WorkerExecutionResult>>,
        InflightExecutionStatus,
    ) {
        let (executor_db_sender, executor_db_receiver) = oneshot::channel();
        let entry = QueueEntry {
            execution_id,
            params,
            store,
            executor_db_sender,
        };
        let send_res = db_to_executor_mpmc_sender.try_send(entry);
        let (executor_db_receiver, new_status) = match send_res {
            Ok(()) => (
                Some(executor_db_receiver),
                InflightExecutionStatus::Enqueued,
            ),
            Err(async_channel::TrySendError::Full(_)) => (None, InflightExecutionStatus::Pending),
            Err(async_channel::TrySendError::Closed(_)) => {
                unreachable!("database holds a receiver")
            }
        };
        debug!("Attempted to enqueue the execution, {old_status:?} -> {new_status}");
        (executor_db_receiver, new_status)
    }

    fn spawn_executor<W: Worker<S, E> + Send + Sync + 'static>(
        &self,
        ffqn: FunctionFqn,
        worker: W,
        max_tasks: u32,
        max_task_duration: Option<Duration>,
    ) -> ExecutorAbortHandle<S, E> {
        let receiver = self
            .db_to_executor_mpmc_queues
            .lock()
            .unwrap_or_log()
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .1
            .clone();
        spawn_executor(ffqn, receiver, worker, max_tasks, max_task_duration)
    }

    fn spawn_listener(
        inflight_executions: Arc<std::sync::Mutex<IndexMap<E, InflightExecution<S>>>>,
        db_to_executor_mpmc_queues: Arc<
            std::sync::Mutex<
                HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
            >,
        >,
        finished_executions: Arc<std::sync::Mutex<HashMap<E, FinishedExecutionResult>>>,
    ) -> AbortHandle {
        tokio::spawn(
            async move {
                loop {
                    Self::listener_tick(
                        &inflight_executions,
                        &db_to_executor_mpmc_queues,
                        &finished_executions,
                    );
                    tokio::time::sleep(Duration::from_micros(10)).await;
                }
            }
            .instrument(info_span!("db_listener")),
        )
        .abort_handle()
    }

    #[instrument(skip_all)]
    /// Responsible for
    /// * purging finished executions from `inflight_executions`
    /// * (re)enqueueing executions to the mpmc queue.
    fn listener_tick(
        inflight_executions: &std::sync::Mutex<IndexMap<E, InflightExecution<S>>>,
        db_to_executor_mpmc_queues: &std::sync::Mutex<
            HashMap<FunctionFqn, (Sender<QueueEntry<S, E>>, Receiver<QueueEntry<S, E>>)>,
        >,
        finished_executions: &std::sync::Mutex<HashMap<E, FinishedExecutionResult>>,
    ) {
        let mut finished = Vec::new();

        // Set the execution to Pending,Enqueued or DelayedUntil.
        let attempt_to_enqueue =
            |inflight_execution: &mut InflightExecution<S>,
             execution_id: E,
             mut delay: Option<DateTime<Utc>>| {
                if let Some(delay_inner) = delay {
                    if delay_inner <= Utc::now() {
                        delay = None;
                    }
                }
                let (executor_db_receiver, status) = {
                    if let Some(delay) = delay {
                        (None, InflightExecutionStatus::DelayedUntil(delay))
                    } else {
                        // Attempt to submit the execution to the mpmc channel.
                        let db_to_executor_mpmc_sender = db_to_executor_mpmc_queues
                            .lock()
                            .unwrap_or_log()
                            .get(&inflight_execution.ffqn)
                            .expect("must be created in `insert`")
                            .0
                            .clone();
                        Self::attempt_to_enqueue(
                            db_to_executor_mpmc_sender,
                            execution_id,
                            inflight_execution.params.clone(),
                            inflight_execution.store.clone(),
                            Some(inflight_execution.status),
                        )
                    }
                };
                inflight_execution.executor_db_receiver = executor_db_receiver;
                inflight_execution.status = status;
            };

        inflight_executions.lock().unwrap_or_log().retain(
            |execution_id, mut inflight_execution| {
                info_span!("listener_tick", %execution_id).in_scope(|| {
                    match inflight_execution
                        .executor_db_receiver
                        .as_mut()
                        .map(|rec| rec.try_recv())
                    {
                        Some(Ok(res)) => {
                            assert_eq!(
                                InflightExecutionStatus::Enqueued,
                                inflight_execution.status
                            );
                            if tracing::enabled!(Level::TRACE) {
                                trace!("Received result: {res:?}");
                            } else {
                                debug!("Received result");
                            }
                            match res {
                                Ok(WorkerCommand::PublishResult(supported_res)) => {
                                    let finished_status = Ok(supported_res);
                                    finished.push((execution_id.clone(), finished_status.clone()));
                                    // Attempt to notify the client.
                                    let db_client_sender = inflight_execution
                                        .db_client_sender
                                        .take()
                                        .expect("db_client_sender must have been set in insert");
                                    let _ = db_client_sender.send(finished_status);
                                    false
                                }
                                Err(WorkerError::Uncategorized) => {
                                    let finished_status =
                                        Err(FinishedExecutionError::UncategorizedError);
                                    finished.push((execution_id.clone(), finished_status.clone()));
                                    // Attempt to notify the client.
                                    let db_client_sender = inflight_execution
                                        .db_client_sender
                                        .take()
                                        .expect("db_client_sender must have been set in insert");
                                    let _ = db_client_sender.send(finished_status);
                                    false
                                }
                                Err(WorkerError::Timeout) => {
                                    // TODO: reenqueue a retry.
                                    let finished_status =
                                        Err(FinishedExecutionError::PermanentTimeout);
                                    finished.push((execution_id.clone(), finished_status.clone()));
                                    // Attempt to notify the client.
                                    let db_client_sender = inflight_execution
                                        .db_client_sender
                                        .take()
                                        .expect("db_client_sender must have been set in insert");
                                    let _ = db_client_sender.send(finished_status);
                                    false
                                }
                                Ok(WorkerCommand::EnqueueNow) => {
                                    // reenqueue now
                                    assert_eq!(
                                        InflightExecutionStatus::Enqueued,
                                        inflight_execution.status
                                    );
                                    attempt_to_enqueue(
                                        &mut inflight_execution,
                                        execution_id.clone(),
                                        None,
                                    );
                                    true
                                }
                                Ok(WorkerCommand::DelayUntil(delay)) => {
                                    // reenqueue later
                                    assert_eq!(
                                        InflightExecutionStatus::Enqueued,
                                        inflight_execution.status
                                    );
                                    attempt_to_enqueue(
                                        &mut inflight_execution,
                                        execution_id.clone(),
                                        Some(delay.clone()),
                                    );
                                    true
                                }
                            }
                        }
                        Some(Err(oneshot::error::TryRecvError::Empty)) => {
                            assert_eq!(
                                InflightExecutionStatus::Enqueued,
                                inflight_execution.status
                            );
                            // No response yet, keep the entry.
                            true
                        }
                        Some(Err(oneshot::error::TryRecvError::Closed)) => {
                            // The executor was aborted while running the execution. Reenqueue.
                            assert_eq!(
                                InflightExecutionStatus::Enqueued,
                                inflight_execution.status
                            );
                            attempt_to_enqueue(&mut inflight_execution, execution_id.clone(), None);
                            true
                        }
                        None => {
                            let delayed = match inflight_execution.status {
                                InflightExecutionStatus::Pending => None,
                                InflightExecutionStatus::DelayedUntil(delay) => Some(delay),
                                InflightExecutionStatus::Enqueued => {
                                    unreachable!()
                                }
                            };
                            attempt_to_enqueue(
                                &mut inflight_execution,
                                execution_id.clone(),
                                delayed,
                            );
                            true
                        }
                    }
                })
            },
        );
        finished_executions
            .lock()
            .unwrap_or_log()
            .extend(finished.into_iter())
    }

    pub async fn close(self) {
        todo!()
    }
}

pub struct ExecutorAbortHandle<S: Debug, E: ExecutionId> {
    ffqn: FunctionFqn,
    executor_task: AbortHandle,
    receiver: Receiver<QueueEntry<S, E>>,
}

impl<S: Debug, E: ExecutionId> ExecutorAbortHandle<S, E> {
    /// Graceful shutdown. Waits until all workers terminate.
    ///
    /// # Panics
    ///
    /// All senders must be closed, otherwise this function will panic.
    #[instrument(skip_all, fields(ffqn = %self.ffqn))]
    pub async fn close(self) {
        // Signal to the executor task.
        self.receiver.close();
        debug!("Gracefully closing");
        while !self.executor_task.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        info!("Gracefully closed");
    }
}

impl<S: Debug, E: ExecutionId> Drop for ExecutorAbortHandle<S, E> {
    #[instrument(skip_all, fields(ffqn = %self.ffqn))]
    fn drop(&mut self) {
        if !self.executor_task.is_finished() {
            trace!("Aborting the executor task");
            self.executor_task.abort();
        }
    }
}

#[instrument(skip_all, fields(%ffqn))]
fn spawn_executor<
    S: Debug + Send + 'static,
    W: Worker<S, E> + Send + Sync + 'static,
    E: ExecutionId,
>(
    ffqn: FunctionFqn,
    receiver: Receiver<QueueEntry<S, E>>,
    worker: W,
    max_tasks: u32,
    max_task_duration: Option<Duration>,
) -> ExecutorAbortHandle<S, E> {
    assert!(max_tasks > 0, "`max_tasks` must be greater than zero");
    let worker = Arc::new(worker);
    let executor_task = {
        let receiver = receiver.clone();
        tokio::spawn(
            async move {
                info!("Spawned executor");
                let mut worker_set = JoinSet::new(); // All worker tasks are cancelled on drop.

                // Add a dummy task so that worker_set.join never returns None
                worker_set.spawn(async {
                    loop {
                        tokio::time::sleep(Duration::from_secs(u64::MAX)).await
                    }
                });

                let semaphore = Arc::new(tokio::sync::Semaphore::new(
                    usize::try_from(max_tasks).expect("usize from u32 should not fail"),
                ));
                // type WorkerTaskVal = (oneshot::Sender<ExecutionResult>, WorkflowId);
                let mut worker_ids_to_worker_task_vals = IndexMap::new();

                let handle_joined = |worker_ids_to_oneshot_senders: &mut IndexMap<_, _>, joined: Result<_, tokio::task::JoinError>| {
                    match joined {
                        Ok((worker_id, execution_result)) => {
                            let (executor_db_sender, execution_id): (oneshot::Sender<WorkerExecutionResult>, E) = worker_ids_to_oneshot_senders.swap_remove(&worker_id).unwrap_or_log();
                            info_span!("joined", %execution_id).in_scope(|| {
                                let send_res = executor_db_sender.send(execution_result);
                                if send_res.is_err() {
                                    debug!("Cannot send the result back to db");
                                }
                            });
                        },
                        Err(join_error) => {
                            let (executor_db_sender, execution_id): (oneshot::Sender<WorkerExecutionResult>, E) = worker_ids_to_oneshot_senders.swap_remove(&join_error.id()).unwrap_or_log();
                            info_span!("joined_error", %execution_id).in_scope(|| {
                                error!("Got uncategorized worker join error. Panic: {panic}", panic = join_error.is_panic());
                                let send_res = executor_db_sender.send(Err(WorkerError::Uncategorized));
                                if send_res.is_err() {
                                    debug!("Cannot send the worker failure back to db");
                                }
                            });
                        }
                    }
                };

                loop {
                    trace!(
                        "Available permits: {permits}",
                        permits = semaphore.available_permits()
                    );

                    let permit = loop {
                        let joined = tokio::select! {
                            joined = worker_set.join_next_with_id() => {
                                joined.expect_or_log("dummy task never finishes")
                            },
                            permit = semaphore.clone().acquire_owned() => {
                                // The permit to be moved to the new task or to grace shutdown.
                                break permit.unwrap_or_log()
                            },
                        };
                        handle_joined(&mut worker_ids_to_worker_task_vals, joined);
                    };
                    trace!("Got permit to receive");
                    let recv = loop {
                        let joined = tokio::select!(
                            joined = worker_set.join_next_with_id() => {
                                joined.expect_or_log("dummy task never finishes")
                            },
                            recv = receiver.recv() => break recv,
                        );
                        handle_joined(&mut worker_ids_to_worker_task_vals, joined);
                    };
                    let Ok(QueueEntry {
                        execution_id,
                        params,
                        store,
                        executor_db_sender,
                    }) = recv
                    else {
                        info!("Graceful shutdown detected, waiting for inflight workers");
                        // Drain the worker set, except for the dummy task.
                        while worker_set.len() > 1 {
                            let joined = worker_set.join_next_with_id().await.unwrap_or_log();
                            handle_joined(&mut worker_ids_to_worker_task_vals, joined);
                        }
                        trace!("All workers have finished");
                        return;
                    };
                    let worker = worker.clone();
                    let worker_span = info_span!("worker", %execution_id);
                    worker_span.in_scope(|| debug!("Spawning worker"));
                    let worker_id = {
                        let execution_id = execution_id.clone();
                        worker_set.spawn(
                            async move {
                                debug!("Spawned worker");
                                let execution_result_fut = worker.run(execution_id, params, store);
                                let execution_result =
                                    if let Some(max_task_duration) = max_task_duration {
                                        tokio::select! {
                                            res = execution_result_fut => res,
                                            _ = tokio::time::sleep(max_task_duration) => Err(WorkerError::Timeout)
                                        }
                                    } else {
                                        execution_result_fut.await
                                    };
                                if tracing::enabled!(tracing::Level::TRACE) {
                                    trace!("Finished: {execution_result:?}");
                                } else {
                                    debug!("Finished");
                                }
                                drop(permit);
                                execution_result
                            }
                            .instrument(worker_span)
                        ).id()
                    };
                    worker_ids_to_worker_task_vals.insert(worker_id, (executor_db_sender, execution_id));
                }
            }
            .instrument(info_span!("executor")),
        )
        .abort_handle()
    };
    ExecutorAbortHandle {
        ffqn,
        executor_task,
        receiver,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutionStatusInfo, FinishedExecutionError, Worker, WorkerExecutionResult};
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, SupportedFunctionResult};
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        time::Instant,
    };
    use tracing_unwrap::OptionExt;

    static INIT: std::sync::Once = std::sync::Once::new();
    fn set_up() {
        INIT.call_once(|| {
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_ansi(false))
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        });
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    #[tokio::test]
    async fn test_simple_workflow() {
        set_up();

        struct SimpleWorker;

        #[async_trait]
        impl Worker<(), WorkflowId> for SimpleWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: (),
            ) -> WorkerExecutionResult {
                WorkerExecutionResult::Ok(WorkerCommand::PublishResult(
                    SupportedFunctionResult::None,
                ))
            }
        }

        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            Default::default(),
            None,
        );
        let _executor_abort_handle = db.spawn_executor(SOME_FFQN.to_owned(), SimpleWorker, 1, None);
        let resp = execution.await.unwrap_or_log();
        assert_eq!(Ok(SupportedFunctionResult::None), resp);
        assert_eq!(
            Some(ExecutionStatusInfo::Finished(Ok(
                SupportedFunctionResult::None
            ))),
            db.get_execution_status(&execution_id)
        );
    }

    #[tokio::test]
    async fn test_semaphore_check_that_no_more_than_max_tasks_are_inflight() {
        set_up();

        struct SemaphoreWorker(tokio::sync::Semaphore);

        #[async_trait]
        impl Worker<(), WorkflowId> for SemaphoreWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: (),
            ) -> WorkerExecutionResult {
                trace!("acquiring");
                let _permit = self.0.try_acquire().unwrap_or_log();
                trace!("sleeping");
                tokio::time::sleep(Duration::from_millis(100)).await;
                trace!("done!");
                WorkerExecutionResult::Ok(WorkerCommand::PublishResult(
                    SupportedFunctionResult::None,
                ))
            }
        }

        let db = InMemoryDatabase::spawn_new(10);
        let max_tasks = 3;
        let executions = (0..max_tasks * 2)
            .map(|_| {
                db.insert_or_schedule(
                    SOME_FFQN.to_owned(),
                    WorkflowId::generate(),
                    Params::default(),
                    Default::default(),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let workflow_worker = SemaphoreWorker(tokio::sync::Semaphore::new(
            usize::try_from(max_tasks).unwrap_or_log(),
        ));
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), workflow_worker, max_tasks, None);
        for execution in executions {
            assert_eq!(
                Ok(SupportedFunctionResult::None),
                execution.await.unwrap_or_log()
            );
        }
    }

    struct SleepyWorker(Option<Arc<AtomicBool>>);

    #[async_trait]
    impl Worker<(), WorkflowId> for SleepyWorker {
        #[instrument(skip_all)]
        async fn run(
            &self,
            _execution_id: WorkflowId,
            params: Params,
            _store: (),
        ) -> WorkerExecutionResult {
            assert_eq!(params.len(), 1);
            let millis = params[0].clone();
            let millis = assert_matches!(millis, wasmtime::component::Val::U64(millis) => millis);
            trace!("sleeping for {millis} ms");
            tokio::time::sleep(Duration::from_millis(millis)).await;
            trace!("done!");
            if let Some(finished_check) = &self.0 {
                assert_eq!(false, finished_check.swap(true, Ordering::SeqCst));
            }
            WorkerExecutionResult::Ok(WorkerCommand::PublishResult(SupportedFunctionResult::None))
        }
    }

    #[tokio::test]
    async fn long_execution_should_timeout() {
        set_up();
        let db = InMemoryDatabase::spawn_new(1);
        let finished_check = Arc::new(AtomicBool::new(false));
        let max_duration_millis = 100;
        let _executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            Some(Duration::from_millis(max_duration_millis)),
        );
        let execution_id = WorkflowId::generate();
        let res = db
            .insert_or_schedule(
                SOME_FFQN.to_owned(),
                execution_id.clone(),
                Params::from([wasmtime::component::Val::U64(max_duration_millis * 2)]),
                Default::default(),
                None,
            )
            .await
            .unwrap_or_log();
        assert_eq!(false, finished_check.load(Ordering::SeqCst));
        assert_eq!(Err(FinishedExecutionError::PermanentTimeout), res);
        assert_eq!(
            Some(ExecutionStatusInfo::Finished(Err(
                FinishedExecutionError::PermanentTimeout
            ))),
            db.get_execution_status(&execution_id)
        );
    }

    #[tokio::test]
    async fn two_executors_should_work_in_parallel() {
        set_up();
        let db = InMemoryDatabase::spawn_new(2);
        let sleep_millis = 100;
        let _executor_1 = db.spawn_executor(SOME_FFQN.to_owned(), SleepyWorker(None), 1, None);
        let _executor_2 = db.spawn_executor(SOME_FFQN.to_owned(), SleepyWorker(None), 1, None);
        let stopwatch = Instant::now();
        let fut_1 = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
            Default::default(),
            None,
        );
        let fut_2 = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
            Default::default(),
            None,
        );
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            fut_1.await.unwrap_or_log()
        );
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            fut_2.await.unwrap_or_log()
        );
        let stopwatch = Instant::now().duration_since(stopwatch);
        assert!(stopwatch < Duration::from_millis(sleep_millis) * 2,);
    }

    #[tokio::test]
    async fn inflight_execution_of_aborted_executor_should_restart_on_a_new_executor() {
        set_up();
        let db = InMemoryDatabase::spawn_new(1);
        let finished_check = Arc::new(AtomicBool::new(false));

        let executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            None,
        );

        let sleep_millis = 100;
        let execution_id = WorkflowId::generate();
        let mut execution = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            execution_id,
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
            Default::default(),
            None,
        );
        // Drop the executor after 10ms.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(executor_abort_handle);
        });
        // Make sure that the worker task was aborted and did not mark the execution as finished.
        tokio::time::sleep(Duration::from_millis(sleep_millis * 2)).await;
        assert_eq!(false, finished_check.load(Ordering::SeqCst));
        assert_eq!(
            oneshot::error::TryRecvError::Empty,
            execution.try_recv().unwrap_err_or_log()
        );
        // Abandoned execution should be picked by another worker spawned the new executor.
        let _executor = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            None,
        );
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        assert_eq!(true, finished_check.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        set_up();
        let db = InMemoryDatabase::spawn_new(1);
        let sleep_millis = 100;
        let execution = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
            Default::default(),
            None,
        );
        let finished_check = Arc::new(AtomicBool::new(false));
        let executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            None,
        );
        tokio::time::sleep(Duration::from_millis(sleep_millis / 2)).await;
        // Close should block until the execution is done.
        executor_abort_handle.close().await;
        assert_eq!(true, finished_check.load(Ordering::SeqCst));
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
    }

    #[tokio::test]
    async fn execution_added_to_full_mpmc_queue_should_finish() {
        const QUEUE_SIZE: usize = 1;
        set_up();
        let db = InMemoryDatabase::spawn_new(QUEUE_SIZE);
        let max_duration_millis = 100;
        let _executor = db.spawn_executor(SOME_FFQN.to_owned(), SleepyWorker(None), 1, None);
        let execute = |execution_id: WorkflowId| {
            (
                execution_id.clone(),
                db.insert_or_schedule(
                    SOME_FFQN.to_owned(),
                    execution_id,
                    Params::from([wasmtime::component::Val::U64(max_duration_millis)]),
                    Default::default(),
                    None,
                ),
            )
        };
        let executions = (0..QUEUE_SIZE + 1)
            .map(|idx| execute(WorkflowId::new(idx.to_string())))
            .collect::<Vec<_>>();
        assert_eq!(
            Some(ExecutionStatusInfo::Enqueued),
            db.get_execution_status(&executions[0].0)
        );
        assert_eq!(
            Some(ExecutionStatusInfo::Pending),
            db.get_execution_status(&executions.last().unwrap_or_log().0)
        );
        for execution in executions {
            assert_eq!(
                Ok(SupportedFunctionResult::None),
                execution.1.await.unwrap_or_log()
            );
            assert_eq!(
                Some(ExecutionStatusInfo::Finished(Ok(
                    SupportedFunctionResult::None
                ))),
                db.get_execution_status(&execution.0)
            );
        }
    }

    #[tokio::test]
    async fn test_panic_in_worker() {
        set_up();
        struct PanicingWorker;
        #[async_trait]
        impl Worker<(), WorkflowId> for PanicingWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: (),
            ) -> WorkerExecutionResult {
                panic!();
            }
        }
        let db = InMemoryDatabase::spawn_new(10);
        let execution = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::default(),
            Default::default(),
            None,
        );
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), PanicingWorker, 1, None);
        let execution = execution.await.unwrap_or_log();
        assert_matches!(execution, Err(FinishedExecutionError::UncategorizedError));
    }

    #[tokio::test]
    async fn test_partial_progress() {
        set_up();
        struct PartialProgressWorker {
            is_waiting: Arc<AtomicBool>,
            should_finish: Arc<AtomicBool>,
        }

        #[async_trait]
        impl Worker<(), WorkflowId> for PartialProgressWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: (),
            ) -> WorkerExecutionResult {
                if self.should_finish.load(Ordering::SeqCst) {
                    trace!("Worker finished");
                    WorkerExecutionResult::Ok(WorkerCommand::PublishResult(
                        SupportedFunctionResult::None,
                    ))
                } else {
                    trace!("Worker waiting");
                    self.is_waiting.store(true, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    WorkerExecutionResult::Ok(WorkerCommand::EnqueueNow)
                }
            }
        }

        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            Default::default(),
            None,
        );
        let is_waiting = Arc::new(AtomicBool::new(false));
        let should_finish = Arc::new(AtomicBool::new(false));
        let _executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            PartialProgressWorker {
                is_waiting: is_waiting.clone(),
                should_finish: should_finish.clone(),
            },
            1,
            None,
        );
        loop {
            if is_waiting.load(Ordering::SeqCst) {
                break;
            }
            trace!("Waiting for in progress");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(!db
            .get_execution_status(&execution_id)
            .unwrap_or_log()
            .is_finished());

        should_finish.store(true, Ordering::SeqCst);
        let status = loop {
            let status = db.get_execution_status(&execution_id).unwrap_or_log();
            if status.is_finished() {
                break status;
            }
            trace!("Waiting to finish");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(
            ExecutionStatusInfo::Finished(Ok(SupportedFunctionResult::None)),
            status
        );
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
    }

    #[tokio::test]
    async fn test_partial_progress_with_store_and_shared_atomics() {
        set_up();
        struct PartialProgressWorker;

        #[derive(Default, Debug, Clone)]
        struct PartialStore {
            is_waiting: Arc<AtomicBool>,
            should_finish: Arc<AtomicBool>,
        }
        let store = PartialStore::default();
        let is_waiting = store.is_waiting.clone();
        let should_finish = store.should_finish.clone();

        #[async_trait]
        impl Worker<PartialStore, WorkflowId> for PartialProgressWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                store: PartialStore,
            ) -> WorkerExecutionResult {
                if store.should_finish.load(Ordering::SeqCst) {
                    trace!("Worker finished");
                    WorkerExecutionResult::Ok(WorkerCommand::PublishResult(
                        SupportedFunctionResult::None,
                    ))
                } else {
                    trace!("Worker waiting");
                    store.is_waiting.store(true, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    WorkerExecutionResult::Ok(WorkerCommand::EnqueueNow)
                }
            }
        }
        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            store,
            None,
        );
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), PartialProgressWorker, 1, None);
        loop {
            if is_waiting.load(Ordering::SeqCst) {
                break;
            }
            debug!("Waiting for in progress");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(!db
            .get_execution_status(&execution_id)
            .unwrap_or_log()
            .is_finished());

        should_finish.store(true, Ordering::SeqCst);
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        assert_eq!(
            ExecutionStatusInfo::Finished(Ok(SupportedFunctionResult::None)),
            db.get_execution_status(&execution_id).unwrap_or_log()
        );
    }

    #[tokio::test]
    async fn test_partial_progress_with_arc_mutex_store() {
        set_up();
        struct PartialProgressWorker;

        #[derive(Default, Debug)]
        struct PartialStore {
            is_waiting: bool,
            should_finish: bool,
        }
        let store = Arc::new(tokio::sync::Mutex::new(PartialStore::default()));

        #[async_trait]
        impl Worker<Arc<tokio::sync::Mutex<PartialStore>>, WorkflowId> for PartialProgressWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                store: Arc<tokio::sync::Mutex<PartialStore>>,
            ) -> WorkerExecutionResult {
                let mut store = store.lock().await;
                if store.should_finish {
                    trace!("Worker finished");
                    WorkerExecutionResult::Ok(WorkerCommand::PublishResult(
                        SupportedFunctionResult::None,
                    ))
                } else {
                    trace!("Worker waiting");
                    store.is_waiting = true;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    WorkerExecutionResult::Ok(WorkerCommand::EnqueueNow)
                }
            }
        }
        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let execution = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            store.clone(),
            None,
        );
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), PartialProgressWorker, 1, None);
        loop {
            let store = store.lock().await;
            if store.is_waiting {
                break;
            }
            debug!("Waiting for in progress");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(!db
            .get_execution_status(&execution_id)
            .unwrap_or_log()
            .is_finished());

        store.lock().await.should_finish = true;
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        assert_eq!(
            ExecutionStatusInfo::Finished(Ok(SupportedFunctionResult::None)),
            db.get_execution_status(&execution_id).unwrap_or_log()
        );
    }

    #[tokio::test]
    async fn test_simple_scheduled_workflow() {
        let delay = Duration::from_millis(500);
        let max_accepted_delay = delay * 2;
        set_up();
        struct SimpleWorker;
        #[async_trait]
        impl Worker<(), WorkflowId> for SimpleWorker {
            async fn run(
                &self,
                _execution_id: WorkflowId,
                _params: Params,
                _store: (),
            ) -> WorkerExecutionResult {
                WorkerExecutionResult::Ok(WorkerCommand::PublishResult(
                    SupportedFunctionResult::None,
                ))
            }
        }

        let db = InMemoryDatabase::spawn_new(1);
        let execution_id = WorkflowId::generate();
        let now = Utc::now();
        let delay = now + delay;
        let execution = db.insert_or_schedule(
            SOME_FFQN.to_owned(),
            execution_id.clone(),
            Params::default(),
            Default::default(),
            Some(delay),
        );
        let _executor_abort_handle = db.spawn_executor(SOME_FFQN.to_owned(), SimpleWorker, 1, None);
        // check that it is scheduled.
        let status = loop {
            if let Some(status) = db.get_execution_status(&execution_id) {
                break status;
            }
            debug!("Waiting for in progress");
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        assert_matches!(status, ExecutionStatusInfo::DelayedUntil(found_delay) if found_delay == delay);
        let stopwatch = Instant::now();
        assert_eq!(
            Ok(SupportedFunctionResult::None),
            execution.await.unwrap_or_log()
        );
        let stopwatch = Instant::now().duration_since(stopwatch);
        assert!(
            stopwatch < max_accepted_delay,
            "Scheduled execution exceeded accepted delay: {stopwatch:?}"
        );
    }
}
