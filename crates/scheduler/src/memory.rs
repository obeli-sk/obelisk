use crate::{ExecutionResult, Worker, WorkerError};
use async_channel::{Receiver, Sender};
use concepts::{workflow_id::WorkflowId, FunctionFqn, Params, SupportedFunctionResult};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinSet},
};
use tracing::{debug, info, info_span, instrument, trace, Instrument, Level};

// Done:
// Execution insertion, enqueueing, executing on a worker.
// Timeouts (permanent)
// Handling executor abortion (re-enqueueing)
// Enqueueing in the background when a queue is full during insertion.

// TODO:
// * feat: interrupts - partial progress + event history passing
// * refactor: remove old db, runtime
// * fix: detect worker corruption e.g. after panic
// * feat: dependent workflows (parent-child)
// * feat: retries on timeouts
// * feat: retries on errors
// * feat: schedule the execution into the future
// * feat: retries with exponential backoff
// * feat: execution id, regenerate between retries
// * feat: execution history - 1.pending, 2. enqueued..
// * resil: limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.
// * perf: wake up the listener by insertion and executor by pushing a workflow id to an mpsc channel.
#[derive(Debug)]
struct QueueEntry {
    workflow_id: WorkflowId,
    params: Params,
    executor_db_sender: oneshot::Sender<ExecutionResult>,
}

#[derive(Debug)]
struct InflightExecution {
    ffqn: FunctionFqn,
    params: Params,
    db_client_sender: Option<oneshot::Sender<ExecutionResult>>, // Hack: Always present so it can be taken and used by the listener.
    executor_db_receiver: Option<oneshot::Receiver<ExecutionResult>>, // None if this entry needs to be submitted to the mpmc channel.
    status: InflightExecutionStatus,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, derive_more::Display)]
enum InflightExecutionStatus {
    Pending,
    Enqueued,
    // #[display(fmt = "IntermittentTimeout({retry_index})")]
    // IntermittentTimeout {
    //     retry_index: usize,
    // },
}

#[derive(Debug, Clone)]
enum FinishedExecutionStatus {
    Finished { result: SupportedFunctionResult },
    PermanentTimeout,
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum ExecutionStatusInfo {
    Pending,
    Enqueued,
    Finished(SupportedFunctionResult),
    PermanentTimeout,
}

#[derive(Debug)]
pub struct InMemoryDatabase {
    queue_capacity: usize,
    db_to_executor_mpmc_queues:
        Arc<std::sync::Mutex<HashMap<FunctionFqn, (Sender<QueueEntry>, Receiver<QueueEntry>)>>>,
    inflight_executions: Arc<std::sync::Mutex<HashMap<WorkflowId, InflightExecution>>>,
    finished_executions: Arc<std::sync::Mutex<HashMap<WorkflowId, FinishedExecutionStatus>>>,
    listener: AbortHandle,
}

impl InMemoryDatabase {
    pub fn spawn_new(queue_capacity: usize) -> Self {
        let inflight_executions: Arc<std::sync::Mutex<HashMap<WorkflowId, InflightExecution>>> =
            Default::default();
        let db_to_executor_mpmc_queues: Arc<
            std::sync::Mutex<HashMap<FunctionFqn, (Sender<QueueEntry>, Receiver<QueueEntry>)>>,
        > = Default::default();
        let finished_executions: Arc<
            std::sync::Mutex<HashMap<WorkflowId, FinishedExecutionStatus>>,
        > = Default::default();
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

    pub fn get_execution_status(&self, workflow_id: &WorkflowId) -> Option<ExecutionStatusInfo> {
        match self.finished_executions.lock().unwrap().get(workflow_id) {
            Some(FinishedExecutionStatus::Finished { result }) => {
                return Some(ExecutionStatusInfo::Finished(result.clone()))
            }
            Some(FinishedExecutionStatus::PermanentTimeout) => {
                return Some(ExecutionStatusInfo::PermanentTimeout)
            }
            None => {}
        };
        match self
            .inflight_executions
            .lock()
            .unwrap()
            .get(workflow_id)
            .map(|found| found.status)
        {
            Some(InflightExecutionStatus::Pending) => Some(ExecutionStatusInfo::Pending),
            Some(InflightExecutionStatus::Enqueued) => Some(ExecutionStatusInfo::Enqueued),
            None => None,
        }
    }

    #[instrument(skip_all, fields(ffqn = ffqn.to_string(), workflow_id = workflow_id.to_string()))]
    pub fn insert(
        &self,
        ffqn: FunctionFqn,
        workflow_id: WorkflowId,
        params: Params,
    ) -> oneshot::Receiver<ExecutionResult> {
        // make sure the mpmc channel is created, obtain its sender
        let db_to_executor_mpmc_sender = self
            .db_to_executor_mpmc_queues
            .lock()
            .unwrap()
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .0
            .clone();

        let (db_client_sender, db_client_receiver) = oneshot::channel();
        // Attempt to enqueue the execution.
        let submitted = {
            let (executor_db_receiver, status) = Self::attempt_to_enqueue(
                db_to_executor_mpmc_sender,
                workflow_id.clone(),
                params.clone(),
                None,
            );
            InflightExecution {
                ffqn,
                params,
                executor_db_receiver,
                status,
                db_client_sender: Some(db_client_sender),
            }
        };
        // Save the execution
        self.inflight_executions
            .lock()
            .unwrap()
            .insert(workflow_id, submitted);

        db_client_receiver
    }

    fn attempt_to_enqueue(
        db_to_executor_mpmc_sender: Sender<QueueEntry>,
        workflow_id: WorkflowId,
        params: Params,
        old_status: Option<InflightExecutionStatus>,
    ) -> (
        Option<oneshot::Receiver<ExecutionResult>>,
        InflightExecutionStatus,
    ) {
        let (executor_db_sender, executor_db_receiver) = oneshot::channel();
        let entry = QueueEntry {
            workflow_id,
            params,
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

    fn spawn_executor<W: Worker + Send + Sync + 'static>(
        &self,
        ffqn: FunctionFqn,
        worker: W,
        max_tasks: u32,
        max_task_duration: Option<Duration>,
    ) -> ExecutorAbortHandle {
        let receiver = self
            .db_to_executor_mpmc_queues
            .lock()
            .unwrap()
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .1
            .clone();
        spawn_executor(ffqn, receiver, worker, max_tasks, max_task_duration)
    }

    fn spawn_listener(
        inflight_executions: Arc<std::sync::Mutex<HashMap<WorkflowId, InflightExecution>>>,
        db_to_executor_mpmc_queues: Arc<
            std::sync::Mutex<HashMap<FunctionFqn, (Sender<QueueEntry>, Receiver<QueueEntry>)>>,
        >,
        finished_executions: Arc<std::sync::Mutex<HashMap<WorkflowId, FinishedExecutionStatus>>>,
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
    fn listener_tick(
        inflight_executions: &std::sync::Mutex<HashMap<WorkflowId, InflightExecution>>,
        db_to_executor_mpmc_queues: &std::sync::Mutex<
            HashMap<FunctionFqn, (Sender<QueueEntry>, Receiver<QueueEntry>)>,
        >,
        finished_executions: &std::sync::Mutex<HashMap<WorkflowId, FinishedExecutionStatus>>,
    ) {
        let mut finished = Vec::new();

        inflight_executions
            .lock()
            .unwrap()
            .retain(|workflow_id, inflight_execution| {
                info_span!("listener_tick", workflow_id = workflow_id.to_string()).in_scope(|| {
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
                            debug!("Received result, notifying client");
                            let finished_status = match &res {
                                Ok(supported_res) => FinishedExecutionStatus::Finished {
                                    result: supported_res.clone(),
                                },
                                Err(WorkerError::Timeout) => {
                                    FinishedExecutionStatus::PermanentTimeout
                                }
                            };
                            finished.push((workflow_id.clone(), finished_status));
                            // notify the client
                            let db_client_sender = inflight_execution
                                .db_client_sender
                                .take()
                                .expect("db_client_sender must have been set in insert");
                            let _ = db_client_sender.send(res);
                            false
                        }
                        Some(Err(oneshot::error::TryRecvError::Empty)) => {
                            assert_eq!(
                                InflightExecutionStatus::Enqueued,
                                inflight_execution.status
                            );
                            // No response yet, keep the entry.
                            true
                        }
                        recv @ Some(Err(oneshot::error::TryRecvError::Closed)) | recv @ None => {
                            if recv.is_some() {
                                // The executor was aborted while running the execution. Update the execution.
                                assert_eq!(
                                    InflightExecutionStatus::Enqueued,
                                    inflight_execution.status
                                );
                            } else {
                                assert_eq!(
                                    InflightExecutionStatus::Pending,
                                    inflight_execution.status
                                );
                            }
                            let (executor_db_receiver, status) = {
                                // Attempt to submit the execution to the mpmc channel.
                                let db_to_executor_mpmc_sender = db_to_executor_mpmc_queues
                                    .lock()
                                    .unwrap()
                                    .get(&inflight_execution.ffqn)
                                    .expect("must be created in `insert`")
                                    .0
                                    .clone();
                                Self::attempt_to_enqueue(
                                    db_to_executor_mpmc_sender,
                                    workflow_id.clone(),
                                    inflight_execution.params.clone(),
                                    Some(inflight_execution.status),
                                )
                            };
                            inflight_execution.executor_db_receiver = executor_db_receiver;
                            inflight_execution.status = status;
                            true
                        }
                    }
                })
            });
        finished_executions
            .lock()
            .unwrap()
            .extend(finished.into_iter())
    }

    pub async fn close(self) {
        todo!()
    }
}

pub struct ExecutorAbortHandle {
    ffqn: FunctionFqn,
    executor_task: AbortHandle,
    receiver: Receiver<QueueEntry>,
}

impl ExecutorAbortHandle {
    /// Graceful shutdown. Waits until all workers terminate.
    ///
    /// # Panics
    ///
    /// All senders must be closed, otherwise this function will panic.
    #[instrument(skip_all, fields(ffqn = self.ffqn.to_string()))]
    pub async fn close(self) {
        self.receiver.close();
        debug!("Gracefully closing");
        while !self.executor_task.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        info!("Gracefully closed");
    }
}

impl Drop for ExecutorAbortHandle {
    #[instrument(skip_all, fields(ffqn = self.ffqn.to_string()))]
    fn drop(&mut self) {
        if !self.executor_task.is_finished() {
            trace!("Aborting the executor task");
            self.executor_task.abort();
        }
    }
}

#[instrument(skip_all, fields(ffqn = ffqn.to_string()))]
fn spawn_executor<W: Worker + Send + Sync + 'static>(
    ffqn: FunctionFqn,
    receiver: Receiver<QueueEntry>,
    worker: W,
    max_tasks: u32,
    max_task_duration: Option<Duration>,
) -> ExecutorAbortHandle {
    assert!(max_tasks > 0, "`max_tasks` must be greater than zero");
    let worker = Arc::new(worker);
    let executor_task = {
        let receiver = receiver.clone();
        tokio::spawn(
            async move {
                info!("Spawned executor");
                let mut worker_set = JoinSet::new(); // All worker tasks are cancelled on drop.
                let semaphore = Arc::new(tokio::sync::Semaphore::new(
                    usize::try_from(max_tasks).expect("usize from u32 should not fail"),
                ));
                loop {
                    trace!(
                        "Available permits: {permits}",
                        permits = semaphore.available_permits()
                    );
                    // The permit to be moved to the new task or to grace shutdown.
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    // receive next entry
                    let Ok(QueueEntry {
                        workflow_id,
                        params,
                        executor_db_sender,
                    }) = receiver.recv().await
                    else {
                        info!("Graceful shutdown detected, waiting for inflight workers");
                        // We already have `permit`. Acquiring all other permits.
                        let _blocking_permits =
                            semaphore.acquire_many(max_tasks - 1).await.unwrap();
                        trace!("All workers have finished");
                        return;
                    };
                    let worker = worker.clone();
                    let worker_span = info_span!("worker", workflow_id = workflow_id.to_string());
                    worker_set.spawn(
                        async move {
                            debug!("Spawned worker");
                            let execution_result_fut = worker.run(workflow_id.clone(), params);
                            let execution_result =
                                if let Some(max_task_duration) = max_task_duration {
                                    tokio::select! {
                                        res = execution_result_fut => res,
                                        _ = tokio::time::sleep(max_task_duration) => Err(WorkerError::Timeout)
                                    }
                                } else {
                                    execution_result_fut.await
                                };
                            if tracing::enabled!(Level::TRACE) {
                                trace!("Finished: {execution_result:?}");
                            } else {
                                debug!("Finished");
                            }
                            if executor_db_sender.send(execution_result).is_err() {
                                debug!("Cannot send the result back to db");
                            }
                            drop(permit);
                        }
                        .instrument(worker_span),
                    );
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
    use crate::{ExecutionResult, Worker};
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, SupportedFunctionResult};
    use std::sync::atomic::AtomicBool;

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
        impl Worker for SimpleWorker {
            async fn run(&self, _workflow_id: WorkflowId, _params: Params) -> ExecutionResult {
                ExecutionResult::Ok(SupportedFunctionResult::None)
            }
        }

        let db = InMemoryDatabase::spawn_new(1);
        let workflow_id = WorkflowId::generate();
        let execution = db.insert(SOME_FFQN.to_owned(), workflow_id.clone(), Params::default());
        let _executor_abort_handle = db.spawn_executor(SOME_FFQN.to_owned(), SimpleWorker, 1, None);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let resp = execution.await.unwrap();
        assert_eq!(ExecutionResult::Ok(SupportedFunctionResult::None), resp);
        assert_eq!(
            Some(ExecutionStatusInfo::Finished(SupportedFunctionResult::None)),
            db.get_execution_status(&workflow_id)
        );
    }

    #[tokio::test]
    async fn test_semaphore_check_that_no_more_than_max_tasks_are_inflight() {
        set_up();

        struct SemaphoreWorker(tokio::sync::Semaphore);

        #[async_trait]
        impl Worker for SemaphoreWorker {
            async fn run(&self, workflow_id: WorkflowId, _params: Params) -> ExecutionResult {
                trace!("[{workflow_id}] acquiring");
                let _permit = self.0.try_acquire().unwrap();
                trace!("[{workflow_id}] sleeping");
                tokio::time::sleep(Duration::from_millis(100)).await;
                trace!("[{workflow_id}] done!");
                ExecutionResult::Ok(SupportedFunctionResult::None)
            }
        }

        let db = InMemoryDatabase::spawn_new(10);
        let max_tasks = 3;
        let executions = (0..max_tasks * 2)
            .map(|_| {
                db.insert(
                    SOME_FFQN.to_owned(),
                    WorkflowId::generate(),
                    Params::default(),
                )
            })
            .collect::<Vec<_>>();
        let workflow_worker = SemaphoreWorker(tokio::sync::Semaphore::new(
            usize::try_from(max_tasks).unwrap(),
        ));
        let _executor_abort_handle =
            db.spawn_executor(SOME_FFQN.to_owned(), workflow_worker, max_tasks, None);
        for execution in executions {
            assert_eq!(
                ExecutionResult::Ok(SupportedFunctionResult::None),
                execution.await.unwrap()
            );
        }
    }

    struct SleepyWorker(Option<Arc<AtomicBool>>);

    #[async_trait]
    impl Worker for SleepyWorker {
        #[instrument(skip_all)]
        async fn run(&self, _workflow_id: WorkflowId, params: Params) -> ExecutionResult {
            assert_eq!(params.len(), 1);
            let millis = params[0].clone();
            let millis = assert_matches!(millis, wasmtime::component::Val::U64(millis) => millis);
            trace!("sleeping for {millis} ms");
            tokio::time::sleep(Duration::from_millis(millis)).await;
            trace!("done!");
            if let Some(finished_check) = &self.0 {
                assert_eq!(
                    false,
                    finished_check.swap(true, std::sync::atomic::Ordering::SeqCst)
                );
            }
            ExecutionResult::Ok(SupportedFunctionResult::None)
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
        let workflow_id = WorkflowId::generate();
        let res = db
            .insert(
                SOME_FFQN.to_owned(),
                workflow_id.clone(),
                Params::from([wasmtime::component::Val::U64(max_duration_millis * 2)]),
            )
            .await
            .unwrap();
        assert_eq!(
            false,
            finished_check.load(std::sync::atomic::Ordering::SeqCst)
        );
        assert_eq!(ExecutionResult::Err(WorkerError::Timeout), res);
        assert_eq!(
            Some(ExecutionStatusInfo::PermanentTimeout),
            db.get_execution_status(&workflow_id)
        );
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
        let workflow_id = WorkflowId::generate();
        let mut execution = db.insert(
            SOME_FFQN.to_owned(),
            workflow_id,
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
        );
        // Drop the executor after 10ms.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(executor_abort_handle);
        });
        // Make sure that the worker task was aborted and did not mark the execution as finished.
        tokio::time::sleep(Duration::from_millis(sleep_millis * 2)).await;
        assert_eq!(
            false,
            finished_check.load(std::sync::atomic::Ordering::SeqCst)
        );
        assert_eq!(
            oneshot::error::TryRecvError::Empty,
            execution.try_recv().unwrap_err()
        );
        // Abandoned execution should be picked by another worker spawned the new executor.
        let _executor = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(Some(finished_check.clone())),
            1,
            None,
        );
        assert_eq!(
            ExecutionResult::Ok(SupportedFunctionResult::None),
            execution.await.unwrap()
        );
        assert_eq!(
            true,
            finished_check.load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        set_up();
        let db = InMemoryDatabase::spawn_new(1);
        let sleep_millis = 100;
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::from([wasmtime::component::Val::U64(sleep_millis)]),
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
        assert_eq!(
            true,
            finished_check.load(std::sync::atomic::Ordering::SeqCst)
        );
        assert_eq!(
            ExecutionResult::Ok(SupportedFunctionResult::None),
            execution.await.unwrap()
        );
    }

    #[tokio::test]
    async fn execution_added_to_full_mpmc_queue_should_finish() {
        const QUEUE_SIZE: usize = 1;
        set_up();
        let db = InMemoryDatabase::spawn_new(QUEUE_SIZE);
        let max_duration_millis = 100;
        let _executor = db.spawn_executor(SOME_FFQN.to_owned(), SleepyWorker(None), 1, None);
        let execute = |workflow_id: WorkflowId| {
            (
                workflow_id.clone(),
                db.insert(
                    SOME_FFQN.to_owned(),
                    workflow_id,
                    Params::from([wasmtime::component::Val::U64(max_duration_millis)]),
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
            db.get_execution_status(&executions.last().unwrap().0)
        );
        for execution in executions {
            assert_eq!(
                ExecutionResult::Ok(SupportedFunctionResult::None),
                execution.1.await.unwrap()
            );
            assert_eq!(
                Some(ExecutionStatusInfo::Finished(SupportedFunctionResult::None)),
                db.get_execution_status(&execution.0)
            );
        }
    }
}
