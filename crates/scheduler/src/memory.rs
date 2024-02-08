use crate::{ExecutionResult, Worker, WorkerError};
use async_channel::{Receiver, Sender};
use concepts::{workflow_id::WorkflowId, FunctionFqn, Params, SupportedFunctionResult};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinSet},
};
use tracing::{debug, info, info_span, instrument, trace, Instrument, Level};

#[derive(Debug)]
struct QueueEntry {
    workflow_id: WorkflowId,
    // execution_id: ExecutionId,
    params: Params,
    executor_db_sender: oneshot::Sender<ExecutionResult>,
}

#[derive(Debug)]
struct InflightExecution {
    ffqn: FunctionFqn,
    version: u64,
    params: Params,
    created_at: SystemTime,
    updated_at: SystemTime,
    db_client_sender: Option<oneshot::Sender<ExecutionResult>>, // Hack: Always present so it can be taken and used by the listener.
    executor_db_receiver: Option<oneshot::Receiver<ExecutionResult>>, // None if this entry needs to be submitted to the mpmc channel.
    status: InflightExecutionStatus,
}

#[derive(Debug, PartialEq, Eq)]
enum InflightExecutionStatus {
    Submitted,
    Resubmitted,
    IntermittentTimeout {
        retry_index: usize,
        updated_at: SystemTime,
    },
}

#[derive(Debug)]
enum FinishedExecutionStatus {
    Finished { result: SupportedFunctionResult },
    PermanentTimeout,
}

pub enum ExecutionStatusInfo {
    Submitted,
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
        let listener = Self::spawn_listener(
            inflight_executions.clone(),
            db_to_executor_mpmc_queues.clone(),
        );
        Self {
            queue_capacity,
            db_to_executor_mpmc_queues,
            inflight_executions,
            finished_executions: Default::default(),
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
        if self
            .inflight_executions
            .lock()
            .unwrap()
            .contains_key(workflow_id)
        {
            Some(ExecutionStatusInfo::Submitted)
        } else {
            None
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
            let (executor_db_sender, executor_db_receiver) = oneshot::channel();
            let entry = QueueEntry {
                workflow_id: workflow_id.clone(),
                params: params.clone(),
                executor_db_sender,
            };
            let now = SystemTime::now();
            let executor_db_receiver = match db_to_executor_mpmc_sender.try_send(entry) {
                Ok(()) => Some(executor_db_receiver),
                Err(async_channel::TrySendError::Full(_)) => None,
                Err(async_channel::TrySendError::Closed(_)) => {
                    unreachable!("database holds a receiver")
                }
            };
            InflightExecution {
                ffqn,
                version: 0,
                params,
                created_at: now,
                updated_at: now,
                executor_db_receiver,
                status: InflightExecutionStatus::Submitted,
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

    #[instrument(skip_all)]
    fn spawn_listener(
        inflight_executions: Arc<std::sync::Mutex<HashMap<WorkflowId, InflightExecution>>>,
        db_to_executor_mpmc_queues: Arc<
            std::sync::Mutex<HashMap<FunctionFqn, (Sender<QueueEntry>, Receiver<QueueEntry>)>>,
        >,
    ) -> AbortHandle {
        info!("Spawning");
        tokio::spawn(
            async move {
                loop {
                    Self::listener_tick(
                        inflight_executions.lock().unwrap(),
                        &db_to_executor_mpmc_queues,
                    );
                    tokio::time::sleep(Duration::from_micros(10)).await; //FIXME: wake up on a signal
                }
            }
            .instrument(info_span!("db_listener")),
        )
        .abort_handle()
    }

    fn listener_tick(
        mut inflight_executions_guard: std::sync::MutexGuard<
            HashMap<WorkflowId, InflightExecution>,
        >,
        db_to_executor_mpmc_queues: &std::sync::Mutex<
            HashMap<FunctionFqn, (Sender<QueueEntry>, Receiver<QueueEntry>)>,
        >,
    ) {
        inflight_executions_guard.retain(|workflow_id, inflight_execution| {
            info_span!("listener_tick", workflow_id=workflow_id.to_string()).in_scope(||
            match inflight_execution
                .executor_db_receiver
                .as_mut()
                .map(|rec| rec.try_recv())
            {
                Some(Ok(res)) => {
                    debug!("Received result, notifying client");
                    // TODO: Move the execution to finished_executions
                    // notify the client
                    let db_client_sender = inflight_execution
                        .db_client_sender
                        .take()
                        .expect("db_client_sender must have been set in insert");
                    let _ = db_client_sender.send(res);
                    false
                }
                Some(Err(oneshot::error::TryRecvError::Empty)) => {
                    // No response yet, keep the entry.
                    true
                }
                Some(Err(oneshot::error::TryRecvError::Closed)) => {
                    // The executor was aborted while running the execution. Update the execution.
                    inflight_execution.updated_at = SystemTime::now();
                    inflight_execution.version = inflight_execution.version + 1;
                    inflight_execution.status = InflightExecutionStatus::Resubmitted;
                    inflight_execution.executor_db_receiver = {
                        // Attempt to submit the execution to the mpmc channel.
                        // The mpmc channel must be created at this point, obtain its sender.
                        let db_to_executor_mpmc_sender = db_to_executor_mpmc_queues
                            .lock()
                            .unwrap()
                            .get(&inflight_execution.ffqn)
                            .expect("must be created in `insert`")
                            .0
                            .clone();

                        let (executor_db_sender, executor_db_receiver) = oneshot::channel();
                        let entry = QueueEntry {
                            workflow_id: workflow_id.clone(),
                            params: inflight_execution.params.clone(),
                            executor_db_sender,
                        };
                        let send_res = db_to_executor_mpmc_sender.try_send(entry);
                        debug!("Attempted to enqueue the execution after the executor was aborted. Success: {send_res}", send_res = send_res.is_ok());
                        match send_res {
                            Ok(()) => Some(executor_db_receiver),
                            Err(async_channel::TrySendError::Full(_)) => None,
                            Err(async_channel::TrySendError::Closed(_)) => {
                                unreachable!("database holds a receiver")
                            }
                        }
                    };
                    true
                }
                None => {
                    // Attempt to submit the execution to the mpmc channel.
                    todo!()
                }
            })
        });
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
        trace!("Aborting the executor task");
        self.executor_task.abort();
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
    info!("Spawning");
    let executor_task = {
        let receiver = receiver.clone();
        tokio::spawn(async move {
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
                    let _blocking_permits = semaphore.acquire_many(max_tasks - 1).await.unwrap();
                    trace!("All workers have finished");
                    return;
                };
                trace!("Received {workflow_id}");
                let worker = worker.clone();
                let worker_span = info_span!("worker", workflow_id = workflow_id.to_string());
                worker_set.spawn(async move {
                    debug!("Spawned");
                    let execution_result_fut = worker.run(workflow_id.clone(), params);
                    let execution_result = if let Some(max_task_duration) = max_task_duration {
                        tokio::select! {
                            res = execution_result_fut => res,
                            _ = tokio::time::sleep(max_task_duration) => Err(WorkerError::Timeout { workflow_id: workflow_id.clone() })
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
                .instrument(worker_span)
                );
                // TODO: retries with backoff, scheduled tasks
                // TODO: Persist the result atomically
            }
        }
        .instrument(info_span!("executor"))
        ).abort_handle()
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
        let execution = db.insert(
            SOME_FFQN.to_owned(),
            WorkflowId::generate(),
            Params::default(),
        );
        let _executor_abort_handle = db.spawn_executor(SOME_FFQN.to_owned(), SimpleWorker, 1, None);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let resp = execution.await.unwrap();
        assert_eq!(ExecutionResult::Ok(SupportedFunctionResult::None), resp);
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

    struct SleepyWorker(Arc<AtomicBool>);

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
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
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
            SleepyWorker(finished_check.clone()),
            1,
            Some(Duration::from_millis(max_duration_millis)),
        );

        let execute = |millis: u64, workflow_id: WorkflowId| {
            db.insert(
                SOME_FFQN.to_owned(),
                workflow_id,
                Params::from([wasmtime::component::Val::U64(millis)]),
            )
        };
        assert_eq!(
            ExecutionResult::Ok(SupportedFunctionResult::None),
            execute(max_duration_millis / 2, WorkflowId::generate())
                .await
                .unwrap()
        );

        let workflow_id = WorkflowId::generate();
        let res = execute(max_duration_millis * 2, workflow_id.clone())
            .await
            .unwrap();
        assert_eq!(
            ExecutionResult::Err(WorkerError::Timeout { workflow_id }),
            res
        );
        assert!(finished_check.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn inflight_execution_of_aborted_executor_should_restart_on_a_new_executor() {
        set_up();
        let db = InMemoryDatabase::spawn_new(1);
        let finished_check = Arc::new(AtomicBool::new(false));

        let executor_abort_handle = db.spawn_executor(
            SOME_FFQN.to_owned(),
            SleepyWorker(finished_check.clone()),
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
        // Drop the executor's abort handle after 10ms.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(executor_abort_handle);
        });
        // Await execution termination.
        tokio::time::sleep(Duration::from_millis(sleep_millis * 2)).await;
        // Make sure that the worker was aborted and did not mark the execution as finished.
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
            SleepyWorker(finished_check.clone()),
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
            SleepyWorker(finished_check.clone()),
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
}
