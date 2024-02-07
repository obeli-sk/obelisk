use crate::{EnqueueError, ExecutionResult, QueueWriter, Worker, WorkerError};
use async_channel::{Receiver, Sender, TrySendError};
use concepts::{workflow_id::WorkflowId, FunctionFqn};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinSet},
};
use tracing::{debug, info, trace, Level};

#[derive(Debug)]
struct QueueEntry {
    workflow_id: WorkflowId,
    // execution_id: ExecutionId,
    params: Vec<wasmtime::component::Val>,
    oneshot_sender: oneshot::Sender<ExecutionResult>,
}

#[derive(Debug, Clone)]
pub struct InMemoryDatabase {
    queue_capacity: usize,
    queues: Arc<std::sync::Mutex<HashMap<FunctionFqn, (Sender<QueueEntry>, Receiver<QueueEntry>)>>>,
}

impl InMemoryDatabase {
    pub fn new(queue_capacity: usize) -> Self {
        Self {
            queue_capacity,
            queues: Default::default(),
        }
    }

    fn sender(&self, ffqn: FunctionFqn) -> Sender<QueueEntry> {
        self.queues
            .lock()
            .unwrap()
            .entry(ffqn)
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .0
            .clone()
    }

    fn receiver(&self, ffqn: FunctionFqn) -> Receiver<QueueEntry> {
        self.queues
            .lock()
            .unwrap()
            .entry(ffqn)
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .1
            .clone()
    }
}

#[derive(Debug)]
pub struct InMemoryQueueWriter {
    sender: Sender<QueueEntry>,
    database: InMemoryDatabase, // Avoid closed channel
}

impl InMemoryQueueWriter {
    fn build(ffqn: FunctionFqn, database: InMemoryDatabase) -> Self {
        let sender = database.sender(ffqn);
        Self { sender, database }
    }
}

impl QueueWriter for InMemoryQueueWriter {
    fn enqueue(
        &self,
        workflow_id: WorkflowId,
        params: Vec<wasmtime::component::Val>,
    ) -> Result<oneshot::Receiver<ExecutionResult>, EnqueueError> {
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        let entry = QueueEntry {
            workflow_id,
            params,
            oneshot_sender,
        };
        match self.sender.try_send(entry) {
            Ok(()) => Ok(oneshot_receiver),
            Err(TrySendError::Full(entry)) => Err(EnqueueError::Full {
                workflow_id: entry.workflow_id,
                params: entry.params,
            }),
            Err(TrySendError::Closed(_)) => panic!("channel cannot be closed"),
        }
    }
}

pub struct QueueReaderTask<W: Worker> {
    ffqn: FunctionFqn,
    receiver: Receiver<QueueEntry>,
    worker: Arc<W>,
    _database: InMemoryDatabase, // Avoid closing the async channel
}

pub struct QueueReaderTaskAbortHandle {
    ffqn: FunctionFqn,
    main_task: AbortHandle,
}

impl Drop for QueueReaderTaskAbortHandle {
    fn drop(&mut self) {
        info!("Dropping {ffqn} queue reader", ffqn = self.ffqn);
        self.main_task.abort();
    }
}

impl<W: Worker + Send + Sync + 'static> QueueReaderTask<W> {
    pub fn new(ffqn: FunctionFqn, worker: W, database: InMemoryDatabase) -> Self {
        let receiver = database.receiver(ffqn.clone());
        Self {
            ffqn,
            receiver,
            worker: Arc::new(worker),
            _database: database,
        }
    }

    pub fn spawn(
        self,
        max_tasks: usize,
        max_task_duration: Option<Duration>,
    ) -> QueueReaderTaskAbortHandle {
        assert!(max_tasks > 0, "`max_tasks` must be greater than zero");
        let ffqn = self.ffqn.clone();
        let main_task = tokio::spawn(async move {
            let mut worker_set = JoinSet::new(); // All worker tasks are cancelled on drop.
            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_tasks));
            loop {
                trace!(
                    "[{ffqn}] Available permits: {permits}",
                    permits = semaphore.available_permits()
                );
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                // receive next entry
                let QueueEntry {
                    workflow_id,
                    params,
                    oneshot_sender,
                } = self
                    .receiver
                    .recv()
                    .await
                    .expect("channel cannot be closed");
                let worker = self.worker.clone();
                let ffqn = ffqn.clone();
                worker_set.spawn(async move {
                    debug!("[{ffqn}, {workflow_id}] spawned");
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
                        trace!("[{ffqn}, {workflow_id}] finished: {execution_result:?}");
                    } else {
                        debug!("[{ffqn}, {workflow_id}] finished");
                    }
                    let _ = oneshot_sender.send(execution_result);
                    drop(permit);
                });
                // TODO: retries with backoff, scheduled tasks
                // TODO: Persist the result atomically
            }
        }).abort_handle();
        QueueReaderTaskAbortHandle {
            ffqn: self.ffqn,
            main_task,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::{ExecutionResult, Worker};
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, SupportedFunctionResult};

    static INIT: std::sync::Once = std::sync::Once::new();
    fn set_up() {
        INIT.call_once(|| {
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        });
    }

    const SIMPLE_WORKFLOW_FFQN: FunctionFqnStr = FunctionFqnStr::new("ifc_fqn", "simple-workflow");

    #[tokio::test]
    async fn test_simple_workflow() {
        set_up();

        struct SimpleWorkflowWorker;

        #[async_trait]
        impl Worker for SimpleWorkflowWorker {
            async fn run(
                &self,
                _workflow_id: WorkflowId,
                _params: Vec<wasmtime::component::Val>,
            ) -> ExecutionResult {
                ExecutionResult::Ok(SupportedFunctionResult::None)
            }
        }

        let database = InMemoryDatabase::new(1);
        let queue_writer =
            InMemoryQueueWriter::build(SIMPLE_WORKFLOW_FFQN.to_owned(), database.clone());
        let execution = queue_writer
            .enqueue(WorkflowId::generate(), Vec::new())
            .unwrap();
        let workflow_worker = SimpleWorkflowWorker;
        let worker_task =
            QueueReaderTask::new(SIMPLE_WORKFLOW_FFQN.to_owned(), workflow_worker, database);
        let _close_handler = worker_task.spawn(1, None);
        let resp = execution.await.unwrap();
        assert_eq!(ExecutionResult::Ok(SupportedFunctionResult::None), resp);
    }

    #[tokio::test]
    async fn test_semaphore_check_that_no_more_than_max_tasks_are_inflight() {
        set_up();

        struct SemaphoreWorker(tokio::sync::Semaphore);

        #[async_trait]
        impl Worker for SemaphoreWorker {
            async fn run(
                &self,
                workflow_id: WorkflowId,
                _params: Vec<wasmtime::component::Val>,
            ) -> ExecutionResult {
                trace!("[{workflow_id}] acquiring");
                let _permit = self.0.try_acquire().unwrap();
                trace!("[{workflow_id}] sleeping");
                tokio::time::sleep(Duration::from_millis(100)).await;
                trace!("[{workflow_id}] done!");
                ExecutionResult::Ok(SupportedFunctionResult::None)
            }
        }

        let database = InMemoryDatabase::new(10);
        let queue_writer =
            InMemoryQueueWriter::build(SIMPLE_WORKFLOW_FFQN.to_owned(), database.clone());
        let max_tasks = 3;
        let executions = (0..max_tasks * 2)
            .map(|_| {
                queue_writer
                    .enqueue(WorkflowId::generate(), Vec::new())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let workflow_worker = SemaphoreWorker(tokio::sync::Semaphore::new(max_tasks));
        let worker_task =
            QueueReaderTask::new(SIMPLE_WORKFLOW_FFQN.to_owned(), workflow_worker, database);
        let _close_handler = worker_task.spawn(max_tasks, None);
        for execution in executions {
            assert_eq!(
                ExecutionResult::Ok(SupportedFunctionResult::None),
                execution.await.unwrap()
            );
        }
    }

    struct SleepyWorkflow(Arc<AtomicBool>);

    #[async_trait]
    impl Worker for SleepyWorkflow {
        async fn run(
            &self,
            workflow_id: WorkflowId,
            mut params: Vec<wasmtime::component::Val>,
        ) -> ExecutionResult {
            assert_eq!(params.len(), 1);
            let millis = assert_matches!(params.pop().unwrap(), wasmtime::component::Val::U64(millis) => millis);
            trace!("[{workflow_id}] sleeping for {millis} ms");
            tokio::time::sleep(Duration::from_millis(millis)).await;
            trace!("[{workflow_id}] done!");
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            ExecutionResult::Ok(SupportedFunctionResult::None)
        }
    }

    #[tokio::test]
    async fn worker_timeout() {
        set_up();
        let database = InMemoryDatabase::new(1);
        let queue_writer =
            InMemoryQueueWriter::build(SIMPLE_WORKFLOW_FFQN.to_owned(), database.clone());
        let finished_check = Arc::new(AtomicBool::new(false));
        let worker_task = QueueReaderTask::new(
            SIMPLE_WORKFLOW_FFQN.to_owned(),
            SleepyWorkflow(finished_check.clone()),
            database,
        );
        let max_duration_millis = 100;
        let _close_handler = worker_task.spawn(1, Some(Duration::from_millis(max_duration_millis)));

        let execute = |millis: u64, workflow_id: WorkflowId| {
            queue_writer
                .enqueue(
                    workflow_id,
                    Vec::from([wasmtime::component::Val::U64(millis)]),
                )
                .unwrap()
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
    async fn queue_reader_task_abort_propagates_to_workers() {
        set_up();
        let database = InMemoryDatabase::new(1);
        let queue_writer =
            InMemoryQueueWriter::build(SIMPLE_WORKFLOW_FFQN.to_owned(), database.clone());
        let finished_check = Arc::new(AtomicBool::new(false));
        let worker_task = QueueReaderTask::new(
            SIMPLE_WORKFLOW_FFQN.to_owned(),
            SleepyWorkflow(finished_check.clone()),
            database,
        );
        let close_handler = worker_task.spawn(1, None);
        let execute = |millis: u64, workflow_id: WorkflowId| {
            queue_writer
                .enqueue(
                    workflow_id,
                    Vec::from([wasmtime::component::Val::U64(millis)]),
                )
                .unwrap()
        };
        let execution_fut = execute(100, WorkflowId::generate());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(close_handler);
        });
        assert_eq!(
            "channel closed",
            execution_fut.await.unwrap_err().to_string()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
        // Make sure that the worker did not finish.
        assert_eq!(
            false,
            finished_check.load(std::sync::atomic::Ordering::SeqCst)
        );
    }
}
