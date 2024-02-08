use crate::{EnqueueError, ExecutionResult, QueueWriter, Worker, WorkerError};
use async_channel::{Receiver, Sender, TrySendError};
use concepts::{workflow_id::WorkflowId, FunctionFqn};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinSet},
};
use tracing::{debug, info, info_span, instrument, trace, Instrument, Level};

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

    pub fn writer(&self, ffqn: FunctionFqn) -> InMemoryQueueWriter {
        let sender = self
            .queues
            .lock()
            .unwrap()
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .0
            .clone();
        InMemoryQueueWriter { sender, ffqn }
    }

    pub fn enqueue(
        &self,
        ffqn: FunctionFqn,
        workflow_id: WorkflowId,
        params: Vec<wasmtime::component::Val>,
    ) -> Result<oneshot::Receiver<ExecutionResult>, EnqueueError> {
        let sender = self
            .queues
            .lock()
            .unwrap()
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .0
            .clone();
        InMemoryQueueWriter::enqueue(&sender, workflow_id, params, &ffqn)
    }

    fn reader<W: Worker + Send + Sync + 'static>(
        &self,
        ffqn: FunctionFqn,
        worker: W,
    ) -> QueueReader<W> {
        let receiver = self
            .queues
            .lock()
            .unwrap()
            .entry(ffqn.clone())
            .or_insert_with(|| async_channel::bounded(self.queue_capacity))
            .1
            .clone();
        QueueReader {
            ffqn,
            receiver,
            worker: Arc::new(worker),
        }
    }
}

#[derive(Debug)]
pub struct InMemoryQueueWriter {
    sender: Sender<QueueEntry>,
    ffqn: FunctionFqn,
}

impl InMemoryQueueWriter {
    #[instrument(skip_all, fields(fffqn = ffqn.to_string(), workflow_id = workflow_id.to_string()))]
    fn enqueue(
        sender: &Sender<QueueEntry>,
        workflow_id: WorkflowId,
        params: Vec<wasmtime::component::Val>,
        ffqn: &FunctionFqn,
    ) -> Result<oneshot::Receiver<ExecutionResult>, EnqueueError> {
        trace!("Enqueuing");
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        let entry = QueueEntry {
            workflow_id,
            params,
            oneshot_sender,
        };
        match sender.try_send(entry) {
            Ok(()) => Ok(oneshot_receiver),
            Err(TrySendError::Full(entry)) => Err(EnqueueError::Full {
                workflow_id: entry.workflow_id,
                params: entry.params,
            }),
            Err(TrySendError::Closed(entry)) => Err(EnqueueError::Closed {
                workflow_id: entry.workflow_id,
                params: entry.params,
            }),
        }
    }
}

impl QueueWriter for InMemoryQueueWriter {
    fn enqueue(
        &self,
        workflow_id: WorkflowId,
        params: Vec<wasmtime::component::Val>,
    ) -> Result<oneshot::Receiver<ExecutionResult>, EnqueueError> {
        Self::enqueue(&self.sender, workflow_id, params, &self.ffqn)
    }
}

pub struct QueueReader<W: Worker> {
    ffqn: FunctionFqn,
    receiver: Receiver<QueueEntry>,
    worker: Arc<W>,
}

pub struct QueueReaderAbortHandle {
    ffqn: FunctionFqn,
    main_task: AbortHandle,
    receiver: Receiver<QueueEntry>,
}

impl QueueReaderAbortHandle {
    /// Graceful shutdown. Waits until all workers terminate.
    ///
    /// # Panics
    ///
    /// All senders must be closed, otherwise this function will panic.
    #[instrument(skip_all, fields(ffqn = self.ffqn.to_string()))]
    pub async fn close(self) {
        assert!(
            self.receiver.is_closed(),
            "the queue channel must be closed"
        );
        debug!("Gracefully closing");
        while !self.main_task.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        info!("Gracefully closed");
    }
}

impl Drop for QueueReaderAbortHandle {
    #[instrument(skip_all, fields(ffqn = self.ffqn.to_string()))]
    fn drop(&mut self) {
        info!("Dropping queue reader");
        self.main_task.abort();
    }
}

impl<W: Worker + Send + Sync + 'static> QueueReader<W> {
    #[instrument(skip_all, fields(ffqn = self.ffqn.to_string()))]
    pub fn spawn(
        self,
        max_tasks: u32,
        max_task_duration: Option<Duration>,
    ) -> QueueReaderAbortHandle {
        assert!(max_tasks > 0, "`max_tasks` must be greater than zero");
        info!("Spawning");
        let receiver = self.receiver.clone();
        let main_task = tokio::spawn(async move {
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
                    oneshot_sender,
                }) = receiver.recv().await
                else {
                    info!("Graceful shutdown detected, waiting for inflight workers");
                    // We already have `permit`. Acquiring all other permits.
                    let _blocking_permits = semaphore.acquire_many(max_tasks - 1).await.unwrap();
                    trace!("All workers have finished");
                    return;
                };
                trace!("Received {workflow_id}");
                let worker = self.worker.clone();
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
                    let _ = oneshot_sender.send(execution_result);
                    drop(permit);
                }
                .instrument(worker_span)
                );
                // TODO: retries with backoff, scheduled tasks
                // TODO: Persist the result atomically
            }
        }
        .instrument(info_span!("main_task"))
        ).abort_handle();
        QueueReaderAbortHandle {
            ffqn: self.ffqn,
            main_task,
            receiver: self.receiver,
        }
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
                .with(tracing_subscriber::fmt::layer())
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        });
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("namespace:package/ifc", "function-name");

    #[tokio::test]
    async fn test_simple_workflow() {
        set_up();

        struct SimpleWorker;

        #[async_trait]
        impl Worker for SimpleWorker {
            async fn run(
                &self,
                _workflow_id: WorkflowId,
                _params: Vec<wasmtime::component::Val>,
            ) -> ExecutionResult {
                ExecutionResult::Ok(SupportedFunctionResult::None)
            }
        }

        let database = InMemoryDatabase::new(1);
        let execution = database
            .enqueue(SOME_FFQN.to_owned(), WorkflowId::generate(), Vec::new())
            .unwrap();
        let queue_reader = database.reader(SOME_FFQN.to_owned(), SimpleWorker);
        let _abort_handle = queue_reader.spawn(1, None);
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
        let queue_writer = database.writer(SOME_FFQN.to_owned());
        let max_tasks = 3;
        let executions = (0..max_tasks * 2)
            .map(|_| {
                queue_writer
                    .enqueue(WorkflowId::generate(), Vec::new())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let workflow_worker = SemaphoreWorker(tokio::sync::Semaphore::new(
            usize::try_from(max_tasks).unwrap(),
        ));
        let queue_reader = database.reader(SOME_FFQN.to_owned(), workflow_worker);
        let _abort_handle = queue_reader.spawn(max_tasks, None);
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
        async fn run(
            &self,
            _workflow_id: WorkflowId,
            mut params: Vec<wasmtime::component::Val>,
        ) -> ExecutionResult {
            assert_eq!(params.len(), 1);
            let millis = assert_matches!(params.pop().unwrap(), wasmtime::component::Val::U64(millis) => millis);
            trace!("sleeping for {millis} ms");
            tokio::time::sleep(Duration::from_millis(millis)).await;
            trace!("done!");
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            ExecutionResult::Ok(SupportedFunctionResult::None)
        }
    }

    #[tokio::test]
    async fn worker_timeout() {
        set_up();
        let database = InMemoryDatabase::new(1);
        let queue_writer = database.writer(SOME_FFQN.to_owned());
        let finished_check = Arc::new(AtomicBool::new(false));
        let queue_reader =
            database.reader(SOME_FFQN.to_owned(), SleepyWorker(finished_check.clone()));
        let max_duration_millis = 100;
        let _abort_handle = queue_reader.spawn(1, Some(Duration::from_millis(max_duration_millis)));

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
    async fn queue_reader_abort_propagates_to_workers() {
        set_up();
        let database = InMemoryDatabase::new(1);
        let queue_writer = database.writer(SOME_FFQN.to_owned());
        let finished_check = Arc::new(AtomicBool::new(false));
        let queue_reader =
            database.reader(SOME_FFQN.to_owned(), SleepyWorker(finished_check.clone()));
        let abort_handle = queue_reader.spawn(1, None);
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
            drop(abort_handle);
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

    #[tokio::test]
    async fn test_graceful_shutdown() {
        set_up();
        let database = InMemoryDatabase::new(1);
        let sleep_millis = 100;
        let execution = database
            .enqueue(
                SOME_FFQN.to_owned(),
                WorkflowId::generate(),
                Vec::from([wasmtime::component::Val::U64(sleep_millis)]),
            )
            .unwrap();
        let finished_check = Arc::new(AtomicBool::new(false));
        let queue_reader =
            database.reader(SOME_FFQN.to_owned(), SleepyWorker(finished_check.clone()));
        let abort_handle = queue_reader.spawn(10, None);
        tokio::time::sleep(Duration::from_millis(sleep_millis / 2)).await;
        info!("Dropping the database");
        drop(database);
        abort_handle.close().await;
        // Make sure that the worker finished.
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
