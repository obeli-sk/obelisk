use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Semaphore, oneshot};

type Complete = Box<dyn FnOnce() + Send + 'static>;
type Job = Box<dyn FnOnce(&Runtime) -> Complete + Send + 'static>;
enum Message {
    Execute(Job),
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
pub struct V8PoolConfig {
    pub max_threads: usize,
    pub max_workflows: usize,
    pub max_activities: usize,
    pub max_webhooks: usize,
    pub thread_stack_size: usize,
    pub idle_timeout: Duration,
    pub max_heap_size: usize,
}

impl Default for V8PoolConfig {
    fn default() -> Self {
        Self {
            max_threads: 64,
            max_workflows: 48,
            max_activities: 16,
            max_webhooks: 16,
            thread_stack_size: 4 * 1024 * 1024,
            idle_timeout: Duration::from_secs(60),
            max_heap_size: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum V8PoolError {
    #[error("native V8 pool is closed")]
    Closed,
    #[error("cannot spawn native V8 worker: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("native V8 worker stopped before returning a result")]
    WorkerStopped,
    #[error("native V8 capacity is exhausted")]
    Overloaded,
}

#[derive(Clone, Copy, Debug)]
pub enum V8Workload {
    Workflow,
    Activity,
    Webhook,
}

#[derive(Clone)]
pub struct V8Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: V8PoolConfig,
    active: Arc<Semaphore>,
    workflows: Arc<Semaphore>,
    activities: Arc<Semaphore>,
    webhooks: Arc<Semaphore>,
    idle: Mutex<Vec<std::sync::mpsc::Sender<Message>>>,
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        for sender in self.idle.get_mut().unwrap().drain(..) {
            let _ = sender.send(Message::Shutdown);
        }
    }
}

impl Drop for V8Pool {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            for sender in self.inner.idle.lock().unwrap().drain(..) {
                let _ = sender.send(Message::Shutdown);
            }
        }
    }
}

impl V8Pool {
    pub fn new(config: V8PoolConfig) -> Self {
        assert!(
            config.max_threads > 0,
            "V8 pool must allow at least one thread"
        );
        assert!(
            config.max_workflows > 0 && config.max_activities > 0 && config.max_webhooks > 0,
            "V8 workload limits must be non-zero"
        );
        assert!(
            config.thread_stack_size > 0,
            "V8 thread stack must be non-zero"
        );
        Self {
            inner: Arc::new(PoolInner {
                active: Arc::new(Semaphore::new(config.max_threads)),
                workflows: Arc::new(Semaphore::new(config.max_workflows)),
                activities: Arc::new(Semaphore::new(config.max_activities)),
                webhooks: Arc::new(Semaphore::new(config.max_webhooks)),
                idle: Mutex::new(Vec::new()),
                config,
            }),
        }
    }

    #[must_use]
    pub fn max_heap_size(&self) -> usize {
        self.inner.config.max_heap_size
    }

    pub async fn execute_for<F, Fut, T>(
        &self,
        workload: V8Workload,
        execute: F,
    ) -> Result<T, V8PoolError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let limiter = match workload {
            V8Workload::Workflow => &self.inner.workflows,
            V8Workload::Activity => &self.inner.activities,
            V8Workload::Webhook => &self.inner.webhooks,
        };
        let permit = limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| V8PoolError::Closed)?;
        let result = self.execute(execute).await;
        drop(permit);
        result
    }

    pub async fn try_execute_for<F, Fut, T>(
        &self,
        workload: V8Workload,
        execute: F,
    ) -> Result<T, V8PoolError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let limiter = match workload {
            V8Workload::Workflow => &self.inner.workflows,
            V8Workload::Activity => &self.inner.activities,
            V8Workload::Webhook => &self.inner.webhooks,
        };
        let workload_permit = limiter
            .clone()
            .try_acquire_owned()
            .map_err(|_| V8PoolError::Overloaded)?;
        let active_permit = self
            .inner
            .active
            .clone()
            .try_acquire_owned()
            .map_err(|_| V8PoolError::Overloaded)?;
        let result = self.execute_with_permit(execute, active_permit).await;
        drop(workload_permit);
        result
    }

    pub async fn execute<F, Fut, T>(&self, execute: F) -> Result<T, V8PoolError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let permit = self
            .inner
            .active
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| V8PoolError::Closed)?;
        self.execute_with_permit(execute, permit).await
    }

    async fn execute_with_permit<F, Fut, T>(
        &self,
        execute: F,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<T, V8PoolError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        let job = Box::new(move |runtime: &Runtime| {
            let future = async move {
                deno_core::unsync::spawn(execute())
                    .await
                    .expect("native V8 local task must not be cancelled")
            };
            // SAFETY: this bootstrap task runs only on the worker's current-thread runtime.
            let future = unsafe { deno_core::unsync::MaskFutureAsSend::new(future) };
            let result = runtime
                .block_on(runtime.spawn(future))
                .expect("native V8 root task must not be cancelled")
                .into_inner();
            Box::new(move || {
                let _ = result_tx.send(result);
                drop(permit);
            }) as Complete
        });
        self.dispatch(job)?;
        result_rx.await.map_err(|_| V8PoolError::WorkerStopped)
    }

    fn dispatch(&self, mut job: Job) -> Result<(), V8PoolError> {
        loop {
            let idle = self.inner.idle.lock().unwrap().pop();
            let Some(sender) = idle else {
                return self.spawn_worker(job);
            };
            match sender.send(Message::Execute(job)) {
                Ok(()) => return Ok(()),
                Err(err) => match err.0 {
                    Message::Execute(returned) => job = returned,
                    Message::Shutdown => unreachable!(),
                },
            }
        }
    }

    fn spawn_worker(&self, first_job: Job) -> Result<(), V8PoolError> {
        let (sender, receiver) = std::sync::mpsc::channel::<Message>();
        let worker_sender = sender.clone();
        let inner = Arc::downgrade(&self.inner);
        let config = self.inner.config;
        std::thread::Builder::new()
            .name("obelisk-v8".to_owned())
            .stack_size(config.thread_stack_size)
            .spawn(move || worker_loop(receiver, worker_sender, inner, config, first_job))
            .map_err(V8PoolError::Spawn)?;
        Ok(())
    }
}

impl Default for V8Pool {
    fn default() -> Self {
        Self::new(V8PoolConfig::default())
    }
}

fn worker_loop(
    receiver: std::sync::mpsc::Receiver<Message>,
    sender: std::sync::mpsc::Sender<Message>,
    inner: Weak<PoolInner>,
    config: V8PoolConfig,
    first_job: Job,
) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("native V8 Tokio runtime must initialize");
    let mut complete = Some(first_job(&runtime));
    while let Some(current) = complete.take() {
        let Some(inner) = inner.upgrade() else {
            current();
            break;
        };
        inner.idle.lock().unwrap().push(sender.clone());
        drop(inner);
        current();
        match receiver.recv_timeout(config.idle_timeout) {
            Ok(Message::Execute(job)) => complete = Some(job(&runtime)),
            Ok(Message::Shutdown)
            | Err(
                std::sync::mpsc::RecvTimeoutError::Timeout
                | std::sync::mpsc::RecvTimeoutError::Disconnected,
            ) => {
                complete = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::ThreadId;
    use tokio::runtime::RuntimeFlavor;

    fn config() -> V8PoolConfig {
        V8PoolConfig {
            max_threads: 1,
            max_workflows: 1,
            max_activities: 1,
            max_webhooks: 1,
            thread_stack_size: 4 * 1024 * 1024,
            idle_timeout: Duration::from_secs(10),
            max_heap_size: 32 * 1024 * 1024,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reuses_current_thread_runtime() {
        let pool = V8Pool::new(config());
        let first = pool
            .execute(|| async {
                assert_eq!(
                    tokio::runtime::Handle::current().runtime_flavor(),
                    RuntimeFlavor::CurrentThread
                );
                let task_thread = deno_core::unsync::spawn(async { std::thread::current().id() })
                    .await
                    .unwrap();
                (std::thread::current().id(), task_thread)
            })
            .await
            .unwrap();
        let second: ThreadId = pool
            .execute(|| async { std::thread::current().id() })
            .await
            .unwrap();
        assert_eq!(first.0, first.1);
        assert_eq!(first.0, second);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_webhook_when_global_capacity_is_exhausted() {
        let pool = V8Pool::new(config());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let active_pool = pool.clone();
        let active = tokio::spawn(async move {
            active_pool
                .execute_for(V8Workload::Workflow, move || async move {
                    let _ = started_tx.send(());
                    let _ = finish_rx.await;
                })
                .await
        });
        started_rx.await.unwrap();
        let result = pool.try_execute_for(V8Workload::Webhook, || async {}).await;
        assert!(matches!(result, Err(V8PoolError::Overloaded)));
        let _ = finish_tx.send(());
        active.await.unwrap().unwrap();
    }
}
