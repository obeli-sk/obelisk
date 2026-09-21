use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Semaphore, oneshot};
use tokio::task::LocalSet;

type Complete = Box<dyn FnOnce() + Send + 'static>;
type Job = Box<dyn FnOnce(&Runtime) -> Complete + Send + 'static>;

#[derive(Debug, Clone, Copy)]
pub struct V8PoolConfig {
    pub max_threads: usize,
    pub thread_stack_size: usize,
    pub idle_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum V8PoolError {
    #[error("native V8 pool is closed")]
    Closed,
    #[error("cannot spawn native V8 worker: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("native V8 worker stopped before returning a result")]
    WorkerStopped,
}

#[derive(Clone)]
pub struct V8Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: V8PoolConfig,
    active: Arc<Semaphore>,
    idle: Mutex<Vec<std::sync::mpsc::Sender<Job>>>,
}

impl V8Pool {
    pub fn new(config: V8PoolConfig) -> Self {
        assert!(
            config.max_threads > 0,
            "V8 pool must allow at least one thread"
        );
        assert!(
            config.thread_stack_size > 0,
            "V8 thread stack must be non-zero"
        );
        Self {
            inner: Arc::new(PoolInner {
                active: Arc::new(Semaphore::new(config.max_threads)),
                idle: Mutex::new(Vec::new()),
                config,
            }),
        }
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
        let (result_tx, result_rx) = oneshot::channel();
        let job = Box::new(move |runtime: &Runtime| {
            let local = LocalSet::new();
            let result = local.block_on(runtime, execute());
            drop(local);
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
            match sender.send(job) {
                Ok(()) => return Ok(()),
                Err(err) => job = err.0,
            }
        }
    }

    fn spawn_worker(&self, first_job: Job) -> Result<(), V8PoolError> {
        let (sender, receiver) = std::sync::mpsc::channel::<Job>();
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

fn worker_loop(
    receiver: std::sync::mpsc::Receiver<Job>,
    sender: std::sync::mpsc::Sender<Job>,
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
            Ok(job) => complete = Some(job(&runtime)),
            Err(
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
            thread_stack_size: 4 * 1024 * 1024,
            idle_timeout: Duration::from_secs(10),
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
}
