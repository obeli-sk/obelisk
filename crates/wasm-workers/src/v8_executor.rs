use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

/// How often [`V8Executor::drain`] re-checks the isolate permits.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// How long shutdown gives [`V8Executor::drain`]. Not an operator knob: no workload behaves
/// differently for a different value, see [`V8Executor::drain`].
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// The workload limits are independent reservations, not shares of a global budget: a resident
/// workflow can never occupy capacity an activity or a webhook is entitled to. Their sum is the
/// process-wide isolate bound.
#[derive(Debug, Clone, Copy)]
pub struct V8ExecutorConfig {
    pub max_workflows: usize,
    pub max_activities: usize,
    pub max_webhooks: usize,
    pub thread_stack_size: usize,
    pub max_heap_size: usize,
}

impl Default for V8ExecutorConfig {
    fn default() -> Self {
        Self {
            max_workflows: 32,
            max_activities: 16,
            max_webhooks: 16,
            thread_stack_size: 4 * 1024 * 1024,
            max_heap_size: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum V8ExecutorError {
    #[error("native V8 executor is closed")]
    Closed,
    #[error("cannot spawn native V8 isolate thread: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("native V8 isolate stopped before returning a result")]
    IsolateStopped,
    #[error("native V8 capacity is exhausted")]
    Overloaded,
}

#[derive(Clone, Copy, Debug)]
pub enum V8Workload {
    Workflow,
    Activity,
    Webhook,
}

/// Runs each native V8 isolate on its own OS thread with its own current-thread Tokio runtime.
/// Owns no threads and no mutable collections: concurrency is bounded by the semaphores below.
#[derive(Clone)]
pub struct V8Executor {
    inner: Arc<ExecutorInner>,
}

struct ExecutorInner {
    config: V8ExecutorConfig,
    workflows: Arc<Semaphore>,
    activities: Arc<Semaphore>,
    webhooks: Arc<Semaphore>,
}

impl Default for V8Executor {
    fn default() -> Self {
        Self::new(V8ExecutorConfig::default())
    }
}

impl V8Executor {
    #[must_use]
    pub fn new(config: V8ExecutorConfig) -> Self {
        assert!(
            config.max_workflows > 0 && config.max_activities > 0 && config.max_webhooks > 0,
            "V8 workload limits must be non-zero"
        );
        assert!(
            config.thread_stack_size > 0,
            "V8 thread stack must be non-zero"
        );
        Self {
            inner: Arc::new(ExecutorInner {
                workflows: Arc::new(Semaphore::new(config.max_workflows)),
                activities: Arc::new(Semaphore::new(config.max_activities)),
                webhooks: Arc::new(Semaphore::new(config.max_webhooks)),
                config,
            }),
        }
    }

    #[must_use]
    pub fn max_heap_size(&self) -> usize {
        self.inner.config.max_heap_size
    }

    fn category(&self, workload: V8Workload) -> &Arc<Semaphore> {
        match workload {
            V8Workload::Workflow => &self.inner.workflows,
            V8Workload::Activity => &self.inner.activities,
            V8Workload::Webhook => &self.inner.webhooks,
        }
    }

    /// Every category, paired with the limit it was configured with.
    fn categories(&self) -> [(&Arc<Semaphore>, usize); 3] {
        [
            (&self.inner.workflows, self.inner.config.max_workflows),
            (&self.inner.activities, self.inner.config.max_activities),
            (&self.inner.webhooks, self.inner.config.max_webhooks),
        ]
    }

    /// Waits for capacity in `workload`'s category. Admission is separate from
    /// [`V8Admission::run`] so a caller that cannot be admitted keeps the state it would otherwise
    /// have moved onto the isolate thread, and can report the refusal against its own execution.
    pub async fn admit(&self, workload: V8Workload) -> Result<V8Admission, V8ExecutorError> {
        let category = self
            .category(workload)
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| V8ExecutorError::Closed)?;
        Ok(self.admission(category))
    }

    /// Like [`Self::admit`], but sheds the load instead of waiting for capacity.
    pub fn try_admit(&self, workload: V8Workload) -> Result<V8Admission, V8ExecutorError> {
        let category = self
            .category(workload)
            .clone()
            .try_acquire_owned()
            .map_err(|_| V8ExecutorError::Overloaded)?;
        Ok(self.admission(category))
    }

    fn admission(&self, category: OwnedSemaphorePermit) -> V8Admission {
        V8Admission {
            category,
            thread_stack_size: self.inner.config.thread_stack_size,
        }
    }

    /// Refuses all further admission: pending and future acquisitions fail instead of queueing.
    pub fn close(&self) {
        for (semaphore, _) in self.categories() {
            semaphore.close();
        }
    }

    /// Waits up to `grace` for every running isolate to be dropped, then reports whatever is
    /// still outstanding. Interruption itself is delivered by the per-workload supervisors (the
    /// executor's interrupt watcher, the webhook termination watcher, the V8 interrupt ticker);
    /// an isolate that will not stop is logged rather than waited on forever.
    ///
    /// Webhooks are what this is for. The workflow and activity supervisors await their isolate
    /// task after interrupting it, so the executor shutdown above already waited for those (they
    /// contribute nothing here, which is why the grace period is not worth configuring). A
    /// webhook has no executor worker: its connection task drops the request future on shutdown,
    /// leaving `TerminateOnDrop` to interrupt an isolate that nothing else will wait for.
    pub async fn drain(&self, grace: Duration) {
        let deadline = tokio::time::Instant::now() + grace;
        loop {
            let outstanding: usize = self
                .categories()
                .into_iter()
                .map(|(semaphore, limit)| limit - semaphore.available_permits())
                .sum();
            if outstanding == 0 {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    outstanding,
                    "Native V8 isolates did not stop within the shutdown grace period"
                );
                return;
            }
            // A closed semaphore cannot be acquired, so the drain is observed through the
            // permit counts instead of by acquiring every permit. `close` is what makes this
            // terminate: without it an admitted caller could keep taking the permits back.
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        }
    }
}

/// Capacity for exactly one isolate, held until that isolate has been dropped.
pub struct V8Admission {
    category: OwnedSemaphorePermit,
    thread_stack_size: usize,
}

impl V8Admission {
    /// Runs `execute` to completion on a dedicated OS thread owning one current-thread Tokio
    /// runtime and one V8 isolate.
    pub async fn run<F, Fut, T>(self, execute: F) -> Result<T, V8ExecutorError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let Self {
            category,
            thread_stack_size,
        } = self;
        let (result_tx, result_rx) = oneshot::channel();
        // The thread is deliberately detached: it ends with its isolate, and shutdown reports a
        // wedged isolate rather than joining it, so a stuck host call cannot block process exit.
        std::thread::Builder::new()
            .name("obelisk-v8".to_owned())
            .stack_size(thread_stack_size)
            .spawn(move || run_isolate(execute, result_tx, category))
            .map_err(V8ExecutorError::Spawn)?;
        // A panic on the isolate thread drops the sender.
        result_rx.await.map_err(|_| V8ExecutorError::IsolateStopped)
    }
}

fn run_isolate<F, Fut, T>(execute: F, result_tx: oneshot::Sender<T>, category: OwnedSemaphorePermit)
where
    F: FnOnce() -> Fut + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let tokio_runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("native V8 Tokio runtime must initialize");
    // Two-level driver: `deno_unsync::spawn` gives the non-`Send` isolate future the local task
    // scheduling `deno_core` ops require, and the bootstrap around it is what the runtime polls.
    // Neither a `LocalSet` nor masking the isolate future directly works: both leave terminated
    // jobs stuck during teardown.
    let bootstrap = async move {
        deno_core::unsync::spawn(execute())
            .await
            .expect("native V8 local task must not be cancelled")
    };
    // SAFETY: the current-thread runtime built above can poll this task only on this thread.
    let bootstrap = unsafe { deno_core::unsync::MaskFutureAsSend::new(bootstrap) };
    let result = tokio_runtime
        .block_on(tokio_runtime.spawn(bootstrap))
        .expect("native V8 root task must not be cancelled")
        .into_inner();
    let _ = result_tx.send(result);
    // The isolate and its host state are gone by now, so admitted capacity always reflects
    // memory actually held. The Tokio runtime outlives the permit and is dropped last.
    drop(category);
    drop(tokio_runtime);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::runtime::RuntimeFlavor;

    fn config() -> V8ExecutorConfig {
        V8ExecutorConfig {
            max_workflows: 1,
            max_activities: 1,
            max_webhooks: 1,
            thread_stack_size: 4 * 1024 * 1024,
            max_heap_size: 32 * 1024 * 1024,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_deno_op_must_run_on_a_current_thread_runtime() {
        let executor = V8Executor::new(config());
        let (job_thread, task_thread) = executor
            .admit(V8Workload::Activity)
            .await
            .unwrap()
            .run(|| async {
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
        assert_eq!(job_thread, task_thread);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn isolate_runtime_must_be_dropped_after_the_isolate() {
        let executor = V8Executor::new(config());
        let task_dropped = Arc::new(AtomicBool::new(false));
        let task_dropped_clone = task_dropped.clone();
        executor
            .admit(V8Workload::Activity)
            .await
            .unwrap()
            .run(move || async move {
                struct DropGuard(Arc<AtomicBool>);
                impl Drop for DropGuard {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::Release);
                    }
                }
                tokio::spawn(async move {
                    let _guard = DropGuard(task_dropped_clone);
                    std::future::pending::<()>().await;
                });
                tokio::task::yield_now().await;
            })
            .await
            .unwrap();
        // The next isolate is admitted only after this thread released its permit, which happens
        // after the isolate's runtime and its tasks are gone.
        executor
            .admit(V8Workload::Activity)
            .await
            .unwrap()
            .run(|| async {})
            .await
            .unwrap();
        assert!(task_dropped.load(Ordering::Acquire));
    }

    /// Holds one resident workflow, the shape that starves other categories if they share a
    /// budget with it, and hands back the channel that ends it.
    async fn resident_workflow(
        executor: &V8Executor,
    ) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let resident = executor.clone();
        let resident = tokio::spawn(async move {
            resident
                .admit(V8Workload::Workflow)
                .await
                .unwrap()
                .run(move || async move {
                    let _ = started_tx.send(());
                    let _ = finish_rx.await;
                })
                .await
                .unwrap();
        });
        started_rx.await.unwrap();
        (finish_tx, resident)
    }

    /// Every category limit is a reservation, so a workflow that stays resident for hours cannot
    /// take capacity an activity or a webhook is entitled to.
    #[tokio::test(flavor = "multi_thread")]
    async fn resident_workflow_must_not_consume_other_categories() {
        let executor = V8Executor::new(config());
        let (finish_tx, resident) = resident_workflow(&executor).await;
        for workload in [V8Workload::Activity, V8Workload::Webhook] {
            executor
                .try_admit(workload)
                .expect("a resident workflow must not occupy other categories")
                .run(|| async {})
                .await
                .unwrap();
        }
        let _ = finish_tx.send(());
        resident.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn webhook_must_be_rejected_when_webhook_capacity_is_exhausted() {
        let executor = V8Executor::new(config());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let inflight = executor.try_admit(V8Workload::Webhook).unwrap();
        let inflight = tokio::spawn(async move {
            inflight
                .run(move || async move {
                    let _ = started_tx.send(());
                    let _ = finish_rx.await;
                })
                .await
        });
        started_rx.await.unwrap();
        assert!(matches!(
            executor.try_admit(V8Workload::Webhook),
            Err(V8ExecutorError::Overloaded)
        ));
        let _ = finish_tx.send(());
        inflight.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn panicking_isolate_must_not_break_the_executor() {
        let executor = V8Executor::new(config());
        let result: Result<(), V8ExecutorError> = executor
            .admit(V8Workload::Activity)
            .await
            .unwrap()
            .run(|| async {
                panic!("test V8 isolate panic");
            })
            .await;
        assert!(matches!(result, Err(V8ExecutorError::IsolateStopped)));
        assert_eq!(
            42,
            executor
                .admit(V8Workload::Activity)
                .await
                .unwrap()
                .run(|| async { 42 })
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_must_close_admission_and_drain() {
        let executor = V8Executor::new(config());
        let (finish_tx, resident) = resident_workflow(&executor).await;
        executor.close();
        // Closing refuses admission while the resident isolate is still running, in every
        // category and whether or not that category still has a free permit.
        assert!(matches!(
            executor.admit(V8Workload::Activity).await,
            Err(V8ExecutorError::Closed)
        ));
        assert!(matches!(
            executor.try_admit(V8Workload::Webhook),
            Err(V8ExecutorError::Overloaded)
        ));
        let drain = {
            let executor = executor.clone();
            tokio::spawn(async move { executor.drain(Duration::from_secs(10)).await })
        };
        let _ = finish_tx.send(());
        resident.await.unwrap();
        drain.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drain_must_report_a_wedged_isolate_instead_of_waiting() {
        let executor = V8Executor::new(config());
        let (started_tx, started_rx) = oneshot::channel();
        let wedged = executor.clone();
        // Blocking the isolate thread in a native call is the `workflow_js_worker.rs` hang shape:
        // `terminate_execution` cannot reach it, so shutdown must give up instead of joining.
        let wedged = tokio::spawn(async move {
            wedged
                .admit(V8Workload::Workflow)
                .await
                .unwrap()
                .run(move || async move {
                    let _ = started_tx.send(());
                    std::thread::sleep(Duration::from_secs(60));
                })
                .await
        });
        started_rx.await.unwrap();
        let started = tokio::time::Instant::now();
        executor.close();
        executor.drain(Duration::from_millis(100)).await;
        assert!(started.elapsed() < Duration::from_secs(5));
        wedged.abort();
    }
}
