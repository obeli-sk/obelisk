use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

/// How often [`V8Executor::drain`] re-checks the isolate permits.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// How long shutdown gives [`V8Executor::drain`]. Not an operator knob: no workload behaves
/// differently for a different value, see [`V8Executor::drain`].
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// How long a finished isolate thread waits for the next isolate before exiting.
const IDLE_THREAD_TIMEOUT: Duration = Duration::from_secs(60);

/// One `(workload, native V8)` cell: the slots it grants and how large one isolate's heap may get.
/// The semaphore is the one the cell's executor admits from, so an isolate and the worker slot
/// running it are one reservation, not two.
#[derive(Debug, Clone)]
pub struct V8Cell {
    semaphore: Arc<Semaphore>,
    count: usize,
    /// `None` leaves V8's own default heap limit in place.
    max_heap_size: Option<usize>,
}

impl V8Cell {
    #[must_use]
    pub fn new(semaphore: Arc<Semaphore>, count: usize, max_heap_size: Option<usize>) -> Self {
        Self {
            semaphore,
            count,
            max_heap_size,
        }
    }

    /// A cell no executor shares, for direct callers and tests.
    #[must_use]
    pub fn standalone(count: usize, max_heap_size: usize) -> Self {
        Self::new(Arc::new(Semaphore::new(count)), count, Some(max_heap_size))
    }
}

/// The cells are independent reservations, not shares of a global budget: a resident workflow can
/// never occupy capacity an activity or a webhook is entitled to. Their sum is the process-wide
/// isolate bound.
#[derive(Debug, Clone)]
pub struct V8ExecutorConfig {
    pub workflows: V8Cell,
    pub activities: V8Cell,
    pub webhooks: V8Cell,
    pub thread_stack_size: usize,
}

impl Default for V8ExecutorConfig {
    fn default() -> Self {
        Self {
            workflows: V8Cell::standalone(32, 256 * 1024 * 1024),
            activities: V8Cell::standalone(16, 256 * 1024 * 1024),
            webhooks: V8Cell::standalone(16, 256 * 1024 * 1024),
            thread_stack_size: 4 * 1024 * 1024,
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
/// Concurrency is bounded by the semaphores below. A thread whose isolate finished is reused for
/// the next one: the first isolate on a fresh thread costs about twice as much to create.
#[derive(Clone)]
pub struct V8Executor {
    inner: Arc<ExecutorInner>,
}

struct ExecutorInner {
    config: V8ExecutorConfig,
    threads: Arc<IdleThreads>,
}

type Prewarm = Box<dyn FnOnce() + Send>;

struct Job {
    run: Box<dyn FnOnce() + Send>,
    /// Runs after `run` once the thread is idle, preparing state for the next isolate.
    prewarm: Option<Prewarm>,
}

thread_local! {
    /// State a prewarm hook prepared for the next isolate on this thread, off the critical path.
    static WARM: RefCell<Option<Box<dyn Any>>> = const { RefCell::new(None) };
}

pub(crate) fn has_warm() -> bool {
    WARM.with(|warm| warm.borrow().is_some())
}

pub(crate) fn put_warm<T: 'static>(value: T) {
    WARM.with(|warm| *warm.borrow_mut() = Some(Box::new(value)));
}

/// Takes the prepared state if it is a `T`; state of another type is dropped.
pub(crate) fn take_warm<T: 'static>() -> Option<T> {
    let warm = WARM.with(|warm| warm.borrow_mut().take())?;
    warm.downcast().ok().map(|warm| *warm)
}

/// Threads waiting for their next isolate. Only threads that finished cleanly get here, so a
/// wedged isolate keeps its thread out of the list and shutdown still does not join it.
#[derive(Default)]
struct IdleThreads {
    next_id: AtomicU64,
    idle: Mutex<Vec<(u64, mpsc::Sender<Job>)>>,
}

impl IdleThreads {
    /// Hands `job` to the most recently idled thread, or returns it if none is waiting.
    fn dispatch(&self, mut job: Job) -> Result<(), Job> {
        while let Some((_, sender)) = self.idle.lock().unwrap().pop() {
            match sender.send(job) {
                Ok(()) => return Ok(()),
                Err(mpsc::SendError(returned)) => job = returned,
            }
        }
        Err(job)
    }
}

fn isolate_thread(mut job: Job, threads: &Weak<IdleThreads>) {
    loop {
        (job.run)();
        let Some(next) = wait_for_job(threads, job.prewarm) else {
            break;
        };
        job = next;
    }
    WARM.with(|warm| warm.borrow_mut().take());
}

fn wait_for_job(threads: &Weak<IdleThreads>, prewarm: Option<Prewarm>) -> Option<Job> {
    let (sender, receiver) = mpsc::channel();
    let id = {
        let threads = threads.upgrade()?;
        let id = threads.next_id.fetch_add(1, Ordering::Relaxed);
        threads.idle.lock().unwrap().push((id, sender));
        id
    };
    // Already listed: a job arriving meanwhile queues here instead of starting a cold thread.
    match prewarm {
        Some(prewarm) => prewarm(),
        None => drop(WARM.with(|warm| warm.borrow_mut().take())),
    }
    match receiver.recv_timeout(IDLE_THREAD_TIMEOUT) {
        Ok(job) => Some(job),
        Err(mpsc::RecvTimeoutError::Disconnected) => None,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            // A dispatcher that popped this thread before the timeout is about to send.
            let still_listed = threads.upgrade().is_some_and(|threads| {
                let mut idle = threads.idle.lock().unwrap();
                let position = idle.iter().position(|(idle_id, _)| *idle_id == id);
                position.map(|position| idle.remove(position)).is_some()
            });
            if still_listed {
                None
            } else {
                receiver.recv().ok()
            }
        }
    }
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
            config.thread_stack_size > 0,
            "V8 thread stack must be non-zero"
        );
        Self {
            inner: Arc::new(ExecutorInner {
                config,
                threads: Arc::default(),
            }),
        }
    }

    #[must_use]
    pub fn max_heap_size(&self, workload: V8Workload) -> Option<usize> {
        self.cell(workload).max_heap_size
    }

    fn cell(&self, workload: V8Workload) -> &V8Cell {
        match workload {
            V8Workload::Workflow => &self.inner.config.workflows,
            V8Workload::Activity => &self.inner.config.activities,
            V8Workload::Webhook => &self.inner.config.webhooks,
        }
    }

    fn cells(&self) -> [&V8Cell; 3] {
        [
            &self.inner.config.workflows,
            &self.inner.config.activities,
            &self.inner.config.webhooks,
        ]
    }

    /// Waits for capacity in `workload`'s cell. Admission is separate from
    /// [`V8Admission::run`] so a caller that cannot be admitted keeps the state it would otherwise
    /// have moved onto the isolate thread, and can report the refusal against its own execution.
    pub async fn admit(&self, workload: V8Workload) -> Result<V8Admission, V8ExecutorError> {
        let permit = self
            .cell(workload)
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| V8ExecutorError::Closed)?;
        Ok(self.admission(Arc::new(permit)))
    }

    /// Like [`Self::admit`], but sheds the load instead of waiting for capacity.
    pub fn try_admit(&self, workload: V8Workload) -> Result<V8Admission, V8ExecutorError> {
        let permit = self
            .cell(workload)
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| V8ExecutorError::Overloaded)?;
        Ok(self.admission(Arc::new(permit)))
    }

    /// Admits on a slot the caller already holds, typically `WorkerContext::instance_permit`.
    /// Acquiring again would charge one isolate twice and deadlock the cell at half its size.
    #[must_use]
    pub fn admit_reserved(&self, reservation: Arc<OwnedSemaphorePermit>) -> V8Admission {
        self.admission(reservation)
    }

    fn admission(&self, reservation: Arc<OwnedSemaphorePermit>) -> V8Admission {
        V8Admission {
            reservation,
            thread_stack_size: self.inner.config.thread_stack_size,
            threads: self.inner.threads.clone(),
            prewarm: None,
        }
    }

    /// Refuses all further admission: pending and future acquisitions fail instead of queueing.
    pub fn close(&self) {
        for cell in self.cells() {
            cell.semaphore.close();
        }
        // Dropping the senders wakes the idle threads, which then exit.
        self.inner.threads.idle.lock().unwrap().clear();
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
                .cells()
                .into_iter()
                .map(|cell| {
                    cell.count
                        .saturating_sub(cell.semaphore.available_permits())
                })
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

/// Capacity for exactly one isolate, held until that isolate has been dropped. A reservation
/// shared with a worker is released once both are finished, keeping a wedged isolate charged.
pub struct V8Admission {
    reservation: Arc<OwnedSemaphorePermit>,
    thread_stack_size: usize,
    threads: Arc<IdleThreads>,
    prewarm: Option<Prewarm>,
}

impl V8Admission {
    /// Runs `prewarm` on the isolate thread after `run` finishes, while it waits for the next
    /// isolate. Whatever it stores with [`put_warm`] is only ever taken on that thread.
    #[must_use]
    pub fn with_prewarm(mut self, prewarm: impl FnOnce() + Send + 'static) -> Self {
        self.prewarm = Some(Box::new(prewarm));
        self
    }

    /// Runs `execute` to completion on an OS thread that runs nothing else meanwhile, with a fresh
    /// current-thread Tokio runtime and a fresh V8 isolate.
    pub async fn run<F, Fut, T>(self, execute: F) -> Result<T, V8ExecutorError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let Self {
            reservation,
            thread_stack_size,
            threads,
            prewarm,
        } = self;
        let (result_tx, result_rx) = oneshot::channel();
        let job = Job {
            run: Box::new(move || run_isolate(execute, result_tx, reservation)),
            prewarm,
        };
        if let Err(job) = threads.dispatch(job) {
            let threads = Arc::downgrade(&threads);
            // The thread is deliberately detached, and shutdown reports a wedged isolate rather
            // than joining it, so a stuck host call cannot block process exit.
            std::thread::Builder::new()
                .name("obelisk-v8".to_owned())
                .stack_size(thread_stack_size)
                .spawn(move || isolate_thread(job, &threads))
                .map_err(V8ExecutorError::Spawn)?;
        }
        // A panic on the isolate thread drops the sender.
        result_rx.await.map_err(|_| V8ExecutorError::IsolateStopped)
    }
}

fn run_isolate<F, Fut, T>(
    execute: F,
    result_tx: oneshot::Sender<T>,
    reservation: Arc<OwnedSemaphorePermit>,
) where
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
    // Tasks the isolate left on the runtime still hold host state, so the permit goes last.
    drop(tokio_runtime);
    drop(reservation);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::runtime::RuntimeFlavor;

    fn config() -> V8ExecutorConfig {
        V8ExecutorConfig {
            workflows: V8Cell::standalone(1, 32 * 1024 * 1024),
            activities: V8Cell::standalone(1, 32 * 1024 * 1024),
            webhooks: V8Cell::standalone(1, 32 * 1024 * 1024),
            thread_stack_size: 4 * 1024 * 1024,
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

    /// Holds one resident workflow, the shape that starves other cells if they share a budget
    /// with it, and hands back the channel that ends it.
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

    /// Every cell is a reservation, so a workflow that stays resident for hours cannot take
    /// capacity an activity or a webhook is entitled to.
    #[tokio::test(flavor = "multi_thread")]
    async fn resident_workflow_must_not_consume_other_cells() {
        let executor = V8Executor::new(config());
        let (finish_tx, resident) = resident_workflow(&executor).await;
        for workload in [V8Workload::Activity, V8Workload::Webhook] {
            executor
                .try_admit(workload)
                .expect("a resident workflow must not occupy other cells")
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

    /// A worker that already holds its cell's permit runs its isolate under that reservation.
    /// Acquiring a second one from the same cell would charge one isolate twice: here that would
    /// block forever, and in a full cell it would deadlock at half the configured size.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reserved_slot_must_not_be_charged_twice() {
        let semaphore = Arc::new(Semaphore::new(1));
        let executor = V8Executor::new(V8ExecutorConfig {
            activities: V8Cell::new(semaphore.clone(), 1, Some(32 * 1024 * 1024)),
            ..config()
        });
        let reservation = Arc::new(semaphore.clone().try_acquire_owned().unwrap());
        assert_eq!(
            42,
            executor
                .admit_reserved(reservation.clone())
                .run(|| async { 42 })
                .await
                .unwrap()
        );
        assert!(
            matches!(
                executor.try_admit(V8Workload::Activity),
                Err(V8ExecutorError::Overloaded)
            ),
            "the reservation is the only charge, and it is still held"
        );
        drop(reservation);
        executor
            .admit(V8Workload::Activity)
            .await
            .expect("the slot returns once the worker and the isolate are both gone");
    }

    /// Creating the first isolate on a fresh thread costs about twice as much as on a used one.
    #[tokio::test(flavor = "multi_thread")]
    async fn sequential_isolates_must_reuse_the_thread() {
        let executor = V8Executor::new(config());
        let mut threads = Vec::new();
        for _ in 0..3 {
            let thread = executor
                .admit(V8Workload::Activity)
                .await
                .unwrap()
                .run(|| async { std::thread::current().id() })
                .await
                .unwrap();
            threads.push(thread);
            // The thread idles itself only after dropping its isolate and runtime.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(threads[0], threads[1]);
        assert_eq!(threads[1], threads[2]);
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
        // Closing refuses admission while the resident isolate is still running, in every cell
        // and whether or not that cell still has a free permit.
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
