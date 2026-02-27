//! A custom [`JobExecutor`] that uses `wstd`'s reactor to drive WASIp2 pollables.
//!
//! This replaces Boa's default `SimpleJobExecutor` which uses `futures_lite::future::block_on`
//! (a busy-polling executor that doesn't understand WASIp2 pollables). By routing through
//! wstd's reactor, concurrent `fetch()` calls get their pollables batched in a single
//! `wasi:io/poll.poll()` call, enabling true I/O concurrency.

use boa_engine::context::time::JsInstant;
use boa_engine::job::{GenericJob, Job, JobExecutor, NativeAsyncJob, PromiseJob, TimeoutJob};
use boa_engine::{Context, JsResult};
use futures_concurrency::future::FutureGroup;
use futures_lite::StreamExt as _;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::rc::Rc;

/// A FIFO job executor backed by wstd's reactor for WASIp2 I/O.
#[derive(Default)]
pub struct WasiJobExecutor {
    promise_jobs: RefCell<VecDeque<PromiseJob>>,
    async_jobs: RefCell<VecDeque<NativeAsyncJob>>,
    timeout_jobs: RefCell<BTreeMap<JsInstant, TimeoutJob>>,
    generic_jobs: RefCell<VecDeque<GenericJob>>,
}

impl WasiJobExecutor {
    fn clear(&self) {
        self.promise_jobs.borrow_mut().clear();
        self.async_jobs.borrow_mut().clear();
        self.timeout_jobs.borrow_mut().clear();
        self.generic_jobs.borrow_mut().clear();
    }
}

impl WasiJobExecutor {
    /// Drive all enqueued jobs to completion (async).
    ///
    /// Called by promise resolution directly inside a single `wstd::runtime::block_on`,
    /// avoiding the repeated `block_on` calls that `JsPromise::await_blocking` → `run_jobs`
    /// would create.
    pub async fn drive_jobs(self: Rc<Self>, context: &RefCell<&mut Context>) -> JsResult<()> {
        JobExecutor::run_jobs_async(self, context).await
    }

    /// Drive jobs until the given condition returns true, then stop early.
    ///
    /// This is useful for promise resolution where we want to stop as soon as
    /// the main promise resolves, abandoning any orphaned jobs (like unwaited timers).
    pub async fn drive_jobs_until<F>(
        self: Rc<Self>,
        context: &RefCell<&mut Context>,
        is_done: F,
    ) -> JsResult<()>
    where
        F: Fn() -> bool,
    {
        let mut group = FutureGroup::new();
        loop {
            // Check done condition before doing any work
            if is_done() {
                break;
            }

            // 1. Move newly enqueued async jobs into the concurrent FutureGroup.
            for job in mem::take(&mut *self.async_jobs.borrow_mut()) {
                group.insert(job.call(context));
            }

            // 2. Run all ready synchronous jobs (promise microtasks, timeouts, generic).
            self.run_sync_jobs(context)?;

            // Check done condition after sync jobs (promise might have resolved)
            if is_done() {
                break;
            }

            // 3. Check if there's any work left.
            if group.is_empty()
                && self.promise_jobs.borrow().is_empty()
                && self.async_jobs.borrow().is_empty()
                && self.generic_jobs.borrow().is_empty()
                && self.timeout_jobs.borrow().is_empty()
            {
                break;
            }

            // 4. If there are async futures in flight, await the next one.
            if !group.is_empty() {
                match group.next().await {
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        self.clear();
                        return Err(err);
                    }
                    None => {}
                }
            } else if let Some(sleep_duration) = self.time_until_next_timeout(context) {
                // 5. No async jobs, but we have pending timeout jobs.
                //    Sleep until the next timeout is ready.
                wstd::task::sleep(sleep_duration).await;
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for WasiJobExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasiJobExecutor").finish_non_exhaustive()
    }
}

impl JobExecutor for WasiJobExecutor {
    fn enqueue_job(self: Rc<Self>, job: Job, context: &mut Context) {
        match job {
            Job::PromiseJob(p) => self.promise_jobs.borrow_mut().push_back(p),
            Job::AsyncJob(a) => self.async_jobs.borrow_mut().push_back(a),
            Job::TimeoutJob(t) => {
                let now = context.clock().now();
                self.timeout_jobs.borrow_mut().insert(now + t.timeout(), t);
            }
            Job::GenericJob(g) => self.generic_jobs.borrow_mut().push_back(g),
            _ => {} // Future-proof against new job types
        }
    }

    fn run_jobs(self: Rc<Self>, context: &mut Context) -> JsResult<()> {
        // This is called by Boa internally (e.g. from await_blocking).
        // We use wstd::runtime::block_on to drive WASIp2 pollables.
        // Note: resolve_if_promise avoids this path by calling run_jobs_async directly.
        wstd::runtime::block_on(self.run_jobs_async(&RefCell::new(context)))
    }

    async fn run_jobs_async(self: Rc<Self>, context: &RefCell<&mut Context>) -> JsResult<()> {
        self.drive_jobs_until(context, || false).await
    }
}

impl WasiJobExecutor {
    /// Calculate the duration until the next timeout job is ready.
    /// Returns `None` if there are no pending timeout jobs.
    fn time_until_next_timeout(
        &self,
        context: &RefCell<&mut Context>,
    ) -> Option<wstd::time::Duration> {
        let timeouts = self.timeout_jobs.borrow();
        let next_deadline = timeouts.keys().next()?;
        let now = context.borrow().clock().now();

        // If deadline is in the past, return zero duration
        if *next_deadline <= now {
            return Some(wstd::time::Duration::from_nanos(0));
        }

        // Calculate the difference: deadline - now
        let js_duration = *next_deadline - now;
        let std_duration: std::time::Duration = js_duration.into();
        Some(wstd::time::Duration::from(std_duration))
    }

    /// Run all ready synchronous jobs: promise microtasks, timeout jobs, generic jobs.
    fn run_sync_jobs(&self, context: &RefCell<&mut Context>) -> JsResult<()> {
        // Timeout jobs
        {
            let now = context.borrow().clock().now();
            let mut timeouts_borrow = self.timeout_jobs.borrow_mut();
            let mut jobs_to_keep = timeouts_borrow.split_off(&now);
            jobs_to_keep.retain(|_, job| !job.is_cancelled());
            let jobs_to_run = mem::replace(&mut *timeouts_borrow, jobs_to_keep);
            drop(timeouts_borrow);

            for job in jobs_to_run.into_values() {
                if let Err(err) = job.call(&mut context.borrow_mut()) {
                    self.clear();
                    return Err(err);
                }
            }
        }

        // Promise microtasks
        let jobs = mem::take(&mut *self.promise_jobs.borrow_mut());
        for job in jobs {
            if let Err(err) = job.call(&mut context.borrow_mut()) {
                self.clear();
                return Err(err);
            }
        }

        // Generic jobs
        let jobs = mem::take(&mut *self.generic_jobs.borrow_mut());
        for job in jobs {
            if let Err(err) = job.call(&mut context.borrow_mut()) {
                self.clear();
                return Err(err);
            }
        }

        context.borrow_mut().clear_kept_objects();
        Ok(())
    }
}
