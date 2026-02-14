//! A custom [`JobExecutor`] that uses `wstd`'s reactor to drive WASIp2 pollables.
//!
//! This replaces Boa's default `SimpleJobExecutor` which uses `futures_lite::future::block_on`
//! (a busy-polling executor that doesn't understand WASIp2 pollables). By routing through
//! wstd's reactor, concurrent `fetch()` calls get their pollables batched in a single
//! `wasi:io/poll.poll()` call, enabling true I/O concurrency.

use boa_engine::context::time::JsInstant;
use boa_engine::job::{
    GenericJob, Job, JobExecutor, NativeAsyncJob, PromiseJob, TimeoutJob,
};
use boa_engine::{Context, JsResult};
use futures_concurrency::future::FutureGroup;
use futures_lite::StreamExt;
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
        // Use wstd's reactor instead of futures_lite::future::block_on.
        // This allows WASIp2 pollables (from wstd::http) to be driven properly.
        wstd::runtime::block_on(self.run_jobs_async(&RefCell::new(context)))
    }

    async fn run_jobs_async(self: Rc<Self>, context: &RefCell<&mut Context>) -> JsResult<()> {
        let mut group = FutureGroup::new();
        loop {
            for job in mem::take(&mut *self.async_jobs.borrow_mut()) {
                group.insert(job.call(context));
            }

            let no_timeout_jobs_to_run = {
                let now = context.borrow().clock().now();
                !self.timeout_jobs.borrow().iter().any(|(t, _)| &now >= t)
            };

            if self.promise_jobs.borrow().is_empty()
                && self.async_jobs.borrow().is_empty()
                && self.generic_jobs.borrow().is_empty()
                && no_timeout_jobs_to_run
                && group.is_empty()
            {
                break;
            }

            if let Some(Err(err)) = futures_lite::future::poll_once(group.next()).await.flatten() {
                self.clear();
                return Err(err);
            }

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

            let jobs = mem::take(&mut *self.promise_jobs.borrow_mut());
            for job in jobs {
                if let Err(err) = job.call(&mut context.borrow_mut()) {
                    self.clear();
                    return Err(err);
                }
            }

            let jobs = mem::take(&mut *self.generic_jobs.borrow_mut());
            for job in jobs {
                if let Err(err) = job.call(&mut context.borrow_mut()) {
                    self.clear();
                    return Err(err);
                }
            }
            context.borrow_mut().clear_kept_objects();
            futures_lite::future::yield_now().await;
        }

        Ok(())
    }
}
