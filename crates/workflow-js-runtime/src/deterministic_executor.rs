use crate::generated::obelisk;
use boa_engine::job::{GenericJob, Job, JobExecutor, NativeAsyncJob, PromiseJob};
use boa_engine::{Context, JsResult};
use futures_concurrency::future::FutureGroup;
use futures_lite::{StreamExt as _, future};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::mem;
use std::rc::Rc;

/// A simple FIFO executor that bails on the first error.
///
/// This is the default job executor for the [`Context`], but it is mostly pretty limited
/// for a custom event loop.
///
/// To disable running promise jobs on the engine, see [`IdleJobExecutor`].
#[allow(clippy::struct_field_names)]
#[derive(Default)]
pub struct DeterministicJobExecutor {
    promise_jobs: RefCell<VecDeque<PromiseJob>>,
    async_jobs: RefCell<VecDeque<NativeAsyncJob>>,
    generic_jobs: RefCell<VecDeque<GenericJob>>,
}

impl DeterministicJobExecutor {
    fn clear(&self) {
        self.promise_jobs.borrow_mut().clear();
        self.async_jobs.borrow_mut().clear();
        self.generic_jobs.borrow_mut().clear();
    }
}

impl JobExecutor for DeterministicJobExecutor {
    fn enqueue_job(self: Rc<Self>, job: Job, _context: &mut Context) {
        match job {
            Job::PromiseJob(p) => self.promise_jobs.borrow_mut().push_back(p),
            Job::AsyncJob(a) => self.async_jobs.borrow_mut().push_back(a),
            Job::TimeoutJob(_) => {
                obelisk::log::log::error(
                    "Workflow must be deterministic, timeout jobs are not supported",
                );
                panic!()
            }
            Job::GenericJob(g) => self.generic_jobs.borrow_mut().push_back(g),
            _other => unreachable!(),
        }
    }

    fn run_jobs(self: Rc<Self>, context: &mut Context) -> JsResult<()> {
        future::block_on(self.run_jobs_async(&RefCell::new(context)))
    }

    async fn run_jobs_async(self: Rc<Self>, context: &RefCell<&mut Context>) -> JsResult<()>
    where
        Self: Sized,
    {
        let mut group = FutureGroup::new();
        loop {
            for job in mem::take(&mut *self.async_jobs.borrow_mut()) {
                group.insert(job.call(context));
            }

            if self.promise_jobs.borrow().is_empty()
                && self.async_jobs.borrow().is_empty()
                && self.generic_jobs.borrow().is_empty()
                && group.is_empty()
            {
                break;
            }

            if let Some(Err(err)) = future::poll_once(group.next()).await.flatten() {
                self.clear();
                return Err(err);
            }

            // Timeout job handling removed

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
            future::yield_now().await;
        }

        Ok(())
    }
}
