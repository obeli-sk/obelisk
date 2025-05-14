use super::wasi::clocks::{monotonic_clock, wall_clock};
use crate::workflow::workflow_ctx::WorkflowCtx;
use concepts::{
    storage::{DbConnection, DbPool},
    time::ClockFn,
};
use wasmtime::component::Resource;
use wasmtime_wasi_io::poll::DynPollable;

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> monotonic_clock::Host for WorkflowCtx<C, DB, P> {
    fn now(&mut self) -> wasmtime::Result<monotonic_clock::Instant> {
        Ok(monotonic_clock::Instant::MIN)
    }
    fn resolution(&mut self) -> wasmtime::Result<monotonic_clock::Duration> {
        Err(wasmtime::Error::msg("wasi:clocks is stubbed"))
    }
    fn subscribe_duration(
        &mut self,
        _duration: monotonic_clock::Duration,
    ) -> wasmtime::Result<Resource<DynPollable>> {
        Err(wasmtime::Error::msg("wasi:clocks is stubbed"))
    }
    fn subscribe_instant(
        &mut self,
        _deadline: monotonic_clock::Instant,
    ) -> wasmtime::Result<Resource<DynPollable>> {
        Err(wasmtime::Error::msg("wasi:clocks is stubbed"))
    }
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> wall_clock::Host for WorkflowCtx<C, DB, P> {
    fn now(&mut self) -> wasmtime::Result<wall_clock::Datetime> {
        Ok(wall_clock::Datetime {
            seconds: 0,
            nanoseconds: 0,
        })
    }

    fn resolution(&mut self) -> wasmtime::Result<wall_clock::Datetime> {
        Err(wasmtime::Error::msg("wasi:clocks is stubbed"))
    }
}
