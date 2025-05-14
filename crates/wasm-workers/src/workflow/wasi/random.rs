use super::wasi::random::random;
use crate::workflow::workflow_ctx::WorkflowCtx;
use concepts::{
    storage::{DbConnection, DbPool},
    time::ClockFn,
};

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> random::Host for WorkflowCtx<C, DB, P> {
    fn get_random_bytes(&mut self, len: u64) -> wasmtime::Result<Vec<u8>> {
        let mut vec = Vec::new();
        vec.resize(len as usize, 0u8);
        Ok(vec)
    }
    fn get_random_u64(&mut self) -> wasmtime::Result<u64> {
        Ok(0)
    }
}
