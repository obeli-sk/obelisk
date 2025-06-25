use super::wasi::random::random;
use crate::workflow::workflow_ctx::WorkflowCtx;
use concepts::time::ClockFn;

impl<C: ClockFn> random::Host for WorkflowCtx<C> {
    fn get_random_bytes(&mut self, len: u64) -> wasmtime::Result<Vec<u8>> {
        let vec = vec![0; usize::try_from(len).unwrap()];
        Ok(vec)
    }
    fn get_random_u64(&mut self) -> wasmtime::Result<u64> {
        Ok(0)
    }
}
