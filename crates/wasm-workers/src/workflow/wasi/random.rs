use super::wasi::{self, random::random};
use crate::workflow::workflow_ctx::WorkflowCtx;

impl random::Host for WorkflowCtx {
    fn get_random_bytes(&mut self, len: u64) -> wasmtime::Result<Vec<u8>> {
        let vec = vec![0; usize::try_from(len).unwrap()];
        Ok(vec)
    }
    fn get_random_u64(&mut self) -> wasmtime::Result<u64> {
        Ok(0)
    }
}

impl wasi::random::insecure::Host for WorkflowCtx {
    fn get_insecure_random_bytes(
        &mut self,
        len: u64,
    ) -> wasmtime::Result<wasmtime::component::__internal::Vec<u8>> {
        Ok(std::iter::repeat_n(0, usize::try_from(len)?).collect())
    }

    fn get_insecure_random_u64(&mut self) -> wasmtime::Result<u64> {
        Ok(0)
    }
}
impl wasi::random::insecure_seed::Host for WorkflowCtx {
    fn insecure_seed(&mut self) -> wasmtime::Result<(u64, u64)> {
        Ok((0, 0))
    }
}
