use super::wasi::cli::{environment, exit, stderr, stdin, stdout};
use crate::workflow::workflow_ctx::WorkflowCtx;
use concepts::time::ClockFn;
use wasmtime::Result;
use wasmtime::component::Resource;
use wasmtime_wasi::p2::{StdinStream as _, pipe};
use wasmtime_wasi_io::streams::{DynInputStream, DynOutputStream};

impl<C: ClockFn> environment::Host for WorkflowCtx<C> {
    fn get_arguments(&mut self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    fn get_environment(&mut self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
    fn initial_cwd(&mut self) -> Result<Option<String>> {
        Ok(None)
    }
}

impl<C: ClockFn> exit::Host for WorkflowCtx<C> {
    fn exit(&mut self, _code: Result<(), ()>) -> Result<()> {
        Err(wasmtime::Error::msg("wasi:cli/exit is stubbed"))
    }
    fn exit_with_code(&mut self, _status_code: u8) -> Result<()> {
        Err(wasmtime::Error::msg("wasi:cli/exit is stubbed"))
    }
}
// see WasiCtxBuilder
impl<C: ClockFn> stdin::Host for WorkflowCtx<C> {
    fn get_stdin(&mut self) -> Result<Resource<DynInputStream>> {
        let stdin = pipe::ClosedInputStream;
        let stream = stdin.stream();
        Ok(self.resource_table.push(stream)?)
    }
}
// see WasiCtxBuilder
impl<C: ClockFn> stdout::Host for WorkflowCtx<C> {
    fn get_stdout(&mut self) -> Result<Resource<DynOutputStream>> {
        let stdout: DynOutputStream = Box::new(pipe::SinkOutputStream);
        Ok(self.resource_table.push(stdout)?)
    }
}
// see WasiCtxBuilder
impl<C: ClockFn> stderr::Host for WorkflowCtx<C> {
    fn get_stderr(&mut self) -> Result<Resource<DynOutputStream>> {
        let stderr: DynOutputStream = Box::new(pipe::SinkOutputStream);
        Ok(self.resource_table.push(stderr)?)
    }
}
