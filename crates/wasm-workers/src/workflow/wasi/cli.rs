use super::wasi::cli::{self, environment, exit, stderr, stdin, stdout};
use crate::workflow::workflow_ctx::WorkflowCtx;
use wasmtime::Result;
use wasmtime::component::Resource;
use wasmtime_wasi::{cli::StdinStream, p2::pipe};
use wasmtime_wasi_io::streams::{DynInputStream, DynOutputStream};

impl environment::Host for WorkflowCtx {
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

impl exit::Host for WorkflowCtx {
    fn exit(&mut self, _code: Result<(), ()>) -> Result<()> {
        Err(wasmtime::Error::msg("wasi:cli/exit is stubbed"))
    }
    fn exit_with_code(&mut self, _status_code: u8) -> Result<()> {
        Err(wasmtime::Error::msg("wasi:cli/exit is stubbed"))
    }
}
// see WasiCtxBuilder
impl stdin::Host for WorkflowCtx {
    fn get_stdin(&mut self) -> Result<Resource<DynInputStream>> {
        let stdin = pipe::ClosedInputStream;
        let stream = stdin.p2_stream();
        Ok(self.resource_table.push(stream)?)
    }
}
// see WasiCtxBuilder
impl stdout::Host for WorkflowCtx {
    fn get_stdout(&mut self) -> Result<Resource<DynOutputStream>> {
        let stdout: DynOutputStream = Box::new(pipe::SinkOutputStream);
        Ok(self.resource_table.push(stdout)?)
    }
}
// see WasiCtxBuilder
impl stderr::Host for WorkflowCtx {
    fn get_stderr(&mut self) -> Result<Resource<DynOutputStream>> {
        let stderr: DynOutputStream = Box::new(pipe::SinkOutputStream);
        Ok(self.resource_table.push(stderr)?)
    }
}

impl cli::terminal_input::Host for WorkflowCtx {}
impl cli::terminal_input::HostTerminalInput for WorkflowCtx {
    fn drop(&mut self, _r: Resource<cli::terminal_input::TerminalInput>) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl cli::terminal_output::Host for WorkflowCtx {}
impl cli::terminal_output::HostTerminalOutput for WorkflowCtx {
    fn drop(&mut self, _r: Resource<cli::terminal_output::TerminalOutput>) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl cli::terminal_stdin::Host for WorkflowCtx {
    fn get_terminal_stdin(
        &mut self,
    ) -> wasmtime::Result<Option<wasmtime::component::Resource<cli::terminal_stdin::TerminalInput>>>
    {
        Ok(None)
    }
}
impl cli::terminal_stdout::Host for WorkflowCtx {
    fn get_terminal_stdout(
        &mut self,
    ) -> wasmtime::Result<Option<wasmtime::component::Resource<cli::terminal_stdout::TerminalOutput>>>
    {
        Ok(None)
    }
}
impl cli::terminal_stderr::Host for WorkflowCtx {
    fn get_terminal_stderr(
        &mut self,
    ) -> wasmtime::Result<Option<wasmtime::component::Resource<cli::terminal_stderr::TerminalOutput>>>
    {
        Ok(None)
    }
}
