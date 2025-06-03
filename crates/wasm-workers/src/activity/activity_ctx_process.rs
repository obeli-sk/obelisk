use super::{activity_ctx::ActivityCtx, activity_worker::ProcessProvider};
use crate::activity::process::HostChildProcess;
use concepts::time::ClockFn;
use process_support_outer::v1_0_0::obelisk::activity::process as process_support;
use wasmtime::component::Resource;
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream};
use wasmtime_wasi_io::IoView as _;

pub(crate) mod process_support_outer {
    pub(crate) mod v1_0_0 {
        wasmtime::component::bindgen!({
            path: "host-wit-activity/",
            async: true,
            inline: "package any:any;
                world bindings {
                    import obelisk:activity/process@1.0.0;
                }",
            world: "any:any/bindings",
            trappable_imports: true,
            with: {
                "obelisk:activity/process/child-process": crate::activity::process::HostChildProcess,
                "wasi:io": wasmtime_wasi_io::bindings::wasi::io,
            }
        });
    }
}

// NB: Only use `?` for translating `ResourceTableError` into anyhow!
impl<C: ClockFn> process_support::Host for ActivityCtx<C> {
    async fn spawn(
        &mut self,
        command: String,
        options: process_support::SpawnOptions,
    ) -> wasmtime::Result<
        Result<Resource<process_support::ChildProcess>, process_support::SpawnError>,
    > {
        let preopened_dir = self
            .preopened_dir
            .clone()
            .expect("process api can only be linked if preopened dir is enabled");
        match self.process_provider {
            Some(ProcessProvider::Local) => {
                match HostChildProcess::spawn_local(command, &options, &preopened_dir) {
                    Ok(child_process) => Ok(Ok(self.table().push(child_process)?)),
                    Err(err) => Ok(Err(err)), // Forward the spawn-error
                }
            }
            None => unreachable!("process api can only be linked if it is enabled"),
        }
    }
}

// Implement methods for the `child-process` resource
impl<C: ClockFn> process_support::HostChildProcess for ActivityCtx<C> {
    async fn id(&mut self, self_handle: Resource<HostChildProcess>) -> wasmtime::Result<u64> {
        let child_process = self.table().get(&self_handle)?;
        Ok(child_process.id())
    }

    async fn take_stdin(
        &mut self,
        self_handle: Resource<HostChildProcess>,
    ) -> wasmtime::Result<Option<Resource<DynOutputStream>>> {
        let table = self.table();
        let host_child_process = table.get_mut(&self_handle)?;
        if let Some(stream) = host_child_process.take_stdin() {
            Ok(Some(table.push(stream)?))
        } else {
            Ok(None)
        }
    }

    async fn take_stdout(
        &mut self,
        self_handle: Resource<HostChildProcess>,
    ) -> wasmtime::Result<Option<Resource<DynInputStream>>> {
        let table = self.table();
        let host_child_process = table.get_mut(&self_handle)?;
        if let Some(stream) = host_child_process.take_stdout() {
            Ok(Some(table.push(stream)?))
        } else {
            Ok(None)
        }
    }

    async fn take_stderr(
        &mut self,
        self_handle: Resource<HostChildProcess>,
    ) -> wasmtime::Result<Option<Resource<DynInputStream>>> {
        let table = self.table();
        let host_child_process = table.get_mut(&self_handle)?;
        if let Some(stream) = host_child_process.take_stderr() {
            Ok(Some(table.push(stream)?))
        } else {
            Ok(None)
        }
    }

    async fn wait(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<Result<Option<i32>, process_support::ProcessError>> {
        let child_process = self.table().get_mut(&self_handle)?;
        Ok(child_process.wait().await)
    }

    async fn kill(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<Result<(), process_support::ProcessError>> {
        let child_process = self.table().get_mut(&self_handle)?;
        Ok(child_process.kill().await)
    }

    async fn drop(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<()> {
        let _child_process = self.table().delete(self_handle)?;
        Ok(())
    }
}
