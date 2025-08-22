use super::activity_ctx::ActivityCtx;
use crate::activity::process::HostChildProcess;
use concepts::time::ClockFn;
use process_support_outer::v1_0_0::{
    SpawnErrorTrappable, obelisk::activity::process as process_support,
};
use wasmtime::component::Resource;
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream};
use wasmtime_wasi_io::IoView as _;

pub(crate) mod process_support_outer {
    pub(crate) mod v1_0_0 {
        wasmtime::component::bindgen!({
            path: "host-wit-activity/",
            inline: "package any:any;
                world bindings {
                    import obelisk:activity/process@1.0.0;
                }",
            world: "any:any/bindings",
            with: {
                "obelisk:activity/process/child-process": crate::activity::process::HostChildProcess,
                "wasi:io": wasmtime_wasi_io::bindings::wasi::io,
            },
            imports: {
                "obelisk:activity/process/[method]child-process.kill": async | trappable,
                default: trappable,
            },
            trappable_error_type: { "obelisk:activity/process/spawn-error" => SpawnErrorTrappable },

        });

        #[derive(Debug, thiserror::Error)]
        pub(crate) enum SpawnErrorTrappable {
            #[error(transparent)]
            Normal(#[from] obelisk::activity::process::SpawnError),
            #[error(transparent)]
            Trap(#[from] wasmtime::Error),
        }
    }
}

impl<C: ClockFn> process_support::Host for ActivityCtx<C> {
    fn spawn(
        &mut self,
        command: String,
        options: process_support::SpawnOptions,
    ) -> Result<Resource<process_support::ChildProcess>, SpawnErrorTrappable> {
        let preopened_dir = self
            .preopened_dir
            .clone()
            .expect("process api can only be linked if preopened dir is enabled");
        let provider = self
            .process_provider
            .expect("process api can only be linked if it is enabled");

        let child_process = HostChildProcess::spawn(provider, command, &options, &preopened_dir)?;
        Ok(self
            .table()
            .push(child_process)
            .map_err(|res_err| wasmtime::Error::new(res_err))?)
    }

    fn convert_spawn_error(
        &mut self,
        err: SpawnErrorTrappable,
    ) -> wasmtime::Result<process_support::SpawnError> {
        match err {
            SpawnErrorTrappable::Normal(err) => Ok(err),
            SpawnErrorTrappable::Trap(err) => Err(err),
        }
    }
}

// Implement methods for the `child-process` resource
impl<C: ClockFn> process_support::HostChildProcess for ActivityCtx<C> {
    fn id(&mut self, self_handle: Resource<HostChildProcess>) -> wasmtime::Result<u32> {
        let child_process = self.table().get(&self_handle)?;
        Ok(child_process.id())
    }

    fn take_stdin(
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

    fn take_stdout(
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

    fn take_stderr(
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

    fn subscribe_wait(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<wasmtime::component::Resource<wasmtime_wasi::p2::DynPollable>> {
        assert!(!self_handle.owned()); // will be considered a child resource by `subscribe` (see docs).
        wasmtime_wasi::p2::subscribe(self.table(), self_handle)
    }

    fn wait(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<Result<Option<i32>, process_support::WaitError>> {
        let child_process = self.table().get_mut(&self_handle)?;
        Ok(child_process.try_wait())
    }

    async fn kill(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<Result<(), process_support::KillError>> {
        let child_process = self.table().get_mut(&self_handle)?;
        Ok(child_process.kill().await)
    }

    fn drop(
        &mut self,
        self_handle: Resource<process_support::ChildProcess>,
    ) -> wasmtime::Result<()> {
        let _child_process = self.table().delete(self_handle)?;
        Ok(())
    }
}
