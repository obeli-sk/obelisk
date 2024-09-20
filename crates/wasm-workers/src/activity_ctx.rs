use crate::envvar::EnvVar;
use crate::std_output_stream::{LogStream, StdOutput};
use concepts::{ConfigId, ExecutionId};
use wasmtime::Engine;
use wasmtime::{component::ResourceTable, Store};
use wasmtime_wasi::{self, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

pub struct ActivityCtx {
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
}

impl WasiView for ActivityCtx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi_ctx
    }
}

impl WasiHttpView for ActivityCtx {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

#[must_use]
pub fn store(
    engine: &Engine,
    execution_id: ExecutionId,
    config_id: &ConfigId,
    forward_stdout: Option<StdOutput>,
    forward_stderr: Option<StdOutput>,
    env_vars: &[EnvVar],
) -> Store<ActivityCtx> {
    let mut wasi_ctx = WasiCtxBuilder::new();
    if let Some(stdout) = forward_stdout {
        let stdout = LogStream::new(format!("[{config_id} {execution_id} stdout]"), stdout);
        wasi_ctx.stdout(stdout);
    }
    if let Some(stderr) = forward_stderr {
        let stderr = LogStream::new(format!("[{config_id} {execution_id} stderr]"), stderr);
        wasi_ctx.stderr(stderr);
    }
    for env_var in env_vars {
        wasi_ctx.env(&env_var.key, &env_var.val);
    }
    let ctx = ActivityCtx {
        table: ResourceTable::new(),
        wasi_ctx: wasi_ctx.build(),
        http_ctx: WasiHttpCtx::new(),
    };
    Store::new(engine, ctx)
}
