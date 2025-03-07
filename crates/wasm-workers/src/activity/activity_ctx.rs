use super::activity_worker::ActivityConfig;
use crate::component_logger::{log_activities, ComponentLogger};
use crate::std_output_stream::LogStream;
use concepts::ExecutionId;
use tracing::Span;
use wasmtime::Engine;
use wasmtime::{component::ResourceTable, Store};
use wasmtime_wasi::{self, IoView, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

pub struct ActivityCtx {
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    component_logger: ComponentLogger,
}

impl WasiView for ActivityCtx {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi_ctx
    }
}

impl IoView for ActivityCtx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiHttpView for ActivityCtx {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http_ctx
    }
}

#[must_use]
pub fn store(
    engine: &Engine,
    execution_id: &ExecutionId,
    config: &ActivityConfig,
    worker_span: Span,
) -> Store<ActivityCtx> {
    let mut wasi_ctx = WasiCtxBuilder::new();
    if let Some(stdout) = config.forward_stdout {
        let stdout = LogStream::new(
            format!(
                "[{component_id} {execution_id} stdout]",
                component_id = config.component_id
            ),
            stdout,
        );
        wasi_ctx.stdout(stdout);
    }
    if let Some(stderr) = config.forward_stderr {
        let stderr = LogStream::new(
            format!(
                "[{component_id} {execution_id} stderr]",
                component_id = config.component_id
            ),
            stderr,
        );
        wasi_ctx.stderr(stderr);
    }
    for env_var in config.env_vars.iter() {
        wasi_ctx.env(&env_var.key, &env_var.val);
    }
    let ctx = ActivityCtx {
        table: ResourceTable::new(),
        wasi_ctx: wasi_ctx.build(),
        http_ctx: WasiHttpCtx::new(),
        component_logger: ComponentLogger { span: worker_span },
    };
    Store::new(engine, ctx)
}

impl log_activities::obelisk::log::log::Host for ActivityCtx {
    fn trace(&mut self, message: String) {
        self.component_logger.trace(&message);
    }

    fn debug(&mut self, message: String) {
        self.component_logger.debug(&message);
    }

    fn info(&mut self, message: String) {
        self.component_logger.info(&message);
    }

    fn warn(&mut self, message: String) {
        self.component_logger.warn(&message);
    }

    fn error(&mut self, message: String) {
        self.component_logger.error(&message);
    }
}
