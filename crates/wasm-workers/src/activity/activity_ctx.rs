use super::activity_worker::{ActivityConfig, ProcessProvider};
use crate::component_logger::{ComponentLogger, LogStrageConfig, log_activities};
use crate::http_hooks::{HttpClientTracesContainer, HttpHooks};
use crate::policy_builder::build_http_policy;
use crate::std_output_stream::{LogStream, StdOutput};
use concepts::storage::LogLevel;
use concepts::time::ClockFn;
use executor::worker::WorkerContext;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wasmtime::Engine;
use wasmtime::{Store, component::ResourceTable};
use wasmtime_wasi::{DirPerms, FilePerms};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::WasiHttpCtx;
use wasmtime_wasi_http::p2::{WasiHttpCtxView, WasiHttpView};
use wasmtime_wasi_io::IoView;

pub struct ActivityCtx {
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    component_logger: ComponentLogger,
    pub(crate) http_hooks: HttpHooks,
    pub(crate) preopened_dir: Option<Arc<Path>>,
    pub(crate) process_provider: Option<ProcessProvider>,
    pub(crate) executor_close_watcher: tokio::sync::watch::Receiver<bool>,
}

impl wasmtime::component::HasData for ActivityCtx {
    type Data<'a> = &'a mut ActivityCtx;
}

impl WasiView for ActivityCtx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.table,
        }
    }
}

impl IoView for ActivityCtx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiHttpView for ActivityCtx {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http_ctx,
            table: &mut self.table,
            hooks: &mut self.http_hooks,
        }
    }
}

pub(crate) struct ActivityPreopenIoError {
    pub err: wasmtime::Error,
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn store(
    engine: &Engine,
    ctx: WorkerContext,
    config: &ActivityConfig,
    clock_fn: Box<dyn ClockFn>,
    preopened_dir: Option<PathBuf>,
    stdout: Option<StdOutput>,
    stderr: Option<StdOutput>,
    logs_storage_config: Option<LogStrageConfig>,
) -> Result<Store<ActivityCtx>, ActivityPreopenIoError> {
    let execution_id = ctx.execution_id;
    let run_id = ctx.locked_event.run_id;

    let mut wasi_ctx = WasiCtxBuilder::new();
    if let Some(stdout) = stdout {
        let stdout = LogStream::new(
            format!(
                "[{component_id} {execution_id} stdout]",
                component_id = config.component_id
            ),
            stdout,
        );
        wasi_ctx.stdout(stdout);
    }
    if let Some(stderr) = stderr {
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

    // Generate fresh placeholders for this execution run
    let http_policy = build_http_policy(&config.allowed_hosts, &mut wasi_ctx);

    if let Some(preopened_dir) = &preopened_dir {
        let res = wasi_ctx.preopened_dir(preopened_dir, ".", DirPerms::all(), FilePerms::all());
        if let Err(err) = res {
            return Err(ActivityPreopenIoError { err });
        }
    }

    let component_logger = ComponentLogger {
        span: ctx.worker_span,
        execution_id,
        run_id,
        logs_storage_config,
    };
    let ctx = ActivityCtx {
        table: ResourceTable::new(),
        wasi_ctx: wasi_ctx.build(),
        http_ctx: WasiHttpCtx::new(),
        http_hooks: HttpHooks {
            clock_fn,
            http_client_traces: HttpClientTracesContainer::default(),
            http_policy,
            component_logger: component_logger.clone(),
        },
        component_logger,
        preopened_dir: preopened_dir.map(Arc::from),
        process_provider: config
            .directories_config
            .as_ref()
            .and_then(|dir| dir.process_provider),
        executor_close_watcher: ctx.executor_close_watcher,
    };
    Ok(Store::new(engine, ctx))
}

impl log_activities::obelisk::log::log::Host for ActivityCtx {
    fn trace(&mut self, message: String) {
        self.component_logger.log(LogLevel::Trace, message);
    }

    fn debug(&mut self, message: String) {
        self.component_logger.log(LogLevel::Debug, message);
    }

    fn info(&mut self, message: String) {
        self.component_logger.log(LogLevel::Info, message);
    }

    fn warn(&mut self, message: String) {
        self.component_logger.log(LogLevel::Warn, message);
    }

    fn error(&mut self, message: String) {
        self.component_logger.log(LogLevel::Error, message);
    }
}
