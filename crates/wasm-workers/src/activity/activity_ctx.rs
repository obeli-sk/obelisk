use super::activity_worker::{ActivityConfig, ProcessProvider};
use crate::component_logger::log_activities::obelisk::log::log::Host;
use crate::component_logger::{ComponentLogger, LogStrageConfig, log_activities};
use crate::http_request_policy::{HttpRequestPolicy, PlaceholderSecret, generate_placeholder};
use crate::std_output_stream::{LogStream, StdOutput};
use bytes::Bytes;
use concepts::ExecutionId;
use concepts::prefixed_ulid::RunId;
use concepts::storage::LogLevel;
use concepts::storage::http_client_trace::{RequestTrace, ResponseTrace};
use concepts::time::ClockFn;
use hyper::body::Body;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{Instrument, Span, debug, info_span};
use wasmtime::Engine;
use wasmtime::component::Resource;
use wasmtime::{Store, component::ResourceTable};
use wasmtime_wasi::{DirPerms, FilePerms};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::bindings::http::types::Scheme;
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::types::{
    HostFutureIncomingResponse, HostIncomingRequest, OutgoingRequestConfig,
    default_send_request_handler,
};
use wasmtime_wasi_http::{HttpResult, WasiHttpCtx, WasiHttpView};
use wasmtime_wasi_io::IoView;

pub type HttpClientTracesContainer = Vec<(RequestTrace, oneshot::Receiver<ResponseTrace>)>;

pub struct ActivityCtx {
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    component_logger: ComponentLogger,
    clock_fn: Box<dyn ClockFn>,
    pub(crate) http_client_traces: HttpClientTracesContainer,
    pub(crate) preopened_dir: Option<Arc<Path>>,
    pub(crate) process_provider: Option<ProcessProvider>,
    http_policy: HttpRequestPolicy,
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
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn new_incoming_request<B>(
        &mut self,
        _scheme: Scheme,
        _req: hyper::Request<B>,
    ) -> wasmtime::Result<Resource<HostIncomingRequest>>
    where
        B: Body<Data = Bytes, Error = hyper::Error> + Send + 'static,
        Self: Sized,
    {
        unreachable!("incoming requests cannot be made")
    }

    /// Send an outgoing request.
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        // Prepare request trace & channel
        let req = RequestTrace {
            sent_at: self.clock_fn.now(),
            uri: request.uri().to_string(),
            method: request.method().to_string(),
        };
        let (resp_trace_tx, resp_trace_rx) = oneshot::channel();
        self.http_client_traces.push((req, resp_trace_rx));

        // Apply HTTP policy (allowlist + placeholder replacement in headers and query params)
        let http_policy_res = self.http_policy.apply(&mut request);
        if let Err(err) = http_policy_res {
            self.warn(format!("{err}")); // Append to execution's logs table
            let _ = resp_trace_tx.send(ResponseTrace {
                finished_at: self.clock_fn.now(),
                status: Err(err.to_string()),
            });
            let err = wasmtime_wasi_http::bindings::http::types::ErrorCode::from(err);
            return Err(err.into());
        }

        let span = info_span!(parent: &self.component_logger.span, "send_request",
            otel.name = format!("send_request {} {}", request.method(), request.uri()),
            method = %request.method(),
            uri = %request.uri(),
        );
        let clock_fn = self.clock_fn.clone_box();
        let http_policy = self.http_policy.clone();
        span.in_scope(|| debug!("Sending {request:?}"));
        let handle = wasmtime_wasi::runtime::spawn(
            async move {
                http_policy.apply_body_replacement(&mut request).await;
                let resp_result = default_send_request_handler(request, config).await;
                debug!("Got response {resp_result:?}");
                let _ = resp_trace_tx.send(ResponseTrace {
                    finished_at: clock_fn.now(),
                    status: resp_result
                        .as_ref()
                        .map(|resp| resp.resp.status().as_u16())
                        .map_err(std::string::ToString::to_string),
                });
                Ok(resp_result)
            }
            .instrument(span),
        );
        Ok(HostFutureIncomingResponse::pending(handle))
    }
}

pub(crate) struct ActivityPreopenIoError {
    pub err: wasmtime::Error,
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn store(
    engine: &Engine,
    execution_id: ExecutionId,
    run_id: RunId,
    config: &ActivityConfig,
    worker_span: Span,
    clock_fn: Box<dyn ClockFn>,
    preopened_dir: Option<PathBuf>,
    stdout: Option<StdOutput>,
    stderr: Option<StdOutput>,
    logs_storage_config: Option<LogStrageConfig>,
) -> Result<Store<ActivityCtx>, ActivityPreopenIoError> {
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
    let mut policy_secrets = Vec::new();
    for secret_config in config.secrets.iter() {
        for (env_key, real_value) in &secret_config.env_mappings {
            let placeholder = generate_placeholder();
            wasi_ctx.env(env_key, &placeholder);
            policy_secrets.push(PlaceholderSecret {
                placeholder,
                real_value: real_value.clone(),
                allowed_hosts: secret_config.hosts.clone(),
                replace_in: secret_config.replace_in.clone(),
            });
        }
    }
    let http_policy = HttpRequestPolicy {
        allowed_hosts: config.allowed_hosts.clone(),
        secrets: policy_secrets,
    };

    if let Some(preopened_dir) = &preopened_dir {
        let res = wasi_ctx.preopened_dir(preopened_dir, ".", DirPerms::all(), FilePerms::all());
        if let Err(err) = res {
            return Err(ActivityPreopenIoError { err });
        }
    }

    let ctx = ActivityCtx {
        table: ResourceTable::new(),
        wasi_ctx: wasi_ctx.build(),
        http_ctx: WasiHttpCtx::new(),
        component_logger: ComponentLogger {
            span: worker_span,
            execution_id,
            run_id,
            logs_storage_config,
        },
        http_client_traces: HttpClientTracesContainer::default(),
        clock_fn,
        preopened_dir: preopened_dir.map(Arc::from),
        process_provider: config
            .directories_config
            .as_ref()
            .and_then(|dir| dir.process_provider),
        http_policy,
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
