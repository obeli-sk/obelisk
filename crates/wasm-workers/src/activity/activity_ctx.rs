use super::activity_worker::ActivityConfig;
use crate::component_logger::{ComponentLogger, log_activities};
use crate::std_output_stream::LogStream;
use bytes::Bytes;
use concepts::ExecutionId;
use concepts::storage::http_client_trace::{RequestTrace, ResponseTrace};
use concepts::time::ClockFn;
use hyper::body::Body;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tracing::{Instrument, Span, debug, info_span};
use wasmtime::Engine;
use wasmtime::component::Resource;
use wasmtime::{Store, component::ResourceTable};
use wasmtime_wasi::p2::{IoView, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::bindings::http::types::Scheme;
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::types::{
    HostFutureIncomingResponse, HostIncomingRequest, OutgoingRequestConfig,
    default_send_request_handler,
};
use wasmtime_wasi_http::{HttpResult, WasiHttpCtx, WasiHttpView};

pub type HttpClientTracesContainer =
    Arc<Mutex<Vec<(RequestTrace, oneshot::Receiver<ResponseTrace>)>>>;

pub struct ActivityCtx<C: ClockFn> {
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    component_logger: ComponentLogger,
    clock_fn: C,
    http_client_traces: HttpClientTracesContainer,
}

impl<C: ClockFn> WasiView for ActivityCtx<C> {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi_ctx
    }
}

impl<C: ClockFn> IoView for ActivityCtx<C> {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl<C: ClockFn + 'static> WasiHttpView for ActivityCtx<C> {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http_ctx
    }

    fn new_incoming_request<B>(
        &mut self,
        _scheme: Scheme,
        _req: hyper::Request<B>,
    ) -> wasmtime::Result<Resource<HostIncomingRequest>>
    where
        B: Body<Data = Bytes, Error = hyper::Error> + Send + Sync + 'static,
        Self: Sized,
    {
        unreachable!("incoming requests cannot be made")
    }

    /// Send an outgoing request.
    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let span = info_span!(parent: &self.component_logger.span, "send_request",
            otel.name = format!("send_request {} {}", request.method(), request.uri()),
            method = %request.method(),
            uri = %request.uri(),
        );
        let req = RequestTrace {
            sent_at: self.clock_fn.now(),
            uri: request.uri().to_string(),
            method: request.method().to_string(),
        };
        let (resp_trace_tx, resp_trace_rx) = oneshot::channel();
        self.http_client_traces
            .lock()
            .unwrap()
            .push((req, resp_trace_rx));
        let clock_fn = self.clock_fn.clone();
        span.in_scope(|| debug!("Sending {request:?}"));
        let handle = wasmtime_wasi::runtime::spawn(
            async move {
                let resp_result = default_send_request_handler(request, config).await;
                debug!("Got response {resp_result:?}");
                let resp_trace = ResponseTrace {
                    finished_at: clock_fn.now(),
                    status: resp_result
                        .as_ref()
                        .map(|resp| resp.resp.status().as_u16())
                        .map_err(std::string::ToString::to_string),
                };
                let _ = resp_trace_tx.send(resp_trace);
                Ok(resp_result)
            }
            .instrument(span),
        );
        Ok(HostFutureIncomingResponse::pending(handle))
    }
}

#[must_use]
pub fn store<C: ClockFn>(
    engine: &Engine,
    execution_id: &ExecutionId,
    config: &ActivityConfig,
    worker_span: Span,
    clock_fn: C,
    http_client_traces: HttpClientTracesContainer,
) -> Store<ActivityCtx<C>> {
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
        http_client_traces,
        clock_fn,
    };
    Store::new(engine, ctx)
}

impl<C: ClockFn> log_activities::obelisk::log::log::Host for ActivityCtx<C> {
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
