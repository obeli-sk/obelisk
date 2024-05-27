// wasmtime/crates/wasi-http/tests/all/main.rs
use std::sync::Arc;
use wasmtime::Engine;
use wasmtime::{component::ResourceTable, Store};
use wasmtime_wasi::{self, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{
    bindings::http::types::ErrorCode,
    body::HyperOutgoingBody,
    types::{self, HostFutureIncomingResponse, OutgoingRequestConfig},
    HttpResult, WasiHttpCtx, WasiHttpView,
};

type RequestSender = Arc<
    dyn Fn(hyper::Request<HyperOutgoingBody>, OutgoingRequestConfig) -> HostFutureIncomingResponse
        + Send
        + Sync,
>;

pub struct Ctx {
    table: ResourceTable,
    wasi: WasiCtx,
    http: WasiHttpCtx,
    // stdout: MemoryOutputPipe,
    // stderr: MemoryOutputPipe,
    send_request: Option<RequestSender>,
    rejected_authority: Option<String>,
}

impl WasiView for Ctx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

impl WasiHttpView for Ctx {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        if let Some(rejected_authority) = &self.rejected_authority {
            let authority = request.uri().authority().map(ToString::to_string).unwrap();
            if &authority == rejected_authority {
                return Err(ErrorCode::HttpRequestDenied.into());
            }
        }
        if let Some(send_request) = self.send_request.clone() {
            Ok(send_request(request, config))
        } else {
            Ok(types::default_send_request(request, config))
        }
    }
}

#[must_use]
pub fn store(engine: &Engine) -> Store<Ctx> {
    // let stdout = MemoryOutputPipe::new(4096);
    // let stderr = MemoryOutputPipe::new(4096);

    // Create our wasi context.
    let mut builder = WasiCtxBuilder::new();
    // builder.stdout(stdout.clone());
    // builder.stderr(stderr.clone());
    let ctx = Ctx {
        table: ResourceTable::new(),
        wasi: builder.build(),
        http: WasiHttpCtx::new(),
        // stderr,
        // stdout,
        send_request: None,
        rejected_authority: None,
    };

    Store::new(engine, ctx)
}
