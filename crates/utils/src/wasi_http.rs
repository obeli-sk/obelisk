// wasmtime/crates/wasi-http/tests/all/main.rs
use std::sync::Arc;
use wasmtime::Engine;
use wasmtime::{
    component::{Resource, ResourceTable},
    Store,
};
use wasmtime_wasi::{self, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::bindings::http::types::ErrorCode;
use wasmtime_wasi_http::{
    types::{self, HostFutureIncomingResponse, OutgoingRequest},
    WasiHttpCtx, WasiHttpView,
};

type RequestSender = Arc<
    dyn Fn(&mut Ctx, OutgoingRequest) -> wasmtime::Result<Resource<HostFutureIncomingResponse>>
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
        request: OutgoingRequest,
    ) -> wasmtime::Result<Resource<HostFutureIncomingResponse>> {
        if let Some(rejected_authority) = &self.rejected_authority {
            let (auth, _port) = request.authority.split_once(':').unwrap();
            if auth == rejected_authority {
                return Err(ErrorCode::HttpRequestDenied.into());
            }
        }
        if let Some(send_request) = self.send_request.clone() {
            send_request(self, request)
        } else {
            types::default_send_request(self, request)
        }
    }
}

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
        http: WasiHttpCtx {},
        // stderr,
        // stdout,
        send_request: None,
        rejected_authority: None,
    };

    Store::new(&engine, ctx)
}
