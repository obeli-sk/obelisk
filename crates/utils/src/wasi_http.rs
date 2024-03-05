// wasmtime/crates/wasi-http/tests/all/main.rs
use std::sync::Arc;
use wasmtime::Engine;
use wasmtime::{
    component::{Resource, ResourceTable},
    Store,
};
use wasmtime_wasi::preview2::{WasiCtx, WasiCtxBuilder, WasiView};
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
    send_request: Option<RequestSender>,
}

impl WasiView for Ctx {
    fn table(&self) -> &ResourceTable {
        &self.table
    }
    fn table_mut(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&self) -> &WasiCtx {
        &self.wasi
    }
    fn ctx_mut(&mut self) -> &mut WasiCtx {
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
        if let Some(send_request) = self.send_request.clone() {
            send_request(self, request)
        } else {
            types::default_send_request(self, request)
        }
    }
}

pub fn store(engine: &Engine) -> Store<Ctx> {
    // Create our wasi context.
    let mut builder = WasiCtxBuilder::new();
    let ctx = Ctx {
        table: ResourceTable::new(),
        wasi: builder.build(),
        http: WasiHttpCtx {},

        send_request: None,
    };

    Store::new(engine, ctx)
}
