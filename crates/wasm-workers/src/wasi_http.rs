use wasmtime::Engine;
use wasmtime::{component::ResourceTable, Store};
use wasmtime_wasi::{self, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

pub struct Ctx {
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
}

impl WasiView for Ctx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi_ctx
    }
}

impl WasiHttpView for Ctx {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

#[must_use]
pub fn store(engine: &Engine) -> Store<Ctx> {
    let mut builder = WasiCtxBuilder::new();
    let ctx = Ctx {
        table: ResourceTable::new(),
        wasi_ctx: builder.build(),
        http_ctx: WasiHttpCtx::new(),
    };
    Store::new(engine, ctx)
}
