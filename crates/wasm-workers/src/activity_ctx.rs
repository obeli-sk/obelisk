use wasmtime::Engine;
use wasmtime::{component::ResourceTable, Store};
use wasmtime_wasi::{self, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

#[allow(clippy::struct_field_names)]
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
pub fn store(engine: &Engine) -> Store<ActivityCtx> {
    let mut builder = WasiCtxBuilder::new();
    let ctx = ActivityCtx {
        table: ResourceTable::new(),
        wasi_ctx: builder.build(),
        http_ctx: WasiHttpCtx::new(),
    };
    Store::new(engine, ctx)
}
