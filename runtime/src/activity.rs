use std::sync::Arc;
use wasmtime::{
    component::{Component, Linker, Resource},
    Config, Engine, Store,
};
use wasmtime_wasi::preview2::{pipe::MemoryOutputPipe, Table, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{
    types::{self, HostFutureIncomingResponse, OutgoingRequest},
    WasiHttpCtx, WasiHttpView,
};

type RequestSender = Arc<
    dyn Fn(&mut Ctx, OutgoingRequest) -> wasmtime::Result<Resource<HostFutureIncomingResponse>>
        + Send
        + Sync,
>;

struct Ctx {
    table: Table,
    wasi: WasiCtx,
    http: WasiHttpCtx,
    stdout: MemoryOutputPipe,
    stderr: MemoryOutputPipe,
    send_request: Option<RequestSender>,
}

impl WasiView for Ctx {
    fn table(&self) -> &Table {
        &self.table
    }
    fn table_mut(&mut self) -> &mut Table {
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

    fn table(&mut self) -> &mut Table {
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

fn store(engine: &Engine) -> Store<Ctx> {
    let stdout = MemoryOutputPipe::new(4096);
    let stderr = MemoryOutputPipe::new(4096);

    // Create our wasi context.
    let mut builder = WasiCtxBuilder::new();
    builder.stdout(stdout.clone());
    builder.stderr(stderr.clone());
    let ctx = Ctx {
        table: Table::new(),
        wasi: builder.build(),
        http: WasiHttpCtx {},
        stderr,
        stdout,
        send_request: None,
    };

    Store::new(&engine, ctx)
}

impl Drop for Ctx {
    fn drop(&mut self) {
        let stdout = self.stdout.contents();
        if !stdout.is_empty() {
            println!("[guest] stdout:\n{}\n===", String::from_utf8_lossy(&stdout));
        }
        let stderr = self.stderr.contents();
        if !stderr.is_empty() {
            println!("[guest] stderr:\n{}\n===", String::from_utf8_lossy(&stderr));
        }
    }
}

pub(crate) async fn activity_example(
    wasm_path: &str,
    function_name: &str,
) -> Result<(), anyhow::Error> {
    let mut config = Config::new();
    // TODO: limit execution with epoch_interruption
    config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
    config.wasm_component_model(true);
    config.async_support(true);
    let engine = Engine::new(&config)?;
    let component = Component::from_file(&engine, wasm_path)?;
    let mut store = store(&engine);
    let mut linker = Linker::new(&engine);

    wasmtime_wasi::preview2::command::add_to_linker(&mut linker)?;
    wasmtime_wasi_http::bindings::http::outgoing_handler::add_to_linker(&mut linker, |t| t)?;
    wasmtime_wasi_http::bindings::http::types::add_to_linker(&mut linker, |t| t)?;

    let instance = linker.instantiate_async(&mut store, &component).await?;
    let func = {
        let mut store = &mut store;
        let mut exports = instance.exports(&mut store);
        let mut exports = exports.root();
        exports
            .typed_func::<(), (Result<String, String>,)>(function_name)?
            .func()
            .clone()
    };
    // call func
    let callee = unsafe {
        wasmtime::component::TypedFunc::<(), (Result<String, String>,)>::new_unchecked(func)
    };
    let (ret,) = callee.call_async(&mut store, ()).await?;
    callee.post_return_async(&mut store).await?;
    println!("activity returned {ret:?}");
    Ok(())
}
