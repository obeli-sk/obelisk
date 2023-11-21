use anyhow::{bail, Context};
use std::{collections::HashMap, sync::Arc};
use wasmtime::{component::Resource, Config, Engine, Store};
use wasmtime_wasi::preview2::{pipe::MemoryOutputPipe, Table, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{
    types::{self, HostFutureIncomingResponse, OutgoingRequest},
    WasiHttpCtx, WasiHttpView,
};
use wit_component::DecodedWasm;

lazy_static::lazy_static! {
    static ref ENGINE: Engine = {
        let mut config = Config::new();
        // TODO: limit execution with epoch_interruption
        config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        config.wasm_component_model(true);
        config.async_support(true);
        Engine::new(&config).unwrap()
    };
}

type RequestSender = Arc<
    dyn Fn(&mut Ctx, OutgoingRequest) -> wasmtime::Result<Resource<HostFutureIncomingResponse>>
        + Send
        + Sync,
>;

pub(crate) struct Ctx {
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

    Store::new(engine, ctx)
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

pub(crate) struct Activities {
    activity_functions: HashMap<String, wit_parser::Function>,
    instance_pre: wasmtime::component::InstancePre<Ctx>,
}

impl Activities {
    pub(crate) async fn new(wasm_path: &str) -> Result<Self, anyhow::Error> {
        let activity_wasm_contents =
            std::fs::read(wasm_path).with_context(|| format!("cannot open {wasm_path}"))?;
        let decoded = wit_component::decode(&activity_wasm_contents)
            .with_context(|| format!("cannot decode {wasm_path}"))?;

        let (resolve, world_id) = match decoded {
            DecodedWasm::Component(resolve, world_id) => (resolve, world_id),
            _ => bail!("cannot parse component"),
        };

        let functions = {
            let world = resolve.worlds.get(world_id).expect("world must exist");
            world.exports.iter().filter_map(|(_, item)| match item {
                wit_parser::WorldItem::Function(f) => Some(f.clone()),
                _ => None,
            })
        };
        let activity_functions = functions
            .map(|function| (function.name.clone(), function))
            .collect::<HashMap<_, _>>();
        let instance_pre: wasmtime::component::InstancePre<Ctx> = {
            let mut linker = wasmtime::component::Linker::new(&ENGINE);
            wasmtime_wasi::preview2::command::add_to_linker(&mut linker)?;
            wasmtime_wasi_http::bindings::http::outgoing_handler::add_to_linker(
                &mut linker,
                |t| t,
            )?;
            wasmtime_wasi_http::bindings::http::types::add_to_linker(&mut linker, |t| t)?;
            // Read and compile the wasm component
            let component =
                wasmtime::component::Component::from_binary(&ENGINE, &activity_wasm_contents)?;
            linker.instantiate_pre(&component)?
        };
        Ok(Self {
            activity_functions,
            instance_pre,
        })
    }

    pub(crate) async fn run(
        &self,
        function_name: &str,
    ) -> Result<Result<String, String>, anyhow::Error> {
        let mut store = store(&ENGINE);
        let instance = self.instance_pre.instantiate_async(&mut store).await?;
        let func = {
            let mut store = &mut store;
            let mut exports = instance.exports(&mut store);
            let mut exports = exports.root();
            *exports
                .typed_func::<(), (Result<String, String>,)>(function_name)?
                .func()
        };
        // call func
        let callee = unsafe {
            wasmtime::component::TypedFunc::<(), (Result<String, String>,)>::new_unchecked(func)
        };
        let (ret,) = callee.call_async(&mut store, ()).await?;
        callee.post_return_async(&mut store).await?;
        Ok(ret)
    }

    pub(crate) fn function_names(&self) -> impl Iterator<Item = &str> {
        self.activity_functions.keys().map(String::as_str)
    }
}
