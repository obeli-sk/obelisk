use anyhow::Context;
use std::{collections::HashMap, fmt::Debug};
use tracing::{debug, info, trace};
use wasmtime::{component::Val, Config, Engine};

use crate::{
    event_history::SupportedFunctionResult,
    wasm_tools::{exported_interfaces, functions_to_metadata},
    workflow::{FunctionFqn, FunctionMetadata},
};

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
mod http {
    // wasmtime/crates/wasi-http/tests/all/main.rs
    use wasmtime::{Engine, Store};
    use wasmtime_wasi::preview2::{
        pipe::MemoryOutputPipe, Table, WasiCtx, WasiCtxBuilder, WasiView,
    };
    use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

    pub(crate) struct Ctx {
        table: Table,
        wasi: WasiCtx,
        http: WasiHttpCtx,
        stdout: MemoryOutputPipe,
        stderr: MemoryOutputPipe,
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
    }

    pub(crate) fn store(engine: &Engine) -> Store<Ctx> {
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
        };
        Store::new(engine, ctx)
    }

    // TODO: delete
    impl Drop for Ctx {
        fn drop(&mut self) {
            let stdout = self.stdout.contents();
            if !stdout.is_empty() {
                // println!("[guest] stdout:\n{}\n===", String::from_utf8_lossy(&stdout));
            }
            let stderr = self.stderr.contents();
            if !stderr.is_empty() {
                // println!("[guest] stderr:\n{}\n===", String::from_utf8_lossy(&stderr));
            }
        }
    }
}

pub struct Activities {
    ifc_fqns_to_function_names: HashMap<
        String,      /* interface FQN: package_name/interface_name */
        Vec<String>, /* function names */
    >,
    functions_to_metadata: HashMap<FunctionFqn, FunctionMetadata>,
    instance_pre: wasmtime::component::InstancePre<http::Ctx>, // pre-started instance
    wasm_path: String,
}

impl Activities {
    pub async fn new(wasm_path: String) -> Result<Self, anyhow::Error> {
        let wasm =
            std::fs::read(&wasm_path).with_context(|| format!("cannot open `{wasm_path}`"))?;
        let decoded =
            wit_component::decode(&wasm).with_context(|| format!("cannot decode `{wasm_path}`"))?;

        let exported_interfaces = exported_interfaces(&decoded)
            .with_context(|| format!("error parsing `{wasm_path}`"))?;

        let mut ifc_fqns_to_function_names = HashMap::new();
        for (package_name, ifc_name, functions) in exported_interfaces.clone() {
            let interface_fqn = format!("{package_name}/{ifc_name}");
            ifc_fqns_to_function_names.insert(
                interface_fqn,
                functions.keys().map(String::to_owned).collect(),
            );
        }
        let functions_to_metadata = functions_to_metadata(exported_interfaces);

        let instance_pre: wasmtime::component::InstancePre<http::Ctx> = {
            let mut linker = wasmtime::component::Linker::new(&ENGINE);
            wasmtime_wasi::preview2::command::add_to_linker(&mut linker)?;
            wasmtime_wasi_http::bindings::http::outgoing_handler::add_to_linker(
                &mut linker,
                |t| t,
            )?;
            wasmtime_wasi_http::bindings::http::types::add_to_linker(&mut linker, |t| t)?;
            // Compile the wasm component
            let component = wasmtime::component::Component::from_binary(&ENGINE, &wasm)?;
            linker.instantiate_pre(&component)?
        };

        info!("Loaded {ifc_fqns_to_function_names:?}");
        Ok(Self {
            ifc_fqns_to_function_names,
            instance_pre,
            wasm_path,
            functions_to_metadata,
        })
    }

    pub(crate) async fn run(
        &self,
        ifc_fqn: &str,
        function_name: &str,
        params: &[Val],
    ) -> Result<SupportedFunctionResult, anyhow::Error> {
        debug!("`{ifc_fqn:?}`.`{function_name}` ");
        let fqn = FunctionFqn {
            ifc_fqn: Some(ifc_fqn.to_string()),
            function_name: function_name.to_string(),
        };
        let results_len = self.functions_to_metadata.get(&fqn).unwrap().results_len;
        trace!("`{ifc_fqn:?}`.`{function_name}`({params:?}) -> results_len:{results_len}");
        let mut store = http::store(&ENGINE);
        let instance = self.instance_pre.instantiate_async(&mut store).await?;
        let func = {
            let mut store = &mut store;
            let mut exports = instance.exports(&mut store);
            let mut exports_instance = exports.root();
            let mut exports_instance = exports_instance.instance(ifc_fqn).unwrap_or_else(|| {
                panic!(
                    "cannot find exported interface: `{ifc_fqn}` in `{wasm_path}`",
                    wasm_path = self.wasm_path
                )
            });
            exports_instance.func(function_name).ok_or(anyhow::anyhow!(
                "function `{ifc_fqn:?}`.`{function_name}` not found"
            ))?
        };
        // call func
        let mut results = Vec::from_iter(std::iter::repeat(Val::Bool(false)).take(results_len));
        func.call_async(&mut store, params, &mut results).await?;
        func.post_return_async(&mut store).await?;
        let results = SupportedFunctionResult::new(results);
        trace!("`{ifc_fqn:?}`.`{function_name}` -> {results:?}");
        Ok(results)
    }

    pub(crate) fn interfaces(&self) -> impl Iterator<Item = &str> {
        self.ifc_fqns_to_function_names.keys().map(String::as_str)
    }

    pub(crate) fn functions(&self, interface: &str) -> impl Iterator<Item = &str> {
        self.ifc_fqns_to_function_names
            .get(interface)
            .map(|vec| vec.iter().map(String::as_str))
            .unwrap()
    }
}

impl Debug for Activities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Activities");
        s.field("interfaces_functions", &self.ifc_fqns_to_function_names);
        s.field("wasm_path", &self.wasm_path);
        s.finish()
    }
}
