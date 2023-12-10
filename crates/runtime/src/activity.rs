use anyhow::Context;
use std::{collections::HashMap, fmt::Debug, ops::DerefMut};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, info, trace};
use wasmtime::{component::Val, Config, Engine};

use crate::{
    event_history::SupportedFunctionResult,
    wasm_tools::{exported_interfaces, functions_to_metadata},
    {FunctionFqn, FunctionMetadata},
};

lazy_static::lazy_static! {
    static ref ENGINE: Engine = {
        let mut config = Config::new();
        // TODO: limit execution with epoch_interruption
        config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        config.wasm_component_model(true);
        config.async_support(true);
        config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(wasmtime::PoolingAllocationConfig::default()));
        Engine::new(&config).unwrap()
    };
}
mod http {
    // wasmtime/crates/wasi-http/tests/all/main.rs
    use wasmtime::{Engine, Store};
    use wasmtime_wasi::preview2::{Table, WasiCtx, WasiCtxBuilder, WasiView};
    use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

    pub(crate) struct Ctx {
        table: Table,
        wasi: WasiCtx,
        http: WasiHttpCtx,
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
        // Create our wasi context.
        let mut builder = WasiCtxBuilder::new();
        let ctx = Ctx {
            table: Table::new(),
            wasi: builder.build(),
            http: WasiHttpCtx {},
        };
        Store::new(engine, ctx)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ActivityPreload {
    Preinstance,
    Instance,
}

impl Default for ActivityPreload {
    fn default() -> Self {
        Self::Preinstance
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ActivityConfig {
    pub preload: ActivityPreload,
}

enum PreloadHolder {
    Preinstance,
    Instance(
        wasmtime::component::Instance,
        Mutex<wasmtime::Store<http::Ctx>>,
    ),
}

pub struct Activities {
    ifc_fqns_to_function_names: HashMap<
        String,      /* interface FQN: package_name/interface_name */
        Vec<String>, /* function names */
    >,
    functions_to_metadata: HashMap<FunctionFqn<'static>, FunctionMetadata>,
    instance_pre: wasmtime::component::InstancePre<http::Ctx>,
    preload_holder: PreloadHolder,
    wasm_path: String,
}

impl Activities {
    pub async fn new(wasm_path: String) -> Result<Self, anyhow::Error> {
        Self::new_with_config(wasm_path, &ActivityConfig::default()).await
    }

    pub async fn new_with_config(
        wasm_path: String,
        config: &ActivityConfig,
    ) -> Result<Self, anyhow::Error> {
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
        let functions_to_metadata = functions_to_metadata(exported_interfaces)?;

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

        let preload_holder = match config.preload {
            ActivityPreload::Preinstance => PreloadHolder::Preinstance,
            ActivityPreload::Instance => {
                let mut store = http::store(&ENGINE);
                let instance = instance_pre.instantiate_async(&mut store).await?;
                PreloadHolder::Instance(instance, Mutex::new(store))
            }
        };
        Ok(Self {
            ifc_fqns_to_function_names,
            instance_pre,
            wasm_path,
            functions_to_metadata,
            preload_holder,
        })
    }

    pub(crate) async fn run(
        &self,
        fqn: &FunctionFqn<'_>,
        params: &[Val],
    ) -> Result<SupportedFunctionResult, anyhow::Error> {
        debug!("Running `{fqn}`");
        let results_len = self.functions_to_metadata.get(fqn).unwrap().results_len;
        trace!("Running `{fqn}`({params:?}) -> results_len:{results_len}");

        fn try_lock(
            preload_holder: &PreloadHolder,
        ) -> Option<(
            &wasmtime::component::Instance,
            MutexGuard<wasmtime::Store<http::Ctx>>,
        )> {
            match preload_holder {
                PreloadHolder::Instance(instance, mutex) => {
                    mutex.try_lock().map(|guard| (instance, guard)).ok()
                }
                PreloadHolder::Preinstance => None,
            }
        }
        let mut store;
        let mut store_guard; // possibly uninitialized
        let instance_owned; // possibly uninitialized
        let (instance, mut store) =
            if let Some((instance, store_guard2)) = try_lock(&self.preload_holder) {
                store_guard = store_guard2;
                (instance, store_guard.deref_mut())
            } else {
                store = http::store(&ENGINE);
                instance_owned = self.instance_pre.instantiate_async(&mut store).await?;
                (&instance_owned, &mut store)
            };

        let func = {
            let mut store = &mut store;
            let mut exports = instance.exports(&mut store);
            let mut exports_instance = exports.root();
            let mut exports_instance =
                exports_instance.instance(&fqn.ifc_fqn).ok_or_else(|| {
                    anyhow::anyhow!(
                        "cannot find exported interface: `{ifc_fqn}` in `{wasm_path}`",
                        ifc_fqn = fqn.ifc_fqn,
                        wasm_path = self.wasm_path
                    )
                })?;
            exports_instance
                .func(&fqn.function_name)
                .ok_or(anyhow::anyhow!("function `{fqn}` not found"))?
        };
        // call func
        let mut results = Vec::from_iter(std::iter::repeat(Val::Bool(false)).take(results_len));
        func.call_async(&mut store, params, &mut results).await?;
        func.post_return_async(&mut store).await?;
        let results = SupportedFunctionResult::new(results);
        trace!("Finished `{fqn}` -> {results:?}");
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
        s.field("functions_to_metadata", &self.functions_to_metadata);
        s.field("wasm_path", &self.wasm_path);
        s.finish()
    }
}
