use anyhow::Context;
use assert_matches::assert_matches;
use std::{collections::HashMap, fmt::Debug, ops::DerefMut, sync::Arc, time::Duration};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, info, trace};
use wasmtime::{component::Val, Config, Engine};

use crate::{
    event_history::{SupportedFunctionResult, HOST_ACTIVITY_SLEEP_FQN},
    wasm_tools::{exported_interfaces, functions_to_metadata},
    ActivityFailed, {FunctionFqn, FunctionMetadata},
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityRequest {
    pub(crate) fqn: Arc<FunctionFqn<'static>>,
    pub(crate) params: Arc<Vec<Val>>,
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

pub struct Activity {
    ifc_fqns_to_function_names: HashMap<
        String,      /* interface FQN: package_name/interface_name */
        Vec<String>, /* function names */
    >,
    functions_to_metadata: HashMap<Arc<FunctionFqn<'static>>, FunctionMetadata>,
    instance_pre: wasmtime::component::InstancePre<http::Ctx>,
    preload_holder: PreloadHolder,
    pub(crate) wasm_path: String,
}

impl Activity {
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
        assert!(
            functions_to_metadata
                .get(&HOST_ACTIVITY_SLEEP_FQN)
                .is_none(),
            "host function `{HOST_ACTIVITY_SLEEP_FQN}` cannot overlap with wasm activity"
        );

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

    pub(crate) async fn run(
        &self,
        request: &ActivityRequest,
    ) -> Result<SupportedFunctionResult, ActivityFailed> {
        debug!("Running `{fqn}`", fqn = request.fqn);
        let results_len = self
            .functions_to_metadata
            .get(&request.fqn)
            .unwrap()
            .results_len;
        trace!(
            "Running `{fqn}`({params:?}) -> results_len:{results_len}",
            fqn = request.fqn,
            params = request.params
        );
        if *request.fqn == HOST_ACTIVITY_SLEEP_FQN {
            // sleep
            assert_eq!(request.params.len(), 1);
            let duration = request.params.get(0).unwrap();
            let duration = *assert_matches!(duration, Val::U64(v) => v);
            tokio::time::sleep(Duration::from_millis(duration)).await;
            return Ok(SupportedFunctionResult::None);
        }

        let mut store;
        let mut store_guard; // possibly uninitialized
        let instance_owned; // possibly uninitialized

        // Get existing or create new store and instance.
        let (instance, mut store) =
            if let Some((instance, store_guard2)) = Self::try_lock(&self.preload_holder) {
                store_guard = store_guard2;
                (instance, store_guard.deref_mut())
            } else {
                store = http::store(&ENGINE);
                instance_owned = self
                    .instance_pre
                    .instantiate_async(&mut store)
                    .await
                    .map_err(|err| ActivityFailed {
                        activity_fqn: request.fqn.clone(),
                        reason: format!("wasm instantiation error: `{err}`"),
                    })?;
                (&instance_owned, &mut store)
            };

        let func = {
            let mut store = &mut store;
            let mut exports = instance.exports(&mut store);
            let mut exports_instance = exports.root();
            let mut exports_instance =
                exports_instance
                    .instance(&request.fqn.ifc_fqn)
                    .ok_or_else(|| ActivityFailed {
                        activity_fqn: request.fqn.clone(),
                        reason: "cannot find exported interface".to_string(),
                    })?;
            exports_instance
                .func(&request.fqn.function_name)
                .ok_or(ActivityFailed {
                    activity_fqn: request.fqn.clone(),
                    reason: "function not found".to_string(),
                })?
        };
        // call func
        let mut results = Vec::from_iter(std::iter::repeat(Val::Bool(false)).take(results_len));
        func.call_async(&mut store, &request.params, &mut results)
            .await
            .map_err(|err| ActivityFailed {
                activity_fqn: request.fqn.clone(),
                reason: format!("wasm function call error: `{err}`"),
            })?;
        func.post_return_async(&mut store)
            .await
            .map_err(|err| ActivityFailed {
                activity_fqn: request.fqn.clone(),
                reason: format!("wasm post function call error: `{err}`"),
            })?;
        let results = SupportedFunctionResult::new(results);
        trace!("Finished `{fqn}` -> {results:?}", fqn = request.fqn);
        Ok(results)
    }

    pub(crate) fn interfaces(&self) -> impl Iterator<Item = &str> {
        self.ifc_fqns_to_function_names.keys().map(String::as_str)
    }

    pub(crate) fn functions_of_interface(&self, interface: &str) -> impl Iterator<Item = &str> {
        self.ifc_fqns_to_function_names
            .get(interface)
            .map(|vec| vec.iter().map(String::as_str))
            .unwrap()
    }

    pub fn functions(&self) -> impl Iterator<Item = &Arc<FunctionFqn<'static>>> {
        self.functions_to_metadata.keys()
    }
}

impl Debug for Activity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Activities");
        s.field("interfaces_functions", &self.ifc_fqns_to_function_names);
        s.field("functions_to_metadata", &self.functions_to_metadata);
        s.field("wasm_path", &self.wasm_path);
        s.finish()
    }
}
