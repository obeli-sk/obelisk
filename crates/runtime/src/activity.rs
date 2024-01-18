use anyhow::Context;
use std::{collections::HashMap, fmt::Debug, ops::DerefMut, sync::Arc};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, trace};
use wasmtime::{component::Val, Engine};

use crate::{
    wasm_tools::{exported_interfaces, functions_to_metadata, is_limit_reached},
    workflow_id::WorkflowId,
    ActivityFailed, SupportedFunctionResult, {FunctionFqn, FunctionMetadata},
};

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
    pub(crate) workflow_id: WorkflowId,
    pub(crate) activity_fqn: FunctionFqn,
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

pub const ACTIVITY_CONFIG_HOT: ActivityConfig = ActivityConfig {
    preload: ActivityPreload::Instance,
};
pub const ACTIVITY_CONFIG_COLD: ActivityConfig = ActivityConfig {
    preload: ActivityPreload::Preinstance,
};

enum PreloadHolder {
    Preinstance,
    Instance(
        wasmtime::component::Instance,
        Mutex<wasmtime::Store<http::Ctx>>,
    ),
}

pub struct Activity {
    engine: Arc<Engine>,
    functions_to_metadata: HashMap<FunctionFqn, FunctionMetadata>,
    instance_pre: wasmtime::component::InstancePre<http::Ctx>,
    preload_holder: PreloadHolder,
    pub(crate) wasm_path: String,
}

impl Activity {
    pub async fn new_with_config(
        wasm_path: String,
        config: &ActivityConfig,
        engine: Arc<Engine>,
    ) -> Result<Self, anyhow::Error> {
        let wasm =
            std::fs::read(&wasm_path).with_context(|| format!("cannot open \"{wasm_path}\""))?;
        let decoded = wit_component::decode(&wasm)
            .with_context(|| format!("cannot decode \"{wasm_path}\""))?;
        let exported_interfaces = exported_interfaces(&decoded)
            .with_context(|| format!("error parsing \"{wasm_path}\""))?;
        let functions_to_metadata = functions_to_metadata(exported_interfaces)?;
        debug!("Decoded functions {:?}", functions_to_metadata.keys());
        trace!("Decoded functions {functions_to_metadata:#?}");
        let instance_pre: wasmtime::component::InstancePre<http::Ctx> = {
            let mut linker = wasmtime::component::Linker::new(&engine);
            wasmtime_wasi::preview2::command::add_to_linker(&mut linker)?;
            wasmtime_wasi_http::bindings::http::outgoing_handler::add_to_linker(
                &mut linker,
                |t| t,
            )?;
            wasmtime_wasi_http::bindings::http::types::add_to_linker(&mut linker, |t| t)?;
            // Compile the wasm component
            let component = wasmtime::component::Component::from_binary(&engine, &wasm)?;
            linker.instantiate_pre(&component)?
        };
        let preload_holder = match config.preload {
            ActivityPreload::Preinstance => PreloadHolder::Preinstance,
            ActivityPreload::Instance => {
                let mut store = http::store(&engine);
                let instance = instance_pre.instantiate_async(&mut store).await?;
                PreloadHolder::Instance(instance, Mutex::new(store))
            }
        };
        Ok(Self {
            engine,
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
        debug!(
            "[{workflow_id}] Running `{fqn}`",
            workflow_id = request.workflow_id,
            fqn = request.activity_fqn
        );
        let results_len = self
            .functions_to_metadata
            .get(&request.activity_fqn)
            .unwrap()
            .results_len;
        trace!(
            "[{workflow_id}] Running `{fqn}`({params:?}) -> results_len:{results_len}",
            workflow_id = request.workflow_id,
            fqn = request.activity_fqn,
            params = request.params
        );
        let mut store;
        let mut store_guard; // possibly uninitialized
        let instance_owned; // possibly uninitialized

        // Get existing or create new store and instance.
        let (instance, mut store) =
            if let Some((instance, store_guard2)) = Self::try_lock(&self.preload_holder) {
                store_guard = store_guard2;
                (instance, store_guard.deref_mut())
            } else {
                store = http::store(&self.engine);
                instance_owned = self
                    .instance_pre
                    .instantiate_async(&mut store)
                    .await
                    .map_err(|err| {
                        let reason = err.to_string();
                        if is_limit_reached(&reason) {
                            ActivityFailed::LimitReached {
                                workflow_id: request.workflow_id.clone(),
                                activity_fqn: request.activity_fqn.clone(),
                                reason,
                            }
                        } else {
                            ActivityFailed::Other {
                                workflow_id: request.workflow_id.clone(),
                                activity_fqn: request.activity_fqn.clone(),
                                reason: format!("wasm instantiation error: `{err}`"),
                            }
                        }
                    })?;
                (&instance_owned, &mut store)
            };

        let func = {
            let mut store = &mut store;
            let mut exports = instance.exports(&mut store);
            let mut exports_instance = exports.root();
            let mut exports_instance = exports_instance
                .instance(&request.activity_fqn.ifc_fqn)
                .ok_or_else(|| ActivityFailed::Other {
                    workflow_id: request.workflow_id.clone(),
                    activity_fqn: request.activity_fqn.clone(),
                    reason: "cannot find exported interface".to_string(),
                })?;
            exports_instance
                .func(&request.activity_fqn.function_name)
                .ok_or(ActivityFailed::Other {
                    workflow_id: request.workflow_id.clone(),
                    activity_fqn: request.activity_fqn.clone(),
                    reason: "function not found".to_string(),
                })?
        };
        // call func
        let mut results = Vec::from_iter(std::iter::repeat(Val::Bool(false)).take(results_len));
        func.call_async(&mut store, &request.params, &mut results)
            .await
            .map_err(|err| ActivityFailed::Other {
                workflow_id: request.workflow_id.clone(),
                activity_fqn: request.activity_fqn.clone(),
                reason: format!("wasm function call error: `{err}`"),
            })?;
        let results = SupportedFunctionResult::new(results);
        func.post_return_async(&mut store)
            .await
            .map_err(|err| ActivityFailed::Other {
                workflow_id: request.workflow_id.clone(),
                activity_fqn: request.activity_fqn.clone(),
                reason: format!("wasm post function call error: `{err}`"),
            })?;
        trace!(
            "[{workflow_id}] Finished `{fqn}` -> {results:?}",
            workflow_id = request.workflow_id,
            fqn = request.activity_fqn
        );
        Ok(results)
    }

    pub fn functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.functions_to_metadata.keys()
    }
}

impl Debug for Activity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Activity");
        s.field("functions_to_metadata", &self.functions_to_metadata);
        s.field("wasm_path", &self.wasm_path);
        s.finish()
    }
}
