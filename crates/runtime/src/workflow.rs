use crate::activity::Activities;
use crate::event_history::{
    CurrentEventHistory, EventHistory, EventWrapper, HostFunctionError, HostImports, WasmActivity,
};
use anyhow::Context;
use std::mem;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, info, trace};
use wasmtime::component::Val;
use wasmtime::AsContextMut;
use wasmtime::{
    self,
    component::{Component, InstancePre, Linker},
    Config, Engine, Store,
};

lazy_static::lazy_static! {
    static ref ENGINE: Engine = {
        let mut config = Config::new();
        // TODO: limit execution with fuel
        config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        config.wasm_component_model(true);
        config.async_support(true);
        Engine::new(&config).unwrap()
    };
}

#[derive(thiserror::Error, Debug)]
enum ExecutionError {
    #[error("non deterministic execution: {0}")]
    NonDeterminismDetected(String),
    #[error("handle: {0:?}")]
    HandleInterrupt(EventWrapper),
    #[error("unknown error: {0:?}")]
    UnknownError(anyhow::Error),
}

pub struct Workflow {
    wasm_path: String,
    instance_pre: InstancePre<HostImports>,
    activities: Arc<Activities>,
}
impl Workflow {
    pub async fn new(
        wasm_path: String,
        activities: Arc<Activities>,
    ) -> Result<Self, anyhow::Error> {
        info!("workflow.new {wasm_path}");
        let wasm =
            std::fs::read(&wasm_path).with_context(|| format!("cannot open `{wasm_path}`"))?;
        let instance_pre = {
            let mut linker = Linker::new(&ENGINE);
            let component = Component::from_binary(&ENGINE, &wasm)?;
            // Add workflow host functions
            HostImports::add_to_linker(&mut linker)?;
            // add activities
            for interface in activities.interfaces() {
                let mut linker_instance = linker.instance(interface)?;
                for function_name in activities.functions(interface) {
                    let ifc_fqn = Arc::new(interface.to_string());
                    let ifc_fqn2 = ifc_fqn.clone();
                    let function_name = Arc::new(function_name.to_string());
                    let function_name2 = function_name.clone();
                    linker_instance
                        .func_new(
                            &component,
                            &function_name.clone(),
                            move |mut store_ctx: wasmtime::StoreContextMut<'_, HostImports>,
                                  params: &[Val],
                                  results: &mut [Val]| {
                                let wasm_activity = WasmActivity {
                                    ifc_fqn: ifc_fqn.clone(),
                                    function_name: function_name.clone(),
                                    params: Vec::from(params),
                                };
                                let store = store_ctx.data_mut();
                                let replay_result = store
                                    .current_event_history
                                    .handle_or_interrupt_wasm_activity(wasm_activity)?;
                                if let Some(replay_result) = replay_result {
                                    results[0] = replay_result;
                                }
                                Ok(())
                            },
                        )
                        .with_context(|| format!(" `{ifc_fqn2:?}`.`{function_name2}`"))?;
                }
            }

            linker.instantiate_pre(&component)?
        };
        Ok(Self {
            instance_pre,
            activities,
            wasm_path,
        })
    }

    pub async fn execute_all(
        &self,
        event_history: &mut EventHistory,
        ifc_fqn: Option<&str>,
        function_name: &str,
        params: &[Val],
    ) -> wasmtime::Result<Val> {
        info!("workflow.execute_all `{ifc_fqn:?}`.`{function_name}`");
        trace!("workflow.execute_all `{ifc_fqn:?}`.`{function_name}`({params:?})");
        loop {
            let res = self
                .execute_translate_error(event_history, ifc_fqn, function_name, params)
                .await;
            trace!("workflow.execute_all `{ifc_fqn:?}`.`{function_name}` -> {res:?}");
            match res {
                Ok(output) => return Ok(output), // TODO Persist result to the history
                Err(ExecutionError::HandleInterrupt(event_wrapper)) => {
                    // Persist and execute the event
                    event_history.persist_start(&event_wrapper);
                    let event = event_wrapper.as_ref();
                    let res = event.handle(self.activities.clone()).await?;
                    event_history.persist_end(event_wrapper, res.clone());
                }
                Err(ExecutionError::NonDeterminismDetected(reason)) => {
                    panic!(
                        "Non determinism detected: {reason} while executing `{wasm_path}`",
                        wasm_path = self.wasm_path
                    )
                }
                Err(ExecutionError::UnknownError(err)) => panic!(
                    "Unknown error: {err:?} while executing `{wasm_path}`",
                    wasm_path = self.wasm_path
                ),
            }
        }
    }

    // Execute the workflow until it is finished or interrupted.
    async fn execute_translate_error(
        &self,
        event_history: &mut EventHistory,
        ifc_fqn: Option<&str>,
        function_name: &str,
        params: &[Val],
    ) -> Result<Val, ExecutionError> {
        let mut store = Store::new(
            &ENGINE,
            HostImports {
                current_event_history: CurrentEventHistory::new(mem::take(event_history)),
            },
        );
        // try
        let res: Result<Val, ExecutionError> = {
            self.execute(&mut store, ifc_fqn, function_name, params)
                .await
                .map_err(|err| {
                    match err
                        .source()
                        .and_then(|source| source.downcast_ref::<HostFunctionError>())
                    {
                        Some(HostFunctionError::NonDeterminismDetected(reason)) => {
                            ExecutionError::NonDeterminismDetected(reason.clone())
                        }
                        Some(HostFunctionError::Interrupt(_)) => {
                            ExecutionError::HandleInterrupt(EventWrapper::new_from_err(err))
                        }
                        None => ExecutionError::UnknownError(err),
                    }
                })
        };
        // finally
        mem::swap(
            event_history,
            &mut store.data_mut().current_event_history.event_history,
        );
        res
    }

    async fn execute(
        &self,
        mut store: &mut Store<HostImports>,
        ifc_fqn: Option<&str>,
        function_name: &str,
        params: &[Val],
    ) -> wasmtime::Result<Val> {
        debug!("`{ifc_fqn:?}`.`{function_name}`");
        trace!("`{ifc_fqn:?}`.`{function_name}`({params:?})");
        let instance = self.instance_pre.instantiate_async(&mut store).await?;
        let func = {
            let mut store = store.as_context_mut();
            let mut exports = instance.exports(&mut store);
            let mut exports_instance = exports.root();
            let mut exports_instance = if let Some(ifc_fqn) = ifc_fqn {
                exports_instance.instance(ifc_fqn).unwrap_or_else(|| {
                    panic!(
                        "cannot find exported interface: `{ifc_fqn}` in `{wasm_path}`",
                        wasm_path = self.wasm_path
                    )
                })
            } else {
                exports_instance
            };
            exports_instance.func(function_name).ok_or(anyhow::anyhow!(
                "function `{ifc_fqn:?}`.`{function_name}` not found"
            ))?
        };
        // call func
        let mut results = vec![Val::Bool(false)];
        func.call_async(&mut store, params, &mut results).await?;
        func.post_return_async(&mut store).await?;
        trace!("`{ifc_fqn:?}`.`{function_name}` -> {results:?}");
        assert_eq!(
            results.len(),
            1,
            "only one result supported, got {results:?}"
        );
        Ok(results.pop().unwrap())
    }
}
