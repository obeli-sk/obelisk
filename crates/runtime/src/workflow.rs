use crate::activity::Activities;
use crate::event_history::{
    CurrentEventHistory, EventHistory, EventWrapper, HostFunctionError, HostImports,
    SupportedFunctionResult, WasmActivity,
};
use crate::wasm_tools::{exported_interfaces, functions_to_metadata};
use crate::{FunctionFqn, FunctionMetadata};
use anyhow::{anyhow, Context};
use std::collections::HashMap;
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
    #[error("interrupted")]
    NewEventPersisted,
    #[error("handling event failed")]
    HandlingEventFailed(anyhow::Error),
    #[error("unknown error: {0:?}")]
    UnknownError(anyhow::Error),
}

pub struct Workflow {
    wasm_path: String,
    instance_pre: InstancePre<HostImports>,
    activities: Arc<Activities>,
    functions_to_metadata: HashMap<FunctionFqn<'static>, FunctionMetadata>,
}

impl Workflow {
    pub async fn new(
        wasm_path: String,
        activities: Arc<Activities>,
    ) -> Result<Self, anyhow::Error> {
        info!("workflow::new {wasm_path}");
        let wasm =
            std::fs::read(&wasm_path).with_context(|| format!("cannot open `{wasm_path}`"))?;
        let instance_pre = {
            let mut linker = Linker::new(&ENGINE);
            let component = Component::from_binary(&ENGINE, &wasm)?;
            // Add workflow host activities
            HostImports::add_to_linker(&mut linker)?;
            // Add wasm activities
            for interface in activities.interfaces() {
                let mut linker_instance = linker.instance(interface)?;
                for function_name in activities.functions(interface) {
                    let fqn = Arc::new(FunctionFqn::new_owned(
                        interface.to_string(),
                        function_name.to_string(),
                    ));
                    let fqn_inner = fqn.clone();
                    linker_instance
                        .func_new(
                            &component,
                            &fqn.function_name,
                            move |mut store_ctx: wasmtime::StoreContextMut<'_, HostImports>,
                                  params: &[Val],
                                  results: &mut [Val]| {
                                let wasm_activity = WasmActivity {
                                    fqn: fqn_inner.clone(),
                                    params: Vec::from(params),
                                };
                                let store = store_ctx.data_mut();
                                let replay_result = store
                                    .current_event_history
                                    .handle_or_interrupt_wasm_activity(wasm_activity)?;
                                assert_eq!(
                                    results.len(),
                                    replay_result.len(),
                                    "unexpected results length"
                                );
                                for (idx, item) in replay_result.into_iter().enumerate() {
                                    results[idx] = item;
                                }
                                Ok(())
                            },
                        )
                        .with_context(|| format!(" `{fqn}`"))?;
                }
            }
            linker.instantiate_pre(&component)?
        };
        let functions_to_metadata = decode_wasm_function_metadata(&wasm)
            .with_context(|| format!("error parsing `{wasm_path}`"))?;
        Ok(Self {
            instance_pre,
            activities,
            wasm_path,
            functions_to_metadata,
        })
    }

    pub fn function_metadata<'a>(&'a self, fqn: &FunctionFqn<'a>) -> Option<&'a FunctionMetadata> {
        self.functions_to_metadata.get(fqn)
    }

    pub async fn execute_all(
        &self,
        event_history: &mut EventHistory,
        fqn: &FunctionFqn<'_>,
        params: &[Val],
    ) -> wasmtime::Result<SupportedFunctionResult> {
        info!("workflow.execute_all `{fqn}`");

        let results_len = self
            .functions_to_metadata
            .get(fqn)
            .ok_or_else(|| {
                anyhow!(
                    "function {fqn} not found in {wasm_path}",
                    wasm_path = self.wasm_path
                )
            })?
            .results_len;
        trace!("workflow.execute_all `{fqn}`({params:?}) -> results_len:{results_len}");
        loop {
            let res = self
                .execute_persist_event(event_history, fqn, params, results_len)
                .await;
            trace!("workflow.execute_all `{fqn}` -> {res:?}");
            match res {
                Ok(output) => return Ok(output), // TODO Persist result to the history
                Err(ExecutionError::NewEventPersisted) => {} // loop again to get to the next step
                Err(ExecutionError::HandlingEventFailed(reason)) => {
                    // TODO: retry
                    return Err(reason);
                }
                Err(ExecutionError::NonDeterminismDetected(reason)) => {
                    panic!(
                        "non determinism detected: {reason} while executing `{wasm_path}`",
                        wasm_path = self.wasm_path
                    )
                }
                Err(ExecutionError::UnknownError(err)) => panic!(
                    "unknown error: {err:?} while executing `{wasm_path}`",
                    wasm_path = self.wasm_path
                ),
            }
        }
    }

    // Execute the workflow until it is finished or interrupted.
    async fn execute_persist_event(
        &self,
        event_history: &mut EventHistory,
        fqn: &FunctionFqn<'_>,
        params: &[Val],
        results_len: usize,
    ) -> Result<SupportedFunctionResult, ExecutionError> {
        let mut store = Store::new(
            &ENGINE,
            HostImports {
                current_event_history: CurrentEventHistory::new(mem::take(event_history)),
            },
        );
        // try
        let res: Result<SupportedFunctionResult, ExecutionError> = {
            match self.execute(&mut store, fqn, params, results_len).await {
                Ok(res) => Ok(res),
                Err(err) => Err(
                    match err
                        .source()
                        .and_then(|source| source.downcast_ref::<HostFunctionError>())
                    {
                        Some(HostFunctionError::NonDeterminismDetected(reason)) => {
                            ExecutionError::NonDeterminismDetected(reason.clone())
                        }
                        Some(HostFunctionError::Interrupt(_)) => {
                            let event_wrapper = EventWrapper::new_from_err(err);
                            // Persist and execute the event
                            store
                                .data_mut()
                                .current_event_history
                                .persist_start(&event_wrapper);
                            let event = event_wrapper.as_ref();
                            let res = event
                                .handle(self.activities.clone())
                                .await
                                .map_err(ExecutionError::HandlingEventFailed)?;
                            store
                                .data_mut()
                                .current_event_history
                                .persist_end(event_wrapper, res.clone());
                            ExecutionError::NewEventPersisted
                        }
                        None => ExecutionError::UnknownError(err),
                    },
                ),
            }
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
        fqn: &FunctionFqn<'_>,
        params: &[Val],
        results_len: usize,
    ) -> Result<SupportedFunctionResult, anyhow::Error> {
        debug!("execute `{fqn}`");
        trace!("execute `{fqn}`({params:?})");
        let instance = self.instance_pre.instantiate_async(&mut store).await?;
        let func = {
            let mut store = store.as_context_mut();
            let mut exports = instance.exports(&mut store);
            let mut exports_instance = exports.root();
            let mut exports_instance = exports_instance
                .instance(&fqn.ifc_fqn)
                .ok_or_else(|| anyhow!("cannot find exported interface: `{}`", fqn.ifc_fqn))?;
            exports_instance
                .func(&fqn.function_name)
                .ok_or(anyhow::anyhow!("function `{fqn}` not found"))?
        };
        // call func
        let mut results = Vec::from_iter(std::iter::repeat(Val::Bool(false)).take(results_len));
        func.call_async(&mut store, params, &mut results).await?;
        func.post_return_async(&mut store).await?;
        let results = SupportedFunctionResult::new(results);
        trace!("execution result `{fqn}` -> {results:?}");
        Ok(results)
    }
}

fn decode_wasm_function_metadata(
    wasm: &[u8],
) -> Result<HashMap<FunctionFqn<'static>, FunctionMetadata>, anyhow::Error> {
    let decoded = wit_component::decode(wasm)?;
    let exported_interfaces = exported_interfaces(&decoded)?;
    Ok(functions_to_metadata(exported_interfaces)?)
}
