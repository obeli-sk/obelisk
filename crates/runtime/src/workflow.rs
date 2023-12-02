use crate::activity::Activities;
use crate::event_history::{
    CurrentEventHistory, EventHistory, EventWrapper, HostFunctionError, HostImports,
    SupportedFunctionResult, WasmActivity,
};
use anyhow::{anyhow, bail, Context};
use std::collections::HashMap;
use std::fmt::Display;
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
use wit_component::DecodedWasm;

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
    functions_to_results: HashMap<Fqn, FunctionMetadata>,
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
            // Add workflow host activities
            HostImports::add_to_linker(&mut linker)?;
            // Add wasm activities
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
                        .with_context(|| format!(" `{ifc_fqn2:?}`.`{function_name2}`"))?;
                }
            }

            linker.instantiate_pre(&component)?
        };
        let functions_to_results = decode_wasm_function_metadata(&wasm)
            .with_context(|| format!("error parsing `{wasm_path}`"))?;
        Ok(Self {
            instance_pre,
            activities,
            wasm_path,
            functions_to_results,
        })
    }

    pub async fn execute_all(
        &self,
        event_history: &mut EventHistory,
        ifc_fqn: Option<&str>,
        function_name: &str,
        params: &[Val],
    ) -> wasmtime::Result<SupportedFunctionResult> {
        info!("workflow.execute_all `{ifc_fqn:?}`.`{function_name}`");
        let fqn = Fqn {
            ifc_fqn: ifc_fqn.map(|ifc_fqn| ifc_fqn.to_string()),
            function_name: function_name.to_string(),
        };

        let results_len = self
            .functions_to_results
            .get(&fqn)
            .ok_or_else(|| {
                anyhow!(
                    "function {fqn} not found in {wasm_path}",
                    wasm_path = self.wasm_path
                )
            })?
            .results_len;
        trace!("workflow.execute_all `{ifc_fqn:?}`.`{function_name}`({params:?}) -> results_len:{results_len}");
        loop {
            let res = self
                .execute_persist_event(event_history, ifc_fqn, function_name, params, results_len)
                .await;
            trace!("workflow.execute_all `{ifc_fqn:?}`.`{function_name}` -> {res:?}");
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
        ifc_fqn: Option<&str>,
        function_name: &str,
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
            match self
                .execute(&mut store, ifc_fqn, function_name, params, results_len)
                .await
            {
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
        ifc_fqn: Option<&str>,
        function_name: &str,
        params: &[Val],
        results_len: usize,
    ) -> Result<SupportedFunctionResult, anyhow::Error> {
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
        let mut results = Vec::from_iter(std::iter::repeat(Val::Bool(false)).take(results_len));
        func.call_async(&mut store, params, &mut results).await?;
        func.post_return_async(&mut store).await?;
        let results = SupportedFunctionResult::new(results);
        trace!("`{ifc_fqn:?}`.`{function_name}` -> {results:?}");
        Ok(results)
    }
}

#[derive(Hash, Clone, PartialEq, Eq)]
pub(crate) struct Fqn {
    pub(crate) ifc_fqn: Option<String>,
    pub(crate) function_name: String,
}

impl Display for Fqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{prefix}{function_name}",
            prefix = if let Some(ifc_fqn) = &self.ifc_fqn {
                ifc_fqn
            } else {
                ""
            },
            function_name = self.function_name
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FunctionMetadata {
    pub(crate) results_len: usize,
}

pub(crate) fn decode_wasm_function_metadata(
    wasm: &[u8],
) -> Result<HashMap<Fqn, FunctionMetadata>, anyhow::Error> {
    let decoded = wit_component::decode(wasm)?;
    let (resolve, world_id) = match decoded {
        DecodedWasm::Component(resolve, world_id) => (resolve, world_id),
        _ => bail!("cannot parse component"),
    };
    let world = resolve
        .worlds
        .get(world_id)
        .ok_or_else(|| anyhow!("world must exist"))?;

    let exported_interfaces = world.exports.iter().filter_map(|(_, item)| match item {
        wit_parser::WorldItem::Interface(ifc) => {
            let ifc = resolve
                .interfaces
                .get(*ifc)
                .unwrap_or_else(|| panic!("interface must exist"));
            let package_name = ifc
                .package
                .and_then(|pkg| resolve.packages.get(pkg))
                .map(|p| &p.name)
                .unwrap_or_else(|| panic!("empty packages are not supported"));
            let ifc_name = ifc
                .name
                .clone()
                .unwrap_or_else(|| panic!("empty interfaces are not supported"));
            Some((package_name, ifc_name, &ifc.functions))
        }
        _ => None,
    });
    let mut functions_to_results = HashMap::new();
    for (package_name, ifc_name, functions) in exported_interfaces.into_iter() {
        let ifc_fqn = format!("{package_name}/{ifc_name}");
        for (function_name, function) in functions.into_iter() {
            let fqn = Fqn {
                ifc_fqn: Some(ifc_fqn.clone()),
                function_name: function_name.clone(),
            };
            functions_to_results.insert(
                fqn,
                FunctionMetadata {
                    results_len: function.results.len(),
                },
            );
        }
    }
    Ok(functions_to_results)
}
