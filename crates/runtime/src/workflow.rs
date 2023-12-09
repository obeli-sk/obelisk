use crate::activity::Activities;
use crate::event_history::{
    CurrentEventHistory, Event, EventHistory, HostFunctionError, HostImports,
    SupportedFunctionResult,
};
use crate::wasm_tools::{exported_interfaces, functions_to_metadata};
use crate::{FunctionFqn, FunctionMetadata};
use anyhow::{anyhow, Context};
use std::collections::HashMap;
use std::mem;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, error, info, trace};
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
pub enum ExecutionError {
    #[error("workflow `{0}` not found")]
    NotFound(FunctionFqn<'static>),
    #[error("workflow `{0}` encountered non deterministic execution, reason: `{1}`")]
    NonDeterminismDetected(FunctionFqn<'static>, String),
    #[error("workflow `{workflow_fqn}`, activity `{activity_fqn}`  failed: `{reason}`")]
    ActivityFailed {
        workflow_fqn: FunctionFqn<'static>,
        activity_fqn: Arc<FunctionFqn<'static>>,
        reason: String,
    },
    #[error("workflow `{0}` encountered an unknown error: `{1:?}`")]
    UnknownError(FunctionFqn<'static>, anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
enum ExecutionInterrupt {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(String),
    #[error("interrupted")]
    NewEventPersisted,
    #[error("activity `{activity_fqn}` failed: `{reason}`")]
    ActivityFailed {
        activity_fqn: Arc<FunctionFqn<'static>>,
        reason: String,
    },
    #[error("unknown error: `{0:?}`")]
    UnknownError(anyhow::Error),
}

pub struct Workflow {
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
                                let event = Event::new_from_wasm_activity(
                                    fqn_inner.clone(),
                                    Arc::new(Vec::from(params)),
                                );
                                let store = store_ctx.data_mut();
                                let replay_result =
                                    store.current_event_history.replay_handle_interrupt(event)?;
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
    ) -> Result<SupportedFunctionResult, ExecutionError> {
        info!("workflow.execute_all `{fqn}`");

        let results_len = self
            .functions_to_metadata
            .get(fqn)
            .ok_or_else(|| ExecutionError::NotFound(fqn.to_owned()))?
            .results_len;
        trace!("workflow.execute_all `{fqn}`({params:?}) -> results_len:{results_len}");
        loop {
            let res = self
                .execute_persist_event(event_history, fqn, params, results_len)
                .await;
            trace!("workflow.execute_all `{fqn}` -> {res:?}");
            match res {
                Ok(output) => return Ok(output), // TODO Persist result to the history
                Err(ExecutionInterrupt::NewEventPersisted) => {} // loop again to get to the next step
                Err(ExecutionInterrupt::ActivityFailed {
                    activity_fqn,
                    reason,
                }) => {
                    return Err(ExecutionError::ActivityFailed {
                        workflow_fqn: fqn.to_owned(),
                        activity_fqn,
                        reason,
                    })
                }
                Err(ExecutionInterrupt::NonDeterminismDetected(reason)) => {
                    return Err(ExecutionError::NonDeterminismDetected(
                        fqn.to_owned(),
                        reason,
                    ))
                }
                Err(ExecutionInterrupt::UnknownError(err)) => {
                    return Err(ExecutionError::UnknownError(fqn.to_owned(), err))
                }
            }
        }
    }

    // Execute the workflow until it is finished or interrupted.
    async fn execute_persist_event(
        &self,
        event_history: &mut EventHistory,
        workflow_fqn: &FunctionFqn<'_>,
        params: &[Val],
        results_len: usize,
    ) -> Result<SupportedFunctionResult, ExecutionInterrupt> {
        let mut store = Store::new(
            &ENGINE,
            HostImports {
                current_event_history: CurrentEventHistory::new(mem::take(event_history)),
            },
        );
        // try
        let res: Result<SupportedFunctionResult, ExecutionInterrupt> = {
            match self
                .execute_one_step(&mut store, workflow_fqn, params, results_len)
                .await
            {
                Ok(res) => Ok(res),
                Err(err) => Err(
                    match err
                        .source()
                        .and_then(|source| source.downcast_ref::<HostFunctionError>())
                    {
                        Some(HostFunctionError::NonDeterminismDetected(reason)) => {
                            ExecutionInterrupt::NonDeterminismDetected(reason.clone())
                        }
                        Some(HostFunctionError::Interrupt {
                            fqn,
                            params,
                            activity_async,
                        }) => {
                            let event = Event::new_from_interrupt(
                                fqn.clone(),
                                params.clone(),
                                activity_async.clone(),
                            );
                            // Persist and execute the event
                            store
                                .data_mut()
                                .current_event_history
                                .persist_start(&event.fqn, &event.params);
                            let res = activity_async
                                .handle(fqn, params, &self.activities)
                                .await
                                .map_err(|err| ExecutionInterrupt::ActivityFailed {
                                    activity_fqn: fqn.clone(),
                                    reason: err.to_string(),
                                })?;
                            store.data_mut().current_event_history.persist_end(
                                event.fqn.clone(),
                                event.params.clone(),
                                res,
                            );
                            ExecutionInterrupt::NewEventPersisted
                        }
                        Some(HostFunctionError::ActivityFailed {
                            activity_fqn: fqn,
                            source,
                        }) => {
                            error!("Activity `{fqn}` failed: {source:?}");
                            ExecutionInterrupt::ActivityFailed {
                                activity_fqn: fqn.clone(),
                                reason: err.to_string(),
                            }
                        }
                        None => ExecutionInterrupt::UnknownError(err),
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

    async fn execute_one_step(
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
