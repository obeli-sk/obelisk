use crate::event_history::{
    CurrentEventHistory, Event, EventHistory, HostFunctionError, HostImports,
    SupportedFunctionResult,
};
use crate::queue::activity_queue::ActivityQueueSender;
use crate::wasm_tools::{exported_interfaces, functions_to_metadata};
use crate::{ActivityFailed, FunctionFqn, FunctionMetadata, Runtime};
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
        config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(wasmtime::PoolingAllocationConfig::default()));
        Engine::new(&config).unwrap()
    };
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AsyncActivityBehavior {
    Restart,     // Shut down the instance and replay all events when the activity finishes.
    KeepWaiting, // Keep the workflow instance running.
}

impl Default for AsyncActivityBehavior {
    fn default() -> Self {
        Self::Restart
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]

pub struct WorkflowConfig {
    pub async_activity_behavior: AsyncActivityBehavior,
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
    #[error(transparent)]
    ActivityFailed(ActivityFailed),
    #[error("unknown error: `{0:?}`")]
    UnknownError(anyhow::Error),
}

pub struct Workflow {
    #[allow(unused)] // avoid shutting down the activity queue task
    runtime: Arc<Runtime>,
    instance_pre: InstancePre<HostImports>,
    activity_queue_sender: ActivityQueueSender,
    functions_to_metadata: HashMap<FunctionFqn<'static>, FunctionMetadata>,
    async_activity_behavior: AsyncActivityBehavior,
}

impl Workflow {
    pub async fn new(wasm_path: String, runtime: Arc<Runtime>) -> Result<Self, anyhow::Error> {
        Self::new_with_config(wasm_path, runtime, &WorkflowConfig::default()).await
    }

    pub async fn new_with_config(
        wasm_path: String,
        runtime: Arc<Runtime>,
        config: &WorkflowConfig,
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
            for interface in runtime.activities.interfaces() {
                let mut linker_instance = linker.instance(interface)?;
                for function_name in runtime.activities.functions(interface) {
                    let fqn = Arc::new(FunctionFqn::new_owned(
                        interface.to_string(),
                        function_name.to_string(),
                    ));
                    let fqn_inner = fqn.clone();
                    linker_instance
                        .func_new_async(
                            &component,
                            &fqn.function_name,
                            move |mut store_ctx: wasmtime::StoreContextMut<'_, HostImports>,
                                  params: &[Val],
                                  results: &mut [Val]| {
                                let fqn_inner = fqn_inner.clone();
                                Box::new(async move {
                                    let event = Event::new_from_wasm_activity(
                                        fqn_inner,
                                        Arc::new(Vec::from(params)),
                                    );
                                    let store = store_ctx.data_mut();
                                    let activity_result = store
                                        .current_event_history
                                        .replay_handle_interrupt(event)
                                        .await?;
                                    assert_eq!(
                                        results.len(),
                                        activity_result.len(),
                                        "unexpected results length"
                                    );
                                    for (idx, item) in activity_result.into_iter().enumerate() {
                                        results[idx] = item;
                                    }
                                    Ok(())
                                })
                            },
                        )
                        .with_context(|| format!(" `{fqn}`"))?;
                }
            }
            linker.instantiate_pre(&component)?
        };
        let functions_to_metadata = decode_wasm_function_metadata(&wasm)
            .with_context(|| format!("error parsing `{wasm_path}`"))?;
        debug!("Loaded {:?}", functions_to_metadata.keys());
        trace!("Loaded {:?}", functions_to_metadata);

        let activity_queue_sender = runtime.activity_queue_writer();
        Ok(Self {
            instance_pre,
            runtime,
            activity_queue_sender,
            functions_to_metadata,
            async_activity_behavior: config.async_activity_behavior,
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
                .execute_in_new_store(event_history, fqn, params, results_len)
                .await;
            trace!("workflow.execute_all `{fqn}` -> {res:?}");
            match res {
                Ok(output) => return Ok(output), // TODO Persist the workflow result to the history
                Err(ExecutionInterrupt::NewEventPersisted) => {} // loop again to get to the next step
                Err(ExecutionInterrupt::ActivityFailed(ActivityFailed {
                    activity_fqn,
                    reason,
                })) => {
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
    async fn execute_in_new_store(
        &self,
        event_history: &mut EventHistory,
        workflow_fqn: &FunctionFqn<'_>,
        params: &[Val],
        results_len: usize,
    ) -> Result<SupportedFunctionResult, ExecutionInterrupt> {
        let mut store = Store::new(
            &ENGINE,
            HostImports {
                current_event_history: CurrentEventHistory::new(
                    mem::take(event_history),
                    self.activity_queue_sender.clone(),
                    self.async_activity_behavior,
                ),
            },
        );
        // try
        let res = self
            .execute_one_step_interpret_errors(&mut store, workflow_fqn, params, results_len)
            .await;
        // finally
        mem::swap(
            event_history,
            &mut store.data_mut().current_event_history.event_history,
        );
        res
    }

    async fn execute_one_step_interpret_errors(
        &self,
        store: &mut Store<HostImports>,
        workflow_fqn: &FunctionFqn<'_>,
        params: &[Val],
        results_len: usize,
    ) -> Result<SupportedFunctionResult, ExecutionInterrupt> {
        match self
            .execute_one_step(store, workflow_fqn, params, results_len)
            .await
        {
            Ok(ok) => Ok(ok),
            Err(err) => Err(self.interpret_errors(store, err).await),
        }
    }

    async fn interpret_errors(
        &self,
        store: &mut Store<HostImports>,
        err: anyhow::Error,
    ) -> ExecutionInterrupt {
        match err
            .source()
            .and_then(|source| source.downcast_ref::<HostFunctionError>())
        {
            Some(HostFunctionError::NonDeterminismDetected(reason)) => {
                ExecutionInterrupt::NonDeterminismDetected(reason.clone())
            }
            Some(HostFunctionError::Interrupt { request }) => {
                // Persist and execute the event
                store
                    .data_mut()
                    .current_event_history
                    .persist_start(request);
                let res =
                    Event::handle_activity_async(request.clone(), &self.activity_queue_sender)
                        .await;
                match res {
                    Ok(_) => {
                        store
                            .data_mut()
                            .current_event_history
                            .persist_end(request.clone(), res);
                        ExecutionInterrupt::NewEventPersisted
                    }
                    Err(err) => {
                        store
                            .data_mut()
                            .current_event_history
                            .persist_end(request.clone(), Err(err.clone()));
                        ExecutionInterrupt::ActivityFailed(err)
                    }
                }
            }
            Some(HostFunctionError::ActivityFailed(activity_failed)) => {
                // TODO: persist activity failure
                ExecutionInterrupt::ActivityFailed(activity_failed.clone())
            }
            None => ExecutionInterrupt::UnknownError(err),
        }
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
