use crate::database::ActivityQueueSender;
use crate::error::{ExecutionError, HostFunctionError, WorkflowFailed};
use crate::event_history::{CurrentEventHistory, Event, EventHistory};
use crate::host_activity::{HostImports, HOST_ACTIVITY_IFC};
use crate::wasm_tools::{self, functions_to_metadata, is_limit_reached};
use crate::workflow_id::WorkflowId;
use crate::SupportedFunctionResult;
use crate::{FunctionFqn, FunctionMetadata};
use anyhow::{anyhow, Context};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::{fmt::Debug, sync::Arc};
use strum::EnumString;
use tracing::{debug, debug_span, info, trace, Instrument};
use wasmtime::component::Val;
use wasmtime::AsContextMut;
use wasmtime::{
    self,
    component::{Component, InstancePre, Linker},
    Engine, Store,
};

#[derive(Debug, PartialEq, Eq, Clone, Copy, EnumString)]
pub enum AsyncActivityBehavior {
    // TODO rename to ActivityBehavior
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

pub const WORKFLOW_CONFIG_COLD: WorkflowConfig = WorkflowConfig {
    async_activity_behavior: AsyncActivityBehavior::Restart,
};
pub const WORKFLOW_CONFIG_HOT: WorkflowConfig = WorkflowConfig {
    async_activity_behavior: AsyncActivityBehavior::KeepWaiting,
};

#[derive(Debug)]
enum PartialExecutionResult {
    Ok(SupportedFunctionResult),
    NewEventPersisted,
}

pub struct Workflow {
    engine: Arc<Engine>,
    pub(crate) wasm_path: String,
    instance_pre: InstancePre<HostImports>,
    pub(crate) functions_to_metadata: HashMap<FunctionFqn, FunctionMetadata>,
    async_activity_behavior: AsyncActivityBehavior,
}

lazy_static! {
    static ref HOST_ACTIVITY_IFC_STRING: String = HOST_ACTIVITY_IFC.to_string();
}

impl Workflow {
    pub(crate) async fn new_with_config(
        wasm_path: String,
        activities: impl Iterator<Item = (&Arc<String>, &Vec<Arc<String>>)>,
        config: &WorkflowConfig,
        engine: Arc<Engine>,
    ) -> Result<Self, anyhow::Error> {
        let wasm =
            std::fs::read(&wasm_path).with_context(|| format!("cannot open \"{wasm_path}\""))?;
        let (resolve, world_id) = wasm_tools::decode(&wasm)?;
        let exp_fns_to_metadata =
            functions_to_metadata(wasm_tools::exported_ifc_fns(&resolve, &world_id)?)
                .with_context(|| {
                    format!("error parsing exported interfaces from \"{wasm_path}\"")
                })?;
        debug!(
            "Decoded exported functions {:?}",
            exp_fns_to_metadata.keys()
        );
        trace!("Decoded exported functions {exp_fns_to_metadata:#?}");
        let imp_ifcs_to_fn_names = {
            let imp_fns_to_metadata =
                functions_to_metadata(wasm_tools::imported_ifc_fns(&resolve, &world_id)?)
                    .with_context(|| {
                        format!("error parsing imported interfaces from \"{wasm_path}\"")
                    })?;
            let mut imp_ifcs_to_fn_names =
                wasm_tools::group_by_ifc_to_fn_names(imp_fns_to_metadata.keys());
            imp_ifcs_to_fn_names.remove(HOST_ACTIVITY_IFC_STRING.deref());
            imp_ifcs_to_fn_names
        };

        let instance_pre = {
            let mut linker = Linker::new(&engine);
            let component = Component::from_binary(&engine, &wasm)?;
            // Add host activities
            HostImports::add_to_linker(&mut linker)?;
            // Add wasm activities
            for (ifc_fqn, functions) in &imp_ifcs_to_fn_names {
                trace!("Adding imported interface {ifc_fqn} to the linker");
                let mut linker_instance = linker.instance(ifc_fqn)?;
                for function_name in functions {
                    trace!("Adding imported function {function_name} to the linker");
                    let fqn_inner = FunctionFqn {
                        ifc_fqn: ifc_fqn.clone(),
                        function_name: function_name.clone(),
                    };
                    let res = linker_instance.func_new_async(
                        &component,
                        function_name,
                        move |mut store_ctx: wasmtime::StoreContextMut<'_, HostImports>,
                              params: &[Val],
                              results: &mut [Val]| {
                            let workflow_id =
                                store_ctx.data().current_event_history.workflow_id.clone();
                            let fqn_inner = fqn_inner.clone();
                            // TODO: is it activity or a workflow
                            Box::new(async move {
                                let event = Event::new_from_wasm_activity(
                                    workflow_id,
                                    fqn_inner,
                                    Arc::new(Vec::from(params)),
                                );
                                let store = store_ctx.data_mut();
                                let activity_result = store
                                    .current_event_history
                                    .replay_enqueue_interrupt(event)
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
                    );
                    if let Err(err) = res {
                        if err.to_string() == format!("import `{function_name}` not found") {
                            debug!("Skipping activity function {function_name}");
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
            linker.instantiate_pre(&component)?
        };

        Ok(Self {
            wasm_path,
            instance_pre,
            functions_to_metadata: exp_fns_to_metadata,
            async_activity_behavior: config.async_activity_behavior,
            engine,
        })
    }

    pub fn function_metadata<'a>(&'a self, fqn: &FunctionFqn) -> Option<&'a FunctionMetadata> {
        self.functions_to_metadata.get(fqn)
    }

    pub fn functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.functions_to_metadata.keys()
    }

    pub fn contains(&self, fqn: &FunctionFqn) -> bool {
        self.function_metadata(fqn).is_some()
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn execute_all(
        &self,
        workflow_id: &WorkflowId,
        activity_queue_sender: &ActivityQueueSender,
        event_history: &mut EventHistory,
        workflow_fqn: &FunctionFqn,
        params: &[Val],
    ) -> Result<SupportedFunctionResult, ExecutionError> {
        info!("[{workflow_id}] workflow.execute_all {workflow_fqn}");

        let results_len = self
            .functions_to_metadata
            .get(workflow_fqn)
            .ok_or_else(|| ExecutionError::NotFound {
                workflow_id: workflow_id.clone(),
                fqn: workflow_fqn.clone(),
            })?
            .results_len;
        trace!(
            "[{workflow_id}] workflow.execute_all {workflow_fqn}({params:?}) -> results_len:{results_len}"
        );

        for run_id in 0.. {
            let res = self
                .execute_in_new_store(
                    workflow_id,
                    run_id,
                    activity_queue_sender,
                    event_history,
                    workflow_fqn,
                    params,
                    results_len,
                )
                .await;
            trace!("[{workflow_id},{run_id}] workflow.execute_all {workflow_fqn} -> {res:?}");
            match res {
                Ok(PartialExecutionResult::Ok(output)) => return Ok(output), // TODO Persist the workflow result to the history
                Ok(PartialExecutionResult::NewEventPersisted) => {} // loop again to get to the next step
                Err(err) => {
                    return Err(err.into_execution_error(
                        workflow_fqn.clone(),
                        workflow_id.clone(),
                        run_id,
                    ))
                }
            }
        }
        unreachable!()
    }

    // Execute the workflow until it is finished or interrupted.
    #[allow(clippy::too_many_arguments)]
    async fn execute_in_new_store(
        &self,
        workflow_id: &WorkflowId,
        run_id: u64,
        activity_queue_sender: &ActivityQueueSender,
        event_history: &mut EventHistory,
        workflow_fqn: &FunctionFqn,
        params: &[Val],
        results_len: usize,
    ) -> Result<PartialExecutionResult, WorkflowFailed> {
        let mut store = Store::new(
            &self.engine,
            HostImports {
                current_event_history: CurrentEventHistory::new(
                    workflow_id.clone(),
                    run_id,
                    mem::take(event_history),
                    activity_queue_sender.clone(),
                    self.async_activity_behavior,
                ),
            },
        );
        // try
        let mut res = self
            .execute_one_step_interpret_errors(
                workflow_id,
                run_id,
                activity_queue_sender,
                &mut store,
                workflow_fqn,
                params,
                results_len,
            )
            .await;
        // finally
        mem::swap(
            event_history,
            &mut store.data_mut().current_event_history.event_history,
        );
        if res.is_ok() && !store.data_mut().current_event_history.replay_is_drained() {
            res = Err(WorkflowFailed::NonDeterminismDetected(
                "replay log was not drained".to_string(),
            ))
        }
        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_one_step_interpret_errors(
        &self,
        workflow_id: &WorkflowId,
        run_id: u64,
        activity_queue_sender: &ActivityQueueSender,
        store: &mut Store<HostImports>,
        workflow_fqn: &FunctionFqn,
        params: &[Val],
        results_len: usize,
    ) -> Result<PartialExecutionResult, WorkflowFailed> {
        match self
            .execute_one_step(
                workflow_id,
                run_id,
                store,
                workflow_fqn,
                params,
                results_len,
            )
            .await
        {
            Ok(ok) => Ok(PartialExecutionResult::Ok(ok)),
            Err(err) => {
                if let Some(err) = self
                    .interpret_errors(activity_queue_sender, store, err)
                    .await
                {
                    Err(err)
                } else {
                    Ok(PartialExecutionResult::NewEventPersisted)
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn interpret_errors(
        &self,
        activity_queue_sender: &ActivityQueueSender,
        store: &mut Store<HostImports>,
        err: anyhow::Error,
    ) -> Option<WorkflowFailed> {
        match err
            .source()
            .and_then(|source| source.downcast_ref::<HostFunctionError>())
        {
            Some(HostFunctionError::NonDeterminismDetected(reason)) => {
                Some(WorkflowFailed::NonDeterminismDetected(reason.clone()))
            }
            Some(HostFunctionError::Interrupt { request }) => {
                trace!("Interrupted at {err:?}");
                let res = activity_queue_sender
                    .push(
                        request.clone(),
                        &mut store.data_mut().current_event_history.event_history,
                    )
                    .await;
                match res {
                    Ok(_) => {
                        None // No error, workflow made progress.
                    }
                    Err(err) => Some(WorkflowFailed::ActivityFailed(err)),
                }
            }
            Some(HostFunctionError::ActivityFailed(activity_failed)) => {
                Some(WorkflowFailed::ActivityFailed(activity_failed.clone()))
            }
            None => {
                let reason = err.to_string();
                if is_limit_reached(&reason) {
                    Some(WorkflowFailed::LimitReached(reason))
                } else {
                    Some(WorkflowFailed::UnknownError(err))
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn execute_one_step(
        &self,
        workflow_id: &WorkflowId,
        run_id: u64,
        mut store: &mut Store<HostImports>,
        fqn: &FunctionFqn,
        params: &[Val],
        results_len: usize,
    ) -> Result<SupportedFunctionResult, anyhow::Error> {
        debug!("[{workflow_id},{run_id}] execute {fqn}");
        trace!("[{workflow_id},{run_id}] execute {fqn}({params:?})");
        let instance = self.instance_pre.instantiate_async(&mut store).await?;
        let func = {
            let mut store = store.as_context_mut();
            let mut exports = instance.exports(&mut store);
            let mut exports_instance = exports.root();
            let mut exports_instance =
                exports_instance.instance(&fqn.ifc_fqn).ok_or_else(|| {
                    anyhow!("cannot find exported interface: {fqn}", fqn = fqn.ifc_fqn)
                })?;
            exports_instance
                .func(&fqn.function_name)
                .ok_or(anyhow::anyhow!("function {fqn} not found"))?
        };
        // call func
        let mut results = Vec::from_iter(std::iter::repeat(Val::Bool(false)).take(results_len));
        func.call_async(&mut store, params, &mut results)
            .instrument(debug_span!("call_async"))
            .await?;

        let results = SupportedFunctionResult::new(results);
        func.post_return_async(&mut store)
            .instrument(debug_span!("post_return_async"))
            .await?;
        trace!("[{workflow_id},{run_id}] execution result {fqn} -> {results:?}");
        Ok(results)
    }
}

impl Debug for Workflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Workflow");
        s.field("functions_to_metadata", &self.functions_to_metadata);
        s.field("wasm_path", &self.wasm_path);
        s.finish()
    }
}
