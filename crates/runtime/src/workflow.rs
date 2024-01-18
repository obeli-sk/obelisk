use crate::activity::Activity;
use crate::database::ActivityQueueSender;
use crate::event_history::{
    CurrentEventHistory, Event, EventHistory, HostFunctionError, HostImports,
};
use crate::wasm_tools::{exported_interfaces, functions_to_metadata, is_limit_reached};
use crate::workflow_id::WorkflowId;
use crate::SupportedFunctionResult;
use crate::{ActivityFailed, FunctionFqn, FunctionMetadata};
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
    Engine, Store,
};

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

pub const WORKFLOW_CONFIG_COLD: WorkflowConfig = WorkflowConfig {
    async_activity_behavior: AsyncActivityBehavior::Restart,
};
pub const WORKFLOW_CONFIG_HOT: WorkflowConfig = WorkflowConfig {
    async_activity_behavior: AsyncActivityBehavior::KeepWaiting,
};

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("[{workflow_id}] workflow `{fqn}` not found")]
    NotFound {
        workflow_id: WorkflowId,
        fqn: FunctionFqn,
    },
    #[error("[{workflow_id},{run_id}] workflow `{workflow_fqn}` encountered non deterministic execution, reason: `{reason}`")]
    NonDeterminismDetected {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] activity failed, workflow `{workflow_fqn}`, activity `{activity_fqn}`, reason: `{reason}`")]
    ActivityFailed {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        activity_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] workflow limit reached, workflow `{workflow_fqn}`, reason: `{reason}`")]
    LimitReached {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] activity limit reached, workflow `{workflow_fqn}`, activity `{activity_fqn}`, reason: `{reason}`")]
    ActivityLimitReached {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        activity_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id},{run_id}] activity not found, workflow `{workflow_fqn}`, activity `{activity_fqn}`")]
    ActivityNotFound {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        activity_fqn: FunctionFqn,
    },
    #[error("[{workflow_id}] workflow `{workflow_fqn}` cannot be scheduled: `{reason}`")]
    SchedulingError {
        workflow_id: WorkflowId,
        workflow_fqn: FunctionFqn,
        reason: String,
    },
    #[error(
        "[{workflow_id},{run_id}] `{workflow_fqn}` encountered an unknown error: `{source:?}`"
    )]
    UnknownError {
        workflow_id: WorkflowId,
        run_id: u64,
        workflow_fqn: FunctionFqn,
        // #[backtrace]
        source: anyhow::Error,
    },
}

#[derive(Debug)]
enum PartialExecutionResult {
    Ok(SupportedFunctionResult),
    NewEventPersisted,
}

#[derive(thiserror::Error, Debug)]
enum WorkflowFailed {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(String),
    #[error(transparent)]
    ActivityFailed(ActivityFailed),
    #[error("limit reached: `{0}`")]
    LimitReached(String),
    #[error("unknown error: `{0:?}`")]
    UnknownError(anyhow::Error),
}

impl WorkflowFailed {
    fn into_execution_error(
        self,
        workflow_fqn: FunctionFqn,
        workflow_id: WorkflowId,
        run_id: u64,
    ) -> ExecutionError {
        match self {
            WorkflowFailed::ActivityFailed(ActivityFailed::Other {
                workflow_id: id,
                activity_fqn,
                reason,
            }) => {
                assert_eq!(id, workflow_id);
                ExecutionError::ActivityFailed {
                    workflow_id,
                    run_id,
                    workflow_fqn,
                    activity_fqn,
                    reason,
                }
            }
            WorkflowFailed::ActivityFailed(ActivityFailed::LimitReached {
                workflow_id: id,
                activity_fqn,
                reason,
            }) => {
                assert_eq!(id, workflow_id);
                ExecutionError::ActivityLimitReached {
                    workflow_id,
                    run_id,
                    workflow_fqn,
                    activity_fqn,
                    reason,
                }
            }
            WorkflowFailed::ActivityFailed(ActivityFailed::NotFound {
                workflow_id: id,
                activity_fqn,
            }) => {
                assert_eq!(id, workflow_id);
                ExecutionError::ActivityNotFound {
                    workflow_id,
                    run_id,
                    workflow_fqn,
                    activity_fqn,
                }
            }
            WorkflowFailed::NonDeterminismDetected(reason) => {
                ExecutionError::NonDeterminismDetected {
                    workflow_id,
                    run_id,
                    workflow_fqn,
                    reason,
                }
            }
            WorkflowFailed::LimitReached(reason) => ExecutionError::LimitReached {
                workflow_id,
                run_id,
                workflow_fqn,
                reason,
            },
            WorkflowFailed::UnknownError(source) => ExecutionError::UnknownError {
                workflow_id,
                run_id,
                workflow_fqn,
                source,
            },
        }
    }
}

pub struct Workflow {
    engine: Arc<Engine>,
    pub(crate) wasm_path: String,
    instance_pre: InstancePre<HostImports>,
    pub(crate) functions_to_metadata: HashMap<FunctionFqn, FunctionMetadata>,
    async_activity_behavior: AsyncActivityBehavior,
}

impl Workflow {
    pub(crate) async fn new_with_config(
        wasm_path: String,
        activities: &HashMap<FunctionFqn, Arc<Activity>>,
        config: &WorkflowConfig,
        engine: Arc<Engine>,
    ) -> Result<Self, anyhow::Error> {
        let wasm =
            std::fs::read(&wasm_path).with_context(|| format!("cannot open \"{wasm_path}\""))?;
        let functions_to_metadata = decode_wasm_function_metadata(&wasm)
            .with_context(|| format!("error parsing \"{wasm_path}\""))?;
        debug!("Decoded functions {:?}", functions_to_metadata.keys());
        trace!("Decoded functions {functions_to_metadata:#?}");
        let instance_pre = {
            let mut linker = Linker::new(&engine);
            let component = Component::from_binary(&engine, &wasm)?;
            // Add workflow host activities
            HostImports::add_to_linker(&mut linker)?;
            // Add wasm activities
            for fqn in activities.keys() {
                trace!("Adding function `{fqn}` to the linker");
                let mut linker_instance = linker.instance(&fqn.ifc_fqn)?;
                let fqn_inner = fqn.clone();
                let res = linker_instance.func_new_async(
                    &component,
                    &fqn.function_name,
                    move |mut store_ctx: wasmtime::StoreContextMut<'_, HostImports>,
                          params: &[Val],
                          results: &mut [Val]| {
                        let workflow_id =
                            store_ctx.data().current_event_history.workflow_id.clone();
                        let fqn_inner = fqn_inner.clone();
                        Box::new(async move {
                            let event = Event::new_from_wasm_activity(
                                workflow_id,
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
                );
                if let Err(err) = res {
                    if err.to_string()
                        == format!("import `{ifc_fqn}` not found", ifc_fqn = fqn.ifc_fqn)
                    {
                        debug!("Skipping function `{fqn}` which is not imported");
                    } else {
                        return Err(err);
                    }
                }
            }
            linker.instantiate_pre(&component)?
        };

        Ok(Self {
            wasm_path,
            instance_pre,
            functions_to_metadata,
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

    pub(crate) async fn execute_all(
        &self,
        workflow_id: &WorkflowId,
        activity_queue_sender: &ActivityQueueSender,
        event_history: &mut EventHistory,
        workflow_fqn: &FunctionFqn,
        params: &[Val],
    ) -> Result<SupportedFunctionResult, ExecutionError> {
        info!("[{workflow_id}] workflow.execute_all `{workflow_fqn}`");

        let results_len = self
            .functions_to_metadata
            .get(workflow_fqn)
            .ok_or_else(|| ExecutionError::NotFound {
                workflow_id: workflow_id.clone(),
                fqn: workflow_fqn.clone(),
            })?
            .results_len;
        trace!(
            "[{workflow_id}] workflow.execute_all `{workflow_fqn}`({params:?}) -> results_len:{results_len}"
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
            trace!("[{workflow_id},{run_id}] workflow.execute_all `{workflow_fqn}` -> {res:?}");
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
                // Persist and execute the event
                store
                    .data_mut()
                    .current_event_history
                    .persist_start(request)
                    .await;
                let res =
                    Event::handle_activity_async(request.clone(), activity_queue_sender).await;
                match res {
                    Ok(_) => {
                        store
                            .data_mut()
                            .current_event_history
                            .persist_end(request.clone(), res)
                            .await;
                        None // No error, workflow made progress.
                    }
                    Err(err) => {
                        store
                            .data_mut()
                            .current_event_history
                            .persist_end(request.clone(), Err(err.clone()))
                            .await;
                        Some(WorkflowFailed::ActivityFailed(err))
                    }
                }
            }
            Some(HostFunctionError::ActivityFailed(activity_failed)) => {
                // TODO: persist activity failure
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

    async fn execute_one_step(
        &self,
        workflow_id: &WorkflowId,
        run_id: u64,
        mut store: &mut Store<HostImports>,
        fqn: &FunctionFqn,
        params: &[Val],
        results_len: usize,
    ) -> Result<SupportedFunctionResult, anyhow::Error> {
        debug!("[{workflow_id},{run_id}] execute `{fqn}`");
        trace!("[{workflow_id},{run_id}] execute `{fqn}`({params:?})");
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
        let results = SupportedFunctionResult::new(results);
        func.post_return_async(&mut store).await?;
        trace!("[{workflow_id},{run_id}] execution result `{fqn}` -> {results:?}");
        Ok(results)
    }
}

fn decode_wasm_function_metadata(
    wasm: &[u8],
) -> Result<HashMap<FunctionFqn, FunctionMetadata>, anyhow::Error> {
    let decoded = wit_component::decode(wasm)?;
    let exported_interfaces = exported_interfaces(&decoded)?;
    Ok(functions_to_metadata(exported_interfaces)?)
}

impl Debug for Workflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Workflow");
        s.field("functions_to_metadata", &self.functions_to_metadata);
        s.field("wasm_path", &self.wasm_path);
        s.finish()
    }
}
