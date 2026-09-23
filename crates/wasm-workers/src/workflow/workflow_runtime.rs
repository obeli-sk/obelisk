use super::deadline_tracker::{EpochCallbackError, InterruptKind};
use super::workflow_ctx::{WorkflowCtx, WorkflowFunctionError};
use super::workflow_worker::{CallFuncResult, RunError};
use async_trait::async_trait;
use concepts::{
    ComponentId, FunctionFqn, Params, ParamsParsingError, SupportedFunctionReturnValue, TrapKind,
};
use std::sync::Arc;
use tracing::info;
use wasmtime::component::types::ComponentFunc;
use wasmtime::component::{ComponentExportIndex, InstancePre, Val};
use wasmtime::{Engine, Store};

pub(crate) enum RuntimePrepareError {
    LimitReached {
        reason: String,
        workflow_ctx: WorkflowCtx,
    },
    CannotInstantiate {
        reason: String,
        detail: Option<String>,
        workflow_ctx: WorkflowCtx,
    },
    ParamsParsing {
        err: ParamsParsingError,
        workflow_ctx: WorkflowCtx,
    },
    LockExpired(WorkflowCtx),
    Interrupt(InterruptKind, WorkflowCtx),
}

#[async_trait]
pub(crate) trait WorkflowInvocation: Send {
    async fn invoke(self: Box<Self>, assigned_fuel: Option<u64>) -> CallFuncResult;
}

#[async_trait]
pub(crate) trait WorkflowRuntime: Send + Sync {
    async fn prepare(
        &self,
        workflow_ctx: WorkflowCtx,
        component_id: &ComponentId,
        ffqn: &FunctionFqn,
        params: &Params,
        fuel: Option<u64>,
        instance_permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    ) -> Result<Box<dyn WorkflowInvocation>, RuntimePrepareError>;
}

pub(crate) struct WasmtimeWorkflowRuntime {
    engine: Arc<Engine>,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    instance_pre: InstancePre<WorkflowCtx>,
    memory: Option<u64>,
}

impl WasmtimeWorkflowRuntime {
    pub(crate) fn new(
        engine: Arc<Engine>,
        exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
        instance_pre: InstancePre<WorkflowCtx>,
        memory: Option<u64>,
    ) -> Self {
        Self {
            engine,
            exported_ffqn_to_index,
            instance_pre,
            memory,
        }
    }
}

struct WasmtimeWorkflowInvocation {
    store: Store<WorkflowCtx>,
    func: wasmtime::component::Func,
    component_func: ComponentFunc,
    params: Arc<[Val]>,
}

#[async_trait]
impl WorkflowRuntime for WasmtimeWorkflowRuntime {
    async fn prepare(
        &self,
        workflow_ctx: WorkflowCtx,
        component_id: &ComponentId,
        ffqn: &FunctionFqn,
        params: &Params,
        fuel: Option<u64>,
        _instance_permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    ) -> Result<Box<dyn WorkflowInvocation>, RuntimePrepareError> {
        let mut store = Store::new(&self.engine, workflow_ctx);
        store.data_mut().memory_limiter = crate::store_limits::StoreMemoryLimiter::new(self.memory);
        store.limiter(|ctx| &mut ctx.memory_limiter);
        if let Some(fuel) = fuel {
            store
                .set_fuel(fuel)
                .expect("engine must have `consume_fuel` enabled");
        }
        store.epoch_deadline_callback(|store_ctx| match store_ctx.data().check_epoch_callback() {
            Ok(()) => Ok(wasmtime::UpdateDeadline::YieldCustom(
                1,
                Box::pin(tokio::task::yield_now()),
            )),
            Err(EpochCallbackError::LockExpired) => {
                info!("Deadline reached in epoch callback");
                Err(wasmtime::Error::from(WorkflowFunctionError::LockExpired))
            }
            Err(EpochCallbackError::Interrupt(kind)) => {
                info!("Execution interrupt detected in epoch callback: {kind:?}");
                Err(wasmtime::Error::from(WorkflowFunctionError::Interrupt(
                    kind,
                )))
            }
        });

        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                if let Some(wf_err) = err.downcast_ref::<WorkflowFunctionError>() {
                    match wf_err {
                        WorkflowFunctionError::LockExpired => {
                            return Err(RuntimePrepareError::LockExpired(store.into_data()));
                        }
                        WorkflowFunctionError::Interrupt(kind) => {
                            return Err(RuntimePrepareError::Interrupt(*kind, store.into_data()));
                        }
                        _ => {}
                    }
                }
                let reason = err.to_string();
                let workflow_ctx = store.into_data();
                if reason.starts_with("maximum concurrent") {
                    return Err(RuntimePrepareError::LimitReached {
                        reason,
                        workflow_ctx,
                    });
                }
                return Err(RuntimePrepareError::CannotInstantiate {
                    reason: format!("cannot instantiate: {err}"),
                    detail: Some(format!("{err:?}")),
                    workflow_ctx,
                });
            }
        };

        let Some(fn_export_index) = self.exported_ffqn_to_index.get(ffqn) else {
            return Err(RuntimePrepareError::CannotInstantiate {
                reason: format!("function {ffqn} not found in exports of {component_id}"),
                detail: None,
                workflow_ctx: store.into_data(),
            });
        };
        let func = instance
            .get_func(&mut store, fn_export_index)
            .expect("exported function must be found");
        let component_func = func.ty(&store);
        let params = match params.as_vals(component_func.params()) {
            Ok(params) => params,
            Err(err) => {
                return Err(RuntimePrepareError::ParamsParsing {
                    err,
                    workflow_ctx: store.into_data(),
                });
            }
        };
        Ok(Box::new(WasmtimeWorkflowInvocation {
            store,
            func,
            component_func,
            params,
        }))
    }
}

#[async_trait]
impl WorkflowInvocation for WasmtimeWorkflowInvocation {
    async fn invoke(mut self: Box<Self>, assigned_fuel: Option<u64>) -> CallFuncResult {
        let result_types = self.component_func.results();
        let mut results = vec![Val::Bool(false); result_types.len()];
        let func_call_result = self
            .func
            .call_async(&mut self.store, &self.params, &mut results)
            .await;
        let workflow_ctx = self.store.into_data();

        match func_call_result {
            Ok(()) => match SupportedFunctionReturnValue::new_from_iterator(
                results.into_iter().zip(result_types),
            ) {
                Ok(result) => Ok((result, workflow_ctx)),
                Err(err) => Err(RunError::ResultParsingError(err, Box::new(workflow_ctx))),
            },
            Err(err) => {
                if let Some(err) = err
                    .source()
                    .and_then(|source| source.downcast_ref::<WorkflowFunctionError>())
                {
                    let partial = err
                        .clone()
                        .into_worker_partial_result(workflow_ctx.version().clone());
                    Err(RunError::WorkerPartialResult(
                        partial,
                        Box::new(workflow_ctx),
                    ))
                } else if let Some(trap) = err
                    .source()
                    .and_then(|source| source.downcast_ref::<wasmtime::Trap>())
                {
                    let (reason, detail, kind) = if *trap == wasmtime::Trap::OutOfFuel {
                        (
                            format!(
                                "total fuel consumed: {}",
                                assigned_fuel.expect("fuel must be set for an out-of-fuel trap")
                            ),
                            None,
                            TrapKind::OutOfFuel,
                        )
                    } else {
                        (trap.to_string(), Some(format!("{err:?}")), TrapKind::Trap)
                    };
                    Err(RunError::Trap {
                        reason,
                        detail,
                        workflow_ctx: Box::new(workflow_ctx),
                        kind,
                    })
                } else {
                    Err(RunError::Trap {
                        reason: err.to_string(),
                        detail: Some(format!("{err:?}")),
                        workflow_ctx: Box::new(workflow_ctx),
                        kind: TrapKind::HostFunctionError,
                    })
                }
            }
        }
    }
}
