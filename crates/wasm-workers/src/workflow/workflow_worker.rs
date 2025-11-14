use super::deadline_tracker::DeadlineTrackerFactory;
use super::event_history::ApplyError;
use super::workflow_ctx::{WorkflowCtx, WorkflowFunctionError};
use crate::workflow::workflow_ctx::{ImportedFnCall, WorkerPartialResult};
use crate::{RunnableComponent, WasmFileError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::storage::DbPool;
use concepts::time::{ClockFn, now_tokio_instant};
use concepts::{
    ComponentId, FunctionFqn, FunctionMetadata, PackageIfcFns, ResultParsingError, StrVariant,
    TrapKind,
};
use concepts::{FunctionRegistry, SupportedFunctionReturnValue};
use executor::worker::{FatalError, WorkerContext, WorkerResult};
use executor::worker::{Worker, WorkerError};
use std::future;
use std::ops::Deref;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{Span, debug, error, info, instrument, trace, warn};
use utils::wasm_tools::DecodeError;
use wasmtime::component::{ComponentExportIndex, InstancePre};
use wasmtime::{Engine, component::Val};
use wasmtime::{Store, UpdateDeadline};

/// Defines behavior of the wasm runtime when `HistoryEvent::JoinNextBlocking` is requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum JoinNextBlockingStrategy {
    /// Shut down the current runtime. When the [`JoinSetResponse`] is appended, workflow is reexecuted with a new `RunId`.
    Interrupt,
    /// Keep the execution hot. Worker will poll the database until the execution lock expires.
    Await { non_blocking_event_batching: u32 },
}
pub const DEFAULT_NON_BLOCKING_EVENT_BATCHING: u32 = 100;

#[derive(Clone, Debug)]
pub struct WorkflowConfig {
    pub component_id: ComponentId,
    pub join_next_blocking_strategy: JoinNextBlockingStrategy,
    pub backtrace_persist: bool,
    pub stub_wasi: bool,
    pub fuel: Option<u64>,
}

pub struct WorkflowWorkerCompiled<C: ClockFn> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    clock_fn: C,
    wasmtime_component: wasmtime::component::Component,
    exported_functions_ext: Vec<FunctionMetadata>,
    exports_hierarchy_ext: Vec<PackageIfcFns>,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    exported_functions_noext: Vec<FunctionMetadata>,
    imported_functions: Vec<FunctionMetadata>,
}

pub struct WorkflowWorkerLinked<C: ClockFn> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    clock_fn: C,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    instance_pre: InstancePre<WorkflowCtx<C>>,
    exported_functions_noext: Vec<FunctionMetadata>,
    fn_registry: Arc<dyn FunctionRegistry>,
}

#[derive(Clone)]
pub struct WorkflowWorker<C: ClockFn> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    exported_functions_noext: Vec<FunctionMetadata>,
    db_pool: Arc<dyn DbPool>,
    clock_fn: C,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    instance_pre: InstancePre<WorkflowCtx<C>>,
    fn_registry: Arc<dyn FunctionRegistry>,
    deadline_factory: Arc<dyn DeadlineTrackerFactory>,
}

const WASI_NAMESPACE: &str = "wasi";

impl<C: ClockFn> WorkflowWorkerCompiled<C> {
    // If `config.stub_wasi` is set, this function must remove WASI exports and imports.
    pub fn new_with_config(
        runnable_component: RunnableComponent,
        config: WorkflowConfig,
        engine: Arc<Engine>,
        clock_fn: C,
    ) -> Result<Self, DecodeError> {
        let exported_functions_ext = runnable_component
            .wasm_component
            .exim
            .get_exports(true)
            .iter()
            .filter(|&fn_meta| {
                if config.stub_wasi {
                    // Hide wasi exports
                    fn_meta.ffqn.ifc_fqn.namespace() != WASI_NAMESPACE
                } else {
                    true
                }
            })
            .cloned()
            .collect();
        let exports_hierarchy_ext = runnable_component
            .wasm_component
            .exim
            .get_exports_hierarchy_ext()
            .iter()
            .filter(|&package_ifc_fns| {
                if config.stub_wasi {
                    // Hide wasi exports
                    package_ifc_fns.ifc_fqn.namespace() != WASI_NAMESPACE
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        let mut exported_ffqn_to_index = runnable_component.index_exported_functions()?;
        exported_ffqn_to_index.retain(|ffqn, _| {
            if config.stub_wasi {
                // Hide wasi exports
                ffqn.ifc_fqn.namespace() != WASI_NAMESPACE
            } else {
                true
            }
        });

        let exported_functions_noext = runnable_component
            .wasm_component
            .exim
            .get_exports(false)
            .iter()
            .filter(|&fn_meta| {
                if config.stub_wasi {
                    // Hide wasi exports
                    fn_meta.ffqn.ifc_fqn.namespace() != WASI_NAMESPACE
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        let imported_functions = runnable_component
            .wasm_component
            .exim
            .imports_flat
            .into_iter()
            .filter(|fn_meta| {
                if config.stub_wasi {
                    // Hide wasi imports
                    fn_meta.ffqn.ifc_fqn.namespace() != WASI_NAMESPACE
                } else {
                    true
                }
            })
            .collect();

        Ok(Self {
            config,
            engine,
            clock_fn,
            wasmtime_component: runnable_component.wasmtime_component,
            exported_functions_ext,
            exports_hierarchy_ext,
            exported_ffqn_to_index,
            exported_functions_noext,
            imported_functions,
        })
    }

    #[instrument(skip_all, fields(component_id = %self.config.component_id))]
    pub fn link(
        self,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Result<WorkflowWorkerLinked<C>, WasmFileError> {
        let mut linker = wasmtime::component::Linker::new(&self.engine);

        // Link obelisk:workflow-support and obelisk:log
        WorkflowCtx::add_to_linker(&mut linker, self.config.stub_wasi)?;

        // Mock imported functions
        for import in fn_registry
            .all_exports()
            .iter()
            // Skip already linked functions to avoid unexpected behavior and security issues.
            .filter(|import| !import.ifc_fqn.is_namespace_obelisk())
        {
            trace!(
                ifc_fqn = %import.ifc_fqn,
                "Adding imported interface to the linker",
            );
            if let Ok(mut linker_instance) = linker.instance(import.ifc_fqn.deref()) {
                for function_name in import.fns.keys() {
                    let ffqn = FunctionFqn {
                        ifc_fqn: import.ifc_fqn.clone(),
                        function_name: function_name.clone(),
                    };
                    trace!("Adding mock for imported function {ffqn} to the linker");
                    let res = linker_instance.func_new_async(function_name.deref(), {
                        let ffqn = ffqn.clone();
                        let fn_registry = fn_registry.clone();
                        move |mut store_ctx: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                              params: &[Val],
                              results: &mut [Val]| {
                            let imported_fn_call = match ImportedFnCall::new(
                                ffqn.clone(),
                                &mut store_ctx,
                                params,
                                self.config.backtrace_persist,
                                fn_registry.as_ref(),
                            ) {
                                Ok(imported_fn_call) => imported_fn_call,
                                Err(err) => {
                                    return Box::new(future::ready(Result::Err(
                                        wasmtime::Error::new(err),
                                    )));
                                }
                            };
                            let ffqn = ffqn.clone();
                            Box::new(async move {
                                let workflow_ctx = store_ctx.data_mut();
                                let called_at = workflow_ctx.clock_fn.now();
                                let val = workflow_ctx
                                    .call_imported_fn(imported_fn_call, called_at, &ffqn)
                                    .await
                                    .map_err(wasmtime::Error::new)?;

                                if results.len() == 1 {
                                    results[0] = val;
                                    Ok(())
                                } else {
                                    error!(
                                        "Function expects incorrect result length {}",
                                        results.len()
                                    );
                                    Err(wasmtime::Error::new(
                                        WorkflowFunctionError::ImportedFunctionCallError {
                                            ffqn,
                                            reason: StrVariant::Static(
                                                "function expects incorrect result length",
                                            ),
                                            detail: Some(format!(
                                                "function expects incorrect result length {}",
                                                results.len()
                                            )),
                                        },
                                    ))
                                }
                            })
                        }
                    });
                    if let Err(err) = res {
                        return Err(WasmFileError::LinkingError {
                            context: StrVariant::Arc(Arc::from(format!(
                                "cannot add mock for imported function {ffqn}"
                            ))),
                            err: err.into(),
                        });
                    }
                }
            } else {
                warn!("Skipping interface {ifc_fqn}", ifc_fqn = import.ifc_fqn);
            }
        }

        // Pre-instantiate to catch missing imports
        let instance_pre = linker
            .instantiate_pre(&self.wasmtime_component)
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("preinstantiation error"),
                err: err.into(),
            })?;

        Ok(WorkflowWorkerLinked {
            config: self.config,
            engine: self.engine,
            clock_fn: self.clock_fn,
            exported_ffqn_to_index: self.exported_ffqn_to_index,
            instance_pre,
            exported_functions_noext: self.exported_functions_noext,
            fn_registry,
        })
    }

    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        &self.exported_functions_ext
    }

    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        &self.exports_hierarchy_ext
    }

    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.imported_functions
    }
}

impl<C: ClockFn> WorkflowWorkerLinked<C> {
    pub fn into_worker(
        self,
        db_pool: Arc<dyn DbPool>,
        deadline_factory: Arc<dyn DeadlineTrackerFactory>,
    ) -> WorkflowWorker<C> {
        WorkflowWorker {
            config: self.config,
            engine: self.engine,
            db_pool,
            clock_fn: self.clock_fn,
            exported_ffqn_to_index: self.exported_ffqn_to_index,
            instance_pre: self.instance_pre,
            exported_functions_noext: self.exported_functions_noext,
            fn_registry: self.fn_registry,
            deadline_factory,
        }
    }
}

enum RunError<C: ClockFn + 'static> {
    ResultParsingError(ResultParsingError, WorkflowCtx<C>),
    /// Error from the wasmtime runtime that can be downcast to `WorkflowFunctionError`
    WorkerPartialResult(WorkerPartialResult, WorkflowCtx<C>),
    /// Error that happened while running the function.
    Trap {
        reason: String,
        detail: Option<String>,
        workflow_ctx: WorkflowCtx<C>,
        kind: TrapKind,
    },
}

enum WorkerResultRefactored<C: ClockFn> {
    Ok(SupportedFunctionReturnValue, WorkflowCtx<C>),
    FatalError(FatalError, WorkflowCtx<C>),
    Retriable(WorkerResult),
}

type CallFuncResult<C> = Result<(SupportedFunctionReturnValue, WorkflowCtx<C>), RunError<C>>;

enum PrepareFuncErr {
    WorkerError(WorkerError),
    DbUpdatedByWorkerOrWatcher,
}

impl<C: ClockFn + 'static> WorkflowWorker<C> {
    async fn prepare_func(
        &self,
        ctx: WorkerContext,
    ) -> Result<(Store<WorkflowCtx<C>>, wasmtime::component::Func, Arc<[Val]>), PrepareFuncErr>
    {
        let started_at = self.clock_fn.now();
        let Ok(deadline_duration) = (ctx.execution_deadline - started_at).to_std() else {
            ctx.worker_span.in_scope(||
                info!(execution_deadline = %ctx.execution_deadline, %started_at, "Timed out - started_at later than execution_deadline")
            );
            return Err(PrepareFuncErr::DbUpdatedByWorkerOrWatcher);
        };
        let deadline_tracker = self.deadline_factory.create(deadline_duration);
        let version_at_start = ctx.version.clone();
        let seed = ctx.execution_id.random_seed();
        let workflow_ctx = WorkflowCtx::new(
            ctx.execution_id,
            self.config.component_id.clone(),
            ctx.event_history,
            ctx.responses,
            seed,
            self.clock_fn.clone(),
            self.config.join_next_blocking_strategy,
            self.db_pool.clone(),
            ctx.version,
            ctx.execution_deadline,
            ctx.worker_span,
            self.config.backtrace_persist,
            deadline_tracker,
            self.fn_registry.clone(),
        );

        let mut store = Store::new(&self.engine, workflow_ctx);

        // Set fuel.
        if let Some(fuel) = self.config.fuel {
            store
                .set_fuel(fuel)
                .expect("engine must have `consume_fuel` enabled");
        }

        // Configure epoch callback before running the initialization to avoid interruption
        store.epoch_deadline_callback(|_store_ctx| {
            Ok(UpdateDeadline::YieldCustom(
                1,
                Box::pin(tokio::task::yield_now()),
            ))
        });

        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                let reason = err.to_string();
                let version = store.into_data().version;
                if reason.starts_with("maximum concurrent") {
                    return Err(PrepareFuncErr::WorkerError(WorkerError::LimitReached {
                        reason,
                        version,
                    }));
                }
                return Err(PrepareFuncErr::WorkerError(WorkerError::FatalError(
                    FatalError::CannotInstantiate {
                        reason: format!("{err}"),
                        detail: format!("{err:?}"),
                    },
                    version,
                )));
            }
        };

        let func = {
            let fn_export_index = self
                .exported_ffqn_to_index
                .get(&ctx.ffqn)
                .expect("executor only calls `run` with ffqns that are exported");
            instance
                .get_func(&mut store, fn_export_index)
                .expect("exported function must be found")
        };

        let params = match ctx.params.as_vals(func.params(&store)) {
            Ok(params) => params,
            Err(err) => {
                return Err(PrepareFuncErr::WorkerError(WorkerError::FatalError(
                    FatalError::ParamsParsingError(err),
                    version_at_start,
                )));
            }
        };
        Ok((store, func, params))
    }

    async fn call_func(
        mut store: Store<WorkflowCtx<C>>,
        func: wasmtime::component::Func,
        params: Arc<[Val]>,
        assigned_fuel: Option<u64>,
    ) -> CallFuncResult<C> {
        let result_types = func.results(&mut store);
        let mut results = vec![Val::Bool(false); result_types.len()];
        let func_call_result = func.call_async(&mut store, &params, &mut results).await;
        if func_call_result.is_ok()
            && let Err(post_return_err) = func.post_return_async(&mut store).await
        {
            return Err(RunError::Trap {
                reason: post_return_err.to_string(),
                detail: Some(format!("{post_return_err:?}")),
                workflow_ctx: store.into_data(),
                kind: TrapKind::PostReturnTrap,
            });
        }
        let workflow_ctx = store.into_data();

        match func_call_result {
            Ok(()) => {
                match SupportedFunctionReturnValue::new(
                    results.into_iter().zip(result_types.iter().cloned()),
                ) {
                    Ok(result) => Ok((result, workflow_ctx)),
                    Err(err) => Err(RunError::ResultParsingError(err, workflow_ctx)),
                }
            }
            Err(err) => {
                // Try to unpack `WorkflowFunctionError`
                if let Some(err) = err
                    .source()
                    .and_then(|source| source.downcast_ref::<WorkflowFunctionError>())
                {
                    let worker_partial_result = err
                        .clone()
                        .into_worker_partial_result(workflow_ctx.version.clone());
                    Err(RunError::WorkerPartialResult(
                        worker_partial_result,
                        workflow_ctx,
                    ))
                } else if let Some(trap) = err
                    .source()
                    .and_then(|source| source.downcast_ref::<wasmtime::Trap>())
                {
                    if *trap == wasmtime::Trap::OutOfFuel {
                        Err(RunError::Trap {
                            reason: format!(
                                "total fuel consumed: {}",
                                assigned_fuel
                                    .expect("must have been set as it was the reason of trap")
                            ),
                            detail: None,
                            workflow_ctx,
                            kind: TrapKind::OutOfFuel,
                        })
                    } else {
                        Err(RunError::Trap {
                            reason: trap.to_string(),
                            detail: Some(format!("{err:?}")),
                            workflow_ctx,
                            kind: TrapKind::Trap,
                        })
                    }
                } else {
                    Err(RunError::Trap {
                        reason: err.to_string(),
                        detail: Some(format!("{err:?}")),
                        workflow_ctx,
                        kind: TrapKind::HostFunctionError,
                    })
                }
            }
        }
    }

    async fn call_func_convert_result(
        store: Store<WorkflowCtx<C>>,
        func: wasmtime::component::Func,
        params: Arc<[Val]>,
        worker_span: &Span,
        execution_deadline: DateTime<Utc>,
        assigned_fuel: Option<u64>,
    ) -> WorkerResult {
        // call_func
        let elapsed = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        let res = Self::call_func(store, func, params, assigned_fuel).await;
        let elapsed = elapsed.elapsed();
        let worker_result_refactored =
            Self::convert_result(res, worker_span, elapsed, execution_deadline).await;
        match worker_result_refactored {
            WorkerResultRefactored::Ok(res, mut workflow_ctx) => {
                match Self::close_join_sets(&mut workflow_ctx).await {
                    Err(worker_result) => {
                        debug!("Error while closing join sets {worker_result:?}");
                        worker_result
                    }
                    Ok(()) => WorkerResult::Ok(res, workflow_ctx.version, None),
                }
            }
            WorkerResultRefactored::FatalError(err, mut workflow_ctx) => {
                match Self::close_join_sets(&mut workflow_ctx).await {
                    Err(worker_result) => {
                        debug!("Error while closing join sets {worker_result:?}");
                        worker_result
                    }
                    Ok(()) => WorkerResult::Err(WorkerError::FatalError(err, workflow_ctx.version)),
                }
            }
            WorkerResultRefactored::Retriable(retriable) => retriable,
        }
    }

    #[instrument(skip_all, fields(res, worker_span))]
    async fn convert_result(
        res: CallFuncResult<C>,
        worker_span: &Span,
        #[expect(unused_variables)] elapsed: Duration,
        #[expect(unused_variables)] execution_deadline: DateTime<Utc>,
    ) -> WorkerResultRefactored<C> {
        match res {
            Ok((supported_result, mut workflow_ctx)) => {
                worker_span.in_scope(|| info!("Finished"));
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(|| error!("Database error: {db_err}"));
                    return WorkerResultRefactored::Retriable(WorkerResult::Err(
                        WorkerError::DbError(db_err),
                    ));
                }
                WorkerResultRefactored::Ok(supported_result, workflow_ctx)
            }
            Err(RunError::Trap {
                reason,
                detail,
                mut workflow_ctx,
                kind,
            }) => {
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(||
                        error!("Database flush error: {db_err:?} while handling {kind}: `{reason}`, `{detail:?}`, execution will be retried")
                    );
                    return WorkerResultRefactored::Retriable(WorkerResult::Err(
                        WorkerError::DbError(db_err),
                    ));
                }
                let version = workflow_ctx.version;
                worker_span.in_scope(|| info!("Trap handled as a fatal error"));
                let err = WorkerError::FatalError(
                    FatalError::WorkflowTrap {
                        reason,
                        trap_kind: kind,
                        detail,
                    },
                    version,
                );

                WorkerResultRefactored::Retriable(WorkerResult::Err(err))
            }
            Err(RunError::WorkerPartialResult(worker_partial_result, mut workflow_ctx)) => {
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(||
                        error!("Database flush error: {db_err:?} while handling WorkerPartialResult: {worker_partial_result:?}")
                    );
                    return WorkerResultRefactored::Retriable(WorkerResult::Err(
                        WorkerError::DbError(db_err),
                    ));
                }
                match worker_partial_result {
                    WorkerPartialResult::FatalError(err, _version) => {
                        worker_span.in_scope(|| info!("Finished with a fatal error: {err}"));
                        WorkerResultRefactored::FatalError(err, workflow_ctx)
                    }
                    WorkerPartialResult::InterruptDbUpdated => {
                        worker_span.in_scope(|| info!("Interrupt requested"));
                        WorkerResultRefactored::Retriable(WorkerResult::DbUpdatedByWorkerOrWatcher)
                    }
                    WorkerPartialResult::DbError(db_err) => WorkerResultRefactored::Retriable(
                        WorkerResult::Err(WorkerError::DbError(db_err)),
                    ),
                }
            }
            Err(RunError::ResultParsingError(err, mut workflow_ctx)) => {
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(|| error!("Database error: {db_err}"));
                    return WorkerResultRefactored::Retriable(WorkerResult::Err(
                        WorkerError::DbError(db_err),
                    ));
                }
                worker_span.in_scope(|| error!("Fatal error: Result parsing error: {err}"));
                WorkerResultRefactored::FatalError(
                    FatalError::ResultParsingError(err),
                    workflow_ctx,
                )
            }
        }
    }

    async fn close_join_sets(workflow_ctx: &mut WorkflowCtx<C>) -> Result<(), WorkerResult> {
        workflow_ctx
            .close_forgotten_join_sets()
            .await
            .map_err(|err| match err {
                ApplyError::InterruptDbUpdated => WorkerResult::DbUpdatedByWorkerOrWatcher,
                ApplyError::DbError(db_error) => WorkerResult::Err(WorkerError::DbError(db_error)),
                ApplyError::NondeterminismDetected(detail) => {
                    WorkerResult::Err(WorkerError::FatalError(
                        FatalError::NondeterminismDetected { detail },
                        workflow_ctx.version.clone(),
                    ))
                }
            })
    }
}

#[async_trait]
impl<C: ClockFn + 'static> Worker for WorkflowWorker<C> {
    fn exported_functions(&self) -> &[FunctionMetadata] {
        &self.exported_functions_noext
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        ctx.worker_span.in_scope(|| info!("Execution run started"));
        if !ctx.can_be_retried {
            warn!(
                "Workflow configuration set to not retry anymore. This can lead to nondeterministic results."
            );
        }
        let worker_span = ctx.worker_span.clone();
        let execution_deadline = ctx.execution_deadline;
        match self.prepare_func(ctx).await {
            Ok((store, func, params)) => {
                Self::call_func_convert_result(
                    store,
                    func,
                    params,
                    &worker_span,
                    execution_deadline,
                    self.config.fuel,
                )
                .await
            }
            Err(PrepareFuncErr::DbUpdatedByWorkerOrWatcher) => {
                WorkerResult::DbUpdatedByWorkerOrWatcher
            }
            Err(PrepareFuncErr::WorkerError(err)) => WorkerResult::Err(err),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::activity::activity_worker::tests::new_activity;
    use crate::activity::activity_worker::tests::{compile_activity_stub, new_activity_worker};
    use crate::testing_fn_registry::{TestingFnRegistry, fn_registry_dummy};
    use crate::workflow::deadline_tracker::DeadlineTrackerFactoryTokio;
    use crate::{
        activity::activity_worker::tests::{
            FIBO_10_INPUT, FIBO_10_OUTPUT, compile_activity, new_activity_fibo, wasm_file_name,
        },
        engines::{EngineConfig, Engines},
    };
    use assert_matches::assert_matches;
    use chrono::DateTime;
    use concepts::prefixed_ulid::ExecutionIdDerived;
    use concepts::storage::{
        AppendEventsToExecution, AppendResponseToExecution, PendingStateFinishedError,
    };
    use concepts::storage::{
        AppendRequest, DbConnection, DbExecutor, ExecutionEventInner, JoinSetResponse,
        JoinSetResponseEvent, JoinSetResponseEventOuter,
    };
    use concepts::time::TokioSleep;
    use concepts::{ComponentRetryConfig, ExecutionId, Params, SupportedFunctionReturnValue};
    use concepts::{
        ComponentType,
        prefixed_ulid::{ExecutorId, RunId},
        storage::{
            CreateRequest, DbPoolCloseable, PendingState, PendingStateFinished,
            PendingStateFinishedResultKind, Version, wait_for_pending_state_fn,
        },
    };
    use db_tests::Database;
    use executor::executor::extract_exported_ffqns_noext_test;
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
    };
    use rstest::rstest;
    use serde_json::json;
    use std::ops::Deref;
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use tracing::debug;
    use tracing::info_span;
    use val_json::{
        type_wrapper::TypeWrapper,
        wast_val::{WastVal, WastValWithType},
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    pub const FIBOA_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA,
    ); // fiboa: func(n: u8, iterations: u32) -> u64;
    const SLEEP1_HOST_ACTIVITY_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SLEEP_HOST_ACTIVITY); // sleep-host-activity: func(millis: u64);

    const SLEEP2_HOST_ACTIVITY_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SLEEP_SCHEDULE_AT); // sleep-host-activity: func(s: schedule-at);

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    const FFQN_WORKFLOW_HTTP_GET_STARGAZERS: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_STARGAZERS);

    const FFQN_WORKFLOW_HTTP_GET: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET,
    );
    const FFQN_WORKFLOW_HTTP_GET_SUCCESSFUL: FunctionFqn = FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL);
    const FFQN_WORKFLOW_HTTP_GET_RESP: FunctionFqn = FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_RESP);

    pub(crate) fn compile_workflow(wasm_path: &str) -> (RunnableComponent, ComponentId) {
        let engine = Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        compile_workflow_with_engine(wasm_path, &engine)
    }

    pub(crate) fn compile_workflow_with_engine(
        wasm_path: &str,
        engine: &Engine,
    ) -> (RunnableComponent, ComponentId) {
        let component_id =
            ComponentId::new(ComponentType::Workflow, wasm_file_name(wasm_path)).unwrap();
        (
            RunnableComponent::new(wasm_path, engine, component_id.component_type).unwrap(),
            component_id,
        )
    }

    fn new_workflow<C: ClockFn + 'static>(
        db_pool: Arc<dyn DbPool>,
        db_exec: Arc<dyn DbExecutor>,
        wasm_path: &'static str,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
    ) -> ExecTask<C> {
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let component_id =
            ComponentId::new(ComponentType::Workflow, wasm_file_name(wasm_path)).unwrap();
        let worker = Arc::new(
            WorkflowWorkerCompiled::new_with_config(
                RunnableComponent::new(wasm_path, &workflow_engine, component_id.component_type)
                    .unwrap(),
                WorkflowConfig {
                    component_id: component_id.clone(),
                    join_next_blocking_strategy,
                    backtrace_persist: false,
                    stub_wasi: false,
                    fuel: None,
                },
                workflow_engine,
                clock_fn.clone(),
            )
            .unwrap()
            .link(fn_registry.clone())
            .unwrap()
            .into_worker(db_pool, Arc::new(DeadlineTrackerFactoryTokio)),
        );
        info!("Instantiated worker");
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            component_id,
            task_limiter: None,
            executor_id: ExecutorId::generate(),
            retry_config: ComponentRetryConfig::WORKFLOW_TEST,
        };
        ExecTask::new_all_ffqns_test(worker, exec_config, clock_fn, db_exec)
    }

    pub(crate) fn new_workflow_fibo<C: ClockFn + 'static>(
        db_pool: Arc<dyn DbPool>,
        db_exec: Arc<dyn DbExecutor>,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
    ) -> ExecTask<C> {
        new_workflow(
            db_pool,
            db_exec,
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            clock_fn,
            join_next_blocking_strategy,
            fn_registry,
        )
    }

    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity_mem(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        fibo_workflow_should_submit_fibo_activity(
            db_pool.clone(),
            db_exec,
            sim_clock,
            join_next_blocking_strategy,
        )
        .await;
        db_close.close().await;
    }

    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_should_submit_fibo_activity_sqlite(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = Database::Sqlite.set_up().await;
        fibo_workflow_should_submit_fibo_activity(
            db_pool.clone(),
            db_exec,
            sim_clock,
            join_next_blocking_strategy,
        )
        .await;
        db_close.close().await;
    }

    async fn fibo_workflow_should_submit_fibo_activity(
        db_pool: Arc<dyn DbPool>,
        db_exec: Arc<dyn DbExecutor>,
        sim_clock: SimClock,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        const INPUT_ITERATIONS: u32 = 1;
        test_utils::set_up();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY),
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW),
        ]);
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            db_exec.clone(),
            sim_clock.clone(),
            join_next_blocking_strategy,
            &fn_registry,
        );
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();

        let params = Params::from_json_values(vec![json!(FIBO_10_INPUT), json!(INPUT_ITERATIONS)]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBOA_WORKFLOW_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                scheduled_by: None,
            })
            .await
            .unwrap();
        info!("Should end as BlockedByJoinSet");

        let executed_workflows = workflow_exec
            .tick_test(sim_clock.now(), RunId::generate())
            .await;

        let executed_workflows =
            if join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt {
                assert_eq!(1, executed_workflows.wait_for_tasks().await.len());
                None
            } else {
                // TODO: Make test more deterministic by waiting for deadline tracker to be called here.
                Some(executed_workflows)
            };

        // No waiting needed in case of `Interrupt`
        wait_for_pending_state_fn(
            db_connection.as_ref(),
            &execution_id,
            |exe_history| {
                matches!(
                    exe_history.pending_state,
                    PendingState::BlockedByJoinSet { .. }
                )
                .then_some(())
            },
            None,
        )
        .await
        .unwrap();

        info!("Execution should call the activity and finish");

        let activity_exec = new_activity_fibo(db_exec.clone(), sim_clock.clone(), TokioSleep);
        let executed_activities = activity_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_activities.len());

        let executed_workflows = if let Some(executed_workflows) = executed_workflows {
            // Await strategy still runs
            executed_workflows.wait_for_tasks().await
        } else {
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
        };
        assert_eq!(1, executed_workflows.len());

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        let res = assert_matches!(res, SupportedFunctionReturnValue::Ok{ok: Some(val)} => val);

        let fibo = assert_matches!(res,
            WastValWithType {value: WastVal::U64(val), r#type: TypeWrapper::U64 } => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);
    }

    #[tokio::test]
    #[should_panic(expected = "LinkingError { context: preinstantiation error")]
    async fn fibo_workflow_with_missing_imports_should_fail() {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, _db_close) = Database::Memory.set_up().await;
        test_utils::set_up();
        let fn_registry = fn_registry_dummy(&[]);
        new_workflow_fibo(
            db_pool.clone(),
            db_exec,
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );
    }

    pub(crate) fn compile_workflow_worker<C: ClockFn>(
        wasm_path: &str,
        db_pool: Arc<dyn DbPool>,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
    ) -> Arc<WorkflowWorker<C>> {
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        Arc::new(
            WorkflowWorkerCompiled::new_with_config(
                RunnableComponent::new(wasm_path, &workflow_engine, ComponentType::Workflow)
                    .unwrap(),
                WorkflowConfig {
                    component_id: ComponentId::dummy_workflow(),
                    join_next_blocking_strategy,
                    backtrace_persist: false,
                    stub_wasi: false,
                    fuel: None,
                },
                workflow_engine,
                clock_fn,
            )
            .unwrap()
            .link(fn_registry.clone())
            .unwrap()
            .into_worker(db_pool, Arc::new(DeadlineTrackerFactoryTokio)),
        )
    }

    #[tokio::test]
    async fn execution_deadline_before_now_should_timeout() {
        const SLEEP_MILLIS: u32 = 100;
        test_utils::set_up();

        let (_guard, db_pool, _db_exec, db_close) = Database::Memory.set_up().await;

        let sim_clock = SimClock::epoch();
        let worker = compile_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &TestingFnRegistry::new_from_components(vec![
                compile_activity(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                ),
                compile_workflow(
                    test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                ),
            ]),
        );
        // simulate a scheduling problem where deadline < now, meaning there is no point in running the execution.
        let execution_deadline = sim_clock.now();
        sim_clock.move_time_forward(Duration::from_millis(100));

        let ctx = WorkerContext {
            execution_id: ExecutionId::generate(),
            metadata: concepts::ExecutionMetadata::empty(),
            ffqn: SLEEP1_HOST_ACTIVITY_FFQN,
            params: Params::from_json_values(vec![json!({"milliseconds": SLEEP_MILLIS})]),
            event_history: Vec::new(),
            responses: Vec::new(),
            version: Version::new(0),
            execution_deadline,
            can_be_retried: false,
            run_id: RunId::generate(),
            worker_span: info_span!("worker-test"),
        };
        let worker_result = worker.run(ctx).await;
        assert_matches!(worker_result, WorkerResult::DbUpdatedByWorkerOrWatcher); // Do not write anything, let the watcher mark execution as timed out.
        db_close.close().await;
    }

    #[tokio::test]
    async fn sleep2_happy_path() {
        const LOCK_DURATION: Duration = Duration::from_secs(1);
        let join_next_blocking_strategy = JoinNextBlockingStrategy::Interrupt;

        test_utils::set_up();
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        let sim_clock = SimClock::epoch();
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();
        {
            let params = Params::from_json_values(vec![json!("now")]);
            db_connection
                .create(CreateRequest {
                    created_at: sim_clock.now(),
                    execution_id: execution_id.clone(),
                    ffqn: SLEEP2_HOST_ACTIVITY_FFQN,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: sim_clock.now(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
        }
        let sim_clock = SimClock::epoch();
        let sleep_exec = {
            let fn_registry = TestingFnRegistry::new_from_components(vec![
                compile_activity(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                ), // not used here
                compile_workflow(
                    test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                ),
            ]);
            let worker = compile_workflow_worker(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                db_pool.clone(),
                sim_clock.clone(),
                join_next_blocking_strategy,
                &fn_registry,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_DURATION,
                tick_sleep: Duration::ZERO, // irrelevant here as we call tick manually
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
            };
            let ffqns = extract_exported_ffqns_noext_test(worker.as_ref());
            ExecTask::new_test(worker, exec_config, sim_clock.clone(), db_exec, ffqns)
        };
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, worker_tasks);
        }
        let blocked_until = {
            let pending_state = db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap();
            assert_matches!(pending_state, PendingState::BlockedByJoinSet {lock_expires_at, .. } => lock_expires_at)
        };
        assert_eq!(sim_clock.now(), blocked_until);
        // expired_timers_watcher should see the AsyncDelay and send the response.
        {
            let timer = expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
                .await
                .unwrap();
            assert_eq!(1, timer.expired_async_timers);
        }
        // Reexecute the worker
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, worker_tasks);
        }
        assert_matches!(
            db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap(),
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind(Ok(())),
                    ..
                },
                ..
            }
        );

        db_close.close().await;
    }

    #[rstest]
    #[tokio::test]
    // TODO: Test await interleaving with timer - execution should finished in one go.
    async fn sleep_should_be_persisted_after_executor_restart(
        #[values(Database::Memory, Database::Sqlite)] database: Database,
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        const SLEEP_MILLIS: u32 = 100;
        const LOCK_DURATION: Duration = Duration::from_secs(1);
        test_utils::set_up();
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, db_exec, db_close) = database.set_up().await;

        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY), // not used here
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW),
        ]);

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: SLEEP1_HOST_ACTIVITY_FFQN,
                params: Params::from_json_values(vec![json!({"milliseconds": SLEEP_MILLIS})]),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let sleep_exec = {
            let worker = compile_workflow_worker(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                db_pool.clone(),
                sim_clock.clone(),
                join_next_blocking_strategy,
                &fn_registry,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_DURATION,
                tick_sleep: Duration::ZERO, // irrelevant here as we call tick manually
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
            };
            let ffqns = extract_exported_ffqns_noext_test(worker.as_ref());
            ExecTask::new_test(worker, exec_config, sim_clock.clone(), db_exec, ffqns)
        };
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, worker_tasks);
        }
        let blocked_until = {
            let pending_state = db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap();
            assert_matches!(pending_state, PendingState::BlockedByJoinSet {lock_expires_at, .. } => lock_expires_at)
        };
        match join_next_blocking_strategy {
            JoinNextBlockingStrategy::Interrupt => assert_eq!(sim_clock.now(), blocked_until),
            JoinNextBlockingStrategy::Await { .. } => {
                assert_eq!(sim_clock.now() + LOCK_DURATION, blocked_until);
            }
        }
        sim_clock.move_time_forward(Duration::from_millis(u64::from(SLEEP_MILLIS)));

        // Run the worker tick again before the response arrives - should be noop
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(0, worker_tasks);
        }
        // expired_timers_watcher should see the AsyncDelay and send the response.
        {
            let timer = expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
                .await
                .unwrap();
            assert_eq!(1, timer.expired_async_timers);
        }

        // Make sure the timer tick set the execution as pending
        {
            let pending_state = db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap();

            let actual_pending_at = assert_matches!(
                pending_state,
                PendingState::PendingAt {
                    scheduled_at
                }
                => scheduled_at
            );
            match join_next_blocking_strategy {
                JoinNextBlockingStrategy::Interrupt => {
                    assert_eq!(sim_clock.now(), actual_pending_at);
                }
                JoinNextBlockingStrategy::Await { .. } => {
                    assert_eq!(blocked_until, actual_pending_at);
                    sim_clock.move_time_to(blocked_until);
                }
            }
        }
        // Reexecute the worker
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, worker_tasks);
        }
        assert_matches!(
            db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap(),
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind(Ok(())),
                    ..
                },
                ..
            }
        );

        db_close.close().await;
    }

    #[rstest]
    #[tokio::test]
    async fn stargazers_should_be_deserialized_after_interrupt(
        #[values(Database::Sqlite, Database::Memory)] db: Database,
    ) {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool, db_exec, db_close) = db.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
        ]);
        let activity_exec = new_activity(
            db_exec.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
        );

        let workflow_exec = new_workflow(
            db_pool.clone(),
            db_exec,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_HTTP_GET_STARGAZERS,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                scheduled_by: None,
            })
            .await
            .unwrap();

        // Tick workflow
        let executed_workflows = workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_workflows.len());
        // Tick activity
        let executed_activities = activity_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_activities.len());
        // Tick workflow
        let executed_workflows = workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_workflows.len());

        // Check the result.
        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        assert_matches!(res, SupportedFunctionReturnValue::Ok { ok: None });
        db_close.close().await;
    }

    #[rstest]
    #[tokio::test]
    async fn http_get(
        #[values(
            FFQN_WORKFLOW_HTTP_GET,
            FFQN_WORKFLOW_HTTP_GET_RESP,
            FFQN_WORKFLOW_HTTP_GET_SUCCESSFUL
        )]
        ffqn: FunctionFqn,
        #[values(Database::Sqlite, Database::Memory)] db: Database,
    ) {
        const BODY: &str = "ok";

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool, db_exec, db_close) = db.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
        ]);
        let activity_exec = new_activity(
            db_exec.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
        );

        let workflow_exec = new_workflow(
            db_pool.clone(),
            db_exec,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .expect(1)
            .mount(&server)
            .await;
        debug!("started mock server on {}", server.address());
        let url = format!("http://127.0.0.1:{}/", server.address().port());
        let params = Params::from_json_values(vec![json!(url)]);
        // Create an execution.
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                scheduled_by: None,
            })
            .await
            .unwrap();
        // Tick workflow
        let executed_workflows = workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_workflows.len());
        // Tick activity
        let executed_activities = activity_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        // Tick workflow
        let executed_workflows = workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_workflows.len());
        assert_eq!(1, executed_activities.len());
        // Check the result.
        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        let val =
            assert_matches!(res, SupportedFunctionReturnValue::Ok{ok: Some(val)} => val.value);
        let val = assert_matches!(val, WastVal::String(val) => val);
        assert_eq!(BODY, val.deref());
        db_close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn http_get_concurrent(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        const BODY: &str = "ok";
        const GET_SUCCESSFUL_CONCURRENTLY_STRESS: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL_CONCURRENTLY_STRESS);

        test_utils::set_up();
        let concurrency = 5;
        let sim_clock = SimClock::new(DateTime::default());
        let created_at = sim_clock.now();
        let (_guard, db_pool, db_exec, db_close) = db.set_up().await;
        let db_connection = db_pool.connection();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
        ]);

        let activity_exec = new_activity(
            db_exec.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
        );
        let workflow_exec = new_workflow(
            db_pool.clone(),
            db_exec,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .mount(&server)
            .await;
        debug!("started mock server on {}", server.address());
        let url = format!("http://127.0.0.1:{}/", server.address().port());
        let params = Params::from_json_values(vec![json!(url), json!(concurrency)]);
        // Create an execution.
        let execution_id = ExecutionId::generate();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: GET_SUCCESSFUL_CONCURRENTLY_STRESS,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                scheduled_by: None,
            })
            .await
            .unwrap();
        // Tick workflow
        let executed_workflows = workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_workflows.len());
        // Tick activities
        let mut remaining_activities = concurrency;
        while remaining_activities > 0 {
            let executed_activities = activity_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len();
            assert!(executed_activities > 0);
            remaining_activities -= executed_activities;
        }

        // Tick workflow - should collect all the responses
        let executed_workflows = workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_workflows.len());

        assert!(
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .is_empty()
        );

        // Check the result.
        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        let val =
            assert_matches!(res, SupportedFunctionReturnValue::Ok{ok: Some(val)} => val.value);
        let val = assert_matches!(val, WastVal::List(vec) => vec);
        assert_eq!(concurrency, val.len());
        for val in val {
            let val = assert_matches!(val, WastVal::String(val) => val);
            assert_eq!(BODY, val.deref());
        }
        db_close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn scheduling_should_work(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_strategy: JoinNextBlockingStrategy,
    ) {
        const SLEEP_DURATION: Duration = Duration::from_millis(100);
        const FFQN_WORKFLOW_SLEEP_SCHEDULE_NOOP_FFQN: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SCHEDULE_NOOP);
        const FFQN_ACTIVITY_SLEEP_NOOP_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_activity_builder::exports::testing::sleep::sleep::NOOP,
        );
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY),
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW),
        ]);
        let worker = compile_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone(),
            join_next_strategy,
            &fn_registry,
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();

        let params =
            Params::from_json_values(vec![json!({"milliseconds": SLEEP_DURATION.as_millis()})]);
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_SLEEP_SCHEDULE_NOOP_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        let exec_task = ExecTask::new_test(
            worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
            },
            sim_clock.clone(),
            db_exec,
            Arc::new([FFQN_WORKFLOW_SLEEP_SCHEDULE_NOOP_FFQN]),
        );
        {
            let task_count = exec_task
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, task_count);
        }
        let res = db_pool.connection().get(&execution_id).await.unwrap();
        assert_matches!(
            res.into_finished_result().unwrap(),
            SupportedFunctionReturnValue::Ok { ok: None }
        );
        sim_clock.move_time_forward(SLEEP_DURATION);
        // The scheduled `noop` execution should be pending.
        let mut next_pending = db_pool
            .connection()
            .lock_pending(
                10,
                sim_clock.now(),
                Arc::from([FFQN_ACTIVITY_SLEEP_NOOP_FFQN]),
                sim_clock.now(),
                ComponentId::dummy_workflow(),
                ExecutorId::generate(),
                sim_clock.now() + Duration::from_secs(1),
                RunId::generate(),
                ComponentRetryConfig::WORKFLOW_TEST,
            )
            .await
            .unwrap();
        assert_eq!(1, next_pending.len());
        let next_pending = next_pending.pop().unwrap();
        assert!(next_pending.parent.is_none());
        let params = serde_json::to_string(&Params::empty()).unwrap();
        assert_eq!(params, serde_json::to_string(&next_pending.params).unwrap());
        db_close.close().await;
    }

    #[rstest]
    #[tokio::test]
    async fn http_get_fallible_err(
        #[values(Database::Memory, Database::Sqlite)] database: Database,
    ) {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool, db_exec, db_close) = database.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
        ]);
        let activity_exec = new_activity(
            db_exec.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
        );

        let workflow_exec = new_workflow(
            db_pool.clone(),
            db_exec,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );

        let url = "http://";
        let params = Params::from_json_values(vec![json!(url)]);
        // Create an execution.
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_HTTP_GET_SUCCESSFUL,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                scheduled_by: None,
            })
            .await
            .unwrap();

        // Tick workflow
        let executed_workflows = workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_workflows.len());
        // Tick activity
        let executed_activities = activity_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_activities.len());
        // Tick workflow
        let executed_workflows = workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_workflows.len());

        // Check the result.
        let res: SupportedFunctionReturnValue = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        let val =
            assert_matches!(res, SupportedFunctionReturnValue::Err{err: Some(val)} => val.value);
        let val = assert_matches!(val, WastVal::String(val) => val);
        assert_eq!("invalid format", val);

        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        assert_matches!(
            pending_state,
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind(Err(
                        PendingStateFinishedError::FallibleError
                    )),
                    ..
                }
            }
        );
        db_close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn stubbing_should_work(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        const FFQN_WORKFLOW_STUB: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::SUBMIT_STUB_AWAIT,
        );
        const INPUT_PARAM: &str = "bar";
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY),
            compile_workflow(test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW),
        ]);

        let worker = compile_workflow_worker(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();

        let params =
            Params::from_json_values(vec![serde_json::Value::String(INPUT_PARAM.to_string())]);
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_STUB,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        let exec_task = ExecTask::new_test(
            worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
            },
            sim_clock.clone(),
            db_exec,
            Arc::new([FFQN_WORKFLOW_STUB]),
        );

        {
            let task_count = exec_task
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, task_count);
        }

        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        let scheduled_at = assert_matches!(pending_state, PendingState::PendingAt { scheduled_at } => scheduled_at);
        assert_eq!(sim_clock.now(), scheduled_at);

        // another tick + await should mark the execution finished.
        assert_eq!(
            1,
            exec_task
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len()
        );

        let res = db_connection.get(&execution_id).await.unwrap();
        let value = assert_matches!(
            res.into_finished_result().unwrap(),
            SupportedFunctionReturnValue::Ok{
                ok: Some(WastValWithType { value, .. })
             } => value
        );
        assert_eq!(WastVal::String(format!("stubbing {INPUT_PARAM}")), value);

        db_close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn two_delays_in_same_join_set(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        const FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::TWO_DELAYS_IN_SAME_JOIN_SET
        );
        execute_workflow_fn_with_single_delay(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            FFQN,
            Some(Duration::from_millis(10)),
            db,
        )
        .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn join_next_produces_all_processed_error(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        const FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::JOIN_NEXT_PRODUCES_ALL_PROCESSED_ERROR
        );
        execute_workflow_fn_with_single_delay(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            FFQN,
            Some(Duration::from_millis(10)),
            db,
        )
        .await;
    }

    async fn execute_workflow_fn_with_single_delay(
        workflow_wasm_path: &'static str,
        ffqn: FunctionFqn,
        delay: Option<Duration>,
        db: db_tests::Database,
    ) {
        const MAX_RUNS: u128 = 100;

        test_utils::set_up();
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, db_exec, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY),
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY),
            compile_workflow(workflow_wasm_path),
        ]);

        let worker = compile_workflow_worker(
            workflow_wasm_path,
            db_pool.clone(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );
        let exec_task = ExecTask::new_test(
            worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id: ExecutorId::from_parts(0, 0),
                retry_config: ComponentRetryConfig::WORKFLOW_TEST,
            },
            sim_clock.clone(),
            db_exec,
            Arc::new([ffqn.clone()]),
        );

        let execution_id = ExecutionId::from_parts(0, 0);
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: ffqn.clone(),
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let mut run_id = {
            let mut id = 0;
            move || {
                id += 1;
                RunId::from_parts(0, id)
            }
        };
        {
            // First exec tick must run.
            let task_count = exec_task
                .tick_test(sim_clock.now(), run_id())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, task_count);
        }

        if let Some(delay) = delay {
            let pending_state = db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap();

            assert_matches!(pending_state, PendingState::BlockedByJoinSet { .. });

            sim_clock.move_time_forward(delay);
            {
                let timer =
                    expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
                        .await
                        .unwrap();
                assert_eq!(1, timer.expired_async_timers);
            }
        }

        // Keep running exec ticks.
        loop {
            let run = run_id();
            assert!(run.random_part() < MAX_RUNS);
            let executed = exec_task
                .tick_test(sim_clock.now(), run)
                .await
                .wait_for_tasks()
                .await
                .len();
            if executed == 0 {
                break;
            }
            assert_eq!(1, executed);
        }

        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        assert_matches!(
            pending_state,
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind(Ok(())),
                    ..
                }
            }
        );
        let execution_log = db_connection.get(&execution_id).await.unwrap();
        insta::with_settings!({snapshot_suffix => ffqn.to_string().replace(':', "_")}, {insta::assert_json_snapshot!(execution_log)});
        assert_matches!(
            execution_log.into_finished_result(),
            Some(SupportedFunctionReturnValue::Ok { ok: None })
        );

        db_close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn await_next_produces_all_processed_error(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        execute_workflow_fn_with_single_delay(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            FunctionFqn::new_static_tuple(
                test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::AWAIT_NEXT_PRODUCES_ALL_PROCESSED_ERROR
            ),
            None,
            db,
        )
        .await;
    }

    #[rstest::rstest]
    #[case(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::SUBMIT_RACE_JOIN_NEXT_STUB)]
    #[case(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::SUBMIT_RACE_JOIN_NEXT_STUB_ERROR)]
    #[tokio::test]
    async fn stub_submit_race_join_next_stub(
        #[case] ffqn_tuple: (&'static str, &'static str),
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        execute_workflow_fn_with_single_delay(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            FunctionFqn::new_static_tuple(ffqn_tuple),
            None,
            db,
        )
        .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn stub_submit_race_join_next_delay(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        execute_workflow_fn_with_single_delay(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            FunctionFqn::new_static_tuple(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::SUBMIT_RACE_JOIN_NEXT_DELAY),
            Some(Duration::from_millis(10)),
            db,
        )
        .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn stub_join_next_in_scope(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        execute_workflow_fn_with_single_delay(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            FunctionFqn::new_static_tuple(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::JOIN_NEXT_IN_SCOPE),
            None,
            db,
        )
        .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn invoke_expect_execution_error(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        const FFQN_WORKFLOW_STUB: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::INVOKE_EXPECT_EXECUTION_ERROR,
        );
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY),
            compile_workflow(test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW),
        ]);

        let worker = compile_workflow_worker(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();

        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_STUB,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        let exec_task = ExecTask::new_test(
            worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
            },
            sim_clock.clone(),
            db_exec,
            Arc::new([FFQN_WORKFLOW_STUB]),
        );

        {
            let task_count = exec_task
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, task_count);
        }

        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        let join_set_id = assert_matches!(pending_state, PendingState::BlockedByJoinSet { join_set_id, lock_expires_at:_, closing: false } => join_set_id);
        let stub_execution_id = execution_id.next_level(&join_set_id);
        write_stub_response(
            db_connection.as_ref(),
            sim_clock.now(),
            stub_execution_id,
            SupportedFunctionReturnValue::Err { err: None },
        )
        .await;

        // another tick + await should mark the execution finished.
        assert_eq!(
            1,
            exec_task
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len()
        );
        let res = db_connection.get(&execution_id).await.unwrap();
        assert_matches!(
            res.into_finished_result().unwrap(),
            SupportedFunctionReturnValue::Ok { ok: None }
        );
        db_close.close().await;
    }

    async fn write_stub_response(
        db_connection: &dyn DbConnection,
        created_at: DateTime<Utc>,
        stub_execution_id: ExecutionIdDerived,
        result: SupportedFunctionReturnValue,
    ) {
        let (parent_execution_id, join_set_id) = stub_execution_id.split_to_parts().unwrap();
        let stub_finished_version = Version::new(1); // Stub activities have no execution log except Created event.
        let finished_req = AppendRequest {
            created_at,
            event: ExecutionEventInner::Finished {
                result: result.clone(),
                http_client_traces: None,
            },
        };
        db_connection
            .append_batch_respond_to_parent(
                AppendEventsToExecution {
                    execution_id: ExecutionId::Derived(stub_execution_id.clone()),
                    version: stub_finished_version.clone(),
                    batch: vec![finished_req],
                },
                AppendResponseToExecution {
                    parent_execution_id,
                    parent_response_event: JoinSetResponseEventOuter {
                        created_at,
                        event: JoinSetResponseEvent {
                            join_set_id,
                            event: JoinSetResponse::ChildExecutionFinished {
                                child_execution_id: stub_execution_id,
                                finished_version: stub_finished_version.clone(),
                                result,
                            },
                        },
                    },
                },
                created_at,
            )
            .await
            .unwrap();
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn execution_failed_variant_should_work(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
    ) {
        const FFQN_WORKFLOW: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_serde_workflow_builder::exports::testing::serde_workflow::serde_workflow::EXPECT_TRAP,
        );
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_exec, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(
                test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
            ),
            compile_workflow(test_programs_serde_workflow_builder::TEST_PROGRAMS_SERDE_WORKFLOW),
        ]);

        let workflow_worker = compile_workflow_worker(
            test_programs_serde_workflow_builder::TEST_PROGRAMS_SERDE_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();

        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let exec_workflow = ExecTask::new_test(
            workflow_worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
            },
            sim_clock.clone(),
            db_exec.clone(),
            Arc::new([FFQN_WORKFLOW]),
        );

        // 1. Tick workflow executor
        {
            let task_count = exec_workflow
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, task_count);
        }

        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        assert_matches!(
            pending_state,
            PendingState::BlockedByJoinSet {
                join_set_id: _,
                lock_expires_at: _,
                closing: false
            }
        );

        // 2. Tick the activity executor
        {
            let (activity_worker, component_id) = new_activity_worker(
                test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap(),
                sim_clock.clone(),
                TokioSleep,
            );
            let exec_activity = ExecTask::new_all_ffqns_test(
                activity_worker,
                ExecConfig {
                    batch_size: 1,
                    lock_expiry: Duration::from_secs(1),
                    tick_sleep: TICK_SLEEP,
                    component_id,
                    task_limiter: None,
                    executor_id: ExecutorId::generate(),
                    retry_config: ComponentRetryConfig::ZERO,
                },
                sim_clock.clone(),
                db_exec.clone(),
            );
            {
                let task_count = exec_activity
                    .tick_test(sim_clock.now(), RunId::generate())
                    .await
                    .wait_for_tasks()
                    .await
                    .len();
                assert_eq!(1, task_count);
            }
        }

        // 3. Tick workflow executor
        {
            let task_count = exec_workflow
                .tick_test(sim_clock.now(), RunId::generate())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, task_count);
        }

        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        assert_matches!(
            pending_state,
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind(Ok(())),
                    ..
                }
            }
        );
        db_close.close().await;
    }
}
