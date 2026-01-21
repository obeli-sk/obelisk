use super::deadline_tracker::DeadlineTrackerFactory;
use super::event_history::ApplyError;
use super::workflow_ctx::{WorkflowCtx, WorkflowFunctionError};
use crate::activity::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use crate::workflow::caching_db_connection::{CachingBuffer, CachingDbConnection};
use crate::workflow::deadline_tracker::DeadlineTrackerFactoryForReplay;
use crate::workflow::workflow_ctx::{ImportedFnCall, WorkerPartialResult};
use crate::{RunnableComponent, WasmFileError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{ExecutorId, RunId};
use concepts::storage::{DbConnection, DbErrorWrite, DbPool, Locked, Version};
use concepts::time::{ClockFn, ConstClock, now_tokio_instant};
use concepts::{
    ComponentId, ExecutionId, ExecutionMetadata, FunctionFqn, FunctionMetadata, PackageIfcFns,
    ResultParsingError, StrVariant, TrapKind,
};
use concepts::{FunctionRegistry, SupportedFunctionReturnValue};
use db_mem::inmemory_dao::InMemoryPool;
use executor::worker::{FatalError, WorkerContext, WorkerResult, WorkerResultOk};
use executor::worker::{Worker, WorkerError};
use std::future;
use std::ops::Deref;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{Span, debug, error, info, instrument, trace, warn};
use utils::wasm_tools::DecodeError;
use wasmtime::Store;
use wasmtime::component::types::ComponentFunc;
use wasmtime::component::{ComponentExportIndex, InstancePre};
use wasmtime::{Engine, component::Val};

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
    pub lock_extension: Duration,
    pub subscription_interruption: Option<Duration>,
}

pub struct WorkflowWorkerCompiled {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    clock_fn: Box<dyn ClockFn>,
    wasmtime_component: wasmtime::component::Component,
    exported_functions_ext: Vec<FunctionMetadata>,
    exports_hierarchy_ext: Vec<PackageIfcFns>,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    exported_functions_noext: Vec<FunctionMetadata>,
    imported_functions: Vec<FunctionMetadata>,
}

pub struct WorkflowWorkerLinked {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    clock_fn: Box<dyn ClockFn>,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    instance_pre: InstancePre<WorkflowCtx>,
    exported_functions_noext: Vec<FunctionMetadata>,
    fn_registry: Arc<dyn FunctionRegistry>,
}

pub struct WorkflowWorker {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    exported_functions_noext: Vec<FunctionMetadata>,
    db_pool: Arc<dyn DbPool>,
    clock_fn: Box<dyn ClockFn>,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    instance_pre: InstancePre<WorkflowCtx>,
    fn_registry: Arc<dyn FunctionRegistry>,
    cancel_registry: CancelRegistry,
    deadline_factory: Arc<dyn DeadlineTrackerFactory>,
    log_storage_config: Option<LogStrageConfig>,
}

const WASI_NAMESPACE: &str = "wasi";

impl WorkflowWorkerCompiled {
    // If `config.stub_wasi` is set, this function must remove WASI exports and imports.
    pub fn new_with_config(
        runnable_component: RunnableComponent,
        config: WorkflowConfig,
        engine: Arc<Engine>,
        clock_fn: Box<dyn ClockFn>,
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
    ) -> Result<WorkflowWorkerLinked, WasmFileError> {
        let mut linker = wasmtime::component::Linker::new(&self.engine);

        // Link obelisk:workflow-support and obelisk:log
        WorkflowCtx::add_to_linker(&mut linker, self.config.stub_wasi)?;

        // Mock imported functions
        for import in fn_registry
            .all_exports()
            .iter()
            // Skip already linked functions to avoid unexpected behavior and security issues.
            .filter(|import| {
                if import.ifc_fqn.is_namespace_obelisk() {
                    warn!("Skipping mocked import {}", import.ifc_fqn); // FIXME: only if imported
                    false
                } else {
                    true
                }
            })
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
                        move |mut store_ctx: wasmtime::StoreContextMut<'_, WorkflowCtx>,
                              _component_func: ComponentFunc,
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
                        return Err(WasmFileError::linking_error(
                            format!("cannot add mock for imported function {ffqn}"),
                            err,
                        ));
                    }
                }
            } else {
                warn!("Skipping interface {ifc_fqn}", ifc_fqn = import.ifc_fqn);
            }
        }

        // Pre-instantiate to catch missing imports
        let instance_pre = linker
            .instantiate_pre(&self.wasmtime_component)
            .map_err(|err| WasmFileError::linking_error("preinstantiation error", err))?;

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

    #[must_use]
    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        &self.exported_functions_ext
    }

    #[must_use]
    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        &self.exports_hierarchy_ext
    }

    #[must_use]
    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.imported_functions
    }
}

impl WorkflowWorkerLinked {
    pub fn into_worker(
        self,
        db_pool: Arc<dyn DbPool>,
        deadline_factory: Arc<dyn DeadlineTrackerFactory>,
        cancel_registry: CancelRegistry,
        log_storage_config: Option<LogStrageConfig>,
    ) -> WorkflowWorker {
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
            cancel_registry,
            log_storage_config,
        }
    }
}

enum RunError {
    ResultParsingError(ResultParsingError, WorkflowCtx),
    /// Error from the wasmtime runtime that can be downcast to `WorkflowFunctionError`
    WorkerPartialResult(WorkerPartialResult, WorkflowCtx),
    /// Error that happened while running the function.
    Trap {
        reason: String,
        detail: Option<String>,
        workflow_ctx: WorkflowCtx,
        kind: TrapKind,
    },
}

enum WorkerResultRefactored {
    Ok(SupportedFunctionReturnValue, WorkflowCtx),
    DbUpdatedByWorkerOrWatcher,
    FatalError(FatalError, WorkflowCtx),
    DbError(DbErrorWrite),
}

type CallFuncResult = Result<(SupportedFunctionReturnValue, WorkflowCtx), RunError>;

#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("limit reached: {reason}")]
    LimitReached { reason: String, version: Version },
    #[error(transparent)]
    DbError(DbErrorWrite),
    #[error("fatal error: {0}")]
    FatalError(FatalError, Version),
}
impl From<WorkflowError> for WorkerError {
    fn from(value: WorkflowError) -> Self {
        match value {
            WorkflowError::LimitReached { reason, version } => {
                WorkerError::LimitReached { reason, version }
            }
            WorkflowError::DbError(db_err) => WorkerError::DbError(db_err),
            WorkflowError::FatalError(fatal_error, version) => {
                WorkerError::FatalError(fatal_error, version)
            }
        }
    }
}

enum PrepareFuncOk {
    DbUpdatedByWorkerOrWatcher,
    Finished {
        store: Store<WorkflowCtx>,
        func: wasmtime::component::Func,
        component_func: ComponentFunc,
        params: Arc<[Val]>,
    },
}

impl WorkflowWorker {
    async fn prepare_func(&self, ctx: WorkerContext) -> Result<PrepareFuncOk, WorkflowError> {
        assert_eq!(
            self.config.component_id.clone(),
            ctx.locked_event.component_id
        );

        let deadline_tracker = match self
            .deadline_factory
            .create(ctx.locked_event.lock_expires_at)
        {
            Ok(deadline_tracker) => deadline_tracker,
            Err(lock_already_expired) => {
                ctx.worker_span.in_scope(|| {
                    info!(execution_deadline = %ctx.locked_event.lock_expires_at, started_at = %lock_already_expired.started_at,
                        "Lock is already expired");
                });
                return Ok(PrepareFuncOk::DbUpdatedByWorkerOrWatcher);
            }
        };

        let version_at_start = ctx.version.clone();
        let seed = ctx.execution_id.random_seed();
        let db_connection = CachingDbConnection {
            db_connection: self.db_pool.connection().await.unwrap(),
            execution_id: ctx.execution_id.clone(),
            caching_buffer: CachingBuffer::new(self.config.join_next_blocking_strategy),
            version: ctx.version,
        };
        let workflow_ctx = WorkflowCtx::new(
            db_connection,
            ctx.event_history,
            ctx.responses,
            seed,
            self.clock_fn.clone_box(),
            self.config.join_next_blocking_strategy,
            ctx.worker_span,
            self.config.backtrace_persist,
            deadline_tracker,
            self.fn_registry.clone(),
            self.cancel_registry.clone(),
            ctx.locked_event,
            self.config.lock_extension,
            self.config.subscription_interruption,
            self.log_storage_config.clone(),
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
            Ok(wasmtime::UpdateDeadline::YieldCustom(
                1,
                Box::pin(tokio::task::yield_now()),
            ))
        });

        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                let reason = err.to_string();
                let version = store.into_data().db_connection.version;
                if reason.starts_with("maximum concurrent") {
                    return Err(WorkflowError::LimitReached { reason, version });
                }
                return Err(WorkflowError::FatalError(
                    FatalError::CannotInstantiate {
                        reason: format!("{err}"),
                        detail: format!("{err:?}"),
                    },
                    version,
                ));
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
        let component_func = func.ty(&store);
        let params = match ctx.params.as_vals(component_func.params()) {
            Ok(params) => params,
            Err(err) => {
                return Err(WorkflowError::FatalError(
                    FatalError::ParamsParsingError(err),
                    version_at_start,
                ));
            }
        };
        Ok(PrepareFuncOk::Finished {
            store,
            func,
            component_func,
            params,
        })
    }

    async fn call_func(
        mut store: Store<WorkflowCtx>,
        func: wasmtime::component::Func,
        component_func: ComponentFunc,
        params: Arc<[Val]>,
        assigned_fuel: Option<u64>,
    ) -> CallFuncResult {
        let result_types = component_func.results();
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
                match SupportedFunctionReturnValue::new_from_iterator(
                    results.into_iter().zip(result_types),
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
                        .into_worker_partial_result(workflow_ctx.db_connection.version.clone());
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
        store: Store<WorkflowCtx>,
        func: wasmtime::component::Func,
        component_func: ComponentFunc,
        params: Arc<[Val]>,
        worker_span: &Span,
        execution_deadline: DateTime<Utc>,
        assigned_fuel: Option<u64>,
    ) -> Result<WorkerResultOk, WorkflowError> {
        // call_func
        let elapsed = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        let res = Self::call_func(store, func, component_func, params, assigned_fuel).await;
        let elapsed = elapsed.elapsed();
        let worker_result_refactored =
            Self::convert_result(res, worker_span, elapsed, execution_deadline).await;

        match worker_result_refactored {
            WorkerResultRefactored::Ok(retval, mut workflow_ctx) => {
                match Self::close_join_sets(&mut workflow_ctx).await {
                    Ok(CloseJoinSetOk::Ok) => Ok(WorkerResultOk::Finished {
                        retval,
                        version: workflow_ctx.db_connection.version,
                        traces: None,
                    }),
                    Ok(CloseJoinSetOk::DbUpdatedByWorkerOrWatcher) => {
                        Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher)
                    }
                    Err(closing_err) => {
                        debug!("Error while closing join sets {closing_err:?}");
                        Err(closing_err)
                    }
                }
            }
            WorkerResultRefactored::DbUpdatedByWorkerOrWatcher => {
                // Made some progress.
                Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher)
            }
            WorkerResultRefactored::FatalError(err, mut workflow_ctx) => {
                // Even on fatal error we try to cancel activities and wait for workflows.
                match Self::close_join_sets(&mut workflow_ctx).await {
                    Ok(CloseJoinSetOk::Ok | CloseJoinSetOk::DbUpdatedByWorkerOrWatcher) => {
                        // Propagate the original error
                        Err(WorkflowError::FatalError(
                            err,
                            workflow_ctx.db_connection.version,
                        ))
                    }
                    Err(closing_err) => {
                        debug!(
                            "Error {closing_err:?} while closing join sets while handling original {err:?}"
                        );
                        // This can be a temporary db or limit reached error, schedule a retry
                        // to properly close join sets.
                        Err(closing_err)
                    }
                }
            }
            WorkerResultRefactored::DbError(err) => Err(WorkflowError::DbError(err)),
        }
    }

    #[instrument(skip_all, fields(res, worker_span))]
    async fn convert_result(
        res: CallFuncResult,
        worker_span: &Span,
        #[expect(unused_variables)] elapsed: Duration,
        #[expect(unused_variables)] execution_deadline: DateTime<Utc>,
    ) -> WorkerResultRefactored {
        match res {
            Ok((supported_result, mut workflow_ctx)) => {
                worker_span.in_scope(|| info!("Finished"));
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(|| error!("Database error: {db_err}"));
                    return WorkerResultRefactored::DbError(db_err);
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
                    return WorkerResultRefactored::DbError(db_err);
                }
                worker_span.in_scope(|| info!("Trap handled as a fatal error"));
                WorkerResultRefactored::FatalError(
                    FatalError::WorkflowTrap {
                        reason,
                        trap_kind: kind,
                        detail,
                    },
                    workflow_ctx,
                )
            }
            Err(RunError::WorkerPartialResult(worker_partial_result, mut workflow_ctx)) => {
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(||
                        error!("Database flush error: {db_err:?} while handling WorkerPartialResult: {worker_partial_result:?}")
                    );
                    return WorkerResultRefactored::DbError(db_err);
                }
                match worker_partial_result {
                    WorkerPartialResult::FatalError(err, _version) => {
                        worker_span.in_scope(|| info!("Finished with a fatal error: {err}"));
                        WorkerResultRefactored::FatalError(err, workflow_ctx)
                    }
                    WorkerPartialResult::InterruptDbUpdated => {
                        worker_span.in_scope(|| info!("Interrupt requested"));
                        WorkerResultRefactored::DbUpdatedByWorkerOrWatcher
                    }
                    WorkerPartialResult::DbError(db_err) => WorkerResultRefactored::DbError(db_err),
                }
            }
            Err(RunError::ResultParsingError(err, mut workflow_ctx)) => {
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(|| error!("Database error: {db_err}"));
                    return WorkerResultRefactored::DbError(db_err);
                }
                worker_span.in_scope(|| error!("Fatal error: Result parsing error: {err}"));
                WorkerResultRefactored::FatalError(
                    FatalError::ResultParsingError(err),
                    workflow_ctx,
                )
            }
        }
    }

    async fn close_join_sets(
        workflow_ctx: &mut WorkflowCtx,
    ) -> Result<CloseJoinSetOk, WorkflowError> {
        match workflow_ctx.join_sets_close_on_finish().await {
            Ok(()) => Ok(CloseJoinSetOk::Ok),
            Err(ApplyError::InterruptDbUpdated) => Ok(CloseJoinSetOk::DbUpdatedByWorkerOrWatcher),

            Err(ApplyError::DbError(db_error)) => Err(WorkflowError::DbError(db_error)),
            Err(ApplyError::NondeterminismDetected(detail)) => Err(WorkflowError::FatalError(
                FatalError::NondeterminismDetected { detail },
                workflow_ctx.db_connection.version.clone(),
            )),
            Err(ApplyError::ConstraintViolation(reason)) => Err(WorkflowError::FatalError(
                FatalError::ConstraintViolation { reason },
                workflow_ctx.db_connection.version.clone(),
            )),
        }
    }

    async fn run_internal(&self, ctx: WorkerContext) -> Result<WorkerResultOk, WorkflowError> {
        ctx.worker_span.in_scope(|| info!("Execution run started"));
        if !ctx.can_be_retried {
            warn!(
                "Workflow configuration set to not retry anymore. This can lead to nondeterministic results."
            );
        }
        let worker_span = ctx.worker_span.clone();
        let execution_deadline = ctx.locked_event.lock_expires_at;
        match self.prepare_func(ctx).await? {
            PrepareFuncOk::Finished {
                store,
                func,
                component_func,
                params,
            } => {
                Self::call_func_convert_result(
                    store,
                    func,
                    component_func,
                    params,
                    &worker_span,
                    execution_deadline,
                    self.config.fuel,
                )
                .await
            }
            PrepareFuncOk::DbUpdatedByWorkerOrWatcher => {
                Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher)
            }
        }
    }

    #[instrument(skip_all, fields(%execution_id))]
    pub async fn replay(
        component_id: ComponentId,
        runnable_component: RunnableComponent,
        engine: Arc<Engine>,
        fn_registry: Arc<dyn FunctionRegistry>,
        db_conn: &dyn DbConnection,
        execution_id: ExecutionId,
    ) -> Result<(), ReplayError> {
        let clock_fn = ConstClock(DateTime::from_timestamp_nanos(0));

        let config = WorkflowConfig {
            join_next_blocking_strategy: JoinNextBlockingStrategy::Interrupt,
            backtrace_persist: false,
            lock_extension: Duration::ZERO,
            subscription_interruption: None,
            component_id,
            stub_wasi: true, // no harm, stub it in any case
            fuel: None,
        };

        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(DbErrorWrite::from)?;
        let ctx = WorkerContext {
            execution_id,
            metadata: ExecutionMetadata::empty(),
            ffqn: log.ffqn().clone(),
            params: log.params().clone(),
            event_history: log.event_history().collect(),
            responses: log.responses,
            version: log.next_version,
            can_be_retried: true, // Just to avoid "Workflow configuration set to not retry anymore" warning
            worker_span: Span::current(),
            locked_event: Locked {
                component_id: config.component_id.clone(),
                executor_id: ExecutorId::generate(), // TODO: Remove
                run_id: RunId::generate(),
                lock_expires_at: clock_fn.now(), // does not matter, using DeadlineTrackerFactoryForReplay
                retry_config: concepts::ComponentRetryConfig::WORKFLOW,
            },
        };

        let compiled = WorkflowWorkerCompiled::new_with_config(
            runnable_component,
            config,
            engine,
            clock_fn.clone_box(),
        )?;
        let linked = compiled.link(fn_registry)?;
        let db_pool = Arc::new(InMemoryPool::new());
        let log_storage_config = None;
        let worker = linked.into_worker(
            db_pool,
            Arc::new(DeadlineTrackerFactoryForReplay {}),
            CancelRegistry::new(),
            log_storage_config,
        );
        worker
            .run_internal(ctx)
            .await
            .map(|_| ())
            .map_err(ReplayError::from)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseJoinSetOk {
    Ok,
    DbUpdatedByWorkerOrWatcher,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error(transparent)]
    DecodeError(#[from] DecodeError),
    #[error(transparent)]
    LinkError(#[from] WasmFileError),
    // Transient error
    #[error("limit reached: {reason}")]
    LimitReached { reason: String, version: Version },
    // Transient error
    #[error(transparent)]
    DbError(#[from] DbErrorWrite),
    /// Replay failed
    #[error("fatal error: {0}")]
    ReplayFailed(FatalError),
}
impl From<WorkflowError> for ReplayError {
    fn from(value: WorkflowError) -> Self {
        match value {
            WorkflowError::LimitReached { reason, version } => {
                ReplayError::LimitReached { reason, version }
            }
            WorkflowError::DbError(db_error_write) => ReplayError::DbError(db_error_write),
            WorkflowError::FatalError(fatal_error, _version) => {
                ReplayError::ReplayFailed(fatal_error)
            }
        }
    }
}

#[async_trait]
impl Worker for WorkflowWorker {
    fn exported_functions(&self) -> &[FunctionMetadata] {
        &self.exported_functions_noext
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        match self.run_internal(ctx).await {
            Ok(ok) => WorkerResult::from(ok),
            Err(workflow_err) => WorkerResult::Err(WorkerError::from(workflow_err)),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::activity::activity_worker::tests::new_activity;
    use crate::activity::activity_worker::tests::{compile_activity_stub, new_activity_worker};
    use crate::activity::cancel_registry::CancelRegistry;
    use crate::testing_fn_registry::{TestingFnRegistry, fn_registry_dummy};
    use crate::workflow::deadline_tracker::DeadlineTrackerFactoryTokio;
    use crate::{
        activity::activity_worker::tests::{
            FIBO_10_INPUT, FIBO_10_OUTPUT, compile_activity, new_activity_fibo,
        },
        engines::{EngineConfig, Engines},
    };
    use assert_matches::assert_matches;
    use chrono::DateTime;
    use concepts::prefixed_ulid::ExecutionIdDerived;
    use concepts::storage::{
        AppendEventsToExecution, AppendResponseToExecution, ExecutionLog, JoinSetResponse, Locked,
        LockedBy, PendingStateFinishedError,
    };
    use concepts::storage::{AppendRequest, DbConnection, DbPool, ExecutionRequest};
    use concepts::time::TokioSleep;
    use concepts::{
        ComponentRetryConfig, ExecutionFailureKind, ExecutionId, Params,
        SupportedFunctionReturnValue,
    };
    use concepts::{
        ComponentType,
        prefixed_ulid::{ExecutorId, RunId},
        storage::{
            CreateRequest, DbPoolCloseable, PendingState, PendingStateFinished,
            PendingStateFinishedResultKind, Version, wait_for_pending_state_fn,
        },
    };
    use db_tests::Database;
    use executor::executor::{LockingStrategy, extract_exported_ffqns_noext_test};
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
    };
    use rstest::rstest;
    use serde_json::json;
    use std::ops::Deref;
    use std::time::Duration;
    use test_db_macro::expand_enum_database;
    use test_utils::ExecutionLogSanitized;
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
        let component_id = ComponentId::dummy_workflow();
        (
            RunnableComponent::new(wasm_path, engine, component_id.component_type).unwrap(),
            component_id,
        )
    }

    fn new_workflow(
        db_pool: Arc<dyn DbPool>,
        wasm_path: &'static str,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        cancel_registry: CancelRegistry,
    ) -> ExecTask {
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let component_id = ComponentId::dummy_workflow();
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
                    lock_extension: Duration::ZERO,
                    subscription_interruption: None,
                },
                workflow_engine,
                clock_fn.clone_box(),
            )
            .unwrap()
            .link(fn_registry.clone())
            .unwrap()
            .into_worker(
                db_pool.clone(),
                Arc::new(DeadlineTrackerFactoryTokio {
                    leeway: Duration::ZERO,
                    clock_fn: clock_fn.clone_box(),
                }),
                cancel_registry,
                None, // log_storage_config
            ),
        );
        info!("Instantiated worker");
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            component_id,
            task_limiter: None,
            executor_id: ExecutorId::generate(),
            retry_config: ComponentRetryConfig::WORKFLOW,
            locking_strategy: LockingStrategy::default(),
        };
        ExecTask::new_all_ffqns_test(worker, exec_config, clock_fn, db_pool)
    }

    pub(crate) fn new_workflow_fibo(
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        cancel_registry: CancelRegistry,
    ) -> ExecTask {
        new_workflow(
            db_pool,
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            clock_fn,
            join_next_blocking_strategy,
            fn_registry,
            cancel_registry,
        )
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity(
        db: Database,
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = db.set_up().await;
        fibo_workflow_should_submit_fibo_activity_inner(
            db_pool.clone(),
            sim_clock,
            join_next_blocking_strategy,
        )
        .await;
        db_close.close().await;
    }

    async fn fibo_workflow_should_submit_fibo_activity_inner(
        db_pool: Arc<dyn DbPool>,
        sim_clock: SimClock,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        const INPUT_ITERATIONS: u32 = 1;
        test_utils::set_up();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY),
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW),
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            join_next_blocking_strategy,
            &fn_registry,
            cancel_registry,
        );
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params =
            Params::from_json_values_test(vec![json!(FIBO_10_INPUT), json!(INPUT_ITERATIONS)]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBOA_WORKFLOW_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: ComponentId::dummy_workflow(),
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

        let activity_exec = new_activity_fibo(db_pool.clone(), sim_clock.clone_box(), TokioSleep);
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
    #[should_panic(expected = "preinstantiation error")]
    async fn fibo_workflow_with_missing_imports_should_fail() {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, _db_close) = Database::Memory.set_up().await;
        test_utils::set_up();
        let fn_registry = fn_registry_dummy(&[]);
        let cancel_registry = CancelRegistry::new();
        new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            cancel_registry,
        );
    }

    pub(crate) fn compile_workflow_worker(
        wasm_path: &str,
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        cancel_registry: CancelRegistry,
    ) -> Arc<WorkflowWorker> {
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
                    lock_extension: Duration::ZERO,
                    subscription_interruption: None,
                },
                workflow_engine,
                clock_fn.clone_box(),
            )
            .unwrap()
            .link(fn_registry.clone())
            .unwrap()
            .into_worker(
                db_pool,
                Arc::new(DeadlineTrackerFactoryTokio {
                    leeway: Duration::ZERO,
                    clock_fn,
                }),
                cancel_registry,
                None, // log_storage_config
            ),
        )
    }

    #[tokio::test]
    async fn execution_deadline_before_now_should_timeout() {
        const SLEEP_MILLIS: u32 = 100;
        test_utils::set_up();

        let (_guard, db_pool, db_close) = Database::Memory.set_up().await;

        let sim_clock = SimClock::epoch();
        let cancel_registry = CancelRegistry::new();
        let worker = compile_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &TestingFnRegistry::new_from_components(vec![
                compile_activity(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                ),
                compile_workflow(
                    test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                ),
            ]),
            cancel_registry,
        );
        // simulate a scheduling problem where deadline < Now.clone_box(), meaning there is no point in running the execution.
        let execution_deadline = sim_clock.now();
        sim_clock.move_time_forward(Duration::from_millis(100));

        let ctx = WorkerContext {
            execution_id: ExecutionId::generate(),
            metadata: concepts::ExecutionMetadata::empty(),
            ffqn: SLEEP1_HOST_ACTIVITY_FFQN,
            params: Params::from_json_values_test(vec![json!({"milliseconds": SLEEP_MILLIS})]),
            event_history: Vec::new(),
            responses: Vec::new(),
            version: Version::new(0),
            can_be_retried: false,
            worker_span: info_span!("worker-test"),
            locked_event: Locked {
                component_id: ComponentId::dummy_workflow(),
                executor_id: ExecutorId::generate(),
                run_id: RunId::generate(),
                lock_expires_at: execution_deadline,
                retry_config: ComponentRetryConfig::ZERO,
            },
        };
        let worker_result = worker.run(ctx).await;
        assert_matches!(worker_result, WorkerResult::DbUpdatedByWorkerOrWatcher); // Do not write anything, let the watcher mark execution as timed out.
        db_close.close().await;
    }

    #[tokio::test]
    async fn sleep2_happy_path() {
        const SLEEP_SCHEDULE_AT_HOST_ACTIVITY_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SLEEP_SCHEDULE_AT); // sleep-host-activity: func(s: schedule-at);

        const LOCK_DURATION: Duration = Duration::from_secs(1);
        let join_next_blocking_strategy = JoinNextBlockingStrategy::Interrupt;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
        let sim_clock = SimClock::epoch();
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection().await.unwrap();
        {
            let params = Params::from_json_values_test(vec![json!("now")]);
            db_connection
                .create(CreateRequest {
                    created_at: sim_clock.now(),
                    execution_id: execution_id.clone(),
                    ffqn: SLEEP_SCHEDULE_AT_HOST_ACTIVITY_FFQN,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: sim_clock.now(),
                    component_id: ComponentId::dummy_workflow(),
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
            let cancel_registry = CancelRegistry::new();
            let worker = compile_workflow_worker(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                db_pool.clone(),
                sim_clock.clone_box(),
                join_next_blocking_strategy,
                &fn_registry,
                cancel_registry,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_DURATION,
                tick_sleep: Duration::ZERO, // irrelevant here as we call tick manually
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::default(),
            };
            let ffqns = extract_exported_ffqns_noext_test(worker.as_ref());
            ExecTask::new_test(
                worker,
                exec_config,
                sim_clock.clone_box(),
                db_pool.clone(),
                ffqns,
            )
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
                .unwrap()
                .pending_state;
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
                .unwrap()
                .pending_state,
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind::Ok,
                    ..
                },
                ..
            }
        );
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    // TODO: Test await interleaving with timer - execution should finished in one go.
    async fn sleep_should_be_persisted_after_executor_restart(
        database: Database,
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        const SLEEP_MILLIS: u32 = 100;
        const LOCK_DURATION: Duration = Duration::from_secs(1);
        test_utils::set_up();
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY), // not used here
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW),
        ]);

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: SLEEP1_HOST_ACTIVITY_FFQN,
                params: Params::from_json_values_test(vec![json!({"milliseconds": SLEEP_MILLIS})]),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let executor_id = ExecutorId::generate();
        let sleep_exec = {
            let cancel_registry = CancelRegistry::new();
            let worker = compile_workflow_worker(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                db_pool.clone(),
                sim_clock.clone_box(),
                join_next_blocking_strategy,
                &fn_registry,
                cancel_registry,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_DURATION,
                tick_sleep: Duration::ZERO, // irrelevant here as we call tick manually
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id,
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::default(),
            };
            let ffqns = extract_exported_ffqns_noext_test(worker.as_ref());
            ExecTask::new_test(
                worker,
                exec_config.clone(),
                sim_clock.clone_box(),
                db_pool.clone(),
                ffqns,
            )
        };

        let run_id_first = RunId::generate();
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now(), run_id_first)
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
                .unwrap()
                .pending_state;
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
                .unwrap()
                .pending_state;

            let (actual_pending_at, found_executor_id, found_run_id) = assert_matches!(
                pending_state,
                PendingState::PendingAt {
                    scheduled_at,
                    last_lock: Some(LockedBy { executor_id, run_id }),
                }
                => (scheduled_at, executor_id, run_id)
            );
            assert_eq!(executor_id, found_executor_id);
            assert_eq!(run_id_first, found_run_id);
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
                .unwrap()
                .pending_state,
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind::Ok,
                    ..
                },
                ..
            }
        );
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn stargazers_should_be_deserialized_after_interrupt(db: Database) {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool, db_close) = db.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection().await.unwrap();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
        ]);
        let activity_exec = new_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone_box(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
        );

        let workflow_exec = new_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
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
                component_id: ComponentId::dummy_workflow(),
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
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn http_get(
        db: Database,
        #[values(
            FFQN_WORKFLOW_HTTP_GET,
            FFQN_WORKFLOW_HTTP_GET_RESP,
            FFQN_WORKFLOW_HTTP_GET_SUCCESSFUL
        )]
        ffqn: FunctionFqn,
    ) {
        const BODY: &str = "ok";

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool, db_close) = db.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection().await.unwrap();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
        ]);
        let activity_exec = new_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone_box(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
        );

        let workflow_exec = new_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
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
        let params = Params::from_json_values_test(vec![json!(url)]);
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
                component_id: ComponentId::dummy_workflow(),
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
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn http_get_concurrent(db: db_tests::Database) {
        const BODY: &str = "ok";
        const GET_SUCCESSFUL_CONCURRENTLY_STRESS: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL_CONCURRENTLY_STRESS);

        test_utils::set_up();
        let concurrency = 5;
        let sim_clock = SimClock::new(DateTime::default());
        let created_at = sim_clock.now();
        let (_guard, db_pool, db_close) = db.set_up().await;
        let db_connection = db_pool.connection().await.unwrap();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
        ]);

        let activity_exec = new_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone_box(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
        );
        let workflow_exec = new_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        );
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .mount(&server)
            .await;
        debug!("started mock server on {}", server.address());
        let url = format!("http://127.0.0.1:{}/", server.address().port());
        let params = Params::from_json_values_test(vec![json!(url), json!(concurrency)]);
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
                component_id: ComponentId::dummy_workflow(),
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
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn scheduling_should_work(
        db: db_tests::Database,
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
        let (_guard, db_pool, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY),
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW),
        ]);
        let worker = compile_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            join_next_strategy,
            &fn_registry,
            CancelRegistry::new(),
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection().await.unwrap();

        let params = Params::from_json_values_test(vec![
            json!({"milliseconds": SLEEP_DURATION.as_millis()}),
        ]);
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_SLEEP_SCHEDULE_NOOP_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: ComponentId::dummy_workflow(),
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
                locking_strategy: LockingStrategy::default(),
            },
            sim_clock.clone_box(),
            db_pool.clone(),
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
        let res = db_pool
            .connection_test()
            .await
            .unwrap()
            .get(&execution_id)
            .await
            .unwrap();
        assert_matches!(
            res.as_finished_result().unwrap(),
            SupportedFunctionReturnValue::Ok { ok: None }
        );
        sim_clock.move_time_forward(SLEEP_DURATION);
        // The scheduled `noop` execution should be pending.
        let mut next_pending = db_pool
            .connection()
            .await
            .unwrap()
            .lock_pending_by_ffqns(
                10,
                sim_clock.now(),
                Arc::from([FFQN_ACTIVITY_SLEEP_NOOP_FFQN]),
                sim_clock.now(),
                ComponentId::dummy_workflow(),
                ExecutorId::generate(),
                sim_clock.now() + Duration::from_secs(1),
                RunId::generate(),
                ComponentRetryConfig::WORKFLOW,
            )
            .await
            .unwrap();
        assert_eq!(1, next_pending.len());
        let next_pending = next_pending.pop().unwrap();
        assert!(next_pending.parent.is_none());
        let params = serde_json::to_string(&Params::empty()).unwrap();
        assert_eq!(params, serde_json::to_string(&next_pending.params).unwrap());
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn http_get_fallible_err(database: Database) {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool, db_close) = database.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection().await.unwrap();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
        ]);
        let activity_exec = new_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone_box(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
        );

        let workflow_exec = new_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        );

        let url = "http://";
        let params = Params::from_json_values_test(vec![json!(url)]);
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
                component_id: ComponentId::dummy_workflow(),
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
        assert_matches!(res, SupportedFunctionReturnValue::Err { err: Some(_) });
        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap()
            .pending_state;
        assert_matches!(
            pending_state,
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind::Err(
                        PendingStateFinishedError::Error
                    ),
                    ..
                },
            }
        );
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn stubbing_should_work(db: db_tests::Database) {
        const FFQN_WORKFLOW_STUB: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::SUBMIT_STUB_AWAIT,
        );
        const INPUT_PARAM: &str = "bar";
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY),
            compile_workflow(test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW),
        ]);

        let worker = compile_workflow_worker(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params =
            Params::from_json_values_test(vec![serde_json::Value::String(INPUT_PARAM.to_string())]);
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_STUB,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        let executor_id = ExecutorId::generate();
        let exec_task = ExecTask::new_test(
            worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
                executor_id,
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::default(),
            },
            sim_clock.clone_box(),
            db_pool.clone(),
            Arc::new([FFQN_WORKFLOW_STUB]),
        );

        let run_id = RunId::generate();
        {
            let task_count = exec_task
                .tick_test(sim_clock.now(), run_id)
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, task_count);
        }

        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap()
            .pending_state;
        let (scheduled_at, found_executor_id, found_run_id) = assert_matches!(pending_state,
            PendingState::PendingAt {
                scheduled_at,
                last_lock: Some(LockedBy { executor_id, run_id }),
            }
            => (scheduled_at, executor_id, run_id));
        assert_eq!(sim_clock.now(), scheduled_at);
        assert_eq!(executor_id, found_executor_id);
        assert_eq!(run_id, found_run_id);

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
            res.as_finished_result().unwrap(),
            SupportedFunctionReturnValue::Ok{
                ok: Some(WastValWithType { value, .. })
             } => value
        );
        assert_eq!(WastVal::String(format!("stubbing {INPUT_PARAM}")), value);
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn two_delays_in_same_join_set(db: db_tests::Database) {
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

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn join_next_produces_all_processed_error(db: db_tests::Database) {
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
    ) -> ExecutionLog {
        const MAX_RUNS: u128 = 100;

        test_utils::set_up();
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY),
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY),
            compile_workflow(workflow_wasm_path),
        ]);

        let worker = compile_workflow_worker(
            workflow_wasm_path,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
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
                retry_config: ComponentRetryConfig::WORKFLOW,
                locking_strategy: LockingStrategy::default(),
            },
            sim_clock.clone_box(),
            db_pool.clone(),
            Arc::new([ffqn.clone()]),
        );

        let execution_id = ExecutionId::from_parts(0, 0);
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: ffqn.clone(),
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: ComponentId::dummy_workflow(),
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
            // First workflow exec tick must run.
            let task_count = exec_task
                .tick_test(sim_clock.now(), run_id())
                .await
                .wait_for_tasks()
                .await
                .len();
            assert_eq!(1, task_count);
        }

        if let Some(delay) = delay {
            // If delay is expected, move time forward and tick expired timers watcher
            let pending_state = db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap()
                .pending_state;

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
            .unwrap()
            .pending_state;
        assert_matches!(pending_state, PendingState::Finished { .. });
        let execution_log = db_connection.get(&execution_id).await.unwrap();
        insta::with_settings!({snapshot_suffix => ffqn.to_string().replace(':', "_")},
            {insta::assert_json_snapshot!(ExecutionLogSanitized::from(execution_log.clone()))});
        drop(db_connection);
        db_close.close().await;
        execution_log
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn await_next_produces_all_processed_error(db: db_tests::Database) {
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
    #[expand_enum_database]
    #[rstest]
    #[case(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::SUBMIT_RACE_JOIN_NEXT_STUB)]
    #[case(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::SUBMIT_RACE_JOIN_NEXT_STUB_ERROR)]
    #[tokio::test]
    async fn stub_submit_race_join_next_stub(
        db: db_tests::Database,
        #[case] ffqn_tuple: (&'static str, &'static str),
    ) {
        execute_workflow_fn_with_single_delay(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            FunctionFqn::new_static_tuple(ffqn_tuple),
            None,
            db,
        )
        .await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn stub_submit_race_join_next_delay(db: db_tests::Database) {
        execute_workflow_fn_with_single_delay(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            FunctionFqn::new_static_tuple(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::SUBMIT_RACE_JOIN_NEXT_DELAY),
            Some(Duration::from_millis(10)),
            db,
        )
        .await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn stub_join_next_in_scope(db: db_tests::Database) {
        execute_workflow_fn_with_single_delay(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            FunctionFqn::new_static_tuple(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::JOIN_NEXT_IN_SCOPE),
            None,
            db,
        )
        .await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn invoke_expect_execution_error(db: db_tests::Database) {
        const FFQN_WORKFLOW_STUB: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::INVOKE_EXPECT_EXECUTION_ERROR,
        );
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY),
            compile_workflow(test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW),
        ]);

        let worker = compile_workflow_worker(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection_test().await.unwrap();

        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_STUB,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: ComponentId::dummy_workflow(),
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
                locking_strategy: LockingStrategy::default(),
            },
            sim_clock.clone_box(),
            db_pool.clone(),
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
            .unwrap()
            .pending_state;
        let join_set_id = assert_matches!(pending_state,
            PendingState::BlockedByJoinSet {
                join_set_id,
                lock_expires_at:_,
                closing: false,
            } => join_set_id);
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
            res.as_finished_result().unwrap(),
            SupportedFunctionReturnValue::Ok { ok: None }
        );
        drop(db_connection);
        db_close.close().await;
    }

    async fn write_stub_response(
        db_connection: &dyn DbConnection,
        created_at: DateTime<Utc>,
        stub_execution_id: ExecutionIdDerived,
        result: SupportedFunctionReturnValue,
    ) {
        let (parent_execution_id, join_set_id) = stub_execution_id.split_to_parts();
        let stub_finished_version = Version::new(1); // Stub activities have no execution log except Created event.
        let finished_req = AppendRequest {
            created_at,
            event: ExecutionRequest::Finished {
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
                    created_at,
                    join_set_id,
                    child_execution_id: stub_execution_id,
                    finished_version: stub_finished_version.clone(),
                    result,
                },
                created_at,
            )
            .await
            .unwrap();
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn activity_trap_should_be_converted_as_custom_err_execution_failed_variant(
        db: db_tests::Database,
    ) {
        const EXPECT_TRAP_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_serde_workflow_builder::exports::testing::serde_workflow::serde_workflow::EXPECT_TRAP,
        );
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(
                test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
            ),
            compile_workflow(test_programs_serde_workflow_builder::TEST_PROGRAMS_SERDE_WORKFLOW),
        ]);

        let workflow_worker = compile_workflow_worker(
            test_programs_serde_workflow_builder::TEST_PROGRAMS_SERDE_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection().await.unwrap();

        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: EXPECT_TRAP_FFQN,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: ComponentId::dummy_workflow(),
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
                locking_strategy: LockingStrategy::default(),
            },
            sim_clock.clone_box(),
            db_pool.clone(),
            Arc::new([EXPECT_TRAP_FFQN]),
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
            .unwrap()
            .pending_state;
        assert_matches!(
            pending_state,
            PendingState::BlockedByJoinSet {
                join_set_id: _,
                lock_expires_at: _,
                closing: false,
            }
        );

        // 2. Tick the activity executor
        {
            let (activity_worker, component_id) = new_activity_worker(
                test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap(),
                sim_clock.clone_box(),
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
                    locking_strategy: LockingStrategy::default(),
                },
                sim_clock.clone_box(),
                db_pool.clone(),
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
            .unwrap()
            .pending_state;
        assert_matches!(
            pending_state,
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind::Ok,
                    ..
                },
            }
        );
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn sleep_activity_submit_should_cancel_the_activity(db: db_tests::Database) {
        let execution_log = execute_workflow_fn_with_single_delay(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SLEEP_ACTIVITY_SUBMIT),
            None,
            db,
        )
        .await;
        // There must be a single activity that is already cancelled
        assert_eq!(1, execution_log.responses.len());
        let resp = execution_log.responses.into_iter().next().unwrap();
        let result = assert_matches!(
            resp.event.event.event,
            JoinSetResponse::ChildExecutionFinished { result, .. } => result
        );
        let result =
            assert_matches!(result, SupportedFunctionReturnValue::ExecutionError(err) => err);
        assert_matches!(result.kind, ExecutionFailureKind::Cancelled);
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn sleep_activity_submit_then_trap_should_cancel_the_activity(db: db_tests::Database) {
        let execution_log = execute_workflow_fn_with_single_delay(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SLEEP_ACTIVITY_SUBMIT_THEN_TRAP),
            None,
            db,
        )
        .await;
        // There must be a single activity that is already cancelled
        assert_eq!(1, execution_log.responses.len());
        let resp = execution_log.responses.into_iter().next().unwrap();
        let result = assert_matches!(
            resp.event.event.event,
            JoinSetResponse::ChildExecutionFinished { result, .. } => result
        );
        let result =
            assert_matches!(result, SupportedFunctionReturnValue::ExecutionError(err) => err);
        assert_matches!(result.kind, ExecutionFailureKind::Cancelled);
    }
}
