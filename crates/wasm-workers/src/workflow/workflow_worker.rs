use super::deadline_tracker::DeadlineTrackerFactory;
use super::event_history::ApplyError;
use super::replay_advance::merge_requested_overrides_into_fresh_prefix;
use super::workflow_ctx::{WorkflowCtx, WorkflowFunctionError};
use crate::activity::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use crate::workflow::caching_db_connection::{
    CachingBuffer, CachingDbConnection, WorkflowDbConnection,
};
use crate::workflow::deadline_tracker::EpochCallbackError;
pub use crate::workflow::replay_advance::{
    AdvanceError, ReplayAdvanceable, ReplayError, ReplayResponse,
};
use crate::workflow::replay_advance::{AdvanceFromLogError, AdvanceResponse, ReplayInternalError};
use crate::workflow::replay_db_proxy::{
    InternalCapturedWrite, ReplayWorkflowDbConnection, apply_writes, bump_versions_for_execution,
};
use crate::workflow::workflow_ctx::{ImportedFnCall, ReplayKind, WorkerPartialResult};
use crate::{RunnableComponent, WasmFileError};
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DeploymentId, ExecutorId, RunId};
use concepts::storage::{
    AppendRequest, CapturedDbWrite, ComponentUpgradeOutcome, ComponentUpgradeReason, DbConnection,
    DbErrorWrite, DbPool, ExecutionLog, ExecutionRequest, Locked, Unlocked, Version,
};
use concepts::time::{ClockFn, now_tokio_instant};
use concepts::{
    ComponentId, ExecutionId, ExecutionMetadata, FinishedExecutionFailure, FunctionFqn,
    FunctionMetadata, JoinSetId, PackageIfcFns, Params, ResultParsingError, StrVariant, TrapKind,
};
use concepts::{FunctionRegistry, SupportedFunctionReturnValue};
use executor::worker::{FatalError, RunFinished, WorkerContext, WorkerResult, WorkerResultOk};
use executor::worker::{Worker, WorkerError};
use itertools::Either;
use std::future;
use std::ops::Deref;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{Span, debug, error, info, instrument, trace, warn};
use utils::wasm_tools::{DecodeError, ExIm};
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
    // Only applicable if `join_next_blocking_strategy` is `JoinNextBlockingStrategy::Await`.
    pub lock_extension: Option<Duration>,
    // Only applicable if `join_next_blocking_strategy` is `JoinNextBlockingStrategy::Await`.
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
    deployment_id: DeploymentId,
    pub(crate) config: WorkflowConfig,
    engine: Arc<Engine>,
    exported_functions_noext: Vec<FunctionMetadata>,
    pub(crate) db_pool: Arc<dyn DbPool>,
    clock_fn: Box<dyn ClockFn>,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    instance_pre: InstancePre<WorkflowCtx>,
    fn_registry: Arc<dyn FunctionRegistry>,
    pub(crate) cancel_registry: CancelRegistry,
    pub(crate) deadline_factory: Arc<dyn DeadlineTrackerFactory>,
    pub(crate) logs_storage_config: Option<LogStrageConfig>,
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
        Self::new_with_config_inner(
            runnable_component.wasmtime_component,
            &runnable_component.wasm_component.exim,
            config,
            engine,
            clock_fn,
        )
    }

    pub(crate) fn new_with_config_inner(
        wasmtime_component: wasmtime::component::Component,
        exim: &ExIm,
        config: WorkflowConfig,
        engine: Arc<Engine>,
        clock_fn: Box<dyn ClockFn>,
    ) -> Result<Self, DecodeError> {
        let exported_functions_ext = exim
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
        let exports_hierarchy_ext = exim
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

        let mut exported_ffqn_to_index =
            RunnableComponent::index_exported_functions(&wasmtime_component, exim)?;
        exported_ffqn_to_index.retain(|ffqn, _| {
            if config.stub_wasi {
                // Hide wasi exports
                ffqn.ifc_fqn.namespace() != WASI_NAMESPACE
            } else {
                true
            }
        });

        let exported_functions_noext = exim
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

        let imported_functions = exim
            .imports_flat
            .iter()
            .filter(|fn_meta| {
                if config.stub_wasi {
                    // Hide wasi imports
                    fn_meta.ffqn.ifc_fqn.namespace() != WASI_NAMESPACE
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        Ok(Self {
            config,
            engine,
            clock_fn,
            wasmtime_component,
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
                    warn!("Skipping system import {}", import.ifc_fqn);
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
            match linker.instance(import.ifc_fqn.deref()) {
                Ok(mut linker_instance) => {
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
                }
                Err(err) => {
                    warn!(
                        "Skipping interface {ifc_fqn} - {err:?}",
                        ifc_fqn = import.ifc_fqn
                    );
                }
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
        deployment_id: DeploymentId,
        db_pool: Arc<dyn DbPool>,
        deadline_factory: Arc<dyn DeadlineTrackerFactory>,
        cancel_registry: CancelRegistry,
        logs_storage_config: Option<LogStrageConfig>,
    ) -> WorkflowWorker {
        WorkflowWorker {
            deployment_id,
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
            logs_storage_config,
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
    DbUpdatedByWorkerOrWatcher(WorkflowCtx),
    FatalError(FatalError, WorkflowCtx),
    DbError(DbErrorWrite),
    LockExpired(WorkflowCtx),
    ExecutorClosing(WorkflowCtx),
    ReplayInterrupt(WorkflowCtx),
}

type CallFuncResult = Result<(SupportedFunctionReturnValue, WorkflowCtx), RunError>;

#[derive(derive_more::Debug, thiserror::Error)]
enum WorkflowError {
    #[error("limit reached: {reason}")]
    LimitReached { reason: String, version: Version },
    #[error(transparent)]
    DbError(DbErrorWrite),
    #[error("fatal error: {err}")]
    FatalError {
        err: FatalError,
        #[debug(skip)]
        db_connection: Box<dyn WorkflowDbConnection>,
    },
    #[error("lock expired")]
    LockExpired(Version),
    #[error("executor closing")]
    ExecutorClosing(Version),
}
impl From<WorkflowError> for WorkerError {
    fn from(value: WorkflowError) -> Self {
        match value {
            WorkflowError::LimitReached { reason, version } => {
                WorkerError::LimitReached { reason, version }
            }
            WorkflowError::DbError(db_err) => WorkerError::DbError(db_err),
            WorkflowError::FatalError { err, db_connection } => {
                WorkerError::FatalError(err, db_connection.version().clone())
            }
            WorkflowError::LockExpired(version) => WorkerError::TemporaryTimeout {
                http_client_traces: None,
                version,
            },
            WorkflowError::ExecutorClosing(version) => WorkerError::ExecutorClosing(version),
        }
    }
}

#[derive(derive_more::Debug, thiserror::Error)]
pub enum JoinSetCloseError {
    #[error(transparent)]
    DbError(DbErrorWrite),
    #[error("fatal error: {err}")]
    FatalError { err: FatalError },
    #[error("executor closing")]
    ExecutorClosing(Version),
}
impl JoinSetCloseError {
    fn into_workflow_error(
        self,
        db_connection: Box<dyn WorkflowDbConnection + 'static>,
    ) -> WorkflowError {
        match self {
            JoinSetCloseError::DbError(db_error_write) => WorkflowError::DbError(db_error_write),
            JoinSetCloseError::FatalError { err } => {
                WorkflowError::FatalError { err, db_connection }
            }
            JoinSetCloseError::ExecutorClosing(version) => WorkflowError::ExecutorClosing(version),
        }
    }
}

struct PrepareFuncFinished {
    store: Store<WorkflowCtx>,
    func: wasmtime::component::Func,
    component_func: ComponentFunc,
    params: Arc<[Val]>,
}

struct ReplayInterrupt;

impl WorkflowWorker {
    pub(crate) async fn capture_replay_writes_from_log(
        &self,
        execution_id: ExecutionId,
        log: ExecutionLog,
        ffqn: FunctionFqn,
        params: Params,
        real_connection: Box<dyn DbConnection>,
    ) -> Result<
        (
            Vec<InternalCapturedWrite>,
            Option<FatalError>,
            Box<dyn DbConnection>,
        ),
        ReplayInternalError,
    > {
        let replay_kind = if log.is_finished() {
            ReplayKind::Finished
        } else {
            ReplayKind::Unfinished
        };
        debug!("Execution replay of kind `{replay_kind}` started");

        let (_executor_close_sender, executor_close_watcher) = tokio::sync::watch::channel(false);
        let parent = log.parent();

        let ctx = WorkerContext {
            execution_id: execution_id.clone(),
            metadata: ExecutionMetadata::empty(),
            component_digest: self.config.component_id.component_digest.clone(),
            ffqn,
            params,
            event_history: log.event_history().collect(),
            responses: log.responses,
            parent: parent.clone(),
            version: log.next_version.clone(),
            can_be_retried: true, // Avoid retryability warnings during replay/advance.
            worker_span: Span::current(),
            locked_event: Locked {
                component_id: self.config.component_id.clone(),
                deployment_id: self.deployment_id,
                executor_id: ExecutorId::generate(),
                run_id: RunId::generate(),
                lock_expires_at: self.clock_fn.now(),
                retry_config: concepts::ComponentRetryConfig::WORKFLOW,
            },
            executor_close_watcher,
        };

        self.replay_internal(
            ctx,
            replay_kind,
            execution_id,
            log.next_version,
            real_connection,
            parent,
        )
        .await
        .map(|(writes, replay_end, db_conn)| {
            (
                writes,
                match replay_end {
                    ReplayPendingState::FinishedWithFailure(fatal_error) => Some(fatal_error),
                    _ => None,
                },
                db_conn,
            )
        })
    }

    pub(crate) async fn advance_from_log(
        db_conn: &dyn DbConnection,
        cancel_registry: &CancelRegistry,
        log_forwarder_sender: Option<
            &tokio::sync::mpsc::Sender<concepts::storage::LogInfoAppendRow>,
        >,
        requested: ReplayAdvanceable,
        fresh_replay: Vec<InternalCapturedWrite>,
        old_version: Version,
    ) -> Result<AdvanceResponse, AdvanceFromLogError> {
        assert!(
            !requested.captured_writes.is_empty(),
            "caller must have checked for NoWrites error"
        );
        let fresh_public_writes: Vec<_> = fresh_replay
            .iter()
            .map(|write| write.write.clone())
            .collect();
        if !requested.is_prefix_of(&fresh_public_writes) {
            debug!(
                "Mismatch between expected and actual captured writes. Requested: {:?}, fresh replay produced: {fresh_public_writes:?}",
                requested.captured_writes,
            );
            return Err(AdvanceFromLogError::ReplayMismatch);
        }

        let writes_to_apply =
            merge_requested_overrides_into_fresh_prefix(&requested.captured_writes, &fresh_replay);
        let _version = apply_writes(
            db_conn,
            cancel_registry,
            log_forwarder_sender,
            writes_to_apply,
            old_version,
        )
        .await?;
        let outcome = AdvanceResponse {
            finished: requested.get_return_value().cloned(),
        };
        info!(
            "Advance written {} captured writes{}",
            requested.captured_writes.len(),
            if outcome.finished.is_some() {
                " finished the execution"
            } else {
                ""
            }
        );
        Ok(outcome)
    }

    async fn prepare_func(
        &self,
        ctx: WorkerContext,
        db_connection: Box<dyn WorkflowDbConnection>,
        is_replay: Option<ReplayKind>,
    ) -> Result<PrepareFuncFinished, WorkflowError> {
        assert_eq!(self.config.component_id, ctx.locked_event.component_id);

        let deadline_tracker = match self
            .deadline_factory
            .create(ctx.locked_event.lock_expires_at, ctx.executor_close_watcher)
        {
            Ok(deadline_tracker) => deadline_tracker,
            Err(lock_already_expired) => {
                ctx.worker_span.in_scope(|| {
                    info!(execution_deadline = %ctx.locked_event.lock_expires_at, started_at = %lock_already_expired.started_at,
                        "Lock is already expired");
                });
                return Err(WorkflowError::LockExpired(ctx.version));
            }
        };

        let seed = ctx.execution_id.random_seed();
        let workflow_ctx = WorkflowCtx::new(
            self.deployment_id,
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
            self.logs_storage_config.clone(),
            is_replay,
        );

        let mut store = Store::new(&self.engine, workflow_ctx);

        // Set fuel.
        if let Some(fuel) = self.config.fuel {
            store
                .set_fuel(fuel)
                .expect("engine must have `consume_fuel` enabled");
        }

        // Configure epoch callback before running the initialization to avoid interruption
        store.epoch_deadline_callback(|store_ctx| {
            let ctx = store_ctx.data();
            match ctx.check_epoch_callback() {
                Ok(()) => Ok(wasmtime::UpdateDeadline::YieldCustom(
                    1,
                    Box::pin(tokio::task::yield_now()),
                )),
                Err(EpochCallbackError::LockExpired) => {
                    info!("Deadline reached in epoch callback");
                    Err(wasmtime::Error::from(WorkflowFunctionError::LockExpired))
                }
                Err(EpochCallbackError::ExecutorClosing) => {
                    info!("Executor closing detected in epoch callback");
                    Err(wasmtime::Error::from(
                        WorkflowFunctionError::ExecutorClosing,
                    ))
                }
            }
        });

        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                let reason = err.to_string();
                let db_connection = store.into_data().db_connection;
                let version = db_connection.version().clone();
                if reason.starts_with("maximum concurrent") {
                    return Err(WorkflowError::LimitReached { reason, version });
                }
                return Err(WorkflowError::FatalError {
                    err: FatalError::CannotInstantiate {
                        reason: format!("cannot instantiate: {err}"),
                        detail: Some(format!("{err:?}")),
                    },
                    db_connection,
                });
            }
        };

        let func = {
            let Some(fn_export_index) = self.exported_ffqn_to_index.get(&ctx.ffqn) else {
                let db_connection = store.into_data().db_connection;
                return Err(WorkflowError::FatalError {
                    err: FatalError::CannotInstantiate {
                        reason: format!(
                            "function {} not found in exports of {}",
                            ctx.ffqn, self.config.component_id
                        ),
                        detail: None,
                    },
                    db_connection,
                });
            };
            instance
                .get_func(&mut store, fn_export_index)
                .expect("exported function must be found")
        };
        let component_func = func.ty(&store);
        let params = match ctx.params.as_vals(component_func.params()) {
            Ok(params) => params,
            Err(err) => {
                let db_connection = store.into_data().db_connection;
                return Err(WorkflowError::FatalError {
                    err: FatalError::ParamsParsingError(err),
                    db_connection,
                });
            }
        };
        Ok(PrepareFuncFinished {
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
                        .into_worker_partial_result(workflow_ctx.db_connection.version().clone());
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
    ) -> Result<
        (
            Either<WorkerResultOk, ReplayInterrupt>,
            Box<dyn WorkflowDbConnection>,
        ),
        WorkflowError,
    > {
        // call_func
        let elapsed = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        let res = Self::call_func(store, func, component_func, params, assigned_fuel).await;
        let elapsed = elapsed.elapsed();
        let worker_result_refactored =
            Self::convert_result(res, worker_span, elapsed, execution_deadline).await;

        match worker_result_refactored {
            WorkerResultRefactored::Ok(retval, mut workflow_ctx) => {
                match Self::close_join_sets(&mut workflow_ctx).await {
                    Ok(Either::Left(CloseJoinSetOk::Ok)) => Ok((
                        Either::Left(WorkerResultOk::RunFinished(RunFinished {
                            retval,
                            version: workflow_ctx.db_connection.version().clone(),
                            http_client_traces: None,
                        })),
                        workflow_ctx.db_connection,
                    )),
                    Ok(Either::Left(CloseJoinSetOk::DbUpdatedByWorkerOrWatcher)) => Ok((
                        Either::Left(WorkerResultOk::DbUpdatedByWorkerOrWatcher),
                        workflow_ctx.db_connection,
                    )),
                    Ok(Either::Right(replay_response)) => {
                        Ok((Either::Right(replay_response), workflow_ctx.db_connection))
                    }
                    Err(closing_err) => {
                        debug!("Error while closing join sets {closing_err:?}");
                        Err(closing_err.into_workflow_error(workflow_ctx.db_connection))
                    }
                }
            }
            WorkerResultRefactored::DbUpdatedByWorkerOrWatcher(workflow_ctx) => {
                // Made some progress.
                Ok((
                    Either::Left(WorkerResultOk::DbUpdatedByWorkerOrWatcher),
                    workflow_ctx.db_connection,
                ))
            }
            WorkerResultRefactored::FatalError(err, mut workflow_ctx) => {
                // Even on fatal error we try to cancel activities and wait for workflows.
                match Self::close_join_sets(&mut workflow_ctx).await {
                    Ok(_) => {
                        // Propagate the original error
                        Err(WorkflowError::FatalError {
                            err,
                            db_connection: workflow_ctx.db_connection,
                        })
                    }
                    Err(closing_err) => {
                        debug!(
                            "Error {closing_err:?} while closing join sets while handling original {err:?}"
                        );
                        // This can be a temporary db or limit reached error, schedule a retry
                        // to properly close join sets.
                        Err(closing_err.into_workflow_error(workflow_ctx.db_connection))
                    }
                }
            }
            WorkerResultRefactored::DbError(err) => Err(WorkflowError::DbError(err)),
            WorkerResultRefactored::LockExpired(mut workflow_ctx) => {
                workflow_ctx.flush().await.map_err(WorkflowError::DbError)?;
                Err(WorkflowError::LockExpired(
                    workflow_ctx.db_connection.version().clone(),
                ))
            }
            WorkerResultRefactored::ExecutorClosing(mut workflow_ctx) => {
                workflow_ctx.flush().await.map_err(WorkflowError::DbError)?;
                Err(WorkflowError::ExecutorClosing(
                    workflow_ctx.db_connection.version().clone(),
                ))
            }
            WorkerResultRefactored::ReplayInterrupt(workflow_ctx) => {
                Ok((Either::Right(ReplayInterrupt), workflow_ctx.db_connection))
            }
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
                worker_span.in_scope(|| debug!("Finished"));
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
                worker_span.in_scope(|| debug!("Trap handled as a fatal error"));
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
                        worker_span.in_scope(|| debug!("Finished with a fatal error: {err}"));
                        WorkerResultRefactored::FatalError(err, workflow_ctx)
                    }
                    WorkerPartialResult::InterruptDbUpdated => {
                        worker_span.in_scope(|| debug!("Interrupt requested"));
                        WorkerResultRefactored::DbUpdatedByWorkerOrWatcher(workflow_ctx)
                    }
                    WorkerPartialResult::DbError(db_err) => WorkerResultRefactored::DbError(db_err),
                    WorkerPartialResult::LockExpired => {
                        // logged in epoch callback
                        WorkerResultRefactored::LockExpired(workflow_ctx)
                    }
                    WorkerPartialResult::ExecutorClosing => {
                        // logged in epoch callback
                        WorkerResultRefactored::ExecutorClosing(workflow_ctx)
                    }
                    WorkerPartialResult::ReplayWaitingForResponse => {
                        WorkerResultRefactored::ReplayInterrupt(workflow_ctx)
                    }
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
    ) -> Result<Either<CloseJoinSetOk, ReplayInterrupt>, JoinSetCloseError> {
        match workflow_ctx.join_sets_close_on_finish().await {
            Ok(()) => Ok(Either::Left(CloseJoinSetOk::Ok)),
            Err(ApplyError::InterruptDbUpdated) => {
                Ok(Either::Left(CloseJoinSetOk::DbUpdatedByWorkerOrWatcher))
            }
            Err(ApplyError::DbError(db_error)) => Err(JoinSetCloseError::DbError(db_error)),
            Err(ApplyError::NondeterminismDetected(detail)) => Err(JoinSetCloseError::FatalError {
                err: FatalError::NondeterminismDetected { detail },
            }),
            Err(ApplyError::ConstraintViolation(reason)) => Err(JoinSetCloseError::FatalError {
                err: FatalError::ConstraintViolation { reason },
            }),
            Err(ApplyError::ExecutorClosing) => Err(JoinSetCloseError::ExecutorClosing(
                workflow_ctx.db_connection.version().clone(),
            )),
            Err(ApplyError::ReplayInterrupt) => Ok(Either::Right(ReplayInterrupt)),
        }
    }

    /// Replay execution from existing execution log, which was loaded into `ctx`.
    /// Repaly can move past the last `ExecutionRequest` in order to capture writes for the `advance` RPC.
    /// It can assume it is the only writer to its own and its children's execution log.
    /// In the worst case, e.g. on a race with an external stub response writer, the advance will
    /// fail, and the new replay will have to be issued.
    async fn replay_internal(
        &self,
        ctx: WorkerContext,
        replay_kind: ReplayKind,
        execution_id: ExecutionId,
        version: Version,
        real_connection: Box<dyn DbConnection>,
        parent: Option<(ExecutionId, JoinSetId)>,
    ) -> Result<
        (
            Vec<InternalCapturedWrite>,
            ReplayPendingState,
            Box<dyn DbConnection>,
        ),
        ReplayInternalError,
    > {
        let replay_db_connection =
            ReplayWorkflowDbConnection::new(execution_id, version, real_connection, parent);
        let (retval, replay_db_connection, replay_outcome) = match self
            .run_internal(ctx, Box::new(replay_db_connection), Some(replay_kind))
            .await
        {
            Ok((Either::Left(WorkerResultOk::RunFinished(RunFinished { retval, .. })), db)) => {
                Ok((Some(retval), db, ReplayPendingState::Finished))
            }
            Ok((Either::Left(WorkerResultOk::DbUpdatedByWorkerOrWatcher), db)) => {
                Ok((None, db, ReplayPendingState::Blocked))
            }
            Ok((Either::Right(ReplayInterrupt), db)) => {
                // Only first phase of stubbing leaves the execution in `PendingState::Locked`
                Ok((None, db, ReplayPendingState::Locked))
            }
            Err(WorkflowError::FatalError { err, db_connection }) => {
                debug!("Replay finished with fatal error: {err:?}");
                let retval = SupportedFunctionReturnValue::ExecutionFailure(
                    FinishedExecutionFailure::from(&err),
                );
                Ok((
                    Some(retval),
                    db_connection,
                    ReplayPendingState::FinishedWithFailure(err),
                ))
            }
            Err(WorkflowError::LimitReached { reason, version }) => {
                Err(ReplayInternalError::LimitReached { reason, version })
            }
            Err(WorkflowError::DbError(db_error_write)) => {
                Err(ReplayInternalError::DbError(db_error_write))
            }
            Err(WorkflowError::LockExpired(version)) => {
                Err(ReplayInternalError::LockExpired(version))
            }
            Err(WorkflowError::ExecutorClosing(version)) => {
                Err(ReplayInternalError::ExecutorClosing(version))
            }
        }?;

        let Ok(mut replay_db_connection) = replay_db_connection
            .as_any()
            .downcast::<ReplayWorkflowDbConnection>()
        else {
            unreachable!("`run_internal` returns the same `db_connection` it was supplied")
        };

        if replay_kind == ReplayKind::Unfinished
            && let Some(retval) = retval
        {
            assert_matches!(
                replay_outcome,
                ReplayPendingState::Finished | ReplayPendingState::FinishedWithFailure(_)
            );
            debug!("Replay finished returning a value: {retval:?}");
            // Capture the Finished event that the executor would write.
            let version = replay_db_connection.version().clone();
            let execution_id = replay_db_connection.execution_id().clone();
            let parent = replay_db_connection.parent();
            replay_db_connection.push_write(CapturedDbWrite::AppendFinished {
                execution_id,
                version,
                current_time: self.clock_fn.now(),
                retval,
                parent,
            });
        }
        let (writes, real_connection) = replay_db_connection.into_parts();
        Ok((writes, replay_outcome, real_connection))
    }

    // Returns the same `db_connection` it was supplied.
    async fn run_internal(
        &self,
        ctx: WorkerContext,
        db_connection: Box<dyn WorkflowDbConnection>,
        is_replay: Option<ReplayKind>,
    ) -> Result<
        (
            Either<WorkerResultOk, ReplayInterrupt>,
            Box<dyn WorkflowDbConnection>,
        ),
        WorkflowError,
    > {
        if !ctx.can_be_retried {
            warn!(
                "Workflow configuration set to not retry anymore. This can lead to nondeterministic results."
            );
        }
        let worker_span = ctx.worker_span.clone();
        let execution_deadline = ctx.locked_event.lock_expires_at;
        let prepare_finished = self.prepare_func(ctx, db_connection, is_replay).await?;
        Self::call_func_convert_result(
            prepare_finished.store,
            prepare_finished.func,
            prepare_finished.component_func,
            prepare_finished.params,
            &worker_span,
            execution_deadline,
            self.config.fuel,
        )
        .await
    }

    #[instrument(skip_all, fields(%execution_id))]
    pub async fn replay(&self, execution_id: ExecutionId) -> Result<ReplayResponse, ReplayError> {
        assert!(
            self.deadline_factory.is_for_replay(),
            "replay() requires DeadlineTrackerFactoryForReplay"
        );
        let db_conn = self
            .db_pool
            .connection()
            .await
            .map_err(DbErrorWrite::from)?;
        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(DbErrorWrite::from)?;
        let already_finished_result = log.as_finished_result();
        let ffqn = log.ffqn().clone();
        let params = log.params().clone();
        let (captured_writes, fatal_error, _db_conn) = self
            .capture_replay_writes_from_log(execution_id, log, ffqn, params, db_conn)
            .await?;
        // Remove side effects
        let captured_writes: Vec<_> = captured_writes
            .into_iter()
            .map(|write| write.write)
            .collect();
        Self::transform_replay_to_response(captured_writes, fatal_error, already_finished_result)
    }

    pub(crate) fn transform_replay_to_response(
        captured_writes: Vec<CapturedDbWrite>,
        fatal_error: Option<FatalError>,
        already_finished_result: Option<SupportedFunctionReturnValue>,
    ) -> Result<ReplayResponse, ReplayError> {
        if let Some(err) = fatal_error {
            Err(ReplayError::ReplayFailed {
                err,
                captured_writes,
            })
        } else if !captured_writes.is_empty() {
            Ok(ReplayResponse::Advanceable(ReplayAdvanceable {
                captured_writes,
            }))
        } else if let Some(result) = already_finished_result {
            Ok(ReplayResponse::Finished { result })
        } else {
            Ok(ReplayResponse::Blocked)
        }
    }

    /// Advance a paused workflow by one interrupt boundary.
    ///
    /// Replays the workflow to capture the next write operations, compares them
    /// against the expected `ReplayResponse`, and writes to the real DB if they match.
    #[instrument(skip_all, fields(%execution_id, component_id = %self.config.component_id, deployment_id = %self.deployment_id))]
    pub async fn advance(
        &self,
        execution_id: ExecutionId,
        requested: ReplayAdvanceable,
    ) -> Result<AdvanceResponse, AdvanceError> {
        assert!(
            self.deadline_factory.is_for_replay(),
            "advance() requires DeadlineTrackerFactoryForReplay"
        );
        info!("Advance to requested {requested:?}");
        // Check version before replaying.
        let db_conn = self
            .db_pool
            .connection()
            .await
            .map_err(DbErrorWrite::from)?;
        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(DbErrorWrite::from)?;

        if requested.captured_writes.is_empty() {
            return Err(AdvanceError::NoWrites);
        }

        if let Some(expected_version) = requested.starting_version()
            && log.next_version != *expected_version
        {
            return Err(AdvanceError::VersionMismatch {
                expected: log.next_version,
            });
        }

        let old_version = log.next_version.clone();
        let ffqn = log.ffqn().clone();
        let params = log.params().clone();
        let log_forwarder_sender = self
            .logs_storage_config
            .as_ref()
            .map(|config| &config.log_sender);
        let (fresh_replay, _fatal_error, db_conn) = self
            .capture_replay_writes_from_log(execution_id, log, ffqn, params, db_conn)
            .await?;

        Ok(Self::advance_from_log(
            db_conn.as_ref(),
            &self.cancel_registry,
            log_forwarder_sender,
            requested,
            fresh_replay,
            old_version,
        )
        .await?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseJoinSetOk {
    Ok,
    DbUpdatedByWorkerOrWatcher,
}

impl WorkflowWorker {
    async fn auto_upgrade_locked(&self, ctx: WorkerContext) -> Result<(), WorkerError> {
        let new_digest = self.config.component_id.component_digest.clone();
        let execution_id = ctx.execution_id.clone();
        let version = ctx.version.clone();
        let parent = ctx.parent.clone();
        let replay_ctx = WorkerContext {
            can_be_retried: true,
            ..ctx
        };

        let (mut fresh_replay, replay_pending_state, db_conn) = self
            .replay_internal(
                replay_ctx,
                ReplayKind::Unfinished,
                execution_id.clone(),
                version.clone(),
                self.db_pool
                    .connection()
                    .await
                    .map_err(|err| WorkerError::DbError(err.into()))?,
                parent,
            )
            .await
            .map_err(|err| match err {
                ReplayInternalError::DbError(db_err) => WorkerError::DbError(db_err),
                ReplayInternalError::LimitReached { reason, version } => {
                    WorkerError::LimitReached { reason, version }
                }
                ReplayInternalError::LockExpired(version) => WorkerError::TemporaryTimeout {
                    http_client_traces: None,
                    version,
                },
                ReplayInternalError::ExecutorClosing(version) => {
                    WorkerError::ExecutorClosing(version)
                }
            })?;

        // Explicit replay/advance may persist this fatal replay outcome as `Finished`.
        // Auto-upgrade is implicit, so it only marks this component digest incompatible
        // and leaves the execution available for manual replay/advance later.
        if let ReplayPendingState::FinishedWithFailure(fatal_error) = replay_pending_state {
            // Set ignore_component_digest to avoid needless locks in the future.
            let created_at = self.clock_fn.now();
            db_conn
                .append_batch(
                    created_at,
                    vec![
                        AppendRequest {
                            created_at,
                            event: ExecutionRequest::ComponentUpgradeFinished {
                                component_digest: self.config.component_id.component_digest.clone(),
                                deployment_id: self.deployment_id,
                                outcome: ComponentUpgradeOutcome::Failed {
                                    reason: StrVariant::from(fatal_error.to_string()),
                                },
                            },
                        },
                        AppendRequest {
                            created_at,
                            event: ExecutionRequest::Unlocked(Unlocked {
                                backoff_expires_at: created_at,
                                reason: "auto-upgrade failed".into(),
                            }),
                        },
                    ],
                    execution_id,
                    version,
                )
                .await
                .map_err(WorkerError::DbError)?;
            return Ok(()); // NOK but db already updated
        }

        if fresh_replay.is_empty() {
            warn!("Auto upgrade failed - empty writes returned by replay"); // Unsure what the next pending state would be.
            let created_at = self.clock_fn.now();
            db_conn
                .append_batch(
                    created_at,
                    vec![
                        AppendRequest {
                            created_at,
                            event: ExecutionRequest::ComponentUpgradeFinished {
                                component_digest: self.config.component_id.component_digest.clone(),
                                deployment_id: self.deployment_id,
                                outcome: ComponentUpgradeOutcome::Failed {
                                    reason: "auto-upgrade replay produced no writes".into(),
                                },
                            },
                        },
                        AppendRequest {
                            created_at,
                            event: ExecutionRequest::Unlocked(Unlocked {
                                backoff_expires_at: created_at,
                                reason: "auto-upgrade failed".into(),
                            }),
                        },
                    ],
                    execution_id,
                    version,
                )
                .await
                .map_err(WorkerError::DbError)?;
            return Ok(());
        }

        let version = db_conn
            .append(
                execution_id.clone(),
                version,
                AppendRequest {
                    created_at: self.clock_fn.now(),
                    event: ExecutionRequest::ComponentUpgradeFinished {
                        component_digest: new_digest,
                        deployment_id: self.deployment_id,
                        outcome: ComponentUpgradeOutcome::Success {
                            reason: ComponentUpgradeReason::Auto,
                        },
                    },
                },
            )
            .await
            .map_err(WorkerError::DbError)?;

        // accomodate for already appended `ExecutionRequest::ComponentUpgradeFinished`
        bump_versions_for_execution(&mut fresh_replay, &execution_id);
        let version = apply_writes(
            db_conn.as_ref(),
            &self.cancel_registry,
            self.logs_storage_config
                .as_ref()
                .map(|config| &config.log_sender),
            fresh_replay,
            version,
        )
        .await
        .map_err(WorkerError::DbError)?;
        match replay_pending_state {
            ReplayPendingState::Finished => {
                debug!("Auto upgrade replay finished the execution");
            }
            ReplayPendingState::Blocked => {
                debug!("Auto upgrade replay blocked the execution");
            }
            ReplayPendingState::Locked => {
                let created_at = self.clock_fn.now();
                db_conn
                    .append(
                        execution_id,
                        version,
                        AppendRequest {
                            created_at,
                            event: ExecutionRequest::Unlocked(Unlocked {
                                backoff_expires_at: created_at,
                                reason: "auto-upgrade succeeded".into(),
                            }),
                        },
                    )
                    .await
                    .map_err(WorkerError::DbError)?;
            }
            ReplayPendingState::FinishedWithFailure(_) => unreachable!("handled above"),
        }
        info!("Execution auto-upgraded");
        Ok(())
    }
}

#[async_trait]
impl Worker for WorkflowWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.exported_functions_noext
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        if ctx.component_digest != self.config.component_id.component_digest {
            self.auto_upgrade_locked(ctx).await?;
            return Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher);
        }

        let db_connection = Box::new(CachingDbConnection::new(
            self.db_pool.connection().await.unwrap(),
            ctx.execution_id.clone(),
            CachingBuffer::new(self.config.join_next_blocking_strategy),
            ctx.version.clone(),
        ));
        let worker_span = ctx.worker_span.clone();
        worker_span.in_scope(|| {
            info!("Execution run started",);
        });
        let res = self
            .run_internal(
                ctx,
                db_connection,
                None, // is_replay
            )
            .await;
        worker_span.in_scope(|| match res {
            Ok((Either::Left(ok), _)) => {
                info!("Execution run finished");
                WorkerResult::Ok(ok)
            }
            Ok((Either::Right(_), _)) => unreachable!("not replaying"),
            Err(workflow_err) => {
                info!("Execution run finished with an error");
                WorkerResult::Err(WorkerError::from(workflow_err))
            }
        })
    }
}

#[derive(Debug)]
enum ReplayPendingState {
    Locked,
    Blocked,
    Finished,
    FinishedWithFailure(FatalError),
}

#[cfg(any(test, feature = "test"))]
pub mod test {
    use crate::{
        RunnableComponent,
        engines::{EngineConfig, Engines},
    };
    use concepts::{ComponentId, ComponentType, StrVariant, component_id::ComponentDigest};
    use utils::sha256sum::calculate_sha256_file;
    use wasmtime::Engine;

    pub async fn compile_workflow(wasm_path: &str) -> (RunnableComponent, ComponentId) {
        let engine = Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        compile_workflow_with_engine(wasm_path, &engine).await
    }

    pub(crate) async fn compile_workflow_with_engine(
        wasm_path: &str,
        engine: &Engine,
    ) -> (RunnableComponent, ComponentId) {
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::empty(),
            ComponentDigest(calculate_sha256_file(wasm_path).await.unwrap().0),
        )
        .unwrap();
        (
            RunnableComponent::new(wasm_path, engine, component_id.component_type).unwrap(),
            component_id,
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::activity::activity_worker::test::{compile_activity, compile_activity_stub};
    use crate::activity::activity_worker::tests::{
        activity_config_allowed_host, new_activity_worker,
    };
    use crate::activity::activity_worker::tests::{new_activity, new_activity_with_config};
    use crate::activity::cancel_registry::CancelRegistry;
    use crate::testing_fn_registry::{TestingFnRegistry, fn_registry_dummy};
    use crate::workflow::deadline_tracker::{
        DeadlineTrackerFactoryForReplay, DeadlineTrackerFactoryTokio,
    };
    use crate::workflow::workflow_worker::test::{compile_workflow, compile_workflow_with_engine};
    use crate::{
        activity::activity_worker::tests::{FIBO_10_INPUT, FIBO_10_OUTPUT, new_activity_fibo},
        engines::{EngineConfig, Engines},
    };
    use assert_matches::assert_matches;
    use chrono::DateTime;
    use concepts::component_id::COMPONENT_DIGEST_DUMMY;
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutionIdDerived};
    use concepts::storage::{
        AppendEventsToExecution, AppendResponseToExecution, ExecutionLog, HistoryEvent,
        JoinSetRequest, JoinSetResponse, Locked, LockedBy, LogEntry, LogInfoAppendRow, LogLevel,
        PendingStateFinishedError, PersistKind,
    };
    use concepts::storage::{
        AppendRequest, DbConnection, DbConnectionTest, DbPool, ExecutionRequest,
    };
    use concepts::time::TokioSleep;
    use concepts::{
        ComponentId, ComponentRetryConfig, ComponentType, ExecutionFailureKind, ExecutionId,
        Params, StrVariant, SupportedFunctionReturnValue,
    };
    use concepts::{
        prefixed_ulid::{ExecutorId, RunId},
        storage::{
            CreateRequest, DbPoolCloseable, PendingState, PendingStateBlockedByJoinSet,
            PendingStateFinished, PendingStateFinishedResultKind, PendingStatePendingAt, Version,
            wait_for_pending_state_fn,
        },
    };
    use db_tests::Database;
    use executor::executor::{LockingStrategy, extract_exported_ffqns_noext_test};
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
    };
    use insta::assert_json_snapshot;
    use rstest::rstest;

    use serde_json::{Value, json};
    use std::collections::VecDeque;
    use std::ops::Deref;
    use std::str::FromStr;
    use std::time::Duration;
    use test_db_macro::expand_enum_database;
    use test_utils::sim_clock::SimClock;
    use test_utils::{ExecutionLogSanitized, redact_component_digest};
    use tokio::sync::mpsc;
    use tracing::debug;
    use tracing::info_span;

    fn drain_forwarded_log_messages(
        receiver: &mut mpsc::Receiver<LogInfoAppendRow>,
    ) -> Vec<String> {
        let mut messages = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(LogInfoAppendRow {
                    log_entry: LogEntry::Log { message, .. },
                    ..
                }) => messages.push(message),
                Ok(_) => {}
                Err(
                    tokio::sync::mpsc::error::TryRecvError::Empty
                    | tokio::sync::mpsc::error::TryRecvError::Disconnected,
                ) => break,
            }
        }
        messages
    }
    use val_json::{
        type_wrapper::TypeWrapper,
        wast_val::{ValKey, WastVal, WastValWithType},
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    pub const FIBOA_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA,
    ); // fiboa: func(n: u8, iterations: u32) -> u64;
    pub const FIBOA_CONCURRENT_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA_CONCURRENT,
    ); // fiboa-concurrent: func(n: u8, iterations: u32) -> u64;
    pub const FIBOA_SUBMIT_JSON_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA_SUBMIT_JSON,
    ); // fiboa-submit-json: func(n: u8) -> u64;
    const TEST_SUBMIT_JSON_UNKNOWN_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::TEST_SUBMIT_JSON_UNKNOWN_FFQN,
    );
    const TEST_SUBMIT_JSON_MALFORMED_PARAMS_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::TEST_SUBMIT_JSON_MALFORMED_PARAMS,
    );

    const TEST_GET_RESULT_JSON_BEFORE_AWAIT_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::TEST_GET_RESULT_JSON_BEFORE_AWAIT,
    );
    const TEST_GET_RESULT_JSON_ERR_VARIANT_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::TEST_GET_RESULT_JSON_ERR_VARIANT,
    );
    const TEST_SCHEDULE_JSON_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::TEST_SCHEDULE_JSON,
    );
    const SLEEP1_HOST_ACTIVITY_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SLEEP_HOST_ACTIVITY); // sleep-host-activity: func(millis: u64);

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    const FFQN_WORKFLOW_SERDE_STARGAZERS: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_serde_workflow_builder::exports::testing::serde_workflow::serde_workflow::GET_STARGAZERS);

    const FFQN_WORKFLOW_HTTP_GET: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET,
    );
    const FFQN_WORKFLOW_HTTP_GET_SUCCESSFUL: FunctionFqn = FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL);
    const FFQN_WORKFLOW_HTTP_GET_RESP: FunctionFqn = FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_RESP);

    pub(crate) async fn compile_workflow_worker(
        wasm_path: &str,
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        cancel_registry: CancelRegistry,
    ) -> Arc<WorkflowWorker> {
        compile_workflow_worker_runnable(
            wasm_path,
            db_pool,
            clock_fn,
            join_next_blocking_strategy,
            fn_registry,
            cancel_registry,
        )
        .await
        .0
    }

    pub(crate) async fn compile_workflow_worker_runnable(
        wasm_path: &str,
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        cancel_registry: CancelRegistry,
    ) -> (Arc<WorkflowWorker>, RunnableComponent) {
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (runnable_component, component_id) =
            compile_workflow_with_engine(wasm_path, &workflow_engine).await;
        (
            Arc::new(
                WorkflowWorkerCompiled::new_with_config(
                    runnable_component.clone(),
                    WorkflowConfig {
                        component_id,
                        join_next_blocking_strategy,
                        backtrace_persist: false,
                        stub_wasi: false,
                        fuel: None,
                        lock_extension: None,
                        subscription_interruption: None,
                    },
                    workflow_engine,
                    clock_fn.clone_box(),
                )
                .unwrap()
                .link(fn_registry.clone())
                .unwrap()
                .into_worker(
                    DEPLOYMENT_ID_DUMMY,
                    db_pool,
                    Arc::new(DeadlineTrackerFactoryTokio::new(Duration::ZERO, clock_fn)),
                    cancel_registry,
                    None, // logs_storage_config
                ),
            ),
            runnable_component,
        )
    }

    /// Build a [`WorkflowWorker`] configured for replay/advance: `Interrupt` strategy,
    /// backtraces enabled, WASI stubbed, infinite-deadline tracker, empty cancel registry.
    #[expect(clippy::too_many_arguments)]
    fn build_workflow_replay_worker(
        deployment_id: DeploymentId,
        component_id: ComponentId,
        runnable_component: &RunnableComponent,
        workflow_engine: Arc<Engine>,
        fn_registry: Arc<dyn FunctionRegistry>,
        db_pool: Arc<dyn DbPool>,
        logs_storage_config: Option<LogStrageConfig>,
        clock_fn: Box<dyn ClockFn>,
    ) -> WorkflowWorker {
        let config = WorkflowConfig {
            component_id,
            join_next_blocking_strategy: JoinNextBlockingStrategy::Interrupt,
            backtrace_persist: true,
            stub_wasi: true,
            fuel: None,
            lock_extension: None,
            subscription_interruption: None,
        };
        WorkflowWorkerCompiled::new_with_config(
            runnable_component.clone(),
            config,
            workflow_engine,
            clock_fn,
        )
        .unwrap()
        .link(fn_registry)
        .unwrap()
        .into_worker(
            deployment_id,
            db_pool,
            Arc::new(DeadlineTrackerFactoryForReplay {}),
            CancelRegistry::new(),
            logs_storage_config,
        )
    }

    const LOCK_EXPIRY_WORKFLOW: Duration = Duration::from_secs(1);

    async fn new_workflow_exec_task(
        db_pool: Arc<dyn DbPool>,
        wasm_path: &'static str,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        cancel_registry: CancelRegistry,
        locking_strategy: LockingStrategy,
    ) -> ExecTask {
        let worker = compile_workflow_worker(
            wasm_path,
            db_pool.clone(),
            clock_fn.clone_box(),
            join_next_blocking_strategy,
            fn_registry,
            cancel_registry,
        )
        .await;
        info!("Instantiated worker");
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: LOCK_EXPIRY_WORKFLOW,
            tick_sleep: TICK_SLEEP,
            component_id: worker.config.component_id.clone(),
            task_limiter_global: None,
            task_limiter_local: None,
            executor_id: ExecutorId::generate(),
            retry_config: ComponentRetryConfig::WORKFLOW,
            locking_strategy,
        };
        ExecTask::new_all_ffqns_test(worker, exec_config, clock_fn, db_pool)
    }

    pub(crate) async fn new_workflow_fibo(
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        cancel_registry: CancelRegistry,
        locking_strategy: LockingStrategy,
    ) -> ExecTask {
        new_workflow_exec_task(
            db_pool,
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            clock_fn,
            join_next_blocking_strategy,
            fn_registry,
            cancel_registry,
            locking_strategy,
        )
        .await
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
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                .await,
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            join_next_blocking_strategy,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest,
        )
        .await;
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
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
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
                    PendingState::BlockedByJoinSet(..)
                )
                .then_some(())
            },
            None,
        )
        .await
        .unwrap();

        info!("Execution should call the activity and finish");

        let activity_exec = new_activity_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            TokioSleep,
            LockingStrategy::ByComponentDigest,
        )
        .await;
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
            .get_finished_result(&execution_id)
            .await
            .unwrap();
        let res = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val);

        let fibo = assert_matches!(res,
            WastValWithType {value: WastVal::U64(val), r#type: TypeWrapper::U64 } => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);
    }

    /// Test for `submit_json` and `get_result_json` workflow functions.
    /// The workflow uses `submit_json` to call the fibo activity with JSON params,
    /// then retrieves the result using `get_result_json`.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn fiboa_submit_json_workflow(
        db: Database,
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = db.set_up().await;
        fiboa_submit_json_workflow_inner(db_pool.clone(), sim_clock, join_next_blocking_strategy)
            .await;
        db_close.close().await;
    }

    async fn fiboa_submit_json_workflow_inner(
        db_pool: Arc<dyn DbPool>,
        sim_clock: SimClock,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        test_utils::set_up();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                .await,
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            join_next_blocking_strategy,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest,
        )
        .await;
        // Create an execution with fiboa_submit_json workflow function
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        // The fiboa_submit_json function takes a single u8 parameter
        let params = Params::from_json_values_test(vec![json!(FIBO_10_INPUT)]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBOA_SUBMIT_JSON_WORKFLOW_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();
        info!("Should end as BlockedByJoinSet (waiting for activity)");

        let executed_workflows = workflow_exec
            .tick_test(sim_clock.now(), RunId::generate())
            .await;

        let executed_workflows =
            if join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt {
                assert_eq!(1, executed_workflows.wait_for_tasks().await.len());
                None
            } else {
                Some(executed_workflows)
            };

        // Wait for the workflow to be blocked by join set
        wait_for_pending_state_fn(
            db_connection.as_ref(),
            &execution_id,
            |exe_history| {
                matches!(
                    exe_history.pending_state,
                    PendingState::BlockedByJoinSet(..)
                )
                .then_some(())
            },
            None,
        )
        .await
        .unwrap();

        info!("Running activity to complete the child execution");

        let activity_exec = new_activity_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            TokioSleep,
            LockingStrategy::ByComponentDigest,
        )
        .await;
        let executed_activities = activity_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_activities.len());

        let executed_workflows = if let Some(executed_workflows) = executed_workflows {
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
        let res = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val);

        let fibo = assert_matches!(res,
            WastValWithType {value: WastVal::U64(val), r#type: TypeWrapper::U64 } => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);
    }

    /// Test: `submit_json` with unknown FFQN → `FunctionNotFound` error
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn test_submit_json_unknown_ffqn(database: Database) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = database.set_up().await;
        test_submit_json_unknown_ffqn_inner(db_pool.clone(), sim_clock).await;
        db_close.close().await;
    }

    async fn test_submit_json_unknown_ffqn_inner(db_pool: Arc<dyn DbPool>, sim_clock: SimClock) {
        test_utils::set_up();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                .await,
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest,
        )
        .await;

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        // No params needed for this test function
        let params = Params::from_json_values_test(vec![]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: TEST_SUBMIT_JSON_UNKNOWN_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        // Should return Ok(()) since the test passed
        assert_matches!(res, SupportedFunctionReturnValue::Ok(None));
    }

    /// Test: `submit_json` with malformed JSON params → `ParamsParsingError`
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn test_submit_json_malformed_params(database: Database) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = database.set_up().await;
        test_submit_json_malformed_params_inner(db_pool.clone(), sim_clock).await;
        db_close.close().await;
    }

    async fn test_submit_json_malformed_params_inner(
        db_pool: Arc<dyn DbPool>,
        sim_clock: SimClock,
    ) {
        test_utils::set_up();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                .await,
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest,
        )
        .await;

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params = Params::from_json_values_test(vec![]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: TEST_SUBMIT_JSON_MALFORMED_PARAMS_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        assert_matches!(res, SupportedFunctionReturnValue::Ok(None));
    }

    /// Test: `get_result_json` before await → `NotFoundInProcessedResponses`
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn test_get_result_json_before_await(database: Database) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = database.set_up().await;
        test_get_result_json_before_await_inner(db_pool.clone(), sim_clock).await;
        db_close.close().await;
    }

    async fn test_get_result_json_before_await_inner(
        db_pool: Arc<dyn DbPool>,
        sim_clock: SimClock,
    ) {
        test_utils::set_up();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                .await,
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest,
        )
        .await;

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params = Params::from_json_values_test(vec![]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: TEST_GET_RESULT_JSON_BEFORE_AWAIT_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        info!(
            "First workflow tick - submits activity, tests get_result_json, calls join_set_close"
        );
        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        // The workflow submitted an activity and called join_set_close which internally
        // calls JoinNext(closing: true). This interrupts and the workflow is scheduled to run again.
        // We need to run the activity executor to cancel/complete the activity.

        info!("Running activity executor to process the child activity");
        let activity_exec = new_activity_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            TokioSleep,
            LockingStrategy::ByComponentDigest,
        )
        .await;
        // The activity may be cancelled or run - either way we process it
        activity_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        info!("Second workflow tick - receives response and completes");
        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        assert_matches!(res, SupportedFunctionReturnValue::Ok(None));
    }

    /// Test: `get_result_json` when activity returns error → Err(None) for unit error type
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn test_get_result_json_err_variant(database: Database) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = database.set_up().await;
        test_get_result_json_err_variant_inner(db_pool.clone(), sim_clock).await;
        db_close.close().await;
    }

    async fn test_get_result_json_err_variant_inner(db_pool: Arc<dyn DbPool>, sim_clock: SimClock) {
        test_utils::set_up();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                .await,
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest,
        )
        .await;

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params = Params::from_json_values_test(vec![]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: TEST_GET_RESULT_JSON_ERR_VARIANT_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        info!("First tick - workflow submits activity, blocks waiting for join set");
        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        // Wait for the workflow to be blocked by join set
        wait_for_pending_state_fn(
            db_connection.as_ref(),
            &execution_id,
            |exe_history| {
                matches!(
                    exe_history.pending_state,
                    PendingState::BlockedByJoinSet(..)
                )
                .then_some(())
            },
            None,
        )
        .await
        .unwrap();

        info!("Running activity that returns error (n > 40)");
        let activity_exec = new_activity_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            TokioSleep,
            LockingStrategy::ByComponentDigest,
        )
        .await;
        let executed_activities = activity_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(1, executed_activities.len());

        info!("Second tick - workflow receives error response, verifies get_result_json");
        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        assert_matches!(res, SupportedFunctionReturnValue::Ok(None));
    }

    /// Test: `schedule_json` function for scheduling executions with JSON params
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn test_schedule_json(database: Database) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = database.set_up().await;
        test_schedule_json_inner(db_pool.clone(), sim_clock).await;
        db_close.close().await;
    }

    async fn test_schedule_json_inner(db_pool: Arc<dyn DbPool>, sim_clock: SimClock) {
        test_utils::set_up();
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                .await,
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest,
        )
        .await;

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params = Params::from_json_values_test(vec![]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: TEST_SCHEDULE_JSON_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        workflow_exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();

        // Extract the execution-id record from the result
        let res = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val);
        let record = assert_matches!(res.value, WastVal::Record(map) => map);

        // Extract the "id" field from the record
        let id_val = record
            .get(&ValKey::new_snake("id"))
            .expect("id field should exist");
        let scheduled_execution_id_str = assert_matches!(id_val, WastVal::String(s) => s);

        // Parse the execution ID
        let scheduled_execution_id =
            ExecutionId::from_str(scheduled_execution_id_str).expect("valid execution id");

        // Verify the scheduled execution exists in the database
        let create_request = db_connection
            .get_create_request(&scheduled_execution_id)
            .await
            .expect("scheduled execution should exist in db");

        // Verify it's the fibo activity that was scheduled
        assert_eq!(
            create_request.ffqn,
            FunctionFqn::new_static("testing:fibo/fibo", "fibo")
        );
    }

    #[tokio::test]
    #[should_panic(expected = "preinstantiation error")]
    async fn fibo_workflow_with_missing_imports_should_fail() {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, _db_close) = Database::Sqlite.set_up().await;
        test_utils::set_up();
        let fn_registry = fn_registry_dummy(&[]);
        let cancel_registry = CancelRegistry::new();
        new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest, // does not matter
        )
        .await;
    }

    #[tokio::test]
    async fn execution_deadline_before_now_should_timeout() {
        const SLEEP_MILLIS: u32 = 100;
        test_utils::set_up();

        let (_guard, db_pool, db_close) = Database::Sqlite.set_up().await;

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
                )
                .await,
                compile_workflow(
                    // TODO: compling workflow twice
                    test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                )
                .await,
            ]),
            cancel_registry,
        )
        .await;
        // simulate a scheduling problem where deadline < Now.clone_box(), meaning there is no point in running the execution.
        let execution_deadline = sim_clock.now();
        sim_clock.move_time_forward(Duration::from_millis(100));
        let ctx = WorkerContext {
            execution_id: ExecutionId::generate(),
            metadata: concepts::ExecutionMetadata::empty(),
            component_digest: worker.config.component_id.component_digest.clone(),
            ffqn: SLEEP1_HOST_ACTIVITY_FFQN,
            params: Params::from_json_values_test(vec![json!({"milliseconds": SLEEP_MILLIS})]),
            event_history: Vec::new(),
            responses: Vec::new(),
            parent: None,
            version: Version::new(0),
            can_be_retried: false,
            worker_span: info_span!("worker-test"),
            locked_event: Locked {
                component_id: worker.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                executor_id: ExecutorId::generate(),
                run_id: RunId::generate(),
                lock_expires_at: execution_deadline,
                retry_config: ComponentRetryConfig::ZERO,
            },
            executor_close_watcher: tokio::sync::watch::channel(false).1,
        };
        let worker_result = worker.run(ctx).await;
        assert_matches!(
            worker_result,
            Err(WorkerError::TemporaryTimeout {
                version: Version(0),
                http_client_traces: None,
            })
        );
        db_close.close().await;
    }

    #[rstest]
    #[tokio::test]
    async fn sleep2_happy_path() {
        const SLEEP_SCHEDULE_AT_HOST_ACTIVITY_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SLEEP_SCHEDULE_AT); // sleep-host-activity: func(s: schedule-at);

        const LOCK_DURATION: Duration = Duration::from_secs(1);
        let join_next_blocking_strategy = JoinNextBlockingStrategy::Interrupt;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = Database::Sqlite.set_up().await;
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection().await.unwrap();
        let sim_clock = SimClock::epoch();
        let sleep_exec = {
            let fn_registry = TestingFnRegistry::new_from_components(vec![
                compile_activity(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                )
                .await, // not used here
                compile_workflow(
                    test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                )
                .await,
            ]);
            let cancel_registry = CancelRegistry::new();
            let worker = compile_workflow_worker(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                db_pool.clone(),
                sim_clock.clone_box(),
                join_next_blocking_strategy,
                &fn_registry,
                cancel_registry,
            )
            .await;
            db_connection
                .create(CreateRequest {
                    created_at: sim_clock.now(),
                    execution_id: execution_id.clone(),
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    ffqn: SLEEP_SCHEDULE_AT_HOST_ACTIVITY_FFQN,
                    params: Params::from_json_values_test(vec![json!("now")]),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: sim_clock.now(),
                    component_id: worker.config.component_id.clone(),
                    scheduled_by: None,
                    paused: false,
                })
                .await
                .unwrap();
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_DURATION,
                tick_sleep: Duration::ZERO, // irrelevant here as we call tick manually
                component_id: worker.config.component_id.clone(),
                task_limiter_global: None,
                task_limiter_local: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::ByComponentDigest,
            };
            let ffqns = extract_exported_ffqns_noext_test(worker.as_ref());
            ExecTask::new_test(
                exec_config,
                worker,
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
            assert_matches!(pending_state, PendingState::BlockedByJoinSet(PendingStateBlockedByJoinSet {lock_expires_at, .. }) => lock_expires_at)
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
            PendingState::Finished(PendingStateFinished {
                result_kind: PendingStateFinishedResultKind::Ok,
                ..
            })
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
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY)
                .await, // not used here
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW)
                .await,
        ]);

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection().await.unwrap();

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
            )
            .await;

            db_connection
                .create(CreateRequest {
                    created_at: sim_clock.now(),
                    execution_id: execution_id.clone(),
                    ffqn: SLEEP1_HOST_ACTIVITY_FFQN,
                    params: Params::from_json_values_test(vec![
                        json!({"milliseconds": SLEEP_MILLIS}),
                    ]),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: sim_clock.now(),
                    component_id: worker.config.component_id.clone(),
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    scheduled_by: None,
                    paused: false,
                })
                .await
                .unwrap();

            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_DURATION,
                tick_sleep: Duration::ZERO, // irrelevant here as we call tick manually
                component_id: worker.config.component_id.clone(),
                task_limiter_global: None,
                task_limiter_local: None,
                executor_id,
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::ByComponentDigest,
            };
            let ffqns = extract_exported_ffqns_noext_test(worker.as_ref());
            ExecTask::new_test(
                exec_config.clone(),
                worker,
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
            assert_matches!(pending_state, PendingState::BlockedByJoinSet(PendingStateBlockedByJoinSet {lock_expires_at, .. }) => lock_expires_at)
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
                PendingState::PendingAt(PendingStatePendingAt {
                    scheduled_at,
                    last_lock: Some(LockedBy { executor_id, run_id }),
                })
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
            PendingState::Finished(PendingStateFinished {
                result_kind: PendingStateFinishedResultKind::Ok,
                ..
            })
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
            compile_activity(test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY)
                .await,
            compile_workflow(test_programs_serde_workflow_builder::TEST_PROGRAMS_SERDE_WORKFLOW)
                .await,
        ]);
        let activity_exec = new_activity(
            db_pool.clone(),
            test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
            sim_clock.clone_box(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
            LockingStrategy::ByComponentDigest,
        )
        .await;

        let workflow_exec = new_workflow_exec_task(
            db_pool.clone(),
            test_programs_serde_workflow_builder::TEST_PROGRAMS_SERDE_WORKFLOW,
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
            LockingStrategy::ByComponentDigest,
        )
        .await;
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_SERDE_STARGAZERS,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
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
        assert_matches!(res, SupportedFunctionReturnValue::Ok(None));
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

        // Start mock server BEFORE creating activity worker to get the allowed host
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .expect(1)
            .mount(&server)
            .await;
        debug!("started mock server on {}", server.address());
        let allowed_host = format!("http://127.0.0.1:{}", server.address().port());
        let url = format!("{allowed_host}/");

        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            )
            .await,
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            )
            .await,
        ]);
        let activity_exec = new_activity_with_config(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone_box(),
            TokioSleep,
            move |component_id| activity_config_allowed_host(component_id, &allowed_host),
            ComponentRetryConfig::ZERO,
            LockingStrategy::ByComponentDigest,
        )
        .await;

        let workflow_exec = new_workflow_exec_task(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
            LockingStrategy::ByComponentDigest,
        )
        .await;
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
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
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
        let val = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val.value);
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

        // Start mock server BEFORE creating activity worker to get the allowed host
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .mount(&server)
            .await;
        debug!("started mock server on {}", server.address());
        let allowed_host = format!("http://127.0.0.1:{}", server.address().port());
        let url = format!("{allowed_host}/");

        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            )
            .await,
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            )
            .await,
        ]);

        let activity_exec = new_activity_with_config(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone_box(),
            TokioSleep,
            move |component_id| activity_config_allowed_host(component_id, &allowed_host),
            ComponentRetryConfig::ZERO,
            LockingStrategy::ByComponentDigest,
        )
        .await;
        let workflow_exec = new_workflow_exec_task(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
            LockingStrategy::ByComponentDigest,
        )
        .await;
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
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
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
        let val = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val.value);
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
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY)
                .await,
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW)
                .await,
        ]);
        let worker = compile_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            join_next_strategy,
            &fn_registry,
            CancelRegistry::new(),
        )
        .await;
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
                component_id: worker.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();
        let exec_task = ExecTask::new_test(
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: worker.config.component_id.clone(),
                task_limiter_global: None,
                task_limiter_local: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::ByComponentDigest,
            },
            worker,
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
            SupportedFunctionReturnValue::Ok(None)
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
                DEPLOYMENT_ID_DUMMY,
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
            )
            .await,
            compile_workflow(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            )
            .await,
        ]);
        let activity_exec = new_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone_box(),
            TokioSleep,
            ComponentRetryConfig::ZERO,
            LockingStrategy::ByComponentDigest,
        )
        .await;

        let workflow_exec = new_workflow_exec_task(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
            LockingStrategy::ByComponentDigest,
        )
        .await;

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
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
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
        assert_matches!(res, SupportedFunctionReturnValue::Err(Some(_)));
        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap()
            .pending_state;
        assert_matches!(
            pending_state,
            PendingState::Finished(PendingStateFinished {
                result_kind: PendingStateFinishedResultKind::Err(PendingStateFinishedError::Error),
                ..
            })
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
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
            compile_workflow(test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW)
                .await,
        ]);

        let worker = compile_workflow_worker(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        )
        .await;
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
                component_id: worker.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();
        let executor_id = ExecutorId::generate();

        let exec_task = ExecTask::new_test(
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: worker.config.component_id.clone(),
                task_limiter_global: None,
                task_limiter_local: None,
                executor_id,
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::ByComponentDigest,
            },
            worker,
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
            PendingState::PendingAt(PendingStatePendingAt {
                scheduled_at,
                last_lock: Some(LockedBy { executor_id, run_id }),
            })
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
            SupportedFunctionReturnValue::Ok(Some(WastValWithType { value, .. })) => value
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
            "two_delays_in_same_join_set",
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
            "join_next_produces_all_processed_error",
        )
        .await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn join_next_try_pending(db: db_tests::Database) {
        const FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::JOIN_NEXT_TRY_PENDING
        );
        execute_workflow_fn_with_single_delay(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            FFQN,
            None, // No delay needed - we want to test that join_next_try returns Pending immediately
            db,
            "join_next_try_pending",
        )
        .await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn join_next_try_all_processed(db: db_tests::Database) {
        const FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::JOIN_NEXT_TRY_ALL_PROCESSED
        );
        execute_workflow_fn_with_single_delay(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            FFQN,
            None, // No delay needed
            db,
            "join_next_try_all_processed",
        )
        .await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn join_next_try_found(db: db_tests::Database) {
        const FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::JOIN_NEXT_TRY_FOUND
        );
        execute_workflow_fn_with_delays(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            FFQN,
            vec![Duration::from_millis(1), Duration::from_millis(9)], // Both delay requests should expire
            db,
            "join_next_try_found",
        )
        .await;
    }

    async fn execute_workflow_fn_with_single_delay(
        workflow_wasm_path: &'static str,
        ffqn: FunctionFqn,
        delay: Option<Duration>,
        db: db_tests::Database,
        test_name: &'static str,
    ) -> ExecutionLog {
        execute_workflow_fn_with_delays(
            workflow_wasm_path,
            ffqn,
            delay.into_iter().collect(),
            db,
            test_name,
        )
        .await
    }

    async fn execute_workflow_fn_with_delays(
        workflow_wasm_path: &'static str,
        ffqn: FunctionFqn,
        delays: Vec<Duration>,
        db: db_tests::Database,
        test_name: &'static str,
    ) -> ExecutionLog {
        const MAX_RUNS: u128 = 100;

        test_utils::set_up();
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, db_close) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY)
                .await,
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
            compile_workflow(workflow_wasm_path).await,
        ]);

        let worker = compile_workflow_worker(
            workflow_wasm_path,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        )
        .await;
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
                component_id: worker.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        let exec_task = ExecTask::new_test(
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: worker.config.component_id.clone(),
                task_limiter_global: None,
                task_limiter_local: None,
                executor_id: ExecutorId::from_parts(0, 0),
                retry_config: ComponentRetryConfig::WORKFLOW,
                locking_strategy: LockingStrategy::ByComponentDigest,
            },
            worker,
            sim_clock.clone_box(),
            db_pool.clone(),
            Arc::new([ffqn.clone()]),
        );

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

        let mut delays = VecDeque::from(delays);
        while let Some(delay) = delays.pop_front() {
            // If delay is expected, move time forward and tick expired timers watcher
            let pending_state = db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap()
                .pending_state;

            assert_matches!(pending_state, PendingState::BlockedByJoinSet(..));

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
        insta::with_settings!({
            snapshot_suffix => format!("{test_name}-{}", ffqn.to_string().replace(':',"_")),
            prepend_module_to_snapshot => false},
            {
                insta::assert_json_snapshot!(ExecutionLogSanitized::from(execution_log.clone()));
            }
        );
        drop(db_connection);
        db_close.close().await;
        execution_log
    }

    async fn execute_paused_workflow_until_finished_without_snapshots(
        db_connection: &dyn DbConnectionTest,
        harness: WorkflowAdvanceHarness,
        sim_clock: &SimClock,
        max_steps: usize,
    ) -> ExecutionLog {
        let mut steps = 0;

        loop {
            sim_clock.move_time_forward(Duration::from_millis(100));
            let replay = harness
                .replay_worker
                .replay(harness.execution_id.clone())
                .await
                .unwrap();

            let replay = match replay {
                ReplayResponse::Advanceable(replay) => replay,
                ReplayResponse::Finished { .. } => {
                    return db_connection.get(&harness.execution_id).await.unwrap();
                }
                ReplayResponse::Blocked => {
                    panic!("replay must be advanceable or finished while stepping paused workflow");
                }
            };

            steps += 1;
            let expected_finished = replay.get_return_value().cloned();
            let advance = harness
                .replay_worker
                .advance(harness.execution_id.clone(), replay)
                .await
                .unwrap();
            assert_eq!(advance.finished, expected_finished);

            if db_connection
                .get_finished_result(&harness.execution_id)
                .await
                .is_ok()
            {
                return db_connection.get(&harness.execution_id).await.unwrap();
            }

            assert!(
                steps < max_steps,
                "execution did not finish after {steps} replay+advance steps",
            );
        }
    }

    fn normalized_cancellable_request_order(execution_log: &ExecutionLog) -> Vec<String> {
        execution_log
            .events
            .iter()
            .filter_map(|event| match &event.event {
                ExecutionRequest::HistoryEvent {
                    event:
                        HistoryEvent::JoinSetRequest {
                            request:
                                JoinSetRequest::ChildExecutionRequest {
                                    child_execution_id, ..
                                },
                            ..
                        },
                } => Some(format!("child:{child_execution_id}")),
                ExecutionRequest::HistoryEvent {
                    event:
                        HistoryEvent::JoinSetRequest {
                            request: JoinSetRequest::DelayRequest { delay_id, .. },
                            ..
                        },
                } => Some(format!("delay:{delay_id}")),
                _ => None,
            })
            .collect()
    }

    fn normalized_response_order(execution_log: &ExecutionLog) -> Vec<String> {
        execution_log
            .responses
            .iter()
            .map(|response| match &response.event.event.event {
                JoinSetResponse::ChildExecutionFinished {
                    child_execution_id, ..
                } => format!("child:{child_execution_id}"),
                JoinSetResponse::DelayFinished { delay_id, .. } => {
                    format!("delay:{delay_id}")
                }
            })
            .collect()
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
            "await_next_produces_all_processed_error"
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
            "stub_submit_race_join_next_stub",
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
            "stub_submit_race_join_next_delay"
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
            "execute_workflow_fn_with_single_delay"
        )
        .await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn stub_join_set_close_cancellation_order_matches_advance(db: db_tests::Database) {
        const FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::JOIN_SET_CLOSE_CANCELLATION_ORDER,
        );

        test_utils::set_up();
        let execution_id = ExecutionId::from_parts(0, 100);
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
            compile_workflow(test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW)
                .await,
        ]);

        let direct_execution_log = {
            let sim_clock = SimClock::epoch();
            let (_guard, db_pool, db_close) = db.set_up().await;
            let db_connection = db_pool.connection_test().await.unwrap();

            let direct_worker = compile_workflow_worker(
                test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
                db_pool.clone(),
                sim_clock.clone_box(),
                JoinNextBlockingStrategy::Interrupt,
                &fn_registry,
                CancelRegistry::new(),
            )
            .await;

            db_connection
                .create(CreateRequest {
                    created_at: sim_clock.now(),
                    execution_id: execution_id.clone(),
                    ffqn: FFQN,
                    params: Params::empty(),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: sim_clock.now(),
                    component_id: direct_worker.config.component_id.clone(),
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    scheduled_by: None,
                    paused: false,
                })
                .await
                .unwrap();

            let direct_exec_task = ExecTask::new_test(
                ExecConfig {
                    batch_size: 1,
                    lock_expiry: Duration::from_secs(1),
                    tick_sleep: TICK_SLEEP,
                    component_id: direct_worker.config.component_id.clone(),
                    task_limiter_global: None,
                    task_limiter_local: None,
                    executor_id: ExecutorId::from_parts(0, 1),
                    retry_config: ComponentRetryConfig::WORKFLOW,
                    locking_strategy: LockingStrategy::ByComponentDigest,
                },
                direct_worker,
                sim_clock.clone_box(),
                db_pool.clone(),
                Arc::new([FFQN]),
            );

            loop {
                let executed = direct_exec_task
                    .tick_test(sim_clock.now(), RunId::generate())
                    .await
                    .wait_for_tasks()
                    .await
                    .len();
                if executed == 0 {
                    break;
                }
                assert_eq!(1, executed);
            }
            let direct_execution_log = db_connection.get(&execution_id).await.unwrap();
            drop(db_connection);
            db_close.close().await;
            direct_execution_log
        };

        let advance_execution_log = {
            let sim_clock = SimClock::epoch();
            let (_guard, db_pool, db_close) = db.set_up().await;
            let db_connection = db_pool.connection_test().await.unwrap();

            let workflow_engine =
                Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let (workflow_runnable, workflow_component_id) = compile_workflow_with_engine(
                test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
                &workflow_engine,
            )
            .await;

            db_connection
                .create(CreateRequest {
                    created_at: sim_clock.now(),
                    execution_id: execution_id.clone(),
                    ffqn: FFQN,
                    params: Params::empty(),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: sim_clock.now(),
                    component_id: workflow_component_id.clone(),
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    scheduled_by: None,
                    paused: true,
                })
                .await
                .unwrap();

            let replay_worker = Arc::new(build_workflow_replay_worker(
                DeploymentId::generate(),
                workflow_component_id,
                &workflow_runnable,
                workflow_engine,
                fn_registry,
                db_pool.clone(),
                None,
                sim_clock.clone_box(),
            ));
            let advance_execution_log = execute_paused_workflow_until_finished_without_snapshots(
                db_connection.as_ref(),
                WorkflowAdvanceHarness {
                    execution_id,
                    replay_worker,
                },
                &sim_clock,
                16,
            )
            .await;
            drop(db_connection);
            db_close.close().await;
            advance_execution_log
        };

        let expected_cancellation_order =
            normalized_cancellable_request_order(&direct_execution_log)
                .into_iter()
                .rev()
                .collect::<Vec<_>>();
        assert_eq!(
            expected_cancellation_order,
            normalized_response_order(&advance_execution_log),
            "stepped replay+advance must match direct cancellation order",
        );
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn stub_not_found(db: db_tests::Database) {
        execute_workflow_fn_with_single_delay(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            FunctionFqn::new_static_tuple(test_programs_stub_workflow_builder::exports::testing::stub_workflow::workflow::STUB_NOT_FOUND),
            None,
            db,
            "stub_not_found"
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
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
            compile_workflow(test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW)
                .await,
        ]);

        let worker = compile_workflow_worker(
            test_programs_stub_workflow_builder::TEST_PROGRAMS_STUB_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        )
        .await;
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
                component_id: worker.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        let exec_task = ExecTask::new_test(
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: worker.config.component_id.clone(),
                task_limiter_global: None,
                task_limiter_local: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::ByComponentDigest,
            },
            worker,
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
            PendingState::BlockedByJoinSet(PendingStateBlockedByJoinSet {
                join_set_id,
                lock_expires_at:_,
                closing: false,
            }) => join_set_id);
        let stub_execution_id = execution_id.next_level(&join_set_id);
        write_stub_response(
            db_connection.as_ref(),
            sim_clock.now(),
            stub_execution_id,
            SupportedFunctionReturnValue::Err(None),
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
            SupportedFunctionReturnValue::Ok(None)
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
                retval: result.clone(),
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
        #[values(LockingStrategy::ByFfqns, LockingStrategy::ByComponentDigest)]
        locking_strategy: LockingStrategy,
    ) {
        const EXPECT_TRAP_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_serde_workflow_builder::exports::testing::serde_workflow::serde_workflow::EXPECT_TRAP,
        );
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = db.set_up().await;
        let (activity_runnable_component, activity_component_id_first) = compile_activity_stub(
            test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
        )
        .await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            (
                activity_runnable_component,
                activity_component_id_first.clone(),
            ),
            compile_workflow(test_programs_serde_workflow_builder::TEST_PROGRAMS_SERDE_WORKFLOW)
                .await,
        ]);

        let workflow_worker = compile_workflow_worker(
            test_programs_serde_workflow_builder::TEST_PROGRAMS_SERDE_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            CancelRegistry::new(),
        )
        .await;
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
                component_id: match locking_strategy {
                    LockingStrategy::Auto | LockingStrategy::ByFfqns => {
                        ComponentId::dummy_workflow()
                    } // must not matter
                    LockingStrategy::ByComponentDigest => {
                        workflow_worker.config.component_id.clone()
                    }
                },
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        let exec_workflow = ExecTask::new_test(
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: workflow_worker.config.component_id.clone(),
                task_limiter_global: None,
                task_limiter_local: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy,
            },
            workflow_worker,
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
            PendingState::BlockedByJoinSet(PendingStateBlockedByJoinSet {
                join_set_id: _,
                lock_expires_at: _,
                closing: false,
            })
        );

        // 2. Tick the activity executor
        {
            let (activity_worker, activity_component_id) = new_activity_worker(
                test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap(),
                sim_clock.clone_box(),
                TokioSleep,
            )
            .await;
            assert_eq!(
                activity_component_id_first.component_digest,
                activity_component_id.component_digest
            );
            let exec_activity = ExecTask::new_all_ffqns_test(
                activity_worker,
                ExecConfig {
                    batch_size: 1,
                    lock_expiry: Duration::from_secs(1),
                    tick_sleep: TICK_SLEEP,
                    component_id: activity_component_id,
                    task_limiter_global: None,
                    task_limiter_local: None,
                    executor_id: ExecutorId::generate(),
                    retry_config: ComponentRetryConfig::ZERO,
                    locking_strategy,
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
            PendingState::Finished(PendingStateFinished {
                result_kind: PendingStateFinishedResultKind::Ok,
                ..
            },)
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
            "sleep_activity_submit_should_cancel_the_activity"
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
            assert_matches!(result, SupportedFunctionReturnValue::ExecutionFailure(err) => err);
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
            "sleep_activity_submit_then_trap_should_cancel_the_activity"
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
            assert_matches!(result, SupportedFunctionReturnValue::ExecutionFailure(err) => err);
        assert_matches!(result.kind, ExecutionFailureKind::Cancelled);
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_each_stage_replay(
        db: Database,
        #[values(1, 3)] activity_iterations: u32,
    ) {
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, db_close) = db.set_up().await;
        Box::pin(fibo_workflow_each_stage_replay_inner(
            activity_iterations,
            &format!("fibo_workflow_each_stage_replay_{activity_iterations}"),
            db_pool.clone(),
            sim_clock,
        ))
        .await;
        db_close.close().await;
    }

    async fn fibo_workflow_each_stage_replay_inner(
        activity_iterations: u32,
        snapshot_name: &str,
        db_pool: Arc<dyn DbPool>,
        sim_clock: SimClock,
    ) {
        let deployment_id = DeploymentId::from_parts(0, u128::from(activity_iterations));
        let execution_id = ExecutionId::from_parts(0, u128::from(activity_iterations));
        test_utils::set_up();

        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (workflow_runnable, workflow_component_id) = compile_workflow_with_engine(
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            &workflow_engine,
        )
        .await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            (workflow_runnable.clone(), workflow_component_id),
        ]);
        let cancel_registry = CancelRegistry::new();
        let workflow_exec = new_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            JoinNextBlockingStrategy::Interrupt,
            &fn_registry,
            cancel_registry,
            LockingStrategy::ByComponentDigest,
        )
        .await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBOA_CONCURRENT_WORKFLOW_FFQN,
                params: Params::from_json_values_test(vec![
                    json!(FIBO_10_INPUT),
                    json!(activity_iterations),
                ]),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_exec.config.component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();
        let activity_iterations = usize::try_from(activity_iterations).unwrap();

        let (log_sender, _log_storage_recv) = mpsc::channel(100);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: concepts::storage::LogLevel::Debug,
            log_sender: log_sender.clone(),
        });
        let replay_worker = Arc::new(build_workflow_replay_worker(
            deployment_id,
            workflow_exec.config.component_id.clone(),
            &workflow_runnable,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            logs_storage_config.clone(),
            sim_clock.clone_box(),
        ));
        // Replay just after creating - execution is unfinished with no events,
        // preview should return the first event(s) the workflow would produce.
        let replay = replay_worker.replay(execution_id.clone()).await.unwrap();
        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        debug!("Preview after creation: {replay:?}");
        assert!(
            replay.get_return_value().is_none(),
            "workflow should not have completed yet"
        );
        let next_events = replay.history_events();
        assert_eq!(
            2 + activity_iterations,
            next_events.len(),
            "unexpected next_events: {next_events:?}"
        );
        // Verify event types: JoinSetCreate, JoinSetRequest (submit) * activity_iterations, JoinNext (await)
        assert_matches!(&next_events[0], HistoryEvent::JoinSetCreate { .. });
        for child_request in next_events.iter().take(activity_iterations + 1).skip(1) {
            assert_matches!(
                child_request,
                HistoryEvent::JoinSetRequest {
                    request: JoinSetRequest::ChildExecutionRequest { .. },
                    ..
                }
            );
        }
        assert_matches!(next_events.last().unwrap(), HistoryEvent::JoinNext { .. });

        let activity_exec = new_activity_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            TokioSleep,
            LockingStrategy::ByComponentDigest,
        )
        .await;
        let (_steps, _saw_trimmed_preview, res) = advance_paused_workflow_until_finished(
            &*db_connection,
            WorkflowAdvanceHarness {
                execution_id: execution_id.clone(),
                replay_worker: replay_worker.clone(),
            },
            snapshot_name,
            None, // trim_to
            Some(&activity_exec),
            &sim_clock,
            32,
        )
        .await;
        let res = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val);

        let fibo = assert_matches!(res,
            WastValWithType {value: WastVal::U64(val), r#type: TypeWrapper::U64 } => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);

        // Replay after workflow was finished - return_value is present.
        let replay = replay_worker.replay(execution_id).await.unwrap();
        let result = assert_matches!(replay, ReplayResponse::Finished { result } => result);
        assert_matches!(result, SupportedFunctionReturnValue::Ok(_));
    }

    #[tokio::test]
    async fn advance_forwards_captured_application_logs() {
        test_utils::set_up();

        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (workflow_runnable, workflow_component_id) = compile_workflow_with_engine(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            &workflow_engine,
        )
        .await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY)
                .await,
            (workflow_runnable.clone(), workflow_component_id.clone()),
        ]);

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: SLEEP1_HOST_ACTIVITY_FFQN,
                params: Params::from_json_values_test(vec![json!({"milliseconds": 10_u64})]),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: workflow_component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let (log_sender, mut log_recv) = mpsc::channel(16);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: LogLevel::Debug,
            log_sender,
        });

        let replay_worker = build_workflow_replay_worker(
            DeploymentId::generate(),
            workflow_component_id.clone(),
            &workflow_runnable,
            workflow_engine,
            fn_registry,
            db_pool,
            logs_storage_config,
            sim_clock.clone_box(),
        );
        let replay = replay_worker.replay(execution_id.clone()).await.unwrap();
        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        assert_matches!(
            log_recv.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );

        replay_worker.advance(execution_id, replay).await.unwrap();

        assert_matches!(
            log_recv.try_recv(),
            Ok(LogInfoAppendRow {
                log_entry: LogEntry::Log {
                    level: LogLevel::Info,
                    message,
                    ..
                },
                ..
            }) if message == "changed"
        );
        assert_matches!(
            log_recv.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty
                | tokio::sync::mpsc::error::TryRecvError::Disconnected)
        );
    }

    #[tokio::test]
    async fn advance_trimmed_writes_forward_only_prefix_logs() {
        test_utils::set_up();

        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let db_connection = db_pool.connection_test().await.unwrap();
        let execution_id = ExecutionId::generate();
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap();
        let ffqn = FunctionFqn::new_static("test:workflow/workflow", "trimmed-logs");
        let current_time = DateTime::UNIX_EPOCH;
        let old_version = db_connection
            .create(CreateRequest {
                created_at: current_time,
                execution_id: execution_id.clone(),
                ffqn,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: current_time,
                component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();
        let run_id = RunId::generate();

        let make_log = |message: &str| LogInfoAppendRow {
            execution_id: execution_id.clone(),
            run_id,
            log_entry: LogEntry::Log {
                created_at: current_time,
                level: LogLevel::Info,
                message: message.to_owned(),
            },
        };
        let make_append = |version: u32, value: u8, message: &str| {
            InternalCapturedWrite::new_for_test(
                CapturedDbWrite::Append {
                    execution_id: execution_id.clone(),
                    version: Version::new(version),
                    req: AppendRequest {
                        created_at: current_time,
                        event: ExecutionRequest::HistoryEvent {
                            event: HistoryEvent::Persist {
                                value: vec![value],
                                kind: PersistKind::ExecutionId,
                            },
                        },
                    },
                    backtraces: vec![],
                },
                vec![make_log(message)],
            )
        };

        let first_version = old_version.0;
        let second_version = first_version + 1;
        let third_version = second_version + 1;

        let first = make_append(first_version, 1, "first");
        let second = make_append(second_version, 2, "second");
        let third = InternalCapturedWrite::new_for_test(
            CapturedDbWrite::AppendFinished {
                execution_id: execution_id.clone(),
                version: Version::new(third_version),
                current_time,
                retval: SupportedFunctionReturnValue::Ok(None),
                parent: None,
            },
            vec![make_log("third")],
        );

        let (log_sender, mut log_recv) = mpsc::channel(16);

        WorkflowWorker::advance_from_log(
            db_connection.as_ref(),
            &CancelRegistry::new(),
            Some(&log_sender),
            ReplayAdvanceable {
                captured_writes: vec![first.write.clone()],
            },
            vec![first.clone(), second.clone(), third.clone()],
            old_version,
        )
        .await
        .unwrap();
        assert_eq!(drain_forwarded_log_messages(&mut log_recv), vec!["first"]);

        WorkflowWorker::advance_from_log(
            db_connection.as_ref(),
            &CancelRegistry::new(),
            Some(&log_sender),
            ReplayAdvanceable {
                captured_writes: vec![second.write.clone()],
            },
            vec![second.clone(), third.clone()],
            Version::new(second_version),
        )
        .await
        .unwrap();
        assert_eq!(drain_forwarded_log_messages(&mut log_recv), vec!["second"]);

        WorkflowWorker::advance_from_log(
            db_connection.as_ref(),
            &CancelRegistry::new(),
            Some(&log_sender),
            ReplayAdvanceable {
                captured_writes: vec![third.write.clone()],
            },
            vec![third],
            Version::new(third_version),
        )
        .await
        .unwrap();
        assert_eq!(drain_forwarded_log_messages(&mut log_recv), vec!["third"]);
    }

    #[expand_enum_database]
    #[rstest]
    #[case::full(None, "advance_paused_workflow_full", 0)]
    #[case::trimmed_to_1(Some(1), "advance_paused_workflow_trimmed_to_1", 1)]
    #[tokio::test]
    async fn advance_paused_workflow(
        db: Database,
        #[case] trim_to: Option<usize>,
        #[case] snapshot_name: &str,
        #[case] execution_idx: u128,
    ) {
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, db_close) = db.set_up().await;
        Box::pin(advance_paused_workflow_inner(
            db_pool.clone(),
            sim_clock,
            trim_to,
            snapshot_name,
            execution_idx,
        ))
        .await;
        db_close.close().await;
    }

    struct WorkflowAdvanceHarness {
        execution_id: ExecutionId,
        replay_worker: Arc<WorkflowWorker>,
    }

    fn redact_replay(replay: &ReplayAdvanceable) -> serde_json::Value {
        let replay = serde_json::to_value(replay).unwrap();
        let replay = redact_backtrace_file(replay);
        let replay = redact_rust_symbol_hash(replay);
        redact_component_digest(replay)
    }

    fn redact_backtrace_file(value: Value) -> Value {
        match value {
            Value::Array(items) => {
                Value::Array(items.into_iter().map(redact_backtrace_file).collect())
            }
            Value::Object(map) => Value::Object(
                map.into_iter()
                    .map(|(key, value)| {
                        if key == "file"
                            && let Value::String(value) = value
                        {
                            let suffix = value
                                .rsplit_once('/')
                                .map(|(_, suffix)| suffix.to_string())
                                .unwrap_or_default();
                            (key, Value::String(format!("<REDACTED>/{suffix}")))
                        } else {
                            (key, redact_backtrace_file(value))
                        }
                    })
                    .collect(),
            ),
            other => other,
        }
    }

    /// Strip Rust symbol hash suffixes (e.g. `::h0e8d9ba882be7d50`) from `func_name` values.
    /// These hashes are not stable across rebuilds and cause snapshot churn.
    fn redact_rust_symbol_hash(value: Value) -> Value {
        match value {
            Value::Array(items) => {
                Value::Array(items.into_iter().map(redact_rust_symbol_hash).collect())
            }
            Value::Object(map) => Value::Object(
                map.into_iter()
                    .map(|(key, value)| {
                        if key == "func_name"
                            && let Value::String(s) = value
                        {
                            (key, Value::String(strip_rust_symbol_hash(&s)))
                        } else {
                            (key, redact_rust_symbol_hash(value))
                        }
                    })
                    .collect(),
            ),
            other => other,
        }
    }

    /// If `s` ends with `::h` followed by exactly 16 hex digits, replace that suffix with `::h<REDACTED>`.
    fn strip_rust_symbol_hash(s: &str) -> String {
        // `::h` + 16 hex chars = 19 bytes from the end
        if s.len() >= 19 {
            let (prefix, suffix) = s.split_at(s.len() - 19);
            if suffix.starts_with("::h")
                && suffix[3..].len() == 16
                && suffix[3..].chars().all(|c| c.is_ascii_hexdigit())
            {
                return format!("{prefix}::h<REDACTED>");
            }
        }
        s.to_string()
    }

    async fn advance_paused_workflow_until_finished(
        db_connection: &dyn DbConnectionTest,
        harness: WorkflowAdvanceHarness,
        test_name: &str,
        trim_to: Option<usize>,
        activity_exec: Option<&ExecTask>,
        sim_clock: &SimClock,
        max_steps: usize,
    ) -> (usize, bool, SupportedFunctionReturnValue) {
        let mut steps = 0;
        let mut saw_trimmed_preview = false; // at least one replay got trimmed

        loop {
            sim_clock.move_time_forward(Duration::from_millis(100));
            let replay = harness
                .replay_worker
                .replay(harness.execution_id.clone())
                .await
                .unwrap();

            let replay = match replay {
                ReplayResponse::Advanceable(replay) => replay,
                ReplayResponse::Finished {
                    result: finished_result,
                } => {
                    return (steps, saw_trimmed_preview, finished_result);
                }
                ReplayResponse::Blocked => {
                    if let Some(activity_exec) = activity_exec {
                        let executed_activities = activity_exec
                            .tick_test_await(sim_clock.now(), RunId::generate())
                            .await;
                        assert_eq!(
                            executed_activities.len(),
                            1,
                            "expected one pending child activity when workflow replay is blocked",
                        );
                        continue;
                    }
                    panic!("replay must be advanceable or finished while stepping paused workflow");
                }
            };

            steps += 1;
            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_replay_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    insta::assert_json_snapshot!(redact_replay(&replay));
                }
            );

            let requested = match trim_to {
                Some(trim_to) => replay.truncate_to(trim_to),
                None => replay.clone(),
            };
            saw_trimmed_preview |= requested.captured_writes.len() < replay.captured_writes.len();
            sim_clock.move_time_forward(Duration::from_millis(100));

            let advance = harness
                .replay_worker
                .advance(harness.execution_id.clone(), requested.clone())
                .await
                .unwrap();
            assert_eq!(advance.finished, requested.get_return_value().cloned());

            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_advance_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    insta::assert_json_snapshot!(json!({
                        "finished": advance.finished.is_some(),
                        "requested_captured_writes_len": requested.captured_writes.len(),
                        "replayed_captured_writes_len": replay.captured_writes.len(),
                    }));
                }
            );

            let log = db_connection.get(&harness.execution_id).await.unwrap();

            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_log_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    assert_json_snapshot!(ExecutionLogSanitized::from(log));
                }
            );

            if let Ok(finished_result) = db_connection
                .get_finished_result(&harness.execution_id)
                .await
            {
                return (steps, saw_trimmed_preview, finished_result);
            }

            assert!(
                steps < max_steps,
                "execution did not finish after {steps} replay+advance steps",
            );
        }
    }

    async fn advance_paused_workflow_inner(
        db_pool: Arc<dyn DbPool>,
        sim_clock: SimClock,
        trim_to: Option<usize>,
        snapshot_name: &str,
        execution_idx: u128,
    ) {
        test_utils::set_up();

        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (workflow_runnable, workflow_component_id) = compile_workflow_with_engine(
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            &workflow_engine,
        )
        .await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            (workflow_runnable.clone(), workflow_component_id.clone()),
        ]);

        // Create a paused execution.
        let execution_id = ExecutionId::from_parts(0, execution_idx);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let version_created = db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBOA_CONCURRENT_WORKFLOW_FFQN,
                params: Params::from_json_values_test(vec![json!(FIBO_10_INPUT), json!(1u32)]),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let (log_sender, _log_storage_recv) = mpsc::channel(100);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: concepts::storage::LogLevel::Debug,
            log_sender: log_sender.clone(),
        });
        let activity_exec = new_activity_fibo(
            db_pool.clone(),
            sim_clock.clone_box(),
            TokioSleep,
            LockingStrategy::ByFfqns,
        )
        .await;

        let deployment_id = DeploymentId::from_parts(0, 0);

        let replay_worker = Arc::new(build_workflow_replay_worker(
            deployment_id,
            workflow_component_id,
            &workflow_runnable,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            logs_storage_config,
            sim_clock.clone_box(),
        ));

        // Step 1: Replay to get expected events and version.
        let replay = replay_worker.replay(execution_id.clone()).await.unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        assert!(
            !replay.captured_writes.is_empty(),
            "paused workflow should have captured writes"
        );
        assert!(
            replay.get_return_value().is_none(),
            "workflow should not have completed yet"
        );
        assert_eq!(
            &version_created,
            replay.starting_version().unwrap(),
            "expected replay starting version must equal to {version_created}",
        );

        // Step 2: Advance with wrong version should fail.
        let wrong_version = Version::new(replay.starting_version().unwrap().0 + 999);
        // Create a replay with a wrong starting version in the first captured write.
        let wrong_replay = {
            let mut writes = replay.captured_writes.clone();
            if let Some(CapturedDbWrite::Append { version, .. }) = writes.first_mut() {
                *version = wrong_version;
            }
            ReplayAdvanceable {
                captured_writes: writes,
            }
        };
        let advance_result = replay_worker
            .advance(execution_id.clone(), wrong_replay)
            .await
            .unwrap_err();
        assert_matches!(
            advance_result,
            AdvanceError::VersionMismatch { expected } if expected == version_created
        );

        // Step 3: Advancing with zero requested writes should fail.
        let advance_result = replay_worker
            .advance(
                execution_id.clone(),
                ReplayAdvanceable {
                    captured_writes: vec![], // empty writes
                },
            )
            .await
            .unwrap_err();
        assert_matches!(advance_result, AdvanceError::NoWrites);

        // Step 4: Step the paused workflow to completion.
        let (steps, saw_trimmed_preview, finished_result) = advance_paused_workflow_until_finished(
            &*db_connection,
            WorkflowAdvanceHarness {
                execution_id: execution_id.clone(),
                replay_worker,
            },
            snapshot_name,
            trim_to,
            Some(&activity_exec),
            &sim_clock,
            32,
        )
        .await;
        assert!(
            steps > 0,
            "step-through harness must execute at least one replay+advance round",
        );
        if trim_to.is_some() {
            assert!(
                saw_trimmed_preview,
                "test must exercise trimmed replay writes",
            );
        }

        // Step 5: Verify the execution log was updated.
        let log = db_connection.get(&execution_id).await.unwrap();
        assert!(log.next_version.0 > version_created.0);

        let result = assert_matches!(
            finished_result,
            SupportedFunctionReturnValue::Ok(Some(WastValWithType {
                value: WastVal::U64(val),
                r#type: TypeWrapper::U64,
            })) => val
        );
        assert_eq!(result, FIBO_10_OUTPUT);
    }

    #[test]
    fn replay_response_accepts_paused_override_for_new_child_execution() {
        let created_at = DateTime::default();
        let execution_id = ExecutionId::from_parts(0, 1);
        let child_execution_id = ExecutionId::from_parts(0, 2);
        let component_id = ComponentId::dummy_activity();
        let deployment_id = DEPLOYMENT_ID_DUMMY;
        let ffqn = FunctionFqn::new_static_tuple(
            test_programs_fibo_activity_builder::exports::testing::fibo::fibo::FIBO,
        );
        let params = Params::from_json_values_test(vec![json!(1u32)]);
        let batch = vec![AppendRequest {
            created_at,
            event: ExecutionRequest::Unpaused,
        }];
        let fresh = ReplayAdvanceable {
            captured_writes: vec![CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time: created_at,
                batch: batch.clone(),
                execution_id: execution_id.clone(),
                version: Version::new(1),
                child_req: vec![CreateRequest {
                    created_at,
                    execution_id: child_execution_id.clone(),
                    ffqn: ffqn.clone(),
                    params: params.clone(),
                    parent: None,
                    scheduled_at: created_at,
                    component_id: component_id.clone(),
                    deployment_id,
                    metadata: ExecutionMetadata::empty(),
                    scheduled_by: None,
                    paused: false,
                }],
                backtraces: vec![],
            }],
        };
        let requested = ReplayAdvanceable {
            captured_writes: vec![CapturedDbWrite::AppendBatchCreateNewExecution {
                current_time: created_at,
                batch,
                execution_id,
                version: Version::new(1),
                child_req: vec![CreateRequest {
                    created_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params,
                    parent: None,
                    scheduled_at: created_at,
                    component_id,
                    deployment_id,
                    metadata: ExecutionMetadata::empty(),
                    scheduled_by: None,
                    paused: true,
                }],
                backtraces: vec![],
            }],
        };

        assert!(requested.is_prefix_of(&fresh.captured_writes));
    }

    #[tokio::test]
    async fn advance_paused_workflow_can_pause_new_child_execution() {
        test_utils::set_up();

        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (workflow_runnable, workflow_component_id) = compile_workflow_with_engine(
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            &workflow_engine,
        )
        .await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
            (workflow_runnable.clone(), workflow_component_id.clone()),
        ]);
        let db_connection = db_pool.connection_test().await.unwrap();
        let execution_id = ExecutionId::from_parts(0, 9001);
        let created_at = sim_clock.now();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBOA_CONCURRENT_WORKFLOW_FFQN,
                params: Params::from_json_values_test(vec![json!(FIBO_10_INPUT), json!(1u32)]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: workflow_component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let deployment_id = DeploymentId::from_parts(0, 0);
        sim_clock.move_time_forward(Duration::from_millis(100));
        let replay_worker = build_workflow_replay_worker(
            deployment_id,
            workflow_component_id,
            &workflow_runnable,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            None,
            sim_clock.clone_box(),
        );
        let replay = replay_worker.replay(execution_id.clone()).await.unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        let mut requested = replay.clone();
        let replayed_child_created_at = requested
            .captured_writes
            .iter()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution { child_req, .. } => {
                    child_req.first().map(|child| child.created_at)
                }
                _ => None,
            })
            .expect("replay should create a child execution");
        assert_eq!(replayed_child_created_at, sim_clock.now());
        let child_execution_id = requested
            .captured_writes
            .iter_mut()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time,
                    batch,
                    child_req,
                    ..
                } => {
                    *current_time = DateTime::UNIX_EPOCH;
                    for req in batch.iter_mut() {
                        req.created_at = DateTime::UNIX_EPOCH;
                    }
                    for child in child_req.iter_mut() {
                        child.paused = true;
                        child.created_at = DateTime::UNIX_EPOCH;
                        child.scheduled_at = DateTime::UNIX_EPOCH;
                    }
                    child_req.first().map(|child| child.execution_id.clone())
                }
                _ => None,
            })
            .expect("replay should create a child execution");
        sim_clock.move_time_forward(Duration::from_millis(100));

        let advance = replay_worker
            .advance(execution_id, requested)
            .await
            .unwrap();

        assert_eq!(advance.finished, None);
        assert_matches!(
            db_connection
                .get_pending_state(&child_execution_id)
                .await
                .unwrap()
                .pending_state,
            PendingState::Paused(_)
        );
        let create_event = db_connection
            .get_execution_event(&child_execution_id, &Version::new(0))
            .await
            .unwrap();
        assert_eq!(create_event.created_at, sim_clock.now());
        let ExecutionRequest::Created { scheduled_at, .. } = create_event.event else {
            panic!("child execution log must start with Created");
        };
        assert_eq!(scheduled_at, sim_clock.now());
    }
}
